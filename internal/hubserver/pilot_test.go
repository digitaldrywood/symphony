package hubserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestPilotIdleRunnerReconciliation(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, 16} {
		t.Run(fmt.Sprintf("runners_%d", count), func(t *testing.T) {
			t.Parallel()
			f := newNativeFixture(t, nil, "", "pilot")
			if err := f.service.stopGitHubReconciliation(); err != nil {
				t.Fatal(err)
			}
			backend := &scriptedReconcileBackend{}
			f.service.config.ReconcileBackend = backend
			repository, _ := seedProjection(t, f.service.database.db)
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE projects SET repository_id = NULL WHERE repository_id = ?", repository); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE projects SET repository_id = ?, github_repository_enabled = 1 WHERE id = ?", repository, f.project.ID); err != nil {
				t.Fatal(err)
			}
			var runners []runnerFixture
			for i := range count {
				runner := prepareRunner(t, f, runnerauth.Read, runnerauth.Claim, runnerauth.Heartbeat)
				runner.redemption.DisplayName = fmt.Sprintf("Pilot host %d", i)
				runner.enroll(t)
				runners = append(runners, runner)
			}
			for _, mode := range []ReconcileMode{ReconcileIncremental, ReconcileFullRepair} {
				for range 3 {
					for _, runner := range runners {
						requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, f.base+"/machines/"+string(runner.binding.MachineID)+"/heartbeat", runner.redemption.Credential, map[string]any{"capacity": 2, "version": "test"}), http.StatusNoContent)
						requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodGet, f.base+"/work-items?state=Todo", runner.redemption.Credential, nil), http.StatusOK)
					}
					backend.steps = append(backend.steps, reconcileStep{snapshot: ReconcileSnapshot{Repository: RepositorySource{NodeID: "R_repo", Owner: "digitaldrywood", Name: "detent", UpdatedAt: time.Now().UTC()}}})
					targets, err := f.service.database.reconcileTargets(t.Context())
					if err != nil || len(targets) != 1 {
						t.Fatalf("reconciliation targets = %d: %v", len(targets), err)
					}
					if err := f.service.reconcileRepository(t.Context(), targets[0], mode); err != nil {
						t.Fatal(err)
					}
				}
			}
			requests := backend.Requests()
			if len(requests) != 6 || len(backend.steps) != 0 {
				t.Fatalf("runner count multiplied reconciliation: %d calls", len(requests))
			}
			for _, request := range requests {
				if request.Profile != "native" || !request.SkipIssues || request.SkipRepository {
					t.Fatalf("incorrect native reconciliation scope: %+v", request)
				}
			}
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE projects SET github_repository_enabled = 0 WHERE id = ?", f.project.ID); err != nil {
				t.Fatal(err)
			}
			f.service.reconcileAllRepositories(t.Context(), ReconcileFullRepair)
			if len(backend.Requests()) != 6 {
				t.Fatal("disabled integration polled GitHub")
			}
			t.Logf("PILOT fleet runners=%d heartbeat_requests=%d candidate_reads=%d reconciliation_cycles=6 backend_calls=6 disabled_calls=0", count, count*6, count*6)
		})
	}
}

func pilotHostedRunner(t *testing.T, f *browserHostedFixture, index int) runnerauth.Redemption {
	t.Helper()
	base := "/api/v2/organizations/" + f.service.config.Hosted.OrganizationID
	binding := runnerauth.NewBinding()
	response := f.setupRequest(t, "owner", http.MethodPost, base+"/runner-enrollments", runnerauth.EnrollmentRequest{Binding: binding, ProjectIDs: []tracker.ProjectID{tracker.ProjectID(f.project), tracker.ProjectID(f.privateProject)}, Operations: []string{runnerauth.Read, runnerauth.Claim, runnerauth.Heartbeat, runnerauth.Events, runnerauth.Collaborate}, TTLSeconds: 900})
	requireNativeStatus(t, response, http.StatusCreated)
	var enrollment runnerauth.Enrollment
	decodeHubResponse(t, response, &enrollment)
	credential, err := apikey.GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	redemption := runnerauth.Redemption{Binding: binding, Credential: credential, Hostname: fmt.Sprintf("pilot-host-%d", index), DisplayName: fmt.Sprintf("Pilot runner %d", index), Capacity: 2, Version: "test", OS: "linux", Architecture: "amd64"}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/runner-enrollments/redeem", enrollment.Token, redemption), http.StatusCreated)
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/runners/"+binding.RunnerID+"/routing", map[string]any{"expected_revision": 1, "display_name": redemption.DisplayName, "tags": []string{"linux", "gpu"}, "state": "active", "capacity_limit": 2, "project_ids": []string{f.project, f.privateProject}}), http.StatusOK)
	return redemption
}

func pilotHostedGrants(t *testing.T, f *browserHostedFixture) {
	t.Helper()
	for _, project := range []string{f.project, f.privateProject} {
		requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {project}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
	}
}

func TestPilotHostedWorkloads(t *testing.T) {
	for _, test := range []struct {
		name    string
		runners int
		jobs    int
	}{
		{"idle_baseline", 0, 0},
		{"idle_one_runner", 1, 0},
		{"idle_sixteen_runners", 16, 0},
		{"active_one_runner", 1, 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newBrowserHostedFixture(t, true)
			hostedTestPlans(t, f.service, map[string]int64{"registered_runners": 20, "connected_runners": 20})
			pilotHostedGrants(t, f)
			base := "/api/v2/organizations/org_browser_preview/projects/" + f.project
			descriptor := hubTestPolicy()
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{Policy: descriptor}), http.StatusOK)
			var runners []runnerauth.Redemption
			for index := range test.runners {
				runners = append(runners, pilotHostedRunner(t, f, index))
			}
			before := pilotUsage(t, f.service)
			baseline := pilotBackupBytes(t, f.service)
			started := time.Now()
			for range 10 {
				for _, runner := range runners {
					path := base + "/machines/" + string(runner.MachineID) + "/heartbeat"
					requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, runner.Credential, map[string]any{"capacity": 2, "version": "test", "os": "linux", "architecture": "amd64"}), http.StatusNoContent)
					requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodGet, base+"/work-items?state=Todo", runner.Credential, nil), http.StatusOK)
				}
				requireNativeStatus(t, f.page(t, "owner", "/organization/plan"), http.StatusOK)
			}
			for index := range test.jobs {
				response := f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: fmt.Sprintf("pilot-job-%d", index)}, Title: fmt.Sprintf("Pilot work %d", index), Body: strings.Repeat("synthetic context ", 64), State: "Todo"})
				requireNativeStatus(t, response, http.StatusOK)
				var issue tracker.NativeIssue
				decodeHubResponse(t, response, &issue)
				path := base + "/work-items/" + string(issue.WorkItemID)
				requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path+"/comments", tracker.CreateComment{Mutation: tracker.Mutation{IdempotencyKey: "discussion"}, Body: "Synthetic pilot discussion"}), http.StatusOK)
				runner := runners[0]
				response = performHubAPIRequest(t, f.service, http.MethodPost, base+"/claims", runner.Credential, tracker.NativeClaim{PolicyID: descriptor.ID, WorkItemID: issue.WorkItemID, MachineID: runner.MachineID, SessionID: fmt.Sprintf("session-%d", index), TTLSeconds: 90, ProtocolMajor: 2, Capabilities: []string{"native_issues", "scoped_collaboration", tracker.NativeExecutionCapability}})
				requireNativeStatus(t, response, http.StatusOK)
				var lease tracker.NativeLease
				decodeHubResponse(t, response, &lease)
				start := nativeStartedEvent(lease)
				finish := start
				finish.Type, finish.IdempotencyKey, finish.Data.Sequence, finish.Data.Outcome = "run.finished", "finish", 2, "succeeded"
				for _, event := range []tracker.NativeRunEvent{start, finish} {
					requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", runner.Credential, event), http.StatusOK)
				}
				requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/leases/"+string(lease.ID)+"/release", runner.Credential, tracker.NativeLeaseMutation{FencingToken: lease.FencingToken, Reason: "completed"}), http.StatusNoContent)
			}
			elapsed := time.Since(started)
			after := pilotUsage(t, f.service)
			for metric := range after {
				after[metric] -= before[metric]
			}
			if after["heartbeats"] != int64(test.runners*10) || after["ingested_events"] != int64(test.jobs*4) {
				t.Fatalf("usage does not match admitted workload: %v", after)
			}
			usage, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
			if err != nil || usage.Usage["concurrent_work"] != 0 {
				t.Fatalf("completed workload retained reservations: %v, %v", usage.Usage, err)
			}
			payload, err := json.Marshal(map[string]any{"workload": test.name, "runners": test.runners, "jobs": test.jobs, "rounds": 10, "elapsed_microseconds": elapsed.Microseconds(), "baseline_backup_bytes": baseline, "final_backup_bytes": pilotBackupBytes(t, f.service), "usage_delta": after})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("PILOT workload %s", payload)
		})
	}
}

func pilotUsage(t *testing.T, service *Service) map[string]int64 {
	t.Helper()
	rows, err := service.database.db.QueryContext(t.Context(), "SELECT metric, SUM(amount) FROM hosted_usage_windows GROUP BY metric")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make(map[string]int64)
	for rows.Next() {
		var metric string
		var amount int64
		if err := rows.Scan(&metric, &amount); err != nil {
			t.Fatal(err)
		}
		values[metric] = amount
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func pilotBackupBytes(t *testing.T, service *Service) int64 {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pilot-backup.db")
	if err := service.Backup(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
