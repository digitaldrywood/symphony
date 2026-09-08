package hubserver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const testHostedPlanAdminToken = "plan-admin-token-for-isolated-tests-2195"

func TestHostedProjectRetryAfterDowngrade(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		projects int64
		features bool
	}{
		{"at project limit", 3, true},
		{"over project limit", 0, true},
		{"collaboration disabled", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newBrowserHostedFixture(t, true)
			config := hostedTestPlans(t, f.service, map[string]int64{"projects": 3})
			project := f.createProject(t, "Resumable allowance project")
			plan := config.Plans[0]
			plan.Version++
			plan.Allowances = maps.Clone(plan.Allowances)
			plan.Allowances["projects"] = test.projects
			if !test.features {
				plan.Features = nil
			}
			config.Plans = append(config.Plans, plan)
			cfg := *f.service.config.Hosted
			cfg.Plans = &config
			if err := f.service.database.configureHostedPlans(t.Context(), &cfg); err != nil {
				t.Fatal(err)
			}
			if err := f.service.database.applyHostedPlanCommand(t.Context(), bootstrapTokenID, hostedPlanCommand{ID: "downgrade", Action: "base", ExpectedRevision: 1, Plan: plan.PlanReference, Reason: "pilot downgrade"}); err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			before, err := f.service.database.hostedPlanUsage(t.Context(), now)
			if err != nil {
				t.Fatal(err)
			}
			var group sync.WaitGroup
			for range 8 {
				group.Go(func() {
					response := f.form(t, "owner", "/projects", url.Values{"name": {"Resumable allowance project"}, "grant_access": {"true"}})
					if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/projects/"+project {
						t.Errorf("project retry = %d %s", response.Code, response.Body.String())
					}
				})
			}
			group.Wait()
			requireNativeStatus(t, f.form(t, "owner", "/projects", url.Values{"name": {"Excess project"}, "grant_access": {"true"}}), http.StatusTooManyRequests)
			after, err := f.service.database.hostedPlanUsage(t.Context(), now)
			if err != nil {
				t.Fatal(err)
			}
			for resource := range before.Allowances {
				if before.Usage[resource] != after.Usage[resource] {
					t.Errorf("retry or rejection changed %s usage: before=%d after=%d", resource, before.Usage[resource], after.Usage[resource])
				}
			}
			for _, table := range []string{"hosted_project_grants", "token_grants", "workflow_states"} {
				var orphans int
				if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table+" WHERE project_id NOT IN (SELECT id FROM projects)").Scan(&orphans); err != nil || orphans != 0 {
					t.Fatalf("rejected allocation left %s records: %d %v", table, orphans, err)
				}
			}
			for _, path := range []string{"/projects/" + project, "/api/v2/organizations/org_browser_preview/projects/" + project + "/onboarding", "/organization/plan", "/api/cloud/billing"} {
				requireNativeStatus(t, f.page(t, "owner", path), http.StatusOK)
			}
		})
	}
}

func hostedTestPlans(t *testing.T, service *Service, limits map[string]int64) HostedPlansConfig {
	t.Helper()
	service.config.Hosted.EntitlementAdminToken = []byte(testHostedPlanAdminToken)
	service.config.Hosted.EntitlementAdministrator = "test-operator"
	config := pilotHostedPlans()
	free := config.Plans[0]
	free.ID = "test_free"
	maps.Copy(free.Allowances, limits)
	paid := free
	paid.ID = "test_paid"
	paid.Allowances = maps.Clone(free.Allowances)
	paid.Allowances["projects"] = 20
	paid.Allowances["concurrent_work"] = 20
	paid.Features = append(slices.Clone(free.Features), "hosted_artifacts")
	config.Plans = []HostedPlan{free, paid}
	config.Base = free.PlanReference
	cfg := *service.config.Hosted
	cfg.Plans = &config
	if err := service.database.configureHostedPlans(t.Context(), &cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := service.database.db.ExecContext(t.Context(), "UPDATE hosted_plan_assignments SET base_id = ?,base_version = ?", free.ID, free.Version); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestHostedPlanResolutionBoundaries(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name         string
		subscription bool
		grant        bool
		revoke       bool
		at           time.Duration
		want         string
		projects     int64
		feature      bool
	}{
		{"free without payment", false, false, false, 0, "base", 2, false},
		{"paid base", true, false, false, 0, "subscription", 20, true},
		{"scoped grant", false, true, false, 0, "base", 20, false},
		{"before grant expiry", false, true, false, time.Minute - time.Nanosecond, "base", 20, false},
		{"exact grant expiry", false, true, false, time.Minute, "base", 2, false},
		{"expiry returns to paid", true, true, false, time.Minute, "subscription", 20, true},
		{"subscription deadline", true, false, false, 2 * time.Minute, "base", 2, false},
		{"revocation", false, true, true, 0, "base", 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedSecurityFixture(t)
			config := hostedTestPlans(t, f.service, map[string]int64{"projects": 2})
			f.service.database.now = func() time.Time { return now }
			revision := int64(1)
			apply := func(command hostedPlanCommand) {
				t.Helper()
				command.ExpectedRevision = revision
				command.Reason = "pilot testing"
				if err := f.service.database.applyHostedPlanCommand(t.Context(), bootstrapTokenID, command); err != nil {
					t.Fatal(err)
				}
				revision++
			}
			if test.subscription {
				until := now.Add(2 * time.Minute)
				apply(hostedPlanCommand{ID: "paid", Action: "subscription", Plan: config.Plans[1].PlanReference, ExpiresAt: &until})
			}
			if test.grant {
				until := now.Add(time.Minute)
				apply(hostedPlanCommand{ID: "grant", Action: "grant", GrantID: "pilot_grant", Plan: config.Plans[1].PlanReference, Scope: []string{"projects"}, ExpiresAt: &until})
			}
			if test.revoke {
				apply(hostedPlanCommand{ID: "revoke", Action: "revoke", GrantID: "pilot_grant"})
			}
			got, err := f.service.database.hostedPlanUsage(t.Context(), now.Add(test.at))
			if err != nil {
				t.Fatal(err)
			}
			if got.Source != test.want || got.Allowances["projects"] != test.projects || slices.Contains(got.Features, "hosted_artifacts") != test.feature {
				t.Fatalf("unexpected entitlement: %+v", got)
			}
			if got.Usage["projects"] != 1 {
				t.Fatal("plan change deleted existing project")
			}
		})
	}
}

func TestHostedPlanCommandsAndPermissions(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	config := hostedTestPlans(t, f.service, nil)
	owner := f.user(t, "owner", "owner", "owner@example.test", "", "")
	member := f.user(t, "member", "member", "member@example.test", "", "")
	command := hostedPlanCommand{ID: "grant", Action: "grant", ExpectedRevision: 1, GrantID: "comp", Plan: config.Plans[1].PlanReference, Scope: []string{"projects"}, Reason: "pilot approval"}
	path := "/api/v2/organizations/org_security/entitlements"
	for _, test := range []struct {
		name, path string
		user       hostedSecurityUser
		want       int
	}{
		{"owner cannot self grant", path, owner, http.StatusForbidden},
		{"member cannot self grant", path, member, http.StatusForbidden},
		{"foreign organization", strings.Replace(path, "org_security", "org_foreign", 1), owner, http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireNativeStatus(t, f.request(t, test.user, http.MethodPost, test.path, command), test.want)
		})
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, testHubAdminToken, command), http.StatusForbidden)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, testHostedPlanAdminToken, command), http.StatusNoContent)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, testHostedPlanAdminToken, command), http.StatusNoContent)
	command.Scope = []string{"concurrent_work"}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, testHostedPlanAdminToken, command), http.StatusConflict)
	var audits int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_plan_audit").Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("duplicate command produced %d audits", audits)
	}
	for _, test := range []struct {
		name string
		user hostedSecurityUser
		want int
	}{
		{"owner sees billing", owner, http.StatusOK}, {"member cannot see billing", member, http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireNativeStatus(t, f.request(t, test.user, http.MethodGet, "/api/cloud/billing", nil), test.want)
		})
	}
	report := performHubAPIRequest(t, f.service, http.MethodGet, "/api/cloud/metadata", testHubAdminToken, nil)
	requireNativeStatus(t, report, http.StatusOK)
	for _, forbidden := range []string{"pilot approval", "private-project-sentinel", "owner@example.test", "record_json", "request_hash"} {
		if strings.Contains(report.Body.String(), forbidden) {
			t.Fatalf("metadata leaked %q", forbidden)
		}
	}
}

func TestHostedConcurrentClaimsDowngradeRelease(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	config := hostedTestPlans(t, f.service, map[string]int64{"concurrent_work": 1})
	now := time.Now().UTC()
	clock := &leaseTestClock{value: now}
	f.service.database.now = clock.Now
	const contenders = 8
	requests := make([]tracker.ClaimRequest, contenders)
	for i := range contenders {
		item := f.seedIssue(t, i+1)
		var id tracker.WorkItemID
		if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM issues WHERE native_id = ?", item).Scan(&id); err != nil {
			t.Fatal(err)
		}
		machine := tracker.MachineID(fmt.Sprintf("machine-%d", i))
		if _, err := f.service.database.db.ExecContext(t.Context(), `INSERT INTO machines(id,hostname,display_name,capacity,version,last_heartbeat_at,registered_at,updated_at,organization_id,token_id) VALUES(?,?,?,1,'test',?,?,?,'org_security','bootstrap-admin')`, machine, machine, machine, formatHubTime(now), formatHubTime(now), formatHubTime(now)); err != nil {
			t.Fatal(err)
		}
		requests[i] = tracker.ClaimRequest{WorkItemID: id, MachineID: machine, SessionID: fmt.Sprintf("session-%d", i), TTL: time.Minute}
	}
	type outcome struct {
		lease tracker.Lease
		err   error
		index int
	}
	start := make(chan struct{})
	results := make(chan outcome, contenders)
	var group sync.WaitGroup
	for i, request := range requests {
		group.Go(func() {
			<-start
			lease, err := f.service.Tracker().Claim(t.Context(), request)
			results <- outcome{lease, err, i}
		})
	}
	close(start)
	group.Wait()
	close(results)
	var winner outcome
	winners := 0
	for result := range results {
		if result.err == nil {
			winner = result
			winners++
			continue
		}
		var limit *hostedLimitError
		if !errors.As(result.err, &limit) || limit.Resource != "concurrent_work" {
			t.Errorf("claim error: %v", result.err)
		}
	}
	if winners != 1 {
		t.Fatalf("got %d winners", winners)
	}
	again, err := f.service.Tracker().Claim(t.Context(), requests[winner.index])
	if err != nil || again.ID != winner.lease.ID {
		t.Fatalf("retry = %v %v", again, err)
	}
	zero := config.Plans[0]
	zero.Version = 2
	zero.Allowances = maps.Clone(zero.Allowances)
	zero.Allowances["concurrent_work"] = 0
	config.Plans = append(config.Plans, zero)
	cfg := *f.service.config.Hosted
	cfg.Plans = &config
	if err := f.service.database.configureHostedPlans(t.Context(), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := f.service.database.applyHostedPlanCommand(t.Context(), bootstrapTokenID, hostedPlanCommand{ID: "downgrade", Action: "base", ExpectedRevision: 1, Plan: zero.PlanReference, Reason: "pilot downgrade"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Tracker().Renew(t.Context(), tracker.RenewRequest{LeaseID: winner.lease.ID, FencingToken: winner.lease.FencingToken, TTL: time.Minute}); err != nil {
		t.Fatalf("downgrade prevented safe renewal: %v", err)
	}
	for i := range 2 {
		err := f.service.Tracker().Release(t.Context(), tracker.ReleaseRequest{LeaseID: winner.lease.ID, FencingToken: winner.lease.FencingToken, Reason: "work_item_hydration_failed"})
		if i == 0 && err != nil || i == 1 && !errors.Is(err, tracker.ErrStaleFencingToken) {
			t.Fatalf("release %d: %v", i, err)
		}
	}
	usage, err := f.service.database.hostedPlanUsage(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Usage["concurrent_work"] != 0 {
		t.Fatal("failed start retained capacity")
	}
	if err := f.service.database.applyHostedPlanCommand(t.Context(), bootstrapTokenID, hostedPlanCommand{ID: "restore", Action: "base", ExpectedRevision: 2, Plan: config.Base, Reason: "pilot restore"}); err != nil {
		t.Fatal(err)
	}
	next := requests[(winner.index+1)%contenders]
	lease, err := f.service.Tracker().Claim(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	next = requests[(winner.index+2)%contenders]
	if _, err := f.service.Tracker().Claim(t.Context(), next); err != nil {
		t.Fatalf("exact expiry retained capacity: %v", err)
	}
	if err := f.service.Tracker().Release(t.Context(), tracker.ReleaseRequest{LeaseID: lease.ID, FencingToken: lease.FencingToken}); !errors.Is(err, tracker.ErrStaleFencingToken) {
		t.Fatalf("stale release=%v", err)
	}
}

func TestHostedMutationQuotaRollbackAndRetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		limits map[string]int64
		want   int
	}{
		{"event exhaustion", map[string]int64{"ingested_events": 0}, http.StatusTooManyRequests},
		{"storage exhaustion", map[string]int64{"collaboration_bytes": 1}, http.StatusTooManyRequests},
		{"history exhaustion", map[string]int64{"history_records": 0}, http.StatusTooManyRequests},
		{"allowed", nil, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedSecurityFixture(t)
			hostedTestPlans(t, f.service, test.limits)
			owner := f.user(t, "owner", "owner", "owner@example.test", "write", "")
			request := map[string]any{"idempotency_key": "create", "title": "private-title", "body": "private-body", "state": "Todo"}
			response := f.request(t, owner, http.MethodPost, f.base+"/work-items", request)
			requireNativeStatus(t, response, test.want)
			var events, commands int
			if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT (SELECT count(*) FROM collaboration_events),(SELECT count(*) FROM native_commands)").Scan(&events, &commands); err != nil {
				t.Fatal(err)
			}
			if test.want == http.StatusTooManyRequests {
				if events != 0 || commands != 0 {
					t.Fatal("rejected mutation persisted data or idempotency receipt")
				}
				return
			}
			before, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			requireNativeStatus(t, f.request(t, owner, http.MethodPost, f.base+"/work-items", request), http.StatusOK)
			after, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			for _, metric := range []string{"http_requests", "http_duration_microseconds", "http_response_bytes"} {
				delete(before.Usage, metric)
				delete(after.Usage, metric)
			}
			if !maps.Equal(before.Usage, after.Usage) {
				t.Fatalf("retry counted twice: %v -> %v", before.Usage, after.Usage)
			}
		})
	}
}

func TestHostedPlanConfigurationAndTelemetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*HostedPlansConfig)
	}{
		{"missing base", func(c *HostedPlansConfig) { c.Base.ID = "absent" }},
		{"negative allowance", func(c *HostedPlansConfig) { c.Plans[0].Allowances["projects"] = -1 }},
		{"unknown allowance", func(c *HostedPlansConfig) { c.Plans[0].Allowances["unlimited"] = 1 }},
		{"unbounded retention", func(c *HostedPlansConfig) { c.RetentionWindows = 721 }},
		{"unknown feature", func(c *HostedPlansConfig) { c.Plans[0].Features = []string{"admin_bypass"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := pilotHostedPlans()
			test.change(&c)
			if c.validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	f := newHostedSecurityFixture(t)
	config := hostedTestPlans(t, f.service, nil)
	config.Plans[0].Allowances["projects"]++
	cfg := *f.service.config.Hosted
	cfg.Plans = &config
	if err := f.service.database.configureHostedPlans(t.Context(), &cfg); err == nil {
		t.Fatal("mutated existing plan version")
	}
	now := time.Now().UTC()
	for i := range 30 {
		tx, err := f.service.database.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.service.database.recordHostedUsage(t.Context(), tx, now.Add(time.Duration(i)*time.Hour), "heartbeats", 1); err != nil {
			t.Fatal(errors.Join(err, tx.Rollback()))
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_usage_windows WHERE metric='heartbeats'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 24 {
		t.Fatalf("retained %d windows", count)
	}
}

func TestHostedPlansDisabledWithoutNetwork(t *testing.T) {
	t.Parallel()
	service := openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "local.db")})
	if service.database.hostedPlans != nil {
		t.Fatal("self-hosted plan restrictions enabled")
	}
	tx, err := service.database.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, check := range []func(context.Context) error{
		func(ctx context.Context) error { return service.database.checkHostedClaim(ctx, tx, time.Now()) },
		func(ctx context.Context) error {
			return service.database.checkHostedGrowth(ctx, tx, nil, time.Now(), false)
		},
		func(ctx context.Context) error {
			return service.database.requireHostedFeature(ctx, tx, "hosted_artifacts", time.Now())
		},
	} {
		if err := check(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := tx.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_plan_assignments").Scan(&count); err != nil || count != 0 {
		t.Fatalf("local assignments %d: %v", count, err)
	}
}

func TestHostedConcurrentProjectsAndInvitationSeats(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	hostedTestPlans(t, f.service, map[string]int64{"projects": 3, "members": 3})
	owner := f.user(t, "owner", "owner", "owner@example.test", "", "")
	for _, kind := range []string{"project", "invitation"} {
		t.Run(kind, func(t *testing.T) {
			start := make(chan struct{})
			results := make(chan int, 8)
			var group sync.WaitGroup
			for i := range 8 {
				group.Go(func() {
					<-start
					if kind == "invitation" {
						err := f.service.reserveHostedInvitation(t.Context(), fmt.Sprintf("pilot%d@example.test", i))
						if err == nil {
							results <- http.StatusOK
							return
						}
						var limit *hostedLimitError
						if !errors.As(err, &limit) {
							t.Errorf("invitation reservation: %v", err)
						}
						results <- http.StatusTooManyRequests
						return
					}
					response := f.request(t, owner, http.MethodPost, "/api/v2/organizations/org_security/projects", map[string]any{
						"idempotency_key": fmt.Sprintf("project%d", i), "name": fmt.Sprintf("project%d", i),
						"states": []tracker.NativeState{{Name: "Todo", Dispatchable: true}},
					})
					results <- response.Code
				})
			}
			close(start)
			group.Wait()
			close(results)
			winners := 0
			for status := range results {
				if status == http.StatusOK {
					winners++
				} else if status != http.StatusTooManyRequests {
					t.Errorf("allocation status = %d", status)
				}
			}
			if winners != 2 {
				t.Fatalf("%d allocations passed the remaining allowance of 2", winners)
			}
		})
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE hosted_member_reservations SET expires_at = ?", time.Now().Add(-time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.reserveHostedInvitation(t.Context(), "replacement@example.test"); err != nil {
		t.Fatal(err)
	}
	if err := f.service.releaseHostedInvitation(t.Context(), "replacement@example.test"); err != nil {
		t.Fatal(err)
	}
	usage, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
	if err != nil || usage.Usage["members"] != 1 {
		t.Fatalf("expired/failed reservations retained seats: %+v %v", usage.Usage, err)
	}
}

func TestHostedRunnerAdmission(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, metric string
		limit        int64
		want         int
	}{
		{"available registration", "registered_runners", 1, http.StatusCreated},
		{"registration exhausted", "registered_runners", 0, http.StatusTooManyRequests},
		{"connections exhausted", "connected_runners", 0, http.StatusTooManyRequests},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			hostedTestPlans(t, f.service, map[string]int64{test.metric: test.limit})
			owner := f.user(t, "owner", "owner", "owner@example.test", "", "")
			f.grant(t, owner, true, true)
			binding := runnerauth.NewBinding()
			path := "/api/v2/organizations/org_security/runner-enrollments"
			request := runnerauth.EnrollmentRequest{Binding: binding, ProjectIDs: []tracker.ProjectID{f.project}, Operations: []string{runnerauth.Read, runnerauth.Heartbeat}, TTLSeconds: 60}
			response := f.request(t, owner, http.MethodPost, path, request)
			requireNativeStatus(t, response, http.StatusCreated)
			var enrollment runnerauth.Enrollment
			decodeHubResponse(t, response, &enrollment)
			credential, err := apikey.GenerateToken()
			if err != nil {
				t.Fatal(err)
			}
			redemption := runnerauth.Redemption{Binding: binding, Credential: credential, Hostname: "pilot-host", DisplayName: "Pilot runner", Capacity: 1, Version: "test"}
			response = performHubAPIRequest(t, f.service, http.MethodPost, path+"/redeem", enrollment.Token, redemption)
			requireNativeStatus(t, response, test.want)
			var count int
			if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM runner_identities").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if test.want == http.StatusCreated && count != 1 || test.want == http.StatusTooManyRequests && count != 0 {
				t.Fatalf("registered runners = %d after status %d", count, test.want)
			}
			if test.want == http.StatusTooManyRequests && !strings.Contains(response.Body.String(), test.metric) {
				t.Fatalf("missing contextual limit: %s", response.Body.String())
			}
		})
	}
}

func TestHostedPlanPages(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	hostedTestPlans(t, f.service, map[string]int64{"projects": 0})
	owner := f.user(t, "owner", "owner", "owner@example.test", "", "")
	viewer := f.user(t, "viewer", "viewer", "viewer@example.test", "", "")
	for _, test := range []struct {
		name string
		user hostedSecurityUser
		want int
	}{
		{"owner sees downgrade", owner, http.StatusOK},
		{"viewer cannot inspect organization allowances", viewer, http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := f.request(t, test.user, http.MethodGet, "/organization/plan", nil)
			requireNativeStatus(t, response, test.want)
			if test.want == http.StatusOK {
				for _, expected := range []string{"Above allowance", "Maximum:", "separate from charges by your model providers", "Billing usage export"} {
					if !strings.Contains(response.Body.String(), expected) {
						t.Errorf("plan page omitted %q", expected)
					}
				}
			}
		})
	}
}
