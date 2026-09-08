package hubserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/onboarding"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestOnboardingProgressRetryAndProjectIsolation(t *testing.T) {
	t.Parallel()
	f := newNativeFixture(t, nil, "", "setup")
	other := newNativeFixture(t, f.service, f.project.OrganizationID, "other")
	request := map[string]any{"idempotency_key": "setup-1", "progress": onboarding.Progress{Repository: "existing", Doctor: true, Artifacts: "local"}}
	for _, tt := range []struct {
		name, path, token string
		body              any
		status            int
	}{
		{"save", f.base, f.token, request, http.StatusOK},
		{"retry", f.base, f.token, request, http.StatusOK},
		{"stale", f.base, f.token, map[string]any{"idempotency_key": "setup-2", "progress": onboarding.Progress{Repository: "generate"}}, http.StatusConflict},
		{"other project", other.base, f.token, request, http.StatusNotFound},
		{"unknown input", f.base, f.token, map[string]any{"idempotency_key": "setup-secret", "progress": map[string]any{"repository": "workflow source"}}, http.StatusUnprocessableEntity},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := performHubAPIRequest(t, f.service, http.MethodPut, tt.path+"/onboarding", tt.token, tt.body)
			requireNativeStatus(t, response, tt.status)
		})
	}
	cfg := f.service.config
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = openTestService(t, cfg)
	response := performHubAPIRequest(t, f.service, http.MethodGet, f.base+"/onboarding", f.token, nil)
	requireNativeStatus(t, response, http.StatusOK)
	var setup onboarding.Project
	decodeHubResponse(t, response, &setup)
	if setup.Progress.Revision != 1 || !setup.Progress.Doctor || setup.Ready {
		t.Fatalf("saved state = %+v", setup)
	}
	response = performHubAPIRequest(t, f.service, http.MethodGet, other.base+"/onboarding", other.token, nil)
	requireNativeStatus(t, response, http.StatusOK)
	decodeHubResponse(t, response, &setup)
	if setup.Progress.Revision != 0 {
		t.Fatal("setup leaked to another project")
	}
}

func TestOnboardingRepositoryPoliciesAndRunners(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, kind  string
		auto, match bool
	}{
		{"human review", "human_review", false, true},
		{"auto merge missing tag", "command", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newNativeFixture(t, nil, "", tt.name)
			r := prepareRunner(t, f, runnerauth.Read, runnerauth.Claim, runnerauth.Heartbeat)
			r.enroll(t)
			descriptor := hubTestPolicy()
			descriptor.Gates.Kind, descriptor.Gates.AutoPromote = tt.kind, tt.auto
			if tt.kind == "human_review" {
				descriptor.Gates.AutomatedReview = ""
			}
			if !tt.match {
				descriptor.Requirements.RequiredTags = []string{"gpu"}
			}
			descriptor = descriptor.WithID()
			approveHubTestPolicy(t, f.service, f.base+"/policy", descriptor)
			response := performHubAPIRequest(t, f.service, http.MethodGet, f.base+"/onboarding", f.token, nil)
			requireNativeStatus(t, response, http.StatusOK)
			var setup onboarding.Project
			decodeHubResponse(t, response, &setup)
			if setup.Policy == nil || setup.Policy.Policy.ID != descriptor.ID || len(setup.Runners) != 1 {
				t.Fatalf("readiness = %+v", setup)
			}
			if (len(setup.Runners[0].Exclusions) == 0) != tt.match {
				t.Fatalf("exclusions = %+v", setup.Runners[0].Exclusions)
			}
			if strings.Contains(response.Body.String(), r.redemption.Credential) || len(setup.Runners[0].Runner.Leases) != 0 {
				t.Fatal("private runner data exposed")
			}
		})
	}
}

func TestHostedOnboardingPermissionsAndArtifacts(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, role, grant  string
		read, write, admin int
	}{
		{"owner", "owner", "write", 200, 200, 200},
		{"member", "member", "write", 200, 200, 404},
		{"viewer", "viewer", "read", 200, 404, 404},
		{"ungranted owner", "owner", "", 404, 404, 404},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newHostedSecurityFixture(t)
			ref, _, _ := seedHostedArtifact(t, f)
			user := f.user(t, "setup-user", tt.role, "setup@example.test", tt.grant, "")
			response := f.request(t, user, http.MethodGet, f.base+"/onboarding", nil)
			requireNativeStatus(t, response, tt.read)
			if tt.read == 200 {
				var setup onboarding.Project
				decodeHubResponse(t, response, &setup)
				if len(setup.Artifacts) != 1 || setup.Artifacts[0].ServiceID != ref.ServiceID || setup.Artifacts[0].PublisherTokenID != "" {
					t.Fatalf("artifact binding = %+v", setup.Artifacts)
				}
			}
			requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/onboarding", map[string]any{"idempotency_key": "save", "progress": onboarding.Progress{Repository: "existing"}}), tt.write)
			descriptor := hubTestPolicy()
			requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/onboarding/policy", policy.Change{Policy: descriptor}), tt.admin)
		})
	}
}

func (f *browserHostedFixture) setupRequest(t *testing.T, account, method, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(method, f.server.URL+path, strings.NewReader(string(raw)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", f.server.URL)
	if cookie := f.cookies[account]; cookie != nil {
		request.AddCookie(cookie)
		request.Header.Set("X-CSRF-Token", hostedCSRF(cookie.Value))
	}
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	return response
}

func TestHostedProjectSetupJourney(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	first := f.createProject(t, "Resumable project")
	if again := f.createProject(t, "Resumable project"); again != first {
		t.Fatal("project retry created duplicate")
	}
	base := "/api/v2/organizations/org_browser_preview/projects/" + first
	request := map[string]any{"idempotency_key": "progress", "progress": onboarding.Progress{Repository: "existing", Doctor: true, Provider: true, Artifacts: "local"}}
	for range 2 {
		requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding", request), http.StatusOK)
	}
	response := f.page(t, "owner", "/projects/"+first)
	requireNativeStatus(t, response, http.StatusOK)
	for _, text := range []string{"Project setup", "existing", "No matching runner", "Missing artifact gateway", "Create your first native issue", "Repository review and merge policy"} {
		if !strings.Contains(response.Body.String(), text) {
			t.Errorf("missing %q", text)
		}
	}
	issue := tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "first-issue"}, Title: "First native issue", Body: "No GitHub issue required", State: "Todo"}
	for range 2 {
		requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", issue), http.StatusOK)
	}
	var count int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM issues WHERE project_id=?", first).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("issues=%d", count)
	}
	requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {first}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
}

func TestOnboardingCustomerBindingValidation(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	ref, _, _ := seedHostedArtifact(t, f)
	user := f.user(t, "setup-owner", "owner", "setup@example.test", "write", "")
	for _, tt := range []struct {
		name, mode, origin string
		want               int
	}{
		{"customer", "customer", "https://artifacts.example.test", 200},
		{"hosted without opt in", "hosted", "https://artifacts.example.test", 422},
		{"unsafe origin", "customer", "https://user:secret@example.test", 422},
	} {
		t.Run(tt.name, func(t *testing.T) {
			binding := artifact.Binding{ServiceID: ref.ServiceID, Origin: tt.origin, Mode: tt.mode, PublisherTokenID: "artifact-publisher"}
			requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/onboarding/artifact-services/"+ref.ServiceID, binding), tt.want)
		})
	}
}

func seedOnboardingBrowserJourney(t *testing.T) *browserHostedFixture {
	t.Helper()
	f := newBrowserHostedFixture(t, true)
	return seedHostedOnboardingJourney(t, f)
}

func seedHostedOnboardingJourney(t *testing.T, f *browserHostedFixture) *browserHostedFixture {
	t.Helper()
	organization := "/api/v2/organizations/" + f.service.config.Hosted.OrganizationID
	for _, project := range []string{f.project, f.privateProject} {
		requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {project}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
	}
	base := organization + "/projects/" + f.project
	descriptor := hubTestPolicy()
	descriptor.Gates.Kind, descriptor.Gates.AutomatedReview = "human_review", ""
	descriptor = descriptor.WithID()
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{Policy: descriptor}), http.StatusOK)
	automatic := hubTestPolicy()
	automatic.Gates.AutoPromote = true
	automatic.Requirements.RequiredTags = []string{"gpu"}
	automatic = automatic.WithID()
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, organization+"/projects/"+f.privateProject+"/onboarding/policy", policy.Change{Policy: automatic}), http.StatusOK)
	binding := runnerauth.NewBinding()
	enrollmentRequest := runnerauth.EnrollmentRequest{Binding: binding, ProjectIDs: []tracker.ProjectID{tracker.ProjectID(f.project), tracker.ProjectID(f.privateProject)}, Operations: []string{runnerauth.Read, runnerauth.Collaborate, runnerauth.Claim, runnerauth.Heartbeat, runnerauth.Events}, TTLSeconds: 900}
	response := f.setupRequest(t, "owner", http.MethodPost, organization+"/runner-enrollments", enrollmentRequest)
	requireNativeStatus(t, response, http.StatusCreated)
	var enrollment runnerauth.Enrollment
	decodeHubResponse(t, response, &enrollment)
	credential, err := apikey.GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	redemption := runnerauth.Redemption{Binding: binding, Credential: credential, Hostname: "customer-build-host", DisplayName: "Customer build runner", Capacity: 2, Version: "test", OS: "linux", Architecture: "amd64"}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, organization+"/runner-enrollments/redeem", enrollment.Token, redemption), http.StatusCreated)
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding", map[string]any{"idempotency_key": "ready", "progress": onboarding.Progress{Repository: "existing", Doctor: true, Provider: true, Artifacts: "local"}}), http.StatusOK)
	response = f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "first-run"}, Title: "First native run", State: "Todo"})
	requireNativeStatus(t, response, http.StatusOK)
	var issue tracker.NativeIssue
	decodeHubResponse(t, response, &issue)
	response = performHubAPIRequest(t, f.service, http.MethodPost, base+"/claims", credential, tracker.NativeClaim{PolicyID: descriptor.ID, WorkItemID: issue.WorkItemID, MachineID: binding.MachineID, SessionID: "first-run", TTLSeconds: 90, ProtocolMajor: 2, Capabilities: []string{"native_issues", "scoped_collaboration", tracker.NativeExecutionCapability}})
	requireNativeStatus(t, response, http.StatusOK)
	var lease tracker.NativeLease
	decodeHubResponse(t, response, &lease)
	start := nativeStartedEvent(lease)
	finish := start
	finish.Type, finish.IdempotencyKey, finish.Data.Sequence, finish.Data.Outcome = "run.finished", "finish", 2, "succeeded"
	for _, event := range []tracker.NativeRunEvent{start, finish} {
		requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/work-items/"+string(issue.WorkItemID)+"/events", credential, event), http.StatusOK)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/leases/"+string(lease.ID)+"/release", credential, tracker.NativeLeaseMutation{FencingToken: lease.FencingToken, Reason: "completed"}), http.StatusNoContent)
	return f
}

func TestHostedOnboardingFirstRun(t *testing.T) {
	t.Parallel()
	f := seedOnboardingBrowserJourney(t)
	for _, tt := range []struct{ project, contains string }{
		{f.project, "Latest execution: succeeded"},
		{f.privateProject, "gpu"},
	} {
		t.Run(tt.project, func(t *testing.T) {
			response := f.page(t, "owner", "/projects/"+tt.project)
			requireNativeStatus(t, response, http.StatusOK)
			if !strings.Contains(response.Body.String(), tt.contains) {
				t.Fatalf("missing %q", tt.contains)
			}
		})
	}
}

func TestOnboardingBrowserPreview(t *testing.T) {
	if os.Getenv("DETENT_ONBOARDING_BROWSER_PREVIEW") == "" {
		t.Skip("isolated onboarding browser preview")
	}
	f := seedOnboardingBrowserJourney(t)
	interrupted := f.createProject(t, "Interrupted setup")
	requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {interrupted}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
	info := map[string]string{"owner": f.server.URL + "/__preview/account/owner", "success": f.server.URL + "/projects/" + f.project, "auto_merge": f.server.URL + "/projects/" + f.privateProject, "interrupted": f.server.URL + "/projects/" + interrupted, "stop": f.server.URL + "/__preview/stop"}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(os.TempDir(), "2194-browser.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("Onboarding browser fixture: %s", f.server.URL)
	timer := time.NewTimer(15 * time.Minute)
	defer timer.Stop()
	select {
	case <-f.stop:
	case <-timer.C:
	case <-t.Context().Done():
	}
}
