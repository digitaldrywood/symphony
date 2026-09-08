package hubserver

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/tracker"
)

func pilotHostedJourney(t *testing.T) (*browserHostedFixture, string, string) {
	t.Helper()
	f := seedOnboardingBrowserJourney(t)
	pilotHostedRunner(t, f, 2)
	base := "/api/v2/organizations/org_browser_preview/projects/" + f.project
	response := f.page(t, "owner", base+"/work-items")
	requireNativeStatus(t, response, http.StatusOK)
	var issues tracker.Page[tracker.NativeIssue]
	decodeHubResponse(t, response, &issues)
	var issue tracker.NativeIssue
	for _, item := range issues.Items {
		if item.Title == "First native run" {
			issue = item
		}
	}
	if issue.WorkItemID == "" {
		t.Fatal("completed onboarding run is missing")
	}
	path := base + "/work-items/" + string(issue.WorkItemID)
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path+"/comments", tracker.CreateComment{Mutation: tracker.Mutation{IdempotencyKey: "pilot-discussion"}, Body: "Pilot discussion remains available with runners offline."}), http.StatusOK)
	response = f.setupRequest(t, "owner", http.MethodPost, path+"/changes", tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "pilot-change"}, Title: "Pilot native Change Request", Body: "Synthetic pilot work; no customer repository was cloned."})
	requireNativeStatus(t, response, http.StatusOK)
	var change tracker.ChangeRequest
	decodeHubResponse(t, response, &change)
	return f, path, path + "/changes/" + change.ID
}

func TestPilotHostedHistoryAfterRunnerLossAndRestart(t *testing.T) {
	t.Parallel()
	f, issue, change := pilotHostedJourney(t)
	for _, query := range []string{"UPDATE runner_identities SET last_heartbeat_at = '2000-01-01T00:00:00Z'", "UPDATE machines SET last_heartbeat_at = '2000-01-01T00:00:00Z'"} {
		if _, err := f.service.database.db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	for _, restart := range []bool{false, true} {
		if restart {
			config := f.service.config
			if err := f.service.Close(); err != nil {
				t.Fatal(err)
			}
			service, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			f.service = service
			t.Cleanup(func() {
				if err := service.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		for _, test := range []struct {
			path     string
			contains string
		}{
			{issue, "First native run"},
			{issue + "/comments", "Pilot discussion remains available"},
			{issue + "/history", "run.finished"},
			{change, "Pilot native Change Request"},
			{"/projects/" + f.project, "Latest execution: succeeded"},
		} {
			response := f.page(t, "owner", test.path)
			requireNativeStatus(t, response, http.StatusOK)
			if !strings.Contains(response.Body.String(), test.contains) {
				t.Fatalf("restart=%t history %s missing %q", restart, test.path, test.contains)
			}
			requireNativeStatus(t, f.page(t, "wrong-organization", test.path), http.StatusForbidden)
			requireNativeStatus(t, f.page(t, "revoked", test.path), http.StatusUnauthorized)
		}
		var detail tracker.ChangeDetail
		decodeHubResponse(t, f.page(t, "owner", change), &detail)
		if detail.Summary.Status != "draft" || len(detail.Versions) != 0 {
			t.Fatalf("unpublished change is falsely ready: %+v", detail.Summary)
		}
	}
}

func TestPilotBrowserPreview(t *testing.T) {
	manifest := os.Getenv("DETENT_PILOT_BROWSER_MANIFEST")
	if manifest == "" {
		t.Skip("isolated pilot browser preview")
	}
	f, issue, change := pilotHostedJourney(t)
	info := map[string]string{"login": f.server.URL + "/login", "owner": f.server.URL + "/__preview/account/owner", "organization": f.server.URL + "/organization", "human_review": f.server.URL + "/projects/" + f.project, "automatic": f.server.URL + "/projects/" + f.privateProject, "issue": f.server.URL + issue, "change": f.server.URL + change, "usage": f.server.URL + "/organization/plan", "stop": f.server.URL + "/__preview/stop"}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(manifest) {
		t.Fatal("browser manifest requires an absolute path")
	}
	if err := os.WriteFile(manifest, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("Pilot browser fixture: %s", f.server.URL)
	timer := time.NewTimer(15 * time.Minute)
	defer timer.Stop()
	select {
	case <-f.stop:
	case <-timer.C:
		t.Fatal("pilot browser fixture was not stopped")
	case <-t.Context().Done():
	}
}
