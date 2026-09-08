package hubserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/tracker"
)

func hostedWorkBrowserData(t *testing.T, f *browserHostedFixture) {
	t.Helper()
	var item string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT native_id FROM issues WHERE project_id = ?", f.project).Scan(&item); err != nil {
		t.Fatal(err)
	}
	path := "/api/v2/organizations/org_browser_preview/projects/" + f.project + "/work-items/" + item
	for i := range 27 {
		response := hostedWorkBrowserMutation(t, f, path+"/comments", tracker.CreateComment{Mutation: tracker.Mutation{IdempotencyKey: fmt.Sprintf("seed-%d", i)}, Body: fmt.Sprintf("Stored discussion %02d", i)})
		requireNativeStatus(t, response, http.StatusOK)
	}
	response := hostedWorkBrowserMutation(t, f, path+"/changes", tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "seed-change"}, Title: "Stored Change", Body: "Change remains readable offline"})
	requireNativeStatus(t, response, http.StatusOK)
	now := formatHubTime(time.Now().Add(-time.Hour))
	for _, statement := range []string{
		"INSERT INTO machines(id,organization_id,hostname,capacity,version,last_heartbeat_at,registered_at,updated_at) VALUES ('offline-machine','org_browser_preview','offline',1,'fixture',?,?,?)",
		"INSERT INTO leases(lease_id,issue_id,machine_id,session_id,expires_at,acquired_at,renewed_at,released_at,created_at,updated_at) SELECT 'offline-lease',id,'offline-machine','offline-session',?,?,?,? ,?,? FROM issues WHERE native_id = ?",
		"INSERT INTO native_attempts(id,organization_id,project_id,work_item_id,lease_id,fencing_token,run_id,sequence,status,data_json,started_at,updated_at) SELECT 'offline-attempt',organization_id,project_id,native_id,'offline-lease',1,'offline-run',1,'succeeded',?, ?,? FROM issues WHERE native_id = ?",
	} {
		var args []any
		switch {
		case strings.Contains(statement, "INSERT INTO machines"):
			args = []any{now, now, now}
		case strings.Contains(statement, "INSERT INTO leases"):
			args = []any{now, now, now, now, now, now, item}
		default:
			raw, err := json.Marshal(tracker.NativeRunData{RunID: "offline-run", AttemptID: "offline-attempt", FencingToken: 1, LeaseID: "offline-lease", Outcome: "succeeded"})
			if err != nil {
				t.Fatal(err)
			}
			args = []any{string(raw), now, now, item}
		}
		if _, err := f.service.database.db.ExecContext(t.Context(), statement, args...); err != nil {
			t.Fatal(err)
		}
	}
}

func hostedWorkBrowserMutation(t *testing.T, f *browserHostedFixture, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, f.server.URL+path, strings.NewReader(string(raw)))
	cookie := f.cookies["owner"]
	if cookie == nil {
		t.Fatal("owner session cookie is missing")
	}
	request.AddCookie(cookie)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-CSRF-Token", hostedCSRF(cookie.Value))
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	return response
}

func TestHostedWorkBrowserFixtureData(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	hostedWorkBrowserData(t, f)
	var item string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT native_id FROM issues WHERE project_id = ?", f.project).Scan(&item); err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"comments", "history", "attempts", "changes"} {
		t.Run(resource, func(t *testing.T) {
			response := f.page(t, "viewer", "/api/v2/organizations/org_browser_preview/projects/"+f.project+"/work-items/"+item+"/"+resource)
			requireNativeStatus(t, response, http.StatusOK)
			if strings.TrimSpace(response.Body.String()) == "[]" {
				t.Error("missing stored fixture data")
			}
		})
	}
}

func TestHostedWorkBrowserPreview(t *testing.T) {
	if os.Getenv("DETENT_HOSTED_WORK_BROWSER") == "" {
		t.Skip("set DETENT_HOSTED_WORK_BROWSER=1 for the isolated hosted work browser fixture")
	}
	f := newBrowserHostedFixture(t, true)
	hostedWorkBrowserData(t, f)
	t.Logf("HOSTED_WORK_BROWSER_URL=%s", f.server.URL)
	timer := time.NewTimer(10 * time.Minute)
	defer timer.Stop()
	select {
	case <-f.stop:
	case <-timer.C:
	case <-t.Context().Done():
	}
}
