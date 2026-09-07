package web_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/hubclient"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestChangeReviewWebAuthorizationAndBinding(t *testing.T) {
	t.Parallel()
	version := tracker.ChangeVersion{ID: "version_example", ChangeVersionInput: tracker.ChangeVersionInput{HeadSHA: strings.Repeat("a", 40), RunID: "run_example", AttemptID: "attempt_example", Code: tracker.ChangeArtifact{SHA256: strings.Repeat("b", 64)}}}
	ref := artifact.Reference{Scope: artifact.Scope{RunID: version.RunID, AttemptID: version.AttemptID, VersionID: version.ID}, ArtifactID: artifact.NewID("artifact"), Revision: 1, Kind: "diff", State: "complete", SHA256: version.Code.SHA256}
	var mu sync.Mutex
	upstreamStatus := 0
	lastToken := ""
	lastPath := ""
	lastBody := map[string]json.RawMessage{}
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		lastToken = r.Header.Get("Authorization")
		lastPath = r.URL.Path
		if upstreamStatus != 0 {
			w.WriteHeader(upstreamStatus)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"private upstream failure"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		var result any = map[string]string{"version_id": version.ID}
		switch {
		case strings.HasSuffix(r.URL.Path, "/changes/change_example"):
			result = tracker.ChangeDetail{Change: tracker.ChangeRequest{ID: "change_example", CurrentVersion: version.ID}, Versions: []tracker.ChangeVersion{version}}
		case strings.HasSuffix(r.URL.Path, "/artifacts"):
			result = []artifact.Reference{ref}
		case strings.HasSuffix(r.URL.Path, "/access"):
			result = artifact.Grant{ArtifactID: ref.ArtifactID, Revision: 1, SHA256: ref.SHA256, ExpiresAt: time.Now().Add(time.Minute)}
		case strings.HasSuffix(r.URL.Path, "/viewed-files") && r.Method == http.MethodGet:
			result = []tracker.ChangeViewedFile{}
		}
		if r.Method != http.MethodGet {
			lastBody = map[string]json.RawMessage{}
			if err := json.NewDecoder(r.Body).Decode(&lastBody); err != nil {
				t.Error(err)
			}
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer hub.Close()
	client, err := hubclient.New(hubclient.Config{URL: hub.URL, TokenSource: func() string { return "configured-operator" }, HTTPClient: hub.Client()})
	if err != nil {
		t.Fatal(err)
	}
	native, err := client.Native("org_example", "prj_example")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &nativeWebFixture{}
	server := nativeWebServerWithClient(t, native, fixture)
	path := "/projects/native/issues/wi_example/changes/change_example/versions/" + version.ID + "/review"
	for _, tt := range []struct {
		name, action, member string
		upstream, want       int
		edit                 func(url.Values)
	}{
		{"load", "load", "member", 0, 200, nil},
		{"approve", "approved", "member", 0, 200, nil},
		{"request changes", "changes_requested", "member", 0, 200, nil},
		{"viewed", "viewed", "member", 0, 200, nil},
		{"discussion", "discuss", "member", 0, 200, nil},
		{"missing member", "approved", "", 0, 403, nil},
		{"invalid revision", "load", "member", 0, 422, func(v url.Values) { v.Set("revision", "bad") }},
		{"wrong bundle", "load", "member", 0, 409, func(v url.Values) { v.Set("sha256", strings.Repeat("c", 64)) }},
		{"unknown action", "merge", "member", 0, 422, nil},
		{"stale action", "approved", "member", 409, 409, nil},
		{"revoked", "approved", "member", 403, 403, nil},
		{"missing scope", "load", "member", 404, 403, nil},
		{"expired", "approved", "member", 410, 410, nil},
		{"invalid", "approved", "member", 422, 422, nil},
		{"offline", "approved", "member", 503, 502, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mu.Lock()
			upstreamStatus = tt.upstream
			lastToken = ""
			lastPath = ""
			mu.Unlock()
			form := url.Values{"action": {tt.action}, "member_token": {tt.member}, "key": {tt.name}, "revision": {"1"}, "artifact_id": {ref.ArtifactID}, "sha256": {ref.SHA256}, "head_sha": {version.HeadSHA}, "body": {"Review message"}, "file_sha256": {strings.Repeat("d", 64)}, "viewed": {"true"}}
			if tt.edit != nil {
				tt.edit(form)
			}
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
			req.Header.Set("Authorization", "Bearer web-secret")
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			res := httptest.NewRecorder()
			server.Handler().ServeHTTP(res, req)
			if res.Code != tt.want {
				t.Fatalf("status %d want %d: %s", res.Code, tt.want, res.Body)
			}
			if strings.Contains(res.Body.String(), "private upstream failure") {
				t.Fatal("upstream details leaked")
			}
			mu.Lock()
			defer mu.Unlock()
			if tt.want == 200 && lastToken != "Bearer member" {
				t.Fatalf("review used wrong identity: %q", lastToken)
			}
			if tt.action == "approved" && tt.want == 200 {
				if !strings.HasSuffix(lastPath, "/versions/"+version.ID+"/reviews") || string(lastBody["expected_version_id"]) != `"`+version.ID+`"` || len(lastBody["bundle"]) == 0 {
					t.Fatal("review lost immutable binding")
				}
			}
		})
	}
	for _, tt := range []struct{ name, token string }{{"missing", ""}, {"read-only", fixture.keys["readnative"]}, {"wrong project", fixture.keys["writeother"]}} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("action=approved&member_token=member"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if tt.token != "" {
			req.Header.Set("Authorization", "Bearer "+tt.token)
		}
		res := httptest.NewRecorder()
		server.Handler().ServeHTTP(res, req)
		if res.Code != http.StatusForbidden {
			t.Fatalf("%s form boundary status %d, want forbidden", tt.name, res.Code)
		}
	}
}
