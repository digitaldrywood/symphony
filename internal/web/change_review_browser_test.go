package web_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

func reviewBrowserPatch() string {
	return "diff --git a/main.go b/main.go\nindex 1111111..2222222 100644\n--- a/main.go\n+++ b/main.go\n@@ -1,2 +1,3 @@\n package main\n-func old() {}\n+func current() {}\n+var text = `<img src=x onerror=window.reviewExecuted=true>`\n" +
		"diff --git a/old.go b/renamed.go\nsimilarity index 100%\nrename from old.go\nrename to renamed.go\n" +
		"diff --git a/image.png b/image.png\nindex 1111111..2222222 100644\nBinary files a/image.png and b/image.png differ\n" +
		"diff --git a/long.go b/long.go\n--- a/long.go\n+++ b/long.go\n@@ -1 +1 @@\n-old\n+" + strings.Repeat("long line ", 400) + "\n" +
		"diff --git a/huge.go b/huge.go\n--- a/huge.go\n+++ b/huge.go\n@@ -0,0 +1,5000 @@\n" + strings.Repeat("+var large = 12345678901234567890123456789012345678901234567890\n", 5000)
}

func TestReviewBrowserFixture(t *testing.T) {
	if os.Getenv("DETENT_REVIEW_BROWSER") != "1" {
		t.Skip("browser fixture is opt-in")
	}
	stop := make(chan struct{})
	var once sync.Once
	mux := http.NewServeMux()
	ui := httptest.NewServer(mux)
	defer ui.Close()
	cfg := artifact.Config{ServiceID: artifact.NewID("service"), OrganizationID: artifact.NewID("org"), Mode: "customer", DatabasePath: filepath.Join(t.TempDir(), "artifacts.db"), AllowedOrigins: []string{ui.URL}, Policy: artifact.Policy{ID: "browser", Limits: artifact.Limits{RetainedBytes: 16 << 20, ReservedBytes: 16 << 20, ArtifactBytes: 4 << 20, UploadBytes: 1 << 20, RetentionSeconds: 3600}, AbandonedUploadSeconds: 600, DeletionRecordSeconds: 7200, BackupSeconds: 3600}}
	storage := &browserArtifactStorage{objects: map[string][]byte{}}
	service, err := artifact.NewService(t.Context(), cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	authority := &browserArtifactAuthority{}
	handler, err := artifact.NewHTTPServer(service, authority)
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	defer gateway.Close()
	scope := artifact.Scope{OrganizationID: cfg.OrganizationID, ProjectID: artifact.NewID("prj"), WorkItemID: artifact.NewID("wi"), RunID: artifact.NewID("run"), AttemptID: artifact.NewID("attempt")}
	change := tracker.ChangeRequest{ID: artifact.NewID("change"), OrganizationID: tracker.OrganizationID(scope.OrganizationID), ProjectID: tracker.ProjectID(scope.ProjectID), WorkItemID: tracker.NativeWorkItemID(scope.WorkItemID), Title: "Review customer-hosted changes", Body: "Verified immutable changes remain readable after execution runners stop."}
	versions := []tracker.ChangeVersion{}
	refs := []artifact.Reference{}
	for index := range 2 {
		version := tracker.ChangeVersion{ID: artifact.NewID("version"), ChangeID: change.ID, Number: int64(index + 1), ChangeVersionInput: tracker.ChangeVersionInput{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat(string(rune('b'+index)), 40), MergeBaseSHA: strings.Repeat("a", 40), RunID: scope.RunID, AttemptID: scope.AttemptID}, CreatedAt: time.Now()}
		scope.VersionID = version.ID
		upload, err := service.Reserve(t.Context(), artifact.Reservation{ServiceID: cfg.ServiceID, Mode: cfg.Mode, Scope: scope, Key: version.ID, Kind: "diff", Bytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		patch := []byte(reviewBrowserPatch())
		if _, err := service.Append(t.Context(), upload.ArtifactID, artifact.Part{Sequence: 0, Side: "diff", MediaType: "text/x-diff; charset=utf-8", Data: patch, SHA256: artifact.Digest(patch)}); err != nil {
			t.Fatal(err)
		}
		ref, err := service.Finalize(t.Context(), upload.ArtifactID, "complete", &artifact.Capture{Base: version.BaseSHA, Head: version.HeadSHA, MergeBase: version.MergeBaseSHA, ContextLines: 3, FileContext: "changed_files"})
		if err != nil {
			t.Fatal(err)
		}
		version.Code = tracker.ChangeArtifact{Kind: "code", SHA256: ref.SHA256, Availability: "available"}
		versions = append(versions, version)
		refs = append(refs, ref)
	}
	var mu sync.Mutex
	current := 0
	approved := ""
	viewed := map[string]map[string]bool{}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(root, "static")))))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		index := current
		if r.URL.Query().Get("version") == versions[0].ID {
			index = 0
		}
		change.CurrentVersion = versions[current].ID
		summary := tracker.ChangeSummary{NativeReview: "pending", Checks: "missing", ExternalReview: "external_gate", Status: "needs_evidence"}
		if approved != "" {
			if approved == change.CurrentVersion {
				summary.NativeReview = "approved"
			} else {
				summary.NativeReview = "stale"
			}
		}
		detail := tracker.ChangeDetail{Change: change, Versions: versions[:current+1], Summary: summary}
		data := templates.ChangePageData{ProjectID: "fixture", IssueID: change.WorkItemID, ChangeID: change.ID, VersionID: versions[index].ID, Detail: &detail, Artifacts: []artifact.Reference{refs[index]}}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if r.URL.Query().Get("content") == "1" {
			err = templates.ChangeContent(data).Render(r.Context(), w)
		} else {
			err = templates.ChangePage(data).Render(r.Context(), w)
		}
		if err != nil && r.Context().Err() == nil {
			t.Error(err)
		}
	})
	mux.HandleFunc("POST /projects/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if authority.revoked.Load() || r.FormValue("member_token") != "fixture-member" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		index := 0
		if strings.Contains(r.URL.Path, versions[1].ID) {
			index = 1
		}
		version := versions[index]
		ref := refs[index]
		w.Header().Set("Content-Type", "application/json")
		action := r.FormValue("action")
		if action == "load" {
			files := []tracker.ChangeViewedFile{}
			for key, value := range viewed[version.ID] {
				files = append(files, tracker.ChangeViewedFile{ManifestSHA256: ref.SHA256, FileSHA256: key, Viewed: value})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"grant": artifact.Grant{Token: "fixture-grant", Origin: gateway.URL, ArtifactID: ref.ArtifactID, Revision: ref.Revision, SHA256: ref.SHA256, ExpiresAt: time.Now().Add(time.Minute)}, "viewed": files, "current_version_id": versions[current].ID})
			return
		}
		if action == "viewed" {
			if viewed[version.ID] == nil {
				viewed[version.ID] = map[string]bool{}
			}
			viewed[version.ID][r.FormValue("file_sha256")] = r.FormValue("viewed") == "true"
		}
		if action == "approved" || action == "changes_requested" {
			if index != current {
				http.Error(w, "stale", http.StatusConflict)
				return
			}
			approved = version.ID
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"version_id": version.ID})
	})
	mux.HandleFunc("POST /fixture/update", func(w http.ResponseWriter, _ *http.Request) { mu.Lock(); current = 1; mu.Unlock(); w.WriteHeader(204) })
	mux.HandleFunc("POST /fixture/reset", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		current = 0
		approved = ""
		viewed = map[string]map[string]bool{}
		mu.Unlock()
		authority.revoked.Store(false)
		w.WriteHeader(204)
	})
	mux.HandleFunc("POST /fixture/revoke", func(w http.ResponseWriter, _ *http.Request) { authority.revoked.Store(true); w.WriteHeader(204) })
	mux.HandleFunc("POST /fixture/stop", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204); once.Do(func() { close(stop) }) })
	fmt.Println("REVIEW_BROWSER_URL=" + ui.URL)
	select {
	case <-stop:
	case <-time.After(30 * time.Minute):
		t.Fatal("browser fixture timed out")
	}
}
