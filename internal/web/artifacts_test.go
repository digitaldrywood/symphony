package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

type browserArtifactStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (s *browserArtifactStorage) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[key]; exists {
		return "", artifact.ErrConflict
	}
	s.objects[key] = bytes.Clone(data)
	return "", nil
}
func (s *browserArtifactStorage) Get(_ context.Context, key, _ string, limit int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, exists := s.objects[key]
	if !exists {
		return nil, artifact.ErrMissing
	}
	if int64(len(data)) > limit {
		return nil, artifact.ErrIntegrity
	}
	return bytes.Clone(data), nil
}
func (s *browserArtifactStorage) Delete(_ context.Context, key, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

type browserArtifactAuthority struct{ revoked atomic.Bool }

func (*browserArtifactAuthority) Upload(context.Context, string, artifact.Reservation) error {
	return artifact.ErrDenied
}
func (a *browserArtifactAuthority) Read(_ context.Context, r artifact.ReadAuthorization) error {
	if a.revoked.Load() || r.Token != "fixture-grant" {
		return artifact.ErrDenied
	}
	return nil
}

func TestArtifactTemplateStates(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"available", "partial", "expired", "inaccessible", "local"} {
		t.Run(state, func(t *testing.T) {
			data := templates.ChangePageData{}
			if state == "inaccessible" {
				data.ArtifactError = "Artifact availability could not be loaded."
			} else if state != "local" {
				data.Artifacts = []artifact.Reference{{Kind: "log", State: state, Availability: state, ArtifactID: "<script>alert(1)</script>"}}
			}
			var out bytes.Buffer
			if err := templates.ArtifactAccess(data).Render(t.Context(), &out); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out.String(), "<script>alert") {
				t.Fatal("unsafe artifact text")
			}
			if state == "expired" && !strings.Contains(out.String(), "disabled") {
				t.Fatal("expired read enabled")
			}
		})
	}
}

func TestArtifactBrowserFixture(t *testing.T) {
	if os.Getenv("DETENT_ARTIFACT_BROWSER") != "1" {
		t.Skip("browser fixture is opt-in")
	}
	stop := make(chan struct{})
	var once sync.Once
	mux := http.NewServeMux()
	ui := httptest.NewServer(mux)
	defer ui.Close()
	cfg := artifact.Config{ServiceID: artifact.NewID("service"), OrganizationID: artifact.NewID("org"), Mode: "customer", DatabasePath: filepath.Join(t.TempDir(), "catalog.db"), AllowedOrigins: []string{ui.URL}, Policy: artifact.Policy{ID: "browser", Limits: artifact.Limits{RetainedBytes: 8 << 20, ReservedBytes: 8 << 20, ArtifactBytes: 4 << 20, UploadBytes: 1 << 20, RetentionSeconds: 3600}, AbandonedUploadSeconds: 600, DeletionRecordSeconds: 7200, BackupSeconds: 3600}}
	storage := &browserArtifactStorage{objects: map[string][]byte{}}
	service, err := artifact.NewService(t.Context(), cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	scope := artifact.Scope{OrganizationID: cfg.OrganizationID, ProjectID: artifact.NewID("prj"), WorkItemID: artifact.NewID("wi"), RunID: artifact.NewID("run"), AttemptID: artifact.NewID("attempt")}
	upload, err := service.Reserve(t.Context(), artifact.Reservation{ServiceID: cfg.ServiceID, Mode: cfg.Mode, Scope: scope, Key: "browser", Kind: "log", Bytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for sequence := range 2 {
		data := []byte("<script>window.artifactExecuted=true</script>\n" + strings.Repeat("large log line\n", 30000))
		if _, err := service.Append(t.Context(), upload.ArtifactID, artifact.Part{Sequence: sequence, MediaType: "text/plain; charset=utf-8", SHA256: artifact.Digest(data), Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := service.Finalize(t.Context(), upload.ArtifactID, "complete", nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := &browserArtifactAuthority{}
	handler, err := artifact.NewHTTPServer(service, authority)
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	defer gateway.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(root, "static")))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<!doctype html><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="stylesheet" href="/static/css/output.css"><main class="p-4 min-w-0 max-w-3xl">`)
		if err := templates.ArtifactAccess(templates.ChangePageData{ProjectID: "fixture", IssueID: tracker.NativeWorkItemID(scope.WorkItemID), Artifacts: []artifact.Reference{ref}}).Render(r.Context(), w); err != nil {
			t.Error(err)
		}
		_, _ = fmt.Fprint(w, `</main>`)
	})
	mux.HandleFunc("POST /projects/", func(w http.ResponseWriter, r *http.Request) {
		if authority.revoked.Load() || r.FormValue("member_token") != "fixture-member" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		revision, err := strconv.ParseInt(r.FormValue("revision"), 10, 64)
		if err != nil {
			http.Error(w, "invalid", http.StatusUnprocessableEntity)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(artifact.Grant{Token: "fixture-grant", Origin: gateway.URL, ArtifactID: ref.ArtifactID, Revision: revision, SHA256: ref.SHA256, ExpiresAt: time.Now().Add(time.Minute)})
	})
	mux.HandleFunc("POST /fixture/revoke", func(w http.ResponseWriter, _ *http.Request) { authority.revoked.Store(true); w.WriteHeader(204) })
	mux.HandleFunc("POST /fixture/restore-access", func(w http.ResponseWriter, _ *http.Request) { authority.revoked.Store(false); w.WriteHeader(204) })
	mux.HandleFunc("POST /fixture/stop", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204); once.Do(func() { close(stop) }) })
	fmt.Println("ARTIFACT_BROWSER_URL=" + ui.URL)
	select {
	case <-stop:
	case <-time.After(5 * time.Minute):
		t.Fatal("browser fixture timed out")
	}
}
