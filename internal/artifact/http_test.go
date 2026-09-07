package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testAuthorizer struct {
	denied     atomic.Bool
	artifactID string
	revision   int64
}

func (a *testAuthorizer) Upload(_ context.Context, token string, _ Reservation) error {
	if a.denied.Load() || token != "worker" {
		return ErrDenied
	}
	return nil
}
func (a *testAuthorizer) Read(_ context.Context, r ReadAuthorization) error {
	if a.denied.Load() || r.Token != "member" || r.ArtifactID != a.artifactID || r.Revision != a.revision {
		return ErrDenied
	}
	return nil
}

func TestHTTPArtifactAccessWithoutRunners(t *testing.T) {
	t.Parallel()
	s, _ := testService(t)
	s.config.AllowedOrigins = []string{"https://hub.example.com"}
	auth := &testAuthorizer{}
	handler, err := NewHTTPServer(s, auth)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &Client{Origin: server.URL, Token: func(context.Context) (string, error) { return "worker", nil }}
	u, err := client.Reserve(t.Context(), testReservation(s))
	if err != nil {
		t.Fatal(err)
	}
	part := testPart(0, strings.Repeat("safe <script> text\n", 32000))
	obj, err := client.Append(t.Context(), u.ArtifactID, part)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := client.Finalize(t.Context(), u.ArtifactID, "complete", nil)
	if err != nil {
		t.Fatal(err)
	}
	auth.artifactID = u.ArtifactID
	auth.revision = ref.Revision
	base := "/v1/artifacts/" + u.ArtifactID + "/manifests/" + strconv.FormatInt(ref.Revision, 10)
	for _, test := range []struct {
		name, path, token, origin string
		revoked                   bool
		want                      int
	}{
		{"manifest stopped runner", base, "member", "", false, 200},
		{"large log stopped runner", base + "/objects/" + obj.ID, "member", "https://hub.example.com", false, 200},
		{"wrong principal", base, "worker", "", false, 403},
		{"revoked", base, "member", "", true, 403},
		{"wrong revision", strings.Replace(base, "/manifests/2", "/manifests/1", 1), "member", "", false, 403},
		{"wrong origin", base, "member", "https://evil.example.com", false, 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth.denied.Store(test.revoked)
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			req.Header.Set("Authorization", "Bearer "+test.token)
			req.Header.Set("Origin", test.origin)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != test.want {
				t.Fatalf("%d: %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("unsafe response headers")
			}
			if test.want == 200 && strings.Contains(test.path, "/objects/") && !bytes.Equal(rec.Body.Bytes(), part.Data) {
				t.Fatal("bounded log differs")
			}
		})
	}
	auth.denied.Store(true)
	if _, err := client.Append(t.Context(), u.ArtifactID, part); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
}

func TestHTTPFailureAndDecodeContracts(t *testing.T) {
	t.Parallel()
	s, _ := testService(t)
	handler, err := NewHTTPServer(s, &testAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		err    error
		status int
	}{{ErrInvalid, 422}, {ErrIntegrity, 422}, {ErrQuota, 413}, {ErrExpired, 410}, {ErrMissing, 404}, {ErrUnsupported, 501}, {ErrAuthorization, 503}, {ErrConflict, 409}, {ErrStorage, 503}} {
		t.Run(test.err.Error(), func(t *testing.T) {
			rec := httptest.NewRecorder()
			ctx := handler.echo.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
			if err := handler.failure(ctx, test.err); err != nil {
				t.Fatal(err)
			}
			if rec.Code != test.status {
				t.Fatal(rec.Code)
			}
		})
	}
	for _, data := range []string{`{"unknown":1}`, `{} {}`, strings.Repeat("x", 33)} {
		var input struct{}
		if err := Decode(strings.NewReader(data), &input, 32); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestHTTPUploadStorageOutage(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"append", "finalize"} {
		t.Run(operation, func(t *testing.T) {
			s, storage := testService(t)
			handler, err := NewHTTPServer(s, &testAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			client := &Client{Origin: server.URL, Token: func(context.Context) (string, error) { return "worker", nil }}
			u, err := client.Reserve(t.Context(), testReservation(s))
			if err != nil {
				t.Fatal(err)
			}
			storage.mu.Lock()
			storage.failure = ErrStorage
			storage.mu.Unlock()
			if operation == "append" {
				_, err = client.Append(t.Context(), u.ArtifactID, testPart(0, "log"))
			} else {
				_, err = client.Finalize(t.Context(), u.ArtifactID, "complete", nil)
			}
			if !errors.Is(err, ErrStorage) {
				t.Fatal("storage outage misreported as lost permission", err)
			}
		})
	}
}

func TestRemoteHubAuthorizationFailsClosed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status int
		want   error
	}{{204, nil}, {403, ErrDenied}, {404, ErrDenied}, {500, ErrAuthorization}, {302, ErrAuthorization}} {
		t.Run(strconv.Itoa(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") == "" {
					t.Error("missing auth")
				}
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			h := &RemoteHub{Origin: server.URL, OrganizationID: NewID("org"), ProjectID: NewID("prj"), PublisherToken: func() string { return "publisher" }}
			r := Reservation{Scope: Scope{OrganizationID: h.OrganizationID, ProjectID: h.ProjectID}}
			for _, err := range []error{h.Upload(t.Context(), "worker", r), h.Read(t.Context(), ReadAuthorization{Token: "member"}), h.Publish(t.Context(), Reference{Scope: r.Scope})} {
				if !errors.Is(err, test.want) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestArtifactServeShutdown(t *testing.T) {
	t.Parallel()
	s, _ := testService(t)
	handler, err := NewHTTPServer(s, &testAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- handler.Serve(ctx, listener, nil) }()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatal(response.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown stuck")
	}
}

func TestConfigAndManifestValidation(t *testing.T) {
	t.Parallel()
	for _, edit := range []func(*Config){func(c *Config) { c.Mode = "hosted" }, func(c *Config) { c.Policy.BackupSeconds = 0 }, func(c *Config) { c.Policy.AbandonedUploadSeconds = 1 << 62 }, func(c *Config) { c.Policy.Limits.RetentionSeconds = 0 }, func(c *Config) { c.Policy.DeletionRecordSeconds = 1 }} {
		cfg := testConfig(t)
		edit(&cfg)
		if cfg.Validate() == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	s, _ := testService(t)
	u, err := s.Reserve(t.Context(), testReservation(s))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), u.ArtifactID, testPart(0, "log")); err != nil {
		t.Fatal(err)
	}
	body, _, err := s.Manifest(t.Context(), u.ArtifactID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(*Manifest)
	}{{"digest", func(m *Manifest) { m.Objects[0].SHA256 = "bad" }}, {"offset", func(m *Manifest) { m.Objects[0].Offset = 1 }}, {"traversal", func(m *Manifest) { m.Objects[0].Path = "../secret" }}, {"total", func(m *Manifest) { m.TotalBytes++ }}, {"scope", func(m *Manifest) { m.OrganizationID = "org_bad" }}, {"media", func(m *Manifest) { m.Objects[0].MediaType = "text/html" }}, {"capture", func(m *Manifest) { m.Kind = "diff" }}} {
		t.Run(test.name, func(t *testing.T) {
			var m Manifest
			if err := json.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
			test.edit(&m)
			if m.Validate() == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}
