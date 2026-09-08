package web_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	admissionmodel "github.com/digitaldrywood/detent/internal/admission/model"
	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/buildinfo"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	configwatcher "github.com/digitaldrywood/detent/internal/config/watcher"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/local"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/healthnotify"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/pause"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/provenance"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
	"github.com/digitaldrywood/detent/internal/store/storetest"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web"
	"github.com/digitaldrywood/detent/internal/web/demofixtures"
)

const sseTestOperationTimeout = 30 * time.Second

func TestMain(m *testing.M) {
	if err := os.Unsetenv("DETENT_API_TOKEN"); err != nil {
		panic("clear DETENT_API_TOKEN: " + err.Error())
	}
	os.Exit(m.Run())
}

func TestNewServerValidatesDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		deps web.Dependencies
		want error
	}{
		{
			name: "missing hub",
			deps: testDeps(t),
			want: web.ErrMissingHub,
		},
		{
			name: "missing store",
			deps: func() web.Dependencies {
				deps := testDeps(t)
				deps.Store = nil
				return deps
			}(),
			want: web.ErrMissingStore,
		},
		{
			name: "missing registry",
			deps: func() web.Dependencies {
				deps := testDeps(t)
				deps.Registry = nil
				return deps
			}(),
			want: web.ErrMissingRegistry,
		},
		{
			name: "missing connector",
			deps: func() web.Dependencies {
				deps := testDeps(t)
				deps.Connector = nil
				return deps
			}(),
			want: web.ErrMissingConnector,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if errors.Is(tt.want, web.ErrMissingHub) {
				tt.deps.Hub = nil
			}

			_, err := web.NewServer(web.Config{}, tt.deps)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewServer() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestNewServerConfiguresHTTPTimeouts(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	httpServer := server.Echo().Server
	if httpServer.ReadHeaderTimeout <= 0 {
		t.Fatalf("ReadHeaderTimeout = %v, want positive duration", httpServer.ReadHeaderTimeout)
	}
	if httpServer.IdleTimeout <= 0 {
		t.Fatalf("IdleTimeout = %v, want positive duration", httpServer.IdleTimeout)
	}
	if httpServer.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %v, want 0 for long-lived SSE streams", httpServer.WriteTimeout)
	}
}

func TestHealthReportsStartupLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		transition    func(*web.StartupLifecycle)
		wantHTTP      int
		wantStatus    string
		wantReady     bool
		wantLifecycle web.StartupLifecycleState
	}{
		{
			name:          "starting",
			transition:    func(*web.StartupLifecycle) {},
			wantHTTP:      http.StatusServiceUnavailable,
			wantStatus:    "not_ready",
			wantLifecycle: web.StartupLifecycleStarting,
		},
		{
			name:          "ready",
			transition:    (*web.StartupLifecycle).MarkReady,
			wantHTTP:      http.StatusOK,
			wantStatus:    "ok",
			wantReady:     true,
			wantLifecycle: web.StartupLifecycleReady,
		},
		{
			name:          "failed",
			transition:    (*web.StartupLifecycle).MarkFailed,
			wantHTTP:      http.StatusServiceUnavailable,
			wantStatus:    "not_ready",
			wantLifecycle: web.StartupLifecycleFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lifecycle := web.NewStartupLifecycle()
			tt.transition(lifecycle)
			deps := testDeps(t)
			deps.StartupLifecycle = lifecycle
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			payload := requestJSON(t, server, http.MethodGet, "/health", tt.wantHTTP)
			if got := payload["status"]; got != tt.wantStatus {
				t.Fatalf("status = %#v, want %q", got, tt.wantStatus)
			}
			if got := payload["ready"]; got != tt.wantReady {
				t.Fatalf("ready = %#v, want %t", got, tt.wantReady)
			}
			if got := payload["lifecycle"]; got != string(tt.wantLifecycle) {
				t.Fatalf("lifecycle = %#v, want %q", got, tt.wantLifecycle)
			}
		})
	}
}

func TestServerRoutes(t *testing.T) {
	t.Parallel()

	staticDir := t.TempDir()
	cssDir := filepath.Join(staticDir, "css")
	if err := os.MkdirAll(cssDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(cssDir, "output.css"), []byte("body{color:black}"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: staticDir}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name        string
		path        string
		wantStatus  int
		wantContent string
	}{
		{
			name:        "dashboard",
			path:        "/",
			wantStatus:  http.StatusOK,
			wantContent: "Detent",
		},
		{
			name:        "settings",
			path:        "/settings",
			wantStatus:  http.StatusOK,
			wantContent: "Settings",
		},
		{
			name:        "analytics",
			path:        "/analytics",
			wantStatus:  http.StatusOK,
			wantContent: "Analytics",
		},
		{
			name:        "library",
			path:        "/library",
			wantStatus:  http.StatusOK,
			wantContent: `id="library-table"`,
		},
		{
			name:        "reports",
			path:        "/reports",
			wantStatus:  http.StatusOK,
			wantContent: `id="reports-kpis"`,
		},
		{
			name:        "health",
			path:        "/health",
			wantStatus:  http.StatusOK,
			wantContent: `"status":"ok"`,
		},
		{
			name:        "static css",
			path:        "/static/css/output.css",
			wantStatus:  http.StatusOK,
			wantContent: "body{color:black}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.wantContent) {
				t.Fatalf("body missing %q:\n%s", tt.wantContent, rec.Body.String())
			}
		})
	}
}

func TestCapacityClearEndpointIsIdempotentWithoutOutages(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := performForm(t, server.Handler(), http.MethodPost, "/api/v1/capacity/clear", url.Values{
		"scope": {"codex"},
	})
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNoContent, recorder.Body.String())
	}
	if recorder.Header().Get("HX-Trigger") != "capacityCleared" {
		t.Fatalf("HX-Trigger = %q", recorder.Header().Get("HX-Trigger"))
	}
}

func TestCapacityClearEndpointAcceptsAsyncRequest(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/capacity/clear", strings.NewReader(url.Values{
		"scope": {"codex"},
	}.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if got := recorder.Body.String(); !strings.Contains(got, `"status":"requested"`) || !strings.Contains(got, `"requested":0`) {
		t.Fatalf("body = %s", got)
	}
}

func TestTrackerAvailabilityClearEndpointIsIdempotentWithoutConditions(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := performForm(t, server.Handler(), http.MethodPost, "/api/v1/tracker/availability/clear", nil)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNoContent, recorder.Body.String())
	}
	if recorder.Header().Get("HX-Trigger") != "trackerAvailabilityCleared" {
		t.Fatalf("HX-Trigger = %q", recorder.Header().Get("HX-Trigger"))
	}
}

func TestForgeAvailabilityClearEndpointIsIdempotentWithoutConditions(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := performForm(t, server.Handler(), http.MethodPost, "/api/v1/forge/availability/clear", nil)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNoContent, recorder.Body.String())
	}
	if recorder.Header().Get("HX-Trigger") != "forgeAvailabilityCleared" {
		t.Fatalf("HX-Trigger = %q", recorder.Header().Get("HX-Trigger"))
	}
}

func TestFailureBreakerCanaryEndpointIsIdempotentWithoutActiveBreakers(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := performForm(t, server.Handler(), http.MethodPost, "/api/v1/failure-breaker/canary", nil)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNoContent, recorder.Body.String())
	}
	if recorder.Header().Get("HX-Trigger") != "failureBreakerCanaryRequested" {
		t.Fatalf("HX-Trigger = %q", recorder.Header().Get("HX-Trigger"))
	}
}

func TestFailureBreakerCanaryEndpointRejectsUnknownProject(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	recorder := performForm(t, server.Handler(), http.MethodPost, "/api/v1/failure-breaker/canary", url.Values{
		"project_id": {"missing"},
	})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestLibraryPageListsLocalArtifactsAndPullRequestRecords(t *testing.T) {
	t.Parallel()

	server := newLibraryTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/library", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		`id="library-table"`,
		"outputs/ad-1/manifest.json",
		"outputs/ad-unsafe/manifest.json",
		"pending_review",
		"review/ad-1",
		"PR #934",
		"https://github.com/digitaldrywood/detent/pull/934",
		"validator clean",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("library page missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "javascript:alert") {
		t.Fatalf("library page rendered unsafe review URL:\n%s", body)
	}
}

func TestLibraryPageFiltersRows(t *testing.T) {
	t.Parallel()

	server := newLibraryTestServer(t)
	tests := []struct {
		name      string
		path      string
		want      string
		forbidden string
	}{
		{
			name:      "pull request kind",
			path:      "/library?kind=pull_request",
			want:      "PR #934",
			forbidden: "outputs/ad-1/manifest.json",
		},
		{
			name:      "artifact status",
			path:      "/library?status=pending_review",
			want:      "outputs/ad-1/manifest.json",
			forbidden: "PR #934",
		},
		{
			name:      "project",
			path:      "/library?project=video",
			want:      "outputs/ad-1/manifest.json",
			forbidden: "PR #934",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			server.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, tt.want) {
				t.Fatalf("filtered library page missing %q:\n%s", tt.want, body)
			}
			if strings.Contains(body, tt.forbidden) {
				t.Fatalf("filtered library page contains %q:\n%s", tt.forbidden, body)
			}
		})
	}
}

func TestAPITokenAuthProtectsAPIRoutes(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusUnauthorized)
	requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusUnauthorized, map[string]string{
		"Authorization": "Bearer wrong",
	})
	requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusOK, map[string]string{
		"X-API-Key": "detent_test_token",
	})
}

func TestAPITokenEnvOverride(t *testing.T) {
	t.Setenv("DETENT_API_TOKEN", "detent_env_token")

	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_config_token"},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusUnauthorized, map[string]string{
		"Authorization": "Bearer detent_config_token",
	})
	requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusOK, map[string]string{
		"Authorization": "Bearer detent_env_token",
	})
}

func TestAPIFailsClosedWithoutTokenOnNonLoopbackBind(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	server, err := web.NewServer(web.Config{
		Logger:        slog.New(slog.NewTextHandler(&logs, nil)),
		ServerAddress: "0.0.0.0:4000",
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusForbidden)
	requestJSON(t, server, http.MethodPost, "/api/v1/refresh", http.StatusForbidden)
	loopback := performJSONWithRemote(t, server.Handler(), http.MethodGet, "/api/v1/state", "", "127.0.0.1:49152", nil)
	if loopback.Code != http.StatusForbidden {
		t.Fatalf("loopback peer status = %d, want %d; body = %s", loopback.Code, http.StatusForbidden, loopback.Body.String())
	}
	if !strings.Contains(logs.String(), "api_token") {
		t.Fatalf("startup warning missing api_token detail:\n%s", logs.String())
	}
}

func TestLoopbackPeerReadTrustRestrictsRoutesAndPeers(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		ServerAddress: "0.0.0.0:0",
		GlobalConfig:  globalconfig.Config{TrustLoopbackPeerRead: true},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	forwarded := map[string]string{
		"Forwarded":       "for=127.0.0.1",
		"X-Forwarded-For": "127.0.0.1",
		"X-Real-IP":       "127.0.0.1",
	}
	tests := []struct {
		name       string
		method     string
		path       string
		remoteAddr string
		headers    map[string]string
		want       int
	}{
		{name: "IPv4 read", method: http.MethodGet, path: "/api/v1/state", remoteAddr: "127.0.0.1:49152", want: http.StatusOK},
		{name: "IPv6 read", method: http.MethodGet, path: "/api/v1/state", remoteAddr: "[::1]:49152", want: http.StatusOK},
		{name: "IPv4-mapped IPv6 read", method: http.MethodGet, path: "/api/v1/state", remoteAddr: "[::ffff:127.0.0.1]:49152", want: http.StatusOK},
		{name: "forwarded loopback ignored", method: http.MethodGet, path: "/api/v1/state", remoteAddr: "192.0.2.10:49152", headers: forwarded, want: http.StatusForbidden},
		{name: "malformed peer", method: http.MethodGet, path: "/api/v1/state", remoteAddr: "127.0.0.1", want: http.StatusForbidden},
		{name: "admin GET", method: http.MethodGet, path: "/api/v1/keys", remoteAddr: "127.0.0.1:49152", want: http.StatusForbidden},
		{name: "POST", method: http.MethodPost, path: "/api/v1/refresh", remoteAddr: "127.0.0.1:49152", want: http.StatusForbidden},
		{name: "DELETE", method: http.MethodDelete, path: "/api/v1/projects/detent/budget/override", remoteAddr: "127.0.0.1:49152", want: http.StatusForbidden},
		{name: "PUT", method: http.MethodPut, path: "/api/v1/state", remoteAddr: "127.0.0.1:49152", want: http.StatusMethodNotAllowed},
		{name: "PATCH", method: http.MethodPatch, path: "/api/v1/state", remoteAddr: "127.0.0.1:49152", want: http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := performJSONWithRemote(t, server.Handler(), tt.method, tt.path, "", tt.remoteAddr, tt.headers)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}

func TestLoopbackPeerReadTrustPreservesLoopbackBindAccess(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	deps.Store = openWebTestStore(t)
	deps.Refresher = &refreshProbe{}
	server, err := web.NewServer(web.Config{
		ServerAddress: "127.0.0.1:0",
		GlobalConfig:  globalconfig.Config{TrustLoopbackPeerRead: true},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	for _, test := range []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{name: "read route", method: http.MethodGet, path: "/api/v1/state", want: http.StatusOK},
		{name: "admin GET", method: http.MethodGet, path: "/api/v1/keys", want: http.StatusOK},
		{name: "mutation", method: http.MethodPost, path: "/api/v1/refresh", want: http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := performJSONWithRemote(t, server.Handler(), test.method, test.path, "", "127.0.0.1:49152", nil)
			if rec.Code != test.want {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, test.want, rec.Body.String())
			}
		})
	}
}

func TestLoopbackPeerReadTrustLiveReload(t *testing.T) {
	cfg := globalconfig.Config{}
	server, err := web.NewServer(web.Config{
		ServerAddress:      "0.0.0.0:0",
		GlobalConfig:       cfg,
		GlobalConfigSource: func() globalconfig.Config { return cfg },
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	for _, step := range []struct {
		name    string
		enabled bool
		want    int
	}{
		{name: "disabled initially", want: http.StatusForbidden},
		{name: "enabled after reload", enabled: true, want: http.StatusOK},
		{name: "disabled after reload", want: http.StatusForbidden},
	} {
		t.Run(step.name, func(t *testing.T) {
			cfg.TrustLoopbackPeerRead = step.enabled
			rec := performJSONWithRemote(t, server.Handler(), http.MethodGet, "/api/v1/state", "", "127.0.0.1:49152", nil)
			if rec.Code != step.want {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, step.want, rec.Body.String())
			}
		})
	}
}

func TestLoopbackPeerReadTrustHonorsStaticTokenAndRejectsSuppliedFailures(t *testing.T) {
	t.Parallel()

	backend := openWebTestStore(t)
	deps := testDeps(t)
	deps.Store = backend
	server, err := web.NewServer(web.Config{
		ServerAddress: "0.0.0.0:0",
		GlobalConfig: globalconfig.Config{
			APIToken:              "detent_admin_token",
			TrustLoopbackPeerRead: true,
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tokenless := performJSONWithRemote(t, server.Handler(), http.MethodGet, "/api/v1/state", "", "127.0.0.1:49152", nil)
	if tokenless.Code != http.StatusOK {
		t.Fatalf("tokenless status = %d, want %d; body = %s", tokenless.Code, http.StatusOK, tokenless.Body.String())
	}
	for name, headers := range map[string]map[string]string{
		"empty bearer":  {"Authorization": "Bearer "},
		"empty API key": {"X-API-Key": " "},
	} {
		t.Run(name, func(t *testing.T) {
			rec := performJSONWithRemote(t, server.Handler(), http.MethodGet, "/api/v1/state", "", "127.0.0.1:49152", headers)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
			}
		})
	}

	revokedToken, revokedID := createAPIKeyThroughHTTP(t, server, `{
		"name": "Revoked loopback client",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	revoke := performJSON(t, server.Handler(), http.MethodDelete, "/api/v1/keys/"+revokedID, "", map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if revoke.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d, want %d; body = %s", revoke.Code, http.StatusNoContent, revoke.Body.String())
	}
	expiredToken := createExpiredAPIKey(t, backend)

	tests := []struct {
		name  string
		token string
		code  string
	}{
		{name: "invalid", token: "invalid", code: "invalid_token"},
		{name: "expired", token: expiredToken, code: "token_expired"},
		{name: "revoked", token: revokedToken, code: "token_revoked"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := performJSONWithRemote(t, server.Handler(), http.MethodGet, "/api/v1/state", "", "127.0.0.1:49152", map[string]string{
				"Authorization": "Bearer " + tt.token,
			})
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
			}
			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("Unmarshal() error = %v; body = %s", err, rec.Body.String())
			}
			if got := nestedString(t, payload, "error", "code"); got != tt.code {
				t.Fatalf("error code = %q, want %q", got, tt.code)
			}
		})
	}
}

func TestAPITokenDoesNotLeakToLogs(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	server, err := web.NewServer(web.Config{
		Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
		GlobalConfig: globalconfig.Config{APIToken: "detent_real_token"},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestJSONWithHeaders(t, server, http.MethodPost, "/api/v1/refresh", http.StatusUnauthorized, map[string]string{
		"Authorization": "Bearer detent_wrong_token",
		"X-API-Key":     "detent_wrong_key",
	})
	for _, secret := range []string{"detent_wrong_token", "detent_wrong_key", "detent_real_token"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("logs leaked token %q:\n%s", secret, logs.String())
		}
	}
}

func TestAPITokenAllowsDashboardHTMXWithUICookie(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Dialog card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_real_token"},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	dialogPath := "/api/v1/kanban/move?project_id=detent&issue_id=I_kw1&current_state=Todo&identifier=digitaldrywood%2Fdetent%231&title=Dialog+card"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, dialogPath, nil)
	req.Header.Set("HX-Request", "true")
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("dialog without cookie status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "detent_real_token") {
		t.Fatalf("dashboard body leaked API token")
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("dashboard response did not set UI API cookie")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/state", nil)
	req.Header.Set("HX-Request", "true")
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("state with UI cookie status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, dialogPath, nil)
	req.Header.Set("HX-Request", "true")
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dialog with UI cookie status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	form := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"Todo"},
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/kanban/move", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("kanban post with UI cookie status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestDashboardHTMXAuthAllowsSameOriginDashboardOnWildcardBindWithoutToken(t *testing.T) {
	t.Parallel()

	origins := []struct {
		name       string
		host       string
		remote     string
		htmxTarget string
	}{
		{
			name:   "localhost",
			host:   "localhost:4000",
			remote: "127.0.0.1:49152",
		},
		{
			name:       "private same-origin host",
			host:       "100.95.107.50:4000",
			remote:     "100.95.107.51:49152",
			htmxTarget: "detail-sheet-host",
		},
	}

	for _, origin := range origins {
		origin := origin
		t.Run(origin.name, func(t *testing.T) {
			t.Parallel()

			fixture := newDashboardHTMXAuthFixture(t)
			tests := []struct {
				name string
				req  dashboardHTMXRequest
				want string
			}{
				{
					name: "card sheet",
					req: dashboardHTMXRequest{
						method: http.MethodGet,
						path:   "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
					},
					want: "Screenshot sheet card",
				},
				{
					name: "move dialog",
					req: dashboardHTMXRequest{
						method: http.MethodGet,
						path:   "/api/v1/kanban/move?project_id=detent&issue_id=I_move&current_state=Backlog&target_state=Todo&identifier=digitaldrywood%2Fdetent%239512&title=Same-origin+move+card",
					},
					want: `hx-post="/api/v1/kanban/move"`,
				},
				{
					name: "move submit",
					req: dashboardHTMXRequest{
						method: http.MethodPost,
						path:   "/api/v1/kanban/move",
						form: url.Values{
							"project_id":    {"detent"},
							"issue_id":      {"I_move"},
							"current_state": {"Backlog"},
							"target_state":  {"Todo"},
						},
					},
					want: "Moved card to Todo.",
				},
				{
					name: "remove submit",
					req: dashboardHTMXRequest{
						method: http.MethodPost,
						path:   "/api/v1/kanban/remove",
						form: url.Values{
							"project_id":    {"detent"},
							"issue_id":      {"I_remove"},
							"current_state": {"Todo"},
						},
					},
					want: "Removed card from project.",
				},
				{
					name: "comment dialog",
					req: dashboardHTMXRequest{
						method: http.MethodGet,
						path:   "/api/v1/kanban/comment?project_id=detent&target=issue&issue_id=I_comment&identifier=digitaldrywood%2Fdetent%239514&title=Same-origin+comment+card",
					},
					want: `hx-post="/api/v1/kanban/comment"`,
				},
				{
					name: "comment submit",
					req: dashboardHTMXRequest{
						method: http.MethodPost,
						path:   "/api/v1/kanban/comment",
						form: url.Values{
							"project_id": {"detent"},
							"target":     {"issue"},
							"issue_id":   {"I_comment"},
							"body":       {"Same-origin dashboard comment"},
						},
					},
					want: "Comment submitted.",
				},
				{
					name: "refresh submit",
					req: dashboardHTMXRequest{
						method: http.MethodPost,
						path:   "/api/v1/refresh",
					},
					want: `id="manual-refresh-status"`,
				},
			}

			for _, tt := range tests {
				tt := tt
				t.Run(tt.name, func(t *testing.T) {
					tt.req.host = origin.host
					tt.req.remote = origin.remote
					tt.req.htmxTarget = origin.htmxTarget
					rec := performDashboardHTMXRequest(t, fixture.server.Handler(), tt.req)
					if rec.Code != http.StatusOK {
						t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
					}
					if !strings.Contains(rec.Body.String(), tt.want) {
						t.Fatalf("body missing %q:\n%s", tt.want, rec.Body.String())
					}
				})
			}

			if fixture.refresher.calls == 0 {
				t.Fatal("refresh calls = 0, want dashboard refresh to reach refresher")
			}
		})
	}
}

func TestAPIKeyDashboardHTMXAuthAllowsSameOriginManagementWithoutToken(t *testing.T) {
	t.Parallel()

	origins := []struct {
		name       string
		host       string
		remote     string
		htmxTarget string
	}{
		{
			name:       "localhost",
			host:       "localhost:4000",
			remote:     "127.0.0.1:49152",
			htmxTarget: "api-keys-table",
		},
		{
			name:       "private same-origin host",
			host:       "100.95.107.50:4000",
			remote:     "100.95.107.51:49152",
			htmxTarget: "api-keys-table",
		},
	}

	for _, origin := range origins {
		origin := origin
		t.Run(origin.name, func(t *testing.T) {
			t.Parallel()

			server, backend := newAPIKeyDashboardHTMXTestServer(t, "")
			session := newAPIKeyDashboardSession(t, server.Handler(), origin.host, origin.remote)
			baseRequest := dashboardHTMXRequest{
				host:       origin.host,
				remote:     origin.remote,
				currentURL: "http://" + origin.host + "/api-keys",
				htmxTarget: origin.htmxTarget,
				headers:    session.headers,
				cookies:    session.cookies,
			}

			list := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
				method:     http.MethodGet,
				path:       "/api/v1/keys",
				host:       baseRequest.host,
				remote:     baseRequest.remote,
				currentURL: baseRequest.currentURL,
				htmxTarget: baseRequest.htmxTarget,
				headers:    baseRequest.headers,
				cookies:    baseRequest.cookies,
			})
			if list.Code != http.StatusOK {
				t.Fatalf("list status = %d, want %d; body = %s", list.Code, http.StatusOK, list.Body.String())
			}
			if !strings.Contains(list.Body.String(), "Create your first key") {
				t.Fatalf("list body missing empty state:\n%s", list.Body.String())
			}

			create := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys",
				host:       baseRequest.host,
				remote:     baseRequest.remote,
				currentURL: baseRequest.currentURL,
				htmxTarget: "api-key-modal-body",
				headers:    baseRequest.headers,
				cookies:    baseRequest.cookies,
				form: url.Values{
					"name":         {"Dashboard client"},
					"scopes":       {"write"},
					"all_projects": {"true"},
					"expires_in":   {"90d"},
				},
			})
			if create.Code != http.StatusOK {
				t.Fatalf("create status = %d, want %d; body = %s", create.Code, http.StatusOK, create.Body.String())
			}
			if create.Header().Get("HX-Trigger") != "apiKeyCreated" {
				t.Fatalf("create HX-Trigger = %q, want apiKeyCreated", create.Header().Get("HX-Trigger"))
			}
			if body := create.Body.String(); !strings.Contains(body, "API key created") || !strings.Contains(body, apikey.TokenPrefix) {
				t.Fatalf("create body missing reveal:\n%s", body)
			}

			keys, err := backend.ListAPIKeys(context.Background())
			if err != nil {
				t.Fatalf("ListAPIKeys() error = %v", err)
			}
			if len(keys) != 1 {
				t.Fatalf("keys len = %d, want 1", len(keys))
			}
			keyID := keys[0].ID

			rotateDialog := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
				method:     http.MethodGet,
				path:       "/api/v1/keys/" + keyID + "/rotate",
				host:       baseRequest.host,
				remote:     baseRequest.remote,
				currentURL: baseRequest.currentURL,
				htmxTarget: "api-key-modal-body",
				headers:    baseRequest.headers,
				cookies:    baseRequest.cookies,
			})
			if rotateDialog.Code != http.StatusOK {
				t.Fatalf("rotate dialog status = %d, want %d; body = %s", rotateDialog.Code, http.StatusOK, rotateDialog.Body.String())
			}
			if !strings.Contains(rotateDialog.Body.String(), "Rotate API key") {
				t.Fatalf("rotate dialog body missing title:\n%s", rotateDialog.Body.String())
			}

			rotate := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys/" + keyID + "/rotate",
				host:       baseRequest.host,
				remote:     baseRequest.remote,
				currentURL: baseRequest.currentURL,
				htmxTarget: "api-key-modal-body",
				headers:    baseRequest.headers,
				cookies:    baseRequest.cookies,
				form: url.Values{
					"grace": {"1h"},
				},
			})
			if rotate.Code != http.StatusOK {
				t.Fatalf("rotate status = %d, want %d; body = %s", rotate.Code, http.StatusOK, rotate.Body.String())
			}
			if rotate.Header().Get("HX-Trigger") != "apiKeyChanged" {
				t.Fatalf("rotate HX-Trigger = %q, want apiKeyChanged", rotate.Header().Get("HX-Trigger"))
			}
			if body := rotate.Body.String(); !strings.Contains(body, "API key created") || !strings.Contains(body, apikey.TokenPrefix) {
				t.Fatalf("rotate body missing reveal:\n%s", body)
			}

			revoke := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
				method:     http.MethodDelete,
				path:       "/api/v1/keys/" + keyID,
				host:       baseRequest.host,
				remote:     baseRequest.remote,
				currentURL: baseRequest.currentURL,
				htmxTarget: "api-keys-table",
				headers:    baseRequest.headers,
				cookies:    baseRequest.cookies,
			})
			if revoke.Code != http.StatusOK {
				t.Fatalf("revoke status = %d, want %d; body = %s", revoke.Code, http.StatusOK, revoke.Body.String())
			}
			if revoke.Header().Get("HX-Trigger") != "apiKeyChanged" {
				t.Fatalf("revoke HX-Trigger = %q, want apiKeyChanged", revoke.Header().Get("HX-Trigger"))
			}
			revoked, err := backend.APIKey(context.Background(), keyID)
			if err != nil {
				t.Fatalf("APIKey() after revoke error = %v", err)
			}
			if revoked.RevokedAt == nil {
				t.Fatalf("RevokedAt = nil, want timestamp")
			}
		})
	}
}

func TestAPIKeyDashboardHTMXAuthAllowsConfiguredTokenUICookie(t *testing.T) {
	t.Parallel()

	server, backend := newAPIKeyDashboardHTMXTestServer(t, "detent_cookie_token")
	request := dashboardHTMXRequest{
		method:     http.MethodGet,
		path:       "/api/v1/keys",
		host:       "100.95.107.50:4000",
		remote:     "100.95.107.51:49152",
		currentURL: "http://100.95.107.50:4000/api-keys",
		htmxTarget: "api-keys-table",
	}

	withoutCookie := performDashboardHTMXRequest(t, server.Handler(), request)
	if withoutCookie.Code != http.StatusUnauthorized {
		t.Fatalf("list without UI cookie status = %d, want %d; body = %s", withoutCookie.Code, http.StatusUnauthorized, withoutCookie.Body.String())
	}

	page := httptest.NewRecorder()
	pageReq := httptest.NewRequest(http.MethodGet, "http://100.95.107.50:4000/api-keys", nil)
	pageReq.Host = "100.95.107.50:4000"
	server.Handler().ServeHTTP(page, pageReq)
	if page.Code != http.StatusOK {
		t.Fatalf("api keys page status = %d, want %d; body = %s", page.Code, http.StatusOK, page.Body.String())
	}
	cookies := page.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("api keys page did not set UI API cookie")
	}

	request.cookies = cookies
	withCookie := performDashboardHTMXRequest(t, server.Handler(), request)
	if withCookie.Code != http.StatusOK {
		t.Fatalf("list with UI cookie status = %d, want %d; body = %s", withCookie.Code, http.StatusOK, withCookie.Body.String())
	}

	create := performDashboardHTMXRequest(t, server.Handler(), dashboardHTMXRequest{
		method:     http.MethodPost,
		path:       "/api/v1/keys",
		host:       "100.95.107.50:4000",
		remote:     "100.95.107.51:49152",
		currentURL: "http://100.95.107.50:4000/api-keys",
		htmxTarget: "api-key-modal-body",
		cookies:    cookies,
		form: url.Values{
			"name":         {"Cookie client"},
			"scopes":       {"read"},
			"all_projects": {"true"},
			"expires_in":   {"90d"},
		},
	})
	if create.Code != http.StatusOK {
		t.Fatalf("create with UI cookie status = %d, want %d; body = %s", create.Code, http.StatusOK, create.Body.String())
	}
	keys, err := backend.ListAPIKeys(context.Background())
	if err != nil {
		t.Fatalf("ListAPIKeys() error = %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1", len(keys))
	}
}

func TestDashboardHTMXAuthRejectsUntrustedRequestsOnWildcardBindWithoutToken(t *testing.T) {
	t.Parallel()

	fixture := newDashboardHTMXAuthFixture(t)

	tests := []struct {
		name string
		req  dashboardHTMXRequest
	}{
		{
			name: "direct api state remains protected from localhost htmx",
			req: dashboardHTMXRequest{
				method: http.MethodGet,
				path:   "/api/v1/state",
			},
		},
		{
			name: "private host card sheet without htmx denied",
			req: dashboardHTMXRequest{
				method: http.MethodGet,
				path:   "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
				host:   "100.95.107.50:4000",
				remote: "100.95.107.51:49152",
				noHX:   true,
			},
		},
		{
			name: "private host card sheet without htmx target denied",
			req: dashboardHTMXRequest{
				method: http.MethodGet,
				path:   "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
				host:   "100.95.107.50:4000",
				remote: "100.95.107.51:49152",
			},
		},
		{
			name: "private host card sheet with cross origin htmx source denied",
			req: dashboardHTMXRequest{
				method:     http.MethodGet,
				path:       "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
				host:       "100.95.107.50:4000",
				remote:     "100.95.107.51:49152",
				htmxTarget: "detail-sheet-host",
				currentURL: "http://dashboard.example.test:4000/",
			},
		},
		{
			name: "spoofed localhost host from external peer denied",
			req: dashboardHTMXRequest{
				method:     http.MethodGet,
				path:       "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
				host:       "localhost:4000",
				remote:     "203.0.113.10:49152",
				htmxTarget: "detail-sheet-host",
			},
		},
		{
			name: "localhost host without htmx denied",
			req: dashboardHTMXRequest{
				method: http.MethodGet,
				path:   "/api/v1/board/card?actions=board&issue=digitaldrywood%2Fdetent%23523&project=detent",
				noHX:   true,
			},
		},
		{
			name: "private host move without htmx target denied",
			req: dashboardHTMXRequest{
				method: http.MethodPost,
				path:   "/api/v1/kanban/move",
				host:   "100.95.107.50:4000",
				remote: "100.95.107.51:49152",
				form: url.Values{
					"project_id":    {"detent"},
					"issue_id":      {"I_move"},
					"current_state": {"Todo"},
					"target_state":  {"In Progress"},
				},
			},
		},
		{
			name: "private host refresh without htmx target denied",
			req: dashboardHTMXRequest{
				method: http.MethodPost,
				path:   "/api/v1/refresh",
				host:   "100.95.107.50:4000",
				remote: "100.95.107.51:49152",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rec := performDashboardHTMXRequest(t, fixture.server.Handler(), tt.req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
	if got := fixture.actionConnector.stateUpdates(); len(got) != 0 {
		t.Fatalf("state updates = %#v, want none", got)
	}
	if got := fixture.actionConnector.comments(); len(got) != 0 {
		t.Fatalf("comments = %#v, want none", got)
	}
	if got := fixture.actionConnector.removals(); len(got) != 0 {
		t.Fatalf("removals = %#v, want none", got)
	}
	if fixture.refresher.calls != 0 {
		t.Fatalf("refresh calls = %d, want none", fixture.refresher.calls)
	}
}

func TestAPIKeyDashboardHTMXAuthRejectsUntrustedManagementWithoutToken(t *testing.T) {
	t.Parallel()

	server, backend := newAPIKeyDashboardHTMXTestServer(t, "")
	createForm := url.Values{
		"name":         {"Blocked client"},
		"scopes":       {"read"},
		"all_projects": {"true"},
		"expires_in":   {"90d"},
	}
	tests := []struct {
		name string
		req  dashboardHTMXRequest
	}{
		{
			name: "raw private host list denied",
			req: dashboardHTMXRequest{
				method: http.MethodGet,
				path:   "/api/v1/keys",
				host:   "100.95.107.50:4000",
				remote: "100.95.107.51:49152",
				noHX:   true,
			},
		},
		{
			name: "private host list without htmx target denied",
			req: dashboardHTMXRequest{
				method:     http.MethodGet,
				path:       "/api/v1/keys",
				host:       "100.95.107.50:4000",
				remote:     "100.95.107.51:49152",
				currentURL: "http://100.95.107.50:4000/api-keys",
			},
		},
		{
			name: "private host create with forged same origin htmx denied",
			req: dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys",
				host:       "100.95.107.50:4000",
				remote:     "100.95.107.51:49152",
				currentURL: "http://100.95.107.50:4000/api-keys",
				htmxTarget: "api-key-modal-body",
				form:       createForm,
			},
		},
		{
			name: "private host create with self minted dashboard token denied",
			req: dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys",
				host:       "100.95.107.50:4000",
				remote:     "100.95.107.51:49152",
				currentURL: "http://100.95.107.50:4000/api-keys",
				htmxTarget: "api-key-modal-body",
				headers: map[string]string{
					"X-Detent-API-Key-Dashboard": "forged",
				},
				cookies: []*http.Cookie{{
					Name:  "detent_api_key_dashboard",
					Value: "forged",
				}},
				form: createForm,
			},
		},
		{
			name: "private host create with cross origin htmx source denied",
			req: dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys",
				host:       "100.95.107.50:4000",
				remote:     "100.95.107.51:49152",
				currentURL: "http://dashboard.example.test:4000/api-keys",
				htmxTarget: "api-key-modal-body",
				form:       createForm,
			},
		},
		{
			name: "spoofed localhost create from external peer denied",
			req: dashboardHTMXRequest{
				method:     http.MethodPost,
				path:       "/api/v1/keys",
				host:       "localhost:4000",
				remote:     "203.0.113.10:49152",
				currentURL: "http://localhost:4000/api-keys",
				htmxTarget: "api-key-modal-body",
				form:       createForm,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rec := performDashboardHTMXRequest(t, server.Handler(), tt.req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
	keys, err := backend.ListAPIKeys(context.Background())
	if err != nil {
		t.Fatalf("ListAPIKeys() error = %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys len = %d, want 0", len(keys))
	}
}

type dashboardHTMXAuthFixture struct {
	server          *web.Server
	actionConnector *kanbanActionConnector
	refresher       *refreshProbe
}

func newDashboardHTMXAuthFixture(t *testing.T) dashboardHTMXAuthFixture {
	t.Helper()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
			"Todo":    {"In Progress"},
		},
	}, actionConnector)
	refresher := &refreshProbe{response: web.RefreshResponse{Queued: true}}
	deps.Refresher = refresher
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_sheet",
				Identifier: "digitaldrywood/detent#523",
				ProjectID:  "detent",
				Title:      "Screenshot sheet card",
				State:      "Backlog",
				URL:        "https://github.com/digitaldrywood/detent/issues/523",
			},
			{
				ID:         "I_move",
				Identifier: "digitaldrywood/detent#9512",
				ProjectID:  "detent",
				Title:      "Same-origin move card",
				State:      "Backlog",
			},
			{
				ID:         "I_remove",
				Identifier: "digitaldrywood/detent#9513",
				ProjectID:  "detent",
				Title:      "Same-origin remove card",
				State:      "Todo",
			},
			{
				ID:         "I_comment",
				Identifier: "digitaldrywood/detent#9514",
				ProjectID:  "detent",
				Title:      "Same-origin comment card",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		ServerAddress: "0.0.0.0:4000",
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return dashboardHTMXAuthFixture{
		server:          server,
		actionConnector: actionConnector,
		refresher:       refresher,
	}
}

func newAPIKeyDashboardHTMXTestServer(t *testing.T, apiToken string) (*web.Server, store.Store) {
	t.Helper()

	backend := openWebTestStore(t)
	deps := testDeps(t)
	deps.Store = backend
	cfg := web.Config{
		ServerAddress: "0.0.0.0:4000",
	}
	if apiToken != "" {
		cfg.GlobalConfig = globalconfig.Config{APIToken: apiToken}
	}
	server, err := web.NewServer(cfg, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server, backend
}

type apiKeyDashboardSession struct {
	headers map[string]string
	cookies []*http.Cookie
}

func newAPIKeyDashboardSession(t *testing.T, handler http.Handler, host string, remote string) apiKeyDashboardSession {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "http://"+host+"/api-keys", nil)
	req.Host = host
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("api keys page status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := html.UnescapeString(rec.Body.String())
	match := regexp.MustCompile(`"X-Detent-API-Key-Dashboard":"([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatalf("api keys page missing dashboard header token:\n%s", rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("api keys page did not set dashboard management cookie")
	}
	return apiKeyDashboardSession{
		headers: map[string]string{
			"X-Detent-API-Key-Dashboard": match[1],
		},
		cookies: cookies,
	}
}

func TestWorkItemAPICreatesLocalSQLiteItem(t *testing.T) {
	t.Parallel()

	server, conn, refresher := newWorkItemAPITestServer(t, "detent_test_token")
	rec := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/video/work-items", `{
		"title": " Author beat visuals ",
		"description": " Render storyboard frames ",
		"labels": ["video-assets"],
		"fields": {"render_status": "queued"},
		"priority": 2,
		"deliverable": {"kind": "artifact", "review_url": "http://127.0.0.1:8090/v/slug/g/assets"}
	}`, map[string]string{
		"Authorization": "Bearer detent_test_token",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload["id"] == "" || payload["identifier"] == "" || payload["url"] == "" {
		t.Fatalf("response missing id, identifier, or url: %#v", payload)
	}
	if payload["number"] != float64(1) {
		t.Fatalf("response number = %#v, want 1", payload["number"])
	}
	if refresher.calls != 1 {
		t.Fatalf("refresh calls = %d, want 1", refresher.calls)
	}
	issues, err := conn.FetchIssuesByStates(context.Background(), []string{"Todo"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues len = %d, want 1", len(issues))
	}
	issue := issues[0]
	if issue.Title != "Author beat visuals" || issue.Description != "Render storyboard frames" {
		t.Fatalf("issue text = %#v", issue)
	}
	if issue.Number != 1 {
		t.Fatalf("issue number = %d, want 1", issue.Number)
	}
	if issue.Fields["render_status"] != "queued" {
		t.Fatalf("Fields = %#v", issue.Fields)
	}
	if issue.Priority == nil || *issue.Priority != 2 {
		t.Fatalf("Priority = %v, want 2", issue.Priority)
	}
	if issue.Deliverable == nil || issue.Deliverable.ReviewURL != "http://127.0.0.1:8090/v/slug/g/assets" {
		t.Fatalf("Deliverable = %#v", issue.Deliverable)
	}
}

func TestWorkItemAPIErrors(t *testing.T) {
	t.Parallel()

	server, _, _ := newWorkItemAPITestServer(t, "detent_test_token")
	tests := []struct {
		name    string
		path    string
		body    string
		headers map[string]string
		want    int
	}{
		{
			name: "missing token",
			path: "/api/v1/projects/video/work-items",
			body: `{"title":"title","description":"body"}`,
			want: http.StatusUnauthorized,
		},
		{
			name: "wrong token",
			path: "/api/v1/projects/video/work-items",
			body: `{"title":"title","description":"body"}`,
			headers: map[string]string{
				"Authorization": "Bearer wrong",
			},
			want: http.StatusUnauthorized,
		},
		{
			name: "unknown project",
			path: "/api/v1/projects/missing/work-items",
			body: `{"title":"title","description":"body"}`,
			headers: map[string]string{
				"Authorization": "Bearer detent_test_token",
			},
			want: http.StatusNotFound,
		},
		{
			name: "invalid state",
			path: "/api/v1/projects/video/work-items",
			body: `{"title":"title","description":"body","state":"Missing"}`,
			headers: map[string]string{
				"Authorization": "Bearer detent_test_token",
			},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "invalid fields shape",
			path: "/api/v1/projects/video/work-items",
			body: `{"title":"title","description":"body","fields":{"render_status":42}}`,
			headers: map[string]string{
				"Authorization": "Bearer detent_test_token",
			},
			want: http.StatusUnprocessableEntity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := performJSON(t, server.Handler(), http.MethodPost, tt.path, tt.body, tt.headers)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}

func TestWorkItemAPIRejectsDuplicateIdentifier(t *testing.T) {
	t.Parallel()

	server, _, _ := newWorkItemAPITestServer(t, "detent_test_token")
	body := `{"title":"title","description":"body","identifier":"external-123"}`
	headers := map[string]string{"Authorization": "Bearer detent_test_token"}
	first := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/video/work-items", body, headers)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want %d; body = %s", first.Code, http.StatusCreated, first.Body.String())
	}
	second := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/video/work-items", body, headers)
	if second.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want %d; body = %s", second.Code, http.StatusConflict, second.Body.String())
	}
}

func TestWorkItemAPIRejectsUnsupportedTracker(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebGitHubLabelProject(t, deps.Registry, "github", "digitaldrywood/detent")
	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	rec := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/github/work-items", `{"title":"title","description":"body"}`, map[string]string{
		"Authorization": "Bearer detent_test_token",
	})
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
}

func TestAPIKeyManagementAndScopedWorkItemAccess(t *testing.T) {
	t.Parallel()

	server, backend, _, _ := newAPIKeyWorkItemTestServer(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	create := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/keys", `{
		"name": "Video Studio",
		"scopes": ["write"],
		"project_ids": ["digitaldrywood-video"],
		"expires_in": "90d"
	}`, map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d; body = %s", create.Code, http.StatusCreated, create.Body.String())
	}
	token := apiKeyTokenFromResponse(t, create.Body.Bytes())
	if token == "" {
		t.Fatalf("create key response did not include token: %s", create.Body.String())
	}

	list := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/keys", "", map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if list.Code != http.StatusOK {
		t.Fatalf("list keys status = %d, want %d; body = %s", list.Code, http.StatusOK, list.Body.String())
	}
	if strings.Contains(list.Body.String(), token) || strings.Contains(list.Body.String(), "key_hash") {
		t.Fatalf("list response leaked token or key hash: %s", list.Body.String())
	}

	allowed := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/digitaldrywood-video/work-items", `{
		"title": "Render storyboard",
		"description": "Queue work item"
	}`, map[string]string{
		"Authorization": "Bearer " + token,
	})
	if allowed.Code != http.StatusCreated {
		t.Fatalf("allowed work item status = %d, want %d; body = %s", allowed.Code, http.StatusCreated, allowed.Body.String())
	}
	waitForAPIUsageLog(t, backend, apiKeyIDFromResponse(t, create.Body.Bytes()))

	disallowedProject := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/projects/detent/work-items", `{
		"title": "Wrong project",
		"description": "Should fail"
	}`, map[string]string{
		"Authorization": "Bearer " + token,
	})
	if disallowedProject.Code != http.StatusForbidden {
		t.Fatalf("disallowed project status = %d, want %d; body = %s", disallowedProject.Code, http.StatusForbidden, disallowedProject.Body.String())
	}

	adminRoute := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/keys", "", map[string]string{
		"Authorization": "Bearer " + token,
	})
	if adminRoute.Code != http.StatusForbidden {
		t.Fatalf("write key management status = %d, want %d; body = %s", adminRoute.Code, http.StatusForbidden, adminRoute.Body.String())
	}
}

func TestAPIKeyCreateFormRejectsEmptyProjectSelection(t *testing.T) {
	t.Parallel()

	server, backend, _, _ := newAPIKeyWorkItemTestServer(t)
	form := url.Values{}
	form.Set("name", "Video Studio")
	form.Add("scopes", "write")
	form.Set("expires_in", "90d")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/keys", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer detent_admin_token")
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("create key status = %d, want %d; body = %s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(create key error) error = %v; body = %s", err, rec.Body.String())
	}
	if nestedString(t, payload, "error", "code") != "project_required" {
		t.Fatalf("create key error = %#v, want project_required", payload)
	}
	keys, err := backend.ListAPIKeys(context.Background())
	if err != nil {
		t.Fatalf("ListAPIKeys() error = %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys created = %d, want 0", len(keys))
	}
}

func TestAPIKeyRevokeExpireRotateAndRateLimit(t *testing.T) {
	t.Parallel()

	server, backend, _, _ := newAPIKeyWorkItemTestServer(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	createdToken, createdID := createAPIKeyThroughHTTP(t, server, `{
		"name": "Client",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	revoke := performJSON(t, server.Handler(), http.MethodDelete, "/api/v1/keys/"+createdID, "", map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if revoke.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d, want %d; body = %s", revoke.Code, http.StatusNoContent, revoke.Body.String())
	}
	revoked := requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusUnauthorized, map[string]string{
		"Authorization": "Bearer " + createdToken,
	})
	if nestedString(t, revoked, "error", "code") != "token_revoked" {
		t.Fatalf("revoked response = %#v, want token_revoked", revoked)
	}
	revokedKey, err := backend.APIKey(context.Background(), createdID)
	if err != nil {
		t.Fatalf("APIKey() after revoke error = %v", err)
	}
	if revokedKey.RevokedAt == nil {
		t.Fatalf("RevokedAt = nil, want timestamp")
	}

	expiredToken := createExpiredAPIKey(t, backend)
	expired := requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusUnauthorized, map[string]string{
		"Authorization": "Bearer " + expiredToken,
	})
	if nestedString(t, expired, "error", "code") != "token_expired" {
		t.Fatalf("expired response = %#v, want token_expired", expired)
	}

	rotateToken, rotateID := createAPIKeyThroughHTTP(t, server, `{
		"name": "Rotating",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	rotate := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/keys/"+rotateID+"/rotate", `{"grace":"1h"}`, map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if rotate.Code != http.StatusCreated {
		t.Fatalf("rotate status = %d, want %d; body = %s", rotate.Code, http.StatusCreated, rotate.Body.String())
	}
	replacementToken := apiKeyTokenFromResponse(t, rotate.Body.Bytes())
	for name, token := range map[string]string{"old": rotateToken, "replacement": replacementToken} {
		rec := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/state", "", map[string]string{
			"Authorization": "Bearer " + token,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("%s rotated token status = %d, want %d; body = %s", name, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	rateToken, _ := createAPIKeyThroughHTTP(t, server, `{
		"name": "Rate limited",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	results := make(chan *httptest.ResponseRecorder, 80)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 80 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/state", nil)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+rateToken)
			server.Handler().ServeHTTP(rec, req)
			results <- rec
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var limited *httptest.ResponseRecorder
	for rec := range results {
		if rec.Code == http.StatusTooManyRequests {
			limited = rec
			break
		}
	}
	if limited == nil {
		t.Fatalf("rate limit did not engage after repeated requests")
	}
	if limited.Header().Get("X-RateLimit-Limit") == "" || limited.Header().Get("X-RateLimit-Remaining") == "" || limited.Header().Get("Retry-After") == "" {
		t.Fatalf("rate limit headers missing: %#v", limited.Header())
	}
}

func TestAPIUsageLogRecordsReturnedHTTPErrorStatus(t *testing.T) {
	t.Parallel()

	server, backend, _, _ := newAPIKeyWorkItemTestServer(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	token, keyID := createAPIKeyThroughHTTP(t, server, `{
		"name": "Read client",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	rec := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/board/card?project=digitaldrywood-video&issue=missing", "", map[string]string{
		"Authorization": "Bearer " + token,
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("board card status = %d, want %d; body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	logs := waitForAPIUsageLogs(t, backend, keyID)
	if logs[0].StatusCode != int64(http.StatusNotFound) {
		t.Fatalf("usage log status = %d, want %d; log = %#v", logs[0].StatusCode, http.StatusNotFound, logs[0])
	}
}

func TestAPIUsageWritesDrainOnShutdown(t *testing.T) {
	t.Parallel()

	server, backend, _, _ := newAPIKeyWorkItemTestServer(t)
	token, keyID := createAPIKeyThroughHTTP(t, server, `{
		"name": "Shutdown drain",
		"scopes": ["read"],
		"expires_in": "90d"
	}`)
	rec := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/state", "", map[string]string{
		"Authorization": "Bearer " + token,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("state status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	queries := backend.Queries()
	if queries == nil {
		t.Fatal("store Queries() = nil")
	}
	logs, err := queries.ListAPIUsageLogsByKey(context.Background(), keyID)
	if err != nil {
		t.Fatalf("ListAPIUsageLogsByKey() error = %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("usage log count for %s = 0, want at least 1", keyID)
	}
	key, err := backend.APIKey(context.Background(), keyID)
	if err != nil {
		t.Fatalf("APIKey() error = %v", err)
	}
	if key.LastUsedAt == nil {
		t.Fatal("LastUsedAt = nil, want shutdown to drain last-used write")
	}
}

func TestDemoScenarioHeadersAreGatedToScreenshotsMode(t *testing.T) {
	t.Parallel()

	normal, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	body := requestHTMLWithHeaders(t, normal.Handler(), http.MethodGet, "/", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "fleet-healthy-parallel-work",
	})
	if !strings.Contains(body, "Detent") {
		t.Fatalf("normal server did not ignore demo scenario header:\n%s", body)
	}
	requestJSONWithHeaders(t, normal, http.MethodGet, "/api/v1/demo/scenarios", http.StatusNotFound, nil)

	demo, err := web.NewServer(web.Config{Demo: web.DemoConfig{Mode: "screenshots"}}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	payload := requestJSONWithHeaders(t, demo, http.MethodGet, "/", http.StatusNotFound, map[string]string{
		web.DemoScenarioHeader: "missing-scenario",
	})
	if nestedString(t, payload, "error", "code") != "demo_scenario_not_found" {
		t.Fatalf("unknown scenario payload = %#v", payload)
	}
}

func TestDemoScenarioManifestPagesAndAPIs(t *testing.T) {
	t.Parallel()

	backend := openWebTestStore(t)
	if err := demofixtures.SeedUsageEvents(context.Background(), backend); err != nil {
		t.Fatalf("SeedUsageEvents() error = %v", err)
	}
	deps := testDeps(t)
	deps.Store = backend
	server, err := web.NewServer(web.Config{Demo: web.DemoConfig{Mode: "screenshots"}}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	manifest := requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/demo/scenarios", http.StatusOK, nil)
	if manifest["header"] != web.DemoScenarioHeader || manifest["clock"] != "frozen" {
		t.Fatalf("manifest metadata = %#v", manifest)
	}
	assertManifestContainsScenarios(t, manifest, []string{
		"fleet-healthy-parallel-work",
		"fleet-kanban-multiproject",
		"kanban-full-integration",
		"reports-normal-window",
		"onboarding-write-success",
		"api-state-full-snapshot",
	})
	assertManifestOmitsScenarios(t, manifest, []string{"events-frozen", "events-play"})

	page := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "fleet-healthy-parallel-work",
	})
	for _, want := range []string{"Implement page-addressable screenshot scenarios", "detent-core", "GitHub quota", `id="agent-activity"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("fleet scenario page missing %q:\n%s", want, page)
		}
	}

	pausedProject := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/projects/mobile-client", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "project-paused-overview",
	})
	for _, want := range []string{"mobile-client is paused.", "Until issue closes: digitaldrywood/release-train#314", "Evaluation: evaluable via release-train"} {
		if !strings.Contains(pausedProject, want) {
			t.Fatalf("paused project scenario missing %q:\n%s", want, pausedProject)
		}
	}

	board := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "fleet-healthy-parallel-work",
	})
	for _, want := range []string{`id="board-lanes"`, "Implement page-addressable screenshot scenarios", "Dependency issue waiting on ledger migration"} {
		if !strings.Contains(board, want) {
			t.Fatalf("board scenario page missing %q:\n%s", want, board)
		}
	}
	if strings.Contains(board, `id="board-exceptions"`) || strings.Contains(board, "Dependency waiting") {
		t.Fatalf("board scenario should keep dependency waits off global alerts:\n%s", board)
	}

	alertsBoard := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "fleet-kanban-blocked-alerts",
	})
	for _, want := range []string{`id="board-exceptions"`, "Needs review", "after_create hook exited 2"} {
		if !strings.Contains(alertsBoard, want) {
			t.Fatalf("blocked-alerts board scenario missing %q:\n%s", want, alertsBoard)
		}
	}
	if strings.Contains(alertsBoard, "Dependency waiting") {
		t.Fatalf("blocked-alerts board scenario should not elevate dependency waits:\n%s", alertsBoard)
	}

	state := requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/state", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "fleet-healthy-parallel-work",
	})
	if state["status"] != "running" {
		t.Fatalf("state status = %#v, want running", state["status"])
	}
	counts := state["counts"].(map[string]any)
	if counts["running"] != float64(3) || counts["retrying"] != float64(3) || counts["ready"] != float64(5) || counts["waiting"] != float64(1) || counts["blocked"] != float64(1) {
		t.Fatalf("state counts = %#v", counts)
	}
	if _, ok := boardStateCountOK(t, state, "Cancelled"); ok {
		t.Fatalf("demo state includes Cancelled on the active board: %#v", state["board"])
	}
	cleanupEvents := state["events"].([]any)
	if len(cleanupEvents) == 0 || cleanupEvents[0].(map[string]any)["event"] != "workspace_reap_succeeded" {
		t.Fatalf("demo events = %#v, want cancellation cleanup event", cleanupEvents)
	}
	if !strings.Contains(cleanupEvents[0].(map[string]any)["message"].(string), "reason=cancelled") {
		t.Fatalf("demo cleanup event = %#v, want cancellation reason", cleanupEvents[0])
	}

	terminalKanban := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/projects/dogfood/kanban", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "kanban-terminal-states",
	})
	cancelledLane := regexp.MustCompile(`<section[^>]*data-board-lane="cancelled"[^>]*>`).FindString(terminalKanban)
	if cancelledLane == "" {
		t.Fatalf("terminal Kanban scenario missing Cancelled lane:\n%s", terminalKanban)
	}
	if !strings.Contains(cancelledLane, `data-lane-hidden="true"`) {
		t.Fatalf("populated Cancelled lane should hide by default: %s", cancelledLane)
	}
	if !strings.Contains(cancelledLane, `data-board-lane-default="false"`) {
		t.Fatalf("Cancelled lane should not be default-visible: %s", cancelledLane)
	}

	usage := requestJSONWithHeaders(t, server, http.MethodGet, "/api/v1/usage?by=project", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "api-usage-populated",
	})
	totals := usage["totals"].(map[string]any)
	if totals["events"].(float64) == 0 {
		t.Fatalf("usage totals = %#v, want seeded ledger events", totals)
	}
}

func TestDemoScenarioEventsPreserveProjectSubview(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{Demo: web.DemoConfig{Mode: "screenshots"}}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	addr := startWebServer(t, server)
	conn, reader := openRawEventStreamWithHeaders(t, addr, "/events?project=dogfood&view=kanban", map[string]string{
		web.DemoScenarioHeader: "kanban-full-integration",
	})

	buildEvent := readRawSSEFrame(t, conn, reader)
	if buildEvent.name != "build" {
		t.Fatalf("event name = %q, want build", buildEvent.name)
	}
	if !strings.Contains(buildEvent.data, `data-detent-build-version`) {
		t.Fatalf("demo build SSE event missing version marker:\n%s", buildEvent.data)
	}

	snapshotEvent := readRawSSEEvent(t, conn, reader)
	if snapshotEvent.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
	}
	for _, want := range []string{`id="board-lanes"`, `data-board-key="project.dogfood"`} {
		if !strings.Contains(snapshotEvent.data, want) {
			t.Fatalf("demo Kanban SSE event missing %q:\n%s", want, snapshotEvent.data)
		}
	}
	if strings.Contains(snapshotEvent.data, "Fleet grid") {
		t.Fatalf("demo Kanban SSE event rendered fleet snapshot:\n%s", snapshotEvent.data)
	}

	liveStatusEvent := readRawSSEEvent(t, conn, reader)
	if liveStatusEvent.name != "live-status" {
		t.Fatalf("event name = %q, want live-status", liveStatusEvent.name)
	}
	if !strings.Contains(liveStatusEvent.data, `data-detent-live-version`) {
		t.Fatalf("demo live-status SSE event missing version marker:\n%s", liveStatusEvent.data)
	}
}

func TestDemoScenarioKanbanFragments(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{Demo: web.DemoConfig{Mode: "screenshots"}}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/api/v1/kanban/move", http.StatusOK, map[string]string{
		web.DemoScenarioHeader: "api-kanban-move-read-only",
	})
	if !strings.Contains(body, "Kanban integration mode is not enabled") {
		t.Fatalf("read-only dialog body = %s", body)
	}

	rec := performDemoForm(t, server.Handler(), "/api/v1/kanban/move", "api-kanban-move-success", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Moved card to Todo") {
		t.Fatalf("move success status = %d body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#snapshot" {
		t.Fatalf("move success HX-Retarget = %q, want #snapshot", rec.Header().Get("HX-Retarget"))
	}
	if rec.Header().Get("HX-Reswap") != "morph:innerHTML" {
		t.Fatalf("move success HX-Reswap = %q, want morph:innerHTML", rec.Header().Get("HX-Reswap"))
	}
	moveBody := rec.Body.String()
	if !strings.Contains(moveBody, `id="board-lanes"`) {
		t.Fatalf("demo move success should return the redesigned board:\n%s", moveBody)
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Backlog observability fixture intake`).MatchString(moveBody) {
		t.Fatalf("demo Todo lane missing moved card:\n%s", moveBody)
	}

	rec = performDemoForm(t, server.Handler(), "/api/v1/kanban/comment", "api-kanban-comment-connector-failure", nil)
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "demo connector failure") {
		t.Fatalf("comment failure status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestSeedUsageEventsPopulatesUsageReports(t *testing.T) {
	t.Parallel()

	backend := openWebTestStore(t)
	ctx := context.Background()
	if err := demofixtures.SeedUsageEvents(ctx, backend); err != nil {
		t.Fatalf("SeedUsageEvents() error = %v", err)
	}
	report, err := backend.UsageReport(ctx, store.UsageReportQuery{By: store.UsageReportByProject})
	if err != nil {
		t.Fatalf("UsageReport() error = %v", err)
	}
	if report.Totals.Events == 0 || report.Totals.TotalTokens == 0 {
		t.Fatalf("usage totals = %#v, want seeded usage", report.Totals)
	}
	if len(report.Rows) < 3 {
		t.Fatalf("usage rows = %d, want multiple projects", len(report.Rows))
	}
}

func TestKanbanActionsRejectReadOnlyMode(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_kw1",
				Identifier: "digitaldrywood/detent#1",
				ProjectID:  "detent",
				Title:      "Read-only card",
				State:      "Todo",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent", http.StatusOK)
	if strings.Contains(body, "/api/v1/kanban/") || strings.Contains(body, `data-kanban-action="`) {
		t.Fatalf("read-only dashboard exposed Kanban mutation UI:\n%s", body)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"Todo"},
		"target_state":  {"In Progress"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if len(actionConnector.stateUpdates()) != 0 {
		t.Fatalf("state updates = %#v, want none", actionConnector.stateUpdates())
	}
}

func TestKanbanDialogFragmentRoutesRenderForms(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Dialog card",
			State:      "Todo",
			PullRequest: &telemetry.PullRequest{
				Number: 42,
				URL:    "https://github.com/digitaldrywood/frontend/pull/42",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name string
		path string
		want []string
	}{
		{
			name: "move",
			path: "/api/v1/kanban/move?project_id=detent&issue_id=I_kw1&current_state=Todo&target_state=In+Progress&pr_number=42&identifier=digitaldrywood%2Fdetent%231&issue_url=https%3A%2F%2Fgithub.com%2Fdigitaldrywood%2Fdetent%2Fissues%2F1&title=Dialog+card",
			want: []string{
				"Move card",
				`hx-post="/api/v1/kanban/move"`,
				`name="kanban_dialog" value="true"`,
				`name="target_state"`,
				`value="In Progress"`,
				`href="https://github.com/digitaldrywood/detent/issues/1"`,
				`name="issue_url" value="https://github.com/digitaldrywood/detent/issues/1"`,
			},
		},
		{
			name: "issue comment",
			path: "/api/v1/kanban/comment?project_id=detent&target=issue&issue_id=I_kw1&identifier=digitaldrywood%2Fdetent%231&issue_url=https%3A%2F%2Fgithub.com%2Fdigitaldrywood%2Fdetent%2Fissues%2F1&title=Dialog+card",
			want: []string{
				"Comment on issue",
				`hx-post="/api/v1/kanban/comment"`,
				`name="kanban_dialog" value="true"`,
				`name="target" value="issue"`,
				`href="https://github.com/digitaldrywood/detent/issues/1"`,
				`<textarea`,
			},
		},
		{
			name: "pull request comment",
			path: "/api/v1/kanban/comment?project_id=detent&target=pr&pr_repository=digitaldrywood%2Ffrontend&pr_number=42&pr_url=https%3A%2F%2Fgithub.com%2Fdigitaldrywood%2Ffrontend%2Fpull%2F42&identifier=digitaldrywood%2Fdetent%231&issue_url=https%3A%2F%2Fgithub.com%2Fdigitaldrywood%2Fdetent%2Fissues%2F1&title=Dialog+card",
			want: []string{
				"Comment on PR",
				`name="target" value="pr"`,
				`name="pr_repository" value="digitaldrywood/frontend"`,
				`name="pr_number" value="42"`,
				`href="https://github.com/digitaldrywood/frontend/pull/42"`,
				`name="pr_url" value="https://github.com/digitaldrywood/frontend/pull/42"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set("HX-Request", "true")
			server.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			for _, want := range tt.want {
				if !strings.Contains(rec.Body.String(), want) {
					t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
				}
			}
		})
	}
}

func TestKanbanMoveDialogUsesWorkflowAwareTargetDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		currentState       string
		targetState        string
		allowedTransitions map[string][]string
		wantSelected       string
	}{
		{
			name:         "backlog defaults to todo",
			currentState: "Backlog",
			allowedTransitions: map[string][]string{
				"Backlog": {"Blocked", "Todo"},
			},
			wantSelected: "Todo",
		},
		{
			name:         "todo defaults to in progress",
			currentState: "Todo",
			allowedTransitions: map[string][]string{
				"Todo": {"Backlog", "In Progress"},
			},
			wantSelected: "In Progress",
		},
		{
			name:         "blocked defaults to todo",
			currentState: "Blocked",
			allowedTransitions: map[string][]string{
				"Blocked": {"Cancelled", "Todo"},
			},
			wantSelected: "Todo",
		},
		{
			name:         "in progress defaults to human review",
			currentState: "In Progress",
			allowedTransitions: map[string][]string{
				"In Progress": {"Blocked", "Human Review"},
			},
			wantSelected: "Human Review",
		},
		{
			name:         "human review defaults to merging",
			currentState: "Human Review",
			allowedTransitions: map[string][]string{
				"Human Review": {"Blocked", "Merging"},
			},
			wantSelected: "Merging",
		},
		{
			name:         "rework defaults to in progress",
			currentState: "Rework",
			allowedTransitions: map[string][]string{
				"Rework": {"Done", "In Progress"},
			},
			wantSelected: "In Progress",
		},
		{
			name:         "preferred target falls back to first allowed",
			currentState: "Todo",
			allowedTransitions: map[string][]string{
				"Todo": {"Blocked", "Cancelled"},
			},
			wantSelected: "Blocked",
		},
		{
			name:         "explicit target wins",
			currentState: "Todo",
			targetState:  "Blocked",
			allowedTransitions: map[string][]string{
				"Todo": {"In Progress", "Blocked"},
			},
			wantSelected: "Blocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "github"}
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode:               workflowconfig.KanbanModeIntegration,
				AllowedTransitions: tt.allowedTransitions,
			}, actionConnector)
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			values := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_kw1"},
				"current_state": {tt.currentState},
				"identifier":    {"digitaldrywood/detent#1"},
				"title":         {"Default dialog card"},
			}
			if tt.targetState != "" {
				values.Set("target_state", tt.targetState)
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/kanban/move?"+values.Encode(), nil)
			req.Header.Set("HX-Request", "true")
			server.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			assertKanbanDialogSelectedTarget(t, rec.Body.String(), tt.wantSelected)
		})
	}
}

func TestKanbanDialogValidationErrorsRenderInsideDialog(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Dialog card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	moveForm := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", moveForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("move status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#kanban-dialog-content" {
		t.Fatalf("move HX-Retarget = %q, want #kanban-dialog-content", rec.Header().Get("HX-Retarget"))
	}
	for _, want := range []string{"Target state is required.", `hx-post="/api/v1/kanban/move"`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("move dialog body missing %q:\n%s", want, rec.Body.String())
		}
	}

	commentForm := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"target":        {"issue"},
		"issue_id":      {"I_kw1"},
	}
	rec = performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", commentForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("comment status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#kanban-dialog-content" {
		t.Fatalf("comment HX-Retarget = %q, want #kanban-dialog-content", rec.Header().Get("HX-Retarget"))
	}
	for _, want := range []string{"Comment body is required.", `hx-post="/api/v1/kanban/comment"`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("comment dialog body missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestKanbanMoveRoutesProjectV2AndIssueFieldUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		kanban      workflowconfig.Kanban
		targetState string
		wantState   []kanbanStateUpdate
		wantField   []kanbanIssueFieldUpdate
	}{
		{
			name:        "project v2 status",
			kanban:      workflowconfig.Kanban{Mode: workflowconfig.KanbanModeIntegration},
			targetState: "In Progress",
			wantState:   []kanbanStateUpdate{{issueID: "I_kw1", state: "In Progress"}},
		},
		{
			name: "issue field status",
			kanban: workflowconfig.Kanban{
				Mode:              workflowconfig.KanbanModeIntegration,
				IssueStateFieldID: 123,
			},
			targetState: "Human Review",
			wantField:   []kanbanIssueFieldUpdate{{issueID: "I_kw1", fieldID: 123, value: "In Review"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "github"}
			mustSetKanbanProject(t, deps.Registry, "detent", tt.kanban, actionConnector)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent"},
				Running: []telemetry.Running{{
					Issue: telemetry.Issue{
						ID:         "I_kw1",
						Identifier: "digitaldrywood/detent#1",
						ProjectID:  "detent",
						Title:      "Movable card",
						State:      "Todo",
					},
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			form := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_kw1"},
				"current_state": {"Todo"},
				"target_state":  {tt.targetState},
			}
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "Moved") {
				t.Fatalf("body missing success feedback: %s", rec.Body.String())
			}
			if got := actionConnector.stateUpdates(); !equalStateUpdates(got, tt.wantState) {
				t.Fatalf("state updates = %#v, want %#v", got, tt.wantState)
			}
			if got := actionConnector.issueFieldUpdates(); !equalIssueFieldUpdates(got, tt.wantField) {
				t.Fatalf("issue field updates = %#v, want %#v", got, tt.wantField)
			}
		})
	}
}

func TestKanbanMoveClosesOnlyLandedTerminalIssues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		issueStateFieldID int
		pullRequestState  string
		wantCloses        []string
	}{
		{name: "project status landed work", pullRequestState: "MERGED", wantCloses: []string{"I_kw1"}},
		{name: "issue field landed work", issueStateFieldID: 123, pullRequestState: "MERGED", wantCloses: []string{"I_kw1"}},
		{name: "terminal without landed work", pullRequestState: "OPEN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "github"}
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode:              workflowconfig.KanbanModeIntegration,
				IssueStateFieldID: tt.issueStateFieldID,
				AllowedTransitions: map[string][]string{
					"Merging": {"Done"},
				},
			}, actionConnector)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent"},
				Pipeline: []telemetry.Issue{{
					ID:         "I_kw1",
					Identifier: "digitaldrywood/detent#1",
					ProjectID:  "detent",
					Title:      "Terminal card",
					State:      "Merging",
					PullRequest: &telemetry.PullRequest{
						Number: 1,
						State:  tt.pullRequestState,
					},
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_kw1"},
				"current_state": {"Merging"},
				"target_state":  {"Done"},
			})
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if got := actionConnector.issueCloses(); !slices.Equal(got, tt.wantCloses) {
				t.Fatalf("CloseIssue() calls = %#v, want %#v", got, tt.wantCloses)
			}
		})
	}
}

func TestKanbanMoveReconcilesOperatorMoveAfterTrackerSuccess(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Blocked": {"Rework"},
		},
	}, actionConnector)
	probe := &operatorMoveProbe{result: orchestrator.OperatorMoveResult{Reconciled: true, BlockedCleared: true}}
	deps.OperatorMoves = probe
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Refresh:     telemetry.Refresh{DataSeq: 42},
		Blocked: []telemetry.Blocked{{
			Issue: telemetry.Issue{
				ID:         "I_operator_move",
				Identifier: "digitaldrywood/detent#1482",
				ProjectID:  "detent",
				Title:      "Operator move",
				State:      "Blocked",
			},
			Source: telemetry.BlockedSourceProjectStatus,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_operator_move"},
		"current_state": {"Blocked"},
		"target_state":  {"Rework"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	want := orchestrator.OperatorMoveRequest{
		ProjectID:  "detent",
		IssueID:    "I_operator_move",
		Identifier: "digitaldrywood/detent#1482",
		FromState:  "Blocked",
		ToState:    "Rework",
	}
	if probe.calls != 1 || probe.request != want {
		t.Fatalf("operator move reconciliation = calls %d request %#v, want one %#v", probe.calls, probe.request, want)
	}
}

func TestKanbanMoveRejectsMissingConnectorCapability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		form       url.Values
		wantStatus int
		wantBody   []string
	}{
		{
			name: "feedback",
			form: url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_linear1024"},
				"current_state": {"Todo"},
				"target_state":  {"In Progress"},
			},
			wantStatus: http.StatusForbidden,
			wantBody: []string{
				`id="kanban-feedback"`,
				"This project's tracker does not support moving cards.",
			},
		},
		{
			name: "dialog",
			form: url.Values{
				"kanban_dialog": {"true"},
				"project_id":    {"detent"},
				"issue_id":      {"I_linear1024"},
				"current_state": {"Todo"},
				"target_state":  {"In Progress"},
			},
			wantStatus: http.StatusOK,
			wantBody: []string{
				"This project's tracker does not support moving cards.",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "linear"}
			reporter := &kanbanCapabilityConnector{
				kanbanActionConnector: actionConnector,
				capabilities:          connector.Capabilities{CreateComment: true},
			}
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode: workflowconfig.KanbanModeIntegration,
				AllowedTransitions: map[string][]string{
					"Todo": {"In Progress"},
				},
			}, reporter)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent"},
				BoardIssues: []telemetry.Issue{{
					ID:         "I_linear1024",
					Identifier: "digitaldrywood/detent#1024",
					ProjectID:  "detent",
					Title:      "Linear move card",
					State:      "Todo",
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", tt.form)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			body := html.UnescapeString(rec.Body.String())
			for _, want := range tt.wantBody {
				if !strings.Contains(body, want) {
					t.Fatalf("body missing %q: %s", want, rec.Body.String())
				}
			}
			if got := actionConnector.stateUpdates(); len(got) != 0 {
				t.Fatalf("state updates = %#v, want none", got)
			}
		})
	}
}

func TestKanbanMoveDialogRejectsMissingConnectorCapability(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "linear"}
	reporter := &kanbanCapabilityConnector{
		kanbanActionConnector: actionConnector,
		capabilities:          connector.Capabilities{CreateComment: true},
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Todo": {"In Progress"},
		},
	}, reporter)
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	values := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_linear1024"},
		"current_state": {"Todo"},
		"target_state":  {"In Progress"},
	}
	body := requestHTML(t, server.Handler(), http.MethodGet, "/api/v1/kanban/move?"+values.Encode(), http.StatusOK)
	if !strings.Contains(html.UnescapeString(body), "This project's tracker does not support moving cards.") {
		t.Fatalf("body missing capability message:\n%s", body)
	}
	if strings.Contains(body, `hx-post="/api/v1/kanban/move"`) {
		t.Fatalf("move dialog rendered a move form despite missing capability:\n%s", body)
	}
	if got := actionConnector.stateUpdates(); len(got) != 0 {
		t.Fatalf("state updates = %#v, want none", got)
	}
}

func TestKanbanMoveSuccessResponseRefreshesProjectBoard(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	metrics := &workflowPhaseEventStoreProbe{}
	deps.Store = metrics
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw559",
			Identifier: "digitaldrywood/detent#559",
			ProjectID:  "detent",
			Title:      "Move regression card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw559"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#snapshot" {
		t.Fatalf("HX-Retarget = %q, want #snapshot", rec.Header().Get("HX-Retarget"))
	}
	if rec.Header().Get("HX-Reswap") != "morph:innerHTML" {
		t.Fatalf("HX-Reswap = %q, want morph:innerHTML", rec.Header().Get("HX-Reswap"))
	}
	for _, want := range []string{
		`id="board-lanes"`,
		"Moved card to Todo.",
		`data-board-lane="backlog"`,
		`data-board-lane="todo"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("response missing %q:\n%s", want, rec.Body.String())
		}
	}
	if got := strings.Count(rec.Body.String(), "Move regression card"); got != 2 {
		t.Fatalf("work item render count = %d, want Board and List representations:\n%s", got, rec.Body.String())
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Move regression card`).MatchString(rec.Body.String()) {
		t.Fatalf("Todo lane missing moved card:\n%s", rec.Body.String())
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw559", state: "Todo"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
	events := metrics.workflowPhaseEvents()
	if len(events) != 2 || events[0].Status != "exited" || events[1].Status != "entered" {
		t.Fatalf("workflow phase events = %#v, want lane exit and entry", events)
	}
	metadata, ok := provenance.Parse(events[1].MetadataJSON)
	if !ok || metadata.Provenance.Origin != provenance.OriginIndeterminate {
		t.Fatalf("lane entry metadata = %#v, want indeterminate origin without trusted request evidence", metadata)
	}
}

func TestKanbanPendingMoveSurvivesMissingProjectRefresh(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw922",
			Identifier: "digitaldrywood/detent#922",
			ProjectID:  "detent",
			Title:      "Pending refresh card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw922"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Pending refresh card`).MatchString(rec.Body.String()) {
		t.Fatalf("Todo lane missing moved card:\n%s", rec.Body.String())
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 1, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if got := strings.Count(body, "Pending refresh card"); got != 2 {
		t.Fatalf("pending work item render count = %d, want Board and List representations:\n%s", got, body)
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Pending refresh card`).MatchString(body) {
		t.Fatalf("Todo lane missing pending card after refresh:\n%s", body)
	}

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 2, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw922",
			Identifier: "digitaldrywood/detent#922",
			ProjectID:  "detent",
			Title:      "Tracker confirmed card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	body = requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Tracker confirmed card`).MatchString(body) {
		t.Fatalf("Todo lane missing tracker-confirmed card:\n%s", body)
	}

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 3, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw922",
			Identifier: "digitaldrywood/detent#922",
			ProjectID:  "detent",
			Title:      "Tracker source card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	body = requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if !regexp.MustCompile(`data-board-lane="backlog"[\s\S]*Tracker source card`).MatchString(body) {
		t.Fatalf("pending overlay did not clear after tracker target state:\n%s", body)
	}
}

func TestKanbanPendingMoveSurvivesAuthorizationFilteredSnapshot(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_auth922",
			Identifier: "digitaldrywood/detent#923",
			ProjectID:  "detent",
			Title:      "Authorization filtered card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_dialog": {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_auth922"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	filtered := orchestrator.State{
		PollInterval:  time.Minute,
		LastRefreshAt: time.Date(2026, 6, 15, 12, 1, 0, 0, time.UTC),
		Authorization: selector.Selector{
			Labels: selector.Labels{Include: []string{"authorized"}},
		},
		BoardIssues: []connector.Issue{{
			ID:         "I_auth922",
			Identifier: "digitaldrywood/detent#923",
			Title:      "Authorization filtered card",
			State:      "Todo",
			Labels:     []string{"detent:todo"},
		}},
	}
	if err := deps.Hub.Publish(filtered.Snapshot(time.Date(2026, 6, 15, 12, 1, 0, 0, time.UTC))); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if got := strings.Count(body, "Authorization filtered card"); got != 2 {
		t.Fatalf("pending work item render count = %d, want Board and List representations:\n%s", got, body)
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Authorization filtered card`).MatchString(body) {
		t.Fatalf("Todo lane missing authorization-filtered pending card:\n%s", body)
	}
}

func TestKanbanMoveFailureDoesNotInsertPendingCard(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, connectorProbe{name: "github"})
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_fail922",
			Identifier: "digitaldrywood/detent#924",
			ProjectID:  "detent",
			Title:      "Failed move card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_fail922"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
	if !strings.Contains(html.UnescapeString(rec.Body.String()), "This project's tracker does not support moving cards.") {
		t.Fatalf("body missing visible move error: %s", rec.Body.String())
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 1, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if strings.Contains(body, "Failed move card") {
		t.Fatalf("failed move inserted pending card:\n%s", body)
	}
}

func TestKanbanMoveBlockedStateUpdateReturnsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldID   int
		dialog    bool
		configure func(*kanbanActionConnector, error)
	}{
		{
			name: "project v2 status",
			configure: func(c *kanbanActionConnector, err error) {
				c.updateErr = err
			},
		},
		{
			name:    "issue field status",
			fieldID: 123,
			configure: func(c *kanbanActionConnector, err error) {
				c.setFieldErr = err
			},
		},
		{
			name:   "project v2 status dialog",
			dialog: true,
			configure: func(c *kanbanActionConnector, err error) {
				c.updateErr = err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			metrics := &workflowPhaseEventStoreProbe{}
			deps.Store = metrics
			blocked := &connector.StateUpdateBlockedError{
				IssueID:      "I_done1021",
				CurrentState: "Done",
				TargetState:  "Todo",
			}
			actionConnector := &kanbanActionConnector{name: "github"}
			tt.configure(actionConnector, blocked)
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode:              workflowconfig.KanbanModeIntegration,
				IssueStateFieldID: tt.fieldID,
				AllowedTransitions: map[string][]string{
					"Done": {"Todo"},
				},
			}, actionConnector)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Projects: []telemetry.ProjectSnapshot{
					{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
				},
				BoardIssues: []telemetry.Issue{{
					ID:         "I_done1021",
					Identifier: "digitaldrywood/detent#1021",
					ProjectID:  "detent",
					Title:      "Blocked terminal card",
					State:      "Done",
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			form := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_done1021"},
				"current_state": {"Done"},
				"target_state":  {"Todo"},
			}
			if tt.dialog {
				form.Set("kanban_dialog", "true")
			}
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			wantStatus := http.StatusUnprocessableEntity
			if tt.dialog {
				wantStatus = http.StatusOK
			}
			if rec.Code != wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, wantStatus, rec.Body.String())
			}
			if tt.dialog && rec.Header().Get("HX-Retarget") != "#kanban-dialog-content" {
				t.Fatalf("HX-Retarget = %q, want #kanban-dialog-content", rec.Header().Get("HX-Retarget"))
			}
			for _, want := range []string{
				"Move to Todo was refused",
				"digitaldrywood/detent#1021",
				"terminal state Done",
			} {
				if !strings.Contains(rec.Body.String(), want) {
					t.Fatalf("body missing %q: %s", want, rec.Body.String())
				}
			}
			if got := kanbanPendingStateCount(t, server); got != 0 {
				t.Fatalf("pending kanban states = %d, want 0", got)
			}
			if got := metrics.workflowPhaseEventCount(); got != 0 {
				t.Fatalf("workflow phase events = %d, want 0", got)
			}
		})
	}
}

func TestKanbanRemoveBlockedStateUpdateReturnsValidation(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	blocked := &connector.StateUpdateBlockedError{
		IssueID:      "I_remove1021",
		CurrentState: "Done",
		TargetState:  "Todo",
	}
	actionConnector := &kanbanActionConnector{name: "github", removeErr: blocked}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_remove1021",
			Identifier: "digitaldrywood/detent#1021",
			ProjectID:  "detent",
			Title:      "Blocked remove card",
			State:      "Done",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_remove1021"},
		"current_state": {"Done"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/remove", form)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	for _, want := range []string{
		"Move to Todo was refused",
		"digitaldrywood/detent#1021",
		"terminal state Done",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q: %s", want, rec.Body.String())
		}
	}
	if got := kanbanPendingRemovalCount(t, server); got != 0 {
		t.Fatalf("pending kanban removals = %d, want 0", got)
	}
}

func TestKanbanMoveAllowsFleetBoardMovePosts(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	mustSetWebProject(t, deps.Registry, "docs-site", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
			{Project: telemetry.Project{ID: "docs-site", DisplayName: "Docs Site"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw764",
				Identifier: "digitaldrywood/detent#764",
				ProjectID:  "detent",
				Title:      "Move fleet board card",
				State:      "Backlog",
			},
			{
				ID:         "I_docs12",
				Identifier: "digitaldrywood/docs-site#12",
				ProjectID:  "docs-site",
				Title:      "Keep read-only fleet card",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_dialog": {"true"},
		"kanban_board":  {"fleet"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw764"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := actionConnector.stateUpdates(); len(got) != 1 {
		t.Fatalf("state updates = %#v, want exactly one", got)
	}

	// A fleet move against a project without Kanban integration stays blocked
	// by the per-project gate.
	blocked := url.Values{
		"kanban_board":  {"fleet"},
		"project_id":    {"docs-site"},
		"issue_id":      {"I_docs12"},
		"current_state": {"Todo"},
		"target_state":  {"Backlog"},
	}
	blockedRec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", blocked)
	if blockedRec.Code != http.StatusForbidden {
		t.Fatalf("read-only project status = %d, want %d; body = %s", blockedRec.Code, http.StatusForbidden, blockedRec.Body.String())
	}
	if !strings.Contains(blockedRec.Body.String(), "Kanban integration mode is not enabled.") {
		t.Fatalf("read-only project body missing integration feedback:\n%s", blockedRec.Body.String())
	}
	if got := actionConnector.stateUpdates(); len(got) != 1 {
		t.Fatalf("state updates after blocked move = %#v, want still one", got)
	}
}

func TestKanbanRemoveSuccessResponseRefreshesProjectBoard(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw739",
			Identifier: "digitaldrywood/detent#739",
			ProjectID:  "detent",
			Title:      "Remove regression card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw739"},
		"current_state": {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/remove", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#snapshot" {
		t.Fatalf("HX-Retarget = %q, want #snapshot", rec.Header().Get("HX-Retarget"))
	}
	if rec.Header().Get("HX-Reswap") != "morph:innerHTML" {
		t.Fatalf("HX-Reswap = %q, want morph:innerHTML", rec.Header().Get("HX-Reswap"))
	}
	body := rec.Body.String()
	for _, want := range []string{
		`id="board-lanes"`,
		"Removed card from project.",
		`data-board-lane="todo"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Remove regression card") {
		t.Fatalf("response still contains removed card:\n%s", body)
	}
	if got, want := actionConnector.removals(), []kanbanRemoval{{issueID: "I_kw739"}}; !equalRemovals(got, want) {
		t.Fatalf("removals = %#v, want %#v", got, want)
	}
}

func TestKanbanRemoveClearsConfiguredIssueField(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode:              workflowconfig.KanbanModeIntegration,
		IssueStateFieldID: 123,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw741",
			Identifier: "digitaldrywood/detent#741",
			ProjectID:  "detent",
			Title:      "Issue field remove card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw741"},
		"current_state": {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/remove", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Removed card from project.") {
		t.Fatalf("body missing success feedback: %s", rec.Body.String())
	}
	if got, want := actionConnector.issueFieldClears(), []kanbanIssueFieldUpdate{{issueID: "I_kw741", fieldID: 123}}; !equalIssueFieldUpdates(got, want) {
		t.Fatalf("issue field clears = %#v, want %#v", got, want)
	}
	if got := actionConnector.removals(); len(got) != 0 {
		t.Fatalf("removals = %#v, want none", got)
	}
}

func TestKanbanRemoveRejectsMissingConnectorCapability(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "linear"}
	reporter := &kanbanCapabilityConnector{
		kanbanActionConnector: actionConnector,
		capabilities: connector.Capabilities{
			UpdateIssueState: true,
			CreateComment:    true,
		},
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, reporter)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_linear1024",
			Identifier: "digitaldrywood/detent#1024",
			ProjectID:  "detent",
			Title:      "Linear remove card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_linear1024"},
		"current_state": {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/remove", form)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if !strings.Contains(html.UnescapeString(rec.Body.String()), "This project's tracker does not support removing cards.") {
		t.Fatalf("body missing capability message: %s", rec.Body.String())
	}
	if got := actionConnector.removals(); len(got) != 0 {
		t.Fatalf("removals = %#v, want none", got)
	}
}

func TestKanbanRemoveReturnsVisibleErrorWhenUnsupported(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github", removeErr: connector.ErrNotImplemented}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw740",
			Identifier: "digitaldrywood/detent#740",
			ProjectID:  "detent",
			Title:      "Unsupported remove card",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw740"},
		"current_state": {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/remove", form)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
	if !strings.Contains(html.UnescapeString(rec.Body.String()), "This project's tracker does not support removing cards.") {
		t.Fatalf("body missing visible unsupported error: %s", rec.Body.String())
	}
}

func TestKanbanDragMoveSuccessResponseRefreshesProjectBoardWithoutInlineFlash(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw579",
			Identifier: "digitaldrywood/detent#579",
			ProjectID:  "detent",
			Title:      "Drag feedback card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_drag":   {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_kw579"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("HX-Retarget") != "#snapshot" {
		t.Fatalf("HX-Retarget = %q, want #snapshot", rec.Header().Get("HX-Retarget"))
	}
	if rec.Header().Get("HX-Reswap") != "morph:innerHTML" {
		t.Fatalf("HX-Reswap = %q, want morph:innerHTML", rec.Header().Get("HX-Reswap"))
	}
	body := rec.Body.String()
	if !strings.Contains(body, `id="board-lanes"`) {
		t.Fatalf("response missing project board:\n%s", body)
	}
	if got := strings.Count(body, "Drag feedback card"); got != 2 {
		t.Fatalf("work item render count = %d, want Board and List representations:\n%s", got, body)
	}
	if strings.Contains(body, "Moved card to Todo.") {
		t.Fatalf("drag success rendered inline success flash:\n%s", body)
	}
	if !regexp.MustCompile(`data-board-lane="todo"[\s\S]*Drag feedback card`).MatchString(body) {
		t.Fatalf("Todo lane missing moved card:\n%s", body)
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw579", state: "Todo"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanMoveUsesVisibleRuntimeStateForFreshness(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"In Progress": {"Blocked"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_runtime579",
			Identifier: "digitaldrywood/detent#580",
			ProjectID:  "detent",
			Title:      "Runtime freshness card",
			State:      "Backlog",
		}},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_runtime579",
				Identifier: "digitaldrywood/detent#580",
				ProjectID:  "detent",
				Title:      "Runtime freshness card",
				State:      "In Progress",
			},
			StartedAt: time.Date(2026, 6, 15, 11, 50, 0, 0, time.UTC),
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_drag":   {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_runtime579"},
		"current_state": {"In Progress"},
		"target_state":  {"Blocked"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if got := strings.Count(body, "Runtime freshness card"); got != 2 {
		t.Fatalf("work item render count = %d, want Board and List representations:\n%s", got, body)
	}
	if !regexp.MustCompile(`data-board-lane="blocked"[\s\S]*Runtime freshness card`).MatchString(body) {
		t.Fatalf("Blocked lane missing moved runtime card:\n%s", body)
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_runtime579", state: "Blocked"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanMoveUsesConfiguredBoardStateOverRawRuntimeFreshness(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Human Review": {"Blocked"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_runtime580",
			Identifier: "digitaldrywood/detent#581",
			ProjectID:  "detent",
			Title:      "Configured freshness card",
			State:      "Human Review",
		}},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_runtime580",
				Identifier: "digitaldrywood/detent#581",
				ProjectID:  "detent",
				Title:      "Configured freshness card",
				State:      "OPEN",
			},
			StartedAt: time.Date(2026, 6, 15, 11, 50, 0, 0, time.UTC),
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_drag":   {"true"},
		"project_id":    {"detent"},
		"issue_id":      {"I_runtime580"},
		"current_state": {"Human Review"},
		"target_state":  {"Blocked"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	if got := strings.Count(body, "Configured freshness card"); got != 2 {
		t.Fatalf("work item render count = %d, want Board and List representations:\n%s", got, body)
	}
	if !regexp.MustCompile(`data-board-lane="blocked"[\s\S]*Configured freshness card`).MatchString(body) {
		t.Fatalf("Blocked lane missing moved configured card:\n%s", body)
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_runtime580", state: "Blocked"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanPendingOverlayDoesNotMutateLatestSnapshot(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	deps.Connector = actionConnector
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		BoardIssues: []telemetry.Issue{{
			ID:         "I_kw560",
			Identifier: "digitaldrywood/detent#560",
			Title:      "Global pending card",
			State:      "Backlog",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		Kanban: workflowconfig.Kanban{
			Mode: workflowconfig.KanbanModeIntegration,
			AllowedTransitions: map[string][]string{
				"Backlog": {"Todo"},
			},
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"issue_id":      {"I_kw560"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	requestHTML(t, server.Handler(), http.MethodGet, "/", http.StatusOK)

	latest, ok := deps.Hub.Latest()
	if !ok {
		t.Fatal("Hub.Latest() = false, want published snapshot")
	}
	if got := latest.BoardIssues[0].State; got != "Backlog" {
		t.Fatalf("latest BoardIssues[0].State = %q, want Backlog", got)
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw560", state: "Todo"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanMoveRejectsDefaultRestrictedActiveTransitions(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "I_progress",
					Identifier: "digitaldrywood/detent#1",
					ProjectID:  "detent",
					Title:      "Active card",
					State:      "In Progress",
				},
			},
			{
				Issue: telemetry.Issue{
					ID:         "I_rework",
					Identifier: "digitaldrywood/detent#2",
					ProjectID:  "detent",
					Title:      "Rework card",
					State:      "Rework",
				},
			},
			{
				Issue: telemetry.Issue{
					ID:         "I_merging",
					Identifier: "digitaldrywood/detent#3",
					ProjectID:  "detent",
					Title:      "Merging card",
					State:      "Merging",
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rejections := []struct {
		name    string
		issueID string
		source  string
		target  string
	}{
		{
			name:    "in progress to human review",
			issueID: "I_progress",
			source:  "In Progress",
			target:  "Human Review",
		},
		{
			name:    "rework to done",
			issueID: "I_rework",
			source:  "Rework",
			target:  "Done",
		},
		{
			name:    "merging to done",
			issueID: "I_merging",
			source:  "Merging",
			target:  "Done",
		},
	}
	for _, tt := range rejections {
		t.Run(tt.name, func(t *testing.T) {
			form := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {tt.issueID},
				"current_state": {tt.source},
				"target_state":  {tt.target},
			}
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "transition policy") {
				t.Fatalf("body missing transition policy feedback: %s", rec.Body.String())
			}
		})
	}
	if got := actionConnector.stateUpdates(); len(got) != 0 {
		t.Fatalf("state updates = %#v, want none before allowed move", got)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_progress"},
		"current_state": {"In Progress"},
		"target_state":  {"Blocked"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_progress", state: "Blocked"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanMoveAllowsConfiguredTransitionOverrides(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"In Progress": {"Human Review"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_kw1",
				Identifier: "digitaldrywood/detent#1",
				ProjectID:  "detent",
				Title:      "Override card",
				State:      "In Progress",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"In Progress"},
		"target_state":  {"Human Review"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw1", state: "Human Review"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanActionsRouteCommentsToIssuesAndPullRequests(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Commentable issue",
			State:      "Todo",
			PullRequest: &telemetry.PullRequest{
				Number: 42,
				URL:    "https://github.com/digitaldrywood/frontend/pull/42",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	issueForm := url.Values{
		"project_id": {"detent"},
		"target":     {"issue"},
		"issue_id":   {"I_kw1"},
		"body":       {"Issue note"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", issueForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("issue comment status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	prForm := url.Values{
		"project_id":    {"detent"},
		"target":        {"pr"},
		"pr_repository": {"digitaldrywood/frontend"},
		"pr_number":     {"42"},
		"body":          {"PR note"},
	}
	rec = performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", prForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("pr comment status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if got, want := actionConnector.comments(), []kanbanComment{{issueID: "I_kw1", body: "Issue note"}}; !equalComments(got, want) {
		t.Fatalf("comments = %#v, want %#v", got, want)
	}
	if got, want := actionConnector.prComments(), []kanbanPRComment{{repository: "digitaldrywood/frontend", number: 42, body: "PR note"}}; !equalPRComments(got, want) {
		t.Fatalf("pr comments = %#v, want %#v", got, want)
	}
}

func TestKanbanCommentEditAndDeleteLocalComments(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	createdAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	actionConnector := &kanbanActionConnector{
		name: "local_sqlite",
		issueComments: map[string][]connector.IssueComment{
			"I_kw1": {{
				ID:          "1",
				Backend:     connector.BackendLocalSQLite.String(),
				Body:        "Original local note",
				AuthorLogin: "detent",
				CreatedAt:   &createdAt,
				Local:       true,
				CanEdit:     true,
				CanDelete:   true,
				TargetType:  connector.IssueCommentTargetIssue,
			}},
		},
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 6, 12, 1, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Refresh:     telemetry.Refresh{Status: telemetry.RefreshStatusReady},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Editable comment issue",
			State:      "Todo",
			Comments: []telemetry.IssueComment{{
				ID:          "1",
				Backend:     connector.BackendLocalSQLite.String(),
				Body:        "Original local note",
				AuthorLogin: "detent",
				CreatedAt:   &createdAt,
				Local:       true,
				CanEdit:     true,
				CanDelete:   true,
				TargetType:  connector.IssueCommentTargetIssue,
			}},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	editForm := url.Values{
		"project_id": {"detent"},
		"issue_id":   {"I_kw1"},
		"comment_id": {"1"},
		"body":       {"Updated local note"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment/edit", editForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("edit status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "Updated local note") || strings.Contains(body, "Original local note") {
		t.Fatalf("edit response did not render updated thread: %s", body)
	}
	if got, want := actionConnector.commentUpdates(), []kanbanCommentEdit{{issueID: "I_kw1", commentID: "1", body: "Updated local note"}}; !equalCommentUpdates(got, want) {
		t.Fatalf("comment updates = %#v, want %#v", got, want)
	}

	deleteForm := url.Values{
		"project_id": {"detent"},
		"issue_id":   {"I_kw1"},
		"comment_id": {"1"},
	}
	rec = performForm(t, server.Handler(), http.MethodDelete, "/api/v1/kanban/comment?project_id=detent&issue_id=I_kw1&comment_id=1", deleteForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "Updated local note") {
		t.Fatalf("delete response still rendered deleted comment: %s", rec.Body.String())
	}
	if got, want := actionConnector.commentRemovals(), []kanbanCommentDelete{{issueID: "I_kw1", commentID: "1"}}; !equalCommentDeletes(got, want) {
		t.Fatalf("comment removals = %#v, want %#v", got, want)
	}
}

func TestKanbanThreadCommentRefreshesIssueComments(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Commentable issue",
			State:      "Todo",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_thread":  {"true"},
		"project_id":     {"detent"},
		"kanban_board":   {"project"},
		"board_actions":  {"true"},
		"target":         {"issue"},
		"issue_id":       {"I_kw1"},
		"identifier":     {"digitaldrywood/detent#1"},
		"issue_identity": {"digitaldrywood/detent#1"},
		"title":          {"Commentable issue"},
		"body":           {"Fresh issue note"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := actionConnector.comments(), []kanbanComment{{issueID: "I_kw1", body: "Fresh issue note"}}; !equalComments(got, want) {
		t.Fatalf("comments = %#v, want %#v", got, want)
	}
	for _, want := range []string{
		`id="kanban-issue-comments-panel"`,
		"Fresh issue note",
		"Comment submitted.",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("thread response missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestKanbanFleetThreadCommentRoutesToOwningProject(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	fleetConnector := &kanbanActionConnector{name: "fleet"}
	projectConnector := &kanbanActionConnector{name: "github"}
	deps.Connector = fleetConnector
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, projectConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "I_fleet_comment",
			Identifier: "digitaldrywood/detent#953",
			ProjectID:  "detent",
			Title:      "Fleet commentable issue",
			State:      "Todo",
			PullRequest: &telemetry.PullRequest{
				Number: 42,
				URL:    "https://github.com/digitaldrywood/frontend/pull/42",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"kanban_thread":  {"true"},
		"project_id":     {"detent"},
		"kanban_board":   {"fleet"},
		"board_actions":  {"true"},
		"target":         {"issue"},
		"issue_id":       {"I_fleet_comment"},
		"identifier":     {"digitaldrywood/detent#953"},
		"issue_identity": {"digitaldrywood/detent#953"},
		"title":          {"Fleet commentable issue"},
		"body":           {"Fleet issue note"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := projectConnector.comments(), []kanbanComment{{issueID: "I_fleet_comment", body: "Fleet issue note"}}; !equalComments(got, want) {
		t.Fatalf("project comments = %#v, want %#v", got, want)
	}
	if got := fleetConnector.comments(); len(got) != 0 {
		t.Fatalf("fleet connector comments = %#v, want none", got)
	}
	for _, want := range []string{
		`id="kanban-issue-comments-panel"`,
		"Fleet issue note",
		"Comment submitted.",
		`name="kanban_board" value="fleet"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("thread response missing %q:\n%s", want, rec.Body.String())
		}
	}

	prForm := url.Values{
		"project_id":    {"detent"},
		"kanban_board":  {"fleet"},
		"target":        {"pr"},
		"pr_repository": {"digitaldrywood/frontend"},
		"pr_number":     {"42"},
		"body":          {"Fleet PR note"},
	}
	rec = performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", prForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("pr status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got, want := projectConnector.prComments(), []kanbanPRComment{{repository: "digitaldrywood/frontend", number: 42, body: "Fleet PR note"}}; !equalPRComments(got, want) {
		t.Fatalf("project PR comments = %#v, want %#v", got, want)
	}
	if got := fleetConnector.prComments(); len(got) != 0 {
		t.Fatalf("fleet connector PR comments = %#v, want none", got)
	}
}

func TestBoardCardSheetShowsLocalCommentControlsOnly(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	createdAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	comments := []telemetry.IssueComment{
		{
			ID:          "remote-1",
			Backend:     connector.BackendGitHub.String(),
			Body:        "Remote note",
			AuthorLogin: "octocat",
			CreatedAt:   &createdAt,
			TargetType:  connector.IssueCommentTargetIssue,
		},
		{
			ID:          "local-1",
			Backend:     connector.BackendLocalSQLite.String(),
			Body:        "Local note",
			AuthorLogin: "detent",
			CreatedAt:   &createdAt,
			Local:       true,
			CanEdit:     true,
			CanDelete:   true,
			TargetType:  connector.IssueCommentTargetIssue,
		},
	}
	actionConnector := &kanbanActionConnector{
		name: "local_sqlite",
		issueComments: map[string][]connector.IssueComment{
			"I_kw1": {
				{
					ID:          "remote-1",
					Backend:     connector.BackendGitHub.String(),
					Body:        "Remote note",
					AuthorLogin: "octocat",
					CreatedAt:   &createdAt,
					TargetType:  connector.IssueCommentTargetIssue,
				},
				{
					ID:          "local-1",
					Backend:     connector.BackendLocalSQLite.String(),
					Body:        "Local note",
					AuthorLogin: "detent",
					CreatedAt:   &createdAt,
					Local:       true,
					CanEdit:     true,
					CanDelete:   true,
					TargetType:  connector.IssueCommentTargetIssue,
				},
			},
		},
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 6, 12, 1, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Refresh:     telemetry.Refresh{Status: telemetry.RefreshStatusReady},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Comment controls issue",
			State:      "Todo",
			Comments:   comments,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/api/v1/board/card?project=detent&issue=I_kw1&scope=project&actions=board", http.StatusOK)
	if !strings.Contains(body, "Loading issue comments") {
		t.Fatalf("body missing pending issue comments: %s", body)
	}
	body = requestHTML(t, server.Handler(), http.MethodGet, "/api/v1/board/conversation?project=detent&issue=I_kw1&scope=project&actions=board&target=issue", http.StatusOK)
	for _, want := range []string{"Remote note", "Local note"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
	if got := strings.Count(body, `hx-post="/api/v1/kanban/comment/edit"`); got != 1 {
		t.Fatalf("edit controls = %d, want 1 in body: %s", got, body)
	}
	if got := strings.Count(body, `hx-delete="/api/v1/kanban/comment?`); got != 1 {
		t.Fatalf("delete controls = %d, want 1 in body: %s", got, body)
	}
}

func TestKanbanActionsHidePullRequestCommentsWhenUnsupported(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, connectorProbe{name: "linear"})
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{{
			ID:         "LIN-123",
			Identifier: "LIN-123",
			ProjectID:  "detent",
			Title:      "Linear commentable issue",
			State:      "Todo",
			PullRequest: &telemetry.PullRequest{
				Number: 42,
				URL:    "https://github.com/digitaldrywood/frontend/pull/42",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTMLWithHeaders(t, server.Handler(), http.MethodGet, "/api/v1/board/card?project=detent&issue=LIN-123&scope=project&actions=board", http.StatusOK, map[string]string{
		"HX-Request": "true",
	})
	if !strings.Contains(body, "Comment on issue") {
		t.Fatalf("card sheet missing issue comment action:\n%s", body)
	}
	if strings.Contains(body, "Comment on PR") {
		t.Fatalf("card sheet rendered unsupported PR comment action:\n%s", body)
	}

	prForm := url.Values{
		"project_id":    {"detent"},
		"target":        {"pr"},
		"pr_repository": {"digitaldrywood/frontend"},
		"pr_number":     {"42"},
		"body":          {"Unsupported PR note"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", prForm)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Comment target is not available on the current board.") {
		t.Fatalf("body missing unsupported target message: %s", rec.Body.String())
	}
}

func TestKanbanCommentRejectsTargetsOutsideCurrentBoard(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{{
			ID:         "I_kw1",
			Identifier: "digitaldrywood/detent#1",
			ProjectID:  "detent",
			Title:      "Commentable issue",
			State:      "Todo",
			PullRequest: &telemetry.PullRequest{
				Number: 42,
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name string
		form url.Values
	}{
		{
			name: "unknown issue",
			form: url.Values{
				"project_id": {"detent"},
				"target":     {"issue"},
				"issue_id":   {"I_hidden"},
				"body":       {"Hidden issue note"},
			},
		},
		{
			name: "unknown pull request",
			form: url.Values{
				"project_id":    {"detent"},
				"target":        {"pr"},
				"pr_repository": {"digitaldrywood/detent"},
				"pr_number":     {"99"},
				"body":          {"Hidden PR note"},
			},
		},
		{
			name: "wrong pull request repository",
			form: url.Values{
				"project_id":    {"detent"},
				"target":        {"pr"},
				"pr_repository": {"other/repo"},
				"pr_number":     {"42"},
				"body":          {"Wrong repo PR note"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/comment", tt.form)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
			}
		})
	}
	if len(actionConnector.comments()) != 0 {
		t.Fatalf("comments = %#v, want none", actionConnector.comments())
	}
	if len(actionConnector.prComments()) != 0 {
		t.Fatalf("pr comments = %#v, want none", actionConnector.prComments())
	}
}

func TestKanbanMoveSerializesMutationsPerProject(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	actionConnector := &kanbanActionConnector{
		name:        "github",
		moveStarted: started,
		releaseMove: release,
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_kw1",
				Identifier: "digitaldrywood/detent#1",
				ProjectID:  "detent",
				Title:      "Serialized card",
				State:      "Todo",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"Todo"},
		"target_state":  {"In Progress"},
	}
	results := make(chan int, 2)
	for range 2 {
		go func() {
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			results <- rec.Code
		}()
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first mutation did not start")
	}
	select {
	case <-started:
		t.Fatal("second mutation started before first completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	statuses := map[int]int{}
	for range 2 {
		select {
		case status := <-results:
			statuses[status]++
		case <-time.After(time.Second):
			t.Fatal("mutation request did not finish")
		}
	}
	if statuses[http.StatusOK] != 1 || statuses[http.StatusConflict] != 1 {
		t.Fatalf("statuses = %#v, want one OK and one conflict", statuses)
	}
	if got := actionConnector.maxActiveMoves(); got != 1 {
		t.Fatalf("max active moves = %d, want 1", got)
	}
	if got := actionConnector.stateUpdates(); len(got) != 1 {
		t.Fatalf("state updates len = %d, want 1; updates = %#v", len(got), got)
	}
}

func TestKanbanMoveRejectsConcurrentTransitionFromStaleSource(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	actionConnector := &kanbanActionConnector{
		name:        "github",
		moveStarted: started,
		releaseMove: release,
	}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"In Progress": {"Blocked", "Human Review"},
			"Blocked":     {"Cancelled"},
		},
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "I_kw1",
				Identifier: "digitaldrywood/detent#1",
				ProjectID:  "detent",
				Title:      "Serialized card",
				State:      "In Progress",
			},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	firstForm := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"In Progress"},
		"target_state":  {"Blocked"},
	}
	secondForm := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"In Progress"},
		"target_state":  {"Human Review"},
	}
	results := make(chan int, 2)
	go func() {
		rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", firstForm)
		results <- rec.Code
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first mutation did not start")
	}

	go func() {
		rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", secondForm)
		results <- rec.Code
	}()
	select {
	case <-started:
		t.Fatal("second mutation started before first completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	statuses := map[int]int{}
	for range 2 {
		select {
		case status := <-results:
			statuses[status]++
		case <-time.After(time.Second):
			t.Fatal("mutation request did not finish")
		}
	}
	if statuses[http.StatusOK] != 1 || statuses[http.StatusConflict] != 1 {
		t.Fatalf("statuses = %#v, want one OK and one conflict", statuses)
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw1", state: "Blocked"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestKanbanMoveUsesLiveStateTransitionForStaleCard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transitions map[string][]string
		wantStatus  int
		wantUpdates []kanbanStateUpdate
	}{
		{
			name: "live transition allowed",
			transitions: map[string][]string{
				"Todo": {"In Progress"},
			},
			wantStatus:  http.StatusOK,
			wantUpdates: []kanbanStateUpdate{{issueID: "I_stale1096", state: "In Progress"}},
		},
		{
			name: "live transition disallowed",
			transitions: map[string][]string{
				"Todo": {"Blocked"},
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "github"}
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode:               workflowconfig.KanbanModeIntegration,
				AllowedTransitions: tt.transitions,
			}, actionConnector)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent"},
				Refresh:     telemetry.Refresh{DataSeq: 41},
				Pipeline: []telemetry.Issue{{
					ID:         "I_stale1096",
					Identifier: "digitaldrywood/detent#1096",
					ProjectID:  "detent",
					Title:      "Advanced under cursor",
					State:      "Todo",
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			form := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_stale1096"},
				"current_state": {"Backlog"},
				"target_state":  {"In Progress"},
			}
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := actionConnector.stateUpdates(); !equalStateUpdates(got, tt.wantUpdates) {
				t.Fatalf("state updates = %#v, want %#v", got, tt.wantUpdates)
			}
		})
	}
}

func TestKanbanMoveLogsOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		currentState   string
		transitions    map[string][]string
		connectorError error
		wantStatus     int
		wantLevel      string
		wantMessage    string
		wantCurrent    string
		wantLive       string
	}{
		{
			name:         "success",
			currentState: "Todo",
			transitions: map[string][]string{
				"Todo": {"In Progress"},
			},
			wantStatus:  http.StatusOK,
			wantLevel:   "INFO",
			wantMessage: "kanban move succeeded",
			wantCurrent: "Todo",
		},
		{
			name:         "stale allowed",
			currentState: "Backlog",
			transitions: map[string][]string{
				"Todo": {"In Progress"},
			},
			wantStatus:  http.StatusOK,
			wantLevel:   "INFO",
			wantMessage: "kanban move succeeded",
			wantCurrent: "Todo",
		},
		{
			name:         "stale disallowed",
			currentState: "Backlog",
			transitions: map[string][]string{
				"Todo": {"Blocked"},
			},
			wantStatus:  http.StatusConflict,
			wantLevel:   "WARN",
			wantMessage: "kanban move rejected: stale card",
			wantCurrent: "Backlog",
			wantLive:    "Todo",
		},
		{
			name:         "transition blocked",
			currentState: "Todo",
			transitions: map[string][]string{
				"Todo": {"Blocked"},
			},
			wantStatus:  http.StatusUnprocessableEntity,
			wantLevel:   "WARN",
			wantMessage: "kanban move blocked by transition policy",
			wantCurrent: "Todo",
		},
		{
			name:         "connector blocked",
			currentState: "Todo",
			transitions: map[string][]string{
				"Todo": {"In Progress"},
			},
			connectorError: &connector.StateUpdateBlockedError{
				IssueID:      "I_log1096",
				CurrentState: "Todo",
				TargetState:  "In Progress",
			},
			wantStatus:  http.StatusUnprocessableEntity,
			wantLevel:   "WARN",
			wantMessage: "kanban move blocked by connector",
			wantCurrent: "Todo",
		},
		{
			name:           "connector error",
			currentState:   "Todo",
			connectorError: errors.New("tracker unavailable"),
			transitions: map[string][]string{
				"Todo": {"In Progress"},
			},
			wantStatus:  http.StatusBadGateway,
			wantLevel:   "WARN",
			wantMessage: "kanban move failed",
			wantCurrent: "Todo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer
			deps := testDeps(t)
			actionConnector := &kanbanActionConnector{name: "github", updateErr: tt.connectorError}
			mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
				Mode:               workflowconfig.KanbanModeIntegration,
				AllowedTransitions: tt.transitions,
			}, actionConnector)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent"},
				Refresh:     telemetry.Refresh{DataSeq: 41},
				Pipeline: []telemetry.Issue{{
					ID:         "I_log1096",
					Identifier: "digitaldrywood/detent#1096",
					ProjectID:  "detent",
					Title:      "Logged move",
					State:      "Todo",
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{
				Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
			}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			form := url.Values{
				"project_id":    {"detent"},
				"issue_id":      {"I_log1096"},
				"current_state": {tt.currentState},
				"target_state":  {"In Progress"},
			}
			rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			records := kanbanMoveLogRecords(t, logs.Bytes())
			if len(records) != 1 {
				t.Fatalf("kanban move log records = %#v, want exactly one", records)
			}
			record := records[0]
			assertJSONLogField(t, record, "level", tt.wantLevel)
			assertJSONLogField(t, record, "msg", tt.wantMessage)
			assertJSONLogField(t, record, "project", "detent")
			assertJSONLogField(t, record, "issue_id", "I_log1096")
			assertJSONLogField(t, record, "identifier", "digitaldrywood/detent#1096")
			assertJSONLogField(t, record, "current_state", tt.wantCurrent)
			assertJSONLogField(t, record, "target_state", "In Progress")
			assertJSONLogField(t, record, "data_seq", float64(41))
			if tt.wantLive != "" {
				assertJSONLogField(t, record, "live_state", tt.wantLive)
			}
		})
	}
}

func TestKanbanMoveLogsOverlayRevert(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Backlog": {"Todo"},
		},
	}, actionConnector)
	publish := func(dataSeq uint64) {
		t.Helper()
		if err := deps.Hub.Publish(telemetry.Snapshot{
			GeneratedAt: time.Date(2026, 7, 9, 12, int(dataSeq), 0, 0, time.UTC),
			Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
			Projects: []telemetry.ProjectSnapshot{{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Refresh: telemetry.Refresh{DataSeq: dataSeq},
			}},
			Refresh: telemetry.Refresh{DataSeq: dataSeq},
			BoardIssues: []telemetry.Issue{{
				ID:         "I_revert1096",
				Identifier: "digitaldrywood/detent#1096",
				ProjectID:  "detent",
				Title:      "Reverted move",
				State:      "Backlog",
			}},
		}); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}
	publish(7)
	server, err := web.NewServer(web.Config{
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_revert1096"},
		"current_state": {"Backlog"},
		"target_state":  {"Todo"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	publish(8)
	requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	publish(9)
	requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)

	records := kanbanMoveLogRecords(t, logs.Bytes())
	var reverts []map[string]any
	for _, record := range records {
		if record["msg"] == "kanban move reverted" {
			reverts = append(reverts, record)
		}
	}
	if len(reverts) != 1 {
		t.Fatalf("kanban revert log records = %#v, want exactly one", reverts)
	}
	record := reverts[0]
	assertJSONLogField(t, record, "level", "WARN")
	assertJSONLogField(t, record, "project", "detent")
	assertJSONLogField(t, record, "issue_id", "I_revert1096")
	assertJSONLogField(t, record, "identifier", "digitaldrywood/detent#1096")
	assertJSONLogField(t, record, "from_state", "Todo")
	assertJSONLogField(t, record, "to_state", "Backlog")
	assertJSONLogField(t, record, "data_seq", float64(9))
	assertJSONLogField(t, record, "contradiction_count", float64(2))
}

func TestKanbanMoveAllowsStaleAndRejectsPROnlyCards(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{name: "github"}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
		Project:     telemetry.Project{ID: "detent"},
		Pipeline: []telemetry.Issue{
			{
				ID:         "I_kw1",
				Identifier: "digitaldrywood/detent#1",
				ProjectID:  "detent",
				Title:      "Stale card",
				State:      "Todo",
			},
			{
				Identifier: "digitaldrywood/detent#2",
				ProjectID:  "detent",
				Title:      "PR-only card",
				State:      "Human Review",
				PullRequest: &telemetry.PullRequest{
					Number: 42,
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	staleForm := url.Values{
		"project_id":    {"detent"},
		"issue_id":      {"I_kw1"},
		"current_state": {"Backlog"},
		"target_state":  {"In Progress"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", staleForm)
	if rec.Code != http.StatusOK {
		t.Fatalf("stale status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Moved") {
		t.Fatalf("stale-allowed body missing success feedback: %s", rec.Body.String())
	}

	prOnlyForm := url.Values{
		"project_id":    {"detent"},
		"current_state": {"Human Review"},
		"target_state":  {"Merging"},
		"pr_number":     {"42"},
	}
	rec = performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", prOnlyForm)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("pr-only status = %d, want %d; body = %s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	if got, want := actionConnector.stateUpdates(), []kanbanStateUpdate{{issueID: "I_kw1", state: "In Progress"}}; !equalStateUpdates(got, want) {
		t.Fatalf("state updates = %#v, want %#v", got, want)
	}
}

func TestServerRendersInstanceNameInPagesStateAndMetadata(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC),
		Instance: telemetry.Instance{
			Name:        "worker-identity",
			GitHubLogin: "detent-bot",
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	cfg := web.Config{
		GlobalConfig: globalconfig.Config{InstanceName: "buildbox"},
		Hostname: func() (string, error) {
			return "fallback.example.com", nil
		},
	}
	server, err := web.NewServer(cfg, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	onboardingServer, err := web.NewServer(web.Config{
		Mode:         web.ModeOnboarding,
		GlobalConfig: cfg.GlobalConfig,
		Hostname:     cfg.Hostname,
	}, web.Dependencies{})
	if err != nil {
		t.Fatalf("NewServer() onboarding error = %v", err)
	}

	tests := []struct {
		name      string
		handler   http.Handler
		path      string
		title     string
		wantBadge bool
	}{
		{name: "dashboard", handler: server.Handler(), path: "/fleet", title: "buildbox · Detent"},
		{name: "analytics", handler: server.Handler(), path: "/analytics", title: "buildbox · Analytics - Detent"},
		{name: "reports", handler: server.Handler(), path: "/reports", title: "buildbox · Detent reports"},
		{name: "settings", handler: server.Handler(), path: "/settings", title: "buildbox · Detent settings"},
		{name: "onboarding", handler: onboardingServer.Handler(), path: "/onboarding", title: "buildbox · Detent onboarding", wantBadge: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := requestHTML(t, tt.handler, http.MethodGet, tt.path, http.StatusOK)
			wants := []string{
				"<title>" + tt.title + "</title>",
				`name="application-name" content="buildbox · Detent"`,
			}
			if tt.wantBadge {
				wants = append(wants, `aria-label="Instance name"`, ">buildbox</span>")
			}
			for _, want := range wants {
				if !strings.Contains(body, want) {
					t.Fatalf("%s body missing %q:\n%s", tt.path, want, body)
				}
			}
		})
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	instance := state["instance"].(map[string]any)
	if instance["display_name"] != "buildbox" {
		t.Fatalf("instance.display_name = %#v, want buildbox", instance["display_name"])
	}
	if instance["name"] != "worker-identity" {
		t.Fatalf("instance.name = %#v, want worker-identity", instance["name"])
	}
}

func TestAPIStateSurfacesProjectAuthHealth(t *testing.T) {
	t.Parallel()

	failedAt := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: failedAt,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "detent"},
				Auth: telemetry.AuthHealth{
					Status:      telemetry.AuthStatusStale,
					LastError:   "github authentication failed: status 401",
					LastErrorAt: &failedAt,
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	projects := state["projects"].([]any)
	project := projects[0].(map[string]any)
	auth := project["auth"].(map[string]any)
	if auth["status"] != string(telemetry.AuthStatusStale) {
		t.Fatalf("auth.status = %#v", auth["status"])
	}
	if auth["last_error"] != "github authentication failed: status 401" {
		t.Fatalf("auth.last_error = %#v", auth["last_error"])
	}
	if auth["last_error_at"] != "2026-06-23T14:00:00Z" {
		t.Fatalf("auth.last_error_at = %#v", auth["last_error_at"])
	}
}

func TestAPIStateProjectsRunningBuildUnderInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		commit  string
	}{
		{name: "release build", version: "v1.3.0", commit: "abcdef123456"},
		{name: "development build", version: "dev", commit: "123456abcdef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{Build: buildinfo.Info{Version: tt.version, Commit: tt.commit}}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
			instance, ok := state["instance"].(map[string]any)
			if !ok {
				t.Fatalf("instance = %#v, want object", state["instance"])
			}
			if instance["version"] != tt.version || instance["commit"] != tt.commit {
				t.Fatalf("instance build = version %#v commit %#v, want %q %q", instance["version"], instance["commit"], tt.version, tt.commit)
			}
		})
	}
}

func TestAPIStateSurfacesSingleProjectAuthHealth(t *testing.T) {
	t.Parallel()

	failedAt := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: failedAt,
		Project:     telemetry.Project{ID: "detent", DisplayName: "detent"},
		Auth: telemetry.AuthHealth{
			Status:      telemetry.AuthStatusStale,
			LastError:   "github authentication failed: status 401",
			LastErrorAt: &failedAt,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	projects := state["projects"].([]any)
	project := projects[0].(map[string]any)
	auth := project["auth"].(map[string]any)
	if auth["status"] != string(telemetry.AuthStatusStale) {
		t.Fatalf("auth.status = %#v", auth["status"])
	}
	if auth["last_error"] != "github authentication failed: status 401" {
		t.Fatalf("auth.last_error = %#v", auth["last_error"])
	}
}

func TestServerUsesHostnameFallbackForInstanceName(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	server, err := web.NewServer(web.Config{
		Hostname: func() (string, error) {
			return "runner-01.example.com", nil
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK)
	if !strings.Contains(body, "<title>runner-01 · Detent</title>") {
		t.Fatalf("body missing hostname title:\n%s", body)
	}
	if !strings.Contains(body, `name="application-name" content="runner-01 · Detent"`) {
		t.Fatalf("body missing hostname application-name:\n%s", body)
	}
}

func TestServerReadsInstanceNameFromCurrentGlobalConfig(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC),
		Instance:    telemetry.Instance{Name: "worker-identity"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	current := globalconfig.Config{InstanceName: "first"}
	server, err := web.NewServer(web.Config{
		GlobalConfigSource: func() globalconfig.Config {
			return current
		},
		Hostname: func() (string, error) {
			return "", nil
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK)
	if !strings.Contains(body, "<title>first · Detent</title>") {
		t.Fatalf("body missing initial instance title:\n%s", body)
	}
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	instance := state["instance"].(map[string]any)
	if instance["display_name"] != "first" {
		t.Fatalf("initial instance.display_name = %#v, want first", instance["display_name"])
	}

	current = globalconfig.Config{InstanceName: "second"}
	body = requestHTML(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK)
	if !strings.Contains(body, "<title>second · Detent</title>") {
		t.Fatalf("body missing reloaded instance title:\n%s", body)
	}
	state = requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	instance = state["instance"].(map[string]any)
	if instance["display_name"] != "second" {
		t.Fatalf("reloaded instance.display_name = %#v, want second", instance["display_name"])
	}
}

func TestServerOmitsInstanceBadgeWhenNameEmpty(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		Hostname: func() (string, error) {
			return "", nil
		},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK)
	if !strings.Contains(body, "<title>Detent</title>") {
		t.Fatalf("body missing default title:\n%s", body)
	}
	if strings.Contains(body, `aria-label="Instance name"`) {
		t.Fatalf("body rendered empty instance badge:\n%s", body)
	}
}

func TestServerEscapesInstanceName(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{InstanceName: "<b>prod</b>"},
		Hostname: func() (string, error) {
			return "", nil
		},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/fleet", http.StatusOK)
	if strings.Contains(body, "<title><b>prod</b> · Detent</title>") {
		t.Fatalf("body rendered raw instance name in title:\n%s", body)
	}
	if strings.Contains(body, "><b>prod</b></span>") {
		t.Fatalf("body rendered raw instance name in badge:\n%s", body)
	}
	for _, want := range []string{
		"<title>&lt;b&gt;prod&lt;/b&gt; · Detent</title>",
		`name="application-name" content="&lt;b&gt;prod&lt;/b&gt; · Detent"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing escaped instance name %q:\n%s", want, body)
		}
	}
}

func TestServerStaticAssetsUseFingerprintsAndCacheHeaders(t *testing.T) {
	t.Parallel()

	staticDir := t.TempDir()
	css := "body{color:purple}"
	favicon := `<svg xmlns="http://www.w3.org/2000/svg"><path fill="#3730A3" d="M0 0h1v1H0z"/></svg>`
	writeTestCSS(t, staticDir, css)
	writeTestStaticAsset(t, staticDir, "img/detent-mark.svg", favicon)

	server, err := web.NewServer(web.Config{StaticDir: staticDir}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	onboardingServer, err := web.NewServer(web.Config{
		Mode:      web.ModeOnboarding,
		StaticDir: staticDir,
	}, web.Dependencies{})
	if err != nil {
		t.Fatalf("NewServer() onboarding error = %v", err)
	}

	fingerprintedPath := "/static/css/output." + shortTestHash(css) + ".css"
	fingerprintedFaviconPath := "/static/img/detent-mark." + shortTestHash(favicon) + ".svg"

	t.Run("html links fingerprinted assets and revalidates", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			handler http.Handler
			path    string
		}{
			{name: "dashboard", handler: server.Handler(), path: "/"},
			{name: "analytics", handler: server.Handler(), path: "/analytics"},
			{name: "settings", handler: server.Handler(), path: "/settings"},
			{name: "reports", handler: server.Handler(), path: "/reports"},
			{name: "onboarding", handler: onboardingServer.Handler(), path: "/onboarding"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, tt.path, nil)

				tt.handler.ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
				}
				if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
					t.Fatalf("Cache-Control = %q, want no-cache", got)
				}
				if !strings.Contains(rec.Body.String(), `href="`+fingerprintedPath+`"`) {
					t.Fatalf("body missing fingerprinted stylesheet %q:\n%s", fingerprintedPath, rec.Body.String())
				}
				if strings.Contains(rec.Body.String(), `href="/static/css/output.css"`) {
					t.Fatalf("body still links non-fingerprinted stylesheet:\n%s", rec.Body.String())
				}
				if !strings.Contains(rec.Body.String(), `rel="icon" type="image/svg+xml" href="`+fingerprintedFaviconPath+`"`) {
					t.Fatalf("body missing fingerprinted favicon %q:\n%s", fingerprintedFaviconPath, rec.Body.String())
				}
				if strings.Contains(rec.Body.String(), `href="/static/img/detent-mark.svg"`) {
					t.Fatalf("body still links non-fingerprinted favicon:\n%s", rec.Body.String())
				}
			})
		}
	})

	t.Run("fingerprinted asset is immutable", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			path    string
			content string
		}{
			{name: "stylesheet", path: fingerprintedPath, content: css},
			{name: "favicon", path: fingerprintedFaviconPath, content: favicon},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, tt.path, nil)

				server.Handler().ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
				}
				if rec.Body.String() != tt.content {
					t.Fatalf("body = %q, want %q", rec.Body.String(), tt.content)
				}
				if got := rec.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
					t.Fatalf("Cache-Control = %q, want immutable static caching", got)
				}
				if got := rec.Header().Get("ETag"); got == "" {
					t.Fatal("ETag is empty")
				}
			})
		}
	})
}

func TestServerLegacyStaticAssetsRequireRevalidation(t *testing.T) {
	t.Parallel()

	staticDir := t.TempDir()
	css := "body{color:green}"
	writeTestCSS(t, staticDir, css)

	server, err := web.NewServer(web.Config{StaticDir: staticDir}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static/css/output.css", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != css {
		t.Fatalf("body = %q, want %q", rec.Body.String(), css)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", got)
	}
	if got := rec.Header().Get("ETag"); got == "" {
		t.Fatal("ETag is empty")
	}
}

func TestServerServesDefaultStaticAssetsFromArbitraryWorkingDirectory(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	defer func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore Chdir() error = %v", err)
		}
	}()

	server, err := web.NewServer(web.Config{Mode: web.ModeOnboarding}, web.Dependencies{})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static/css/output.css", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/css") {
		t.Fatalf("Content-Type = %q, want text/css", got)
	}
	if !strings.Contains(rec.Body.String(), "tailwindcss") {
		t.Fatalf("body missing embedded CSS marker:\n%s", rec.Body.String())
	}
}

func writeTestCSS(t *testing.T, staticDir string, css string) {
	t.Helper()

	writeTestStaticAsset(t, staticDir, "css/output.css", css)
}

func writeTestStaticAsset(t *testing.T, staticDir string, name string, content string) {
	t.Helper()

	filePath := filepath.Join(staticDir, name)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func shortTestHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])[:12]
}

func TestOnboardingModeDoesNotRequireRuntimeDependencies(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		Mode:      web.ModeOnboarding,
		StaticDir: t.TempDir(),
	}, web.Dependencies{})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name         string
		path         string
		wantStatus   int
		wantContent  string
		wantLocation string
	}{
		{
			name:         "root redirects to onboarding",
			path:         "/",
			wantStatus:   http.StatusFound,
			wantLocation: "/onboarding",
		},
		{
			name:        "onboarding page",
			path:        "/onboarding",
			wantStatus:  http.StatusOK,
			wantContent: "Detent onboarding",
		},
		{
			name:        "health",
			path:        "/health",
			wantStatus:  http.StatusOK,
			wantContent: `"mode":"onboarding"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantLocation != "" && rec.Header().Get("Location") != tt.wantLocation {
				t.Fatalf("Location = %q, want %q", rec.Header().Get("Location"), tt.wantLocation)
			}
			if tt.wantContent != "" && !strings.Contains(rec.Body.String(), tt.wantContent) {
				t.Fatalf("body missing %q:\n%s", tt.wantContent, rec.Body.String())
			}
		})
	}
}

func TestDashboardRendersLatestSnapshot(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC),
		Counts: telemetry.Counts{
			Running:   1,
			Queue:     2,
			Blocked:   3,
			Completed: 4,
		},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "issue-35",
					Identifier: "digitaldrywood/detent#35",
					Title:      "Dashboard templates",
					State:      "In Progress",
				},
				TurnCount:      2,
				RuntimeSeconds: 120,
				Tokens: telemetry.Tokens{
					Total: 42_000,
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{
		"digitaldrywood/detent",
		"#35",
		"Dashboard templates",
		"2 turns",
		"tps",
		`id="agent-digitaldrywood-detent-35"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestDashboardRendersSidebarStateFromCookie(t *testing.T) {
	t.Parallel()

	routes := []struct {
		name  string
		path  string
		shell string
	}{
		{name: "board", path: "/", shell: "v2"},
		{name: "dashboard", path: "/fleet", shell: "v2"},
		{name: "project", path: "/projects/detent", shell: "v2"},
		{name: "reports", path: "/reports", shell: "v2"},
		{name: "settings", path: "/settings", shell: "v2"},
	}
	states := []struct {
		name          string
		cookie        *http.Cookie
		wantLegacy    string
		forbidLegacy  string
		wantShellV2   string
		forbidShellV2 string
	}{
		{
			name:          "defaults expanded",
			wantLegacy:    `data-tui-sidebar-state="expanded"`,
			forbidLegacy:  `data-tui-sidebar-state="collapsed"`,
			wantShellV2:   `data-rail="false"`,
			forbidShellV2: `data-rail="true"`,
		},
		{
			name: "renders collapsed from templui cookie",
			cookie: &http.Cookie{
				Name:  "sidebar_state",
				Value: "false",
			},
			wantLegacy:    `data-tui-sidebar-state="collapsed"`,
			forbidLegacy:  `data-tui-sidebar-state="expanded"`,
			wantShellV2:   `data-rail="true"`,
			forbidShellV2: `data-rail="false"`,
		},
		{
			name: "renders expanded from templui cookie",
			cookie: &http.Cookie{
				Name:  "sidebar_state",
				Value: "true",
			},
			wantLegacy:    `data-tui-sidebar-state="expanded"`,
			forbidLegacy:  `data-tui-sidebar-state="collapsed"`,
			wantShellV2:   `data-rail="false"`,
			forbidShellV2: `data-rail="true"`,
		},
	}

	for _, route := range routes {
		for _, state := range states {
			t.Run(route.name+" "+state.name, func(t *testing.T) {
				t.Parallel()

				deps := testDeps(t)
				mustSetWebProject(t, deps.Registry, "detent", false)
				if err := deps.Hub.Publish(telemetry.Snapshot{
					GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
					Projects: []telemetry.ProjectSnapshot{
						{
							Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
						},
					},
				}); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
				if err != nil {
					t.Fatalf("NewServer() error = %v", err)
				}

				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, route.path, nil)
				if state.cookie != nil {
					req.AddCookie(state.cookie)
				}

				server.Handler().ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
				}
				wantState, forbidState := state.wantLegacy, state.forbidLegacy
				if route.shell == "v2" {
					wantState, forbidState = state.wantShellV2, state.forbidShellV2
				}
				if !strings.Contains(rec.Body.String(), wantState) {
					t.Fatalf("%s missing %q:\n%s", route.path, wantState, rec.Body.String())
				}
				if strings.Contains(rec.Body.String(), forbidState) {
					t.Fatalf("%s rendered forbidden state %q:\n%s", route.path, forbidState, rec.Body.String())
				}
			})
		}
	}
}

func TestStaticPagesPreserveProjectSidebarContext(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Running: 3},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name       string
		path       string
		activeHref string
		sseConnect string
	}{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := requestHTML(t, server.Handler(), http.MethodGet, tt.path, http.StatusOK)
			for _, want := range []string{
				tt.sseConnect,
				`aria-label="Project views"`,
				`href="/projects/detent"`,
				`href="/projects/detent/kanban"`,
				`href="/projects/detent/runs"`,
				`href="/projects/detent/configuration"`,
				`href="/projects/detent/diagnostics"`,
				`href="/health/ui"`,
				`href="/analytics"`,
				`href="/reports?project=detent"`,
				`href="/settings?project=detent"`,
				`data-dashboard-view="kanban"`,
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing project-context sidebar marker %q:\n%s", tt.path, want, body)
				}
			}
			assertSharedDashboardShellOnce(t, body, tt.path)
			assertSingleCurrentSidebarItem(t, body)
			assertActiveSidebarLink(t, body, tt.activeHref)
			assertInactiveSidebarLink(t, body, "/health/ui")
			assertInactiveSidebarLink(t, body, "/analytics")
			if tt.name == "settings" {
				assertInactiveSidebarLink(t, body, "/settings?project=detent")
			}
		})
	}
}

func TestProjectDashboardRouteScopesSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	mustSetWebProject(t, deps.Registry, "pyroapex", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{DisplayName: "multiple projects"},
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Running: 1},
				Tokens:  telemetry.Tokens{Total: 42_000},
			},
			{
				Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:  telemetry.Counts{Running: 1},
				Tokens:  telemetry.Tokens{Total: 88_000},
			},
		},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "detent-running",
					Identifier: "digitaldrywood/detent#377",
					Title:      "Detent dashboard",
					State:      "In Progress",
					ProjectID:  "detent",
				},
				Tokens: telemetry.Tokens{Total: 42_000},
			},
			{
				Issue: telemetry.Issue{
					ID:         "pyro-running",
					Identifier: "digitaldrywood/pyroapex#12",
					Title:      "Pyro Apex migration",
					State:      "In Progress",
					ProjectID:  "pyroapex",
				},
				Tokens: telemetry.Tokens{Total: 88_000},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/runs", http.StatusOK)
	for _, want := range []string{
		"Detent",
		`href="/projects/detent"`,
		`href="/projects/detent/runs"`,
		`aria-current="page"`,
		`sse-connect="/events?project=detent&amp;view=runs"`,
		"#377",
		"Detent dashboard",
		"42.0K",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("project dashboard missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{
		"Pyro Apex migration",
		"88.0K",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("project dashboard rendered forbidden %q:\n%s", forbidden, body)
		}
	}
}

func TestProjectDashboardRouteLinksGitHubRepositoryIssues(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebGitHubLabelProject(t, deps.Registry, "detent", "digitaldrywood/detent")
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{{
			Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent", http.StatusOK)
	for _, want := range []string{
		`href="https://github.com/digitaldrywood/detent/issues"`,
		`target="_blank"`,
		`aria-label="Open Detent issues"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("project dashboard missing GitHub issues link marker %q:\n%s", want, body)
		}
	}
}

func TestProjectDashboardRouteRendersConfiguredKanbanOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 15, 15, 0, 0, time.UTC)
	stageAt := now.Add(-time.Minute)
	deps := testDeps(t)
	mustSetWebProjectWithWorkflowStates(t, deps.Registry, "detent", false,
		[]string{"Todo", "In Progress", "Human Review"},
		[]string{"Backlog", "Blocked"},
		[]string{"Done"},
	)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Queue: 1},
			},
		},
		Pipeline: []telemetry.Issue{
			{
				ID:             "review",
				Identifier:     "digitaldrywood/detent#478",
				ProjectID:      "detent",
				Title:          "Render Kanban",
				State:          "Human Review",
				StageUpdatedAt: &stageAt,
			},
		},
		Queue: []telemetry.Queued{
			{
				Issue: telemetry.Issue{
					ID:             "todo",
					Identifier:     "digitaldrywood/detent#477",
					ProjectID:      "detent",
					Title:          "Read workflow state",
					State:          "Todo",
					StageUpdatedAt: &stageAt,
				},
				Attempt: 1,
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	kanbanStart := strings.Index(body, `id="board-lanes"`)
	if kanbanStart < 0 {
		t.Fatalf("project Kanban page missing board lanes:\n%s", body)
	}
	kanban := body[kanbanStart:]
	backlogIndex := strings.Index(kanban, `data-board-lane="backlog"`)
	todoIndex := strings.Index(kanban, `data-board-lane="todo"`)
	reviewIndex := strings.Index(kanban, `data-board-lane="human-review"`)
	doneIndex := strings.Index(kanban, `data-board-lane="done"`)
	if backlogIndex < 0 || todoIndex < 0 || reviewIndex < 0 || doneIndex < 0 {
		t.Fatalf("kanban missing configured lanes: backlog=%d todo=%d review=%d done=%d\n%s", backlogIndex, todoIndex, reviewIndex, doneIndex, kanban)
	}
	if backlogIndex >= todoIndex || todoIndex >= reviewIndex || reviewIndex >= doneIndex {
		t.Fatalf("kanban lanes are not in configured Detent order: backlog=%d todo=%d review=%d done=%d\n%s", backlogIndex, todoIndex, reviewIndex, doneIndex, kanban)
	}
}

func TestProjectDashboardRoutesSplitOverviewAndDetailPages(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 18, 14, 0, 0, 0, time.UTC)
	stageAt := now.Add(-10 * time.Minute)
	startedAt := now.Add(-15 * time.Minute)
	completedAt := now.Add(-2 * time.Minute)
	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, &kanbanActionConnector{name: "github"})
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Running: 1, Queue: 1, Blocked: 1, Completed: 1},
				Tokens:  telemetry.Tokens{Total: 42_000},
			},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:             "todo",
				Identifier:     "digitaldrywood/detent#500",
				URL:            "https://github.com/digitaldrywood/detent/issues/500",
				ProjectID:      "detent",
				Title:          "Todo issue",
				State:          "Todo",
				StageUpdatedAt: &stageAt,
			},
		},
		Pipeline: []telemetry.Issue{
			{
				ID:             "review",
				Identifier:     "digitaldrywood/detent#501",
				URL:            "https://github.com/digitaldrywood/detent/issues/501",
				ProjectID:      "detent",
				Title:          "Review issue",
				State:          "Human Review",
				StageUpdatedAt: &stageAt,
				PullRequest: &telemetry.PullRequest{
					Number:   701,
					URL:      "https://github.com/digitaldrywood/detent/pull/701",
					CIStatus: "pass",
				},
			},
		},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "running",
					Identifier: "digitaldrywood/detent#502",
					ProjectID:  "detent",
					Title:      "Running issue",
					State:      "In Progress",
				},
				SessionID:   "session-running",
				StartedAt:   startedAt,
				TurnCount:   2,
				LastEvent:   "turn_started",
				LastMessage: "working",
			},
		},
		Queue: []telemetry.Queued{
			{
				Issue: telemetry.Issue{
					ID:         "queued",
					Identifier: "digitaldrywood/detent#503",
					ProjectID:  "detent",
					Title:      "Queued issue",
					State:      "Todo",
				},
				Attempt: 1,
			},
		},
		Blocked: []telemetry.Blocked{
			{
				Issue: telemetry.Issue{
					ID:         "blocked",
					Identifier: "digitaldrywood/detent#504",
					ProjectID:  "detent",
					Title:      "Blocked issue",
					State:      "Blocked",
				},
				Error:     "waiting on dependency",
				BlockedAt: &stageAt,
			},
		},
		Completed: []telemetry.Completed{
			{
				Issue: telemetry.Issue{
					ID:         "completed",
					Identifier: "digitaldrywood/detent#505",
					ProjectID:  "detent",
					Title:      "Completed issue",
				},
				StartedAt:   startedAt,
				CompletedAt: completedAt,
				FinalState:  "Done",
			},
		},
		RateLimits: &telemetry.RateLimits{
			LimitName: "Codex",
			Primary: &telemetry.RateLimitBucket{
				Remaining:      800,
				Used:           200,
				Limit:          1_000,
				ResetInSeconds: 3_600,
			},
		},
		CycleTime: telemetry.CycleTimeReport{
			Available:      true,
			AverageSeconds: int64(45 * time.Minute / time.Second),
			Issues: []telemetry.CycleTimeIssue{
				{Key: "digitaldrywood/detent#505", DurationSeconds: int64(45 * time.Minute / time.Second)},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	overview := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=detent&amp;view=overview"`,
		`href="/projects/detent/kanban"`,
		`href="/projects/detent/runs"`,
		`href="/projects/detent/configuration"`,
		`href="/projects/detent/diagnostics"`,
		`id="agent-activity"`,
		`id="project-recent-runs"`,
		"Recent runs",
		"Running issue",
	} {
		if !strings.Contains(overview, want) {
			t.Fatalf("project overview missing %q:\n%s", want, overview)
		}
	}
	for _, forbidden := range []string{
		`id="board-lanes"`,
		`aria-label="Agent activity timeline"`,
		`id="running-issues"`,
		`data-detent-charts`,
	} {
		if strings.Contains(overview, forbidden) {
			t.Fatalf("project overview rendered detail section %q:\n%s", forbidden, overview)
		}
	}

	runs := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/runs", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=detent&amp;view=runs"`,
		`id="project-runs"`,
		"#502",
		"#505",
		"Running issue",
		"Completed issue",
		"In progress",
		"Completed",
	} {
		if !strings.Contains(runs, want) {
			t.Fatalf("project runs route missing %q:\n%s", want, runs)
		}
	}
	for _, forbidden := range []string{
		`id="board-lanes"`,
		`data-detent-charts`,
	} {
		if strings.Contains(runs, forbidden) {
			t.Fatalf("project runs route rendered forbidden detail %q:\n%s", forbidden, runs)
		}
	}

	diagnostics := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/diagnostics", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=detent&amp;view=diagnostics"`,
		`aria-label="Project views"`,
		`href="/projects/detent/diagnostics"`,
		`aria-label="Board health"`,
		"Rate limits",
	} {
		if !strings.Contains(diagnostics, want) {
			t.Fatalf("project diagnostics route missing %q:\n%s", want, diagnostics)
		}
	}
	// The diagnostics tab stays inside the redesigned project frame rather
	// than dropping to the legacy shell.
	diagnosticsTab := regexp.MustCompile(`<a[^>]*href="/projects/detent/diagnostics"[^>]*aria-current="page"[^>]*>`).FindString(diagnostics)
	if diagnosticsTab == "" {
		t.Fatalf("project diagnostics route missing active diagnostics tab:\n%s", diagnostics)
	}
	for _, forbidden := range []string{
		`id="board-lanes"`,
		`id="agent-activity"`,
		`data-tui-sidebar-layout`,
	} {
		if strings.Contains(diagnostics, forbidden) {
			t.Fatalf("project diagnostics route rendered forbidden detail %q:\n%s", forbidden, diagnostics)
		}
	}

	configuration := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/configuration", http.StatusOK)
	for _, want := range []string{
		`id="settings-global"`,
		`id="settings-projects"`,
		"Runtime paths",
	} {
		if !strings.Contains(configuration, want) {
			t.Fatalf("project configuration route missing %q:\n%s", want, configuration)
		}
	}
	configTab := regexp.MustCompile(`<a[^>]*href="/projects/detent/configuration"[^>]*aria-current="page"[^>]*>`).FindString(configuration)
	if configTab == "" {
		t.Fatalf("project configuration route missing active configuration tab:\n%s", configuration)
	}
	for _, forbidden := range []string{
		`aria-label="Project Kanban"`,
		`aria-label="Agent activity timeline"`,
		`data-detent-charts`,
	} {
		if strings.Contains(configuration, forbidden) {
			t.Fatalf("project configuration route rendered forbidden detail %q:\n%s", forbidden, configuration)
		}
	}
}

func TestProjectKanbanRouteRendersOnlyLiveBoard(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)
	stageAt := now.Add(-12 * time.Minute)
	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, &kanbanActionConnector{name: "github"})
	mustSetWebProject(t, deps.Registry, "pyroapex", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Queue: 1},
			},
			{
				Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:  telemetry.Counts{Queue: 1},
			},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:             "I_kw490",
				Identifier:     "digitaldrywood/detent#490",
				URL:            "https://github.com/digitaldrywood/detent/issues/490",
				ProjectID:      "detent",
				Title:          "Add a live Kanban-only board view",
				State:          "Todo",
				StageUpdatedAt: &stageAt,
				PullRequest: &telemetry.PullRequest{
					Number:           701,
					URL:              "https://github.com/digitaldrywood/detent/pull/701",
					CIStatus:         "pass",
					CodexReviewState: "approved",
				},
			},
			{
				ID:         "I_pyro12",
				Identifier: "digitaldrywood/pyroapex#12",
				URL:        "https://github.com/digitaldrywood/pyroapex/issues/12",
				ProjectID:  "pyroapex",
				Title:      "Pyro Apex migration",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=detent&amp;view=kanban"`,
		`sse-swap="snapshot"`,
		`hx-swap="morph:innerHTML"`,
		`id="board-lanes"`,
		`data-board-key="project.detent"`,
		`id="card-digitaldrywood-detent-490"`,
		`href="/projects/detent"`,
		`href="/projects/detent/kanban"`,
		"Add a live Kanban-only board view",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("Kanban page missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{
		`aria-label="Dashboard health"`,
		`aria-label="Pull request pipeline"`,
		`data-detent-charts`,
		`id="live-tick"`,
		"Pyro Apex migration",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("Kanban page rendered forbidden %q:\n%s", forbidden, body)
		}
	}
}

func TestProjectKanbanRouteHidesMutationControlsInReadOnlyMode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 16, 14, 30, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, &kanbanActionConnector{name: "github"})
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw490",
				Identifier: "digitaldrywood/detent#490",
				URL:        "https://github.com/digitaldrywood/detent/issues/490",
				ProjectID:  "detent",
				Title:      "Read-only board card",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	if !strings.Contains(body, `id="card-digitaldrywood-detent-490"`) {
		t.Fatalf("read-only Kanban page missing board card:\n%s", body)
	}
	for _, forbidden := range []string{
		"/api/v1/kanban/",
		`data-kanban-action="`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("read-only Kanban page rendered mutation UI %q:\n%s", forbidden, body)
		}
	}
}

func TestBoardRouteRendersFleetBoard(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 16, 14, 45, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
		AllowedTransitions: map[string][]string{
			"Todo": {"In Progress"},
			"Done": {},
		},
	}, &kanbanActionConnector{name: "github"})
	mustSetWebProject(t, deps.Registry, "docs-site", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent", Color: "#1192e8"},
				Counts:  telemetry.Counts{Queue: 1},
			},
			{
				Project: telemetry.Project{ID: "docs-site", DisplayName: "Docs Site"},
				Counts:  telemetry.Counts{Running: 1},
			},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw542",
				Identifier: "digitaldrywood/detent#542",
				URL:        "https://github.com/digitaldrywood/detent/issues/542",
				ProjectID:  "detent",
				Title:      "Add top-level multi-project Kanban board",
				State:      "Todo",
			},
			{
				ID:         "I_kw543",
				Identifier: "digitaldrywood/detent#543",
				URL:        "https://github.com/digitaldrywood/detent/issues/543",
				ProjectID:  "detent",
				Title:      "Transitionless fleet card",
				State:      "Done",
			},
			{
				ID:         "I_docs12",
				Identifier: "digitaldrywood/docs-site#12",
				URL:        "https://github.com/digitaldrywood/docs-site/issues/12",
				ProjectID:  "docs-site",
				Title:      "Document fleet Kanban",
				State:      "In Progress",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/kanban", nil))
	if rec.Code != http.StatusFound {
		t.Fatalf("/kanban status = %d, want %d", rec.Code, http.StatusFound)
	}
	if got := rec.Header().Get("Location"); got != "/" {
		t.Fatalf("/kanban redirect = %q, want /", got)
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?view=board"`,
		`id="board-lanes"`,
		`data-board-key="fleet"`,
		`data-board-lane="todo"`,
		`data-board-lane="in-progress"`,
		`id="card-digitaldrywood-detent-542"`,
		`id="card-digitaldrywood-docs-site-12"`,
		"Add top-level multi-project Kanban board",
		"Document fleet Kanban",
		`id="fig-running"`,
		`id="board-lane-picker"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("board page missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{
		`hx-get="/api/v1/kanban/move?`,
		`aria-label="Dashboard health"`,
		`data-detent-charts`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("board page rendered forbidden %q:\n%s", forbidden, body)
		}
	}
	if got := strings.Count(body, `id="card-digitaldrywood-detent-543"`); got != 1 {
		t.Fatalf("done card render count = %d, want 1", got)
	}
}

func TestFleetKanbanActionUsesRegistryConnector(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	fallbackConnector := &kanbanActionConnector{name: "fallback"}
	projectConnector := &kanbanActionConnector{name: "project"}
	deps.Connector = fallbackConnector
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, projectConnector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 16, 14, 55, 0, 0, time.UTC),
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw542",
				Identifier: "digitaldrywood/detent#542",
				Title:      "Move from fleet board",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{
		Kanban: workflowconfig.Kanban{
			Mode: workflowconfig.KanbanModeIntegration,
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"issue_id":      {"I_kw542"},
		"current_state": {"Todo"},
		"target_state":  {"In Progress"},
	}
	rec := performForm(t, server.Handler(), http.MethodPost, "/api/v1/kanban/move", form)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := projectConnector.stateUpdates(); len(got) != 1 || got[0].issueID != "I_kw542" || got[0].state != "In Progress" {
		t.Fatalf("project connector state updates = %#v, want fleet move", got)
	}
	if got := fallbackConnector.stateUpdates(); len(got) != 0 {
		t.Fatalf("fallback connector state updates = %#v, want none", got)
	}
}

func TestProjectKanbanEventsSendBoardOnlySnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 16, 15, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, &kanbanActionConnector{name: "github"})
	mustSetWebProject(t, deps.Registry, "docs", false)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?project=detent&view=kanban")

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
			{Project: telemetry.Project{ID: "docs", DisplayName: "Docs"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw490",
				Identifier: "digitaldrywood/detent#490",
				URL:        "https://github.com/digitaldrywood/detent/issues/490",
				ProjectID:  "detent",
				Title:      "SSE board card",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	event := readRawSSEEvent(t, conn, reader)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{
		`id="board-lanes"`,
		`id="card-digitaldrywood-detent-490"`,
		"SSE board card",
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("Kanban snapshot event missing %q:\n%s", want, event.data)
		}
	}
	for _, forbidden := range []string{
		`aria-label="Dashboard health"`,
		`aria-label="Pull request pipeline"`,
		"Running issues",
	} {
		if strings.Contains(event.data, forbidden) {
			t.Fatalf("Kanban snapshot event rendered forbidden %q:\n%s", forbidden, event.data)
		}
	}

	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	for _, want := range []string{
		`href="/projects/detent/kanban"`,
		`data-sidebar-project="detent"`,
		`aria-current="page"`,
	} {
		if !strings.Contains(sidebarEvent.data, want) {
			t.Fatalf("Kanban sidebar event missing %q:\n%s", want, sidebarEvent.data)
		}
	}
}

func TestFleetKanbanEventsSendBoardOnlySnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 16, 15, 15, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	mustSetWebProject(t, deps.Registry, "docs-site", false)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?view=kanban")

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent", Color: "#1192e8"}},
			{Project: telemetry.Project{ID: "docs-site", DisplayName: "Docs Site"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "I_kw542",
				Identifier: "digitaldrywood/detent#542",
				URL:        "https://github.com/digitaldrywood/detent/issues/542",
				ProjectID:  "detent",
				Title:      "Fleet SSE board card",
				State:      "Todo",
			},
			{
				ID:         "I_docs12",
				Identifier: "digitaldrywood/docs-site#12",
				URL:        "https://github.com/digitaldrywood/docs-site/issues/12",
				ProjectID:  "docs-site",
				Title:      "Docs SSE board card",
				State:      "In Progress",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	event := readRawSSEEvent(t, conn, reader)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{
		`id="board-lanes"`,
		`data-board-key="fleet"`,
		`id="card-digitaldrywood-detent-542"`,
		`id="card-digitaldrywood-docs-site-12"`,
		"Fleet SSE board card",
		"Docs SSE board card",
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("fleet Kanban snapshot event missing %q:\n%s", want, event.data)
		}
	}
	for _, forbidden := range []string{
		`aria-label="Dashboard health"`,
		`aria-label="Pull request pipeline"`,
		"Running issues",
		`data-kanban-action="`,
	} {
		if strings.Contains(event.data, forbidden) {
			t.Fatalf("fleet Kanban snapshot event rendered forbidden %q:\n%s", forbidden, event.data)
		}
	}

	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	for _, want := range []string{
		`href="/"`,
		`data-sidebar-nav-item="board"`,
		`aria-current="page"`,
	} {
		if !strings.Contains(sidebarEvent.data, want) {
			t.Fatalf("fleet Kanban sidebar event missing %q:\n%s", want, sidebarEvent.data)
		}
	}
	assertAppSidebarLinkActive(t, sidebarEvent.data, "/")
}

func TestProjectRoutesAllowEscapedSlashIDs(t *testing.T) {
	t.Parallel()

	projectID := "digitaldrywood/kanban"
	escapedProjectID := "digitaldrywood%2Fkanban"
	now := time.Date(2026, 6, 12, 15, 30, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, projectID, false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: projectID, DisplayName: "Detent"},
				Counts:  telemetry.Counts{Running: 1},
				Tokens:  telemetry.Tokens{Total: 42},
			},
		},
		Running: []telemetry.Running{
			{Issue: telemetry.Issue{ID: "detent-running", Identifier: "digitaldrywood/kanban#377", ProjectID: projectID}},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/projects/"+escapedProjectID, nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{
		`href="/projects/digitaldrywood%2Fkanban"`,
		`sse-connect="/events?project=digitaldrywood%2Fkanban&amp;view=overview"`,
		`href="/projects/digitaldrywood%2Fkanban/runs"`,
		`href="/projects/digitaldrywood%2Fkanban/diagnostics"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("project dashboard missing %q:\n%s", want, rec.Body.String())
		}
	}
	runsBody := requestHTML(t, server.Handler(), http.MethodGet, "/projects/"+escapedProjectID+"/runs", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=digitaldrywood%2Fkanban&amp;view=runs"`,
		`href="/projects/digitaldrywood%2Fkanban/runs"`,
		"#377",
	} {
		if !strings.Contains(runsBody, want) {
			t.Fatalf("project runs route missing %q:\n%s", want, runsBody)
		}
	}

	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/"+escapedProjectID+"/kanban", http.StatusOK)
	for _, want := range []string{
		`sse-connect="/events?project=digitaldrywood%2Fkanban&amp;view=kanban"`,
		`href="/projects/digitaldrywood%2Fkanban"`,
		`href="/projects/digitaldrywood%2Fkanban/kanban"`,
		"#377",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("project Kanban route missing %q:\n%s", want, body)
		}
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/projects/"+escapedProjectID+"/state", http.StatusOK)
	if got := nestedString(t, state, "counts", "running"); got != "1" {
		t.Fatalf("counts.running = %s, want 1", got)
	}
	series := requestJSON(t, server, http.MethodGet, "/api/v1/projects/"+escapedProjectID+"/timeseries", http.StatusOK)
	if series["scope"] != "project" || series["project_id"] != projectID {
		t.Fatalf("series scope/project_id = %#v/%#v; payload = %#v", series["scope"], series["project_id"], series)
	}
}

func TestProjectDashboardRouteReturnsNotFoundForUnknownProject(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/projects/missing", nil)
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", rec.Code, rec.Body.String())
	}
}

func TestProjectStateAPIScopesSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 16, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:  telemetry.Counts{Running: 1},
				Tokens:  telemetry.Tokens{Total: 42},
			},
			{
				Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:  telemetry.Counts{Running: 1},
				Tokens:  telemetry.Tokens{Total: 88},
			},
		},
		Running: []telemetry.Running{
			{Issue: telemetry.Issue{ID: "detent-running", Identifier: "digitaldrywood/detent#377", ProjectID: "detent"}},
			{Issue: telemetry.Issue{ID: "pyro-running", Identifier: "digitaldrywood/pyroapex#12", ProjectID: "pyroapex"}},
		},
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Remaining: 4878, Used: 122, Limit: 5000},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/projects/detent/state", http.StatusOK)
	if got := nestedString(t, state, "counts", "running"); got != "1" {
		t.Fatalf("counts.running = %s, want 1", got)
	}
	if got := nestedString(t, state, "codex_totals", "total_tokens"); got != "42" {
		t.Fatalf("codex_totals.total_tokens = %s, want 42", got)
	}
	running := state["running"].([]any)
	if len(running) != 1 || running[0].(map[string]any)["issue_identifier"] != "digitaldrywood/detent#377" {
		t.Fatalf("running = %#v, want only detent row", running)
	}
	if got := nestedString(t, state, "rate_limits", "github_rest", "remaining"); got != "4878" {
		t.Fatalf("rate_limits.github_rest.remaining = %s, want 4878", got)
	}

	missing := requestJSON(t, server, http.MethodGet, "/api/v1/projects/missing/state", http.StatusNotFound)
	if nestedString(t, missing, "error", "code") != "project_not_found" {
		t.Fatalf("missing project payload = %#v", missing)
	}
}

func TestStateAPICachesEnrichmentQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "fleet", path: "/api/v1/state"},
		{name: "project", path: "/api/v1/projects/detent/state"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := &enrichmentQueryCountingStore{}
			deps := testDeps(t)
			deps.Store = backend
			mustSetWebProject(t, deps.Registry, "detent", false)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 8, 15, 22, 16, 27, 0, time.UTC),
				Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Projects: []telemetry.ProjectSnapshot{{
					Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			cold := requestJSON(t, server, http.MethodGet, tt.path, http.StatusOK)
			if enrichment := cold["enrichment"].(map[string]any); enrichment["status"] != "omitted" {
				t.Fatalf("cold enrichment = %#v, want omitted", enrichment)
			}
			if got := backend.workflowMetricsCalls.Load(); got != 0 {
				t.Fatalf("cold WorkflowMetricsReport() calls = %d, want 0", got)
			}
			if got := backend.budgetCostCalls.Load(); got != 0 {
				t.Fatalf("cold BudgetCostEvents() calls = %d, want 0", got)
			}

			requestDashboardEnrichment(t, server)
			workflowCalls := backend.workflowMetricsCalls.Load()
			budgetCalls := backend.budgetCostCalls.Load()
			if workflowCalls == 0 || budgetCalls == 0 {
				t.Fatalf("dashboard enrichment calls = workflow %d, budget %d", workflowCalls, budgetCalls)
			}
			for range 2 {
				cached := requestJSON(t, server, http.MethodGet, tt.path, http.StatusOK)
				if enrichment := cached["enrichment"].(map[string]any); enrichment["status"] != "ready" {
					t.Fatalf("cached enrichment = %#v, want ready", enrichment)
				}
			}
			if got := backend.workflowMetricsCalls.Load(); got != workflowCalls {
				t.Fatalf("cached WorkflowMetricsReport() calls = %d, want %d", got, workflowCalls)
			}
			if got := backend.budgetCostCalls.Load(); got != budgetCalls {
				t.Fatalf("cached BudgetCostEvents() calls = %d, want %d", got, budgetCalls)
			}
		})
	}
}

func TestStateAPIIncludesGitHubGraphQLRateLimitStatus(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	generatedAt := time.Date(2026, 6, 29, 15, 0, 0, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		RateLimits: &telemetry.RateLimits{
			GitHubREST:    &telemetry.RateLimitBucket{Remaining: 4608, Used: 392, Limit: 5000},
			GitHubGraphQL: &telemetry.RateLimitBucket{Status: telemetry.RateLimitStatusExhausted},
		},
		BackendOutages: []telemetry.BackendOutage{{
			BackendID: "codex",
			Provider:  "openai",
			Reason:    "provider usage limit reached",
			ResumeAt:  generatedAt.Add(44 * time.Minute),
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	if got := nestedString(t, state, "rate_limits", "github_rest", "remaining"); got != "4608" {
		t.Fatalf("rate_limits.github_rest.remaining = %s, want 4608", got)
	}
	if got := nestedString(t, state, "rate_limits", "github_graphql", "status"); got != telemetry.RateLimitStatusExhausted {
		t.Fatalf("rate_limits.github_graphql.status = %s, want %s", got, telemetry.RateLimitStatusExhausted)
	}
	outages := state["backend_outages"].([]any)
	if len(outages) != 1 || outages[0].(map[string]any)["backend_id"] != "codex" {
		t.Fatalf("backend_outages = %#v", outages)
	}
	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	healthOutages := health["backend_outages"].([]any)
	if len(healthOutages) != 1 || healthOutages[0].(map[string]any)["backend_id"] != "codex" {
		t.Fatalf("health backend_outages = %#v", healthOutages)
	}
}

func TestStateAndHealthReportWorkspaceCleanupFailures(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	observedAt := time.Date(2026, 9, 4, 15, 30, 0, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: observedAt,
		CleanupFaults: []telemetry.CleanupFault{{
			ProjectID:         "detent",
			AffectedPathCount: 3,
			LastError:         "permission denied",
			ObservedAt:        observedAt,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	for _, path := range []string{"/api/v1/state", "/health"} {
		response := requestJSON(t, server, http.MethodGet, path, http.StatusOK)
		failures := response["workspace_cleanup_failures"].([]any)
		if len(failures) != 1 || failures[0].(map[string]any)["affected_path_count"] != float64(3) {
			t.Fatalf("%s workspace cleanup failures = %#v", path, failures)
		}
	}
	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if health["status"] != "needs_attention" {
		t.Fatalf("health status = %v, want needs_attention", health["status"])
	}
}

func TestStateAPIIncludesProjectFailureBreakerEvidence(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	now := time.Date(2026, 8, 15, 18, 0, 0, 0, time.UTC)
	eligibleCandidates := 0
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		FailureBreakers: []telemetry.FailureBreaker{{
			ProjectID: "digitaldrywood-video", Class: "runner_error:b6c174a86dfb", Count: 5, AttemptCount: 5, DistinctItemCount: 1,
			Cause: "provider usage limit reached", RepresentativeError: "You've hit your limit. Try again at 9:39 PM", BackendID: "claude-code", BackendKind: "claude_code", Provider: "anthropic", EligibleCandidateCount: &eligibleCandidates,
			Items: []telemetry.FailureBreakerItem{{IssueID: "video-1", Identifier: "2026-07-10-detent-not-vibe-coding-short", IssueURL: "https://example.test/items/video-1", Title: "Author beat visuals", CurrentState: "Blocked", AttemptCount: 5, Parked: true}},
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	breakers := state["failure_breakers"].([]any)
	if len(breakers) != 1 {
		t.Fatalf("failure_breakers = %#v", breakers)
	}
	breaker := breakers[0].(map[string]any)
	if breaker["attempt_count"] != float64(5) || breaker["distinct_item_count"] != float64(1) || breaker["cause"] != "provider usage limit reached" || breaker["eligible_candidate_count"] != float64(0) {
		t.Fatalf("failure breaker evidence = %#v", breaker)
	}
	items := breaker["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["title"] != "Author beat visuals" || items[0].(map[string]any)["parked"] != true {
		t.Fatalf("failure breaker items = %#v", items)
	}
	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	healthBreakers := health["failure_breakers"].([]any)
	if len(healthBreakers) != 1 || healthBreakers[0].(map[string]any)["provider"] != "anthropic" {
		t.Fatalf("health failure_breakers = %#v", healthBreakers)
	}
}

func TestHealthAndStateReportHostPressureAndAgentRSS(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	now := time.Date(2026, 8, 18, 17, 0, 0, 0, time.UTC)
	observedAt := now.Add(-time.Second)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		MemoryPressure: telemetry.MemoryPressure{
			Supported:    true,
			Some:         telemetry.PressureAverages{Avg60: 12.5},
			SomeAvg60Max: 10,
			DispatchHeld: true,
			ObservedAt:   observedAt,
		},
		IOPressure: telemetry.IOPressure{
			Supported:                    true,
			Some:                         telemetry.PressureAverages{Avg10: 78.81},
			Full:                         telemetry.PressureAverages{Avg10: 63.64},
			FullAvg10Max:                 5,
			DegradedMaxConcurrentAgents:  1,
			EffectiveMaxConcurrentAgents: 1,
			CapacityConstrained:          true,
			ConstrainedSince:             now.Add(-5 * time.Minute),
			ConstrainedForMS:             300000,
			ObservedAt:                   observedAt,
		},
		CPUPressure: telemetry.CPUPressure{
			Supported:    true,
			Some:         telemetry.PressureAverages{Avg10: 91.2},
			SomeAvg10Max: 80,
			DispatchHeld: true,
			ObservedAt:   observedAt,
		},
		Running: []telemetry.Running{{
			Issue:           telemetry.Issue{ProjectID: "detent", Identifier: "digitaldrywood/detent#1899"},
			RSSBytes:        7 << 30,
			RSSCeilingBytes: 8 << 30,
			RSSObservedAt:   observedAt,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if health["status"] != "needs_attention" {
		t.Fatalf("health status = %#v, want needs_attention", health["status"])
	}
	pressure := health["memory_pressure"].(map[string]any)
	if pressure["dispatch_held"] != true || pressure["some"].(map[string]any)["avg60"] != 12.5 {
		t.Fatalf("health memory_pressure = %#v", pressure)
	}
	if pressure := health["io_pressure"].(map[string]any); pressure["dispatch_held"] != false || pressure["capacity_constrained"] != true || pressure["degraded_max_concurrent_agents"] != float64(1) || pressure["effective_max_concurrent_agents"] != float64(1) || pressure["constrained_for_ms"] != float64(300000) || pressure["full"].(map[string]any)["avg10"] != 63.64 {
		t.Fatalf("health io_pressure = %#v", pressure)
	}
	if pressure := health["cpu_pressure"].(map[string]any); pressure["dispatch_held"] != true || pressure["some"].(map[string]any)["avg10"] != 91.2 {
		t.Fatalf("health cpu_pressure = %#v", pressure)
	}
	agents := health["agent_memory"].([]any)
	if len(agents) != 1 || agents[0].(map[string]any)["rss_bytes"] != float64(7<<30) || agents[0].(map[string]any)["rss_ceiling_bytes"] != float64(8<<30) {
		t.Fatalf("health agent_memory = %#v", agents)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	if state["memory_pressure"].(map[string]any)["dispatch_held"] != true {
		t.Fatalf("state memory_pressure = %#v", state["memory_pressure"])
	}
	if pressure := state["io_pressure"].(map[string]any); pressure["dispatch_held"] != false || pressure["capacity_constrained"] != true || pressure["effective_max_concurrent_agents"] != float64(1) {
		t.Fatalf("state io_pressure = %#v", state["io_pressure"])
	}
	if state["cpu_pressure"].(map[string]any)["dispatch_held"] != true {
		t.Fatalf("state cpu_pressure = %#v", state["cpu_pressure"])
	}
	running := state["running"].([]any)
	if len(running) != 1 || running[0].(map[string]any)["rss_bytes"] != float64(7<<30) {
		t.Fatalf("state running = %#v", running)
	}
}

func TestHealthReportsCICondition(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", true)
	now := time.Date(2026, 8, 8, 20, 0, 0, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		CIUnavailable: []telemetry.CICondition{{
			ProjectID:           "detent",
			UnstartedCheckCount: 6,
			PullRequestCount:    2,
			OldestQueueSeconds:  47 * 60,
			DetectedAt:          now.Add(-32 * time.Minute),
			LastObservedAt:      now,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if got := nestedString(t, health, "status"); got != "needs_attention" {
		t.Fatalf("health status = %q, want needs_attention", got)
	}
	conditions := health["ci_unavailable"].([]any)
	if len(conditions) != 1 || nestedString(t, conditions[0].(map[string]any), "unstarted_check_count") != "6" {
		t.Fatalf("health ci_unavailable = %#v", conditions)
	}
	projects := health["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["status"] != "needs_human_attention" {
		t.Fatalf("health projects = %#v, want detent needs_human_attention", projects)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	conditions = state["ci_unavailable"].([]any)
	if len(conditions) != 1 || nestedString(t, conditions[0].(map[string]any), "pull_request_count") != "2" {
		t.Fatalf("state ci_unavailable = %#v", conditions)
	}
}

func TestHealthReportsUnevaluablePauseExitCondition(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "digitaldrywood-video", true)
	deps.Registry.SetPauseExitStatus(pause.ExitStatus{
		ProjectID:           "digitaldrywood-video",
		Reference:           "digitaldrywood/video-studio#147",
		ResolverProjectID:   "video-studio",
		LastError:           "pause exit issue digitaldrywood/video-studio#147 was not found",
		ConsecutiveFailures: pause.DefaultEvaluationFailureThreshold,
		NeedsAttention:      true,
		EvaluatedAt:         time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
	})
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if health["status"] != "needs_attention" {
		t.Fatalf("health status = %#v, want needs_attention", health["status"])
	}
	projects := health["projects"].([]any)
	projectHealth := projects[0].(map[string]any)
	if projectHealth["status"] != "needs_human_attention" {
		t.Fatalf("project health = %#v, want needs_human_attention", projectHealth)
	}
	pauseExit := projectHealth["pause_exit"].(map[string]any)
	for key, want := range map[string]any{
		"project_id":      "digitaldrywood-video",
		"reference":       "digitaldrywood/video-studio#147",
		"last_error":      "pause exit issue digitaldrywood/video-studio#147 was not found",
		"needs_attention": true,
	} {
		if pauseExit[key] != want {
			t.Fatalf("pause_exit[%q] = %#v, want %#v", key, pauseExit[key], want)
		}
	}
}

func TestHealthReportsNotificationDeliveryFailures(t *testing.T) {
	t.Parallel()
	deps := testDeps(t)
	deps.HealthNotifications = healthNotificationFailureReader{failures: []healthnotify.Failure{{
		EventID:     "health-event-1",
		Identity:    "fleet",
		Scope:       healthnotify.ScopeFleet,
		Transition:  healthnotify.TransitionEntry,
		Attempts:    2,
		MaxAttempts: 5,
		LastError:   "receiver unavailable",
	}}}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	failures := health["health_notification_failures"].([]any)
	if len(failures) != 1 || failures[0].(map[string]any)["event_id"] != "health-event-1" {
		t.Fatalf("health notification failures = %#v", failures)
	}
}

func TestHealthReportsTotalSelectorExclusionAsNeedsHumanAttention(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", true)
	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	lastSelectedAt := now.Add(-4 * time.Hour)
	stall := telemetry.DispatchStatus{
		ProjectID: "detent", CandidateCount: 8, SkippedCount: 8, WaitReason: "authorization selector excludes every candidate", WaitReasonCode: "authorization_selector_declined", LastSelectedAt: &lastSelectedAt, StallDurationSeconds: 10_800, Stalled: true, NeedsHumanAttention: true,
		RateWindowPacing: telemetry.RateWindowPacing{Mode: "floor", FloorPercent: 25, StaleAfterSeconds: 900, Applicable: true, BucketStatus: telemetry.RateWindowBucketFresh, PermitCeiling: 2, ScalingApplied: true},
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: now, Dispatch: stall, DispatchStalls: []telemetry.DispatchStatus{stall}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if health["status"] != "needs_attention" {
		t.Fatalf("health status = %#v, want needs_attention", health["status"])
	}
	projects := health["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["status"] != "needs_human_attention" {
		t.Fatalf("health projects = %#v, want detent needs_human_attention", projects)
	}
	stalls := health["dispatch_stalls"].([]any)
	if len(stalls) != 1 || nestedString(t, stalls[0].(map[string]any), "candidate_count") != "8" {
		t.Fatalf("health dispatch_stalls = %#v", stalls)
	}
	if got := nestedString(t, health, "dispatch", "rate_window_pacing", "mode"); got != "floor" {
		t.Fatalf("health dispatch pacing mode = %q, want floor", got)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	if nestedString(t, state, "dispatch", "last_selected_at") != lastSelectedAt.Format(time.RFC3339) {
		t.Fatalf("state dispatch = %#v, want last_selected_at %s", state["dispatch"], lastSelectedAt)
	}
	if got := nestedString(t, state, "dispatch", "rate_window_pacing", "permit_ceiling"); got != "2" {
		t.Fatalf("state dispatch pacing ceiling = %q, want 2", got)
	}
}

func TestHealthStatusIncludesOnlyFaultDispatchConditions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		reasonCode string
		wantStatus string
	}{
		{name: "provider pacing", reasonCode: "provider_rate_window_backpressure", wantStatus: "ok"},
		{name: "review queue", reasonCode: "artifact_gate_wait_status", wantStatus: "ok"},
		{name: "total selector exclusion", reasonCode: "authorization_selector_declined", wantStatus: "needs_attention"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			deps := testDeps(t)
			stall := telemetry.DispatchStatus{ProjectID: "detent", CandidateCount: 2, SkippedCount: 2, WaitReasonCode: tt.reasonCode, Stalled: true}
			if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: time.Now(), DispatchStalls: []telemetry.DispatchStatus{stall}}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}
			health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if health["status"] != tt.wantStatus {
				t.Fatalf("health status = %#v, want %q", health["status"], tt.wantStatus)
			}
		})
	}
}

func TestHealthReportsOnlyWorkerProcessesWithoutLiveSessions(t *testing.T) {
	now := time.Date(2026, 8, 18, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name                 string
		terminal             bool
		live                 bool
		snapshotBeforeWorker bool
		wantCount            string
		wantStatus           string
	}{
		{name: "terminal session is orphaned", terminal: true, wantCount: "2", wantStatus: "needs_attention"},
		{name: "live tracked session is excluded", live: true, wantCount: "0", wantStatus: "ok"},
		{name: "new session awaits fresh snapshot", snapshotBeforeWorker: true, wantCount: "0", wantStatus: "ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := testDeps(t)
			deps.Store = openWebTestStore(t)
			deps.WorkerProcesses = deps.Store
			startedAt := now.Add(-2 * time.Hour)
			sessionID, err := deps.Store.StartSession(t.Context(), store.SessionStart{
				IssueID:    "issue-1885",
				Identifier: "digitaldrywood/detent#1885",
				StartedAt:  startedAt,
			})
			if err != nil {
				t.Fatalf("StartSession() error = %v", err)
			}
			if err := deps.Store.UpdateSessionWorkerProcess(t.Context(), sessionID, store.WorkerProcessRegistration{WorkerProcessIdentity: store.WorkerProcessIdentity{PID: 1885, GroupID: 1885, StartedAt: startedAt}}); err != nil {
				t.Fatalf("UpdateSessionWorkerProcess() error = %v", err)
			}
			if tt.terminal {
				if err := deps.Store.FinishSession(t.Context(), sessionID, store.SessionFinish{CompletedAt: now.Add(-time.Hour), FinalState: "completed"}); err != nil {
					t.Fatalf("FinishSession() error = %v", err)
				}
			}
			observedIdentities := 0
			deps.ObserveProcesses = func(identities []procgroup.Identity) ([]procgroup.Observation, error) {
				observedIdentities = len(identities)
				observations := make([]procgroup.Observation, 0, len(identities))
				for _, identity := range identities {
					observations = append(observations, procgroup.Observation{Identity: identity, Alive: true, ProcessCount: 2, RSSBytes: 256 * 1024 * 1024})
				}
				return observations, nil
			}
			snapshot := telemetry.Snapshot{GeneratedAt: now}
			if tt.snapshotBeforeWorker {
				snapshot.GeneratedAt = startedAt.Add(-time.Second)
			}
			if tt.live {
				snapshot.Running = []telemetry.Running{{DetentSessionID: sessionID}}
			}
			if err := deps.Hub.Publish(snapshot); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{Now: func() time.Time { return now }}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if got := nestedString(t, health, "orphaned_agent_processes", "count"); got != tt.wantCount {
				t.Fatalf("orphaned process count = %s, want %s", got, tt.wantCount)
			}
			if health["status"] != tt.wantStatus {
				t.Fatalf("health status = %#v, want %q", health["status"], tt.wantStatus)
			}
			wantObserved := 1
			if tt.live || tt.snapshotBeforeWorker {
				wantObserved = 0
			}
			if observedIdentities != wantObserved {
				t.Fatalf("observed identities = %d, want %d", observedIdentities, wantObserved)
			}
		})
	}
}

func TestHealthReportsTickLivenessAndSnapshotAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 1, 39, 0, 0, time.UTC)
	tests := []struct {
		name             string
		generatedAt      time.Time
		nextRefreshAt    time.Time
		liveness         telemetry.TickLiveness
		wantHealthStatus string
		wantRefresh      telemetry.RefreshStatus
		wantAgeSeconds   string
	}{
		{
			name:          "healthy loop remains ready",
			generatedAt:   now.Add(-time.Second),
			nextRefreshAt: now.Add(time.Minute),
			liveness: telemetry.TickLiveness{
				ProjectID:     "detent",
				Status:        telemetry.TickLivenessStatusReady,
				LastTickAt:    new(now.Add(-time.Second)),
				NextRefreshAt: new(now.Add(time.Minute)),
			},
			wantHealthStatus: "ok",
			wantRefresh:      telemetry.RefreshStatusReady,
			wantAgeSeconds:   "1",
		},
		{
			name:          "frozen loop needs attention",
			generatedAt:   now.Add(-18 * time.Minute),
			nextRefreshAt: now.Add(-9 * time.Minute),
			liveness: telemetry.TickLiveness{
				ProjectID:             "detent",
				Status:                telemetry.TickLivenessStatusNeedsAttention,
				LastTickAt:            new(now.Add(-18 * time.Minute)),
				NextRefreshAt:         new(now.Add(-9 * time.Minute)),
				NextRefreshOverdue:    true,
				FrozenAt:              new(now.Add(-18 * time.Minute)),
				MissedIntervals:       2,
				WatchdogIntervalCount: 2,
			},
			wantHealthStatus: "needs_attention",
			wantRefresh:      telemetry.RefreshStatusBehind,
			wantAgeSeconds:   "1080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			deps.TickLiveness = tickLivenessProbe{values: []telemetry.TickLiveness{tt.liveness}}
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: tt.generatedAt,
				Refresh: telemetry.Refresh{
					Status:        telemetry.RefreshStatusReady,
					LastRefreshAt: &tt.generatedAt,
					NextRefreshAt: &tt.nextRefreshAt,
				},
				Counts: telemetry.Counts{Running: 1},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{Now: func() time.Time { return now }}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if health["status"] != tt.wantHealthStatus {
				t.Fatalf("health status = %#v, want %q", health["status"], tt.wantHealthStatus)
			}
			if got := nestedString(t, health, "snapshot_age_seconds"); got != tt.wantAgeSeconds {
				t.Fatalf("health snapshot_age_seconds = %s, want %s", got, tt.wantAgeSeconds)
			}
			if got := nestedString(t, health, "refresh", "status"); got != string(tt.wantRefresh) {
				t.Fatalf("health refresh.status = %q, want %q", got, tt.wantRefresh)
			}
			liveness := health["tick_liveness"].([]any)
			if len(liveness) != 1 || liveness[0].(map[string]any)["status"] != string(tt.liveness.Status) {
				t.Fatalf("health tick_liveness = %#v", liveness)
			}

			state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
			if got := nestedString(t, state, "snapshot_age_seconds"); got != tt.wantAgeSeconds {
				t.Fatalf("state snapshot_age_seconds = %s, want %s", got, tt.wantAgeSeconds)
			}
			if got := nestedString(t, state, "refresh", "status"); got != string(tt.wantRefresh) {
				t.Fatalf("state refresh.status = %q, want %q", got, tt.wantRefresh)
			}
		})
	}
}

func TestHealthReportsProjectRefreshFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 14, 0, 0, 0, time.UTC)
	lastRefreshAt := now.Add(-time.Minute)
	lastErrorAt := now.Add(-30 * time.Second)
	tests := []struct {
		name             string
		refresh          telemetry.Refresh
		wantHealthStatus string
		wantFailures     int
	}{
		{
			name: "single transient failure remains healthy",
			refresh: telemetry.Refresh{
				Status:           telemetry.RefreshStatusDegraded,
				FailureThreshold: 3,
				LastRefreshAt:    &lastRefreshAt,
				LastError:        "candidate endpoint unavailable",
				LastErrorAt:      &lastErrorAt,
				Sources: []telemetry.RefreshSource{{
					Name: telemetry.RefreshSourceCandidates, FailureStreak: 1, LastError: "candidate endpoint unavailable", LastErrorAt: &lastErrorAt,
				}},
			},
			wantHealthStatus: "ok",
		},
		{
			name: "consecutive failures need attention",
			refresh: telemetry.Refresh{
				Status:           telemetry.RefreshStatusDegraded,
				FailureThreshold: 3,
				LastRefreshAt:    &lastRefreshAt,
				LastError:        "candidate endpoint unavailable",
				LastErrorAt:      &lastErrorAt,
				Sources: []telemetry.RefreshSource{{
					Name: telemetry.RefreshSourceCandidates, FailureStreak: 3, LastError: "candidate endpoint unavailable", LastErrorAt: &lastErrorAt,
				}},
			},
			wantHealthStatus: "needs_attention",
			wantFailures:     1,
		},
		{
			name: "never refreshed failure needs attention immediately",
			refresh: telemetry.Refresh{
				Status:           telemetry.RefreshStatusDegraded,
				FailureThreshold: 3,
				LastError:        "Bad credentials",
				LastErrorAt:      &lastErrorAt,
				Sources: []telemetry.RefreshSource{{
					Name: telemetry.RefreshSourceCandidates, FailureStreak: 1, LastError: "Bad credentials", LastErrorAt: &lastErrorAt,
				}},
			},
			wantHealthStatus: "needs_attention",
			wantFailures:     1,
		},
		{
			name: "loop behind remains healthy",
			refresh: telemetry.Refresh{
				Status:               telemetry.RefreshStatusBehind,
				FailureThreshold:     3,
				LastRefreshAt:        &lastRefreshAt,
				NextRefreshAt:        &lastErrorAt,
				NextRefreshOverdue:   true,
				BehindBySeconds:      30,
				ObservedSweepSeconds: 160,
				Sources: []telemetry.RefreshSource{{
					Name: telemetry.RefreshSourceCandidates, LastSuccessAt: &lastRefreshAt,
				}},
			},
			wantHealthStatus: "ok",
		},
		{
			name: "successful refresh clears attention",
			refresh: telemetry.Refresh{
				Status:           telemetry.RefreshStatusReady,
				FailureThreshold: 3,
				LastRefreshAt:    &lastRefreshAt,
				Sources: []telemetry.RefreshSource{{
					Name: telemetry.RefreshSourceCandidates, LastSuccessAt: &lastRefreshAt,
				}},
			},
			wantHealthStatus: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			mustSetWebProject(t, deps.Registry, "pyroapex", true)
			deps.TickLiveness = tickLivenessProbe{values: []telemetry.TickLiveness{{
				ProjectID: "pyroapex", Status: telemetry.TickLivenessStatusReady, LastTickAt: &now,
			}}}
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: now,
				Project:     telemetry.Project{ID: "pyroapex"},
				Projects: []telemetry.ProjectSnapshot{{
					Project: telemetry.Project{ID: "pyroapex"},
					Refresh: tt.refresh,
				}},
				Refresh: tt.refresh,
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{Now: func() time.Time { return now }}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if health["status"] != tt.wantHealthStatus {
				t.Fatalf("health status = %#v, want %q", health["status"], tt.wantHealthStatus)
			}
			failures, _ := health["refresh_failures"].([]any)
			if len(failures) != tt.wantFailures {
				t.Fatalf("refresh_failures = %#v, want %d entries", health["refresh_failures"], tt.wantFailures)
			}
			if tt.wantFailures > 0 {
				failure := failures[0].(map[string]any)
				if failure["project_id"] != "pyroapex" || failure["last_error"] != tt.refresh.LastError {
					t.Fatalf("refresh failure = %#v", failure)
				}
				projects := health["projects"].([]any)
				project := projects[0].(map[string]any)
				if project["status"] != "needs_human_attention" || project["last_error"] != tt.refresh.LastError {
					t.Fatalf("project health = %#v", project)
				}
			}
			liveness := health["tick_liveness"].([]any)
			if len(liveness) != 1 || liveness[0].(map[string]any)["status"] != string(telemetry.TickLivenessStatusReady) {
				t.Fatalf("tick_liveness = %#v, want advancing loop", liveness)
			}
		})
	}
}

func TestProjectStateAPIRendersConfiguredProjectWithoutTelemetryRows(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", true)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 16, 30, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/projects/detent/state", http.StatusOK)
	if got := nestedString(t, state, "counts", "running"); got != "0" {
		t.Fatalf("counts.running = %s, want 0", got)
	}
	if len(state["running"].([]any)) != 0 {
		t.Fatalf("running = %#v, want empty", state["running"])
	}
}

func TestTimeSeriesAPIRoutesReturnChartDatasets(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 17, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	mustSetWebProject(t, deps.Registry, "pyroapex", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project:    telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Counts:     telemetry.Counts{Running: 1, Queue: 2, Blocked: 1, Completed: 3},
				Tokens:     telemetry.Tokens{Total: 42},
				Throughput: telemetry.TokenThroughput{TokensPerSecond: 2.5},
			},
			{
				Project:    telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:     telemetry.Counts{Running: 2, Completed: 5},
				Tokens:     telemetry.Tokens{Total: 88},
				Throughput: telemetry.TokenThroughput{TokensPerSecond: 4.5},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	fleet := requestJSON(t, server, http.MethodGet, "/api/v1/timeseries?window=2m&bucket=1m", http.StatusOK)
	if fleet["scope"] != "fleet" {
		t.Fatalf("fleet scope = %#v, want fleet; payload = %#v", fleet["scope"], fleet)
	}
	if len(fleet["labels"].([]any)) == 0 || len(fleet["running_agents"].([]any)) != 2 || len(fleet["completions"].([]any)) != 2 {
		t.Fatalf("fleet datasets = %#v", fleet)
	}
	if fleet["board_flow"] != nil {
		t.Fatalf("fleet board_flow = %#v, want omitted", fleet["board_flow"])
	}

	projectPayload := requestJSON(t, server, http.MethodGet, "/api/v1/projects/detent/timeseries?window=2m&bucket=1m", http.StatusOK)
	if projectPayload["scope"] != "project" || projectPayload["project_id"] != "detent" {
		t.Fatalf("project scope = %#v project_id = %#v; payload = %#v", projectPayload["scope"], projectPayload["project_id"], projectPayload)
	}
	if len(projectPayload["running_agents"].([]any)) != 1 || len(projectPayload["board_flow"].([]any)) != 4 {
		t.Fatalf("project datasets = %#v", projectPayload)
	}

	invalid := requestJSON(t, server, http.MethodGet, "/api/v1/timeseries?window=not-a-duration", http.StatusBadRequest)
	if nestedString(t, invalid, "error", "code") != "invalid_duration" {
		t.Fatalf("invalid window payload = %#v", invalid)
	}
}

func TestHealthReportsDraining(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 2,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"draining"`) {
		t.Fatalf("body missing draining status:\n%s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"sessions_remaining":2`) {
		t.Fatalf("body missing sessions remaining:\n%s", rec.Body.String())
	}
}

func TestHealthReportsStrandedActiveIssues(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		StrandedActiveIssues: []telemetry.StrandedIssue{{
			ProjectID: "detent", Identifier: "digitaldrywood/detent#1606", DurationSeconds: 900, LastRefusalReason: "priority reservation",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	issues, ok := payload["stranded_active_issues"].([]any)
	if !ok || len(issues) != 1 {
		t.Fatalf("stranded_active_issues = %#v, want one issue", payload["stranded_active_issues"])
	}
	issue, ok := issues[0].(map[string]any)
	if !ok || issue["identifier"] != "digitaldrywood/detent#1606" || issue["last_refusal_reason"] != "priority reservation" {
		t.Fatalf("stranded_active_issues[0] = %#v", issues[0])
	}
}

func TestHealthReportsActiveDispatchLoops(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetWebProject(t, deps.Registry, "detent", false)
	completedAt := time.Date(2026, 8, 18, 16, 0, 0, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		DispatchLoops: []telemetry.DispatchLoop{{
			ProjectID: "detent", Identifier: "digitaldrywood/pyroapex#1850", Lane: "Rework", ConsecutiveDispatches: 2, DispatchLimit: 3, LastCompletedAt: &completedAt,
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if payload["status"] != "needs_attention" {
		t.Fatalf("status = %#v, want needs_attention", payload["status"])
	}
	loops, ok := payload["dispatch_loops"].([]any)
	if !ok || len(loops) != 1 {
		t.Fatalf("dispatch_loops = %#v, want one loop", payload["dispatch_loops"])
	}
	loop := loops[0].(map[string]any)
	if loop["identifier"] != "digitaldrywood/pyroapex#1850" || loop["lane"] != "Rework" || loop["consecutive_dispatches"] != float64(2) || loop["dispatch_limit"] != float64(3) || loop["tripped"] != false {
		t.Fatalf("dispatch_loops[0] = %#v", loop)
	}
	projects := payload["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["status"] != "needs_human_attention" {
		t.Fatalf("projects = %#v, want detent needs_human_attention", projects)
	}
}

func TestHealthDistinguishesProcessHealthFromProjectConnectorDegradation(t *testing.T) {
	t.Parallel()

	lastErrorAt := time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC)
	nextRetryAt := lastErrorAt.Add(30 * time.Second)
	deps := testDeps(t)
	if err := deps.Registry.SetPending(globalconfig.Project{ID: "alpha"}, project.RuntimeError{
		Message:     "provision project connector: github transient error",
		At:          lastErrorAt,
		NextRetryAt: nextRetryAt,
	}); err != nil {
		t.Fatalf("Registry.SetPending() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if payload["status"] != "ok" {
		t.Fatalf("status = %#v, want process health ok", payload["status"])
	}
	if payload["project_status"] != "degraded" {
		t.Fatalf("project_status = %#v, want degraded", payload["project_status"])
	}
	projects, ok := payload["projects"].([]any)
	if !ok || len(projects) != 1 {
		t.Fatalf("projects = %#v, want one degraded project", payload["projects"])
	}
	projectHealth, ok := projects[0].(map[string]any)
	if !ok {
		t.Fatalf("projects[0] = %#v, want object", projects[0])
	}
	if projectHealth["project_id"] != "alpha" || projectHealth["status"] != "degraded" {
		t.Fatalf("project health = %#v, want alpha degraded", projectHealth)
	}
	if projectHealth["last_error"] != "provision project connector: github transient error" {
		t.Fatalf("last_error = %#v", projectHealth["last_error"])
	}
	if projectHealth["next_retry_at"] != nextRetryAt.Format(time.RFC3339) {
		t.Fatalf("next_retry_at = %#v, want %q", projectHealth["next_retry_at"], nextRetryAt.Format(time.RFC3339))
	}
	if projectHealth["retry_stopped"] != false {
		t.Fatalf("retry_stopped = %#v, want false", projectHealth["retry_stopped"])
	}
}

func TestHealthReportsProjectPinnedToLastGoodWorkflow(t *testing.T) {
	updates := make(chan configwatcher.Update, 1)
	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent", Workflow: "WORKFLOW.md"},
		Workflow: workflowconfig.Workflow{
			Config:     workflowCfg,
			Prompt:     "last-good",
			SourceHash: "last-good-hash",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "memory"},
		Runner:    orchestrator.FakeRunner{},
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			return healthWorkflowWatcher{updates: updates}, nil
		},
	})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	if err := trackedProject.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := trackedProject.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Project.Stop() error = %v", err)
		}
	})
	deadline := time.Now().Add(time.Second)
	for !trackedProject.WorkflowSourceStatus().WatcherArmed {
		if time.Now().After(deadline) {
			t.Fatal("workflow watcher did not arm")
		}
		time.Sleep(10 * time.Millisecond)
	}
	updates <- configwatcher.Update{Path: "WORKFLOW.md", Err: errors.New("effort section missing"), At: time.Now()}
	for trackedProject.WorkflowSourceStatus().LastReloadError == "" {
		if time.Now().After(deadline) {
			t.Fatal("workflow reload failure was not recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}

	deps := testDeps(t)
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	if payload["project_status"] != "degraded" {
		t.Fatalf("project_status = %#v, want degraded", payload["project_status"])
	}
	projects := payload["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["status"] != "degraded" || !strings.Contains(projects[0].(map[string]any)["last_error"].(string), "workflow reload failed") {
		t.Fatalf("projects = %#v, want degraded reload failure", projects)
	}
	workflows := payload["workflows"].([]any)
	if len(workflows) != 1 || !strings.Contains(workflows[0].(map[string]any)["last_reload_error"].(string), "effort section missing") || workflows[0].(map[string]any)["reload_failed_at"] == nil {
		t.Fatalf("workflows = %#v, want persisted reload failure", workflows)
	}
}

func TestHealthRespondsDuringDrainWithoutSupplementalProjectReads(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	trackedProject, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg, Prompt: "Work the issue."},
	}, project.Dependencies{
		Connector: connectorProbe{name: "memory"},
		Runner: blockingHealthBudgetRunner{
			blocked: blocked,
			release: release,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 2,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
		result <- rec
	}()

	select {
	case rec := <-result:
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"draining"`) {
			t.Fatalf("draining health response = %d %s", rec.Code, rec.Body.String())
		}
		select {
		case <-blocked:
			t.Fatal("draining health request read supplemental project budget")
		default:
		}
	case <-time.After(250 * time.Millisecond):
		close(release)
		<-result
		t.Fatal("draining health request blocked on supplemental project budget")
	}
}

func TestHealthReportsRunningBuildDistinctFromAppliedUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		commit  string
	}{
		{name: "release build", version: "v1.3.0", commit: "abcdef123456"},
		{name: "development build", version: "dev", commit: "123456abcdef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lastCheck := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
			nextCheck := lastCheck.Add(24 * time.Hour)
			deps := testDeps(t)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: lastCheck,
				Update: telemetry.Update{
					Enabled:            true,
					CheckIntervalHours: 24,
					State:              "scheduled",
					LastCheckAt:        &lastCheck,
					LastAppliedVersion: "v1.2.4",
					NextCheckAt:        &nextCheck,
				},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			server, err := web.NewServer(web.Config{Build: buildinfo.Info{Version: tt.version, Commit: tt.commit}}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}
			payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if payload["version"] != tt.version || payload["commit"] != tt.commit {
				t.Fatalf("running build = version %#v commit %#v, want %q %q", payload["version"], payload["commit"], tt.version, tt.commit)
			}
			update, ok := payload["update"].(map[string]any)
			if !ok {
				t.Fatalf("update payload = %#v, want object", payload["update"])
			}
			if update["state"] != "scheduled" || update["last_applied_version"] != "v1.2.4" || update["next_check_at"] == nil {
				t.Fatalf("update payload = %#v", update)
			}
		})
	}
}

func TestHealthReportsUpdateDeferralAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC)
	pendingSince := now.Add(-4 * time.Hour)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Update: telemetry.Update{
			Enabled:          true,
			AutoApplyEnabled: true,
			State:            "pending_idle",
			PendingSince:     &pendingSince,
			MaxDeferralHours: 6,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{Now: func() time.Time { return now }}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	update, ok := payload["update"].(map[string]any)
	if !ok {
		t.Fatalf("update payload = %#v, want object", payload["update"])
	}
	if update["state"] != "pending_idle (4h, max 6h)" {
		t.Fatalf("update state = %#v, want deferral age", update["state"])
	}
}

func TestHealthReportsEnvironmentPath(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	payload := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	environment, ok := payload["environment"].(map[string]any)
	if !ok {
		t.Fatalf("environment payload = %#v, want object", payload["environment"])
	}
	if environment["path"] != os.Getenv("PATH") {
		t.Fatalf("environment path = %#v, want %q", environment["path"], os.Getenv("PATH"))
	}
}

func TestHealthReportsEnforcedProjectBudgets(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent"},
		Workflow: workflowconfig.Workflow{
			Config:     workflowCfg,
			Prompt:     "Work the issue.",
			SourceHash: "loaded-workflow-hash",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "memory"},
		Runner: healthBudgetRunner{budget: workflowconfig.Budget{
			Enabled:        true,
			PerDayMaxUSD:   250,
			PerIssueMaxUSD: 25,
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{
		`"project_id":"detent"`,
		`"enabled":true`,
		`"per_day_max_usd":250`,
		`"per_issue_max_usd":25`,
		`"source_hash":"loaded-workflow-hash"`,
		`"watcher_armed":false`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestAPIStateReportsDraining(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: requestedAt,
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 1,
			RequestedAt:       &requestedAt,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/state", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload struct {
		Status   string `json:"status"`
		Shutdown struct {
			Status            string `json:"status"`
			Draining          bool   `json:"draining"`
			SessionsRemaining int    `json:"sessions_remaining"`
			RequestedAt       string `json:"requested_at"`
		} `json:"shutdown"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v; body = %s", err, rec.Body.String())
	}
	if payload.Status != "draining" {
		t.Fatalf("Status = %q, want draining", payload.Status)
	}
	if payload.Shutdown.Status != "draining" || !payload.Shutdown.Draining || payload.Shutdown.SessionsRemaining != 1 {
		t.Fatalf("Shutdown = %#v, want draining with one session", payload.Shutdown)
	}
	if payload.Shutdown.RequestedAt != "2026-06-12T15:00:00Z" {
		t.Fatalf("Shutdown.RequestedAt = %q, want RFC3339 timestamp", payload.Shutdown.RequestedAt)
	}
}

func TestAPIStateRespondsDuringDrainWithoutStoreEnrichment(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	deps.Store = storeProbe{
		budgetCostEvents: func(ctx context.Context, _ store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
			select {
			case blocked <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, nil
			}
		},
	}
	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	workflowCfg.Budget.Enabled = true
	workflowCfg.Budget.PerDayMaxUSD = 100
	trackedProject, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg, Prompt: "Work the issue."},
	}, project.Dependencies{
		Connector: connectorProbe{name: "memory"},
		Runner: healthBudgetRunner{budget: workflowconfig.Budget{
			Enabled:      true,
			PerDayMaxUSD: 100,
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	requestedAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: requestedAt,
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 1,
			RequestedAt:       &requestedAt,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/state", nil))
		result <- rec
	}()

	select {
	case rec := <-result:
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"draining"`) {
			t.Fatalf("draining state response = %d %s", rec.Code, rec.Body.String())
		}
		select {
		case <-blocked:
			t.Fatal("draining state request enriched from the store")
		default:
		}
	case <-time.After(250 * time.Millisecond):
		close(release)
		<-result
		t.Fatal("draining state request blocked on store enrichment")
	}
}

func TestProjectStateAPIRespondsDuringConcurrentRefreshAndStoreEnrichment(t *testing.T) {
	t.Parallel()

	const requestDeadline = 500 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var logs bytes.Buffer
	deps := testDeps(t)
	deps.Store = storeProbe{
		cycleTimeReport: func(ctx context.Context) (store.CycleTimeReport, error) {
			startedOnce.Do(func() { close(started) })
			select {
			case <-release:
				return store.CycleTimeReport{}, nil
			case <-ctx.Done():
				return store.CycleTimeReport{}, ctx.Err()
			}
		},
	}
	generatedAt := time.Date(2026, 9, 4, 15, 4, 5, 0, time.UTC)
	publish := func(index int) {
		t.Helper()
		if err := deps.Hub.Publish(telemetry.Snapshot{
			GeneratedAt: generatedAt.Add(time.Duration(index) * time.Second),
			Projects: []telemetry.ProjectSnapshot{{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
			}},
		}); err != nil {
			t.Fatalf("Publish(%d) error = %v", index, err)
		}
	}
	publish(0)

	server, err := web.NewServer(web.Config{
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	enrichmentResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/reports", nil))
		enrichmentResult <- recorder
	}()
	select {
	case <-started:
	case <-time.After(requestDeadline):
		t.Fatal("project state enrichment did not reach the blocked store query")
	}

	for index := 1; index <= 8; index++ {
		publish(index)
		result := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			result <- projectStateRequest(server, context.Background())
		}()
		select {
		case recorder := <-result:
			if recorder.Code != http.StatusOK {
				t.Fatalf("request %d status = %d, want %d; body = %s", index, recorder.Code, http.StatusOK, recorder.Body.String())
			}
			var payload map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("request %d Unmarshal() error = %v", index, err)
			}
			if payload["generated_at"] != generatedAt.Add(time.Duration(index)*time.Second).Format(time.RFC3339) {
				t.Fatalf("request %d generated_at = %#v", index, payload["generated_at"])
			}
			enrichment, ok := payload["enrichment"].(map[string]any)
			if !ok || enrichment["status"] != "pending" {
				t.Fatalf("request %d enrichment = %#v, want pending", index, payload["enrichment"])
			}
		case <-time.After(requestDeadline):
			t.Fatalf("request %d exceeded %s while enrichment was blocked", index, requestDeadline)
		}
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := projectStateRequest(server, canceledCtx)
	if canceled.Code != http.StatusOK {
		t.Fatalf("canceled request status = %d, want %d; body = %s", canceled.Code, http.StatusOK, canceled.Body.String())
	}
	if strings.Contains(logs.String(), "context canceled") {
		t.Fatalf("request cancellation reached enrichment queries:\n%s", logs.String())
	}

	health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
	stateEndpoints, ok := health["state_endpoints"].(map[string]any)
	if !ok {
		t.Fatalf("state_endpoints = %#v", health["state_endpoints"])
	}
	projectEndpoint, ok := stateEndpoints["project"].(map[string]any)
	if !ok || projectEndpoint["request_count"].(float64) < 9 || projectEndpoint["target_latency_ms"] != float64(500) {
		t.Fatalf("project state endpoint telemetry = %#v", stateEndpoints["project"])
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case recorder := <-enrichmentResult:
		if recorder.Code != http.StatusOK {
			t.Fatalf("enrichment request status = %d, want %d", recorder.Code, http.StatusOK)
		}
	case <-time.After(requestDeadline):
		t.Fatal("dashboard enrichment request did not complete after store release")
	}
}

func TestHealthReportsStateEndpointLatencyAndCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		path               string
		endpoint           string
		requestContext     func() (context.Context, context.CancelFunc)
		wantOutcome        string
		wantEndpointStatus string
		wantHealthStatus   string
		wantTimeouts       float64
		wantCancellations  float64
	}{
		{
			name:     "fleet cancellation",
			path:     "/api/v1/state",
			endpoint: "fleet",
			requestContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			wantOutcome:        "canceled",
			wantEndpointStatus: "ok",
			wantHealthStatus:   "ok",
			wantCancellations:  1,
		},
		{
			name:     "project timeout",
			path:     "/api/v1/projects/detent/state",
			endpoint: "project",
			requestContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantOutcome:        "timeout",
			wantEndpointStatus: "degraded",
			wantHealthStatus:   "needs_attention",
			wantTimeouts:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 9, 4, 15, 4, 5, 0, time.UTC),
				Projects: []telemetry.ProjectSnapshot{{
					Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			ctx, cancel := tt.requestContext()
			defer cancel()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(ctx, http.MethodGet, tt.path, nil)
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("state status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
			}

			health := requestJSON(t, server, http.MethodGet, "/health", http.StatusOK)
			if health["status"] != tt.wantHealthStatus || health["ready"] != true || health["lifecycle"] != "ready" {
				t.Fatalf("health summary = %#v, want ready %s", health, tt.wantHealthStatus)
			}
			endpoints := health["state_endpoints"].(map[string]any)
			endpoint := endpoints[tt.endpoint].(map[string]any)
			if endpoint["status"] != tt.wantEndpointStatus || endpoint["last_outcome"] != tt.wantOutcome {
				t.Fatalf("%s endpoint = %#v", tt.endpoint, endpoint)
			}
			if endpoint["request_count"] != float64(1) || endpoint["target_latency_ms"] != float64(500) {
				t.Fatalf("%s endpoint counts = %#v", tt.endpoint, endpoint)
			}
			if endpoint["timeout_count"] != tt.wantTimeouts || endpoint["canceled_request_count"] != tt.wantCancellations {
				t.Fatalf("%s endpoint termination counts = %#v", tt.endpoint, endpoint)
			}
		})
	}
}

func projectStateRequest(server *web.Server, ctx context.Context) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/projects/detent/state", nil)
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestDashboardReadsLatestSnapshotWithoutSubscribing(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "issue-latest",
					Identifier: "digitaldrywood/detent#329",
					Title:      "Use latest snapshot",
					State:      "In Progress",
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	deps.Hub.Close()

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Use latest snapshot") {
		t.Fatalf("body missing latest snapshot:\n%s", rec.Body.String())
	}
}

func TestBoardFirstPaintDoesNotWaitForSnapshotEnrichment(t *testing.T) {
	t.Parallel()

	const synchronizationWatchdog = 2 * time.Minute
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var fetchCalls atomic.Int64
	deps := testDeps(t)
	deps.Connector = connectorProbe{
		name: "github",
		fetchCandidateIssues: func(context.Context) ([]connector.Issue, error) {
			fetchCalls.Add(1)
			return nil, errors.New("github rest budget reserved")
		},
	}
	deps.Store = storeProbe{
		runtimeEvidence: func(ctx context.Context, _ store.RuntimeEvidenceQuery) (store.RuntimeEvidence, error) {
			startedOnce.Do(func() { close(started) })
			select {
			case <-release:
				return store.RuntimeEvidence{}, errors.New("github rest budget reserved")
			case <-ctx.Done():
				return store.RuntimeEvidence{}, ctx.Err()
			}
		},
	}
	generatedAt := time.Date(2026, 7, 15, 15, 4, 5, 0, time.UTC)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		BoardIssues: []telemetry.Issue{{
			ID:         "issue-cached",
			Identifier: "digitaldrywood/detent#1318",
			Title:      "Cached board issue",
			State:      "In Progress",
		}},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	eventsCtx, cancelEvents := context.WithCancel(context.Background())
	t.Cleanup(cancelEvents)
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(eventsCtx, http.MethodGet, "/events?view=board", nil)
		server.Handler().ServeHTTP(rec, req)
	}()

	select {
	case <-started:
	case <-time.After(synchronizationWatchdog):
		t.Fatal("snapshot enrichment did not start before synchronization watchdog")
	}

	type response struct {
		code int
		body string
	}
	responseReady := make(chan response, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		server.Handler().ServeHTTP(rec, req)
		responseReady <- response{code: rec.Code, body: rec.Body.String()}
	}()

	select {
	case got := <-responseReady:
		if got.code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body = %s", got.code, http.StatusOK, got.body)
		}
		for _, want := range []string{"Cached board issue", generatedAt.Format(time.RFC3339), `id="live-clock"`} {
			if !strings.Contains(got.body, want) {
				t.Fatalf("body missing %q:\n%s", want, got.body)
			}
		}
	case <-time.After(synchronizationWatchdog):
		close(release)
		cancelEvents()
		<-responseReady
		t.Fatal("board first paint waited for blocked enrichment until synchronization watchdog")
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("FetchCandidateIssues calls = %d, want 0", got)
	}

	close(release)
	cancelEvents()
	select {
	case <-eventsDone:
	case <-time.After(synchronizationWatchdog):
		t.Fatal("event stream did not stop before synchronization watchdog")
	}
}

func TestBoardPageNavigationUsesCurrentInMemorySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		path              string
		wantBody          string
		wantPendingBudget bool
	}{
		{name: "fleet", path: "/fleet", wantBody: `id="snapshot"`},
		{name: "project overview", path: "/projects/detent", wantBody: "Recent runs", wantPendingBudget: true},
		{name: "project kanban", path: "/projects/detent/kanban", wantBody: "Cached project board issue"},
		{name: "project runs", path: "/projects/detent/runs", wantBody: "Runs"},
		{name: "project diagnostics", path: "/projects/detent/diagnostics", wantBody: "Diagnostics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			releaseStore := make(chan struct{})
			t.Cleanup(func() { close(releaseStore) })
			var fetchCalls atomic.Int64
			blockingStore := &renderBlockingStore{release: releaseStore}
			deps := testDeps(t)
			deps.Connector = connectorProbe{
				name: "github",
				fetchCandidateIssues: func(context.Context) ([]connector.Issue, error) {
					fetchCalls.Add(1)
					return nil, errors.New("unexpected tracker hydration")
				},
			}
			deps.Store = blockingStore
			if err := deps.Registry.Set(newBudgetTestProject(t, "detent", 100, 10)); err != nil {
				t.Fatalf("Registry.Set() error = %v", err)
			}
			mustSetWebProject(t, deps.Registry, "pyroapex", false)
			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 7, 16, 15, 4, 5, 0, time.UTC),
				Projects: []telemetry.ProjectSnapshot{
					{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
					{Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"}},
				},
				BoardIssues: []telemetry.Issue{{
					ID:         "issue-cached",
					Identifier: "digitaldrywood/detent#1356",
					Title:      "Cached project board issue",
					State:      "In Progress",
					ProjectID:  "detent",
				}},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			type response struct {
				code int
				body string
			}
			responseReady := make(chan response, 1)
			go func() {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, tt.path, nil)
				server.Handler().ServeHTTP(rec, req)
				responseReady <- response{code: rec.Code, body: rec.Body.String()}
			}()

			select {
			case got := <-responseReady:
				if got.code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body = %s", got.code, http.StatusOK, got.body)
				}
				if !strings.Contains(got.body, tt.wantBody) {
					t.Fatalf("body missing %q:\n%s", tt.wantBody, got.body)
				}
				if tt.wantPendingBudget {
					if !strings.Contains(got.body, "Loading budget data…") {
						t.Fatalf("body missing pending budget state:\n%s", got.body)
					}
					if strings.Contains(got.body, "$0.00 / $100.00") {
						t.Fatalf("body rendered zero spend while budget enrichment was pending:\n%s", got.body)
					}
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("board navigation exceeded 500ms while current in-memory state was available")
			}
			if got := fetchCalls.Load(); got != 0 {
				t.Fatalf("FetchCandidateIssues calls = %d, want 0", got)
			}
			if got := blockingStore.calls.Load(); got != 0 {
				t.Fatalf("render-path store calls = %d, want 0", got)
			}
		})
	}
}

func TestBoardFirstPaintColdBootRendersPlaceholders(t *testing.T) {
	t.Parallel()

	var runtimeEvidenceCalls atomic.Int64
	registry := project.NewRegistry()
	if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10)); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	deps := testDeps(t)
	deps.Registry = registry
	deps.Store = storeProbe{
		runtimeEvidence: func(context.Context, store.RuntimeEvidenceQuery) (store.RuntimeEvidence, error) {
			runtimeEvidenceCalls.Add(1)
			return store.RuntimeEvidence{}, nil
		},
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{`class="dt-skeleton`, `id="live-clock"`, "--:--:--"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), "Connect a repository") {
		t.Fatalf("configured cold boot rendered unconfigured first-run state:\n%s", rec.Body.String())
	}
	if got := runtimeEvidenceCalls.Load(); got != 0 {
		t.Fatalf("RuntimeEvidence calls = %d, want 0", got)
	}
}

func TestDashboardEnrichesCycleTimeFromStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usageStore := openWebTestStore(t)
	startedAt := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	sessionID, err := usageStore.StartSession(ctx, store.SessionStart{
		IssueID:    "issue-215",
		Identifier: "digitaldrywood/detent#215",
		StartedAt:  startedAt,
		Model:      "gpt-5-codex",
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := usageStore.FinishSession(ctx, sessionID, store.SessionFinish{
		CompletedAt:    startedAt.Add(90 * time.Minute),
		RuntimeSeconds: int64(90 * time.Minute / time.Second),
		FinalState:     "completed",
		Model:          "gpt-5-codex",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}

	deps := testDeps(t)
	deps.Store = usageStore
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: startedAt.Add(2 * time.Hour)}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/reports", nil)
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{
		"Cycle time",
		"1 completed",
		"1h 30m",
		"<title>1-4h: 1 issues</title>",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("reports page missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestDashboardRendersServerMetadata(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		StaticDir: t.TempDir(),
		Version:   "v9.8.7",
		Build: buildinfo.Info{
			Version: "v9.8.7",
			Commit:  "abcdef1234567890",
			Date:    "2026-06-05T21:00:00Z",
		},
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
	req.Host = "dashboard.example.test:4100"

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{
		"v9.8.7",
		`href="/"`,
		`href="/analytics"`,
		`href="/reports"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), "Build v9.8.7 (abcdef1) 2026-06-05T21:00:00Z") {
		t.Fatalf("fleet page rendered the full build string; it belongs to Settings:\n%s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `href="http://localhost:4000"`) {
		t.Fatalf("dashboard rendered the dashboard URL chip:\n%s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "http://dashboard.example.test:4100") {
		t.Fatalf("dashboard link used request host:\n%s", rec.Body.String())
	}
}

func TestDashboardWiresHTMXSSE(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
		Refresh: telemetry.Refresh{
			Status: telemetry.RefreshStatusReady,
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{DashboardURL: "http://localhost:4101"}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	for _, want := range []string{
		`href="/"`,
		`src="https://unpkg.com/htmx.org@2.0.4"`,
		`src="https://cdn.jsdelivr.net/npm/htmx-ext-sse@2.2.4"`,
		`src="https://cdn.jsdelivr.net/npm/idiomorph@0.7.3/dist/idiomorph-ext.min.js"`,
		`hx-ext="sse, morph"`,
		`sse-connect="/events?view=fleet"`,
		`sse-swap="snapshot"`,
		`sse-swap="live-status"`,
		`hx-swap="morph:innerHTML"`,
		`id="agent-activity"`,
		`id="fleet-pr-pipeline"`,
		`id="fleet-metrics"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("dashboard missing %q:\n%s", want, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), `hx-ext="sse morph"`) {
		t.Fatalf("dashboard rendered space-separated htmx extensions:\n%s", rec.Body.String())
	}
	for _, forbidden := range []string{
		`/static/vendor/chartjs/chart.umd.min`,
		`/static/js/dashboard-charts`,
		`data-detent-charts`,
	} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("fleet page rendered forbidden Chart.js wiring %q:\n%s", forbidden, rec.Body.String())
		}
	}
}

func TestSettingsRendersConfigProjectsAndRuntimePaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, "global.yaml")
	workflowPath := filepath.Join(root, "WORKFLOW.md")
	workdir := filepath.Join(root, "repo")
	worktreeRoot := filepath.Join(root, "worktrees")
	dbPath := filepath.Join(root, "detent.db")
	logPath := filepath.Join(root, "detent.log")
	projectURL := "https://github.com/orgs/digitaldrywood/projects/4"
	workflowDetailsURL := projectURL + "/workflows"

	registry := project.NewRegistry()
	trackedProject := newSettingsTestProject(t, globalconfig.Project{
		ID:       "detent",
		Workflow: workflowPath,
		Workdir:  workdir,
		Weight:   3,
		Priority: 2,
		Paused:   true,
	}, worktreeRoot, projectURL)
	if err := registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	deps := testDeps(t)
	deps.Registry = registry
	server, err := web.NewServer(web.Config{
		StaticDir:      t.TempDir(),
		Version:        "v1.2.3",
		GlobalConfig:   globalconfig.Config{Path: configPath},
		ConfigPathRule: globalconfig.PathRuleFlag,
		RuntimeDBPath:  dbPath,
		RuntimeLogPath: logPath,
		ServerAddress:  "127.0.0.1:4101",
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/settings", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{
		"Settings",
		"Read-only view of the running configuration.",
		"Live reload",
		"Restart required",
		`href="/"`,
		`href="/reports"`,
		`href="/settings"`,
		`aria-current="page"`,
		"v1.2.3",
		"Resolved global config path",
		configPath,
		string(globalconfig.PathRuleFlag),
		"detent",
		workflowPath,
		workdir,
		worktreeRoot,
		">3</span>",
		">2</span>",
		">true</span>",
		"github",
		projectURL,
		`href="` + workflowDetailsURL + `"`,
		`aria-label="Open detent workflow details"`,
		`target="_blank"`,
		`rel="noopener noreferrer"`,
		"View workflow ↗",
		"Dependency auto-unblock",
		"enabled: Blocked, Waiting -&gt; Todo when terminal_or_merged",
		dbPath,
		logPath,
		"127.0.0.1:4101",
		"navigator.clipboard.writeText",
		"Copied!",
		"Build v1.2.3",
		"Theme",
		"Density",
		`data-copy="` + configPath + `"`,
		`data-copy="` + workflowPath + `"`,
		`data-copy="` + workdir + `"`,
		`data-copy="` + worktreeRoot + `"`,
		`data-copy="` + projectURL + `"`,
		`data-copy="` + dbPath + `"`,
		`data-copy="` + logPath + `"`,
		`data-copy="127.0.0.1:4101"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestServerEventsReplaysLatestSnapshot(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{Counts: telemetry.Counts{Running: 2, Queue: 3, Blocked: 1, Completed: 5}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := openEventStream(t, server)

	event := readSSEEvent(t, body)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{"Running", "2", "Queue", "3", "Blocked", "1", "Completed", "5"} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("snapshot event missing %q:\n%s", want, event.data)
		}
	}
}

func TestServerEventsStartsWithBuildVersion(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		Version:         "v9.8.7",
		SSETickInterval: time.Hour,
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := openEventStream(t, server)

	event := readSSEFrame(t, body)
	if event.name != "build" {
		t.Fatalf("event name = %q, want build", event.name)
	}
	for _, want := range []string{`id="detent-build-version"`, `data-detent-build-version="v9.8.7"`, "v9.8.7"} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("build event missing %q:\n%s", want, event.data)
		}
	}
}

func TestServerEventsStreamsPublishedSnapshots(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := openEventStream(t, server)

	if err := deps.Hub.Publish(telemetry.Snapshot{Counts: telemetry.Counts{Running: 4}}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	event := readSSEEvent(t, body)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	if !strings.Contains(event.data, "4") {
		t.Fatalf("snapshot event missing running count:\n%s", event.data)
	}
}

func TestServerEventsStreamsSidebarGitHubAPIHealth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 14, 30, 0, 0, time.UTC)
	backoffUntil := now.Add(5 * time.Minute)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		RateLimits: &telemetry.RateLimits{
			GitHubREST:    &telemetry.RateLimitBucket{Remaining: 4878, Used: 122, Limit: 5000},
			GitHubGraphQL: &telemetry.RateLimitBucket{Remaining: 4880, Used: 120, Limit: 5000},
			RESTUsage: &telemetry.RESTUsage{
				RateLimited:  true,
				BackoffUntil: &backoffUntil,
				Contributors: []telemetry.RESTUsageContributor{
					{EndpointFamily: "pull requests", Count: 2, RateLimited: true, LastStatus: 429},
					{EndpointFamily: "check runs", Count: 1, RateLimited: true, LastStatus: 429},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr)

	var event sseEvent
	for index, want := range []string{"snapshot", "live-status", "github-api-health"} {
		event = readRawSSEEvent(t, conn, reader)
		if event.name != want {
			t.Fatalf("event %d name = %q, want %q", index+1, event.name, want)
		}
	}
	for _, want := range []string{
		`id="github-api-health"`,
		`href="/health/ui"`,
		`sse-swap="github-api-health"`,
		`hx-swap="morph:outerHTML"`,
		`data-dashboard-static-nav="health"`,
		"Health",
		"Backoff",
		"GitHub secondary throttle active for pull requests/check runs",
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("github api health event missing %q:\n%s", want, event.data)
		}
	}
	if event := readRawSSEEvent(t, conn, reader); event.name != "sidebar-v2" {
		t.Fatalf("fourth event name = %q, want sidebar-v2", event.name)
	}
	for _, forbidden := range []string{
		`data-preserve-details="github-api-health"`,
		"Primary REST quota is healthy",
		"Retrying at 14:35 UTC",
		"REST primary",
		"GraphQL primary",
	} {
		if strings.Contains(event.data, forbidden) {
			t.Fatalf("github api health event rendered diagnostic content %q:\n%s", forbidden, event.data)
		}
	}
}

func TestServerEventsPreserveProjectKanbanVisibilityMetadata(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, deps.Connector)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{
					ID:          "detent",
					DisplayName: "Detent",
				},
			},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "todo",
				Identifier: "digitaldrywood/detent#496",
				ProjectID:  "detent",
				Title:      "Fix empty-lane toggle",
				State:      "Todo",
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	ctx, cancel := context.WithTimeout(t.Context(), sseTestOperationTimeout)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events?project=detent&view=kanban", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("ReadAll() error = %v", readErr)
		}
		t.Fatalf("status = %d, want %d; body = %s", resp.StatusCode, http.StatusOK, string(body))
	}

	event := readSSEEvent(t, resp.Body)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{
		`data-board-key="project.detent"`,
		`data-board-lane-default=`,
		`data-board-lane-picker`,
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("snapshot event missing %q:\n%s", want, event.data)
		}
	}
}

func TestProjectKanbanRendersConfiguredDispatchPriorityLabel(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, deps.Connector, "hotfix", "bug")
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 9, 20, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "hotfix",
				Identifier: "digitaldrywood/detent#1128",
				ProjectID:  "detent",
				Title:      "Prioritized incident fix",
				State:      "Todo",
				Labels:     []string{"hotfix"},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{StaticDir: t.TempDir()}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	body := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/kanban", http.StatusOK)
	for _, want := range []string{
		`id="card-digitaldrywood-detent-1128-priority"`,
		`data-board-priority`,
		`data-board-priority-top="true"`,
		`data-help-description="Label hotfix is configured at dispatch label rank 1."`,
		`>hotfix</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("project Kanban missing configured priority marker %q:\n%s", want, body)
		}
	}
}

func TestServerEventsProjectKanbanUsesReloadedConfigOnRepublishedSnapshot(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	actionConnector := &kanbanActionConnector{}
	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeReadOnly,
	}, actionConnector)
	snapshot := telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 20, 15, 0, 0, 0, time.UTC),
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{
					ID:          "detent",
					DisplayName: "Detent",
				},
			},
		},
		BoardIssues: []telemetry.Issue{
			{
				ID:         "todo",
				Identifier: "digitaldrywood/detent#565",
				ProjectID:  "detent",
				Title:      "Live reload Kanban mode",
				State:      "Todo",
			},
		},
	}
	if err := deps.Hub.Publish(snapshot); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{
		SSETickInterval:     time.Hour,
		SSEFragmentInterval: -1,
		SSEHealthInterval:   -1,
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?project=detent&view=kanban")

	event := readRawSSEEvent(t, conn, reader)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	if !strings.Contains(event.data, "Live reload Kanban mode") {
		t.Fatalf("initial snapshot event missing board card:\n%s", event.data)
	}
	if strings.Contains(event.data, `hx-get="/api/v1/kanban/move?`) {
		t.Fatalf("board snapshot rendered move controls:\n%s", event.data)
	}
	liveStatusEvent := readRawSSEEvent(t, conn, reader)
	if liveStatusEvent.name != "live-status" {
		t.Fatalf("event name = %q, want live-status", liveStatusEvent.name)
	}
	healthEvent := readRawSSEEvent(t, conn, reader)
	if healthEvent.name != "github-api-health" {
		t.Fatalf("event name = %q, want github-api-health", healthEvent.name)
	}
	sidebarV2Event := readRawSSEEvent(t, conn, reader)
	if sidebarV2Event.name != "sidebar-v2" {
		t.Fatalf("event name = %q, want sidebar-v2", sidebarV2Event.name)
	}

	mustSetKanbanProject(t, deps.Registry, "detent", workflowconfig.Kanban{
		Mode: workflowconfig.KanbanModeIntegration,
	}, actionConnector)
	snapshot.BoardIssues[0].State = "In Progress"
	snapshot.GeneratedAt = snapshot.GeneratedAt.Add(time.Minute)
	if err := deps.Hub.Publish(snapshot); err != nil {
		t.Fatalf("republish snapshot error = %v", err)
	}

	event = readRawSSEEvent(t, conn, reader)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	if !strings.Contains(event.data, `data-board-lane="in-progress"`) {
		t.Fatalf("reloaded snapshot event missing moved card lane:\n%s", event.data)
	}
}

func TestServerEventsStreamsSidebarUpdates(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr)

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
		BoardIssues: []telemetry.Issue{
			{ID: "detent-1", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-2", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-3", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-4", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-5", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-6", ProjectID: "detent", State: "In Progress"},
			{ID: "detent-7", ProjectID: "detent", State: "In Progress"},
		},
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{
					ID:          "detent",
					DisplayName: "Detent",
				},
				Counts: telemetry.Counts{
					Running: 7,
				},
			},
			{
				Project: telemetry.Project{
					ID:          "docs",
					DisplayName: "Docs",
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	snapshotEvent := readRawSSEEvent(t, conn, reader)
	if snapshotEvent.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
	}
	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	for _, want := range []string{
		"Detent",
		`href="/projects/detent/kanban"`,
		`data-sidebar-project="detent"`,
		`data-sidebar-project-status="active"`,
		`data-sidebar-project-badge`,
		`aria-label="7 board load"`,
		">7</span>",
	} {
		if !strings.Contains(sidebarEvent.data, want) {
			t.Fatalf("sidebar event missing %q:\n%s", want, sidebarEvent.data)
		}
	}
	for _, forbidden := range []string{
		`data-tui-sidebar-state=`,
		`data-tui-sidebar-trigger`,
		`sse-swap="sidebar"`,
	} {
		if strings.Contains(sidebarEvent.data, forbidden) {
			t.Fatalf("sidebar event rendered wrapper marker %q:\n%s", forbidden, sidebarEvent.data)
		}
	}
}

func TestServerEventsPreservesStaticSidebarNavigation(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?nav=reports")

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	snapshotEvent := readRawSSEEvent(t, conn, reader)
	if snapshotEvent.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
	}
	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	assertAppSidebarLinkActive(t, sidebarEvent.data, "/reports")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/health/ui")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/analytics")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/settings")
}

func TestServerEventsStreamsAnalyticsSnapshotForAnalyticsNav(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?nav=analytics")

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
		BoardIssues: []telemetry.Issue{
			{ID: "issue-1", State: "Todo"},
		},
		CycleTime: telemetry.CycleTimeReport{
			Available:      true,
			AverageSeconds: int64(90 * time.Minute / time.Second),
			Buckets:        []telemetry.CycleTimeBucket{{Label: "1-4h", Count: 1}},
		},
		Budget: telemetry.Budget{Enabled: true},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	snapshotEvent := readRawSSEEvent(t, conn, reader)
	if snapshotEvent.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
	}
	for _, want := range []string{
		`id="analytics-summary"`,
		`id="analytics-log"`,
		"Scheduler internals",
	} {
		if !strings.Contains(snapshotEvent.data, want) {
			t.Fatalf("analytics snapshot event missing %q:\n%s", want, snapshotEvent.data)
		}
	}

	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	assertAppSidebarLinkActive(t, sidebarEvent.data, "/analytics")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/health/ui")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/reports")
}

func TestServerEventsStreamsHealthSnapshotForHealthNav(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 14, 30, 0, 0, time.UTC)
	backoffUntil := now.Add(5 * time.Minute)
	lastRefreshAt := now.Add(-90 * time.Second)
	nextRefreshAt := now.Add(-30 * time.Second)
	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour, Now: func() time.Time { return now }}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr, "/events?nav=health")

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Refresh: telemetry.Refresh{
			PollIntervalSeconds: 60,
			StaleAfterSeconds:   120,
			Status:              telemetry.RefreshStatusReady,
			LastRefreshAt:       &lastRefreshAt,
			NextRefreshAt:       &nextRefreshAt,
		},
		RateLimits: &telemetry.RateLimits{
			GitHubREST:    &telemetry.RateLimitBucket{Remaining: 4878, Used: 122, Limit: 5000},
			GitHubGraphQL: &telemetry.RateLimitBucket{Remaining: 4880, Used: 120, Limit: 5000},
			RESTUsage: &telemetry.RESTUsage{
				RateLimited:  true,
				BackoffUntil: &backoffUntil,
				Contributors: []telemetry.RESTUsageContributor{
					{EndpointFamily: "pull requests", Count: 2, RateLimited: true, LastStatus: 429},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	snapshotEvent := readRawSSEEvent(t, conn, reader)
	if snapshotEvent.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
	}
	for _, want := range []string{
		`id="health-verdict"`,
		`id="health-details"`,
		`id="health-scheduler"`,
		`id="health-update"`,
		"All systems nominal",
	} {
		if !strings.Contains(snapshotEvent.data, want) {
			t.Fatalf("health snapshot event missing %q:\n%s", want, snapshotEvent.data)
		}
	}
	for _, unwanted := range []string{`id="health-github-rest"`, `id="health-github-graphql"`, "Requests in backoff", "Loop behind"} {
		if strings.Contains(snapshotEvent.data, unwanted) {
			t.Fatalf("health snapshot event included diagnostic %q:\n%s", unwanted, snapshotEvent.data)
		}
	}

	sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
	assertAppSidebarLinkActive(t, sidebarEvent.data, "/health/ui")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/")
	assertAppSidebarLinkInactive(t, sidebarEvent.data, "/analytics")
}

func TestServerEventsPreserveProjectContextForStaticSidebarNavigation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{
			name: "reports",
			path: "/events?nav=reports&project=detent",
		},
		{
			name: "settings",
			path: "/events?nav=settings&project=detent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			mustSetWebProject(t, deps.Registry, "detent", false)
			mustSetWebProject(t, deps.Registry, "docs", false)
			server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			addr := startWebServer(t, server)
			conn, reader := openRawEventStream(t, addr, tt.path)

			if err := deps.Hub.Publish(telemetry.Snapshot{
				GeneratedAt: time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC),
				Projects: []telemetry.ProjectSnapshot{
					{
						Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
						Counts:  telemetry.Counts{Running: 7},
					},
					{Project: telemetry.Project{ID: "docs", DisplayName: "Docs"}},
				},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			snapshotEvent := readRawSSEEvent(t, conn, reader)
			if snapshotEvent.name != "snapshot" {
				t.Fatalf("event name = %q, want snapshot", snapshotEvent.name)
			}
			sidebarEvent := readRawSSEEventNamed(t, conn, reader, "sidebar-v2")
			for _, want := range []string{
				`href="/projects/detent/kanban"`,
				`data-sidebar-project="detent"`,
				`href="/health/ui"`,
				`href="/analytics"`,
				`href="/reports"`,
				`href="/settings"`,
			} {
				if !strings.Contains(sidebarEvent.data, want) {
					t.Fatalf("sidebar event missing project-context marker %q:\n%s", want, sidebarEvent.data)
				}
			}
			assertAppSidebarLinkActive(t, sidebarEvent.data, "/projects/detent/kanban")
			for _, href := range []string{"/", "/health/ui", "/analytics", "/reports", "/settings"} {
				assertAppSidebarLinkInactive(t, sidebarEvent.data, href)
			}
		})
	}
}

func TestServerEventsEnrichesSnapshotOncePerPublish(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name              string
		reverseClockOrder bool
	}{
		{name: "concurrent subscribers"},
		{name: "earlier clock sample arrives after enrichment starts", reverseClockOrder: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			generatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
			year, month, day := generatedAt.UTC().Date()
			budgetPeriodStart := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
			budgetQueryFrom := budgetPeriodStart.AddDate(0, 0, -6)

			enrichmentStarted := make(chan struct{})
			var enrichmentStartedOnce sync.Once
			var clockCalls atomic.Int64
			var cycleTimeCalls atomic.Int64
			var budgetCalls atomic.Int64

			registry := project.NewRegistry()
			if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10, workflowconfig.BillingModeSubscription)); err != nil {
				t.Fatalf("Registry.Set() error = %v", err)
			}

			deps := testDeps(t)
			deps.Registry = registry
			deps.Store = storeProbe{
				cycleTimeReport: func(context.Context) (store.CycleTimeReport, error) {
					cycleTimeCalls.Add(1)
					enrichmentStartedOnce.Do(func() { close(enrichmentStarted) })
					return store.CycleTimeReport{}, nil
				},
				budgetCostEvents: func(_ context.Context, query store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
					if query.From.Equal(budgetQueryFrom) {
						budgetCalls.Add(1)
					}
					return nil, nil
				},
			}
			server, err := web.NewServer(web.Config{SSETickInterval: time.Hour, Now: func() time.Time {
				if !tt.reverseClockOrder {
					return time.Now()
				}
				if clockCalls.Add(1) == 1 {
					<-enrichmentStarted
					return generatedAt
				}
				return generatedAt.Add(time.Nanosecond)
			}}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			first := openEventStream(t, server)
			second := openEventStream(t, server)
			t.Cleanup(func() { enrichmentStartedOnce.Do(func() { close(enrichmentStarted) }) })

			if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: generatedAt}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			for _, body := range []io.Reader{first, second} {
				event := readSSEEvent(t, body)
				if event.name != "snapshot" {
					t.Fatalf("event name = %q, want snapshot", event.name)
				}
			}

			if got := cycleTimeCalls.Load(); got != 1 {
				t.Fatalf("CycleTimeReport calls = %d, want 1", got)
			}
			if got := budgetCalls.Load(); got != 1 {
				t.Fatalf("BudgetCostEvents calls = %d, want 1", got)
			}
		})
	}
}

func TestStateRequestCancellationDoesNotReachSnapshotEnrichment(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var budgetCalls atomic.Int64

	registry := project.NewRegistry()
	if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10)); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	deps := testDeps(t)
	deps.Registry = registry
	deps.Store = storeProbe{
		budgetCostEvents: func(ctx context.Context, _ store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
			budgetCalls.Add(1)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		},
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: generatedAt}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/state", nil)
	server.Handler().ServeHTTP(rec, req)
	if got := budgetCalls.Load(); got != 0 {
		t.Fatalf("BudgetCostEvents calls after canceled state request = %d, want 0", got)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	budget := state["budget"].(map[string]any)
	if reason, ok := budget["degraded_reason"]; ok {
		t.Fatalf("budget degraded_reason = %#v, want omitted", reason)
	}
	if got := budgetCalls.Load(); got != 2 {
		t.Fatalf("BudgetCostEvents calls = %d, want snapshot and project dashboard enrichment calls", got)
	}
}

func TestServerEventsStreamsLiveDashboardSections(t *testing.T) {
	t.Parallel()

	perDay := 25.0
	perIssue := 5.0
	deps := testDeps(t)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := openEventStream(t, server)
	generatedAt := time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC)

	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		Counts: telemetry.Counts{
			Running:   5,
			Queue:     4,
			Blocked:   3,
			Completed: 2,
		},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "issue-live",
					Identifier: "DD-LIVE",
					Title:      "Live dashboard row",
					State:      "In Progress",
				},
				SessionID:   "thread-live",
				TurnCount:   6,
				StartedAt:   generatedAt.Add(-6 * time.Minute),
				DiffAdded:   4,
				DiffRemoved: 2,
				DiffFiles:   3,
				DiffStatus:  "ok",
				Tokens: telemetry.Tokens{
					Input:  100,
					Output: 221,
					Total:  321,
				},
			},
		},
		Completed: []telemetry.Completed{
			{
				Issue: telemetry.Issue{
					ID:         "issue-done-1",
					Identifier: "DD-DONE-1",
				},
				StartedAt:   generatedAt.Add(-2 * time.Minute),
				CompletedAt: generatedAt.Add(-45 * time.Second),
			},
			{
				Issue: telemetry.Issue{
					ID:         "issue-done-2",
					Identifier: "DD-DONE-2",
				},
				StartedAt:   generatedAt.Add(-5 * time.Minute),
				CompletedAt: generatedAt.Add(-3 * time.Minute),
			},
		},
		Budget: telemetry.Budget{
			Enabled:          true,
			PerDayMaxUSD:     &perDay,
			PerIssueMaxUSD:   &perIssue,
			CurrentSpendUSD:  12.34,
			ProjectedCostUSD: 20,
		},
		RateLimits: &telemetry.RateLimits{
			LimitName: "Codex",
			Primary: &telemetry.RateLimitBucket{
				Remaining:      87,
				Used:           13,
				Limit:          100,
				ResetInSeconds: 3600,
			},
		},
		Tokens: telemetry.Tokens{
			Input:          100,
			Output:         221,
			Total:          321,
			RuntimeSeconds: 60,
		},
		Throughput: telemetry.TokenThroughput{
			TokensPerSecond: 2.85,
			WindowSeconds:   60,
			Tokens:          171,
		},
		TokenTrend: []telemetry.TokenTrendPoint{
			{
				At:     time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC),
				Input:  50,
				Output: 100,
				Total:  150,
			},
			{
				At:     time.Date(2026, 5, 31, 15, 1, 0, 0, time.UTC),
				Input:  100,
				Output: 221,
				Total:  321,
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	event := readSSEEvent(t, body)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{
		"Running",
		"5",
		"Queue",
		"4",
		"Blocked",
		"3",
		"Completed",
		"2",
		"DD-LIVE",
		"Live dashboard row",
		"+4 -2 (3 files)",
		"321",
		"Rate limits",
		"Primary",
		"87",
		"13",
		"100",
		"Token trend",
		"Input {{detent-time:time:2026-05-31T15:01:00Z}}: 100 tokens",
		"Output {{detent-time:time:2026-05-31T15:01:00Z}}: 221 tokens",
		"Token throughput",
		"2.9 tps",
		"Last 1m token throughput",
		"Runtime",
		"1m 0s",
		"Agent activity",
		"Live now",
		"DD-LIVE",
		"DD-DONE-1",
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("snapshot event missing %q:\n%s", want, event.data)
		}
	}
}

func TestServerEventsSendsTickEvents(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{SSETickInterval: time.Millisecond}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	body := openEventStream(t, server)

	event := readSSEEvent(t, body)
	if event.name != "tick" {
		t.Fatalf("event name = %q, want tick", event.name)
	}
	if strings.TrimSpace(event.data) == "" {
		t.Fatal("tick event data is empty")
	}
}

func TestServerEventsStreamsPastHTTPTimeouts(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{
		SSETickInterval:       75 * time.Millisecond,
		HTTPReadHeaderTimeout: 25 * time.Millisecond,
		HTTPIdleTimeout:       25 * time.Millisecond,
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	conn, reader := openRawEventStream(t, addr)

	for range 2 {
		event := readRawSSEEvent(t, conn, reader)
		if event.name != "tick" {
			t.Fatalf("event name = %q, want tick", event.name)
		}
		if strings.TrimSpace(event.data) == "" {
			t.Fatal("tick event data is empty")
		}
	}
}

func TestServerReadHeaderTimeoutDropsStalledHeaders(t *testing.T) {
	t.Parallel()

	readHeaderTimeout := 100 * time.Millisecond
	server, err := web.NewServer(web.Config{
		HTTPReadHeaderTimeout: readHeaderTimeout,
		HTTPIdleTimeout:       time.Second,
	}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	addr := startWebServer(t, server)
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	if _, err := io.WriteString(conn, "GET /health HTTP/1.1\r\nHost: "+addr+"\r\nX-Slow:"); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}

	start := time.Now()
	if err := conn.SetReadDeadline(start.Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	var buf [64]byte
	for {
		_, err := conn.Read(buf[:])
		if err == nil {
			continue
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatalf("connection remained open after %v", time.Since(start))
		}
		if elapsed := time.Since(start); elapsed > readHeaderTimeout+500*time.Millisecond {
			t.Fatalf("connection closed after %v, want close near %v", elapsed, readHeaderTimeout)
		}
		return
	}
}

func TestServerAPIRoutes(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 5, 31, 3, 25, 0, 0, time.UTC)
	startedAt := generatedAt.Add(-5 * time.Minute)
	blockedAt := generatedAt.Add(-2 * time.Minute)
	lastEventAt := generatedAt.Add(-time.Minute)
	dueAt := generatedAt.Add(time.Minute)
	completedAt := generatedAt.Add(-30 * time.Second)
	perDay := 50.0
	runningContextWindow := int64(40)
	completedContextWindow := int64(600)
	totalContextWindow := int64(100)

	events := hub.New[telemetry.Snapshot]()
	if err := events.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		Events: []telemetry.ActivityEvent{
			{
				At:      generatedAt.Add(-20 * time.Second),
				Event:   "workspace_reap_succeeded",
				Message: "workspace cleanup succeeded for digitaldrywood/detent#586 reason=cancelled worktrees=1 branches=1 processes=2",
			},
		},
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:          "issue-running",
					Identifier:  "digitaldrywood/detent#37",
					URL:         "https://github.com/digitaldrywood/detent/issues/37",
					Title:       "REST API",
					Description: strings.Repeat("api ", 90),
					State:       "In Progress",
					PullRequest: &telemetry.PullRequest{
						Number: 137,
						URL:    "https://github.com/digitaldrywood/detent/pull/137",
					},
				},
				WorkerHost:    "host-a",
				WorkspacePath: "/workspaces/DD-37",
				SessionID:     "thread-running",
				TurnCount:     3,
				StartedAt:     startedAt,
				LastEventAt:   &lastEventAt,
				LastEvent:     "notification",
				LastMessage:   "rendered",
				RecentEvents: []telemetry.ActivityEvent{
					{At: lastEventAt.Add(-time.Second), Event: "turn_started", Message: "turn started"},
					{At: lastEventAt, Event: "notification", Message: "rendered"},
				},
				RuntimeSeconds: 300,
				DiffAdded:      4,
				DiffRemoved:    2,
				DiffFiles:      3,
				DiffStatus:     "ok",
				Tokens: telemetry.Tokens{
					Input:              10,
					CachedInput:        5,
					Output:             20,
					Total:              30,
					ModelContextWindow: &runningContextWindow,
				},
			},
		},
		Queue: []telemetry.Queued{
			{
				Issue: telemetry.Issue{
					ID:         "issue-retry",
					Identifier: "DD-RETRY",
					URL:        "https://github.com/digitaldrywood/detent/issues/38",
					Title:      "Retry API",
					State:      "Todo",
				},
				Attempt:       2,
				DueAt:         &dueAt,
				Error:         "no available orchestrator slots",
				WorkspacePath: "/workspaces/DD-RETRY",
			},
		},
		Blocked: []telemetry.Blocked{
			{
				Issue: telemetry.Issue{
					ID:         "issue-blocked",
					Identifier: "DD-BLOCKED",
					URL:        "https://github.com/digitaldrywood/detent/issues/39",
					Title:      "Blocked API",
					State:      "Todo",
				},
				WorkerHost:              "host-b",
				WorkspacePath:           "/workspaces/DD-BLOCKED",
				SessionID:               "thread-blocked",
				Error:                   "dependency is not merged",
				Source:                  telemetry.BlockedSourceDependency,
				RecoveryAction:          "defer",
				RecoveryReason:          "dependency_recovery",
				RecoveryTarget:          "Rework",
				RecoveryRemedy:          "Resolve the held root.",
				RecoveryReachability:    "deferred",
				RecoveryIntentResumable: true,
				RecoveryRoot: &telemetry.BlockedRecoveryRoot{
					IssueIdentifier: "digitaldrywood/detent#6",
					Reason:          "invalid_workpad_signal",
					Remedy:          "Move the issue to a fresh-work lane.",
				},
				BlockedAt:   &blockedAt,
				LastEventAt: &lastEventAt,
				LastEvent:   "turn_input_required",
				LastMessage: "waiting for operator input",
			},
		},
		Completed: []telemetry.Completed{
			{
				Issue: telemetry.Issue{
					ID:         "issue-completed",
					Identifier: "DD-DONE",
					URL:        "https://github.com/digitaldrywood/detent/issues/40",
					PullRequest: &telemetry.PullRequest{
						Number: 140,
					},
				},
				StartedAt:      startedAt,
				CompletedAt:    completedAt,
				Turns:          2,
				RuntimeSeconds: 45,
				FinalState:     "Done",
				Model:          "gpt-5",
				Tokens: telemetry.Tokens{
					Input:              100,
					CachedInput:        25,
					Output:             200,
					Total:              300,
					ModelContextWindow: &completedContextWindow,
				},
			},
		},
		RateLimits: &telemetry.RateLimits{
			Primary: &telemetry.RateLimitBucket{Remaining: 11},
		},
		Tokens: telemetry.Tokens{
			Input:              11,
			CachedInput:        1,
			Output:             22,
			Total:              33,
			ModelContextWindow: &totalContextWindow,
			RuntimeSeconds:     44.5,
		},
		Throughput: telemetry.TokenThroughput{
			TokensPerSecond: 7.5,
			WindowSeconds:   60,
			Tokens:          450,
		},
		LifetimeTotals: telemetry.LifetimeTotals{
			Available:      true,
			InputTokens:    1000,
			OutputTokens:   250,
			TotalTokens:    1250,
			RuntimeSeconds: 600,
			Sessions:       5,
			Runs:           2,
		},
		Budget: telemetry.Budget{
			Enabled:         true,
			CurrentSpendUSD: 1.25,
			PerDayMaxUSD:    &perDay,
			Days: []telemetry.BudgetDay{
				{Date: "2026-05-31", SpendUSD: 1.25},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	refresher := &refreshProbe{
		response: web.RefreshResponse{
			Queued:      true,
			Coalesced:   false,
			RequestedAt: generatedAt,
			Operations:  []string{"poll", "reconcile"},
		},
	}

	deps := testDeps(t)
	deps.Hub = events
	deps.Refresher = refresher

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	if state["generated_at"] != generatedAt.Format(time.RFC3339) {
		t.Fatalf("generated_at = %q, want %q", state["generated_at"], generatedAt.Format(time.RFC3339))
	}
	cleanupEvents := state["events"].([]any)
	if len(cleanupEvents) != 1 || cleanupEvents[0].(map[string]any)["event"] != "workspace_reap_succeeded" {
		t.Fatalf("events = %#v, want workspace cleanup event", cleanupEvents)
	}
	if !strings.Contains(cleanupEvents[0].(map[string]any)["message"].(string), "reason=cancelled") {
		t.Fatalf("cleanup event message = %#v, want cancellation reason", cleanupEvents[0])
	}
	if got := nestedString(t, state, "counts", "running"); got != "1" {
		t.Fatalf("counts.running = %s, want 1", got)
	}
	if got := nestedString(t, state, "counts", "retrying"); got != "1" {
		t.Fatalf("counts.retrying = %s, want 1", got)
	}
	if got := nestedString(t, state, "counts", "ready"); got != "1" {
		t.Fatalf("counts.ready = %s, want 1", got)
	}
	if got := nestedString(t, state, "counts", "waiting"); got != "1" {
		t.Fatalf("counts.waiting = %s, want 1", got)
	}
	if got := nestedString(t, state, "counts", "blocked"); got != "0" {
		t.Fatalf("counts.blocked = %s, want 0", got)
	}
	if got := boardStateCount(t, state, "Todo"); got != "2" {
		t.Fatalf("board Todo count = %s, want 2", got)
	}
	if got := boardStateCount(t, state, "In Progress"); got != "1" {
		t.Fatalf("board In Progress count = %s, want 1", got)
	}
	if got, ok := boardStateCountOK(t, state, "Blocked"); ok {
		t.Fatalf("board Blocked count = %s, want missing", got)
	}
	if got, ok := boardStateCountOK(t, state, "Done"); ok {
		t.Fatalf("board Done count = %s, want missing because completed sessions are history", got)
	}
	flow := state["board"].(map[string]any)["flow"].([]any)
	if len(flow) != 1 || flow[0].(map[string]any)["label"] != "03:24" || flow[0].(map[string]any)["count"] != float64(1) {
		t.Fatalf("board flow = %#v", flow)
	}

	running := state["running"].([]any)[0].(map[string]any)
	if running["issue_identifier"] != "digitaldrywood/detent#37" || running["issue_title"] != "REST API" {
		t.Fatalf("running row = %#v", running)
	}
	if running["pull_request_url"] != "https://github.com/digitaldrywood/detent/pull/137" || running["pull_request_number"] != float64(137) {
		t.Fatalf("running PR metadata = %#v/%#v; row = %#v", running["pull_request_url"], running["pull_request_number"], running)
	}
	description := running["issue_description"].(string)
	if len(description) != 250 || !strings.HasSuffix(description, "...") {
		t.Fatalf("issue_description length = %d, suffix ok = %v", len(description), strings.HasSuffix(description, "..."))
	}
	if running["budget_alert?"] != false {
		t.Fatalf("budget_alert? = %#v, want false", running["budget_alert?"])
	}
	for key, want := range map[string]any{
		"diff_added":   float64(4),
		"diff_removed": float64(2),
		"diff_files":   float64(3),
		"diff_status":  "ok",
	} {
		if running[key] != want {
			t.Fatalf("running[%q] = %#v, want %#v; row = %#v", key, running[key], want, running)
		}
	}
	if running["turn_count"] != float64(3) {
		t.Fatalf("running.turn_count = %#v, want 3", running["turn_count"])
	}
	runningTokens := running["tokens"].(map[string]any)
	if runningTokens["input_tokens"] != float64(10) || runningTokens["output_tokens"] != float64(20) || runningTokens["total_tokens"] != float64(30) {
		t.Fatalf("running.tokens = %#v, want live token counts", runningTokens)
	}
	if runningTokens["model_context_window"] != float64(40) || runningTokens["cache_read_fraction"] != 0.5 {
		t.Fatalf("running.tokens context/cache = %#v", runningTokens)
	}
	runningPressure := runningTokens["context_pressure"].(map[string]any)
	if runningPressure["percent_used"] != 75.0 || runningPressure["threshold_state"] != string(telemetry.ContextPressureWatch) {
		t.Fatalf("running.tokens.context_pressure = %#v", runningPressure)
	}
	runningEvents := running["recent_events"].([]any)
	if len(runningEvents) != 2 || runningEvents[1].(map[string]any)["message"] != "rendered" {
		t.Fatalf("running.recent_events = %#v", runningEvents)
	}

	retrying := state["retrying"].([]any)[0].(map[string]any)
	if retrying["issue_identifier"] != "DD-RETRY" || retrying["attempt"] != float64(2) {
		t.Fatalf("retrying row = %#v", retrying)
	}
	blocked := state["blocked"].([]any)[0].(map[string]any)
	for key, want := range map[string]any{
		"recovery_action":           "defer",
		"recovery_reason":           "dependency_recovery",
		"recovery_target":           "Rework",
		"recovery_remedy":           "Resolve the held root.",
		"recovery_reachability":     "deferred",
		"recovery_intent_resumable": true,
		"needs_human_attention":     false,
	} {
		if blocked[key] != want {
			t.Fatalf("blocked[%q] = %#v, want %#v; row = %#v", key, blocked[key], want, blocked)
		}
	}
	if root := blocked["recovery_root"].(map[string]any); root["issue_identifier"] != "digitaldrywood/detent#6" || root["reason"] != "invalid_workpad_signal" {
		t.Fatalf("blocked.recovery_root = %#v", root)
	}

	if got := nestedString(t, state, "codex_totals", "seconds_running"); got != "44.5" {
		t.Fatalf("codex_totals.seconds_running = %s, want 44.5", got)
	}
	codexTotals := state["codex_totals"].(map[string]any)
	if _, ok := codexTotals["context_pressure"]; ok {
		t.Fatalf("codex_totals.context_pressure present for aggregate totals: %#v", codexTotals["context_pressure"])
	}
	if got := nestedString(t, state, "throughput", "tokens_per_second"); got != "7.5" {
		t.Fatalf("throughput.tokens_per_second = %s, want 7.5", got)
	}
	if got := nestedString(t, state, "throughput", "tokens"); got != "450" {
		t.Fatalf("throughput.tokens = %s, want 450", got)
	}
	if got := nestedString(t, state, "lifetime_totals", "total_tokens"); got != "1250" {
		t.Fatalf("lifetime_totals.total_tokens = %s, want 1250", got)
	}
	if got := nestedString(t, state, "lifetime_totals", "sessions"); got != "5" {
		t.Fatalf("lifetime_totals.sessions = %s, want 5", got)
	}
	if len(state["recent_sessions"].([]any)) != 1 {
		t.Fatalf("recent_sessions = %#v, want one entry", state["recent_sessions"])
	}
	recentSession := state["recent_sessions"].([]any)[0].(map[string]any)
	if recentSession["pull_request_url"] != "https://github.com/digitaldrywood/detent/pull/140" || recentSession["pull_request_number"] != float64(140) {
		t.Fatalf("recent session PR metadata = %#v/%#v; row = %#v", recentSession["pull_request_url"], recentSession["pull_request_number"], recentSession)
	}
	recentPressure := recentSession["context_pressure"].(map[string]any)
	if recentSession["model_context_window"] != float64(600) || recentSession["cache_read_fraction"] != 0.25 || recentPressure["percent_used"] != 50.0 {
		t.Fatalf("recent session context/cache = %#v", recentSession)
	}
	if got := nestedString(t, state, "budget", "today_spend_usd"); got != "1.25" {
		t.Fatalf("budget.today_spend_usd = %s, want 1.25", got)
	}
	days := state["budget"].(map[string]any)["days"].([]any)
	if len(days) != 1 || days[0].(map[string]any)["date"] != "2026-05-31" || days[0].(map[string]any)["spend_usd"] != float64(1.25) {
		t.Fatalf("budget.days = %#v", days)
	}

	issue := requestJSON(t, server, http.MethodGet, "/api/v1/digitaldrywood/detent%2337", http.StatusOK)
	if issue["status"] != "running" || issue["issue_id"] != "issue-running" {
		t.Fatalf("issue payload = %#v", issue)
	}
	if issue["retry"] != nil || issue["blocked"] != nil || issue["last_error"] != nil {
		t.Fatalf("running issue nullable fields = %#v", issue)
	}
	runningIssue := issue["running"].(map[string]any)
	for key, want := range map[string]any{
		"diff_added":   float64(4),
		"diff_removed": float64(2),
		"diff_files":   float64(3),
		"diff_status":  "ok",
	} {
		if runningIssue[key] != want {
			t.Fatalf("issue.running[%q] = %#v, want %#v; running = %#v", key, runningIssue[key], want, runningIssue)
		}
	}
	if runningIssue["turn_count"] != float64(3) {
		t.Fatalf("issue.running.turn_count = %#v, want 3", runningIssue["turn_count"])
	}
	issueTokens := runningIssue["tokens"].(map[string]any)
	if issueTokens["input_tokens"] != float64(10) || issueTokens["output_tokens"] != float64(20) || issueTokens["total_tokens"] != float64(30) {
		t.Fatalf("issue.running.tokens = %#v, want live token counts", issueTokens)
	}
	issuePressure := issueTokens["context_pressure"].(map[string]any)
	if issueTokens["model_context_window"] != float64(40) || issuePressure["threshold_state"] != string(telemetry.ContextPressureWatch) {
		t.Fatalf("issue.running.tokens context = %#v", issueTokens)
	}
	issueEvents := issue["recent_events"].([]any)
	if len(issueEvents) != 2 || issueEvents[1].(map[string]any)["event"] != "notification" {
		t.Fatalf("issue.recent_events = %#v", issueEvents)
	}

	retryIssue := requestJSON(t, server, http.MethodGet, "/api/v1/DD-RETRY", http.StatusOK)
	if retryIssue["status"] != "retrying" || retryIssue["last_error"] != "no available orchestrator slots" {
		t.Fatalf("retry issue payload = %#v", retryIssue)
	}

	blockedIssue := requestJSON(t, server, http.MethodGet, "/api/v1/DD-BLOCKED", http.StatusOK)
	if blockedIssue["status"] != "blocked" || blockedIssue["last_error"] != "dependency is not merged" {
		t.Fatalf("blocked issue payload = %#v", blockedIssue)
	}
	blockedOverlay := blockedIssue["blocked"].(map[string]any)
	if blockedOverlay["recovery_action"] != "defer" || blockedOverlay["recovery_reachability"] != "deferred" || blockedOverlay["recovery_intent_resumable"] != true {
		t.Fatalf("blocked issue recovery = %#v", blockedOverlay)
	}
	if root := blockedOverlay["recovery_root"].(map[string]any); root["issue_identifier"] != "digitaldrywood/detent#6" {
		t.Fatalf("blocked issue recovery root = %#v", root)
	}

	missing := requestJSON(t, server, http.MethodGet, "/api/v1/DD-MISSING", http.StatusNotFound)
	if nestedString(t, missing, "error", "code") != "issue_not_found" {
		t.Fatalf("missing issue response = %#v", missing)
	}

	refresh := requestJSON(t, server, http.MethodPost, "/api/v1/refresh", http.StatusAccepted)
	if refresher.calls != 1 || refresh["queued"] != true || refresh["coalesced"] != false {
		t.Fatalf("refresh calls = %d, payload = %#v", refresher.calls, refresh)
	}
	if operations := refresh["operations"].([]any); len(operations) != 2 || operations[0] != "poll" || operations[1] != "reconcile" {
		t.Fatalf("refresh operations = %#v", refresh["operations"])
	}
}

func TestServerAPIStateSeparatesReadyWaitingAndBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		waiting telemetry.Blocked
	}{
		{
			name: "dependency source",
			waiting: telemetry.Blocked{
				Issue:  telemetry.Issue{ID: "waiting", State: "Todo"},
				Source: telemetry.BlockedSourceDependency,
			},
		},
		{
			name: "dependency reference",
			waiting: telemetry.Blocked{
				Issue: telemetry.Issue{
					ID:        "waiting",
					State:     "Todo",
					BlockedBy: []telemetry.BlockedRef{{Identifier: "digitaldrywood/detent#512", State: "In Progress"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := testDeps(t)
			snapshot := telemetry.Snapshot{
				BoardIssues: []telemetry.Issue{
					{ID: "ready", State: "Todo"},
					tt.waiting.Issue,
					{ID: "blocked", State: "Blocked"},
				},
				Blocked: []telemetry.Blocked{
					tt.waiting,
					{Issue: telemetry.Issue{ID: "blocked", State: "Blocked"}},
				},
			}
			if err := deps.Hub.Publish(snapshot); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
			counts := state["counts"].(map[string]any)
			for key, want := range map[string]float64{"ready": 1, "waiting": 1, "blocked": 1} {
				if got := counts[key]; got != want {
					t.Fatalf("counts[%q] = %#v, want %v; counts = %#v", key, got, want, counts)
				}
			}
			if got := boardStateCount(t, state, "Todo"); got != "2" {
				t.Fatalf("board Todo count = %s, want 2", got)
			}
			if got := boardStateCount(t, state, "Blocked"); got != "1" {
				t.Fatalf("board Blocked count = %s, want 1", got)
			}
		})
	}
}

func TestAPIRefreshOverlaysManualRequestOnStaleDegradedState(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 6, 24, 14, 0, 0, 0, time.UTC)
	requestedAt := generatedAt.Add(5 * time.Minute)
	lastErrorAt := generatedAt.Add(-time.Minute)
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		Refresh: telemetry.Refresh{
			Status:      telemetry.RefreshStatusDegraded,
			LastError:   "fetch workspace cleanup candidates failed: status 504",
			LastErrorAt: &lastErrorAt,
		},
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
				Refresh: telemetry.Refresh{
					Status:      telemetry.RefreshStatusDegraded,
					LastError:   "fetch workspace cleanup candidates failed: status 504",
					LastErrorAt: &lastErrorAt,
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	refresher := &refreshProbe{
		response: web.RefreshResponse{
			RequestID:   "manual-681",
			Status:      telemetry.RefreshAttemptStatusInProgress,
			Queued:      true,
			RequestedAt: requestedAt,
			Operations:  []string{"poll", "reconcile"},
		},
	}
	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Refresher = refresher
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	refresh := requestJSON(t, server, http.MethodPost, "/api/v1/refresh", http.StatusAccepted)
	if refresh["request_id"] != "manual-681" || refresh["status"] != string(telemetry.RefreshAttemptStatusInProgress) {
		t.Fatalf("refresh response = %#v, want correlated in-progress manual refresh", refresh)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	if state["generated_at"] != generatedAt.Format(time.RFC3339) {
		t.Fatalf("generated_at = %#v, want stale snapshot timestamp %s", state["generated_at"], generatedAt.Format(time.RFC3339))
	}
	refreshState := state["refresh"].(map[string]any)
	if refreshState["status"] != string(telemetry.RefreshStatusDegraded) {
		t.Fatalf("refresh.status = %#v, want degraded", refreshState["status"])
	}
	manual := refreshState["manual"].(map[string]any)
	if manual["id"] != "manual-681" || manual["status"] != string(telemetry.RefreshAttemptStatusInProgress) {
		t.Fatalf("refresh.manual = %#v, want manual-681 in progress", manual)
	}
	if manual["requested_at"] != requestedAt.Format(time.RFC3339) {
		t.Fatalf("manual.requested_at = %#v, want %s", manual["requested_at"], requestedAt.Format(time.RFC3339))
	}
	if operations := manual["operations"].([]any); len(operations) != 2 || operations[0] != "poll" || operations[1] != "reconcile" {
		t.Fatalf("manual.operations = %#v, want poll/reconcile", manual["operations"])
	}

	projectState := requestJSON(t, server, http.MethodGet, "/api/v1/projects/detent/state", http.StatusOK)
	projectRefresh := projectState["refresh"].(map[string]any)
	projectManual := projectRefresh["manual"].(map[string]any)
	if projectManual["id"] != "manual-681" || projectManual["status"] != string(telemetry.RefreshAttemptStatusInProgress) {
		t.Fatalf("project refresh.manual = %#v, want manual-681 in progress", projectManual)
	}
}

func TestServerEventsOverlayManualRefreshOnStaleDegradedState(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 6, 24, 14, 0, 0, 0, time.UTC)
	requestedAt := generatedAt.Add(5 * time.Minute)
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		Refresh: telemetry.Refresh{
			Status:    telemetry.RefreshStatusDegraded,
			LastError: "fetch tracker state failed: status 504",
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	refresher := &refreshProbe{
		response: web.RefreshResponse{
			RequestID:   "manual-sse",
			Status:      telemetry.RefreshAttemptStatusInProgress,
			Queued:      true,
			RequestedAt: requestedAt,
			Operations:  []string{"poll", "reconcile"},
		},
	}
	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Refresher = refresher
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	refresh := requestJSON(t, server, http.MethodPost, "/api/v1/refresh", http.StatusAccepted)
	if refresh["request_id"] != "manual-sse" || refresh["status"] != string(telemetry.RefreshAttemptStatusInProgress) {
		t.Fatalf("refresh response = %#v, want correlated in-progress manual refresh", refresh)
	}

	body := openEventStream(t, server)

	event := readSSEEvent(t, body)
	if event.name != "snapshot" {
		t.Fatalf("event name = %q, want snapshot", event.name)
	}
	for _, want := range []string{
		`id="manual-refresh-status"`,
		"Retrying",
	} {
		if !strings.Contains(event.data, want) {
			t.Fatalf("snapshot event missing %q:\n%s", want, event.data)
		}
	}
}

func TestAPIRefreshRefusesDuringGitHubGraphQLBackoff(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	retryAt := now.Add(5 * time.Minute)
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: now.Add(-time.Minute),
		Refresh: telemetry.Refresh{
			Status: telemetry.RefreshStatusDegraded,
		},
		RateLimits: &telemetry.RateLimits{
			GitHubGraphQL: &telemetry.RateLimitBucket{
				Remaining: 0,
				Used:      5000,
				Limit:     5000,
				ResetAt:   &retryAt,
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	refresher := &refreshProbe{
		response: web.RefreshResponse{
			RequestID:   "manual-unexpected",
			Status:      telemetry.RefreshAttemptStatusInProgress,
			Queued:      true,
			RequestedAt: now,
			Operations:  []string{"poll", "reconcile"},
		},
	}
	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Refresher = refresher
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	refresh := requestJSON(t, server, http.MethodPost, "/api/v1/refresh", http.StatusTooManyRequests)
	if refresher.calls != 0 {
		t.Fatalf("refresh calls = %d, want 0 while hard backoff is active", refresher.calls)
	}
	if refresh["status"] != string(telemetry.RefreshAttemptStatusRefused) || refresh["refused"] != true {
		t.Fatalf("refresh response = %#v, want refused status", refresh)
	}
	if refresh["queued"] != false || refresh["coalesced"] != false {
		t.Fatalf("refresh response = %#v, want not queued or coalesced", refresh)
	}
	if lastError, ok := refresh["last_error"].(string); !ok || !strings.Contains(lastError, "GitHub GraphQL backoff is active") {
		t.Fatalf("last_error = %#v, want GitHub GraphQL backoff reason", refresh["last_error"])
	}
	if refresh["retry_at"] == "" {
		t.Fatalf("retry_at = %#v, want populated retry time", refresh["retry_at"])
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	refreshState := state["refresh"].(map[string]any)
	manual := refreshState["manual"].(map[string]any)
	if manual["status"] != string(telemetry.RefreshAttemptStatusRefused) {
		t.Fatalf("refresh.manual = %#v, want refused overlay", manual)
	}
	if manual["retry_at"] != retryAt.Format(time.RFC3339Nano) {
		t.Fatalf("refresh.manual.retry_at = %#v, want %s", manual["retry_at"], retryAt.Format(time.RFC3339Nano))
	}
}

func TestAPIRefreshHTMXRendersRefusalFragment(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	retryAt := now.Add(5 * time.Minute)
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: now.Add(-time.Minute),
		Refresh: telemetry.Refresh{
			Status: telemetry.RefreshStatusDegraded,
		},
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{
				Remaining:      100,
				Used:           4900,
				Limit:          5000,
				ResetAt:        &retryAt,
				ResetInSeconds: int64((5 * time.Minute) / time.Second),
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	refresher := &refreshProbe{}
	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Refresher = refresher
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/refresh", nil)
	req.Header.Set("HX-Request", "true")
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/refresh status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if refresher.calls != 0 {
		t.Fatalf("refresh calls = %d, want 0 while hard backoff is active", refresher.calls)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`id="manual-refresh-status"`,
		"Refresh refused",
		"GitHub REST backoff is active",
		"Retry at {{detent-time:time:" + retryAt.Format(time.RFC3339Nano) + "}}",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("HTMX fragment missing %q:\n%s", want, body)
		}
	}

	form := url.Values{`manual_refresh_status_id`: {`github-api-manual-refresh-status`}}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/refresh", strings.NewReader(form.Encode()))
	req.Header.Set("HX-Request", "true")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/refresh sidebar status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body = rec.Body.String()
	for _, want := range []string{
		`id="github-api-manual-refresh-status"`,
		"Refresh refused",
		"GitHub REST backoff is active",
		"Retry at {{detent-time:time:" + retryAt.Format(time.RFC3339Nano) + "}}",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("HTMX sidebar fragment missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `id="manual-refresh-status"`) {
		t.Fatalf("HTMX sidebar fragment rendered legacy status id:\n%s", body)
	}
}

func TestServerEnrichesBudgetBurnDownFromStoreAndRegistry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	generatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)
	events := []store.UsageEvent{
		{
			ProjectID:  "detent",
			CostUSD:    1.25,
			StartedAt:  time.Date(2026, 5, 31, 8, 0, 0, 0, time.UTC),
			FinishedAt: time.Date(2026, 5, 31, 8, 1, 0, 0, time.UTC),
			Outcome:    "completed",
		},
		{
			ProjectID:  "detent",
			CostUSD:    1,
			StartedAt:  time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC),
			FinishedAt: time.Date(2026, 6, 1, 6, 1, 0, 0, time.UTC),
			Outcome:    "completed",
		},
		{
			ProjectID:  "pyroapex",
			CostUSD:    9,
			StartedAt:  time.Date(2026, 6, 1, 7, 0, 0, 0, time.UTC),
			FinishedAt: time.Date(2026, 6, 1, 7, 1, 0, 0, time.UTC),
			Outcome:    "completed",
		},
		{
			ProjectID:  "detent",
			CostUSD:    2.5,
			StartedAt:  time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC),
			FinishedAt: time.Date(2026, 6, 1, 11, 1, 0, 0, time.UTC),
			Outcome:    "completed",
		},
	}
	for _, event := range events {
		if _, err := backend.RecordUsageEvent(ctx, event); err != nil {
			t.Fatalf("RecordUsageEvent() error = %v", err)
		}
	}

	registry := project.NewRegistry()
	if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10, workflowconfig.BillingModeSubscription)); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{GeneratedAt: generatedAt}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Store = backend
	deps.Registry = registry
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	budget := state["budget"].(map[string]any)
	if budget["enabled"] != true || budget["today_spend_usd"] != float64(3.5) || budget["projected_spend_usd"] != float64(7) {
		t.Fatalf("budget = %#v, want enabled 3.5 today and 7 projected", budget)
	}
	if budget["per_day_max_usd"] != float64(100) || budget["per_issue_max_usd"] != float64(10) {
		t.Fatalf("budget caps = %#v", budget)
	}
	points := budget["spend_points"].([]any)
	if len(points) != 2 || points[1].(map[string]any)["spend_usd"] != float64(3.5) {
		t.Fatalf("budget spend_points = %#v", points)
	}
	days := budget["days"].([]any)
	if len(days) != 2 || days[0].(map[string]any)["date"] != "2026-05-31" || days[1].(map[string]any)["spend_usd"] != float64(3.5) {
		t.Fatalf("budget days = %#v", days)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/reports", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("reports status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{
		`id="reports-budget"`,
		"Daily budget",
		"$3.50 / $100.00",
		"2026-05-31: 1.25 USD",
		"2026-06-01: 3.5 USD",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("reports page missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestServerBudgetOverrideUIUsesSharedStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openWebTestStore(t)
	registry := project.NewRegistry()
	if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10)); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: time.Now().UTC(),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		Projects:    []telemetry.ProjectSnapshot{{Project: telemetry.Project{ID: "detent", DisplayName: "Detent"}}},
		Counts:      telemetry.Counts{Queue: 1},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Store = backend
	deps.Registry = registry
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	form := url.Values{
		"per_day_max_usd": {"200"},
		"duration":        {"4h"},
		"reason":          {"release work"},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/detent/budget/override", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("set override status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{`id="project-budget"`, "$0.00 / $200.00", "Override active", "release work", "Clear override early"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("override response missing %q:\n%s", want, rec.Body.String())
		}
	}
	active, err := backend.(store.BudgetOverrideStore).ActiveBudgetOverride(ctx, "detent", time.Now().UTC())
	if err != nil {
		t.Fatalf("ActiveBudgetOverride() error = %v", err)
	}
	if active.PerDayMaxUSD == nil || *active.PerDayMaxUSD != 200 || active.Reason != "release work" {
		t.Fatalf("active override = %#v", active)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/projects/detent/budget/override", nil)
	req.Header.Set("HX-Request", "true")
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "Override active") {
		t.Fatalf("clear override status/body = %d/%s", rec.Code, rec.Body.String())
	}
}

func TestDashboardRendersProjectSmallMultiplesFromSnapshots(t *testing.T) {
	t.Parallel()

	firstAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	secondAt := firstAt.Add(time.Minute)
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: firstAt,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent", URL: "https://github.com/digitaldrywood/detent"},
				Counts:  telemetry.Counts{Running: 1, Queue: 1},
				Tokens:  telemetry.Tokens{Total: 100},
			},
			{
				Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:  telemetry.Counts{Queue: 2},
				Tokens:  telemetry.Tokens{Total: 40},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Store = storeProbe{
		budgetCostEvents: func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
			return []store.BudgetCostEvent{
				{ProjectID: "detent", At: secondAt.Add(-30 * time.Second), CostUSD: 2.5},
				{ProjectID: "pyroapex", At: secondAt.Add(-20 * time.Second), CostUSD: 1},
			}, nil
		},
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: secondAt,
		Projects: []telemetry.ProjectSnapshot{
			{
				Project: telemetry.Project{ID: "detent", DisplayName: "Detent", URL: "https://github.com/digitaldrywood/detent"},
				Counts:  telemetry.Counts{Running: 1, Queue: 3},
				Tokens:  telemetry.Tokens{Total: 220},
			},
			{
				Project: telemetry.Project{ID: "pyroapex", DisplayName: "Pyro Apex"},
				Counts:  telemetry.Counts{Queue: 2},
				Tokens:  telemetry.Tokens{Total: 70},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/fleet", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	html := rec.Body.String()
	for _, want := range []string{
		">Detent</span>",
		">Pyro Apex</span>",
		`href="/projects/detent/kanban"`,
		`href="/projects/pyroapex/kanban"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard sidebar missing project row %q:\n%s", want, html)
		}
	}
	if !strings.Contains(html, `id="fig-running"`) {
		t.Fatalf("fleet page missing running figure:\n%s", html)
	}
}

func TestAdmissionProposalsRefreshOnDiagnosticsWithoutBoardFaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)
	proposal := admissionmodel.Proposal{
		ID:              "proposal-open",
		ProjectID:       "detent",
		IssueID:         "issue-1623",
		IssueIdentifier: "digitaldrywood/detent#1623",
		IssueURL:        "https://github.com/digitaldrywood/detent/issues/1623",
		TargetState:     "Todo",
		Fingerprint:     "fingerprint-open",
		CriteriaSection: "Admission Criteria",
		CriteriaText:    "Admit operator-visible defects.",
		Findings: []admissionmodel.Finding{{
			Dimension:      "Alignment",
			CriterionQuote: "Fleet-visible defects.",
			Matched:        true,
			Rationale:      "The operator saw the failure.",
		}},
		Confidence: 0.88,
		Status:     admissionmodel.ProposalOpen,
		CreatedAt:  now.Add(-time.Hour),
		ExpiresAt:  now.Add(23 * time.Hour),
	}
	if created, err := backend.CreateAdmissionProposal(ctx, proposal); err != nil || !created {
		t.Fatalf("CreateAdmissionProposal() = %t, %v", created, err)
	}
	expired := proposal
	expired.ID = "proposal-expired"
	expired.IssueID = "issue-expired"
	expired.IssueIdentifier = "digitaldrywood/detent#1600"
	expired.Fingerprint = "fingerprint-expired"
	expired.CreatedAt = now.Add(-48 * time.Hour)
	expired.ExpiresAt = now
	if created, err := backend.CreateAdmissionProposal(ctx, expired); err != nil || !created {
		t.Fatalf("CreateAdmissionProposal(expired) = %t, %v", created, err)
	}

	deps := testDeps(t)
	deps.Store = backend
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: now}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	diagnostics := requestHTML(t, server.Handler(), http.MethodGet, "/diagnostics", http.StatusOK)
	for _, want := range []string{`data-diagnostics-condition-class="review_queue"`, "Admission decision", "digitaldrywood/detent#1623", "88% confidence", "age 1h 0m", "expires in 23h 0m"} {
		if !strings.Contains(diagnostics, want) {
			t.Fatalf("Diagnostics missing %q:\n%s", want, diagnostics)
		}
	}
	if strings.Contains(diagnostics, "digitaldrywood/detent#1600") {
		t.Fatalf("Diagnostics rendered wall-clock-expired proposal:\n%s", diagnostics)
	}
	board := requestHTML(t, server.Handler(), http.MethodGet, "/", http.StatusOK)
	if strings.Contains(board, `data-board-alert="admission-proposal"`) || strings.Contains(board, `id="board-alerts"`) {
		t.Fatalf("Board rendered review queue as a fault:\n%s", board)
	}

	if err := backend.TransitionAdmissionProposal(ctx, proposal.ID, admissionmodel.ProposalOpen, admissionmodel.ProposalAccepted, now.Add(time.Minute)); err != nil {
		t.Fatalf("TransitionAdmissionProposal() error = %v", err)
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("Publish(refresh) error = %v", err)
	}
	diagnostics = requestHTML(t, server.Handler(), http.MethodGet, "/diagnostics", http.StatusOK)
	if strings.Contains(diagnostics, "diagnostics-condition-admission") || strings.Contains(diagnostics, proposal.IssueIdentifier) {
		t.Fatalf("resolved proposal remained after one refresh:\n%s", diagnostics)
	}
}

func TestServerPreservesSnapshotBudgetWhenSpendQueryFails(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	capUSD := 44.0
	snapshots := hub.New[telemetry.Snapshot]()
	if err := snapshots.Publish(telemetry.Snapshot{
		GeneratedAt: generatedAt,
		Budget: telemetry.Budget{
			Enabled:           true,
			CurrentSpendUSD:   12.34,
			ProjectedSpendUSD: 56.78,
			PerDayMaxUSD:      &capUSD,
			Days: []telemetry.BudgetDay{
				{Date: "2026-06-01", SpendUSD: 12.34},
			},
			SpendPoints: []telemetry.BudgetSpendPoint{
				{At: generatedAt, SpendUSD: 12.34},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	registry := project.NewRegistry()
	if err := registry.Set(newBudgetTestProject(t, "detent", 100, 10)); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	deps := testDeps(t)
	deps.Hub = snapshots
	deps.Registry = registry
	deps.Store = storeProbe{
		budgetCostEvents: func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
			return nil, errors.New("store is busy")
		},
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	budget := state["budget"].(map[string]any)
	if budget["today_spend_usd"] != float64(12.34) || budget["projected_spend_usd"] != float64(56.78) {
		t.Fatalf("budget = %#v, want preserved snapshot spend", budget)
	}
	if budget["per_day_max_usd"] != float64(44) {
		t.Fatalf("budget per_day_max_usd = %#v, want preserved snapshot cap", budget["per_day_max_usd"])
	}
	days := budget["days"].([]any)
	if len(days) != 1 || days[0].(map[string]any)["spend_usd"] != float64(12.34) {
		t.Fatalf("budget days = %#v, want preserved snapshot days", days)
	}
	points := budget["spend_points"].([]any)
	if len(points) != 1 || points[0].(map[string]any)["spend_usd"] != float64(12.34) {
		t.Fatalf("budget spend_points = %#v, want preserved snapshot points", points)
	}
}

func TestServerSurfacesFleetSpendWhenBudgetCapsAreDisabled(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	registry := project.NewRegistry()
	mustSetWebProject(t, registry, "detent", false)
	var gotQuery store.BudgetCostQuery
	deps := testDeps(t)
	deps.Registry = registry
	deps.Store = storeProbe{
		budgetCostEvents: func(_ context.Context, query store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
			gotQuery = query
			return []store.BudgetCostEvent{
				{ProjectID: "detent", At: generatedAt.Add(-time.Hour), CostUSD: 3.22},
				{ProjectID: "detent", At: generatedAt.AddDate(0, 0, -1), CostUSD: 363.50},
			}, nil
		},
	}
	if err := deps.Hub.Publish(telemetry.Snapshot{GeneratedAt: generatedAt}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	budgetState := state["budget"].(map[string]any)
	if budgetState["enabled"] != false || budgetState["today_spend_usd"] != float64(3.22) {
		t.Fatalf("budget = %#v, want uncapped fleet spend", budgetState)
	}
	regression := budgetState["spend_regression"].(map[string]any)
	if regression["drop_percent"].(float64) < 98 || regression["previous_spend_usd"] != float64(363.50) || regression["projected_spend_usd"] != float64(6.44) {
		t.Fatalf("spend regression = %#v, want same-day fleet regression", regression)
	}
	if len(gotQuery.ProjectIDs) != 1 || gotQuery.ProjectIDs[0] != "detent" {
		t.Fatalf("BudgetCostQuery.ProjectIDs = %#v, want all configured projects", gotQuery.ProjectIDs)
	}
}

func TestServerDistinguishesNoBudgetSpendFromSpendQueryFailure(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	capUSD := 100.0

	tests := []struct {
		name             string
		budgetCostEvents func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error)
		wantReason       string
		wantHTML         string
		forbiddenHTML    []string
	}{
		{
			name: "successful empty query",
			budgetCostEvents: func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
				return nil, nil
			},
			wantHTML:      "No notional USD yet.",
			forbiddenHTML: []string{"Budget data unavailable."},
		},
		{
			name: "failed query",
			budgetCostEvents: func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
				return nil, errors.New("store is busy")
			},
			wantReason:    "budget spend query failed",
			wantHTML:      "Budget data unavailable.",
			forbiddenHTML: []string{"No notional USD yet.", "$0.00 / $100.00"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snapshots := hub.New[telemetry.Snapshot]()
			if err := snapshots.Publish(telemetry.Snapshot{
				GeneratedAt: generatedAt,
				Budget: telemetry.Budget{
					Enabled:      true,
					PerDayMaxUSD: &capUSD,
				},
			}); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			registry := project.NewRegistry()
			if err := registry.Set(newBudgetTestProject(t, "detent", capUSD, 10)); err != nil {
				t.Fatalf("Registry.Set() error = %v", err)
			}

			deps := testDeps(t)
			deps.Hub = snapshots
			deps.Registry = registry
			deps.Store = storeProbe{budgetCostEvents: tt.budgetCostEvents}
			server, err := web.NewServer(web.Config{}, deps)
			if err != nil {
				t.Fatalf("NewServer() error = %v", err)
			}

			requestDashboardEnrichment(t, server)
			state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
			budget := state["budget"].(map[string]any)
			if tt.wantReason == "" {
				if _, ok := budget["degraded_reason"]; ok {
					t.Fatalf("budget degraded_reason = %#v, want omitted", budget["degraded_reason"])
				}
			} else if budget["degraded_reason"] != tt.wantReason {
				t.Fatalf("budget degraded_reason = %#v, want %q", budget["degraded_reason"], tt.wantReason)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
			server.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("fleet status = %d, want 200; body = %s", rec.Code, rec.Body.String())
			}
			html := rec.Body.String()
			if !strings.Contains(html, tt.wantHTML) {
				t.Fatalf("fleet page missing %q:\n%s", tt.wantHTML, html)
			}
			for _, forbidden := range tt.forbiddenHTML {
				if strings.Contains(html, forbidden) {
					t.Fatalf("fleet page rendered %q:\n%s", forbidden, html)
				}
			}
		})
	}
}

func TestDashboardRendersDisabledBudgetAsSingleNote(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 30, 14, 0, 0, 0, time.UTC),
		RateLimits: &telemetry.RateLimits{
			Primary: &telemetry.RateLimitBucket{Remaining: 95, Used: 5, Limit: 100},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fleet status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	html := rec.Body.String()
	if !strings.Contains(html, "Budget disabled — enable a daily cap in configuration.") {
		t.Fatalf("fleet page missing compact disabled budget note:\n%s", html)
	}
	for _, forbidden := range []string{
		"Budget history",
		"No budget history yet.",
		"Projected",
		"Issue cap",
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("fleet page rendered disabled budget detail %q:\n%s", forbidden, html)
		}
	}
}

func TestFleetRendersGitHubQuotaMetric(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 30, 15, 0, 0, 0, time.UTC)
	deps := testDeps(t)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 6, 30, 14, 30, 0, 0, time.UTC),
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{
				Remaining: 4200,
				Used:      800,
				Limit:     5000,
				ResetAt:   &resetAt,
			},
			RESTUsage: &telemetry.RESTUsage{
				TotalRequests: 8,
				Contributors: []telemetry.RESTUsageContributor{
					{EndpointFamily: "pull requests", Count: 5, Remaining: 4200, Limit: 5000, LastStatus: 200},
					{EndpointFamily: "check runs", Count: 3, Remaining: 4197, Limit: 5000, LastStatus: 200},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	html := rec.Body.String()
	for _, want := range []string{
		`id="metric-quota"`,
		"GitHub quota",
		"800 / 5,000",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("fleet quota metric missing %q:\n%s", want, html)
		}
	}
}

func TestServerAPIPreservesUnknownDiffStatus(t *testing.T) {
	t.Parallel()

	events := hub.New[telemetry.Snapshot]()
	if err := events.Publish(telemetry.Snapshot{
		Running: []telemetry.Running{
			{
				Issue: telemetry.Issue{
					ID:         "issue-running",
					Identifier: "DD-RUNNING",
					State:      "In Progress",
				},
			},
		},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	deps := testDeps(t)
	deps.Hub = events

	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	running := state["running"].([]any)[0].(map[string]any)
	if got, ok := running["diff_status"].(string); !ok || got != "" {
		t.Fatalf("state running diff_status = %#v, want empty string", running["diff_status"])
	}

	issue := requestJSON(t, server, http.MethodGet, "/api/v1/DD-RUNNING", http.StatusOK)
	runningIssue := issue["running"].(map[string]any)
	if got, ok := runningIssue["diff_status"].(string); !ok || got != "" {
		t.Fatalf("issue running diff_status = %#v, want empty string", runningIssue["diff_status"])
	}
}

func TestServerAPIErrorRoutes(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "state method not allowed",
			method:     http.MethodPost,
			path:       "/api/v1/state",
			wantStatus: http.StatusMethodNotAllowed,
			wantCode:   "method_not_allowed",
		},
		{
			name:       "refresh method not allowed",
			method:     http.MethodGet,
			path:       "/api/v1/refresh",
			wantStatus: http.StatusMethodNotAllowed,
			wantCode:   "method_not_allowed",
		},
		{
			name:       "unknown route",
			method:     http.MethodGet,
			path:       "/unknown",
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:       "state unavailable",
			method:     http.MethodGet,
			path:       "/api/v1/state",
			wantStatus: http.StatusOK,
			wantCode:   "snapshot_unavailable",
		},
		{
			name:       "refresh unavailable",
			method:     http.MethodPost,
			path:       "/api/v1/refresh",
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "orchestrator_unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload := requestJSON(t, server, tt.method, tt.path, tt.wantStatus)
			if got := nestedString(t, payload, "error", "code"); got != tt.wantCode {
				t.Fatalf("error.code = %q, want %q; payload = %#v", got, tt.wantCode, payload)
			}
		})
	}
}

func TestServerUsageAPIReportsAggregates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usageStore, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := usageStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	seedUsageAPIEvents(t, ctx, usageStore)

	deps := testDeps(t)
	deps.Store = usageStore
	server, err := web.NewServer(web.Config{
		Pricing: budget.PricingTable{
			"gpt-report": {
				USDPerInputToken:  0.01,
				USDPerOutputToken: 0.02,
			},
			"gpt-report-mini": {
				USDPerInputToken:  0.001,
				USDPerOutputToken: 0.002,
			},
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	day := requestJSON(t, server, http.MethodGet, "/api/v1/usage?by=day&from=2026-05-31&to=2026-05-31", http.StatusOK)
	if day["by"] != "day" {
		t.Fatalf("by = %#v, want day", day["by"])
	}
	if got := nestedString(t, day, "totals", "total_tokens"); got != "225" {
		t.Fatalf("totals.total_tokens = %s, want 225", got)
	}
	if got := nestedString(t, day, "totals", "spend_usd"); got != "2.1" {
		t.Fatalf("totals.spend_usd = %s, want 2.1", got)
	}
	series := day["series"].([]any)
	if len(series) != 1 {
		t.Fatalf("series len = %d, want 1: %#v", len(series), series)
	}
	point := series[0].(map[string]any)
	if point["bucket"] != "2026-05-31" || point["date"] != "2026-05-31" || point["events"] != float64(2) {
		t.Fatalf("day point = %#v", point)
	}

	project := requestJSON(t, server, http.MethodGet, "/api/v1/usage?by=project&from=2026-05-31&to=2026-06-01", http.StatusOK)
	breakdowns := project["breakdowns"].([]any)
	if len(breakdowns) != 2 {
		t.Fatalf("breakdowns len = %d, want 2: %#v", len(breakdowns), breakdowns)
	}
	detent := usageBucket(t, breakdowns, "detent")
	if detent["total_tokens"] != float64(225) || detent["spend_usd"] != 2.1 {
		t.Fatalf("detent breakdown = %#v", detent)
	}

	tests := []struct {
		name       string
		path       string
		wantBucket string
	}{
		{name: "issue", path: "/api/v1/usage?by=issue", wantBucket: "digitaldrywood/detent#119"},
		{name: "pr", path: "/api/v1/usage?by=pr", wantBucket: "detent#141"},
		{name: "model", path: "/api/v1/usage?by=model", wantBucket: "gpt-report"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload := requestJSON(t, server, http.MethodGet, tt.path, http.StatusOK)
			rows := payload["breakdowns"].([]any)
			if usageBucket(t, rows, tt.wantBucket) == nil {
				t.Fatalf("missing bucket %q in %#v", tt.wantBucket, rows)
			}
		})
	}
}

func TestServerWorkflowTimelineAPI(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openWebTestStore(t)
	startedAt := time.Date(2026, 6, 26, 14, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(30 * time.Minute)
	if _, err := backend.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:       "detent",
		IssueID:         "issue-722",
		Identifier:      "digitaldrywood/detent#722",
		IssueURL:        "https://github.com/digitaldrywood/detent/issues/722",
		PhaseType:       store.WorkflowPhaseTypeLane,
		PhaseName:       "Todo",
		Status:          "exited",
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		DurationSeconds: int64((30 * time.Minute) / time.Second),
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}

	deps := testDeps(t)
	deps.Store = backend
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	missing := requestJSON(t, server, http.MethodGet, "/api/v1/workflow/timeline", http.StatusBadRequest)
	if got := nestedString(t, missing, "error", "code"); got != "missing_project_id" {
		t.Fatalf("error.code = %q, want missing_project_id", got)
	}
	missing = requestJSON(t, server, http.MethodGet, "/api/v1/workflow/timeline?project_id=detent", http.StatusBadRequest)
	if got := nestedString(t, missing, "error", "code"); got != "missing_issue_identity" {
		t.Fatalf("error.code = %q, want missing_issue_identity", got)
	}

	payload := requestJSON(t, server, http.MethodGet, "/api/v1/workflow/timeline?project_id=detent&identifier=digitaldrywood/detent%23722", http.StatusOK)
	events := payload["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	event := events[0].(map[string]any)
	if event["phase_type"] != "lane" || event["phase_name"] != "Todo" || event["duration_seconds"] != float64(1800) {
		t.Fatalf("timeline event = %#v, want Todo lane duration", event)
	}
}

func TestWorkflowMetricsStateAPIIncludesLaneTrendComparisons(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)
	seedWorkflowTrendEvents(t, ctx, backend, now)

	deps := testDeps(t)
	deps.Store = backend
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	window := workflowMetricsWindow(t, state, "24h")

	merge := workflowMetricLane(t, window, "Merging")
	if merge["average_seconds"] != float64(300) || merge["p50_seconds"] != float64(300) || merge["p90_seconds"] != float64(300) || merge["p95_seconds"] != float64(300) {
		t.Fatalf("Merging lane = %#v, want current average and percentiles", merge)
	}
	mergeComparison := metricComparison(t, merge)
	if mergeComparison["direction"] != "faster" || mergeComparison["previous_average_seconds"] != float64(480) || mergeComparison["delta_seconds"] != float64(-180) {
		t.Fatalf("Merging comparison = %#v, want faster from previous average 480s", mergeComparison)
	}

	review := workflowMetricLane(t, window, "Human Review")
	if review["bottleneck"] != true {
		t.Fatalf("Human Review lane bottleneck = %#v, want true", review["bottleneck"])
	}
	reviewComparison := metricComparison(t, review)
	if reviewComparison["direction"] != "slower" || reviewComparison["previous_average_seconds"] != float64(420) || reviewComparison["delta_seconds"] != float64(180) {
		t.Fatalf("Human Review comparison = %#v, want slower from previous average 420s", reviewComparison)
	}

	todo := workflowMetricLane(t, window, "Todo")
	todoComparison := metricComparison(t, todo)
	if todoComparison["direction"] != "unchanged" || todoComparison["delta_seconds"] != float64(0) {
		t.Fatalf("Todo comparison = %#v, want unchanged", todoComparison)
	}

	inProgress := workflowMetricLane(t, window, "In Progress")
	inProgressComparison := metricComparison(t, inProgress)
	if inProgressComparison["direction"] != "insufficient_history" || inProgressComparison["previous_count"] != float64(0) {
		t.Fatalf("In Progress comparison = %#v, want insufficient history", inProgressComparison)
	}
}

func TestProjectDiagnosticsRendersRuntimeStoreEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	backend, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    dbPath,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	seedWorkflowTrendEvents(t, ctx, backend, now)

	deps := testDeps(t)
	deps.Store = backend
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	html := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/diagnostics", http.StatusOK)
	for _, want := range []string{
		"Runtime store",
		"SQLite-backed history",
		dbPath,
		"workflow_phase_events",
		"8 project rows",
		"Newest event",
		"24h",
		"7d",
		"30d",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics missing runtime evidence %q:\n%s", want, html)
		}
	}

	metrics := state["workflow_metrics"].(map[string]any)
	runtimeStore := metrics["runtime_store"].(map[string]any)
	if runtimeStore["backend"] != "sqlite" || runtimeStore["path"] != dbPath || runtimeStore["status"] != "healthy" {
		t.Fatalf("runtime_store = %#v, want healthy sqlite evidence for %q", runtimeStore, dbPath)
	}
	workflowPhaseEvents := runtimeStore["workflow_phase_events"].(map[string]any)
	if workflowPhaseEvents["row_count"] != float64(8) {
		t.Fatalf("workflow_phase_events = %#v, want row_count 8", workflowPhaseEvents)
	}
	tables := runtimeStore["tables"].([]any)
	if got := runtimeStoreTableCount(t, tables, "workflow_phase_events"); got != 8 {
		t.Fatalf("workflow_phase_events table count = %d, want 8", got)
	}
}

func TestProjectDiagnosticsRendersWorkflowMetricsEmptyHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)

	deps := testDeps(t)
	deps.Store = backend
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	html := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/diagnostics", http.StatusOK)
	for _, want := range []string{
		"SQLite history is empty.",
		"Lane averages appear after Detent records lane exits.",
		"workflow_phase_events",
		"0 project rows",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics missing empty workflow metric state %q:\n%s", want, html)
		}
	}
}

func TestProjectDiagnosticsRendersWorkflowMetricsTrends(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)
	seedWorkflowTrendEvents(t, ctx, backend, now)

	deps := testDeps(t)
	deps.Store = backend
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	html := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/diagnostics", http.StatusOK)
	for _, want := range []string{
		"Lane trends",
		"24h vs previous 24h",
		"Human Review",
		"Bottleneck",
		"Slower",
		"+3m 0s",
		"Merging",
		"Faster",
		"-3m 0s",
		"Todo",
		"Unchanged",
		"In Progress",
		"No prior",
		"AI Active",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics missing workflow metric trend %q:\n%s", want, html)
		}
	}
}

func TestProjectDiagnosticsRendersWorkflowFlowEfficiencyCharts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	backend := openWebTestStore(t)
	seedWorkflowFlowEvents(t, ctx, backend, now)

	deps := testDeps(t)
	deps.Store = backend
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	requestDashboardEnrichment(t, server)
	state := requestJSON(t, server, http.MethodGet, "/api/v1/state", http.StatusOK)
	window := workflowMetricsWindow(t, state, "24h")
	inProgress := workflowMetricLane(t, window, "In Progress")
	if inProgress["active_seconds"] != float64(300) || inProgress["wait_seconds"] != float64(300) || inProgress["active_percent"] != float64(50) {
		t.Fatalf("In Progress flow = %#v, want active/wait/percent 300/300/50", inProgress)
	}
	if !workflowLaneTrendIncludes(t, window, "Rework") {
		t.Fatalf("lane_trends missing Rework: %#v", window["lane_trends"])
	}
	if workflowLaneTrendIncludes(t, window, "Todo") {
		t.Fatalf("lane_trends included Todo: %#v", window["lane_trends"])
	}

	html := requestHTML(t, server.Handler(), http.MethodGet, "/projects/detent/diagnostics", http.StatusOK)
	for _, want := range []string{
		"Average lane trend",
		"Flow efficiency",
		"50% active",
		"In Progress",
		"Rework",
		"Active",
		"Wait",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics missing workflow flow chart %q:\n%s", want, html)
		}
	}
}

func TestServerUsageAPIRejectsInvalidParameters(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	deps.Store = openWebTestStore(t)
	server, err := web.NewServer(web.Config{}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	tests := []struct {
		name     string
		path     string
		wantCode string
	}{
		{name: "invalid group", path: "/api/v1/usage?by=week", wantCode: "invalid_usage_group"},
		{name: "invalid from", path: "/api/v1/usage?by=day&from=2026-31-05", wantCode: "invalid_date"},
		{name: "invalid range", path: "/api/v1/usage?by=day&from=2026-06-02&to=2026-06-01", wantCode: "invalid_date_range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload := requestJSON(t, server, http.MethodGet, tt.path, http.StatusBadRequest)
			if got := nestedString(t, payload, "error", "code"); got != tt.wantCode {
				t.Fatalf("error.code = %q, want %q; payload = %#v", got, tt.wantCode, payload)
			}
		})
	}
}

func TestReportsPageRendersUsageCharts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usageStore := openWebTestStore(t)
	seedUsageAPIEvents(t, ctx, usageStore)

	deps := testDeps(t)
	deps.Store = usageStore
	server, err := web.NewServer(web.Config{
		Pricing: budget.PricingTable{
			"gpt-report": {
				USDPerInputToken:  0.01,
				USDPerOutputToken: 0.02,
			},
			"gpt-report-mini": {
				USDPerInputToken:  0.001,
				USDPerOutputToken: 0.002,
			},
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/reports", nil)

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	for _, want := range []string{
		`href="/reports"`,
		`href="/"`,
		`href="/settings"`,
		`id="reports-kpis"`,
		"Total notional USD",
		"Cache hit",
		"Sessions",
		"Notional USD · by day",
		"Tokens · cumulative",
		"Top issues by tokens",
		"Top PRs by tokens",
		"$3.40",
		"#119",
		"#141",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("reports page missing %q:\n%s", want, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), "/static/vendor/chartjs/") {
		t.Fatalf("reports page loaded Chart.js; charts must be server-rendered SVG:\n%s", rec.Body.String())
	}
}

func TestReportsDailyDigestReconcilesSeededDay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openWebTestStore(t)
	today := time.Now().UTC().Truncate(24 * time.Hour)
	sessionID, err := backend.StartSession(ctx, store.SessionStart{IssueID: "issue-digest", Identifier: "digitaldrywood/detent#1203", StartedAt: today.Add(time.Hour), Model: "gpt-digest", OrphanRecoveryOutcome: store.OrphanRecoveryResumed})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, sessionID, store.SessionFinish{CompletedAt: today.Add(2 * time.Hour), InputTokens: 100, CachedInputTokens: 50, OutputTokens: 20, TotalTokens: 120, FinalState: "completed", Model: "gpt-digest"}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
	createdAt := today.Add(30 * time.Minute)
	shippedAt := today.Add(3 * time.Hour)
	releasedAt := today.Add(4 * time.Hour)

	deps := testDeps(t)
	deps.Store = backend
	mustSetWebProject(t, deps.Registry, "detent", false)
	if err := deps.Hub.Publish(telemetry.Snapshot{
		GeneratedAt: today.Add(5 * time.Hour),
		Project:     telemetry.Project{ID: "detent", DisplayName: "Detent"},
		BoardIssues: []telemetry.Issue{{ID: "issue-digest", ProjectID: "detent", State: "Done", CreatedAt: &createdAt, StageUpdatedAt: &shippedAt}},
		Release:     telemetry.Release{ProjectID: "detent", LastRelease: "v1.2.3", LastReleaseAt: &releasedAt},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	server, err := web.NewServer(web.Config{Pricing: budget.PricingTable{"gpt-digest": {USDPerInputToken: 0.01, USDPerCachedInputToken: 0.002, USDPerOutputToken: 0.01}}}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	html := requestHTML(t, server.Handler(), http.MethodGet, "/reports?tz=UTC", http.StatusOK)
	for _, want := range []string{
		`id="reports-daily-digest"`,
		`data-digest-date="` + today.Format(time.DateOnly) + `"`,
		`data-digest-metric="sessions"`,
		`data-digest-metric="cache"`,
		`data-digest-metric="cost"`,
		`data-digest-metric="tokens-per-merged"`,
		`data-digest-metric="efficiency-anomalies"`,
		"50%",
		"$0.80",
		"1 reattached · 0 fresh",
		`data-digest-project="detent"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("daily digest missing %q:\n%s", want, html)
		}
	}
}

func assertActiveSidebarLink(t *testing.T, body string, href string) {
	t.Helper()

	if !sidebarLinkActive(body, href) {
		t.Fatalf("body missing active sidebar link %q:\n%s", href, body)
	}
}

func assertInactiveSidebarLink(t *testing.T, body string, href string) {
	t.Helper()

	if sidebarLinkActive(body, href) {
		t.Fatalf("body rendered inactive sidebar link %q as active:\n%s", href, body)
	}
}

func assertAppSidebarLinkActive(t *testing.T, body string, href string) {
	t.Helper()

	if !appSidebarLinkActive(body, href) {
		t.Fatalf("body missing active app sidebar link %q:\n%s", href, body)
	}
}

func assertAppSidebarLinkInactive(t *testing.T, body string, href string) {
	t.Helper()

	if appSidebarLinkActive(body, href) {
		t.Fatalf("body rendered inactive app sidebar link %q as active:\n%s", href, body)
	}
}

func assertSingleCurrentSidebarItem(t *testing.T, body string) {
	t.Helper()

	currentLinks := regexp.MustCompile(`<a[^>]*aria-current="page"[^>]*>`).FindAllString(body, -1)
	if len(currentLinks) != 1 {
		t.Fatalf("body rendered %d current sidebar links, want 1: %v\n%s", len(currentLinks), currentLinks, body)
	}
	if !strings.Contains(currentLinks[0], `data-tui-sidebar-active="true"`) {
		t.Fatalf("current sidebar link missing active marker: %s\n%s", currentLinks[0], body)
	}
}

func assertSharedDashboardShellOnce(t *testing.T, body string, path string) {
	t.Helper()

	for _, marker := range []string{
		`data-tui-sidebar-layout`,
		`/static/js/templui/sidebar.min.js`,
		`/static/js/templui/dialog.min.js`,
	} {
		if got := strings.Count(body, marker); got != 1 {
			t.Fatalf("%s rendered %q %d times, want 1:\n%s", path, marker, got, body)
		}
	}
}

func sidebarLinkActive(body string, href string) bool {
	pattern := `<a[^>]*href="` + regexp.QuoteMeta(href) + `"[^>]*>`
	for _, link := range regexp.MustCompile(pattern).FindAllString(body, -1) {
		if strings.Contains(link, `data-tui-sidebar-active="true"`) && strings.Contains(link, `aria-current="page"`) {
			return true
		}
	}
	return false
}

func appSidebarLinkActive(body string, href string) bool {
	pattern := `<a[^>]*href="` + regexp.QuoteMeta(href) + `"[^>]*>`
	for _, link := range regexp.MustCompile(pattern).FindAllString(body, -1) {
		if strings.Contains(link, `aria-current="page"`) {
			return true
		}
	}
	return false
}

func testDeps(t *testing.T) web.Dependencies {
	t.Helper()

	return web.Dependencies{
		Hub:       hub.New[telemetry.Snapshot](),
		Store:     storeProbe{},
		Registry:  project.NewRegistry(),
		Connector: connectorProbe{name: "memory"},
	}
}

func newLibraryTestServer(t *testing.T) *web.Server {
	t.Helper()

	ctx := context.Background()
	backend := openWebTestStore(t)
	registry := project.NewRegistry()

	localPath := filepath.Join(t.TempDir(), "work-items.db")
	createdAt := time.Date(2026, 7, 2, 9, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 7, 2, 10, 30, 0, 0, time.UTC)
	issue := connector.NewIssue()
	issue.ID = "ad-1"
	issue.Identifier = "video/ad-1"
	issue.Title = "Produce summer sale ad"
	issue.State = "Review"
	issue.Fields = map[string]string{"render_status": "pending_review"}
	issue.CreatedAt = &createdAt
	issue.UpdatedAt = &updatedAt
	issue.StageUpdatedAt = &updatedAt
	issue.Deliverable = &connector.Deliverable{
		Kind:      "video_ad",
		Path:      "outputs/ad-1/manifest.json",
		ReviewURL: "http://127.0.0.1:8080/review/ad-1",
		Metadata:  map[string]string{"format": "mp4", "aspect": "9:16"},
	}
	unsafeIssue := connector.NewIssue()
	unsafeIssue.ID = "ad-unsafe"
	unsafeIssue.Identifier = "video/ad-unsafe"
	unsafeIssue.Title = "Unsafe review URL"
	unsafeIssue.State = "Review"
	unsafeIssue.Fields = map[string]string{"render_status": "pending_review"}
	unsafeIssue.CreatedAt = &createdAt
	unsafeIssue.UpdatedAt = &updatedAt
	unsafeIssue.StageUpdatedAt = &updatedAt
	unsafeIssue.Deliverable = &connector.Deliverable{
		Kind:      "video_ad",
		Path:      "outputs/ad-unsafe/manifest.json",
		ReviewURL: "javascript:alert(1)",
	}
	conn, err := local.New(local.Config{
		Path:      localPath,
		ProjectID: "video-local",
		Issues:    []connector.Issue{issue, unsafeIssue},
	})
	if err != nil {
		t.Fatalf("local.New() error = %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerLocalSQLite
	workflowCfg.Tracker.LocalSQLite.Path = localPath
	workflowCfg.Tracker.LocalSQLite.ProjectID = "video-local"
	workflowCfg.Deliverable.Kind = workflowconfig.DeliverableArtifact
	workflowCfg.Gate.Kind = gate.KindArtifact
	workflowCfg.Gate.Artifact.StatusField = "render_status"
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "video", Workdir: t.TempDir()},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "local_sqlite"},
	})
	if err != nil {
		t.Fatalf("project.New(video) error = %v", err)
	}
	if err := registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set(video) error = %v", err)
	}
	mustSetWebGitHubLabelProject(t, registry, "detent", "digitaldrywood/detent")

	prUpdatedAt := time.Date(2026, 7, 3, 15, 0, 0, 0, time.UTC)
	if err := backend.RecordValidatorVerdict(ctx, store.ValidatorVerdict{
		ProjectID:  "detent",
		IssueID:    "issue-933",
		HeadSHA:    "1234567890abcdef",
		Identifier: "digitaldrywood/detent#933",
		IssueURL:   "https://github.com/digitaldrywood/detent/issues/933",
		PRNumber:   int64Pointer(934),
		Submitted:  true,
		Verdict:    "pass",
		Score:      0.94,
		Summary:    "validator clean",
		RecordedAt: prUpdatedAt.Add(-time.Minute),
		UpdatedAt:  prUpdatedAt,
	}); err != nil {
		t.Fatalf("RecordValidatorVerdict() error = %v", err)
	}

	deps := testDeps(t)
	deps.Store = backend
	deps.Registry = registry
	deps.Connector = connectorProbe{name: "mixed"}
	server, err := web.NewServer(web.Config{DashboardURL: "http://127.0.0.1:4000"}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

func newWorkItemAPITestServer(t *testing.T, apiToken string) (*web.Server, *local.Connector, *refreshProbe) {
	t.Helper()

	conn, err := local.New(local.Config{
		Path:           filepath.Join(t.TempDir(), "work-items.db"),
		ProjectID:      "video",
		ActiveStates:   []string{"Todo", "In Progress"},
		ObservedStates: []string{"Backlog", "Blocked"},
		TerminalStates: []string{"Done"},
	})
	if err != nil {
		t.Fatalf("local.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerLocalSQLite
	workflowCfg.Tracker.LocalSQLite.Path = filepath.Join(t.TempDir(), "unused.db")
	workflowCfg.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflowCfg.Tracker.ObservedStates = []string{"Backlog", "Blocked"}
	workflowCfg.Tracker.TerminalStates = []string{"Done"}
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "video"},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: conn,
	})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}

	refresher := &refreshProbe{}
	deps := testDeps(t)
	deps.Connector = conn
	deps.Refresher = refresher
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		DashboardURL: "http://127.0.0.1:4000",
		GlobalConfig: globalconfig.Config{
			APIToken: apiToken,
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server, conn, refresher
}

func newAPIKeyWorkItemTestServer(t *testing.T) (*web.Server, store.Store, *local.Connector, *refreshProbe) {
	t.Helper()

	backend := openWebTestStore(t)
	conn, err := local.New(local.Config{
		Path:           filepath.Join(t.TempDir(), "work-items.db"),
		ProjectID:      "digitaldrywood-video",
		ActiveStates:   []string{"Todo", "In Progress"},
		ObservedStates: []string{"Backlog", "Blocked"},
		TerminalStates: []string{"Done"},
	})
	if err != nil {
		t.Fatalf("local.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerLocalSQLite
	workflowCfg.Tracker.LocalSQLite.Path = filepath.Join(t.TempDir(), "unused.db")
	workflowCfg.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflowCfg.Tracker.ObservedStates = []string{"Backlog", "Blocked"}
	workflowCfg.Tracker.TerminalStates = []string{"Done"}
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "digitaldrywood-video"},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: conn,
	})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}

	refresher := &refreshProbe{}
	deps := testDeps(t)
	deps.Store = backend
	deps.Connector = conn
	deps.Refresher = refresher
	if err := deps.Registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	server, err := web.NewServer(web.Config{
		DashboardURL: "http://127.0.0.1:4000",
		GlobalConfig: globalconfig.Config{
			APIToken: "detent_admin_token",
		},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server, backend, conn, refresher
}

func createAPIKeyThroughHTTP(t *testing.T, server *web.Server, body string) (string, string) {
	t.Helper()

	rec := performJSON(t, server.Handler(), http.MethodPost, "/api/v1/keys", body, map[string]string{
		"Authorization": "Bearer detent_admin_token",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create api key status = %d, want %d; body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	return apiKeyTokenFromResponse(t, rec.Body.Bytes()), apiKeyIDFromResponse(t, rec.Body.Bytes())
}

func apiKeyTokenFromResponse(t *testing.T, body []byte) string {
	t.Helper()

	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("Unmarshal(api key response) error = %v; body = %s", err, string(body))
	}
	return payload.Token
}

func apiKeyIDFromResponse(t *testing.T, body []byte) string {
	t.Helper()

	var payload struct {
		Key struct {
			ID string `json:"id"`
		} `json:"key"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("Unmarshal(api key response) error = %v; body = %s", err, string(body))
	}
	if payload.Key.ID == "" {
		t.Fatalf("api key response missing key.id: %s", string(body))
	}
	return payload.Key.ID
}

func createExpiredAPIKey(t *testing.T, backend store.Store) string {
	t.Helper()

	token, err := apikey.GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	createdAt := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(time.Hour)
	_, err = backend.CreateAPIKey(context.Background(), store.APIKeyCreate{
		ID:          "expired-key",
		Name:        "Expired",
		PrefixLast4: apikey.PrefixLast4(token),
		KeyHash:     apikey.HashToken(token),
		Scopes:      []string{"read"},
		CreatedAt:   createdAt,
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		t.Fatalf("CreateAPIKey(expired) error = %v", err)
	}
	return token
}

func waitForAPIUsageLog(t *testing.T, backend store.Store, keyID string) {
	t.Helper()

	_ = waitForAPIUsageLogs(t, backend, keyID)
}

func waitForAPIUsageLogs(t *testing.T, backend store.Store, keyID string) []sqlc.ApiUsageLog {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		queries := backend.Queries()
		if queries == nil {
			t.Fatalf("store Queries() = nil")
		}
		logs, err := queries.ListAPIUsageLogsByKey(context.Background(), keyID)
		if err != nil {
			t.Fatalf("ListAPIUsageLogsByKey() error = %v", err)
		}
		if len(logs) > 0 {
			return logs
		}
		if time.Now().After(deadline) {
			t.Fatalf("usage log count for %s stayed 0", keyID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustSetWebProject(t *testing.T, registry *project.Registry, id string, paused bool) {
	t.Helper()

	mustSetWebProjectWithWorkflowStates(t, registry, id, paused, nil, nil, nil)
}

type healthBudgetRunner struct {
	budget workflowconfig.Budget
}

type blockingHealthBudgetRunner struct {
	blocked chan<- struct{}
	release <-chan struct{}
}

type healthWorkflowWatcher struct {
	updates <-chan configwatcher.Update
}

func (w healthWorkflowWatcher) Watch(context.Context) (<-chan configwatcher.Update, error) {
	return w.updates, nil
}

func (blockingHealthBudgetRunner) Run(context.Context, orchestrator.RunRequest) (orchestrator.RunResult, error) {
	return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
}

func (r blockingHealthBudgetRunner) EnforcedBudget() (workflowconfig.Budget, bool) {
	select {
	case r.blocked <- struct{}{}:
	default:
	}
	<-r.release
	return workflowconfig.Budget{}, false
}

func (r healthBudgetRunner) Run(context.Context, orchestrator.RunRequest) (orchestrator.RunResult, error) {
	return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
}

func (r healthBudgetRunner) EnforcedBudget() (workflowconfig.Budget, bool) {
	return r.budget, true
}

func mustSetWebGitHubLabelProject(t *testing.T, registry *project.Registry, id string, repository string) {
	t.Helper()

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerGitHub
	workflowCfg.Tracker.APIKey = "$GITHUB_TOKEN"
	workflowCfg.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceLabel
	workflowCfg.Tracker.Repository = repository
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: id},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "github"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
}

func mustSetWebProjectWithWorkflowStates(
	t *testing.T,
	registry *project.Registry,
	id string,
	paused bool,
	active []string,
	observed []string,
	terminal []string,
) {
	t.Helper()

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	if active != nil {
		workflowCfg.Tracker.ActiveStates = append([]string(nil), active...)
	}
	if observed != nil {
		workflowCfg.Tracker.ObservedStates = append([]string(nil), observed...)
	}
	if terminal != nil {
		workflowCfg.Tracker.TerminalStates = append([]string(nil), terminal...)
	}
	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: id, Paused: paused},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "memory"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
}

func newSettingsTestProject(t *testing.T, cfg globalconfig.Project, worktreeRoot string, projectURL string) *project.Project {
	t.Helper()

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerGitHub
	workflowCfg.Tracker.Endpoint = "https://api.github.com/graphql"
	workflowCfg.Tracker.APIKey = "$GITHUB_TOKEN"
	workflowCfg.Tracker.ProjectSlug = "PVT_settings"
	workflowCfg.Tracker.DependencyAutoUnblock.Enabled = true
	workflowCfg.Tracker.DependencyAutoUnblock.SourceStates = []string{"Blocked", "Waiting"}
	workflowCfg.Tracker.DependencyAutoUnblock.TargetState = "Todo"
	workflowCfg.Tracker.DependencyAutoUnblock.Readiness = workflowconfig.DependencyReadinessTerminalOrMerged
	workflowCfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework"}
	workflowCfg.Workspace.Root = worktreeRoot
	workflowCfg.Workspace.SourceRoot = cfg.Workdir

	trackedProject, err := project.New(project.Config{
		Project: cfg,
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "github", projectURL: projectURL},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return trackedProject
}

func newBudgetTestProject(t *testing.T, id string, perDayMaxUSD float64, perIssueMaxUSD float64, billingModes ...string) *project.Project {
	t.Helper()

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerGitHub
	workflowCfg.Tracker.Endpoint = "https://api.github.com/graphql"
	workflowCfg.Tracker.APIKey = "$GITHUB_TOKEN"
	workflowCfg.Tracker.ProjectSlug = "https://github.com/orgs/digitaldrywood/projects/4"
	workflowCfg.Budget.Enabled = true
	if len(billingModes) > 0 {
		workflowCfg.Budget.BillingMode = billingModes[0]
	}
	workflowCfg.Budget.PerDayMaxUSD = perDayMaxUSD
	workflowCfg.Budget.PerIssueMaxUSD = perIssueMaxUSD

	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: id},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: connectorProbe{name: "github"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return trackedProject
}

func openWebTestStore(t *testing.T) store.Store {
	t.Helper()
	return storetest.Open(t)
}

func seedWorkflowTrendEvents(t *testing.T, ctx context.Context, backend store.Store, now time.Time) {
	t.Helper()

	events := []struct {
		phaseName string
		finished  time.Time
		duration  time.Duration
		phaseType store.WorkflowPhaseType
		turns     int64
		tokens    int64
	}{
		{phaseName: "Merging", finished: now.Add(-time.Hour), duration: 5 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "Merging", finished: now.Add(-25 * time.Hour), duration: 8 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "Human Review", finished: now.Add(-2 * time.Hour), duration: 10 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "Human Review", finished: now.Add(-26 * time.Hour), duration: 7 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "Todo", finished: now.Add(-3 * time.Hour), duration: 2 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "Todo", finished: now.Add(-27 * time.Hour), duration: 2 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "In Progress", finished: now.Add(-4 * time.Hour), duration: 4 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{phaseName: "agent_active", finished: now.Add(-30 * time.Minute), duration: 3 * time.Minute, phaseType: store.WorkflowPhaseTypeAgentSession, turns: 2, tokens: 600},
	}
	for i, event := range events {
		attrs := store.WorkflowPhaseEvent{
			ProjectID:       "detent",
			IssueID:         "issue-" + strconv.Itoa(i+1),
			Identifier:      "digitaldrywood/detent#" + strconv.Itoa(900+i),
			PhaseType:       event.phaseType,
			PhaseName:       event.phaseName,
			Status:          "completed",
			StartedAt:       event.finished.Add(-event.duration),
			FinishedAt:      event.finished,
			DurationSeconds: int64(event.duration / time.Second),
			Turns:           event.turns,
			TotalTokens:     event.tokens,
			EndpointFamily:  "codex",
		}
		if _, err := backend.RecordWorkflowPhaseEvent(ctx, attrs); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}
}

func seedWorkflowFlowEvents(t *testing.T, ctx context.Context, backend store.Store, now time.Time) {
	t.Helper()

	events := []struct {
		issueID   string
		phaseName string
		finished  time.Time
		duration  time.Duration
		phaseType store.WorkflowPhaseType
	}{
		{issueID: "issue-progress", phaseName: "In Progress", finished: now.Add(-2 * time.Hour), duration: 10 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{issueID: "issue-progress", phaseName: "agent_active", finished: now.Add(-2*time.Hour - 6*time.Minute), duration: 2 * time.Minute, phaseType: store.WorkflowPhaseTypeAgentSession},
		{issueID: "issue-progress", phaseName: "ci", finished: now.Add(-2*time.Hour - time.Minute), duration: 3 * time.Minute, phaseType: store.WorkflowPhaseTypeCI},
		{issueID: "issue-progress", phaseName: "github_backoff", finished: now.Add(-2*time.Hour - 4*time.Minute), duration: 2 * time.Minute, phaseType: store.WorkflowPhaseTypeGitHubBackoff},
		{issueID: "issue-rework", phaseName: "Rework", finished: now.Add(-90 * time.Minute), duration: 5 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
		{issueID: "issue-todo", phaseName: "Todo", finished: now.Add(-80 * time.Minute), duration: 4 * time.Minute, phaseType: store.WorkflowPhaseTypeLane},
	}
	for i, event := range events {
		attrs := store.WorkflowPhaseEvent{
			ProjectID:       "detent",
			IssueID:         event.issueID,
			Identifier:      "digitaldrywood/detent#" + strconv.Itoa(980+i),
			PhaseType:       event.phaseType,
			PhaseName:       event.phaseName,
			Status:          "completed",
			StartedAt:       event.finished.Add(-event.duration),
			FinishedAt:      event.finished,
			DurationSeconds: int64(event.duration / time.Second),
			EndpointFamily:  "codex",
		}
		if _, err := backend.RecordWorkflowPhaseEvent(ctx, attrs); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}
}

func seedUsageAPIEvents(t *testing.T, ctx context.Context, backend store.Store) {
	t.Helper()

	events := []store.UsageEvent{
		{
			ProjectID:      "detent",
			IssueID:        "issue-119",
			Identifier:     "digitaldrywood/detent#119",
			PRNumber:       new(int64(141)),
			Model:          "gpt-report",
			InputTokens:    100,
			OutputTokens:   50,
			TotalTokens:    150,
			RuntimeSeconds: 30,
			StartedAt:      time.Date(2026, 5, 31, 9, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 5, 31, 9, 1, 0, 0, time.UTC),
			Outcome:        "completed",
		},
		{
			ProjectID:      "detent",
			IssueID:        "issue-120",
			Identifier:     "digitaldrywood/detent#120",
			PRNumber:       new(int64(142)),
			Model:          "gpt-report-mini",
			InputTokens:    50,
			OutputTokens:   25,
			TotalTokens:    75,
			RuntimeSeconds: 15,
			StartedAt:      time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 5, 31, 10, 1, 0, 0, time.UTC),
			Outcome:        "completed",
		},
		{
			ProjectID:      "pyroapex",
			IssueID:        "issue-119",
			Identifier:     "digitaldrywood/detent#119",
			PRNumber:       new(int64(141)),
			Model:          "gpt-report",
			InputTokens:    70,
			OutputTokens:   30,
			TotalTokens:    100,
			RuntimeSeconds: 25,
			StartedAt:      time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 1, 11, 1, 0, 0, time.UTC),
			Outcome:        "completed",
		},
	}

	for _, event := range events {
		if _, err := backend.RecordUsageEvent(ctx, event); err != nil {
			t.Fatalf("RecordUsageEvent() error = %v", err)
		}
	}
}

func usageBucket(t *testing.T, rows []any, bucket string) map[string]any {
	t.Helper()

	for _, row := range rows {
		object := row.(map[string]any)
		if object["bucket"] == bucket {
			return object
		}
	}
	t.Fatalf("missing bucket %q in %#v", bucket, rows)
	return nil
}

func requestDashboardEnrichment(t *testing.T, server *web.Server) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/reports", nil)
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /reports status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
}

func requestJSON(t *testing.T, server *web.Server, method string, path string, wantStatus int) map[string]any {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(%s %s) error = %v; body = %s", method, path, err, rec.Body.String())
	}
	return payload
}

func requestJSONWithHeaders(t *testing.T, server *web.Server, method string, path string, wantStatus int, headers map[string]string) map[string]any {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(%s %s) error = %v; body = %s", method, path, err, rec.Body.String())
	}
	return payload
}

func performJSON(t *testing.T, handler http.Handler, method string, path string, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	handler.ServeHTTP(rec, req)
	return rec
}

func performJSONWithRemote(t *testing.T, handler http.Handler, method string, path string, body string, remoteAddr string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = remoteAddr
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	handler.ServeHTTP(rec, req)
	return rec
}

func requestHTML(t *testing.T, handler http.Handler, method string, path string, wantStatus int) string {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Body.String()
}

func requestHTMLWithHeaders(t *testing.T, handler http.Handler, method string, path string, wantStatus int, headers map[string]string) string {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	handler.ServeHTTP(rec, req)

	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Body.String()
}

type dashboardHTMXRequest struct {
	method     string
	path       string
	host       string
	remote     string
	form       url.Values
	currentURL string
	htmxTarget string
	headers    map[string]string
	cookies    []*http.Cookie
	noHX       bool
}

func performDashboardHTMXRequest(t *testing.T, handler http.Handler, input dashboardHTMXRequest) *httptest.ResponseRecorder {
	t.Helper()

	host := strings.TrimSpace(input.host)
	if host == "" {
		host = "localhost:4000"
	}
	remote := strings.TrimSpace(input.remote)
	if remote == "" {
		remote = "127.0.0.1:49152"
	}
	method := strings.TrimSpace(input.method)
	if method == "" {
		method = http.MethodGet
	}

	var body io.Reader
	if input.form != nil {
		body = strings.NewReader(input.form.Encode())
	}
	req := httptest.NewRequest(method, "http://"+host+input.path, body)
	req.Host = host
	req.RemoteAddr = remote
	if input.form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if !input.noHX {
		req.Header.Set("HX-Request", "true")
		currentURL := strings.TrimSpace(input.currentURL)
		if currentURL == "" {
			currentURL = "http://" + host + "/"
		}
		req.Header.Set("HX-Current-URL", currentURL)
		if htmxTarget := strings.TrimSpace(input.htmxTarget); htmxTarget != "" {
			req.Header.Set("HX-Target", htmxTarget)
		}
	}
	for _, cookie := range input.cookies {
		req.AddCookie(cookie)
	}
	for key, value := range input.headers {
		req.Header.Set(key, value)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func assertManifestContainsScenarios(t *testing.T, manifest map[string]any, ids []string) {
	t.Helper()

	scenarios, ok := manifest["scenarios"].([]any)
	if !ok {
		t.Fatalf("manifest scenarios = %T, want list: %#v", manifest["scenarios"], manifest)
	}
	seen := map[string]struct{}{}
	for _, raw := range scenarios {
		entry, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("manifest scenario entry = %T, want object", raw)
		}
		id, ok := entry["id"].(string)
		if !ok || id == "" {
			t.Fatalf("manifest scenario id = %#v", entry["id"])
		}
		seen[id] = struct{}{}
		if entry["route"] == "" || entry["wait_selector"] == "" || entry["screenshot_name"] == "" {
			t.Fatalf("manifest scenario missing route/wait/screenshot fields: %#v", entry)
		}
		headers, ok := entry["headers"].(map[string]any)
		if !ok || headers[web.DemoScenarioHeader] != id {
			t.Fatalf("manifest scenario headers = %#v, want %s=%s", entry["headers"], web.DemoScenarioHeader, id)
		}
	}
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			t.Fatalf("manifest missing scenario %q", id)
		}
	}
}

func assertManifestOmitsScenarios(t *testing.T, manifest map[string]any, ids []string) {
	t.Helper()

	scenarios, ok := manifest["scenarios"].([]any)
	if !ok {
		t.Fatalf("manifest scenarios = %T, want list: %#v", manifest["scenarios"], manifest)
	}
	seen := map[string]struct{}{}
	for _, raw := range scenarios {
		entry, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("manifest scenario entry = %T, want object", raw)
		}
		id, ok := entry["id"].(string)
		if !ok || id == "" {
			t.Fatalf("manifest scenario id = %#v", entry["id"])
		}
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("manifest includes non-screenshot stream scenario %q", id)
		}
	}
}

func nestedString(t *testing.T, payload map[string]any, keys ...string) string {
	t.Helper()

	var current any = payload
	for _, key := range keys {
		object, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("value for %v is %T, want object", keys, current)
		}
		current = object[key]
	}
	switch value := current.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		t.Fatalf("value for %v is %T, want string or number", keys, current)
		return ""
	}
}

func workflowMetricsWindow(t *testing.T, payload map[string]any, label string) map[string]any {
	t.Helper()

	metrics, ok := payload["workflow_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("workflow_metrics = %T, want object", payload["workflow_metrics"])
	}
	windows, ok := metrics["windows"].([]any)
	if !ok {
		t.Fatalf("workflow_metrics.windows = %T, want list", metrics["windows"])
	}
	for _, raw := range windows {
		window, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("window = %T, want object", raw)
		}
		if window["label"] == label {
			return window
		}
	}
	t.Fatalf("workflow_metrics.windows missing %q: %#v", label, windows)
	return nil
}

func workflowMetricLane(t *testing.T, window map[string]any, phaseName string) map[string]any {
	t.Helper()

	lanes, ok := window["lanes"].([]any)
	if !ok {
		t.Fatalf("window.lanes = %T, want list", window["lanes"])
	}
	for _, raw := range lanes {
		lane, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("lane = %T, want object", raw)
		}
		if lane["phase_name"] == phaseName {
			return lane
		}
	}
	t.Fatalf("window.lanes missing %q: %#v", phaseName, lanes)
	return nil
}

func workflowLaneTrendIncludes(t *testing.T, window map[string]any, phaseName string) bool {
	t.Helper()

	trends, ok := window["lane_trends"].([]any)
	if !ok {
		t.Fatalf("window.lane_trends = %T, want list", window["lane_trends"])
	}
	for _, raw := range trends {
		trend, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("lane trend = %T, want object", raw)
		}
		if trend["phase_name"] == phaseName {
			return true
		}
	}
	return false
}

func metricComparison(t *testing.T, lane map[string]any) map[string]any {
	t.Helper()

	comparison, ok := lane["comparison"].(map[string]any)
	if !ok {
		t.Fatalf("lane.comparison = %T, want object: %#v", lane["comparison"], lane)
	}
	return comparison
}

func runtimeStoreTableCount(t *testing.T, tables []any, name string) int64 {
	t.Helper()

	for _, raw := range tables {
		table, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("runtime store table = %T, want object", raw)
		}
		if table["name"] != name {
			continue
		}
		count, ok := table["row_count"].(float64)
		if !ok {
			t.Fatalf("%s.row_count = %T, want number", name, table["row_count"])
		}
		return int64(count)
	}
	t.Fatalf("runtime store table %q missing from %#v", name, tables)
	return 0
}

func boardStateCount(t *testing.T, payload map[string]any, stateName string) string {
	t.Helper()

	if count, ok := boardStateCountOK(t, payload, stateName); ok {
		return count
	}
	board := payload["board"].(map[string]any)
	distribution := board["state_distribution"].([]any)
	t.Fatalf("board state %q missing from %#v", stateName, distribution)
	return ""
}

func boardStateCountOK(t *testing.T, payload map[string]any, stateName string) (string, bool) {
	t.Helper()

	board := payload["board"].(map[string]any)
	distribution := board["state_distribution"].([]any)
	for _, entry := range distribution {
		row := entry.(map[string]any)
		if row["state"] == stateName {
			return strconv.FormatFloat(row["count"].(float64), 'f', -1, 64), true
		}
	}
	return "", false
}

type storeProbe struct {
	store.Store

	cycleTimeReport   func(context.Context) (store.CycleTimeReport, error)
	budgetCostEvents  func(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error)
	runtimeEvidence   func(context.Context, store.RuntimeEvidenceQuery) (store.RuntimeEvidence, error)
	validatorVerdicts func(context.Context, store.ValidatorVerdictQuery) ([]store.ValidatorVerdict, error)
}

type healthNotificationFailureReader struct {
	failures []healthnotify.Failure
}

func (r healthNotificationFailureReader) Failures(context.Context) ([]healthnotify.Failure, error) {
	return append([]healthnotify.Failure(nil), r.failures...), nil
}

type renderBlockingStore struct {
	storeProbe

	release <-chan struct{}
	calls   atomic.Int64
}

type enrichmentQueryCountingStore struct {
	storeProbe

	workflowMetricsCalls atomic.Int64
	budgetCostCalls      atomic.Int64
}

func (s *enrichmentQueryCountingStore) WorkflowMetricsReport(context.Context, store.WorkflowMetricsQuery) (store.WorkflowMetricsReport, error) {
	s.workflowMetricsCalls.Add(1)
	return store.WorkflowMetricsReport{}, nil
}

func (s *enrichmentQueryCountingStore) BudgetCostEvents(context.Context, store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
	s.budgetCostCalls.Add(1)
	return nil, nil
}

func (s *renderBlockingStore) wait(ctx context.Context) error {
	s.calls.Add(1)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *renderBlockingStore) CycleTimeReport(ctx context.Context) (store.CycleTimeReport, error) {
	return store.CycleTimeReport{}, s.wait(ctx)
}

func (s *renderBlockingStore) BudgetCostEvents(ctx context.Context, _ store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
	return nil, s.wait(ctx)
}

func (s *renderBlockingStore) WorkflowMetricsReport(ctx context.Context, _ store.WorkflowMetricsQuery) (store.WorkflowMetricsReport, error) {
	return store.WorkflowMetricsReport{}, s.wait(ctx)
}

func (s *renderBlockingStore) RuntimeEvidence(ctx context.Context, _ store.RuntimeEvidenceQuery) (store.RuntimeEvidence, error) {
	return store.RuntimeEvidence{}, s.wait(ctx)
}

func (s *renderBlockingStore) ListEfficiencyReceipts(ctx context.Context, _ efficiency.Query) ([]efficiency.Receipt, error) {
	return nil, s.wait(ctx)
}

func (storeProbe) LifetimeTotals(context.Context) (store.LifetimeTotals, error) {
	return store.LifetimeTotals{}, nil
}

func (storeProbe) UsageReport(_ context.Context, query store.UsageReportQuery) (store.UsageReport, error) {
	return store.UsageReport{By: query.By}, nil
}

func (storeProbe) DailyDigest(_ context.Context, windows []store.DailyDigestWindow) ([]store.DailyDigestDay, error) {
	days := make([]store.DailyDigestDay, 0, len(windows))
	for _, window := range windows {
		days = append(days, store.DailyDigestDay{Date: window.Date, Models: []store.UsageReportModel{}})
	}
	return days, nil
}

func (storeProbe) EfficiencyReceipt(context.Context, string, string, string) (efficiency.Receipt, error) {
	return efficiency.Receipt{}, sql.ErrNoRows
}

func (storeProbe) ListEfficiencyReceipts(context.Context, efficiency.Query) ([]efficiency.Receipt, error) {
	return nil, nil
}

func (storeProbe) EfficiencyRollup(context.Context, efficiency.Query) (efficiency.Rollup, error) {
	return efficiency.Rollup{}, nil
}

func (storeProbe) CostPerOutcome(_ context.Context, query efficiency.CostPerOutcomeQuery) (efficiency.CostPerOutcomeReport, error) {
	return efficiency.CostPerOutcomeReport{From: query.From, To: query.To}, nil
}

func (p storeProbe) CycleTimeReport(ctx context.Context) (store.CycleTimeReport, error) {
	if p.cycleTimeReport != nil {
		return p.cycleTimeReport(ctx)
	}
	return store.CycleTimeReport{}, nil
}

func (p storeProbe) BudgetCostEvents(ctx context.Context, query store.BudgetCostQuery) ([]store.BudgetCostEvent, error) {
	if p.budgetCostEvents != nil {
		return p.budgetCostEvents(ctx, query)
	}
	return nil, nil
}

func (storeProbe) RecordWorkflowPhaseEvent(context.Context, store.WorkflowPhaseEvent) (int64, error) {
	return 0, nil
}

func (storeProbe) LatestIssueAgentResumeState(context.Context, store.IssueIdentity) (store.AgentResumeState, error) {
	return store.AgentResumeState{}, store.ErrNotFound
}

type workflowPhaseEventStoreProbe struct {
	storeProbe

	mu     sync.Mutex
	events []store.WorkflowPhaseEvent
}

func (p *workflowPhaseEventStoreProbe) RecordWorkflowPhaseEvent(_ context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, event)
	return int64(len(p.events)), nil
}

func (p *workflowPhaseEventStoreProbe) workflowPhaseEventCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.events)
}

func (p *workflowPhaseEventStoreProbe) workflowPhaseEvents() []store.WorkflowPhaseEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]store.WorkflowPhaseEvent(nil), p.events...)
}

func (storeProbe) WorkflowMetricsReport(context.Context, store.WorkflowMetricsQuery) (store.WorkflowMetricsReport, error) {
	return store.WorkflowMetricsReport{}, nil
}

func (storeProbe) IssueWorkflowTimeline(context.Context, store.IssueIdentity) (store.WorkflowTimeline, error) {
	return store.WorkflowTimeline{}, nil
}

func (p storeProbe) ListValidatorVerdicts(ctx context.Context, query store.ValidatorVerdictQuery) ([]store.ValidatorVerdict, error) {
	if p.validatorVerdicts != nil {
		return p.validatorVerdicts(ctx, query)
	}
	return nil, nil
}

func (p storeProbe) RuntimeEvidence(ctx context.Context, query store.RuntimeEvidenceQuery) (store.RuntimeEvidence, error) {
	if p.runtimeEvidence != nil {
		return p.runtimeEvidence(ctx, query)
	}
	return store.RuntimeEvidence{
		Backend:         store.BackendSQLite,
		Healthy:         true,
		MigrationStatus: "applied through 0",
		Tables: []store.RuntimeTableEvidence{
			{Name: "workflow_phase_events", Scope: "project"},
		},
	}, nil
}

func (storeProbe) Queries() *sqlc.Queries {
	return nil
}

func (storeProbe) Close() error {
	return nil
}

type connectorProbe struct {
	name                 string
	projectURL           string
	fetchCandidateIssues func(context.Context) ([]connector.Issue, error)
}

func (p connectorProbe) Name() string {
	return p.name
}

func (p connectorProbe) ProjectURL(context.Context) (string, error) {
	return p.projectURL, nil
}

func (p connectorProbe) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	if p.fetchCandidateIssues != nil {
		return p.fetchCandidateIssues(ctx)
	}
	return nil, connector.ErrNotImplemented
}

func (p connectorProbe) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (p connectorProbe) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (p connectorProbe) CreateComment(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (p connectorProbe) UpdateIssueState(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (p connectorProbe) SetAssignee(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (p connectorProbe) SetField(context.Context, string, string, string) error {
	return connector.ErrNotImplemented
}

type kanbanStateUpdate struct {
	issueID string
	state   string
}

type kanbanIssueFieldUpdate struct {
	issueID string
	fieldID int
	value   string
}

type kanbanRemoval struct {
	issueID string
}

type kanbanComment struct {
	issueID string
	body    string
}

type kanbanCommentEdit struct {
	issueID   string
	commentID string
	body      string
}

type kanbanCommentDelete struct {
	issueID   string
	commentID string
}

type kanbanPRComment struct {
	repository string
	number     int
	body       string
}

type kanbanActionConnector struct {
	name string

	mu             sync.Mutex
	updateErr      error
	setFieldErr    error
	clearFieldErr  error
	removeErr      error
	states         []kanbanStateUpdate
	fields         []kanbanIssueFieldUpdate
	fieldClears    []kanbanIssueFieldUpdate
	removes        []kanbanRemoval
	closes         []string
	commentLog     []kanbanComment
	commentEdits   []kanbanCommentEdit
	commentDeletes []kanbanCommentDelete
	prCommentLog   []kanbanPRComment
	issueComments  map[string][]connector.IssueComment
	prThreads      map[string][]connector.IssueComment
	activeMoves    int
	maxMoves       int
	moveStarted    chan<- struct{}
	releaseMove    <-chan struct{}
}

type kanbanCapabilityConnector struct {
	*kanbanActionConnector
	capabilities connector.Capabilities
}

func (c *kanbanCapabilityConnector) Capabilities() connector.Capabilities {
	return c.capabilities
}

func (c *kanbanActionConnector) Name() string {
	return c.name
}

func (c *kanbanActionConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (c *kanbanActionConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (c *kanbanActionConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (c *kanbanActionConnector) CreateComment(_ context.Context, issueID string, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.commentLog = append(c.commentLog, kanbanComment{issueID: issueID, body: body})
	if c.issueComments == nil {
		c.issueComments = map[string][]connector.IssueComment{}
	}
	c.issueComments[strings.TrimSpace(issueID)] = append(c.issueComments[strings.TrimSpace(issueID)], connector.IssueComment{
		ID:          strconv.Itoa(len(c.issueComments[strings.TrimSpace(issueID)]) + 1),
		Backend:     connector.BackendGitHub.String(),
		Body:        body,
		AuthorLogin: "detent",
		TargetType:  connector.IssueCommentTargetIssue,
	})
	return nil
}

func (c *kanbanActionConnector) FetchIssueComments(_ context.Context, issue connector.Issue) ([]connector.IssueComment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return cloneTestIssueComments(c.issueComments[strings.TrimSpace(issue.ID)]), nil
}

func (c *kanbanActionConnector) UpdateIssueComment(_ context.Context, issueID string, commentID string, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	issueID = strings.TrimSpace(issueID)
	commentID = strings.TrimSpace(commentID)
	c.commentEdits = append(c.commentEdits, kanbanCommentEdit{issueID: issueID, commentID: commentID, body: body})
	comments := c.issueComments[issueID]
	for index := range comments {
		if strings.TrimSpace(comments[index].ID) == commentID {
			comments[index].Body = body
			c.issueComments[issueID] = comments
			return nil
		}
	}
	return sql.ErrNoRows
}

func (c *kanbanActionConnector) DeleteIssueComment(_ context.Context, issueID string, commentID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	issueID = strings.TrimSpace(issueID)
	commentID = strings.TrimSpace(commentID)
	c.commentDeletes = append(c.commentDeletes, kanbanCommentDelete{issueID: issueID, commentID: commentID})
	comments := c.issueComments[issueID]
	for index := range comments {
		if strings.TrimSpace(comments[index].ID) == commentID {
			c.issueComments[issueID] = append(comments[:index], comments[index+1:]...)
			return nil
		}
	}
	return sql.ErrNoRows
}

func (c *kanbanActionConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	c.mu.Lock()
	c.activeMoves++
	if c.activeMoves > c.maxMoves {
		c.maxMoves = c.activeMoves
	}
	c.states = append(c.states, kanbanStateUpdate{issueID: issueID, state: state})
	started := c.moveStarted
	release := c.releaseMove
	c.mu.Unlock()

	if started != nil {
		started <- struct{}{}
	}
	if release != nil {
		<-release
	}

	c.mu.Lock()
	c.activeMoves--
	err := c.updateErr
	c.mu.Unlock()
	return err
}

func (c *kanbanActionConnector) CloseIssue(_ context.Context, issueID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closes = append(c.closes, issueID)
	return nil
}

func (c *kanbanActionConnector) SetAssignee(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (c *kanbanActionConnector) SetField(context.Context, string, string, string) error {
	return connector.ErrNotImplemented
}

func (c *kanbanActionConnector) SetIssueField(_ context.Context, issueID string, fieldID int, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fields = append(c.fields, kanbanIssueFieldUpdate{issueID: issueID, fieldID: fieldID, value: value})
	return c.setFieldErr
}

func (c *kanbanActionConnector) ClearIssueField(_ context.Context, issueID string, fieldID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fieldClears = append(c.fieldClears, kanbanIssueFieldUpdate{issueID: issueID, fieldID: fieldID})
	return c.clearFieldErr
}

func (c *kanbanActionConnector) RemoveIssueFromProject(_ context.Context, issueID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removes = append(c.removes, kanbanRemoval{issueID: issueID})
	return c.removeErr
}

func (c *kanbanActionConnector) CreatePullRequestComment(_ context.Context, repository string, number int, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.prCommentLog = append(c.prCommentLog, kanbanPRComment{repository: repository, number: number, body: body})
	return nil
}

func (c *kanbanActionConnector) stateUpdates() []kanbanStateUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanStateUpdate(nil), c.states...)
}

func (c *kanbanActionConnector) issueFieldUpdates() []kanbanIssueFieldUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanIssueFieldUpdate(nil), c.fields...)
}

func (c *kanbanActionConnector) issueFieldClears() []kanbanIssueFieldUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanIssueFieldUpdate(nil), c.fieldClears...)
}

func (c *kanbanActionConnector) removals() []kanbanRemoval {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanRemoval(nil), c.removes...)
}

func (c *kanbanActionConnector) issueCloses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.closes...)
}

func (c *kanbanActionConnector) comments() []kanbanComment {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanComment(nil), c.commentLog...)
}

func (c *kanbanActionConnector) commentUpdates() []kanbanCommentEdit {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanCommentEdit(nil), c.commentEdits...)
}

func (c *kanbanActionConnector) commentRemovals() []kanbanCommentDelete {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanCommentDelete(nil), c.commentDeletes...)
}

func (c *kanbanActionConnector) prComments() []kanbanPRComment {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]kanbanPRComment(nil), c.prCommentLog...)
}

type kanbanPRCommentReaderConnector struct {
	*kanbanActionConnector
}

func (c *kanbanPRCommentReaderConnector) FetchPullRequestComments(_ context.Context, repository string, number int) ([]connector.IssueComment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.prThreads == nil {
		return []connector.IssueComment{}, nil
	}
	comments := c.prThreads[kanbanPRThreadKey(repository, number)]
	return append([]connector.IssueComment(nil), comments...), nil
}

func kanbanPRThreadKey(repository string, number int) string {
	return strings.TrimSpace(repository) + "#" + strconv.Itoa(number)
}

func (c *kanbanActionConnector) maxActiveMoves() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.maxMoves
}

func mustSetKanbanProject(t *testing.T, registry *project.Registry, id string, kanban workflowconfig.Kanban, actionConnector connector.Connector, dispatchPriorityByLabel ...string) {
	t.Helper()

	workflowCfg := workflowconfig.Default()
	workflowCfg.Tracker.Kind = workflowconfig.TrackerMemory
	workflowCfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Human Review", "Rework", "Merging"}
	workflowCfg.Tracker.ObservedStates = []string{"Backlog", "Blocked"}
	workflowCfg.Tracker.TerminalStates = []string{"Done", "Cancelled"}
	workflowCfg.Tracker.StateMap = workflowconfig.MapValue(map[string]any{
		"Human Review": "In Review",
	})
	workflowCfg.Agent.DispatchPriorityByLabel = append([]string(nil), dispatchPriorityByLabel...)
	workflowCfg.Server.Kanban = kanban

	trackedProject, err := project.New(project.Config{
		Project: globalconfig.Project{ID: id},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Work the issue.",
		},
	}, project.Dependencies{
		Connector: actionConnector,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := registry.Set(trackedProject); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
}

func performForm(t *testing.T, handler http.Handler, method string, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	handler.ServeHTTP(rec, req)
	return rec
}

func kanbanMoveLogRecords(t *testing.T, data []byte) []map[string]any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	var records []map[string]any
	for {
		var record map[string]any
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("decode log record: %v", err)
		}
		message, _ := record["msg"].(string)
		if strings.HasPrefix(message, "kanban move") {
			records = append(records, record)
		}
	}
	return records
}

func assertJSONLogField(t *testing.T, record map[string]any, key string, want any) {
	t.Helper()

	if got := record[key]; !reflect.DeepEqual(got, want) {
		t.Fatalf("log field %q = %#v, want %#v; record = %#v", key, got, want, record)
	}
}

func kanbanPendingStateCount(t *testing.T, server *web.Server) int {
	t.Helper()

	return kanbanPendingMutationCount(t, server, "states")
}

func kanbanPendingRemovalCount(t *testing.T, server *web.Server) int {
	t.Helper()

	return kanbanPendingMutationCount(t, server, "removed")
}

func kanbanPendingMutationCount(t *testing.T, server *web.Server, field string) int {
	t.Helper()

	serverValue := reflect.ValueOf(server)
	if serverValue.Kind() != reflect.Ptr || serverValue.IsNil() {
		t.Fatalf("server value = %v, want non-nil pointer", serverValue.Kind())
	}
	mutations := serverValue.Elem().FieldByName("kanbanMutations")
	if !mutations.IsValid() || mutations.IsNil() {
		return 0
	}
	values := mutations.Elem().FieldByName(field)
	if !values.IsValid() {
		t.Fatalf("kanbanMutations.%s is not available", field)
	}
	return values.Len()
}

func assertKanbanDialogSelectedTarget(t *testing.T, body string, target string) {
	t.Helper()

	selectedOptions := regexp.MustCompile(`<option[^>]*\sselected[^>]*>`).FindAllString(body, -1)
	if len(selectedOptions) != 1 {
		t.Fatalf("selected options = %#v, want exactly one in:\n%s", selectedOptions, body)
	}
	optionPattern := regexp.MustCompile(`<option value="` + regexp.QuoteMeta(target) + `"[^>]*>`)
	option := optionPattern.FindString(body)
	if option == "" {
		t.Fatalf("target option %q missing from dialog:\n%s", target, body)
	}
	if !strings.Contains(option, "selected") {
		t.Fatalf("target option %q is not selected: %s\nbody:\n%s", target, option, body)
	}
}

func performDemoForm(t *testing.T, handler http.Handler, path string, scenario string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	req.Header.Set(web.DemoScenarioHeader, scenario)
	handler.ServeHTTP(rec, req)
	return rec
}

func equalStateUpdates(left, right []kanbanStateUpdate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalIssueFieldUpdates(left, right []kanbanIssueFieldUpdate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalRemovals(left, right []kanbanRemoval) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalComments(left, right []kanbanComment) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalCommentUpdates(left, right []kanbanCommentEdit) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalCommentDeletes(left, right []kanbanCommentDelete) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalPRComments(left, right []kanbanPRComment) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func cloneTestIssueComments(comments []connector.IssueComment) []connector.IssueComment {
	if comments == nil {
		return nil
	}
	out := make([]connector.IssueComment, len(comments))
	for index, comment := range comments {
		out[index] = comment
		if comment.CreatedAt != nil {
			createdAt := *comment.CreatedAt
			out[index].CreatedAt = &createdAt
		}
		if comment.UpdatedAt != nil {
			updatedAt := *comment.UpdatedAt
			out[index].UpdatedAt = &updatedAt
		}
	}
	return out
}

type sseEvent struct {
	name string
	data string
}

func openEventStream(t *testing.T, server *web.Server) io.ReadCloser {
	t.Helper()

	ts := httptest.NewServer(server.Handler())
	ctx, cancel := context.WithTimeout(context.Background(), sseTestOperationTimeout)
	var body io.ReadCloser
	t.Cleanup(func() {
		cancel()
		if body != nil {
			_ = body.Close()
		}
		ts.Close()
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("ReadAll() error = %v", readErr)
		}
		t.Fatalf("status = %d, want %d; body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	body = resp.Body
	return body
}

func startWebServer(t *testing.T, server *web.Server) string {
	t.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	errs := make(chan error, 1)
	go func() {
		errs <- server.StartListener(listener)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), sseTestOperationTimeout)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}

		joinTimer := time.NewTimer(sseTestOperationTimeout)
		defer joinTimer.Stop()
		select {
		case err := <-errs:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("StartListener() error = %v", err)
			}
		case <-joinTimer.C:
			t.Errorf("timed out waiting for StartListener to return")
		}
	})

	return listener.Addr().String()
}

func openRawEventStream(t *testing.T, addr string, paths ...string) (net.Conn, *bufio.Reader) {
	t.Helper()

	path := "/events"
	if len(paths) > 0 {
		path = paths[0]
	}
	return openRawEventStreamWithHeaders(t, addr, path, nil)
}

func openRawEventStreamWithHeaders(t *testing.T, addr string, path string, headers map[string]string) (net.Conn, *bufio.Reader) {
	t.Helper()

	var dialer net.Dialer
	dialContext, cancelDial := context.WithTimeout(t.Context(), sseTestOperationTimeout)
	conn, err := dialer.DialContext(dialContext, "tcp", addr)
	cancelDial()
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	var body io.ReadCloser
	t.Cleanup(func() {
		_ = conn.Close()
		if body != nil {
			_ = body.Close()
		}
	})

	var request strings.Builder
	request.WriteString("GET " + path + " HTTP/1.1\r\n")
	request.WriteString("Host: " + addr + "\r\n")
	request.WriteString("Accept: text/event-stream\r\n")
	for key, value := range headers {
		request.WriteString(key + ": " + value + "\r\n")
	}
	request.WriteString("\r\n")
	if _, err := io.WriteString(conn, request.String()); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(sseTestOperationTimeout)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("ReadResponse() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	body = resp.Body
	return conn, bufio.NewReader(body)
}

func readRawSSEEvent(t *testing.T, conn net.Conn, reader *bufio.Reader) sseEvent {
	t.Helper()

	for {
		event := readRawSSEFrame(t, conn, reader)
		if event.name != "build" {
			return event
		}
	}
}

func readRawSSEEventNamed(t *testing.T, conn net.Conn, reader *bufio.Reader, name string) sseEvent {
	t.Helper()

	for {
		event := readRawSSEEvent(t, conn, reader)
		if event.name == name {
			return event
		}
	}
}

func readRawSSEFrame(t *testing.T, conn net.Conn, reader *bufio.Reader) sseEvent {
	t.Helper()

	var event sseEvent
	deadline := time.Now().Add(sseTestOperationTimeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("reading SSE stream: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event.name == "" {
				t.Fatal("SSE event missing name")
			}
			return event
		}
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			event.name = name
			continue
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			if event.data != "" {
				event.data += "\n"
			}
			event.data += data
			continue
		}
		t.Fatalf("unexpected SSE line %q", line)
	}
}

func readSSEEvent(t *testing.T, r io.Reader) sseEvent {
	t.Helper()
	return readSSEFrameMatching(t, r, false)
}

func readSSEFrame(t *testing.T, r io.Reader) sseEvent {
	t.Helper()
	return readSSEFrameMatching(t, r, true)
}

func readSSEFrameMatching(t *testing.T, r io.Reader, includeBuild bool) sseEvent {
	t.Helper()

	lines := make(chan string)
	errs := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		errs <- scanner.Err()
		close(lines)
	}()

	var event sseEvent
	deadline := time.After(sseTestOperationTimeout)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				if err := <-errs; err != nil {
					t.Fatalf("reading SSE stream: %v", err)
				}
				t.Fatal("SSE stream closed before event")
			}
			if line == "" {
				if event.name == "" {
					t.Fatal("SSE event missing name")
				}
				if event.name == "build" && !includeBuild {
					event = sseEvent{}
					continue
				}
				return event
			}
			if name, ok := strings.CutPrefix(line, "event: "); ok {
				event.name = name
				continue
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				if event.data != "" {
					event.data += "\n"
				}
				event.data += data
				continue
			}
			t.Fatalf("unexpected SSE line %q", line)
		case <-deadline:
			t.Fatal("timed out waiting for SSE event")
		}
	}
}

type refreshProbe struct {
	response web.RefreshResponse
	err      error
	calls    int
}

type tickLivenessProbe struct {
	values []telemetry.TickLiveness
}

func (p tickLivenessProbe) TickLiveness(time.Time) []telemetry.TickLiveness {
	return append([]telemetry.TickLiveness(nil), p.values...)
}

type operatorMoveProbe struct {
	request orchestrator.OperatorMoveRequest
	result  orchestrator.OperatorMoveResult
	err     error
	calls   int
}

func (p *operatorMoveProbe) ReconcileOperatorMove(_ context.Context, request orchestrator.OperatorMoveRequest) (orchestrator.OperatorMoveResult, error) {
	p.calls++
	p.request = request
	return p.result, p.err
}

func (p *refreshProbe) RequestRefresh(context.Context) (web.RefreshResponse, error) {
	p.calls++
	if p.err != nil {
		return web.RefreshResponse{}, p.err
	}
	return p.response, nil
}

func int64Pointer(value int64) *int64 {
	return &value
}
