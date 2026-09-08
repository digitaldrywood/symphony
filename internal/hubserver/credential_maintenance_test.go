package hubserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func reopenCredentialFixture(t *testing.T, f *browserHostedFixture, maintenance bool) {
	t.Helper()
	cfg := f.service.config
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.CredentialMaintenance = maintenance
	cfg.ListenAddress = "127.0.0.1:0"
	service, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.service = service
}

func testMaintenanceCheckLifecycle(t *testing.T, f *browserHostedFixture, path string, created tokenResponse, version tracker.ChangeVersion) {
	t.Helper()
	request := changeTestResult(version)
	for _, test := range []struct {
		name, method, suffix string
		status               int
	}{
		{"rotation", http.MethodPost, "/rotate", http.StatusOK},
		{"revocation", http.MethodDelete, "", http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			reopenCredentialFixture(t, f, true)
			response := performHubAPIRequest(t, f.service, test.method, "/api/v1/tokens/"+created.ID+test.suffix, testHubAdminToken, nil)
			requireNativeStatus(t, response, test.status)
			oldToken := created.Token
			if test.name == "rotation" {
				var rotated tokenResponse
				decodeHubResponse(t, response, &rotated)
				if rotated.ID != created.ID || rotated.Token == oldToken {
					t.Fatal("rotation changed principal or retained the old secret")
				}
				created = rotated
			}
			reopenCredentialFixture(t, f, false)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, oldToken, request), http.StatusUnauthorized)
			if test.name == "rotation" {
				requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, created.Token, request), http.StatusOK)
			}
		})
	}
}

func TestCredentialMaintenanceAccess(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	for _, project := range []string{f.project, f.privateProject} {
		requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {project}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
	}
	binding := runnerauth.NewBinding()
	path := "/api/v2/organizations/org_browser_preview/runner-enrollments"
	response := f.setupRequest(t, "owner", http.MethodPost, path, runnerauth.EnrollmentRequest{Binding: binding, ProjectIDs: []tracker.ProjectID{tracker.ProjectID(f.project)}, Operations: []string{runnerauth.Read, runnerauth.Heartbeat}, TTLSeconds: 60})
	requireNativeStatus(t, response, http.StatusCreated)
	var enrollment runnerauth.Enrollment
	decodeHubResponse(t, response, &enrollment)
	runnerToken, err := apikey.GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/redeem", enrollment.Token, runnerauth.Redemption{Binding: binding, Credential: runnerToken, Hostname: "maintenance-test", DisplayName: "Maintenance test", Capacity: 1, Version: "test"}), http.StatusCreated)
	f.service.config.InitialAdminToken = []byte("replacement-bootstrap-secret")
	reopenCredentialFixture(t, f, true)
	if second, err := Open(t.Context(), f.service.config); err == nil {
		second.Close()
		t.Fatal("second database owner was admitted")
	}
	var scopedAdmin, reporter, worker string
	for _, scope := range []apiScope{apiScopeAdmin, apiScopeOperator, apiScopeWorker} {
		response := performHubAPIRequest(t, f.service, http.MethodPost, "/api/v1/tokens", testHubAdminToken, tokenRequest{Name: string(scope), Scope: scope})
		requireNativeStatus(t, response, http.StatusCreated)
		var token tokenResponse
		decodeHubResponse(t, response, &token)
		requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, "/api/v2/tokens/"+token.ID+"/grants", testHubAdminToken, map[string]string{"organization_id": "org_browser_preview", "project_id": f.project}), http.StatusNoContent)
		switch scope {
		case apiScopeAdmin:
			scopedAdmin = token.Token
		case apiScopeOperator:
			reporter = token.Token
		case apiScopeWorker:
			worker = token.Token
		}
	}
	for _, test := range []struct {
		name, bearer, cookie, origin string
	}{
		{name: "anonymous loopback"},
		{name: "invalid", bearer: "invalid"},
		{name: "reporter", bearer: reporter},
		{name: "worker", bearer: worker},
		{name: "enrolled runner", bearer: runnerToken},
		{name: "replacement bootstrap", bearer: "replacement-bootstrap-secret"},
		{name: "project administrator", bearer: scopedAdmin},
		{name: "owner cookie", cookie: "owner"},
		{name: "staff cookie", cookie: "staff"},
		{name: "support cookie", cookie: "support-viewer"},
		{name: "browser origin", bearer: testHubAdminToken, origin: f.server.URL},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, route := range []struct{ method, path string }{
				{http.MethodPost, "/api/v1/tokens"},
				{http.MethodPost, "/api/v1/tokens/example/rotate"},
				{http.MethodDelete, "/api/v1/tokens/example"},
				{http.MethodPost, "/api/v2/tokens/example/grants"},
			} {
				request := httptest.NewRequest(route.method, route.path, strings.NewReader("{}"))
				request.RemoteAddr = "127.0.0.1:12345"
				request.Header.Set("Content-Type", "application/json")
				if test.bearer != "" {
					request.Header.Set("Authorization", "Bearer "+test.bearer)
				}
				if test.cookie != "" {
					request.AddCookie(f.cookies[test.cookie])
				}
				if test.origin != "" {
					request.Header.Set("Origin", test.origin)
				}
				response := httptest.NewRecorder()
				f.service.Handler().ServeHTTP(response, request)
				if response.Code != http.StatusUnauthorized && response.Code != http.StatusForbidden {
					t.Fatalf("%s %s: status %d", route.method, route.path, response.Code)
				}
			}
		})
	}
	for _, path := range []string{"/", "/health", "/api/v1/work-items", "/api/v2/organizations/org_browser_preview/projects"} {
		requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodGet, path, testHubAdminToken, nil), http.StatusNotFound)
	}
	var events int
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_audit WHERE event = 'credential_maintenance' AND actual_actor = ? AND effective_user = ? AND organization_id = ?", bootstrapTokenID, bootstrapTokenID, "org_browser_preview").Scan(&events); err != nil || events != 6 {
		t.Fatalf("attributed maintenance events = %d, error = %v", events, err)
	}
	reopenCredentialFixture(t, f, false)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, "/api/v1/tokens", testHubAdminToken, tokenRequest{Name: "public-denied", Scope: apiScopeOperator}), http.StatusNotFound)
}

func TestCredentialMaintenanceConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"non-hosted", func(c *Config) { c.Hosted = nil }},
		{"public listener", func(c *Config) { c.ListenAddress = "0.0.0.0:7778" }},
		{"public TLS listener", func(c *Config) { c.ListenAddress = "0.0.0.0:7778"; c.TLSCertFile = "cert"; c.TLSKeyFile = "key" }},
		{"proxy", func(c *Config) { c.TrustedProxy = true }},
		{"unspecified provider", func(c *Config) { c.Hosted.WorkOSOrganizationID = "" }},
		{"wrong organization", func(c *Config) { c.Hosted.OrganizationID = "org_other" }},
		{"wrong provider", func(c *Config) { c.Hosted.WorkOSOrganizationID = "org_other" }},
		{"wrong origin", func(c *Config) { c.Hosted.PublicURL = "https://other.example.test" }},
		{"wrong bootstrap", func(c *Config) { c.Hosted.BootstrapSubject = "other-owner" }},
		{"unbound", func(c *Config) { c.DatabasePath = filepath.Join(t.TempDir(), "unbound.db") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newBrowserHostedFixture(t, true)
			cfg := f.service.config
			if err := f.service.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.CredentialMaintenance = true
			test.edit(&cfg)
			service, err := Open(t.Context(), cfg)
			if err == nil {
				service.Close()
				t.Fatal("invalid maintenance configuration was accepted")
			}
			if strings.HasPrefix(test.name, "wrong ") || test.name == "unbound" {
				if !errors.Is(err, ErrHostedDatabaseBinding) {
					t.Fatalf("binding error = %v", err)
				}
			}
		})
	}
}
