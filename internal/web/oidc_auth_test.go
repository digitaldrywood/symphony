package web_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/auth"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestOIDCFlowBindsTransactionAndGatesBoard(t *testing.T) {
	t.Parallel()

	provider := &webOIDCProvider{identity: auth.Identity{Subject: "subject-1", Email: "operator@example.com", EmailVerified: true}}
	server := newOIDCWebServer(t, provider, []string{"operator@example.com"}, nil, nil)
	protected := performWebAuthRequest(t, server, http.MethodGet, "/projects/detent/kanban?view=compact", "", nil)
	if protected.Code != http.StatusSeeOther || protected.Header().Get("Location") != "/login?next=%2Fprojects%2Fdetent%2Fkanban%3Fview%3Dcompact" {
		t.Fatalf("protected response = %d Location %q", protected.Code, protected.Header().Get("Location"))
	}
	login := performWebAuthRequest(t, server, http.MethodGet, protected.Header().Get("Location"), "", nil)
	if login.Code != http.StatusOK || !strings.Contains(login.Body.String(), "Continue with identity provider") {
		t.Fatalf("login response = %d %s", login.Code, login.Body.String())
	}

	start := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/start?next=%2Fprojects%2Fdetent%2Fkanban%3Fview%3Dcompact", "", nil)
	if start.Code != http.StatusSeeOther || provider.state == "" || provider.nonce == "" || len(provider.verifier) != 43 {
		t.Fatalf("start response = %d provider = %#v", start.Code, provider)
	}
	transactionCookie := responseCookie(start.Result(), "detent_oidc_transaction")
	if transactionCookie == nil || !transactionCookie.HttpOnly || transactionCookie.Path != "/auth/oidc" || transactionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("transaction cookie = %#v", transactionCookie)
	}

	tampered := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/callback?code=authorization-code&state=altered", "", transactionCookie)
	if tampered.Code != http.StatusUnauthorized || !strings.Contains(tampered.Body.String(), "Identity unavailable") || provider.exchangeCalls != 0 {
		t.Fatalf("tampered callback = %d %s; exchange calls = %d", tampered.Code, tampered.Body.String(), provider.exchangeCalls)
	}

	start = performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/start?next=%2Fprojects%2Fdetent%2Fkanban%3Fview%3Dcompact", "", nil)
	transactionCookie = responseCookie(start.Result(), "detent_oidc_transaction")
	callbackPath := "/auth/oidc/callback?code=authorization-code&state=" + url.QueryEscape(provider.state)
	callback := performWebAuthRequest(t, server, http.MethodGet, callbackPath, "", transactionCookie)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/projects/detent/kanban?view=compact" {
		t.Fatalf("callback = %d Location %q body %s", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	sessionCookie := responseCookie(callback.Result(), "detent_session")
	if sessionCookie == nil || !sessionCookie.HttpOnly || sessionCookie.Path != "/" || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie = %#v", sessionCookie)
	}
	for _, path := range []string{"/", "/api/v1/state", "/reports"} {
		response := performWebAuthRequest(t, server, http.MethodGet, path, "", sessionCookie)
		if response.Code != http.StatusOK {
			t.Fatalf("authenticated GET %s = %d, want 200; body = %s", path, response.Code, response.Body.String())
		}
	}
	unauthenticatedSSE := performOIDCRequestWithHeaders(t, server, "/events", nil, map[string]string{"Accept": "text/event-stream"})
	if unauthenticatedSSE.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated SSE = %d, want 401", unauthenticatedSSE.Code)
	}
}

func TestOIDCFlowRejectsUnauthorizedAndUnverifiedEmails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity auth.Identity
		provider error
		want     int
		title    string
	}{
		{name: "email is outside allowlist", identity: auth.Identity{Subject: "subject-2", Email: "other@example.net", EmailVerified: true}, want: http.StatusForbidden, title: "Access denied"},
		{name: "domain is allowed", identity: auth.Identity{Subject: "subject-3", Email: "member@example.org", EmailVerified: true}, want: http.StatusSeeOther},
		{name: "unverified exact email", identity: auth.Identity{Subject: "subject-4", Email: "operator@example.com"}, want: http.StatusForbidden, title: "Access denied"},
		{name: "hosted customer cannot become local operator", identity: auth.Identity{Subject: "subject-5", Email: "operator@example.com", EmailVerified: true, Hosted: &auth.HostedIdentity{Subject: "subject-5", OrganizationID: "org_customer"}}, want: http.StatusForbidden, title: "Access denied"},
		{name: "hosted support cannot become local operator", identity: auth.Identity{Subject: "subject-6", Email: "operator@example.com", EmailVerified: true, Hosted: &auth.HostedIdentity{Subject: "subject-6", OrganizationID: "org_customer", SupportActor: "support@example.com"}}, want: http.StatusForbidden, title: "Access denied"},
		{name: "provider rejects unverified email", provider: auth.ErrOIDCUnverifiedEmail, want: http.StatusForbidden, title: "Access denied"},
		{name: "provider rejects nonce", provider: auth.ErrOIDCInvalidNonce, want: http.StatusUnauthorized, title: "Identity unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			provider := &webOIDCProvider{identity: tt.identity, err: tt.provider}
			server := newOIDCWebServer(t, provider, []string{"operator@example.com"}, []string{"example.org"}, nil)
			start := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/start?next=%2Freports", "", nil)
			transactionCookie := responseCookie(start.Result(), "detent_oidc_transaction")
			if transactionCookie == nil {
				t.Fatal("OIDC start response did not set a transaction cookie")
			}
			callback := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/callback?code=authorization-code&state="+url.QueryEscape(provider.state), "", transactionCookie)
			if callback.Code != tt.want {
				t.Fatalf("callback status = %d, want %d; body = %s", callback.Code, tt.want, callback.Body.String())
			}
			if tt.title != "" && !strings.Contains(callback.Body.String(), tt.title) {
				t.Fatalf("callback body = %s, want title %q", callback.Body.String(), tt.title)
			}
			if tt.want != http.StatusSeeOther && responseCookie(callback.Result(), "detent_session") != nil {
				t.Fatal("rejected callback created a Detent session")
			}
		})
	}
}

func TestOIDCCallbackDoesNotLogCredentials(t *testing.T) {
	t.Parallel()

	var logs strings.Builder
	provider := &webOIDCProvider{err: auth.ErrOIDCExchange}
	server := newOIDCWebServer(t, provider, []string{"operator@example.com"}, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	start := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/start", "", nil)
	transactionCookie := responseCookie(start.Result(), "detent_oidc_transaction")
	if transactionCookie == nil {
		t.Fatal("OIDC start response did not set a transaction cookie")
		return
	}
	code := "authorization-code-must-not-be-logged"
	callback := performWebAuthRequest(t, server, http.MethodGet, "/auth/oidc/callback?code="+code+"&state="+url.QueryEscape(provider.state), "", transactionCookie)
	if callback.Code != http.StatusUnauthorized {
		t.Fatalf("callback status = %d, want 401", callback.Code)
	}
	for _, secret := range []string{code, "test-client-secret", transactionCookie.Value, provider.verifier, provider.nonce} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("logs leaked credential %q: %s", secret, logs.String())
		}
	}
}

func TestOIDCRequiresSecurePublicURL(t *testing.T) {
	t.Parallel()

	deps := testDeps(t)
	deps.Store = openWebTestStore(t)
	deps.IdentityProvider = &webOIDCProvider{}
	_, err := web.NewServer(web.Config{
		DashboardURL: "http://detent.example.com",
		GlobalConfig: globalconfig.Config{Auth: globalconfig.Auth{
			Mode:          globalconfig.AuthModeOIDC,
			AllowedEmails: []string{"operator@example.com"},
		}},
	}, deps)
	if err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("NewServer() error = %v, want https requirement", err)
	}
}

type webOIDCProvider struct {
	state         string
	nonce         string
	verifier      string
	identity      auth.Identity
	err           error
	exchangeCalls int
}

func (p *webOIDCProvider) AuthorizationURL(state string, nonce string, verifier string) string {
	p.state = state
	p.nonce = nonce
	p.verifier = verifier
	query := url.Values{"state": {state}, "nonce": {nonce}}
	return "https://issuer.example.com/authorize?" + query.Encode()
}

func (p *webOIDCProvider) Exchange(_ context.Context, code string, verifier string, nonce string) (auth.Identity, error) {
	p.exchangeCalls++
	if code != "authorization-code" || verifier != p.verifier || nonce != p.nonce {
		return auth.Identity{}, errors.New("unexpected oidc transaction")
	}
	return p.identity, p.err
}

func newOIDCWebServer(t *testing.T, provider auth.IdentityProvider, allowedEmails []string, allowedDomains []string, logger *slog.Logger) *web.Server {
	t.Helper()
	deps := testDeps(t)
	deps.Store = openWebTestStore(t)
	deps.IdentityProvider = provider
	server, err := web.NewServer(web.Config{
		Logger:        logger,
		DashboardURL:  "http://127.0.0.1:4000",
		ServerAddress: "0.0.0.0:4000",
		GlobalConfig: globalconfig.Config{Auth: globalconfig.Auth{
			Mode:           globalconfig.AuthModeOIDC,
			AllowedEmails:  allowedEmails,
			AllowedDomains: allowedDomains,
			SessionTTL:     "1h",
			OIDC: globalconfig.OIDC{
				IssuerURL:    "https://issuer.example.com",
				ClientID:     "test-client",
				ClientSecret: "test-client-secret",
			},
		}},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

func performOIDCRequestWithHeaders(t *testing.T, server *web.Server, target string, cookie *http.Cookie, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}
