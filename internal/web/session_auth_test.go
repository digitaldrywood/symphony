package web_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/auth"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestMagicLinkAuthFlowAndRouteGate(t *testing.T) {
	t.Parallel()

	sender := &webAuthSender{}
	server := newMagicLinkWebServer(t, sender)

	protected := performWebAuthRequest(t, server, http.MethodGet, "/projects/detent/kanban?view=compact", "", nil)
	if protected.Code != http.StatusSeeOther {
		t.Fatalf("protected status = %d, want %d", protected.Code, http.StatusSeeOther)
	}
	if got := protected.Header().Get("Location"); got != "/login?next=%2Fprojects%2Fdetent%2Fkanban%3Fview%3Dcompact" {
		t.Fatalf("protected Location = %q", got)
	}

	login := performWebAuthRequest(t, server, http.MethodGet, protected.Header().Get("Location"), "", nil)
	if login.Code != http.StatusOK || !strings.Contains(login.Body.String(), "Email me a sign-in link") {
		t.Fatalf("login response = %d %s", login.Code, login.Body.String())
	}
	next := "/projects/detent/kanban?view=compact"
	denied := requestMagicLinkPage(t, server, "other@example.com", next)
	allowed := requestMagicLinkPage(t, server, "Operator@Example.com", next)
	if denied.Code != http.StatusOK || allowed.Code != http.StatusOK || denied.Body.String() != allowed.Body.String() {
		t.Fatalf("allowed and non-allowed responses differ:\nallowed=%d %s\nnon-allowed=%d %s", allowed.Code, allowed.Body.String(), denied.Code, denied.Body.String())
	}
	if len(sender.messages) != 1 {
		t.Fatalf("sender messages = %d, want 1", len(sender.messages))
	}
	link, err := url.Parse(sender.messages[0].URL)
	if err != nil {
		t.Fatalf("Parse(magic link) error = %v", err)
	}
	callback := performWebAuthRequest(t, server, http.MethodGet, link.RequestURI(), "", nil)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != next {
		t.Fatalf("callback = %d Location %q", callback.Code, callback.Header().Get("Location"))
	}
	sessionCookie := responseCookie(callback.Result(), "detent_session")
	if sessionCookie == nil || !sessionCookie.HttpOnly || sessionCookie.Path != "/" || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie = %#v", sessionCookie)
	}

	authenticated := performWebAuthRequest(t, server, http.MethodGet, "/", "", sessionCookie)
	if authenticated.Code != http.StatusOK {
		t.Fatalf("authenticated dashboard status = %d, want 200; body = %s", authenticated.Code, authenticated.Body.String())
	}
	api := performWebAuthRequest(t, server, http.MethodGet, "/api/v1/state", "", sessionCookie)
	if api.Code != http.StatusOK {
		t.Fatalf("authenticated API status = %d, want 200; body = %s", api.Code, api.Body.String())
	}
	reuse := performWebAuthRequest(t, server, http.MethodGet, link.RequestURI(), "", nil)
	if reuse.Code != http.StatusUnauthorized || !strings.Contains(reuse.Body.String(), "already been used") {
		t.Fatalf("reused link response = %d %s", reuse.Code, reuse.Body.String())
	}
}

func TestMagicLinkGateHandlesHTMXSSEAndAPIKeys(t *testing.T) {
	t.Parallel()

	server := newMagicLinkWebServer(t, &webAuthSender{})
	tests := []struct {
		name       string
		path       string
		headers    map[string]string
		wantStatus int
		wantHeader string
	}{
		{
			name:       "htmx navigates whole page to login",
			path:       "/api/v1/board/card",
			headers:    map[string]string{"HX-Request": "true"},
			wantStatus: http.StatusOK,
			wantHeader: "/login?next=%2Fapi%2Fv1%2Fboard%2Fcard",
		},
		{
			name:       "sse is rejected without redirect loop",
			path:       "/events",
			headers:    map[string]string{"Accept": "text/event-stream"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "api key request reaches existing auth",
			path:       "/api/v1/state",
			headers:    map[string]string{"Authorization": "Bearer invalid"},
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			for name, value := range tt.headers {
				req.Header.Set(name, value)
			}
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, req)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if tt.wantHeader != "" && recorder.Header().Get("HX-Redirect") != tt.wantHeader {
				t.Fatalf("HX-Redirect = %q, want %q", recorder.Header().Get("HX-Redirect"), tt.wantHeader)
			}
		})
	}
}

func TestDisabledMagicLinkAuthLeavesRoutesUnchanged(t *testing.T) {
	t.Parallel()

	server, err := web.NewServer(web.Config{}, testDeps(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	root := performWebAuthRequest(t, server, http.MethodGet, "/", "", nil)
	if root.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", root.Code)
	}
	login := performWebAuthRequest(t, server, http.MethodGet, "/login", "", nil)
	if login.Code != http.StatusNotFound {
		t.Fatalf("GET /login status = %d, want 404 when auth disabled", login.Code)
	}
}

func TestLocalSessionGateRejectsHostedIdentity(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		path       string
		accept     string
		wantStatus int
	}{
		{name: "dashboard", path: "/", wantStatus: http.StatusSeeOther},
		{name: "API", path: "/api/v1/state", wantStatus: http.StatusSeeOther},
		{name: "SSE", path: "/events", accept: "text/event-stream", wantStatus: http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			deps := testDeps(t)
			deps.Store = hostedWebSessionStore{Store: openWebTestStore(t)}
			deps.MagicLinkSender = &webAuthSender{}
			server, err := web.NewServer(web.Config{
				DashboardURL: "https://detent.test",
				GlobalConfig: globalconfig.Config{Auth: globalconfig.Auth{
					Mode: globalconfig.AuthModeMagicLink, AllowedEmails: []string{"operator@example.com"}, LinkTTL: "15m", SessionTTL: "1h",
				}},
			}, deps)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, tt.path, nil)
			request.Header.Set("Accept", tt.accept)
			request.AddCookie(&http.Cookie{Name: "detent_session", Value: "hosted-session"})
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			cookie := responseCookie(recorder.Result(), "detent_session")
			if recorder.Code != tt.wantStatus || cookie == nil || cookie.MaxAge != -1 {
				t.Fatalf("hosted session status=%d clearing cookie=%#v", recorder.Code, cookie)
			}
		})
	}
}

type hostedWebSessionStore struct {
	store.Store
}

func (hostedWebSessionStore) WebSession(context.Context, string, time.Time) (auth.Session, error) {
	return auth.Session{Email: "operator@example.com", ExpiresAt: time.Now().Add(time.Hour), Identity: &auth.HostedIdentity{Subject: "user_customer", OrganizationID: "org_customer"}}, nil
}

type webAuthSender struct {
	messages []auth.Message
}

func (s *webAuthSender) SendMagicLink(_ context.Context, message auth.Message) error {
	s.messages = append(s.messages, message)
	return nil
}

func newMagicLinkWebServer(t *testing.T, sender auth.Sender) *web.Server {
	t.Helper()
	server, err := web.NewServer(web.Config{
		DashboardURL:  "http://detent.test",
		ServerAddress: "0.0.0.0:4000",
		GlobalConfig: globalconfig.Config{Auth: globalconfig.Auth{
			Mode:          globalconfig.AuthModeMagicLink,
			AllowedEmails: []string{"operator@example.com"},
			LinkTTL:       "15m",
			SessionTTL:    "1h",
		}},
	}, func() web.Dependencies {
		deps := testDeps(t)
		deps.Store = openWebTestStore(t)
		deps.MagicLinkSender = sender
		return deps
	}())
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

func requestMagicLinkPage(t *testing.T, server *web.Server, email string, next string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{"email": {email}, "next": {next}}.Encode()
	return performWebAuthRequest(t, server, http.MethodPost, "/login", form, nil)
}

func performWebAuthRequest(t *testing.T, server *web.Server, method string, target string, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func responseCookie(response *http.Response, name string) *http.Cookie {
	for _, cookie := range response.Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}
