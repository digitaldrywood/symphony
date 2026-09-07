package hubserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type browserHostedProvider struct {
	mu             sync.Mutex
	base           string
	organization   auth.Organization
	members        map[string]auth.Membership
	sessions       map[string]auth.HostedIdentity
	invitations    map[string]auth.Invitation
	inviteRoles    map[string]string
	authorizations map[string]string
	codes          map[string]auth.Identity
	sequence       int
}

func (p *browserHostedProvider) AuthorizationURL(state, _ string, verifier string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.authorizations[state] = verifier
	return p.base + "/__preview/authorize?state=" + url.QueryEscape(state)
}

func (p *browserHostedProvider) Exchange(_ context.Context, code, verifier, nonce string) (auth.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	identity, ok := p.codes[code]
	if !ok || verifier == "" || p.authorizations[nonce] != verifier {
		return auth.Identity{}, auth.ErrHostedIdentity
	}
	delete(p.codes, code)
	delete(p.authorizations, nonce)
	return identity, nil
}

func (p *browserHostedProvider) CurrentSession(_ context.Context, identity auth.HostedIdentity) (auth.HostedIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.sessions[identity.SessionID]
	if !ok || !current.ExpiresAt.After(time.Now()) {
		return auth.HostedIdentity{}, auth.ErrHostedIdentity
	}
	return current, nil
}

func (p *browserHostedProvider) Memberships(_ context.Context, user, organization string) ([]auth.Membership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var members []auth.Membership
	for _, member := range p.members {
		if (user == "" || member.UserID == user) && (organization == "" || member.OrganizationID == organization) {
			members = append(members, member)
		}
	}
	return members, nil
}

func (p *browserHostedProvider) Organization(_ context.Context, id string) (auth.Organization, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id != p.organization.ID {
		return auth.Organization{}, auth.ErrHostedIdentity
	}
	return p.organization, nil
}

func (p *browserHostedProvider) CreateOrganization(_ context.Context, externalID, name string) (auth.Organization, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.organization = auth.Organization{ID: "org_browser_provider", ExternalID: externalID, Name: name}
	return p.organization, nil
}

func (p *browserHostedProvider) CreateMembership(_ context.Context, user, organization, role string) (auth.Membership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	member := auth.Membership{ID: "membership_" + user, UserID: user, OrganizationID: organization, Status: "active"}
	member.Role.Slug = role
	p.members[member.ID] = member
	return member, nil
}

func (p *browserHostedProvider) SetMembershipRole(_ context.Context, id, role string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	member, ok := p.members[id]
	if !ok {
		return auth.ErrHostedIdentity
	}
	member.Role.Slug = role
	p.members[id] = member
	return nil
}

func (p *browserHostedProvider) RevokeMembership(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.members, id)
	return nil
}

func (p *browserHostedProvider) Invite(_ context.Context, organization, email, role, _ string) (auth.Invitation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sequence++
	invitation := auth.Invitation{ID: fmt.Sprintf("invitation_browser_%d", p.sequence), Email: email, OrganizationID: organization, State: "pending", ExpiresAt: time.Now().Add(time.Hour)}
	p.invitations[invitation.ID], p.inviteRoles[invitation.ID] = invitation, role
	return invitation, nil
}

func (p *browserHostedProvider) Invitation(_ context.Context, token string) (auth.Invitation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	invitation, ok := p.invitations[token]
	if !ok {
		return auth.Invitation{}, auth.ErrHostedIdentity
	}
	return invitation, nil
}

func (p *browserHostedProvider) AcceptInvitation(_ context.Context, token, user string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	invitation, ok := p.invitations[token]
	if !ok || invitation.State != "pending" {
		return auth.ErrHostedIdentity
	}
	invitation.State, invitation.AcceptedUserID = "accepted", user
	p.invitations[token] = invitation
	member := auth.Membership{ID: "membership_" + user, UserID: user, OrganizationID: invitation.OrganizationID, Status: "active"}
	member.Role.Slug = p.inviteRoles[token]
	p.members[member.ID] = member
	return nil
}

func (p *browserHostedProvider) RevokeSession(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, id)
	return nil
}

func (p *browserHostedProvider) identity(user, email, organization, support string) auth.Identity {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sequence++
	now := time.Now().UTC()
	hosted := auth.HostedIdentity{Subject: user, OrganizationID: organization, SessionID: fmt.Sprintf("session_browser_%d", p.sequence), CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(15 * time.Minute), SupportActor: support}
	if support != "" {
		hosted.SupportReason = "customer-request"
	}
	p.sessions[hosted.SessionID] = hosted
	return auth.Identity{Subject: user, Email: email, EmailVerified: true, Hosted: &hosted}
}

type browserHostedFixture struct {
	service        *Service
	server         *httptest.Server
	provider       *browserHostedProvider
	cookies        map[string]*http.Cookie
	project        string
	privateProject string
	stop           chan struct{}
	stopOnce       sync.Once
}

func newBrowserHostedFixture(t *testing.T, allocated bool) *browserHostedFixture {
	t.Helper()
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	base := "http://" + server.Listener.Addr().String()
	provider := &browserHostedProvider{
		base: base, organization: auth.Organization{ID: "org_browser_provider", ExternalID: "org_browser_preview", Name: "Browser organization"},
		members: make(map[string]auth.Membership), sessions: make(map[string]auth.HostedIdentity), invitations: make(map[string]auth.Invitation), inviteRoles: make(map[string]string), authorizations: make(map[string]string), codes: make(map[string]auth.Identity),
	}
	cfg := Config{DatabasePath: filepath.Join(t.TempDir(), "hosted-browser.db"), GitHubDisabled: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Hosted: &HostedConfig{
		OrganizationID: "org_browser_preview", BootstrapSubject: "user_browser_owner", PublicURL: base, Provider: provider,
		StaffEmails: []string{"staff@example.test", "support@example.test"}, SupportActors: []string{"support@example.test"},
		Directory: []HostedDestination{{OrganizationID: "org_browser_preview", WorkOSOrganizationID: "org_browser_provider", PublicURL: base}},
	}}
	if allocated {
		cfg.Hosted.WorkOSOrganizationID = provider.organization.ID
	}
	service, err := Open(t.Context(), cfg)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	fixture := &browserHostedFixture{service: service, server: server, provider: provider, cookies: make(map[string]*http.Cookie), stop: make(chan struct{})}
	t.Cleanup(func() {
		server.Close()
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, account := range []struct {
		name, user, email, role, support string
	}{
		{name: "owner", user: "user_browser_owner", email: "owner@example.test", role: "owner"},
		{name: "viewer", user: "user_browser_viewer", email: "viewer@example.test", role: "viewer"},
		{name: "staff", user: "user_browser_staff", email: "staff@example.test"},
		{name: "support-staff", user: "user_browser_support", email: "support@example.test"},
		{name: "support-viewer", user: "user_browser_viewer", email: "viewer@example.test", support: "support@example.test"},
		{name: "invitee", user: "user_browser_invitee", email: "invitee@example.test"},
		{name: "wrong-organization", user: "user_browser_owner", email: "owner@example.test"},
		{name: "revoked", user: "user_browser_viewer", email: "viewer@example.test"},
		{name: "expired", user: "user_browser_viewer", email: "viewer@example.test"},
	} {
		organization := ""
		if allocated && (account.role != "" || account.support != "" || account.name == "revoked" || account.name == "expired") {
			organization = provider.organization.ID
		}
		if account.name == "wrong-organization" {
			organization = "org_browser_other"
		}
		identity := provider.identity(account.user, account.email, organization, account.support)
		if allocated && account.role != "" {
			membership, err := provider.CreateMembership(t.Context(), account.user, organization, account.role)
			if err != nil {
				t.Fatal(err)
			}
			if account.name == "owner" {
				err = service.bootstrapHostedMember(t.Context(), identity)
			} else {
				err = service.addHostedMember(t.Context(), identity, membership)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		token, session, err := service.hostedSessions.CreateIdentitySession(t.Context(), identity)
		if err != nil {
			t.Fatal(err)
		}
		fixture.cookies[account.name] = &http.Cookie{Name: hostedCookie, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Expires: session.ExpiresAt}
		if account.name == "revoked" {
			if err := provider.RevokeSession(t.Context(), identity.Hosted.SessionID); err != nil {
				t.Fatal(err)
			}
		}
		if account.name == "expired" {
			provider.mu.Lock()
			identity.Hosted.ExpiresAt = time.Now().Add(-time.Second)
			provider.sessions[identity.Hosted.SessionID] = *identity.Hosted
			provider.mu.Unlock()
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /__preview/account/{account}", func(w http.ResponseWriter, r *http.Request) {
		cookie, ok := fixture.cookies[r.PathValue("account")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		http.SetCookie(w, cookie)
		http.Redirect(w, r, "/organization", http.StatusSeeOther)
	})
	mux.HandleFunc("GET /__preview/authorize", func(w http.ResponseWriter, r *http.Request) {
		state := r.URL.Query().Get("state")
		provider.mu.Lock()
		organization := provider.organization.ID
		provider.mu.Unlock()
		identity := provider.identity("user_browser_owner", "owner@example.test", organization, "")
		provider.mu.Lock()
		code := "code_" + identity.Hosted.SessionID
		provider.codes[code] = identity
		provider.mu.Unlock()
		http.Redirect(w, r, "/auth/oidc/callback?code="+url.QueryEscape(code)+"&state="+url.QueryEscape(state), http.StatusSeeOther)
	})
	mux.HandleFunc("POST /__preview/stop", func(w http.ResponseWriter, _ *http.Request) {
		fixture.stopOnce.Do(func() { close(fixture.stop) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", service.Handler())
	server.Config.Handler = mux
	server.Start()
	if allocated {
		fixture.project = fixture.createProject(t, "Browser collaboration")
		fixture.privateProject = fixture.createProject(t, "Owner private project")
		response := fixture.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_viewer"}, "project": {fixture.project}})
		browserHostedStatus(t, response, http.StatusSeeOther)
		payload, err := json.Marshal(tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "browser-preview-issue"}, Title: "Review the invitation flow", Body: "Browser fixture private body", State: "Todo"})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, base+"/api/v2/organizations/org_browser_preview/projects/"+fixture.project+"/work-items", strings.NewReader(string(payload)))
		request.Header.Set("Content-Type", "application/json")
		cookie := fixture.cookies["owner"]
		if cookie == nil {
			t.Fatal("owner session cookie is missing")
		}
		request.Header.Set("X-CSRF-Token", hostedCSRF(cookie.Value))
		request.AddCookie(cookie)
		response = httptest.NewRecorder()
		service.Handler().ServeHTTP(response, request)
		browserHostedStatus(t, response, http.StatusOK)
	}
	return fixture
}

func (f *browserHostedFixture) form(t *testing.T, account, path string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	cookie := f.cookies[account]
	if cookie == nil {
		t.Fatal("account session cookie is missing")
	}
	values.Set("csrf", hostedCSRF(cookie.Value))
	request := httptest.NewRequest(http.MethodPost, f.server.URL+path, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", f.server.URL)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	return response
}

func (f *browserHostedFixture) page(t *testing.T, account, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, f.server.URL+path, nil)
	if cookie := f.cookies[account]; cookie != nil {
		request.AddCookie(cookie)
	}
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	return response
}

func (f *browserHostedFixture) createProject(t *testing.T, name string) string {
	t.Helper()
	response := f.form(t, "owner", "/projects", url.Values{"name": {name}, "grant_access": {"true"}})
	browserHostedStatus(t, response, http.StatusSeeOther)
	project := strings.TrimPrefix(response.Header().Get("Location"), "/projects/")
	if !strings.HasPrefix(project, "prj_") {
		t.Fatal("project form returned an invalid project location")
	}
	return project
}

func browserHostedStatus(t *testing.T, response *httptest.ResponseRecorder, status int) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d: %s", response.Code, status, response.Body.String())
	}
}

func TestHostedBrowserHTTPPages(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	tests := []struct {
		name, account, path, contains, excludes string
		status                                  int
	}{
		{name: "login", path: "/login", status: http.StatusOK, contains: "Continue with WorkOS"},
		{name: "owner organization", account: "owner", path: "/organization", status: http.StatusOK, contains: "Members and invitations"},
		{name: "viewer organization", account: "viewer", path: "/organization", status: http.StatusOK, contains: "Browser collaboration", excludes: "Owner private project"},
		{name: "viewer project", account: "viewer", path: "/projects/" + f.project, status: http.StatusOK, contains: "Review the invitation flow", excludes: "Browser fixture private body"},
		{name: "ungranted project", account: "viewer", path: "/projects/" + f.privateProject, status: http.StatusForbidden, contains: "unavailable", excludes: "Owner private project"},
		{name: "ordinary staff", account: "staff", path: "/projects/" + f.project, status: http.StatusForbidden, excludes: "Review the invitation flow"},
		{name: "support viewer", account: "support-viewer", path: "/projects/" + f.project, status: http.StatusOK, contains: "Exit support session"},
		{name: "wrong organization", account: "wrong-organization", path: "/projects/" + f.project, status: http.StatusForbidden, excludes: "Review the invitation flow"},
		{name: "revoked session", account: "revoked", path: "/projects/" + f.project, status: http.StatusUnauthorized, excludes: "Review the invitation flow"},
		{name: "expired session", account: "expired", path: "/projects/" + f.project, status: http.StatusUnauthorized, excludes: "Review the invitation flow"},
		{name: "support entry", account: "support-staff", path: "/support", status: http.StatusOK, contains: "Start support sign-in"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := f.page(t, tt.account, tt.path)
			browserHostedStatus(t, response, tt.status)
			html := response.Body.String()
			if tt.contains != "" && !strings.Contains(html, tt.contains) {
				t.Errorf("page missing %q", tt.contains)
			}
			if tt.excludes != "" && strings.Contains(html, tt.excludes) {
				t.Errorf("page contains forbidden %q", tt.excludes)
			}
			for _, forbidden := range []string{`sse-connect`, `hx-get`, `/api/v1/`, `chat-panel`} {
				if strings.Contains(html, forbidden) {
					t.Errorf("hosted response includes %q", forbidden)
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Error("hosted page is cacheable")
			}
		})
	}
}

func TestHostedBrowserHTTPForms(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	tests := []struct {
		name, account, path string
		values              url.Values
		status              int
	}{
		{name: "project requires explicit access", account: "owner", path: "/projects", values: url.Values{"name": {"Unapproved project"}}, status: http.StatusUnprocessableEntity},
		{name: "viewer cannot create", account: "viewer", path: "/projects", values: url.Values{"name": {"Viewer project"}, "grant_access": {"true"}}, status: http.StatusForbidden},
		{name: "owner creates project", account: "owner", path: "/projects", values: url.Values{"name": {"Form project"}, "grant_access": {"true"}}, status: http.StatusSeeOther},
		{name: "owner invites member", account: "owner", path: "/organization/invite", values: url.Values{"email": {"invitee@example.test"}, "role": {"viewer"}}, status: http.StatusSeeOther},
		{name: "staff cannot invite", account: "staff", path: "/organization/invite", values: url.Values{"email": {"other@example.test"}, "role": {"member"}}, status: http.StatusForbidden},
		{name: "wrong organization cannot switch", account: "viewer", path: "/organization/switch", values: url.Values{"organization": {"org_unknown"}}, status: http.StatusForbidden},
		{name: "authorized support starts", account: "support-staff", path: "/support/start", values: url.Values{}, status: http.StatusOK},
		{name: "ordinary staff cannot impersonate", account: "staff", path: "/support/start", values: url.Values{}, status: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := f.form(t, tt.account, tt.path, tt.values)
			browserHostedStatus(t, response, tt.status)
		})
	}
	var invitation string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM hosted_invitations WHERE email = 'invitee@example.test'").Scan(&invitation); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		account string
		status  int
	}{
		{name: "wrong invited account", account: "viewer", status: http.StatusForbidden},
		{name: "invitation accepted", account: "invitee", status: http.StatusSeeOther},
		{name: "invitation replay", account: "invitee", status: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := f.form(t, tt.account, "/organization/join", url.Values{"token": {invitation}})
			browserHostedStatus(t, response, tt.status)
		})
	}
}

func TestHostedBrowserFirstOrganization(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, false)
	response := f.page(t, "owner", "/organization")
	browserHostedStatus(t, response, http.StatusOK)
	for _, expected := range []string{`action="/organization/create"`, `action="/organization/join"`} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("onboarding missing %q", expected)
		}
	}
	response = f.form(t, "owner", "/organization/create", url.Values{"name": {"New browser organization"}})
	browserHostedStatus(t, response, http.StatusSeeOther)
	var organization, role string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT provider_id FROM hosted_tenant").Scan(&organization); err != nil {
		t.Fatal(err)
	}
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT role FROM hosted_members WHERE user_id = 'user_browser_owner'").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if organization != "org_browser_provider" || role != "owner" || response.Header().Get("Location") != "/auth/oidc/start" {
		t.Fatalf("organization creation = organization %q, role %q, location %q", organization, role, response.Header().Get("Location"))
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	base, err := url.Parse(f.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(base, []*http.Cookie{f.cookies["owner"]})
	client := f.server.Client()
	client.Jar = jar
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, f.server.URL+"/auth/oidc/start", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(result.Body)
	closeErr := result.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read sign-in response: %v; close: %v", readErr, closeErr)
	}
	if result.StatusCode != http.StatusOK || result.Request.URL.Path != "/organization" || !strings.Contains(string(body), "New browser organization") || !strings.Contains(string(body), `action="/projects"`) {
		t.Fatalf("first sign-in after organization creation failed: status %d, path %s, body %s", result.StatusCode, result.Request.URL.Path, body)
	}
	for _, cookie := range jar.Cookies(base) {
		if cookie.Name == hostedCookie {
			f.cookies["owner"] = cookie
		}
	}
	project := f.createProject(t, "First browser project")
	response = f.page(t, "owner", "/projects/"+project)
	browserHostedStatus(t, response, http.StatusOK)
	if !strings.Contains(response.Body.String(), "No work items in this project yet") {
		t.Error("new project did not open its empty work list")
	}
}

func TestHostedBrowserPreview(t *testing.T) {
	if os.Getenv("DETENT_HOSTED_BROWSER_PREVIEW") == "" {
		t.Skip("set DETENT_HOSTED_BROWSER_PREVIEW=1 to run the isolated browser preview")
	}
	f := newBrowserHostedFixture(t, true)
	accounts := make(map[string]string, len(f.cookies))
	for account := range f.cookies {
		accounts[account] = f.server.URL + "/__preview/account/" + account
	}
	fixture := struct {
		URL            string            `json:"url"`
		Login          string            `json:"login"`
		Organization   string            `json:"organization"`
		Project        string            `json:"project"`
		PrivateProject string            `json:"private_project"`
		Accounts       map[string]string `json:"accounts"`
		Stop           string            `json:"stop"`
		Expires        time.Time         `json:"expires"`
	}{f.server.URL, f.server.URL + "/login", f.server.URL + "/organization", f.server.URL + "/projects/" + f.project, f.server.URL + "/projects/" + f.privateProject, accounts, f.server.URL + "/__preview/stop", time.Now().Add(5 * time.Minute)}
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "hosted-browser-preview.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("Hosted browser fixture: %s", path)
	t.Logf("Hosted browser URL: %s", f.server.URL)
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	select {
	case <-f.stop:
	case <-timer.C:
	case <-t.Context().Done():
	}
}
