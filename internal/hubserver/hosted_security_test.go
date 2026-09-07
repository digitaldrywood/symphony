package hubserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type hostedSecurityProvider struct {
	mu              sync.Mutex
	sessions        map[string]auth.HostedIdentity
	members         map[string]auth.Membership
	codes           map[string]auth.Identity
	invitations     map[string]auth.Invitation
	invitationRoles map[string]string
	exchanges       int
}

func newHostedSecurityProvider() *hostedSecurityProvider {
	return &hostedSecurityProvider{
		sessions:        make(map[string]auth.HostedIdentity),
		members:         make(map[string]auth.Membership),
		codes:           make(map[string]auth.Identity),
		invitations:     make(map[string]auth.Invitation),
		invitationRoles: make(map[string]string),
	}
}

func (p *hostedSecurityProvider) AuthorizationURL(state, nonce, verifier string) string {
	return "https://identity.example.test/authorize?" + url.Values{"state": {state}, "nonce": {nonce}, "code_challenge": {verifier}}.Encode()
}

func (p *hostedSecurityProvider) Exchange(_ context.Context, code, _, _ string) (auth.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exchanges++
	identity, ok := p.codes[code]
	if !ok {
		return auth.Identity{}, auth.ErrHostedIdentity
	}
	delete(p.codes, code)
	return identity, nil
}

func (p *hostedSecurityProvider) CurrentSession(_ context.Context, identity auth.HostedIdentity) (auth.HostedIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.sessions[identity.SessionID]
	if !ok {
		return auth.HostedIdentity{}, auth.ErrHostedIdentity
	}
	return current, nil
}

func (p *hostedSecurityProvider) Memberships(_ context.Context, user, organization string) ([]auth.Membership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var members []auth.Membership
	for _, membership := range p.members {
		if (user == "" || membership.UserID == user) && (organization == "" || membership.OrganizationID == organization) {
			members = append(members, membership)
		}
	}
	return members, nil
}

func (p *hostedSecurityProvider) Organization(_ context.Context, id string) (auth.Organization, error) {
	return auth.Organization{ID: id, ExternalID: "org_security", Name: "Organization"}, nil
}

func (p *hostedSecurityProvider) CreateOrganization(_ context.Context, externalID, name string) (auth.Organization, error) {
	return auth.Organization{ID: "org_provider", ExternalID: externalID, Name: name}, nil
}

func (p *hostedSecurityProvider) CreateMembership(_ context.Context, user, organization, role string) (auth.Membership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	member := auth.Membership{ID: "membership_" + user, UserID: user, OrganizationID: organization, Status: "active"}
	member.Role.Slug = role
	p.members[member.ID] = member
	return member, nil
}

func (p *hostedSecurityProvider) SetMembershipRole(_ context.Context, id, role string) error {
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

func (p *hostedSecurityProvider) RevokeMembership(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	member, ok := p.members[id]
	if !ok {
		return auth.ErrHostedIdentity
	}
	member.Status = "inactive"
	p.members[id] = member
	return nil
}

func (p *hostedSecurityProvider) Invite(_ context.Context, organization, email, role, _ string) (auth.Invitation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	invitation := auth.Invitation{ID: fmt.Sprintf("invitation_%d", len(p.invitations)+1), Email: email, OrganizationID: organization, State: "pending", ExpiresAt: time.Now().Add(time.Hour)}
	p.invitations[invitation.ID] = invitation
	p.invitationRoles[invitation.ID] = role
	return invitation, nil
}

func (p *hostedSecurityProvider) Invitation(_ context.Context, token string) (auth.Invitation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	invitation, ok := p.invitations[token]
	if !ok {
		return auth.Invitation{}, auth.ErrHostedIdentity
	}
	return invitation, nil
}

func (p *hostedSecurityProvider) AcceptInvitation(_ context.Context, token, user string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	invitation, ok := p.invitations[token]
	if !ok || invitation.State != "pending" {
		return auth.ErrHostedIdentity
	}
	invitation.State, invitation.AcceptedUserID = "accepted", user
	p.invitations[token] = invitation
	member := auth.Membership{ID: "membership_" + user, UserID: user, OrganizationID: invitation.OrganizationID, Status: "active"}
	member.Role.Slug = p.invitationRoles[token]
	p.members[member.ID] = member
	return nil
}

func (p *hostedSecurityProvider) RevokeSession(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, id)
	return nil
}

type hostedSecurityUser struct {
	identity auth.Identity
	token    string
}

type hostedSecurityFixture struct {
	service  *Service
	provider *hostedSecurityProvider
	project  tracker.ProjectID
	base     string
}

var hostedMigratedDatabase struct {
	once     sync.Once
	contents []byte
	err      error
}

func hostedTestDatabasePath(t *testing.T) string {
	t.Helper()
	hostedMigratedDatabase.once.Do(func() {
		path := filepath.Join(t.TempDir(), "schema.db")
		db, err := openDatabase(t.Context(), Config{DatabasePath: path, Logger: discardLogger()}.normalized())
		if err != nil {
			hostedMigratedDatabase.err = err
			return
		}
		if err := db.Close(); err != nil {
			hostedMigratedDatabase.err = err
			return
		}
		hostedMigratedDatabase.contents, hostedMigratedDatabase.err = os.ReadFile(path)
	})
	if hostedMigratedDatabase.err != nil {
		t.Fatalf("build hosted schema fixture: %v", hostedMigratedDatabase.err)
	}
	path := filepath.Join(t.TempDir(), "hosted.db")
	if err := os.WriteFile(path, hostedMigratedDatabase.contents, 0o600); err != nil {
		t.Fatalf("write hosted schema fixture: %v", err)
	}
	return path
}

func TestHostedSchemaFixtureTenantIsolation(t *testing.T) {
	t.Parallel()
	for _, organization := range []string{"org_first", "org_second", "org_third"} {
		t.Run(organization, func(t *testing.T) {
			t.Parallel()
			service := openTestService(t, Config{
				DatabasePath: hostedTestDatabasePath(t),
				Hosted: &HostedConfig{
					OrganizationID:       organization,
					WorkOSOrganizationID: "provider_" + organization,
					PublicURL:            "https://" + organization + ".example.test",
					Provider:             newHostedSecurityProvider(),
				},
			})
			if service.database.schemaVersion != supportedSchemaVersion {
				t.Fatalf("schema version = %d, want %d", service.database.schemaVersion, supportedSchemaVersion)
			}
			var boundOrganization string
			if err := service.database.db.QueryRowContext(t.Context(), "SELECT organization_id FROM hosted_tenant").Scan(&boundOrganization); err != nil {
				t.Fatal(err)
			}
			if boundOrganization != organization {
				t.Fatalf("tenant binding = %q, want %q", boundOrganization, organization)
			}
			var sessions int
			if err := service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_sessions").Scan(&sessions); err != nil {
				t.Fatal(err)
			}
			if sessions != 0 {
				t.Fatalf("new fixture contains %d sessions", sessions)
			}
			if _, err := service.database.db.ExecContext(t.Context(), "INSERT INTO hosted_sessions (token_hash,email,identity_json,expires_at,created_at) VALUES ('same-token','fixture@example.test','{}',?,?)", testTimestamp, testTimestamp); err != nil {
				t.Fatal(err)
			}
			if _, err := service.database.db.ExecContext(t.Context(), "CREATE TABLE fixture_probe (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatalf("fixture schema was shared: %v", err)
			}
			if err := service.database.health(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newHostedSecurityFixture(t *testing.T) hostedSecurityFixture {
	t.Helper()
	provider := newHostedSecurityProvider()
	service := openTestService(t, Config{
		DatabasePath:   hostedTestDatabasePath(t),
		GitHubDisabled: true,
		Hosted: &HostedConfig{
			OrganizationID:       "org_security",
			WorkOSOrganizationID: "org_provider",
			BootstrapSubject:     "user_owner",
			PublicURL:            "http://127.0.0.1:7777",
			StaffEmails:          []string{"staff@example.test", "support@example.test"},
			SupportActors:        []string{"support@example.test"},
			Provider:             provider,
		},
	})
	states := []tracker.NativeState{{Name: "Todo", Dispatchable: true, Transitions: []string{"Done"}}, {Name: "Done", Terminal: true, Transitions: []string{"Todo"}}}
	raw, err := json.Marshal(states)
	if err != nil {
		t.Fatal(err)
	}
	project := tracker.ProjectID("prj_security")
	now := formatHubTime(time.Now())
	if _, err := service.database.db.ExecContext(t.Context(), "INSERT INTO projects(id,organization_id,name,profile,states_json,created_at,github_repository_enabled) VALUES (?,?,'private-project-sentinel','native',?,?,0)", project, service.config.Hosted.OrganizationID, string(raw), now); err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if _, err := service.database.db.ExecContext(t.Context(), "INSERT INTO workflow_states(project_id,source_name,detent_state,terminal,dispatchable,created_at,updated_at) VALUES (?,?,?,?,?,?,?)", project, state.Name, state.Name, state.Terminal, state.Dispatchable, now, now); err != nil {
			t.Fatal(err)
		}
	}
	return hostedSecurityFixture{service: service, provider: provider, project: project, base: "/api/v2/organizations/org_security/projects/" + string(project)}
}

func (f hostedSecurityFixture) user(t *testing.T, name, role, email, grant, supportActor string) hostedSecurityUser {
	t.Helper()
	now := time.Now().UTC()
	identity := auth.Identity{Subject: "user_" + name, Email: email, EmailVerified: true, Hosted: &auth.HostedIdentity{
		Subject: "user_" + name, OrganizationID: "org_provider", SessionID: "session_" + name,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), SupportActor: supportActor,
	}}
	if supportActor != "" {
		identity.Hosted.SupportReason = "customer-request"
	}
	f.provider.mu.Lock()
	f.provider.sessions[identity.Hosted.SessionID] = *identity.Hosted
	f.provider.mu.Unlock()
	membership, err := f.provider.CreateMembership(t.Context(), identity.Subject, identity.Hosted.OrganizationID, role)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.addHostedMember(t.Context(), identity, membership); err != nil {
		t.Fatal(err)
	}
	token, _, err := f.service.hostedSessions.CreateIdentitySession(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	user := hostedSecurityUser{identity: identity, token: token}
	if grant != "" {
		f.grant(t, user, grant == "write", false)
	}
	return user
}

func (f hostedSecurityFixture) grant(t *testing.T, user hostedSecurityUser, write, runner bool) {
	t.Helper()
	var principal string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT principal_id FROM hosted_members WHERE user_id = ?", user.identity.Subject).Scan(&principal); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO hosted_project_grants(user_id,organization_id,project_id,can_write,manage_runner) VALUES (?,'org_security',?,?,?) ON CONFLICT(user_id,project_id) DO UPDATE SET can_write=excluded.can_write,manage_runner=excluded.manage_runner", user.identity.Subject, f.project, write, runner); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES (?,'org_security',?) ON CONFLICT DO NOTHING", principal, f.project); err != nil {
		t.Fatal(err)
	}
}

func (f hostedSecurityFixture) seedIssue(t *testing.T, number int) tracker.NativeWorkItemID {
	t.Helper()
	var id string
	now := formatHubTime(time.Now())
	err := f.service.database.db.QueryRowContext(t.Context(), `
INSERT INTO issues (organization_id,project_id,number,title,body,url,github_state,source_version,source_updated_at,synchronized_at,created_at,updated_at,workflow_state_id)
VALUES ('org_security',?,?,'private-issue-sentinel','private-body-sentinel','','open','1',?,?,?,?,(SELECT id FROM workflow_states WHERE project_id = ? AND source_name = 'Todo'))
RETURNING native_id`, f.project, number, now, now, now, now, f.project).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return tracker.NativeWorkItemID(id)
}

func (f hostedSecurityFixture) request(t *testing.T, user hostedSecurityUser, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	contentType := ""
	switch value := body.(type) {
	case nil:
	case url.Values:
		reader = strings.NewReader(value.Encode())
		contentType = "application/x-www-form-urlencoded"
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		reader = strings.NewReader(string(raw))
		contentType = "application/json"
	}
	request := httptest.NewRequest(method, path, reader)
	if user.token != "" {
		request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
		request.Header.Set("X-CSRF-Token", hostedCSRF(user.token))
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	return response
}

func TestHostedSecurityRoleProjectMatrix(t *testing.T) {
	t.Parallel()
	for _, role := range []string{"owner", "admin", "member", "viewer"} {
		for _, grant := range []string{"", "read", "write"} {
			t.Run(role+"/"+grant, func(t *testing.T) {
				t.Parallel()
				f := newHostedSecurityFixture(t)
				user := f.user(t, role, role, role+"@example.test", grant, "")
				readStatus := http.StatusNotFound
				if grant != "" {
					readStatus = http.StatusOK
				}
				requireNativeStatus(t, f.request(t, user, http.MethodGet, f.base, nil), readStatus)
				writeStatus := http.StatusNotFound
				if grant == "write" && role != "viewer" {
					writeStatus = http.StatusOK
				}
				command := tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "create"}, Title: "new issue", Body: "private-body-sentinel", State: "Todo"}
				requireNativeStatus(t, f.request(t, user, http.MethodPost, f.base+"/work-items", command), writeStatus)
				requireNativeStatus(t, f.request(t, user, http.MethodPost, f.base+"/claims", map[string]any{}), http.StatusForbidden)
				requireNativeStatus(t, f.request(t, user, http.MethodGet, "/api/v2/organizations/org_security/runners", nil), http.StatusNotFound)
			})
		}
	}
}

func TestHostedSecurityStaffMetadataBoundary(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	f.seedIssue(t, 1)
	staff := f.user(t, "staff", "owner", "staff@example.test", "write", "")
	customer := f.user(t, "customer", "owner", "customer@example.test", "write", "")
	for _, test := range []struct {
		name   string
		path   string
		user   hostedSecurityUser
		bearer string
		want   int
	}{
		{name: "staff report", path: "/api/cloud/metadata", user: staff, want: http.StatusOK},
		{name: "reporting bearer", path: "/api/cloud/metadata", bearer: testHubAdminToken, want: http.StatusOK},
		{name: "customer report", path: "/api/cloud/metadata", user: customer, want: http.StatusForbidden},
		{name: "staff native content", path: f.base + "/work-items", user: staff, want: http.StatusForbidden},
		{name: "staff project page", path: "/projects/" + string(f.project), user: staff, want: http.StatusForbidden},
		{name: "bootstrap native content", path: f.base + "/work-items", bearer: testHubAdminToken, want: http.StatusNotFound},
		{name: "staff legacy content", path: "/api/v1/work-items", user: staff, want: http.StatusNotFound},
		{name: "legacy health", path: "/health", bearer: testHubAdminToken, want: http.StatusNotFound},
		{name: "staff global tokens", path: "/api/v2/organizations", user: staff, want: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.user.token != "" {
				request.AddCookie(&http.Cookie{Name: hostedCookie, Value: test.user.token})
			}
			if test.bearer != "" {
				request.Header.Set("Authorization", "Bearer "+test.bearer)
			}
			response := httptest.NewRecorder()
			f.service.Handler().ServeHTTP(response, request)
			requireNativeStatus(t, response, test.want)
			for _, sentinel := range []string{"private-project-sentinel", "private-issue-sentinel", "private-body-sentinel", testHubAdminToken} {
				if strings.Contains(response.Body.String(), sentinel) {
					t.Fatalf("response exposed %q", sentinel)
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("hosted response permits caching")
			}
		})
	}
	requireNativeStatus(t, f.request(t, staff, http.MethodPost, "/support/start", url.Values{}), http.StatusForbidden)
}

func TestHostedSecurityRunnerPermissionsAreSeparate(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		role   string
		runner bool
		want   int
	}{
		{role: "owner", want: http.StatusNotFound},
		{role: "admin", want: http.StatusNotFound},
		{role: "member", runner: true, want: http.StatusOK},
		{role: "viewer", runner: true, want: http.StatusNotFound},
	} {
		t.Run(test.role, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, test.role, test.role, test.role+"@example.test", "read", "")
			f.grant(t, user, false, test.runner)
			requireNativeStatus(t, f.request(t, user, http.MethodGet, "/api/v2/organizations/org_security/runners", nil), test.want)
			requireNativeStatus(t, f.request(t, user, http.MethodPost, f.base+"/work-items/wi_unknown/change-requests/change_unknown/versions/version_unknown/checks", map[string]any{}), http.StatusNotFound)
		})
	}
}

func TestHostedSecuritySupportSessionConstraints(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		role   string
		grant  string
		change func(*testing.T, hostedSecurityFixture, hostedSecurityUser)
		path   string
		want   int
	}{
		{name: "effective project read", role: "member", grant: "read", want: http.StatusOK},
		{name: "effective project write", role: "member", grant: "write", want: http.StatusOK},
		{name: "no effective project grant", role: "owner", want: http.StatusNotFound},
		{name: "viewer read", role: "viewer", grant: "write", want: http.StatusOK},
		{name: "wrong organization", role: "member", grant: "write", path: "/api/v2/organizations/org_other/projects/prj_security", want: http.StatusNotFound},
		{name: "expired provider session", role: "member", grant: "write", change: func(t *testing.T, f hostedSecurityFixture, u hostedSecurityUser) {
			f.provider.mu.Lock()
			identity := f.provider.sessions[u.identity.Hosted.SessionID]
			identity.ExpiresAt = time.Now().Add(-time.Minute)
			f.provider.sessions[identity.SessionID] = identity
			f.provider.mu.Unlock()
		}, want: http.StatusUnauthorized},
		{name: "revoked provider session", role: "member", grant: "write", change: func(t *testing.T, f hostedSecurityFixture, u hostedSecurityUser) {
			if err := f.provider.RevokeSession(t.Context(), u.identity.Hosted.SessionID); err != nil {
				t.Fatal(err)
			}
		}, want: http.StatusUnauthorized},
		{name: "removed membership", role: "member", grant: "write", change: func(t *testing.T, f hostedSecurityFixture, u hostedSecurityUser) {
			if err := f.provider.RevokeMembership(t.Context(), "membership_"+u.identity.Subject); err != nil {
				t.Fatal(err)
			}
		}, want: http.StatusForbidden},
		{name: "revoked local session", role: "member", grant: "write", change: func(t *testing.T, f hostedSecurityFixture, u hostedSecurityUser) {
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE hosted_sessions SET revoked_at = ? WHERE token_hash = ?", formatHubTime(time.Now()), apikey.HashToken(u.token)); err != nil {
				t.Fatal(err)
			}
		}, want: http.StatusUnauthorized},
		{name: "changed support actor", role: "member", grant: "write", change: func(t *testing.T, f hostedSecurityFixture, u hostedSecurityUser) {
			f.provider.mu.Lock()
			identity := f.provider.sessions[u.identity.Hosted.SessionID]
			identity.SupportActor = "staff@example.test"
			f.provider.sessions[identity.SessionID] = identity
			f.provider.mu.Unlock()
		}, want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, "customer", test.role, "customer@example.test", test.grant, "support@example.test")
			if test.change != nil {
				test.change(t, f, user)
			}
			path := test.path
			if path == "" {
				path = f.base
			}
			requireNativeStatus(t, f.request(t, user, http.MethodGet, path, nil), test.want)
			if test.want == http.StatusOK {
				command := tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "support-write"}, Title: "private-body-sentinel", State: "Todo"}
				wantWrite := http.StatusNotFound
				if test.role != "viewer" && test.grant == "write" {
					wantWrite = http.StatusOK
				}
				requireNativeStatus(t, f.request(t, user, http.MethodPost, f.base+"/work-items", command), wantWrite)
				requireNativeStatus(t, f.request(t, user, http.MethodGet, "/api/cloud/metadata", nil), http.StatusForbidden)
				var actor, effective, organization, reason, raw string
				if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT actual_actor,effective_user,organization_id,reason,event || route || project_id FROM hosted_audit WHERE session_id = ? ORDER BY id LIMIT 1", user.identity.Hosted.SessionID).Scan(&actor, &effective, &organization, &reason, &raw); err != nil {
					t.Fatal(err)
				}
				if actor != "support@example.test" || effective != user.identity.Subject || organization != "org_security" || reason != "customer-request" || strings.Contains(raw, "private-body-sentinel") || strings.Contains(raw, user.token) {
					t.Fatalf("audit actor=%q effective=%q organization=%q reason=%q", actor, effective, organization, reason)
				}
			}
		})
	}
}

func TestHostedSecurityReplayAndCursorRevocation(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	writer := f.user(t, "writer", "member", "writer@example.test", "write", "")
	reader := f.user(t, "reader", "viewer", "reader@example.test", "read", "")
	f.seedIssue(t, 1)
	f.seedIssue(t, 2)
	command := tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "cached-command"}, Title: "cached-private-sentinel", State: "Todo"}
	requireNativeStatus(t, f.request(t, writer, http.MethodPost, f.base+"/work-items", command), http.StatusOK)
	response := f.request(t, writer, http.MethodGet, f.base+"/work-items?limit=1", nil)
	requireNativeStatus(t, response, http.StatusOK)
	var page tracker.Page[tracker.NativeIssue]
	decodeHubResponse(t, response, &page)
	if page.NextCursor == "" {
		t.Fatal("missing cursor")
	}
	requireNativeStatus(t, f.request(t, reader, http.MethodGet, f.base+"/work-items?limit=1&cursor="+url.QueryEscape(page.NextCursor), nil), http.StatusUnprocessableEntity)
	secondIdentity := writer.identity
	secondHosted := *writer.identity.Hosted
	secondHosted.SessionID = "session_writer_second"
	secondIdentity.Hosted = &secondHosted
	f.provider.mu.Lock()
	f.provider.sessions[secondHosted.SessionID] = secondHosted
	f.provider.mu.Unlock()
	secondToken, _, err := f.service.hostedSessions.CreateIdentitySession(t.Context(), secondIdentity)
	if err != nil {
		t.Fatal(err)
	}
	secondSession := hostedSecurityUser{identity: secondIdentity, token: secondToken}
	requireNativeStatus(t, f.request(t, secondSession, http.MethodGet, f.base+"/work-items?limit=1&cursor="+url.QueryEscape(page.NextCursor), nil), http.StatusUnprocessableEntity)
	if _, err := f.service.database.db.ExecContext(t.Context(), "DELETE FROM hosted_project_grants WHERE user_id = ?", writer.identity.Subject); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method, path string
		body         any
	}{
		{method: http.MethodPost, path: f.base + "/work-items", body: command},
		{method: http.MethodGet, path: f.base + "/work-items?limit=1&cursor=" + url.QueryEscape(page.NextCursor)},
	} {
		response := f.request(t, writer, test.method, test.path, test.body)
		requireNativeStatus(t, response, http.StatusNotFound)
		if strings.Contains(response.Body.String(), "cached-private-sentinel") {
			t.Fatal("revoked grant exposed cached response")
		}
	}
}

func TestHostedSecurityCookieMutationsRequireCSRF(t *testing.T) {
	t.Parallel()
	for _, authorization := range []string{"", "Bearer invalid"} {
		t.Run(authorization, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, "owner", "owner", "owner@example.test", "", "")
			request := httptest.NewRequest(http.MethodPost, "/projects", strings.NewReader(url.Values{"name": {"Rejected"}, "grant_access": {"true"}}.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.Header.Set("Authorization", authorization)
			request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
			response := httptest.NewRecorder()
			f.service.Handler().ServeHTTP(response, request)
			requireNativeStatus(t, response, http.StatusForbidden)
		})
	}
}

func TestHostedSecurityAdministrationRejectsBearerCookieMix(t *testing.T) {
	t.Parallel()
	for _, authorization := range []string{"Bearer invalid", "Bearer " + testHubAdminToken} {
		for _, route := range []struct {
			method string
			path   string
			body   string
		}{
			{method: http.MethodPost, path: "/api/v2/organizations/org_security/projects", body: `{"idempotency_key":"mixed-auth","name":"Unauthorized project","states":[{"name":"Todo","dispatchable":true,"transitions":["Done"]},{"name":"Done","terminal":true,"transitions":["Todo"]}]}`},
			{method: http.MethodPost, path: "/api/v2/organizations/org_security/runner-enrollments", body: `{}`},
			{method: http.MethodGet, path: "/api/v2/organizations/org_security/runners"},
			{method: http.MethodPut, path: "/api/v2/organizations/org_security/runners/runner_unknown/routing", body: `{}`},
		} {
			t.Run(authorization+"/"+route.method+route.path, func(t *testing.T) {
				t.Parallel()
				f := newHostedSecurityFixture(t)
				user := f.user(t, "owner", "owner", "owner@example.test", "write", "")
				f.grant(t, user, true, true)
				request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", authorization)
				request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
				response := httptest.NewRecorder()
				f.service.Handler().ServeHTTP(response, request)
				requireNativeStatus(t, response, http.StatusNotFound)
				var count int
				if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM projects").Scan(&count); err != nil || count != 1 {
					t.Fatalf("mixed authentication changed projects: count=%d, error=%v", count, err)
				}
			})
		}
	}
}

func TestHostedSecurityReplacementMembershipRequiresNewGrants(t *testing.T) {
	t.Parallel()
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%v", replacement), func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, "member", "member", "member@example.test", "write", "")
			f.grant(t, user, true, true)
			f.provider.mu.Lock()
			membership := f.provider.members["membership_"+user.identity.Subject]
			if replacement {
				delete(f.provider.members, membership.ID)
				membership.ID += "_replacement"
				f.provider.members[membership.ID] = membership
			}
			f.provider.mu.Unlock()
			if err := f.service.addHostedMember(t.Context(), user.identity, membership); err != nil {
				t.Fatal(err)
			}
			want := http.StatusOK
			if replacement {
				want = http.StatusNotFound
			}
			requireNativeStatus(t, f.request(t, user, http.MethodGet, f.base, nil), want)
			requireNativeStatus(t, f.request(t, user, http.MethodGet, "/api/v2/organizations/org_security/runners", nil), want)
			if replacement {
				f.grant(t, user, false, false)
				requireNativeStatus(t, f.request(t, user, http.MethodGet, f.base, nil), http.StatusOK)
				requireNativeStatus(t, f.request(t, user, http.MethodGet, "/api/v2/organizations/org_security/runners", nil), http.StatusNotFound)
			}
		})
	}
}

func TestHostedSecuritySSEAudit(t *testing.T) {
	t.Parallel()
	for _, actor := range []string{"", "support@example.test"} {
		t.Run(actor, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			f.seedIssue(t, 1)
			user := f.user(t, "viewer", "viewer", "viewer@example.test", "read", actor)
			server := httptest.NewServer(f.service.Handler())
			t.Cleanup(server.Close)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/projects/"+string(f.project)+"/events", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			scanner := bufio.NewScanner(response.Body)
			if response.StatusCode != http.StatusOK || !scanner.Scan() || scanner.Text() != "event: activity" {
				t.Fatalf("stream status=%d, error=%v", response.StatusCode, scanner.Err())
			}
			var actual, effective, organization, project, reason string
			var count int
			err = f.service.database.db.QueryRowContext(t.Context(), "SELECT actual_actor,effective_user,organization_id,project_id,reason,count(*) FROM hosted_audit WHERE session_id = ? AND route = ? GROUP BY actual_actor,effective_user,organization_id,project_id,reason", user.identity.Hosted.SessionID, "GET /projects/:project/events").Scan(&actual, &effective, &organization, &project, &reason, &count)
			if err != nil {
				t.Fatal(err)
			}
			wantActor := actor
			if wantActor == "" {
				wantActor = user.identity.Subject
			}
			if actual != wantActor || effective != user.identity.Subject || organization != "org_security" || project != string(f.project) || reason != user.identity.Hosted.SupportReason || count != 1 {
				t.Fatalf("stream audit actor=%q effective=%q organization=%q project=%q reason=%q count=%d", actual, effective, organization, project, reason, count)
			}
			var record string
			if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT actual_actor || effective_user || organization_id || project_id || reason || event || route FROM hosted_audit WHERE session_id = ?", user.identity.Hosted.SessionID).Scan(&record); err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{user.token, "private-project-sentinel", "private-issue-sentinel", "private-body-sentinel"} {
				if strings.Contains(record, forbidden) {
					t.Fatalf("stream audit exposed %q", forbidden)
				}
			}
		})
	}
}

func TestHostedSecuritySSERevocation(t *testing.T) {
	t.Parallel()
	for _, revocation := range []string{"provider session", "membership", "project grant"} {
		t.Run(revocation, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, "viewer", "viewer", "viewer@example.test", "read", "")
			server := httptest.NewServer(f.service.Handler())
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/projects/"+string(f.project)+"/events", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
				t.Fatalf("stream response = %d %s", response.StatusCode, response.Header.Get("Content-Type"))
			}
			scanner := bufio.NewScanner(response.Body)
			if !scanner.Scan() || scanner.Text() != "event: activity" || !scanner.Scan() || scanner.Text() != "data: 0" || !scanner.Scan() || scanner.Text() != "" {
				t.Fatalf("initial event is invalid: %v", scanner.Err())
			}
			switch revocation {
			case "provider session":
				if err := f.provider.RevokeSession(t.Context(), user.identity.Hosted.SessionID); err != nil {
					t.Fatal(err)
				}
			case "membership":
				if err := f.provider.RevokeMembership(t.Context(), "membership_"+user.identity.Subject); err != nil {
					t.Fatal(err)
				}
			case "project grant":
				if _, err := f.service.database.db.ExecContext(t.Context(), "DELETE FROM hosted_project_grants WHERE user_id = ?", user.identity.Subject); err != nil {
					t.Fatal(err)
				}
			}
			if scanner.Scan() {
				t.Fatalf("revoked stream produced %q", scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("stream did not close cleanly: %v", err)
			}
		})
	}
}

func TestHostedSecuritySSEDeniesUnscopedProject(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	user := f.user(t, "viewer", "viewer", "viewer@example.test", "", "")
	response := f.request(t, user, http.MethodGet, "/projects/"+string(f.project)+"/events", nil)
	if response.Code != http.StatusForbidden && response.Code != http.StatusNotFound {
		t.Fatalf("ungranted project stream returned %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "event:") {
		t.Fatal("ungranted project emitted an event")
	}
}

func TestHostedSecurityLoginTransactions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, start, email string
		wrongState         bool
		want               int
	}{
		{name: "customer provider organization", start: "/auth/oidc/start", email: "owner@example.test", want: http.StatusSeeOther},
		{name: "staff without organization", start: "/auth/oidc/start?staff=1", email: "staff@example.test", want: http.StatusSeeOther},
		{name: "wrong callback state", start: "/auth/oidc/start", email: "owner@example.test", wrongState: true, want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			user := f.user(t, "owner", "owner", test.email, "", "")
			f.provider.mu.Lock()
			f.provider.codes["code"] = user.identity
			f.provider.mu.Unlock()
			start := f.request(t, hostedSecurityUser{}, http.MethodGet, test.start, nil)
			requireNativeStatus(t, start, http.StatusSeeOther)
			location, err := url.Parse(start.Header().Get("Location"))
			if err != nil {
				t.Fatal(err)
			}
			state := location.Query().Get("state")
			if state == "" {
				t.Fatal("missing login state")
			}
			if test.wrongState {
				state = "wrong-state"
			}
			var cookie *http.Cookie
			for _, current := range start.Result().Cookies() {
				if current.Name == hostedTransactionCookie {
					cookie = current
				}
			}
			if cookie == nil {
				t.Fatal("missing transaction cookie")
			}
			path := "/auth/oidc/callback?" + url.Values{"state": {state}, "code": {"code"}}.Encode()
			for attempt := range 2 {
				request := httptest.NewRequest(http.MethodGet, path, nil)
				request.AddCookie(cookie)
				response := httptest.NewRecorder()
				f.service.Handler().ServeHTTP(response, request)
				want := test.want
				if attempt == 1 {
					want = http.StatusUnauthorized
				}
				requireNativeStatus(t, response, want)
			}
			f.provider.mu.Lock()
			exchanges := f.provider.exchanges
			f.provider.mu.Unlock()
			wantExchanges := 1
			if test.wrongState {
				wantExchanges = 0
			}
			if exchanges != wantExchanges {
				t.Fatalf("provider exchanges = %d, want %d", exchanges, wantExchanges)
			}
		})
	}
}

var _ auth.HostedProvider = (*hostedSecurityProvider)(nil)

func TestHostedSecurityRoleRevocationAppliesImmediately(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	user := f.user(t, "member", "member", "member@example.test", "write", "")
	command := tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "before-role-change"}, Title: "private-mutation-sentinel", State: "Todo"}
	response := f.request(t, user, http.MethodPost, f.base+"/work-items", command)
	requireNativeStatus(t, response, http.StatusOK)
	replay := f.request(t, user, http.MethodPost, f.base+"/work-items", command)
	requireNativeStatus(t, replay, http.StatusOK)
	if response.Body.String() != replay.Body.String() {
		t.Fatal("idempotent replay changed the mutation response")
	}
	if err := f.provider.SetMembershipRole(t.Context(), "membership_"+user.identity.Subject, "viewer"); err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, f.request(t, user, http.MethodGet, f.base, nil), http.StatusOK)
	response = f.request(t, user, http.MethodPost, f.base+"/work-items", command)
	requireNativeStatus(t, response, http.StatusNotFound)
	if strings.Contains(response.Body.String(), "private-mutation-sentinel") {
		t.Fatal("role downgrade exposed cached mutation")
	}
}

func TestHostedSecuritySupportLoginAndExit(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	staff := f.user(t, "support", "owner", "support@example.test", "", "")
	customer := f.user(t, "customer", "viewer", "customer@example.test", "read", "support@example.test")
	start := f.request(t, staff, http.MethodPost, "/support/start", url.Values{})
	requireNativeStatus(t, start, http.StatusOK)
	var transaction *http.Cookie
	for _, cookie := range start.Result().Cookies() {
		if cookie.Name == hostedTransactionCookie {
			transaction = cookie
		}
	}
	if transaction == nil {
		t.Fatal("support login transaction cookie was not set")
	}
	f.provider.mu.Lock()
	f.provider.codes["support-code"] = customer.identity
	f.provider.mu.Unlock()
	request := httptest.NewRequest(http.MethodGet, "/auth/oidc/callback?code=support-code", nil)
	request.AddCookie(transaction)
	request.AddCookie(&http.Cookie{Name: hostedCookie, Value: staff.token})
	callback := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(callback, request)
	requireNativeStatus(t, callback, http.StatusSeeOther)
	var sessionCookie *http.Cookie
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == hostedCookie {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil {
		t.Fatal("support session cookie was not set")
	}
	support := hostedSecurityUser{identity: customer.identity, token: sessionCookie.Value}
	page := f.request(t, support, http.MethodGet, "/projects/"+string(f.project), nil)
	requireNativeStatus(t, page, http.StatusOK)
	if !strings.Contains(page.Body.String(), "support@example.test") || !strings.Contains(page.Body.String(), "/logout") {
		t.Fatal("support indicator or exit flow is missing")
	}
	requireNativeStatus(t, f.request(t, support, http.MethodPost, "/logout", url.Values{}), http.StatusSeeOther)
	requireNativeStatus(t, f.request(t, support, http.MethodGet, f.base, nil), http.StatusUnauthorized)
	if _, err := f.provider.CurrentSession(t.Context(), *customer.identity.Hosted); !errors.Is(err, auth.ErrHostedIdentity) {
		t.Fatalf("provider support session was not revoked: %v", err)
	}
	var starts, ends int
	err := f.service.database.db.QueryRowContext(t.Context(), "SELECT sum(event = 'session_started'),sum(event = 'session_ended') FROM hosted_audit WHERE session_id = ? AND actual_actor = ? AND effective_user = ?", customer.identity.Hosted.SessionID, "support@example.test", customer.identity.Subject).Scan(&starts, &ends)
	if err != nil || starts != 1 || ends != 1 {
		t.Fatalf("support lifecycle audit starts=%d ends=%d error=%v", starts, ends, err)
	}
}

func TestHostedSecurityLogsExcludeCustomerContent(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	provider := newHostedSecurityProvider()
	service, err := Open(t.Context(), Config{
		DatabasePath:   filepath.Join(t.TempDir(), "hub.db"),
		GitHubDisabled: true,
		Logger:         slog.New(slog.NewTextHandler(&logs, nil)),
		Hosted:         &HostedConfig{OrganizationID: "org_logs", WorkOSOrganizationID: "org_provider", PublicURL: "https://logs.example.test", Provider: provider},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	const sentinel = "private-content-bearer-sentinel"
	service.echo.GET("/security/error", func(c echo.Context) error {
		service.config.Logger.Error(sentinel, "credential", sentinel, "error", errors.New(sentinel))
		return service.nativeAPIError(c, errors.New(sentinel))
	})
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/security/error", nil))
	requireNativeStatus(t, response, http.StatusInternalServerError)
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if logs.Len() == 0 {
		t.Fatal("test produced no log records")
	}
	if strings.Contains(logs.String(), sentinel) || strings.Contains(response.Body.String(), sentinel) {
		t.Fatal("hosted logs or errors exposed customer content")
	}
}
