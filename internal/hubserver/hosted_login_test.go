package hubserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/digitaldrywood/detent/internal/auth"
)

func TestHostedLoginCallbackProtection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		wantStatus int
		exchanged  bool
	}{
		{name: "valid", wantStatus: http.StatusSeeOther, exchanged: true},
		{name: "wrong state", wantStatus: http.StatusUnauthorized},
		{name: "missing cookie", wantStatus: http.StatusUnauthorized},
		{name: "expired transaction", wantStatus: http.StatusUnauthorized},
		{name: "missing verifier", wantStatus: http.StatusUnauthorized},
		{name: "provider error callback", wantStatus: http.StatusUnauthorized},
		{name: "exchange failure", wantStatus: http.StatusUnauthorized, exchanged: true},
		{name: "wrong organization", wantStatus: http.StatusForbidden, exchanged: true},
		{name: "unverified email", wantStatus: http.StatusUnauthorized, exchanged: true},
		{name: "support without start", wantStatus: http.StatusForbidden, exchanged: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			transaction, state := hostedLoginStart(t, s, false)
			query := url.Values{"code": {"callback_code"}, "state": {state}}
			cookies := []*http.Cookie{transaction}
			switch tt.name {
			case "wrong state":
				query.Set("state", "another_browser")
			case "missing cookie":
				cookies = nil
			case "expired transaction":
				hostedLoginExec(t, s, "UPDATE hosted_transactions SET expires_at = ?", formatHubTime(time.Now().Add(-time.Hour)))
			case "missing verifier":
				hostedLoginExec(t, s, "UPDATE hosted_transactions SET verifier = ''")
			case "provider error callback":
				query.Set("error", "access_denied")
			case "exchange failure":
				p.exchangeErr = errors.New("private provider error callback_code")
			case "wrong organization":
				p.identity.Hosted.OrganizationID = "org_other_provider"
			case "unverified email":
				p.identity.EmailVerified = false
			case "support without start":
				p.identity.Hosted.SupportActor, p.identity.Hosted.SupportReason = "support@example.test", "troubleshooting"
			}
			recorder := hostedLoginRequest(s, http.MethodGet, "/auth/oidc/callback?"+query.Encode(), "", nil, false, cookies...)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("callback status = %d, want %d: %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if (len(p.exchanges) != 0) != tt.exchanged {
				t.Fatalf("provider exchange calls = %d, want exchanged %v", len(p.exchanges), tt.exchanged)
			}
			if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Referrer-Policy") != "no-referrer" {
				t.Fatal("callback omitted sensitive-response headers")
			}
			if tt.exchanged && (p.exchanges[0].verifier == "" || p.exchanges[0].nonce != state) {
				t.Fatal("callback lost its server-side PKCE transaction")
			}
			if recorder.Code == http.StatusSeeOther {
				cookie := hostedLoginCookie(t, recorder, hostedCookie)
				if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode || cookie.Path != "/" || !cookie.Expires.Equal(p.identity.Hosted.ExpiresAt) {
					t.Fatalf("session cookie protection or expiry = %#v", cookie)
				}
				session, err := s.hostedSessions.Authenticate(t.Context(), cookie.Value)
				if err != nil || !session.ExpiresAt.Equal(p.identity.Hosted.ExpiresAt) {
					t.Fatalf("persisted session expiry = %v, error = %v", session.ExpiresAt, err)
				}
				replayed := hostedLoginRequest(s, http.MethodGet, "/auth/oidc/callback?"+query.Encode(), "", nil, false, transaction)
				if replayed.Code != http.StatusUnauthorized || len(p.exchanges) != 1 {
					t.Fatalf("callback replay status = %d, exchange count = %d", replayed.Code, len(p.exchanges))
				}
			}
			if strings.Contains(recorder.Body.String(), "private provider error") || strings.Contains(recorder.Body.String(), "callback_code") {
				t.Fatal("callback error exposed provider details")
			}
		})
	}
}

func TestHostedLoginStaffAndUnallocatedStart(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		allocated bool
		staff     bool
	}{
		{name: "staff", allocated: true, staff: true},
		{name: "new organization", allocated: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, tt.allocated))
			_, _ = hostedLoginStart(t, s, tt.staff)
		})
	}
}

func TestHostedLoginSupportBrowserBinding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "authorized browser", wantStatus: http.StatusSeeOther},
		{name: "missing staff cookie", wantStatus: http.StatusForbidden},
		{name: "different staff session", wantStatus: http.StatusForbidden},
		{name: "revoked staff session", wantStatus: http.StatusForbidden},
		{name: "wrong support actor", wantStatus: http.StatusForbidden},
		{name: "wrong organization", wantStatus: http.StatusForbidden},
		{name: "ordinary identity", wantStatus: http.StatusForbidden},
		{name: "unapproved reason", wantStatus: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			staff := hostedLoginIdentity("user_staff", "support@example.test", "")
			staffToken := hostedLoginSession(t, s, p, staff)
			start := hostedLoginRequest(s, http.MethodPost, "/support/start", staffToken, nil, true)
			if start.Code != http.StatusOK {
				t.Fatalf("support start status = %d: %s", start.Code, start.Body.String())
			}
			transaction := hostedLoginCookie(t, start, hostedTransactionCookie)
			p.identity.Hosted.SupportActor, p.identity.Hosted.SupportReason = staff.Email, "troubleshooting"
			p.identity.Hosted.ExpiresAt = time.Now().UTC().Add(7 * time.Minute).Truncate(time.Second)
			switch tt.name {
			case "missing staff cookie":
				staffToken = ""
			case "different staff session":
				staff.Hosted.SessionID = "session_another_staff_browser"
				staffToken = hostedLoginSession(t, s, p, staff)
			case "revoked staff session":
				delete(p.sessions, staff.Hosted.SessionID)
			case "wrong support actor":
				p.identity.Hosted.SupportActor = "other@example.test"
			case "wrong organization":
				p.identity.Hosted.OrganizationID = "org_another_provider"
			case "ordinary identity":
				p.identity.Hosted.SupportActor, p.identity.Hosted.SupportReason = "", ""
			case "unapproved reason":
				p.identity.Hosted.SupportReason = "private customer content"
			}
			p.sessions[p.identity.Hosted.SessionID] = *p.identity.Hosted
			callback := hostedLoginRequest(s, http.MethodGet, "/auth/oidc/callback?code=support_code", staffToken, nil, false, transaction)
			if callback.Code != tt.wantStatus {
				t.Fatalf("support callback status = %d, want %d: %s", callback.Code, tt.wantStatus, callback.Body.String())
			}
			if tt.wantStatus == http.StatusSeeOther {
				if len(p.exchanges) != 1 || p.exchanges[0].verifier != "" {
					t.Fatal("support callback did not use its support transaction")
				}
				cookie := hostedLoginCookie(t, callback, hostedCookie)
				if !cookie.Expires.Equal(p.identity.Hosted.ExpiresAt) {
					t.Fatal("support expiry became an ordinary session lifetime")
				}
				if _, err := s.hostedSessions.Authenticate(t.Context(), staffToken); !errors.Is(err, auth.ErrInvalidSession) {
					t.Fatalf("previous browser session remains active: %v", err)
				}
				var actor, effective, reason string
				if err := s.database.db.QueryRowContext(t.Context(), "SELECT actual_actor,effective_user,reason FROM hosted_audit WHERE event = 'session_started'").Scan(&actor, &effective, &reason); err != nil {
					t.Fatal(err)
				}
				if actor != staff.Email || effective != p.identity.Subject || reason != "troubleshooting" {
					t.Fatalf("support audit identities = %q, %q, %q", actor, effective, reason)
				}
			}
		})
	}
}

func TestHostedLoginSupportStartRequiresAuthorizationAndCSRF(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		email         string
		csrf          bool
		authorization string
	}{
		{name: "ordinary staff", email: "staff@example.test", csrf: true},
		{name: "customer", email: "customer@example.test", csrf: true},
		{name: "missing CSRF", email: "support@example.test"},
		{name: "arbitrary authorization", email: "support@example.test", authorization: "arbitrary"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			token := hostedLoginSession(t, s, p, hostedLoginIdentity("user_staff", tt.email, ""))
			form := url.Values{}
			if tt.csrf {
				form.Set("csrf", hostedCSRF(token))
			}
			request := httptest.NewRequest(http.MethodPost, "/support/start", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.Header.Set("Authorization", tt.authorization)
			request.AddCookie(&http.Cookie{Name: hostedCookie, Value: token})
			recorder := httptest.NewRecorder()
			s.echo.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusForbidden {
				t.Fatalf("support start status = %d, want forbidden", recorder.Code)
			}
		})
	}
}

func TestHostedLoginInvitationIntentAndReplay(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		wantStatus int
		acceptCall bool
	}{
		{name: "valid", wantStatus: http.StatusSeeOther, acceptCall: true},
		{name: "wrong recipient", wantStatus: http.StatusForbidden},
		{name: "wrong organization", wantStatus: http.StatusForbidden},
		{name: "provider acceptance recovery", wantStatus: http.StatusSeeOther},
		{name: "accepted by another user", wantStatus: http.StatusForbidden},
		{name: "expired invitation", wantStatus: http.StatusForbidden},
		{name: "unissued invitation", wantStatus: http.StatusForbidden},
		{name: "used local invitation", wantStatus: http.StatusForbidden},
		{name: "membership role mismatch", wantStatus: http.StatusForbidden, acceptCall: true},
		{name: "provider acceptance failure", wantStatus: http.StatusForbidden, acceptCall: true},
		{name: "missing token", wantStatus: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			identity := hostedLoginIdentity("user_invited", "invitee@example.test", "")
			token := hostedLoginSession(t, s, p, identity)
			p.invitation = auth.Invitation{ID: "invitation_login", Email: identity.Email, OrganizationID: "org_provider_login", State: "pending", ExpiresAt: time.Now().Add(time.Hour)}
			membership := hostedLoginMembership(identity.Subject, "org_provider_login", "member")
			p.memberships = append(p.memberships, membership)
			hostedLoginExec(t, s, "INSERT INTO hosted_invitations(id,email,organization_id,role,created_at) VALUES (?,?,?,?,?)", p.invitation.ID, identity.Email, "org_local_login", "member", formatHubTime(time.Now()))
			form := url.Values{"token": {"invitation_secret"}}
			switch tt.name {
			case "wrong recipient":
				p.invitation.Email = "other@example.test"
			case "wrong organization":
				p.invitation.OrganizationID = "org_other_provider"
			case "provider acceptance recovery":
				p.invitation.State, p.invitation.AcceptedUserID = "accepted", identity.Subject
			case "accepted by another user":
				p.invitation.State, p.invitation.AcceptedUserID = "accepted", "user_other"
			case "expired invitation":
				p.invitation.ExpiresAt = time.Now().Add(-time.Hour)
			case "unissued invitation":
				p.invitation.ID = "invitation_not_issued_locally"
			case "used local invitation":
				hostedLoginExec(t, s, "UPDATE hosted_invitations SET accepted_user_id = ?", identity.Subject)
			case "membership role mismatch":
				p.memberships[len(p.memberships)-1].Role.Slug = "admin"
			case "provider acceptance failure":
				p.acceptErr = auth.ErrHostedIdentity
			case "missing token":
				form.Del("token")
			}
			recorder := hostedLoginRequest(s, http.MethodPost, "/organization/join", token, form, true)
			if recorder.Code != tt.wantStatus || (p.accepted != 0) != tt.acceptCall {
				t.Fatalf("join status = %d, acceptance calls = %d: %s", recorder.Code, p.accepted, recorder.Body.String())
			}
			var count int
			if err := s.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_members WHERE user_id = ? AND active = 1", identity.Subject).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if tt.wantStatus == http.StatusSeeOther {
				if count != 1 || recorder.Header().Get("Location") != "/auth/oidc/start" {
					t.Fatal("accepted invitation did not establish membership and restart scoped login")
				}
				replay := hostedLoginRequest(s, http.MethodPost, "/organization/join", token, form, true)
				if replay.Code != http.StatusForbidden || (p.accepted != 0) != tt.acceptCall {
					t.Fatal("used invitation was accepted twice")
				}
			} else if count != 0 {
				t.Fatal("rejected invitation granted local membership")
			}
		})
	}
}

func TestHostedLoginInvitationEmailEntry(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		entryCode  int
		resultCode int
	}{
		{name: "pending invitation", entryCode: http.StatusSeeOther, resultCode: http.StatusSeeOther},
		{name: "provider acceptance recovery", entryCode: http.StatusSeeOther, resultCode: http.StatusSeeOther},
		{name: "wrong recipient login", entryCode: http.StatusSeeOther, resultCode: http.StatusForbidden},
		{name: "wrong organization", entryCode: http.StatusForbidden},
		{name: "unissued invitation", entryCode: http.StatusForbidden},
		{name: "reused local invitation", entryCode: http.StatusForbidden},
		{name: "accepted by another user", entryCode: http.StatusSeeOther, resultCode: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			p.identity = hostedLoginIdentity("user_invited", "invitee@example.test", "")
			p.sessions[p.identity.Hosted.SessionID] = *p.identity.Hosted
			p.memberships = append(p.memberships, hostedLoginMembership(p.identity.Subject, "org_provider_login", "member"))
			p.invitation = auth.Invitation{ID: "invitation_email", Email: p.identity.Email, OrganizationID: "org_provider_login", State: "pending", ExpiresAt: time.Now().Add(time.Hour)}
			hostedLoginExec(t, s, "INSERT INTO hosted_invitations(id,email,organization_id,role,created_at) VALUES (?,?,?,?,?)", p.invitation.ID, p.identity.Email, "org_local_login", "member", formatHubTime(time.Now()))
			switch tt.name {
			case "provider acceptance recovery":
				p.invitation.State, p.invitation.AcceptedUserID = "accepted", p.identity.Subject
			case "wrong recipient login":
				p.identity.Email = "different@example.test"
			case "wrong organization":
				p.invitation.OrganizationID = "org_other_provider"
			case "unissued invitation":
				p.invitation.ID = "invitation_unissued"
			case "reused local invitation":
				hostedLoginExec(t, s, "UPDATE hosted_invitations SET accepted_user_id = ?", p.identity.Subject)
			case "accepted by another user":
				p.invitation.State, p.invitation.AcceptedUserID = "accepted", "user_other"
			}
			entry := hostedLoginRequest(s, http.MethodGet, "/invite?invitation_token=invitation_secret", "", nil, false)
			if entry.Code != tt.entryCode {
				t.Fatalf("invitation entry status = %d, want %d: %s", entry.Code, tt.entryCode, entry.Body.String())
			}
			if tt.entryCode != http.StatusSeeOther {
				return
			}
			authorization, err := url.Parse(entry.Header().Get("Location"))
			if err != nil || authorization.Query().Has("invitation_token") || authorization.Query().Has("organization_id") || strings.Contains(authorization.String(), "invitation_secret") {
				t.Fatalf("invitation forwarded before recipient verification: %q, %v", entry.Header().Get("Location"), err)
			}
			transaction := hostedLoginCookie(t, entry, hostedTransactionCookie)
			query := url.Values{"code": {"invitation_callback"}, "state": {authorization.Query().Get("state")}, "invitation_token": {"attacker_token"}}
			callback := hostedLoginRequest(s, http.MethodGet, "/auth/oidc/callback?"+query.Encode(), "", nil, false, transaction)
			if callback.Code != tt.resultCode {
				t.Fatalf("invitation callback status = %d, want %d: %s", callback.Code, tt.resultCode, callback.Body.String())
			}
			if p.lastInvitationToken != "invitation_secret" {
				t.Fatal("callback used invitation token outside the protected transaction")
			}
			if tt.resultCode == http.StatusSeeOther {
				if callback.Header().Get("Location") != "/auth/oidc/start" {
					t.Fatal("invitation callback omitted fresh organization login")
				}
				var acceptedUser string
				if err := s.database.db.QueryRowContext(t.Context(), "SELECT accepted_user_id FROM hosted_invitations WHERE id = ?", p.invitation.ID).Scan(&acceptedUser); err != nil || acceptedUser != p.identity.Subject {
					t.Fatalf("local invitation recipient = %q, error = %v", acceptedUser, err)
				}
				replay := hostedLoginRequest(s, http.MethodGet, "/auth/oidc/callback?"+query.Encode(), "", nil, false, transaction)
				if replay.Code != http.StatusUnauthorized || len(p.exchanges) != 1 {
					t.Fatal("invitation callback transaction was reusable")
				}
			}
		})
	}
}

func TestHostedLoginOrganizationCreation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		wantStatus int
	}{
		{name: "reserved creator", wantStatus: http.StatusSeeOther},
		{name: "another creator", wantStatus: http.StatusForbidden},
		{name: "staff creator", wantStatus: http.StatusForbidden},
		{name: "empty name", wantStatus: http.StatusUnprocessableEntity},
		{name: "wrong stable identity", wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, false))
			identity := hostedLoginIdentity("user_customer", "customer@example.test", "")
			form := url.Values{"name": {"Customer organization"}}
			switch tt.name {
			case "another creator":
				identity.Subject, identity.Hosted.Subject = "user_other", "user_other"
			case "staff creator":
				identity.Email = "staff@example.test"
			case "empty name":
				form.Set("name", "  ")
			case "wrong stable identity":
				p.organization.ExternalID = "org_another_local"
			}
			token := hostedLoginSession(t, s, p, identity)
			recorder := hostedLoginRequest(s, http.MethodPost, "/organization/create", token, form, true)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("organization create status = %d, want %d: %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if recorder.Code == http.StatusSeeOther {
				providerID, err := s.hostedProviderOrganization(t.Context())
				if err != nil || providerID != "org_provider_login" || p.createdOrganization != "org_local_login" || p.createdRole != "owner" {
					t.Fatalf("organization binding = %q, error = %v, external = %q, role = %q", providerID, err, p.createdOrganization, p.createdRole)
				}
				var role string
				if err := s.database.db.QueryRowContext(t.Context(), "SELECT role FROM hosted_members WHERE user_id = ?", identity.Subject).Scan(&role); err != nil || role != "owner" {
					t.Fatalf("local creator role = %q, error = %v", role, err)
				}
				_, _ = hostedLoginStart(t, s, false)
			}
		})
	}
}

func TestHostedLoginOrganizationSetupRecovery(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name             string
		membershipFailed bool
	}{
		{name: "provider membership failed", membershipFailed: true},
		{name: "local organization name failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			provider := newHostedLoginProvider()
			service := openTestService(t, hostedLoginConfig(t, provider, false))
			identity := hostedLoginIdentity("user_customer", "customer@example.test", "")
			token := hostedLoginSession(t, service, provider, identity)
			if tt.membershipFailed {
				provider.createMembershipErr = auth.ErrHostedIdentity
			} else {
				hostedLoginExec(t, service, "CREATE TRIGGER reject_hosted_name BEFORE UPDATE OF name ON organizations BEGIN SELECT RAISE(ABORT, 'fixture'); END")
			}
			form := url.Values{"name": {"Recovered organization"}}
			response := hostedLoginRequest(service, http.MethodPost, "/organization/create", token, form, true)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("interrupted setup status = %d", response.Code)
			}
			providerID, err := service.hostedProviderOrganization(t.Context())
			if err != nil || providerID != provider.organization.ID {
				t.Fatalf("interrupted provider binding = %q, error = %v", providerID, err)
			}
			var members int
			if err := service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_members").Scan(&members); err != nil || members != 0 {
				t.Fatalf("incomplete local setup persisted members = %d, error = %v", members, err)
			}
			page := hostedLoginRequest(service, http.MethodGet, "/organization", token, nil, false)
			if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), `action="/organization/create"`) {
				t.Fatal("interrupted setup lost the organization creation form")
			}
			provider.createMembershipErr = nil
			if !tt.membershipFailed {
				hostedLoginExec(t, service, "DROP TRIGGER reject_hosted_name")
			}
			response = hostedLoginRequest(service, http.MethodPost, "/organization/create", token, form, true)
			if response.Code != http.StatusSeeOther {
				t.Fatalf("recovered setup status = %d: %s", response.Code, response.Body.String())
			}
			var name string
			if err := service.database.db.QueryRowContext(t.Context(), "SELECT name FROM organizations WHERE id = ?", service.config.Hosted.OrganizationID).Scan(&name); err != nil || name != "Recovered organization" {
				t.Fatalf("recovered organization name = %q, error = %v", name, err)
			}
			page = hostedLoginRequest(service, http.MethodGet, "/organization", token, nil, false)
			if page.Code != http.StatusOK || strings.Contains(page.Body.String(), `action="/organization/create"`) {
				t.Fatal("completed setup still offers organization creation")
			}
		})
	}
}

func TestHostedLoginBootstrapMembershipLifecycle(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		role       string
		existing   bool
		wrongOrg   bool
		wantStatus int
	}{
		{name: "initial owner", role: "owner", wantStatus: http.StatusSeeOther},
		{name: "initial member", role: "member", wantStatus: http.StatusForbidden},
		{name: "changed owner role", role: "viewer", existing: true, wantStatus: http.StatusSeeOther},
		{name: "wrong provider organization", role: "owner", wrongOrg: true, wantStatus: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			provider := newHostedLoginProvider()
			service := openTestService(t, hostedLoginConfig(t, provider, true))
			provider.memberships[0].Role.Slug = tt.role
			if tt.wrongOrg {
				provider.organization.ID = "org_unrelated"
			}
			if tt.existing {
				if err := service.addHostedMember(t.Context(), provider.identity, provider.memberships[0]); err != nil {
					t.Fatal(err)
				}
			}
			transaction, state := hostedLoginStart(t, service, false)
			callback := hostedLoginRequest(service, http.MethodGet, "/auth/oidc/callback?"+url.Values{"code": {"callback"}, "state": {state}}.Encode(), "", nil, false, transaction)
			if callback.Code != tt.wantStatus {
				t.Fatalf("bootstrap callback=%d, want=%d: %s", callback.Code, tt.wantStatus, callback.Body.String())
			}
			if tt.wantStatus == http.StatusSeeOther && !tt.existing {
				var name string
				if err := service.database.db.QueryRowContext(t.Context(), "SELECT name FROM organizations WHERE id = ?", service.config.Hosted.OrganizationID).Scan(&name); err != nil || name != provider.organization.Name {
					t.Fatalf("organization name=%q, error=%v", name, err)
				}
			}
		})
	}
}

func TestHostedLoginOrganizationSwitching(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		wantStatus int
	}{
		{name: "trusted target", wantStatus: http.StatusSeeOther},
		{name: "untrusted target", wantStatus: http.StatusForbidden},
		{name: "no membership", wantStatus: http.StatusForbidden},
		{name: "inactive membership", wantStatus: http.StatusForbidden},
		{name: "support session", wantStatus: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			identity := p.identity
			form := url.Values{"organization": {"org_destination"}, "url": {"https://attacker.example.test"}}
			p.memberships = append(p.memberships, hostedLoginMembership(identity.Subject, "org_provider_destination", "member"))
			switch tt.name {
			case "untrusted target":
				form.Set("organization", "https://attacker.example.test")
			case "no membership":
				p.memberships = nil
			case "inactive membership":
				p.memberships[len(p.memberships)-1].Status = "inactive"
			case "support session":
				identity.Hosted.SupportActor, identity.Hosted.SupportReason = "support@example.test", "troubleshooting"
			}
			token := hostedLoginSession(t, s, p, identity)
			recorder := hostedLoginRequest(s, http.MethodPost, "/organization/switch", token, form, true)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("switch status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if recorder.Code == http.StatusSeeOther && recorder.Header().Get("Location") != "https://destination.example.test/auth/oidc/start" {
				t.Fatalf("untrusted redirect %q", recorder.Header().Get("Location"))
			}
		})
	}
}

func TestHostedLoginLogoutRevokesLocalAndProviderSessions(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		wantStatus int
	}{
		{name: "customer", wantStatus: http.StatusSeeOther},
		{name: "support", wantStatus: http.StatusSeeOther},
		{name: "expired local session", wantStatus: http.StatusSeeOther},
		{name: "provider failure", wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			identity := p.identity
			if tt.name == "support" {
				identity.Hosted.SupportActor, identity.Hosted.SupportReason = "support@example.test", "troubleshooting"
			}
			if tt.name == "provider failure" {
				p.revokeErr = errors.New("private provider credential")
			}
			token := hostedLoginSession(t, s, p, identity)
			if tt.name == "expired local session" {
				hostedLoginExec(t, s, "UPDATE hosted_sessions SET expires_at = ?", formatHubTime(time.Now().Add(-time.Minute)))
				if _, err := s.hostedSessions.Authenticate(t.Context(), token); !errors.Is(err, auth.ErrInvalidSession) {
					t.Fatalf("expired local session authenticated before logout: %v", err)
				}
				page := hostedLoginRequest(s, http.MethodGet, "/organization", token, nil, false)
				if page.Code != http.StatusSeeOther || page.Header().Get("Location") != "/login" {
					t.Fatal("expired session reopened organization access")
				}
			}
			recorder := hostedLoginRequest(s, http.MethodPost, "/logout", token, nil, true)
			if recorder.Code != tt.wantStatus || len(p.revoked) != 1 || p.revoked[0] != identity.Hosted.SessionID {
				t.Fatalf("logout status = %d, revoked = %#v", recorder.Code, p.revoked)
			}
			cookie := hostedLoginCookie(t, recorder, hostedCookie)
			if cookie.Value != "" || cookie.MaxAge != -1 {
				t.Fatal("logout did not clear browser session")
			}
			if _, err := s.hostedSessions.Authenticate(t.Context(), token); !errors.Is(err, auth.ErrInvalidSession) {
				t.Fatalf("logged-out local session remains active: %v", err)
			}
			if strings.Contains(recorder.Body.String(), "private provider credential") {
				t.Fatal("logout exposed provider error")
			}
		})
	}
}

func TestHostedLoginSessionExpiryAndRevocation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"short expiry", "expired identity", "remote revocation", "remote organization change", "shortened remote expiry"} {
		t.Run(mode, func(t *testing.T) {
			p := newHostedLoginProvider()
			s := openTestService(t, hostedLoginConfig(t, p, true))
			identity := p.identity
			identity.Hosted.ExpiresAt = time.Now().UTC().Add(2 * time.Minute).Truncate(time.Second)
			if mode == "expired identity" {
				identity.Hosted.ExpiresAt = time.Now().Add(-time.Minute)
				if _, _, err := s.hostedSessions.CreateIdentitySession(t.Context(), identity); !errors.Is(err, auth.ErrInvalidSession) {
					t.Fatalf("expired identity session creation = %v", err)
				}
				return
			}
			token := hostedLoginSession(t, s, p, identity)
			switch mode {
			case "remote revocation":
				delete(p.sessions, identity.Hosted.SessionID)
			case "remote organization change":
				changed := *identity.Hosted
				changed.OrganizationID = "org_other_provider"
				p.sessions[changed.SessionID] = changed
			case "shortened remote expiry":
				changed := *identity.Hosted
				changed.ExpiresAt = time.Now().UTC().Add(time.Minute).Truncate(time.Second)
				p.sessions[changed.SessionID] = changed
			}
			session, err := s.hostedSessions.Authenticate(t.Context(), token)
			if mode == "remote revocation" || mode == "remote organization change" {
				if !errors.Is(err, auth.ErrInvalidSession) {
					t.Fatalf("stale session error = %v", err)
				}
			} else if err != nil || session.ExpiresAt.After(identity.Hosted.ExpiresAt) || (mode == "shortened remote expiry" && !session.ExpiresAt.Equal(p.sessions[identity.Hosted.SessionID].ExpiresAt)) {
				t.Fatalf("bounded session expiry = %v, error = %v", session.ExpiresAt, err)
			}
		})
	}
}

func hostedLoginConfig(t *testing.T, provider *hostedLoginProvider, allocated bool) Config {
	t.Helper()
	cfg := Config{DatabasePath: filepath.Join(t.TempDir(), "hosted-login.db"), Hosted: &HostedConfig{
		OrganizationID: "org_local_login", BootstrapSubject: "user_customer", PublicURL: "https://login.example.test", Provider: provider,
		StaffEmails: []string{"staff@example.test", "support@example.test"}, SupportActors: []string{"support@example.test"},
		Directory: []HostedDestination{{OrganizationID: "org_destination", WorkOSOrganizationID: "org_provider_destination", PublicURL: "https://destination.example.test"}},
	}}
	if allocated {
		cfg.Hosted.WorkOSOrganizationID = "org_provider_login"
	}
	return cfg
}

func hostedLoginStart(t *testing.T, s *Service, staff bool) (*http.Cookie, string) {
	t.Helper()
	target := "/auth/oidc/start"
	if staff {
		target += "?staff=1"
	}
	recorder := hostedLoginRequest(s, http.MethodGet, target, "", nil, false)
	if recorder.Code != http.StatusSeeOther {
		t.Fatalf("login start status = %d: %s", recorder.Code, recorder.Body.String())
	}
	location, err := url.Parse(recorder.Header().Get("Location"))
	if err != nil || location.Host != "identity.example.test" || location.Query().Get("state") == "" || location.Query().Get("code_challenge") == "" {
		t.Fatalf("authorization redirect = %q, error = %v", recorder.Header().Get("Location"), err)
	}
	providerID, err := s.hostedProviderOrganization(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if staff {
		providerID = ""
	}
	if location.Query().Get("organization_id") != providerID {
		t.Fatalf("authorization organization = %q, want %q", location.Query().Get("organization_id"), providerID)
	}
	return hostedLoginCookie(t, recorder, hostedTransactionCookie), location.Query().Get("state")
}

func hostedLoginRequest(s *Service, method, target, token string, values url.Values, csrf bool, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	if values == nil {
		values = url.Values{}
	}
	if csrf {
		values.Set("csrf", hostedCSRF(token))
	}
	request := httptest.NewRequest(method, target, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		request.AddCookie(&http.Cookie{Name: hostedCookie, Value: token})
	}
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	s.echo.ServeHTTP(recorder, request)
	return recorder
}

func hostedLoginCookie(t *testing.T, recorder *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	t.Fatalf("missing cookie %s", name)
	return nil
}

func hostedLoginSession(t *testing.T, s *Service, provider *hostedLoginProvider, identity auth.Identity) string {
	t.Helper()
	provider.sessions[identity.Hosted.SessionID] = *identity.Hosted
	token, _, err := s.hostedSessions.CreateIdentitySession(t.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func hostedLoginExec(t *testing.T, s *Service, query string, args ...any) {
	t.Helper()
	if _, err := s.database.db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func hostedLoginIdentity(subject, email, organization string) auth.Identity {
	now := time.Now().UTC().Truncate(time.Second)
	return auth.Identity{Subject: subject, Email: email, EmailVerified: true, Hosted: &auth.HostedIdentity{
		Subject: subject, OrganizationID: organization, SessionID: "session_" + subject, CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(30 * time.Minute),
	}}
}

func hostedLoginMembership(user, organization, role string) auth.Membership {
	membership := auth.Membership{ID: "membership_" + user, UserID: user, OrganizationID: organization, Status: "active"}
	membership.Role.Slug = role
	return membership
}

type hostedLoginExchange struct {
	code, verifier, nonce string
}

type hostedLoginProvider struct {
	mu                  sync.Mutex
	identity            auth.Identity
	sessions            map[string]auth.HostedIdentity
	memberships         []auth.Membership
	organization        auth.Organization
	invitation          auth.Invitation
	exchanges           []hostedLoginExchange
	exchangeErr         error
	acceptErr           error
	revokeErr           error
	accepted            int
	revoked             []string
	createdOrganization string
	createdRole         string
	createMembershipErr error
	lastInvitationToken string
}

func newHostedLoginProvider() *hostedLoginProvider {
	identity := hostedLoginIdentity("user_customer", "customer@example.test", "org_provider_login")
	return &hostedLoginProvider{
		identity: identity, sessions: map[string]auth.HostedIdentity{identity.Hosted.SessionID: *identity.Hosted},
		memberships:  []auth.Membership{hostedLoginMembership(identity.Subject, identity.Hosted.OrganizationID, "owner")},
		organization: auth.Organization{ID: "org_provider_login", ExternalID: "org_local_login", Name: "Customer organization"},
	}
}

func (*hostedLoginProvider) AuthorizationURL(state, _ string, verifier string) string {
	return "https://identity.example.test/authorize?" + url.Values{"state": {state}, "code_challenge": {oauth2.S256ChallengeFromVerifier(verifier)}}.Encode()
}

func (p *hostedLoginProvider) Exchange(_ context.Context, code, verifier, nonce string) (auth.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exchanges = append(p.exchanges, hostedLoginExchange{code, verifier, nonce})
	return p.identity, p.exchangeErr
}

func (p *hostedLoginProvider) CurrentSession(_ context.Context, identity auth.HostedIdentity) (auth.HostedIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[identity.SessionID]
	if !ok || !session.ExpiresAt.After(time.Now()) {
		return auth.HostedIdentity{}, auth.ErrHostedIdentity
	}
	return session, nil
}

func (p *hostedLoginProvider) Memberships(_ context.Context, user, organization string) ([]auth.Membership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var result []auth.Membership
	for _, membership := range p.memberships {
		if (user == "" || membership.UserID == user) && (organization == "" || membership.OrganizationID == organization) {
			result = append(result, membership)
		}
	}
	return result, nil
}

func (p *hostedLoginProvider) Organization(context.Context, string) (auth.Organization, error) {
	return p.organization, nil
}

func (p *hostedLoginProvider) CreateOrganization(_ context.Context, external, _ string) (auth.Organization, error) {
	p.createdOrganization = external
	return p.organization, nil
}

func (p *hostedLoginProvider) CreateMembership(_ context.Context, user, organization, role string) (auth.Membership, error) {
	p.createdRole = role
	return hostedLoginMembership(user, organization, role), p.createMembershipErr
}

func (*hostedLoginProvider) SetMembershipRole(context.Context, string, string) error {
	return nil
}

func (*hostedLoginProvider) RevokeMembership(context.Context, string) error {
	return nil
}

func (p *hostedLoginProvider) Invite(context.Context, string, string, string, string) (auth.Invitation, error) {
	return p.invitation, nil
}

func (p *hostedLoginProvider) Invitation(_ context.Context, token string) (auth.Invitation, error) {
	p.lastInvitationToken = token
	return p.invitation, nil
}

func (p *hostedLoginProvider) AcceptInvitation(_ context.Context, _, user string) error {
	p.accepted++
	if p.acceptErr != nil {
		return p.acceptErr
	}
	p.invitation.State, p.invitation.AcceptedUserID = "accepted", user
	return nil
}

func (p *hostedLoginProvider) RevokeSession(_ context.Context, id string) error {
	p.revoked = append(p.revoked, id)
	if p.revokeErr != nil {
		return p.revokeErr
	}
	delete(p.sessions, id)
	return nil
}

var _ auth.HostedProvider = (*hostedLoginProvider)(nil)
