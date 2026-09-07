package hubserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/auth"
)

const hostedCookie = "detent_hosted_session"

func (s *Service) CreateWebSession(ctx context.Context, record auth.SessionRecord) error {
	if s.config.Hosted == nil || record.Identity == nil {
		return auth.ErrInvalidSession
	}
	encoded, err := json.Marshal(record.Identity)
	if err != nil {
		return auth.ErrInvalidSession
	}
	_, err = s.database.db.ExecContext(ctx, "INSERT INTO hosted_sessions (token_hash,email,identity_json,expires_at,created_at) VALUES (?,?,?,?,?)", record.TokenHash, record.Email, string(encoded), formatHubTime(record.ExpiresAt), formatHubTime(record.CreatedAt))
	return err
}

func (s *Service) WebSession(ctx context.Context, hash string, now time.Time) (auth.Session, error) {
	var session auth.Session
	var encoded, expiry string
	err := s.database.db.QueryRowContext(ctx, "SELECT email,identity_json,expires_at FROM hosted_sessions WHERE token_hash = ? AND revoked_at IS NULL", hash).Scan(&session.Email, &encoded, &expiry)
	if err != nil || json.Unmarshal([]byte(encoded), &session.Identity) != nil || session.Identity == nil {
		return auth.Session{}, auth.ErrInvalidSession
	}
	session.ExpiresAt, err = parseTimeValue(expiry)
	if err != nil || !session.ExpiresAt.After(now) {
		return auth.Session{}, auth.ErrInvalidSession
	}
	current, err := s.config.Hosted.Provider.CurrentSession(ctx, *session.Identity)
	if err != nil || !current.ExpiresAt.After(now) || current.Subject != session.Identity.Subject || current.SessionID != session.Identity.SessionID || current.OrganizationID != session.Identity.OrganizationID || current.SupportActor != session.Identity.SupportActor || current.SupportReason != session.Identity.SupportReason {
		return auth.Session{}, auth.ErrInvalidSession
	}
	if current.ExpiresAt.Before(session.ExpiresAt) {
		session.ExpiresAt = current.ExpiresAt
	}
	session.Identity.ExpiresAt = session.ExpiresAt
	if current.SupportActor != "" && (!hostedEmailListed(s.config.Hosted.SupportActors, current.SupportActor) || !auth.ValidSupportReason(current.SupportReason)) {
		return auth.Session{}, auth.ErrInvalidSession
	}
	return session, nil
}

func (s *Service) hostedSession(c echo.Context) (auth.Session, string, error) {
	cookie, err := c.Cookie(hostedCookie)
	if err != nil || s.hostedSessions == nil {
		return auth.Session{}, "", auth.ErrInvalidSession
	}
	session, err := s.hostedSessions.Authenticate(c.Request().Context(), cookie.Value)
	if err == nil {
		c.Set("hosted_session", session)
	}
	return session, apikey.HashToken(cookie.Value), err
}

func (s *Service) hostedMembership(ctx context.Context, identity *auth.HostedIdentity) (auth.Membership, error) {
	if identity == nil || identity.OrganizationID == "" {
		return auth.Membership{}, auth.ErrHostedIdentity
	}
	memberships, err := s.config.Hosted.Provider.Memberships(ctx, identity.Subject, identity.OrganizationID)
	if err != nil {
		return auth.Membership{}, auth.ErrHostedIdentity
	}
	for _, membership := range memberships {
		if membership.UserID == identity.Subject && membership.OrganizationID == identity.OrganizationID && membership.Status == "active" && auth.ValidOrganizationRole(membership.Role.Slug) {
			return membership, nil
		}
	}
	return auth.Membership{}, auth.ErrHostedIdentity
}

func (s *Service) hostedCredential(c echo.Context) (apiCredential, int, error) {
	session, hash, err := s.hostedSession(c)
	if err != nil {
		return apiCredential{}, http.StatusUnauthorized, auth.ErrInvalidSession
	}
	return s.hostedSessionCredential(c.Request().Context(), session, hash)
}

func (s *Service) hostedSessionCredential(ctx context.Context, session auth.Session, hash string) (apiCredential, int, error) {
	if hostedEmailListed(s.config.Hosted.StaffEmails, session.Email) && session.Identity.SupportActor == "" {
		return apiCredential{}, http.StatusForbidden, auth.ErrHostedIdentity
	}
	providerID, err := s.hostedProviderOrganization(ctx)
	if err != nil || providerID == "" || session.Identity.OrganizationID != providerID {
		return apiCredential{}, http.StatusForbidden, auth.ErrHostedIdentity
	}
	membership, err := s.hostedMembership(ctx, session.Identity)
	if err != nil {
		return apiCredential{}, http.StatusForbidden, err
	}
	credential := apiCredential{Scope: apiScopeOperator, NativeOnly: true, Hosted: session.Identity, SessionHash: hash, HostedRole: membership.Role.Slug}
	err = s.database.db.QueryRowContext(ctx, "SELECT m.principal_id,t.token_hash FROM hosted_members m JOIN api_tokens t ON t.id = m.principal_id WHERE m.user_id = ? AND m.membership_id = ? AND m.active = 1 AND t.revoked_at IS NULL", session.Identity.Subject, membership.ID).Scan(&credential.ID, &credential.Hash)
	if err != nil {
		return apiCredential{}, http.StatusForbidden, auth.ErrHostedIdentity
	}
	return credential, http.StatusOK, nil
}

func hostedCSRF(token string) string {
	sum := sha256.Sum256([]byte("detent-hosted-csrf:" + token))
	return hex.EncodeToString(sum[:])
}

func (s *Service) hostedCSRFValid(c echo.Context) bool {
	cookie, err := c.Cookie(hostedCookie)
	if err != nil {
		return false
	}
	value := c.Request().Header.Get("X-CSRF-Token")
	if value == "" && strings.HasPrefix(c.Request().Header.Get(echo.HeaderContentType), echo.MIMEApplicationForm) {
		c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxAPIRequestBodyBytes)
		value = c.FormValue("csrf")
	}
	return subtle.ConstantTimeCompare([]byte(value), []byte(hostedCSRF(cookie.Value))) == 1
}

func hostedReadRequest(c echo.Context) bool {
	return c.Request().Method == http.MethodGet || c.Request().Method == http.MethodHead
}

func (s *Service) hostedBoundary(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		c.Response().Header().Set("Referrer-Policy", "no-referrer")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		if organization := c.Param("organization"); organization != "" && organization != s.config.Hosted.OrganizationID {
			return s.nativeAPIError(c, nativeNotFound())
		}
		if strings.HasPrefix(c.Path(), "/api/v1/") || c.Path() == "/health" {
			return s.nativeAPIError(c, nativeNotFound())
		}
		bearerAPI := strings.HasPrefix(c.Path(), "/api/v2/") && c.Request().Header.Get(echo.HeaderAuthorization) != ""
		if !hostedReadRequest(c) && !bearerAPI && !strings.HasSuffix(c.Path(), "/redeem") && !s.hostedCSRFValid(c) {
			return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "invalid_csrf", Message: "Reload the form and try again"})
		}
		if !hostedReadRequest(c) && !bearerAPI {
			s.hostedMutationMu.Lock()
			defer s.hostedMutationMu.Unlock()
		}
		return next(c)
	}
}

func (s *Service) requireHostedProject(ctx context.Context, query nativeQueryer, scope nativeScope, write bool) error {
	if scope.credential.Hosted == nil {
		return nil
	}
	if string(scope.organization) != s.config.Hosted.OrganizationID || scope.project == "" {
		return nativeNotFound()
	}
	var count int
	err := query.QueryRowContext(ctx, `SELECT count(*) FROM hosted_project_grants g JOIN hosted_members m ON m.user_id = g.user_id
WHERE m.user_id = ? AND m.active = 1 AND g.organization_id = ? AND g.project_id = ? AND (? = 0 OR g.can_write = 1)`, scope.credential.Hosted.Subject, scope.organization, scope.project, write).Scan(&count)
	if err != nil {
		return err
	}
	if count != 1 || write && scope.credential.HostedRole == "viewer" {
		return nativeNotFound()
	}
	return nil
}

func (s *Service) recheckHostedMutation(ctx context.Context, tx *sql.Tx, scope nativeScope) error {
	if scope.credential.Hosted == nil {
		return nil
	}
	identity, err := s.config.Hosted.Provider.CurrentSession(ctx, *scope.credential.Hosted)
	if err != nil || !identity.ExpiresAt.After(s.config.now()) || identity.Subject != scope.credential.Hosted.Subject || identity.SessionID != scope.credential.Hosted.SessionID || identity.SupportActor != scope.credential.Hosted.SupportActor || identity.SupportReason != scope.credential.Hosted.SupportReason || identity.OrganizationID != scope.credential.Hosted.OrganizationID {
		return auth.ErrHostedIdentity
	}
	membership, err := s.hostedMembership(ctx, scope.credential.Hosted)
	if err != nil || membership.Role.Slug == "viewer" {
		return auth.ErrHostedIdentity
	}
	var count int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM hosted_sessions s, hosted_members m
WHERE s.token_hash = ? AND s.revoked_at IS NULL AND julianday(s.expires_at) > julianday(?) AND m.user_id = ? AND m.membership_id = ? AND m.active = 1`, scope.credential.SessionHash, formatHubTime(s.config.now()), identity.Subject, membership.ID).Scan(&count)
	if err != nil || count != 1 {
		return auth.ErrHostedIdentity
	}
	if scope.project != "" {
		return s.requireHostedProject(ctx, tx, scope, true)
	}
	if scope.credential.ManageRunners {
		var projects, granted int
		err := tx.QueryRowContext(ctx, `SELECT count(*), COALESCE(sum(EXISTS(SELECT 1 FROM hosted_project_grants g WHERE g.project_id = p.id AND g.user_id = ? AND g.manage_runner = 1)),0) FROM projects p WHERE p.organization_id = ?`, identity.Subject, scope.organization).Scan(&projects, &granted)
		if err != nil || projects == 0 || projects != granted {
			return auth.ErrHostedIdentity
		}
		return nil
	}
	if membership.Role.Slug != "owner" && membership.Role.Slug != "admin" {
		return auth.ErrHostedIdentity
	}
	return nil
}

func (s *Service) addHostedMember(ctx context.Context, identity auth.Identity, membership auth.Membership) error {
	return s.storeHostedMember(ctx, identity, membership, "")
}

func (s *Service) storeHostedMember(ctx context.Context, identity auth.Identity, membership auth.Membership, organizationName string) (resultErr error) {
	if !identity.EmailVerified || identity.Subject != membership.UserID || membership.Status != "active" || !auth.ValidOrganizationRole(membership.Role.Slug) {
		return auth.ErrHostedIdentity
	}
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	principal := "hosted_" + apikey.HashToken(identity.Subject)[:32]
	now := formatHubTime(s.config.now())
	token, err := s.config.generateToken()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO api_tokens (id,name,token_hash,token_fingerprint,scope,created_at,updated_at,native_only)
VALUES (?,?,?,?, 'operator',?,?,1) ON CONFLICT(id) DO NOTHING`, principal, principal, apikey.HashToken(token), tokenFingerprint(apikey.HashToken(token)), now, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM token_grants WHERE token_id IN (
SELECT principal_id FROM hosted_members WHERE user_id = ? AND (membership_id != ? OR active = 0))`, identity.Subject, membership.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM hosted_project_grants WHERE user_id IN (
SELECT user_id FROM hosted_members WHERE user_id = ? AND (membership_id != ? OR active = 0))`, identity.Subject, membership.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO hosted_members(user_id,email,membership_id,role,active,principal_id,created_at,updated_at)
VALUES (?,?,?,?,1,?,?,?) ON CONFLICT(user_id) DO UPDATE SET email=excluded.email,membership_id=excluded.membership_id,role=excluded.role,active=1,updated_at=excluded.updated_at`, identity.Subject, strings.ToLower(identity.Email), membership.ID, membership.Role.Slug, principal, now, now)
	if err != nil {
		return err
	}
	if organizationName != "" {
		if _, err := tx.ExecContext(ctx, "UPDATE organizations SET name = ? WHERE id = ?", organizationName, s.config.Hosted.OrganizationID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) hostedAudit(ctx context.Context, identity *auth.HostedIdentity, event, route, project string, status int) error {
	if identity == nil {
		return nil
	}
	actor := identity.Subject
	if identity.SupportActor != "" {
		actor = identity.SupportActor
	}
	_, err := s.database.db.ExecContext(ctx, `INSERT INTO hosted_audit(organization_id,session_id,actual_actor,effective_user,reason,event,route,project_id,status,started_at,expires_at,recorded_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, s.config.Hosted.OrganizationID, identity.SessionID, actor, identity.Subject, identity.SupportReason, event, route, project, status, formatHubTime(identity.CreatedAt), formatHubTime(identity.ExpiresAt), formatHubTime(s.config.now()))
	return err
}
