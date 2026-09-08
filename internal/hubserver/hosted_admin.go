package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func (s *Service) hostedAdministrator(c echo.Context) (apiCredential, error) {
	credential, _, err := s.hostedCredential(c)
	if err != nil || credential.HostedRole != "owner" && credential.HostedRole != "admin" {
		return apiCredential{}, auth.ErrHostedIdentity
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "administration", c.Request().Method+" "+c.Path(), "", 0); err != nil {
		return apiCredential{}, err
	}
	return credential, nil
}

func (s *Service) requireHostedAdministration(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		credential, _, err := s.authenticateAPIRequest(c)
		if err != nil || credential.Hosted == nil || c.Param("organization") != s.config.Hosted.OrganizationID {
			return s.nativeAPIError(c, nativeNotFound())
		}
		switch {
		case c.Path() == "/api/v2/organizations/:organization/projects" && c.Request().Method == http.MethodPost:
			if credential.HostedRole != "owner" && credential.HostedRole != "admin" {
				return s.nativeAPIError(c, nativeNotFound())
			}
		case strings.HasPrefix(c.Path(), enrollmentBase) || strings.HasPrefix(c.Path(), runnerBase) || c.Path() == "/api/v2/organizations/:organization/machines/:machine/routing":
			if credential.HostedRole == "viewer" || !s.hostedAllRunnerGrants(c.Request().Context(), credential) {
				return s.nativeAPIError(c, nativeNotFound())
			}
			credential.ManageRunners = true
		default:
			return s.nativeAPIError(c, nativeNotFound())
		}
		c.Set("hub_api_credential", credential)
		if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "administration", c.Request().Method+" "+c.Path(), "", 0); err != nil {
			return s.nativeAPIError(c, err)
		}
		return next(c)
	}
}

func (s *Service) hostedAllRunnerGrants(ctx context.Context, credential apiCredential) bool {
	var projects, granted int
	err := s.database.db.QueryRowContext(ctx, `SELECT count(*), COALESCE(sum(EXISTS(SELECT 1 FROM hosted_project_grants g WHERE g.project_id = p.id AND g.user_id = ? AND g.manage_runner = 1)),0) FROM projects p WHERE p.organization_id = ?`, credential.Hosted.Subject, s.config.Hosted.OrganizationID).Scan(&projects, &granted)
	return err == nil && projects > 0 && projects == granted
}

func (s *Service) inviteHostedMember(c echo.Context) error {
	credential, err := s.hostedAdministrator(c)
	role := c.FormValue("role")
	if err != nil || !auth.ValidOrganizationRole(role) || role == "owner" && credential.HostedRole != "owner" {
		return s.hostedError(c, http.StatusForbidden, "You cannot invite a member with this role")
	}
	email := strings.ToLower(strings.TrimSpace(c.FormValue("email")))
	if email == "" || len(email) > 254 || !strings.Contains(email, "@") || hostedEmailListed(s.config.Hosted.StaffEmails, email) {
		return s.hostedError(c, http.StatusUnprocessableEntity, "Enter the customer's email address")
	}
	if err := s.reserveHostedInvitation(c.Request().Context(), email); err != nil {
		var limit *hostedLimitError
		if errors.As(err, &limit) {
			return s.hostedError(c, http.StatusTooManyRequests, limit.Error())
		}
		return s.hostedError(c, http.StatusServiceUnavailable, "The invitation could not be reserved")
	}
	invitation, err := s.config.Hosted.Provider.Invite(c.Request().Context(), credential.Hosted.OrganizationID, email, role, credential.Hosted.Subject)
	if err != nil || invitation.OrganizationID != credential.Hosted.OrganizationID || !strings.EqualFold(invitation.Email, email) || invitation.State != "pending" {
		return s.hostedInvitationFailure(c, email, err)
	}
	_, err = s.database.db.ExecContext(c.Request().Context(), `INSERT INTO hosted_invitations(id,email,organization_id,role,created_at) VALUES (?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, invitation.ID, email, s.config.Hosted.OrganizationID, role, formatHubTime(s.config.now()))
	if err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "The invitation could not be recorded")
	}
	return c.Redirect(http.StatusSeeOther, "/organization")
}

func (s *Service) hostedManagedMember(c echo.Context, credential apiCredential, removingOwner bool) (auth.Membership, error) {
	members, err := s.config.Hosted.Provider.Memberships(c.Request().Context(), "", credential.Hosted.OrganizationID)
	if err != nil {
		return auth.Membership{}, auth.ErrHostedIdentity
	}
	var selected auth.Membership
	owners := 0
	for _, member := range members {
		if member.OrganizationID != credential.Hosted.OrganizationID || member.Status != "active" {
			continue
		}
		if member.Role.Slug == "owner" {
			owners++
		}
		if member.ID == c.Param("member") {
			selected = member
		}
	}
	if selected.ID == "" || selected.Role.Slug == "owner" && (credential.HostedRole != "owner" || removingOwner && owners <= 1) {
		return auth.Membership{}, auth.ErrHostedIdentity
	}
	return selected, nil
}

func (s *Service) revokeHostedMember(c echo.Context) error {
	credential, err := s.hostedAdministrator(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "You cannot remove organization members")
	}
	member, err := s.hostedManagedMember(c, credential, true)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "This member cannot be removed; the organization must retain an owner")
	}
	if err := s.revokeHostedMemberLocally(c.Request().Context(), member.UserID); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Membership removal is temporarily unavailable")
	}
	if err := s.config.Hosted.Provider.RevokeMembership(c.Request().Context(), member.ID); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Local access is revoked. Provider revocation could not be confirmed; retry removal.")
	}
	return c.Redirect(http.StatusSeeOther, "/organization")
}

func (s *Service) revokeHostedMemberLocally(ctx context.Context, user string) (resultErr error) {
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	statements := []struct {
		query string
		args  []any
	}{
		{"UPDATE hosted_members SET active = 0,updated_at = ? WHERE user_id = ?", []any{formatHubTime(s.config.now()), user}},
		{"DELETE FROM token_grants WHERE token_id = (SELECT principal_id FROM hosted_members WHERE user_id = ?)", []any{user}},
		{"DELETE FROM hosted_project_grants WHERE user_id = ?", []any{user}},
		{"UPDATE hosted_sessions SET revoked_at = ? WHERE json_extract(identity_json,'$.subject') = ? AND revoked_at IS NULL", []any{formatHubTime(s.config.now()), user}},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) changeHostedRole(c echo.Context) error {
	credential, err := s.hostedAdministrator(c)
	role := c.FormValue("role")
	if err != nil || !auth.ValidOrganizationRole(role) || role == "owner" && credential.HostedRole != "owner" {
		return s.hostedError(c, http.StatusForbidden, "You cannot assign this organization role")
	}
	member, err := s.hostedManagedMember(c, credential, role != "owner")
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "This role cannot be changed; the organization must retain an owner")
	}
	if err := s.config.Hosted.Provider.SetMembershipRole(c.Request().Context(), member.ID, role); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "The role could not be changed")
	}
	if _, err := s.database.db.ExecContext(c.Request().Context(), "UPDATE hosted_members SET role = ?,updated_at = ? WHERE user_id = ?", role, formatHubTime(s.config.now()), member.UserID); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "The role could not be recorded")
	}
	return c.Redirect(http.StatusSeeOther, "/organization")
}

func (s *Service) changeHostedGrant(c echo.Context) error {
	credential, err := s.hostedAdministrator(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "You cannot manage project grants")
	}
	user, project := c.FormValue("user"), c.FormValue("project")
	if !hostedSafeID(user) || !hostedSafeID(project) {
		return s.hostedError(c, http.StatusUnprocessableEntity, "Select a member and project")
	}
	err = s.hostedGrant(c.Request().Context(), credential, user, project, hostedFormTrue(c, "write"), hostedFormTrue(c, "runner"), hostedFormTrue(c, "revoke"))
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "The project grant could not be changed")
	}
	return c.Redirect(http.StatusSeeOther, "/organization")
}

func (s *Service) hostedGrant(ctx context.Context, credential apiCredential, user, project string, write, runner, revoke bool) (resultErr error) {
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := s.recheckHostedMutation(ctx, tx, nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), credential: credential}); err != nil {
		return err
	}
	var principal string
	err = tx.QueryRowContext(ctx, "SELECT principal_id FROM hosted_members WHERE user_id = ? AND active = 1", user).Scan(&principal)
	if err != nil {
		return err
	}
	if revoke {
		if _, err := tx.ExecContext(ctx, "DELETE FROM hosted_project_grants WHERE user_id = ? AND project_id = ?", user, project); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM token_grants WHERE token_id = ? AND project_id = ?", principal, project); err != nil {
			return err
		}
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO hosted_project_grants(user_id,organization_id,project_id,can_write,manage_runner) VALUES (?,?,?,?,?) ON CONFLICT(user_id,project_id) DO UPDATE SET can_write=excluded.can_write,manage_runner=excluded.manage_runner`, user, s.config.Hosted.OrganizationID, project, write, runner)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES (?,?,?) ON CONFLICT DO NOTHING", principal, s.config.Hosted.OrganizationID, project); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) createHostedProject(c echo.Context) error {
	credential, err := s.hostedAdministrator(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "You cannot create projects")
	}
	name := strings.TrimSpace(c.FormValue("name"))
	if name == "" || len(name) > 120 || !hostedFormTrue(c, "grant_access") {
		return s.hostedError(c, http.StatusUnprocessableEntity, "Enter a project name and explicitly grant yourself project access")
	}
	project, err := s.createHostedProjectRecord(c.Request().Context(), credential, name)
	if err != nil {
		var limit *hostedLimitError
		if errors.As(err, &limit) {
			return s.hostedError(c, http.StatusTooManyRequests, limit.Error())
		}
		return s.hostedError(c, http.StatusConflict, "The project could not be created; check that its name is unique")
	}
	return c.Redirect(http.StatusSeeOther, "/projects/"+project)
}

func (s *Service) createHostedProjectRecord(ctx context.Context, credential apiCredential, name string) (project string, resultErr error) {
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := s.recheckHostedMutation(ctx, tx, nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), credential: credential}); err != nil {
		return "", err
	}
	err = tx.QueryRowContext(ctx, `SELECT p.id FROM projects p JOIN hosted_project_grants g ON g.project_id=p.id WHERE p.organization_id=? AND p.name=? AND g.user_id=? AND g.can_write=1`, s.config.Hosted.OrganizationID, name, credential.Hosted.Subject).Scan(&project)
	if err == nil {
		return project, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	stamp := s.config.now()
	before, err := s.database.hostedConsumption(ctx, tx, stamp)
	if err != nil {
		return "", err
	}
	if err := s.database.requireHostedFeature(ctx, tx, "collaboration", stamp); err != nil {
		return "", err
	}
	project = newNativeID("prj")
	states := []tracker.NativeState{{Name: "Todo", Dispatchable: true, Transitions: []string{"In Progress", "Done"}}, {Name: "In Progress", Dispatchable: true, Transitions: []string{"Todo", "Done"}}, {Name: "Done", Terminal: true, Transitions: []string{"Todo"}}}
	now := formatHubTime(s.config.now())
	encoded, err := marshalNative(states)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO projects(id,organization_id,name,profile,states_json,created_at,github_repository_enabled) VALUES (?,?,?,'native',?,?,0)", project, s.config.Hosted.OrganizationID, name, encoded, now); err != nil {
		return "", err
	}
	for _, state := range states {
		if _, err := tx.ExecContext(ctx, "INSERT INTO workflow_states(project_id,source_name,detent_state,terminal,dispatchable,created_at,updated_at) VALUES (?,?,?,?,?,?,?)", project, state.Name, state.Name, state.Terminal, state.Dispatchable, now, now); err != nil {
			return "", err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO hosted_project_grants(user_id,organization_id,project_id,can_write) VALUES (?,?,?,1)", credential.Hosted.Subject, s.config.Hosted.OrganizationID, project); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES (?,?,?)", credential.ID, s.config.Hosted.OrganizationID, project); err != nil {
		return "", err
	}
	if err := s.database.checkHostedGrowth(ctx, tx, before, stamp, false); err != nil {
		return "", err
	}
	return project, tx.Commit()
}
