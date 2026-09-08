package hubserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

func (s *Service) registerHostedRoutes(e *echo.Echo) {
	e.GET("/static/*", echo.WrapHandler(http.StripPrefix("/static/", http.FileServerFS(detent.StaticFS()))))
	e.GET("/", s.hostedHome)
	e.GET("/login", func(c echo.Context) error {
		return s.renderHosted(c, http.StatusOK, templates.HostedPageData{Mode: "login", Title: "Sign in"})
	})
	e.GET("/auth/oidc/start", s.startHostedLogin)
	e.GET("/auth/oidc/callback", s.completeHostedLogin)
	e.GET("/invite", s.startHostedInvitation)
	e.POST("/logout", s.logoutHosted)
	e.POST("/support/start", s.startHostedSupport)
	e.GET("/support", s.hostedSupportPage)
	e.GET("/organization", s.hostedHome)
	e.GET("/organization/plan", s.hostedPlanPage)
	e.POST("/organization/create", s.createHostedOrganization)
	e.POST("/organization/join", s.acceptHostedInvitation)
	e.POST("/organization/switch", s.switchHostedOrganization)
	e.POST("/organization/invite", s.inviteHostedMember)
	e.POST("/organization/members/:member/revoke", s.revokeHostedMember)
	e.POST("/organization/members/:member/role", s.changeHostedRole)
	e.POST("/organization/grants", s.changeHostedGrant)
	e.POST("/projects", s.createHostedProject)
	e.GET("/projects/:project", s.hostedProject)
	e.GET("/projects/:project/issues/:item", s.hostedWork)
	e.GET("/projects/:project/issues/:item/changes/:change", s.hostedWork)
	e.GET("/projects/:project/changes", s.hostedWork)
	e.GET("/projects/:project/events", s.hostedEvents)
	e.GET("/api/cloud/metadata", s.hostedMetadata)
	e.GET("/api/cloud/billing", s.hostedBilling)
	e.POST("/api/v2/organizations/:organization/entitlements", s.updateHostedPlan)
	e.POST("/api/v2/organizations/:organization/artifact-allowances/:service", s.hostedArtifactAllowances)
}

func (s *Service) renderHosted(c echo.Context, status int, data templates.HostedPageData) error {
	data.OrganizationID = s.config.Hosted.OrganizationID
	data.Assets.Favicon = "/static/img/detent-mark.svg"
	if data.OrganizationName == "" {
		data.OrganizationName = data.OrganizationID
	}
	if session, ok := c.Get("hosted_session").(auth.Session); ok {
		data.Email = session.Email
		if session.Identity != nil && session.Identity.SupportActor != "" {
			data.SupportActor, data.SupportReason = session.Identity.SupportActor, session.Identity.SupportReason
			data.SupportExpiry = session.ExpiresAt.UTC().Format(time.RFC3339)
		}
	}
	if cookie, err := c.Cookie(hostedCookie); err == nil {
		data.CSRF = hostedCSRF(cookie.Value)
	}
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	c.Response().WriteHeader(status)
	return templates.HostedPage(data).Render(c.Request().Context(), c.Response())
}

func (s *Service) hostedError(c echo.Context, status int, message string) error {
	return s.renderHosted(c, status, templates.HostedPageData{Mode: "denied", Title: "Access unavailable", Error: message})
}

func (s *Service) hostedHome(c echo.Context) error {
	session, _, err := s.hostedSession(c)
	if err != nil {
		return c.Redirect(http.StatusSeeOther, "/login")
	}
	data := templates.HostedPageData{Mode: "onboarding", Title: "Your organization", Email: session.Email}
	var members int
	if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM hosted_members").Scan(&members); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Organization information is temporarily unavailable")
	}
	data.CanCreate = members == 0 && session.Identity.Subject == s.config.Hosted.BootstrapSubject && session.Identity.SupportActor == "" && !hostedEmailListed(s.config.Hosted.StaffEmails, session.Email)
	data.CanSupport = session.Identity.SupportActor == "" && hostedEmailListed(s.config.Hosted.SupportActors, session.Email)
	if hostedEmailListed(s.config.Hosted.StaffEmails, session.Email) && session.Identity.SupportActor == "" {
		data.Mode = "organization"
		data.Notice = "Staff access is limited to account and usage metadata. Customer content requires authorized temporary support access."
		return s.renderHosted(c, http.StatusOK, data)
	}
	credential, _, accessErr := s.hostedCredential(c)
	if accessErr != nil {
		data.Notice = "Create your reserved organization, join with an invitation token, or switch to an organization you belong to."
	} else {
		data.Mode = "organization"
		data.CanManage = credential.HostedRole == "owner" || credential.HostedRole == "admin"
		data.CanManageOwnership = credential.HostedRole == "owner"
		data.CanCreate = data.CanManage
		if err := s.hostedPageData(c, credential, &data); err != nil {
			return s.hostedError(c, http.StatusServiceUnavailable, "Organization information is temporarily unavailable")
		}
	}
	if session.Identity.SupportActor == "" {
		memberships, err := s.config.Hosted.Provider.Memberships(c.Request().Context(), session.Identity.Subject, "")
		if err != nil {
			return s.hostedError(c, http.StatusServiceUnavailable, "Organization membership is temporarily unavailable")
		}
		for _, destination := range s.config.Hosted.Directory {
			for _, membership := range memberships {
				if membership.OrganizationID == destination.WorkOSOrganizationID && membership.UserID == session.Identity.Subject && membership.Status == "active" {
					data.Organizations = append(data.Organizations, templates.HostedOrganizationChoice{ID: destination.OrganizationID, Name: destination.OrganizationID})
				}
			}
		}
	}
	return s.renderHosted(c, http.StatusOK, data)
}

func (s *Service) hostedPageData(c echo.Context, credential apiCredential, data *templates.HostedPageData) error {
	identity := credential.Hosted
	if identity.SupportActor != "" {
		data.SupportActor, data.SupportReason = identity.SupportActor, identity.SupportReason
		data.SupportExpiry = identity.ExpiresAt.UTC().Format(time.RFC3339)
	}
	if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT name FROM organizations WHERE id = ?", s.config.Hosted.OrganizationID).Scan(&data.OrganizationName); err != nil {
		return err
	}
	rows, err := s.database.db.QueryContext(c.Request().Context(), `SELECT p.id,p.name,g.manage_runner FROM projects p JOIN hosted_project_grants g ON g.project_id = p.id WHERE g.user_id = ? AND p.organization_id = ? ORDER BY p.id`, identity.Subject, s.config.Hosted.OrganizationID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var project templates.HostedProjectChoice
		var runner bool
		if err := rows.Scan(&project.ID, &project.Name, &runner); err != nil {
			return errors.Join(err, rows.Close())
		}
		data.Projects = append(data.Projects, project)
		data.CanManageRunners = data.CanManageRunners || runner
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !data.CanManage {
		return nil
	}
	members, err := s.config.Hosted.Provider.Memberships(c.Request().Context(), "", identity.OrganizationID)
	if err != nil {
		return err
	}
	for _, member := range members {
		if member.OrganizationID == identity.OrganizationID && member.Status == "active" {
			data.Members = append(data.Members, templates.HostedMember{ID: member.ID, UserID: member.UserID, Role: member.Role.Slug})
		}
	}
	return nil
}

func (s *Service) hostedProject(c echo.Context) error {
	credential, status, err := s.hostedCredential(c)
	if err != nil {
		return s.hostedError(c, status, "This project is unavailable to this account")
	}
	scope := nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), project: tracker.ProjectID(c.Param("project")), credential: credential}
	if err := s.requireHostedProject(c.Request().Context(), s.database.db, scope, false); err != nil {
		return s.hostedError(c, http.StatusForbidden, "This project is unavailable to this account")
	}
	data := templates.HostedPageData{Mode: "project", Title: "Project", SelectedProject: string(scope.project)}
	if session, ok := c.Get("hosted_session").(auth.Session); ok {
		data.Email = session.Email
	}
	if err := s.hostedPageData(c, credential, &data); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Project information is temporarily unavailable")
	}
	setup, err := s.projectOnboarding(c.Request().Context(), scope)
	if err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Project readiness is temporarily unavailable. Retry without recreating the project.")
	}
	data.Setup = &setup
	data.SetupAPI = "/api/v2/organizations/" + string(scope.organization) + "/projects/" + string(scope.project)
	data.CanWriteProject = s.requireHostedProject(c.Request().Context(), s.database.db, scope, true) == nil
	data.CanManage = credential.HostedRole == "owner" || credential.HostedRole == "admin"
	data.CanManageRunners = credential.HostedRole != "viewer" && s.hostedAllRunnerGrants(c.Request().Context(), credential)
	project, err := readNativeProject(c.Request().Context(), s.database.db, scope)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	data.Title = project.Name
	data.ProjectStates = project.States
	integration, err := readProjectIntegration(c.Request().Context(), s.database.db, scope)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	data.IntegrationRevision = fmt.Sprint(integration.Revision)
	data.GitHubRepository, data.GitHubIntake, data.GitHubProjection = integration.Repository, integration.Intake, integration.Projection
	data.GitHubPR = integration.RepositoryEnabled
	data.GitHubAvailable = s.config.ReconcileBackend != nil
	data.IntegrationSummary = fmt.Sprintf("Profile: %s · GitHub intake: %s · projection: %s · repository/PR integration: %t", integration.Profile, integration.Intake, integration.Projection, integration.RepositoryEnabled)
	rows, err := s.database.db.QueryContext(c.Request().Context(), `SELECT i.native_id,i.number,i.title,COALESCE(w.source_name,'') FROM issues i LEFT JOIN workflow_states w ON w.id = i.workflow_state_id WHERE i.organization_id = ? AND i.project_id = ? ORDER BY i.number DESC LIMIT 100`, scope.organization, scope.project)
	if err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Project information is temporarily unavailable")
	}
	defer rows.Close()
	for rows.Next() {
		var issue tracker.NativeIssue
		if err := rows.Scan(&issue.WorkItemID, &issue.Number, &issue.Title, &issue.State); err != nil {
			closeErr := rows.Close()
			return s.nativeAPIError(c, errors.Join(err, closeErr))
		}
		issue.OrganizationID, issue.ProjectID = scope.organization, scope.project
		data.Issues = append(data.Issues, issue)
	}
	if err := rows.Close(); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := rows.Err(); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "action", "GET /projects/:project", string(scope.project), http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	return s.renderHosted(c, http.StatusOK, data)
}

func (s *Service) hostedEvents(c echo.Context) error {
	initial, status, err := s.hostedCredential(c)
	if err != nil {
		return c.NoContent(status)
	}
	initialScope := nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), project: tracker.ProjectID(c.Param("project")), credential: initial}
	if err := s.requireHostedProject(c.Request().Context(), s.database.db, initialScope, false); err != nil {
		return c.NoContent(http.StatusForbidden)
	}
	if err := s.hostedAudit(c.Request().Context(), initial.Hosted, "action", "GET /projects/:project/events", string(initialScope.project), http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	c.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		credential, _, err := s.hostedCredential(c)
		if err != nil {
			return nil
		}
		scope := nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), project: tracker.ProjectID(c.Param("project")), credential: credential}
		if err := s.requireHostedProject(c.Request().Context(), s.database.db, scope, false); err != nil {
			return nil
		}
		var sequence int64
		if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT COALESCE(MAX(event_sequence),0) FROM issues WHERE organization_id = ? AND project_id = ?", scope.organization, scope.project).Scan(&sequence); err != nil {
			return nil
		}
		if _, err := fmt.Fprintf(c.Response(), "event: activity\ndata: %d\n\n", sequence); err != nil {
			return nil
		}
		c.Response().Flush()
		select {
		case <-c.Request().Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) hostedMetadata(c echo.Context) error {
	if c.Request().Header.Get(echo.HeaderAuthorization) != "" {
		credential, _, err := s.authenticateAPIRequest(c)
		if err != nil || credential.ID != bootstrapTokenID || credential.Scope != apiScopeAdmin {
			return c.NoContent(http.StatusForbidden)
		}
	} else {
		session, _, err := s.hostedSession(c)
		if err != nil || session.Identity.SupportActor != "" || !hostedEmailListed(s.config.Hosted.StaffEmails, session.Email) {
			return c.NoContent(http.StatusForbidden)
		}
	}
	report, err := s.database.hostedMetadata(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, apiErrorResponse{Code: "metadata_unavailable", Message: "Service metadata is temporarily unavailable"})
	}
	usage, err := s.hostedUsage(c.Request().Context(), report)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, usage)
}

type hostedUsageReport struct {
	Entitlement HostedEntitlement `json:"entitlement"`
	HostedMetadata
	PlanID            string `json:"plan_id"`
	StorageQuotaBytes int64  `json:"storage_quota_bytes,omitempty"`
	EventQuota        int64  `json:"event_quota,omitempty"`
}

func (s *Service) hostedUsage(ctx context.Context, report HostedMetadata) (hostedUsageReport, error) {
	entitlement, err := s.database.hostedPlanUsage(ctx, s.config.now())
	return hostedUsageReport{HostedMetadata: report, Entitlement: entitlement, PlanID: entitlement.EffectiveBase.ID, StorageQuotaBytes: entitlement.Allowances["collaboration_bytes"], EventQuota: entitlement.Allowances["ingested_events"]}, err
}

func (s *Service) hostedBilling(c echo.Context) error {
	credential, _, err := s.hostedCredential(c)
	if err != nil || credential.HostedRole != "owner" {
		return s.nativeAPIError(c, auth.ErrHostedIdentity)
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "billing_viewed", "GET /api/cloud/billing", "", http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	report, err := s.database.hostedMetadata(c.Request().Context())
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	usage, err := s.hostedUsage(c.Request().Context(), report)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, usage)
}

func hostedFormTrue(c echo.Context, key string) bool {
	return strings.EqualFold(c.FormValue(key), "true")
}
