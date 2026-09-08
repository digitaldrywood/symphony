package hubserver

import (
	"database/sql"
	"errors"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

func (s *Service) hostedWork(c echo.Context) error {
	credential, status, err := s.hostedCredential(c)
	if err != nil {
		return s.hostedError(c, status, "This project is unavailable to this account")
	}
	scope := nativeScope{organization: tracker.OrganizationID(s.config.Hosted.OrganizationID), project: tracker.ProjectID(c.Param("project")), credential: credential}
	ctx := c.Request().Context()
	if err := s.requireHostedProject(ctx, s.database.db, scope, false); err != nil {
		return s.hostedError(c, http.StatusForbidden, "This project is unavailable to this account")
	}
	if err := s.database.authorizeNativeProject(ctx, scope); err != nil {
		return s.hostedWorkError(c, err)
	}
	data := templates.HostedPageData{Mode: "changes", Title: "Changes", SelectedProject: string(scope.project)}
	if err := s.hostedPageData(c, credential, &data); err != nil {
		return s.hostedWorkError(c, err)
	}
	data.CanWriteProject = s.requireHostedProject(ctx, s.database.db, scope, true) == nil
	data.SetupAPI = "/api/v2/organizations/" + url.PathEscape(string(scope.organization)) + "/projects/" + url.PathEscape(string(scope.project))
	if c.Param("item") == "" {
		data.Changes, err = changeRows[tracker.ChangeRequest](ctx, s.database.db, "SELECT record_json FROM change_requests WHERE organization_id = ? AND project_id = ? ORDER BY rowid DESC", scope.organization, scope.project)
	} else {
		var issue tracker.NativeIssue
		issue, _, err = readNativeIssue(ctx, s.database.db, scope, c.Param("item"))
		data.Issue, data.Mode, data.Title = &issue, "issue", issue.Title
		data.SetupAPI += "/work-items/" + url.PathEscape(c.Param("item"))
		if err == nil && c.Param("change") != "" {
			var detail tracker.ChangeDetail
			var tx *sql.Tx
			tx, err = s.database.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err == nil {
				defer tx.Rollback()
				detail, err = readChangeDetail(ctx, tx, scope, c.Param("item"), c.Param("change"), s.config.now())
				if err == nil {
					err = tx.Commit()
				}
			}
			data.Change, data.Mode, data.Title = &detail, "change", detail.Change.Title
		}
	}
	if err != nil {
		return s.hostedWorkError(c, err)
	}
	if err := s.hostedAudit(ctx, credential.Hosted, "action", "GET "+c.Path(), string(scope.project), http.StatusOK); err != nil {
		return s.hostedWorkError(c, err)
	}
	return s.renderHosted(c, http.StatusOK, data)
}

func (s *Service) hostedWorkError(c echo.Context, err error) error {
	var failure *nativeError
	if errors.Is(err, sql.ErrNoRows) || errors.As(err, &failure) && failure.status == http.StatusNotFound {
		return s.hostedError(c, http.StatusNotFound, "This work is unavailable to this account")
	}
	return s.hostedError(c, http.StatusServiceUnavailable, "Work information is temporarily unavailable. Retry this page.")
}
