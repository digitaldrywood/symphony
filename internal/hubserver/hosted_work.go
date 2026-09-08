package hubserver

import (
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"strconv"

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
		err = s.hostedChanges(c, scope, &data)
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
	if errors.As(err, &failure) && failure.status == http.StatusUnprocessableEntity {
		return s.hostedError(c, http.StatusUnprocessableEntity, "This page request is invalid. Return to the project to restart navigation.")
	}
	if errors.Is(err, sql.ErrNoRows) || errors.As(err, &failure) && failure.status == http.StatusNotFound {
		return s.hostedError(c, http.StatusNotFound, "This work is unavailable to this account")
	}
	return s.hostedError(c, http.StatusServiceUnavailable, "Work information is temporarily unavailable. Retry this page.")
}

func (s *Service) hostedChanges(c echo.Context, scope nativeScope, data *templates.HostedPageData) error {
	var before int64
	if value := c.QueryParam("before"); value != "" {
		var err error
		before, err = strconv.ParseInt(value, 10, 64)
		if err != nil || before < 1 {
			return nativeInvalid("Change page cursor is invalid")
		}
	}
	rows, err := s.database.db.QueryContext(c.Request().Context(), `SELECT id, work_item_id, json_extract(record_json, '$.title'), rowid
FROM change_requests WHERE organization_id = ? AND project_id = ? AND (? = 0 OR rowid < ?) ORDER BY rowid DESC LIMIT 26`, scope.organization, scope.project, before, before)
	if err != nil {
		return err
	}
	defer rows.Close()
	var last int64
	for rows.Next() {
		if len(data.Changes) == 25 {
			data.NextChanges = "/projects/" + url.PathEscape(string(scope.project)) + "/changes?before=" + strconv.FormatInt(last, 10)
			break
		}
		var change tracker.ChangeRequest
		if err := rows.Scan(&change.ID, &change.WorkItemID, &change.Title, &last); err != nil {
			return err
		}
		data.Changes = append(data.Changes, change)
	}
	return rows.Err()
}
