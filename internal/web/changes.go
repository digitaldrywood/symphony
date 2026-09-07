package web

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/hubclient"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

func (s *Server) changePageData(c echo.Context) (templates.ChangePageData, tracker.ChangeReader, error) {
	projectID := strings.TrimSpace(c.Param("project_id"))
	tracked, ok := s.registry.Get(project.ID(projectID))
	if !ok {
		return templates.ChangePageData{}, nil, echo.NewHTTPError(http.StatusNotFound, "Project not found")
	}
	reader, ok := tracked.Connector().(tracker.ChangeReader)
	if !ok {
		return templates.ChangePageData{}, nil, echo.NewHTTPError(http.StatusNotFound, "Native changes are unavailable for this project")
	}
	dashboard, ok := s.projectDashboardData(c.Request().Context(), projectID, s.latestSnapshot(c.Request().Context()))
	if !ok {
		return templates.ChangePageData{}, nil, echo.NewHTTPError(http.StatusNotFound, "Project not found")
	}
	applyDashboardPreferences(c.Request(), &dashboard)
	nativeScopedDashboard(c, &dashboard)
	return templates.ChangePageData{Dashboard: dashboard, ProjectID: projectID, IssueID: tracker.NativeWorkItemID(c.Param("issue_ref")), ChangeID: c.Param("change"), VersionID: c.QueryParam("version")}, reader, nil
}

func (s *Server) changeDetail(c echo.Context) error {
	data, reader, err := s.changePageData(c)
	if err != nil {
		return err
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	if c.QueryParam("content") != "1" {
		data.Loading = true
		return render(c, templates.ChangePage(data))
	}
	detail, err := reader.FetchChange(c.Request().Context(), data.IssueID, data.ChangeID)
	if err != nil {
		data.Error = "Change Request could not be loaded. Retry when the Hub is available."
		var apiErr *hubclient.APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			data.Error = "Change Request was not found in this project."
		}
	} else {
		data.Detail = &detail
		s.loadArtifacts(c, &data)
	}
	return render(c, templates.ChangeContent(data))
}

func (s *Server) nativeRunDetail(c echo.Context) error {
	data, reader, err := s.changePageData(c)
	if err != nil {
		return err
	}
	attempts, err := reader.FetchNativeAttempts(c.Request().Context(), data.IssueID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadGateway, "Run history could not be loaded").SetInternal(err)
	}
	for _, attempt := range attempts {
		if attempt.AttemptID != c.Param("attempt") {
			continue
		}
		changes, err := reader.FetchChanges(c.Request().Context(), data.IssueID)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadGateway, "Linked changes could not be loaded").SetInternal(err)
		}
		s.loadArtifacts(c, &data)
		filtered := data.Artifacts[:0]
		for _, ref := range data.Artifacts {
			if ref.AttemptID == attempt.AttemptID {
				filtered = append(filtered, ref)
			}
		}
		data.Artifacts = filtered
		return render(c, templates.NativeRunPage(data, attempt, changes))
	}
	return echo.NewHTTPError(http.StatusNotFound, "Run not found")
}
