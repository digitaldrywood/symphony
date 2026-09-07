package web

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

func (s *Server) loadArtifacts(c echo.Context, data *templates.ChangePageData) {
	tracked, ok := s.registry.Get(project.ID(data.ProjectID))
	if !ok {
		return
	}
	source, ok := tracked.Connector().(nativeClientSource)
	if !ok || source.NativeClient() == nil {
		return
	}
	refs, err := source.NativeClient().Artifacts(c.Request().Context(), data.IssueID)
	if err != nil {
		data.ArtifactError = "Artifact availability could not be loaded. Retry when the Hub is reachable."
		return
	}
	data.Artifacts = refs
	if data.Detail != nil {
		data.Artifacts = nil
		if version, ok := data.Version(); ok {
			for _, ref := range refs {
				matches := ref.SHA256 == version.Code.SHA256
				for _, pinned := range version.Artifacts {
					matches = matches || pinned.SHA256 == ref.SHA256
				}
				if ref.VersionID == version.ID || ref.VersionID == "" && matches && ref.AttemptID == version.AttemptID && ref.RunID == version.RunID {
					data.Artifacts = append(data.Artifacts, ref)
				}
			}
		}
	}
	data.ArtifactFormToken = s.apiKeyDashboardToken()
}

func (s *Server) artifactAccess(c echo.Context) error {
	_, client, err := s.nativeFormData(c)
	if err != nil {
		return err
	}
	token := c.FormValue("member_token")
	if token == "" || len(token) > 4096 {
		return echo.NewHTTPError(http.StatusForbidden, "A current project member credential is required")
	}
	revision, err := artifact.Revision(c.FormValue("revision"))
	if err != nil {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "Invalid artifact revision")
	}
	grant, err := client.ArtifactReader(token).ArtifactGrant(c.Request().Context(), tracker.NativeWorkItemID(c.Param("issue_ref")), c.Param("artifact"), revision)
	if err != nil {
		return echo.NewHTTPError(http.StatusForbidden, "Artifact access is unavailable or your project permission was revoked")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, grant)
}
