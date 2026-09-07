package web

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/hubclient"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func (s *Server) changeReviewAction(c echo.Context) error {
	if credential, ok := apiCredentialFromContext(c.Request().Context()); !ok || !apikey.HasScope(credential.Scopes, apikey.ScopeWrite) || !apikey.AllowsProject(credential.ProjectIDs, c.Param("project_id")) {
		return echo.NewHTTPError(http.StatusForbidden, "Project access is required")
	}
	_, client, err := s.nativeFormData(c)
	if err != nil {
		return err
	}
	token := c.FormValue("member_token")
	if token == "" || len(token) > 4096 {
		return echo.NewHTTPError(http.StatusForbidden, "A current project member credential is required")
	}
	client = client.ArtifactReader(token)
	ctx := c.Request().Context()
	item := tracker.NativeWorkItemID(c.Param("issue_ref"))
	id, versionID := c.Param("change"), c.Param("version")
	mutation := tracker.Mutation{IdempotencyKey: c.FormValue("key")}
	action := c.FormValue("action")
	if action == "discuss" {
		result, err := client.DiscussChange(ctx, item, id, tracker.DiscussChange{Mutation: mutation, VersionID: versionID, Body: c.FormValue("body")})
		return changeActionResponse(c, result, err)
	}
	revision, err := strconv.ParseInt(c.FormValue("revision"), 10, 64)
	if err != nil || revision < 1 {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "Select an immutable review bundle")
	}
	bundle := tracker.ChangeReviewBundle{ArtifactID: c.FormValue("artifact_id"), Revision: revision, SHA256: c.FormValue("sha256"), HeadSHA: c.FormValue("head_sha")}
	switch action {
	case "load":
		detail, err := client.Change(ctx, item, id)
		if err != nil {
			return changeActionResponse(c, nil, err)
		}
		var version tracker.ChangeVersion
		for _, candidate := range detail.Versions {
			if candidate.ID == versionID {
				version = candidate
			}
		}
		refs, err := client.Artifacts(ctx, item)
		if err != nil {
			return changeActionResponse(c, nil, err)
		}
		matched := false
		for _, ref := range refs {
			matched = matched || ref.ArtifactID == bundle.ArtifactID && ref.Revision == bundle.Revision && ref.SHA256 == bundle.SHA256 && bundle.HeadSHA == version.HeadSHA && changerequest.ReviewBundleMatches(version, ref)
		}
		if !matched || version.ID == "" {
			return echo.NewHTTPError(http.StatusConflict, "Artifact does not match the selected version. Reload the change.")
		}
		grant, err := client.ArtifactGrant(ctx, item, bundle.ArtifactID, bundle.Revision)
		if err != nil {
			return changeActionResponse(c, nil, err)
		}
		viewed, err := client.ChangeViewedFiles(ctx, item, id, versionID)
		if err != nil {
			return changeActionResponse(c, nil, err)
		}
		return c.JSON(http.StatusOK, struct {
			Grant            artifact.Grant             `json:"grant"`
			Viewed           []tracker.ChangeViewedFile `json:"viewed"`
			CurrentVersionID string                     `json:"current_version_id"`
		}{grant, viewed, detail.Change.CurrentVersion})
	case "viewed":
		result, err := client.ViewChangeFile(ctx, item, id, versionID, tracker.ViewChangeFile{Mutation: mutation, Bundle: bundle, FileSHA256: c.FormValue("file_sha256"), Viewed: c.FormValue("viewed") == "true"})
		return changeActionResponse(c, result, err)
	case "approved", "changes_requested":
		result, err := client.ReviewChange(ctx, item, id, versionID, tracker.ReviewChange{Mutation: mutation, Decision: action, Body: c.FormValue("body"), ExpectedVersionID: versionID, Bundle: &bundle})
		return changeActionResponse(c, result, err)
	default:
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "Unknown review action")
	}
}

func changeActionResponse(c echo.Context, result any, err error) error {
	if err == nil {
		return c.JSON(http.StatusOK, result)
	}
	var apiErr *hubclient.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusConflict:
			return echo.NewHTTPError(http.StatusConflict, "The change or action has changed. Reload before reviewing the current version.")
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			return echo.NewHTTPError(http.StatusForbidden, "Access revoked, expired, or unavailable for this project.")
		case http.StatusGone:
			return echo.NewHTTPError(http.StatusGone, "Artifact retention has expired.")
		case http.StatusUnprocessableEntity:
			return echo.NewHTTPError(http.StatusUnprocessableEntity, "Review identity is invalid or the artifact is unavailable.")
		}
	}
	return echo.NewHTTPError(http.StatusBadGateway, "Review service is unreachable. Retry when it is available.")
}
