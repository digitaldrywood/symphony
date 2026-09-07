package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func validateReviewBundle(ctx context.Context, tx *sql.Tx, scope nativeScope, change tracker.ChangeRequest, version tracker.ChangeVersion, bundle tracker.ChangeReviewBundle, now time.Time) error {
	if !artifact.ValidID(bundle.ArtifactID, "artifact") || bundle.Revision < 1 || !artifact.ValidHash(bundle.SHA256, 64) || bundle.HeadSHA != version.HeadSHA {
		return nativeInvalid("Review bundle identity does not match the version")
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT reference_json FROM artifact_references WHERE organization_id=? AND project_id=? AND work_item_id=? AND artifact_id=? AND revision=?", scope.organization, scope.project, change.WorkItemID, bundle.ArtifactID, bundle.Revision).Scan(&raw); err != nil {
		return err
	}
	var ref artifact.Reference
	if err := json.Unmarshal(raw, &ref); err != nil {
		return err
	}
	if ref.SHA256 != bundle.SHA256 || !changerequest.ReviewBundleMatches(version, ref) || !now.Before(ref.ExpiresAt) || ref.Availability != "available" {
		return nativeInvalid("Review bundle is unavailable or does not match the version")
	}
	return nil
}

func (s *Service) changeViewedFiles(c echo.Context) error {
	ctx, scope := c.Request().Context(), nativeRequestScope(c)
	change, err := readChange(ctx, s.database.db, scope, c.Param("item"), c.Param("change"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	version, err := readChangeVersion(ctx, s.database.db, change.ID, c.Param("version"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	rows, err := s.database.db.QueryContext(ctx, "SELECT manifest_sha256, file_sha256, viewed FROM change_viewed_files WHERE version_id=? AND principal_id=? ORDER BY manifest_sha256, file_sha256 LIMIT 4096", version.ID, scope.credential.ID)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer rows.Close()
	result := []tracker.ChangeViewedFile{}
	for rows.Next() {
		var value tracker.ChangeViewedFile
		if err := rows.Scan(&value.ManifestSHA256, &value.FileSHA256, &value.Viewed); err != nil {
			return s.nativeAPIError(c, err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) viewChangeFile(c echo.Context) error {
	var request tracker.ViewChangeFile
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) (any, error) {
		change, err := readChange(ctx, tx, scope, c.Param("item"), c.Param("change"))
		if err != nil {
			return nil, err
		}
		version, err := readChangeVersion(ctx, tx, change.ID, c.Param("version"))
		if err != nil {
			return nil, err
		}
		if !artifact.ValidHash(request.FileSHA256, 64) {
			return nil, nativeInvalid("An opaque file digest is required")
		}
		if err := validateReviewBundle(ctx, tx, scope, change, version, request.Bundle, now); err != nil {
			return nil, err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM change_viewed_files WHERE version_id=? AND principal_id=? AND NOT (manifest_sha256=? AND file_sha256=?)", version.ID, scope.credential.ID, request.Bundle.SHA256, request.FileSHA256).Scan(&count); err != nil {
			return nil, err
		}
		if count >= 4096 {
			return nil, nativeInvalid("Viewed file limit reached for this version")
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO change_viewed_files(version_id,principal_id,manifest_sha256,file_sha256,viewed) VALUES(?,?,?,?,?) ON CONFLICT(version_id,principal_id,manifest_sha256,file_sha256) DO UPDATE SET viewed=excluded.viewed", version.ID, scope.credential.ID, request.Bundle.SHA256, request.FileSHA256, request.Viewed)
		return tracker.ChangeViewedFile{ManifestSHA256: request.Bundle.SHA256, FileSHA256: request.FileSHA256, Viewed: request.Viewed}, err
	})
}
