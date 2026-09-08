package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func insertChangeEvidence(ctx context.Context, tx *sql.Tx, changeID, versionID, kind, sourceKey string, value any) error {
	raw, err := marshalNative(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO change_evidence (change_id, version_id, kind, source_key, record_json) VALUES (?, NULLIF(?, ''), ?, NULLIF(?, ''), ?)", changeID, versionID, kind, sourceKey, raw)
	return err
}

func (s *Service) reviewChange(c echo.Context) error {
	var request tracker.ReviewChange
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
		if request.Decision == "approved" && change.CurrentVersion != version.ID || request.ExpectedVersionID != "" && (request.ExpectedVersionID != version.ID || change.CurrentVersion != version.ID) {
			return nil, nativeConflict(change.Revision)
		}
		if request.Bundle != nil {
			if err := validateReviewBundle(ctx, tx, scope, change, version, *request.Bundle, now); err != nil {
				return nil, err
			}
		}
		if !slices.Contains([]string{"approved", "changes_requested", "commented"}, request.Decision) || len(request.Body) > 64<<10 {
			return nil, nativeInvalid("Review decision is invalid or body exceeds 64 KiB")
		}
		review := tracker.ChangeReview{ID: newNativeID("review"), VersionID: c.Param("version"), Decision: request.Decision, Body: request.Body, Actor: scope.actor(), CreatedAt: now}
		return review, insertChangeEvidence(ctx, tx, change.ID, review.VersionID, "review", "", review)
	})
}

func (s *Service) submitChangeCheck(c echo.Context) error {
	var request tracker.SubmitChangeCheck
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	if s.config.Hosted != nil && request.Source != "independent" {
		return s.nativeAPIError(c, nativeNotFound())
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
		if err := changerequest.ValidateResult(version, request.ChangeCheckResult, scope.credential.ID, scope.credential.Scope == apiScopeWorker); err != nil {
			return nil, nativeInvalid(err.Error())
		}
		if request.CompletedAt.After(now) {
			return nil, nativeInvalid("CI completion time cannot be in the future")
		}
		var previous string
		err = tx.QueryRowContext(ctx, "SELECT record_json FROM change_evidence WHERE change_id = ? AND kind = 'check' AND source_key = ?", change.ID, request.CheckRunID).Scan(&previous)
		if err == nil {
			var result tracker.ChangeCheck
			if err := json.Unmarshal([]byte(previous), &result); err != nil {
				return nil, err
			}
			oldContent, err := marshalNative(result.ChangeCheckResult)
			if err != nil {
				return nil, err
			}
			newContent, err := marshalNative(request.ChangeCheckResult)
			if err != nil {
				return nil, err
			}
			if oldContent != newContent || result.Actor.PrincipalID != scope.credential.ID {
				return nil, nativeConflict(change.Revision)
			}
			return result, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		check := tracker.ChangeCheck{ChangeCheckResult: request.ChangeCheckResult, VersionID: version.ID, Actor: scope.actor(), ReceivedAt: now}
		return check, insertChangeEvidence(ctx, tx, change.ID, version.ID, "check", check.CheckRunID, check)
	})
}

func (s *Service) discussChange(c echo.Context) error {
	var request tracker.DiscussChange
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) (any, error) {
		change, err := readChange(ctx, tx, scope, c.Param("item"), c.Param("change"))
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(request.Body) == "" || len(request.Body) > 64<<10 {
			return nil, nativeInvalid("Discussion requires 1 byte to 64 KiB")
		}
		if request.VersionID != "" {
			if _, err := readChangeVersion(ctx, tx, change.ID, request.VersionID); err != nil {
				return nil, err
			}
		}
		if err := validateNativeProvenance(scope, request.Provenance); err != nil {
			return nil, err
		}
		sourceKey := ""
		if request.Provenance != nil {
			sourceKey = request.Provenance.Provider + ":" + request.Provenance.ExternalID
			var raw string
			err := tx.QueryRowContext(ctx, "SELECT record_json FROM change_evidence WHERE change_id = ? AND kind = 'discussion' AND source_key = ?", change.ID, sourceKey).Scan(&raw)
			if err == nil {
				var existing tracker.ChangeDiscussion
				err = json.Unmarshal([]byte(raw), &existing)
				return existing, err
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		comment := tracker.ChangeDiscussion{ID: newNativeID("cmt"), VersionID: request.VersionID, Body: request.Body, Actor: scope.actor(), Provenance: request.Provenance, CreatedAt: now}
		return comment, insertChangeEvidence(ctx, tx, change.ID, comment.VersionID, "discussion", sourceKey, comment)
	})
}
