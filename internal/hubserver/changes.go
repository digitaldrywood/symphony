package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const changeBase = nativeBase + "/work-items/:item/changes"

func (s *Service) registerChangeRoutes(e *echo.Echo) {
	read := s.requireNativeScope(apiScopeWorker, apiScopeOperator)
	write := s.requireNativeScope(apiScopeWorker, apiScopeOperator)
	operator := s.requireNativeScope(apiScopeOperator)
	e.GET(nativeBase+"/change-review-policy", s.getChangeReviewPolicy, read)
	e.PUT(nativeBase+"/change-review-policy", s.approveChangeReviewPolicy, s.requireChangeReviewPolicyAdmin())
	e.GET(changeBase, s.listChanges, read)
	e.POST(changeBase, s.createChange, write)
	e.GET(changeBase+"/:change", s.getChange, read)
	e.POST(changeBase+"/:change/versions", s.publishChangeVersion, write)
	e.POST(changeBase+"/:change/discussion", s.discussChange, write)
	e.POST(changeBase+"/:change/versions/:version/reviews", s.reviewChange, operator)
	e.GET(changeBase+"/:change/versions/:version/viewed-files", s.changeViewedFiles, operator)
	e.POST(changeBase+"/:change/versions/:version/viewed-files", s.viewChangeFile, operator)
	e.POST(changeBase+"/:change/versions/:version/checks", s.submitChangeCheck, write)
}

func (s *Service) requireChangeReviewPolicyAdmin() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		if s.config.Hosted == nil {
			return s.requireInstanceAdmin()(s.requireNativeScope(apiScopeAdmin)(next))
		}
		return s.requireOnboardingAdmin()(func(c echo.Context) error {
			scope := nativeRequestScope(c)
			scope.requireHostedAdmin = true
			c.Set("native_scope", scope)
			return next(c)
		})
	}
}

func readChangePolicy(ctx context.Context, query nativeQueryer, scope nativeScope) (tracker.ChangeReviewPolicy, error) {
	var result tracker.ChangeReviewPolicy
	var raw string
	if err := query.QueryRowContext(ctx, "SELECT policy_json FROM change_review_policies WHERE organization_id = ? AND project_id = ?", scope.organization, scope.project).Scan(&raw); err != nil {
		return result, err
	}
	err := json.Unmarshal([]byte(raw), &result)
	return result, err
}

func (s *Service) getChangeReviewPolicy(c echo.Context) error {
	result, err := readChangePolicy(c.Request().Context(), s.database.db, nativeRequestScope(c))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) approveChangeReviewPolicy(c echo.Context) error {
	var request tracker.ApproveChangeReviewPolicy
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, _ time.Time) (any, error) {
		approved, err := readProjectPolicy(ctx, tx, string(scope.organization)+"/"+string(scope.project))
		if err != nil {
			return nil, err
		}
		if err := changerequest.ValidatePolicy(request.Policy, approved.Policy); err != nil {
			return nil, nativeInvalid(err.Error())
		}
		for _, check := range request.Policy.RequiredChecks {
			var role string
			err := tx.QueryRowContext(ctx, `SELECT scope FROM api_tokens t WHERE id = ? AND revoked_at IS NULL
AND EXISTS (SELECT 1 FROM token_grants g WHERE g.token_id = t.id AND g.organization_id = ? AND g.project_id = ?)`, check.PrincipalID, scope.organization, scope.project).Scan(&role)
			if errors.Is(err, sql.ErrNoRows) || err == nil && check.Source == "independent" && role == string(apiScopeWorker) {
				return nil, nativeInvalid("Expected CI principals must have a project grant; independent validation requires an operator credential")
			}
			if err != nil {
				return nil, err
			}
		}
		previous, err := readChangePolicy(ctx, tx, scope)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if previous.ID != request.ExpectedID {
			return nil, policyMismatch("Review policy changed; supply its current identity")
		}
		rules := request.Policy
		rules.ID = changerequest.PolicyID(rules)
		raw, err := marshalNative(rules)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO change_review_policies (organization_id, project_id, policy_json) VALUES (?, ?, ?)
ON CONFLICT (organization_id, project_id) DO UPDATE SET policy_json = excluded.policy_json`, scope.organization, scope.project, raw)
		return rules, err
	})
}

func readChange(ctx context.Context, query nativeQueryer, scope nativeScope, item, id string) (tracker.ChangeRequest, error) {
	var change tracker.ChangeRequest
	var raw string
	err := query.QueryRowContext(ctx, `SELECT c.record_json FROM change_requests c JOIN change_issue_links l ON l.change_id = c.id
WHERE c.organization_id = ? AND c.project_id = ? AND l.work_item_id = ? AND c.id = ?`, scope.organization, scope.project, item, id).Scan(&raw)
	if err != nil {
		return change, err
	}
	err = json.Unmarshal([]byte(raw), &change)
	return change, err
}

func readChangeVersion(ctx context.Context, query nativeQueryer, changeID, versionID string) (tracker.ChangeVersion, error) {
	var version tracker.ChangeVersion
	var raw string
	if err := query.QueryRowContext(ctx, "SELECT record_json FROM change_versions WHERE change_id = ? AND id = ?", changeID, versionID).Scan(&raw); err != nil {
		return version, err
	}
	err := json.Unmarshal([]byte(raw), &version)
	return version, err
}

func changeRows[T any](ctx context.Context, query nativeQueryer, statement string, args ...any) ([]T, error) {
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []T{}
	for rows.Next() {
		var raw string
		var item T
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Service) listChanges(c echo.Context) error {
	scope := nativeRequestScope(c)
	if _, _, err := readNativeIssue(c.Request().Context(), s.database.db, scope, c.Param("item")); err != nil {
		return s.nativeAPIError(c, err)
	}
	result, err := changeRows[tracker.ChangeRequest](c.Request().Context(), s.database.db, `SELECT c.record_json FROM change_requests c JOIN change_issue_links l ON l.change_id = c.id
WHERE c.organization_id = ? AND c.project_id = ? AND l.work_item_id = ? ORDER BY c.rowid`, scope.organization, scope.project, c.Param("item"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) createChange(c echo.Context) error {
	var request tracker.CreateChange
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) (any, error) {
		if strings.TrimSpace(request.Title) == "" || len(request.Title) > 512 || len(request.Body) > 64<<10 || len(request.LinkedIssues) > 32 {
			return nil, nativeInvalid("Change requires a title up to 512 bytes, discussion up to 64 KiB, and at most 32 linked issues")
		}
		change := tracker.ChangeRequest{ID: newNativeID("change"), OrganizationID: scope.organization, ProjectID: scope.project, WorkItemID: tracker.NativeWorkItemID(c.Param("item")), Title: request.Title, Body: request.Body, Revision: 1, CreatedAt: now, UpdatedAt: now}
		change.LinkedIssues = append([]tracker.NativeWorkItemID{change.WorkItemID}, request.LinkedIssues...)
		slices.Sort(change.LinkedIssues)
		change.LinkedIssues = slices.Compact(change.LinkedIssues)
		for _, id := range change.LinkedIssues {
			if _, _, err := readNativeIssue(ctx, tx, scope, string(id)); err != nil {
				return nil, err
			}
		}
		raw, err := marshalNative(change)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO change_requests (id, organization_id, project_id, work_item_id, record_json) VALUES (?, ?, ?, ?, ?)", change.ID, scope.organization, scope.project, change.WorkItemID, raw); err != nil {
			return nil, err
		}
		for _, id := range change.LinkedIssues {
			if _, err := tx.ExecContext(ctx, "INSERT INTO change_issue_links VALUES (?, ?, ?, ?)", change.ID, scope.organization, scope.project, id); err != nil {
				return nil, err
			}
			if err := appendNativeHistory(ctx, tx, scope, string(id), "change.created", tracker.CollaborationData{Change: &tracker.NativeChangeReference{ChangeID: change.ID}}, now); err != nil {
				return nil, err
			}
		}
		return change, nil
	})
}

func (s *Service) getChange(c echo.Context) error {
	scope, ctx := nativeRequestScope(c), c.Request().Context()
	tx, err := s.database.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	result, err := readChangeDetail(ctx, tx, scope, c.Param("item"), c.Param("change"), s.config.now())
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func readChangeDetail(ctx context.Context, query nativeQueryer, scope nativeScope, item, id string, now time.Time) (tracker.ChangeDetail, error) {
	var result tracker.ChangeDetail
	var err error
	result.Change, err = readChange(ctx, query, scope, item, id)
	if err != nil {
		return result, err
	}
	result.Versions, err = changeRows[tracker.ChangeVersion](ctx, query, "SELECT record_json FROM change_versions WHERE change_id = ? ORDER BY number", id)
	if err != nil {
		return result, err
	}
	result.Reviews, err = changeRows[tracker.ChangeReview](ctx, query, "SELECT record_json FROM change_evidence WHERE change_id = ? AND kind = 'review' ORDER BY sequence", id)
	if err != nil {
		return result, err
	}
	result.Checks, err = changeRows[tracker.ChangeCheck](ctx, query, "SELECT record_json FROM change_evidence WHERE change_id = ? AND kind = 'check' ORDER BY sequence", id)
	if err != nil {
		return result, err
	}
	result.Discussion, err = changeRows[tracker.ChangeDiscussion](ctx, query, "SELECT record_json FROM change_evidence WHERE change_id = ? AND kind = 'discussion' ORDER BY sequence", id)
	if err != nil {
		return result, err
	}
	var approvedID string
	err = query.QueryRowContext(ctx, "SELECT policy_id FROM project_policies WHERE scope = ?", string(scope.organization)+"/"+string(scope.project)).Scan(&approvedID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	rules, err := readChangePolicy(ctx, query, scope)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	result.Summary = changerequest.Summarize(result, approvedID, rules.ID, now)
	err = loadChangeExternal(ctx, query, scope, &result)
	return result, err
}

func loadChangeExternal(ctx context.Context, query nativeQueryer, scope nativeScope, detail *tracker.ChangeDetail) error {
	for _, version := range detail.Versions {
		if version.ID != detail.Change.CurrentVersion || version.External == nil {
			continue
		}
		var issueID tracker.WorkItemID
		err := query.QueryRowContext(ctx, `SELECT pr.issue_id FROM pull_requests pr JOIN projects p ON p.repository_id = pr.repository_id
WHERE p.organization_id = ? AND p.id = ? AND pr.url = ? AND pr.issue_id IS NOT NULL`, scope.organization, scope.project, version.External.URL).Scan(&issueID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		record := &tracker.Record{}
		if err := loadWorkItemPullRequests(ctx, query, map[tracker.WorkItemID]*tracker.Record{issueID: record}, []tracker.WorkItemID{issueID}); err != nil {
			return err
		}
		for _, pr := range record.PullRequests {
			if pr.URL == version.External.URL {
				detail.External = &pr
				if pr.HeadSHA != version.HeadSHA {
					detail.Summary.ExternalReview = "stale_head"
				} else if pr.Reviews.Decision != "" {
					detail.Summary.ExternalReview = "snapshot: " + pr.Reviews.Decision
				}
				return nil
			}
		}
	}
	return nil
}
