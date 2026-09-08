package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/tracker"
)

type GitHubImport struct {
	EditSequence    int64            `json:"edit_sequence,string"`
	IntakePending   bool             `json:"intake_pending"`
	ID              string           `json:"import_id"`
	IssueNumber     int              `json:"issue_number"`
	WorkItemID      string           `json:"work_item_id,omitempty"`
	Stage           string           `json:"stage"`
	Cursor          string           `json:"cursor,omitempty"`
	Revision        tracker.Revision `json:"revision,string"`
	Pages           int              `json:"pages"`
	Status          string           `json:"status"`
	Gaps            []string         `json:"gaps"`
	LastError       string           `json:"last_error,omitempty"`
	RetryAfter      *time.Time       `json:"retry_after,omitempty"`
	SourceUpdatedAt string           `json:"source_updated_at,omitempty"`
	ObservedAt      string           `json:"observed_at"`
}

type GitHubImportRequest struct {
	SourceID    string
	Repository  string
	Profile     string
	IssueNumber int
	Stage       string
	Cursor      string
}

type GitHubImportRecord struct {
	CurrentDependency *bool              `json:"current_dependency,omitempty"`
	SourceKey         string             `json:"source_key"`
	Kind              string             `json:"kind"`
	Data              json.RawMessage    `json:"data"`
	Provenance        tracker.Provenance `json:"provenance"`
	Body              string             `json:"body,omitempty"`
	DependencyID      string             `json:"dependency_id,omitempty"`
}

type GitHubImportPage struct {
	RetryAt       time.Time
	EditSequence  int64
	EditsFinished bool
	Issue         *IssueSource
	Records       []GitHubImportRecord
	NextCursor    string
	Gaps          []string
}

type ImportBackend interface {
	FetchImportPage(context.Context, GitHubImportRequest) (GitHubImportPage, error)
}

func readGitHubImport(ctx context.Context, query nativeQueryer, scope nativeScope, id string) (GitHubImport, error) {
	var result GitHubImport
	var gaps string
	var retry sql.NullString
	err := query.QueryRowContext(ctx, `SELECT g.id, g.issue_number, COALESCE(g.work_item_id, ''), g.stage, g.cursor, g.revision, g.pages, g.status, g.gaps_json, g.last_error, g.retry_after, g.source_updated_at, g.observed_at, g.intake_pending, g.edit_sequence
FROM github_imports g JOIN projects p ON p.id = g.project_id WHERE p.organization_id = ? AND p.id = ? AND g.id = ?`, scope.organization, scope.project, id).Scan(
		&result.ID, &result.IssueNumber, &result.WorkItemID, &result.Stage, &result.Cursor, &result.Revision, &result.Pages, &result.Status, &gaps, &result.LastError, &retry, &result.SourceUpdatedAt, &result.ObservedAt, &result.IntakePending, &result.EditSequence)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal([]byte(gaps), &result.Gaps); err != nil {
		return result, err
	}
	if retry.Valid {
		value, err := parseTimeValue(retry.String)
		if err != nil {
			return result, err
		}
		result.RetryAfter = &value
	}
	return result, nil
}

func (s *Service) startGitHubImport(c echo.Context) error {
	var request struct {
		tracker.Mutation
		IssueNumber      int              `json:"issue_number"`
		Restart          bool             `json:"restart"`
		ExpectedRevision tracker.Revision `json:"expected_revision,string"`
	}
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) (any, error) {
		integration, err := readProjectIntegration(ctx, tx, scope)
		if err != nil {
			return nil, err
		}
		if integration.Intake != "manual" || integration.RepositoryID == 0 || request.IssueNumber <= 0 {
			return nil, nativeInvalid("Enable manual intake on a repository-backed project and provide a positive issue number")
		}
		var id string
		err = tx.QueryRowContext(ctx, "SELECT id FROM github_imports WHERE project_id = ? AND issue_number = ?", scope.project, request.IssueNumber).Scan(&id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if id == "" {
			id = newNativeID("imp")
			_, err = tx.ExecContext(ctx, "INSERT INTO github_imports (id, project_id, issue_number, observed_at) VALUES (?, ?, ?, ?)", id, scope.project, request.IssueNumber, formatHubTime(now))
			if err != nil {
				return nil, err
			}
		} else if request.Restart {
			current, err := readGitHubImport(ctx, tx, scope, id)
			if err != nil {
				return nil, err
			}
			if current.Revision != request.ExpectedRevision {
				return nil, nativeConflict(current.Revision)
			}
			_, err = tx.ExecContext(ctx, "UPDATE github_imports SET stage = 'issue', cursor = '', edit_sequence = 0, status = 'pending', gaps_json = '[]', last_error = '', retry_after = NULL, revision = revision + 1 WHERE id = ?", id)
			if err != nil {
				return nil, err
			}
		}
		return readGitHubImport(ctx, tx, scope, id)
	})
}

func (s *Service) getGitHubImport(c echo.Context) error {
	result, err := readGitHubImport(c.Request().Context(), s.database.db, nativeRequestScope(c), c.Param("import"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) advanceGitHubImport(c echo.Context) error {
	var request struct {
		ExpectedRevision tracker.Revision `json:"expected_revision,string"`
	}
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	if s.config.ImportBackend == nil {
		return s.nativeAPIError(c, nativeInvalid("GitHub import transport is not configured"))
	}
	ctx, scope := c.Request().Context(), nativeRequestScope(c)
	current, err := readGitHubImport(ctx, s.database.db, scope, c.Param("import"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if current.Revision != request.ExpectedRevision {
		return s.nativeAPIError(c, nativeConflict(current.Revision))
	}
	if current.Stage == "finished" {
		return c.JSON(http.StatusOK, current)
	}
	if current.RetryAfter != nil && s.config.now().Before(*current.RetryAfter) {
		return c.JSON(http.StatusTooManyRequests, current)
	}
	integration, err := readProjectIntegration(ctx, s.database.db, scope)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if integration.Intake != "manual" {
		return s.nativeAPIError(c, nativeInvalid("Manual intake is disabled"))
	}
	page, fetchErr := s.fetchImportPage(ctx, integration, current)
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	latest, err := readGitHubImport(ctx, tx, scope, current.ID)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if latest.Revision != current.Revision {
		return s.nativeAPIError(c, nativeConflict(latest.Revision))
	}
	active, err := readProjectIntegration(ctx, tx, scope)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if active.Revision != integration.Revision {
		return s.nativeAPIError(c, nativeConflict(active.Revision))
	}
	now := s.config.now().UTC()
	before, err := s.database.hostedConsumption(ctx, tx, now)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if fetchErr != nil {
		retry := now.Add(time.Minute)
		if page.RetryAt.After(retry) {
			retry = page.RetryAt
		}
		_, err = tx.ExecContext(ctx, "UPDATE github_imports SET status = 'partial', last_error = ?, retry_after = ?, revision = revision + 1 WHERE id = ?", truncateOutboxError(fetchErr.Error()), formatHubTime(retry), current.ID)
	} else {
		err = applyGitHubImportPage(ctx, tx, scope, integration, current, page, now)
	}
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	result, err := readGitHubImport(ctx, tx, scope, current.ID)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := s.database.checkHostedGrowth(ctx, tx, before, now, false); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func applyGitHubImportPage(ctx context.Context, tx *sql.Tx, scope nativeScope, integration ProjectIntegration, current GitHubImport, page GitHubImportPage, now time.Time) error {
	if page.NextCursor != "" && page.NextCursor == current.Cursor {
		return nativeInvalid("GitHub pagination did not advance")
	}
	if current.Stage == "issue" {
		if page.Issue == nil || page.Issue.Number != current.IssueNumber {
			return nativeInvalid("GitHub import returned the wrong issue")
		}
		if err := importGitHubIssue(ctx, tx, scope, integration, &current, *page.Issue, now); err != nil {
			return err
		}
	}
	if current.Stage == "dependencies" && current.Cursor == "" {
		if _, err := tx.ExecContext(ctx, "UPDATE github_import_records SET current_dependency = 0 WHERE import_id = ? AND kind = 'dependency'", current.ID); err != nil {
			return err
		}
	}
	for _, record := range page.Records {
		if record.SourceKey == "" || !json.Valid(record.Data) {
			return nativeInvalid("GitHub import record is incomplete")
		}
		raw, err := marshalNative(record)
		if err != nil {
			return err
		}
		inserted, err := tx.ExecContext(ctx, `INSERT INTO github_import_records (import_id, source_key, kind, record_json, observed_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, current.ID, record.SourceKey, record.Kind, raw, formatHubTime(now))
		if err != nil {
			return err
		}
		count, err := inserted.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			if record.Kind == "dependency" {
				if _, err := tx.ExecContext(ctx, "UPDATE github_import_records SET current_dependency = 1 WHERE import_id = ? AND source_key = ?", current.ID, record.SourceKey); err != nil {
					return err
				}
			}
			continue
		}
		if record.Kind == "comment" && record.Body != "" {
			if err := importGitHubComment(ctx, tx, scope, current.WorkItemID, record, now); err != nil {
				return err
			}
		} else if err := appendNativeHistory(ctx, tx, scope, current.WorkItemID, "github.imported", tracker.CollaborationData{Operation: record.Kind, Reason: record.SourceKey}, now); err != nil {
			return err
		}
	}
	current.Gaps = append(current.Gaps, page.Gaps...)
	stage := current.Stage
	if page.NextCursor == "" {
		switch stage {
		case "issue":
			stage = "comments"
		case "comments":
			stage = "timeline"
		case "timeline":
			stage = "dependencies"
		case "dependencies":
			stage = "edits"
		case "edits":
			if page.EditsFinished {
				stage = "finished"
			}
		}
	}
	if page.EditSequence != 0 && page.NextCursor == "" {
		current.EditSequence = page.EditSequence
	}
	status := "pending"
	if stage == "finished" {
		status = "retrieved"
	}
	gaps, err := marshalNative(current.Gaps)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE github_imports SET work_item_id = ?, stage = ?, cursor = ?, status = ?, gaps_json = ?, source_updated_at = ?, observed_at = ?, pages = pages + 1, revision = revision + 1, last_error = '', retry_after = NULL, edit_sequence = ? WHERE id = ?`, current.WorkItemID, stage, page.NextCursor, status, gaps, current.SourceUpdatedAt, formatHubTime(now), current.EditSequence, current.ID)
	if err != nil {
		return err
	}
	return resolveImportedDependencies(ctx, tx, scope, now)
}

func (s *Service) fetchImportPage(ctx context.Context, integration ProjectIntegration, job GitHubImport) (GitHubImportPage, error) {
	request := GitHubImportRequest{Repository: integration.Repository, Profile: integration.Profile, IssueNumber: job.IssueNumber, Stage: job.Stage, Cursor: job.Cursor}
	var sequence int64
	if job.Stage == "edits" {
		err := s.database.db.QueryRowContext(ctx, `SELECT sequence, COALESCE(json_extract(record_json, '$.provenance.external_id'), '') FROM github_import_records WHERE import_id = ? AND kind IN ('issue', 'comment') AND sequence > ? ORDER BY sequence LIMIT 1`, job.ID, job.EditSequence).Scan(&sequence, &request.SourceID)
		if errors.Is(err, sql.ErrNoRows) {
			return GitHubImportPage{EditsFinished: true}, nil
		}
		if err != nil {
			return GitHubImportPage{}, err
		}
		if request.SourceID == "" {
			return GitHubImportPage{EditSequence: sequence, Gaps: []string{"Source node ID was unavailable for an edit-history lookup"}}, nil
		}
	}
	page, err := s.config.ImportBackend.FetchImportPage(ctx, request)
	page.EditSequence = sequence
	return page, err
}

func importGitHubIssue(ctx context.Context, tx *sql.Tx, scope nativeScope, integration ProjectIntegration, current *GitHubImport, source IssueSource, now time.Time) error {
	if source.NodeID == "" || source.CreatedAt.IsZero() || source.UpdatedAt.IsZero() {
		return nativeInvalid("GitHub issue provenance is incomplete")
	}
	if integration.Profile == "github_compatible" {
		stamp, err := newSourceStamp(source.UpdatedAt, normalizedIssue(source))
		if err != nil {
			return err
		}
		if _, err := applyIssueProjection(ctx, tx, integration.RepositoryID, normalizedIssue(source), stamp, now, true); err != nil {
			return err
		}
	}
	err := tx.QueryRowContext(ctx, "SELECT native_id FROM issues WHERE project_id = ? AND github_node_id = ?", scope.project, source.NodeID).Scan(&current.WorkItemID)
	if errors.Is(err, sql.ErrNoRows) && integration.Profile == "native" {
		project, readErr := readNativeProject(ctx, tx, scope)
		if readErr != nil {
			return readErr
		}
		if len(project.States) == 0 {
			return nativeInvalid("Native intake requires configured states")
		}
		author := source.AuthorID
		if author == "" {
			author = "unavailable"
		}
		created, createErr := createNativeIssueTx(ctx, tx, scope, tracker.CreateIssue{Title: source.Title, Body: source.Body, State: project.States[0].Name, Labels: source.Labels, Assignees: source.Assignees, Provenance: &tracker.Provenance{Provider: "github", ExternalID: source.NodeID, AuthorID: author, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt, ObservedAt: now}}, now)
		if createErr != nil {
			return createErr
		}
		issue, ok := created.(tracker.NativeIssue)
		if !ok {
			return nativeInvalid("Native issue import returned an invalid identity")
		}
		current.WorkItemID = string(issue.WorkItemID)
		if _, err := tx.ExecContext(ctx, "UPDATE github_imports SET intake_pending = 1 WHERE id = ?", current.ID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE issues SET repository_id = ?, github_node_id = ?, github_number = ?, github_database_id = ?, source_updated_at = ?, synchronized_at = ?, created_at = ?, author_login = ? WHERE native_id = ?", integration.RepositoryID, source.NodeID, source.Number, source.DatabaseID, formatHubTime(source.UpdatedAt), formatHubTime(now), formatHubTime(source.CreatedAt), author, current.WorkItemID)
	}
	if err != nil {
		return err
	}
	current.SourceUpdatedAt = formatHubTime(source.UpdatedAt)
	return nil
}

func importGitHubComment(ctx context.Context, tx *sql.Tx, scope nativeScope, item string, record GitHubImportRecord, now time.Time) error {
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM native_comments WHERE project_id = ? AND work_item_id = ? AND source_key = ?", scope.project, item, "github:"+record.Provenance.ExternalID).Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	comment := tracker.NativeComment{ID: newNativeID("cmt"), OrganizationID: scope.organization, ProjectID: scope.project, WorkItemID: tracker.NativeWorkItemID(item), Revision: 1, Body: record.Body, Actor: scope.actor(), Provenance: &record.Provenance, CreatedAt: now, UpdatedAt: now}
	if err := tx.QueryRowContext(ctx, "SELECT event_sequence + 1 FROM issues WHERE native_id = ?", item).Scan(&comment.Sequence); err != nil {
		return err
	}
	actor, err := marshalNative(comment.Actor)
	if err != nil {
		return err
	}
	provenance, err := marshalNative(record.Provenance)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO native_comments (id, organization_id, project_id, work_item_id, revision, sequence, body, actor_json, provenance_json, source_key, created_at, updated_at) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)`, comment.ID, scope.organization, scope.project, item, comment.Sequence, comment.Body, actor, provenance, "github:"+record.Provenance.ExternalID, formatHubTime(now), formatHubTime(now))
	if err != nil {
		return err
	}
	return recordNativeChange(ctx, tx, scope, comment, item, 1, "comment.imported", tracker.CollaborationData{CommentID: comment.ID, Revision: 1}, now)
}

func resolveImportedDependencies(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM issue_dependencies WHERE provenance = 'github_import'
AND dependent_issue_id IN (SELECT i.id FROM issues i JOIN github_imports g ON g.work_item_id = i.native_id WHERE g.project_id = ? AND g.intake_pending = 1)
AND NOT EXISTS (SELECT 1 FROM github_import_records r JOIN github_imports g ON g.id = r.import_id JOIN issues b ON b.id = issue_dependencies.blocker_issue_id JOIN issues i ON i.id = issue_dependencies.dependent_issue_id
WHERE g.work_item_id = i.native_id AND r.kind = 'dependency' AND r.current_dependency = 1 AND b.github_node_id = json_extract(r.record_json, '$.dependency_id'))`, scope.project)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO issue_dependencies (blocker_issue_id, dependent_issue_id, provenance, created_at, updated_at)
SELECT b.id, i.id, 'github_import', ?, ? FROM github_import_records r JOIN github_imports g ON g.id = r.import_id JOIN issues i ON i.native_id = g.work_item_id JOIN issues b ON b.project_id = i.project_id AND b.github_node_id = json_extract(r.record_json, '$.dependency_id') WHERE g.project_id = ? AND g.intake_pending = 1 AND r.kind = 'dependency' AND r.current_dependency = 1 ON CONFLICT DO NOTHING`, formatHubTime(now), formatHubTime(now), scope.project)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE github_imports SET intake_pending = 0 WHERE project_id = ? AND intake_pending = 1 AND stage = 'finished' AND NOT EXISTS (
SELECT 1 FROM github_import_records r WHERE r.import_id = github_imports.id AND r.kind = 'dependency' AND r.current_dependency = 1 AND NOT EXISTS (
SELECT 1 FROM issues i WHERE i.project_id = github_imports.project_id AND i.github_node_id = json_extract(r.record_json, '$.dependency_id')))`, scope.project)
	return err
}

func (s *Service) listGitHubImportRecords(c echo.Context) error {
	scope := nativeRequestScope(c)
	if _, err := readGitHubImport(c.Request().Context(), s.database.db, scope, c.Param("import")); err != nil {
		return s.nativeAPIError(c, err)
	}
	limit, cursor, key, err := s.nativePage(c)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	after, err := strconv.ParseInt("0"+cursor.After, 10, 64)
	if err != nil {
		return s.nativeAPIError(c, nativeInvalid("Invalid import cursor"))
	}
	rows, err := s.database.db.QueryContext(c.Request().Context(), "SELECT sequence, record_json, current_dependency FROM github_import_records WHERE import_id = ? AND sequence > ? ORDER BY sequence LIMIT ?", c.Param("import"), after, limit+1)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer rows.Close()
	result := tracker.Page[GitHubImportRecord]{Items: []GitHubImportRecord{}}
	for rows.Next() {
		var sequence int64
		var raw string
		var currentDependency bool
		if err := rows.Scan(&sequence, &raw, &currentDependency); err != nil {
			return s.nativeAPIError(c, err)
		}
		if len(result.Items) == limit {
			result.NextCursor, err = encodeNativeCursor(cursor, key)
			if err != nil {
				return s.nativeAPIError(c, err)
			}
			break
		}
		var record GitHubImportRecord
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return s.nativeAPIError(c, err)
		}
		if record.Kind == "dependency" {
			record.CurrentDependency = &currentDependency
		}
		result.Items = append(result.Items, record)
		cursor.After = strconv.FormatInt(sequence, 10)
	}
	if err := rows.Err(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}
