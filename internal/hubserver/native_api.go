package hubserver

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const nativeBase = "/api/v2/organizations/:organization/projects/:project"

type nativeScope struct {
	organization tracker.OrganizationID
	project      tracker.ProjectID
	credential   apiCredential
}

type nativeError struct {
	Code            string           `json:"code"`
	Message         string           `json:"message"`
	CurrentRevision tracker.Revision `json:"current_revision,string,omitempty"`
	status          int
}

func (e *nativeError) Error() string { return e.Code }

func nativeInvalid(message string) error {
	return &nativeError{Code: "invalid_request", Message: message, status: http.StatusUnprocessableEntity}
}

func nativeNotFound() error {
	return &nativeError{Code: "not_found", Message: "Resource was not found", status: http.StatusNotFound}
}

func nativeConflict(revision tracker.Revision) error {
	return &nativeError{Code: "revision_conflict", Message: "Resource has changed", CurrentRevision: revision, status: http.StatusConflict}
}

func (s *Service) nativeAPIError(c echo.Context, err error) error {
	if errors.Is(err, auth.ErrHostedIdentity) || errors.Is(err, auth.ErrInvalidSession) {
		return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "access_denied", Message: "Access is no longer available"})
	}
	var failure *nativeError
	if errors.As(err, &failure) {
		if s.config.Hosted != nil {
			return c.JSON(failure.status, apiErrorResponse{Code: failure.Code, Message: "The requested operation is unavailable"})
		}
		return c.JSON(failure.status, failure)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return s.nativeAPIError(c, nativeNotFound())
	}
	if errors.Is(err, tracker.ErrInvalidClaimRequest) || errors.Is(err, tracker.ErrInvalidLeaseRequest) || errors.Is(err, tracker.ErrInvalidWorkEvent) || errors.Is(err, tracker.ErrInvalidCandidateQuery) {
		return trackerAPIError(c, err)
	}
	if errors.Is(err, tracker.ErrLeaseConflict) || errors.Is(err, tracker.ErrStaleFencingToken) || errors.Is(err, tracker.ErrLeaseNotFound) || errors.Is(err, tracker.ErrMachineNotFound) || errors.Is(err, tracker.ErrWorkItemNotFound) || errors.Is(err, ErrNoClaimableWork) {
		return trackerAPIError(c, err)
	}
	return s.internalAPIError(c, "native_operation_failed", "Native operation could not be completed", err)
}

func (s *Service) registerNativeRoutes(e *echo.Echo) {
	s.registerOnboardingRoutes(e)
	s.registerArtifactRoutes(e)
	s.registerIntegrationRoutes(e)
	s.registerChangeRoutes(e)
	read := s.requireNativeScope(apiScopeWorker, apiScopeOperator)
	write := s.requireNativeScope(apiScopeWorker, apiScopeOperator)
	admin := s.requireInstanceAdmin()
	worker := s.requireNativeScope(apiScopeWorker)
	e.GET(nativeBase+"/policy", s.getProjectPolicy, s.requireNativeScope(apiScopeWorker, apiScopeOperator, apiScopeAdmin))
	e.PUT(nativeBase+"/policy", s.approveProjectPolicy, admin)
	e.DELETE(nativeBase+"/policy", s.revokeProjectPolicy, admin)
	e.GET("/api/v2/capabilities", s.nativeCapabilities, s.requireAPIScope(apiScopeWorker, apiScopeOperator))
	e.GET("/api/v2/organizations", s.nativeOrganizations, admin)
	e.POST("/api/v2/organizations", s.createNativeOrganization, admin)
	e.POST("/api/v2/organizations/:organization/projects", s.createNativeProject, admin)
	e.POST("/api/v2/tokens/:id/grants", s.grantNativeToken, admin)
	e.GET(nativeBase, s.getNativeProject, read)
	e.GET(nativeBase+"/work-items", s.listNativeIssues, read)
	e.POST(nativeBase+"/work-items", s.createNativeIssue, write)
	e.GET(nativeBase+"/work-items/:item", s.getNativeIssue, read)
	e.PATCH(nativeBase+"/work-items/:item", s.updateNativeIssue, write)
	e.POST(nativeBase+"/work-items/:item/workflow", s.transitionNativeIssue, write)
	e.POST(nativeBase+"/work-items/:item/dependencies", s.changeNativeDependency, write)
	e.GET(nativeBase+"/work-items/:item/comments", s.listNativeComments, read)
	e.POST(nativeBase+"/work-items/:item/comments", s.createNativeComment, write)
	e.PATCH(nativeBase+"/work-items/:item/comments/:comment", s.updateNativeComment, write)
	e.GET(nativeBase+"/work-items/:item/history", s.listNativeHistory, read)
	e.GET(nativeBase+"/work-items/:item/attempts", s.listNativeAttempts, read)
	e.GET(nativeBase+"/work-items/:item/versions/:revision", s.getNativeVersion, read)
	e.GET(nativeBase+"/work-items/:item/comments/:comment/versions/:revision", s.getNativeVersion, read)
	e.POST(nativeBase+"/claims", s.claimNativeIssue, worker)
	e.POST(nativeBase+"/claims/preview", s.previewProviderCandidates, worker)
	e.POST(nativeBase+"/leases/:lease/renew", s.renewNativeLease, worker)
	e.POST(nativeBase+"/leases/:lease/release", s.releaseNativeLease, worker)
	e.POST(nativeBase+"/machines/register", s.registerNativeMachine, worker)
	e.POST(nativeBase+"/work-items/:item/events", s.appendNativeRunEvent, worker)
}

func (s *Service) requireInstanceAdmin() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		if s.config.Hosted != nil {
			return s.requireHostedAdministration(next)
		}
		return s.requireAPIScope(apiScopeAdmin)(func(c echo.Context) error {
			credential, ok := c.Get("hub_api_credential").(apiCredential)
			if !ok || credential.NativeOnly {
				return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "insufficient_scope", Message: "Instance administrator is required"})
			}
			return next(c)
		})
	}
}

func (s *Service) requireNativeScope(roles ...apiScope) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return s.requireAPIScope(roles...)(func(c echo.Context) error {
			credential, ok := c.Get("hub_api_credential").(apiCredential)
			if !ok {
				return s.nativeAPIError(c, nativeNotFound())
			}
			scope := nativeScope{organization: tracker.OrganizationID(c.Param("organization")), project: tracker.ProjectID(c.Param("project")), credential: credential}
			write := !hostedReadRequest(c) && !artifactReadGrantRequest(c)
			if err := s.requireHostedProject(c.Request().Context(), s.database.db, scope, write); err != nil {
				return s.nativeAPIError(c, err)
			}
			if err := s.database.authorizeNativeProject(c.Request().Context(), scope); err != nil {
				return s.nativeAPIError(c, err)
			}
			c.Set("native_scope", scope)
			c.Response().Header().Set("Cache-Control", "no-store")
			if credential.Hosted != nil {
				if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "action", c.Request().Method+" "+c.Path(), string(scope.project), 0); err != nil {
					return s.nativeAPIError(c, err)
				}
			}
			return next(c)
		})
	}
}

func (d *database) authorizeNativeProject(ctx context.Context, scope nativeScope) error {
	if scope.credential.Runner.RunnerID != "" && scope.credential.Runner.OrganizationID != scope.organization {
		return nativeNotFound()
	}
	var count int
	query := "SELECT count(*) FROM projects WHERE organization_id = ? AND id = ?"
	args := []any{scope.organization, scope.project}
	if scope.credential.Scope != apiScopeAdmin || scope.credential.NativeOnly {
		query += " AND EXISTS (SELECT 1 FROM token_grants g WHERE g.token_id = ? AND g.organization_id = projects.organization_id AND g.project_id = projects.id)"
		args = append(args, scope.credential.ID)
	}
	if err := d.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return nativeNotFound()
	}
	return nil
}

func nativeRequestScope(c echo.Context) nativeScope {
	scope, ok := c.Get("native_scope").(nativeScope)
	if !ok {
		return nativeScope{}
	}
	return scope
}

func (scope nativeScope) actor() tracker.Actor {
	kind := "human"
	if scope.credential.Scope == apiScopeWorker {
		kind = "runner"
	}
	return tracker.Actor{Kind: kind, PrincipalID: scope.credential.ID}
}

func newNativeID(prefix string) string {
	return prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
}

func marshalNative(value any) (string, error) {
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func (s *Service) nativeMutation(c echo.Context, command tracker.Mutation, input any, operation func(context.Context, *sql.Tx, nativeScope, time.Time) (any, error)) (resultErr error) {
	if strings.TrimSpace(command.IdempotencyKey) == "" || len(command.IdempotencyKey) > 128 {
		return s.nativeAPIError(c, nativeInvalid("An idempotency key of at most 128 bytes is required"))
	}
	scope := nativeRequestScope(c)
	ctx := c.Request().Context()
	encoded, err := json.Marshal(input)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	hash := sha256.Sum256(encoded)
	requestHash := hex.EncodeToString(hash[:])
	operationID := c.Request().Method + " " + c.Request().URL.EscapedPath()
	if scope.credential.Hosted != nil {
		operationID += " " + scope.credential.Hosted.SessionID
	}
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := s.recheckHostedMutation(ctx, tx, scope); err != nil {
		return s.nativeAPIError(c, err)
	}
	var storedHash, response string
	err = tx.QueryRowContext(ctx, `SELECT request_hash, response_json FROM native_commands WHERE organization_id = ? AND actor_id = ? AND operation = ? AND command_key = ?`, scope.organization, scope.credential.ID, operationID, command.IdempotencyKey).Scan(&storedHash, &response)
	if err == nil {
		if storedHash != requestHash {
			return s.nativeAPIError(c, &nativeError{Code: "idempotency_conflict", Message: "Idempotency key has different content", status: http.StatusConflict})
		}
		if err := tx.Commit(); err != nil {
			return s.nativeAPIError(c, err)
		}
		return c.JSONBlob(http.StatusOK, []byte(response))
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return s.nativeAPIError(c, err)
	}
	now, err := s.database.currentTime()
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := requireRunnerAuthority(ctx, tx, scope, now); err != nil {
		return s.nativeAPIError(c, err)
	}
	if scope.credential.Scope == apiScopeWorker && c.Param("item") != "" && !strings.HasSuffix(c.Path(), "/events") && c.Path() != changeBase+"/:change/versions/:version/checks" {
		if err := requireNativeMutationLease(ctx, tx, scope, c.Param("item"), command, now); err != nil {
			return s.nativeAPIError(c, err)
		}
	}
	value, err := operation(ctx, tx, scope, now)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	response, err = marshalNative(value)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO native_commands (organization_id, actor_id, operation, command_key, request_hash, response_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, scope.organization, scope.credential.ID, operationID, command.IdempotencyKey, requestHash, response, formatHubTime(now)); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSONBlob(http.StatusOK, []byte(response))
}

func (s *Service) requireCompatibilityResource(c echo.Context) error {
	var query string
	path := c.Path()
	switch {
	case strings.HasPrefix(path, "/api/v1/work-items/:id"):
		query = "SELECT count(*) FROM issues i JOIN projects p ON p.id = i.project_id WHERE i.id = ? AND p.profile = 'native'"
	case strings.HasPrefix(path, "/api/v1/leases/:id"):
		query = "SELECT count(*) FROM leases l JOIN issues i ON i.id = l.issue_id JOIN projects p ON p.id = i.project_id WHERE l.lease_id = ? AND p.profile = 'native'"
	case strings.HasPrefix(path, "/api/v1/machines/:id"):
		query = "SELECT count(*) FROM machines WHERE id = ? AND organization_id IS NOT NULL"
	default:
		return nil
	}
	var count int
	if err := s.database.db.QueryRowContext(c.Request().Context(), query, c.Param("id")).Scan(&count); err != nil {
		return fmt.Errorf("check compatibility resource: %w", err)
	}
	if count != 0 {
		return echo.NewHTTPError(http.StatusNotFound, apiErrorResponse{Code: "not_found", Message: "Resource was not found"})
	}
	return nil
}
