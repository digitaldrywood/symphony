package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/onboarding"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func (s *Service) registerOnboardingRoutes(e *echo.Echo) {
	read := s.requireNativeScope(apiScopeOperator, apiScopeWorker, apiScopeAdmin)
	write := s.requireNativeScope(apiScopeOperator, apiScopeAdmin)
	e.PUT(nativeBase+"/onboarding/integration", s.updateProjectIntegration, s.requireOnboardingAdmin())
	e.POST(nativeBase+"/onboarding/repository", s.bindNativeRepository, s.requireOnboardingAdmin())
	e.GET(nativeBase+"/onboarding", s.getOnboarding, read)
	e.PUT(nativeBase+"/onboarding", s.saveOnboarding, write)
	e.PUT(nativeBase+"/onboarding/policy", s.approveProjectPolicy, s.requireOnboardingAdmin())
	e.PUT(nativeBase+"/onboarding/artifact-services/:service", s.bindArtifactService, s.requireOnboardingAdmin())
}

func readOnboardingProgress(ctx context.Context, query nativeQueryer, scope nativeScope) (onboarding.Progress, error) {
	var progress onboarding.Progress
	var raw string
	err := query.QueryRowContext(ctx, "SELECT progress_json FROM project_onboarding WHERE organization_id=? AND project_id=?", scope.organization, scope.project).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return progress, nil
	}
	if err != nil {
		return progress, err
	}
	return progress, json.Unmarshal([]byte(raw), &progress)
}

func (s *Service) projectOnboarding(ctx context.Context, scope nativeScope) (onboarding.Project, error) {
	result := onboarding.Project{Runners: []runnerauth.Eligibility{}, Artifacts: []artifact.Binding{}}
	var err error
	result.Progress, err = readOnboardingProgress(ctx, s.database.db, scope)
	if err != nil {
		return result, err
	}
	approval, err := readProjectPolicy(ctx, s.database.db, string(scope.organization)+"/"+string(scope.project))
	if err == nil {
		result.Policy = &approval
	} else {
		var missing *nativeError
		if !errors.As(err, &missing) || missing.Code != "policy_mismatch" {
			return result, err
		}
	}
	rows, err := s.database.db.QueryContext(ctx, `SELECT r.id FROM runner_identities r WHERE r.organization_id=? AND EXISTS (SELECT 1 FROM token_grants g WHERE g.token_id=r.token_id AND g.organization_id=? AND g.project_id=?) ORDER BY r.display_name,r.id`, scope.organization, scope.organization, scope.project)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return result, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, err
	}
	for _, id := range ids {
		runner, err := readRunner(ctx, s.database.db, scope.organization, id, s.config.now())
		if err != nil {
			return result, err
		}
		exclusions := runner.Exclusions(scope.project, approval.Policy.Requirements, false)
		runner.ProjectIDs = []tracker.ProjectID{scope.project}
		runner.Leases = nil
		runner.ProviderCapacity = nil
		result.Runners = append(result.Runners, runnerauth.Eligibility{Runner: runner, Exclusions: exclusions})
	}
	rows, err = s.database.db.QueryContext(ctx, "SELECT binding_json FROM artifact_services WHERE organization_id=? AND project_id=? ORDER BY id", scope.organization, scope.project)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var binding artifact.Binding
		if err := rows.Scan(&raw); err != nil {
			return result, errors.Join(err, rows.Close())
		}
		if err := json.Unmarshal(raw, &binding); err != nil {
			return result, errors.Join(err, rows.Close())
		}
		binding.PublisherTokenID = ""
		result.Artifacts = append(result.Artifacts, binding)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, err
	}
	err = s.database.db.QueryRowContext(ctx, "SELECT status FROM native_attempts WHERE organization_id=? AND project_id=? ORDER BY fencing_token DESC LIMIT 1", scope.organization, scope.project).Scan(&result.LatestRun)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	result.Evaluate()
	return result, nil
}

func (s *Service) getOnboarding(c echo.Context) error {
	result, err := s.projectOnboarding(c.Request().Context(), nativeRequestScope(c))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Service) saveOnboarding(c echo.Context) error {
	var request struct {
		tracker.Mutation
		Progress onboarding.Progress `json:"progress"`
	}
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	if err := request.Progress.Validate(); err != nil {
		return s.nativeAPIError(c, nativeInvalid(err.Error()))
	}
	return s.nativeMutation(c, request.Mutation, request, func(ctx context.Context, tx *sql.Tx, scope nativeScope, now time.Time) (any, error) {
		current, err := readOnboardingProgress(ctx, tx, scope)
		if err != nil {
			return nil, err
		}
		if current.Revision != request.Progress.Revision {
			return nil, nativeConflict(current.Revision)
		}
		progress := request.Progress
		progress.Revision++
		progress.UpdatedAt = formatHubTime(now)
		raw, err := marshalNative(progress)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO project_onboarding(organization_id,project_id,revision,progress_json,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(organization_id,project_id) DO UPDATE SET revision=excluded.revision,progress_json=excluded.progress_json,updated_at=excluded.updated_at`, scope.organization, scope.project, progress.Revision, raw, progress.UpdatedAt)
		return progress, err
	})
}

func (s *Service) requireOnboardingAdmin() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return s.requireNativeScope(apiScopeOperator, apiScopeAdmin)(func(c echo.Context) error {
			credential := nativeRequestScope(c).credential
			if credential.Hosted != nil {
				if credential.HostedRole != "owner" && credential.HostedRole != "admin" {
					return s.nativeAPIError(c, nativeNotFound())
				}
			} else if credential.Scope != apiScopeAdmin {
				return s.nativeAPIError(c, nativeNotFound())
			}
			return next(c)
		})
	}
}
