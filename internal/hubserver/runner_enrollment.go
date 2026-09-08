package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const runnerBase = "/api/v2/organizations/:organization/runners"
const enrollmentBase = "/api/v2/organizations/:organization/runner-enrollments"

func (s *Service) registerRunnerRoutes(e *echo.Echo) {
	admin := s.requireInstanceAdmin()
	worker := s.requireAPIScope(apiScopeWorker)
	e.POST(enrollmentBase, s.createRunnerEnrollment, admin)
	e.DELETE(enrollmentBase+"/:enrollment", s.revokeRunnerEnrollment, admin)
	e.POST(enrollmentBase+"/redeem", s.redeemRunnerEnrollment)
	e.GET(runnerBase+"/:runner", s.getRunnerIdentity, worker)
	e.POST(runnerBase+"/:runner/renew", s.renewRunnerIdentity, worker)
	e.POST(runnerBase+"/:runner/rotate", s.rotateRunnerIdentity, worker)
	e.DELETE(runnerBase+"/:runner", s.revokeRunnerIdentity, admin)
	e.GET(runnerBase, s.listRunnerRouting, admin)
	e.GET(runnerBase+"/:runner/routing", s.getRunnerRouting, s.requireAPIScope(apiScopeWorker, apiScopeAdmin))
	e.PUT(runnerBase+"/:runner/routing", s.updateRunnerRouting, admin)
	e.PUT("/api/v2/organizations/:organization/machines/:machine/routing", s.updateRunnerHost, admin)
	e.POST(nativeBase+"/leases/:lease/validate", s.validateRunnerLease, s.requireNativeScope(apiScopeWorker))
	e.POST(nativeBase+"/machines/:machine/heartbeat", s.heartbeatNativeMachine, s.requireNativeScope(apiScopeWorker))
}

func runnerTimeValid(now time.Time, created, expiry string) bool {
	start, err := parseTimeValue(created)
	if err != nil {
		return false
	}
	end, err := parseTimeValue(expiry)
	return err == nil && !now.Before(start) && now.Before(end)
}

func runnerUnauthorized() error {
	return &nativeError{Code: "unauthorized", Message: "Runner credential or enrollment is invalid or inactive", status: http.StatusUnauthorized}
}

func runnerCollision() error {
	return &nativeError{Code: "identity_collision", Message: "Identity is already registered; create a fresh host identity and enroll it explicitly", status: http.StatusConflict}
}

func (s *Service) runnerTransaction(c echo.Context, status int, operation func(context.Context, *sql.Tx, time.Time) (any, error)) error {
	ctx := c.Request().Context()
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	now, err := s.database.currentTime()
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if credential, ok := c.Get("hub_api_credential").(apiCredential); ok {
		if err := s.recheckHostedMutation(ctx, tx, nativeScope{organization: tracker.OrganizationID(c.Param("organization")), credential: credential}); err != nil {
			return s.nativeAPIError(c, err)
		}
		if err := requireCredentialAuthority(ctx, tx, credential, now); err != nil {
			return s.nativeAPIError(c, err)
		}
	}
	before, err := s.database.hostedConsumption(ctx, tx, now)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	value, err := operation(ctx, tx, now)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	heartbeat := strings.HasSuffix(c.Path(), "/heartbeat")
	completion := c.Request().Method == http.MethodDelete
	if s.database.hostedPlans != nil && (heartbeat || strings.HasSuffix(c.Path(), "/machines/register")) {
		credential, ok := c.Get("hub_api_credential").(apiCredential)
		if !ok {
			return s.nativeAPIError(c, runnerUnauthorized())
		}
		active, err := countHostedAfter(ctx, tx, "SELECT l.expires_at FROM leases l JOIN lease_runners r ON r.lease_id=l.lease_id WHERE l.released_at IS NULL AND r.runner_id=?", now, credential.Runner.RunnerID)
		if err != nil {
			return s.nativeAPIError(c, err)
		}
		completion = active > 0
	}
	if err := s.database.checkHostedGrowth(ctx, tx, before, now, completion); err != nil {
		return s.nativeAPIError(c, err)
	}
	if heartbeat {
		if err := s.database.recordHostedUsage(ctx, tx, now, "heartbeats", 1); err != nil {
			return s.nativeAPIError(c, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	if status == http.StatusNoContent {
		return c.NoContent(status)
	}
	return c.JSON(status, value)
}

func (s *Service) createRunnerEnrollment(c echo.Context) error {
	var request runnerauth.EnrollmentRequest
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	if !request.Valid() || !runnerauth.ValidOperations(request.Operations) || len(request.ProjectIDs) == 0 || len(request.ProjectIDs) > 100 || request.TTLSeconds <= 0 || request.TTLSeconds > int64(runnerauth.MaxEnrollmentTTL/time.Second) {
		return s.nativeAPIError(c, nativeInvalid("Enrollment requires host-generated IDs, explicit projects and operations, and a TTL of 1 to 900 seconds"))
	}
	credential, ok := c.Get("hub_api_credential").(apiCredential)
	if !ok {
		return s.nativeAPIError(c, runnerUnauthorized())
	}
	return s.runnerTransaction(c, http.StatusCreated, func(ctx context.Context, tx *sql.Tx, now time.Time) (any, error) {
		for i, project := range request.ProjectIDs {
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM projects WHERE organization_id = ? AND id = ?", c.Param("organization"), project).Scan(&count); err != nil {
				return nil, err
			}
			if count != 1 || slices.Contains(request.ProjectIDs[:i], project) {
				return nil, nativeInvalid("Enrollment projects must be unique and belong to the organization")
			}
		}
		if err := runnerBindingAvailable(ctx, tx, request.Binding, c.Param("organization"), request.SharedMachine); err != nil {
			return nil, err
		}
		token, err := s.config.generateToken()
		if err != nil {
			return nil, err
		}
		result := runnerauth.Enrollment{ID: newNativeID("enrollment"), Token: token, ExpiresAt: now.Add(time.Duration(request.TTLSeconds) * time.Second)}
		operations, err := marshalNative(request.Operations)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runner_enrollments (id, organization_id, runner_id, machine_id, token_hash, operations_json, created_at, expires_at, created_by, shared_machine)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, result.ID, c.Param("organization"), request.RunnerID, request.MachineID, apikey.HashToken(token), operations, formatHubTime(now), formatHubTime(result.ExpiresAt), credential.ID, request.SharedMachine); err != nil {
			return nil, err
		}
		for _, project := range request.ProjectIDs {
			if _, err := tx.ExecContext(ctx, "INSERT INTO runner_enrollment_projects (enrollment_id, organization_id, project_id) VALUES (?, ?, ?)", result.ID, c.Param("organization"), project); err != nil {
				return nil, err
			}
		}
		return result, nil
	})
}

func runnerIDsAvailable(ctx context.Context, tx *sql.Tx, binding runnerauth.Binding) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM machines WHERE id = ?) + (SELECT count(*) FROM runner_identities WHERE id = ?)`, binding.MachineID, binding.RunnerID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return runnerCollision()
	}
	return nil
}

func runnerBindingAvailable(ctx context.Context, tx *sql.Tx, binding runnerauth.Binding, organization string, shared bool) error {
	if !shared {
		return runnerIDsAvailable(ctx, tx, binding)
	}
	var hosts, runners int
	if err := tx.QueryRowContext(ctx, `SELECT
(SELECT count(*) FROM machines m WHERE m.id = ? AND m.organization_id = ? AND EXISTS (SELECT 1 FROM runner_identities r WHERE r.machine_id = m.id)),
(SELECT count(*) FROM runner_identities WHERE id = ?)`, binding.MachineID, organization, binding.RunnerID).Scan(&hosts, &runners); err != nil {
		return err
	}
	if hosts != 1 || runners != 0 {
		return runnerCollision()
	}
	return nil
}

func (s *Service) revokeRunnerEnrollment(c echo.Context) error {
	return s.runnerTransaction(c, http.StatusNoContent, func(ctx context.Context, tx *sql.Tx, now time.Time) (any, error) {
		result, err := tx.ExecContext(ctx, "UPDATE runner_enrollments SET revoked_at = ? WHERE id = ? AND organization_id = ? AND redeemed_at IS NULL AND revoked_at IS NULL", formatHubTime(now), c.Param("enrollment"), c.Param("organization"))
		return struct{}{}, requireRunnerUpdate(result, err)
	})
}

func requireRunnerUpdate(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return nativeNotFound()
	}
	return nil
}

func (s *Service) redeemRunnerEnrollment(c echo.Context) error {
	token, err := apiBearerToken(c)
	if err != nil {
		return s.nativeAPIError(c, runnerUnauthorized())
	}
	var request runnerauth.Redemption
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	if !request.Valid() || !runnerauth.ValidCredential(request.Credential) || token == request.Credential || strings.TrimSpace(request.Hostname) == "" || len(request.Hostname) > 200 || len(request.DisplayName) > 200 || request.Capacity < 0 || strings.TrimSpace(request.Version) == "" || len(request.Version) > 100 || !validRunnerPlatform(request.OS, request.Architecture) {
		return s.nativeAPIError(c, nativeInvalid("Host identity, a separate generated credential, hostname, version and nonnegative capacity are required"))
	}
	return s.runnerTransaction(c, http.StatusCreated, func(ctx context.Context, tx *sql.Tx, now time.Time) (any, error) {
		var id, operations, created, expires, actor string
		var binding runnerauth.Binding
		var redeemed, revoked sql.NullString
		var shared bool
		err := tx.QueryRowContext(ctx, `SELECT id, runner_id, machine_id, operations_json, created_at, expires_at, created_by, redeemed_at, revoked_at, shared_machine
FROM runner_enrollments WHERE token_hash = ? AND organization_id = ?`, apikey.HashToken(token), c.Param("organization")).Scan(&id, &binding.RunnerID, &binding.MachineID, &operations, &created, &expires, &actor, &redeemed, &revoked, &shared)
		if err != nil || binding != request.Binding || redeemed.Valid || revoked.Valid || !runnerTimeValid(now, created, expires) {
			return nil, runnerUnauthorized()
		}
		if err := runnerBindingAvailable(ctx, tx, binding, c.Param("organization"), shared); err != nil {
			return nil, err
		}
		identity := runnerauth.Identity{Binding: binding, OrganizationID: tracker.OrganizationID(c.Param("organization")), ExpiresAt: now.Add(runnerauth.CredentialTTL)}
		if err := json.Unmarshal([]byte(operations), &identity.Operations); err != nil {
			return nil, err
		}
		hash := apikey.HashToken(request.Credential)
		if _, err := tx.ExecContext(ctx, `INSERT INTO api_tokens (id, name, token_hash, token_fingerprint, scope, created_at, updated_at, native_only, expires_at)
VALUES (?, ?, ?, ?, 'worker', ?, ?, 1, ?)`, binding.RunnerID, binding.RunnerID, hash, tokenFingerprint(hash), formatHubTime(now), formatHubTime(now), formatHubTime(identity.ExpiresAt)); err != nil {
			return nil, runnerCollision()
		}
		if !shared {
			if _, err := tx.ExecContext(ctx, `INSERT INTO machines (id, hostname, display_name, capacity, version, last_heartbeat_at, registered_at, updated_at, organization_id, token_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, binding.MachineID, request.Hostname, request.DisplayName, request.Capacity, request.Version, formatHubTime(now), formatHubTime(now), formatHubTime(now), identity.OrganizationID, binding.RunnerID); err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runner_identities (id, organization_id, machine_id, token_id, enrollment_id, operations_json, created_at, display_name, capacity_limit, reported_capacity, os, architecture, last_heartbeat_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, binding.RunnerID, identity.OrganizationID, binding.MachineID, binding.RunnerID, id, operations, formatHubTime(now), request.DisplayName, request.Capacity, request.Capacity, request.OS, request.Architecture, formatHubTime(now)); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO token_grants (token_id, organization_id, project_id) SELECT ?, organization_id, project_id FROM runner_enrollment_projects WHERE enrollment_id = ?`, binding.RunnerID, id); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE runner_enrollments SET redeemed_at = ? WHERE id = ?", formatHubTime(now), id); err != nil {
			return nil, err
		}
		if err := recordRunnerEvent(ctx, tx, binding.RunnerID, actor, "enrolled", now); err != nil {
			return nil, err
		}
		identity.ProjectIDs, err = readRunnerProjects(ctx, tx, binding.RunnerID)
		return identity, err
	})
}
