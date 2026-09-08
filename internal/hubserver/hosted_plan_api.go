package hubserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/tracker"
)

type hostedPlanCommand struct {
	ID               string        `json:"idempotency_key"`
	Action           string        `json:"action"`
	ExpectedRevision int64         `json:"expected_revision"`
	Plan             PlanReference `json:"plan"`
	GrantID          string        `json:"grant_id"`
	Scope            []string      `json:"scope"`
	StartsAt         time.Time     `json:"starts_at"`
	ExpiresAt        *time.Time    `json:"expires_at"`
	Reason           string        `json:"reason"`
}

func (s *Service) updateHostedPlan(c echo.Context) error {
	token, err := apiBearerToken(c)
	supplied := sha256.Sum256([]byte(token))
	configured := sha256.Sum256(s.config.Hosted.EntitlementAdminToken)
	if err != nil || len(s.config.Hosted.EntitlementAdminToken) < 32 || subtle.ConstantTimeCompare(supplied[:], configured[:]) != 1 {
		return c.NoContent(http.StatusForbidden)
	}

	var command hostedPlanCommand
	if err := decodeAPIJSON(c, &command); err != nil {
		return invalidAPIRequest(c, err)
	}
	if err := s.database.applyHostedPlanCommand(c.Request().Context(), s.config.Hosted.EntitlementAdministrator, command); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (d *database) applyHostedPlanCommand(ctx context.Context, actor string, command hostedPlanCommand) error {
	if d.hostedBilling && (command.Action == "subscription" || command.Action == "end_subscription") {
		return nativeInvalid("Stripe reconciliation owns subscription access; use separate complimentary grants for operator access")
	}
	if d.hostedPlans == nil || !hostedSafeID(command.ID) || strings.TrimSpace(command.Reason) == "" || len(command.Reason) > 500 || command.ExpectedRevision < 1 || !slices.Contains([]string{"base", "subscription", "end_subscription", "grant", "revoke"}, command.Action) {
		return nativeInvalid("A command ID, revision, action and bounded audit reason are required")
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	hash := hex.EncodeToString(digest[:])
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now, err := d.currentTime()
	if err != nil {
		return err
	}
	var existing string
	err = tx.QueryRowContext(ctx, "SELECT request_hash FROM hosted_plan_audit WHERE command_id = ?", command.ID).Scan(&existing)
	if err == nil {
		if existing != hash {
			return nativeConflict(tracker.Revision(command.ExpectedRevision))
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM hosted_plan_assignments WHERE organization_id = ?", d.hostedOrganization).Scan(&revision); err != nil {
		return err
	}
	if revision != command.ExpectedRevision {
		return nativeConflict(tracker.Revision(revision))
	}
	if slices.Contains([]string{"base", "subscription", "grant"}, command.Action) {
		if _, err := readHostedPlan(ctx, tx, command.Plan); err != nil {
			return nativeInvalid("The plan version must exist")
		}
	}
	switch command.Action {
	case "base":
		_, err = tx.ExecContext(ctx, "UPDATE hosted_plan_assignments SET base_id = ?,base_version = ? WHERE organization_id = ?", command.Plan.ID, command.Plan.Version, d.hostedOrganization)
	case "subscription":
		if command.ExpiresAt == nil || !command.ExpiresAt.After(now) {
			return nativeInvalid("Subscription-derived access requires a future validity deadline")
		}
		_, err = tx.ExecContext(ctx, "UPDATE hosted_plan_assignments SET subscription_id = ?,subscription_version = ?,subscription_expires_at = ? WHERE organization_id = ?", command.Plan.ID, command.Plan.Version, formatHubTime(*command.ExpiresAt), d.hostedOrganization)
	case "end_subscription":
		_, err = tx.ExecContext(ctx, "UPDATE hosted_plan_assignments SET subscription_id = NULL,subscription_version = NULL,subscription_expires_at = NULL WHERE organization_id = ?", d.hostedOrganization)
	case "grant":
		err = insertHostedComplimentaryGrant(ctx, tx, command, now)
	case "revoke":
		var raw string
		var grant HostedGrant
		if err := tx.QueryRowContext(ctx, "SELECT record_json FROM hosted_complimentary_grants WHERE id = ?", command.GrantID).Scan(&raw); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(raw), &grant); err != nil {
			return err
		}
		if grant.RevokedAt == nil {
			stamp := now.UTC()
			grant.RevokedAt = &stamp
		}
		encoded, encodeErr := json.Marshal(grant)
		if encodeErr != nil {
			return encodeErr
		}
		_, err = tx.ExecContext(ctx, "UPDATE hosted_complimentary_grants SET record_json = ? WHERE id = ?", string(encoded), grant.ID)
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE hosted_plan_assignments SET revision = revision + 1 WHERE organization_id = ?", d.hostedOrganization); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO hosted_plan_audit(command_id,request_hash,actor_id,reason,action,recorded_at,record_json) VALUES(?,?,?,?,?,?,?)", command.ID, hash, actor, command.Reason, command.Action, formatHubTime(now), string(raw)); err != nil {
		return err
	}
	return tx.Commit()
}

func insertHostedComplimentaryGrant(ctx context.Context, tx *sql.Tx, command hostedPlanCommand, now time.Time) error {
	if !hostedSafeID(command.GrantID) || len(command.Scope) == 0 || len(command.Scope) > len(hostedAllowanceNames())+len(hostedFeatureNames()) {
		return nativeInvalid("A grant ID and explicit allowance or feature scope are required")
	}
	for _, scope := range command.Scope {
		if !slices.Contains(hostedAllowanceNames(), scope) && !slices.Contains(hostedFeatureNames(), scope) {
			return nativeInvalid("Grant scope is unknown")
		}
	}
	if command.StartsAt.IsZero() {
		command.StartsAt = now.UTC()
	}
	if command.ExpiresAt != nil && (!command.ExpiresAt.After(now) || !command.ExpiresAt.After(command.StartsAt)) {
		return nativeInvalid("Grant expiry must follow its start and the current time")
	}
	raw, err := json.Marshal(HostedGrant{ID: command.GrantID, Plan: command.Plan, Scope: command.Scope, StartsAt: command.StartsAt, ExpiresAt: command.ExpiresAt})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO hosted_complimentary_grants(id,record_json) VALUES(?,?)", command.GrantID, string(raw))
	return err
}

func hostedCompletionMutation(c echo.Context, input any) bool {
	if event, ok := input.(tracker.NativeRunEvent); ok {
		return event.Type == "run.finished" || event.Type == "run.checkpointed"
	}
	return false
}

func hostedMutationFeature(c echo.Context) string {
	if strings.Contains(c.Path(), "/integration") || strings.Contains(c.Path(), "/imports") {
		return "github_integration"
	}
	return "collaboration"
}
