package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/digitaldrywood/detent/internal/billing"
)

type hostedBillingState struct {
	Snapshot    billing.Snapshot `json:"subscription"`
	Status      string           `json:"status"`
	Plan        PlanReference    `json:"plan"`
	PaidThrough time.Time        `json:"paid_through"`
	AccessUntil time.Time        `json:"access_until"`
	GraceUntil  time.Time        `json:"grace_until"`
}

func resolveHostedBilling(config *HostedBillingConfig, previous hostedBillingState, snapshot billing.Snapshot, now time.Time) hostedBillingState {
	state := hostedBillingState{Snapshot: snapshot, Status: snapshot.Status}
	for _, price := range config.Prices {
		if price.PriceID == snapshot.PriceID {
			state.Plan = price.Plan
		}
	}
	if state.Plan.ID == "" || snapshot.PeriodEnd.After(now.AddDate(2, 0, 0)) {
		if snapshot.SubscriptionID != "" {
			state.Status = "unapproved_plan"
		}
		return state
	}
	if snapshot.PaymentHold != "" {
		state.Status = snapshot.PaymentHold
		return state
	}
	grace := time.Duration(config.GraceSeconds) * time.Second
	switch snapshot.Status {
	case "active":
		if snapshot.InvoiceID != "" && snapshot.InvoiceStatus == "paid" {
			state.PaidThrough = snapshot.PeriodEnd
			state.GraceUntil = snapshot.PeriodEnd.Add(grace)
			state.AccessUntil = state.GraceUntil
			break
		}
		fallthrough
	case "past_due":
		state.Status = "payment_failed"
		if previous.Snapshot.SubscriptionID == snapshot.SubscriptionID && !previous.PaidThrough.IsZero() {
			state.PaidThrough = previous.PaidThrough
			state.GraceUntil = previous.PaidThrough.Add(grace)
			state.AccessUntil = state.GraceUntil
			state.Plan = previous.Plan
			if state.GraceUntil.After(now) {
				state.Status = "grace"
			}
		}
	case "trialing":
		if !snapshot.TrialEnd.IsZero() && !snapshot.TrialEnd.After(snapshot.PeriodEnd) {
			state.AccessUntil = snapshot.TrialEnd
		}
	}
	if !state.AccessUntil.IsZero() && (snapshot.CancelAtPeriodEnd || !snapshot.CancelAt.IsZero()) {
		end := snapshot.CancelAt
		if end.IsZero() || snapshot.CancelAtPeriodEnd && snapshot.PeriodEnd.Before(end) {
			end = snapshot.PeriodEnd
		}
		if end.Before(state.AccessUntil) {
			state.AccessUntil = end
			state.GraceUntil = time.Time{}
		}
		if state.Status == "active" || state.Status == "trialing" {
			state.Status = "canceling"
		}
	}
	return state
}

func (d *database) readHostedBilling(ctx context.Context) (hostedBillingState, error) {
	var state hostedBillingState
	var raw string
	err := d.db.QueryRowContext(ctx, "SELECT state_json FROM hosted_billing_accounts WHERE organization_id=?", d.hostedOrganization).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return hostedBillingState{Status: "free"}, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal([]byte(raw), &state)
	if state.Status == "" {
		state.Status = "free"
	}
	return state, err
}

func (d *database) commitHostedBilling(ctx context.Context, state hostedBillingState, through int64, now time.Time) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous string
	if err := tx.QueryRowContext(ctx, "SELECT state_json FROM hosted_billing_accounts WHERE organization_id=?", d.hostedOrganization).Scan(&previous); err != nil {
		return err
	}
	if previous != string(raw) {
		if err := d.insertBillingAudit(ctx, tx, "stripe", "subscription_reconciled", string(raw), now); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE hosted_billing_accounts SET state_json=?,reconciled_at=? WHERE organization_id=?", string(raw), formatHubTime(now), d.hostedOrganization); err != nil {
		return err
	}
	var plan, version, expiry any
	if state.Plan.ID != "" && !state.AccessUntil.IsZero() {
		plan, version, expiry = state.Plan.ID, state.Plan.Version, formatHubTime(state.AccessUntil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE hosted_plan_assignments SET subscription_id=?,subscription_version=?,subscription_expires_at=?,revision=revision+1 WHERE organization_id=? AND (subscription_id IS NOT ? OR subscription_version IS NOT ? OR subscription_expires_at IS NOT ?)`, plan, version, expiry, d.hostedOrganization, plan, version, expiry); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE hosted_billing_events SET processed_at=? WHERE sequence<=? AND processed_at IS NULL", formatHubTime(now), through); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *database) insertBillingAudit(ctx context.Context, tx *sql.Tx, actor, action, raw string, now time.Time) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO hosted_billing_audit(organization_id,actor_id,action,record_json,recorded_at) VALUES(?,?,?,?,?)", d.hostedOrganization, actor, action, raw, formatHubTime(now))
	return err
}
