package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/web/templates"
)

type hostedBillingReport struct {
	OrganizationID string                         `json:"organization_id"`
	State          hostedBillingState             `json:"state"`
	Entitlement    HostedEntitlement              `json:"entitlement"`
	ReconciledAt   string                         `json:"reconciled_at"`
	PendingEvents  int64                          `json:"pending_events"`
	Audit          []templates.HostedBillingAudit `json:"recent_audit"`
}

func (s *Service) hostedBillingReport(ctx context.Context) (hostedBillingReport, error) {
	report := hostedBillingReport{OrganizationID: s.config.Hosted.OrganizationID, Audit: []templates.HostedBillingAudit{}}
	var err error
	report.State, err = s.database.readHostedBilling(ctx)
	if err != nil {
		return report, err
	}
	report.Entitlement, err = s.database.hostedPlanUsage(ctx, s.config.now())
	if err != nil {
		return report, err
	}
	var checked sql.NullString
	err = s.database.db.QueryRowContext(ctx, "SELECT reconciled_at FROM hosted_billing_accounts WHERE organization_id=?", s.config.Hosted.OrganizationID).Scan(&checked)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, err
	}
	report.ReconciledAt = checked.String
	if err := s.database.db.QueryRowContext(ctx, "SELECT count(*) FROM hosted_billing_events WHERE processed_at IS NULL").Scan(&report.PendingEvents); err != nil {
		return report, err
	}
	rows, err := s.database.db.QueryContext(ctx, "SELECT actor_id,action,record_json,recorded_at FROM hosted_billing_audit WHERE organization_id=? ORDER BY id DESC LIMIT 50", s.config.Hosted.OrganizationID)
	if err != nil {
		return report, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry templates.HostedBillingAudit
		var raw string
		if err := rows.Scan(&entry.Actor, &entry.Action, &raw, &entry.At); err != nil {
			return report, err
		}
		var summary struct {
			Status  string `json:"status"`
			PriceID string `json:"price_id"`
		}
		if err := json.Unmarshal([]byte(raw), &summary); err != nil {
			return report, err
		}
		entry.Summary = strings.TrimSpace(summary.Status + " " + summary.PriceID)
		report.Audit = append(report.Audit, entry)
	}
	return report, rows.Err()
}

func (s *Service) hostedBillingExport(c echo.Context) error {
	credential, err := s.hostedBillingOwner(c)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "billing_exported", c.Path(), "", http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	report, err := s.hostedBillingReport(c.Request().Context())
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, report)
}

func (s *Service) hostedBillingPage(c echo.Context) error {
	credential, err := s.hostedBillingOwner(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "Billing requires an organization owner without support impersonation")
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "billing_viewed", c.Path(), "", http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	report, err := s.hostedBillingReport(c.Request().Context())
	if err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Billing information is temporarily unavailable")
	}
	now := s.config.now()
	data := templates.HostedPageData{Mode: "billing", Title: "Organization billing", CanManage: true, CanManageOwnership: true}
	data.PlanName = fmt.Sprintf("%s · version %d", report.Entitlement.EffectiveBase.ID, report.Entitlement.EffectiveBase.Version)
	data.BillingStatus, data.BillingMessage = hostedBillingMessage(report.State, now)
	data.BillingAudit = report.Audit
	data.BillingCheckedAt = report.ReconciledAt
	if report.PendingEvents > 0 {
		data.Notice = "A billing update is awaiting verification. Current access follows the last verified subscription deadline."
	}
	if c.QueryParam("checkout") != "" {
		data.Notice = "Checkout returned. Paid access starts only after Stripe confirms the subscription; this page does not activate a purchase."
	}
	for _, grant := range report.Entitlement.Grants {
		if grant.RevokedAt == nil && !grant.StartsAt.After(now) && (grant.ExpiresAt == nil || grant.ExpiresAt.After(now)) {
			message := "Complimentary access: " + strings.Join(grant.Scope, ", ")
			if grant.ExpiresAt != nil {
				message += " · expires " + grant.ExpiresAt.UTC().Format(time.RFC3339)
			}
			data.PlanGrants = append(data.PlanGrants, message)
		}
	}
	if cfg := s.config.Hosted.Billing; cfg != nil {
		data.BillingEnabled = true
		data.BillingCanPurchase = report.State.Snapshot.SubscriptionID == "" && report.State.Status != "multiple_subscriptions"
		for _, price := range cfg.Prices {
			data.BillingPrices = append(data.BillingPrices, templates.HostedBillingPrice{ID: price.PriceID, Label: price.Label})
		}
	}
	return s.renderHosted(c, http.StatusOK, data)
}

func hostedBillingMessage(state hostedBillingState, now time.Time) (string, string) {
	if !state.AccessUntil.IsZero() && !state.AccessUntil.After(now) {
		return "Subscription access ended", "The base plan and valid complimentary grants apply. Update your subscription in the portal to recover paid allowances."
	}
	switch state.Status {
	case "active":
		if !state.PaidThrough.After(now) {
			return "Renewal verification grace", "Renewal has not been verified. Paid allowances continue until " + state.AccessUntil.Format(time.RFC3339) + "."
		}
		return "Subscribed", "Current paid period ends " + state.PaidThrough.Format(time.RFC3339) + "."
	case "canceling":
		return "Cancellation scheduled", "Subscription access continues until " + state.AccessUntil.Format(time.RFC3339) + ", then returns to the base plan and valid complimentary grants."
	case "trialing":
		return "Subscription trial", "Stripe trial access ends " + state.AccessUntil.Format(time.RFC3339) + ". Trials are separate from complimentary grants."
	case "grace":
		return "Payment failed · grace active", "Update payment in the portal. Paid allowances continue until " + state.GraceUntil.Format(time.RFC3339) + "; after that, new excess allocations pause."
	case "past_due", "payment_failed", "unpaid":
		return "Payment failed", "The base plan and valid complimentary grants apply. Update payment in the portal to recover paid access."
	case "refunded", "disputed":
		return "Payment review · " + state.Status, "Subscription allowances are suspended for a full current-invoice refund or unresolved/lost dispute. The base plan and complimentary grants remain available."
	case "canceled", "incomplete_expired":
		return "Canceled", "Your subscription has ended. Free access and valid complimentary grants remain available."
	case "incomplete":
		return "Payment pending", "Complete payment or authentication in Stripe. Paid access has not started."
	case "free", "":
		return "Free", "Free access requires no card or Stripe subscription."
	default:
		return "Subscription needs attention", "This subscription is paused or outside the approved plan configuration. Use the portal or contact your organization operator."
	}
}
