package hubserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/billing"
)

type hostedCheckout struct {
	Key       string          `json:"key"`
	PriceID   string          `json:"price_id"`
	ExpiresAt time.Time       `json:"expires_at"`
	Session   billing.Session `json:"session"`
}

func (s *Service) hostedBillingOwner(c echo.Context) (apiCredential, error) {
	credential, _, err := s.hostedCredential(c)
	if err != nil || credential.Hosted == nil || credential.HostedRole != "owner" || credential.Hosted.SupportActor != "" {
		return apiCredential{}, auth.ErrHostedIdentity
	}
	return credential, nil
}

func (s *Service) hostedBillingCheckout(c echo.Context) error {
	if _, err := s.hostedBillingOwner(c); err != nil {
		return s.hostedError(c, http.StatusForbidden, "Billing requires an organization owner without support impersonation")
	}
	if s.billing == nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Subscription checkout is not enabled for this organization")
	}
	w := s.billing
	w.mu.Lock()
	defer w.mu.Unlock()
	credential, err := s.hostedBillingOwner(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "Organization ownership changed; sign in again")
	}
	cfg := s.config.Hosted.Billing
	priceID := c.FormValue("price")
	approved := false
	for _, price := range cfg.Prices {
		approved = approved || price.PriceID == priceID
	}
	if !approved {
		return s.hostedError(c, http.StatusBadRequest, "Choose an approved subscription plan")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), 45*time.Second)
	defer cancel()
	if err := w.reconcile(ctx); err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "Billing is temporarily unavailable. Your current access deadline is unchanged.")
	}
	state, err := s.database.readHostedBilling(ctx)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if state.Snapshot.SubscriptionID != "" || state.Status == "multiple_subscriptions" {
		return s.hostedError(c, http.StatusConflict, "An existing subscription must be managed through the billing portal")
	}
	checkout, err := s.prepareHostedCheckout(ctx, credential.Hosted.Subject, priceID)
	if err != nil {
		return s.hostedError(c, http.StatusConflict, "A checkout is already pending. Retry the same plan or wait for that checkout to expire.")
	}
	if checkout.Session.URL == "" {
		session, err := cfg.Provider.Checkout(ctx, billing.CheckoutRequest{Binding: cfg.binding(s.config.Hosted.OrganizationID), PriceID: checkout.PriceID, IdempotencyKey: checkout.Key, ExpiresAt: checkout.ExpiresAt, ReturnURL: s.config.Hosted.PublicURL + "/organization/billing"})
		if err != nil {
			return s.hostedError(c, http.StatusServiceUnavailable, "Checkout is temporarily unavailable. Retry to resume the same purchase.")
		}
		checkout.Session = session
		if err := s.saveHostedCheckout(ctx, credential.Hosted.Subject, checkout, "checkout_created"); err != nil {
			return s.nativeAPIError(c, err)
		}
	}
	return c.Redirect(http.StatusSeeOther, checkout.Session.URL)
}

func (s *Service) prepareHostedCheckout(ctx context.Context, actor, price string) (hostedCheckout, error) {
	var checkout hostedCheckout
	var raw string
	if err := s.database.db.QueryRowContext(ctx, "SELECT checkout_json FROM hosted_billing_accounts WHERE organization_id=?", s.config.Hosted.OrganizationID).Scan(&raw); err != nil {
		return checkout, err
	}
	if err := json.Unmarshal([]byte(raw), &checkout); err != nil {
		return checkout, err
	}
	now := s.config.now()
	if checkout.ExpiresAt.After(now) {
		if checkout.PriceID != price {
			return checkout, errors.New("a different checkout is pending")
		}
		return checkout, nil
	}
	checkout = hostedCheckout{Key: "detent_" + s.config.newLeaseID(), PriceID: price, ExpiresAt: now.Truncate(time.Second).Add(time.Hour)}
	return checkout, s.saveHostedCheckout(ctx, actor, checkout, "checkout_requested")
}

func (s *Service) saveHostedCheckout(ctx context.Context, actor string, checkout hostedCheckout, action string) error {
	raw, err := json.Marshal(checkout)
	if err != nil {
		return err
	}
	audit, err := json.Marshal(struct {
		Key     string `json:"request_id"`
		PriceID string `json:"price_id"`
	}{checkout.Key, checkout.PriceID})
	if err != nil {
		return err
	}
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE hosted_billing_accounts SET checkout_json=? WHERE organization_id=?", string(raw), s.config.Hosted.OrganizationID); err != nil {
		return err
	}
	if err := s.database.insertBillingAudit(ctx, tx, actor, action, string(audit), s.config.now()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) hostedBillingPortal(c echo.Context) error {
	credential, err := s.hostedBillingOwner(c)
	if err != nil {
		return s.hostedError(c, http.StatusForbidden, "Billing requires an organization owner without support impersonation")
	}
	cfg := s.config.Hosted.Billing
	if cfg == nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "The subscription portal is not enabled for this organization")
	}
	if err := s.hostedAudit(c.Request().Context(), credential.Hosted, "billing_portal_requested", "/organization/billing/portal", "", http.StatusOK); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := s.recordBillingAction(c.Request().Context(), credential.Hosted.Subject, "portal_requested"); err != nil {
		return s.nativeAPIError(c, err)
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), 45*time.Second)
	defer cancel()
	session, err := cfg.Provider.Portal(ctx, cfg.binding(s.config.Hosted.OrganizationID), cfg.PortalConfigurationID, s.config.Hosted.PublicURL+"/organization/billing")
	if err != nil {
		return s.hostedError(c, http.StatusServiceUnavailable, "The billing portal is temporarily unavailable. Existing data and exports remain available.")
	}
	return c.Redirect(http.StatusSeeOther, session.URL)
}

func (s *Service) recordBillingAction(ctx context.Context, actor, action string) error {
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.database.insertBillingAudit(ctx, tx, actor, action, "{}", s.config.now()); err != nil {
		return err
	}
	return tx.Commit()
}
