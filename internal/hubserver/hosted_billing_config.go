package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/digitaldrywood/detent/internal/billing"
)

type HostedBillingPrice struct {
	PriceID string        `yaml:"price_id"`
	Label   string        `yaml:"label"`
	Plan    PlanReference `yaml:"plan"`
}

type HostedBillingConfig struct {
	AccountID             string
	CustomerID            string
	PortalConfigurationID string
	WebhookSecret         []byte
	GraceSeconds          int64
	ReconcileSeconds      int64
	Prices                []HostedBillingPrice
	Provider              billing.Provider
}

func (c *HostedBillingConfig) validate(plans *HostedPlansConfig) error {
	if c == nil {
		return nil
	}
	if plans == nil || c.Provider == nil || !strings.HasPrefix(c.AccountID, "acct_") || !hostedSafeID(c.AccountID) || !strings.HasPrefix(c.CustomerID, "cus_") || !hostedSafeID(c.CustomerID) || !strings.HasPrefix(c.PortalConfigurationID, "bpc_") || !hostedSafeID(c.PortalConfigurationID) || len(c.WebhookSecret) < 16 || !strings.HasPrefix(string(c.WebhookSecret), "whsec_") || c.GraceSeconds < 0 || c.GraceSeconds > 7*86400 || c.ReconcileSeconds < 60 || c.ReconcileSeconds > 3600 || len(c.Prices) == 0 || len(c.Prices) > 20 {
		return errors.New("hosted billing requires a test provider, explicit account/customer/portal binding, webhook secret, plans and bounded grace/reconciliation settings")
	}
	seen := make(map[string]bool)
	for _, price := range c.Prices {
		found := false
		for _, plan := range plans.Plans {
			found = found || plan.PlanReference == price.Plan
		}
		if !strings.HasPrefix(price.PriceID, "price_") || !hostedSafeID(price.PriceID) || seen[price.PriceID] || strings.TrimSpace(price.Label) == "" || len(price.Label) > 80 || !found || price.Plan == plans.Base {
			return errors.New("hosted billing prices require unique approved paid plans and bounded labels")
		}
		seen[price.PriceID] = true
	}
	return nil
}

func (c *HostedBillingConfig) binding(organization string) billing.Binding {
	return billing.Binding{AccountID: c.AccountID, CustomerID: c.CustomerID, OrganizationID: organization}
}

func (d *database) configureHostedBilling(ctx context.Context, cfg *HostedConfig) error {
	if cfg == nil || cfg.Billing == nil {
		return nil
	}
	c := cfg.Billing
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_billing_accounts(organization_id,account_id,customer_id) VALUES(?,?,?) ON CONFLICT DO NOTHING`, d.hostedOrganization, c.AccountID, c.CustomerID); err != nil {
		return err
	}
	var account, customer string
	if err := tx.QueryRowContext(ctx, "SELECT account_id,customer_id FROM hosted_billing_accounts WHERE organization_id=?", d.hostedOrganization).Scan(&account, &customer); err != nil {
		return err
	}
	if account != c.AccountID || customer != c.CustomerID {
		return errors.New("hosted Stripe account/customer binding is immutable")
	}
	for _, price := range c.Prices {
		var existing PlanReference
		err := tx.QueryRowContext(ctx, "SELECT plan_id,plan_version FROM hosted_billing_prices WHERE price_id=?", price.PriceID).Scan(&existing.ID, &existing.Version)
		if err == nil && existing != price.Plan {
			return errors.New("hosted Stripe price mappings are immutable; configure a new price")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO hosted_billing_prices(price_id,plan_id,plan_version) VALUES(?,?,?) ON CONFLICT DO NOTHING", price.PriceID, price.Plan.ID, price.Plan.Version); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.hostedBilling = true
	return nil
}
