package cli

import (
	"errors"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/billing"
	"github.com/digitaldrywood/detent/internal/hubserver"
)

type hostedFileConfig struct {
	Billing                  *hostedBillingFileConfig      `yaml:"billing"`
	EntitlementAdministrator string                        `yaml:"entitlement_administrator"`
	EntitlementAdminTokenEnv string                        `yaml:"entitlement_admin_token_env"`
	Plans                    *hubserver.HostedPlansConfig  `yaml:"entitlements"`
	OrganizationID           string                        `yaml:"organization_id"`
	WorkOSOrganizationID     string                        `yaml:"workos_organization_id"`
	BootstrapSubject         string                        `yaml:"bootstrap_subject"`
	PublicURL                string                        `yaml:"public_url"`
	StaffEmails              []string                      `yaml:"staff_emails"`
	SupportActors            []string                      `yaml:"support_actors"`
	PlanID                   string                        `yaml:"plan_id"`
	StorageQuotaBytes        int64                         `yaml:"storage_quota_bytes"`
	EventQuota               int64                         `yaml:"event_quota"`
	Directory                []hubserver.HostedDestination `yaml:"directory"`
	WorkOS                   struct {
		ClientID  string `yaml:"client_id"`
		APIKeyEnv string `yaml:"api_key_env"`
		APIURL    string `yaml:"api_url"`
		IssuerURL string `yaml:"issuer_url"`
	} `yaml:"workos"`
}

type hostedBillingFileConfig struct {
	AccountID             string                         `yaml:"account_id"`
	CustomerID            string                         `yaml:"customer_id"`
	PortalConfigurationID string                         `yaml:"portal_configuration_id"`
	APIKeyEnv             string                         `yaml:"api_key_env"`
	WebhookSecretEnv      string                         `yaml:"webhook_secret_env"`
	GraceSeconds          int64                          `yaml:"grace_seconds"`
	ReconcileSeconds      int64                          `yaml:"reconcile_seconds"`
	Prices                []hubserver.HostedBillingPrice `yaml:"prices"`
}

func readHostedBillingConfig(config *hostedBillingFileConfig, lookupEnv func(string) string) (*hubserver.HostedBillingConfig, error) {
	if config == nil {
		return nil, errors.New("billing configuration is required")
	}
	if !validEnvName(config.APIKeyEnv) || !validEnvName(config.WebhookSecretEnv) {
		return nil, errors.New("billing secret environment variable names are required and must be valid")
	}
	provider, err := billing.NewStripe(billing.StripeConfig{APIKey: lookupEnv(config.APIKeyEnv)})
	if err != nil {
		return nil, err
	}
	secret := lookupEnv(config.WebhookSecretEnv)
	if len(secret) < 16 || !strings.HasPrefix(secret, "whsec_") {
		return nil, errors.New("billing webhook secret is unavailable or invalid")
	}
	return &hubserver.HostedBillingConfig{
		AccountID: config.AccountID, CustomerID: config.CustomerID, PortalConfigurationID: config.PortalConfigurationID,
		WebhookSecret: []byte(secret), GraceSeconds: config.GraceSeconds, ReconcileSeconds: config.ReconcileSeconds,
		Prices: config.Prices, Provider: provider,
	}, nil
}

func readHostedConfig(path string, lookupEnv func(string) string) (result *hubserver.HostedConfig, enabled bool, resultErr error) {
	if strings.TrimSpace(path) == "" {
		return nil, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, errors.New("hosted configuration could not be opened")
	}
	defer func() {
		if err := file.Close(); err != nil {
			resultErr = errors.Join(resultErr, errors.New("hosted configuration could not be closed"))
		}
	}()
	decoder := yaml.NewDecoder(io.LimitReader(file, 128*1024))
	decoder.KnownFields(true)
	var config hostedFileConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, false, errors.New("hosted configuration is invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, false, errors.New("hosted configuration must contain one document")
	}
	if config.WorkOS.APIKeyEnv == "" {
		config.WorkOS.APIKeyEnv = "WORKOS_API_KEY"
	}
	if !validEnvName(config.WorkOS.APIKeyEnv) {
		return nil, false, errors.New("hosted API key environment variable name is invalid")
	}
	provider, err := auth.NewHostedProvider(auth.IdentityProviderWorkOS, auth.WorkOSConfig{
		APIURL: config.WorkOS.APIURL, IssuerURL: config.WorkOS.IssuerURL, ClientID: config.WorkOS.ClientID,
		APIKey: lookupEnv(config.WorkOS.APIKeyEnv), RedirectURL: config.PublicURL + "/auth/oidc/callback",
	})
	if err != nil {
		return nil, false, err
	}
	var entitlementToken []byte
	if config.EntitlementAdminTokenEnv != "" {
		if !validEnvName(config.EntitlementAdminTokenEnv) {
			return nil, false, errors.New("entitlement token environment name is invalid")
		}
		entitlementToken = []byte(lookupEnv(config.EntitlementAdminTokenEnv))
		if len(entitlementToken) < 32 {
			return nil, false, errors.New("entitlement administration token is unavailable or too short")
		}
	}
	var billingConfig *hubserver.HostedBillingConfig
	if config.Billing != nil {
		billingConfig, err = readHostedBillingConfig(config.Billing, lookupEnv)
		if err != nil {
			return nil, false, err
		}
	}
	return &hubserver.HostedConfig{
		Billing:                  billingConfig,
		EntitlementAdministrator: config.EntitlementAdministrator, EntitlementAdminToken: entitlementToken,
		Plans:          config.Plans,
		OrganizationID: config.OrganizationID, WorkOSOrganizationID: config.WorkOSOrganizationID,
		BootstrapSubject: config.BootstrapSubject, PublicURL: config.PublicURL,
		StaffEmails: config.StaffEmails, SupportActors: config.SupportActors, Directory: config.Directory, Provider: provider,
		PlanID: config.PlanID, StorageQuotaBytes: config.StorageQuotaBytes, EventQuota: config.EventQuota,
	}, true, nil
}
