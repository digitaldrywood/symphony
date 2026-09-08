package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadHostedBillingConfig(t *testing.T) {
	t.Parallel()
	base := "organization_id: org_fixture\npublic_url: https://cloud.example.test\nworkos:\n  client_id: client_fixture\nbilling:\n  account_id: acct_fixture\n  customer_id: cus_fixture\n  portal_configuration_id: bpc_fixture\n  api_key_env: TEST_STRIPE_KEY\n  webhook_secret_env: TEST_STRIPE_WEBHOOK\n  grace_seconds: 3600\n  reconcile_seconds: 120\n  prices:\n    - price_id: price_fixture\n      label: Extended pilot\n      plan: {id: extended, version: 2}\n"
	for _, test := range []struct {
		name, replace, with, key, secret string
		wantError                        bool
	}{
		{name: "test configuration", key: "sk_test_fixture_2196", secret: "whsec_fixture_2196_secret"},
		{name: "restricted test key", key: "rk_test_fixture_2196", secret: "whsec_fixture_2196_secret"},
		{name: "live key", key: "sk_live_private_sentinel", secret: "whsec_fixture_2196_secret", wantError: true},
		{name: "missing key", secret: "whsec_fixture_2196_secret", wantError: true},
		{name: "missing webhook", key: "sk_test_fixture_2196", wantError: true},
		{name: "bad variable", replace: "TEST_STRIPE_KEY", with: "bad-name", key: "sk_test_fixture_2196", secret: "whsec_fixture_2196_secret", wantError: true},
		{name: "literal key rejected", replace: "api_key_env: TEST_STRIPE_KEY", with: "api_key: private_sentinel", wantError: true},
		{name: "live setting rejected", replace: "account_id: acct_fixture", with: "livemode: true", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := base
			if test.replace != "" {
				body = strings.Replace(body, test.replace, test.with, 1)
			}
			path := filepath.Join(t.TempDir(), "hosted.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			config, enabled, err := readHostedConfig(path, func(name string) string {
				switch name {
				case "WORKOS_API_KEY":
					return "workos_fixture"
				case "TEST_STRIPE_KEY":
					return test.key
				case "TEST_STRIPE_WEBHOOK":
					return test.secret
				default:
					t.Errorf("unexpected environment lookup %q", name)
					return ""
				}
			})
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				if strings.Contains(err.Error(), "sentinel") {
					t.Fatal("error exposed a secret")
				}
				return
			}
			if !enabled || config.Billing == nil || config.Billing.Provider == nil || config.Billing.AccountID != "acct_fixture" || config.Billing.CustomerID != "cus_fixture" || config.Billing.Prices[0].Plan.Version != 2 || config.Billing.GraceSeconds != 3600 || config.Billing.ReconcileSeconds != 120 {
				t.Fatal("billing configuration was not preserved")
			}
		})
	}
}
