package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/hubserver"
)

func TestReadHostedConfig(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, body, key string
		wantError       bool
	}{
		{name: "valid", body: "organization_id: org_customer\nbootstrap_subject: user_customer\npublic_url: https://tenant.example.test\nworkos:\n  client_id: client_example\n", key: "test-workos-key"},
		{name: "unknown content field", body: "customer_prompt: content-sentinel\n", wantError: true},
		{name: "literal key rejected", body: "workos:\n  api_key: credential-sentinel\n", wantError: true},
		{name: "missing key", body: "public_url: https://tenant.example.test\nworkos:\n  client_id: client_example\n", wantError: true},
		{name: "multiple documents", body: "organization_id: org_customer\n---\nprivate: content-sentinel\n", wantError: true},
		{name: "invalid yaml", body: "x: [content-sentinel", wantError: true},
		{name: "invalid env name", body: "workos:\n  api_key_env: bad-name\n", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "hosted.yaml")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			config, enabled, err := readHostedConfig(path, func(name string) string {
				if name != "WORKOS_API_KEY" {
					t.Errorf("unexpected environment lookup %q", name)
				}
				return test.key
			})
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, want error %v", err, test.wantError)
			}
			if err != nil && (strings.Contains(err.Error(), "sentinel") || strings.Contains(err.Error(), test.body)) {
				t.Fatal("configuration error contains private input")
			}
			if err == nil && (!enabled || config.OrganizationID != "org_customer" || config.Provider == nil) {
				t.Fatal("hosted configuration was not constructed")
			}
		})
	}
	config, enabled, err := readHostedConfig("", func(string) string { t.Fatal("local config read cloud credential"); return "" })
	if err != nil || config != nil || enabled {
		t.Fatalf("local config = %v, %v", config, err)
	}
	if _, _, err := readHostedConfig(filepath.Join(t.TempDir(), "absent"), func(string) string { return "" }); err == nil {
		t.Fatal("missing configuration accepted")
	}
}

func TestReadHostedEntitlementConfig(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, variable, token string
		wantError             bool
	}{
		{name: "configured", variable: "TEST_ENTITLEMENT_TOKEN", token: strings.Repeat("x", 32)},
		{name: "missing secret", variable: "TEST_ENTITLEMENT_TOKEN", wantError: true},
		{name: "short secret", variable: "TEST_ENTITLEMENT_TOKEN", token: "private-sentinel", wantError: true},
		{name: "invalid variable", variable: "bad-name", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := "organization_id: org_customer\nbootstrap_subject: user_customer\npublic_url: https://tenant.example.test\nworkos:\n  client_id: client_example\nentitlement_administrator: pilot_operator\nentitlement_admin_token_env: " + test.variable + "\nentitlements:\n  base: {id: pilot, version: 2}\n  window_seconds: 3600\n  retention_windows: 24\n  connected_seconds: 90\n  invitation_seconds: 86400\n  plans:\n    - id: pilot\n      version: 2\n      features: [collaboration]\n      allowances: {projects: 3}\n"
			path := filepath.Join(t.TempDir(), "hosted.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			config, enabled, err := readHostedConfig(path, func(name string) string {
				switch name {
				case "WORKOS_API_KEY":
					return "workos-fixture"
				case "TEST_ENTITLEMENT_TOKEN":
					return test.token
				default:
					t.Errorf("unexpected environment lookup %q", name)
					return ""
				}
			})
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, want error %v", err, test.wantError)
			}
			if err != nil {
				if test.token != "" && strings.Contains(err.Error(), test.token) {
					t.Fatal("configuration error exposed the credential")
				}
				return
			}
			if !enabled || config.EntitlementAdministrator != "pilot_operator" || string(config.EntitlementAdminToken) != test.token || config.Plans == nil || config.Plans.Base.ID != "pilot" || config.Plans.Base.Version != 2 || len(config.Plans.Plans) != 1 || config.Plans.Plans[0].Allowances["projects"] != 3 {
				t.Fatal("entitlement configuration was not preserved")
			}
		})
	}
}

func TestHubServeIdentityModes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		maintenance bool
		name        string
		hosted      bool
		native      bool
		wantMode    bool
	}{
		{name: "local compatibility"},
		{name: "local native", native: true, wantMode: true},
		{name: "hosted without GitHub flag", hosted: true, wantMode: true},
		{name: "hosted credential maintenance", hosted: true, maintenance: true, wantMode: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			args := []string{"serve", "--database", filepath.Join(directory, "hub.db"), "--listen", "127.0.0.1:0"}
			if test.hosted {
				path := filepath.Join(directory, "hosted.yaml")
				body := "organization_id: org_customer\nbootstrap_subject: user_customer\npublic_url: https://tenant.example.test\nworkos:\n  client_id: client_example\n"
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--hosted-config", path)
			}
			if test.native {
				args = append(args, "--github-disabled")
			}
			if test.maintenance {
				args = append(args, "--credential-maintenance")
			}
			called := false
			cmd := newHubCommandWithRun("test", func(name string) string {
				switch name {
				case "DETENT_HUB_ADMIN_TOKEN":
					return "reporter-fixture"
				case "WORKOS_API_KEY":
					if !test.hosted {
						t.Error("local mode resolved a WorkOS credential")
					}
					return "workos-fixture"
				default:
					return ""
				}
			}, func(_ context.Context, cfg hubserver.Config) error {
				called = true
				if cfg.CredentialMaintenance != test.maintenance || cfg.GitHubDisabled != test.wantMode || (cfg.Hosted != nil) != test.hosted {
					t.Errorf("mode = native %t, hosted %t", cfg.GitHubDisabled, cfg.Hosted != nil)
				}
				return nil
			})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(args)
			if err := cmd.ExecuteContext(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("Hub was not started")
			}
		})
	}
}
