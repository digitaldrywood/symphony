package global

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseHubClient(t *testing.T) {
	t.Parallel()
	cfg, err := Parse([]byte(`apiVersion: detent/v1
kind: GlobalConfig
client:
  hub_url: https://hub.example.test/
  token_env: HUB_TOKEN
  organization_id: org_example
  native_projects:
    local: prj_example
  machine_id: machine-a
  display_name: Worker A
  capacity: 3
  heartbeat_interval_seconds: 20
  lease_ttl_seconds: 75
  request_timeout_ms: 2500
global:
  max_concurrent_agents: 8
  scheduling: weighted
projects: []
`), "hub-client.yaml")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	client := cfg.Client.Normalized()
	if client.OrganizationID != "org_example" || client.NativeProjects["local"] != "prj_example" {
		t.Fatalf("native mapping = %#v", client)
	}
	if client.URL != "https://hub.example.test" || client.TokenEnvironment != "HUB_TOKEN" || client.MachineID != "machine-a" || client.DisplayName != "Worker A" || client.Capacity != 3 {
		t.Fatalf("Client = %#v", client)
	}
	if client.HeartbeatInterval() != 20*time.Second || client.LeaseTTL() != 75*time.Second || client.RequestTimeout() != 2500*time.Millisecond {
		t.Fatalf("Client durations = heartbeat %s TTL %s timeout %s", client.HeartbeatInterval(), client.LeaseTTL(), client.RequestTimeout())
	}
}

func TestHubRunnerIdentityConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*HubClient)
		valid  bool
	}{
		{"enrolled identity", func(*HubClient) {}, true},
		{"durable artifacts", func(c *HubClient) {
			c.ArtifactServiceID = "service_" + strings.Repeat("a", 32)
			c.ArtifactBytes = 4 << 20
		}, true},
		{"artifact budget missing", func(c *HubClient) { c.ArtifactServiceID = "service_" + strings.Repeat("a", 32) }, false},
		{"artifact service malformed", func(c *HubClient) { c.ArtifactServiceID = "service_bad"; c.ArtifactBytes = 4 << 20 }, false},
		{"artifact budget only", func(c *HubClient) { c.ArtifactBytes = 4 << 20 }, false},
		{"relative path", func(c *HubClient) { c.IdentityFile = "identity.json" }, false},
		{"ambiguous token source", func(c *HubClient) { c.TokenEnvironment = "LEGACY_TOKEN" }, false},
		{"no projects", func(c *HubClient) { c.NativeProjects = nil }, false},
		{"provider reports", func(c *HubClient) { c.ProviderCapacityFile = filepath.Join(t.TempDir(), "reports.json") }, true},
		{"relative provider reports", func(c *HubClient) { c.ProviderCapacityFile = "reports.json" }, false},
		{"unenrolled provider reports", func(c *HubClient) {
			c.ProviderCapacityFile = filepath.Join(t.TempDir(), "reports.json")
			c.IdentityFile = ""
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := HubClient{URL: "https://hub.example.test", IdentityFile: filepath.Join(t.TempDir(), "private", "identity.json"), OrganizationID: "org_example", NativeProjects: map[string]string{"local": "prj_example"}}
			test.change(&config)
			if valid := len(config.Validate()) == 0; valid != test.valid {
				t.Fatalf("valid=%v, want %v: %v", valid, test.valid, config.Validate())
			}
			if config.IsZero() {
				t.Fatal("configured identity is zero")
			}
			if test.valid && config.Normalized().TokenEnvironment != "" {
				t.Fatal("enrolled mode gained legacy token fallback")
			}
		})
	}
}

func TestHubClientValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config string
		want   string
	}{
		{name: "mapping", config: "client: enabled", want: "client: must be a mapping"},
		{name: "native organization", config: "client:\n  hub_url: https://hub.example.test\n  native_projects:\n    local: prj_example", want: "client.organization_id is required"},
		{name: "native project path", config: "client:\n  hub_url: https://hub.example.test\n  organization_id: org_example\n  native_projects:\n    local: prj_bad/path", want: "client.native_projects must map"},
		{name: "URL required", config: "client:\n  capacity: 2", want: "client.hub_url is required"},
		{name: "absolute URL", config: "client:\n  hub_url: hub.internal", want: "client.hub_url must be an absolute"},
		{name: "token environment", config: "client:\n  hub_url: https://hub.example.test\n  token_env: invalid-name", want: "client.token_env must be an environment variable name"},
		{name: "heartbeat before lease", config: "client:\n  hub_url: https://hub.example.test\n  heartbeat_interval_seconds: 90\n  lease_ttl_seconds: 90", want: "client.heartbeat_interval_seconds must be shorter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse([]byte(`apiVersion: detent/v1
kind: GlobalConfig
`+test.config+`
global:
  max_concurrent_agents: 8
  scheduling: weighted
projects: []
`), test.name+".yaml")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want containing %q", err, test.want)
			}
		})
	}
}
