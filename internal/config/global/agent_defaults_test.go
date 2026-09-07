package global

import (
	"testing"

	"gopkg.in/yaml.v3"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
)

func TestParseGlobalAgentDefaults(t *testing.T) {
	for _, tt := range []struct {
		name, extra string
		invalid     bool
	}{
		{name: "preset", extra: "model_selection: {preset: sol_first}"},
		{name: "disabled", extra: "model_selection: {preset: sol_first, enabled: false}"},
		{name: "invalid reference", extra: "routes: [{name: design, backend: missing}]", invalid: true},
		{name: "invalid preset", extra: "model_selection: {preset: unknown}", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := yaml.Marshal(map[string]any{"apiVersion": "detent/v1", "kind": "GlobalConfig", "global": map[string]any{"max_concurrent_agents": 2, "scheduling": "weighted", "agents": map[string]any{}}, "projects": []any{}})
			if err != nil {
				t.Fatal(err)
			}
			var attrs map[string]any
			if err := yaml.Unmarshal(raw, &attrs); err != nil {
				t.Fatal(err)
			}
			var agents map[string]any
			if err := yaml.Unmarshal([]byte(tt.extra), &agents); err != nil {
				t.Fatal(err)
			}
			attrs["global"].(map[string]any)["agents"] = agents
			raw, err = yaml.Marshal(attrs)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Parse(raw, "", WithHome(t.TempDir()))
			if (err != nil) != tt.invalid {
				t.Fatalf("Parse error=%v", err)
			}
			if tt.invalid {
				return
			}
			policy := workflowconfig.ResolveModelSelection(got.Global.Agents.ModelSelection, workflowconfig.ModelSelection{})
			if policy.Active() != (tt.name == "preset") || policy.Model("normal") != "gpt-5.6-sol" {
				t.Fatalf("policy=%+v", policy)
			}
		})
	}
}
