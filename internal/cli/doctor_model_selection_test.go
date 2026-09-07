package cli

import (
	"strings"
	"testing"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
)

func TestDoctorModelSelectionProvenance(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			cfg := workflowconfig.Default()
			cfg.Agents.ModelSelection.ComplexModel = new("project-complex")
			cfg.Agents.ModelSelection.Enabled = &enabled
			host := workflowconfig.Agents{
				Backends:       []workflowconfig.AgentBackend{{ID: "design", Kind: workflowconfig.AgentBackendClaudeCode, Command: "SECRET_ENV=value /private/secret-wrapper"}},
				Routes:         []workflowconfig.AgentRoute{{Name: "design", Backend: "design", Model: "sonnet"}},
				ModelSelection: workflowconfig.ModelSelection{Preset: new("sol_first"), NormalModel: new("host-normal")},
			}
			got := checkDoctorModelSelection("alpha", cfg.WithAgentDefaults(host, workflowconfig.AgentBudgetDefaults{}))
			if got.Status != doctorOK {
				t.Fatalf("check = %+v", got)
			}
			for _, want := range []string{"normal_model=instance", "complex_model=project", "levels.normal.effort=preset", "backend design (claude_code): instance", "route design: instance", "subscription charges"} {
				if !strings.Contains(got.Detail, want) {
					t.Errorf("detail missing %q", want)
				}
			}
			for _, secret := range []string{"SECRET_ENV", "secret-wrapper", "/private"} {
				if strings.Contains(got.Detail, secret) {
					t.Errorf("detail leaks %q", secret)
				}
			}
		})
	}
}
