package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
)

func TestDoctorTerminalAttemptRecovery(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		local string
		want  []string
	}{
		{name: "default", want: []string{"terminal_attempt_retry_limit=3", "failure 3", "cooldown recovery"}},
		{name: "local zero", local: "0", want: []string{"terminal_attempt_retry_limit=0", "failure 1", "external review", "no automatic cooldown recovery"}},
		{name: "local one", local: "1", want: []string{"terminal_attempt_retry_limit=1", "failure 2", "cooldown recovery"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			local := "---\n---\n"
			if tt.local != "" {
				local = "---\nrecovery:\n  terminal_attempt_retry_limit: " + tt.local + "\n---\n"
			}
			workflow, err := workflowconfig.ParseWorkflowOverlay([]byte("---\ntracker:\n  kind: memory\n---\n"), []byte(local), "detent.local.yaml")
			if err != nil {
				t.Fatal(err)
			}
			check := checkDoctorTerminalAttemptRecovery("example", workflow.Config)
			if check.Status != doctorOK || check.Name != "Project example terminal attempt recovery" {
				t.Fatalf("check = %#v", check)
			}
			for _, want := range append(tt.want, "workspace-preparation failure limit=3") {
				if !strings.Contains(check.Detail, want) {
					t.Fatalf("detail = %q, want %q", check.Detail, want)
				}
			}
		})
	}
}

func TestDoctorProjectDefinitionLayouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		legacy     bool
		split      bool
		promptOnly bool
		wantStatus doctorStatus
		wantLayout string
		wantFix    bool
	}{
		{name: "legacy", legacy: true, wantStatus: doctorWarn, wantLayout: "legacy", wantFix: true},
		{name: "split", split: true, wantStatus: doctorOK, wantLayout: "split"},
		{name: "mixed", legacy: true, split: true, wantStatus: doctorFail, wantLayout: "mixed"},
		{name: "incomplete", promptOnly: true, wantStatus: doctorFail, wantLayout: "incomplete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sourceRoot := t.TempDir()
			definitionRoot := filepath.Join(t.TempDir(), "external-definition")
			if err := os.MkdirAll(definitionRoot, 0o755); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			workflowPath := filepath.Join(definitionRoot, "WORKFLOW.md")
			configRaw := doctorProjectDefinitionConfig(t, sourceRoot)
			workflowRaw := []byte("Portable agent direction.\n")
			if tt.legacy {
				workflowRaw = append([]byte("---\n"), configRaw...)
				workflowRaw = append(workflowRaw, "---\nPortable agent direction.\n"...)
			}
			if err := os.WriteFile(workflowPath, workflowRaw, 0o644); err != nil {
				t.Fatalf("WriteFile(WORKFLOW.md) error = %v", err)
			}
			if tt.split {
				detentRaw := append([]byte("schema: 1\n"), configRaw...)
				if err := os.WriteFile(filepath.Join(definitionRoot, "detent.yaml"), detentRaw, 0o644); err != nil {
					t.Fatalf("WriteFile(detent.yaml) error = %v", err)
				}
			}

			deps := successfulDoctorDeps()
			deps.loadWorkflow = workflowconfig.LoadWorkflow
			checks := checkDoctorProject(context.Background(), globalconfig.Project{
				ID:       "alpha",
				Workflow: workflowPath,
				Workdir:  sourceRoot,
			}, deps, RuntimeSecret{}, false)
			if len(checks) == 0 {
				t.Fatal("checkDoctorProject() returned no checks")
			}
			check := checks[0]
			if check.Status != tt.wantStatus {
				t.Fatalf("status = %s, want %s; detail = %s", check.Status, tt.wantStatus, check.Detail)
			}
			if check.ProjectDefinition == nil {
				t.Fatal("ProjectDefinition = nil")
			}
			if check.ProjectDefinition.Layout != tt.wantLayout {
				t.Fatalf("layout = %q, want %q", check.ProjectDefinition.Layout, tt.wantLayout)
			}
			if (check.ProjectDefinition.FixCommand != "") != tt.wantFix {
				t.Fatalf("FixCommand = %q, want present=%t", check.ProjectDefinition.FixCommand, tt.wantFix)
			}
			if tt.wantFix && !strings.Contains(check.Hint, "detent fix workflow-layout --workflow") {
				t.Fatalf("Hint = %q, want exact fix command", check.Hint)
			}

			report := doctorReport{Checks: []doctorCheck{check}}
			var pretty bytes.Buffer
			if err := writeDoctorReport(&pretty, report); err != nil {
				t.Fatalf("writeDoctorReport() error = %v", err)
			}
			if !strings.Contains(pretty.String(), "layout="+tt.wantLayout) {
				t.Fatalf("pretty output = %q, want layout", pretty.String())
			}
			raw, err := json.Marshal(newDoctorOutputReport(report))
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			if !strings.Contains(string(raw), `"layout":"`+tt.wantLayout+`"`) {
				t.Fatalf("JSON output = %s, want layout", raw)
			}
		})
	}
}

func doctorProjectDefinitionConfig(t *testing.T, sourceRoot string) []byte {
	t.Helper()
	cfg := validDoctorWorkflow(sourceRoot)
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	return raw
}
