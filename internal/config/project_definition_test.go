package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadProjectDefinitionTerminalAttemptRecovery(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		shared   string
		local    string
		want     int
		failures int
		wantErr  string
	}{
		{name: "default", want: 3, failures: 3},
		{name: "shared zero", shared: "0", failures: 1},
		{name: "shared one", shared: "1", want: 1, failures: 2},
		{name: "shared three", shared: "3", want: 3, failures: 3},
		{name: "overlay zero", shared: "3", local: "0", failures: 1},
		{name: "overlay one", shared: "0", local: "1", want: 1, failures: 2},
		{name: "overlay three", shared: "0", local: "3", want: 3, failures: 3},
		{name: "higher limit", shared: "6", want: 6, failures: 6},
		{name: "negative shared", shared: "-1", wantErr: "recovery.terminal_attempt_retry_limit must be greater than or equal to 0"},
		{name: "negative overlay", shared: "3", local: "-1", wantErr: "recovery.terminal_attempt_retry_limit must be greater than or equal to 0"},
		{name: "invalid type", shared: "false", wantErr: "cannot unmarshal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			workflowPath := filepath.Join(dir, "WORKFLOW.md")
			writeProjectDefinitionTestFile(t, workflowPath, "Implement the issue.\n", 0o644)
			shared := "schema: 1\ntracker:\n  kind: memory\n"
			if tt.shared != "" {
				shared += "recovery:\n  terminal_attempt_retry_limit: " + tt.shared + "\n"
			}
			writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.yaml"), shared, 0o644)
			if tt.local != "" {
				writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.local.yaml"), "schema: 1\nrecovery:\n  terminal_attempt_retry_limit: "+tt.local+"\n", 0o600)
			}
			workflow, err := LoadProjectDefinition(workflowPath)
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("LoadProjectDefinition() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := workflow.Config.Recovery.EffectiveTerminalAttemptRetryLimit(); got != tt.want {
				t.Fatalf("effective limit = %d, want %d", got, tt.want)
			}
			if got := workflow.Config.Recovery.TerminalAttemptFailureLimit(); got != tt.failures {
				t.Fatalf("failure limit = %d, want %d", got, tt.failures)
			}
		})
	}
}

func TestLoadProjectDefinitionLayouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		workflow    string
		config      string
		local       string
		localConfig string
		wantLayout  ProjectDefinitionLayout
		wantPrompt  string
		wantKeys    []string
		wantErr     string
	}{
		{
			name:       "legacy",
			workflow:   "---\ntracker:\n  kind: memory\n---\nShared direction.\n",
			wantLayout: ProjectDefinitionLegacy,
			wantPrompt: "Shared direction.\n",
			wantKeys:   []string{"tracker"},
		},
		{
			name:       "split",
			workflow:   "Shared direction.\n",
			config:     "schema: 1\ntracker:\n  kind: memory\n",
			wantLayout: ProjectDefinitionSplit,
			wantPrompt: "Shared direction.\n",
		},
		{
			name:        "split with local files",
			workflow:    "Shared direction.\n",
			config:      "schema: 1\ntracker:\n  kind: memory\nagent:\n  max_turns: 10\n",
			local:       "Local direction.\n",
			localConfig: "schema: 1\nagent:\n  max_turns: 12\n",
			wantLayout:  ProjectDefinitionSplit,
			wantPrompt:  "Shared direction.\n\n---\n\n## Machine-local workflow overlay\n\nLocal direction.\n",
		},
		{
			name:       "mixed shared authority",
			workflow:   "---\ntracker:\n  kind: memory\n---\nShared direction.\n",
			config:     "schema: 1\ntracker:\n  kind: memory\n",
			wantLayout: ProjectDefinitionMixed,
			wantErr:    "ambiguous authority",
		},
		{
			name:       "mixed local authority",
			workflow:   "Shared direction.\n",
			config:     "schema: 1\ntracker:\n  kind: memory\n",
			local:      "---\nagent:\n  max_turns: 12\n---\nLocal direction.\n",
			wantLayout: ProjectDefinitionMixed,
			wantErr:    "WORKFLOW.local.md structured frontmatter",
		},
		{
			name:       "incomplete",
			workflow:   "Shared direction.\n",
			wantLayout: ProjectDefinitionIncomplete,
			wantErr:    "detent.yaml is missing",
		},
		{
			name:       "unsupported schema",
			workflow:   "Shared direction.\n",
			config:     "schema: 2\ntracker:\n  kind: memory\n",
			wantLayout: ProjectDefinitionSplit,
			wantErr:    "unsupported schema version 2",
		},
		{
			name:       "missing schema",
			workflow:   "Shared direction.\n",
			config:     "tracker:\n  kind: memory\n",
			wantLayout: ProjectDefinitionSplit,
			wantErr:    "schema is required",
		},
		{
			name:       "invalid split YAML",
			workflow:   "Shared direction.\n",
			config:     "schema: 1\ntracker: [\n",
			wantLayout: ProjectDefinitionSplit,
			wantErr:    "parse",
		},
		{
			name:       "invalid legacy YAML",
			workflow:   "---\ntracker: [\n---\nShared direction.\n",
			wantLayout: ProjectDefinitionLegacy,
			wantErr:    "parse legacy workflow config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			workflowPath := filepath.Join(dir, "WORKFLOW.md")
			writeProjectDefinitionTestFile(t, workflowPath, tt.workflow, 0o644)
			if tt.config != "" {
				writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.yaml"), tt.config, 0o644)
			}
			if tt.local != "" {
				writeProjectDefinitionTestFile(t, filepath.Join(dir, "WORKFLOW.local.md"), tt.local, 0o600)
			}
			if tt.localConfig != "" {
				writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.local.yaml"), tt.localConfig, 0o600)
			}

			workflow, err := LoadProjectDefinition(workflowPath)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("LoadProjectDefinition() error = %v, want containing %q", err, tt.wantErr)
				}
				var definitionErr *ProjectDefinitionError
				if !errors.As(err, &definitionErr) {
					t.Fatalf("error type = %T, want *ProjectDefinitionError", err)
				}
				if definitionErr.Definition.Layout != tt.wantLayout {
					t.Fatalf("error layout = %q, want %q", definitionErr.Definition.Layout, tt.wantLayout)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadProjectDefinition() error = %v", err)
			}
			if workflow.Definition.Layout != tt.wantLayout {
				t.Fatalf("layout = %q, want %q", workflow.Definition.Layout, tt.wantLayout)
			}
			if workflow.Prompt != tt.wantPrompt {
				t.Fatalf("Prompt = %q, want %q", workflow.Prompt, tt.wantPrompt)
			}
			if !reflect.DeepEqual(workflow.Definition.LegacyKeys, tt.wantKeys) {
				t.Fatalf("LegacyKeys = %#v, want %#v", workflow.Definition.LegacyKeys, tt.wantKeys)
			}
			if workflow.Definition.Revision == "" {
				t.Fatal("Revision is blank")
			}
		})
	}
}

func TestLoadProjectDefinitionMissingWorkflowIsIncomplete(t *testing.T) {
	t.Parallel()

	workflowPath := filepath.Join(t.TempDir(), "WORKFLOW.md")
	_, err := LoadProjectDefinition(workflowPath)
	if err == nil {
		t.Fatal("LoadProjectDefinition() error = nil")
	}
	var definitionErr *ProjectDefinitionError
	if !errors.As(err, &definitionErr) {
		t.Fatalf("error type = %T, want *ProjectDefinitionError", err)
	}
	if definitionErr.Definition.Layout != ProjectDefinitionIncomplete {
		t.Fatalf("layout = %q, want incomplete", definitionErr.Definition.Layout)
	}
}

func TestLoadProjectDefinitionUsesConfiguredExternalRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	definitionRoot := filepath.Join(root, "orchestration", "detent")
	workflowPath := filepath.Join(definitionRoot, "WORKFLOW.md")
	writeProjectDefinitionTestFile(t, workflowPath, "External direction.\n", 0o644)
	writeProjectDefinitionTestFile(t, filepath.Join(definitionRoot, "detent.yaml"), "schema: 1\ntracker:\n  kind: memory\n", 0o644)

	workflow, err := LoadProjectDefinition(workflowPath)
	if err != nil {
		t.Fatalf("LoadProjectDefinition() error = %v", err)
	}
	if workflow.Definition.ConfigPath != filepath.Join(definitionRoot, "detent.yaml") {
		t.Fatalf("ConfigPath = %q, want configured definition root", workflow.Definition.ConfigPath)
	}
}

func TestLoadProjectDefinitionWorkspaceCleanupConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cleanup       string
		wantIdleTTL   int
		wantSweepTime int
	}{
		{
			name:          "omitted cleanup uses safe defaults",
			wantIdleTTL:   86400000,
			wantSweepTime: 600000,
		},
		{
			name:          "configured cleanup is preserved",
			cleanup:       "  cleanup_idle_ttl_ms: 172800000\n  cleanup_sweep_interval_ms: 300000\n",
			wantIdleTTL:   172800000,
			wantSweepTime: 300000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			workflowPath := filepath.Join(dir, "WORKFLOW.md")
			writeProjectDefinitionTestFile(t, workflowPath, "Project instructions.\n", 0o644)
			writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.yaml"), "schema: 1\ntracker:\n  kind: memory\nworkspace:\n  cache_strategy: shared\n  auto_branch: true\n"+tt.cleanup, 0o644)
			writeProjectDefinitionTestFile(t, filepath.Join(dir, "detent.local.yaml"), "schema: 1\nworkspace:\n  root: "+filepath.Join(dir, "workspaces")+"\n  source_root: "+filepath.Join(dir, "source")+"\n", 0o600)

			workflow, err := LoadProjectDefinition(workflowPath)
			if err != nil {
				t.Fatalf("LoadProjectDefinition() error = %v", err)
			}
			if workflow.Config.Workspace.CleanupIdleTTLMS != tt.wantIdleTTL {
				t.Fatalf("CleanupIdleTTLMS = %d, want %d", workflow.Config.Workspace.CleanupIdleTTLMS, tt.wantIdleTTL)
			}
			if workflow.Config.Workspace.CleanupSweepIntervalMS != tt.wantSweepTime {
				t.Fatalf("CleanupSweepIntervalMS = %d, want %d", workflow.Config.Workspace.CleanupSweepIntervalMS, tt.wantSweepTime)
			}
		})
	}
}

func writeProjectDefinitionTestFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}
