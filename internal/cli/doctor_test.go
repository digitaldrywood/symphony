package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/buildinfo"
	"github.com/digitaldrywood/detent/internal/codex"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	ghconnector "github.com/digitaldrywood/detent/internal/connector/github"
	"github.com/digitaldrywood/detent/internal/instancelock"
	runnerpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/workpad"
)

// This deadline only detects broken test synchronization and is deliberately
// generous enough for loaded hosted runners; timeout behavior uses controlled timers.
const doctorTestSafetyTimeout = 30 * time.Second

func TestCheckDoctorUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        globalconfig.Update
		wantStatus doctorStatus
		wantDetail string
	}{
		{
			name:       "unset suggests automatic checks",
			wantStatus: doctorWarn,
			wantDetail: "update.auto_check_enabled",
		},
		{
			name: "enabled is healthy",
			cfg: globalconfig.Update{
				AutoCheckEnabled:   true,
				CheckIntervalHours: 12,
			},
			wantStatus: doctorOK,
			wantDetail: "enabled every 12 hours",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := checkDoctorUpdate(tt.cfg, nil)
			if got.Status != tt.wantStatus || !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("checkDoctorUpdate() = %#v, want status %s detail %q", got, tt.wantStatus, tt.wantDetail)
			}
		})
	}
}

func TestCheckDoctorUpdateRuntimeShowsHealthTimestamps(t *testing.T) {
	t.Parallel()

	lastCheck := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	nextCheck := lastCheck.Add(24 * time.Hour)
	port := 4101
	got := checkDoctorUpdateRuntime(context.Background(), globalconfig.Update{
		AutoCheckEnabled: true,
	}, BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{
		httpDo: func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "http://127.0.0.1:4101/health" {
				t.Fatalf("request URL = %q", req.URL)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"update":{"enabled":true,"state":"scheduled","last_check_at":"` + lastCheck.Format(time.RFC3339) + `","last_applied_version":"1.2.4","next_check_at":"` + nextCheck.Format(time.RFC3339) + `"}}`)),
			}, nil
		},
	})
	for _, want := range []string{lastCheck.Format(time.RFC3339), "last applied version: 1.2.4", nextCheck.Format(time.RFC3339)} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail = %q, want %q", got.Detail, want)
		}
	}
}

func TestCheckDoctorBinary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resolve    func(string, string) (string, error)
		runCommand func(context.Context, string, []string, string, ...string) error
		want       doctorStatus
		wantDetail string
	}{
		{
			name: "missing from path",
			resolve: func(string, string) (string, error) {
				return "", errors.New("missing")
			},
			runCommand: func(context.Context, string, []string, string, ...string) error {
				return nil
			},
			want:       doctorFail,
			wantDetail: "checked PATH:",
		},
		{
			name: "not runnable",
			resolve: func(string, string) (string, error) {
				return "/usr/bin/codex", nil
			},
			runCommand: func(context.Context, string, []string, string, ...string) error {
				return errors.New("permission denied")
			},
			want:       doctorFail,
			wantDetail: "permission denied",
		},
		{
			name: "runnable",
			resolve: func(string, string) (string, error) {
				return "/usr/bin/codex", nil
			},
			runCommand: func(context.Context, string, []string, string, ...string) error {
				return nil
			},
			want:       doctorOK,
			wantDetail: "is runnable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := checkDoctorBinary(context.Background(), doctorDeps{
				resolveCommandOnPath: tt.resolve,
				runCommandInDir:      tt.runCommand,
			}, doctorBinaryEnvironment{
				Source:         doctorBinaryPathSourceDoctor,
				CheckedPath:    "/doctor/bin",
				DoctorPath:     "/doctor/bin",
				FallbackReason: "orchestrator PATH is unavailable because no running instance was reachable",
			}, "codex", "codex binary", "--version", "install codex")
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s", got.Status, tt.want)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
		})
	}
}

func TestDoctorBinaryResolutionUsesSelectedEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		binary               string
		doctorPath           string
		orchestratorPath     string
		reachableErr         error
		wantSource           string
		wantCheckedPath      string
		wantDiffer           bool
		wantDetail           []string
		wantOrchestratorPath string
	}{
		{
			name:                 "agreeing environments use orchestrator",
			binary:               "codex",
			doctorPath:           "/shared/bin",
			orchestratorPath:     "/shared/bin",
			wantSource:           doctorBinaryPathSourceOrchestrator,
			wantCheckedPath:      "/shared/bin",
			wantDetail:           []string{"checked PATH: /shared/bin (orchestrator)"},
			wantOrchestratorPath: "/shared/bin",
		},
		{
			name:                 "binary present only in differing orchestrator environment",
			binary:               "claude",
			doctorPath:           "/doctor/bin",
			orchestratorPath:     "/orchestrator/bin",
			wantSource:           doctorBinaryPathSourceOrchestrator,
			wantCheckedPath:      "/orchestrator/bin",
			wantDiffer:           true,
			wantDetail:           []string{filepath.Join("/orchestrator/bin", "claude") + " is runnable", "doctor PATH: /doctor/bin", "doctor PATH differs from orchestrator PATH"},
			wantOrchestratorPath: "/orchestrator/bin",
		},
		{
			name:            "no reachable instance falls back to doctor",
			binary:          "git",
			doctorPath:      "/doctor/bin",
			reachableErr:    errors.New("connection refused"),
			wantSource:      doctorBinaryPathSourceDoctor,
			wantCheckedPath: "/doctor/bin",
			wantDetail:      []string{"checked PATH: /doctor/bin (doctor fallback)", "no running instance was reachable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := doctorDeps{
				lookupEnv: func(key string) string {
					if key != "PATH" {
						t.Fatalf("lookupEnv(%q), want PATH", key)
					}
					return tt.doctorPath
				},
				httpDo: func(*http.Request) (*http.Response, error) {
					if tt.reachableErr != nil {
						return nil, tt.reachableErr
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Body: io.NopCloser(strings.NewReader(
							`{"status":"ok","mode":"running","checks":{"hub":"configured","store":"configured","registry":"configured","connector":"configured"},"environment":{"path":"` + tt.orchestratorPath + `"}}`,
						)),
					}, nil
				},
				resolveCommandOnPath: func(pathValue string, binary string) (string, error) {
					if binary != tt.binary {
						t.Fatalf("binary = %q, want %q", binary, tt.binary)
					}
					if pathValue != tt.wantCheckedPath {
						t.Fatalf("pathValue = %q, want %q", pathValue, tt.wantCheckedPath)
					}
					return filepath.Join(tt.wantCheckedPath, binary), nil
				},
				runCommandInDir: func(_ context.Context, _ string, environment []string, _ string, _ ...string) error {
					wantEnvironment := []string{"PATH=" + tt.wantCheckedPath}
					if !slices.Equal(environment, wantEnvironment) {
						t.Fatalf("environment = %#v, want %#v", environment, wantEnvironment)
					}
					return nil
				},
			}
			port := 4100
			environment := resolveDoctorBinaryEnvironment(context.Background(), globalconfig.PathResolution{Path: "/config/global.yaml"}, BootConfig{Host: "127.0.0.1", Port: &port}, deps)
			check := checkDoctorBinary(context.Background(), deps, environment, tt.binary, tt.binary+" binary", "--version", "install "+tt.binary)

			if check.Status != doctorOK {
				t.Fatalf("Status = %s, want %s", check.Status, doctorOK)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(check.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", check.Detail, want)
				}
			}
			if check.BinaryResolution == nil {
				t.Fatal("BinaryResolution = nil")
			}
			if check.BinaryResolution.Source != tt.wantSource ||
				check.BinaryResolution.CheckedPath != tt.wantCheckedPath ||
				check.BinaryResolution.OrchestratorPath != tt.wantOrchestratorPath ||
				check.BinaryResolution.EnvironmentsDiffer != tt.wantDiffer {
				t.Fatalf("BinaryResolution = %#v", check.BinaryResolution)
			}

			raw, err := json.Marshal(check)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			var output struct {
				BinaryResolution doctorBinaryResolution `json:"binary_resolution"`
			}
			if err := json.Unmarshal(raw, &output); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if output.BinaryResolution.Source != tt.wantSource ||
				output.BinaryResolution.CheckedPath != tt.wantCheckedPath ||
				output.BinaryResolution.EnvironmentsDiffer != tt.wantDiffer {
				t.Fatalf("JSON binary_resolution = %#v", output.BinaryResolution)
			}
		})
	}
}

func TestDoctorLiveBootUsesConfiguredPortForZero(t *testing.T) {
	t.Parallel()

	configuredPort := 4100
	zero := 0
	explicit := 4200
	tests := []struct {
		name string
		boot BootConfig
		cfg  *globalconfig.Config
		want int
	}{
		{name: "explicit doctor port", boot: BootConfig{Port: &explicit}, cfg: &globalconfig.Config{Port: &configuredPort}, want: explicit},
		{name: "zero doctor port uses configured live port", boot: BootConfig{Port: &zero}, cfg: &globalconfig.Config{Port: &configuredPort}, want: configuredPort},
		{name: "zero doctor port uses default live port", boot: BootConfig{Port: &zero}, cfg: &globalconfig.Config{}, want: defaultWebPort},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doctorLiveBoot(tt.boot, tt.cfg)
			if doctorServerPort(got) != tt.want {
				t.Fatalf("doctorServerPort() = %d, want %d", doctorServerPort(got), tt.want)
			}
		})
	}
}

func TestResolveDoctorCommandOnPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	executable := "backend"
	mode := os.FileMode(0o755)
	if runtime.GOOS == "windows" {
		executable += ".exe"
		mode = 0o644
	}
	want := filepath.Join(root, executable)
	if err := os.WriteFile(want, []byte("example"), mode); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got, err := resolveDoctorCommandOnPath(root, executable)
	if err != nil {
		t.Fatalf("resolveDoctorCommandOnPath() error = %v", err)
	}
	if got != want {
		t.Fatalf("resolveDoctorCommandOnPath() = %q, want %q", got, want)
	}
}

func TestCheckDoctorClaudeCodeUsesVersionAndHint(t *testing.T) {
	t.Parallel()

	var gotArgs []string
	got := checkDoctorClaudeCode(context.Background(), doctorDeps{
		resolveCommandOnPath: func(_ string, binary string) (string, error) {
			if binary != "claude" {
				t.Fatalf("resolveCommandOnPath(%q), want claude", binary)
			}
			return "/usr/bin/claude", nil
		},
		runCommandInDir: func(_ context.Context, _ string, environment []string, path string, args ...string) error {
			if path != "/usr/bin/claude" {
				t.Fatalf("runCommandInDir path = %q, want /usr/bin/claude", path)
			}
			if !slices.Equal(environment, []string{"PATH=/usr/bin"}) {
				t.Fatalf("environment = %#v, want selected PATH", environment)
			}
			gotArgs = append([]string{}, args...)
			return errors.New("not logged in")
		},
	}, doctorBinaryEnvironment{Source: doctorBinaryPathSourceDoctor, CheckedPath: "/usr/bin", DoctorPath: "/usr/bin"})

	if got.Status != doctorFail {
		t.Fatalf("Status = %s, want %s", got.Status, doctorFail)
	}
	if !slices.Equal(gotArgs, []string{"--version"}) {
		t.Fatalf("runCommand args = %#v, want --version", gotArgs)
	}
	if !strings.Contains(got.Hint, "Install Claude Code and run `claude` once to log in (or set ANTHROPIC_API_KEY).") {
		t.Fatalf("Hint = %q, want Claude Code install/login hint", got.Hint)
	}
}

func TestCheckDoctorCodexRequiresInitializeHandshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		versionErr    error
		initializeErr error
		wantStatus    doctorStatus
		wantDetail    string
		wantProbes    int
	}{
		{
			name:       "version failure skips initialize",
			versionErr: errors.New("version failed"),
			wantStatus: doctorFail,
			wantDetail: "version failed",
		},
		{
			name:          "initialize failure includes diagnostics",
			initializeErr: errors.New("exit status 23: stderr: broken launch environment"),
			wantStatus:    doctorFail,
			wantDetail:    "broken launch environment",
			wantProbes:    1,
		},
		{
			name:       "initialize success",
			wantStatus: doctorOK,
			wantDetail: "completed an app-server initialize handshake",
			wantProbes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			probes := 0
			check := checkDoctorCodex(context.Background(), doctorDeps{
				resolveCommandOnPath: func(_ string, binary string) (string, error) {
					if binary != "codex" {
						t.Fatalf("resolveCommandOnPath(%q), want codex", binary)
					}
					return "/usr/bin/codex", nil
				},
				runCommandInDir: func(_ context.Context, _ string, environment []string, path string, args ...string) error {
					if path != "/usr/bin/codex" || !slices.Equal(args, []string{"--version"}) {
						t.Fatalf("runCommandInDir(%q, %#v), want codex --version", path, args)
					}
					if !slices.Equal(environment, []string{"PATH=/usr/bin"}) {
						t.Fatalf("environment = %#v, want selected PATH", environment)
					}
					return tt.versionErr
				},
				codexInitialize: func(_ context.Context, path string, environment []string) error {
					probes++
					if path != "/usr/bin/codex" {
						t.Fatalf("codexInitialize(%q), want /usr/bin/codex", path)
					}
					if !slices.Equal(environment, []string{"PATH=/usr/bin"}) {
						t.Fatalf("initialize environment = %#v, want selected PATH", environment)
					}
					return tt.initializeErr
				},
			}, doctorBinaryEnvironment{Source: doctorBinaryPathSourceDoctor, CheckedPath: "/usr/bin", DoctorPath: "/usr/bin"})
			if check.Status != tt.wantStatus || !strings.Contains(check.Detail, tt.wantDetail) {
				t.Fatalf("check = %#v, want %s containing %q", check, tt.wantStatus, tt.wantDetail)
			}
			if probes != tt.wantProbes {
				t.Fatalf("initialize probes = %d, want %d", probes, tt.wantProbes)
			}
		})
	}
}

func TestRunDoctorAgentBinaryChecksFollowWorkflowBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		projects     []string
		workflows    map[string]workflowconfig.Config
		loadErrors   map[string]error
		wantChecks   []string
		wantMissing  []string
		wantCommands []string
	}{
		{
			name:     "codex only keeps existing codex check",
			projects: []string{"alpha"},
			workflows: map[string]workflowconfig.Config{
				"alpha/WORKFLOW.md": validDoctorWorkflow("/alpha"),
			},
			wantChecks:   []string{"codex binary"},
			wantMissing:  []string{"claude binary"},
			wantCommands: []string{"codex --version"},
		},
		{
			name:     "claude only checks claude",
			projects: []string{"alpha"},
			workflows: map[string]workflowconfig.Config{
				"alpha/WORKFLOW.md": validDoctorWorkflowWithBackends("/alpha", doctorClaudeCodeAgentBackend("claude-worker")),
			},
			wantChecks:   []string{"claude binary"},
			wantMissing:  []string{"codex binary"},
			wantCommands: []string{"claude --version"},
		},
		{
			name:     "mixed backends are deduplicated",
			projects: []string{"alpha", "beta"},
			workflows: map[string]workflowconfig.Config{
				"alpha/WORKFLOW.md": validDoctorWorkflowWithBackends("/alpha", doctorCodexAgentBackend("codex-worker"), doctorClaudeCodeAgentBackend("claude-worker")),
				"beta/WORKFLOW.md":  validDoctorWorkflowWithBackends("/beta", doctorClaudeCodeAgentBackend("claude-worker")),
			},
			wantChecks:   []string{"codex binary", "claude binary"},
			wantCommands: []string{"claude --version", "codex --version"},
		},
		{
			name:     "falls back to codex when workflows cannot load",
			projects: []string{"alpha"},
			loadErrors: map[string]error{
				"alpha/WORKFLOW.md": errors.New("missing workflow"),
			},
			wantChecks:   []string{"codex binary"},
			wantMissing:  []string{"claude binary"},
			wantCommands: []string{"codex --version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "global.yaml")
			global := validDoctorGlobalWithProjects(configPath, tt.projects...)
			deps := successfulDoctorDeps()
			deps.loadWorkflow = func(path string) (workflowconfig.Workflow, error) {
				if err := tt.loadErrors[path]; err != nil {
					return workflowconfig.Workflow{}, err
				}
				workflow, ok := tt.workflows[path]
				if !ok {
					return workflowconfig.Workflow{}, errors.New("unexpected workflow path: " + path)
				}
				return workflowconfig.Workflow{Config: workflow}, nil
			}

			var commandsMu sync.Mutex
			commands := []string{}
			deps.runCommandInDir = func(_ context.Context, _ string, _ []string, path string, args ...string) error {
				binary := filepath.Base(path)
				if binary == "codex" || binary == "claude" {
					commandsMu.Lock()
					commands = append(commands, binary+" "+strings.Join(args, " "))
					commandsMu.Unlock()
				}
				return nil
			}

			report := runDoctor(context.Background(), doctorConfig{
				ConfigPath:   configPath,
				Output:       io.Discard,
				CheckTimeout: time.Second,
				Flags: runtimeFlags{
					Port: runtimeIntFlag{Value: 0, Set: true},
				},
			}, successfulDoctorOptionsWithConfig(configPath, global), deps)

			for _, want := range tt.wantChecks {
				assertDoctorCheck(t, report, want, doctorOK, "is runnable")
			}
			for _, want := range tt.wantMissing {
				assertDoctorMissingCheck(t, report, want)
			}
			commandsMu.Lock()
			gotCommands := append([]string{}, commands...)
			commandsMu.Unlock()
			slices.Sort(gotCommands)
			wantCommands := append([]string{}, tt.wantCommands...)
			slices.Sort(wantCommands)
			if !slices.Equal(gotCommands, wantCommands) {
				t.Fatalf("agent commands = %#v, want %#v", gotCommands, wantCommands)
			}
		})
	}
}

func TestRunDoctorFailsRejectedPinnedRouteModelWithoutChangingModelChoice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	if err := os.WriteFile(workflowPath, []byte(`---
tracker:
  kind: memory
workspace:
  source_root: `+dir+`
agents:
  backends:
    - id: codex-main
      kind: codex
      command: codex app-server
  routes:
    - name: default
      backend: codex-main
      default: true
      model: gpt-5-codex
---
Prompt
`), 0o600); err != nil {
		t.Fatalf("WriteFile(WORKFLOW.md) error = %v", err)
	}
	configPath := filepath.Join(dir, "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{{
			ID:       "pyroapex",
			Workflow: workflowPath,
			Workdir:  dir,
			Weight:   1,
		}},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = workflowconfig.LoadWorkflow
	deps.modelProbe = func(_ context.Context, req doctorRouteModelProbeRequest) error {
		if req.ProjectID != "pyroapex" || req.RouteName != "default" || req.Model != "gpt-5-codex" {
			t.Fatalf("probe request = %#v, want pyroapex default gpt-5-codex", req)
		}
		return errors.New(`{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"model rejected"}}`)
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:       configPath,
		Output:           io.Discard,
		CheckTimeout:     time.Second,
		WorkflowDiff:     true,
		AllowWriteProbes: false,
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), deps)

	assertDoctorCheck(t, report, "Project pyroapex pinned route models", doctorFail, "gpt-5-codex")
	check := doctorCheckByName(t, report, "Project pyroapex pinned route models")
	for _, want := range []string{"pyroapex", "default", "gpt-5-codex", "model rejected"} {
		if !strings.Contains(check.Detail, want) {
			t.Fatalf("route model detail missing %q:\n%s", want, check.Detail)
		}
	}
	if strings.Contains(report.WorkflowOptimization.Diff, "model:") {
		t.Fatalf("diff changed the project's model choice:\n%s", report.WorkflowOptimization.Diff)
	}
	proposal := doctorWorkflowProposalBySignal(t, report.WorkflowOptimization.Proposals, "doctor_finding", doctorWorkflowRulePinnedRouteModelRejected)
	if !strings.Contains(proposal.SuggestedChange, "backend-supported pin") || !strings.Contains(proposal.SuggestedChange, "remove the pin") {
		t.Fatalf("proposal is not model-choice neutral: %#v", proposal)
	}

	written, err := writeDoctorWorkflowOptimizationPatches(report.WorkflowOptimization)
	if err != nil {
		t.Fatalf("writeDoctorWorkflowOptimizationPatches() error = %v", err)
	}
	if len(written) != 0 {
		t.Fatalf("written = %#v, want no automatic model-choice change", written)
	}
	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	if got := workflow.Config.AgentRouteConfigs()[0].Model; got != "gpt-5-codex" {
		t.Fatalf("route model after write = %q, want preserved pin", got)
	}
}

func TestCheckDoctorRouteModelsFailsRejectedBackendCommandPin(t *testing.T) {
	t.Parallel()

	workflow, err := workflowconfig.ParseWorkflow([]byte(`---
tracker:
  kind: memory
agents:
  backends:
    - id: codex-main
      kind: codex
      command: codex --config 'model="gpt-5.5"' app-server
  routes:
    - name: default
      backend: codex-main
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	check := checkDoctorRouteModels(context.Background(), "detent", globalconfig.Project{ID: "detent", Workflow: workflowPath, Workdir: dir}, workflow.Config, doctorDeps{
		modelProbe: func(_ context.Context, req doctorRouteModelProbeRequest) error {
			if req.Model != "gpt-5.5" || req.RouteName != "default" || req.Backend.ID != "codex-main" {
				t.Fatalf("probe request = %#v, want backend command pin", req)
			}
			return errors.New("model retired")
		},
	})
	if check.Status != doctorFail || !strings.Contains(check.Detail, "agents.backends.command") || !strings.Contains(check.Detail, "model retired") {
		t.Fatalf("check = %#v, want rejected backend command pin", check)
	}
	finding := doctorWorkflowFindingByRule(t, check.WorkflowOptimization.Findings, doctorWorkflowRulePinnedRouteModelRejected)
	if got := finding.Evidence["configured_model_source"]; got != "agents.backends.command" {
		t.Fatalf("configured_model_source = %#v, want agents.backends.command", got)
	}
	proposal := doctorWorkflowProposalBySignal(t, check.WorkflowOptimization.Proposals, "doctor_finding", doctorWorkflowRulePinnedRouteModelRejected)
	if proposal.TargetPath != "agents.backends.command" {
		t.Fatalf("proposal target = %q, want agents.backends.command", proposal.TargetPath)
	}
}

func TestCheckDoctorRouteModelsProbesRoleBackendCommandPins(t *testing.T) {
	t.Parallel()

	workflow, err := workflowconfig.ParseWorkflow([]byte(`---
tracker:
  kind: memory
agents:
  backends:
    - id: codex-code
      kind: codex
      command: codex app-server
    - id: codex-plan
      kind: codex
      command: codex --config 'model="gpt-retired-plan"' app-server
  routes:
    - name: code-default
      backend: codex-code
      default: true
    - name: plan-default
      role: plan
      backend: codex-plan
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	dir := t.TempDir()
	check := checkDoctorRouteModels(context.Background(), "detent", globalconfig.Project{ID: "detent", Workflow: filepath.Join(dir, "WORKFLOW.md"), Workdir: dir}, workflow.Config, doctorDeps{
		modelProbe: func(_ context.Context, req doctorRouteModelProbeRequest) error {
			if req.Model != "gpt-retired-plan" || req.RouteName != "plan-default" || req.RouteRole != "plan" || req.Backend.ID != "codex-plan" {
				t.Fatalf("probe request = %#v, want plan backend command pin", req)
			}
			return errors.New("model retired")
		},
	})
	if check.Status != doctorFail || !strings.Contains(check.Detail, "plan-default") || !strings.Contains(check.Detail, "gpt-retired-plan") {
		t.Fatalf("check = %#v, want rejected role backend command pin", check)
	}
}

func TestCheckDoctorIssueAgentModels(t *testing.T) {
	t.Parallel()

	models := []runnerpkg.AgentModel{
		{ID: "gpt-default", Model: "gpt-default", SupportedReasoningEfforts: []string{"low", "high"}},
		{ID: "gpt-5.5", Model: "gpt-5.5", SupportedReasoningEfforts: []string{"low", "medium", "high"}},
		{ID: "gpt-retired", Model: "gpt-retired", Upgrade: "gpt-5.5", SupportedReasoningEfforts: []string{"medium"}},
	}
	tests := []struct {
		name         string
		issue        connector.Issue
		defaultModel string
		configure    func(*workflowconfig.Config)
		wantStatus   doctorStatus
		wantDetail   []string
		wantHint     string
		wantModel    string
		wantEffort   string
		wantBackend  string
		wantProbes   int
	}{
		{
			name:       "no block",
			issue:      connector.Issue{ID: "issue-1", Identifier: "digitaldrywood/detent#1", Description: "Ship it."},
			wantStatus: doctorOK,
			wantDetail: []string{"validated 0 detent-agent model override(s)", "0 effort override(s)"},
		},
		{
			name:       "healthy model",
			issue:      connector.Issue{ID: "issue-2", Identifier: "digitaldrywood/detent#2", Description: "```detent-agent\nschema: 1\nmodel: gpt-5.5\n```"},
			wantStatus: doctorOK,
			wantDetail: []string{"validated 1 detent-agent model override(s)", "0 effort override(s)"},
			wantModel:  "gpt-5.5",
			wantProbes: 1,
		},
		{
			name:         "valid effort uses project default model",
			issue:        connector.Issue{ID: "issue-3", Identifier: "digitaldrywood/detent#3", Description: "```detent-agent\nschema: 1\neffort: high\n```"},
			defaultModel: "gpt-default",
			wantStatus:   doctorOK,
			wantDetail:   []string{"validated 0 detent-agent model override(s)", "1 effort override(s)"},
			wantModel:    "gpt-default",
			wantEffort:   "high",
			wantProbes:   1,
		},
		{
			name:         "issue model controls effort validation",
			issue:        connector.Issue{ID: "issue-4", Identifier: "digitaldrywood/detent#4", Description: "```detent-agent\nschema: 1\nmodel: gpt-5.5\neffort: medium\n```"},
			defaultModel: "gpt-default",
			wantStatus:   doctorOK,
			wantDetail:   []string{"validated 1 detent-agent model override(s)", "1 effort override(s)"},
			wantModel:    "gpt-5.5",
			wantEffort:   "medium",
			wantProbes:   1,
		},
		{
			name: "effort uses issue routed model and backend",
			issue: connector.Issue{
				ID:          "issue-routed",
				Identifier:  "digitaldrywood/detent#8",
				Description: "```detent-agent\nschema: 1\neffort: medium\n```",
				Labels:      []string{"tier:routed"},
			},
			configure: func(cfg *workflowconfig.Config) {
				cfg.Agents.Backends = []workflowconfig.AgentBackend{
					doctorCodexAgentBackend("codex-default"),
					doctorCodexAgentBackend("codex-routed"),
				}
				cfg.Agents.Routes = []workflowconfig.AgentRoute{
					{
						Name:    "routed",
						Backend: "codex-routed",
						Model:   "gpt-5.5",
						Selector: selector.Selector{
							Labels: selector.Labels{Include: []string{"tier:routed"}},
						},
					},
					{Name: "default", Backend: "codex-default", Model: "gpt-default", Default: true},
				}
			},
			wantStatus:  doctorOK,
			wantDetail:  []string{"validated 0 detent-agent model override(s)", "1 effort override(s)"},
			wantModel:   "gpt-5.5",
			wantEffort:  "medium",
			wantBackend: "codex-routed",
			wantProbes:  1,
		},
		{
			name:         "unsupported effort names issue and supported efforts",
			issue:        connector.Issue{ID: "issue-5", Identifier: "digitaldrywood/detent#5", Description: "```detent-agent\nschema: 1\neffort: bogus\n```"},
			defaultModel: "gpt-default",
			wantStatus:   doctorFail,
			wantDetail:   []string{"digitaldrywood/detent#5 detent-agent effort bogus", `effort "bogus" is not supported by model "gpt-default"`, "supported efforts: low, high"},
			wantHint:     "remove the effort key",
			wantModel:    "gpt-default",
			wantEffort:   "bogus",
			wantProbes:   1,
		},
		{
			name:         "unsupported merge effort names role field",
			issue:        connector.Issue{ID: "issue-merge", Identifier: "digitaldrywood/detent#9", Description: "```detent-agent\nschema: 1\nmerge:\n  effort: bogus\n```"},
			defaultModel: "gpt-default",
			wantStatus:   doctorFail,
			wantDetail:   []string{"digitaldrywood/detent#9 detent-agent merge.effort bogus", `effort "bogus" is not supported by model "gpt-default"`, "supported efforts: low, high"},
			wantHint:     "remove the effort key",
			wantModel:    "gpt-default",
			wantEffort:   "bogus",
			wantProbes:   1,
		},
		{
			name:       "retired model",
			issue:      connector.Issue{ID: "issue-6", Identifier: "digitaldrywood/detent#6", Description: "```detent-agent\nschema: 1\nmodel: gpt-retired\n```"},
			wantStatus: doctorFail,
			wantDetail: []string{"digitaldrywood/detent#6 detent-agent model gpt-retired", `retired; use "gpt-5.5"`},
			wantModel:  "gpt-retired",
			wantProbes: 1,
		},
		{
			name: "comment block ignored",
			issue: connector.Issue{ID: "issue-7", Identifier: "digitaldrywood/detent#7", Comments: []connector.IssueComment{{
				Body: "```detent-agent\nschema: 1\nmodel: gpt-retired\n```",
			}}},
			wantStatus: doctorOK,
			wantDetail: []string{"validated 0 detent-agent model override(s)", "0 effort override(s)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validDoctorWorkflow(t.TempDir())
			if tt.configure != nil {
				tt.configure(&cfg)
			}
			if tt.defaultModel != "" {
				cfg.Agents.Routes = []workflowconfig.AgentRoute{{
					Name:    "default",
					Backend: workflowconfig.DefaultAgentBackendID,
					Model:   tt.defaultModel,
					Default: true,
				}}
			}
			probes := 0
			check := checkDoctorIssueAgentModels(context.Background(), "detent", globalconfig.Project{ID: "detent", Workdir: t.TempDir()}, cfg, doctorDeps{
				autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
					return &fakeDoctorAutoPromoteConnector{issues: []connector.Issue{tt.issue}}, nil
				},
				modelProbe: func(_ context.Context, req doctorRouteModelProbeRequest) error {
					probes++
					wantBackend := tt.wantBackend
					if wantBackend == "" {
						wantBackend = workflowconfig.DefaultAgentBackendID
					}
					if req.Model != tt.wantModel || req.Effort != tt.wantEffort {
						t.Fatalf("probe model/effort = %q/%q, want %q/%q", req.Model, req.Effort, tt.wantModel, tt.wantEffort)
					}
					if req.Backend.ID != wantBackend {
						t.Fatalf("probe backend = %q, want %q", req.Backend.ID, wantBackend)
					}
					if err := validateDoctorModelCatalog(models, req.Model); err != nil {
						return err
					}
					if req.Effort == "" {
						return nil
					}
					return validateDoctorEffortCatalog(models, req.Model, req.Effort)
				},
			})
			if check.Status != tt.wantStatus {
				t.Fatalf("check status = %s, want %s: %#v", check.Status, tt.wantStatus, check)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(check.Detail, want) {
					t.Fatalf("check detail = %q, want containing %q", check.Detail, want)
				}
			}
			if tt.wantHint != "" && !strings.Contains(check.Hint, tt.wantHint) {
				t.Fatalf("check hint = %q, want containing %q", check.Hint, tt.wantHint)
			}
			if probes != tt.wantProbes {
				t.Fatalf("probes = %d, want %d", probes, tt.wantProbes)
			}
		})
	}
}

func TestCheckDoctorIssueAgentModelsProbesInheritedReworkEffort(t *testing.T) {
	t.Parallel()

	cfg := validDoctorWorkflow(t.TempDir())
	cfg.Agents.Backends = []workflowconfig.AgentBackend{
		doctorCodexAgentBackend("codex-code"),
		doctorCodexAgentBackend("codex-rework"),
	}
	cfg.Agents.Routes = []workflowconfig.AgentRoute{
		{Name: "rework", Role: runnerpkg.RoleRework, Backend: "codex-rework", Model: "gpt-rework", Default: true},
		{Name: "default", Backend: "codex-code", Model: "gpt-code", Default: true},
	}
	issue := connector.Issue{
		ID:          "issue-inherited-rework",
		Identifier:  "digitaldrywood/detent#10",
		Description: "```detent-agent\nschema: 1\ncode:\n  effort: high\n```",
	}
	probedRoles := []string{}
	check := checkDoctorIssueAgentModels(context.Background(), "detent", globalconfig.Project{ID: "detent", Workdir: t.TempDir()}, cfg, doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return &fakeDoctorAutoPromoteConnector{issues: []connector.Issue{issue}}, nil
		},
		modelProbe: func(_ context.Context, req doctorRouteModelProbeRequest) error {
			probedRoles = append(probedRoles, req.RouteName)
			if req.Effort != "high" {
				t.Fatalf("probe effort = %q, want high", req.Effort)
			}
			if strings.HasSuffix(req.RouteName, ":rework") {
				if req.Model != "gpt-rework" || req.Backend.ID != "codex-rework" {
					t.Fatalf("rework probe = %#v, want routed rework model/backend", req)
				}
				return errors.New("effort high is not supported by rework model")
			}
			if req.Model != "gpt-code" || req.Backend.ID != "codex-code" {
				t.Fatalf("code probe = %#v, want routed code model/backend", req)
			}
			return nil
		},
	})
	if check.Status != doctorFail || !strings.Contains(check.Detail, "code.effort high") || !strings.Contains(check.Detail, "not supported by rework model") {
		t.Fatalf("check = %#v, want inherited rework rejection", check)
	}
	if len(probedRoles) != 2 || !strings.HasSuffix(probedRoles[0], ":code") || !strings.HasSuffix(probedRoles[1], ":rework") {
		t.Fatalf("probed roles = %#v, want code then rework", probedRoles)
	}
}

func TestValidateDoctorEffortCatalog(t *testing.T) {
	t.Parallel()

	models := []runnerpkg.AgentModel{
		{ID: "gpt-default", Model: "gpt-default", SupportedReasoningEfforts: []string{"low", "high"}},
		{ID: "gpt-5.5", Model: "gpt-5.5", SupportedReasoningEfforts: []string{"low", "medium", "high"}},
		{ID: "gpt-retired", Model: "gpt-retired", Upgrade: "gpt-5.5", SupportedReasoningEfforts: []string{"medium"}},
	}
	tests := []struct {
		name    string
		model   string
		effort  string
		wantErr []string
	}{
		{name: "supported effort", model: "gpt-default", effort: "HIGH"},
		{name: "model-specific effort", model: "gpt-5.5", effort: "medium"},
		{name: "unsupported effort", model: "gpt-default", effort: "medium", wantErr: []string{`effort "medium"`, `model "gpt-default"`, "supported efforts: low, high"}},
		{name: "unknown model", model: "gpt-unknown", effort: "high", wantErr: []string{"not available"}},
		{name: "retired model", model: "gpt-retired", effort: "medium", wantErr: []string{`retired; use "gpt-5.5"`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateDoctorEffortCatalog(models, tt.model, tt.effort)
			if len(tt.wantErr) == 0 {
				if err != nil {
					t.Fatalf("validateDoctorEffortCatalog() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validateDoctorEffortCatalog() error = nil")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("validateDoctorEffortCatalog() error = %v, want containing %q", err, want)
				}
			}
		})
	}
}

func TestValidateDoctorModelCatalog(t *testing.T) {
	t.Parallel()

	models := []runnerpkg.AgentModel{
		{ID: "gpt-5.5", Model: "gpt-5.5"},
		{ID: "gpt-retired", Model: "gpt-retired", Upgrade: "gpt-5.5"},
	}
	tests := []struct {
		name    string
		model   string
		wantErr string
	}{
		{name: "available model", model: "gpt-5.5"},
		{name: "retired model", model: "gpt-retired", wantErr: `retired; use "gpt-5.5"`},
		{name: "unknown model", model: "gpt-unknown", wantErr: "not available"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateDoctorModelCatalog(models, tt.model)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateDoctorModelCatalog() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateDoctorModelCatalog() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestCheckDoctorProjects(t *testing.T) {
	t.Parallel()

	parsedDisabledBudget, err := workflowconfig.ParseWorkflow([]byte("---\nbudget:\n  per_day_max_usd: 50\n  per_issue_max_usd: 5\n---\n"))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	disabledBudgetWorkflow := validDoctorWorkflow("/repo")
	disabledBudgetWorkflow.Budget = parsedDisabledBudget.Config.Budget
	omittedBudgetWorkflow := validDoctorWorkflow("/repo")
	omittedBudgetWorkflow.Budget = workflowconfig.Default().Budget
	disabledProgressWorkflow := validDoctorWorkflow("/repo")
	disabledProgressWorkflow.Budget.BillingMode = workflowconfig.BillingModeSubscription
	disabledProgressWorkflow.Agent.NoProgressTokenLimit = 0
	disabledProgressWorkflow.Agent.NoProgressSpendLimitUSD = 0

	tests := []struct {
		name       string
		projects   []globalconfig.Project
		workflow   workflowconfig.Workflow
		loadErr    error
		gitErr     error
		wantStatus []doctorStatus
		wantDetail []string
	}{
		{
			name:       "no projects configured",
			wantStatus: []doctorStatus{doctorWarn},
			wantDetail: []string{"no projects configured"},
		},
		{
			name: "workflow cannot load",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			loadErr:    errors.New("missing workflow"),
			wantStatus: []doctorStatus{doctorFail, doctorWarn, doctorOK, doctorOK},
			wantDetail: []string{"missing workflow", "skipped", "skipped because WORKFLOW.md could not be loaded", "skipped because WORKFLOW.md could not be loaded"},
		},
		{
			name: "workflow invalid",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: workflowconfig.Config{}},
			wantStatus: []doctorStatus{doctorFail, doctorWarn, doctorOK, doctorOK},
			wantDetail: []string{"tracker.kind", "skipped", "skipped because WORKFLOW.md is invalid", "skipped because WORKFLOW.md is invalid"},
		},
		{
			name: "budget caps configured while disabled",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: disabledBudgetWorkflow},
			wantStatus: []doctorStatus{doctorOK, doctorOK, doctorWarn, doctorOK, doctorOK, doctorWarn, doctorOK, doctorOK, doctorOK, doctorWarn, doctorOK},
			wantDetail: []string{"is valid", "advisory:", "subscription billing is the default", "effective cross-session progress brake", "recovery.terminal_attempt_retry_limit=3", "budget.enabled=false disables configured caps", "enabled=true provides prompt guidance", "validated 0 pinned Codex route model(s)", "is a git worktree", "contain no detent-agent guidance", "loaded=0; dropped=0"},
		},
		{
			name: "inherited spend breaker warns about billing ambiguity",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: omittedBudgetWorkflow},
			wantStatus: []doctorStatus{doctorOK, doctorOK, doctorWarn, doctorOK, doctorOK, doctorOK, doctorOK, doctorOK, doctorWarn, doctorOK},
			wantDetail: []string{"is valid", "advisory:", "subscription billing is the default", "effective cross-session progress brake", "recovery.terminal_attempt_retry_limit=3", "enabled=true provides prompt guidance", "validated 0 pinned Codex route model(s)", "is a git worktree", "contain no detent-agent guidance", "loaded=0; dropped=0"},
		},
		{
			name: "source repo missing",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: validDoctorWorkflow("/repo")},
			gitErr:     errors.New("not a git worktree"),
			wantStatus: []doctorStatus{doctorOK, doctorOK, doctorWarn, doctorOK, doctorOK, doctorOK, doctorOK, doctorFail, doctorOK, doctorWarn},
			wantDetail: []string{"is valid", "advisory:", "configuration footgun:", "effective cross-session progress brake", "recovery.terminal_attempt_retry_limit=3", "enabled=true provides prompt guidance", "validated 0 pinned Codex route model(s)", "not a git worktree", "skipped because source repository is unavailable locally", "skipped because source repository is unavailable locally"},
		},
		{
			name: "all progress brakes disabled",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: disabledProgressWorkflow},
			wantStatus: []doctorStatus{doctorOK, doctorOK, doctorWarn, doctorWarn, doctorOK, doctorOK, doctorOK, doctorOK, doctorWarn, doctorOK},
			wantDetail: []string{"is valid", "advisory:", "billing_mode=subscription", "no effective cross-session progress brake", "recovery.terminal_attempt_retry_limit=3", "enabled=true provides prompt guidance", "validated 0 pinned Codex route model(s)", "is a git worktree", "contain no detent-agent guidance", "loaded=0; dropped=0"},
		},
		{
			name: "workflow and source repo valid",
			projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			},
			workflow:   workflowconfig.Workflow{Config: validDoctorWorkflow("/repo")},
			wantStatus: []doctorStatus{doctorOK, doctorOK, doctorWarn, doctorOK, doctorOK, doctorOK, doctorOK, doctorOK, doctorWarn, doctorOK},
			wantDetail: []string{"is valid", "advisory:", "configuration footgun:", "effective cross-session progress brake", "recovery.terminal_attempt_retry_limit=3", "enabled=true provides prompt guidance", "validated 0 pinned Codex route model(s)", "is a git worktree", "contain no detent-agent guidance", "loaded=0; dropped=0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := checkDoctorProjects(context.Background(), globalconfig.Config{Projects: tt.projects}, doctorDeps{
				loadWorkflow: func(string) (workflowconfig.Workflow, error) {
					return tt.workflow, tt.loadErr
				},
				gitWorkTree: func(context.Context, string) error {
					return tt.gitErr
				},
			}, RuntimeSecret{}, false)
			if len(got) != len(tt.wantStatus) {
				t.Fatalf("len(checks) = %d, want %d: %#v", len(got), len(tt.wantStatus), got)
			}
			for i, check := range got {
				if check.Status != tt.wantStatus[i] {
					t.Fatalf("checks[%d].Status = %s, want %s", i, check.Status, tt.wantStatus[i])
				}
				if !strings.Contains(check.Detail, tt.wantDetail[i]) {
					t.Fatalf("checks[%d].Detail = %q, want containing %q", i, check.Detail, tt.wantDetail[i])
				}
			}
		})
	}
}

func TestCheckDoctorDisabledBudgetCaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		budgetYAML string
		wantOK     bool
		wantDetail string
	}{
		{
			name:       "enabled budget does not warn",
			budgetYAML: "  enabled: true\n  per_day_max_usd: 50\n  per_issue_max_usd: 5\n",
		},
		{name: "disabled budget without caps does not warn", budgetYAML: "  enabled: false\n"},
		{
			name:       "disabled daily cap warns",
			budgetYAML: "  per_day_max_usd: 50\n",
			wantOK:     true,
			wantDetail: "budget.enabled=false disables configured caps: budget.per_day_max_usd=50",
		},
		{
			name:       "disabled issue and daily caps warn",
			budgetYAML: "  per_day_max_usd: 646.07\n  per_issue_max_usd: 5\n",
			wantOK:     true,
			wantDetail: "budget.enabled=false disables configured caps: budget.per_day_max_usd=646.07, budget.per_issue_max_usd=5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := workflowconfig.ParseWorkflow([]byte("---\nbudget:\n" + tt.budgetYAML + "---\n"))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			got, ok := checkDoctorDisabledBudgetCaps("pyroapex", workflow.Config.Budget)
			if ok != tt.wantOK {
				t.Fatalf("checkDoctorDisabledBudgetCaps() ok = %t, want %t: %#v", ok, tt.wantOK, got)
			}
			if !tt.wantOK {
				return
			}
			if got.Name != "Project pyroapex budget" || got.Status != doctorWarn || got.Detail != tt.wantDetail {
				t.Fatalf("checkDoctorDisabledBudgetCaps() = %#v", got)
			}
			if got.Hint != "Add this exact line under budget: in WORKFLOW.md:\n  enabled: true" {
				t.Fatalf("Hint = %q, want copy-paste enabled line", got.Hint)
			}
		})
	}
}

func TestCheckDoctorBillingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		budget     workflowconfig.Budget
		spendLimit float64
		wantOK     bool
		wantDetail string
	}{
		{
			name:       "undeclared enabled budget warns about default subscription mode",
			budget:     workflowconfig.Budget{Enabled: true},
			wantOK:     true,
			wantDetail: "subscription billing is the default",
		},
		{
			name:       "undeclared spend breaker warns that USD is inert",
			spendLimit: 3,
			wantOK:     true,
			wantDetail: "subscription billing is the default",
		},
		{
			name: "undeclared mode without USD controls does not warn",
		},
		{
			name:       "declared metered mode warns for subscription auth",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeMetered, Enabled: true},
			spendLimit: 3,
			wantOK:     true,
			wantDetail: "notional USD brakes",
		},
		{
			name:       "subscription budget cap warns that enforcement is advisory",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeSubscription, Enabled: true},
			wantOK:     true,
			wantDetail: "budget.enabled=true",
		},
		{
			name:       "subscription spend breaker warns that enforcement is advisory",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeSubscription},
			spendLimit: 3,
			wantOK:     true,
			wantDetail: "agent.no_progress_spend_limit_usd=3",
		},
		{
			name:   "subscription without USD controls does not warn",
			budget: workflowconfig.Budget{BillingMode: workflowconfig.BillingModeSubscription},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := checkDoctorBillingMode("detent", workflowconfig.Config{
				Budget: tt.budget,
				Agent:  workflowconfig.Agent{NoProgressSpendLimitUSD: tt.spendLimit},
			}, true)
			if ok != tt.wantOK {
				t.Fatalf("checkDoctorBillingMode() ok = %t, want %t: %#v", ok, tt.wantOK, got)
			}
			if tt.wantOK && (got.Status != doctorWarn || !strings.Contains(got.Detail, tt.wantDetail)) {
				t.Fatalf("checkDoctorBillingMode() = %#v, want warning containing %q", got, tt.wantDetail)
			}
			if tt.name == "declared metered mode warns for subscription auth" {
				for _, want := range []string{"budget.per_day_max_usd", "budget.per_issue_max_usd", "agent.no_progress_spend_limit_usd", "budget.enabled=false does not disarm", "billing_mode: subscription"} {
					if !strings.Contains(got.Detail+" "+got.Hint, want) {
						t.Fatalf("checkDoctorBillingMode() = %#v, want %q", got, want)
					}
				}
			}
		})
	}
	cfg := workflowconfig.Config{
		Budget: workflowconfig.Budget{BillingMode: workflowconfig.BillingModeMetered},
		Worker: workflowconfig.Worker{GitHubToken: "gh"},
	}
	if got, ok := checkDoctorBillingMode("detent", cfg, false); ok {
		t.Fatalf("checkDoctorBillingMode() = %#v, want worker credential warning excluded", got)
	}
}

func TestCheckDoctorWorkerGitHubCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		token    string
		wantOK   bool
		wantText string
	}{
		{name: "unset does not warn"},
		{name: "ambient gh warns", token: "gh", wantOK: true, wantText: "shared-budget mode"},
		{name: "configured token warns about same principal", token: "$DETENT_WORKER_GITHUB_TOKEN", wantOK: true, wantText: "true rate-limit isolation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := workflowconfig.Config{Worker: workflowconfig.Worker{GitHubToken: tt.token}}
			got, ok := checkDoctorWorkerGitHubCredential("detent", cfg)
			if ok != tt.wantOK {
				t.Fatalf("checkDoctorWorkerGitHubCredential() ok = %t, want %t: %#v", ok, tt.wantOK, got)
			}
			if tt.wantOK && (got.Status != doctorWarn || !strings.Contains(got.Detail+" "+got.Hint, tt.wantText)) {
				t.Fatalf("checkDoctorWorkerGitHubCredential() = %#v, want warning containing %q", got, tt.wantText)
			}
		})
	}
}

func TestCheckDoctorMeteredBillingAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		account    codex.Account
		wantStatus doctorStatus
		wantDetail []string
	}{
		{
			name:       "flat subscription warns with every armed brake",
			account:    codex.Account{Type: "chatgpt", PlanType: "plus"},
			wantStatus: doctorWarn,
			wantDetail: []string{"subscription auth", "backend codex-wrapper", `command "codex-wrapper app-server"`, "budget.per_day_max_usd", "budget.per_issue_max_usd", "agent.no_progress_spend_limit_usd", "budget.enabled=false does not disarm", "billing_mode: subscription"},
		},
		{
			name:       "API key preserves metered enforcement",
			account:    codex.Account{Type: "apiKey"},
			wantStatus: doctorOK,
			wantDetail: []string{"apiKey", "does not use a flat subscription"},
		},
		{
			name:       "usage based ChatGPT plan preserves metered enforcement",
			account:    codex.Account{Type: "chatgpt", PlanType: "enterprise_cbp_usage_based"},
			wantStatus: doctorOK,
			wantDetail: []string{"enterprise_cbp_usage_based", "does not use a flat subscription"},
		},
		{
			name:       "unknown auth warns",
			account:    codex.Account{},
			wantStatus: doctorWarn,
			wantDetail: []string{"did not identify an account type"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow := validDoctorWorkflow("/repo")
			workflow.Agents.Backends = []workflowconfig.AgentBackend{{
				ID:       "codex-wrapper",
				Kind:     workflowconfig.AgentBackendCodex,
				Protocol: "app-server",
				Command:  "codex-wrapper app-server",
			}}
			var gotBackend workflowconfig.AgentBackend
			checks := checkDoctorMeteredBillingAuth(context.Background(), globalconfig.Config{
				Projects: []globalconfig.Project{{ID: "detent", Workflow: "WORKFLOW.md"}},
			}, doctorDeps{
				loadWorkflow: func(string) (workflowconfig.Workflow, error) {
					return workflowconfig.Workflow{Config: workflow}, nil
				},
				codexAccount: func(_ context.Context, backend workflowconfig.AgentBackend, _ []string) (codex.Account, error) {
					gotBackend = backend
					return tt.account, nil
				},
			}, doctorBinaryEnvironment{CheckedPath: "/usr/local/bin"})
			if len(checks) != 1 || checks[0].Status != tt.wantStatus {
				t.Fatalf("checkDoctorMeteredBillingAuth() = %#v, want one %s check", checks, tt.wantStatus)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(checks[0].Detail+" "+checks[0].Hint, want) {
					t.Fatalf("checkDoctorMeteredBillingAuth() = %#v, want %q", checks, want)
				}
			}
			if gotBackend.ID != "codex-wrapper" || gotBackend.Command != "codex-wrapper app-server" {
				t.Fatalf("probed backend = %#v, want configured Codex wrapper", gotBackend)
			}
		})
	}
}

func TestCheckDoctorMeteredBillingAuthSkipsUnroutedCodexBackend(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Agents.Backends = []workflowconfig.AgentBackend{
		{ID: "unused-codex", Kind: workflowconfig.AgentBackendCodex, Protocol: "app-server", Command: "codex app-server"},
		{ID: "claude", Kind: workflowconfig.AgentBackendClaudeCode, Protocol: "headless", Command: "claude"},
	}
	workflow.Agents.Routes = []workflowconfig.AgentRoute{{Name: "default", Backend: "claude", Default: true}}
	called := false
	checks := checkDoctorMeteredBillingAuth(context.Background(), globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "detent", Workflow: "WORKFLOW.md"}},
	}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		codexAccount: func(context.Context, workflowconfig.AgentBackend, []string) (codex.Account, error) {
			called = true
			return codex.Account{}, nil
		},
	}, doctorBinaryEnvironment{})
	if called || len(checks) != 0 {
		t.Fatalf("checkDoctorMeteredBillingAuth() = %#v, probe called=%t; want unrouted Codex backend skipped", checks, called)
	}
}

func TestCheckDoctorProgressBrake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		budget     workflowconfig.Budget
		tokenLimit int64
		spendLimit float64
		wantStatus doctorStatus
		wantDetail string
	}{
		{
			name:       "subscription token brake remains effective",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeSubscription},
			tokenLimit: 25_000_000,
			spendLimit: 3,
			wantStatus: doctorOK,
			wantDetail: "tokens=25000000; USD=3 is inert",
		},
		{
			name:       "metered reports both effective brakes",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeMetered},
			tokenLimit: 25_000_000,
			spendLimit: 3,
			wantStatus: doctorOK,
			wantDetail: "tokens=25000000, USD=3",
		},
		{
			name:       "metered USD brake is effective by itself",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeMetered},
			spendLimit: 3,
			wantStatus: doctorOK,
			wantDetail: "USD=3",
		},
		{
			name:       "all brakes disabled warns",
			budget:     workflowconfig.Budget{BillingMode: workflowconfig.BillingModeSubscription},
			wantStatus: doctorWarn,
			wantDetail: "no effective cross-session progress brake",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := checkDoctorProgressBrake("detent", workflowconfig.Config{
				Budget: tt.budget,
				Agent: workflowconfig.Agent{
					NoProgressTokenLimit:    tt.tokenLimit,
					NoProgressSpendLimitUSD: tt.spendLimit,
				},
			})
			if got.Status != tt.wantStatus || !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("checkDoctorProgressBrake() = %#v, want %s containing %q", got, tt.wantStatus, tt.wantDetail)
			}
		})
	}
}

func TestCheckDoctorIssueEffortGuidance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		files       map[string]string
		directories []string
		missingRepo bool
		wantStatus  doctorStatus
		wantDetail  string
		wantHint    string
	}{
		{
			name:       "agents guidance passes",
			files:      map[string]string{"AGENTS.md": "Use a detent-agent block for every issue."},
			wantStatus: doctorOK,
			wantDetail: "AGENTS.md mentions detent-agent effort guidance",
		},
		{
			name:       "claude guidance passes case insensitively",
			files:      map[string]string{"AGENTS.md": "No overrides documented.", "CLAUDE.md": "Choose effort with a DETENT-AGENT block."},
			wantStatus: doctorOK,
			wantDetail: "CLAUDE.md mentions detent-agent effort guidance",
		},
		{
			name:       "docs without guidance warn",
			files:      map[string]string{"AGENTS.md": "General agent instructions.", "CLAUDE.md": "General Claude instructions."},
			wantStatus: doctorWarn,
			wantDetail: "contain no detent-agent guidance",
			wantHint:   "docs/ONBOARDING.md#per-issue-agent-overrides",
		},
		{
			name:       "missing docs warn",
			wantStatus: doctorWarn,
			wantDetail: "contain no detent-agent guidance",
			wantHint:   "effort-selection rubric",
		},
		{
			name:        "unreadable agent doc skips",
			directories: []string{"AGENTS.md"},
			wantStatus:  doctorOK,
			wantDetail:  "skipped because AGENTS.md could not be read",
		},
		{
			name:        "missing source repo skips",
			missingRepo: true,
			wantStatus:  doctorOK,
			wantDetail:  "skipped because source repository is unavailable locally",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			for name, content := range tt.files {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
					t.Fatalf("WriteFile(%s) error = %v", name, err)
				}
			}
			for _, name := range tt.directories {
				if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
					t.Fatalf("Mkdir(%s) error = %v", name, err)
				}
			}

			var got doctorCheck
			if tt.missingRepo {
				got = checkDoctorIssueEffortGuidanceForSource("alpha", globalconfig.Project{Workdir: filepath.Join(root, "missing")}, workflowconfig.Default())
			} else {
				got = checkDoctorIssueEffortGuidance("alpha", root)
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
			if tt.wantHint != "" && !strings.Contains(got.Hint, tt.wantHint) {
				t.Fatalf("Hint = %q, want containing %q", got.Hint, tt.wantHint)
			}
		})
	}
}

func TestCheckDoctorFollowupGuidance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        workflowconfig.Followups
		body       string
		available  bool
		wantStatus doctorStatus
		wantDetail string
		wantHint   string
	}{
		{
			name:       "enabled passes without workflow prose",
			cfg:        workflowconfig.Followups{Enabled: true},
			available:  true,
			wantStatus: doctorOK,
			wantDetail: "enabled=true provides prompt guidance",
		},
		{
			name:       "disabled passes with workflow prose",
			body:       "File a separate follow-up issue in Backlog for meaningful out-of-scope work.",
			available:  true,
			wantStatus: doctorOK,
			wantDetail: "WORKFLOW.md body provides out-of-scope follow-up filing guidance",
		},
		{
			name:       "disabled warns without workflow prose",
			body:       "Keep changes scoped to the current issue.",
			available:  true,
			wantStatus: doctorWarn,
			wantDetail: "contains no out-of-scope follow-up filing guidance",
			wantHint:   "Enable agent.followups.enabled",
		},
		{
			name:       "unavailable workflow body skips",
			wantStatus: doctorOK,
			wantDetail: "skipped because WORKFLOW.md could not be loaded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got doctorCheck
			if tt.available {
				got = checkDoctorFollowupGuidance("alpha", tt.cfg, tt.body)
			} else {
				got = checkDoctorFollowupGuidanceUnavailable("alpha", "WORKFLOW.md could not be loaded")
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
			if tt.wantHint != "" && !strings.Contains(got.Hint, tt.wantHint) {
				t.Fatalf("Hint = %q, want containing %q", got.Hint, tt.wantHint)
			}
		})
	}
}

func TestCheckDoctorProjectSkills(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(*testing.T, string, *workflowconfig.Skills)
		available  bool
		wantStatus doctorStatus
		wantDetail []string
	}{
		{
			name: "disabled reports configuration without reading files",
			configure: func(_ *testing.T, _ string, cfg *workflowconfig.Skills) {
				cfg.Enabled = false
				cfg.Creation.Enabled = false
			},
			available:  true,
			wantStatus: doctorOK,
			wantDetail: []string{"enabled=false", "creation_enabled=false", "max_drafts_per_run=1", "loaded=0", "dropped=0"},
		},
		{
			name: "healthy directory reports loaded count",
			configure: func(t *testing.T, root string, _ *workflowconfig.Skills) {
				writeDoctorSkill(t, root, "deploy.md", "deploy")
			},
			available:  true,
			wantStatus: doctorOK,
			wantDetail: []string{"enabled=true", "path=.detent/skills", "max_skills_in_prompt=50", "loaded=1", "dropped=0"},
		},
		{
			name: "invalid duplicate and over limit files warn with reasons",
			configure: func(t *testing.T, root string, cfg *workflowconfig.Skills) {
				cfg.MaxSkillsInPrompt = 1
				writeDoctorSkill(t, root, "01-deploy.md", "deploy")
				writeDoctorSkill(t, root, "02-duplicate.md", "deploy")
				writeDoctorSkill(t, root, "03-test.md", "test")
				path := filepath.Join(root, ".detent", "skills", "04-invalid.md")
				if err := os.WriteFile(path, []byte("missing front matter\n"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			},
			available:  true,
			wantStatus: doctorWarn,
			wantDetail: []string{"loaded=1", "dropped=3", "02-duplicate.md (duplicate:", "03-test.md (max_skills_in_prompt:", "04-invalid.md (invalid:"},
		},
		{
			name:       "missing source repository skips",
			available:  false,
			wantStatus: doctorWarn,
			wantDetail: []string{"enabled=true", "skipped because source repository is unavailable locally"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			cfg := workflowconfig.Default().Agent.Skills
			if tt.configure != nil {
				tt.configure(t, root, &cfg)
			}
			var got doctorCheck
			if tt.available {
				got = checkDoctorProjectSkills("alpha", root, cfg)
			} else {
				got = checkDoctorProjectSkillsUnavailable("alpha", cfg, "source repository is unavailable locally")
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func TestCheckDoctorFilesystemProjectSkills(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(*testing.T, *globalconfig.Project, *workflowconfig.Config)
		wantStatus doctorStatus
		wantDetail []string
	}{
		{
			name: "configured source root is inspected",
			configure: func(t *testing.T, _ *globalconfig.Project, cfg *workflowconfig.Config) {
				cfg.Workspace.SourceRoot = t.TempDir()
				writeDoctorSkill(t, cfg.Workspace.SourceRoot, "deploy.md", "deploy")
			},
			wantStatus: doctorOK,
			wantDetail: []string{"loaded=1", "dropped=0"},
		},
		{
			name: "project workdir is inspected",
			configure: func(t *testing.T, project *globalconfig.Project, _ *workflowconfig.Config) {
				project.Workdir = t.TempDir()
			},
			wantStatus: doctorOK,
			wantDetail: []string{"loaded=0", "dropped=0"},
		},
		{
			name: "missing source root skips",
			configure: func(t *testing.T, _ *globalconfig.Project, cfg *workflowconfig.Config) {
				cfg.Workspace.SourceRoot = filepath.Join(t.TempDir(), "missing")
			},
			wantStatus: doctorWarn,
			wantDetail: []string{"skipped because source repository is unavailable locally"},
		},
		{
			name:       "unconfigured source root skips",
			wantStatus: doctorWarn,
			wantDetail: []string{"skipped because source root is not configured"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			project := globalconfig.Project{}
			cfg := workflowconfig.Default()
			cfg.Workspace.Kind = workflowconfig.WorkspaceFilesystem
			cfg.Workspace.Root = ""
			cfg.Workspace.SourceRoot = ""
			if tt.configure != nil {
				tt.configure(t, &project, &cfg)
			}
			got := checkDoctorFilesystemProjectSkills("alpha", project, cfg)
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func writeDoctorSkill(t *testing.T, root string, name string, skillName string) {
	t.Helper()

	dir := filepath.Join(root, ".detent", "skills")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	content := "---\nname: " + skillName + "\ndescription: Test skill.\nwhen_to_use: Doctor tests.\n---\nBody\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func TestCheckDoctorProjectWorkflowReportsReviewFlowChoiceAndProseMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        workflowconfig.Config
		prompt     string
		wantStatus doctorStatus
		wantDetail []string
	}{
		{
			name: "autopilot clean",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.Enabled = true
				cfg.Agent.AutoPromote.QuietSeconds = 0
				cfg.Agent.AutoPromote.GateWaitState = workflowconfig.AutoPromoteGateWaitStateSource
				return cfg
			}(),
			prompt:     "When complete, keep the issue in In Progress and set `detent-status` to `complete`.",
			wantStatus: doctorOK,
			wantDetail: []string{"review-flow=autopilot"},
		},
		{
			name: "autopilot contradicted by review prose",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.Enabled = true
				cfg.Agent.AutoPromote.QuietSeconds = 0
				cfg.Agent.AutoPromote.GateWaitState = workflowconfig.AutoPromoteGateWaitStateSource
				return cfg
			}(),
			prompt:     "When complete, move the issue to `Human Review`.",
			wantStatus: doctorWarn,
			wantDetail: []string{"review-flow=autopilot", "review-flow prose mismatch", "instructs agents to enter the review state"},
		},
		{
			name: "autopilot permits negated review prose",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.Enabled = true
				cfg.Agent.AutoPromote.QuietSeconds = 0
				cfg.Agent.AutoPromote.GateWaitState = workflowconfig.AutoPromoteGateWaitStateSource
				return cfg
			}(),
			prompt:     "Do NOT move the issue to Human Review. Never move the work item to Human Review.",
			wantStatus: doctorOK,
			wantDetail: []string{"review-flow=autopilot"},
		},
		{
			name: "autopilot detects affirmative instruction in mixed prose",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.Enabled = true
				cfg.Agent.AutoPromote.QuietSeconds = 0
				cfg.Agent.AutoPromote.GateWaitState = workflowconfig.AutoPromoteGateWaitStateSource
				return cfg
			}(),
			prompt:     "Do not move the issue to Human Review.\nWhen requested, move the issue to Human Review.",
			wantStatus: doctorWarn,
			wantDetail: []string{"review-flow=autopilot", "review-flow prose mismatch"},
		},
		{
			name: "autopilot contradicted by custom review state prose",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.Enabled = true
				cfg.Agent.AutoPromote.QuietSeconds = 0
				cfg.Agent.AutoPromote.GateWaitState = workflowconfig.AutoPromoteGateWaitStateSource
				cfg.Agent.AutoPromote.SourceState = "Review"
				return cfg
			}(),
			prompt:     "When complete, move the work item to `Review`.",
			wantStatus: doctorWarn,
			wantDetail: []string{"review-flow=autopilot", "review-flow prose mismatch", "instructs agents to enter the review state"},
		},
		{
			name:       "review gate clean",
			cfg:        validDoctorWorkflow("/repo"),
			prompt:     "When complete, move the issue to `Human Review` for approval.",
			wantStatus: doctorOK,
			wantDetail: []string{"review-flow=review-gate"},
		},
		{
			name:       "review gate contradicted by direct promotion prose",
			cfg:        validDoctorWorkflow("/repo"),
			prompt:     "Do not move the issue to Human Review; Detent promotes the issue directly to `Merging`.",
			wantStatus: doctorWarn,
			wantDetail: []string{"review-flow=review-gate", "review-flow prose mismatch", "promises direct review-state skipping"},
		},
		{
			name:       "review gate permits negated skip and direct promotion prose",
			cfg:        validDoctorWorkflow("/repo"),
			prompt:     "Do not skip Human Review. Do not promote the issue directly to Merging.",
			wantStatus: doctorOK,
			wantDetail: []string{"review-flow=review-gate"},
		},
		{
			name: "review gate contradicted by custom direct promotion prose",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorWorkflow("/repo")
				cfg.Agent.AutoPromote.SourceState = "Review"
				cfg.Agent.AutoPromote.PassState = "Done"
				return cfg
			}(),
			prompt:     "Never move the issue to Review; promote directly to `Done`.",
			wantStatus: doctorWarn,
			wantDetail: []string{"review-flow=review-gate", "review-flow prose mismatch", "promises direct review-state skipping"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checks := checkDoctorProject(context.Background(), globalconfig.Project{
				ID:       "alpha",
				Workflow: "WORKFLOW.md",
				Workdir:  "/repo",
			}, doctorDeps{
				loadWorkflow: func(string) (workflowconfig.Workflow, error) {
					return workflowconfig.Workflow{Config: tt.cfg, Prompt: tt.prompt}, nil
				},
				gitWorkTree: func(context.Context, string) error {
					return nil
				},
			}, RuntimeSecret{}, false)
			if len(checks) == 0 {
				t.Fatal("checks len = 0, want workflow check")
			}
			got := checks[0]
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
			if tt.wantStatus == doctorWarn && len(got.WorkflowOptimization.Proposals) == 0 {
				t.Fatalf("WorkflowOptimization.Proposals len = 0, want governed proposal: %#v", got.WorkflowOptimization)
			}
		})
	}
}

func TestCheckDoctorConfigReload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "global.yaml")
	if err := os.WriteFile(path, []byte("global"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got := checkDoctorConfigReload(context.Background(), globalconfig.Config{Path: path}, func(context.Context, string, ...string) (string, error) {
		return "", errors.New("not a git repository")
	})
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s", got.Status, doctorOK)
	}
	if !strings.Contains(got.Detail, "is watched for live reload") {
		t.Fatalf("Detail = %q, want live reload detail", got.Detail)
	}
}

func TestCheckDoctorConfigReloadReportsSymlinkTarget(t *testing.T) {
	t.Parallel()

	linkDir := t.TempDir()
	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "global.yaml")
	linkPath := filepath.Join(linkDir, "global.yaml")
	if err := os.WriteFile(targetPath, []byte("global"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	resolvedTarget, err := filepath.EvalSymlinks(targetPath)
	if err != nil {
		t.Fatalf("EvalSymlinks() error = %v", err)
	}

	got := checkDoctorConfigReload(context.Background(), globalconfig.Config{Path: linkPath}, func(context.Context, string, ...string) (string, error) {
		return "", errors.New("not a git repository")
	})
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s", got.Status, doctorOK)
	}
	for _, want := range []string{linkPath, resolvedTarget, "symlink", "live reload watches"} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
		}
	}
}

func TestCheckDoctorConfigReloadReportsGitStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T) (globalconfig.Config, CommandRunner)
		wantStatus doctorStatus
		wantDetail []string
		wantHint   []string
		avoid      []string
	}{
		{
			name: "regular tracked config is clean",
			setup: func(t *testing.T) (globalconfig.Config, CommandRunner) {
				repo := t.TempDir()
				path := filepath.Join(repo, "global.yaml")
				if err := os.WriteFile(path, []byte("global"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				runDoctorWorkflowSourceGit(t, repo, "init")
				runDoctorWorkflowSourceGit(t, repo, "add", "global.yaml")
				runDoctorWorkflowSourceGit(t, repo, "-c", "user.name=Detent Test", "-c", "user.email=detent@example.com", "commit", "-m", "initial")
				return globalconfig.Config{Path: path}, defaultCommandRunner
			},
			wantStatus: doctorOK,
			wantDetail: []string{"is watched for live reload", "git repository", "global.yaml", "tracked and clean"},
		},
		{
			name: "symlink target has tracked changes",
			setup: func(t *testing.T) (globalconfig.Config, CommandRunner) {
				repo := t.TempDir()
				targetPath := filepath.Join(repo, "global.yaml")
				if err := os.WriteFile(targetPath, []byte("before"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				runDoctorWorkflowSourceGit(t, repo, "init")
				runDoctorWorkflowSourceGit(t, repo, "add", "global.yaml")
				runDoctorWorkflowSourceGit(t, repo, "-c", "user.name=Detent Test", "-c", "user.email=detent@example.com", "commit", "-m", "initial")
				if err := os.WriteFile(targetPath, []byte("after"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				linkPath := filepath.Join(t.TempDir(), "global.yaml")
				if err := os.Symlink(targetPath, linkPath); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
				return globalconfig.Config{Path: linkPath}, defaultCommandRunner
			},
			wantStatus: doctorWarn,
			wantDetail: []string{"symlink", "git repository", "global.yaml", "tracked status", " M global.yaml"},
			wantHint:   []string{"not durably recorded", "git checkout", "git reset", "discard"},
		},
		{
			name: "config outside repository is skipped",
			setup: func(t *testing.T) (globalconfig.Config, CommandRunner) {
				path := filepath.Join(t.TempDir(), "global.yaml")
				if err := os.WriteFile(path, []byte("global"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				return globalconfig.Config{Path: path}, func(context.Context, string, ...string) (string, error) {
					return "", errors.New("not a git repository")
				}
			},
			wantStatus: doctorOK,
			wantDetail: []string{"is watched for live reload"},
			avoid:      []string{"git repository", "not durably recorded"},
		},
		{
			name: "git unavailable is skipped",
			setup: func(t *testing.T) (globalconfig.Config, CommandRunner) {
				path := filepath.Join(t.TempDir(), "global.yaml")
				if err := os.WriteFile(path, []byte("global"), 0o600); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				return globalconfig.Config{Path: path}, func(context.Context, string, ...string) (string, error) {
					return "", errors.New("git unavailable")
				}
			},
			wantStatus: doctorOK,
			wantDetail: []string{"is watched for live reload"},
			avoid:      []string{"git repository", "not durably recorded"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, runner := tt.setup(t)
			got := checkDoctorConfigReload(context.Background(), cfg, runner)
			if got.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.wantStatus, got)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
			for _, want := range tt.wantHint {
				if !strings.Contains(got.Hint, want) {
					t.Fatalf("Hint = %q, want containing %q", got.Hint, want)
				}
			}
			for _, avoid := range tt.avoid {
				if strings.Contains(got.Detail+got.Hint, avoid) {
					t.Fatalf("check output = %q, want not containing %q", got.Detail+got.Hint, avoid)
				}
			}
		})
	}
}

func TestProjectSourceRootPrefersProjectWorkdirBeforeWorkspaceRoot(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Workspace.Root = "/worktrees"
	project := globalconfig.Project{Workdir: "/source"}

	if got := projectSourceRoot(project, cfg); got != "/source" {
		t.Fatalf("projectSourceRoot() = %q, want /source", got)
	}

	cfg.Workspace.SourceRoot = "/configured-source"
	if got := projectSourceRoot(project, cfg); got != "/configured-source" {
		t.Fatalf("projectSourceRoot() with source_root = %q, want /configured-source", got)
	}
}

func TestCheckDoctorAutoPromote(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	oldActivity := now.Add(-20 * time.Minute)
	prNumber := 42
	waitingIssue := doctorAutoPromoteIssue("issue-ci", &connector.PullRequest{
		Number:           41,
		URL:              "https://github.test/pull/41",
		State:            "OPEN",
		CIStatus:         "fail",
		CodexReviewState: "COMMENTED",
	})
	missingReviewIssue := doctorAutoPromoteIssue("issue-review", &connector.PullRequest{
		Number:   43,
		URL:      "https://github.test/pull/43",
		State:    "OPEN",
		CIStatus: "success",
	})
	conflictingIssue := doctorAutoPromoteIssue("issue-conflicting", &connector.PullRequest{
		Number:         45,
		URL:            "https://github.test/pull/45",
		State:          "OPEN",
		MergeableState: "dirty",
	})
	linkedWithoutMetadata := doctorAutoPromoteIssue("issue-missing-pr", nil)
	linkedWithoutMetadata.PRNumber = &prNumber
	readyIssue := doctorAutoPromoteIssue("issue-ready", &connector.PullRequest{
		Number:                 44,
		URL:                    "https://github.test/pull/44",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldActivity,
	})
	mergedWithoutReview := doctorAutoPromoteIssue("issue-merged-missing-review", &connector.PullRequest{
		Number: 46,
		URL:    "https://github.test/pull/46",
		State:  "MERGED",
	})
	mergedWithReview := doctorAutoPromoteIssue("issue-merged-reviewed", &connector.PullRequest{
		Number:           47,
		URL:              "https://github.test/pull/47",
		State:            "MERGED",
		CodexReviewState: "COMMENTED",
	})

	tests := []struct {
		name        string
		cfg         workflowconfig.Config
		connector   *fakeDoctorAutoPromoteConnector
		want        doctorStatus
		wantDetails []string
	}{
		{
			name:        "disabled",
			cfg:         validDoctorWorkflow("/repo"),
			want:        doctorOK,
			wantDetails: []string{"review-flow=review-gate", "auto_promote.enabled=false"},
		},
		{
			name: "human review observed state is not required",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorAutoPromoteWorkflow()
				cfg.Tracker.ObservedStates = []string{"Blocked"}
				return cfg
			}(),
			connector:   &fakeDoctorAutoPromoteConnector{},
			want:        doctorOK,
			wantDetails: []string{"sampled 0 Human Review candidate"},
		},
		{
			name: "missing merging active state",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorAutoPromoteWorkflow()
				cfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework"}
				return cfg
			}(),
			want:        doctorFail,
			wantDetails: []string{"tracker.active_states", "Merging"},
		},
		{
			name: "missing blocked state with rework limit",
			cfg: func() workflowconfig.Config {
				cfg := validDoctorAutoPromoteWorkflow()
				cfg.Agent.AutoPromote.ReworkLimit = 1
				cfg.Tracker.ObservedStates = []string{"Human Review"}
				return cfg
			}(),
			want:        doctorFail,
			wantDetails: []string{"agent.auto_promote.rework_limit", "Blocked", "tracker.observed_states"},
		},
		{
			name: "status option verification fails",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				verifyErr: errors.New("github status option not found: Human Review maps to Reviewing"),
			},
			want:        doctorFail,
			wantDetails: []string{"status option", "Human Review", "Reviewing"},
		},
		{
			name: "linked pr missing metadata fails",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{linkedWithoutMetadata},
			},
			want:        doctorFail,
			wantDetails: []string{"missing_pull_request", "linked PR #42", "issue-missing-pr"},
		},
		{
			name: "expected waiting reasons pass with counts",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{waitingIssue, missingReviewIssue},
			},
			want:        doctorOK,
			wantDetails: []string{"automated_review_missing=1", "ci_not_green=1"},
		},
		{
			name: "conflicting pull request reports merge conflict reason",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{conflictingIssue},
			},
			want:        doctorOK,
			wantDetails: []string{"merge_conflicts=1", "issue-conflicting", "PR #45", "mergeable=dirty", "reason=merge_conflicts"},
		},
		{
			name: "ready candidate passes with count",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{readyIssue},
			},
			want:        doctorOK,
			wantDetails: []string{"ready=1"},
		},
		{
			name: "dead automated review producer warns",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				mergedIssues: []connector.Issue{mergedWithoutReview},
			},
			want:        doctorWarn,
			wantDetails: []string{"recent merged PR automated reviews=0/1", "producer appears inactive"},
		},
		{
			name: "working automated review producer passes",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				mergedIssues: []connector.Issue{mergedWithReview},
			},
			want:        doctorOK,
			wantDetails: []string{"recent merged PR automated reviews=1/1"},
		},
		{
			name: "project state count discrepancy fails",
			cfg:  validDoctorAutoPromoteWorkflow(),
			connector: &fakeDoctorAutoPromoteConnector{
				scan: &connector.IssueStateScan{
					Issues:           []connector.Issue{},
					BoardCounts:      map[string]int{"Human Review": 2},
					EnumeratedCounts: map[string]int{"Human Review": 0},
					ItemsFetched:     1002,
					TotalItems:       1002,
				},
			},
			want:        doctorFail,
			wantDetails: []string{"ProjectV2 items fetched=1002 total=1002", "Human Review counts board=2 enumerated=0", "disagree"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := doctorDeps{}
			if tt.connector != nil {
				deps.autoPromoteConnector = func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
					return tt.connector, nil
				}
			}
			got := checkDoctorAutoPromote(context.Background(), "alpha", tt.cfg, deps, now)
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.want, got)
			}
			for _, want := range tt.wantDetails {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func TestCheckDoctorAutoPromoteWarnsOnInvalidWorkpadStatus(t *testing.T) {
	t.Parallel()

	cfg := validDoctorAutoPromoteWorkflow()
	issue := doctorAutoPromoteIssue("issue-invalid-status", &connector.PullRequest{
		Number:         981,
		URL:            "https://github.test/pull/981",
		State:          "OPEN",
		HeadSHA:        "head-invalid",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.Comments = []connector.IssueComment{{
		URL:  "https://github.test/digitaldrywood/detent/issues/981#issuecomment-1",
		Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: human-review\nblockers: []\nhuman_action: null\n```",
	}}

	got := checkDoctorAutoPromote(context.Background(), "alpha", cfg, doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return &fakeDoctorAutoPromoteConnector{issues: []connector.Issue{issue}}, nil
		},
	}, time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC))

	if got.Status != doctorWarn {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorWarn, got)
	}
	for _, want := range []string{
		`workpad_status_invalid=status "human-review"`,
		"workpad_comment_url=https://github.test/digitaldrywood/detent/issues/981#issuecomment-1",
		"one of in_progress, blocked, or complete",
	} {
		if !strings.Contains(got.Detail, want) && !strings.Contains(got.Hint, want) {
			t.Fatalf("check missing %q:\nDetail: %s\nHint: %s", want, got.Detail, got.Hint)
		}
	}
	if len(got.AutoPromoteCandidates) != 1 || !strings.Contains(got.AutoPromoteCandidates[0].WorkpadStatusInvalid, `"human-review"`) {
		t.Fatalf("AutoPromoteCandidates = %#v, want invalid status diagnostic", got.AutoPromoteCandidates)
	}
}

func TestCheckDoctorAutoPromoteVerifiesBlockedStatusWhenReworkLimitEnabled(t *testing.T) {
	t.Parallel()

	cfg := validDoctorAutoPromoteWorkflow()
	cfg.Agent.AutoPromote.ReworkLimit = 1
	fake := &fakeDoctorAutoPromoteConnector{}
	got := checkDoctorAutoPromote(context.Background(), "alpha", cfg, doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return fake, nil
		},
	}, time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC))

	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
	}
	for _, want := range []string{"Human Review", "Merging", "Rework", "Blocked"} {
		if !stringSliceContains(fake.verifyStates, want) {
			t.Fatalf("VerifyStatusOptions states = %#v, want %q", fake.verifyStates, want)
		}
	}
}

func TestCheckDoctorAutoPromoteCandidateDiagnostics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)

	tests := []struct {
		name  string
		issue connector.Issue
		want  doctorAutoPromoteCandidateDiagnostic
	}{
		{
			name: "missing review",
			issue: doctorAutoPromoteIssue("issue-missing-review", &connector.PullRequest{
				Number:   403,
				URL:      "https://github.test/pull/403",
				State:    "OPEN",
				HeadSHA:  "head-missing-review",
				CIStatus: "success",
			}),
			want: doctorAutoPromoteCandidateDiagnostic{
				IssueID:         "issue-missing-review",
				IssueIdentifier: "digitaldrywood/detent#399",
				PRNumber:        403,
				PRURL:           "https://github.test/pull/403",
				PRHeadSHA:       "head-missing-review",
				Reason:          "automated_review_missing",
			},
		},
		{
			name: "stale review",
			issue: doctorAutoPromoteIssue("issue-stale-review", &connector.PullRequest{
				Number:                       411,
				URL:                          "https://github.test/pull/411",
				State:                        "OPEN",
				HeadSHA:                      "head-current",
				CIStatus:                     "success",
				LatestCodexReviewState:       "COMMENTED",
				LatestCodexReviewCommitSHA:   "head-previous",
				LatestCodexReviewSubmittedAt: &oldReview,
			}),
			want: doctorAutoPromoteCandidateDiagnostic{
				IssueID:                      "issue-stale-review",
				IssueIdentifier:              "digitaldrywood/detent#399",
				PRNumber:                     411,
				PRURL:                        "https://github.test/pull/411",
				PRHeadSHA:                    "head-current",
				LatestCodexReviewState:       "COMMENTED",
				LatestCodexReviewCommitSHA:   "head-previous",
				LatestCodexReviewSubmittedAt: &oldReview,
				Reason:                       "stale_automated_review",
			},
		},
		{
			name: "ready current head review",
			issue: doctorAutoPromoteIssue("issue-ready-review", &connector.PullRequest{
				Number:                       398,
				URL:                          "https://github.test/pull/398",
				State:                        "OPEN",
				HeadSHA:                      "head-ready",
				CIStatus:                     "success",
				CodexReviewState:             "COMMENTED",
				CodexReviewSource:            connector.PullRequestReviewSourceSummaryComment,
				CodexReviewSubmittedAt:       &oldReview,
				LatestCodexReviewState:       "COMMENTED",
				LatestCodexReviewCommitSHA:   "head-ready",
				LatestCodexReviewSubmittedAt: &oldReview,
			}),
			want: doctorAutoPromoteCandidateDiagnostic{
				IssueID:                      "issue-ready-review",
				IssueIdentifier:              "digitaldrywood/detent#399",
				PRNumber:                     398,
				PRURL:                        "https://github.test/pull/398",
				PRHeadSHA:                    "head-ready",
				CodexReviewSource:            connector.PullRequestReviewSourceSummaryComment,
				LatestCodexReviewState:       "COMMENTED",
				LatestCodexReviewCommitSHA:   "head-ready",
				LatestCodexReviewSubmittedAt: &oldReview,
				Reason:                       "ready",
			},
		},
		{
			name: "merge conflict",
			issue: doctorAutoPromoteIssue("issue-conflicting", &connector.PullRequest{
				Number:         614,
				URL:            "https://github.test/pull/614",
				State:          "OPEN",
				HeadSHA:        "head-conflicting",
				MergeableState: "dirty",
			}),
			want: doctorAutoPromoteCandidateDiagnostic{
				IssueID:          "issue-conflicting",
				IssueIdentifier:  "digitaldrywood/detent#399",
				PRNumber:         614,
				PRURL:            "https://github.test/pull/614",
				PRHeadSHA:        "head-conflicting",
				PRMergeableState: "dirty",
				Reason:           "merge_conflicts",
			},
		},
		{
			name: "clean green workpad blocker",
			issue: func() connector.Issue {
				issue := doctorAutoPromoteIssue("issue-workpad-blocker", &connector.PullRequest{
					Number:         1480,
					URL:            "https://github.test/pull/1480",
					State:          "OPEN",
					HeadSHA:        "head-workpad",
					MergeableState: "clean",
					CIStatus:       "success",
				})
				issue.Comments = []connector.IssueComment{{
					Body: "## Codex Workpad\n\n### Blockers\n- Owner approval is still required.",
				}}
				return issue
			}(),
			want: doctorAutoPromoteCandidateDiagnostic{
				IssueID:             "issue-workpad-blocker",
				IssueIdentifier:     "digitaldrywood/detent#399",
				PRNumber:            1480,
				PRURL:               "https://github.test/pull/1480",
				PRHeadSHA:           "head-workpad",
				PRMergeableState:    "clean",
				Reason:              "workpad_blocker",
				WorkpadBlocker:      "Owner approval is still required.",
				WorkpadSignalSource: "prose_section",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := checkDoctorAutoPromote(context.Background(), "alpha", validDoctorAutoPromoteWorkflow(), doctorDeps{
				autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
					return &fakeDoctorAutoPromoteConnector{issues: []connector.Issue{tt.issue}}, nil
				},
			}, now)
			if got.Status != doctorOK {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
			}
			if len(got.AutoPromoteCandidates) != 1 {
				t.Fatalf("AutoPromoteCandidates len = %d, want 1: %#v", len(got.AutoPromoteCandidates), got.AutoPromoteCandidates)
			}
			if got.AutoPromoteCandidates[0] != tt.want {
				t.Fatalf("AutoPromoteCandidates[0] = %#v, want %#v", got.AutoPromoteCandidates[0], tt.want)
			}
			for _, want := range []string{tt.want.IssueID, tt.want.PRHeadSHA, tt.want.Reason} {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
			if tt.want.WorkpadBlocker != "" && !strings.Contains(got.Detail, tt.want.WorkpadBlocker) {
				t.Fatalf("Detail = %q, want containing WorkpadBlocker %q", got.Detail, tt.want.WorkpadBlocker)
			}
			if tt.want.CodexReviewSource != "" && !strings.Contains(got.Detail, "review_source="+tt.want.CodexReviewSource) {
				t.Fatalf("Detail = %q, want review source %q", got.Detail, tt.want.CodexReviewSource)
			}
		})
	}
}

func TestWriteDoctorJSONReportIncludesAutoPromoteCandidateDiagnostics(t *testing.T) {
	t.Parallel()

	reviewedAt := time.Date(2026, 6, 12, 11, 40, 0, 0, time.UTC)
	report := doctorReport{Checks: []doctorCheck{{
		Name:   "Project alpha auto-promote",
		Status: doctorOK,
		Detail: "sampled 1 Human Review candidate",
		AutoPromoteCandidates: []doctorAutoPromoteCandidateDiagnostic{{
			IssueID:                      "issue-stale-review",
			IssueIdentifier:              "digitaldrywood/detent#399",
			PRNumber:                     411,
			PRURL:                        "https://github.test/pull/411",
			PRHeadSHA:                    "head-current",
			LatestCodexReviewState:       "COMMENTED",
			LatestCodexReviewCommitSHA:   "head-previous",
			LatestCodexReviewSubmittedAt: &reviewedAt,
			Reason:                       "stale_automated_review",
		}},
	}}}

	var output bytes.Buffer
	if err := json.NewEncoder(&output).Encode(newDoctorOutputReport(report)); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	var got struct {
		Checks []struct {
			AutoPromoteCandidates []doctorAutoPromoteCandidateDiagnostic `json:"auto_promote_candidates"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\n%s", err, output.String())
	}
	if len(got.Checks) != 1 || len(got.Checks[0].AutoPromoteCandidates) != 1 {
		t.Fatalf("decoded candidates = %#v, want one candidate", got)
	}
	if got.Checks[0].AutoPromoteCandidates[0].Reason != "stale_automated_review" {
		t.Fatalf("Reason = %q, want stale_automated_review", got.Checks[0].AutoPromoteCandidates[0].Reason)
	}
}

func TestCheckDoctorDependencyAutoUnblock(t *testing.T) {
	t.Parallel()

	readyRef := "digitaldrywood/detent#387"
	unresolvedRef := "digitaldrywood/detent#9999"

	tests := []struct {
		name        string
		cfg         workflowconfig.Config
		connector   *fakeDoctorAutoPromoteConnector
		want        doctorStatus
		wantDetails []string
	}{
		{
			name: "disabled auto unblock warns with candidates and references",
			cfg:  validDoctorDependencyWorkflow(false),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					doctorDependencyIssue("issue-blocked", []connector.BlockedRef{{Identifier: readyRef}}),
				},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_auto_unblock_disabled",
				"issue-blocked",
				"digitaldrywood/detent#blocked",
				readyRef,
				"tracker.dependency_auto_unblock.enabled: true",
			},
		},
		{
			name: "unresolved dependency reference warns with issue content fix",
			cfg:  validDoctorDependencyWorkflow(true),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					func() connector.Issue {
						issue := doctorDependencyIssueWithBody("issue-unresolved", "Depends on: "+unresolvedRef)
						issue.BlockedBy = []connector.BlockedRef{{Identifier: unresolvedRef}}
						return issue
					}(),
				},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_reference_unresolved",
				"issue-unresolved",
				unresolvedRef,
				"Depends on:",
			},
		},
		{
			name: "prose only dependency refs warn with native dependency fix",
			cfg:  validDoctorDependencyWorkflow(true),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					doctorDependencyIssue("issue-416", []connector.BlockedRef{
						{Identifier: "digitaldrywood/detent#415"},
						{Identifier: "digitaldrywood/detent#416"},
					}),
				},
				capabilities: []connector.DependencyCapability{{
					Repository:      "digitaldrywood/detent",
					NativeBlockedBy: "unavailable",
					Source:          workflowconfig.DependencySourceMerged,
					Detail:          "status 404",
				}},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_prose_only",
				"issue-416",
				"digitaldrywood/detent#415",
				"source=prose",
				"native GitHub issue dependencies",
				"dependency_capability repository=digitaldrywood/detent native_blocked_by=unavailable source=merged detail=status 404",
			},
		},
		{
			name: "dependency line without structured ref warns with issue content fix",
			cfg:  validDoctorDependencyWorkflow(true),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					doctorDependencyIssueWithBody("issue-line", "Depends on: release train"),
				},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_reference_unresolved",
				"issue-line",
				"release train",
				"Depends on:",
			},
		},
		{
			name: "ready blockers still blocked warns with config fix",
			cfg:  validDoctorDependencyWorkflow(true),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					func() connector.Issue {
						issue := doctorDependencyIssueWithBody("issue-ready", "Depends on: "+readyRef)
						issue.BlockedBy = []connector.BlockedRef{{Identifier: readyRef}}
						return issue
					}(),
				},
				resolvedIssues: []connector.Issue{
					doctorDependencyResolvedIssue("blocker-done", readyRef, "Done", false, nil),
				},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_terminal_but_still_blocked",
				"issue-ready",
				readyRef,
				"consumed-signature latch",
			},
		},
		{
			name: "hydrated body metadata is merged with connector refs",
			cfg:  validDoctorDependencyWorkflow(true),
			connector: &fakeDoctorAutoPromoteConnector{
				issues: []connector.Issue{
					doctorDependencyIssue("issue-416", []connector.BlockedRef{{Identifier: "digitaldrywood/detent#415"}}),
				},
				hydratedIssues: []connector.Issue{
					doctorDependencyIssueWithBody("issue-416", strings.Join([]string{
						"Depends on: #414",
						"Depends on: #415",
					}, "\n")),
				},
				resolvedIssues: []connector.Issue{
					doctorDependencyResolvedIssue("blocker-414", "digitaldrywood/detent#414", "Done", false, nil),
					doctorDependencyResolvedIssue("blocker-415", "digitaldrywood/detent#415", "Done", false, nil),
				},
			},
			want: doctorWarn,
			wantDetails: []string{
				"dependency_terminal_but_still_blocked",
				"issue-416",
				"digitaldrywood/detent#414",
				"digitaldrywood/detent#415",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := doctorDeps{
				autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
					return tt.connector, nil
				},
			}
			got := checkDoctorDependencyAutoUnblock(context.Background(), "alpha", tt.cfg, deps)
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s: %#v", got.Status, tt.want, got)
			}
			for _, want := range tt.wantDetails {
				if !strings.Contains(got.Detail, want) && !strings.Contains(got.Hint, want) {
					t.Fatalf("check missing %q:\nDetail: %s\nHint: %s", want, got.Detail, got.Hint)
				}
			}
			if tt.connector.limit != doctorDependencyAutoUnblockSampleLimit {
				t.Fatalf("FetchIssuesByStatesLimit limit = %d, want %d", tt.connector.limit, doctorDependencyAutoUnblockSampleLimit)
			}
			if !stringSliceContains(tt.connector.verifyStates, "Blocked") {
				t.Fatalf("VerifyStatusOptions states = %#v, want Blocked", tt.connector.verifyStates)
			}
			if tt.cfg.Tracker.DependencyAutoUnblock.Enabled && !stringSliceContains(tt.connector.verifyStates, "Todo") {
				t.Fatalf("VerifyStatusOptions states = %#v, want Todo", tt.connector.verifyStates)
			}
			if tt.cfg.Tracker.DependencyAutoUnblock.Enabled && !stringSliceContains(tt.connector.verifyStates, "Rework") {
				t.Fatalf("VerifyStatusOptions states = %#v, want Rework", tt.connector.verifyStates)
			}
		})
	}
}

func TestDoctorDependencyTextBlockedRefsAcceptsSharedDependencyLines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "depends on no colon",
			body: "Depends on #414",
			want: []string{"digitaldrywood/detent#414 source=prose"},
		},
		{
			name: "blocked by no colon",
			body: "Blocked by #415",
			want: []string{"digitaldrywood/detent#415 source=prose"},
		},
		{
			name: "depends on colon",
			body: "Depends on: #416",
			want: []string{"digitaldrywood/detent#416 source=prose"},
		},
		{
			name: "depends hyphen owner repo",
			body: "depends-on digitaldrywood/agent-runtime#27",
			want: []string{"digitaldrywood/agent-runtime#27 source=prose"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doctorBlockedRefLabels(doctorDependencyTextBlockedRefs(doctorDependencyIssueWithBody("issue-416", tt.body)))
			if !slices.Equal(got, tt.want) {
				t.Fatalf("doctorDependencyTextBlockedRefs() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestCheckDoctorDependencyAutoUnblockRequiresActiveRework(t *testing.T) {
	t.Parallel()

	cfg := validDoctorDependencyWorkflow(true)
	cfg.Tracker.ActiveStates = []string{"Todo", "In Progress"}

	got := checkDoctorDependencyAutoUnblock(context.Background(), "alpha", cfg, doctorDeps{})
	if got.Status != doctorFail {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorFail, got)
	}
	for _, want := range []string{"tracker.active_states", "Rework"} {
		if !strings.Contains(got.Detail, want) && !strings.Contains(got.Hint, want) {
			t.Fatalf("check missing %q:\nDetail: %s\nHint: %s", want, got.Detail, got.Hint)
		}
	}
}

func TestCheckDoctorBlockedRecovery(t *testing.T) {
	t.Parallel()

	prNumber := 426
	humanPRNumber := 427
	recoverable := doctorDependencyIssue("issue-recoverable", nil)
	recoverable.PRNumber = &prNumber
	recoverable.PullRequest = &connector.PullRequest{
		Number:         prNumber,
		State:          "OPEN",
		HeadSHA:        "head-current",
		MergeableState: "dirty",
	}
	recoverable.BlockerReason = "PR #426 conflicts with main and needs agent maintenance."
	recoverable.WorkpadSignal = &workpad.Signal{
		Source:     workpad.SourceStructured,
		Status:     workpad.StatusBlocked,
		ReasonCode: "merge_conflict",
	}
	humanOnly := doctorDependencyIssue("issue-human", nil)
	humanOnly.PRNumber = &humanPRNumber
	humanOnly.PullRequest = &connector.PullRequest{
		Number:         humanPRNumber,
		State:          "OPEN",
		HeadSHA:        "head-current",
		MergeableState: "dirty",
	}
	humanOnly.BlockerReason = "Waiting on explicit human approval."
	manualPRNumber := 429
	manual := doctorDependencyIssue("issue-manual", nil)
	manual.PRNumber = &manualPRNumber
	manual.PullRequest = &connector.PullRequest{
		Number:         manualPRNumber,
		State:          "OPEN",
		HeadSHA:        "head-current",
		MergeableState: "dirty",
	}
	fake := &fakeDoctorAutoPromoteConnector{
		issues: []connector.Issue{recoverable, humanOnly, manual},
	}
	cfg := validDoctorDependencyWorkflow(true)
	cfg.Tracker.BlockedRecovery.Enabled = true

	got := checkDoctorBlockedRecovery(context.Background(), "alpha", cfg, "", doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return fake, nil
		},
	})

	if got.Status != doctorWarn {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorWarn, got)
	}
	for _, want := range []string{
		"pr_condition_match_pending_timeline_authorization",
		"issue-recoverable",
		"merge_conflicts",
		"Rework",
	} {
		if !strings.Contains(got.Detail, want) && !strings.Contains(got.Hint, want) {
			t.Fatalf("check missing %q:\nDetail: %s\nHint: %s", want, got.Detail, got.Hint)
		}
	}
	for _, excluded := range []string{"issue-human"} {
		if strings.Contains(got.Detail, excluded) {
			t.Fatalf("Detail = %q, want %q omitted from condition diagnostics", got.Detail, excluded)
		}
	}
	if !strings.Contains(got.Detail, "issue-manual") || len(got.BlockedWithoutRecovery) != 1 {
		t.Fatalf("relation-less diagnostics = %#v; detail = %q", got.BlockedWithoutRecovery, got.Detail)
	}
	if !stringSliceContains(fake.verifyStates, "Blocked") || !stringSliceContains(fake.verifyStates, "Rework") {
		t.Fatalf("VerifyStatusOptions states = %#v, want Blocked and Rework", fake.verifyStates)
	}
}

func TestDoctorBlockedRecoveryDistinguishesHeldFromDeferred(t *testing.T) {
	t.Parallel()

	held := doctorDependencyIssue("issue-held", nil)
	held.Identifier = "digitaldrywood/detent#6"
	held.WorkpadSignal = &workpad.Signal{
		Source:  workpad.SourceStructured,
		Invalid: &workpad.Invalid{Message: "status must be in_progress, blocked, or complete"},
	}
	dependent := doctorDependencyIssue("issue-dependent", []connector.BlockedRef{{ID: held.ID, Identifier: held.Identifier}})
	deferred := doctorDependencyIssue("issue-deferred", []connector.BlockedRef{{Identifier: "digitaldrywood/detent#999"}})

	tests := []struct {
		name       string
		issues     []connector.Issue
		wantStatus doctorStatus
		wantHeld   int
	}{
		{
			name:       "held root is reported once while dependent is attributed to it",
			issues:     []connector.Issue{dependent, held},
			wantStatus: doctorWarn,
			wantHeld:   1,
		},
		{
			name:       "deferred dependency is not human attention",
			issues:     []connector.Issue{deferred},
			wantStatus: doctorOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			check := checkDoctorBlockedRecoveryLive(
				t.Context(),
				"Project alpha blocked recovery",
				&fakeDoctorAutoPromoteConnector{issues: tt.issues},
				validDoctorDependencyWorkflow(true),
				time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
				nil,
			)

			if check.Status != tt.wantStatus || len(check.PermanentlyHeldRecoveries) != tt.wantHeld {
				t.Fatalf("check = %#v", check)
			}
			if tt.wantHeld == 0 {
				if strings.Contains(check.Detail, "needs human attention") {
					t.Fatalf("Detail = %q", check.Detail)
				}
				return
			}
			diagnostic := check.PermanentlyHeldRecoveries[0]
			if diagnostic.IssueIdentifier != held.Identifier || diagnostic.Action != "hold" || diagnostic.Reason != "invalid_workpad_signal" || !diagnostic.NeedsHumanAttention {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
			if !strings.Contains(diagnostic.Remedy, "fresh-work lane") || !strings.Contains(diagnostic.Remedy, "no pull request") {
				t.Fatalf("Remedy = %q", diagnostic.Remedy)
			}
			if strings.Contains(check.Detail, dependent.ID) {
				t.Fatalf("Detail = %q, dependent must not appear as a second symptom", check.Detail)
			}
		})
	}
}

func TestDoctorBlockedRecoveryClassifiesHumanHolds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(*workflowconfig.Config)
		issue      connector.Issue
		wantReason string
		wantRemedy string
	}{
		{
			name: "structured human action",
			issue: func() connector.Issue {
				issue := doctorDependencyIssue("issue-human-action", nil)
				issue.WorkpadSignal = &workpad.Signal{Source: workpad.SourceStructured, HumanAction: "Approve the external deployment."}
				return issue
			}(),
			wantReason: "human_action",
			wantRemedy: "Approve the external deployment.",
		},
		{
			name: "configured optout label",
			configure: func(cfg *workflowconfig.Config) {
				cfg.Agent.AutoPromote.OptoutLabel = "operator-review"
			},
			issue: func() connector.Issue {
				issue := doctorDependencyIssue("issue-optout", nil)
				issue.Labels = []string{"operator-review"}
				return issue
			}(),
			wantReason: "human_action",
			wantRemedy: "Complete the requested human action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validDoctorDependencyWorkflow(true)
			if tt.configure != nil {
				tt.configure(&cfg)
			}
			check := checkDoctorBlockedRecoveryLive(
				t.Context(),
				"Project alpha blocked recovery",
				&fakeDoctorAutoPromoteConnector{issues: []connector.Issue{tt.issue}},
				cfg,
				time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
				nil,
			)

			if check.Status != doctorWarn || len(check.PermanentlyHeldRecoveries) != 1 {
				t.Fatalf("check = %#v", check)
			}
			diagnostic := check.PermanentlyHeldRecoveries[0]
			if diagnostic.Reason != tt.wantReason || !diagnostic.NeedsHumanAttention || !strings.Contains(diagnostic.Remedy, tt.wantRemedy) {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
		})
	}
}

func TestCheckDoctorBlockedRecoveryUsesConfiguredStates(t *testing.T) {
	t.Parallel()

	prNumber := 428
	recoverable := doctorDependencyIssue("issue-configured", nil)
	recoverable.State = "Parked"
	recoverable.PRNumber = &prNumber
	recoverable.PullRequest = &connector.PullRequest{
		Number:         prNumber,
		State:          "OPEN",
		HeadSHA:        "head-current",
		MergeableState: "dirty",
	}
	recoverable.WorkpadSignal = &workpad.Signal{
		Source:     workpad.SourceStructured,
		Status:     workpad.StatusBlocked,
		ReasonCode: "merge_conflict",
	}
	fake := &fakeDoctorAutoPromoteConnector{
		issues: []connector.Issue{recoverable},
	}
	cfg := validDoctorDependencyWorkflow(true)
	cfg.Tracker.BlockedRecovery.Enabled = true
	cfg.Tracker.BlockedRecovery.SourceStates = []string{"Parked"}
	cfg.Tracker.BlockedRecovery.TargetState = "Repair"

	got := checkDoctorBlockedRecovery(context.Background(), "alpha", cfg, "", doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return fake, nil
		},
	})

	if got.Status != doctorWarn {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorWarn, got)
	}
	for _, want := range []string{"issue-configured", "Repair"} {
		if !strings.Contains(got.Detail, want) && !strings.Contains(got.Hint, want) {
			t.Fatalf("check missing %q:\nDetail: %s\nHint: %s", want, got.Detail, got.Hint)
		}
	}
	if !stringSliceContains(fake.verifyStates, "Parked") || !stringSliceContains(fake.verifyStates, "Repair") {
		t.Fatalf("VerifyStatusOptions states = %#v, want Parked and Repair", fake.verifyStates)
	}
}

func TestCheckDoctorBlockedRecoveryWarnsOnlyWithoutCurrentPredicate(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = db.Close()
	})
	if _, err := db.ExecContext(t.Context(), `
CREATE TABLE workflow_phase_events (
  id INTEGER PRIMARY KEY,
  issue_id TEXT,
  identifier TEXT,
  issue_url TEXT,
  phase_type TEXT,
  phase_name TEXT,
  status TEXT,
  started_at TEXT,
  metadata_json TEXT
)`); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	currentAt := time.Date(2026, 7, 29, 19, 0, 0, 0, time.UTC)
	current := doctorDependencyIssue("issue-current-predicate", nil)
	current.StageUpdatedAt = &currentAt
	stale := doctorDependencyIssue("issue-stale-predicate", nil)
	staleAt := currentAt.Add(time.Hour)
	stale.StageUpdatedAt = &staleAt
	metadata := `{"blocked_recovery":{"owner":"orchestrator","cause":"no_progress_limit","predicate":"fingerprint_changed","cause_fingerprint":"fingerprint"}}`
	for _, row := range []struct {
		issue connector.Issue
		at    time.Time
	}{
		{issue: current, at: currentAt},
		{issue: stale, at: currentAt},
	} {
		if _, err := db.ExecContext(t.Context(),
			`INSERT INTO workflow_phase_events (issue_id, identifier, issue_url, phase_type, phase_name, status, started_at, metadata_json) VALUES (?, ?, ?, 'lane', 'Blocked', 'entered', ?, ?)`,
			row.issue.ID,
			row.issue.Identifier,
			row.issue.URL,
			row.at.Format(time.RFC3339Nano),
			metadata,
		); err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	cfg := validDoctorDependencyWorkflow(true)
	cfg.Tracker.BlockedRecovery.Enabled = false
	check := checkDoctorBlockedRecoveryLive(
		t.Context(),
		"Project alpha blocked recovery",
		&fakeDoctorAutoPromoteConnector{issues: []connector.Issue{current, stale}},
		cfg,
		currentAt.Add(2*time.Hour),
		db,
	)

	if check.Status != doctorWarn || len(check.BlockedWithoutRecovery) != 1 {
		t.Fatalf("check = %#v, want one stale-predicate warning", check)
	}
	if check.BlockedWithoutRecovery[0].IssueID != stale.ID {
		t.Fatalf("BlockedWithoutRecovery = %#v, want %q", check.BlockedWithoutRecovery, stale.ID)
	}
}

func TestCheckDoctorBlockedRecoveryScansEveryBlockedIssue(t *testing.T) {
	t.Parallel()

	issues := make([]connector.Issue, 0, 7)
	for index := range 7 {
		issues = append(issues, doctorDependencyIssue("issue-full-scan-"+strconv.Itoa(index), nil))
	}
	tracker := &fakeDoctorAutoPromoteConnector{issues: issues}
	cfg := validDoctorDependencyWorkflow(true)

	check := checkDoctorBlockedRecoveryLive(
		t.Context(),
		"Project alpha blocked recovery",
		tracker,
		cfg,
		time.Date(2026, 7, 29, 19, 0, 0, 0, time.UTC),
		nil,
	)

	if check.Status != doctorWarn || len(check.BlockedWithoutRecovery) != len(issues) {
		t.Fatalf("check = %#v, want all relation-less issues reported", check)
	}
	if tracker.limit != 0 {
		t.Fatalf("FetchIssuesByStatesLimit limit = %d, want full scan", tracker.limit)
	}
}

func TestCheckDoctorLabelStatusDriftReportsUntrackedAndTerminalIssues(t *testing.T) {
	t.Parallel()

	cfg := validDoctorWorkflow("/repo")
	cfg.Tracker.Kind = workflowconfig.TrackerGitHub
	cfg.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceLabel
	cfg.Tracker.Repository = "digitaldrywood/detent"
	cfg.Tracker.StatusLabelPrefix = "detent:"
	fake := &fakeDoctorAutoPromoteConnector{
		drift: connector.StatusDrift{
			UntrackedOpen: []connector.Issue{{
				ID:         "I_771",
				Identifier: "digitaldrywood/detent#771",
				Title:      "Untracked issue",
				URL:        "https://github.com/digitaldrywood/detent/issues/771",
				Labels:     []string{"bug"},
			}},
			OpenTerminal: []connector.Issue{{
				ID:         "I_583",
				Identifier: "digitaldrywood/detent#583",
				Title:      "Done but open",
				State:      "Done",
				URL:        "https://github.com/digitaldrywood/detent/issues/583",
				Labels:     []string{"detent:done"},
			}},
			ClosedActive: []connector.Issue{{
				ID:         "I_584",
				Identifier: "digitaldrywood/detent#584",
				Title:      "Closed but active",
				State:      "In Progress",
				Closed:     true,
				URL:        "https://github.com/digitaldrywood/detent/issues/584",
				Labels:     []string{"detent:in-progress"},
			}},
		},
	}

	got := checkDoctorLabelStatusDrift(context.Background(), "alpha", cfg, doctorDeps{
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return fake, nil
		},
	})

	if got.Status != doctorWarn {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorWarn, got)
	}
	for _, want := range []string{
		"1 open issue(s) without configured status label",
		"digitaldrywood/detent#771",
		"https://github.com/digitaldrywood/detent/issues/771",
		"1 open issue(s) with terminal status label",
		"digitaldrywood/detent#583",
		"https://github.com/digitaldrywood/detent/issues/583",
		"1 closed issue(s) with active status label",
		"digitaldrywood/detent#584",
		"https://github.com/digitaldrywood/detent/issues/584",
	} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
		}
	}
	if len(got.UntrackedIssues) != 1 || got.UntrackedIssues[0].IssueIdentifier != "digitaldrywood/detent#771" {
		t.Fatalf("UntrackedIssues = %#v, want #771 diagnostic", got.UntrackedIssues)
	}
	if len(got.OpenTerminalIssues) != 1 || got.OpenTerminalIssues[0].State != "Done" {
		t.Fatalf("OpenTerminalIssues = %#v, want Done diagnostic", got.OpenTerminalIssues)
	}
	if len(got.ClosedActiveIssues) != 1 || got.ClosedActiveIssues[0].State != "In Progress" {
		t.Fatalf("ClosedActiveIssues = %#v, want In Progress diagnostic", got.ClosedActiveIssues)
	}
}

func TestDoctorWorkflowDetailSurfacesIdentityAndAuthorization(t *testing.T) {
	t.Parallel()

	cfg := validDoctorWorkflow("/repo")
	cfg.Identity = workflowconfig.Identity{
		Name:          "release-captain",
		GitHubLogin:   "detent-bot",
		OwnershipMode: workflowconfig.IdentityOwnershipField,
		OwnerField:    "Owner",
	}
	cfg.Tracker.Authorization = selector.Selector{
		AssigneeIn: []string{"@me"},
	}
	project := globalconfig.Project{
		Authorization: selector.Selector{
			Labels: selector.Labels{Include: []string{"release"}},
		},
	}

	got := doctorWorkflowDetail("WORKFLOW.md", project, cfg)
	for _, want := range []string{
		"WORKFLOW.md is valid",
		"identity release-captain",
		"worker-model=provider-default",
		"session-guard=max_turns=20, max_turn_duration_ms=disabled, max_session_duration_ms=7200000, no_progress_timeout_ms=5400000, merge_worker_max_duration_ms=21600000, max_session_tokens=disabled, max_session_context_multiplier=disabled",
		"billing-mode=metered",
		"orphan-recovery=resume_orphaned_sessions=true, experimental_thread_resume=true",
		"prioritize-unblockers=true",
		"authorization selectors from global.yaml and WORKFLOW.md",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctorWorkflowDetail() = %q, want substring %q", got, want)
		}
	}
}

func TestDoctorWorkflowDetailReportsPinnedModelAndSessionGuard(t *testing.T) {
	t.Parallel()

	cfg := validDoctorWorkflow("/repo")
	cfg.Agents.Routes = []workflowconfig.AgentRoute{{
		Name:    "default",
		Backend: workflowconfig.DefaultAgentBackendID,
		Model:   "gpt-5.5",
		Default: true,
	}}
	cfg.Agent.MaxSessionTokens = 2_000_000
	cfg.Agent.MaxSessionContextMultiplier = 4
	cfg.Agent.MaxTurnDurationMS = 900000
	cfg.Agent.MaxSessionDurationMS = 3600000
	cfg.Agent.NoProgressTimeoutMS = 1800000
	cfg.Agent.MergeWorkerMaxDurationMS = 7200000

	got := doctorWorkflowDetail("WORKFLOW.md", globalconfig.Project{}, cfg)
	for _, want := range []string{
		"worker-model=pinned gpt-5.5 via agents.routes.model",
		"session-guard=max_turns=20, max_turn_duration_ms=900000, max_session_duration_ms=3600000, no_progress_timeout_ms=1800000, merge_worker_max_duration_ms=7200000, max_session_tokens=2000000, max_session_context_multiplier=4",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctorWorkflowDetail() = %q, want substring %q", got, want)
		}
	}
}

func TestCheckDoctorLocalWorkflowOverlay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		overlay    workflowconfig.WorkflowOverlay
		tracked    bool
		trackedErr error
		wantOK     bool
		wantStatus doctorStatus
		wantDetail []string
		wantHint   string
	}{
		{
			name: "active untracked overlay",
			overlay: workflowconfig.WorkflowOverlay{
				Path:           "/repo/WORKFLOW.local.md",
				OverriddenKeys: []string{"polling.interval_ms", "tracker.assignee"},
			},
			wantOK:     true,
			wantStatus: doctorOK,
			wantDetail: []string{"is active", "polling.interval_ms, tracker.assignee", "not tracked by Git"},
		},
		{
			name: "tracked overlay warns",
			overlay: workflowconfig.WorkflowOverlay{
				Path:           "/repo/WORKFLOW.local.md",
				OverriddenKeys: []string{"tracker.assignee"},
			},
			tracked:    true,
			wantOK:     true,
			wantStatus: doctorWarn,
			wantDetail: []string{"is active", "tracker.assignee", "tracked by Git"},
			wantHint:   "Add WORKFLOW.local.md to .gitignore",
		},
		{
			name: "prose-only overlay",
			overlay: workflowconfig.WorkflowOverlay{
				Path: "/repo/WORKFLOW.local.md",
			},
			wantOK:     true,
			wantStatus: doctorOK,
			wantDetail: []string{"is active", "prose only"},
		},
		{
			name:       "tracking lookup failure warns",
			overlay:    workflowconfig.WorkflowOverlay{Path: "/repo/WORKFLOW.local.md"},
			trackedErr: errors.New("git unavailable"),
			wantOK:     true,
			wantStatus: doctorWarn,
			wantDetail: []string{"could not be determined", "git unavailable"},
			wantHint:   "listed in .gitignore",
		},
		{name: "inactive overlay"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			check, ok := checkDoctorLocalWorkflowOverlay(context.Background(), "detent", workflowconfig.Workflow{Overlay: tt.overlay}, doctorDeps{
				gitTracked: func(context.Context, string) (bool, error) {
					return tt.tracked, tt.trackedErr
				},
			})
			if ok != tt.wantOK {
				t.Fatalf("ok = %t, want %t", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if check.Status != tt.wantStatus {
				t.Fatalf("Status = %s, want %s", check.Status, tt.wantStatus)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(check.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", check.Detail, want)
				}
			}
			if tt.wantHint != "" && !strings.Contains(check.Hint, tt.wantHint) {
				t.Fatalf("Hint = %q, want containing %q", check.Hint, tt.wantHint)
			}
		})
	}
}

func TestDefaultGitTracked(t *testing.T) {
	t.Parallel()

	repo := t.TempDir()
	if err := runDoctorCommandInDir(context.Background(), ".", nil, "git", "-C", repo, "init"); err != nil {
		t.Fatalf("git init error = %v", err)
	}
	path := filepath.Join(repo, "WORKFLOW.local.md")
	if err := os.WriteFile(path, []byte("---\n---\nlocal\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	tracked, err := defaultGitTracked(context.Background(), path)
	if err != nil {
		t.Fatalf("defaultGitTracked() untracked error = %v", err)
	}
	if tracked {
		t.Fatal("defaultGitTracked() = true, want false before git add")
	}

	if err := runDoctorCommandInDir(context.Background(), ".", nil, "git", "-C", repo, "add", "WORKFLOW.local.md"); err != nil {
		t.Fatalf("git add error = %v", err)
	}
	tracked, err = defaultGitTracked(context.Background(), path)
	if err != nil {
		t.Fatalf("defaultGitTracked() tracked error = %v", err)
	}
	if !tracked {
		t.Fatal("defaultGitTracked() = false, want true after git add")
	}
}

func TestCheckDoctorInstanceIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identity   globalconfig.Identity
		want       doctorStatus
		wantDetail string
	}{
		{
			name:       "omitted identity is valid",
			want:       doctorOK,
			wantDetail: "not configured",
		},
		{
			name: "configured identity",
			identity: globalconfig.Identity{
				Name:          "release-captain",
				GitHubLogin:   "detent-bot",
				OwnershipMode: "field",
				OwnerField:    "Owner",
			},
			want:       doctorOK,
			wantDetail: "release-captain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := checkDoctorInstanceIdentity(globalconfig.Config{
				Global: globalconfig.Settings{Identity: tt.identity},
			})
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s", got.Status, tt.want)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
		})
	}
}

func TestCheckDoctorProjectsExpandsSourceRootBeforeGit(t *testing.T) {
	t.Parallel()

	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("UserHomeDir() error = %v", err)
	}

	workflow := validDoctorWorkflow("~/repo")
	var gotPath string
	checks := checkDoctorProjects(context.Background(), globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "alpha", Workflow: "WORKFLOW.md"}},
	}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		gitWorkTree: func(_ context.Context, path string) error {
			gotPath = path
			return nil
		},
	}, RuntimeSecret{}, false)

	wantPath := filepath.Join(home, "repo")
	if gotPath != wantPath {
		t.Fatalf("git path = %q, want %q", gotPath, wantPath)
	}
	var sourceRepo doctorCheck
	for _, check := range checks {
		if check.Name == "Project alpha source repo" {
			sourceRepo = check
			break
		}
	}
	if sourceRepo.Status != doctorOK {
		t.Fatalf("checks = %#v, want source repo OK", checks)
	}
}

func TestRunDoctorUsesReadOnlyWriteChecksByDefaultForExistingConfiguredProject(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceLabel
	workflow.Tracker.Repository = "digitaldrywood/detent"
	workflow.Tracker.StatusLabelPrefix = "detent:"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflow.Tracker.ObservedStates = []string{"Human Review", "Blocked"}
	workflow.Tracker.TerminalStates = []string{"Done"}
	workflow.Tracker.WriteProbeIssue = "digitaldrywood/detent#1"
	workflow.Agent.AutoPromote.NoProgressLimit = 0
	workflow.Server.Kanban.Mode = workflowconfig.KanbanModeIntegration

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{{
			ID:       "existing",
			Workflow: "WORKFLOW.md",
			Workdir:  "/repo",
			Weight:   1,
		}},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(string) (workflowconfig.Workflow, error) {
		return workflowconfig.Workflow{Config: workflow}, nil
	}
	deps.gitRemoteURL = func(context.Context, string) (string, error) {
		return "https://github.com/digitaldrywood/detent.git", nil
	}
	deps.autoPromoteConnector = func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
		return &fakeDoctorAutoPromoteConnector{}, nil
	}
	var gotReadiness ghconnector.ReadinessConfig
	deps.githubReadiness = func(_ context.Context, _ ghconnector.Config, readiness ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
		gotReadiness = readiness
		return []ghconnector.ReadinessCheck{{
			Name:   "GitHub issue write permission digitaldrywood/detent",
			Status: ghconnector.ReadinessOK,
			Detail: "repository permission push permits issue writes",
		}}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Output:       io.Discard,
		CheckTimeout: time.Second,
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), deps)

	if !doctorGitHubReadinessRequiresWrites(gotReadiness) {
		t.Fatalf("readiness write requirements = %#v, want default doctor to retain read-only write checks", gotReadiness)
	}
	if gotReadiness.AllowWriteProbes {
		t.Fatalf("AllowWriteProbes = true, want default doctor to avoid mutation probes")
	}
	assertDoctorCheck(t, report, "Project existing GitHub issue write permission digitaldrywood/detent", doctorOK, "repository permission push")
	for _, check := range report.Checks {
		if check.Name == "Project existing GitHub write probes" {
			t.Fatalf("checks include legacy write-probe warning: %#v", check)
		}
	}
}

func TestRunDoctorWithProjectScopeSkipsUnrelatedProjectFailures(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{
			{ID: "alpha", Workflow: "alpha/WORKFLOW.md", Workdir: "/alpha", Weight: 1},
			{ID: "beta", Workflow: "beta/WORKFLOW.md", Workdir: "/beta", Weight: 1},
		},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(path string) (workflowconfig.Workflow, error) {
		if strings.Contains(path, "beta") {
			return workflowconfig.Workflow{}, errors.New("beta workflow is broken")
		}
		return workflowconfig.Workflow{Config: validDoctorWorkflow("/alpha")}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:       configPath,
		Output:           io.Discard,
		CheckTimeout:     time.Second,
		ProjectID:        "alpha",
		AllowWriteProbes: false,
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), deps)

	if report.HasFailures() {
		t.Fatalf("HasFailures() = true, want scoped doctor to ignore beta failures: %#v", report.Checks)
	}
	if report.Scope.SelectedProject != "alpha" {
		t.Fatalf("SelectedProject = %q, want alpha", report.Scope.SelectedProject)
	}
	if !slices.Equal(report.Scope.SkippedProjects, []string{"beta"}) {
		t.Fatalf("SkippedProjects = %#v, want beta", report.Scope.SkippedProjects)
	}
	assertDoctorCheck(t, report, "Project alpha workflow", doctorOK, "is valid")
	assertDoctorMissingCheck(t, report, "Project beta workflow")

	output := newDoctorOutputReport(report)
	if output.Scope.SelectedProject != "alpha" || !slices.Equal(output.Scope.SkippedProjects, []string{"beta"}) {
		t.Fatalf("JSON scope = %#v, want selected alpha and skipped beta", output.Scope)
	}
}

func TestRunDoctorWithProjectScopeSkipsUnrelatedProjectPathValidation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	alphaWorkdir := filepath.Join(root, "alpha")
	if err := os.MkdirAll(alphaWorkdir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	alphaWorkflow := filepath.Join(alphaWorkdir, "WORKFLOW.md")
	if err := os.WriteFile(alphaWorkflow, []byte("Alpha workflow.\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	configPath := filepath.Join(root, "global.yaml")
	raw := strings.Join([]string{
		"apiVersion: detent/v1",
		"kind: GlobalConfig",
		"global:",
		"  max_concurrent_agents: 1",
		"  scheduling: weighted",
		"projects:",
		"  - id: alpha",
		"    workflow: " + alphaWorkflow,
		"    workdir: " + alphaWorkdir,
		"    weight: 1",
		"    priority: 0",
		"  - id: beta",
		"    workflow: " + filepath.Join(root, "missing-beta", "WORKFLOW.md"),
		"    workdir: " + filepath.Join(root, "missing-beta"),
		"    weight: 1",
		"    priority: 0",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(path string) (workflowconfig.Workflow, error) {
		if strings.Contains(path, "missing-beta") {
			t.Fatalf("loadWorkflow(%q) called for skipped project", path)
		}
		return workflowconfig.Workflow{Config: validDoctorWorkflow(alphaWorkdir)}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Output:       io.Discard,
		CheckTimeout: time.Second,
		ProjectID:    "alpha",
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, options{
		resolvePath: func(string) (globalconfig.PathResolution, error) {
			return globalconfig.PathResolution{Path: configPath, Rule: globalconfig.PathRuleFlag}, nil
		},
		stdoutTTY: func() bool { return true },
	}, deps)

	if report.HasFailures() {
		t.Fatalf("HasFailures() = true, want scoped doctor to ignore beta path validation: %#v", report.Checks)
	}
	if report.Scope.SelectedProject != "alpha" {
		t.Fatalf("SelectedProject = %q, want alpha", report.Scope.SelectedProject)
	}
	if !slices.Equal(report.Scope.SkippedProjects, []string{"beta"}) {
		t.Fatalf("SkippedProjects = %#v, want beta", report.Scope.SkippedProjects)
	}
	assertDoctorMissingCheck(t, report, "Project beta workflow")
}

func TestRunDoctorWithMissingProjectScopeFails(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{
			{ID: "alpha", Workflow: "alpha/WORKFLOW.md", Workdir: "/alpha", Weight: 1},
			{ID: "beta", Workflow: "beta/WORKFLOW.md", Workdir: "/beta", Weight: 1},
		},
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Output:       io.Discard,
		CheckTimeout: time.Second,
		ProjectID:    "missing",
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), successfulDoctorDeps())

	if !report.HasFailures() {
		t.Fatalf("HasFailures() = false, want missing project to fail: %#v", report.Checks)
	}
	if report.Scope.SelectedProject != "missing" {
		t.Fatalf("SelectedProject = %q, want missing", report.Scope.SelectedProject)
	}
	if !slices.Equal(report.Scope.SkippedProjects, []string{"alpha", "beta"}) {
		t.Fatalf("SkippedProjects = %#v, want alpha and beta", report.Scope.SkippedProjects)
	}
	assertDoctorCheck(t, report, "Project scope", doctorFail, "project missing not found")
	assertDoctorMissingCheck(t, report, "Project alpha workflow")
	assertDoctorMissingCheck(t, report, "Project beta workflow")
}

func TestRunDoctorHostWideIncludesAllProjectFailures(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{
			{ID: "alpha", Workflow: "alpha/WORKFLOW.md", Workdir: "/alpha", Weight: 1},
			{ID: "beta", Workflow: "beta/WORKFLOW.md", Workdir: "/beta", Weight: 1},
		},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(path string) (workflowconfig.Workflow, error) {
		if strings.Contains(path, "beta") {
			return workflowconfig.Workflow{}, errors.New("beta workflow is broken")
		}
		return workflowconfig.Workflow{Config: validDoctorWorkflow("/alpha")}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Output:       io.Discard,
		CheckTimeout: time.Second,
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), deps)

	if !report.HasFailures() {
		t.Fatalf("HasFailures() = false, want host-wide doctor to include beta failure: %#v", report.Checks)
	}
	if report.Scope.SelectedProject != "" || len(report.Scope.SkippedProjects) != 0 {
		t.Fatalf("Scope = %#v, want empty host-wide scope", report.Scope)
	}
	assertDoctorCheck(t, report, "Project alpha workflow", doctorOK, "is valid")
	assertDoctorCheck(t, report, "Project beta workflow", doctorFail, "beta workflow is broken")
}

func TestDoctorReportFailurePredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		report     doctorReport
		want       bool
		wantStrict bool
	}{
		{
			name: "ok checks pass",
			report: doctorReport{Checks: []doctorCheck{{
				Name:   "Config resolution",
				Status: doctorOK,
			}}},
		},
		{
			name: "warnings are advisory by default",
			report: doctorReport{Checks: []doctorCheck{{
				Name:   "Workflow optimization",
				Status: doctorWarn,
			}}},
			wantStrict: true,
		},
		{
			name: "workflow findings are advisory by default",
			report: doctorReport{
				WorkflowOptimization: doctorWorkflowOptimizationReport{
					Findings: []doctorWorkflowOptimizationFinding{{
						RuleID: "advisory",
					}},
				},
			},
			wantStrict: true,
		},
		{
			name: "fail checks fail by default",
			report: doctorReport{Checks: []doctorCheck{{
				Name:   "SQLite database",
				Status: doctorFail,
			}}},
			want:       true,
			wantStrict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.report.HasFailures(); got != tt.want {
				t.Fatalf("HasFailures() = %v, want %v", got, tt.want)
			}
			if got := tt.report.HasStrictFailures(); got != tt.wantStrict {
				t.Fatalf("HasStrictFailures() = %v, want %v", got, tt.wantStrict)
			}
		})
	}
}

func TestDoctorCommandProjectFlagScopesJSONReport(t *testing.T) {
	t.Setenv("DETENT_FORMAT", "json")

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{
			{ID: "alpha", Workflow: "alpha/WORKFLOW.md", Workdir: "/alpha", Weight: 1},
			{ID: "beta", Workflow: "beta/WORKFLOW.md", Workdir: "/beta", Weight: 1},
		},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(path string) (workflowconfig.Workflow, error) {
		if strings.Contains(path, "beta") {
			return workflowconfig.Workflow{}, errors.New("beta workflow is broken")
		}
		return workflowconfig.Workflow{Config: validDoctorWorkflow("/alpha")}, nil
	}
	envFlag := ""
	logLevelFlag := ""
	hostFlag := "127.0.0.1"
	portFlag := 0
	cmd := newDoctorCommandWithDeps(&configPath, &envFlag, &logLevelFlag, &hostFlag, &portFlag, successfulDoctorOptionsWithConfig(configPath, global), deps)
	cmd.SetArgs([]string{"--project", "alpha"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, stdout.String())
	}

	var got struct {
		Checks []struct {
			Name string `json:"name"`
		} `json:"checks"`
		Scope doctorScope `json:"scope"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("Unmarshal() error = %v\n%s", err, stdout.String())
	}
	if got.Scope.SelectedProject != "alpha" || !slices.Equal(got.Scope.SkippedProjects, []string{"beta"}) {
		t.Fatalf("scope = %#v, want selected alpha and skipped beta", got.Scope)
	}
	for _, check := range got.Checks {
		if strings.Contains(check.Name, "beta") {
			t.Fatalf("check %q contains skipped project beta in JSON output:\n%s", check.Name, stdout.String())
		}
	}
}

func TestDoctorCommandAllowWriteProbesFlagEnablesWriteReadiness(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceLabel
	workflow.Tracker.Repository = "digitaldrywood/detent"
	workflow.Tracker.StatusLabelPrefix = "detent:"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflow.Tracker.WriteProbeIssue = "digitaldrywood/detent#1"
	workflow.Server.Kanban.Mode = workflowconfig.KanbanModeIntegration

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{{
			ID:       "existing",
			Workflow: "WORKFLOW.md",
			Workdir:  "/repo",
			Weight:   1,
		}},
	}
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(string) (workflowconfig.Workflow, error) {
		return workflowconfig.Workflow{Config: workflow}, nil
	}
	deps.gitRemoteURL = func(context.Context, string) (string, error) {
		return "https://github.com/digitaldrywood/detent.git", nil
	}
	deps.autoPromoteConnector = func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
		return &fakeDoctorAutoPromoteConnector{}, nil
	}
	var gotReadiness ghconnector.ReadinessConfig
	deps.githubReadiness = func(_ context.Context, _ ghconnector.Config, readiness ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
		gotReadiness = readiness
		return []ghconnector.ReadinessCheck{{
			Name:   "GitHub status label update",
			Status: ghconnector.ReadinessOK,
			Detail: "write probe requested",
		}}, nil
	}

	configFlag := configPath
	envFlag := ""
	logLevelFlag := ""
	hostFlag := ""
	portFlag := 0
	cmd := newDoctorCommandWithDeps(&configFlag, &envFlag, &logLevelFlag, &hostFlag, &portFlag, successfulDoctorOptionsWithConfig(configPath, global), deps)
	cmd.SetArgs([]string{"--allow-write-probes"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, stdout.String())
	}
	if !gotReadiness.RequireLabelStatusWrite || !gotReadiness.RequireIssueComments {
		t.Fatalf("readiness write requirements = %#v, want write probes enabled by flag", gotReadiness)
	}
	if !gotReadiness.AllowWriteProbes {
		t.Fatalf("AllowWriteProbes = false, want write probes enabled by flag")
	}
}

func TestRunDoctorSuppressesConnectorLogsFromProgress(t *testing.T) {
	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.APIKey = "token"
	workflow.Tracker.ProjectSlug = "PVT_1"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflow.Tracker.ObservedStates = []string{"Human Review", "Blocked"}
	workflow.Tracker.TerminalStates = []string{"Done", "Cancelled"}

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	global := validDoctorGlobalWithProjects(configPath, "alpha")
	deps := successfulDoctorDeps()
	deps.loadWorkflow = func(string) (workflowconfig.Workflow, error) {
		return workflowconfig.Workflow{Config: workflow}, nil
	}
	deps.autoPromoteConnector = func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
		return &fakeDoctorAutoPromoteConnector{}, nil
	}

	var progress bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&progress, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	deps.githubReadiness = func(_ context.Context, connectorCfg ghconnector.Config, _ ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
		logger := connectorCfg.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn(
			"github rest budget preserved",
			"method", "GET",
			"path", "/repos/digitaldrywood/detent/pulls?state=all",
			"endpoint_family", "pull requests",
		)
		return []ghconnector.ReadinessCheck{{
			Name:   "GitHub readiness",
			Status: ghconnector.ReadinessOK,
			Detail: "ready",
		}}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Output:       &progress,
		CheckTimeout: time.Second,
		Flags: runtimeFlags{
			Port: runtimeIntFlag{Value: 0, Set: true},
		},
	}, successfulDoctorOptionsWithConfig(configPath, global), deps)

	assertDoctorCheck(t, report, "Project alpha GitHub readiness", doctorOK, "ready")
	got := progress.String()
	if strings.Contains(got, "github rest budget preserved") {
		t.Fatalf("doctor progress contains connector log record:\n%s", got)
	}
	if !strings.Contains(got, "RUN    Project alpha checks") || !strings.Contains(got, "OK     Project alpha GitHub readiness") {
		t.Fatalf("doctor progress missing expected check lines:\n%s", got)
	}
}

func TestCheckDoctorProjectBuildsGitHubReadinessInventory(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.ProjectSlug = "PVT_1"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress", "Merging"}
	workflow.Tracker.ObservedStates = []string{"Backlog", "Human Review", "Blocked"}
	workflow.Tracker.TerminalStates = []string{"Done", "Cancelled"}
	workflow.Tracker.WriteProbeIssue = "digitaldrywood/detent#1"
	workflow.Tracker.Claims = workflowconfig.Claims{
		Enabled:          true,
		LeaseField:       "Detent Lease",
		TTLSeconds:       300,
		HeartbeatSeconds: 60,
	}
	workflow.Identity = workflowconfig.Identity{
		Name:          "release-captain",
		OwnershipMode: workflowconfig.IdentityOwnershipField,
		OwnerField:    "Owner",
	}
	var gotConnector ghconnector.Config
	var gotReadiness ghconnector.ReadinessConfig

	checks := checkDoctorProject(context.Background(), globalconfig.Project{
		ID:       "alpha",
		Workflow: "WORKFLOW.md",
	}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		gitWorkTree: func(context.Context, string) error {
			return nil
		},
		gitWorktrees: func(context.Context, string) ([]doctorGitWorktree, error) {
			return nil, nil
		},
		gitRemoteURL: func(context.Context, string) (string, error) {
			return "git@github.com:digitaldrywood/detent.git", nil
		},
		githubReadiness: func(_ context.Context, connectorCfg ghconnector.Config, readinessCfg ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
			gotConnector = connectorCfg
			gotReadiness = readinessCfg
			return []ghconnector.ReadinessCheck{{Name: "GitHub auth path", Status: ghconnector.ReadinessOK, Detail: readinessCfg.AuthPath}}, nil
		},
	}, RuntimeSecret{Value: "token", Source: "github_token", ResolvedVia: "gh"}, true)

	if gotConnector.APIKey != "token" {
		t.Fatalf("connector APIKey = %q, want injected runtime token", gotConnector.APIKey)
	}
	if gotConnector.ProjectSlug != "PVT_1" {
		t.Fatalf("connector ProjectSlug = %q, want PVT_1", gotConnector.ProjectSlug)
	}
	if gotReadiness.AuthPath != "gh-resolved token" {
		t.Fatalf("AuthPath = %q, want gh-resolved token", gotReadiness.AuthPath)
	}
	if gotReadiness.WriteProbeIssue != "digitaldrywood/detent#1" {
		t.Fatalf("WriteProbeIssue = %q, want configured probe", gotReadiness.WriteProbeIssue)
	}
	if len(gotReadiness.Repositories) != 1 || gotReadiness.Repositories[0] != "digitaldrywood/detent" {
		t.Fatalf("Repositories = %#v, want digitaldrywood/detent", gotReadiness.Repositories)
	}
	if !gotReadiness.RequireProjectStatusWrite || !gotReadiness.RequireIssueComments || gotReadiness.RequireAssigneeWrite || !gotReadiness.RequireIssueClose {
		t.Fatalf("write/read requirements = %#v", gotReadiness)
	}
	if !gotReadiness.RequireIssueCommentsRead || !gotReadiness.RequireDependencyMetadataRead || !gotReadiness.RequireIssueChildrenRead || !gotReadiness.RequireIssueParentsRead {
		t.Fatalf("issue read requirements = %#v", gotReadiness)
	}
	if !gotReadiness.RequirePullRequestRead || !gotReadiness.RequirePullRequestReviews || !gotReadiness.RequirePullRequestChecks {
		t.Fatalf("pull request read requirements = %#v", gotReadiness)
	}
	if len(gotReadiness.ProjectFieldWrites) != 2 {
		t.Fatalf("ProjectFieldWrites = %#v, want lease and owner fields", gotReadiness.ProjectFieldWrites)
	}
	for _, want := range []string{"Todo", "Human Review", "Done"} {
		if !stringSliceContains(gotReadiness.StatusStates, want) {
			t.Fatalf("StatusStates = %#v, want %q", gotReadiness.StatusStates, want)
		}
	}
	if len(checks) < 3 || checks[len(checks)-1].Status != doctorOK {
		t.Fatalf("checks = %#v, want readiness OK check appended", checks)
	}
}

func TestCheckDoctorProjectBuildsGitHubIssueFieldReadinessInventory(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceIssueField
	workflow.Tracker.ProjectSlug = ""
	workflow.Tracker.Repository = "digitaldrywood/detent"
	workflow.Tracker.StatusField = "Status"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflow.Tracker.ObservedStates = []string{"Backlog", "Human Review", "Blocked"}
	workflow.Tracker.TerminalStates = []string{"Done", "Cancelled"}
	workflow.Tracker.WriteProbeIssue = "digitaldrywood/detent#1"
	var gotConnector ghconnector.Config
	var gotReadiness ghconnector.ReadinessConfig

	checks := checkDoctorProject(context.Background(), globalconfig.Project{
		ID:       "alpha",
		Workflow: "WORKFLOW.md",
	}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		gitWorkTree: func(context.Context, string) error {
			return nil
		},
		gitRemoteURL: func(context.Context, string) (string, error) {
			return "git@github.com:digitaldrywood/detent.git", nil
		},
		githubReadiness: func(_ context.Context, connectorCfg ghconnector.Config, readinessCfg ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
			gotConnector = connectorCfg
			gotReadiness = readinessCfg
			return []ghconnector.ReadinessCheck{{Name: "GitHub issue field access", Status: ghconnector.ReadinessOK, Detail: "read Status"}}, nil
		},
	}, RuntimeSecret{Value: "token", Source: "github_token", ResolvedVia: "gh"}, true)

	if gotConnector.ProjectSlug != "" {
		t.Fatalf("connector ProjectSlug = %q, want empty in issue_field mode", gotConnector.ProjectSlug)
	}
	if gotConnector.Repository != "digitaldrywood/detent" {
		t.Fatalf("connector Repository = %q, want digitaldrywood/detent", gotConnector.Repository)
	}
	if gotConnector.GitHubStatusSource != workflowconfig.GitHubStatusSourceIssueField {
		t.Fatalf("connector GitHubStatusSource = %q, want issue_field", gotConnector.GitHubStatusSource)
	}
	if gotReadiness.RequireProjectRead || gotReadiness.RequireProjectStatusWrite {
		t.Fatalf("project requirements = %#v, want no ProjectV2 requirements in issue_field mode", gotReadiness)
	}
	if !gotReadiness.RequireIssueFieldRead || !gotReadiness.RequireIssueFieldStatusWrite {
		t.Fatalf("issue-field requirements = %#v, want issue-field read and status write", gotReadiness)
	}
	if !gotReadiness.RequireIssueComments {
		t.Fatalf("RequireIssueComments = false, want comment write probe for integration-capable workflow")
	}
	if len(gotReadiness.Repositories) != 1 || gotReadiness.Repositories[0] != "digitaldrywood/detent" {
		t.Fatalf("Repositories = %#v, want digitaldrywood/detent", gotReadiness.Repositories)
	}
	if len(checks) < 3 || checks[len(checks)-1].Status != doctorOK {
		t.Fatalf("checks = %#v, want readiness OK check appended", checks)
	}
}

func TestDoctorGitHubReadinessRequiresKanbanIntegrationWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		mode              string
		statusSource      string
		projectSlug       string
		repository        string
		wantProjectStatus bool
		wantIssueStatus   bool
		wantLabelStatus   bool
		wantIssueComments bool
	}{
		{
			name:         "read only",
			mode:         workflowconfig.KanbanModeReadOnly,
			statusSource: workflowconfig.GitHubStatusSourceProjectV2,
			projectSlug:  "PVT_1",
			repository:   "digitaldrywood/detent",
		},
		{
			name:              "project v2 integration",
			mode:              workflowconfig.KanbanModeIntegration,
			statusSource:      workflowconfig.GitHubStatusSourceProjectV2,
			projectSlug:       "PVT_1",
			repository:        "digitaldrywood/detent",
			wantProjectStatus: true,
			wantIssueComments: true,
		},
		{
			name:              "issue field integration",
			mode:              workflowconfig.KanbanModeIntegration,
			statusSource:      workflowconfig.GitHubStatusSourceIssueField,
			repository:        "digitaldrywood/detent",
			wantIssueStatus:   true,
			wantIssueComments: true,
		},
		{
			name:              "label integration",
			mode:              workflowconfig.KanbanModeIntegration,
			statusSource:      workflowconfig.GitHubStatusSourceLabel,
			repository:        "digitaldrywood/detent",
			wantLabelStatus:   true,
			wantIssueComments: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow := validDoctorWorkflow("/repo")
			workflow.Tracker.Kind = workflowconfig.TrackerGitHub
			workflow.Tracker.GitHubStatusSource = tt.statusSource
			workflow.Tracker.ProjectSlug = tt.projectSlug
			workflow.Tracker.Repository = tt.repository
			workflow.Tracker.ActiveStates = nil
			workflow.Tracker.ObservedStates = []string{"Todo", "In Progress"}
			workflow.Tracker.TerminalStates = nil
			workflow.Server.Kanban.Mode = tt.mode

			got := doctorGitHubReadinessConfig(context.Background(), globalconfig.Project{}, workflow, doctorDeps{}, RuntimeSecret{}, "/repo")
			if got.RequireProjectStatusWrite != tt.wantProjectStatus {
				t.Fatalf("RequireProjectStatusWrite = %v, want %v", got.RequireProjectStatusWrite, tt.wantProjectStatus)
			}
			if got.RequireIssueFieldStatusWrite != tt.wantIssueStatus {
				t.Fatalf("RequireIssueFieldStatusWrite = %v, want %v", got.RequireIssueFieldStatusWrite, tt.wantIssueStatus)
			}
			if got.RequireLabelStatusWrite != tt.wantLabelStatus {
				t.Fatalf("RequireLabelStatusWrite = %v, want %v", got.RequireLabelStatusWrite, tt.wantLabelStatus)
			}
			if got.RequireIssueComments != tt.wantIssueComments {
				t.Fatalf("RequireIssueComments = %v, want %v", got.RequireIssueComments, tt.wantIssueComments)
			}
		})
	}
}

func TestCheckDoctorRuntimeSettingsReportsSources(t *testing.T) {
	t.Parallel()

	got := checkDoctorRuntimeSettings(RuntimeSettings{
		ConfigPath:  RuntimeValue{Value: "/tmp/global.yaml", Source: string(globalconfig.PathRuleFlag)},
		Env:         RuntimeValue{Value: "prod", Source: runtimeSourceDefault},
		LogLevel:    RuntimeValue{Value: "debug", Source: "LOG_LEVEL"},
		Port:        RuntimeIntValue{Value: 4000, Source: runtimeSourceConfig},
		GitHubToken: RuntimeSecret{Value: "secret-token", Source: "github_token", ResolvedVia: "gh"},
	})

	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s", got.Status, doctorOK)
	}
	for _, want := range []string{
		"config_path=/tmp/global.yaml (--config)",
		"env=prod (default)",
		"log_level=debug (LOG_LEVEL)",
		"port=4000 (config)",
		"github_token=resolved via gh",
	} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail missing %q:\n%s", want, got.Detail)
		}
	}
	if strings.Contains(got.Detail, "secret-token") {
		t.Fatalf("Detail leaked token: %s", got.Detail)
	}
}

func TestRunDoctorPreservesHintedRuntimeErrorHint(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	opts := successfulDoctorOptions(configPath)
	opts.read = func(string) (globalconfig.Config, error) {
		return globalconfig.Config{
			Path:       configPath,
			APIVersion: globalconfig.APIVersion,
			Kind:       globalconfig.Kind,
			Global: globalconfig.Settings{
				MaxConcurrentAgents: 1,
				Scheduling:          globalconfig.SchedulingWeighted,
			},
			Projects: []globalconfig.Project{
				{ID: "api", Workflow: "WORKFLOW.md", Workdir: "/repo"},
			},
		}, nil
	}
	deps := successfulDoctorDeps()
	deps.lookupEnv = func(string) string {
		return ""
	}
	deps.loadWorkflow = func(string) (workflowconfig.Workflow, error) {
		cfg := validDoctorWorkflow("/repo")
		cfg.Tracker.Kind = workflowconfig.TrackerGitHub
		return workflowconfig.Workflow{Config: cfg}, nil
	}

	report := runDoctor(context.Background(), doctorConfig{
		ConfigPath:   configPath,
		Host:         "127.0.0.1",
		Output:       io.Discard,
		CheckTimeout: time.Second,
	}, opts, deps)

	for _, check := range report.Checks {
		if check.Name != "Runtime settings" {
			continue
		}
		if check.Hint != githubAuthHint {
			t.Fatalf("Runtime settings hint = %q, want %q", check.Hint, githubAuthHint)
		}
		if strings.Contains(check.Detail, githubAuthHint) {
			t.Fatalf("Runtime settings detail includes hint: %q", check.Detail)
		}
		return
	}
	t.Fatal("Runtime settings check not found")
}

func TestCheckDoctorDetentExecutableReportsRunningBinary(t *testing.T) {
	t.Parallel()

	executablePath := filepath.Join("Users", "corylanou", "go", "bin", "detent")
	got := checkDoctorDetentExecutable(buildinfo.Info{
		Version: "v1.2.3",
		Commit:  "abcdef123456",
		Date:    "2026-06-13T15:35:40Z",
	}, doctorDeps{
		executable: func() (string, error) {
			return executablePath, nil
		},
	})

	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s", got.Status, doctorOK)
	}
	for _, want := range []string{executablePath, "v1.2.3", "abcdef1"} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail missing %q:\n%s", want, got.Detail)
		}
	}
}

func TestCheckDoctorDetentServiceReportsRemoteBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		commit  string
	}{
		{name: "release build", version: "v1.3.0", commit: "abcdef123456"},
		{name: "development build", version: "dev", commit: "123456abcdef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload, err := json.Marshal(map[string]any{
				"status":  "ok",
				"version": tt.version,
				"commit":  tt.commit,
				"mode":    "running",
				"checks": map[string]string{
					"hub": "configured", "store": "configured", "registry": "configured", "connector": "configured",
				},
			})
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			port := 4017
			checks := checkDoctorDetentService(t.Context(), BootConfig{Host: "127.0.0.1", Port: &port}, buildinfo.Info{}, doctorDeps{
				httpDo: func(request *http.Request) (*http.Response, error) {
					if request.URL.String() != "http://127.0.0.1:4017/health" {
						t.Errorf("URL = %q, want configured health endpoint", request.URL)
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload))}, nil
				},
			})

			if len(checks) != 1 {
				t.Fatalf("checks = %#v, want one remote service check", checks)
			}
			got := checks[0]
			if got.Name != "Remote Detent service" || got.Status != doctorOK {
				t.Fatalf("check = %#v, want remote service OK", got)
			}
			for _, want := range []string{"remote service build", tt.version, tt.commit, "http://127.0.0.1:4017/health"} {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func TestCheckDoctorDetentServiceSkipsAbsentService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "connection refused", err: errors.New("connection refused")},
		{name: "request timeout", err: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checks := checkDoctorDetentService(t.Context(), BootConfig{}, buildinfo.Info{}, doctorDeps{
				httpDo: func(*http.Request) (*http.Response, error) {
					return nil, tt.err
				},
			})
			if len(checks) != 0 {
				t.Fatalf("checks = %#v, want absent service omitted", checks)
			}
		})
	}
}

func TestCheckDoctorDetentServiceReportsBuildDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		running    buildinfo.Info
		installed  buildinfo.Info
		wantStatus doctorStatus
		wantDetail string
		wantHint   string
	}{
		{
			name:       "matching build",
			running:    buildinfo.Info{Version: "v1.2.3", Commit: "abcdef123456"},
			installed:  buildinfo.Info{Version: "v1.2.3", Commit: "abcdef123456"},
			wantStatus: doctorOK,
			wantDetail: "matches installed executable build",
		},
		{
			name:       "stale running build",
			running:    buildinfo.Info{Version: "v1.2.2", Commit: "123456abcdef"},
			installed:  buildinfo.Info{Version: "v1.2.3", Commit: "abcdef123456"},
			wantStatus: doctorWarn,
			wantDetail: "differs from installed executable build",
			wantHint:   "detent start --restart",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload, err := json.Marshal(map[string]any{
				"status":  "ok",
				"version": tt.running.Version,
				"commit":  tt.running.Commit,
				"mode":    "running",
				"checks": map[string]string{
					"hub": "configured", "store": "configured", "registry": "configured", "connector": "configured",
				},
			})
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			port := 4017
			checks := checkDoctorDetentService(t.Context(), BootConfig{Host: "127.0.0.1", Port: &port}, tt.installed, doctorDeps{
				httpDo: func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload))}, nil
				},
			})
			if len(checks) != 2 {
				t.Fatalf("checks = %#v, want remote service and build drift checks", checks)
			}
			got := checks[1]
			if got.Name != "Detent build drift" || got.Status != tt.wantStatus {
				t.Fatalf("check = %#v, want status %s", got, tt.wantStatus)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) || !strings.Contains(got.Hint, tt.wantHint) {
				t.Fatalf("check = %#v, want detail %q and hint %q", got, tt.wantDetail, tt.wantHint)
			}
		})
	}
}

func TestCheckDoctorGitHub(t *testing.T) {
	t.Parallel()

	githubWorkflow := validDoctorWorkflow("/repo")
	githubWorkflow.Tracker.Kind = workflowconfig.TrackerGitHub
	githubWorkflow.Tracker.APIKey = "$PROJECT_TOKEN"

	tests := []struct {
		name       string
		cfg        *globalconfig.Config
		token      RuntimeSecret
		scopes     []string
		scopeErr   error
		workflow   workflowconfig.Config
		env        map[string]string
		want       doctorStatus
		wantDetail string
		wantHint   string
	}{
		{
			name:       "missing token",
			want:       doctorFail,
			wantDetail: "GITHUB_TOKEN is not set",
		},
		{
			name:       "scope check fails",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopeErr:   errors.New("unauthorized"),
			want:       doctorFail,
			wantDetail: "scope check failed",
		},
		{
			name:       "missing required scopes",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{"repo"},
			want:       doctorFail,
			wantDetail: "read:org, read:project, project",
			wantHint:   `gh auth refresh -h github.com --scopes "repo,read:org,read:project,project"`,
		},
		{
			name:       "missing write project scope",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{"repo", "read:org", "read:project"},
			want:       doctorFail,
			wantDetail: "missing scope(s): project",
			wantHint:   `gh auth refresh -h github.com --scopes "repo,read:org,read:project,project"`,
		},
		{
			name:       "project scope satisfies normalized read project scope",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{"repo", "read:org", "project"},
			want:       doctorOK,
			wantDetail: "GITHUB_TOKEN has classic PAT scopes",
		},
		{
			name:       "fine grained token has no classic scopes",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{},
			want:       doctorOK,
			wantDetail: "fine-grained or resource-scoped token",
		},
		{
			name: "non github projects skip token scopes",
			cfg: &globalconfig.Config{Projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			}},
			workflow:   validDoctorWorkflow("/repo"),
			want:       doctorWarn,
			wantDetail: "token scope check skipped",
		},
		{
			name: "github app projects skip token scopes",
			cfg: &globalconfig.Config{Projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			}},
			workflow: githubAppWorkflow(),
			env: map[string]string{
				"APP_ID":           "12345",
				"INSTALLATION_ID":  "67890",
				"PRIVATE_KEY_PATH": ".detent/github-app.pem",
			},
			want:       doctorOK,
			wantDetail: "GitHub App credentials configured",
		},
		{
			name: "github app env refs missing require token",
			cfg: &globalconfig.Config{Projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			}},
			workflow:   githubAppWorkflow(),
			want:       doctorFail,
			wantDetail: "GITHUB_TOKEN is not set",
		},
		{
			name: "workflow token has required scopes",
			cfg: &globalconfig.Config{Projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			}},
			token:      RuntimeSecret{Value: "token", Source: "PROJECT_TOKEN"},
			scopes:     []string{"project", "read:project", "read:org", "repo"},
			want:       doctorOK,
			wantDetail: "PROJECT_TOKEN has classic PAT scopes",
		},
		{
			name: "boardless workflow token does not require project scopes",
			cfg: &globalconfig.Config{Projects: []globalconfig.Project{
				{ID: "alpha", Workflow: "WORKFLOW.md"},
			}},
			workflow: func() workflowconfig.Config {
				cfg := githubWorkflow
				cfg.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceIssueField
				cfg.Tracker.ProjectSlug = ""
				cfg.Tracker.Repository = "digitaldrywood/detent"
				return cfg
			}(),
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{"repo", "read:org"},
			want:       doctorOK,
			wantDetail: "GITHUB_TOKEN has classic PAT scopes: repo, read:org",
		},
		{
			name:       "environment token has required scopes",
			token:      RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"},
			scopes:     []string{"workflow", "project", "read:project", "read:org", "repo"},
			want:       doctorOK,
			wantDetail: "GITHUB_TOKEN has classic PAT scopes",
		},
		{
			name:       "gh sentinel token has required scopes",
			token:      RuntimeSecret{Value: "token", Source: "github_token", ResolvedVia: "gh"},
			scopes:     []string{"project", "read:project", "read:org", "repo"},
			want:       doctorOK,
			wantDetail: "github_token resolved via gh has classic PAT scopes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow := githubWorkflow
			if tt.workflow.Tracker.Kind != "" {
				workflow = tt.workflow
			}
			got := checkDoctorGitHub(context.Background(), tt.cfg, tt.token, doctorDeps{
				lookupEnv: mapLookup(tt.env),
				loadWorkflow: func(string) (workflowconfig.Workflow, error) {
					return workflowconfig.Workflow{Config: workflow}, nil
				},
				githubScopes: func(context.Context, string) ([]string, error) {
					return tt.scopes, tt.scopeErr
				},
			})
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s", got.Status, tt.want)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
			if tt.wantHint != "" && !strings.Contains(got.Hint, tt.wantHint) {
				t.Fatalf("Hint = %q, want containing %q", got.Hint, tt.wantHint)
			}
		})
	}
}

func TestCheckDoctorGitHubScopesSkipAppBackedWorkflows(t *testing.T) {
	t.Parallel()

	boardlessWorkflow := validDoctorWorkflow("/repo")
	boardlessWorkflow.Tracker.Kind = workflowconfig.TrackerGitHub
	boardlessWorkflow.Tracker.APIKey = "$GITHUB_TOKEN"
	boardlessWorkflow.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceIssueField
	boardlessWorkflow.Tracker.ProjectSlug = ""
	boardlessWorkflow.Tracker.Repository = "digitaldrywood/detent"

	appWorkflow := githubAppWorkflow()
	workflows := map[string]workflowconfig.Config{
		"boardless.md": boardlessWorkflow,
		"projectv2.md": appWorkflow,
	}
	cfg := &globalconfig.Config{Projects: []globalconfig.Project{
		{ID: "boardless", Workflow: "boardless.md"},
		{ID: "projectv2", Workflow: "projectv2.md"},
	}}

	got := checkDoctorGitHub(context.Background(), cfg, RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"}, doctorDeps{
		lookupEnv: mapLookup(map[string]string{
			"APP_ID":           "12345",
			"INSTALLATION_ID":  "67890",
			"PRIVATE_KEY_PATH": ".detent/github-app.pem",
		}),
		loadWorkflow: func(path string) (workflowconfig.Workflow, error) {
			workflow, ok := workflows[path]
			if !ok {
				t.Fatalf("unexpected workflow path %q", path)
			}
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		githubScopes: func(context.Context, string) ([]string, error) {
			return []string{"repo", "read:org"}, nil
		},
	})
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
	}
	if !strings.Contains(got.Detail, "repo, read:org") {
		t.Fatalf("Detail = %q, want boardless PAT scopes", got.Detail)
	}
	if strings.Contains(got.Detail, "read:project") || strings.Contains(got.Detail, ", project") {
		t.Fatalf("Detail = %q, want no ProjectV2 PAT scopes for app-backed workflow", got.Detail)
	}
}

func TestCheckDoctorGitHubScopesLabelModeRequiresRepoOnly(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHub
	workflow.Tracker.APIKey = "$GITHUB_TOKEN"
	workflow.Tracker.GitHubStatusSource = workflowconfig.GitHubStatusSourceLabel
	workflow.Tracker.ProjectSlug = ""
	workflow.Tracker.Repository = "digitaldrywood/detent"
	cfg := &globalconfig.Config{Projects: []globalconfig.Project{{ID: "detent", Workflow: "workflow.md"}}}

	got := checkDoctorGitHub(context.Background(), cfg, RuntimeSecret{Value: "token", Source: "GITHUB_TOKEN"}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: workflow}, nil
		},
		githubScopes: func(context.Context, string) ([]string, error) {
			return []string{"repo"}, nil
		},
	})
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
	}
	if !strings.Contains(got.Detail, "repo") {
		t.Fatalf("Detail = %q, want repo scope", got.Detail)
	}
	for _, forbidden := range []string{"read:org", "read:project", ", project"} {
		if strings.Contains(got.Detail, forbidden) {
			t.Fatalf("Detail = %q, want no %s scope requirement in label mode", got.Detail, forbidden)
		}
	}
}

func TestExpandDoctorWorkspacePath(t *testing.T) {
	t.Parallel()

	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("UserHomeDir() error = %v", err)
	}

	got, err := expandDoctorWorkspacePath("~/repo")
	if err != nil {
		t.Fatalf("expandDoctorWorkspacePath() error = %v", err)
	}
	want := filepath.Join(home, "repo")
	if got != want {
		t.Fatalf("expandDoctorWorkspacePath() = %q, want %q", got, want)
	}
}

func TestCheckDoctorSQLite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		openErr    error
		closeErr   error
		want       doctorStatus
		wantDetail string
	}{
		{
			name:       "missing config path",
			want:       doctorFail,
			wantDetail: "global config path is unavailable",
		},
		{
			name:       "open fails",
			path:       "/tmp/detent/global.yaml",
			openErr:    errors.New("readonly"),
			want:       doctorFail,
			wantDetail: "readonly",
		},
		{
			name:       "close fails",
			path:       "/tmp/detent/global.yaml",
			closeErr:   errors.New("close failed"),
			want:       doctorFail,
			wantDetail: "close failed",
		},
		{
			name:       "database reachable",
			path:       "/tmp/detent/global.yaml",
			want:       doctorOK,
			wantDetail: "detent.db is reachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := checkDoctorSQLite(context.Background(), globalconfig.PathResolution{Path: tt.path}, doctorDeps{
				openSQLite: func(_ context.Context, path string) (doctorStore, error) {
					if tt.openErr != nil {
						return nil, tt.openErr
					}
					if got := filepath.Base(path); got != "detent.db" {
						t.Fatalf("store path base = %q, want detent.db", got)
					}
					return fakeDoctorStore{closeErr: tt.closeErr}, nil
				},
			})
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s", got.Status, tt.want)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
		})
	}
}

func TestCheckDoctorInstanceLock(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	resolution := globalconfig.PathResolution{Path: filepath.Join(root, "global.yaml")}
	lockPath := filepath.Join(root, "detent.db.lock")

	check := checkDoctorInstanceLock(resolution)
	if check.Status != doctorOK || !strings.Contains(check.Detail, "no active or stale holder") {
		t.Fatalf("missing lock check = %+v, want OK", check)
	}

	if err := os.WriteFile(lockPath, []byte("pid=987654\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	check = checkDoctorInstanceLock(resolution)
	if check.Status != doctorWarn || !strings.Contains(check.Detail, "stale lock:") {
		t.Fatalf("stale lock check = %+v, want explicit stale warning", check)
	}

	lock, err := instancelock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	check = checkDoctorInstanceLock(resolution)
	if check.Status != doctorOK || !strings.Contains(check.Detail, "is held by pid") {
		t.Fatalf("held lock check = %+v, want holder detail", check)
	}
}

func TestCheckDoctorDailyBudgetAccuracy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		schema     string
		seed       string
		want       doctorStatus
		wantDetail string
	}{
		{
			name:       "migration missing",
			schema:     "CREATE TABLE codex_sessions (completed_at TEXT)",
			want:       doctorWarn,
			wantDetail: "migration has not been applied",
		},
		{
			name:       "unattributed session today",
			schema:     "CREATE TABLE codex_sessions (completed_at TEXT, project_id TEXT)",
			seed:       "INSERT INTO codex_sessions (completed_at) VALUES ('2026-07-12T19:00:00Z')",
			want:       doctorWarn,
			wantDetail: "count toward every project",
		},
		{
			name:       "all sessions attributed",
			schema:     "CREATE TABLE codex_sessions (completed_at TEXT, project_id TEXT)",
			seed:       "INSERT INTO codex_sessions (completed_at, project_id) VALUES ('2026-07-12T19:00:00Z', 'detent')",
			want:       doctorOK,
			wantDetail: "all completed sessions today have project attribution",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "detent.db")
			db, err := sql.Open("sqlite", dbPath)
			if err != nil {
				t.Fatalf("sql.Open() error = %v", err)
			}
			if _, err := db.ExecContext(t.Context(), tt.schema); err != nil {
				t.Fatalf("create schema error = %v", err)
			}
			if tt.seed != "" {
				if _, err := db.ExecContext(t.Context(), tt.seed); err != nil {
					t.Fatalf("seed session error = %v", err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("db.Close() error = %v", err)
			}

			got := checkDoctorDailyBudgetAccuracy(context.Background(), globalconfig.PathResolution{Path: filepath.Join(dir, "global.yaml")}, doctorDeps{
				openSQLiteReadOnly: openDoctorSQLiteReadOnly,
			}, now)
			if got.Status != tt.want || !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("check = %#v, want status %s detail containing %q", got, tt.want, tt.wantDetail)
			}
		})
	}
}

func TestDoctorGitHubLocalReadinessUsesLocalStatusMode(t *testing.T) {
	t.Parallel()

	workflow := validDoctorWorkflow("/repo")
	workflow.Tracker.Kind = workflowconfig.TrackerGitHubLocal
	workflow.Tracker.Repository = "digitaldrywood/detent"
	workflow.Tracker.LocalSQLite.Path = ".detent/work-items.db"
	workflow.Tracker.ActiveStates = []string{"Todo", "In Progress"}
	workflow.Tracker.ObservedStates = []string{"Blocked", "Human Review"}
	workflow.Tracker.TerminalStates = []string{"Done"}

	got := doctorGitHubReadinessConfig(context.Background(), globalconfig.Project{Workdir: "/repo"}, workflow, doctorDeps{}, RuntimeSecret{}, "/repo")
	if !got.LocalStatusMode {
		t.Fatalf("LocalStatusMode = false, want true")
	}
	if got.RequireProjectRead || got.RequireProjectStatusWrite || got.RequireIssueFieldStatusWrite || got.RequireLabelStatusWrite || got.RequireIssueComments {
		t.Fatalf("write/status requirements = %#v, want local-only status mode", got)
	}
	if !got.RequireIssueCommentsRead || !got.RequireDependencyMetadataRead || !got.RequirePullRequestRead || !got.RequirePullRequestReviews || !got.RequirePullRequestChecks {
		t.Fatalf("read requirements = %#v, want GitHub issue/PR/check reads", got)
	}
	if len(got.Repositories) != 1 || got.Repositories[0] != "digitaldrywood/detent" {
		t.Fatalf("Repositories = %#v, want digitaldrywood/detent", got.Repositories)
	}
}

func TestCheckDoctorLocalSQLiteTrackerResolvesProjectRelativePath(t *testing.T) {
	t.Parallel()

	workdir := t.TempDir()
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerLocalSQLite
	cfg.Tracker.LocalSQLite.Path = ".detent/work-items.db"

	var opened string
	got := checkDoctorLocalSQLiteTracker(context.Background(), "video", globalconfig.Project{Workdir: workdir}, cfg, doctorDeps{
		openSQLite: func(_ context.Context, path string) (doctorStore, error) {
			opened = path
			return fakeDoctorStore{}, nil
		},
	})
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
	}
	want := filepath.Join(workdir, ".detent", "work-items.db")
	if opened != want {
		t.Fatalf("opened path = %q, want %q", opened, want)
	}
}

func TestCheckDoctorFilesystemWorkspaceValidatesRootAndOutput(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outputRoot := t.TempDir()
	cfg := workflowconfig.Default()
	cfg.Workspace.Kind = workflowconfig.WorkspaceFilesystem
	cfg.Workspace.Root = root
	cfg.Deliverable.OutputRoot = outputRoot

	got := checkDoctorFilesystemWorkspace("video", cfg)
	if got.Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", got.Status, doctorOK, got)
	}
	for _, want := range []string{root, outputRoot, "artifact output"} {
		if !strings.Contains(got.Detail, want) {
			t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
		}
	}
}

func TestDoctorSQLitePingErrorWrapsPingAndCloseErrors(t *testing.T) {
	t.Parallel()

	pingErr := errors.New("ping failed")
	closeErr := errors.New("close failed")

	err := doctorSQLitePingError(pingErr, closeErr)
	if !errors.Is(err, pingErr) {
		t.Fatalf("doctorSQLitePingError() error = %v, want ping error in chain", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("doctorSQLitePingError() error = %v, want close error in chain", err)
	}
}

func TestDoctorWorkflowSpendBreakerDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		billingMode string
		tokenLimit  int64
		spendLimit  float64
		want        string
	}{
		{name: "disabled", want: "progress-breaker=no_progress_token_limit=disabled, no_progress_spend_limit_usd=disabled"},
		{name: "subscription", billingMode: workflowconfig.BillingModeSubscription, tokenLimit: 25_000_000, spendLimit: 7.5, want: "progress-breaker=no_progress_token_limit=25000000, no_progress_spend_limit_usd=7.50 (inert)"},
		{name: "metered", billingMode: workflowconfig.BillingModeMetered, tokenLimit: 25_000_000, spendLimit: 7.5, want: "progress-breaker=no_progress_token_limit=25000000, no_progress_spend_limit_usd=7.50"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := workflowconfig.Config{}
			cfg.Budget.BillingMode = tt.billingMode
			cfg.Agent.NoProgressTokenLimit = tt.tokenLimit
			cfg.Agent.NoProgressSpendLimitUSD = tt.spendLimit
			if got := doctorWorkflowSpendBreakerDetail(cfg); got != tt.want {
				t.Fatalf("doctorWorkflowSpendBreakerDetail() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckDoctorServerPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		listenErr  error
		closeErr   error
		want       doctorStatus
		wantDetail string
	}{
		{
			name:       "port unavailable",
			listenErr:  errors.New("address already in use"),
			want:       doctorFail,
			wantDetail: "address already in use",
		},
		{
			name:       "close fails after bind",
			closeErr:   errors.New("close failed"),
			want:       doctorWarn,
			wantDetail: "close failed",
		},
		{
			name:       "port available",
			want:       doctorOK,
			wantDetail: "available for pre-start bind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port := 0
			got := checkDoctorServerPort(context.Background(), BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{
				listen: func(_, address string) (net.Listener, error) {
					if address != "127.0.0.1:0" {
						t.Fatalf("listen address = %q, want 127.0.0.1:0", address)
					}
					if tt.listenErr != nil {
						return nil, tt.listenErr
					}
					return fakeDoctorListener{addr: fakeDoctorAddr("127.0.0.1:49152"), closeErr: tt.closeErr}, nil
				},
			})
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s", got.Status, tt.want)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Fatalf("Detail = %q, want containing %q", got.Detail, tt.wantDetail)
			}
		})
	}
}

func TestCheckDoctorWorkflowDriftReportsStaleLiveConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	if err := os.WriteFile(workflowPath, []byte("---\ntracker:\n  kind: memory\n---\ncurrent\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	loadedAt := time.Date(2026, 7, 12, 14, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(map[string]any{
		"status": "ok",
		"mode":   "running",
		"checks": map[string]string{
			"hub": "configured", "store": "configured", "registry": "configured", "connector": "configured",
		},
		"workflows": []map[string]any{{
			"project_id":  "detent",
			"path":        workflowPath,
			"source_hash": "stale-hash",
			"loaded_at":   loadedAt,
		}},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	port := 4001
	checks := checkDoctorWorkflowDrift(context.Background(), globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "detent", Workflow: workflowPath}},
	}, BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{
		loadWorkflow: workflowconfig.LoadWorkflow,
		httpDo: func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(payload)),
			}, nil
		},
	})

	if len(checks) != 1 {
		t.Fatalf("checks = %#v, want one", checks)
	}
	if checks[0].Status != doctorFail {
		t.Fatalf("Status = %s, want %s: %#v", checks[0].Status, doctorFail, checks[0])
	}
	for _, want := range []string{
		"project detent is running stale config",
		"file changed at ",
		"loaded at 2026-07-12T14:00:00Z",
	} {
		if !strings.Contains(checks[0].Detail, want) {
			t.Fatalf("Detail = %q, want containing %q", checks[0].Detail, want)
		}
	}
}

func TestCheckDoctorWorkflowDriftReportsLastGoodReloadFailure(t *testing.T) {
	t.Parallel()

	loadedAt := time.Date(2026, 8, 20, 15, 0, 0, 0, time.UTC)
	failedAt := loadedAt.Add(5 * time.Minute)
	tests := []struct {
		name      string
		reloadErr string
		want      string
	}{
		{name: "watch reload validation", reloadErr: "workflow reload validation failed: effort section missing", want: "workflow reload validation failed"},
		{name: "reconcile load", reloadErr: "workflow reconcile failed: invalid detent.yaml", want: "workflow reconcile failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			payload, err := json.Marshal(map[string]any{
				"status": "ok",
				"mode":   "running",
				"checks": map[string]string{
					"hub": "configured", "store": "configured", "registry": "configured", "connector": "configured",
				},
				"workflows": []map[string]any{{
					"project_id":        "detent",
					"path":              "WORKFLOW.md",
					"source_hash":       "last-good-hash",
					"loaded_at":         loadedAt,
					"last_reload_error": tt.reloadErr,
					"reload_failed_at":  failedAt,
				}},
			})
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			port := 4001
			checks := checkDoctorWorkflowDrift(context.Background(), globalconfig.Config{
				Projects: []globalconfig.Project{{ID: "detent", Workflow: "WORKFLOW.md"}},
			}, BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{
				httpDo: func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(payload))}, nil
				},
			})

			if len(checks) != 1 || checks[0].Status != doctorFail {
				t.Fatalf("checks = %#v, want one failure", checks)
			}
			for _, want := range []string{"running its last-good workflow", "loaded at 2026-08-20T15:00:00Z", "reload failed at 2026-08-20T15:05:00Z", tt.want} {
				if !strings.Contains(checks[0].Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", checks[0].Detail, want)
				}
			}
		})
	}
}

func TestCheckDoctorWorkflowDriftDefersPausedProject(t *testing.T) {
	t.Parallel()

	port := 4001
	checks := checkDoctorWorkflowDrift(context.Background(), globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "detent", Workflow: "WORKFLOW.md", Paused: true}},
	}, BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{
		httpDo: func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"status":"ok","mode":"running","checks":{"hub":"configured","store":"configured","registry":"configured","connector":"configured"},"workflows":[{"project_id":"detent","source_hash":"stale-hash"}]}`)),
			}, nil
		},
	})

	if len(checks) != 1 {
		t.Fatalf("checks = %#v, want one", checks)
	}
	if checks[0].Status != doctorOK {
		t.Fatalf("Status = %s, want %s: %#v", checks[0].Status, doctorOK, checks[0])
	}
	if !strings.Contains(checks[0].Detail, "paused; workflow drift comparison deferred until unpause") {
		t.Fatalf("Detail = %q, want paused drift deferral", checks[0].Detail)
	}
}

func TestCheckDoctorServerPortProbesExistingInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		statusCode int
		body       string
		want       doctorStatus
		wantDetail []string
	}{
		{
			name:       "healthy running detent on wildcard host",
			host:       "0.0.0.0",
			statusCode: http.StatusOK,
			body:       `{"status":"ok","mode":"running","checks":{"hub":"configured","store":"configured","registry":"configured","connector":"configured"},"budgets":[{"project_id":"detent","enabled":true,"per_day_max_usd":250,"per_issue_max_usd":25}]}`,
			want:       doctorWarn,
			wantDetail: []string{
				"pre-start bind",
				"healthy Detent instance",
				"http://127.0.0.1:",
				"/health",
				"status ok",
				"mode running",
				"enforced budget: detent enabled=true per_day_max_usd=250.00 per_issue_max_usd=25.00",
			},
		},
		{
			name:       "unhealthy detent service",
			host:       "127.0.0.1",
			statusCode: http.StatusOK,
			body:       `{"status":"error","mode":"running","checks":{"hub":"configured","store":"configured","registry":"configured","connector":"configured"}}`,
			want:       doctorFail,
			wantDetail: []string{
				"pre-start bind",
				"health probe",
				"did not report healthy status",
			},
		},
		{
			name:       "non-detent service",
			host:       "127.0.0.1",
			statusCode: http.StatusOK,
			body:       `{"status":"ok"}`,
			want:       doctorFail,
			wantDetail: []string{
				"pre-start bind",
				"health probe",
				"did not return Detent health",
			},
		},
		{
			name:       "generic health service",
			host:       "127.0.0.1",
			statusCode: http.StatusOK,
			body:       `{"status":"ok","mode":"ready","checks":{}}`,
			want:       doctorFail,
			wantDetail: []string{
				"pre-start bind",
				"health probe",
				"did not return Detent health",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port := occupiedDoctorPort(t, doctorHealthProbeHost(tt.host), tt.statusCode, tt.body)
			probeAddr := net.JoinHostPort(doctorHealthProbeHost(tt.host), strconv.Itoa(port))
			var listenConfig net.ListenConfig
			shadow, err := listenConfig.Listen(t.Context(), "tcp", probeAddr)
			if err == nil {
				if closeErr := shadow.Close(); closeErr != nil {
					t.Errorf("Close() shadow listener error = %v", closeErr)
				}
				t.Fatalf("health fixture does not reserve probe address %s", probeAddr)
			}
			if !doctorListenErrIndicatesOccupied(err) {
				t.Fatalf("Listen(%q) error = %v, want occupied address", probeAddr, err)
			}
			got := checkDoctorServerPort(context.Background(), BootConfig{Host: tt.host, Port: &port}, doctorDeps{
				listen: func(network, address string) (net.Listener, error) {
					wantAddr := net.JoinHostPort(tt.host, strconv.Itoa(port))
					if network != "tcp" || address != wantAddr {
						t.Fatalf("listen(%q, %q), want tcp, %q", network, address, wantAddr)
					}
					return listenConfig.Listen(t.Context(), network, probeAddr)
				},
			})
			if got.Status != tt.want {
				t.Fatalf("Status = %s, want %s: %+v", got.Status, tt.want, got)
			}
			for _, want := range tt.wantDetail {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func TestCheckDoctorServerPortRejectsHealthProbeEOF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantDetail string
	}{
		{
			name: "connection closes before response",
			handler: func(http.ResponseWriter, *http.Request) {
				panic(http.ErrAbortHandler)
			},
			wantDetail: "could not be reached",
		},
		{
			name: "incomplete health response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				if _, err := io.WriteString(w, `{"status":`); err != nil {
					t.Errorf("WriteString() error = %v", err)
				}
			},
			wantDetail: "did not return Detent health",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(tt.handler)
			t.Cleanup(server.Close)
			port := doctorPortFromAddr(t, server.Listener.Addr())
			got := checkDoctorServerPort(t.Context(), BootConfig{Host: "127.0.0.1", Port: &port}, doctorDeps{})
			if got.Status != doctorFail {
				t.Fatalf("Status = %s, want %s: %+v", got.Status, doctorFail, got)
			}
			for _, want := range []string{tt.wantDetail, "EOF"} {
				if !strings.Contains(got.Detail, want) {
					t.Fatalf("Detail = %q, want containing %q", got.Detail, want)
				}
			}
		})
	}
}

func TestDoctorHealthProbeHostMapsWildcardToLoopback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want string
	}{
		{name: "empty uses default", want: "127.0.0.1"},
		{name: "ipv4 wildcard", host: "0.0.0.0", want: "127.0.0.1"},
		{name: "ipv6 wildcard", host: "::", want: "::1"},
		{name: "bracketed ipv6 wildcard", host: "[::]", want: "::1"},
		{name: "loopback unchanged", host: "127.0.0.1", want: "127.0.0.1"},
		{name: "hostname unchanged", host: "dashboard.internal", want: "dashboard.internal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := doctorHealthProbeHost(tt.host); got != tt.want {
				t.Fatalf("doctorHealthProbeHost(%q) = %q, want %q", tt.host, got, tt.want)
			}
		})
	}
}

func TestServerAddrNormalizesBracketedIPv6Host(t *testing.T) {
	t.Parallel()

	port := 4001
	if got := serverAddr(BootConfig{Host: "[::]", Port: &port}); got != "[::]:4001" {
		t.Fatalf("serverAddr() = %q, want [::]:4001", got)
	}
}

func TestDoctorListenErrIndicatesOccupied(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "unix bind collision", err: errors.New("bind: address already in use"), want: true},
		{name: "windows bind collision", err: errors.New("bind: Only one usage of each socket address (protocol/network address/port) is normally permitted."), want: true},
		{name: "other listen error", err: errors.New("bind: permission denied"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := doctorListenErrIndicatesOccupied(tt.err); got != tt.want {
				t.Fatalf("doctorListenErrIndicatesOccupied() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDoctorCommandExitStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		deps       doctorDeps
		wantErr    error
		wantOutput string
	}{
		{
			name:       "passes with warnings only",
			deps:       successfulDoctorDeps(),
			wantOutput: "Result: PASS",
		},
		{
			name:       "strict fails with warnings only",
			args:       []string{"--strict"},
			deps:       successfulDoctorDeps(),
			wantErr:    ErrDoctorFailed,
			wantOutput: "Result: FAIL",
		},
		{
			name: "fails when any check fails",
			deps: doctorDeps{
				runCommandInDir: func(_ context.Context, _ string, _ []string, path string, _ ...string) error {
					if strings.HasSuffix(path, "codex") {
						return errors.New("not runnable")
					}
					return nil
				},
			},
			wantErr:    ErrDoctorFailed,
			wantOutput: "Result: FAIL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "global.yaml")
			env := ""
			logLevel := ""
			host := "127.0.0.1"
			port := 0
			opts := successfulDoctorOptions(configPath)
			deps := successfulDoctorDeps()
			if tt.deps.runCommandInDir != nil {
				deps.runCommandInDir = tt.deps.runCommandInDir
			}
			cmd := newDoctorCommandWithDeps(&configPath, &env, &logLevel, &host, &port, opts, deps)
			cmd.SetArgs(tt.args)
			var stdout bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&bytes.Buffer{})

			err := cmd.Execute()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(stdout.String(), tt.wantOutput) {
				t.Fatalf("output missing %q:\n%s", tt.wantOutput, stdout.String())
			}
		})
	}
}

func TestDoctorCommandStreamsProgressBeforeSlowCheckCompletes(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	env := ""
	logLevel := ""
	host := "127.0.0.1"
	port := 0
	opts := successfulDoctorOptions(configPath)
	deps := successfulDoctorDeps()
	codexStarted := make(chan struct{})
	releaseCodex := make(chan struct{})
	var once sync.Once
	deps.runCommandInDir = func(ctx context.Context, _ string, _ []string, path string, _ ...string) error {
		if strings.HasSuffix(path, "codex") {
			once.Do(func() {
				close(codexStarted)
			})
			select {
			case <-releaseCodex:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	cmd := newDoctorCommandWithDeps(&configPath, &env, &logLevel, &host, &port, opts, deps)
	stdout := &synchronizedBuffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(&bytes.Buffer{})

	errs := make(chan error, 1)
	go func() {
		errs <- cmd.Execute()
	}()

	select {
	case <-codexStarted:
	case <-time.After(doctorTestSafetyTimeout):
		t.Fatal("codex check did not start")
	}
	if got := stdout.String(); !strings.Contains(got, "RUN    codex binary") {
		t.Fatalf("progress output missing codex start before check returned:\n%s", got)
	}
	close(releaseCodex)

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("Execute() error = %v, want nil", err)
		}
	case <-time.After(doctorTestSafetyTimeout):
		t.Fatal("timed out waiting for doctor command")
	}
}

func TestDoctorCommandTimeoutFlagBoundsCheck(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	env := ""
	logLevel := ""
	host := "127.0.0.1"
	port := 0
	opts := successfulDoctorOptions(configPath)
	deps := successfulDoctorDeps()
	releaseCodex := make(chan struct{})
	defer close(releaseCodex)
	deps.runCommandInDir = func(_ context.Context, _ string, _ []string, path string, _ ...string) error {
		if strings.HasSuffix(path, "codex") {
			<-releaseCodex
		}
		return nil
	}

	cmd := newDoctorCommandWithDeps(&configPath, &env, &logLevel, &host, &port, opts, deps)
	cmd.SetArgs([]string{"--timeout", "20ms"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&bytes.Buffer{})

	err := cmd.Execute()
	if !errors.Is(err, ErrDoctorFailed) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrDoctorFailed)
	}
	for _, want := range []string{"FAIL", "codex binary", "timed out after 20ms", "Result: FAIL"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorCommandWritesJSONReport(t *testing.T) {
	t.Setenv("DETENT_FORMAT", "json")

	configPath := filepath.Join(t.TempDir(), "global.yaml")
	env := ""
	logLevel := ""
	host := "127.0.0.1"
	port := 0
	opts := successfulDoctorOptions(configPath)
	deps := successfulDoctorDeps()

	cmd := newDoctorCommandWithDeps(&configPath, &env, &logLevel, &host, &port, opts, deps)
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&bytes.Buffer{})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	var got struct {
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Detail string `json:"detail"`
			Hint   string `json:"hint,omitempty"`
		} `json:"checks"`
		Summary struct {
			OK   int `json:"ok"`
			Warn int `json:"warn"`
			Fail int `json:"fail"`
		} `json:"summary"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("Unmarshal() error = %v\n%s", err, stdout.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		t.Fatalf("Unmarshal() raw error = %v\n%s", err, stdout.String())
	}
	if _, ok := raw["scope"]; ok {
		t.Fatalf("host-wide JSON includes scope, want scope omitted:\n%s", stdout.String())
	}
	if got.Result != "PASS" {
		t.Fatalf("result = %q, want PASS", got.Result)
	}
	if got.Summary.OK == 0 {
		t.Fatalf("summary.ok = 0, want successful checks\n%s", stdout.String())
	}
	if len(got.Checks) == 0 {
		t.Fatal("checks length = 0, want checks")
	}
	if strings.Contains(stdout.String(), "RUN    ") {
		t.Fatalf("json stdout contains progress lines:\n%s", stdout.String())
	}
}

func TestRunDoctorChecksRunsJobsInParallel(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	jobs := []doctorCheckJob{
		{
			Name: "first",
			Run: func(context.Context) []doctorCheck {
				close(firstStarted)
				<-secondStarted
				return []doctorCheck{{Name: "first", Status: doctorOK, Detail: "done"}}
			},
		},
		{
			Name: "second",
			Run: func(context.Context) []doctorCheck {
				close(secondStarted)
				<-firstStarted
				return []doctorCheck{{Name: "second", Status: doctorOK, Detail: "done"}}
			},
		},
	}

	results := runDoctorChecks(context.Background(), jobs, time.Second, io.Discard)
	if len(results) != 2 || len(results[0]) != 1 || len(results[1]) != 1 {
		t.Fatalf("results = %#v", results)
	}
	if results[0][0].Name != "first" || results[1][0].Name != "second" {
		t.Fatalf("results order = %#v, want first then second", results)
	}
}

func TestParallelDoctorValidationDoesNotMutateCachedWorkflow(t *testing.T) {
	t.Parallel()

	const laneCount = 10_000
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = ""
	cfg.Observability.Staleness.Lanes = make([]workflowconfig.StalenessLaneThreshold, laneCount)
	for index := range cfg.Observability.Staleness.Lanes {
		cfg.Observability.Staleness.Lanes[index] = workflowconfig.StalenessLaneThreshold{
			State:          " Lane " + strconv.Itoa(index) + " ",
			ThresholdHours: 1,
		}
	}
	wantLanes := slices.Clone(cfg.Observability.Staleness.Lanes)
	workflow := workflowconfig.Workflow{Config: cfg}
	project := globalconfig.Project{ID: "detent", Workflow: "WORKFLOW.md", Workdir: "/repo"}
	global := globalconfig.Config{Projects: []globalconfig.Project{project}}
	deps := doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflow, nil
		},
		workflowCache: newDoctorWorkflowCache(),
	}
	if _, err := loadDoctorProjectWorkflow(context.Background(), project, deps); err != nil {
		t.Fatalf("loadDoctorProjectWorkflow() error = %v", err)
	}

	jobs := []doctorCheckJob{
		{
			Name: "Project detent checks",
			Run: func(ctx context.Context) []doctorCheck {
				return checkDoctorProject(ctx, project, deps, RuntimeSecret{}, false)
			},
		},
		{
			Name: doctorWorkflowOptimizationCheckName,
			Run: func(ctx context.Context) []doctorCheck {
				report, err := doctorWorkflowOptimization(ctx, nil, "", global, deps, "", doctorWorkflowOptimizationOptions{})
				if err != nil {
					return []doctorCheck{{Name: doctorWorkflowOptimizationCheckName, Status: doctorFail, Detail: err.Error()}}
				}
				return []doctorCheck{{Name: doctorWorkflowOptimizationCheckName, Status: doctorOK, WorkflowOptimization: report}}
			},
		},
	}

	results := runDoctorChecks(context.Background(), jobs, doctorTestSafetyTimeout, io.Discard)
	if len(results) != len(jobs) {
		t.Fatalf("results len = %d, want %d", len(results), len(jobs))
	}
	if !slices.Equal(cfg.Observability.Staleness.Lanes, wantLanes) {
		t.Fatal("parallel doctor validation mutated cached workflow staleness lanes")
	}
}

func TestDoctorReportKeepsStableOrderAfterParallelChecks(t *testing.T) {
	t.Parallel()

	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	jobs := []doctorCheckJob{
		{
			Name: "first",
			Run: func(context.Context) []doctorCheck {
				<-secondDone
				close(firstDone)
				return []doctorCheck{{Name: "first", Status: doctorOK, Detail: "done"}}
			},
		},
		{
			Name: "second",
			Run: func(context.Context) []doctorCheck {
				close(secondDone)
				return []doctorCheck{{Name: "second", Status: doctorOK, Detail: "done"}}
			},
		},
	}

	var report doctorReport
	for _, checks := range runDoctorChecks(context.Background(), jobs, time.Second, io.Discard) {
		report.Checks = append(report.Checks, checks...)
	}
	select {
	case <-firstDone:
	default:
		t.Fatal("first check did not complete")
	}

	var output bytes.Buffer
	if err := writeDoctorReport(&output, report); err != nil {
		t.Fatalf("writeDoctorReport() error = %v", err)
	}
	got := output.String()
	firstIndex := strings.Index(got, "first")
	secondIndex := strings.Index(got, "second")
	if firstIndex < 0 || secondIndex < 0 || firstIndex > secondIndex {
		t.Fatalf("report order =\n%s\nwant first before second", got)
	}
}

func TestRunDoctorCheckTimesOutUnresponsiveJob(t *testing.T) {
	t.Parallel()

	timer := &controlledDoctorCheckTimer{deadline: make(chan time.Time)}
	checksDone := make(chan []doctorCheck, 1)
	go func() {
		checksDone <- runDoctorCheckWithTimer(context.Background(), doctorCheckJob{
			Name: "slow check",
			Run: func(context.Context) []doctorCheck {
				select {}
			},
		}, 20*time.Millisecond, timer)
	}()
	timer.Expire()
	checks := <-checksDone

	if len(checks) != 1 {
		t.Fatalf("checks len = %d, want 1", len(checks))
	}
	if checks[0].Status != doctorFail {
		t.Fatalf("Status = %s, want %s", checks[0].Status, doctorFail)
	}
	if checks[0].Name != "slow check" || !strings.Contains(checks[0].Detail, "timed out after 20ms") {
		t.Fatalf("check = %#v, want timeout detail", checks[0])
	}
	if !strings.Contains(checks[0].Hint, "detent doctor --timeout 30s --port 0") {
		t.Fatalf("Hint = %q, want retry command", checks[0].Hint)
	}
}

func TestRunDoctorCheckTimeoutReportsCurrentInnerCheck(t *testing.T) {
	t.Parallel()

	timer := &controlledDoctorCheckTimer{deadline: make(chan time.Time)}
	checksDone := make(chan []doctorCheck, 1)
	go func() {
		checksDone <- runDoctorCheckWithTimer(context.Background(), doctorCheckJob{
			Name:    "Project alpha checks",
			Current: func() string { return "Project alpha GitHub readiness" },
			Run: func(context.Context) []doctorCheck {
				select {}
			},
		}, 20*time.Millisecond, timer)
	}()
	timer.Expire()
	checks := <-checksDone

	if len(checks) != 1 {
		t.Fatalf("checks len = %d, want 1", len(checks))
	}
	for _, want := range []string{"timed out after 20ms", "Project alpha GitHub readiness"} {
		if !strings.Contains(checks[0].Detail, want) {
			t.Fatalf("Detail = %q, want containing %q", checks[0].Detail, want)
		}
	}
}

func TestRunDoctorCheckRenewsTimeoutForReportedProgress(t *testing.T) {
	t.Parallel()

	const timeout = 100 * time.Millisecond
	progress := newDoctorCheckProgress()
	timer := &controlledDoctorCheckTimer{
		deadline: make(chan time.Time),
		resets:   make(chan time.Duration),
	}
	advance := make(chan struct{})
	checksDone := make(chan []doctorCheck, 1)
	stuck := time.NewTimer(doctorTestSafetyTimeout)
	defer stuck.Stop()
	go func() {
		checksDone <- runDoctorCheckWithTimer(context.Background(), doctorCheckJob{
			Name:     "Project alpha checks",
			Current:  progress.Current,
			Progress: progress.Updates(),
			Run: func(ctx context.Context) []doctorCheck {
				for _, name := range []string{"first", "second", "third"} {
					progress.Set("Project alpha "+name, nil)
					select {
					case <-ctx.Done():
						return nil
					case <-advance:
					}
				}
				return []doctorCheck{{Name: "Project alpha checks", Status: doctorOK, Detail: "done"}}
			},
		}, timeout, timer)
	}()

	for range 3 {
		select {
		case got := <-timer.resets:
			if got != timeout {
				t.Fatalf("timer reset = %s, want %s", got, timeout)
			}
		case <-stuck.C:
			t.Fatal("timed out waiting for timer reset")
		}
		advance <- struct{}{}
	}

	var checks []doctorCheck
	select {
	case checks = <-checksDone:
	case <-stuck.C:
		t.Fatal("timed out waiting for completed project checks")
	}
	if len(checks) != 1 || checks[0].Status != doctorOK {
		t.Fatalf("checks = %#v, want completed project checks", checks)
	}
}

type controlledDoctorCheckTimer struct {
	deadline chan time.Time
	resets   chan time.Duration
}

func (t *controlledDoctorCheckTimer) C() <-chan time.Time {
	return t.deadline
}

func (t *controlledDoctorCheckTimer) Reset(timeout time.Duration) bool {
	if t.resets != nil {
		t.resets <- timeout
	}
	return true
}

func (t *controlledDoctorCheckTimer) Stop() bool {
	return true
}

func (t *controlledDoctorCheckTimer) Expire() {
	t.deadline <- time.Time{}
}

func TestRunDoctorCheckFreezesProgressBeforeCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		timeout      time.Duration
		cancelParent bool
		wantErr      error
	}{
		{
			name:    "timer expiration",
			timeout: 20 * time.Millisecond,
			wantErr: context.DeadlineExceeded,
		},
		{
			name:         "parent cancellation",
			timeout:      time.Second,
			cancelParent: true,
			wantErr:      context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			progress := newDoctorCheckProgress()
			postCancelPublication := make(chan struct{})
			workerContext := make(chan context.Context, 1)
			workerStarted := make(chan struct{})
			parentCtx, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			timer := &controlledDoctorCheckTimer{deadline: make(chan time.Time)}
			if tt.cancelParent {
				go func() {
					<-workerStarted
					cancelParent()
				}()
			} else {
				go func() {
					<-workerStarted
					timer.Expire()
				}()
			}

			var freezeSawCanceledWorker bool
			completed := doctorCheck{Name: "Project alpha workflow", Status: doctorOK, Detail: "loaded"}
			canceled := doctorCheck{Name: "Project alpha route models", Status: doctorFail, Detail: "context canceled"}
			checks := runDoctorCheckWithTimer(parentCtx, doctorCheckJob{
				Name: "Project alpha checks",
				Freeze: func() doctorCheckSnapshot {
					ctx := <-workerContext
					select {
					case <-ctx.Done():
						freezeSawCanceledWorker = true
					default:
					}
					return progress.Freeze()
				},
				Progress: progress.Updates(),
				Run: func(ctx context.Context) []doctorCheck {
					workerContext <- ctx
					progress.Set("Project alpha route models", []doctorCheck{completed})
					close(workerStarted)
					<-ctx.Done()
					progress.Set("Project alpha GitHub readiness", []doctorCheck{completed, canceled})
					close(postCancelPublication)
					return []doctorCheck{completed, canceled}
				},
			}, tt.timeout, timer)

			if freezeSawCanceledWorker {
				t.Fatal("worker context was canceled before progress was frozen")
			}
			select {
			case <-postCancelPublication:
			case <-time.After(doctorTestSafetyTimeout):
				t.Fatal("job did not attempt to publish after cancellation")
			}
			postPublicationSnapshot := progress.Freeze()
			if postPublicationSnapshot.Current != "Project alpha route models" {
				t.Fatalf("current stage after rejected publication = %q, want %q", postPublicationSnapshot.Current, "Project alpha route models")
			}
			if len(postPublicationSnapshot.Checks) != 1 ||
				postPublicationSnapshot.Checks[0].Name != completed.Name ||
				postPublicationSnapshot.Checks[0].Status != completed.Status ||
				postPublicationSnapshot.Checks[0].Detail != completed.Detail {
				t.Fatalf("checks after rejected publication = %#v, want [%#v]", postPublicationSnapshot.Checks, completed)
			}
			if len(checks) != 2 {
				t.Fatalf("checks = %#v, want frozen completed check followed by failure", checks)
			}
			if checks[0].Name != completed.Name || checks[0].Status != completed.Status || checks[0].Detail != completed.Detail {
				t.Fatalf("checks[0] = %#v, want %#v", checks[0], completed)
			}
			if checks[1].Name != "Project alpha checks" || !strings.Contains(checks[1].Detail, "while running Project alpha route models") {
				t.Fatalf("failure check = %#v, want frozen route-model stage", checks[1])
			}
			if !strings.Contains(checks[1].Detail, tt.wantErr.Error()) {
				t.Fatalf("failure detail = %q, want parent error %q", checks[1].Detail, tt.wantErr)
			}
		})
	}
}

func TestDoctorProjectCheckJobRenewsTimeoutForConnectorProgress(t *testing.T) {
	t.Parallel()

	const responses = 3
	connectorStarted := make(chan struct{})
	requestProgress := make(chan struct{})
	progressReported := make(chan struct{})
	advance := make(chan struct{})
	var reportProgress sync.Once
	projectConnector := &fakeDoctorAutoPromoteConnector{
		fetchIssuesByStatesLimit: func(ctx context.Context, _ []string, _ int) ([]connector.Issue, error) {
			var progressErr error
			reportProgress.Do(func() {
				close(connectorStarted)
				for range responses {
					select {
					case <-ctx.Done():
						progressErr = ctx.Err()
						return
					case <-requestProgress:
					}
					connector.ReportProgress(ctx)
					progressReported <- struct{}{}
					select {
					case <-ctx.Done():
						progressErr = ctx.Err()
						return
					case <-advance:
					}
				}
			})
			return nil, progressErr
		},
	}
	jobs := doctorProjectCheckJobs(globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "alpha", Workflow: "WORKFLOW.md"}},
	}, doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: validDoctorDependencyWorkflow(false)}, nil
		},
		gitWorkTree: func(context.Context, string) error {
			return nil
		},
		gitRemoteURL: func(context.Context, string) (string, error) {
			return "https://github.com/digitaldrywood/detent", nil
		},
		autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
			return projectConnector, nil
		},
		githubReadiness: func(context.Context, ghconnector.Config, ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
			return nil, nil
		},
		githubMergeSettings: func(context.Context, workflowconfig.Config, string) (ghconnector.RepositoryMergeSettings, error) {
			return ghconnector.RepositoryMergeSettings{AllowSquashMerge: true}, nil
		},
	}, RuntimeSecret{Value: "token", Source: "github_token"}, false, doctorWorkflowDefaultTokenThreshold)
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}

	checksDone := make(chan []doctorCheck, 1)
	go func() {
		checksDone <- jobs[0].Run(context.Background())
	}()
	stuck := time.NewTimer(doctorTestSafetyTimeout)
	defer stuck.Stop()
	select {
	case <-connectorStarted:
	case <-stuck.C:
		t.Fatal("timed out waiting for connector check")
	}
	for {
		select {
		case <-jobs[0].Progress:
		default:
			goto progressDrained
		}
	}

progressDrained:
	for range responses {
		requestProgress <- struct{}{}
		select {
		case <-progressReported:
		case <-stuck.C:
			t.Fatal("timed out waiting for connector progress report")
		}
		select {
		case <-jobs[0].Progress:
		case <-stuck.C:
			t.Fatal("timed out waiting for connector progress signal")
		}
		advance <- struct{}{}
	}

	var checks []doctorCheck
	select {
	case checks = <-checksDone:
	case <-stuck.C:
		t.Fatal("timed out waiting for completed project checks")
	}

	for _, name := range []string{"Project alpha dependency auto-unblock", "Project alpha blocked recovery"} {
		var found bool
		for _, check := range checks {
			if check.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("checks = %#v, want completed %s check", checks, name)
		}
	}
	for _, check := range checks {
		if check.Status == doctorFail && strings.Contains(check.Detail, "timed out") {
			t.Fatalf("checks = %#v, want connector progress to renew timeout", checks)
		}
	}
}

func TestDoctorProjectCheckJobTimeoutPreservesCompletedChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		current         string
		lastCompleted   string
		blockOverlay    bool
		blockDependency bool
	}{
		{name: "GitHub readiness", current: "GitHub readiness", lastCompleted: "Project alpha skills"},
		{name: "local workflow overlay", current: "local workflow overlay", lastCompleted: "Project alpha capabilities", blockOverlay: true},
		{name: "dependency auto-unblock", current: "dependency auto-unblock", lastCompleted: "Project alpha repository merge policy", blockDependency: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow := validDoctorDependencyWorkflow(false)
			releaseBlockedCheck := make(chan struct{})
			blockedCheckStarted := make(chan struct{})
			var blockedCheckOnce sync.Once
			markBlockedCheckStarted := func() {
				blockedCheckOnce.Do(func() {
					close(blockedCheckStarted)
				})
			}
			t.Cleanup(func() {
				close(releaseBlockedCheck)
			})
			loadedWorkflow := workflowconfig.Workflow{Config: workflow}
			if tt.blockOverlay {
				loadedWorkflow.Overlay.Path = "/repo/WORKFLOW.local.md"
			}
			jobs := doctorProjectCheckJobs(globalconfig.Config{
				Projects: []globalconfig.Project{{ID: "alpha", Workflow: "WORKFLOW.md"}},
			}, doctorDeps{
				loadWorkflow: func(string) (workflowconfig.Workflow, error) {
					return loadedWorkflow, nil
				},
				gitTracked: func(context.Context, string) (bool, error) {
					if tt.blockOverlay {
						markBlockedCheckStarted()
						<-releaseBlockedCheck
					}
					return false, nil
				},
				gitWorkTree: func(context.Context, string) error {
					return nil
				},
				gitRemoteURL: func(context.Context, string) (string, error) {
					return "https://github.com/digitaldrywood/detent", nil
				},
				autoPromoteConnector: func(workflowconfig.Config) (doctorAutoPromoteConnector, error) {
					projectConnector := &fakeDoctorAutoPromoteConnector{}
					if tt.blockDependency {
						projectConnector.fetchIssuesByStatesLimit = func(context.Context, []string, int) ([]connector.Issue, error) {
							markBlockedCheckStarted()
							<-releaseBlockedCheck
							return nil, nil
						}
					}
					return projectConnector, nil
				},
				githubReadiness: func(context.Context, ghconnector.Config, ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
					if !tt.blockOverlay {
						if !tt.blockDependency {
							markBlockedCheckStarted()
						}
						<-releaseBlockedCheck
					}
					return nil, nil
				},
				githubMergeSettings: func(context.Context, workflowconfig.Config, string) (ghconnector.RepositoryMergeSettings, error) {
					return ghconnector.RepositoryMergeSettings{AllowSquashMerge: true}, nil
				},
			}, RuntimeSecret{Value: "token", Source: "github_token"}, false, doctorWorkflowDefaultTokenThreshold)
			if len(jobs) != 1 {
				t.Fatalf("jobs len = %d, want 1", len(jobs))
			}

			timer := &controlledDoctorCheckTimer{deadline: make(chan time.Time)}
			checksDone := make(chan []doctorCheck, 1)
			go func() {
				checksDone <- runDoctorCheckWithTimer(context.Background(), jobs[0], 20*time.Millisecond, timer)
			}()
			stuck := time.NewTimer(doctorTestSafetyTimeout)
			defer stuck.Stop()
			select {
			case <-blockedCheckStarted:
			case <-stuck.C:
				t.Fatalf("timed out waiting for blocked %s check", tt.current)
			}
			timer.Expire()

			var checks []doctorCheck
			select {
			case checks = <-checksDone:
			case <-stuck.C:
				t.Fatal("timed out waiting for controlled doctor timeout")
			}

			if len(checks) < 2 {
				t.Fatalf("checks = %#v, want completed checks followed by timeout", checks)
			}
			completedChecks := checks[:len(checks)-1]
			if got := completedChecks[0].Name; got != "Project alpha workflow" {
				t.Fatalf("first completed check = %q, want Project alpha workflow", got)
			}
			if got := completedChecks[len(completedChecks)-1].Name; got != tt.lastCompleted {
				t.Fatalf("last completed check = %q, want %q", got, tt.lastCompleted)
			}
			timeoutCheck := checks[len(checks)-1]
			for _, want := range []string{"Project alpha checks", "timed out after 20ms", "while running Project alpha " + tt.current} {
				if !strings.Contains(timeoutCheck.Name+" "+timeoutCheck.Detail, want) {
					t.Fatalf("timeout check = %#v, want containing %q", timeoutCheck, want)
				}
			}
		})
	}
}

func TestDoctorNormalizedTimeoutDefaultsToLiveReadinessBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "zero uses default", want: 30 * time.Second},
		{name: "negative uses default", timeout: -time.Second, want: 30 * time.Second},
		{name: "positive is preserved", timeout: 250 * time.Millisecond, want: 250 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := doctorNormalizedTimeout(tt.timeout); got != tt.want {
				t.Fatalf("doctorNormalizedTimeout(%s) = %s, want %s", tt.timeout, got, tt.want)
			}
		})
	}
}

func validDoctorWorkflow(sourceRoot string) workflowconfig.Config {
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Workspace.Root = sourceRoot
	cfg.Budget.BillingMode = workflowconfig.BillingModeMetered
	cfg.Budget.Enabled = true
	return cfg
}

func validDoctorWorkflowWithBackends(sourceRoot string, backends ...workflowconfig.AgentBackend) workflowconfig.Config {
	cfg := validDoctorWorkflow(sourceRoot)
	cfg.Agents.Backends = append([]workflowconfig.AgentBackend{}, backends...)
	return cfg
}

func doctorCodexAgentBackend(id string) workflowconfig.AgentBackend {
	return workflowconfig.AgentBackend{
		ID:       id,
		Kind:     workflowconfig.AgentBackendCodex,
		Protocol: "app-server",
		Command:  "codex",
	}
}

func doctorClaudeCodeAgentBackend(id string) workflowconfig.AgentBackend {
	return workflowconfig.AgentBackend{
		ID:       id,
		Kind:     workflowconfig.AgentBackendClaudeCode,
		Protocol: "headless",
		Command:  "claude",
	}
}

func validDoctorGlobalWithProjects(configPath string, ids ...string) globalconfig.Config {
	projects := make([]globalconfig.Project, 0, len(ids))
	for _, id := range ids {
		projects = append(projects, globalconfig.Project{
			ID:       id,
			Workflow: id + "/WORKFLOW.md",
			Workdir:  "/" + id,
			Weight:   1,
		})
	}
	return globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: projects,
	}
}

func validDoctorAutoPromoteWorkflow() workflowconfig.Config {
	cfg := validDoctorWorkflow("/repo")
	cfg.Tracker.Kind = workflowconfig.TrackerGitHub
	cfg.Tracker.APIKey = "token"
	cfg.Tracker.ProjectSlug = "PVT_1"
	cfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework", "Merging"}
	cfg.Tracker.ObservedStates = []string{"Backlog", "Human Review", "Blocked"}
	cfg.Agent.AutoPromote.Enabled = true
	cfg.Agent.AutoPromote.QuietSeconds = 600
	return cfg
}

func validDoctorDependencyWorkflow(enabled bool) workflowconfig.Config {
	cfg := validDoctorWorkflow("/repo")
	cfg.Tracker.Kind = workflowconfig.TrackerGitHub
	cfg.Tracker.APIKey = "token"
	cfg.Tracker.ProjectSlug = "PVT_1"
	cfg.Tracker.DependencyAutoUnblock.Enabled = enabled
	cfg.Tracker.DependencyAutoUnblock.SourceStates = []string{"Blocked"}
	cfg.Tracker.DependencyAutoUnblock.TargetState = "Todo"
	cfg.Tracker.DependencyAutoUnblock.Readiness = workflowconfig.DependencyReadinessTerminalOrMerged
	cfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework"}
	return cfg
}

func doctorAutoPromoteIssue(id string, pullRequest *connector.PullRequest) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = "digitaldrywood/detent#399"
	issue.Title = "Auto promote diagnostic"
	issue.State = "Human Review"
	issue.PullRequest = pullRequest
	return issue
}

func doctorDependencyIssue(id string, blockedBy []connector.BlockedRef) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = "digitaldrywood/detent#" + strings.TrimPrefix(id, "issue-")
	issue.Title = "Dependency diagnostic"
	issue.State = "Blocked"
	issue.BlockedBy = blockedBy
	return issue
}

func doctorDependencyIssueWithBody(id string, body string) connector.Issue {
	issue := doctorDependencyIssue(id, nil)
	issue.Description = body
	return issue
}

func doctorDependencyResolvedIssue(id string, identifier string, state string, closed bool, pullRequest *connector.PullRequest) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = identifier
	issue.Title = "Dependency blocker"
	issue.State = state
	issue.Closed = closed
	issue.PullRequest = pullRequest
	return issue
}

type fakeDoctorAutoPromoteConnector struct {
	issues                   []connector.Issue
	mergedIssues             []connector.Issue
	hydratedIssues           []connector.Issue
	resolvedIssues           []connector.Issue
	capabilities             []connector.DependencyCapability
	drift                    connector.StatusDrift
	driftErr                 error
	verifyErr                error
	verifyStates             []string
	limit                    int
	scan                     *connector.IssueStateScan
	fetchIssuesByStatesLimit func(context.Context, []string, int) ([]connector.Issue, error)
}

func (c *fakeDoctorAutoPromoteConnector) FetchIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	return c.issuesForStates(states), nil
}

func (c *fakeDoctorAutoPromoteConnector) FetchIssuesByStatesLimit(ctx context.Context, states []string, limit int) ([]connector.Issue, error) {
	c.limit = limit
	if c.fetchIssuesByStatesLimit != nil {
		return c.fetchIssuesByStatesLimit(ctx, states, limit)
	}
	return c.issuesForStates(states), nil
}

func (c *fakeDoctorAutoPromoteConnector) FetchIssuesByStatesScan(_ context.Context, states []string, limit int) (connector.IssueStateScan, error) {
	c.limit = limit
	if c.scan != nil && !doctorFakeTerminalStates(states) {
		return *c.scan, nil
	}
	scan := doctorIssueStateScan(c.issuesForStates(states))
	if limit > 0 && len(scan.Issues) > limit {
		scan.Issues = scan.Issues[:limit]
	}
	return scan, nil
}

func (c *fakeDoctorAutoPromoteConnector) issuesForStates(states []string) []connector.Issue {
	if doctorFakeTerminalStates(states) {
		return c.mergedIssues
	}
	return c.issues
}

func doctorFakeTerminalStates(states []string) bool {
	for _, state := range states {
		switch strings.ToLower(strings.TrimSpace(state)) {
		case "done", "cancelled", "canceled":
			return true
		}
	}
	return false
}

func (c *fakeDoctorAutoPromoteConnector) FetchIssueStatesByIdentifiers(_ context.Context, identifiers []string) ([]connector.Issue, error) {
	wanted := make(map[string]struct{}, len(identifiers))
	for _, identifier := range identifiers {
		wanted[strings.ToLower(strings.TrimSpace(identifier))] = struct{}{}
	}
	issues := make([]connector.Issue, 0, len(c.hydratedIssues)+len(c.resolvedIssues))
	for _, issue := range append(c.hydratedIssues, c.resolvedIssues...) {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(issue.Identifier))]; ok {
			issues = append(issues, issue)
		}
	}
	return issues, nil
}

func (c *fakeDoctorAutoPromoteConnector) VerifyStatusOptions(_ context.Context, states []string) error {
	c.verifyStates = append([]string(nil), states...)
	return c.verifyErr
}

func (c *fakeDoctorAutoPromoteConnector) FetchStatusDrift(context.Context) (connector.StatusDrift, error) {
	return c.drift, c.driftErr
}

func (c *fakeDoctorAutoPromoteConnector) DependencyCapabilities() []connector.DependencyCapability {
	return append([]connector.DependencyCapability(nil), c.capabilities...)
}

func stringSliceContains(values []string, want string) bool {
	return slices.Contains(values, want)
}

func successfulDoctorOptions(configPath string) options {
	return successfulDoctorOptionsWithConfig(configPath, globalconfig.Config{
		Path:       configPath,
		APIVersion: globalconfig.APIVersion,
		Kind:       globalconfig.Kind,
		Global: globalconfig.Settings{
			MaxConcurrentAgents: 1,
			Scheduling:          globalconfig.SchedulingWeighted,
		},
		Projects: []globalconfig.Project{},
	})
}

func successfulDoctorOptionsWithConfig(configPath string, cfg globalconfig.Config) options {
	return options{
		resolvePath: func(string) (globalconfig.PathResolution, error) {
			return globalconfig.PathResolution{Path: configPath, Rule: globalconfig.PathRuleFlag}, nil
		},
		read: func(string) (globalconfig.Config, error) {
			cfg.Path = configPath
			return cfg, nil
		},
		readProject: func(_ string, projectID string) (globalconfig.Config, []string, error) {
			scoped, scope, _ := scopeDoctorGlobalConfig(cfg, projectID)
			scoped.Path = configPath
			return scoped, scope.SkippedProjects, nil
		},
		stdoutTTY: func() bool { return true },
	}
}

func assertDoctorCheck(t *testing.T, report doctorReport, name string, status doctorStatus, detail string) {
	t.Helper()

	for _, check := range report.Checks {
		if check.Name != name {
			continue
		}
		if check.Status != status {
			t.Fatalf("%s status = %s, want %s", name, check.Status, status)
		}
		if !strings.Contains(check.Detail, detail) && !strings.Contains(check.Hint, detail) {
			t.Fatalf("%s missing %q:\nDetail: %s\nHint: %s", name, detail, check.Detail, check.Hint)
		}
		return
	}
	t.Fatalf("missing doctor check %q in %#v", name, report.Checks)
}

func doctorCheckByName(t *testing.T, report doctorReport, name string) doctorCheck {
	t.Helper()

	for _, check := range report.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("missing doctor check %q in %#v", name, report.Checks)
	return doctorCheck{}
}

func assertDoctorMissingCheck(t *testing.T, report doctorReport, name string) {
	t.Helper()

	for _, check := range report.Checks {
		if check.Name == name {
			t.Fatalf("found doctor check %q, want it skipped: %#v", name, check)
		}
	}
}

func successfulDoctorDeps() doctorDeps {
	return doctorDeps{
		loadWorkflow: func(string) (workflowconfig.Workflow, error) {
			return workflowconfig.Workflow{Config: validDoctorWorkflow("/repo")}, nil
		},
		lookupEnv: func(key string) string {
			if key == "GITHUB_TOKEN" {
				return "token"
			}
			if key == "PATH" {
				return "/usr/bin"
			}
			return ""
		},
		resolveCommandOnPath: func(pathValue string, executable string) (string, error) {
			return filepath.Join(pathValue, executable), nil
		},
		resolveCommandInDir: func(_ context.Context, _ string, _ []string, executable string) (string, error) {
			return "/usr/bin/" + executable, nil
		},
		runCommandInDir: func(context.Context, string, []string, string, ...string) error {
			return nil
		},
		codexInitialize: func(context.Context, string, []string) error {
			return nil
		},
		httpDo: func(*http.Request) (*http.Response, error) {
			return nil, errors.New("health unavailable")
		},
		githubScopes: func(context.Context, string) ([]string, error) {
			return []string{"repo", "read:org", "read:project", "project"}, nil
		},
		githubReadiness: func(context.Context, ghconnector.Config, ghconnector.ReadinessConfig) ([]ghconnector.ReadinessCheck, error) {
			return []ghconnector.ReadinessCheck{
				{Name: "GitHub readiness", Status: ghconnector.ReadinessOK, Detail: "ready"},
			}, nil
		},
		githubMergeSettings: func(context.Context, workflowconfig.Config, string) (ghconnector.RepositoryMergeSettings, error) {
			return ghconnector.RepositoryMergeSettings{AllowSquashMerge: true}, nil
		},
		githubRepositoryInfo: func(_ context.Context, _ workflowconfig.Config, repository string) (ghconnector.RepositoryInfo, error) {
			return ghconnector.RepositoryInfo{
				ID:            1,
				NameWithOwner: repository,
				Private:       true,
				Visibility:    "private",
				DefaultBranch: "main",
			}, nil
		},
		githubLabels: func(_ context.Context, cfg workflowconfig.Config, _ string) ([]string, error) {
			return []string{
				cfg.Agent.AutoPromote.OptoutLabel,
				cfg.Agent.MaxSessionTokenOverrideLabel,
			}, nil
		},
		listen: func(string, string) (net.Listener, error) {
			return fakeDoctorListener{addr: fakeDoctorAddr("127.0.0.1:49152")}, nil
		},
		openSQLite: func(context.Context, string) (doctorStore, error) {
			return fakeDoctorStore{}, nil
		},
		gitWorkTree: func(context.Context, string) error {
			return nil
		},
		gitWorktrees: func(context.Context, string) ([]doctorGitWorktree, error) {
			return nil, nil
		},
		workflowSourcePolicy: func(_ context.Context, projectID string, _ globalconfig.Project, _ string, _ string) doctorCheck {
			return doctorCheck{
				Name:   "Project " + projectID + " workflow source policy",
				Status: doctorOK,
				Detail: "workflow source policy verified",
			}
		},
		executable: func() (string, error) {
			return filepath.Join("Users", "corylanou", "go", "bin", "detent"), nil
		},
		shipSkillProbe: func(string) (doctorShipSkill, error) {
			return doctorShipSkill{Version: "1.6.0", Path: "/plugins/go-workflow/1.6.0/skills/ship/SKILL.md"}, nil
		},
		now: time.Now,
	}
}

type fakeDoctorStore struct {
	closeErr error
}

func (s fakeDoctorStore) Close() error {
	return s.closeErr
}

type fakeDoctorListener struct {
	addr     net.Addr
	closeErr error
}

func (l fakeDoctorListener) Accept() (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (l fakeDoctorListener) Close() error {
	return l.closeErr
}

func (l fakeDoctorListener) Addr() net.Addr {
	return l.addr
}

type fakeDoctorAddr string

func (a fakeDoctorAddr) Network() string {
	return "tcp"
}

func (a fakeDoctorAddr) String() string {
	return string(a)
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func occupiedDoctorPort(t *testing.T, host string, statusCode int, body string) int {
	t.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", host, err)
	}
	port := doctorPortFromAddr(t, listener.Addr())

	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("Write() error = %v", err)
		}
	})}}
	server.Start()
	t.Cleanup(server.Close)

	return port
}

func doctorPortFromAddr(t *testing.T, addr net.Addr) int {
	t.Helper()

	_, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", addr.String(), err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Atoi(%q) error = %v", portText, err)
	}
	return port
}
