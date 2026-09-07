package cli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/budget"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/observability"
	projectpkg "github.com/digitaldrywood/detent/internal/project"
	runnerpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	detentupdate "github.com/digitaldrywood/detent/internal/update"
	"github.com/digitaldrywood/detent/internal/workspace"
)

var errProjectFactoryStub = errors.New("project factory stub")

func TestTelemetryUpdateStatus(t *testing.T) {
	t.Parallel()

	lastCheck := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	pendingSince := lastCheck.Add(-4 * time.Hour)
	got := telemetryUpdateStatus([]autoUpdateStatusSource{autoUpdateStatusStub{status: detentupdate.AutoStatus{
		Enabled:            true,
		AutoApplyEnabled:   true,
		CheckInterval:      12 * time.Hour,
		State:              "scheduled",
		LastCheckAt:        &lastCheck,
		LastAppliedVersion: "1.2.4",
		PendingSince:       &pendingSince,
		MaxDeferral:        6 * time.Hour,
		Critical:           true,
	}}})
	if !got.Enabled || !got.AutoApplyEnabled || got.CheckIntervalHours != 12 || got.LastAppliedVersion != "1.2.4" || got.LastCheckAt == nil || got.PendingSince == nil || got.MaxDeferralHours != 6 || !got.Critical {
		t.Fatalf("telemetryUpdateStatus() = %#v", got)
	}
}

func TestTelemetryAgentPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source agentPoolSnapshotSource
		want   []telemetry.AgentPool
	}{
		{name: "nil source"},
		{
			name: "copies valid scheduler snapshots",
			source: agentPoolSnapshotSourceStub{
				{Name: "code", Used: 5, Capacity: 5, Guaranteed: 5, BurstTo: 5, Generation: 2},
				{Name: "video", Used: 12, Capacity: 15, Guaranteed: 10, BurstTo: 15, Borrowed: 2, Available: 3, Reclaiming: true, Generation: 3},
			},
			want: []telemetry.AgentPool{
				{Name: "code", Used: 5, Capacity: 5, Guaranteed: 5, BurstTo: 5, Generation: 2},
				{Name: "video", Used: 12, Capacity: 15, Guaranteed: 10, BurstTo: 15, Borrowed: 2, Available: 3, Reclaiming: true, Generation: 3},
			},
		},
		{
			name: "drops unusable snapshots",
			source: agentPoolSnapshotSourceStub{
				{Name: " ", Capacity: 5},
				{Name: "code"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := telemetryAgentPools(tt.source)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("telemetryAgentPools() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestProjectSnapshotMetadataIncludesAgentPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pool string
		want string
	}{
		{name: "configured pool", pool: "code", want: "code"},
		{name: "implicit default", want: scheduler.DefaultPoolName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := projectSnapshotMetadataFromConfig(globalconfig.Project{ID: "detent", Pool: tt.pool}, time.Now())
			if got.Pool != tt.want {
				t.Fatalf("projectSnapshotMetadataFromConfig().Pool = %q, want %q", got.Pool, tt.want)
			}
		})
	}
}

func TestStampSnapshotProjectIDPreservesDegradedFreshness(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 15, 0, 0, 0, time.UTC)
	snapshot := stampSnapshotProjectID(telemetry.Snapshot{
		Project: telemetry.Project{ID: "docs"},
		StrandedActiveIssues: []telemetry.StrandedIssue{{
			Identifier: "digitaldrywood/docs#1",
		}},
		Refresh: telemetry.Refresh{
			Status:      telemetry.RefreshStatusDegraded,
			LastError:   "project runtime unavailable",
			LastErrorAt: &now,
		},
	})

	if len(snapshot.Refresh.Sources) != 1 {
		t.Fatalf("Refresh.Sources = %#v, want one project source", snapshot.Refresh.Sources)
	}
	source := snapshot.Refresh.Sources[0]
	if source.ProjectID != "docs" || source.Name != telemetry.RefreshSourceProject || !source.Degraded {
		t.Fatalf("project refresh source = %#v", source)
	}
	if source.LastError != "project runtime unavailable" || source.LastErrorAt == nil || !source.LastErrorAt.Equal(now) {
		t.Fatalf("project refresh error = %q at %v", source.LastError, source.LastErrorAt)
	}
	if !snapshot.Refresh.Stale(now) {
		t.Fatal("Refresh.Stale() = false, want true")
	}
	if len(snapshot.StrandedActiveIssues) != 1 || snapshot.StrandedActiveIssues[0].ProjectID != "docs" {
		t.Fatalf("StrandedActiveIssues = %#v, want docs project ID", snapshot.StrandedActiveIssues)
	}
}

type autoUpdateStatusStub struct {
	status detentupdate.AutoStatus
}

func (s autoUpdateStatusStub) Status() detentupdate.AutoStatus {
	return s.status
}

func TestBuildRunnerReturnsRunner(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Workspace.Root = t.TempDir()

	run, err := buildRunner(workflowconfig.Workflow{Config: cfg}, "alpha", "", globalconfig.Memory{}, nil, nil)
	if err != nil {
		t.Fatalf("buildRunner() error = %v", err)
	}
	if run == nil {
		t.Fatal("buildRunner() = nil, want non-nil runner")
	}
	if _, ok := run.(*runnerpkg.Runner); !ok {
		t.Fatalf("buildRunner() = %T, want *runner.Runner", run)
	}
}

func TestBuildRunnerSupportsClaudeCodeBackendRoutes(t *testing.T) {
	t.Parallel()

	source := initRunnerSourceRepo(t)
	claudeCommand, argsPath, stdinPath := writeRunnerClaudeStub(t)
	sessionStore := &runnerSessionStore{sessionID: 833}
	startedAt := time.Date(2026, 7, 2, 13, 30, 0, 0, time.UTC)

	workflow, err := workflowconfig.ParseWorkflow([]byte(`---
tracker:
  kind: memory
workspace:
  root: ` + strconv.Quote(filepath.Join(t.TempDir(), "workspaces")) + `
agents:
  backends:
    - id: codex-main
      kind: codex
      command: codex app-server
    - id: claude-worker
      kind: claude_code
      command: ` + strconv.Quote(runnerShellQuote(claudeCommand)) + `
      provider: custom
      options:
        effort: high
        permission_mode: acceptEdits
        allowed_tools:
          - Bash
          - Edit
        disallowed_tools:
          - WebFetch
        include_partial_messages: true
        turn_timeout_ms: 60000
        stall_timeout_ms: 0
        shell: sh
        extra_args:
          - --custom
          - value
  routes:
    - name: validator-codex
      role: validator
      backend: codex-main
      model: gpt-5-codex
    - name: code-claude
      backend: claude-worker
      model: fable
      default: true
---
Prompt {{ issue.identifier }}
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	run, err := buildRunner(workflow, "detent", source, globalconfig.Memory{}, sessionStore, nil)
	if err != nil {
		t.Fatalf("buildRunner() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	result, err := run.Run(ctx, runnerpkg.RunRequest{
		Issue: connector.Issue{
			ID:         "issue-833",
			Identifier: "digitaldrywood/detent#833",
			Title:      "Wire claude_code into agent backend factory",
			BranchName: "detent/issue-833",
		},
		StartedAt: startedAt,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.FinalState != runnerpkg.FinalStateCompleted || result.Output != "claude streamed" {
		t.Fatalf("Run() result = %#v, want completed claude streamed output", result)
	}
	if result.Tokens.InputTokens != 11 || result.Tokens.OutputTokens != 5 || result.Tokens.TotalTokens != 16 {
		t.Fatalf("Run() tokens = %#v, want final claude usage", result.Tokens)
	}
	if sessionStore.started.Model != "" || sessionStore.started.RequestedModel != "fable" || sessionStore.usage.Model != "fable" {
		t.Fatalf("recorded models = start resolved %q requested %q usage %q, want unresolved/fable/fable", sessionStore.started.Model, sessionStore.started.RequestedModel, sessionStore.usage.Model)
	}
	if sessionStore.started.RuntimeIdentity.Provider.Value != "custom" || sessionStore.started.RuntimeIdentity.ReasoningEffort.Value != "high" {
		t.Fatalf("configured runtime identity = %#v, want custom provider and high effort", sessionStore.started.RuntimeIdentity)
	}
	if result.RuntimeIdentity.ResolvedModel.Value != "fable" || result.RuntimeIdentity.ReasoningEffort.Value != "high" {
		t.Fatalf("resolved runtime identity = %#v, want runtime fable and configured high effort", result.RuntimeIdentity)
	}
	if sessionStore.phase.EndpointFamily != workflowconfig.AgentBackendClaudeCode {
		t.Fatalf("WorkflowPhaseEvent EndpointFamily = %q, want claude_code", sessionStore.phase.EndpointFamily)
	}
	if sessionStore.phase.TotalTokens != 16 {
		t.Fatalf("WorkflowPhaseEvent TotalTokens = %d, want 16", sessionStore.phase.TotalTokens)
	}

	args := readRunnerLines(t, argsPath)
	wantPrefix := []string{
		"-p", "--output-format", "stream-json", "--verbose",
		"--model", "fable",
		"--max-turns", "20",
		"--effort", "high",
		"--permission-mode", "acceptEdits",
		"--allowedTools", "Bash", "Edit",
		"--disallowedTools", "WebFetch",
		"--include-partial-messages",
	}
	if len(args) < len(wantPrefix) {
		t.Fatalf("claude args = %#v, want prefix %#v", args, wantPrefix)
	}
	if !runnerStringPrefix(args, wantPrefix) {
		t.Fatalf("claude args = %#v, want prefix %#v", args, wantPrefix)
	}
	if len(args) < 2 || args[len(args)-2] != "--custom" || args[len(args)-1] != "value" {
		t.Fatalf("claude args = %#v, want extra args at end", args)
	}
	stdin := readRunnerFile(t, stdinPath)
	if !strings.Contains(stdin, "Prompt digitaldrywood/detent#833") {
		t.Fatalf("claude stdin = %q, want rendered issue prompt", stdin)
	}
}

func TestBuildAgentBackendUnsupportedKindNamesSupportedKinds(t *testing.T) {
	t.Parallel()

	_, err := buildAgentBackend(workflowconfig.AgentBackend{
		ID:      "local-llama",
		Kind:    "llama",
		Command: "llama",
	})
	if err == nil {
		t.Fatal("buildAgentBackend() error = nil, want unsupported kind")
	}
	for _, want := range []string{"unsupported agent backend kind \"llama\"", "supported kinds: codex, claude_code"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("buildAgentBackend() error = %v, missing %q", err, want)
		}
	}
}

func TestBuildRunnerUsesTopLevelPricingPath(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Workspace.Root = t.TempDir()
	cfg.Budget.PricingPath = filepath.Join(t.TempDir(), "missing-models.yaml")

	_, err := buildRunner(workflowconfig.Workflow{Config: cfg}, "alpha", "", globalconfig.Memory{}, nil, nil)
	if err == nil {
		t.Fatal("buildRunner() error = nil, want pricing load error")
	}
	if !strings.Contains(err.Error(), "load pricing") {
		t.Fatalf("buildRunner() error = %v, want load pricing error", err)
	}
}

func TestBuildBudgetDispatchGuards(t *testing.T) {
	t.Parallel()

	disabled := workflowconfig.Default().Budget
	checker, estimator, err := buildBudgetDispatchGuards("alpha", disabled, nil, nil)
	if err != nil {
		t.Fatalf("buildBudgetDispatchGuards(disabled) error = %v", err)
	}
	if checker != nil || estimator != nil {
		t.Fatalf("disabled guards = %T/%T, want nil guards", checker, estimator)
	}

	enabled := disabled
	enabled.Enabled = true
	enabled.BillingMode = workflowconfig.BillingModeMetered
	_, _, err = buildBudgetDispatchGuards("alpha", enabled, &runnerSessionStore{}, nil)
	if !errors.Is(err, budget.ErrMissingSpendStore) {
		t.Fatalf("buildBudgetDispatchGuards(missing spend store) error = %v, want ErrMissingSpendStore", err)
	}

	subscription := enabled
	subscription.BillingMode = workflowconfig.BillingModeSubscription
	checker, estimator, err = buildBudgetDispatchGuards("alpha", subscription, &runnerSessionStore{}, nil)
	if err != nil {
		t.Fatalf("buildBudgetDispatchGuards(subscription) error = %v", err)
	}
	if checker == nil || estimator != nil {
		t.Fatalf("subscription guards = %T/%T, want advisory checker and nil estimator", checker, estimator)
	}
	decision, err := checker.CheckDispatch(t.Context(), budget.DispatchRequest{Model: "gpt-5", Estimate: budget.TokenEstimate{TotalTokens: 1_000_000}})
	if err != nil || !decision.Allowed || decision.Refusal != nil {
		t.Fatalf("subscription decision = %#v, error = %v, want allowed", decision, err)
	}

	ctx := context.Background()
	storeBackend, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := storeBackend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	checker, estimator, err = buildBudgetDispatchGuards("alpha", enabled, storeBackend, nil)
	if err != nil {
		t.Fatalf("buildBudgetDispatchGuards(enabled) error = %v", err)
	}
	if checker == nil || estimator == nil {
		t.Fatalf("enabled guards = %T/%T, want checker and estimator", checker, estimator)
	}

	startedAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	for index, inputTokens := range []int64{10, 20, 30, 40, 50} {
		sessionID, err := storeBackend.StartSession(ctx, store.SessionStart{
			StartedAt: startedAt.Add(time.Duration(index) * time.Minute),
			Model:     "gpt-history",
		})
		if err != nil {
			t.Fatalf("StartSession() error = %v", err)
		}
		if err := storeBackend.FinishSession(ctx, sessionID, store.SessionFinish{
			CompletedAt: startedAt.Add(time.Duration(index+1) * time.Minute),
			InputTokens: inputTokens,
			TotalTokens: inputTokens,
			FinalState:  runnerpkg.FinalStateCompleted,
			Model:       "gpt-history",
		}); err != nil {
			t.Fatalf("FinishSession() error = %v", err)
		}
	}
	estimate, err := estimator.EstimateDispatch(ctx, "gpt-history")
	if err != nil {
		t.Fatalf("EstimateDispatch() error = %v", err)
	}
	if estimate.Sessions != 5 || estimate.InputTokens != 50 {
		t.Fatalf("EstimateDispatch() = %#v, want five-session p90 input 50", estimate)
	}
}

func TestBuildBudgetDispatchGuardsIsolatesProjectDailySpend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	now := time.Date(2026, 7, 12, 20, 0, 0, 0, time.UTC)
	for _, session := range []struct {
		projectID string
		tokens    int64
	}{
		{projectID: "quiet", tokens: 10},
		{projectID: "busy", tokens: 200},
	} {
		sessionID, err := backend.StartSession(ctx, store.SessionStart{ProjectID: session.projectID, StartedAt: now.Add(-time.Minute), Model: "gpt-test"})
		if err != nil {
			t.Fatalf("StartSession(%s) error = %v", session.projectID, err)
		}
		if err := backend.FinishSession(ctx, sessionID, store.SessionFinish{CompletedAt: now, InputTokens: session.tokens, TotalTokens: session.tokens, FinalState: "complete", Model: "gpt-test"}); err != nil {
			t.Fatalf("FinishSession(%s) error = %v", session.projectID, err)
		}
	}

	cfg := workflowconfig.Default().Budget
	cfg.Enabled = true
	cfg.BillingMode = workflowconfig.BillingModeMetered
	cfg.PerDayMaxUSD = 100
	pricing := budget.PricingTable{"gpt-test": {USDPerInputToken: 1}}
	for _, tt := range []struct {
		projectID string
		wantAllow bool
	}{
		{projectID: "quiet", wantAllow: true},
		{projectID: "busy", wantAllow: false},
	} {
		t.Run(tt.projectID, func(t *testing.T) {
			checker, _, err := buildBudgetDispatchGuards(tt.projectID, cfg, backend, pricing)
			if err != nil {
				t.Fatalf("buildBudgetDispatchGuards() error = %v", err)
			}
			decision, err := checker.CheckDispatch(ctx, budget.DispatchRequest{Model: "gpt-test", Now: now, Estimate: budget.TokenEstimate{InputTokens: 1}})
			if err != nil {
				t.Fatalf("CheckDispatch() error = %v", err)
			}
			if decision.Allowed != tt.wantAllow {
				t.Fatalf("Allowed = %t, want %t; refusal = %#v", decision.Allowed, tt.wantAllow, decision.Refusal)
			}
			if !tt.wantAllow && (decision.Refusal == nil || decision.Refusal.CurrentSpendUSD != 200) {
				t.Fatalf("Refusal = %#v, want project-scoped current spend 200", decision.Refusal)
			}
		})
	}
}

func TestBuildCodexCommandUsesConfiguredShell(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Codex.Command = "codex app-server --experimental"
	cfg.Codex.Shell = "bash"

	cmd := buildCodexCommand(context.Background(), cfg)
	if got := strings.Join(cmd.Args, "\x00"); got != "bash\x00-c\x00codex app-server --experimental" {
		t.Fatalf("Args = %#v, want bash -c configured command", cmd.Args)
	}
}

func TestBuildWorkspaceBackendUsesProjectWorkdirAsSourceRoot(t *testing.T) {
	t.Parallel()

	source := initRunnerSourceRepo(t)
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Workspace.Root = filepath.Join(t.TempDir(), "workspaces")
	cfg.Workspace.SourceRoot = ""

	backend, err := buildWorkspaceBackend(cfg, source, nil)
	if err != nil {
		t.Fatalf("buildWorkspaceBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), workspace.Issue{Identifier: "DD-129"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if got := strings.TrimSpace(runRunnerGit(t, info.Path, "branch", "--show-current")); got != "detent/dd-129" {
		t.Fatalf("worktree branch = %q, want detent/dd-129", got)
	}
	if got := readRunnerFile(t, filepath.Join(info.Path, "README.md")); got != "source repo\n" {
		t.Fatalf("README.md = %q, want source repo", got)
	}
}

func TestProjectDependenciesInjectsNonNilRunner(t *testing.T) {
	t.Parallel()

	var captured projectpkg.Dependencies
	base := projectpkg.Dependencies{Logger: nil}
	factory := withRunnerFactory(base, nil, func(d projectpkg.Dependencies) (*projectpkg.Project, error) {
		captured = d
		return nil, errProjectFactoryStub
	})

	workflowPath := writeWorkflowFile(t)
	_, err := factory(globalconfig.Project{
		ID:       "alpha",
		Workflow: workflowPath,
		Workdir:  filepath.Dir(workflowPath),
		Weight:   1,
	})
	if !errors.Is(err, errProjectFactoryStub) {
		t.Fatalf("ProjectFactory() error = %v, want stub", err)
	}
	if captured.Runner == nil {
		t.Fatal("project dependencies Runner = nil, want non-nil injected runner")
	}
	if _, ok := captured.Runner.(*runnerpkg.Runner); !ok {
		t.Fatalf("injected Runner = %T, want *runner.Runner", captured.Runner)
	}
}

func TestProjectDependenciesUseRuntimeGitHubTokenSource(t *testing.T) {
	t.Parallel()

	var captured projectpkg.Dependencies
	token := "first-token"
	factory := withRunnerFactory(projectpkg.Dependencies{}, nil, func(d projectpkg.Dependencies) (*projectpkg.Project, error) {
		captured = d
		return nil, errProjectFactoryStub
	}, func() string {
		return token
	})

	workflowPath := writeWorkflowFile(t)
	_, err := factory(globalconfig.Project{
		ID:       "alpha",
		Workflow: workflowPath,
		Workdir:  filepath.Dir(workflowPath),
		Weight:   1,
	})
	if !errors.Is(err, errProjectFactoryStub) {
		t.Fatalf("ProjectFactory() error = %v, want %v", err, errProjectFactoryStub)
	}
	if captured.GitHubToken != "first-token" {
		t.Fatalf("GitHubToken = %q, want first-token", captured.GitHubToken)
	}

	token = "second-token"
	_, err = factory(globalconfig.Project{
		ID:       "bravo",
		Workflow: workflowPath,
		Workdir:  filepath.Dir(workflowPath),
		Weight:   1,
	})
	if !errors.Is(err, errProjectFactoryStub) {
		t.Fatalf("ProjectFactory() error = %v, want %v", err, errProjectFactoryStub)
	}
	if captured.GitHubToken != "second-token" {
		t.Fatalf("GitHubToken = %q, want second-token", captured.GitHubToken)
	}
}

func TestStartupSnapshotMarksTrackerStateInitializing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 22, 9, 0, 0, 0, time.UTC)
	snapshot := startupSnapshot(context.Background(), globalconfig.Config{
		Projects: []globalconfig.Project{{ID: "alpha", Color: "#1192e8"}},
	}, nil, "http://localhost:4101", now)

	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusInitializing {
		t.Fatalf("snapshot Refresh status = %q, want %q", got, telemetry.RefreshStatusInitializing)
	}
	if snapshot.Refresh.NextRefreshAt == nil || !snapshot.Refresh.NextRefreshAt.Equal(now) {
		t.Fatalf("snapshot.Refresh.NextRefreshAt = %v, want %v", snapshot.Refresh.NextRefreshAt, now)
	}
	if len(snapshot.Projects) != 1 {
		t.Fatalf("snapshot.Projects len = %d, want 1", len(snapshot.Projects))
	}
	if got := snapshot.Projects[0].Refresh.ReadinessStatus(); got != telemetry.RefreshStatusInitializing {
		t.Fatalf("project Refresh status = %q, want %q", got, telemetry.RefreshStatusInitializing)
	}
}

func TestPublishSnapshotsPublishesToHub(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	healthy := startRefreshProject(t, "alpha")
	waitForProjectDataSeq(t, healthy, 1)
	mustSetProject(t, registry, healthy)

	snapshotHub := hub.New[telemetry.Snapshot]()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var seq atomic.Uint64
	go func() {
		defer close(done)
		publishSnapshots(
			ctx,
			registry,
			agentPoolSnapshotSourceStub{{Name: scheduler.DefaultPoolName, Used: 1, Capacity: 5, Generation: 1}},
			snapshotHub,
			&seq,
			nil,
			nil,
			"http://localhost:4101",
			nil,
			5*time.Millisecond,
			func() time.Time { return now },
		)
	}()

	var (
		snapshot telemetry.Snapshot
		ok       bool
	)
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if snapshot, ok = snapshotHub.Latest(); ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if !ok {
		t.Fatal("publishSnapshots did not publish any snapshot")
	}
	if !snapshot.GeneratedAt.Equal(now) {
		t.Fatalf("snapshot.GeneratedAt = %v, want %v", snapshot.GeneratedAt, now)
	}
	if snapshot.Project.DisplayName != "alpha" {
		t.Fatalf("snapshot.Project.DisplayName = %q, want alpha", snapshot.Project.DisplayName)
	}
	if len(snapshot.Projects) != 1 {
		t.Fatalf("snapshot.Projects len = %d, want 1", len(snapshot.Projects))
	}
	if snapshot.Projects[0].Project.ID != "alpha" || snapshot.Projects[0].Project.DisplayName != "alpha" {
		t.Fatalf("snapshot.Projects[0].Project = %#v, want alpha metadata", snapshot.Projects[0].Project)
	}
	if snapshot.Projects[0].Project.Pool != scheduler.DefaultPoolName {
		t.Fatalf("snapshot.Projects[0].Project.Pool = %q, want default", snapshot.Projects[0].Project.Pool)
	}
	if !reflect.DeepEqual(snapshot.AgentPools, []telemetry.AgentPool{{Name: scheduler.DefaultPoolName, Used: 1, Capacity: 5, Generation: 1}}) {
		t.Fatalf("snapshot.AgentPools = %#v, want scheduler utilization", snapshot.AgentPools)
	}
	if snapshot.DashboardURL != "http://localhost:4101" {
		t.Fatalf("snapshot.DashboardURL = %q, want dashboard URL", snapshot.DashboardURL)
	}
	if snapshot.Refresh.NextRefreshAt == nil {
		t.Fatalf("snapshot.Refresh.NextRefreshAt = nil, want next refresh")
	}
}

func TestPublishSnapshotOnceAssignsMonotonicSeq(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, startRefreshProject(t, "alpha"))

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	for index := range 3 {
		if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now.Add(time.Duration(index)*time.Second), nil, nil, "http://localhost:4101", nil); err != nil {
			t.Fatalf("publishSnapshotOnce(%d) error = %v", index, err)
		}
		snapshot, ok := snapshotHub.Latest()
		if !ok {
			t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
		}
		want := uint64(index + 1)
		if snapshot.Seq != want {
			t.Fatalf("publish %d Seq = %d, want %d", index, snapshot.Seq, want)
		}
	}
}

func TestPublishSnapshotOnceMarksStateFailureSectionsUnknown(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, startRefreshProject(t, "alpha"))

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := publishSnapshotOnce(ctx, registry, nil, snapshotHub, &seq, nil, time.Now(), nil, nil, "", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok || len(snapshot.Projects) != 1 {
		t.Fatalf("snapshot = %#v, want one degraded project", snapshot)
	}
	projectSnapshot := snapshot.Projects[0]
	if projectSnapshot.Tracker.Source != telemetry.SnapshotSourceUnknown || projectSnapshot.Runtime.Source != telemetry.SnapshotSourceUnknown {
		t.Fatalf("project sections = tracker %#v runtime %#v, want unknown", projectSnapshot.Tracker, projectSnapshot.Runtime)
	}
	if telemetry.BoardWorkloadCompleteForProject(snapshot, "alpha") {
		t.Fatal("BoardWorkloadCompleteForProject() = true, want false")
	}
}

func TestPublishSnapshotOnceUsesLastKnownTrackerStateUntilHydration(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	trackedProject, err := projectpkg.New(projectpkg.Config{
		Project:  globalconfig.Project{ID: "alpha", Workdir: t.TempDir(), Weight: 1},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, trackedProject)

	cached := telemetry.Snapshot{
		LastKnown:      true,
		LastKnownUntil: time.Date(2026, 7, 16, 18, 5, 0, 0, time.UTC),
		GeneratedAt:    time.Date(2026, 7, 16, 17, 59, 0, 0, time.UTC),
		Refresh:        telemetry.Refresh{Status: telemetry.RefreshStatusReady},
		BoardIssues:    []telemetry.Issue{{ID: "cached-issue"}},
	}
	snapshotHub := hub.New[telemetry.Snapshot]()
	if err := snapshotHub.Publish(cached); err != nil {
		t.Fatalf("Publish(cached) error = %v", err)
	}
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, cached.GeneratedAt.Add(time.Minute), nil, nil, "", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	got, ok := snapshotHub.Latest()
	if !ok || got.LastKnown || len(got.BoardIssues) != 1 || got.BoardIssues[0].ID != "cached-issue" {
		t.Fatalf("Latest() = %#v, %v; want live snapshot with cached tracker state", got, ok)
	}
	if got.Tracker.Source != telemetry.SnapshotSourceCached || got.Runtime.Source != telemetry.SnapshotSourceUnknown {
		t.Fatalf("Latest() provenance = tracker %#v, runtime %#v", got.Tracker, got.Runtime)
	}
}

func TestPublishSnapshotOnceReplacesLastKnownStatePerProject(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name              string
		startAlpha        bool
		wantIssues        []string
		wantTrackerSource telemetry.SnapshotSource
		wantRuntimeSource telemetry.SnapshotSource
	}{
		{
			name:              "all projects initializing",
			wantIssues:        []string{"cached-alpha", "cached-bravo"},
			wantTrackerSource: telemetry.SnapshotSourceCached,
			wantRuntimeSource: telemetry.SnapshotSourceUnknown,
		},
		{
			name:              "ready project replaces its cached state",
			startAlpha:        true,
			wantIssues:        []string{"live-alpha", "cached-bravo"},
			wantTrackerSource: telemetry.SnapshotSourceMixed,
			wantRuntimeSource: telemetry.SnapshotSourceMixed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := projectpkg.NewRegistry()
			alpha := newRefreshProjectWithConnector(t, "alpha", memory.New(memory.Config{Issues: []connector.Issue{{
				ID:         "live-alpha",
				Identifier: "example/alpha#1",
				State:      "Todo",
			}}}))
			if tt.startAlpha {
				if err := alpha.Start(context.Background()); err != nil {
					t.Fatalf("alpha.Start() error = %v", err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := alpha.Stop(ctx); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
						t.Fatalf("alpha.Stop() error = %v", err)
					}
				})
				waitForProjectDataSeq(t, alpha, 1)
			}
			mustSetProject(t, registry, alpha)
			mustSetProject(t, registry, newRefreshProjectWithConnector(t, "bravo", memory.New(memory.Config{})))

			cached := telemetry.Snapshot{
				LastKnown:      true,
				LastKnownUntil: now.Add(5 * time.Minute),
				GeneratedAt:    now.Add(-time.Minute),
				Refresh:        telemetry.Refresh{Status: telemetry.RefreshStatusReady},
				Projects: []telemetry.ProjectSnapshot{
					{Project: telemetry.Project{ID: "alpha"}, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusReady}},
					{Project: telemetry.Project{ID: "bravo"}, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusReady}},
				},
				BoardIssues: []telemetry.Issue{
					{ID: "cached-alpha", ProjectID: "alpha", State: "Todo"},
					{ID: "cached-bravo", ProjectID: "bravo", State: "Todo"},
				},
				StalenessWarnings: []telemetry.StalenessWarning{
					{ID: "cached-alpha-warning", ProjectID: "alpha"},
					{ID: "cached-bravo-warning", ProjectID: "bravo"},
				},
			}
			snapshotHub := hub.New[telemetry.Snapshot]()
			if err := snapshotHub.Publish(cached); err != nil {
				t.Fatalf("Publish(cached) error = %v", err)
			}

			var seq atomic.Uint64
			if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "", nil); err != nil {
				t.Fatalf("publishSnapshotOnce() error = %v", err)
			}
			got, ok := snapshotHub.Latest()
			if !ok {
				t.Fatal("Latest() ok = false, want composite snapshot")
			}
			if got.LastKnown {
				t.Fatal("Latest().LastKnown = true, want per-project startup state")
			}
			if len(got.StalenessWarnings) != 0 {
				t.Fatalf("Latest().StalenessWarnings = %#v, want routine startup warnings excluded", got.StalenessWarnings)
			}
			issues := make([]string, 0, len(got.BoardIssues))
			for _, issue := range got.BoardIssues {
				issues = append(issues, issue.ID)
			}
			if !reflect.DeepEqual(issues, tt.wantIssues) {
				t.Fatalf("Latest().BoardIssues = %v, want %v", issues, tt.wantIssues)
			}
			if got.Tracker.Source != tt.wantTrackerSource || got.Runtime.Source != tt.wantRuntimeSource {
				t.Fatalf("Latest() provenance = tracker %#v, runtime %#v", got.Tracker, got.Runtime)
			}
			if got.Runtime.Complete {
				t.Fatal("Latest().Runtime.Complete = true while a project is initializing")
			}
			projectSources := map[string]telemetry.SnapshotSource{}
			for _, project := range got.Projects {
				projectSources[project.Project.ID] = project.Tracker.Source
			}
			if tt.startAlpha && projectSources["alpha"] != telemetry.SnapshotSourceLive {
				t.Fatalf("alpha tracker source = %q, want live", projectSources["alpha"])
			}
			if projectSources["bravo"] != telemetry.SnapshotSourceCached {
				t.Fatalf("bravo tracker source = %q, want cached", projectSources["bravo"])
			}
		})
	}
}

func TestPublishSnapshotOnceExpiresLastKnownSnapshotDuringHydration(t *testing.T) {
	t.Parallel()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	trackedProject, err := projectpkg.New(projectpkg.Config{
		Project:  globalconfig.Project{ID: "alpha", Workdir: t.TempDir(), Weight: 1},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, trackedProject)

	expiresAt := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	snapshotHub := hub.New[telemetry.Snapshot]()
	if err := snapshotHub.Publish(telemetry.Snapshot{
		LastKnown:      true,
		LastKnownUntil: expiresAt,
		GeneratedAt:    expiresAt.Add(-time.Minute),
		Refresh:        telemetry.Refresh{Status: telemetry.RefreshStatusReady},
		BoardIssues:    []telemetry.Issue{{ID: "expired-issue"}},
	}); err != nil {
		t.Fatalf("Publish(cached) error = %v", err)
	}
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, expiresAt, nil, nil, "", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	got, ok := snapshotHub.Latest()
	if !ok || got.LastKnown || len(got.BoardIssues) != 0 || got.Refresh.ReadinessStatus() != telemetry.RefreshStatusInitializing {
		t.Fatalf("Latest() = %#v, %v; want initializing live snapshot after cache expiry", got, ok)
	}
}

func TestPersistBoardSnapshotsWritesLatestEligibleSnapshot(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	snapshotHub := hub.New[telemetry.Snapshot]()
	cache := &boardSnapshotStoreStub{saved: make(chan telemetry.Snapshot, 1)}
	done := make(chan struct{})
	go func() {
		persistBoardSnapshots(ctx, snapshotHub, cache, time.Millisecond, nil)
		close(done)
	}()

	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	if err := snapshotHub.Publish(telemetry.Snapshot{
		GeneratedAt: now.Add(-time.Second),
		Refresh:     telemetry.Refresh{Status: telemetry.RefreshStatusInitializing},
	}); err != nil {
		t.Fatalf("Publish(initializing) error = %v", err)
	}
	if err := snapshotHub.Publish(telemetry.Snapshot{
		Seq:         2,
		GeneratedAt: now,
		Refresh:     telemetry.Refresh{Status: telemetry.RefreshStatusReady},
	}); err != nil {
		t.Fatalf("Publish(ready) error = %v", err)
	}

	select {
	case got := <-cache.saved:
		if got.Seq != 2 {
			t.Fatalf("saved snapshot Seq = %d, want 2", got.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for board snapshot persistence")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for board snapshot persistence shutdown")
	}
}

type boardSnapshotStoreStub struct {
	saved chan telemetry.Snapshot
}

func (s *boardSnapshotStoreStub) Load(context.Context) (telemetry.Snapshot, bool, error) {
	return telemetry.Snapshot{}, false, nil
}

func (s *boardSnapshotStoreStub) Save(_ context.Context, snapshot telemetry.Snapshot) error {
	s.saved <- snapshot
	return nil
}

func TestPublishSnapshotOncePreservesProjectDataSeq(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	alpha := startRefreshProject(t, "alpha")
	bravo := startRefreshProject(t, "bravo")
	mustSetProject(t, registry, alpha)
	mustSetProject(t, registry, bravo)

	alphaSeq := waitForProjectDataSeq(t, alpha, 1)
	if _, err := alpha.Orchestrator().RequestRefresh(context.Background()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	alphaSeq = waitForProjectDataSeq(t, alpha, alphaSeq+1)
	bravoSeq := waitForProjectDataSeq(t, bravo, 1)

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	now := time.Date(2026, 7, 8, 12, 15, 0, 0, time.UTC)
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}

	got := map[string]uint64{}
	for _, projectSnapshot := range snapshot.Projects {
		got[projectSnapshot.Project.ID] = projectSnapshot.Refresh.DataSeq
	}
	if got["alpha"] != alphaSeq {
		t.Fatalf("alpha DataSeq = %d, want %d; projects = %#v", got["alpha"], alphaSeq, snapshot.Projects)
	}
	if got["bravo"] != bravoSeq {
		t.Fatalf("bravo DataSeq = %d, want %d; projects = %#v", got["bravo"], bravoSeq, snapshot.Projects)
	}
}

func TestPublishSnapshotOnceDoesNotLetPausedProjectsHoldFleetReadiness(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, startRefreshProject(t, "alpha"))

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	pausedProject, err := projectpkg.New(projectpkg.Config{
		Project: globalconfig.Project{
			ID:      "bravo",
			Workdir: t.TempDir(),
			Weight:  1,
			Paused:  true,
		},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	mustSetProject(t, registry, pausedProject)

	snapshotHub := hub.New[telemetry.Snapshot]()
	now := time.Date(2026, 6, 22, 14, 0, 0, 0, time.UTC)
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusReady {
		t.Fatalf("snapshot Refresh status = %q, want %q", got, telemetry.RefreshStatusReady)
	}
	if len(snapshot.Projects) != 2 {
		t.Fatalf("snapshot.Projects len = %d, want 2", len(snapshot.Projects))
	}

	var paused telemetry.ProjectSnapshot
	for _, projectSnapshot := range snapshot.Projects {
		if projectSnapshot.Project.ID == "bravo" {
			paused = projectSnapshot
			break
		}
	}
	if paused.Project.ID == "" {
		t.Fatalf("paused project snapshot missing: %#v", snapshot.Projects)
	}
	if refreshHasSignal(paused.Refresh) {
		t.Fatalf("paused project Refresh = %#v, want no readiness signal", paused.Refresh)
	}
}

func TestPublishSnapshotOnceSurfacesProjectStartupError(t *testing.T) {
	t.Parallel()

	provisionErr := errors.New("provision failed")
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Tracker.AutoProvision = true
	failedProject, err := projectpkg.New(projectpkg.Config{
		Project: globalconfig.Project{
			ID:      "bravo",
			Workdir: t.TempDir(),
			Weight:  1,
		},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{
		Connector: bootProvisioningConnector{provision: func(context.Context) error {
			return provisionErr
		}},
	})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	if err := failedProject.Start(context.Background()); !errors.Is(err, provisionErr) {
		t.Fatalf("Project.Start() error = %v, want %v", err, provisionErr)
	}

	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, failedProject)
	snapshotHub := hub.New[telemetry.Snapshot]()
	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if len(snapshot.Projects) != 1 {
		t.Fatalf("snapshot.Projects len = %d, want 1", len(snapshot.Projects))
	}
	refresh := snapshot.Projects[0].Refresh
	if got := refresh.ReadinessStatus(); got != telemetry.RefreshStatusDegraded {
		t.Fatalf("project Refresh status = %q, want %q; refresh = %#v", got, telemetry.RefreshStatusDegraded, refresh)
	}
	if !strings.Contains(refresh.LastError, "provision failed") {
		t.Fatalf("project Refresh LastError = %q, want provision failure", refresh.LastError)
	}
	if refresh.LastErrorAt == nil {
		t.Fatal("project Refresh LastErrorAt = nil, want timestamp")
	}
}

func TestPublishSnapshotOnceSurfacesPendingConnectorRetry(t *testing.T) {
	t.Parallel()

	lastErrorAt := time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC)
	nextRetryAt := lastErrorAt.Add(30 * time.Second)
	registry := projectpkg.NewRegistry()
	if err := registry.SetPending(globalconfig.Project{ID: "alpha"}, projectpkg.RuntimeError{
		Message:     "create project connector: github transient error",
		At:          lastErrorAt,
		NextRetryAt: nextRetryAt,
	}); err != nil {
		t.Fatalf("Registry.SetPending() error = %v", err)
	}

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	if err := publishSnapshotOnce(
		context.Background(),
		registry,
		nil,
		snapshotHub,
		&seq,
		nil,
		lastErrorAt,
		nil,
		nil,
		"http://localhost:4101",
		nil,
	); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok || len(snapshot.Projects) != 1 {
		t.Fatalf("snapshot = %#v, want one pending project", snapshot)
	}
	refresh := snapshot.Projects[0].Refresh
	if refresh.ReadinessStatus() != telemetry.RefreshStatusDegraded {
		t.Fatalf("Refresh status = %q, want degraded", refresh.ReadinessStatus())
	}
	if refresh.LastError != "create project connector: github transient error" {
		t.Fatalf("Refresh LastError = %q", refresh.LastError)
	}
	if refresh.NextRefreshAt == nil || !refresh.NextRefreshAt.Equal(nextRetryAt) {
		t.Fatalf("Refresh NextRefreshAt = %v, want %v", refresh.NextRefreshAt, nextRetryAt)
	}
}

func TestPublishSnapshotOnceIsolatesInvalidWorkflowProject(t *testing.T) {
	t.Parallel()

	const validationError = "create project pyroapex: validate project workflow: schedule_ownership.enabled must be true when scheduled work is configured"
	now := time.Date(2026, 9, 3, 14, 0, 0, 0, time.UTC)
	registry := projectpkg.NewRegistry()
	healthy := startRefreshProject(t, "detent")
	mustSetProject(t, registry, healthy)
	waitForProjectDataSeq(t, healthy, 1)
	if err := registry.SetPending(globalconfig.Project{ID: "pyroapex"}, projectpkg.RuntimeError{
		Message:  validationError,
		At:       now.Add(-time.Minute),
		Terminal: true,
	}); err != nil {
		t.Fatalf("Registry.SetPending() error = %v", err)
	}

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusPartial {
		t.Fatalf("fleet Refresh status = %q, want partial", got)
	}
	if !snapshot.Tracker.Available() || snapshot.Tracker.Complete {
		t.Fatalf("fleet Tracker = %#v, want available partial data", snapshot.Tracker)
	}
	projects := make(map[string]telemetry.ProjectSnapshot, len(snapshot.Projects))
	for _, projectSnapshot := range snapshot.Projects {
		projects[projectSnapshot.Project.ID] = projectSnapshot
	}
	if got := projects["detent"]; !got.Refresh.Ready() || !got.Tracker.Available() || !got.Tracker.Complete {
		t.Fatalf("healthy project snapshot = %#v, want current tracker data", got)
	}
	if got := projects["pyroapex"]; !got.Refresh.Degraded() || got.Refresh.LastError != validationError || got.Tracker.Available() {
		t.Fatalf("invalid project snapshot = %#v, want unavailable validation failure", got)
	}
	failures := snapshot.RefreshFailures()
	if len(failures) != 1 || failures[0].ProjectID != "pyroapex" || failures[0].LastError != validationError {
		t.Fatalf("RefreshFailures() = %#v, want pyroapex validation failure", failures)
	}

	recovered := startRefreshProject(t, "pyroapex")
	waitForProjectDataSeq(t, recovered, 1)
	mustSetProject(t, registry, recovered)
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now.Add(time.Minute), nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce(recovered) error = %v", err)
	}
	recoveredSnapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() after recovery ok = false, want published snapshot")
	}
	if got := recoveredSnapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusReady {
		t.Fatalf("recovered fleet Refresh status = %q, want ready", got)
	}
	if failures := recoveredSnapshot.RefreshFailures(); len(failures) != 0 {
		t.Fatalf("recovered RefreshFailures() = %#v, want none", failures)
	}
	if !recoveredSnapshot.Tracker.Available() || !recoveredSnapshot.Tracker.Complete {
		t.Fatalf("recovered fleet Tracker = %#v, want complete live data", recoveredSnapshot.Tracker)
	}
}

func TestMergeRefreshAggregatesProjectReadiness(t *testing.T) {
	t.Parallel()

	ready := telemetry.Refresh{Status: telemetry.RefreshStatusReady}
	degraded := telemetry.Refresh{Status: telemetry.RefreshStatusDegraded, LastError: "workflow invalid"}
	partial := telemetry.Refresh{Status: telemetry.RefreshStatusPartial, LastError: "workflow invalid"}
	tests := []struct {
		name  string
		left  telemetry.Refresh
		right telemetry.Refresh
		want  telemetry.RefreshStatus
	}{
		{name: "healthy projects stay ready", left: ready, right: ready, want: telemetry.RefreshStatusReady},
		{name: "healthy then degraded is partial", left: ready, right: degraded, want: telemetry.RefreshStatusPartial},
		{name: "degraded then healthy is partial", left: degraded, right: ready, want: telemetry.RefreshStatusPartial},
		{name: "partial remains partial", left: partial, right: ready, want: telemetry.RefreshStatusPartial},
		{name: "all projects degraded stays fail closed", left: degraded, right: degraded, want: telemetry.RefreshStatusDegraded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := mergeRefresh(tt.left, tt.right).ReadinessStatus(); got != tt.want {
				t.Fatalf("mergeRefresh() status = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPublishSnapshotOnceFailsClosedWhenEveryProjectIsInvalid(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 15, 0, 0, 0, time.UTC)
	registry := projectpkg.NewRegistry()
	for _, id := range []string{"detent", "pyroapex"} {
		if err := registry.SetPending(globalconfig.Project{ID: id}, projectpkg.RuntimeError{
			Message:  "create project " + id + ": validate project workflow: workflow invalid",
			At:       now,
			Terminal: true,
		}); err != nil {
			t.Fatalf("Registry.SetPending(%q) error = %v", id, err)
		}
	}

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}
	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusDegraded {
		t.Fatalf("fleet Refresh status = %q, want degraded", got)
	}
	if snapshot.Tracker.Available() || snapshot.Runtime.Available() {
		t.Fatalf("fleet sections = tracker %#v runtime %#v, want unavailable", snapshot.Tracker, snapshot.Runtime)
	}
	if failures := snapshot.RefreshFailures(); len(failures) != 2 {
		t.Fatalf("RefreshFailures() = %#v, want both invalid projects", failures)
	}
}

type agentPoolSnapshotSourceStub []scheduler.PoolSnapshot

func (s agentPoolSnapshotSourceStub) PoolSnapshots() []scheduler.PoolSnapshot {
	return append([]scheduler.PoolSnapshot(nil), s...)
}

func TestPublishSnapshotOncePausedProjectSuppressesStartupError(t *testing.T) {
	t.Parallel()

	provisionErr := errors.New("provision failed")
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Tracker.AutoProvision = true
	failedProject, err := projectpkg.New(projectpkg.Config{
		Project: globalconfig.Project{
			ID:      "bravo",
			Workdir: t.TempDir(),
			Weight:  1,
		},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{
		Connector: bootProvisioningConnector{provision: func(context.Context) error {
			return provisionErr
		}},
	})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	if err := failedProject.Start(context.Background()); !errors.Is(err, provisionErr) {
		t.Fatalf("Project.Start() error = %v, want %v", err, provisionErr)
	}
	if err := failedProject.Pause(context.Background()); err != nil {
		t.Fatalf("Project.Pause() error = %v", err)
	}

	registry := projectpkg.NewRegistry()
	healthy := startRefreshProject(t, "alpha")
	waitForProjectDataSeq(t, healthy, 1)
	mustSetProject(t, registry, healthy)
	mustSetProject(t, registry, failedProject)
	snapshotHub := hub.New[telemetry.Snapshot]()
	now := time.Date(2026, 6, 23, 14, 30, 0, 0, time.UTC)
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusReady {
		t.Fatalf("snapshot Refresh status = %q, want %q", got, telemetry.RefreshStatusReady)
	}
	var paused telemetry.ProjectSnapshot
	for _, projectSnapshot := range snapshot.Projects {
		if projectSnapshot.Project.ID == "bravo" {
			paused = projectSnapshot
			break
		}
	}
	if paused.Project.ID == "" {
		t.Fatalf("paused project snapshot missing: %#v", snapshot.Projects)
	}
	if refreshHasSignal(paused.Refresh) {
		t.Fatalf("paused project Refresh = %#v, want no readiness signal", paused.Refresh)
	}
}

func TestRepublishSnapshotsOnProjectEventsPublishesLatestSnapshot(t *testing.T) {
	t.Parallel()

	events := hub.New[projectpkg.Event]()
	snapshotHub := hub.New[telemetry.Snapshot](hub.WithBuffer(2))
	now := time.Date(2026, 6, 20, 15, 30, 0, 0, time.UTC)
	latest := telemetry.Snapshot{
		GeneratedAt: now,
		Counts:      telemetry.Counts{Running: 1},
	}
	if err := snapshotHub.Publish(latest); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := snapshotHub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()
	receiveSnapshot(t, sub.C())

	done := make(chan struct{})
	go func() {
		defer close(done)
		republishSnapshotsOnProjectEvents(ctx, events, snapshotHub, nil)
	}()

	if err := events.Publish(projectpkg.Event{
		ProjectID: "alpha",
		Kind:      projectpkg.EventWorkflowReloaded,
		At:        now,
	}); err != nil {
		t.Fatalf("events.Publish() error = %v", err)
	}

	republished := receiveSnapshot(t, sub.C())
	if !republished.GeneratedAt.Equal(now) || republished.Counts.Running != 1 {
		t.Fatalf("republished snapshot = %#v, want latest snapshot", republished)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for project event republisher to stop")
	}
}

func TestRepublishSnapshotsOnProjectEventsLogsPauseTransitions(t *testing.T) {
	t.Parallel()

	events := hub.New[projectpkg.Event]()
	snapshotHub := hub.New[telemetry.Snapshot](hub.WithBuffer(3))
	if err := snapshotHub.Publish(telemetry.Snapshot{}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := snapshotHub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()
	receiveSnapshot(t, sub.C())

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	done := make(chan struct{})
	go func() {
		defer close(done)
		republishSnapshotsOnProjectEvents(ctx, events, snapshotHub, logger)
	}()

	tests := []struct {
		name    string
		kind    projectpkg.EventKind
		wantLog string
	}{
		{name: "paused", kind: projectpkg.EventPaused, wantLog: "level=INFO msg=\"project paused\" project_id=alpha"},
		{name: "unpaused", kind: projectpkg.EventUnpaused, wantLog: "level=INFO msg=\"project unpaused\" project_id=alpha"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := events.Publish(projectpkg.Event{
				ProjectID: "alpha",
				Kind:      tt.kind,
			}); err != nil {
				t.Fatalf("events.Publish() error = %v", err)
			}
			receiveSnapshot(t, sub.C())
			if got := logs.String(); !strings.Contains(got, tt.wantLog) {
				t.Fatalf("logs missing %q: %s", tt.wantLog, got)
			}
		})
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for project event republisher to stop")
	}
}

func TestPublishSnapshotOncePreservesPipeline(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := now.Add(-7 * time.Minute)
	pipelineIssue := connector.Issue{
		ID:         "i-212",
		Identifier: "digitaldrywood/detent#212",
		Title:      "Add PR pipeline lanes",
		State:      "Human Review",
		UpdatedAt:  &updatedAt,
		PullRequest: &connector.PullRequest{
			Number:           218,
			URL:              "https://github.com/digitaldrywood/detent/pull/218",
			State:            "OPEN",
			CIStatus:         "pending",
			CodexReviewState: "P1",
		},
	}
	project := newRefreshProjectWithConnector(t, "alpha", memory.New(memory.Config{
		Issues: []connector.Issue{pipelineIssue},
	}))
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := project.Stop(ctx); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Stop() error = %v", err)
		}
	})
	waitForProjectDataSeq(t, project, 1)
	mustSetProject(t, registry, project)

	snapshotHub := hub.New[telemetry.Snapshot]()
	var seq atomic.Uint64
	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, nil, now, nil, nil, "http://localhost:4101", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}

	snapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("snapshotHub.Latest() ok = false, want published snapshot")
	}
	if len(snapshot.Pipeline) != 1 {
		t.Fatalf("Pipeline len = %d, want 1", len(snapshot.Pipeline))
	}
	got := snapshot.Pipeline[0]
	if got.ID != "i-212" || got.State != "Human Review" || got.Title != "Add PR pipeline lanes" {
		t.Fatalf("Pipeline[0] = %#v, want issue #212 in Human Review", got)
	}
	if got.UpdatedAt == nil || !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("Pipeline[0].UpdatedAt = %v, want %v", got.UpdatedAt, updatedAt)
	}
	if got.PullRequest == nil || got.PullRequest.Number != 218 || got.PullRequest.CIStatus != "pending" || got.PullRequest.CodexReviewState != "P1" {
		t.Fatalf("Pipeline[0].PullRequest = %#v, want PR #218 pending with P1 review", got.PullRequest)
	}
}

func TestMergeSnapshotMergesInstanceScope(t *testing.T) {
	t.Parallel()

	base := telemetry.Snapshot{
		Instance: telemetry.Instance{
			Name:               "release-captain",
			GitHubLogin:        "detent-bot",
			AuthorizationScope: "All issues",
		},
	}
	got := mergeSnapshot(telemetry.Snapshot{}, base)
	if got.Instance != base.Instance {
		t.Fatalf("first merge Instance = %#v, want %#v", got.Instance, base.Instance)
	}

	got = mergeSnapshot(got, telemetry.Snapshot{
		Instance: telemetry.Instance{
			Name:                    "release-captain",
			GitHubLogin:             "detent-bot",
			AuthorizationScope:      "labels include release",
			AuthorizationConfigured: true,
		},
	})
	want := telemetry.Instance{
		Name:                    "release-captain",
		GitHubLogin:             "detent-bot",
		AuthorizationScope:      "Multiple authorization scopes",
		AuthorizationConfigured: true,
	}
	if got.Instance != want {
		t.Fatalf("merged Instance = %#v, want %#v", got.Instance, want)
	}
}

func TestMergeSnapshotUsesNewestHostPressureObservations(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	newer := older.Add(time.Second)
	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		MemoryPressure: telemetry.MemoryPressure{ObservedAt: newer, SomeAvg60Max: 10},
		IOPressure:     telemetry.IOPressure{ObservedAt: older, FullAvg10Max: 5},
		CPUPressure:    telemetry.CPUPressure{ObservedAt: newer, SomeAvg10Max: 80},
	})
	got = mergeSnapshot(got, telemetry.Snapshot{
		MemoryPressure: telemetry.MemoryPressure{ObservedAt: older, SomeAvg60Max: 11},
		IOPressure:     telemetry.IOPressure{ObservedAt: newer, FullAvg10Max: 6},
		CPUPressure:    telemetry.CPUPressure{ObservedAt: older, SomeAvg10Max: 81},
	})

	if got.MemoryPressure.SomeAvg60Max != 10 {
		t.Fatalf("memory pressure threshold = %.2f, want newest value 10", got.MemoryPressure.SomeAvg60Max)
	}
	if got.IOPressure.FullAvg10Max != 6 {
		t.Fatalf("IO pressure threshold = %.2f, want newest value 6", got.IOPressure.FullAvg10Max)
	}
	if got.CPUPressure.SomeAvg10Max != 80 {
		t.Fatalf("CPU pressure threshold = %.2f, want newest value 80", got.CPUPressure.SomeAvg10Max)
	}
}

func TestMergeSnapshotAttributesFleetRESTBudgets(t *testing.T) {
	t.Parallel()

	firstObserved := time.Date(2026, 8, 8, 18, 0, 0, 0, time.UTC)
	secondObserved := firstObserved.Add(time.Minute)
	coreReset := firstObserved.Add(time.Hour)
	searchReset := firstObserved.Add(2 * time.Minute)
	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Limit: 5000, Remaining: 4200, ObservedAt: &firstObserved},
			GitHubRESTBudgets: []telemetry.RESTBudget{
				{CredentialIdentity: "github-rest:shared", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 4200, ResetAt: &coreReset, ObservedAt: &firstObserved},
			},
			RESTUsage: &telemetry.RESTUsage{
				TotalRequests: 3, BillableRequests: 3,
				Divergences: []telemetry.RESTUsageDivergence{{CredentialIdentity: "github-rest:shared", Resource: "core", ObservedRequests: 11, AttributedRequests: 10, LastObservedAt: &firstObserved, ResetAt: &coreReset}},
			},
		},
	})
	got = mergeSnapshot(got, telemetry.Snapshot{
		BackendOutages: []telemetry.BackendOutage{{Kind: "github_rest_rate_limit", Reason: "dispatch paused"}},
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Limit: 5000, Remaining: 300, ObservedAt: &secondObserved},
			GitHubRESTBudgets: []telemetry.RESTBudget{
				{CredentialIdentity: "github-rest:shared", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 300, ResetAt: &coreReset, ObservedAt: &secondObserved},
				{CredentialIdentity: "github-rest:search", EndpointFamily: "issue search", Resource: "search", Limit: 30, Remaining: 20, ResetAt: &searchReset, ObservedAt: &secondObserved},
			},
			RESTUsage: &telemetry.RESTUsage{
				TotalRequests: 2, BillableRequests: 1,
				Divergences: []telemetry.RESTUsageDivergence{{CredentialIdentity: "github-rest:shared", Resource: "core", ObservedRequests: 22, AttributedRequests: 20, LastObservedAt: &secondObserved, ResetAt: &coreReset}},
			},
		},
	})

	if got.RateLimits == nil || len(got.RateLimits.GitHubRESTBudgets) != 2 {
		t.Fatalf("RateLimits = %#v, want two credential endpoint budgets", got.RateLimits)
	}
	if budget := got.RateLimits.GitHubRESTBudgets[0]; budget.CredentialIdentity != "github-rest:search" || budget.ResetAt == nil || !budget.ResetAt.Equal(searchReset) {
		t.Fatalf("GitHubRESTBudgets[0] = %#v, want search credential and reset", budget)
	}
	if budget := got.RateLimits.GitHubRESTBudgets[1]; budget.CredentialIdentity != "github-rest:shared" || budget.Remaining != 300 || budget.ResetAt == nil || !budget.ResetAt.Equal(coreReset) {
		t.Fatalf("GitHubRESTBudgets[1] = %#v, want latest shared credential core budget", budget)
	}
	if got.RateLimits.RESTUsage == nil || got.RateLimits.RESTUsage.TotalRequests != 5 || got.RateLimits.RESTUsage.BillableRequests != 4 {
		t.Fatalf("RESTUsage = %#v, want aggregated project request counts", got.RateLimits.RESTUsage)
	}
	if divergences := got.RateLimits.RESTUsage.Divergences; len(divergences) != 1 || divergences[0].AttributedRequests != 20 {
		t.Fatalf("RESTUsage.Divergences = %#v, want latest shared registry counters without double counting", divergences)
	}
	if got.RateLimits.GitHubREST == nil || got.RateLimits.GitHubREST.Remaining != 300 {
		t.Fatalf("GitHubREST = %#v, want most constrained compatibility bucket", got.RateLimits.GitHubREST)
	}
	if len(got.BackendOutages) != 1 || got.BackendOutages[0].Kind != "github_rest_rate_limit" {
		t.Fatalf("BackendOutages = %#v, want fleet-visible REST dispatch condition", got.BackendOutages)
	}
}

func TestFleetRESTBucketFromBudgetsUsesCurrentCredentialResourceSnapshot(t *testing.T) {
	t.Parallel()

	oldObserved := time.Date(2026, 8, 8, 18, 0, 0, 0, time.UTC)
	newObserved := oldObserved.Add(time.Hour)
	oldReset := oldObserved.Add(30 * time.Minute)
	newReset := newObserved.Add(time.Hour)
	tests := []struct {
		name          string
		budgets       []telemetry.RESTBudget
		wantRemaining int64
		wantReset     time.Time
	}{
		{
			name: "new post-reset observation replaces stale exhausted family",
			budgets: []telemetry.RESTBudget{
				{CredentialIdentity: "shared", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 0, ResetAt: &oldReset, ObservedAt: &oldObserved},
				{CredentialIdentity: "shared", EndpointFamily: "pull requests", Resource: "core", Limit: 5000, Remaining: 4990, ResetAt: &newReset, ObservedAt: &newObserved},
			},
			wantRemaining: 4990,
			wantReset:     newReset,
		},
		{
			name: "distinct credential keeps most constrained core budget",
			budgets: []telemetry.RESTBudget{
				{CredentialIdentity: "first", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 4000, ResetAt: &newReset, ObservedAt: &newObserved},
				{CredentialIdentity: "second", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 300, ResetAt: &newReset, ObservedAt: &newObserved},
			},
			wantRemaining: 300,
			wantReset:     newReset,
		},
		{
			name: "core compatibility bucket outranks search resource",
			budgets: []telemetry.RESTBudget{
				{CredentialIdentity: "shared", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 4000, ResetAt: &newReset, ObservedAt: &newObserved},
				{CredentialIdentity: "shared", EndpointFamily: "issue search", Resource: "search", Limit: 30, Remaining: 0, ResetAt: &oldReset, ObservedAt: &newObserved},
			},
			wantRemaining: 4000,
			wantReset:     newReset,
		},
		{
			name: "exhausted worker budget does not constrain orchestrator dispatch",
			budgets: []telemetry.RESTBudget{
				{Consumer: telemetry.RESTConsumerOrchestrator, CredentialIdentity: "orchestrator", EndpointFamily: "issues", Resource: "core", Limit: 5000, Remaining: 4000, ResetAt: &newReset, ObservedAt: &newObserved},
				{Consumer: telemetry.RESTConsumerWorker, CredentialIdentity: "worker", EndpointFamily: "worker credential", Resource: "core", Limit: 5000, Remaining: 0, ResetAt: &newReset, ObservedAt: &newObserved},
			},
			wantRemaining: 4000,
			wantReset:     newReset,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bucket := fleetRESTBucketFromBudgets(test.budgets)
			if bucket == nil || bucket.Remaining != test.wantRemaining || bucket.ResetAt == nil || !bucket.ResetAt.Equal(test.wantReset) {
				t.Fatalf("bucket = %#v, want remaining %d reset %v", bucket, test.wantRemaining, test.wantReset)
			}
		})
	}
}

func TestDedupeSnapshotIssues(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	laterCompletedAt := completedAt.Add(time.Hour)
	tests := []struct {
		name           string
		snapshot       telemetry.Snapshot
		wantRunning    []string
		wantQueue      []string
		wantBlocked    []string
		wantCompleted  []string
		wantCounts     telemetry.Counts
		wantEmptyCount int
	}{
		{
			name: "dedupes completed issue across projects and reduces capped count",
			snapshot: telemetry.Snapshot{
				Completed: []telemetry.Completed{
					{Issue: telemetry.Issue{URL: " https://github.com/digitaldrywood/detent/issues/1171 ", ProjectID: "first"}, CompletedAt: completedAt},
					{Issue: telemetry.Issue{URL: "https://github.com/digitaldrywood/detent/issues/1171", ProjectID: "second"}, CompletedAt: completedAt},
				},
				Counts: telemetry.Counts{Completed: 7},
			},
			wantCompleted: []string{"first"},
			wantCounts:    telemetry.Counts{Completed: 6},
		},
		{
			name: "dedupes running issue across scopes and floors count at slice length",
			snapshot: telemetry.Snapshot{
				Running: []telemetry.Running{
					{Issue: telemetry.Issue{Identifier: " digitaldrywood/detent#1171 ", ProjectID: "first"}},
					{Issue: telemetry.Issue{Identifier: "digitaldrywood/detent#1171", ProjectID: "second"}},
				},
			},
			wantRunning: []string{"first"},
			wantCounts:  telemetry.Counts{Running: 1},
		},
		{
			name: "dedupes queued and blocked issues",
			snapshot: telemetry.Snapshot{
				Queue: []telemetry.Queued{
					{Issue: telemetry.Issue{ID: "queued", ProjectID: "first"}},
					{Issue: telemetry.Issue{ID: "queued", ProjectID: "second"}},
				},
				Blocked: []telemetry.Blocked{
					{Issue: telemetry.Issue{ID: "blocked", ProjectID: "first"}},
					{Issue: telemetry.Issue{ID: "blocked", ProjectID: "second"}},
				},
				Counts: telemetry.Counts{Queue: 5, Blocked: 2},
			},
			wantQueue:   []string{"first"},
			wantBlocked: []string{"first"},
			wantCounts:  telemetry.Counts{Queue: 4, Blocked: 1},
		},
		{
			name: "preserves distinct issues",
			snapshot: telemetry.Snapshot{
				Running: []telemetry.Running{
					{Issue: telemetry.Issue{ID: "first", ProjectID: "first"}},
					{Issue: telemetry.Issue{ID: "second", ProjectID: "second"}},
				},
				Counts: telemetry.Counts{Running: 2},
			},
			wantRunning: []string{"first", "second"},
			wantCounts:  telemetry.Counts{Running: 2},
		},
		{
			name: "retains empty issue keys",
			snapshot: telemetry.Snapshot{
				Completed: []telemetry.Completed{
					{Issue: telemetry.Issue{ProjectID: "first"}, CompletedAt: completedAt},
					{Issue: telemetry.Issue{URL: " ", Identifier: " ", ID: " ", ProjectID: "second"}, CompletedAt: laterCompletedAt},
				},
				Counts: telemetry.Counts{Completed: 2},
			},
			wantCompleted:  []string{"first", "second"},
			wantCounts:     telemetry.Counts{Completed: 2},
			wantEmptyCount: 2,
		},
		{
			name: "keeps latest completed entry and survivor order",
			snapshot: telemetry.Snapshot{
				Completed: []telemetry.Completed{
					{Issue: telemetry.Issue{ID: "duplicate", ProjectID: "older"}, CompletedAt: completedAt},
					{Issue: telemetry.Issue{ID: "distinct", ProjectID: "middle"}, CompletedAt: completedAt},
					{Issue: telemetry.Issue{ID: "duplicate", ProjectID: "latest"}, CompletedAt: laterCompletedAt},
				},
				Counts: telemetry.Counts{Completed: 3},
			},
			wantCompleted: []string{"middle", "latest"},
			wantCounts:    telemetry.Counts{Completed: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := dedupeSnapshotIssues(tt.snapshot)
			if got.Counts != tt.wantCounts {
				t.Fatalf("Counts = %#v, want %#v", got.Counts, tt.wantCounts)
			}
			if len(got.Running) != len(tt.wantRunning) {
				t.Fatalf("Running len = %d, want %d", len(got.Running), len(tt.wantRunning))
			}
			for i, wantProjectID := range tt.wantRunning {
				if got.Running[i].ProjectID != wantProjectID {
					t.Fatalf("Running[%d].ProjectID = %q, want %q", i, got.Running[i].ProjectID, wantProjectID)
				}
			}
			if len(got.Queue) != len(tt.wantQueue) {
				t.Fatalf("Queue len = %d, want %d", len(got.Queue), len(tt.wantQueue))
			}
			for i, wantProjectID := range tt.wantQueue {
				if got.Queue[i].ProjectID != wantProjectID {
					t.Fatalf("Queue[%d].ProjectID = %q, want %q", i, got.Queue[i].ProjectID, wantProjectID)
				}
			}
			if len(got.Blocked) != len(tt.wantBlocked) {
				t.Fatalf("Blocked len = %d, want %d", len(got.Blocked), len(tt.wantBlocked))
			}
			for i, wantProjectID := range tt.wantBlocked {
				if got.Blocked[i].ProjectID != wantProjectID {
					t.Fatalf("Blocked[%d].ProjectID = %q, want %q", i, got.Blocked[i].ProjectID, wantProjectID)
				}
			}
			if len(got.Completed) != len(tt.wantCompleted) {
				t.Fatalf("Completed len = %d, want %d", len(got.Completed), len(tt.wantCompleted))
			}
			for i, wantProjectID := range tt.wantCompleted {
				if got.Completed[i].ProjectID != wantProjectID {
					t.Fatalf("Completed[%d].ProjectID = %q, want %q", i, got.Completed[i].ProjectID, wantProjectID)
				}
			}
			if tt.wantEmptyCount > 0 && len(got.Completed) != tt.wantEmptyCount {
				t.Fatalf("empty-key Completed len = %d, want %d", len(got.Completed), tt.wantEmptyCount)
			}
		})
	}
}

func TestIssueKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		issue telemetry.Issue
		want  string
	}{
		{name: "URL takes precedence", issue: telemetry.Issue{URL: " url ", Identifier: "identifier", ID: "id"}, want: "url"},
		{name: "identifier falls back from blank URL", issue: telemetry.Issue{URL: " ", Identifier: " identifier ", ID: "id"}, want: "identifier"},
		{name: "ID falls back from blank identifier", issue: telemetry.Issue{Identifier: " ", ID: " id "}, want: "id"},
		{name: "all blank", issue: telemetry.Issue{URL: " ", Identifier: "\t", ID: "\n"}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := issueKey(tt.issue); got != tt.want {
				t.Fatalf("issueKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMergeSnapshotStampsProjectIDOnIssueRows(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 14, 30, 0, 0, time.UTC)
	stageAt := now.Add(-time.Minute)
	completedAt := now.Add(-30 * time.Second)
	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
		BoardIssues: []telemetry.Issue{
			{ID: "board", Identifier: "digitaldrywood/detent#6"},
		},
		TrackerDrift: telemetry.TrackerDrift{
			UntrackedOpen: []telemetry.Issue{
				{ID: "untracked", Identifier: "digitaldrywood/detent#771"},
			},
			OpenTerminal: []telemetry.Issue{
				{ID: "terminal", Identifier: "digitaldrywood/detent#583", State: "Done"},
			},
		},
		Pipeline: []telemetry.Issue{
			{ID: "pipeline", Identifier: "digitaldrywood/detent#1", StageUpdatedAt: &stageAt},
		},
		Running: []telemetry.Running{
			{Issue: telemetry.Issue{ID: "running", Identifier: "digitaldrywood/detent#2"}},
		},
		Queue: []telemetry.Queued{
			{Issue: telemetry.Issue{ID: "queued", Identifier: "digitaldrywood/detent#3"}},
		},
		Blocked: []telemetry.Blocked{
			{Issue: telemetry.Issue{ID: "blocked", Identifier: "digitaldrywood/detent#4"}},
		},
		Completed: []telemetry.Completed{
			{Issue: telemetry.Issue{ID: "completed", Identifier: "digitaldrywood/detent#5"}, CompletedAt: completedAt},
		},
		CIUnavailable:      []telemetry.CICondition{{UnstartedCheckCount: 4, PullRequestCount: 2}},
		FailureBreakers:    []telemetry.FailureBreaker{{Class: "session_token_ceiling"}},
		DispatchRecoveries: []telemetry.DispatchRecovery{{Kind: "github_rest", Status: "ramping"}},
	})

	tests := []struct {
		name string
		got  string
	}{
		{name: "pipeline", got: got.Pipeline[0].ProjectID},
		{name: "board", got: got.BoardIssues[0].ProjectID},
		{name: "untracked drift", got: got.TrackerDrift.UntrackedOpen[0].ProjectID},
		{name: "terminal drift", got: got.TrackerDrift.OpenTerminal[0].ProjectID},
		{name: "running", got: got.Running[0].ProjectID},
		{name: "queued", got: got.Queue[0].ProjectID},
		{name: "blocked", got: got.Blocked[0].ProjectID},
		{name: "completed", got: got.Completed[0].ProjectID},
		{name: "CI unavailable", got: got.CIUnavailable[0].ProjectID},
		{name: "failure breaker", got: got.FailureBreakers[0].ProjectID},
		{name: "dispatch recovery", got: got.DispatchRecoveries[0].ProjectID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != "detent" {
				t.Fatalf("ProjectID = %q, want detent", tt.got)
			}
		})
	}
}

func TestMergeSnapshotMergesSchedulerRuntimeRows(t *testing.T) {
	t.Parallel()

	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		Project: telemetry.Project{ID: "detent", DisplayName: "Detent"},
		WorkAttempts: []telemetry.WorkAttempt{
			{AttemptID: 41, Identifier: "digitaldrywood/detent#737"},
		},
		SchedulerDecisions: []telemetry.SchedulerDecision{
			{ID: 51, Identifier: "digitaldrywood/detent#737", Result: "selected"},
		},
	})
	got = mergeSnapshot(got, telemetry.Snapshot{
		Project: telemetry.Project{ID: "drywood", DisplayName: "Drywood"},
		WorkAttempts: []telemetry.WorkAttempt{
			{AttemptID: 42, ProjectID: "custom", Identifier: "digitaldrywood/detent#738"},
		},
		SchedulerDecisions: []telemetry.SchedulerDecision{
			{ID: 52, ProjectID: "custom", Identifier: "digitaldrywood/detent#738", Result: "skipped"},
		},
	})

	if len(got.WorkAttempts) != 2 {
		t.Fatalf("WorkAttempts len = %d, want 2", len(got.WorkAttempts))
	}
	if got.WorkAttempts[0].AttemptID != 41 || got.WorkAttempts[0].ProjectID != "detent" {
		t.Fatalf("WorkAttempts[0] = %#v, want detent attempt 41", got.WorkAttempts[0])
	}
	if got.WorkAttempts[1].AttemptID != 42 || got.WorkAttempts[1].ProjectID != "custom" {
		t.Fatalf("WorkAttempts[1] = %#v, want custom attempt 42", got.WorkAttempts[1])
	}
	if len(got.SchedulerDecisions) != 2 {
		t.Fatalf("SchedulerDecisions len = %d, want 2", len(got.SchedulerDecisions))
	}
	if got.SchedulerDecisions[0].ID != 51 || got.SchedulerDecisions[0].ProjectID != "detent" {
		t.Fatalf("SchedulerDecisions[0] = %#v, want detent decision 51", got.SchedulerDecisions[0])
	}
	if got.SchedulerDecisions[1].ID != 52 || got.SchedulerDecisions[1].ProjectID != "custom" {
		t.Fatalf("SchedulerDecisions[1] = %#v, want custom decision 52", got.SchedulerDecisions[1])
	}
}

func TestMergeSnapshotMergesFleetDispatchStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
	alphaSelectedAt := now.Add(-4 * time.Hour)
	betaSelectedAt := now.Add(-time.Hour)
	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "alpha"},
		Dispatch: telemetry.DispatchStatus{
			CandidateCount: 2, SkippedCount: 2, LastSelectedAt: &alphaSelectedAt, Stalled: true, NeedsHumanAttention: true, WaitReason: "authorization selector excludes every candidate", WaitReasonCode: "authorization_selector_declined",
		},
		DispatchStalls: []telemetry.DispatchStatus{{CandidateCount: 2, Stalled: true, NeedsHumanAttention: true, WaitReasonCode: "authorization_selector_declined"}},
	})
	got = mergeSnapshot(got, telemetry.Snapshot{
		GeneratedAt: now,
		Project:     telemetry.Project{ID: "beta"},
		Dispatch: telemetry.DispatchStatus{
			CandidateCount: 1, SelectedCount: 1, LastSelectedAt: &betaSelectedAt,
		},
	})

	if got.Dispatch.CandidateCount != 3 || got.Dispatch.SelectedCount != 1 || got.Dispatch.SkippedCount != 2 || !got.Dispatch.Stalled || !got.Dispatch.NeedsHumanAttention {
		t.Fatalf("fleet Dispatch = %#v", got.Dispatch)
	}
	if got.Dispatch.Class != observability.ClassFault || got.Dispatch.WaitReasonCode != "authorization_selector_declined" {
		t.Fatalf("fleet Dispatch class/reason = %q/%q, want fault authorization exclusion", got.Dispatch.Class, got.Dispatch.WaitReasonCode)
	}
	if got.Dispatch.LastSelectedAt == nil || !got.Dispatch.LastSelectedAt.Equal(betaSelectedAt) {
		t.Fatalf("fleet LastSelectedAt = %#v, want %s", got.Dispatch.LastSelectedAt, betaSelectedAt)
	}
	if got.Dispatch.SecondsSinceLastSelected == nil || *got.Dispatch.SecondsSinceLastSelected != 3600 {
		t.Fatalf("fleet SecondsSinceLastSelected = %#v, want 3600", got.Dispatch.SecondsSinceLastSelected)
	}
	if len(got.DispatchStalls) != 1 || got.DispatchStalls[0].ProjectID != "alpha" {
		t.Fatalf("fleet DispatchStalls = %#v, want stamped alpha stall", got.DispatchStalls)
	}
	if len(got.Projects) != 2 || got.Projects[0].Dispatch.ProjectID != "alpha" || got.Projects[1].Dispatch.ProjectID != "beta" {
		t.Fatalf("project dispatch snapshots = %#v", got.Projects)
	}
}

func TestMergeSnapshotMergesDrainingShutdown(t *testing.T) {
	t.Parallel()

	firstRequestedAt := time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
	secondRequestedAt := firstRequestedAt.Add(2 * time.Minute)
	got := mergeSnapshot(telemetry.Snapshot{}, telemetry.Snapshot{
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 2,
			RequestedAt:       &secondRequestedAt,
		},
	})
	got = mergeSnapshot(got, telemetry.Snapshot{
		Shutdown: telemetry.Shutdown{
			Status:            "draining",
			Draining:          true,
			SessionsRemaining: 1,
			RequestedAt:       &firstRequestedAt,
		},
	})

	if got.Shutdown.Status != "draining" || !got.Shutdown.Draining {
		t.Fatalf("Shutdown = %#v, want draining", got.Shutdown)
	}
	if got.Shutdown.SessionsRemaining != 3 {
		t.Fatalf("Shutdown.SessionsRemaining = %d, want 3", got.Shutdown.SessionsRemaining)
	}
	if got.Shutdown.RequestedAt == nil || !got.Shutdown.RequestedAt.Equal(firstRequestedAt) {
		t.Fatalf("Shutdown.RequestedAt = %v, want %v", got.Shutdown.RequestedAt, firstRequestedAt)
	}
}

func TestTokenTrendRecorderAppliesRollingWindow(t *testing.T) {
	t.Parallel()

	recorder := newTokenTrendRecorder(2)
	start := time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC)

	snapshots := []telemetry.Snapshot{
		{GeneratedAt: start, Tokens: telemetry.Tokens{Input: 10, Output: 1, Total: 11}},
		{GeneratedAt: start.Add(time.Minute), Tokens: telemetry.Tokens{Input: 20, Output: 2, Total: 22}},
		{GeneratedAt: start.Add(2 * time.Minute), Tokens: telemetry.Tokens{Input: 30, Output: 3, Total: 33}},
	}

	var got telemetry.Snapshot
	for _, snapshot := range snapshots {
		got = recorder.apply(snapshot)
	}

	if len(got.TokenTrend) != 2 {
		t.Fatalf("TokenTrend len = %d, want 2", len(got.TokenTrend))
	}
	if !got.TokenTrend[0].At.Equal(start.Add(time.Minute)) {
		t.Fatalf("TokenTrend[0].At = %v, want second sample", got.TokenTrend[0].At)
	}
	if got.TokenTrend[1].Input != 30 || got.TokenTrend[1].Output != 3 || got.TokenTrend[1].Total != 33 {
		t.Fatalf("TokenTrend[1] = %#v, want latest totals", got.TokenTrend[1])
	}
}

func TestTokenTrendRecorderCalculatesRollingThroughput(t *testing.T) {
	t.Parallel()

	recorder := newTokenTrendRecorder(10)
	start := time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC)
	snapshots := []telemetry.Snapshot{
		{GeneratedAt: start, Tokens: telemetry.Tokens{Input: 90, Output: 10, Total: 100}},
		{GeneratedAt: start.Add(10 * time.Second), Tokens: telemetry.Tokens{Input: 225, Output: 25, Total: 250}},
		{GeneratedAt: start.Add(70 * time.Second), Tokens: telemetry.Tokens{Input: 279, Output: 31, Total: 310}},
	}

	var got telemetry.Snapshot
	for _, snapshot := range snapshots {
		got = recorder.apply(snapshot)
	}

	if got.Throughput.TokensPerSecond != 1 {
		t.Fatalf("Throughput.TokensPerSecond = %v, want 1", got.Throughput.TokensPerSecond)
	}
	if got.Throughput.WindowSeconds != 60 {
		t.Fatalf("Throughput.WindowSeconds = %d, want 60", got.Throughput.WindowSeconds)
	}
	if got.Throughput.Tokens != 60 {
		t.Fatalf("Throughput.Tokens = %d, want 60", got.Throughput.Tokens)
	}
}

func TestTokenTrendRecorderResetsThroughputWhenTotalsDecrease(t *testing.T) {
	t.Parallel()

	recorder := newTokenTrendRecorder(10)
	start := time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC)
	_ = recorder.apply(telemetry.Snapshot{
		GeneratedAt: start,
		Tokens:      telemetry.Tokens{Input: 90, Output: 10, Total: 100},
	})
	got := recorder.apply(telemetry.Snapshot{
		GeneratedAt: start.Add(10 * time.Second),
		Tokens:      telemetry.Tokens{Input: 40, Output: 10, Total: 50},
	})

	if len(got.TokenTrend) != 1 {
		t.Fatalf("TokenTrend len = %d, want 1 after reset", len(got.TokenTrend))
	}
	if got.Throughput.TokensPerSecond != 0 || got.Throughput.Tokens != 0 {
		t.Fatalf("Throughput = %#v, want reset zero throughput", got.Throughput)
	}
}

func TestTokenTrendRecorderKeepsEmptyStateWithoutUsage(t *testing.T) {
	t.Parallel()

	recorder := newTokenTrendRecorder(2)
	got := recorder.apply(telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC),
		Tokens:      telemetry.Tokens{},
	})

	if len(got.TokenTrend) != 0 {
		t.Fatalf("TokenTrend len = %d, want 0", len(got.TokenTrend))
	}
}

func TestTokenTrendRecorderClearsStaleUsage(t *testing.T) {
	t.Parallel()

	recorder := newTokenTrendRecorder(2)
	now := time.Date(2026, 5, 31, 15, 0, 0, 0, time.UTC)

	_ = recorder.apply(telemetry.Snapshot{
		GeneratedAt: now,
		Tokens:      telemetry.Tokens{Input: 10, Output: 1, Total: 11},
	})
	got := recorder.apply(telemetry.Snapshot{
		GeneratedAt: now.Add(time.Minute),
		Tokens:      telemetry.Tokens{},
	})

	if len(got.TokenTrend) != 0 {
		t.Fatalf("TokenTrend len = %d, want 0", len(got.TokenTrend))
	}
}

func writeWorkflowFile(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	content := "---\n" +
		"tracker:\n  kind: memory\n" +
		"codex:\n  command: codex app-server\n" +
		"workspace:\n  root: " + filepath.Join(dir, "workspaces") + "\n" +
		"---\n\nTest workflow prompt.\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write workflow file: %v", err)
	}
	return path
}

func initRunnerSourceRepo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	runRunnerCommand(t, dir, "git", "init", "-b", "main")
	runRunnerGit(t, dir, "config", "core.autocrlf", "false")
	runRunnerGit(t, dir, "config", "user.name", "Test User")
	runRunnerGit(t, dir, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("source repo\n"), 0o600); err != nil {
		t.Fatalf("write README.md: %v", err)
	}
	runRunnerGit(t, dir, "add", "README.md")
	runRunnerGit(t, dir, "commit", "-m", "initial")

	return dir
}

func runRunnerGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return runRunnerCommand(t, dir, "git", args...)
}

func runRunnerCommand(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, output)
	}
	return string(output)
}

func readRunnerFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func readRunnerLines(t *testing.T, path string) []string {
	t.Helper()

	raw := strings.TrimSuffix(readRunnerFile(t, path), "\n")
	if raw == "" {
		return []string{}
	}
	return strings.Split(raw, "\n")
}

func runnerStringPrefix(values []string, prefix []string) bool {
	if len(values) < len(prefix) {
		return false
	}
	for index, want := range prefix {
		if values[index] != want {
			return false
		}
	}
	return true
}

func writeRunnerClaudeStub(t *testing.T) (string, string, string) {
	t.Helper()

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "claude-stub")
	argsPath := filepath.Join(dir, "claude-args.txt")
	stdinPath := filepath.Join(dir, "claude-stdin.txt")
	lines := []string{
		"#!/bin/sh",
		"printf '%s\\n' \"$@\" > " + runnerShellQuote(argsPath),
		"cat > " + runnerShellQuote(stdinPath),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"system","subtype":"init","session_id":"session-cli","model":"fable"}`),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"stream_event","session_id":"session-cli","event":{"type":"message_start","message":{"id":"msg-cli","type":"message","role":"assistant","model":"fable","content":[]}}}`),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"stream_event","session_id":"session-cli","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"claude "}}}`),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"stream_event","session_id":"session-cli","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"streamed"}}}`),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"assistant","session_id":"session-cli","message":{"id":"msg-cli","type":"message","role":"assistant","model":"fable","content":[{"type":"text","text":"ignored full text"}],"usage":{"input_tokens":7,"output_tokens":3}}}`),
		"printf '%s\\n' " + runnerShellQuote(`{"type":"result","subtype":"success","session_id":"session-cli","usage":{"input_tokens":11,"output_tokens":5}}`),
	}
	if err := os.WriteFile(scriptPath, []byte(strings.Join(lines, "\n")+"\n"), 0o700); err != nil {
		t.Fatalf("write claude stub: %v", err)
	}
	return scriptPath, argsPath, stdinPath
}

func runnerShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

type runnerSessionStore struct {
	sessionID int64
	started   store.SessionStart
	finished  store.SessionFinish
	usage     store.UsageEvent
	phase     store.WorkflowPhaseEvent
}

func (s *runnerSessionStore) StartSession(_ context.Context, attrs store.SessionStart) (int64, error) {
	s.started = attrs
	return s.sessionID, nil
}

func (s *runnerSessionStore) FinishSession(_ context.Context, _ int64, attrs store.SessionFinish) error {
	s.finished = attrs
	return nil
}

func (s *runnerSessionStore) RecordUsageEvent(_ context.Context, attrs store.UsageEvent) (int64, error) {
	s.usage = attrs
	return 1, nil
}

func (s *runnerSessionStore) RecordWorkflowPhaseEvent(_ context.Context, attrs store.WorkflowPhaseEvent) (int64, error) {
	s.phase = attrs
	return 1, nil
}

func newRefreshProjectWithConnector(t *testing.T, id string, projectConnector connector.Connector) *projectpkg.Project {
	t.Helper()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	project, err := projectpkg.New(projectpkg.Config{
		Project: globalconfig.Project{
			ID:      id,
			Workdir: t.TempDir(),
			Weight:  1,
		},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{Connector: projectConnector})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	return project
}

func receiveSnapshot(t *testing.T, ch <-chan telemetry.Snapshot) telemetry.Snapshot {
	t.Helper()

	select {
	case snapshot := <-ch:
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}

	return telemetry.Snapshot{}
}

func waitForProjectDataSeq(t *testing.T, project *projectpkg.Project, wantAtLeast uint64) uint64 {
	t.Helper()

	orch := project.Orchestrator()
	if orch == nil {
		t.Fatal("Project.Orchestrator() = nil")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		state, err := orch.State(ctx)
		cancel()
		if err == nil && state.DataSeq >= wantAtLeast {
			return state.DataSeq
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting for DataSeq >= %d", wantAtLeast)
	return 0
}
