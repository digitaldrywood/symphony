package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestMemoryConnectorRunnerE2EGateCreatesBranchDiffStatAndSQLiteTokens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sourceRoot := e2eInitSourceRepo(t)
	workspacesRoot := filepath.Join(t.TempDir(), "workspaces")
	dbPath := filepath.Join(t.TempDir(), "detent.db")

	storeBackend, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    dbPath,
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := storeBackend.Close(); err != nil {
			t.Fatalf("store Close() error = %v", err)
		}
	})

	workspaceBackend, err := workspace.NewBackend(workspace.KindLocalGit, workspace.LocalGitOptions{
		Root:       workspacesRoot,
		SourceRoot: sourceRoot,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("workspace.NewBackend() error = %v", err)
	}

	runnerBackend, err := runpkg.NewRunner(runpkg.Dependencies{
		ProjectID: "detent",
		Workflow: config.Workflow{
			Config: config.Config{
				Codex: config.Codex{
					ApprovalPolicy: config.StringValue("never"),
				},
			},
			Prompt: "Work on {{ issue.identifier }}",
		},
		Workspace: workspaceBackend,
		AgentBackend: &e2eAgentBackend{
			inputTokens:       321,
			cachedInputTokens: 300,
			outputTokens:      45,
			totalTokens:       366,
			lastInputTokens:   50,
			lastCachedTokens:  45,
			lastOutputTokens:  5,
			lastTotalTokens:   55,
		},
		Store: storeBackend,
		Pricing: budget.PricingTable{
			"gpt-5-codex": {
				USDPerInputToken:       0.001,
				USDPerCachedInputToken: 0.0001,
				USDPerOutputToken:      0.01,
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("runner.NewRunner() error = %v", err)
	}

	issue := connector.NewIssue()
	issue.ID = "I_kwDOSskuwc8AAAABD42gFg"
	issue.Identifier = "digitaldrywood/detent#23"
	issue.Title = "transcript byte-parity + e2e gate"
	issue.State = "Todo"
	issue.URL = "https://github.com/digitaldrywood/detent/issues/23"
	issue.ModelOverride = "gpt-5-codex"
	createdAt := time.Now().UTC().Add(-time.Hour)
	issue.CreatedAt = &createdAt

	orch, err := New(Config{
		PollInterval:           time.Hour,
		MaxConcurrentAgents:    1,
		ActiveStates:           []string{"Todo"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Hour,
	}, Dependencies{
		Connector: memory.New(memory.Config{Issues: []connector.Issue{issue}}),
		Runner:    runnerBackend,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- orch.Run(runCtx)
	}()
	t.Cleanup(cancel)

	state := waitForCompletedIssue(t, orch, issue.ID)
	cancel()
	assertOrchestratorStopped(t, errCh)

	completed := state.Completed[issue.ID]
	if completed.FinalState != runpkg.FinalStateCompleted {
		t.Fatalf("completed final state = %q, want %q", completed.FinalState, runpkg.FinalStateCompleted)
	}
	if completed.Tokens.Last == nil || completed.Tokens.Last.TotalTokens != 55 {
		t.Fatalf("completed last-call tokens = %#v, want 55", completed.Tokens.Last)
	}
	diffStat := state.DiffStats[issue.ID]
	if diffStat.FilesChanged != 1 || diffStat.AddedLines != 1 || diffStat.RemovedLines != 0 || diffStat.Status != "changed" {
		t.Fatalf("DiffStats = %#v, want one added file", diffStat)
	}

	entries, err := os.ReadDir(workspacesRoot)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	var workspaceKeys []string
	for _, entry := range entries {
		if entry.Name() != ".detent" {
			workspaceKeys = append(workspaceKeys, entry.Name())
		}
	}
	if len(workspaceKeys) != 1 {
		t.Fatalf("workspace entries = %v, want one issue workspace", workspaceKeys)
	}
	if records, err := os.ReadDir(filepath.Join(workspacesRoot, ".detent", "cleanup-ownership")); err != nil || len(records) != 1 {
		t.Fatalf("recovery ownership records = %v, error = %v", records, err)
	}
	workspaceKey := workspaceKeys[0]
	workspacePath := filepath.Join(workspacesRoot, workspaceKey)
	wantBranch := "detent/" + strings.ToLower(workspaceKey)
	if got := strings.TrimSpace(e2eRunGit(t, workspacePath, "branch", "--show-current")); got != wantBranch {
		t.Fatalf("workspace branch = %q, want %q", got, wantBranch)
	}
	if got := e2eReadFile(t, filepath.Join(workspacePath, "agent-output.txt")); got != "done\n" {
		t.Fatalf("agent-output.txt = %q, want done", got)
	}

	spend, err := storeBackend.IssueTokenSpend(ctx, store.IssueIdentity{ProjectID: "detent", Identifier: issue.Identifier})
	if err != nil {
		t.Fatalf("IssueTokenSpend() error = %v", err)
	}
	if spend.InputTokens != 321 || spend.OutputTokens != 45 || spend.TotalTokens != 366 || spend.Sessions != 1 {
		t.Fatalf("IssueTokenSpend() = %#v, want recorded codex totals", spend)
	}
	if len(spend.ByModel) != 1 || spend.ByModel[0].Model != "gpt-5-codex" {
		t.Fatalf("IssueTokenSpend().ByModel = %#v, want gpt-5-codex", spend.ByModel)
	}
	issueSpend, err := storeBackend.IssueSpendSince(ctx, store.IssueSpendSinceQuery{
		ProjectID:  "detent",
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		Since:      createdAt,
	})
	if err != nil {
		t.Fatalf("IssueSpendSince() error = %v", err)
	}
	if issueSpend.TotalTokens != 366 || issueSpend.Sessions != 1 || math.Abs(issueSpend.CostUSD-0.501) > 0.000001 {
		t.Fatalf("IssueSpendSince() = %#v, want cumulative tokens and cost", issueSpend)
	}
	orch.cfg.Project.ID = "detent"
	orch.cfg.BillingMode = config.BillingModeSubscription
	orch.cfg.NoProgressTokenLimit = 366
	orch.progressSpend = storeBackend
	orch.workAttempts = nil
	decision := orch.evaluateSpendProgress(ctx, Running{Issue: issue}, time.Now().UTC(), false, "")
	if !decision.Block || decision.BlockedBy != "tokens" || decision.Spend.TotalTokens != 366 {
		t.Fatalf("spend progress decision = %#v, want cumulative token block", decision)
	}
}

type e2eAgentBackend struct {
	inputTokens       int64
	cachedInputTokens int64
	outputTokens      int64
	totalTokens       int64
	lastInputTokens   int64
	lastCachedTokens  int64
	lastOutputTokens  int64
	lastTotalTokens   int64
}

func (c *e2eAgentBackend) RunTurn(_ context.Context, req runpkg.AgentTurnRequest, onUpdate runpkg.AgentUpdateHandler) (runpkg.AgentTurnResult, error) {
	if strings.TrimSpace(req.Workspace) == "" {
		return runpkg.AgentTurnResult{}, errors.New("workspace is required")
	}
	if err := os.WriteFile(filepath.Join(req.Workspace, "agent-output.txt"), []byte("done\n"), 0o600); err != nil {
		return runpkg.AgentTurnResult{}, fmt.Errorf("write agent output: %w", err)
	}
	if onUpdate != nil {
		threadTotal := &runpkg.AgentTokenCounts{
			InputTokens:       c.inputTokens,
			CachedInputTokens: c.cachedInputTokens,
			OutputTokens:      c.outputTokens,
			TotalTokens:       c.totalTokens,
		}
		if err := onUpdate(runpkg.AgentUpdate{
			Type: runpkg.AgentUpdateTokenUsage,
			Tokens: runpkg.AgentTokenUsage{
				InputTokens:       c.inputTokens,
				CachedInputTokens: c.cachedInputTokens,
				OutputTokens:      c.outputTokens,
				TotalTokens:       c.totalTokens,
				ThreadTotal:       threadTotal,
				Last: &runpkg.AgentTokenCounts{
					InputTokens:       c.lastInputTokens,
					CachedInputTokens: c.lastCachedTokens,
					OutputTokens:      c.lastOutputTokens,
					TotalTokens:       c.lastTotalTokens,
				},
			},
		}); err != nil {
			return runpkg.AgentTurnResult{}, err
		}
	}
	return runpkg.AgentTurnResult{ThreadID: "thread-e2e", TurnID: "turn-e2e", SessionID: "thread-e2e-turn-e2e"}, nil
}

func waitForCompletedIssue(t *testing.T, orch *Orchestrator, issueID string) State {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		stateCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		state, err := orch.State(stateCtx)
		cancel()
		if err == nil {
			if _, ok := state.Completed[issueID]; ok {
				return state
			}
		} else if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("State() error = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for completed issue %s", issueID)
	return State{}
}

func assertOrchestratorStopped(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("orchestrator Run() error = %v, want context canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for orchestrator to stop")
	}
}

func e2eInitSourceRepo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	e2eRunCommand(t, dir, "git", "init", "-b", "main")
	e2eRunGit(t, dir, "config", "user.name", "Test User")
	e2eRunGit(t, dir, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("source repo\n"), 0o600); err != nil {
		t.Fatalf("write README.md: %v", err)
	}
	e2eRunGit(t, dir, "add", "README.md")
	e2eRunGit(t, dir, "commit", "-m", "initial")

	return dir
}

func e2eRunGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return e2eRunCommand(t, dir, "git", args...)
}

func e2eRunCommand(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, output)
	}
	return string(output)
}

func e2eReadFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
