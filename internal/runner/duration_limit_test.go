package runner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestRunnerEnforcesConfiguredDurationLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		agent           config.Agent
		want            error
		wantMaxDuration time.Duration
	}{
		{
			name:            "turn duration",
			agent:           config.Agent{MaxTurnDurationMS: 25},
			want:            ErrTurnDurationExceeded,
			wantMaxDuration: 25 * time.Millisecond,
		},
		{
			name:  "session duration",
			agent: config.Agent{MaxSessionDurationMS: 25},
			want:  ErrSessionDurationExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workspaceBackend := &fakeWorkspaceBackend{
				info: workspace.Info{Path: t.TempDir(), Key: "issue-duration"},
			}
			durationLimit := &controlledDurationLimit{}
			agentBackend := &durationBlockingAgentBackend{expireDuration: durationLimit.Expire}
			sessionStore := &fakeSessionStore{sessionID: 1496}
			runner, err := NewRunner(Dependencies{
				Workflow: config.Workflow{
					Config: config.Config{Agent: tt.agent},
					Prompt: "Work",
				},
				Workspace:    workspaceBackend,
				AgentBackend: agentBackend,
				Store:        sessionStore,
				sessionLimit: durationLimit.Context,
				turnLimit:    durationLimit.Context,
			})
			if err != nil {
				t.Fatalf("NewRunner() error = %v", err)
			}

			_, err = runner.Run(context.Background(), RunRequest{
				Issue: connector.Issue{
					ID:         "issue-duration",
					Identifier: "digitaldrywood/detent#1496",
				},
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Run() error = %v, want %v", err, tt.want)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run() error = %v, want context deadline exceeded", err)
			}
			if !strings.Contains(err.Error(), "after 25ms") {
				t.Fatalf("Run() error = %v, want configured duration", err)
			}
			if durationLimit.duration != 25*time.Millisecond {
				t.Fatalf("duration limit = %v, want 25ms", durationLimit.duration)
			}
			if !errors.Is(durationLimit.limit, tt.want) {
				t.Fatalf("duration limit error = %v, want %v", durationLimit.limit, tt.want)
			}
			if agentBackend.request.TurnTimeout != 0 {
				t.Fatalf("AgentTurnRequest.TurnTimeout = %v, want backend liveness timeout unchanged", agentBackend.request.TurnTimeout)
			}
			if agentBackend.request.MaxDuration != tt.wantMaxDuration {
				t.Fatalf("AgentTurnRequest.MaxDuration = %v, want %v", agentBackend.request.MaxDuration, tt.wantMaxDuration)
			}
			if len(agentBackend.deadlines) != 4 {
				t.Fatalf("backend deadlines = %v, want one deadline before and after each update", agentBackend.deadlines)
			}
			for _, deadline := range agentBackend.deadlines[1:] {
				if !deadline.Equal(agentBackend.deadlines[0]) {
					t.Fatalf("backend deadlines = %v, want activity not to extend total duration", agentBackend.deadlines)
				}
			}
			if sessionStore.finishCalls != 1 {
				t.Fatalf("FinishSession() calls = %d, want 1", sessionStore.finishCalls)
			}
			if !workspaceBackend.afterRun {
				t.Fatal("AfterRun() was not called")
			}
			if workspaceBackend.afterRunErr != nil {
				t.Fatalf("AfterRun() context error = %v, want fresh cleanup context", workspaceBackend.afterRunErr)
			}
		})
	}
}

func TestRunnerSessionLifetimeReapsWorkerAndRecordsCause(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 8, 18, 14, 0, 0, 0, time.UTC)
	identity := procgroup.Identity{PID: 1885, GroupID: 1885, StartedAt: startedAt}
	workspacePath := t.TempDir()
	durationLimit := &controlledDurationLimit{}
	processStore := &durationReapSessionStore{
		fakeSessionStore: &fakeSessionStore{sessionID: 1885},
		process: store.WorkerProcess{
			SessionID: 1885,
			WorkerProcessIdentity: store.WorkerProcessIdentity{
				PID:       identity.PID,
				GroupID:   identity.GroupID,
				StartedAt: identity.StartedAt,
			},
		},
	}
	backend := &durationWorkerAgentBackend{identity: identity, expireSession: durationLimit.Expire}
	var reaped procgroup.Identity
	var workspaceReaped string
	var logs bytes.Buffer
	runner, err := NewRunner(Dependencies{
		Workflow:     config.Workflow{Config: config.Config{Agent: config.Agent{MaxSessionDurationMS: 25}}, Prompt: "Work"},
		Workspace:    &fakeWorkspaceBackend{info: workspace.Info{Path: workspacePath, Key: "issue-session-lifetime"}},
		AgentBackend: backend,
		Store:        processStore,
		Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
		sessionLimit: durationLimit.Context,
		ReapWorkerProcess: func(_ context.Context, got procgroup.Identity, _ time.Duration) (procgroup.TerminationOutcome, error) {
			reaped = got
			return procgroup.TerminationOutcomeKilled, nil
		},
		ReapWorkspaceProcesses: func(_ context.Context, path string, _ time.Duration) (int, error) {
			workspaceReaped = path
			return 2, nil
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	_, err = runner.Run(context.Background(), RunRequest{Issue: connector.Issue{ID: "issue-session-lifetime", Identifier: "digitaldrywood/detent#1885"}})
	if !errors.Is(err, ErrSessionDurationExceeded) {
		t.Fatalf("Run() error = %v, want ErrSessionDurationExceeded", err)
	}
	if reaped != identity {
		t.Fatalf("reaped identity = %#v, want %#v", reaped, identity)
	}
	if workspaceReaped != workspacePath {
		t.Fatalf("reaped workspace = %q, want %q", workspaceReaped, workspacePath)
	}
	for _, want := range []string{
		"event=worker_orphan_processes_reaped",
		"issue_identifier=digitaldrywood/detent#1885",
		"count=2",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %q:\n%s", want, logs.String())
		}
	}
	if len(processStore.reaps) != 1 {
		t.Fatalf("reap records = %#v, want one", processStore.reaps)
	}
	if got := processStore.reaps[0]; got.Outcome != store.WorkerProcessOutcomeKilled || got.Reason != "maximum_session_lifetime_exceeded" {
		t.Fatalf("reap record = %#v, want killed maximum session lifetime", got)
	}
	if processStore.finished.FinalState != FinalStateSessionDurationExceeded {
		t.Fatalf("final state = %q, want %q", processStore.finished.FinalState, FinalStateSessionDurationExceeded)
	}
}

func TestWorkerSessionCanceled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "turn duration", err: ErrTurnDurationExceeded, want: true},
		{name: "session duration", err: ErrSessionDurationExceeded, want: true},
		{name: "merge fallback budget", err: ErrMergeFallbackBudgetExceeded, want: true},
		{name: "session turn limit", err: ErrSessionTurnLimitExceeded, want: true},
		{name: "session no progress", err: ErrSessionNoProgress, want: true},
		{name: "session memory ceiling", err: ErrSessionMemoryCeilingExceeded, want: true},
		{name: "ordinary failure", err: errors.New("provider failed")},
		{name: "success"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := workerSessionCanceled(tt.err); got != tt.want {
				t.Fatalf("workerSessionCanceled(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestRunnerReapsWorkerAfterTerminalTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		turnErr    error
		wantReason string
	}{
		{name: "completed", wantReason: "turn_completed"},
		{name: "failed", turnErr: errors.New("provider failed"), wantReason: "turn_failed"},
		{name: "cancelled", turnErr: context.Canceled, wantReason: "session_cancelled"},
		{name: "no progress", turnErr: ErrSessionNoProgress, wantReason: SessionBrakeReasonNoProgress},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			startedAt := time.Date(2026, 8, 18, 14, 0, 0, 0, time.UTC)
			identity := procgroup.Identity{PID: 1885, GroupID: 1885, StartedAt: startedAt}
			workspacePath := t.TempDir()
			var logs bytes.Buffer
			processStore := &durationReapSessionStore{
				fakeSessionStore: &fakeSessionStore{sessionID: 1885},
				process: store.WorkerProcess{SessionID: 1885, WorkerProcessIdentity: store.WorkerProcessIdentity{
					PID: identity.PID, GroupID: identity.GroupID, StartedAt: identity.StartedAt,
				}},
			}
			workspaceReaped := ""
			runner, err := NewRunner(Dependencies{
				Workflow:     config.Workflow{Prompt: "Work"},
				Workspace:    &fakeWorkspaceBackend{info: workspace.Info{Path: workspacePath, Key: "issue-terminal-worker"}},
				AgentBackend: terminalWorkerAgentBackend{identity: identity, err: tt.turnErr},
				Store:        processStore,
				Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
				ReapWorkerProcess: func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error) {
					return procgroup.TerminationOutcomeAlreadyExited, nil
				},
				ReapWorkspaceProcesses: func(_ context.Context, path string, _ time.Duration) (int, error) {
					workspaceReaped = path
					return 0, nil
				},
			})
			if err != nil {
				t.Fatalf("NewRunner() error = %v", err)
			}
			_, runErr := runner.Run(context.Background(), RunRequest{Issue: connector.Issue{ID: "issue-terminal-worker", Identifier: "digitaldrywood/detent#1885"}})
			if tt.turnErr == nil && runErr != nil {
				t.Fatalf("Run() error = %v", runErr)
			}
			if tt.turnErr != nil && !errors.Is(runErr, tt.turnErr) {
				t.Fatalf("Run() error = %v, want %v", runErr, tt.turnErr)
			}
			if len(processStore.reaps) != 1 || processStore.reaps[0].Reason != tt.wantReason || processStore.reaps[0].Outcome != store.WorkerProcessOutcomeAlreadyExited {
				t.Fatalf("reap records = %#v, want reason %q", processStore.reaps, tt.wantReason)
			}
			if workspaceReaped != workspacePath {
				t.Fatalf("reaped workspace = %q, want %q", workspaceReaped, workspacePath)
			}
			for _, want := range []string{
				"event=worker_orphan_processes_reaped",
				"issue_identifier=digitaldrywood/detent#1885",
				"reason=" + tt.wantReason,
				"count=0",
			} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("logs missing %q:\n%s", want, logs.String())
				}
			}
		})
	}
}

func TestRunnerSessionDurationSpansResumeFallback(t *testing.T) {
	t.Parallel()

	sessionDurationLimit := &controlledDurationLimit{}
	agentBackend := &durationResumeFallbackAgentBackend{
		expireSession: sessionDurationLimit.Expire,
	}
	sessionStore := &fakeSessionStore{
		sessionID: 1496,
		resumeState: store.AgentResumeState{
			DetentSessionID:   1495,
			ProviderThreadID:  "thread-1495",
			ProviderSessionID: "session-1495",
		},
	}
	runner, err := NewRunner(Dependencies{
		Workflow: config.Workflow{
			Config: config.Config{
				Agent: config.Agent{
					ExperimentalThreadResume: true,
					MaxSessionDurationMS:     25,
				},
				Agents: config.Agents{Routes: []config.AgentRoute{{
					Backend: config.DefaultAgentBackendID,
					Model:   "gpt-5-codex",
					Default: true,
				}}},
			},
			Prompt: "Work",
		},
		Workspace: &fakeWorkspaceBackend{
			info: workspace.Info{Path: t.TempDir(), Key: "issue-duration-resume"},
		},
		AgentBackend: agentBackend,
		Store:        sessionStore,
		sessionLimit: sessionDurationLimit.Context,
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	_, err = runner.Run(context.Background(), RunRequest{
		Issue: connector.Issue{
			ID:          "issue-duration-resume",
			Identifier:  "digitaldrywood/detent#1496",
			PullRequest: &connector.PullRequest{Number: 42, HeadSHA: "head-current", BaseSHA: "base-current"},
		},
		DispatchSourceState: "Rework",
	})
	if !errors.Is(err, ErrSessionDurationExceeded) {
		t.Fatalf("Run() error = %v, want ErrSessionDurationExceeded", err)
	}
	if agentBackend.calls != 2 {
		t.Fatalf("RunTurn() calls = %d, want resume attempt and fresh fallback", agentBackend.calls)
	}
	if agentResumeEmpty(agentBackend.requests[0].Resume) {
		t.Fatal("first RunTurn() resume state is empty")
	}
	if !agentResumeEmpty(agentBackend.requests[1].Resume) {
		t.Fatalf("second RunTurn() resume state = %#v, want fresh fallback", agentBackend.requests[1].Resume)
	}
	if sessionDurationLimit.duration != 25*time.Millisecond {
		t.Fatalf("session duration = %v, want 25ms", sessionDurationLimit.duration)
	}
	if !errors.Is(sessionDurationLimit.limit, ErrSessionDurationExceeded) {
		t.Fatalf("session duration limit = %v, want ErrSessionDurationExceeded", sessionDurationLimit.limit)
	}
}

func TestRunnerMergeFallbackUsesDedicatedBudget(t *testing.T) {
	t.Parallel()

	durationLimit := &controlledDurationLimit{}
	agentBackend := &durationBlockingAgentBackend{expireDuration: durationLimit.Expire}
	workspaceBackend := &fakeMergeWorkspaceBackend{
		fakeWorkspaceBackend: fakeWorkspaceBackend{info: workspace.Info{Path: t.TempDir(), Key: "issue-merge-fallback"}},
		prepareResult:        workspace.MergePrepareResult{Status: workspace.MergePrepareStatusConflict},
	}
	runner, err := NewRunner(Dependencies{
		Workflow: config.Workflow{Config: config.Config{Agent: config.Agent{
			MaxSessionDurationMS:       int(time.Hour / time.Millisecond),
			MergeFallbackMaxDurationMS: 25,
		}}},
		Workspace:    workspaceBackend,
		AgentBackend: agentBackend,
		sessionLimit: durationLimit.Context,
		turnLimit: func(ctx context.Context, _ time.Duration, _ error) (context.Context, context.CancelFunc) {
			return ctx, func() {}
		},
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	result, err := runner.Run(t.Context(), RunRequest{
		Issue: connector.Issue{
			ID:          "issue-merge-fallback",
			Identifier:  "digitaldrywood/detent#1809",
			BranchName:  "detent/fallback",
			PullRequest: &connector.PullRequest{State: "open", BaseRef: "main"},
		},
		Mode: RunModeMerge,
	})
	if !errors.Is(err, ErrMergeFallbackBudgetExceeded) {
		t.Fatalf("Run() error = %v, want ErrMergeFallbackBudgetExceeded", err)
	}
	if result.FinalState != FinalStateMergeFallbackExceeded {
		t.Fatalf("FinalState = %q, want %q", result.FinalState, FinalStateMergeFallbackExceeded)
	}
	if result.Output != "workingworkingworking" {
		t.Fatalf("Output = %q, want bounded partial findings", result.Output)
	}
	if durationLimit.duration != 25*time.Millisecond || !errors.Is(durationLimit.limit, ErrMergeFallbackBudgetExceeded) {
		t.Fatalf("fallback limit = (%s, %v), want (25ms, ErrMergeFallbackBudgetExceeded)", durationLimit.duration, durationLimit.limit)
	}
	if agentBackend.request.MaxDuration != 25*time.Millisecond {
		t.Fatalf("AgentTurnRequest.MaxDuration = %s, want 25ms", agentBackend.request.MaxDuration)
	}
	if workspaceBackend.prepareCalls != 1 {
		t.Fatalf("PrepareMerge() calls = %d, want only the initial conflict precheck", workspaceBackend.prepareCalls)
	}
}

func TestRunnerMergeFallbackValidationOutlivesResolutionBudget(t *testing.T) {
	t.Parallel()

	durationLimit := &controlledDurationLimit{}
	workspaceBackend := &fakeMergeWorkspaceBackend{
		fakeWorkspaceBackend: fakeWorkspaceBackend{info: workspace.Info{Path: t.TempDir(), Key: "issue-merge-fallback-verify"}},
		prepareFunc: func(ctx context.Context, call int) (workspace.MergePrepareResult, error) {
			if call == 0 {
				return workspace.MergePrepareResult{Status: workspace.MergePrepareStatusConflict}, nil
			}
			deadline, bounded := ctx.Deadline()
			if !bounded || time.Until(deadline) > time.Hour {
				t.Fatal("deterministic validation must have a separate bounded context")
			}
			durationLimit.Expire()
			if err := ctx.Err(); err != nil {
				return workspace.MergePrepareResult{}, err
			}
			return workspace.MergePrepareResult{Status: workspace.MergePrepareStatusClean, HeadSHA: "validated-head"}, nil
		},
	}
	runner, err := NewRunner(Dependencies{
		Workflow: config.Workflow{Config: config.Config{Agent: config.Agent{
			MergeFallbackMaxDurationMS: 25,
		}}},
		Workspace: workspaceBackend,
		AgentBackend: &fakeCodexClient{updates: []AgentUpdate{{
			Type:  AgentUpdateMessageDelta,
			Delta: "Resolved the conflict.\nDETENT_MERGE_FALLBACK: resolved",
		}}},
		sessionLimit: durationLimit.Context,
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	result, err := runner.Run(t.Context(), RunRequest{
		Issue: connector.Issue{
			ID:          "issue-merge-fallback-verify",
			Identifier:  "digitaldrywood/detent#1809",
			BranchName:  "detent/fallback",
			PullRequest: &connector.PullRequest{State: "open", BaseRef: "main"},
		},
		Mode: RunModeMerge,
	})
	if err != nil {
		t.Fatalf("Run() error = %v, want resolved handoff after validation outlives the resolution budget", err)
	}
	if result.Output != RunOutputMergeFallbackResolved {
		t.Fatalf("Output = %q, want resolved handoff", result.Output)
	}
	if result.MergeFallbackFindings == "" {
		t.Fatal("MergeFallbackFindings is empty")
	}
	if workspaceBackend.prepareCalls != 2 {
		t.Fatalf("PrepareMerge() calls = %d, want initial precheck and re-verification", workspaceBackend.prepareCalls)
	}
}

func TestRunAgentBackendTurnPreservesParentCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err, cleanupErr := runAgentBackendTurn(ctx, &durationBlockingAgentBackend{}, AgentTurnRequest{
		MaxDuration: time.Hour,
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runAgentBackendTurn() error = %v, want context canceled", err)
	}
	if errors.Is(err, ErrTurnDurationExceeded) {
		t.Fatalf("runAgentBackendTurn() error = %v, want parent cancellation", err)
	}
	if cleanupErr != nil {
		t.Fatalf("runAgentBackendTurn() cleanup error = %v, want nil", cleanupErr)
	}
}

func TestRunAgentBackendTurnKeepsLivenessTimeoutSeparateFromMaxDuration(t *testing.T) {
	t.Parallel()

	durationLimit := &controlledDurationLimit{}
	backend := &durationBlockingAgentBackend{expireDuration: durationLimit.Expire}
	_, err, cleanupErr := runAgentBackendTurnWithToolsUsingLimit(
		context.Background(),
		backend,
		AgentTurnRequest{
			TurnTimeout: time.Hour,
			MaxDuration: 25 * time.Millisecond,
		},
		nil,
		nil,
		nil,
		durationLimit.Context,
	)
	if !errors.Is(err, ErrTurnDurationExceeded) {
		t.Fatalf("runAgentBackendTurn() error = %v, want ErrTurnDurationExceeded", err)
	}
	if cleanupErr != nil {
		t.Fatalf("runAgentBackendTurn() cleanup error = %v, want nil", cleanupErr)
	}
	if backend.request.TurnTimeout != time.Hour {
		t.Fatalf("backend TurnTimeout = %v, want liveness timeout preserved", backend.request.TurnTimeout)
	}
	if backend.request.MaxDuration != 25*time.Millisecond {
		t.Fatalf("backend MaxDuration = %v, want total duration preserved", backend.request.MaxDuration)
	}
	if durationLimit.duration != 25*time.Millisecond {
		t.Fatalf("duration limit = %v, want 25ms", durationLimit.duration)
	}
}

func TestRunAgentBackendTurnDoesNotTreatLivenessTimeoutAsTotalDuration(t *testing.T) {
	t.Parallel()

	backend := &deadlineObservingAgentBackend{}
	_, err, cleanupErr := runAgentBackendTurn(context.Background(), backend, AgentTurnRequest{
		TurnTimeout: 25 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("runAgentBackendTurn() error = %v", err)
	}
	if cleanupErr != nil {
		t.Fatalf("runAgentBackendTurn() cleanup error = %v", cleanupErr)
	}
	if backend.hasDeadline {
		t.Fatal("backend context has a total deadline from the liveness timeout")
	}
	if backend.request.TurnTimeout != 25*time.Millisecond {
		t.Fatalf("backend TurnTimeout = %v, want liveness timeout preserved", backend.request.TurnTimeout)
	}
}

func TestRunAgentBackendTurnLeavesDurationDisabledWithoutDeadline(t *testing.T) {
	t.Parallel()

	backend := &deadlineObservingAgentBackend{}
	_, err, cleanupErr := runAgentBackendTurn(context.Background(), backend, AgentTurnRequest{}, nil)
	if err != nil {
		t.Fatalf("runAgentBackendTurn() error = %v", err)
	}
	if cleanupErr != nil {
		t.Fatalf("runAgentBackendTurn() cleanup error = %v", cleanupErr)
	}
	if backend.hasDeadline {
		t.Fatal("backend context has a deadline with duration limit disabled")
	}
}

func TestRunAgentBackendTurnPropagatesDurationContextToUpdates(t *testing.T) {
	t.Parallel()

	durationLimit := &controlledDurationLimit{}
	hasDeadline := false
	_, err, cleanupErr := runAgentBackendTurnWithToolsUsingLimit(
		context.Background(),
		&durationUpdateAgentBackend{},
		AgentTurnRequest{MaxDuration: 25 * time.Millisecond},
		nil,
		nil,
		func(ctx context.Context, _ AgentUpdate) error {
			_, hasDeadline = ctx.Deadline()
			durationLimit.Expire()
			<-ctx.Done()
			return ctx.Err()
		},
		durationLimit.Context,
	)
	if !errors.Is(err, ErrTurnDurationExceeded) {
		t.Fatalf("runAgentBackendTurnWithTools() error = %v, want ErrTurnDurationExceeded", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runAgentBackendTurnWithTools() error = %v, want context deadline exceeded", err)
	}
	if cleanupErr != nil {
		t.Fatalf("runAgentBackendTurnWithTools() cleanup error = %v, want nil", cleanupErr)
	}
	if !hasDeadline {
		t.Fatal("update context has no duration deadline")
	}
}

func TestRunnerValidatorUpdatePersistenceUsesSessionDurationContext(t *testing.T) {
	t.Parallel()

	sessionDurationLimit := &controlledDurationLimit{}
	sessionStore := &durationBlockingSessionStore{
		fakeSessionStore: &fakeSessionStore{sessionID: 1496},
		expireSession:    sessionDurationLimit.Expire,
	}
	runner, err := NewRunner(Dependencies{
		Workflow: config.Workflow{
			Config: config.Config{
				Agent: config.Agent{MaxSessionDurationMS: 25},
				Gate:  gate.Config{Validator: gate.ValidatorConfig{Enabled: true}},
			},
			Prompt: "Work",
		},
		Workspace: &fakeWorkspaceBackend{
			info: workspace.Info{Path: t.TempDir(), Key: "issue-validator-duration"},
		},
		AgentBackend: &durationUpdateAgentBackend{},
		Store:        sessionStore,
		sessionLimit: sessionDurationLimit.Context,
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	_, err = runner.Validate(context.Background(), ValidatorRequest{
		Issue: connector.Issue{
			ID:         "issue-validator-duration",
			Identifier: "digitaldrywood/detent#1496",
		},
	})
	if !errors.Is(err, ErrSessionDurationExceeded) {
		t.Fatalf("Validate() error = %v, want ErrSessionDurationExceeded", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Validate() error = %v, want context deadline exceeded", err)
	}
	if !sessionStore.hasDeadline {
		t.Fatal("UpdateSessionWorkerProcess() context has no session deadline")
	}
	if sessionDurationLimit.duration != 25*time.Millisecond {
		t.Fatalf("session duration = %v, want 25ms", sessionDurationLimit.duration)
	}
}

type durationBlockingAgentBackend struct {
	request        AgentTurnRequest
	deadlines      []time.Time
	expireDuration func()
}

type durationWorkerAgentBackend struct {
	identity      procgroup.Identity
	expireSession func()
}

type terminalWorkerAgentBackend struct {
	identity procgroup.Identity
	err      error
}

func (b terminalWorkerAgentBackend) RunTurn(_ context.Context, _ AgentTurnRequest, onUpdate AgentUpdateHandler) (AgentTurnResult, error) {
	if err := onUpdate(AgentUpdate{Type: AgentUpdateProcessStarted, WorkerProcess: b.identity}); err != nil {
		return AgentTurnResult{}, err
	}
	return AgentTurnResult{ThreadID: "thread-1885", TurnID: "turn-1885", SessionID: "session-1885"}, b.err
}

func (b *durationWorkerAgentBackend) RunTurn(ctx context.Context, _ AgentTurnRequest, onUpdate AgentUpdateHandler) (AgentTurnResult, error) {
	if err := onUpdate(AgentUpdate{Type: AgentUpdateProcessStarted, WorkerProcess: b.identity}); err != nil {
		return AgentTurnResult{}, err
	}
	b.expireSession()
	<-ctx.Done()
	return AgentTurnResult{}, ctx.Err()
}

func (b *durationBlockingAgentBackend) RunTurn(ctx context.Context, request AgentTurnRequest, onUpdate AgentUpdateHandler) (AgentTurnResult, error) {
	b.request = request
	b.recordDeadline(ctx)
	for range 3 {
		if onUpdate != nil {
			if err := onUpdate(AgentUpdate{
				Type:   AgentUpdateMessageDelta,
				ItemID: "message",
				Delta:  "working",
			}); err != nil {
				return AgentTurnResult{}, err
			}
		}
		b.recordDeadline(ctx)
	}
	if b.expireDuration != nil {
		b.expireDuration()
	}
	<-ctx.Done()
	return AgentTurnResult{}, ctx.Err()
}

func (b *durationBlockingAgentBackend) recordDeadline(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok {
		b.deadlines = append(b.deadlines, deadline)
	}
}

type durationUpdateAgentBackend struct{}

func (b *durationUpdateAgentBackend) RunTurn(_ context.Context, _ AgentTurnRequest, onUpdate AgentUpdateHandler) (AgentTurnResult, error) {
	if onUpdate == nil {
		return AgentTurnResult{}, nil
	}
	return AgentTurnResult{}, onUpdate(AgentUpdate{
		Type: AgentUpdateProcessStarted,
		WorkerProcess: procgroup.Identity{
			PID:       1496,
			GroupID:   1496,
			StartedAt: time.Now(),
		},
	})
}

type durationResumeFallbackAgentBackend struct {
	calls         int
	requests      []AgentTurnRequest
	expireSession func()
}

func (b *durationResumeFallbackAgentBackend) RunTurn(ctx context.Context, request AgentTurnRequest, _ AgentUpdateHandler) (AgentTurnResult, error) {
	b.calls++
	b.requests = append(b.requests, request)
	if !agentResumeEmpty(request.Resume) {
		return AgentTurnResult{}, errors.New("resume failed")
	}
	b.expireSession()
	<-ctx.Done()
	return AgentTurnResult{}, ctx.Err()
}

type controlledDurationLimit struct {
	cancel   context.CancelCauseFunc
	duration time.Duration
	limit    error
}

func (l *controlledDurationLimit) Context(ctx context.Context, duration time.Duration, limit error) (context.Context, context.CancelFunc) {
	if duration <= 0 {
		return ctx, func() {}
	}
	cancelCtx, cancel := context.WithCancelCause(ctx)
	l.cancel = cancel
	l.duration = duration
	l.limit = limit
	deadlineCtx, cancelDeadline := context.WithDeadline(cancelCtx, time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC).Add(duration))
	return deadlineCtx, func() {
		cancelDeadline()
		l.cancel(context.Canceled)
	}
}

func (l *controlledDurationLimit) Expire() {
	l.cancel(&agentDurationLimitError{
		limit:    l.limit,
		duration: l.duration,
	})
}

type deadlineObservingAgentBackend struct {
	hasDeadline bool
	request     AgentTurnRequest
}

func (b *deadlineObservingAgentBackend) RunTurn(ctx context.Context, request AgentTurnRequest, _ AgentUpdateHandler) (AgentTurnResult, error) {
	_, b.hasDeadline = ctx.Deadline()
	b.request = request
	return AgentTurnResult{}, nil
}

type durationBlockingSessionStore struct {
	*fakeSessionStore
	hasDeadline   bool
	expireSession func()
}

type durationReapSessionStore struct {
	*fakeSessionStore
	process store.WorkerProcess
	reaps   []store.WorkerProcessReap
}

func (s *durationReapSessionStore) ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error) {
	return []store.WorkerProcess{s.process}, nil
}

func (s *durationReapSessionStore) MarkSessionWorkerProcessReaped(_ context.Context, sessionID int64, reap store.WorkerProcessReap) error {
	if sessionID == s.process.SessionID {
		s.reaps = append(s.reaps, reap)
	}
	return nil
}

func (s *durationBlockingSessionStore) UpdateSessionWorkerProcess(ctx context.Context, _ int64, _ store.WorkerProcessRegistration) error {
	_, s.hasDeadline = ctx.Deadline()
	s.expireSession()
	<-ctx.Done()
	return ctx.Err()
}

func TestMergeFallbackValidationBoundaries(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		cancel bool
		fail   bool
	}{
		{name: "gate failure", fail: true},
		{name: "validation deadline"},
		{name: "lease or shutdown cancellation", cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				backend := &fakeMergeWorkspaceBackend{prepareFunc: func(ctx context.Context, _ int) (workspace.MergePrepareResult, error) {
					if tt.fail {
						return workspace.MergePrepareResult{}, errors.Join(workspace.ErrMergeResolutionInvalid, errors.New("make check failed"))
					}
					if tt.cancel {
						cancel()
					}
					<-ctx.Done()
					return workspace.MergePrepareResult{}, ctx.Err()
				}}
				started := time.Now()
				result, err := (&Runner{}).verifyMergeFallback(ctx, backend, workspace.Info{}, workspace.Issue{}, workspace.MergePrepareOptions{}, RunResult{
					FinalState: FinalStateCompleted, Output: "DETENT_MERGE_FALLBACK: resolved",
				})
				if tt.cancel {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("error = %v, want parent cancellation", err)
					}
					return
				}
				if err != nil || result.Output != RunOutputMergeFallbackRework || !strings.Contains(result.MergeFallbackFindings, "Deterministic validation failed") {
					t.Fatalf("verification = %#v, %v; want actionable Rework", result, err)
				}
				if !tt.fail && time.Since(started) != mergeFallbackValidationTimeout {
					t.Fatalf("validation elapsed = %v, want bounded deadline %v", time.Since(started), mergeFallbackValidationTimeout)
				}
			})
		})
	}
}
