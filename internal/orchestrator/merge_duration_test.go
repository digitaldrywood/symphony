package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestMergeWorkerDurationCeilingCancelsProgressingRunAndReleasesSlot(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	ceiling := 6 * time.Hour
	completedAt := startedAt.Add(ceiling)
	issue := mergeDurationTestIssue("issue-1547-timeout")
	var events []memory.Event
	tracker := memory.New(memory.Config{
		Issues:    []connector.Issue{issue},
		Stateful:  true,
		Now:       func() time.Time { return completedAt },
		EventSink: func(event memory.Event) { events = append(events, event) },
	})
	project := scheduler.ProjectCandidate{ID: "detent", Weight: 1}
	dispatchGate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 1}))
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:        1,
		MaxConcurrentAgentsByState: map[string]int{"Merging": 1},
		MergeWorkerMaxDuration:     ceiling,
		Project:                    project,
		ActiveStates:               []string{"Merging"},
		ObservedStates:             []string{"Blocked"},
		TerminalStates:             []string{"Done", "Cancelled"},
	})
	durationLimit := &controlledMergeDurationLimit{}
	runner := &progressingMergeRunner{progressed: make(chan struct{})}
	attempts := &recordingWorkAttemptStore{}
	var logs bytes.Buffer
	supervisor, err := runpkg.NewSupervisor(runner, runpkg.SupervisorConfig{
		MaxRetryBackoff:       cfg.MaxRetryBackoff,
		FailureRetryBaseDelay: cfg.FailureRetryBaseDelay,
		OverloadRetryDelay:    cfg.OverloadRetryDelay,
		Now:                   func() time.Time { return completedAt },
	})
	if err != nil {
		t.Fatalf("NewSupervisor() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:                cfg,
		connector:          tracker,
		supervisor:         supervisor,
		workAttempts:       attempts,
		globalDispatchGate: dispatchGate,
		mergeWorkerLimit:   durationLimit.Context,
		runResults:         make(chan runpkg.Completion, 1),
		runUpdates:         make(chan runUpdate, 1),
		logger:             slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	if !orch.dispatchIssue(t.Context(), &state, issue, 1, startedAt, "") {
		t.Fatal("dispatchIssue() = false, want true")
	}
	select {
	case <-runner.progressed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for merge worker progress")
	}
	select {
	case update := <-orch.runUpdates:
		orch.handleRunUpdate(&state, update)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for merge worker usage update")
	}
	if durationLimit.duration != ceiling {
		t.Fatalf("duration limit = %s, want %s", durationLimit.duration, ceiling)
	}
	if !errors.Is(durationLimit.limit, runpkg.ErrMergeWorkerDurationExceeded) {
		t.Fatalf("duration limit cause = %v, want ErrMergeWorkerDurationExceeded", durationLimit.limit)
	}

	durationLimit.Expire()
	var completion runpkg.Completion
	select {
	case completion = <-orch.runResults:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancelled merge worker completion")
	}
	if !errors.Is(completion.Err, runpkg.ErrMergeWorkerDurationExceeded) {
		t.Fatalf("completion error = %v, want ErrMergeWorkerDurationExceeded", completion.Err)
	}
	if completion.Retryable || completion.RetryAttempt != 0 || completion.RetryDelay != 0 {
		t.Fatalf(
			"retry state = retryable %v attempt %d delay %s, want no runner retry",
			completion.Retryable,
			completion.RetryAttempt,
			completion.RetryDelay,
		)
	}
	if completion.Result.FinalState != runpkg.FinalStateMergeDurationExceeded {
		t.Fatalf(
			"final state = %q, want %q",
			completion.Result.FinalState,
			runpkg.FinalStateMergeDurationExceeded,
		)
	}
	orch.handleRunResult(t.Context(), &state, completion)

	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after duration breach", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after duration breach", issue.ID)
	}
	blocked, ok := state.Blocked[issue.ID]
	if !ok || blocked.Issue.State != "Blocked" || blocked.Reason != mergeWorkerDurationExceededReason {
		t.Fatalf("Blocked[%q] = %#v, want duration breach parked in Blocked", issue.ID, blocked)
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions = %#v, want one", attempts.completions)
	}
	attemptCompletion := attempts.completions[0]
	if attemptCompletion.TerminalState != store.WorkAttemptTerminalTimedOut ||
		attemptCompletion.ErrorClass != workAttemptErrorMergeDuration {
		t.Fatalf("work attempt completion = %#v, want merge duration timeout", attemptCompletion)
	}
	if availableSlots(&state) != 1 {
		t.Fatalf("availableSlots() = %d, want released local slot", availableSlots(&state))
	}
	if _, ok, err := dispatchGate.TryAcquire(
		t.Context(),
		project,
		scheduler.SlotRequest{State: "Merging"},
		completedAt,
	); err != nil || !ok {
		t.Fatalf("TryAcquire() after duration breach = %v, %v, want released global slot", ok, err)
	}

	var stateUpdated bool
	var comment string
	for _, event := range events {
		switch event.Kind {
		case memory.EventKindStateUpdate:
			stateUpdated = event.State == "Blocked"
		case memory.EventKindComment:
			comment = event.Body
		}
	}
	if !stateUpdated {
		t.Fatalf("events = %#v, want Blocked state update", events)
	}
	for _, want := range []string{
		"merge_worker_duration_exceeded",
		"elapsed: 6h0m0s",
		"configured_ceiling: 6h0m0s",
		"last_progress_marker: tool_output",
	} {
		if !strings.Contains(comment, want) {
			t.Fatalf("duration breach comment = %q, want %q", comment, want)
		}
	}
	if got := logs.String(); !strings.Contains(got, "level=WARN") ||
		!strings.Contains(got, "msg=merge_worker_duration_exceeded") ||
		!strings.Contains(got, "last_progress_marker=tool_output") {
		t.Fatalf("logs = %q, want WARN breach with progress marker", got)
	}

	orch.setBlockedStatusIssue(t.Context(), &state, connector.Issue{
		ID:    issue.ID,
		State: blockedStatusState,
	}, completedAt.Add(time.Minute))
	blocked = state.Blocked[issue.ID]
	if blocked.Reason != mergeWorkerDurationExceededReason ||
		blocked.RecoveryTarget != autoPromoteMergingState {
		t.Fatalf("Blocked[%q] after refresh = %#v, want preserved duration breach", issue.ID, blocked)
	}
}

func TestNormalDurationMergeIsUnaffected(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(5 * time.Minute)
	issue := mergeDurationTestIssue("issue-1547-normal")
	tracker := memory.New(memory.Config{
		Issues:   []connector.Issue{issue},
		Stateful: true,
		Now:      func() time.Time { return completedAt },
	})
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		MergeWorkerMaxDuration: 6 * time.Hour,
		Project:                scheduler.ProjectCandidate{ID: "detent", Weight: 1},
		ActiveStates:           []string{"Merging"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
	})
	durationLimit := &controlledMergeDurationLimit{}
	supervisor, err := runpkg.NewSupervisor(instantMergeRunner{}, runpkg.SupervisorConfig{
		Now: func() time.Time { return completedAt },
	})
	if err != nil {
		t.Fatalf("NewSupervisor() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:              cfg,
		connector:        tracker,
		supervisor:       supervisor,
		mergeWorkerLimit: durationLimit.Context,
		runResults:       make(chan runpkg.Completion, 1),
	}
	state := newState(cfg)

	if !orch.dispatchIssue(t.Context(), &state, issue, 1, startedAt, "") {
		t.Fatal("dispatchIssue() = false, want true")
	}
	var completion runpkg.Completion
	select {
	case completion = <-orch.runResults:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for normal merge completion")
	}
	if completion.Err != nil {
		t.Fatalf("completion error = %v, want nil", completion.Err)
	}
	if err := tracker.UpdateIssueState(t.Context(), issue.ID, "Done"); err != nil {
		t.Fatalf("UpdateIssueState(Done) error = %v", err)
	}
	orch.handleRunResult(t.Context(), &state, completion)

	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] present after normal merge", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after normal merge", issue.ID)
	}
	if completed, ok := state.Completed[issue.ID]; !ok || completed.FinalState != "Done" {
		t.Fatalf("Completed[%q] = %#v, want normal Done completion", issue.ID, completed)
	}
	if durationLimit.duration != 6*time.Hour {
		t.Fatalf("duration limit = %s, want 6h", durationLimit.duration)
	}
}

func TestMergeWorkerDurationCeilingStartsAtSlotAcquisition(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	issue := mergeDurationTestIssue("issue-1547-startup-timeout")
	store := newClaimTestStore([]connector.Issue{issue})
	claimStarted := make(chan struct{}, 1)
	claimCause := make(chan error, 1)
	store.assigneeHook = func(ctx context.Context, _ string, _ string) error {
		claimStarted <- struct{}{}
		<-ctx.Done()
		claimCause <- context.Cause(ctx)
		return ctx.Err()
	}
	tracker := claimTestConnector{store: store, login: "worker"}
	project := scheduler.ProjectCandidate{ID: "detent", Weight: 1}
	dispatchGate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 1}))
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		MergeWorkerMaxDuration: 6 * time.Hour,
		Project:                project,
		ActiveStates:           []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
		SelectorContext:        selectorContextForClaimTest("worker"),
		Claiming: ClaimingConfig{
			Enabled:           true,
			OwnershipMode:     "assignee",
			Owner:             "worker",
			AssigneeLogin:     "worker",
			LeaseField:        "Detent Lease",
			LeaseTTL:          time.Minute,
			HeartbeatInterval: 10 * time.Second,
		},
	})
	durationLimit := &controlledMergeDurationLimit{}
	orch := &Orchestrator{
		cfg:                cfg,
		connector:          tracker,
		globalDispatchGate: dispatchGate,
		mergeWorkerLimit:   durationLimit.Context,
	}
	state := newState(cfg)
	outcome := make(chan dispatchIssueOutcome, 1)
	go func() {
		outcome <- orch.dispatchIssueWithOutcome(t.Context(), &state, issue, 1, startedAt, "")
	}()

	select {
	case <-claimStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for startup claim")
	}
	durationLimit.Expire()
	select {
	case cause := <-claimCause:
		if !errors.Is(cause, runpkg.ErrMergeWorkerDurationExceeded) {
			t.Fatalf("claim context cause = %v, want ErrMergeWorkerDurationExceeded", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for claim cancellation")
	}
	select {
	case got := <-outcome:
		if got.dispatched || got.reason != dispatchIssueFailureClaimFailed {
			t.Fatalf("dispatch outcome = %#v, want claim failure", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatch outcome")
	}
	if _, ok, err := dispatchGate.TryAcquire(
		t.Context(),
		project,
		scheduler.SlotRequest{State: "Merging"},
		startedAt,
	); err != nil || !ok {
		t.Fatalf("TryAcquire() after startup timeout = %v, %v, want released global slot", ok, err)
	}
}

func TestMergeWorkerDurationCeilingHonorsLatestTerminalState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		terminate func(*testing.T, *memory.Connector, connector.Issue)
	}{
		{
			name: "issue moved to terminal state",
			terminate: func(t *testing.T, tracker *memory.Connector, issue connector.Issue) {
				t.Helper()
				if err := tracker.UpdateIssueState(t.Context(), issue.ID, "Done"); err != nil {
					t.Fatalf("UpdateIssueState() error = %v", err)
				}
			},
		},
		{
			name: "pull request merged",
			terminate: func(t *testing.T, tracker *memory.Connector, issue connector.Issue) {
				t.Helper()
				if err := tracker.MergePullRequest(
					t.Context(),
					issue.PRRepository,
					issue.PullRequest.Number,
					issue.PullRequest.HeadSHA,
					"squash",
				); err != nil {
					t.Fatalf("MergePullRequest() error = %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
			completedAt := startedAt.Add(6 * time.Hour)
			issue := mergeDurationTestIssue("issue-1547-terminal-" + strings.ReplaceAll(tt.name, " ", "-"))
			prNumber := 1547
			issue.PRNumber = &prNumber
			issue.PRRepository = "digitaldrywood/detent"
			issue.PullRequest = &connector.PullRequest{
				Number:  prNumber,
				State:   "OPEN",
				HeadSHA: "terminal-head",
			}
			tracker := memory.New(memory.Config{
				Issues:   []connector.Issue{issue},
				Stateful: true,
				Now:      func() time.Time { return completedAt },
			})
			cfg := normalizeConfig(Config{
				MaxConcurrentAgents:    1,
				MergeWorkerMaxDuration: 6 * time.Hour,
				Project:                scheduler.ProjectCandidate{ID: "detent", Weight: 1},
				ActiveStates:           []string{"Merging"},
				ObservedStates:         []string{"Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
			})
			durationLimit := &controlledMergeDurationLimit{}
			runner := &progressingMergeRunner{progressed: make(chan struct{})}
			supervisor, err := runpkg.NewSupervisor(runner, runpkg.SupervisorConfig{
				Now: func() time.Time { return completedAt },
			})
			if err != nil {
				t.Fatalf("NewSupervisor() error = %v", err)
			}
			orch := &Orchestrator{
				cfg:              cfg,
				connector:        tracker,
				supervisor:       supervisor,
				mergeWorkerLimit: durationLimit.Context,
				runResults:       make(chan runpkg.Completion, 1),
				runUpdates:       make(chan runUpdate, 1),
			}
			state := newState(cfg)

			if !orch.dispatchIssue(t.Context(), &state, issue, 1, startedAt, "") {
				t.Fatal("dispatchIssue() = false, want true")
			}
			select {
			case <-runner.progressed:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for merge worker progress")
			}
			tt.terminate(t, tracker, issue)
			durationLimit.Expire()
			var completion runpkg.Completion
			select {
			case completion = <-orch.runResults:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for cancelled merge worker completion")
			}
			orch.handleRunResult(t.Context(), &state, completion)

			if _, ok := state.Blocked[issue.ID]; ok {
				t.Fatalf("Blocked[%q] present after latest state became terminal", issue.ID)
			}
			if tt.name == "pull request merged" {
				if len(state.deferredCompletions) != 1 || len(orch.pendingLaneRevocations) != 0 {
					t.Fatal("PR-only terminal evidence must defer until the lane changes")
				}
				return
			}
			if _, ok := state.Completed[issue.ID]; !ok {
				t.Fatalf("Completed[%q] missing after latest state became terminal", issue.ID)
			}
		})
	}
}

func TestMergeWorkerDurationTransitionFailureKeepsDispatchBlocked(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(6 * time.Hour)
	issue := mergeDurationTestIssue("issue-1547-transition-failure")
	memoryTracker := memory.New(memory.Config{
		Issues:   []connector.Issue{issue},
		Stateful: true,
		Now:      func() time.Time { return completedAt },
	})
	tracker := &mergeDurationTransitionConnector{
		Connector:         memoryTracker,
		stateUpdateErrors: 1,
	}
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		MergeWorkerMaxDuration: 6 * time.Hour,
		Project:                scheduler.ProjectCandidate{ID: "detent", Weight: 1},
		ActiveStates:           []string{"Merging"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
	})
	durationLimit := &controlledMergeDurationLimit{}
	runner := &progressingMergeRunner{progressed: make(chan struct{})}
	supervisor, err := runpkg.NewSupervisor(runner, runpkg.SupervisorConfig{
		Now: func() time.Time { return completedAt },
	})
	if err != nil {
		t.Fatalf("NewSupervisor() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:              cfg,
		connector:        tracker,
		supervisor:       supervisor,
		mergeWorkerLimit: durationLimit.Context,
		runResults:       make(chan runpkg.Completion, 1),
		runUpdates:       make(chan runUpdate, 1),
	}
	state := newState(cfg)

	if !orch.dispatchIssue(t.Context(), &state, issue, 1, startedAt, "") {
		t.Fatal("dispatchIssue() = false, want true")
	}
	select {
	case <-runner.progressed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for merge worker progress")
	}
	durationLimit.Expire()
	var completion runpkg.Completion
	select {
	case completion = <-orch.runResults:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancelled merge worker completion")
	}
	orch.handleRunResult(t.Context(), &state, completion)

	blocked, ok := state.Blocked[issue.ID]
	if !ok || blocked.Source != BlockedSourceMergeDuration ||
		normalizeState(blocked.Issue.State) != normalizeState(autoPromoteMergingState) {
		t.Fatalf("Blocked[%q] = %#v, want merge duration reconciliation hold", issue.ID, blocked)
	}
	orch.trackCandidateBlockedStatusIssues(t.Context(), &state, []connector.Issue{issue}, completedAt)
	if _, ok := state.Blocked[issue.ID]; !ok {
		t.Fatalf("Blocked[%q] cleared by tracker hydration", issue.ID)
	}
	if decision := orch.dispatchPlanner().dispatchableIssueDecision(issue, &state, false, completedAt, ""); decision.dispatchable {
		t.Fatalf("dispatchableIssueDecision() = %#v, want blocked", decision)
	}

	transitioned := orch.reconcileMergeDurationHolds(
		t.Context(),
		&state,
		[]connector.Issue{issue},
		completedAt.Add(time.Minute),
	)
	if _, ok := transitioned[issue.ID]; !ok {
		t.Fatalf("reconcileMergeDurationHolds() = %#v, want transitioned issue", transitioned)
	}
	blocked = state.Blocked[issue.ID]
	if blocked.Source != BlockedSourceProjectStatus ||
		normalizeState(blocked.Issue.State) != normalizeState(blockedStatusState) {
		t.Fatalf("Blocked[%q] after retry = %#v, want project status block", issue.ID, blocked)
	}
}

type controlledMergeDurationLimit struct {
	cancel   context.CancelCauseFunc
	duration time.Duration
	limit    error
}

func (l *controlledMergeDurationLimit) Context(
	ctx context.Context,
	duration time.Duration,
	limit error,
) (context.Context, context.CancelFunc) {
	cancelCtx, cancel := context.WithCancelCause(ctx)
	l.cancel = cancel
	l.duration = duration
	l.limit = limit
	deadlineCtx, cancelDeadline := context.WithDeadline(
		cancelCtx,
		time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC).Add(duration),
	)
	return deadlineCtx, func() {
		cancelDeadline()
		cancel(context.Canceled)
	}
}

func (l *controlledMergeDurationLimit) Expire() {
	l.cancel(l.limit)
}

type progressingMergeRunner struct {
	progressed chan struct{}
}

func (r *progressingMergeRunner) Run(ctx context.Context, request RunRequest) (RunResult, error) {
	if request.OnUsageUpdate == nil {
		return RunResult{}, errors.New("missing usage update callback")
	}
	if err := request.OnUsageUpdate(runpkg.UsageUpdate{
		SessionID:   "merge-session-1547",
		TurnCount:   3,
		LastEventAt: request.StartedAt.Add(5*time.Hour + 30*time.Minute),
		LastEvent:   "tool_output",
		LastMessage: "CI is still progressing",
	}); err != nil {
		return RunResult{}, err
	}
	close(r.progressed)
	<-ctx.Done()
	return RunResult{}, ctx.Err()
}

type instantMergeRunner struct{}

func (instantMergeRunner) Run(context.Context, RunRequest) (RunResult, error) {
	return RunResult{FinalState: FinalStateCompleted}, nil
}

type mergeDurationTransitionConnector struct {
	connector.Connector
	stateUpdateErrors int
}

func (c *mergeDurationTransitionConnector) UpdateIssueState(
	ctx context.Context,
	issueID string,
	state string,
) error {
	if c.stateUpdateErrors > 0 {
		c.stateUpdateErrors--
		return errors.New("transient state update failure")
	}
	return c.Connector.UpdateIssueState(ctx, issueID, state)
}

func mergeDurationTestIssue(id string) connector.Issue {
	return connector.Issue{
		ID:               id,
		Identifier:       "digitaldrywood/detent#1547",
		Title:            "Merge duration test",
		State:            "Merging",
		AssignedToWorker: true,
	}
}
