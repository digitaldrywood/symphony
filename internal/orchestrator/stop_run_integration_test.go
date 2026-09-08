package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const operatorStopIntegrationWaitTimeout = 10 * time.Second

func TestStopRunTargetsOneRunAndBlocksRedispatch(t *testing.T) {
	issue := testIssue("issue-stop", "digitaldrywood/detent#1311", "In Progress")
	other := testIssue("issue-other", "digitaldrywood/detent#1312", "In Progress")
	tracker := newFakeConnector(issue, other)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 4)}
	reaper := &fakeWorkspaceReaper{}
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	completionStore := &operatorStopCompletionStore{Store: runtimeStore, completed: make(chan store.WorkflowPhaseEvent)}
	gate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 2}))
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, MaxConcurrentAgents: 2, MaxRetryBackoff: time.Hour, FailureRetryBaseDelay: time.Hour, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo", "In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunTargetState: "Blocked"}, orchestrator.Dependencies{Connector: tracker, Runner: runner, WorkspaceReaper: reaper, WorkAttempts: completionStore, WorkflowMetrics: completionStore, GlobalDispatchGate: gate})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool { return len(state.Running) == 2 })
	running := state.Running[issue.ID]
	request := orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt, WorkAttemptID: running.WorkAttemptID, DetentSessionID: running.DetentSessionID, ProviderSessionID: running.SessionID}
	result, err := orch.StopRun(t.Context(), request)
	if err != nil {
		t.Fatalf("StopRun() error = %v", err)
	}
	if result.Outcome != "pending" || result.Destination != "Blocked" || !result.CompletedAt.IsZero() {
		t.Fatalf("StopRun() result = %#v, want pending Blocked acknowledgement", result)
	}
	waitForOperatorStopCompletion(t, completionStore.completed, issue.ID)
	state, err = orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, stoppedRunning := state.Running[issue.ID]; stoppedRunning {
		t.Fatalf("stopped issue %q remains active", issue.ID)
	}
	if _, otherRunning := state.Running[other.ID]; !otherRunning {
		t.Fatalf("unrelated issue %q was interrupted", other.ID)
	}
	if _, retrying := state.Retry[issue.ID]; retrying {
		t.Fatalf("Retry[%q] present after operator stop", issue.ID)
	}
	if attempt, ok := workAttemptSnapshot(state, running.WorkAttemptID); !ok || attempt.Phase != "operator_stop_succeeded" || attempt.NextAction != "await operator resume" {
		t.Fatalf("work attempt snapshot = %#v, %v, want successful operator stop", attempt, ok)
	}
	if got := reaper.reapedIssues(); len(got) != 0 {
		t.Fatalf("workspace reaps = %#v, want none", got)
	}
	receipt, err := runtimeStore.WorkAttempt(t.Context(), running.WorkAttemptID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.TerminalState != store.WorkAttemptTerminalOperatorStopped || receipt.Phase != "operator_stop_succeeded" {
		t.Fatalf("work attempt = %#v, want successful operator stop", receipt)
	}
	timeline, err := runtimeStore.IssueWorkflowTimeline(t.Context(), store.IssueIdentity{ProjectID: "detent", IssueID: issue.ID})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	if !hasOperatorStopAudit(timeline.Events, result) {
		t.Fatalf("workflow events = %#v, want successful operator stop audit", timeline.Events)
	}
	repeated, err := orch.StopRun(t.Context(), request)
	if err != nil || !repeated.AlreadyStopped {
		t.Fatalf("repeated StopRun() = %#v, %v, want idempotent success", repeated, err)
	}
	sparseRepeated, err := orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt})
	if err != nil || !sparseRepeated.AlreadyStopped || sparseRepeated.WorkAttemptID != running.WorkAttemptID {
		t.Fatalf("sparse repeated StopRun() = %#v, %v, want idempotent success for original run", sparseRepeated, err)
	}
	third := testIssue("issue-third", "digitaldrywood/detent#1313", "In Progress")
	tracker.mu.Lock()
	tracker.candidates = append(tracker.candidates, third)
	tracker.mu.Unlock()
	if _, err := orch.RequestRefresh(t.Context()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	waitForOperatorStopRunnerIssue(t, runner.started, third.ID)
	state, err = orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, runningAgain := state.Running[issue.ID]; runningAgain {
		t.Fatalf("stopped issue %q redispatched", issue.ID)
	}
	if _, otherRunning := state.Running[other.ID]; !otherRunning {
		t.Fatalf("unrelated issue %q was interrupted", other.ID)
	}
}

func TestStopRunRoutesToOperatorDestination(t *testing.T) {
	tests := []struct {
		name         string
		destination  string
		priority     int
		priorityName string
	}{
		{name: "blocked", destination: orchestrator.StopRunDestinationBlocked},
		{name: "backlog", destination: orchestrator.StopRunDestinationBacklog},
		{name: "cancelled", destination: orchestrator.StopRunDestinationCancelled},
		{name: "prioritized todo", destination: orchestrator.StopRunDestinationTodo, priority: 3, priorityName: "Medium"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			issue := testIssue("issue-stop-route", "digitaldrywood/detent#1354", "In Progress")
			tracker := newFakeConnector(issue)
			runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 2)}
			orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo", "In Progress"}, ObservedStates: []string{"Backlog", "Blocked"}, TerminalStates: []string{"Cancelled", "Done"}}, orchestrator.Dependencies{Connector: tracker, Runner: runner})
			if err != nil {
				t.Fatalf("orchestrator.New() error = %v", err)
			}
			stop := runOrchestrator(t, orch)
			t.Cleanup(stop)
			state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool { return state.Running[issue.ID].Issue.ID != "" })
			running := state.Running[issue.ID]
			result, err := orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt, Destination: tt.destination, Priority: tt.priority})
			if err != nil {
				t.Fatalf("StopRun() error = %v", err)
			}
			if result.Destination != tt.destination || result.Priority != tt.priority || result.PriorityName != tt.priorityName {
				t.Fatalf("StopRun() result = %#v", result)
			}
			if result.Outcome != "pending" || !result.CompletedAt.IsZero() {
				t.Fatalf("StopRun() result = %#v, want pending acknowledgement", result)
			}
			waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
				return len(tracker.stateUpdateCalls()) > 0 && state.Running[issue.ID].Issue.ID == ""
			})
			updates := tracker.stateUpdateCalls()
			if len(updates) == 0 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: tt.destination}) {
				t.Fatalf("state updates = %#v, want destination %s", updates, tt.destination)
			}
			if tt.priorityName != "" && !tracker.hasSetField(issue.ID, "Priority", tt.priorityName) {
				t.Fatalf("priority updates = %#v, want %s", tracker.setFieldCalls(), tt.priorityName)
			}
		})
	}
}

func TestStopRunPreservesConfiguredCustomDefault(t *testing.T) {
	issue := testIssue("issue-stop-custom", "digitaldrywood/detent#1354", "In Progress")
	tracker := newFakeConnector(issue)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"In Progress"}, ObservedStates: []string{"Paused"}, TerminalStates: []string{"Done"}, StopRunTargetState: "Paused"}, orchestrator.Dependencies{Connector: tracker, Runner: runner})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	t.Cleanup(stop)
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool { return state.Running[issue.ID].Issue.ID != "" })
	running := state.Running[issue.ID]
	result, err := orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt})
	if err != nil {
		t.Fatalf("StopRun() error = %v", err)
	}
	if result.Destination != "Paused" || result.Priority != 0 {
		t.Fatalf("StopRun() result = %#v, want configured Paused default", result)
	}
	if result.Outcome != "pending" || !result.CompletedAt.IsZero() {
		t.Fatalf("StopRun() result = %#v, want pending acknowledgement", result)
	}
	waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
		return len(tracker.stateUpdateCalls()) > 0 && state.Running[issue.ID].Issue.ID == ""
	})
	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: "Paused"}) {
		t.Fatalf("state updates = %#v, want configured Paused default", updates)
	}
}

func TestStopRunAppliesTodoPriorityBeforeRedispatch(t *testing.T) {
	issue := testIssue("issue-stop-priority", "digitaldrywood/detent#1354", "In Progress")
	tracker := newOperatorStopAtomicConnector(issue)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 2)}
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: 5 * time.Millisecond, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo", "In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunPriorityNames: map[int]string{2: "High"}}, orchestrator.Dependencies{Connector: tracker, Runner: runner, WorkAttempts: runtimeStore, WorkflowMetrics: runtimeStore})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	t.Cleanup(stop)
	waitForOperatorStopRunnerIssue(t, runner.started, issue.ID)
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool { return state.Running[issue.ID].Issue.ID != "" })
	running := state.Running[issue.ID]
	reply := make(chan stopRunTestReply, 1)
	go func() {
		result, stopErr := orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt, WorkAttemptID: running.WorkAttemptID, Destination: orchestrator.StopRunDestinationTodo, Priority: 2, Reason: "making room for the release blocker"})
		reply <- stopRunTestReply{result: result, err: stopErr}
	}()
	select {
	case <-tracker.priorityStarted:
	case <-time.After(operatorStopIntegrationWaitTimeout):
		t.Fatal("timed out waiting for priority update")
	}
	select {
	case request := <-runner.started:
		t.Fatalf("stopped item redispatched before priority/state completed: %#v", request)
	case <-time.After(25 * time.Millisecond):
	}
	close(tracker.releasePriority)
	var stopped stopRunTestReply
	select {
	case stopped = <-reply:
	case <-time.After(operatorStopIntegrationWaitTimeout):
		t.Fatal("timed out waiting for stop result")
	}
	if stopped.err != nil || stopped.result.Outcome != "pending" || stopped.result.PriorityName != "High" || !stopped.result.CompletedAt.IsZero() {
		t.Fatalf("StopRun() = %#v, %v", stopped.result, stopped.err)
	}
	waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
		return len(tracker.operationsSnapshot()) >= 2
	})
	operations := tracker.operationsSnapshot()
	priorityAt := slices.Index(operations, "priority:High")
	stateAt := slices.Index(operations, "state:Todo")
	if priorityAt < 0 || stateAt < 0 || priorityAt > stateAt {
		t.Fatalf("tracker operations = %#v, want first priority before first state", operations)
	}
	waitForOperatorStopRunnerIssue(t, runner.started, issue.ID)
	timeline, err := runtimeStore.IssueWorkflowTimeline(t.Context(), store.IssueIdentity{ProjectID: "detent", IssueID: issue.ID})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	if !hasOperatorStopReason(timeline.Events, "making room for the release blocker") {
		t.Fatalf("workflow events = %#v, want operator reason", timeline.Events)
	}
}

func TestStopRunAcknowledgesBeforeTrackerTransitionCompletes(t *testing.T) {
	issue := testIssue("issue-stop-acknowledgement", "digitaldrywood/detent#1435", "In Progress")
	tracker := newOperatorStopAtomicConnector(issue)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunPriorityNames: map[int]string{2: "High"}}, orchestrator.Dependencies{Connector: tracker, Runner: runner})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	t.Cleanup(stop)
	t.Cleanup(func() {
		select {
		case <-tracker.releasePriority:
		default:
			close(tracker.releasePriority)
		}
	})
	waitForOperatorStopRunnerIssue(t, runner.started, issue.ID)
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool { return state.Running[issue.ID].Issue.ID != "" })
	running := state.Running[issue.ID]
	reply := make(chan stopRunTestReply, 1)
	go func() {
		result, stopErr := orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt, WorkAttemptID: running.WorkAttemptID, Destination: orchestrator.StopRunDestinationTodo, Priority: 2})
		reply <- stopRunTestReply{result: result, err: stopErr}
	}()

	var accepted stopRunTestReply
	select {
	case accepted = <-reply:
	case <-time.After(operatorStopIntegrationWaitTimeout):
		t.Fatal("timed out waiting for stop acknowledgement before tracker transition")
	}
	if accepted.err != nil || accepted.result.Outcome != "pending" || accepted.result.PriorityName != "High" || !accepted.result.CompletedAt.IsZero() {
		t.Fatalf("StopRun() = %#v, %v, want pending acknowledgement", accepted.result, accepted.err)
	}
	select {
	case <-tracker.priorityStarted:
	case <-time.After(operatorStopIntegrationWaitTimeout):
		t.Fatal("timed out waiting for tracker transition")
	}
	close(tracker.releasePriority)
}

func TestStopRunRecoveryReconcilesDurableHoldBeforeDispatch(t *testing.T) {
	issue := testIssue("issue-stop-recovery", "digitaldrywood/detent#1311", "In Progress")
	tracker := &operatorStopRecoveryConnector{fakeConnector: newFakeConnector(issue), stateUpdated: make(chan struct{}), releaseStateUpdate: make(chan struct{})}
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	attemptID, err := runtimeStore.StartWorkAttempt(t.Context(), store.WorkAttemptStart{ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, IssueURL: issue.URL, WorkerType: "agent", Lane: issue.State, AttemptNumber: 0, StartedAt: now.Add(-time.Minute), LeaseExpiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	metadata, err := json.Marshal(map[string]any{"operator_stop": map[string]any{"project_id": "detent", "issue_id": issue.ID, "identifier": issue.Identifier, "attempt": 0, "work_attempt_id": attemptID, "destination": "Blocked", "outcome": "transition_failed", "requested_at": now.Add(-30 * time.Second), "completed_at": now.Add(-20 * time.Second)}})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := runtimeStore.CompleteWorkAttempt(t.Context(), store.WorkAttemptCompletion{AttemptID: attemptID, CompletedAt: now.Add(-20 * time.Second), Status: store.WorkAttemptStatusTerminal, TerminalState: store.WorkAttemptTerminalOperatorStopped, Phase: "operator_stop_transition_failed", StatusMessage: "run stopped; tracker transition failed", WorkerMetadataJSON: string(metadata), NextAction: "retry tracker transition to Blocked"}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: 5 * time.Millisecond, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunTargetState: "Blocked"}, orchestrator.Dependencies{Connector: tracker, Runner: runner, WorkAttempts: runtimeStore, WorkflowMetrics: runtimeStore})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	releaseStateUpdate := sync.OnceFunc(func() { close(tracker.releaseStateUpdate) })
	defer releaseStateUpdate()
	select {
	case <-tracker.stateUpdated:
	case <-time.After(operatorStopIntegrationWaitTimeout):
		t.Fatal("timed out waiting for recovered tracker transition")
	}
	state, err := orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if len(tracker.stateUpdateCalls()) == 0 || len(state.Running) != 0 {
		t.Fatal("want tracker update with no running items while reconciliation is paused")
	}
	if attempt, ok := workAttemptSnapshot(state, attemptID); !ok || attempt.Phase != "operator_stop_transition_failed" || attempt.NextAction != "retry tracker transition to Blocked" {
		t.Fatalf("work attempt snapshot = %#v, %v, want unreconciled operator stop while transition is paused", attempt, ok)
	}
	recovered := func(state orchestrator.State) bool {
		attempt, ok := workAttemptSnapshot(state, attemptID)
		return ok && attempt.Phase == "operator_stop_succeeded" && attempt.NextAction == "await operator resume" && len(state.Running) == 0 && len(state.Blocked) == 0
	}
	for _, tt := range []struct {
		name  string
		state orchestrator.State
	}{
		{name: "paused tracker transition", state: state},
		{name: "published collections ahead of receipt", state: orchestrator.State{WorkAttempts: state.WorkAttempts}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if recovered(tt.state) {
				t.Fatal("recovery predicate accepted the unreconciled operator stop snapshot")
			}
		})
	}
	releaseStateUpdate()
	state = waitForOperatorStopState(t, orch, recovered)
	if attempt, ok := workAttemptSnapshot(state, attemptID); !ok || attempt.Phase != "operator_stop_succeeded" || attempt.NextAction != "await operator resume" {
		t.Fatalf("work attempt snapshot = %#v, %v, want reconciled operator stop", attempt, ok)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("recovered stopped item redispatched: %#v", request)
	case <-time.After(25 * time.Millisecond):
	}
	receipt, err := runtimeStore.WorkAttempt(t.Context(), attemptID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.Phase != "operator_stop_succeeded" || receipt.NextAction != "await operator resume" {
		t.Fatalf("work attempt = %#v, want reconciled operator stop", receipt)
	}
}

func TestStopRunRecoveryAppliesTodoPriorityBeforeDispatch(t *testing.T) {
	issue := testIssue("issue-stop-todo-recovery", "digitaldrywood/detent#1354", "Todo")
	tracker := newOperatorStopAtomicConnector(issue)
	close(tracker.releasePriority)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	attemptID, err := runtimeStore.StartWorkAttempt(t.Context(), store.WorkAttemptStart{ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, IssueURL: issue.URL, WorkerType: "agent", Lane: "In Progress", AttemptNumber: 0, StartedAt: now.Add(-time.Minute), LeaseExpiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	metadata, err := json.Marshal(map[string]any{"operator_stop": map[string]any{"project_id": "detent", "issue_id": issue.ID, "identifier": issue.Identifier, "attempt": 0, "work_attempt_id": attemptID, "destination": "Todo", "priority": 2, "priority_name": "High", "reason": "resume after the release blocker", "outcome": "transition_failed", "requested_at": now.Add(-30 * time.Second), "completed_at": now.Add(-20 * time.Second)}})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := runtimeStore.CompleteWorkAttempt(t.Context(), store.WorkAttemptCompletion{AttemptID: attemptID, CompletedAt: now.Add(-20 * time.Second), Status: store.WorkAttemptStatusTerminal, TerminalState: store.WorkAttemptTerminalOperatorStopped, Phase: "operator_stop_transition_failed", StatusMessage: "run stopped; tracker transition failed", WorkerMetadataJSON: string(metadata), NextAction: "retry tracker transition to Todo"}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: 5 * time.Millisecond, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunPriorityNames: map[int]string{2: "High"}}, orchestrator.Dependencies{Connector: tracker, Runner: runner, WorkAttempts: runtimeStore, WorkflowMetrics: runtimeStore})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	t.Cleanup(stop)
	waitForOperatorStopRunnerIssue(t, runner.started, issue.ID)
	if operations := tracker.operationsSnapshot(); !slices.Equal(operations, []string{"priority:High", "state:Todo"}) {
		t.Fatalf("tracker operations = %#v, want recovered priority before state and dispatch", operations)
	}
	receipt, err := runtimeStore.WorkAttempt(t.Context(), attemptID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.Phase != "operator_stop_succeeded" || receipt.NextAction != "await scheduler at priority High" {
		t.Fatalf("work attempt = %#v, want recovered prioritized Todo stop", receipt)
	}
}

func TestStopRunHoldsItemWhenTrackerTransitionFails(t *testing.T) {
	issue := testIssue("issue-stop-failure", "digitaldrywood/detent#1311", "In Progress")
	tracker := &operatorStopFailingConnector{fakeConnector: newFakeConnector(issue), err: errors.New("tracker unavailable")}
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}, StopRunTargetState: "Blocked"}, orchestrator.Dependencies{Connector: tracker, Runner: runner, WorkAttempts: runtimeStore})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Running[issue.ID]
		return ok
	})
	running := state.Running[issue.ID]
	request := orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: running.Attempt}
	result, err := orch.StopRun(t.Context(), request)
	if err != nil || result.Outcome != "pending" || !result.CompletedAt.IsZero() {
		t.Fatalf("StopRun() = %#v, %v, want pending acknowledgement", result, err)
	}
	state = waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
		attempt, ok := workAttemptSnapshot(state, running.WorkAttemptID)
		return state.Blocked[issue.ID].Source != "" && ok && attempt.Phase == "operator_stop_transition_failed"
	})
	if state.Blocked[issue.ID].Destination != "Blocked" {
		t.Fatalf("Blocked = %#v, want Blocked reconciliation hold", state.Blocked[issue.ID])
	}
	if _, retrying := state.Retry[issue.ID]; retrying {
		t.Fatalf("Retry[%q] present while reconciliation is pending", issue.ID)
	}
	if attempt, ok := workAttemptSnapshot(state, running.WorkAttemptID); !ok || attempt.Phase != "operator_stop_transition_failed" || attempt.NextAction != "retry tracker transition to Blocked" {
		t.Fatalf("work attempt snapshot = %#v, %v, want failed operator stop transition", attempt, ok)
	}
	tracker.setError(nil)
	result, err = orch.StopRun(t.Context(), request)
	if err != nil || result.Outcome != "succeeded" || !result.AlreadyStopped {
		t.Fatalf("retry StopRun() = %#v, %v, want success", result, err)
	}
	state, err = orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if attempt, ok := workAttemptSnapshot(state, running.WorkAttemptID); !ok || attempt.Phase != "operator_stop_succeeded" || attempt.NextAction != "await operator resume" {
		t.Fatalf("work attempt snapshot = %#v, %v, want successful operator stop retry", attempt, ok)
	}
}

func TestStopRunRejectsStaleIdentity(t *testing.T) {
	issue := testIssue("issue-stale-stop", "digitaldrywood/detent#1311", "In Progress")
	tracker := newFakeConnector(issue)
	runner := &operatorStopBlockingRunner{started: make(chan orchestrator.RunRequest, 1)}
	orch, err := orchestrator.New(orchestrator.Config{PollInterval: time.Hour, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"In Progress"}, ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}}, orchestrator.Dependencies{Connector: tracker, Runner: runner})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	state := waitForOperatorStopState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Running[issue.ID]
		return ok
	})
	_, err = orch.StopRun(t.Context(), orchestrator.StopRunRequest{ProjectID: "detent", IssueID: issue.ID, Attempt: state.Running[issue.ID].Attempt + 1})
	if !errors.Is(err, orchestrator.ErrStopRunStale) {
		t.Fatalf("StopRun() error = %v, want ErrStopRunStale", err)
	}
	state, err = orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, running := state.Running[issue.ID]; !running {
		t.Fatalf("run %q stopped for stale identity", issue.ID)
	}
	if len(tracker.stateUpdateCalls()) != 0 {
		t.Fatalf("state updates = %#v, want none", tracker.stateUpdateCalls())
	}
}

type operatorStopBlockingRunner struct {
	started chan orchestrator.RunRequest
}

func (r *operatorStopBlockingRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	select {
	case r.started <- request:
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}
	<-ctx.Done()
	return orchestrator.RunResult{}, ctx.Err()
}

type operatorStopCompletionStore struct {
	store.Store
	completed chan store.WorkflowPhaseEvent
}

func (s *operatorStopCompletionStore) RecordWorkflowPhaseEvent(ctx context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	eventID, err := s.Store.RecordWorkflowPhaseEvent(ctx, event)
	if err != nil {
		return eventID, err
	}
	if event.PhaseType != store.WorkflowPhaseTypeOperatorAction || event.PhaseName != "stop_run" {
		return eventID, nil
	}
	select {
	case s.completed <- event:
		return eventID, nil
	case <-ctx.Done():
		return eventID, ctx.Err()
	}
}

type operatorStopRecoveryConnector struct {
	*fakeConnector
	stateUpdated       chan struct{}
	releaseStateUpdate chan struct{}
	stateUpdateOnce    sync.Once
}

func (c *operatorStopRecoveryConnector) UpdateIssueState(ctx context.Context, issueID string, state string) error {
	if err := c.fakeConnector.UpdateIssueState(ctx, issueID, state); err != nil {
		return err
	}
	c.stateUpdateOnce.Do(func() { close(c.stateUpdated) })
	select {
	case <-c.releaseStateUpdate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type operatorStopFailingConnector struct {
	*fakeConnector
	mu  sync.Mutex
	err error
}

type stopRunTestReply struct {
	result orchestrator.StopRunResult
	err    error
}

type operatorStopAtomicConnector struct {
	*fakeConnector
	mu              sync.Mutex
	operations      []string
	priorityOnce    sync.Once
	priorityStarted chan struct{}
	releasePriority chan struct{}
}

func newOperatorStopAtomicConnector(issue connector.Issue) *operatorStopAtomicConnector {
	return &operatorStopAtomicConnector{fakeConnector: newFakeConnector(issue), priorityStarted: make(chan struct{}), releasePriority: make(chan struct{})}
}

func (c *operatorStopAtomicConnector) SetField(ctx context.Context, issueID string, field string, value string) error {
	c.mu.Lock()
	c.operations = append(c.operations, "priority:"+value)
	c.mu.Unlock()
	c.priorityOnce.Do(func() {
		close(c.priorityStarted)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.releasePriority:
	}
	return c.fakeConnector.SetField(ctx, issueID, field, value)
}

func (c *operatorStopAtomicConnector) UpdateIssueState(ctx context.Context, issueID string, state string) error {
	c.mu.Lock()
	c.operations = append(c.operations, "state:"+state)
	c.mu.Unlock()
	return c.fakeConnector.UpdateIssueState(ctx, issueID, state)
}

func (c *operatorStopAtomicConnector) operationsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.operations...)
}

func (c *operatorStopFailingConnector) UpdateIssueState(ctx context.Context, issueID string, state string) error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.fakeConnector.UpdateIssueState(ctx, issueID, state)
}

func (c *operatorStopFailingConnector) setError(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}

func waitForOperatorStopRunnerIssue(t *testing.T, started <-chan orchestrator.RunRequest, issueID string) {
	t.Helper()
	deadline := time.NewTimer(operatorStopIntegrationWaitTimeout)
	defer deadline.Stop()
	for {
		select {
		case request := <-started:
			if request.Issue.ID == issueID {
				return
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for runner issue %q", issueID)
		}
	}
}

func waitForOperatorStopCompletion(t *testing.T, completed <-chan store.WorkflowPhaseEvent, issueID string) {
	t.Helper()
	deadline := time.NewTimer(operatorStopIntegrationWaitTimeout)
	defer deadline.Stop()
	for {
		select {
		case event := <-completed:
			if event.IssueID == issueID && event.Status == "succeeded" {
				return
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for operator stop completion for %q", issueID)
		}
	}
}

func waitForOperatorStopState(t *testing.T, orch *orchestrator.Orchestrator, ready func(orchestrator.State) bool) orchestrator.State {
	t.Helper()
	deadline := time.NewTimer(operatorStopIntegrationWaitTimeout)
	defer deadline.Stop()
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		state, err := orch.State(ctx)
		cancel()
		if err == nil && ready(state) {
			return state
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for operator stop state")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func hasOperatorStopAudit(events []store.WorkflowPhaseEvent, result orchestrator.StopRunResult) bool {
	for _, event := range events {
		if event.PhaseType == store.WorkflowPhaseTypeOperatorAction && event.PhaseName == "stop_run" && event.Status == "succeeded" && event.SessionID == result.DetentSessionID {
			return true
		}
	}
	return false
}

func hasOperatorStopReason(events []store.WorkflowPhaseEvent, reason string) bool {
	for _, event := range events {
		if event.PhaseType == store.WorkflowPhaseTypeOperatorAction && event.PhaseName == "stop_run" && event.Reason == reason {
			return true
		}
	}
	return false
}

func workAttemptSnapshot(state orchestrator.State, attemptID int64) (telemetry.WorkAttempt, bool) {
	for _, attempt := range state.WorkAttempts {
		if attempt.AttemptID == attemptID {
			return attempt, true
		}
	}
	return telemetry.WorkAttempt{}, false
}
