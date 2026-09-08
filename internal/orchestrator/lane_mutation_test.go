package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/provenance"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestLiveLeaseLaneMutationFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		generation   uint64
		attemptDelta int64
		dispositions []laneMutationDisposition
		wantErr      string
	}{
		{name: "missing disposition", generation: 7, wantErr: errLaneMutationDispositionRequired.Error()},
		{name: "missing generation", dispositions: []laneMutationDisposition{laneMutationPreserveOwnership}, wantErr: "work-attempt ID and generation"},
		{name: "stale work attempt", generation: 7, attemptDelta: 1, dispositions: []laneMutationDisposition{laneMutationPreserveOwnership}, wantErr: "persist live worker lane mutation receipt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			now := time.Date(2026, 8, 27, 14, 0, 0, 0, time.UTC)
			issue := laneRevocationIssue("issue-fail-closed", "digitaldrywood/detent#1999", "In Progress")
			cfg := laneMutationTestConfig()
			runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, issue, now)
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, &recordingWorkAttemptStore{}, now)
			state := newState(cfg)
			state.Running[issue.ID] = Running{
				Issue:         issue,
				WorkAttemptID: attemptID + tt.attemptDelta,
				Generation:    tt.generation,
			}

			err := orch.updateIssueStateByID(ctx, &state, issue.ID, issue, "Rework", now, "test_transition", tt.dispositions...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("updateIssueStateByID() error = %v, want containing %q", err, tt.wantErr)
			}
			if len(tracker.updates) != 0 {
				t.Fatalf("tracker updates = %#v, want none", tracker.updates)
			}
		})
	}
}

func TestLaneMutationReceiptDistinguishesEchoFromLaterReentry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		later     bool
		restarted bool
		project   bool
	}{
		{name: "cached Detent echo"},
		{name: "persisted Detent echo", restarted: true},
		{name: "cached later operator reentry", later: true},
		{name: "persisted later operator reentry", later: true, restarted: true},
		{name: "cached project echo", project: true},
		{name: "persisted project echo", project: true, restarted: true},
		{name: "later project reentry", project: true, later: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 4, 4, 11, 0, 0, time.UTC)
			issue := laneRevocationIssue("echo-owner", "digitaldrywood/detent#2138", "In Progress")
			parked := cloneIssue(issue)
			parked.State = "Human Review"
			cfg := laneMutationTestConfig()
			runtimeStore, attemptID := openLaneMutationTestStore(t, t.Context(), cfg.Project.ID, issue, now)
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
			orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, &recordingWorkAttemptStore{}, now)
			confirmedAt := now
			if tt.project {
				confirmedAt = now.Add(2 * time.Second)
				parked.StageUpdatedAt = &confirmedAt
				orch.connector = &workflowMetricsConnector{stateIssues: []connector.Issue{parked}}
			}
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(t.Context())
			state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID, Generation: 38, stop: stop}
			if err := orch.updateIssueState(t.Context(), &state, issue, parked.State, now, "gate_transition", laneMutationPreserveOwnership); err != nil {
				t.Fatal(err)
			}
			observedAt := confirmedAt
			if tt.later {
				observedAt = observedAt.Add(time.Minute)
			}
			parked.StageUpdatedAt = &observedAt
			parked.StageUpdatedActor = connector.IssueActor{Login: "shared-token", Kind: "User"}
			tracker.stateIssues = []connector.Issue{parked}
			if tt.project {
				orch.connector.(*workflowMetricsConnector).stateIssues = []connector.Issue{parked}
			}
			if tt.restarted {
				running := state.Running[issue.ID]
				running.laneMutation = store.LaneMutationReceipt{}
				state.Running[issue.ID] = running
			}
			orch.reconcileRunningIssues(t.Context(), &state, observedAt)
			if errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
				t.Fatal("unchanged lane timestamp revoked worker ownership")
			}
			if got := hasLaneRevocationEvent(state.RecentEvents, "lane_revocation_unverified"); got != tt.later {
				t.Fatalf("unverified reentry = %v, want %v", got, tt.later)
			}
			if tt.later {
				parked.State = "Blocked"
				tracker.stateIssues = []connector.Issue{parked}
				if tt.project {
					orch.connector.(*workflowMetricsConnector).stateIssues = []connector.Issue{parked}
				}
				orch.reconcileRunningIssues(t.Context(), &state, observedAt.Add(max(cfg.PollInterval, defaultRunningReconcileInterval)))
				if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
					t.Fatal("verified subsequent lane change did not revoke worker")
				}
				pending := orch.pendingLaneRevocations[issue.ID]
				if pending == nil || pending.origin != provenance.OriginIndeterminate || pending.mutation.ID != 0 {
					t.Fatalf("later shared-token change borrowed previous Detent attribution: %#v", pending)
				}
			}
		})
	}
}

func TestGatePromotionReceiptSurvivesRestartAndCompletionFence(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 8, 27, 14, 10, 0, 0, time.UTC)
	runningIssue := laneRevocationIssue("issue-gate-promotion", "digitaldrywood/detent#1999", "In Progress")
	gateIssue := cloneIssue(runningIssue)
	gateIssue.State = "Human Review"
	cfg := laneMutationTestConfig()
	runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, runningIssue, now)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{gateIssue}}
	attempts := &recordingWorkAttemptStore{}
	orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
	state := newState(cfg)
	runCtx, stop := context.WithCancelCause(context.Background())
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		WorkAttemptID: attemptID,
		Generation:    11,
		StartedAt:     now.Add(-time.Minute),
		stop:          stop,
	}

	if !orch.applyAutoPromoteDecision(ctx, &state, gateIssue, AutoPromoteSummary{}, autoPromoteDecision(AutoPromoteActionPromote, AutoPromoteReasonReady), "Merging", now) {
		t.Fatal("applyAutoPromoteDecision() = false, want transition")
	}
	receipt := state.Running[runningIssue.ID].laneMutation
	assertLaneMutationReceipt(t, receipt, laneMutationPreserveOwnership, "In Progress", "Merging", string(AutoPromoteReasonReady))
	if cause := context.Cause(runCtx); cause != nil {
		t.Fatalf("worker context cause = %v, want active lease", cause)
	}

	restartedRunning := state.Running[runningIssue.ID]
	restartedRunning.laneMutation = store.LaneMutationReceipt{}
	state.Running[runningIssue.ID] = restartedRunning
	state.LastRunningReconcileAt = time.Time{}
	orch.reconcileRunningIssues(ctx, &state, now.Add(time.Second))
	if got := state.Running[runningIssue.ID].laneMutation.ID; got != receipt.ID {
		t.Fatalf("recovered receipt ID = %d, want %d", got, receipt.ID)
	}
	if cause := context.Cause(runCtx); cause != nil {
		t.Fatalf("worker context cause after reconciliation = %v, want active lease", cause)
	}

	orch.handleRunResult(ctx, &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		Request:     runpkg.RunRequest{Issue: runningIssue, WorkAttemptID: attemptID, Generation: 11},
		CompletedAt: now.Add(2 * time.Second),
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted, TurnStarted: true},
	})
	assertLaneMutationConsumed(t, ctx, runtimeStore, receipt)
}

func TestReworkLimitParkingRevokesGenerationWithExactReceipt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 8, 27, 14, 20, 0, 0, time.UTC)
	runningIssue := laneRevocationIssue("issue-rework-limit-lease", "digitaldrywood/detent#1999", "In Progress")
	gateIssue := cloneIssue(runningIssue)
	gateIssue.State = "Human Review"
	gateIssue.PullRequest = &connector.PullRequest{Number: 1999, State: "OPEN"}
	cfg := laneMutationTestConfig()
	cfg.AutoPromote.ReworkLimit = 1
	runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, runningIssue, now)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{gateIssue}}
	attempts := &recordingWorkAttemptStore{}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	if _, err := metrics.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:    cfg.Project.ID,
		IssueID:      gateIssue.ID,
		Identifier:   gateIssue.Identifier,
		PhaseType:    store.WorkflowPhaseTypeLane,
		PhaseName:    "Rework",
		Reason:       string(AutoPromoteReasonP1Findings),
		Status:       "entered",
		StartedAt:    now.Add(-time.Hour),
		MetadataJSON: "{}",
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
	orch.workflowMetrics = metrics
	state := newState(cfg)
	runCtx, stop := context.WithCancelCause(context.Background())
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		WorkAttemptID: attemptID,
		Generation:    12,
		StartedAt:     now.Add(-time.Minute),
		stop:          stop,
	}

	if !orch.applyAutoPromoteDecision(ctx, &state, gateIssue, AutoPromoteSummary{}, autoPromoteDecision(AutoPromoteActionRework, AutoPromoteReasonP1Findings), "Rework", now) {
		t.Fatal("applyAutoPromoteDecision() = false, want transition")
	}
	if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
		t.Fatalf("worker context cause = %v, want ErrLaneRevoked", context.Cause(runCtx))
	}
	pending := orch.pendingLaneRevocations[runningIssue.ID]
	if pending == nil {
		t.Fatal("pending lane revocation missing")
	}
	assertLaneMutationReceipt(t, pending.mutation, laneMutationRevokeWorker, "In Progress", "Blocked", "rework_limit")
	receipt := pending.mutation

	restartedCtx, restartedStop := context.WithCancelCause(context.Background())
	restartedRunning := state.Running[runningIssue.ID]
	restartedRunning.Issue = runningIssue
	restartedRunning.laneMutation = store.LaneMutationReceipt{}
	restartedRunning.stop = restartedStop
	state.Running[runningIssue.ID] = restartedRunning
	state.LastRunningReconcileAt = time.Time{}
	orch.pendingLaneRevocations = map[string]*pendingLaneRevocation{}
	orch.reconcileRunningIssues(ctx, &state, now.Add(time.Second))
	if !errors.Is(context.Cause(restartedCtx), runpkg.ErrLaneRevoked) {
		t.Fatalf("restarted worker context cause = %v, want ErrLaneRevoked", context.Cause(restartedCtx))
	}
	pending = orch.pendingLaneRevocations[runningIssue.ID]
	if pending == nil || pending.mutation.ID != receipt.ID {
		t.Fatalf("restarted pending lane revocation = %#v, want receipt %d", pending, receipt.ID)
	}

	metadata := finishLaneMutationRevocation(t, ctx, orch, &state, attempts, runningIssue, attemptID, 12, now.Add(2*time.Second))
	if metadata.FromState != "In Progress" || metadata.ToState != "Blocked" || metadata.Reason != "rework_limit" || metadata.Origin != provenance.OriginDetent {
		t.Fatalf("lane revocation metadata = %#v", metadata)
	}
	assertLaneMutationConsumed(t, ctx, runtimeStore, pending.mutation)
}

func TestMergeRevocationParkingRevokesGenerationWithExactReceipt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 8, 27, 14, 30, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-merge-revocation-lease", "digitaldrywood/detent#1999", "Merging")
	issue.PullRequest = &connector.PullRequest{Number: 2001, State: "OPEN"}
	cfg := laneMutationTestConfig()
	runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, issue, now)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	prNumber := int64(issue.PullRequest.Number)
	attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{
		{TerminalState: store.WorkAttemptTerminalMergeRevoked, ErrorMessage: "head_changed", PRNumber: &prNumber},
		{TerminalState: store.WorkAttemptTerminalMergeRevoked, ErrorMessage: "head_changed", PRNumber: &prNumber},
		{TerminalState: store.WorkAttemptTerminalMergeRevoked, ErrorMessage: "head_changed", PRNumber: &prNumber},
	}}
	orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
	state := newState(cfg)
	runCtx, stop := context.WithCancelCause(context.Background())
	state.Running[issue.ID] = Running{
		Issue:         issue,
		Mode:          runpkg.RunModeMerge,
		WorkAttemptID: attemptID,
		Generation:    13,
		StartedAt:     now.Add(-time.Minute),
		stop:          stop,
	}

	handled, parked := orch.parkRepeatedMergeRevocations(ctx, &state, issue, now)
	if !handled || !parked {
		t.Fatalf("parkRepeatedMergeRevocations() = (%v, %v), want (true, true)", handled, parked)
	}
	if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
		t.Fatalf("worker context cause = %v, want ErrLaneRevoked", context.Cause(runCtx))
	}
	pending := orch.pendingLaneRevocations[issue.ID]
	if pending == nil {
		t.Fatal("pending lane revocation missing")
	}
	assertLaneMutationReceipt(t, pending.mutation, laneMutationRevokeWorker, "Merging", "Blocked", string(AutoPromoteReasonMergeRevocationLimit))

	metadata := finishLaneMutationRevocation(t, ctx, orch, &state, attempts, issue, attemptID, 13, now.Add(time.Second))
	if metadata.FromState != "Merging" || metadata.ToState != "Blocked" || metadata.Reason != string(AutoPromoteReasonMergeRevocationLimit) || metadata.Origin != provenance.OriginDetent {
		t.Fatalf("lane revocation metadata = %#v", metadata)
	}
	assertLaneMutationConsumed(t, ctx, runtimeStore, pending.mutation)
}

func TestRecoveryLaneMutationsPreserveGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		apply  func(context.Context, *Orchestrator, *State, connector.Issue, time.Time) bool
		target string
		reason string
	}{
		{
			name: "dependency unblocking",
			apply: func(ctx context.Context, orch *Orchestrator, state *State, issue connector.Issue, now time.Time) bool {
				return orch.applyDependencyAutoUnblock(ctx, state, issue, nil, "resolved-set", "Todo", now)
			},
			target: "Todo",
			reason: "dependency_auto_unblock",
		},
		{
			name: "blocked-cause recovery",
			apply: func(ctx context.Context, orch *Orchestrator, state *State, issue connector.Issue, now time.Time) bool {
				parkedInspector := staticBlockedRecoveryInspector{snapshot: runpkg.BlockedRecoverySnapshot{ConfigFingerprint: "old", Health: "ready", WorkspaceStatus: "missing"}}
				currentInspector := staticBlockedRecoveryInspector{snapshot: runpkg.BlockedRecoverySnapshot{ConfigFingerprint: "new", Health: "ready", WorkspaceStatus: "missing"}}
				orch.recoveryInspector = parkedInspector
				metadata := orch.newBlockedRecoveryMetadata(ctx, issue, runpkg.RunModeImplement, "test_cause", blockedRecoveryPredicateFingerprintChange, "Todo", DiffStats{})
				orch.recoveryInspector = currentInspector
				state.Blocked[issue.ID] = Blocked{Issue: issue, Source: BlockedSourceProjectStatus, BlockedAt: now.Add(-time.Minute), Recovery: metadata.BlockedRecovery}
				return orch.recoverCauseBlockedIssue(ctx, state, issue, now)
			},
			target: "Todo",
			reason: "cause_blocked_recovery",
		},
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Date(2026, 8, 27, 14, 40+index, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("issue-recovery-"+strings.ReplaceAll(tt.name, " ", "-"), "Blocked")
			cfg := laneMutationTestConfig()
			runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, issue, now)
			tracker := &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}}
			attempts := &recordingWorkAttemptStore{}
			orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
			orch.workflowMetrics = runtimeStore
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(context.Background())
			state.Running[issue.ID] = Running{
				Issue:         issue,
				WorkAttemptID: attemptID,
				Generation:    uint64(20 + index),
				StartedAt:     now.Add(-time.Minute),
				stop:          stop,
			}

			if !tt.apply(ctx, orch, &state, issue, now) {
				t.Fatalf("%s transition = false", tt.name)
			}
			receipt := state.Running[issue.ID].laneMutation
			assertLaneMutationReceipt(t, receipt, laneMutationPreserveOwnership, "Blocked", tt.target, tt.reason)
			if cause := context.Cause(runCtx); cause != nil {
				t.Fatalf("worker context cause = %v, want active lease", cause)
			}
		})
	}
}

func TestAcceptCompletionLaneMutationConsumesExactReceipt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 8, 27, 14, 50, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-accepted-completion", "digitaldrywood/detent#1999", "In Progress")
	cfg := laneMutationTestConfig()
	runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, issue, now)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	attempts := &recordingWorkAttemptStore{}
	orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
	state := newState(cfg)
	state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID, Generation: 31, StartedAt: now.Add(-time.Minute)}

	if err := orch.updateIssueStateByID(ctx, &state, issue.ID, issue, "Done", now, "accepted_completion", laneMutationAcceptCompletion); err != nil {
		t.Fatalf("updateIssueStateByID() error = %v", err)
	}
	receipt := state.Running[issue.ID].laneMutation
	assertLaneMutationReceipt(t, receipt, laneMutationAcceptCompletion, "In Progress", "Done", "accepted_completion")
	orch.handleRunResult(ctx, &state, runpkg.Completion{
		IssueID:     issue.ID,
		Request:     runpkg.RunRequest{Issue: issue, WorkAttemptID: attemptID, Generation: 31},
		CompletedAt: now.Add(time.Second),
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted, TurnStarted: true},
	})
	assertLaneMutationConsumed(t, ctx, runtimeStore, receipt)
	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalSuccess {
		t.Fatalf("work attempt completions = %#v, want success", attempts.completions)
	}
}

func TestDurableCompletionConsumesAcceptReceiptCreatedAfterFence(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 8, 27, 14, 55, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-post-fence-completion", "digitaldrywood/detent#1999", "Merging")
	cfg := laneMutationTestConfig()
	runtimeStore, attemptID := openLaneMutationTestStore(t, ctx, cfg.Project.ID, issue, now)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	attempts := &recordingWorkAttemptStore{}
	orch := newLaneMutationTestOrchestrator(cfg, tracker, runtimeStore, attempts, now)
	state := newState(cfg)
	running := Running{Issue: issue, WorkAttemptID: attemptID, Generation: 32, StartedAt: now.Add(-time.Minute)}
	state.Running[issue.ID] = running

	if err := orch.updateIssueStateByID(ctx, &state, issue.ID, issue, "Done", now, "post_fence_completion", laneMutationAcceptCompletion); err != nil {
		t.Fatalf("updateIssueStateByID() error = %v", err)
	}
	receipt := state.Running[issue.ID].laneMutation
	assertLaneMutationReceipt(t, receipt, laneMutationAcceptCompletion, "Merging", "Done", "post_fence_completion")
	if !orch.completeDurableWorkAttemptWithMetadata(ctx, &state, running, now.Add(time.Second), store.WorkAttemptTerminalSuccess, "", "", "completed", "worker completed", nil) {
		t.Fatal("completeDurableWorkAttemptWithMetadata() = false, want true")
	}
	assertLaneMutationConsumed(t, ctx, runtimeStore, receipt)
}

type laneMutationRevocationMetadata struct {
	FromState string            `json:"from_state"`
	ToState   string            `json:"to_state"`
	Reason    string            `json:"reason"`
	Origin    provenance.Origin `json:"origin"`
}

func laneMutationTestConfig() Config {
	return normalizeConfig(Config{
		Project:        scheduler.ProjectCandidate{ID: "detent"},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates: []string{"Human Review", "Blocked"},
		TerminalStates: []string{"Done", "Cancelled"},
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			PassState:   "Merging",
			ReworkState: "Rework",
		},
	})
}

func openLaneMutationTestStore(
	t *testing.T,
	ctx context.Context,
	projectID string,
	issue connector.Issue,
	now time.Time,
) (store.Store, int64) {
	t.Helper()

	runtimeStore, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	attemptID, err := runtimeStore.StartWorkAttempt(ctx, store.WorkAttemptStart{
		ProjectID:      projectID,
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		WorkerType:     "agent",
		Lane:           issue.State,
		AttemptNumber:  1,
		StartedAt:      now.Add(-time.Minute),
		LeaseExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	return runtimeStore, attemptID
}

func newLaneMutationTestOrchestrator(
	cfg Config,
	tracker connector.Connector,
	runtimeStore store.Store,
	attempts store.WorkAttemptStore,
	now time.Time,
) *Orchestrator {
	return &Orchestrator{
		cfg:                    cfg,
		connector:              tracker,
		workflowMetrics:        runtimeStore,
		workAttempts:           attempts,
		laneMutations:          runtimeStore,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:                    func() time.Time { return now },
	}
}

func assertLaneMutationReceipt(
	t *testing.T,
	receipt store.LaneMutationReceipt,
	disposition laneMutationDisposition,
	fromState string,
	toState string,
	reason string,
) {
	t.Helper()

	if receipt.ID <= 0 || receipt.Disposition != disposition || receipt.FromState != fromState || receipt.ToState != toState || receipt.Reason != reason || receipt.TrackerResult != store.LaneMutationTrackerApplied {
		t.Fatalf("lane mutation receipt = %#v", receipt)
	}
}

func assertLaneMutationConsumed(t *testing.T, ctx context.Context, runtimeStore store.Store, receipt store.LaneMutationReceipt) {
	t.Helper()

	_, err := runtimeStore.LaneMutationReceipt(ctx, store.LaneMutationLookup{
		ProjectID:     receipt.ProjectID,
		IssueID:       receipt.IssueID,
		WorkAttemptID: receipt.WorkAttemptID,
		Generation:    receipt.Generation,
		ToState:       receipt.ToState,
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LaneMutationReceipt() error = %v, want ErrNotFound after consumption", err)
	}
}

func finishLaneMutationRevocation(
	t *testing.T,
	ctx context.Context,
	orch *Orchestrator,
	state *State,
	attempts *recordingWorkAttemptStore,
	issue connector.Issue,
	attemptID int64,
	generation uint64,
	completedAt time.Time,
) laneMutationRevocationMetadata {
	t.Helper()

	orch.handleRunResult(ctx, state, runpkg.Completion{
		IssueID:     issue.ID,
		Request:     runpkg.RunRequest{Issue: issue, WorkAttemptID: attemptID, Generation: generation},
		CompletedAt: completedAt,
		Err:         runpkg.ErrLaneRevoked,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateLaneRevoked, TurnStarted: true},
	})
	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalLaneRevoked {
		t.Fatalf("work attempt completions = %#v, want lane_revoked", attempts.completions)
	}
	var decoded struct {
		LaneRevocation laneMutationRevocationMetadata `json:"lane_revocation"`
	}
	if err := json.Unmarshal([]byte(attempts.completions[0].WorkerMetadataJSON), &decoded); err != nil {
		t.Fatalf("decode lane revocation metadata: %v", err)
	}
	return decoded.LaneRevocation
}
