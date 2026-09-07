package orchestrator

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestStateReadersCrossStartupAndRefreshBoundaries(t *testing.T) {
	for _, starting := range []bool{false, true} {
		name := "refresh starts with reader waiting"
		if starting {
			name = "reader precedes initial publication"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := newState(normalizeConfig(Config{}))
				orch := &Orchestrator{done: make(chan struct{}), initialStateReady: make(chan struct{}), refreshStarted: make(chan struct{}), stateRequests: make(chan stateRequest)}
				if !starting {
					orch.publishState(&state)
				}
				done := make(chan error, 1)
				go func() {
					_, err := orch.State(t.Context())
					done <- err
				}()
				synctest.Wait()
				orch.startTick(&state, time.Now())
				orch.publishState(&state)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestRefreshProgressPreservesTrackerFreshness(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		age  time.Duration
		want telemetry.RefreshStatus
	}{
		{name: "initializing", want: telemetry.RefreshStatusInitializing},
		{name: "recent tracker data", age: time.Second, want: telemetry.RefreshStatusReady},
		{name: "stale tracker data", age: time.Hour, want: telemetry.RefreshStatusDegraded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			state := newState(normalizeConfig(Config{PollInterval: time.Minute}))
			if tt.age > 0 {
				state.LastRefreshAt = now.Add(-tt.age)
			}
			state.RefreshProgress = telemetry.RefreshProgress{Stage: "tracker_fetch", StartedAt: now.Add(-10 * time.Second), StageStartedAt: now.Add(-5 * time.Second)}
			refresh := state.Snapshot(now).Refresh.WithFreshness(now)
			if refresh.ReadinessStatus() != tt.want || refresh.InFlight == nil || refresh.InFlight.ElapsedSeconds != 10 || refresh.InFlight.StageElapsedSeconds != 5 {
				t.Fatalf("refresh = %#v, want %s with progress", refresh, tt.want)
			}
			later := refresh.WithFreshness(now.Add(time.Second))
			if later.InFlight.ElapsedSeconds != 11 || refresh.InFlight.ElapsedSeconds != 10 {
				t.Fatal("freshness update failed to advance duration on an independent copy")
			}
		})
	}
}

func TestStatePreservesPublishedRunningAttemptsDuringRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		running     int
		wantRunning int
	}{
		{name: "no active attempts", running: 0, wantRunning: 0},
		{name: "one active attempt", running: 1, wantRunning: 1},
		{name: "six active attempts", running: 6, wantRunning: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{MaxConcurrentAgents: tt.running})
			state := newState(cfg)
			for index := range tt.running {
				id := fmt.Sprintf("issue-%d", index)
				state.Running[id] = Running{Issue: connector.Issue{ID: id}}
			}
			orch := &Orchestrator{cfg: cfg, done: make(chan struct{})}
			orch.publishState(&state)
			orch.refreshInProgress.Store(true)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			published, err := orch.State(ctx)
			if err != nil {
				t.Fatalf("State() error = %v", err)
			}
			if got := published.Snapshot(time.Now()).EffectiveCounts().Running; got != tt.wantRunning {
				t.Fatalf("running count = %d, want %d", got, tt.wantRunning)
			}
		})
	}
}

func TestStateSnapshotEmpty(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))

	snapshot := state.Snapshot(now)

	if !snapshot.GeneratedAt.Equal(now) {
		t.Fatalf("GeneratedAt = %v, want %v", snapshot.GeneratedAt, now)
	}
	if snapshot.Counts != (telemetry.Counts{}) {
		t.Fatalf("Counts = %#v, want zero", snapshot.Counts)
	}
	if len(snapshot.Running) != 0 {
		t.Fatalf("Running = %#v, want empty", snapshot.Running)
	}
	if len(snapshot.Queue) != 0 {
		t.Fatalf("Queue = %#v, want empty", snapshot.Queue)
	}
	if len(snapshot.Blocked) != 0 {
		t.Fatalf("Blocked = %#v, want empty", snapshot.Blocked)
	}
	if len(snapshot.Completed) != 0 {
		t.Fatalf("Completed = %#v, want empty", snapshot.Completed)
	}
	if len(snapshot.BoardIssues) != 0 {
		t.Fatalf("BoardIssues = %#v, want empty", snapshot.BoardIssues)
	}
}

func TestStateSnapshotCarriesIssueAuthor(t *testing.T) {
	t.Parallel()

	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{
		ID:       "issue-1581",
		AuthorID: "corylanou",
	}}

	snapshot := state.Snapshot(time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC))
	if got := snapshot.BoardIssues[0].AuthorID; got != "corylanou" {
		t.Fatalf("BoardIssues[0].AuthorID = %q, want corylanou", got)
	}
}

func TestStateSnapshotCarriesLatestCompletionProgress(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{ID: "issue-1838", Identifier: "digitaldrywood/detent#1838"}}
	state.WorkAttempts = []telemetry.WorkAttempt{
		{
			AttemptID:          1,
			IssueID:            "issue-1838",
			Status:             string(store.WorkAttemptStatusTerminal),
			TerminalState:      string(store.WorkAttemptTerminalNoProgress),
			CompletedAt:        timePointer(now.Add(-time.Minute)),
			WorkerMetadataJSON: `{"completion_progress":{"outcome":"no_progress","reason":"completed_clean_diff_without_pull_request","consecutive_no_progress":1,"no_progress_limit":3}}`,
		},
		{
			AttemptID:          2,
			IssueID:            "issue-1838",
			Status:             string(store.WorkAttemptStatusTerminal),
			TerminalState:      string(store.WorkAttemptTerminalSuccess),
			CompletedAt:        timePointer(now),
			WorkerMetadataJSON: `{"completion_progress":{"outcome":"success","reason":"operational_completion","progress_kinds":["operational_completion"],"completion_kind":"operational","no_progress_limit":3}}`,
		},
	}

	progress := state.Snapshot(now).BoardIssues[0].CompletionProgress
	if progress.Outcome != "success" || progress.Reason != implementOperationalCompletion || progress.CompletionKind != "operational" || !reflect.DeepEqual(progress.Kinds, []string{"operational_completion"}) {
		t.Fatalf("CompletionProgress = %#v", progress)
	}
}

func TestStateSnapshotSurfacesActiveDispatchLoops(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 16, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		count     int
		wantLoops int
		wantTrip  bool
	}{
		{name: "single slow run is not an active loop", count: 1},
		{name: "repetition is surfaced before trip", count: 2, wantLoops: 1},
		{name: "tripped loop remains visible", count: 3, wantLoops: 1, wantTrip: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := newState(normalizeConfig(Config{}))
			state.BoardIssues = []connector.Issue{{ID: "issue-1886", Identifier: "digitaldrywood/detent#1886", Title: "Stop dispatch loops", State: "Rework"}}
			state.WorkAttempts = []telemetry.WorkAttempt{{
				AttemptID:          int64(tt.count),
				IssueID:            "issue-1886",
				Identifier:         "digitaldrywood/detent#1886",
				Status:             string(store.WorkAttemptStatusTerminal),
				TerminalState:      string(store.WorkAttemptTerminalSuccess),
				CompletedAt:        timePointer(now),
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: "success", Reason: implementDependencyDeferralReason, TrackerState: "Rework", ConsecutiveNoProgress: tt.count, NoProgressLimit: 3}}),
			}}

			loops := state.Snapshot(now).DispatchLoops
			if len(loops) != tt.wantLoops {
				t.Fatalf("DispatchLoops = %#v, want %d", loops, tt.wantLoops)
			}
			if tt.wantLoops == 0 {
				return
			}
			loop := loops[0]
			if loop.Identifier != "digitaldrywood/detent#1886" || loop.Lane != "Rework" || loop.ConsecutiveDispatches != tt.count || loop.DispatchLimit != 3 || loop.Tripped != tt.wantTrip || loop.LastCompletedAt == nil || !loop.LastCompletedAt.Equal(now) {
				t.Fatalf("DispatchLoops[0] = %#v", loop)
			}
		})
	}
}

func TestStateSnapshotCarriesOperatorStopRecovery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.Blocked["issue-1435"] = Blocked{
		Issue:           connector.Issue{ID: "issue-1435", Identifier: "digitaldrywood/detent#1435"},
		Reason:          "run stopped; retry the transition to Todo: tracker unavailable",
		RecoveryReason:  "transition_failed",
		RecoveryTarget:  "Todo",
		BlockedAt:       now.Add(-time.Minute),
		Source:          BlockedSourceOperatorStop,
		Attempt:         2,
		WorkAttemptID:   1435,
		DetentSessionID: 608,
		SessionID:       "thread-1435",
		Destination:     "Todo",
		Priority:        2,
		PriorityName:    "High",
		StopReason:      "operator requested",
	}

	blocked := state.Snapshot(now).Blocked[0]
	if blocked.RecoveryReason != "transition_failed" || blocked.RecoveryTarget != "Todo" || blocked.Priority != 2 || blocked.PriorityName != "High" || blocked.StopReason != "operator requested" {
		t.Fatalf("Blocked[0] = %#v", blocked)
	}
}

func TestStateSnapshotCountsRecentTransientOverloadRetries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 21, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Minute)
	old := now.Add(-2 * time.Hour)
	state := newState(normalizeConfig(Config{}))
	state.WorkAttempts = []telemetry.WorkAttempt{
		{AttemptID: 1, ErrorClass: backendcapacity.TransientOverloadErrorClass, CompletedAt: &recent},
		{AttemptID: 2, ErrorClass: backendcapacity.TransientOverloadErrorClass, CompletedAt: &old},
		{AttemptID: 3, ErrorClass: backendcapacity.ErrorClass, CompletedAt: &recent},
	}

	if got := state.Snapshot(now).OverloadRetriesLastHour; got != 1 {
		t.Fatalf("OverloadRetriesLastHour = %d, want 1", got)
	}
}

func TestStateSnapshotAnnotatesDirectUnblockerCount(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		ActiveStates:         []string{"Todo", "Blocked"},
		TerminalStates:       []string{"Done"},
		PrioritizeUnblockers: true,
	})
	state := newState(cfg)
	blocker := connector.Issue{ID: "blocker", Identifier: "owner/repo#1", State: "Todo"}
	dependent := connector.Issue{
		ID:         "dependent",
		Identifier: "owner/repo#2",
		State:      "Blocked",
		BlockedBy:  []connector.BlockedRef{{ID: blocker.ID, Identifier: blocker.Identifier}},
	}
	state.BoardIssues = []connector.Issue{blocker, dependent}
	state.Blocked[dependent.ID] = Blocked{Issue: dependent, Source: BlockedSourceDependency}

	snapshot := state.Snapshot(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC))
	for _, issue := range snapshot.BoardIssues {
		if issue.ID == blocker.ID {
			if issue.UnblockerCount != 1 {
				t.Fatalf("UnblockerCount = %d, want 1", issue.UnblockerCount)
			}
			return
		}
	}
	t.Fatal("snapshot missing blocker issue")
}

func TestStateSnapshotPreservesMappedTrackerPriority(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 22, 0, 0, 0, time.UTC)
	priority := 1
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{
		{
			ID:           "mapped-priority",
			Identifier:   "digitaldrywood/detent#1128",
			State:        "Todo",
			Priority:     &priority,
			PriorityName: "P0",
		},
		{
			ID:           "unmapped-priority",
			Identifier:   "digitaldrywood/detent#1129",
			State:        "Todo",
			PriorityName: "No priority",
		},
	}

	snapshot := state.Snapshot(now)
	if got := snapshot.BoardIssues[0]; got.Priority == nil || *got.Priority != 1 || got.PriorityName != "P0" {
		t.Fatalf("mapped priority = %#v/%q, want 1/P0", got.Priority, got.PriorityName)
	}
	if got := snapshot.BoardIssues[1]; got.Priority != nil || got.PriorityName != "" {
		t.Fatalf("unmapped priority = %#v/%q, want nil/empty", got.Priority, got.PriorityName)
	}
}

func TestStateSnapshotIncludesBackendOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	resetAt := now.Add(44 * time.Minute)
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages["codex"] = BackendOutage{
		Scope:          backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"},
		Kind:           "usageLimitExceeded",
		Reason:         "provider usage limit reached",
		DetectedAt:     now,
		LastObservedAt: now,
		ResetAt:        resetAt,
		ResumeAt:       resetAt.Add(backendCapacityResetJitter),
	}

	snapshot := state.Snapshot(now)
	if len(snapshot.BackendOutages) != 1 {
		t.Fatalf("BackendOutages = %#v, want one", snapshot.BackendOutages)
	}
	outage := snapshot.BackendOutages[0]
	if outage.BackendID != "codex" || outage.Provider != "openai" || outage.ResetAt == nil || !outage.ResetAt.Equal(resetAt) {
		t.Fatalf("BackendOutages[0] = %#v", outage)
	}
}

func TestStateSnapshotExplainsProjectFailureBreakerEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 22, 0, 0, 0, time.UTC)
	resetAt := now.Add(39 * time.Minute)
	tests := []struct {
		name               string
		failures           []ProjectFailure
		blocked            map[string]Blocked
		outage             *BackendOutage
		wantAttempts       int
		wantItems          int
		wantCause          string
		wantRepresentative string
		wantBackendOutage  bool
		wantParked         bool
	}{
		{
			name: "five usage-limit attempts from one blocked item",
			failures: []ProjectFailure{
				{IssueID: "video-1", Identifier: "2026-07-10-detent-not-vibe-coding-short", IssueURL: "https://example.test/items/video-1", Title: "Author beat visuals", Cause: "provider usage limit reached", ErrorMessage: "You've hit your limit. Try again at 9:39 PM", BackendID: "claude-code", BackendKind: "claude_code", Provider: "anthropic", At: now.Add(-5 * time.Minute)},
				{IssueID: "video-1", At: now.Add(-4 * time.Minute)},
				{IssueID: "video-1", At: now.Add(-3 * time.Minute)},
				{IssueID: "video-1", At: now.Add(-2 * time.Minute)},
				{IssueID: "video-1", At: now.Add(-time.Minute)},
			},
			blocked: map[string]Blocked{
				"video-1": {Issue: connector.Issue{ID: "video-1", State: "Blocked"}, RecoveryAction: "move to Rework after provider reset", RecoveryReason: "provider_capacity"},
			},
			outage: &BackendOutage{
				Scope: backendcapacity.Scope{BackendID: "claude-code", BackendKind: "claude_code", Provider: "anthropic"},
				Kind:  "usage_limit_exceeded", Reason: "provider usage limit reached", ResetAt: resetAt, ResumeAt: resetAt.Add(5 * time.Second),
			},
			wantAttempts:       5,
			wantItems:          1,
			wantCause:          "provider usage limit reached",
			wantRepresentative: "You've hit your limit. Try again at 9:39 PM",
			wantBackendOutage:  true,
			wantParked:         true,
		},
		{
			name: "same opaque failure class across multiple items",
			failures: []ProjectFailure{
				{IssueID: "issue-a", Identifier: "ITEM-A", ErrorMessage: "runner transport closed", At: now.Add(-2 * time.Minute)},
				{IssueID: "issue-b", Identifier: "ITEM-B", ErrorMessage: "runner transport closed", At: now.Add(-time.Minute)},
			},
			wantAttempts:       2,
			wantItems:          2,
			wantCause:          "runner transport closed",
			wantRepresentative: "runner transport closed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			class := "runner_error:b6c174a86dfb"
			state := newState(normalizeConfig(Config{}))
			state.FailureBreaker.Class = class
			state.FailureBreaker.Count = len(tt.failures)
			state.FailureBreaker.Failures[class] = tt.failures
			state.FailureBreaker.FirstFailureAt = tt.failures[0].At
			state.FailureBreaker.TrippedAt = tt.failures[len(tt.failures)-1].At
			state.FailureBreaker.ResumeAt = now.Add(time.Hour)
			state.Blocked = tt.blocked
			if tt.outage != nil {
				state.BackendOutages[tt.outage.Scope.Key()] = *tt.outage
			}

			snapshot := state.Snapshot(now)
			if len(snapshot.FailureBreakers) != 1 {
				t.Fatalf("FailureBreakers = %#v, want one", snapshot.FailureBreakers)
			}
			breaker := snapshot.FailureBreakers[0]
			if breaker.AttemptCount != tt.wantAttempts || breaker.DistinctItemCount != tt.wantItems {
				t.Fatalf("attempt/item counts = %d/%d, want %d/%d", breaker.AttemptCount, breaker.DistinctItemCount, tt.wantAttempts, tt.wantItems)
			}
			if breaker.Cause != tt.wantCause || breaker.RepresentativeError != tt.wantRepresentative {
				t.Fatalf("cause/error = %q/%q, want %q/%q", breaker.Cause, breaker.RepresentativeError, tt.wantCause, tt.wantRepresentative)
			}
			if (breaker.BackendOutage != nil) != tt.wantBackendOutage {
				t.Fatalf("BackendOutage = %#v, want present %t", breaker.BackendOutage, tt.wantBackendOutage)
			}
			if len(breaker.Items) != tt.wantItems || breaker.Items[0].Parked != tt.wantParked {
				t.Fatalf("Items = %#v, want %d items with first parked %t", breaker.Items, tt.wantItems, tt.wantParked)
			}
			if breaker.EligibleCandidateCount == nil || *breaker.EligibleCandidateCount != 0 {
				t.Fatalf("EligibleCandidateCount = %v, want 0", breaker.EligibleCandidateCount)
			}
		})
	}
}

func TestStateSnapshotUsesActiveThenMostRecentPersistedRuntimeIdentity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 21, 0, 0, 0, time.UTC)
	issue := connector.Issue{ID: "issue-1118", Identifier: "digitaldrywood/detent#1118", URL: "https://github.com/digitaldrywood/detent/issues/1118", State: "In Progress"}
	active := agentidentity.Configured("codex-high", "codex", "high", "code", "gpt-5.5", "", "", "", now).
		Merge(agentidentity.RuntimeUpdate("gpt-5.6-sol", "openai", "xhigh", "priority", now))
	recent := agentidentity.Configured("claude-local", "claude_code", "local", "validator", "fable", "ollama", "high", "", now.Add(-time.Hour)).
		Merge(agentidentity.RuntimeUpdate("qwen3-coder", "", "", "", now.Add(-time.Hour)))
	older := agentidentity.Configured("codex-old", "codex", "default", "plan", "gpt-5.5", "", "", "", now.Add(-2*time.Hour))
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{issue}
	state.Running[issue.ID] = Running{Issue: issue, RuntimeIdentity: active}
	state.WorkAttempts = []telemetry.WorkAttempt{
		{AttemptID: 2, IssueID: issue.ID, RuntimeIdentity: recent},
		{AttemptID: 1, IssueID: issue.ID, RuntimeIdentity: older},
	}

	snapshot := state.Snapshot(now)
	if len(snapshot.BoardIssues) != 1 || !snapshot.BoardIssues[0].RuntimeIdentity.MateriallyEqual(active) {
		t.Fatalf("active board identity = %#v, want active runtime identity", snapshot.BoardIssues)
	}

	delete(state.Running, issue.ID)
	state.Completed[issue.ID] = Completed{
		Issue:           issue,
		SessionID:       "session-recent",
		StartedAt:       now.Add(-time.Hour),
		CompletedAt:     now.Add(-30 * time.Minute),
		FinalState:      "Human Review",
		RuntimeIdentity: recent,
	}
	snapshot = state.Snapshot(now.Add(time.Minute))
	if len(snapshot.BoardIssues) != 1 || !snapshot.BoardIssues[0].RuntimeIdentity.MateriallyEqual(recent) {
		t.Fatalf("recent board identity = %#v, want newest persisted attempt", snapshot.BoardIssues)
	}
	if len(snapshot.Completed) != 1 || snapshot.Completed[0].SessionID != "session-recent" || snapshot.Completed[0].Model != "qwen3-coder" || !snapshot.Completed[0].RuntimeIdentity.MateriallyEqual(recent) {
		t.Fatalf("completed runtime identity = %#v, want persisted completed identity", snapshot.Completed)
	}
}

func TestStateSnapshotMarksGatePendingBoardIssues(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		AutoPromote: AutoPromoteConfig{
			Enabled:         true,
			GateWaitTimeout: 15 * time.Minute,
			Gate: gate.Config{
				Kind:            gate.KindCommand,
				AutomatedReview: gate.AutomatedReviewOptional,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	gatePending := snapshotGatePendingIssue("gate-pending", "In Progress")
	running := snapshotGatePendingIssue("running", "In Progress")
	queued := snapshotGatePendingIssue("queued", "Todo")
	rework := snapshotGatePendingIssue("rework", "Rework")
	plain := connector.Issue{
		ID:         "plain",
		Identifier: "digitaldrywood/detent#104",
		State:      "In Progress",
	}
	state.BoardIssues = []connector.Issue{gatePending, running, queued, rework, plain}
	for _, issue := range []connector.Issue{gatePending, running, queued, rework} {
		state.Completed[issue.ID] = Completed{
			Issue:       issue,
			CompletedAt: now.Add(-time.Minute),
			FinalState:  FinalStateCompleted,
		}
	}
	state.Running[running.ID] = Running{Issue: running}
	state.Retry[queued.ID] = Retry{Issue: queued}

	snapshot := state.Snapshot(now)
	byID := map[string]telemetry.Issue{}
	for _, issue := range snapshot.BoardIssues {
		byID[issue.ID] = issue
	}

	tests := []struct {
		id   string
		want bool
	}{
		{id: "gate-pending", want: true},
		{id: "running", want: true},
		{id: "queued", want: true},
		{id: "rework"},
		{id: "plain"},
	}
	for _, tt := range tests {
		got, ok := byID[tt.id]
		if !ok {
			t.Fatalf("snapshot BoardIssues missing %q: %#v", tt.id, snapshot.BoardIssues)
		}
		if got.GatePending != tt.want {
			t.Fatalf("BoardIssues[%q].GatePending = %t, want %t", tt.id, got.GatePending, tt.want)
		}
	}
	if snapshot.Running[0].GatePending {
		t.Fatal("running issue GatePending = true, want false")
	}
	if snapshot.Queue[0].GatePending {
		t.Fatal("queued issue GatePending = true, want false")
	}
	pendingMetadata := byID["gate-pending"].Metadata
	if got := pendingMetadata[automatedReviewModeMetadataKey]; got != gate.AutomatedReviewOptional {
		t.Fatalf("automated review mode metadata = %q, want optional", got)
	}
	if got := pendingMetadata[automatedReviewTimeoutActionMetadataKey]; got != autoPromoteGateWaitTimeoutMerge {
		t.Fatalf("automated review timeout action metadata = %q, want merge", got)
	}
	wantDeadline := now.Add(14 * time.Minute).UTC().Format(time.RFC3339)
	if got := pendingMetadata[automatedReviewDeadlineMetadataKey]; got != wantDeadline {
		t.Fatalf("automated review deadline metadata = %q, want %q", got, wantDeadline)
	}
}

func snapshotGatePendingIssue(id string, state string) connector.Issue {
	return connector.Issue{
		ID:         id,
		Identifier: "digitaldrywood/detent#" + id,
		State:      state,
		PullRequest: &connector.PullRequest{
			Number:   100,
			State:    "open",
			CIStatus: "pending",
		},
	}
}

func TestStateSnapshotIncludesAutoPromoteDecisionReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 14, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
			Gate:    gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	issue := connector.Issue{
		ID:         "auto-promote-await",
		Identifier: "digitaldrywood/detent#1107",
		State:      "Human Review",
		PullRequest: &connector.PullRequest{
			Number:   1108,
			State:    "open",
			CIStatus: "success",
		},
	}
	state.BoardIssues = []connector.Issue{issue}
	state.Pipeline = []connector.Issue{issue}
	state.AutoPromoteDecisions[issue.ID] = autoPromoteDecision(AutoPromoteActionAwaitReview, AutoPromoteReasonWorkpadStatusInvalid)

	snapshot := state.Snapshot(now)

	if got := snapshot.BoardIssues[0].Metadata[autoPromoteActionMetadataKey]; got != string(AutoPromoteActionAwaitReview) {
		t.Fatalf("BoardIssues auto promote action = %q, want await_review", got)
	}
	if got := snapshot.BoardIssues[0].Metadata[autoPromoteReasonMetadataKey]; got != string(AutoPromoteReasonWorkpadStatusInvalid) {
		t.Fatalf("BoardIssues auto promote reason = %q, want workpad_status_invalid", got)
	}
	if got := snapshot.Pipeline[0].Metadata[autoPromoteReasonMetadataKey]; got != string(AutoPromoteReasonWorkpadStatusInvalid) {
		t.Fatalf("Pipeline auto promote reason = %q, want workpad_status_invalid", got)
	}
}

func TestStateSnapshotIncludesArtifactGateWaitDispatchReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 40, 0, 0, time.UTC)
	stageUpdatedAt := now.Add(-2 * time.Minute)
	updatedAt := now.Add(-time.Minute)
	cfg := normalizeConfig(Config{
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
			Gate: gate.Config{
				Kind: gate.KindArtifact,
				Artifact: gate.ArtifactConfig{
					StatusField:  "render_status",
					WaitStatuses: []string{"pending_review"},
				},
			},
		},
		ActiveStates:   []string{"Todo", "Production", "Rework"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	state.BoardIssues = []connector.Issue{{
		ID:             "artifact-wait",
		Identifier:     "wi-artifact-wait",
		State:          "Todo",
		Fields:         map[string]string{"render_status": "pending_review"},
		FieldUpdatedAt: map[string]time.Time{"render_status": updatedAt},
		StageUpdatedAt: &stageUpdatedAt,
		UpdatedAt:      &updatedAt,
	}, {
		ID:             "artifact-seeded-wait",
		Identifier:     "wi-artifact-seeded-wait",
		State:          "Todo",
		Fields:         map[string]string{"render_status": "queued"},
		FieldUpdatedAt: map[string]time.Time{"render_status": updatedAt},
		StageUpdatedAt: &updatedAt,
		UpdatedAt:      &updatedAt,
	}}

	snapshot := state.Snapshot(now)

	if got := snapshot.BoardIssues[0].Metadata["detent.dispatch_skip_reason"]; got != dispatchSkipArtifactGateWaitStatus {
		t.Fatalf("dispatch skip reason = %q, want %q", got, dispatchSkipArtifactGateWaitStatus)
	}
	if got := snapshot.BoardIssues[0].Metadata["detent.artifact_gate_status"]; got != "pending_review" {
		t.Fatalf("artifact gate status = %q, want pending_review", got)
	}
	if got := snapshot.BoardIssues[1].Metadata["detent.dispatch_skip_reason"]; got != "" {
		t.Fatalf("seeded wait dispatch skip reason = %q, want empty", got)
	}
}

func TestStateSnapshotComputesDisabledAutoPromoteReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 14, 5, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{
		ID:         "auto-promote-disabled",
		Identifier: "digitaldrywood/detent#1108",
		State:      "Human Review",
	}, {
		ID:         "merging-disabled",
		Identifier: "digitaldrywood/detent#1109",
		State:      "Merging",
	}}

	snapshot := state.Snapshot(now)

	if got := snapshot.BoardIssues[0].Metadata[autoPromoteActionMetadataKey]; got != string(AutoPromoteActionSkip) {
		t.Fatalf("auto promote action = %q, want skip", got)
	}
	if got := snapshot.BoardIssues[0].Metadata[autoPromoteReasonMetadataKey]; got != string(AutoPromoteReasonDisabled) {
		t.Fatalf("auto promote reason = %q, want disabled", got)
	}
	if got := snapshot.BoardIssues[1].Metadata[autoPromoteReasonMetadataKey]; got != "" {
		t.Fatalf("merging auto promote reason = %q, want empty", got)
	}
}

func TestStateSnapshotIncludesStatusDrift(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.StatusDrift = connector.StatusDrift{
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
	}

	snapshot := state.Snapshot(now)

	if len(snapshot.TrackerDrift.UntrackedOpen) != 1 || snapshot.TrackerDrift.UntrackedOpen[0].Identifier != "digitaldrywood/detent#771" {
		t.Fatalf("TrackerDrift.UntrackedOpen = %#v, want #771", snapshot.TrackerDrift.UntrackedOpen)
	}
	if len(snapshot.TrackerDrift.OpenTerminal) != 1 || snapshot.TrackerDrift.OpenTerminal[0].State != "Done" {
		t.Fatalf("TrackerDrift.OpenTerminal = %#v, want Done #583", snapshot.TrackerDrift.OpenTerminal)
	}
}

func TestStateSnapshotIncludesInstanceIdentityAndScope(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	state := newState(normalizeConfig(Config{
		Authorization: selector.Selector{
			AssigneeIn: []string{"@me"},
			Labels: selector.Labels{
				Include: []string{"release"},
			},
		},
		SelectorContext: selector.Context{
			InstanceLogin: "detent-bot",
			Persona:       "release-captain",
		},
	}))

	snapshot := state.Snapshot(now)

	if snapshot.Instance.Name != "release-captain" {
		t.Fatalf("Instance.Name = %q, want release-captain", snapshot.Instance.Name)
	}
	if snapshot.Instance.GitHubLogin != "detent-bot" {
		t.Fatalf("Instance.GitHubLogin = %q, want detent-bot", snapshot.Instance.GitHubLogin)
	}
	if !snapshot.Instance.AuthorizationConfigured {
		t.Fatal("Instance.AuthorizationConfigured = false, want true")
	}
	wantScope := "assignee in @me (detent-bot, release-captain); labels include release"
	if snapshot.Instance.AuthorizationScope != wantScope {
		t.Fatalf("Instance.AuthorizationScope = %q, want %q", snapshot.Instance.AuthorizationScope, wantScope)
	}
}

func TestStateSnapshotFiltersIssueListsByAuthorization(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name              string
		cfg               Config
		boardIssues       []connector.Issue
		pipeline          []connector.Issue
		statusDrift       connector.StatusDrift
		wantBoard         []string
		wantPipeline      []string
		wantUntrackedOpen []string
		wantOpenTerminal  []string
	}{
		{
			name: "no authorization leaves board and pipeline unchanged",
			cfg:  Config{},
			boardIssues: []connector.Issue{
				{ID: "mine", AuthorID: "detent-bot", State: "Todo"},
				{ID: "teammate", AuthorID: "teammate", State: "In Progress"},
			},
			pipeline: []connector.Issue{
				{ID: "review", AuthorID: "teammate", State: "Human Review"},
			},
			statusDrift: connector.StatusDrift{
				UntrackedOpen: []connector.Issue{
					{ID: "drift-mine", AuthorID: "detent-bot", State: "Backlog"},
					{ID: "drift-teammate", AuthorID: "teammate", State: "Backlog"},
				},
				OpenTerminal: []connector.Issue{
					{ID: "done-teammate", AuthorID: "teammate", State: "Done"},
				},
			},
			wantBoard:         []string{"mine", "teammate"},
			wantPipeline:      []string{"review"},
			wantUntrackedOpen: []string{"drift-mine", "drift-teammate"},
			wantOpenTerminal:  []string{"done-teammate"},
		},
		{
			name: "author selector keeps only matching board and pipeline issues",
			cfg: Config{
				Authorization: selector.Selector{
					AuthorIn: []string{"@me"},
				},
				SelectorContext: selector.Context{
					InstanceLogin: "detent-bot",
					Persona:       "release-captain",
				},
			},
			boardIssues: []connector.Issue{
				{ID: "mine", AuthorID: "detent-bot", State: "Todo"},
				{ID: "persona", AuthorID: "release-captain", State: "In Progress"},
				{ID: "teammate", AuthorID: "teammate", State: "Todo"},
			},
			pipeline: []connector.Issue{
				{ID: "review-mine", AuthorID: "detent-bot", State: "Human Review"},
				{ID: "review-teammate", AuthorID: "teammate", State: "Human Review"},
			},
			statusDrift: connector.StatusDrift{
				UntrackedOpen: []connector.Issue{
					{ID: "drift-mine", AuthorID: "detent-bot", State: "Backlog"},
					{ID: "drift-persona", AuthorID: "release-captain", State: "Backlog"},
					{ID: "drift-teammate", AuthorID: "teammate", State: "Backlog"},
				},
				OpenTerminal: []connector.Issue{
					{ID: "done-mine", AuthorID: "detent-bot", State: "Done"},
					{ID: "done-teammate", AuthorID: "teammate", State: "Done"},
				},
			},
			wantBoard:         []string{"mine", "persona"},
			wantPipeline:      []string{"review-mine"},
			wantUntrackedOpen: []string{"drift-mine", "drift-persona"},
			wantOpenTerminal:  []string{"done-mine"},
		},
		{
			name: "compound selector uses selector match semantics",
			cfg: Config{
				Authorization: selector.Selector{
					Labels:     selector.Labels{Include: []string{"release"}},
					AssigneeIn: []string{"@me"},
				},
				SelectorContext: selector.Context{
					InstanceLogin: "detent-bot",
				},
			},
			boardIssues: []connector.Issue{
				{ID: "assigned-release", Labels: []string{"release"}, Assignees: []string{"detent-bot"}, State: "Todo"},
				{ID: "assigned-other-label", Labels: []string{"bug"}, Assignees: []string{"detent-bot"}, State: "Todo"},
				{ID: "release-unassigned", Labels: []string{"release"}, Assignees: []string{"teammate"}, State: "Todo"},
			},
			pipeline: []connector.Issue{
				{ID: "pipeline-release", Labels: []string{"release"}, Assignees: []string{"detent-bot"}, State: "Merging"},
				{ID: "pipeline-other", Labels: []string{"release"}, Assignees: []string{"teammate"}, State: "Merging"},
			},
			statusDrift: connector.StatusDrift{
				UntrackedOpen: []connector.Issue{
					{ID: "drift-release", Labels: []string{"release"}, Assignees: []string{"detent-bot"}, State: "Backlog"},
					{ID: "drift-other-label", Labels: []string{"bug"}, Assignees: []string{"detent-bot"}, State: "Backlog"},
					{ID: "drift-unassigned", Labels: []string{"release"}, Assignees: []string{"teammate"}, State: "Backlog"},
				},
				OpenTerminal: []connector.Issue{
					{ID: "done-release", Labels: []string{"release"}, Assignees: []string{"detent-bot"}, State: "Done"},
					{ID: "done-unassigned", Labels: []string{"release"}, Assignees: []string{"teammate"}, State: "Done"},
				},
			},
			wantBoard:         []string{"assigned-release"},
			wantPipeline:      []string{"pipeline-release"},
			wantUntrackedOpen: []string{"drift-release"},
			wantOpenTerminal:  []string{"done-release"},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(normalizeConfig(tt.cfg))
			state.BoardIssues = tt.boardIssues
			state.Pipeline = tt.pipeline
			state.StatusDrift = tt.statusDrift

			snapshot := state.Snapshot(now)

			if got := telemetryIssueIDs(snapshot.BoardIssues); !slices.Equal(got, tt.wantBoard) {
				t.Fatalf("BoardIssues ids = %#v, want %#v", got, tt.wantBoard)
			}
			if got := telemetryIssueIDs(snapshot.Pipeline); !slices.Equal(got, tt.wantPipeline) {
				t.Fatalf("Pipeline ids = %#v, want %#v", got, tt.wantPipeline)
			}
			if got := telemetryIssueIDs(snapshot.TrackerDrift.UntrackedOpen); !slices.Equal(got, tt.wantUntrackedOpen) {
				t.Fatalf("TrackerDrift.UntrackedOpen ids = %#v, want %#v", got, tt.wantUntrackedOpen)
			}
			if got := telemetryIssueIDs(snapshot.TrackerDrift.OpenTerminal); !slices.Equal(got, tt.wantOpenTerminal) {
				t.Fatalf("TrackerDrift.OpenTerminal ids = %#v, want %#v", got, tt.wantOpenTerminal)
			}
		})
	}
}

func TestStateSnapshotFiltersRuntimeRowsUsingCurrentAuthorization(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 16, 0, 0, 0, time.UTC)
	issue := func(id, state string, authorized bool) connector.Issue {
		labels := []string{"bug"}
		if authorized {
			labels = append(labels, "detent:cory")
		}
		return connector.Issue{ID: id, Identifier: "getparable/parable#" + id, State: state, Labels: labels}
	}
	state := newState(normalizeConfig(Config{
		Authorization: selector.Selector{Labels: selector.Labels{Include: []string{"detent:cory"}}},
	}))
	state.BoardIssues = []connector.Issue{
		issue("authorized-queue", "Todo", true),
		issue("unauthorized-running", "In Progress", false),
	}
	state.Retry["authorized-queue"] = Retry{Issue: issue("authorized-queue", "Todo", false), Attempt: 2}
	state.Retry["unauthorized-queue"] = Retry{Issue: issue("unauthorized-queue", "Todo", false), Attempt: 2}
	state.Running["authorized-running"] = Running{Issue: issue("authorized-running", "In Progress", true), Attempt: 1, StartedAt: now.Add(-time.Hour)}
	state.Running["unauthorized-running"] = Running{Issue: issue("unauthorized-running", "In Progress", true), Attempt: 1, StartedAt: now.Add(-time.Hour)}
	state.Blocked["authorized-blocked"] = Blocked{Issue: issue("authorized-blocked", "Blocked", true), Attempt: 1, BlockedAt: now.Add(-time.Hour)}
	state.Blocked["unauthorized-blocked"] = Blocked{Issue: issue("unauthorized-blocked", "Blocked", false), Attempt: 1, BlockedAt: now.Add(-time.Hour)}
	state.Blocked["authorized-dependency-wait"] = Blocked{
		Issue:     issue("authorized-dependency-wait", "Todo", true),
		Reason:    "waiting for getparable/parable#2230 Open",
		Source:    BlockedSourceDependency,
		BlockedAt: now.Add(-time.Hour),
	}
	state.Blocked["unauthorized-dependency-wait"] = Blocked{
		Issue:     issue("unauthorized-dependency-wait", "Todo", false),
		Reason:    "waiting for getparable/parable#2230 Open",
		Source:    BlockedSourceDependency,
		BlockedAt: now.Add(-time.Hour),
	}
	state.Completed["authorized-completed"] = Completed{Issue: issue("authorized-completed", "Done", true), CompletedAt: now.Add(-time.Hour)}
	state.Completed["unauthorized-completed"] = Completed{Issue: issue("unauthorized-completed", "Done", false), CompletedAt: now.Add(-time.Hour)}
	state.WorkAttempts = []telemetry.WorkAttempt{
		{AttemptID: 1, IssueID: "authorized-completed", Identifier: "getparable/parable#authorized-completed", Status: string(store.WorkAttemptStatusTerminal), CompletedAt: timePointer(now.Add(-time.Hour))},
		{AttemptID: 2, IssueID: "unauthorized-completed", Identifier: "getparable/parable#unauthorized-completed", Status: string(store.WorkAttemptStatusTerminal), CompletedAt: timePointer(now.Add(-time.Hour))},
	}

	snapshot := state.Snapshot(now)
	tests := []struct {
		name string
		got  []telemetry.Issue
		want []string
	}{
		{name: "queue", got: queuedTelemetryIssues(snapshot.Queue), want: []string{"authorized-queue"}},
		{name: "running", got: runningTelemetryIssues(snapshot.Running), want: []string{"authorized-running"}},
		{name: "blocked", got: blockedTelemetryIssues(snapshot.Blocked), want: []string{"authorized-blocked", "authorized-dependency-wait"}},
		{name: "completed", got: completedTelemetryIssues(snapshot.Completed), want: []string{"authorized-completed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := telemetryIssueIDs(tt.got); !slices.Equal(got, tt.want) {
				t.Fatalf("runtime issue ids = %#v, want %#v", got, tt.want)
			}
		})
	}

	wantCounts := telemetry.Counts{Running: 1, Queue: 1, Blocked: 2, Completed: 1}
	if snapshot.Counts != wantCounts {
		t.Fatalf("Counts = %#v, want %#v", snapshot.Counts, wantCounts)
	}
	if len(snapshot.WorkAttempts) != 1 || snapshot.WorkAttempts[0].IssueID != "authorized-completed" {
		t.Fatalf("WorkAttempts = %#v, want only authorized-completed", snapshot.WorkAttempts)
	}
	wantLaneCounts := []telemetry.BoardStateCount{
		{State: "Todo", Count: 2},
		{State: "In Progress", Count: 1},
		{State: "Blocked", Count: 1},
	}
	if got := telemetry.BoardStateCounts(snapshot); !reflect.DeepEqual(got, wantLaneCounts) {
		t.Fatalf("BoardStateCounts() = %#v, want %#v", got, wantLaneCounts)
	}
	wantWorkload := telemetry.BoardWorkloadCounts{Load: 3, Todo: 1, Active: 1, Waiting: 1, Blocked: 1}
	if got := telemetry.BoardWorkload(snapshot); got != wantWorkload {
		t.Fatalf("BoardWorkload() = %#v, want %#v", got, wantWorkload)
	}
}

func TestStateSnapshotCarriesIssueComments(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	commentCreatedAt := now.Add(-30 * time.Second)
	commentUpdatedAt := now.Add(-15 * time.Second)
	state := newState(normalizeConfig(Config{}))
	state.Running["issue-1"] = Running{
		Issue: connector.Issue{
			ID:         "issue-1",
			Identifier: "digitaldrywood/detent#1",
			State:      "In Progress",
			Comments: []connector.IssueComment{{
				ID:          "IC_1",
				Backend:     connector.BackendGitHub.String(),
				Body:        "## Codex Workpad\n\n### Status\nIn Progress",
				URL:         "https://github.test/comment/1",
				AuthorLogin: "detent-bot",
				CreatedAt:   &commentCreatedAt,
				UpdatedAt:   &commentUpdatedAt,
				TargetType:  connector.IssueCommentTargetIssue,
			}},
		},
		StartedAt: now.Add(-time.Minute),
	}

	snapshot := state.Snapshot(now)

	if len(snapshot.Running) != 1 {
		t.Fatalf("Running len = %d, want 1", len(snapshot.Running))
	}
	comments := snapshot.Running[0].Comments
	if len(comments) != 1 {
		t.Fatalf("Running comments = %#v, want one comment", comments)
	}
	if comments[0].ID != "IC_1" ||
		comments[0].Backend != connector.BackendGitHub.String() ||
		comments[0].Body != "## Codex Workpad\n\n### Status\nIn Progress" ||
		comments[0].URL != "https://github.test/comment/1" ||
		comments[0].AuthorLogin != "detent-bot" ||
		comments[0].TargetType != connector.IssueCommentTargetIssue {
		t.Fatalf("Running comment = %#v, want normalized Workpad metadata", comments[0])
	}
	if comments[0].CreatedAt == nil || !comments[0].CreatedAt.Equal(commentCreatedAt) {
		t.Fatalf("Running comment CreatedAt = %v, want %v", comments[0].CreatedAt, commentCreatedAt)
	}
	if comments[0].UpdatedAt == nil || !comments[0].UpdatedAt.Equal(commentUpdatedAt) {
		t.Fatalf("Running comment UpdatedAt = %v, want %v", comments[0].UpdatedAt, commentUpdatedAt)
	}
}

func TestStateSnapshotCarriesArtifactDeliverableMetadata(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.Pipeline = []connector.Issue{{
		ID:         "ad-1",
		Identifier: "store/ad-1",
		Number:     8,
		Title:      "Summer sale video ad",
		State:      "Review",
		Metadata:   map[string]string{"store": "creswood"},
		Deliverable: &connector.Deliverable{
			Kind:             "video_ad",
			Path:             "outputs/ad-1/manifest.json",
			ReviewURL:        "http://127.0.0.1:8080/review/ad-1",
			ValidationStatus: "pending",
			ExternalID:       "creative-101",
			Metadata:         map[string]string{"aspect_ratio": "9:16"},
		},
	}}

	snapshot := state.Snapshot(now)
	if len(snapshot.Pipeline) != 1 {
		t.Fatalf("Pipeline len = %d, want 1", len(snapshot.Pipeline))
	}
	got := snapshot.Pipeline[0]
	if got.Metadata["store"] != "creswood" {
		t.Fatalf("Metadata = %#v", got.Metadata)
	}
	if got.Number != 8 {
		t.Fatalf("Number = %d, want 8", got.Number)
	}
	if got.Deliverable == nil {
		t.Fatal("Deliverable = nil, want artifact deliverable")
	}
	if got.Deliverable.Kind != "video_ad" ||
		got.Deliverable.Path != "outputs/ad-1/manifest.json" ||
		got.Deliverable.ReviewURL != "http://127.0.0.1:8080/review/ad-1" ||
		got.Deliverable.ValidationStatus != "pending" ||
		got.Deliverable.ExternalID != "creative-101" ||
		got.Deliverable.Metadata["aspect_ratio"] != "9:16" {
		t.Fatalf("Deliverable = %#v", got.Deliverable)
	}
}

func TestStateSnapshotIncludesAuthHealth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	failedAt := now.Add(-time.Minute)
	recoveredAt := now.Add(-30 * time.Second)
	state := newState(normalizeConfig(Config{}))
	state.Auth = connector.AuthHealth{
		Status:          connector.AuthStatusRecovered,
		LastError:       "github authentication failed: status 401",
		LastErrorAt:     failedAt,
		LastRecoveredAt: recoveredAt,
	}

	snapshot := state.Snapshot(now)

	if snapshot.Auth.Status != telemetry.AuthStatusRecovered {
		t.Fatalf("snapshot Auth.Status = %q, want %q", snapshot.Auth.Status, telemetry.AuthStatusRecovered)
	}
	if snapshot.Auth.LastError != "github authentication failed: status 401" {
		t.Fatalf("snapshot Auth.LastError = %q", snapshot.Auth.LastError)
	}
	if snapshot.Auth.LastErrorAt == nil || !snapshot.Auth.LastErrorAt.Equal(failedAt) {
		t.Fatalf("snapshot Auth.LastErrorAt = %v, want %v", snapshot.Auth.LastErrorAt, failedAt)
	}
	if snapshot.Auth.LastRecoveredAt == nil || !snapshot.Auth.LastRecoveredAt.Equal(recoveredAt) {
		t.Fatalf("snapshot Auth.LastRecoveredAt = %v, want %v", snapshot.Auth.LastRecoveredAt, recoveredAt)
	}
}

func TestStateSnapshotIncludesClaimLeaseState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 2, 15, 0, 0, 0, time.UTC)
	renewedAt := now.Add(-30 * time.Second)
	expiresAt := renewedAt.Add(time.Minute)
	state := newState(normalizeConfig(Config{}))
	state.Running["issue-1"] = Running{
		Issue:     connector.Issue{ID: "issue-1", Identifier: "digitaldrywood/detent#1", Title: "Claimed", State: "Todo"},
		StartedAt: now.Add(-time.Minute),
	}
	state.Claimed["issue-1"] = Claimed{
		Issue:          state.Running["issue-1"].Issue,
		ClaimedAt:      now.Add(-time.Minute),
		Owner:          "alpha",
		LeaseRenewedAt: renewedAt,
		LeaseExpiresAt: expiresAt,
	}
	state.Retry["issue-2"] = Retry{
		Issue:   connector.Issue{ID: "issue-2", Identifier: "digitaldrywood/detent#2", Title: "Retry", State: "Todo"},
		Attempt: 2,
		DueAt:   now.Add(time.Minute),
	}
	state.Claimed["issue-2"] = Claimed{
		Issue:          state.Retry["issue-2"].Issue,
		ClaimedAt:      now.Add(-2 * time.Minute),
		Owner:          "beta",
		LeaseRenewedAt: now.Add(-2 * time.Minute),
		LeaseExpiresAt: now.Add(-time.Minute),
	}

	snapshot := state.Snapshot(now)

	if len(snapshot.Running) != 1 {
		t.Fatalf("Running len = %d, want 1", len(snapshot.Running))
	}
	running := snapshot.Running[0]
	if running.Owner != "alpha" {
		t.Fatalf("Running owner = %q, want alpha", running.Owner)
	}
	if running.LeaseRenewedAt == nil || !running.LeaseRenewedAt.Equal(renewedAt) {
		t.Fatalf("Running lease renewed = %v, want %v", running.LeaseRenewedAt, renewedAt)
	}
	if running.LeaseExpiresAt == nil || !running.LeaseExpiresAt.Equal(expiresAt) {
		t.Fatalf("Running lease expires = %v, want %v", running.LeaseExpiresAt, expiresAt)
	}
	if running.LeaseStale {
		t.Fatal("Running lease stale = true, want false")
	}
	if len(snapshot.Queue) != 1 {
		t.Fatalf("Queue len = %d, want 1", len(snapshot.Queue))
	}
	queued := snapshot.Queue[0]
	if queued.Owner != "beta" {
		t.Fatalf("Queued owner = %q, want beta", queued.Owner)
	}
	if !queued.LeaseStale {
		t.Fatal("Queued lease stale = false, want true")
	}
}

func TestStateSnapshotPopulated(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	startedAt := now.Add(-2 * time.Minute)
	dueAt := now.Add(30 * time.Second)
	completedAt := now.Add(-time.Minute)
	blockedAt := now.Add(-3 * time.Minute)
	pipelineUpdatedAt := now.Add(-4 * time.Minute)
	prHydrationRetryAt := now.Add(5 * time.Minute)

	state := newState(normalizeConfig(Config{}))
	state.LastRefreshAt = now.Add(-30 * time.Second)
	state.NextRefreshAt = now.Add(30 * time.Second)
	state.BoardIssues = []connector.Issue{
		{
			ID:             "i-board",
			Identifier:     "ISS-BOARD",
			Title:          "Backlog board issue",
			State:          "Backlog",
			StageUpdatedAt: &pipelineUpdatedAt,
		},
	}
	state.Pipeline = []connector.Issue{
		{
			ID:         "i-pr",
			Identifier: "ISS-PR",
			Title:      "Pipeline PR",
			State:      "Human Review",
			Labels:     []string{"enhancement"},
			Assignees:  []string{"release-captain"},
			BlockedBy:  []connector.BlockedRef{{Identifier: "digitaldrywood/detent#415", State: "Done"}},
			UpdatedAt:  &pipelineUpdatedAt,
			PullRequest: &connector.PullRequest{
				Number:                  142,
				URL:                     "https://github.com/digitaldrywood/detent/pull/142",
				State:                   "OPEN",
				CIStatus:                "pending",
				CIQueueSeconds:          120,
				CIDurationSeconds:       900,
				HydrationDegradedReason: connector.PullRequestHydrationReasonStaleCachedPullData,
				HydrationNextRetryAt:    &prHydrationRetryAt,
				SlowChecks: []connector.PullRequestCheck{
					{Name: "GoReleaser Snapshot", DurationSeconds: 247, QueueSeconds: 60},
				},
				RunningChecks: []string{"Test Coverage"},
				UnstartedChecks: []connector.PullRequestCheck{
					{Name: "Portability Verify", Status: "queued", QueueSeconds: 47 * 60},
				},
				CodexReviewState: "P2",
			},
		},
	}
	state.Running["i-2"] = Running{
		Issue:           connector.Issue{ID: "i-2", Identifier: "ISS-2", Title: "Two", State: "In Progress", URL: "u2"},
		Attempt:         1,
		StartedAt:       startedAt,
		WorkerHost:      "host-b",
		ProcessIdentity: "4242",
		WorkspacePath:   "/tmp/detent-workspaces/i-2",
		SessionID:       "thread-2-turn-2",
		TurnCount:       2,
		LastEventAt:     now.Add(-10 * time.Second),
		LastEvent:       "agent_message_delta",
		LastMessage:     "editing dashboard telemetry",
		RecentEvents: []telemetry.ActivityEvent{
			{At: now.Add(-12 * time.Second), Event: "turn_started", Message: "turn started"},
			{At: now.Add(-10 * time.Second), Event: "agent_message_delta", Message: "editing dashboard telemetry"},
		},
		DiffStats: DiffStats{FilesChanged: 3, AddedLines: 12, RemovedLines: 4, Status: "ok"},
		Tokens:    TokenTotals{InputTokens: 20, OutputTokens: 8, TotalTokens: 28, RuntimeSeconds: 30},
	}
	state.Running["i-1"] = Running{
		Issue:     connector.Issue{ID: "i-1", Identifier: "ISS-1", Title: "One", State: "In Progress"},
		Attempt:   0,
		StartedAt: startedAt,
		TurnCount: 1,
		Tokens:    TokenTotals{InputTokens: 2, OutputTokens: 1, TotalTokens: 3, RuntimeSeconds: 15},
	}
	state.Retry["i-3"] = Retry{
		Issue:      connector.Issue{ID: "i-3", Identifier: "ISS-3", Title: "Three", State: "Todo"},
		Attempt:    2,
		DueAt:      dueAt,
		Error:      "boom",
		WorkerHost: "host-c",
	}
	state.Blocked["i-4"] = Blocked{
		Issue:     connector.Issue{ID: "i-4", Identifier: "ISS-4", Title: "Four", State: "Todo"},
		Reason:    "blocked by non-terminal dependency",
		BlockedAt: blockedAt,
		Source:    BlockedSourceDependency,
	}
	state.Completed["i-5"] = Completed{
		Issue:       connector.Issue{ID: "i-5", Identifier: "ISS-5", Title: "Five", State: "Done"},
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		FinalState:  FinalStateCompleted,
		Tokens:      TokenTotals{InputTokens: 10, OutputTokens: 5, TotalTokens: 15, RuntimeSeconds: 60},
	}
	state.TokenTotals = TokenTotals{InputTokens: 100, OutputTokens: 50, TotalTokens: 150, RuntimeSeconds: 120}
	state.RateLimits = &telemetry.RateLimits{LimitID: "lim", LimitName: "name"}

	snapshot := state.Snapshot(now)

	wantCounts := telemetry.Counts{Running: 2, Queue: 1, Blocked: 1, Completed: 1}
	if snapshot.Counts != wantCounts {
		t.Fatalf("Counts = %#v, want %#v", snapshot.Counts, wantCounts)
	}

	if len(snapshot.BoardIssues) != 1 {
		t.Fatalf("BoardIssues len = %d, want 1", len(snapshot.BoardIssues))
	}
	boardIssue := snapshot.BoardIssues[0]
	if boardIssue.ID != "i-board" || boardIssue.State != "Backlog" || boardIssue.StageUpdatedAt == nil || !boardIssue.StageUpdatedAt.Equal(pipelineUpdatedAt) {
		t.Fatalf("BoardIssues[0] = %#v", boardIssue)
	}

	if len(snapshot.Pipeline) != 1 {
		t.Fatalf("Pipeline len = %d, want 1", len(snapshot.Pipeline))
	}
	pipeline := snapshot.Pipeline[0]
	if pipeline.ID != "i-pr" || pipeline.State != "Human Review" || pipeline.UpdatedAt == nil || !pipeline.UpdatedAt.Equal(pipelineUpdatedAt) {
		t.Fatalf("Pipeline[0] = %#v", pipeline)
	}
	if len(pipeline.Assignees) != 1 || pipeline.Assignees[0] != "release-captain" {
		t.Fatalf("Pipeline[0].Assignees = %#v, want release-captain", pipeline.Assignees)
	}
	if len(pipeline.BlockedBy) != 1 || pipeline.BlockedBy[0].Identifier != "digitaldrywood/detent#415" || pipeline.BlockedBy[0].State != "Done" {
		t.Fatalf("Pipeline[0].BlockedBy = %#v, want dependency ref", pipeline.BlockedBy)
	}
	if pipeline.PullRequest == nil || pipeline.PullRequest.Number != 142 || pipeline.PullRequest.CIStatus != "pending" || pipeline.PullRequest.CodexReviewState != "P2" {
		t.Fatalf("Pipeline[0].PullRequest = %#v", pipeline.PullRequest)
	}
	if pipeline.PullRequest.CIDurationSeconds != 900 {
		t.Fatalf("Pipeline[0].PullRequest.CIDurationSeconds = %d, want 900", pipeline.PullRequest.CIDurationSeconds)
	}
	if pipeline.PullRequest.CIQueueSeconds != 120 {
		t.Fatalf("Pipeline[0].PullRequest.CIQueueSeconds = %d, want 120", pipeline.PullRequest.CIQueueSeconds)
	}
	if pipeline.PullRequest.HydrationDegradedReason != connector.PullRequestHydrationReasonStaleCachedPullData {
		t.Fatalf("Pipeline[0].PullRequest.HydrationDegradedReason = %q, want stale cached data", pipeline.PullRequest.HydrationDegradedReason)
	}
	if pipeline.PullRequest.HydrationNextRetryAt == nil || !pipeline.PullRequest.HydrationNextRetryAt.Equal(prHydrationRetryAt) {
		t.Fatalf("Pipeline[0].PullRequest.HydrationNextRetryAt = %v, want %v", pipeline.PullRequest.HydrationNextRetryAt, prHydrationRetryAt)
	}
	if len(pipeline.PullRequest.SlowChecks) != 1 || pipeline.PullRequest.SlowChecks[0].Name != "GoReleaser Snapshot" {
		t.Fatalf("Pipeline[0].PullRequest.SlowChecks = %#v", pipeline.PullRequest.SlowChecks)
	}
	if len(pipeline.PullRequest.RunningChecks) != 1 || pipeline.PullRequest.RunningChecks[0] != "Test Coverage" {
		t.Fatalf("Pipeline[0].PullRequest.RunningChecks = %#v", pipeline.PullRequest.RunningChecks)
	}
	if len(pipeline.PullRequest.UnstartedChecks) != 1 || pipeline.PullRequest.UnstartedChecks[0].Name != "Portability Verify" || pipeline.PullRequest.UnstartedChecks[0].QueueSeconds != 47*60 {
		t.Fatalf("Pipeline[0].PullRequest.UnstartedChecks = %#v", pipeline.PullRequest.UnstartedChecks)
	}

	if len(snapshot.Running) != 2 {
		t.Fatalf("Running len = %d, want 2", len(snapshot.Running))
	}
	// Deterministic ordering by issue id.
	if snapshot.Running[0].ID != "i-1" || snapshot.Running[1].ID != "i-2" {
		t.Fatalf("Running order = [%s, %s], want [i-1, i-2]", snapshot.Running[0].ID, snapshot.Running[1].ID)
	}
	if snapshot.Running[1].WorkerHost != "host-b" {
		t.Fatalf("Running[1].WorkerHost = %q, want host-b", snapshot.Running[1].WorkerHost)
	}
	if snapshot.Running[1].ProcessIdentity != "4242" {
		t.Fatalf("Running[1].ProcessIdentity = %q, want 4242", snapshot.Running[1].ProcessIdentity)
	}
	if snapshot.Running[1].WorkspacePath != "/tmp/detent-workspaces/i-2" {
		t.Fatalf("Running[1].WorkspacePath = %q, want /tmp/detent-workspaces/i-2", snapshot.Running[1].WorkspacePath)
	}
	if snapshot.Running[0].Identifier != "ISS-1" || snapshot.Running[0].Title != "One" {
		t.Fatalf("Running[0] issue mapping = %#v", snapshot.Running[0].Issue)
	}
	if !snapshot.Running[0].StartedAt.Equal(startedAt) {
		t.Fatalf("Running[0].StartedAt = %v, want %v", snapshot.Running[0].StartedAt, startedAt)
	}
	if snapshot.Running[0].TurnCount != 1 || snapshot.Running[0].Tokens.Total != 3 {
		t.Fatalf("Running[0] live usage = turns %d tokens %#v, want 1 turn and 3 tokens", snapshot.Running[0].TurnCount, snapshot.Running[0].Tokens)
	}
	if snapshot.Running[1].RuntimeSeconds != 30 {
		t.Fatalf("Running[1].RuntimeSeconds = %v, want 30", snapshot.Running[1].RuntimeSeconds)
	}
	if snapshot.Running[1].SessionID != "thread-2-turn-2" || snapshot.Running[1].LastEvent != "agent_message_delta" {
		t.Fatalf("Running[1] live activity = %#v", snapshot.Running[1])
	}
	if snapshot.Running[1].LastEventAt == nil || !snapshot.Running[1].LastEventAt.Equal(now.Add(-10*time.Second)) {
		t.Fatalf("Running[1].LastEventAt = %v", snapshot.Running[1].LastEventAt)
	}
	if snapshot.Running[1].LastMessage != "editing dashboard telemetry" {
		t.Fatalf("Running[1].LastMessage = %q", snapshot.Running[1].LastMessage)
	}
	if len(snapshot.Running[1].RecentEvents) != 2 || snapshot.Running[1].RecentEvents[1].Event != "agent_message_delta" {
		t.Fatalf("Running[1].RecentEvents = %#v", snapshot.Running[1].RecentEvents)
	}
	if snapshot.Running[1].DiffFiles != 3 || snapshot.Running[1].DiffAdded != 12 || snapshot.Running[1].DiffRemoved != 4 || snapshot.Running[1].DiffStatus != "ok" {
		t.Fatalf("Running[1] diff = %#v", snapshot.Running[1])
	}

	if len(snapshot.Queue) != 1 {
		t.Fatalf("Queue len = %d, want 1", len(snapshot.Queue))
	}
	q := snapshot.Queue[0]
	if q.ID != "i-3" || q.Attempt != 2 || q.Error != "boom" || q.WorkerHost != "host-c" {
		t.Fatalf("Queue[0] = %#v", q)
	}
	if q.DueAt == nil || !q.DueAt.Equal(dueAt) {
		t.Fatalf("Queue[0].DueAt = %v, want %v", q.DueAt, dueAt)
	}

	if len(snapshot.Blocked) != 1 {
		t.Fatalf("Blocked len = %d, want 1", len(snapshot.Blocked))
	}
	b := snapshot.Blocked[0]
	if b.ID != "i-4" || b.Error != "blocked by non-terminal dependency" || b.Source != telemetry.BlockedSourceDependency {
		t.Fatalf("Blocked[0] = %#v", b)
	}
	if b.BlockedAt == nil || !b.BlockedAt.Equal(blockedAt) {
		t.Fatalf("Blocked[0].BlockedAt = %v, want %v", b.BlockedAt, blockedAt)
	}

	if len(snapshot.Completed) != 1 {
		t.Fatalf("Completed len = %d, want 1", len(snapshot.Completed))
	}
	c := snapshot.Completed[0]
	if c.ID != "i-5" || c.FinalState != FinalStateCompleted {
		t.Fatalf("Completed[0] = %#v", c)
	}
	if !c.CompletedAt.Equal(completedAt) {
		t.Fatalf("Completed[0].CompletedAt = %v, want %v", c.CompletedAt, completedAt)
	}
	if c.Tokens.Total != 15 {
		t.Fatalf("Completed[0].Tokens.Total = %d, want 15", c.Tokens.Total)
	}

	wantTokens := telemetry.Tokens{Input: 122, Output: 59, Total: 181, RuntimeSeconds: 165}
	if snapshot.Tokens != wantTokens {
		t.Fatalf("Tokens = %#v, want %#v", snapshot.Tokens, wantTokens)
	}
	if snapshot.RateLimits == nil || snapshot.RateLimits.LimitID != "lim" {
		t.Fatalf("RateLimits = %#v, want lim", snapshot.RateLimits)
	}
	if snapshot.Refresh.PollIntervalSeconds != 30 || snapshot.Refresh.LastRefreshAt == nil || snapshot.Refresh.NextRefreshAt == nil {
		t.Fatalf("Refresh = %#v, want poll interval and refresh timestamps", snapshot.Refresh)
	}
}

func TestStateSnapshotIncludesPullRequestMergeWaitTelemetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 14, 15, 30, 0, 0, time.UTC)
	stageUpdatedAt := now.Add(-5 * time.Minute)
	prActivityAt := stageUpdatedAt.Add(-12 * time.Minute)
	reviewSubmittedAt := stageUpdatedAt.Add(-10 * time.Minute)
	state := newState(normalizeConfig(Config{}))
	state.AutoPromoteQuietDuration = 10 * time.Minute
	state.PollInterval = time.Minute
	state.Pipeline = []connector.Issue{
		{
			ID:             "merge",
			Identifier:     "digitaldrywood/detent#461",
			Title:          "Merge wait telemetry",
			State:          "Merging",
			StageUpdatedAt: &stageUpdatedAt,
			PullRequest: &connector.PullRequest{
				Number:                 461,
				ActivityAt:             &prActivityAt,
				CodexReviewSubmittedAt: &reviewSubmittedAt,
				CIQueueSeconds:         120,
				CIDurationSeconds:      480,
				SlowChecks: []connector.PullRequestCheck{
					{Name: "GoReleaser Snapshot", DurationSeconds: 247, QueueSeconds: 60},
				},
				RunningChecks: []string{"Test Coverage"},
			},
		},
	}

	snapshot := state.Snapshot(now)
	if len(snapshot.Pipeline) != 1 || snapshot.Pipeline[0].PullRequest == nil {
		t.Fatalf("Pipeline = %#v, want one PR pipeline row", snapshot.Pipeline)
	}
	pr := snapshot.Pipeline[0].PullRequest
	if pr.QuietWaitSeconds != 600 {
		t.Fatalf("QuietWaitSeconds = %d, want 600", pr.QuietWaitSeconds)
	}
	if pr.CIDurationSeconds != 480 {
		t.Fatalf("CIDurationSeconds = %d, want 480", pr.CIDurationSeconds)
	}
	if pr.CIQueueSeconds != 120 {
		t.Fatalf("CIQueueSeconds = %d, want 120", pr.CIQueueSeconds)
	}
	if len(pr.SlowChecks) != 1 || pr.SlowChecks[0].Name != "GoReleaser Snapshot" {
		t.Fatalf("SlowChecks = %#v", pr.SlowChecks)
	}
	if len(pr.RunningChecks) != 1 || pr.RunningChecks[0] != "Test Coverage" {
		t.Fatalf("RunningChecks = %#v", pr.RunningChecks)
	}
}

func TestStateSnapshotIncludesMergeTimingTelemetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	enteredAt := now.Add(-9 * time.Minute)
	slotAcquiredAt := now.Add(-7 * time.Minute)
	startedAt := now.Add(-6 * time.Minute)
	mergedAt := now.Add(-2 * time.Minute)
	state := newState(normalizeConfig(Config{}))
	state.Pipeline = []connector.Issue{{
		ID:             "queued",
		Identifier:     "digitaldrywood/detent#721",
		State:          "Merging",
		StageUpdatedAt: &enteredAt,
		PullRequest: &connector.PullRequest{
			Number:  721,
			HeadSHA: "queued-head",
			BaseSHA: "queued-base",
		},
	}}
	state.Running["active"] = Running{
		Issue: connector.Issue{
			ID:         "active",
			Identifier: "digitaldrywood/detent#722",
			State:      "Merging",
			PullRequest: &connector.PullRequest{
				Number:  722,
				HeadSHA: "active-head",
				BaseSHA: "active-base",
			},
		},
		StartedAt: startedAt,
	}
	state.Completed["done"] = Completed{
		Issue: connector.Issue{
			ID:         "done",
			Identifier: "digitaldrywood/detent#723",
			State:      "Merging",
			PullRequest: &connector.PullRequest{
				Number:  723,
				HeadSHA: "done-head",
				BaseSHA: "done-base",
			},
		},
		StartedAt:   startedAt,
		CompletedAt: mergedAt,
		FinalState:  "Done",
		MergeTiming: MergeTiming{
			EnteredMergingAt:           enteredAt,
			MergeWorkerSlotAcquiredAt:  slotAcquiredAt,
			MergeStartedAt:             startedAt,
			MergedAt:                   mergedAt,
			QueueWaitSeconds:           120,
			ActiveMergeDurationSeconds: 240,
			TotalMergingSeconds:        420,
		},
	}
	state.MergeTimings["queued"] = MergeTiming{EnteredMergingAt: enteredAt}
	state.MergeTimings["active"] = MergeTiming{
		EnteredMergingAt:          enteredAt,
		MergeWorkerSlotAcquiredAt: slotAcquiredAt,
		MergeStartedAt:            startedAt,
	}

	snapshot := state.Snapshot(now)

	if len(snapshot.Pipeline) != 1 || snapshot.Pipeline[0].MergeTiming == nil {
		t.Fatalf("Pipeline = %#v, want queued merge timing", snapshot.Pipeline)
	}
	if snapshot.Pipeline[0].CurrentLaneAgeSeconds != 540 {
		t.Fatalf("Pipeline current lane age = %d, want 540 seconds", snapshot.Pipeline[0].CurrentLaneAgeSeconds)
	}
	queued := snapshot.Pipeline[0].MergeTiming
	if queued.QueueWaitSeconds != 540 || queued.TotalMergingSeconds != 540 || queued.HeadSHA != "queued-head" || queued.BaseSHA != "queued-base" {
		t.Fatalf("queued MergeTiming = %#v, want queue duration and PR SHAs", queued)
	}
	if len(snapshot.Running) != 1 || snapshot.Running[0].MergeTiming == nil {
		t.Fatalf("Running = %#v, want active merge timing", snapshot.Running)
	}
	active := snapshot.Running[0].MergeTiming
	if active.QueueWaitSeconds != 120 || active.ActiveMergeDurationSeconds != 420 || active.TotalMergingSeconds != 540 {
		t.Fatalf("active MergeTiming = %#v, want live active durations", active)
	}
	if len(snapshot.Completed) != 1 || snapshot.Completed[0].MergeTiming == nil {
		t.Fatalf("Completed = %#v, want completed merge timing", snapshot.Completed)
	}
	done := snapshot.Completed[0].MergeTiming
	if done.QueueWaitSeconds != 120 || done.ActiveMergeDurationSeconds != 300 || done.TotalMergingSeconds != 420 || done.MergedAt == nil {
		t.Fatalf("completed MergeTiming = %#v, want terminal durations", done)
	}
}

func TestStateSnapshotOmitsStalePullRequestQuietWait(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 14, 15, 30, 0, 0, time.UTC)
	stageUpdatedAt := now.Add(-5 * time.Minute)
	prActivityAt := stageUpdatedAt.Add(-3 * time.Hour)
	state := newState(normalizeConfig(Config{
		PollInterval: time.Minute,
		AutoPromote: AutoPromoteConfig{
			QuietDuration: 10 * time.Minute,
		},
	}))
	state.Pipeline = []connector.Issue{
		{
			ID:             "merge",
			Identifier:     "digitaldrywood/detent#461",
			Title:          "Merge wait telemetry",
			State:          "Merging",
			StageUpdatedAt: &stageUpdatedAt,
			PullRequest: &connector.PullRequest{
				Number:     461,
				ActivityAt: &prActivityAt,
			},
		},
	}

	snapshot := state.Snapshot(now)
	if len(snapshot.Pipeline) != 1 || snapshot.Pipeline[0].PullRequest == nil {
		t.Fatalf("Pipeline = %#v, want one PR pipeline row", snapshot.Pipeline)
	}
	if got := snapshot.Pipeline[0].PullRequest.QuietWaitSeconds; got != 0 {
		t.Fatalf("QuietWaitSeconds = %d, want 0 for stale activity", got)
	}
}

func TestStateSnapshotBudgetRefusals(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	maxUSD := 12.5
	state := newState(normalizeConfig(Config{}))
	state.BudgetRefusals["i-9"] = BudgetRefusal{
		Issue:            connector.Issue{ID: "i-9", Identifier: "ISS-9"},
		Code:             string(budget.ReasonPerIssueMaxUSD),
		Message:          "too expensive",
		CurrentSpendUSD:  3,
		ProjectedCostUSD: 9,
		MaxUSD:           &maxUSD,
		RefusedAt:        now,
	}

	snapshot := state.Snapshot(now)

	if len(snapshot.Budget.Refusals) != 1 {
		t.Fatalf("Budget.Refusals len = %d, want 1", len(snapshot.Budget.Refusals))
	}
	refusal := snapshot.Budget.Refusals[0]
	if refusal.IssueID != "i-9" || refusal.Identifier != "ISS-9" || refusal.Code != string(budget.ReasonPerIssueMaxUSD) {
		t.Fatalf("Refusals[0] = %#v", refusal)
	}
	if !refusal.HardHold {
		t.Fatal("Refusals[0].HardHold = false, want true")
	}
	if refusal.MaxUSD == nil || *refusal.MaxUSD != maxUSD {
		t.Fatalf("Refusals[0].MaxUSD = %v, want %v", refusal.MaxUSD, maxUSD)
	}
}

func TestStateSnapshotDeterministicOrdering(t *testing.T) {
	t.Parallel()

	now := time.Now()
	state := newState(normalizeConfig(Config{}))
	for _, id := range []string{"c", "a", "b"} {
		state.Running[id] = Running{Issue: connector.Issue{ID: id, Identifier: id, Title: id, State: "In Progress"}}
	}

	first := state.Snapshot(now)
	second := state.Snapshot(now)

	for i := range first.Running {
		if first.Running[i].ID != second.Running[i].ID {
			t.Fatalf("non-deterministic ordering at %d: %s vs %s", i, first.Running[i].ID, second.Running[i].ID)
		}
	}
	if first.Running[0].ID != "a" || first.Running[1].ID != "b" || first.Running[2].ID != "c" {
		t.Fatalf("Running order = [%s,%s,%s], want [a,b,c]",
			first.Running[0].ID, first.Running[1].ID, first.Running[2].ID)
	}
}

func TestStateSnapshotSameLaneUpdateKeepsLaneEntry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 18, 0, 0, 0, time.UTC)
	enteredAt := now.Add(-time.Hour)
	updatedAt := now.Add(-5 * time.Minute)
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{
		ID:        "issue-1130",
		State:     "In Progress",
		UpdatedAt: &enteredAt,
	}}
	orch := &Orchestrator{}
	orch.refreshCurrentLaneEntries(t.Context(), &state, now)

	first := state.Snapshot(now)
	state.BoardIssues[0].UpdatedAt = &updatedAt
	orch.refreshCurrentLaneEntries(t.Context(), &state, now)
	second := state.Snapshot(now)

	if first.BoardIssues[0].CurrentLaneEnteredAt == nil || second.BoardIssues[0].CurrentLaneEnteredAt == nil {
		t.Fatalf("CurrentLaneEnteredAt = %v then %v, want timestamps", first.BoardIssues[0].CurrentLaneEnteredAt, second.BoardIssues[0].CurrentLaneEnteredAt)
	}
	if !second.BoardIssues[0].CurrentLaneEnteredAt.Equal(*first.BoardIssues[0].CurrentLaneEnteredAt) {
		t.Fatalf("CurrentLaneEnteredAt = %v after same-lane update, want %v", second.BoardIssues[0].CurrentLaneEnteredAt, first.BoardIssues[0].CurrentLaneEnteredAt)
	}
}

func telemetryIssueIDs(issues []telemetry.Issue) []string {
	out := make([]string, 0, len(issues))
	for _, issue := range issues {
		out = append(out, issue.ID)
	}
	return out
}

func queuedTelemetryIssues(rows []telemetry.Queued) []telemetry.Issue {
	issues := make([]telemetry.Issue, 0, len(rows))
	for _, row := range rows {
		issues = append(issues, row.Issue)
	}
	return issues
}

func runningTelemetryIssues(rows []telemetry.Running) []telemetry.Issue {
	issues := make([]telemetry.Issue, 0, len(rows))
	for _, row := range rows {
		issues = append(issues, row.Issue)
	}
	return issues
}

func blockedTelemetryIssues(rows []telemetry.Blocked) []telemetry.Issue {
	issues := make([]telemetry.Issue, 0, len(rows))
	for _, row := range rows {
		issues = append(issues, row.Issue)
	}
	return issues
}

func completedTelemetryIssues(rows []telemetry.Completed) []telemetry.Issue {
	issues := make([]telemetry.Issue, 0, len(rows))
	for _, row := range rows {
		issues = append(issues, row.Issue)
	}
	return issues
}
