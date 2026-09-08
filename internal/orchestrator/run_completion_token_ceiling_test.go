package orchestrator

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
)

func TestHandleRunResultParksSlowTokenCeilingFailureWithoutRetry(t *testing.T) {
	t.Parallel()

	tracker := &dependencyAutoUnblockConnector{}
	cfg := normalizeConfig(Config{
		ActiveStates:   []string{"Todo", "In Progress", "Rework"},
		ObservedStates: []string{"Blocked"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	orch := &Orchestrator{cfg: cfg, connector: tracker}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	orch.workflowMetrics = metrics
	state := newState(cfg)
	issue := connector.Issue{
		ID:         "issue-token-ceiling",
		Identifier: "digitaldrywood/detent#1221",
		Title:      "Token ceiling retry loop",
		State:      "In Progress",
	}
	completedAt := time.Date(2026, 7, 10, 23, 5, 0, 0, time.UTC)
	state.Running[issue.ID] = Running{
		Issue:     issue,
		Attempt:   1,
		StartedAt: completedAt.Add(-5 * time.Minute),
	}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: completedAt.Add(-5 * time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateTokenCeilingExceeded,
			Tokens:     TokenTotals{TotalTokens: 16_100_000, RuntimeSeconds: 300},
		},
		Err: &runpkg.SessionTokenCeilingError{
			TotalTokens:   16_100_000,
			CeilingTokens: 16_000_000,
			Source:        runpkg.TokenCeilingSourceAbsolute,
		},
		CompletedAt:  completedAt,
		RetryAttempt: 2,
		RetryDelay:   time.Minute,
	})

	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after token ceiling failure", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after token ceiling failure", issue.ID)
	}
	blocked, ok := state.Blocked[issue.ID]
	if !ok {
		t.Fatalf("Blocked[%q] missing after token ceiling failure", issue.ID)
	}
	if blocked.Issue.State != blockedStatusState {
		t.Fatalf("Blocked[%q].Issue.State = %q, want %q", issue.ID, blocked.Issue.State, blockedStatusState)
	}
	if blocked.RecoveryAction != "defer" || blocked.RecoveryReason != blockedRecoveryReasonBreakerCooldownActive || blocked.RecoveryTarget != "In Progress" {
		t.Fatalf("Blocked[%q] recovery = %q/%q to %q, want cooldown recovery to In Progress", issue.ID, blocked.RecoveryAction, blocked.RecoveryReason, blocked.RecoveryTarget)
	}
	if blocked.Recovery == nil ||
		blocked.Recovery.Owner != blockedRecoveryOwnerOrchestrator ||
		blocked.Recovery.Cause != "token_ceiling_circuit_breaker" ||
		blocked.Recovery.Predicate != blockedRecoveryPredicateBreakerCooldown ||
		blocked.Recovery.CauseFingerprint == "" {
		t.Fatalf("Blocked[%q].Recovery = %#v, want durable token-ceiling predicate", issue.ID, blocked.Recovery)
	}
	events := metrics.snapshot()
	if len(events) != 2 || events[1].PhaseName != blockedStatusState {
		t.Fatalf("workflow events = %#v, want Blocked lane entry", events)
	}
	laneMetadata, ok := workflowLaneMetadataFromJSON(events[1].MetadataJSON)
	if !ok || laneMetadata.BlockedRecovery == nil ||
		laneMetadata.BlockedRecovery.Cause != "token_ceiling_circuit_breaker" ||
		laneMetadata.BlockedRecovery.Predicate != blockedRecoveryPredicateBreakerCooldown ||
		laneMetadata.BlockedRecovery.CauseFingerprint == "" {
		t.Fatalf("workflow recovery metadata = %#v, want durable token-ceiling predicate", laneMetadata.BlockedRecovery)
	}
	for _, want := range []string{"token ceiling", "16100000", "16000000", "max_session_tokens"} {
		if !strings.Contains(blocked.Reason, want) {
			t.Fatalf("Blocked[%q].Reason = %q, want %q", issue.ID, blocked.Reason, want)
		}
	}
	if !stickyBlockReason(blocked.Reason) {
		t.Fatalf("stickyBlockReason(%q) = false, want current token ceiling park to suppress dependency recovery", blocked.Reason)
	}
	orch.setBlockedStatusIssue(t.Context(), &state, connector.Issue{
		ID:         issue.ID,
		Identifier: issue.Identifier,
		Title:      issue.Title,
		State:      blockedStatusState,
	}, completedAt.Add(time.Minute))
	if got := state.Blocked[issue.ID].Reason; got != blocked.Reason {
		t.Fatalf("Blocked[%q].Reason after status refresh = %q, want preserved %q", issue.ID, got, blocked.Reason)
	}
	if got := tracker.updates; len(got) != 1 || got[0] != (dependencyAutoUnblockUpdate{issueID: issue.ID, state: blockedStatusState}) {
		t.Fatalf("state updates = %#v, want one Blocked transition", got)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one token ceiling comment", tracker.comments)
	}
	for _, want := range []string{
		"16100000",
		"16000000",
		"max_session_token_override_label",
		"max_session_tokens",
		"split the issue",
	} {
		if !strings.Contains(tracker.comments[0].body, want) {
			t.Fatalf("comment = %q, want %q", tracker.comments[0].body, want)
		}
	}
}

func TestHandleRunResultParksBudgetProjectionFailureWithoutRetry(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	tracker := &dependencyAutoUnblockConnector{}
	cfg := normalizeConfig(Config{
		ActiveStates:   []string{"Todo", "In Progress", "Rework"},
		ObservedStates: []string{"Blocked"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	orch := &Orchestrator{cfg: cfg, connector: tracker, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	state := newState(cfg)
	issue := connector.Issue{
		ID:         "issue-budget-projection",
		Identifier: "digitaldrywood/parable#2255",
		State:      "In Progress",
	}
	completedAt := time.Date(2026, 8, 19, 3, 0, 0, 0, time.UTC)
	state.Running[issue.ID] = Running{Issue: issue, Attempt: 1, StartedAt: completedAt.Add(-time.Hour)}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: completedAt.Add(-time.Hour)}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Issue: issue, Attempt: 1, Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState:  runpkg.FinalStateBudgetProjectionExceeded,
			TurnStarted: true,
			Tokens:      TokenTotals{TotalTokens: 51_900_000},
		},
		Err: &runpkg.SessionBudgetProjectionError{
			ObservedCostUSD:  40.25,
			ProjectedCostUSD: 10.00,
			Model:            "gpt-5.6-sol",
			EstimateSource:   budget.EstimateSourceHistorical,
		},
		CompletedAt:  completedAt,
		Retryable:    false,
		RetryAttempt: 2,
		RetryDelay:   time.Minute,
	})

	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after projection ceiling failure", issue.ID)
	}
	blocked, ok := state.Blocked[issue.ID]
	if !ok || blocked.Reason != budgetProjectionCeilingFailureCause || blocked.Recovery == nil || blocked.Recovery.Owner != blockedRecoveryOwnerHuman {
		t.Fatalf("Blocked[%q] = %#v, want durable human-owned projection park", issue.ID, blocked)
	}
	if len(tracker.updates) != 1 || tracker.updates[0].state != blockedStatusState {
		t.Fatalf("state updates = %#v, want one Blocked transition", tracker.updates)
	}
	if len(tracker.comments) != 3 || !strings.Contains(tracker.comments[2].body, "40.250000") || !strings.Contains(tracker.comments[2].body, "10.000000") || !strings.Contains(tracker.comments[2].body, "historical") {
		t.Fatalf("comments = %#v, want observed and projected cost", tracker.comments)
	}
	for _, want := range []string{"worker_budget_projection_ceiling_tripped", "estimate_source=historical"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs = %q, want %q", logs.String(), want)
		}
	}
}
