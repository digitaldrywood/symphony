package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestMergeFallbackRoutesBoundedOutcomesToRework(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		result            runpkg.RunResult
		runErr            error
		wantReason        string
		wantTerminalState store.WorkAttemptTerminalState
	}{
		{
			name: "structured review finding",
			result: runpkg.RunResult{
				FinalState:            runpkg.FinalStateCompleted,
				Output:                runpkg.RunOutputMergeFallbackRework,
				MergeFallbackFindings: "Found an unrelated authorization defect and stopped.",
			},
			wantReason:        mergeFallbackRequiresReworkReason,
			wantTerminalState: store.WorkAttemptTerminalSuccess,
		},
		{
			name: "fallback budget exceeded",
			result: runpkg.RunResult{
				FinalState: runpkg.FinalStateMergeFallbackExceeded,
				Output:     "Conflict resolved; validation still running.",
			},
			runErr:            errors.Join(runpkg.ErrMergeFallbackBudgetExceeded, context.DeadlineExceeded),
			wantReason:        mergeFallbackBudgetExceededReason,
			wantTerminalState: store.WorkAttemptTerminalTimedOut,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 8, 15, 14, 0, 0, 0, time.UTC)
			issue := connector.Issue{
				ID:           "issue-1809-" + strings.ReplaceAll(tt.name, " ", "-"),
				Identifier:   "digitaldrywood/detent#1809",
				State:        "Merging",
				PRRepository: "digitaldrywood/detent",
				PullRequest: &connector.PullRequest{
					Number:         1810,
					URL:            "https://github.test/digitaldrywood/detent/pull/1810",
					State:          "OPEN",
					MergeableState: "dirty",
					HeadSHA:        "conflicted-head",
				},
			}
			tracker := &autoPromoteTickMergeConnector{
				autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
			}
			cfg := normalizeConfig(Config{
				ActiveStates:   []string{"Rework", "Merging"},
				ObservedStates: []string{"Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			attempts := &recordingWorkAttemptStore{}
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			state := newState(cfg)
			state.Running[issue.ID] = Running{
				Issue:         cloneIssue(issue),
				Attempt:       1,
				WorkAttemptID: 1809,
				StartedAt:     now.Add(-20 * time.Minute),
				Mode:          runpkg.RunModeMerge,
			}
			state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-20 * time.Minute)}

			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID:     issue.ID,
				CompletedAt: now,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
				Result:      tt.result,
				Err:         tt.runErr,
			})

			if len(tracker.updates) != 1 || tracker.updates[0].state != "Rework" {
				t.Fatalf("state updates = %#v, want one Rework transition", tracker.updates)
			}
			if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, tt.wantReason) {
				t.Fatalf("comments = %#v, want reason %q", tracker.comments, tt.wantReason)
			}
			if !strings.Contains(tracker.comments[0].body, "authorization defect") &&
				!strings.Contains(tracker.comments[0].body, "validation still running") {
				t.Fatalf("comment = %q, want preserved merge-fallback findings", tracker.comments[0].body)
			}
			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != tt.wantTerminalState {
				t.Fatalf("attempt completions = %#v, want terminal state %q", attempts.completions, tt.wantTerminalState)
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after Rework handoff", issue.ID)
			}
			if _, ok := state.Claimed[issue.ID]; ok {
				t.Fatalf("Claimed[%q] present after Rework handoff", issue.ID)
			}
		})
	}
}

func TestMergeFallbackResolvedHeadHandoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		head       string
		ci         string
		wantRework bool
	}{
		{name: "resolved pushed head waits past resolution deadline", head: "validated-head", ci: "pending"},
		{name: "validation failure", head: "validated-head", ci: "failure", wantRework: true},
		{name: "replaced head with green CI", head: "replacement-head", ci: "success", wantRework: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 7, 22, 2, 24, 0, time.UTC)
			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1, MergeFastPathEnabled: true,
				ActiveStates: []string{"Rework", "Merging"}, ObservedStates: []string{"Merging"}, TerminalStates: []string{"Done"},
			})
			issue := connector.Issue{
				ID: "issue-2273", Identifier: "digitaldrywood/detent#2273", State: "Merging", PRRepository: "digitaldrywood/detent",
				PullRequest: &connector.PullRequest{
					Number: 2274, State: "OPEN", MergeableState: "clean", HeadSHA: tt.head, CIStatus: tt.ci,
				},
			}
			tracker := &autoPromoteTickMergeConnector{autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}}
			orch := &Orchestrator{cfg: cfg, connector: tracker, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			state := newState(cfg)
			state.Running[issue.ID] = Running{Issue: issue, Attempt: 1, Mode: runpkg.RunModeMerge, StartedAt: now.Add(-21 * time.Minute)}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-21 * time.Minute)}
			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID: issue.ID, CompletedAt: now, Request: runpkg.RunRequest{Mode: runpkg.RunModeMerge},
				Result: runpkg.RunResult{
					FinalState: runpkg.FinalStateCompleted, Output: runpkg.RunOutputMergeFallbackResolved,
					MergePrecheck: &runpkg.MergePrecheck{Status: "clean", HeadSHA: "validated-head"},
				},
			})
			if len(tracker.merges) != 0 || len(state.Running) != 0 || len(state.Claimed) != 0 {
				t.Fatalf("handoff retained execution or merged: merges=%v running=%v claimed=%v", tracker.merges, state.Running, state.Claimed)
			}
			if tt.wantRework {
				if len(tracker.updates) != 1 || tracker.updates[0].state != "Rework" {
					t.Fatalf("updates = %#v, want Rework", tracker.updates)
				}
				return
			}
			if len(tracker.updates) != 0 {
				t.Fatalf("updates = %#v, want passive CI wait", tracker.updates)
			}
			retry := state.Retry[issue.ID]
			if retry.Wait.Kind != retryWaitCurrentHeadCI || retry.Attempt != 1 {
				t.Fatalf("retry = %#v, want current-head CI wait without another implementation", retry)
			}
			reservation := state.mergeReservations[issue.PRRepository]
			if reservation.ExpiresAt.IsZero() || !reservation.ExpiresAt.After(now) {
				t.Fatalf("reservation = %#v, want bounded merge ownership", reservation)
			}
			_, handled, _ := orch.pollMergeWorkerCurrentHeadCI(t.Context(), &state, issue, retry, now.Add(time.Minute))
			if !handled || len(state.Running) != 0 || len(tracker.updates) != 0 {
				t.Fatal("pending CI redispatched resolution or transitioned the issue after the fallback deadline")
			}
		})
	}
}
