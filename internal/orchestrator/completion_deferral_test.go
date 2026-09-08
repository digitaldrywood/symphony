package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/github"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestCompletionFenceDeferralOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fenceErr error
	}{
		{name: "503", fenceErr: completionDeferralAvailabilityError()},
		{name: "403", fenceErr: &github.StatusError{StatusCode: 403, Err: github.ErrAuthenticationFailed}},
		{name: "primary rate limit", fenceErr: &github.StatusError{StatusCode: 429, Err: github.ErrRateLimited}},
		{name: "secondary rate limit", fenceErr: &github.StatusError{StatusCode: 403, Err: github.ErrRateLimited, RateLimitKind: "secondary"}},
		{name: "GraphQL quota", fenceErr: &github.GraphQLErrorList{Err: github.ErrRateLimited, Errors: []github.GraphQLError{{Type: "RATE_LIMITED", Message: "API rate limit exceeded"}}}},
		{name: "reserve exhausted", fenceErr: fmt.Errorf("completion lane: %w", connector.ErrResourceExhausted)},
		{name: "timeout", fenceErr: context.DeadlineExceeded},
		{name: "canceled read", fenceErr: context.Canceled},
		{name: "transport", fenceErr: errors.New("connection reset by peer")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 8, 17, 19, 0, 0, 0, time.UTC)
			issue := completionDeferralIssue("issue-fence", "In Progress")
			tracker := &completionDeferralConnector{
				backendCapacityTestConnector: backendCapacityTestConnector{},
				issue:                        issue,
				firstErr:                     tt.fenceErr,
			}
			runtimeStore := openCompletionDeferralStore(t, filepath.Join(t.TempDir(), "detent.db"))
			cfg := completionDeferralConfig()
			orch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: runtimeStore, now: func() time.Time { return now }}
			state := newState(cfg)
			attemptID := startCompletionDeferralAttempt(t, runtimeStore, issue, now)
			state.Running[issue.ID] = completionDeferralRunning(issue, attemptID, now)
			state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

			orch.handleRunResult(t.Context(), &state, completionDeferralEvent(issue, attemptID, now))

			record, deferred := state.deferredCompletions[issue.ID]
			if !deferred {
				t.Fatal("fence read failure did not defer completion")
			}
			if got := tracker.fetchCount(); got != 1 {
				t.Fatalf("completion fence fetches = %d, want 1", got)
			}
			receipt, err := runtimeStore.WorkAttempt(t.Context(), attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Status != store.WorkAttemptStatusActive || receipt.Phase != deferredCompletionPhase || receipt.WaitReason != connector.TrackerUnavailableCondition || receipt.NextAction != deferredCompletionNextAction {
				t.Fatalf("deferred work attempt = %#v, want active tracker wait", receipt)
			}
			if strings.Contains(receipt.WorkerMetadataJSON, `"lane_revocation"`) || len(orch.pendingLaneRevocations) != 0 || len(tracker.comments) != 0 {
				t.Fatalf("unreadable fence produced a revocation: %s", receipt.WorkerMetadataJSON)
			}
			if !strings.Contains(record.Availability.Message, tt.fenceErr.Error()) {
				t.Fatalf("fence error = %q, want original error %v", record.Availability.Message, tt.fenceErr)
			}
			if record.Result.Output != "validated completion" || record.Result.Tokens.TotalTokens != 37 {
				t.Fatalf("preserved result = %#v", record.Result)
			}
			snapshot := state.Snapshot(now)
			if len(snapshot.Queue) != 1 || snapshot.Queue[0].QueueState != telemetry.QueueStateWaitingOnTracker {
				t.Fatalf("snapshot queue = %#v, want waiting_on_tracker", snapshot.Queue)
			}
			if outcome := orch.dispatchIssueWithOutcome(t.Context(), &state, issue, 1, now, ""); outcome.dispatched || outcome.reason != dispatchSkipCompletionDeferred {
				t.Fatalf("dispatch while completion deferred = %#v, want suppressed", outcome)
			}
		})
	}
}

func TestDeferredCompletionRestartAndRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		recoveredState     string
		retryCount         int
		wantDeferred       bool
		wantTerminal       store.WorkAttemptTerminalState
		wantAcceptedTokens int64
	}{
		{
			name:           "deferral survives restart",
			recoveredState: "In Progress",
			wantDeferred:   true,
		},
		{
			name:               "retry after recovery accepts intact lane",
			recoveredState:     "In Progress",
			retryCount:         1,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantAcceptedTokens: 37,
		},
		{
			name:               "retry after recovery honors lane revocation",
			recoveredState:     "Todo",
			retryCount:         1,
			wantTerminal:       store.WorkAttemptTerminalLaneRevoked,
			wantAcceptedTokens: 37,
		},
		{
			name:               "completion cannot be accepted twice",
			recoveredState:     "In Progress",
			retryCount:         2,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantAcceptedTokens: 37,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 8, 17, 19, 30, 0, 0, time.UTC)
			dbPath := filepath.Join(t.TempDir(), "detent.db")
			issue := completionDeferralIssue("issue-restart", "In Progress")
			tracker := &completionDeferralConnector{
				backendCapacityTestConnector: backendCapacityTestConnector{},
				issue:                        issue,
				firstErr:                     completionDeferralAvailabilityError(),
			}
			cfg := completionDeferralConfig()
			initialStore := openCompletionDeferralStoreWithoutCleanup(t, dbPath)
			attemptID := startCompletionDeferralAttempt(t, initialStore, issue, now)
			initialOrch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: initialStore, now: func() time.Time { return now }}
			initialState := newState(cfg)
			initialState.Running[issue.ID] = completionDeferralRunning(issue, attemptID, now)
			initialState.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}
			initialOrch.handleRunResult(t.Context(), &initialState, completionDeferralEvent(issue, attemptID, now))
			if err := initialStore.Close(); err != nil {
				t.Fatalf("Close(initial store) error = %v", err)
			}

			restartedStore := openCompletionDeferralStore(t, dbPath)
			recoveredIssue := cloneIssue(issue)
			recoveredIssue.State = tt.recoveredState
			tracker.setIssue(recoveredIssue)
			restartAt := now.Add(2 * time.Minute)
			restartedOrch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: restartedStore, now: func() time.Time { return restartAt }}
			restartedState := newState(cfg)
			restartedOrch.recoverDurableWorkAttempts(t.Context(), &restartedState, restartAt)

			if _, ok := restartedState.deferredCompletions[issue.ID]; !ok {
				t.Fatalf("deferred completion missing after restart: %#v", restartedState.deferredCompletions)
			}
			if retry, ok := restartedState.Retry[issue.ID]; !ok || !retry.CompletionDeferred {
				t.Fatalf("Retry[%q] = %#v, want recovered completion deferral", issue.ID, retry)
			}
			if _, ok := restartedState.Claimed[issue.ID]; !ok {
				t.Fatalf("Claimed[%q] missing after restart", issue.ID)
			}
			if outcome := restartedOrch.dispatchIssueWithOutcome(t.Context(), &restartedState, recoveredIssue, 1, restartAt, ""); outcome.dispatched || outcome.reason != dispatchSkipCompletionDeferred {
				t.Fatalf("dispatch after restart = %#v, want suppressed", outcome)
			}
			if tt.retryCount > 1 {
				fetchesBeforeDuplicate := tracker.fetchCount()
				restartedOrch.handleRunResult(t.Context(), &restartedState, completionDeferralEvent(issue, attemptID, restartAt))
				if tracker.fetchCount() != fetchesBeforeDuplicate {
					t.Fatalf("duplicate delivery retried completion fence")
				}
				receipt, err := restartedStore.WorkAttempt(t.Context(), attemptID)
				if err != nil {
					t.Fatalf("WorkAttempt() after duplicate delivery error = %v", err)
				}
				if receipt.Status != store.WorkAttemptStatusActive || receipt.Phase != deferredCompletionPhase {
					t.Fatalf("duplicate delivery changed deferred receipt = %#v", receipt)
				}
			}

			fetchesAfterFirstRetry := 0
			for retryIndex := range tt.retryCount {
				if ok := restartedOrch.retryDeferredCompletions(t.Context(), &restartedState, restartAt); !ok {
					t.Fatal("retryDeferredCompletions() = false, want completed fence decision")
				}
				if retryIndex == 0 {
					fetchesAfterFirstRetry = tracker.fetchCount()
				}
			}
			if tt.retryCount > 1 && tracker.fetchCount() != fetchesAfterFirstRetry {
				t.Fatalf("completion fence fetches after duplicate retry = %d, want unchanged %d", tracker.fetchCount(), fetchesAfterFirstRetry)
			}

			receipt, err := restartedStore.WorkAttempt(t.Context(), attemptID)
			if err != nil {
				t.Fatalf("WorkAttempt() error = %v", err)
			}
			if tt.wantDeferred {
				if receipt.Status != store.WorkAttemptStatusActive || receipt.Phase != deferredCompletionPhase {
					t.Fatalf("restarted receipt = %#v, want active completion deferral", receipt)
				}
				return
			}
			if receipt.TerminalState != tt.wantTerminal {
				t.Fatalf("terminal state = %q, want %q", receipt.TerminalState, tt.wantTerminal)
			}
			if _, ok := restartedState.deferredCompletions[issue.ID]; ok {
				t.Fatalf("deferred completion remains after fence decision")
			}
			if restartedState.TokenTotals.TotalTokens != tt.wantAcceptedTokens {
				t.Fatalf("accepted tokens = %d, want %d", restartedState.TokenTotals.TotalTokens, tt.wantAcceptedTokens)
			}
		})
	}
}

type completionDeferralConnector struct {
	backendCapacityTestConnector
	mu       sync.Mutex
	issue    connector.Issue
	firstErr error
	fetches  int
}

func (c *completionDeferralConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fetches++
	if c.fetches == 1 && c.firstErr != nil {
		return nil, c.firstErr
	}
	return []connector.Issue{cloneIssue(c.issue)}, nil
}

func (c *completionDeferralConnector) fetchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetches
}

func (c *completionDeferralConnector) setIssue(issue connector.Issue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.issue = cloneIssue(issue)
}

func completionDeferralAvailabilityError() error {
	return connector.NewTrackerAvailabilityError(connector.TrackerAvailabilityScope{
		Connector:          "github",
		Endpoint:           "https://api.github.test/graphql",
		Operation:          "issue_lookup",
		CredentialIdentity: "github-rest:test",
	}, connector.TrackerAvailabilityClassServer, errors.New("upstream returned status 503"))
}

func completionDeferralConfig() Config {
	return normalizeConfig(Config{
		Project:                scheduler.ProjectCandidate{ID: "detent"},
		PollInterval:           time.Minute,
		ContinuationRetryDelay: time.Minute,
		ActiveStates:           []string{"In Progress"},
		TerminalStates:         []string{"Done"},
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Minute,
		OverloadRetryDelay:     time.Minute,
	})
}

func completionDeferralIssue(id string, state string) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = "digitaldrywood/detent#1869"
	issue.Title = "Defer completion fence"
	issue.URL = "https://github.com/digitaldrywood/detent/issues/1869"
	issue.State = state
	return issue
}

func completionDeferralRunning(issue connector.Issue, attemptID int64, now time.Time) Running {
	return Running{
		Issue:         cloneIssue(issue),
		Attempt:       2,
		WorkAttemptID: attemptID,
		Generation:    7,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		WorkerHost:    "worker-a",
	}
}

func completionDeferralEvent(issue connector.Issue, attemptID int64, now time.Time) runpkg.Completion {
	return runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{
			ProjectID:     "detent",
			Issue:         cloneIssue(issue),
			Attempt:       2,
			WorkAttemptID: attemptID,
			Generation:    7,
			Mode:          runpkg.RunModeImplement,
			StartedAt:     now.Add(-time.Minute),
			WorkerHost:    "worker-a",
		},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     "validated completion",
			Tokens:     runpkg.TokenTotals{InputTokens: 25, OutputTokens: 12, TotalTokens: 37},
			DiffStats:  runpkg.DiffStats{FilesChanged: 2, AddedLines: 8, RemovedLines: 3, Status: "clean"},
		},
		CompletedAt: now,
	}
}

func startCompletionDeferralAttempt(t *testing.T, runtimeStore store.Store, issue connector.Issue, now time.Time) int64 {
	t.Helper()
	attemptID, err := runtimeStore.StartWorkAttempt(t.Context(), store.WorkAttemptStart{
		ProjectID:      "detent",
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		IssueURL:       issue.URL,
		WorkerType:     "implement",
		WorkerHost:     "worker-a",
		Lane:           issue.State,
		AttemptNumber:  2,
		StartedAt:      now.Add(-time.Minute),
		LeaseExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	return attemptID
}

func openCompletionDeferralStore(t *testing.T, path string) store.Store {
	t.Helper()
	runtimeStore := openCompletionDeferralStoreWithoutCleanup(t, path)
	t.Cleanup(func() {
		if err := runtimeStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return runtimeStore
}

func openCompletionDeferralStoreWithoutCleanup(t *testing.T, path string) store.Store {
	t.Helper()
	runtimeStore, err := store.Open(t.Context(), store.Config{Backend: store.BackendSQLite, Path: path})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	return runtimeStore
}

func TestCompletionFenceUnchangedOrUnknownLaneDefers(t *testing.T) {
	t.Parallel()
	for _, lane := range []string{"Blocked", "Done", ""} {
		t.Run(lane, func(t *testing.T) {
			now := time.Now().UTC()
			issue := completionDeferralIssue("unchanged", lane)
			cfg := completionDeferralConfig()
			tracker := &completionDeferralConnector{issue: issue}
			attempts := &recordingWorkAttemptStore{}
			orch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts, now: func() time.Time { return now }}
			state := newState(cfg)
			state.Running[issue.ID] = completionDeferralRunning(issue, 2297, now)
			orch.handleRunResult(t.Context(), &state, completionDeferralEvent(issue, 2297, now))
			if len(state.deferredCompletions) != 1 || len(orch.pendingLaneRevocations) != 0 || len(attempts.completions) != 0 || len(tracker.comments) != 0 {
				t.Fatal("unchanged or unknown lane finalized as revoked")
			}
		})
	}
}

func TestCompletionFenceRateLimitDeadlineSurvivesRestart(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		after time.Duration
		reset time.Duration
		want  time.Duration
	}{
		{name: "reset", reset: time.Hour, want: time.Hour},
		{name: "secondary backoff", after: 10 * time.Minute, want: 10 * time.Minute},
		{name: "later reset", after: time.Minute, reset: time.Hour, want: time.Hour},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now().UTC()
			issue := completionDeferralIssue("quota", "In Progress")
			cfg := completionDeferralConfig()
			tracker := &completionDeferralConnector{issue: issue, firstErr: &github.StatusError{Err: github.ErrRateLimited, StatusCode: 429, RetryAfter: tt.after, ResetAt: now.Add(tt.reset)}}
			backend := openCompletionDeferralStore(t, filepath.Join(t.TempDir(), "detent.db"))
			id := startCompletionDeferralAttempt(t, backend, issue, now)
			orch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: backend, now: func() time.Time { return now }}
			state := newState(cfg)
			state.Running[issue.ID] = completionDeferralRunning(issue, id, now)
			orch.handleRunResult(t.Context(), &state, completionDeferralEvent(issue, id, now))
			recovered := newState(cfg)
			orch.recoverDurableWorkAttempts(t.Context(), &recovered, now.Add(time.Second))
			if got := recovered.Retry[issue.ID].DueAt; !got.Equal(now.Add(tt.want)) {
				t.Fatalf("retry = %s, want %s", got, now.Add(tt.want))
			}
			orch.retryDeferredCompletions(t.Context(), &recovered, now.Add(tt.want-time.Second))
			if tracker.fetchCount() != 1 {
				t.Fatal("fence retried before quota recovered")
			}
		})
	}
}

func TestCompletionFenceMissingLaneDefers(t *testing.T) {
	t.Parallel()
	for _, returnedID := range []string{"", "missing-lane"} {
		t.Run(returnedID, func(t *testing.T) {
			now := time.Now().UTC()
			issue := completionDeferralIssue("missing-lane", "In Progress")
			cfg := completionDeferralConfig()
			tracker := &completionDeferralConnector{issue: connector.Issue{ID: returnedID}}
			attempts := &recordingWorkAttemptStore{}
			orch := Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts, now: func() time.Time { return now }}
			state := newState(cfg)
			state.Running[issue.ID] = completionDeferralRunning(issue, 2297, now)
			orch.handleRunResult(t.Context(), &state, completionDeferralEvent(issue, 2297, now))
			if len(state.deferredCompletions) != 1 || len(attempts.completions) != 0 {
				t.Fatal("missing tracker lane did not defer completion")
			}
		})
	}
}
