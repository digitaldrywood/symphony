package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestHandleRunResultRoutesTerminalRetryByWorkProduct(t *testing.T) {
	t.Parallel()

	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	capacityReset := time.Date(2026, 7, 18, 16, 0, 0, 0, time.UTC)
	tests := []struct {
		name            string
		limit           *int
		wantBlocked     bool
		completionErr   bool
		noStore         bool
		noAttemptID     bool
		issue           connector.Issue
		result          runpkg.RunResult
		runError        error
		wantState       string
		wantTransitions []string
	}{
		{name: "zero parks failed persistence", limit: new(0), issue: terminalRetryTestIssue("zero-store-error"), runError: errors.New("runner failed"), completionErr: true, wantBlocked: true},
		{name: "zero parks missing store", limit: new(0), issue: terminalRetryTestIssue("zero-no-store"), runError: errors.New("runner failed"), noStore: true, wantBlocked: true},
		{name: "zero parks missing attempt ID", limit: new(0), issue: terminalRetryTestIssue("zero-no-id"), runError: errors.New("runner failed"), noAttemptID: true, wantBlocked: true},
		{name: "zero parks overload without store", limit: new(0), issue: terminalRetryTestIssue("zero-overload-no-store"), runError: backendcapacity.NewError(scope, backendcapacity.Details{Type: backendcapacity.ErrorTypeTransientOverload, Kind: "serverOverloaded"}, errors.New("provider overloaded")), noStore: true, wantBlocked: true},
		{name: "zero preserves capacity without store", limit: new(0), issue: terminalRetryTestIssue("zero-capacity-no-store"), runError: backendcapacity.NewError(scope, backendcapacity.Details{Kind: "usageLimitExceeded", ResetAt: &capacityReset}, errors.New("provider usage limit reached")), noStore: true, wantState: "Todo", wantTransitions: []string{"Todo"}},
		{name: "zero preserves pushed work without store", limit: new(0), issue: terminalRetryTestIssue("zero-pushed-no-store"), result: runpkg.RunResult{PullRequestHeadPushed: true}, runError: errors.New("runner failed"), noStore: true, wantState: "In Progress"},
		{name: "default preserves failed persistence retry", issue: terminalRetryTestIssue("default-store-error"), runError: errors.New("runner failed"), completionErr: true, wantState: "Todo", wantTransitions: []string{"Todo"}},
		{name: "zero parks runner failure", limit: new(0), issue: terminalRetryTestIssue("zero-failure"), runError: errors.New("runner failed"), wantBlocked: true},
		{name: "zero parks transient overload", limit: new(0), issue: terminalRetryTestIssue("zero-overload"), runError: backendcapacity.NewError(scope, backendcapacity.Details{Type: backendcapacity.ErrorTypeTransientOverload, Kind: "serverOverloaded"}, errors.New("provider overloaded")), wantBlocked: true},
		{name: "zero preserves capacity wait", limit: new(0), issue: terminalRetryTestIssue("zero-capacity"), runError: backendcapacity.NewError(scope, backendcapacity.Details{Kind: "usageLimitExceeded", ResetAt: &capacityReset}, errors.New("provider usage limit reached")), wantState: "Todo", wantTransitions: []string{"Todo"}},
		{name: "zero preserves pushed work", limit: new(0), issue: terminalRetryTestIssue("zero-pushed"), result: runpkg.RunResult{PullRequestHeadPushed: true}, runError: errors.New("runner failed"), wantState: "In Progress"},
		{name: "zero preserves linked PR", limit: new(0), issue: terminalRetryTestIssueWithPullRequest("zero-pr"), runError: errors.New("runner failed"), wantState: "In Progress"},
		{
			name:            "no pushed work returns to todo",
			issue:           terminalRetryTestIssue("empty"),
			runError:        errors.New("runner failed"),
			wantState:       "Todo",
			wantTransitions: []string{"Todo"},
		},
		{
			name:            "transient overload returns to todo",
			issue:           terminalRetryTestIssue("overload"),
			runError:        backendcapacity.NewError(scope, backendcapacity.Details{Type: backendcapacity.ErrorTypeTransientOverload, Kind: "serverOverloaded"}, errors.New("provider overloaded")),
			wantState:       "Todo",
			wantTransitions: []string{"Todo"},
		},
		{
			name:            "startup timeout returns to todo",
			issue:           terminalRetryTestIssue("startup-timeout"),
			runError:        backendcapacity.NewError(scope, backendcapacity.Details{Type: backendcapacity.ErrorTypeTransientOverload, Kind: backendcapacity.StartupTimeoutKind}, context.DeadlineExceeded),
			wantState:       "Todo",
			wantTransitions: []string{"Todo"},
		},
		{
			name:            "provider capacity returns to todo",
			issue:           terminalRetryTestIssue("capacity"),
			runError:        backendcapacity.NewError(scope, backendcapacity.Details{Kind: "usageLimitExceeded", ResetAt: &capacityReset}, errors.New("provider usage limit reached")),
			wantState:       "Todo",
			wantTransitions: []string{"Todo"},
		},
		{
			name:      "pushed branch keeps in progress",
			issue:     terminalRetryTestIssue("pushed"),
			result:    runpkg.RunResult{PullRequestHeadPushed: true},
			runError:  errors.New("runner failed"),
			wantState: "In Progress",
		},
		{
			name:      "linked pull request keeps in progress",
			issue:     terminalRetryTestIssueWithPullRequest("pull-request"),
			runError:  errors.New("runner failed"),
			wantState: "In Progress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{tt.issue.ID: cloneIssue(tt.issue)}}
			attempts := &terminalRetryWorkAttemptStore{}
			if tt.completionErr {
				attempts.completionErr = errors.New("store unavailable")
			}
			cfg := normalizeConfig(Config{
				Recovery:              workflowconfig.Recovery{TerminalAttemptRetryLimit: tt.limit},
				ActiveStates:          []string{"Todo", "In Progress"},
				TerminalStates:        []string{"Done"},
				MaxRetryBackoff:       time.Minute,
				FailureRetryBaseDelay: time.Second,
			})
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			if tt.noStore {
				o.workAttempts = nil
			}
			state := newState(cfg)
			state.Running[tt.issue.ID] = Running{
				Issue:         cloneIssue(tt.issue),
				Attempt:       1,
				WorkAttemptID: 42,
				Mode:          runpkg.RunModeImplement,
				StartedAt:     now.Add(-time.Minute),
			}
			state.Claimed[tt.issue.ID] = Claimed{Issue: cloneIssue(tt.issue), ClaimedAt: now.Add(-time.Minute)}
			if tt.noAttemptID {
				running := state.Running[tt.issue.ID]
				running.WorkAttemptID = 0
				state.Running[tt.issue.ID] = running
			}

			o.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID:      tt.issue.ID,
				Result:       tt.result,
				Err:          tt.runError,
				CompletedAt:  now,
				RetryAttempt: 2,
				RetryDelay:   time.Minute,
			})

			if tt.wantBlocked {
				blocked, ok := state.Blocked[tt.issue.ID]
				if !ok || blocked.Reason != terminalAttemptRetryLimitCause || blocked.Recovery.Owner != blockedRecoveryOwnerOperator {
					t.Fatalf("Blocked = %#v, want operator-owned terminal failure", blocked)
				}
				if len(state.Retry) != 0 || len(state.Claimed) != 0 || !slices.Equal(tracker.transitionStates(), []string{"Blocked"}) {
					t.Fatal("parked failure retained retry/claim or transitioned to Todo")
				}
				return
			}
			retry, ok := state.Retry[tt.issue.ID]
			if !ok {
				t.Fatalf("Retry[%q] missing", tt.issue.ID)
			}
			if retry.Issue.State != tt.wantState {
				t.Fatalf("Retry[%q].Issue.State = %q, want %q", tt.issue.ID, retry.Issue.State, tt.wantState)
			}
			if _, claimed := state.Claimed[tt.issue.ID]; claimed {
				t.Fatalf("Claimed[%q] present after terminal completion", tt.issue.ID)
			}
			if got := tracker.transitionStates(); !slices.Equal(got, tt.wantTransitions) {
				t.Fatalf("state transitions = %v, want %v", got, tt.wantTransitions)
			}
			if tt.noStore || tt.noAttemptID {
				if len(attempts.completions) != 0 {
					t.Fatal("unexpected completion persistence")
				}
				return
			}
			if len(attempts.completions) != 1 {
				t.Fatalf("work attempt completions = %d, want 1", len(attempts.completions))
			}
			if got := terminalRetryMetadataPushed(attempts.completions[0].WorkerMetadataJSON); got != tt.result.PullRequestHeadPushed {
				t.Fatalf("persisted work_product_pushed = %v, want %v", got, tt.result.PullRequestHeadPushed)
			}
		})
	}
}

func TestTerminalAttemptRetryableFailureExcludesBackendCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		terminal      store.WorkAttemptTerminalState
		errorClass    string
		wantRetryable bool
	}{
		{name: "service restart remains resumable", terminal: store.WorkAttemptTerminalAbandoned, errorClass: "service_restart", wantRetryable: true},
		{name: "ordinary failure", terminal: store.WorkAttemptTerminalFailure, errorClass: workAttemptErrorRunner, wantRetryable: true},
		{name: "provider capacity", terminal: store.WorkAttemptTerminalCapacity, errorClass: backendcapacity.ErrorClass},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := terminalAttemptRetryableFailure(telemetry.WorkAttempt{
				TerminalState: string(tt.terminal),
				ErrorClass:    tt.errorClass,
			})
			if got != tt.wantRetryable {
				t.Fatalf("terminalAttemptRetryableFailure() = %v, want %v", got, tt.wantRetryable)
			}
		})
	}
}

func TestHandleRunResultPersistsPublishedPushCommandEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)
	issue := terminalRetryTestIssue("published-post-push-failure")
	tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
	attempts := &terminalRetryWorkAttemptStore{}
	cfg := normalizeConfig(Config{
		ActiveStates:          []string{"Todo", "In Progress"},
		TerminalStates:        []string{"Done"},
		MaxRetryBackoff:       time.Minute,
		FailureRetryBaseDelay: time.Second,
	})
	orchestrator := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:         cloneIssue(issue),
		Attempt:       1,
		WorkAttemptID: 2029,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}
	exitCode := 19
	command := "git push origin HEAD && exit 19"
	targetRef := &runpkg.DeliverableTargetRefEvidence{
		Remote:                     "origin",
		Ref:                        "refs/heads/detent/published",
		PostCommandLocalHeadSHA:    "new-head",
		PostCommandRemoteHeadSHA:   "new-head",
		PostCommandRemoteRefExists: true,
		InitialObserved:            true,
		PostCommandObserved:        true,
		AdvancedToLocalHead:        true,
	}
	runErr := &runpkg.DeliverableCommandError{
		OperationClass: "post_push",
		Operation:      "post-push command",
		Arguments:      "exit 19",
		ItemID:         "push-item",
		Command:        command,
		Status:         "failed",
		ExitCode:       &exitCode,
		Message:        "later assertion failed",
	}

	orchestrator.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState:            runpkg.FinalStateFailed,
			PullRequestHeadPushed: true,
			DeliverableCommands: []runpkg.DeliverableCommandEvidence{{
				ItemID:         "push-item",
				OperationClass: "post_push",
				Operation:      "post-push command",
				Command:        command,
				Status:         "failed",
				ExitCode:       &exitCode,
				Outcome:        "published_post_push_failed",
				TargetRef:      targetRef,
			}},
		},
		Err:          runErr,
		CompletedAt:  now,
		RetryAttempt: 2,
		RetryDelay:   time.Minute,
	})

	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions = %d, want one", len(attempts.completions))
	}
	completion := attempts.completions[0]
	if completion.ErrorClass != workAttemptErrorPostPushCommand {
		t.Fatalf("ErrorClass = %q, want %q", completion.ErrorClass, workAttemptErrorPostPushCommand)
	}
	var metadata struct {
		WorkProductPushed   bool                                `json:"work_product_pushed"`
		DeliverableCommands []runpkg.DeliverableCommandEvidence `json:"deliverable_commands"`
	}
	if err := json.Unmarshal([]byte(completion.WorkerMetadataJSON), &metadata); err != nil {
		t.Fatalf("decode worker metadata: %v", err)
	}
	if !metadata.WorkProductPushed || len(metadata.DeliverableCommands) != 1 {
		t.Fatalf("worker metadata = %#v, want pushed command evidence", metadata)
	}
	persisted := metadata.DeliverableCommands[0]
	if persisted.Command != command || persisted.ExitCode == nil || *persisted.ExitCode != exitCode || persisted.TargetRef == nil || !persisted.TargetRef.AdvancedToLocalHead {
		t.Fatalf("persisted command evidence = %#v", persisted)
	}
	wantBreakerClass := projectFailureClassDeliverableCommand + ":post-push command"
	if len(state.FailureBreaker.Failures[wantBreakerClass]) != 1 {
		t.Fatalf("failure breaker classes = %#v, want %q", state.FailureBreaker.Failures, wantBreakerClass)
	}
	if len(state.FailureBreaker.Failures[projectFailureClassDeliverableCommand+":git push"]) != 0 {
		t.Fatalf("published work charged to git push breaker: %#v", state.FailureBreaker.Failures)
	}
}

func TestHandleRunResultParksTerminalRetryAtDurableLimit(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		limit    *int
		failures int
	}{
		{name: "default", failures: 3},
		{name: "zero", limit: new(0), failures: 1},
		{name: "one", limit: new(1), failures: 2},
		{name: "three", limit: new(3), failures: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC)
			issue := terminalRetryTestIssue("runtime-limit")
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			attempts := &terminalRetryWorkAttemptStore{}
			if tt.limit != nil && *tt.limit == 0 {
				attempts.historyErr = errors.New("history unavailable")
			}
			cfg := normalizeConfig(Config{
				Recovery:              workflowconfig.Recovery{TerminalAttemptRetryLimit: tt.limit},
				ActiveStates:          []string{"Todo", "In Progress"},
				ObservedStates:        []string{"Blocked"},
				TerminalStates:        []string{"Done"},
				MaxRetryBackoff:       time.Minute,
				FailureRetryBaseDelay: time.Second,
			})
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			o.workflowMetrics = metrics
			state := newState(cfg)

			for attempt := 1; attempt <= tt.failures; attempt++ {
				completedAt := now.Add(time.Duration(attempt) * time.Minute)
				issue.State = planImplementationState
				tracker.issues[issue.ID] = cloneIssue(issue)
				state.Running[issue.ID] = Running{
					Issue:         cloneIssue(issue),
					Attempt:       attempt,
					WorkAttemptID: int64(attempt),
					Mode:          runpkg.RunModePlan,
					StartedAt:     completedAt.Add(-time.Minute),
				}
				state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: completedAt.Add(-time.Minute)}
				o.upsertWorkAttemptSnapshot(&state, telemetry.WorkAttempt{
					AttemptID: int64(attempt), IssueID: issue.ID, Identifier: issue.Identifier,
					Status: string(store.WorkAttemptStatusActive), StartedAt: completedAt.Add(-time.Minute),
				})

				o.handleRunResult(t.Context(), &state, runpkg.Completion{
					IssueID:      issue.ID,
					Request:      runpkg.RunRequest{Mode: runpkg.RunModePlan},
					Err:          fmt.Errorf("runner failed on attempt %d before producing work", attempt),
					CompletedAt:  completedAt,
					RetryAttempt: attempt + 1,
					RetryDelay:   time.Second,
				})

				if attempt < tt.failures {
					if retry, ok := state.Retry[issue.ID]; !ok || retry.Issue.State != "Todo" {
						t.Fatalf("attempt %d Retry[%q] = %#v, want Todo retry", attempt, issue.ID, retry)
					}
				}
			}

			blocked, ok := state.Blocked[issue.ID]
			if !ok || blocked.Reason != terminalAttemptRetryLimitCause || blocked.Recovery == nil {
				t.Fatalf("Blocked[%q] = %#v, want terminal retry limit park", issue.ID, blocked)
			}
			wantError := fmt.Sprintf("runner failed on attempt %d before producing work", tt.failures)
			if blocked.AttemptError != wantError || blocked.WorkAttemptID != int64(tt.failures) {
				t.Fatalf("Blocked[%q] attempt evidence = %q/%d, want %q/%d", issue.ID, blocked.AttemptError, blocked.WorkAttemptID, wantError, tt.failures)
			}
			if blocked.Recovery.AttemptError != wantError || blocked.Recovery.WorkAttemptID != int64(tt.failures) {
				t.Fatalf("Blocked[%q].Recovery attempt evidence = %q/%d, want %q/%d", issue.ID, blocked.Recovery.AttemptError, blocked.Recovery.WorkAttemptID, wantError, tt.failures)
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after durable terminal retry limit", issue.ID)
			}
			if len(attempts.completions) != tt.failures {
				t.Fatalf("work attempt completions = %d, want %d", len(attempts.completions), tt.failures)
			}
			wantComments := 1
			if tt.limit != nil && *tt.limit == 0 {
				wantComments = 3
			}
			if len(tracker.comments) != wantComments || !strings.Contains(tracker.comments[wantComments-1], wantError) {
				t.Fatalf("retry-limit comments = %#v, want latest parked-attempt error", tracker.comments)
			}

			hydratedIssue := cloneIssue(issue)
			hydratedIssue.State = blockedStatusState
			parkedAt := now.Add(time.Duration(tt.failures) * time.Minute)
			hydratedIssue.StageUpdatedAt = &parkedAt
			restarted := &Orchestrator{cfg: cfg, workflowMetrics: metrics}
			restartedState := newState(cfg)
			restarted.setBlockedStatusIssue(t.Context(), &restartedState, hydratedIssue, parkedAt.Add(time.Minute))
			restartedBlocked := restartedState.Blocked[issue.ID]
			if restartedBlocked.Reason != terminalAttemptRetryLimitCause || restartedBlocked.AttemptError != wantError || restartedBlocked.WorkAttemptID != int64(tt.failures) {
				t.Fatalf("restarted Blocked[%q] = %#v, want durable cause and attempt evidence", issue.ID, restartedBlocked)
			}
			snapshot := blockedSnapshots(restartedState.Blocked, nil, parkedAt.Add(time.Minute), nil)
			if len(snapshot) != 1 || snapshot[0].Error != terminalAttemptRetryLimitCause || snapshot[0].AttemptError != wantError || snapshot[0].WorkAttemptID != int64(tt.failures) {
				t.Fatalf("restarted blocked snapshot = %#v, want durable cause and attempt evidence", snapshot)
			}

			if tt.limit != nil && *tt.limit == 0 {
				if blocked.Recovery.Owner != blockedRecoveryOwnerOperator || !blocked.NeedsHumanAttention || blocked.RecoveryIntentResumable {
					t.Fatalf("zero policy park = %#v, want operator-owned hold", blocked)
				}
				if !strings.Contains(tracker.comments[wantComments-1], "external review") || strings.Contains(tracker.comments[wantComments-1], "automatically") {
					t.Fatalf("zero policy comment = %q", tracker.comments[wantComments-1])
				}
			}
		})
	}
}

func TestRetryCycleAttemptErrorIsBoundedByOutputPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configMax int
		wantMax   int
	}{
		{name: "configured output policy", configMax: len(runtimeoutput.Marker) + 32, wantMax: len(runtimeoutput.Marker) + 32},
		{name: "retry evidence hard limit", wantMax: retryCycleAttemptErrorMaxBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			orchestrator := &Orchestrator{cfg: Config{OutputTruncationMaxBytes: tt.configMax}}
			got := retryCycleAttemptError(orchestrator, telemetry.WorkAttempt{ErrorMessage: strings.Repeat("sensitive diagnostic ", 1000)})
			if len(got) > tt.wantMax || !strings.Contains(got, runtimeoutput.Marker) {
				t.Fatalf("retryCycleAttemptError() bytes = %d, want <= %d with truncation marker", len(got), tt.wantMax)
			}
		})
	}
}

func TestHandleRunResultReconcilesDeliverableRecoveryExactHead(t *testing.T) {
	t.Parallel()

	const (
		branch  = "detent/acme_widgets_18"
		headSHA = "current-head"
	)
	tests := []struct {
		name             string
		cached           *connector.PullRequest
		lookup           *connector.PullRequest
		lookupErrors     []error
		lookupFoundAfter int
		wantBlocked      bool
		wantReason       string
		wantLookupCalls  int
		wantMergedReason bool
		wantReasonCode   string
		commitsAhead     int
		remoteBranch     bool
	}{
		{
			name: "open pull request on exact current head reconciles",
			lookup: &connector.PullRequest{
				Number: 18, BranchName: branch, State: "OPEN", HeadSHA: headSHA,
			},
			wantLookupCalls: 1,
			commitsAhead:    1,
			remoteBranch:    true,
		},
		{
			name:            "no pull request parks",
			wantBlocked:     true,
			wantReason:      "no exact-head pull request",
			wantLookupCalls: 3,
			commitsAhead:    1,
			remoteBranch:    true,
		},
		{
			name:            "zero commits ahead parks without pull request lookup",
			wantBlocked:     true,
			wantReason:      "no_commits_to_deliver",
			wantReasonCode:  noCommitsToDeliverReason,
			wantLookupCalls: 0,
		},
		{
			name:            "deleted remote branch parks accurately",
			wantBlocked:     true,
			wantReason:      "remote branch is missing",
			wantLookupCalls: 0,
			commitsAhead:    1,
		},
		{
			name: "transient not found retries then reconciles",
			lookup: &connector.PullRequest{
				Number: 18, BranchName: branch, State: "OPEN", HeadSHA: headSHA,
			},
			lookupFoundAfter: 3,
			wantLookupCalls:  3,
			commitsAhead:     1,
			remoteBranch:     true,
		},
		{
			name: "merged pull request routes to merged deliverable reconciliation",
			lookup: &connector.PullRequest{
				Number: 18, BranchName: branch, State: "MERGED", HeadSHA: headSHA,
				CIStatus: "success", CheckRunCount: 1,
			},
			wantLookupCalls:  1,
			wantMergedReason: true,
			commitsAhead:     1,
			remoteBranch:     true,
		},
		{
			name: "closed unmerged pull request parks accurately",
			lookup: &connector.PullRequest{
				Number: 18, BranchName: branch, State: "CLOSED", HeadSHA: headSHA,
			},
			wantBlocked:     true,
			wantReason:      "closed without merge",
			wantLookupCalls: 1,
			commitsAhead:    1,
			remoteBranch:    true,
		},
		{
			name: "lookup unavailable retries then parks",
			lookupErrors: []error{
				errors.New("lookup unavailable 1"),
				errors.New("lookup unavailable 2"),
				errors.New("lookup unavailable 3"),
			},
			wantBlocked:     true,
			wantReason:      "PR lookup unavailable",
			wantLookupCalls: 3,
			commitsAhead:    1,
			remoteBranch:    true,
		},
		{
			name: "stale cached hydration is not trusted",
			cached: &connector.PullRequest{
				Number: 18, BranchName: branch, State: "OPEN", HeadSHA: headSHA,
			},
			wantBlocked:     true,
			wantReason:      "lookup result: no exact-head pull request",
			wantLookupCalls: 3,
			commitsAhead:    1,
			remoteBranch:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 8, 12, 18, 0, 0, 0, time.UTC)
			issue := terminalRetryTestIssue(tt.name)
			issue.BranchName = branch
			issue.PRRepository = "acme/widgets"
			issue.PullRequest = tt.cached
			issue.Comments = []connector.IssueComment{{
				Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
			}}
			tracker := &terminalRetryConnector{
				issues:           map[string]connector.Issue{issue.ID: cloneIssue(issue)},
				lookup:           tt.lookup,
				lookupErrors:     append([]error(nil), tt.lookupErrors...),
				lookupFoundAfter: tt.lookupFoundAfter,
			}
			attempts := &terminalRetryWorkAttemptStore{}
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress"}, TerminalStates: []string{"Done"}})
			o := &Orchestrator{
				cfg: cfg, connector: tracker, workAttempts: attempts,
				deliverableRecoveryWait: func(context.Context, time.Duration) bool { return true },
			}
			state := newState(cfg)
			state.Running[issue.ID] = Running{
				Issue: issue, Attempt: 1, WorkAttemptID: 42, Mode: runpkg.RunModeImplement,
				StartedAt: now.Add(-time.Minute), WorkProductPushed: true, WorkspacePath: "/work/" + branch,
				DiffStats: DiffStats{
					Status: "clean", HeadSHA: headSHA,
					DeliveryStateChecked: true, CommitsAhead: tt.commitsAhead, RemoteBranchExists: tt.remoteBranch,
				},
			}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
			commandErr := &runpkg.DeliverableCommandError{
				Operation: "codex_apps/github.create_pull_request", Arguments: `{"head":"` + branch + `"}`,
				Status: "failed", Message: "HTTP 503: unavailable",
			}

			o.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID: issue.ID,
				Result: runpkg.RunResult{
					FinalState: runpkg.FinalStateNeedsHumanAttention, PullRequestHeadPushed: true,
					DiffStats: runpkg.DiffStats{
						Status: "clean", HeadSHA: headSHA,
						DeliveryStateChecked: true, CommitsAhead: tt.commitsAhead, RemoteBranchExists: tt.remoteBranch,
					},
				},
				Err:         &runpkg.DeliverableRecoveryError{Branch: branch, Err: commandErr},
				CompletedAt: now,
			})

			if tracker.lookupCalls != tt.wantLookupCalls {
				t.Fatalf("lookup calls = %d, want %d", tracker.lookupCalls, tt.wantLookupCalls)
			}
			if tt.wantLookupCalls > 0 && (tracker.lookupRepository != "acme/widgets" || tracker.lookupBranch != branch || tracker.lookupHeadSHA != headSHA) {
				t.Fatalf(
					"lookup target = %q/%q@%q, want acme/widgets/%s@%s",
					tracker.lookupRepository,
					tracker.lookupBranch,
					tracker.lookupHeadSHA,
					branch,
					headSHA,
				)
			}
			blocked, ok := state.Blocked[issue.ID]
			if ok != tt.wantBlocked {
				t.Fatalf("Blocked[%q] present = %v, want %v: %#v", issue.ID, ok, tt.wantBlocked, blocked)
			}
			if tt.wantBlocked {
				wantReasonCode := tt.wantReasonCode
				if wantReasonCode == "" {
					wantReasonCode = deliverableRecoveryNeedsHumanReason
				}
				if len(attempts.completions) != 1 || attempts.completions[0].ErrorClass != wantReasonCode {
					t.Fatalf("work attempt error class = %#v, want %q", attempts.completions, wantReasonCode)
				}
				if !strings.Contains(blocked.Reason, branch) || !strings.Contains(blocked.Reason, tt.wantReason) {
					t.Fatalf("Blocked[%q].Reason = %q, want branch and %q", issue.ID, blocked.Reason, tt.wantReason)
				}
				if len(tracker.comments) != 3 || !strings.Contains(tracker.comments[2], "local commits ahead: ") ||
					!strings.Contains(tracker.comments[2], "remote branch exists: ") || !strings.Contains(tracker.comments[2], tt.wantReason) {
					t.Fatalf("comments = %#v, want delivery diagnostics containing %q", tracker.comments, tt.wantReason)
				}
				if got := tracker.transitionStates(); !slices.Equal(got, []string{blockedStatusState}) {
					t.Fatalf("state transitions = %v, want [%s]", got, blockedStatusState)
				}
				if tt.cached != nil && tt.lookup == nil && blocked.Issue.PullRequest != nil {
					t.Fatalf("Blocked[%q].Issue.PullRequest = %#v, want stale hydration cleared", issue.ID, blocked.Issue.PullRequest)
				}
				if tt.lookup != nil && normalizePullRequestState(tt.lookup.State) == "closed" &&
					(blocked.Issue.PullRequest == nil || normalizePullRequestState(blocked.Issue.PullRequest.State) != "closed") {
					t.Fatalf("Blocked[%q].Issue.PullRequest = %#v, want fresh closed PR", issue.ID, blocked.Issue.PullRequest)
				}
				return
			}
			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalSuccess {
				t.Fatalf("work attempt completions = %#v, want successful reconciliation", attempts.completions)
			}
			completed := state.Completed[issue.ID]
			if completed.Issue.PullRequest == nil || completed.Issue.PullRequest.Number != 18 {
				t.Fatalf("Completed[%q].Issue.PullRequest = %#v, want reconciled PR 18", issue.ID, completed.Issue.PullRequest)
			}
			if tt.wantMergedReason {
				record := implementProgressRecordFromCompletion(t, attempts.completions[0])
				if record.Reason != implementMergedCompletionReason {
					t.Fatalf("completion reason = %q, want %q", record.Reason, implementMergedCompletionReason)
				}
			}
		})
	}
}

func TestReconcileTerminalAttemptRetryStatesDemotesRecoveredEmptyAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	empty := terminalRetryTestIssue("service-restart-empty")
	pushed := terminalRetryTestIssue("service-restart-pushed")
	tracker := &terminalRetryConnector{issues: map[string]connector.Issue{
		empty.ID:  cloneIssue(empty),
		pushed.ID: cloneIssue(pushed),
	}}
	cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress"}, TerminalStates: []string{"Done"}})
	o := &Orchestrator{cfg: cfg, connector: tracker}
	state := newState(cfg)
	state.WorkAttempts = []telemetry.WorkAttempt{
		{
			AttemptID:          2,
			IssueID:            pushed.ID,
			Identifier:         pushed.Identifier,
			Status:             string(store.WorkAttemptStatusTerminal),
			TerminalState:      string(store.WorkAttemptTerminalAbandoned),
			ErrorClass:         "service_restart",
			CompletedAt:        timePointer(now.Add(-time.Minute)),
			WorkerMetadataJSON: `{"work_product_pushed":true}`,
		},
		{
			AttemptID:     1,
			IssueID:       empty.ID,
			Identifier:    empty.Identifier,
			Status:        string(store.WorkAttemptStatusTerminal),
			TerminalState: string(store.WorkAttemptTerminalAbandoned),
			ErrorClass:    "service_restart",
			CompletedAt:   timePointer(now.Add(-2 * time.Minute)),
		},
	}

	transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{pushed, empty}, now)

	if len(transitions) != 1 || transitions[0].ID != empty.ID || transitions[0].State != "Todo" {
		t.Fatalf("transitions = %#v, want empty attempt moved to Todo", transitions)
	}
	if got := tracker.transitionStates(); !slices.Equal(got, []string{"Todo"}) {
		t.Fatalf("state transitions = %v, want [Todo]", got)
	}
}

func TestReconcileTerminalAttemptRetryStatesBoundsDurableFailures(t *testing.T) {
	t.Parallel()

	const retryLimit = 3
	now := time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		attempts     []telemetry.WorkAttempt
		wantState    string
		wantBlocked  bool
		wantRecovery string
	}{
		{
			name:      "below limit returns to todo",
			attempts:  terminalRetryFailureAttempts("issue-below-limit", now, retryLimit-1),
			wantState: "Todo",
		},
		{
			name:         "limit parks in blocked",
			attempts:     terminalRetryFailureAttempts("issue-at-limit", now, retryLimit),
			wantState:    "Blocked",
			wantBlocked:  true,
			wantRecovery: blockedRecoveryReasonBreakerCooldownActive,
		},
		{
			name: "successful completion resets the sequence",
			attempts: []telemetry.WorkAttempt{
				{
					AttemptID: 3, IssueID: "issue-success-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalFailure), CompletedAt: timePointer(now),
				},
				{
					AttemptID: 2, IssueID: "issue-success-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalSuccess), CompletedAt: timePointer(now.Add(-time.Minute)),
				},
				{
					AttemptID: 1, IssueID: "issue-success-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalFailure), CompletedAt: timePointer(now.Add(-2 * time.Minute)),
				},
			},
			wantState: "Todo",
		},
		{
			name: "pushed work resets the sequence",
			attempts: []telemetry.WorkAttempt{
				{
					AttemptID: 3, IssueID: "issue-push-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalFailure), CompletedAt: timePointer(now),
				},
				{
					AttemptID: 2, IssueID: "issue-push-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalFailure), CompletedAt: timePointer(now.Add(-time.Minute)),
					WorkerMetadataJSON: `{"work_product_pushed":true}`,
				},
				{
					AttemptID: 1, IssueID: "issue-push-reset", Status: string(store.WorkAttemptStatusTerminal),
					TerminalState: string(store.WorkAttemptTerminalFailure), CompletedAt: timePointer(now.Add(-2 * time.Minute)),
				},
			},
			wantState: "Todo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issueID := tt.attempts[0].IssueID
			issue := terminalRetryTestIssue(strings.TrimPrefix(issueID, "issue-"))
			issue.ID = issueID
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			cfg := normalizeConfig(Config{
				ActiveStates:   []string{"Todo", "In Progress"},
				ObservedStates: []string{"Blocked"},
				TerminalStates: []string{"Done"},
			})
			o := &Orchestrator{cfg: cfg, connector: tracker}
			state := newState(cfg)
			state.WorkAttempts = append([]telemetry.WorkAttempt(nil), tt.attempts...)

			transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{issue}, now)

			if len(transitions) != 1 || transitions[0].State != tt.wantState {
				t.Fatalf("transitions = %#v, want one transition to %s", transitions, tt.wantState)
			}
			if got := tracker.transitionStates(); !slices.Equal(got, []string{tt.wantState}) {
				t.Fatalf("state transitions = %v, want [%s]", got, tt.wantState)
			}
			blocked, ok := state.Blocked[issue.ID]
			if ok != tt.wantBlocked {
				t.Fatalf("Blocked[%q] present = %v, want %v: %#v", issue.ID, ok, tt.wantBlocked, blocked)
			}
			if tt.wantBlocked && (blocked.RecoveryReason != tt.wantRecovery || blocked.Recovery == nil || blocked.Recovery.Predicate != blockedRecoveryPredicateBreakerCooldown) {
				t.Fatalf("Blocked[%q] = %#v, want durable breaker cooldown recovery", issue.ID, blocked)
			}
		})
	}
}

func TestReconcileTerminalAttemptRetryStatesUsesDurableIssueHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		errorClass  string
		historyErr  error
		wantState   string
		wantBlocked bool
		wantReason  string
		wantEvent   string
	}{
		{name: "persisted terminal history reaches limit", errorClass: workAttemptErrorRunner, wantState: "Blocked", wantBlocked: true, wantReason: terminalAttemptRetryLimitCause},
		{name: "persisted workspace history reaches limit", errorClass: workAttemptErrorWorkspace, wantState: "Blocked", wantBlocked: true, wantReason: workspacePreparationRetryLimitCause},
		{name: "history lookup failure fails open", errorClass: workAttemptErrorRunner, historyErr: errors.New("history unavailable"), wantState: "Todo", wantEvent: terminalAttemptRetryHistoryUnavailableEvent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := terminalRetryTestIssue("durable-history")
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			attempts := &terminalRetryWorkAttemptStore{historyErr: tt.historyErr}
			for attempt := 3; attempt >= 1; attempt-- {
				attempts.history = append(attempts.history, store.WorkAttempt{
					ID:            int64(attempt),
					IssueID:       issue.ID,
					Identifier:    issue.Identifier,
					Status:        store.WorkAttemptStatusTerminal,
					TerminalState: store.WorkAttemptTerminalFailure,
					ErrorClass:    tt.errorClass,
					CompletedAt:   now.Add(-time.Duration(3-attempt) * time.Minute),
				})
			}
			cfg := normalizeConfig(Config{
				Project:        scheduler.ProjectCandidate{ID: "detent"},
				ActiveStates:   []string{"Todo", "In Progress"},
				ObservedStates: []string{"Blocked"},
				TerminalStates: []string{"Done"},
			})
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)
			state.WorkAttempts = terminalRetryFailureAttempts(issue.ID, now, 1)
			state.WorkAttempts[0].ErrorClass = tt.errorClass

			transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{issue}, now)

			if len(transitions) != 1 || transitions[0].State != tt.wantState {
				t.Fatalf("transitions = %#v, want one transition to %s", transitions, tt.wantState)
			}
			blocked, ok := state.Blocked[issue.ID]
			if ok != tt.wantBlocked {
				t.Fatalf("Blocked[%q] present = %v, want %v", issue.ID, ok, tt.wantBlocked)
			}
			if tt.wantBlocked && blocked.Reason != tt.wantReason {
				t.Fatalf("Blocked[%q].Reason = %q, want %q", issue.ID, blocked.Reason, tt.wantReason)
			}
			if len(attempts.historyQueries) != 1 {
				t.Fatalf("history queries = %#v, want one", attempts.historyQueries)
			}
			query := attempts.historyQueries[0]
			if query.ProjectID != "detent" || query.IssueID != issue.ID || query.Identifier != issue.Identifier || query.Limit != consecutiveRetryCycleLimit {
				t.Fatalf("history query = %#v, want durable issue identity and limit", query)
			}
			if tt.wantEvent != "" {
				if event, ok := recentStateEvent(state, tt.wantEvent); !ok || event.Message == "" {
					t.Fatalf("RecentEvents = %#v, want %s", state.RecentEvents, tt.wantEvent)
				}
			}
		})
	}
}

func TestRetryCycleAttemptMatches(t *testing.T) {
	t.Parallel()

	base := telemetry.WorkAttempt{
		Status:        string(store.WorkAttemptStatusTerminal),
		TerminalState: string(store.WorkAttemptTerminalFailure),
		ErrorClass:    workAttemptErrorRunner,
	}
	tests := []struct {
		name         string
		mutate       func(*telemetry.WorkAttempt)
		cause        string
		wantMatching bool
	}{
		{name: "service restart does not count", cause: terminalAttemptRetryLimitCause, mutate: func(attempt *telemetry.WorkAttempt) {
			attempt.ErrorClass = "service_restart"
			attempt.TerminalState = string(store.WorkAttemptTerminalAbandoned)
		}},
		{name: "failure", cause: terminalAttemptRetryLimitCause, wantMatching: true},
		{name: "timed out", cause: terminalAttemptRetryLimitCause, wantMatching: true, mutate: func(attempt *telemetry.WorkAttempt) {
			attempt.TerminalState = string(store.WorkAttemptTerminalTimedOut)
		}},
		{name: "abandoned", cause: terminalAttemptRetryLimitCause, wantMatching: true, mutate: func(attempt *telemetry.WorkAttempt) {
			attempt.TerminalState = string(store.WorkAttemptTerminalAbandoned)
		}},
		{name: "capacity", cause: terminalAttemptRetryLimitCause, wantMatching: true, mutate: func(attempt *telemetry.WorkAttempt) {
			attempt.TerminalState = string(store.WorkAttemptTerminalCapacity)
		}},
		{name: "success resets terminal", cause: terminalAttemptRetryLimitCause, mutate: func(attempt *telemetry.WorkAttempt) { attempt.TerminalState = string(store.WorkAttemptTerminalSuccess) }},
		{name: "pushed work resets terminal", cause: terminalAttemptRetryLimitCause, mutate: func(attempt *telemetry.WorkAttempt) { attempt.WorkerMetadataJSON = `{"work_product_pushed":true}` }},
		{name: "forge outage resets terminal", cause: terminalAttemptRetryLimitCause, mutate: func(attempt *telemetry.WorkAttempt) { attempt.ErrorClass = forgeUnavailableErrorClass }},
		{name: "workspace is separate from terminal", cause: terminalAttemptRetryLimitCause, mutate: func(attempt *telemetry.WorkAttempt) { attempt.ErrorClass = workAttemptErrorWorkspace }},
		{name: "workspace failure", cause: workspacePreparationRetryLimitCause, wantMatching: true, mutate: func(attempt *telemetry.WorkAttempt) { attempt.ErrorClass = workAttemptErrorWorkspace }},
		{name: "runner failure resets workspace", cause: workspacePreparationRetryLimitCause},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			attempt := base
			if tt.mutate != nil {
				tt.mutate(&attempt)
			}
			if got := retryCycleAttemptMatches(attempt, tt.cause); got != tt.wantMatching {
				t.Fatalf("retryCycleAttemptMatches() = %v, want %v for %#v", got, tt.wantMatching, attempt)
			}
		})
	}
}

func terminalRetryFailureAttempts(issueID string, now time.Time, count int) []telemetry.WorkAttempt {
	attempts := make([]telemetry.WorkAttempt, 0, count)
	for index := range count {
		attemptID := int64(count - index)
		attempts = append(attempts, telemetry.WorkAttempt{
			AttemptID:     attemptID,
			IssueID:       issueID,
			Status:        string(store.WorkAttemptStatusTerminal),
			TerminalState: string(store.WorkAttemptTerminalFailure),
			ErrorClass:    "runner_error",
			CompletedAt:   timePointer(now.Add(-time.Duration(index) * time.Minute)),
		})
	}
	return attempts
}

func TestReconcileTerminalAttemptRetryStatesHandlesGitHubRESTCapacityCompatibility(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	resetAt := now.Add(time.Hour)
	retryAt := resetAt.Add(backendCapacityResetJitter)
	durableMetadata := `{"github_rest_wait":{"consumer":"shared_pool","credential_identity":"github-rest:worker","remaining":1009,"limit":5000,"reserve":1250,"reset_at":"` + resetAt.Format(time.RFC3339) + `","retry_at":"` + retryAt.Format(time.RFC3339) + `"}}`
	tests := []struct {
		name         string
		metadata     string
		wantState    string
		wantDemotion bool
	}{
		{name: "legacy metadata-less attempt", wantState: "Todo", wantDemotion: true},
		{name: "durable wait metadata", metadata: durableMetadata, wantState: "In Progress"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := terminalRetryTestIssue(strings.ReplaceAll(tt.name, " ", "-"))
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress"}, TerminalStates: []string{"Done"}})
			o := &Orchestrator{cfg: cfg, connector: tracker}
			state := newState(cfg)
			state.WorkAttempts = []telemetry.WorkAttempt{{
				AttemptID:          1,
				IssueID:            issue.ID,
				Identifier:         issue.Identifier,
				Status:             string(store.WorkAttemptStatusTerminal),
				TerminalState:      string(store.WorkAttemptTerminalCapacity),
				ErrorClass:         githubRESTCapacityError,
				CompletedAt:        timePointer(now.Add(-time.Minute)),
				WorkerMetadataJSON: tt.metadata,
			}}

			transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{issue}, now)

			if got := len(transitions) == 1; got != tt.wantDemotion {
				t.Fatalf("demoted = %v, want %v: %#v", got, tt.wantDemotion, transitions)
			}
			if tt.wantDemotion && transitions[0].State != tt.wantState {
				t.Fatalf("transition state = %q, want %q", transitions[0].State, tt.wantState)
			}
			if got := tracker.transitionStates(); tt.wantDemotion && !slices.Equal(got, []string{tt.wantState}) {
				t.Fatalf("state transitions = %v, want [%s]", got, tt.wantState)
			} else if !tt.wantDemotion && len(got) != 0 {
				t.Fatalf("state transitions = %v, want none", got)
			}
		})
	}
}

func TestReconcileTerminalAttemptRetryStatesRespectsLiveForeignClaim(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 13, 30, 0, 0, time.UTC)
	foreign := terminalRetryTestIssue("foreign-claim")
	foreign.Assignees = []string{"other-worker"}
	foreign.Fields["Lease"] = formatClaimTime(now.Add(-30 * time.Second))
	tracker := &terminalRetryConnector{issues: map[string]connector.Issue{foreign.ID: cloneIssue(foreign)}}
	cfg := normalizeConfig(Config{
		ActiveStates:   []string{"Todo", "In Progress"},
		TerminalStates: []string{"Done"},
		Claiming: ClaimingConfig{
			Enabled:       true,
			AssigneeLogin: "detent-worker",
			LeaseField:    "Lease",
			LeaseTTL:      time.Minute,
		},
	})
	o := &Orchestrator{cfg: cfg, connector: tracker}
	state := newState(cfg)
	state.WorkAttempts = []telemetry.WorkAttempt{{
		AttemptID:     1,
		IssueID:       foreign.ID,
		Identifier:    foreign.Identifier,
		Status:        string(store.WorkAttemptStatusTerminal),
		TerminalState: string(store.WorkAttemptTerminalFailure),
		ErrorClass:    "runner_error",
		CompletedAt:   timePointer(now.Add(-time.Minute)),
	}}

	transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{foreign}, now)

	if len(transitions) != 0 {
		t.Fatalf("transitions = %#v, want active foreign claim left In Progress", transitions)
	}
	if got := tracker.transitionStates(); len(got) != 0 {
		t.Fatalf("state transitions = %v, want none", got)
	}
}

func TestTerminalAttemptDemotionLetsUrgentTodoRankFirst(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	retrying := terminalRetryTestIssue("retrying")
	urgent := terminalRetryTestIssue("urgent")
	urgent.State = "Todo"
	urgentPriority := 1
	urgent.Priority = &urgentPriority
	tracker := &terminalRetryConnector{issues: map[string]connector.Issue{retrying.ID: cloneIssue(retrying), urgent.ID: cloneIssue(urgent)}}
	cfg := normalizeConfig(Config{
		ActiveStates:            []string{"Todo", "In Progress"},
		TerminalStates:          []string{"Done"},
		DispatchPriorityByState: []string{"In Progress", "Todo"},
	})
	o := &Orchestrator{cfg: cfg, connector: tracker}
	state := newState(cfg)
	state.WorkAttempts = []telemetry.WorkAttempt{{
		AttemptID:     1,
		IssueID:       retrying.ID,
		Identifier:    retrying.Identifier,
		Status:        string(store.WorkAttemptStatusTerminal),
		TerminalState: string(store.WorkAttemptTerminalFailure),
		ErrorClass:    "runner_error",
		CompletedAt:   timePointer(now.Add(-time.Minute)),
	}}

	transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{retrying, urgent}, now)
	issues := overlayIssueStateSnapshots([]connector.Issue{retrying, urgent}, transitions)
	sortIssuesForDispatch(issues, cfg.DispatchPriorityByState, cfg.DispatchPriorityByLabel, false)

	if len(issues) != 2 || issues[0].ID != urgent.ID {
		t.Fatalf("dispatch order = %v, want urgent Todo first", []string{issues[0].ID, issues[1].ID})
	}
}

func terminalRetryTestIssue(suffix string) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = "issue-" + suffix
	issue.Identifier = "digitaldrywood/detent#1432-" + suffix
	issue.Title = "Terminal retry " + suffix
	issue.State = "In Progress"
	return issue
}

func terminalRetryTestIssueWithPullRequest(suffix string) connector.Issue {
	issue := terminalRetryTestIssue(suffix)
	prNumber := 1432
	issue.PRNumber = &prNumber
	issue.PullRequest = &connector.PullRequest{Number: prNumber, State: "OPEN"}
	return issue
}

type terminalRetryConnector struct {
	updateErrors     map[string]error
	issues           map[string]connector.Issue
	transitions      []string
	comments         []string
	lookup           *connector.PullRequest
	lookupErrors     []error
	lookupCalls      int
	lookupFoundAfter int
	lookupRepository string
	lookupBranch     string
	lookupHeadSHA    string
}

func (c *terminalRetryConnector) Name() string { return "terminal-retry" }

func (c *terminalRetryConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *terminalRetryConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *terminalRetryConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	issues := make([]connector.Issue, 0, len(ids))
	for _, id := range ids {
		if issue, ok := c.issues[id]; ok {
			issues = append(issues, cloneIssue(issue))
		}
	}
	return issues, nil
}

func (c *terminalRetryConnector) FetchIssueComments(_ context.Context, issue connector.Issue) ([]connector.IssueComment, error) {
	return append([]connector.IssueComment(nil), c.issues[issue.ID].Comments...), nil
}

func (c *terminalRetryConnector) LookupPullRequestByHead(_ context.Context, repository string, branch string, headSHA string) (connector.PullRequest, bool, error) {
	c.lookupCalls++
	c.lookupRepository = repository
	c.lookupBranch = branch
	c.lookupHeadSHA = headSHA
	if c.lookupCalls <= len(c.lookupErrors) {
		return connector.PullRequest{}, false, c.lookupErrors[c.lookupCalls-1]
	}
	if c.lookup == nil {
		return connector.PullRequest{}, false, nil
	}
	if c.lookupFoundAfter > c.lookupCalls {
		return connector.PullRequest{}, false, nil
	}
	pullRequest := *c.lookup
	return pullRequest, true, nil
}

func (c *terminalRetryConnector) HydratePullRequest(_ context.Context, issue connector.Issue) (connector.Issue, error) {
	if c.lookup == nil {
		return issue, nil
	}
	pullRequest := *c.lookup
	issue.PullRequest = &pullRequest
	issue.PRRepository = "acme/widgets"
	number := pullRequest.Number
	issue.PRNumber = &number
	return issue, nil
}

func (c *terminalRetryConnector) CreateComment(_ context.Context, _ string, body string) error {
	c.comments = append(c.comments, body)
	return nil
}

func (c *terminalRetryConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	if err := c.updateErrors[state]; err != nil {
		return err
	}
	issue := c.issues[issueID]
	issue.State = state
	c.issues[issueID] = issue
	c.transitions = append(c.transitions, state)
	return nil
}

func (c *terminalRetryConnector) SetAssignee(context.Context, string, string) error { return nil }

func (c *terminalRetryConnector) SetField(context.Context, string, string, string) error { return nil }

func (c *terminalRetryConnector) transitionStates() []string {
	return append([]string(nil), c.transitions...)
}

type terminalRetryWorkAttemptStore struct {
	completionErr  error
	completions    []store.WorkAttemptCompletion
	history        []store.WorkAttempt
	historyErr     error
	historyQueries []store.WorkAttemptHistoryQuery
}

func (s *terminalRetryWorkAttemptStore) StartWorkAttempt(context.Context, store.WorkAttemptStart) (int64, error) {
	return 0, nil
}

func (s *terminalRetryWorkAttemptStore) WorkAttempt(context.Context, int64) (store.WorkAttempt, error) {
	return store.WorkAttempt{}, store.ErrNotFound
}

func (s *terminalRetryWorkAttemptStore) RecordWorkAttemptHeartbeat(context.Context, store.WorkAttemptHeartbeat) error {
	return nil
}

func (s *terminalRetryWorkAttemptStore) CompleteWorkAttempt(_ context.Context, completion store.WorkAttemptCompletion) error {
	s.completions = append(s.completions, completion)
	return s.completionErr
}

func (s *terminalRetryWorkAttemptStore) TimeoutExpiredWorkAttempts(context.Context, store.WorkAttemptTimeout) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *terminalRetryWorkAttemptStore) ReclaimActiveWorkAttempts(context.Context, store.WorkAttemptReclaim) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *terminalRetryWorkAttemptStore) ListActiveWorkAttempts(context.Context, store.WorkAttemptQuery) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *terminalRetryWorkAttemptStore) ListRecentTerminalWorkAttempts(_ context.Context, query store.WorkAttemptHistoryQuery) ([]store.WorkAttempt, error) {
	s.historyQueries = append(s.historyQueries, query)
	return append([]store.WorkAttempt(nil), s.history...), s.historyErr
}

func (s *terminalRetryWorkAttemptStore) RecordSchedulerDecision(context.Context, store.SchedulerDecision) (int64, error) {
	return 0, nil
}

func (s *terminalRetryWorkAttemptStore) ListRecentSchedulerDecisions(context.Context, store.SchedulerDecisionQuery) ([]store.SchedulerDecision, error) {
	return nil, nil
}

func terminalRetryMetadataPushed(raw string) bool {
	return workAttemptMetadataHasPushedProduct(raw)
}

func TestConfiguredTerminalRetryAfterStoreRestart(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		limit     *int
		sequence  string
		wantState string
	}{
		{name: "zero first failure", limit: new(0), sequence: "F", wantState: "Blocked"},
		{name: "one permits recovery", limit: new(1), sequence: "F", wantState: "Todo"},
		{name: "one bounds recovery", limit: new(1), sequence: "FRF", wantState: "Blocked"},
		{name: "default permits two", sequence: "FF", wantState: "Todo"},
		{name: "default bounds recovery", sequence: "FRFRF", wantState: "Blocked"},
		{name: "three bounds recovery", limit: new(3), sequence: "FFF", wantState: "Blocked"},
		{name: "larger configured limit", limit: new(6), sequence: "FFRRFFRRFF", wantState: "Blocked"},
		{name: "zero capacity wait", limit: new(0), sequence: "C", wantState: "In Progress"},
		{name: "zero GitHub wait", limit: new(0), sequence: "G", wantState: "In Progress"},
		{name: "zero forge wait", limit: new(0), sequence: "A", wantState: "In Progress"},
		{name: "zero service restart", limit: new(0), sequence: "RRR", wantState: "Todo"},
		{name: "capacity does not consume", limit: new(1), sequence: "CCF", wantState: "Todo"},
		{name: "GitHub does not consume", limit: new(1), sequence: "GGF", wantState: "Todo"},
		{name: "forge does not consume", limit: new(1), sequence: "AAF", wantState: "Todo"},
		{name: "capacity retains reset behavior", limit: new(1), sequence: "FCF", wantState: "Todo"},
		{name: "success resets", limit: new(1), sequence: "FSF", wantState: "Todo"},
		{name: "pushed work resets", limit: new(1), sequence: "FPF", wantState: "Todo"},
		{name: "pushed work prevents retry", limit: new(0), sequence: "P", wantState: "In Progress"},
		{name: "workspace still retries", limit: new(0), sequence: "WW", wantState: "Todo"},
		{name: "workspace still parks at three", limit: new(0), sequence: "WWW", wantState: "Blocked"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			issue := terminalRetryTestIssue("configured-restart")
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			workflow, err := workflowconfig.ParseWorkflow([]byte("---\ntracker:\n  kind: memory\n---\n"))
			if err != nil {
				t.Fatal(err)
			}
			workflow.Config.Recovery.TerminalAttemptRetryLimit = tt.limit
			cfg := normalizeConfig(ConfigFromWorkflow(workflow.Config))
			cfg.Project.ID = "detent"
			path := filepath.Join(t.TempDir(), "attempts.db")
			db, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: path})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			var latestID int64
			for index, kind := range tt.sequence {
				at := now.Add(time.Duration(index) * time.Minute)
				id, err := db.StartWorkAttempt(ctx, store.WorkAttemptStart{
					ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier,
					WorkerType: "implement", StartedAt: at.Add(-time.Second), LeaseExpiresAt: at.Add(time.Hour),
				})
				if err != nil {
					t.Fatal(err)
				}
				latestID = id
				completion := store.WorkAttemptCompletion{AttemptID: id, CompletedAt: at, TerminalState: store.WorkAttemptTerminalFailure, ErrorClass: workAttemptErrorRunner, ErrorMessage: "worker failed"}
				switch kind {
				case 'R':
					completion.TerminalState = store.WorkAttemptTerminalAbandoned
					completion.ErrorClass = "service_restart"
				case 'C':
					completion.TerminalState = store.WorkAttemptTerminalCapacity
					completion.ErrorClass = backendcapacity.ErrorClass
				case 'G':
					completion.TerminalState = store.WorkAttemptTerminalCapacity
					completion.ErrorClass = githubRESTCapacityError
					completion.WorkerMetadataJSON = `{"github_rest_wait":{"consumer":"shared_pool","credential_identity":"github-rest:worker","remaining":0,"limit":5000,"reserve":1250,"reset_at":"2026-09-08T18:00:00Z","retry_at":"2026-09-08T18:01:00Z"}}`
				case 'A':
					completion.ErrorClass = forgeUnavailableErrorClass
				case 'W':
					completion.ErrorClass = workAttemptErrorWorkspace
				case 'S':
					completion.TerminalState = store.WorkAttemptTerminalSuccess
				case 'P':
					completion.WorkerMetadataJSON = `{"work_product_pushed":true}`
				}
				if err := db.CompleteWorkAttempt(ctx, completion); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: path})
			if err != nil {
				t.Fatal(err)
			}
			latest, err := db.WorkAttempt(ctx, latestID)
			if err != nil {
				t.Fatal(err)
			}
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: db, workflowMetrics: metrics}
			state := newState(cfg)
			state.WorkAttempts = []telemetry.WorkAttempt{telemetryWorkAttempt(latest, now)}
			at := now.Add(24 * time.Hour)
			o.reconcileTerminalAttemptRetryStates(ctx, &state, []connector.Issue{issue}, at)
			if got := tracker.issues[issue.ID].State; got != tt.wantState {
				t.Fatalf("state after reopening store = %q, want %q", got, tt.wantState)
			}
			if tt.limit == nil || *tt.limit != 0 || tt.sequence != "F" {
				return
			}
			blockedIssue := tracker.issues[issue.ID]
			blockedIssue.StageUpdatedAt = &at
			restarted := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: db, workflowMetrics: metrics}
			restartedState := newState(cfg)
			restarted.setBlockedStatusIssue(ctx, &restartedState, blockedIssue, at.Add(time.Hour))
			if restarted.recoverCauseBlockedIssue(ctx, &restartedState, blockedIssue, at.Add(30*24*time.Hour)) {
				t.Fatal("zero policy released operator hold after restart and cooldown")
			}
			blocked := restartedState.Blocked[issue.ID]
			if blocked.Recovery == nil || blocked.Recovery.Owner != blockedRecoveryOwnerOperator || blocked.RecoveryAction != "hold" || !blocked.NeedsHumanAttention || blocked.WorkAttemptID != latestID {
				t.Fatalf("restarted operator hold = %#v", blocked)
			}
			if len(restartedState.Retry) != 0 {
				t.Fatal("operator hold scheduled a retry")
			}
		})
	}
}

func TestConsecutiveRetryCycleCountAcrossServiceRestarts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sequence   string
		cause      string
		wantCount  int
		wantLatest int64
	}{
		{name: "restarts alone", sequence: "RRR"},
		{name: "restart after failure", sequence: "FR", wantCount: 1, wantLatest: 1},
		{name: "failure after restart", sequence: "RF", wantCount: 1, wantLatest: 2},
		{name: "failures straddle restarts", sequence: "FRFRF", wantCount: 3, wantLatest: 5},
		{name: "restart at existing limit", sequence: "FFFR", wantCount: 3, wantLatest: 3},
		{name: "restart history beyond snapshot window", sequence: "FF" + strings.Repeat("R", 60) + "F", wantCount: 3, wantLatest: 63},
		{name: "success resets across restart", sequence: "FFRSRF", wantCount: 1, wantLatest: 6},
		{name: "pushed restart resets", sequence: "FFRPRF", wantCount: 1, wantLatest: 6},
		{name: "linked PR restart resets", sequence: "FFRLRF", wantCount: 1, wantLatest: 6},
		{name: "workspace failures straddle restarts", sequence: "WRWRW", cause: workspacePreparationRetryLimitCause, wantCount: 3, wantLatest: 5},
		{name: "workspace failure resets terminal", sequence: "FFRWRF", wantCount: 1, wantLatest: 6},
		{name: "implementation resets workspace", sequence: "WWRFRW", cause: workspacePreparationRetryLimitCause, wantCount: 1, wantLatest: 6},
	}
	for _, tt := range tests {
		for _, durable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/durable=%t", tt.name, durable), func(t *testing.T) {
				t.Parallel()
				now := time.Date(2026, 9, 7, 17, 0, 0, 0, time.UTC)
				issue := terminalRetryTestIssue("restart-history")
				cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}})
				o := &Orchestrator{cfg: cfg}
				state := newState(cfg)
				if durable {
					o.workAttempts = openWorkAttemptRecoveryStore(t, t.Context())
				}
				for index, kind := range tt.sequence {
					at := now.Add(time.Duration(index) * time.Minute)
					attempt := store.WorkAttempt{
						ID: int64(index + 1), ProjectID: "detent", IssueID: issue.ID,
						Identifier: issue.Identifier, Status: store.WorkAttemptStatusTerminal,
						TerminalState: store.WorkAttemptTerminalFailure, ErrorClass: workAttemptErrorRunner,
						CompletedAt: at,
					}
					switch kind {
					case 'R', 'P', 'L':
						attempt.TerminalState = store.WorkAttemptTerminalAbandoned
						attempt.ErrorClass = "service_restart"
						if kind == 'P' {
							attempt.WorkerMetadataJSON = `{"work_product_pushed":true}`
						}
						if kind == 'L' {
							pr := int64(42)
							attempt.PRNumber = &pr
						}
					case 'S':
						attempt.TerminalState = store.WorkAttemptTerminalSuccess
					case 'W':
						attempt.ErrorClass = workAttemptErrorWorkspace
					}
					if durable {
						id, err := o.workAttempts.StartWorkAttempt(t.Context(), store.WorkAttemptStart{
							ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier,
							WorkerType: "implement", StartedAt: at.Add(-time.Second), LeaseExpiresAt: at.Add(time.Minute),
							PRNumber: attempt.PRNumber,
						})
						if err != nil {
							t.Fatal(err)
						}
						attempt.ID = id
						if err := o.workAttempts.CompleteWorkAttempt(t.Context(), store.WorkAttemptCompletion{
							AttemptID: id, CompletedAt: at, TerminalState: attempt.TerminalState,
							ErrorClass: attempt.ErrorClass, WorkerMetadataJSON: attempt.WorkerMetadataJSON,
						}); err != nil {
							t.Fatal(err)
						}
					}
					if !durable || index == len(tt.sequence)-1 {
						state.WorkAttempts = append(state.WorkAttempts, telemetryWorkAttempt(attempt, at))
					}
				}
				cause := tt.cause
				if cause == "" {
					cause = terminalAttemptRetryLimitCause
				}
				count, latest, known := o.consecutiveRetryCycleCount(t.Context(), &state, issue, cause, now)
				if !known || count != tt.wantCount || latest.AttemptID != tt.wantLatest {
					t.Fatalf("count/latest/known = %d/%d/%t, want %d/%d/true", count, latest.AttemptID, known, tt.wantCount, tt.wantLatest)
				}
			})
		}
	}
}

func TestServiceRestartsRecoverRetainedWorkAndDispatch(t *testing.T) {
	t.Parallel()

	for _, restarts := range []int{3, 6} {
		t.Run(fmt.Sprintf("%d restarts", restarts), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			runtimeStore := openWorkAttemptRecoveryStore(t, ctx)
			workspacePath := t.TempDir()
			retainedPath := filepath.Join(workspacePath, "implementation.go")
			retainedWork := []byte("package implementation\n")
			if err := os.WriteFile(retainedPath, retainedWork, 0o600); err != nil {
				t.Fatal(err)
			}
			metadata, err := json.Marshal(map[string]any{"workspace_path": workspacePath, "work_product_pushed": false})
			if err != nil {
				t.Fatal(err)
			}
			issue := terminalRetryTestIssue("restart-retained-work")
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			cfg := normalizeConfig(Config{
				Project: scheduler.ProjectCandidate{ID: "detent"}, MaxConcurrentAgents: 1,
				ActiveStates: []string{"Todo", "In Progress"}, TerminalStates: []string{"Done"},
			})
			now := time.Now().UTC()
			var o *Orchestrator
			var state State
			for index := range restarts {
				at := now.Add(time.Duration(index) * time.Minute)
				issue.State = "In Progress"
				tracker.issues[issue.ID] = cloneIssue(issue)
				id, err := runtimeStore.StartWorkAttempt(ctx, store.WorkAttemptStart{
					ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier,
					WorkerType: "implement", StartedAt: at, LeaseExpiresAt: at.Add(time.Hour),
					WorkerMetadataJSON: string(metadata),
				})
				if err != nil {
					t.Fatal(err)
				}
				o = &Orchestrator{cfg: cfg, connector: tracker, workAttempts: runtimeStore}
				state = newState(cfg)
				o.recoverDurableWorkAttempts(ctx, &state, at.Add(time.Second))
				recovered, err := runtimeStore.WorkAttempt(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if recovered.ErrorClass != "service_restart" || recovered.Phase != "recovered" || recovered.ErrorMessage != "work attempt reclaimed after scheduler restart" || recovered.WorkerMetadataJSON != string(metadata) {
					t.Fatalf("recovered attempt = %#v, want restart with retained workspace metadata", recovered)
				}
				transitions := o.reconcileTerminalAttemptRetryStates(ctx, &state, []connector.Issue{issue}, at.Add(time.Second))
				if len(transitions) != 1 || transitions[0].State != "Todo" {
					t.Fatalf("restart %d transitions = %#v, want Todo", index+1, transitions)
				}
				issue = transitions[0]
				if _, blocked := state.Blocked[issue.ID]; blocked || len(tracker.comments) != 0 {
					t.Fatal("service restart parked issue or reported failure limit")
				}
			}
			runner := newWorkerHostRunner()
			o.supervisor = newTestSupervisor(t, runner, cfg)
			o.runResults = make(chan runpkg.Completion, 1)
			o.dispatchReadyIssues(ctx, &state, []connector.Issue{issue}, now.Add(time.Duration(restarts)*time.Minute))
			request := receiveWorkerHostRunRequest(t, runner.started)
			if request.Issue.ID != issue.ID || request.WorkAttemptID <= int64(restarts) {
				t.Fatalf("dispatched request = %#v, want new attempt for recovered issue", request)
			}
			retained, err := os.ReadFile(retainedPath)
			if err != nil || string(retained) != string(retainedWork) {
				t.Fatalf("retained work = %q, error = %v", retained, err)
			}
			state.Running[issue.ID].cancel()
		})
	}
}

func TestTerminalRetryTransitionFailures(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		limit      *int
		failState  string
		wantState  string
		wantParked bool
	}{
		{name: "zero holds locally when tracker park fails", limit: new(0), failState: "Blocked", wantState: "Blocked", wantParked: true},
		{name: "default preserves retry when tracker park fails", failState: "Blocked", wantState: "Todo"},
		{name: "retry transition failure retains original lane", failState: "Todo", wantState: "In Progress"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			issue := terminalRetryTestIssue(tt.name)
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: issue}, updateErrors: map[string]error{tt.failState: errors.New("tracker unavailable")}}
			cfg := normalizeConfig(Config{Recovery: workflowconfig.Recovery{TerminalAttemptRetryLimit: tt.limit}, ActiveStates: []string{"Todo", "In Progress"}})
			o := &Orchestrator{cfg: cfg, connector: tracker, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			state := newState(cfg)
			count := 3
			if tt.failState == "Todo" {
				count = 1
			}
			for i := 1; i <= count; i++ {
				state.WorkAttempts = append(state.WorkAttempts, telemetry.WorkAttempt{AttemptID: int64(i), IssueID: issue.ID, Status: string(store.WorkAttemptStatusTerminal), TerminalState: string(store.WorkAttemptTerminalFailure), ErrorClass: workAttemptErrorRunner})
			}
			updated, _, parked := o.demoteTerminalAttemptRetry(t.Context(), &state, issue, false, terminalAttemptRetryLimitCause, true, RunModeImplement, DiffStats{}, now)
			if updated.State != tt.wantState || parked != tt.wantParked {
				t.Fatalf("state/parked = %s/%t, want %s/%t", updated.State, parked, tt.wantState, tt.wantParked)
			}
			if tt.wantParked {
				if len(state.Retry) != 0 || state.Blocked[issue.ID].Recovery.Owner != blockedRecoveryOwnerOperator {
					t.Fatal("failed tracker park did not retain a local operator hold")
				}
				if tracker.issues[issue.ID].State != "In Progress" {
					t.Fatal("failed tracker park changed the remote lane")
				}
				if len(tracker.comments) != 1 {
					t.Fatalf("failed tracker park comments = %v, want pending marker", tracker.comments)
				}
				marker, valid := parseTrackerRecoveryPark(tracker.comments[0])
				if !valid || marker.Phase != "pending" {
					t.Fatalf("failed tracker park marker = %#v, want pending operation", marker)
				}
				o.trackCandidateBlockedStatusIssues(t.Context(), &state, []connector.Issue{updated}, now)
				if _, held := state.Blocked[issue.ID]; !held {
					t.Fatal("candidate reconciliation cleared the local hold")
				}
				delete(tracker.updateErrors, "Blocked")
				transitions := o.reconcileTerminalAttemptRetryStates(t.Context(), &state, []connector.Issue{tracker.issues[issue.ID]}, now.Add(time.Minute))
				if len(transitions) != 1 || tracker.issues[issue.ID].State != "Blocked" || len(state.Retry) != 0 {
					t.Fatal("reconciliation did not complete the park without redispatch")
				}
			}
		})
	}
}
