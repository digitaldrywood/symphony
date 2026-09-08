package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestMergeRevocationCompletionReleasesAttemptAndCapacity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC)
	issue := connector.Issue{
		ID:           "issue-revoked-capacity",
		Identifier:   "digitaldrywood/detent#1434",
		State:        "Merging",
		PRRepository: "digitaldrywood/detent",
		PullRequest: &connector.PullRequest{
			Number: 1435,
			State:  "OPEN",
		},
	}
	revoked := cloneIssue(issue)
	revoked.State = "Blocked"
	project := scheduler.ProjectCandidate{ID: "detent", Weight: 1}
	dispatchGate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 1}))
	slot, ok, err := dispatchGate.TryAcquire(t.Context(), project, scheduler.SlotRequest{State: "Merging"}, now)
	if err != nil || !ok {
		t.Fatalf("TryAcquire() = %#v, %v, want acquired slot", slot, err)
	}
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		Project:             project,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	attempts := &recordingWorkAttemptStore{}
	tracker := &runningStateConnector{issues: []connector.Issue{revoked}}
	orch := &Orchestrator{
		cfg:                cfg,
		connector:          tracker,
		workAttempts:       attempts,
		globalDispatchGate: dispatchGate,
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	runCtx, stop := context.WithCancelCause(context.Background())
	state.Running[issue.ID] = Running{
		Issue:         cloneIssue(issue),
		Attempt:       2,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeMerge,
		StartedAt:     now.Add(-time.Hour),
		globalSlot:    slot,
		stop:          stop,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Hour)}
	state.Retry[issue.ID] = Retry{Issue: cloneIssue(issue), Attempt: 3, DueAt: now.Add(time.Hour)}

	orch.reconcileRunningIssues(t.Context(), &state, now)
	if !errors.Is(context.Cause(runCtx), runpkg.ErrMergeRevoked) {
		t.Fatalf("context cause = %v, want ErrMergeRevoked", context.Cause(runCtx))
	}
	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now.Add(time.Second),
		Err:         runpkg.ErrMergeRevoked,
	})

	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after merge revocation", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after merge revocation", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after merge revocation", issue.ID)
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions = %#v, want one", attempts.completions)
	}
	completion := attempts.completions[0]
	if completion.TerminalState != store.WorkAttemptTerminalMergeRevoked || completion.Phase != "merge_revoked" {
		t.Fatalf("work attempt completion = %#v, want merge_revoked terminal state", completion)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("tracker updates = %#v, want operator-selected Blocked state preserved", tracker.updates)
	}
	next, ok, err := dispatchGate.TryAcquire(t.Context(), project, scheduler.SlotRequest{State: "Todo"}, now.Add(2*time.Second))
	if err != nil || !ok {
		t.Fatalf("TryAcquire() after revocation = %#v, %v, want released capacity", next, err)
	}
}

func TestProgrammaticMergeRechecksEligibilityBeforeMerge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	issue := connector.Issue{
		ID:           "issue-final-recheck",
		Identifier:   "digitaldrywood/detent#1434",
		State:        "Merging",
		PRRepository: "digitaldrywood/detent",
		PullRequest: &connector.PullRequest{
			Number:         1435,
			URL:            "https://github.test/digitaldrywood/detent/pull/1435",
			State:          "OPEN",
			MergeableState: "clean",
			CIStatus:       "success",
			HeadSHA:        "revoked-head",
			Labels:         []string{"Ready to Merge"},
		},
	}
	revoked := cloneIssue(issue)
	revoked.PullRequest.Labels = []string{}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{revoked}}
	mergeConnector := &autoPromoteTickMergeConnector{autoPromoteTickConnector: tracker}
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			SourceState: "Human Review",
			Gate: gate.Config{
				Kind:           gate.KindCommand,
				CITriggerLabel: "Ready to Merge",
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := &Orchestrator{
		cfg:                     cfg,
		connector:               mergeConnector,
		workAttempts:            attempts,
		pendingMergeRevocations: map[string]mergeRevocation{},
		logger:                  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:         cloneIssue(issue),
		Attempt:       1,
		WorkAttemptID: 43,
		Mode:          runpkg.RunModeMerge,
		StartedAt:     now.Add(-time.Minute),
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState:  runpkg.FinalStateCompleted,
			Output:      runpkg.RunOutputMergeFastPathClean,
			TurnStarted: true,
		},
		CompletedAt: now,
	})

	if len(mergeConnector.merges) != 0 {
		t.Fatalf("programmatic merges = %#v, want none after eligibility revocation", mergeConnector.merges)
	}
	if len(tracker.updates) != 1 || tracker.updates[0].state != "Human Review" {
		t.Fatalf("tracker updates = %#v, want Human Review demotion", tracker.updates)
	}
	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalMergeRevoked {
		t.Fatalf("work attempt completions = %#v, want merge_revoked", attempts.completions)
	}
}

func TestDraftMergeRevocationUsesConfiguredSourceState(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{AutoPromote: AutoPromoteConfig{SourceState: "Review"}})
	issue := connector.Issue{
		ID:    "issue-custom-review-lane",
		State: "Merging",
		PullRequest: &connector.PullRequest{
			State: "OPEN",
			Draft: true,
		},
	}

	revocation, revoked := mergeRevocationForIssue(issue, cfg, true, false)
	if !revoked {
		t.Fatal("mergeRevocationForIssue() did not revoke a draft pull request")
	}
	if revocation.targetState != "Review" {
		t.Fatalf("target state = %q, want configured source state Review", revocation.targetState)
	}
}

func TestMergeRevocationDoesNotReportMissingPullRequestForOperationalCompletion(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{AutoPromote: AutoPromoteConfig{SourceState: "Human Review"}})
	tests := []struct {
		name                          string
		body                          string
		operationalCompletionAccepted bool
		wantRevoked                   bool
		wantReason                    string
	}{
		{
			name:                          "accepted operational completion",
			body:                          operationalCompletionWorkpadBody("Runner service is healthy and accepting jobs."),
			operationalCompletionAccepted: true,
		},
		{
			name:        "current declaration without accepted attempt",
			body:        operationalCompletionWorkpadBody("Runner service is healthy and accepting jobs."),
			wantRevoked: true,
			wantReason:  mergeRevocationMissingPullRequest,
		},
		{
			name:        "ordinary no diff completion",
			body:        "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
			wantRevoked: true,
			wantReason:  mergeRevocationMissingPullRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := connector.Issue{
				ID:          "issue-operational-merge",
				State:       "Merging",
				Description: operationalCompletionAuthorizationBody(),
				Comments: []connector.IssueComment{{
					Body: tt.body,
				}},
			}
			revocation, revoked := mergeRevocationForIssue(issue, cfg, true, tt.operationalCompletionAccepted)
			if revoked != tt.wantRevoked || revocation.reason != tt.wantReason {
				t.Fatalf("mergeRevocationForIssue() = %#v, %t; want revoked %t reason %q", revocation, revoked, tt.wantRevoked, tt.wantReason)
			}
		})
	}
}

func TestMergeRevocationCommentsDeduplicateReasonAndHeadSHA(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	tracker := &mergeRevocationCommentConnector{now: now}
	orch := &Orchestrator{
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:       func() time.Time { return now },
	}
	revocation := mergeRevocation{
		issue: connector.Issue{
			ID:    "issue-comment-dedup",
			State: "Merging",
			PullRequest: &connector.PullRequest{
				HeadSHA: "same-head",
			},
		},
		reason:      mergeRevocationDraftPullRequest,
		targetState: "In Progress",
	}

	orch.commentMergeRevocation(t.Context(), &State{}, revocation, now)
	orch = &Orchestrator{
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:       func() time.Time { return now },
	}
	for range 19 {
		orch.commentMergeRevocation(t.Context(), &State{}, revocation, now)
	}

	if got := len(tracker.comments); got != 1 {
		t.Fatalf("comments = %d, want 1", got)
	}
	body := tracker.comments[0].Body
	for _, want := range []string{
		"- reason: " + mergeRevocationDraftPullRequest,
		"- head_sha: same-head",
		mergeRevocationCommentSignature(revocation),
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("comment body = %q, want %q", body, want)
		}
	}
}

func TestMergeRevocationCommentBudgetWarnsAndEscalatesOnce(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	tracker := &mergeRevocationCommentConnector{now: now}
	for index := range mergeRevocationCommentLimit {
		createdAt := now.Add(-time.Duration(index) * time.Minute)
		revocation := mergeRevocation{
			issue: connector.Issue{
				ID: "issue-comment-budget",
				PullRequest: &connector.PullRequest{
					HeadSHA: fmt.Sprintf("prior-head-%d", index),
				},
			},
			reason: mergeRevocationDraftPullRequest,
		}
		tracker.comments = append(tracker.comments, connector.IssueComment{
			Body:      mergeRevocationCommentSignature(revocation),
			CreatedAt: &createdAt,
		})
	}
	var logs bytes.Buffer
	orch := &Orchestrator{
		connector: tracker,
		cfg: Config{
			AutoPromote: AutoPromoteConfig{SourceState: "Review"},
		},
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
		now:    func() time.Time { return now },
	}
	revocation := mergeRevocation{
		issue: connector.Issue{
			ID:    "issue-comment-budget",
			State: "Merging",
			PullRequest: &connector.PullRequest{
				HeadSHA: "new-head",
			},
		},
		reason: mergeRevocationDraftPullRequest,
	}

	orch.commentMergeRevocation(t.Context(), &State{}, revocation, now)
	orch.commentMergeRevocation(t.Context(), &State{}, revocation, now.Add(time.Minute))

	if got := len(tracker.comments); got != mergeRevocationCommentLimit {
		t.Fatalf("comments = %d, want budget limit %d", got, mergeRevocationCommentLimit)
	}
	if got, want := tracker.updates, []string{"Review"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if got := strings.Count(logs.String(), "merge revocation comment budget exhausted"); got != 1 {
		t.Fatalf("budget warnings = %d, want 1: %s", got, logs.String())
	}
}

func TestMergeRevocationCommentResourceExhaustionEscalates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	tracker := &mergeRevocationCommentConnector{
		now:            now,
		commentErr:     fmt.Errorf("create github comment: %w", connector.ErrResourceExhausted),
		updateFailures: 1,
	}
	var logs bytes.Buffer
	orch := &Orchestrator{
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
		now:       func() time.Time { return now },
	}
	revocation := mergeRevocation{
		issue: connector.Issue{
			ID:    "issue-comment-cap",
			State: "Merging",
			PullRequest: &connector.PullRequest{
				HeadSHA: "capped-head",
			},
		},
		reason: mergeRevocationDraftPullRequest,
	}

	orch.commentMergeRevocation(t.Context(), &State{}, revocation, now)
	orch.commentMergeRevocation(t.Context(), &State{}, revocation, now.Add(time.Minute))

	if tracker.commentAttempts != 1 {
		t.Fatalf("comment attempts = %d, want 1", tracker.commentAttempts)
	}
	if tracker.updateAttempts != 2 {
		t.Fatalf("update attempts = %d, want 2", tracker.updateAttempts)
	}
	if got, want := tracker.updates, []string{autoPromoteSourceState}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if !strings.Contains(logs.String(), "comment resource exhausted") {
		t.Fatalf("logs = %q, want resource exhaustion", logs.String())
	}
}

type mergeRevocationCommentConnector struct {
	now             time.Time
	comments        []connector.IssueComment
	updates         []string
	commentErr      error
	commentAttempts int
	updateFailures  int
	updateAttempts  int
}

func (c *mergeRevocationCommentConnector) Name() string {
	return "merge-revocation-comment"
}

func (c *mergeRevocationCommentConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *mergeRevocationCommentConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *mergeRevocationCommentConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *mergeRevocationCommentConnector) FetchIssueComments(context.Context, connector.Issue) ([]connector.IssueComment, error) {
	return append([]connector.IssueComment(nil), c.comments...), nil
}

func (c *mergeRevocationCommentConnector) CreateComment(_ context.Context, _ string, body string) error {
	c.commentAttempts++
	if c.commentErr != nil {
		return c.commentErr
	}
	createdAt := c.now
	c.comments = append(c.comments, connector.IssueComment{
		Body:      body,
		CreatedAt: &createdAt,
	})
	return nil
}

func (c *mergeRevocationCommentConnector) UpdateIssueState(_ context.Context, _ string, state string) error {
	c.updateAttempts++
	if c.updateFailures > 0 {
		c.updateFailures--
		return connector.NewRetryableError("transient state update")
	}
	c.updates = append(c.updates, state)
	return nil
}

func (c *mergeRevocationCommentConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *mergeRevocationCommentConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func TestAutoPromoteParksIssueAfterRepeatedIdenticalMergeRevocations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-repeated-draft-revocation", []string{"bug"}, &connector.PullRequest{
		Number:         45,
		URL:            "https://github.test/digitaldrywood/video-studio/pull/45",
		State:          "OPEN",
		Draft:          true,
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.Identifier = "digitaldrywood/video-studio#41"
	prNumber := int64(45)
	attempts := &recordingWorkAttemptStore{}
	for index := range maxIdenticalMergeRevocations {
		attempts.history = append(attempts.history, store.WorkAttempt{
			ID:            int64(maxIdenticalMergeRevocations - index),
			IssueID:       issue.ID,
			Identifier:    issue.Identifier,
			PRNumber:      &prNumber,
			WorkerType:    "agent",
			TerminalState: store.WorkAttemptTerminalMergeRevoked,
			ErrorMessage:  mergeRevocationDraftPullRequest,
			CompletedAt:   now.Add(-time.Duration(index+1) * time.Minute),
		})
	}
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "video-studio"},
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	result := orch.autoPromoteHumanReviewIssues(t.Context(), &state, []connector.Issue{issue}, now)

	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %q parked", result.transitioned, issue.ID)
	}
	if len(result.dispatchCandidates) != 0 {
		t.Fatalf("dispatch candidates = %#v, want none", result.dispatchCandidates)
	}
	if len(tracker.updates) != 1 || tracker.updates[0] != (autoPromoteTickUpdate{issueID: issue.ID, state: blockedStatusState}) {
		t.Fatalf("updates = %#v, want Blocked transition", tracker.updates)
	}
	if len(tracker.comments) != 3 {
		t.Fatalf("comments = %#v, want pending and applied park markers and explanation", tracker.comments)
	}
	for _, fragment := range []string{
		"reason: merge_revocation_limit",
		"revocation_reason: draft_pull_request",
		"consecutive_revocations: 3",
		"human_action:",
	} {
		if !strings.Contains(tracker.comments[2].body, fragment) {
			t.Fatalf("comment = %q, missing %q", tracker.comments[2].body, fragment)
		}
	}
	for _, fragment := range []string{
		"auto promote decision",
		"action=skip",
		"reason=merge_revocation_limit",
		"merge_worker_revocation_limit",
		"revocation_reason=draft_pull_request",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs = %q, missing %q", logs.String(), fragment)
		}
	}
	if len(attempts.historyQueries) != 1 ||
		attempts.historyQueries[0].WorkerType != "" ||
		attempts.historyQueries[0].Limit != maxIdenticalMergeRevocations+1 {
		t.Fatalf("history queries = %#v, want bounded all-worker lookup", attempts.historyQueries)
	}
}

func TestMergeRevocationStreakStopsAtDifferentOutcomeOrReason(t *testing.T) {
	t.Parallel()

	prNumber := int64(45)
	revoked := func(reason string) store.WorkAttempt {
		return store.WorkAttempt{
			PRNumber:      &prNumber,
			TerminalState: store.WorkAttemptTerminalMergeRevoked,
			ErrorMessage:  reason,
		}
	}
	tests := []struct {
		name     string
		attempts []store.WorkAttempt
		want     mergeRevocationStreak
	}{
		{
			name: "identical revocations count consecutively",
			attempts: []store.WorkAttempt{
				revoked(mergeRevocationDraftPullRequest),
				revoked(mergeRevocationDraftPullRequest),
			},
			want: mergeRevocationStreak{reason: mergeRevocationDraftPullRequest, count: 2},
		},
		{
			name: "different reason ends streak",
			attempts: []store.WorkAttempt{
				revoked(mergeRevocationDraftPullRequest),
				revoked(mergeRevocationCITriggerLabelRemoved),
				revoked(mergeRevocationDraftPullRequest),
			},
			want: mergeRevocationStreak{reason: mergeRevocationDraftPullRequest, count: 1},
		},
		{
			name: "successful attempt ends streak",
			attempts: []store.WorkAttempt{
				revoked(mergeRevocationDraftPullRequest),
				{PRNumber: &prNumber, TerminalState: store.WorkAttemptTerminalSuccess},
				revoked(mergeRevocationDraftPullRequest),
			},
			want: mergeRevocationStreak{reason: mergeRevocationDraftPullRequest, count: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mergeRevocationStreakFromAttempts(tt.attempts, int(prNumber))
			if got != tt.want {
				t.Fatalf("mergeRevocationStreakFromAttempts() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
