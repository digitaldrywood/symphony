package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

func TestHandleRunResultClassifiesImplementWorkerProgress(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC)
	signature := autoPromoteReworkSignature{
		PRNumber:     1070,
		HeadSHA:      "same-head",
		FailedChecks: []string{"Test"},
	}

	tests := []struct {
		name               string
		runningIssue       connector.Issue
		hydratedIssue      connector.Issue
		hydrateErr         error
		history            []store.WorkAttempt
		diffStats          DiffStats
		noProgressLimit    int
		wantTerminal       store.WorkAttemptTerminalState
		wantErrorClass     string
		wantReason         string
		wantPreviousHead   string
		wantCurrentHead    string
		wantHydrations     int
		wantBlocked        bool
		wantReview         bool
		wantComment        string
		wantRetry          bool
		wantLogContains    string
		wantEvent          string
		wantFailedAdded    []string
		wantFailedRemoved  []string
		wantConsecutive    int
		wantBlockReason    string
		wantRejectedRef    string
		wantProgressKinds  []string
		wantCompletionKind string
		workpadHumanAction string
		workpadBlockerRef  string
		runningWorkpadBody string
		currentWorkpadBody string
		resolvedBlockers   []connector.Issue
		completionErr      error
		wantClaimed        bool
		refreshedState     string
		pullRequestUpdated bool
	}{
		{
			name:            "first attempt succeeds with linked PR",
			runningIssue:    implementProgressIssue("same-head", "Test"),
			hydratedIssue:   implementProgressIssue("same-head", "Test"),
			diffStats:       DiffStats{Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "first_completed_attempt",
			wantCurrentHead: "same-head",
			wantHydrations:  1,
			wantConsecutive: 1,
			wantReview:      true,
		},
		{
			name:             "new head SHA succeeds",
			runningIssue:     implementProgressIssue("same-head", "Test"),
			hydratedIssue:    implementProgressIssue("new-head", "Test"),
			history:          []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats:        DiffStats{Status: "clean"},
			noProgressLimit:  3,
			wantTerminal:     store.WorkAttemptTerminalSuccess,
			wantReason:       "signature_changed",
			wantPreviousHead: "same-head",
			wantCurrentHead:  "new-head",
			wantHydrations:   1,
			wantReview:       true,
		},
		{
			name:            "stale remote ref with pushed pull request head is not stranded",
			runningIssue:    implementProgressIssue("same-head", "Test"),
			hydratedIssue:   implementProgressIssue("same-head", "Test"),
			diffStats:       DiffStats{UnpushedCommits: 1, HeadSHA: "same-head", Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "first_completed_attempt",
			wantCurrentHead: "same-head",
			wantHydrations:  1,
			wantConsecutive: 0,
			wantReview:      true,
		},
		{
			name:          "hydrated pull request head supersedes stale commit comparison",
			runningIssue:  implementProgressIssue("dispatch-head", "Test"),
			hydratedIssue: implementProgressIssue("pushed-head", "Test"),
			diffStats: DiffStats{
				UnpushedCommits:                1,
				UnpushedCommitRefs:             []string{"abc123 fix: pushed work"},
				CommitsNotInPullRequest:        []string{"abc123 fix: pushed work"},
				PullRequestComparisonAvailable: true,
				HeadSHA:                        "pushed-head",
				Status:                         "clean",
			},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "first_completed_attempt",
			wantCurrentHead: "pushed-head",
			wantHydrations:  1,
			wantConsecutive: 0,
			wantReview:      true,
		},
		{
			name:          "new pull request head with newer unpushed commit remains stranded",
			runningIssue:  implementProgressIssue("same-head", "Test"),
			hydratedIssue: implementProgressIssue("new-head", "Test"),
			history:       []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats: DiffStats{
				UnpushedCommits:                1,
				CommitsNotInPullRequest:        []string{"abc123 fix: preserve work"},
				PullRequestComparisonAvailable: true,
				HeadSHA:                        "workspace-head",
				Status:                         "clean",
			},
			noProgressLimit:  3,
			wantTerminal:     store.WorkAttemptTerminalNoProgress,
			wantReason:       strandedUnpushedWorkReason,
			wantConsecutive:  1,
			wantPreviousHead: "same-head",
			wantCurrentHead:  "new-head",
			wantHydrations:   1,
			wantRetry:        true,
		},
		{
			name:             "unchanged signature and clean diff records no progress",
			runningIssue:     implementProgressIssue("same-head", "Test"),
			hydratedIssue:    implementProgressIssue("same-head", "Test"),
			history:          []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats:        DiffStats{Status: "clean"},
			noProgressLimit:  3,
			wantTerminal:     store.WorkAttemptTerminalNoProgress,
			wantReason:       "unchanged_signature_clean_diff",
			wantConsecutive:  2,
			wantPreviousHead: "same-head",
			wantCurrentHead:  "same-head",
			wantHydrations:   1,
			wantRetry:        true,
		},
		{
			name:          "stranded unpushed work on existing pull request is distinct",
			runningIssue:  implementProgressIssue("same-head", "Test"),
			hydratedIssue: implementProgressIssue("same-head", "Test"),
			history: []store.WorkAttempt{
				implementProgressStrandedHistoryAttempt(2, signature),
				implementProgressStrandedHistoryAttempt(1, signature),
			},
			diffStats: DiffStats{
				UnpushedCommits:                1,
				UnpushedCommitRefs:             []string{"abc123 fix: preserve work"},
				CommitsNotInPullRequest:        []string{"abc123 fix: preserve work"},
				PullRequestComparisonAvailable: true,
				HeadSHA:                        "workspace-head",
				Status:                         "clean",
			},
			noProgressLimit:  3,
			wantTerminal:     store.WorkAttemptTerminalNoProgress,
			wantReason:       strandedUnpushedWorkReason,
			wantConsecutive:  3,
			wantPreviousHead: "same-head",
			wantCurrentHead:  "same-head",
			wantHydrations:   1,
			wantBlocked:      true,
			wantBlockReason:  strandedUnpushedWorkReason,
			wantComment:      "commits_not_in_pull_request: \"abc123 fix: preserve work\"",
		},
		{
			name:          "untracked file with matching pull request head is not stranded",
			runningIssue:  implementProgressIssue("same-head", "Test"),
			hydratedIssue: implementProgressIssue("same-head", "Test"),
			diffStats: DiffStats{
				FilesChanged:                   1,
				AddedLines:                     1,
				UnpushedCommits:                1,
				PullRequestComparisonAvailable: true,
				HeadSHA:                        "same-head",
				Status:                         "changed",
			},
			noProgressLimit: 1,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "first_completed_attempt",
			wantConsecutive: 0,
			wantCurrentHead: "same-head",
			wantHydrations:  1,
			wantReview:      true,
		},
		{
			name:          "tracked file with matching pull request head remains stranded",
			runningIssue:  implementProgressIssue("same-head", "Test"),
			hydratedIssue: implementProgressIssue("same-head", "Test"),
			diffStats: DiffStats{
				FilesChanged:                   1,
				AddedLines:                     1,
				TrackedPaths:                   []string{"tracked.go"},
				PullRequestComparisonAvailable: true,
				HeadSHA:                        "same-head",
				Status:                         "changed",
			},
			noProgressLimit: 1,
			wantTerminal:    store.WorkAttemptTerminalNoProgress,
			wantReason:      strandedUnpushedWorkReason,
			wantConsecutive: 1,
			wantCurrentHead: "same-head",
			wantHydrations:  1,
			wantBlocked:     true,
			wantBlockReason: strandedUnpushedWorkReason,
			wantComment:     "tracked_paths: \"tracked.go\"",
		},
		{
			name:            "missing workspace head defers unpushed classification",
			runningIssue:    implementProgressIssue("same-head", "Test"),
			hydratedIssue:   implementProgressIssue("same-head", "Test"),
			diffStats:       DiffStats{UnpushedCommits: 1, Status: "clean"},
			noProgressLimit: 1,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "workspace_head_unavailable_for_unpushed_check",
			wantCurrentHead: "same-head",
			wantHydrations:  1,
			wantReview:      true,
		},
		{
			name:          "limit trip blocks with comment",
			runningIssue:  implementProgressIssue("same-head", "Test"),
			hydratedIssue: implementProgressIssue("same-head", "Test"),
			history: []store.WorkAttempt{
				implementProgressHistoryAttempt(2, signature, store.WorkAttemptTerminalNoProgress),
				implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalNoProgress),
			},
			diffStats:        DiffStats{Status: "clean"},
			noProgressLimit:  3,
			wantTerminal:     store.WorkAttemptTerminalNoProgress,
			wantReason:       dispatchLoopDetectedReason,
			wantConsecutive:  3,
			wantPreviousHead: "same-head",
			wantCurrentHead:  "same-head",
			wantHydrations:   1,
			wantBlocked:      true,
			wantBlockReason:  dispatchLoopDetectedReason,
			wantComment:      "loop detected after 3 dispatches",
		},
		{
			name:               "unverified pull request update counts as no progress",
			runningIssue:       implementProgressIssueWithoutPR(),
			diffStats:          DiffStats{Status: "clean"},
			noProgressLimit:    3,
			pullRequestUpdated: true,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantReason:         "pull_request_created_or_updated",
			wantConsecutive:    1,
			wantRetry:          true,
		},
		{
			name:            "first clean completion without linked PR records no progress",
			runningIssue:    implementProgressIssueWithoutPR(),
			diffStats:       DiffStats{Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalNoProgress,
			wantReason:      "completed_clean_diff_without_pull_request",
			wantConsecutive: 1,
			wantRetry:       true,
		},
		{
			name: "preauthorized operational completion is deliverable progress",
			runningIssue: func() connector.Issue {
				issue := implementProgressIssueWithoutPR()
				issue.Description = operationalCompletionAuthorizationBody()
				return issue
			}(),
			diffStats:          DiffStats{Status: "clean"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantReason:         string(AutoPromoteReasonOperationalCompletion),
			wantProgressKinds:  []string{"operational_completion"},
			wantCompletionKind: workpad.CompletionOperational,
			wantReview:         true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: operationalCompletionWorkpadBody("Backfill completed and verified."),
		},
		{
			name: "operational completion with undelivered commits is stranded",
			runningIssue: func() connector.Issue {
				issue := implementProgressIssueWithoutPR()
				issue.Description = operationalCompletionAuthorizationBody()
				return issue
			}(),
			diffStats:          DiffStats{UnpushedCommits: 1, CommitsAhead: 1, Status: "clean"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         strandedUnpushedWorkReason,
			wantConsecutive:    1,
			wantRetry:          true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: operationalCompletionWorkpadBody("Backfill completed and verified."),
		},
		{
			name:               "undeclared operational assertion remains no progress",
			runningIssue:       implementProgressIssueWithoutPR(),
			diffStats:          DiffStats{Status: "clean"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         "completed_clean_diff_without_pull_request",
			wantConsecutive:    1,
			wantRetry:          true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: operationalCompletionWorkpadBody("Backfill completed and verified."),
		},
		{
			name:              "first dependency deferral releases claim without tripping loop",
			runningIssue:      implementProgressIssueWithoutPR(),
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalSuccess,
			wantReason:        implementDependencyDeferralReason,
			workpadBlockerRef: "digitaldrywood/detent#134",
			resolvedBlockers:  []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: "Todo"}},
			wantProgressKinds: nil,
		},
		{
			name:         "third same lane dependency deferral remains deferred",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressDependencyDeferralHistoryAttempt(2, "digitaldrywood/detent#134", "Todo"),
				implementProgressDependencyDeferralHistoryAttempt(1, "digitaldrywood/detent#134", "Todo"),
			},
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalSuccess,
			wantReason:        implementDependencyDeferralReason,
			workpadBlockerRef: "digitaldrywood/detent#134",
			resolvedBlockers:  []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: "Todo"}},
		},
		{
			name:              "dependency deferral persistence failure retains claim",
			runningIssue:      implementProgressIssueWithoutPR(),
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalSuccess,
			wantReason:        "dependency_deferral",
			workpadBlockerRef: "digitaldrywood/detent#134",
			resolvedBlockers:  []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: "Todo"}},
			completionErr:     errors.New("attempt store unavailable"),
			wantClaimed:       true,
			wantLogContains:   "complete work attempt failed",
		},
		{
			name:              "malformed blocker ref counts as no progress",
			runningIssue:      implementProgressIssueWithoutPR(),
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalNoProgress,
			wantReason:        "completed_clean_diff_without_pull_request",
			wantConsecutive:   1,
			workpadBlockerRef: "fabricated-ref",
			wantRejectedRef:   "fabricated-ref",
			wantLogContains:   "fabricated-ref",
			wantRetry:         true,
		},
		{
			name:              "unresolvable blocker ref counts as no progress",
			runningIssue:      implementProgressIssueWithoutPR(),
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalNoProgress,
			wantReason:        "completed_clean_diff_without_pull_request",
			wantConsecutive:   1,
			workpadBlockerRef: "digitaldrywood/detent#9999",
			wantRejectedRef:   "digitaldrywood/detent#9999",
			wantLogContains:   "digitaldrywood/detent#9999",
			wantRetry:         true,
		},
		{
			name:              "already terminal blocker does not defer empty attempt",
			runningIssue:      implementProgressIssueWithoutPR(),
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalNoProgress,
			wantReason:        "completed_clean_diff_without_pull_request",
			wantConsecutive:   1,
			workpadBlockerRef: "digitaldrywood/detent#134",
			resolvedBlockers:  []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: "Done"}},
			wantRetry:         true,
		},
		{
			name:         "legacy telemetry replay fails open without dispatch start evidence",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressLegacyNoPRHistoryAttempt(2),
				implementProgressLegacyNoPRHistoryAttempt(1),
			},
			diffStats:       DiffStats{Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalNoProgress,
			wantReason:      "completed_clean_diff_without_pull_request",
			wantConsecutive: 0,
			wantRetry:       true,
		},
		{
			name:         "identical non-empty diff trips third completion without linked PR",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(2, DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"}, "", ""),
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"}, "", ""),
			},
			diffStats:       DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalNoProgress,
			wantReason:      dispatchLoopDetectedReason,
			wantConsecutive: 3,
			wantBlocked:     true,
			wantBlockReason: dispatchLoopDetectedReason,
			wantComment:     "consecutive_no_progress_attempts: 3",
		},
		{
			name:         "audit field does not reset identical non-empty diff loop",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(2, DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"}, "", ""),
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"}, "", ""),
			},
			diffStats:          DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "same-diff", Status: "changed"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         dispatchLoopDetectedReason,
			wantProgressKinds:  []string{"audit_artifact"},
			wantConsecutive:    3,
			wantBlocked:        true,
			wantBlockReason:    dispatchLoopDetectedReason,
			wantComment:        "loop detected after 3 dispatches",
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", map[string]string{"duplicate_active_email_groups": "23"}),
		},
		{
			name:         "unpushed arithmetic without linked pull request defers",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(2, DiffStats{UnpushedCommits: 1, Status: "clean"}, "", ""),
				implementProgressNoPRHistoryAttempt(1, DiffStats{UnpushedCommits: 1, Status: "clean"}, "", ""),
			},
			diffStats:       DiffStats{UnpushedCommits: 1, Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "unpushed_remote_truth_unavailable_without_pull_request",
			wantRetry:       true,
		},
		{
			name:         "changing non-empty diff does not trip without linked PR",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(2, DiffStats{FilesChanged: 9, AddedLines: 120, RemovedLines: 30, Fingerprint: "second-diff", Status: "changed"}, "", ""),
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 8, AddedLines: 119, RemovedLines: 30, Fingerprint: "first-diff", Status: "changed"}, "", ""),
			},
			diffStats:         DiffStats{FilesChanged: 10, AddedLines: 121, RemovedLines: 30, Fingerprint: "third-diff", Status: "changed"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalSuccess,
			wantReason:        "workspace_diff_present_without_pull_request",
			wantProgressKinds: []string{"workspace_diff"},
			wantRetry:         true,
		},
		{
			name:               "verifiable non-diff audit field remains in no-progress streak",
			runningIssue:       implementProgressIssueWithoutPR(),
			history:            []store.WorkAttempt{implementProgressLegacyNoPRHistoryAttempt(1)},
			diffStats:          DiffStats{Status: "clean"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantReason:         "verifiable_non_diff_progress",
			wantProgressKinds:  []string{"audit_artifact"},
			wantConsecutive:    0,
			wantRetry:          true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", map[string]string{"duplicate_active_email_groups": "23"}),
		},
		{
			name:               "prose-only workpad update counts as no progress",
			runningIssue:       implementProgressIssueWithoutPR(),
			history:            []store.WorkAttempt{implementProgressLegacyNoPRHistoryAttempt(1)},
			diffStats:          DiffStats{Status: "clean"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         "completed_clean_diff_without_pull_request",
			wantConsecutive:    0,
			wantRetry:          true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "baseline prose", nil),
			currentWorkpadBody: implementProgressStructuredWorkpad("in_progress", "expanded prose without a machine artifact", nil),
		},
		{
			name:               "mixed diff and audit field records both progress kinds",
			runningIssue:       implementProgressIssueWithoutPR(),
			diffStats:          DiffStats{FilesChanged: 1, AddedLines: 4, Fingerprint: "new-diff", Status: "changed"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantReason:         "workspace_diff_and_verifiable_non_diff_progress",
			wantProgressKinds:  []string{"workspace_diff", "audit_artifact"},
			wantRetry:          true,
			runningWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", nil),
			currentWorkpadBody: implementProgressStructuredWorkpad("in_progress", "", map[string]string{"duplicate_active_email_groups": "23"}),
		},
		{
			name:         "repeated blocked human action trips despite diff noise",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 1, AddedLines: 1, Status: "changed"}, "Choose the exhaustive review path.", "In Progress"),
			},
			diffStats:          DiffStats{FilesChanged: 12, AddedLines: 240, RemovedLines: 17, Status: "changed"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         "workpad_blocked_unactioned",
			wantBlocked:        true,
			wantBlockReason:    "workpad_blocked_unactioned",
			wantComment:        "> Choose the exhaustive review path.",
			workpadHumanAction: "Choose the exhaustive review path.",
			workpadBlockerRef:  "digitaldrywood/detent#134",
		},
		{
			name:         "changing blocked human action does not hide dispatch loop",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(2, DiffStats{FilesChanged: 2, AddedLines: 2, Fingerprint: "same-diff", Status: "changed"}, "Choose the old review path.", "In Progress"),
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 2, AddedLines: 2, Fingerprint: "same-diff", Status: "changed"}, "Choose the old review path.", "In Progress"),
			},
			diffStats:          DiffStats{FilesChanged: 2, AddedLines: 2, Fingerprint: "same-diff", Status: "changed"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalNoProgress,
			wantReason:         dispatchLoopDetectedReason,
			wantConsecutive:    3,
			wantBlocked:        true,
			wantBlockReason:    dispatchLoopDetectedReason,
			wantComment:        "loop detected after 3 dispatches",
			workpadHumanAction: "Choose the new review path.",
		},
		{
			name:         "tracker state change resets repeated blocked human action",
			runningIssue: implementProgressIssueWithoutPR(),
			history: []store.WorkAttempt{
				implementProgressNoPRHistoryAttempt(1, DiffStats{FilesChanged: 1, AddedLines: 1, Fingerprint: "old-diff", Status: "changed"}, "Choose the review path.", "In Progress"),
			},
			diffStats:          DiffStats{FilesChanged: 2, AddedLines: 2, Fingerprint: "new-diff", Status: "changed"},
			noProgressLimit:    3,
			wantTerminal:       store.WorkAttemptTerminalSuccess,
			wantReason:         implementProgressReasonMixed,
			wantProgressKinds:  []string{"workspace_diff", "tracker_state_transition"},
			wantRetry:          true,
			workpadHumanAction: "Choose the review path.",
			refreshedState:     "Rework",
		},
		{
			name:            "hydration failure fails open to success",
			runningIssue:    implementProgressIssue("same-head", "Test"),
			hydratedIssue:   implementProgressIssue("same-head", "Test"),
			hydrateErr:      errors.New("github hiccup"),
			history:         []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats:       DiffStats{Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "pull_request_hydration_failed",
			wantHydrations:  1,
			wantReview:      true,
			wantLogContains: "implement worker progress check failed open",
		},
		{
			name:            "degraded hydration fails open to success",
			runningIssue:    implementProgressIssue("same-head", "Test"),
			hydratedIssue:   implementProgressIssueWithHydrationDegraded("same-head", connector.PullRequestHydrationReasonStaleCachedPullData, "Test"),
			history:         []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats:       DiffStats{UnpushedCommits: 1, HeadSHA: "same-head", Status: "clean"},
			noProgressLimit: 3,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantReason:      "pull_request_hydration_unavailable",
			wantHydrations:  1,
			wantReview:      true,
			wantLogContains: "implement worker progress check failed open",
		},
		{
			name:              "failed check delta is recorded",
			runningIssue:      implementProgressIssue("new-head", "Test", "Lint"),
			hydratedIssue:     implementProgressIssue("new-head", "Test", "Lint"),
			history:           []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			diffStats:         DiffStats{Status: "clean"},
			noProgressLimit:   3,
			wantTerminal:      store.WorkAttemptTerminalSuccess,
			wantReason:        "signature_changed",
			wantPreviousHead:  "same-head",
			wantCurrentHead:   "new-head",
			wantHydrations:    1,
			wantReview:        true,
			wantFailedAdded:   []string{"Lint"},
			wantFailedRemoved: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diffStatsPresent(tt.diffStats) && tt.wantReason != workspaceHeadUnavailableReason && strings.TrimSpace(tt.diffStats.HeadSHA) == "" {
				tt.diffStats.HeadSHA = "same-workspace-head"
			}
			dispatchStartDiff := tt.diffStats
			if slices.Contains(tt.wantProgressKinds, "workspace_diff") {
				dispatchStartDiff.FilesChanged = 0
				dispatchStartDiff.AddedLines = 0
				dispatchStartDiff.RemovedLines = 0
				dispatchStartDiff.UnpushedCommits = 0
				dispatchStartDiff.Fingerprint = ""
				dispatchStartDiff.Status = "clean"
			}

			if tt.runningWorkpadBody != "" {
				tt.runningIssue.Comments = []connector.IssueComment{{Body: tt.runningWorkpadBody, URL: "https://github.test/workpad"}}
			}
			var logs bytes.Buffer
			refreshed := tt.runningIssue
			if tt.refreshedState != "" {
				refreshed.State = tt.refreshedState
			}
			if tt.currentWorkpadBody != "" {
				refreshed.Comments = []connector.IssueComment{{Body: tt.currentWorkpadBody, URL: "https://github.test/workpad"}}
			} else if tt.workpadHumanAction != "" || tt.workpadBlockerRef != "" {
				refreshed.Comments = []connector.IssueComment{{
					Body: implementProgressWorkpadComment(tt.workpadBlockerRef, tt.workpadHumanAction),
					URL:  "https://github.test/workpad",
				}}
			}
			tracker := &implementProgressConnector{
				hydrated:         tt.hydratedIssue,
				refreshed:        refreshed,
				hydrateErr:       tt.hydrateErr,
				resolvedBlockers: tt.resolvedBlockers,
			}
			attempts := &implementProgressAttemptStore{history: tt.history, completionErr: tt.completionErr}
			cfg := normalizeConfig(Config{
				Project:                scheduler.ProjectCandidate{ID: "detent"},
				AutoPromote:            AutoPromoteConfig{NoProgressLimit: tt.noProgressLimit},
				ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates:         []string{"Human Review", "Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
				ContinuationRetryDelay: time.Minute,
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				logger:       slog.New(slog.NewTextHandler(&logs, nil)),
			}
			state := newState(cfg)
			running := Running{
				Issue:            tt.runningIssue,
				Attempt:          1,
				WorkAttemptID:    42,
				Mode:             runpkg.RunModeImplement,
				StartedAt:        base.Add(-time.Minute),
				DiffStats:        tt.diffStats,
				DispatchProgress: implementProgressArtifactSnapshotFromIssue(tt.runningIssue, true),
				DispatchLoopStart: dispatchLoopTestStart(
					tt.runningIssue.State,
					autoPromoteReworkSignatureFromIssue(tt.runningIssue, AutoPromoteSummaryFromIssue(tt.runningIssue)),
					implementProgressDiffStatsFromDiffStats(dispatchStartDiff),
				),
			}
			state.Running[tt.runningIssue.ID] = running
			state.Claimed[tt.runningIssue.ID] = Claimed{Issue: tt.runningIssue, ClaimedAt: running.StartedAt}

			orch.handleRunResult(context.Background(), &state, runpkg.Completion{
				IssueID:     tt.runningIssue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result: runpkg.RunResult{
					FinalState:         FinalStateCompleted,
					DiffStats:          tt.diffStats,
					PullRequestUpdated: tt.pullRequestUpdated,
				},
			})

			if len(attempts.completions) != 1 {
				t.Fatalf("completions len = %d, want 1", len(attempts.completions))
			}
			completion := attempts.completions[0]
			if completion.TerminalState != tt.wantTerminal {
				t.Fatalf("TerminalState = %q, want %q", completion.TerminalState, tt.wantTerminal)
			}
			if tt.wantErrorClass != "" && completion.ErrorClass != tt.wantErrorClass {
				t.Fatalf("ErrorClass = %q, want %q", completion.ErrorClass, tt.wantErrorClass)
			}
			record := implementProgressRecordFromCompletion(t, completion)
			if record.Reason != tt.wantReason {
				t.Fatalf("metadata reason = %q, want %q", record.Reason, tt.wantReason)
			}
			if record.PreviousHeadSHA != tt.wantPreviousHead {
				t.Fatalf("previous head = %q, want %q", record.PreviousHeadSHA, tt.wantPreviousHead)
			}
			if record.CurrentHeadSHA != tt.wantCurrentHead {
				t.Fatalf("current head = %q, want %q", record.CurrentHeadSHA, tt.wantCurrentHead)
			}
			if record.ConsecutiveNoProgress != tt.wantConsecutive {
				t.Fatalf("consecutive no progress = %d, want %d", record.ConsecutiveNoProgress, tt.wantConsecutive)
			}
			if record.BlockReason != tt.wantBlockReason {
				t.Fatalf("block reason = %q, want %q", record.BlockReason, tt.wantBlockReason)
			}
			if record.CompletionKind != tt.wantCompletionKind {
				t.Fatalf("completion kind = %q, want %q", record.CompletionKind, tt.wantCompletionKind)
			}
			if tt.wantProgressKinds != nil && !reflect.DeepEqual(record.ProgressKinds, tt.wantProgressKinds) {
				t.Fatalf("progress kinds = %#v, want %#v", record.ProgressKinds, tt.wantProgressKinds)
			}
			if tt.wantRejectedRef != "" && !strings.Contains(strings.Join(record.RejectedBlockerRefs, ","), tt.wantRejectedRef) {
				t.Fatalf("rejected blocker refs = %#v, want %q", record.RejectedBlockerRefs, tt.wantRejectedRef)
			}
			if !slicesEqual(record.FailedChecksAdded, tt.wantFailedAdded) {
				t.Fatalf("failed checks added = %#v, want %#v", record.FailedChecksAdded, tt.wantFailedAdded)
			}
			if !slicesEqual(record.FailedChecksRemoved, tt.wantFailedRemoved) {
				t.Fatalf("failed checks removed = %#v, want %#v", record.FailedChecksRemoved, tt.wantFailedRemoved)
			}
			if tracker.hydrations != tt.wantHydrations {
				t.Fatalf("hydrations = %d, want %d", tracker.hydrations, tt.wantHydrations)
			}
			if _, ok := state.Blocked[tt.runningIssue.ID]; ok != tt.wantBlocked {
				t.Fatalf("blocked present = %v, want %v", ok, tt.wantBlocked)
			}
			if tt.wantBlocked {
				if len(tracker.updates) != 1 || tracker.updates[0].state != blockedStatusState {
					t.Fatalf("updates = %#v, want one Blocked update", tracker.updates)
				}
				blocked := state.Blocked[tt.runningIssue.ID]
				if tt.wantBlockReason == noProgressLimitReason || tt.wantBlockReason == dispatchLoopDetectedReason {
					if blocked.Recovery == nil || blocked.Recovery.Owner != blockedRecoveryOwnerHuman || blocked.Recovery.Predicate != blockedRecoveryPredicateManaged {
						t.Fatalf("Blocked[%q].Recovery = %#v, want durable human acknowledgement", tt.runningIssue.ID, blocked.Recovery)
					}
				}
				wantComments := 1
				if tt.wantBlockReason == noProgressLimitReason || tt.wantBlockReason == dispatchLoopDetectedReason || tt.wantBlockReason == "workpad_blocked_unactioned" {
					wantComments = 3
				}
				if len(tracker.comments) != wantComments || !strings.Contains(tracker.comments[wantComments-1].body, tt.wantComment) {
					t.Fatalf("comments = %#v, want comment containing %q", tracker.comments, tt.wantComment)
				}
				if _, ok := state.Retry[tt.runningIssue.ID]; ok {
					t.Fatalf("Retry[%q] present after block", tt.runningIssue.ID)
				}
			} else if tt.wantReview {
				if len(tracker.updates) != 1 || tracker.updates[0].state != autoPromoteSourceState {
					t.Fatalf("updates = %#v, want one Human Review update", tracker.updates)
				}
				if _, ok := state.Retry[tt.runningIssue.ID]; ok {
					t.Fatalf("Retry[%q] present after review transition", tt.runningIssue.ID)
				}
			} else if _, ok := state.Retry[tt.runningIssue.ID]; ok != tt.wantRetry {
				t.Fatalf("retry present = %v, want %v", ok, tt.wantRetry)
			}
			if _, ok := state.Claimed[tt.runningIssue.ID]; ok != tt.wantClaimed {
				t.Fatalf("claimed present = %v, want %v", ok, tt.wantClaimed)
			}
			if tt.wantLogContains != "" && !strings.Contains(logs.String(), tt.wantLogContains) {
				t.Fatalf("logs did not contain %q:\n%s", tt.wantLogContains, logs.String())
			}
			if tt.wantEvent != "" {
				found := false
				for _, event := range state.RecentEvents {
					found = found || event.Event == tt.wantEvent
				}
				if !found {
					t.Fatalf("RecentEvents = %#v, want %q", state.RecentEvents, tt.wantEvent)
				}
			}
		})
	}
}

func TestHandleRunResultResetsFailureBreakersOnlyForDurableWorkProduct(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		issue          connector.Issue
		hydrated       connector.Issue
		refreshedState string
		completionLane string
		wantReset      bool
	}{
		{
			name:  "yieldless completion preserves failures",
			issue: implementProgressIssueWithoutPR(),
		},
		{
			name:      "linked pull request clears failures",
			issue:     implementProgressIssue("head", "Test"),
			hydrated:  implementProgressIssue("head", "Test"),
			wantReset: true,
		},
		{
			name:           "current attempt completion lane clears failures",
			issue:          implementProgressIssueWithoutPR(),
			completionLane: "Human Review",
			wantReset:      true,
		},
		{
			name:           "tracker state transition clears failures",
			issue:          implementProgressIssueWithoutPR(),
			refreshedState: "Rework",
			wantReset:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			refreshed := cloneIssue(tt.issue)
			if tt.refreshedState != "" {
				refreshed.State = tt.refreshedState
			}
			tracker := &implementProgressConnector{refreshed: refreshed, hydrated: tt.hydrated}
			attempts := &implementProgressAttemptStore{}
			cfg := normalizeConfig(Config{
				Project:                scheduler.ProjectCandidate{ID: "detent"},
				AutoPromote:            AutoPromoteConfig{NoProgressLimit: 3},
				ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates:         []string{"Human Review", "Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
				ContinuationRetryDelay: time.Minute,
			})
			orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)
			state.Running[tt.issue.ID] = Running{
				Issue:            tt.issue,
				Attempt:          3,
				WorkAttemptID:    42,
				Mode:             runpkg.RunModeImplement,
				StartedAt:        base.Add(-time.Minute),
				DiffStats:        DiffStats{Status: "clean"},
				DispatchProgress: implementProgressArtifactSnapshotFromIssue(tt.issue, true),
				CompletionLane:   tt.completionLane,
			}
			state.Claimed[tt.issue.ID] = Claimed{Issue: tt.issue, ClaimedAt: base.Add(-time.Minute)}
			state.InstantFailures[tt.issue.ID] = InstantFailure{Issue: tt.issue, Count: 2}
			state.RepeatedFailures[tt.issue.ID] = RepeatedFailure{Issue: tt.issue, Count: 2}

			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID:     tt.issue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result:      runpkg.RunResult{FinalState: FinalStateCompleted, DiffStats: DiffStats{Status: "clean"}},
			})

			_, instantPresent := state.InstantFailures[tt.issue.ID]
			_, repeatedPresent := state.RepeatedFailures[tt.issue.ID]
			if tt.wantReset && (instantPresent || repeatedPresent) {
				t.Fatalf("failure breakers remain after durable work product: instant=%t repeated=%t", instantPresent, repeatedPresent)
			}
			if !tt.wantReset && (!instantPresent || !repeatedPresent) {
				t.Fatalf("failure breakers cleared after yieldless completion: instant=%t repeated=%t", instantPresent, repeatedPresent)
			}
		})
	}
}

func TestRunnerWorkAttemptErrorClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "generic runner failure", err: errors.New("runner failed"), want: workAttemptErrorRunner},
		{name: "post-push command failure", err: &runpkg.DeliverableCommandError{OperationClass: "post_push"}, want: workAttemptErrorPostPushCommand},
		{name: "interrupted backend turn", err: backendStatusTestError{status: "interrupted"}, want: workAttemptErrorInterrupted},
		{name: "failed backend turn", err: backendStatusTestError{status: "failed"}, want: workAttemptErrorRunner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := runnerWorkAttemptErrorClass(tt.err); got != tt.want {
				t.Fatalf("runnerWorkAttemptErrorClass() = %q, want %q", got, tt.want)
			}
		})
	}
}

type backendStatusTestError struct {
	status string
}

func (e backendStatusTestError) Error() string {
	return "backend turn " + e.status
}

func (e backendStatusTestError) BackendErrorStatus() string {
	return e.status
}

func TestHandleRunResultAcceptsMergedNoDiffCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 8, 20, 40, 30, 0, time.UTC)
	runningIssue := implementProgressIssueWithoutPR()
	runningIssue.ID = "issue-1711"
	runningIssue.Identifier = "digitaldrywood/detent#1711"
	runningIssue.URL = "https://github.test/digitaldrywood/detent/issues/1711"
	prNumber := 1708
	refreshedIssue := cloneIssue(runningIssue)
	refreshedIssue.PRNumber = &prNumber
	refreshedIssue.PRRepository = "digitaldrywood/detent"
	refreshedIssue.Comments = []connector.IssueComment{{
		Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
		URL:  "https://github.test/digitaldrywood/detent/issues/1711#workpad",
	}}
	hydratedIssue := cloneIssue(refreshedIssue)
	hydratedIssue.PullRequest = &connector.PullRequest{
		Number:        prNumber,
		URL:           "https://github.test/digitaldrywood/detent/pull/1708",
		State:         "MERGED",
		HeadSHA:       "b2c2639ba9d8dbaddda1dc6adc5fc7b77c0d2b1d",
		CIStatus:      "success",
		CheckRunCount: 2,
	}
	tracker := &implementProgressConnector{refreshed: refreshedIssue, hydrated: hydratedIssue}
	attempts := &implementProgressAttemptStore{history: []store.WorkAttempt{
		implementProgressLegacyNoPRHistoryAttempt(3),
		implementProgressLegacyNoPRHistoryAttempt(2),
		implementProgressLegacyNoPRHistoryAttempt(1),
	}}
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote: AutoPromoteConfig{
			Enabled:         true,
			NoProgressLimit: 3,
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Minute,
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	running := Running{
		Issue:         runningIssue,
		Attempt:       4,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		DiffStats:     DiffStats{Status: "clean"},
	}
	state.Running[runningIssue.ID] = running
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: running.StartedAt}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState: FinalStateCompleted,
			DiffStats:  runpkg.DiffStats{Status: "clean"},
		},
	})

	if len(attempts.completions) != 1 {
		t.Fatalf("completions = %#v, want one", attempts.completions)
	}
	completion := attempts.completions[0]
	if completion.TerminalState != store.WorkAttemptTerminalSuccess {
		t.Fatalf("TerminalState = %q, want success", completion.TerminalState)
	}
	record := implementProgressRecordFromCompletion(t, completion)
	if record.Reason != implementMergedCompletionReason {
		t.Fatalf("Reason = %q, want %s", record.Reason, implementMergedCompletionReason)
	}
	if record.ConsecutiveNoProgress != 0 {
		t.Fatalf("ConsecutiveNoProgress = %d, want 0", record.ConsecutiveNoProgress)
	}
	if _, blocked := state.Blocked[runningIssue.ID]; blocked {
		t.Fatalf("Blocked[%q] present after accepted merged completion", runningIssue.ID)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no completion-time Blocked transition", tracker.updates)
	}
	if tracker.hydrations != 1 {
		t.Fatalf("hydrations = %d, want tracker-discovered PR hydrated once", tracker.hydrations)
	}
	completed := state.Completed[runningIssue.ID]
	if completed.Issue.PullRequest == nil || completed.Issue.PullRequest.State != "MERGED" {
		t.Fatalf("completed issue pull request = %#v, want refreshed merged PR", completed.Issue.PullRequest)
	}

	transitioned := orch.reconcileStaleLinkedPullRequestIssues(
		context.Background(),
		&state,
		[]connector.Issue{hydratedIssue},
		now.Add(time.Minute),
	)
	if _, ok := transitioned[runningIssue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want issue %q", transitioned, runningIssue.ID)
	}
	if len(tracker.updates) != 1 || tracker.updates[0] != (implementProgressUpdate{issueID: runningIssue.ID, state: "Done"}) {
		t.Fatalf("updates = %#v, want Done reconciliation", tracker.updates)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "reason: pull_request_merged") {
		t.Fatalf("comments = %#v, want merged PR reconciliation", tracker.comments)
	}
}

func TestHandleRunResultStopsCompletedGateWaitContinuations(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 9, 18, 50, 0, 0, time.UTC)
	issue := implementProgressIssue("same-head", "Test")
	signature := autoPromoteReworkSignature{
		PRNumber:     1070,
		HeadSHA:      "same-head",
		FailedChecks: []string{"Test"},
	}
	tests := []struct {
		name           string
		history        []store.WorkAttempt
		wantTerminal   store.WorkAttemptTerminalState
		wantHydrations int
	}{
		{
			name:           "initial success waits for gate without continuation",
			wantTerminal:   store.WorkAttemptTerminalSuccess,
			wantHydrations: 1,
		},
		{
			name:         "redundant dispatch is superseded without breaker strike",
			history:      []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			wantTerminal: store.WorkAttemptTerminalSuperseded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracker := &implementProgressConnector{hydrated: issue}
			attempts := &implementProgressAttemptStore{history: tt.history}
			cfg := normalizeConfig(Config{
				Project: scheduler.ProjectCandidate{ID: "detent"},
				AutoPromote: AutoPromoteConfig{
					Enabled:         true,
					QuietDuration:   0,
					GateWaitState:   autoPromoteGateWaitSource,
					NoProgressLimit: 1,
					Gate:            gate.Config{Kind: gate.KindCommand},
				},
				ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates:         []string{"Human Review", "Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
				ContinuationRetryDelay: time.Minute,
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				logger:       slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
			}
			state := newState(cfg)
			state.Running[issue.ID] = Running{
				Issue:         issue,
				Attempt:       2,
				WorkAttemptID: 42,
				Mode:          runpkg.RunModeImplement,
				StartedAt:     base.Add(-time.Minute),
				DiffStats:     DiffStats{Status: "clean"},
			}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: base.Add(-time.Minute)}

			orch.handleRunResult(context.Background(), &state, runpkg.Completion{
				IssueID:     issue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result: runpkg.RunResult{
					FinalState: FinalStateCompleted,
					DiffStats:  DiffStats{Status: "clean"},
				},
			})

			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != tt.wantTerminal {
				t.Fatalf("completions = %#v, want terminal %q", attempts.completions, tt.wantTerminal)
			}
			if tracker.hydrations != tt.wantHydrations {
				t.Fatalf("hydrations = %d, want %d", tracker.hydrations, tt.wantHydrations)
			}
			if _, ok := state.Completed[issue.ID]; !ok {
				t.Fatalf("Completed[%q] missing after gate-wait completion", issue.ID)
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after gate-wait completion", issue.ID)
			}
			if _, ok := state.Claimed[issue.ID]; ok {
				t.Fatalf("Claimed[%q] present after gate-wait completion", issue.ID)
			}
			if _, ok := state.Blocked[issue.ID]; ok {
				t.Fatalf("Blocked[%q] present after gate-wait completion", issue.ID)
			}
			if len(tracker.updates) != 0 {
				t.Fatalf("state updates = %#v, want breaker untouched", tracker.updates)
			}
		})
	}
}

func TestHandleRunResultDoesNotSupersedeReworkPullRequestUpdates(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 28, 19, 15, 0, 0, time.UTC)
	completedIssue := implementProgressIssue("completed-head")
	completedIssue.State = "Rework"
	replacementIssue := cloneIssue(completedIssue)
	replacementIssue.PullRequest.HeadSHA = "replacement-head"
	replacementIssue.Comments = []connector.IssueComment{{Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```"}}
	signature := autoPromoteReworkSignature{PRNumber: 1070, HeadSHA: "completed-head"}
	tests := []struct {
		name   string
		result runpkg.RunResult
	}{
		{
			name:   "head push",
			result: runpkg.RunResult{PullRequestHeadPushed: true},
		},
		{
			name:   "pull request update",
			result: runpkg.RunResult{PullRequestUpdated: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracker := &implementProgressConnector{hydrated: replacementIssue, refreshed: replacementIssue}
			attempts := &implementProgressAttemptStore{history: []store.WorkAttempt{
				successfulReworkGateWaitAttempt(base.Add(-2*time.Minute), completedIssue, signature, true),
			}}
			cfg := normalizeConfig(Config{
				Project: scheduler.ProjectCandidate{ID: "detent"},
				AutoPromote: AutoPromoteConfig{
					Enabled:       true,
					GateWaitState: autoPromoteGateWaitSource,
					Gate:          gate.Config{Kind: gate.KindCommand},
				},
				ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates:         []string{"Human Review", "Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
				ContinuationRetryDelay: time.Minute,
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			state := newState(cfg)
			state.Running[completedIssue.ID] = Running{
				Issue:               completedIssue,
				Attempt:             2,
				WorkAttemptID:       42,
				Mode:                runpkg.RunModeImplement,
				DispatchSourceState: "Rework",
				StartedAt:           base.Add(-time.Minute),
				DiffStats:           DiffStats{Status: "clean"},
			}
			state.Claimed[completedIssue.ID] = Claimed{Issue: completedIssue, ClaimedAt: base.Add(-time.Minute)}

			result := tt.result
			result.FinalState = FinalStateCompleted
			result.DiffStats = DiffStats{Status: "clean"}
			orch.handleRunResult(context.Background(), &state, runpkg.Completion{
				IssueID:     completedIssue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result:      result,
			})

			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalSuccess {
				t.Fatalf("completions = %#v, want successful replacement-head completion", attempts.completions)
			}
			record := implementProgressRecordFromCompletion(t, attempts.completions[0])
			if record.CurrentSignature.HeadSHA != "replacement-head" {
				t.Fatalf("completion head = %q, want replacement-head", record.CurrentSignature.HeadSHA)
			}
			completed := state.Completed[completedIssue.ID]
			if completed.gateWaitEvidence.PullRequest == nil || completed.gateWaitEvidence.PullRequest.HeadSHA != "replacement-head" {
				t.Fatalf("gate-wait evidence = %#v, want replacement head", completed.gateWaitEvidence.PullRequest)
			}
		})
	}
}

func TestHandleRunResultHoldsCompletedReworkGateWait(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 28, 18, 0, 0, 0, time.UTC)
	signature := autoPromoteReworkSignature{
		PRNumber: 1070,
		HeadSHA:  "same-head",
	}
	tests := []struct {
		name          string
		history       []store.WorkAttempt
		completionErr error
		wantReason    string
		wantClaimed   bool
		wantTerminal  store.WorkAttemptTerminalState
	}{
		{
			name:         "first successful completion",
			wantReason:   "first_completed_attempt",
			wantTerminal: store.WorkAttemptTerminalSuccess,
		},
		{
			name:         "successful completion with unchanged fingerprint",
			history:      []store.WorkAttempt{implementProgressHistoryAttempt(1, signature, store.WorkAttemptTerminalSuccess)},
			wantReason:   "unchanged_signature_clean_diff",
			wantTerminal: store.WorkAttemptTerminalSuccess,
		},
		{
			name:          "persistence failure retains claim",
			completionErr: errors.New("attempt store unavailable"),
			wantReason:    "first_completed_attempt",
			wantClaimed:   true,
			wantTerminal:  store.WorkAttemptTerminalSuccess,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := implementProgressIssue("same-head")
			issue.State = "Rework"
			issue.Comments = []connector.IssueComment{{Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```"}}
			tracker := &implementProgressConnector{hydrated: issue, refreshed: issue}
			attempts := &implementProgressAttemptStore{history: tt.history, completionErr: tt.completionErr}
			cfg := normalizeConfig(Config{
				Project: scheduler.ProjectCandidate{ID: "detent"},
				AutoPromote: AutoPromoteConfig{
					Enabled:         true,
					GateWaitState:   autoPromoteGateWaitSource,
					NoProgressLimit: 3,
					Gate:            gate.Config{Kind: gate.KindCommand},
				},
				ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates:         []string{"Human Review", "Blocked"},
				TerminalStates:         []string{"Done", "Cancelled"},
				ContinuationRetryDelay: time.Minute,
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			state := newState(cfg)
			state.Running[issue.ID] = Running{
				Issue:               issue,
				Attempt:             2,
				WorkAttemptID:       42,
				Mode:                runpkg.RunModeImplement,
				DispatchSourceState: "Rework",
				StartedAt:           base.Add(-time.Minute),
				DiffStats:           DiffStats{Status: "clean"},
				DispatchProgress:    implementProgressArtifactSnapshotFromIssue(issue, true),
			}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: base.Add(-time.Minute)}

			orch.handleRunResult(context.Background(), &state, runpkg.Completion{
				IssueID:     issue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result: runpkg.RunResult{
					FinalState: FinalStateCompleted,
					DiffStats:  DiffStats{Status: "clean"},
				},
			})

			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != tt.wantTerminal {
				t.Fatalf("completions = %#v, want terminal %q", attempts.completions, tt.wantTerminal)
			}
			persisted := store.WorkAttempt{
				TerminalState:      attempts.completions[0].TerminalState,
				WorkerMetadataJSON: attempts.completions[0].WorkerMetadataJSON,
			}
			record, ok := implementProgressRecordFromAttempt(persisted)
			if !ok || record.Reason != tt.wantReason {
				t.Fatalf("completion progress = %#v, want reason %q", record, tt.wantReason)
			}
			if reason := completionGateWaitReasonFromAttempt(persisted); reason != completedReworkGateWaitReason {
				t.Fatalf("completion gate wait reason = %q, want %q", reason, completedReworkGateWaitReason)
			}
			completed, ok := state.Completed[issue.ID]
			if !ok {
				t.Fatalf("Completed[%q] missing after Rework gate-wait completion", issue.ID)
			}
			if completed.GateWaitReason != completedReworkGateWaitReason {
				t.Fatalf("Completed[%q].GateWaitReason = %q, want %q", issue.ID, completed.GateWaitReason, completedReworkGateWaitReason)
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after Rework gate-wait completion", issue.ID)
			}
			_, claimed := state.Claimed[issue.ID]
			if claimed != tt.wantClaimed {
				t.Fatalf("Claimed[%q] present = %v, want %v", issue.ID, claimed, tt.wantClaimed)
			}
			if !tt.wantClaimed {
				if !autoPromoteActiveGatePendingIssue(issue, &state, cfg, cfg.AutoPromote) {
					t.Fatal("Rework completion is not pending the source-lane gate")
				}
			}
		})
	}
}

func TestHandleRunResultCommentsOnObservedLaneTransition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 9, 30, 0, 0, time.UTC)
	runningIssue := implementProgressIssue("head")
	hydratedIssue := cloneIssue(runningIssue)
	hydratedIssue.State = blockedStatusState
	hydratedIssue.WorkpadSignal = &workpad.Signal{
		Source:      workpad.SourceStructured,
		Status:      workpad.StatusBlocked,
		HumanAction: "Restart the browser session.",
	}
	tracker := &implementProgressConnector{hydrated: hydratedIssue}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project:                scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote:            AutoPromoteConfig{NoProgressLimit: 3},
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Minute,
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		Attempt:       1,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		DiffStats:     DiffStats{Status: "clean"},
	}
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			DiffStats:  runpkg.DiffStats{Status: "clean"},
		},
	})

	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one observed routing audit comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Observed this issue move from In Progress to Blocked during worker completion.",
		"source: tracker_refresh",
		"reason: workpad_blocked",
		"human_action: Restart the browser session.",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing %q", tracker.comments[0].body, fragment)
		}
	}
}

func TestHandleRunResultReappliesCITriggerAfterWorkerPush(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 9, 45, 0, 0, time.UTC)
	staggerSeconds := 15
	runningIssue := implementProgressIssue("old-head")
	hydratedIssue := implementProgressIssue("new-head")
	relabelStarted := make(chan autoPromoteTickRelabel, 1)
	relabelRelease := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case relabelRelease <- struct{}{}:
		default:
		}
	})
	tracker := &implementProgressConnector{
		hydrated:       hydratedIssue,
		relabelStarted: relabelStarted,
		relabelRelease: relabelRelease,
	}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote: AutoPromoteConfig{
			NoProgressLimit: 3,
			Gate: gate.Config{
				Kind:                         gate.KindCommand,
				RequiredStatusChecks:         []string{"Test", "Checks"},
				CITriggerLabel:               "ci:ready",
				CITriggerLabelStaggerSeconds: &staggerSeconds,
			},
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Minute,
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		Attempt:       1,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		DiffStats:     DiffStats{Status: "clean"},
	}
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState:            runpkg.FinalStateCompleted,
			DiffStats:             runpkg.DiffStats{Status: "clean"},
			PullRequestHeadPushed: true,
		},
	})

	select {
	case got := <-relabelStarted:
		want := autoPromoteTickRelabel{repository: "digitaldrywood/detent", number: 1070, label: "ci:ready", stagger: 15 * time.Second}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("relabel = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker-push trigger-label reapplication")
	}
}

func TestHandleRunResultRefreshesNewPullRequestBeforeCITrigger(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	staggerSeconds := 15
	runningIssue := implementProgressIssue("")
	runningIssue.PullRequest = nil
	refreshedIssue := implementProgressIssue("new-head")
	relabelStarted := make(chan autoPromoteTickRelabel, 1)
	relabelRelease := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case relabelRelease <- struct{}{}:
		default:
		}
	})
	tracker := &implementProgressConnector{
		refreshed:      refreshedIssue,
		hydrated:       refreshedIssue,
		relabelStarted: relabelStarted,
		relabelRelease: relabelRelease,
	}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote: AutoPromoteConfig{
			NoProgressLimit: 3,
			Gate: gate.Config{
				Kind:                         gate.KindCommand,
				RequiredStatusChecks:         []string{"Test", "Checks"},
				CITriggerLabel:               "ci:ready",
				CITriggerLabelStaggerSeconds: &staggerSeconds,
			},
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Minute,
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		Attempt:       1,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		DiffStats:     DiffStats{Status: "clean"},
	}
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState:            runpkg.FinalStateCompleted,
			DiffStats:             runpkg.DiffStats{Status: "clean"},
			PullRequestUpdated:    true,
			PullRequestHeadPushed: true,
		},
	})

	select {
	case got := <-relabelStarted:
		want := autoPromoteTickRelabel{repository: "digitaldrywood/detent", number: 1070, label: "ci:ready", stagger: 15 * time.Second}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("relabel = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for new-PR trigger-label reapplication")
	}
	if tracker.hydrations != 2 {
		t.Fatalf("hydrations = %d, want 2", tracker.hydrations)
	}
}

func TestHandleRunResultRefreshesStalePullRequestAfterHydrationFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 10, 30, 0, 0, time.UTC)
	staggerSeconds := 15
	runningIssue := implementProgressIssue("old-head")
	refreshedIssue := implementProgressIssue("new-head")
	relabelStarted := make(chan autoPromoteTickRelabel, 1)
	relabelRelease := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case relabelRelease <- struct{}{}:
		default:
		}
	})
	tracker := &implementProgressConnector{
		hydrated: refreshedIssue,
		hydrateErrs: []error{
			errors.New("completion hydration failure"),
			errors.New("push refresh hydration failure"),
		},
		relabelStarted: relabelStarted,
		relabelRelease: relabelRelease,
	}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote: AutoPromoteConfig{
			NoProgressLimit: 3,
			Gate: gate.Config{
				Kind:                         gate.KindCommand,
				RequiredStatusChecks:         []string{"Test", "Checks"},
				CITriggerLabel:               "ci:ready",
				CITriggerLabelStaggerSeconds: &staggerSeconds,
			},
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Minute,
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		ciTriggerLabelHeads: map[string]ciTriggerLabelHead{
			"digitaldrywood/detent#1070|ci:ready": {HeadSHA: "old-head"},
		},
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:         runningIssue,
		Attempt:       1,
		WorkAttemptID: 42,
		Mode:          runpkg.RunModeImplement,
		StartedAt:     now.Add(-time.Minute),
		DiffStats:     DiffStats{Status: "clean"},
	}
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState:            runpkg.FinalStateCompleted,
			DiffStats:             runpkg.DiffStats{Status: "clean"},
			PullRequestHeadPushed: true,
		},
	})

	select {
	case got := <-relabelStarted:
		want := autoPromoteTickRelabel{repository: "digitaldrywood/detent", number: 1070, label: "ci:ready", stagger: 15 * time.Second}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("relabel = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refreshed-head trigger-label reapplication")
	}
	if tracker.hydrations != 3 {
		t.Fatalf("hydrations = %d, want completion hydration, push refresh, and trigger refresh", tracker.hydrations)
	}
}

func TestStickyBlockReasonIncludesCircuitBreakers(t *testing.T) {
	t.Parallel()

	for _, reason := range []string{
		noProgressLimitReason,
		dispatchLoopDetectedReason,
		terminalAttemptRetryLimitCause,
		workspacePreparationRetryLimitCause,
		workpadBlockedUnactionedReason,
		"token_ceiling_circuit_breaker",
		tokenCeilingBlockedReasonPrefix + "observed 16100000 tokens above the 16000000 max_session_tokens ceiling",
		mergeWorkerRetryExhaustedReason,
	} {
		if !stickyBlockReason(reason) {
			t.Fatalf("stickyBlockReason(%q) = false, want true", reason)
		}
	}
}

func TestBlockImplementProgressRequiresHumanAcknowledgement(t *testing.T) {
	t.Parallel()

	for _, reason := range []string{noProgressLimitReason, dispatchLoopDetectedReason} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			tracker := &implementProgressConnector{}
			orch := &Orchestrator{connector: tracker}
			state := newState(normalizeConfig(Config{}))
			issue := connector.Issue{ID: "issue-" + reason, Identifier: "digitaldrywood/detent#1943", State: "Rework"}
			state.Claimed[issue.ID] = Claimed{Issue: issue}
			blockedAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

			if !orch.blockImplementProgress(t.Context(), &state, Running{Issue: issue, Mode: runpkg.RunModeImplement}, implementCompletionProgressDecision{
				Issue:                 issue,
				BlockReason:           reason,
				NoProgressLimit:       3,
				ConsecutiveNoProgress: 3,
			}, blockedAt) {
				t.Fatal("blockImplementProgress() = false, want true")
			}
			blocked := state.Blocked[issue.ID]
			if blocked.Recovery == nil || blocked.Recovery.Owner != blockedRecoveryOwnerHuman || blocked.Recovery.Predicate != blockedRecoveryPredicateManaged {
				t.Fatalf("Blocked[%q].Recovery = %#v, want human-owned managed park", issue.ID, blocked.Recovery)
			}
			if blocked.RecoveryReason == "" || !strings.Contains(blocked.RecoveryReason, "human") {
				t.Fatalf("Blocked[%q].RecoveryReason = %q, want human acknowledgement", issue.ID, blocked.RecoveryReason)
			}
		})
	}
}

func TestImplementProgressHelperBoundaries(t *testing.T) {
	t.Parallel()

	issue := connector.Issue{WorkpadSignal: &workpad.Signal{
		Source:      workpad.SourceStructured,
		Status:      workpad.StatusBlocked,
		HumanAction: " approve the deployment ",
	}}
	if status, action := implementProgressBlockedHumanAction(issue); status != workpad.StatusBlocked || action != "approve the deployment" {
		t.Fatalf("implementProgressBlockedHumanAction() = %q, %q", status, action)
	}
	issue.WorkpadSignal.Source = workpad.SourceProse
	if status, action := implementProgressBlockedHumanAction(issue); status != "" || action != "" {
		t.Fatalf("prose workpad signal = %q, %q, want empty", status, action)
	}

	usable := autoPromoteReworkSignature{PRNumber: 42, HeadSHA: " head ", FailedChecks: []string{"test"}}
	attempts := []store.WorkAttempt{
		{TerminalState: store.WorkAttemptTerminalFailure},
		implementProgressHistoryAttempt(1, usable, store.WorkAttemptTerminalSuccess),
	}
	if got, ok := latestImplementProgressSignature(attempts); !ok || got.PRNumber != usable.PRNumber || got.HeadSHA != "head" {
		t.Fatalf("latestImplementProgressSignature() = %#v, %t", got, ok)
	}
	if got := consecutiveImplementBlockedHumanActionAttempts(nil, "", "Blocked"); got != 0 {
		t.Fatalf("consecutiveImplementBlockedHumanActionAttempts() = %d, want 0", got)
	}

	legacy := implementProgressRecord{
		Outcome:            string(store.WorkAttemptTerminalSuccess),
		Reason:             "no_linked_pull_request",
		WorkspaceDiffStats: implementProgressDiffStats{Status: "clean"},
	}
	if !implementProgressRecordMatchesNoProgress(legacy, autoPromoteReworkSignature{}) {
		t.Fatal("legacy clean completion did not match no progress")
	}
	legacy.WorkspaceDiffStats.FilesChanged = 1
	if implementProgressRecordMatchesNoProgress(legacy, autoPromoteReworkSignature{}) {
		t.Fatal("legacy dirty completion matched no progress")
	}

	evidence := DiffStats{
		UnpushedCommitRefs:             []string{"abc123 fix: preserve work"},
		TrackedPaths:                   []string{"tracked.go"},
		CommitsNotInPullRequest:        []string{"abc123 fix: preserve work"},
		PullRequestComparisonAvailable: true,
	}
	recordedEvidence := implementProgressDiffStatsFromDiffStats(evidence)
	if !reflect.DeepEqual(recordedEvidence.UnpushedCommitRefs, evidence.UnpushedCommitRefs) ||
		!reflect.DeepEqual(recordedEvidence.TrackedPaths, evidence.TrackedPaths) ||
		!reflect.DeepEqual(recordedEvidence.CommitsNotInPullRequest, evidence.CommitsNotInPullRequest) ||
		!recordedEvidence.PullRequestComparisonAvailable {
		t.Fatalf("implementProgressDiffStatsFromDiffStats() evidence = %#v, want %#v", recordedEvidence, evidence)
	}

	invalidAttempts := []store.WorkAttempt{
		{TerminalState: store.WorkAttemptTerminalFailure, WorkerMetadataJSON: `{}`},
		{TerminalState: store.WorkAttemptTerminalSuccess, WorkerMetadataJSON: `{`},
		{TerminalState: store.WorkAttemptTerminalSuccess, WorkerMetadataJSON: `{}`},
	}
	for _, attempt := range invalidAttempts {
		if _, ok := implementProgressRecordFromAttempt(attempt); ok {
			t.Fatalf("implementProgressRecordFromAttempt(%#v) unexpectedly succeeded", attempt)
		}
	}

	added, removed := implementProgressFailedCheckDelta([]string{"test", "lint"}, []string{"lint", "build"})
	if !slicesEqual(added, []string{"build"}) || !slicesEqual(removed, []string{"test"}) {
		t.Fatalf("implementProgressFailedCheckDelta() = %#v, %#v", added, removed)
	}
}

func TestImplementProgressMergedCompletionQualification(t *testing.T) {
	t.Parallel()

	mergedIssue := func() connector.Issue {
		prNumber := 1708
		return connector.Issue{
			PRNumber: &prNumber,
			WorkpadSignal: &workpad.Signal{
				Source: workpad.SourceStructured,
				Status: workpad.StatusComplete,
			},
			PullRequest: &connector.PullRequest{
				Number:        prNumber,
				State:         "MERGED",
				HeadSHA:       "current-head",
				CIStatus:      "success",
				CheckRunCount: 2,
			},
		}
	}

	tests := []struct {
		name      string
		mutate    func(*connector.Issue, *DiffStats)
		qualifies bool
	}{
		{name: "complete merged green clean", qualifies: true},
		{name: "missing workpad", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.WorkpadSignal = nil }},
		{name: "workpad still in progress", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.WorkpadSignal.Status = workpad.StatusInProgress }},
		{name: "prose workpad", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.WorkpadSignal.Source = workpad.SourceProse }},
		{name: "human action remains", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.WorkpadSignal.HumanAction = "approve release" }},
		{name: "blocker remains", mutate: func(issue *connector.Issue, _ *DiffStats) {
			issue.WorkpadSignal.Blockers = []workpad.Blocker{{Identifier: "digitaldrywood/detent#1700"}}
		}},
		{name: "pull request is open", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.PullRequest.State = "OPEN" }},
		{name: "pull request link missing", mutate: func(issue *connector.Issue, _ *DiffStats) {
			issue.PRNumber = nil
			issue.PullRequest.Number = 0
		}},
		{name: "head missing", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.PullRequest.HeadSHA = "" }},
		{name: "check evidence missing", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.PullRequest.CheckRunCount = 0 }},
		{name: "ci pending", mutate: func(issue *connector.Issue, _ *DiffStats) { issue.PullRequest.CIStatus = "pending" }},
		{name: "required check pending", mutate: func(issue *connector.Issue, _ *DiffStats) {
			issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{Name: "Test", Status: "in_progress"}}
		}},
		{name: "hydration degraded", mutate: func(issue *connector.Issue, _ *DiffStats) {
			issue.PullRequest.HydrationDegradedReason = connector.PullRequestHydrationReasonStaleCachedPullData
		}},
		{name: "workspace dirty", mutate: func(_ *connector.Issue, diffStats *DiffStats) { diffStats.FilesChanged = 1 }},
		{name: "commit unpushed", mutate: func(_ *connector.Issue, diffStats *DiffStats) { diffStats.UnpushedCommits = 1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := mergedIssue()
			diffStats := DiffStats{Status: "clean"}
			if tt.mutate != nil {
				tt.mutate(&issue, &diffStats)
			}
			if got := implementProgressMergedCompletion(issue, diffStats); got != tt.qualifies {
				t.Fatalf("implementProgressMergedCompletion() = %t, want %t", got, tt.qualifies)
			}
		})
	}
}

func TestImplementProgressOperationalWorkspaceClean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		diffStats DiffStats
		want      bool
	}{
		{name: "clean delivery", diffStats: DiffStats{Status: "clean"}, want: true},
		{name: "workspace diff", diffStats: DiffStats{FilesChanged: 1, Status: "changed"}},
		{name: "unpushed commit", diffStats: DiffStats{UnpushedCommits: 1, Status: "clean"}},
		{name: "commit ahead", diffStats: DiffStats{CommitsAhead: 1, Status: "clean"}},
		{name: "tracked path", diffStats: DiffStats{TrackedPaths: []string{"tracked.go"}, Status: "clean"}},
		{name: "commit absent from pull request", diffStats: DiffStats{CommitsNotInPullRequest: []string{"abc123 fix: preserve work"}, Status: "clean"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := implementProgressOperationalWorkspaceClean(tt.diffStats); got != tt.want {
				t.Fatalf("implementProgressOperationalWorkspaceClean() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestCompletedReworkGateWaitProgressPreservesStrandedCommitDecision(t *testing.T) {
	t.Parallel()

	issue := implementProgressIssue("pull-request-head")
	issue.State = "Rework"
	cfg := normalizeConfig(Config{
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	decision := implementCompletionProgressDecision{
		Issue:            issue,
		Outcome:          store.WorkAttemptTerminalNoProgress,
		Reason:           strandedUnpushedWorkReason,
		CurrentSignature: autoPromoteReworkSignature{PRNumber: 1070, HeadSHA: "pull-request-head"},
		WorkspaceDiffStats: DiffStats{
			CommitsNotInPullRequest:        []string{"abc123 fix: preserve work"},
			PullRequestComparisonAvailable: true,
			HeadSHA:                        "workspace-head",
			Status:                         "clean",
		},
	}
	running := Running{Issue: issue, DispatchSourceState: "Rework"}

	got, reason := completedReworkGateWaitProgress(running, decision, cfg, FinalStateCompleted)
	if reason != "" || got.Outcome != store.WorkAttemptTerminalNoProgress || got.Reason != strandedUnpushedWorkReason {
		t.Fatalf("completedReworkGateWaitProgress() = outcome %q reason %q gate wait %q, want preserved stranded decision", got.Outcome, got.Reason, reason)
	}
}

func TestImplementProgressBlockComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		decision     implementCompletionProgressDecision
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "boundary evidence",
			decision: implementCompletionProgressDecision{
				BlockReason:            workpadBlockedUnactionedReason,
				NoProgressLimit:        3,
				ConsecutiveNoProgress:  2,
				ConsecutiveHumanAction: 3,
				HumanAction:            "Approve release\nConfirm rollback",
				CurrentSignature:       autoPromoteReworkSignature{PRNumber: 42, HeadSHA: "head", FailedChecks: []string{"test"}},
				PreviousSignature:      autoPromoteReworkSignature{HeadSHA: "previous"},
				FailedChecksAdded:      []string{"test"},
				FailedChecksRemoved:    []string{"lint"},
				WorkspaceDiffStats:     DiffStats{Status: "clean"},
			},
			wantContains: []string{"workpad_blocked_unactioned", "pull/42", "head", "previous", "failed_checks_added", "0 files", "> Approve release", "> Confirm rollback"},
		},
		{
			name: "dispatch loop stale carry",
			decision: implementCompletionProgressDecision{
				BlockReason:           dispatchLoopDetectedReason,
				NoProgressLimit:       3,
				ConsecutiveNoProgress: 3,
				WorkspaceDiffStats: DiffStats{
					FilesChanged:    1,
					AddedLines:      1,
					RemovedLines:    1,
					UnpushedCommits: 2,
					HeadSHA:         "workspace-head",
					Fingerprint:     "diff-fingerprint",
					Status:          "changed",
				},
			},
			wantContains: []string{
				"workspace unchanged across 3 attempts",
				"head `workspace-head`",
				"diff fingerprint `diff-fingerprint`",
				"identical since attempt 1",
				"carrying 1 changed file (+1/-1), 2 unpushed commits, unchanged since attempt 1",
			},
			wantAbsent: []string{"workspace_diffstat", "(changed)"},
		},
		{
			name: "dispatch loop unavailable workspace evidence",
			decision: implementCompletionProgressDecision{
				BlockReason:           dispatchLoopDetectedReason,
				NoProgressLimit:       3,
				ConsecutiveNoProgress: 3,
			},
			wantContains: []string{"workspace evidence: unavailable"},
			wantAbsent:   []string{"workspace unchanged", "identical since"},
		},
	}
	issue := connector.Issue{PullRequest: &connector.PullRequest{URL: "https://github.test/pull/42"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			comment := implementProgressBlockComment(issue, tt.decision)
			for _, want := range tt.wantContains {
				if !strings.Contains(comment, want) {
					t.Fatalf("comment missing %q:\n%s", want, comment)
				}
			}
			for _, unwanted := range tt.wantAbsent {
				if strings.Contains(comment, unwanted) {
					t.Fatalf("comment contains misleading evidence %q:\n%s", unwanted, comment)
				}
			}
		})
	}
	if got := implementProgressRecoveryReason(tests[0].decision); got != tests[0].decision.HumanAction {
		t.Fatalf("implementProgressRecoveryReason() = %q, want %q", got, tests[0].decision.HumanAction)
	}
}

func TestEvaluateImplementCompletionProgressFailureBoundaries(t *testing.T) {
	t.Parallel()

	noPR := implementProgressIssueWithoutPR()
	var logs bytes.Buffer
	orch := &Orchestrator{
		cfg:          Config{AutoPromote: AutoPromoteConfig{NoProgressLimit: 3}},
		connector:    &implementProgressConnector{},
		workAttempts: &implementProgressAttemptStore{historyErr: errors.New("history unavailable")},
		logger:       slog.New(slog.NewTextHandler(&logs, nil)),
	}
	decision := orch.evaluateImplementCompletionProgress(t.Context(), Running{Issue: noPR}, FinalStateCompleted, false)
	if decision.Reason != "attempt_history_lookup_failed" || !strings.Contains(decision.Warning, "history unavailable") || !strings.Contains(logs.String(), "history unavailable") {
		t.Fatalf("history failure decision = %#v logs = %q", decision, logs.String())
	}

	orch.workAttempts = &implementProgressAttemptStore{}
	decision = orch.evaluateImplementCompletionProgress(t.Context(), Running{
		Issue:     noPR,
		DiffStats: DiffStats{FilesChanged: 1, Status: "dirty"},
	}, FinalStateCompleted, false)
	if decision.Reason != "workspace_diff_fingerprint_unavailable_without_pull_request" {
		t.Fatalf("dirty no-PR decision = %#v", decision)
	}

	linked := implementProgressIssue("head")
	orch.connector = &backendCapacityTestConnector{}
	decision = orch.evaluateImplementCompletionProgress(t.Context(), Running{Issue: linked}, FinalStateCompleted, false)
	if decision.Reason != "pull_request_hydrator_unavailable" {
		t.Fatalf("missing hydrator decision = %#v", decision)
	}

	orch.warnImplementProgressRefresh(linked, "refresh unavailable", errors.New("tracker unavailable"))
	if !strings.Contains(logs.String(), "tracker unavailable") {
		t.Fatalf("refresh warning logs = %q", logs.String())
	}
}

func TestImplementProgressArtifactKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous implementProgressArtifactSnapshot
		current  implementProgressArtifactSnapshot
		want     []string
	}{
		{name: "unchanged structured record", previous: implementProgressArtifactSnapshot{TrackerState: "in progress", WorkpadRead: true}, current: implementProgressArtifactSnapshot{TrackerState: "in progress", WorkpadRead: true}, want: []string{}},
		{name: "tracker transition", previous: implementProgressArtifactSnapshot{TrackerState: "in progress"}, current: implementProgressArtifactSnapshot{TrackerState: "blocked"}, want: []string{"tracker_state_transition"}},
		{name: "linked blocker", previous: implementProgressArtifactSnapshot{}, current: implementProgressArtifactSnapshot{NativeBlockers: []string{"blocker-id"}}, want: []string{"linked_blocker"}},
		{name: "typed workpad predicate", previous: implementProgressArtifactSnapshot{WorkpadRead: true}, current: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadReason: "missing_current_head_ci"}, want: []string{"workpad_predicate"}},
		{name: "completion receipt", previous: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadStatus: workpad.StatusComplete, WorkpadReceiptHash: "before"}, current: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadStatus: workpad.StatusComplete, WorkpadReceiptHash: "after"}, want: []string{"artifact_receipt"}},
		{name: "unchanged completion receipt", previous: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadStatus: workpad.StatusComplete, WorkpadReceiptHash: "same"}, current: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadStatus: workpad.StatusComplete, WorkpadReceiptHash: "same"}, want: []string{}},
		{name: "audit artifact", previous: implementProgressArtifactSnapshot{WorkpadRead: true}, current: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadFields: map[string]string{"duplicate_groups": "23"}}, want: []string{"audit_artifact"}},
		{name: "completion assertion is not an audit artifact", previous: implementProgressArtifactSnapshot{WorkpadRead: true}, current: implementProgressArtifactSnapshot{WorkpadRead: true, WorkpadFields: map[string]string{workpad.FieldCompletionKind: workpad.CompletionOperational, workpad.FieldCompletionEvidence: "done"}}, want: []string{}},
		{name: "unverified workpad is ignored", previous: implementProgressArtifactSnapshot{}, current: implementProgressArtifactSnapshot{WorkpadReason: "missing_current_head_ci", WorkpadFields: map[string]string{"duplicate_groups": "23"}}, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := implementProgressArtifactKinds(tt.previous, tt.current); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("implementProgressArtifactKinds() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestImplementProgressDispatchArtifactSnapshotWithoutReader(t *testing.T) {
	t.Parallel()

	issue := implementProgressIssueWithoutPR()
	issue.BlockedBy = []connector.BlockedRef{{ID: "blocker-id"}}
	for _, orch := range []*Orchestrator{nil, {connector: &backendCapacityTestConnector{}}} {
		snapshot := orch.implementProgressDispatchArtifactSnapshot(t.Context(), issue)
		if snapshot.WorkpadRead || !reflect.DeepEqual(snapshot.NativeBlockers, []string{"blocker-id"}) {
			t.Fatalf("snapshot = %#v", snapshot)
		}
	}
}

func implementProgressIssue(headSHA string, failedChecks ...string) connector.Issue {
	prNumber := 1070
	issue := connector.Issue{
		ID:           "issue-1070",
		Identifier:   "digitaldrywood/detent#1070",
		Title:        "No progress",
		State:        "In Progress",
		URL:          "https://github.test/digitaldrywood/detent/issues/1070",
		PRNumber:     &prNumber,
		PRRepository: "digitaldrywood/detent",
		PullRequest: &connector.PullRequest{
			Number:  prNumber,
			URL:     "https://github.test/digitaldrywood/detent/pull/1070",
			State:   "OPEN",
			HeadSHA: headSHA,
		},
	}
	for _, check := range failedChecks {
		issue.PullRequest.RequiredCheckFailures = append(issue.PullRequest.RequiredCheckFailures, connector.PullRequestCheck{
			Name:       check,
			Status:     "completed",
			Conclusion: "failure",
		})
	}
	return issue
}

func implementProgressIssueWithHydrationDegraded(headSHA string, reason string, failedChecks ...string) connector.Issue {
	issue := implementProgressIssue(headSHA, failedChecks...)
	issue.PullRequest.HydrationDegradedReason = reason
	return issue
}

func implementProgressIssueWithoutPR() connector.Issue {
	return connector.Issue{
		ID:         "issue-plan",
		Identifier: "digitaldrywood/detent#1200",
		Title:      "Plan only",
		State:      "In Progress",
		URL:        "https://github.test/digitaldrywood/detent/issues/1200",
	}
}

func implementProgressWorkpadComment(blockerRef string, humanAction string) string {
	var body strings.Builder
	body.WriteString("## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: blocked\n")
	if strings.TrimSpace(blockerRef) == "" {
		body.WriteString("blockers: []\n")
	} else {
		body.WriteString("blockers:\n  - ref: \"")
		body.WriteString(blockerRef)
		body.WriteString("\"\n    reason: \"waiting for dependency\"\n")
	}
	if strings.TrimSpace(humanAction) == "" {
		body.WriteString("human_action: null\n```")
	} else {
		body.WriteString("human_action: \"")
		body.WriteString(humanAction)
		body.WriteString("\"\n```")
	}
	return body.String()
}

func implementProgressStructuredWorkpad(status string, prose string, fields map[string]string) string {
	var body strings.Builder
	body.WriteString("## Codex Workpad\n\n")
	if prose != "" {
		body.WriteString(prose)
		body.WriteString("\n\n")
	}
	body.WriteString("```detent-status\nschema: 1\nstatus: ")
	body.WriteString(status)
	body.WriteString("\nblockers: []\nhuman_action: null\n")
	if len(fields) > 0 {
		body.WriteString("fields:\n")
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			body.WriteString("  ")
			body.WriteString(name)
			body.WriteString(": \"")
			body.WriteString(fields[name])
			body.WriteString("\"\n")
		}
	}
	body.WriteString("```")
	return body.String()
}

func implementProgressHistoryAttempt(id int64, signature autoPromoteReworkSignature, terminal store.WorkAttemptTerminalState) store.WorkAttempt {
	diff := implementProgressDiffStats{HeadSHA: "same-workspace-head", Status: "clean"}
	return store.WorkAttempt{
		ID:                 id,
		ProjectID:          "detent",
		IssueID:            "issue-1070",
		Identifier:         "digitaldrywood/detent#1070",
		IssueURL:           "https://github.test/digitaldrywood/detent/issues/1070",
		WorkerType:         "agent",
		Lane:               "In Progress",
		Status:             store.WorkAttemptStatusTerminal,
		TerminalState:      terminal,
		CompletedAt:        time.Date(2026, 7, 8, 15, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: implementProgressMetadataJSONWithDiff(signature, diff, terminal),
	}
}

func implementProgressLegacyNoPRHistoryAttempt(id int64) store.WorkAttempt {
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "gopher-ai",
		IssueID:       "issue-213",
		Identifier:    "gopherguides/gopher-ai#213",
		IssueURL:      "https://github.test/gopherguides/gopher-ai/issues/213",
		WorkerType:    "agent",
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: store.WorkAttemptTerminalSuccess,
		CompletedAt:   time.Date(2026, 7, 10, 22, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode": runpkg.RunModeImplement,
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:            string(store.WorkAttemptTerminalSuccess),
				Reason:             "no_linked_pull_request",
				WorkspaceDiffStats: implementProgressDiffStats{Status: "clean"},
			},
		}),
	}
}

func implementProgressDependencyDeferralHistoryAttempt(id int64, identifier string, state string) store.WorkAttempt {
	diff := implementProgressDiffStats{HeadSHA: "same-workspace-head", Status: "clean"}
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "detent",
		IssueID:       "issue-plan",
		Identifier:    "digitaldrywood/detent#1200",
		IssueURL:      "https://github.test/digitaldrywood/detent/issues/1200",
		WorkerType:    "agent",
		Lane:          "In Progress",
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: store.WorkAttemptTerminalSuccess,
		CompletedAt:   time.Date(2026, 8, 8, 18, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode":                   runpkg.RunModeImplement,
			dispatchLoopStartMetadataKey: dispatchLoopTestStart("In Progress", autoPromoteReworkSignature{}, diff),
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:            string(store.WorkAttemptTerminalSuccess),
				Reason:             implementDependencyDeferralReason,
				DependencyDeferral: true,
				DependencyBlockers: []implementDependencyBlocker{{ID: "blocker", Identifier: identifier, State: state}},
				WorkspaceDiffStats: diff,
				TrackerState:       "In Progress",
			},
		}),
	}
}

func implementProgressNoPRHistoryAttempt(id int64, diffStats DiffStats, humanAction string, trackerState string) store.WorkAttempt {
	workpadStatus := ""
	if strings.TrimSpace(humanAction) != "" {
		workpadStatus = "blocked"
	}
	diffStats = dispatchLoopTestRunnerDiff(diffStats)
	lane := strings.TrimSpace(trackerState)
	if lane == "" {
		lane = "In Progress"
	}
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "detent",
		IssueID:       "issue-plan",
		Identifier:    "digitaldrywood/detent#1200",
		IssueURL:      "https://github.test/digitaldrywood/detent/issues/1200",
		WorkerType:    "agent",
		Lane:          lane,
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: store.WorkAttemptTerminalSuccess,
		CompletedAt:   time.Date(2026, 7, 11, 12, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode":                   runpkg.RunModeImplement,
			dispatchLoopStartMetadataKey: dispatchLoopTestStart(lane, autoPromoteReworkSignature{}, implementProgressDiffStatsFromDiffStats(diffStats)),
			implementProgressMetadataKey: map[string]any{
				"outcome":            string(store.WorkAttemptTerminalSuccess),
				"reason":             "workspace_diff_present_without_pull_request",
				"workspace_diffstat": implementProgressDiffStatsFromDiffStats(diffStats),
				"workpad_status":     workpadStatus,
				"human_action":       humanAction,
				"tracker_state":      lane,
			},
		}),
	}
}

func implementProgressStrandedHistoryAttempt(id int64, signature autoPromoteReworkSignature) store.WorkAttempt {
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "detent",
		IssueID:       "issue-plan",
		Identifier:    "digitaldrywood/detent#1200",
		IssueURL:      "https://github.test/digitaldrywood/detent/issues/1200",
		WorkerType:    "agent",
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: store.WorkAttemptTerminalNoProgress,
		CompletedAt:   time.Date(2026, 7, 12, 12, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode": runpkg.RunModeImplement,
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:            implementProgressOutcomeNoProgress,
				Reason:             strandedUnpushedWorkReason,
				CurrentSignature:   implementProgressSignatureRecordFromSignature(signature),
				CurrentHeadSHA:     signature.HeadSHA,
				WorkspaceDiffStats: implementProgressDiffStats{UnpushedCommits: 1, Status: "clean"},
			},
		}),
	}
}

func implementProgressMetadataJSON(signature autoPromoteReworkSignature, terminal store.WorkAttemptTerminalState) string {
	return implementProgressMetadataJSONWithDiff(
		signature,
		implementProgressDiffStats{HeadSHA: "same-workspace-head", Status: "clean"},
		terminal,
	)
}

func implementProgressMetadataJSONWithDiff(signature autoPromoteReworkSignature, diff implementProgressDiffStats, terminal store.WorkAttemptTerminalState) string {
	return marshalWorkAttemptJSON(map[string]any{
		"run_mode":                   runpkg.RunModeImplement,
		dispatchLoopStartMetadataKey: dispatchLoopTestStart("In Progress", signature, diff),
		implementProgressMetadataKey: implementProgressRecord{
			Outcome:            string(terminal),
			Reason:             "test_history",
			CurrentSignature:   implementProgressSignatureRecordFromSignature(signature),
			CurrentHeadSHA:     signature.HeadSHA,
			WorkspaceDiffStats: diff,
			TrackerState:       "In Progress",
		},
	})
}

func implementProgressRecordFromCompletion(t *testing.T, completion store.WorkAttemptCompletion) implementProgressRecord {
	t.Helper()

	attempt := store.WorkAttempt{
		TerminalState:      completion.TerminalState,
		WorkerMetadataJSON: completion.WorkerMetadataJSON,
	}
	record, ok := implementProgressRecordFromAttempt(attempt)
	if !ok {
		t.Fatalf("completion metadata did not include progress record: %s", completion.WorkerMetadataJSON)
	}
	return record
}

type implementProgressConnector struct {
	hydrated           connector.Issue
	refreshed          connector.Issue
	referenced         connector.Issue
	hydrateErr         error
	hydrateErrs        []error
	refreshErr         error
	referenceErr       error
	hydrations         int
	referenceRefreshes int
	updates            []implementProgressUpdate
	comments           []implementProgressComment
	relabelStarted     chan autoPromoteTickRelabel
	relabelRelease     chan struct{}
	resolvedBlockers   []connector.Issue
}

type implementProgressUpdate struct {
	issueID string
	state   string
}

type implementProgressComment struct {
	issueID string
	body    string
}

func (c *implementProgressConnector) Name() string {
	return "implement-progress"
}

func (c *implementProgressConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *implementProgressConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *implementProgressConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	if c.refreshErr != nil {
		return nil, c.refreshErr
	}
	if strings.TrimSpace(c.refreshed.ID) == "" {
		return nil, nil
	}
	return []connector.Issue{cloneIssue(c.refreshed)}, nil
}

func (c *implementProgressConnector) RefreshPullRequestReference(_ context.Context, issue connector.Issue) (connector.Issue, error) {
	c.referenceRefreshes++
	if c.referenceErr != nil {
		return connector.Issue{}, c.referenceErr
	}
	if strings.TrimSpace(c.referenced.ID) == "" {
		return cloneIssue(issue), nil
	}
	return cloneIssue(c.referenced), nil
}

func (c *implementProgressConnector) FetchIssueComments(context.Context, connector.Issue) ([]connector.IssueComment, error) {
	return cloneIssueComments(c.refreshed.Comments), nil
}

func (c *implementProgressConnector) FetchIssueStatesByIdentifiers(context.Context, []string) ([]connector.Issue, error) {
	return cloneIssues(c.resolvedBlockers), nil
}

func (c *implementProgressConnector) CreateComment(_ context.Context, issueID string, body string) error {
	c.comments = append(c.comments, implementProgressComment{issueID: issueID, body: body})
	return nil
}

func (c *implementProgressConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	c.updates = append(c.updates, implementProgressUpdate{issueID: issueID, state: state})
	return nil
}

func (c *implementProgressConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *implementProgressConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (c *implementProgressConnector) HydratePullRequest(context.Context, connector.Issue) (connector.Issue, error) {
	c.hydrations++
	if c.hydrations <= len(c.hydrateErrs) && c.hydrateErrs[c.hydrations-1] != nil {
		return connector.Issue{}, c.hydrateErrs[c.hydrations-1]
	}
	if c.hydrateErr != nil {
		return connector.Issue{}, c.hydrateErr
	}
	return cloneIssue(c.hydrated), nil
}

func (c *implementProgressConnector) ReapplyPullRequestLabel(ctx context.Context, repository string, number int, label string, stagger time.Duration) error {
	relabel := autoPromoteTickRelabel{repository: repository, number: number, label: label, stagger: stagger}
	if c.relabelStarted != nil {
		c.relabelStarted <- relabel
	}
	if c.relabelRelease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.relabelRelease:
		}
	}
	return nil
}

type implementProgressAttemptStore struct {
	history       []store.WorkAttempt
	historyErr    error
	completionErr error
	heartbeatErr  error
	completions   []store.WorkAttemptCompletion
	starts        []store.WorkAttemptStart
	heartbeats    []store.WorkAttemptHeartbeat
	historyCalls  int
	queries       []store.WorkAttemptHistoryQuery
}

func (s *implementProgressAttemptStore) StartWorkAttempt(_ context.Context, attrs store.WorkAttemptStart) (int64, error) {
	s.starts = append(s.starts, attrs)
	return 1, nil
}

func (s *implementProgressAttemptStore) WorkAttempt(context.Context, int64) (store.WorkAttempt, error) {
	return store.WorkAttempt{}, store.ErrNotFound
}

func (s *implementProgressAttemptStore) RecordWorkAttemptHeartbeat(_ context.Context, attrs store.WorkAttemptHeartbeat) error {
	s.heartbeats = append(s.heartbeats, attrs)
	return s.heartbeatErr
}

func (s *implementProgressAttemptStore) CompleteWorkAttempt(_ context.Context, attrs store.WorkAttemptCompletion) error {
	s.completions = append(s.completions, attrs)
	return s.completionErr
}

func (s *implementProgressAttemptStore) ListActiveWorkAttempts(context.Context, store.WorkAttemptQuery) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *implementProgressAttemptStore) ListRecentTerminalWorkAttempts(_ context.Context, query store.WorkAttemptHistoryQuery) ([]store.WorkAttempt, error) {
	s.historyCalls++
	s.queries = append(s.queries, query)
	return append([]store.WorkAttempt(nil), s.history...), s.historyErr
}

func (s *implementProgressAttemptStore) TimeoutExpiredWorkAttempts(context.Context, store.WorkAttemptTimeout) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *implementProgressAttemptStore) ReclaimActiveWorkAttempts(context.Context, store.WorkAttemptReclaim) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *implementProgressAttemptStore) RecordSchedulerDecision(context.Context, store.SchedulerDecision) (int64, error) {
	return 0, nil
}

func (s *implementProgressAttemptStore) ListRecentSchedulerDecisions(context.Context, store.SchedulerDecisionQuery) ([]store.SchedulerDecision, error) {
	return nil, nil
}

func slicesEqual(left []string, right []string) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
