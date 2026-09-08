package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestMergingFastPathCurrentReadyPreservesCheckedHead(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 20, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		MergeFastPathEnabled:  true,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates: []string{"Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-clean-fast-path", []string{"enhancement"}, &connector.PullRequest{
		Number:         860,
		URL:            "https://github.test/digitaldrywood/detent/pull/860",
		BranchName:     "detent/detent-digitaldrywood_detent_860-030a2359de53",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-fast-path",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#860"
	issue.PRRepository = "digitaldrywood/detent"
	issue.BranchName = "detent/detent-digitaldrywood_detent_860-030a2359de53"

	workspaceBackend := &mergeFastPathWorkspace{
		info: workspace.Info{
			Path:   t.TempDir(),
			Key:    "digitaldrywood_detent_860",
			Branch: issue.BranchName,
		},
		result: workspace.MergePrepareResult{
			Status:   workspace.MergePrepareStatusClean,
			DiffStat: workspace.DiffStat{},
		},
	}
	agentBackend := &mergeFastPathAgentBackend{}
	runner, err := runpkg.NewRunner(runpkg.Dependencies{
		Workflow:     workflowconfig.Workflow{},
		Workspace:    workspaceBackend,
		AgentBackend: agentBackend,
		Now: func() time.Time {
			return now
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	previous := cloneIssue(issue)
	previous.ID = "previous-candidate"
	reservation := reserveMergeCandidate(&state, previous, now.Add(-mergeWorkerCurrentHeadCIWaitTimeout))
	reservation.RefreshHeadSHA = issue.PullRequest.HeadSHA
	state.mergeReservations[reservation.Repository] = reservation
	orch.dispatchReadyIssues(context.Background(), &state, []connector.Issue{issue}, now)

	completion := receiveMergeFastPathCompletion(t, orch.runResults)
	if completion.Err != nil {
		t.Fatalf("completion.Err = %v", completion.Err)
	}
	if completion.Request.Mode != runpkg.RunModeMerge {
		t.Fatalf("completion.Request.Mode = %q, want %q", completion.Request.Mode, runpkg.RunModeMerge)
	}
	orch.handleRunResult(context.Background(), &state, completion)

	if got := workspaceBackend.createCalls.Load(); got != 0 {
		t.Fatalf("Create() calls = %d, want 0", got)
	}
	if got := workspaceBackend.prepareCalls.Load(); got != 0 {
		t.Fatalf("PrepareMerge() calls = %d, want 0", got)
	}
	if got := workspaceBackend.afterRunCalls.Load(); got != 0 {
		t.Fatalf("AfterRun() calls = %d, want 0", got)
	}
	if got := agentBackend.calls.Load(); got != 0 {
		t.Fatalf("AgentBackend.RunTurn() calls = %d, want 0", got)
	}
	if got := tracker.merges; len(got) != 1 {
		t.Fatalf("merges = %#v, want one programmatic merge", got)
	}
	if got := tracker.merges[0]; got.repository != "digitaldrywood/detent" || got.number != 860 || got.headSHA != "head-fast-path" {
		t.Fatalf("merge request = %#v, want repository digitaldrywood/detent PR 860 head-fast-path", got)
	}
	if got := tracker.hydrations; !reflect.DeepEqual(got, []autoPromoteTickHydration{{
		issueID:    issue.ID,
		repository: "digitaldrywood/detent",
		number:     860,
	}}) {
		t.Fatalf("hydrations = %#v, want fresh PR hydration", got)
	}
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Done"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after clean merge fast-path", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after clean merge fast-path", issue.ID)
	}
	completed, ok := state.Completed[issue.ID]
	if !ok {
		t.Fatalf("Completed[%q] missing after clean merge fast-path", issue.ID)
	}
	if completed.FinalState != "Done" {
		t.Fatalf("Completed[%q].FinalState = %q, want Done", issue.ID, completed.FinalState)
	}
	if completed.Issue.PullRequest == nil || completed.Issue.PullRequest.State != "MERGED" {
		t.Fatalf("Completed[%q].Issue.PullRequest = %#v, want merged PR", issue.ID, completed.Issue.PullRequest)
	}
}

func TestMergingFastPathMissingRequiredChecksRoutesToRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 14, 13, 25, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-missing-required-checks", []string{"bug"}, &connector.PullRequest{
		Number:         862,
		URL:            "https://github.test/digitaldrywood/detent/pull/862",
		BranchName:     "detent/merge-fast-path-missing-checks",
		State:          "OPEN",
		MergeableState: "blocked",
		CIStatus:       "pending",
		HeadSHA:        "head-missing-required-checks",
		RequiredCheckFailures: []connector.PullRequestCheck{
			{Name: "Test", Status: "missing", Conclusion: "missing"},
			{Name: "Checks", Status: "missing", Conclusion: "missing"},
		},
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#862"
	issue.PRRepository = "digitaldrywood/detent"
	issue.BranchName = "detent/merge-fast-path-missing-checks"

	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    maxMergeWorkerRunnerFailures,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     runpkg.RunOutputMergeFastPathClean,
		},
	})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none with missing required checks", tracker.merges)
	}
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "Test, Checks") {
		t.Fatalf("comments = %#v, want missing required check names", tracker.comments)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after Rework handoff", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after Rework handoff", issue.ID)
	}
}

func TestMergingFastPathUnresolvedReviewThreadsRouteToRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 4, 13, 5, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-unresolved-review-threads", []string{"bug"}, &connector.PullRequest{
		Number:         2124,
		URL:            "https://github.test/digitaldrywood/detent/pull/2124",
		BranchName:     "detent/unresolved-review-threads",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-unresolved-review-threads",
		UnresolvedReviewThreads: []connector.PullRequestReviewThread{
			{Path: "internal/orchestrator/autopromote_tick.go", Line: 126},
			{Path: "internal/orchestrator/run_completion.go", Line: 1582},
		},
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#2103"
	issue.PRRepository = "digitaldrywood/detent"

	tracker, state := completeMergeFastPathTestRun(t, issue, now, 1, gate.Config{})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none with unresolved review threads", tracker.merges)
	}
	if got, want := tracker.reviewThreadHydrations, []string{issue.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("review thread hydrations = %#v, want %#v", got, want)
	}
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one Rework routing comment", tracker.comments)
	}
	for _, fragment := range []string{
		"reason: unresolved_review_threads",
		"unresolved_review_threads: 2",
		"first_unresolved_review_thread: internal/orchestrator/autopromote_tick.go:126",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment = %q, want fragment %q", tracker.comments[0].body, fragment)
		}
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after Rework handoff", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after Rework handoff", issue.ID)
	}
}

func TestMergingFastPathChangedHeadReappliesCITriggerBeforeMerge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 9, 15, 0, 0, time.UTC)
	staggerSeconds := 15
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
		AutoPromote: AutoPromoteConfig{Gate: gate.Config{
			Kind:                         gate.KindCommand,
			RequiredStatusChecks:         []string{"Test", "Checks"},
			CITriggerLabel:               "ci:ready",
			CITriggerLabelStaggerSeconds: &staggerSeconds,
		}},
	})
	issue := autoPromoteTickIssue("issue-changed-head", []string{"bug"}, &connector.PullRequest{
		Number:         867,
		URL:            "https://github.test/digitaldrywood/detent/pull/867",
		BranchName:     "detent/changed-head",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "new-head",
		Labels:         []string{},
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#867"
	issue.PRRepository = "digitaldrywood/detent"

	relabelStarted := make(chan autoPromoteTickRelabel, 1)
	relabelRelease := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case relabelRelease <- struct{}{}:
		default:
		}
	})
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
		relabelStarted:           relabelStarted,
		relabelRelease:           relabelRelease,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState:              runpkg.FinalStateCompleted,
			Output:                  runpkg.RunOutputMergeFastPathClean,
			PullRequestHeadPushed:   true,
			CITriggerLabelReapplied: false,
		},
	})

	select {
	case got := <-relabelStarted:
		want := autoPromoteTickRelabel{repository: "digitaldrywood/detent", number: 867, label: "ci:ready", stagger: 15 * time.Second}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("relabel = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for changed-head trigger-label reapplication")
	}
	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none before changed-head checks run", tracker.merges)
	}
	if _, ok := state.Retry[issue.ID]; !ok {
		t.Fatalf("Retry[%q] missing while changed-head checks propagate", issue.ID)
	}
}

func TestMergingFallbackPushWaitsAfterWorkerReappliesCITriggerWhenHydrationIsGreen(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 10, 15, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-fallback-pushed-head", []string{"bug"}, &connector.PullRequest{
		Number:         868,
		URL:            "https://github.test/digitaldrywood/detent/pull/868",
		BranchName:     "detent/fallback-pushed-head",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "new-fallback-head",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#868"
	issue.PRRepository = "digitaldrywood/detent"

	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState:              runpkg.FinalStateCompleted,
			PullRequestHeadPushed:   true,
			CITriggerLabelReapplied: true,
		},
	})

	if len(tracker.relabels) != 0 {
		t.Fatalf("relabels = %#v, want no duplicate after worker reapplication", tracker.relabels)
	}
	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none while current-head checks run", tracker.merges)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no lane transition while current-head checks run", tracker.updates)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing while current-head checks run", issue.ID)
	}
	if retry.Attempt != 1 {
		t.Fatalf("Retry[%q].Attempt = %d, want unchanged attempt 1", issue.ID, retry.Attempt)
	}
	if !strings.Contains(retry.Error, "current-head CI") {
		t.Fatalf("Retry[%q].Error = %q, want current-head CI wait", issue.ID, retry.Error)
	}
}

func TestMergingFastPathMissingRequiredChecksPrecedeUnknownMergeability(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 14, 13, 25, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-propagating-required-checks", []string{"bug"}, &connector.PullRequest{
		Number:         864,
		URL:            "https://github.test/digitaldrywood/detent/pull/864",
		BranchName:     "detent/merge-fast-path-propagating-checks",
		State:          "OPEN",
		MergeableState: "unknown",
		CIStatus:       "success",
		HeadSHA:        "head-propagating-required-checks",
		RequiredCheckFailures: []connector.PullRequestCheck{
			{Name: "Test", Status: "missing", Conclusion: "missing"},
		},
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#864"
	issue.PRRepository = "digitaldrywood/detent"

	staggerSeconds := 0
	tracker, state := completeMergeFastPathTestRun(t, issue, now, 1, gate.Config{
		Kind:                         gate.KindCommand,
		CITriggerLabel:               "ci:ready",
		CITriggerLabelStaggerSeconds: &staggerSeconds,
	})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none while required checks propagate", tracker.merges)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no state transition while required checks propagate", tracker.updates)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing while required checks propagate", issue.ID)
	}
	if retry.Attempt != 2 {
		t.Fatalf("Retry[%q].Attempt = %d, want 2", issue.ID, retry.Attempt)
	}
	if !strings.Contains(retry.Error, "required checks") {
		t.Fatalf("Retry[%q].Error = %q, want required-check propagation wait", issue.ID, retry.Error)
	}
	if strings.Contains(retry.Error, "mergeability") {
		t.Fatalf("Retry[%q].Error = %q, want required-check handling to take precedence", issue.ID, retry.Error)
	}
}

func TestMergingFastPathMainAdvanceRetriggersMissingCurrentHeadChecks(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 13, 25, 0, 0, time.UTC)
	staggerSeconds := 15
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
		AutoPromote: AutoPromoteConfig{Gate: gate.Config{
			Kind:                         gate.KindCommand,
			RequiredStatusChecks:         []string{"Test", "Checks"},
			CITriggerLabel:               "ci:ready",
			CITriggerLabelStaggerSeconds: &staggerSeconds,
		}},
	})
	issue := autoPromoteTickIssue("issue-sibling-blocked-after-main-advance", []string{"bug"}, &connector.PullRequest{
		Number:         866,
		URL:            "https://github.test/digitaldrywood/detent/pull/866",
		BranchName:     "detent/sibling-blocked-after-main-advance",
		State:          "OPEN",
		MergeableState: "blocked",
		CIStatus:       "pending",
		HeadSHA:        "head-after-main-advance",
		BaseSHA:        "advanced-main",
		RequiredCheckFailures: []connector.PullRequestCheck{
			{Name: "Test", Status: "missing", Conclusion: "missing"},
			{Name: "Checks", Status: "missing", Conclusion: "missing"},
		},
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#866"
	issue.PRRepository = "digitaldrywood/detent"

	relabelStarted := make(chan autoPromoteTickRelabel, 2)
	relabelRelease := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case relabelRelease <- struct{}{}:
		default:
		}
	})
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
		relabelStarted:           relabelStarted,
		relabelRelease:           relabelRelease,
	}
	var logs mergeFastPathLockedBuffer
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     runpkg.RunOutputMergeFastPathClean,
		},
	})

	want := autoPromoteTickRelabel{
		repository: "digitaldrywood/detent",
		number:     866,
		label:      "ci:ready",
		stagger:    15 * time.Second,
	}
	select {
	case got := <-relabelStarted:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("relabel = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trigger-label relabel to start")
	}
	if !strings.Contains(logs.String(), "ci_trigger_label_scheduled") {
		t.Fatalf("logs = %q, want scheduled relabel decision", logs.String())
	}
	if _, ok := state.Retry[issue.ID]; !ok {
		t.Fatalf("Retry[%q] missing while retriggered checks propagate", issue.ID)
	}
	if !orch.scheduleCITriggerLabel(context.Background(), issue, []string{"Test", "Checks"}, 2, false, false) {
		t.Fatal("second scheduleCITriggerLabel() = false, want pending relabel")
	}
	if !strings.Contains(logs.String(), "reapply_pending_for_head") {
		t.Fatalf("logs = %q, want same-head pending decision", logs.String())
	}
	select {
	case got := <-relabelStarted:
		t.Fatalf("unexpected duplicate relabel while first is pending: %#v", got)
	default:
	}
	relabelRelease <- struct{}{}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !strings.Contains(logs.String(), "ci_trigger_label_reapplied") {
		select {
		case <-deadline.C:
			t.Fatalf("logs = %q, want applied relabel decision", logs.String())
		case <-ticker.C:
		}
	}
	if orch.scheduleCITriggerLabel(context.Background(), issue, []string{"Test", "Checks"}, 2, false, false) {
		t.Fatal("third scheduleCITriggerLabel() = true, want same-head skip")
	}
	if !strings.Contains(logs.String(), "ci_trigger_label_skipped") || !strings.Contains(logs.String(), "already_reapplied_for_head") {
		t.Fatalf("logs = %q, want same-head skipped decision", logs.String())
	}
	select {
	case got := <-relabelStarted:
		t.Fatalf("unexpected duplicate relabel after first completed: %#v", got)
	default:
	}
}

type mergeFastPathLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *mergeFastPathLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *mergeFastPathLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func TestMergingFastPathHydrationUnavailableDefers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 14, 13, 25, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-fast-path-hydration-unavailable", []string{"bug"}, &connector.PullRequest{
		Number:                     865,
		URL:                        "https://github.test/digitaldrywood/detent/pull/865",
		BranchName:                 "detent/merge-fast-path-hydration-unavailable",
		State:                      "OPEN",
		MergeableState:             "behind",
		CIStatus:                   "success",
		HeadSHA:                    "head-hydration-unavailable",
		HydrationUnavailableReason: connector.PullRequestHydrationReasonRESTBudgetReserved,
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#865"
	issue.PRRepository = "digitaldrywood/detent"

	tracker, state := completeMergeFastPathTestRun(t, issue, now, 2, gate.Config{})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none without fresh pull request hydration", tracker.merges)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no state transition without fresh pull request hydration", tracker.updates)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing without fresh pull request hydration", issue.ID)
	}
	if retry.Attempt != 2 {
		t.Fatalf("Retry[%q].Attempt = %d, want unchanged attempt 2", issue.ID, retry.Attempt)
	}
	if !strings.Contains(retry.Error, "pull request hydration") {
		t.Fatalf("Retry[%q].Error = %q, want pull request hydration wait", issue.ID, retry.Error)
	}
}

func completeMergeFastPathTestRun(
	t *testing.T,
	issue connector.Issue,
	now time.Time,
	attempt int,
	gateConfig gate.Config,
	ciWaitAge ...time.Duration,
) (*autoPromoteTickMergeConnector, State) {
	t.Helper()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
		AutoPromote:            AutoPromoteConfig{Gate: gateConfig},
	})
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	if len(ciWaitAge) > 0 {
		state.MergeTimings[issue.ID] = MergeTiming{CIWaitStartedAt: now.Add(-ciWaitAge[0])}
	}
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    attempt,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}
	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     runpkg.RunOutputMergeFastPathClean,
		},
	})
	return tracker, state
}

func TestMergingFastPathWaitsForMergeabilityComputation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mergeable string
	}{
		{name: "unknown", mergeable: "unknown"},
		{name: "empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 7, 24, 13, 25, 0, 0, time.UTC)
			issue := autoPromoteTickIssue("issue-mergeability-"+tt.name, []string{"bug"}, &connector.PullRequest{
				Number:         869,
				URL:            "https://github.test/digitaldrywood/detent/pull/869",
				BranchName:     "detent/mergeability-" + tt.name,
				State:          "OPEN",
				MergeableState: tt.mergeable,
				CIStatus:       "success",
				HeadSHA:        "head-mergeability-" + tt.name,
			})
			issue.State = "Merging"
			issue.Identifier = "digitaldrywood/detent#869"
			issue.PRRepository = "digitaldrywood/detent"

			tracker, state := completeMergeFastPathTestRun(t, issue, now, 2, gate.Config{})

			if len(tracker.merges) != 0 {
				t.Fatalf("merges = %#v, want none while GitHub computes mergeability", tracker.merges)
			}
			if len(tracker.updates) != 0 {
				t.Fatalf("updates = %#v, want no lane transition while GitHub computes mergeability", tracker.updates)
			}
			retry, ok := state.Retry[issue.ID]
			if !ok {
				t.Fatalf("Retry[%q] missing while GitHub computes mergeability", issue.ID)
			}
			if retry.Attempt != 2 || retry.WorkerHost != "worker-a" {
				t.Fatalf("Retry[%q] = %#v, want same-attempt retry on worker-a", issue.ID, retry)
			}
			if !strings.Contains(retry.Error, "mergeability computation") {
				t.Fatalf("Retry[%q].Error = %q, want mergeability computation reason", issue.ID, retry.Error)
			}
			if !retry.DueAt.Equal(now.Add(5 * time.Second)) {
				t.Fatalf("Retry[%q].DueAt = %s, want continuation retry delay", issue.ID, retry.DueAt)
			}
		})
	}
}

func TestMergingFastPathCleanPrecheckWaitsForCurrentHeadCI(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 25, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		FailureRetryBaseDelay:  time.Minute,
		MaxRetryBackoff:        time.Hour,
		ContinuationRetryDelay: 5 * time.Second,
		MergeFastPathEnabled:   true,
		ActiveStates:           []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-clean-fast-path-ci", []string{"enhancement"}, &connector.PullRequest{
		Number:         861,
		URL:            "https://github.test/digitaldrywood/detent/pull/861",
		BranchName:     "detent/merge-fast-path-ci",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "pending",
		HeadSHA:        "head-fast-path-ci",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/detent#861"
	issue.PRRepository = "digitaldrywood/detent"
	issue.BranchName = "detent/merge-fast-path-ci"

	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
		Mode:       runpkg.RunModeMerge,
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeMerge},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     runpkg.RunOutputMergeFastPathClean,
		},
	})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none while CI is pending", tracker.merges)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no state transition while CI is pending", tracker.updates)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after fast-path CI wait", issue.ID)
	}
	if _, ok := state.Completed[issue.ID]; ok {
		t.Fatalf("Completed[%q] present while CI is pending", issue.ID)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing while CI is pending", issue.ID)
	}
	if retry.Attempt != 1 || retry.WorkerHost != "worker-a" {
		t.Fatalf("Retry[%q] = %#v, want unchanged attempt 1 wait on worker-a", issue.ID, retry)
	}
	if !strings.Contains(retry.Error, "current-head CI") {
		t.Fatalf("Retry[%q].Error = %q, want current-head CI wait", issue.ID, retry.Error)
	}
	if !retry.DueAt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("Retry[%q].DueAt = %s, want continuation retry delay", issue.ID, retry.DueAt)
	}
	if retry.Wait.Kind != retryWaitCurrentHeadCI || retry.Wait.PollCount != 1 {
		t.Fatalf("Retry[%q].Wait = %#v, want first current-head CI poll", issue.ID, retry.Wait)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] retained while waiting for CI", issue.ID)
	}
}

func TestMergeWorkerCurrentHeadCIRetryBound(t *testing.T) {
	t.Parallel()

	const pendingCheck = "Portability Verify (windows-latest)"
	tests := []struct {
		name          string
		attempt       int
		ciStatus      string
		mergeable     string
		runningChecks []string
		failures      []connector.PullRequestCheck
		ciWaitAge     time.Duration
		wantRetry     int
		wantBlocked   bool
		wantMerged    bool
	}{
		{
			name:          "attempt stays fixed across waits",
			attempt:       1,
			ciStatus:      "pending",
			mergeable:     "blocked",
			runningChecks: []string{pendingCheck},
			failures: []connector.PullRequestCheck{{
				Name:   pendingCheck,
				Status: "in_progress",
			}},
			ciWaitAge: time.Minute,
			wantRetry: 1,
		},
		{
			name:          "later wait keeps attempt fixed",
			attempt:       2,
			ciStatus:      "pending",
			mergeable:     "blocked",
			runningChecks: []string{pendingCheck},
			failures: []connector.PullRequestCheck{{
				Name:   pendingCheck,
				Status: "in_progress",
			}},
			ciWaitAge: 30 * time.Minute,
			wantRetry: 2,
		},
		{
			name:          "pending check escalates at bound",
			attempt:       maxMergeWorkerRunnerFailures,
			ciStatus:      "pending",
			mergeable:     "blocked",
			runningChecks: []string{pendingCheck},
			failures: []connector.PullRequestCheck{{
				Name:   pendingCheck,
				Status: "in_progress",
			}},
			ciWaitAge:   mergeWorkerCurrentHeadCIWaitTimeout,
			wantBlocked: true,
		},
		{
			name:          "pending check remains retryable before elapsed bound",
			attempt:       maxMergeWorkerRunnerFailures,
			ciStatus:      "pending",
			mergeable:     "blocked",
			runningChecks: []string{pendingCheck},
			failures: []connector.PullRequestCheck{{
				Name:   pendingCheck,
				Status: "in_progress",
			}},
			ciWaitAge: mergeWorkerCurrentHeadCIWaitTimeout - time.Second,
			wantRetry: maxMergeWorkerRunnerFailures,
		},
		{
			name:       "green check before bound merges",
			attempt:    maxMergeWorkerRunnerFailures,
			ciStatus:   "success",
			mergeable:  "clean",
			ciWaitAge:  mergeWorkerCurrentHeadCIWaitTimeout - time.Second,
			wantMerged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 8, 7, 22, 14, 45, 0, time.UTC)
			issue := autoPromoteTickIssue("issue-current-head-ci-"+strings.ReplaceAll(tt.name, " ", "-"), []string{"bug"}, &connector.PullRequest{
				Number:                1636,
				URL:                   "https://github.test/digitaldrywood/detent/pull/1636",
				State:                 "OPEN",
				MergeableState:        tt.mergeable,
				CIStatus:              tt.ciStatus,
				HeadSHA:               "29869e149edf099f34e92136199c5ee45056fddf",
				RunningChecks:         append([]string(nil), tt.runningChecks...),
				RequiredCheckFailures: append([]connector.PullRequestCheck(nil), tt.failures...),
			})
			issue.State = "Merging"
			issue.Identifier = "digitaldrywood/detent#1634"
			issue.PRRepository = "digitaldrywood/detent"

			tracker, state := completeMergeFastPathTestRun(t, issue, now, tt.attempt, gate.Config{
				RequiredStatusChecks: []string{pendingCheck},
			}, tt.ciWaitAge)

			if tt.wantRetry > 0 {
				retry, ok := state.Retry[issue.ID]
				if !ok {
					t.Fatalf("Retry[%q] missing", issue.ID)
				}
				if retry.Attempt != tt.wantRetry {
					t.Fatalf("Retry[%q].Attempt = %d, want %d", issue.ID, retry.Attempt, tt.wantRetry)
				}
				return
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after terminal disposition", issue.ID)
			}
			if tt.wantBlocked {
				blocked, ok := state.Blocked[issue.ID]
				if !ok {
					t.Fatalf("Blocked[%q] missing", issue.ID)
				}
				if !strings.Contains(blocked.Reason, pendingCheck) {
					t.Fatalf("Blocked[%q].Reason = %q, want pending check %q", issue.ID, blocked.Reason, pendingCheck)
				}
				if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, pendingCheck) {
					t.Fatalf("comments = %#v, want pending check %q", tracker.comments, pendingCheck)
				}
				return
			}
			if tt.wantMerged && len(tracker.merges) != 1 {
				t.Fatalf("merges = %#v, want one merge", tracker.merges)
			}
		})
	}
}

func TestMergeWorkerProgrammaticMergeDisposition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		state       string
		ci          string
		running     []string
		wantReady   bool
		wantPending bool
		wantWait    bool
	}{
		{name: "clean green", state: "clean", ci: "success", wantReady: true},
		{name: "behind green requires integration", state: "behind", ci: "success"},
		{name: "behind pending", state: "behind", ci: "pending", wantWait: true},
		{name: "unknown green", state: "unknown", ci: "success", wantPending: true},
		{name: "empty green", ci: "success", wantPending: true},
		{name: "unknown pending", state: "unknown", ci: "pending", wantWait: true},
		{name: "unknown failed", state: "unknown", ci: "failure"},
		{name: "blocked running", state: "blocked", ci: "pending", running: []string{"Test"}, wantWait: true},
		{name: "blocked without running checks", state: "blocked", ci: "pending"},
		{name: "dirty", state: "dirty", ci: "success"},
		{name: "failed", state: "clean", ci: "failure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := connector.Issue{
				ID:           "issue-disposition",
				PRRepository: "digitaldrywood/detent",
				PullRequest: &connector.PullRequest{
					Number:         863,
					State:          "open",
					MergeableState: tt.state,
					CIStatus:       tt.ci,
					HeadSHA:        "head-disposition",
					RunningChecks:  tt.running,
				},
			}
			if got := mergeWorkerProgrammaticMergeReady(issue); got != tt.wantReady {
				t.Fatalf("mergeWorkerProgrammaticMergeReady() = %t, want %t", got, tt.wantReady)
			}
			if got := mergeWorkerMergeabilityPending(issue); got != tt.wantPending {
				t.Fatalf("mergeWorkerMergeabilityPending() = %t, want %t", got, tt.wantPending)
			}
			if got := mergeWorkerProgrammaticMergeWaiting(issue); got != tt.wantWait {
				t.Fatalf("mergeWorkerProgrammaticMergeWaiting() = %t, want %t", got, tt.wantWait)
			}
		})
	}
}

func receiveMergeFastPathCompletion(t *testing.T, completions <-chan runpkg.Completion) runpkg.Completion {
	t.Helper()

	select {
	case completion := <-completions:
		return completion
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for merge fast-path completion")
	}
	return runpkg.Completion{}
}

type mergeFastPathWorkspace struct {
	info          workspace.Info
	result        workspace.MergePrepareResult
	createCalls   atomic.Int64
	prepareCalls  atomic.Int64
	afterRunCalls atomic.Int64
}

func (w *mergeFastPathWorkspace) Create(context.Context, workspace.Issue) (workspace.Info, error) {
	w.createCalls.Add(1)
	return w.info, nil
}

func (w *mergeFastPathWorkspace) Cleanup(context.Context, string) error {
	return nil
}

func (w *mergeFastPathWorkspace) BeforeRun(context.Context, workspace.Info, workspace.Issue) error {
	return nil
}

func (w *mergeFastPathWorkspace) AfterRun(context.Context, workspace.Info, workspace.Issue) {
	w.afterRunCalls.Add(1)
}

func (w *mergeFastPathWorkspace) DiffStat(context.Context, workspace.Info, workspace.Issue) (workspace.DiffStat, error) {
	return w.result.DiffStat, nil
}

func (w *mergeFastPathWorkspace) PrepareMerge(context.Context, workspace.Info, workspace.Issue, workspace.MergePrepareOptions) (workspace.MergePrepareResult, error) {
	w.prepareCalls.Add(1)
	return w.result, nil
}

type mergeFastPathAgentBackend struct {
	calls atomic.Int64
}

func (b *mergeFastPathAgentBackend) RunTurn(context.Context, runpkg.AgentTurnRequest, runpkg.AgentUpdateHandler) (runpkg.AgentTurnResult, error) {
	b.calls.Add(1)
	return runpkg.AgentTurnResult{}, errors.New("agent backend should not run during clean merge fast-path")
}
