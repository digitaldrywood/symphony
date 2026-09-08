package orchestrator_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const slowCIIntegrationWaitTimeout = 10 * time.Second

func TestRunDispatchesCandidateAndRecordsCompletion(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-1", "digitaldrywood/detent#10", "Todo")
	tracker := newFakeConnector(issue)
	runner := &staticRunner{
		result: orchestrator.RunResult{
			Tokens: orchestrator.TokenTotals{
				InputTokens:    100,
				OutputTokens:   25,
				TotalTokens:    125,
				RuntimeSeconds: 1.5,
			},
			DiffStats: orchestrator.DiffStats{
				FilesChanged: 2,
				AddedLines:   4,
				RemovedLines: 1,
				Status:       "ok",
			},
			RateLimits: &telemetry.RateLimits{
				LimitID:   "codex",
				LimitName: "Codex",
			},
			FinalState: "Human Review",
		},
	}

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, completed := state.Completed[issue.ID]
		return completed
	})

	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("runner calls = %d, want 1", got)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after completed run", issue.ID)
	}
	if got := state.Completed[issue.ID].FinalState; got != "Human Review" {
		t.Fatalf("Completed[%q].FinalState = %q, want Human Review", issue.ID, got)
	}
	if got := state.Retry[issue.ID].Attempt; got != 1 {
		t.Fatalf("Retry[%q].Attempt = %d, want 1", issue.ID, got)
	}
	if got := state.TokenTotals.TotalTokens; got != 125 {
		t.Fatalf("TokenTotals.TotalTokens = %d, want 125", got)
	}
	if got := state.DiffStats[issue.ID].AddedLines; got != 4 {
		t.Fatalf("DiffStats[%q].AddedLines = %d, want 4", issue.ID, got)
	}
	if state.RateLimits == nil || state.RateLimits.LimitID != "codex" {
		t.Fatalf("RateLimits = %#v, want codex rate limit", state.RateLimits)
	}
	if got := tracker.fetchCandidateCalls(); got == 0 {
		t.Fatal("FetchCandidateIssues() calls = 0, want at least 1")
	}
}

func TestRunRefreshesStatusDrift(t *testing.T) {
	t.Parallel()

	tracker := &statusDriftConnector{
		fakeConnector: newFakeConnector(),
		drift: connector.StatusDrift{
			UntrackedOpen: []connector.Issue{{
				ID:         "I_771",
				Identifier: "digitaldrywood/detent#771",
				Title:      "Untracked issue",
			}},
			OpenTerminal: []connector.Issue{{
				ID:         "I_583",
				Identifier: "digitaldrywood/detent#583",
				Title:      "Done but open",
				State:      "Done",
			}},
		},
	}
	orch := newTestOrchestrator(t, tracker, &staticRunner{})
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return len(state.StatusDrift.UntrackedOpen) == 1 && len(state.StatusDrift.OpenTerminal) == 1
	})

	if state.StatusDrift.UntrackedOpen[0].Identifier != "digitaldrywood/detent#771" {
		t.Fatalf("UntrackedOpen = %#v, want #771", state.StatusDrift.UntrackedOpen)
	}
	if state.StatusDrift.OpenTerminal[0].State != "Done" {
		t.Fatalf("OpenTerminal = %#v, want Done issue", state.StatusDrift.OpenTerminal)
	}
}

func TestRunCompletionTransitionsLabelModeIssueToHumanReview(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-label-complete", "digitaldrywood/detent#638", "Todo")
	issue.Labels = []string{"detent:todo", "bug"}
	tracker := newFakeLabelConnector(issue)
	runner := &staticRunner{
		result: orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted},
		onRun: func(request orchestrator.RunRequest) {
			tracker.setCandidatePullRequest(request.Issue.ID, &connector.PullRequest{
				Number:         17,
				URL:            "https://github.com/digitaldrywood/detent/pull/17",
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "success",
			})
		},
	}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress", "Rework"},
		ObservedStates:         []string{"Human Review"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForStateUpdate(t, tracker, stateUpdateCall{issueID: issue.ID, state: "Human Review"})
	wantUpdates := []stateUpdateCall{
		{issueID: issue.ID, state: "In Progress"},
		{issueID: issue.ID, state: "Human Review"},
	}
	if updates := tracker.stateUpdateCalls(); !stateUpdateCallsContainInOrder(updates, wantUpdates) {
		t.Fatalf("state updates = %#v, want sequence %#v", updates, wantUpdates)
	}

	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("runner calls = %d, want 1", got)
	}
	got := tracker.candidateIssue(issue.ID)
	if got.State != "Human Review" {
		t.Fatalf("State = %q, want Human Review", got.State)
	}
	if count := statusLabelCount(got.Labels, "detent:"); count != 1 {
		t.Fatalf("detent status label count = %d in %#v, want 1", count, got.Labels)
	}
	if !labelListContains(got.Labels, "detent:human-review") {
		t.Fatalf("Labels = %#v, want detent:human-review", got.Labels)
	}
}

func TestRunCompletionPromotesLabelModeWorkpadDirectlyToMerging(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-label-autopilot", "digitaldrywood/detent#1288", "Todo")
	issue.Labels = []string{"detent:todo", "bug"}
	tracker := newFakeLabelConnector(issue)
	activityAt := time.Now().Add(-time.Hour)
	runner := &staticRunner{
		result: orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted},
		onRun: func(request orchestrator.RunRequest) {
			tracker.setCandidatePullRequest(request.Issue.ID, &connector.PullRequest{
				Number:         1288,
				URL:            "https://github.com/digitaldrywood/detent/pull/1288",
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "success",
				ActivityAt:     &activityAt,
			})
			tracker.setIssueComments(request.Issue.ID, []connector.IssueComment{{
				Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
			}})
		},
	}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:          5 * time.Millisecond,
		MaxConcurrentAgents:   1,
		MaxRetryBackoff:       time.Hour,
		FailureRetryBaseDelay: time.Hour,
		AutoPromote: orchestrator.AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			GateWaitState: "source",
			OptoutLabel:   "requires-human-review",
			Gate: gate.Config{
				Kind:                   gate.KindCommand,
				RequireAutomatedReview: new(false),
			},
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework"},
		ObservedStates:         []string{"Human Review", "Merging"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForStateUpdate(t, tracker, stateUpdateCall{issueID: issue.ID, state: "Merging"})
	updates := tracker.stateUpdateCalls()
	wantUpdates := []stateUpdateCall{
		{issueID: issue.ID, state: "In Progress"},
		{issueID: issue.ID, state: "Merging"},
	}
	if !stateUpdateCallsContainInOrder(updates, wantUpdates) {
		t.Fatalf("state updates = %#v, want sequence %#v", updates, wantUpdates)
	}
	for _, update := range updates {
		if update.issueID == issue.ID && update.state == "Human Review" {
			t.Fatalf("state updates = %#v, want no Human Review transition", updates)
		}
	}

	got := tracker.candidateIssue(issue.ID)
	if got.State != "Merging" {
		t.Fatalf("State = %q, want Merging", got.State)
	}
	if count := statusLabelCount(got.Labels, "detent:"); count != 1 {
		t.Fatalf("detent status label count = %d in %#v, want 1", count, got.Labels)
	}
	if !labelListContains(got.Labels, "detent:merging") {
		t.Fatalf("Labels = %#v, want detent:merging", got.Labels)
	}
}

func TestRunStartsLabelModeTodoIssueInProgress(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-label-start", "digitaldrywood/detent#669", "Todo")
	issue.Labels = []string{"detent:todo", "bug"}
	tracker := newFakeLabelConnector(issue)
	runner := newBlockingRunner()

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()
	defer close(runner.release)

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Issue.State != "In Progress" {
		t.Fatalf("RunRequest.Issue.State = %q, want In Progress", request.Issue.State)
	}
	if request.DispatchSourceState != "Todo" || request.DispatchTargetState != "In Progress" {
		t.Fatalf("RunRequest dispatch transition = %q -> %q, want Todo -> In Progress", request.DispatchSourceState, request.DispatchTargetState)
	}

	waitForStateUpdate(t, tracker, stateUpdateCall{issueID: issue.ID, state: "In Progress"})

	got := tracker.candidateIssue(issue.ID)
	if got.State != "In Progress" {
		t.Fatalf("State = %q, want In Progress", got.State)
	}
	if count := statusLabelCount(got.Labels, "detent:"); count != 1 {
		t.Fatalf("detent status label count = %d in %#v, want 1", count, got.Labels)
	}
	if !labelListContains(got.Labels, "detent:in-progress") {
		t.Fatalf("Labels = %#v, want detent:in-progress", got.Labels)
	}
}

func TestRunRecordsLaneTransitionMetrics(t *testing.T) {
	t.Parallel()

	stageAt := time.Now().Add(-2 * time.Hour)
	issue := testIssue("issue-metrics-start", "digitaldrywood/detent#722", "Todo")
	issue.StageUpdatedAt = &stageAt
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()
	metrics := &workflowMetricsRecorder{}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		Project:                scheduler.ProjectCandidate{ID: "detent", Weight: 1},
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          runner,
		WorkflowMetrics: metrics,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	defer close(runner.release)

	receiveRunRequest(t, runner.started)
	waitForStateUpdate(t, tracker, stateUpdateCall{issueID: issue.ID, state: "In Progress"})
	events := waitForWorkflowMetricEvents(t, metrics, 2)

	if events[0].ProjectID != "detent" || events[0].IssueID != issue.ID || events[0].Identifier != issue.Identifier {
		t.Fatalf("exit event identity = %#v", events[0])
	}
	if events[0].PhaseType != store.WorkflowPhaseTypeLane || events[0].PhaseName != "Todo" || events[0].Status != "exited" {
		t.Fatalf("exit event phase = %#v, want Todo exited lane", events[0])
	}
	if !events[0].StartedAt.Equal(stageAt) || events[0].FinishedAt.IsZero() || events[0].DurationSeconds <= 0 {
		t.Fatalf("exit event timing = %#v, want duration from stage timestamp", events[0])
	}
	if events[1].PhaseType != store.WorkflowPhaseTypeLane || events[1].PhaseName != "In Progress" || events[1].PreviousPhaseName != "Todo" || events[1].Status != "entered" {
		t.Fatalf("enter event phase = %#v, want In Progress entered from Todo", events[1])
	}
}

func TestRunPlanDispatchPostsPlanAndMovesToPlanReview(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-plan", "digitaldrywood/detent#521", "Todo")
	tracker := newFakeConnector(issue)
	runner := &staticRunner{
		result: orchestrator.RunResult{
			FinalState: orchestrator.FinalStateCompleted,
			Output:     "## Plan\n- Add focused tests\n- Implement the plan stop\n",
		},
	}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 1,
		Plan: gate.PlanConfig{
			Enabled: true,
			Review:  gate.PlanReviewHuman,
			Stop:    gate.DefaultPlanStop,
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework"},
		ObservedStates:         []string{"Backlog", "Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		completed, ok := state.Completed[issue.ID]
		return ok && completed.FinalState == gate.DefaultPlanStop
	})

	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after plan completion", issue.ID)
	}
	comments := tracker.commentCalls()
	if len(comments) != 1 {
		t.Fatalf("comments = %#v, want one plan artifact", comments)
	}
	for _, want := range []string{
		"## Detent Plan",
		"digitaldrywood/detent#521",
		"## Plan\n- Add focused tests",
	} {
		if !strings.Contains(comments[0].body, want) {
			t.Fatalf("plan comment %q missing %q", comments[0].body, want)
		}
	}
	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[len(updates)-1].state != gate.DefaultPlanStop {
		t.Fatalf("state updates = %#v, want final Plan Review", updates)
	}
	requests := runner.requests()
	if len(requests) != 1 || requests[0].Mode != orchestrator.RunModePlan {
		t.Fatalf("runner requests = %#v, want one plan-mode request", requests)
	}
}

func TestTickPlanReviewGateTransitionsIssues(t *testing.T) {
	t.Parallel()

	finding := connector.PullRequestFinding{
		Body: "Plan omits acceptance criteria.",
		URL:  "https://github.test/comment/plan-review",
	}

	tests := []struct {
		name                 string
		cfg                  gate.PlanConfig
		issue                connector.Issue
		wantUpdates          []stateUpdateCall
		wantCommentFragments []string
	}{
		{
			name: "human approval advances to implementation",
			cfg:  gate.PlanConfig{Enabled: true, Review: gate.PlanReviewHuman, Stop: gate.DefaultPlanStop},
			issue: func() connector.Issue {
				issue := testIssue("issue-human-plan", "digitaldrywood/detent#522", gate.DefaultPlanStop)
				issue.Labels = []string{"plan-approved"}
				return issue
			}(),
			wantUpdates: []stateUpdateCall{{issueID: "issue-human-plan", state: "In Progress"}},
		},
		{
			name: "automated comment approval advances without pull request",
			cfg:  gate.PlanConfig{Enabled: true, Review: gate.PlanReviewAutomated, Stop: gate.DefaultPlanStop},
			issue: func() connector.Issue {
				issue := testIssue("issue-automated-comment-plan", "digitaldrywood/detent#524", gate.DefaultPlanStop)
				issue.Comments = []connector.IssueComment{{
					Body: "## Detent Plan Review\n\n- state: approved\n\nThe plan satisfies the acceptance criteria.",
					URL:  "https://github.test/comment/plan-review-approved",
				}}
				return issue
			}(),
			wantUpdates: []stateUpdateCall{{issueID: "issue-automated-comment-plan", state: "In Progress"}},
		},
		{
			name: "automated p1 routes to rework with feedback",
			cfg:  gate.PlanConfig{Enabled: true, Review: gate.PlanReviewAutomated, Stop: gate.DefaultPlanStop},
			issue: func() connector.Issue {
				issue := testIssue("issue-automated-plan", "digitaldrywood/detent#523", gate.DefaultPlanStop)
				issue.PullRequest = &connector.PullRequest{
					State:               "OPEN",
					CodexReviewState:    "P1",
					CodexReviewFindings: []connector.PullRequestFinding{finding},
				}
				return issue
			}(),
			wantUpdates: []stateUpdateCall{{issueID: "issue-automated-plan", state: "Rework"}},
			wantCommentFragments: []string{
				"Plan review routed this issue from Plan Review to Rework.",
				"reason: p1_findings",
				"Plan omits acceptance criteria.",
				"https://github.test/comment/plan-review",
			},
		},
		{
			name: "automated comment p1 routes to rework with feedback",
			cfg:  gate.PlanConfig{Enabled: true, Review: gate.PlanReviewAutomated, Stop: gate.DefaultPlanStop},
			issue: func() connector.Issue {
				issue := testIssue("issue-automated-comment-p1-plan", "digitaldrywood/detent#525", gate.DefaultPlanStop)
				issue.Comments = []connector.IssueComment{{
					Body: "## Detent Plan Review\n\n- state: P1\n\n### Findings\n\n- Plan omits acceptance criteria.",
					URL:  "https://github.test/comment/plan-review-comment-p1",
				}}
				return issue
			}(),
			wantUpdates: []stateUpdateCall{{issueID: "issue-automated-comment-p1-plan", state: "Rework"}},
			wantCommentFragments: []string{
				"Plan review routed this issue from Plan Review to Rework.",
				"reason: p1_findings",
				"Plan omits acceptance criteria.",
				"https://github.test/comment/plan-review-comment-p1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracker := &planReviewFeedbackConnector{
				fakeConnector: newFakeConnector(),
				release:       make(chan struct{}),
			}
			tracker.setStateIssues(tt.issue)
			cfg := orchestrator.Config{
				PollInterval:        time.Minute,
				MaxConcurrentAgents: 1,
				Plan:                tt.cfg,
				AutoPromote: orchestrator.AutoPromoteConfig{
					Gate: gate.Config{ApprovalLabel: gate.DefaultApprovalLabel},
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework"},
				ObservedStates: []string{"Backlog", "Human Review", "Blocked"},
				TerminalStates: []string{"Done", "Cancelled"},
			}
			orch, err := orchestrator.New(cfg, orchestrator.Dependencies{
				Connector: tracker,
				Runner:    &staticRunner{},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			stop := runOrchestrator(t, orch)
			defer stop()

			waitForState(t, orch, func(orchestrator.State) bool {
				return len(tracker.stateUpdateCalls()) >= len(tt.wantUpdates)
			})

			if comments := tracker.commentCalls(); len(comments) != 0 {
				t.Fatalf("comments = %#v, want none before feedback publication is released", comments)
			}
			close(tracker.release)

			wantComments := 0
			if len(tt.wantCommentFragments) > 0 {
				wantComments = 1
			}
			waitForState(t, orch, func(orchestrator.State) bool {
				return len(tracker.stateUpdateCalls()) >= len(tt.wantUpdates) &&
					len(tracker.commentCalls()) >= wantComments
			})

			if got := tracker.stateUpdateCalls(); !stateUpdatesEqual(got, tt.wantUpdates) {
				t.Fatalf("state updates = %#v, want %#v", got, tt.wantUpdates)
			}
			requests := tracker.fetchByStatesRequests()
			if len(requests) == 0 || !stateListIncludes(requests[0], gate.DefaultPlanStop) {
				t.Fatalf("FetchIssuesByStates requests = %#v, want Plan Review included", requests)
			}
			comments := tracker.commentCalls()
			if len(tt.wantCommentFragments) == 0 {
				if len(comments) != 0 {
					t.Fatalf("comments = %#v, want none", comments)
				}
			} else {
				if len(comments) != 1 {
					t.Fatalf("comments = %#v, want one rework feedback comment", comments)
				}
				for _, fragment := range tt.wantCommentFragments {
					if !strings.Contains(comments[0].body, fragment) {
						t.Fatalf("comment %q missing %q", comments[0].body, fragment)
					}
				}
			}
		})
	}
}

func TestRejectedPlanReworkDispatchesInPlanMode(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-plan-rework", "digitaldrywood/detent#526", gate.DefaultPlanStop)
	issue.Comments = []connector.IssueComment{{
		Body: "## Detent Plan Review\n\n- state: P1\n\n### Findings\n\n- Plan skips acceptance criteria.",
		URL:  "https://github.test/comment/plan-review-p1",
	}}
	tracker := newFakeConnector(issue)
	tracker.setStateIssues(issue)
	runner := &staticRunner{
		result: orchestrator.RunResult{
			FinalState: orchestrator.FinalStateCompleted,
			Output:     "## Revised Plan\n- Add acceptance criteria coverage\n",
		},
	}
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 1,
		Plan: gate.PlanConfig{
			Enabled: true,
			Review:  gate.PlanReviewAutomated,
			Stop:    gate.DefaultPlanStop,
		},
		ActiveStates:           []string{"Todo", "In Progress", "Rework"},
		ObservedStates:         []string{"Backlog", "Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForState(t, orch, func(orchestrator.State) bool {
		return len(runner.requests()) > 0
	})

	requests := runner.requests()
	if got := requests[0].Mode; got != orchestrator.RunModePlan {
		t.Fatalf("RunRequest.Mode = %q, want plan", got)
	}
}

func TestRunRedispatchesInProgressIssueWithOpenPullRequestAfterRestart(t *testing.T) {
	t.Parallel()

	prNumber := 245
	issue := testIssue("issue-in-progress-pr", "digitaldrywood/detent#245", "In Progress")
	issue.PRNumber = &prNumber
	issue.PullRequest = &connector.PullRequest{
		Number:     prNumber,
		URL:        "https://github.com/digitaldrywood/detent/pull/245",
		BranchName: "detent/digitaldrywood_detent_245",
		State:      "OPEN",
	}
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()
	defer close(runner.release)

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Issue.PullRequest == nil || request.Issue.PullRequest.State != "OPEN" {
		t.Fatalf("RunRequest.Issue.PullRequest = %#v, want open pull request", request.Issue.PullRequest)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, ok := state.Running[issue.ID]; !ok {
		t.Fatalf("Running[%q] missing after startup redispatch", issue.ID)
	}
}

func TestRunReportsRunningStateWhileRunnerIsInFlight(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-2", "digitaldrywood/detent#11", "In Progress")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	started := receiveRunRequest(t, runner.started)
	if started.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", started.Issue.ID, issue.ID)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, ok := state.Running[issue.ID]; !ok {
		t.Fatalf("Running[%q] missing while runner is blocked", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; !ok {
		t.Fatalf("Claimed[%q] missing while runner is blocked", issue.ID)
	}

	close(runner.release)

	waitForState(t, orch, func(state orchestrator.State) bool {
		_, completed := state.Completed[issue.ID]
		return completed
	})
}

func TestDrainStopsDispatchAndLetsRunningSessionFinish(t *testing.T) {
	t.Parallel()

	runningIssue := testIssue("issue-running", "digitaldrywood/detent#11", "In Progress")
	nextIssue := testIssue("issue-next", "digitaldrywood/detent#12", "Todo")
	tracker := newFakeConnector(runningIssue, nextIssue)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:            5 * time.Millisecond,
		MaxConcurrentAgents:     1,
		DispatchPriorityByState: []string{"In Progress", "Todo"},
		ActiveStates:            []string{"Todo", "In Progress"},
		TerminalStates:          []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay:  time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	started := receiveRunRequest(t, runner.started)
	if started.Issue.ID != runningIssue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", started.Issue.ID, runningIssue.ID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.Drain(ctx); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if !state.Draining {
		t.Fatal("State().Draining = false, want true")
	}

	close(runner.release)
	waitForState(t, orch, func(state orchestrator.State) bool {
		return state.Draining && len(state.Running) == 0
	})

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected dispatch while draining = %#v", request)
	case <-time.After(50 * time.Millisecond):
	}

	state, err = orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() final error = %v", err)
	}
	if _, ok := state.Retry[runningIssue.ID]; ok {
		t.Fatalf("Retry[%q] present after drain completion", runningIssue.ID)
	}
	if _, ok := state.Running[nextIssue.ID]; ok {
		t.Fatalf("Running[%q] present while draining", nextIssue.ID)
	}
}

func TestBeginDrainStopsPendingDispatchTick(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-pending-shutdown", "digitaldrywood/detent#1546", "Merging")
	issue.PullRequest = &connector.PullRequest{
		Number:         1546,
		URL:            "https://github.test/digitaldrywood/detent/pull/1546",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	}
	issue.PRRepository = "digitaldrywood/detent"
	tracker := &pendingDispatchConnector{
		fakeConnector: newFakeConnector(issue),
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	tracker.stateIssues = []connector.Issue{issue}
	runner := newBlockingRunner()
	var logs bytes.Buffer

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:         time.Hour,
		MergeFastPathEnabled: true,
		MaxConcurrentAgents:  1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:           []string{"Merging"},
		ObservedStates:         []string{"Merging"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
		Logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	select {
	case <-tracker.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch tick did not reach pending tracker fetch")
	}

	orch.BeginDrain()
	close(tracker.release)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.Drain(ctx); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}

	state, err := orch.State(ctx)
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if !state.Draining {
		t.Fatal("State().Draining = false, want true")
	}
	if len(state.Running) != 0 {
		t.Fatalf("State().Running = %#v, want no work started", state.Running)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected dispatch after drain began = %#v", request)
	default:
	}
	for _, event := range []string{"merge_worker_pickup", "merge_worker_slot_acquired"} {
		if strings.Contains(logs.String(), event) {
			t.Fatalf("logs contain %q after drain began:\n%s", event, logs.String())
		}
	}
}

func TestWaitForDispatchQuiescedHonorsContext(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-starting-shutdown", "digitaldrywood/detent#1546", "Todo")
	tracker := &dispatchStartBlockingConnector{
		fakeConnector: newFakeConnector(issue),
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	runner := newBlockingRunner()
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		Claiming: orchestrator.ClaimingConfig{
			Enabled:           true,
			OwnershipMode:     workflowconfig.IdentityOwnershipField,
			Owner:             "detent-test",
			OwnerField:        "Owner",
			LeaseField:        "Lease",
			LeaseTTL:          time.Minute,
			HeartbeatInterval: time.Hour,
		},
		ActiveStates:   []string{"Todo"},
		TerminalStates: []string{"Done", "Cancelled"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	select {
	case <-tracker.started:
	case <-time.After(time.Second):
		t.Fatal("dispatch startup did not reach blocking connector")
	}

	beginDone := make(chan struct{})
	go func() {
		orch.BeginDrain()
		close(beginDone)
	}()
	select {
	case <-beginDone:
	case <-time.After(time.Second):
		close(tracker.release)
		t.Fatal("BeginDrain blocked on active dispatch startup")
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := orch.WaitForDispatchQuiesced(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForDispatchQuiesced() error = %v, want context canceled", err)
	}

	close(tracker.release)
	settleCtx, cancelSettle := context.WithTimeout(context.Background(), time.Second)
	defer cancelSettle()
	if err := orch.WaitForDispatchQuiesced(settleCtx); err != nil {
		t.Fatalf("WaitForDispatchQuiesced() after release error = %v", err)
	}

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("admitted dispatch did not become visible after startup resumed")
	}
	close(runner.release)
}

func TestForceQuitInterruptsRunningSessionAndAbandonsClaim(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-force", "digitaldrywood/detent#383", "In Progress")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 1,
		Claiming: orchestrator.ClaimingConfig{
			Enabled:           true,
			OwnershipMode:     workflowconfig.IdentityOwnershipField,
			Owner:             "detent-test",
			OwnerField:        "Owner",
			LeaseField:        "Lease",
			LeaseTTL:          time.Minute,
			HeartbeatInterval: time.Hour,
		},
		ActiveStates:           []string{"In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	receiveRunRequest(t, runner.started)
	waitForState(t, orch, func(state orchestrator.State) bool {
		_, running := state.Running[issue.ID]
		_, claimed := state.Claimed[issue.ID]
		return running && claimed
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.ForceQuit(ctx); err != nil {
		t.Fatalf("ForceQuit() error = %v", err)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if !state.Draining {
		t.Fatal("State().Draining = false, want true")
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after force quit", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after force quit", issue.ID)
	}
	if !tracker.hasSetField(issue.ID, "Lease", "") {
		t.Fatalf("SetField(%q, Lease, empty) not recorded; calls = %#v", issue.ID, tracker.setFieldCalls())
	}
}

func TestDrainCompletionAbandonsClaim(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-drain-claim", "digitaldrywood/detent#384", "In Progress")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 1,
		Claiming: orchestrator.ClaimingConfig{
			Enabled:           true,
			OwnershipMode:     workflowconfig.IdentityOwnershipField,
			Owner:             "detent-test",
			OwnerField:        "Owner",
			LeaseField:        "Lease",
			LeaseTTL:          time.Minute,
			HeartbeatInterval: time.Hour,
		},
		ActiveStates:           []string{"In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	receiveRunRequest(t, runner.started)
	waitForState(t, orch, func(state orchestrator.State) bool {
		_, running := state.Running[issue.ID]
		_, claimed := state.Claimed[issue.ID]
		return running && claimed
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.Drain(ctx); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	close(runner.release)

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, completed := state.Completed[issue.ID]
		_, running := state.Running[issue.ID]
		_, claimed := state.Claimed[issue.ID]
		return state.Draining && completed && !running && !claimed
	})
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after drain completion", issue.ID)
	}
	if !tracker.hasSetField(issue.ID, "Lease", "") {
		t.Fatalf("SetField(%q, Lease, empty) not recorded; calls = %#v", issue.ID, tracker.setFieldCalls())
	}
}

func TestRunAppliesUsageUpdateWhileRunnerIsInFlight(t *testing.T) {
	t.Parallel()

	lastEventAt := time.Date(2026, 5, 31, 14, 30, 0, 0, time.UTC)
	issue := testIssue("issue-live-usage", "digitaldrywood/detent#115", "In Progress")
	tracker := newFakeConnector(issue)
	runner := newUsageStreamingRunner(orchestrator.UsageUpdate{
		SessionID:     "thread-live-turn-live",
		WorkspacePath: "/tmp/detent-workspaces/issue-live-usage",
		TurnCount:     1,
		LastEventAt:   lastEventAt,
		LastEvent:     "agent_message_delta",
		LastMessage:   "editing telemetry",
		Tokens: orchestrator.TokenTotals{
			InputTokens:    40,
			OutputTokens:   12,
			TotalTokens:    52,
			RuntimeSeconds: 3.5,
		},
		RecentEvents: []telemetry.ActivityEvent{
			{At: lastEventAt.Add(-time.Second), Event: "turn_started", Message: "turn started"},
			{At: lastEventAt, Event: "agent_message_delta", Message: "editing telemetry"},
		},
		DiffStats: orchestrator.DiffStats{
			FilesChanged: 2,
			AddedLines:   9,
			RemovedLines: 1,
			Status:       "ok",
		},
	})

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	receiveRunRequest(t, runner.started)

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		running, ok := state.Running[issue.ID]
		return ok && running.Tokens.TotalTokens == 52 && running.TurnCount == 1
	})

	running := state.Running[issue.ID]
	if running.Tokens.InputTokens != 40 || running.Tokens.OutputTokens != 12 {
		t.Fatalf("Running[%q].Tokens = %#v, want input 40 output 12", issue.ID, running.Tokens)
	}
	if running.Tokens.RuntimeSeconds != 3.5 {
		t.Fatalf("Running[%q].Tokens.RuntimeSeconds = %v, want 3.5", issue.ID, running.Tokens.RuntimeSeconds)
	}
	if running.SessionID != "thread-live-turn-live" || running.LastEvent != "agent_message_delta" || running.LastMessage != "editing telemetry" {
		t.Fatalf("Running[%q] live activity = %#v", issue.ID, running)
	}
	if running.WorkspacePath != "/tmp/detent-workspaces/issue-live-usage" {
		t.Fatalf("Running[%q].WorkspacePath = %q, want /tmp/detent-workspaces/issue-live-usage", issue.ID, running.WorkspacePath)
	}
	if !running.LastEventAt.Equal(lastEventAt) {
		t.Fatalf("Running[%q].LastEventAt = %v, want %v", issue.ID, running.LastEventAt, lastEventAt)
	}
	if len(running.RecentEvents) != 2 || running.RecentEvents[1].Message != "editing telemetry" {
		t.Fatalf("Running[%q].RecentEvents = %#v", issue.ID, running.RecentEvents)
	}
	if running.DiffStats.FilesChanged != 2 || running.DiffStats.AddedLines != 9 || running.DiffStats.RemovedLines != 1 || running.DiffStats.Status != "ok" {
		t.Fatalf("Running[%q].DiffStats = %#v, want live diff stats", issue.ID, running.DiffStats)
	}
	if state.TokenTotals.TotalTokens != 0 {
		t.Fatalf("TokenTotals.TotalTokens = %d, want completed totals only", state.TokenTotals.TotalTokens)
	}

	close(runner.release)
}

func TestRunRefreshHydratesRunningIssueComments(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-workpad", "digitaldrywood/detent#646", "Todo")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()
	orch := newTestOrchestratorWithPollInterval(t, tracker, runner, time.Hour)
	stop := runOrchestrator(t, orch)
	defer stop()

	receiveRunRequest(t, runner.started)
	tracker.setIssueComments(issue.ID, []connector.IssueComment{{
		Body: "## Codex Workpad\n\n### Status\nIn Progress",
		URL:  "https://github.test/comment/646",
	}})
	if _, err := orch.RequestRefresh(context.Background()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		running, ok := state.Running[issue.ID]
		return ok && len(running.Issue.Comments) == 1
	})
	comments := state.Running[issue.ID].Issue.Comments
	if comments[0].Body != "## Codex Workpad\n\n### Status\nIn Progress" || comments[0].URL != "https://github.test/comment/646" {
		t.Fatalf("Running[%q].Comments = %#v, want Workpad comment", issue.ID, comments)
	}

	close(runner.release)
}

func TestUpdateConfigAppliesBeforeNextTick(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-reload", "digitaldrywood/detent#41", "Todo")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Backlog"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected run before config update = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}

	updateCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.UpdateConfig(updateCtx, orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	}); err != nil {
		t.Fatalf("UpdateConfig() error = %v", err)
	}

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if state.PollInterval != 5*time.Millisecond {
		t.Fatalf("State().PollInterval = %s, want 5ms", state.PollInterval)
	}
	if state.MaxConcurrentAgents != 2 {
		t.Fatalf("State().MaxConcurrentAgents = %d, want 2", state.MaxConcurrentAgents)
	}

	close(runner.release)
}

func TestStateReportsLiveAgentPoolCapacity(t *testing.T) {
	t.Parallel()

	project := scheduler.ProjectCandidate{ID: "detent", Pool: "video"}
	pools := []scheduler.PoolConfig{
		{Name: scheduler.DefaultPoolName, Scheduler: scheduler.Config{Kind: "weighted", Capacity: 1}},
		{Name: "video", Scheduler: scheduler.Config{Kind: "weighted", Capacity: 5}},
	}
	dispatch, err := scheduler.NewPoolRegistry(pools, []scheduler.ProjectCandidate{project})
	if err != nil {
		t.Fatalf("NewPoolRegistry() error = %v", err)
	}
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             project,
	}, orchestrator.Dependencies{
		Connector:          newFakeConnector(),
		GlobalDispatchGate: dispatch,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if state.PoolName != "video" || state.PoolCapacity != 5 {
		t.Fatalf("State() pool = %q/%d, want video/5", state.PoolName, state.PoolCapacity)
	}

	pools[1].Scheduler.Capacity = 7
	if err := dispatch.Reconfigure(pools, []scheduler.ProjectCandidate{project}); err != nil {
		t.Fatalf("Reconfigure() error = %v", err)
	}
	state, err = orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() after reconfigure error = %v", err)
	}
	if state.PoolCapacity != 7 {
		t.Fatalf("State().PoolCapacity after reconfigure = %d, want 7", state.PoolCapacity)
	}
}

func TestUpdateRuntimeSwapsConnectorBeforeNextTick(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-reload-connector", "digitaldrywood/detent#41", "Todo")
	initialTracker := newFakeConnector()
	reloadedTracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: initialTracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected run before connector update = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}

	updateCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.UpdateRuntime(updateCtx, orchestrator.RuntimeUpdate{
		Config: orchestrator.Config{
			PollInterval:        5 * time.Millisecond,
			MaxConcurrentAgents: 1,
			ActiveStates:        []string{"Todo"},
			TerminalStates:      []string{"Done"},
		},
		Connector: reloadedTracker,
	}); err != nil {
		t.Fatalf("UpdateRuntime() error = %v", err)
	}

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}

	close(runner.release)
}

func TestUpdateRuntimeClearsAuthHealthWhenConnectorHasNoReport(t *testing.T) {
	t.Parallel()

	failedAt := time.Now().Add(-time.Minute)
	initialTracker := &authHealthConnector{
		fakeConnector: newFakeConnector(),
		health: connector.AuthHealth{
			Status:      connector.AuthStatusStale,
			LastError:   "github authentication failed: status 401",
			LastErrorAt: failedAt,
		},
		ok: true,
	}
	reloadedTracker := &authHealthConnector{fakeConnector: newFakeConnector()}
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        5 * time.Millisecond,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: initialTracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForState(t, orch, func(state orchestrator.State) bool {
		return state.Auth.Status == connector.AuthStatusStale
	})

	updateCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := orch.UpdateRuntime(updateCtx, orchestrator.RuntimeUpdate{
		Config: orchestrator.Config{
			PollInterval:        5 * time.Millisecond,
			MaxConcurrentAgents: 1,
			ActiveStates:        []string{"Todo"},
			TerminalStates:      []string{"Done"},
		},
		Connector: reloadedTracker,
	}); err != nil {
		t.Fatalf("UpdateRuntime() error = %v", err)
	}

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return reloadedTracker.fetchCandidateCalls() > 0 && state.Auth == (connector.AuthHealth{})
	})
	if state.Auth != (connector.AuthHealth{}) {
		t.Fatalf("Auth = %#v, want cleared", state.Auth)
	}
}

func TestRunDispatchesByStateRankBeforePriorityAndAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	todo := rankedTestIssue(testIssue("todo-old-urgent", "digitaldrywood/detent#20", "Todo"), 1, now.Add(-4*time.Hour))
	rework := rankedTestIssue(testIssue("rework-new-low", "digitaldrywood/detent#21", "Rework"), 4, now.Add(-time.Hour))
	merging := rankedTestIssue(testIssue("merging-new-low", "digitaldrywood/detent#22", "Merging"), 4, now.Add(-30*time.Minute))
	tracker := newFakeConnector(todo, rework, merging)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:            time.Hour,
		MaxConcurrentAgents:     1,
		DispatchPriorityByState: []string{"Merging", "Rework"},
		ActiveStates:            []string{"Todo", "Rework", "Merging"},
		TerminalStates:          []string{"Done", "Cancelled", "Canceled", "Closed"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != merging.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, merging.ID)
	}

	close(runner.release)
}

func TestRunSchedulesRetryAfterRunnerError(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-3", "digitaldrywood/detent#12", "Todo")
	tracker := newFakeConnector(issue)
	runner := &staticRunner{err: errors.New("runner failed")}

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		retry, ok := state.Retry[issue.ID]
		return ok && retry.Error != ""
	})

	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after runner error", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after terminal runner error", issue.ID)
	}
	if got := state.Retry[issue.ID].Attempt; got != 1 {
		t.Fatalf("Retry[%q].Attempt = %d, want 1", issue.ID, got)
	}
	if got := state.Retry[issue.ID].Error; got != "runner failed" {
		t.Fatalf("Retry[%q].Error = %q, want runner failed", issue.ID, got)
	}
}

func TestRunSchedulesRetryAfterRunnerPanic(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-panic", "digitaldrywood/detent#22", "Todo")
	tracker := newFakeConnector(issue)
	runner := panicRunner{}

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		retry, ok := state.Retry[issue.ID]
		return ok && retry.Error != ""
	})

	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after runner panic", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after terminal runner panic", issue.ID)
	}
	if got := state.Retry[issue.ID].Attempt; got != 1 {
		t.Fatalf("Retry[%q].Attempt = %d, want 1", issue.ID, got)
	}
	if got := state.Retry[issue.ID].Error; !strings.Contains(got, "runner panic: boom") {
		t.Fatalf("Retry[%q].Error = %q, want runner panic", issue.ID, got)
	}
}

func TestRunParksIssueAfterRepeatedInstantBackendFailures(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-instant-fail", "digitaldrywood/detent#927", "Todo")
	tracker := newFakeConnector(issue)
	backendBody := `{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"model rejected"}}`
	runner := &staticRunner{err: instantBackendError{body: backendBody}}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Millisecond,
		FailureRetryBaseDelay:  time.Millisecond,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[issue.ID]
		return ok
	})

	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls = %d, want 5", got)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after circuit breaker", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after circuit breaker", issue.ID)
	}
	if !strings.Contains(state.Blocked[issue.ID].Reason, backendBody) {
		t.Fatalf("Blocked[%q].Reason = %q, want backend body", issue.ID, state.Blocked[issue.ID].Reason)
	}
	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: "Blocked"}) {
		t.Fatalf("state updates = %#v, want final Blocked transition", updates)
	}
	comments := tracker.commentCalls()
	if len(comments) != 1 {
		t.Fatalf("comments = %#v, want one circuit breaker comment", comments)
	}
	if !strings.Contains(comments[0].body, backendBody) || !strings.Contains(comments[0].body, "stopped retrying") {
		t.Fatalf("comment body missing backend error:\n%s", comments[0].body)
	}
}

func TestRunPausesBackendAfterQuotaErrorWithoutBreakerStrike(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	resetAt := now.Add(44 * time.Minute)
	issue := testIssue("issue-quota-exhausted", "digitaldrywood/detent#1142", "Todo")
	tracker := newFakeConnector(issue)
	backendBody := fmt.Sprintf(`{"type":"error","error":{"type":"usageLimitExceeded","resetAt":%d,"message":"usage limit reached"}}`, resetAt.Unix())
	capacityErr := backendcapacity.NewError(
		backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"},
		backendcapacity.Details{Kind: "usageLimitExceeded", Reason: "provider usage limit reached", ResetAt: &resetAt},
		instantBackendError{body: backendBody},
	)
	runner := &staticRunner{err: capacityErr}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Retry[issue.ID]
		return ok
	})

	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("runner calls = %d, want one capacity probe", got)
	}
	if len(state.InstantFailures) != 0 {
		t.Fatalf("InstantFailures = %#v, want no breaker strikes", state.InstantFailures)
	}
	if len(state.RepeatedFailures) != 0 {
		t.Fatalf("RepeatedFailures = %#v, want no breaker strikes", state.RepeatedFailures)
	}
	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] present after quota error", issue.ID)
	}
	retry := state.Retry[issue.ID]
	if retry.Issue.State != "Todo" {
		t.Fatalf("Retry[%q].Issue.State = %q, want pre-probe Todo state", issue.ID, retry.Issue.State)
	}
	if retry.Attempt != 0 {
		t.Fatalf("Retry[%q].Attempt = %d, want unchanged initial attempt", issue.ID, retry.Attempt)
	}
	probeAt := now.Add(5 * time.Minute)
	if retry.DueAt.Before(probeAt) || retry.DueAt.After(probeAt.Add(time.Minute)) {
		t.Fatalf("Retry[%q].DueAt = %s, want early canary around %s before provider reset %s", issue.ID, retry.DueAt, probeAt, resetAt)
	}
	updates := tracker.stateUpdateCalls()
	if len(updates) < 2 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: "Todo"}) {
		t.Fatalf("state updates = %#v, want capacity probe restored to Todo", updates)
	}
}

func TestRunParksIssueAfterRepeatedInstantBackendFailuresTruncatesConfiguredOutput(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-instant-fail-truncated", "digitaldrywood/detent#978", "Todo")
	tracker := newFakeConnector(issue)
	backendBody := "0123456789abcdefghijklmnopqrstuvwxyz"
	runner := &staticRunner{err: instantBackendError{body: backendBody}}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:             time.Millisecond,
		MaxConcurrentAgents:      1,
		MaxRetryBackoff:          time.Millisecond,
		FailureRetryBaseDelay:    time.Millisecond,
		ActiveStates:             []string{"Todo", "In Progress"},
		ObservedStates:           []string{"Blocked"},
		TerminalStates:           []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay:   time.Second,
		OutputTruncationMaxBytes: len(runtimeoutput.Marker) + 10,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[issue.ID]
		return ok
	})

	wantBody := "01234" + runtimeoutput.Marker + "vwxyz"
	reason := state.Blocked[issue.ID].Reason
	if !strings.Contains(reason, wantBody) {
		t.Fatalf("Blocked[%q].Reason = %q, want truncated backend body %q", issue.ID, reason, wantBody)
	}
	if strings.Contains(reason, backendBody) {
		t.Fatalf("Blocked[%q].Reason included unbounded backend body: %q", issue.ID, reason)
	}
	comments := tracker.commentCalls()
	if len(comments) != 1 {
		t.Fatalf("comments = %#v, want one circuit breaker comment", comments)
	}
	if !strings.Contains(comments[0].body, wantBody) {
		t.Fatalf("comment body missing truncated backend error:\n%s", comments[0].body)
	}
	if strings.Contains(comments[0].body, backendBody) {
		t.Fatalf("comment body included unbounded backend error:\n%s", comments[0].body)
	}
}

func TestRunInstantFailureCircuitBreakerComparesFullBackendErrorKey(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-instant-fail-key", "digitaldrywood/detent#979", "Todo")
	tracker := newFakeConnector(issue)
	backendBodies := []string{
		"01234" + strings.Repeat("a", 20) + "vwxyz",
		"01234" + strings.Repeat("b", 20) + "vwxyz",
	}
	runner := &staticRunner{}
	runner.onRun = func(orchestrator.RunRequest) {
		call := runner.calls.Load()
		runner.err = instantBackendError{body: backendBodies[int(call-1)%len(backendBodies)]}
	}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:             time.Millisecond,
		MaxConcurrentAgents:      1,
		MaxRetryBackoff:          time.Millisecond,
		FailureRetryBaseDelay:    time.Millisecond,
		ActiveStates:             []string{"Todo", "In Progress"},
		ObservedStates:           []string{"Blocked"},
		TerminalStates:           []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay:   time.Second,
		OutputTruncationMaxBytes: len(runtimeoutput.Marker) + 10,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[issue.ID]
		return ok
	})

	// Alternating backend errors must not trip the instant-failure breaker
	// (the full error key differs each attempt even though the truncated
	// operator text matches); the repeated-failure breaker still parks the
	// issue after five consecutive failures regardless of error text.
	reason := state.Blocked[issue.ID].Reason
	if strings.HasPrefix(reason, "instant fail circuit breaker: ") {
		t.Fatalf("Blocked[%q].Reason = %q, instant breaker tripped on alternating backend errors", issue.ID, reason)
	}
	if !strings.HasPrefix(reason, "repeated failure circuit breaker: ") {
		t.Fatalf("Blocked[%q].Reason = %q, want repeated failure circuit breaker prefix", issue.ID, reason)
	}
	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls = %d, want 5", got)
	}
	comments := tracker.commentCalls()
	if len(comments) != 1 {
		t.Fatalf("comments = %#v, want one repeated failure comment", comments)
	}
	if !strings.Contains(comments[0].body, "consecutive failed attempts") {
		t.Fatalf("comment body missing repeated failure text:\n%s", comments[0].body)
	}
}

func TestRunParksInstantBackendFailuresInBlockedWithDefaultStates(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-default-instant-fail", "digitaldrywood/detent#928", "Todo")
	tracker := newFakeConnector(issue)
	backendBody := `{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"model rejected"}}`
	runner := &staticRunner{err: instantBackendError{body: backendBody}}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Millisecond,
		FailureRetryBaseDelay:  time.Millisecond,
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		blocked, ok := state.Blocked[issue.ID]
		return ok && strings.Contains(blocked.Reason, backendBody)
	})

	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: "Blocked"}) {
		t.Fatalf("state updates = %#v, want final Blocked transition", updates)
	}
	if got := state.Blocked[issue.ID].Issue.State; got != "Blocked" {
		t.Fatalf("Blocked[%q].Issue.State = %q, want Blocked", issue.ID, got)
	}
	fetchesAtBlock := tracker.fetchByStatesCalls()
	state = waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[issue.ID]
		return ok && tracker.fetchByStatesCalls() > fetchesAtBlock
	})
	if _, ok := state.Blocked[issue.ID]; !ok {
		t.Fatalf("Blocked[%q] missing after blocked-status refresh", issue.ID)
	}
	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls = %d, want 5", got)
	}
}

func TestRunRedispatchesDueRetryWithExistingClaim(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-retry", "digitaldrywood/detent#16", "Todo")
	tracker := newFakeConnector(issue)
	runner := newRetryRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        5 * time.Millisecond,
		FailureRetryBaseDelay:  5 * time.Millisecond,
		ContinuationRetryDelay: time.Second,
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	request := receiveRunRequest(t, runner.retryStarted)
	if request.Issue.ID != issue.ID {
		t.Fatalf("retry RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Attempt != 1 {
		t.Fatalf("retry RunRequest.Attempt = %d, want 1", request.Attempt)
	}

	state, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if got := state.Running[issue.ID].Attempt; got != 1 {
		t.Fatalf("Running[%q].Attempt = %d, want 1", issue.ID, got)
	}
	if _, ok := state.Claimed[issue.ID]; !ok {
		t.Fatalf("Claimed[%q] missing during retry run", issue.ID)
	}

	close(runner.release)
}

func TestRunSkipsTodoBlockedByNonTerminalDependency(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-4", "digitaldrywood/detent#13", "Todo")
	issue.BlockedBy = []connector.BlockedRef{{
		Identifier: "digitaldrywood/detent#4",
		State:      "In Progress",
	}}
	tracker := newFakeConnector(issue)
	runner := &staticRunner{}

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, blocked := state.Blocked[issue.ID]
		return blocked
	})

	if got := runner.calls.Load(); got != 0 {
		t.Fatalf("runner calls = %d, want 0", got)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present for blocked issue", issue.ID)
	}
	if got := state.Blocked[issue.ID].Issue.BlockedBy[0].State; got != "In Progress" {
		t.Fatalf("Blocked dependency state = %q, want In Progress", got)
	}
}

func TestRunTracksBlockedStatusIssuesForDisplayOnly(t *testing.T) {
	t.Parallel()

	candidate := testIssue("issue-ready", "digitaldrywood/detent#170", "Todo")
	blocked := testIssue("issue-blocked-status", "digitaldrywood/detent#98", "Blocked")
	blocked.BlockerReason = "Create public repository digitaldrywood/homebrew-tap"
	tracker := newFakeConnector(candidate)
	tracker.setStateIssues(blocked)
	runner := newBlockingRunner()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    2,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()
	defer close(runner.release)

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != candidate.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, candidate.ID)
	}

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[blocked.ID]
		return ok
	})

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected display-only blocked dispatch = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}
	if got := tracker.fetchByStatesCalls(); got == 0 {
		t.Fatal("FetchIssuesByStates() calls = 0, want blocked status fetch")
	}
	if _, ok := state.Claimed[blocked.ID]; ok {
		t.Fatalf("Claimed[%q] present for display-only blocked issue", blocked.ID)
	}
	entry := state.Blocked[blocked.ID]
	if entry.Issue.ID != blocked.ID || entry.Issue.State != "Blocked" {
		t.Fatalf("Blocked[%q].Issue = %#v, want Blocked issue", blocked.ID, entry.Issue)
	}
	if entry.Reason != blocked.BlockerReason {
		t.Fatalf("Blocked[%q].Reason = %q, want %q", blocked.ID, entry.Reason, blocked.BlockerReason)
	}
	snapshot := state.Snapshot(time.Now())
	if snapshot.Counts.Blocked != 1 || len(snapshot.Blocked) != 1 {
		t.Fatalf("snapshot blocked count = %d len = %d, want 1", snapshot.Counts.Blocked, len(snapshot.Blocked))
	}
}

func TestRunReapsTerminalWorkspacesOnStartupSweep(t *testing.T) {
	t.Parallel()

	done := testIssue("issue-done", "digitaldrywood/detent#80", "Done")
	reaper := &fakeWorkspaceReaper{result: orchestrator.WorkspaceReapResult{Worktrees: 1, Branches: 1}}
	tracker := newFakeConnector()
	tracker.setStateIssues(done)

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:                  time.Hour,
		MaxConcurrentAgents:           1,
		ActiveStates:                  []string{"Todo", "In Progress", "Rework"},
		TerminalStates:                []string{"Done", "Cancelled"},
		ObservedStates:                []string{"Human Review"},
		WorkspaceCleanupIdleTTL:       time.Hour,
		WorkspaceCleanupSweepInterval: time.Hour,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          &staticRunner{},
		WorkspaceReaper: reaper,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	reaped := waitForWorkspaceReaps(t, reaper, 1)
	if reaped[0].ID != done.ID {
		t.Fatalf("reaped issue ID = %q, want %q", reaped[0].ID, done.ID)
	}
	if !stateRequestsContain(tracker.fetchByStatesRequests(), []string{"Done", "Cancelled", "Human Review"}) {
		t.Fatalf("FetchIssuesByStates requests = %#v, want cleanup request", tracker.fetchByStatesRequests())
	}
}

func TestRunReapsIdleObservedWorkspacesWithoutTouchingActiveIssues(t *testing.T) {
	t.Parallel()

	now := time.Now()
	staleAt := now.Add(-2 * time.Hour)
	recentAt := now.Add(-5 * time.Minute)
	idle := testIssue("issue-review", "digitaldrywood/detent#81", "Human Review")
	idle.StageUpdatedAt = &staleAt
	recent := testIssue("issue-blocked", "digitaldrywood/detent#82", "Blocked")
	recent.StageUpdatedAt = &recentAt
	active := testIssue("issue-active", "digitaldrywood/detent#83", "In Progress")
	active.StageUpdatedAt = &staleAt

	reaper := &fakeWorkspaceReaper{result: orchestrator.WorkspaceReapResult{Worktrees: 1}}
	tracker := newFakeConnector()
	tracker.setStateIssues(idle, recent, active)

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:                  time.Hour,
		MaxConcurrentAgents:           1,
		ActiveStates:                  []string{"Todo", "In Progress", "Rework"},
		TerminalStates:                []string{"Done"},
		ObservedStates:                []string{"Human Review", "Blocked", "In Progress"},
		WorkspaceCleanupIdleTTL:       time.Hour,
		WorkspaceCleanupSweepInterval: time.Hour,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          &staticRunner{},
		WorkspaceReaper: reaper,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	reaped := waitForWorkspaceReaps(t, reaper, 1)
	if reaped[0].ID != idle.ID {
		t.Fatalf("reaped issue ID = %q, want %q", reaped[0].ID, idle.ID)
	}

	time.Sleep(25 * time.Millisecond)
	if got := reaper.reapedIssues(); len(got) != 1 {
		t.Fatalf("reaped issues = %#v, want only idle issue", got)
	}
}

func TestRunRetriesTransientStartupCleanupFetchAndDispatchesCandidate(t *testing.T) {
	t.Parallel()

	candidate := testIssue("issue-cleanup-transient", "digitaldrywood/detent#640", "Todo")
	transientErr := errors.New("fetch github pull request reviews: github transient error: status 504")
	tracker := newTransientCleanupConnector(candidate, transientErr)
	runner := newBlockingRunner()
	logs := &lockedBuffer{}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:                  time.Hour,
		MaxConcurrentAgents:           1,
		ActiveStates:                  []string{"Todo", "In Progress", "Rework"},
		TerminalStates:                []string{"Done", "Cancelled"},
		ObservedStates:                []string{"Human Review"},
		WorkspaceCleanupIdleTTL:       time.Hour,
		WorkspaceCleanupSweepInterval: time.Hour,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          runner,
		WorkspaceReaper: &fakeWorkspaceReaper{},
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != candidate.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, candidate.ID)
	}

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return recentEventContains(state.RecentEvents, "workspace_cleanup_fetch_failed", "status 504")
	})
	if got := state.Snapshot(time.Now()).Refresh.ReadinessStatus(); got != telemetry.RefreshStatusReady {
		t.Fatalf("snapshot Refresh status = %q, want ready", got)
	}
	if got := logs.String(); !strings.Contains(got, "fetch workspace cleanup candidates failed") || !strings.Contains(got, "status 504") {
		t.Fatalf("logs = %q, want startup cleanup fetch failure", got)
	}

	if _, err := orch.RequestRefresh(context.Background()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	tracker.waitForCleanupAttempts(t, 2)

	if got := tracker.cleanupAttempts(); got != 2 {
		t.Fatalf("cleanup attempts = %d, want transient failure retried once", got)
	}
	state = waitForState(t, orch, func(state orchestrator.State) bool {
		return tracker.cleanupAttempts() >= 2 && state.Snapshot(time.Now()).Refresh.ReadinessStatus() == telemetry.RefreshStatusReady
	})
	if state.LastRefreshError != "" || !state.LastRefreshErrorAt.IsZero() {
		t.Fatalf("refresh error = %q at %v, want cleared after retry", state.LastRefreshError, state.LastRefreshErrorAt)
	}
	close(runner.release)
}

func TestRunKeepsRefreshReadyWhenTerminalCleanupFetchFailsAfterSnapshot(t *testing.T) {
	t.Parallel()

	candidate := testIssue("issue-cleanup-after-snapshot", "digitaldrywood/detent#682", "Todo")
	transientErr := errors.New("fetch github pull request: github transient error: status 502")
	tracker := newTransientCleanupConnector(candidate, transientErr).failCleanupOnAttempt(2)
	runner := newBlockingRunner()
	logs := &lockedBuffer{}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:                  time.Hour,
		MaxConcurrentAgents:           1,
		ActiveStates:                  []string{"Todo", "In Progress", "Rework"},
		TerminalStates:                []string{"Done", "Cancelled"},
		ObservedStates:                []string{"Human Review"},
		WorkspaceCleanupIdleTTL:       time.Hour,
		WorkspaceCleanupSweepInterval: time.Nanosecond,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          runner,
		WorkspaceReaper: &fakeWorkspaceReaper{},
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != candidate.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, candidate.ID)
	}
	tracker.waitForCleanupAttempts(t, 1)

	initialState := waitForState(t, orch, func(state orchestrator.State) bool {
		return !state.LastRefreshAt.IsZero() &&
			state.Snapshot(time.Now()).Refresh.ReadinessStatus() == telemetry.RefreshStatusReady
	})
	if initialState.LastRefreshError != "" || !initialState.LastRefreshErrorAt.IsZero() {
		t.Fatalf("initial refresh error = %q at %v, want none", initialState.LastRefreshError, initialState.LastRefreshErrorAt)
	}

	if _, err := orch.RequestRefresh(context.Background()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	tracker.waitForCleanupAttempts(t, 2)

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return recentEventContains(state.RecentEvents, "workspace_cleanup_fetch_failed", "status 502")
	})
	if got := state.Snapshot(time.Now()).Refresh.ReadinessStatus(); got != telemetry.RefreshStatusReady {
		t.Fatalf("snapshot Refresh status = %q, want ready", got)
	}
	if state.LastRefreshError != "" || !state.LastRefreshErrorAt.IsZero() {
		t.Fatalf("refresh error = %q at %v, want none for cleanup failure", state.LastRefreshError, state.LastRefreshErrorAt)
	}
	if got := logs.String(); !strings.Contains(got, "fetch workspace cleanup candidates failed") || !strings.Contains(got, "status 502") {
		t.Fatalf("logs = %q, want terminal cleanup fetch failure", got)
	}
	close(runner.release)
}

func TestRunFetchesOnlyActionableObservedStates(t *testing.T) {
	t.Parallel()

	tracker := newFakeConnector()
	orch := newTestOrchestrator(t, tracker, &staticRunner{})
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchByStatesCalls(t, tracker, 1)

	got := tracker.fetchByStatesRequests()
	want := []string{"Blocked", "Human Review", "Merging"}
	for _, request := range got {
		if !stateRequestsContain([][]string{request}, want) {
			t.Fatalf("FetchIssuesByStates request = %#v, want combined status request containing %#v; all requests = %#v", request, want, got)
		}
		for _, forbidden := range []string{"Backlog", "Done", "Cancelled", "Canceled", "Closed"} {
			for _, state := range request {
				if strings.EqualFold(strings.TrimSpace(state), forbidden) {
					t.Fatalf("FetchIssuesByStates request = %#v, want no non-actionable state %q", request, forbidden)
				}
			}
		}
	}
}

func TestRunPublishesBoardIssuesFromCandidatesAndObservedStates(t *testing.T) {
	t.Parallel()

	candidate := testIssue("issue-progress", "digitaldrywood/detent#91", "In Progress")
	backlog := testIssue("issue-backlog", "digitaldrywood/detent#92", "Backlog")
	review := testIssue("issue-review", "digitaldrywood/detent#93", "Human Review")
	tracker := newFakeConnector(candidate)
	tracker.setStateIssues(backlog, review)
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Backlog", "Human Review"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchByStatesCalls(t, tracker, 1)
	if !stateRequestsContain(tracker.fetchByStatesRequests(), []string{"Blocked", "Human Review", "Merging", "Backlog"}) {
		t.Fatalf("FetchIssuesByStates requests = %#v, want configured observed states included", tracker.fetchByStatesRequests())
	}

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return len(state.BoardIssues) >= 3
	})
	got := map[string]string{}
	for _, issue := range state.BoardIssues {
		got[issue.ID] = issue.State
	}
	want := map[string]string{
		"issue-progress": "In Progress",
		"issue-backlog":  "Backlog",
		"issue-review":   "Human Review",
	}
	if !maps.Equal(got, want) {
		t.Fatalf("BoardIssues = %#v, want %#v", got, want)
	}
}

func TestRunTracksStatusLabelConflictCandidateAsBlocked(t *testing.T) {
	t.Parallel()

	conflict := testIssue("issue-conflict", "digitaldrywood/detent#606", "Blocked")
	conflict.Labels = []string{"detent:todo", "detent:in-progress", "bug"}
	conflict.BlockerReason = "multiple configured Detent status labels: detent:in-progress, detent:todo; remove all but one status label"
	tracker := newFakeConnector(conflict)
	tracker.fetchByStatesErr = errors.New("status polling failed")
	runner := &staticRunner{}
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           5 * time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Backlog", "Human Review", "Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[conflict.ID]
		return ok
	})
	if got := runner.calls.Load(); got != 0 {
		t.Fatalf("runner calls = %d, want 0", got)
	}
	blocked := state.Blocked[conflict.ID]
	if !strings.Contains(blocked.Reason, "multiple configured Detent status labels") {
		t.Fatalf("blocked reason = %q, want status-label conflict", blocked.Reason)
	}
	snapshot := state.Snapshot(time.Now())
	if snapshot.Counts.Blocked != 1 {
		t.Fatalf("snapshot blocked count = %d, want 1", snapshot.Counts.Blocked)
	}
	counts := telemetry.BoardStateCounts(snapshot)
	if len(counts) != 1 || counts[0].State != "Blocked" || counts[0].Count != 1 {
		t.Fatalf("board state counts = %#v, want one Blocked issue", counts)
	}
}

func TestRunDispatchesOperatorMovedBlockedIssueDuringDegradedTransitionRefresh(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-operator-move", "digitaldrywood/detent#1482", "Blocked")
	unrelated := testIssue("issue-unrelated-blocked", "digitaldrywood/detent#1483", "Blocked")
	tracker := &operatorMoveConnector{fakeConnector: newFakeConnector(issue, unrelated)}
	tracker.setStateIssues(issue, unrelated)
	runner := newBlockingRunner()
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "Rework"},
		ObservedStates:      []string{"Blocked"},
		TerminalStates:      []string{"Done", "Cancelled"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForState(t, orch, func(state orchestrator.State) bool {
		_, movedBlocked := state.Blocked[issue.ID]
		_, unrelatedBlocked := state.Blocked[unrelated.ID]
		return movedBlocked && unrelatedBlocked
	})
	tracker.issueStateRefreshFailures.Store(1)
	if err := tracker.UpdateIssueState(t.Context(), issue.ID, "Rework"); err != nil {
		t.Fatalf("UpdateIssueState() error = %v", err)
	}
	result, err := orch.ReconcileOperatorMove(t.Context(), orchestrator.OperatorMoveRequest{
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		FromState:  "Blocked",
		ToState:    "Rework",
	})
	if err != nil {
		t.Fatalf("ReconcileOperatorMove() error = %v", err)
	}
	if !result.Reconciled || !result.BlockedCleared {
		t.Fatalf("ReconcileOperatorMove() = %#v, want reconciled runtime block", result)
	}
	state, err := orch.State(t.Context())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, ok := state.Blocked[unrelated.ID]; !ok {
		t.Fatalf("Blocked[%q] missing after item-scoped reconcile", unrelated.ID)
	}
	if _, err := orch.RequestRefresh(t.Context()); err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}

	request := receiveRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID || request.Issue.State != "Rework" {
		t.Fatalf("RunRequest.Issue = %#v, want moved Rework issue", request.Issue)
	}
	state = waitForState(t, orch, func(state orchestrator.State) bool {
		for _, decision := range state.SchedulerDecisions {
			if decision.IssueID == issue.ID && decision.Selected {
				return true
			}
		}
		return false
	})
	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] restored by degraded refresh", issue.ID)
	}
	snapshot := state.Snapshot(time.Now())
	if snapshot.Counts.Blocked != 1 || len(snapshot.Blocked) != 1 || snapshot.Blocked[0].ID != unrelated.ID {
		t.Fatalf("blocked snapshot = count %d rows %#v, want unrelated issue only", snapshot.Counts.Blocked, snapshot.Blocked)
	}
	close(runner.release)
}

func TestRunFetchesCandidatesBeforeObservedStates(t *testing.T) {
	t.Parallel()

	tracker := newParallelFetchConnector()
	orch := newTestOrchestratorWithPollInterval(t, tracker, &staticRunner{}, time.Hour)
	stop := runOrchestrator(t, orch)
	defer stop()

	tracker.waitCandidateStarted(t)
	tracker.releaseFetches()
	tracker.waitStatusStarted(t)

	waitForState(t, orch, func(state orchestrator.State) bool {
		return !state.LastRefreshAt.IsZero()
	})
	if got := tracker.candidateCalls.Load(); got != 1 {
		t.Fatalf("FetchCandidateIssues() calls = %d, want 1", got)
	}
	if got := tracker.statusCalls.Load(); got != 1 {
		t.Fatalf("FetchIssuesByStates() calls = %d, want 1", got)
	}
}

func TestStateReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-5", "digitaldrywood/detent#14", "In Progress")
	tracker := newFakeConnector(issue)
	runner := newBlockingRunner()

	orch := newTestOrchestrator(t, tracker, runner)
	stop := runOrchestrator(t, orch)
	defer stop()

	receiveRunRequest(t, runner.started)

	first, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	delete(first.Running, issue.ID)
	first.Claimed[issue.ID] = orchestrator.Claimed{}

	second, err := orch.State(context.Background())
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if _, ok := second.Running[issue.ID]; !ok {
		t.Fatalf("Running[%q] missing after mutating previous snapshot", issue.ID)
	}
	if second.Claimed[issue.ID].Issue.ID != issue.ID {
		t.Fatalf("Claimed[%q].Issue.ID = %q, want %q", issue.ID, second.Claimed[issue.ID].Issue.ID, issue.ID)
	}

	close(runner.release)
}

func TestRequestRefreshQueuesImmediateTick(t *testing.T) {
	t.Parallel()

	tracker := newFakeConnector()
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchCalls(t, tracker, 1)

	refresh, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	if !refresh.Queued || refresh.Coalesced || len(refresh.Operations) != 2 {
		t.Fatalf("RequestRefresh() = %#v, want queued non-coalesced poll/reconcile", refresh)
	}
	if refresh.Operations[0] != "poll" || refresh.Operations[1] != "reconcile" {
		t.Fatalf("Operations = %#v, want poll/reconcile", refresh.Operations)
	}
	if refresh.RequestedAt.IsZero() {
		t.Fatal("RequestedAt is zero")
	}

	waitForFetchCalls(t, tracker, 2)
}

func TestRequestRefreshPublishesManualOutcome(t *testing.T) {
	t.Parallel()

	tracker := newFakeConnector()
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchCalls(t, tracker, 1)

	refresh, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	if refresh.RequestID == "" {
		t.Fatal("RequestID is empty")
	}
	if refresh.Status != telemetry.RefreshAttemptStatusInProgress {
		t.Fatalf("Status = %q, want %q", refresh.Status, telemetry.RefreshAttemptStatusInProgress)
	}

	waitForFetchCalls(t, tracker, 2)
	state := waitForState(t, orch, func(state orchestrator.State) bool {
		snapshot := state.Snapshot(time.Now())
		return snapshot.Refresh.Manual != nil &&
			snapshot.Refresh.Manual.ID == refresh.RequestID &&
			snapshot.Refresh.Manual.Status == telemetry.RefreshAttemptStatusSucceeded
	})
	manual := state.Snapshot(time.Now()).Refresh.Manual
	if manual == nil {
		t.Fatal("snapshot Refresh.Manual = nil")
		return
	}
	if manual.RequestedAt == nil || !manual.RequestedAt.Equal(refresh.RequestedAt) {
		t.Fatalf("Manual.RequestedAt = %v, want %v", manual.RequestedAt, refresh.RequestedAt)
	}
	if manual.CompletedAt == nil {
		t.Fatal("Manual.CompletedAt = nil")
	}
	if len(manual.Operations) != 2 || manual.Operations[0] != "poll" || manual.Operations[1] != "reconcile" {
		t.Fatalf("Manual.Operations = %#v, want poll/reconcile", manual.Operations)
	}
}

func TestRequestRefreshPublishesFailureWhileDegradedPersists(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("fetch github issues: github transient error: status 504")
	issue := testIssue("issue-refresh", "digitaldrywood/detent#504", "Todo")
	tracker := newFakeConnector(issue)

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchCalls(t, tracker, 1)
	first := waitForState(t, orch, func(state orchestrator.State) bool {
		snapshot := state.Snapshot(time.Now())
		return snapshot.Refresh.LastRefreshAt != nil &&
			len(snapshot.BoardIssues) == 1 &&
			snapshot.BoardIssues[0].ID == issue.ID
	})
	firstRefreshAt := first.Snapshot(time.Now()).Refresh.LastRefreshAt
	if firstRefreshAt == nil {
		t.Fatal("first snapshot Refresh.LastRefreshAt = nil")
	}

	tracker.setFetchCandidateError(transientErr, 2)

	refresh, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	waitForFetchCalls(t, tracker, 2)

	state := waitForStateWithin(t, orch, slowCIIntegrationWaitTimeout, func(state orchestrator.State) bool {
		snapshot := state.Snapshot(time.Now())
		return snapshot.Refresh.Manual != nil &&
			snapshot.Refresh.Manual.ID == refresh.RequestID &&
			snapshot.Refresh.Manual.Status == telemetry.RefreshAttemptStatusFailed
	})
	manual := state.Snapshot(time.Now()).Refresh.Manual
	if manual == nil {
		t.Fatal("snapshot Refresh.Manual = nil")
		return
	}
	if manual.LastError == "" || !strings.Contains(manual.LastError, "status 504") {
		t.Fatalf("Manual.LastError = %q, want status 504", manual.LastError)
	}
	if manual.LastErrorAt == nil || !manual.LastErrorAt.Equal(state.LastRefreshErrorAt) {
		t.Fatalf("Manual.LastErrorAt = %v, want %v", manual.LastErrorAt, state.LastRefreshErrorAt)
	}
	if state.LastRefreshError == "" || !strings.Contains(state.LastRefreshError, "fetch candidate issues failed") {
		t.Fatalf("LastRefreshError = %q, want candidate fetch failure", state.LastRefreshError)
	}
	snapshot := state.Snapshot(time.Now())
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusDegraded {
		t.Fatalf("snapshot Refresh status = %q, want degraded", got)
	}
	if snapshot.Refresh.LastRefreshAt == nil || !snapshot.Refresh.LastRefreshAt.Equal(*firstRefreshAt) {
		t.Fatalf("snapshot Refresh.LastRefreshAt = %v, want preserved successful refresh %v", snapshot.Refresh.LastRefreshAt, firstRefreshAt)
	}
	if len(snapshot.BoardIssues) != 1 || snapshot.BoardIssues[0].ID != issue.ID {
		t.Fatalf("snapshot BoardIssues = %#v, want preserved issue %q", snapshot.BoardIssues, issue.ID)
	}
}

func TestRequestRefreshPreservesBoardAfterObservedStateFailure(t *testing.T) {
	t.Parallel()

	statusErr := errors.New("fetch github issues: github transient error: status 401: Bad credentials")
	candidate := testIssue("issue-refresh", "digitaldrywood/detent#504", "Todo")
	observed := testIssue("issue-backlog", "digitaldrywood/detent#505", "Backlog")
	tracker := newFakeConnector(candidate)
	tracker.setStateIssues(observed)

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress"},
		ObservedStates:      []string{"Backlog"},
		TerminalStates:      []string{"Done"},
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    &staticRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchByStatesCalls(t, tracker, 1)
	first := waitForState(t, orch, func(state orchestrator.State) bool {
		snapshot := state.Snapshot(time.Now())
		return snapshot.Refresh.LastRefreshAt != nil && len(snapshot.BoardIssues) == 2
	})
	firstRefreshAt := first.Snapshot(time.Now()).Refresh.LastRefreshAt
	if firstRefreshAt == nil {
		t.Fatal("first snapshot Refresh.LastRefreshAt = nil")
	}
	want := map[string]string{}
	for _, issue := range first.Snapshot(time.Now()).BoardIssues {
		want[issue.ID] = issue.State
	}
	tracker.setFetchByStatesError(statusErr)

	refresh, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("RequestRefresh() error = %v", err)
	}
	waitForFetchByStatesCalls(t, tracker, 2)

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		snapshot := state.Snapshot(time.Now())
		return snapshot.Refresh.Manual != nil &&
			snapshot.Refresh.Manual.ID == refresh.RequestID &&
			snapshot.Refresh.Manual.Status == telemetry.RefreshAttemptStatusFailed
	})
	snapshot := state.Snapshot(time.Now())
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusDegraded {
		t.Fatalf("snapshot Refresh status = %q, want degraded", got)
	}
	if snapshot.Refresh.LastRefreshAt == nil || !snapshot.Refresh.LastRefreshAt.Equal(*firstRefreshAt) {
		t.Fatalf("snapshot Refresh.LastRefreshAt = %v, want preserved successful refresh %v", snapshot.Refresh.LastRefreshAt, firstRefreshAt)
	}
	got := map[string]string{}
	for _, issue := range snapshot.BoardIssues {
		got[issue.ID] = issue.State
	}
	if !maps.Equal(got, want) {
		t.Fatalf("snapshot BoardIssues = %#v, want preserved board %#v", got, want)
	}
}

func TestInitialRefreshFailureDoesNotClaimSuccessfulRefresh(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("fetch github issues: github transient error: status 401: Bad credentials")
	tracker := newFakeConnector()
	tracker.setFetchCandidateError(transientErr, 1)
	orch := newTestOrchestrator(t, tracker, &staticRunner{})
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForFetchCalls(t, tracker, 1)
	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return strings.Contains(state.LastRefreshError, "Bad credentials")
	})
	snapshot := state.Snapshot(time.Now())
	if got := snapshot.Refresh.ReadinessStatus(); got != telemetry.RefreshStatusDegraded {
		t.Fatalf("snapshot Refresh status = %q, want degraded", got)
	}
	if snapshot.Refresh.LastRefreshAt != nil {
		t.Fatalf("snapshot Refresh.LastRefreshAt = %v, want nil after first failed refresh", snapshot.Refresh.LastRefreshAt)
	}
}

func TestRequestRefreshCoalescesPendingTick(t *testing.T) {
	t.Parallel()

	orch := newTestOrchestrator(t, newFakeConnector(), &staticRunner{})

	first, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("first RequestRefresh() error = %v", err)
	}
	second, err := orch.RequestRefresh(context.Background())
	if err != nil {
		t.Fatalf("second RequestRefresh() error = %v", err)
	}
	if first.Coalesced {
		t.Fatalf("first RequestRefresh().Coalesced = true, want false")
	}
	if !second.Coalesced {
		t.Fatalf("second RequestRefresh().Coalesced = false, want true")
	}
	if second.RequestID == "" {
		t.Fatal("second RequestID is empty")
	}
	if second.Status != telemetry.RefreshAttemptStatusCoalesced {
		t.Fatalf("second Status = %q, want %q", second.Status, telemetry.RefreshAttemptStatusCoalesced)
	}
}

func TestRequestRefreshReturnsStoppedAfterRunStops(t *testing.T) {
	t.Parallel()

	tracker := newFakeConnector()
	orch := newTestOrchestrator(t, tracker, &staticRunner{})
	stop := runOrchestrator(t, orch)
	waitForFetchCalls(t, tracker, 1)
	stop()

	if _, err := orch.RequestRefresh(context.Background()); !errors.Is(err, orchestrator.ErrStopped) {
		t.Fatalf("RequestRefresh() error = %v, want ErrStopped", err)
	}
}

func TestFakeRunnerCompletes(t *testing.T) {
	t.Parallel()

	result, err := orchestrator.FakeRunner{}.Run(context.Background(), orchestrator.RunRequest{
		Issue: testIssue("issue-6", "digitaldrywood/detent#15", "Todo"),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.FinalState != orchestrator.FinalStateCompleted {
		t.Fatalf("FinalState = %q, want %q", result.FinalState, orchestrator.FinalStateCompleted)
	}
}

func newTestOrchestrator(t *testing.T, tracker connector.Connector, runner orchestrator.Runner) *orchestrator.Orchestrator {
	t.Helper()

	return newTestOrchestratorWithPollInterval(t, tracker, runner, 5*time.Millisecond)
}

func newTestOrchestratorWithPollInterval(
	t *testing.T,
	tracker connector.Connector,
	runner orchestrator.Runner,
	pollInterval time.Duration,
) *orchestrator.Orchestrator {
	t.Helper()

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           pollInterval,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return orch
}

func runOrchestrator(t *testing.T, orch *orchestrator.Orchestrator) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx)
	}()

	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(slowCIIntegrationWaitTimeout):
			stateCtx, stateCancel := context.WithTimeout(context.Background(), time.Second)
			state, stateErr := orch.State(stateCtx)
			stateCancel()
			t.Fatalf(
				"timed out waiting for Run() to stop; state=%#v event_history=%#v state_error=%v",
				state,
				state.RecentEvents,
				stateErr,
			)
		}
	}
}

func waitForState(t *testing.T, orch *orchestrator.Orchestrator, ready func(orchestrator.State) bool) orchestrator.State {
	t.Helper()

	return waitForStateWithin(t, orch, time.Second, ready)
}

func waitForStateWithin(t *testing.T, orch *orchestrator.Orchestrator, timeout time.Duration, ready func(orchestrator.State) bool) orchestrator.State {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var lastState orchestrator.State
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		state, err := orch.State(ctx)
		cancel()
		lastState = state
		lastErr = err
		if err == nil && ready(state) {
			return state
		}

		select {
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for orchestrator state; last_state=%#v event_history=%#v last_error=%v",
				lastState,
				lastState.RecentEvents,
				lastErr,
			)
		case <-ticker.C:
		}
	}
}

func waitForFetchCalls(t *testing.T, tracker *fakeConnector, want int) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if tracker.fetchCandidateCalls() >= want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d FetchCandidateIssues() calls; got %d", want, tracker.fetchCandidateCalls())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitForFetchByStatesCalls(t *testing.T, tracker *fakeConnector, want int) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if tracker.fetchByStatesCalls() >= want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d FetchIssuesByStates() calls; got %d", want, tracker.fetchByStatesCalls())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func stateRequestsContain(requests [][]string, want []string) bool {
	wanted := make(map[string]struct{}, len(want))
	for _, state := range want {
		wanted[strings.ToLower(strings.TrimSpace(state))] = struct{}{}
	}
	for _, request := range requests {
		if len(request) != len(wanted) {
			continue
		}
		matched := 0
		for _, state := range request {
			if _, ok := wanted[strings.ToLower(strings.TrimSpace(state))]; ok {
				matched++
			}
		}
		if matched == len(wanted) {
			return true
		}
	}
	return false
}

func receiveRunRequest(t *testing.T, requests <-chan orchestrator.RunRequest) orchestrator.RunRequest {
	t.Helper()

	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner request")
	}

	return orchestrator.RunRequest{}
}

func testIssue(id, identifier, state string) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = identifier
	issue.Title = "Port orchestrator"
	issue.State = state
	issue.URL = "https://github.com/digitaldrywood/detent/issues/10"
	return issue
}

func rankedTestIssue(issue connector.Issue, priority int, createdAt time.Time) connector.Issue {
	issue.Priority = &priority
	issue.CreatedAt = &createdAt
	return issue
}

type planReviewFeedbackConnector struct {
	*fakeConnector
	release chan struct{}
}

func (c *planReviewFeedbackConnector) CreateComment(ctx context.Context, issueID string, body string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
		return c.fakeConnector.CreateComment(ctx, issueID, body)
	}
}

type fakeConnector struct {
	mu                  sync.Mutex
	candidates          []connector.Issue
	stateIssues         []connector.Issue
	fetchCandidateCount int
	fetchByStatesCount  int
	fetchByStatesLog    [][]string
	setFields           []setFieldCall
	comments            []commentCall
	stateUpdates        []stateUpdateCall
	fetchCandidateErr   error
	fetchCandidateErrAt int
	fetchByStatesErr    error
	statusLabelPrefix   string
}

type pendingDispatchConnector struct {
	*fakeConnector
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type dispatchStartBlockingConnector struct {
	*fakeConnector
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *dispatchStartBlockingConnector) SetField(ctx context.Context, issueID string, field string, value string) error {
	c.once.Do(func() {
		close(c.started)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
		return c.fakeConnector.SetField(ctx, issueID, field, value)
	}
}

func (c *pendingDispatchConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	c.once.Do(func() {
		close(c.started)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return c.fakeConnector.FetchCandidateIssues(ctx)
	}
}

type operatorMoveConnector struct {
	*fakeConnector
	issueStateRefreshFailures atomic.Int64
}

func (c *operatorMoveConnector) FetchIssueStatesByIDs(ctx context.Context, ids []string) ([]connector.Issue, error) {
	for {
		remaining := c.issueStateRefreshFailures.Load()
		if remaining <= 0 {
			break
		}
		if c.issueStateRefreshFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, errors.New("blocked status transition refresh degraded")
		}
	}
	return c.fakeConnector.FetchIssueStatesByIDs(ctx, ids)
}

type authHealthConnector struct {
	*fakeConnector
	health connector.AuthHealth
	ok     bool
}

func (c *authHealthConnector) AuthHealth() (connector.AuthHealth, bool) {
	return c.health, c.ok
}

type statusDriftConnector struct {
	*fakeConnector
	drift connector.StatusDrift
	err   error
}

func (c *statusDriftConnector) FetchStatusDrift(context.Context) (connector.StatusDrift, error) {
	return connector.StatusDrift{
		UntrackedOpen: cloneIssues(c.drift.UntrackedOpen),
		OpenTerminal:  cloneIssues(c.drift.OpenTerminal),
		ClosedActive:  cloneIssues(c.drift.ClosedActive),
	}, c.err
}

type setFieldCall struct {
	issueID string
	field   string
	value   string
}

type commentCall struct {
	issueID string
	body    string
}

type stateUpdateCall struct {
	issueID string
	state   string
}

type workflowMetricsRecorder struct {
	mu     sync.Mutex
	events []store.WorkflowPhaseEvent
}

func (r *workflowMetricsRecorder) RecordWorkflowPhaseEvent(_ context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
	return int64(len(r.events)), nil
}

func (r *workflowMetricsRecorder) snapshot() []store.WorkflowPhaseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]store.WorkflowPhaseEvent(nil), r.events...)
}

func newFakeConnector(issues ...connector.Issue) *fakeConnector {
	return &fakeConnector{candidates: cloneIssues(issues)}
}

func newFakeLabelConnector(issues ...connector.Issue) *fakeConnector {
	return &fakeConnector{candidates: cloneIssues(issues), statusLabelPrefix: "detent:"}
}

func (c *fakeConnector) Name() string {
	return "fake"
}

func (c *fakeConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fetchCandidateCount++
	if c.fetchCandidateErr != nil && (c.fetchCandidateErrAt <= 0 || c.fetchCandidateCount >= c.fetchCandidateErrAt) {
		return nil, c.fetchCandidateErr
	}
	return cloneIssues(c.candidates), nil
}

func (c *fakeConnector) FetchIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fetchByStatesCount++
	c.fetchByStatesLog = append(c.fetchByStatesLog, append([]string(nil), states...))
	if c.fetchByStatesErr != nil {
		return nil, c.fetchByStatesErr
	}
	wanted := make(map[string]struct{}, len(states))
	for _, state := range states {
		wanted[strings.ToLower(strings.TrimSpace(state))] = struct{}{}
	}
	issues := make([]connector.Issue, 0, len(c.stateIssues))
	for _, issue := range c.stateIssues {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(issue.State))]; ok {
			issues = append(issues, issue)
		}
	}
	return cloneIssues(issues), nil
}

func (c *fakeConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	byID := make(map[string]connector.Issue, len(c.candidates))
	for _, issue := range c.candidates {
		byID[issue.ID] = issue
	}

	issues := make([]connector.Issue, 0, len(ids))
	for _, id := range ids {
		if issue, ok := byID[id]; ok {
			issues = append(issues, issue)
		}
	}

	return cloneIssues(issues), nil
}

func (c *fakeConnector) FetchIssueComments(_ context.Context, issue connector.Issue) ([]connector.IssueComment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, candidate := range c.candidates {
		if candidate.ID == issue.ID {
			return cloneConnectorComments(candidate.Comments), nil
		}
	}
	for _, stateIssue := range c.stateIssues {
		if stateIssue.ID == issue.ID {
			return cloneConnectorComments(stateIssue.Comments), nil
		}
	}
	return nil, nil
}

func (c *fakeConnector) CreateComment(_ context.Context, issueID string, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.comments = append(c.comments, commentCall{issueID: issueID, body: body})
	return nil
}

func (c *fakeConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stateUpdates = append(c.stateUpdates, stateUpdateCall{issueID: issueID, state: state})
	c.applyIssueStateLocked(issueID, state)
	return nil
}

func (c *fakeConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *fakeConnector) SetField(_ context.Context, issueID string, field string, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setFields = append(c.setFields, setFieldCall{issueID: issueID, field: field, value: value})
	for i := range c.candidates {
		if c.candidates[i].ID != issueID {
			continue
		}
		if c.candidates[i].Fields == nil {
			c.candidates[i].Fields = map[string]string{}
		}
		c.candidates[i].Fields[field] = value
	}
	for i := range c.stateIssues {
		if c.stateIssues[i].ID != issueID {
			continue
		}
		if c.stateIssues[i].Fields == nil {
			c.stateIssues[i].Fields = map[string]string{}
		}
		c.stateIssues[i].Fields[field] = value
	}
	return nil
}

func (c *fakeConnector) hasSetField(issueID string, field string, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, call := range c.setFields {
		if call.issueID == issueID && call.field == field && call.value == value {
			return true
		}
	}
	return false
}

func (c *fakeConnector) setFieldCalls() []setFieldCall {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]setFieldCall(nil), c.setFields...)
}

func (c *fakeConnector) commentCalls() []commentCall {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]commentCall(nil), c.comments...)
}

func (c *fakeConnector) stateUpdateCalls() []stateUpdateCall {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]stateUpdateCall(nil), c.stateUpdates...)
}

func (c *fakeConnector) setIssueComments(issueID string, comments []connector.IssueComment) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.candidates {
		if c.candidates[i].ID == issueID {
			c.candidates[i].Comments = cloneConnectorComments(comments)
		}
	}
	for i := range c.stateIssues {
		if c.stateIssues[i].ID == issueID {
			c.stateIssues[i].Comments = cloneConnectorComments(comments)
		}
	}
}

func (c *fakeConnector) fetchCandidateCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.fetchCandidateCount
}

func (c *fakeConnector) fetchByStatesCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.fetchByStatesCount
}

func (c *fakeConnector) fetchByStatesRequests() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	requests := make([][]string, len(c.fetchByStatesLog))
	for index, request := range c.fetchByStatesLog {
		requests[index] = append([]string(nil), request...)
	}
	return requests
}

func (c *fakeConnector) setStateIssues(issues ...connector.Issue) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stateIssues = cloneIssues(issues)
}

func (c *fakeConnector) setFetchCandidateError(err error, atCall int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fetchCandidateErr = err
	c.fetchCandidateErrAt = atCall
}

func (c *fakeConnector) setFetchByStatesError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fetchByStatesErr = err
}

func (c *fakeConnector) applyIssueStateLocked(issueID string, state string) {
	for i := range c.candidates {
		if c.candidates[i].ID == issueID {
			c.candidates[i].State = state
			c.candidates[i].Labels = c.updatedStatusLabels(c.candidates[i].Labels, state)
		}
	}
	for i := range c.stateIssues {
		if c.stateIssues[i].ID == issueID {
			c.stateIssues[i].State = state
			c.stateIssues[i].Labels = c.updatedStatusLabels(c.stateIssues[i].Labels, state)
		}
	}
}

func (c *fakeConnector) updatedStatusLabels(labels []string, state string) []string {
	if c.statusLabelPrefix == "" {
		return append([]string(nil), labels...)
	}
	next := make([]string, 0, len(labels)+1)
	for _, label := range labels {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(label)), c.statusLabelPrefix) {
			continue
		}
		next = append(next, label)
	}
	next = append(next, c.statusLabelPrefix+statusLabelSlug(state))
	return next
}

func statusLabelSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastSeparator := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastSeparator = false
		default:
			if b.Len() == 0 || lastSeparator {
				continue
			}
			b.WriteByte('-')
			lastSeparator = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func (c *fakeConnector) candidateIssue(issueID string) connector.Issue {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, issue := range c.candidates {
		if issue.ID == issueID {
			return cloneIssues([]connector.Issue{issue})[0]
		}
	}
	return connector.Issue{}
}

func (c *fakeConnector) setCandidatePullRequest(issueID string, pullRequest *connector.PullRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.candidates {
		if c.candidates[i].ID == issueID {
			c.candidates[i].PullRequest = pullRequest
		}
	}
}

func waitForStateUpdate(t *testing.T, tracker *fakeConnector, want stateUpdateCall) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		for _, got := range tracker.stateUpdateCalls() {
			if got == want {
				return
			}
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for state update %#v; got %#v", want, tracker.stateUpdateCalls())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitForWorkflowMetricEvents(t *testing.T, recorder *workflowMetricsRecorder, want int) []store.WorkflowPhaseEvent {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		events := recorder.snapshot()
		if len(events) >= want {
			return events
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d workflow metric events; got %#v", want, events)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func stateUpdateCallsContainInOrder(got []stateUpdateCall, want []stateUpdateCall) bool {
	index := 0
	for _, update := range got {
		if index >= len(want) {
			return true
		}
		if update == want[index] {
			index++
		}
	}
	return index == len(want)
}

func statusLabelCount(labels []string, prefix string) int {
	count := 0
	for _, label := range labels {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(label)), strings.ToLower(prefix)) {
			count++
		}
	}
	return count
}

func labelListContains(labels []string, want string) bool {
	for _, label := range labels {
		if strings.EqualFold(strings.TrimSpace(label), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}

func stateUpdatesEqual(got []stateUpdateCall, want []stateUpdateCall) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func stateListIncludes(states []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	for _, state := range states {
		if strings.ToLower(strings.TrimSpace(state)) == want {
			return true
		}
	}
	return false
}

func recentEventContains(events []telemetry.ActivityEvent, event string, message string) bool {
	for _, candidate := range events {
		if candidate.Event == event && strings.Contains(candidate.Message, message) {
			return true
		}
	}
	return false
}

type transientCleanupConnector struct {
	mu                  sync.Mutex
	candidate           connector.Issue
	cleanupErr          error
	cleanupAttemptCount int
	cleanupFailureAt    int
	changed             chan struct{}
}

func newTransientCleanupConnector(candidate connector.Issue, cleanupErr error) *transientCleanupConnector {
	return &transientCleanupConnector{
		candidate:        candidate,
		cleanupErr:       cleanupErr,
		cleanupFailureAt: 1,
		changed:          make(chan struct{}, 8),
	}
}

func (c *transientCleanupConnector) failCleanupOnAttempt(attempt int) *transientCleanupConnector {
	c.cleanupFailureAt = attempt
	return c
}

func (c *transientCleanupConnector) Name() string {
	return "transient-cleanup"
}

func (c *transientCleanupConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return cloneIssues([]connector.Issue{c.candidate}), nil
}

func (c *transientCleanupConnector) FetchIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if transientCleanupRequest(states) {
		c.cleanupAttemptCount++
		c.changed <- struct{}{}
		if c.cleanupAttemptCount == c.cleanupFailureAt {
			return nil, c.cleanupErr
		}
	}
	return nil, nil
}

func (c *transientCleanupConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range ids {
		if id == c.candidate.ID {
			return cloneIssues([]connector.Issue{c.candidate}), nil
		}
	}
	return nil, nil
}

func (c *transientCleanupConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c *transientCleanupConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c *transientCleanupConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *transientCleanupConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (c *transientCleanupConnector) cleanupAttempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cleanupAttemptCount
}

func (c *transientCleanupConnector) waitForCleanupAttempts(t *testing.T, want int) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if c.cleanupAttempts() >= want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d cleanup attempts; got %d", want, c.cleanupAttempts())
		case <-c.changed:
		}
	}
}

func transientCleanupRequest(states []string) bool {
	return stateListIncludes(states, "Done") &&
		stateListIncludes(states, "Cancelled")
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

type parallelFetchConnector struct {
	candidateStarted chan struct{}
	statusStarted    chan struct{}
	release          chan struct{}
	candidateOnce    sync.Once
	statusOnce       sync.Once
	releaseOnce      sync.Once
	candidateCalls   atomic.Int64
	statusCalls      atomic.Int64
}

func newParallelFetchConnector() *parallelFetchConnector {
	return &parallelFetchConnector{
		candidateStarted: make(chan struct{}),
		statusStarted:    make(chan struct{}),
		release:          make(chan struct{}),
	}
}

func (c *parallelFetchConnector) Name() string {
	return "parallel"
}

func (c *parallelFetchConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	c.candidateCalls.Add(1)
	c.candidateOnce.Do(func() {
		close(c.candidateStarted)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, nil
	}
}

func (c *parallelFetchConnector) FetchIssuesByStates(ctx context.Context, _ []string) ([]connector.Issue, error) {
	c.statusCalls.Add(1)
	c.statusOnce.Do(func() {
		close(c.statusStarted)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.candidateStarted:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, nil
	}
}

func (c *parallelFetchConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *parallelFetchConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c *parallelFetchConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c *parallelFetchConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *parallelFetchConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (c *parallelFetchConnector) waitCandidateStarted(t *testing.T) {
	t.Helper()

	select {
	case <-c.candidateStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for candidate fetch to start")
	}
}

func (c *parallelFetchConnector) waitStatusStarted(t *testing.T) {
	t.Helper()

	select {
	case <-c.statusStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observed status fetch to start")
	}
}

func (c *parallelFetchConnector) releaseFetches() {
	c.releaseOnce.Do(func() {
		close(c.release)
	})
}

type staticRunner struct {
	calls      atomic.Int64
	requestMu  sync.Mutex
	requestLog []orchestrator.RunRequest
	result     orchestrator.RunResult
	err        error
	onRun      func(orchestrator.RunRequest)
}

func (r *staticRunner) Run(_ context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	r.calls.Add(1)
	r.requestMu.Lock()
	r.requestLog = append(r.requestLog, request)
	r.requestMu.Unlock()
	if r.onRun != nil {
		r.onRun(request)
	}
	return r.result, r.err
}

type instantBackendError struct {
	body string
}

func (e instantBackendError) Error() string {
	return "codex turn failed: status failed: " + e.body
}

func (e instantBackendError) BackendErrorBody() string {
	return e.body
}

func (e instantBackendError) BackendErrorMessage() string {
	return "model rejected"
}

func (r *staticRunner) requests() []orchestrator.RunRequest {
	r.requestMu.Lock()
	defer r.requestMu.Unlock()

	return append([]orchestrator.RunRequest(nil), r.requestLog...)
}

type fakeWorkspaceReaper struct {
	mu      sync.Mutex
	result  orchestrator.WorkspaceReapResult
	issues  []connector.Issue
	changed chan struct{}
}

func (r *fakeWorkspaceReaper) ReapWorkspace(_ context.Context, issue connector.Issue) (orchestrator.WorkspaceReapResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.changed == nil {
		r.changed = make(chan struct{}, 8)
	}
	r.issues = append(r.issues, issue)
	r.changed <- struct{}{}
	return r.result, nil
}

func (r *fakeWorkspaceReaper) reapedIssues() []connector.Issue {
	r.mu.Lock()
	defer r.mu.Unlock()

	return cloneIssues(r.issues)
}

func waitForWorkspaceReaps(t *testing.T, reaper *fakeWorkspaceReaper, want int) []connector.Issue {
	t.Helper()

	reaper.mu.Lock()
	if reaper.changed == nil {
		reaper.changed = make(chan struct{}, 8)
	}
	changed := reaper.changed
	reaper.mu.Unlock()

	deadline := time.After(time.Second)
	for {
		if got := reaper.reapedIssues(); len(got) >= want {
			return got
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d workspace reaps; got %#v", want, reaper.reapedIssues())
		case <-changed:
		}
	}
}

type blockingRunner struct {
	started chan orchestrator.RunRequest
	release chan struct{}
}

func newBlockingRunner() *blockingRunner {
	return &blockingRunner{
		started: make(chan orchestrator.RunRequest, 1),
		release: make(chan struct{}),
	}
}

func (r *blockingRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	select {
	case r.started <- request:
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}

	select {
	case <-r.release:
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}
}

type usageStreamingRunner struct {
	started chan orchestrator.RunRequest
	release chan struct{}
	update  orchestrator.UsageUpdate
}

func newUsageStreamingRunner(update orchestrator.UsageUpdate) *usageStreamingRunner {
	return &usageStreamingRunner{
		started: make(chan orchestrator.RunRequest, 1),
		release: make(chan struct{}),
		update:  update,
	}
}

func (r *usageStreamingRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	select {
	case r.started <- request:
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}

	if request.OnUsageUpdate == nil {
		return orchestrator.RunResult{}, errors.New("missing usage update callback")
	}
	if err := request.OnUsageUpdate(r.update); err != nil {
		return orchestrator.RunResult{}, err
	}

	select {
	case <-r.release:
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted, Tokens: r.update.Tokens}, nil
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}
}

type retryRunner struct {
	calls        atomic.Int64
	retryStarted chan orchestrator.RunRequest
	release      chan struct{}
}

func newRetryRunner() *retryRunner {
	return &retryRunner{
		retryStarted: make(chan orchestrator.RunRequest, 1),
		release:      make(chan struct{}),
	}
}

type panicRunner struct{}

func (panicRunner) Run(context.Context, orchestrator.RunRequest) (orchestrator.RunResult, error) {
	panic("boom")
}

func (r *retryRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	call := r.calls.Add(1)
	if call == 1 {
		return orchestrator.RunResult{}, errors.New("runner failed")
	}

	select {
	case r.retryStarted <- request:
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}

	select {
	case <-r.release:
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}
}

func cloneIssues(issues []connector.Issue) []connector.Issue {
	cloned := make([]connector.Issue, len(issues))
	for i, issue := range issues {
		cloned[i] = issue
		if issue.PRNumber != nil {
			prNumber := *issue.PRNumber
			cloned[i].PRNumber = &prNumber
		}
		if issue.PullRequest != nil {
			pullRequest := *issue.PullRequest
			if issue.PullRequest.ActivityAt != nil {
				activityAt := *issue.PullRequest.ActivityAt
				pullRequest.ActivityAt = &activityAt
			}
			if issue.PullRequest.CodexReviewSubmittedAt != nil {
				submittedAt := *issue.PullRequest.CodexReviewSubmittedAt
				pullRequest.CodexReviewSubmittedAt = &submittedAt
			}
			if issue.PullRequest.LatestCodexReviewSubmittedAt != nil {
				submittedAt := *issue.PullRequest.LatestCodexReviewSubmittedAt
				pullRequest.LatestCodexReviewSubmittedAt = &submittedAt
			}
			pullRequest.CodexReviewFindings = append([]connector.PullRequestFinding(nil), issue.PullRequest.CodexReviewFindings...)
			cloned[i].PullRequest = &pullRequest
		}
		cloned[i].BlockedBy = append([]connector.BlockedRef(nil), issue.BlockedBy...)
		cloned[i].Labels = append([]string(nil), issue.Labels...)
		cloned[i].Comments = cloneConnectorComments(issue.Comments)
		if issue.Fields != nil {
			cloned[i].Fields = make(map[string]string, len(issue.Fields))
			maps.Copy(cloned[i].Fields, issue.Fields)
		}
	}
	return cloned
}

func cloneConnectorComments(comments []connector.IssueComment) []connector.IssueComment {
	return append([]connector.IssueComment(nil), comments...)
}
