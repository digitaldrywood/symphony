package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/dependencyline"
	"github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestHumanAndEpicDispatchExclusion(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		body   string
		labels []string
	}{
		{name: "human marker", body: "```detent-human\nschema: 1\n```"},
		{name: "human label", labels: []string{"human-owned"}},
		{name: "tracking epic", labels: []string{"epic"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo"}, TerminalStates: []string{"Done"}, MaxConcurrentAgents: 1})
			issue := dispatchTestIssue("human-prerequisite", "Todo")
			issue.Description = tt.body
			issue.Labels = tt.labels
			state := newState(cfg)
			if got := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, time.Now(), ""); got.dispatchable {
				t.Fatalf("non-executable issue dispatched: %#v", got)
			}
			blocker := dependencyBlocker{Resolved: true, Issue: connector.Issue{ID: "human", Identifier: "owner/repo#1", State: "Backlog", Description: tt.body, Labels: tt.labels}}
			if blockerAutoPromoteEligible(connector.Issue{Identifier: "owner/repo#2"}, blocker, BlockerAutoPromoteConfig{BlockerStates: []string{"Backlog"}, TargetState: "Todo"}, cfg.TerminalStates) {
				t.Fatal("non-executable blocker eligible for promotion")
			}
		})
	}
}

func humanDependencyIssue(evidence string, closed bool) connector.Issue {
	return connector.Issue{ID: "human", Identifier: "owner/repo#10", State: "Backlog", Closed: closed,
		Description: fmt.Sprintf("```detent-human\nschema: 1\nkey: credential\naction: Enable test account\nowner: Account administrator\ncompletion_criteria: Authentication verified\napproval_constraint: Publishing requires separate approval\ncompletion_evidence: %q\n```", evidence)}
}

func TestHumanDependencyChildrenAndPriorPhase(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, evidence string
		closed, ready  bool
	}{
		{name: "waiting"},
		{name: "closure is insufficient", closed: true},
		{name: "evidence while open", evidence: "Verified authentication"},
		{name: "human completed", evidence: "Verified authentication", closed: true, ready: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			blocker := humanDependencyIssue(tt.evidence, tt.closed)
			tracker := memory.New(memory.Config{Issues: []connector.Issue{blocker}, Stateful: true})
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "Rework", "Merging"}, TerminalStates: []string{"Done"}, MaxConcurrentAgents: 2})
			o := &Orchestrator{cfg: cfg, connector: tracker}
			for _, lane := range []string{"Todo", "Rework", "Merging"} {
				for _, id := range []string{"child-one", "child-two"} {
					issue := dispatchTestIssue(id, lane)
					issue.BlockedBy = []connector.BlockedRef{{Identifier: blocker.Identifier}}
					issue.BlockedBy = dependencyResolvedBlockerRefs(o.resolveDependencyBlockers(t.Context(), issue))
					state := newState(cfg)
					decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, time.Now(), "")
					if decision.dispatchable != tt.ready {
						t.Fatalf("%s %s dispatch = %+v", lane, id, decision)
					}
					if !tt.ready && !strings.Contains(decision.detail, "waiting on human prerequisite owner/repo#10") {
						t.Fatalf("missing waiting explanation: %+v", decision)
					}
					if issue.State != lane || len(state.Blocked) != 0 {
						t.Fatal("dependency changed the lane or created a failure park")
					}
				}
			}
			unrelated := dispatchTestIssue("unrelated", "Todo")
			unrelated.BlockedBy = []connector.BlockedRef{{Identifier: "owner/repo#11", State: "Backlog", HumanOwned: true}}
			if !todoBlockedByNonTerminal(unrelated, cfg.TerminalStates) {
				t.Fatal("unrelated human dependency released")
			}
			resolved := dependencyBlocker{Resolved: true, Issue: blocker}
			if got := dependencyBlockerReady(resolved, DependencyAutoUnblockConfig{Readiness: DependencyReadinessTerminalOrMerged}, cfg.TerminalStates); got != tt.ready {
				t.Fatalf("auto-unblock readiness = %v", got)
			}
			if got := implementDependencyTerminal(blocker, cfg.TerminalStates); got != tt.ready {
				t.Fatalf("durable deferral readiness = %v", got)
			}
		})
	}
}

func TestDependencyWaitRestoresLaneAndKeepsDeliverable(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, source, want string
		pr                 bool
	}{
		{name: "unstarted", source: "Todo", want: "Todo"},
		{name: "rework", source: "Rework", want: "Rework", pr: true},
		{name: "merge", source: "Merging", want: "Merging", pr: true},
		{name: "existing PR", source: "In Progress", want: "Rework", pr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			issue := dispatchTestIssue("child", tt.source)
			if tt.pr {
				issue.PullRequest = &connector.PullRequest{Number: 40, State: "open", HeadSHA: "accepted-head"}
			}
			tracker := memory.New(memory.Config{Issues: []connector.Issue{issue}, Stateful: true})
			o := &Orchestrator{cfg: normalizeConfig(Config{}), connector: tracker}
			state := newState(o.cfg)
			o.finishImplementDependencyDeferral(t.Context(), &state, issue, time.Now())
			current, err := tracker.FetchIssueStatesByIDs(t.Context(), []string{issue.ID})
			if err != nil || len(current) != 1 || current[0].State != tt.want {
				t.Fatalf("lane = %+v, %v", current, err)
			}
			if tt.pr && (current[0].PullRequest == nil || current[0].PullRequest.HeadSHA != "accepted-head") {
				t.Fatal("PR lost while waiting")
			}
		})
	}
}

func TestHumanClosurePreservesBreakerPark(t *testing.T) {
	t.Parallel()
	child := dependencyAutoUnblockIssue("child", "Blocked")
	child.Identifier = "owner/repo#20"
	child.Description = "Depends on: owner/repo#10"
	blocker := humanDependencyIssue("Verified authentication", true)
	tracker := &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{child}, hydratedIssues: []connector.Issue{child}, blockers: []connector.Issue{blocker}}
	o := dependencyAutoUnblockOrchestrator(tracker, DependencyAutoUnblockConfig{Enabled: true, SourceStates: []string{"Blocked"}, TargetState: "Todo"})
	state := newState(o.cfg)
	state.Blocked[child.ID] = Blocked{Issue: child, Reason: repeatedFailureCircuitBreakerCause}
	if got := o.autoUnblockDependencyIssues(t.Context(), &state, []connector.Issue{child}, time.Now()); len(got) != 0 {
		t.Fatal("closure bypassed breaker")
	}
	if len(tracker.updates) != 0 || state.Blocked[child.ID].Reason != repeatedFailureCircuitBreakerCause {
		t.Fatal("breaker was acknowledged")
	}
	if o.applyBlockerAutoPromote(t.Context(), &state, connector.Issue{Identifier: "owner/repo#21"}, child, "Todo", time.Now()) {
		t.Fatal("auto-promotion bypassed breaker")
	}
}

func TestExistingPRHumanDeferralPreservesProgress(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		stats     DiffStats
		deferWork bool
	}{
		{name: "published PR waits", stats: DiffStats{Status: "clean", HeadSHA: "accepted-head", CommitsAhead: 2}, deferWork: true},
		{name: "unpublished work remains saved while waiting", stats: DiffStats{Status: "clean", HeadSHA: "new-head", UnpushedCommits: 1}, deferWork: true},
		{name: "dirty work remains saved while waiting", stats: DiffStats{Status: "dirty", HeadSHA: "accepted-head", FilesChanged: 1}, deferWork: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			issue := implementProgressIssue("accepted-head")
			issue.State = "Rework"
			issue.Description = "Depends on: owner/repo#10"
			issue.Comments = []connector.IssueComment{{Body: implementProgressWorkpadComment("owner/repo#10", ""), URL: "https://github.test/workpad"}}
			tracker := &implementProgressConnector{hydrated: issue, refreshed: issue, resolvedBlockers: []connector.Issue{humanDependencyIssue("", false)}}
			o := &Orchestrator{cfg: normalizeConfig(Config{}), connector: tracker}
			decision := o.evaluateImplementCompletionProgress(t.Context(), Running{Issue: issue, DiffStats: tt.stats}, FinalStateCompleted, false)
			if decision.DependencyDeferral != tt.deferWork {
				t.Fatalf("deferral = %+v", decision)
			}
			if tt.deferWork && (decision.Outcome != store.WorkAttemptTerminalSuccess || decision.Block || decision.Issue.PullRequest.HeadSHA != "accepted-head" || decision.Issue.State != "Rework") {
				t.Fatalf("waiting changed deliverable or charged a failure: %+v", decision)
			}
		})
	}
}

type humanToolTracker struct {
	*memory.Connector
	dependent string
	err       error
}

func (tracker *humanToolTracker) EnsureHumanPrerequisite(_ context.Context, dependent string, _ connector.HumanPrerequisiteRequest) (connector.HumanPrerequisiteResult, error) {
	tracker.dependent = dependent
	return connector.HumanPrerequisiteResult{Issue: connector.Issue{Identifier: "owner/repo#10"}}, tracker.err
}

func TestHumanPrerequisiteToolScope(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, tool, input string
		fail, success     bool
	}{
		{name: "scoped", tool: "ensure_human_prerequisite", input: `{}`, success: true},
		{name: "unknown tool", tool: "other", input: `{}`},
		{name: "malformed", tool: "ensure_human_prerequisite", input: `{`},
		{name: "extra field", tool: "ensure_human_prerequisite", input: `{"dependent":"other/repo#1"}`},
		{name: "extra document", tool: "ensure_human_prerequisite", input: `{} {}`},
		{name: "tracker error", tool: "ensure_human_prerequisite", input: `{}`, fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &humanToolTracker{Connector: memory.New(memory.Config{})}
			if tt.fail {
				tracker.err = errors.New("tracker unavailable")
			}
			o := &Orchestrator{connector: tracker}
			request := RunRequest{Issue: connector.Issue{Identifier: "owner/repo#20"}}
			o.attachHumanPrerequisiteTool(&request)
			result, err := request.AgentToolHandler(t.Context(), runner.AgentToolCall{Name: tt.tool, Arguments: json.RawMessage(tt.input)})
			if err != nil || result.Success != tt.success {
				t.Fatalf("tool result = %+v, %v", result, err)
			}
			if tracker.dependent != "" && tracker.dependent != request.Issue.Identifier {
				t.Fatal("tool escaped assigned issue")
			}
		})
	}
	o := &Orchestrator{connector: memory.New(memory.Config{})}
	request := RunRequest{}
	o.attachHumanPrerequisiteTool(&request)
	if request.AgentToolHandler != nil {
		t.Fatal("unsupported tracker offered a mutation tool")
	}
}

func TestHumanDeferralRestartRequiresCompletionEvidence(t *testing.T) {
	t.Parallel()
	for _, ready := range []bool{false, true} {
		blocker := humanDependencyIssue("", true)
		if ready {
			blocker = humanDependencyIssue("Verified authentication", true)
		}
		issue := implementProgressIssueWithoutPR()
		o := &Orchestrator{cfg: normalizeConfig(Config{}), connector: hydratingDispatchConnector{issue: issue, blockers: []connector.Issue{blocker}}, workAttempts: &recordingWorkAttemptStore{history: []store.WorkAttempt{implementProgressDependencyDeferralHistoryAttempt(1, blocker.Identifier, "Backlog")}}}
		if got := len(o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue})); (got == 1) != ready {
			t.Fatalf("restart released=%v, completion verified=%v", got == 1, ready)
		}
		if !ready {
			decisions := o.workAttempts.(*recordingWorkAttemptStore).decisions
			if len(decisions) != 1 || !strings.Contains(decisions[0].WaitReason, "waiting on human prerequisite owner/repo#10") {
				t.Fatalf("missing current explanation evidence: %+v", decisions)
			}
		}
	}
}

func assertHumanDependencyBoundary(t *testing.T, body, refs string) {
	t.Helper()
	issue := connector.Issue{Description: body, Closed: true, State: "Done"}
	if connector.HumanOwned(issue) && !connector.HumanPrerequisiteReady(issue) && implementDependencyTerminal(issue, []string{"Done"}) {
		t.Fatal("unverified human completion released dependency")
	}
	updated, err := dependencyline.Append(refs, "owner/repo", "owner/repo#42")
	if err != nil {
		return
	}
	retry, err := dependencyline.Append(updated, "owner/repo", "#42")
	if err != nil || retry != updated || !strings.HasPrefix(updated, refs) {
		t.Fatal("dependency mutation lost content or duplicated an edge")
	}
}
