package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/explain"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

func TestCompletedDependencyWaitReleasesOwnedMergeReservation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		foreignOwner  bool
		completionErr error
		wantBlocked   bool
	}{
		{name: "saved wait releases repository"},
		{name: "another issue retains reservation", foreignOwner: true, wantBlocked: true},
		{name: "failed persistence retains reservation", completionErr: errors.New("attempt store unavailable"), wantBlocked: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 8, 0, 10, 33, 0, time.UTC)
			issue := nativeMergeQueueTestIssue(2279, "success")
			issue.Comments = []connector.IssueComment{{Body: implementProgressWorkpadComment("digitaldrywood/detent#2282", "")}}
			tracker := &implementProgressConnector{refreshed: issue, resolvedBlockers: []connector.Issue{{ID: "blocker", Identifier: "digitaldrywood/detent#2282", State: "Todo"}}}
			attempts := &implementProgressAttemptStore{completionErr: tt.completionErr}
			cfg := normalizeConfig(Config{MaxConcurrentAgents: 1, ActiveStates: []string{"Todo", "Merging"}, TerminalStates: []string{"Done"}})
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)
			owner := issue
			if tt.foreignOwner {
				owner = nativeMergeQueueTestIssue(2200, "success")
			}
			original := reserveMergeCandidate(&state, owner, now)
			state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: 1, Mode: runpkg.RunModeImplement}
			state.Claimed[issue.ID] = Claimed{Issue: issue}
			o.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, CompletedAt: now, Result: runpkg.RunResult{FinalState: FinalStateCompleted, DiffStats: DiffStats{Status: "clean", HeadSHA: issue.PullRequest.HeadSHA}}})
			if tt.completionErr == nil {
				record := implementProgressRecordFromCompletion(t, attempts.completions[0])
				if !record.DependencyDeferral {
					t.Fatal("completion did not persist the dependency wait")
				}
			}
			next := nativeMergeQueueTestIssue(2282, "success")
			later := now.Add(time.Minute)
			reconcileMergeReservations(&state, []connector.Issue{next}, cfg, later)
			reservation, blocked := mergeReservationBlocks(&state, next, later)
			if blocked != tt.wantBlocked {
				t.Fatalf("next merge blocked = %v, want %v; reservation = %+v", blocked, tt.wantBlocked, reservation)
			}
			if tt.wantBlocked {
				if reservation != original {
					t.Fatalf("unreleased reservation changed: %+v, want %+v", reservation, original)
				}
				return
			}
			plan := newDispatchPlanner(cfg).plan(&state, []connector.Issue{next}, later, dispatchPlanHooks{})
			if len(plan.Dispatches) != 1 || plan.Dispatches[0].IssueID != next.ID {
				t.Fatalf("dispatches = %+v, want prerequisite merge", plan.Dispatches)
			}
			resumed := reserveMergeCandidate(&state, issue, later)
			if !resumed.StartedAt.Equal(later) || resumed.ReleasedReason != "" {
				t.Fatalf("resumed issue did not acquire a fresh reservation: %+v", resumed)
			}
		})
	}
}

func TestCompletedDependencyWaitRequiresCurrentMachineSignal(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		status       string
		humanAction  string
		owner        string
		refreshErr   error
		blockerState string
		invalid      bool
		wantWait     bool
	}{
		{name: "machine wait", status: workpad.StatusBlocked, owner: "orchestrator", wantWait: true},
		{name: "independent work", status: workpad.StatusInProgress},
		{name: "body dependency alone"},
		{name: "human action", status: workpad.StatusBlocked, humanAction: "Approve publishing"},
		{name: "human owned blocker", status: workpad.StatusBlocked, owner: "human"},
		{name: "stale workpad", status: workpad.StatusBlocked, refreshErr: errors.New("tracker unavailable")},
		{name: "invalid workpad", status: workpad.StatusBlocked, invalid: true},
		{name: "dependency already completed", status: workpad.StatusBlocked, blockerState: "Done"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := implementProgressIssueWithoutPR()
			issue.Description = "Depends on: digitaldrywood/detent#2282"
			body := implementProgressWorkpadComment("digitaldrywood/detent#2282", tt.humanAction)
			body = strings.Replace(body, "status: blocked", "status: "+tt.status, 1)
			if tt.owner != "" {
				body = strings.Replace(body, "    reason:", "    owner: "+tt.owner+"\n    reason:", 1)
			}
			if tt.invalid {
				body = strings.Replace(body, "schema: 1", "schema: 99", 1)
			}
			if tt.status != "" {
				issue.Comments = []connector.IssueComment{{Body: body}}
			}
			blocker := connector.Issue{ID: "blocker", Identifier: "digitaldrywood/detent#2282", State: firstNonBlank(tt.blockerState, "Todo")}
			tracker := &implementProgressConnector{refreshed: issue, refreshErr: tt.refreshErr, resolvedBlockers: []connector.Issue{blocker}}
			o := &Orchestrator{cfg: normalizeConfig(Config{TerminalStates: []string{"Done"}}), connector: tracker}
			decision := o.evaluateImplementCompletionProgress(t.Context(), Running{Issue: issue, DiffStats: DiffStats{Status: "clean", UnpushedCommits: 1}}, FinalStateCompleted, false)
			if decision.DependencyDeferral != tt.wantWait {
				t.Fatalf("dependency deferral = %v, want %v (%s)", decision.DependencyDeferral, tt.wantWait, decision.Reason)
			}
			if !tt.wantWait {
				o.workAttempts = &implementProgressAttemptStore{}
				if got := o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue}); len(got) != 1 {
					t.Fatal("dependency declared before its barrier prevented independent work")
				}
			}
		})
	}
}

func TestCompletedDependencyWaitPreservesUnrelatedParks(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{repeatedFailureCircuitBreakerCause, dispatchLoopDetectedReason, workpadBlockedUnactionedReason} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			issue := implementProgressIssueWithoutPR()
			issue.State = "Blocked"
			issue.Comments = []connector.IssueComment{{Body: implementProgressWorkpadComment("digitaldrywood/detent#2282", "")}}
			tracker := &implementProgressConnector{refreshed: issue, resolvedBlockers: []connector.Issue{{ID: "blocker", Identifier: "digitaldrywood/detent#2282", State: "Todo"}}}
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress", "Rework"}, TerminalStates: []string{"Done"}})
			attempts := &implementProgressAttemptStore{}
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)
			park := Blocked{Issue: issue, Reason: reason}
			state.Blocked[issue.ID] = park
			state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: 1, Mode: runpkg.RunModeImplement}
			state.Claimed[issue.ID] = Claimed{Issue: issue}
			o.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, CompletedAt: time.Now(), Result: runpkg.RunResult{FinalState: FinalStateCompleted, DiffStats: DiffStats{Status: "clean", UnpushedCommits: 1}}})
			if state.Blocked[issue.ID].Reason != reason || len(tracker.updates) != 0 {
				t.Fatalf("wait acknowledged unrelated park: %v, %v", state.Blocked, tracker.updates)
			}
			completion := attempts.completions[0]
			attempts.history = []store.WorkAttempt{{WorkerMetadataJSON: completion.WorkerMetadataJSON}}
			tracker.resolvedBlockers[0].State = "Done"
			candidates := o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue})
			if len(candidates) != 1 || o.dispatchable(candidates[0], &state, time.Now()) || state.Blocked[issue.ID].Reason != reason {
				t.Fatal("dependency completion cleared an unrelated hold")
			}
		})
	}
}

func TestCompletedDependencyWaitAdmitsPrerequisiteAfterRestart(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		lane          string
		withPR        bool
		blockerClosed bool
	}{
		{name: "saved commit", lane: "In Progress"},
		{name: "saved PR", lane: "Rework", withPR: true},
		{name: "merge return lane", lane: "Merging", withPR: true},
		{name: "dependency completed", lane: "Rework", blockerClosed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			now := time.Date(2026, 9, 8, 0, 10, 33, 0, time.UTC)
			issue := implementProgressIssueWithoutPR()
			if tt.withPR {
				issue = implementProgressIssue("published-head", "Test")
			}
			issue.State = tt.lane
			issue.AssignedToWorker = true
			issue.BranchName = "saved-work"
			blocker := dispatchTestIssue("blocker", "Todo")
			blocker.Identifier = "digitaldrywood/detent#2282"
			issue.Description = "Depends on: " + blocker.Identifier
			issue.Comments = []connector.IssueComment{{Body: implementProgressWorkpadComment(blocker.Identifier, "")}}
			issue.BlockedBy = []connector.BlockedRef{{ID: blocker.ID, Identifier: blocker.Identifier, State: blocker.State}}
			unrelated := dispatchTestIssue("unrelated", "Todo")
			unrelated.Identifier = "digitaldrywood/detent#1"
			tracker := memory.New(memory.Config{Stateful: true, Issues: []connector.Issue{issue, blocker, unrelated}})
			cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent", Weight: 1}, MaxConcurrentAgents: 1, PrioritizeUnblockers: true, ActiveStates: []string{"Todo", "In Progress", "Rework", "Merging"}, TerminalStates: []string{"Done"}, DispatchPriorityByState: []string{"Merging", "Rework", "In Progress", "Todo"}})
			path := filepath.Join(t.TempDir(), "detent.db")
			backend, err := store.Open(ctx, store.Config{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := backend.Close(); err != nil {
					t.Error(err)
				}
			})
			attempts := backend.(store.WorkAttemptStore)
			attemptID, err := attempts.StartWorkAttempt(ctx, store.WorkAttemptStart{ProjectID: cfg.Project.ID, IssueID: issue.ID, Identifier: issue.Identifier, WorkerType: workAttemptWorkerType(issue, runpkg.RunModeImplement), Lane: issue.State, StartedAt: now.Add(-time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			globalGate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 1}))
			slot, acquired, err := globalGate.TryAcquire(ctx, cfg.Project, scheduler.SlotRequest{State: issue.State}, now)
			if err != nil || !acquired {
				t.Fatalf("initial slot: %v, %v", acquired, err)
			}
			workspace := t.TempDir()
			savedPath := filepath.Join(workspace, "saved-change.txt")
			if err := os.WriteFile(savedPath, []byte("saved implementation"), 0o600); err != nil {
				t.Fatal(err)
			}
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts, globalDispatchGate: globalGate}
			state := newState(cfg)
			state.Running[issue.ID] = Running{Issue: issue, Attempt: 1, WorkAttemptID: attemptID, Mode: runpkg.RunModeImplement, StartedAt: now.Add(-time.Minute), WorkspacePath: workspace, globalSlot: slot}
			state.Claimed[issue.ID] = Claimed{Issue: issue}
			o.handleRunResult(ctx, &state, runpkg.Completion{IssueID: issue.ID, CompletedAt: now, Request: runpkg.RunRequest{Mode: runpkg.RunModeImplement}, Result: runpkg.RunResult{FinalState: FinalStateCompleted, DiffStats: DiffStats{Status: "clean", HeadSHA: "saved-head", UnpushedCommits: 1}}})
			if globalGate.PoolSnapshot().Available != 1 || len(state.Claimed) != 0 || len(state.Retry) != 0 {
				t.Fatal("completed wait did not release capacity")
			}
			current, err := tracker.FetchIssueStatesByIDs(ctx, []string{issue.ID})
			wantLane := tt.lane
			if wantLane == "In Progress" {
				wantLane = "Rework"
			}
			if err != nil || len(current) != 1 || current[0].State != wantLane || current[0].BranchName != issue.BranchName {
				t.Fatalf("return lane or workspace branch lost: %+v, %v", current, err)
			}
			if tt.withPR && (current[0].PullRequest == nil || current[0].PullRequest.HeadSHA != "published-head") {
				t.Fatal("wait lost the PR")
			}
			if contents, err := os.ReadFile(savedPath); err != nil || string(contents) != "saved implementation" {
				t.Fatalf("saved workspace changed: %q, %v", contents, err)
			}
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			backend, err = store.Open(ctx, store.Config{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			if tt.blockerClosed {
				if err := tracker.UpdateIssueState(ctx, blocker.ID, "Done"); err != nil {
					t.Fatal(err)
				}
				current[0].BlockedBy[0].State = "Done"
			}
			runner := newWorkerHostRunner()
			o = &Orchestrator{cfg: cfg, connector: tracker, workAttempts: backend.(store.WorkAttemptStore), globalDispatchGate: globalGate, supervisor: newTestSupervisor(t, runner, cfg), runResults: make(chan runpkg.Completion, 1)}
			state = newState(cfg)
			candidates := []connector.Issue{current[0], unrelated, blocker}
			if tt.blockerClosed {
				candidates = candidates[:2]
			}
			o.dispatchReadyIssues(ctx, &state, candidates, now.Add(time.Minute))
			wantIssue := blocker.ID
			if tt.blockerClosed {
				wantIssue = issue.ID
			}
			if len(state.Running) != 1 || state.Running[wantIssue].Issue.ID != wantIssue {
				t.Fatalf("capacity-one dispatch: %v, want %s", sortedKeys(state.Running), wantIssue)
			}
			t.Cleanup(func() {
				o.cancelRunning(&state, wantIssue)
				select {
				case <-o.runResults:
				case <-time.After(10 * time.Second):
					t.Error("worker did not finish after cancellation")
				}
			})
			if request := receiveWorkerHostRunRequest(t, runner.started); request.Issue.ID != wantIssue {
				t.Fatalf("launched %s, want %s", request.Issue.ID, wantIssue)
			}
			for tick := 2; tick <= 4; tick++ {
				o.dispatchReadyIssues(ctx, &state, candidates, now.Add(time.Duration(tick)*time.Minute))
				if len(state.Running) != 1 || state.Running[wantIssue].Issue.ID != wantIssue {
					t.Fatal("later tick displaced the executable worker")
				}
			}
		})
	}
}

func TestCompletedDependencyWaitPreservesSavedWork(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		withPR    bool
		updatedPR bool
		stats     DiffStats
	}{
		{name: "recorded saved commit without PR", stats: DiffStats{Status: "clean", UnpushedCommits: 1, CommitsAhead: 1}},
		{name: "saved diff without PR", stats: DiffStats{Status: "changed", FilesChanged: 1, Fingerprint: "saved-diff"}},
		{name: "unavailable diff without PR"},
		{name: "new PR", updatedPR: true, stats: DiffStats{Status: "clean"}},
		{name: "saved commit with PR", withPR: true, stats: DiffStats{Status: "clean", UnpushedCommits: 1}},
		{name: "saved diff with PR", withPR: true, stats: DiffStats{Status: "changed", FilesChanged: 1, Fingerprint: "saved-diff"}},
		{name: "repeated clean wait", stats: DiffStats{Status: "clean"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := implementProgressIssueWithoutPR()
			if tt.withPR {
				issue = implementProgressIssue("published-head", "Test")
			}
			issue.Identifier = "digitaldrywood/detent#2279"
			issue.Description = "Depends on: digitaldrywood/detent#2282"
			issue.Comments = []connector.IssueComment{{Body: implementProgressWorkpadComment("digitaldrywood/detent#2282", "")}}
			blocker := connector.Issue{ID: "blocker-2282", Identifier: "digitaldrywood/detent#2282", State: "Todo"}
			tracker := &implementProgressConnector{refreshed: issue, hydrated: issue, resolvedBlockers: []connector.Issue{blocker}}
			attempts := &implementProgressAttemptStore{}
			cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}, AutoPromote: AutoPromoteConfig{NoProgressLimit: 3}, ActiveStates: []string{"Todo", "In Progress", "Rework"}, TerminalStates: []string{"Done"}})
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			stats := tt.stats
			if diffStatsPresent(stats) {
				stats.HeadSHA = "saved-head"
			}
			for index, completedAt := range []time.Time{
				time.Date(2026, 9, 8, 0, 10, 33, 0, time.UTC),
				time.Date(2026, 9, 8, 0, 12, 50, 0, time.UTC),
				time.Date(2026, 9, 8, 0, 15, 27, 0, time.UTC),
			} {
				state := newState(cfg)
				running := Running{Issue: issue, Attempt: 1, WorkAttemptID: int64(index + 1), Mode: runpkg.RunModeImplement, StartedAt: completedAt.Add(-time.Minute), WorkspacePath: t.TempDir(), DiffStats: stats,
					DispatchLoopStart: dispatchLoopTestStart(issue.State, autoPromoteReworkSignatureFromIssue(issue, AutoPromoteSummaryFromIssue(issue)), implementProgressDiffStatsFromDiffStats(stats))}
				state.Running[issue.ID] = running
				state.Claimed[issue.ID] = Claimed{Issue: issue}
				o.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, CompletedAt: completedAt, Request: runpkg.RunRequest{Mode: runpkg.RunModeImplement}, Result: runpkg.RunResult{FinalState: FinalStateCompleted, DiffStats: stats, PullRequestUpdated: tt.updatedPR}})
				if len(attempts.completions) != index+1 {
					t.Fatalf("attempt %d did not complete", index+1)
				}
				completion := attempts.completions[index]
				record := implementProgressRecordFromCompletion(t, completion)
				if !record.DependencyDeferral || record.Reason != implementDependencyDeferralReason || completion.TerminalState != store.WorkAttemptTerminalSuccess {
					t.Fatalf("attempt %d failed to preserve wait: %+v", index+1, record)
				}
				if len(state.Retry) != 0 || len(state.Running) != 0 || len(state.Claimed) != 0 || len(state.Blocked) != 0 {
					t.Fatalf("wait retained capacity or scheduled another session: retries=%v running=%v claimed=%v blocked=%v", state.Retry, state.Running, state.Claimed, state.Blocked)
				}
				attempts.history = append([]store.WorkAttempt{{ID: running.WorkAttemptID, Lane: issue.State, WorkerType: "implement", TerminalState: completion.TerminalState, CompletedAt: completedAt, WorkerMetadataJSON: completion.WorkerMetadataJSON}}, attempts.history...)
				restarted := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
				if got := restarted.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue}); len(got) != 0 {
					t.Fatalf("attempt %d permits redispatch while prerequisite is open", index+1)
				}
			}
			tracker.resolvedBlockers[0].State = "Done"
			if got := o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue}); len(got) != 1 {
				t.Fatal("completed dependency did not release wait")
			}
		})
	}
}

func TestDependencyDeferralRefusalDetails(t *testing.T) {
	t.Parallel()
	const ref = "owner/repo#10"
	human := humanDependencyIssue("", true)
	readyHuman := humanDependencyIssue("Verified authentication", true)
	for _, tt := range []struct {
		name       string
		tracker    connector.Connector
		historyErr error
		ref        string
		want       []string
	}{
		{name: "ordinary blocker", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, State: "Rework"}}}, ref: ref, want: []string{"waiting on dependency " + ref}},
		{name: "missing resolver", tracker: struct{ connector.Connector }{memory.New(memory.Config{})}, ref: ref, want: []string{"dependency resolution unavailable", ref, "issue reference resolver unavailable"}},
		{name: "failed lookup", tracker: dependencyFailureTracker{Connector: memory.New(memory.Config{}), resolveErr: errors.New("tracker unavailable")}, ref: ref, want: []string{"dependency resolution unavailable", ref, "tracker unavailable"}},
		{name: "missing result", tracker: hydratingDispatchConnector{}, ref: ref, want: []string{"missing blocker result for " + ref}},
		{name: "missing identifier", tracker: hydratingDispatchConnector{}, want: []string{"no blocker identifiers"}},
		{name: "history unavailable", historyErr: errors.New("history offline"), want: []string{"dependency deferral history unavailable: history offline"}},
		{name: "human pending", tracker: hydratingDispatchConnector{blockers: []connector.Issue{human}}, ref: ref, want: []string{"waiting on human prerequisite " + ref, "closure and completion evidence required"}},
		{name: "human ready", tracker: hydratingDispatchConnector{blockers: []connector.Issue{readyHuman}}, ref: ref},
		{name: "terminal", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, State: "Done"}}}, ref: ref},
		{name: "closed", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, Closed: true}}}, ref: ref},
		{name: "merged", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, PullRequest: &connector.PullRequest{State: "merged"}}}}, ref: ref},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := implementProgressIssueWithoutPR()
			attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{implementProgressDependencyDeferralHistoryAttempt(1, tt.ref, "Todo")}, historyErr: tt.historyErr}
			o := &Orchestrator{cfg: normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}, TerminalStates: []string{"Done"}}), connector: tt.tracker, workAttempts: attempts}
			filtered := o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue})
			if len(tt.want) == 0 {
				if len(filtered) != 1 || len(attempts.decisions) != 0 {
					t.Fatalf("ready dependency: candidates=%+v refusals=%+v", filtered, attempts.decisions)
				}
				return
			}
			if len(filtered) != 0 || len(attempts.decisions) != 1 {
				t.Fatalf("suppression: candidates=%+v refusals=%+v", filtered, attempts.decisions)
			}
			decision := attempts.decisions[0]
			if decision.Reason != dispatchSkipBlockedByDependency || decision.Result != store.SchedulerDecisionResultSkipped || decision.Selected || decision.IssueID != issue.ID {
				t.Fatalf("refusal=%+v", decision)
			}
			for _, detail := range tt.want {
				if !strings.Contains(decision.WaitReason, detail) {
					t.Fatalf("detail=%q, want %q", decision.WaitReason, detail)
				}
			}
		})
	}
}

func TestDependencyDeferralEvidenceAndReleaseAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "detent.db")
	cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}, TerminalStates: []string{"Done"}})
	issue := implementProgressIssueWithoutPR()
	history := implementProgressDependencyDeferralHistoryAttempt(1, "digitaldrywood/detent#134", "Todo")
	backend, err := store.Open(ctx, store.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	attempts := backend.(store.WorkAttemptStore)
	id, err := attempts.StartWorkAttempt(ctx, store.WorkAttemptStart{ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, WorkerType: "agent", Lane: issue.State, StartedAt: history.CompletedAt.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if err := attempts.CompleteWorkAttempt(ctx, store.WorkAttemptCompletion{AttemptID: id, CompletedAt: history.CompletedAt, Status: store.WorkAttemptStatusTerminal, TerminalState: store.WorkAttemptTerminalSuccess, WorkerMetadataJSON: history.WorkerMetadataJSON}); err != nil {
		t.Fatal(err)
	}
	o := &Orchestrator{cfg: cfg, workAttempts: attempts}
	o.recordSchedulerDecision(ctx, nil, history.CompletedAt.Add(time.Minute), dispatchPlanDecision{Issue: issue, Selected: true}, string(store.SchedulerDecisionResultSelected), "selected")
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, state   string
		wantCandidate bool
	}{
		{name: "restart suppresses open dependency", state: "Todo"},
		{name: "restart releases completed dependency", state: "Done", wantCandidate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reopened, err := store.Open(ctx, store.Config{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			o := &Orchestrator{cfg: cfg, workAttempts: reopened.(store.WorkAttemptStore), connector: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: "digitaldrywood/detent#134", State: tt.state}}}}
			reader := reopened.(store.IssueSchedulerDecisionStore)
			service := explain.New(explain.Dependencies{Scheduler: reader})
			query := explain.Query{ProjectID: "detent", IssueID: issue.ID}
			if tt.wantCandidate {
				explanation, err := service.Explain(ctx, query)
				if err != nil || explanation.Eligibility.State != explain.EligibilityRefused {
					t.Fatalf("persisted refusal after restart=%+v, error=%v", explanation.Eligibility, err)
				}
			}
			filtered := o.filterImplementDependencyDeferrals(ctx, []connector.Issue{issue})
			if (len(filtered) == 1) != tt.wantCandidate {
				t.Fatalf("candidates=%+v", filtered)
			}
			decisions, err := reader.ListIssueSchedulerDecisions(ctx, store.IssueSchedulerDecisionQuery{Identity: store.IssueIdentity{ProjectID: "detent", IssueID: issue.ID}})
			if err != nil || len(decisions) != 2 {
				t.Fatalf("decisions=%+v, error=%v", decisions, err)
			}
			explanation, err := service.Explain(ctx, query)
			if err != nil || explanation.Eligibility.State != explain.EligibilityRefused || explanation.Eligibility.Latest == nil {
				t.Fatalf("explanation=%+v, error=%v", explanation.Eligibility, err)
			}
			latest := explanation.Eligibility.Latest
			if latest.EvidenceID != fmt.Sprintf("scheduler:%d", decisions[0].ID) || latest.Reason != "waiting on dependency digitaldrywood/detent#134" {
				t.Fatalf("latest=%+v, decisions=%+v", latest, decisions)
			}
		})
	}
}
