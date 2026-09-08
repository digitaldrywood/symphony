package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/provenance"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestCrossHostParkRecovery(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{spendProgressReason, dispatchLoopDetectedReason, noProgressLimitReason, "operator_investigation"} {
		t.Run(cause, func(t *testing.T) {
			t.Parallel()
			parkedAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "In Progress")
			blocker := dependencyAutoUnblockIssue("2108", "Done")
			issue.BlockedBy = []connector.BlockedRef{{Identifier: blocker.Identifier, State: blocker.State, Source: connector.BlockedRefSourceNative}}
			tracker := &crossHostParkConnector{
				dependencyAutoUnblockConnector: &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}, blockers: []connector.Issue{blocker}},
				now:                            parkedAt,
			}
			pathA := filepath.Join(t.TempDir(), "host-a.db")
			storeA := openCompletionDeferralStoreWithoutCleanup(t, pathA)
			storeB := openValidatorMemoStore(t)
			newHost := func(db store.Store) *Orchestrator {
				cfg := dependencyAutoUnblockOrchestrator(tracker.dependencyAutoUnblockConnector, DependencyAutoUnblockConfig{Enabled: true}).cfg
				host, err := New(cfg, Dependencies{Connector: tracker})
				if err != nil {
					t.Fatal(err)
				}
				host.workflowMetrics = db
				return host
			}
			hostA, hostB := newHost(storeA), newHost(storeB)
			stateA, stateB := newState(hostA.cfg), newState(hostB.cfg)
			park := workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{
				Owner: blockedRecoveryOwnerHuman, Cause: cause, CauseFingerprint: "current-park", TargetState: "Todo",
			}}
			if err := hostA.updateIssueStateByIDStrictWithMetadata(t.Context(), &stateA, issue.ID, issue, "Blocked", parkedAt, cause, park); err != nil {
				t.Fatal(err)
			}
			if err := tracker.CreateComment(t.Context(), issue.ID, "Routed this issue to Blocked because resource consumption continued without any PR evidence.\n\n- reason: "+cause); err != nil {
				t.Fatal(err)
			}
			tracker.now = parkedAt.Add(57 * time.Second)
			if got := hostA.recoverBlockedIssues(t.Context(), &stateA, tracker.stateIssues, tracker.now); len(got) != 0 {
				t.Fatalf("parking host released human-owned park: %v", got)
			}
			if got := hostA.autoUnblockDependencyIssues(t.Context(), &stateA, tracker.stateIssues, tracker.now); len(got) != 0 {
				t.Fatalf("parking host cleared park through dependencies: %v", got)
			}
			if got := hostB.autoUnblockDependencyIssues(t.Context(), &stateB, tracker.stateIssues, tracker.now); len(got) != 0 {
				t.Errorf("peer unparked human-owned %s: %v", cause, got)
			}
			if err := tracker.UpdateIssueState(t.Context(), issue.ID, "Todo"); err != nil {
				t.Fatal(err)
			}
			if err := tracker.CreateComment(t.Context(), issue.ID, "Dependency blockers cleared. Moved this issue from Blocked to Todo. Cleared dependencies: #2108 (state: Done)"); err != nil {
				t.Fatal(err)
			}
			stateA.Pipeline = cloneIssues(tracker.stateIssues)
			hostA.refreshCurrentLaneEntries(t.Context(), &stateA, parkedAt.Add(5*time.Minute))
			if err := storeA.Close(); err != nil {
				t.Fatal(err)
			}
			hostA = newHost(openCompletionDeferralStore(t, pathA))
			stateA = newState(hostA.cfg)
			hostA.dispatchReadyIssues(t.Context(), &stateA, tracker.stateIssues, parkedAt.Add(5*time.Minute))
			if len(stateA.Running) != 0 {
				t.Fatalf("restarted parking host redispatched unacknowledged park: %v", stateA.Running)
			}
			blocked := cloneIssue(tracker.stateIssues[0])
			blocked.State = "Blocked"
			hostA.recordLaneTransition(t.Context(), blocked, "Todo", parkedAt.Add(6*time.Minute), "operator_kanban_move", workflowLaneMetadata{
				Provenance: provenance.AttributionFromSource(provenance.SourceHumanSession, provenance.Actor{Login: "shared-user", Kind: "User"}),
			})
			stateA = newState(hostA.cfg)
			hostA.dispatchReadyIssues(t.Context(), &stateA, tracker.stateIssues, parkedAt.Add(7*time.Minute))
			if len(stateA.Running) != 1 {
				t.Fatalf("explicit human acknowledgement did not permit dispatch: blocked=%v retry=%v", stateA.Blocked, stateA.Retry)
			}
			<-hostA.runResults
		})
	}
}

type crossHostParkConnector struct {
	*dependencyAutoUnblockConnector
	now            time.Time
	commentErr     error
	instanceLogin  string
	updateDelay    time.Duration
	afterUpdate    func()
	updateErr      error
	commentFailure func(string) error
}

func (c *crossHostParkConnector) InstanceLogin() string {
	return c.instanceLogin
}

func (c *crossHostParkConnector) UpdateIssueState(ctx context.Context, issueID, state string) error {
	if c.updateErr != nil {
		return c.updateErr
	}
	c.now = c.now.Add(c.updateDelay)
	if err := c.dependencyAutoUnblockConnector.UpdateIssueState(ctx, issueID, state); err != nil {
		return err
	}
	for i := range c.stateIssues {
		if c.stateIssues[i].ID == issueID {
			c.stateIssues[i].State = state
			c.stateIssues[i].StageUpdatedAt = timePointer(c.now)
			c.stateIssues[i].StageUpdatedActor = connector.IssueActor{Login: "shared-user", Kind: "User"}
		}
	}
	if c.afterUpdate != nil {
		c.afterUpdate()
	}
	return nil
}

func (c *crossHostParkConnector) CreateComment(ctx context.Context, issueID, body string) error {
	if c.commentFailure != nil {
		if err := c.commentFailure(body); err != nil {
			return err
		}
	}
	if err := c.dependencyAutoUnblockConnector.CreateComment(ctx, issueID, body); err != nil {
		return err
	}
	for i := range c.stateIssues {
		if c.stateIssues[i].ID == issueID {
			c.stateIssues[i].Comments = append(c.stateIssues[i].Comments, connector.IssueComment{
				Body: body, AuthorLogin: "shared-user", AuthorKind: "User", AuthorAuthorized: true, CreatedAt: timePointer(c.now),
			})
		}
	}
	return nil
}

func (c *crossHostParkConnector) FetchIssueComments(_ context.Context, issue connector.Issue) ([]connector.IssueComment, error) {
	if c.commentErr != nil {
		return nil, c.commentErr
	}
	for _, current := range c.stateIssues {
		if current.ID == issue.ID {
			return cloneIssueComments(current.Comments), nil
		}
	}
	return nil, nil
}

func TestCrossHostDependencyRecoveryControls(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		body          string
		authorized    bool
		age           time.Duration
		readErr       error
		wantHold      bool
		author        string
		instanceLogin string
	}{
		{name: "ordinary dependency"},
		{name: "legacy spend park", body: "Routed this issue to Blocked because resource consumption continued without any PR evidence.\n\n- reason: " + spendProgressReason, authorized: true, wantHold: true},
		{name: "legacy loop park", body: "Routed this issue to Blocked: loop detected after 3 dispatches.\n\n- reason: " + dispatchLoopDetectedReason, authorized: true, wantHold: true},
		{name: "legacy no progress park", body: "Routed this issue to Blocked because the implement worker completed repeatedly without deliverable progress.\n\n- reason: " + noProgressLimitReason, authorized: true, wantHold: true},
		{name: "legacy human owner", body: "Routed this issue to Blocked.\n\n- reason: investigate\n- owner: human", authorized: true, wantHold: true},
		{name: "untrusted marker", body: trackerRecoveryParkPrefix + `{"schema":1,"owner":"human"}` + "\n```"},
		{name: "own integration marker", body: trackerRecoveryParkPrefix + `{"schema":1,"owner":"human"}` + "\n```", author: "detent[bot]", instanceLogin: "detent[bot]", wantHold: true},
		{name: "other integration marker", body: trackerRecoveryParkPrefix + `{"schema":1,"owner":"human"}` + "\n```", author: "other[bot]", instanceLogin: "detent[bot]"},
		{name: "prior park occupancy", body: trackerRecoveryParkPrefix + `{"schema":1,"owner":"human"}` + "\n```", authorized: true, age: time.Hour},
		{name: "malformed marker", body: trackerRecoveryParkPrefix + "invalid\n```", authorized: true, wantHold: true},
		{name: "unsupported marker", body: trackerRecoveryParkPrefix + `{"schema":2,"owner":"human"}` + "\n```", authorized: true, wantHold: true},
		{name: "incomplete marker", body: trackerRecoveryParkPrefix + `{"schema":1`, authorized: true, wantHold: true},
		{name: "dependency marker", body: trackerRecoveryParkPrefix + `{"schema":1,"owner":"orchestrator","cause":"dependency"}` + "\n```", authorized: true},
		{name: "comment read unavailable", readErr: errors.New("tracker unavailable"), wantHold: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "Blocked")
			issue.StageUpdatedAt = &now
			blocker := dependencyAutoUnblockIssue("2108", "Done")
			issue.BlockedBy = []connector.BlockedRef{{Identifier: blocker.Identifier, State: blocker.State}}
			issue.Comments = []connector.IssueComment{{Body: tt.body, AuthorAuthorized: tt.authorized, AuthorLogin: tt.author, CreatedAt: timePointer(now.Add(-tt.age))}}
			tracker := &crossHostParkConnector{dependencyAutoUnblockConnector: &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}, blockers: []connector.Issue{blocker}}, now: now.Add(57 * time.Second), commentErr: tt.readErr}
			tracker.instanceLogin = tt.instanceLogin
			host := dependencyAutoUnblockOrchestrator(tracker.dependencyAutoUnblockConnector, DependencyAutoUnblockConfig{Enabled: true})
			host.connector = tracker
			host.workflowMetrics = openValidatorMemoStore(t)
			state := newState(host.cfg)
			got := host.autoUnblockDependencyIssues(t.Context(), &state, tracker.stateIssues, tracker.now)
			if (len(got) == 0) != tt.wantHold {
				t.Fatalf("transitions = %v, wantHold = %t", got, tt.wantHold)
			}
		})
	}
}

func TestCrossHostParkProtectsVisibleTransition(t *testing.T) {
	t.Parallel()
	for _, delay := range []time.Duration{0, 2 * time.Minute} {
		t.Run(delay.String(), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "In Progress")
			blocker := dependencyAutoUnblockIssue("2108", "Done")
			issue.BlockedBy = []connector.BlockedRef{{Identifier: blocker.Identifier, State: blocker.State}}
			tracker := &crossHostParkConnector{dependencyAutoUnblockConnector: &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}, blockers: []connector.Issue{blocker}}, now: now, updateDelay: delay}
			hostA := blockedCauseTestOrchestrator(tracker.dependencyAutoUnblockConnector)
			hostA.connector = tracker
			hostA.workflowMetrics = openValidatorMemoStore(t)
			hostB := dependencyAutoUnblockOrchestrator(tracker.dependencyAutoUnblockConnector, DependencyAutoUnblockConfig{Enabled: true})
			hostB.connector = tracker
			hostB.workflowMetrics = openValidatorMemoStore(t)
			stateA, stateB := newState(hostA.cfg), newState(hostB.cfg)
			observed := false
			tracker.afterUpdate = func() {
				tracker.afterUpdate = nil
				observed = true
				if got := hostB.autoUnblockDependencyIssues(t.Context(), &stateB, tracker.stateIssues, tracker.now); len(got) != 0 {
					t.Error("peer unparked the visible transition before marker completion")
				}
			}
			metadata := workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: spendProgressReason, CauseFingerprint: "current-park"}}
			if err := hostA.updateIssueStateByIDStrictWithMetadata(t.Context(), &stateA, issue.ID, issue, "Blocked", now, spendProgressReason, metadata); err != nil {
				t.Fatal(err)
			}
			if !observed {
				t.Fatal("peer never observed the visible transition")
			}
			if got := hostB.autoUnblockDependencyIssues(t.Context(), &stateB, tracker.stateIssues, tracker.now); len(got) != 0 {
				t.Fatal("peer unparked completed recovery marker")
			}
		})
	}
}

func TestRecoveryParkAcknowledgementBoundaries(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		owner  string
		reason string
		source provenance.Source
		want   bool
	}{
		{name: "shared identity observation", owner: "human", reason: "tracker_state_observed", source: provenance.SourceTrackerObservation},
		{name: "peer dependency transition", owner: "human", reason: "dependency_auto_unblock", source: provenance.SourceDetentInstance},
		{name: "human action", owner: "human", reason: "operator_kanban_move", source: provenance.SourceHumanSession, want: true},
		{name: "explicit CLI move", owner: "human", reason: "kanban_move", source: provenance.SourceExternalAutomation, want: true},
		{name: "explicit field move", owner: "human", reason: "kanban_move_field", source: provenance.SourceHumanSession, want: true},
		{name: "configured cooldown", owner: "orchestrator", reason: workflowActionCauseBlockedRecovery, source: provenance.SourceDetentInstance, want: true},
		{name: "cooldown cannot acknowledge human park", owner: "human", reason: workflowActionCauseBlockedRecovery, source: provenance.SourceDetentInstance},
		{name: "spend park is not ready PR recovery", owner: "orchestrator", reason: workflowActionBlockedReadyPRReconciliation, source: provenance.SourceDetentInstance},
		{name: "obsolete artifact recovery", owner: "orchestrator", reason: "obsolete_artifact_spend_recovery", source: provenance.SourceDetentInstance, want: true},
		{name: "dependency cannot acknowledge cooldown park", owner: "orchestrator", reason: "dependency_auto_unblock", source: provenance.SourceDetentInstance},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := recoveryParkAcknowledged(store.WorkflowPhaseEvent{Reason: tt.reason}, workflowLaneMetadata{Provenance: provenance.AttributionFromSource(tt.source, provenance.Actor{Login: "shared-user", Kind: "User"})}, workflowLaneBlockedRecoveryMetadata{Owner: tt.owner, Cause: spendProgressReason})
			if got != tt.want {
				t.Fatalf("acknowledged = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRecoveryParkMarkerIncludesFingerprint(t *testing.T) {
	t.Parallel()
	tracker := &dependencyAutoUnblockConnector{}
	host := blockedCauseTestOrchestrator(tracker)
	park := newTrackerRecoveryPark("Blocked", workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: "investigate", CauseFingerprint: "current-fingerprint"}})
	if err := host.publishTrackerRecoveryPark(t.Context(), "2131", park); err != nil {
		t.Fatal(err)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, `"cause_fingerprint":"current-fingerprint"`) {
		t.Fatalf("comments = %v, want tracker-visible park fingerprint", tracker.comments)
	}
}

func TestRecoveryParkAcknowledgementRearmsForNextPark(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	issue := dependencyAutoUnblockIssue("2131", "In Progress")
	host := blockedCauseTestOrchestrator(&dependencyAutoUnblockConnector{})
	host.workflowMetrics = openValidatorMemoStore(t)
	state := newState(host.cfg)
	park := workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: dispatchLoopDetectedReason, CauseFingerprint: "same-cause-fingerprint"}}
	for cycle := range 2 {
		at := now.Add(time.Duration(cycle) * time.Hour)
		host.recordLaneTransition(t.Context(), issue, "Blocked", at, dispatchLoopDetectedReason, park)
		candidate := cloneIssue(issue)
		candidate.State = "Todo"
		observedAt := at.Add(time.Minute)
		candidate.StageUpdatedAt = &observedAt
		host.recordObservedLaneEntry(t.Context(), candidate, observedAt, provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{Login: "shared-user", Kind: "User"}))
		host.retainUnacknowledgedRecoveryParks(t.Context(), &state, []connector.Issue{candidate})
		if _, held := state.Blocked[issue.ID]; !held {
			t.Fatalf("cycle %d reused an old acknowledgement", cycle)
		}
		blocked := cloneIssue(candidate)
		blocked.State = "Blocked"
		host.recordLaneTransition(t.Context(), blocked, "Todo", at.Add(2*time.Minute), "kanban_move", workflowLaneMetadata{})
		host.retainUnacknowledgedRecoveryParks(t.Context(), &state, []connector.Issue{candidate})
		if _, held := state.Blocked[issue.ID]; held {
			t.Fatalf("cycle %d retained explicitly acknowledged park", cycle)
		}
	}
}

func TestCrossHostParkPreservesConfiguredCooldownRecovery(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{spendProgressReason, dispatchLoopDetectedReason, noProgressLimitReason} {
		t.Run(cause, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "Todo")
			tracker := &crossHostParkConnector{dependencyAutoUnblockConnector: &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}}, now: now}
			host := blockedCauseTestOrchestrator(tracker.dependencyAutoUnblockConnector)
			host.connector = tracker
			host.workflowMetrics = openValidatorMemoStore(t)
			host.recoveryInspector = staticBlockedRecoveryInspector{snapshot: runpkg.BlockedRecoverySnapshot{ConfigFingerprint: "unchanged", Health: "ready"}}
			state := newState(host.cfg)
			metadata := host.newBlockedRecoveryMetadata(t.Context(), issue, RunModeImplement, cause, blockedRecoveryPredicateFingerprintChange, "Todo", DiffStats{})
			if err := host.updateIssueStateByIDStrictWithMetadata(t.Context(), &state, issue.ID, issue, "Blocked", now, cause, metadata); err != nil {
				t.Fatal(err)
			}
			tracker.now = now.Add(defaultBreakerParkCooldown)
			if got := host.recoverBlockedIssues(t.Context(), &state, tracker.stateIssues, tracker.now); len(got) != 1 {
				t.Fatalf("configured cooldown did not recover: %v", state.Blocked)
			}
			host.retainUnacknowledgedRecoveryParks(t.Context(), &state, tracker.stateIssues)
			if _, held := state.Blocked[issue.ID]; held {
				t.Fatal("dispatch guard rejected configured cooldown recovery")
			}
		})
	}
}

func TestRecoveryParkEventIdentity(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		event store.WorkflowPhaseEvent
		want  bool
	}{
		{name: "same id", event: store.WorkflowPhaseEvent{IssueID: "2131"}, want: true},
		{name: "other id sharing URL", event: store.WorkflowPhaseEvent{IssueID: "other", IssueURL: "https://tracker.test/issue"}},
		{name: "identifier fallback", event: store.WorkflowPhaseEvent{Identifier: "DIGITALDRYWOOD/DETENT#2131"}, want: true},
		{name: "other identifier sharing URL", event: store.WorkflowPhaseEvent{Identifier: "digitaldrywood/detent#2108", IssueURL: "https://tracker.test/issue"}},
		{name: "URL fallback", event: store.WorkflowPhaseEvent{IssueURL: "https://tracker.test/issue"}, want: true},
		{name: "no identity"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			issue := connector.Issue{ID: "2131", Identifier: "digitaldrywood/detent#2131", URL: "https://tracker.test/issue"}
			if got := recoveryParkEventMatchesIssue(tt.event, issue); got != tt.want {
				t.Fatalf("matches = %t, want %t", got, tt.want)
			}
		})
	}
}

func (c *crossHostParkConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	var issues []connector.Issue
	for _, id := range ids {
		for _, issue := range c.stateIssues {
			if issue.ID == id {
				issues = append(issues, cloneIssue(issue))
			}
		}
	}
	return issues, nil
}

func TestTrackerRecoveryParkOperationSettlement(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		phase       string
		operationID string
		authorized  bool
		current     bool
		wantHold    bool
	}{
		{name: "pending protects slow transition", wantHold: true},
		{name: "applied current park", phase: "applied", operationID: "park", authorized: true, current: true, wantHold: true},
		{name: "applied prior occupancy", phase: "applied", operationID: "park", authorized: true},
		{name: "cancelled transition", phase: "cancelled", operationID: "park", authorized: true},
		{name: "untrusted cancellation", phase: "cancelled", operationID: "park", wantHold: true},
		{name: "other operation cancellation", phase: "cancelled", operationID: "other", authorized: true, wantHold: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "Blocked")
			issue.StageUpdatedAt = &now
			marker := trackerRecoveryPark{Schema: 1, Owner: "human", Cause: spendProgressReason, OperationID: "park", Phase: "pending"}
			encode := func(marker trackerRecoveryPark) string {
				t.Helper()
				data, err := json.Marshal(marker)
				if err != nil {
					t.Fatal(err)
				}
				return trackerRecoveryParkPrefix + string(data) + "\n```"
			}
			issue.Comments = []connector.IssueComment{{Body: encode(marker), AuthorAuthorized: true, CreatedAt: timePointer(now.Add(-time.Hour))}}
			if tt.phase != "" {
				marker.Phase, marker.OperationID = tt.phase, tt.operationID
				at := now.Add(-time.Hour)
				if tt.current {
					at = now
				}
				issue.Comments = append(issue.Comments, connector.IssueComment{Body: encode(marker), AuthorAuthorized: tt.authorized, CreatedAt: &at})
			}
			host := blockedCauseTestOrchestrator(&dependencyAutoUnblockConnector{})
			if got := host.trackerRecoveryParkHold(t.Context(), issue); (got != "") != tt.wantHold {
				t.Fatalf("hold = %q, wantHold = %t", got, tt.wantHold)
			}
		})
	}
}

func TestTrackerRecoveryParkPublicationFailures(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		failPhase   string
		updateErr   error
		wantUpdates int
		wantHold    bool
	}{
		{name: "pending publication fails", failPhase: "pending"},
		{name: "applied publication fails", failPhase: "applied", wantUpdates: 1, wantHold: true},
		{name: "ambiguous transition failure", updateErr: errors.New("connection lost"), wantHold: true},
		{name: "known rejected transition", updateErr: connector.ErrStateUpdateBlocked},
		{name: "cancellation publication fails", updateErr: connector.ErrStateUpdateBlocked, failPhase: "cancelled", wantHold: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
			issue := dependencyAutoUnblockIssue("2131", "In Progress")
			tracker := &crossHostParkConnector{dependencyAutoUnblockConnector: &dependencyAutoUnblockConnector{stateIssues: []connector.Issue{issue}}, now: now, updateErr: tt.updateErr}
			tracker.commentFailure = func(body string) error {
				if tt.failPhase != "" && strings.Contains(body, `"phase":"`+tt.failPhase+`"`) {
					return errors.New("comment unavailable")
				}
				return nil
			}
			host := blockedCauseTestOrchestrator(tracker.dependencyAutoUnblockConnector)
			host.connector = tracker
			host.workflowMetrics = openValidatorMemoStore(t)
			state := newState(host.cfg)
			metadata := workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: spendProgressReason}}
			if err := host.updateIssueStateByIDStrictWithMetadata(t.Context(), &state, issue.ID, issue, "Blocked", now, spendProgressReason, metadata); err == nil {
				t.Fatal("expected publication or transition failure")
			}
			if len(tracker.updates) != tt.wantUpdates {
				t.Fatalf("updates = %v, want %d", tracker.updates, tt.wantUpdates)
			}
			candidate := cloneIssue(tracker.stateIssues[0])
			candidate.State = "Blocked"
			candidate.StageUpdatedAt = timePointer(now.Add(time.Hour))
			if got := host.trackerRecoveryParkHold(t.Context(), candidate); (got != "") != tt.wantHold {
				t.Fatalf("hold = %q, wantHold = %t", got, tt.wantHold)
			}
		})
	}
}
