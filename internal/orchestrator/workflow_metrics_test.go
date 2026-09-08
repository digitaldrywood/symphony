package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/lessons"
	"github.com/digitaldrywood/detent/internal/provenance"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestTickPreservesOrchestratorTransitionSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	issue := connector.Issue{
		ID:           "issue-closed",
		Identifier:   "digitaldrywood/detent#1131",
		State:        "Human Review",
		Closed:       true,
		ClosedReason: "completed",
	}
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates:      []string{"Blocked", "Done"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	tracker := &workflowMetricsConnector{stateIssues: []connector.Issue{cloneIssue(issue)}}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.BoardIssues = []connector.Issue{cloneIssue(issue)}
	state.Pipeline = []connector.Issue{cloneIssue(issue)}

	orch.tick(context.Background(), &state, now)

	snapshot := state.Snapshot(now.Add(time.Second))
	if len(snapshot.BoardIssues) != 1 {
		t.Fatalf("snapshot BoardIssues len = %d, want 1", len(snapshot.BoardIssues))
	}
	if got := snapshot.BoardIssues[0].State; got != "Done" {
		t.Fatalf("snapshot BoardIssues state = %q, want Done", got)
	}
	if len(snapshot.Pipeline) != 1 {
		t.Fatalf("snapshot Pipeline len = %d, want 1", len(snapshot.Pipeline))
	}
	if got := snapshot.Pipeline[0].State; got != "Done" {
		t.Fatalf("snapshot Pipeline state = %q, want Done", got)
	}

	external := cloneIssue(issue)
	external.Closed = false
	external.ClosedReason = ""
	tracker.stateIssues = []connector.Issue{external}
	orch.tick(context.Background(), &state, now.Add(time.Minute))

	snapshot = state.Snapshot(now.Add(time.Minute + time.Second))
	if len(snapshot.BoardIssues) != 1 || snapshot.BoardIssues[0].State != "Human Review" {
		t.Fatalf("snapshot BoardIssues = %#v, want external Human Review state", snapshot.BoardIssues)
	}
	if len(snapshot.Pipeline) != 1 || snapshot.Pipeline[0].State != "Human Review" {
		t.Fatalf("snapshot Pipeline = %#v, want external Human Review state", snapshot.Pipeline)
	}
}

func TestApplyAutoPromoteDecisionUpdatesSnapshotBeforePoll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		trackerName string
	}{
		{name: "label", trackerName: "github_label"},
		{name: "ProjectV2", trackerName: "github_project_v2"},
		{name: "issue field", trackerName: "github_issue_field"},
		{name: "local", trackerName: "local"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transitionAt := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
			previousStageAt := transitionAt.Add(-5 * time.Minute)
			issue := connector.Issue{
				ID:             "issue-promote",
				Identifier:     "digitaldrywood/detent#1131",
				State:          "In Progress",
				StageUpdatedAt: &previousStageAt,
			}
			tracker := &workflowMetricsConnector{name: tt.trackerName, stateEnteredAt: transitionAt, stateEnteredAtFound: true}
			orch := &Orchestrator{connector: tracker}
			state := newState(Config{})
			state.BoardIssues = []connector.Issue{cloneIssue(issue)}
			state.Pipeline = []connector.Issue{cloneIssue(issue)}

			applied := orch.applyAutoPromoteDecision(
				context.Background(),
				&state,
				issue,
				AutoPromoteSummary{},
				autoPromoteDecision(AutoPromoteActionPromote, AutoPromoteReasonReady),
				"Merging",
				transitionAt,
			)
			if !applied {
				t.Fatal("applyAutoPromoteDecision() = false, want true")
			}

			snapshot := state.Snapshot(transitionAt.Add(time.Second))
			if len(snapshot.BoardIssues) != 1 {
				t.Fatalf("snapshot BoardIssues len = %d, want 1", len(snapshot.BoardIssues))
			}
			if got := snapshot.BoardIssues[0].State; got != "Merging" {
				t.Fatalf("snapshot BoardIssues state = %q, want Merging", got)
			}
			if snapshot.BoardIssues[0].StageUpdatedAt == nil || !snapshot.BoardIssues[0].StageUpdatedAt.Equal(transitionAt) {
				t.Fatalf("snapshot BoardIssues StageUpdatedAt = %v, want %v", snapshot.BoardIssues[0].StageUpdatedAt, transitionAt)
			}
			if len(snapshot.Pipeline) != 1 {
				t.Fatalf("snapshot Pipeline len = %d, want 1", len(snapshot.Pipeline))
			}
			if got := snapshot.Pipeline[0].State; got != "Merging" {
				t.Fatalf("snapshot Pipeline state = %q, want Merging", got)
			}
			if tracker.fetches != 0 {
				t.Fatalf("tracker fetches = %d, want none", tracker.fetches)
			}
		})
	}
}

func TestResolveCurrentLaneEnteredAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 18, 0, 0, 0, time.UTC)
	createdAt := now.Add(-4 * time.Hour)
	enteredAt := now.Add(-time.Hour)
	transitionAt := now.Add(-10 * time.Minute)
	updatedAt := now.Add(-5 * time.Minute)

	tests := []struct {
		name             string
		issue            connector.Issue
		previous         time.Time
		trackerEnteredAt time.Time
		observedAt       time.Time
		events           []store.WorkflowPhaseEvent
		want             time.Time
	}{
		{
			name: "same-lane update keeps previous entry",
			issue: connector.Issue{
				State:     "In Progress",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			previous: enteredAt,
			want:     enteredAt,
		},
		{
			name: "same-lane event may not move entry forward",
			issue: connector.Issue{
				State:     "In Progress",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			previous: enteredAt,
			events: []store.WorkflowPhaseEvent{
				{ID: 1, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "In Progress", Status: "entered", StartedAt: updatedAt},
			},
			want: enteredAt,
		},
		{
			name: "agent-moved lane uses tracker transition",
			issue: connector.Issue{
				State:     "Blocked",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			trackerEnteredAt: transitionAt,
			observedAt:       now,
			want:             transitionAt,
		},
		{
			name:       "missing tracker and phase event uses poll observation",
			issue:      connector.Issue{State: "Blocked"},
			observedAt: now,
			want:       now,
		},
		{
			name: "lane change uses transition event",
			issue: connector.Issue{
				State:     "Merging",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			events: []store.WorkflowPhaseEvent{
				{ID: 1, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "In Progress", Status: "entered", StartedAt: enteredAt},
				{ID: 2, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "merging", Status: "ENTERED", StartedAt: transitionAt},
			},
			want: transitionAt,
		},
		{
			name: "leave and return uses latest durable entry",
			issue: connector.Issue{
				State:     "In Progress",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			previous: enteredAt,
			events: []store.WorkflowPhaseEvent{
				{ID: 1, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "In Progress", Status: "entered", StartedAt: enteredAt},
				{ID: 2, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "Merging", Status: "entered", StartedAt: transitionAt.Add(-time.Minute)},
				{ID: 3, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "In Progress", Status: "entered", StartedAt: transitionAt},
			},
			want: transitionAt,
		},
		{
			name: "tracker reentry supersedes stale durable entry",
			issue: connector.Issue{
				State:     "Human Review",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			previous:         enteredAt,
			trackerEnteredAt: transitionAt,
			events: []store.WorkflowPhaseEvent{
				{ID: 1, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "Human Review", Status: "entered", StartedAt: enteredAt},
			},
			want: transitionAt,
		},
		{
			name: "restart restores durable entry",
			issue: connector.Issue{
				State:     "In Progress",
				CreatedAt: &createdAt,
				UpdatedAt: &updatedAt,
			},
			events: []store.WorkflowPhaseEvent{
				{ID: 1, PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "in progress", Status: "entered", StartedAt: enteredAt},
			},
			want: enteredAt,
		},
		{
			name: "missing phase events uses tracker fallback",
			issue: connector.Issue{
				State:     "Todo",
				CreatedAt: &createdAt,
				UpdatedAt: &enteredAt,
			},
			want: enteredAt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := resolveCurrentLaneEnteredAt(tt.issue, tt.previous, tt.trackerEnteredAt, tt.observedAt, tt.events)
			if !got.Equal(tt.want) {
				t.Fatalf("resolveCurrentLaneEnteredAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObservedLaneAttribution(t *testing.T) {
	t.Parallel()

	issue := connector.Issue{ID: "issue-1761"}
	operatorActor := connector.IssueActor{Login: "corylanou", Kind: "User"}
	activeAgent := newState(Config{})
	activeAgent.Running[issue.ID] = Running{Issue: issue}
	tests := []struct {
		name          string
		state         *State
		actor         connector.IssueActor
		wantOrigin    provenance.Origin
		wantInitiator provenance.Initiator
	}{
		{name: "active worker with user actor remains indeterminate", state: &activeAgent, actor: operatorActor, wantOrigin: provenance.OriginIndeterminate, wantInitiator: provenance.InitiatorIndeterminate},
		{name: "unverified user actor", state: statePointer(newState(Config{})), actor: operatorActor, wantOrigin: provenance.OriginIndeterminate, wantInitiator: provenance.InitiatorIndeterminate},
		{name: "external bot", state: statePointer(newState(Config{})), actor: connector.IssueActor{Login: "audit[bot]", Kind: "Bot"}, wantOrigin: provenance.OriginAutomation, wantInitiator: provenance.InitiatorExternalAutomation},
		{name: "missing state and actor", wantOrigin: provenance.OriginIndeterminate, wantInitiator: provenance.InitiatorIndeterminate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := observedLaneAttribution(tt.state, issue, tt.actor)
			if got.Origin != tt.wantOrigin || got.Initiator != tt.wantInitiator {
				t.Fatalf("observedLaneAttribution() = %#v, want origin %q initiator %q", got, tt.wantOrigin, tt.wantInitiator)
			}
		})
	}
}

func statePointer(state State) *State {
	return &state
}

func TestRefreshCurrentLaneEntriesPersistsPollObservationAcrossRestart(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	enteredAt := time.Date(2026, 7, 10, 14, 36, 55, 0, time.UTC)
	updatedAt := enteredAt.Add(2 * time.Hour)

	backend, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{
		ID:         "issue-1162",
		Identifier: "digitaldrywood/detent#1162",
		State:      "Blocked",
	}}
	orch := &Orchestrator{cfg: normalizeConfig(Config{}), workflowMetrics: backend}
	orch.refreshCurrentLaneEntries(ctx, &state, enteredAt)

	timeline, err := backend.IssueWorkflowTimeline(ctx, store.IssueIdentity{ProjectID: defaultWorkflowMetricsProjectID, IssueID: "issue-1162"})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	if len(timeline.Events) != 1 {
		t.Fatalf("workflow events = %#v, want one synthetic lane entry", timeline.Events)
	}
	if event := timeline.Events[0]; event.PhaseType != store.WorkflowPhaseTypeLane || event.PhaseName != "Blocked" || event.Status != "entered" || !event.StartedAt.Equal(enteredAt) {
		t.Fatalf("workflow event = %#v, want Blocked entered at %v", event, enteredAt)
	}
	metadata, ok := provenance.Parse(timeline.Events[0].MetadataJSON)
	if !ok || metadata.Provenance.Origin != provenance.OriginIndeterminate || metadata.Provenance.Initiator != provenance.InitiatorIndeterminate || metadata.Provenance.Actor != nil {
		t.Fatalf("workflow metadata = %#v, want indeterminate origin without actor", metadata)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("backend.Close() error = %v", err)
	}

	restarted, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("store.Open(restart) error = %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restartedState := newState(normalizeConfig(Config{}))
	restartedState.BoardIssues = []connector.Issue{{
		ID:         "issue-1162",
		Identifier: "digitaldrywood/detent#1162",
		State:      "Blocked",
		UpdatedAt:  &updatedAt,
	}}
	restartedOrch := &Orchestrator{cfg: normalizeConfig(Config{}), workflowMetrics: restarted}
	restartedOrch.refreshCurrentLaneEntries(ctx, &restartedState, updatedAt.Add(time.Minute))

	snapshot := restartedState.Snapshot(updatedAt.Add(time.Minute))
	if got := snapshot.BoardIssues[0].CurrentLaneEnteredAt; got == nil || !got.Equal(enteredAt) {
		t.Fatalf("CurrentLaneEnteredAt after restart = %v, want %v", got, enteredAt)
	}
}

func TestRefreshCurrentLaneEntriesUsesTrackerTransitionAcrossPollsAndRestart(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	enteredAt := time.Date(2026, 7, 10, 14, 36, 55, 0, time.UTC)
	firstPollAt := enteredAt.Add(2 * time.Hour)
	secondPollAt := firstPollAt.Add(2 * time.Minute)
	updatedAt := firstPollAt
	tracker := &workflowMetricsConnector{stateEnteredAt: enteredAt, stateEnteredAtFound: true}

	backend, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	state := newState(normalizeConfig(Config{}))
	state.BoardIssues = []connector.Issue{{
		ID:         "issue-1162",
		Identifier: "digitaldrywood/detent#1162",
		State:      "Blocked",
		UpdatedAt:  &updatedAt,
	}}
	orch := &Orchestrator{
		cfg:             normalizeConfig(Config{AdmissionTargetState: "Blocked"}),
		connector:       tracker,
		workflowMetrics: backend,
	}
	orch.refreshCurrentLaneEntries(ctx, &state, firstPollAt)
	first := state.Snapshot(firstPollAt)
	if got := first.BoardIssues[0].CurrentLaneEnteredAt; got == nil || !got.Equal(enteredAt) {
		t.Fatalf("first CurrentLaneEnteredAt = %v, want %v", got, enteredAt)
	}
	if got := first.BoardIssues[0].Origin; got != string(provenance.OriginIndeterminate) {
		t.Fatalf("first Origin = %q, want %q", got, provenance.OriginIndeterminate)
	}
	timeline, err := backend.IssueWorkflowTimeline(ctx, store.IssueIdentity{ProjectID: defaultWorkflowMetricsProjectID, IssueID: "issue-1162"})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	metadata, ok := provenance.Parse(timeline.Events[0].MetadataJSON)
	if !ok || metadata.Admission == nil || metadata.Admission.Attributed {
		t.Fatalf("observed transition metadata = %#v, want unattributed admission", metadata)
	}

	updatedAt = secondPollAt
	state.BoardIssues[0].UpdatedAt = &updatedAt
	orch.refreshCurrentLaneEntries(ctx, &state, secondPollAt)
	second := state.Snapshot(secondPollAt)
	if got := second.BoardIssues[0].CurrentLaneEnteredAt; got == nil || !got.Equal(enteredAt) {
		t.Fatalf("second CurrentLaneEnteredAt = %v, want %v", got, enteredAt)
	}
	if tracker.stateEnteredAtCalls != 1 {
		t.Fatalf("IssueStateEnteredAt() calls = %d, want 1 after durable healing", tracker.stateEnteredAtCalls)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("backend.Close() error = %v", err)
	}

	restarted, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("store.Open(restart) error = %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restartedState := newState(normalizeConfig(Config{}))
	restartedState.BoardIssues = cloneIssues(state.BoardIssues)
	restartedOrch := &Orchestrator{cfg: normalizeConfig(Config{}), connector: tracker, workflowMetrics: restarted}
	restartedOrch.refreshCurrentLaneEntries(ctx, &restartedState, secondPollAt.Add(time.Minute))
	restartedSnapshot := restartedState.Snapshot(secondPollAt.Add(time.Minute))
	if got := restartedSnapshot.BoardIssues[0].CurrentLaneEnteredAt; got == nil || !got.Equal(enteredAt) {
		t.Fatalf("restarted CurrentLaneEnteredAt = %v, want %v", got, enteredAt)
	}
	if tracker.stateEnteredAtCalls != 1 {
		t.Fatalf("IssueStateEnteredAt() calls after restart = %d, want persisted event to avoid another call", tracker.stateEnteredAtCalls)
	}
}

func TestRefreshCurrentLaneEntriesUsesHydratedTrackerReentry(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	firstEntry := time.Date(2026, 8, 17, 4, 53, 35, 0, time.UTC)
	latestEntry := time.Date(2026, 8, 18, 22, 30, 8, 0, time.UTC)
	backend, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	if _, err := backend.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:  "corp",
		IssueID:    "I_74",
		Identifier: "gopherguides/corp#74",
		PhaseType:  store.WorkflowPhaseTypeLane,
		PhaseName:  "Human Review",
		Status:     "entered",
		StartedAt:  firstEntry,
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "corp"}})
	state := newState(cfg)
	state.laneEntries["id:I_74\x00human review"] = firstEntry
	state.BoardIssues = []connector.Issue{{
		ID:                "I_74",
		Identifier:        "gopherguides/corp#74",
		State:             "Human Review",
		StageUpdatedAt:    &latestEntry,
		StageUpdatedActor: connector.IssueActor{Login: "corylanou", Kind: "User"},
	}}
	orch := &Orchestrator{cfg: cfg, workflowMetrics: backend}

	orch.refreshCurrentLaneEntries(ctx, &state, latestEntry.Add(time.Hour))
	snapshot := state.Snapshot(latestEntry.Add(time.Hour))
	if got := snapshot.BoardIssues[0].CurrentLaneEnteredAt; got == nil || !got.Equal(latestEntry) {
		t.Fatalf("CurrentLaneEnteredAt = %v, want %v", got, latestEntry)
	}
	timeline, err := backend.IssueWorkflowTimeline(ctx, store.IssueIdentity{ProjectID: "corp", IssueID: "I_74"})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	latest, ok := latestCurrentLaneEntryForAt(timeline.Events, "Human Review", latestEntry)
	if !ok {
		t.Fatalf("latest lane entry missing from timeline: %#v", timeline.Events)
	}
	metadata, ok := provenance.Parse(latest.MetadataJSON)
	if !ok || metadata.Provenance.Actor == nil || metadata.Provenance.Actor.Login != "corylanou" {
		t.Fatalf("latest lane provenance = %#v, want hydrated tracker actor", metadata.Provenance)
	}
}

func TestRefreshCurrentLaneEntriesSurvivesStoreRestart(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	enteredAt := time.Date(2026, 7, 9, 17, 0, 0, 0, time.UTC)

	seed, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("store.Open(seed) error = %v", err)
	}
	if _, err := seed.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID: "detent",
		IssueID:   "issue-1130",
		PhaseType: store.WorkflowPhaseTypeLane,
		PhaseName: "In Progress",
		Status:    "entered",
		StartedAt: enteredAt,
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed.Close() error = %v", err)
	}

	for index, updatedAt := range []time.Time{enteredAt.Add(30 * time.Minute), enteredAt.Add(time.Hour)} {
		backend, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: dbPath})
		if err != nil {
			t.Fatalf("store.Open(restart %d) error = %v", index+1, err)
		}

		cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}})
		state := newState(cfg)
		state.BoardIssues = []connector.Issue{{
			ID:        "issue-1130",
			State:     "In Progress",
			UpdatedAt: &updatedAt,
		}}
		orch := &Orchestrator{cfg: cfg, workflowMetrics: backend}
		orch.refreshCurrentLaneEntries(ctx, &state, updatedAt)
		snapshot := state.Snapshot(updatedAt.Add(time.Minute))
		if snapshot.BoardIssues[0].CurrentLaneEnteredAt == nil || !snapshot.BoardIssues[0].CurrentLaneEnteredAt.Equal(enteredAt) {
			t.Errorf("restart %d CurrentLaneEnteredAt = %v, want %v", index+1, snapshot.BoardIssues[0].CurrentLaneEnteredAt, enteredAt)
		}
		if err := backend.Close(); err != nil {
			t.Fatalf("backend.Close(restart %d) error = %v", index+1, err)
		}
	}
}

func TestUpdateIssueStateByIDSkipsWorkflowMetricsForBlockedUpdate(t *testing.T) {
	t.Parallel()

	recorder := &workflowMetricsRecorderSpy{}
	orch := &Orchestrator{
		connector: &workflowMetricsConnector{
			err: &connector.StateUpdateBlockedError{
				IssueID:      "issue-blocked",
				CurrentState: "Done",
				TargetState:  "Todo",
			},
		},
		workflowMetrics: recorder,
	}
	state := newState(Config{})
	state.BoardIssues = []connector.Issue{{ID: "issue-blocked", State: "Done"}}

	err := orch.updateIssueStateByID(
		context.Background(),
		&state,
		"issue-blocked",
		connector.Issue{
			ID:         "issue-blocked",
			Identifier: "digitaldrywood/detent#100",
			State:      "Done",
		},
		"Todo",
		time.Now(),
		"test",
	)
	if err != nil {
		t.Fatalf("updateIssueStateByID() error = %v, want nil", err)
	}
	if len(recorder.events) != 0 {
		t.Fatalf("workflow metric events = %#v, want none", recorder.events)
	}
	if got := state.BoardIssues[0].State; got != "Done" {
		t.Fatalf("snapshot BoardIssues state = %q, want Done", got)
	}
}

func TestUpdateIssueStateRecordsTrackerMutationConfirmation(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 8, 25, 20, 13, 0, 0, time.UTC)
	trackerStageAt := requestedAt.Add(2 * time.Second)
	responseAt := requestedAt.Add(3 * time.Second)
	issue := connector.Issue{
		ID:         "issue-mutation-confirmation",
		Identifier: "digitaldrywood/detent#1987",
		State:      "Human Review",
	}
	recorder := &autoPromoteWorkflowMetricsRecorder{}
	tracker := &workflowMetricsConnector{
		stateEnteredAt:      trackerStageAt,
		stateEnteredAtFound: true,
	}
	orch := &Orchestrator{
		cfg:             normalizeConfig(Config{}),
		connector:       tracker,
		workflowMetrics: recorder,
		now:             func() time.Time { return responseAt },
	}
	state := newState(orch.cfg)
	state.BoardIssues = []connector.Issue{cloneIssue(issue)}
	metadata := workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Cause: "rework_limit"}}

	if err := orch.updateIssueStateByIDWithMetadata(
		t.Context(),
		&state,
		issue.ID,
		issue,
		blockedStatusState,
		requestedAt,
		"rework_limit",
		metadata,
	); err != nil {
		t.Fatalf("updateIssueStateByIDWithMetadata() error = %v", err)
	}

	events := recorder.snapshot()
	if len(events) != 2 {
		t.Fatalf("workflow events = %#v, want exit and entry", events)
	}
	entered := events[1]
	gotMetadata, ok := workflowLaneMetadataFromJSON(entered.MetadataJSON)
	if !ok || gotMetadata.TrackerMutationAt != trackerStageAt.Format(time.RFC3339Nano) {
		t.Fatalf("tracker mutation confirmation = %q, want tracker time %q instead of response time %q", gotMetadata.TrackerMutationAt, trackerStageAt.Format(time.RFC3339Nano), responseAt.Format(time.RFC3339Nano))
	}

	current := cloneIssue(issue)
	current.State = blockedStatusState
	current.StageUpdatedAt = &trackerStageAt
	state.BoardIssues = []connector.Issue{current}
	orch.refreshCurrentLaneEntries(t.Context(), &state, requestedAt.Add(time.Minute))
	if got := len(recorder.snapshot()); got != len(events) {
		t.Fatalf("workflow event count after tracker reconciliation = %d, want %d", got, len(events))
	}
}

func TestDetentLaneWriteEchoKeepsWriter(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name            string
		target          string
		labelTransition bool
	}{
		{"label blocked", "Blocked", true},
		{"label rework", "Rework", true},
		{"label merging", "Merging", true},
		{"project blocked", "Blocked", false},
		{"project rework", "Rework", false},
		{"project merging", "Merging", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.target
			t.Parallel()
			requestedAt := time.Date(2026, 9, 4, 4, 11, 0, 0, time.UTC)
			trackerAt := requestedAt.Add(2 * time.Second)
			issue := laneRevocationIssue("echo", "digitaldrywood/detent#2138", "In Progress")
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			confirmed := cloneIssue(issue)
			confirmed.State = target
			confirmed.StageUpdatedAt = &trackerAt
			tracker := &workflowMetricsConnector{stateEnteredAt: trackerAt, stateEnteredAtFound: tt.labelTransition, stateIssues: []connector.Issue{confirmed}}
			orch := &Orchestrator{cfg: normalizeConfig(Config{}), connector: tracker, workflowMetrics: metrics}
			state := newState(orch.cfg)
			state.BoardIssues = []connector.Issue{issue}
			if err := orch.updateIssueState(t.Context(), &state, issue, target, requestedAt, "machine_transition"); err != nil {
				t.Fatal(err)
			}
			issue.State = target
			issue.StageUpdatedAt = &trackerAt
			issue.StageUpdatedActor = connector.IssueActor{Login: "shared-token", Kind: "User"}
			state.BoardIssues = []connector.Issue{issue}
			orch.refreshCurrentLaneEntries(t.Context(), &state, requestedAt.Add(time.Minute))
			if got := len(metrics.snapshot()); got != 2 {
				t.Fatalf("events after write echo = %d, want only the original exit and entry", got)
			}
			if got := laneRevocationAttribution(&state, issue); got.Origin != provenance.OriginDetent || got.Basis != provenance.BasisDetentOperation {
				t.Fatalf("write echo attribution = %#v, want Detent", got)
			}
			later := trackerAt.Add(time.Minute)
			issue.StageUpdatedAt = &later
			state.BoardIssues = []connector.Issue{issue}
			orch.refreshCurrentLaneEntries(t.Context(), &state, later)
			if got := laneRevocationAttribution(&state, issue); got.Origin != provenance.OriginIndeterminate {
				t.Fatalf("later shared-token reentry attribution = %#v, want indeterminate", got)
			}
		})
	}
}

func TestUpdateIssueStateByIDCapturesReworkLesson(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		reason          string
		pullRequest     *connector.PullRequest
		wantFailureKind string
		wantContext     []string
	}{
		{
			name:   "CI failure includes failed checks",
			reason: string(AutoPromoteReasonCINotGreen),
			pullRequest: &connector.PullRequest{
				Number: 1401,
				URL:    "https://github.com/digitaldrywood/detent/pull/1401",
				RequiredCheckFailures: []connector.PullRequestCheck{
					{Name: "test", Conclusion: "failure"},
					{Name: "lint", Conclusion: "failure"},
				},
			},
			wantFailureKind: "ci_failure",
			wantContext:     []string{"failed checks: test, lint", "https://github.com/digitaldrywood/detent/pull/1401"},
		},
		{
			name:   "requested changes include review findings",
			reason: string(AutoPromoteReasonP1Findings),
			pullRequest: &connector.PullRequest{
				Number:           1402,
				CodexReviewState: "CHANGES_REQUESTED",
				CodexReviewFindings: []connector.PullRequestFinding{
					{Body: "Add rollback coverage."},
				},
			},
			wantFailureKind: "changes_requested",
			wantContext:     []string{"CHANGES_REQUESTED: Add rollback coverage.", "PR #1402"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), ".detent", "lessons.md")
			transitionAt := time.Date(2026, 7, 17, 15, 30, 0, 0, time.UTC)
			issue := connector.Issue{
				ID:          "issue-1397",
				Identifier:  "digitaldrywood/detent#1397",
				Number:      1397,
				Title:       "Capture rework lessons",
				State:       "In Progress",
				PullRequest: tt.pullRequest,
			}
			orch := &Orchestrator{
				cfg: Config{
					Project: scheduler.ProjectCandidate{ID: "detent"},
					Lessons: LessonCaptureConfig{Enabled: true, Path: path, MaxEntries: 10},
				},
				connector: &workflowMetricsConnector{},
			}
			state := newState(Config{})
			if err := orch.updateIssueStateByID(t.Context(), &state, issue.ID, issue, "Rework", transitionAt, tt.reason); err != nil {
				t.Fatalf("updateIssueStateByID() error = %v", err)
			}

			patterns, err := lessons.FailureKindPatterns(path, 1)
			if err != nil {
				t.Fatalf("FailureKindPatterns() error = %v", err)
			}
			if len(patterns) != 1 || patterns[0].FailureKind != tt.wantFailureKind {
				t.Fatalf("FailureKindPatterns() = %#v, want %q", patterns, tt.wantFailureKind)
			}
			entries, err := lessons.ReadAll(path)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("ReadAll() len = %d, want 1", len(entries))
			}
			for _, want := range tt.wantContext {
				if !strings.Contains(entries[0], want) {
					t.Errorf("lesson missing %q:\n%s", want, entries[0])
				}
			}
		})
	}
}

func TestRefreshCurrentLaneEntriesCapturesObservedReworkOnce(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), ".detent", "lessons.md")
	enteredAt := time.Date(2026, 7, 17, 16, 0, 0, 0, time.UTC)
	issue := connector.Issue{
		ID:             "issue-1397",
		Identifier:     "digitaldrywood/detent#1397",
		Number:         1397,
		Title:          "Capture observed rework",
		PullRequest:    &connector.PullRequest{UnresolvedReviewThreads: []connector.PullRequestReviewThread{{Path: "worker.go", Body: "Preserve command evidence."}}},
		State:          "Rework",
		StageUpdatedAt: &enteredAt,
	}
	cfg := Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		Lessons: LessonCaptureConfig{Enabled: true, Path: path, MaxEntries: 10},
	}
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	state.BoardIssues = []connector.Issue{issue}

	orch.refreshCurrentLaneEntries(t.Context(), &state, enteredAt.Add(time.Minute))
	orch.refreshCurrentLaneEntries(t.Context(), &state, enteredAt.Add(2*time.Minute))

	entries, err := lessons.ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ReadAll() len = %d, want one deduplicated capture", len(entries))
	}
	patterns, err := lessons.FailureKindPatterns(path, 1)
	if err != nil {
		t.Fatalf("FailureKindPatterns() error = %v", err)
	}
	if len(patterns) != 1 || patterns[0].FailureKind != reworkTransitionFailureKind {
		t.Fatalf("FailureKindPatterns() = %#v, want %q", patterns, reworkTransitionFailureKind)
	}
}

func TestRecordObservedBlockedEntryClassifiesUnrecordedCause(t *testing.T) {
	t.Parallel()

	enteredAt := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		issue       connector.Issue
		attribution provenance.Attribution
		wantStatus  string
	}{
		{
			name:        "indeterminate observation without cause",
			issue:       connector.Issue{ID: "issue-1", State: "Blocked"},
			attribution: provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{}),
			wantStatus:  blockedCauseStatusUnrecorded,
		},
		{
			name:        "tracker cause is recorded",
			issue:       connector.Issue{ID: "issue-2", State: "Blocked", BlockerReason: "waiting for operator approval"},
			attribution: provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{}),
		},
		{
			name:        "agent provenance is already attributable",
			issue:       connector.Issue{ID: "issue-3", State: "Blocked"},
			attribution: provenance.AttributionFromSource(provenance.SourceDetentAgentSession, provenance.Actor{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			recorder := &workflowMetricsRecorderSpy{}
			orch := &Orchestrator{cfg: normalizeConfig(Config{}), workflowMetrics: recorder}
			orch.recordObservedLaneEntry(t.Context(), tt.issue, enteredAt, tt.attribution)
			if len(recorder.events) != 1 {
				t.Fatalf("events = %#v, want one", recorder.events)
			}
			metadata, ok := workflowLaneMetadataFromJSON(recorder.events[0].MetadataJSON)
			if !ok {
				t.Fatalf("metadata = %q, want valid lane metadata", recorder.events[0].MetadataJSON)
			}
			if metadata.BlockedCauseStatus != tt.wantStatus {
				t.Fatalf("BlockedCauseStatus = %q, want %q", metadata.BlockedCauseStatus, tt.wantStatus)
			}
		})
	}
}

type workflowMetricsRecorderSpy struct {
	events []store.WorkflowPhaseEvent
}

func (r *workflowMetricsRecorderSpy) RecordWorkflowPhaseEvent(_ context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	r.events = append(r.events, event)
	return int64(len(r.events)), nil
}

type workflowMetricsConnector struct {
	name                string
	err                 error
	fetches             int
	candidates          []connector.Issue
	stateIssues         []connector.Issue
	stateEnteredAt      time.Time
	stateEnteredAtFound bool
	stateEnteredAtErr   error
	stateEnteredAtCalls int
}

func (c *workflowMetricsConnector) Name() string {
	return c.name
}

func (c *workflowMetricsConnector) IssueStateTransition(context.Context, connector.Issue) (connector.IssueStateTransition, bool, error) {
	c.stateEnteredAtCalls++
	return connector.IssueStateTransition{
		EnteredAt: c.stateEnteredAt,
		Actor: connector.IssueActor{
			Login: "ada",
			Kind:  "User",
		},
	}, c.stateEnteredAtFound, c.stateEnteredAtErr
}

func (c *workflowMetricsConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	c.fetches++
	return cloneIssues(c.candidates), nil
}

func (c *workflowMetricsConnector) FetchIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	c.fetches++
	return issuesInStates(c.stateIssues, states), nil
}

func (c *workflowMetricsConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	c.fetches++
	return cloneIssues(c.stateIssues), nil
}

func (c *workflowMetricsConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c *workflowMetricsConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	if c.err != nil {
		return c.err
	}
	for index := range c.candidates {
		if c.candidates[index].ID == issueID {
			c.candidates[index].State = state
		}
	}
	for index := range c.stateIssues {
		if c.stateIssues[index].ID == issueID {
			c.stateIssues[index].State = state
		}
	}
	return nil
}

func (c *workflowMetricsConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *workflowMetricsConnector) SetField(context.Context, string, string, string) error {
	return nil
}
