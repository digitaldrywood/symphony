package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/provenance"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestLaneRevocationPreservesTrackerDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		destination   string
		wantCompleted bool
	}{
		{name: "blocked lane", destination: "Blocked"},
		{name: "review lane", destination: "Review"},
		{name: "terminal lane", destination: "Done", wantCompleted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 8, 16, 18, 35, 0, 0, time.UTC)
			issue := laneRevocationIssue("issue-22", "digitaldrywood/video-studio#22", "Production")
			parked := cloneIssue(issue)
			parked.State = tt.destination
			attempts := &recordingWorkAttemptStore{}
			tracker := &runningStateConnector{issues: []connector.Issue{parked}}
			cfg := normalizeConfig(Config{
				Project:        scheduler.ProjectCandidate{ID: "video-studio"},
				ActiveStates:   []string{"Todo", "Production", "Rework"},
				ObservedStates: []string{"Blocked", "Review"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{
				cfg:                    cfg,
				connector:              tracker,
				workAttempts:           attempts,
				pendingLaneRevocations: map[string]*pendingLaneRevocation{},
				logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
				now:                    func() time.Time { return now },
			}
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(context.Background())
			state.Running[issue.ID] = Running{
				Issue:         issue,
				Attempt:       2,
				WorkAttemptID: 42,
				Generation:    7,
				StartedAt:     now.Add(-time.Hour),
				stop:          stop,
			}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Hour)}
			state.Retry[issue.ID] = Retry{Issue: issue, Attempt: 3, DueAt: now.Add(time.Hour)}

			orch.reconcileRunningIssues(t.Context(), &state, now)

			if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
				t.Fatalf("context cause = %v, want ErrLaneRevoked", context.Cause(runCtx))
			}
			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID: issue.ID,
				Request: runpkg.RunRequest{
					Issue:         issue,
					WorkAttemptID: 42,
					Generation:    7,
				},
				CompletedAt: now.Add(time.Second),
				Err:         runpkg.ErrLaneRevoked,
				Result:      runpkg.RunResult{FinalState: runpkg.FinalStateLaneRevoked},
			})

			if _, ok := state.Running[issue.ID]; ok {
				t.Fatalf("Running[%q] present after lane revocation", issue.ID)
			}
			if _, ok := state.Claimed[issue.ID]; ok {
				t.Fatalf("Claimed[%q] present after lane revocation", issue.ID)
			}
			if _, ok := state.Retry[issue.ID]; ok {
				t.Fatalf("Retry[%q] present after lane revocation", issue.ID)
			}
			_, completed := state.Completed[issue.ID]
			if completed != tt.wantCompleted {
				t.Fatalf("Completed[%q] present = %v, want %v", issue.ID, completed, tt.wantCompleted)
			}
			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalLaneRevoked {
				t.Fatalf("work attempt completions = %#v, want lane_revoked", attempts.completions)
			}
			if len(tracker.updates) != 0 || len(tracker.setFieldCalls) != 0 {
				t.Fatalf("tracker writes = updates %#v fields %#v, want none", tracker.updates, tracker.setFieldCalls)
			}
			if tracker.issues[0].State != tt.destination {
				t.Fatalf("tracker state = %q, want preserved %q", tracker.issues[0].State, tt.destination)
			}
		})
	}
}

func TestLaneRevocationDoesNotInferWorkLoss(t *testing.T) {
	t.Parallel()

	for _, origin := range []provenance.Origin{provenance.OriginHuman, provenance.OriginDetent, provenance.OriginAgent, provenance.OriginIndeterminate} {
		t.Run(string(origin), func(t *testing.T) {
			t.Parallel()
			outcome := classifyLaneRevocation(nil, nil, true, laneRevocationStateChanged, origin)
			if outcome.workDiscarded {
				t.Fatal("revocation declares unpushed output discarded without inspecting or removing the workspace")
			}
		})
	}
}

func TestLaneRevocationPreservesPushedWork(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 30, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-delivered", "digitaldrywood/detent#1998", "In Progress")
	issue.PullRequest = &connector.PullRequest{Number: 2001, HeadSHA: "abc123"}
	parked := cloneIssue(issue)
	parked.State = "Human Review"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
	attempts := &recordingWorkAttemptStore{}
	cfg := normalizeConfig(Config{
		Project:        scheduler.ProjectCandidate{ID: "detent"},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates: []string{"Human Review"},
	})
	orch := &Orchestrator{
		cfg:                    cfg,
		connector:              tracker,
		reaper:                 &laneRetentionProbe{t: t, result: workspace.Preservation{Preserved: true, LocalChangesVerified: true, Delivery: &workspace.DeliverableState{CommitsAhead: 1, Remote: "origin", RemoteRef: "refs/heads/detent/1998", LocalHeadSHA: "abc123", RemoteHeadSHA: "abc123", RemoteBranchExists: true}}},
		workAttempts:           attempts,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:                    func() time.Time { return now },
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:         issue,
		Attempt:       4,
		WorkAttemptID: 1998,
		Generation:    3,
		StartedAt:     now.Add(-time.Hour),
		Tokens:        runpkg.TokenTotals{OutputTokens: 2_000, TotalTokens: 8_000, RuntimeSeconds: 600},
	}

	orch.reconcileRunningIssues(t.Context(), &state, now)
	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Issue: issue, WorkAttemptID: 1998, Generation: 3},
		Result: runpkg.RunResult{
			FinalState:            runpkg.FinalStateLaneRevoked,
			Output:                "pushed PR head",
			TurnStarted:           true,
			PullRequestHeadPushed: true,
		},
		CompletedAt: now.Add(time.Second),
		Err:         runpkg.ErrLaneRevoked,
	})

	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions = %#v, want one", attempts.completions)
	}
	completion := attempts.completions[0]
	if completion.TerminalState != store.WorkAttemptTerminalDelivered {
		t.Fatalf("terminal state = %q, want delivered", completion.TerminalState)
	}
	if completion.SessionFinalState != "delivered" {
		t.Fatalf("session final state = %q, want delivered", completion.SessionFinalState)
	}
	if !strings.Contains(completion.WorkerMetadataJSON, `"delivery_receipt":{"schema":1,"kind":"pushed_work_product"`) {
		t.Fatalf("worker metadata = %s, want durable pushed-work receipt", completion.WorkerMetadataJSON)
	}
	if strings.Contains(completion.WorkerMetadataJSON, `"work_discarded":true`) {
		t.Fatalf("worker metadata = %s, must not mark pushed work discarded", completion.WorkerMetadataJSON)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "work was pushed but finalization was rejected") {
		t.Fatalf("comments = %#v, want pushed-work finalization notice", tracker.comments)
	}
	if hasLaneRevocationEvent(state.RecentEvents, "worker_lane_output_discarded") {
		t.Fatalf("RecentEvents = %#v, must not report discarded output", state.RecentEvents)
	}
	if !hasLaneRevocationEvent(state.RecentEvents, "worker_lane_delivery_preserved") {
		t.Fatalf("RecentEvents = %#v, want preserved delivery event", state.RecentEvents)
	}
}

func TestLaneRevocationAccountsTokensAfterDurableCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 20, 45, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-completion-retry", "digitaldrywood/detent#1998", "In Progress")
	parked := cloneIssue(issue)
	parked.State = "Human Review"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
	attempts := &recordingWorkAttemptStore{completionErrors: []error{errors.New("store unavailable")}}
	cfg := normalizeConfig(Config{
		Project:        scheduler.ProjectCandidate{ID: "detent"},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates: []string{"Human Review"},
	})
	orch := &Orchestrator{
		cfg:                    cfg,
		connector:              tracker,
		workAttempts:           attempts,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:                    func() time.Time { return now },
	}
	state := newState(cfg)
	state.TokenTotals = TokenTotals{InputTokens: 3, OutputTokens: 2, TotalTokens: 5, RuntimeSeconds: 1}
	state.Running[issue.ID] = Running{
		Issue:         issue,
		WorkAttemptID: 1998,
		Generation:    4,
		StartedAt:     now.Add(-time.Minute),
		Tokens:        TokenTotals{InputTokens: 30, OutputTokens: 20, TotalTokens: 50, RuntimeSeconds: 10},
	}

	orch.reconcileRunningIssues(t.Context(), &state, now)
	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		Request:     runpkg.RunRequest{Issue: issue, WorkAttemptID: 1998, Generation: 4},
		CompletedAt: now.Add(time.Second),
		Err:         runpkg.ErrLaneRevoked,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateLaneRevoked},
	})

	if got := state.TokenTotals.TotalTokens; got != 5 {
		t.Fatalf("token total after failed completion = %d, want 5", got)
	}
	pending := orch.pendingLaneRevocations[issue.ID]
	if pending == nil {
		t.Fatal("pending lane revocation missing after failed completion")
	}
	orch.finishLaneRevocation(t.Context(), &state, pending)

	if got := state.TokenTotals.TotalTokens; got != 55 {
		t.Fatalf("token total after completion retry = %d, want 55", got)
	}
	if len(attempts.completions) != 2 {
		t.Fatalf("work attempt completion calls = %d, want 2", len(attempts.completions))
	}
	if _, ok := orch.pendingLaneRevocations[issue.ID]; ok {
		t.Fatal("pending lane revocation retained after successful retry")
	}
}

func TestClassifyLaneRevocationDrivesAllOutcomeSurfaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		receipt       *laneRevocationDeliveryReceipt
		workProduced  bool
		wantClass     string
		wantTerminal  store.WorkAttemptTerminalState
		wantSession   string
		wantEvent     string
		wantComment   bool
		wantDiscarded bool
	}{
		{
			name:         "pushed work with rejected finalization",
			receipt:      &laneRevocationDeliveryReceipt{Schema: 1, Kind: laneRevocationDeliveryReceiptKind, Remote: "origin", RemoteRef: "refs/heads/detent/1998", RemoteHeadSHA: "abc123"},
			workProduced: true,
			wantClass:    laneRevocationDeliveredClassification,
			wantTerminal: store.WorkAttemptTerminalDelivered,
			wantSession:  "delivered",
			wantEvent:    "worker_lane_delivery_preserved",
			wantComment:  true,
		},
		{
			name:          "genuinely unpushed work",
			workProduced:  true,
			wantClass:     laneRevocationUnverifiedClassification,
			wantTerminal:  store.WorkAttemptTerminalLaneRevoked,
			wantSession:   runpkg.FinalStateLaneRevoked,
			wantEvent:     "worker_lane_preservation_unverified",
			wantComment:   true,
			wantDiscarded: false,
		},
		{
			name:         "revoked before work",
			wantClass:    laneRevocationEmptyClassification,
			wantTerminal: store.WorkAttemptTerminalLaneRevoked,
			wantSession:  runpkg.FinalStateLaneRevoked,
			wantEvent:    "worker_lane_revoked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := classifyLaneRevocation(tt.receipt, nil, tt.workProduced, laneRevocationStateChanged, provenance.OriginHuman)
			if got.classification != tt.wantClass || got.terminalState != tt.wantTerminal || got.sessionFinalState != tt.wantSession {
				t.Fatalf("classification = %#v, want class %q terminal %q session %q", got, tt.wantClass, tt.wantTerminal, tt.wantSession)
			}
			if got.activityEvent != tt.wantEvent || got.comment != tt.wantComment || got.workDiscarded != tt.wantDiscarded {
				t.Fatalf("rendering outcome = %#v, want event %q comment %v discarded %v", got, tt.wantEvent, tt.wantComment, tt.wantDiscarded)
			}
			if got.terminalState == store.WorkAttemptTerminalDelivered {
				if got.errorClass != "" || got.errorMessage != "" || strings.Contains(got.statusMessage, "discard") || strings.Contains(got.activityEvent, "discard") {
					t.Fatalf("delivered outcome contains revoked/discarded surface: %#v", got)
				}
			}
		})
	}
}

func TestCompletionLaneHandshakeClassifiesCurrentAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 21, 10, 53, 0, time.UTC)
	tests := []struct {
		name                string
		handshakeAttempt    int64
		handshakeGeneration uint64
		reconcileFirst      bool
		wantTerminal        store.WorkAttemptTerminalState
		wantRevoked         bool
	}{
		{
			name:                "agent lane write before delivery completion",
			handshakeAttempt:    3295,
			handshakeGeneration: 7,
			reconcileFirst:      true,
			wantTerminal:        store.WorkAttemptTerminalSuccess,
		},
		{
			name:                "delivery completion observes agent lane write",
			handshakeAttempt:    3295,
			handshakeGeneration: 7,
			wantTerminal:        store.WorkAttemptTerminalSuccess,
		},
		{
			name:           "operator move to completion lane",
			reconcileFirst: true,
			wantTerminal:   store.WorkAttemptTerminalLaneRevoked,
			wantRevoked:    true,
		},
		{
			name:                "agent that no longer holds attempt",
			handshakeAttempt:    3238,
			handshakeGeneration: 6,
			reconcileFirst:      true,
			wantTerminal:        store.WorkAttemptTerminalLaneRevoked,
			wantRevoked:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := laneRevocationIssue("issue-completion-handshake", "gopherguides/corp#72", "Rework")
			issue.PullRequest = &connector.PullRequest{Number: 189, State: "OPEN", HeadSHA: "d130765c"}
			parked := cloneIssue(issue)
			parked.State = "Human Review"
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
			if tt.handshakeAttempt > 0 {
				recordedAt := now.Add(-time.Minute)
				tracker.issueComments = map[string][]connector.IssueComment{
					issue.ID: {{
						Body:      completionHandshakeWorkpadBody(tt.handshakeAttempt, tt.handshakeGeneration),
						URL:       "https://github.test/workpad",
						UpdatedAt: &recordedAt,
					}},
				}
			}
			attempts := &recordingWorkAttemptStore{}
			cfg := normalizeConfig(Config{
				Project:        scheduler.ProjectCandidate{ID: "corp"},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates: []string{"Human Review"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{
				cfg:                    cfg,
				connector:              tracker,
				workAttempts:           attempts,
				pendingLaneRevocations: map[string]*pendingLaneRevocation{},
				logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
				now:                    func() time.Time { return now },
			}
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(context.Background())
			state.Running[issue.ID] = Running{
				Issue:         issue,
				Attempt:       3,
				WorkAttemptID: 3295,
				Generation:    7,
				StartedAt:     now.Add(-20 * time.Minute),
				stop:          stop,
			}
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-20 * time.Minute)}

			if tt.reconcileFirst {
				orch.reconcileRunningIssues(t.Context(), &state, now)
			}
			if got := errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked); got != tt.wantRevoked {
				t.Fatalf("worker revoked = %v, want %v; cause = %v", got, tt.wantRevoked, context.Cause(runCtx))
			}

			completion := runpkg.Completion{
				IssueID:     issue.ID,
				Request:     runpkg.RunRequest{Issue: issue, WorkAttemptID: 3295, Generation: 7},
				CompletedAt: now.Add(time.Second),
				Result: runpkg.RunResult{
					FinalState:            runpkg.FinalStateCompleted,
					PullRequestUpdated:    !tt.wantRevoked,
					PullRequestHeadPushed: !tt.wantRevoked,
					TurnStarted:           true,
					DiffStats:             runpkg.DiffStats{Status: "clean", RecoveryStateExpected: true, RecoveryStateAvailable: true},
				},
			}
			if tt.wantRevoked {
				completion.Err = runpkg.ErrLaneRevoked
				completion.Result.FinalState = runpkg.FinalStateLaneRevoked
			}
			orch.handleRunResult(t.Context(), &state, completion)

			if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != tt.wantTerminal {
				t.Fatalf("work attempt completions = %#v, want %q", attempts.completions, tt.wantTerminal)
			}
			if tt.wantTerminal == store.WorkAttemptTerminalSuccess {
				if attempts.completions[0].ErrorClass != "" || attempts.completions[0].ErrorMessage != "" {
					t.Fatalf("successful completion error = %q/%q", attempts.completions[0].ErrorClass, attempts.completions[0].ErrorMessage)
				}
				if _, ok := state.Claimed[issue.ID]; ok {
					t.Fatalf("Claimed[%q] present after accepted completion", issue.ID)
				}
				if got := state.Completed[issue.ID].Issue.State; got != "Human Review" {
					t.Fatalf("completed issue state = %q, want Human Review", got)
				}
				attribution := state.laneProvenance[workflowLaneEntryKey(parked)]
				if provenance.NormalizeOrigin(attribution.Origin) != provenance.OriginAgent {
					t.Fatalf("lane provenance = %#v, want agent", attribution)
				}
			} else if _, ok := orch.pendingLaneRevocations[issue.ID]; ok {
				t.Fatalf("pending lane revocation retained after completion")
			}
		})
	}
}

func TestLaneRevocationRecordsOriginAndUnverifiedWork(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 19, 20, 0, 0, time.UTC)
	tests := []struct {
		name            string
		origin          provenance.Attribution
		originCached    bool
		transitionActor connector.IssueActor
		workerActor     connector.IssueActor
		detentAuthored  bool
		wantReason      string
		wantErrorClass  string
		wantOrigin      provenance.Origin
	}{
		{
			name:            "active user move before provenance refresh",
			transitionActor: connector.IssueActor{Login: "operator-token", Kind: "User"},
			wantReason:      laneRevocationStateChanged,
			wantErrorClass:  string(store.WorkAttemptTerminalLaneRevoked),
			wantOrigin:      provenance.OriginIndeterminate,
		},
		{
			name:            "active app worker move before provenance refresh",
			transitionActor: connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"},
			workerActor:     connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"},
			wantReason:      laneRevocationStateChanged,
			wantErrorClass:  string(store.WorkAttemptTerminalLaneRevoked),
			wantOrigin:      provenance.OriginAgent,
		},
		{
			name:            "external automation actor overrides active agent fallback",
			transitionActor: connector.IssueActor{Login: "external-bot", Kind: "Bot"},
			workerActor:     connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"},
			wantReason:      laneRevocationStateChanged,
			wantErrorClass:  string(store.WorkAttemptTerminalLaneRevoked),
			wantOrigin:      provenance.OriginAutomation,
		},
		{
			name:           "operator move keeps tracker revocation",
			origin:         provenance.AttributionFromSource(provenance.SourceHumanSession, provenance.Actor{Login: "operator", Kind: "User"}),
			originCached:   true,
			wantReason:     laneRevocationStateChanged,
			wantErrorClass: string(store.WorkAttemptTerminalLaneRevoked),
			wantOrigin:     provenance.OriginHuman,
		},
		{
			name:           "Detent move has distinct revocation",
			detentAuthored: true,
			wantReason:     "detent_tracker_lane_changed",
			wantErrorClass: laneRevocationDetentErrorClass,
			wantOrigin:     provenance.OriginDetent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := laneRevocationIssue("issue-origin", "digitaldrywood/detent#1903", "Rework")
			parked := cloneIssue(issue)
			parked.State = "Human Review"
			parked.StageUpdatedActor = tt.transitionActor
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
			attempts := &recordingWorkAttemptStore{}
			cfg := normalizeConfig(Config{
				Project:        scheduler.ProjectCandidate{ID: "detent"},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates: []string{"Human Review"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{
				cfg:                    cfg,
				connector:              tracker,
				workAttempts:           attempts,
				pendingLaneRevocations: map[string]*pendingLaneRevocation{},
				logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
				now:                    func() time.Time { return now },
			}
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(context.Background())
			state.Running[issue.ID] = Running{
				Issue:             issue,
				Attempt:           3,
				WorkAttemptID:     1903,
				Generation:        9,
				StartedAt:         now.Add(-397 * time.Second),
				WorkerGitHubActor: tt.workerActor,
				Tokens: runpkg.TokenTotals{
					OutputTokens:   13_422,
					TotalTokens:    42_000,
					RuntimeSeconds: 397,
				},
				stop: stop,
			}
			if tt.detentAuthored {
				recordIssueStateMutationProvenance(&state, issue.ID, issue, parked.State, now.Add(-time.Minute), "completed_active_review_transition", workflowLaneMetadata{})
			} else if tt.originCached {
				state.laneProvenance[workflowLaneEntryKey(parked)] = tt.origin
			}

			orch.refreshActiveRuns(t.Context(), &state, now, githubBudgetReserveDecision{degraded: true})

			if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
				t.Fatalf("context cause = %v, want ErrLaneRevoked", context.Cause(runCtx))
			}
			pending := orch.pendingLaneRevocations[issue.ID]
			if pending == nil || pending.reason != tt.wantReason {
				t.Fatalf("pending revocation = %#v, want reason %q", pending, tt.wantReason)
			}
			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID: issue.ID,
				Request: runpkg.RunRequest{Issue: issue, WorkAttemptID: 1903, Generation: 9},
				Result: runpkg.RunResult{
					FinalState:  runpkg.FinalStateLaneRevoked,
					Output:      "completed rework summary",
					TurnStarted: true,
				},
				CompletedAt: now.Add(time.Second),
				Err:         runpkg.ErrLaneRevoked,
			})

			if len(attempts.completions) != 1 {
				t.Fatalf("work attempt completions = %#v, want one", attempts.completions)
			}
			completion := attempts.completions[0]
			if completion.ErrorClass != tt.wantErrorClass {
				t.Fatalf("error class = %q, want %q", completion.ErrorClass, tt.wantErrorClass)
			}
			if completion.ErrorMessage != tt.wantReason {
				t.Fatalf("error message = %q, want %q", completion.ErrorMessage, tt.wantReason)
			}
			var metadata struct {
				LaneRevocation struct {
					Origin        provenance.Origin `json:"origin"`
					WorkDiscarded bool              `json:"work_discarded"`
					OutputTokens  int64             `json:"output_tokens"`
				} `json:"lane_revocation"`
			}
			if err := json.Unmarshal([]byte(completion.WorkerMetadataJSON), &metadata); err != nil {
				t.Fatalf("decode worker metadata: %v", err)
			}
			if metadata.LaneRevocation.Origin != tt.wantOrigin || metadata.LaneRevocation.WorkDiscarded || metadata.LaneRevocation.OutputTokens != 13_422 {
				t.Fatalf("lane revocation metadata = %#v, want origin %q without declaring discarded output", metadata.LaneRevocation, tt.wantOrigin)
			}
			if len(tracker.comments) != 1 {
				t.Fatalf("comments = %#v, want preservation-unverified notice", tracker.comments)
			}
			for _, fragment := range []string{tt.wantReason, "lane_change_origin: " + string(tt.wantOrigin), "output_tokens: 13422", "runtime_seconds: 397"} {
				if !strings.Contains(tracker.comments[0].body, fragment) {
					t.Fatalf("comment %q missing %q", tracker.comments[0].body, fragment)
				}
			}
			if !hasLaneRevocationEvent(state.RecentEvents, "worker_lane_preservation_unverified") {
				t.Fatalf("RecentEvents = %#v, want preservation-unverified event", state.RecentEvents)
			}
		})
	}
}

func TestLaneRevocationAttributionEvidencePrecedence(t *testing.T) {
	t.Parallel()

	issue := laneRevocationIssue("issue-attribution", "digitaldrywood/detent#1988", "Human Review")
	human := provenance.AttributionFromSource(provenance.SourceHumanSession, provenance.Actor{Login: "operator", Kind: "User"})
	indeterminate := provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{})
	tests := []struct {
		name            string
		running         bool
		cached          *provenance.Attribution
		transitionActor connector.IssueActor
		workerActor     connector.IssueActor
		want            provenance.Origin
	}{
		{name: "user actor during active run", running: true, transitionActor: connector.IssueActor{Login: "operator-token", Kind: "User"}, want: provenance.OriginIndeterminate},
		{name: "active app worker", running: true, transitionActor: connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"}, workerActor: connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"}, want: provenance.OriginAgent},
		{name: "external automation", running: true, transitionActor: connector.IssueActor{Login: "external-bot", Kind: "Bot"}, workerActor: connector.IssueActor{Login: "detent-worker[bot]", Kind: "Bot"}, want: provenance.OriginAutomation},
		{name: "operator", running: true, cached: &human, want: provenance.OriginHuman},
		{name: "missing evidence", want: provenance.OriginIndeterminate},
		{name: "indeterminate cache remains indeterminate during active run", running: true, cached: &indeterminate, want: provenance.OriginIndeterminate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := State{
				Running:        map[string]Running{},
				laneProvenance: map[string]provenance.Attribution{},
			}
			if tt.running {
				state.Running[issue.ID] = Running{Issue: issue, WorkerGitHubActor: tt.workerActor}
			}
			if tt.cached != nil {
				state.laneProvenance[workflowLaneEntryKey(issue)] = *tt.cached
			}
			observed := cloneIssue(issue)
			observed.StageUpdatedActor = tt.transitionActor

			got := laneRevocationAttribution(&state, observed)
			if got.Origin != tt.want {
				t.Fatalf("laneRevocationAttribution() = %#v, want origin %q", got, tt.want)
			}
		})
	}
}

func TestLaneChangeFinishRaceRejectsArtifactCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 35, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-artifact-22", "digitaldrywood/video-studio#22", "Production")
	issue.Deliverable = &connector.Deliverable{Kind: "artifact"}
	issue.Fields = map[string]string{"render_status": "recut"}
	parked := cloneIssue(issue)
	parked.State = "Blocked"
	workpad := "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nfields:\n  render_status: pending_review\nblockers: []\nhuman_action: null\n```"
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{parked},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{Body: workpad}},
		},
	}
	attempts := &recordingWorkAttemptStore{}
	cfg := normalizeConfig(Config{
		Project:        scheduler.ProjectCandidate{ID: "video-studio"},
		ActiveStates:   []string{"Todo", "Production", "Rework"},
		ObservedStates: []string{"Blocked", "Review"},
		TerminalStates: []string{"Done", "Cancelled"},
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			SourceState: "Review",
			Gate:        artifactCompletionTestGate(),
		},
	})
	orch := &Orchestrator{
		cfg:                    cfg,
		connector:              tracker,
		workAttempts:           attempts,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:                    func() time.Time { return now },
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:               issue,
		Attempt:             1,
		WorkAttemptID:       44,
		Generation:          4,
		DispatchWorkpadRead: true,
		StartedAt:           now.Add(-time.Hour),
	}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{
			Issue:         issue,
			WorkAttemptID: 44,
			Generation:    4,
		},
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if len(tracker.setFields) != 0 || len(tracker.updates) != 0 {
		t.Fatalf("tracker writes = fields %#v updates %#v, want none", tracker.setFields, tracker.updates)
	}
	if got := tracker.stateIssues[0].Fields["render_status"]; got != "recut" {
		t.Fatalf("render_status = %q, want recut", got)
	}
	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalLaneRevoked {
		t.Fatalf("work attempt completions = %#v, want lane_revoked", attempts.completions)
	}
	var metadata struct {
		LaneRevocation struct {
			FromState string `json:"from_state"`
			ToState   string `json:"to_state"`
		} `json:"lane_revocation"`
	}
	if err := json.Unmarshal([]byte(attempts.completions[0].WorkerMetadataJSON), &metadata); err != nil {
		t.Fatalf("decode lane revocation metadata: %v", err)
	}
	if metadata.LaneRevocation.FromState != "Production" || metadata.LaneRevocation.ToState != "Blocked" {
		t.Fatalf("lane transition = %s -> %s, want Production -> Blocked", metadata.LaneRevocation.FromState, metadata.LaneRevocation.ToState)
	}
	if !hasLaneRevocationEvent(state.RecentEvents, "stale_worker_completion_rejected") {
		t.Fatalf("RecentEvents = %#v, want stale completion rejection", state.RecentEvents)
	}
}

func TestStaleWorkerGenerationCannotCompleteFreshLease(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 40, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-generation", "digitaldrywood/video-studio#23", "Production")
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	tracker := &runningStateConnector{issues: []connector.Issue{issue}}
	cfg := normalizeConfig(Config{
		Project:      scheduler.ProjectCandidate{ID: "video-studio"},
		ActiveStates: []string{"Todo", "Production", "Rework"},
	})
	orch := &Orchestrator{
		cfg:             cfg,
		connector:       tracker,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:             func() time.Time { return now },
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: 202, Generation: 2}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{
			Issue:         issue,
			WorkAttemptID: 101,
			Generation:    1,
		},
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	running, ok := state.Running[issue.ID]
	if !ok || running.Generation != 2 || running.WorkAttemptID != 202 {
		t.Fatalf("Running[%q] = %#v, want fresh generation 2 attempt 202", issue.ID, running)
	}
	if len(tracker.updates) != 0 || len(tracker.setFieldCalls) != 0 {
		t.Fatalf("tracker writes = updates %#v fields %#v, want none", tracker.updates, tracker.setFieldCalls)
	}
	events := metrics.snapshot()
	if len(events) != 1 || events[0].PhaseName != "stale_completion_rejected" || events[0].Status != "rejected" {
		t.Fatalf("workflow events = %#v, want stale completion rejection audit", events)
	}
}

func TestCompletionAfterLeaseCleanupRecordsRejection(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 42, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-cleaned-lease", "digitaldrywood/video-studio#23", "Production")
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	cfg := normalizeConfig(Config{
		Project:      scheduler.ProjectCandidate{ID: "video-studio"},
		ActiveStates: []string{"Todo", "Production", "Rework"},
	})
	orch := &Orchestrator{
		cfg:             cfg,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:             func() time.Time { return now },
	}
	state := newState(cfg)

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{
			Issue:         issue,
			WorkAttemptID: 101,
			Generation:    1,
		},
		CompletedAt: now,
	})

	if !hasLaneRevocationEvent(state.RecentEvents, "stale_worker_completion_rejected") {
		t.Fatalf("RecentEvents = %#v, want stale completion rejection", state.RecentEvents)
	}
	events := metrics.snapshot()
	if len(events) != 1 || events[0].PhaseName != "stale_completion_rejected" || events[0].Status != "rejected" {
		t.Fatalf("workflow events = %#v, want stale completion rejection audit", events)
	}
}

func TestMovingBackToActiveLaneCreatesFreshGeneration(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 45, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-reentry", "digitaldrywood/video-studio#24", "Production")
	tracker := &runningStateConnector{issues: []connector.Issue{issue}}
	runner := newWorkerHostRunner()
	cfg := normalizeConfig(Config{
		Project:             scheduler.ProjectCandidate{ID: "video-studio"},
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "Production", "Rework"},
		ObservedStates:      []string{"Blocked"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	orch, err := New(cfg, Dependencies{Connector: tracker, Runner: runner, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	state := newState(orch.cfg)
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if !orch.dispatchIssue(runCtx, &state, issue, 1, now, "") {
		t.Fatal("first dispatch = false, want true")
	}
	firstRequest := receiveWorkerHostRunRequest(t, runner.started)
	parked := cloneIssue(issue)
	parked.State = "Blocked"
	tracker.issues = []connector.Issue{parked}
	orch.reconcileRunningIssues(runCtx, &state, now.Add(time.Second))
	firstCompletion := receiveLaneRevocationCompletion(t, orch.runResults)
	orch.handleRunResult(runCtx, &state, firstCompletion)

	tracker.issues = []connector.Issue{issue}
	if !orch.dispatchIssue(runCtx, &state, issue, 2, now.Add(2*time.Second), "") {
		t.Fatal("second dispatch = false, want true")
	}
	secondRequest := receiveWorkerHostRunRequest(t, runner.started)
	if secondRequest.Generation <= firstRequest.Generation {
		t.Fatalf("second generation = %d, want greater than first generation %d", secondRequest.Generation, firstRequest.Generation)
	}

	orch.handleRunResult(runCtx, &state, firstCompletion)
	running, ok := state.Running[issue.ID]
	if !ok || running.Generation != secondRequest.Generation {
		t.Fatalf("Running[%q] = %#v, want fresh generation %d", issue.ID, running, secondRequest.Generation)
	}
	if !hasLaneRevocationEvent(state.RecentEvents, "stale_worker_completion_rejected") {
		t.Fatalf("RecentEvents = %#v, want stale completion rejection", state.RecentEvents)
	}

	if running.stop != nil {
		running.stop(runpkg.ErrLaneRevoked)
	}
	_ = receiveLaneRevocationCompletion(t, orch.runResults)
}

func TestLaneRevocationRecordsGraceTimeoutEscalation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 50, 0, 0, time.UTC)
	issue := laneRevocationIssue("issue-escalation", "digitaldrywood/video-studio#25", "Production")
	parked := cloneIssue(issue)
	parked.State = "Blocked"
	identity := procgroup.Identity{PID: 2501, GroupID: 2501, StartedAt: now.Add(-time.Hour)}
	processes := &laneRevocationProcessStore{active: []store.WorkerProcess{{
		SessionID: 55,
		IssueID:   issue.ID,
		WorkerProcessIdentity: store.WorkerProcessIdentity{
			PID:       identity.PID,
			GroupID:   identity.GroupID,
			StartedAt: identity.StartedAt,
		},
	}}}
	cfg := normalizeConfig(Config{ActiveStates: []string{"Production"}})
	orch := &Orchestrator{
		cfg:                    cfg,
		connector:              &runningStateConnector{issues: []connector.Issue{parked}},
		workerProcesses:        processes,
		workerReapGrace:        20 * time.Millisecond,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		reapWorkerProcess: func(_ context.Context, got procgroup.Identity, grace time.Duration) (procgroup.TerminationOutcome, error) {
			if got != identity || grace != 20*time.Millisecond {
				t.Fatalf("reap arguments = %#v, %s, want %#v, 20ms", got, grace, identity)
			}
			return procgroup.TerminationOutcomeKilled, nil
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:    func() time.Time { return now },
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:           issue,
		Generation:      5,
		DetentSessionID: 55,
		WorkerProcess:   identity,
	}

	orch.reconcileRunningIssues(t.Context(), &state, now)

	for _, event := range []string{"worker_lane_stop_requested", "worker_lane_stop_escalated", "worker_lane_stop_result"} {
		if !hasLaneRevocationEvent(state.RecentEvents, event) {
			t.Fatalf("RecentEvents = %#v, want %s", state.RecentEvents, event)
		}
	}
	if len(processes.reaped) != 1 || processes.reaped[0].Outcome != store.WorkerProcessOutcomeKilled {
		t.Fatalf("process reap records = %#v, want killed_after_timeout", processes.reaped)
	}
}

func TestReconcileRunningIssuesStopsVideoStudioIncidentWorkers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 16, 18, 35, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{ActiveStates: []string{"Production"}})
	state := newState(cfg)
	tracker := &runningStateConnector{}
	orch := &Orchestrator{
		cfg:                    cfg,
		connector:              tracker,
		pendingLaneRevocations: map[string]*pendingLaneRevocation{},
		logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:                    func() time.Time { return now },
	}
	contexts := make([]context.Context, 0, 6)
	for number := 22; number <= 27; number++ {
		id := "wi-video-studio-" + strconv.Itoa(number)
		if number == 22 {
			id = "wi-0c7d736611a111641bd57b97"
		}
		issue := laneRevocationIssue(id, "digitaldrywood/video-studio#"+strconv.Itoa(number), "Production")
		parked := cloneIssue(issue)
		parked.State = "Blocked"
		tracker.issues = append(tracker.issues, parked)
		runCtx, stop := context.WithCancelCause(context.Background())
		contexts = append(contexts, runCtx)
		state.Running[id] = Running{Issue: issue, Generation: uint64(number), stop: stop}
	}

	orch.reconcileRunningIssues(t.Context(), &state, now)

	for index, runCtx := range contexts {
		if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
			t.Fatalf("worker #%d context cause = %v, want ErrLaneRevoked", index+22, context.Cause(runCtx))
		}
	}
	if len(orch.pendingLaneRevocations) != 6 {
		t.Fatalf("pending lane revocations = %d, want 6", len(orch.pendingLaneRevocations))
	}
}

func laneRevocationIssue(id string, identifier string, state string) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = identifier
	issue.Title = "Lane revocation test"
	issue.State = state
	return issue
}

func completionHandshakeWorkpadBody(workAttemptID int64, generation uint64) string {
	return "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nfields:\n" +
		"  completion_work_attempt_id: \"" + strconv.FormatInt(workAttemptID, 10) + "\"\n" +
		"  completion_generation: \"" + strconv.FormatUint(generation, 10) + "\"\n" +
		"blockers: []\nhuman_action: null\n```"
}

func receiveLaneRevocationCompletion(t *testing.T, completions <-chan runpkg.Completion) runpkg.Completion {
	t.Helper()

	select {
	case completion := <-completions:
		return completion
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker completion")
		return runpkg.Completion{}
	}
}

func hasLaneRevocationEvent(events []telemetry.ActivityEvent, name string) bool {
	for _, event := range events {
		if event.Event == name {
			return true
		}
	}
	return false
}

type laneRevocationProcessStore struct {
	active []store.WorkerProcess
	reaped []store.WorkerProcessReap
}

func (s *laneRevocationProcessStore) ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error) {
	return append([]store.WorkerProcess(nil), s.active...), nil
}

func (s *laneRevocationProcessStore) MarkSessionWorkerProcessReaped(_ context.Context, _ int64, reap store.WorkerProcessReap) error {
	s.reaped = append(s.reaped, reap)
	return nil
}
