package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/provenance"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestLaneRevocationRetainsWorkspaceForEveryWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		source     provenance.Source
		pushed     bool
		filesystem bool
	}{
		{name: "operator unpushed", source: provenance.SourceHumanSession},
		{name: "operator pushed", source: provenance.SourceHumanSession, pushed: true},
		{name: "Detent unpushed", source: provenance.SourceDetentInstance},
		{name: "Detent pushed", source: provenance.SourceDetentInstance, pushed: true},
		{name: "agent unpushed", source: provenance.SourceDetentAgentSession},
		{name: "external automation unpushed", source: provenance.SourceExternalAutomation},
		{name: "filesystem artifact", source: provenance.SourceHumanSession, filesystem: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 4, 4, 11, 0, 0, time.UTC)
			issue := laneRevocationIssue("retention", "digitaldrywood/detent#2138", "In Progress")
			parked := cloneIssue(issue)
			parked.State = "Done"
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{parked}}
			attempts := &recordingWorkAttemptStore{}
			retention := &laneRetentionProbe{t: t, result: workspace.Preservation{
				Path: "/retained/workspace", Branch: "detent/2138", HeadSHA: "abc123", Preserved: true, LocalChangesVerified: true,
				UnpushedCommits: 1, TrackedPaths: []string{"implementation.go"},
			}}
			if tt.pushed {
				retention.result.Delivery = &workspace.DeliverableState{CommitsAhead: 1, Remote: "origin", RemoteRef: "refs/heads/detent/2138", LocalHeadSHA: "abc123", RemoteHeadSHA: "abc123", RemoteBranchExists: true}
			}
			if tt.filesystem {
				retention.result = workspace.Preservation{Path: "/retained/workspace", Preserved: true, LocalChangesVerified: true, Files: 1}
			}
			cfg := laneMutationTestConfig()
			orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts, reaper: retention,
				logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: func() time.Time { return now }}
			state := newState(cfg)
			runCtx, stop := context.WithCancelCause(t.Context())
			state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: 2138, Generation: 38,
				StartedAt: now.Add(-time.Hour), WorkProductPushed: tt.pushed, stop: stop}
			state.Claimed[issue.ID] = Claimed{Issue: issue}
			attribution := provenance.AttributionFromSource(tt.source, provenance.Actor{Login: "writer", Kind: "User"})
			state.laneProvenance[workflowLaneEntryKey(parked)] = attribution
			orch.reconcileRunningIssues(t.Context(), &state, now)
			if !errors.Is(context.Cause(runCtx), runpkg.ErrLaneRevoked) {
				t.Fatalf("worker cause = %v, want revocation", context.Cause(runCtx))
			}
			pending := orch.pendingLaneRevocations[issue.ID]
			if pending == nil || !pending.reapDone {
				t.Fatal("worker must be reaped before preservation")
			}
			orch.handleRunResult(runCtx, &state, runpkg.Completion{
				IssueID: issue.ID, Request: runpkg.RunRequest{Issue: issue, WorkAttemptID: 2138, Generation: 38},
				CompletedAt: now, Err: runpkg.ErrLaneRevoked, Result: runpkg.RunResult{FinalState: runpkg.FinalStateLaneRevoked},
			})
			if len(attempts.completions) != 1 || retention.preserveCalls != 1 || retention.reapCalls != 1 {
				t.Fatalf("completions = %d, preservation calls = %d, reap calls = %d", len(attempts.completions), retention.preserveCalls, retention.reapCalls)
			}
			var metadata struct {
				Preservation workspace.Preservation `json:"workspace_preservation"`
				Revocation   struct {
					Classification string                 `json:"classification"`
					Provenance     provenance.Attribution `json:"provenance"`
					WorkDiscarded  bool                   `json:"work_discarded"`
				} `json:"lane_revocation"`
			}
			completion := attempts.completions[0]
			if err := json.Unmarshal([]byte(completion.WorkerMetadataJSON), &metadata); err != nil {
				t.Fatal(err)
			}
			wantClass, wantTerminal := laneRevocationPreservedClassification, store.WorkAttemptTerminalLaneRevoked
			if tt.pushed {
				wantClass, wantTerminal = laneRevocationDeliveredClassification, store.WorkAttemptTerminalDelivered
			}
			if completion.TerminalState != wantTerminal || metadata.Revocation.Classification != wantClass || metadata.Revocation.WorkDiscarded {
				t.Fatalf("completion = %#v, metadata = %#v", completion, metadata)
			}
			if metadata.Preservation.Path != retention.result.Path || metadata.Revocation.Provenance.Initiator != attribution.Initiator {
				t.Fatalf("retention/writer evidence = %#v", metadata)
			}
			if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "workspace_path: /retained/workspace") {
				t.Fatalf("retention notice = %#v", tracker.comments)
			}
			if len(tracker.updates) != 0 || len(state.Running) != 0 || len(state.Claimed) != 0 || len(state.CleanupFailures) != 0 {
				t.Fatal("revocation must release ownership without changing the tracker or reporting retention as a cleanup fault")
			}
			if !hasLaneRevocationEvent(state.RecentEvents, "workspace_reap_preserved") {
				t.Fatal("terminal cleanup did not report preserved work")
			}
		})
	}
}

func TestLaneRevocationRetriesFailedRetentionBeforeReleasingOwnership(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	issue := laneRevocationIssue("retention-retry", "digitaldrywood/detent#2138", "Blocked")
	retention := &laneRetentionProbe{t: t, err: errors.New("retention record unavailable")}
	attempts := &recordingWorkAttemptStore{}
	orch := &Orchestrator{cfg: laneMutationTestConfig(), reaper: retention, workAttempts: attempts}
	state := newState(orch.cfg)
	running := Running{Issue: issue, WorkAttemptID: 2138, Generation: 38}
	state.Running[issue.ID] = running
	state.Claimed[issue.ID] = Claimed{Issue: issue}
	pending := &pendingLaneRevocation{issue: issue, running: running, fromState: "In Progress", toState: "Blocked", reapDone: true, mutationRead: true,
		completion: &runpkg.Completion{IssueID: issue.ID, CompletedAt: now}}
	orch.pendingLaneRevocations = map[string]*pendingLaneRevocation{issue.ID: pending}
	orch.finishLaneRevocation(t.Context(), &state, pending)
	if len(attempts.completions) != 0 || len(state.Running) != 1 || len(state.Claimed) != 1 {
		t.Fatal("failed retention released durable ownership")
	}
	retention.result = workspace.Preservation{Path: "/retained/retry", Preserved: true, LocalChangesVerified: true, UnpushedCommits: 1}
	retention.err = nil
	orch.finishLaneRevocation(t.Context(), &state, pending)
	if len(attempts.completions) != 1 || len(state.Running) != 0 || len(state.Claimed) != 0 || retention.preserveCalls != 2 {
		t.Fatal("successful retention retry did not finish revocation exactly once")
	}
}

type laneRetentionProbe struct {
	t             *testing.T
	result        workspace.Preservation
	err           error
	preserveCalls int
	reapCalls     int
}

func (p *laneRetentionProbe) PreserveWorkspace(ctx context.Context, _ connector.Issue) (workspace.Preservation, error) {
	p.t.Helper()
	if err := ctx.Err(); err != nil {
		p.t.Fatalf("preservation inherited worker cancellation: %v", err)
	}
	p.preserveCalls++
	return p.result, p.err
}

func (p *laneRetentionProbe) ReapWorkspace(context.Context, connector.Issue) (WorkspaceReapResult, error) {
	p.t.Helper()
	if p.preserveCalls == 0 {
		p.t.Fatal("cleanup preceded preservation")
	}
	p.reapCalls++
	return WorkspaceReapResult{}, workspace.ErrWorkspacePreserved
}

func TestLaneRevocationRequiresVerifiedWork(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		preservation *workspace.Preservation
		pushed       bool
		want         string
	}{
		{name: "output alone", want: laneRevocationUnverifiedClassification},
		{name: "clean base", preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, HeadSHA: "base"}, want: laneRevocationEmptyClassification},
		{name: "clean base with push signal", pushed: true, preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, HeadSHA: "base"}, want: laneRevocationEmptyClassification},
		{name: "dirty files", preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, TrackedPaths: []string{"main.go"}}, want: laneRevocationPreservedClassification},
		{name: "local commits", preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, UnpushedCommits: 1}, want: laneRevocationPreservedClassification},
		{name: "failed local inspection", preservation: &workspace.Preservation{Preserved: true, HeadSHA: "base"}, want: laneRevocationUnverifiedClassification},
		{name: "push signal only", pushed: true, want: laneRevocationUnverifiedClassification},
		{name: "verified push", pushed: true, preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, Delivery: &workspace.DeliverableState{RemoteBranchExists: true, CommitsAhead: 1, Remote: "origin", RemoteRef: "refs/heads/worker", LocalHeadSHA: "work", RemoteHeadSHA: "work"}}, want: laneRevocationDeliveredClassification},
		{name: "base pushed", pushed: true, preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, Delivery: &workspace.DeliverableState{RemoteBranchExists: true, Remote: "origin", RemoteRef: "refs/heads/worker", LocalHeadSHA: "base", RemoteHeadSHA: "base"}}, want: laneRevocationEmptyClassification},
		{name: "different remote head", pushed: true, preservation: &workspace.Preservation{Preserved: true, LocalChangesVerified: true, UnpushedCommits: 1, Delivery: &workspace.DeliverableState{RemoteBranchExists: true, CommitsAhead: 1, Remote: "origin", RemoteRef: "refs/heads/worker", LocalHeadSHA: "work", RemoteHeadSHA: "old"}}, want: laneRevocationPreservedClassification},
	} {
		t.Run(tt.name, func(t *testing.T) {
			running := Running{WorkProductPushed: tt.pushed}
			event := runpkg.Completion{Result: runpkg.RunResult{TurnStarted: true, Output: "completed", Tokens: TokenTotals{TotalTokens: 100}}}
			receipt := laneRevocationReceipt(event, running, tt.preservation, time.Now())
			outcome := classifyLaneRevocation(receipt, tt.preservation, true, laneRevocationDetentStateChanged, provenance.OriginDetent)
			if outcome.classification != tt.want {
				t.Fatalf("classification = %s, want %s", outcome.classification, tt.want)
			}
			if !outcome.comment {
				return
			}
			pending := &pendingLaneRevocation{fromState: "In Progress", toState: "Blocked", reason: laneRevocationDetentStateChanged, origin: provenance.OriginDetent}
			body := laneRevocationOutcomeComment(pending, event, running, event.Result.Tokens, outcome)
			if !strings.Contains(body, "verified detent lane change from In Progress to Blocked") || strings.Contains(body, "the tracker moved") {
				t.Fatalf("misleading provenance: %s", body)
			}
			if strings.Contains(body, "work was pushed") != (tt.want == laneRevocationDeliveredClassification) {
				t.Fatalf("unverified delivery claim: %s", body)
			}
		})
	}
}
