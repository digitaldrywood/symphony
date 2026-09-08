package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

const (
	laneRevocationStateChanged               = "tracker_lane_changed"
	laneRevocationDetentStateChanged         = "detent_tracker_lane_changed"
	laneRevocationDetentErrorClass           = "detent_lane_revoked"
	laneRevocationCompletionFenceUnavailable = "completion_fence_unavailable"
	laneRevocationDeliveredClassification    = "delivered_before_revocation"
	laneRevocationPreservedClassification    = "unpushed_work_preserved"
	laneRevocationUnverifiedClassification   = "work_preservation_unverified"
	laneRevocationEmptyClassification        = "revoked_without_work"
	laneRevocationDeliveryReceiptKind        = "pushed_work_product"
)

type laneRevocationDeliveryReceipt struct {
	Schema        int       `json:"schema"`
	Kind          string    `json:"kind"`
	RecordedAt    time.Time `json:"recorded_at"`
	Source        string    `json:"source"`
	PRNumber      int       `json:"pr_number,omitempty"`
	PRHeadSHA     string    `json:"pr_head_sha,omitempty"`
	Remote        string    `json:"remote"`
	RemoteRef     string    `json:"remote_ref"`
	RemoteHeadSHA string    `json:"remote_head_sha"`
}

type laneRevocationOutcome struct {
	classification    string
	terminalState     store.WorkAttemptTerminalState
	sessionFinalState string
	errorClass        string
	errorMessage      string
	phase             string
	statusMessage     string
	activityEvent     string
	comment           bool
	workDiscarded     bool
}

type pendingLaneRevocation struct {
	issue            connector.Issue
	fromState        string
	toState          string
	reason           string
	origin           provenance.Origin
	requestedAt      time.Time
	generation       uint64
	running          Running
	completion       *runpkg.Completion
	workerProcess    procgroup.Identity
	reapOutcome      procgroup.TerminationOutcome
	reapDone         bool
	reapErr          error
	mutation         store.LaneMutationReceipt
	mutationRead     bool
	attribution      provenance.Attribution
	preservation     *workspace.Preservation
	preservationRead bool
	preservationErr  error
}

func laneRevocationTransitionError(fromState, toState string) error {
	fromState, toState = strings.TrimSpace(fromState), strings.TrimSpace(toState)
	if fromState == "" || toState == "" {
		return errors.New("lane change cannot be verified: before or after lane is unknown")
	}
	if strings.EqualFold(fromState, toState) {
		return fmt.Errorf("lane unchanged at %s; completion fence requires a verified transition", fromState)
	}
	return nil
}

func (o *Orchestrator) beginLaneRevocation(
	ctx context.Context,
	state *State,
	running Running,
	refreshed connector.Issue,
	now time.Time,
	reason string,
) {
	o.beginLaneRevocationWithMutation(ctx, state, running, refreshed, now, reason, store.LaneMutationReceipt{})
}

func (o *Orchestrator) beginLaneRevocationForMutation(
	ctx context.Context,
	state *State,
	running Running,
	refreshed connector.Issue,
	now time.Time,
	receipt store.LaneMutationReceipt,
) {
	o.beginLaneRevocationWithMutation(ctx, state, running, refreshed, now, receipt.Reason, receipt)
}

func (o *Orchestrator) beginLaneRevocationWithMutation(
	ctx context.Context,
	state *State,
	running Running,
	refreshed connector.Issue,
	now time.Time,
	reason string,
	receipt store.LaneMutationReceipt,
) {
	issueID := strings.TrimSpace(running.Issue.ID)
	if state == nil || issueID == "" {
		return
	}
	if pending, ok := o.pendingLaneRevocations[issueID]; ok {
		if !pending.reapDone {
			o.reapPendingLaneRevocation(ctx, state, pending)
		}
		if pending.completion != nil && pending.reapDone && !pending.mutationRead {
			o.consumePendingLaneRevocation(ctx, pending, pending.completion.CompletedAt)
		}
		if pending.completion != nil && pending.reapDone && pending.mutationRead {
			o.finishLaneRevocation(ctx, state, pending)
		}
		return
	}
	if o.pendingLaneRevocations == nil {
		o.pendingLaneRevocations = map[string]*pendingLaneRevocation{}
	}
	if now.IsZero() {
		now = o.clockNow().UTC()
	}
	fromState := strings.TrimSpace(running.Issue.State)
	attribution := laneRevocationAttribution(state, refreshed)
	if receipt.ID > 0 {
		fromState = strings.TrimSpace(receipt.FromState)
		attribution = provenance.AttributionFromSource(provenance.SourceDetentInstance, provenance.Actor{})
		reason = strings.TrimSpace(receipt.Reason)
	}
	if err := laneRevocationTransitionError(fromState, refreshed.State); err != nil {
		recordStateEvent(state, telemetry.ActivityEvent{At: now, Event: "lane_revocation_unverified", Message: err.Error()})
		return
	}
	running.Issue = mergeIssueTrackerFields(running.Issue, refreshed)
	reason = laneRevocationReason(reason, attribution)
	pending := &pendingLaneRevocation{
		issue:        cloneIssue(running.Issue),
		fromState:    fromState,
		toState:      strings.TrimSpace(running.Issue.State),
		reason:       strings.TrimSpace(reason),
		origin:       attribution.Origin,
		requestedAt:  now.UTC(),
		generation:   running.Generation,
		running:      running,
		mutation:     receipt,
		mutationRead: receipt.ID == 0,
		attribution:  attribution,
	}
	o.pendingLaneRevocations[issueID] = pending
	state.Running[issueID] = running
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	if running.stop != nil {
		running.stop(runpkg.ErrLaneRevoked)
	} else if running.cancel != nil {
		running.cancel()
	}
	if o.logger != nil {
		o.logger.Info(
			"worker lane stop requested",
			"project_id", strings.TrimSpace(o.cfg.Project.ID),
			"issue_id", issueID,
			"identifier", running.Issue.Identifier,
			"generation", running.Generation,
			"work_attempt_id", running.WorkAttemptID,
			"from_state", fromState,
			"to_state", running.Issue.State,
			"reason", pending.reason,
			"lane_change_origin", pending.origin,
			"lane_change_initiator", attribution.Initiator,
			"lane_change_basis", attribution.Basis,
			"lane_mutation_receipt_id", receipt.ID,
			"grace", o.workerReapGrace,
		)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      pending.requestedAt,
		Event:   "worker_lane_stop_requested",
		Message: "stopping worker for " + issueLabel(running.Issue) + " after lane changed from " + fromState + " to " + running.Issue.State,
	})
	o.reapPendingLaneRevocation(ctx, state, pending)
}

func (o *Orchestrator) reapPendingLaneRevocation(ctx context.Context, state *State, pending *pendingLaneRevocation) {
	if pending == nil || pending.reapDone {
		return
	}
	outcome, identity, err := o.reapRunningWorker(ctx, pending.running, pending.workerProcess, "lane_revoked")
	if identity.PID > 0 {
		pending.workerProcess = identity
	}
	pending.reapOutcome = outcome
	pending.reapDone = err == nil
	pending.reapErr = err
	at := o.clockNow().UTC()
	if outcome == procgroup.TerminationOutcomeKilled {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "worker_lane_stop_escalated",
			Message: "worker for " + issueLabel(pending.issue) + " exceeded the graceful stop bound and was killed",
		})
	}
	if o.logger != nil {
		attrs := []any{
			"project_id", strings.TrimSpace(o.cfg.Project.ID),
			"issue_id", pending.issue.ID,
			"identifier", pending.issue.Identifier,
			"generation", pending.generation,
			"work_attempt_id", pending.running.WorkAttemptID,
			"decision", string(outcome),
			"pid", identity.PID,
			"pgid", identity.GroupID,
			"grace", o.workerReapGrace,
		}
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		o.logger.Info("worker lane stop result", attrs...)
	}
	event := "worker_lane_stop_result"
	message := "worker stop for " + issueLabel(pending.issue) + " finished with " + string(outcome)
	if err != nil {
		event = "worker_lane_stop_failed"
		message = "worker stop for " + issueLabel(pending.issue) + " failed: " + err.Error()
	}
	recordStateEvent(state, telemetry.ActivityEvent{At: at, Event: event, Message: message})
}

func (o *Orchestrator) handleLaneRevocationCompletion(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) bool {
	pending, ok := o.pendingLaneRevocations[event.IssueID]
	if !ok {
		return false
	}
	completedAt := event.CompletedAt.UTC()
	if completedAt.IsZero() {
		completedAt = o.clockNow().UTC()
	}
	releaseWorkerGitHubMonitorProbe(state, event.IssueID, "deferred", pending.reason, completedAt)
	pending.running = running
	pending.running.Issue = cloneIssue(pending.issue)
	pending.completion = &event
	if !pending.reapDone {
		o.reapPendingLaneRevocation(ctx, state, pending)
	}
	if pending.reapDone && !pending.mutationRead {
		o.consumePendingLaneRevocation(ctx, pending, event.CompletedAt)
	}
	if pending.reapDone && pending.mutationRead {
		o.finishLaneRevocation(ctx, state, pending)
	}
	return true
}

func (o *Orchestrator) consumePendingLaneRevocation(ctx context.Context, pending *pendingLaneRevocation, at time.Time) {
	if pending == nil || pending.mutationRead || pending.mutation.ID <= 0 {
		return
	}
	consumed, err := o.consumeLaneMutationReceipt(ctx, pending.mutation, pending.running, pending.toState, at)
	if err != nil {
		pending.reapErr = errors.Join(pending.reapErr, err)
		if o.logger != nil {
			o.logger.Warn("lane revocation receipt consumption failed", "issue_id", pending.issue.ID, "receipt_id", pending.mutation.ID, "error", err)
		}
		return
	}
	pending.mutation = consumed
	pending.mutationRead = true
}

func (o *Orchestrator) finishLaneRevocation(ctx context.Context, state *State, pending *pendingLaneRevocation) {
	if pending == nil || pending.completion == nil || !pending.reapDone || !pending.mutationRead {
		return
	}
	if err := laneRevocationTransitionError(pending.fromState, pending.toState); err != nil {
		delete(o.pendingLaneRevocations, pending.issue.ID)
		o.deferTrackerUnavailableCompletion(ctx, state, *pending.completion, pending.running, err)
		return
	}
	event := *pending.completion
	running := pending.running
	running.Issue = cloneIssue(pending.issue)
	completedAt := event.CompletedAt.UTC()
	if completedAt.IsZero() {
		completedAt = o.clockNow().UTC()
	}
	if !o.preserveLaneRevocationWorkspace(ctx, state, pending, completedAt) {
		return
	}
	o.heartbeats.remove(event.IssueID)
	o.releaseGlobalDispatchSlot(running.globalSlot)
	running.globalSlot = scheduler.Slot{}
	if !event.Result.RuntimeIdentity.IsZero() {
		running.RuntimeIdentity = running.RuntimeIdentity.Merge(event.Result.RuntimeIdentity)
	}
	if diffStatsPresent(event.Result.DiffStats) {
		running.DiffStats = event.Result.DiffStats
		state.DiffStats[event.IssueID] = event.Result.DiffStats
	}
	tokens := event.Result.Tokens
	if tokens == (TokenTotals{}) {
		tokens = running.Tokens
	}
	running.Tokens = tokens
	running.WorkProductPushed = running.WorkProductPushed || event.Result.PullRequestHeadPushed || event.Result.PullRequestUpdated
	receipt := laneRevocationReceipt(event, running, pending.preservation, completedAt)
	workProduced := laneRevocationProducedWork(event, running, tokens) || laneRevocationLocalWork(pending.preservation)
	outcome := classifyLaneRevocation(receipt, pending.preservation, workProduced, pending.reason, pending.origin)
	metadata := map[string]any{"lane_revocation": map[string]any{
		"classification":   outcome.classification,
		"generation":       pending.generation,
		"from_state":       pending.fromState,
		"to_state":         pending.toState,
		"reason":           pending.reason,
		"origin":           pending.origin,
		"provenance":       pending.attribution,
		"mutation_receipt": pending.mutation,
		"requested_at":     pending.requestedAt,
		"reap_outcome":     pending.reapOutcome,
		"work_discarded":   outcome.workDiscarded,
		"output_tokens":    tokens.OutputTokens,
		"total_tokens":     tokens.TotalTokens,
		"runtime_seconds":  tokens.RuntimeSeconds,
		"turns":            running.TurnCount,
		"files_changed":    running.DiffStats.FilesChanged,
	}}
	if receipt != nil {
		metadata["delivery_receipt"] = receipt
	}
	if pending.preservation != nil {
		metadata["workspace_preservation"] = pending.preservation
	}
	if pending.preservationErr != nil {
		metadata["workspace_preservation_error"] = pending.preservationErr.Error()
	}
	attemptCompleted := o.completeDurableWorkAttemptWithSessionState(
		ctx,
		state,
		running,
		completedAt,
		outcome.terminalState,
		outcome.sessionFinalState,
		outcome.errorClass,
		outcome.errorMessage,
		outcome.phase,
		outcome.statusMessage,
		metadata,
	)
	if o.workAttempts != nil && running.WorkAttemptID > 0 && !attemptCompleted {
		return
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, tokens)
	state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	attemptErr := error(nil)
	if outcome.terminalState == store.WorkAttemptTerminalLaneRevoked {
		attemptErr = runpkg.ErrLaneRevoked
	}
	o.recordProjectAttemptOutcome(
		state,
		event.IssueID,
		completedAt,
		outcome.terminalState,
		attemptErr,
		outcome.errorClass,
		outcome.errorMessage,
	)
	o.reportLaneRevocationOutcome(ctx, state, pending, event, running, tokens, outcome)
	delete(o.pendingLaneRevocations, event.IssueID)
	delete(state.Running, event.IssueID)
	delete(state.Claimed, event.IssueID)
	delete(state.Retry, event.IssueID)
	delete(state.BudgetRefusals, event.IssueID)
	delete(state.PriorAttempts, event.IssueID)
	delete(state.InstantFailures, event.IssueID)
	delete(state.RepeatedFailures, event.IssueID)
	releaseDispatchRecoveryAdmission(state, event.IssueID)
	releaseProjectFailureBreakerCanary(state, event.IssueID)
	if err := o.abandonClaim(context.WithoutCancel(ctx), event.IssueID); err != nil && o.logger != nil {
		o.logger.Warn("lane revocation claim release failed", "issue_id", event.IssueID, "error", err)
	}
	if workspaceIssueTerminal(running.Issue, o.cfg.TerminalStates) {
		finalState := strings.TrimSpace(running.Issue.State)
		if finalState == "" {
			finalState = runpkg.FinalStateLaneRevoked
		}
		state.Completed[event.IssueID] = Completed{
			Issue:           cloneIssue(running.Issue),
			SessionID:       running.SessionID,
			StartedAt:       running.StartedAt,
			CompletedAt:     terminalCompletedAt(running.Issue, o.cfg.TerminalStates, completedAt),
			FinalState:      finalState,
			Tokens:          tokens,
			RuntimeIdentity: running.RuntimeIdentity,
		}
		o.recordEfficiencyReceipt(ctx, running.Issue, completedAt)
		if !workProduced || pending.preservation != nil && pending.preservation.Preserved {
			o.reapWorkspace(ctx, state, running.Issue, workspaceReapReason(running.Issue, o.cfg.TerminalStates), completedAt)
		}
	}
	if o.logger != nil {
		o.logger.Info(
			"worker lane revocation completed",
			"project_id", strings.TrimSpace(o.cfg.Project.ID),
			"issue_id", event.IssueID,
			"identifier", running.Issue.Identifier,
			"generation", pending.generation,
			"work_attempt_id", running.WorkAttemptID,
			"to_state", running.Issue.State,
			"reap_outcome", pending.reapOutcome,
		)
	}
}

func (o *Orchestrator) preserveLaneRevocationWorkspace(ctx context.Context, state *State, pending *pendingLaneRevocation, at time.Time) bool {
	if pending.preservationRead {
		return true
	}
	preserver, ok := o.reaper.(runpkg.WorkspacePreserver)
	if !ok {
		pending.preservationErr = runpkg.ErrWorkspacePreservationUnavailable
		pending.preservationRead = true
		return true
	}
	preservationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	preservation, err := preserver.PreserveWorkspace(preservationCtx, pending.issue)
	pending.preservation = &preservation
	pending.preservationErr = err
	pending.preservationRead = err == nil || preservation.Preserved ||
		errors.Is(err, workspace.ErrMissingWorkspace) || errors.Is(err, runpkg.ErrWorkspacePreservationUnavailable)
	if !pending.preservationRead {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "worker_lane_preservation_failed",
			Message: "retaining worker ownership for " + issueLabel(pending.issue) + " until workspace preservation succeeds: " + err.Error(),
		})
	}
	return pending.preservationRead
}

func laneRevocationLocalWork(preservation *workspace.Preservation) bool {
	return preservation != nil && preservation.LocalChangesVerified && (preservation.Files > 0 || preservation.UnpushedCommits > 0 || len(preservation.TrackedPaths) > 0 || len(preservation.UntrackedPaths) > 0)
}

func laneRevocationAttribution(state *State, issue connector.Issue) provenance.Attribution {
	if state != nil {
		key := workflowLaneEntryKey(issue)
		current := issue.StageUpdatedAt == nil || issue.StageUpdatedAt.IsZero() || !issue.StageUpdatedAt.After(state.laneEntries[key])
		if attribution, ok := state.laneProvenance[key]; ok && current {
			attribution = provenance.Prepare(attribution)
			if provenance.NormalizeOrigin(attribution.Origin) != provenance.OriginIndeterminate {
				return attribution
			}
		}
	}
	return observedLaneAttribution(state, issue, issue.StageUpdatedActor)
}

func laneRevocationReason(reason string, attribution provenance.Attribution) string {
	reason = strings.TrimSpace(reason)
	if reason == laneRevocationStateChanged && provenance.NormalizeOrigin(attribution.Origin) == provenance.OriginDetent {
		return laneRevocationDetentStateChanged
	}
	return reason
}

func laneRevocationErrorClass(reason string, origin provenance.Origin) string {
	if strings.TrimSpace(reason) == laneRevocationDetentStateChanged || provenance.NormalizeOrigin(origin) == provenance.OriginDetent {
		return laneRevocationDetentErrorClass
	}
	return string(store.WorkAttemptTerminalLaneRevoked)
}

func laneRevocationReceipt(event runpkg.Completion, running Running, preservation *workspace.Preservation, completedAt time.Time) *laneRevocationDeliveryReceipt {
	if !running.WorkProductPushed || preservation == nil || preservation.Delivery == nil {
		return nil
	}
	delivery := preservation.Delivery
	if !delivery.RemoteBranchExists || delivery.CommitsAhead <= 0 || strings.TrimSpace(delivery.Remote) == "" ||
		strings.TrimSpace(delivery.RemoteRef) == "" || strings.TrimSpace(delivery.RemoteHeadSHA) == "" ||
		delivery.LocalHeadSHA != delivery.RemoteHeadSHA {
		return nil
	}
	source := "work_attempt"
	if event.Result.PullRequestHeadPushed || event.Result.PullRequestUpdated {
		source = "runner_result"
	}
	receipt := &laneRevocationDeliveryReceipt{
		Schema:        1,
		Kind:          laneRevocationDeliveryReceiptKind,
		RecordedAt:    completedAt,
		Source:        source,
		Remote:        delivery.Remote,
		RemoteRef:     delivery.RemoteRef,
		RemoteHeadSHA: delivery.RemoteHeadSHA,
	}
	if running.Issue.PullRequest != nil {
		receipt.PRNumber = running.Issue.PullRequest.Number
		receipt.PRHeadSHA = strings.TrimSpace(running.Issue.PullRequest.HeadSHA)
	}
	return receipt
}

func classifyLaneRevocation(receipt *laneRevocationDeliveryReceipt, preservation *workspace.Preservation, workProduced bool, reason string, origin provenance.Origin) laneRevocationOutcome {
	if receipt != nil && receipt.Schema == 1 && receipt.Kind == laneRevocationDeliveryReceiptKind &&
		strings.TrimSpace(receipt.Remote) != "" && strings.TrimSpace(receipt.RemoteRef) != "" && strings.TrimSpace(receipt.RemoteHeadSHA) != "" {
		return laneRevocationOutcome{
			classification:    laneRevocationDeliveredClassification,
			terminalState:     store.WorkAttemptTerminalDelivered,
			sessionFinalState: string(store.WorkAttemptTerminalDelivered),
			phase:             "completed",
			statusMessage:     "work was pushed but finalization was rejected",
			activityEvent:     "worker_lane_delivery_preserved",
			comment:           true,
		}
	}
	outcome := laneRevocationOutcome{
		classification:    laneRevocationEmptyClassification,
		terminalState:     store.WorkAttemptTerminalLaneRevoked,
		sessionFinalState: runpkg.FinalStateLaneRevoked,
		errorClass:        laneRevocationErrorClass(reason, origin),
		errorMessage:      strings.TrimSpace(reason),
		phase:             "lane_revoked",
		statusMessage:     "worker stopped after leaving a worker-owned lane",
		activityEvent:     "worker_lane_revoked",
	}
	localWork := laneRevocationLocalWork(preservation)
	if localWork || workProduced && (preservation == nil || !preservation.LocalChangesVerified) {
		outcome.classification = laneRevocationUnverifiedClassification
		outcome.activityEvent = "worker_lane_preservation_unverified"
		outcome.statusMessage = "worker stopped; workspace preservation could not be verified"
		outcome.comment = true
		if localWork && preservation.Preserved {
			outcome.classification = laneRevocationPreservedClassification
			outcome.activityEvent = "worker_lane_workspace_preserved"
			outcome.statusMessage = "worker stopped; local workspace retained for recovery"
		}
	}
	return outcome
}

func laneRevocationProducedWork(event runpkg.Completion, running Running, tokens TokenTotals) bool {
	return event.Result.TurnStarted ||
		running.TurnCount > 0 ||
		strings.TrimSpace(event.Result.Output) != "" ||
		strings.TrimSpace(running.LastMessage) != "" ||
		tokens.OutputTokens > 0 ||
		tokens.TotalTokens > 0 ||
		diffStatsPresent(event.Result.DiffStats) ||
		diffStatsPresent(running.DiffStats)
}

func (o *Orchestrator) reportLaneRevocationOutcome(
	ctx context.Context,
	state *State,
	pending *pendingLaneRevocation,
	event runpkg.Completion,
	running Running,
	tokens TokenTotals,
	outcome laneRevocationOutcome,
) {
	at := event.CompletedAt.UTC()
	if at.IsZero() {
		at = o.clockNow().UTC()
	}
	origin := string(provenance.NormalizeOrigin(pending.origin))
	message := "worker for " + issueLabel(running.Issue) + " was stopped after a " + origin + " lane change from " + pending.fromState + " to " + pending.toState
	if outcome.terminalState == store.WorkAttemptTerminalDelivered {
		message = "pushed work for " + issueLabel(running.Issue) + " was preserved after finalization was rejected by a " + origin + " lane change from " + pending.fromState + " to " + pending.toState
	} else if outcome.classification == laneRevocationPreservedClassification {
		message = "local workspace for " + issueLabel(running.Issue) + " was retained after a " + origin + " lane change from " + pending.fromState + " to " + pending.toState
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   outcome.activityEvent,
		Message: message,
	})
	if !outcome.comment || o.connector == nil {
		return
	}
	body := laneRevocationOutcomeComment(pending, event, running, tokens, outcome)
	if err := o.connector.CreateComment(context.WithoutCancel(ctx), running.Issue.ID, body); err != nil {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "worker_lane_outcome_notice_failed",
			Message: "failed to report lane-revocation outcome for " + issueLabel(running.Issue) + ": " + err.Error(),
		})
		if o.logger != nil {
			o.logger.Warn("lane revocation outcome comment failed", "issue_id", running.Issue.ID, "identifier", running.Issue.Identifier, "error", err)
		}
	}
}

func laneRevocationOutcomeComment(
	pending *pendingLaneRevocation,
	event runpkg.Completion,
	running Running,
	tokens TokenTotals,
	outcome laneRevocationOutcome,
) string {
	var b strings.Builder
	b.WriteString("Detent stopped this worker after a verified ")
	b.WriteString(string(provenance.NormalizeOrigin(pending.origin)))
	b.WriteString(" lane change from ")
	b.WriteString(pending.fromState)
	b.WriteString(" to ")
	b.WriteString(pending.toState)
	b.WriteString(". ")
	if outcome.terminalState == store.WorkAttemptTerminalDelivered {
		b.WriteString("Your work was pushed but finalization was rejected; the pushed work remains available.")
		b.WriteString("\n\n- reason: worker_lane_revocation_delivery_preserved")
	} else if outcome.classification == laneRevocationPreservedClassification {
		b.WriteString("The local workspace is retained for recovery, including unpushed commits and uncommitted files. Finalization was rejected; the work has not been delivered.")
		b.WriteString("\n\n- reason: worker_lane_revocation_workspace_preserved")
	} else {
		b.WriteString("Workspace preservation could not be verified. The session's output is not evidence that local work was discarded.")
		b.WriteString("\n\n- reason: worker_lane_revocation_preservation_unverified")
	}
	b.WriteString("\n- classification: ")
	b.WriteString(outcome.classification)
	b.WriteString("\n- revocation_reason: ")
	b.WriteString(pending.reason)
	b.WriteString("\n- lane_change_origin: ")
	b.WriteString(string(provenance.NormalizeOrigin(pending.origin)))
	b.WriteString("\n- lane_change_initiator: ")
	b.WriteString(string(pending.attribution.Initiator))
	b.WriteString("\n- lane_change_basis: ")
	b.WriteString(string(pending.attribution.Basis))
	if pending.attribution.Actor != nil {
		b.WriteString("\n- lane_change_actor: ")
		b.WriteString(pending.attribution.Actor.Login)
	}
	if pending.mutation.ID > 0 {
		b.WriteString("\n- lane_mutation_receipt_id: ")
		b.WriteString(strconv.FormatInt(pending.mutation.ID, 10))
		b.WriteString("\n- lane_mutation_disposition: ")
		b.WriteString(string(pending.mutation.Disposition))
	}
	if pending.preservation != nil {
		b.WriteString("\n- workspace_path: ")
		b.WriteString(pending.preservation.Path)
		b.WriteString("\n- workspace_head: ")
		b.WriteString(pending.preservation.HeadSHA)
	}
	if pending.preservationErr != nil {
		b.WriteString("\n- preservation_error: ")
		b.WriteString(pending.preservationErr.Error())
	}
	b.WriteString("\n- from_state: ")
	b.WriteString(pending.fromState)
	b.WriteString("\n- to_state: ")
	b.WriteString(pending.toState)
	b.WriteString("\n- attempt: ")
	b.WriteString(strconv.Itoa(running.Attempt))
	b.WriteString("\n- output_tokens: ")
	b.WriteString(strconv.FormatInt(tokens.OutputTokens, 10))
	b.WriteString("\n- total_tokens: ")
	b.WriteString(strconv.FormatInt(tokens.TotalTokens, 10))
	b.WriteString("\n- runtime_seconds: ")
	b.WriteString(strconv.FormatFloat(tokens.RuntimeSeconds, 'f', -1, 64))
	b.WriteString("\n- files_changed: ")
	b.WriteString(strconv.Itoa(running.DiffStats.FilesChanged))
	if finalState := strings.TrimSpace(event.Result.FinalState); finalState != "" {
		b.WriteString("\n- final_state: ")
		b.WriteString(finalState)
	}
	return b.String()
}

func (o *Orchestrator) reapRunningWorker(
	ctx context.Context,
	running Running,
	identity procgroup.Identity,
	reason string,
) (procgroup.TerminationOutcome, procgroup.Identity, error) {
	found := identity.PID > 0
	if !found {
		var err error
		identity, found, err = o.persistedWorkerProcess(ctx, running)
		if err != nil {
			return "", procgroup.Identity{}, fmt.Errorf("load persisted identity: %w", err)
		}
	}
	if !found {
		return procgroup.TerminationOutcomeAlreadyExited, procgroup.Identity{}, nil
	}
	reap := o.reapWorkerProcess
	if reap == nil {
		reap = procgroup.Terminate
	}
	outcome, err := reap(context.WithoutCancel(ctx), identity, o.workerReapGrace)
	if err != nil {
		return "", identity, err
	}
	if outcome == procgroup.TerminationOutcomeStaleIdentity {
		return outcome, identity, nil
	}
	if o.workerProcesses != nil && running.DetentSessionID > 0 {
		if err := o.workerProcesses.MarkSessionWorkerProcessReaped(context.WithoutCancel(ctx), running.DetentSessionID, store.WorkerProcessReap{
			ReapedAt: o.clockNow().UTC(),
			Outcome:  string(outcome),
			Reason:   strings.TrimSpace(reason),
		}); err != nil {
			return outcome, identity, fmt.Errorf("persist reap outcome: %w", err)
		}
	}
	return outcome, identity, nil
}

func completionMatchesRunning(event runpkg.Completion, running Running) bool {
	if event.Request.Generation > 0 && running.Generation > 0 && event.Request.Generation != running.Generation {
		return false
	}
	if event.Request.WorkAttemptID > 0 && running.WorkAttemptID > 0 && event.Request.WorkAttemptID != running.WorkAttemptID {
		return false
	}
	return true
}

func (o *Orchestrator) refreshCompletionLane(ctx context.Context, running Running) (connector.Issue, error) {
	if o == nil || o.connector == nil {
		return cloneIssue(running.Issue), nil
	}
	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{running.Issue.ID})
	if err != nil {
		return connector.Issue{}, err
	}
	for _, issue := range issues {
		if strings.TrimSpace(issue.ID) == strings.TrimSpace(running.Issue.ID) {
			if strings.TrimSpace(issue.State) == "" {
				return connector.Issue{}, fmt.Errorf("issue %s has no lane in completion fence", issueLabel(running.Issue))
			}
			return mergeIssueTrackerFields(running.Issue, issue), nil
		}
	}
	return connector.Issue{}, fmt.Errorf("issue %s was not returned by completion fence", issueLabel(running.Issue))
}

func (o *Orchestrator) rejectWorkerCompletion(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	reason string,
	err error,
) {
	at := event.CompletedAt.UTC()
	if at.IsZero() {
		at = o.clockNow().UTC()
	}
	message := "rejected stale completion for " + issueLabel(event.Request.Issue) + ": " + reason
	if strings.TrimSpace(event.Request.Issue.ID) == "" {
		message = "rejected stale completion for " + issueLabel(running.Issue) + ": " + reason
	}
	if err != nil {
		message += ": " + err.Error()
	}
	recordStateEvent(state, telemetry.ActivityEvent{At: at, Event: "stale_worker_completion_rejected", Message: message})
	if o.logger != nil {
		o.logger.Warn(
			"stale worker completion rejected",
			"project_id", strings.TrimSpace(o.cfg.Project.ID),
			"issue_id", event.IssueID,
			"generation", event.Request.Generation,
			"current_generation", running.Generation,
			"work_attempt_id", event.Request.WorkAttemptID,
			"current_work_attempt_id", running.WorkAttemptID,
			"reason", reason,
			"error", err,
		)
	}
	if o.workflowMetrics != nil {
		_, recordErr := o.workflowMetrics.RecordWorkflowPhaseEvent(context.WithoutCancel(ctx), store.WorkflowPhaseEvent{
			ProjectID:  strings.TrimSpace(o.cfg.Project.ID),
			SessionID:  running.DetentSessionID,
			IssueID:    event.IssueID,
			Identifier: running.Issue.Identifier,
			PhaseType:  store.WorkflowPhaseTypeRecovery,
			PhaseName:  "stale_completion_rejected",
			Reason:     reason,
			Status:     "rejected",
			StartedAt:  at,
			FinishedAt: at,
			MetadataJSON: marshalWorkAttemptJSON(map[string]any{
				"generation":              event.Request.Generation,
				"current_generation":      running.Generation,
				"work_attempt_id":         event.Request.WorkAttemptID,
				"current_work_attempt_id": running.WorkAttemptID,
				"error":                   errorString(err),
			}),
		})
		if recordErr != nil && o.logger != nil {
			o.logger.Warn("stale completion audit persistence failed", "issue_id", event.IssueID, "error", recordErr)
		}
	}
}
