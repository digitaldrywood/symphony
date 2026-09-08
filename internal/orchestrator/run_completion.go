package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/budget"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/provenance"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const (
	mergeWorkerRequiredChecksMissingReason = "required_checks_missing_after_head_update"
	mergeWorkerFastPathNotReadyReason      = "merge_fast_path_head_not_ready"
	retryWaitCurrentHeadCI                 = "current_head_ci"
)

func (o *Orchestrator) handleRunUpdate(state *State, event runUpdate) {
	running, ok := state.Running[event.issueID]
	if !ok {
		return
	}
	if event.progress != nil && event.progress != running.progress {
		return
	}
	running = applyRunningUsage(running, event.usage).withProgress()
	state.Running[event.issueID] = running
	o.trackRunningHeartbeat(state, running, state.Claimed[event.issueID], o.clockNow())
	if event.usage.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.usage.RateLimits)
		o.recoverWorkerGitHubMonitorFromUpdate(state, running, event.usage.RateLimits, event.usage.LastEventAt)
		o.recoverBackendCapacityFromStatus(state, running, event.usage.RateLimits, event.usage.LastEventAt)
	}
	if strings.TrimSpace(event.usage.SessionID) != "" || event.usage.TurnCount > 0 {
		progressedAt := event.usage.LastEventAt
		if progressedAt.IsZero() {
			progressedAt = o.clockNow()
		}
		o.recordProjectFailureBreakerProgress(state, event.issueID, progressedAt)
		o.advanceDispatchRecovery(state, event.issueID, progressedAt)
	}
	if event.progress == nil && o.workAttempts != nil && running.WorkAttemptID > 0 {
		now := event.usage.LastEventAt
		if now.IsZero() {
			now = time.Now()
		}
		heartbeat := o.runningWorkAttemptHeartbeat(state, running, now)
		if err := o.workAttempts.RecordWorkAttemptHeartbeat(context.Background(), heartbeat); err != nil {
			if event.usage.DispatchLoopStart != nil {
				running.DispatchLoopStart.Persisted = false
				state.Running[event.issueID] = running
			}
			if o.logger != nil {
				o.logger.Warn("work attempt usage heartbeat failed", "attempt_id", running.WorkAttemptID, "issue_id", event.issueID, "error", err)
			}
		} else {
			o.applyWorkAttemptHeartbeatSnapshot(state, running.WorkAttemptID, heartbeat, event.usage.LastMessageTruncation)
		}
	}
}

func (o *Orchestrator) handleRunResult(ctx context.Context, state *State, event runpkg.Completion) {
	running, ok := state.Running[event.IssueID]
	if !ok {
		if event.Request.Generation > 0 || event.Request.WorkAttemptID > 0 {
			o.rejectWorkerCompletion(ctx, state, event, Running{}, "worker lease is no longer active", nil)
		}
		return
	}
	running = running.withProgress()
	state.Running[event.IssueID] = running
	if !completionMatchesRunning(event, running) {
		o.rejectWorkerCompletion(ctx, state, event, running, "worker generation or work-attempt lease no longer owns the item", nil)
		return
	}
	if o.handleLaneRevocationCompletion(ctx, state, event, running) {
		return
	}
	if running.Generation > 0 {
		beforeRefresh := running
		refreshed, err := o.refreshCompletionLane(ctx, running)
		if err != nil {
			if !errors.Is(event.Err, runpkg.ErrWorkerGitHubBudgetMonitor) && !errors.Is(event.Err, runpkg.ErrWorkerGitHubTokenResolution) {
				o.deferTrackerUnavailableCompletion(ctx, state, event, running, err)
				return
			}
			if o.logger != nil {
				o.logger.Warn("worker GitHub monitor completion lane refresh unavailable; preserving monitor deferral", "issue_id", event.IssueID, "error", err)
			}
		} else {
			receiptAccepted := false
			receipt, receiptFound, receiptErr := o.laneMutationReceipt(ctx, running, refreshed)
			if receiptErr != nil {
				o.deferTrackerUnavailableCompletion(ctx, state, event, running, fmt.Errorf("lane mutation receipt is unavailable: %w", receiptErr))
				return
			}
			if receiptFound {
				if receipt.Disposition == laneMutationRevokeWorker {
					if err := laneRevocationTransitionError(receipt.FromState, refreshed.State); err != nil {
						o.deferTrackerUnavailableCompletion(ctx, state, event, beforeRefresh, err)
						return
					}
				}
				running.laneMutation = receipt
				consumed, consumeErr := o.consumeLaneMutationReceipt(ctx, receipt, running, refreshed.State, event.CompletedAt)
				if consumeErr != nil {
					o.deferTrackerUnavailableCompletion(ctx, state, event, running, fmt.Errorf("lane mutation receipt could not be consumed: %w", consumeErr))
					return
				}
				receipt = consumed
				running.laneMutation = store.LaneMutationReceipt{}
				switch receipt.Disposition {
				case laneMutationPreserveOwnership:
					receiptAccepted = true
					if !stateIn(refreshed.State, o.cfg.ActiveStates) || workspaceIssueTerminal(refreshed, o.cfg.TerminalStates) {
						running.CompletionLane = strings.TrimSpace(refreshed.State)
						running.CompletionAcceptedAt = event.CompletedAt.UTC()
					}
				case laneMutationAcceptCompletion:
					receiptAccepted = true
					running.CompletionLane = strings.TrimSpace(refreshed.State)
					running.CompletionAcceptedAt = event.CompletedAt.UTC()
				case laneMutationRevokeWorker:
					o.beginLaneRevocationForMutation(ctx, state, beforeRefresh, refreshed, event.CompletedAt, receipt)
					o.handleLaneRevocationCompletion(ctx, state, event, beforeRefresh)
					return
				}
			}
			running.Issue = refreshed
			state.Running[event.IssueID] = running
			if claimed, found := state.Claimed[event.IssueID]; found {
				claimed.Issue = cloneIssue(refreshed)
				state.Claimed[event.IssueID] = claimed
			}
			if !receiptAccepted && (!stateIn(refreshed.State, o.cfg.ActiveStates) || workspaceIssueTerminal(refreshed, o.cfg.TerminalStates)) {
				if accepted, ok := o.acceptCurrentAttemptCompletionLane(ctx, state, running, refreshed, event.CompletedAt); ok {
					running = accepted
				} else {
					if err := laneRevocationTransitionError(beforeRefresh.Issue.State, refreshed.State); err != nil {
						o.deferTrackerUnavailableCompletion(ctx, state, event, beforeRefresh, err)
						return
					}
					o.rejectWorkerCompletion(ctx, state, event, running, "current tracker lane is not worker-owned", nil)
					o.beginLaneRevocation(ctx, state, beforeRefresh, refreshed, event.CompletedAt, laneRevocationStateChanged)
					o.handleLaneRevocationCompletion(ctx, state, event, beforeRefresh)
					return
				}
			}
		}
	}
	o.heartbeats.remove(event.IssueID)
	if o.retrospector != nil {
		defer o.retrospector.Trigger("completion")
	}
	if o.handleOperatorStopCompletion(ctx, state, event, running) {
		return
	}
	if !running.CompletionOwnershipReleased {
		o.releaseGlobalDispatchSlot(running.globalSlot)
		o.logWorkerLifecycle(running.Issue, "worker_capacity_released",
			telemetry.WorkAttemptIDKey, running.WorkAttemptID,
			telemetry.DetentSessionIDKey, running.DetentSessionID,
			telemetry.ProviderSessionIDKey, running.SessionID,
			"attempt", running.Attempt,
			"worker_host", strings.TrimSpace(running.WorkerHost),
			"reason", "run_completed",
		)
		if running.cancel != nil {
			running.cancel()
		}
	}
	running.globalSlot = scheduler.Slot{}
	running.CompletionOwnershipReleased = true
	if !event.Result.RuntimeIdentity.IsZero() {
		running.RuntimeIdentity = running.RuntimeIdentity.Merge(event.Result.RuntimeIdentity)
	}
	running.WorkProductPushed = running.WorkProductPushed || event.Result.PullRequestHeadPushed || event.Result.PullRequestUpdated
	running.ArtifactEvidence = event.Result.ArtifactEvidence
	if event.Result.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	}
	delete(state.Running, event.IssueID)
	if o.handleWorkerGitHubTokenResolutionCompletion(ctx, state, event, running) {
		return
	}
	if o.handleGitHubMonitorCompletion(ctx, state, event, running) {
		return
	}
	o.refreshEfficiencyReceipt(ctx, running.Issue, event.CompletedAt)
	if o.handleModelPermitDeferred(ctx, state, event, running) {
		return
	}
	if o.handleWorkspaceBranchHoldCompletion(ctx, state, event, running) {
		return
	}
	if o.handleForgeUnavailableCompletion(ctx, state, event, running) {
		return
	}
	o.finishForgeAvailabilityProbe(state, event, running)
	deliverableLookup := deliverableRecoveryLookupResult{}
	var deliverableRecoveryErr *runpkg.DeliverableRecoveryError
	if errors.As(event.Err, &deliverableRecoveryErr) && deliverableRecoveryErr != nil {
		if diffStatsPresent(event.Result.DiffStats) {
			running.DiffStats = event.Result.DiffStats
		}
		deliverableLookup = o.lookupDeliverableRecovery(ctx, running, deliverableRecoveryErr)
		if deliverableLookup.reconciles() {
			running.Issue = deliverableRecoveryIssue(running, deliverableLookup)
			event.Err = nil
			event.Result.FinalState = FinalStateCompleted
			event.Result.PullRequestUpdated = true
			recordStateEvent(state, telemetry.ActivityEvent{
				At:      event.CompletedAt,
				Event:   "deliverable_recovery_reconciled",
				Message: deliverableLookup.LookupResult + " for " + issueLabel(running.Issue),
			})
			telemetry.LogLifecycle(o.logger, slog.LevelInfo, telemetry.LifecycleWorkAttempt, "deliverable_recovery_reconciled", o.runningLifecycleCorrelation(running.Issue, running),
				"workspace_branch", deliverableLookup.Branch,
				"workspace_head_sha", deliverableLookup.HeadSHA,
				"hydration_state", deliverableLookup.HydrationState,
				"lookup_result", deliverableLookup.LookupResult,
				"pull_request_number", deliverableLookup.PullRequest.Number,
			)
		}
	}
	if event.Err != nil {
		o.releaseTerminalAttemptClaim(ctx, state, running.Issue, event.CompletedAt)
	}
	if event.Err == nil || event.Result.TurnStarted {
		o.recordProjectFailureBreakerProgress(state, event.IssueID, event.CompletedAt)
		o.advanceDispatchRecovery(state, event.IssueID, event.CompletedAt)
	} else if !isGitHubRESTBudgetHeadroomError(event.Err) {
		delay := event.RetryDelay
		if delay <= 0 {
			delay = o.cfg.OverloadRetryDelay
		}
		o.backoffDispatchRecovery(state, event.IssueID, event.CompletedAt, delay)
	}
	releaseDispatchRecoveryAdmission(state, event.IssueID)
	if o.handleCIUnavailableCompletion(ctx, state, event, running) {
		return
	}
	if o.handleTrackerUnavailableCompletion(ctx, state, event, running) {
		return
	}
	if o.handleMergeRevocationCompletion(ctx, state, event, running) {
		return
	}
	if o.handleMergeWorkerStartupTimeout(ctx, state, event, running) {
		return
	}
	if o.handleMergeWorkerDurationExceeded(ctx, state, event, running) {
		return
	}
	if o.handleMergeFallbackBudgetExceeded(ctx, state, event, running) {
		return
	}
	if capacityErr, ok := backendcapacity.As(event.Err); ok {
		if capacityErr.Details.Type == backendcapacity.ErrorTypeTransientOverload {
			o.handleTransientOverload(ctx, state, event, running, capacityErr)
			return
		}
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalCapacity, event.Err, "backend_capacity", errorString(event.Err))
		o.handleBackendCapacityError(ctx, state, event, running, capacityErr)
		return
	}
	if o.handleSessionBrake(ctx, state, event, running) {
		return
	}
	if o.handleGitHubRESTCapacityCompletion(ctx, state, event, running) {
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalCapacity, event.Err, githubRESTCapacityError, errorString(event.Err))
		return
	}
	if event.Err == nil || event.Result.TurnStarted {
		o.recoverBackendCapacity(state, running, event.CompletedAt)
	} else {
		o.deferBackendCapacityProbe(state, running, event.CompletedAt, event.Err)
	}

	if !deliverableLookup.reconciles() && workspaceIssueTerminal(running.Issue, o.cfg.TerminalStates) {
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
		tokens := event.Result.Tokens
		if tokens == (TokenTotals{}) {
			tokens = running.Tokens
		}
		if diffStatsPresent(event.Result.DiffStats) {
			running.DiffStats = event.Result.DiffStats
		}
		o.logWorkerLifecycle(running.Issue, "worker_"+workerOutcome(event.Err, event.Result.FinalState),
			telemetry.WorkAttemptIDKey, running.WorkAttemptID,
			telemetry.DetentSessionIDKey, running.DetentSessionID,
			telemetry.ProviderSessionIDKey, running.SessionID,
			"attempt", running.Attempt,
			"worker_host", strings.TrimSpace(running.WorkerHost),
			"final_state", strings.TrimSpace(running.Issue.State),
		)
		o.completeTerminalRunning(context.Background(), state, event.IssueID, running, terminalCompletedAt(running.Issue, o.cfg.TerminalStates, event.CompletedAt), tokens)
		return
	}
	if errors.Is(event.Err, runpkg.ErrWorkspacePreparation) {
		o.handleWorkspacePreparationFailure(ctx, state, event, running)
		return
	}

	if event.Err != nil {
		o.logWorkerLifecycle(running.Issue, "worker_"+workerOutcome(event.Err, event.Result.FinalState),
			telemetry.WorkAttemptIDKey, running.WorkAttemptID,
			telemetry.DetentSessionIDKey, running.DetentSessionID,
			telemetry.ProviderSessionIDKey, running.SessionID,
			"attempt", running.Attempt,
			"worker_host", strings.TrimSpace(running.WorkerHost),
			"retry_attempt", event.RetryAttempt,
			"retry_delay_seconds", int64(event.RetryDelay/time.Second),
			"error", event.Err,
		)
		pushEvidenceRefreshed := false
		if event.Result.PullRequestHeadPushed && !event.Result.CITriggerLabelReapplied {
			var warning string
			running.Issue, warning = o.refreshSpendProgressIssue(ctx, running.Issue)
			pushEvidenceRefreshed = warning == ""
			o.scheduleCITriggerLabel(ctx, running.Issue, gate.Effective(o.cfg.AutoPromote.Gate).RequiredStatusChecks, running.Attempt, true, warning != "")
		}
		progress := implementCompletionProgressDecision{}
		progressMetadata := map[string]any{}
		if !mergeWorkerIssue(running.Issue) && strings.TrimSpace(event.Request.Mode) != runpkg.RunModePlan && strings.TrimSpace(running.Mode) != runpkg.RunModePlan {
			if diffStatsPresent(event.Result.DiffStats) {
				running.DiffStats = event.Result.DiffStats
			}
			progress = o.evaluateImplementCompletionProgress(ctx, running, event.Result.FinalState, event.Result.PullRequestUpdated)
			progress = o.evaluateDispatchLoopProgress(ctx, running, progress)
			running.Issue = progress.Issue
			progressMetadata = implementCompletionProgressMetadata(progress)
		}
		spendProgress := spendProgressDecision{}
		if !mergeWorkerIssue(running.Issue) {
			evidenceWarning := ""
			if !pushEvidenceRefreshed && o.spendProgressEnabled() {
				running.Issue, evidenceWarning = o.refreshSpendProgressIssue(ctx, running.Issue)
			}
			spendProgress = o.evaluateSpendProgress(ctx, running, event.CompletedAt, false, "")
			if evidenceWarning != "" {
				spendProgress.Warning = evidenceWarning
				spendProgress.Block = false
			}
		}
		terminalState := terminalStateForRun(event.Err, event.Result.FinalState)
		errorClass := runnerWorkAttemptErrorClass(event.Err)
		errorMessage := event.Err.Error()
		phase := "failed"
		statusMessage := "worker failed"
		deliverableRecoveryErr = nil
		credentialFailure := runpkg.IsDeliverableConfigurationError(event.Err)
		var projectionErr *runpkg.SessionBudgetProjectionError
		projectionFailure := errors.As(event.Err, &projectionErr) && projectionErr != nil
		if errors.As(event.Err, &deliverableRecoveryErr) && deliverableRecoveryErr != nil {
			errorClass = deliverableRecoveryReasonCode(deliverableLookup)
			phase = "blocked"
			statusMessage = "branch " + deliverableRecoveryBranch(deliverableRecoveryErr, running) + " needs delivery recovery"
		} else if credentialFailure {
			errorClass = deliverableConfigurationFailureCause
			phase = "blocked"
			statusMessage = "deliverable credentials require human configuration"
		} else if projectionFailure {
			errorClass = budgetProjectionCeilingFailureCause
			phase = "blocked"
			statusMessage = "session stopped at its projected budget ceiling"
		}
		if progress.Block && progress.BlockReason == dispatchLoopDetectedReason && deliverableRecoveryErr == nil && !credentialFailure && !projectionFailure && !errors.Is(event.Err, runpkg.ErrSessionTokenCeilingExceeded) {
			terminalState = store.WorkAttemptTerminalNoProgress
			errorClass = dispatchLoopDetectedReason
			errorMessage = dispatchLoopBlockMessage(progress)
			phase = "no_progress"
			statusMessage = "dispatch loop circuit breaker tripped"
		} else if spendProgress.Block && deliverableRecoveryErr == nil && !credentialFailure && !projectionFailure && !errors.Is(event.Err, runpkg.ErrSessionTokenCeilingExceeded) {
			terminalState = store.WorkAttemptTerminalNoProgress
			errorClass = spendProgressReason
			errorMessage = spendProgressBlockMessage(spendProgress)
			phase = "no_progress"
			statusMessage = "no-progress circuit breaker tripped"
		}
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, terminalState, event.Err, errorClass, errorMessage)
		attemptCompleted := o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, terminalState, errorClass, errorMessage, phase, statusMessage, mergeWorkAttemptMetadata(
			progressMetadata,
			spendProgressMetadata(spendProgress),
			deliverableCommandEvidenceMetadata(event.Result),
		))
		attempt := event.RetryAttempt
		if attempt < 1 {
			attempt = nextAttempt(running.Attempt)
		}
		if o.blockDeliverableRecoveryFailure(ctx, state, event, running, deliverableLookup) {
			return
		}
		if credentialFailure && o.blockHumanOwnedWorkerFailure(
			ctx,
			state,
			event,
			running,
			deliverableConfigurationFailureCause,
			"GitHub credentials are unavailable for the deliverable command",
			"configure GitHub CLI authentication or GH_TOKEN, then move the issue to Rework",
			"worker_deliverable_configuration_blocked",
		) {
			return
		}
		if projectionFailure && o.blockHumanOwnedWorkerFailure(
			ctx,
			state,
			event,
			running,
			budgetProjectionCeilingFailureCause,
			fmt.Sprintf("session cost %.6f USD exceeded the admitted projection %.6f USD using estimate source %q", projectionErr.ObservedCostUSD, projectionErr.ProjectedCostUSD, projectionErr.EstimateSource),
			"inspect the preserved worktree and either narrow the task or adjust the budget policy before moving the issue to Rework",
			"worker_budget_projection_ceiling_tripped",
			"estimate_source", projectionErr.EstimateSource,
		) {
			return
		}
		if o.tripTokenCeilingCircuitBreaker(ctx, state, event, running, attempt) {
			return
		}
		if progress.Block && progress.BlockReason == dispatchLoopDetectedReason && o.blockImplementProgress(ctx, state, running, progress, event.CompletedAt) {
			return
		}
		if spendProgress.Block && o.blockSpendProgress(ctx, state, running, spendProgress, event.CompletedAt) {
			return
		}
		if mergeWorkerIssue(running.Issue) {
			o.logMergeWorkerFailure(running.Issue, "runner_failed", event.Err)
			o.recordMergeFailed(state, running.Issue, event.CompletedAt, "runner_failed", event.Err)
		}
		if mergeWorkerIssue(running.Issue) && attempt > maxMergeWorkerRunnerFailures {
			if o.blockExhaustedMergeWorker(ctx, state, running, event.CompletedAt, mergeWorkerRetryExhaustedReason, attempt, event.Err) {
				return
			}
		}
		if o.tripInstantFailureCircuitBreaker(ctx, state, event, running, attempt) {
			return
		}
		if o.tripRepeatedFailureCircuitBreaker(ctx, state, event, running, attempt) {
			return
		}
		if terminalAttemptStateRetryDemotable(terminalState) {
			var parked bool
			running.Issue, _, parked = o.demoteTerminalAttemptRetry(
				ctx,
				state,
				running.Issue,
				running.WorkProductPushed,
				terminalAttemptRetryLimitCause,
				attemptCompleted,
				running.Mode,
				running.DiffStats,
				event.CompletedAt,
				terminalAttemptFailureEvidence(running, terminalState, errorClass, errorMessage, event.CompletedAt),
			)
			if parked {
				return
			}
		}
		delay := event.RetryDelay
		if delay <= 0 {
			delay = o.retryDelay(attempt, false)
		}
		o.scheduleRetryAfter(
			state,
			running.Issue,
			attempt,
			event.CompletedAt,
			delay,
			event.Err.Error(),
			running.WorkerHost,
		)
		return
	}

	if mergeWorkerIssue(running.Issue) {
		resetWorkerFailureBreakers(state, event.IssueID)
	}

	if event.Request.Mode == runpkg.RunModePlan {
		o.logWorkerLifecycle(running.Issue, "worker_"+workerOutcome(event.Err, event.Result.FinalState),
			telemetry.WorkAttemptIDKey, running.WorkAttemptID,
			telemetry.DetentSessionIDKey, running.DetentSessionID,
			telemetry.ProviderSessionIDKey, running.SessionID,
			"attempt", running.Attempt,
			"worker_host", strings.TrimSpace(running.WorkerHost),
			"mode", strings.TrimSpace(event.Request.Mode),
			"final_state", strings.TrimSpace(event.Result.FinalState),
		)
		o.completePlanRunning(ctx, state, event, running)
		return
	}

	var dependencyProgress implementCompletionProgressDecision
	if mergeWorkerIssue(running.Issue) && mergeWorkerTurnSucceeded(event) && !state.Draining {
		if diffStatsPresent(event.Result.DiffStats) {
			running.DiffStats = event.Result.DiffStats
		}
		dependencyProgress = o.evaluateCompletedDependencyDeferral(ctx, running)
	}
	if mergeWorkerIssue(running.Issue) && !dependencyProgress.DependencyDeferral {
		if o.completeLatestTerminalMergeWorkerResult(ctx, state, event, running) {
			return
		}
		if state.Draining {
			releaseProjectFailureBreakerCanary(state, event.IssueID)
			o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalCancelled, "draining", "worker stopped during drain", "cancelled", "worker stopped during drain")
			o.cleanupDrainedRun(ctx, state, event.IssueID)
			return
		}
		o.handleIncompleteMergeWorkerResult(ctx, state, event, running)
		return
	}
	if o.completeRedundantGateWaitRun(ctx, state, event, running) {
		releaseProjectFailureBreakerCanary(state, event.IssueID)
		return
	}

	finalState := event.Result.FinalState
	if finalState == "" {
		finalState = FinalStateCompleted
	}
	o.logWorkerLifecycle(running.Issue, "worker_"+workerOutcome(nil, finalState),
		telemetry.WorkAttemptIDKey, running.WorkAttemptID,
		telemetry.DetentSessionIDKey, running.DetentSessionID,
		telemetry.ProviderSessionIDKey, running.SessionID,
		"attempt", running.Attempt,
		"worker_host", strings.TrimSpace(running.WorkerHost),
		"mode", strings.TrimSpace(event.Request.Mode),
		"final_state", strings.TrimSpace(finalState),
	)
	terminalState := terminalStateForRun(nil, finalState)
	errorClass := ""
	errorMessage := ""
	if terminalState == store.WorkAttemptTerminalFailure {
		errorClass = "runner_final_state"
		errorMessage = finalState
	}
	if diffStatsPresent(event.Result.DiffStats) {
		running.DiffStats = event.Result.DiffStats
	}
	dispatchedIssue := cloneIssue(running.Issue)
	if terminalState == store.WorkAttemptTerminalSuccess {
		running.Issue = o.applyArtifactGateCompletionFields(ctx, running.Issue, running.DispatchWorkpadHash, running.DispatchWorkpadRead)
	}
	var progress implementCompletionProgressDecision
	if dependencyProgress.DependencyDeferral {
		progress = dependencyProgress
	} else {
		progress = o.evaluateImplementCompletionProgress(ctx, running, finalState, event.Result.PullRequestUpdated)
	}
	progress = o.evaluateDispatchLoopProgress(ctx, running, progress)
	progress, gateWaitReason := completedReworkGateWaitProgress(running, progress, o.cfg, finalState)
	running.Issue = progress.Issue
	completionEvidence := progress.WorkspaceDiffStats
	if !diffStatsPresent(completionEvidence) {
		completionEvidence = running.DiffStats
	}
	completionCleanliness := o.evaluateCompletionCleanliness(ctx, running, running.Issue, completionEvidence)
	completionRejected := completionCleanliness.Attempted && completionCleanliness.Outcome != completionCleanlinessAccepted
	if terminalState == store.WorkAttemptTerminalSuccess && !completionRejected && implementCompletionHasDurableProgress(running, progress) {
		resetWorkerFailureBreakers(state, event.IssueID)
	}
	if event.Result.PullRequestHeadPushed && !event.Result.CITriggerLabelReapplied {
		forceReapply := false
		if pullRequestRepository(running.Issue) == "" || pullRequestNumber(running.Issue) <= 0 || running.Issue.PullRequest == nil || strings.TrimSpace(progress.CurrentSignature.HeadSHA) == "" {
			refreshed, warning := o.refreshSpendProgressIssue(ctx, running.Issue)
			running.Issue = refreshed
			progress.Issue = refreshed
			forceReapply = warning != ""
			if warning != "" && o.logger != nil {
				o.logger.Warn("worker push pull request refresh failed", "issue_id", running.Issue.ID, "identifier", running.Issue.Identifier, "warning", warning)
			}
		}
		o.scheduleCITriggerLabel(ctx, running.Issue, gate.Effective(o.cfg.AutoPromote.Gate).RequiredStatusChecks, running.Attempt, true, forceReapply)
	}
	o.commentObservedLaneTransition(ctx, dispatchedIssue, running.Issue, event.CompletedAt)
	evidenceWarning := ""
	if o.spendProgressEnabled() && (o.spendProgressDeliverableKind(running) == workflowconfig.DeliverableArtifact || !implementProgressLinkedPullRequest(running.Issue)) {
		running.Issue, evidenceWarning = o.refreshSpendProgressIssue(ctx, running.Issue)
	}
	accepted, acceptedReason := implementAcceptedStateChange(running, progress)
	if completionRejected {
		accepted = false
		acceptedReason = ""
	}
	spendProgress := o.evaluateSpendProgress(ctx, running, event.CompletedAt, accepted, acceptedReason)
	if evidenceWarning != "" {
		spendProgress.Warning = evidenceWarning
		spendProgress.Block = false
	}
	if progress.Warning != "" && strings.HasPrefix(progress.Reason, "pull_request_hydrat") {
		spendProgress.Warning = progress.Warning
		spendProgress.Block = false
	}
	if terminalState != store.WorkAttemptTerminalSuccess {
		progress.Outcome = terminalState
	}
	phase := "completed"
	statusMessage := "worker completed"
	artifactConvergence := artifactGateConvergenceRecord{}
	if terminalState == store.WorkAttemptTerminalSuccess {
		artifactConvergence = o.evaluateArtifactGateConvergence(ctx, dispatchedIssue, running.Issue, running)
	}
	if terminalState == store.WorkAttemptTerminalSuccess && progress.Outcome == store.WorkAttemptTerminalNoProgress {
		terminalState = store.WorkAttemptTerminalNoProgress
		phase = "no_progress"
		statusMessage = "worker completed without PR progress"
		if progress.BlockReason == dispatchLoopDetectedReason {
			errorClass = dispatchLoopDetectedReason
			errorMessage = dispatchLoopBlockMessage(progress)
			statusMessage = "dispatch loop circuit breaker tripped"
		}
	}
	if spendProgress.Block {
		terminalState = store.WorkAttemptTerminalNoProgress
		progress.Outcome = store.WorkAttemptTerminalNoProgress
		errorClass = spendProgressReason
		errorMessage = spendProgressBlockMessage(spendProgress)
		phase = "no_progress"
		statusMessage = "no-progress circuit breaker tripped"
	}
	if artifactConvergence.Tripped {
		phase = "blocked"
		statusMessage = "artifact gate convergence breaker tripped"
	}
	if completionRejected {
		phase = "completion_rejected"
		statusMessage = "completion rejected because workspace is not clean"
	}
	if completionCleanliness.Block {
		terminalState = store.WorkAttemptTerminalNoProgress
		phase = "blocked"
		statusMessage = "dirty completion requires human resolution"
		errorClass = dirtyCompletionEscalationReason
		errorMessage = dirtyCompletionEscalationReason
	}
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, terminalState, nil, errorClass, errorMessage)
	attemptCompleted := o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, terminalState, errorClass, errorMessage, phase, statusMessage, mergeWorkAttemptMetadata(
		implementCompletionProgressMetadata(progress),
		completionCleanlinessMetadata(completionCleanliness),
		spendProgressMetadata(spendProgress),
		artifactGateConvergenceMetadata(artifactConvergence),
		deliverableCommandEvidenceMetadata(event.Result),
		completionGateWaitMetadata(gateWaitReason, progress.Issue),
	))

	state.Completed[event.IssueID] = Completed{
		Issue:            cloneIssue(running.Issue),
		SessionID:        running.SessionID,
		StartedAt:        running.StartedAt,
		CompletedAt:      event.CompletedAt,
		FinalState:       finalState,
		CompletionKind:   strings.TrimSpace(progress.CompletionKind),
		GateWaitReason:   gateWaitReason,
		gateWaitEvidence: completionGateWaitEvidence(gateWaitReason, progress.Issue),
		Tokens:           event.Result.Tokens,
		RuntimeIdentity:  running.RuntimeIdentity,
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, event.Result.Tokens)
	if event.Result.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	}
	if diffStatsPresent(event.Result.DiffStats) {
		state.DiffStats[event.IssueID] = event.Result.DiffStats
	}
	if event.Result.BudgetRefusal != nil && !o.cfg.subscriptionBilling() {
		refusal := *event.Result.BudgetRefusal
		refusal.Issue = cloneIssue(running.Issue)
		state.BudgetRefusals[event.IssueID] = refusal
		o.commentBudgetRefusal(ctx, event.IssueID, refusal)
	}
	if artifactConvergence.Tripped {
		o.parkArtifactGateConvergence(ctx, state, running.Issue, running.Attempt, event.CompletedAt, artifactConvergence)
		return
	}
	if completionCleanliness.Block {
		if attemptCompleted && o.blockCompletionCleanliness(ctx, state, event, running, completionCleanliness) {
			return
		}
		if !attemptCompleted {
			recordStateEvent(state, telemetry.ActivityEvent{
				At:      event.CompletedAt,
				Event:   "dirty_completion_persist_failed",
				Message: "retained claim for " + issueLabel(running.Issue) + " after dirty completion persistence failed",
			})
			return
		}
	}
	if completionRejected {
		if !attemptCompleted {
			recordStateEvent(state, telemetry.ActivityEvent{
				At:      event.CompletedAt,
				Event:   "dirty_completion_persist_failed",
				Message: "retained claim for " + issueLabel(running.Issue) + " after dirty completion persistence failed",
			})
			return
		}
		o.commentCompletionCleanlinessRejection(ctx, running.Issue, completionCleanliness)
		o.scheduleContinuationRetry(ctx, state, running.Issue, 1, event.CompletedAt, dirtyCompletionReason, running.WorkerHost)
		return
	}

	if state.Draining {
		o.cleanupDrainedRun(ctx, state, event.IssueID)
		return
	}
	if terminalState == store.WorkAttemptTerminalSuccess &&
		autoPromoteActiveGatePendingIssue(running.Issue, state, o.cfg, o.cfg.AutoPromote) {
		if gateWaitReason != "" && !attemptCompleted {
			recordStateEvent(state, telemetry.ActivityEvent{
				At:      event.CompletedAt,
				Event:   "rework_gate_wait_persist_failed",
				Message: "retained claim for " + issueLabel(running.Issue) + " after Rework gate-wait persistence failed",
			})
			return
		}
		o.finishCompletedGateWaitRun(ctx, state, running.Issue)
		return
	}
	if spendProgress.Block && o.blockSpendProgress(ctx, state, running, spendProgress, event.CompletedAt) {
		return
	}
	if terminalState == store.WorkAttemptTerminalNoProgress && progress.Block {
		if o.blockImplementProgress(ctx, state, running, progress, event.CompletedAt) {
			return
		}
	}
	if progress.DependencyDeferral {
		if attemptCompleted {
			o.finishImplementDependencyDeferral(ctx, state, running.Issue, event.CompletedAt)
		} else {
			recordStateEvent(state, telemetry.ActivityEvent{
				At:      event.CompletedAt,
				Event:   "implement_dependency_deferral_persist_failed",
				Message: "retained claim for " + issueLabel(running.Issue) + " after dependency deferral persistence failed",
			})
		}
		return
	}
	if running.CompletionLane != "" {
		o.finishAcceptedCompletionLaneRun(ctx, state, running, event.CompletedAt)
		return
	}
	if terminalState == store.WorkAttemptTerminalSuccess {
		transition := o.transitionCompletedActiveIssuesToReview(ctx, state, []connector.Issue{running.Issue}, event.CompletedAt)
		if _, transitioned := transition.transitioned[event.IssueID]; transitioned {
			return
		}
	}
	if event.Result.BudgetRefusal != nil && !o.cfg.subscriptionBilling() && event.Result.BudgetRefusal.Code == string(budget.ReasonPerIssueMaxUSD) {
		if err := o.abandonClaim(ctx, event.IssueID); err != nil && o.logger != nil {
			o.logger.Warn("per-issue budget hold claim release failed", "issue_id", event.IssueID, "error", err)
		}
		delete(state.Claimed, event.IssueID)
		delete(state.Retry, event.IssueID)
		delete(state.PriorAttempts, event.IssueID)
		return
	}
	o.scheduleContinuationRetry(ctx, state, running.Issue, 1, event.CompletedAt, "", running.WorkerHost)
}

func deliverableCommandEvidenceMetadata(result runpkg.RunResult) map[string]any {
	if len(result.DeliverableCommands) == 0 {
		return nil
	}
	return map[string]any{"deliverable_commands": result.DeliverableCommands}
}

func (o *Orchestrator) handleWorkspacePreparationFailure(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) {
	errorMessage := event.Err.Error()
	o.logWorkerLifecycle(running.Issue, "worker_workspace_preparation_failed",
		telemetry.WorkAttemptIDKey, running.WorkAttemptID,
		"attempt", running.Attempt,
		"worker_host", strings.TrimSpace(running.WorkerHost),
		"error", event.Err,
	)
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalFailure, event.Err, workAttemptErrorWorkspace, errorMessage)
	attemptCompleted := o.completeDurableWorkAttemptWithMetadata(
		ctx,
		state,
		running,
		event.CompletedAt,
		store.WorkAttemptTerminalFailure,
		workAttemptErrorWorkspace,
		errorMessage,
		"workspace_repair",
		"workspace preparation failed",
		nil,
	)
	if attemptCompleted {
		count, latest, known := o.consecutiveRetryCycleCount(ctx, state, running.Issue, workspacePreparationRetryLimitCause, event.CompletedAt)
		switch {
		case !known:
			o.recordRetryCycleHistoryUnavailable(state, running.Issue, workspacePreparationRetryLimitCause, event.CompletedAt)
		case count >= consecutiveRetryCycleLimit:
			if _, parked := o.parkRetryCycleLimit(
				ctx,
				state,
				running.Issue,
				running.Mode,
				running.DiffStats,
				workspacePreparationRetryLimitCause,
				count,
				latest,
				event.CompletedAt,
			); parked {
				return
			}
		}
	}
	attempt := event.RetryAttempt
	if attempt < 1 {
		attempt = nextAttempt(running.Attempt)
	}
	delay := event.RetryDelay
	if delay <= 0 {
		delay = o.retryDelay(attempt, false)
	}
	o.scheduleRetryAfter(state, running.Issue, attempt, event.CompletedAt, delay, errorMessage, running.WorkerHost)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "workspace_repair_retry_scheduled",
		Message: "scheduled workspace repair retry for " + issueLabel(running.Issue),
	})
}

func (o *Orchestrator) completeRedundantGateWaitRun(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) bool {
	if event.Result.PullRequestHeadPushed || event.Result.PullRequestUpdated {
		return false
	}
	if !autoPromoteDurableGateWaitTrackedIssue(running.Issue, o.cfg, o.cfg.AutoPromote) {
		return false
	}
	attempt, ok, err := o.latestSuccessfulGateWaitAttempt(ctx, running.Issue)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"gate-wait completion history lookup failed",
				"issue_id", running.Issue.ID,
				"identifier", running.Issue.Identifier,
				"error", err,
			)
		}
		return false
	}
	if !ok {
		return false
	}

	o.completeDurableWorkAttempt(
		ctx,
		state,
		running,
		event.CompletedAt,
		store.WorkAttemptTerminalSuperseded,
		"awaiting_gate",
		"completed gate-wait work already has a successful attempt",
		"superseded",
		"ignored redundant gate-wait dispatch",
	)
	state.Completed[event.IssueID] = completedFromGateWaitAttempt(running.Issue, attempt)
	tokens := event.Result.Tokens
	if tokens == (TokenTotals{}) {
		tokens = running.Tokens
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, tokens)
	if event.Result.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	}
	if diffStatsPresent(event.Result.DiffStats) {
		state.DiffStats[event.IssueID] = event.Result.DiffStats
	}
	o.finishCompletedGateWaitRun(ctx, state, running.Issue)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "gate_wait_dispatch_superseded",
		Message: "ignored redundant dispatch for completed gate-wait " + issueLabel(running.Issue),
	})
	return true
}

func (o *Orchestrator) finishCompletedGateWaitRun(ctx context.Context, state *State, issue connector.Issue) {
	issueID := strings.TrimSpace(issue.ID)
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("release completed gate-wait claim failed", "issue_id", issueID, "error", err)
	}
	o.releaseClaim(state, issueID)
}

func (o *Orchestrator) commentBudgetRefusal(ctx context.Context, issueID string, refusal BudgetRefusal) {
	if o.connector == nil {
		return
	}
	body := strings.TrimSpace(refusal.Comment)
	if body == "" {
		body = strings.TrimSpace(refusal.Message)
	}
	if body == "" {
		return
	}
	if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
		o.logger.Warn(
			"budget refusal comment failed",
			"issue_id", issueID,
			"identifier", refusal.Issue.Identifier,
			"code", refusal.Code,
			"error", err,
		)
	}
}

func resetWorkerFailureBreakers(state *State, issueID string) {
	delete(state.InstantFailures, issueID)
	delete(state.RepeatedFailures, issueID)
}

func implementCompletionHasDurableProgress(running Running, decision implementCompletionProgressDecision) bool {
	if strings.TrimSpace(running.CompletionLane) != "" || implementProgressLinkedPullRequest(decision.Issue) ||
		strings.TrimSpace(decision.CompletionKind) == workpad.CompletionOperational {
		return true
	}
	for _, kind := range decision.ProgressKinds {
		if strings.TrimSpace(kind) == "tracker_state_transition" {
			return true
		}
	}
	return false
}

func (o *Orchestrator) tripInstantFailureCircuitBreaker(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	attempt int,
) bool {
	if state == nil || event.Err == nil || !instantFailureDuration(running, event) {
		delete(state.InstantFailures, event.IssueID)
		return false
	}
	if state.InstantFailures == nil {
		state.InstantFailures = map[string]InstantFailure{}
	}
	key := instantFailureErrorKey(event.Err)
	displayError := o.operatorText(key)
	if displayError == "" {
		displayError = o.operatorText(event.Err.Error())
	}
	failure := state.InstantFailures[event.IssueID]
	failureKey := failure.errorKey
	if failureKey == "" {
		failureKey = failure.Error
	}
	if failureKey != key {
		failure = InstantFailure{
			Issue:          cloneIssue(running.Issue),
			Error:          displayError,
			errorKey:       key,
			FirstFailureAt: event.CompletedAt,
		}
	}
	failure.Count++
	failure.Issue = cloneIssue(running.Issue)
	failure.LastFailureAt = event.CompletedAt
	state.InstantFailures[event.IssueID] = failure
	if failure.Count < instantFailureThreshold {
		return false
	}

	o.parkInstantFailure(ctx, state, event, running, failure, attempt)
	return true
}

func instantFailureDuration(running Running, event runpkg.Completion) bool {
	if !running.StartedAt.IsZero() && !event.CompletedAt.IsZero() {
		duration := event.CompletedAt.Sub(running.StartedAt)
		return duration >= 0 && duration < instantFailureMaxDuration
	}
	if event.Result.Tokens.RuntimeSeconds > 0 {
		return event.Result.Tokens.RuntimeSeconds < instantFailureMaxDuration.Seconds()
	}
	return false
}

func instantFailureErrorKey(err error) string {
	var carrier interface {
		BackendErrorBody() string
	}
	if errors.As(err, &carrier) {
		if body := strings.TrimSpace(carrier.BackendErrorBody()); body != "" {
			return body
		}
	}
	return strings.TrimSpace(err.Error())
}

func (o *Orchestrator) parkInstantFailure(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	failure InstantFailure,
	attempt int,
) {
	targetState := o.instantFailureParkState()
	issue := cloneIssue(running.Issue)
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		issue,
		running.Mode,
		instantFailureCircuitBreakerCause,
		blockedRecoveryPredicateFingerprintChange,
		"Todo",
		running.DiffStats,
	)
	if targetState != "" {
		if err := o.updateIssueStateByIDWithMetadata(ctx, state, issue.ID, issue, targetState, event.CompletedAt, instantFailureCircuitBreakerLaneReason, metadata, laneMutationRevokeWorker); err != nil {
			if o.logger != nil {
				o.logger.Error(
					"instant fail circuit breaker state transition failed",
					"issue_id", issue.ID,
					"identifier", issue.Identifier,
					"target_state", targetState,
					"error", err,
				)
			}
		} else {
			issue.State = targetState
		}
	}
	if o.connector != nil {
		if err := o.connector.CreateComment(ctx, issue.ID, instantFailureComment(issue, event.Err, failure, attempt, targetState, o.cfg.OutputTruncationMaxBytes)); err != nil && o.logger != nil {
			o.logger.Error("instant fail circuit breaker comment failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
	}
	if err := o.abandonClaim(ctx, issue.ID); err != nil && o.logger != nil {
		o.logger.Error("instant fail circuit breaker claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
	delete(state.Claimed, issue.ID)
	delete(state.Retry, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.PriorAttempts, issue.ID)
	delete(state.RepeatedFailures, issue.ID)
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	state.Blocked[issue.ID] = Blocked{
		Issue:                   issue,
		Reason:                  instantFailureBlockedReasonPrefix + failure.Error,
		RecoveryAction:          "defer",
		RecoveryReason:          blockedRecoveryReasonBreakerCooldownActive,
		RecoveryTarget:          metadata.BlockedRecovery.TargetState,
		RecoveryRemedy:          BlockedRecoveryOperatorRemedy(issue, blockedRecoveryReasonBreakerCooldownActive),
		RecoveryReachability:    blockedRecoveryReachability("defer"),
		RecoveryIntentResumable: true,
		BlockedAt:               event.CompletedAt,
		Source:                  BlockedSourceProjectStatus,
		Recovery:                metadata.BlockedRecovery,
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "worker_instant_fail_circuit_breaker_tripped",
		Message: "parked " + issueLabel(issue) + " after repeated instant worker failures: " + failure.Error,
	})
	if o.logger != nil {
		attrs := []any{
			"attempt", attempt,
			"instant_failures", failure.Count,
			"target_state", targetState,
			"error", event.Err,
		}
		if body := o.operatorText(instantFailureErrorKey(event.Err)); body != "" {
			attrs = append(attrs, "backend_error_body", body)
		}
		var carrier interface {
			BackendErrorMessage() string
		}
		if errors.As(event.Err, &carrier) {
			if message := o.operatorText(carrier.BackendErrorMessage()); message != "" {
				attrs = append(attrs, "backend_error_message", message)
			}
		}
		telemetry.LogLifecycleMessage(o.logger, slog.LevelError, telemetry.LifecycleSafetyControl, "worker_instant_fail_circuit_breaker_tripped", "worker instant fail circuit breaker tripped", o.runningLifecycleCorrelation(issue, running), attrs...)
	}
}

func (o *Orchestrator) instantFailureParkState() string {
	return blockedStatusState
}

func (o *Orchestrator) tripTokenCeilingCircuitBreaker(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	attempt int,
) bool {
	var ceilingErr *runpkg.SessionTokenCeilingError
	if state == nil || !errors.As(event.Err, &ceilingErr) || ceilingErr == nil {
		return false
	}
	failure := o.recordRepeatedFailure(state, event, running)
	targetState := o.instantFailureParkState()
	issue := cloneIssue(running.Issue)
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		issue,
		running.Mode,
		"token_ceiling_circuit_breaker",
		blockedRecoveryPredicateFingerprintChange,
		autoPromoteReworkState,
		running.DiffStats,
	)
	if targetState != "" {
		if err := o.updateIssueStateByIDWithMetadata(ctx, state, issue.ID, issue, targetState, event.CompletedAt, "token_ceiling_circuit_breaker", metadata, laneMutationRevokeWorker); err != nil {
			if o.logger != nil {
				o.logger.Error(
					"token ceiling circuit breaker state transition failed",
					"issue_id", issue.ID,
					"identifier", issue.Identifier,
					"target_state", targetState,
					"error", err,
				)
			}
		} else {
			issue.State = targetState
		}
	}
	if o.connector != nil {
		comment := tokenCeilingFailureComment(issue, ceilingErr, running.Attempt, attempt, targetState)
		if err := o.connector.CreateComment(ctx, issue.ID, comment); err != nil && o.logger != nil {
			o.logger.Error("token ceiling circuit breaker comment failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
	}
	if err := o.abandonClaim(ctx, issue.ID); err != nil && o.logger != nil {
		o.logger.Error("token ceiling circuit breaker claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
	delete(state.Claimed, issue.ID)
	delete(state.Retry, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.PriorAttempts, issue.ID)
	delete(state.InstantFailures, issue.ID)
	delete(state.RepeatedFailures, issue.ID)
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	reason := tokenCeilingFailureReason(ceilingErr)
	state.Blocked[issue.ID] = Blocked{
		Issue:                   issue,
		Reason:                  reason,
		RecoveryAction:          "defer",
		RecoveryReason:          blockedRecoveryReasonBreakerCooldownActive,
		RecoveryTarget:          metadata.BlockedRecovery.TargetState,
		RecoveryRemedy:          BlockedRecoveryOperatorRemedy(issue, blockedRecoveryReasonBreakerCooldownActive),
		RecoveryReachability:    blockedRecoveryReachability("defer"),
		RecoveryIntentResumable: true,
		BlockedAt:               event.CompletedAt,
		Source:                  BlockedSourceProjectStatus,
		Recovery:                metadata.BlockedRecovery,
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "worker_token_ceiling_circuit_breaker_tripped",
		Message: "parked " + issueLabel(issue) + " after a session token ceiling failure: " + reason,
	})
	if o.logger != nil {
		telemetry.LogLifecycleMessage(o.logger, slog.LevelError, telemetry.LifecycleSafetyControl, "worker_token_ceiling_circuit_breaker_tripped", "worker token ceiling circuit breaker tripped", o.runningLifecycleCorrelation(issue, running),
			"failed_attempt", running.Attempt,
			"prevented_retry_attempt", attempt,
			"repeated_failures", failure.Count,
			"target_state", targetState,
			"observed_total_tokens", ceilingErr.TotalTokens,
			"ceiling_tokens", ceilingErr.CeilingTokens,
			"ceiling_source", ceilingErr.Source,
			"error", event.Err,
		)
	}
	return true
}

func tokenCeilingFailureReason(ceilingErr *runpkg.SessionTokenCeilingError) string {
	source := strings.TrimSpace(ceilingErr.Source)
	if source == "" {
		source = "unknown"
	}
	return fmt.Sprintf(
		"%sobserved %d tokens above the %d %s ceiling",
		tokenCeilingBlockedReasonPrefix,
		ceilingErr.TotalTokens,
		ceilingErr.CeilingTokens,
		source,
	)
}

func tokenCeilingFailureComment(
	issue connector.Issue,
	ceilingErr *runpkg.SessionTokenCeilingError,
	failedAttempt int,
	preventedRetryAttempt int,
	targetState string,
) string {
	var b strings.Builder
	b.WriteString("Detent stopped retrying this worker because the session exceeded its configured token ceiling.")
	if targetState = strings.TrimSpace(targetState); targetState != "" {
		b.WriteString("\n\nIssue parked in `")
		b.WriteString(targetState)
		b.WriteString("` until the configured breaker cooldown ends.")
	}
	b.WriteString("\n\n- issue: ")
	b.WriteString(issueLabel(issue))
	b.WriteString("\n- failed_attempt: ")
	b.WriteString(strconv.Itoa(failedAttempt))
	b.WriteString("\n- prevented_retry_attempt: ")
	b.WriteString(strconv.Itoa(preventedRetryAttempt))
	b.WriteString("\n- observed_total_tokens: ")
	b.WriteString(strconv.FormatInt(ceilingErr.TotalTokens, 10))
	b.WriteString("\n- ceiling_tokens: ")
	b.WriteString(strconv.FormatInt(ceilingErr.CeilingTokens, 10))
	b.WriteString("\n- ceiling_source: ")
	b.WriteString(strings.TrimSpace(ceilingErr.Source))
	b.WriteString("\n\nBefore the cooldown ends, split the issue into narrower work, apply the label configured by `agent.max_session_token_override_label` for a deliberate per-issue bypass, or raise `agent.max_session_tokens` (and `agent.max_session_context_multiplier` when it is the active guard). Detent then returns the issue to its prior lane automatically.")
	return b.String()
}

// tripRepeatedFailureCircuitBreaker parks an issue after too many consecutive
// worker failures of any duration. The instant-failure breaker only counts
// sub-instantFailureMaxDuration failures with identical error text, which lets
// a long-running failure — one that spends minutes of paid agent time per
// attempt, like a session token ceiling hit — retry forever. This breaker
// counts every worker failure and resets only on a successful completion.
func (o *Orchestrator) tripRepeatedFailureCircuitBreaker(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	attempt int,
) bool {
	if state == nil || event.Err == nil {
		return false
	}
	failure := o.recordRepeatedFailure(state, event, running)
	if failure.Count < repeatedFailureThreshold {
		return false
	}

	o.parkRepeatedFailure(ctx, state, event, running, failure, attempt)
	return true
}

func (o *Orchestrator) recordRepeatedFailure(state *State, event runpkg.Completion, running Running) RepeatedFailure {
	if state.RepeatedFailures == nil {
		state.RepeatedFailures = map[string]RepeatedFailure{}
	}
	failure := state.RepeatedFailures[event.IssueID]
	if failure.Count == 0 {
		failure.FirstFailureAt = event.CompletedAt
	}
	failure.Count++
	if isGitHubRESTBudgetHeadroomError(event.Err) {
		failure.GitHubRESTBudgetFailures++
	}
	failure.Issue = cloneIssue(running.Issue)
	failure.Error = o.operatorText(event.Err.Error())
	failure.LastFailureAt = event.CompletedAt
	state.RepeatedFailures[event.IssueID] = failure
	return failure
}

func (o *Orchestrator) parkRepeatedFailure(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	failure RepeatedFailure,
	attempt int,
) {
	targetState := o.instantFailureParkState()
	issue := cloneIssue(running.Issue)
	recoveryPredicate := blockedRecoveryPredicateFingerprintChange
	recoveryTarget := "Todo"
	budgetEvidence := githubRESTBudgetEvidence{}
	budgetPark := failure.GitHubRESTBudgetFailures == failure.Count
	if budgetPark {
		var ok bool
		budgetEvidence, ok = o.githubRESTBudgetParkEvidence(state, event.Err, issue.State)
		budgetPark = ok
		if budgetPark {
			recoveryPredicate = blockedRecoveryPredicateGitHubRESTBudget
			recoveryTarget = budgetEvidence.TargetState
		}
	}
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		issue,
		running.Mode,
		repeatedFailureCircuitBreakerCause,
		recoveryPredicate,
		recoveryTarget,
		running.DiffStats,
	)
	if budgetPark {
		metadata.BlockedRecovery.TargetState = recoveryTarget
		metadata.BlockedRecovery.IntentResumable = true
		applyGitHubRESTBudgetEvidence(metadata.BlockedRecovery, budgetEvidence)
	}
	if targetState != "" {
		if err := o.updateIssueStateByIDWithMetadata(ctx, state, issue.ID, issue, targetState, event.CompletedAt, "repeated_failure_circuit_breaker", metadata, laneMutationRevokeWorker); err != nil {
			if o.logger != nil {
				o.logger.Error(
					"repeated failure circuit breaker state transition failed",
					"issue_id", issue.ID,
					"identifier", issue.Identifier,
					"target_state", targetState,
					"error", err,
				)
			}
		} else {
			issue.State = targetState
		}
	}
	if o.connector != nil {
		if err := o.connector.CreateComment(ctx, issue.ID, repeatedFailureComment(issue, event.Err, failure, attempt, targetState, o.cfg.OutputTruncationMaxBytes, budgetPark)); err != nil && o.logger != nil {
			o.logger.Error("repeated failure circuit breaker comment failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
	}
	if err := o.abandonClaim(ctx, issue.ID); err != nil && o.logger != nil {
		o.logger.Error("repeated failure circuit breaker claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
	delete(state.Claimed, issue.ID)
	delete(state.Retry, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.PriorAttempts, issue.ID)
	delete(state.InstantFailures, issue.ID)
	delete(state.RepeatedFailures, issue.ID)
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	blocked := Blocked{
		Issue:                   issue,
		Reason:                  repeatedFailureBlockedReasonPrefix + failure.Error,
		RecoveryAction:          "defer",
		RecoveryReason:          blockedRecoveryReasonBreakerCooldownActive,
		RecoveryTarget:          metadata.BlockedRecovery.TargetState,
		RecoveryRemedy:          BlockedRecoveryOperatorRemedy(issue, blockedRecoveryReasonBreakerCooldownActive),
		RecoveryReachability:    blockedRecoveryReachability("defer"),
		RecoveryIntentResumable: true,
		BlockedAt:               event.CompletedAt,
		Source:                  BlockedSourceProjectStatus,
		Recovery:                metadata.BlockedRecovery,
	}
	if budgetPark {
		blocked.Reason = githubRESTBudgetStatusMessage("waiting for capacity", budgetEvidence)
		blocked.RecoveryAction = "defer"
		blocked.RecoveryReason = githubRESTBudgetWaitingReason
		blocked.RecoveryRemedy = BlockedRecoveryOperatorRemedy(issue, githubRESTBudgetWaitingReason)
		blocked.RecoveryReachability = blockedRecoveryReachability("defer")
		blocked.RecoveryIntentResumable = true
	}
	state.Blocked[issue.ID] = blocked
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "worker_repeated_failure_circuit_breaker_tripped",
		Message: "parked " + issueLabel(issue) + " after repeated worker failures: " + failure.Error,
	})
	if o.logger != nil {
		telemetry.LogLifecycleMessage(o.logger, slog.LevelError, telemetry.LifecycleSafetyControl, "worker_repeated_failure_circuit_breaker_tripped", "worker repeated failure circuit breaker tripped", o.runningLifecycleCorrelation(issue, running),
			"attempt", attempt,
			"repeated_failures", failure.Count,
			"first_failure_at", failure.FirstFailureAt,
			"target_state", targetState,
			"error", event.Err,
		)
	}
}

func repeatedFailureComment(issue connector.Issue, err error, failure RepeatedFailure, attempt int, targetState string, maxBytes int, budgetPark bool) string {
	var b strings.Builder
	if budgetPark {
		b.WriteString("Detent paused this worker after ")
	} else {
		b.WriteString("Detent stopped retrying this worker after ")
	}
	b.WriteString(strconv.Itoa(failure.Count))
	if budgetPark {
		b.WriteString(" consecutive attempts reached the worker's GitHub REST reserve. This is a transient resource wait; Detent will re-evaluate it when the matching credential budget recovers.")
	} else {
		b.WriteString(" consecutive failed attempts. Each attempt ran to failure and spent real agent time, so retrying without a change is waste.")
	}
	if targetState = strings.TrimSpace(targetState); targetState != "" {
		b.WriteString("\n\nIssue parked in `")
		b.WriteString(targetState)
		b.WriteString("`.")
	}
	b.WriteString("\n\n- issue: ")
	b.WriteString(issueLabel(issue))
	b.WriteString("\n- latest_attempt: ")
	b.WriteString(strconv.Itoa(attempt))
	b.WriteString("\n- first_failure_at: ")
	b.WriteString(failure.FirstFailureAt.UTC().Format(time.RFC3339))
	b.WriteString("\n- last error:\n\n```text\n")
	b.WriteString(runtimeoutput.Truncate(strings.TrimSpace(err.Error()), maxBytes).Value)
	b.WriteString("\n```")
	if budgetPark {
		b.WriteString("\n\nDetent returns the issue to its prior lane once per GitHub REST exhaustion episode after remaining capacity rises above the recorded worker reserve.")
	} else {
		b.WriteString("\n\nFix the workflow or agent configuration causing every attempt to fail before the breaker cooldown ends. Detent then returns the issue to its prior lane automatically.")
	}
	return b.String()
}

func instantFailureComment(issue connector.Issue, err error, failure InstantFailure, attempt int, targetState string, maxBytes int) string {
	var b strings.Builder
	b.WriteString("Detent stopped retrying this worker after ")
	b.WriteString(strconv.Itoa(failure.Count))
	b.WriteString(" consecutive instant failures with the same backend error.")
	if targetState = strings.TrimSpace(targetState); targetState != "" {
		b.WriteString("\n\nIssue parked in `")
		b.WriteString(targetState)
		b.WriteString("`.")
	}
	b.WriteString("\n\n- issue: ")
	b.WriteString(issueLabel(issue))
	b.WriteString("\n- latest_attempt: ")
	b.WriteString(strconv.Itoa(attempt))
	b.WriteString("\n- failure_window_seconds: ")
	b.WriteString(strconv.FormatInt(int64(instantFailureMaxDuration/time.Second), 10))
	b.WriteString("\n- error:\n\n```text\n")
	errorText := runtimeoutput.Truncate(strings.TrimSpace(err.Error()), maxBytes).Value
	b.WriteString(errorText)
	b.WriteString("\n```")
	if body := runtimeoutput.Truncate(instantFailureErrorKey(err), maxBytes).Value; body != "" && body != errorText {
		b.WriteString("\n\n- backend_error_body:\n\n```json\n")
		b.WriteString(body)
		b.WriteString("\n```")
	}
	b.WriteString("\n\nFix the pinned agent model or backend configuration before the breaker cooldown ends. Detent then returns the issue to its prior lane automatically.")
	return b.String()
}

func (o *Orchestrator) operatorText(value string) string {
	return runtimeoutput.Truncate(strings.TrimSpace(value), o.cfg.OutputTruncationMaxBytes).Value
}

func (o *Orchestrator) completeLatestTerminalMergeWorkerResult(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) bool {
	issueID := strings.TrimSpace(event.IssueID)
	if issueID == "" || o.connector == nil {
		return false
	}
	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{issueID})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"merge_worker_terminal_state_refresh_failed",
				"issue_id", issueID,
				"identifier", running.Issue.Identifier,
				"error", err,
			)
		}
		return false
	}
	for _, issue := range issues {
		if strings.TrimSpace(issue.ID) != issueID {
			continue
		}
		issue = mergeIssueTrackerFields(running.Issue, issue)
		if !workspaceIssueTerminal(issue, o.cfg.TerminalStates) {
			return o.completeProgrammaticMergeWorkerResult(ctx, state, event, running, issue)
		}
		tokens := event.Result.Tokens
		if tokens == (TokenTotals{}) {
			tokens = running.Tokens
		}
		if diffStatsPresent(event.Result.DiffStats) {
			running.DiffStats = event.Result.DiffStats
		}
		running.Issue = issue
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
		o.completeTerminalRunning(ctx, state, issueID, running, terminalCompletedAt(issue, o.cfg.TerminalStates, event.CompletedAt), tokens)
		if event.Result.RateLimits != nil {
			state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
		}
		return true
	}
	return false
}

func (o *Orchestrator) completeProgrammaticMergeWorkerResult(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) bool {
	if state != nil && state.Draining {
		return false
	}
	if !mergeWorkerTurnSucceeded(event) {
		return false
	}
	if strings.TrimSpace(event.Result.Output) == runpkg.RunOutputMergeFallbackRework {
		o.reworkMergeWorkerResult(ctx, state, event, running, issue, mergeFallbackRequiresReworkReason, nil, event.Result.MergeFallbackFindings)
		return true
	}
	hydrator, ok := o.connector.(connector.PullRequestHydrator)
	if !ok {
		return false
	}
	refreshedIssue, err := hydrator.HydratePullRequest(ctx, issue)
	if err != nil {
		running.Issue = issue
		o.failProgrammaticMergeWorkerResult(ctx, state, event, running, "programmatic_merge_pr_refresh_failed", err)
		return true
	}
	issue = refreshedIssue
	if revocation, revoked := mergeRevocationForIssue(
		issue,
		o.cfg,
		true,
		autoPromoteOperationalCompletionAccepted(state, issue.ID),
	); revoked &&
		mergeRevocationRequiresImmediateStop(revocation, event.Result) {
		o.finishMergeRevocation(ctx, state, event, running, revocation)
		return true
	}
	if event.Result.Output == runpkg.RunOutputMergeFallbackResolved &&
		event.Result.MergePrecheck != nil && event.Result.MergePrecheck.HeadSHA != "" &&
		(issue.PullRequest == nil || issue.PullRequest.HeadSHA != event.Result.MergePrecheck.HeadSHA) {
		o.reworkMergeWorkerResult(ctx, state, event, running, issue, mergeFallbackRequiresReworkReason, nil, "Pull request head changed after deterministic merge-fallback validation.")
		return true
	}
	missingChecks := mergeWorkerMissingRequiredChecks(issue)
	streaks := o.evaluateMergeRequiredCheckStreaks(ctx, issue, missingChecks, event.CompletedAt)
	if persistent := persistentMissingRequiredCheckStreaks(streaks); len(persistent) > 0 {
		o.blockPersistentlyMissingRequiredChecks(ctx, state, event, running, issue, persistent)
		return true
	}
	if event.Result.PullRequestHeadPushed {
		o.recordMergeReservationWait(state, issue, event.CompletedAt)
		triggerPending := false
		if !event.Result.CITriggerLabelReapplied {
			triggerPending = o.scheduleCITriggerLabel(ctx, issue, gate.Effective(o.cfg.AutoPromote.Gate).RequiredStatusChecks, running.Attempt, true, false)
		}
		if triggerPending {
			o.waitForMergeWorkerCITriggerLabel(ctx, state, event, running, issue)
			return true
		}
		o.waitForMergeWorkerCurrentHeadCI(ctx, state, event, running, issue)
		return true
	}
	if mergeWorkerCheckedPullRequest(issue) &&
		strings.EqualFold(strings.TrimSpace(issue.PullRequest.MergeableState), "behind") {
		o.refreshMergeWorkerBase(ctx, state, event, running, issue, "pull_request_base_behind")
		return true
	}
	if !mergeWorkerProgrammaticMergeReady(issue) {
		if pullRequestHydrationBlocksProgress(issue.PullRequest) {
			o.waitForMergeWorkerPullRequestHydration(ctx, state, event, running, issue)
			return true
		}
		if len(missingChecks) > 0 {
			triggerPending := o.scheduleCITriggerLabel(ctx, issue, missingChecks, running.Attempt, false, false)
			if triggerPending || mergeWorkerMissingRequiredChecksPropagating(issue, running.Attempt) {
				o.waitForMergeWorkerRequiredCheckPropagation(ctx, state, event, running, issue)
				return true
			}
			o.reworkMergeWorkerResult(ctx, state, event, running, issue, mergeWorkerRequiredChecksMissingReason, missingChecks, "")
			return true
		}
		if mergeFastPathResult(event) && mergeWorkerMergeabilityPending(issue) {
			o.waitForMergeWorkerMergeability(ctx, state, event, running, issue)
			return true
		}
		if mergeFastPathResult(event) && mergeWorkerProgrammaticMergeWaiting(issue) {
			o.waitForMergeWorkerCurrentHeadCI(ctx, state, event, running, issue)
			return true
		}
		if mergeFastPathResult(event) {
			o.reworkMergeWorkerResult(ctx, state, event, running, issue, mergeWorkerFastPathNotReadyReason, nil, "")
			return true
		}
		return false
	}
	audit := o.securityAuditEvaluation(ctx, issue)
	if gate.Effective(o.cfg.AutoPromote.Gate).SecurityAudit.Enabled && !audit.Allowed {
		reason := "security_audit_" + audit.Reason
		if audit.Reason == securityaudit.ReasonMissing || audit.Reason == securityaudit.ReasonStale {
			o.startSecurityAuditStage(ctx, issue, event.CompletedAt)
			running.Issue = issue
			o.failProgrammaticMergeWorkerResult(ctx, state, event, running, reason, errors.New(reason))
			return true
		}
		o.reworkMergeWorkerResult(ctx, state, event, running, issue, reason, nil, "")
		return true
	}
	if gateRequiresPullRequest(o.cfg.AutoPromote.Gate) {
		var hydrated bool
		issue, hydrated = o.hydrateAutoPromoteReviewThreads(ctx, issue)
		if !hydrated {
			o.waitForMergeWorkerPullRequestHydration(ctx, state, event, running, issue)
			return true
		}
		if issue.PullRequest != nil && len(issue.PullRequest.UnresolvedReviewThreads) > 0 {
			o.reworkMergeWorkerResult(
				ctx,
				state,
				event,
				running,
				issue,
				string(AutoPromoteReasonUnresolvedReviewThreads),
				nil,
				"",
			)
			return true
		}
	}
	merger, ok := o.connector.(connector.PullRequestMerger)
	if !ok {
		return false
	}
	issueID := strings.TrimSpace(event.IssueID)
	repository := pullRequestRepository(issue)
	number := pullRequestNumber(issue)
	headSHA := strings.TrimSpace(issue.PullRequest.HeadSHA)
	if err := merger.MergePullRequest(ctx, repository, number, headSHA, o.cfg.MergeMethod); err != nil {
		if errors.Is(err, connector.ErrPullRequestBaseOutOfDate) {
			o.refreshMergeWorkerBase(ctx, state, event, running, issue, "merge_api_rejected_out_of_date_base")
			return true
		}
		running.Issue = issue
		o.failProgrammaticMergeWorkerResult(ctx, state, event, running, "programmatic_merge_failed", err)
		return true
	}

	targetState := doneStateName(o.cfg.TerminalStates)
	mergedIssue := cloneIssue(issue)
	if mergedIssue.PullRequest != nil {
		mergedIssue.PullRequest.State = "MERGED"
		activityAt := event.CompletedAt.UTC()
		mergedIssue.PullRequest.ActivityAt = &activityAt
	}
	if err := o.updateIssueStateByID(ctx, state, issueID, mergedIssue, targetState, event.CompletedAt, "merge_worker_programmatic_merge", laneMutationAcceptCompletion); err != nil {
		running.Issue = mergedIssue
		o.failProgrammaticMergeWorkerResult(ctx, state, event, running, "programmatic_merge_state_update_failed", err)
		return true
	}

	updatedAt := event.CompletedAt.UTC()
	mergedIssue.State = targetState
	mergedIssue.UpdatedAt = &updatedAt
	mergedIssue.StageUpdatedAt = &updatedAt
	mergeTimingIssue := running.Issue
	running.Issue = mergedIssue
	tokens := event.Result.Tokens
	if tokens == (TokenTotals{}) {
		tokens = running.Tokens
	}
	if diffStatsPresent(event.Result.DiffStats) {
		running.DiffStats = event.Result.DiffStats
	}
	if o.logger != nil {
		o.logger.Info("merge_worker_programmatic_merge", mergeWorkerLogAttrs(mergedIssue, "target_state", targetState)...)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "merge_worker_programmatic_merge",
		Message: "programmatically merged " + issueLabel(mergedIssue) + " and moved it to " + targetState,
	})
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
	o.completeTerminalRunning(ctx, state, issueID, running, terminalCompletedAt(mergedIssue, o.cfg.TerminalStates, event.CompletedAt), tokens)
	mergeTiming := o.recordMergeCompleted(state, mergeTimingIssue, event.CompletedAt, targetState)
	if completed, ok := state.Completed[issueID]; ok {
		completed.MergeTiming = mergeTiming
		state.Completed[issueID] = completed
	}
	o.logMergeWorkerSuccess(mergeTimingIssue, targetState)
	if event.Result.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	}
	return true
}

func (o *Orchestrator) waitForMergeWorkerCurrentHeadCI(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) {
	attempt := running.Attempt
	if attempt < 1 {
		attempt = 1
	}
	retryError := mergeWorkerCurrentHeadCIWaitReason(issue)
	exceeded := o.mergeWorkerCurrentHeadCIWaitExceeded(state, issue, event.CompletedAt)
	reservation := o.recordMergeReservationWait(state, issue, event.CompletedAt)
	running.Issue = issue
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
	o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalSuccess, "", "", "waiting", retryError,
		map[string]any{mergeReservationMetadataKey: reservation})
	o.releaseTerminalAttemptClaim(ctx, state, issue, event.CompletedAt)
	o.scheduleRetry(state, issue, attempt, event.CompletedAt, retryError, true, running.WorkerHost)
	retry := state.Retry[issue.ID]
	retry.Wait = RetryWait{
		Kind:                  retryWaitCurrentHeadCI,
		StartedAt:             state.MergeTimings[strings.TrimSpace(issue.ID)].CIWaitStartedAt,
		PollCount:             1,
		PendingChecks:         mergeWorkerCurrentHeadCIPendingChecks(issue),
		WorkspaceCreateCount:  1,
		WorkspaceDestroyCount: 1,
	}
	state.Retry[issue.ID] = retry
	o.logMergeWorkerCurrentHeadCIWait(state, issue, retry, event.CompletedAt)
	if exceeded {
		err := fmt.Errorf("current-head CI wait exceeded %s: %s", mergeWorkerCurrentHeadCIWaitTimeout, retryError)
		if o.blockExhaustedMergeWorker(ctx, state, running, event.CompletedAt, mergeWorkerCurrentHeadCIWaitExceededReason, attempt, err) {
			return
		}
	}
}

func (o *Orchestrator) pollMergeWorkerCurrentHeadCI(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	retry Retry,
	now time.Time,
) (Retry, bool, string) {
	if retry.Wait.Kind != retryWaitCurrentHeadCI {
		return retry, false, ""
	}
	if !mergeWorkerProgrammaticMergeWaiting(issue) {
		retry.Attempt = nextAttempt(retry.Attempt)
		retry.Wait = RetryWait{}
		state.Retry[issue.ID] = retry
		return retry, false, ""
	}

	timing := reconcileMergeWorkerCurrentHeadCIWait(state, issue, now)
	retry.Issue = cloneIssue(issue)
	retry.Error = mergeWorkerCurrentHeadCIWaitReason(issue)
	if retry.Wait.StartedAt.IsZero() || !retry.Wait.StartedAt.Equal(timing.CIWaitStartedAt) {
		retry.Wait.StartedAt = timing.CIWaitStartedAt
		retry.Wait.PollCount = 0
	}
	retry.Wait.PollCount++
	retry.Wait.PendingChecks = mergeWorkerCurrentHeadCIPendingChecks(issue)
	retry.DueAt = mergeWorkerCurrentHeadCINextPollAt(retry.Wait.StartedAt, now, o.cfg.ContinuationRetryDelay)
	state.Retry[issue.ID] = retry
	o.logMergeWorkerCurrentHeadCIWait(state, issue, retry, now)

	if !now.Before(retry.Wait.StartedAt.Add(mergeWorkerCurrentHeadCIWaitTimeout)) {
		err := fmt.Errorf("current-head CI wait exceeded %s: %s", mergeWorkerCurrentHeadCIWaitTimeout, retry.Error)
		running := Running{
			Issue:      cloneIssue(issue),
			Attempt:    retry.Attempt,
			WorkerHost: retry.WorkerHost,
			DiffStats:  state.DiffStats[issue.ID],
		}
		if o.blockExhaustedMergeWorker(ctx, state, running, now, mergeWorkerCurrentHeadCIWaitExceededReason, retry.Attempt, err) {
			return retry, true, mergeWorkerCurrentHeadCIWaitExceededReason
		}
	}
	return retry, true, dispatchSkipCurrentHeadCIWait
}

func mergeWorkerCurrentHeadCINextPollAt(startedAt, now time.Time, delay time.Duration) time.Time {
	if delay < 0 {
		delay = 0
	}
	next := now.Add(delay)
	if !startedAt.IsZero() {
		deadline := startedAt.Add(mergeWorkerCurrentHeadCIWaitTimeout)
		if deadline.Before(next) {
			next = deadline
		}
	}
	if next.Before(now) {
		return now
	}
	return next
}

func (o *Orchestrator) logMergeWorkerCurrentHeadCIWait(
	state *State,
	issue connector.Issue,
	retry Retry,
	now time.Time,
) {
	elapsed := now.Sub(retry.Wait.StartedAt)
	if retry.Wait.StartedAt.IsZero() || elapsed < 0 {
		elapsed = 0
	}
	if o.logger != nil {
		o.logger.Info(
			"merge_worker_waiting_current_head_ci",
			mergeWorkerLogAttrs(
				issue,
				"attempt", retry.Attempt,
				"poll_count", retry.Wait.PollCount,
				"elapsed_wait", elapsed.String(),
				"elapsed_wait_seconds", int64(elapsed/time.Second),
				"pending_checks", strings.Join(retry.Wait.PendingChecks, ", "),
				"workspace_create_count", retry.Wait.WorkspaceCreateCount,
				"workspace_destroy_count", retry.Wait.WorkspaceDestroyCount,
				"reason", retry.Error,
			)...,
		)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "merge_worker_waiting_current_head_ci",
		Message: fmt.Sprintf("waiting on current-head CI for %s (poll %d)", issueLabel(issue), retry.Wait.PollCount),
	})
}

func (o *Orchestrator) waitForMergeWorkerMergeability(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) {
	o.waitForMergeWorkerRetry(
		ctx,
		state,
		event,
		running,
		issue,
		running.Attempt,
		"waiting for GitHub mergeability computation",
		"merge_worker_waiting_mergeability",
		"merge worker is waiting for GitHub mergeability computation for ",
	)
}

func (o *Orchestrator) waitForMergeWorkerRequiredCheckPropagation(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) {
	o.waitForMergeWorkerRetry(
		ctx,
		state,
		event,
		running,
		issue,
		nextAttempt(running.Attempt),
		"waiting for required checks to appear on the current head",
		"merge_worker_waiting_required_check_propagation",
		"merge worker is waiting for required checks to appear for ",
	)
}

func (o *Orchestrator) waitForMergeWorkerCITriggerLabel(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) {
	o.waitForMergeWorkerRetry(
		ctx,
		state,
		event,
		running,
		issue,
		running.Attempt,
		"waiting for CI trigger label after current-head push",
		"merge_worker_waiting_ci_trigger_label",
		"merge worker is waiting for the CI trigger label after pushing ",
	)
}

func (o *Orchestrator) waitForMergeWorkerPullRequestHydration(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
) {
	o.waitForMergeWorkerRetry(
		ctx,
		state,
		event,
		running,
		issue,
		running.Attempt,
		"waiting for fresh pull request hydration",
		"merge_worker_waiting_pull_request_hydration",
		"merge worker is waiting for fresh pull request hydration for ",
	)
}

func (o *Orchestrator) waitForMergeWorkerRetry(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
	attempt int,
	retryError string,
	eventName string,
	eventMessage string,
) {
	running.Issue = issue
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
	o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalSuccess, "", "", "waiting", retryError,
		map[string]any{mergeReservationMetadataKey: state.mergeReservations[mergeWorkerRepositoryKey(issue)]})
	o.releaseTerminalAttemptClaim(ctx, state, issue, event.CompletedAt)
	if attempt < 1 {
		attempt = 1
	}
	o.scheduleRetry(state, issue, attempt, event.CompletedAt, retryError, true, running.WorkerHost)
	if o.logger != nil {
		o.logger.Info(eventName, mergeWorkerLogAttrs(issue, "attempt", attempt, "reason", retryError)...)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   eventName,
		Message: eventMessage + issueLabel(issue),
	})
}

func (o *Orchestrator) mergeWorkerCurrentHeadCIWaitExceeded(state *State, issue connector.Issue, completedAt time.Time) bool {
	if state == nil || completedAt.IsZero() {
		return false
	}
	timing := reconcileMergeWorkerCurrentHeadCIWait(state, issue, completedAt)
	return !completedAt.Before(timing.CIWaitStartedAt.Add(mergeWorkerCurrentHeadCIWaitTimeout))
}

func mergeWorkerCurrentHeadCIWaitReason(issue connector.Issue) string {
	const reason = "waiting for current-head CI"
	if issue.PullRequest == nil {
		return reason
	}
	if unstartedChecks := pullRequestCheckNames(issue.PullRequest.UnstartedChecks); len(unstartedChecks) > 0 {
		return reason + ": unstarted checks: " + strings.Join(unstartedChecks, ", ")
	}
	pendingRequiredChecks := make([]string, 0, len(issue.PullRequest.RequiredCheckFailures))
	for _, check := range issue.PullRequest.RequiredCheckFailures {
		if autoPromoteCheckPending(check) && strings.TrimSpace(check.Name) != "" {
			pendingRequiredChecks = append(pendingRequiredChecks, check.Name)
		}
	}
	if pendingRequiredChecks = uniqueStrings(pendingRequiredChecks); len(pendingRequiredChecks) > 0 {
		return reason + ": pending required checks: " + strings.Join(pendingRequiredChecks, ", ")
	}
	if runningChecks := uniqueStrings(issue.PullRequest.RunningChecks); len(runningChecks) > 0 {
		return reason + ": pending checks: " + strings.Join(runningChecks, ", ")
	}
	return reason
}

func mergeWorkerCurrentHeadCIPendingChecks(issue connector.Issue) []string {
	return autoPromotePendingChecksFromPullRequest(issue.PullRequest)
}

func (o *Orchestrator) refreshMergeWorkerBase(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
	reason string,
) {
	reservation := reserveMergeCandidate(state, issue, event.CompletedAt)
	reservation.RefreshHeadSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
	state.mergeReservations[reservation.Repository] = reservation
	running.Issue = issue
	o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalSuccess,
		"", "", "waiting", "base refresh required before merge", map[string]any{mergeReservationMetadataKey: reservation})
	o.releaseTerminalAttemptClaim(ctx, state, issue, event.CompletedAt)
	o.scheduleRetry(state, issue, nextAttempt(running.Attempt), event.CompletedAt, "base refresh required before merge", true, running.WorkerHost)
	if o.logger != nil {
		o.logger.Info("merge_base_refresh_required", mergeWorkerLogAttrs(issue,
			"reason", reason, "prior_validation_invalidated", true, "reservation_expires_at", reservation.ExpiresAt)...)
	}
}

func (o *Orchestrator) failProgrammaticMergeWorkerResult(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	reason string,
	err error,
) {
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalFailure, err, reason, errorString(err))
	o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalFailure, reason, errorString(err), "merging", "programmatic merge failed")
	o.logMergeWorkerFailure(running.Issue, reason, err)
	o.recordMergeFailed(state, running.Issue, event.CompletedAt, reason, err)
	attempt := nextAttempt(running.Attempt)
	if attempt > maxMergeWorkerRunnerFailures {
		if o.blockExhaustedMergeWorker(ctx, state, running, event.CompletedAt, mergeWorkerRetryExhaustedReason, attempt, err) {
			return
		}
	}
	o.scheduleRetry(
		state,
		running.Issue,
		attempt,
		event.CompletedAt,
		errorString(err),
		false,
		running.WorkerHost,
	)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "merge_worker_programmatic_merge_failed",
		Message: "programmatic merge failed for " + issueLabel(running.Issue) + ": " + errorString(err),
	})
}

func mergeWorkerTurnSucceeded(event runpkg.Completion) bool {
	return event.Err == nil && !strings.EqualFold(strings.TrimSpace(event.Result.FinalState), runpkg.FinalStateFailed)
}

func mergeFastPathResult(event runpkg.Completion) bool {
	if event.Request.Mode != runpkg.RunModeMerge {
		return false
	}
	switch strings.TrimSpace(event.Result.Output) {
	case runpkg.RunOutputMergeFastPathClean, runpkg.RunOutputMergeFastPathCheckedHead, runpkg.RunOutputMergeFallbackResolved:
		return true
	default:
		return false
	}
}

func mergeWorkerProgrammaticMergeReady(issue connector.Issue) bool {
	return mergeWorkerCheckedPullRequest(issue) &&
		strings.EqualFold(strings.TrimSpace(issue.PullRequest.MergeableState), "clean")
}

func mergeWorkerCheckedPullRequest(issue connector.Issue) bool {
	if strings.TrimSpace(issue.ID) == "" || issue.PullRequest == nil {
		return false
	}
	pullRequest := issue.PullRequest
	if pullRequestHydrationBlocksProgress(pullRequest) {
		return false
	}
	if normalizePullRequestState(pullRequest.State) != "open" || pullRequest.Draft {
		return false
	}
	if !mergeWorkerCIGreen(pullRequest.CIStatus) || len(pullRequest.RequiredCheckFailures) > 0 || pullRequest.MergeQueueEntry != nil {
		return false
	}
	return pullRequestRepository(issue) != "" &&
		pullRequestNumber(issue) > 0 &&
		strings.TrimSpace(pullRequest.HeadSHA) != ""
}

func mergeWorkerMergeabilityPending(issue connector.Issue) bool {
	if strings.TrimSpace(issue.ID) == "" || issue.PullRequest == nil {
		return false
	}
	pullRequest := issue.PullRequest
	if pullRequestHydrationBlocksProgress(pullRequest) {
		return false
	}
	if normalizePullRequestState(pullRequest.State) != "open" || pullRequest.Draft {
		return false
	}
	if !mergeWorkerCIGreen(pullRequest.CIStatus) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(pullRequest.MergeableState)) {
	case "", "unknown":
	default:
		return false
	}
	return pullRequestRepository(issue) != "" &&
		pullRequestNumber(issue) > 0 &&
		strings.TrimSpace(pullRequest.HeadSHA) != ""
}

func mergeWorkerProgrammaticMergeWaiting(issue connector.Issue) bool {
	if strings.TrimSpace(issue.ID) == "" || issue.PullRequest == nil {
		return false
	}
	pullRequest := issue.PullRequest
	if pullRequestHydrationBlocksProgress(pullRequest) || mergeWorkerCIFailed(pullRequest) {
		return false
	}
	if normalizePullRequestState(pullRequest.State) != "open" || pullRequest.Draft {
		return false
	}
	mergeable := strings.ToLower(strings.TrimSpace(pullRequest.MergeableState))
	if mergeable != "" && mergeable != "clean" && mergeable != "unknown" && mergeable != "behind" && mergeable != "blocked" {
		return false
	}
	if mergeable == "blocked" && len(pullRequest.RunningChecks) == 0 && len(pullRequest.UnstartedChecks) == 0 {
		return false
	}
	if pullRequestRepository(issue) == "" || pullRequestNumber(issue) <= 0 || strings.TrimSpace(pullRequest.HeadSHA) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(pullRequest.CIStatus)) {
	case "", "pending", "running", "queued", "in_progress", "waiting":
		return true
	default:
		return false
	}
}

func mergeWorkerMissingRequiredChecks(issue connector.Issue) []string {
	if issue.PullRequest == nil {
		return nil
	}
	checks := make([]string, 0, len(issue.PullRequest.RequiredCheckFailures))
	seen := map[string]struct{}{}
	for _, check := range issue.PullRequest.RequiredCheckFailures {
		if !strings.EqualFold(strings.TrimSpace(check.Status), "missing") &&
			!strings.EqualFold(strings.TrimSpace(check.Conclusion), "missing") {
			continue
		}
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		checks = append(checks, name)
	}
	return checks
}

func mergeWorkerMissingRequiredChecksPropagating(issue connector.Issue, attempt int) bool {
	if len(mergeWorkerMissingRequiredChecks(issue)) == 0 || attempt >= maxMergeWorkerRunnerFailures {
		return false
	}
	if issue.PullRequest == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(issue.PullRequest.CIStatus)) {
	case "", "pending", "running", "queued", "in_progress", "waiting":
		return true
	default:
		return false
	}
}

func (o *Orchestrator) scheduleCITriggerLabel(ctx context.Context, issue connector.Issue, checkNames []string, attempt int, afterHeadPush bool, forceReapply bool) bool {
	cfg := gate.Effective(o.cfg.AutoPromote.Gate)
	label := strings.TrimSpace(cfg.CITriggerLabel)
	attrs := mergeWorkerLogAttrs(issue, "required_checks", strings.Join(checkNames, ","))
	if label == "" {
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "not_configured")...)
		}
		return false
	}
	if len(cfg.RequiredStatusChecks) > 0 {
		hydrator, ok := o.connector.(connector.PullRequestHydrator)
		if !ok {
			if o.logger != nil {
				o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "hydration_unsupported")...)
			}
			return false
		}
		refreshed, err := hydrator.HydratePullRequest(ctx, issue)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("ci_trigger_label_skipped", append(attrs, "reason", "pull_request_refresh_failed", "error", err)...)
			}
			return false
		}
		issue = refreshed
		attrs = mergeWorkerLogAttrs(issue, "required_checks", strings.Join(checkNames, ","))
		if pullRequestHydrationBlocksProgress(issue.PullRequest) {
			if o.logger != nil {
				o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "pull_request_hydration_unavailable")...)
			}
			return false
		}
	}
	checkStates, green := ciTriggerRequiredCheckStates(issue.PullRequest, cfg.RequiredStatusChecks)
	attrs = append(attrs, "required_check_states", checkStates, "after_head_push", afterHeadPush, "force_reapply", forceReapply)
	if green && !forceReapply {
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "required_checks_green")...)
		}
		return false
	}
	repository := pullRequestRepository(issue)
	number := pullRequestNumber(issue)
	headSHA := ""
	if issue.PullRequest != nil {
		headSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
	}
	attrs = append(attrs, "label", label)
	if repository == "" || number <= 0 || headSHA == "" {
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "pull_request_identity_incomplete")...)
		}
		return false
	}
	reapplier, ok := o.connector.(connector.PullRequestLabelReapplier)
	if !ok {
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "connector_unsupported")...)
		}
		return false
	}
	stagger := time.Duration(gate.DefaultCITriggerLabelStaggerSeconds) * time.Second
	if cfg.CITriggerLabelStaggerSeconds != nil {
		stagger = time.Duration(*cfg.CITriggerLabelStaggerSeconds) * time.Second
	}
	key := strings.ToLower(repository) + "#" + strconv.Itoa(number) + "|" + strings.ToLower(label)
	o.ciTriggerLabelMu.Lock()
	if o.ciTriggerLabelHeads == nil {
		o.ciTriggerLabelHeads = map[string]ciTriggerLabelHead{}
	}
	current, exists := o.ciTriggerLabelHeads[key]
	if exists && current.HeadSHA == headSHA && (current.Pending || !forceReapply) {
		o.ciTriggerLabelMu.Unlock()
		reason := "already_reapplied_for_head"
		if current.Pending {
			reason = "reapply_pending_for_head"
		}
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", reason)...)
		}
		return current.Pending
	}
	if !afterHeadPush && attempt >= maxMergeWorkerRunnerFailures {
		o.ciTriggerLabelMu.Unlock()
		if o.logger != nil {
			o.logger.Info("ci_trigger_label_skipped", append(attrs, "reason", "attempt_limit_reached", "attempt", attempt)...)
		}
		return false
	}
	o.ciTriggerLabelHeads[key] = ciTriggerLabelHead{HeadSHA: headSHA, Pending: true}
	o.ciTriggerLabelMu.Unlock()
	if o.logger != nil {
		reason := "required_checks_not_green"
		if forceReapply {
			reason = "forced_reapply"
		} else if afterHeadPush {
			reason = "after_head_push"
		}
		o.logger.Info("ci_trigger_label_scheduled", append(attrs, "reason", reason, "stagger", stagger, "attempt", attempt)...)
	}
	go o.reapplyMergeWorkerCITriggerLabel(ctx, reapplier, key, headSHA, repository, number, label, stagger, attrs)
	return true
}

func ciTriggerRequiredCheckStates(pr *connector.PullRequest, required []string) ([]connector.PullRequestCheck, bool) {
	required = gate.NormalizeRequiredStatusChecks(required)
	checks := make(map[string]connector.PullRequestCheck)
	if pr != nil {
		for _, check := range pr.Checks {
			checks[strings.TrimSpace(check.Name)] = check
		}
		for _, check := range pr.RequiredCheckFailures {
			checks[strings.TrimSpace(check.Name)] = check
		}
	}
	states := make([]connector.PullRequestCheck, 0, len(required))
	green := len(required) > 0
	for _, name := range required {
		check, ok := checks[name]
		if !ok {
			check = connector.PullRequestCheck{Name: name, Status: "missing", Conclusion: "missing"}
		}
		status := strings.ToLower(strings.TrimSpace(check.Status))
		if !strings.EqualFold(strings.TrimSpace(check.Conclusion), "success") ||
			(status != "success" && status != "completed") {
			green = false
		}
		states = append(states, check)
	}
	return states, green
}

func (o *Orchestrator) commentObservedLaneTransition(
	ctx context.Context,
	before connector.Issue,
	after connector.Issue,
	at time.Time,
) {
	if o == nil || strings.TrimSpace(after.ID) == "" {
		return
	}
	fromState := displayStateName(before.State)
	toState := displayStateName(after.State)
	if fromState == "" || toState == "" || normalizeState(fromState) == normalizeState(toState) {
		return
	}
	reason := "tracker_lane_transition"
	humanAction := ""
	blockers := []workpad.Blocker(nil)
	if signal := after.WorkpadSignal; signal != nil && signal.Source == workpad.SourceStructured && strings.TrimSpace(signal.Status) == workpad.StatusBlocked {
		reason = "workpad_blocked"
		humanAction = strings.TrimSpace(signal.HumanAction)
		blockers = signal.Blockers
	}
	if _, reasonCode, ok := evaluateStructuredBlockedRecovery(after, o.cfg.BlockedRecovery); ok {
		reason = reasonCode
	}
	o.recordLaneTransition(ctx, before, toState, at, reason, workflowLaneMetadata{
		Provenance: provenance.AttributionFromSource(provenance.SourceDetentAgentSession, provenance.Actor{}),
	})
	if o.connector == nil {
		return
	}
	var body strings.Builder
	body.WriteString("Observed this issue move from ")
	body.WriteString(fromState)
	body.WriteString(" to ")
	body.WriteString(toState)
	body.WriteString(" during worker completion.\n\n- source: tracker_refresh\n- reason: ")
	body.WriteString(reason)
	if humanAction != "" {
		body.WriteString("\n- human_action: ")
		body.WriteString(humanAction)
	}
	for _, blocker := range blockers {
		ref := strings.TrimSpace(blocker.Identifier)
		if ref == "" {
			ref = strings.TrimSpace(blocker.Ref)
		}
		detail := strings.TrimSpace(blocker.Reason)
		if ref == "" && detail == "" {
			continue
		}
		body.WriteString("\n- blocker: ")
		body.WriteString(ref)
		if ref != "" && detail != "" {
			body.WriteString(" — ")
		}
		body.WriteString(detail)
	}
	prURL := ""
	if after.PullRequest != nil {
		prURL = strings.TrimSpace(after.PullRequest.URL)
	}
	if prURL != "" {
		body.WriteString("\n- pull request: ")
		body.WriteString(prURL)
	}
	if err := o.connector.CreateComment(ctx, after.ID, body.String()); err != nil && o.logger != nil {
		o.logger.Warn("observed lane transition comment failed", "issue_id", after.ID, "identifier", after.Identifier, "from_state", fromState, "target_state", toState, "reason", reason, "error", err)
	}
}

func (o *Orchestrator) reapplyMergeWorkerCITriggerLabel(
	ctx context.Context,
	reapplier connector.PullRequestLabelReapplier,
	key string,
	headSHA string,
	repository string,
	number int,
	label string,
	stagger time.Duration,
	attrs []any,
) {
	err := reapplier.ReapplyPullRequestLabel(ctx, repository, number, label, stagger)
	o.ciTriggerLabelMu.Lock()
	current, exists := o.ciTriggerLabelHeads[key]
	if exists && current.HeadSHA == headSHA {
		if err != nil {
			delete(o.ciTriggerLabelHeads, key)
		} else {
			current.Pending = false
			o.ciTriggerLabelHeads[key] = current
		}
	}
	o.ciTriggerLabelMu.Unlock()
	if err != nil {
		if o.logger != nil {
			o.logger.Error("ci_trigger_label_failed", append(attrs, "stagger", stagger, "error", err)...)
		}
		return
	}
	if o.logger != nil {
		o.logger.Info("ci_trigger_label_reapplied", append(attrs, "stagger", stagger)...)
	}
}

func (o *Orchestrator) reworkMergeWorkerResult(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	issue connector.Issue,
	reason string,
	missingChecks []string,
	findings string,
) {
	issueID := strings.TrimSpace(event.IssueID)
	running.Issue = issue
	metadata := workflowLaneMetadata{}
	if event.Result.MergePrecheck != nil {
		metadata.LessonEvidence.ConflictPaths = event.Result.MergePrecheck.ConflictPaths
	}
	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issueID, issue, autoPromoteReworkState, event.CompletedAt, reason, metadata, laneMutationAcceptCompletion); err != nil {
		o.failProgrammaticMergeWorkerResult(ctx, state, event, running, "merge_worker_rework_failed", err)
		return
	}
	if comment := mergeWorkerReworkComment(issue, reason, missingChecks, findings); comment != "" {
		if err := o.connector.CreateComment(ctx, issueID, comment); err != nil && o.logger != nil {
			o.logger.Warn("merge worker rework comment failed", "issue_id", issueID, "reason", reason, "error", err)
		}
	}
	terminalState := store.WorkAttemptTerminalSuccess
	errorClass := ""
	errorMessage := ""
	if event.Err != nil {
		terminalState = store.WorkAttemptTerminalTimedOut
		errorClass = reason
		errorMessage = event.Err.Error()
	}
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, terminalState, event.Err, errorClass, errorMessage)
	o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, terminalState, errorClass, errorMessage, "rework", "merge worker routed current head to Rework")
	o.logMergeWorkerFailure(issue, reason, event.Err)
	o.recordMergeFailed(state, issue, event.CompletedAt, reason, event.Err)
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("abandon merge worker rework claim failed", "issue_id", issueID, "error", err)
	}
	o.clearAutoPromotedIssueDispatchMemory(state, issueID)
	if state.PriorAttempts == nil {
		state.PriorAttempts = map[string]runpkg.PriorAttempt{}
	}
	state.PriorAttempts[issueID] = runpkg.PriorAttempt{Source: "merge_worker", Reason: reason}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "merge_worker_routed_to_rework",
		Message: "routed merge worker to Rework for " + issueLabel(issue) + ": " + reason,
	})
}

func mergeWorkerReworkComment(issue connector.Issue, reason string, missingChecks []string, findings string) string {
	var b strings.Builder
	b.WriteString("Merge worker routed this issue from Merging to Rework.")
	b.WriteString("\n\n- reason: ")
	b.WriteString(reason)
	if len(missingChecks) > 0 {
		b.WriteString("\n- missing_required_checks: ")
		b.WriteString(strings.Join(missingChecks, ", "))
	}
	if findings = strings.TrimSpace(findings); findings != "" {
		b.WriteString("\n\nMerge-fallback findings:\n\n```text\n")
		b.WriteString(findings)
		b.WriteString("\n```")
	}
	if issue.PullRequest != nil {
		if reason == string(AutoPromoteReasonUnresolvedReviewThreads) && len(issue.PullRequest.UnresolvedReviewThreads) > 0 {
			b.WriteString("\n- unresolved_review_threads: ")
			b.WriteString(strconv.Itoa(len(issue.PullRequest.UnresolvedReviewThreads)))
			if location := pullRequestReviewThreadLocation(issue.PullRequest.UnresolvedReviewThreads[0]); location != "" {
				b.WriteString("\n- first_unresolved_review_thread: ")
				b.WriteString(location)
			}
		}
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			b.WriteString("\n- pull request: ")
			b.WriteString(url)
		}
		if headSHA := strings.TrimSpace(issue.PullRequest.HeadSHA); headSHA != "" {
			b.WriteString("\n- head_sha: ")
			b.WriteString(headSHA)
		}
		if mergeableState := strings.TrimSpace(issue.PullRequest.MergeableState); mergeableState != "" {
			b.WriteString("\n- mergeable_state: ")
			b.WriteString(mergeableState)
		}
	}
	if reason == string(AutoPromoteReasonUnresolvedReviewThreads) {
		b.WriteString("\n\nResolve the outstanding review threads, then complete the normal Rework gate.")
	} else {
		b.WriteString("\n\nRefresh or re-push the current PR head so required checks run, then complete the normal Rework gate.")
	}
	return b.String()
}

func mergeWorkerCIGreen(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success", "green", "pass", "passed":
		return true
	default:
		return false
	}
}

func (o *Orchestrator) handleIncompleteMergeWorkerResult(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) {
	err := errors.New(mergeWorkerTerminalStateMissing)
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalFailure, err, workAttemptErrorMergeIncomplete, err.Error())
	o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalFailure, workAttemptErrorMergeIncomplete, err.Error(), "merging", "merge worker completed without terminal state")
	o.logMergeWorkerFailure(running.Issue, "terminal_state_missing", err)
	o.recordMergeFailed(state, running.Issue, event.CompletedAt, "terminal_state_missing", err)
	attempt := nextAttempt(running.Attempt)
	if attempt > maxMergeWorkerRunnerFailures {
		if o.blockExhaustedMergeWorker(ctx, state, running, event.CompletedAt, mergeWorkerRetryExhaustedReason, attempt, err) {
			return
		}
	}
	o.scheduleRetry(
		state,
		running.Issue,
		attempt,
		event.CompletedAt,
		err.Error(),
		false,
		running.WorkerHost,
	)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "merge_worker_terminal_state_missing",
		Message: "merge worker completed without terminal state for " + issueLabel(running.Issue),
	})
}

func (o *Orchestrator) blockExhaustedMergeWorker(
	ctx context.Context,
	state *State,
	running Running,
	completedAt time.Time,
	reasonCode string,
	attempt int,
	err error,
) bool {
	issueID := strings.TrimSpace(running.Issue.ID)
	if issueID == "" || o.connector == nil {
		return false
	}
	reasonCode = strings.TrimSpace(reasonCode)
	if reasonCode == "" {
		reasonCode = mergeWorkerRetryExhaustedReason
	}
	reason := reasonCode
	if detail := errorString(err); detail != "" {
		reason += ": " + detail
	}
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		running.Issue,
		RunModeMerge,
		reason,
		blockedRecoveryPredicateFingerprintChange,
		autoPromoteReworkState,
		running.DiffStats,
	)
	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issueID, running.Issue, blockedStatusState, completedAt, reason, metadata, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"merge_worker_block_failed",
				"issue_id", issueID,
				"identifier", running.Issue.Identifier,
				"reason", reason,
				"target_state", blockedStatusState,
				"error", err,
			)
		}
		return false
	}
	if comment := mergeWorkerRetryExhaustedComment(running.Issue, reasonCode, attempt, err); strings.TrimSpace(comment) != "" {
		if err := o.connector.CreateComment(ctx, issueID, comment); err != nil && o.logger != nil {
			o.logger.Warn(
				"merge_worker_block_comment_failed",
				"issue_id", issueID,
				"identifier", running.Issue.Identifier,
				"reason", reason,
				"error", err,
			)
		}
	}
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("abandon exhausted merge worker claim failed", "issue_id", issueID, "error", err)
	}
	if state.MergeTimings[issueID].MergeFailedAt.IsZero() {
		o.recordMergeFailed(state, running.Issue, completedAt, reasonCode, err)
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
	delete(state.Completed, issueID)
	issue := cloneIssue(running.Issue)
	issue.State = blockedStatusState
	issue.BlockerReason = reason
	blockedAt := completedAt.UTC()
	issue.StageUpdatedAt = &blockedAt
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	state.Blocked[issueID] = Blocked{
		Issue:     issue,
		Reason:    reason,
		BlockedAt: completedAt,
		Source:    BlockedSourceProjectStatus,
		Recovery:  metadata.BlockedRecovery,
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      completedAt,
		Event:   reasonCode,
		Message: reasonCode + " for " + issueLabel(running.Issue) + ": " + errorString(err),
	})
	return true
}

func mergeWorkerRetryExhaustedComment(issue connector.Issue, reasonCode string, attempt int, err error) string {
	var b strings.Builder
	if reasonCode == mergeWorkerCurrentHeadCIWaitExceededReason {
		b.WriteString("The current-head CI wait exceeded its bounded deadline; parked this issue in Blocked to stop automatic redispatch.")
	} else {
		b.WriteString("Merge worker retries were exhausted; parked this issue in Blocked to stop automatic redispatch.")
	}
	b.WriteString("\n\n- reason: ")
	b.WriteString(reasonCode)
	if attempt > 0 {
		b.WriteString("\n- attempt: ")
		b.WriteString(strconv.Itoa(attempt))
	}
	if errText := errorString(err); errText != "" {
		b.WriteString("\n- error: ")
		b.WriteString(errText)
	}
	if issue.PullRequest != nil {
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			b.WriteString("\n- pull request: ")
			b.WriteString(url)
		}
		if mergeableState := strings.ToLower(strings.TrimSpace(issue.PullRequest.MergeableState)); mergeableState != "" {
			b.WriteString("\n- mergeable_state: ")
			b.WriteString(mergeableState)
		}
		if ciStatus := strings.TrimSpace(issue.PullRequest.CIStatus); ciStatus != "" {
			b.WriteString("\n- ci_status: ")
			b.WriteString(ciStatus)
		}
	}
	b.WriteString("\n\nResolve the merge failure, then move the issue back to Merging to retry.")
	return b.String()
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func (o *Orchestrator) completePlanRunning(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) {
	cfg := gate.EffectivePlan(o.cfg.Plan)
	issueID := strings.TrimSpace(event.IssueID)
	issue := cloneIssue(running.Issue)
	body := planArtifactComment(issue, event.Result.Output)
	if err := o.connector.CreateComment(ctx, issueID, body); err != nil {
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalFailure, err, "plan_comment_failed", err.Error())
		o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalFailure, "plan_comment_failed", err.Error(), "reviewing", "plan comment failed")
		o.scheduleRetry(state, issue, nextAttempt(running.Attempt), event.CompletedAt, "plan comment failed: "+err.Error(), false, running.WorkerHost)
		return
	}
	if err := o.updateIssueStateByID(ctx, state, issueID, issue, cfg.Stop, event.CompletedAt, "plan_artifact_created", laneMutationAcceptCompletion); err != nil {
		o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalFailure, err, "plan_transition_failed", err.Error())
		o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalFailure, "plan_transition_failed", err.Error(), "reviewing", "plan review transition failed")
		o.scheduleRetry(state, issue, nextAttempt(running.Attempt), event.CompletedAt, "plan review transition failed: "+err.Error(), false, running.WorkerHost)
		return
	}
	o.recordProjectAttemptOutcome(state, event.IssueID, event.CompletedAt, store.WorkAttemptTerminalSuccess, nil, "", "")
	o.completeDurableWorkAttempt(ctx, state, running, event.CompletedAt, store.WorkAttemptTerminalSuccess, "", "", "completed", "plan review created")
	resetWorkerFailureBreakers(state, issueID)
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("abandon completed plan claim failed", "issue_id", issueID, "error", err)
	}
	delete(state.planRework, issueID)
	issue.State = cfg.Stop
	state.Completed[issueID] = Completed{
		Issue:           issue,
		SessionID:       running.SessionID,
		StartedAt:       running.StartedAt,
		CompletedAt:     event.CompletedAt,
		FinalState:      cfg.Stop,
		Tokens:          event.Result.Tokens,
		RuntimeIdentity: running.RuntimeIdentity,
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, event.Result.Tokens)
	if event.Result.RateLimits != nil {
		state.RateLimits = mergeRateLimits(state.RateLimits, event.Result.RateLimits)
	}
	if diffStatsPresent(event.Result.DiffStats) {
		state.DiffStats[issueID] = event.Result.DiffStats
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   "plan_review_created",
		Message: "created plan artifact for " + issueLabel(issue) + " and moved to " + cfg.Stop,
	})
}

func (o *Orchestrator) scheduleRetry(
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	err string,
	continuation bool,
	workerHost string,
) {
	o.dispatchPlanner().scheduleRetry(state, issue, attempt, now, err, continuation, workerHost)
}

func (o *Orchestrator) scheduleContinuationRetry(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	err string,
	workerHost string,
) {
	if releaseErr := o.abandonClaim(ctx, issue.ID); releaseErr != nil {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      cleanupEventAt(now),
			Event:   "claim_release_failed",
			Message: fmt.Sprintf("claim lease release failed for %s: %v", issueLabel(issue), releaseErr),
		})
	}
	o.scheduleRetry(state, issue, attempt, now, err, true, workerHost)
	delete(state.Claimed, issue.ID)
}

func (o *Orchestrator) scheduleRetryAfter(
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	delay time.Duration,
	err string,
	workerHost string,
) {
	o.dispatchPlanner().scheduleRetryAfter(state, issue, attempt, now, delay, err, workerHost)
}

func (o *Orchestrator) retryDelay(attempt int, continuation bool) time.Duration {
	return o.dispatchPlanner().retryDelay(attempt, continuation)
}

func (o *Orchestrator) releaseClaim(state *State, issueID string) {
	o.cancelRunning(state, issueID)
	o.heartbeats.remove(issueID)
	delete(state.Running, issueID)
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
}

func (o *Orchestrator) completeTerminalRunning(
	ctx context.Context,
	state *State,
	issueID string,
	running Running,
	completedAt time.Time,
	tokens TokenTotals,
) {
	o.heartbeats.remove(issueID)
	o.clearMergeRequiredCheckStreaks(ctx, running.Issue)
	o.completeDurableWorkAttempt(ctx, state, running, completedAt, store.WorkAttemptTerminalSuccess, "", "", "completed", "worker reached terminal state")
	o.releaseGlobalDispatchSlot(running.globalSlot)
	if running.cancel != nil {
		running.cancel()
	}
	delete(state.Running, issueID)
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
	delete(state.InstantFailures, issueID)
	delete(state.RepeatedFailures, issueID)
	if err := o.abandonClaim(ctx, issueID); err != nil {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      cleanupEventAt(completedAt),
			Event:   "claim_release_failed",
			Message: fmt.Sprintf("claim lease release failed for %s: %v", issueLabel(running.Issue), err),
		})
	}
	issue := o.ensureClosedCompletedRunningIssueDone(ctx, state, issueID, running.Issue, completedAt)
	finalState := strings.TrimSpace(issue.State)
	if finalState == "" {
		finalState = FinalStateCompleted
	}
	mergeTiming := MergeTiming{}
	if mergeWorkerIssue(running.Issue) {
		mergeTiming = o.recordMergeCompleted(state, running.Issue, completedAt, finalState)
	}
	o.recordEfficiencyReceipt(ctx, issue, completedAt)
	state.Completed[issueID] = Completed{
		Issue:           cloneIssue(issue),
		SessionID:       running.SessionID,
		StartedAt:       running.StartedAt,
		CompletedAt:     completedAt,
		FinalState:      finalState,
		Tokens:          tokens,
		MergeTiming:     mergeTiming,
		RuntimeIdentity: running.RuntimeIdentity,
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, tokens)
	if diffStatsPresent(running.DiffStats) {
		state.DiffStats[issueID] = running.DiffStats
	}
	if mergeWorkerIssue(running.Issue) {
		o.logMergeWorkerSuccess(running.Issue, finalState)
	}
	o.reapWorkspace(ctx, state, issue, workspaceReapReason(issue, o.cfg.TerminalStates), completedAt)
}

func (o *Orchestrator) recordEfficiencyReceipt(ctx context.Context, issue connector.Issue, completedAt time.Time) {
	if o.efficiency == nil || normalizeState(issue.State) != normalizeState(doneStateName(o.cfg.TerminalStates)) {
		return
	}
	receipt, err := o.efficiency.CompleteEfficiencyReceipt(ctx, efficiency.Completion{
		ProjectID:   o.workflowMetricsProjectID(),
		IssueID:     issue.ID,
		Identifier:  issue.Identifier,
		IssueURL:    issue.URL,
		PRNumber:    workflowMetricsPRNumber(issue),
		CompletedAt: completedAt,
		Thresholds:  o.cfg.EfficiencyThresholds,
	})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("record efficiency receipt failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return
	}
	if o.lifecycleExporter == nil {
		return
	}
	if err := o.lifecycleExporter.ExportLifecycle(ctx, receipt); err != nil && o.logger != nil {
		o.logger.Warn("export efficiency lifecycle failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
}

func (o *Orchestrator) refreshEfficiencyReceipt(ctx context.Context, issue connector.Issue, observedAt time.Time) {
	recorder, ok := o.efficiency.(efficiency.LiveRecorder)
	if !ok || recorder == nil || stateIn(issue.State, o.cfg.TerminalStates) {
		return
	}
	_, _, err := recorder.RefreshEfficiencyReceipt(ctx, efficiency.Observation{
		ProjectID:               o.workflowMetricsProjectID(),
		IssueID:                 issue.ID,
		Identifier:              issue.Identifier,
		IssueURL:                issue.URL,
		PRNumber:                workflowMetricsPRNumber(issue),
		ObservedAt:              observedAt,
		RefreshIntervalSessions: 5,
		Thresholds:              o.cfg.EfficiencyThresholds,
	})
	if err != nil && o.logger != nil {
		o.logger.Warn("refresh live efficiency receipt failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
	}
}

func (o *Orchestrator) ensureClosedCompletedRunningIssueDone(ctx context.Context, state *State, issueID string, issue connector.Issue, now time.Time) connector.Issue {
	if !issue.Closed || !closedReasonCompleted(issue.ClosedReason) {
		return issue
	}
	targetState := doneStateName(o.cfg.TerminalStates)
	if strings.TrimSpace(targetState) == "" {
		return issue
	}
	if err := o.updateIssueStateByID(ctx, state, issueID, issue, targetState, now, "closed_completed_running_done", laneMutationAcceptCompletion); err != nil {
		if o.logger != nil {
			o.logger.Warn("mark closed completed running issue done failed", "issue_id", issueID, "identifier", issue.Identifier, "from_state", issue.State, "target_state", targetState, "error", err)
		}
		return issue
	}
	if o.logger != nil {
		o.logger.Info("marked closed completed running issue done", "issue_id", issueID, "identifier", issue.Identifier, "from_state", issue.State, "target_state", targetState)
	}
	issue.State = targetState
	return issue
}

func terminalCompletedAt(issue connector.Issue, terminalStates []string, fallback time.Time) time.Time {
	if stateIn(issue.State, terminalStates) && issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() {
		return *issue.StageUpdatedAt
	}
	if issue.UpdatedAt != nil && !issue.UpdatedAt.IsZero() {
		return *issue.UpdatedAt
	}
	if issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() {
		return *issue.StageUpdatedAt
	}
	if !fallback.IsZero() {
		return fallback
	}
	return time.Now().UTC()
}

func (o *Orchestrator) cancelRunning(state *State, issueID string) {
	running, ok := state.Running[issueID]
	if !ok {
		return
	}
	o.releaseGlobalDispatchSlot(running.globalSlot)
	running.globalSlot = scheduler.Slot{}
	state.Running[issueID] = running
	cancelRunning(state, issueID)
}

func cancelRunning(state *State, issueID string) {
	running, ok := state.Running[issueID]
	if !ok || running.cancel == nil {
		return
	}
	running.cancel()
	running.cancel = nil
	state.Running[issueID] = running
}

func (o *Orchestrator) releaseRunningSlots(state *State) {
	for issueID, running := range state.Running {
		running.progress.close()
		o.releaseGlobalDispatchSlot(running.globalSlot)
		running.globalSlot = scheduler.Slot{}
		state.Running[issueID] = running
	}
}
