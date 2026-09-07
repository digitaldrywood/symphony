package orchestrator

import (
	"context"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/activity"
	"github.com/digitaldrywood/detent/internal/budget"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/dispatchpriority"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func (o *Orchestrator) dispatchPlanner() dispatchPlanner {
	return newDispatchPlanner(o.cfg)
}

func (o *Orchestrator) pruneBudgetRefusals(ctx context.Context, state *State, now time.Time) {
	o.dispatchPlanner().pruneBudgetRefusals(
		state,
		now,
		o.currentDailyBudgetStatus(ctx, state, now),
		o.currentIssueBudgetStatuses(ctx, state),
	)
}

func (o *Orchestrator) currentIssueBudgetStatuses(ctx context.Context, state *State) map[string]IssueBudgetStatus {
	if o.issueBudgetStatus == nil {
		return nil
	}

	statuses := make(map[string]IssueBudgetStatus)
	for issueID, refusal := range state.BudgetRefusals {
		if refusal.Code != string(budget.ReasonPerIssueMaxUSD) {
			continue
		}
		status, known, err := o.issueBudgetStatus.IssueBudgetStatus(ctx, refusal.Issue)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("per-issue budget hold re-evaluation failed", "issue_id", issueID, "error", err)
			}
			continue
		}
		if known {
			statuses[issueID] = status
		}
	}
	return statuses
}

func (o *Orchestrator) currentDailyBudgetStatus(ctx context.Context, state *State, now time.Time) *DailyBudgetStatus {
	if o.dailyBudgetStatus == nil || !hasActiveDailyBudgetRefusal(state, now) {
		return nil
	}

	status, known, err := o.dailyBudgetStatus.DailyBudgetStatus(ctx, now)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("daily budget refusal re-evaluation failed", "error", err)
		}
		return nil
	}
	if !known {
		return nil
	}
	return &status
}

func hasActiveDailyBudgetRefusal(state *State, now time.Time) bool {
	for _, refusal := range state.BudgetRefusals {
		if refusal.Code == "per_day_max_usd" && refusal.ResetAt != nil && now.Before(*refusal.ResetAt) {
			return true
		}
	}
	return false
}

func (o *Orchestrator) selectWorkerHost(state *State, preferredWorkerHost string) (string, bool) {
	return o.dispatchPlanner().selectWorkerHost(state, preferredWorkerHost)
}

func leastLoadedWorkerHost(state *State, hosts []string) string {
	selected := hosts[0]
	selectedCount := runningWorkerHostCount(state, selected)
	for _, host := range hosts[1:] {
		count := runningWorkerHostCount(state, host)
		if count < selectedCount {
			selected = host
			selectedCount = count
		}
	}
	return selected
}

func runningWorkerHostCount(state *State, workerHost string) int {
	count := 0
	for _, running := range state.Running {
		if running.WorkerHost == workerHost {
			count++
		}
	}
	return count
}

func normalizeWorkerHosts(hosts []string) []string {
	normalized := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		normalized = append(normalized, host)
	}
	return normalized
}

func (o *Orchestrator) dispatchReadyIssues(ctx context.Context, state *State, issues []connector.Issue, now time.Time) {
	o.beginGlobalProjectCycle()
	defer o.endGlobalProjectCycle()
	if state.Draining || o.dispatchQuiesced() {
		return
	}
	issues = o.filterImplementDependencyDeferrals(ctx, issues)
	o.enforceLifetimeLimits(ctx, state, issues, now)
	o.observePullRequestHydrationRecovery(state, issues, now)
	planner := o.dispatchPlanner()
	o.logOwnershipEligibilityStartup(planner, issues)
	var lastDispatchFailure string
	decisions := make([]dispatchPlanDecision, 0, len(issues))
	outcomes := make(map[string]dispatchIssueOutcome, len(issues))
	selections := make(map[string]dispatchPlanDecision, len(issues))
	planner.plan(state, issues, now, dispatchPlanHooks{
		hydrate: func(issue connector.Issue) (connector.Issue, bool) {
			return o.hydrateDispatchIssue(ctx, state, issue, now)
		},
		beforeDispatch: func(_ connector.Issue, continuationIndex int) bool {
			if continuationIndex < 0 {
				return true
			}
			return waitForDispatchBackoff(ctx, continuationDelay(continuationIndex))
		},
		dispatch: func(action dispatchAction) bool {
			outcome := o.dispatchIssueWithAction(ctx, state, action, now)
			if identity := workflowIssueIdentityKey(action.issue); identity != "" {
				outcomes[identity] = outcome
				if !outcome.dispatched {
					if selection, ok := selections[identity]; ok {
						o.recordPostSelectionDispatchRefusal(ctx, state, now, selection, outcome)
					}
				}
			}
			if !outcome.dispatched {
				lastDispatchFailure = outcome.reason
			} else {
				lastDispatchFailure = ""
			}
			return outcome.dispatched
		},
		dispatchFailed: func(issue connector.Issue) bool {
			return !mergeWorkerIssue(issue) || lastDispatchFailure != dispatchIssueFailureGlobalSlotUnavailable
		},
		retryDispatchFailed: func(issue connector.Issue, retry Retry) {
			releaseForgeAvailabilityProbe(state, issue.ID, "deferred", dispatchFailureRetryReason(lastDispatchFailure), now)
			releaseWorkerGitHubMonitorProbe(state, issue.ID, "deferred", dispatchFailureRetryReason(lastDispatchFailure), now)
			planner.scheduleRetry(state, issue, retry.Attempt, now, dispatchFailureRetryReason(lastDispatchFailure), false, retry.WorkerHost)
			rescheduled := state.Retry[issue.ID]
			rescheduled.RetryMode = retry.RetryMode
			rescheduled.ResumeState = retry.ResumeState
			rescheduled.MergePrecheck = cloneMergePrecheck(retry.MergePrecheck)
			rescheduled.ForgeUnavailable = retry.ForgeUnavailable
			rescheduled.ForgeHost = retry.ForgeHost
			rescheduled.ForgeRetry = cloneForgeRetry(retry.ForgeRetry)
			rescheduled.GitHubMonitor = retry.GitHubMonitor
			rescheduled.GitHubCredential = retry.GitHubCredential
			rescheduled.Wait = retry.Wait
			rescheduled.Wait.PendingChecks = append([]string(nil), retry.Wait.PendingChecks...)
			state.Retry[issue.ID] = rescheduled
		},
		pollRetryWait: func(issue connector.Issue, retry Retry) (Retry, bool, string) {
			if retry.Wait.Kind == retryWaitWorkspaceBranchHeld {
				return o.pollWorkspaceBranchHold(ctx, state, issue, retry, now)
			}
			return o.pollMergeWorkerCurrentHeadCI(ctx, state, issue, retry, now)
		},
		preserveMissingDueRetry: func(retry Retry) bool {
			return o.preserveMissingDueRetry(state, retry)
		},
		decision: func(decision dispatchPlanDecision) {
			decisions = append(decisions, decision)
			o.logDispatchPlanDecision(ctx, state, now, decision)
			if decision.Selected {
				if identity := workflowIssueIdentityKey(decision.Issue); identity != "" {
					selections[identity] = decision
				}
			}
		},
	})
	o.releaseDeferredSchedulingClaims(ctx, state, issues)
	o.observeProjectDispatchStatus(ctx, state, issues, decisions, outcomes, now)
}

func (o *Orchestrator) releaseDeferredSchedulingClaims(ctx context.Context, state *State, issues []connector.Issue) {
	if o.scheduling == nil {
		return
	}
	for _, issue := range issues {
		if _, running := state.Running[issue.ID]; running {
			continue
		}
		if err := o.scheduling.ReleaseClaim(ctx, issue.ID, "dispatch_deferred"); err != nil && o.logger != nil {
			o.logger.Warn("release deferred Hub claim failed", "issue_id", issue.ID, "error", err)
		}
	}
}

func (o *Orchestrator) logOwnershipEligibilityStartup(planner dispatchPlanner, issues []connector.Issue) {
	if o == nil || o.ownershipStartupLogged || o.logger == nil || !planner.cfg.Claiming.OwnershipSet || planner.cfg.Claiming.OwnershipMode != workflowconfig.IdentityOwnershipAssignee {
		return
	}
	o.ownershipStartupLogged = true
	candidates := planner.assigneeEligibilityCandidates(issues)
	identifiers := make([]string, 0, len(candidates))
	for _, issue := range candidates {
		identifier := strings.TrimSpace(issue.Identifier)
		if identifier == "" {
			identifier = strings.TrimSpace(issue.ID)
		}
		identifiers = append(identifiers, identifier)
	}
	message := "ownership eligibility compatibility grace active"
	if planner.cfg.Claiming.AssigneeRequired {
		message = "ownership eligibility enforcement active"
	}
	o.logger.Warn(
		message,
		"project_id", strings.TrimSpace(o.projectID),
		"config_key", "identity.assignee_required",
		"ownership_mode", workflowconfig.IdentityOwnershipAssignee,
		"enforced", planner.cfg.Claiming.AssigneeRequired,
		"blocked_issue_count", len(candidates),
		"affected_issues", strings.Join(identifiers, ","),
	)
}

func dispatchFailureRetryReason(reason string) string {
	switch reason {
	case dispatchIssueFailureClaimFailed:
		return "claim verification failed"
	case dispatchIssueFailureGlobalSlotUnavailable:
		return dispatchSkipGlobalCapacityFull
	default:
		return reason
	}
}

func (o *Orchestrator) preserveMissingDueRetry(state *State, retry Retry) bool {
	if normalizeState(retry.Issue.State) != normalizeState(autoPromoteMergingState) {
		return false
	}
	return !o.mergeWorkerLocalSlotsAvailable(state)
}

func (o *Orchestrator) hydrateDispatchIssue(ctx context.Context, state *State, issue connector.Issue, now time.Time) (connector.Issue, bool) {
	if strings.TrimSpace(issue.ID) == "" || len(issue.Fields) > 0 || o.connector == nil {
		return issue, true
	}
	if retry, ok := state.Retry[issue.ID]; ok {
		if _, outage, active := matchingBackendOutage(state.BackendOutages, retry.CapacityScope); active {
			probeAt := outage.NextProbeAt
			if probeAt.IsZero() {
				probeAt = outage.ResumeAt
			}
			if strings.TrimSpace(outage.ProbeIssueID) != "" || now.Before(probeAt) {
				return issue, true
			}
		}
	}
	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{issue.ID})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("hydrate dispatch issue failed", "issue_id", issue.ID, "error", err)
		}
		return connector.Issue{}, false
	}
	for _, hydrated := range issues {
		if hydrated.ID == issue.ID {
			return mergeIssueTrackerFields(issue, hydrated), true
		}
	}
	return connector.Issue{}, false
}

func (o *Orchestrator) dispatchCandidates(ctx context.Context, state *State, issues []connector.Issue, now time.Time) {
	o.beginGlobalProjectCycle()
	defer o.endGlobalProjectCycle()
	if state.Draining || o.dispatchQuiesced() {
		return
	}
	issues = o.filterImplementDependencyDeferrals(ctx, issues)
	o.enforceLifetimeLimits(ctx, state, issues, now)
	for _, issue := range issues {
		if o.dispatchPlanner().hardAvailableSlots(state) == 0 {
			return
		}
		issue, ok := o.hydrateDispatchIssue(ctx, state, issue, now)
		if !ok {
			continue
		}
		if !o.dispatchable(issue, state, now) {
			continue
		}

		o.dispatchIssue(ctx, state, issue, 0, now, "")
	}
}

func dueRetriesByIssue(state *State, now time.Time) map[string]Retry {
	retries := make(map[string]Retry, len(state.Retry))
	for _, retry := range state.Retry {
		if !retry.DueAt.After(now) {
			retries[retry.Issue.ID] = retry
		}
	}
	return retries
}

func (o *Orchestrator) dispatchable(issue connector.Issue, state *State, now time.Time) bool {
	return o.dispatchPlanner().dispatchable(issue, state, now)
}

const (
	dispatchIssueFailureDraining              = "dispatch_draining"
	dispatchIssueFailureLocalSlotUnavailable  = "local_slot_unavailable"
	dispatchIssueFailureWorkerHostUnavailable = "worker_host_unavailable"
	dispatchIssueFailureGlobalSlotUnavailable = "global_slot_unavailable"
	dispatchIssueFailureClaimFailed           = "claim_failed"
	dispatchIssueFailureWorkAttemptStart      = "work_attempt_start_failed"
	dispatchIssueFailureStartStateTransition  = "start_state_transition_failed"
	dispatchIssueFailureBackendCapacityPaused = "backend_capacity_paused"
	dispatchIssueFailureGitHubRESTPaused      = "github_rest_capacity_paused"
	dispatchIssueFailureGitHubLookupPaused    = "github_lookup_backoff"
	dispatchIssueFailureTrackerUnavailable    = "tracker_unavailable"
	dispatchIssueFailureForgeUnavailable      = "forge_unavailable"
	dispatchIssueFailureGitHubMonitor         = "worker_github_budget_monitor_unavailable"
	dispatchIssueFailureCIUnavailable         = "ci_unavailable"
	dispatchIssueFailureMemoryPressure        = "memory_pressure_high"
	dispatchIssueFailureIOPressure            = "io_pressure_high"
	dispatchIssueFailureCPUPressure           = "cpu_pressure_high"
	dispatchIssueFailureRecoveryRamp          = "dispatch_recovery_ramp"
)

type dispatchIssueOutcome struct {
	dispatched bool
	reason     string
	waitReason string
}

func (o *Orchestrator) dispatchIssue(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	preferredWorkerHost string,
) bool {
	return o.dispatchIssueWithOutcome(ctx, state, issue, attempt, now, preferredWorkerHost).dispatched
}

func (o *Orchestrator) dispatchIssueWithAction(
	ctx context.Context,
	state *State,
	action dispatchAction,
	now time.Time,
) dispatchIssueOutcome {
	return o.dispatchIssueWithAdmission(
		ctx,
		state,
		action.issue,
		action.attempt,
		now,
		action.workerHost,
		action.modelPermitRequired,
		action.retryState,
	)
}

func (o *Orchestrator) dispatchIssueWithOutcome(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	preferredWorkerHost string,
) dispatchIssueOutcome {
	return o.dispatchIssueWithAdmission(
		ctx,
		state,
		issue,
		attempt,
		now,
		preferredWorkerHost,
		o.dispatchPlanner().modelPermitRequiredAtDispatch(issue),
		nil,
	)
}

func (o *Orchestrator) dispatchIssueWithAdmission(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	preferredWorkerHost string,
	modelPermitRequired bool,
	retryState *Retry,
) dispatchIssueOutcome {
	if err := o.checkDispatchPolicy(ctx); err != nil {
		return dispatchIssueOutcome{reason: "policy_mismatch", waitReason: err.Error()}
	}
	if reason := connector.NonExecutableReason(issue); reason != "" {
		return dispatchIssueOutcome{reason: dispatchSkipInactiveState, waitReason: reason}
	}
	if reason := humanDependencyWaitReason(issue.BlockedBy); reason != "" {
		return dispatchIssueOutcome{reason: dispatchSkipBlockedByDependency, waitReason: reason}
	}
	if !o.beginDispatchStart() {
		return dispatchIssueOutcome{reason: dispatchIssueFailureDraining}
	}
	defer o.finishDispatchStart()
	if state.Draining {
		return dispatchIssueOutcome{reason: dispatchIssueFailureDraining}
	}
	if _, deferred := state.deferredCompletions[issue.ID]; deferred {
		return dispatchIssueOutcome{reason: dispatchSkipCompletionDeferred}
	}
	queuedRetry, retryQueued := state.Retry[issue.ID]
	if retryState != nil {
		queuedRetry = *retryState
		retryQueued = true
	}
	if activeTrackerUnavailable(state) && (trackerDependentDispatch(issue) || trackerUnavailableRetry(state, issue.ID)) {
		return dispatchIssueOutcome{reason: dispatchIssueFailureTrackerUnavailable}
	}
	if activeCIUnavailable(state) && (ciDependentDispatch(issue) || ciUnavailableRetry(state, issue.ID)) {
		return dispatchIssueOutcome{reason: dispatchIssueFailureCIUnavailable}
	}
	if forgeAvailabilityBlocks(state, issue, queuedRetry, o.cfg.ForgeHost, now) {
		return dispatchIssueOutcome{reason: dispatchIssueFailureForgeUnavailable}
	}
	if workerGitHubMonitorBlocks(state, issue.ID, queuedRetry, now) {
		return dispatchIssueOutcome{reason: dispatchIssueFailureGitHubMonitor}
	}
	if _, paused := activeGitHubRESTCapacityOutage(state, now); paused {
		return dispatchIssueOutcome{reason: dispatchIssueFailureGitHubRESTPaused}
	}
	if !projectFailureBreakerAllowsDispatch(state, now) {
		return dispatchIssueOutcome{reason: projectFailureBreakerDispatchPaused}
	}
	o.observeHostPressure(ctx, state, o.clockNow())
	if state.MemoryPressure.DispatchHeld {
		return dispatchIssueOutcome{reason: dispatchIssueFailureMemoryPressure}
	}
	if state.IOPressure.DispatchHeld {
		return dispatchIssueOutcome{
			reason:     dispatchIssueFailureIOPressure,
			waitReason: hostPressureWaitReason(state, dispatchIssueFailureIOPressure),
		}
	}
	if state.CPUPressure.DispatchHeld {
		return dispatchIssueOutcome{
			reason:     dispatchIssueFailureCPUPressure,
			waitReason: hostPressureWaitReason(state, dispatchIssueFailureCPUPressure),
		}
	}
	pressureConstraint, pressureConstrained := activeHostPressureConstraint(state)
	if reason := dispatchRecoveryBlockReason(state, now); reason != "" {
		return dispatchIssueOutcome{reason: reason}
	}
	runMode := o.dispatchMode(ctx, state, issue)
	capacityRequest := runpkg.RunRequest{Issue: issue, Mode: runMode, SelectorContext: o.selectorContext()}
	capacityScope, capacityProbeKey, capacityPaused := o.backendCapacityDispatch(state, capacityRequest, now)
	if o.scheduling == nil && !githubLookupBackoffAllowsDispatch(state, capacityProbeKey) {
		return dispatchIssueOutcome{reason: dispatchIssueFailureGitHubLookupPaused}
	}
	if capacityPaused {
		return dispatchIssueOutcome{reason: dispatchIssueFailureBackendCapacityPaused}
	}
	targetState := dispatchStartTransitionState(issue, runMode, o.cfg.ActiveStates)
	slotIssue := issue
	if targetState != "" {
		slotIssue = cloneIssue(issue)
		slotIssue.State = targetState
		if !o.dispatchPlanner().slotsAvailableForModelRequirement(slotIssue, state, preferredWorkerHost, modelPermitRequired) {
			return dispatchIssueOutcome{reason: dispatchIssueFailureLocalSlotUnavailable}
		}
	}
	projectStats := o.projectStateSlotStats(slotIssue, state)

	workerHost, ok := o.selectWorkerHost(state, preferredWorkerHost)
	if !ok {
		o.logMergeWorkerFailure(issue, "worker_host_unavailable", nil)
		o.recordMergeFailed(state, issue, now, "worker_host_unavailable", nil)
		return dispatchIssueOutcome{reason: dispatchIssueFailureWorkerHostUnavailable}
	}

	pressureCapacity := 0
	if pressureConstrained {
		pressureCapacity = pressureConstraint.capacity
	}
	globalSlot, ok, decision := o.acquireGlobalDispatchSlot(ctx, slotIssue, workerHost, now, pressureCapacity)
	if !ok {
		o.recordDispatchGateRefusal(ctx, state, issue, attempt, workerHost, now, decision, projectStats)
		o.logSchedulerSlotDecision(issue, "waiting", decision, projectStats)
		if mergeWorkerIssue(issue) {
			o.logMergeBaseRefreshDeferred(issue, mergeBaseRefreshGlobalUnavailable)
			o.logMergeWorkerSlotWait(issue, decision, projectStats)
		} else {
			o.logDispatchSlotWait(issue, decision, projectStats)
			o.recordDispatchSlotWait(state, issue, decision, projectStats, now)
		}
		if decision.Reason == scheduler.DispatchGateReasonPressureCapacityFull && pressureConstrained {
			return dispatchIssueOutcome{
				reason:     pressureConstraint.reason,
				waitReason: hostPressureWaitReason(state, pressureConstraint.reason),
			}
		}
		return dispatchIssueOutcome{reason: dispatchIssueFailureGlobalSlotUnavailable}
	}
	runCtx := ctx
	cancelDurationLimit := func() {}
	if mergeWorkerIssue(slotIssue) {
		limit := o.mergeWorkerLimit
		if limit == nil {
			limit = context.WithTimeoutCause
		}
		runCtx, cancelDurationLimit = limit(
			ctx,
			o.cfg.MergeWorkerMaxDuration,
			runpkg.ErrMergeWorkerDurationExceeded,
		)
	}
	durationLimitTransferred := false
	defer func() {
		if !durationLimitTransferred {
			cancelDurationLimit()
		}
	}()
	mergeTiming := o.markMergeWorkerSlotAcquired(state, issue, now)
	o.logSchedulerSlotDecision(issue, "acquired", decision, projectStats)
	o.logMergeWorkerSlotAcquired(issue, decision, projectStats, mergeTiming)
	o.logWorkerLifecycle(issue, "worker_slot_acquired",
		"attempt", attempt,
		"worker_host", strings.TrimSpace(workerHost),
	)
	recovery, allowed, recoveryReason := tryReserveDispatchRecovery(state, issue.ID, now)
	if !allowed {
		o.releaseGlobalDispatchSlot(globalSlot)
		if recoveryReason == "" {
			recoveryReason = dispatchIssueFailureRecoveryRamp
		}
		return dispatchIssueOutcome{reason: recoveryReason}
	}
	canary, allowed := tryReserveProjectFailureBreakerCanary(state, issue.ID, now)
	if !allowed {
		if recovery {
			releaseDispatchRecoveryAdmission(state, issue.ID)
		}
		o.releaseGlobalDispatchSlot(globalSlot)
		return dispatchIssueOutcome{reason: projectFailureBreakerDispatchPaused}
	}

	claimedIssue, claim, ok := o.claimIssue(runCtx, issue, now)
	if !ok {
		if recovery {
			releaseDispatchRecoveryAdmission(state, issue.ID)
		}
		if canary {
			releaseProjectFailureBreakerCanary(state, issue.ID)
		}
		o.releaseGlobalDispatchSlot(globalSlot)
		o.logWorkerLifecycle(issue, "worker_capacity_released",
			"attempt", attempt,
			"worker_host", strings.TrimSpace(workerHost),
			"reason", dispatchIssueFailureClaimFailed,
		)
		o.logMergeWorkerFailure(issue, "claim_failed", nil)
		o.recordMergeFailed(state, issue, now, "claim_failed", nil)
		return dispatchIssueOutcome{reason: dispatchIssueFailureClaimFailed}
	}

	issue = cloneIssue(claimedIssue)
	priorAttempt := state.PriorAttempts[issue.ID]
	if !priorAttemptPresent(priorAttempt) {
		if breakerAttempt, ok := o.spendProgressPriorAttempt(runCtx, issue); ok {
			priorAttempt = breakerAttempt
		}
	}
	dispatchLoopStart := newDispatchLoopStartRecord(issue, runMode)
	workAttemptID, ok := o.startDurableWorkAttempt(runCtx, state, issue, attempt, now, workerHost, runMode, dispatchLoopStart)
	if !ok {
		if recovery {
			releaseDispatchRecoveryAdmission(state, issue.ID)
		}
		if canary {
			releaseProjectFailureBreakerCanary(state, issue.ID)
		}
		o.releaseGlobalDispatchSlot(globalSlot)
		o.logWorkerLifecycle(issue, "worker_capacity_released",
			"attempt", attempt,
			"worker_host", strings.TrimSpace(workerHost),
			"reason", dispatchIssueFailureWorkAttemptStart,
		)
		if abandonErr := o.abandonClaim(ctx, issue.ID); abandonErr != nil && o.logger != nil {
			o.logger.Warn("abandon claim after work attempt start failed", "issue_id", issue.ID, "error", abandonErr)
		}
		o.logMergeWorkerFailure(issue, "work_attempt_start_failed", nil)
		o.recordMergeFailed(state, issue, now, "work_attempt_start_failed", nil)
		return dispatchIssueOutcome{reason: dispatchIssueFailureWorkAttemptStart}
	}
	dispatchSourceState := ""
	dispatchTargetState := ""
	dispatchStartSourceState := ""
	dispatchStartTargetState := ""
	if targetState != "" {
		sourceState := issue.State
		if err := o.updateIssueState(runCtx, state, issue, targetState, now, "dispatch_start", laneMutationPreserveOwnership); err != nil {
			if recovery {
				releaseDispatchRecoveryAdmission(state, issue.ID)
			}
			if canary {
				releaseProjectFailureBreakerCanary(state, issue.ID)
			}
			o.releaseGlobalDispatchSlot(globalSlot)
			o.completeDurableWorkAttempt(ctx, state, Running{
				Issue:             issue,
				Attempt:           attempt,
				WorkAttemptID:     workAttemptID,
				Mode:              runMode,
				DispatchLoopStart: dispatchLoopStart,
				StartedAt:         now,
				WorkerHost:        workerHost,
			}, now, store.WorkAttemptTerminalFailure, workAttemptErrorStartTransition, err.Error(), "starting", "start state transition failed")
			o.logWorkerLifecycle(issue, "worker_capacity_released",
				telemetry.WorkAttemptIDKey, workAttemptID,
				"attempt", attempt,
				"worker_host", strings.TrimSpace(workerHost),
				"reason", dispatchIssueFailureStartStateTransition,
			)
			if abandonErr := o.abandonClaim(ctx, issue.ID); abandonErr != nil && o.logger != nil {
				o.logger.Warn("abandon claim after start state transition failed", "issue_id", issue.ID, "error", abandonErr)
			}
			if o.logger != nil {
				o.logger.Warn("start state transition failed", "issue_id", issue.ID, "identifier", issue.Identifier, "from_state", issue.State, "target_state", targetState, "error", err)
			}
			o.logMergeWorkerFailure(issue, "start_state_transition_failed", err)
			o.recordMergeFailed(state, issue, now, "start_state_transition_failed", err)
			return dispatchIssueOutcome{reason: dispatchIssueFailureStartStateTransition}
		}
		issue.State = targetState
		dispatchSourceState = sourceState
		dispatchTargetState = targetState
		dispatchStartSourceState = sourceState
		dispatchStartTargetState = targetState
	}
	if dispatchSourceState == "" || dispatchTargetState == "" {
		dispatchSourceState, dispatchTargetState = o.dispatchTimelineTransitionContext(runCtx, issue)
	}
	o.markMergeStarted(state, issue, now)
	claim.Issue = issue
	runCtx, stop := context.WithCancelCause(runCtx)
	var startupTimer mergeWorkerStartupTimer
	cancelRun := func(cause error) {
		stop(cause)
		cancelDurationLimit()
	}
	if runMode == runpkg.RunModeMerge {
		timerFactory := o.mergeWorkerStartupTimer
		if timerFactory == nil {
			timerFactory = newMergeWorkerStartupTimer
		}
		startupTimer = timerFactory(o.cfg.MergeWorkerStartupTimeout, func() {
			if o.logger != nil {
				o.logger.Warn(
					"merge worker startup deadline expired",
					"issue_id", issue.ID,
					"identifier", issue.Identifier,
					"attempt", attempt,
					"configured_timeout", o.cfg.MergeWorkerStartupTimeout,
				)
			}
			cancelRun(runpkg.ErrMergeWorkerStartupTimeout)
		})
	}
	cancelCause := func(cause error) {
		if startupTimer != nil {
			startupTimer.Stop()
		}
		cancelRun(cause)
	}
	cancel := func() {
		cancelCause(nil)
	}
	o.markBackendCapacityProbe(state, capacityProbeKey, issue.ID, now)
	dispatchWorkpadHash, dispatchComments, dispatchWorkpadRead := o.artifactGateDispatchWorkpadSnapshot(runCtx, issue)
	dispatchProgress := implementProgressArtifactSnapshotFromIssue(issue, false)
	dispatchArtifactStatus := ""
	artifactStatusField := ""
	if o.cfg.DeliverableKind == workflowconfig.DeliverableArtifact {
		artifactStatusField = gate.Effective(o.cfg.AutoPromote.Gate).Artifact.StatusField
		dispatchArtifactStatus, _ = artifactStatusFieldFromIssue(issue, artifactStatusField)
	}
	if runMode == runpkg.RunModeImplement {
		if dispatchWorkpadRead {
			progressIssue := cloneIssue(issue)
			progressIssue.Comments = append([]connector.IssueComment(nil), dispatchComments...)
			dispatchProgress = implementProgressArtifactSnapshotFromIssue(progressIssue, true)
		} else {
			dispatchProgress = o.implementProgressDispatchArtifactSnapshot(runCtx, issue)
		}
	}
	generation := o.workerGeneration.Add(1)
	state.Running[issue.ID] = Running{
		Issue:                  issue,
		Attempt:                attempt,
		WorkAttemptID:          workAttemptID,
		Policy:                 o.cfg.Policy,
		Generation:             generation,
		Mode:                   runMode,
		DispatchSourceState:    dispatchStartSourceState,
		DispatchTargetState:    dispatchStartTargetState,
		DispatchWorkpadHash:    dispatchWorkpadHash,
		DispatchWorkpadRead:    dispatchWorkpadRead,
		DispatchProgress:       dispatchProgress,
		DispatchArtifactStatus: strings.TrimSpace(dispatchArtifactStatus),
		ArtifactStatusField:    strings.TrimSpace(artifactStatusField),
		DeliverableKind:        o.cfg.DeliverableKind,
		DispatchLoopStart:      dispatchLoopStart,
		StartedAt:              now,
		WorkerHost:             workerHost,
		CapacityScope:          capacityScope,
		CapacityProbe:          capacityProbeKey != "",
		ForgeProbeHost:         reservedForgeProbeHost(state, issue.ID),
		GitHubCredential:       reservedGitHubCredential(state, issue.ID),
		ModelPermitExempt:      !modelPermitRequired,
		StopDestination:        o.cfg.StopRunTargetState,
		StopPriorityOptions:    stopRunPriorityOptions(o.cfg.StopRunPriorityNames),
		globalSlot:             globalSlot,
		cancel:                 cancel,
		stop:                   cancelCause,
	}
	o.setGlobalDispatchPreempt(globalSlot, cancel)
	state.Claimed[issue.ID] = claim
	delete(state.Retry, issue.ID)
	delete(state.Blocked, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.ReapedWorkspaces, issue.ID)
	delete(state.Completed, issue.ID)

	runningProgress := state.Running[issue.ID]
	progress := newWorkerProgress(runningProgress, o.runningWorkAttemptHeartbeat(state, runningProgress, now), o.workAttempts, o.cfg.OutputTruncationMaxBytes)
	runningProgress.progress = progress
	state.Running[issue.ID] = runningProgress

	reservation := reserveMergeCandidate(state, issue, now)
	request := RunRequest{
		Policy:              o.cfg.Policy,
		ProjectID:           strings.TrimSpace(o.cfg.Project.ID),
		Issue:               issue,
		Attempt:             attempt,
		WorkAttemptID:       workAttemptID,
		Generation:          generation,
		Mode:                runMode,
		DispatchSourceState: dispatchSourceState,
		DispatchTargetState: dispatchTargetState,
		PriorAttempt:        priorAttempt,
		StartedAt:           now,
		WorkerHost:          workerHost,
		SelectorContext:     o.selectorContext(),
		OnUsageUpdate:       o.usageUpdateHandler(runCtx, issue.ID, startupTimer, progress),
		OnActivityUpdate:    o.activityUpdateHandler(runCtx, issue),
		OnOverrideRejected:  o.agentOverrideRejectionHandler(runCtx, issue),
		ProgressProbe:       o.sessionProgressProbe(issue),
		MergePrecheck:       cloneMergePrecheck(queuedRetry.MergePrecheck),
		MergeRefreshHeadSHA: reservation.RefreshHeadSHA,
		ForgeRetry:          cloneForgeRetry(queuedRetry.ForgeRetry),
	}
	o.attachHumanPrerequisiteTool(&request)
	if source, ok := o.scheduling.(interface{ RunExecution(string) runpkg.Execution }); ok {
		request.Execution = source.RunExecution(issue.ID)
	}
	if !modelPermitRequired {
		request.AcquireModelPermit = o.modelPermitAcquirer(issue.ID)
	}
	if retryQueued {
		request.RetryMode = queuedRetry.RetryMode
		request.ResumeState = queuedRetry.ResumeState
	}
	if priorAttempt.ExplainBeforeRetry {
		delete(state.PriorAttempts, issue.ID)
	}
	o.logMergeWorkerAttempt(issue, attempt, workerHost)
	o.logWorkerLifecycle(issue, "worker_attempt_started",
		telemetry.WorkAttemptIDKey, workAttemptID,
		"attempt", attempt,
		"worker_host", strings.TrimSpace(workerHost),
		"mode", strings.TrimSpace(runMode),
	)
	o.publishRuntimeState(state)
	running := state.Running[issue.ID]
	running.done = o.supervisor.Dispatch(runCtx, request, o.runResults)
	state.Running[issue.ID] = running
	durationLimitTransferred = true
	o.trackRunningHeartbeat(state, running, claim, now)
	return dispatchIssueOutcome{dispatched: true}
}

func (o *Orchestrator) dispatchTimelineTransitionContext(ctx context.Context, issue connector.Issue) (string, string) {
	match, ok := o.latestWorkflowLaneEntry(ctx, issue)
	if !ok {
		return "", ""
	}
	sourceState := strings.TrimSpace(match.Event.PreviousPhaseName)
	targetState := strings.TrimSpace(match.Event.PhaseName)
	if sourceState == "" || targetState == "" {
		return "", ""
	}
	if normalizeState(targetState) != normalizeState(issue.State) {
		return "", ""
	}
	return sourceState, targetState
}

// dispatchWorkingStates lists the active-state names recognized as the
// "agent is working" lane, in preference order: "In Progress" is the GitHub
// Projects convention, "Production" the non-code artifact template's. Without
// this resolution, projects using the template vocabulary never leave Todo
// while an agent works them and the board shows nothing running.
var dispatchWorkingStates = []string{planImplementationState, "Production"}

func dispatchStartTransitionState(issue connector.Issue, mode string, activeStates []string) string {
	if mode != runpkg.RunModeImplement {
		return ""
	}
	if normalizeState(issue.State) != "todo" {
		return ""
	}
	for _, working := range dispatchWorkingStates {
		if stateIn(working, activeStates) {
			return working
		}
	}
	return ""
}

func (o *Orchestrator) dispatchMode(ctx context.Context, state *State, issue connector.Issue) string {
	if normalizeState(issue.State) == normalizeState(autoPromoteMergingState) && o.cfg.MergeFastPathEnabled {
		return runpkg.RunModeMerge
	}
	cfg := gate.EffectivePlan(o.cfg.Plan)
	if !cfg.Enabled {
		return runpkg.RunModeImplement
	}
	switch normalizeState(issue.State) {
	case "todo":
		return runpkg.RunModePlan
	case normalizeState(autoPromoteReworkState):
		if match, ok := o.latestWorkflowLaneEntry(ctx, issue); ok {
			if normalizeState(match.Event.PhaseName) == normalizeState(autoPromoteReworkState) &&
				workflowLaneMetadataHasAction(match.Metadata, workflowActionPlanReviewRework) {
				return runpkg.RunModePlan
			}
			return runpkg.RunModeImplement
		}
		issueID := strings.TrimSpace(issue.ID)
		if issueID != "" {
			if _, ok := state.planRework[issueID]; ok {
				return runpkg.RunModePlan
			}
		}
	}
	return runpkg.RunModeImplement
}

func (o *Orchestrator) markGlobalProjectIdle() {
	if o.globalDispatchGate == nil {
		return
	}
	o.globalDispatchGate.MarkIdle(o.cfg.Project)
}

type projectCycleDispatchGate interface {
	BeginProjectCycle(scheduler.ProjectCandidate)
	EndProjectCycle(string)
}

func (o *Orchestrator) beginGlobalProjectCycle() {
	if gate, ok := o.globalDispatchGate.(projectCycleDispatchGate); ok {
		gate.BeginProjectCycle(o.cfg.Project)
		return
	}
	o.markGlobalProjectIdle()
}

func (o *Orchestrator) endGlobalProjectCycle() {
	if gate, ok := o.globalDispatchGate.(projectCycleDispatchGate); ok {
		gate.EndProjectCycle(o.cfg.Project.ID)
	}
}

type projectStateSlotStats struct {
	capacity  int
	used      int
	available int
}

type detailedProjectDispatchGate interface {
	TryAcquireWithDecision(context.Context, scheduler.ProjectCandidate, scheduler.SlotRequest, time.Time) (
		scheduler.Slot,
		bool,
		scheduler.DispatchGateDecision,
		error,
	)
}

func (o *Orchestrator) acquireGlobalDispatchSlot(
	ctx context.Context,
	issue connector.Issue,
	workerHost string,
	now time.Time,
	pressureCapacity int,
) (scheduler.Slot, bool, scheduler.DispatchGateDecision) {
	if o.globalDispatchGate == nil {
		return scheduler.Slot{}, true, scheduler.DispatchGateDecision{
			PoolName: scheduler.DefaultPoolName,
			Reason:   scheduler.DispatchGateReasonGranted,
		}
	}

	req := scheduler.SlotRequest{
		State:            issue.State,
		Host:             workerHost,
		Priority:         o.dispatchStatePriority(issue.State),
		PressureCapacity: pressureCapacity,
	}
	var (
		slot     scheduler.Slot
		ok       bool
		decision scheduler.DispatchGateDecision
		err      error
	)
	if detailed, hasDecision := o.globalDispatchGate.(detailedProjectDispatchGate); hasDecision {
		slot, ok, decision, err = detailed.TryAcquireWithDecision(ctx, o.cfg.Project, req, now)
	} else {
		slot, ok, err = o.globalDispatchGate.TryAcquire(ctx, o.cfg.Project, req, now)
		if ok {
			decision.Reason = scheduler.DispatchGateReasonGranted
		}
	}
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("global dispatch slot unavailable", "project_id", o.cfg.Project.ID, "issue_id", issue.ID, "error", err)
		}
		return scheduler.Slot{}, false, decision
	}
	return slot, ok, decision
}

func (o *Orchestrator) dispatchPoolSnapshot() scheduler.PoolSnapshot {
	if o == nil {
		return scheduler.PoolSnapshot{Name: scheduler.DefaultPoolName}
	}
	fallback := scheduler.PoolSnapshot{
		Name:       scheduler.DefaultPoolName,
		Capacity:   o.cfg.MaxConcurrentAgents,
		Guaranteed: o.cfg.MaxConcurrentAgents,
		BurstTo:    o.cfg.MaxConcurrentAgents,
		Available:  o.cfg.MaxConcurrentAgents,
	}
	if o.globalDispatchGate == nil {
		return fallback
	}
	if snapshotter, ok := o.globalDispatchGate.(scheduler.ProjectPoolSnapshotter); ok {
		snapshot := snapshotter.PoolSnapshotFor(o.cfg.Project.ID)
		if snapshot.Capacity > 0 {
			return snapshot
		}
	}
	if snapshotter, ok := o.globalDispatchGate.(scheduler.PoolSnapshotter); ok {
		snapshot := snapshotter.PoolSnapshot()
		if snapshot.Capacity > 0 {
			return snapshot
		}
	}
	return fallback
}

func (o *Orchestrator) dispatchStatePriority(state string) int {
	return dispatchpriority.New(o.cfg.DispatchPriorityByState, nil).State(state)
}

func (o *Orchestrator) projectStateSlotStats(issue connector.Issue, state *State) projectStateSlotStats {
	limit := o.cfg.MaxConcurrentAgents
	normalized := normalizeState(issue.State)
	if stateLimit, ok := o.cfg.MaxConcurrentAgentsByState[normalized]; ok {
		limit = stateLimit
	}

	used := 0
	if state != nil {
		for _, running := range state.Running {
			if normalizeState(running.Issue.State) == normalized {
				used++
			}
		}
	}
	available := limit - used
	if available < 0 {
		available = 0
	}
	return projectStateSlotStats{capacity: limit, used: used, available: available}
}

func (o *Orchestrator) releaseGlobalDispatchSlot(slot scheduler.Slot) {
	if o.globalDispatchGate == nil || slot == (scheduler.Slot{}) {
		return
	}
	if err := o.globalDispatchGate.Release(slot); err != nil && o.logger != nil {
		o.logger.Warn("release global dispatch slot failed", "project_id", o.cfg.Project.ID, "error", err)
	}
}

func (o *Orchestrator) setGlobalDispatchPreempt(slot scheduler.Slot, preempt func()) {
	if o.globalDispatchGate == nil || slot == (scheduler.Slot{}) {
		return
	}
	o.globalDispatchGate.SetPreempt(slot, preempt)
}

func (o *Orchestrator) selectorContext() selector.Context {
	ctx := selector.Context{
		Persona: o.cfg.SelectorPersona,
	}
	if identifier, ok := o.connector.(connector.InstanceIdentifier); ok {
		ctx.InstanceLogin = identifier.InstanceLogin()
	}
	return ctx
}

func (o *Orchestrator) usageUpdateHandler(
	ctx context.Context,
	issueID string,
	startupTimer mergeWorkerStartupTimer,
	progresses ...*workerProgress,
) runpkg.UsageUpdateHandler {
	var progress *workerProgress
	if len(progresses) > 0 {
		progress = progresses[0]
	}
	return func(update runpkg.UsageUpdate) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if startupTimer != nil {
			startupTimer.Stop()
		}
		if progress != nil {
			if err := progress.observe(ctx, update); err != nil {
				return err
			}
		}
		if update.DispatchLoopStart != nil && progress == nil {
			applied := make(chan struct{})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case o.runUpdates <- runUpdate{issueID: issueID, usage: update, applied: applied}:
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-applied:
				return nil
			}
		}
		if strings.TrimSpace(update.WorkerGitHubActor.Login) != "" && progress == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case o.runUpdates <- runUpdate{issueID: issueID, usage: update, progress: progress}:
				return nil
			}
		}

		select {
		case o.runUpdates <- runUpdate{issueID: issueID, usage: update, progress: progress}:
			return nil
		default:
			return nil
		}
	}
}

func (o *Orchestrator) activityUpdateHandler(ctx context.Context, issue connector.Issue) runpkg.AgentActivityUpdateHandler {
	return func(update runpkg.AgentActivityUpdate) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if o.activity == nil {
			return nil
		}
		o.activity.Publish(activity.Key{ProjectID: o.cfg.Project.ID, IssueID: issue.ID}, activity.Event{
			At:                update.At,
			DetentSessionID:   update.DetentSessionID,
			ProviderSessionID: update.ProviderSessionID,
			TurnID:            update.TurnID,
			ItemID:            update.ItemID,
			Kind:              string(update.Type),
			Title:             activityUpdateTitle(update),
			Content:           update.Content,
			Status:            update.Status,
			Model:             update.Model,
			TotalTokens:       update.TotalTokens,
		})
		return nil
	}
}

func activityUpdateTitle(update runpkg.AgentActivityUpdate) string {
	switch update.Type {
	case runpkg.AgentUpdateMessageDelta:
		return "Agent"
	case runpkg.AgentUpdateToolStarted:
		return "Tool started · " + strings.TrimSpace(update.Tool)
	case runpkg.AgentUpdateToolOutput:
		return "Tool output · " + strings.TrimSpace(update.Tool)
	case runpkg.AgentUpdateToolCompleted:
		return "Tool finished · " + strings.TrimSpace(update.Tool)
	case runpkg.AgentUpdateMCPElicitation:
		if tool := strings.TrimSpace(update.Tool); tool != "" {
			return "MCP elicitation · " + tool
		}
		return "MCP elicitation"
	case runpkg.AgentUpdateTokenUsage:
		return "Usage"
	case runpkg.AgentUpdateTurnStarted:
		return "Turn started"
	case runpkg.AgentUpdateTurnCompleted:
		return "Turn finished"
	case runpkg.AgentUpdateProcessStarted:
		return "Worker started"
	case runpkg.AgentUpdateModelUpdated:
		return "Model updated"
	default:
		return strings.TrimSpace(string(update.Type))
	}
}

func validCandidate(issue connector.Issue) bool {
	return issue.ID != "" &&
		issue.Identifier != "" &&
		issue.Title != "" &&
		issue.State != "" &&
		!issue.Closed &&
		issue.AssignedToWorker
}

func duplicatePullRequestWork(issue connector.Issue) bool {
	if issue.PullRequest == nil {
		return false
	}
	switch normalizePullRequestState(issue.PullRequest.State) {
	case "merged":
		return !staleMergedPullRequestHasFailedCIEvidence(issue.PullRequest, staleMergedPullRequestSummaryFromIssue(issue))
	case "open":
		return normalizeState(issue.State) == "todo"
	default:
		return false
	}
}

func mergedPullRequestReconciliationPending(issue connector.Issue, cfg Config) bool {
	if issue.PullRequest == nil || normalizePullRequestState(issue.PullRequest.State) != "merged" {
		return false
	}
	summary := staleMergedPullRequestSummaryFromIssue(issue)
	decision := staleMergedPullRequestDecision(issue, summary)
	targetState := staleMergedPullRequestTargetState(decision, cfg.AutoPromote, cfg.TerminalStates)
	return targetState != "" && normalizeState(targetState) != normalizeState(issue.State)
}

func continuationDispatch(issue connector.Issue) bool {
	state := normalizeState(issue.State)
	return state != "" && state != "todo"
}

func continuationDelay(index int) time.Duration {
	if index <= 0 {
		return 0
	}
	return continuationDispatchBackoff
}

func waitForDispatchBackoff(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func todoBlockedByNonTerminal(issue connector.Issue, terminalStates []string) bool {
	for _, blocker := range issue.BlockedBy {
		if blocker.HumanOwned {
			if !blocker.HumanCompletionReady {
				return true
			}
			continue
		}
		if normalizeState(issue.State) != "todo" {
			continue
		}
		if strings.TrimSpace(blocker.State) == "" {
			continue
		}
		if !stateIn(blocker.State, terminalStates) {
			return true
		}
	}
	return false
}

func availableSlots(state *State) int {
	available := state.MaxConcurrentAgents - len(state.Running)
	if available < 0 {
		return 0
	}
	return available
}
