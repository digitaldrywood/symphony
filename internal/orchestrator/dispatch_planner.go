package orchestrator

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/budget"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

type dispatchPlanner struct {
	cfg Config
	now time.Time
}

type dispatchPlanHooks struct {
	rankingIssues           []connector.Issue
	hydrate                 func(connector.Issue) (connector.Issue, bool)
	beforeDispatch          func(connector.Issue, int) bool
	dispatch                func(dispatchAction) bool
	dispatchFailed          func(connector.Issue) bool
	retryDispatchFailed     func(connector.Issue, Retry)
	pollRetryWait           func(connector.Issue, Retry) (Retry, bool, string)
	preserveMissingDueRetry func(Retry) bool
	decision                func(dispatchPlanDecision)
}

type dispatchAction struct {
	issue               connector.Issue
	attempt             int
	workerHost          string
	retry               bool
	modelPermitRequired bool
	retryState          *Retry
}

type dispatchPlanDecision struct {
	Issue                 connector.Issue
	QueuePosition         int
	Attempt               int
	WorkerHost            string
	Retry                 bool
	Selected              bool
	SkipReason            string
	SkipDetail            string
	AuthorizationDecision *selector.Decision
	SelectionReason       string
	UnblockerCount        int
}

func newDispatchPlanner(cfg Config) dispatchPlanner {
	return dispatchPlanner{cfg: normalizeConfig(cfg)}
}

func (p dispatchPlanner) plan(
	state *State,
	candidates []connector.Issue,
	now time.Time,
	hooks dispatchPlanHooks,
) DispatchPlan {
	p.now = now
	state.ensureInitialized(p.cfg)
	reconcileMergeReservations(state, candidates, p.cfg, now)
	p.trackOwnershipAttentionCandidates(state, candidates, now)

	plannedCandidates := cloneIssues(candidates)
	rankingIssues := cloneIssues(candidates)
	rankingIssues = append(rankingIssues, hooks.rankingIssues...)
	for _, blocked := range state.Blocked {
		rankingIssues = append(rankingIssues, cloneIssue(blocked.Issue))
	}
	annotateUnblockerCounts(
		plannedCandidates,
		rankingIssues,
		p.cfg.ActiveStates,
		p.cfg.TerminalStates,
		p.cfg.PrioritizeUnblockers,
	)
	clearBlockedUnblockerCounts(plannedCandidates, state.Blocked)
	sortIssuesForDispatch(plannedCandidates, p.cfg.DispatchPriorityByState, p.cfg.DispatchPriorityByLabel, p.cfg.PrioritizeUnblockers)
	dueRetries := dueRetriesByIssue(state, now)
	p.releaseMissingDueRetries(state, plannedCandidates, dueRetries, hooks)
	dueRetries = dueRetriesByIssue(state, now)
	mergePriority := prioritizeReadyMergingIssues(plannedCandidates, state, now, p.cfg.MergeFairnessAge)
	logDecision := func(decision dispatchPlanDecision) {
		decision.SelectionReason = mergePriority.reasons[strings.TrimSpace(decision.Issue.ID)]
		p.logDecision(hooks, decision)
	}

	plan := DispatchPlan{}
	continuations := 0
	for index, issue := range plannedCandidates {
		queuePosition := index + 1
		if correctableDispatchEscalated(state, issue.ID, dispatchSkipOwnershipAssigneeRequired) {
			continue
		}
		if reservation, blocked := mergeReservationBlocks(state, issue, now); blocked {
			logDecision(dispatchPlanDecision{
				Issue: issue, QueuePosition: queuePosition,
				SkipReason: "merge_ci_reservation",
				SkipDetail: "reserved for " + reservation.IssueID + " head=" + reservation.HeadSHA + " base=" + reservation.BaseSHA + " until=" + reservation.ExpiresAt.Format(time.RFC3339),
			})
			continue
		}
		if mergeFairnessBlocks(state, mergePriority.stickyIssueID, issue, now) {
			logDecision(dispatchPlanDecision{
				Issue:         issue,
				QueuePosition: queuePosition,
				SkipReason:    dispatchSkipMergeFairnessReserved,
			})
			continue
		}
		if retry, ok := dueRetries[issue.ID]; ok {
			if retry.Wait.Kind == retryWaitCurrentHeadCI || retry.Wait.Kind == retryWaitWorkspaceBranchHeld {
				var handled bool
				reason := ""
				if hooks.pollRetryWait != nil {
					retry, handled, reason = hooks.pollRetryWait(issue, retry)
				} else {
					retry, handled = p.previewCurrentHeadCIWait(state, issue, retry, now)
					if handled {
						reason = dispatchSkipCurrentHeadCIWait
					}
				}
				if handled {
					p.logDecision(hooks, dispatchPlanDecision{
						Issue:         issue,
						QueuePosition: queuePosition,
						Attempt:       retry.Attempt,
						WorkerHost:    retry.WorkerHost,
						Retry:         true,
						SkipReason:    reason,
					})
					continue
				}
			}
			action, ok, reason := p.retryAction(state, issue, retry, now)
			if !ok {
				logDecision(dispatchPlanDecision{
					Issue:         issue,
					QueuePosition: queuePosition,
					Attempt:       retry.Attempt,
					WorkerHost:    retry.WorkerHost,
					Retry:         true,
					SkipReason:    reason,
				})
				continue
			}
			logDecision(dispatchPlanDecision{
				Issue:         action.issue,
				QueuePosition: queuePosition,
				Attempt:       action.attempt,
				WorkerHost:    action.workerHost,
				Retry:         true,
				Selected:      true,
			})
			if p.applyDispatchAction(state, action, now, hooks) {
				plan.Dispatches = append(plan.Dispatches, action.decision())
			} else if hooks.retryDispatchFailed != nil {
				hooks.retryDispatchFailed(action.issue, retry)
			}
			continue
		}
		if p.hardAvailableSlots(state) == 0 {
			for skipIndex := index; skipIndex < len(plannedCandidates); skipIndex++ {
				logDecision(dispatchPlanDecision{
					Issue:         plannedCandidates[skipIndex],
					QueuePosition: skipIndex + 1,
					SkipReason:    dispatchSkipGlobalCapacityFull,
				})
			}
			break
		}
		if hooks.hydrate != nil {
			var ok bool
			issue, ok = hooks.hydrate(issue)
			if !ok {
				logDecision(dispatchPlanDecision{
					Issue:         issue,
					QueuePosition: queuePosition,
					SkipReason:    dispatchSkipHydrationFailed,
				})
				continue
			}
		}
		action, ok, reason := p.dispatchAction(state, issue, now)
		if !ok {
			logDecision(dispatchPlanDecision{
				Issue:         issue,
				QueuePosition: queuePosition,
				SkipReason:    reason,
			})
			continue
		}
		continuationIndex := -1
		if continuationDispatch(action.issue) {
			continuationIndex = continuations
			continuations++
		}
		if hooks.beforeDispatch != nil && !hooks.beforeDispatch(action.issue, continuationIndex) {
			logDecision(dispatchPlanDecision{
				Issue:         action.issue,
				QueuePosition: queuePosition,
				Attempt:       action.attempt,
				WorkerHost:    action.workerHost,
				Retry:         action.retry,
				SkipReason:    dispatchSkipDispatchBackoffCancelled,
			})
			break
		}
		logDecision(dispatchPlanDecision{
			Issue:         action.issue,
			QueuePosition: queuePosition,
			Attempt:       action.attempt,
			WorkerHost:    action.workerHost,
			Retry:         action.retry,
			Selected:      true,
		})
		if p.applyDispatchAction(state, action, now, hooks) {
			plan.Dispatches = append(plan.Dispatches, action.decision())
		} else if hooks.dispatchFailed != nil && !hooks.dispatchFailed(action.issue) {
			break
		}
	}

	plan.Claimed = claimedIDs(state.Claimed)
	plan.Blocked = blockedIDs(state.Blocked)
	plan.BudgetRefusals = budgetRefusalIDs(state.BudgetRefusals)
	plan.Retry = retryIDs(state.Retry)
	return plan
}

func clearBlockedUnblockerCounts(issues []connector.Issue, blocked map[string]Blocked) {
	for index := range issues {
		if _, ok := blocked[issues[index].ID]; ok {
			issues[index].UnblockerCount = 0
		}
	}
}

func (p dispatchPlanner) logDecision(hooks dispatchPlanHooks, decision dispatchPlanDecision) {
	if decision.SkipReason == dispatchSkipAuthorizationSelector && decision.AuthorizationDecision == nil {
		authorization := p.authorizationDecision(decision.Issue)
		decision.AuthorizationDecision = &authorization
		decision.SkipDetail = authorization.Detail
	}
	decision.UnblockerCount = decision.Issue.UnblockerCount
	if hooks.decision != nil {
		hooks.decision(decision)
	}
}

func (p dispatchPlanner) applyDispatchAction(
	state *State,
	action dispatchAction,
	now time.Time,
	hooks dispatchPlanHooks,
) bool {
	if hooks.dispatch != nil {
		return hooks.dispatch(action)
	}
	p.markDispatched(state, action, now)
	return true
}

func (p dispatchPlanner) retryAction(
	state *State,
	issue connector.Issue,
	retry Retry,
	now time.Time,
) (dispatchAction, bool, string) {
	if activeTrackerUnavailable(state) && (trackerDependentDispatch(issue) || retry.TrackerUnavailable) {
		return dispatchAction{}, false, dispatchSkipTrackerUnavailable
	}
	if retry.CompletionDeferred {
		return dispatchAction{}, false, dispatchSkipCompletionDeferred
	}
	if activeCIUnavailable(state) && (ciDependentDispatch(issue) || retry.CIUnavailable) {
		return dispatchAction{}, false, dispatchSkipCIUnavailable
	}
	if forgeAvailabilityBlocks(state, issue, retry, p.cfg.ForgeHost, now) {
		return dispatchAction{}, false, dispatchSkipForgeUnavailable
	}
	if workerGitHubMonitorBlocks(state, issue.ID, retry, now) {
		return dispatchAction{}, false, dispatchSkipGitHubMonitor
	}
	if outage, paused := activeGitHubRESTCapacityOutage(state, now); paused {
		if retry.DueAt.Before(outage.ResumeAt) {
			retry.DueAt = outage.ResumeAt
			state.Retry[retry.Issue.ID] = retry
		}
		return dispatchAction{}, false, dispatchSkipGitHubRESTCapacity
	}
	if reason := dispatchRecoveryBlockReason(state, now); reason != "" {
		return dispatchAction{}, false, reason
	}
	forgeProbeReserved := false
	if retry.ForgeUnavailable {
		if _, active := forgeCondition(state, retry.ForgeHost); active {
			_, forgeProbeReserved = reserveForgeAvailabilityProbe(state, issue.ID, retry, now)
			if !forgeProbeReserved {
				return dispatchAction{}, false, dispatchSkipForgeUnavailable
			}
		}
	}
	workerGitHubMonitorProbeReserved := false
	if retry.GitHubMonitor {
		if _, active := state.GitHubMonitors[strings.TrimSpace(retry.GitHubCredential)]; active {
			_, workerGitHubMonitorProbeReserved = reserveWorkerGitHubMonitorProbe(state, issue.ID, retry, now)
			if !workerGitHubMonitorProbeReserved {
				return dispatchAction{}, false, dispatchSkipGitHubMonitor
			}
		}
	}
	delete(state.Retry, retry.Issue.ID)

	modelPermitRequired := p.modelPermitRequiredAtDispatch(issue) || retry.MergePrecheck != nil
	decision := p.dispatchableIssueDecisionForModelRequirement(
		issue,
		state,
		true,
		now,
		retry.WorkerHost,
		modelPermitRequired,
	)
	if !decision.dispatchable {
		if forgeProbeReserved {
			releaseForgeAvailabilityProbe(state, issue.ID, "deferred", decision.reason, now)
		}
		if workerGitHubMonitorProbeReserved {
			releaseWorkerGitHubMonitorProbe(state, issue.ID, "deferred", decision.reason, now)
		}
		if decision.reason == dispatchSkipProjectFailureBreaker {
			if retry.DueAt.Before(state.FailureBreaker.ResumeAt) {
				retry.DueAt = state.FailureBreaker.ResumeAt
			}
			state.Retry[retry.Issue.ID] = retry
			return dispatchAction{}, false, decision.reason
		}
		if reason := p.budgetRefusalWaitReason(state, issue.ID, now); reason != "" {
			if reason == dispatchSkipBudgetCooldown {
				p.rescheduleRetry(state, retry, now, "budget cooldown active", false)
			} else {
				p.parkBudgetHardHold(state, issue.ID)
			}
			return dispatchAction{}, false, decision.reason
		}
		if !p.slotsAvailableForModelRequirement(issue, state, retry.WorkerHost, modelPermitRequired) {
			p.rescheduleRetry(state, retry, now, "no available orchestrator slots", false)
			return dispatchAction{}, false, decision.reason
		}
		if _, blocked := state.Blocked[issue.ID]; blocked {
			p.releaseClaim(state, issue.ID)
			return dispatchAction{}, false, decision.reason
		}

		p.releaseIssue(state, issue.ID)
		return dispatchAction{}, false, decision.reason
	}

	action, ok := p.newDispatchAction(state, issue, retry.Attempt, retry.WorkerHost, true, modelPermitRequired, &retry)
	if !ok {
		if forgeProbeReserved {
			releaseForgeAvailabilityProbe(state, issue.ID, "deferred", dispatchSkipWorkerHostUnavailable, now)
		}
		if workerGitHubMonitorProbeReserved {
			releaseWorkerGitHubMonitorProbe(state, issue.ID, "deferred", dispatchSkipWorkerHostUnavailable, now)
		}
		return dispatchAction{}, false, dispatchSkipWorkerHostUnavailable
	}
	return action, true, ""
}

func (p dispatchPlanner) dispatchAction(state *State, issue connector.Issue, now time.Time) (dispatchAction, bool, string) {
	decision := p.dispatchableIssueDecision(issue, state, false, now, "")
	if !decision.dispatchable {
		if todoBlockedByNonTerminal(issue, p.cfg.TerminalStates) {
			state.Blocked[issue.ID] = Blocked{
				Issue:     cloneIssue(issue),
				Reason:    blockedReasonDependency,
				BlockedAt: now,
				Source:    BlockedSourceDependency,
			}
		}
		return dispatchAction{}, false, decision.reason
	}

	action, ok := p.newDispatchAction(state, issue, 0, "", false, p.modelPermitRequiredAtDispatch(issue), nil)
	if !ok {
		return dispatchAction{}, false, dispatchSkipWorkerHostUnavailable
	}
	return action, true, ""
}

func (p dispatchPlanner) newDispatchAction(
	state *State,
	issue connector.Issue,
	attempt int,
	preferredWorkerHost string,
	retry bool,
	modelPermitRequired bool,
	retryState *Retry,
) (dispatchAction, bool) {
	workerHost, ok := p.selectWorkerHost(state, preferredWorkerHost)
	if !ok {
		return dispatchAction{}, false
	}

	return dispatchAction{
		issue:               cloneIssue(issue),
		attempt:             attempt,
		workerHost:          workerHost,
		retry:               retry,
		modelPermitRequired: modelPermitRequired,
		retryState:          retryState,
	}, true
}

func (p dispatchPlanner) markDispatched(state *State, action dispatchAction, now time.Time) {
	issue := cloneIssue(action.issue)
	reserveMergeCandidate(state, issue, now)
	state.Running[issue.ID] = Running{
		Issue:             issue,
		Attempt:           action.attempt,
		StartedAt:         now,
		WorkerHost:        action.workerHost,
		GitHubCredential:  reservedGitHubCredential(state, issue.ID),
		ModelPermitExempt: !action.modelPermitRequired,
	}
	state.Claimed[issue.ID] = Claimed{
		Issue:     issue,
		ClaimedAt: now,
	}
	delete(state.Retry, issue.ID)
	delete(state.Blocked, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.Completed, issue.ID)
}

func (a dispatchAction) decision() DispatchDecision {
	return DispatchDecision{
		IssueID:    a.issue.ID,
		Identifier: a.issue.Identifier,
		State:      a.issue.State,
		Attempt:    a.attempt,
		WorkerHost: a.workerHost,
		Retry:      a.retry,
	}
}

func (p dispatchPlanner) pruneBudgetRefusals(
	state *State,
	now time.Time,
	dailyStatus *DailyBudgetStatus,
	issueStatuses map[string]IssueBudgetStatus,
) {
	if p.cfg.subscriptionBilling() {
		clear(state.BudgetRefusals)
		return
	}
	for issueID, refusal := range state.BudgetRefusals {
		var issueStatus *IssueBudgetStatus
		if status, ok := issueStatuses[issueID]; ok {
			issueStatus = &status
		}
		if !p.budgetRefusalActive(refusal, now, dailyStatus, issueStatus) {
			delete(state.BudgetRefusals, issueID)
		}
	}
}

func (p dispatchPlanner) pruneInactiveIssueBudgetRefusals(state *State, candidates []connector.Issue) {
	active := make(map[string]struct{}, len(candidates))
	for _, issue := range candidates {
		active[issue.ID] = struct{}{}
	}
	for issueID, refusal := range state.BudgetRefusals {
		if refusal.Code != string(budget.ReasonPerIssueMaxUSD) {
			continue
		}
		if _, ok := active[issueID]; !ok {
			delete(state.BudgetRefusals, issueID)
		}
	}
}

func (p dispatchPlanner) budgetRefusalWaitReason(state *State, issueID string, now time.Time) string {
	if p.cfg.subscriptionBilling() {
		return ""
	}
	refusal, ok := state.BudgetRefusals[issueID]
	if !ok {
		return ""
	}

	if !p.budgetRefusalActive(refusal, now, nil, nil) {
		return ""
	}
	if refusal.Code == string(budget.ReasonPerIssueMaxUSD) {
		return dispatchSkipBudgetHardHold
	}
	return dispatchSkipBudgetCooldown
}

func (p dispatchPlanner) budgetRefusalActive(
	refusal BudgetRefusal,
	now time.Time,
	dailyStatus *DailyBudgetStatus,
	issueStatus *IssueBudgetStatus,
) bool {
	if refusal.Code == string(budget.ReasonPerIssueMaxUSD) {
		if issueStatus == nil {
			return true
		}
		return issueStatus.Active && issueStatus.CurrentSpendUSD+refusal.ProjectedCostUSD > issueStatus.MaxUSD
	}
	if refusal.ResetAt != nil && now.Before(*refusal.ResetAt) {
		if refusal.Code == string(budget.ReasonPerDayMaxUSD) && dailyStatus != nil {
			if !dailyStatus.Active {
				return false
			}
			return dailyStatus.CurrentSpendUSD+refusal.ProjectedCostUSD > dailyStatus.MaxUSD
		}
		return true
	}
	if p.cfg.BudgetRefusalCooldown <= 0 || refusal.RefusedAt.IsZero() {
		return false
	}

	return now.Before(refusal.RefusedAt.Add(p.cfg.BudgetRefusalCooldown))
}

func (p dispatchPlanner) trackBlockedCandidates(state *State, issues []connector.Issue, now time.Time) {
	seenBlocked := make(map[string]struct{})
	for _, issue := range issues {
		if issue.ID == "" {
			continue
		}
		if todoBlockedByNonTerminal(issue, p.cfg.TerminalStates) {
			seenBlocked[issue.ID] = struct{}{}
			state.Blocked[issue.ID] = Blocked{
				Issue:     cloneIssue(issue),
				Reason:    blockedReasonDependency,
				BlockedAt: now,
				Source:    BlockedSourceDependency,
			}
		}
	}

	for issueID, blocked := range state.Blocked {
		if !blockedFromDependency(blocked) {
			continue
		}
		if _, ok := seenBlocked[issueID]; !ok {
			delete(state.Blocked, issueID)
		}
	}
}

func (p dispatchPlanner) releaseMissingDueRetries(
	state *State,
	issues []connector.Issue,
	dueRetries map[string]Retry,
	hooks dispatchPlanHooks,
) {
	if len(dueRetries) == 0 {
		return
	}

	byID := make(map[string]struct{}, len(issues))
	for _, issue := range issues {
		byID[issue.ID] = struct{}{}
	}

	for issueID, retry := range dueRetries {
		if _, ok := byID[issueID]; !ok {
			if hooks.preserveMissingDueRetry != nil && hooks.preserveMissingDueRetry(retry) {
				continue
			}
			if _, blocked := state.Blocked[issueID]; blocked {
				p.releaseClaim(state, issueID)
				continue
			}
			p.releaseIssue(state, issueID)
		}
	}
}

func (p dispatchPlanner) dispatchable(issue connector.Issue, state *State, now time.Time) bool {
	return p.dispatchableIssue(issue, state, false, now, "")
}

func (p dispatchPlanner) dispatchableIssue(
	issue connector.Issue,
	state *State,
	allowClaimed bool,
	now time.Time,
	preferredWorkerHost string,
) bool {
	return p.dispatchableIssueDecision(issue, state, allowClaimed, now, preferredWorkerHost).dispatchable
}

type dispatchableDecision struct {
	dispatchable  bool
	reason        string
	detail        string
	authorization *selector.Decision
}

const (
	dispatchSkipInvalidCandidate          = scheduler.DecisionReasonInvalidCandidate
	dispatchSkipInactiveState             = scheduler.DecisionReasonInactiveState
	dispatchSkipTerminalState             = scheduler.DecisionReasonTerminalState
	dispatchSkipPullRequestHydration      = scheduler.DecisionReasonPullRequestHydrationUnavailable
	dispatchSkipAwaitingGate              = scheduler.DecisionReasonAwaitingGate
	dispatchSkipArtifactGateWaitStatus    = scheduler.DecisionReasonArtifactGateWaitStatus
	dispatchSkipMergedPullRequest         = scheduler.DecisionReasonMergedPullRequestPending
	dispatchSkipDuplicatePullRequest      = scheduler.DecisionReasonDuplicatePullRequestWork
	dispatchSkipAuthorizationSelector     = scheduler.DecisionReasonAuthorizationSelectorDeclined
	dispatchSkipOwnershipAssigneeRequired = scheduler.DecisionReasonOwnershipAssigneeRequired
	dispatchSkipBlockedByDependency       = scheduler.DecisionReasonBlockedByDependency
	dispatchSkipAlreadyRunning            = scheduler.DecisionReasonAlreadyRunning
	dispatchSkipRetryPending              = scheduler.DecisionReasonRetryPending
	dispatchSkipAlreadyClaimed            = scheduler.DecisionReasonAlreadyClaimed
	dispatchSkipBlocked                   = scheduler.DecisionReasonBlocked
	dispatchSkipBudgetCooldown            = scheduler.DecisionReasonBudgetCooldown
	dispatchSkipBudgetHardHold            = scheduler.DecisionReasonBudgetHardHold
	dispatchSkipLifetimeLimit             = scheduler.DecisionReasonLifetimeLimit
	dispatchSkipLocalSlotUnavailable      = scheduler.DecisionReasonLocalSlotUnavailable
	dispatchSkipWorkerHostUnavailable     = scheduler.DecisionReasonWorkerHostUnavailable
	dispatchSkipGlobalCapacityFull        = scheduler.DecisionReasonGlobalCapacityFull
	dispatchSkipHydrationFailed           = scheduler.DecisionReasonHydrateFailed
	dispatchSkipDispatchBackoffCancelled  = scheduler.DecisionReasonDispatchBackoffCancelled
	dispatchSkipMergeFairnessReserved     = scheduler.DecisionReasonMergeFairnessHeadReserved
	dispatchSkipGitHubRESTCapacity        = scheduler.DecisionReasonGitHubRESTCapacityPaused
	dispatchSkipTrackerUnavailable        = scheduler.DecisionReasonTrackerUnavailable
	dispatchSkipCompletionDeferred        = scheduler.DecisionReasonCompletionDeferred
	dispatchSkipForgeUnavailable          = scheduler.DecisionReasonForgeUnavailable
	dispatchSkipGitHubMonitor             = scheduler.DecisionReasonGitHubMonitor
	dispatchSkipCIUnavailable             = scheduler.DecisionReasonCIUnavailable
	dispatchSkipProjectFailureBreaker     = scheduler.DecisionReasonProjectFailureBreakerPaused
	dispatchSkipRateWindowBackpressure    = scheduler.DecisionReasonProviderRateWindowBackpressure
	dispatchSkipCurrentHeadCIWait         = scheduler.DecisionReasonCurrentHeadCIWait
	dispatchSkipWorkspaceBranchHeld       = scheduler.DecisionReasonWorkspaceBranchHeld
)

func (p dispatchPlanner) previewCurrentHeadCIWait(
	state *State,
	issue connector.Issue,
	retry Retry,
	now time.Time,
) (Retry, bool) {
	if !mergeWorkerProgrammaticMergeWaiting(issue) {
		retry.Attempt = nextAttempt(retry.Attempt)
		retry.Wait = RetryWait{}
		state.Retry[issue.ID] = retry
		return retry, false
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
	retry.DueAt = mergeWorkerCurrentHeadCINextPollAt(retry.Wait.StartedAt, now, p.cfg.ContinuationRetryDelay)
	state.Retry[issue.ID] = retry
	return retry, true
}

func (p dispatchPlanner) dispatchableIssueDecision(
	issue connector.Issue,
	state *State,
	allowClaimed bool,
	now time.Time,
	preferredWorkerHost string,
) dispatchableDecision {
	return p.dispatchableIssueDecisionForModelRequirement(
		issue,
		state,
		allowClaimed,
		now,
		preferredWorkerHost,
		p.modelPermitRequiredAtDispatch(issue),
	)
}

func (p dispatchPlanner) dispatchableIssueDecisionForModelRequirement(
	issue connector.Issue,
	state *State,
	allowClaimed bool,
	now time.Time,
	preferredWorkerHost string,
	modelPermitRequired bool,
) dispatchableDecision {
	p.now = now
	if reason := connector.NonExecutableReason(issue); reason != "" {
		return dispatchableDecision{reason: dispatchSkipInactiveState, detail: reason}
	}
	if !validCandidate(issue) {
		return dispatchableDecision{reason: dispatchSkipInvalidCandidate}
	}
	if !stateIn(issue.State, p.cfg.ActiveStates) {
		return dispatchableDecision{reason: dispatchSkipInactiveState}
	}
	if stateIn(issue.State, p.cfg.TerminalStates) {
		return dispatchableDecision{reason: dispatchSkipTerminalState}
	}
	if activeTrackerUnavailable(state) && trackerDependentDispatch(issue) {
		return dispatchableDecision{reason: dispatchSkipTrackerUnavailable}
	}
	if forgeAvailabilityBlocks(state, issue, Retry{}, p.cfg.ForgeHost, now) {
		return dispatchableDecision{reason: dispatchSkipForgeUnavailable}
	}
	if workerGitHubMonitorBlocks(state, issue.ID, Retry{}, now) {
		return dispatchableDecision{reason: dispatchSkipGitHubMonitor}
	}
	if activeCIUnavailable(state) && ciDependentDispatch(issue) {
		return dispatchableDecision{reason: dispatchSkipCIUnavailable}
	}
	if _, paused := activeGitHubRESTCapacityOutage(state, now); paused {
		return dispatchableDecision{reason: dispatchSkipGitHubRESTCapacity}
	}
	if reason := dispatchRecoveryBlockReason(state, now); reason != "" {
		return dispatchableDecision{reason: reason}
	}
	if pullRequestHydrationBlocksDispatch(issue) {
		return dispatchableDecision{reason: dispatchSkipPullRequestHydration}
	}
	if artifactGateWaitStatusBlocksDispatch(issue, p.cfg.AutoPromote.Gate) {
		return dispatchableDecision{reason: dispatchSkipArtifactGateWaitStatus}
	}
	if autoPromoteActiveGatePendingIssue(issue, state, p.cfg, p.cfg.AutoPromote) {
		return dispatchableDecision{reason: dispatchSkipAwaitingGate}
	}
	if mergedPullRequestReconciliationPending(issue, p.cfg) {
		return dispatchableDecision{reason: dispatchSkipMergedPullRequest}
	}
	if duplicatePullRequestWork(issue) {
		return dispatchableDecision{reason: dispatchSkipDuplicatePullRequest}
	}
	if authorization := p.authorizationDecision(issue); !authorization.Matched {
		return dispatchableDecision{
			reason:        dispatchSkipAuthorizationSelector,
			detail:        authorization.Detail,
			authorization: &authorization,
		}
	}
	if p.needsAssignee(issue) {
		return dispatchableDecision{reason: dispatchSkipOwnershipAssigneeRequired}
	}
	if todoBlockedByNonTerminal(issue, p.cfg.TerminalStates) {
		return dispatchableDecision{reason: dispatchSkipBlockedByDependency, detail: humanDependencyWaitReason(issue.BlockedBy)}
	}
	if _, ok := state.Running[issue.ID]; ok {
		return dispatchableDecision{reason: dispatchSkipAlreadyRunning}
	}
	if _, ok := state.deferredCompletions[issue.ID]; ok {
		return dispatchableDecision{reason: dispatchSkipCompletionDeferred}
	}
	if _, ok := state.Retry[issue.ID]; ok {
		return dispatchableDecision{reason: dispatchSkipRetryPending}
	}
	if _, ok := state.Claimed[issue.ID]; ok && !allowClaimed {
		return dispatchableDecision{reason: dispatchSkipAlreadyClaimed}
	}
	if blocked, ok := state.Blocked[issue.ID]; ok {
		reason := strings.ToLower(strings.TrimSpace(blocked.Reason))
		if reason == lifetimeLimitReason || strings.HasPrefix(reason, lifetimeLimitBlockedReasonPrefix) {
			return dispatchableDecision{reason: dispatchSkipLifetimeLimit}
		}
		return dispatchableDecision{reason: dispatchSkipBlocked}
	}
	if reason := p.budgetRefusalWaitReason(state, issue.ID, now); reason != "" {
		return dispatchableDecision{reason: reason}
	}
	if p.hardAvailableSlots(state) == 0 {
		return dispatchableDecision{reason: dispatchSkipGlobalCapacityFull}
	}
	if modelPermitRequired && p.availableSlots(state) == 0 {
		if p.rateWindowBackpressureActive(state) {
			return dispatchableDecision{reason: dispatchSkipRateWindowBackpressure}
		}
		return dispatchableDecision{reason: dispatchSkipGlobalCapacityFull}
	}
	if !p.stateSlotsAvailable(issue, state) {
		return dispatchableDecision{reason: dispatchSkipLocalSlotUnavailable}
	}
	if !p.workerSlotsAvailable(state, preferredWorkerHost) {
		return dispatchableDecision{reason: dispatchSkipWorkerHostUnavailable}
	}
	if !projectFailureBreakerAllowsDispatch(state, now) {
		return dispatchableDecision{reason: dispatchSkipProjectFailureBreaker}
	}
	return dispatchableDecision{dispatchable: true}
}

func pullRequestHydrationBlocksDispatch(issue connector.Issue) bool {
	pullRequest := issue.PullRequest
	if !pullRequestHydrationBlocksProgress(pullRequest) {
		return false
	}
	if normalizeState(issue.State) != "todo" {
		return true
	}
	return pullRequest.Number > 0 ||
		strings.TrimSpace(pullRequest.URL) != "" ||
		strings.TrimSpace(pullRequest.BranchName) != "" ||
		normalizePullRequestState(pullRequest.State) != ""
}

func artifactGateWaitStatusBlocksDispatch(issue connector.Issue, cfg gate.Config) bool {
	cfg = gate.Effective(cfg)
	if cfg.Kind != gate.KindArtifact {
		return false
	}
	status := normalizeArtifactGateStatus(artifactStatusFromIssue(issue, cfg.Artifact.StatusField))
	if status == "" {
		return false
	}
	for _, waitStatus := range cfg.Artifact.WaitStatuses {
		if status == normalizeArtifactGateStatus(waitStatus) {
			return artifactGateWaitStatusIsCurrent(issue, cfg.Artifact.StatusField)
		}
	}
	return false
}

func artifactGateWaitStatusIsCurrent(issue connector.Issue, statusField string) bool {
	for field, updatedAt := range issue.FieldUpdatedAt {
		if strings.EqualFold(strings.TrimSpace(field), strings.TrimSpace(statusField)) &&
			issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() && !updatedAt.IsZero() {
			return updatedAt.After(*issue.StageUpdatedAt)
		}
	}
	if issue.StageUpdatedAt == nil || issue.StageUpdatedAt.IsZero() || issue.UpdatedAt == nil || issue.UpdatedAt.IsZero() {
		return true
	}
	return !issue.StageUpdatedAt.Equal(*issue.UpdatedAt)
}

func normalizeArtifactGateStatus(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func (p dispatchPlanner) authorized(issue connector.Issue) bool {
	if !p.cfg.Authorization.Configured() {
		return true
	}
	return selector.Match(issue, p.cfg.Authorization, p.cfg.SelectorContext)
}

func (p dispatchPlanner) authorizationDecision(issue connector.Issue) selector.Decision {
	return selector.Decide(issue, p.cfg.Authorization, p.cfg.SelectorContext)
}

func (p dispatchPlanner) needsAssignee(issue connector.Issue) bool {
	return p.cfg.Claiming.AssigneeRequired && p.assigneeEligibilityApplies(issue)
}

func (p dispatchPlanner) assigneeEligibilityApplies(issue connector.Issue) bool {
	if !p.cfg.Claiming.OwnershipSet || p.cfg.Claiming.OwnershipMode != workflowconfig.IdentityOwnershipAssignee {
		return false
	}
	if strings.TrimSpace(issue.AssigneeID) != "" {
		return false
	}
	for _, assignee := range issue.Assignees {
		if strings.TrimSpace(assignee) != "" {
			return false
		}
	}
	return true
}

func (p dispatchPlanner) assigneeEligibilityCandidates(issues []connector.Issue) []connector.Issue {
	candidates := make([]connector.Issue, 0)
	for _, issue := range issues {
		if strings.TrimSpace(issue.ID) == "" || !validCandidate(issue) || !stateIn(issue.State, p.cfg.ActiveStates) || stateIn(issue.State, p.cfg.TerminalStates) || !p.authorized(issue) || !p.assigneeEligibilityApplies(issue) {
			continue
		}
		candidates = append(candidates, cloneIssue(issue))
	}
	return candidates
}

func (p dispatchPlanner) trackOwnershipAttentionCandidates(state *State, issues []connector.Issue, now time.Time) {
	seen := make(map[string]struct{})
	for _, issue := range issues {
		if strings.TrimSpace(issue.ID) == "" || !validCandidate(issue) || !stateIn(issue.State, p.cfg.ActiveStates) || stateIn(issue.State, p.cfg.TerminalStates) || !p.authorized(issue) || !p.needsAssignee(issue) {
			continue
		}
		seen[issue.ID] = struct{}{}
		existing, ok := state.Blocked[issue.ID]
		if ok && existing.Source == BlockedSourceOwnership {
			existing.Issue = cloneIssue(issue)
			state.Blocked[issue.ID] = existing
			continue
		}
		state.Blocked[issue.ID] = Blocked{
			Issue:          cloneIssue(issue),
			Reason:         "issue needs an assignee under ownership_mode: assignee",
			RecoveryReason: "human_blocker",
			BlockedAt:      now,
			Source:         BlockedSourceOwnership,
		}
	}
	for issueID, blocked := range state.Blocked {
		if blocked.Source != BlockedSourceOwnership {
			continue
		}
		if _, ok := seen[issueID]; ok {
			continue
		}
		delete(state.Blocked, issueID)
		delete(state.DispatchEscalations, correctableDispatchEscalationKey(issueID, dispatchSkipOwnershipAssigneeRequired))
	}
}

func (p dispatchPlanner) slotsAvailableForModelRequirement(
	issue connector.Issue,
	state *State,
	preferredWorkerHost string,
	modelPermitRequired bool,
) bool {
	if p.hardAvailableSlots(state) == 0 {
		return false
	}
	if modelPermitRequired && p.availableSlots(state) == 0 {
		return false
	}
	return p.stateSlotsAvailable(issue, state) &&
		p.workerSlotsAvailable(state, preferredWorkerHost)
}

func (p dispatchPlanner) availableSlots(state *State) int {
	return min(p.hardAvailableSlots(state), p.providerModelPermitSlots(state))
}

func (p dispatchPlanner) hardAvailableSlots(state *State) int {
	if state == nil {
		return 0
	}
	return max(0, state.MaxConcurrentAgents-len(state.Running))
}

func (p dispatchPlanner) providerModelPermitSlots(state *State) int {
	if state == nil {
		return 0
	}
	// A depleted rate window is intentionally floored to one permit, serializing project dispatch instead of stopping it.
	evaluation := evaluateProviderRateWindowPacing(
		p.cfg.BillingMode,
		p.cfg.RateWindowPacing,
		state.MaxConcurrentAgents,
		state.RateLimits,
		p.now,
	)
	available := evaluation.permitCeiling - modelPermitsUsed(state)
	return max(0, available)
}

func (p dispatchPlanner) rateWindowBackpressureActive(state *State) bool {
	if state == nil {
		return false
	}
	evaluation := evaluateProviderRateWindowPacing(
		p.cfg.BillingMode,
		p.cfg.RateWindowPacing,
		state.MaxConcurrentAgents,
		state.RateLimits,
		p.now,
	)
	return evaluation.scalingApplied && p.hardAvailableSlots(state) > 0 && p.providerModelPermitSlots(state) == 0
}

func (p dispatchPlanner) modelPermitRequiredAtDispatch(issue connector.Issue) bool {
	return !p.mechanicalMergeAdmission(issue)
}

func (p dispatchPlanner) mechanicalMergeAdmission(issue connector.Issue) bool {
	return p.cfg.MergeFastPathEnabled && normalizeState(issue.State) == normalizeState(autoPromoteMergingState)
}

func modelPermitsUsed(state *State) int {
	if state == nil {
		return 0
	}
	used := 0
	for _, running := range state.Running {
		if !running.ModelPermitExempt {
			used++
		}
	}
	return used
}

type providerRateWindowPacingEvaluation struct {
	mode                     string
	floorPercent             float64
	staleAfter               time.Duration
	applicable               bool
	bucketStatus             string
	observedRemainingPercent *float64
	observedAt               *time.Time
	permitCeiling            int
	scalingApplied           bool
}

func evaluateProviderRateWindowPacing(
	billingMode string,
	pacing workflowconfig.RateWindowPacing,
	maxConcurrent int,
	limits *telemetry.RateLimits,
	now time.Time,
) providerRateWindowPacingEvaluation {
	pacing = pacing.Normalized()
	evaluation := providerRateWindowPacingEvaluation{
		mode:          pacing.Mode,
		floorPercent:  pacing.FloorPercent,
		staleAfter:    time.Duration(pacing.StaleAfterSeconds) * time.Second,
		permitCeiling: max(0, maxConcurrent),
	}
	remaining, observedAt, status := providerRateWindowObservation(limits, now, evaluation.staleAfter)
	evaluation.bucketStatus = status
	evaluation.observedRemainingPercent = remaining
	evaluation.observedAt = observedAt
	evaluation.applicable = !strings.EqualFold(strings.TrimSpace(billingMode), workflowconfig.BillingModeMetered) && pacing.Mode != workflowconfig.RateWindowPacingOff
	if !evaluation.applicable || status != telemetry.RateWindowBucketFresh || remaining == nil || maxConcurrent <= 0 {
		return evaluation
	}
	if pacing.Mode == workflowconfig.RateWindowPacingFloor && *remaining >= pacing.FloorPercent {
		return evaluation
	}
	evaluation.permitCeiling = max(1, int(math.Ceil(float64(maxConcurrent)*(*remaining)/100)))
	evaluation.scalingApplied = evaluation.permitCeiling < maxConcurrent
	return evaluation
}

func providerRateWindowObservation(
	limits *telemetry.RateLimits,
	now time.Time,
	staleAfter time.Duration,
) (*float64, *time.Time, string) {
	if limits == nil {
		return nil, nil, telemetry.RateWindowBucketMissing
	}
	var freshRemaining *float64
	var freshObservedAt *time.Time
	var staleRemaining *float64
	var staleObservedAt *time.Time
	seen := false
	for _, bucket := range []*telemetry.RateLimitBucket{limits.Primary, limits.Secondary} {
		if bucket == nil || bucket.Limit <= 0 {
			continue
		}
		seen = true
		percent := float64(bucket.Remaining) / float64(bucket.Limit) * 100
		percent = max(0, min(100, percent))
		if bucket.ObservedAt == nil || bucket.ObservedAt.IsZero() || !now.IsZero() && now.Sub(*bucket.ObservedAt) > staleAfter {
			if staleRemaining == nil || percent < *staleRemaining {
				staleRemaining = &percent
				staleObservedAt = cloneTimePointer(bucket.ObservedAt)
			}
			continue
		}
		if freshRemaining == nil || percent < *freshRemaining {
			freshRemaining = &percent
			freshObservedAt = cloneTimePointer(bucket.ObservedAt)
		}
	}
	if freshRemaining != nil {
		return freshRemaining, freshObservedAt, telemetry.RateWindowBucketFresh
	}
	if seen {
		return staleRemaining, staleObservedAt, telemetry.RateWindowBucketStale
	}
	return nil, nil, telemetry.RateWindowBucketMissing
}

func (p dispatchPlanner) stateSlotsAvailable(issue connector.Issue, state *State) bool {
	limit := p.cfg.MaxConcurrentAgents
	if stateLimit, ok := p.cfg.MaxConcurrentAgentsByState[normalizeState(issue.State)]; ok {
		limit = stateLimit
	}

	used := 0
	normalized := normalizeState(issue.State)
	for _, running := range state.Running {
		if normalizeState(running.Issue.State) == normalized {
			used++
		}
	}

	return used < limit
}

func (p dispatchPlanner) workerSlotsAvailable(state *State, preferredWorkerHost string) bool {
	_, ok := p.selectWorkerHost(state, preferredWorkerHost)
	return ok
}

func (p dispatchPlanner) selectWorkerHost(state *State, preferredWorkerHost string) (string, bool) {
	if len(p.cfg.WorkerHosts) == 0 {
		return "", true
	}

	availableHosts := make([]string, 0, len(p.cfg.WorkerHosts))
	for _, host := range p.cfg.WorkerHosts {
		if p.workerHostSlotsAvailable(state, host) {
			availableHosts = append(availableHosts, host)
		}
	}
	if len(availableHosts) == 0 {
		return "", false
	}

	preferredWorkerHost = strings.TrimSpace(preferredWorkerHost)
	if preferredWorkerHost != "" {
		if slices.Contains(availableHosts, preferredWorkerHost) {
			return preferredWorkerHost, true
		}
	}

	return leastLoadedWorkerHost(state, availableHosts), true
}

func (p dispatchPlanner) workerHostSlotsAvailable(state *State, workerHost string) bool {
	if p.cfg.MaxConcurrentAgentsPerHost <= 0 {
		return true
	}

	return runningWorkerHostCount(state, workerHost) < p.cfg.MaxConcurrentAgentsPerHost
}

func (p dispatchPlanner) scheduleRetry(
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	err string,
	continuation bool,
	workerHost string,
) {
	if attempt < 1 {
		attempt = 1
	}

	p.scheduleRetryAfter(state, issue, attempt, now, p.retryDelay(attempt, continuation), err, workerHost)
}

func (p dispatchPlanner) rescheduleRetry(
	state *State,
	retry Retry,
	now time.Time,
	err string,
	continuation bool,
) {
	if retry.Attempt < 1 {
		retry.Attempt = 1
	}
	retry.DueAt = now.Add(p.retryDelay(retry.Attempt, continuation))
	retry.Error = p.operatorText(err)
	retry.MergePrecheck = cloneMergePrecheck(retry.MergePrecheck)
	state.Retry[retry.Issue.ID] = retry
}

func (p dispatchPlanner) scheduleRetryAfter(
	state *State,
	issue connector.Issue,
	attempt int,
	now time.Time,
	delay time.Duration,
	err string,
	workerHost string,
) {
	if attempt < 1 {
		attempt = 1
	}
	if delay < 0 {
		delay = 0
	}

	issue = cloneIssue(issue)
	state.Retry[issue.ID] = Retry{
		Issue:      issue,
		Attempt:    attempt,
		DueAt:      now.Add(delay),
		Error:      p.operatorText(err),
		WorkerHost: workerHost,
	}
}

func (p dispatchPlanner) operatorText(value string) string {
	return runtimeoutput.Truncate(strings.TrimSpace(value), p.cfg.OutputTruncationMaxBytes).Value
}

func (p dispatchPlanner) retryDelay(attempt int, continuation bool) time.Duration {
	if continuation {
		return p.cfg.ContinuationRetryDelay
	}
	if attempt < 1 {
		attempt = 1
	}
	exponent := min(attempt-1, 30)

	delay := p.cfg.FailureRetryBaseDelay * time.Duration(math.Pow(2, float64(exponent)))
	if delay > p.cfg.MaxRetryBackoff {
		return p.cfg.MaxRetryBackoff
	}
	return delay
}

func (p dispatchPlanner) releaseIssue(state *State, issueID string) {
	cancelRunning(state, issueID)
	delete(state.Running, issueID)
	delete(state.Claimed, issueID)
	delete(state.Blocked, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
}

func (p dispatchPlanner) parkBudgetHardHold(state *State, issueID string) {
	cancelRunning(state, issueID)
	delete(state.Running, issueID)
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
}

func (p dispatchPlanner) releaseClaim(state *State, issueID string) {
	cancelRunning(state, issueID)
	delete(state.Running, issueID)
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
}
