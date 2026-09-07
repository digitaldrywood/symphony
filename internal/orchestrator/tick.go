package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/providercapacity"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const githubRateLimitResetSkew = 5 * time.Second

type tickPreviousState struct {
	lastRefreshAt            time.Time
	pipeline                 []connector.Issue
	epicTransitionWatch      []connector.Issue
	blockedStatusIssues      []connector.Issue
	pendingEpicParentLookups map[string]connector.Issue
}

type tickFetchedIssues struct {
	candidates []connector.Issue
	status     []connector.Issue
	statusOK   bool
}

type tickTransitionRefresh struct {
	issues               []connector.Issue
	pendingTransitions   []connector.Issue
	pendingParentLookups map[string]connector.Issue
	blockedRefreshOK     bool
}

type githubBudgetReserveDecision struct {
	degraded       bool
	restRemaining  int64
	restReserve    int64
	graphRemaining int64
	graphReserve   int64
}

func (o *Orchestrator) tick(ctx context.Context, state *State, now time.Time) {
	o.tickWithManual(ctx, state, now, nil)
}

func (o *Orchestrator) tickManual(ctx context.Context, state *State, request manualRefreshRequest) {
	o.tickWithManual(ctx, state, request.requestedAt, &request)
}

func (o *Orchestrator) tickWithManual(ctx context.Context, state *State, now time.Time, manual *manualRefreshRequest) {
	o.beginGlobalProjectCycle()
	defer o.endGlobalProjectCycle()
	completed := false
	timing := newRefreshTiming(o.logger, o.cfg.Project.ID, manual != nil)
	timing.progress = &o.refreshProgress
	timing.next("preflight")
	defer func() {
		state.LastRefreshDuration = timing.log(ctx, completed, state)
	}()
	state.tickTransitions = &issueStateSnapshotTransitions{}
	defer func() {
		state.tickTransitions = nil
	}()
	if manual != nil {
		startManualRefresh(state, *manual, now)
	}
	restRateLimitsCaptured := false
	previous := captureTickPreviousState(state)
	o.markRefresh(state, now)
	o.observeHostPressure(ctx, state, now)
	defer func() {
		o.finishRefresh(state, now, !restRateLimitsCaptured)
		if manual != nil {
			finishManualRefresh(state, *manual, completed)
		}
	}()

	o.syncGitHubRESTCapacityOutage(state, now)
	if o.scheduling == nil && o.githubLookupBackoffGate(ctx, state, now) {
		return
	}
	if pause := o.gitHubGraphQLPause(state, now); pause > 0 {
		o.logger.Warn("github graphql polling paused", "remaining", gitHubGraphQLRemaining(state), "pause", pause)
		if o.scheduling == nil {
			return
		}
	}
	if pause := o.gitHubRESTPause(state, now); pause > 0 {
		o.logger.Warn("github rest polling paused", "remaining", gitHubRESTRemaining(state), "pause", pause)
		if o.scheduling == nil {
			return
		}
	}
	if o.trackerAvailabilityPaused(ctx, state, now) && o.scheduling == nil {
		return
	}
	if !o.retryDeferredCompletions(ctx, state, now) && o.scheduling == nil {
		return
	}

	reserve := o.githubBudgetReserveDecision(state, now)
	if reserve.degraded {
		o.logGitHubBudgetReserveDecision(reserve)
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      now,
			Event:   "github_budget_reserved",
			Message: githubBudgetReserveMessage(reserve),
		})
	}
	timing.next("status_drift")
	o.refreshStatusDrift(ctx, state, now, reserve)
	if o.scheduling == nil && o.observeGitHubLookupBackoff(state, now) {
		return
	}
	timing.next("release")
	o.evaluateRelease(ctx, state, now)
	timing.next("active_runs")
	o.refreshActiveRuns(ctx, state, now, reserve)
	if o.scheduling == nil {
		o.syncGitHubLookupBackoff(state, now)
		if _, outage, active := githubLookupBackoff(state.BackendOutages); active && outage.LastProbeResult != githubLookupProbeResultProviderDue {
			return
		}
	}
	if state.Draining || o.dispatchQuiesced() {
		return
	}
	timing.next("tracker_fetch")
	fetched, ok := o.fetchTickIssues(ctx, state, now, reserve)
	if !ok {
		return
	}
	nativeQueueTerminalIssues := o.fetchUnsafeNativeMergeQueueTerminalIssues(ctx, state, now, reserve)
	timing.next("reconciliation")
	fetched = retainUnavailablePullRequestsFromPrevious(fetched, previous)
	fetched = applyStatusPullRequestHydrationBlocksToCandidates(fetched)
	fetched = o.revalidateTickPullRequestAssociations(ctx, fetched)
	if fetched.statusOK {
		fetched = filterReconciledTickIssues(state, fetched, o.recoverStaleTodoReviews(ctx, state, fetched.status, now))
		ciIssues := mergeIssueSlices(fetched.candidates, fetched.status)
		for _, running := range state.Running {
			ciIssues = mergeIssueSlices(ciIssues, []connector.Issue{running.Issue})
		}
		o.syncCIAvailability(state, ciIssues, now)
	}
	terminalRetryTransitions := o.reconcileTerminalAttemptRetryStates(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now)
	fetched.candidates = overlayIssueStateSnapshots(fetched.candidates, terminalRetryTransitions)
	fetched.status = overlayIssueStateSnapshots(fetched.status, terminalRetryTransitions)
	o.observePullRequestHydrationSkips(mergeIssueSlices(fetched.candidates, fetched.status))
	o.restoreDurableMergeReservations(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now)
	o.reconcileMergeReservations(state, mergeIssueSlices(fetched.candidates, fetched.status), now)
	o.restoreDurableGateWaitCompletions(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status))
	fetched = filterReconciledTickIssues(state, fetched, o.reconcileOperatorStopHolds(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now))
	fetched = filterReconciledTickIssues(state, fetched, o.reconcileMergeDurationHolds(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now))

	transitions := o.refreshTransitionSets(ctx, state, fetched, previous)
	completedEpics := o.resolveCompletedEpics(ctx, state, transitions, previous)
	fetched = filterReconciledTickIssues(
		state,
		fetched,
		o.reconcileClosedCompletedIssueStatuses(ctx, state, transitions.issues, now),
	)
	reviewThreadQueueIssues := o.reconcileUnsafeNativeMergeQueueIssues(
		ctx,
		state,
		mergeIssueSlices(
			mergeIssueSlices(fetched.status, fetched.candidates),
			nativeQueueTerminalIssues,
		),
		previous.pipeline,
		now,
	)
	fetched.status = overlayNativeMergeQueueIssues(fetched.status, reviewThreadQueueIssues)
	fetched.candidates = overlayNativeMergeQueueIssues(fetched.candidates, reviewThreadQueueIssues)
	state.Pipeline = overlayNativeMergeQueueIssues(state.Pipeline, reviewThreadQueueIssues)
	if fetched.statusOK {
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.recoverBlockedIssues(ctx, state, fetched.status, now),
		)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.recoverBackendCapacityBlockedIssues(ctx, state, fetched.status, now),
		)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.autoPromoteBlockerIssues(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now),
		)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.autoUnblockDependencyIssues(ctx, state, fetched.status, now),
		)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.reviewPlanIssues(ctx, state, fetched.status, now),
		)
		autoPromoted := o.autoPromoteHumanReviewIssues(ctx, state, mergeIssueSlices(fetched.status, fetched.candidates), now)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			autoPromoted.transitioned,
		)
		fetched = filterReconciledTickIssues(
			state,
			fetched,
			o.reconcileStaleMergingPullRequestIssues(ctx, state, fetched.status, now),
		)
		fetched.candidates = mergeIssueSlices(
			fetched.candidates,
			autoPromoted.dispatchCandidates,
		)
		mergeQueueIssues := o.delegateNativeMergeQueueIssues(
			ctx,
			state,
			mergeIssueSlices(fetched.status, fetched.candidates),
			now,
		)
		fetched.status = overlayNativeMergeQueueIssues(fetched.status, mergeQueueIssues)
		fetched.candidates = overlayNativeMergeQueueIssues(fetched.candidates, mergeQueueIssues)
		state.Pipeline = overlayNativeMergeQueueIssues(state.Pipeline, mergeQueueIssues)
		fetched.candidates = mergeIssueSlices(
			fetched.candidates,
			o.mergeWorkerDispatchCandidates(state, mergeQueueIssues, now),
		)
	}
	fetched = filterReconciledTickIssues(
		state,
		fetched,
		o.reconcileStaleLinkedPullRequestIssues(ctx, state, mergeIssueSlices(fetched.candidates, fetched.status), now),
	)
	completedTransitions := o.transitionCompletedActiveIssuesToReview(ctx, state, fetched.candidates, now)
	fetched = filterReconciledTickIssues(
		state,
		fetched,
		completedTransitions.transitioned,
	)
	fetched.candidates = mergeIssueSlices(
		fetched.candidates,
		completedTransitions.dispatchCandidates,
	)
	artifactWaitTransitions := o.transitionActiveArtifactGateWaitIssuesToReview(ctx, state, fetched.candidates, now)
	fetched = filterReconciledTickIssues(
		state,
		fetched,
		artifactWaitTransitions.transitioned,
	)
	fetched = filterReconciledTickIssues(
		state,
		fetched,
		o.recoverStrandedActiveIssues(ctx, state, fetched.candidates, now),
	)
	timing.next("rate_limits")
	restCycle := o.captureConnectorRESTRateLimits(state, now)
	o.logRESTRateLimitCycle(restCycle)
	o.syncGitHubRESTCapacityOutage(state, now)
	restRateLimitsCaptured = true
	timing.next("dispatch")
	o.dispatchTickIssues(ctx, state, fetched, transitions, previous, completedEpics, now)
	timing.next("publish")
	refreshOK := refreshSucceeded(state)
	if refreshOK {
		state.BoardIssues = overlayIssueStateSnapshots(
			boardIssuesFromFetched(fetched),
			state.tickTransitions.boardIssues,
		)
		o.refreshCurrentLaneEntries(ctx, state, now)
		o.refreshStalenessWarnings(ctx, state, fetched.candidates, now)
		o.markRefreshSucceeded(state, now)
	}
	state.Pipeline = overlayIssueStateSnapshots(state.Pipeline, state.tickTransitions.pipeline)
	if refreshOK {
		timing.next("workspace_cleanup")
		o.reapDueWorkspacesAfterRefresh(ctx, state, now)
	}
	completed = true
}

func startManualRefresh(state *State, request manualRefreshRequest, now time.Time) {
	if state == nil {
		return
	}
	requestedAt := request.requestedAt.UTC()
	startedAt := now.UTC()
	state.ManualRefresh = telemetry.RefreshAttempt{
		ID:          request.id,
		Status:      telemetry.RefreshAttemptStatusInProgress,
		RequestedAt: &requestedAt,
		StartedAt:   &startedAt,
		Operations:  append([]string(nil), request.operations...),
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      startedAt,
		Event:   "manual_refresh_started",
		Message: "manual refresh " + request.id + " started",
	})
}

func finishManualRefresh(state *State, request manualRefreshRequest, completed bool) {
	if state == nil {
		return
	}
	completedAt := time.Now().UTC()
	manual := state.ManualRefresh
	if strings.TrimSpace(manual.ID) != request.id {
		manual = telemetry.RefreshAttempt{
			ID:          request.id,
			RequestedAt: timePointer(request.requestedAt),
			StartedAt:   timePointer(request.requestedAt),
			Operations:  append([]string(nil), request.operations...),
		}
	}
	manual.CompletedAt = &completedAt
	if completed && strings.TrimSpace(state.LastRefreshError) == "" && state.LastRefreshErrorAt.IsZero() {
		manual.Status = telemetry.RefreshAttemptStatusSucceeded
		manual.LastError = ""
		manual.LastErrorAt = nil
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      completedAt,
			Event:   "manual_refresh_succeeded",
			Message: "manual refresh " + request.id + " succeeded",
		})
		state.ManualRefresh = manual
		return
	}

	manual.Status = telemetry.RefreshAttemptStatusFailed
	manual.LastError = strings.TrimSpace(state.LastRefreshError)
	if manual.LastError == "" {
		manual.LastError = "manual refresh did not complete"
	}
	errorAt := state.LastRefreshErrorAt
	if errorAt.IsZero() {
		errorAt = completedAt
	}
	errorAt = errorAt.UTC()
	manual.LastErrorAt = &errorAt
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      completedAt,
		Event:   "manual_refresh_failed",
		Message: "manual refresh " + request.id + " failed: " + manual.LastError,
	})
	state.ManualRefresh = manual
}

func captureTickPreviousState(state *State) tickPreviousState {
	return tickPreviousState{
		lastRefreshAt:            state.LastRefreshAt,
		pipeline:                 cloneIssues(state.Pipeline),
		epicTransitionWatch:      cloneIssues(state.epicTransitionWatch),
		blockedStatusIssues:      blockedStatusTransitionIssues(state.Blocked),
		pendingEpicParentLookups: cloneIssueMap(state.pendingEpicParentLookups),
	}
}

func (o *Orchestrator) refreshActiveRuns(ctx context.Context, state *State, now time.Time, reserve githubBudgetReserveDecision) {
	if reserve.degraded {
		o.logger.Warn(
			"workspace cleanup skipped to preserve shared github budget",
			"rest_remaining", reserve.restRemaining,
			"rest_reserve", reserve.restReserve,
			"graphql_remaining", reserve.graphRemaining,
			"graphql_reserve", reserve.graphReserve,
		)
	} else {
		o.reapWorkspacesIfDue(ctx, state, now)
	}
	o.reconcileRunningIssues(ctx, state, now)
	if o.heartbeats == nil || !o.heartbeats.Running() {
		o.heartbeatRunningClaims(ctx, state, now)
		o.heartbeatRunningWorkAttempts(ctx, state, now)
	}
}

func (o *Orchestrator) fetchTickIssues(
	ctx context.Context,
	state *State,
	now time.Time,
	reserve githubBudgetReserveDecision,
) (tickFetchedIssues, bool) {
	observedStates := o.observedStatusFetchStatesForTick(state)
	if fetcher, ok := o.connector.(connector.RefreshIssueFetcher); o.scheduling == nil && ok && fetcher.CombinedRefreshEnabled() && !reserve.degraded {
		return o.fetchCombinedTickIssues(ctx, state, now, observedStates, fetcher)
	}

	candidateIssues, err := o.fetchCandidateIssuesForTick(ctx, state)
	if err != nil {
		o.logger.Warn("fetch candidate issues failed", "error", err)
		recordRefreshSourceFailure(state, telemetry.RefreshSourceCandidates, err, now)
		if o.scheduling == nil {
			o.observeTrackerReadFailure(state, telemetry.RefreshSourceCandidates, err, now)
		}
		markRefreshError(state, "fetch candidate issues failed: "+err.Error(), now)
		return tickFetchedIssues{}, false
	}
	recordRefreshSourceSuccess(state, telemetry.RefreshSourceCandidates, now)
	if o.scheduling == nil {
		o.recordTrackerReadSuccess(state, telemetry.RefreshSourceCandidates, now)
	}

	fetched := tickFetchedIssues{
		candidates: cloneIssues(candidateIssues),
	}
	if len(observedStates) == 0 {
		fetched.statusOK = true
		clearRefreshError(state)
		return fetched, true
	}
	if reserve.degraded {
		o.logger.Warn(
			"observed status polling skipped to preserve shared github budget",
			"state_count", len(observedStates),
			"rest_remaining", reserve.restRemaining,
			"rest_reserve", reserve.restReserve,
			"graphql_remaining", reserve.graphRemaining,
			"graphql_reserve", reserve.graphReserve,
		)
		return fetched, true
	}
	if !tickHasActiveWork(state, candidateIssues) {
		exists, probeErr := o.observedWorkExists(ctx, observedStates)
		if probeErr != nil {
			o.logger.Warn("fetch observed status probe failed", "error", probeErr)
			recordRefreshSourceFailure(state, telemetry.RefreshSourceStatuses, probeErr, now)
			o.observeTrackerReadFailure(state, telemetry.RefreshSourceStatuses, probeErr, now)
			markRefreshError(state, "fetch observed status probe failed: "+probeErr.Error(), now)
			return fetched, true
		}
		if !exists {
			fetched.statusOK = true
			recordRefreshSourceSuccess(state, telemetry.RefreshSourceStatuses, now)
			o.recordTrackerReadSuccess(state, telemetry.RefreshSourceStatuses, now)
			clearRefreshError(state)
			return fetched, true
		}
	}

	statusIssues, statusErr := o.fetchObservedIssuesByStates(ctx, observedStates)
	if statusErr != nil {
		o.logger.Warn("fetch observed status issues failed", "error", statusErr)
		recordRefreshSourceFailure(state, telemetry.RefreshSourceStatuses, statusErr, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceStatuses, statusErr, now)
		markRefreshError(state, "fetch observed status issues failed: "+statusErr.Error(), now)
		return fetched, true
	}
	recordRefreshSourceSuccess(state, telemetry.RefreshSourceStatuses, now)
	o.recordTrackerReadSuccess(state, telemetry.RefreshSourceStatuses, now)
	fetched.status = cloneIssues(statusIssues)
	fetched.statusOK = true
	if err := o.hydratePlanIssueComments(ctx, &fetched); err != nil {
		recordRefreshSourceFailure(state, telemetry.RefreshSourceStatuses, err, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceStatuses, err, now)
		markRefreshError(state, "fetch plan issue comments failed: "+err.Error(), now)
		return tickFetchedIssues{}, false
	}
	clearRefreshError(state)
	return fetched, true
}

func (o *Orchestrator) fetchCombinedTickIssues(
	ctx context.Context,
	state *State,
	now time.Time,
	observedStates []string,
	fetcher connector.RefreshIssueFetcher,
) (tickFetchedIssues, bool) {
	result := fetcher.FetchRefreshIssues(
		ctx,
		o.candidateFetchStatesForTick(state),
		observedStates,
		o.authorizationFilterHint(),
	)
	if result.CandidateError != nil {
		err := result.CandidateError
		o.logger.Warn("fetch candidate issues failed", "error", err)
		recordRefreshSourceFailure(state, telemetry.RefreshSourceCandidates, err, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceCandidates, err, now)
		markRefreshError(state, "fetch candidate issues failed: "+err.Error(), now)
		return tickFetchedIssues{}, false
	}
	recordRefreshSourceSuccess(state, telemetry.RefreshSourceCandidates, now)
	o.recordTrackerReadSuccess(state, telemetry.RefreshSourceCandidates, now)

	fetched := tickFetchedIssues{candidates: cloneIssues(result.Candidates)}
	if len(observedStates) == 0 {
		fetched.statusOK = true
		clearRefreshError(state)
		return fetched, true
	}
	if result.StatusError != nil {
		err := result.StatusError
		o.logger.Warn("fetch observed status issues failed", "error", err)
		recordRefreshSourceFailure(state, telemetry.RefreshSourceStatuses, err, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceStatuses, err, now)
		markRefreshError(state, "fetch observed status issues failed: "+err.Error(), now)
		return fetched, true
	}
	recordRefreshSourceSuccess(state, telemetry.RefreshSourceStatuses, now)
	o.recordTrackerReadSuccess(state, telemetry.RefreshSourceStatuses, now)
	fetched.status = cloneIssues(result.Statuses)
	fetched.statusOK = true
	if err := o.hydratePlanIssueComments(ctx, &fetched); err != nil {
		recordRefreshSourceFailure(state, telemetry.RefreshSourceStatuses, err, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceStatuses, err, now)
		markRefreshError(state, "fetch plan issue comments failed: "+err.Error(), now)
		return tickFetchedIssues{}, false
	}
	clearRefreshError(state)
	return fetched, true
}

func (o *Orchestrator) fetchObservedIssuesByStates(ctx context.Context, states []string) ([]connector.Issue, error) {
	if fetcher, ok := o.connector.(connector.FreshIssuesByStatesFetcher); ok {
		return fetcher.FetchFreshIssuesByStates(ctx, states)
	}
	return o.connector.FetchIssuesByStates(ctx, states)
}

func (o *Orchestrator) fetchCandidateIssuesForTick(ctx context.Context, state *State) ([]connector.Issue, error) {
	states := o.candidateFetchStatesForTick(state)
	if len(states) == 0 {
		return []connector.Issue{}, nil
	}
	if o.scheduling != nil {
		request := SchedulingRequest{
			Policy:         o.cfg.Policy,
			ProjectID:      o.cfg.Project.ID,
			Repository:     o.cfg.SchedulingRepository,
			WorkflowStates: states,
			Filter:         o.authorizationFilterHint(),
		}
		if resolver := o.providerCapacity; resolver != nil {
			request.ProviderRequirement = func(ctx context.Context, issue connector.Issue) (providercapacity.Requirement, error) {
				return resolver.DispatchCapacity(ctx, runpkg.RunRequest{Issue: issue, Mode: o.dispatchMode(ctx, state, issue), SelectorContext: o.selectorContext()})
			}
		}
		return o.scheduling.FetchCandidateIssues(ctx, request)
	}
	if fetcher, ok := o.connector.(connector.CandidateIssuesFilterFetcher); ok {
		return fetcher.FetchCandidateIssuesByStatesWithFilter(ctx, states, o.authorizationFilterHint())
	}
	if fetcher, ok := o.connector.(connector.CandidateIssuesByStatesFetcher); ok {
		return fetcher.FetchCandidateIssuesByStates(ctx, states)
	}
	return o.connector.FetchCandidateIssues(ctx)
}

func (o *Orchestrator) authorizationFilterHint() connector.IssueFilterHint {
	return authorizationFilterHint(o.cfg.Authorization, o.cfg.SelectorContext)
}

func authorizationFilterHint(auth selector.Selector, ctx selector.Context) connector.IssueFilterHint {
	return connector.IssueFilterHint{
		Authors:      resolveFilterHintIdentities(auth.AuthorIn, ctx),
		Assignees:    resolveFilterHintIdentities(auth.AssigneeIn, ctx),
		LabelInclude: filterHintStrings(auth.Labels.Include),
		LabelExclude: filterHintStrings(auth.Labels.Exclude),
	}
}

func resolveFilterHintIdentities(values []string, ctx selector.Context) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if strings.EqualFold(value, "@me") {
			for _, resolved := range []string{ctx.InstanceLogin, ctx.Persona} {
				appendFilterHintString(&out, seen, resolved)
			}
			continue
		}
		appendFilterHintString(&out, seen, value)
	}
	return out
}

func filterHintStrings(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		appendFilterHintString(&out, seen, value)
	}
	return out
}

func appendFilterHintString(out *[]string, seen map[string]struct{}, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	key := strings.ToLower(value)
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*out = append(*out, value)
}

func (o *Orchestrator) refreshStatusDrift(
	ctx context.Context,
	state *State,
	now time.Time,
	reserve githubBudgetReserveDecision,
) {
	reader, ok := o.connector.(connector.StatusDriftReader)
	if !ok {
		state.StatusDrift = connector.StatusDrift{}
		return
	}
	if reserve.degraded {
		o.logger.Warn(
			"tracker status drift polling skipped to preserve shared github budget",
			"rest_remaining", reserve.restRemaining,
			"rest_reserve", reserve.restReserve,
			"graphql_remaining", reserve.graphRemaining,
			"graphql_reserve", reserve.graphReserve,
		)
		return
	}
	drift, err := reader.FetchStatusDrift(ctx)
	if err != nil {
		o.logger.Warn("fetch tracker status drift failed", "error", err)
		recordRefreshSourceFailure(state, telemetry.RefreshSourceDrift, err, now)
		o.observeTrackerReadFailure(state, telemetry.RefreshSourceDrift, err, now)
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      now,
			Event:   "tracker_status_drift_failed",
			Message: "fetch tracker status drift failed: " + err.Error(),
		})
		return
	}
	drift.OpenTerminal = o.reconcileOpenTerminalIssueDrift(ctx, state, drift.OpenTerminal, now)
	recordRefreshSourceSuccess(state, telemetry.RefreshSourceDrift, now)
	o.recordTrackerReadSuccess(state, telemetry.RefreshSourceDrift, now)
	state.StatusDrift = cloneStatusDrift(drift)
}

func (o *Orchestrator) reconcileOpenTerminalIssueDrift(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) []connector.Issue {
	remaining := make([]connector.Issue, 0, len(issues))
	for _, issue := range issues {
		if issue.Closed {
			continue
		}
		closed, err := o.closeLandedTerminalIssue(ctx, issue)
		if err != nil {
			o.logger.Warn("reconcile open terminal issue failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
			remaining = append(remaining, issue)
			continue
		}
		if !closed {
			remaining = append(remaining, issue)
			continue
		}
		o.logger.Info("reconciled open terminal issue", "issue_id", issue.ID, "identifier", issue.Identifier)
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      now,
			Event:   "open_terminal_issue_reconciled",
			Message: "closed " + issueLabel(issue) + " after confirming its linked pull request is merged",
		})
	}
	return remaining
}

func (o *Orchestrator) candidateFetchStatesForTick(state *State) []string {
	states := append([]string(nil), o.cfg.ActiveStates...)
	if o.mergeWorkerLocalSlotsAvailable(state) {
		return states
	}
	return statesWithoutState(states, autoPromoteMergingState)
}

func markRefreshError(state *State, message string, at time.Time) {
	if state == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "tracker refresh failed"
	}
	if at.IsZero() {
		at = time.Now()
	}
	state.LastRefreshError = message
	state.LastRefreshErrorAt = at.UTC()
}

func clearRefreshError(state *State) {
	if state == nil {
		return
	}
	state.LastRefreshError = ""
	state.LastRefreshErrorAt = time.Time{}
}

func refreshSucceeded(state *State) bool {
	return state != nil &&
		strings.TrimSpace(state.LastRefreshError) == "" &&
		state.LastRefreshErrorAt.IsZero()
}

func (o *Orchestrator) observedWorkExists(ctx context.Context, observedStates []string) (bool, error) {
	if prober, ok := o.connector.(connector.IssueStateProber); ok {
		issues, err := prober.FetchIssueStateProbe(ctx, observedStates, 1)
		if err != nil {
			return false, fmt.Errorf("fetch lightweight observed status probe: %w", err)
		}
		return len(issues) > 0, nil
	}
	limiter, ok := o.connector.(connector.IssuesByStatesLimiter)
	if !ok {
		return true, nil
	}
	issues, err := limiter.FetchIssuesByStatesLimit(ctx, observedStates, 1)
	if err != nil {
		return false, fmt.Errorf("fetch bounded observed status probe: %w", err)
	}
	return len(issues) > 0, nil
}

func (o *Orchestrator) githubBudgetReserveDecision(state *State, now time.Time) githubBudgetReserveDecision {
	decision := githubBudgetReserveDecision{
		restRemaining:  gitHubRESTRemaining(state),
		restReserve:    o.cfg.GitHubRESTMinReserve,
		graphRemaining: gitHubGraphQLRemaining(state),
		graphReserve:   o.cfg.GitHubGraphQLMinReserve,
	}
	if bucket := gitHubRESTBucketFromState(state); budgetBelowReserve(bucket, o.cfg.GitHubRESTMinReserve, now) && !o.conditionalPollingEnabled() {
		decision.degraded = true
	}
	if bucket := gitHubGraphQLBucketFromState(state); budgetBelowReserve(bucket, o.cfg.GitHubGraphQLMinReserve, now) {
		decision.degraded = true
	}
	return decision
}

func (o *Orchestrator) conditionalPollingEnabled() bool {
	poller, ok := o.connector.(connector.ConditionalPoller)
	return ok && poller.ConditionalPollingEnabled()
}

func budgetBelowReserve(bucket *telemetry.RateLimitBucket, reserve int64, now time.Time) bool {
	return bucket != nil &&
		reserve > 0 &&
		bucket.Limit > 0 &&
		bucket.Remaining <= reserve &&
		(bucket.ResetAt == nil || !now.After(bucket.ResetAt.Add(githubRateLimitResetSkew)))
}

func (o *Orchestrator) logGitHubBudgetReserveDecision(decision githubBudgetReserveDecision) {
	o.logger.Warn(
		"github polling degraded to preserve shared budget",
		"rest_remaining", decision.restRemaining,
		"rest_reserve", decision.restReserve,
		"graphql_remaining", decision.graphRemaining,
		"graphql_reserve", decision.graphReserve,
	)
}

func githubBudgetReserveMessage(decision githubBudgetReserveDecision) string {
	return fmt.Sprintf(
		"GitHub polling degraded to preserve shared budget for user and AI work; REST remaining=%d reserve=%d GraphQL remaining=%d reserve=%d",
		decision.restRemaining,
		decision.restReserve,
		decision.graphRemaining,
		decision.graphReserve,
	)
}

func tickHasActiveWork(state *State, candidates []connector.Issue) bool {
	if len(candidates) > 0 {
		return true
	}
	if state == nil {
		return false
	}
	return len(state.Running) > 0 ||
		len(state.Retry) > 0 ||
		len(state.Blocked) > 0 ||
		len(state.Pipeline) > 0 ||
		len(state.epicTransitionWatch) > 0 ||
		len(state.pendingEpicParentLookups) > 0
}

func (o *Orchestrator) refreshTransitionSets(
	ctx context.Context,
	state *State,
	fetched tickFetchedIssues,
	previous tickPreviousState,
) tickTransitionRefresh {
	transitionIssues := cloneIssues(fetched.candidates)
	pipelineIssues, pipelineRefreshOK := o.fetchEpicTransitionIssueStates(ctx, previous.pipeline)
	transitionIssues = append(transitionIssues, pipelineIssues...)
	watchedIssues, watchRefreshOK := o.fetchEpicTransitionIssueStates(ctx, previous.epicTransitionWatch)
	transitionIssues = append(transitionIssues, watchedIssues...)
	blockedIssues, blockedRefreshOK := o.fetchEpicTransitionIssueStates(ctx, previous.blockedStatusIssues)
	transitionIssues = append(transitionIssues, blockedIssues...)
	pendingTransitions, pendingParentLookups := o.refreshPendingEpicParentLookups(ctx, previous.pendingEpicParentLookups)
	transitionIssues = append(transitionIssues, pendingTransitions...)

	state.epicTransitionWatch = issuesInStates(fetched.candidates, o.cfg.ActiveStates)
	if !watchRefreshOK {
		state.epicTransitionWatch = mergeIssueSlices(state.epicTransitionWatch, previous.epicTransitionWatch)
	}
	if fetched.statusOK {
		transitionIssues = append(transitionIssues, fetched.status...)
		state.Pipeline = issuesInStates(fetched.status, autoPromoteFetchStates(o.cfg.AutoPromote))
		if !pipelineRefreshOK || !o.mergeWorkerLocalSlotsAvailable(state) {
			state.Pipeline = mergeIssueSlices(state.Pipeline, previous.pipeline)
		}
	}

	return tickTransitionRefresh{
		issues:               transitionIssues,
		pendingTransitions:   pendingTransitions,
		pendingParentLookups: pendingParentLookups,
		blockedRefreshOK:     blockedRefreshOK,
	}
}

func (o *Orchestrator) resolveCompletedEpics(
	ctx context.Context,
	state *State,
	transitions tickTransitionRefresh,
	previous tickPreviousState,
) map[string]struct{} {
	previousTransitions := mergeIssueSlices(previous.pipeline, previous.epicTransitionWatch)
	previousTransitions = mergeIssueSlices(previousTransitions, previous.blockedStatusIssues)
	completedEpics, failedParentLookups := o.closeCompletedEpicsForTerminalTransitions(
		ctx,
		state,
		transitions.issues,
		previousTransitions,
		previous.lastRefreshAt,
		transitions.pendingTransitions,
	)
	state.pendingEpicParentLookups = mergeIssueMaps(transitions.pendingParentLookups, failedParentLookups)
	return completedEpics
}

func filterReconciledTickIssues(
	state *State,
	fetched tickFetchedIssues,
	reconciled map[string]struct{},
) tickFetchedIssues {
	fetched.candidates = filterReconciledIssues(fetched.candidates, reconciled)
	fetched.status = filterReconciledIssues(fetched.status, reconciled)
	state.epicTransitionWatch = filterReconciledIssues(state.epicTransitionWatch, reconciled)
	state.Pipeline = filterReconciledIssues(state.Pipeline, reconciled)
	return fetched
}

func boardIssuesFromFetched(fetched tickFetchedIssues) []connector.Issue {
	issues := cloneIssues(fetched.candidates)
	if fetched.statusOK {
		issues = mergeIssueSlices(issues, fetched.status)
	}
	return issues
}

func retainUnavailablePullRequestsFromPrevious(fetched tickFetchedIssues, previous tickPreviousState) tickFetchedIssues {
	previousIssues := mergeIssueSlices(previous.pipeline, previous.epicTransitionWatch)
	fetched.candidates = retainUnavailablePullRequests(fetched.candidates, previousIssues)
	fetched.status = retainUnavailablePullRequests(fetched.status, previousIssues)
	return fetched
}

func applyStatusPullRequestHydrationBlocksToCandidates(fetched tickFetchedIssues) tickFetchedIssues {
	if len(fetched.candidates) == 0 || len(fetched.status) == 0 {
		return fetched
	}
	statusByKey := make(map[string]connector.PullRequest, len(fetched.status))
	for _, issue := range fetched.status {
		key := issueIdentityKey(issue)
		if key == "" || !pullRequestHydrationBlocksProgress(issue.PullRequest) {
			continue
		}
		statusByKey[key] = *cloneIssue(issue).PullRequest
	}
	if len(statusByKey) == 0 {
		return fetched
	}
	candidates := cloneIssues(fetched.candidates)
	for index, issue := range candidates {
		statusPullRequest, ok := statusByKey[issueIdentityKey(issue)]
		if !ok {
			continue
		}
		if candidates[index].PullRequest == nil {
			candidates[index].PullRequest = &connector.PullRequest{}
		}
		applyPullRequestHydrationBlock(candidates[index].PullRequest, statusPullRequest)
		if candidates[index].PRNumber == nil && statusPullRequest.Number > 0 {
			prNumber := statusPullRequest.Number
			candidates[index].PRNumber = &prNumber
		}
	}
	fetched.candidates = candidates
	return fetched
}

func applyPullRequestHydrationBlock(target *connector.PullRequest, source connector.PullRequest) {
	if target == nil {
		return
	}
	if reason := strings.TrimSpace(source.HydrationUnavailableReason); reason != "" {
		target.HydrationUnavailableReason = reason
	}
	if reason := strings.TrimSpace(source.HydrationDegradedReason); reason != "" {
		target.HydrationDegradedReason = reason
	}
	if source.HydrationNextRetryAt != nil {
		target.HydrationNextRetryAt = cloneTime(source.HydrationNextRetryAt)
	}
	if target.Number == 0 && source.Number > 0 {
		target.Number = source.Number
	}
	if strings.TrimSpace(target.URL) == "" {
		target.URL = strings.TrimSpace(source.URL)
	}
	if strings.TrimSpace(target.BranchName) == "" {
		target.BranchName = strings.TrimSpace(source.BranchName)
	}
	if normalizePullRequestState(target.State) == "" {
		target.State = source.State
	}
	if strings.TrimSpace(target.MergeableState) == "" {
		target.MergeableState = source.MergeableState
	}
	if strings.TrimSpace(target.CIStatus) == "" {
		target.CIStatus = source.CIStatus
	}
	if len(target.Checks) == 0 {
		target.Checks = append([]connector.PullRequestCheck(nil), source.Checks...)
	}
	if len(target.RequiredCheckFailures) == 0 {
		target.RequiredCheckFailures = append([]connector.PullRequestCheck(nil), source.RequiredCheckFailures...)
	}
	if len(target.UnstartedChecks) == 0 {
		target.UnstartedChecks = append([]connector.PullRequestCheck(nil), source.UnstartedChecks...)
	}
	if len(target.StaleSuccessfulChecks) == 0 {
		target.StaleSuccessfulChecks = append([]connector.PullRequestCheck(nil), source.StaleSuccessfulChecks...)
	}
}

func retainUnavailablePullRequests(current []connector.Issue, previous []connector.Issue) []connector.Issue {
	if len(current) == 0 || len(previous) == 0 {
		return current
	}
	previousByKey := make(map[string]connector.Issue, len(previous))
	for _, issue := range previous {
		key := issueIdentityKey(issue)
		if key == "" {
			continue
		}
		previousByKey[key] = cloneIssue(issue)
	}
	out := cloneIssues(current)
	for index, issue := range out {
		reason := pullRequestHydrationUnavailableReason(issue.PullRequest)
		if reason == "" {
			continue
		}
		prior, ok := previousByKey[issueIdentityKey(issue)]
		if !ok || prior.PullRequest == nil {
			continue
		}
		retained := cloneIssue(prior).PullRequest
		retained.HydrationUnavailableReason = reason
		retained.HydrationNextRetryAt = cloneTime(issue.PullRequest.HydrationNextRetryAt)
		out[index].PullRequest = retained
		if out[index].PRNumber == nil && prior.PRNumber != nil {
			prNumber := *prior.PRNumber
			out[index].PRNumber = &prNumber
		}
		if out[index].PRRepository == "" {
			out[index].PRRepository = prior.PRRepository
		}
	}
	return out
}

func (o *Orchestrator) dispatchTickIssues(
	ctx context.Context,
	state *State,
	fetched tickFetchedIssues,
	transitions tickTransitionRefresh,
	previous tickPreviousState,
	completedEpics map[string]struct{},
	now time.Time,
) {
	issues := filterCompletedEpicCandidates(fetched.candidates, completedEpics)
	planner := o.dispatchPlanner()
	planner.pruneInactiveIssueBudgetRefusals(state, fetched.candidates)
	o.pruneBudgetRefusals(ctx, state, now)
	planner.trackBlockedCandidates(state, issues, now)
	candidateBlockedStatusIssues := issuesInStates(fetched.candidates, []string{blockedStatusState})
	if fetched.statusOK {
		currentBlockedStatusIssues := candidateBlockedStatusIssues
		currentBlockedStatusIssues = mergeIssueSlices(currentBlockedStatusIssues, issuesInStates(fetched.status, []string{blockedStatusState}))
		if !transitions.blockedRefreshOK {
			currentBlockedStatusIssues = mergeIssueSlices(currentBlockedStatusIssues, previous.blockedStatusIssues)
		}
		o.trackBlockedStatusIssues(ctx, state, currentBlockedStatusIssues, now)
	} else {
		o.trackCandidateBlockedStatusIssues(ctx, state, fetched.candidates, now)
	}
	o.dispatchReadyIssues(ctx, state, issues, now)
}
