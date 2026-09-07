package orchestrator

import (
	"sort"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/provenance"
	releasepkg "github.com/digitaldrywood/detent/internal/release"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	artifactGateStatusMetadataKey           = "detent.artifact_gate_status"
	autoPromoteActionMetadataKey            = "detent.auto_promote_action"
	autoPromoteReasonMetadataKey            = "detent.auto_promote_reason"
	automatedReviewModeMetadataKey          = "detent.automated_review_mode"
	automatedReviewDeadlineMetadataKey      = "detent.automated_review_deadline"
	automatedReviewTimeoutActionMetadataKey = "detent.automated_review_timeout_action"
	dispatchSkipReasonMetadataKey           = "detent.dispatch_skip_reason"
)

// Snapshot converts the orchestrator State into a telemetry.Snapshot suitable
// for publishing to the web dashboard. Slices are sorted by issue id so the
// output is deterministic.
func (s State) Snapshot(now time.Time) telemetry.Snapshot {
	s = s.authorizedSnapshotRuntime()
	poolCapacity := s.PoolCapacity
	if poolCapacity <= 0 {
		poolCapacity = s.MaxConcurrentAgents
	}
	staleAfter := refreshStaleAfter(s.PollInterval)
	sources := refreshSourceSnapshots(s.RefreshSources)
	failureThreshold := s.RefreshFailureThreshold
	if failureThreshold <= 0 {
		failureThreshold = refreshFailureDegradedThreshold
	}
	refresh := telemetry.Refresh{
		PollIntervalSeconds: int64(s.PollInterval / time.Second),
		StaleAfterSeconds:   int64(staleAfter / time.Second),
		LastDurationSeconds: durationSecondsCeil(s.LastRefreshDuration),
		FailureThreshold:    failureThreshold,
		DataSeq:             s.DataSeq,
		LastRefreshAt:       timePointer(s.LastRefreshAt),
		NextRefreshAt:       timePointer(s.NextRefreshAt),
		Sources:             sources,
	}
	if s.RefreshProgress.Stage != "" {
		progress := s.RefreshProgress
		progress.ElapsedSeconds = max(0, int64(now.Sub(progress.StartedAt)/time.Second))
		progress.StageElapsedSeconds = max(0, int64(now.Sub(progress.StageStartedAt)/time.Second))
		refresh.InFlight = &progress
	}
	if !s.ManualRefresh.IsZero() {
		manual := cloneRefreshAttempt(s.ManualRefresh)
		refresh.Manual = &manual
	}
	if source, ok := degradedRefreshSource(sources, refresh.FailureThreshold, now, staleAfter); ok {
		refresh.Status = telemetry.RefreshStatusDegraded
		refresh.LastError = source.LastError
		refresh.LastErrorAt = cloneTimePointer(source.LastErrorAt)
		if refresh.LastError == "" {
			refresh.LastError = "tracker " + string(source.Name) + " data is stale"
		}
	} else if s.LastRefreshError != "" || !s.LastRefreshErrorAt.IsZero() {
		refresh.Status = telemetry.RefreshStatusDegraded
		refresh.LastError = s.LastRefreshError
		refresh.LastErrorAt = timePointer(s.LastRefreshErrorAt)
	} else if refresh.LastRefreshAt == nil {
		refresh.Status = telemetry.RefreshStatusInitializing
	} else {
		refresh.Status = telemetry.RefreshStatusReady
	}
	boardIssues := authorizedSnapshotIssues(s.BoardIssues, s.Authorization, s.SelectorContext)
	rankingIssues := cloneIssues(boardIssues)
	for _, blocked := range s.Blocked {
		rankingIssues = append(rankingIssues, cloneIssue(blocked.Issue))
	}
	annotateUnblockerCounts(boardIssues, rankingIssues, s.ActiveStates, s.TerminalStates, s.PrioritizeUnblockers)
	clearBlockedUnblockerCounts(boardIssues, s.Blocked)
	pipeline := authorizedSnapshotIssues(s.Pipeline, s.Authorization, s.SelectorContext)
	statusDrift := authorizedStatusDrift(s.StatusDrift, s.Authorization, s.SelectorContext)
	boardIssueSnapshots := issueSnapshots(boardIssues, s.AutoPromoteQuietDuration, s.PollInterval, now, s.laneEntries)
	applyIssueRuntimeIdentities(boardIssueSnapshots, s.Running, s.WorkAttempts)
	applyIssueCompletionProgress(boardIssueSnapshots, s.WorkAttempts)
	dispatchLoops := dispatchLoopSnapshots(boardIssueSnapshots, s.WorkAttempts)
	s.applyGatePendingSnapshots(boardIssueSnapshots, boardIssues)
	s.applyAutoPromoteDecisionSnapshots(boardIssueSnapshots, boardIssues, now)
	s.applyArtifactGateWaitDispatchSnapshots(boardIssueSnapshots, boardIssues)
	strandedActiveIssues := strandedActiveIssueSnapshots(s, boardIssueSnapshots, now)
	pipelineIssueSnapshots := pipelineSnapshots(pipeline, s.AutoPromoteQuietDuration, s.PollInterval, s.MergeTimings, now, s.laneEntries)
	applyIssueRuntimeIdentities(pipelineIssueSnapshots, s.Running, s.WorkAttempts)
	applyIssueCompletionProgress(pipelineIssueSnapshots, s.WorkAttempts)
	s.applyAutoPromoteDecisionSnapshots(pipelineIssueSnapshots, pipeline, now)
	s.applyArtifactGateWaitDispatchSnapshots(pipelineIssueSnapshots, pipeline)
	snapshot := telemetry.Snapshot{
		GeneratedAt:             now,
		Instance:                s.Instance,
		Auth:                    telemetryAuthHealth(s.Auth),
		Shutdown:                shutdownSnapshot(s),
		Events:                  cloneActivityEvents(s.RecentEvents),
		Refresh:                 refresh,
		TrackerDrift:            statusDriftSnapshot(statusDrift, s.AutoPromoteQuietDuration, s.PollInterval, now, s.laneEntries),
		BoardIssues:             boardIssueSnapshots,
		Pipeline:                pipelineIssueSnapshots,
		Running:                 runningSnapshots(s.Running, s.Claimed, s.MergeTimings, now, s.laneEntries),
		WorkAttempts:            cloneTelemetryWorkAttempts(s.WorkAttempts),
		SchedulerDecisions:      cloneTelemetrySchedulerDecisions(s.SchedulerDecisions),
		Dispatch:                dispatchStatusSnapshot(s.DispatchStatus, s.DispatchStallThreshold, now),
		Release:                 releaseSnapshot(s.Release),
		Queue:                   queueSnapshots(s.Retry, s.Claimed, s.MergeTimings, now, s.laneEntries),
		Blocked:                 blockedSnapshots(s.Blocked, s.Claimed, now, s.laneEntries),
		Completed:               completedSnapshots(s.Completed, s.Claimed, now, s.laneEntries),
		RateLimits:              cloneRateLimits(s.RateLimits),
		TrackerUnavailable:      trackerUnavailableSnapshots(s.TrackerUnavailable),
		ForgeUnavailable:        forgeUnavailableSnapshots(s.ForgeUnavailable),
		GitHubMonitors:          workerGitHubMonitorSnapshots(s.GitHubMonitors),
		CIUnavailable:           ciUnavailableSnapshots(s.CIUnavailable),
		BackendOutages:          backendOutageSnapshots(s.BackendOutages),
		FailureBreakers:         projectFailureBreakerSnapshots(s),
		MemoryPressure:          s.MemoryPressure,
		IOPressure:              s.IOPressure,
		CPUPressure:             s.CPUPressure,
		DispatchLoops:           dispatchLoops,
		DispatchRecoveries:      dispatchRecoverySnapshots(s.DispatchRecoveries, s.PoolName, poolCapacity),
		StalenessWarnings:       stalenessWarningSnapshots(s.StalenessWarnings),
		StrandedActiveIssues:    strandedActiveIssues,
		CleanupFaults:           workspaceCleanupFailureSnapshots(s),
		OverloadRetriesLastHour: overloadRetriesLastHour(s.WorkAttempts, now),
		Tokens:                  tokensFromTokenTotals(s.liveTokenTotals()),
		Budget: telemetry.Budget{
			Refusals: budgetRefusalSnapshots(s.BudgetRefusals),
		},
	}
	snapshot.Dispatch.RateWindowPacing = rateWindowPacingSnapshot(s, now)
	if snapshot.Dispatch.Stalled {
		snapshot.DispatchStalls = []telemetry.DispatchStatus{snapshot.Dispatch}
	}
	snapshot.Counts = telemetry.Counts{
		Running:   len(snapshot.Running),
		Queue:     len(snapshot.Queue),
		Blocked:   len(snapshot.Blocked),
		Completed: len(snapshot.Completed),
	}
	applySnapshotLaneProvenance(&snapshot, s.laneProvenance)
	return snapshot
}

func rateWindowPacingSnapshot(state State, now time.Time) telemetry.RateWindowPacing {
	evaluation := evaluateProviderRateWindowPacing(
		state.BillingMode,
		state.RateWindowPacing,
		state.MaxConcurrentAgents,
		state.RateLimits,
		now,
	)
	return telemetry.RateWindowPacing{
		Mode:                     evaluation.mode,
		FloorPercent:             evaluation.floorPercent,
		StaleAfterSeconds:        int64(evaluation.staleAfter / time.Second),
		Applicable:               evaluation.applicable,
		BucketStatus:             evaluation.bucketStatus,
		ObservedRemainingPercent: evaluation.observedRemainingPercent,
		ObservedAt:               evaluation.observedAt,
		PermitCeiling:            evaluation.permitCeiling,
		ScalingApplied:           evaluation.scalingApplied,
	}
}

func durationSecondsCeil(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64((duration + time.Second - 1) / time.Second)
}

func dispatchStatusSnapshot(status store.ProjectDispatchStatus, threshold time.Duration, now time.Time) telemetry.DispatchStatus {
	result := telemetry.DispatchStatus{
		ProjectID:              strings.TrimSpace(status.ProjectID),
		CandidateCount:         status.CandidateCount,
		EligibleCandidateCount: status.EligibleCandidateCount,
		SelectedCount:          status.SelectedCount,
		SkippedCount:           status.SkippedCount,
		WaitReason:             strings.TrimSpace(status.WaitReason),
		WaitReasonCode:         strings.TrimSpace(status.WaitReasonCode),
		AllSkippedSince:        cloneTimePointer(status.AllSkippedSince),
		LastSelectedAt:         cloneTimePointer(status.LastSelectedAt),
		StallThresholdSeconds:  int64(threshold / time.Second),
		ObservedAt:             status.ObservedAt,
	}
	if status.LastSelectedAt != nil && !status.LastSelectedAt.IsZero() && !now.IsZero() {
		seconds := max(int64(now.Sub(*status.LastSelectedAt)/time.Second), 0)
		result.SecondsSinceLastSelected = &seconds
	}
	if status.AllSkippedSince == nil || status.AllSkippedSince.IsZero() || now.IsZero() {
		return result
	}
	result.StallDurationSeconds = max(int64(now.Sub(*status.AllSkippedSince)/time.Second), 0)
	result.Stalled = threshold > 0 &&
		status.CandidateCount > 0 &&
		status.SelectedCount == 0 &&
		status.SkippedCount == status.CandidateCount &&
		result.WaitReason != "" &&
		now.Sub(*status.AllSkippedSince) >= threshold
	result.Class = observability.Dispatch(result.Stalled, result.WaitReasonCode)
	result.NeedsHumanAttention = result.Class == observability.ClassFault
	return result
}

func ciUnavailableSnapshots(condition *CICondition) []telemetry.CICondition {
	if condition == nil {
		return nil
	}
	return []telemetry.CICondition{*condition}
}

func trackerUnavailableSnapshots(condition *TrackerCondition) []telemetry.TrackerCondition {
	if condition == nil {
		return nil
	}
	return []telemetry.TrackerCondition{*condition}
}

func forgeUnavailableSnapshots(conditions map[string]ForgeCondition) []telemetry.ForgeCondition {
	result := make([]telemetry.ForgeCondition, 0, len(conditions))
	for _, key := range sortedKeys(conditions) {
		result = append(result, conditions[key])
	}
	return result
}

func workerGitHubMonitorSnapshots(conditions map[string]GitHubMonitor) []telemetry.GitHubMonitor {
	result := make([]telemetry.GitHubMonitor, 0, len(conditions))
	for _, key := range sortedKeys(conditions) {
		result = append(result, conditions[key])
	}
	return result
}

func applySnapshotLaneProvenance(snapshot *telemetry.Snapshot, laneProvenance map[string]provenance.Attribution) {
	if snapshot == nil {
		return
	}
	apply := func(issue *telemetry.Issue) {
		if issue == nil {
			return
		}
		attribution, ok := laneProvenance[telemetryIssueLaneKey(*issue)]
		if !ok {
			return
		}
		issue.Origin = string(provenance.NormalizeOrigin(attribution.Origin))
		if attribution.Actor != nil {
			issue.OriginActor = attribution.Actor.Login
			issue.OriginActorKind = attribution.Actor.Kind
		}
	}
	for index := range snapshot.BoardIssues {
		apply(&snapshot.BoardIssues[index])
	}
	for index := range snapshot.Pipeline {
		apply(&snapshot.Pipeline[index])
	}
	for index := range snapshot.TrackerDrift.UntrackedOpen {
		apply(&snapshot.TrackerDrift.UntrackedOpen[index])
	}
	for index := range snapshot.TrackerDrift.OpenTerminal {
		apply(&snapshot.TrackerDrift.OpenTerminal[index])
	}
	for index := range snapshot.TrackerDrift.ClosedActive {
		apply(&snapshot.TrackerDrift.ClosedActive[index])
	}
	for index := range snapshot.Running {
		apply(&snapshot.Running[index].Issue)
	}
	for index := range snapshot.Queue {
		apply(&snapshot.Queue[index].Issue)
	}
	for index := range snapshot.Blocked {
		apply(&snapshot.Blocked[index].Issue)
	}
	for index := range snapshot.Completed {
		apply(&snapshot.Completed[index].Issue)
	}
}

func telemetryIssueLaneKey(issue telemetry.Issue) string {
	identity := ""
	switch {
	case strings.TrimSpace(issue.ID) != "":
		identity = "id:" + strings.TrimSpace(issue.ID)
	case strings.TrimSpace(issue.Identifier) != "":
		identity = "identifier:" + strings.TrimSpace(issue.Identifier)
	case strings.TrimSpace(issue.URL) != "":
		identity = "url:" + strings.TrimSpace(issue.URL)
	}
	state := normalizeState(issue.State)
	if identity == "" || state == "" {
		return ""
	}
	return identity + "\x00" + state
}

func projectFailureBreakerSnapshots(state State) []telemetry.FailureBreaker {
	breaker := state.FailureBreaker
	if !breaker.Active() {
		return nil
	}
	failures := breaker.Failures[breaker.Class]
	items := make([]telemetry.FailureBreakerItem, 0, len(failures))
	itemIndexes := make(map[string]int, len(failures))
	representative := ProjectFailure{}
	for _, failure := range failures {
		issueID := strings.TrimSpace(failure.IssueID)
		index, ok := itemIndexes[issueID]
		if !ok {
			index = len(items)
			itemIndexes[issueID] = index
			items = append(items, telemetry.FailureBreakerItem{
				IssueID:      issueID,
				Identifier:   strings.TrimSpace(failure.Identifier),
				IssueURL:     strings.TrimSpace(failure.IssueURL),
				Title:        strings.TrimSpace(failure.Title),
				AttemptCount: 1,
			})
		} else {
			items[index].AttemptCount++
		}
		if failure.ErrorMessage != "" || failure.Cause != "" || failure.BackendID != "" || failure.BackendKind != "" || failure.Provider != "" {
			representative = failure
		}
	}
	for index := range items {
		items[index] = failureBreakerItemCurrentState(state, items[index])
	}
	attemptCount := len(failures)
	if attemptCount == 0 {
		attemptCount = breaker.Count
	}
	candidateCount := state.DispatchStatus.EligibleCandidateCount
	row := telemetry.FailureBreaker{
		Class:                  breaker.Class,
		Count:                  breaker.Count,
		AttemptCount:           attemptCount,
		DistinctItemCount:      len(items),
		Cause:                  failureBreakerCause(breaker.Class, representative.Cause, representative.ErrorMessage),
		RepresentativeError:    representative.ErrorMessage,
		BackendID:              representative.BackendID,
		BackendKind:            representative.BackendKind,
		Provider:               representative.Provider,
		EligibleCandidateCount: &candidateCount,
		Items:                  items,
		WindowSeconds:          int64(breaker.Config.Window / time.Second),
		CooldownSeconds:        int64(breaker.Config.Cooldown / time.Second),
		FirstFailureAt:         breaker.FirstFailureAt,
		TrippedAt:              breaker.TrippedAt,
		ResumeAt:               breaker.ResumeAt,
		CanaryIssueID:          breaker.CanaryIssueID,
	}
	if scope := (backendcapacity.Scope{BackendID: row.BackendID, BackendKind: row.BackendKind, Provider: row.Provider}).Normalize(); scope.BackendID != "" || scope.BackendKind != "" || scope.Provider != "" {
		if _, outage, ok := matchingBackendOutage(state.BackendOutages, scope); ok {
			value := backendOutageSnapshot(outage)
			row.BackendOutage = &value
		}
	}
	return []telemetry.FailureBreaker{row}
}

func failureBreakerItemCurrentState(state State, item telemetry.FailureBreakerItem) telemetry.FailureBreakerItem {
	issueID := strings.TrimSpace(item.IssueID)
	if blocked, ok := state.Blocked[issueID]; ok {
		item.CurrentState = strings.TrimSpace(blocked.Issue.State)
		if item.CurrentState == "" {
			item.CurrentState = "Blocked"
		}
		item.Parked = true
		item.RecoveryAction = strings.TrimSpace(blocked.RecoveryAction)
		item.RecoveryReason = strings.TrimSpace(blocked.RecoveryReason)
		item.RecoveryIntentResumable = blocked.RecoveryIntentResumable
		return item
	}
	if running, ok := state.Running[issueID]; ok {
		item.CurrentState = strings.TrimSpace(running.Issue.State)
		return item
	}
	if retry, ok := state.Retry[issueID]; ok {
		item.CurrentState = strings.TrimSpace(retry.Issue.State)
		return item
	}
	if completed, ok := state.Completed[issueID]; ok {
		item.CurrentState = strings.TrimSpace(completed.Issue.State)
		return item
	}
	for _, issues := range [][]connector.Issue{state.BoardIssues, state.Pipeline} {
		for _, issue := range issues {
			if strings.TrimSpace(issue.ID) == issueID {
				item.CurrentState = strings.TrimSpace(issue.State)
				return item
			}
		}
	}
	return item
}

func failureBreakerCause(class string, cause string, representativeError string) string {
	if cause = strings.TrimSpace(cause); cause != "" {
		return cause
	}
	if representativeError = strings.TrimSpace(representativeError); representativeError != "" {
		return representativeError
	}
	normalized := strings.TrimSpace(class)
	prefix, _, _ := strings.Cut(normalized, ":")
	switch prefix {
	case projectFailureClassSessionTokenCeiling:
		return "agent session token ceiling reached"
	case projectFailureClassDeliverableCommand:
		return "deliverable command failed"
	case projectFailureClassNoProgress:
		return "agent made no work-product progress"
	case projectFailureClassRunnerFinalState:
		return "runner ended in a failed state"
	case projectFailureClassBackendError:
		return "agent backend returned an error"
	case projectFailureClassRunnerError:
		return "runner failed"
	default:
		return strings.ReplaceAll(normalized, "_", " ")
	}
}

func overloadRetriesLastHour(attempts []telemetry.WorkAttempt, now time.Time) int {
	cutoff := now.Add(-time.Hour)
	count := 0
	for _, attempt := range attempts {
		if attempt.ErrorClass != backendcapacity.TransientOverloadErrorClass || attempt.CompletedAt == nil {
			continue
		}
		completedAt := attempt.CompletedAt.UTC()
		if completedAt.Before(cutoff) || completedAt.After(now) {
			continue
		}
		count++
	}
	return count
}

func releaseSnapshot(status releasepkg.Status) telemetry.Release {
	return telemetry.Release{
		ProjectID:        "",
		Enabled:          status.Enabled,
		State:            status.State,
		LastRelease:      status.LastRelease,
		LastReleaseAt:    timePointer(status.LastReleaseAt),
		UnreleasedMerges: status.UnreleasedMerges,
		NextTriggerAt:    timePointer(status.NextTriggerAt),
		CandidateSHA:     status.CandidateSHA,
		PendingTag:       status.PendingTag,
		LastError:        status.LastError,
	}
}

func backendOutageSnapshots(outages map[string]BackendOutage) []telemetry.BackendOutage {
	keys := sortedKeys(outages)
	rows := make([]telemetry.BackendOutage, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, backendOutageSnapshot(outages[key]))
	}
	return rows
}

func backendOutageSnapshot(outage BackendOutage) telemetry.BackendOutage {
	row := telemetry.BackendOutage{
		BackendID:       outage.Scope.BackendID,
		BackendKind:     outage.Scope.BackendKind,
		Provider:        outage.Scope.Provider,
		Kind:            outage.Kind,
		Reason:          outage.Reason,
		Trigger:         outage.Trigger,
		DetectedAt:      outage.DetectedAt,
		LastObservedAt:  outage.LastObservedAt,
		ResumeAt:        outage.ResumeAt,
		NextProbeAt:     timePointer(outage.NextProbeAt),
		LastProbeAt:     timePointer(outage.LastProbeAt),
		LastProbeResult: outage.LastProbeResult,
		LastProbeDetail: outage.LastProbeDetail,
		ProbeAttempts:   outage.ProbeAttempts,
		ProbeIssueID:    outage.ProbeIssueID,
	}
	if !outage.ResetAt.IsZero() {
		resetAt := outage.ResetAt
		row.ResetAt = &resetAt
	}
	return row
}

func (s State) applyGatePendingSnapshots(snapshots []telemetry.Issue, issues []connector.Issue) {
	if len(snapshots) == 0 || len(issues) == 0 {
		return
	}
	cfg := Config{
		AutoPromote:    s.AutoPromote,
		ActiveStates:   append([]string(nil), s.ActiveStates...),
		TerminalStates: append([]string(nil), s.TerminalStates...),
	}
	for i := range snapshots {
		if i >= len(issues) {
			return
		}
		issueID := strings.TrimSpace(issues[i].ID)
		if completed, ok := s.Completed[issueID]; ok && !completed.CompletedAt.IsZero() {
			autoCfg := normalizeAutoPromoteConfig(s.AutoPromote)
			mode := gate.AutomatedReviewMode(autoCfg.Gate)
			if autoCfg.Gate.Kind == gate.KindCommand && mode != gate.AutomatedReviewOff {
				if snapshots[i].Metadata == nil {
					snapshots[i].Metadata = map[string]string{}
				}
				snapshots[i].Metadata[automatedReviewModeMetadataKey] = mode
				snapshots[i].Metadata[automatedReviewDeadlineMetadataKey] = completed.CompletedAt.Add(autoCfg.GateWaitTimeout).UTC().Format(time.RFC3339)
				snapshots[i].Metadata[automatedReviewTimeoutActionMetadataKey] = autoCfg.GateWaitTimeoutAction
			}
		}
		if autoPromoteActiveGatePendingIssue(issues[i], &s, cfg, s.AutoPromote) {
			snapshots[i].GatePending = true
		}
	}
}

func (s State) applyAutoPromoteDecisionSnapshots(snapshots []telemetry.Issue, issues []connector.Issue, now time.Time) {
	if len(snapshots) == 0 || len(issues) == 0 {
		return
	}
	for i := range snapshots {
		if i >= len(issues) {
			return
		}
		issueID := strings.TrimSpace(issues[i].ID)
		if required, ok := s.RequiredGates[issueID]; ok && issues[i].PullRequest != nil &&
			required.PRNumber == issues[i].PullRequest.Number && required.HeadSHA == strings.TrimSpace(issues[i].PullRequest.HeadSHA) &&
			required.BaseSHA == strings.TrimSpace(issues[i].PullRequest.BaseSHA) {
			snapshots[i].RequiredGate = &required
		}
		decision, ok := s.AutoPromoteDecisions[issueID]
		if !ok {
			if !s.shouldComputeAutoPromoteSnapshotDecision(issues[i]) {
				continue
			}
			summary := AutoPromoteSummaryFromIssue(issues[i])
			summary.CompletedFinalState = autoPromoteCompletedFinalState(&s, issueID)
			summary.OperationalCompletionAccepted = autoPromoteOperationalCompletionAccepted(&s, issueID)
			summary.AutomatedReviewWaitExpired = autoPromoteReviewWaitExpired(&s, issueID, s.AutoPromote, now)
			decision = EvaluateAutoPromote(issues[i], summary, s.AutoPromote, now)
		}
		if !autoPromoteDecisionVisibleOnCard(decision) {
			continue
		}
		if snapshots[i].Metadata == nil {
			snapshots[i].Metadata = map[string]string{}
		}
		snapshots[i].Metadata[autoPromoteActionMetadataKey] = string(decision.Action)
		snapshots[i].Metadata[autoPromoteReasonMetadataKey] = string(decision.Reason)
	}
}

func (s State) applyArtifactGateWaitDispatchSnapshots(snapshots []telemetry.Issue, issues []connector.Issue) {
	for i := range snapshots {
		if i >= len(issues) {
			return
		}
		issue := issues[i]
		if !stateIn(issue.State, s.ActiveStates) || stateIn(issue.State, s.TerminalStates) ||
			!artifactGateWaitStatusBlocksDispatch(issue, s.AutoPromote.Gate) {
			continue
		}
		if snapshots[i].Metadata == nil {
			snapshots[i].Metadata = map[string]string{}
		}
		snapshots[i].Metadata[dispatchSkipReasonMetadataKey] = dispatchSkipArtifactGateWaitStatus
		snapshots[i].Metadata[artifactGateStatusMetadataKey] = strings.TrimSpace(artifactStatusFromIssue(issue, s.AutoPromote.Gate.Artifact.StatusField))
	}
}

func (s State) shouldComputeAutoPromoteSnapshotDecision(issue connector.Issue) bool {
	cfg := normalizeAutoPromoteConfig(s.AutoPromote)
	if normalizeState(issue.State) == normalizeState(cfg.SourceState) {
		return true
	}
	return autoPromoteActiveGatePendingIssue(issue, &s, Config{
		AutoPromote:    cfg,
		ActiveStates:   append([]string(nil), s.ActiveStates...),
		TerminalStates: append([]string(nil), s.TerminalStates...),
	}, cfg)
}

func autoPromoteDecisionVisibleOnCard(decision AutoPromoteDecision) bool {
	return decision.Action == AutoPromoteActionAwaitReview || decision.Action == AutoPromoteActionSkip
}

func authorizedSnapshotIssues(issues []connector.Issue, authorization selector.Selector, ctx selector.Context) []connector.Issue {
	if !authorization.Configured() {
		return issues
	}
	out := make([]connector.Issue, 0, len(issues))
	for _, issue := range issues {
		if snapshotIssueAuthorized(issue, authorization, ctx) {
			out = append(out, issue)
		}
	}
	return out
}

func snapshotIssueAuthorized(issue connector.Issue, authorization selector.Selector, ctx selector.Context) bool {
	return !authorization.Configured() || selector.Match(issue, authorization, ctx)
}

type snapshotRuntimeAuthorizer struct {
	authorization selector.Selector
	ctx           selector.Context
	byID          map[string]connector.Issue
	byIdentifier  map[string]connector.Issue
	byURL         map[string]connector.Issue
}

func newSnapshotRuntimeAuthorizer(state State) snapshotRuntimeAuthorizer {
	authorizer := snapshotRuntimeAuthorizer{
		authorization: state.Authorization,
		ctx:           state.SelectorContext,
		byID:          map[string]connector.Issue{},
		byIdentifier:  map[string]connector.Issue{},
		byURL:         map[string]connector.Issue{},
	}
	for _, issue := range state.BoardIssues {
		authorizer.add(issue)
	}
	for _, issue := range state.Pipeline {
		authorizer.add(issue)
	}
	for _, id := range sortedKeys(state.Retry) {
		authorizer.add(state.Retry[id].Issue)
	}
	for _, id := range sortedKeys(state.Running) {
		authorizer.add(state.Running[id].Issue)
	}
	for _, id := range sortedKeys(state.Blocked) {
		authorizer.add(state.Blocked[id].Issue)
	}
	for _, id := range sortedKeys(state.Completed) {
		authorizer.add(state.Completed[id].Issue)
	}
	return authorizer
}

func (a snapshotRuntimeAuthorizer) add(issue connector.Issue) {
	if id := strings.TrimSpace(issue.ID); id != "" {
		if _, exists := a.byID[id]; !exists {
			a.byID[id] = issue
		}
	}
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" {
		if _, exists := a.byIdentifier[identifier]; !exists {
			a.byIdentifier[identifier] = issue
		}
	}
	if issueURL := strings.TrimSpace(issue.URL); issueURL != "" {
		if _, exists := a.byURL[issueURL]; !exists {
			a.byURL[issueURL] = issue
		}
	}
}

func (a snapshotRuntimeAuthorizer) authorized(issue connector.Issue) bool {
	if !a.authorization.Configured() {
		return true
	}
	if current, ok := a.byID[strings.TrimSpace(issue.ID)]; ok {
		issue = current
	} else if current, ok := a.byIdentifier[strings.TrimSpace(issue.Identifier)]; ok {
		issue = current
	} else if current, ok := a.byURL[strings.TrimSpace(issue.URL)]; ok {
		issue = current
	}
	return snapshotIssueAuthorized(issue, a.authorization, a.ctx)
}

func (s State) authorizedSnapshotRuntime() State {
	if !s.Authorization.Configured() {
		return s
	}
	authorizer := newSnapshotRuntimeAuthorizer(s)
	s.Retry = authorizedRuntimeEntries(s.Retry, authorizer, func(entry Retry) connector.Issue { return entry.Issue })
	s.Running = authorizedRuntimeEntries(s.Running, authorizer, func(entry Running) connector.Issue { return entry.Issue })
	s.Blocked = authorizedRuntimeEntries(s.Blocked, authorizer, func(entry Blocked) connector.Issue { return entry.Issue })
	s.Completed = authorizedRuntimeEntries(s.Completed, authorizer, func(entry Completed) connector.Issue { return entry.Issue })
	s.WorkAttempts = authorizedSnapshotWorkAttempts(s.WorkAttempts, authorizer)
	return s
}

func authorizedRuntimeEntries[T any](entries map[string]T, authorizer snapshotRuntimeAuthorizer, issue func(T) connector.Issue) map[string]T {
	out := make(map[string]T, len(entries))
	for id, entry := range entries {
		if authorizer.authorized(issue(entry)) {
			out[id] = entry
		}
	}
	return out
}

func authorizedSnapshotWorkAttempts(attempts []telemetry.WorkAttempt, authorizer snapshotRuntimeAuthorizer) []telemetry.WorkAttempt {
	out := make([]telemetry.WorkAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		issue := connector.Issue{ID: attempt.IssueID, Identifier: attempt.Identifier, URL: attempt.IssueURL}
		if authorizer.authorized(issue) {
			out = append(out, attempt)
		}
	}
	return out
}

func authorizedStatusDrift(drift connector.StatusDrift, authorization selector.Selector, ctx selector.Context) connector.StatusDrift {
	if !authorization.Configured() {
		return drift
	}
	return connector.StatusDrift{
		UntrackedOpen: authorizedSnapshotIssues(drift.UntrackedOpen, authorization, ctx),
		OpenTerminal:  authorizedSnapshotIssues(drift.OpenTerminal, authorization, ctx),
		ClosedActive:  authorizedSnapshotIssues(drift.ClosedActive, authorization, ctx),
	}
}

func statusDriftSnapshot(
	drift connector.StatusDrift,
	quietDuration time.Duration,
	pollInterval time.Duration,
	now time.Time,
	laneEntries map[string]time.Time,
) telemetry.TrackerDrift {
	return telemetry.TrackerDrift{
		UntrackedOpen: issueSnapshots(drift.UntrackedOpen, quietDuration, pollInterval, now, laneEntries),
		OpenTerminal:  issueSnapshots(drift.OpenTerminal, quietDuration, pollInterval, now, laneEntries),
		ClosedActive:  issueSnapshots(drift.ClosedActive, quietDuration, pollInterval, now, laneEntries),
	}
}

func telemetryAuthHealth(health connector.AuthHealth) telemetry.AuthHealth {
	out := telemetry.AuthHealth{
		Status:    telemetry.AuthStatus(health.Status),
		LastError: health.LastError,
	}
	if !health.LastErrorAt.IsZero() {
		value := health.LastErrorAt
		out.LastErrorAt = &value
	}
	if !health.LastRecoveredAt.IsZero() {
		value := health.LastRecoveredAt
		out.LastRecoveredAt = &value
	}
	return out
}

func shutdownSnapshot(state State) telemetry.Shutdown {
	if !state.Draining {
		return telemetry.Shutdown{Status: "running"}
	}

	return telemetry.Shutdown{
		Status:            "draining",
		Draining:          true,
		SessionsRemaining: len(state.Running),
		RequestedAt:       timePointer(state.DrainStartedAt),
	}
}

func instanceSnapshot(cfg Config) telemetry.Instance {
	return telemetry.Instance{
		Name:                    cfg.SelectorContext.Persona,
		GitHubLogin:             cfg.SelectorContext.InstanceLogin,
		AuthorizationScope:      selector.Describe(cfg.Authorization, cfg.SelectorContext),
		AuthorizationConfigured: cfg.Authorization.Configured(),
	}
}

func pipelineSnapshots(
	issues []connector.Issue,
	quietDuration time.Duration,
	pollInterval time.Duration,
	mergeTimings map[string]MergeTiming,
	now time.Time,
	laneEntries map[string]time.Time,
) []telemetry.Issue {
	out := make([]telemetry.Issue, 0, len(issues))
	for _, issue := range issues {
		item := telemetryIssue(issue, quietDuration, pollInterval, now, laneEntries)
		applyMergeTimingSnapshot(&item, issue, mergeTimings[strings.TrimSpace(issue.ID)], now)
		out = append(out, item)
	}
	return out
}

func issueSnapshots(issues []connector.Issue, quietDuration time.Duration, pollInterval time.Duration, now time.Time, laneEntries map[string]time.Time) []telemetry.Issue {
	out := make([]telemetry.Issue, 0, len(issues))
	for _, issue := range issues {
		out = append(out, telemetryIssue(issue, quietDuration, pollInterval, now, laneEntries))
	}
	return out
}

func runningSnapshots(running map[string]Running, claims map[string]Claimed, mergeTimings map[string]MergeTiming, now time.Time, laneEntries map[string]time.Time) []telemetry.Running {
	ids := sortedKeys(running)
	out := make([]telemetry.Running, 0, len(ids))
	for _, id := range ids {
		entry := running[id]
		lastEventAt := timePointer(entry.LastEventAt)
		issue := telemetryIssue(entry.Issue, 0, 0, now, laneEntries)
		timing := mergeTimings[strings.TrimSpace(entry.Issue.ID)]
		if mergeWorkerIssue(entry.Issue) && timing.MergeStartedAt.IsZero() && !entry.StartedAt.IsZero() {
			timing.MergeStartedAt = entry.StartedAt
		}
		applyMergeTimingSnapshot(&issue, entry.Issue, timing, now)
		applyClaimSnapshot(&issue, claims[id], now)
		out = append(out, telemetry.Running{
			Issue:                 issue,
			Attempt:               entry.Attempt,
			WorkAttemptID:         entry.WorkAttemptID,
			StopDestination:       entry.StopDestination,
			StopPriorityOptions:   append([]telemetry.StopRunPriorityOption(nil), entry.StopPriorityOptions...),
			DetentSessionID:       entry.DetentSessionID,
			WorkerHost:            entry.WorkerHost,
			ProcessIdentity:       entry.ProcessIdentity,
			WorkspacePath:         entry.WorkspacePath,
			SessionID:             entry.SessionID,
			StartedAt:             entry.StartedAt,
			LastEventAt:           lastEventAt,
			LastEvent:             entry.LastEvent,
			LastMessage:           entry.LastMessage,
			LastMessageTruncation: runtimeoutput.CloneTruncation(entry.LastMessageTruncation),
			RecentEvents:          cloneActivityEvents(entry.RecentEvents),
			TurnCount:             entry.TurnCount,
			RuntimeSeconds:        entry.Tokens.RuntimeSeconds,
			DiffAdded:             entry.DiffStats.AddedLines,
			DiffRemoved:           entry.DiffStats.RemovedLines,
			DiffFiles:             entry.DiffStats.FilesChanged,
			DiffStatus:            entry.DiffStats.Status,
			Tokens:                tokensFromTokenTotals(entry.Tokens),
			RSSBytes:              entry.RSSBytes,
			RSSCeilingBytes:       entry.RSSCeilingBytes,
			RSSObservedAt:         entry.RSSObservedAt,
		})
		out[len(out)-1].RuntimeIdentity = entry.RuntimeIdentity
	}
	return out
}

func queueSnapshots(retry map[string]Retry, claims map[string]Claimed, mergeTimings map[string]MergeTiming, now time.Time, laneEntries map[string]time.Time) []telemetry.Queued {
	ids := sortedKeys(retry)
	out := make([]telemetry.Queued, 0, len(ids))
	for _, id := range ids {
		entry := retry[id]
		issue := telemetryIssue(entry.Issue, 0, 0, now, laneEntries)
		applyMergeTimingSnapshot(&issue, entry.Issue, mergeTimings[strings.TrimSpace(entry.Issue.ID)], now)
		applyClaimSnapshot(&issue, claims[id], now)
		queued := telemetry.Queued{
			Issue:      issue,
			Attempt:    entry.Attempt,
			Error:      entry.Error,
			WorkerHost: entry.WorkerHost,
			QueueState: telemetry.QueueStateRetrying,
		}
		if entry.Wait.Kind == retryWaitCurrentHeadCI {
			queued.QueueState = telemetry.QueueStateWaitingOnCI
			queued.WaitStartedAt = timePointer(entry.Wait.StartedAt)
			queued.PollCount = entry.Wait.PollCount
			queued.PendingChecks = append([]string(nil), entry.Wait.PendingChecks...)
			queued.WorkspaceCreateCount = entry.Wait.WorkspaceCreateCount
			queued.WorkspaceDestroyCount = entry.Wait.WorkspaceDestroyCount
		}
		if entry.CompletionDeferred {
			queued.QueueState = telemetry.QueueStateWaitingOnTracker
		}
		if !entry.DueAt.IsZero() {
			dueAt := entry.DueAt
			queued.DueAt = &dueAt
		}
		out = append(out, queued)
	}
	return out
}

func blockedSnapshots(blocked map[string]Blocked, claims map[string]Claimed, now time.Time, laneEntries map[string]time.Time) []telemetry.Blocked {
	ids := sortedKeys(blocked)
	out := make([]telemetry.Blocked, 0, len(ids))
	for _, id := range ids {
		entry := blocked[id]
		issue := telemetryIssue(entry.Issue, 0, 0, now, laneEntries)
		applyClaimSnapshot(&issue, claims[id], now)
		item := telemetry.Blocked{
			Issue:                   issue,
			Error:                   entry.Reason,
			AttemptError:            entry.AttemptError,
			Source:                  entry.Source,
			RecoveryAction:          entry.RecoveryAction,
			RecoveryReason:          entry.RecoveryReason,
			RecoveryTarget:          entry.RecoveryTarget,
			RecoveryRemedy:          entry.RecoveryRemedy,
			RecoveryReachability:    entry.RecoveryReachability,
			RecoveryIntentResumable: entry.RecoveryIntentResumable,
			NeedsHumanAttention:     entry.NeedsHumanAttention,
			BlockerEvidence:         cloneBlockerEvidence(entry.BlockerEvidence),
			RecoveryRoot:            entry.RecoveryRoot,
			Attempt:                 entry.Attempt,
			WorkAttemptID:           entry.WorkAttemptID,
			DetentSessionID:         entry.DetentSessionID,
			SessionID:               entry.SessionID,
			Destination:             entry.Destination,
			Priority:                entry.Priority,
			PriorityName:            entry.PriorityName,
			StopReason:              entry.StopReason,
		}
		if !entry.BlockedAt.IsZero() {
			blockedAt := entry.BlockedAt
			item.BlockedAt = &blockedAt
		}
		out = append(out, item)
	}
	return out
}

func completedSnapshots(completed map[string]Completed, claims map[string]Claimed, now time.Time, laneEntries map[string]time.Time) []telemetry.Completed {
	ids := sortedKeys(completed)
	out := make([]telemetry.Completed, 0, len(ids))
	for _, id := range ids {
		entry := completed[id]
		issue := telemetryIssue(entry.Issue, 0, 0, now, laneEntries)
		applyMergeTimingSnapshot(&issue, entry.Issue, entry.MergeTiming, entry.CompletedAt)
		applyClaimSnapshot(&issue, claims[id], now)
		out = append(out, telemetry.Completed{
			Issue:          issue,
			SessionID:      entry.SessionID,
			StartedAt:      entry.StartedAt,
			CompletedAt:    entry.CompletedAt,
			FinalState:     entry.FinalState,
			Model:          entry.RuntimeIdentity.Model(),
			RuntimeSeconds: entry.Tokens.RuntimeSeconds,
			Tokens:         tokensFromTokenTotals(entry.Tokens),
		})
		out[len(out)-1].RuntimeIdentity = entry.RuntimeIdentity
	}
	return out
}

func applyIssueRuntimeIdentities(issues []telemetry.Issue, running map[string]Running, attempts []telemetry.WorkAttempt) {
	for index := range issues {
		for _, entry := range running {
			if snapshotIssueMatches(issues[index], entry.Issue.ID, entry.Issue.Identifier, entry.Issue.URL) && !entry.RuntimeIdentity.IsZero() {
				issues[index].RuntimeIdentity = entry.RuntimeIdentity
				break
			}
		}
		if !issues[index].RuntimeIdentity.IsZero() {
			continue
		}
		for _, attempt := range attempts {
			if snapshotIssueMatches(issues[index], attempt.IssueID, attempt.Identifier, attempt.IssueURL) && !attempt.RuntimeIdentity.IsZero() {
				issues[index].RuntimeIdentity = attempt.RuntimeIdentity
				break
			}
		}
	}
}

func applyIssueCompletionProgress(issues []telemetry.Issue, attempts []telemetry.WorkAttempt) {
	for index := range issues {
		var latest *telemetry.WorkAttempt
		for attemptIndex := range attempts {
			attempt := &attempts[attemptIndex]
			if !strings.EqualFold(strings.TrimSpace(attempt.Status), string(store.WorkAttemptStatusTerminal)) ||
				!snapshotIssueMatches(issues[index], attempt.IssueID, attempt.Identifier, attempt.IssueURL) {
				continue
			}
			if latest == nil || workAttemptCompletedAfter(*attempt, *latest) {
				latest = attempt
			}
		}
		if latest == nil {
			continue
		}
		record, ok := implementProgressRecordFromAnyAttempt(store.WorkAttempt{
			TerminalState:      store.WorkAttemptTerminalState(strings.TrimSpace(latest.TerminalState)),
			WorkerMetadataJSON: latest.WorkerMetadataJSON,
		})
		if !ok {
			continue
		}
		issues[index].CompletionProgress = telemetry.CompletionProgress{
			Outcome:               strings.TrimSpace(record.Outcome),
			Reason:                strings.TrimSpace(record.Reason),
			Kinds:                 append([]string(nil), record.ProgressKinds...),
			CompletionKind:        strings.TrimSpace(record.CompletionKind),
			ConsecutiveNoProgress: record.ConsecutiveNoProgress,
			NoProgressLimit:       record.NoProgressLimit,
		}
	}
}

func dispatchLoopSnapshots(issues []telemetry.Issue, attempts []telemetry.WorkAttempt) []telemetry.DispatchLoop {
	var loops []telemetry.DispatchLoop
	for _, issue := range issues {
		progress := issue.CompletionProgress
		if progress.NoProgressLimit <= 0 || progress.ConsecutiveNoProgress < 2 {
			continue
		}
		var latest *telemetry.WorkAttempt
		for attemptIndex := range attempts {
			attempt := &attempts[attemptIndex]
			if !strings.EqualFold(strings.TrimSpace(attempt.Status), string(store.WorkAttemptStatusTerminal)) ||
				!snapshotIssueMatches(issue, attempt.IssueID, attempt.Identifier, attempt.IssueURL) {
				continue
			}
			if latest == nil || workAttemptCompletedAfter(*attempt, *latest) {
				latest = attempt
			}
		}
		var lastCompletedAt *time.Time
		if latest != nil {
			lastCompletedAt = cloneTimePointer(latest.CompletedAt)
		}
		loops = append(loops, telemetry.DispatchLoop{
			ProjectID:             issue.ProjectID,
			IssueID:               issue.ID,
			Identifier:            issue.Identifier,
			IssueURL:              issue.URL,
			Title:                 issue.Title,
			Lane:                  issue.State,
			ConsecutiveDispatches: progress.ConsecutiveNoProgress,
			DispatchLimit:         progress.NoProgressLimit,
			Tripped:               progress.ConsecutiveNoProgress >= progress.NoProgressLimit,
			LastCompletedAt:       lastCompletedAt,
		})
	}
	return loops
}

func snapshotIssueMatches(issue telemetry.Issue, issueID string, identifier string, issueURL string) bool {
	if issue.ID != "" && strings.TrimSpace(issueID) == issue.ID {
		return true
	}
	if issue.Identifier != "" && strings.TrimSpace(identifier) == issue.Identifier {
		return true
	}
	return issue.URL != "" && strings.TrimSpace(issueURL) == issue.URL
}

func budgetRefusalSnapshots(refusals map[string]BudgetRefusal) []telemetry.BudgetRefusal {
	if len(refusals) == 0 {
		return nil
	}
	ids := sortedKeys(refusals)
	out := make([]telemetry.BudgetRefusal, 0, len(ids))
	for _, id := range ids {
		entry := refusals[id]
		refusal := telemetry.BudgetRefusal{
			IssueID:          entry.Issue.ID,
			Identifier:       entry.Issue.Identifier,
			Code:             entry.Code,
			Message:          entry.Message,
			CurrentSpendUSD:  entry.CurrentSpendUSD,
			ProjectedCostUSD: entry.ProjectedCostUSD,
			RefusedAt:        entry.RefusedAt,
			HardHold:         entry.Code == string(budget.ReasonPerIssueMaxUSD),
		}
		if entry.MaxUSD != nil {
			maxUSD := *entry.MaxUSD
			refusal.MaxUSD = &maxUSD
		}
		if entry.ResetAt != nil {
			resetAt := *entry.ResetAt
			refusal.ResetAt = &resetAt
		}
		out = append(out, refusal)
	}
	return out
}

func applyClaimSnapshot(issue *telemetry.Issue, claim Claimed, now time.Time) {
	if issue == nil || claim.Owner == "" {
		return
	}
	issue.Owner = claim.Owner
	issue.LeaseRenewedAt = timePointer(claim.LeaseRenewedAt)
	issue.LeaseExpiresAt = timePointer(claim.LeaseExpiresAt)
	issue.LeaseStale = !claim.LeaseExpiresAt.IsZero() && !now.Before(claim.LeaseExpiresAt)
}

func applyMergeTimingSnapshot(snapshot *telemetry.Issue, issue connector.Issue, timing MergeTiming, now time.Time) {
	if snapshot == nil {
		return
	}
	value, ok := telemetryMergeTiming(issue, timing, now)
	if !ok {
		return
	}
	snapshot.MergeTiming = &value
}

func telemetryMergeTiming(issue connector.Issue, timing MergeTiming, now time.Time) (telemetry.MergeTiming, bool) {
	if timing == (MergeTiming{}) && !mergeWorkerIssue(issue) {
		return telemetry.MergeTiming{}, false
	}
	if timing.EnteredMergingAt.IsZero() {
		timing.EnteredMergingAt = mergeQueueEnteredAt(issue, now)
	}
	terminalAt := firstNonZeroTime(timing.MergedAt, timing.MergeFailedAt)
	if terminalAt.IsZero() {
		terminalAt = now
	}
	timing = timing.withDurations(terminalAt)
	out := telemetry.MergeTiming{
		EnteredMergingAt:           timePointer(timing.EnteredMergingAt),
		MergeWorkerSlotAcquiredAt:  timePointer(timing.MergeWorkerSlotAcquiredAt),
		MergeStartedAt:             timePointer(timing.MergeStartedAt),
		BaseRefreshStartedAt:       timePointer(timing.BaseRefreshStartedAt),
		BaseRefreshFinishedAt:      timePointer(timing.BaseRefreshFinishedAt),
		CIWaitStartedAt:            timePointer(timing.CIWaitStartedAt),
		CIWaitFinishedAt:           timePointer(timing.CIWaitFinishedAt),
		MergedAt:                   timePointer(timing.MergedAt),
		MergeFailedAt:              timePointer(timing.MergeFailedAt),
		MergeFailureReason:         timing.MergeFailureReason,
		QueueWaitSeconds:           timing.QueueWaitSeconds,
		ActiveMergeDurationSeconds: timing.ActiveMergeDurationSeconds,
		TotalMergingSeconds:        timing.TotalMergingSeconds,
		Repository:                 pullRequestRepository(issue),
		PullRequestNumber:          pullRequestNumber(issue),
		IssueNumber:                issueNumberFromIdentifier(issue.Identifier),
	}
	if issue.PullRequest != nil {
		out.HeadSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
		out.BaseSHA = strings.TrimSpace(issue.PullRequest.BaseSHA)
	}
	return out, true
}

func telemetryIssue(issue connector.Issue, quietDuration time.Duration, pollInterval time.Duration, now time.Time, laneEntries map[string]time.Time) telemetry.Issue {
	laneEnteredAt := telemetryIssueLaneEnteredAt(issue, laneEntries)
	return telemetry.Issue{
		ID:                    issue.ID,
		Identifier:            issue.Identifier,
		Number:                issue.Number,
		URL:                   issue.URL,
		Title:                 issue.Title,
		Description:           issue.Description,
		Priority:              cloneIntPointer(issue.Priority),
		PriorityName:          telemetryIssuePriorityName(issue),
		UnblockerCount:        issue.UnblockerCount,
		State:                 issue.State,
		AuthorID:              issue.AuthorID,
		Labels:                append([]string(nil), issue.Labels...),
		Assignees:             append([]string(nil), issue.Assignees...),
		Comments:              telemetryIssueComments(issue.Comments),
		BlockedBy:             telemetryBlockedRefs(issue.BlockedBy),
		PullRequest:           telemetryPullRequest(issue, quietDuration, pollInterval),
		Deliverable:           telemetryDeliverable(issue.Deliverable),
		Metadata:              cloneStringMap(issue.Metadata),
		CreatedAt:             timePointerFromPtr(issue.CreatedAt),
		UpdatedAt:             timePointerFromPtr(issue.UpdatedAt),
		StageUpdatedAt:        timePointerFromPtr(issue.StageUpdatedAt),
		CurrentLaneEnteredAt:  timePointerFromPtr(laneEnteredAt),
		CurrentLaneAgeSeconds: telemetryIssueLaneAgeSeconds(laneEnteredAt, now),
	}
}

func telemetryIssuePriorityName(issue connector.Issue) string {
	if issue.Priority == nil {
		return ""
	}
	return strings.TrimSpace(issue.PriorityName)
}

func telemetryDeliverable(deliverable *connector.Deliverable) *telemetry.Deliverable {
	if deliverable == nil {
		return nil
	}
	return &telemetry.Deliverable{
		Kind:             deliverable.Kind,
		Path:             deliverable.Path,
		ReviewURL:        deliverable.ReviewURL,
		ValidationStatus: deliverable.ValidationStatus,
		ExternalID:       deliverable.ExternalID,
		Metadata:         cloneStringMap(deliverable.Metadata),
	}
}

func telemetryIssueLaneEnteredAt(issue connector.Issue, laneEntries map[string]time.Time) *time.Time {
	if enteredAt := laneEntries[workflowLaneEntryKey(issue)]; !enteredAt.IsZero() {
		return timePointer(enteredAt)
	}
	return timePointer(workflowLaneFallbackAt(issue))
}

func telemetryIssueLaneAgeSeconds(startedAt *time.Time, now time.Time) int64 {
	if startedAt == nil || startedAt.IsZero() || now.IsZero() || now.Before(*startedAt) {
		return 0
	}
	return int64(now.Sub(*startedAt) / time.Second)
}

func telemetryIssueComments(comments []connector.IssueComment) []telemetry.IssueComment {
	out := make([]telemetry.IssueComment, 0, len(comments))
	for _, comment := range comments {
		out = append(out, telemetry.IssueComment{
			ID:                comment.ID,
			Backend:           comment.Backend,
			Body:              comment.Body,
			URL:               comment.URL,
			AuthorLogin:       comment.AuthorLogin,
			AuthorKind:        comment.AuthorKind,
			AuthorDisplayName: comment.AuthorDisplayName,
			CreatedAt:         cloneTime(comment.CreatedAt),
			UpdatedAt:         cloneTime(comment.UpdatedAt),
			Local:             comment.Local,
			CanEdit:           comment.CanEdit,
			CanDelete:         comment.CanDelete,
			TargetType:        comment.TargetType,
		})
	}
	return out
}

func telemetryBlockedRefs(refs []connector.BlockedRef) []telemetry.BlockedRef {
	out := make([]telemetry.BlockedRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, telemetry.BlockedRef{
			HumanOwned:           ref.HumanOwned,
			HumanCompletionReady: ref.HumanCompletionReady,
			ID:                   ref.ID,
			Identifier:           ref.Identifier,
			State:                ref.State,
			TrackerState:         ref.TrackerState,
			Source:               ref.Source,
		})
	}
	return out
}

func telemetryPullRequest(issue connector.Issue, quietDuration time.Duration, pollInterval time.Duration) *telemetry.PullRequest {
	pullRequest := issue.PullRequest
	prNumber := issue.PRNumber
	if pullRequest == nil && prNumber == nil {
		return nil
	}
	if pullRequest == nil {
		pullRequest = &connector.PullRequest{Number: *prNumber}
	}
	out := &telemetry.PullRequest{
		Number:                     pullRequest.Number,
		URL:                        pullRequest.URL,
		BranchName:                 pullRequest.BranchName,
		State:                      pullRequest.State,
		MergeableState:             pullRequest.MergeableState,
		HeadSHA:                    pullRequest.HeadSHA,
		BaseSHA:                    pullRequest.BaseSHA,
		HydrationUnavailableReason: pullRequest.HydrationUnavailableReason,
		HydrationDegradedReason:    pullRequest.HydrationDegradedReason,
		HydrationNextRetryAt:       cloneTime(pullRequest.HydrationNextRetryAt),
		CIStatus:                   pullRequest.CIStatus,
		CheckRunCount:              pullRequest.CheckRunCount,
		StatusContextCount:         pullRequest.StatusContextCount,
		CIQueueSeconds:             pullRequest.CIQueueSeconds,
		CIDurationSeconds:          pullRequest.CIDurationSeconds,
		QuietWaitSeconds:           pullRequestQuietWaitSeconds(issue, quietDuration, pollInterval),
		SlowChecks:                 telemetryPullRequestChecks(pullRequest.SlowChecks),
		RunningChecks:              append([]string(nil), pullRequest.RunningChecks...),
		UnstartedCheckCount:        pullRequest.UnstartedCheckCount,
		UnstartedChecks:            telemetryPullRequestChecks(pullRequest.UnstartedChecks),
		RequiredCheckFailures:      telemetryPullRequestChecks(pullRequest.RequiredCheckFailures),
		CodexReviewState:           pullRequest.CodexReviewState,
		CodexReviewSource:          pullRequest.CodexReviewSource,
	}
	if pullRequest.MergeQueueEntry != nil {
		out.MergeQueueEntry = &telemetry.PullRequestMergeQueueEntry{
			ID:                          pullRequest.MergeQueueEntry.ID,
			State:                       pullRequest.MergeQueueEntry.State,
			Position:                    pullRequest.MergeQueueEntry.Position,
			Depth:                       pullRequest.MergeQueueEntry.Depth,
			EstimatedTimeToMergeSeconds: pullRequest.MergeQueueEntry.EstimatedTimeToMergeSeconds,
			EnqueuedAt:                  cloneTime(pullRequest.MergeQueueEntry.EnqueuedAt),
			URL:                         pullRequest.MergeQueueEntry.URL,
		}
	}
	return out
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func telemetryPullRequestChecks(checks []connector.PullRequestCheck) []telemetry.PullRequestCheck {
	out := make([]telemetry.PullRequestCheck, 0, len(checks))
	for _, check := range checks {
		out = append(out, telemetry.PullRequestCheck{
			Name:            check.Name,
			Status:          check.Status,
			Conclusion:      check.Conclusion,
			QueueSeconds:    check.QueueSeconds,
			DurationSeconds: check.DurationSeconds,
		})
	}
	return out
}

func pullRequestQuietWaitSeconds(issue connector.Issue, quietDuration time.Duration, pollInterval time.Duration) int64 {
	if issue.PullRequest == nil || issue.StageUpdatedAt == nil || issue.StageUpdatedAt.IsZero() {
		return 0
	}
	switch normalizeState(issue.State) {
	case "merging", "done", "cancelled", "canceled", "closed":
	default:
		return 0
	}
	stageAt := *issue.StageUpdatedAt
	var latest *time.Time
	latest = latestBefore(latest, issue.PullRequest.ActivityAt, stageAt)
	latest = latestBefore(latest, issue.PullRequest.CodexReviewSubmittedAt, stageAt)
	latest = latestBefore(latest, issue.UpdatedAt, stageAt)
	if latest == nil || stageAt.Before(*latest) {
		return 0
	}
	wait := stageAt.Sub(*latest)
	if quietDuration > 0 {
		maxWait := quietDuration
		if pollInterval > 0 {
			maxWait += pollInterval
		}
		if wait > maxWait {
			return 0
		}
	}
	return int64(wait / time.Second)
}

func latestBefore(current *time.Time, candidate *time.Time, before time.Time) *time.Time {
	if candidate == nil || candidate.IsZero() || candidate.After(before) {
		return current
	}
	if current == nil || candidate.After(*current) {
		value := *candidate
		return &value
	}
	return current
}

func tokensFromTokenTotals(totals TokenTotals) telemetry.Tokens {
	var last *telemetry.TokenBreakdown
	if totals.Last != nil {
		last = &telemetry.TokenBreakdown{
			Input:           totals.Last.InputTokens,
			CachedInput:     totals.Last.CachedInputTokens,
			Output:          totals.Last.OutputTokens,
			ReasoningOutput: totals.Last.ReasoningOutputTokens,
			Total:           totals.Last.TotalTokens,
		}
	}
	return telemetry.Tokens{
		Input:              totals.InputTokens,
		CachedInput:        totals.CachedInputTokens,
		Output:             totals.OutputTokens,
		ReasoningOutput:    totals.ReasoningOutputTokens,
		Total:              totals.TotalTokens,
		Last:               last,
		ModelContextWindow: totals.ModelContextWindow,
		RuntimeSeconds:     totals.RuntimeSeconds,
	}
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func timePointerFromPtr(value *time.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	cloned := *value
	return &cloned
}

func (s State) liveTokenTotals() TokenTotals {
	totals := s.TokenTotals
	for _, running := range s.Running {
		totals = addTokenTotals(totals, running.Tokens)
	}
	return totals
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
