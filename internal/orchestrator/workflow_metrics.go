package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/provenance"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workflowmetrics"
)

const defaultWorkflowMetricsProjectID = "default"

const (
	workflowActionBlockedRecovery          = "blocked_recovery"
	workflowActionBlockedRecoveryExhausted = "blocked_recovery_exhausted"
	workflowActionPlanReviewRework         = "plan_review_rework"
	workflowActionReworkBreakerAutoUnpark  = "rework_breaker_auto_unpark"
	blockedCauseStatusUnrecorded           = "unrecorded"
)

type WorkflowMetricsRecorder interface {
	RecordWorkflowPhaseEvent(context.Context, store.WorkflowPhaseEvent) (int64, error)
}

type WorkflowMetricsTimelineReader interface {
	IssueWorkflowTimeline(context.Context, store.IssueIdentity) (store.WorkflowTimeline, error)
}

type WorkflowMetricsMetadataUpdater interface {
	UpdateWorkflowPhaseEventMetadata(context.Context, int64, string) error
}

type workflowLaneMetadata struct {
	LessonEvidence        reworkLessonEvidence                       `json:"-"`
	Reconciliation        string                                     `json:"reconciliation,omitempty"`
	PullRequest           *workflowLanePullRequestMetadata           `json:"pull_request,omitempty"`
	DependencyAutoUnblock *workflowLaneDependencyAutoUnblockMetadata `json:"dependency_auto_unblock,omitempty"`
	ReworkBreaker         *workflowLaneReworkBreakerMetadata         `json:"rework_breaker,omitempty"`
	BlockedRecovery       *workflowLaneBlockedRecoveryMetadata       `json:"blocked_recovery,omitempty"`
	TrackerMutationAt     string                                     `json:"tracker_mutation_at,omitempty"`
	BlockedCauseStatus    string                                     `json:"blocked_cause_status,omitempty"`
	ActionSignatures      []workflowLaneActionSignatureMetadata      `json:"action_signatures,omitempty"`
	Provenance            provenance.Attribution                     `json:"provenance"`
	Admission             *provenance.Admission                      `json:"admission,omitempty"`
}

type workflowLanePullRequestMetadata struct {
	Repository           string    `json:"repository,omitempty"`
	AssociationSource    string    `json:"association_source,omitempty"`
	AssociationCheckedAt time.Time `json:"association_checked_at,omitzero"`
	Number               int64     `json:"number,omitempty"`
	HeadSHA              string    `json:"head_sha,omitempty"`
	FailedChecks         []string  `json:"failed_checks,omitempty"`
}

type workflowLaneDependencyAutoUnblockMetadata struct {
	BlockerSet string   `json:"blocker_set,omitempty"`
	Blockers   []string `json:"blockers,omitempty"`
}

type workflowLaneReworkBreakerMetadata struct {
	Reason string `json:"reason,omitempty"`
}

type workflowLaneBlockedRecoveryMetadata struct {
	Owner                   string `json:"owner,omitempty"`
	Cause                   string `json:"cause,omitempty"`
	Predicate               string `json:"predicate,omitempty"`
	CauseFingerprint        string `json:"cause_fingerprint,omitempty"`
	CauseFingerprintVersion int    `json:"cause_fingerprint_version,omitempty"`
	TargetState             string `json:"target_state,omitempty"`
	RunMode                 string `json:"run_mode,omitempty"`
	IntentResumable         bool   `json:"intent_resumable,omitempty"`
	Resumable               bool   `json:"resumable,omitempty"`
	Reachability            string `json:"reachability,omitempty"`
	HoldReason              string `json:"hold_reason,omitempty"`
	OperatorRemedy          string `json:"operator_remedy,omitempty"`
	ResourceKind            string `json:"resource_kind,omitempty"`
	ResourceConsumer        string `json:"resource_consumer,omitempty"`
	ResourceCredential      string `json:"resource_credential,omitempty"`
	ResourceRemaining       int64  `json:"resource_remaining,omitempty"`
	ResourceLimit           int64  `json:"resource_limit,omitempty"`
	ResourceReserve         int64  `json:"resource_reserve,omitempty"`
	ResourceResetAt         string `json:"resource_reset_at,omitempty"`
	ResourceObservedAt      string `json:"resource_observed_at,omitempty"`
	ResumeAt                string `json:"resume_at,omitempty"`
	LifetimeSessions        int64  `json:"lifetime_sessions,omitempty"`
	LifetimeTokens          int64  `json:"lifetime_tokens,omitempty"`
	LifetimeSessionLimit    int64  `json:"lifetime_session_limit,omitempty"`
	LifetimeTokenLimit      int64  `json:"lifetime_token_limit,omitempty"`
	WorkAttemptID           int64  `json:"work_attempt_id,omitempty"`
	AttemptNumber           int    `json:"attempt_number,omitempty"`
	AttemptError            string `json:"attempt_error,omitempty"`
}

func (m workflowLaneBlockedRecoveryMetadata) intentResumable() bool {
	return m.IntentResumable || m.Resumable
}

type workflowLaneActionSignatureMetadata struct {
	Action    string `json:"action,omitempty"`
	Signature string `json:"signature,omitempty"`
}

type issueStateSnapshotTransitions struct {
	boardIssues []connector.Issue
	pipeline    []connector.Issue
}

func (o *Orchestrator) updateIssueState(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	dispositions ...laneMutationDisposition,
) error {
	return o.updateIssueStateByID(ctx, state, issue.ID, issue, targetState, at, reason, dispositions...)
}

func (o *Orchestrator) updateIssueStateByID(
	ctx context.Context,
	state *State,
	issueID string,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	dispositions ...laneMutationDisposition,
) error {
	return o.updateIssueStateByIDWithMetadata(ctx, state, issueID, issue, targetState, at, reason, workflowLaneMetadata{}, dispositions...)
}

func (o *Orchestrator) updateIssueStateByIDWithMetadata(
	ctx context.Context,
	state *State,
	issueID string,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	metadata workflowLaneMetadata,
	dispositions ...laneMutationDisposition,
) error {
	return o.updateIssueStateByIDWithMetadataMode(ctx, state, issueID, issue, targetState, at, reason, metadata, false, dispositions...)
}

func (o *Orchestrator) updateIssueStateByIDStrictWithMetadata(
	ctx context.Context,
	state *State,
	issueID string,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	metadata workflowLaneMetadata,
	dispositions ...laneMutationDisposition,
) error {
	return o.updateIssueStateByIDWithMetadataMode(ctx, state, issueID, issue, targetState, at, reason, metadata, true, dispositions...)
}

func (o *Orchestrator) updateIssueStateByIDWithMetadataMode(
	ctx context.Context,
	state *State,
	issueID string,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	metadata workflowLaneMetadata,
	strict bool,
	dispositions ...laneMutationDisposition,
) error {
	receipt, running, leased, err := o.prepareLaneMutation(ctx, state, issueID, issue, targetState, at, reason, dispositions)
	if err != nil {
		return err
	}
	if err := o.connector.UpdateIssueState(ctx, issueID, targetState); err != nil {
		if errors.Is(err, connector.ErrStateUpdateBlocked) && !strict {
			if receiptErr := o.resolveLaneMutation(ctx, receipt, store.LaneMutationTrackerBlocked, at, err); receiptErr != nil {
				return receiptErr
			}
			if o.logger != nil {
				o.logger.Debug("skip blocked issue state update", "issue_id", issueID, "target_state", targetState, "error", err)
			}
			return nil
		}
		return errors.Join(err, o.resolveLaneMutation(ctx, receipt, store.LaneMutationTrackerFailed, at, err))
	}
	transitionAt := at
	if normalizeState(issue.State) != normalizeState(targetState) {
		mutationAt, ok := o.confirmTrackerStateTransition(ctx, issueID, issue, targetState, metadata.BlockedRecovery != nil)
		if ok {
			metadata.TrackerMutationAt = mutationAt.Format(time.RFC3339Nano)
			transitionAt = mutationAt
		}
	}
	receiptErr := o.resolveLaneMutation(ctx, receipt, store.LaneMutationTrackerApplied, transitionAt, nil)
	if stateIn(targetState, o.cfg.TerminalStates) {
		terminalIssue := cloneIssue(issue)
		if strings.TrimSpace(terminalIssue.ID) == "" {
			terminalIssue.ID = issueID
		}
		terminalIssue.State = targetState
		closed, err := o.closeLandedTerminalIssue(ctx, terminalIssue)
		if err != nil {
			return fmt.Errorf("close terminal issue %s: %w", issueID, err)
		}
		if closed {
			issue.Closed = true
			issue.ClosedReason = "completed"
		}
		o.clearMergeRequiredCheckStreaks(ctx, terminalIssue)
	}
	updateIssueStateSnapshots(state, issueID, issue, targetState, transitionAt)
	recordIssueStateMutationProvenance(state, issueID, issue, targetState, transitionAt, reason, metadata)
	if strings.TrimSpace(issue.ID) == "" {
		issue.ID = issueID
	}
	o.recordLaneTransition(ctx, issue, targetState, at, reason, metadata)
	if normalizeState(targetState) == normalizeState(autoPromoteReworkState) && normalizeState(issue.State) != normalizeState(targetState) {
		o.captureReworkLesson(issue, at, reason, metadata.LessonEvidence)
	}
	if leased {
		o.applyLaneMutationDisposition(ctx, state, running, receipt, issue, transitionAt)
	}
	return receiptErr
}

func (o *Orchestrator) confirmTrackerStateTransition(
	ctx context.Context,
	issueID string,
	issue connector.Issue,
	targetState string,
	allowStateFetch bool,
) (time.Time, bool) {
	transitioned := cloneIssue(issue)
	transitioned.ID = strings.TrimSpace(firstNonBlank(transitioned.ID, issueID))
	transitioned.State = strings.TrimSpace(targetState)
	transitioned.StageUpdatedAt = nil
	transitioned.StageUpdatedActor = connector.IssueActor{}
	if reader, ok := o.connector.(connector.IssueStateTransitionReader); ok && reader != nil {
		allowStateFetch = true
		transition, found, err := reader.IssueStateTransition(ctx, transitioned)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("tracker state transition confirmation failed", "issue_id", issueID, "target_state", targetState, "error", err)
			}
		} else if found && !transition.EnteredAt.IsZero() {
			return transition.EnteredAt.UTC(), true
		}
	}
	if !allowStateFetch {
		return time.Time{}, false
	}

	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{issueID})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("tracker state confirmation failed", "issue_id", issueID, "target_state", targetState, "error", err)
		}
		return time.Time{}, false
	}
	for _, current := range issues {
		if !sameIssueIdentity(transitioned, current) || normalizeState(current.State) != normalizeState(targetState) ||
			current.StageUpdatedAt == nil || current.StageUpdatedAt.IsZero() {
			continue
		}
		return current.StageUpdatedAt.UTC(), true
	}
	return time.Time{}, false
}

func updateIssueStateSnapshots(state *State, issueID string, issue connector.Issue, targetState string, at time.Time) {
	if state == nil {
		return
	}
	issueID = strings.TrimSpace(issueID)
	targetState = strings.TrimSpace(targetState)
	if issueID == "" || targetState == "" {
		return
	}

	transitioned := cloneIssue(issue)
	if strings.TrimSpace(transitioned.ID) == "" {
		transitioned.ID = issueID
	}
	applyIssueStateSnapshot(&transitioned, targetState, at)

	update := func(issues []connector.Issue) (connector.Issue, bool) {
		for index := range issues {
			if strings.TrimSpace(issues[index].ID) != issueID {
				continue
			}
			applyIssueStateSnapshot(&issues[index], targetState, at)
			return cloneIssue(issues[index]), true
		}
		return connector.Issue{}, false
	}
	boardTransition := transitioned
	if updated, ok := update(state.BoardIssues); ok {
		boardTransition = updated
	}
	if state.tickTransitions != nil {
		state.tickTransitions.boardIssues = upsertIssueStateSnapshot(
			state.tickTransitions.boardIssues,
			boardTransition,
		)
	}
	if updated, ok := update(state.Pipeline); ok && state.tickTransitions != nil {
		state.tickTransitions.pipeline = upsertIssueStateSnapshot(
			state.tickTransitions.pipeline,
			updated,
		)
	}
	if completed, ok := state.Completed[issueID]; ok {
		completed.Issue = mergeIssueTrackerFields(completed.Issue, transitioned)
		state.Completed[issueID] = completed
	}
}

func applyIssueStateSnapshot(issue *connector.Issue, targetState string, at time.Time) {
	if issue == nil {
		return
	}
	stateChanged := normalizeState(issue.State) != normalizeState(targetState)
	issue.State = targetState
	if stateChanged && !at.IsZero() {
		stageUpdatedAt := at.UTC()
		issue.StageUpdatedAt = &stageUpdatedAt
	}
}

func upsertIssueStateSnapshot(issues []connector.Issue, issue connector.Issue) []connector.Issue {
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return issues
	}
	for index := range issues {
		if strings.TrimSpace(issues[index].ID) == issueID {
			issues[index] = cloneIssue(issue)
			return issues
		}
	}
	return append(issues, cloneIssue(issue))
}

func overlayIssueStateSnapshots(issues []connector.Issue, transitions []connector.Issue) []connector.Issue {
	out := cloneIssues(issues)
	for _, transition := range transitions {
		issueID := strings.TrimSpace(transition.ID)
		if issueID == "" {
			continue
		}
		found := false
		for index := range out {
			if strings.TrimSpace(out[index].ID) != issueID {
				continue
			}
			out[index].State = transition.State
			out[index].StageUpdatedAt = timePointerFromPtr(transition.StageUpdatedAt)
			found = true
			break
		}
		if !found {
			out = append(out, cloneIssue(transition))
		}
	}
	return out
}

func (o *Orchestrator) recordLaneTransition(
	ctx context.Context,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	metadata workflowLaneMetadata,
) {
	if o.workflowMetrics == nil {
		return
	}

	sourceState := strings.TrimSpace(issue.State)
	targetState = strings.TrimSpace(targetState)
	if targetState == "" || normalizeState(sourceState) == normalizeState(targetState) {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "state_transition"
	}
	metadata.Provenance = workflowLaneMutationAttribution(reason, metadata)
	if err := workflowmetrics.RecordLaneTransition(ctx, o.workflowMetrics, workflowmetrics.LaneTransition{
		ProjectID:    o.workflowMetricsProjectID(),
		Issue:        issue,
		TargetState:  targetState,
		At:           at,
		Reason:       reason,
		MetadataJSON: workflowLaneMetadataJSON(issue, metadata),
	}); err != nil && o.logger != nil {
		o.logger.Warn("record lane transition metric failed", "issue_id", issue.ID, "identifier", issue.Identifier, "from_state", sourceState, "target_state", targetState, "error", err)
	}
}

func recordIssueStateMutationProvenance(
	state *State,
	issueID string,
	issue connector.Issue,
	targetState string,
	at time.Time,
	reason string,
	metadata workflowLaneMetadata,
) {
	if state == nil || normalizeState(issue.State) == normalizeState(targetState) {
		return
	}
	transitioned := cloneIssue(issue)
	if strings.TrimSpace(transitioned.ID) == "" {
		transitioned.ID = strings.TrimSpace(issueID)
	}
	transitioned.State = strings.TrimSpace(targetState)
	laneKey := workflowLaneEntryKey(transitioned)
	if laneKey == "" {
		return
	}
	if state.laneProvenance == nil {
		state.laneProvenance = map[string]provenance.Attribution{}
	}
	state.laneProvenance[laneKey] = workflowLaneMutationAttribution(reason, metadata)
	if !at.IsZero() {
		if state.laneEntries == nil {
			state.laneEntries = map[string]time.Time{}
		}
		state.laneEntries[laneKey] = at.UTC()
	}
}

func workflowLaneMutationAttribution(reason string, metadata workflowLaneMetadata) provenance.Attribution {
	attribution := metadata.Provenance
	if attribution.Origin == "" {
		attribution = provenance.AttributionFromSource(provenance.SourceDetentInstance, provenance.Actor{})
		attribution.Origin = workflowOriginForReason(reason)
	}
	return provenance.Prepare(attribution)
}

func (o *Orchestrator) recordWorkflowReviewAction(
	ctx context.Context,
	issue connector.Issue,
	phaseName string,
	reason string,
	at time.Time,
	metadata workflowLaneMetadata,
) {
	recorder := o.workflowMetrics
	if recorder == nil {
		return
	}
	phaseName = strings.TrimSpace(phaseName)
	if phaseName == "" {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = phaseName
	}
	if _, err := recorder.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:      o.workflowMetricsProjectID(),
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		IssueURL:       issue.URL,
		PRNumber:       workflowMetricsPRNumber(issue),
		PhaseType:      store.WorkflowPhaseTypeReview,
		PhaseName:      phaseName,
		Reason:         reason,
		Status:         "completed",
		StartedAt:      at,
		FinishedAt:     at,
		MetadataJSON:   workflowLaneMetadataJSON(issue, metadata),
		EndpointFamily: "tracker",
	}); err != nil && o.logger != nil {
		o.logger.Warn("record workflow review action metric failed", "issue_id", issue.ID, "identifier", issue.Identifier, "phase_name", phaseName, "reason", reason, "error", err)
	}
}

func (o *Orchestrator) workflowMetricsProjectID() string {
	projectID := strings.TrimSpace(o.cfg.Project.ID)
	if projectID == "" {
		return defaultWorkflowMetricsProjectID
	}
	return projectID
}

func (o *Orchestrator) refreshCurrentLaneEntries(ctx context.Context, state *State, observedAt time.Time) {
	if state == nil {
		return
	}

	type timelineResult struct {
		timeline store.WorkflowTimeline
	}

	previous := state.laneEntries
	next := make(map[string]time.Time)
	nextProvenance := make(map[string]provenance.Attribution)
	timelines := make(map[string]timelineResult)
	for _, issue := range stateLaneEntryIssues(state) {
		laneKey := workflowLaneEntryKey(issue)
		if laneKey == "" {
			continue
		}
		if _, exists := next[laneKey]; exists {
			continue
		}

		identityKey := workflowIssueIdentityKey(issue)
		result, exists := timelines[identityKey]
		if !exists {
			result.timeline, _ = o.issueWorkflowTimeline(ctx, issue)
			timelines[identityKey] = result
		}

		latestEvent, eventBacked := latestCurrentLaneEntry(result.timeline.Events, issue.State)
		trackerTransition := connector.IssueStateTransition{}
		if issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() {
			trackerTransition = connector.IssueStateTransition{
				EnteredAt: issue.StageUpdatedAt.UTC(),
				Actor:     issue.StageUpdatedActor,
			}
		} else if !eventBacked {
			trackerTransition = o.trackerIssueStateTransition(ctx, issue)
		}
		observedAttribution := observedLaneAttribution(state, issue, trackerTransition.Actor)
		enteredAt := resolveCurrentLaneEnteredAt(issue, previous[laneKey], trackerTransition.EnteredAt, observedAt, result.timeline.Events)
		if !enteredAt.IsZero() {
			next[laneKey] = enteredAt
		}
		if !eventBacked || trackerTransition.EnteredAt.After(workflowLaneTransitionAt(latestEvent)) {
			o.recordObservedLaneEntry(ctx, issue, enteredAt, observedAttribution)
			if normalizeState(issue.State) == normalizeState(autoPromoteReworkState) {
				observed := cloneIssue(issue)
				observed.State = ""
				o.captureReworkLesson(observed, enteredAt, "tracker_state_observed")
			}
			latestEvent, eventBacked = latestCurrentLaneEntryForAt(result.timeline.Events, issue.State, enteredAt)
		}
		if eventBacked {
			if metadata, ok := provenance.Parse(latestEvent.MetadataJSON); ok {
				nextProvenance[laneKey] = metadata.Provenance
			} else {
				nextProvenance[laneKey] = provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{})
			}
		} else {
			nextProvenance[laneKey] = observedAttribution
		}
	}
	state.laneEntries = next
	state.laneProvenance = nextProvenance
}

func (o *Orchestrator) trackerIssueStateTransition(ctx context.Context, issue connector.Issue) connector.IssueStateTransition {
	if issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() {
		return connector.IssueStateTransition{EnteredAt: issue.StageUpdatedAt.UTC(), Actor: issue.StageUpdatedActor}
	}
	reader, ok := o.connector.(connector.IssueStateTransitionReader)
	if ok && reader != nil {
		transition, found, err := reader.IssueStateTransition(ctx, issue)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("tracker lane transition read failed", "issue_id", issue.ID, "identifier", issue.Identifier, "state", issue.State, "error", err)
			}
		} else if found {
			transition.EnteredAt = transition.EnteredAt.UTC()
			return transition
		}
	}
	return connector.IssueStateTransition{}
}

func (o *Orchestrator) recordObservedLaneEntry(ctx context.Context, issue connector.Issue, enteredAt time.Time, attribution provenance.Attribution) {
	if o.workflowMetrics == nil || enteredAt.IsZero() || strings.TrimSpace(issue.State) == "" {
		return
	}
	metadata := workflowLaneMetadata{
		Provenance: attribution,
	}
	if normalizeState(issue.State) == normalizeState(blockedStatusState) &&
		provenance.NormalizeOrigin(attribution.Origin) == provenance.OriginIndeterminate &&
		firstNonBlank(strings.TrimSpace(issue.BlockerReason), workpadParkCause(issue)) == "" {
		metadata.BlockedCauseStatus = blockedCauseStatusUnrecorded
	}
	if strings.EqualFold(strings.TrimSpace(issue.State), strings.TrimSpace(o.cfg.AdmissionTargetState)) &&
		strings.TrimSpace(o.cfg.AdmissionTargetState) != "" {
		metadata.Admission = &provenance.Admission{Attributed: false}
	}
	if _, err := o.workflowMetrics.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:      o.workflowMetricsProjectID(),
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		IssueURL:       issue.URL,
		PRNumber:       workflowMetricsPRNumber(issue),
		PhaseType:      store.WorkflowPhaseTypeLane,
		PhaseName:      issue.State,
		Reason:         "tracker_state_observed",
		Status:         "entered",
		StartedAt:      enteredAt,
		MetadataJSON:   workflowLaneMetadataJSON(issue, metadata),
		EndpointFamily: "tracker",
	}); err != nil && o.logger != nil {
		o.logger.Warn("record observed lane enter metric failed", "issue_id", issue.ID, "identifier", issue.Identifier, "state", issue.State, "error", err)
	}
}

func stateLaneEntryIssues(state *State) []connector.Issue {
	issues := make([]connector.Issue, 0, len(state.BoardIssues)+len(state.Pipeline)+len(state.Running)+len(state.Retry)+len(state.Blocked)+len(state.Completed))
	issues = append(issues, state.BoardIssues...)
	issues = append(issues, state.Pipeline...)
	for _, id := range sortedKeys(state.Running) {
		issues = append(issues, state.Running[id].Issue)
	}
	for _, id := range sortedKeys(state.Retry) {
		issues = append(issues, state.Retry[id].Issue)
	}
	for _, id := range sortedKeys(state.Blocked) {
		issues = append(issues, state.Blocked[id].Issue)
	}
	for _, id := range sortedKeys(state.Completed) {
		issues = append(issues, state.Completed[id].Issue)
	}
	issues = append(issues, state.StatusDrift.UntrackedOpen...)
	issues = append(issues, state.StatusDrift.OpenTerminal...)
	issues = append(issues, state.StatusDrift.ClosedActive...)
	return issues
}

func resolveCurrentLaneEnteredAt(issue connector.Issue, previous time.Time, trackerEnteredAt time.Time, observedAt time.Time, events []store.WorkflowPhaseEvent) time.Time {
	enteredAt, eventBacked := latestCurrentLaneEnteredAt(events, issue.State)
	mayMoveForward := eventBacked && laneOccupancyChangedSince(events, issue.State, previous)
	if !trackerEnteredAt.IsZero() && (enteredAt.IsZero() || trackerEnteredAt.After(enteredAt)) {
		enteredAt = trackerEnteredAt
		mayMoveForward = true
	}
	if enteredAt.IsZero() {
		enteredAt = workflowLaneWeakFallbackAt(issue)
	}
	if enteredAt.IsZero() {
		enteredAt = observedAt
	}
	if !previous.IsZero() && (enteredAt.IsZero() || (enteredAt.After(previous) && !mayMoveForward)) {
		return previous
	}
	return enteredAt
}

func laneOccupancyChangedSince(events []store.WorkflowPhaseEvent, state string, previous time.Time) bool {
	if previous.IsZero() {
		return true
	}
	state = normalizeState(state)
	for _, event := range events {
		if event.PhaseType != store.WorkflowPhaseTypeLane {
			continue
		}
		eventAt := event.StartedAt
		if event.FinishedAt.After(eventAt) {
			eventAt = event.FinishedAt
		}
		if eventAt.IsZero() || !eventAt.After(previous) {
			continue
		}
		if normalizeState(event.PhaseName) != state || strings.EqualFold(strings.TrimSpace(event.Status), "exited") {
			return true
		}
	}
	return false
}

func latestCurrentLaneEnteredAt(events []store.WorkflowPhaseEvent, state string) (time.Time, bool) {
	event, ok := latestCurrentLaneEntry(events, state)
	return event.StartedAt, ok
}

func latestCurrentLaneEntry(events []store.WorkflowPhaseEvent, state string) (store.WorkflowPhaseEvent, bool) {
	state = normalizeState(state)
	if state == "" {
		return store.WorkflowPhaseEvent{}, false
	}

	var latest store.WorkflowPhaseEvent
	for _, event := range events {
		if event.PhaseType != store.WorkflowPhaseTypeLane ||
			normalizeState(event.PhaseName) != state ||
			!strings.EqualFold(strings.TrimSpace(event.Status), "entered") ||
			event.StartedAt.IsZero() {
			continue
		}
		if latest.StartedAt.IsZero() || event.StartedAt.After(latest.StartedAt) ||
			(event.StartedAt.Equal(latest.StartedAt) && event.ID > latest.ID) {
			latest = event
		}
	}
	if latest.StartedAt.IsZero() {
		return store.WorkflowPhaseEvent{}, false
	}
	return latest, true
}

func latestCurrentLaneEntryForAt(events []store.WorkflowPhaseEvent, state string, enteredAt time.Time) (store.WorkflowPhaseEvent, bool) {
	event, ok := latestCurrentLaneEntry(events, state)
	if !ok || !event.StartedAt.Equal(enteredAt) {
		return store.WorkflowPhaseEvent{}, false
	}
	return event, true
}

func workflowLaneFallbackAt(issue connector.Issue) time.Time {
	for _, candidate := range []*time.Time{issue.StageUpdatedAt, issue.UpdatedAt, issue.CreatedAt} {
		if candidate != nil && !candidate.IsZero() {
			return *candidate
		}
	}
	return time.Time{}
}

func workflowLaneWeakFallbackAt(issue connector.Issue) time.Time {
	for _, candidate := range []*time.Time{issue.UpdatedAt, issue.CreatedAt} {
		if candidate != nil && !candidate.IsZero() {
			return *candidate
		}
	}
	return time.Time{}
}

func workflowLaneEntryKey(issue connector.Issue) string {
	identity := workflowIssueIdentityKey(issue)
	lane := normalizeState(issue.State)
	if identity == "" || lane == "" {
		return ""
	}
	return identity + "\x00" + lane
}

func workflowIssueIdentityKey(issue connector.Issue) string {
	if value := strings.TrimSpace(issue.ID); value != "" {
		return "id:" + value
	}
	if value := strings.TrimSpace(issue.Identifier); value != "" {
		return "identifier:" + value
	}
	if value := strings.TrimSpace(issue.URL); value != "" {
		return "url:" + value
	}
	return ""
}

func workflowMetricsPRNumber(issue connector.Issue) *int64 {
	switch {
	case issue.PRNumber != nil:
		value := int64(*issue.PRNumber)
		return &value
	case issue.PullRequest != nil && issue.PullRequest.Number > 0:
		value := int64(issue.PullRequest.Number)
		return &value
	default:
		return nil
	}
}

func workflowLaneMetadataJSON(issue connector.Issue, metadata workflowLaneMetadata) string {
	if metadata.PullRequest == nil {
		metadata.PullRequest = workflowLanePullRequestMetadataFromIssue(issue)
	}
	if metadata.Provenance.Origin == "" {
		metadata.Provenance = provenance.AttributionFromSource(provenance.SourceTrackerObservation, provenance.Actor{})
	} else {
		metadata.Provenance = provenance.Prepare(metadata.Provenance)
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func workflowOriginForReason(reason string) provenance.Origin {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "dependency_auto_unblock", "blocker_auto_promote":
		return provenance.OriginDependency
	default:
		return provenance.OriginDetent
	}
}

func observedLaneAttribution(state *State, issue connector.Issue, transitionActor connector.IssueActor) provenance.Attribution {
	actor := provenance.Actor{
		Login: transitionActor.Login,
		Kind:  transitionActor.Kind,
	}
	attribution := provenance.AttributionFromSource(provenance.SourceTrackerObservation, actor)
	if state != nil {
		if running, ok := state.Running[strings.TrimSpace(issue.ID)]; ok {
			if provenance.NormalizeOrigin(attribution.Origin) == provenance.OriginAutomation {
				if sameTrackerActor(transitionActor, running.WorkerGitHubActor) {
					return provenance.AttributionFromSource(provenance.SourceDetentAgentSession, actor)
				}
			}
		}
	}
	return attribution
}

func sameTrackerActor(left connector.IssueActor, right connector.IssueActor) bool {
	leftLogin := strings.TrimSpace(left.Login)
	rightLogin := strings.TrimSpace(right.Login)
	return leftLogin != "" && rightLogin != "" && strings.EqualFold(leftLogin, rightLogin)
}

func workflowLaneMetadataFromJSON(raw string) (workflowLaneMetadata, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return workflowLaneMetadata{}, false
	}
	var metadata workflowLaneMetadata
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return workflowLaneMetadata{}, false
	}
	return metadata, true
}

func workflowLaneTransitionAt(event store.WorkflowPhaseEvent) time.Time {
	at := event.StartedAt.UTC()
	metadata, ok := workflowLaneMetadataFromJSON(event.MetadataJSON)
	if !ok || strings.TrimSpace(metadata.TrackerMutationAt) == "" {
		return at
	}
	mutationAt, err := time.Parse(time.RFC3339Nano, metadata.TrackerMutationAt)
	if err != nil || !mutationAt.After(at) {
		return at
	}
	return mutationAt.UTC()
}

func workflowLaneEntryMatchesCurrent(issue connector.Issue, event store.WorkflowPhaseEvent) bool {
	return blockedEntryMatchesCurrent(issue, workflowLaneTransitionAt(event))
}

func BlockedIssueHasCurrentRecoveryPredicate(
	issue connector.Issue,
	phaseName string,
	enteredAt time.Time,
	metadataJSON string,
) bool {
	if normalizeState(issue.State) != normalizeState(blockedStatusState) ||
		normalizeState(phaseName) != normalizeState(blockedStatusState) {
		return false
	}
	metadata, ok := workflowLaneMetadataFromJSON(metadataJSON)
	if !workflowLaneEntryMatchesCurrent(issue, store.WorkflowPhaseEvent{StartedAt: enteredAt, MetadataJSON: metadataJSON}) {
		return false
	}
	return ok &&
		metadata.BlockedRecovery != nil &&
		strings.TrimSpace(metadata.BlockedRecovery.Owner) != "" &&
		strings.TrimSpace(metadata.BlockedRecovery.Predicate) != ""
}

func workflowLaneMetadataWithActionSignature(metadata workflowLaneMetadata, action string, signature string) workflowLaneMetadata {
	action = strings.TrimSpace(action)
	signature = strings.TrimSpace(signature)
	if action == "" || signature == "" {
		return metadata
	}
	if workflowLaneMetadataHasActionSignature(metadata, action, signature) {
		return metadata
	}
	metadata.ActionSignatures = append(metadata.ActionSignatures, workflowLaneActionSignatureMetadata{
		Action:    action,
		Signature: signature,
	})
	return metadata
}

func workflowLaneMetadataHasActionSignature(metadata workflowLaneMetadata, action string, signature string) bool {
	action = strings.TrimSpace(action)
	signature = strings.TrimSpace(signature)
	if action == "" || signature == "" {
		return false
	}
	for _, candidate := range metadata.ActionSignatures {
		if strings.EqualFold(strings.TrimSpace(candidate.Action), action) &&
			strings.TrimSpace(candidate.Signature) == signature {
			return true
		}
	}
	return false
}

func workflowLaneMetadataHasAction(metadata workflowLaneMetadata, action string) bool {
	action = strings.TrimSpace(action)
	if action == "" {
		return false
	}
	for _, candidate := range metadata.ActionSignatures {
		if strings.EqualFold(strings.TrimSpace(candidate.Action), action) {
			return true
		}
	}
	return false
}

func workflowLanePullRequestMetadataFromIssue(issue connector.Issue) *workflowLanePullRequestMetadata {
	var metadata workflowLanePullRequestMetadata
	metadata.Repository = issue.PRRepository
	metadata.AssociationSource = issue.PRSource
	metadata.AssociationCheckedAt = issue.PRVerifiedAt
	if number := workflowMetricsPRNumber(issue); number != nil && *number > 0 {
		metadata.Number = *number
	}
	if issue.PullRequest != nil {
		metadata.HeadSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
		metadata.FailedChecks = autoPromoteCanonicalChecks(autoPromoteFailedChecksFromPullRequest(issue.PullRequest))
	}
	if metadata.Number <= 0 && metadata.HeadSHA == "" && len(metadata.FailedChecks) == 0 {
		return nil
	}
	return &metadata
}
