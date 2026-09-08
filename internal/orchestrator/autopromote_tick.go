package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const (
	autoPromoteSourceState                = "Human Review"
	autoPromoteMergingState               = "Merging"
	autoPromoteReworkState                = "Rework"
	autoPromoteGateWaitSource             = "source"
	autoPromoteGateWaitReview             = "review"
	autoPromoteGateWaitTimeoutMerge       = "merge"
	autoPromoteGateWaitTimeoutHumanReview = "human_review"
	defaultAutoPromoteGateWaitTimeout     = time.Hour
	completedReworkGateWaitReason         = "completed_rework_gate_wait"
	completionGateWaitMetadataKey         = "completion_gate_wait"
	mergeWorkerProjectStateFull           = "project_state_capacity_full"
	mergeBaseRefreshRequiredChecksPending = "required_checks_pending"
	mergeBaseRefreshLaneUnavailable       = "merge_lane_capacity_unavailable"
	mergeBaseRefreshGlobalUnavailable     = "global_capacity_unavailable"
	mergeSelectionReasonStickyAged        = "sticky_aged_head"
	mergeSelectionReasonAged              = "aged_head"
	mergeSelectionReasonClean             = "clean_head"
	mergeSelectionReasonQueue             = "queue_order"
)

type completionGateWaitRecord struct {
	Reason         string               `json:"reason"`
	MergeableState string               `json:"mergeable_state,omitempty"`
	P1Findings     []AutoPromoteFinding `json:"p1_findings,omitempty"`
}

type autoPromoteTickResult struct {
	transitioned       map[string]struct{}
	dispatchCandidates []connector.Issue
}

type staleMergingPullRequestDecision struct {
	targetState         string
	reason              string
	operationalEvidence string
	workpadURL          string
}

type mergeBaseRefreshDecision struct {
	applicable bool
	proceed    bool
	reason     string
}

type mergingIssuePriority struct {
	reasons       map[string]string
	stickyIssueID string
}

type autoPromoteReworkLimitSummary struct {
	Limit        int
	Count        int
	ReasonCounts []autoPromoteReworkReasonCount
	Signature    autoPromoteReworkSignature
}

type autoPromoteReworkReasonCount struct {
	Reason string
	Count  int
}

type autoPromoteReworkSignature struct {
	PRNumber     int64
	HeadSHA      string
	FailedChecks []string
}

func (o *Orchestrator) autoPromoteHumanReviewIssues(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) autoPromoteTickResult {
	cfg := normalizeAutoPromoteConfig(o.cfg.AutoPromote)
	if !cfg.Enabled {
		if o.logger != nil {
			o.logger.Debug("auto promote skipped", "reason", AutoPromoteReasonDisabled)
		}
		return autoPromoteTickResult{}
	}

	result := autoPromoteTickResult{transitioned: map[string]struct{}{}}
	for _, issue := range o.autoPromoteEvaluationIssues(state, issues, cfg) {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if _, operational := operationalCompletionFromIssue(issue); !operational {
			if handled, transitioned := o.parkRepeatedMergeRevocations(ctx, state, issue, now); handled {
				if transitioned {
					result.transitioned[issueID] = struct{}{}
					o.clearAutoPromotedIssueDispatchMemory(state, issueID)
				}
				continue
			}
		}
		if gateRequiresPullRequest(cfg.Gate) {
			var hydrated bool
			issue, hydrated = o.hydrateAutoPromoteReviewThreads(ctx, issue)
			if !hydrated {
				continue
			}
		}

		issue, securityAudit := o.liveSecurityAuditEvaluation(ctx, issue)
		summary := AutoPromoteSummaryFromIssue(issue)
		summary.CompletedFinalState = autoPromoteCompletedFinalState(state, issueID)
		summary.OperationalCompletionAccepted = autoPromoteOperationalCompletionAccepted(state, issueID)
		summary.AutomatedReviewWaitExpired = autoPromoteReviewWaitExpired(state, issueID, cfg, now)
		summary.SecurityAudit = securityAudit
		decision := EvaluateAutoPromote(issue, summary, cfg, now)
		if decision.Reason == AutoPromoteReasonSecurityAuditMissing {
			o.startSecurityAuditStage(ctx, issue, now)
			recordAutoPromoteSnapshotDecision(state, issueID, decision)
			o.logAutoPromoteDecision(issue, decision, "")
			continue
		}
		if decision.Reason == AutoPromoteReasonValidatorMissing {
			validation, shouldComment, ok := o.validatorStageResult(ctx, issue)
			if !ok {
				o.startValidatorStage(ctx, state, issue, now)
				recordAutoPromoteSnapshotDecision(state, issueID, decision)
				o.logAutoPromoteDecision(issue, decision, "")
				continue
			}
			summary.Validator = validation
			if shouldComment {
				o.commentValidatorResult(ctx, issue, validation)
				o.markValidatorResultCommented(ctx, issue)
			}
			decision = EvaluateAutoPromote(issue, summary, cfg, now)
		}
		if decision.Reason == AutoPromoteReasonCINotGreen &&
			o.retryTransientPullRequestChecks(ctx, state, issue, now, string(AutoPromoteReasonCINotGreen)) {
			decision := autoPromoteDecision(AutoPromoteActionSkip, AutoPromoteReasonCINotGreen)
			recordAutoPromoteSnapshotDecision(state, issueID, decision)
			o.logAutoPromoteDecision(issue, decision, "")
			continue
		}
		if autoPromoteDecisionNeedsWorkpadHydration(decision) {
			issue, decision = o.hydrateAutoPromoteWorkpadDecision(ctx, issue, summary, cfg, now)
		}
		targetState := autoPromoteTargetState(decision.Action, cfg)
		if targetState == "" {
			recordAutoPromoteSnapshotDecision(state, issueID, decision)
			o.logAutoPromoteDecision(issue, decision, "")
			continue
		}
		if !o.applyAutoPromoteDecision(ctx, state, issue, summary, decision, targetState, now) {
			continue
		}
		o.recordAutoPromoteReworkHandoff(state, issue, summary, decision, targetState)
		result.transitioned[issueID] = struct{}{}
		o.clearAutoPromotedIssueDispatchMemory(state, issueID)
		if mergeWorkerIssue(promotedIssue(issue, targetState, now)) {
			promoted := promotedIssue(issue, targetState, now)
			o.recordMergeQueueEntered(state, promoted, now, "auto_promote")
			result.dispatchCandidates = append(result.dispatchCandidates, promoted)
			o.logMergeWorkerPickup(promoted, "auto_promote")
		}
	}
	if len(result.transitioned) == 0 {
		return autoPromoteTickResult{}
	}
	return result
}

func autoPromoteCompletedFinalState(state *State, issueID string) string {
	if state == nil {
		return ""
	}
	completed, ok := state.Completed[strings.TrimSpace(issueID)]
	if !ok {
		return ""
	}
	return completed.FinalState
}

func autoPromoteOperationalCompletionAccepted(state *State, issueID string) bool {
	if state == nil {
		return false
	}
	completed, ok := state.Completed[strings.TrimSpace(issueID)]
	return ok && strings.TrimSpace(completed.CompletionKind) == workpad.CompletionOperational
}

func autoPromoteReviewWaitExpired(state *State, issueID string, cfg AutoPromoteConfig, now time.Time) bool {
	if state == nil {
		return false
	}
	completed, ok := state.Completed[strings.TrimSpace(issueID)]
	if !ok || completed.CompletedAt.IsZero() {
		return false
	}
	cfg = normalizeAutoPromoteConfig(cfg)
	return cfg.GateWaitTimeoutAction == autoPromoteGateWaitTimeoutMerge &&
		!now.Before(completed.CompletedAt.Add(cfg.GateWaitTimeout))
}

func recordAutoPromoteSnapshotDecision(state *State, issueID string, decision AutoPromoteDecision) {
	if state == nil {
		return
	}
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return
	}
	if decision.Action != AutoPromoteActionAwaitReview && decision.Action != AutoPromoteActionSkip {
		delete(state.AutoPromoteDecisions, issueID)
		return
	}
	if state.AutoPromoteDecisions == nil {
		state.AutoPromoteDecisions = map[string]AutoPromoteDecision{}
	}
	state.AutoPromoteDecisions[issueID] = cloneAutoPromoteDecision(decision)
}

func (o *Orchestrator) autoPromoteEvaluationIssues(
	state *State,
	issues []connector.Issue,
	cfg AutoPromoteConfig,
) []connector.Issue {
	out := issuesInStates(issues, []string{cfg.SourceState})
	seen := make(map[string]struct{}, len(out))
	for _, issue := range out {
		if issueID := strings.TrimSpace(issue.ID); issueID != "" {
			seen[issueID] = struct{}{}
		}
	}

	for _, issue := range issuesInStates(issues, o.cfg.ActiveStates) {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if normalizeState(issue.State) == "todo" && !autoPromoteIssueCompleted(state, issueID) {
			continue
		}
		if _, ok := seen[issueID]; ok {
			continue
		}
		if !autoPromoteSourceGateWaitEnabled(cfg) {
			continue
		}
		if !autoPromoteActiveGatePendingIssue(issue, state, o.cfg, cfg) {
			continue
		}
		out = append(out, cloneIssue(issue))
		seen[issueID] = struct{}{}
	}
	return out
}

func autoPromoteIssueCompleted(state *State, issueID string) bool {
	if state == nil {
		return false
	}
	_, ok := state.Completed[issueID]
	return ok
}

func (o *Orchestrator) restoreDurableGateWaitCompletions(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
) {
	if o == nil || state == nil {
		return
	}
	issues = o.refreshRequiredGateEvidence(ctx, state, issues)
	if o.workAttempts == nil {
		return
	}
	autoCfg := normalizeAutoPromoteConfig(o.cfg.AutoPromote)
	gateWaitTracking := autoPromoteDurableGateWaitTrackingEnabled(autoCfg)
	if state.Completed == nil {
		state.Completed = map[string]Completed{}
	}
	for _, issue := range issues {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if completed, ok := state.Completed[issueID]; ok {
			if completed.GateWaitReason == completedReworkGateWaitReason &&
				(!completedReworkGateWaitEvidenceCurrent(completed, issue) || !o.reworkGateWaitCurrent(ctx, issue)) {
				delete(state.Completed, issueID)
			}
			continue
		}
		completion, operational := operationalCompletionFromIssue(issue)
		var (
			attempt store.WorkAttempt
			ok      bool
			err     error
		)
		switch {
		case operational:
			attempt, ok, err = o.latestSuccessfulOperationalCompletionAttempt(ctx, issue, completion)
		case gateWaitTracking && autoPromoteDurableGateWaitTrackedIssue(issue, o.cfg, autoCfg):
			attempt, ok, err = o.latestSuccessfulGateWaitAttempt(ctx, issue)
		default:
			continue
		}
		if err != nil {
			if o.logger != nil {
				o.logger.Warn(
					"restore durable completion failed",
					"issue_id", issueID,
					"identifier", issue.Identifier,
					"error", err,
				)
			}
			continue
		}
		if !ok {
			continue
		}
		state.Completed[issueID] = completedFromGateWaitAttempt(issue, attempt)
	}
}

func (o *Orchestrator) latestSuccessfulGateWaitAttempt(
	ctx context.Context,
	issue connector.Issue,
) (store.WorkAttempt, bool, error) {
	if o == nil || o.workAttempts == nil {
		return store.WorkAttempt{}, false, nil
	}
	if normalizeState(issue.State) == normalizeState(normalizeAutoPromoteConfig(o.cfg.AutoPromote).ReworkState) && !o.reworkGateWaitCurrent(ctx, issue) {
		return store.WorkAttempt{}, false, nil
	}
	attempts, err := o.recentAgentTerminalAttempts(ctx, issue)
	if err != nil {
		return store.WorkAttempt{}, false, err
	}
	for _, attempt := range attempts {
		if normalizeState(issue.State) == normalizeState(normalizeAutoPromoteConfig(o.cfg.AutoPromote).ReworkState) {
			if record, ok := implementProgressRecordFromAttempt(attempt); ok && normalizeState(record.TrackerState) == normalizeState(issue.State) &&
				attempt.TerminalState != store.WorkAttemptTerminalSuccess {
				return store.WorkAttempt{}, false, nil
			}
		}
		if attempt.TerminalState != store.WorkAttemptTerminalSuccess {
			continue
		}
		if !gateWaitAttemptMatchesPullRequest(attempt, issue, normalizeAutoPromoteConfig(o.cfg.AutoPromote).ReworkState) {
			continue
		}
		return attempt, true, nil
	}
	return store.WorkAttempt{}, false, nil
}

func (o *Orchestrator) latestSuccessfulOperationalCompletionAttempt(
	ctx context.Context,
	issue connector.Issue,
	completion operationalCompletion,
) (store.WorkAttempt, bool, error) {
	if completion.recordedAt == nil || completion.recordedAt.IsZero() {
		return store.WorkAttempt{}, false, nil
	}
	attempts, err := o.recentAgentTerminalAttempts(ctx, issue)
	if err != nil {
		return store.WorkAttempt{}, false, err
	}
	for _, attempt := range attempts {
		if attempt.TerminalState != store.WorkAttemptTerminalSuccess || attempt.CompletedAt.Before(*completion.recordedAt) {
			continue
		}
		record, ok := implementProgressRecordFromAttempt(attempt)
		if !ok || strings.TrimSpace(record.Outcome) != string(store.WorkAttemptTerminalSuccess) {
			continue
		}
		if strings.TrimSpace(record.WorkpadStatus) != workpad.StatusComplete ||
			strings.TrimSpace(record.CompletionKind) != workpad.CompletionOperational ||
			!slices.Contains(record.ProgressKinds, "operational_completion") {
			continue
		}
		return attempt, true, nil
	}
	return store.WorkAttempt{}, false, nil
}

func (o *Orchestrator) recentAgentTerminalAttempts(
	ctx context.Context,
	issue connector.Issue,
) ([]store.WorkAttempt, error) {
	if o == nil || o.workAttempts == nil {
		return nil, nil
	}
	limit := normalizeAutoPromoteConfig(o.cfg.AutoPromote).NoProgressLimit + 1
	if limit < 20 {
		limit = 20
	}
	attempts, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{
		ProjectID:  strings.TrimSpace(o.cfg.Project.ID),
		IssueID:    strings.TrimSpace(issue.ID),
		Identifier: strings.TrimSpace(issue.Identifier),
		IssueURL:   strings.TrimSpace(issue.URL),
		WorkerType: "agent",
		Limit:      limit,
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func gateWaitAttemptMatchesPullRequest(attempt store.WorkAttempt, issue connector.Issue, reworkState string) bool {
	record, ok := implementProgressRecordFromAttempt(attempt)
	if !ok || strings.TrimSpace(record.Outcome) != string(store.WorkAttemptTerminalSuccess) {
		return false
	}
	if issue.PullRequest == nil {
		return false
	}
	currentHeadSHA := strings.TrimSpace(issue.PullRequest.HeadSHA)
	attemptHeadSHA := strings.TrimSpace(record.CurrentSignature.HeadSHA)
	if currentHeadSHA == "" || attemptHeadSHA == "" || attemptHeadSHA != currentHeadSHA {
		return false
	}
	prNumber := int64(pullRequestNumber(issue))
	if prNumber <= 0 {
		return false
	}
	matched := false
	if attempt.PRNumber != nil && *attempt.PRNumber > 0 {
		if *attempt.PRNumber != prNumber {
			return false
		}
		matched = true
	}
	if record.CurrentSignature.PRNumber > 0 {
		if record.CurrentSignature.PRNumber != prNumber {
			return false
		}
		matched = true
	}
	gateWaitRecord, _ := completionGateWaitRecordFromAttempt(attempt)
	gateWaitReason := strings.TrimSpace(gateWaitRecord.Reason)
	if gateWaitReason == completedReworkGateWaitReason {
		if normalizeState(issue.State) != normalizeState(reworkState) ||
			strings.TrimSpace(record.WorkpadStatus) == workpad.StatusBlocked || strings.TrimSpace(record.HumanAction) != "" ||
			!reworkGateWaitPullRequestReady(issue) || reworkGateWaitWorkpadBlocked(issue) ||
			normalizeState(record.TrackerState) != normalizeState(issue.State) ||
			record.WorkspaceDiffStats.FilesChanged != 0 ||
			record.WorkspaceDiffStats.AddedLines != 0 ||
			record.WorkspaceDiffStats.RemovedLines != 0 ||
			record.WorkspaceDiffStats.UnpushedCommits != 0 ||
			strings.TrimSpace(record.WorkspaceDiffStats.Status) == "" {
			return false
		}
		if issue.StageUpdatedAt != nil && issue.StageUpdatedAt.After(attempt.CompletedAt) {
			return false
		}
	} else if normalizeState(issue.State) == normalizeState(reworkState) {
		return false
	}
	return matched
}

func completedFromGateWaitAttempt(issue connector.Issue, attempt store.WorkAttempt) Completed {
	completionKind := ""
	if record, ok := implementProgressRecordFromAttempt(attempt); ok {
		completionKind = strings.TrimSpace(record.CompletionKind)
	}
	gateWaitReason := completionGateWaitReasonFromAttempt(attempt)
	return Completed{
		Issue:            cloneIssue(issue),
		StartedAt:        attempt.StartedAt,
		CompletedAt:      attempt.CompletedAt,
		FinalState:       FinalStateCompleted,
		CompletionKind:   completionKind,
		GateWaitReason:   gateWaitReason,
		gateWaitEvidence: completionGateWaitEvidence(gateWaitReason, issue),
	}
}

func completionGateWaitEvidence(reason string, issue connector.Issue) connector.Issue {
	if strings.TrimSpace(reason) != completedReworkGateWaitReason {
		return connector.Issue{}
	}
	return cloneIssue(issue)
}

func completedReworkGateWaitProgress(
	running Running,
	decision implementCompletionProgressDecision,
	cfg Config,
	finalState string,
) (implementCompletionProgressDecision, string) {
	autoCfg := normalizeAutoPromoteConfig(cfg.AutoPromote)
	dispatchState := firstNonBlank(running.DispatchSourceState, running.Issue.State)
	if strings.TrimSpace(finalState) != FinalStateCompleted ||
		normalizeState(dispatchState) != normalizeState(autoCfg.ReworkState) ||
		!autoPromoteReworkGateWaitTrackedIssue(decision.Issue, cfg, autoCfg) ||
		decision.Block || decision.DependencyDeferral ||
		decision.WorkpadStatus != workpad.StatusComplete || decision.HumanAction != "" || decision.Warning != "" ||
		!reworkGateWaitWorkpadComplete(decision.Issue) || !reworkGateWaitPullRequestReady(decision.Issue) ||
		!reworkGateWaitAuditReady(cfg.AutoPromote.Gate, decision.SecurityAudit) ||
		!implementProgressSignatureUsable(decision.CurrentSignature) ||
		!implementProgressOperationalWorkspaceClean(decision.WorkspaceDiffStats) {
		return decision, ""
	}
	decision.Outcome = store.WorkAttemptTerminalSuccess
	decision.Block = false
	decision.BlockReason = ""
	return decision, completedReworkGateWaitReason
}

func completionGateWaitMetadata(reason string, issue connector.Issue) map[string]any {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil
	}
	summary := AutoPromoteSummaryFromIssue(issue)
	return map[string]any{completionGateWaitMetadataKey: completionGateWaitRecord{
		Reason:         reason,
		MergeableState: strings.TrimSpace(summary.MergeableState),
		P1Findings:     append([]AutoPromoteFinding(nil), summary.P1Findings...),
	}}
}

func completionGateWaitReasonFromAttempt(attempt store.WorkAttempt) string {
	record, _ := completionGateWaitRecordFromAttempt(attempt)
	return strings.TrimSpace(record.Reason)
}

func completionGateWaitRecordFromAttempt(attempt store.WorkAttempt) (completionGateWaitRecord, bool) {
	var root struct {
		CompletionGateWait completionGateWaitRecord `json:"completion_gate_wait"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(attempt.WorkerMetadataJSON)), &root); err != nil {
		return completionGateWaitRecord{}, false
	}
	return root.CompletionGateWait, strings.TrimSpace(root.CompletionGateWait.Reason) != ""
}

func autoPromoteActiveGatePendingIssue(
	issue connector.Issue,
	state *State,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	if state == nil {
		return false
	}
	completed, ok := state.Completed[strings.TrimSpace(issue.ID)]
	if !ok {
		return false
	}
	if strings.TrimSpace(completed.GateWaitReason) == completedReworkGateWaitReason {
		return autoPromoteCompletedReworkGateWaitCurrent(issue, completed, cfg, autoCfg)
	}
	if !autoPromoteActiveGateEligibleIssue(issue, cfg, autoCfg) {
		return false
	}
	autoCfg = normalizeAutoPromoteConfig(autoCfg)
	operationalCompletionAccepted := completedOperationalCompletionAccepted(issue, completed.CompletionKind)
	return completedActiveFinalStateReviewEligible(completed.FinalState, autoCfg.SourceState) &&
		completedActiveIssueReadyForReview(issue, gateRequiresPullRequest(autoCfg.Gate), operationalCompletionAccepted)
}

func autoPromoteActiveGateTrackedIssue(
	issue connector.Issue,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	return autoPromoteActiveGateEligibleIssue(issue, cfg, autoCfg) && issueHasOpenPullRequest(issue)
}

func autoPromoteDurableGateWaitTrackedIssue(
	issue connector.Issue,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	if autoPromoteActiveGateTrackedIssue(issue, cfg, autoCfg) ||
		autoPromoteReworkGateWaitTrackedIssue(issue, cfg, autoCfg) {
		return true
	}
	autoCfg = normalizeAutoPromoteConfig(autoCfg)
	return autoPromoteReviewStateDeadlineTrackingEnabled(autoCfg) &&
		normalizeState(issue.State) == normalizeState(autoCfg.SourceState) &&
		!stateIn(issue.State, cfg.TerminalStates) &&
		!autoPromoteHumanReviewRequired(issue, autoCfg, autoCfg.Gate) &&
		issueHasOpenPullRequest(issue)
}

func autoPromoteReworkGateWaitTrackedIssue(
	issue connector.Issue,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	autoCfg = normalizeAutoPromoteConfig(autoCfg)
	return strings.TrimSpace(issue.ID) != "" &&
		stateIn(issue.State, cfg.ActiveStates) &&
		!stateIn(issue.State, cfg.TerminalStates) &&
		normalizeState(issue.State) == normalizeState(autoCfg.ReworkState) &&
		autoPromoteSourceGateWaitEnabled(autoCfg) &&
		!autoPromoteHumanReviewRequired(issue, autoCfg, autoCfg.Gate) &&
		issueHasOpenPullRequest(issue)
}

func autoPromoteCompletedReworkGateWaitCurrent(
	issue connector.Issue,
	completed Completed,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	autoCfg = normalizeAutoPromoteConfig(autoCfg)
	if !autoPromoteReworkGateWaitTrackedIssue(issue, cfg, autoCfg) ||
		strings.TrimSpace(completed.GateWaitReason) != completedReworkGateWaitReason ||
		!completedActiveFinalStateReviewEligible(completed.FinalState, autoCfg.SourceState) {
		return false
	}
	return completedReworkGateWaitEvidenceCurrent(completed, issue)
}

func completedReworkGateWaitEvidenceCurrent(completed Completed, issue connector.Issue) bool {
	if !reworkGateWaitPullRequestReady(issue) || reworkGateWaitWorkpadBlocked(issue) {
		return false
	}
	previous := completed.gateWaitEvidence
	if strings.TrimSpace(previous.ID) == "" {
		previous = completed.Issue
	}
	if normalizeState(previous.State) != normalizeState(issue.State) ||
		previous.PullRequest == nil || issue.PullRequest == nil ||
		pullRequestNumber(previous) != pullRequestNumber(issue) ||
		strings.TrimSpace(previous.PullRequest.HeadSHA) == "" ||
		strings.TrimSpace(previous.PullRequest.HeadSHA) != strings.TrimSpace(issue.PullRequest.HeadSHA) {
		return false
	}
	if issue.StageUpdatedAt != nil && !completed.CompletedAt.IsZero() && issue.StageUpdatedAt.After(completed.CompletedAt) {
		return false
	}
	return true
}

func (o *Orchestrator) reworkGateWaitCurrent(ctx context.Context, issue connector.Issue) bool {
	if !reworkGateWaitPullRequestReady(issue) || reworkGateWaitWorkpadBlocked(issue) {
		return false
	}
	refreshed, current := o.refreshImplementCompletionIssue(ctx, issue)
	return current && reworkGateWaitWorkpadComplete(refreshed) &&
		reworkGateWaitAuditReady(o.cfg.AutoPromote.Gate, o.securityAuditEvaluation(ctx, refreshed))
}

func reworkGateWaitWorkpadComplete(issue connector.Issue) bool {
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	return ok && signal != nil && signal.Invalid == nil && signal.Source == workpad.SourceStructured &&
		strings.TrimSpace(signal.Status) == workpad.StatusComplete && len(signal.Blockers) == 0 && strings.TrimSpace(signal.HumanAction) == ""
}

func reworkGateWaitWorkpadBlocked(issue connector.Issue) bool {
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	return ok && signal != nil && !reworkGateWaitWorkpadComplete(issue)
}

func reworkGateWaitPullRequestReady(issue connector.Issue) bool {
	if issue.PullRequest == nil || implementProgressHydrationUnavailableReason(issue.PullRequest) != "" {
		return false
	}
	summary := AutoPromoteSummaryFromIssue(issue)
	return !issue.PullRequest.Draft && !autoPromoteMergeConflicts(summary.MergeableState) && len(summary.P1Findings) == 0 &&
		len(summary.UnresolvedReviewThreads) == 0 && len(summary.FailedChecks) == 0
}

func reworkGateWaitAuditReady(cfg gate.Config, evaluation securityaudit.Evaluation) bool {
	return !gate.Effective(cfg).SecurityAudit.Enabled || evaluation.Allowed || evaluation.Reason == securityaudit.ReasonMissing
}

func autoPromoteActiveGateEligibleIssue(
	issue connector.Issue,
	cfg Config,
	autoCfg AutoPromoteConfig,
) bool {
	autoCfg = normalizeAutoPromoteConfig(autoCfg)
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return false
	}
	if !stateIn(issue.State, cfg.ActiveStates) || stateIn(issue.State, cfg.TerminalStates) {
		return false
	}
	if !autoPromoteActiveGateTrackingEnabled(autoCfg) {
		return false
	}
	switch normalizeState(issue.State) {
	case normalizeState(autoCfg.SourceState), normalizeState(autoCfg.PassState), normalizeState(autoCfg.ReworkState):
		return false
	}
	return !autoPromoteHumanReviewRequired(issue, autoCfg, autoCfg.Gate)
}

func autoPromoteSourceGateWaitEnabled(cfg AutoPromoteConfig) bool {
	cfg = normalizeAutoPromoteConfig(cfg)
	return cfg.Enabled &&
		cfg.GateWaitState == autoPromoteGateWaitSource &&
		gate.Effective(cfg.Gate).Kind == gate.KindCommand
}

func autoPromoteActiveGateTrackingEnabled(cfg AutoPromoteConfig) bool {
	cfg = normalizeAutoPromoteConfig(cfg)
	return cfg.Enabled && (cfg.QuietDuration > 0 || cfg.GateWaitState == autoPromoteGateWaitSource)
}

func autoPromoteDurableGateWaitTrackingEnabled(cfg AutoPromoteConfig) bool {
	return autoPromoteActiveGateTrackingEnabled(cfg) || autoPromoteReviewStateDeadlineTrackingEnabled(cfg)
}

func autoPromoteReviewStateDeadlineTrackingEnabled(cfg AutoPromoteConfig) bool {
	cfg = normalizeAutoPromoteConfig(cfg)
	mode := gate.AutomatedReviewMode(cfg.Gate)
	return cfg.Enabled &&
		cfg.GateWaitState == autoPromoteGateWaitReview &&
		cfg.GateWaitTimeoutAction == autoPromoteGateWaitTimeoutMerge &&
		(mode == gate.AutomatedReviewRequired || mode == gate.AutomatedReviewOptional)
}

func issueHasOpenPullRequest(issue connector.Issue) bool {
	return issue.PullRequest != nil && normalizePullRequestState(issue.PullRequest.State) == "open"
}

func (o *Orchestrator) hydrateAutoPromoteWorkpadDecision(
	ctx context.Context,
	issue connector.Issue,
	summary AutoPromoteSummary,
	cfg AutoPromoteConfig,
	now time.Time,
) (connector.Issue, AutoPromoteDecision) {
	if len(issue.Comments) == 0 {
		reader, ok := o.connector.(connector.IssueCommentReader)
		if !ok {
			return issue, EvaluateAutoPromote(issue, summary, cfg, now)
		}
		comments, err := reader.FetchIssueComments(ctx, issue)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("fetch auto-promote workpad comments failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
			}
			return issue, autoPromoteDecision(AutoPromoteActionSkip, AutoPromoteReasonWorkpadHydrationUnavailable)
		}
		issue = cloneIssue(issue)
		issue.Comments = comments
	}
	issue = o.hydrateAutoPromoteWorkpadBlockerRefs(ctx, issue, cfg)
	decision := EvaluateAutoPromote(issue, summary, cfg, now)
	if decision.Reason == AutoPromoteReasonWorkpadStatusInvalid && decision.Action != AutoPromoteActionRework {
		o.commentInvalidWorkpadStatus(ctx, issue, decision)
	}
	if decision.WorkpadProseFallbackDisabled && o.logger != nil {
		o.logger.Warn(
			"workpad prose fallback disabled",
			"issue_id", strings.TrimSpace(issue.ID),
			"identifier", issue.Identifier,
			"workpad_comment_url", decision.WorkpadCommentURL,
			"workpad_signal_source", decision.WorkpadSignalSource,
		)
	}
	return issue, decision
}

func (o *Orchestrator) hydrateAutoPromoteWorkpadBlockerRefs(
	ctx context.Context,
	issue connector.Issue,
	cfg AutoPromoteConfig,
) connector.Issue {
	refs := autoPromoteWorkpadBlockerRefs(issue, cfg.WorkpadStructuredOnly)
	if len(refs) == 0 {
		return issue
	}
	resolver, ok := o.connector.(connector.IssueReferenceResolver)
	if !ok {
		return issue
	}
	identifiers := make([]string, 0, len(refs))
	for _, ref := range refs {
		if identifier := strings.TrimSpace(ref.Identifier); identifier != "" {
			identifiers = append(identifiers, identifier)
		}
	}
	if len(identifiers) == 0 {
		return issue
	}
	resolved, err := resolver.FetchIssueStatesByIdentifiers(ctx, identifiers)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("hydrate auto-promote workpad blockers failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return issue
	}
	blockedBy := make([]connector.BlockedRef, 0, len(resolved))
	for _, resolvedIssue := range resolved {
		ref := autoPromoteBlockedRefFromIssue(resolvedIssue, cfg.TerminalStates)
		if strings.TrimSpace(ref.Identifier) != "" {
			blockedBy = append(blockedBy, ref)
		}
	}
	if len(blockedBy) == 0 {
		return issue
	}
	issue = cloneIssue(issue)
	issue.BlockedBy = mergeDependencyBlockedRefs(blockedBy, issue.BlockedBy)
	return issue
}

func autoPromoteDecisionNeedsWorkpadHydration(decision AutoPromoteDecision) bool {
	return decision.Action == AutoPromoteActionPromote ||
		decision.Reason == AutoPromoteReasonWorkpadBlocker ||
		decision.Reason == AutoPromoteReasonWorkpadStatusInvalid
}

func autoPromoteWorkpadBlockerRefs(issue connector.Issue, structuredOnly bool) []connector.BlockedRef {
	signal, ok := autoPromoteIssueWorkpadSignal(issue)
	if !ok || signal == nil || signal.Invalid != nil {
		return nil
	}
	if structuredOnly && signal.Source != workpad.SourceStructured {
		return nil
	}
	if signal.Source == workpad.SourceStructured {
		refs := make([]connector.BlockedRef, 0, len(signal.Blockers))
		for _, blocker := range signal.Blockers {
			if identifier := strings.TrimSpace(blocker.Identifier); identifier != "" {
				refs = append(refs, connector.BlockedRef{Identifier: identifier})
			}
		}
		return refs
	}
	reason := workpad.Reason(signal)
	if reason == "" {
		return nil
	}
	return dependencyRefsInText(reason, dependencyIssueRepo(issue.Identifier))
}

func autoPromoteBlockedRefFromIssue(issue connector.Issue, terminalStates []string) connector.BlockedRef {
	state := strings.TrimSpace(issue.State)
	if issue.Closed || autoPromotePullRequestMerged(issue.PullRequest) {
		state = doneStateName(terminalStates)
	}
	return connector.BlockedRef{
		ID:         strings.TrimSpace(issue.ID),
		Identifier: strings.TrimSpace(issue.Identifier),
		State:      state,
	}
}

func autoPromotePullRequestMerged(pullRequest *connector.PullRequest) bool {
	return pullRequest != nil && normalizePullRequestState(pullRequest.State) == "merged"
}

func (o *Orchestrator) commentInvalidWorkpadStatus(ctx context.Context, issue connector.Issue, decision AutoPromoteDecision) {
	hash := strings.TrimSpace(decision.WorkpadStatusInvalidHash)
	message := strings.TrimSpace(decision.WorkpadStatusInvalid)
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" || hash == "" || message == "" {
		return
	}
	marker := invalidWorkpadStatusCommentMarker(hash)
	for _, comment := range issue.Comments {
		if strings.Contains(comment.Body, marker) {
			return
		}
	}
	if err := o.connector.CreateComment(ctx, issueID, invalidWorkpadStatusComment(decision)); err != nil && o.logger != nil {
		o.logger.Warn(
			"workpad status invalid comment failed",
			"issue_id", issueID,
			"identifier", issue.Identifier,
			"workpad_status_hash", hash,
			"error", err,
		)
	}
}

func invalidWorkpadStatusCommentMarker(hash string) string {
	return "<!-- detent-workpad-status-invalid:" + strings.TrimSpace(hash) + " -->"
}

func invalidWorkpadStatusComment(decision AutoPromoteDecision) string {
	hash := strings.TrimSpace(decision.WorkpadStatusInvalidHash)
	var b strings.Builder
	b.WriteString(invalidWorkpadStatusCommentMarker(hash))
	b.WriteString("\nDetent could not parse the `detent-status` block in the latest `## Codex Workpad` comment.")
	b.WriteString("\n\n- reason: ")
	b.WriteString(strings.TrimSpace(decision.WorkpadStatusInvalid))
	b.WriteString("\n- allowed_statuses: ")
	b.WriteString(autoPromoteAllowedWorkpadStatuses())
	if url := strings.TrimSpace(decision.WorkpadCommentURL); url != "" {
		b.WriteString("\n- workpad_comment: ")
		b.WriteString(url)
	}
	b.WriteString("\n- block_hash: ")
	b.WriteString(hash)
	b.WriteString("\n\nUse this schema:")
	b.WriteString("\n\n```detent-status\nschema: 1\nstatus: in_progress\nblockers: []\nhuman_action: null\n```")
	return b.String()
}

func autoPromoteAllowedWorkpadStatuses() string {
	return strings.Join([]string{workpad.StatusInProgress, workpad.StatusBlocked, workpad.StatusComplete}, ", ")
}

func (o *Orchestrator) reconcileStaleLinkedPullRequestIssues(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) map[string]struct{} {
	transitioned := map[string]struct{}{}
	for _, issue := range issuesInStates(issues, staleLinkedPullRequestReconciliationStates(o.cfg)) {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" || issue.PullRequest == nil || issue.PullRequest.HydrationUnavailableReason == associationUnavailable {
			continue
		}
		pullRequestState := normalizePullRequestState(issue.PullRequest.State)
		if pullRequestState != "open" && pullRequestState != "merged" {
			continue
		}
		if stateIn(issue.State, o.cfg.TerminalStates) {
			continue
		}
		if staleTodoPullRequestAlreadyActive(state, issueID) {
			continue
		}

		if pullRequestState == "merged" {
			summary := staleMergedPullRequestSummaryFromIssue(issue)
			decision := staleMergedPullRequestDecision(issue, summary)
			if !stateIn(issue.State, o.cfg.ActiveStates) && decision.Reason == AutoPromoteReasonPullRequestHydrationUnavailable {
				o.logAutoPromoteDecision(issue, decision, "")
				continue
			}
			targetState := staleMergedPullRequestTargetState(decision, o.cfg.AutoPromote, o.cfg.TerminalStates)
			if targetState == "" {
				o.logAutoPromoteDecision(issue, decision, "")
				continue
			}
			if normalizeState(targetState) == normalizeState(issue.State) {
				continue
			}
			if !o.applyStaleMergedPullRequestDecision(ctx, state, issue, summary, decision, targetState, now) {
				continue
			}
			transitioned[issueID] = struct{}{}
			o.clearAutoPromotedIssueDispatchMemory(state, issueID)
			continue
		}

		if normalizeState(issue.State) != "todo" {
			continue
		}
		if gateRequiresPullRequest(o.cfg.AutoPromote.Gate) {
			var hydrated bool
			issue, hydrated = o.hydrateAutoPromoteReviewThreads(ctx, issue)
			if !hydrated {
				continue
			}
		}
		summary := AutoPromoteSummaryFromIssue(issue)
		if !summary.PullRequestPresent {
			continue
		}
		decision := staleTodoPullRequestDecision(issue, summary, o.cfg.AutoPromote, now)
		if autoPromoteDecisionNeedsWorkpadHydration(decision) {
			issue, decision = o.hydrateAutoPromoteWorkpadDecision(ctx, issue, summary, o.cfg.AutoPromote, now)
		}
		targetState := staleTodoPullRequestTargetState(decision, o.cfg.AutoPromote)
		if autoPromoteActiveGatePendingIssue(issue, state, o.cfg, o.cfg.AutoPromote) &&
			normalizeState(targetState) == normalizeState(normalizeAutoPromoteConfig(o.cfg.AutoPromote).SourceState) &&
			staleTodoPullRequestShouldStayActive(decision) {
			continue
		}
		if targetState == "" {
			o.logAutoPromoteDecision(issue, decision, "")
			continue
		}
		if !o.applyStaleTodoPullRequestDecision(ctx, state, issue, summary, decision, targetState, now) {
			continue
		}
		transitioned[issueID] = struct{}{}
		o.clearAutoPromotedIssueDispatchMemory(state, issueID)
	}
	if len(transitioned) == 0 {
		return nil
	}
	return transitioned
}

func staleLinkedPullRequestReconciliationStates(cfg Config) []string {
	autoCfg := normalizeAutoPromoteConfig(cfg.AutoPromote)
	return appendUniqueStates(cfg.ActiveStates, cfg.ObservedStates, []string{autoCfg.SourceState})
}

func (o *Orchestrator) reconcileStaleMergingPullRequestIssues(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) map[string]struct{} {
	transitioned := map[string]struct{}{}
	o.recordMergeQueueEntries(state, issues, now, "tracker")
	consumedRepositories := activeMergeWorkerRepositories(state)
	for _, issue := range staleMergingQueueIssues(issues, o.cfg, state, now) {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if _, deferred := state.nativeMergeQueueDeferred[issueID]; deferred {
			continue
		}
		repository := mergeWorkerRepositoryKey(issue)
		decision := staleMergingPullRequestDecisionForIssue(issue, o.cfg)
		if mergeWorkerRepositoryConsumed(consumedRepositories, repository) && decision.reason != string(AutoPromoteReasonCINotGreen) {
			continue
		}
		if staleMergingPullRequestDispatchActive(state, issueID) {
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		if decision.reason == string(AutoPromoteReasonOperationalCompletion) {
			completion, _ := operationalCompletionFromIssue(issue)
			_, completed, err := o.latestSuccessfulOperationalCompletionAttempt(ctx, issue, completion)
			if err != nil {
				if o.logger != nil {
					o.logger.Warn(
						"stale operational completion reconciliation failed",
						"issue_id", issueID,
						"identifier", issue.Identifier,
						"error", err,
					)
				}
				consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
				continue
			}
			if !completed {
				o.logStaleMergingPullRequestDeferred(issue, decision)
				consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
				continue
			}
		}
		if decision.reason == string(AutoPromoteReasonCINotGreen) &&
			o.retryTransientPullRequestChecks(ctx, state, issue, now, decision.reason) {
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		if decision.targetState == "" {
			if strings.TrimSpace(decision.reason) != "" {
				o.logStaleMergingPullRequestDeferred(issue, decision)
			}
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		if !o.applyStaleMergingPullRequestDecision(ctx, state, issue, decision, now) {
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		transitioned[issueID] = struct{}{}
		o.clearAutoPromotedIssueDispatchMemory(state, issueID)
	}
	if len(transitioned) == 0 {
		return nil
	}
	return transitioned
}

func staleMergingPullRequestDecisionForIssue(issue connector.Issue, cfg Config) staleMergingPullRequestDecision {
	if strings.TrimSpace(issue.ID) == "" {
		return staleMergingPullRequestDecision{}
	}
	if closedCompletedIssueNeedsStatusReconciliation(issue, cfg.TerminalStates) {
		return staleMergingPullRequestDecision{targetState: doneStateName(cfg.TerminalStates), reason: "issue_closed_completed"}
	}
	if completion, ok := operationalCompletionFromIssue(issue); ok {
		return staleMergingPullRequestDecision{
			targetState:         doneStateName(cfg.TerminalStates),
			reason:              string(AutoPromoteReasonOperationalCompletion),
			operationalEvidence: completion.evidence,
			workpadURL:          completion.workpadURL,
		}
	}
	if _, revoked := mergeApprovalLabelRevoked(issue, cfg); revoked {
		return staleMergingPullRequestDecision{
			targetState: normalizeAutoPromoteConfig(cfg.AutoPromote).SourceState,
			reason:      mergeRevocationApprovalLabelRemoved,
		}
	}
	pullRequest := issue.PullRequest
	if pullRequest == nil {
		return staleMergingPullRequestDecision{targetState: autoPromoteSourceState, reason: string(AutoPromoteReasonMissingPullRequest)}
	}
	pullRequestState := normalizePullRequestState(pullRequest.State)
	if pullRequestState == "" && pullRequestHydrationBlocksProgress(pullRequest) {
		return staleMergingPullRequestDecision{reason: string(AutoPromoteReasonPullRequestHydrationUnavailable)}
	}
	switch pullRequestState {
	case "merged":
		return staleMergingPullRequestDecision{targetState: doneStateName(cfg.TerminalStates), reason: "pull_request_merged"}
	case "open":
		if pullRequestHydrationBlocksProgress(pullRequest) {
			return staleMergingPullRequestDecision{reason: string(AutoPromoteReasonPullRequestHydrationUnavailable)}
		}
		if _, revoked := mergeCITriggerLabelRevoked(issue, cfg); revoked {
			return staleMergingPullRequestDecision{
				targetState: normalizeAutoPromoteConfig(cfg.AutoPromote).SourceState,
				reason:      mergeRevocationCITriggerLabelRemoved,
			}
		}
		if pullRequest.Draft {
			return staleMergingPullRequestDecision{targetState: autoPromoteSourceState, reason: "draft_pull_request"}
		}
		if mergeWorkerCIFailed(pullRequest) {
			return staleMergingPullRequestDecision{targetState: autoPromoteReworkState, reason: string(AutoPromoteReasonCINotGreen)}
		}
		return staleMergingPullRequestDecision{}
	default:
		return staleMergingPullRequestDecision{targetState: autoPromoteReworkState, reason: "pull_request_not_open"}
	}
}

func (o *Orchestrator) applyStaleMergingPullRequestDecision(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	decision staleMergingPullRequestDecision,
	now time.Time,
) bool {
	issueID := strings.TrimSpace(issue.ID)
	if err := o.updateIssueStateByID(ctx, state, issueID, issue, decision.targetState, now, decision.reason, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"stale_merging_pr_reconciliation_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.reason,
				"target_state", decision.targetState,
				"error", err,
			)
		}
		return false
	}

	body := staleMergingPullRequestComment(issue, decision)
	if strings.TrimSpace(body) != "" {
		if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
			o.logger.Warn(
				"stale_merging_pr_reconciliation_comment_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.reason,
				"target_state", decision.targetState,
				"error", err,
			)
		}
	}

	o.logStaleMergingPullRequestDecision(issue, decision)
	if decision.reason != string(AutoPromoteReasonOperationalCompletion) {
		if normalizeState(decision.targetState) == normalizeState(doneStateName(o.cfg.TerminalStates)) {
			o.recordMergeCompleted(state, issue, now, decision.targetState)
		} else {
			o.recordMergeFailed(state, issue, now, decision.reason, nil)
		}
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "stale_merging_pr_reconciled",
		Message: "reconciled stale Merging PR for " + issueLabel(issue) + " to " + decision.targetState + ": " + decision.reason,
	})
	return true
}

func staleMergingPullRequestComment(issue connector.Issue, decision staleMergingPullRequestDecision) string {
	var b strings.Builder
	b.WriteString("Reconciled this issue from Merging to ")
	b.WriteString(decision.targetState)
	b.WriteString(".")
	b.WriteString("\n\n- reason: ")
	b.WriteString(decision.reason)
	if evidence := strings.TrimSpace(decision.operationalEvidence); evidence != "" {
		b.WriteString("\n- operational_evidence: ")
		b.WriteString(evidence)
	}
	if url := strings.TrimSpace(decision.workpadURL); url != "" {
		b.WriteString("\n- workpad_comment: ")
		b.WriteString(url)
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
	return b.String()
}

func (o *Orchestrator) logStaleMergingPullRequestDecision(issue connector.Issue, decision staleMergingPullRequestDecision) {
	if o.logger == nil {
		return
	}
	attrs := mergeWorkerLogAttrs(issue,
		"reason", decision.reason,
		"target_state", decision.targetState,
	)
	if evidence := strings.TrimSpace(decision.operationalEvidence); evidence != "" {
		attrs = append(attrs, "operational_evidence", evidence)
	}
	if url := strings.TrimSpace(decision.workpadURL); url != "" {
		attrs = append(attrs, "workpad_comment_url", url)
	}
	o.logger.Info("stale_merging_pr_reconciled", attrs...)
}

func (o *Orchestrator) logStaleMergingPullRequestDeferred(issue connector.Issue, decision staleMergingPullRequestDecision) {
	if o.logger == nil {
		return
	}
	attrs := mergeWorkerLogAttrs(issue, "reason", decision.reason)
	o.logger.Info("stale_merging_pr_reconciliation_deferred", attrs...)
}

func (o *Orchestrator) logMergeWorkerPickup(issue connector.Issue, source string, attrs ...any) {
	if !o.beginDispatchStart() {
		return
	}
	defer o.finishDispatchStart()
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	values := mergeWorkerLogAttrs(issue, "source", strings.TrimSpace(source))
	values = append(values, attrs...)
	o.logger.Info("merge_worker_pickup", values...)
}

func (o *Orchestrator) logMergeWorkerAttempt(issue connector.Issue, attempt int, workerHost string) {
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	attrs := mergeWorkerLogAttrs(issue,
		"attempt", attempt,
		"worker_host", strings.TrimSpace(workerHost),
	)
	o.logger.Info("merge_worker_attempt", attrs...)
}

func (o *Orchestrator) logMergeWorkerSuccess(issue connector.Issue, finalState string) {
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	attrs := mergeWorkerLogAttrs(issue, "final_state", strings.TrimSpace(finalState))
	o.logger.Info("merge_worker_success", attrs...)
}

func (o *Orchestrator) logMergeWorkerFailure(issue connector.Issue, reason string, err error) {
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	attrs := mergeWorkerLogAttrs(issue, "reason", strings.TrimSpace(reason))
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	o.logger.Warn("merge_worker_failure", attrs...)
}

func (o *Orchestrator) logMergeWorkerSlotWait(
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
) {
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	o.logger.Info("merge_worker_slot_wait", mergeWorkerSlotDecisionAttrs(issue, decision, projectStats)...)
}

func (o *Orchestrator) logMergeWorkerSlotAcquired(
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
	timing MergeTiming,
) {
	if o.logger == nil || !mergeWorkerIssue(issue) {
		return
	}
	attrs := mergeWorkerSlotDecisionAttrs(issue, decision, projectStats)
	attrs = append(attrs, mergeTimingAttrs(timing)...)
	o.logger.Info("merge_worker_slot_acquired", attrs...)
}

func (o *Orchestrator) logDispatchSlotWait(
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
) {
	if o.logger == nil {
		return
	}
	o.logger.Info("dispatch_slot_wait", mergeWorkerSlotDecisionAttrs(issue, decision, projectStats)...)
}

func (o *Orchestrator) recordDispatchSlotWait(
	state *State,
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
	now time.Time,
) {
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "dispatch_slot_wait",
		Message: dispatchSlotWaitMessage(issue, decision, projectStats),
	})
}

func dispatchSlotWaitMessage(
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
) string {
	reason := strings.TrimSpace(decision.Reason)
	if reason == "" {
		reason = dispatchIssueFailureGlobalSlotUnavailable
	}
	return fmt.Sprintf(
		"dispatch waiting for %s state=%s reason=%s global_capacity=%d global_used=%d global_available=%d project_state_capacity=%d project_state_used=%d project_state_available=%d selected_project_id=%s selected_state=%s",
		issueLabel(issue),
		strings.TrimSpace(issue.State),
		reason,
		decision.GlobalCapacity,
		decision.GlobalUsed,
		decision.GlobalAvailable,
		projectStats.capacity,
		projectStats.used,
		projectStats.available,
		strings.TrimSpace(decision.SelectedProjectID),
		strings.TrimSpace(decision.SelectedState),
	)
}

func mergeWorkerSlotDecisionAttrs(
	issue connector.Issue,
	decision scheduler.DispatchGateDecision,
	projectStats projectStateSlotStats,
) []any {
	reason := strings.TrimSpace(decision.Reason)
	if reason == "" {
		reason = dispatchIssueFailureGlobalSlotUnavailable
	}
	attrs := mergeWorkerLogAttrs(issue,
		"state", strings.TrimSpace(issue.State),
		"reason", reason,
		"pool", strings.TrimSpace(decision.PoolName),
		"global_capacity", decision.GlobalCapacity,
		"global_used", decision.GlobalUsed,
		"global_available", decision.GlobalAvailable,
		"guaranteed_capacity", decision.GuaranteedCapacity,
		"burst_capacity", decision.BurstCapacity,
		"borrowed_slots", decision.BorrowedSlots,
		"shared_capacity", decision.SharedCapacity,
		"shared_used", decision.SharedUsed,
		"shared_available", decision.SharedAvailable,
		"project_state_capacity", projectStats.capacity,
		"project_state_used", projectStats.used,
		"project_state_available", projectStats.available,
		"lower_priority_running", decision.LowerPriorityRunning,
		"selected_project_id", strings.TrimSpace(decision.SelectedProjectID),
		"selected_state", strings.TrimSpace(decision.SelectedState),
		"ready_projects", decision.ReadyProjects,
		"running_projects", decision.RunningProjects,
	)
	return attrs
}

func mergeWorkerIssue(issue connector.Issue) bool {
	return normalizeState(issue.State) == normalizeState(autoPromoteMergingState)
}

func mergeWorkerLogAttrs(issue connector.Issue, attrs ...any) []any {
	out := []any{
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", issue.Identifier,
	}
	if issue.PullRequest != nil {
		if issue.PullRequest.Number > 0 {
			out = append(out, "pull_request_number", issue.PullRequest.Number)
		}
		if repository := pullRequestRepository(issue); repository != "" {
			out = append(out, "repository", repository)
		}
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			out = append(out, "pull_request", url)
		}
		if mergeableState := strings.TrimSpace(issue.PullRequest.MergeableState); mergeableState != "" {
			out = append(out, "mergeable_state", strings.ToLower(mergeableState))
		}
		if ciStatus := strings.TrimSpace(issue.PullRequest.CIStatus); ciStatus != "" {
			out = append(out, "ci_status", ciStatus)
		}
		if headSHA := strings.TrimSpace(issue.PullRequest.HeadSHA); headSHA != "" {
			out = append(out, "head_sha", headSHA)
		}
		if baseSHA := strings.TrimSpace(issue.PullRequest.BaseSHA); baseSHA != "" {
			out = append(out, "base_sha", baseSHA)
		}
		if reason := pullRequestHydrationUnavailableReason(issue.PullRequest); reason != "" {
			out = append(out, "pull_request_hydration_reason", reason)
		}
		if reason := strings.TrimSpace(issue.PullRequest.HydrationDegradedReason); reason != "" {
			out = append(out, "pull_request_hydration_degraded_reason", reason)
		}
		if issue.PullRequest.HydrationNextRetryAt != nil && !issue.PullRequest.HydrationNextRetryAt.IsZero() {
			out = append(out, "pull_request_hydration_next_retry_at", issue.PullRequest.HydrationNextRetryAt.UTC().Format(time.RFC3339))
		}
	}
	return append(out, attrs...)
}

func staleMergingPullRequestDispatchActive(state *State, issueID string) bool {
	if state == nil {
		return false
	}
	if _, ok := state.Running[issueID]; ok {
		return true
	}
	if _, ok := state.Claimed[issueID]; ok {
		return true
	}
	if _, ok := state.Retry[issueID]; ok {
		return true
	}
	return false
}

func staleMergingQueueIssues(issues []connector.Issue, cfg Config, state *State, now time.Time) []connector.Issue {
	queue := issuesInStates(issues, []string{autoPromoteMergingState})
	sortIssuesForDispatch(queue, cfg.DispatchPriorityByState, cfg.DispatchPriorityByLabel, cfg.PrioritizeUnblockers)
	prioritizeReadyMergingIssues(queue, state, now, cfg.MergeFairnessAge)
	return queue
}

func activeMergeWorkerRepositories(state *State) map[string]struct{} {
	if state == nil {
		return nil
	}
	repositories := map[string]struct{}{}
	for _, running := range state.Running {
		repositories = consumeActiveMergeWorkerRepository(repositories, running.Issue)
	}
	for _, claimed := range state.Claimed {
		repositories = consumeActiveMergeWorkerRepository(repositories, claimed.Issue)
	}
	for _, retry := range state.Retry {
		if reservation := state.mergeReservations[mergeWorkerRepositoryKey(retry.Issue)]; reservation.IssueID == retry.Issue.ID && reservation.ReleasedReason != "" {
			continue
		}
		repositories = consumeActiveMergeWorkerRepository(repositories, retry.Issue)
	}
	if len(repositories) == 0 {
		return nil
	}
	return repositories
}

func consumeActiveMergeWorkerRepository(repositories map[string]struct{}, issue connector.Issue) map[string]struct{} {
	if !mergeWorkerIssue(issue) {
		return repositories
	}
	return consumeMergeWorkerRepository(repositories, mergeWorkerRepositoryKey(issue))
}

func consumeMergeWorkerRepository(repositories map[string]struct{}, repository string) map[string]struct{} {
	repository = strings.TrimSpace(repository)
	if repository == "" {
		return repositories
	}
	if repositories == nil {
		repositories = map[string]struct{}{}
	}
	repositories[repository] = struct{}{}
	return repositories
}

func mergeWorkerRepositoryConsumed(repositories map[string]struct{}, repository string) bool {
	if repository == "" || len(repositories) == 0 {
		return false
	}
	_, ok := repositories[repository]
	return ok
}

func mergeWorkerRepositoryKey(issue connector.Issue) string {
	return strings.ToLower(strings.TrimSpace(pullRequestRepository(issue)))
}

func (o *Orchestrator) mergeWorkerDispatchCandidates(state *State, issues []connector.Issue, now time.Time) []connector.Issue {
	if o.dispatchQuiesced() {
		return nil
	}
	o.logMergeWorkerQueueCycle(state, issues, now)
	stickyID := stickyMergingIssueID(state, issues, now, o.cfg.MergeFairnessAge)
	candidates := o.staleMergingQueueDispatchCandidates(state, issues, now)
	if len(candidates) == 0 {
		return nil
	}
	out := make([]connector.Issue, 0, len(candidates))
	selectedByState := map[string]int{}
	for _, issue := range candidates {
		if mergeFairnessBlocks(state, stickyID, issue, now) {
			continue
		}
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if staleMergingPullRequestDispatchActive(state, issueID) {
			continue
		}
		if _, deferred := state.nativeMergeQueueDeferred[issueID]; deferred {
			continue
		}
		stateKey := normalizeState(issue.State)
		projectStats := o.projectStateSlotStats(issue, state)
		selected := selectedByState[stateKey]
		if selected > 0 {
			projectStats.used += selected
			projectStats.available -= selected
			if projectStats.available < 0 {
				projectStats.available = 0
			}
		}
		if projectStats.available <= 0 {
			o.logMergeBaseRefreshDeferred(issue, mergeBaseRefreshLaneUnavailable)
			o.logMergeWorkerSlotWait(
				issue,
				scheduler.DispatchGateDecision{Reason: mergeWorkerProjectStateFull},
				projectStats,
			)
			break
		}
		selectedByState[stateKey] = selected + 1
		o.clearAutoPromotedIssueDispatchMemory(state, issueID)
		laneAge := mergeWorkerIssueLaneAge(issue, now)
		o.logMergeWorkerPickup(issue, "stale_merging",
			"selection_position", len(out)+1,
			"selection_reason", mergeWorkerSelectionReason(issue, state, now, o.cfg.MergeFairnessAge),
			"lane_age_seconds", int64(laneAge/time.Second),
			"fairness_age_seconds", int64(o.cfg.MergeFairnessAge/time.Second),
		)
		out = append(out, issue)
	}
	return out
}

func (o *Orchestrator) logMergeWorkerQueueCycle(state *State, issues []connector.Issue, now time.Time) {
	if o.logger == nil {
		return
	}
	queueDepth := 0
	readyCount := 0
	agedCount := 0
	oldestLaneAge := time.Duration(0)
	queueIssueIDs := map[string]struct{}{}
	for _, issue := range issues {
		if !mergeWorkerIssue(issue) {
			continue
		}
		queueDepth++
		queueIssueIDs[strings.TrimSpace(issue.ID)] = struct{}{}
		if mergeWorkerHeadReady(issue) {
			readyCount++
		}
		laneAge := mergeWorkerIssueLaneAge(issue, now)
		if laneAge > oldestLaneAge {
			oldestLaneAge = laneAge
		}
		if mergeWorkerIssueAged(issue, now, o.cfg.MergeFairnessAge) {
			agedCount++
		}
	}
	projectStats := o.projectStateSlotStats(connector.Issue{State: autoPromoteMergingState}, state)
	occupant, occupantCount := mergeWorkerLaneOccupant(state)
	queuedBehind := queueDepth
	for issueID := range queueIssueIDs {
		if staleMergingPullRequestDispatchActive(state, issueID) {
			queuedBehind--
		}
	}
	if queuedBehind < 0 {
		queuedBehind = 0
	}
	attrs := []any{
		"queue_depth", queueDepth,
		"ready_count", readyCount,
		"aged_count", agedCount,
		"oldest_lane_age_seconds", int64(oldestLaneAge / time.Second),
		"fairness_age_seconds", int64(o.cfg.MergeFairnessAge / time.Second),
		"lane_occupied", occupantCount > 0,
		"lane_saturated", projectStats.available <= 0,
		"lane_occupant_count", occupantCount,
		"queued_behind", queuedBehind,
	}
	if occupantCount > 0 {
		occupancySeconds := int64(0)
		if !occupant.StartedAt.IsZero() && !now.Before(occupant.StartedAt) {
			occupancySeconds = int64(now.Sub(occupant.StartedAt) / time.Second)
		}
		attrs = append(attrs,
			"occupying_issue_id", strings.TrimSpace(occupant.Issue.ID),
			"occupying_issue_identifier", strings.TrimSpace(occupant.Issue.Identifier),
			"occupying_issue_number", issueNumberFromIdentifier(occupant.Issue.Identifier),
			"occupancy_seconds", occupancySeconds,
		)
	}
	if stickyIssueID := stickyMergingIssueID(state, issues, now, o.cfg.MergeFairnessAge); stickyIssueID != "" {
		attrs = append(attrs, "sticky_issue_id", stickyIssueID)
	}
	o.logger.Info("merge_worker_queue_cycle", attrs...)
}

func mergeWorkerLaneOccupant(state *State) (Running, int) {
	if state == nil {
		return Running{}, 0
	}
	var occupant Running
	count := 0
	for _, running := range state.Running {
		if !mergeWorkerIssue(running.Issue) {
			continue
		}
		count++
		if occupant.Issue.ID == "" ||
			(!running.StartedAt.IsZero() && (occupant.StartedAt.IsZero() || running.StartedAt.Before(occupant.StartedAt))) {
			occupant = running
		}
	}
	return occupant, count
}

func prioritizeReadyMergingIssues(issues []connector.Issue, state *State, now time.Time, fairnessAge time.Duration) mergingIssuePriority {
	priority := mergingIssuePriority{
		reasons:       map[string]string{},
		stickyIssueID: stickyMergingIssueID(state, issues, now, fairnessAge),
	}
	ordered := make([]connector.Issue, 0, len(issues))
	appended := make([]bool, len(issues))
	repositoryHeads := make(map[string]int)
	for index, issue := range issues {
		if !mergeWorkerIssue(issue) {
			continue
		}
		repository := mergeWorkerRepositoryKey(issue)
		if repository == "" {
			continue
		}
		if _, seen := repositoryHeads[repository]; !seen {
			repositoryHeads[repository] = index
		}
	}
	appendMatching := func(reason string, matches func(int, connector.Issue) bool) {
		for index, issue := range issues {
			if appended[index] || !mergeWorkerIssue(issue) || !matches(index, issue) {
				continue
			}
			appended[index] = true
			ordered = append(ordered, issue)
			if issueID := strings.TrimSpace(issue.ID); issueID != "" {
				priority.reasons[issueID] = reason
			}
		}
	}
	appendMatching(mergeSelectionReasonStickyAged, func(_ int, issue connector.Issue) bool {
		return strings.TrimSpace(issue.ID) == priority.stickyIssueID
	})
	agedIssues := make([]connector.Issue, 0, len(issues))
	for index, issue := range issues {
		if !appended[index] && mergeWorkerIssueAged(issue, now, fairnessAge) {
			appended[index] = true
			agedIssues = append(agedIssues, issue)
		}
	}
	slices.SortStableFunc(agedIssues, func(leftIssue connector.Issue, rightIssue connector.Issue) int {
		left := mergeQueueEnteredAt(leftIssue, time.Time{})
		right := mergeQueueEnteredAt(rightIssue, time.Time{})
		if left.Before(right) {
			return -1
		}
		if left.After(right) {
			return 1
		}
		return 0
	})
	for _, issue := range agedIssues {
		ordered = append(ordered, issue)
		if issueID := strings.TrimSpace(issue.ID); issueID != "" {
			priority.reasons[issueID] = mergeSelectionReasonAged
		}
	}
	appendMatching(mergeSelectionReasonClean, func(index int, issue connector.Issue) bool {
		repository := mergeWorkerRepositoryKey(issue)
		if repository != "" && repositoryHeads[repository] != index {
			return false
		}
		return mergeWorkerHeadReady(issue)
	})
	appendMatching(mergeSelectionReasonQueue, func(_ int, _ connector.Issue) bool { return true })
	if len(ordered) == 0 {
		return priority
	}
	next := 0
	for index := range issues {
		if !mergeWorkerIssue(issues[index]) {
			continue
		}
		issues[index] = ordered[next]
		next++
	}
	return priority
}

func stickyMergingIssueID(state *State, issues []connector.Issue, now time.Time, fairnessAge time.Duration) string {
	if state == nil || (len(state.Retry) == 0 && len(state.Running) == 0) {
		return ""
	}
	current := make(map[string]connector.Issue, len(issues))
	for _, issue := range issues {
		if issueID := strings.TrimSpace(issue.ID); issueID != "" {
			current[issueID] = issue
		}
	}
	selectedID := ""
	selectedEnteredAt := time.Time{}
	consider := func(issueID string, issue connector.Issue) {
		issueID = strings.TrimSpace(issueID)
		if refreshed, ok := current[issueID]; ok {
			issue = refreshed
		}
		if reservation := state.mergeReservations[mergeWorkerRepositoryKey(issue)]; reservation.IssueID == issueID {
			return
		}
		if !mergeWorkerIssueAged(issue, now, fairnessAge) {
			return
		}
		enteredAt := mergeQueueEnteredAt(issue, time.Time{})
		if selectedID == "" || enteredAt.Before(selectedEnteredAt) || enteredAt.Equal(selectedEnteredAt) && issueID < selectedID {
			selectedID = issueID
			selectedEnteredAt = enteredAt
		}
	}
	for issueID, running := range state.Running {
		consider(issueID, running.Issue)
	}
	for issueID, retry := range state.Retry {
		consider(issueID, retry.Issue)
	}
	return selectedID
}

func mergeWorkerSelectionReason(issue connector.Issue, state *State, now time.Time, fairnessAge time.Duration) string {
	if strings.TrimSpace(issue.ID) == stickyMergingIssueID(state, []connector.Issue{issue}, now, fairnessAge) {
		return mergeSelectionReasonStickyAged
	}
	if mergeWorkerIssueAged(issue, now, fairnessAge) {
		return mergeSelectionReasonAged
	}
	if mergeWorkerHeadReady(issue) {
		return mergeSelectionReasonClean
	}
	return mergeSelectionReasonQueue
}

func mergeWorkerIssueAged(issue connector.Issue, now time.Time, fairnessAge time.Duration) bool {
	return mergeWorkerIssue(issue) && fairnessAge > 0 && mergeWorkerIssueLaneAge(issue, now) >= fairnessAge
}

func mergeWorkerIssueLaneAge(issue connector.Issue, now time.Time) time.Duration {
	if now.IsZero() {
		return 0
	}
	enteredAt := mergeQueueEnteredAt(issue, time.Time{})
	if enteredAt.IsZero() || now.Before(enteredAt) {
		return 0
	}
	return now.Sub(enteredAt)
}

func mergeWorkerHeadReady(issue connector.Issue) bool {
	if !mergeWorkerIssue(issue) || strings.TrimSpace(issue.ID) == "" || issue.PullRequest == nil {
		return false
	}
	return mergeWorkerProgrammaticMergeReady(issue) &&
		strings.EqualFold(strings.TrimSpace(issue.PullRequest.MergeableState), "clean") &&
		len(issue.PullRequest.RequiredCheckFailures) == 0
}

func (o *Orchestrator) staleMergingQueueDispatchCandidates(state *State, issues []connector.Issue, now time.Time) []connector.Issue {
	o.reconcileMergeReservations(state, issues, now)
	candidates := []connector.Issue{}
	consumedRepositories := activeMergeWorkerRepositories(state)
	for _, issue := range staleMergingQueueIssues(issues, o.cfg, state, now) {
		issueID := strings.TrimSpace(issue.ID)
		repository := mergeWorkerRepositoryKey(issue)
		if staleMergingPullRequestDispatchActive(state, issueID) {
			if reservation := state.mergeReservations[repository]; reservation.IssueID == issueID && reservation.ReleasedReason != "" {
				continue
			}
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		if mergeWorkerRepositoryConsumed(consumedRepositories, repository) {
			continue
		}
		if !staleMergingIssueReadyForDispatch(issue, o.cfg) {
			decision := decideMergeBaseRefresh(issue, true, true)
			if decision.applicable && !decision.proceed {
				o.logMergeBaseRefreshDeferred(issue, decision.reason)
			}
			consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
			continue
		}
		candidates = append(candidates, cloneIssue(issue))
		consumedRepositories = consumeMergeWorkerRepository(consumedRepositories, repository)
	}
	return candidates
}

func staleMergingIssueReadyForDispatch(issue connector.Issue, cfg Config) bool {
	if strings.TrimSpace(issue.ID) == "" || issue.Closed || issue.PullRequest == nil {
		return false
	}
	if _, revoked := mergeApprovalLabelRevoked(issue, cfg); revoked {
		return false
	}
	pullRequest := issue.PullRequest
	if pullRequest.MergeQueueEntry != nil && strings.TrimSpace(pullRequest.MergeQueueEntry.ID) != "" {
		return false
	}
	if pullRequestHydrationBlocksProgress(pullRequest) {
		return false
	}
	if _, revoked := mergeCITriggerLabelRevoked(issue, cfg); revoked {
		return false
	}
	if normalizePullRequestState(pullRequest.State) != "open" {
		return false
	}
	if pullRequest.Draft {
		return false
	}
	if staleMergingCIRed(pullRequest.CIStatus) {
		return false
	}
	decision := decideMergeBaseRefresh(issue, true, true)
	return !decision.applicable || decision.proceed
}

func decideMergeBaseRefresh(issue connector.Issue, laneAvailable bool, globalAvailable bool) mergeBaseRefreshDecision {
	if issue.PullRequest == nil || !strings.EqualFold(strings.TrimSpace(issue.PullRequest.MergeableState), "behind") {
		return mergeBaseRefreshDecision{}
	}
	if len(issue.PullRequest.RequiredCheckFailures) > 0 {
		return mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshRequiredChecksPending}
	}
	if !laneAvailable {
		return mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshLaneUnavailable}
	}
	if !globalAvailable {
		return mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshGlobalUnavailable}
	}
	return mergeBaseRefreshDecision{applicable: true, proceed: true}
}

func (o *Orchestrator) logMergeBaseRefreshDeferred(issue connector.Issue, reason string) {
	if o.logger == nil || issue.PullRequest == nil ||
		!strings.EqualFold(strings.TrimSpace(issue.PullRequest.MergeableState), "behind") {
		return
	}
	o.logger.Info("merge_base_refresh_deferred", mergeWorkerLogAttrs(issue, "reason", strings.TrimSpace(reason))...)
}

func staleMergingCIRed(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "red", "fail", "failed", "failure", "error", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func staleTodoPullRequestAlreadyActive(state *State, issueID string) bool {
	if state == nil {
		return false
	}
	if _, ok := state.Running[issueID]; ok {
		return true
	}
	if _, ok := state.Claimed[issueID]; ok {
		return true
	}
	return false
}

func staleTodoPullRequestDecision(
	issue connector.Issue,
	summary AutoPromoteSummary,
	cfg AutoPromoteConfig,
	now time.Time,
) AutoPromoteDecision {
	if autoPromoteMergeConflicts(summary.MergeableState) {
		return autoPromoteDecision(AutoPromoteActionRework, AutoPromoteReasonMergeConflicts)
	}
	cfg = normalizeAutoPromoteConfig(cfg)
	if gateRequiresPullRequest(cfg.Gate) && len(summary.UnresolvedReviewThreads) > 0 {
		return autoPromoteDecision(AutoPromoteActionRework, AutoPromoteReasonUnresolvedReviewThreads)
	}
	if !cfg.Enabled {
		return autoPromoteDecision(AutoPromoteActionAwaitReview, AutoPromoteReasonDisabled)
	}
	return EvaluateAutoPromote(issue, summary, cfg, now)
}

func staleTodoPullRequestShouldStayActive(decision AutoPromoteDecision) bool {
	return decision.Reason != AutoPromoteReasonWorkpadBlocker
}

func staleTodoPullRequestTargetState(decision AutoPromoteDecision, cfg AutoPromoteConfig) string {
	cfg = normalizeAutoPromoteConfig(cfg)
	if targetState := autoPromoteTargetState(decision.Action, cfg); targetState != "" {
		return targetState
	}
	switch decision.Reason {
	case AutoPromoteReasonMissingPullRequest:
		return ""
	default:
		return cfg.SourceState
	}
}

func staleMergedPullRequestSummaryFromIssue(issue connector.Issue) AutoPromoteSummary {
	summary := AutoPromoteSummary{
		LastActivityAt: autoPromoteLastActivityAt(issue),
		ArtifactStatus: artifactStatusFromIssue(issue, gate.DefaultArtifactStatusField),
	}
	if issue.PullRequest == nil {
		return summary
	}
	pullRequest := issue.PullRequest
	summary.PullRequestPresent = true
	summary.PullRequestURL = strings.TrimSpace(pullRequest.URL)
	summary.PullRequestHydrationUnavailableReason = pullRequestHydrationUnavailableReason(pullRequest)
	summary.PullRequestHydrationDegradedReason = pullRequestHydrationDegradedReason(pullRequest)
	summary.MergeableState = strings.ToLower(strings.TrimSpace(pullRequest.MergeableState))
	summary.CIStatus = strings.TrimSpace(pullRequest.CIStatus)
	summary.ReviewState = pullRequest.CodexReviewState
	summary.FailedChecks = autoPromoteFailedChecksFromPullRequest(pullRequest)
	summary.P1Findings = autoPromoteFindingsFromPullRequest(pullRequest)
	return summary
}

func staleMergedPullRequestDecision(issue connector.Issue, summary AutoPromoteSummary) AutoPromoteDecision {
	if strings.TrimSpace(summary.PullRequestHydrationUnavailableReason) != "" ||
		strings.TrimSpace(summary.PullRequestHydrationDegradedReason) != "" {
		return autoPromoteDecision(AutoPromoteActionSkip, AutoPromoteReasonPullRequestHydrationUnavailable)
	}
	if staleMergedPullRequestHasFailedCIEvidence(issue.PullRequest, summary) {
		decision := autoPromoteDecision(AutoPromoteActionRework, AutoPromoteReasonCINotGreen)
		decision.CIStatus = strings.TrimSpace(summary.CIStatus)
		return decision
	}
	return autoPromoteDecision(AutoPromoteActionPromote, AutoPromoteReasonPullRequestMerged)
}

func staleMergedPullRequestTargetState(decision AutoPromoteDecision, cfg AutoPromoteConfig, terminalStates []string) string {
	cfg = normalizeAutoPromoteConfig(cfg)
	switch decision.Reason {
	case AutoPromoteReasonCINotGreen:
		return cfg.ReworkState
	case AutoPromoteReasonPullRequestMerged:
		return doneStateName(terminalStates)
	case AutoPromoteReasonPullRequestHydrationUnavailable:
		return cfg.SourceState
	default:
		return ""
	}
}

func staleMergedPullRequestHasFailedCIEvidence(pullRequest *connector.PullRequest, summary AutoPromoteSummary) bool {
	if pullRequest == nil {
		return false
	}
	if staleMergingCIRed(pullRequest.CIStatus) {
		return true
	}
	return len(summary.FailedChecks) > 0
}

func (o *Orchestrator) applyStaleMergedPullRequestDecision(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	targetState string,
	now time.Time,
) bool {
	issueID := strings.TrimSpace(issue.ID)
	if err := o.updateIssueStateByID(ctx, state, issueID, issue, targetState, now, string(decision.Reason), laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"stale_merged_pr_reconciliation_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
		return false
	}

	body := staleMergedPullRequestComment(summary, decision, displayStateName(issue.State), targetState)
	if strings.TrimSpace(body) != "" {
		if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
			o.logger.Warn(
				"stale_merged_pr_reconciliation_comment_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
	}

	o.logStaleTodoPullRequestDecision(issue, decision, targetState)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "stale_merged_pr_reconciled",
		Message: "reconciled merged linked PR for " + issueLabel(issue) + " from " + displayStateName(issue.State) + " to " + targetState + ": " + string(decision.Reason),
	})
	return true
}

func staleMergedPullRequestComment(
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	sourceState string,
	targetState string,
) string {
	var b strings.Builder
	sourceState = displayStateName(sourceState)
	if sourceState == "" {
		sourceState = "active"
	}
	switch decision.Reason {
	case AutoPromoteReasonCINotGreen:
		b.WriteString("Reconciled this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(" because its merged linked PR has failing CI evidence.")
	case AutoPromoteReasonPullRequestHydrationUnavailable:
		b.WriteString("Reconciled this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(" because linked PR status hydration is unavailable.")
	case AutoPromoteReasonPullRequestMerged:
		b.WriteString("Reconciled this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(" because its linked PR is already merged.")
	default:
		return ""
	}

	b.WriteString("\n\n")
	b.WriteString("- reason: ")
	b.WriteString(string(decision.Reason))
	if summary.PullRequestURL != "" {
		b.WriteString("\n- pull request: ")
		b.WriteString(summary.PullRequestURL)
	}
	if summary.MergeableState != "" {
		b.WriteString("\n- mergeable_state: ")
		b.WriteString(summary.MergeableState)
	}
	if decision.CIStatus != "" {
		b.WriteString("\n- ci_status: ")
		b.WriteString(decision.CIStatus)
	}
	if failedChecks := strings.Join(summary.FailedChecks, ", "); failedChecks != "" {
		b.WriteString("\n- failed_checks: ")
		b.WriteString(failedChecks)
	}
	if summary.PullRequestHydrationUnavailableReason != "" {
		b.WriteString("\n- pull_request_hydration_unavailable_reason: ")
		b.WriteString(summary.PullRequestHydrationUnavailableReason)
	}
	if summary.PullRequestHydrationDegradedReason != "" {
		b.WriteString("\n- pull_request_hydration_degraded_reason: ")
		b.WriteString(summary.PullRequestHydrationDegradedReason)
	}
	return b.String()
}

func (o *Orchestrator) applyStaleTodoPullRequestDecision(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	targetState string,
	now time.Time,
) bool {
	issueID := strings.TrimSpace(issue.ID)
	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issueID, issue, targetState, now, string(decision.Reason), workflowLaneMetadata{Reconciliation: "stale_todo_pr"}, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"stale_todo_pr_reconciliation_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
		return false
	}

	body := autoPromoteComment(summary, decision, displayStateName(issue.State), targetState)
	if strings.TrimSpace(body) != "" {
		if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
			o.logger.Warn(
				"stale_todo_pr_reconciliation_comment_failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
	}

	o.logStaleTodoPullRequestDecision(issue, decision, targetState)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "stale_todo_pr_reconciled",
		Message: "reconciled stale linked PR for " + issueLabel(issue) + " from " + displayStateName(issue.State) + " to " + targetState + ": " + string(decision.Reason),
	})
	return true
}

func (o *Orchestrator) logStaleTodoPullRequestDecision(issue connector.Issue, decision AutoPromoteDecision, targetState string) {
	if o.logger == nil {
		return
	}
	attrs := []any{
		"pull_request_association_source", issue.PRSource,
		"pull_request_association_checked_at", issue.PRVerifiedAt,
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", issue.Identifier,
		"reason", decision.Reason,
		"target_state", targetState,
	}
	if issue.PullRequest != nil {
		if issue.PullRequest.Number > 0 {
			attrs = append(attrs, "pull_request_number", issue.PullRequest.Number)
		}
		if mergeableState := strings.TrimSpace(issue.PullRequest.MergeableState); mergeableState != "" {
			attrs = append(attrs, "mergeable_state", strings.ToLower(mergeableState))
		}
		if ciStatus := strings.TrimSpace(issue.PullRequest.CIStatus); ciStatus != "" {
			attrs = append(attrs, "ci_status", ciStatus)
		}
		if failedChecks := strings.Join(autoPromoteFailedChecksFromPullRequest(issue.PullRequest), ", "); failedChecks != "" {
			attrs = append(attrs, "failed_checks", failedChecks)
		}
		if reason := pullRequestHydrationUnavailableReason(issue.PullRequest); reason != "" {
			attrs = append(attrs, "pull_request_hydration_unavailable_reason", reason)
		}
		if reason := pullRequestHydrationDegradedReason(issue.PullRequest); reason != "" {
			attrs = append(attrs, "pull_request_hydration_degraded_reason", reason)
		}
	}
	if decision.WorkpadBlocker != "" {
		attrs = append(attrs, "workpad_blocker", decision.WorkpadBlocker)
	}
	if len(decision.ResolvedWorkpadBlockers) > 0 {
		attrs = append(attrs, "resolved_workpad_blockers", strings.Join(decision.ResolvedWorkpadBlockers, ","))
	}
	attrs = appendAutoPromoteWorkpadAttrs(attrs, decision)
	o.logger.Info("stale_todo_pr_reconciled", attrs...)
}

func (o *Orchestrator) clearAutoPromotedIssueDispatchMemory(state *State, issueID string) {
	if state == nil {
		return
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.Blocked, issueID)
	delete(state.Completed, issueID)
	delete(state.AutoPromoteDecisions, issueID)
}

func (o *Orchestrator) startValidatorStage(ctx context.Context, state *State, issue connector.Issue, now time.Time) {
	identity := validatorStageIdentityForIssue(issue)
	if identity.Key == "" {
		if o.logger != nil {
			o.logger.Error(
				"validator stage identity unavailable",
				"issue_id", strings.TrimSpace(issue.ID),
				"identifier", issue.Identifier,
				"pull_request", pullRequestNumber(issue),
			)
		}
		return
	}
	validatorConfig := gate.Effective(o.cfg.AutoPromote.Gate).Validator
	if o.validator == nil {
		failureErr := errors.New("validator runner unavailable")
		result := validatorFailureResult(failureErr, validatorConfig.MaxAttempts, validatorConfig.MaxAttempts)
		result.Summary = "validator review production could not start: " + failureErr.Error()
		result.Findings[0].Body = result.Summary
		o.validatorMu.Lock()
		if o.validatorResults == nil {
			o.validatorResults = map[string]validatorStageResult{}
		}
		o.validatorResults[identity.Key] = validatorStageResult{Result: result}
		o.validatorMu.Unlock()
		o.recordValidatorStageOutcome(ctx, issue, identity, result, validatorConfig.MaxAttempts, nil, now.UTC())
		if o.logger != nil {
			o.logger.Error(
				"validator stage unavailable",
				"issue_id", identity.IssueID,
				"identifier", issue.Identifier,
				"pull_request", pullRequestNumber(issue),
				"head_sha", identity.HeadSHA,
				"error", failureErr,
			)
		}
		return
	}
	if _, _, ok := o.validatorStageResult(ctx, issue); ok {
		return
	}
	capacityScope, capacityProbeKey, capacityPaused := o.validatorCapacityDispatch(state, issue, now)
	if capacityPaused {
		return
	}

	o.validatorMu.Lock()
	if o.validatorRuns == nil {
		o.validatorRuns = map[string]struct{}{}
	}
	if o.validatorResults == nil {
		o.validatorResults = map[string]validatorStageResult{}
	}
	if o.validatorFailures == nil {
		o.validatorFailures = map[string]validatorStageFailure{}
	}
	if _, ok := o.validatorRuns[identity.Key]; ok {
		o.validatorMu.Unlock()
		return
	}
	if _, ok := o.validatorResults[identity.Key]; ok {
		o.validatorMu.Unlock()
		return
	}
	if failure, ok := o.validatorFailures[identity.Key]; ok && failure.NextRetryAt.After(now) {
		o.validatorMu.Unlock()
		if o.logger != nil {
			o.logger.Debug(
				"validator stage backoff active",
				"issue_id", identity.IssueID,
				"identifier", issue.Identifier,
				"head_sha", identity.HeadSHA,
				"retry_at", failure.NextRetryAt,
				"attempt", failure.Attempt,
			)
		}
		return
	}
	o.validatorRuns[identity.Key] = struct{}{}
	if capacityProbeKey != "" {
		o.markBackendCapacityProbe(state, capacityProbeKey, "validator:"+identity.IssueID, now)
	}
	o.validatorWG.Add(1)
	o.validatorMu.Unlock()

	selectorContext := o.cfg.SelectorContext
	retryConfig := Config{
		MaxRetryBackoff:       o.cfg.MaxRetryBackoff,
		FailureRetryBaseDelay: o.cfg.FailureRetryBaseDelay,
	}
	go func() {
		defer o.validatorWG.Done()

		result, err := o.validator.Validate(ctx, ValidatorRequest{
			Issue:            issue,
			StartedAt:        now.UTC(),
			SelectorContext:  selectorContext,
			OnActivityUpdate: o.activityUpdateHandler(ctx, issue),
		})

		completedAt := o.clockNow().UTC()
		o.validatorMu.Lock()
		if err != nil {
			delete(o.validatorRuns, identity.Key)
			if capacityErr, ok := backendcapacity.As(err); ok {
				if capacityErr.Details.Type == backendcapacity.ErrorTypeTransientOverload {
					failure := o.validatorFailures[identity.Key]
					failure.NextRetryAt = completedAt.Add(o.cfg.OverloadRetryDelay)
					failure.Error = string(backendcapacity.ErrorTypeTransientOverload)
					o.validatorFailures[identity.Key] = failure
					o.validatorMu.Unlock()
					if o.logger != nil {
						o.logger.Info(
							"validator transient overload retry scheduled",
							"reason", backendcapacity.ErrorTypeTransientOverload,
							"issue_id", identity.IssueID,
							"retry_at", failure.NextRetryAt,
							"error", err,
						)
					}
					o.publishValidatorCapacityEvent(ctx, validatorCapacityEvent{
						Scope:         capacityErr.Scope,
						CapacityErr:   capacityErr,
						CapacityProbe: capacityProbeKey != "",
						CompletedAt:   completedAt,
					})
					return
				}
				o.validatorMu.Unlock()
				o.publishValidatorCapacityEvent(ctx, validatorCapacityEvent{
					Scope:         capacityErr.Scope,
					CapacityErr:   capacityErr,
					CapacityProbe: capacityProbeKey != "",
					CompletedAt:   completedAt,
				})
				return
			}
			attempt := o.validatorFailures[identity.Key].Attempt + 1
			retryAt := completedAt.Add(validatorStageRetryDelay(retryConfig, attempt))
			failure := validatorStageFailure{Attempt: attempt, NextRetryAt: retryAt, Error: err.Error()}
			exhausted := attempt >= validatorConfig.MaxAttempts
			failureResult := validatorFailureResult(err, attempt, validatorConfig.MaxAttempts)
			if exhausted {
				delete(o.validatorFailures, identity.Key)
				o.validatorResults[identity.Key] = validatorStageResult{Result: failureResult}
			} else {
				o.validatorFailures[identity.Key] = failure
			}
			o.validatorMu.Unlock()
			var nextRetryAt *time.Time
			if !exhausted {
				nextRetryAt = &retryAt
			}
			o.recordValidatorStageOutcome(ctx, issue, identity, failureResult, attempt, nextRetryAt, completedAt)
			if o.logger != nil {
				attrs := []any{
					"issue_id", strings.TrimSpace(issue.ID),
					"identifier", issue.Identifier,
					"pull_request", pullRequestNumber(issue),
					"head_sha", identity.HeadSHA,
					"attempt", attempt,
					"max_attempts", validatorConfig.MaxAttempts,
					"error", err,
				}
				if exhausted {
					o.logger.Error("validator stage retries exhausted", attrs...)
				} else {
					attrs = append(attrs, "retry_at", failure.NextRetryAt)
					o.logger.Warn("validator stage failed; retry scheduled", attrs...)
				}
			}
			if capacityProbeKey != "" {
				o.publishValidatorCapacityEvent(ctx, validatorCapacityEvent{
					Scope:         capacityScope,
					ProbeErr:      err,
					CapacityProbe: true,
					CompletedAt:   completedAt,
				})
			}
			return
		}
		o.validatorMu.Unlock()
		if capacityProbeKey != "" {
			o.publishValidatorCapacityEvent(ctx, validatorCapacityEvent{
				Scope:         capacityScope,
				CapacityProbe: true,
				CompletedAt:   completedAt,
			})
		}
		o.recordValidatorVerdict(ctx, issue, identity, result, completedAt)

		o.validatorMu.Lock()
		delete(o.validatorRuns, identity.Key)
		delete(o.validatorFailures, identity.Key)
		o.validatorResults[identity.Key] = validatorStageResult{Result: result}
		o.validatorMu.Unlock()
	}()
}

func (o *Orchestrator) validatorStageResult(ctx context.Context, issue connector.Issue) (gate.ValidatorResult, bool, bool) {
	identity := validatorStageIdentityForIssue(issue)
	if identity.Key == "" {
		return gate.ValidatorResult{}, false, false
	}
	o.validatorMu.Lock()
	result, ok := o.validatorResults[identity.Key]
	o.validatorMu.Unlock()
	if !ok {
		var loaded bool
		result, loaded = o.loadValidatorVerdict(ctx, issue, identity)
		if !loaded {
			return gate.ValidatorResult{}, false, false
		}
		o.validatorMu.Lock()
		if o.validatorResults == nil {
			o.validatorResults = map[string]validatorStageResult{}
		}
		o.validatorResults[identity.Key] = result
		o.validatorMu.Unlock()
	}
	return result.Result, !result.Commented, true
}

func (o *Orchestrator) markValidatorResultCommented(ctx context.Context, issue connector.Issue) {
	identity := validatorStageIdentityForIssue(issue)
	if identity.Key == "" {
		return
	}
	o.validatorMu.Lock()
	result, ok := o.validatorResults[identity.Key]
	if !ok {
		o.validatorMu.Unlock()
		return
	}
	result.Commented = true
	o.validatorResults[identity.Key] = result
	o.validatorMu.Unlock()
	o.markValidatorVerdictCommented(ctx, identity)
}

func (o *Orchestrator) commentValidatorResult(ctx context.Context, issue connector.Issue, result gate.ValidatorResult) {
	commenter, ok := o.connector.(connector.PullRequestCommenter)
	if !ok {
		return
	}
	repository := pullRequestRepository(issue)
	number := pullRequestNumber(issue)
	if repository == "" || number <= 0 {
		return
	}
	if err := commenter.CreatePullRequestComment(ctx, repository, number, validatorResultComment(result)); err != nil && o.logger != nil {
		o.logger.Warn(
			"validator result comment failed",
			"issue_id", strings.TrimSpace(issue.ID),
			"identifier", issue.Identifier,
			"pull_request", number,
			"error", err,
		)
	}
}

func validatorResultComment(result gate.ValidatorResult) string {
	var b strings.Builder
	b.WriteString("Validator verdict: ")
	b.WriteString(strings.TrimSpace(result.Verdict))
	if result.Score > 0 {
		b.WriteString("\n- score: ")
		b.WriteString(fmt.Sprintf("%.2f", result.Score))
	}
	if strings.TrimSpace(result.Summary) != "" {
		b.WriteString("\n- summary: ")
		b.WriteString(strings.TrimSpace(result.Summary))
	}
	if len(result.Findings) > 0 {
		b.WriteString("\n\nFindings:")
		for _, finding := range result.Findings {
			b.WriteString("\n- ")
			b.WriteString(autoPromoteFindingText(AutoPromoteFinding{
				Body: finding.Body,
				URL:  finding.URL,
				Path: finding.Path,
				Line: finding.Line,
			}))
		}
	}
	return b.String()
}

func pullRequestRepository(issue connector.Issue) string {
	if strings.TrimSpace(issue.PRRepository) != "" {
		return strings.TrimSpace(issue.PRRepository)
	}
	identifier := strings.TrimSpace(issue.Identifier)
	repository, _, ok := strings.Cut(identifier, "#")
	if ok {
		return strings.TrimSpace(repository)
	}
	return ""
}

func pullRequestNumber(issue connector.Issue) int {
	if issue.PullRequest != nil && issue.PullRequest.Number > 0 {
		return issue.PullRequest.Number
	}
	if issue.PRNumber != nil {
		return *issue.PRNumber
	}
	return 0
}

type validatorStageIdentity struct {
	Key     string
	IssueID string
	HeadSHA string
}

func validatorStageIdentityForIssue(issue connector.Issue) validatorStageIdentity {
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return validatorStageIdentity{}
	}
	headSHA := ""
	if issue.PullRequest != nil {
		headSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
	}
	if headSHA == "" && issue.PullRequest != nil {
		headSHA = strings.TrimSpace(issue.PullRequest.BranchName)
	}
	if headSHA == "" {
		headSHA = strings.TrimSpace(issue.BranchName)
	}
	if headSHA == "" {
		return validatorStageIdentity{}
	}
	return validatorStageIdentity{
		Key:     issueID + ":" + headSHA,
		IssueID: issueID,
		HeadSHA: headSHA,
	}
}

func validatorStageRetryDelay(cfg Config, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	maxRetryBackoff := cfg.MaxRetryBackoff
	if maxRetryBackoff <= 0 {
		maxRetryBackoff = defaultMaxRetryBackoff
	}
	failureRetryBaseDelay := cfg.FailureRetryBaseDelay
	if failureRetryBaseDelay <= 0 {
		failureRetryBaseDelay = defaultFailureRetryBaseDelay
	}
	delay := failureRetryBaseDelay
	for range attempt - 1 {
		if delay >= maxRetryBackoff || delay > maxRetryBackoff/2 {
			return maxRetryBackoff
		}
		delay *= 2
	}
	if delay > maxRetryBackoff {
		return maxRetryBackoff
	}
	return delay
}

func (o *Orchestrator) loadValidatorVerdict(ctx context.Context, issue connector.Issue, identity validatorStageIdentity) (validatorStageResult, bool) {
	if o.validatorMemo == nil {
		return validatorStageResult{}, false
	}
	verdict, err := o.validatorMemo.ValidatorVerdict(ctx, store.ValidatorVerdictKey{
		ProjectID: o.workflowMetricsProjectID(),
		IssueID:   identity.IssueID,
		HeadSHA:   identity.HeadSHA,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return validatorStageResult{}, false
		}
		if o.logger != nil {
			o.logger.Warn(
				"validator verdict lookup failed",
				"issue_id", identity.IssueID,
				"identifier", issue.Identifier,
				"head_sha", identity.HeadSHA,
				"error", err,
			)
		}
		return validatorStageResult{}, false
	}
	validatorConfig := gate.Effective(o.cfg.AutoPromote.Gate).Validator
	if !verdict.Submitted && strings.EqualFold(strings.TrimSpace(verdict.Verdict), gate.ValidatorVerdictError) && verdict.FailureAttempts < validatorConfig.MaxAttempts {
		nextRetryAt := o.clockNow().UTC()
		if verdict.NextRetryAt != nil {
			nextRetryAt = verdict.NextRetryAt.UTC()
		}
		o.validatorMu.Lock()
		if o.validatorFailures == nil {
			o.validatorFailures = map[string]validatorStageFailure{}
		}
		failure := o.validatorFailures[identity.Key]
		if verdict.FailureAttempts > failure.Attempt {
			o.validatorFailures[identity.Key] = validatorStageFailure{
				Attempt:     verdict.FailureAttempts,
				NextRetryAt: nextRetryAt,
				Error:       verdict.Summary,
			}
		}
		o.validatorMu.Unlock()
		return validatorStageResult{}, false
	}
	return validatorStageResult{
		Result: gate.ValidatorResult{
			Submitted: verdict.Submitted,
			Verdict:   verdict.Verdict,
			Score:     verdict.Score,
			Summary:   verdict.Summary,
			Findings:  gateFindingsFromStore(verdict.Findings),
		},
		Commented: verdict.Commented,
	}, true
}

func (o *Orchestrator) recordValidatorVerdict(
	ctx context.Context,
	issue connector.Issue,
	identity validatorStageIdentity,
	result gate.ValidatorResult,
	recordedAt time.Time,
) {
	o.recordValidatorStageOutcome(ctx, issue, identity, result, 0, nil, recordedAt)
}

func (o *Orchestrator) recordValidatorStageOutcome(
	ctx context.Context,
	issue connector.Issue,
	identity validatorStageIdentity,
	result gate.ValidatorResult,
	failureAttempts int,
	nextRetryAt *time.Time,
	recordedAt time.Time,
) {
	if o.validatorMemo == nil {
		return
	}
	if recordedAt.IsZero() {
		recordedAt = o.clockNow().UTC()
	}
	if err := o.validatorMemo.RecordValidatorVerdict(ctx, store.ValidatorVerdict{
		ProjectID:       o.workflowMetricsProjectID(),
		IssueID:         identity.IssueID,
		HeadSHA:         identity.HeadSHA,
		Identifier:      issue.Identifier,
		IssueURL:        issue.URL,
		PRNumber:        workflowMetricsPRNumber(issue),
		Submitted:       result.Submitted,
		Verdict:         result.Verdict,
		Score:           result.Score,
		Summary:         result.Summary,
		Findings:        storeFindingsFromGate(result.Findings),
		FailureAttempts: failureAttempts,
		NextRetryAt:     nextRetryAt,
		RecordedAt:      recordedAt,
		UpdatedAt:       recordedAt,
	}); err != nil && o.logger != nil {
		o.logger.Warn(
			"validator verdict persistence failed",
			"issue_id", identity.IssueID,
			"identifier", issue.Identifier,
			"head_sha", identity.HeadSHA,
			"error", err,
		)
	}
}

func validatorFailureResult(err error, attempt int, maxAttempts int) gate.ValidatorResult {
	if attempt < 1 {
		attempt = 1
	}
	if maxAttempts < 1 {
		maxAttempts = gate.DefaultValidatorMaxAttempts
	}
	summary := fmt.Sprintf("validator review production attempt %d/%d failed: %v", attempt, maxAttempts, err)
	if attempt >= maxAttempts {
		summary = fmt.Sprintf("validator review production failed after %d attempts: %v", attempt, err)
	}
	return gate.ValidatorResult{
		Verdict: gate.ValidatorVerdictError,
		Summary: summary,
		Findings: []gate.Finding{{
			Severity: "p1",
			Body:     summary,
		}},
	}
}

func (o *Orchestrator) markValidatorVerdictCommented(ctx context.Context, identity validatorStageIdentity) {
	if o.validatorMemo == nil {
		return
	}
	if err := o.validatorMemo.MarkValidatorVerdictCommented(ctx, store.ValidatorVerdictKey{
		ProjectID: o.workflowMetricsProjectID(),
		IssueID:   identity.IssueID,
		HeadSHA:   identity.HeadSHA,
	}, o.clockNow().UTC()); err != nil && !errors.Is(err, store.ErrNotFound) && o.logger != nil {
		o.logger.Warn(
			"validator verdict comment marker failed",
			"issue_id", identity.IssueID,
			"head_sha", identity.HeadSHA,
			"error", err,
		)
	}
}

func (o *Orchestrator) clockNow() time.Time {
	if o != nil && o.now != nil {
		return o.now()
	}
	return time.Now()
}

func storeFindingsFromGate(findings []gate.Finding) []store.ValidatorFinding {
	out := make([]store.ValidatorFinding, 0, len(findings))
	for _, finding := range findings {
		out = append(out, store.ValidatorFinding{
			Severity: finding.Severity,
			Body:     finding.Body,
			URL:      finding.URL,
			Path:     finding.Path,
			Line:     finding.Line,
		})
	}
	return out
}

func gateFindingsFromStore(findings []store.ValidatorFinding) []gate.Finding {
	out := make([]gate.Finding, 0, len(findings))
	for _, finding := range findings {
		out = append(out, gate.Finding{
			Severity: finding.Severity,
			Body:     finding.Body,
			URL:      finding.URL,
			Path:     finding.Path,
			Line:     finding.Line,
		})
	}
	return out
}

func AutoPromoteSummaryFromIssue(issue connector.Issue) AutoPromoteSummary {
	summary := AutoPromoteSummary{
		LastActivityAt: autoPromoteLastActivityAt(issue),
		ArtifactStatus: artifactStatusFromIssue(issue, gate.DefaultArtifactStatusField),
	}
	if issue.PullRequest == nil {
		return summary
	}

	pullRequest := issue.PullRequest
	summary.PullRequestHydrationUnavailableReason = pullRequestHydrationUnavailableReason(pullRequest)
	summary.PullRequestHydrationDegradedReason = pullRequestHydrationDegradedReason(pullRequest)
	if normalizePullRequestState(pullRequest.State) != "open" {
		return summary
	}
	summary.PullRequestPresent = true
	summary.PullRequestURL = strings.TrimSpace(pullRequest.URL)
	summary.MergeableState = strings.ToLower(strings.TrimSpace(pullRequest.MergeableState))
	summary.CIStatus = pullRequest.CIStatus
	summary.ReviewState = pullRequest.CodexReviewState
	summary.FailedChecks = autoPromoteFailedChecksFromPullRequest(pullRequest)
	summary.UnresolvedReviewThreads = append([]connector.PullRequestReviewThread(nil), pullRequest.UnresolvedReviewThreads...)
	summary.P1Findings = autoPromoteFindingsFromPullRequest(pullRequest)
	return summary
}

func (o *Orchestrator) hydrateAutoPromoteReviewThreads(ctx context.Context, issue connector.Issue) (connector.Issue, bool) {
	hydrator, ok := o.connector.(connector.PullRequestReviewThreadHydrator)
	if !ok || issue.PullRequest == nil || normalizePullRequestState(issue.PullRequest.State) != "open" {
		return issue, true
	}
	hydrated, err := hydrator.HydratePullRequestReviewThreads(ctx, issue)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"pull request review thread hydration failed",
				"issue_id", strings.TrimSpace(issue.ID),
				"identifier", issue.Identifier,
				"pull_request", pullRequestNumber(issue),
				"error", err,
			)
		}
		return issue, false
	}
	if hydrated.PullRequest != nil && pullRequestHydrationUnavailableReason(hydrated.PullRequest) != "" {
		o.logAutoPromoteDecision(
			hydrated,
			autoPromoteDecision(AutoPromoteActionSkip, AutoPromoteReasonPullRequestHydrationUnavailable),
			"",
		)
		return hydrated, false
	}
	return hydrated, true
}

func pullRequestHydrationUnavailableReason(pullRequest *connector.PullRequest) string {
	if pullRequest == nil {
		return ""
	}
	return strings.TrimSpace(pullRequest.HydrationUnavailableReason)
}

func pullRequestHydrationDegradedReason(pullRequest *connector.PullRequest) string {
	if pullRequest == nil {
		return ""
	}
	return strings.TrimSpace(pullRequest.HydrationDegradedReason)
}

func pullRequestHydrationBlocksProgress(pullRequest *connector.PullRequest) bool {
	return pullRequestHydrationUnavailableReason(pullRequest) != "" ||
		pullRequestHydrationDegradedReason(pullRequest) != ""
}

func autoPromoteLastActivityAt(issue connector.Issue) *time.Time {
	var latest *time.Time
	latest = latestTime(latest, issue.StageUpdatedAt)
	latest = latestTime(latest, issue.UpdatedAt)
	if issue.PullRequest != nil {
		latest = latestTime(latest, issue.PullRequest.ActivityAt)
		latest = latestTime(latest, issue.PullRequest.CodexReviewSubmittedAt)
	}
	return latest
}

func latestTime(current *time.Time, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		value := *candidate
		return &value
	}
	return current
}

func autoPromoteFindingsFromPullRequest(pullRequest *connector.PullRequest) []AutoPromoteFinding {
	if pullRequest == nil {
		return nil
	}
	findings := make([]AutoPromoteFinding, 0, len(pullRequest.CodexReviewFindings))
	for _, finding := range pullRequest.CodexReviewFindings {
		findings = append(findings, AutoPromoteFinding{
			Body: finding.Body,
			URL:  finding.URL,
			Path: finding.Path,
			Line: finding.Line,
		})
	}
	if len(findings) == 0 && strings.EqualFold(strings.TrimSpace(pullRequest.CodexReviewState), "P1") {
		findings = append(findings, AutoPromoteFinding{
			Body: "Codex review reported P1 findings.",
			URL:  strings.TrimSpace(pullRequest.URL),
		})
	}
	return findings
}

func autoPromoteFailedChecksFromPullRequest(pullRequest *connector.PullRequest) []string {
	if pullRequest == nil {
		return nil
	}
	allChecks := append([]connector.PullRequestCheck{}, pullRequest.SlowChecks...)
	allChecks = append(allChecks, pullRequest.RequiredCheckFailures...)
	allChecks = append(allChecks, pullRequest.TransientFailedChecks...)
	checks := make([]string, 0, len(allChecks))
	for _, check := range allChecks {
		if !autoPromoteCheckFailed(check) {
			continue
		}
		checks = append(checks, check.Name)
	}
	return uniqueStrings(checks)
}

func autoPromotePendingChecksFromPullRequest(pullRequest *connector.PullRequest) []string {
	if pullRequest == nil {
		return nil
	}
	checks := append([]string(nil), pullRequest.RunningChecks...)
	checks = append(checks, pullRequestCheckNames(pullRequest.UnstartedChecks)...)
	for _, check := range pullRequest.RequiredCheckFailures {
		if !autoPromoteCheckPending(check) {
			continue
		}
		checks = append(checks, check.Name)
	}
	return uniqueStrings(checks)
}

func pullRequestCheckNames(checks []connector.PullRequestCheck) []string {
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		names = append(names, check.Name)
	}
	return uniqueStrings(names)
}

func autoPromoteCheckPending(check connector.PullRequestCheck) bool {
	status := strings.ToLower(strings.TrimSpace(check.Status))
	conclusion := strings.ToLower(strings.TrimSpace(check.Conclusion))
	if conclusion == "missing" {
		return true
	}
	switch status {
	case "missing", "pending", "queued", "waiting", "in_progress", "in progress", "requested", "expected":
		return true
	default:
		return false
	}
}

func autoPromoteStaleSuccessfulChecks(pullRequest *connector.PullRequest) []string {
	if pullRequest == nil {
		return nil
	}
	checks := make([]string, 0, len(pullRequest.StaleSuccessfulChecks))
	for _, check := range pullRequest.StaleSuccessfulChecks {
		if name := strings.TrimSpace(check.Name); name != "" {
			checks = append(checks, name)
		}
	}
	return uniqueStrings(checks)
}

func autoPromoteCheckFailed(check connector.PullRequestCheck) bool {
	switch strings.ToLower(strings.TrimSpace(check.Conclusion)) {
	case "failure", "failed", "error", "timed_out", "startup_failure", "action_required", "missing", "neutral":
		return true
	default:
		return false
	}
}

func autoPromoteTargetState(action AutoPromoteAction, cfg AutoPromoteConfig) string {
	cfg = normalizeAutoPromoteConfig(cfg)
	switch action {
	case AutoPromoteActionComplete:
		return doneStateName(cfg.TerminalStates)
	case AutoPromoteActionPromote:
		return cfg.PassState
	case AutoPromoteActionRework:
		return cfg.ReworkState
	default:
		return ""
	}
}

func promotedIssue(issue connector.Issue, targetState string, now time.Time) connector.Issue {
	promoted := cloneIssue(issue)
	promoted.State = targetState
	promotedAt := now.UTC()
	promoted.StageUpdatedAt = &promotedAt
	return promoted
}

func (o *Orchestrator) applyAutoPromoteDecision(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	targetState string,
	now time.Time,
) bool {
	_, applied := o.applyAutoPromoteDecisionWithTarget(ctx, state, issue, summary, decision, targetState, now)
	return applied
}

func (o *Orchestrator) applyAutoPromoteDecisionWithTarget(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	targetState string,
	now time.Time,
) (string, bool) {
	if normalizeState(issue.State) == normalizeState(targetState) {
		return "", false
	}

	issueID := strings.TrimSpace(issue.ID)
	transitionReason := string(decision.Reason)
	disposition := laneMutationPreserveOwnership
	body := autoPromoteComment(summary, decision, displayStateName(issue.State), targetState)
	metadata := workflowLaneMetadata{}
	if decision.Action == AutoPromoteActionRework {
		limit, err := o.autoPromoteReworkLimit(ctx, issue, summary)
		if err != nil {
			if o.logger != nil {
				o.logger.Warn(
					"auto promote rework limit check failed",
					"issue_id", issueID,
					"identifier", issue.Identifier,
					"action", decision.Action,
					"reason", decision.Reason,
					"target_state", targetState,
					"error", err,
				)
			}
			return "", false
		}
		if limit.Exceeded() {
			disposition = laneMutationRevokeWorker
			targetState = blockedStatusState
			transitionReason = "rework_limit"
			body = autoPromoteReworkLimitComment(summary, decision, displayStateName(issue.State), limit)
			metadata.ReworkBreaker = &workflowLaneReworkBreakerMetadata{Reason: string(decision.Reason)}
			recovery := o.newBlockedRecoveryMetadata(
				ctx,
				issue,
				RunModeImplement,
				"rework_limit",
				blockedRecoveryPredicateManaged,
				autoPromoteReworkState,
				DiffStats{},
			)
			metadata.BlockedRecovery = recovery.BlockedRecovery
		}
	}

	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issueID, issue, targetState, now, transitionReason, metadata, disposition); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"auto promote transition failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"action", decision.Action,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
		return "", false
	}

	if strings.TrimSpace(body) != "" {
		if err := o.connector.CreateComment(ctx, issueID, body); err != nil && o.logger != nil {
			o.logger.Warn(
				"auto promote comment failed",
				"issue_id", issueID,
				"identifier", issue.Identifier,
				"action", decision.Action,
				"reason", decision.Reason,
				"target_state", targetState,
				"error", err,
			)
		}
	}

	o.logAutoPromoteDecision(issue, decision, targetState)
	sourceState := displayStateName(issue.State)
	if sourceState == "" {
		sourceState = autoPromoteSourceState
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "auto_promote_transition",
		Message: "auto-promoted " + issueLabel(issue) + " from " + sourceState + " to " + targetState,
	})
	return targetState, true
}

func (s autoPromoteReworkLimitSummary) Exceeded() bool {
	return s.Limit > 0 && s.Count >= s.Limit
}

func (o *Orchestrator) autoPromoteReworkLimit(
	ctx context.Context,
	issue connector.Issue,
	summary AutoPromoteSummary,
) (autoPromoteReworkLimitSummary, error) {
	cfg := normalizeAutoPromoteConfig(o.cfg.AutoPromote)
	limitSummary := autoPromoteReworkLimitSummary{
		Limit:     cfg.ReworkLimit,
		Signature: autoPromoteReworkSignatureFromIssue(issue, summary),
	}
	if cfg.ReworkLimit <= 0 {
		return limitSummary, nil
	}
	if normalizeState(issue.State) == normalizeState(cfg.ReworkState) {
		return limitSummary, nil
	}
	reader, ok := o.workflowMetrics.(WorkflowMetricsTimelineReader)
	if !ok || reader == nil {
		return limitSummary, errors.New("workflow metrics timeline reader unavailable")
	}

	timeline, err := reader.IssueWorkflowTimeline(ctx, store.IssueIdentity{
		ProjectID:  o.workflowMetricsProjectID(),
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		IssueURL:   issue.URL,
	})
	if err != nil {
		return limitSummary, err
	}
	entries := autoPromoteReworkLaneEntries(timeline.Events, cfg.ReworkState)
	entries = autoPromoteReworkLaneEntriesForSignature(entries, limitSummary.Signature)
	limitSummary.Count = len(entries)
	limitSummary.ReasonCounts = autoPromoteReworkReasonCounts(entries)
	return limitSummary, nil
}

func autoPromoteReworkLaneEntries(events []store.WorkflowPhaseEvent, reworkState string) []store.WorkflowPhaseEvent {
	reworkState = normalizeState(reworkState)
	entries := make([]store.WorkflowPhaseEvent, 0, len(events))
	currentLane := ""
	for _, event := range events {
		if event.PhaseType != store.WorkflowPhaseTypeLane {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(event.Status), "entered") {
			continue
		}
		lane := normalizeState(event.PhaseName)
		previousLane := normalizeState(event.PreviousPhaseName)
		if previousLane == "" {
			previousLane = currentLane
		}
		if lane == reworkState && previousLane != reworkState {
			entries = append(entries, event)
		}
		currentLane = lane
	}
	return entries
}

func autoPromoteReworkReasonCounts(events []store.WorkflowPhaseEvent) []autoPromoteReworkReasonCount {
	counts := map[string]int{}
	order := make([]string, 0, len(events))
	for _, event := range events {
		reason := strings.TrimSpace(event.Reason)
		if reason == "" {
			reason = "state_transition"
		}
		if _, ok := counts[reason]; !ok {
			order = append(order, reason)
		}
		counts[reason]++
	}

	out := make([]autoPromoteReworkReasonCount, 0, len(order))
	for _, reason := range order {
		out = append(out, autoPromoteReworkReasonCount{Reason: reason, Count: counts[reason]})
	}
	return out
}

func autoPromoteReworkLaneEntriesForSignature(
	events []store.WorkflowPhaseEvent,
	signature autoPromoteReworkSignature,
) []store.WorkflowPhaseEvent {
	if signature.empty() {
		return events
	}
	matching := make([]store.WorkflowPhaseEvent, 0, len(events))
	for _, event := range events {
		if autoPromoteReworkSignatureMatches(signature, autoPromoteReworkSignatureFromEvent(event)) {
			matching = append(matching, event)
		}
	}
	return matching
}

func autoPromoteReworkSignatureFromIssue(issue connector.Issue, summary AutoPromoteSummary) autoPromoteReworkSignature {
	signature := autoPromoteReworkSignature{}
	if issue.PRNumber != nil && *issue.PRNumber > 0 {
		signature.PRNumber = int64(*issue.PRNumber)
	}
	if issue.PullRequest != nil {
		if issue.PullRequest.Number > 0 {
			signature.PRNumber = int64(issue.PullRequest.Number)
		}
		signature.HeadSHA = strings.TrimSpace(issue.PullRequest.HeadSHA)
	}
	signature.FailedChecks = autoPromoteCanonicalChecks(summary.FailedChecks)
	if len(signature.FailedChecks) == 0 && issue.PullRequest != nil {
		signature.FailedChecks = autoPromoteCanonicalChecks(autoPromoteFailedChecksFromPullRequest(issue.PullRequest))
	}
	return signature
}

func autoPromoteReworkSignatureFromEvent(event store.WorkflowPhaseEvent) autoPromoteReworkSignature {
	signature := autoPromoteReworkSignature{}
	if event.PRNumber != nil && *event.PRNumber > 0 {
		signature.PRNumber = *event.PRNumber
	}
	if metadata, ok := workflowLaneMetadataFromJSON(event.MetadataJSON); ok {
		if metadata.PullRequest != nil {
			if metadata.PullRequest.Number > 0 {
				signature.PRNumber = metadata.PullRequest.Number
			}
			signature.HeadSHA = strings.TrimSpace(metadata.PullRequest.HeadSHA)
			signature.FailedChecks = autoPromoteCanonicalChecks(metadata.PullRequest.FailedChecks)
		}
	}
	return signature
}

func autoPromoteReworkSignatureMatches(current autoPromoteReworkSignature, event autoPromoteReworkSignature) bool {
	if current.empty() {
		return true
	}
	if current.PRNumber > 0 && event.PRNumber > 0 && current.PRNumber != event.PRNumber {
		return false
	}
	if current.HeadSHA != "" && event.HeadSHA != current.HeadSHA {
		return false
	}
	if len(current.FailedChecks) > 0 && !slices.Equal(current.FailedChecks, event.FailedChecks) {
		return false
	}
	if current.HeadSHA != "" || len(current.FailedChecks) > 0 {
		return true
	}
	return current.PRNumber <= 0 || event.PRNumber <= 0 || current.PRNumber == event.PRNumber
}

func (s autoPromoteReworkSignature) empty() bool {
	return s.PRNumber <= 0 && s.HeadSHA == "" && len(s.FailedChecks) == 0
}

func autoPromoteCanonicalChecks(checks []string) []string {
	checks = uniqueStrings(checks)
	if len(checks) == 0 {
		return nil
	}
	slices.Sort(checks)
	return checks
}

func (o *Orchestrator) recordAutoPromoteReworkHandoff(
	state *State,
	issue connector.Issue,
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	targetState string,
) {
	if state == nil || normalizeState(targetState) != normalizeState(autoPromoteReworkState) {
		return
	}
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" {
		return
	}
	if state.PriorAttempts == nil {
		state.PriorAttempts = map[string]runpkg.PriorAttempt{}
	}
	state.PriorAttempts[issueID] = runpkg.PriorAttempt{
		Source:    "auto_promote",
		Reason:    string(decision.Reason),
		Validator: summary.Validator,
	}
}

func (o *Orchestrator) logAutoPromoteDecision(issue connector.Issue, decision AutoPromoteDecision, targetState string) {
	if o.logger == nil {
		return
	}

	attrs := []any{
		"issue_id", strings.TrimSpace(issue.ID),
		"identifier", issue.Identifier,
		"action", decision.Action,
		"reason", decision.Reason,
	}
	if decision.CIStatus != "" {
		attrs = append(attrs, "ci_status", decision.CIStatus)
	}
	if issue.PullRequest != nil {
		if issue.PullRequest.Number > 0 {
			attrs = append(attrs, "pull_request_number", issue.PullRequest.Number)
		}
		if url := strings.TrimSpace(issue.PullRequest.URL); url != "" {
			attrs = append(attrs, "pull_request", url)
		}
		if mergeableState := strings.TrimSpace(issue.PullRequest.MergeableState); mergeableState != "" {
			attrs = append(attrs, "mergeable_state", mergeableState)
		}
		if reason := pullRequestHydrationUnavailableReason(issue.PullRequest); reason != "" {
			attrs = append(attrs, "pull_request_hydration_reason", reason)
		}
		if reason := strings.TrimSpace(issue.PullRequest.HydrationDegradedReason); reason != "" {
			attrs = append(attrs, "pull_request_hydration_degraded_reason", reason)
		}
		if issue.PullRequest.HydrationNextRetryAt != nil && !issue.PullRequest.HydrationNextRetryAt.IsZero() {
			attrs = append(attrs, "pull_request_hydration_next_retry_at", issue.PullRequest.HydrationNextRetryAt.UTC().Format(time.RFC3339))
		}
		if failedChecks := strings.Join(autoPromoteFailedChecksFromPullRequest(issue.PullRequest), ", "); failedChecks != "" {
			attrs = append(attrs, "failed_checks", failedChecks)
		}
		if pendingChecks := strings.Join(autoPromotePendingChecksFromPullRequest(issue.PullRequest), ", "); pendingChecks != "" {
			attrs = append(attrs, "pending_checks", pendingChecks)
		}
		if staleSuccessfulChecks := strings.Join(autoPromoteStaleSuccessfulChecks(issue.PullRequest), ", "); staleSuccessfulChecks != "" {
			attrs = append(attrs,
				"ci_anomaly", "stale_successful_check_run",
				"stale_successful_checks", staleSuccessfulChecks,
				"ci_anomaly_action", "treated_completed_successful_check_runs_as_passed",
			)
		}
		attrs = appendPullRequestReviewDisagreementAttrs(attrs, issue.PullRequest)
	}
	if decision.QuietRemaining > 0 {
		attrs = append(attrs, "quiet_remaining", decision.QuietRemaining)
	}
	if decision.WorkpadBlocker != "" {
		attrs = append(attrs, "workpad_blocker", decision.WorkpadBlocker)
	}
	if len(decision.ResolvedWorkpadBlockers) > 0 {
		attrs = append(attrs, "resolved_workpad_blockers", strings.Join(decision.ResolvedWorkpadBlockers, ","))
	}
	attrs = appendAutoPromoteWorkpadAttrs(attrs, decision)
	if targetState != "" {
		attrs = append(attrs, "target_state", targetState)
		o.logger.Info("auto promote decision", attrs...)
		return
	}
	o.logger.Info("auto promote decision", attrs...)
}

func appendAutoPromoteWorkpadAttrs(attrs []any, decision AutoPromoteDecision) []any {
	if evidence := strings.TrimSpace(decision.OperationalEvidence); evidence != "" {
		attrs = append(attrs,
			"completion_kind", workpad.CompletionOperational,
			"operational_evidence", evidence,
		)
	}
	if url := strings.TrimSpace(decision.WorkpadCommentURL); url != "" {
		attrs = append(attrs, "workpad_comment_url", url)
	}
	if source := strings.TrimSpace(decision.WorkpadSignalSource); source != "" {
		attrs = append(attrs, "workpad_signal_source", source)
	}
	if invalid := strings.TrimSpace(decision.WorkpadStatusInvalid); invalid != "" {
		attrs = append(attrs, "workpad_status_invalid", invalid)
	}
	if hash := strings.TrimSpace(decision.WorkpadStatusInvalidHash); hash != "" {
		attrs = append(attrs, "workpad_status_hash", hash)
	}
	if len(decision.WorkpadBlockerVerifications) > 0 {
		attrs = append(attrs, "workpad_blocker_verifications", strings.Join(autoPromoteWorkpadBlockerVerificationStrings(decision.WorkpadBlockerVerifications), "; "))
	}
	if decision.WorkpadProseFallbackDisabled {
		attrs = append(attrs, "workpad_prose_fallback_disabled", true)
	}
	return attrs
}

func autoPromoteWorkpadBlockerVerificationStrings(verifications []AutoPromoteWorkpadBlockerVerification) []string {
	out := make([]string, 0, len(verifications))
	for _, verification := range verifications {
		parts := []string{
			"ref=" + strings.TrimSpace(verification.Identifier),
			"status=" + strings.TrimSpace(verification.Status),
		}
		if state := strings.TrimSpace(verification.State); state != "" {
			parts = append(parts, "state="+state)
		}
		if reason := strings.TrimSpace(verification.Reason); reason != "" {
			parts = append(parts, "reason="+reason)
		}
		out = append(out, strings.Join(parts, " "))
	}
	return out
}

func autoPromoteComment(
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	sourceState string,
	targetState string,
) string {
	var b strings.Builder
	sourceState = displayStateName(sourceState)
	if sourceState == "" {
		sourceState = autoPromoteSourceState
	}
	targetState = displayStateName(targetState)
	if targetState == "" {
		return ""
	}
	switch {
	case decision.Action == AutoPromoteActionComplete:
		b.WriteString("Completed this issue operationally from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(" without a pull request.")
	case decision.Action == AutoPromoteActionPromote:
		b.WriteString("Auto-promoted this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(".")
	case decision.Action == AutoPromoteActionRework:
		b.WriteString("Auto-promote routed this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		switch decision.Reason {
		case AutoPromoteReasonCINotGreen:
			b.WriteString(": current-head CI is failing")
		case AutoPromoteReasonMergeConflicts:
			b.WriteString(": linked PR has merge conflicts")
		case AutoPromoteReasonUnresolvedReviewThreads:
			b.WriteString(": linked PR has ")
			b.WriteString(strconv.Itoa(len(summary.UnresolvedReviewThreads)))
			b.WriteString(" unresolved review thread")
			if len(summary.UnresolvedReviewThreads) != 1 {
				b.WriteString("s")
			}
		case AutoPromoteReasonWorkpadStatusInvalid:
			b.WriteString(": workpad status is invalid")
		}
		b.WriteString(".")
	case normalizeState(targetState) == normalizeState(autoPromoteSourceState):
		b.WriteString("Reconciled this issue from ")
		b.WriteString(sourceState)
		b.WriteString(" to ")
		b.WriteString(targetState)
		b.WriteString(" because it already has a linked PR.")
	default:
		return ""
	}

	b.WriteString("\n\n")
	b.WriteString("- reason: ")
	b.WriteString(string(decision.Reason))
	if summary.PullRequestURL != "" {
		b.WriteString("\n- pull request: ")
		b.WriteString(summary.PullRequestURL)
	}
	if summary.MergeableState != "" {
		b.WriteString("\n- mergeable_state: ")
		b.WriteString(summary.MergeableState)
	}
	if decision.CIStatus != "" {
		b.WriteString("\n- ci_status: ")
		b.WriteString(decision.CIStatus)
	}
	if failedChecks := strings.Join(summary.FailedChecks, ", "); failedChecks != "" {
		b.WriteString("\n- failed_checks: ")
		b.WriteString(failedChecks)
	}
	if len(summary.UnresolvedReviewThreads) > 0 {
		b.WriteString("\n- unresolved_review_threads: ")
		b.WriteString(strconv.Itoa(len(summary.UnresolvedReviewThreads)))
		if location := pullRequestReviewThreadLocation(summary.UnresolvedReviewThreads[0]); location != "" {
			b.WriteString("\n- first_unresolved_review_thread: ")
			b.WriteString(location)
		}
	}
	if evidence := strings.TrimSpace(decision.OperationalEvidence); evidence != "" {
		b.WriteString("\n- completion_kind: ")
		b.WriteString(workpad.CompletionOperational)
		b.WriteString("\n- operational_evidence: ")
		b.WriteString(evidence)
	}
	appendAutoPromoteWorkpadCommentFields(&b, decision)

	if len(decision.Findings) > 0 {
		b.WriteString("\n\nFindings:")
		for _, finding := range decision.Findings {
			b.WriteString("\n- ")
			b.WriteString(autoPromoteFindingText(finding))
		}
	}

	return b.String()
}

func pullRequestReviewThreadLocation(thread connector.PullRequestReviewThread) string {
	path := strings.TrimSpace(thread.Path)
	if path == "" {
		return ""
	}
	if thread.Line > 0 {
		return fmt.Sprintf("%s:%d", path, thread.Line)
	}
	return path
}

func appendAutoPromoteWorkpadCommentFields(b *strings.Builder, decision AutoPromoteDecision) {
	if invalid := strings.TrimSpace(decision.WorkpadStatusInvalid); invalid != "" {
		b.WriteString("\n- workpad_status_invalid: ")
		b.WriteString(invalid)
		b.WriteString("\n- allowed_statuses: ")
		b.WriteString(autoPromoteAllowedWorkpadStatuses())
	}
	if url := strings.TrimSpace(decision.WorkpadCommentURL); url != "" {
		b.WriteString("\n- workpad_comment: ")
		b.WriteString(url)
	}
	if hash := strings.TrimSpace(decision.WorkpadStatusInvalidHash); hash != "" {
		b.WriteString("\n- workpad_status_hash: ")
		b.WriteString(hash)
	}
}

func autoPromoteReworkLimitComment(
	summary AutoPromoteSummary,
	decision AutoPromoteDecision,
	sourceState string,
	limit autoPromoteReworkLimitSummary,
) string {
	var b strings.Builder
	sourceState = displayStateName(sourceState)
	if sourceState == "" {
		sourceState = autoPromoteSourceState
	}
	b.WriteString("Auto-promote routed this issue from ")
	b.WriteString(sourceState)
	b.WriteString(" to Blocked because the Rework limit was reached.")
	b.WriteString("\n\n")
	b.WriteString("- rework_limit: ")
	b.WriteString(strconv.Itoa(limit.Limit))
	b.WriteString("\n- prior_rework_transitions: ")
	b.WriteString(strconv.Itoa(limit.Count))
	b.WriteString("\n- current_rework_reason: ")
	b.WriteString(string(decision.Reason))
	if reasons := autoPromoteReworkReasonsText(limit.ReasonCounts); reasons != "" {
		b.WriteString("\n- repeated_rework_reasons: ")
		b.WriteString(reasons)
	}
	if summary.PullRequestURL != "" {
		b.WriteString("\n- pull request: ")
		b.WriteString(summary.PullRequestURL)
	}
	if summary.MergeableState != "" {
		b.WriteString("\n- mergeable_state: ")
		b.WriteString(summary.MergeableState)
	}
	if decision.CIStatus != "" {
		b.WriteString("\n- ci_status: ")
		b.WriteString(decision.CIStatus)
	}
	if failedChecks := strings.Join(summary.FailedChecks, ", "); failedChecks != "" {
		b.WriteString("\n- failed_checks: ")
		b.WriteString(failedChecks)
	}
	if len(summary.UnresolvedReviewThreads) > 0 {
		b.WriteString("\n- unresolved_review_threads: ")
		b.WriteString(strconv.Itoa(len(summary.UnresolvedReviewThreads)))
		if location := pullRequestReviewThreadLocation(summary.UnresolvedReviewThreads[0]); location != "" {
			b.WriteString("\n- first_unresolved_review_thread: ")
			b.WriteString(location)
		}
	}

	if len(decision.Findings) > 0 {
		b.WriteString("\n\nCurrent findings:")
		for _, finding := range decision.Findings {
			b.WriteString("\n- ")
			b.WriteString(autoPromoteFindingText(finding))
		}
	}

	return b.String()
}

func autoPromoteReworkReasonsText(counts []autoPromoteReworkReasonCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		if strings.TrimSpace(count.Reason) == "" || count.Count <= 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s x%d", count.Reason, count.Count))
	}
	return strings.Join(parts, ", ")
}

func autoPromoteFindingText(finding AutoPromoteFinding) string {
	body := strings.Join(strings.Fields(finding.Body), " ")
	if body == "" {
		body = "P1 finding"
	}
	if finding.Path != "" && finding.Line > 0 {
		body = fmt.Sprintf("%s (%s:%d)", body, finding.Path, finding.Line)
	} else if finding.Path != "" {
		body = body + " (" + finding.Path + ")"
	}
	if finding.URL != "" {
		body = body + " " + finding.URL
	}
	return body
}
