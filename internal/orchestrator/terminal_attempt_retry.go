package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	terminalAttemptWithoutWorkProductReason     = "terminal_attempt_without_work_product"
	terminalAttemptRetryLimitCause              = "terminal_attempt_retry_limit"
	workspacePreparationRetryLimitCause         = "workspace_preparation_retry_limit"
	consecutiveRetryCycleLimit                  = 3
	terminalAttemptRetryLimitEvent              = "terminal_attempt_retry_limit_reached"
	workspacePreparationRetryLimitEvent         = "workspace_preparation_retry_limit_reached"
	terminalAttemptRetryHistoryUnavailableEvent = "terminal_attempt_retry_history_unavailable"
	workspaceRetryHistoryUnavailableEvent       = "workspace_preparation_retry_history_unavailable"
	retryCycleAttemptErrorMaxBytes              = 4096
)

func (o *Orchestrator) demoteTerminalAttemptRetry(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	workProductPushed bool,
	retryCause string,
	durable bool,
	runMode string,
	diffStats DiffStats,
	at time.Time,
) (connector.Issue, bool, bool) {
	if o == nil || o.connector == nil ||
		normalizeState(issue.State) != normalizeState(planImplementationState) ||
		terminalAttemptHasWorkProduct(issue, workProductPushed) ||
		pullRequestHydrationBlocksProgress(issue.PullRequest) ||
		o.terminalAttemptClaimBlocksDemotion(ctx, issue, at) {
		return issue, false, false
	}
	if durable {
		count, latest, known := o.consecutiveRetryCycleCount(ctx, state, issue, retryCause, at)
		switch {
		case !known:
			o.recordRetryCycleHistoryUnavailable(state, issue, retryCause, at)
		case count >= consecutiveRetryCycleLimit:
			parked, ok := o.parkRetryCycleLimit(ctx, state, issue, runMode, diffStats, retryCause, count, latest, at)
			if ok {
				return parked, true, true
			}
		}
	}
	targetState := terminalAttemptTodoState(o.cfg.ActiveStates)
	if targetState == "" {
		return issue, false, false
	}
	if err := o.updateIssueState(ctx, state, issue, targetState, at, terminalAttemptWithoutWorkProductReason, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Warn(
				"terminal attempt retry demotion failed",
				"issue_id", issue.ID,
				"identifier", issue.Identifier,
				"from_state", issue.State,
				"target_state", targetState,
				"error", err,
			)
		}
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "terminal_attempt_retry_demotion_failed",
			Message: "failed to move " + issueLabel(issue) + " to " + targetState + " after a terminal attempt without pushed work",
		})
		return issue, false, false
	}

	fromState := issue.State
	issue.State = targetState
	if claimed, ok := state.Claimed[issue.ID]; ok {
		claimed.Issue = cloneIssue(issue)
		state.Claimed[issue.ID] = claimed
	}
	if retry, ok := state.Retry[issue.ID]; ok {
		retry.Issue = cloneIssue(issue)
		state.Retry[issue.ID] = retry
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   "terminal_attempt_retry_demoted",
		Message: "moved " + issueLabel(issue) + " from " + fromState + " to " + targetState + " after a terminal attempt without pushed work",
	})
	if o.logger != nil {
		o.logger.Info(
			"terminal attempt retry demoted",
			"issue_id", issue.ID,
			"identifier", issue.Identifier,
			"from_state", fromState,
			"target_state", targetState,
			"reason", terminalAttemptWithoutWorkProductReason,
		)
	}
	return issue, true, false
}

func (o *Orchestrator) consecutiveRetryCycleCount(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	cause string,
	at time.Time,
) (int, telemetry.WorkAttempt, bool) {
	for limit := consecutiveRetryCycleLimit; ; limit *= 2 {
		attempts, ok := o.recentIssueTerminalAttempts(ctx, state, issue, limit, at)
		if !ok {
			return 0, telemetry.WorkAttempt{}, false
		}
		count := 0
		latest := telemetry.WorkAttempt{}
		for _, attempt := range attempts {
			if strings.TrimSpace(attempt.ErrorClass) == "service_restart" &&
				terminalAttemptRetryableFailure(attempt) && !workAttemptHasPushedProduct(attempt) {
				continue
			}
			if !retryCycleAttemptMatches(attempt, cause) {
				return count, latest, true
			}
			if count == 0 {
				latest = attempt
			}
			count++
			if count >= consecutiveRetryCycleLimit {
				return count, latest, true
			}
		}
		if len(attempts) < limit {
			return count, latest, true
		}
	}
}

func (o *Orchestrator) recentIssueTerminalAttempts(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	limit int,
	at time.Time,
) ([]telemetry.WorkAttempt, bool) {
	byID := make(map[int64]telemetry.WorkAttempt)
	withoutID := make([]telemetry.WorkAttempt, 0)
	add := func(attempt telemetry.WorkAttempt) {
		if !workAttemptMatchesIssue(attempt, issue) ||
			!strings.EqualFold(strings.TrimSpace(attempt.Status), string(store.WorkAttemptStatusTerminal)) {
			return
		}
		if attempt.AttemptID > 0 {
			byID[attempt.AttemptID] = attempt
			return
		}
		withoutID = append(withoutID, attempt)
	}
	if state != nil {
		for _, attempt := range state.WorkAttempts {
			add(attempt)
		}
	}
	if o != nil && o.workAttempts != nil {
		stored, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{
			ProjectID:  strings.TrimSpace(o.cfg.Project.ID),
			IssueID:    strings.TrimSpace(issue.ID),
			Identifier: strings.TrimSpace(issue.Identifier),
			IssueURL:   strings.TrimSpace(issue.URL),
			Limit:      limit,
		})
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("retry cycle work attempt history lookup failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
			}
			return nil, false
		}
		for _, attempt := range stored {
			add(telemetryWorkAttempt(attempt, at))
		}
	}
	attempts := make([]telemetry.WorkAttempt, 0, len(byID)+len(withoutID))
	for _, attempt := range byID {
		attempts = append(attempts, attempt)
	}
	attempts = append(attempts, withoutID...)
	sort.Slice(attempts, func(left, right int) bool {
		return workAttemptCompletedAfter(attempts[left], attempts[right])
	})
	if limit > 0 && len(attempts) > limit {
		attempts = attempts[:limit]
	}
	return attempts, true
}

func workAttemptMatchesIssue(attempt telemetry.WorkAttempt, issue connector.Issue) bool {
	if issueID := strings.TrimSpace(issue.ID); issueID != "" && strings.TrimSpace(attempt.IssueID) == issueID {
		return true
	}
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" && strings.TrimSpace(attempt.Identifier) == identifier {
		return true
	}
	issueURL := strings.TrimSpace(issue.URL)
	return issueURL != "" && strings.TrimSpace(attempt.IssueURL) == issueURL
}

func retryCycleAttemptMatches(attempt telemetry.WorkAttempt, cause string) bool {
	switch strings.TrimSpace(cause) {
	case workspacePreparationRetryLimitCause:
		return strings.TrimSpace(attempt.ErrorClass) == workAttemptErrorWorkspace &&
			strings.EqualFold(strings.TrimSpace(attempt.TerminalState), string(store.WorkAttemptTerminalFailure))
	case terminalAttemptRetryLimitCause:
		return strings.TrimSpace(attempt.ErrorClass) != workAttemptErrorWorkspace &&
			strings.TrimSpace(attempt.ErrorClass) != "service_restart" &&
			terminalAttemptRetryableFailure(attempt) &&
			!workAttemptHasPushedProduct(attempt)
	default:
		return false
	}
}

func retryCycleCauseForAttempt(attempt telemetry.WorkAttempt) string {
	if strings.TrimSpace(attempt.ErrorClass) == workAttemptErrorWorkspace {
		return workspacePreparationRetryLimitCause
	}
	return terminalAttemptRetryLimitCause
}

func (o *Orchestrator) parkRetryCycleLimit(
	ctx context.Context,
	state *State,
	issue connector.Issue,
	runMode string,
	diffStats DiffStats,
	cause string,
	count int,
	latest telemetry.WorkAttempt,
	at time.Time,
) (connector.Issue, bool) {
	if o == nil || o.connector == nil || state == nil {
		return issue, false
	}
	targetState := blockedStatusState
	metadata := o.newBlockedRecoveryMetadata(
		ctx,
		issue,
		runMode,
		cause,
		blockedRecoveryPredicateFingerprintChange,
		"Todo",
		diffStats,
	)
	attemptError := retryCycleAttemptError(o, latest)
	metadata.BlockedRecovery.WorkAttemptID = latest.AttemptID
	metadata.BlockedRecovery.AttemptNumber = latest.AttemptNumber
	metadata.BlockedRecovery.AttemptError = attemptError
	if err := o.updateIssueStateByIDWithMetadata(ctx, state, issue.ID, issue, targetState, at, cause, metadata, laneMutationRevokeWorker); err != nil {
		if o.logger != nil {
			o.logger.Error("retry cycle limit state transition failed", "issue_id", issue.ID, "identifier", issue.Identifier, "cause", cause, "target_state", targetState, "error", err)
		}
		return issue, false
	}
	issue.State = targetState
	if err := o.abandonClaim(ctx, issue.ID); err != nil && o.logger != nil {
		o.logger.Warn("retry cycle limit claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "cause", cause, "error", err)
	}
	delete(state.Claimed, issue.ID)
	delete(state.Retry, issue.ID)
	delete(state.BudgetRefusals, issue.ID)
	delete(state.PriorAttempts, issue.ID)
	if state.Blocked == nil {
		state.Blocked = map[string]Blocked{}
	}
	state.Blocked[issue.ID] = Blocked{
		Issue:                   cloneIssue(issue),
		Reason:                  cause,
		RecoveryAction:          "defer",
		RecoveryReason:          blockedRecoveryReasonBreakerCooldownActive,
		RecoveryTarget:          metadata.BlockedRecovery.TargetState,
		RecoveryRemedy:          BlockedRecoveryOperatorRemedy(issue, blockedRecoveryReasonBreakerCooldownActive),
		RecoveryReachability:    blockedRecoveryReachability("defer"),
		RecoveryIntentResumable: true,
		BlockedAt:               at,
		Source:                  BlockedSourceProjectStatus,
		AttemptError:            attemptError,
		WorkAttemptID:           latest.AttemptID,
		Recovery:                metadata.BlockedRecovery,
	}
	if o.connector != nil {
		comment := retryCycleLimitComment(issue, cause, count, targetState, latest, attemptError)
		if err := o.connector.CreateComment(ctx, issue.ID, comment); err != nil && o.logger != nil {
			o.logger.Warn("retry cycle limit comment failed", "issue_id", issue.ID, "identifier", issue.Identifier, "cause", cause, "error", err)
		}
	}
	event := retryCycleLimitEvent(cause)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   event,
		Message: fmt.Sprintf("parked %s after %d consecutive %s attempts", issueLabel(issue), count, retryCycleDescription(cause)),
	})
	if o.logger != nil {
		telemetry.LogLifecycleMessage(o.logger, slog.LevelError, telemetry.LifecycleSafetyControl, event, "retry cycle limit reached", o.issueLifecycleCorrelation(issue),
			"cause", cause,
			"consecutive_attempts", count,
			"limit", consecutiveRetryCycleLimit,
			"target_state", targetState,
		)
	}
	return issue, true
}

func retryCycleAttemptError(o *Orchestrator, attempt telemetry.WorkAttempt) string {
	value := strings.TrimSpace(attempt.ErrorMessage)
	if o != nil {
		value = o.operatorText(value)
	}
	return runtimeoutput.Truncate(value, retryCycleAttemptErrorMaxBytes).Value
}

func retryCycleLimitComment(
	issue connector.Issue,
	cause string,
	count int,
	targetState string,
	attempt telemetry.WorkAttempt,
	attemptError string,
) string {
	comment := fmt.Sprintf(
		"Detent stopped retrying %s after %d consecutive %s attempts. The issue is parked in `%s` until the configured breaker cooldown ends, then Detent returns it to its prior lane automatically.",
		issueLabel(issue),
		count,
		retryCycleDescription(cause),
		targetState,
	)
	if attemptError == "" {
		return comment
	}
	attemptNumber := attempt.AttemptNumber
	if attemptNumber <= 0 && attempt.AttemptID > 0 {
		attemptNumber = int(attempt.AttemptID)
	}
	return fmt.Sprintf("%s\n\n- latest_attempt: %d\n- last_error:\n\n```text\n%s\n```", comment, attemptNumber, attemptError)
}

func retryCycleDescription(cause string) string {
	if strings.TrimSpace(cause) == workspacePreparationRetryLimitCause {
		return "workspace-preparation"
	}
	return "terminal-without-work-product"
}

func retryCycleLimitEvent(cause string) string {
	if strings.TrimSpace(cause) == workspacePreparationRetryLimitCause {
		return workspacePreparationRetryLimitEvent
	}
	return terminalAttemptRetryLimitEvent
}

func (o *Orchestrator) recordRetryCycleHistoryUnavailable(state *State, issue connector.Issue, cause string, at time.Time) {
	event := terminalAttemptRetryHistoryUnavailableEvent
	if strings.TrimSpace(cause) == workspacePreparationRetryLimitCause {
		event = workspaceRetryHistoryUnavailableEvent
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   event,
		Message: "continued retrying " + issueLabel(issue) + " because durable retry history was unavailable",
	})
}

func (o *Orchestrator) releaseTerminalAttemptClaim(ctx context.Context, state *State, issue connector.Issue, at time.Time) {
	if err := o.abandonClaim(ctx, issue.ID); err != nil {
		if o.logger != nil {
			o.logger.Warn("terminal attempt claim release failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "claim_release_failed",
			Message: "claim lease release failed for " + issueLabel(issue) + " after terminal attempt: " + err.Error(),
		})
	}
	delete(state.Claimed, issue.ID)
}

func (o *Orchestrator) reconcileTerminalAttemptRetryStates(
	ctx context.Context,
	state *State,
	issues []connector.Issue,
	now time.Time,
) []connector.Issue {
	if state == nil || len(issues) == 0 || len(state.WorkAttempts) == 0 {
		return nil
	}
	latestByIssue := latestTerminalAttemptsByIssue(state.WorkAttempts)
	transitions := make([]connector.Issue, 0)
	seen := make(map[string]struct{}, len(issues))
	for _, issue := range issues {
		issueID := strings.TrimSpace(issue.ID)
		if issueID == "" {
			continue
		}
		if _, ok := seen[issueID]; ok {
			continue
		}
		seen[issueID] = struct{}{}
		if normalizeState(issue.State) != normalizeState(planImplementationState) {
			continue
		}
		if _, running := state.Running[issueID]; running {
			continue
		}
		attempt, ok := latestByIssue[issueID]
		if !ok || !terminalAttemptRetryableFailure(attempt) {
			continue
		}
		updated, changed, _ := o.demoteTerminalAttemptRetry(
			ctx,
			state,
			issue,
			workAttemptHasPushedProduct(attempt),
			retryCycleCauseForAttempt(attempt),
			true,
			workAttemptRunMode(attempt),
			DiffStats{},
			now,
		)
		if changed {
			transitions = append(transitions, updated)
		}
	}
	return transitions
}

func workAttemptRunMode(attempt telemetry.WorkAttempt) string {
	var metadata struct {
		RunMode string `json:"run_mode"`
	}
	if json.Unmarshal([]byte(attempt.WorkerMetadataJSON), &metadata) == nil && strings.TrimSpace(metadata.RunMode) != "" {
		return strings.TrimSpace(metadata.RunMode)
	}
	return RunModeImplement
}

func (o *Orchestrator) terminalAttemptClaimBlocksDemotion(ctx context.Context, issue connector.Issue, now time.Time) bool {
	if !o.cfg.Claiming.Enabled {
		return false
	}
	owner := o.claimOwner()
	if owner == "" || o.cfg.Claiming.LeaseField == "" || o.fieldClaimMissingOwnerField() {
		return true
	}
	return !o.claimable(ctx, issue, owner, now)
}

func latestTerminalAttemptsByIssue(attempts []telemetry.WorkAttempt) map[string]telemetry.WorkAttempt {
	latest := make(map[string]telemetry.WorkAttempt)
	for _, attempt := range attempts {
		if !strings.EqualFold(strings.TrimSpace(attempt.Status), string(store.WorkAttemptStatusTerminal)) {
			continue
		}
		issueID := strings.TrimSpace(attempt.IssueID)
		if issueID == "" {
			continue
		}
		current, ok := latest[issueID]
		if ok && !workAttemptCompletedAfter(attempt, current) {
			continue
		}
		latest[issueID] = attempt
	}
	return latest
}

func workAttemptCompletedAfter(left telemetry.WorkAttempt, right telemetry.WorkAttempt) bool {
	if left.CompletedAt != nil && right.CompletedAt != nil && !left.CompletedAt.Equal(*right.CompletedAt) {
		return left.CompletedAt.After(*right.CompletedAt)
	}
	if left.CompletedAt != nil && right.CompletedAt == nil {
		return true
	}
	if left.CompletedAt == nil && right.CompletedAt != nil {
		return false
	}
	return left.AttemptID > right.AttemptID
}

func terminalAttemptRetryableFailure(attempt telemetry.WorkAttempt) bool {
	errorClass := strings.TrimSpace(attempt.ErrorClass)
	if errorClass == backendcapacity.ErrorClass || errorClass == forgeUnavailableErrorClass || errorClass == workspaceBranchHoldErrorClass || errorClass == workerGitHubMonitorErrorClass || errorClass == workerGitHubTokenResolutionErrorClass {
		return false
	}
	if errorClass == githubRESTCapacityError {
		_, durable := githubRESTWaitMetadataFromAttempt(store.WorkAttempt{
			TerminalState:      store.WorkAttemptTerminalState(strings.ToLower(strings.TrimSpace(attempt.TerminalState))),
			ErrorClass:         errorClass,
			WorkerMetadataJSON: attempt.WorkerMetadataJSON,
		})
		if durable {
			return false
		}
	}
	return terminalAttemptStateRetryDemotable(store.WorkAttemptTerminalState(strings.ToLower(strings.TrimSpace(attempt.TerminalState))))
}

func terminalAttemptStateRetryDemotable(state store.WorkAttemptTerminalState) bool {
	switch state {
	case store.WorkAttemptTerminalFailure,
		store.WorkAttemptTerminalTimedOut,
		store.WorkAttemptTerminalAbandoned,
		store.WorkAttemptTerminalCapacity:
		return true
	default:
		return false
	}
}

func terminalAttemptTodoState(activeStates []string) string {
	for _, state := range activeStates {
		if normalizeState(state) == "todo" {
			return displayStateName(state)
		}
	}
	return ""
}

func terminalAttemptHasWorkProduct(issue connector.Issue, pushed bool) bool {
	return pushed || implementProgressLinkedPullRequest(issue)
}

func workAttemptHasPushedProduct(attempt telemetry.WorkAttempt) bool {
	return attempt.PRNumber != nil && *attempt.PRNumber > 0 || workAttemptMetadataHasPushedProduct(attempt.WorkerMetadataJSON)
}

func workAttemptMetadataHasPushedProduct(raw string) bool {
	var metadata struct {
		WorkProductPushed bool `json:"work_product_pushed"`
	}
	return json.Unmarshal([]byte(raw), &metadata) == nil && metadata.WorkProductPushed
}
