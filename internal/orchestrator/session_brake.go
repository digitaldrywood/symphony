package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

const sessionBrakeMetadataKey = "session_brake"

func (o *Orchestrator) sessionProgressProbe(issue connector.Issue) runpkg.SessionProgressProbe {
	reader, ok := o.connector.(connector.IssueCommentReader)
	if !ok {
		return nil
	}
	return func(ctx context.Context) (string, error) {
		comments, err := reader.FetchIssueComments(ctx, issue)
		if err != nil {
			return "", err
		}
		for index := len(comments) - 1; index >= 0; index-- {
			if autoPromoteIsWorkpadComment(comments[index].Body) {
				return workpad.ContentHash(strings.TrimSpace(comments[index].Body)), nil
			}
		}
		return "", nil
	}
}

func (o *Orchestrator) handleSessionBrake(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
) bool {
	var brake *runpkg.SessionBrakeError
	if !errors.As(event.Err, &brake) || brake == nil {
		return false
	}
	completedAt := event.CompletedAt.UTC()
	if completedAt.IsZero() {
		completedAt = o.clockNow().UTC()
	}
	if diffStatsPresent(event.Result.DiffStats) {
		running.DiffStats = event.Result.DiffStats
		state.DiffStats[event.IssueID] = event.Result.DiffStats
	}
	running.WorkProductPushed = running.WorkProductPushed ||
		event.Result.PullRequestHeadPushed ||
		event.Result.PullRequestUpdated
	resumable := brake.Resumable ||
		running.WorkProductPushed ||
		terminalAttemptHasWorkProduct(running.Issue, running.WorkProductPushed) ||
		sessionBrakeDiffResumable(running.DiffStats)
	targetState := sessionBrakeTargetState(o.cfg, resumable)

	if o.logger != nil {
		telemetry.LogLifecycle(o.logger, slog.LevelWarn, telemetry.LifecycleSafetyControl, "session_brake_tripped", o.runningLifecycleCorrelation(running.Issue, running),
			"reason", brake.Reason,
			"cause_fingerprint", brake.CauseFingerprint,
			"elapsed", brake.Elapsed,
			"turns", brake.Turns,
			"tokens", brake.Tokens,
			"rss_bytes", brake.RSSBytes,
			"rss_ceiling_bytes", brake.RSSCeilingBytes,
			"target_state", targetState,
			"resumable", resumable,
		)
	}

	terminalState := store.WorkAttemptTerminalTimedOut
	phase := "timed_out"
	statusMessage := "agent session exceeded its configured catastrophe bound"
	if errors.Is(brake, runpkg.ErrSessionMemoryCeilingExceeded) {
		terminalState = store.WorkAttemptTerminalMemoryCeiling
		phase = runpkg.FinalStateMemoryCeilingExceeded
		statusMessage = "agent session stopped after exceeding its memory ceiling"
	} else if errors.Is(brake, runpkg.ErrSessionNoProgress) {
		terminalState = store.WorkAttemptTerminalNoProgress
		phase = "no_progress"
		statusMessage = "agent session stopped after no work-product progress"
	}
	o.recordProjectAttemptOutcome(
		state,
		event.IssueID,
		completedAt,
		terminalState,
		event.Err,
		brake.Reason,
		event.Err.Error(),
	)
	o.completeDurableWorkAttemptWithMetadata(
		ctx,
		state,
		running,
		completedAt,
		terminalState,
		brake.Reason,
		event.Err.Error(),
		phase,
		statusMessage,
		sessionBrakeMetadata(brake, targetState, resumable),
	)

	issue := cloneIssue(running.Issue)
	metadata := workflowLaneMetadata{
		LessonEvidence: reworkLessonEvidence{LastCommand: running.LastCommand},
		BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{
			Owner:            blockedRecoveryOwnerOrchestrator,
			Cause:            brake.Reason,
			Predicate:        blockedRecoveryPredicateManaged,
			CauseFingerprint: brake.CauseFingerprint,
			TargetState:      targetState,
			RunMode:          running.Mode,
			IntentResumable:  resumable,
		},
	}
	if err := o.updateIssueStateByIDStrictWithMetadata(
		ctx,
		state,
		issue.ID,
		issue,
		targetState,
		completedAt,
		brake.Reason,
		metadata,
		laneMutationRevokeWorker,
	); err != nil {
		if o.logger != nil {
			o.logger.Error(
				"session brake state transition failed",
				"issue_id", issue.ID,
				"identifier", issue.Identifier,
				"target_state", targetState,
				"cause_fingerprint", brake.CauseFingerprint,
				"error", err,
			)
		}
	} else {
		issue.State = targetState
	}
	if o.connector != nil {
		if err := o.connector.CreateComment(ctx, issue.ID, sessionBrakeComment(brake, targetState, resumable)); err != nil && o.logger != nil {
			o.logger.Warn(
				"session brake comment failed",
				"issue_id", issue.ID,
				"identifier", issue.Identifier,
				"cause_fingerprint", brake.CauseFingerprint,
				"error", err,
			)
		}
	}

	tokens := event.Result.Tokens
	if tokens == (TokenTotals{}) {
		tokens = running.Tokens
	}
	state.Completed[event.IssueID] = Completed{
		Issue:           issue,
		SessionID:       running.SessionID,
		StartedAt:       running.StartedAt,
		CompletedAt:     completedAt,
		FinalState:      event.Result.FinalState,
		Tokens:          tokens,
		RuntimeIdentity: running.RuntimeIdentity,
	}
	state.TokenTotals = addTokenTotals(state.TokenTotals, tokens)
	delete(state.Claimed, event.IssueID)
	delete(state.Retry, event.IssueID)
	delete(state.Blocked, event.IssueID)
	delete(state.BudgetRefusals, event.IssueID)
	delete(state.PriorAttempts, event.IssueID)
	delete(state.InstantFailures, event.IssueID)
	delete(state.RepeatedFailures, event.IssueID)
	releaseDispatchRecoveryAdmission(state, event.IssueID)
	releaseProjectFailureBreakerCanary(state, event.IssueID)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      completedAt,
		Event:   "session_brake_tripped",
		Message: "parked " + issueLabel(issue) + " in " + targetState + " after " + brake.Reason,
	})
	return true
}

func sessionBrakeDiffResumable(diff DiffStats) bool {
	return diff.UnpushedCommits > 0 ||
		diff.FilesChanged > 0 ||
		diff.AddedLines > 0 ||
		diff.RemovedLines > 0
}

func sessionBrakeTargetState(cfg Config, resumable bool) string {
	reworkState := sessionBrakeActiveState(cfg.ActiveStates, cfg.AutoPromote.ReworkState)
	if resumable && reworkState != "" {
		return reworkState
	}
	if todoState := terminalAttemptTodoState(cfg.ActiveStates); todoState != "" {
		return todoState
	}
	if reworkState != "" {
		return reworkState
	}
	for _, state := range cfg.ActiveStates {
		if state = displayStateName(state); state != "" {
			return state
		}
	}
	return ""
}

func sessionBrakeActiveState(activeStates []string, preferred string) string {
	preferred = normalizeState(preferred)
	for _, state := range activeStates {
		if normalizeState(state) == preferred {
			return displayStateName(state)
		}
	}
	return ""
}

func sessionBrakeMetadata(brake *runpkg.SessionBrakeError, targetState string, resumable bool) map[string]any {
	return map[string]any{
		sessionBrakeMetadataKey: map[string]any{
			"reason":              brake.Reason,
			"cause_fingerprint":   brake.CauseFingerprint,
			"elapsed_seconds":     brake.Elapsed.Seconds(),
			"turns":               brake.Turns,
			"tokens":              brake.Tokens,
			"rss_bytes":           brake.RSSBytes,
			"rss_ceiling_bytes":   brake.RSSCeilingBytes,
			"configured_duration": brake.Limit.String(),
			"configured_turns":    brake.MaxTurns,
			"last_progress_at":    brake.LastProgressAt.UTC().Format(time.RFC3339Nano),
			"target_state":        strings.TrimSpace(targetState),
			"resumable":           resumable,
			"checkpoint":          brake.Checkpoint,
		},
	}
}

func sessionBrakeComment(brake *runpkg.SessionBrakeError, targetState string, resumable bool) string {
	var body strings.Builder
	body.WriteString("Detent cancelled this agent session after a configured catastrophe bound was exceeded.")
	body.WriteString("\n\n- reason: ")
	body.WriteString(brake.Reason)
	body.WriteString("\n- cause_fingerprint: ")
	body.WriteString(brake.CauseFingerprint)
	body.WriteString("\n- elapsed: ")
	body.WriteString(brake.Elapsed.String())
	body.WriteString("\n- turns: ")
	body.WriteString(strconv.Itoa(brake.Turns))
	body.WriteString("\n- tokens: ")
	body.WriteString(strconv.FormatInt(brake.Tokens, 10))
	if brake.RSSCeilingBytes > 0 {
		body.WriteString("\n- rss_bytes: ")
		body.WriteString(strconv.FormatUint(brake.RSSBytes, 10))
		body.WriteString("\n- rss_ceiling_bytes: ")
		body.WriteString(strconv.FormatUint(brake.RSSCeilingBytes, 10))
	}
	body.WriteString("\n- resumable: ")
	body.WriteString(strconv.FormatBool(resumable))
	body.WriteString("\n- parked_state: ")
	body.WriteString(strings.TrimSpace(targetState))
	if brake.Checkpoint != nil {
		body.WriteString("\n\nCheckpoint: ")
		body.WriteString(brake.Checkpoint.Detail)
		body.WriteString("\n- workspace: ")
		body.WriteString(brake.Checkpoint.WorkspacePath)
		body.WriteString("\n- checkpoint_head: ")
		body.WriteString(brake.Checkpoint.HeadSHA)
	}
	return body.String()
}
