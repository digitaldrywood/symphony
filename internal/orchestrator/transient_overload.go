package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func (o *Orchestrator) handleTransientOverload(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	overloadErr *backendcapacity.Error,
) {
	releaseBackendCapacityProbe(state, running)
	delay := event.RetryDelay
	if delay <= 0 {
		delay = o.cfg.OverloadRetryDelay
	}
	if delay <= 0 {
		delay = defaultOverloadRetryDelay
	}
	errorClass := backendcapacity.TransientOverloadErrorClass
	terminalState := store.WorkAttemptTerminalFailure
	statusMessage := "retrying after transient provider overload"
	retryEvent := "transient_overload_retry"
	retryMessage := "transient provider overload"
	if backendcapacity.IsStartupFailureKind(overloadErr.Details.Kind) {
		errorClass = backendcapacity.StartupFailureErrorClass
		statusMessage = "retrying after backend startup failure"
		retryEvent = "backend_startup_failure_retry"
		retryMessage = "backend startup handshake failed"
		if overloadErr.Details.Kind == backendcapacity.StartupTimeoutKind {
			errorClass = backendcapacity.StartupTimeoutErrorClass
			terminalState = store.WorkAttemptTerminalTimedOut
			statusMessage = "retrying after backend startup timeout"
			retryEvent = "backend_startup_timeout_retry"
			retryMessage = "backend startup handshake timed out"
		}
		o.recordProjectAttemptOutcome(
			state,
			event.IssueID,
			event.CompletedAt,
			store.WorkAttemptTerminalTimedOut,
			event.Err,
			errorClass,
			event.Err.Error(),
		)
	} else {
		o.deferProjectFailureBreakerCanary(state, event.IssueID, event.CompletedAt, delay)
	}
	attemptCompleted := o.completeDurableWorkAttemptWithMetadata(
		ctx,
		state,
		running,
		event.CompletedAt,
		terminalState,
		errorClass,
		event.Err.Error(),
		"waiting",
		statusMessage,
		startupFailureMetadata(overloadErr.Details),
	)
	if workspaceIssueTerminal(running.Issue, o.cfg.TerminalStates) {
		o.releaseClaim(state, running.Issue.ID)
		return
	}
	attempt := event.RetryAttempt
	if attempt < 1 {
		attempt = running.Attempt
	}
	if attempt < 1 {
		attempt = 1
	}
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
		terminalAttemptFailureEvidence(running, terminalState, errorClass, event.Err.Error(), event.CompletedAt),
	)
	if parked {
		return
	}
	o.scheduleRetryAfter(
		state,
		running.Issue,
		attempt,
		event.CompletedAt,
		delay,
		errorClass,
		running.WorkerHost,
	)
	retryAt := event.CompletedAt.Add(delay)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      event.CompletedAt,
		Event:   retryEvent,
		Message: retryMessage + "; retrying affected issue at " + retryAt.Format(time.RFC3339),
	})
	if o.logger != nil {
		o.logger.Log(
			ctx,
			slog.LevelInfo,
			"transient overload retry scheduled",
			"reason", errorClass,
			"backend_id", overloadErr.Scope.BackendID,
			"backend_kind", overloadErr.Scope.BackendKind,
			"provider", overloadErr.Scope.Provider,
			"issue_id", running.Issue.ID,
			"attempt", attempt,
			"retry_at", retryAt,
			"error", event.Err,
		)
	}
}

func startupFailureMetadata(details backendcapacity.Details) map[string]any {
	if details.Startup == nil {
		return nil
	}
	return map[string]any{"backend_startup": details.Startup}
}
