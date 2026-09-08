package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/github"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	deferredCompletionSchema       = 1
	deferredCompletionMetadataKey  = "deferred_completion"
	deferredCompletionPhase        = "completion_deferred"
	deferredCompletionStatus       = "completion waiting on tracker lane fence"
	deferredCompletionNextAction   = "retry completion fence"
	deferredCompletionInvalidClass = "completion_deferral_invalid"
)

type deferredCompletion struct {
	Schema       int                            `json:"schema"`
	Running      Running                        `json:"running"`
	Request      deferredCompletionRequest      `json:"request"`
	Result       runpkg.RunResult               `json:"result"`
	Error        string                         `json:"error,omitempty"`
	CompletedAt  time.Time                      `json:"completed_at"`
	Retryable    bool                           `json:"retryable,omitempty"`
	RetryAttempt int                            `json:"retry_attempt,omitempty"`
	RetryDelay   time.Duration                  `json:"retry_delay,omitempty"`
	FenceRetryAt time.Time                      `json:"fence_retry_at,omitzero"`
	DeferredAt   time.Time                      `json:"deferred_at"`
	Availability deferredCompletionAvailability `json:"availability"`
	Persisted    bool                           `json:"-"`
}

type deferredCompletionRequest struct {
	ProjectID           string                 `json:"project_id,omitempty"`
	Issue               connector.Issue        `json:"issue"`
	Attempt             int                    `json:"attempt,omitempty"`
	WorkAttemptID       int64                  `json:"work_attempt_id,omitempty"`
	Generation          uint64                 `json:"generation,omitempty"`
	Mode                string                 `json:"mode,omitempty"`
	DispatchSourceState string                 `json:"dispatch_source_state,omitempty"`
	DispatchTargetState string                 `json:"dispatch_target_state,omitempty"`
	PriorAttempt        runpkg.PriorAttempt    `json:"prior_attempt,omitzero"`
	StartedAt           time.Time              `json:"started_at,omitzero"`
	WorkerHost          string                 `json:"worker_host,omitempty"`
	RetryMode           runpkg.RetryMode       `json:"retry_mode,omitempty"`
	ResumeState         store.AgentResumeState `json:"resume_state,omitzero"`
	MergePrecheck       *runpkg.MergePrecheck  `json:"merge_precheck,omitempty"`
}

type deferredCompletionAvailability struct {
	Scope   connector.TrackerAvailabilityScope `json:"scope"`
	Class   string                             `json:"class,omitempty"`
	Message string                             `json:"message"`
}

func newDeferredCompletion(event runpkg.Completion, running Running, fenceErr error, deferredAt time.Time) deferredCompletion {
	running.CompletionOwnershipReleased = true
	record := deferredCompletion{
		Schema:       deferredCompletionSchema,
		Running:      running,
		Request:      deferredCompletionRequestFromRun(event.Request),
		Result:       event.Result,
		Error:        errorString(event.Err),
		CompletedAt:  event.CompletedAt,
		Retryable:    event.Retryable,
		RetryAttempt: event.RetryAttempt,
		RetryDelay:   event.RetryDelay,
		DeferredAt:   deferredAt,
	}
	if fenceErr != nil {
		record.Availability = deferredCompletionAvailability{Class: laneRevocationCompletionFenceUnavailable, Message: fenceErr.Error()}
	}
	if availabilityErr, ok := connector.AsTrackerAvailability(fenceErr); ok {
		record.Availability = deferredCompletionAvailability{
			Scope:   availabilityErr.Scope.Normalize(),
			Class:   strings.TrimSpace(availabilityErr.Class),
			Message: strings.TrimSpace(availabilityErr.Error()),
		}
	}
	return record
}

func deferredCompletionRequestFromRun(request runpkg.RunRequest) deferredCompletionRequest {
	return deferredCompletionRequest{
		ProjectID:           request.ProjectID,
		Issue:               cloneIssue(request.Issue),
		Attempt:             request.Attempt,
		WorkAttemptID:       request.WorkAttemptID,
		Generation:          request.Generation,
		Mode:                request.Mode,
		DispatchSourceState: request.DispatchSourceState,
		DispatchTargetState: request.DispatchTargetState,
		PriorAttempt:        request.PriorAttempt,
		StartedAt:           request.StartedAt,
		WorkerHost:          request.WorkerHost,
		RetryMode:           request.RetryMode,
		ResumeState:         request.ResumeState,
		MergePrecheck:       cloneMergePrecheck(request.MergePrecheck),
	}
}

func (r deferredCompletion) completion() runpkg.Completion {
	event := runpkg.Completion{
		IssueID: r.Running.Issue.ID,
		Request: runpkg.RunRequest{
			ProjectID:           r.Request.ProjectID,
			Issue:               cloneIssue(r.Request.Issue),
			Attempt:             r.Request.Attempt,
			WorkAttemptID:       r.Request.WorkAttemptID,
			Generation:          r.Request.Generation,
			Mode:                r.Request.Mode,
			DispatchSourceState: r.Request.DispatchSourceState,
			DispatchTargetState: r.Request.DispatchTargetState,
			PriorAttempt:        r.Request.PriorAttempt,
			StartedAt:           r.Request.StartedAt,
			WorkerHost:          r.Request.WorkerHost,
			RetryMode:           r.Request.RetryMode,
			ResumeState:         r.Request.ResumeState,
			MergePrecheck:       cloneMergePrecheck(r.Request.MergePrecheck),
		},
		Result:       r.Result,
		CompletedAt:  r.CompletedAt,
		Retryable:    r.Retryable,
		RetryAttempt: r.RetryAttempt,
		RetryDelay:   r.RetryDelay,
	}
	if strings.TrimSpace(r.Error) != "" {
		event.Err = errors.New(r.Error)
	}
	return event
}

func (o *Orchestrator) deferTrackerUnavailableCompletion(
	ctx context.Context,
	state *State,
	event runpkg.Completion,
	running Running,
	fenceErr error,
) {
	deferredAt := o.clockNow().UTC()
	if deferredAt.IsZero() {
		deferredAt = event.CompletedAt.UTC()
	}
	if deferredAt.IsZero() {
		deferredAt = time.Now().UTC()
	}
	o.observeTrackerReadFailure(state, "", fenceErr, deferredAt)
	record := newDeferredCompletion(event, running, fenceErr, deferredAt)
	record.FenceRetryAt = o.completionFenceRetryAt(state, fenceErr, deferredAt, event.RetryDelay)
	record.Persisted = o.persistDeferredCompletion(ctx, state, record)

	if !running.CompletionOwnershipReleased {
		o.releaseGlobalDispatchSlot(running.globalSlot)
		o.logWorkerLifecycle(running.Issue, "worker_capacity_released",
			telemetry.WorkAttemptIDKey, running.WorkAttemptID,
			telemetry.DetentSessionIDKey, running.DetentSessionID,
			telemetry.ProviderSessionIDKey, running.SessionID,
			"attempt", running.Attempt,
			"worker_host", strings.TrimSpace(running.WorkerHost),
			"reason", deferredCompletionPhase,
		)
		if running.cancel != nil {
			running.cancel()
		}
	}
	o.heartbeats.remove(event.IssueID)
	delete(state.Running, event.IssueID)
	releaseDispatchRecoveryAdmission(state, event.IssueID)
	if state.deferredCompletions == nil {
		state.deferredCompletions = map[string]deferredCompletion{}
	}
	state.deferredCompletions[event.IssueID] = record
	state.Retry[event.IssueID] = Retry{
		Issue:              cloneIssue(running.Issue),
		Attempt:            running.Attempt,
		DueAt:              record.FenceRetryAt,
		Error:              deferredCompletionStatus,
		WorkerHost:         running.WorkerHost,
		TrackerUnavailable: true,
		CompletionDeferred: true,
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      deferredAt,
		Event:   deferredCompletionPhase,
		Message: "preserved completed result for " + issueLabel(running.Issue) + " while waiting on the tracker lane fence",
	})
	if !record.Persisted {
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      deferredAt,
			Event:   "completion_deferral_persist_failed",
			Message: "retained completed result in memory for " + issueLabel(running.Issue) + " after durable persistence failed",
		})
	}
}

func (o *Orchestrator) completionFenceRetryAt(state *State, err error, now time.Time, retryDelay time.Duration) time.Time {
	dueAt := now.Add(max(retryDelay, o.cfg.PollInterval, time.Second))
	var statusErr *github.StatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		if retryAt := now.Add(statusErr.RetryAfter); retryAt.After(dueAt) {
			dueAt = retryAt
		}
		if statusErr.ResetAt.After(dueAt) {
			dueAt = statusErr.ResetAt
		}
	}
	if signal, ok := o.currentGitHubLookupSignal(state, now); ok && signal.resetAt.After(dueAt) {
		dueAt = signal.resetAt
	}
	if reporter, ok := o.connector.(connector.RateLimitReporter); ok {
		if rate, known := reporter.GraphQLRateLimit(); known && (rate.Remaining <= o.cfg.GitHubGraphQLMinReserve || rate.RetryAfter > 0) {
			if rate.ResetAt.After(dueAt) {
				dueAt = rate.ResetAt
			}
			if retryAt := now.Add(rate.RetryAfter); retryAt.After(dueAt) {
				dueAt = retryAt
			}
		}
	}
	return dueAt
}

func (o *Orchestrator) persistDeferredCompletion(ctx context.Context, state *State, record deferredCompletion) bool {
	if o == nil || o.workAttempts == nil || record.Running.WorkAttemptID <= 0 {
		return true
	}
	metadata, err := deferredCompletionMetadataJSON(record)
	if err != nil {
		if o.logger != nil {
			o.logger.Error("deferred completion serialization failed", "attempt_id", record.Running.WorkAttemptID, "issue_id", record.Running.Issue.ID, "error", err)
		}
		return false
	}
	heartbeat := store.WorkAttemptHeartbeat{
		AttemptID:              record.Running.WorkAttemptID,
		HeartbeatAt:            record.DeferredAt,
		Phase:                  deferredCompletionPhase,
		StatusMessage:          deferredCompletionStatus,
		WaitReason:             connector.TrackerUnavailableCondition,
		GitHubRateSnapshotJSON: o.githubRateSnapshotJSON(state),
		CIState:                workAttemptCIState(record.Running.Issue),
		CapacitySnapshotJSON:   o.capacitySnapshotJSON(state, record.Running.Issue),
		WorkerMetadataJSON:     metadata,
		MetricsJSON:            runningWorkAttemptMetricsJSON(record.Running),
		NextAction:             deferredCompletionNextAction,
		ErrorClass:             connector.TrackerUnavailableCondition,
		ErrorMessage:           record.Availability.Message,
		DetentSessionID:        record.Running.DetentSessionID,
		ProviderSessionID:      record.Running.SessionID,
		RuntimeIdentity:        record.Running.RuntimeIdentity,
	}
	if err := o.workAttempts.RecordWorkAttemptHeartbeat(ctx, heartbeat); err != nil {
		if o.logger != nil {
			o.logger.Error("deferred completion persistence failed", "attempt_id", record.Running.WorkAttemptID, "issue_id", record.Running.Issue.ID, "error", err)
		}
		return false
	}
	o.applyWorkAttemptHeartbeatSnapshot(state, record.Running.WorkAttemptID, heartbeat, nil)
	return true
}

func deferredCompletionMetadataJSON(record deferredCompletion) (string, error) {
	metadata := map[string]any{
		"run_mode":                    strings.TrimSpace(record.Running.Mode),
		"issue_title":                 strings.TrimSpace(record.Running.Issue.Title),
		"work_product_pushed":         record.Running.WorkProductPushed,
		deferredCompletionMetadataKey: record,
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("marshal deferred completion: %w", err)
	}
	return string(payload), nil
}

func decodeDeferredCompletion(attempt store.WorkAttempt) (deferredCompletion, error) {
	var metadata struct {
		DeferredCompletion json.RawMessage `json:"deferred_completion"`
	}
	if err := json.Unmarshal([]byte(attempt.WorkerMetadataJSON), &metadata); err != nil {
		return deferredCompletion{}, fmt.Errorf("decode work attempt metadata: %w", err)
	}
	if len(metadata.DeferredCompletion) == 0 {
		return deferredCompletion{}, errors.New("deferred completion metadata is missing")
	}
	var record deferredCompletion
	if err := json.Unmarshal(metadata.DeferredCompletion, &record); err != nil {
		return deferredCompletion{}, fmt.Errorf("decode deferred completion: %w", err)
	}
	if record.Schema != deferredCompletionSchema {
		return deferredCompletion{}, fmt.Errorf("deferred completion schema = %d, want %d", record.Schema, deferredCompletionSchema)
	}
	if strings.TrimSpace(record.Running.Issue.ID) == "" {
		return deferredCompletion{}, errors.New("deferred completion issue_id is required")
	}
	if record.Running.WorkAttemptID != attempt.ID {
		return deferredCompletion{}, fmt.Errorf("deferred completion attempt_id = %d, want %d", record.Running.WorkAttemptID, attempt.ID)
	}
	record.Running.CompletionOwnershipReleased = true
	record.Persisted = true
	return record, nil
}

func (o *Orchestrator) recoverDeferredCompletions(ctx context.Context, state *State, attempts []store.WorkAttempt, now time.Time) {
	for _, attempt := range attempts {
		if strings.TrimSpace(attempt.Phase) != deferredCompletionPhase {
			continue
		}
		record, err := decodeDeferredCompletion(attempt)
		if err != nil {
			o.invalidateDeferredCompletion(ctx, state, attempt, now, err)
			continue
		}
		issueID := record.Running.Issue.ID
		state.deferredCompletions[issueID] = record
		dueAt := now
		if record.FenceRetryAt.IsZero() {
			record.FenceRetryAt = record.DeferredAt.Add(o.cfg.PollInterval)
		}
		if candidate := record.FenceRetryAt; candidate.After(dueAt) {
			dueAt = candidate
		}
		state.Retry[issueID] = Retry{
			Issue:              cloneIssue(record.Running.Issue),
			Attempt:            record.Running.Attempt,
			DueAt:              dueAt,
			Error:              deferredCompletionStatus,
			WorkerHost:         record.Running.WorkerHost,
			TrackerUnavailable: true,
			CompletionDeferred: true,
		}
		state.Claimed[issueID] = recoveredDeferredCompletionClaim(o, record.Running.Issue, now)
		o.upsertWorkAttemptSnapshot(state, telemetryWorkAttempt(attempt, now))
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      now,
			Event:   "completion_deferral_recovered",
			Message: "recovered tracker-fenced completion for " + issueLabel(record.Running.Issue),
		})
	}
}

func recoveredDeferredCompletionClaim(o *Orchestrator, issue connector.Issue, now time.Time) Claimed {
	if o != nil && o.cfg.Claiming.Enabled {
		if claim, ok := o.verifiedClaim(issue, o.claimOwner()); ok {
			return claim
		}
	}
	return Claimed{Issue: cloneIssue(issue), ClaimedAt: now}
}

func (o *Orchestrator) invalidateDeferredCompletion(ctx context.Context, state *State, attempt store.WorkAttempt, now time.Time, cause error) {
	completion := store.WorkAttemptCompletion{
		AttemptID:          attempt.ID,
		CompletedAt:        now,
		Status:             store.WorkAttemptStatusTerminal,
		TerminalState:      store.WorkAttemptTerminalAbandoned,
		ErrorClass:         deferredCompletionInvalidClass,
		ErrorMessage:       cause.Error(),
		Phase:              "recovered",
		StatusMessage:      "invalid deferred completion was abandoned",
		WorkerMetadataJSON: attempt.WorkerMetadataJSON,
		MetricsJSON:        attempt.MetricsJSON,
		NextAction:         "inspect work attempt",
	}
	if err := o.workAttempts.CompleteWorkAttempt(ctx, completion); err != nil {
		if o.logger != nil {
			o.logger.Error("invalid deferred completion abandonment failed", "attempt_id", attempt.ID, "issue_id", attempt.IssueID, "error", err)
		}
		return
	}
	if o.logger != nil {
		o.logger.Error("invalid deferred completion abandoned", "attempt_id", attempt.ID, "issue_id", attempt.IssueID, "error", cause)
	}
	o.applyWorkAttemptCompletionSnapshot(state, Running{Issue: recoveryIssueFromStoreAttempt(attempt), WorkAttemptID: attempt.ID, Attempt: attempt.AttemptNumber, StartedAt: attempt.StartedAt}, completion)
}

func recoveryIssueFromStoreAttempt(attempt store.WorkAttempt) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = attempt.IssueID
	issue.Identifier = attempt.Identifier
	issue.URL = attempt.IssueURL
	issue.State = attempt.Lane
	return issue
}

func (o *Orchestrator) retryDeferredCompletions(ctx context.Context, state *State, now time.Time) bool {
	for _, issueID := range sortedKeys(state.deferredCompletions) {
		retry := state.Retry[issueID]
		if !retry.DueAt.IsZero() && now.Before(retry.DueAt) {
			continue
		}
		record := state.deferredCompletions[issueID]
		if !record.Persisted {
			record.DeferredAt = now
			if !o.persistDeferredCompletion(ctx, state, record) {
				retry.DueAt = now.Add(o.cfg.PollInterval)
				state.Retry[issueID] = retry
				return false
			}
			record.Persisted = true
			state.deferredCompletions[issueID] = record
		}
		delete(state.deferredCompletions, issueID)
		delete(state.Retry, issueID)
		state.Running[issueID] = record.Running
		if _, ok := state.Claimed[issueID]; !ok {
			state.Claimed[issueID] = recoveredDeferredCompletionClaim(o, record.Running.Issue, now)
		}
		o.handleRunResult(ctx, state, record.completion())
		if _, deferred := state.deferredCompletions[issueID]; deferred {
			return false
		}
	}
	return true
}

func cloneDeferredCompletions(source map[string]deferredCompletion) map[string]deferredCompletion {
	cloned := make(map[string]deferredCompletion, len(source))
	for issueID, record := range source {
		record.Running.Issue = cloneIssue(record.Running.Issue)
		record.Running.LastMessageTruncation = runtimeoutput.CloneTruncation(record.Running.LastMessageTruncation)
		record.Running.RecentEvents = cloneActivityEvents(record.Running.RecentEvents)
		record.Running.StopPriorityOptions = append([]telemetry.StopRunPriorityOption(nil), record.Running.StopPriorityOptions...)
		record.Request.Issue = cloneIssue(record.Request.Issue)
		record.Request.MergePrecheck = cloneMergePrecheck(record.Request.MergePrecheck)
		record.Result.RateLimits = cloneRateLimits(record.Result.RateLimits)
		cloned[issueID] = record
	}
	return cloned
}
