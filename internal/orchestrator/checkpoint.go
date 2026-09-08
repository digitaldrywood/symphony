package orchestrator

import (
	"context"
	"fmt"

	"github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

func (o *Orchestrator) checkpointValidator(issueID string, attemptID int64, generation uint64) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		runtime := o.latestRuntimeState.Load()
		if runtime == nil {
			return runner.ErrExecutionAuthorityUnavailable
		}
		running, ok := runtime.Running[issueID]
		if !ok || running.WorkAttemptID != attemptID || running.Generation != generation || running.Mode != runner.RunModeImplement {
			return runner.ErrExecutionAuthorityUnavailable
		}
		if o.workAttempts == nil || o.connector == nil {
			return runner.ErrExecutionAuthorityUnavailable
		}
		attempt, err := o.workAttempts.WorkAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
		if attempt.IssueID != issueID || attempt.Status != store.WorkAttemptStatusActive || !attempt.LeaseExpiresAt.After(o.clockNow()) {
			return runner.ErrExecutionAuthorityUnavailable
		}
		issue, err := o.refreshCompletionLane(ctx, running)
		if err != nil {
			return err
		}
		if normalizeState(issue.State) != normalizeState(running.Issue.State) {
			return fmt.Errorf("%w: checkpoint lane changed", runner.ErrExecutionAuthorityUnavailable)
		}
		current := o.latestRuntimeState.Load()
		if current == nil {
			return runner.ErrExecutionAuthorityUnavailable
		}
		owned, ok := current.Running[issueID]
		if !ok || owned.WorkAttemptID != attemptID || owned.Generation != generation || owned.Mode != runner.RunModeImplement || !attempt.LeaseExpiresAt.After(o.clockNow()) {
			return runner.ErrExecutionAuthorityUnavailable
		}
		return ctx.Err()
	}
}
