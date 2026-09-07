package hubclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type nativeExecution struct {
	artifacts nativeArtifacts
	scheduler *Scheduler
	claim     nativeClaim
	mu        sync.Mutex
	data      tracker.NativeRunData
	pending   *tracker.NativeRunEvent
	cancel    context.CancelCauseFunc
}

type nativeMutationAuthorityKey struct{}

type nativeMutationAuthority struct {
	scope string
	lease tracker.NativeLease
}

func executionID(prefix, value string) string {
	digest := sha256.Sum256([]byte(value))
	return prefix + "_" + hex.EncodeToString(digest[:16])
}

func (c *NativeClient) fencedMutation(ctx context.Context, id tracker.NativeWorkItemID, mutation tracker.Mutation) tracker.Mutation {
	if mutation.LeaseID != "" {
		return mutation
	}
	if authority, ok := ctx.Value(nativeMutationAuthorityKey{}).(nativeMutationAuthority); ok && authority.scope == c.base() && authority.lease.WorkItemID == id {
		mutation.LeaseID, mutation.FencingToken = authority.lease.ID, authority.lease.FencingToken
		return mutation
	}
	if value, ok := c.client.nativeLeases.Load(c.base() + "/" + string(id)); ok {
		if lease, ok := value.(tracker.NativeLease); ok {
			mutation.LeaseID, mutation.FencingToken = lease.ID, lease.FencingToken
		}
	}
	return mutation
}

func (s *Scheduler) RunExecution(issueID string) runner.Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	claim, ok := s.nativeClaims[issueID]
	if !ok {
		if strings.HasPrefix(issueID, "wi_") {
			return &nativeExecution{scheduler: s, claim: nativeClaim{lease: tracker.NativeLease{WorkItemID: tracker.NativeWorkItemID(issueID)}}}
		}
		return nil
	}
	return &nativeExecution{scheduler: s, claim: claim, data: tracker.NativeRunData{
		RunID: executionID("run", string(claim.lease.WorkItemID)), AttemptID: executionID("attempt", string(claim.lease.ID)),
		PolicyID: claim.lease.PolicyID, LeaseID: claim.lease.ID, FencingToken: claim.lease.FencingToken,
	}}
}

func (e *nativeExecution) Recovery() tracker.NativeRecovery {
	recovery := e.claim.recovery
	recovery.Lease = e.claim.lease
	return recovery
}

func (e *nativeExecution) remaining() time.Duration {
	e.scheduler.mu.Lock()
	defer e.scheduler.mu.Unlock()
	claim, ok := e.scheduler.nativeClaims[string(e.claim.lease.WorkItemID)]
	if !ok || claim.lease.FencingToken != e.claim.lease.FencingToken {
		return 0
	}
	margin := min(5*time.Second, e.scheduler.leaseTTL/10)
	return claim.deadline.Sub(e.scheduler.now()) - margin
}

func (e *nativeExecution) Guard(ctx context.Context) (context.Context, func(), error) {
	if err := e.Validate(ctx); err != nil {
		return ctx, func() {}, err
	}
	bound := context.WithValue(ctx, nativeMutationAuthorityKey{}, nativeMutationAuthority{scope: e.claim.source.client.base(), lease: e.claim.lease})
	guarded, cancel := context.WithCancelCause(bound)
	e.mu.Lock()
	e.cancel = cancel
	e.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			remaining := e.remaining()
			if remaining <= 0 {
				cancel(runner.ErrExecutionAuthorityUnavailable)
				return
			}
			timer := time.NewTimer(remaining)
			select {
			case <-guarded.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return guarded, func() { cancel(context.Canceled); <-done }, nil
}

func (e *nativeExecution) unavailable(err error) error {
	cause := errors.Join(runner.ErrExecutionAuthorityUnavailable, err)
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	return cause
}

func (e *nativeExecution) Validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return e.unavailable(err)
	}
	if e.remaining() <= 0 {
		return e.unavailable(nil)
	}
	if err := e.scheduler.checkClaimPolicy(ctx, string(e.claim.lease.WorkItemID), e.claim.lease.PolicyID); err != nil {
		return e.unavailable(err)
	}
	if e.scheduler.client.runner == nil {
		if _, err := e.claim.source.client.ValidateLease(ctx, e.claim.lease); err != nil {
			return e.unavailable(err)
		}
	}
	return nil
}

func (e *nativeExecution) Start(ctx context.Context, identity tracker.NativeExecutionIdentity) error {
	e.mu.Lock()
	if e.data.Identity != nil && e.data.Sequence > 0 {
		defer e.mu.Unlock()
		if *e.data.Identity != identity {
			return errors.New("native execution identity changed during an attempt")
		}
		return e.flush(ctx)
	}
	e.mu.Unlock()
	if err := e.Validate(ctx); err != nil {
		return err
	}
	if err := e.validateProviderStart(identity); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.data.Identity != nil {
		if *e.data.Identity != identity {
			return errors.New("native execution identity changed during an attempt")
		}
		return e.flush(ctx)
	}
	e.data.Identity = &identity
	return e.append(ctx, "run.started", "", nil)
}

func (e *nativeExecution) Checkpoint(ctx context.Context, checkpoint tracker.NativeCheckpoint) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.data.Identity == nil {
		return nil
	}
	if err := e.flush(ctx); err != nil {
		return err
	}
	previous, err := json.Marshal(e.data.Handoff)
	if err != nil {
		return err
	}
	current, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	if string(previous) == string(current) {
		return nil
	}
	return e.append(ctx, "run.checkpointed", "", &checkpoint)
}

func (e *nativeExecution) Finish(ctx context.Context, outcome string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.flush(ctx); err != nil {
		return err
	}
	if e.data.Identity == nil || e.data.Outcome != "" {
		return nil
	}
	return e.append(ctx, "run.finished", outcome, nil)
}

func (e *nativeExecution) append(ctx context.Context, kind, outcome string, checkpoint *tracker.NativeCheckpoint) error {
	if err := e.flush(ctx); err != nil {
		return err
	}
	data := e.data
	data.Sequence++
	data.Outcome = outcome
	data.Handoff = checkpoint
	e.pending = &tracker.NativeRunEvent{Mutation: tracker.Mutation{IdempotencyKey: data.AttemptID + ":" + strconv.FormatInt(data.Sequence, 10)}, Type: kind, SchemaVersion: 1, Data: data}
	return e.flush(ctx)
}

func (e *nativeExecution) flush(ctx context.Context) error {
	if e.pending == nil {
		return nil
	}
	if e.remaining() <= 0 {
		return runner.ErrExecutionAuthorityUnavailable
	}
	if err := e.claim.source.client.AppendEvent(ctx, e.claim.lease.WorkItemID, *e.pending); err != nil {
		return errors.Join(runner.ErrExecutionAuthorityUnavailable, err)
	}
	e.data = e.pending.Data
	e.pending = nil
	return nil
}
