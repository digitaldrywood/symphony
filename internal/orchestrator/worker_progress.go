package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/store"
)

type workerProgress struct {
	mu          sync.Mutex
	latest      atomic.Pointer[Running]
	persisted   atomic.Pointer[store.WorkAttemptHeartbeat]
	closed      bool
	base        store.WorkAttemptHeartbeat
	attempts    store.WorkAttemptStore
	leaseTTL    time.Duration
	outputLimit int
}

func newWorkerProgress(running Running, heartbeat store.WorkAttemptHeartbeat, attempts store.WorkAttemptStore, outputLimit int) *workerProgress {
	running.Issue = cloneIssue(running.Issue)
	running.progress = nil
	progress := &workerProgress{
		base:        heartbeat,
		attempts:    attempts,
		leaseTTL:    heartbeat.LeaseExpiresAt.Sub(heartbeat.HeartbeatAt),
		outputLimit: outputLimit,
	}
	progress.latest.Store(&running)
	return progress
}

func applyRunningUsage(running Running, update runpkg.UsageUpdate) Running {
	if update.DispatchLoopStart != nil {
		running.DispatchLoopStart = dispatchLoopStartRecordFromSnapshot(running, *update.DispatchLoopStart)
	}

	if update.SessionID != "" {
		running.SessionID = update.SessionID
	}
	if update.DetentSessionID > 0 {
		running.DetentSessionID = update.DetentSessionID
	}
	if !update.RuntimeIdentity.IsZero() {
		running.RuntimeIdentity = running.RuntimeIdentity.Merge(update.RuntimeIdentity)
	}
	if strings.TrimSpace(update.WorkerGitHubActor.Login) != "" {
		running.WorkerGitHubActor = update.WorkerGitHubActor
	}
	if update.TurnCount > 0 {
		running.TurnCount = update.TurnCount
	}
	if !update.LastEventAt.IsZero() {
		running.LastEventAt = update.LastEventAt
	}
	if update.LastEvent != "" {
		running.LastEvent = update.LastEvent
	}
	if update.LastCommand != "" {
		running.LastCommand = update.LastCommand
	}
	if update.LastMessage != "" {
		running.LastMessage = update.LastMessage
		running.LastMessageTruncation = runtimeoutput.CloneTruncation(update.LastMessageTruncation)
	}
	if len(update.RecentEvents) > 0 {
		running.RecentEvents = cloneActivityEvents(update.RecentEvents)
	}
	if update.ProcessIdentity != "" {
		running.ProcessIdentity = update.ProcessIdentity
	}
	if update.WorkerProcess.PID > 0 && !update.WorkerProcess.StartedAt.IsZero() {
		running.WorkerProcess = update.WorkerProcess
	}
	if update.WorkspacePath != "" {
		running.WorkspacePath = update.WorkspacePath
	}
	if update.WorkProductPushed {
		running.WorkProductPushed = true
	}
	if diffStatsPresent(update.DiffStats) {
		running.DiffStats = update.DiffStats
	}
	if !update.RSSObservedAt.IsZero() {
		running.RSSBytes = update.RSSBytes
		running.RSSCeilingBytes = update.RSSCeilingBytes
		running.RSSObservedAt = update.RSSObservedAt
	}
	running.Tokens = update.Tokens
	return running
}

func (r Running) withProgress() Running {
	if r.progress == nil {
		return r
	}
	p := r.progress.latest.Load()
	if p == nil {
		return r
	}
	r = applyRunningUsage(r, runpkg.UsageUpdate{
		SessionID:             p.SessionID,
		DetentSessionID:       p.DetentSessionID,
		RuntimeIdentity:       p.RuntimeIdentity,
		WorkerGitHubActor:     p.WorkerGitHubActor,
		TurnCount:             p.TurnCount,
		LastEventAt:           p.LastEventAt,
		LastEvent:             p.LastEvent,
		LastCommand:           p.LastCommand,
		LastMessage:           p.LastMessage,
		LastMessageTruncation: p.LastMessageTruncation,
		RecentEvents:          p.RecentEvents,
		ProcessIdentity:       p.ProcessIdentity,
		WorkerProcess:         p.WorkerProcess,
		WorkspacePath:         p.WorkspacePath,
		WorkProductPushed:     p.WorkProductPushed,
		DiffStats:             p.DiffStats,
		RSSObservedAt:         p.RSSObservedAt,
		RSSBytes:              p.RSSBytes,
		RSSCeilingBytes:       p.RSSCeilingBytes,
		Tokens:                p.Tokens,
	})
	r.DispatchLoopStart = p.DispatchLoopStart
	return r
}

func (p *workerProgress) observe(ctx context.Context, update runpkg.UsageUpdate) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.closed {
		return context.Canceled
	}
	running := applyRunningUsage(*p.latest.Load(), update)
	persistCheckpoint := update.DispatchLoopStart != nil && p.attempts != nil && running.WorkAttemptID > 0
	visible := running
	if persistCheckpoint {
		visible.DispatchLoopStart.Persisted = false
	}
	p.latest.Store(&visible)
	if !persistCheckpoint {
		return nil
	}
	operationCtx, cancel := context.WithTimeout(ctx, heartbeatOperationTimeout)
	defer cancel()
	now := time.Now()
	base := p.base
	base.LeaseExpiresAt = now.Add(p.leaseTTL)
	heartbeat := p.heartbeat(base, now)
	heartbeat.WorkerMetadataJSON = runningWorkAttemptMetadataJSON(running, nil)
	if err := p.attempts.RecordWorkAttemptHeartbeat(operationCtx, heartbeat); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			p.closed = true
		}
		return err
	}
	p.latest.Store(&running)
	p.persisted.Store(&heartbeat)
	return nil
}

func (p *workerProgress) heartbeat(base store.WorkAttemptHeartbeat, now time.Time) store.WorkAttemptHeartbeat {
	running := p.latest.Load()
	base.HeartbeatAt = now
	if base.Phase != "backoff" {
		base.Phase = runningWorkAttemptPhase(*running, nil)
	}
	message := firstNonBlank(running.LastMessage, running.LastEvent, "worker running")
	base.StatusMessage = runtimeoutput.Truncate(strings.TrimSpace(message), p.outputLimit).Value
	base.WorkerMetadataJSON = runningWorkAttemptMetadataJSON(*running, nil)
	base.MetricsJSON = runningWorkAttemptMetricsJSON(*running)
	base.NextAction = runningWorkAttemptNextAction(*running, base.Phase)
	base.DetentSessionID = running.DetentSessionID
	base.ProviderSessionID = running.SessionID
	base.RuntimeIdentity = running.RuntimeIdentity
	return base
}

func (p *workerProgress) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func (s *State) syncWorkerProgress() {
	if s == nil {
		return
	}
	for issueID, running := range s.Running {
		s.Running[issueID] = running.withProgress()
	}
}
