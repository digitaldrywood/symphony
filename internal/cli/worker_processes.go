package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"syscall"
	"time"

	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

type workerProcessStore interface {
	ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error)
	MarkSessionWorkerProcessReaped(context.Context, int64, store.WorkerProcessReap) error
}

type workerProcessReapFunc func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error)

func reapWorkerProcesses(
	ctx context.Context,
	processStore workerProcessStore,
	logger *slog.Logger,
	reason string,
	grace time.Duration,
	now func() time.Time,
	reap workerProcessReapFunc,
) error {
	return reapWorkerProcessesWithCleanup(ctx, processStore, logger, reason, grace, now, reap, cleanupWorkerProcessArtifacts)
}

func reapWorkerProcessesWithCleanup(
	ctx context.Context,
	processStore workerProcessStore,
	logger *slog.Logger,
	reason string,
	grace time.Duration,
	now func() time.Time,
	reap workerProcessReapFunc,
	cleanup func(store.WorkerProcess) error,
) error {
	if processStore == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	if reap == nil {
		reap = procgroup.Terminate
	}
	processes, err := processStore.ListActiveWorkerProcesses(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, process := range processes {
		identity := procgroup.Identity{
			PID:       process.PID,
			GroupID:   process.GroupID,
			StartedAt: process.StartedAt,
		}
		outcome, reapErr := reap(ctx, identity, grace)
		attrs := []any{
			"operation", "worker_process_reap",
			"reason", strings.TrimSpace(reason),
			"decision", string(outcome),
			"detent_session_id", process.SessionID,
			"issue_id", strings.TrimSpace(process.IssueID),
			"issue_identifier", strings.TrimSpace(process.Identifier),
			"pid", process.PID,
			"pgid", process.GroupID,
		}
		if reapErr != nil {
			attrs = append(attrs, "error", reapErr)
			logger.Info("worker process lifecycle decision", attrs...)
			result = errors.Join(result, reapErr)
			continue
		}
		if cleanupErr := retryWorkerProcessArtifactCleanup(ctx, process, logger.With(attrs...), cleanup); cleanupErr != nil {
			attrs = append(attrs, "error", cleanupErr)
			logger.Info("worker process lifecycle decision", attrs...)
			result = errors.Join(result, cleanupErr)
			continue
		}
		logger.Info("worker process lifecycle decision", attrs...)
		if err := processStore.MarkSessionWorkerProcessReaped(ctx, process.SessionID, store.WorkerProcessReap{
			ReapedAt: now().UTC(),
			Outcome:  string(outcome),
			Reason:   strings.TrimSpace(reason),
		}); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func retryWorkerProcessArtifactCleanup(ctx context.Context, process store.WorkerProcess, logger *slog.Logger, cleanup func(store.WorkerProcess) error) error {
	const maxAttempts = 5
	var cleanupErr error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(cleanupErr, err)
		}
		cleanupErr = cleanup(process)
		if cleanupErr == nil {
			return nil
		}
		if !errors.Is(cleanupErr, syscall.ENOTEMPTY) {
			return cleanupErr
		}
		if attempt == maxAttempts {
			return fmt.Errorf("clean worker process artifacts after %d attempts: %w", attempt, cleanupErr)
		}
		delay := 250 * time.Millisecond << (attempt - 1)
		logger.Warn("worker artifact cleanup retry",
			"cleanup_path", process.CleanupPath,
			"cleanup_attempt", attempt,
			"cleanup_max_attempts", maxAttempts,
			"retry_delay", delay,
			"error", cleanupErr,
		)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(cleanupErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func cleanupWorkerProcessArtifacts(process store.WorkerProcess) error {
	if err := workspace.CleanupOwnedPath(process.CleanupRoot, process.CleanupPath); err != nil {
		return fmt.Errorf("clean worker process artifacts: %w", err)
	}
	return nil
}
