package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestReapWorkerProcessesRetriesNonEmptyArtifacts(t *testing.T) {
	t.Parallel()

	textErr := errors.New("directory not empty")
	tests := []struct {
		name         string
		failures     int
		cleanupErr   error
		cancelBefore bool
		cancelAfter  time.Duration
		wantErr      error
		wantAttempts int
		wantWait     time.Duration
	}{
		{name: "clean first attempt", wantAttempts: 1},
		{name: "transient non-empty", failures: 1, cleanupErr: syscall.ENOTEMPTY, wantAttempts: 2, wantWait: 250 * time.Millisecond},
		{name: "last attempt succeeds", failures: 4, cleanupErr: syscall.ENOTEMPTY, wantAttempts: 5, wantWait: 3750 * time.Millisecond},
		{name: "persistent non-empty", failures: 10, cleanupErr: syscall.ENOTEMPTY, wantErr: syscall.ENOTEMPTY, wantAttempts: 5, wantWait: 3750 * time.Millisecond},
		{name: "permission denied", failures: 1, cleanupErr: os.ErrPermission, wantErr: os.ErrPermission, wantAttempts: 1},
		{name: "unrelated failure", failures: 1, cleanupErr: syscall.EIO, wantErr: syscall.EIO, wantAttempts: 1},
		{name: "untyped non-empty message", failures: 1, cleanupErr: textErr, wantErr: textErr, wantAttempts: 1},
		{name: "canceled before cleanup", cancelBefore: true, wantErr: context.Canceled},
		{name: "cancel during backoff", failures: 10, cleanupErr: syscall.ENOTEMPTY, cancelAfter: 100 * time.Millisecond, wantErr: context.DeadlineExceeded, wantAttempts: 1, wantWait: 100 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				scratch := filepath.Join(root, ".detent", "tmp")
				if err := os.MkdirAll(scratch, 0o700); err != nil {
					t.Fatal(err)
				}
				retained := filepath.Join(root, "work.txt")
				if err := os.WriteFile(retained, []byte("retained user work"), 0o600); err != nil {
					t.Fatal(err)
				}
				startedAt := time.Now()
				process := store.WorkerProcess{
					SessionID: 2193, Identifier: "digitaldrywood/detent#2193",
					WorkerProcessIdentity: store.WorkerProcessIdentity{PID: 4242, GroupID: 4242, StartedAt: startedAt},
					CleanupRoot:           root, CleanupPath: scratch,
				}
				processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{process}}
				var logs bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&logs, nil))
				ctx := t.Context()
				if tt.cancelBefore {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if tt.cancelAfter > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tt.cancelAfter)
					defer cancel()
				}
				attempts, reaps := 0, 0
				err := reapWorkerProcessesWithCleanup(ctx, processStore, logger, "startup", time.Millisecond, time.Now,
					func(_ context.Context, identity procgroup.Identity, _ time.Duration) (procgroup.TerminationOutcome, error) {
						reaps++
						if identity.PID != process.PID || identity.GroupID != process.GroupID || !identity.StartedAt.Equal(startedAt) {
							t.Fatalf("process identity changed: %#v", identity)
						}
						return procgroup.TerminationOutcomeAlreadyExited, nil
					},
					func(process store.WorkerProcess) error {
						attempts++
						if len(processStore.reaped) != 0 {
							t.Fatal("process marked reaped before artifacts were removed")
						}
						if attempts <= tt.failures {
							if err := os.WriteFile(filepath.Join(scratch, "late-cache"), []byte("scratch"), 0o600); err != nil {
								t.Fatal(err)
							}
							var cleanupErr error = &os.PathError{Op: "unlinkat", Path: scratch, Err: tt.cleanupErr}
							if errors.Is(tt.cleanupErr, syscall.ENOTEMPTY) && runtime.GOOS != "windows" {
								cleanupErr = os.Remove(scratch)
								if !errors.Is(cleanupErr, syscall.ENOTEMPTY) {
									t.Fatalf("remove repopulated scratch = %v, want ENOTEMPTY", cleanupErr)
								}
							}
							return fmt.Errorf("clean worker process artifacts: remove owned path: %w", cleanupErr)
						}
						return cleanupWorkerProcessArtifacts(process)
					})
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("recovery error = %v, want %v", err, tt.wantErr)
				}
				if attempts != tt.wantAttempts || reaps != 1 {
					t.Fatalf("cleanup attempts = %d, reaps = %d; want %d, 1", attempts, reaps, tt.wantAttempts)
				}
				if elapsed := time.Since(startedAt); elapsed != tt.wantWait {
					t.Fatalf("retry wait = %s, want %s", elapsed, tt.wantWait)
				}
				if got := len(processStore.reaped); got != 0 && tt.wantErr != nil || got != 1 && tt.wantErr == nil {
					t.Fatalf("reaped records = %#v, error = %v", processStore.reaped, err)
				}
				if tt.wantErr == nil {
					reaped := processStore.reaped[0]
					if reaped.reap.Outcome != "already_exited" || reaped.reap.Reason != "startup" || !reaped.reap.ReapedAt.Equal(time.Now()) {
						t.Fatalf("incorrect reap bookkeeping: %#v", reaped)
					}
				}
				_, statErr := os.Stat(scratch)
				if tt.wantErr == nil && !errors.Is(statErr, os.ErrNotExist) || tt.wantErr != nil && statErr != nil {
					t.Fatalf("scratch stat error = %v, recovery error = %v", statErr, err)
				}
				if data, err := os.ReadFile(retained); err != nil || string(data) != "retained user work" {
					t.Fatalf("retained work = %q, error = %v", data, err)
				}
				if count := strings.Count(logs.String(), "worker process lifecycle decision"); count != 1 {
					t.Fatalf("lifecycle decision count = %d, logs:\n%s", count, logs.String())
				}
				if tt.wantAttempts > 1 {
					for _, want := range []string{"worker artifact cleanup retry", "reason=startup", "decision=already_exited", "issue_identifier=digitaldrywood/detent#2193", "cleanup_path=", "cleanup_attempt=1", "cleanup_max_attempts=5", "retry_delay=250ms"} {
						if !strings.Contains(logs.String(), want) {
							t.Fatalf("missing retry diagnostic %q:\n%s", want, logs.String())
						}
					}
				}
				if tt.cancelAfter > 0 && !errors.Is(err, syscall.ENOTEMPTY) {
					t.Fatalf("cancellation lost cleanup error: %v", err)
				}
				if errors.Is(tt.wantErr, syscall.ENOTEMPTY) && !strings.Contains(err.Error(), "5 attempts") {
					t.Fatalf("missing exhaustion diagnostics: %v", err)
				}
			})
		})
	}
}

func TestReapWorkerProcessesRevalidatesArtifactsAfterRetry(t *testing.T) {
	t.Parallel()

	for _, parent := range []bool{false, true} {
		t.Run(fmt.Sprintf("parent_symlink_%t", parent), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root, outside := t.TempDir(), t.TempDir()
				scratch := filepath.Join(root, ".detent", "tmp")
				if err := os.MkdirAll(scratch, 0o700); err != nil {
					t.Fatal(err)
				}
				outsideScratch := filepath.Join(outside, "tmp")
				if err := os.Mkdir(outsideScratch, 0o700); err != nil {
					t.Fatal(err)
				}
				retained := filepath.Join(outsideScratch, "work.txt")
				if err := os.WriteFile(retained, []byte("retained"), 0o600); err != nil {
					t.Fatal(err)
				}
				process := store.WorkerProcess{SessionID: 2193, CleanupRoot: root, CleanupPath: scratch}
				processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{process}}
				attempts := 0
				err := reapWorkerProcessesWithCleanup(t.Context(), processStore, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), "startup", time.Millisecond, time.Now,
					func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error) {
						return procgroup.TerminationOutcomeAlreadyExited, nil
					}, func(process store.WorkerProcess) error {
						attempts++
						if attempts == 1 {
							target, replacement := scratch, outsideScratch
							if parent {
								target, replacement = filepath.Dir(scratch), outside
							}
							if err := os.Rename(target, filepath.Join(root, "previous")); err != nil {
								t.Fatal(err)
							}
							if err := os.Symlink(replacement, target); err != nil {
								if runtime.GOOS == "windows" {
									t.Skipf("symlinks unavailable: %v", err)
								}
								t.Fatal(err)
							}
							return &os.PathError{Op: "unlinkat", Path: scratch, Err: syscall.ENOTEMPTY}
						}
						return cleanupWorkerProcessArtifacts(process)
					})
				var pathErr *workspace.PathError
				if !errors.As(err, &pathErr) || attempts != 2 || len(processStore.reaped) != 0 {
					t.Fatalf("recovery error = %v, attempts = %d, reaped = %#v", err, attempts, processStore.reaped)
				}
				if data, err := os.ReadFile(retained); err != nil || string(data) != "retained" {
					t.Fatalf("outside work = %q, error = %v", data, err)
				}
			})
		})
	}
}

func TestReapWorkerProcessesPreservesInterruptedSession(t *testing.T) {
	t.Parallel()

	for _, failures := range []int{1, 5} {
		t.Run(fmt.Sprintf("cleanup_failures_%d", failures), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			backend, err := store.Open(ctx, store.Config{Path: filepath.Join(t.TempDir(), "recovery.db")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { backend.Close() })
			startedAt := time.Date(2026, 9, 7, 16, 33, 55, 0, time.UTC)
			attemptID, err := backend.StartWorkAttempt(ctx, store.WorkAttemptStart{
				ProjectID: "detent", IssueID: "issue-2193", Identifier: "digitaldrywood/detent#2193",
				WorkerType: "agent", Lane: "In Progress", AttemptNumber: 1, StartedAt: startedAt,
			})
			if err != nil {
				t.Fatal(err)
			}
			sessionID, err := backend.StartSession(ctx, store.SessionStart{
				WorkAttemptID: attemptID, ProjectID: "detent", IssueID: "issue-2193", Identifier: "digitaldrywood/detent#2193", StartedAt: startedAt,
				ProviderThreadID: "thread-2193", ProviderSessionID: "turn-2193", AgentBackendKind: "codex", AgentRole: "code",
			})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			scratch := filepath.Join(root, ".detent", "tmp")
			if err := os.MkdirAll(scratch, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := backend.UpdateSessionWorkerProcess(ctx, sessionID, store.WorkerProcessRegistration{
				WorkerProcessIdentity: store.WorkerProcessIdentity{PID: 4242, GroupID: 4242, StartedAt: startedAt},
				CleanupRoot:           root, CleanupPath: scratch,
			}); err != nil {
				t.Fatal(err)
			}
			logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
			reap := func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error) {
				return procgroup.TerminationOutcomeAlreadyExited, nil
			}
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				err := reapWorkerProcessesWithCleanup(t.Context(), backend, logger, "startup", time.Millisecond, time.Now, reap, func(process store.WorkerProcess) error {
					attempts++
					if attempts <= failures {
						return &os.PathError{Op: "unlinkat", Path: scratch, Err: syscall.ENOTEMPTY}
					}
					return cleanupWorkerProcessArtifacts(process)
				})
				if got := err != nil; got != (failures == 5) {
					t.Fatalf("recovery error = %v, failures = %d", err, failures)
				}
			})
			active, err := backend.ListActiveWorkerProcesses(ctx)
			wantActive := 0
			if failures == 5 {
				wantActive = 1
			}
			if err != nil || len(active) != wantActive {
				t.Fatalf("active worker processes = %#v, error = %v", active, err)
			}
			if failures == 5 {
				if err := reapWorkerProcesses(ctx, backend, logger, "startup", time.Millisecond, time.Now, reap); err != nil {
					t.Fatalf("later recovery failed: %v", err)
				}
			}
			session, err := backend.Queries().GetCodexSession(ctx, sessionID)
			if err != nil {
				t.Fatal(err)
			}
			if !session.WorkerReapedAt.Valid || session.WorkerReapOutcome.String != "already_exited" || session.WorkerReapReason.String != "startup" {
				t.Fatalf("incorrect persisted reap: %#v", session)
			}
			if session.CompletedAt.Valid || session.FinalState.String != store.SessionStateRunning {
				t.Fatalf("interrupted session incorrectly completed: %#v", session)
			}
			orphans, err := backend.ListOrphanedAgentSessions(ctx, "detent")
			if err != nil || len(orphans) != 1 {
				t.Fatalf("recoverable sessions = %#v, error = %v", orphans, err)
			}
			resume := orphans[0].ResumeState
			if resume.DetentSessionID != sessionID || resume.ProviderThreadID != "thread-2193" || resume.ProviderSessionID != "turn-2193" {
				t.Fatalf("incorrect resume state: %#v", resume)
			}
		})
	}
}

func TestReapWorkerProcessesCleansArtifactsOnlyAfterVerifiedExit(t *testing.T) {
	t.Parallel()

	reapErr := errors.New("process group remained alive")
	tests := []struct {
		name       string
		reapErr    error
		wantExists bool
		wantReaped bool
	}{
		{name: "verified exit", wantReaped: true},
		{name: "unverified exit", reapErr: reapErr, wantExists: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cleanupRoot := t.TempDir()
			cleanupPath := filepath.Join(cleanupRoot, "run-2011")
			if err := os.MkdirAll(filepath.Join(cleanupPath, ".detent", "tmp"), 0o700); err != nil {
				t.Fatalf("create cleanup path: %v", err)
			}
			processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{{
				SessionID: 2011,
				WorkerProcessIdentity: store.WorkerProcessIdentity{
					PID:       4242,
					GroupID:   4242,
					StartedAt: time.Date(2026, 8, 27, 12, 40, 0, 0, time.UTC),
				},
				CleanupRoot: cleanupRoot,
				CleanupPath: cleanupPath,
			}}}

			err := reapWorkerProcesses(
				context.Background(),
				processStore,
				slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
				"startup",
				time.Millisecond,
				time.Now,
				func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error) {
					return procgroup.TerminationOutcomeTerminated, tt.reapErr
				},
			)
			if got := errors.Is(err, reapErr); got != (tt.reapErr != nil) {
				t.Fatalf("reapWorkerProcesses() error = %v, want reap error %v", err, tt.reapErr != nil)
			}
			_, statErr := os.Stat(cleanupPath)
			if got := statErr == nil; got != tt.wantExists {
				t.Fatalf("cleanup path exists = %v, want %v, stat error = %v", got, tt.wantExists, statErr)
			}
			if got := len(processStore.reaped) == 1; got != tt.wantReaped {
				t.Fatalf("process marked reaped = %v, want %v", got, tt.wantReaped)
			}
		})
	}
}

func TestReapWorkerProcessesAtStartup(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC)
	reapedAt := startedAt.Add(time.Minute)
	processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{
		{
			SessionID:  1214,
			Identifier: "digitaldrywood/detent#1214",
			WorkerProcessIdentity: store.WorkerProcessIdentity{
				PID:       4242,
				GroupID:   4242,
				StartedAt: startedAt,
			},
		},
		{
			SessionID:  1215,
			Identifier: "digitaldrywood/detent#1215",
			WorkerProcessIdentity: store.WorkerProcessIdentity{
				PID:       4343,
				GroupID:   4343,
				StartedAt: startedAt,
			},
		},
	}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	err := reapWorkerProcesses(
		context.Background(),
		processStore,
		logger,
		"startup",
		time.Millisecond,
		func() time.Time { return reapedAt },
		func(_ context.Context, identity procgroup.Identity, _ time.Duration) (procgroup.TerminationOutcome, error) {
			if identity.PID == 4242 {
				return procgroup.TerminationOutcomeTerminated, nil
			}
			return procgroup.TerminationOutcomeAlreadyExited, nil
		},
	)
	if err != nil {
		t.Fatalf("reapWorkerProcesses() error = %v", err)
	}
	if len(processStore.reaped) != 2 {
		t.Fatalf("reaped processes = %#v", processStore.reaped)
	}
	if processStore.reaped[0].reap.Outcome != store.WorkerProcessOutcomeTerminated || processStore.reaped[1].reap.Outcome != store.WorkerProcessOutcomeAlreadyExited {
		t.Fatalf("reap outcomes = %#v", processStore.reaped)
	}
	for _, reaped := range processStore.reaped {
		if !reaped.reap.ReapedAt.Equal(reapedAt) {
			t.Fatalf("reaped at = %s, want %s", reaped.reap.ReapedAt, reapedAt)
		}
		if reaped.reap.Reason != "startup" {
			t.Fatalf("reap reason = %q, want startup", reaped.reap.Reason)
		}
	}
	for _, want := range []string{
		"reason=startup",
		"decision=terminated",
		"decision=already_exited",
		"issue_identifier=digitaldrywood/detent#1214",
		"issue_identifier=digitaldrywood/detent#1215",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %q:\n%s", want, logs.String())
		}
	}
}
