package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/instancelock"
)

func TestValidationQueueCancellation(t *testing.T) {
	t.Parallel()
	for _, olderWaiter := range []bool{false, true} {
		t.Run(fmt.Sprintf("older_waiter=%t", olderWaiter), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), validationIntegrationTimeout)
			defer cancel()
			path := filepath.Join(t.TempDir(), "validation.lock")
			holder := acquireTestLock(t, path)
			if olderWaiter {
				older, err := registerValidationWaiter(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { older.lock.Close() })
			}
			waitingCtx, stopWaiting := context.WithCancel(ctx)
			defer stopWaiting()
			writer := newNotifyingWriter()
			done := startValidationWaiter(waitingCtx, path, writer)
			select {
			case <-writer.notified:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stopWaiting()
			select {
			case result := <-done:
				if !errors.Is(result.err, context.Canceled) || result.lock != nil {
					t.Fatalf("canceled waiter = %+v", result)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			inspection, err := instancelock.Inspect(path)
			if err != nil || inspection.Status != instancelock.StatusHeld {
				t.Fatalf("holder after waiter cancellation = %+v, %v", inspection, err)
			}
			next, err := registerValidationWaiter(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { next.lock.Close() })
			position, size, err := validationPosition(ctx, path, next.name)
			want := 1
			if olderWaiter {
				want = 2
			}
			if err != nil || position != want || size != want {
				t.Fatalf("queue after cancellation = (%d, %d, %v), want (%d, %d, nil)", position, size, err, want, want)
			}
			if err := holder.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidationQueueDeadlines(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"registration", "validation"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "validation.lock")
				heldPath := path
				if stage == "registration" {
					heldPath += ".queue.lock"
				}
				acquireTestLock(t, heldPath)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				var stderr bytes.Buffer
				lock, _, err := acquireValidationLock(ctx, path, &stderr)
				if !errors.Is(err, context.DeadlineExceeded) || lock != nil {
					t.Fatalf("deadline result = %v, %v", lock, err)
				}
				if !strings.Contains(err.Error(), "wait") {
					t.Fatalf("deadline lacks waiting context: %v", err)
				}
				inspection, err := instancelock.Inspect(heldPath)
				if err != nil || inspection.Status != instancelock.StatusHeld {
					t.Fatalf("deadline released live lock: %+v, %v", inspection, err)
				}
			})
		})
	}
}

func TestValidationQueueCanceledBeforeAcquisition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path := filepath.Join(t.TempDir(), "validation.lock")
	lock, _, err := acquireValidationLock(ctx, path, io.Discard)
	if !errors.Is(err, context.Canceled) || lock != nil {
		t.Fatalf("already canceled acquisition = %v, %v", lock, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("already canceled invocation touched validation lock: %v", err)
	}
}

func TestValidationWaitDeadlineDoesNotReleaseActiveLock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "validation.lock")
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		lock, _, err := acquireValidationLock(ctx, path, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		<-ctx.Done()
		inspection, err := instancelock.Inspect(path)
		if err != nil || inspection.Status != instancelock.StatusHeld {
			t.Fatalf("wait deadline released active validation: %+v, %v", inspection, err)
		}
	})
}

func TestValidationQueueProcessRecovery(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"waiter", "registration", "holder", "crashed holder"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), validationIntegrationTimeout)
			defer cancel()
			path := filepath.Join(t.TempDir(), "validation.lock")
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestValidationQueueProcessHelper$")
			command.Env = append(os.Environ(), "DETENT_CHECKLOCK_QUEUE_HELPER="+path, "DETENT_CHECKLOCK_QUEUE_MODE="+mode, "GOCOVERDIR="+t.TempDir())
			ready := newNotifyingWriter()
			var stderr bytes.Buffer
			command.Stdout, command.Stderr = ready, &stderr
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			processDone := make(chan struct{})
			var processErr error
			go func() {
				processErr = command.Wait()
				close(processDone)
			}()
			t.Cleanup(func() {
				input.Close()
				cancel()
				<-processDone
			})
			select {
			case <-ready.notified:
			case <-processDone:
				t.Fatalf("helper exited before acquiring: %v: %s", processErr, &stderr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if mode != "registration" {
				waiter, err := registerValidationWaiter(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				position, _, err := validationPosition(ctx, path, waiter.name)
				want := 1
				if mode == "waiter" {
					want = 2
				}
				if err != nil || position != want {
					t.Fatalf("live helper queue position = %d, %v, want %d", position, err, want)
				}
				waiter.lock.Close()
			}
			if mode == "holder" || mode == "crashed holder" {
				inspection, err := instancelock.Inspect(path)
				if err != nil || inspection.Status != instancelock.StatusHeld || inspection.Owner.PID != command.Process.Pid {
					t.Fatalf("active process owner = %+v, %v", inspection, err)
				}
			}
			if _, err := io.WriteString(input, "exit\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-processDone:
				if processErr != nil {
					t.Fatalf("helper failed: %v: %s", processErr, &stderr)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			lock, _, err := acquireValidationLock(ctx, path, io.Discard)
			if err != nil {
				t.Fatalf("acquire after helper exit: %v", err)
			}
			defer lock.Close()
			inspection, err := instancelock.Inspect(path)
			if err != nil || inspection.Owner.PID != os.Getpid() {
				t.Fatalf("owner handoff = %+v, %v", inspection, err)
			}
		})
	}
}

func TestValidationQueueProcessHelper(t *testing.T) {
	path := os.Getenv("DETENT_CHECKLOCK_QUEUE_HELPER")
	if path == "" {
		t.Skip("helper process")
	}
	mode := os.Getenv("DETENT_CHECKLOCK_QUEUE_MODE")
	var lock *instancelock.Lock
	var err error
	switch mode {
	case "waiter":
		var waiter validationWaiter
		waiter, err = registerValidationWaiter(t.Context(), path)
		lock = waiter.lock
	case "registration":
		lock, err = instancelock.Acquire(path + ".queue.lock")
	default:
		lock, err = instancelock.Acquire(path)
	}
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stdout, "ready")
	if _, err := io.ReadFull(os.Stdin, make([]byte, len("exit\n"))); err != nil {
		t.Fatal(err)
	}
	if mode == "holder" {
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	os.Exit(0)
}

func TestValidationQueueDiagnostics(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), validationIntegrationTimeout)
	defer cancel()
	path := filepath.Join(t.TempDir(), "validation.lock")
	acquireTestLock(t, path)
	waitingCtx, stop := context.WithCancel(ctx)
	defer stop()
	writer := newNotifyingWriter()
	done := startValidationWaiter(waitingCtx, path, writer)
	select {
	case <-writer.notified:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	output := writer.data.String()
	for _, field := range []string{"position=1", "queued=1", "owner=held", fmt.Sprintf("owner_pid=%d", os.Getpid()), "owner_since=", "waited="} {
		if !strings.Contains(output, field) {
			t.Errorf("diagnostic %q missing %q", output, field)
		}
	}
	if strings.Contains(output, filepath.Dir(path)) {
		t.Fatalf("diagnostic exposes repository path: %q", output)
	}
}

func acquireTestLock(t *testing.T, path string) *instancelock.Lock {
	t.Helper()
	lock, err := instancelock.Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lock.Close(); err != nil {
			t.Error(err)
		}
	})
	return lock
}

func TestValidationQueueCapacity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "validation.lock")
	for i := range validationQueueLimit {
		acquireTestLock(t, filepath.Join(path+".queue", fmt.Sprintf("%020d.lock", i+1)))
	}
	if _, err := registerValidationWaiter(t.Context(), path); err == nil || !strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("full queue registration error = %v", err)
	}
}

func TestValidationWaiterPruning(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		failures  int
		cancel    bool
		wantCalls int
		wantErr   error
	}{
		{name: "removed", wantCalls: 1},
		{name: "unlocked handle still closing", failures: 1, wantCalls: 2},
		{name: "permanent failure", failures: 5, wantCalls: 5, wantErr: os.ErrPermission},
		{name: "canceled retry", failures: 1, cancel: true, wantCalls: 1, wantErr: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls := 0
				err := pruneValidationWaiter(ctx, "waiter.lock", func(string) error {
					calls++
					if tt.cancel {
						cancel()
					}
					if calls <= tt.failures {
						return os.ErrPermission
					}
					return nil
				})
				if !errors.Is(err, tt.wantErr) || calls != tt.wantCalls {
					t.Fatalf("pruning = (%d calls, %v), want (%d calls, %v)", calls, err, tt.wantCalls, tt.wantErr)
				}
			})
		})
	}
}

func TestValidationWaiterPruningOpenHandle(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "waiter.lock")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	calls := 0
	err = pruneValidationWaiter(t.Context(), path, func(path string) error {
		calls++
		err := os.Remove(path)
		if calls == 1 {
			if closeErr := file.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		return err
	})
	wantCalls := 1
	if runtime.GOOS == "windows" {
		wantCalls = 2
	}
	if err != nil || calls != wantCalls {
		t.Fatalf("prune open handle = %v after %d attempts, want nil after %d", err, calls, wantCalls)
	}
}

func TestValidationQueueRejectsInvalidState(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"unexpected", "xxxxxxxxxxxxxxxxxxxx.lock", "00000000000000000001.lock"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "validation.lock")
			if err := os.MkdirAll(filepath.Join(path+".queue", name), 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := registerValidationWaiter(t.Context(), path); err == nil || !strings.Contains(err.Error(), "invalid validation queue") {
				t.Fatalf("invalid queue registration error = %v", err)
			}
			guard := acquireTestLock(t, path+".queue.lock")
			if err := guard.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
