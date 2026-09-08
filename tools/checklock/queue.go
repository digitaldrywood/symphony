package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/instancelock"
)

const validationQueueLimit = 1024

type validationWaiter struct {
	name string
	lock *instancelock.Lock
}

func acquireValidationLock(ctx context.Context, path string, stderr io.Writer) (*instancelock.Lock, bool, error) {
	started := time.Now()
	waiter, err := registerValidationWaiter(ctx, path)
	if err != nil {
		return nil, false, fmt.Errorf("register validation waiter after %s: %w", time.Since(started).Round(time.Millisecond), err)
	}
	defer func() {
		if err := waiter.lock.Close(); err != nil {
			fmt.Fprintf(stderr, "release validation waiter: %v\n", err)
		}
	}()

	waited := false
	lastDiagnostic := ""
	lastReport := time.Time{}
	for {
		position, size, err := validationPosition(ctx, path, waiter.name)
		if err != nil {
			return nil, waited, fmt.Errorf("wait for validation queue after %s: %w", time.Since(started).Round(time.Millisecond), err)
		}
		if err := ctx.Err(); err != nil {
			return nil, waited, err
		}
		if position == 1 {
			lock, err := instancelock.Acquire(path)
			if err == nil {
				return lock, waited, nil
			}
			if !errors.Is(err, instancelock.ErrHeld) {
				return nil, waited, err
			}
		}
		owner, err := instancelock.Inspect(path)
		if err != nil {
			return nil, waited, err
		}
		diagnostic := fmt.Sprintf("position=%d queued=%d owner=%s", position, size, owner.Status)
		if owner.Status == instancelock.StatusHeld && owner.MetadataError == nil {
			diagnostic += fmt.Sprintf(" owner_pid=%d owner_since=%s", owner.Owner.PID, owner.Owner.StartedAt.Format(time.RFC3339Nano))
		}
		if diagnostic != lastDiagnostic || time.Since(lastReport) >= 30*time.Second {
			fmt.Fprintf(stderr, "validation gate waiting: %s waited=%s\n", diagnostic, time.Since(started).Round(time.Millisecond))
			lastDiagnostic = diagnostic
			lastReport = time.Now()
		}
		waited = true
		if err := waitValidationPoll(ctx); err != nil {
			return nil, waited, fmt.Errorf("validation queue wait ended (%s waited=%s; validation has not started): %w", diagnostic, time.Since(started).Round(time.Millisecond), err)
		}
	}
}

func registerValidationWaiter(ctx context.Context, path string) (validationWaiter, error) {
	var waiter validationWaiter
	err := withValidationQueue(ctx, path, func() error {
		if err := os.MkdirAll(path+".queue", 0o700); err != nil {
			return err
		}
		names, err := liveValidationWaiters(ctx, path)
		if err != nil {
			return err
		}
		if len(names) >= validationQueueLimit {
			return fmt.Errorf("validation queue is full (limit=%d)", validationQueueLimit)
		}
		var ticket uint64
		if len(names) != 0 {
			ticket, err = strconv.ParseUint(strings.TrimSuffix(names[len(names)-1], ".lock"), 10, 64)
			if err != nil {
				return err
			}
		}
		if ticket == ^uint64(0) {
			return errors.New("validation queue ticket space exhausted")
		}
		waiter.name = fmt.Sprintf("%020d.lock", ticket+1)
		waiter.lock, err = instancelock.Acquire(filepath.Join(path+".queue", waiter.name))
		return err
	})
	if err != nil {
		return validationWaiter{}, errors.Join(err, waiter.lock.Close())
	}
	return waiter, nil
}

func validationPosition(ctx context.Context, path, name string) (int, int, error) {
	position, size := 0, 0
	err := withValidationQueue(ctx, path, func() error {
		names, err := liveValidationWaiters(ctx, path)
		if err != nil {
			return err
		}
		size = len(names)
		for i, candidate := range names {
			if candidate == name {
				position = i + 1
				return nil
			}
		}
		return errors.New("validation waiter missing from queue")
	})
	return position, size, err
}

func liveValidationWaiters(ctx context.Context, path string) ([]string, error) {
	entries, err := os.ReadDir(path + ".queue")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 25 || !strings.HasSuffix(name, ".lock") || entry.Type() != 0 {
			return nil, errors.New("invalid validation queue entry")
		}
		if _, err := strconv.ParseUint(strings.TrimSuffix(name, ".lock"), 10, 64); err != nil {
			return nil, errors.New("invalid validation queue ticket")
		}
		waiterPath := filepath.Join(path+".queue", name)
		inspection, err := instancelock.Inspect(waiterPath)
		if err != nil {
			return nil, err
		}
		if inspection.Status == instancelock.StatusHeld {
			names = append(names, name)
			continue
		}
		if err := pruneValidationWaiter(ctx, waiterPath, os.Remove); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func pruneValidationWaiter(ctx context.Context, path string, remove func(string) error) error {
	for attempt := range 5 {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := remove(path)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if attempt == 4 {
			return err
		}
		if waitErr := waitValidationPoll(ctx); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
	return nil
}

func withValidationQueue(ctx context.Context, path string, action func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		guard, err := instancelock.Acquire(path + ".queue.lock")
		if err == nil {
			if err := ctx.Err(); err != nil {
				return errors.Join(err, guard.Close())
			}
			return errors.Join(action(), guard.Close())
		}
		if !errors.Is(err, instancelock.ErrHeld) {
			return err
		}
		if err := waitValidationPoll(ctx); err != nil {
			return err
		}
	}
}

func waitValidationPoll(ctx context.Context) error {
	timer := time.NewTimer(validationLockPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
