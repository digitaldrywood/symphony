package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestWorkerProgressCheckpointPersistence(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "persisted"},
		{name: "storage unavailable", err: errors.New("storage unavailable")},
		{name: "retired attempt", err: store.ErrNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			attempts := &progressAttemptStore{err: tt.err}
			running := Running{Issue: connector.Issue{ID: "worker", State: "In Progress"}, WorkAttemptID: 2270, Generation: 1, Mode: runpkg.RunModeImplement}
			base := store.WorkAttemptHeartbeat{AttemptID: running.WorkAttemptID, HeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute)}
			progress := newWorkerProgress(running, base, attempts, 4096)
			running.progress = progress
			update := runpkg.UsageUpdate{LastCommand: "go test ./...", SessionID: "session", LastMessage: "tests running", DispatchLoopStart: &runpkg.DispatchLoopStartSnapshot{WorkspaceDiffAvailable: true}}
			if err := progress.observe(t.Context(), update); !errors.Is(err, tt.err) {
				t.Fatalf("observe() = %v, want %v", err, tt.err)
			}
			if got := running.withProgress(); got.DispatchLoopStart.Persisted != (tt.err == nil) || got.LastMessage != update.LastMessage || got.LastCommand != update.LastCommand {
				t.Fatalf("progress = %#v", got)
			}
			if got := progress.persisted.Load(); (got != nil) != (tt.err == nil) {
				t.Fatalf("persisted heartbeat = %#v", got)
			}
			if len(attempts.heartbeats) != 1 || !strings.Contains(attempts.heartbeats[0].WorkerMetadataJSON, "dispatch_loop_start") {
				t.Fatalf("checkpoint heartbeats = %#v", attempts.heartbeats)
			}
			progress.close()
			if err := progress.observe(t.Context(), runpkg.UsageUpdate{LastMessage: "late callback"}); !errors.Is(err, context.Canceled) {
				t.Fatalf("retired callback = %v", err)
			}
			if got := running.withProgress().LastMessage; got != update.LastMessage {
				t.Fatalf("retired worker changed to %q", got)
			}
		})
	}
}

func TestWorkerProgressHeartbeatUsesLatestObservation(t *testing.T) {
	t.Parallel()
	now := time.Now()
	attempts := &progressAttemptStore{}
	running := Running{Issue: connector.Issue{ID: "worker"}, WorkAttemptID: 2270, Generation: 1, Mode: runpkg.RunModeImplement}
	base := store.WorkAttemptHeartbeat{AttemptID: running.WorkAttemptID, HeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute)}
	progress := newWorkerProgress(running, base, attempts, 4096)
	running.progress = progress
	manager := newHeartbeatManager(normalizeConfig(Config{}), nil, attempts, time.Now, nil)
	target := heartbeatTarget{issueID: running.Issue.ID, progress: progress, workAttemptHeartbeat: base}
	manager.upsert(target)
	for _, message := range []string{"workspace created", "tests running"} {
		update := runpkg.UsageUpdate{SessionID: "session", LastMessage: message, RecentEvents: []telemetry.ActivityEvent{{Message: message}}}
		if err := progress.observe(t.Context(), update); err != nil {
			t.Fatal(err)
		}
		update.RecentEvents[0].Message = "caller changed its copy"
	}
	if len(attempts.heartbeats) != 0 {
		t.Fatal("ordinary observations performed synchronous persistence")
	}
	cfg := normalizeConfig(Config{Claiming: ClaimingConfig{LeaseTTL: 2 * time.Minute}})
	manager.configure(cfg, nil, attempts)
	manager.execute(t.Context(), target)
	if len(attempts.heartbeats) != 1 || attempts.heartbeats[0].StatusMessage != "tests running" || attempts.heartbeats[0].Phase != "testing" {
		t.Fatalf("dedicated heartbeat = %#v", attempts.heartbeats)
	}
	if got := attempts.heartbeats[0]; got.LeaseExpiresAt.Sub(got.HeartbeatAt) != 2*time.Minute {
		t.Fatalf("heartbeat ignored updated lease duration: %#v", got)
	}
	if got := running.withProgress(); got.SessionID != "session" || got.RecentEvents[0].Message != "tests running" {
		t.Fatalf("progress snapshot = %#v", got)
	}
	state := newState(Config{})
	state.Running[running.Issue.ID] = running
	state.WorkAttempts = []telemetry.WorkAttempt{{AttemptID: running.WorkAttemptID}}
	orch := &Orchestrator{}
	observed := orch.observableState(state.clone())
	if got := observed.WorkAttempts[0]; got.StatusMessage != "tests running" || got.HeartbeatAt == nil {
		t.Fatalf("observable attempt = %#v", got)
	}
	previous := attempts.heartbeats[0].HeartbeatAt
	heartbeat, err := manager.persistHeartbeat(t.Context(), target, manager.settingsSnapshot(), now.Add(-time.Hour))
	if err != nil || heartbeat.HeartbeatAt.Before(previous) {
		t.Fatalf("delayed heartbeat regressed timestamp: %v, %v", heartbeat.HeartbeatAt, err)
	}
	manager.remove(running.Issue.ID)
	if err := progress.observe(t.Context(), runpkg.UsageUpdate{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("removed heartbeat target accepted callback: %v", err)
	}
}

func TestWorkerProgressCheckpointCancellationAndRetirement(t *testing.T) {
	for _, cancelCheckpoint := range []bool{false, true} {
		name := "retirement joins persistence"
		if cancelCheckpoint {
			name = "checkpoint honors cancellation"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now()
				attempts := &progressAttemptStore{started: make(chan struct{}), release: make(chan struct{})}
				running := Running{Issue: connector.Issue{ID: "worker"}, WorkAttemptID: 2270}
				progress := newWorkerProgress(running, store.WorkAttemptHeartbeat{AttemptID: 2270, HeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute)}, attempts, 4096)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					done <- progress.observe(ctx, runpkg.UsageUpdate{LastMessage: "checkpoint", DispatchLoopStart: &runpkg.DispatchLoopStartSnapshot{}})
				}()
				<-attempts.started
				inFlight := progress.latest.Load()
				if inFlight.LastMessage != "checkpoint" || inFlight.DispatchLoopStart.Persisted {
					t.Fatalf("in-flight checkpoint = %#v", inFlight)
				}
				retired := make(chan struct{})
				go func() { progress.close(); close(retired) }()
				select {
				case <-retired:
					t.Fatal("retirement returned with checkpoint persistence in flight")
				default:
				}
				if cancelCheckpoint {
					cancel()
				} else {
					close(attempts.release)
				}
				err := <-done
				if cancelCheckpoint && !errors.Is(err, context.Canceled) || !cancelCheckpoint && err != nil {
					t.Fatalf("checkpoint = %v", err)
				}
				<-retired
				if inFlight.DispatchLoopStart.Persisted || progress.latest.Load().DispatchLoopStart.Persisted == cancelCheckpoint {
					t.Fatal("checkpoint persistence mutated an earlier snapshot or published the wrong outcome")
				}
			})
		})
	}
}

func TestRunUpdateRejectsRetiredWorkerProgress(t *testing.T) {
	t.Parallel()
	state := newState(Config{})
	current := &workerProgress{}
	state.Running["worker"] = Running{Issue: connector.Issue{ID: "worker"}, progress: current, LastMessage: "current"}
	orch := &Orchestrator{}
	orch.handleRunUpdate(&state, runUpdate{issueID: "worker", progress: &workerProgress{}, usage: runpkg.UsageUpdate{LastMessage: "retired"}})
	if got := state.Running["worker"].LastMessage; got != "current" {
		t.Fatalf("replacement worker progress = %q", got)
	}
}

type progressAttemptStore struct {
	recordingWorkAttemptStore
	err     error
	started chan struct{}
	release chan struct{}
}

func (s *progressAttemptStore) RecordWorkAttemptHeartbeat(ctx context.Context, heartbeat store.WorkAttemptHeartbeat) error {
	if s.started != nil {
		close(s.started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
		}
	}
	s.heartbeats = append(s.heartbeats, heartbeat)
	return s.err
}
