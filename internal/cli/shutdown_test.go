package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/procgroup"
	projectpkg "github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestShutdownControllerQueuesRequests(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	controller.RequestDrain()
	controller.RequestForce()

	if got := <-controller.Requests(); got != ShutdownRequestDrain {
		t.Fatalf("first request = %v, want drain", got)
	}
	if got := <-controller.Requests(); got != ShutdownRequestForce {
		t.Fatalf("second request = %v, want force", got)
	}
}

func TestShutdownControllerInterruptRequestsDrainThenForce(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	if controller.RequestInterrupt() {
		t.Fatal("inactive interrupt was handled")
	}
	if request, handled := controller.RequestInterruptKind(); handled || request != 0 {
		t.Fatalf("inactive interrupt kind = %v, %v, want 0, false", request, handled)
	}

	deactivate := controller.activate()
	defer deactivate()

	request, handled := controller.RequestInterruptKind()
	if !handled {
		t.Fatal("active interrupt kind was not handled")
	}
	if request != ShutdownRequestDrain {
		t.Fatalf("first interrupt kind = %v, want drain", request)
	}
	if got := <-controller.Requests(); got != ShutdownRequestDrain {
		t.Fatalf("first interrupt = %v, want drain", got)
	}

	request, handled = controller.RequestInterruptKind()
	if !handled {
		t.Fatal("second interrupt kind was not handled")
	}
	if request != ShutdownRequestForce {
		t.Fatalf("second interrupt kind = %v, want force", request)
	}
	if got := <-controller.Requests(); got != ShutdownRequestForce {
		t.Fatalf("second interrupt = %v, want force", got)
	}
}

func TestShutdownControllerDrainMakesNextInterruptForce(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	deactivate := controller.activate()
	defer deactivate()

	controller.RequestDrain()
	if got := <-controller.Requests(); got != ShutdownRequestDrain {
		t.Fatalf("explicit drain = %v, want drain", got)
	}

	if !controller.RequestInterrupt() {
		t.Fatal("interrupt after drain was not handled")
	}
	if got := <-controller.Requests(); got != ShutdownRequestForce {
		t.Fatalf("interrupt after drain = %v, want force", got)
	}
}

func TestRequestTerminalShutdownInterruptQueuesForcedShutdown(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	deactivate := controller.activate()
	defer deactivate()

	if !requestTerminalShutdownInterrupt(controller) {
		t.Fatal("first interrupt was not handled")
	}
	if got := <-controller.Requests(); got != ShutdownRequestDrain {
		t.Fatalf("first interrupt request = %v, want drain", got)
	}

	if !requestTerminalShutdownInterrupt(controller) {
		t.Fatal("second interrupt was not handled")
	}
	if got := <-controller.Requests(); got != ShutdownRequestForce {
		t.Fatalf("second interrupt request = %v, want force", got)
	}
}

func TestRunWithShutdownZeroSessionsExitsGracefully(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	var output bytes.Buffer
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:       controller,
			Registry:         projectpkg.NewRegistry(),
			SnapshotHub:      hub.New[telemetry.Snapshot](),
			Output:           &output,
			ProgressInterval: time.Millisecond,
			HardTimeout:      time.Second,
			Now: func() time.Time {
				return time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if got := output.String(); !strings.Contains(got, "shutdown requested — no agent sessions in flight") {
		t.Fatalf("output missing zero-session notice:\n%s", got)
	}
}

func TestRunWithShutdownTerminalDashboardSuppressesPlainOutput(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	var output bytes.Buffer
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:        controller,
			Registry:          projectpkg.NewRegistry(),
			SnapshotHub:       hub.New[telemetry.Snapshot](),
			Output:            &output,
			TerminalDashboard: true,
			HardTimeout:       time.Second,
			Now: func() time.Time {
				return time.Date(2026, 6, 16, 16, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if got := output.String(); got != "" {
		t.Fatalf("terminal dashboard shutdown wrote plain output:\n%s", got)
	}
}

func TestRunWithShutdownTerminalDashboardSuppressesSignalNotices(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	suppressed := make(chan bool, 1)
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:        controller,
			Registry:          projectpkg.NewRegistry(),
			SnapshotHub:       hub.New[telemetry.Snapshot](),
			Output:            io.Discard,
			TerminalDashboard: true,
			HardTimeout:       time.Second,
		}, func(ctx context.Context) error {
			suppressed <- controller.SignalNoticesSuppressed()
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	if got := <-suppressed; !got {
		t.Fatal("signal notices were not suppressed while terminal dashboard was running")
	}
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if controller.SignalNoticesSuppressed() {
		t.Fatal("signal notices remained suppressed after shutdown runner exited")
	}
}

func TestRunWithShutdownZeroSessionsLogsCleanupBoundaries(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	var logs bytes.Buffer
	errs := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:       controller,
			Registry:         projectpkg.NewRegistry(),
			SnapshotHub:      hub.New[telemetry.Snapshot](),
			Output:           io.Discard,
			Logger:           logger,
			DrainTimeout:     time.Hour,
			ProgressInterval: time.Hour,
			HardTimeout:      time.Second,
			Now: func() time.Time {
				return time.Date(2026, 6, 16, 16, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	start := time.Now()
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if elapsed := time.Since(start); elapsed >= shutdownDrainPollInterval {
		t.Fatalf("zero-session shutdown waited %s, want less than %s", elapsed, shutdownDrainPollInterval)
	}

	got := logs.String()
	for _, want := range []string{
		"operation=initial_running_session_inventory",
		"operation=drain_projects",
		"operation=shutdown_snapshot_publish",
		"operation=shutdown_drain_blockers",
		"operation=stop_projects",
		"operation=serve_cancel",
		"operation=wait_for_serve_exit",
		"sessions=0",
		"blockers=0",
		"duration=",
		"result=ok",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("debug logs missing %q:\n%s", want, got)
		}
	}
}

func TestRunWithShutdownRunningProjectNoActiveSessionsExitsGracefully(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	registry := projectpkg.NewRegistry()
	project := newShutdownRuntimeProject(t, "detent", nil, orchestrator.FakeRunner{})
	if err := registry.Set(project); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := project.Close(); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Close() error = %v", err)
		}
	})

	started := make(chan struct{})
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:       controller,
			Registry:         registry,
			SnapshotHub:      hub.New[telemetry.Snapshot](),
			Output:           io.Discard,
			Logger:           logger,
			DrainTimeout:     time.Hour,
			ProgressInterval: time.Hour,
			HardTimeout:      time.Second,
			Now: func() time.Time {
				return time.Date(2026, 6, 23, 15, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	start := time.Now()
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if elapsed := time.Since(start); elapsed >= shutdownDrainPollInterval {
		t.Fatalf("zero-session shutdown waited %s, want less than %s", elapsed, shutdownDrainPollInterval)
	}
	got := logs.String()
	for _, want := range []string{
		"operation=shutdown_drain_blockers",
		"blockers=0",
		"operation=project_stop",
		"project_id=detent",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs missing %q:\n%s", want, got)
		}
	}
}

func TestRunWithShutdownZeroSessionsExitsWhenServeIgnoresCancellation(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:       controller,
			Registry:         projectpkg.NewRegistry(),
			SnapshotHub:      hub.New[telemetry.Snapshot](),
			Output:           io.Discard,
			ProgressInterval: time.Millisecond,
			HardTimeout:      20 * time.Millisecond,
			Now: func() time.Time {
				return time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
			},
		}, func(context.Context) error {
			close(started)
			select {}
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRunWithShutdownActiveChildProcessReportsDrainBlockersAndTimesOut(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	registry := projectpkg.NewRegistry()
	runner := &shutdownBlockingRunner{
		started:  make(chan struct{}, 1),
		canceled: make(chan struct{}, 1),
	}
	project := newShutdownRuntimeProject(t, "detent", []connector.Issue{{
		ID:               "issue-641",
		Identifier:       "digitaldrywood/detent#641",
		Title:            "service restart can hang while draining",
		State:            "Todo",
		AssignedToWorker: true,
	}}, runner)
	if err := registry.Set(project); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := project.Close(); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Close() error = %v", err)
		}
	})

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}
	waitForShutdownSession(t, registry, func(session telemetry.Running) bool {
		return session.ProcessIdentity == "4242" && session.SessionID == "thread-641-turn-1"
	})

	started := make(chan struct{})
	var output bytes.Buffer
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	processStartedAt := time.Date(2026, 6, 23, 14, 59, 0, 0, time.UTC)
	processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{{
		SessionID:  1214,
		IssueID:    "issue-641",
		Identifier: "digitaldrywood/detent#641",
		WorkerProcessIdentity: store.WorkerProcessIdentity{
			PID:       4242,
			GroupID:   4242,
			StartedAt: processStartedAt,
		},
	}}}
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:      controller,
			Registry:        registry,
			SnapshotHub:     hub.New[telemetry.Snapshot](),
			Output:          &output,
			Logger:          logger,
			DrainTimeout:    20 * time.Millisecond,
			HardTimeout:     time.Second,
			WorkerProcesses: processStore,
			ReapWorkerProcess: func(_ context.Context, identity procgroup.Identity, _ time.Duration) (procgroup.TerminationOutcome, error) {
				if identity.PID != 4242 || identity.GroupID != 4242 || !identity.StartedAt.Equal(processStartedAt) {
					t.Errorf("worker identity = %#v", identity)
				}
				return procgroup.TerminationOutcomeTerminated, nil
			},
			Now: func() time.Time {
				return time.Date(2026, 6, 23, 15, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	select {
	case err := <-errs:
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("runWithShutdown() error = %v, want ErrShutdownTimeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	select {
	case <-runner.canceled:
	case <-time.After(time.Second):
		t.Fatal("runner was not canceled by forced shutdown")
	}
	if len(processStore.reaped) != 1 || processStore.reaped[0].sessionID != 1214 || processStore.reaped[0].reap.Outcome != store.WorkerProcessOutcomeTerminated {
		t.Fatalf("reaped worker processes = %#v", processStore.reaped)
	}

	gotOutput := output.String()
	for _, want := range []string{
		"shutdown requested — 1 agent session in flight",
		"20ms remaining until force quit",
		"#641",
		"process=4242",
		"session=thread-641-turn-1",
		"drain timeout reached — interrupting 1 agent session",
	} {
		if !strings.Contains(gotOutput, want) {
			t.Fatalf("output missing %q:\n%s", want, gotOutput)
		}
	}

	gotLogs := logs.String()
	for _, want := range []string{
		"operation=shutdown_drain_blockers",
		"operation=shutdown_drain_timeout",
		"blockers=1",
		"process=4242",
		"session=thread-641-turn-1",
		"digitaldrywood/detent#641",
		"worker process lifecycle decision",
		"reason=\"drain timeout\"",
		"decision=terminated",
	} {
		if !strings.Contains(gotLogs, want) {
			t.Fatalf("logs missing %q:\n%s", want, gotLogs)
		}
	}
}

func TestShutdownRunningSessionsIncludesActiveMergeWorker(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	runner := &shutdownBlockingRunner{
		started: make(chan struct{}, 1),
		release: release,
	}
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Tracker.ActiveStates = []string{"Merging"}
	cfg.Tracker.Issues = []connector.Issue{{
		ID:               "issue-1546-merge",
		Identifier:       "digitaldrywood/detent#1546",
		Title:            "quiesce merge dispatch during shutdown",
		State:            "Merging",
		AssignedToWorker: true,
		PullRequest: &connector.PullRequest{
			Number:         1546,
			State:          "OPEN",
			MergeableState: "clean",
			CIStatus:       "success",
		},
	}}
	cfg.Polling.IntervalMS = 60000
	cfg.Agent.MaxConcurrentAgents = 1
	cfg.Agent.MergeFastPath.Enabled = true
	project := newShutdownRuntimeProjectWithConfig(t, "detent", cfg, runner)
	registry := projectpkg.NewRegistry()
	if err := registry.Set(project); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		close(release)
		if err := project.Close(); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Close() error = %v", err)
		}
	})

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("merge worker did not start")
	}

	sessions := shutdownRunningSessions(context.Background(), registry, time.Now())
	if len(sessions) != 1 {
		t.Fatalf("shutdownRunningSessions() = %#v, want one active merge worker", sessions)
	}
	if sessions[0].ID != "issue-1546-merge" || sessions[0].State != "Merging" {
		t.Fatalf("active merge blocker = %#v, want issue-1546-merge in Merging", sessions[0])
	}
	if summary := shutdownSessionSummary(sessions[0]); !strings.Contains(summary, "digitaldrywood/detent#1546") {
		t.Fatalf("shutdownSessionSummary() = %q, want named merge blocker", summary)
	}
}

func TestRunWithShutdownDoesNotTrustStaleEmptySnapshotOverLiveSession(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	registry := projectpkg.NewRegistry()
	releaseRunner := make(chan struct{})
	runner := &shutdownBlockingRunner{
		started:  make(chan struct{}, 1),
		canceled: make(chan struct{}, 1),
		release:  releaseRunner,
	}
	project := newShutdownRuntimeProject(t, "detent", []connector.Issue{{
		ID:               "issue-1484",
		Identifier:       "digitaldrywood/detent#1484",
		Title:            "stale empty shutdown inventory",
		State:            "Todo",
		AssignedToWorker: true,
	}}, runner)
	if err := registry.Set(project); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-releaseRunner:
		default:
			close(releaseRunner)
		}
		if err := project.Close(); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Close() error = %v", err)
		}
	})

	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}
	waitForShutdownSession(t, registry, func(session telemetry.Running) bool {
		return session.ID == "issue-1484"
	})

	snapshotHub := hub.New[telemetry.Snapshot]()
	if err := snapshotHub.Publish(telemetry.Snapshot{
		Seq:         1,
		GeneratedAt: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		Running:     []telemetry.Running{},
		Counts:      telemetry.Counts{Running: 0},
	}); err != nil {
		t.Fatalf("SnapshotHub.Publish() error = %v", err)
	}
	snapshotCtx, cancelSnapshots := context.WithTimeout(context.Background(), time.Second)
	defer cancelSnapshots()
	snapshots, err := snapshotHub.Subscribe(snapshotCtx)
	if err != nil {
		t.Fatalf("SnapshotHub.Subscribe() error = %v", err)
	}
	defer snapshots.Close()

	serveStarted := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:       controller,
			Registry:         registry,
			SnapshotHub:      snapshotHub,
			DrainTimeout:     time.Second,
			ProgressInterval: time.Hour,
			HardTimeout:      time.Second,
		}, func(ctx context.Context) error {
			close(serveStarted)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-serveStarted:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	drainObserved := false
	for !drainObserved {
		select {
		case err := <-errCh:
			t.Fatalf("shutdown completed while live session was active: %v", err)
		case snapshot := <-snapshots.C():
			if !snapshot.Shutdown.Draining {
				continue
			}
			if snapshot.Shutdown.SessionsRemaining != 1 {
				t.Fatalf("SessionsRemaining = %d, want live inventory count 1", snapshot.Shutdown.SessionsRemaining)
			}
			drainObserved = true
		case <-snapshotCtx.Done():
			t.Fatal("timed out waiting for draining snapshot")
		}
	}

	select {
	case <-runner.canceled:
		t.Fatal("live session was canceled from stale empty shutdown inventory")
	default:
	}

	close(releaseRunner)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shutdown after live session completed")
	}
}

type shutdownWorkerProcessStore struct {
	processes []store.WorkerProcess
	reaped    []shutdownWorkerProcessReap
}

type shutdownWorkerProcessReap struct {
	sessionID int64
	reap      store.WorkerProcessReap
}

func (s *shutdownWorkerProcessStore) ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error) {
	return append([]store.WorkerProcess(nil), s.processes...), nil
}

func (s *shutdownWorkerProcessStore) MarkSessionWorkerProcessReaped(_ context.Context, sessionID int64, reap store.WorkerProcessReap) error {
	s.reaped = append(s.reaped, shutdownWorkerProcessReap{sessionID: sessionID, reap: reap})
	return nil
}

func TestRunWithShutdownMarksControllerActive(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	ctx, cancel := context.WithCancel(context.Background())
	active := make(chan bool, 1)
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(ctx, runningShutdownConfig{
			Controller:  controller,
			Registry:    projectpkg.NewRegistry(),
			SnapshotHub: hub.New[telemetry.Snapshot](),
			HardTimeout: time.Second,
		}, func(ctx context.Context) error {
			active <- controller.Active()
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case got := <-active:
		if !got {
			t.Fatal("controller active = false while runWithShutdown is serving")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	cancel()

	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runWithShutdown() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
	if controller.Active() {
		t.Fatal("controller active = true after runWithShutdown returned")
	}
}

func TestRunWithShutdownForceReturnsForcedError(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	var output bytes.Buffer
	errs := make(chan error, 1)

	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:  controller,
			Registry:    projectpkg.NewRegistry(),
			SnapshotHub: hub.New[telemetry.Snapshot](),
			Output:      &output,
			HardTimeout: time.Second,
			Now: func() time.Time {
				return time.Date(2026, 6, 12, 15, 0, 0, 0, time.UTC)
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestForce()

	select {
	case err := <-errs:
		if !errors.Is(err, ErrShutdownForced) {
			t.Fatalf("runWithShutdown() error = %v, want ErrShutdownForced", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for force shutdown")
	}
	if got := output.String(); !strings.Contains(got, "force quit requested — interrupting 0 agent sessions") {
		t.Fatalf("output missing force notice:\n%s", got)
	}
}

func TestRunWithShutdownForceReapsOwnedWorkerGroupWithinHardDeadline(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	processStartedAt := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	processStore := &shutdownWorkerProcessStore{processes: []store.WorkerProcess{
		{
			SessionID:  1484,
			IssueID:    "issue-1484",
			Identifier: "digitaldrywood/detent#1484",
			WorkerProcessIdentity: store.WorkerProcessIdentity{
				PID:       41484,
				GroupID:   41484,
				StartedAt: processStartedAt,
			},
		},
		{
			SessionID:  1485,
			IssueID:    "issue-1485",
			Identifier: "digitaldrywood/detent#1485",
			WorkerProcessIdentity: store.WorkerProcessIdentity{
				PID:       41485,
				GroupID:   41485,
				StartedAt: processStartedAt,
			},
		},
	}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:      controller,
			Registry:        projectpkg.NewRegistry(),
			SnapshotHub:     hub.New[telemetry.Snapshot](),
			HardTimeout:     50 * time.Millisecond,
			WorkerProcesses: processStore,
			ReapWorkerProcess: func(ctx context.Context, identity procgroup.Identity, _ time.Duration) (procgroup.TerminationOutcome, error) {
				if identity.GroupID == 41484 {
					return procgroup.TerminationOutcomeTerminated, nil
				}
				<-ctx.Done()
				return procgroup.TerminationOutcome(""), ctx.Err()
			},
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	forceStarted := time.Now()
	controller.RequestForce()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrShutdownForced) {
			t.Fatalf("runWithShutdown() error = %v, want ErrShutdownForced", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("forced shutdown exceeded hard deadline")
	}
	if elapsed := time.Since(forceStarted); elapsed >= 500*time.Millisecond {
		t.Fatalf("forced shutdown duration = %s, want under 500ms", elapsed)
	}
	if len(processStore.reaped) != 1 || processStore.reaped[0].sessionID != 1484 {
		t.Fatalf("reaped worker processes = %#v, want owned group 41484", processStore.reaped)
	}
}

func TestRunWithShutdownPublishesDrainStatusDuringBlockedConnectorRefresh(t *testing.T) {
	t.Parallel()

	refreshStarted := make(chan struct{}, 1)
	releaseRefresh := make(chan struct{})
	project := newRefreshProjectWithConnector(t, "detent", shutdownBlockingConnector{
		started: refreshStarted,
		release: releaseRefresh,
	})
	if err := project.Start(context.Background()); err != nil {
		t.Fatalf("Project.Start() error = %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-releaseRefresh:
		default:
			close(releaseRefresh)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := project.Stop(ctx); err != nil && !errors.Is(err, projectpkg.ErrNotRunning) {
			t.Fatalf("Project.Stop() error = %v", err)
		}
	})

	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("connector refresh did not start")
	}

	registry := projectpkg.NewRegistry()
	mustSetProject(t, registry, project)
	requestedAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	snapshotHub := hub.New[telemetry.Snapshot]()
	if err := snapshotHub.Publish(telemetry.Snapshot{
		GeneratedAt: requestedAt.Add(-time.Second),
		Running: []telemetry.Running{{
			Issue: telemetry.Issue{
				ID:         "issue-1484",
				Identifier: "digitaldrywood/detent#1484",
			},
		}},
		Counts: telemetry.Counts{Running: 1},
	}); err != nil {
		t.Fatalf("SnapshotHub.Publish() error = %v", err)
	}

	controller := NewShutdownController()
	serveStarted := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:  controller,
			Registry:    registry,
			SnapshotHub: snapshotHub,
			HardTimeout: 100 * time.Millisecond,
			Now:         func() time.Time { return requestedAt },
		}, func(ctx context.Context) error {
			close(serveStarted)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-serveStarted:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	for {
		snapshot, ok := snapshotHub.Latest()
		if ok && snapshot.Shutdown.Draining {
			if snapshot.Shutdown.SessionsRemaining != 0 {
				t.Fatalf("SessionsRemaining = %d, want 0 from live initializing runtime", snapshot.Shutdown.SessionsRemaining)
			}
			break
		}
		select {
		case <-deadline.C:
			close(releaseRefresh)
			<-errs
			t.Fatal("drain status was not published while connector refresh was blocked")
		case <-time.After(time.Millisecond):
		}
	}

	close(releaseRefresh)
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestRunWithShutdownForcedResultPrecedesServeCleanupError(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		errs <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:  controller,
			Registry:    projectpkg.NewRegistry(),
			SnapshotHub: hub.New[telemetry.Snapshot](),
			HardTimeout: time.Second,
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			cause := fmt.Errorf("resolve github_token via gh auth token: %w", errors.New("context deadline exceeded"))
			return GitHubAuthError(cause)
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestForce()

	select {
	case err := <-errs:
		if !errors.Is(err, ErrShutdownForced) {
			t.Fatalf("runWithShutdown() error = %v, want ErrShutdownForced", err)
		}
		if got := ClassifyError(err).Slug; got != errorCodeShutdownForced {
			t.Fatalf("ClassifyError() = %q, want %q", got, errorCodeShutdownForced)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for force shutdown")
	}
}

func TestRunWithShutdownGracefulResultPrecedesServeCleanupError(t *testing.T) {
	t.Parallel()

	controller := NewShutdownController()
	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- runWithShutdown(context.Background(), runningShutdownConfig{
			Controller:  controller,
			Registry:    projectpkg.NewRegistry(),
			SnapshotHub: hub.New[telemetry.Snapshot](),
			HardTimeout: time.Second,
		}, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			cause := fmt.Errorf("resolve github_token via gh auth token: %w", errors.New("context deadline exceeded"))
			return GitHubAuthError(cause)
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	controller.RequestDrain()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runWithShutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for graceful shutdown")
	}
}

func TestRunStartupAndServeForcedResultPrecedesStartupAuthError(t *testing.T) {
	t.Parallel()

	err := runStartupAndServe(context.Background(), web.NewStartupLifecycle(), func(ctx context.Context) error {
		<-ctx.Done()
		cause := fmt.Errorf("resolve github_token via gh auth token: %w", errors.New("context deadline exceeded"))
		return GitHubAuthError(cause)
	}, startupReadiness{}, func(context.Context) error {
		return ErrShutdownForced
	})

	if !errors.Is(err, ErrShutdownForced) {
		t.Fatalf("runStartupAndServe() error = %v, want ErrShutdownForced", err)
	}
	if got := ClassifyError(err).Slug; got != errorCodeShutdownForced {
		t.Fatalf("ClassifyError() = %q, want %q", got, errorCodeShutdownForced)
	}
}

func TestRunStartupAndServeMarksFailedBeforeCancelingServe(t *testing.T) {
	t.Parallel()

	lifecycle := web.NewStartupLifecycle()
	serveStarted := make(chan struct{})
	observed := make(chan web.StartupLifecycleState, 1)
	startupErr := errors.New("startup failed")
	err := runStartupAndServe(context.Background(), lifecycle, func(context.Context) error {
		<-serveStarted
		return startupErr
	}, startupReadiness{}, func(ctx context.Context) error {
		close(serveStarted)
		<-ctx.Done()
		observed <- lifecycle.State()
		return ctx.Err()
	})

	if !errors.Is(err, startupErr) {
		t.Fatalf("runStartupAndServe() error = %v, want %v", err, startupErr)
	}
	if got := <-observed; got != web.StartupLifecycleFailed {
		t.Fatalf("lifecycle at serve cancellation = %q, want %q", got, web.StartupLifecycleFailed)
	}
}

func TestRunStartupAndServeMarksHealthyAfterServingReadiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		serveBeforeReady bool
		wantHealthy      int32
	}{
		{name: "serving failure preserves rollback state", serveBeforeReady: true},
		{name: "serving readiness commits healthy startup", wantHealthy: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startupDone := make(chan struct{})
			serveReady := make(chan struct{})
			serveFailed := make(chan struct{})
			healthyMarked := make(chan struct{})
			var healthyCalls atomic.Int32
			serveErr := errors.New("serve failed before readiness")

			done := make(chan error, 1)
			go func() {
				done <- runStartupAndServe(ctx, web.NewStartupLifecycle(), func(context.Context) error {
					close(startupDone)
					return nil
				}, startupReadiness{
					AwaitServe: func(ctx context.Context) error {
						select {
						case <-serveReady:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					},
					MarkHealthy: func(context.Context) error {
						healthyCalls.Add(1)
						close(healthyMarked)
						return nil
					},
				}, func(ctx context.Context) error {
					if tt.serveBeforeReady {
						<-startupDone
						close(serveFailed)
						return serveErr
					}
					<-ctx.Done()
					return ctx.Err()
				})
			}()

			select {
			case <-startupDone:
			case <-time.After(time.Second):
				t.Fatal("startup did not finish")
			}
			if tt.serveBeforeReady {
				select {
				case <-serveFailed:
				case <-time.After(time.Second):
					t.Fatal("serving failure did not occur")
				}
			} else {
				close(serveReady)
				select {
				case <-healthyMarked:
				case <-done:
					t.Fatal("runStartupAndServe returned before marking healthy")
				case <-time.After(time.Second):
					t.Fatal("startup was not marked healthy after serving readiness")
				}
				cancel()
			}

			err := <-done
			if tt.serveBeforeReady && !errors.Is(err, serveErr) {
				t.Fatalf("runStartupAndServe() error = %v, want %v", err, serveErr)
			}
			if got := healthyCalls.Load(); got != tt.wantHealthy {
				t.Fatalf("healthy calls = %d, want %d", got, tt.wantHealthy)
			}
		})
	}
}

func TestRunningShutdownConfigComputesDrainTimeoutFromCurrentRegistry(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	cfg := runningShutdownConfig{
		Registry: registry,
		DrainTimeoutSource: func() time.Duration {
			return shutdownDrainTimeout(registry)
		},
	}

	wantDefault := time.Duration(workflowconfig.DefaultShutdownDrainTimeoutMS) * time.Millisecond
	if got := shutdownDrainTimeoutForConfig(cfg); got != wantDefault {
		t.Fatalf("shutdownDrainTimeoutForConfig() = %v, want %v", got, wantDefault)
	}

	project := newShutdownProject(t, "alpha", 0)
	if err := registry.Set(project); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}

	if got := shutdownDrainTimeoutForConfig(cfg); got != wantDefault {
		t.Fatalf("shutdownDrainTimeoutForConfig() after registry update = %v, want %v", got, wantDefault)
	}
}

func TestPublishShutdownSnapshotSharesSnapshotSeq(t *testing.T) {
	t.Parallel()

	registry := projectpkg.NewRegistry()
	project := startRefreshProject(t, "alpha")
	waitForProjectDataSeq(t, project, 1)
	mustSetProject(t, registry, project)

	snapshotHub := hub.New[telemetry.Snapshot]()
	controller := NewShutdownController()
	var seq atomic.Uint64
	seq.Store(4)
	now := time.Date(2026, 7, 8, 12, 30, 0, 0, time.UTC)
	if err := snapshotHub.Publish(telemetry.Snapshot{Seq: 4, GeneratedAt: now.Add(-time.Second)}); err != nil {
		t.Fatalf("SnapshotHub.Publish() error = %v", err)
	}
	publishShutdownSnapshot(runningShutdownConfig{
		Controller:  controller,
		Registry:    registry,
		SnapshotHub: snapshotHub,
		SnapshotSeq: &seq,
	}, now, now, nil)

	shutdownSnapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("SnapshotHub.Latest() ok = false, want shutdown snapshot")
	}
	if shutdownSnapshot.Seq != 5 {
		t.Fatalf("shutdown snapshot Seq = %d, want 5", shutdownSnapshot.Seq)
	}
	if !shutdownSnapshot.Shutdown.Draining {
		t.Fatal("shutdown snapshot Draining = false, want true")
	}

	if err := publishSnapshotOnce(context.Background(), registry, nil, snapshotHub, &seq, controller, now.Add(time.Second), nil, nil, "", nil); err != nil {
		t.Fatalf("publishSnapshotOnce() error = %v", err)
	}
	nextSnapshot, ok := snapshotHub.Latest()
	if !ok {
		t.Fatal("SnapshotHub.Latest() ok = false, want next snapshot")
	}
	if nextSnapshot.Seq != 6 {
		t.Fatalf("next snapshot Seq = %d, want 6", nextSnapshot.Seq)
	}
	if !nextSnapshot.Shutdown.Draining {
		t.Fatal("next snapshot Draining = false, want latched shutdown state")
	}
}

func TestShutdownBannerFormatsRunningSessions(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	writeShutdownBanner(&output, []telemetry.Running{
		{
			Issue: telemetry.Issue{
				ID:         "issue-365",
				Identifier: "digitaldrywood/detent#365",
				Title:      "docs(onboarding): tighten setup",
				State:      "In Progress",
			},
			RuntimeSeconds: 724,
		},
	}, 75*time.Second)

	got := output.String()
	for _, want := range []string{
		"shutdown requested — 1 agent session in flight",
		"#365 docs(onboarding)",
		"In Progress",
		"12m 4s",
		"draining: no new work will be dispatched",
		"1m 15s remaining until force quit",
		"press Ctrl+C again to force quit immediately",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("banner missing %q:\n%s", want, got)
		}
	}
}

func TestShutdownProgressFormatsRunningSessions(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	writeShutdownProgress(&output, []telemetry.Running{
		{
			Issue: telemetry.Issue{
				Identifier: "digitaldrywood/detent#641",
			},
			RuntimeSeconds:  724,
			SessionID:       "thread-641-turn-1",
			ProcessIdentity: "4242",
		},
	}, 20*time.Millisecond)

	want := "1 agent session remaining — 20ms remaining until force quit — #641 (12m 4s, session=thread-641-turn-1 process=4242)\n"
	if got := output.String(); got != want {
		t.Fatalf("progress = %q, want %q", got, want)
	}
}

func newShutdownProject(t *testing.T, id string, drainTimeoutMS int) *projectpkg.Project {
	t.Helper()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Agent.Shutdown.DrainTimeoutMS = drainTimeoutMS
	return newShutdownRuntimeProjectWithConfig(t, id, cfg, orchestrator.FakeRunner{})
}

func newShutdownRuntimeProject(t *testing.T, id string, issues []connector.Issue, runner orchestrator.Runner) *projectpkg.Project {
	t.Helper()

	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	cfg.Tracker.Issues = issues
	cfg.Polling.IntervalMS = 60000
	cfg.Agent.MaxConcurrentAgents = 1
	return newShutdownRuntimeProjectWithConfig(t, id, cfg, runner)
}

func newShutdownRuntimeProjectWithConfig(t *testing.T, id string, cfg workflowconfig.Config, runner orchestrator.Runner) *projectpkg.Project {
	t.Helper()

	project, err := projectpkg.New(projectpkg.Config{
		Project: globalconfig.Project{
			ID:      id,
			Workdir: t.TempDir(),
			Weight:  1,
		},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Test workflow prompt."},
	}, projectpkg.Dependencies{Runner: runner})
	if err != nil {
		t.Fatalf("project.New() error = %v", err)
	}
	return project
}

type shutdownBlockingRunner struct {
	started  chan struct{}
	canceled chan struct{}
	release  <-chan struct{}
}

type shutdownBlockingConnector struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (c shutdownBlockingConnector) Name() string {
	return "blocking"
}

func (c shutdownBlockingConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, nil
	}
}

func (shutdownBlockingConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (shutdownBlockingConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (shutdownBlockingConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (shutdownBlockingConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (shutdownBlockingConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (shutdownBlockingConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (r *shutdownBlockingRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	if request.OnUsageUpdate != nil {
		if err := request.OnUsageUpdate(orchestrator.UsageUpdate{
			SessionID:       "thread-641-turn-1",
			ProcessIdentity: "4242",
			Tokens: orchestrator.TokenTotals{
				RuntimeSeconds: 12,
			},
		}); err != nil {
			return orchestrator.RunResult{}, err
		}
	}
	select {
	case r.started <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		select {
		case r.canceled <- struct{}{}:
		default:
		}
		return orchestrator.RunResult{}, ctx.Err()
	case <-r.release:
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
	}
}

func waitForShutdownSession(t *testing.T, registry *projectpkg.Registry, ready func(telemetry.Running) bool) telemetry.Running {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		for _, session := range shutdownRunningSessions(context.Background(), registry, time.Now()) {
			if ready(session) {
				return session
			}
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for shutdown session")
		case <-time.After(time.Millisecond):
		}
	}
}
