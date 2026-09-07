package project_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	configwatcher "github.com/digitaldrywood/detent/internal/config/watcher"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/coordination"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/project"
	routinemodel "github.com/digitaldrywood/detent/internal/routine/model"
	"github.com/digitaldrywood/detent/internal/schedulehealth"
	"github.com/digitaldrywood/detent/internal/scheduleowner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestNewBuildsProjectLifecycleDependencies(t *testing.T) {
	t.Parallel()

	events := hub.New[project.Event]()
	sched := scheduler.NewCountingSemaphore(scheduler.Config{Capacity: 3})
	global := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 8}))
	created := make(chan orchestrator.Config, 1)
	createdDeps := make(chan orchestrator.Dependencies, 1)
	workflowCfg := workflowConfigWithMemoryIssue("issue-1")
	workflowCfg.Identity.Name = "workflow-persona"
	workflowCfg.Identity.GitHubLogin = "workflow-bot"
	workflowCfg.Tracker.Authorization = selector.Selector{
		AssigneeIn: []string{"@me"},
	}

	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:       "detent",
			Pool:     "code",
			Workflow: "workflow.md",
			Workdir:  "/workspace/detent",
			Weight:   2,
			Priority: 10,
			Identity: globalconfig.Identity{
				Name:        "release-captain",
				GitHubLogin: "detent-bot",
			},
			Authorization: selector.Selector{
				Labels: selector.Labels{Include: []string{"release"}},
			},
		},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			Prompt: "Run issue",
		},
	}, project.Dependencies{
		Scheduler:          sched,
		GlobalDispatchGate: global,
		Events:             events,
		Runner:             orchestrator.FakeRunner{},
		OrchestratorFactory: func(cfg orchestrator.Config, deps orchestrator.Dependencies) (*orchestrator.Orchestrator, error) {
			created <- cfg
			createdDeps <- deps
			return orchestrator.New(cfg, deps)
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got.ID() != project.ID("detent") {
		t.Fatalf("ID() = %q, want detent", got.ID())
	}
	if got.Config().Workdir != "/workspace/detent" {
		t.Fatalf("Config().Workdir = %q, want /workspace/detent", got.Config().Workdir)
	}
	if got.Connector().Name() != connector.BackendMemory.String() {
		t.Fatalf("Connector().Name() = %q, want memory", got.Connector().Name())
	}
	if got.Scheduler() != sched {
		t.Fatalf("Scheduler() = %p, want %p", got.Scheduler(), sched)
	}
	if got.Events() != events {
		t.Fatalf("Events() = %p, want %p", got.Events(), events)
	}
	if got.Orchestrator() == nil {
		t.Fatal("Orchestrator() = nil, want configured orchestrator")
	}
	if got.Workflow().Config.Identity.Name != "release-captain" {
		t.Fatalf("Workflow().Config.Identity.Name = %q, want release-captain", got.Workflow().Config.Identity.Name)
	}

	select {
	case cfg := <-created:
		if cfg.MaxConcurrentAgents != 4 {
			t.Fatalf("orchestrator MaxConcurrentAgents = %d, want 4", cfg.MaxConcurrentAgents)
		}
		if cfg.SelectorContext.InstanceLogin != "detent-bot" {
			t.Fatalf("SelectorContext.InstanceLogin = %q, want detent-bot", cfg.SelectorContext.InstanceLogin)
		}
		if len(cfg.Authorization.And) != 2 {
			t.Fatalf("Authorization.And = %#v, want workflow and project selectors", cfg.Authorization.And)
		}
		if cfg.Project.ID != "detent" || cfg.Project.Pool != "code" ||
			cfg.Project.Weight != 2 || cfg.Project.Priority != 10 {
			t.Fatalf("orchestrator Project = %#v, want detent in code pool with weight 2 priority 10", cfg.Project)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for orchestrator factory")
	}

	select {
	case deps := <-createdDeps:
		if deps.GlobalDispatchGate != global {
			t.Fatalf("orchestrator GlobalDispatchGate = %T, want shared gate", deps.GlobalDispatchGate)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for orchestrator dependencies")
	}
}

func TestNewConfiguresScheduledRoutines(t *testing.T) {
	t.Parallel()
	workflowCfg := workflowConfig("memory")
	workflowCfg.Routines = []workflowconfig.Routine{{Name: "dependency-audit", Schedule: "0 3 * * 1", Prompt: "Inspect configured dependency criteria."}}
	configureTestScheduleOwnership(&workflowCfg)
	coordinationStore := &projectCoordinationStore{}
	scheduleRuns := &projectScheduledRunStore{}
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workdir: "/workspace/detent"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg},
	}, project.Dependencies{
		Runner: orchestrator.FakeRunner{}, RoutineStore: &projectRoutineStore{},
		ScheduleStore: coordinationStore, ScheduleRuns: scheduleRuns,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = got.Close() })
	if got.Routines() == nil || !got.Routines().Enabled() {
		t.Fatalf("Routines() = %#v, want enabled manager", got.Routines())
	}
}

func TestNewConfiguresBacklogAdmissionFromSharedWorkflow(t *testing.T) {
	t.Parallel()

	runtimeStore, err := store.Open(context.Background(), store.Config{
		Backend: store.BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtimeStore.Close(); err != nil {
			t.Fatalf("runtimeStore.Close() error = %v", err)
		}
	})

	workflowCfg := workflowConfigWithMemoryIssue("issue-1535")
	workflowCfg.Tracker.Issues[0].State = "Backlog"
	workflowCfg.BacklogAdmission.Enabled = true
	workflowCfg.BacklogAdmission.Sources.States = []string{"Backlog"}
	workflowCfg.BacklogAdmission.TargetState = "Todo"
	workflowCfg.BacklogAdmission.CriteriaSection = "Admission criteria"
	workflowCfg.BacklogAdmission.RequireEffort = true
	workflowCfg.BacklogAdmission.EffortSection = "Issue effort selection"
	configureTestScheduleOwnership(&workflowCfg)
	got, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent", Workdir: "/workspace/detent"},
		Workflow: workflowconfig.Workflow{
			Config: workflowCfg,
			SharedPrompt: `# Workflow

## Admission criteria

- **Evidence** — requires a reproducible signal.

## Issue effort selection

- **medium** — small and mechanical.
- **high** — standard feature work.
`,
		},
	}, project.Dependencies{
		Runner:         orchestrator.FakeRunner{},
		AdmissionStore: runtimeStore,
		ScheduleStore:  &projectCoordinationStore{},
		ScheduleRuns:   runtimeStore,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = got.Close() })
	if got.Admission() == nil || !got.Admission().Enabled() {
		t.Fatalf("Admission() = %#v, want enabled manager", got.Admission())
	}
}

func TestProjectWithDisabledSchedulersDoesNotAcquireOwnership(t *testing.T) {
	t.Parallel()

	workflowCfg := workflowConfig("memory")
	configureTestScheduleOwnership(&workflowCfg)
	coordinationStore := &projectCoordinationStore{}
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workdir: "/workspace/detent"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg},
	}, project.Dependencies{
		Runner: orchestrator.FakeRunner{}, ScheduleStore: coordinationStore,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := got.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := coordinationStore.Calls(); got != 0 {
		t.Fatalf("coordination calls = %d, want 0", got)
	}
}

func TestProjectAcquiresScheduleOwnershipAfterWorkflowReloadEnablesSchedulers(t *testing.T) {
	t.Parallel()

	updates := make(chan configwatcher.Update, 1)
	workflowCfg := workflowConfig("memory")
	configureTestScheduleOwnership(&workflowCfg)
	coordinationStore := &projectCoordinationStore{}
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workdir: "/workspace/detent", Workflow: "workflow.md"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg, Prompt: "disabled"},
	}, project.Dependencies{
		Runner: blockingRunner{}, RoutineStore: &projectRoutineStore{},
		ScheduleStore: coordinationStore, ScheduleRuns: &projectScheduledRunStore{},
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			return fakeWorkflowWatcher{updates: updates}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = got.Stop(context.Background()) })
	waitForWorkflowWatcherArmed(t, got)
	if calls := coordinationStore.Calls(); calls != 0 {
		t.Fatalf("coordination calls before enabling schedules = %d, want 0", calls)
	}

	reloaded := workflowCfg
	reloaded.Routines = []workflowconfig.Routine{{Name: "audit", Schedule: "* * * * *", Prompt: "Inspect."}}
	updates <- configwatcher.Update{Workflow: workflowconfig.Workflow{Config: reloaded, Prompt: "enabled"}}
	waitForWorkflowPrompt(t, got, "enabled")
	waitForCoordinationCalls(t, coordinationStore, 1)
}

func TestScheduleOwnershipAcquisitionFailureDegradesRunningProject(t *testing.T) {
	t.Parallel()

	workflowCfg := workflowConfig("memory")
	workflowCfg.Routines = []workflowconfig.Routine{{Name: "audit", Schedule: "* * * * *", Prompt: "Inspect."}}
	configureTestScheduleOwnership(&workflowCfg)
	coordinationStore := &failingProjectCoordinationStore{called: make(chan struct{})}
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workdir: "/workspace/detent"},
		Workflow: workflowconfig.Workflow{Config: workflowCfg},
	}, project.Dependencies{
		Runner: orchestrator.FakeRunner{}, RoutineStore: &projectRoutineStore{},
		ScheduleStore: coordinationStore, ScheduleRuns: &projectScheduledRunStore{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = got.Stop(context.Background()) })
	select {
	case <-coordinationStore.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for schedule ownership acquisition")
	}
	registry := project.NewRegistry()
	if err := registry.Set(got); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	waitForScheduleOwnershipFault(t, registry)
}

func TestNewLeavesBacklogAdmissionDisabledByDefault(t *testing.T) {
	t.Parallel()

	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workdir: "/workspace/detent"},
		Workflow: workflowconfig.Workflow{Config: workflowConfig("memory")},
	}, project.Dependencies{Runner: orchestrator.FakeRunner{}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = got.Close() })
	if got.Admission() == nil || got.Admission().Enabled() {
		t.Fatalf("Admission() = %#v, want disabled manager", got.Admission())
	}
}

func TestProjectGitHubRuntimeTokenRefreshesAfterAuthFailure(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		requests <- token
		switch token {
		case "Bearer stale-token":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
		case "Bearer fresh-token":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"detent-bot"},"node":{"__typename":"ProjectV2","id":"PVT_1"}}}`))
		default:
			t.Fatalf("Authorization = %q, want stale then fresh token", token)
		}
	}))
	t.Cleanup(server.Close)

	workflowCfg := workflowConfig("github")
	workflowCfg.Tracker.Endpoint = server.URL
	workflowCfg.Tracker.ProjectSlug = "PVT_1"
	var refreshes atomic.Int64
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
		Workflow: workflowconfig.Workflow{Config: workflowCfg},
	}, project.Dependencies{
		GitHubToken: "stale-token",
		RefreshGitHubToken: func(context.Context) (string, error) {
			refreshes.Add(1)
			return "fresh-token", nil
		},
		Runner: orchestrator.FakeRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	authenticator, ok := got.Connector().(connector.Authenticator)
	if !ok {
		t.Fatalf("Connector() = %T, want connector.Authenticator", got.Connector())
	}
	if err := authenticator.Authenticate(context.Background()); err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if refreshes.Load() != 1 {
		t.Fatalf("RefreshGitHubToken() calls = %d, want 1", refreshes.Load())
	}
	if first, second := <-requests, <-requests; first != "Bearer stale-token" || second != "Bearer fresh-token" {
		t.Fatalf("Authorization sequence = %q, %q; want stale then fresh", first, second)
	}
	reporter, ok := got.Connector().(connector.AuthHealthReporter)
	if !ok {
		t.Fatalf("Connector() = %T, want connector.AuthHealthReporter", got.Connector())
	}
	health, ok := reporter.AuthHealth()
	if !ok {
		t.Fatal("AuthHealth() ok = false, want true")
	}
	if health.Status != connector.AuthStatusRecovered {
		t.Fatalf("AuthHealth().Status = %q, want %q", health.Status, connector.AuthStatusRecovered)
	}
}

func TestProjectStartStopPublishesLifecycleEvents(t *testing.T) {
	t.Parallel()

	events := hub.New[project.Event](hub.WithBuffer(2))
	sub, err := events.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
		Workflow: workflowconfig.Workflow{
			Config: workflowConfigWithMemoryIssue("issue-1"),
		},
	}, project.Dependencies{
		Events: events,
		Runner: blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := got.Start(context.Background()); !errors.Is(err, project.ErrAlreadyRunning) {
		t.Fatalf("Start() second error = %v, want %v", err, project.ErrAlreadyRunning)
	}

	started := receiveEvent(t, sub.C())
	if started.ProjectID != got.ID() || started.Kind != project.EventStarted {
		t.Fatalf("started event = %#v, want project started", started)
	}

	if err := got.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := receiveEvent(t, sub.C())
	if stopped.ProjectID != got.ID() || stopped.Kind != project.EventStopped {
		t.Fatalf("stopped event = %#v, want project stopped", stopped)
	}
	if err := got.Stop(context.Background()); !errors.Is(err, project.ErrNotRunning) {
		t.Fatalf("Stop() second error = %v, want %v", err, project.ErrNotRunning)
	}
	if err := got.Start(context.Background()); !errors.Is(err, project.ErrProjectStopped) {
		t.Fatalf("Start() after Stop error = %v, want %v", err, project.ErrProjectStopped)
	}
}

func TestProjectCloseStopsAndClosesConnector(t *testing.T) {
	t.Parallel()

	events := hub.New[project.Event](hub.WithBuffer(2))
	sub, err := events.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	projectConnector := newCloseTrackingConnector()
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
		Workflow: workflowconfig.Workflow{Config: workflowConfig("memory")},
	}, project.Dependencies{
		Connector: projectConnector,
		Events:    events,
		Runner:    blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	receiveEvent(t, sub.C())
	if err := got.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stopped := receiveEvent(t, sub.C())
	if stopped.ProjectID != got.ID() || stopped.Kind != project.EventStopped {
		t.Fatalf("stopped event = %#v, want project stopped", stopped)
	}
	assertConnectorClosed(t, projectConnector)
	if err := got.Close(); err != nil {
		t.Fatalf("Close() second error = %v", err)
	}
	if err := got.Start(context.Background()); !errors.Is(err, project.ErrProjectStopped) {
		t.Fatalf("Start() after Close error = %v, want %v", err, project.ErrProjectStopped)
	}
}

func TestProjectAppliesWorkflowReloadsToRunningOrchestrator(t *testing.T) {
	t.Parallel()

	updates := make(chan configwatcher.Update, 1)
	runner := newProjectBlockingRunner()
	initial := workflowConfig("memory")
	initial.Polling.IntervalMS = int(time.Hour / time.Millisecond)
	initial.Tracker.ActiveStates = []string{"Todo"}

	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:       "detent",
			Workflow: "workflow.md",
			Weight:   1,
			Identity: globalconfig.Identity{
				Name:        "release-captain",
				GitHubLogin: "detent-bot",
			},
		},
		Workflow: workflowconfig.Workflow{
			Config: initial,
			Prompt: "initial",
		},
	}, project.Dependencies{
		Runner: runner,
		WorkflowWatcherFactory: func(path string) (project.WorkflowWatcher, error) {
			if path != "workflow.md" {
				t.Fatalf("watch path = %q, want workflow.md", path)
			}
			return fakeWorkflowWatcher{updates: updates}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected run before workflow reload = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}

	issue := connector.NewIssue()
	issue.ID = "issue-1"
	issue.Identifier = "issue-1"
	issue.Title = "Reload workflow"
	issue.State = "Todo"

	reloaded := initial
	reloaded.Polling.IntervalMS = 60000
	reloaded.Tracker.Issues = []connector.Issue{issue}
	updates <- configwatcher.Update{
		Workflow: workflowconfig.Workflow{
			Config: reloaded,
			Prompt: "reloaded",
		},
	}

	waitForWorkflowPrompt(t, got, "reloaded")
	if got.Workflow().Prompt != "reloaded" {
		t.Fatalf("Workflow().Prompt = %q, want reloaded", got.Workflow().Prompt)
	}
	issues, err := got.Connector().FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("Connector().FetchCandidateIssues() error = %v", err)
	}
	if len(issues) != 1 || issues[0].ID != "issue-1" {
		t.Fatalf("reloaded connector issues = %#v, want issue-1", issues)
	}

	close(runner.release)
}

func TestProjectReestablishesClosedWorkflowWatcher(t *testing.T) {
	t.Parallel()

	firstUpdates := make(chan configwatcher.Update)
	close(firstUpdates)
	secondUpdates := make(chan configwatcher.Update, 1)
	rearmed := make(chan struct{})
	var watcherCreates atomic.Int32
	var logs lockedBuffer

	initial := workflowConfig("memory")
	initial.Polling.IntervalMS = int(time.Hour / time.Millisecond)
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workflow: "workflow.md", Weight: 1},
		Workflow: workflowconfig.Workflow{Config: initial, Prompt: "initial"},
	}, project.Dependencies{
		Runner: blockingRunner{},
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			if watcherCreates.Add(1) == 1 {
				return fakeWorkflowWatcher{updates: firstUpdates}, nil
			}
			select {
			case <-rearmed:
			default:
				close(rearmed)
			}
			return fakeWorkflowWatcher{updates: secondUpdates}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	})

	select {
	case <-rearmed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for workflow watcher re-establishment")
	}
	if !strings.Contains(logs.String(), "workflow watcher stopped") {
		t.Fatalf("logs = %q, want watcher failure warning", logs.String())
	}
	waitForWorkflowWatcherArmed(t, got)

	reloaded := initial
	reloaded.Polling.IntervalMS = 60000
	secondUpdates <- configwatcher.Update{
		Workflow: workflowconfig.Workflow{Config: reloaded, Prompt: "reloaded"},
	}
	waitForWorkflowPrompt(t, got, "reloaded")
	if got.WorkflowSourceStatus().LastWatchEventAt.IsZero() {
		t.Fatal("LastWatchEventAt is zero after re-established watcher update")
	}
}

func TestProjectReconcilesWorkflowWhenWatcherMissesEdit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	writeProjectGateWorkflow(t, workflowPath, 60000, "", "initial")
	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	stalledUpdates := make(chan configwatcher.Update)
	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workflow: workflowPath, Weight: 1},
		Workflow: workflow,
	}, project.Dependencies{
		Runner:                    blockingRunner{},
		WorkflowReconcileInterval: 20 * time.Millisecond,
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			return fakeWorkflowWatcher{updates: stalledUpdates}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	})

	writeProjectGateWorkflow(t, workflowPath, 60000, "", "reconciled")
	waitForWorkflowPrompt(t, got, "reconciled\n")

	status := got.WorkflowSourceStatus()
	if status.LastReconcileAt.IsZero() {
		t.Fatal("LastReconcileAt is zero, want periodic reconciliation timestamp")
	}
	if status.Hash == "" || status.Hash != got.Workflow().SourceHash {
		t.Fatalf("workflow source hash = %q, workflow hash = %q", status.Hash, got.Workflow().SourceHash)
	}
}

func TestProjectUnpauseReloadsWorkflowEditedWhilePaused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	writeProjectGateWorkflow(t, workflowPath, 60000, "", "initial")
	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workflow: workflowPath, Weight: 1, Paused: true},
		Workflow: workflow,
	}, project.Dependencies{
		Runner: blockingRunner{},
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			return fakeWorkflowWatcher{updates: make(chan configwatcher.Update)}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	writeProjectGateWorkflow(t, workflowPath, 60000, "", "reloaded before unpause")
	if err := got.Unpause(context.Background()); err != nil {
		t.Fatalf("Unpause() error = %v", err)
	}
	t.Cleanup(func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	})

	if got.Workflow().Prompt != "reloaded before unpause\n" {
		t.Fatalf("Workflow().Prompt = %q, want reloaded workflow", got.Workflow().Prompt)
	}
}

func TestProjectUnpauseKeepsProjectPausedWhenWorkflowReloadFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	writeProjectGateWorkflow(t, workflowPath, 60000, "", "initial")
	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	got, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: "detent", Workflow: workflowPath, Weight: 1, Paused: true},
		Workflow: workflow,
	}, project.Dependencies{Runner: blockingRunner{}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	})

	if err := os.WriteFile(workflowPath, []byte("---\ntracker: [\n---\ninvalid\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := got.Unpause(context.Background()); err == nil {
		t.Fatal("Unpause() error = nil, want workflow reload failure")
	}
	if !got.Paused() {
		t.Fatal("Paused() = false after failed workflow reload, want true")
	}
	if got.Workflow().Prompt != "initial\n" {
		t.Fatalf("Workflow().Prompt = %q, want original workflow", got.Workflow().Prompt)
	}
}

func TestProjectWorkflowReloadRefreshesRestartDependencies(t *testing.T) {
	t.Parallel()

	updates := make(chan configwatcher.Update, 1)
	runner := newProjectBlockingRunner()
	configs := make(chan orchestrator.Config, 2)
	connectors := make(chan connector.Connector, 2)
	initial := workflowConfig("memory")
	initial.Polling.IntervalMS = int(time.Hour / time.Millisecond)
	initial.Tracker.ActiveStates = []string{"Todo"}

	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:       "detent",
			Workflow: "workflow.md",
			Weight:   1,
			Identity: globalconfig.Identity{
				Name:        "release-captain",
				GitHubLogin: "detent-bot",
			},
		},
		Workflow: workflowconfig.Workflow{
			Config: initial,
			Prompt: "initial",
		},
	}, project.Dependencies{
		Runner: runner,
		OrchestratorFactory: func(cfg orchestrator.Config, deps orchestrator.Dependencies) (*orchestrator.Orchestrator, error) {
			configs <- cfg
			connectors <- deps.Connector
			return orchestrator.New(cfg, deps)
		},
		WorkflowWatcherFactory: func(string) (project.WorkflowWatcher, error) {
			return fakeWorkflowWatcher{updates: updates}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := receiveOrchestratorConfig(t, configs)
	if cfg.PollInterval != time.Hour {
		t.Fatalf("initial PollInterval = %v, want %v", cfg.PollInterval, time.Hour)
	}
	if cfg.SelectorContext.InstanceLogin != "detent-bot" {
		t.Fatalf("initial SelectorContext.InstanceLogin = %q, want detent-bot", cfg.SelectorContext.InstanceLogin)
	}
	_ = receiveConnector(t, connectors)

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected run before workflow reload = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}

	issue := connector.NewIssue()
	issue.ID = "issue-1"
	issue.Identifier = "issue-1"
	issue.Title = "Reload workflow"
	issue.State = "Todo"

	reloaded := initial
	reloaded.Polling.IntervalMS = 60000
	reloaded.Tracker.Issues = []connector.Issue{issue}
	updates <- configwatcher.Update{
		Workflow: workflowconfig.Workflow{
			Config: reloaded,
			Prompt: "reloaded",
		},
	}
	waitForWorkflowPrompt(t, got, "reloaded")

	if err := got.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	if err := got.Unpause(context.Background()); err != nil {
		t.Fatalf("Unpause() error = %v", err)
	}

	cfg = receiveOrchestratorConfig(t, configs)
	if cfg.PollInterval != time.Minute {
		t.Fatalf("restarted PollInterval = %v, want %v", cfg.PollInterval, time.Minute)
	}
	if cfg.SelectorContext.InstanceLogin != "detent-bot" {
		t.Fatalf("restarted SelectorContext.InstanceLogin = %q, want detent-bot", cfg.SelectorContext.InstanceLogin)
	}
	restartedConnector := receiveConnector(t, connectors)
	issues, err := restartedConnector.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("restarted connector FetchCandidateIssues() error = %v", err)
	}
	if len(issues) != 1 || issues[0].ID != "issue-1" {
		t.Fatalf("restarted connector issues = %#v, want issue-1", issues)
	}

	close(runner.release)
	if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestProjectHotReloadsWorkflowFileWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	writeProjectGateWorkflow(t, workflowPath, int(time.Hour/time.Millisecond), "", "initial")

	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	events := hub.New[project.Event](hub.WithBuffer(4))
	sub, err := events.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	runner := newProjectBlockingRunner()
	watcherReady := make(chan struct{})
	var orchestratorCreates atomic.Int32
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:       "detent",
			Workflow: workflowPath,
			Weight:   1,
		},
		Workflow: workflow,
	}, project.Dependencies{
		Events: events,
		Runner: runner,
		OrchestratorFactory: func(cfg orchestrator.Config, deps orchestrator.Dependencies) (*orchestrator.Orchestrator, error) {
			orchestratorCreates.Add(1)
			return orchestrator.New(cfg, deps)
		},
		WorkflowWatcherFactory: func(path string) (project.WorkflowWatcher, error) {
			if path != workflowPath {
				t.Fatalf("watch path = %q, want %q", path, workflowPath)
			}
			watcher, err := configwatcher.New(path, configwatcher.WithDebounce(10*time.Millisecond))
			if err != nil {
				return nil, err
			}
			return &readyWorkflowWatcher{watcher: watcher, ready: watcherReady}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if orchestratorCreates.Load() != 1 {
		t.Fatalf("orchestrator creations = %d, want 1", orchestratorCreates.Load())
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	started := receiveEvent(t, sub.C())
	if started.Kind != project.EventStarted {
		t.Fatalf("started event = %#v, want project started", started)
	}
	waitForWorkflowWatcher(t, watcherReady)

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected run before workflow reload = %#v", request)
	case <-time.After(25 * time.Millisecond):
	}

	writeProjectGateWorkflow(t, workflowPath, 60000, "issue-43", "reloaded")
	waitForWorkflowPrompt(t, got, "reloaded\n")
	if got.Workflow().Prompt != "reloaded\n" {
		t.Fatalf("Workflow().Prompt = %q, want reloaded", got.Workflow().Prompt)
	}
	issues, err := got.Connector().FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("Connector().FetchCandidateIssues() error = %v", err)
	}
	if len(issues) != 1 || issues[0].ID != "issue-43" {
		t.Fatalf("reloaded connector issues = %#v, want issue-43", issues)
	}
	if orchestratorCreates.Load() != 1 {
		t.Fatalf("orchestrator creations after reload = %d, want no restart", orchestratorCreates.Load())
	}

	reloaded := receiveEvent(t, sub.C())
	if reloaded.ProjectID != got.ID() || reloaded.Kind != project.EventWorkflowReloaded {
		t.Fatalf("workflow reload event = %#v, want project workflow reload", reloaded)
	}

	select {
	case event := <-sub.C():
		t.Fatalf("unexpected extra event after hot reload = %#v", event)
	case <-time.After(25 * time.Millisecond):
	}

	close(runner.release)
	if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestProjectHotReloadAppliesRuntimeGitHubTokenBeforeValidation(t *testing.T) {
	dir := t.TempDir()
	workflowPath := filepath.Join(dir, "WORKFLOW.md")
	writeProjectGitHubWorkflow(t, workflowPath, int(time.Hour/time.Millisecond), "initial")

	workflow, err := workflowconfig.LoadWorkflow(workflowPath)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	connectorConfigs := make(chan workflowconfig.Config, 2)
	var logs lockedBuffer
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:       "pyroapex",
			Workflow: workflowPath,
			Weight:   1,
		},
		Workflow: workflow,
	}, project.Dependencies{
		GitHubToken: "global-token",
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
		ConnectorFactory: func(cfg workflowconfig.Config) (connector.Connector, error) {
			connectorConfigs <- cfg
			return provisioningConnector{}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	initial := receiveConnectorConfig(t, connectorConfigs)
	if initial.Tracker.APIKey != "global-token" {
		t.Fatalf("initial Tracker.APIKey = %q, want runtime token", initial.Tracker.APIKey)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := got.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	waitForWorkflowWatcherArmed(t, got)

	writeProjectGitHubWorkflow(t, workflowPath, 60000, "reloaded")
	waitForProjectLog(t, &logs, "workflow reloaded")

	reloaded := receiveConnectorConfig(t, connectorConfigs)
	if reloaded.Tracker.APIKey != "global-token" {
		t.Fatalf("reloaded Tracker.APIKey = %q, want runtime token", reloaded.Tracker.APIKey)
	}
	if got.Workflow().Prompt != "reloaded\n" {
		t.Fatalf("Workflow().Prompt = %q, want reloaded", got.Workflow().Prompt)
	}

	if err := os.WriteFile(workflowPath, []byte("---\ntracker: [\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", workflowPath, err)
	}
	waitForProjectLog(t, &logs, "workflow reload failed")
	waitForWorkflowReloadError(t, got)
	select {
	case extra := <-connectorConfigs:
		t.Fatalf("connector rebuilt after invalid reload = %#v", extra)
	default:
	}
	if got.Workflow().Prompt != "reloaded\n" {
		t.Fatalf("Workflow().Prompt after invalid reload = %q, want last valid workflow", got.Workflow().Prompt)
	}
	sourceStatus := got.WorkflowSourceStatus()
	if !strings.Contains(sourceStatus.LastReloadError, "workflow reload failed") || sourceStatus.ReloadFailedAt.IsZero() {
		t.Fatalf("WorkflowSourceStatus() = %#v, want persistent reload failure", sourceStatus)
	}
	registry := project.NewRegistry()
	if err := registry.Set(got); err != nil {
		t.Fatalf("Registry.Set() error = %v", err)
	}
	health := registry.Health()
	if len(health) != 1 || health[0].Status != project.HealthStatusDegraded || !strings.Contains(health[0].LastError, "workflow reload failed") {
		t.Fatalf("Registry.Health() = %#v, want degraded last-good project", health)
	}

	writeProjectGitHubWorkflow(t, workflowPath, 60000, "recovered")
	waitForWorkflowPrompt(t, got, "recovered\n")
	_ = receiveConnectorConfig(t, connectorConfigs)
	if recovered := got.WorkflowSourceStatus(); recovered.LastReloadError != "" || !recovered.ReloadFailedAt.IsZero() {
		t.Fatalf("WorkflowSourceStatus() after recovery = %#v, want cleared reload failure", recovered)
	}
	health = registry.Health()
	if len(health) != 1 || health[0].Status != project.HealthStatusReady || health[0].LastError != "" {
		t.Fatalf("Registry.Health() after recovery = %#v, want ready project", health)
	}
}

func TestProjectStartRunsProvisionerWhenAutoProvisionEnabled(t *testing.T) {
	t.Parallel()

	provisioned := false
	c := provisioningConnector{
		provision: func(context.Context) error {
			provisioned = true
			return nil
		},
	}
	got, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent", Weight: 1},
		Workflow: workflowconfig.Workflow{
			Config: workflowConfig("memory"),
		},
	}, project.Dependencies{
		Connector: c,
		Runner:    blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !provisioned {
		t.Fatal("Provision() was not called")
	}
	if err := got.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestProjectStartSkipsProvisionerWhenAutoProvisionDisabled(t *testing.T) {
	t.Parallel()

	c := provisioningConnector{
		provision: func(context.Context) error {
			t.Fatal("Provision() called")
			return nil
		},
	}
	cfg := workflowConfig("memory")
	cfg.Tracker.AutoProvision = false
	got, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent", Weight: 1},
		Workflow: workflowconfig.Workflow{
			Config: cfg,
		},
	}, project.Dependencies{
		Connector: c,
		Runner:    blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := got.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestProjectStartReturnsProvisionerError(t *testing.T) {
	t.Parallel()

	want := errors.New("provision failed")
	c := provisioningConnector{
		provision: func(context.Context) error {
			return want
		},
	}
	got, err := project.New(project.Config{
		Project: globalconfig.Project{ID: "detent", Weight: 1},
		Workflow: workflowconfig.Workflow{
			Config: workflowConfig("memory"),
		},
	}, project.Dependencies{
		Connector: c,
		Runner:    blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Start() error = %v, want %v", err, want)
	}
	if got.Running() {
		t.Fatal("Running() = true, want false")
	}
}

func TestNewRejectsInvalidProjectConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  project.Config
		deps project.Dependencies
		want error
	}{
		{
			name: "missing id",
			cfg:  project.Config{},
			want: project.ErrMissingProjectID,
		},
		{
			name: "missing connector",
			cfg: project.Config{
				Project:  globalconfig.Project{ID: "memory"},
				Workflow: workflowconfig.Workflow{Config: workflowConfig("memory")},
			},
			deps: project.Dependencies{
				ConnectorFactory: func(workflowconfig.Config) (connector.Connector, error) {
					return nil, nil //nolint:nilnil // Test verifies project construction rejects a nil connector without a factory error.
				},
			},
			want: project.ErrMissingConnector,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := project.New(tt.cfg, tt.deps)
			if got != nil {
				t.Fatalf("New() project = %#v, want nil", got)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("New() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestProjectWaitersReceiveRunError(t *testing.T) {
	t.Parallel()

	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
		Workflow: workflowconfig.Workflow{
			Config: workflowConfigWithMemoryIssue("issue-1"),
		},
	}, project.Dependencies{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	runCtx := newManualDeadlineContext()
	if err := got.Start(runCtx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	errs := make(chan error, 2)
	waitersReady := make(chan struct{}, 2)
	for range 2 {
		waitCtx := newObservedDoneContext(waitersReady)
		go func() {
			errs <- got.Wait(waitCtx)
		}()
	}
	for range 2 {
		<-waitersReady
	}
	runCtx.expire()

	for range 2 {
		if err := <-errs; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait() error = %v, want %v", err, context.DeadlineExceeded)
		}
	}
}

func TestProjectStartRejectsPausedProject(t *testing.T) {
	t.Parallel()

	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "paused",
			Paused: true,
			Weight: 1,
		},
	}, project.Dependencies{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); !errors.Is(err, project.ErrProjectPaused) {
		t.Fatalf("Start() error = %v, want %v", err, project.ErrProjectPaused)
	}
}

func TestProjectUnpauseRebuildsGlobalDispatchCandidate(t *testing.T) {
	t.Parallel()

	workflowCfg := workflowConfigWithMemoryIssue("issue-paused")
	workflowCfg.Tracker.Issues[0].State = "Todo"
	workflowCfg.Tracker.Issues[0].Title = "Run initially paused project"
	workflowCfg.Tracker.Issues[0].AssignedToWorker = true
	runner := newProjectBlockingRunner()
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "paused",
			Paused: true,
			Weight: 1,
		},
		Workflow: workflowconfig.Workflow{Config: workflowCfg, Prompt: "Run paused project."},
	}, project.Dependencies{
		Runner:             runner,
		GlobalDispatchGate: scheduler.NewGlobalDispatchGate(scheduler.NewWeightedFair(scheduler.Config{Capacity: 1})),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if !got.Running() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := got.Stop(ctx); err != nil && !errors.Is(err, project.ErrNotRunning) {
			cancel()
			t.Fatalf("Stop() error = %v", err)
		}
		cancel()
	})

	if err := got.Start(context.Background()); !errors.Is(err, project.ErrProjectPaused) {
		t.Fatalf("Start() error = %v, want %v", err, project.ErrProjectPaused)
	}
	if err := got.Unpause(context.Background()); err != nil {
		t.Fatalf("Unpause() error = %v", err)
	}

	select {
	case request := <-runner.started:
		if request.Issue.ID != "issue-paused" {
			t.Fatalf("RunRequest.Issue.ID = %q, want issue-paused", request.Issue.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unpaused project dispatch")
	}
}

func TestProjectPauseUnpauseRestartsProject(t *testing.T) {
	t.Parallel()

	events := hub.New[project.Event](hub.WithBuffer(4))
	sub, err := events.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
	}, project.Dependencies{
		Events: events,
		Runner: blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if event := receiveEvent(t, sub.C()); event.Kind != project.EventStarted {
		t.Fatalf("first event = %#v, want started", event)
	}

	if err := got.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	stopped := receiveEvent(t, sub.C())
	paused := receiveEvent(t, sub.C())
	if stopped.Kind != project.EventStopped || paused.Kind != project.EventPaused {
		t.Fatalf("pause events = %#v %#v, want stopped then paused", stopped, paused)
	}
	if !got.Paused() {
		t.Fatal("Paused() = false, want true")
	}

	if err := got.Unpause(context.Background()); err != nil {
		t.Fatalf("Unpause() error = %v", err)
	}
	unpaused := receiveEvent(t, sub.C())
	restarted := receiveEvent(t, sub.C())
	if unpaused.Kind != project.EventStarted || restarted.Kind != project.EventUnpaused {
		t.Fatalf("unpause events = %#v %#v, want started then unpaused", unpaused, restarted)
	}
	if got.Paused() {
		t.Fatal("Paused() = true, want false")
	}
}

func TestProjectPauseDoesNotMarkPausedWhenShutdownTimesOut(t *testing.T) {
	t.Parallel()

	blocker := newPauseBlockingConnector()
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
	}, project.Dependencies{
		Connector: blocker,
		Runner:    blockingRunner{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	blocker.waitEntered(t)

	pauseCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := got.Pause(pauseCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Pause() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if got.Paused() {
		t.Fatal("Paused() = true after failed Pause, want false")
	}

	blocker.release()
	if err := got.Wait(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
}

func TestProjectUnpauseKeepsProjectPausedWhenRestartFails(t *testing.T) {
	t.Parallel()

	events := hub.New[project.Event](hub.WithBuffer(4))
	sub, err := events.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	calls := 0
	got, err := project.New(project.Config{
		Project: globalconfig.Project{
			ID:     "detent",
			Weight: 1,
		},
	}, project.Dependencies{
		Events: events,
		Runner: blockingRunner{},
		OrchestratorFactory: func(cfg orchestrator.Config, deps orchestrator.Dependencies) (*orchestrator.Orchestrator, error) {
			calls++
			if calls > 1 {
				return nil, errors.New("recreate failed")
			}
			return orchestrator.New(cfg, deps)
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := got.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	receiveEvent(t, sub.C())
	if err := got.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	receiveEvent(t, sub.C())
	receiveEvent(t, sub.C())

	if err := got.Unpause(context.Background()); err == nil || err.Error() != "create project orchestrator: recreate failed" {
		t.Fatalf("Unpause() error = %v, want recreate failure", err)
	}
	if !got.Paused() {
		t.Fatal("Paused() = false after failed Unpause, want true")
	}
}

func workflowConfigWithMemoryIssue(id string) workflowconfig.Config {
	cfg := workflowConfig("memory")
	cfg.Agent.MaxConcurrentAgents = 4
	cfg.Polling.IntervalMS = 60000
	cfg.Tracker.Issues = []connector.Issue{{
		ID:         id,
		Identifier: id,
		State:      "Done",
	}}
	return cfg
}

func workflowConfig(kind string) workflowconfig.Config {
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = kind
	return cfg
}

func configureTestScheduleOwnership(cfg *workflowconfig.Config) {
	cfg.ScheduleOwnership = scheduleowner.Config{
		Enabled: true, Backend: scheduleowner.BackendGitHubRef, Key: "example/detent",
		Repository: "example/coordination", Branch: scheduleowner.DefaultBranch,
		LeaseSeconds: scheduleowner.DefaultLeaseSeconds, HeartbeatSeconds: scheduleowner.DefaultHeartbeatSeconds,
		RetrySeconds: scheduleowner.DefaultRetrySeconds, MaxClockSkewSeconds: scheduleowner.DefaultMaxClockSkewSeconds,
	}
}

type projectCoordinationStore struct {
	mu      sync.Mutex
	calls   int
	record  coordination.Record
	version int
}

func (s *projectCoordinationStore) Get(context.Context, string) (coordination.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.record, s.record.Version != "", nil
}

func (s *projectCoordinationStore) CompareAndSwap(_ context.Context, _ string, expected string, value []byte) (coordination.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.record.Version != expected {
		return coordination.Record{}, false, nil
	}
	s.version++
	s.record = coordination.Record{Value: append([]byte(nil), value...), Version: fmt.Sprintf("v%d", s.version), ModifiedAt: time.Now().UTC()}
	return s.record, true, nil
}

func (s *projectCoordinationStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type projectScheduledRunStore struct {
	mu   sync.Mutex
	runs []schedulehealth.Run
}

type failingProjectCoordinationStore struct {
	once   sync.Once
	called chan struct{}
}

func (s *failingProjectCoordinationStore) Get(context.Context, string) (coordination.Record, bool, error) {
	s.once.Do(func() { close(s.called) })
	return coordination.Record{}, false, errors.New("coordination unavailable")
}

func (s *failingProjectCoordinationStore) CompareAndSwap(context.Context, string, string, []byte) (coordination.Record, bool, error) {
	return coordination.Record{}, false, errors.New("coordination unavailable")
}

func (s *projectScheduledRunStore) LatestScheduledRun(_ context.Context, projectID string, scheduleID string) (schedulehealth.Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := len(s.runs) - 1; index >= 0; index-- {
		run := s.runs[index]
		if run.ProjectID == projectID && run.ScheduleID == scheduleID {
			return run, true, nil
		}
	}
	return schedulehealth.Run{}, false, nil
}

func (s *projectScheduledRunStore) RecordScheduledRun(_ context.Context, run schedulehealth.Run) error {
	s.mu.Lock()
	s.runs = append(s.runs, run)
	s.mu.Unlock()
	return nil
}

type blockingRunner struct{}

type projectRoutineStore struct {
	records []routinemodel.RunRecord
}

func (s *projectRoutineStore) LatestRoutineRun(_ context.Context, projectID string, name string) (routinemodel.RunRecord, bool, error) {
	for index := len(s.records) - 1; index >= 0; index-- {
		if s.records[index].ProjectID == projectID && s.records[index].RoutineName == name {
			return s.records[index], true, nil
		}
	}
	return routinemodel.RunRecord{}, false, nil
}

func (s *projectRoutineStore) RecordRoutineRun(_ context.Context, record routinemodel.RunRecord) error {
	s.records = append(s.records, record)
	return nil
}

func (s *projectRoutineStore) OpenRoutineIssueIDs(_ context.Context, projectID string, name string) ([]string, error) {
	var issueIDs []string
	for _, record := range s.records {
		if record.ProjectID != projectID || record.RoutineName != name {
			continue
		}
		for _, issue := range record.Issues {
			issueIDs = append(issueIDs, issue.ID)
		}
	}
	return issueIDs, nil
}

func (s *projectRoutineStore) RecordRoutineIssue(context.Context, string, string, routinemodel.IssueRecord) error {
	return nil
}

func (s *projectRoutineStore) CloseRoutineIssues(context.Context, string, string, []string) error {
	return nil
}

func (blockingRunner) Run(ctx context.Context, _ orchestrator.RunRequest) (orchestrator.RunResult, error) {
	<-ctx.Done()
	return orchestrator.RunResult{}, ctx.Err()
}

type releaseBlockingRunner struct {
	release <-chan struct{}
}

func (r releaseBlockingRunner) Run(ctx context.Context, _ orchestrator.RunRequest) (orchestrator.RunResult, error) {
	<-r.release
	return orchestrator.RunResult{}, ctx.Err()
}

type manualDeadlineContext struct {
	done chan struct{}
	once sync.Once
}

func newManualDeadlineContext() *manualDeadlineContext {
	return &manualDeadlineContext{done: make(chan struct{})}
}

func (c *manualDeadlineContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *manualDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *manualDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *manualDeadlineContext) Value(any) any {
	return nil
}

func (c *manualDeadlineContext) expire() {
	c.once.Do(func() {
		close(c.done)
	})
}

type observedDoneContext struct {
	ready chan<- struct{}
	once  sync.Once
}

func newObservedDoneContext(ready chan<- struct{}) *observedDoneContext {
	return &observedDoneContext{ready: ready}
}

func (c *observedDoneContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() {
		c.ready <- struct{}{}
	})
	return nil
}

func (c *observedDoneContext) Err() error {
	return nil
}

func (c *observedDoneContext) Value(any) any {
	return nil
}

type pauseBlockingConnector struct {
	entered  chan struct{}
	releasec chan struct{}
	once     sync.Once
}

func newPauseBlockingConnector() *pauseBlockingConnector {
	return &pauseBlockingConnector{
		entered:  make(chan struct{}),
		releasec: make(chan struct{}),
	}
}

func (c *pauseBlockingConnector) Name() string {
	return "pause-blocking"
}

func (c *pauseBlockingConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	c.once.Do(func() {
		close(c.entered)
	})
	<-c.releasec
	return nil, ctx.Err()
}

func (c *pauseBlockingConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) CreateComment(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) UpdateIssueState(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) SetAssignee(context.Context, string, string) error {
	return connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) SetField(context.Context, string, string, string) error {
	return connector.ErrNotImplemented
}

func (c *pauseBlockingConnector) waitEntered(t *testing.T) {
	t.Helper()

	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connector fetch")
	}
}

func (c *pauseBlockingConnector) release() {
	close(c.releasec)
}

type projectBlockingRunner struct {
	started chan orchestrator.RunRequest
	release chan struct{}
}

func newProjectBlockingRunner() *projectBlockingRunner {
	return &projectBlockingRunner{
		started: make(chan orchestrator.RunRequest, 1),
		release: make(chan struct{}),
	}
}

func (r *projectBlockingRunner) Run(ctx context.Context, request orchestrator.RunRequest) (orchestrator.RunResult, error) {
	select {
	case r.started <- request:
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}

	select {
	case <-r.release:
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
	case <-ctx.Done():
		return orchestrator.RunResult{}, ctx.Err()
	}
}

type fakeWorkflowWatcher struct {
	updates <-chan configwatcher.Update
}

func (w fakeWorkflowWatcher) Watch(context.Context) (<-chan configwatcher.Update, error) {
	return w.updates, nil
}

type readyWorkflowWatcher struct {
	watcher project.WorkflowWatcher
	ready   chan<- struct{}
	once    sync.Once
}

func (w *readyWorkflowWatcher) Watch(ctx context.Context) (<-chan configwatcher.Update, error) {
	updates, err := w.watcher.Watch(ctx)
	if err == nil {
		w.once.Do(func() {
			close(w.ready)
		})
	}
	return updates, err
}

func waitForWorkflowWatcher(t *testing.T, ready <-chan struct{}) {
	t.Helper()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow watcher")
	}
}

func waitForProjectLog(t *testing.T, logs *lockedBuffer, want string) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)

	for {
		if strings.Contains(logs.String(), want) {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out waiting for project log %q; logs = %q", want, logs.String())
		}
	}
}

func waitForWorkflowReloadError(t *testing.T, got *project.Project) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)

	for {
		status := got.WorkflowSourceStatus()
		if status.LastReloadError != "" && !status.ReloadFailedAt.IsZero() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out waiting for workflow reload failure; status = %#v", got.WorkflowSourceStatus())
		}
	}
}

func waitForWorkflowWatcherArmed(t *testing.T, got *project.Project) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)

	for {
		if got.WorkflowSourceStatus().WatcherArmed {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("timed out waiting for workflow watcher to become armed")
		}
	}
}

func waitForWorkflowPrompt(t *testing.T, got *project.Project, want string) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(time.Second)

	for {
		if got.Workflow().Prompt == want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out waiting for workflow prompt %q, got %q", want, got.Workflow().Prompt)
		}
	}
}

func waitForCoordinationCalls(t *testing.T, store *projectCoordinationStore, minimum int) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(time.Second)
	for {
		if calls := store.Calls(); calls >= minimum {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out waiting for %d coordination calls; got %d", minimum, store.Calls())
		}
	}
}

func waitForScheduleOwnershipFault(t *testing.T, registry *project.Registry) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)
	for {
		health := registry.Health()
		if len(health) == 1 && health[0].Status == project.HealthStatusDegraded && strings.Contains(health[0].LastError, "coordination unavailable") {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("Registry.Health() = %#v, want persistent schedule ownership fault", health)
		}
	}
}

func writeProjectGateWorkflow(t *testing.T, path string, intervalMS int, issueID string, prompt string) {
	t.Helper()

	issues := ""
	if issueID != "" {
		issues = fmt.Sprintf(`  issues:
    - id: %s
      identifier: %s
      title: Hot reload gate
      state: Todo
      assigned_to_worker: true
`, issueID, issueID)
	}

	raw := fmt.Sprintf(`---
tracker:
  kind: memory
%spolling:
  interval_ms: %d
---
%s
`, issues, intervalMS, prompt)
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func writeProjectGitHubWorkflow(t *testing.T, path string, intervalMS int, prompt string) {
	t.Helper()

	raw := fmt.Sprintf(`---
tracker:
  kind: github
  project_slug: PVT_pyroapex
polling:
  interval_ms: %d
---
%s
`, intervalMS, prompt)
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func receiveOrchestratorConfig(t *testing.T, ch <-chan orchestrator.Config) orchestrator.Config {
	t.Helper()

	select {
	case cfg := <-ch:
		return cfg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for orchestrator config")
	}

	return orchestrator.Config{}
}

func receiveConnectorConfig(t *testing.T, ch <-chan workflowconfig.Config) workflowconfig.Config {
	t.Helper()

	select {
	case cfg := <-ch:
		return cfg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connector config")
	}

	return workflowconfig.Config{}
}

func receiveConnector(t *testing.T, ch <-chan connector.Connector) connector.Connector {
	t.Helper()

	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connector")
	}

	return nil
}

type provisioningConnector struct {
	provision func(context.Context) error
}

func (provisioningConnector) Name() string {
	return "provisioning"
}

func (provisioningConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (provisioningConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (provisioningConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (provisioningConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (provisioningConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (provisioningConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (provisioningConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (c provisioningConnector) Provision(ctx context.Context) error {
	if c.provision == nil {
		return nil
	}
	return c.provision(ctx)
}

type closeTrackingConnector struct {
	closed chan struct{}
	once   sync.Once
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func newCloseTrackingConnector() *closeTrackingConnector {
	return &closeTrackingConnector{closed: make(chan struct{})}
}

func (c *closeTrackingConnector) Name() string {
	return "close-tracking"
}

func (c *closeTrackingConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *closeTrackingConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *closeTrackingConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *closeTrackingConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c *closeTrackingConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c *closeTrackingConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *closeTrackingConnector) SetField(context.Context, string, string, string) error {
	return nil
}

func (c *closeTrackingConnector) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

type closeTrackingProvisioningConnector struct {
	*closeTrackingConnector
	provision func(context.Context) error
}

func newCloseTrackingProvisioningConnector() *closeTrackingProvisioningConnector {
	return &closeTrackingProvisioningConnector{closeTrackingConnector: newCloseTrackingConnector()}
}

func (c *closeTrackingProvisioningConnector) Provision(ctx context.Context) error {
	if c.provision == nil {
		return nil
	}
	return c.provision(ctx)
}

func assertConnectorClosed(t *testing.T, projectConnector *closeTrackingConnector) {
	t.Helper()

	select {
	case <-projectConnector.closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connector close")
	}
}

func assertConnectorOpen(t *testing.T, projectConnector *closeTrackingConnector) {
	t.Helper()

	select {
	case <-projectConnector.closed:
		t.Fatal("connector closed, want open")
	default:
	}
}

func receiveEvent(t *testing.T, ch <-chan project.Event) project.Event {
	t.Helper()

	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for project event")
	}

	return project.Event{}
}
