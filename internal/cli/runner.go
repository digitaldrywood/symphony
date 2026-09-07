package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/digitaldrywood/detent/internal/activehours"
	"github.com/digitaldrywood/detent/internal/boardsnapshot"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/claudecode"
	"github.com/digitaldrywood/detent/internal/codex"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/projectcolor"
	runnerpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	commandshell "github.com/digitaldrywood/detent/internal/shell"
	"github.com/digitaldrywood/detent/internal/statuspage"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	detentupdate "github.com/digitaldrywood/detent/internal/update"
	"github.com/digitaldrywood/detent/internal/workspace"
)

const (
	defaultSnapshotInterval      = time.Second
	defaultTelemetryReadTimeout  = 500 * time.Millisecond
	defaultBoardSnapshotInterval = 30 * time.Second
	defaultTokenTrendWindowSize  = 60
	defaultTokenThroughputWindow = time.Minute
)

type lifetimeTotalsSource interface {
	LifetimeTotals(context.Context) (store.LifetimeTotals, error)
}

type autoUpdateStatusSource interface {
	Status() detentupdate.AutoStatus
}

type agentPoolSnapshotSource interface {
	PoolSnapshots() []scheduler.PoolSnapshot
}

type providerStatusEnricher interface {
	Enrich(telemetry.Snapshot, []statuspage.Source) telemetry.Snapshot
}

type telemetrySnapshotPublisher interface {
	Publish(telemetry.Snapshot) error
	Latest() (telemetry.Snapshot, bool)
}

// withRunnerFactory returns a project.Factory that constructs a
// per-project agent Runner from the project's own workflow (so each project's
// codex command and workspace root are honored), injects it into the project's
// dependencies, and then delegates to load.
//
// If load is nil, the default project.Load is used.
func withRunnerFactory(
	deps project.Dependencies,
	sessionStore runnerpkg.SessionStore,
	load func(project.Dependencies) (*project.Project, error),
	githubTokenSource ...func() string,
) project.Factory {
	return func(cfg globalconfig.Project) (*project.Project, error) {
		workflow, err := project.LoadWorkflow(cfg)
		if err != nil {
			return nil, fmt.Errorf("load project workflow %s: %w", cfg.ID, err)
		}
		if cfg.Identity.Configured() {
			workflow.Config.Identity = cfg.Identity
			workflow.Config.Identity.Normalize()
		}
		if len(githubTokenSource) > 0 && githubTokenSource[0] != nil {
			token := strings.TrimSpace(githubTokenSource[0]())
			if token != "" && (workflow.Config.Tracker.Kind == workflowconfig.TrackerGitHub || workflow.Config.Tracker.Kind == workflowconfig.TrackerGitHubLocal) {
				workflow.Config.Tracker.APIKey = token
			}
		}

		run := deps.Runner
		if run == nil {
			var err error
			run, err = buildRunner(workflow, cfg.ID, cfg.Workdir, cfg.EffectiveMemory(), sessionStore, deps.Logger)
			if err != nil {
				return nil, fmt.Errorf("build project runner %s: %w", cfg.ID, err)
			}
		}

		projectDeps := deps
		projectDeps.Runner = run
		if len(githubTokenSource) > 0 && githubTokenSource[0] != nil {
			projectDeps.GitHubToken = githubTokenSource[0]()
		}

		if load != nil {
			return load(projectDeps)
		}
		return project.Load(cfg, projectDeps)
	}
}

// buildRunner constructs the agent Runner for a single project's workflow,
// wiring its workspace backend, codex app-server client, and session store.
func buildRunner(
	workflow workflowconfig.Workflow,
	projectID string,
	projectWorkdir string,
	memory globalconfig.Memory,
	sessionStore runnerpkg.SessionStore,
	logger *slog.Logger,
) (orchestrator.Runner, error) {
	cfg := workflow.Config

	backend, err := buildWorkspaceBackend(cfg, projectWorkdir, logger)
	if err != nil {
		return nil, err
	}

	pricing, err := budget.PricingForConfig(budget.Config{
		PricingPath: cfg.Budget.PricingPath,
	})
	if err != nil {
		return nil, fmt.Errorf("load pricing: %w", err)
	}
	budgetGuardBuilder := func(cfg workflowconfig.Budget) (runnerpkg.BudgetChecker, runnerpkg.DispatchEstimator, error) {
		return buildBudgetDispatchGuards(projectID, cfg, sessionStore, pricing)
	}

	run, err := runnerpkg.NewRunner(runnerpkg.Dependencies{
		ProjectID:           projectID,
		Workflow:            workflow,
		Workspace:           backend,
		AgentBackendFactory: runnerpkg.AgentBackendFactoryFunc(buildAgentBackend),
		Store:               sessionStore,
		Pricing:             pricing,
		BudgetGuardBuilder:  budgetGuardBuilder,
		MaxAgentRSSBytes:    uint64(memory.MaxAgentRSSBytes),
		RSSPollInterval:     time.Duration(memory.PollIntervalMS) * time.Millisecond,
		Logger:              logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create runner: %w", err)
	}
	return run, nil
}

func buildBudgetDispatchGuards(
	projectID string,
	cfg workflowconfig.Budget,
	sessionStore runnerpkg.SessionStore,
	pricing budget.PricingTable,
) (runnerpkg.BudgetChecker, runnerpkg.DispatchEstimator, error) {
	if !cfg.Enabled {
		return nil, nil, nil
	}
	checkerConfig := budget.Config{
		ProjectID:       projectID,
		BillingMode:     cfg.EffectiveBillingMode(),
		Enabled:         cfg.Enabled,
		PerDayMaxUSD:    cfg.PerDayMaxUSD,
		PerIssueMaxUSD:  cfg.PerIssueMaxUSD,
		RefusalCooldown: time.Duration(cfg.RefusalCooldownSeconds) * time.Second,
		PricingPath:     cfg.PricingPath,
		Overrides:       budgetOverrideStore(sessionStore),
	}
	if cfg.EffectiveBillingMode() == workflowconfig.BillingModeSubscription {
		return budget.NewChecker(checkerConfig, nil, pricing), nil, nil
	}

	spendStore, ok := sessionStore.(budget.SpendStore)
	if !ok {
		return nil, nil, budget.ErrMissingSpendStore
	}
	checker := budget.NewChecker(checkerConfig, spendStore, pricing)

	estimateStore, ok := sessionStore.(budget.DispatchEstimateStore)
	if !ok {
		return checker, nil, nil
	}
	return checker, budget.NewDispatchEstimator(estimateStore), nil
}

func budgetOverrideStore(value any) budget.OverrideStore {
	store, ok := value.(budget.OverrideStore)
	if !ok {
		return nil
	}
	return store
}

func buildWorkspaceBackend(cfg workflowconfig.Config, sourceRootFallback string, logger *slog.Logger) (workspace.Backend, error) {
	root := strings.TrimSpace(cfg.Workspace.Root)
	sourceRoot := strings.TrimSpace(cfg.Workspace.SourceRoot)
	if sourceRoot == "" {
		sourceRoot = strings.TrimSpace(sourceRootFallback)
	}
	if sourceRoot == "" {
		sourceRoot = root
	}
	workspaceKind := strings.TrimSpace(cfg.Workspace.Kind)
	if workspaceKind == "" {
		workspaceKind = workspace.KindLocalGit
	}
	outputRoot := strings.TrimSpace(cfg.Workspace.OutputRoot)
	if outputRoot == "" {
		outputRoot = strings.TrimSpace(cfg.Deliverable.OutputRoot)
	}
	backend, err := workspace.NewBackend(workspaceKind, workspace.LocalGitOptions{
		Root:       root,
		SourceRoot: sourceRoot,
		OutputRoot: outputRoot,
		AutoBranch: cfg.Workspace.AutoBranch,
		Hooks: workspace.Hooks{
			Shell:        cfg.Hooks.Shell,
			AfterCreate:  cfg.Hooks.AfterCreate,
			BeforeRun:    cfg.Hooks.BeforeRun,
			AfterRun:     cfg.Hooks.AfterRun,
			BeforeRemove: cfg.Hooks.BeforeRemove,
			Timeout:      durationFromMillis(cfg.Hooks.TimeoutMS),
		},
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create workspace backend: %w", err)
	}
	return backend, nil
}

func buildAgentBackend(backend workflowconfig.AgentBackend) (runnerpkg.AgentBackend, error) {
	switch backend.Kind {
	case workflowconfig.AgentBackendCodex:
		return buildCodexAgentBackend(backend.Command, backend.CodexOptions())
	case workflowconfig.AgentBackendClaudeCode:
		return buildClaudeAgentBackend(backend.Command, backend.ClaudeCodeOptions())
	default:
		return nil, fmt.Errorf("unsupported agent backend kind %q; supported kinds: %s, %s",
			backend.Kind,
			workflowconfig.AgentBackendCodex,
			workflowconfig.AgentBackendClaudeCode,
		)
	}
}

func buildClaudeAgentBackend(command string, cfg workflowconfig.ClaudeCodeOptions) (runnerpkg.AgentBackend, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, errors.New("claude command is required")
	}

	backend, err := claudecode.NewAgentBackend(claudecode.Options{
		CommandFactoryWithArgs: func(ctx context.Context, args []string) *exec.Cmd {
			return buildClaudeCommandFromConfig(ctx, command, cfg.Shell, args)
		},
		PermissionMode:         cfg.PermissionMode,
		Effort:                 cfg.Effort,
		AllowedTools:           cfg.AllowedTools,
		DisallowedTools:        cfg.DisallowedTools,
		IncludePartialMessages: cfg.IncludePartialMessages,
		ExtraArgs:              cfg.ExtraArgs,
		TurnTimeout:            durationFromMillis(cfg.TurnTimeoutMS),
		StallTimeout:           durationFromMillis(cfg.StallTimeoutMS),
	})
	if err != nil {
		return nil, fmt.Errorf("create claude backend: %w", err)
	}
	return backend, nil
}

func buildCodexAgentBackend(command string, cfg workflowconfig.CodexOptions) (runnerpkg.AgentBackend, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, errors.New("codex command is required")
	}
	serviceCommand, err := prepareCodexCommandForRuntime(command)
	if err != nil {
		return nil, fmt.Errorf("prepare Codex service profile: %w", err)
	}
	command = serviceCommand.Command
	if len(serviceCommand.OmittedSkills) > 0 {
		slog.Warn(
			"launchd Codex profile omitted skills in macOS privacy-protected locations",
			"skills", strings.Join(serviceCommand.OmittedSkills, ","),
		)
	}

	factory, err := codex.NewLocalTransportFactory(func(ctx context.Context) *exec.Cmd {
		cmd := buildCodexCommandFromConfig(ctx, command, cfg.Shell)
		procgroup.SetEnvironment(cmd, procgroup.Environment{Variables: serviceCommand.Environment})
		return cmd
	})
	if err != nil {
		return nil, fmt.Errorf("create codex transport factory: %w", err)
	}

	opts := []codex.AppServerOption{}
	if timeout := durationFromMillis(cfg.ReadTimeoutMS); timeout > 0 {
		opts = append(opts, codex.WithReadTimeout(timeout))
	}
	if timeout := durationFromMillis(cfg.TurnTimeoutMS); timeout > 0 {
		opts = append(opts, codex.WithTurnTimeout(timeout))
	}

	client, err := codex.NewAppServer(factory, opts...)
	if err != nil {
		return nil, fmt.Errorf("create codex app-server: %w", err)
	}
	backend, err := codex.NewAgentBackend(client, codex.OptionsFromConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("create codex backend: %w", err)
	}
	return backend, nil
}

func buildCodexCommand(ctx context.Context, cfg workflowconfig.Config) *exec.Cmd {
	return buildCodexCommandFromConfig(ctx, cfg.Codex.Command, cfg.Codex.Shell)
}

func buildCodexCommandFromConfig(ctx context.Context, command string, shell string) *exec.Cmd {
	return commandshell.Command(ctx, strings.TrimSpace(command), shell)
}

func buildClaudeCommandFromConfig(ctx context.Context, command string, shell string, args []string) *exec.Cmd {
	return commandshell.CommandWithArgs(ctx, strings.TrimSpace(command), shell, args)
}

// publishSnapshots ticks at interval, building a merged telemetry snapshot
// across every running project's orchestrator and publishing it to hub until
// ctx is cancelled.
func publishSnapshots(
	ctx context.Context,
	registry *project.Registry,
	poolSource agentPoolSnapshotSource,
	snapshotPublisher telemetrySnapshotPublisher,
	seq *atomic.Uint64,
	shutdown *ShutdownController,
	lifetimeSource lifetimeTotalsSource,
	dashboardURL string,
	providerStatus providerStatusEnricher,
	interval time.Duration,
	now func() time.Time,
	updateSources ...autoUpdateStatusSource,
) {
	if registry == nil || snapshotPublisher == nil {
		return
	}
	if interval <= 0 {
		interval = defaultSnapshotInterval
	}
	if now == nil {
		now = time.Now
	}
	if seq == nil {
		seq = &atomic.Uint64{}
	}

	trend := newTokenTrendRecorder(defaultTokenTrendWindowSize)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := publishSnapshotOnce(ctx, registry, poolSource, snapshotPublisher, seq, shutdown, now(), trend, lifetimeSource, dashboardURL, providerStatus, updateSources...); err != nil {
			slog.Default().Warn("publish telemetry snapshot failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func republishSnapshotsOnProjectEvents(
	ctx context.Context,
	events *hub.Hub[project.Event],
	snapshotHub *hub.Hub[telemetry.Snapshot],
	logger *slog.Logger,
) {
	if events == nil || snapshotHub == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	sub, err := events.Subscribe(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn("subscribe project events for snapshot republish failed", "error", err)
		}
		return
	}
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.C():
			if !ok {
				return
			}
			switch event.Kind {
			case project.EventPaused:
				logger.Info("project paused", "project_id", event.ProjectID)
			case project.EventUnpaused:
				logger.Info("project unpaused", "project_id", event.ProjectID)
			}
			republishLatestSnapshot(snapshotHub, logger)
		}
	}
}

func republishLatestSnapshot(snapshotHub *hub.Hub[telemetry.Snapshot], logger *slog.Logger) {
	if snapshotHub == nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := snapshotHub.Republish(); err != nil {
		logger.Warn("republish telemetry snapshot failed", "error", err)
	}
}

func persistBoardSnapshots(
	ctx context.Context,
	snapshotHub *hub.Hub[telemetry.Snapshot],
	cache boardsnapshot.Store,
	interval time.Duration,
	logger *slog.Logger,
) {
	if snapshotHub == nil || cache == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		interval = defaultBoardSnapshotInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	subscription, err := snapshotHub.Subscribe(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn("subscribe board snapshot cache failed", "error", err)
		}
		return
	}
	defer subscription.Close()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var pending telemetry.Snapshot
	dirty := false
	for {
		select {
		case <-ctx.Done():
			return
		case snapshot, ok := <-subscription.C():
			if !ok {
				return
			}
			if boardsnapshot.Eligible(snapshot) {
				pending = snapshot
				dirty = true
			}
		case <-ticker.C:
			if !dirty {
				continue
			}
			if err := cache.Save(ctx, pending); err != nil {
				logger.Warn("persist board snapshot failed", "error", err)
				continue
			}
			dirty = false
		}
	}
}

func publishStartupSnapshotOnce(
	ctx context.Context,
	cfg globalconfig.Config,
	snapshotPublisher telemetrySnapshotPublisher,
	lifetimeSource lifetimeTotalsSource,
	dashboardURL string,
	now time.Time,
	updateSources ...autoUpdateStatusSource,
) error {
	if snapshotPublisher == nil {
		return nil
	}
	snapshot := startupSnapshot(ctx, cfg, lifetimeSource, dashboardURL, now, updateSources...)
	if err := snapshotPublisher.Publish(snapshot); err != nil {
		return fmt.Errorf("publish startup snapshot: %w", err)
	}
	return nil
}

func startupSnapshot(
	ctx context.Context,
	cfg globalconfig.Config,
	lifetimeSource lifetimeTotalsSource,
	dashboardURL string,
	now time.Time,
	updateSources ...autoUpdateStatusSource,
) telemetry.Snapshot {
	nextRefreshAt := now
	refresh := telemetry.Refresh{Status: telemetry.RefreshStatusInitializing, NextRefreshAt: &nextRefreshAt}
	unknown := telemetry.SnapshotSection{Source: telemetry.SnapshotSourceUnknown}
	snapshot := telemetry.Snapshot{
		GeneratedAt:    now,
		Tracker:        unknown,
		Runtime:        unknown,
		Instance:       startupSnapshotInstance(cfg),
		Projects:       startupProjectSnapshots(cfg, refresh, now),
		DashboardURL:   cleanDashboardURL(dashboardURL),
		Shutdown:       telemetry.Shutdown{Status: "running"},
		Refresh:        refresh,
		LifetimeTotals: lifetimeTotals(ctx, lifetimeSource),
		Update:         telemetryUpdateStatus(updateSources),
	}
	switch len(snapshot.Projects) {
	case 0:
	case 1:
		snapshot.Project = snapshot.Projects[0].Project
	default:
		snapshot.Project = telemetry.Project{DisplayName: "multiple projects"}
	}
	return snapshot
}

func startupSnapshotInstance(cfg globalconfig.Config) telemetry.Instance {
	identity := cfg.Global.Identity
	identity.Normalize()
	return telemetry.Instance{
		Name:        identity.Name,
		GitHubLogin: identity.GitHubLogin,
	}
}

func startupProjectSnapshots(cfg globalconfig.Config, refresh telemetry.Refresh, now time.Time) []telemetry.ProjectSnapshot {
	out := make([]telemetry.ProjectSnapshot, 0, len(cfg.Projects))
	unknown := telemetry.SnapshotSection{Source: telemetry.SnapshotSourceUnknown}
	for _, projectConfig := range cfg.Projects {
		id := strings.TrimSpace(projectConfig.ID)
		if id == "" {
			continue
		}
		projectConfig.GlobalActiveHours = cfg.Global.ActiveHours
		out = append(out, telemetry.ProjectSnapshot{
			Project: projectSnapshotMetadataFromConfig(projectConfig, now),
			Tracker: unknown,
			Runtime: unknown,
			Refresh: refresh,
		})
	}
	return out
}

func publishSnapshotOnce(
	ctx context.Context,
	registry *project.Registry,
	poolSource agentPoolSnapshotSource,
	snapshotPublisher telemetrySnapshotPublisher,
	seq *atomic.Uint64,
	shutdown *ShutdownController,
	now time.Time,
	trend *tokenTrendRecorder,
	lifetimeSource lifetimeTotalsSource,
	dashboardURL string,
	providerStatus providerStatusEnricher,
	updateSources ...autoUpdateStatusSource,
) error {
	merged := telemetry.Snapshot{GeneratedAt: now}
	trackedProjects := registry.List()
	projectHealth := registry.Health()
	if len(projectHealth) == 0 {
		return nil
	}
	tracked := make(map[project.ID]struct{}, len(trackedProjects))
	for _, trackedProject := range trackedProjects {
		tracked[trackedProject.ID()] = struct{}{}
		projectMetadata := projectSnapshotMetadata(trackedProject, now)
		if !trackedProject.Running() {
			if trackedProject.Paused() {
				merged = mergeSnapshot(merged, telemetry.Snapshot{
					Project:      projectMetadata,
					Runtime:      liveSnapshotSection(now),
					DashboardURL: cleanDashboardURL(dashboardURL),
					Shutdown:     telemetry.Shutdown{Status: "running"},
				})
				continue
			}
			if runtimeErr := trackedProject.RuntimeError(); runtimeErr.Message != "" {
				merged = mergeSnapshot(merged, telemetry.Snapshot{
					Project:      projectMetadata,
					Tracker:      unknownSnapshotSection(),
					Runtime:      unknownSnapshotSection(),
					DashboardURL: cleanDashboardURL(dashboardURL),
					Shutdown:     telemetry.Shutdown{Status: "running"},
					Refresh:      runtimeErrorRefresh(runtimeErr),
				})
				continue
			}
			nextRefreshAt := now
			refresh := telemetry.Refresh{Status: telemetry.RefreshStatusInitializing, NextRefreshAt: &nextRefreshAt}
			merged = mergeSnapshot(merged, telemetry.Snapshot{
				Project:      projectMetadata,
				Tracker:      unknownSnapshotSection(),
				Runtime:      unknownSnapshotSection(),
				DashboardURL: cleanDashboardURL(dashboardURL),
				Shutdown:     telemetry.Shutdown{Status: "running"},
				Refresh:      refresh,
			})
			continue
		}
		orch := trackedProject.Orchestrator()
		if orch == nil {
			continue
		}
		state, err := readTelemetrySource(ctx, "project_state", string(trackedProject.ID()), orch.State)
		if err != nil {
			lastErrorAt := now
			merged = mergeSnapshot(merged, telemetry.Snapshot{
				Project:      projectMetadata,
				Tracker:      unknownSnapshotSection(),
				Runtime:      unknownSnapshotSection(),
				DashboardURL: cleanDashboardURL(dashboardURL),
				Shutdown:     telemetry.Shutdown{Status: "running"},
				Refresh: telemetry.Refresh{
					Status:      telemetry.RefreshStatusDegraded,
					LastError:   err.Error(),
					LastErrorAt: &lastErrorAt,
				},
			})
			continue
		}
		snapshot := state.Snapshot(now)
		snapshot.Project = projectMetadata
		snapshot.Tracker = unknownSnapshotSection()
		if !state.LastRefreshAt.IsZero() {
			snapshot.Tracker = liveSnapshotSection(state.LastRefreshAt)
		}
		snapshot.Runtime = liveSnapshotSection(now)
		snapshot.DashboardURL = cleanDashboardURL(dashboardURL)
		merged = mergeSnapshot(merged, snapshot)
	}
	for _, health := range projectHealth {
		id := project.ID(health.Project.ID)
		if _, ok := tracked[id]; ok {
			continue
		}
		merged = mergeSnapshot(merged, telemetry.Snapshot{
			Project:      projectSnapshotMetadataFromConfig(health.Project, now),
			Tracker:      unknownSnapshotSection(),
			Runtime:      unknownSnapshotSection(),
			DashboardURL: cleanDashboardURL(dashboardURL),
			Shutdown:     telemetry.Shutdown{Status: "running"},
			Refresh: runtimeErrorRefresh(project.RuntimeError{
				Message:     health.LastError,
				At:          health.LastErrorAt,
				NextRetryAt: health.NextRetryAt,
				Terminal:    health.RetryStopped,
			}),
		})
	}
	merged = dedupeSnapshotIssues(merged)
	if providerStatus != nil {
		merged = providerStatus.Enrich(merged, providerStatusSources(registry))
	}
	merged.AgentPools = telemetryAgentPools(poolSource)
	if trend != nil {
		merged = trend.apply(merged)
	}
	merged.LifetimeTotals = lifetimeTotals(ctx, lifetimeSource)
	merged.Update = telemetryUpdateStatus(updateSources)
	if status, ok := shutdown.currentShutdownStatus(); ok {
		merged.Shutdown = status
	}
	merged.Seq = seq.Add(1)
	summarizeSnapshotSections(&merged)
	if current, ok := snapshotPublisher.Latest(); ok {
		merged = composeLastKnownTrackerState(current, merged, now)
	}
	if err := snapshotPublisher.Publish(merged); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	return nil
}

func providerStatusSources(registry *project.Registry) []statuspage.Source {
	if registry == nil {
		return nil
	}
	projects := registry.List()
	sources := make([]statuspage.Source, 0, len(projects))
	for _, trackedProject := range projects {
		if trackedProject == nil {
			continue
		}
		tracker := trackedProject.Workflow().Config.Tracker
		if strings.TrimSpace(tracker.StatusPageURL) == "" {
			continue
		}
		sources = append(sources, statuspage.SourceForTracker(string(trackedProject.ID()), tracker.Kind, tracker.StatusPageURL))
	}
	return sources
}

func telemetryUpdateStatus(sources []autoUpdateStatusSource) telemetry.Update {
	if len(sources) == 0 || sources[0] == nil {
		return telemetry.Update{}
	}
	status := sources[0].Status()
	return telemetry.Update{
		Enabled:            status.Enabled,
		AutoApplyEnabled:   status.AutoApplyEnabled,
		CheckIntervalHours: int(status.CheckInterval / time.Hour),
		State:              status.State,
		LastCheckAt:        status.LastCheckAt,
		LastAppliedVersion: status.LastAppliedVersion,
		NextCheckAt:        status.NextCheckAt,
		AvailableVersion:   status.AvailableVersion,
		PendingSince:       status.PendingSince,
		MaxDeferralHours:   int(status.MaxDeferral / time.Hour),
		Critical:           status.Critical,
		LastError:          status.LastError,
	}
}

func projectSnapshotMetadata(trackedProject *project.Project, now time.Time) telemetry.Project {
	if trackedProject == nil {
		return telemetry.Project{}
	}

	cfg := trackedProject.Config()
	workflow := trackedProject.Workflow()
	metadata := projectSnapshotMetadataFromConfig(cfg, now)
	metadata.URL = projectURLFromWorkflow(workflow.Config)
	metadata.ActiveHours = telemetryActiveHours(project.EffectiveActiveHours(cfg, workflow.Config.ActiveHours), cfg.ActiveHoursOverrideUntil, now)
	return metadata
}

func projectSnapshotMetadataFromConfig(cfg globalconfig.Project, now time.Time) telemetry.Project {
	id := strings.TrimSpace(cfg.ID)
	pool := strings.TrimSpace(cfg.Pool)
	if pool == "" {
		pool = scheduler.DefaultPoolName
	}
	return telemetry.Project{
		ID:          id,
		DisplayName: id,
		Color:       projectcolor.ColorFor(id, cfg.Color),
		Pool:        pool,
		ActiveHours: telemetryActiveHours(project.EffectiveActiveHours(cfg, activehours.Config{}), cfg.ActiveHoursOverrideUntil, now),
	}
}

func telemetryActiveHours(config activehours.Config, overrideValue string, now time.Time) telemetry.ActiveHours {
	if !config.Configured() {
		return telemetry.ActiveHours{}
	}
	overrideUntil := activehours.ParsePersistedOverride(overrideValue)
	status, err := activehours.Evaluate(config, now, overrideUntil)
	if err != nil {
		return telemetry.ActiveHours{Configured: true, Timezone: config.Timezone}
	}
	return telemetry.ActiveHoursFromStatus(status)
}

func telemetryAgentPools(source agentPoolSnapshotSource) []telemetry.AgentPool {
	if source == nil {
		return nil
	}
	snapshots := source.PoolSnapshots()
	pools := make([]telemetry.AgentPool, 0, len(snapshots))
	for _, snapshot := range snapshots {
		name := strings.TrimSpace(snapshot.Name)
		if name == "" || snapshot.Capacity <= 0 {
			continue
		}
		pools = append(pools, telemetry.AgentPool{
			Name:       name,
			Used:       snapshot.Used,
			Capacity:   snapshot.Capacity,
			Guaranteed: snapshot.Guaranteed,
			BurstTo:    snapshot.BurstTo,
			Borrowed:   snapshot.Borrowed,
			Available:  snapshot.Available,
			Draining:   snapshot.Draining,
			Reclaiming: snapshot.Reclaiming,
			Generation: snapshot.Generation,
		})
	}
	if len(pools) == 0 {
		return nil
	}
	return pools
}

func runtimeErrorRefresh(runtimeErr project.RuntimeError) telemetry.Refresh {
	lastErrorAt := runtimeErr.At
	refresh := telemetry.Refresh{
		Status:      telemetry.RefreshStatusDegraded,
		LastError:   runtimeErr.Message,
		LastErrorAt: &lastErrorAt,
	}
	if !runtimeErr.NextRetryAt.IsZero() {
		nextRetryAt := runtimeErr.NextRetryAt
		refresh.NextRefreshAt = &nextRetryAt
	}
	return refresh
}

func projectURLFromWorkflow(cfg workflowconfig.Config) string {
	slug := strings.TrimSpace(cfg.Tracker.ProjectSlug)
	if strings.HasPrefix(slug, "http://") || strings.HasPrefix(slug, "https://") {
		return slug
	}
	return ""
}

func cleanDashboardURL(value string) string {
	return strings.TrimSpace(value)
}

type tokenTrendRecorder struct {
	limit  int
	window time.Duration
	points []telemetry.TokenTrendPoint
}

func newTokenTrendRecorder(limit int) *tokenTrendRecorder {
	if limit <= 0 {
		limit = defaultTokenTrendWindowSize
	}
	return &tokenTrendRecorder{limit: limit, window: defaultTokenThroughputWindow}
}

func (r *tokenTrendRecorder) apply(snapshot telemetry.Snapshot) telemetry.Snapshot {
	if snapshot.Tokens.Input > 0 || snapshot.Tokens.Output > 0 || snapshot.Tokens.Total > 0 {
		total := snapshot.Tokens.Total
		if total <= 0 {
			total = snapshot.Tokens.Input + snapshot.Tokens.Output
		}
		point := telemetry.TokenTrendPoint{
			At:     snapshot.GeneratedAt,
			Input:  snapshot.Tokens.Input,
			Output: snapshot.Tokens.Output,
			Total:  total,
		}
		if r.shouldReset(point) {
			r.points = nil
		}
		r.points = append(r.points, point)
		if len(r.points) > r.limit {
			r.points = append([]telemetry.TokenTrendPoint(nil), r.points[len(r.points)-r.limit:]...)
		}
	} else {
		r.points = nil
	}
	snapshot.TokenTrend = append([]telemetry.TokenTrendPoint(nil), r.points...)
	snapshot.Throughput = r.throughput()
	return snapshot
}

func (r *tokenTrendRecorder) shouldReset(point telemetry.TokenTrendPoint) bool {
	if len(r.points) == 0 {
		return false
	}
	latest := r.points[len(r.points)-1]
	return point.Total < latest.Total || !point.At.After(latest.At)
}

func (r *tokenTrendRecorder) throughput() telemetry.TokenThroughput {
	window := r.window
	if window <= 0 {
		window = defaultTokenThroughputWindow
	}

	throughput := telemetry.TokenThroughput{WindowSeconds: int64(window / time.Second)}
	if len(r.points) < 2 {
		return throughput
	}

	latest := r.points[len(r.points)-1]
	windowStart := latest.At.Add(-window)
	base := latest
	for _, point := range r.points[:len(r.points)-1] {
		if point.At.Before(windowStart) {
			continue
		}
		base = point
		break
	}

	elapsed := latest.At.Sub(base.At).Seconds()
	if elapsed <= 0 {
		return throughput
	}

	tokens := latest.Total - base.Total
	if tokens <= 0 {
		return throughput
	}

	throughput.Tokens = tokens
	throughput.TokensPerSecond = float64(tokens) / elapsed
	return throughput
}

func lifetimeTotals(ctx context.Context, source lifetimeTotalsSource) telemetry.LifetimeTotals {
	if source == nil {
		return telemetry.LifetimeTotals{DegradedReason: "runtime store unavailable"}
	}
	totals, err := readTelemetrySource(ctx, "lifetime_totals", "", source.LifetimeTotals)
	if err != nil {
		return telemetry.LifetimeTotals{DegradedReason: "read runtime store lifetime totals: " + err.Error()}
	}
	return telemetry.LifetimeTotals{
		Available:             true,
		InputTokens:           totals.InputTokens,
		CachedInputTokens:     totals.CachedInputTokens,
		OutputTokens:          totals.OutputTokens,
		ReasoningOutputTokens: totals.ReasoningOutputTokens,
		TotalTokens:           totals.TotalTokens,
		RuntimeSeconds:        totals.RuntimeSeconds,
		Sessions:              totals.Sessions,
		Runs:                  totals.Runs,
		OrphanResumed:         totals.OrphanResumed,
		OrphanFresh:           totals.OrphanFresh,
		ResumedInputTokens:    totals.ResumedInputTokens,
		ResumedCachedTokens:   totals.ResumedCachedTokens,
	}
}

func readTelemetrySource[T any](ctx context.Context, source, projectID string, read func(context.Context) (T, error)) (T, error) {
	readCtx, cancel := context.WithTimeout(ctx, defaultTelemetryReadTimeout)
	defer cancel()
	started := time.Now()
	value, err := read(readCtx)
	if err != nil {
		err = fmt.Errorf("telemetry source %s: %w", source, err)
		if ctx.Err() == nil {
			slog.Default().Warn("telemetry source read failed",
				"source", source,
				"project_id", projectID,
				"elapsed", time.Since(started),
				"timeout", defaultTelemetryReadTimeout,
				"error", err,
			)
		}
	}
	return value, err
}

func mergeSnapshot(current, next telemetry.Snapshot) telemetry.Snapshot {
	next = stampSnapshotProjectID(next)
	current.Project = mergeProject(current.Project, next.Project)
	current.Instance = mergeInstance(current.Instance, next.Instance)
	if project := projectSnapshot(next); project.Project != (telemetry.Project{}) {
		current.Projects = append(current.Projects, project)
	}
	if strings.TrimSpace(current.DashboardURL) == "" {
		current.DashboardURL = next.DashboardURL
	}
	current.Refresh = mergeRefresh(current.Refresh, next.Refresh)
	current.Shutdown = mergeShutdown(current.Shutdown, next.Shutdown)

	current.Running = append(current.Running, next.Running...)
	current.WorkAttempts = append(current.WorkAttempts, next.WorkAttempts...)
	current.SchedulerDecisions = append(current.SchedulerDecisions, next.SchedulerDecisions...)
	current.Dispatch = mergeFleetDispatchStatus(current.Dispatch, next.Dispatch, next.GeneratedAt)
	current.DispatchStalls = append(current.DispatchStalls, next.DispatchStalls...)
	current.CleanupFaults = append(current.CleanupFaults, next.CleanupFaults...)
	if !next.Release.IsZero() {
		current.Releases = append(current.Releases, next.Release)
		if current.Release.IsZero() {
			current.Release = next.Release
		}
	}
	current.Queue = append(current.Queue, next.Queue...)
	current.Blocked = append(current.Blocked, next.Blocked...)
	current.Completed = append(current.Completed, next.Completed...)
	current.BoardIssues = append(current.BoardIssues, next.BoardIssues...)
	current.Pipeline = append(current.Pipeline, next.Pipeline...)
	current.TrackerDrift.UntrackedOpen = append(current.TrackerDrift.UntrackedOpen, next.TrackerDrift.UntrackedOpen...)
	current.TrackerDrift.OpenTerminal = append(current.TrackerDrift.OpenTerminal, next.TrackerDrift.OpenTerminal...)
	current.TrackerDrift.ClosedActive = append(current.TrackerDrift.ClosedActive, next.TrackerDrift.ClosedActive...)
	current.Budget.Refusals = append(current.Budget.Refusals, next.Budget.Refusals...)
	current.TrackerUnavailable = append(current.TrackerUnavailable, next.TrackerUnavailable...)
	current.ForgeUnavailable = append(current.ForgeUnavailable, next.ForgeUnavailable...)
	current.CIUnavailable = append(current.CIUnavailable, next.CIUnavailable...)
	current.BackendOutages = append(current.BackendOutages, next.BackendOutages...)
	current.FailureBreakers = append(current.FailureBreakers, next.FailureBreakers...)
	current.DispatchLoops = append(current.DispatchLoops, next.DispatchLoops...)
	current.DispatchRecoveries = append(current.DispatchRecoveries, next.DispatchRecoveries...)
	current.StalenessWarnings = append(current.StalenessWarnings, next.StalenessWarnings...)
	current.StrandedActiveIssues = append(current.StrandedActiveIssues, next.StrandedActiveIssues...)
	current.AgentPools = append(current.AgentPools, next.AgentPools...)
	current.OverloadRetriesLastHour += next.OverloadRetriesLastHour

	current.Counts.Running += next.Counts.Running
	current.Counts.Queue += next.Counts.Queue
	current.Counts.Blocked += next.Counts.Blocked
	current.Counts.Completed += next.Counts.Completed

	current.Tokens.Input += next.Tokens.Input
	current.Tokens.Output += next.Tokens.Output
	current.Tokens.Total += next.Tokens.Total
	current.Tokens.RuntimeSeconds += next.Tokens.RuntimeSeconds

	current.RateLimits = mergeFleetRateLimits(current.RateLimits, next.RateLimits)
	if current.MemoryPressure.ObservedAt.IsZero() || next.MemoryPressure.ObservedAt.After(current.MemoryPressure.ObservedAt) {
		current.MemoryPressure = next.MemoryPressure
	}
	if current.IOPressure.ObservedAt.IsZero() || next.IOPressure.ObservedAt.After(current.IOPressure.ObservedAt) {
		current.IOPressure = next.IOPressure
	}
	if current.CPUPressure.ObservedAt.IsZero() || next.CPUPressure.ObservedAt.After(current.CPUPressure.ObservedAt) {
		current.CPUPressure = next.CPUPressure
	}
	return current
}

func mergeFleetRateLimits(current *telemetry.RateLimits, incoming *telemetry.RateLimits) *telemetry.RateLimits {
	if incoming == nil {
		return current
	}
	if current == nil {
		cloned := *incoming
		cloned.GitHubREST = cloneFleetRateLimitBucket(incoming.GitHubREST)
		cloned.GitHubRESTBudgets = mergeFleetRESTBudgets(nil, incoming.GitHubRESTBudgets)
		cloned.RESTUsage = mergeFleetRESTUsage(nil, incoming.RESTUsage)
		return &cloned
	}

	merged := *current
	merged.GitHubRESTBudgets = mergeFleetRESTBudgets(current.GitHubRESTBudgets, incoming.GitHubRESTBudgets)
	merged.GitHubREST = mergeFleetRESTBucket(current.GitHubREST, incoming.GitHubREST)
	if bucket := fleetRESTBucketFromBudgets(merged.GitHubRESTBudgets); bucket != nil {
		bucket.Cost = rateLimitBucketCost(current.GitHubREST) + rateLimitBucketCost(incoming.GitHubREST)
		merged.GitHubREST = bucket
	}
	merged.RESTUsage = mergeFleetRESTUsage(current.RESTUsage, incoming.RESTUsage)
	if merged.GitHubGraphQL == nil {
		merged.GitHubGraphQL = incoming.GitHubGraphQL
	}
	if merged.GraphQLCost == nil {
		merged.GraphQLCost = incoming.GraphQLCost
	}
	return &merged
}

func rateLimitBucketCost(bucket *telemetry.RateLimitBucket) int64 {
	if bucket == nil {
		return 0
	}
	return bucket.Cost
}

func mergeFleetRESTBucket(current *telemetry.RateLimitBucket, incoming *telemetry.RateLimitBucket) *telemetry.RateLimitBucket {
	if current == nil {
		return cloneFleetRateLimitBucket(incoming)
	}
	if incoming == nil {
		return cloneFleetRateLimitBucket(current)
	}
	chosen := current
	if rateLimitRemainingRatio(incoming) < rateLimitRemainingRatio(current) {
		chosen = incoming
	} else if rateLimitRemainingRatio(incoming) == rateLimitRemainingRatio(current) && rateLimitObservedAfter(incoming, current) {
		chosen = incoming
	}
	merged := cloneFleetRateLimitBucket(chosen)
	merged.Cost = current.Cost + incoming.Cost
	return merged
}

func rateLimitRemainingRatio(bucket *telemetry.RateLimitBucket) float64 {
	if bucket == nil || bucket.Limit <= 0 {
		return 2
	}
	return float64(bucket.Remaining) / float64(bucket.Limit)
}

func rateLimitObservedAfter(left *telemetry.RateLimitBucket, right *telemetry.RateLimitBucket) bool {
	return left != nil && left.ObservedAt != nil && (right == nil || right.ObservedAt == nil || left.ObservedAt.After(*right.ObservedAt))
}

func cloneFleetRateLimitBucket(bucket *telemetry.RateLimitBucket) *telemetry.RateLimitBucket {
	if bucket == nil {
		return nil
	}
	cloned := *bucket
	if bucket.ResetAt != nil {
		resetAt := *bucket.ResetAt
		cloned.ResetAt = &resetAt
	}
	if bucket.ObservedAt != nil {
		observedAt := *bucket.ObservedAt
		cloned.ObservedAt = &observedAt
	}
	return &cloned
}

func mergeFleetRESTBudgets(current []telemetry.RESTBudget, incoming []telemetry.RESTBudget) []telemetry.RESTBudget {
	byKey := make(map[string]telemetry.RESTBudget, len(current)+len(incoming))
	for _, budget := range append(append([]telemetry.RESTBudget(nil), current...), incoming...) {
		key := restBudgetConsumer(budget.Consumer) + "\x00" + strings.TrimSpace(budget.CredentialIdentity) + "\x00" + strings.TrimSpace(budget.EndpointFamily) + "\x00" + strings.TrimSpace(budget.Resource)
		existing, ok := byKey[key]
		if !ok || restBudgetObservedAfter(budget, existing) {
			byKey[key] = cloneFleetRESTBudget(budget)
		}
	}
	if len(byKey) == 0 {
		return nil
	}
	budgets := make([]telemetry.RESTBudget, 0, len(byKey))
	for _, budget := range byKey {
		budgets = append(budgets, budget)
	}
	sort.Slice(budgets, func(i, j int) bool {
		if restBudgetConsumer(budgets[i].Consumer) != restBudgetConsumer(budgets[j].Consumer) {
			return restBudgetConsumer(budgets[i].Consumer) < restBudgetConsumer(budgets[j].Consumer)
		}
		if budgets[i].CredentialIdentity != budgets[j].CredentialIdentity {
			return budgets[i].CredentialIdentity < budgets[j].CredentialIdentity
		}
		if budgets[i].EndpointFamily != budgets[j].EndpointFamily {
			return budgets[i].EndpointFamily < budgets[j].EndpointFamily
		}
		return budgets[i].Resource < budgets[j].Resource
	})
	return budgets
}

func restBudgetObservedAfter(left telemetry.RESTBudget, right telemetry.RESTBudget) bool {
	return left.ObservedAt != nil && (right.ObservedAt == nil || left.ObservedAt.After(*right.ObservedAt))
}

func cloneFleetRESTBudget(budget telemetry.RESTBudget) telemetry.RESTBudget {
	if budget.ResetAt != nil {
		resetAt := *budget.ResetAt
		budget.ResetAt = &resetAt
	}
	if budget.ObservedAt != nil {
		observedAt := *budget.ObservedAt
		budget.ObservedAt = &observedAt
	}
	return budget
}

func fleetRESTBucketFromBudgets(budgets []telemetry.RESTBudget) *telemetry.RateLimitBucket {
	currentByCredentialResource := make(map[string]telemetry.RESTBudget, len(budgets))
	for _, budget := range budgets {
		if restBudgetConsumer(budget.Consumer) != telemetry.RESTConsumerOrchestrator {
			continue
		}
		key := strings.TrimSpace(budget.CredentialIdentity) + "\x00" + strings.TrimSpace(budget.Resource)
		existing, ok := currentByCredentialResource[key]
		if !ok || restBudgetObservedAfter(budget, existing) || (restBudgetObservedTogether(budget, existing) && restBudgetRemainingRatio(budget) < restBudgetRemainingRatio(existing)) {
			currentByCredentialResource[key] = budget
		}
	}
	candidates := make([]telemetry.RESTBudget, 0, len(currentByCredentialResource))
	hasCore := false
	for _, budget := range currentByCredentialResource {
		if strings.EqualFold(strings.TrimSpace(budget.Resource), "core") {
			hasCore = true
		}
		candidates = append(candidates, budget)
	}
	var selected *telemetry.RESTBudget
	for index := range candidates {
		candidate := &candidates[index]
		if hasCore && !strings.EqualFold(strings.TrimSpace(candidate.Resource), "core") {
			continue
		}
		if selected == nil || restBudgetRemainingRatio(*candidate) < restBudgetRemainingRatio(*selected) {
			selected = candidate
		}
	}
	if selected == nil {
		return nil
	}
	return &telemetry.RateLimitBucket{
		Remaining:  selected.Remaining,
		Used:       selected.Used,
		Limit:      selected.Limit,
		ResetAt:    cloneTimePointer(selected.ResetAt),
		ObservedAt: cloneTimePointer(selected.ObservedAt),
	}
}

func restBudgetConsumer(consumer string) string {
	consumer = strings.TrimSpace(consumer)
	if consumer == "" {
		return telemetry.RESTConsumerOrchestrator
	}
	return consumer
}

func restBudgetObservedTogether(left telemetry.RESTBudget, right telemetry.RESTBudget) bool {
	if left.ObservedAt == nil || right.ObservedAt == nil {
		return left.ObservedAt == nil && right.ObservedAt == nil
	}
	return left.ObservedAt.Equal(*right.ObservedAt)
}

func restBudgetRemainingRatio(budget telemetry.RESTBudget) float64 {
	if budget.Limit <= 0 {
		return 2
	}
	return float64(budget.Remaining) / float64(budget.Limit)
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func mergeFleetRESTUsage(current *telemetry.RESTUsage, incoming *telemetry.RESTUsage) *telemetry.RESTUsage {
	if current == nil && incoming == nil {
		return nil
	}
	merged := &telemetry.RESTUsage{}
	contributors := make(map[string]telemetry.RESTUsageContributor)
	divergences := make(map[string]telemetry.RESTUsageDivergence)
	for _, usage := range []*telemetry.RESTUsage{current, incoming} {
		if usage == nil {
			continue
		}
		merged.TotalRequests += usage.TotalRequests
		merged.ConditionalRequests += usage.ConditionalRequests
		merged.NotModifiedRequests += usage.NotModifiedRequests
		merged.BillableRequests += usage.BillableRequests
		merged.RateLimited = merged.RateLimited || usage.RateLimited
		if usage.BackoffUntil != nil && (merged.BackoffUntil == nil || usage.BackoffUntil.After(*merged.BackoffUntil)) {
			backoffUntil := *usage.BackoffUntil
			merged.BackoffUntil = &backoffUntil
		}
		for _, contributor := range usage.Contributors {
			key := restBudgetConsumer(contributor.Consumer) + "\x00" + contributor.CredentialIdentity + "\x00" + contributor.EndpointFamily + "\x00" + contributor.Resource
			existing := contributors[key]
			existing.Consumer = restBudgetConsumer(contributor.Consumer)
			existing.CredentialIdentity = contributor.CredentialIdentity
			existing.EndpointFamily = contributor.EndpointFamily
			existing.Resource = contributor.Resource
			existing.Count += contributor.Count
			existing.Conditional += contributor.Conditional
			existing.NotModified += contributor.NotModified
			existing.Billable += contributor.Billable
			existing.RateLimited = existing.RateLimited || contributor.RateLimited
			if contributor.LastStatus != 0 {
				existing.LastStatus = contributor.LastStatus
			}
			if contributor.Limit > 0 || contributor.Remaining > 0 {
				existing.Limit = contributor.Limit
				existing.Remaining = contributor.Remaining
			}
			if contributor.ResetAt != nil {
				resetAt := *contributor.ResetAt
				existing.ResetAt = &resetAt
			}
			if contributor.RetryAfterMS > existing.RetryAfterMS {
				existing.RetryAfterMS = contributor.RetryAfterMS
			}
			contributors[key] = existing
		}
		for _, divergence := range usage.Divergences {
			resetAt := ""
			if divergence.ResetAt != nil {
				resetAt = divergence.ResetAt.UTC().Format(time.RFC3339Nano)
			}
			key := divergence.CredentialIdentity + "\x00" + divergence.Resource + "\x00" + resetAt
			existing, ok := divergences[key]
			if !ok || divergence.ObservedRequests > existing.ObservedRequests ||
				divergence.ObservedRequests == existing.ObservedRequests && divergence.LastObservedAt != nil &&
					(existing.LastObservedAt == nil || divergence.LastObservedAt.After(*existing.LastObservedAt)) {
				divergences[key] = cloneRESTUsageDivergence(divergence)
			}
		}
	}
	for _, contributor := range contributors {
		merged.Contributors = append(merged.Contributors, contributor)
	}
	for _, divergence := range divergences {
		merged.Divergences = append(merged.Divergences, divergence)
	}
	sort.Slice(merged.Contributors, func(i, j int) bool {
		if restBudgetConsumer(merged.Contributors[i].Consumer) != restBudgetConsumer(merged.Contributors[j].Consumer) {
			return restBudgetConsumer(merged.Contributors[i].Consumer) < restBudgetConsumer(merged.Contributors[j].Consumer)
		}
		if merged.Contributors[i].CredentialIdentity != merged.Contributors[j].CredentialIdentity {
			return merged.Contributors[i].CredentialIdentity < merged.Contributors[j].CredentialIdentity
		}
		return merged.Contributors[i].EndpointFamily < merged.Contributors[j].EndpointFamily
	})
	sort.Slice(merged.Divergences, func(i, j int) bool {
		if merged.Divergences[i].CredentialIdentity != merged.Divergences[j].CredentialIdentity {
			return merged.Divergences[i].CredentialIdentity < merged.Divergences[j].CredentialIdentity
		}
		if merged.Divergences[i].Resource != merged.Divergences[j].Resource {
			return merged.Divergences[i].Resource < merged.Divergences[j].Resource
		}
		return timePointerBefore(merged.Divergences[i].ResetAt, merged.Divergences[j].ResetAt)
	})
	return merged
}

func cloneRESTUsageDivergence(divergence telemetry.RESTUsageDivergence) telemetry.RESTUsageDivergence {
	divergence.WindowStartedAt = cloneTimePointer(divergence.WindowStartedAt)
	divergence.LastObservedAt = cloneTimePointer(divergence.LastObservedAt)
	divergence.ResetAt = cloneTimePointer(divergence.ResetAt)
	return divergence
}

func timePointerBefore(left *time.Time, right *time.Time) bool {
	if left == nil || right == nil {
		return left != nil
	}
	return left.Before(*right)
}

func issueKey(issue telemetry.Issue) string {
	for _, value := range []string{issue.URL, issue.Identifier, issue.ID} {
		if key := strings.TrimSpace(value); key != "" {
			return key
		}
	}
	return ""
}

func dedupeSnapshotIssues(snapshot telemetry.Snapshot) telemetry.Snapshot {
	var removed int
	snapshot.Running, removed = dedupeIssueRows(snapshot.Running, func(row telemetry.Running) telemetry.Issue { return row.Issue })
	snapshot.Counts.Running = dedupedCount(snapshot.Counts.Running, removed, len(snapshot.Running))
	snapshot.Queue, removed = dedupeIssueRows(snapshot.Queue, func(row telemetry.Queued) telemetry.Issue { return row.Issue })
	snapshot.Counts.Queue = dedupedCount(snapshot.Counts.Queue, removed, len(snapshot.Queue))
	snapshot.Blocked, removed = dedupeIssueRows(snapshot.Blocked, func(row telemetry.Blocked) telemetry.Issue { return row.Issue })
	snapshot.Counts.Blocked = dedupedCount(snapshot.Counts.Blocked, removed, len(snapshot.Blocked))
	snapshot.Completed, removed = dedupeCompleted(snapshot.Completed)
	snapshot.Counts.Completed = dedupedCount(snapshot.Counts.Completed, removed, len(snapshot.Completed))
	return snapshot
}

func dedupeIssueRows[T any](rows []T, issue func(T) telemetry.Issue) ([]T, int) {
	seen := make(map[string]int, len(rows))
	deduped := make([]T, 0, len(rows))
	for _, row := range rows {
		key := issueKey(issue(row))
		if key == "" {
			deduped = append(deduped, row)
			continue
		}
		_, ok := seen[key]
		if !ok {
			seen[key] = len(deduped)
			deduped = append(deduped, row)
			continue
		}
	}
	return deduped, len(rows) - len(deduped)
}

func dedupeCompleted(rows []telemetry.Completed) ([]telemetry.Completed, int) {
	latest := make(map[string]int, len(rows))
	for i, row := range rows {
		key := issueKey(row.Issue)
		if key == "" {
			continue
		}
		current, ok := latest[key]
		if !ok || row.CompletedAt.After(rows[current].CompletedAt) {
			latest[key] = i
		}
	}

	deduped := make([]telemetry.Completed, 0, len(latest))
	for i, row := range rows {
		key := issueKey(row.Issue)
		if key == "" || latest[key] == i {
			deduped = append(deduped, row)
		}
	}
	return deduped, len(rows) - len(deduped)
}

func dedupedCount(count, removed, length int) int {
	count -= removed
	if count < length {
		return length
	}
	return count
}

func stampSnapshotProjectID(snapshot telemetry.Snapshot) telemetry.Snapshot {
	projectID := strings.TrimSpace(snapshot.Project.ID)
	if projectID == "" {
		return snapshot
	}
	if !snapshot.Release.IsZero() && strings.TrimSpace(snapshot.Release.ProjectID) == "" {
		snapshot.Release.ProjectID = projectID
	}
	for i := range snapshot.Refresh.Sources {
		if strings.TrimSpace(snapshot.Refresh.Sources[i].ProjectID) == "" {
			snapshot.Refresh.Sources[i].ProjectID = projectID
		}
	}
	if len(snapshot.Refresh.Sources) == 0 && snapshot.Refresh.Degraded() {
		snapshot.Refresh.Sources = []telemetry.RefreshSource{{
			ProjectID:     projectID,
			Name:          telemetry.RefreshSourceProject,
			LastSuccessAt: cloneTime(snapshot.Refresh.LastRefreshAt),
			Degraded:      true,
			LastError:     snapshot.Refresh.LastError,
			LastErrorAt:   cloneTime(snapshot.Refresh.LastErrorAt),
		}}
	}

	for i := range snapshot.Pipeline {
		snapshot.Pipeline[i] = stampIssueProjectID(snapshot.Pipeline[i], projectID)
	}
	for i := range snapshot.BoardIssues {
		snapshot.BoardIssues[i] = stampIssueProjectID(snapshot.BoardIssues[i], projectID)
	}
	for i := range snapshot.TrackerDrift.UntrackedOpen {
		snapshot.TrackerDrift.UntrackedOpen[i] = stampIssueProjectID(snapshot.TrackerDrift.UntrackedOpen[i], projectID)
	}
	for i := range snapshot.TrackerDrift.OpenTerminal {
		snapshot.TrackerDrift.OpenTerminal[i] = stampIssueProjectID(snapshot.TrackerDrift.OpenTerminal[i], projectID)
	}
	for i := range snapshot.TrackerDrift.ClosedActive {
		snapshot.TrackerDrift.ClosedActive[i] = stampIssueProjectID(snapshot.TrackerDrift.ClosedActive[i], projectID)
	}
	for i := range snapshot.Running {
		snapshot.Running[i].Issue = stampIssueProjectID(snapshot.Running[i].Issue, projectID)
	}
	for i := range snapshot.Queue {
		snapshot.Queue[i].Issue = stampIssueProjectID(snapshot.Queue[i].Issue, projectID)
	}
	for i := range snapshot.Blocked {
		snapshot.Blocked[i].Issue = stampIssueProjectID(snapshot.Blocked[i].Issue, projectID)
	}
	for i := range snapshot.Completed {
		snapshot.Completed[i].Issue = stampIssueProjectID(snapshot.Completed[i].Issue, projectID)
	}
	for i := range snapshot.WorkAttempts {
		if strings.TrimSpace(snapshot.WorkAttempts[i].ProjectID) == "" {
			snapshot.WorkAttempts[i].ProjectID = projectID
		}
	}
	for i := range snapshot.SchedulerDecisions {
		if strings.TrimSpace(snapshot.SchedulerDecisions[i].ProjectID) == "" {
			snapshot.SchedulerDecisions[i].ProjectID = projectID
		}
	}
	if strings.TrimSpace(snapshot.Dispatch.ProjectID) == "" {
		snapshot.Dispatch.ProjectID = projectID
	}
	for i := range snapshot.DispatchStalls {
		if strings.TrimSpace(snapshot.DispatchStalls[i].ProjectID) == "" {
			snapshot.DispatchStalls[i].ProjectID = projectID
		}
	}
	for i := range snapshot.BackendOutages {
		if strings.TrimSpace(snapshot.BackendOutages[i].ProjectID) == "" {
			snapshot.BackendOutages[i].ProjectID = projectID
		}
	}
	for i := range snapshot.CIUnavailable {
		if strings.TrimSpace(snapshot.CIUnavailable[i].ProjectID) == "" {
			snapshot.CIUnavailable[i].ProjectID = projectID
		}
	}
	for i := range snapshot.TrackerUnavailable {
		if strings.TrimSpace(snapshot.TrackerUnavailable[i].ProjectID) == "" {
			snapshot.TrackerUnavailable[i].ProjectID = projectID
		}
	}
	for i := range snapshot.ForgeUnavailable {
		if strings.TrimSpace(snapshot.ForgeUnavailable[i].ProjectID) == "" {
			snapshot.ForgeUnavailable[i].ProjectID = projectID
		}
	}
	for i := range snapshot.FailureBreakers {
		if strings.TrimSpace(snapshot.FailureBreakers[i].ProjectID) == "" {
			snapshot.FailureBreakers[i].ProjectID = projectID
		}
	}
	for i := range snapshot.DispatchLoops {
		if strings.TrimSpace(snapshot.DispatchLoops[i].ProjectID) == "" {
			snapshot.DispatchLoops[i].ProjectID = projectID
		}
	}
	for i := range snapshot.DispatchRecoveries {
		if strings.TrimSpace(snapshot.DispatchRecoveries[i].ProjectID) == "" {
			snapshot.DispatchRecoveries[i].ProjectID = projectID
		}
	}
	for i := range snapshot.StalenessWarnings {
		if strings.TrimSpace(snapshot.StalenessWarnings[i].ProjectID) == "" {
			snapshot.StalenessWarnings[i].ProjectID = projectID
		}
	}
	for i := range snapshot.StrandedActiveIssues {
		if strings.TrimSpace(snapshot.StrandedActiveIssues[i].ProjectID) == "" {
			snapshot.StrandedActiveIssues[i].ProjectID = projectID
		}
	}
	for i := range snapshot.CleanupFaults {
		if strings.TrimSpace(snapshot.CleanupFaults[i].ProjectID) == "" {
			snapshot.CleanupFaults[i].ProjectID = projectID
		}
	}
	return snapshot
}

func stampIssueProjectID(issue telemetry.Issue, projectID string) telemetry.Issue {
	if strings.TrimSpace(issue.ProjectID) == "" {
		issue.ProjectID = projectID
	}
	return issue
}

func mergeShutdown(current, next telemetry.Shutdown) telemetry.Shutdown {
	if next == (telemetry.Shutdown{}) {
		if current == (telemetry.Shutdown{}) {
			return telemetry.Shutdown{Status: "running"}
		}
		return current
	}
	if current == (telemetry.Shutdown{}) {
		current = telemetry.Shutdown{Status: "running"}
	}
	if strings.TrimSpace(next.Status) == "" {
		next.Status = "running"
	}
	if !current.Draining && !next.Draining {
		if current.Status == "" || current.Status == "running" {
			current.Status = next.Status
		}
		return current
	}

	current.Status = "draining"
	current.Draining = current.Draining || next.Draining
	current.SessionsRemaining += next.SessionsRemaining
	current.RequestedAt = earliestTime(current.RequestedAt, next.RequestedAt)
	current.CompletedAt = latestTime(current.CompletedAt, next.CompletedAt)
	if strings.TrimSpace(next.Result) != "" {
		current.Result = next.Result
	}
	return current
}

func projectSnapshot(snapshot telemetry.Snapshot) telemetry.ProjectSnapshot {
	return telemetry.ProjectSnapshot{
		Project:    snapshot.Project,
		Tracker:    snapshot.Tracker,
		Runtime:    snapshot.Runtime,
		Counts:     snapshot.Counts,
		Tokens:     snapshot.Tokens,
		Throughput: snapshot.Throughput,
		Auth:       snapshot.Auth,
		Refresh:    snapshot.Refresh,
		Dispatch:   snapshot.Dispatch,
	}
}

func liveSnapshotSection(observedAt time.Time) telemetry.SnapshotSection {
	return telemetry.SnapshotSection{
		Source:     telemetry.SnapshotSourceLive,
		ObservedAt: observedAt,
		Complete:   true,
	}
}

func unknownSnapshotSection() telemetry.SnapshotSection {
	return telemetry.SnapshotSection{Source: telemetry.SnapshotSourceUnknown}
}

func summarizeSnapshotSections(snapshot *telemetry.Snapshot) {
	if snapshot == nil {
		return
	}
	snapshot.Tracker = summarizeProjectSections(snapshot.Projects, func(project telemetry.ProjectSnapshot) telemetry.SnapshotSection {
		return project.Tracker
	})
	snapshot.Runtime = summarizeProjectSections(snapshot.Projects, func(project telemetry.ProjectSnapshot) telemetry.SnapshotSection {
		return project.Runtime
	})
}

func summarizeProjectSections(
	projects []telemetry.ProjectSnapshot,
	section func(telemetry.ProjectSnapshot) telemetry.SnapshotSection,
) telemetry.SnapshotSection {
	if len(projects) == 0 {
		return unknownSnapshotSection()
	}
	result := telemetry.SnapshotSection{Complete: true}
	var source telemetry.SnapshotSource
	considered := 0
	for _, project := range projects {
		item := section(project)
		if item.IsZero() {
			continue
		}
		considered++
		if !item.Available() || !item.Complete {
			result.Complete = false
		}
		if item.Available() && (result.ObservedAt.IsZero() || item.ObservedAt.Before(result.ObservedAt)) {
			result.ObservedAt = item.ObservedAt
		}
		if source == "" {
			source = item.Source
		} else if source != item.Source {
			source = telemetry.SnapshotSourceMixed
		}
	}
	if considered == 0 {
		return telemetry.SnapshotSection{}
	}
	result.Source = source
	if !result.Available() {
		result.ObservedAt = time.Time{}
	}
	return result
}

func composeLastKnownTrackerState(current, live telemetry.Snapshot, now time.Time) telemetry.Snapshot {
	if current.LastKnownUntil.IsZero() || !now.Before(current.LastKnownUntil) {
		return live
	}
	cachedProjects := make(map[string]telemetry.ProjectSnapshot, len(current.Projects))
	for _, project := range current.Projects {
		projectID := strings.TrimSpace(project.Project.ID)
		if projectID != "" {
			cachedProjects[projectID] = project
		}
	}
	cachedFallbackProjectID := strings.TrimSpace(current.Project.ID)
	if cachedFallbackProjectID == "" && len(current.Projects) == 1 {
		cachedFallbackProjectID = strings.TrimSpace(current.Projects[0].Project.ID)
	}
	if cachedFallbackProjectID == "" && len(live.Projects) == 1 {
		cachedFallbackProjectID = strings.TrimSpace(live.Projects[0].Project.ID)
	}

	useCached := make(map[string]struct{})
	for index := range live.Projects {
		project := &live.Projects[index]
		projectID := strings.TrimSpace(project.Project.ID)
		if projectID == "" || project.Tracker.Available() || !snapshotRefreshHasSignal(project.Refresh) {
			continue
		}
		cachedProject, hasCachedProject := cachedProjects[projectID]
		if !hasCachedProject && !snapshotHasCachedTrackerData(current, projectID, cachedFallbackProjectID) {
			continue
		}
		observedAt := cachedProject.Tracker.ObservedAt
		if observedAt.IsZero() {
			observedAt = current.GeneratedAt
		}
		project.Tracker = telemetry.SnapshotSection{
			Source:     telemetry.SnapshotSourceCached,
			ObservedAt: observedAt,
			Complete:   true,
		}
		useCached[projectID] = struct{}{}
	}
	if len(useCached) == 0 {
		return live
	}

	live.BoardIssues = appendCachedTrackerIssues(live.BoardIssues, current.BoardIssues, useCached, cachedFallbackProjectID)
	live.Pipeline = appendCachedTrackerIssues(live.Pipeline, current.Pipeline, useCached, cachedFallbackProjectID)
	live.TrackerDrift.UntrackedOpen = appendCachedTrackerIssues(live.TrackerDrift.UntrackedOpen, current.TrackerDrift.UntrackedOpen, useCached, cachedFallbackProjectID)
	live.TrackerDrift.OpenTerminal = appendCachedTrackerIssues(live.TrackerDrift.OpenTerminal, current.TrackerDrift.OpenTerminal, useCached, cachedFallbackProjectID)
	live.LastKnown = false
	live.LastKnownUntil = current.LastKnownUntil
	summarizeSnapshotSections(&live)
	return live
}

func snapshotRefreshHasSignal(refresh telemetry.Refresh) bool {
	return refresh.PollIntervalSeconds != 0 ||
		refresh.StaleAfterSeconds != 0 ||
		refresh.FailureThreshold != 0 ||
		refresh.Status != "" ||
		refresh.LastRefreshAt != nil ||
		refresh.NextRefreshAt != nil ||
		strings.TrimSpace(refresh.LastError) != "" ||
		refresh.LastErrorAt != nil ||
		len(refresh.Sources) > 0
}

func snapshotHasCachedTrackerData(snapshot telemetry.Snapshot, projectID, fallbackProjectID string) bool {
	for _, issue := range snapshot.BoardIssues {
		if snapshotIssueProjectID(issue, fallbackProjectID) == projectID {
			return true
		}
	}
	for _, issue := range snapshot.Pipeline {
		if snapshotIssueProjectID(issue, fallbackProjectID) == projectID {
			return true
		}
	}
	return false
}

func appendCachedTrackerIssues(
	live []telemetry.Issue,
	cached []telemetry.Issue,
	projectIDs map[string]struct{},
	fallbackProjectID string,
) []telemetry.Issue {
	for _, issue := range cached {
		if _, ok := projectIDs[snapshotIssueProjectID(issue, fallbackProjectID)]; ok {
			live = append(live, issue)
		}
	}
	return live
}

func snapshotIssueProjectID(issue telemetry.Issue, fallbackProjectID string) string {
	if projectID := strings.TrimSpace(issue.ProjectID); projectID != "" {
		return projectID
	}
	return strings.TrimSpace(fallbackProjectID)
}

func mergeFleetDispatchStatus(current telemetry.DispatchStatus, next telemetry.DispatchStatus, now time.Time) telemetry.DispatchStatus {
	currentClass := observability.Normalize(current.Class, observability.Dispatch(current.Stalled, current.WaitReasonCode))
	nextClass := observability.Normalize(next.Class, observability.Dispatch(next.Stalled, next.WaitReasonCode))
	merged := telemetry.DispatchStatus{
		CandidateCount:         current.CandidateCount + next.CandidateCount,
		EligibleCandidateCount: current.EligibleCandidateCount + next.EligibleCandidateCount,
		SelectedCount:          current.SelectedCount + next.SelectedCount,
		SkippedCount:           current.SkippedCount + next.SkippedCount,
		Stalled:                current.Stalled || next.Stalled,
		Class:                  observability.Merge(currentClass, nextClass),
		RateWindowPacing:       mergeFleetRateWindowPacing(current.RateWindowPacing, next.RateWindowPacing),
	}
	merged.NeedsHumanAttention = merged.Class == observability.ClassFault
	merged.LastSelectedAt = latestTime(current.LastSelectedAt, next.LastSelectedAt)
	merged.ObservedAt = current.ObservedAt
	if next.ObservedAt.After(merged.ObservedAt) {
		merged.ObservedAt = next.ObservedAt
	}
	if merged.LastSelectedAt != nil && !now.IsZero() {
		seconds := max(int64(now.Sub(*merged.LastSelectedAt)/time.Second), 0)
		merged.SecondsSinceLastSelected = &seconds
	}
	if current.Stalled && next.Stalled && (strings.TrimSpace(current.WaitReason) != strings.TrimSpace(next.WaitReason) || strings.TrimSpace(current.WaitReasonCode) != strings.TrimSpace(next.WaitReasonCode)) {
		return merged
	}
	if next.Stalled {
		merged.WaitReason = next.WaitReason
		merged.WaitReasonCode = next.WaitReasonCode
	} else if current.Stalled {
		merged.WaitReason = current.WaitReason
		merged.WaitReasonCode = current.WaitReasonCode
	}
	return merged
}

func mergeFleetRateWindowPacing(current telemetry.RateWindowPacing, next telemetry.RateWindowPacing) telemetry.RateWindowPacing {
	if strings.TrimSpace(current.Mode) == "" {
		return next
	}
	if strings.TrimSpace(next.Mode) == "" {
		return current
	}
	merged := telemetry.RateWindowPacing{
		Mode:              current.Mode,
		FloorPercent:      current.FloorPercent,
		StaleAfterSeconds: current.StaleAfterSeconds,
		Applicable:        current.Applicable || next.Applicable,
		BucketStatus:      current.BucketStatus,
		PermitCeiling:     current.PermitCeiling + next.PermitCeiling,
		ScalingApplied:    current.ScalingApplied || next.ScalingApplied,
	}
	if current.Mode != next.Mode {
		merged.Mode = "mixed"
	}
	if current.FloorPercent != next.FloorPercent {
		merged.FloorPercent = 0
	}
	if current.StaleAfterSeconds != next.StaleAfterSeconds {
		merged.StaleAfterSeconds = 0
	}
	if current.BucketStatus != next.BucketStatus {
		merged.BucketStatus = "mixed"
	}
	merged.ObservedRemainingPercent = current.ObservedRemainingPercent
	merged.ObservedAt = current.ObservedAt
	if next.ObservedRemainingPercent != nil && (merged.ObservedRemainingPercent == nil || *next.ObservedRemainingPercent < *merged.ObservedRemainingPercent) {
		remaining := *next.ObservedRemainingPercent
		merged.ObservedRemainingPercent = &remaining
		merged.ObservedAt = next.ObservedAt
	}
	return merged
}

func mergeProject(current, next telemetry.Project) telemetry.Project {
	if current == (telemetry.Project{}) {
		return next
	}
	if next == (telemetry.Project{}) || current == next {
		return current
	}
	return telemetry.Project{DisplayName: "multiple projects"}
}

func mergeInstance(current, next telemetry.Instance) telemetry.Instance {
	if current == (telemetry.Instance{}) {
		return next
	}
	if next == (telemetry.Instance{}) || current == next {
		return current
	}
	return telemetry.Instance{
		Name:                    mergeInstanceValue(current.Name, next.Name, "multiple instances"),
		GitHubLogin:             mergeInstanceValue(current.GitHubLogin, next.GitHubLogin, "multiple logins"),
		AuthorizationScope:      mergeInstanceValue(current.AuthorizationScope, next.AuthorizationScope, "Multiple authorization scopes"),
		AuthorizationConfigured: current.AuthorizationConfigured || next.AuthorizationConfigured,
	}
}

func mergeInstanceValue(current, next string, mixed string) string {
	current = strings.TrimSpace(current)
	next = strings.TrimSpace(next)
	switch {
	case current == "":
		return next
	case next == "" || current == next:
		return current
	default:
		return mixed
	}
}

func mergeRefresh(current, next telemetry.Refresh) telemetry.Refresh {
	currentHadSignal := refreshHasSignal(current)
	nextHadSignal := refreshHasSignal(next)
	currentStatus := current.ReadinessStatus()
	nextStatus := next.ReadinessStatus()
	if current.PollIntervalSeconds == 0 ||
		(next.PollIntervalSeconds > 0 && next.PollIntervalSeconds < current.PollIntervalSeconds) {
		current.PollIntervalSeconds = next.PollIntervalSeconds
	}
	if current.StaleAfterSeconds == 0 ||
		(next.StaleAfterSeconds > 0 && next.StaleAfterSeconds < current.StaleAfterSeconds) {
		current.StaleAfterSeconds = next.StaleAfterSeconds
	}
	if current.FailureThreshold == 0 ||
		(next.FailureThreshold > 0 && next.FailureThreshold < current.FailureThreshold) {
		current.FailureThreshold = next.FailureThreshold
	}
	if next.DataSeq > current.DataSeq {
		current.DataSeq = next.DataSeq
	}
	current.ObservedSweepSeconds += next.LastDurationSeconds
	if next.BehindBySeconds > current.BehindBySeconds {
		current.BehindBySeconds = next.BehindBySeconds
	}
	current.StalenessWindowExceeded = current.StalenessWindowExceeded || next.StalenessWindowExceeded
	current.LastRefreshAt = latestTime(current.LastRefreshAt, next.LastRefreshAt)
	current.NextRefreshAt = earliestTime(current.NextRefreshAt, next.NextRefreshAt)
	if strings.TrimSpace(next.LastError) != "" {
		if strings.TrimSpace(current.LastError) == "" ||
			current.LastErrorAt == nil ||
			next.LastErrorAt == nil ||
			current.LastErrorAt.Before(*next.LastErrorAt) {
			current.LastError = next.LastError
		}
	}
	current.LastErrorAt = latestTime(current.LastErrorAt, next.LastErrorAt)
	current.Sources = mergeRefreshSources(current.Sources, next.Sources)
	current.Manual = mergeRefreshAttempt(current.Manual, next.Manual)
	switch {
	case !currentHadSignal && nextHadSignal:
		current.Status = nextStatus
	case currentHadSignal && !nextHadSignal:
		current.Status = currentStatus
	case currentStatus == telemetry.RefreshStatusPartial || nextStatus == telemetry.RefreshStatusPartial:
		current.Status = telemetry.RefreshStatusPartial
	case currentStatus == telemetry.RefreshStatusDegraded && nextStatus == telemetry.RefreshStatusDegraded:
		current.Status = telemetry.RefreshStatusDegraded
	case currentStatus == telemetry.RefreshStatusDegraded || nextStatus == telemetry.RefreshStatusDegraded:
		current.Status = telemetry.RefreshStatusPartial
	case currentStatus == telemetry.RefreshStatusInitializing || nextStatus == telemetry.RefreshStatusInitializing:
		current.Status = telemetry.RefreshStatusInitializing
	case currentStatus == telemetry.RefreshStatusBehind || nextStatus == telemetry.RefreshStatusBehind:
		current.Status = telemetry.RefreshStatusBehind
	case currentHadSignal || nextHadSignal:
		current.Status = telemetry.RefreshStatusReady
	default:
		current.Status = ""
	}
	return current
}

func refreshHasSignal(refresh telemetry.Refresh) bool {
	return refresh.PollIntervalSeconds != 0 ||
		refresh.StaleAfterSeconds != 0 ||
		refresh.FailureThreshold != 0 ||
		refresh.Status != "" ||
		refresh.LastRefreshAt != nil ||
		refresh.NextRefreshAt != nil ||
		strings.TrimSpace(refresh.LastError) != "" ||
		refresh.LastErrorAt != nil ||
		len(refresh.Sources) > 0 ||
		refresh.Manual != nil
}

func mergeRefreshSources(current []telemetry.RefreshSource, next []telemetry.RefreshSource) []telemetry.RefreshSource {
	merged := make([]telemetry.RefreshSource, 0, len(current)+len(next))
	index := make(map[string]int, len(current)+len(next))
	appendSource := func(source telemetry.RefreshSource) {
		key := strings.TrimSpace(source.ProjectID) + "\x00" + string(source.Name)
		if existing, ok := index[key]; ok {
			if refreshSourceObservedAt(source).After(refreshSourceObservedAt(merged[existing])) {
				merged[existing] = cloneRefreshSource(source)
			}
			return
		}
		index[key] = len(merged)
		merged = append(merged, cloneRefreshSource(source))
	}
	for _, source := range current {
		appendSource(source)
	}
	for _, source := range next {
		appendSource(source)
	}
	return merged
}

func refreshSourceObservedAt(source telemetry.RefreshSource) time.Time {
	if source.LastErrorAt != nil && (source.LastSuccessAt == nil || source.LastErrorAt.After(*source.LastSuccessAt)) {
		return source.LastErrorAt.UTC()
	}
	if source.LastSuccessAt != nil {
		return source.LastSuccessAt.UTC()
	}
	return time.Time{}
}

func cloneRefreshSource(source telemetry.RefreshSource) telemetry.RefreshSource {
	source.LastSuccessAt = cloneTime(source.LastSuccessAt)
	source.LastErrorAt = cloneTime(source.LastErrorAt)
	return source
}

func mergeRefreshAttempt(current *telemetry.RefreshAttempt, next *telemetry.RefreshAttempt) *telemetry.RefreshAttempt {
	if current == nil {
		return cloneRefreshAttemptPtr(next)
	}
	if next == nil {
		return cloneRefreshAttemptPtr(current)
	}
	if refreshAttemptRequestedAt(next).After(refreshAttemptRequestedAt(current)) {
		return cloneRefreshAttemptPtr(next)
	}
	return cloneRefreshAttemptPtr(current)
}

func refreshAttemptRequestedAt(attempt *telemetry.RefreshAttempt) time.Time {
	if attempt == nil || attempt.RequestedAt == nil {
		return time.Time{}
	}
	return attempt.RequestedAt.UTC()
}

func cloneRefreshAttemptPtr(attempt *telemetry.RefreshAttempt) *telemetry.RefreshAttempt {
	if attempt == nil {
		return nil
	}
	cloned := *attempt
	cloned.RequestedAt = cloneTime(attempt.RequestedAt)
	cloned.StartedAt = cloneTime(attempt.StartedAt)
	cloned.CompletedAt = cloneTime(attempt.CompletedAt)
	cloned.LastErrorAt = cloneTime(attempt.LastErrorAt)
	cloned.Operations = append([]string(nil), attempt.Operations...)
	return &cloned
}

func latestTime(current *time.Time, next *time.Time) *time.Time {
	switch {
	case current == nil:
		return cloneTime(next)
	case next == nil || current.After(*next):
		return cloneTime(current)
	default:
		return cloneTime(next)
	}
}

func earliestTime(current *time.Time, next *time.Time) *time.Time {
	switch {
	case current == nil:
		return cloneTime(next)
	case next == nil || current.Before(*next):
		return cloneTime(current)
	default:
		return cloneTime(next)
	}
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func durationFromMillis(ms int) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}
