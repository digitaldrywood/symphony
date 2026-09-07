package project

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/pause"
)

var (
	ErrManagerRunning  = errors.New("project manager already running")
	ErrProjectExists   = errors.New("project already exists")
	ErrProjectNotFound = errors.New("project not found")
)

const (
	defaultMaxConcurrentStarts    = 4
	defaultStartupRollbackTimeout = 5 * time.Second
	defaultRetryInitialBackoff    = time.Second
	defaultRetryMaxBackoff        = time.Minute
	defaultRetryJitter            = 250 * time.Millisecond
)

type Factory func(globalconfig.Project) (*Project, error)

type StartupConfig struct {
	Jitter              time.Duration
	MaxSpawnPerSecond   int
	MaxConcurrentStarts int
}

type ManagerConfig struct {
	Identity                 globalconfig.Identity
	Projects                 []globalconfig.Project
	Startup                  StartupConfig
	RuntimeCredentialVersion string
}

type ConnectorRetryConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Jitter         time.Duration
}

type ReconcileResult struct {
	Added           []ID
	Removed         []ID
	Changed         []ID
	Unchanged       []ID
	DrainedSessions []ReconcileSession
}

type ReconcileSession struct {
	ProjectID         ID
	IssueID           string
	Identifier        string
	WorkAttemptID     int64
	DetentSessionID   int64
	ProviderSessionID string
}

type startedProject struct {
	project *Project
}

type rollbackProject struct {
	project    *Project
	wasRunning bool
}

type rollbackActiveHours struct {
	project *Project
	config  globalconfig.Project
}

type ManagerDependencies struct {
	Registry               *Registry
	ProjectFactory         Factory
	ProjectDependencies    Dependencies
	Events                 *hub.Hub[Event]
	Logger                 *slog.Logger
	Sleep                  func(context.Context, time.Duration) error
	Jitter                 func(time.Duration) time.Duration
	ConnectorRetry         ConnectorRetryConfig
	RetrySleep             func(context.Context, time.Duration) error
	RetryJitter            func(time.Duration) time.Duration
	Now                    func() time.Time
	StartupRollbackTimeout time.Duration
}

type retryRun struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type Manager struct {
	operationMu            sync.Mutex
	mu                     sync.Mutex
	cfg                    ManagerConfig
	registry               *Registry
	factory                Factory
	sleep                  func(context.Context, time.Duration) error
	jitter                 func(time.Duration) time.Duration
	logger                 *slog.Logger
	retry                  ConnectorRetryConfig
	retrySleep             func(context.Context, time.Duration) error
	retryJitter            func(time.Duration) time.Duration
	now                    func() time.Time
	retryRuns              map[ID]*retryRun
	retryWG                sync.WaitGroup
	startupRollbackTimeout time.Duration

	running bool
	spawned bool
}

func ManagerConfigFromGlobal(cfg globalconfig.Config) ManagerConfig {
	projects := append([]globalconfig.Project(nil), cfg.Projects...)
	for index := range projects {
		projects[index].GlobalAgents = cfg.Global.Agents
		projects[index].GlobalBudget = cfg.Global.Budget
		projects[index].GlobalKnowledge = cfg.Global.Knowledge
		projects[index].GlobalRateWindowPacing = cfg.Global.RateWindowPacing
		projects[index].GlobalMemory = cfg.Global.Memory
		projects[index].GlobalIO = cfg.Global.IO
		projects[index].GlobalCPU = cfg.Global.CPU
		if cfg.Global.ActiveHours != nil {
			activeHours := cfg.Global.ActiveHours.Normalize()
			projects[index].GlobalActiveHours = &activeHours
		}
	}

	return normalizeManagerConfig(ManagerConfig{
		Identity: cfg.Global.Identity,
		Projects: projects,
		Startup: StartupConfig{
			Jitter:              time.Duration(startupInt(cfg.Global.Startup, "jitter_seconds")) * time.Second,
			MaxSpawnPerSecond:   startupInt(cfg.Global.Startup, "max_spawn_per_second"),
			MaxConcurrentStarts: startupInt(cfg.Global.Startup, "max_concurrent_starts"),
		},
	})
}

func NewManager(cfg ManagerConfig, deps ManagerDependencies) (*Manager, error) {
	registry := deps.Registry
	if registry == nil {
		registry = NewRegistry()
	}

	projectDeps := deps.ProjectDependencies
	if projectDeps.Events == nil {
		projectDeps.Events = deps.Events
	}
	if projectDeps.Logger == nil {
		projectDeps.Logger = deps.Logger
	}

	factory := deps.ProjectFactory
	if factory == nil {
		factory = func(cfg globalconfig.Project) (*Project, error) {
			return Load(cfg, projectDeps)
		}
	}

	sleep := deps.Sleep
	if sleep == nil {
		sleep = sleepContext
	}

	jitter := deps.Jitter
	if jitter == nil {
		jitter = randomJitter
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	retrySleep := deps.RetrySleep
	if retrySleep == nil {
		retrySleep = sleepContext
	}
	retryJitter := deps.RetryJitter
	if retryJitter == nil {
		retryJitter = randomJitter
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	startupRollbackTimeout := deps.StartupRollbackTimeout
	if startupRollbackTimeout <= 0 {
		startupRollbackTimeout = defaultStartupRollbackTimeout
	}

	cfg = normalizeManagerConfig(cfg)
	return &Manager{
		cfg:                    cfg,
		registry:               registry,
		factory:                factory,
		sleep:                  sleep,
		jitter:                 jitter,
		logger:                 logger,
		retry:                  normalizeConnectorRetryConfig(deps.ConnectorRetry),
		retrySleep:             retrySleep,
		retryJitter:            retryJitter,
		now:                    now,
		retryRuns:              map[ID]*retryRun{},
		startupRollbackTimeout: startupRollbackTimeout,
	}, nil
}

func (m *Manager) Registry() *Registry {
	return m.registry
}

func (m *Manager) Wait() {
	if m == nil {
		return
	}
	m.retryWG.Wait()
}

func (m *Manager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return ErrManagerRunning
	}
	m.running = true

	projects := make([]*Project, 0, len(m.cfg.Projects))
	rollbackIDs := make([]ID, 0, len(m.cfg.Projects))
	for _, cfg := range m.cfg.Projects {
		id, project, err := m.createProjectLocked(cfg)
		if err != nil {
			if m.handleInitialCreationFailureLocked(ctx, cfg, err) {
				if errors.Is(err, ErrProjectDefinition) {
					rollbackIDs = append(rollbackIDs, normalizeProjectID(ID(cfg.ID)))
				}
				continue
			}
			for _, rollbackID := range rollbackIDs {
				m.registry.Delete(rollbackID)
			}
			m.running = false
			m.mu.Unlock()
			return errors.Join(err, closeProjectSlice(ctx, projects))
		}
		if _, ok := m.registry.Get(id); ok {
			for _, rollbackID := range rollbackIDs {
				m.registry.Delete(rollbackID)
			}
			m.running = false
			m.mu.Unlock()
			return errors.Join(ErrProjectExists, project.close(ctx, false), closeProjectSlice(ctx, projects))
		}
		if err := m.registry.Set(project); err != nil {
			for _, rollbackID := range rollbackIDs {
				m.registry.Delete(rollbackID)
			}
			m.running = false
			m.mu.Unlock()
			return errors.Join(err, project.close(ctx, false), closeProjectSlice(ctx, projects))
		}
		rollbackIDs = append(rollbackIDs, id)
		projects = append(projects, project)
	}
	if err := validatePauseExitReferences(projects); err != nil {
		for _, rollbackID := range rollbackIDs {
			m.registry.Delete(rollbackID)
		}
		m.running = false
		m.mu.Unlock()
		return errors.Join(err, closeProjectSlice(ctx, projects))
	}
	startup := m.cfg.Startup
	m.mu.Unlock()

	started, err := m.startInitialProjects(ctx, projects, startup)
	if err != nil {
		return errors.Join(err, m.rollbackInitialStart(ctx, rollbackIDs, projects, started))
	}
	if len(started) > 0 {
		m.mu.Lock()
		m.spawned = true
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) Add(ctx context.Context, cfg globalconfig.Project) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.addLocked(ctx, cfg)
}

func (m *Manager) Remove(ctx context.Context, id ID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeLocked(ctx, id)
}

func (m *Manager) Reconcile(ctx context.Context, cfg ManagerConfig) (ReconcileResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	cfg = normalizeManagerConfig(cfg)
	desired := make(map[ID]globalconfig.Project, len(cfg.Projects))
	for i, project := range cfg.Projects {
		normalized := project
		id := ID(normalized.ID)
		if id == "" {
			return ReconcileResult{}, ErrMissingProjectID
		}
		if _, ok := desired[id]; ok {
			return ReconcileResult{}, ErrProjectExists
		}
		cfg.Projects[i] = normalized
		desired[id] = normalized
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	runtimeCredentialChanged := m.cfg.RuntimeCredentialVersion != cfg.RuntimeCredentialVersion
	result := ReconcileResult{}
	seen := make(map[ID]struct{}, len(m.cfg.Projects))
	pending := map[ID]struct{}{}
	liveConfigChanges := map[ID]*Project{}
	for _, current := range m.registry.List() {
		id := current.ID()
		seen[id] = struct{}{}
		next, ok := desired[id]
		if !ok {
			result.Removed = append(result.Removed, id)
			continue
		}
		if !runtimeCredentialChanged && sameProjectConfig(current.Config(), next) {
			result.Unchanged = append(result.Unchanged, id)
			continue
		}
		if !runtimeCredentialChanged && sameProjectConfigExceptLiveFields(current.Config(), next) {
			result.Changed = append(result.Changed, id)
			liveConfigChanges[id] = current
			continue
		}
		result.Changed = append(result.Changed, id)
	}
	for _, health := range m.registry.Health() {
		id := normalizeProjectID(ID(health.Project.ID))
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		pending[id] = struct{}{}
		next, ok := desired[id]
		if !ok {
			result.Removed = append(result.Removed, id)
			continue
		}
		if !runtimeCredentialChanged && sameProjectConfig(health.Project, next) {
			result.Unchanged = append(result.Unchanged, id)
			continue
		}
		result.Changed = append(result.Changed, id)
	}

	for _, next := range cfg.Projects {
		id := ID(next.ID)
		if _, ok := seen[id]; !ok {
			result.Added = append(result.Added, id)
		}
	}

	prepared := make(map[ID]*Project, len(result.Added)+len(result.Changed))
	handled := map[ID]struct{}{}
	for _, id := range result.Changed {
		if _, ok := liveConfigChanges[id]; ok {
			continue
		}
		_, preparedProject, err := m.createProjectLocked(desired[id])
		if err != nil {
			if _, ok := pending[id]; ok && m.handleInitialCreationFailureLocked(ctx, desired[id], err) {
				handled[id] = struct{}{}
				continue
			}
			return result, errors.Join(err, closePreparedProjects(ctx, prepared))
		}
		prepared[id] = preparedProject
	}
	for _, id := range result.Added {
		_, preparedProject, err := m.createProjectLocked(desired[id])
		if err != nil {
			if m.handleInitialCreationFailureLocked(ctx, desired[id], err) {
				handled[id] = struct{}{}
				continue
			}
			return result, errors.Join(err, closePreparedProjects(ctx, prepared))
		}
		prepared[id] = preparedProject
	}
	validationProjects := make([]*Project, 0, len(desired))
	for id := range desired {
		if preparedProject := prepared[id]; preparedProject != nil {
			validationProjects = append(validationProjects, preparedProject)
			continue
		}
		if current, ok := m.registry.Get(id); ok && current != nil {
			validationProjects = append(validationProjects, current)
		}
	}
	if err := validatePauseExitReferences(validationProjects); err != nil {
		return result, errors.Join(err, closePreparedProjects(ctx, prepared))
	}
	result.DrainedSessions = m.reconcileSessionInventory(ctx, result, liveConfigChanges)

	previous := m.cfg
	previousSpawned := m.spawned
	m.cfg.Startup = cfg.Startup
	stopped := make([]rollbackProject, 0, len(result.Removed)+len(result.Changed))
	started := make([]startedProject, 0, len(prepared))
	added := map[ID]struct{}{}
	updatedLiveConfigs := make([]rollbackActiveHours, 0, len(liveConfigChanges))
	rollback := func() error {
		cleanupErr := m.stopUncommittedStartedProjects(ctx, started)
		cleanupErr = errors.Join(cleanupErr, closePreparedProjects(ctx, prepared))
		for i := len(updatedLiveConfigs) - 1; i >= 0; i-- {
			item := updatedLiveConfigs[i]
			cleanupErr = errors.Join(cleanupErr, item.project.updateLiveConfig(ctx, item.config))
		}
		for id := range added {
			m.registry.Delete(id)
		}
		for i := len(stopped) - 1; i >= 0; i-- {
			item := stopped[i]
			if err := m.registry.Set(item.project); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		m.cfg = previous
		m.spawned = previousSpawned
		for i := len(stopped) - 1; i >= 0; i-- {
			item := stopped[i]
			if !item.wasRunning || item.project.Running() {
				continue
			}
			if err := m.restartProjectLocked(ctx, item.project, false); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		return cleanupErr
	}

	for _, id := range result.Removed {
		if err := m.cancelAndWaitRetryLocked(ctx, id); err != nil {
			return result, errors.Join(err, rollback())
		}
		current, ok := m.registry.Get(id)
		if !ok {
			if _, pending := m.registry.Pending(id); pending && m.registry.Delete(id) {
				continue
			}
		}
		if !ok || current == nil {
			return result, errors.Join(ErrProjectNotFound, rollback())
		}
		wasRunning := current.Running()
		if wasRunning {
			if err := current.stop(ctx, false); err != nil {
				return result, errors.Join(err, rollback())
			}
		}
		stopped = append(stopped, rollbackProject{project: current, wasRunning: wasRunning})
		if !m.registry.Delete(id) {
			return result, errors.Join(ErrProjectNotFound, rollback())
		}
	}
	for _, id := range result.Changed {
		if _, ok := handled[id]; ok {
			continue
		}
		if current, ok := liveConfigChanges[id]; ok {
			previousConfig := current.Config()
			if err := current.updateLiveConfig(ctx, desired[id]); err != nil {
				return result, errors.Join(err, rollback())
			}
			updatedLiveConfigs = append(updatedLiveConfigs, rollbackActiveHours{
				project: current,
				config:  previousConfig,
			})
			continue
		}
		preparedProject, err := preparedProjectByID(prepared, id)
		if err != nil {
			return result, errors.Join(err, rollback())
		}
		if _, wasPending := pending[id]; wasPending {
			didStart, startErr := m.startPreparedProjectLocked(ctx, preparedProject)
			if startErr != nil && ctx.Err() != nil {
				return result, errors.Join(startErr, rollback())
			}
			if err := m.cancelAndWaitRetryLocked(ctx, id); err != nil {
				return result, errors.Join(err, rollback())
			}
			m.registry.Delete(id)
			if startErr != nil {
				m.handleProjectStartupFailureLocked(ctx, preparedProject, startErr)
			}
			if err := m.registry.Set(preparedProject); err != nil {
				return result, errors.Join(err, rollback())
			}
			if didStart {
				started = append(started, startedProject{project: preparedProject})
			}
			continue
		}
		if err := m.cancelAndWaitRetryLocked(ctx, id); err != nil {
			return result, errors.Join(err, rollback())
		}
		current, ok := m.registry.Get(id)
		if !ok || current == nil {
			return result, errors.Join(ErrProjectNotFound, rollback())
		}
		wasRunning := current.Running()
		if wasRunning {
			if err := current.stop(ctx, false); err != nil {
				return result, errors.Join(err, rollback())
			}
		}
		stopped = append(stopped, rollbackProject{project: current, wasRunning: wasRunning})
		didStart, err := m.startPreparedProjectLocked(ctx, preparedProject)
		if err != nil {
			return result, errors.Join(err, rollback())
		}
		if didStart {
			started = append(started, startedProject{project: preparedProject})
		}
		if err := m.registry.Set(preparedProject); err != nil {
			return result, errors.Join(err, rollback())
		}
	}
	for _, id := range result.Added {
		if _, ok := handled[id]; ok {
			continue
		}
		preparedProject, err := preparedProjectByID(prepared, id)
		if err != nil {
			return result, errors.Join(err, rollback())
		}
		didStart, err := m.startPreparedProjectLocked(ctx, preparedProject)
		if err != nil {
			if len(result.Removed) > 0 || len(result.Changed) > 0 || !retainAddedProjectStartFailure(preparedProject, err) {
				return result, errors.Join(err, rollback())
			}
			m.handleProjectStartupFailureLocked(ctx, preparedProject, err)
			if err := m.registry.Set(preparedProject); err != nil {
				return result, errors.Join(err, rollback())
			}
			added[id] = struct{}{}
			continue
		}
		if didStart {
			started = append(started, startedProject{project: preparedProject})
		}
		if err := m.registry.Set(preparedProject); err != nil {
			return result, errors.Join(err, rollback())
		}
		added[id] = struct{}{}
	}

	m.cfg = cfg
	for _, item := range stopped {
		if item.wasRunning {
			item.project.publishStopped(nil)
		}
	}
	for _, item := range started {
		item.project.publishStarted()
	}
	return result, closeStoppedProjects(ctx, stopped)
}

func (m *Manager) reconcileSessionInventory(
	ctx context.Context,
	result ReconcileResult,
	liveConfigChanges map[ID]*Project,
) []ReconcileSession {
	projectIDs := append([]ID(nil), result.Removed...)
	for _, id := range result.Changed {
		if _, live := liveConfigChanges[id]; !live {
			projectIDs = append(projectIDs, id)
		}
	}

	var sessions []ReconcileSession
	for _, id := range projectIDs {
		trackedProject, ok := m.registry.Get(id)
		if !ok || trackedProject == nil || !trackedProject.Running() || trackedProject.Orchestrator() == nil {
			continue
		}
		state, err := trackedProject.Orchestrator().State(ctx)
		if err != nil {
			m.logger.Warn("inventory project reconciliation sessions failed", "project_id", id, "error", err)
			continue
		}
		for issueID, running := range state.Running {
			sessions = append(sessions, ReconcileSession{
				ProjectID:         id,
				IssueID:           issueID,
				Identifier:        running.Issue.Identifier,
				WorkAttemptID:     running.WorkAttemptID,
				DetentSessionID:   running.DetentSessionID,
				ProviderSessionID: running.SessionID,
			})
		}
	}
	sort.Slice(sessions, func(left int, right int) bool {
		if sessions[left].ProjectID != sessions[right].ProjectID {
			return sessions[left].ProjectID < sessions[right].ProjectID
		}
		if sessions[left].Identifier != sessions[right].Identifier {
			return sessions[left].Identifier < sessions[right].Identifier
		}
		return sessions[left].IssueID < sessions[right].IssueID
	})
	return sessions
}

func validatePauseExitReferences(projects []*Project) error {
	trackers := make([]pause.Tracker, 0, len(projects))
	for _, trackedProject := range projects {
		if trackedProject == nil {
			continue
		}
		workflow := trackedProject.Workflow().Config
		trackers = append(trackers, pause.Tracker{
			ProjectID:  string(trackedProject.ID()),
			Kind:       workflow.Tracker.Kind,
			Repository: workflow.Tracker.Repository,
		})
	}
	for _, trackedProject := range projects {
		if trackedProject == nil {
			continue
		}
		projectConfig := trackedProject.Config()
		reference := strings.TrimSpace(projectConfig.PausedUntilIssue)
		if !projectConfig.Paused || reference == "" {
			continue
		}
		resolution, err := pause.ResolveReference(projectConfig.ID, reference, trackers)
		if err != nil {
			return fmt.Errorf("validate project %s paused_until_issue %s: %w", projectConfig.ID, reference, err)
		}
		owner, ok := projectByID(projects, ID(resolution.ProjectID))
		if !ok {
			return fmt.Errorf("validate project %s paused_until_issue %s: resolver project %s is unavailable", projectConfig.ID, reference, resolution.ProjectID)
		}
		if _, ok := owner.Connector().(connector.IssueReferenceResolver); !ok {
			return fmt.Errorf("validate project %s paused_until_issue %s: project %s tracker kind %s cannot resolve issue references", projectConfig.ID, reference, resolution.ProjectID, owner.Workflow().Config.Tracker.Kind)
		}
	}
	return nil
}

func projectByID(projects []*Project, id ID) (*Project, bool) {
	id = normalizeProjectID(id)
	for _, trackedProject := range projects {
		if trackedProject != nil && normalizeProjectID(trackedProject.ID()) == id {
			return trackedProject, true
		}
	}
	return nil, false
}

func (m *Manager) removeLocked(ctx context.Context, id ID) error {
	if err := m.cancelAndWaitRetryLocked(ctx, id); err != nil {
		return err
	}
	project, ok := m.registry.Get(id)
	if !ok || project == nil {
		if _, pending := m.registry.Pending(id); pending && m.registry.Delete(id) {
			return nil
		}
		return ErrProjectNotFound
	}
	if err := project.close(ctx, true); err != nil {
		return err
	}
	m.registry.Delete(id)
	return nil
}

func (m *Manager) Pause(ctx context.Context, id ID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.cancelAndWaitRetryLocked(ctx, id); err != nil {
		return err
	}

	project, ok := m.registry.Get(id)
	if !ok || project == nil {
		return ErrProjectNotFound
	}
	return project.Pause(ctx)
}

func (m *Manager) Unpause(ctx context.Context, id ID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	project, ok := m.registry.Get(id)
	if !ok || project == nil {
		return ErrProjectNotFound
	}
	if !project.Paused() {
		return nil
	}
	if m.running {
		if err := m.waitBeforeSpawn(ctx); err != nil {
			return err
		}
	}
	if err := project.Unpause(ctx); err != nil {
		return err
	}
	m.spawned = true
	return nil
}

func (m *Manager) addLocked(ctx context.Context, cfg globalconfig.Project) error {
	cfg = normalizeManagerProjectConfigWithIdentity(cfg, m.cfg.Identity)
	id := ID(cfg.ID)
	if id == "" {
		return ErrMissingProjectID
	}
	if _, ok := m.registry.Get(id); ok {
		return ErrProjectExists
	}

	_, project, err := m.createProjectLocked(cfg)
	if err != nil {
		return err
	}
	return m.registerProjectLocked(ctx, id, project)
}

func (m *Manager) createProjectLocked(cfg globalconfig.Project) (ID, *Project, error) {
	id := normalizeProjectID(ID(cfg.ID))
	if id == "" {
		return "", nil, ErrMissingProjectID
	}
	cfg.ID = string(id)
	project, err := m.factory(cfg)
	if err != nil {
		return "", nil, fmt.Errorf("create project %s: %w", id, err)
	}
	if project == nil {
		return "", nil, ErrMissingProject
	}
	return id, project, nil
}

func (m *Manager) registerProjectLocked(ctx context.Context, id ID, project *Project) error {
	if project == nil {
		return ErrMissingProject
	}
	if err := m.registry.Set(project); err != nil {
		return err
	}
	if !m.running || project.Paused() {
		return nil
	}
	if err := m.startLocked(ctx, project); err != nil {
		m.registry.Delete(id)
		return errors.Join(err, project.close(ctx, false))
	}
	return nil
}

func (m *Manager) startPreparedProjectLocked(ctx context.Context, project *Project) (bool, error) {
	if project == nil {
		return false, ErrMissingProject
	}
	if !m.running || project.Paused() {
		return false, nil
	}
	if err := m.startProjectLocked(ctx, project, false); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) startLocked(ctx context.Context, project *Project) error {
	return m.startProjectLocked(ctx, project, true)
}

func (m *Manager) startProjectLocked(ctx context.Context, project *Project, publishEvents bool) error {
	if project == nil {
		return ErrMissingProject
	}
	if err := m.waitBeforeSpawn(ctx); err != nil {
		return err
	}
	if err := project.start(ctx, startOptions{provision: true, publishEvents: publishEvents}); err != nil {
		return err
	}
	m.spawned = true
	return nil
}

func (m *Manager) restartProjectLocked(ctx context.Context, project *Project, publishEvents bool) error {
	if err := m.waitBeforeSpawn(ctx); err != nil {
		return err
	}
	if err := project.restart(ctx, startOptions{provision: true, publishEvents: publishEvents}); err != nil {
		return err
	}
	m.spawned = true
	return nil
}

func (m *Manager) stopUncommittedStartedProjects(ctx context.Context, started []startedProject) error {
	var cleanupErr error
	for i := len(started) - 1; i >= 0; i-- {
		item := started[i]
		if item.project.Running() {
			cleanupErr = errors.Join(cleanupErr, item.project.stop(ctx, false))
		}
	}
	return cleanupErr
}

func (m *Manager) startInitialProjects(
	ctx context.Context,
	projects []*Project,
	startup StartupConfig,
) ([]startedProject, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(maxConcurrentStarts(startup, len(projects)))

	limiter := startupLimiter{
		startup: startup,
		sleep:   m.sleep,
		jitter:  m.jitter,
	}
	var startedMu sync.Mutex
	started := make([]startedProject, 0, len(projects))
	for _, project := range projects {
		trackedProject := project
		if trackedProject.Paused() {
			m.logger.Info("project startup skipped", "project_id", trackedProject.ID(), "reason", "paused")
			continue
		}
		group.Go(func() error {
			if err := limiter.wait(groupCtx); err != nil {
				return err
			}
			if err := trackedProject.provision(groupCtx); err != nil {
				if groupCtx.Err() != nil {
					return err
				}
				m.handleProjectStartupFailure(ctx, trackedProject, err)
				return nil
			}
			if err := groupCtx.Err(); err != nil {
				return err
			}
			if err := trackedProject.start(ctx, startOptions{provision: false, publishEvents: true}); err != nil {
				if ctx.Err() != nil {
					return err
				}
				m.handleProjectStartupFailure(ctx, trackedProject, err)
				return nil
			}
			startedMu.Lock()
			started = append(started, startedProject{project: trackedProject})
			startedMu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return started, err
	}
	return started, nil
}

func (m *Manager) rollbackInitialStart(
	ctx context.Context,
	rollbackIDs []ID,
	projects []*Project,
	started []startedProject,
) error {
	m.cancelAllRetries()
	m.Wait()

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.startupRollbackTimeout)
	defer cancel()
	cleanupErr := m.stopUncommittedStartedProjects(cleanupCtx, started)
	cleanupErr = errors.Join(cleanupErr, closeProjectSlice(cleanupCtx, projects))

	m.mu.Lock()
	defer m.mu.Unlock()

	if cleanupCtx.Err() == nil {
		for _, id := range rollbackIDs {
			m.registry.Delete(id)
		}
	}
	m.running = false
	m.spawned = false
	return cleanupErr
}

type startupLimiter struct {
	startup StartupConfig
	sleep   func(context.Context, time.Duration) error
	jitter  func(time.Duration) time.Duration

	mu      sync.Mutex
	spawned bool
}

func (l *startupLimiter) wait(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	delay := spawnDelay(l.startup, l.spawned, l.jitter)
	if delay > 0 {
		if err := l.sleep(ctx, delay); err != nil {
			return err
		}
	}
	l.spawned = true
	return nil
}

func maxConcurrentStarts(startup StartupConfig, projectCount int) int {
	limit := startup.MaxConcurrentStarts
	if limit <= 0 {
		limit = defaultMaxConcurrentStarts
	}
	if projectCount > 0 && limit > projectCount {
		return projectCount
	}
	return limit
}

func closeProjectSlice(ctx context.Context, projects []*Project) error {
	var cleanupErr error
	for _, project := range projects {
		if project == nil {
			continue
		}
		cleanupErr = errors.Join(cleanupErr, project.close(ctx, false))
	}
	return cleanupErr
}

func closePreparedProjects(ctx context.Context, prepared map[ID]*Project) error {
	var cleanupErr error
	for _, project := range prepared {
		if project == nil {
			continue
		}
		cleanupErr = errors.Join(cleanupErr, project.close(ctx, false))
	}
	return cleanupErr
}

func closeStoppedProjects(ctx context.Context, stopped []rollbackProject) error {
	var cleanupErr error
	for _, item := range stopped {
		if item.project == nil {
			continue
		}
		cleanupErr = errors.Join(cleanupErr, item.project.close(ctx, false))
	}
	return cleanupErr
}

func preparedProjectByID(prepared map[ID]*Project, id ID) (*Project, error) {
	project := prepared[id]
	if project == nil {
		return nil, ErrMissingProject
	}
	return project, nil
}

func (m *Manager) waitBeforeSpawn(ctx context.Context) error {
	delay := spawnDelay(m.cfg.Startup, m.spawned, m.jitter)
	if delay <= 0 {
		return nil
	}
	return m.sleep(ctx, delay)
}

func (m *Manager) handleInitialCreationFailureLocked(
	ctx context.Context,
	cfg globalconfig.Project,
	err error,
) bool {
	isConnectorFailure := errors.Is(err, ErrConnectorCreation)
	if !isConnectorFailure && !errors.Is(err, ErrProjectDefinition) {
		return false
	}

	id := normalizeProjectID(ID(cfg.ID))
	if id == "" {
		return false
	}
	cfg.ID = string(id)
	if !isConnectorFailure || !connector.IsRetryable(err) {
		runtimeErr := RuntimeError{Message: err.Error(), At: m.nowUTC(), Terminal: true}
		if pendingErr := m.registry.SetPending(cfg, runtimeErr); pendingErr != nil {
			return false
		}
		m.logProjectStartupFailure(id, err, time.Time{}, true)
		return true
	}

	attempt := 1
	delay := m.retryDelay(attempt)
	runtimeErr := m.retryRuntimeError(err, delay)
	if pendingErr := m.registry.SetPending(cfg, runtimeErr); pendingErr != nil {
		return false
	}
	retryCtx, cancel := context.WithCancel(ctx)
	run := &retryRun{cancel: cancel, done: make(chan struct{})}
	m.cancelRetryLocked(id)
	m.retryRuns[id] = run
	m.retryWG.Add(1)
	m.logProjectStartupFailure(id, err, runtimeErr.NextRetryAt, false)
	go m.retryPendingProject(retryCtx, run, cfg, attempt, delay)
	return true
}

func (m *Manager) handleProjectStartupFailure(ctx context.Context, trackedProject *Project, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handleProjectStartupFailureLocked(ctx, trackedProject, err)
}

func (m *Manager) handleProjectStartupFailureLocked(ctx context.Context, trackedProject *Project, err error) {
	if trackedProject == nil || err == nil {
		return
	}
	id := trackedProject.ID()
	if !connector.IsRetryable(err) {
		trackedProject.recordRuntimeErrorState(err, m.nowUTC(), time.Time{}, true)
		m.logProjectStartupFailure(id, err, time.Time{}, true)
		return
	}

	attempt := 1
	delay := m.retryDelay(attempt)
	runtimeErr := m.retryRuntimeError(err, delay)
	trackedProject.recordRuntimeErrorState(err, runtimeErr.At, runtimeErr.NextRetryAt, false)
	retryCtx, cancel := context.WithCancel(ctx)
	run := &retryRun{cancel: cancel, done: make(chan struct{})}
	m.cancelRetryLocked(id)
	m.retryRuns[id] = run
	m.retryWG.Add(1)
	m.logProjectStartupFailure(id, err, runtimeErr.NextRetryAt, false)
	go m.retryProject(retryCtx, run, trackedProject, attempt, delay)
}

func (m *Manager) retryPendingProject(
	ctx context.Context,
	run *retryRun,
	cfg globalconfig.Project,
	attempt int,
	delay time.Duration,
) {
	id := normalizeProjectID(ID(cfg.ID))
	defer m.finishRetry(id, run)
	var trackedProject *Project

	for {
		if err := m.retrySleep(ctx, delay); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}

		if trackedProject == nil {
			_, candidate, err := m.createProjectLocked(cfg)
			if err != nil {
				if !errors.Is(err, ErrConnectorCreation) || !connector.IsRetryable(err) {
					m.recordPendingFailure(cfg, err, time.Time{}, true)
					m.logProjectStartupFailure(id, err, time.Time{}, true)
					return
				}
				attempt++
				delay = m.retryDelay(attempt)
				runtimeErr := m.retryRuntimeError(err, delay)
				m.recordPendingFailure(cfg, err, runtimeErr.NextRetryAt, false)
				m.logProjectStartupFailure(id, err, runtimeErr.NextRetryAt, false)
				continue
			}
			if !m.activateRetriedProject(ctx, cfg, candidate) {
				return
			}
			trackedProject = candidate
			if trackedProject.Paused() {
				return
			}
		}

		err := trackedProject.start(ctx, startOptions{provision: true, publishEvents: true})
		if err == nil {
			m.markProjectRetryRecovered(id)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if !connector.IsRetryable(err) {
			trackedProject.recordRuntimeErrorState(err, m.nowUTC(), time.Time{}, true)
			m.logProjectStartupFailure(id, err, time.Time{}, true)
			return
		}
		attempt++
		delay = m.retryDelay(attempt)
		runtimeErr := m.retryRuntimeError(err, delay)
		trackedProject.recordRuntimeErrorState(err, runtimeErr.At, runtimeErr.NextRetryAt, false)
		m.logProjectStartupFailure(id, err, runtimeErr.NextRetryAt, false)
	}
}

func (m *Manager) retryProject(
	ctx context.Context,
	run *retryRun,
	trackedProject *Project,
	attempt int,
	delay time.Duration,
) {
	id := trackedProject.ID()
	defer m.finishRetry(id, run)

	for {
		if err := m.retrySleep(ctx, delay); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}

		err := trackedProject.start(ctx, startOptions{provision: true, publishEvents: true})
		if err == nil {
			m.markProjectRetryRecovered(id)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if !connector.IsRetryable(err) {
			trackedProject.recordRuntimeErrorState(err, m.nowUTC(), time.Time{}, true)
			m.logProjectStartupFailure(id, err, time.Time{}, true)
			return
		}
		attempt++
		delay = m.retryDelay(attempt)
		runtimeErr := m.retryRuntimeError(err, delay)
		trackedProject.recordRuntimeErrorState(err, runtimeErr.At, runtimeErr.NextRetryAt, false)
		m.logProjectStartupFailure(id, err, runtimeErr.NextRetryAt, false)
	}
}

func (m *Manager) activateRetriedProject(ctx context.Context, cfg globalconfig.Project, trackedProject *Project) bool {
	id := normalizeProjectID(ID(cfg.ID))
	m.mu.Lock()
	current := m.running && ctx.Err() == nil && m.projectConfigCurrentLocked(cfg)
	if current {
		_, current = m.registry.Get(id)
		current = !current
	}
	if current {
		current = m.registry.Set(trackedProject) == nil
	}
	m.mu.Unlock()
	if current {
		return true
	}
	if err := trackedProject.close(context.WithoutCancel(ctx), false); err != nil {
		m.logger.Warn("close unused retried project failed", "project_id", id, "error", err)
	}
	return false
}

func (m *Manager) projectConfigCurrentLocked(cfg globalconfig.Project) bool {
	id := normalizeProjectID(ID(cfg.ID))
	for _, current := range m.cfg.Projects {
		if normalizeProjectID(ID(current.ID)) == id {
			return sameProjectConfig(current, cfg)
		}
	}
	return false
}

func (m *Manager) recordPendingFailure(
	cfg globalconfig.Project,
	err error,
	nextRetryAt time.Time,
	terminal bool,
) {
	runtimeErr := RuntimeError{
		Message:     err.Error(),
		At:          m.nowUTC(),
		NextRetryAt: nextRetryAt,
		Terminal:    terminal,
	}
	if pendingErr := m.registry.SetPending(cfg, runtimeErr); pendingErr != nil {
		m.logger.Warn("record pending project startup failed", "project_id", cfg.ID, "error", pendingErr)
	}
}

func (m *Manager) retryRuntimeError(err error, delay time.Duration) RuntimeError {
	at := m.nowUTC()
	return RuntimeError{Message: err.Error(), At: at, NextRetryAt: at.Add(delay)}
}

func (m *Manager) retryDelay(attempt int) time.Duration {
	delay := m.retry.InitialBackoff
	for current := 1; current < attempt && delay < m.retry.MaxBackoff; current++ {
		if delay > m.retry.MaxBackoff/2 {
			delay = m.retry.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay >= m.retry.MaxBackoff || m.retry.Jitter <= 0 {
		return min(delay, m.retry.MaxBackoff)
	}
	jitter := m.retryJitter(m.retry.Jitter)
	if jitter < 0 {
		jitter = 0
	}
	return min(delay+jitter, m.retry.MaxBackoff)
}

func (m *Manager) nowUTC() time.Time {
	return m.now().UTC()
}

func (m *Manager) markProjectRetryRecovered(id ID) {
	m.mu.Lock()
	m.spawned = true
	m.mu.Unlock()
	m.logger.Info("project startup recovered", "project_id", id)
}

func (m *Manager) finishRetry(id ID, run *retryRun) {
	close(run.done)
	m.mu.Lock()
	if m.retryRuns[id] == run {
		delete(m.retryRuns, id)
	}
	m.mu.Unlock()
	m.retryWG.Done()
}

func (m *Manager) cancelRetryLocked(id ID) *retryRun {
	run := m.retryRuns[normalizeProjectID(id)]
	if run != nil {
		run.cancel()
	}
	return run
}

func (m *Manager) cancelAndWaitRetryLocked(ctx context.Context, id ID) error {
	run := m.cancelRetryLocked(id)
	if run == nil {
		return nil
	}
	m.mu.Unlock()
	var err error
	select {
	case <-run.done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	m.mu.Lock()
	return err
}

func (m *Manager) cancelAllRetries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, run := range m.retryRuns {
		run.cancel()
	}
}

func (m *Manager) logProjectStartupFailure(id ID, err error, nextRetryAt time.Time, retryStopped bool) {
	logger := m.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn(
		"project startup failed",
		"project_id", id,
		"error", err,
		"next_retry_at", nextRetryAt,
		"retry_stopped", retryStopped,
	)
}

func retainAddedProjectStartFailure(project *Project, err error) bool {
	if project == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return project.RuntimeError().Message != ""
}

func spawnDelay(startup StartupConfig, spawned bool, jitter func(time.Duration) time.Duration) time.Duration {
	if !spawned {
		return 0
	}
	delay := time.Duration(0)
	if startup.MaxSpawnPerSecond > 0 {
		delay += time.Second / time.Duration(startup.MaxSpawnPerSecond)
	}
	if startup.Jitter > 0 && jitter != nil {
		delay += jitter(startup.Jitter)
	}
	return delay
}

func startupInt(values map[string]any, key string) int {
	value, ok := values[key]
	if !ok {
		return 0
	}
	number, ok := value.(int)
	if !ok || number <= 0 {
		return 0
	}
	return number
}

func normalizeManagerProjectConfig(cfg globalconfig.Project) globalconfig.Project {
	cfg.ID = string(normalizeProjectID(ID(cfg.ID)))
	cfg.Identity.Normalize()
	cfg.GlobalKnowledge.Normalize()
	if cfg.ActiveHours != nil {
		activeHours := cfg.ActiveHours.Normalize()
		cfg.ActiveHours = &activeHours
	}
	if cfg.GlobalActiveHours != nil {
		activeHours := cfg.GlobalActiveHours.Normalize()
		cfg.GlobalActiveHours = &activeHours
	}
	cfg.ActiveHoursOverrideUntil = strings.TrimSpace(cfg.ActiveHoursOverrideUntil)
	cfg.Knowledge.Normalize()
	cfg.Intake.Normalize()
	return cfg
}

func normalizeManagerProjectConfigWithIdentity(
	cfg globalconfig.Project,
	identity globalconfig.Identity,
) globalconfig.Project {
	cfg = normalizeManagerProjectConfig(cfg)
	identity.Normalize()
	if identity.Configured() {
		cfg.Identity = identity
	}
	return cfg
}

func normalizeManagerConfig(cfg ManagerConfig) ManagerConfig {
	cfg.Identity.Normalize()
	cfg.Projects = append([]globalconfig.Project(nil), cfg.Projects...)
	for i := range cfg.Projects {
		cfg.Projects[i] = normalizeManagerProjectConfigWithIdentity(cfg.Projects[i], cfg.Identity)
	}
	return cfg
}

func normalizeConnectorRetryConfig(cfg ConnectorRetryConfig) ConnectorRetryConfig {
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultRetryInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultRetryMaxBackoff
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = cfg.InitialBackoff
	}
	if cfg.Jitter <= 0 {
		cfg.Jitter = defaultRetryJitter
	}
	return cfg
}

func sameProjectConfig(left globalconfig.Project, right globalconfig.Project) bool {
	return reflect.DeepEqual(normalizeManagerProjectConfig(left), normalizeManagerProjectConfig(right))
}

func sameProjectConfigExceptLiveFields(left globalconfig.Project, right globalconfig.Project) bool {
	left = normalizeManagerProjectConfig(left)
	right = normalizeManagerProjectConfig(right)
	left.ActiveHours = right.ActiveHours
	left.GlobalActiveHours = right.GlobalActiveHours
	left.ActiveHoursOverrideUntil = right.ActiveHoursOverrideUntil
	left.GlobalRateWindowPacing = right.GlobalRateWindowPacing
	left.GlobalMemory.PressureSomeAvg60Threshold = right.GlobalMemory.PressureSomeAvg60Threshold
	left.GlobalMemory.PollIntervalMS = right.GlobalMemory.PollIntervalMS
	left.GlobalIO = right.GlobalIO
	left.GlobalCPU = right.GlobalCPU
	left.GlobalAgents = right.GlobalAgents
	left.GlobalBudget = right.GlobalBudget
	return reflect.DeepEqual(left, right)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}

	value, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0
	}
	return time.Duration(value.Int64())
}
