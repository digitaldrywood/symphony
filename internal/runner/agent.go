package runner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/forgeavailability"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/lessons"
	"github.com/digitaldrywood/detent/internal/notes"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/skills"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/workspace"
)

const (
	defaultAfterRunTimeout         = time.Minute
	liveDiffStatsInterval          = 2 * time.Second
	recentActivityLimit            = 5
	defaultProjectID               = "default"
	orphanResumePrompt             = "The Detent process restarted while this session was running. Continue from your last state and complete the assigned work."
	implausibleUsageRuntimeSeconds = int64(1800)
	implausibleUsageOutputTokens   = int64(1000)
)

var (
	ErrMissingWorkspace    = errors.New("runner workspace backend is required")
	ErrMissingAgentBackend = errors.New("runner agent backend is required")
)

type SessionStore interface {
	StartSession(context.Context, store.SessionStart) (int64, error)
	FinishSession(context.Context, int64, store.SessionFinish) error
	RecordUsageEvent(context.Context, store.UsageEvent) (int64, error)
}

type sessionIdentityStore interface {
	UpdateSessionIdentity(context.Context, int64, agentidentity.Identity) error
}

type sessionProviderStore interface {
	UpdateSessionProviderIdentity(context.Context, int64, store.SessionProviderIdentity) error
}

type sessionWorkerProcessStore interface {
	UpdateSessionWorkerProcess(context.Context, int64, store.WorkerProcessRegistration) error
}

type sessionWorkerProcessReaper interface {
	ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error)
	MarkSessionWorkerProcessReaped(context.Context, int64, store.WorkerProcessReap) error
}

type workerProcessReapFunc func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error)
type workspaceProcessReapFunc func(context.Context, string, time.Duration) (int, error)

type sessionResumeStore interface {
	UpdateSessionResumeState(context.Context, int64, store.SessionResumeState) error
}

type BudgetChecker interface {
	CheckDispatch(context.Context, budget.DispatchRequest) (budget.Decision, error)
}

type DispatchEstimator interface {
	EstimateDispatch(context.Context, string) (budget.TokenEstimate, error)
}

type BudgetGuardBuilder func(config.Budget) (BudgetChecker, DispatchEstimator, error)

type workflowPhaseStore interface {
	RecordWorkflowPhaseEvent(context.Context, store.WorkflowPhaseEvent) (int64, error)
}

type AgentBackendFactory interface {
	NewAgentBackend(config.AgentBackend) (AgentBackend, error)
}

type AgentBackendFactoryFunc func(config.AgentBackend) (AgentBackend, error)

func (f AgentBackendFactoryFunc) NewAgentBackend(cfg config.AgentBackend) (AgentBackend, error) {
	return f(cfg)
}

type durationLimitContextFactory func(context.Context, time.Duration, error) (context.Context, context.CancelFunc)

type Dependencies struct {
	ProjectID              string
	Workflow               config.Workflow
	Workspace              workspace.Backend
	AgentBackend           AgentBackend
	AgentBackends          map[string]AgentBackend
	AgentBackendFactory    AgentBackendFactory
	Store                  SessionStore
	Pricing                budget.PricingTable
	BudgetChecker          BudgetChecker
	DispatchEstimator      DispatchEstimator
	BudgetGuardBuilder     BudgetGuardBuilder
	Now                    func() time.Time
	Logger                 *slog.Logger
	SecurityAuditRoot      string
	AfterRunTimeout        time.Duration
	MaxAgentRSSBytes       uint64
	RSSPollInterval        time.Duration
	ProcessRSS             func(context.Context, procgroup.Identity) (uint64, error)
	WorkerReapGrace        time.Duration
	ReapWorkerProcess      workerProcessReapFunc
	ReapWorkspaceProcesses workspaceProcessReapFunc
	sessionLimit           durationLimitContextFactory
	turnLimit              durationLimitContextFactory
	progressTicker         sessionProgressTickerFactory
	lookupEnv              func(string) string
}

type Runner struct {
	mu                        sync.RWMutex
	projectID                 string
	workflow                  config.Workflow
	workspace                 workspace.Backend
	agentRuntime              agentRuntime
	agentBackendFactory       AgentBackendFactory
	store                     SessionStore
	pricing                   budget.PricingTable
	budgetChecker             BudgetChecker
	dispatchEstimator         DispatchEstimator
	budgetGuardBuilder        BudgetGuardBuilder
	enforcedBudget            config.Budget
	enforcedBudgetKnown       bool
	now                       func() time.Time
	logger                    *slog.Logger
	securityAuditRoot         string
	afterRunTimeout           time.Duration
	maxAgentRSSBytes          uint64
	rssPollInterval           time.Duration
	processRSS                func(context.Context, procgroup.Identity) (uint64, error)
	workerReapGrace           time.Duration
	reapWorkerProcess         workerProcessReapFunc
	reapWorkspaceProcesses    workspaceProcessReapFunc
	cleanupWorkerArtifacts    func(string, string) error
	waitWorkerArtifactCleanup func(context.Context, time.Duration) error
	sessionLimit              durationLimitContextFactory
	turnLimit                 durationLimitContextFactory
	progressTicker            sessionProgressTickerFactory
	admissionLeaks            admissionWorkspaceLeakTracker
	lookupEnv                 func(string) string
}

func NewRunner(deps Dependencies) (*Runner, error) {
	if deps.Workspace == nil {
		return nil, ErrMissingWorkspace
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if strings.TrimSpace(deps.SecurityAuditRoot) == "" {
		deps.SecurityAuditRoot = filepath.Join(os.TempDir(), "detent-security-audits")
	}
	if deps.AfterRunTimeout <= 0 {
		deps.AfterRunTimeout = defaultAfterRunTimeout
	}
	if deps.RSSPollInterval <= 0 {
		deps.RSSPollInterval = time.Second
	}
	if deps.ProcessRSS == nil {
		deps.ProcessRSS = procgroup.RSS
	}
	if deps.WorkerReapGrace <= 0 {
		deps.WorkerReapGrace = procgroup.DefaultTerminationGrace
	}
	if deps.ReapWorkerProcess == nil {
		deps.ReapWorkerProcess = procgroup.Terminate
	}
	if deps.ReapWorkspaceProcesses == nil {
		deps.ReapWorkspaceProcesses = workspace.ReapProcesses
	}
	if deps.sessionLimit == nil {
		deps.sessionLimit = withAgentDurationLimit
	}
	if deps.turnLimit == nil {
		deps.turnLimit = withAgentDurationLimit
	}
	if deps.progressTicker == nil {
		deps.progressTicker = newSessionProgressTicker
	}
	if deps.lookupEnv == nil {
		deps.lookupEnv = os.Getenv
	}
	projectID := strings.TrimSpace(deps.ProjectID)
	if projectID == "" {
		projectID = defaultProjectID
	}
	agentBackends := cloneAgentBackends(deps.AgentBackends)
	if deps.AgentBackend != nil {
		if agentBackends == nil {
			agentBackends = map[string]AgentBackend{}
		}
		agentBackends[config.DefaultAgentBackendID] = deps.AgentBackend
	}
	runtime, err := newAgentRuntime(deps.Workflow, agentBackends, deps.AgentBackendFactory)
	if err != nil {
		return nil, err
	}

	budgetChecker := deps.BudgetChecker
	dispatchEstimator := deps.DispatchEstimator
	if deps.BudgetGuardBuilder != nil {
		budgetChecker, dispatchEstimator, err = deps.BudgetGuardBuilder(deps.Workflow.Config.Budget)
		if err != nil {
			return nil, err
		}
	}
	enforcedBudget, enforcedBudgetKnown := enforcedBudgetConfig(deps.Workflow.Config.Budget, budgetChecker)

	return &Runner{
		projectID:                 projectID,
		workflow:                  deps.Workflow,
		workspace:                 deps.Workspace,
		agentRuntime:              runtime,
		agentBackendFactory:       deps.AgentBackendFactory,
		store:                     deps.Store,
		pricing:                   deps.Pricing,
		budgetChecker:             budgetChecker,
		dispatchEstimator:         dispatchEstimator,
		budgetGuardBuilder:        deps.BudgetGuardBuilder,
		enforcedBudget:            enforcedBudget,
		enforcedBudgetKnown:       enforcedBudgetKnown,
		now:                       deps.Now,
		logger:                    deps.Logger,
		securityAuditRoot:         filepath.Clean(deps.SecurityAuditRoot),
		afterRunTimeout:           deps.AfterRunTimeout,
		maxAgentRSSBytes:          deps.MaxAgentRSSBytes,
		rssPollInterval:           deps.RSSPollInterval,
		processRSS:                deps.ProcessRSS,
		workerReapGrace:           deps.WorkerReapGrace,
		reapWorkerProcess:         deps.ReapWorkerProcess,
		reapWorkspaceProcesses:    deps.ReapWorkspaceProcesses,
		cleanupWorkerArtifacts:    workspace.CleanupOwnedPath,
		waitWorkerArtifactCleanup: waitForPathRemovalRetry,
		sessionLimit:              deps.sessionLimit,
		turnLimit:                 deps.turnLimit,
		progressTicker:            deps.progressTicker,
		lookupEnv:                 deps.lookupEnv,
	}, nil
}

func (r *Runner) UpdateWorkflow(workflow config.Workflow) {
	if err := r.UpdateWorkflowChecked(workflow); err != nil {
		r.logger.Warn("reload agent configuration rejected; retaining last known good workflow", "error", err)
	}
}

func (r *Runner) UpdateWorkflowChecked(workflow config.Workflow) error {
	apply, err := r.PrepareWorkflowUpdate(workflow)
	if err != nil {
		return err
	}
	apply()
	return nil
}

func (r *Runner) PrepareWorkflowUpdate(workflow config.Workflow) (func(), error) {
	r.mu.RLock()
	currentBackends := cloneAgentBackends(r.agentRuntime.backends)
	factory := r.agentBackendFactory
	budgetGuardBuilder := r.budgetGuardBuilder
	currentBudgetChecker := r.budgetChecker
	currentDispatchEstimator := r.dispatchEstimator
	r.mu.RUnlock()

	runtime, err := newAgentRuntime(workflow, currentBackends, factory)
	if err != nil {
		return nil, err
	}
	budgetChecker := currentBudgetChecker
	dispatchEstimator := currentDispatchEstimator
	var budgetErr error
	if budgetGuardBuilder != nil {
		budgetChecker, dispatchEstimator, budgetErr = budgetGuardBuilder(workflow.Config.Budget)
	}

	if budgetErr != nil {
		return nil, fmt.Errorf("reload budget dispatch guards: %w", budgetErr)
	}
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.workflow = workflow
		r.budgetChecker = budgetChecker
		r.dispatchEstimator = dispatchEstimator
		r.enforcedBudget, r.enforcedBudgetKnown = enforcedBudgetConfig(workflow.Config.Budget, budgetChecker)
		if budgetGuardBuilder != nil {
			if r.enforcedBudgetKnown {
				r.logger.Info(
					"budget dispatch guards reloaded",
					"enabled", r.enforcedBudget.Enabled,
					"per_day_max_usd", r.enforcedBudget.PerDayMaxUSD,
					"per_issue_max_usd", r.enforcedBudget.PerIssueMaxUSD,
				)
			} else {
				r.logger.Warn("budget dispatch guards reloaded without enforced config reporting")
			}
		}
		r.agentRuntime = runtime
	}, nil
}

func (r *Runner) EnforcedBudget() (config.Budget, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.enforcedBudget, r.enforcedBudgetKnown
}

func (r *Runner) DailyBudgetStatus(ctx context.Context, now time.Time) (DailyBudgetStatus, bool, error) {
	_, _, checker, _ := r.runtimeSnapshot()
	provider, ok := checker.(interface {
		DailyStatus(context.Context, time.Time) (budget.DailyStatus, error)
	})
	if !ok {
		return DailyBudgetStatus{}, false, nil
	}

	status, err := provider.DailyStatus(ctx, now)
	if err != nil {
		return DailyBudgetStatus{}, true, err
	}
	return DailyBudgetStatus{
		Active:          status.Active,
		CurrentSpendUSD: status.CurrentSpendUSD,
		MaxUSD:          status.MaxUSD,
	}, true, nil
}

func (r *Runner) IssueBudgetStatus(ctx context.Context, issue connector.Issue) (IssueBudgetStatus, bool, error) {
	_, _, checker, _ := r.runtimeSnapshot()
	provider, ok := checker.(interface {
		IssueStatus(context.Context, store.IssueIdentity) (budget.IssueStatus, error)
	})
	if !ok {
		enforced, known := r.EnforcedBudget()
		if known && (!enforced.Enabled || enforced.PerIssueMaxUSD <= 0) {
			return IssueBudgetStatus{}, true, nil
		}
		return IssueBudgetStatus{}, false, nil
	}

	status, err := provider.IssueStatus(ctx, store.IssueIdentity{
		ProjectID:  r.projectID,
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		IssueURL:   issue.URL,
	})
	if err != nil {
		return IssueBudgetStatus{}, true, err
	}
	return IssueBudgetStatus{
		Active:          status.Active,
		CurrentSpendUSD: status.CurrentSpendUSD,
		MaxUSD:          status.MaxUSD,
	}, true, nil
}

func enforcedBudgetConfig(requested config.Budget, checker BudgetChecker) (config.Budget, bool) {
	if checker == nil {
		if requested.Enabled {
			return config.Budget{}, false
		}
		return requested, true
	}

	reporter, ok := checker.(interface {
		EnforcedConfig() budget.Config
	})
	if !ok {
		return config.Budget{}, false
	}
	enforced := reporter.EnforcedConfig()
	return config.Budget{
		BillingMode:            enforced.BillingMode,
		Enabled:                enforced.Enabled,
		PerDayMaxUSD:           enforced.PerDayMaxUSD,
		PerIssueMaxUSD:         enforced.PerIssueMaxUSD,
		RefusalCooldownSeconds: int(enforced.RefusalCooldown / time.Second),
		PricingPath:            enforced.PricingPath,
	}, true
}

type agentRuntime struct {
	backends       map[string]AgentBackend
	backendConfigs map[string]config.AgentBackend
	router         *Router
}

func newAgentRuntime(
	workflow config.Workflow,
	staticBackends map[string]AgentBackend,
	factory AgentBackendFactory,
) (agentRuntime, error) {
	if problems := workflow.Config.EffectiveModelSelection().Validate(); len(problems) > 0 {
		return agentRuntime{}, errors.New(strings.Join(problems, "; "))
	}
	backendConfigs := workflow.Config.AgentBackendConfigs()
	backends := make(map[string]AgentBackend, len(backendConfigs))
	configsByID := make(map[string]config.AgentBackend, len(backendConfigs))
	for _, backendConfig := range backendConfigs {
		if strings.TrimSpace(backendConfig.ID) == "" {
			continue
		}
		configsByID[backendConfig.ID] = backendConfig
		if factory != nil {
			backend, err := factory.NewAgentBackend(backendConfig)
			if err != nil {
				return agentRuntime{}, fmt.Errorf("create agent backend %s: %w", backendConfig.ID, err)
			}
			backends[backendConfig.ID] = backend
			continue
		}
		backend, ok := staticBackends[backendConfig.ID]
		if !ok {
			return agentRuntime{}, fmt.Errorf("%w: %s", ErrMissingAgentBackend, backendConfig.ID)
		}
		backends[backendConfig.ID] = backend
	}
	if len(backends) == 0 {
		return agentRuntime{}, ErrMissingAgentBackend
	}

	router, err := NewRouter(routesFromConfig(workflow.Config.AgentRouteConfigs()))
	if err != nil {
		return agentRuntime{}, err
	}
	for _, route := range router.routes {
		if _, ok := backends[route.BackendID]; !ok {
			return agentRuntime{}, fmt.Errorf("%w: %s", ErrMissingAgentBackend, route.BackendID)
		}
	}

	return agentRuntime{
		backends:       backends,
		backendConfigs: configsByID,
		router:         router,
	}, nil
}

func routesFromConfig(routes []config.AgentRoute) []Route {
	out := make([]Route, 0, len(routes))
	for _, route := range routes {
		out = append(out, Route{
			Name:       route.Name,
			Role:       route.Role,
			BackendID:  route.Backend,
			Model:      route.Model,
			ModelField: route.ModelField,
			Default:    route.Default,
			Selector:   route.Selector,
		})
	}
	return out
}

func (r agentRuntime) selectBackendForRole(issue connector.Issue, ctx selector.Context, role string) (RouteSelection, AgentBackend, config.AgentBackend, error) {
	selection, err := r.router.RouteForRole(issue, ctx, role)
	if err != nil {
		return RouteSelection{}, nil, config.AgentBackend{}, err
	}
	backend, ok := r.backends[selection.BackendID]
	if !ok {
		return RouteSelection{}, nil, config.AgentBackend{}, fmt.Errorf("%w: %s", ErrMissingAgentBackend, selection.BackendID)
	}
	backendConfig, ok := r.backendConfigs[selection.BackendID]
	if !ok {
		return RouteSelection{}, nil, config.AgentBackend{}, fmt.Errorf("agent backend config not found: %s", selection.BackendID)
	}
	return selection, backend, backendConfig, nil
}

func (r agentRuntime) defaultModelForRole(role string) string {
	if r.router == nil {
		return ""
	}
	role = normalizeRole(role)
	index, ok := r.router.defaultIndexes[role]
	if !ok && role != RoleCode {
		index, ok = r.router.defaultIndexes[RoleCode]
	}
	if !ok {
		return ""
	}
	return strings.TrimSpace(r.router.routes[index].Model)
}

func (r agentRuntime) effectiveRunRole(role string) string {
	role = normalizeRole(role)
	if role == RoleCode || r.router.HasRole(role) {
		return role
	}
	return RoleCode
}

func configuredRuntimeIdentity(selection RouteSelection, backend config.AgentBackend, role string, requestedModel string, observedAt time.Time) agentidentity.Identity {
	provider := strings.TrimSpace(backend.Provider)
	effort := ""
	serviceTier := ""
	switch backend.Kind {
	case config.AgentBackendCodex:
		options := backend.CodexOptions()
		if provider == "" {
			provider = options.ModelProvider
		}
		serviceTier = options.ServiceTier
	case config.AgentBackendClaudeCode:
		effort = backend.ClaudeCodeOptions().Effort
	}
	return agentidentity.Configured(
		selection.BackendID,
		backend.Kind,
		selection.RouteName,
		role,
		requestedModel,
		provider,
		effort,
		serviceTier,
		observedAt,
	)
}

func agentTurnIdentityOptions(backend config.AgentBackend) (modelProvider string, serviceTier string, effort string) {
	switch backend.Kind {
	case config.AgentBackendCodex:
		options := backend.CodexOptions()
		return options.ModelProvider, options.ServiceTier, ""
	case config.AgentBackendClaudeCode:
		return "", "", backend.ClaudeCodeOptions().Effort
	default:
		return "", "", ""
	}
}

func cloneAgentBackends(in map[string]AgentBackend) map[string]AgentBackend {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]AgentBackend, len(in))
	maps.Copy(out, in)
	return out
}

func selectorContext(ctx selector.Context, workflow config.Workflow) selector.Context {
	if strings.TrimSpace(ctx.Persona) == "" {
		ctx.Persona = workflow.Config.Tracker.Assignee
	}
	return ctx
}

func normalizeRunMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", RunModeImplement:
		return RunModeImplement
	case RunModePlan:
		return RunModePlan
	case RunModeMerge:
		return RunModeMerge
	case RunModeRoutine:
		return RunModeRoutine
	default:
		return RunModeImplement
	}
}

func agentTurnDeliverable(cfg config.Config, issue connector.Issue, mode string) (string, string) {
	switch normalizeRunMode(mode) {
	case RunModeImplement, RunModeMerge:
	default:
		return "", ""
	}
	repository := strings.TrimSpace(issue.PRRepository)
	if repository == "" {
		repository = strings.TrimSpace(cfg.Tracker.Repository)
	}
	return strings.TrimSpace(cfg.Deliverable.Kind), repository
}

func agentTurnIssueRepository(cfg config.Config, issue connector.Issue) string {
	identifier := strings.TrimSpace(issue.Identifier)
	if githubIssueNumber(identifier) != "" {
		repository := strings.TrimSpace(identifier[:strings.LastIndex(identifier, "#")])
		if repository != "" {
			return repository
		}
	}
	return strings.TrimSpace(cfg.Tracker.Repository)
}

func runRole(mode string, issue connector.Issue) string {
	switch normalizeRunMode(mode) {
	case RunModePlan:
		return RolePlan
	case RunModeRoutine:
		return RoleRoutine
	}
	switch strings.ToLower(strings.TrimSpace(issue.State)) {
	case RoleRework:
		return RoleRework
	case RoleMerge, "merging":
		return RoleMerge
	default:
		return RoleCode
	}
}

func extraWritableRootsForWorkspace(ctx context.Context, workspaceKind string, workspacePath string, logger *slog.Logger) []string {
	if workspaceKind == config.WorkspaceFilesystem {
		return nil
	}
	roots, err := workspace.GitMetadataWritableRoots(ctx, workspacePath)
	if err != nil {
		if logger != nil {
			logger.Warn("workspace git metadata writable roots unavailable", slog.String("workspace_path", workspacePath), slog.Any("error", err))
		}
		return nil
	}
	return roots
}

func (r *Runner) prepareMergeFastPath(
	ctx context.Context,
	req RunRequest,
	info workspace.Info,
	issue workspace.Issue,
) (RunResult, workspace.MergePrepareResult, bool, error) {
	preparer, ok := r.workspace.(workspace.MergePreparer)
	if !ok {
		return RunResult{}, workspace.MergePrepareResult{}, false, nil
	}
	opts := workspace.MergePrepareOptions{}
	if req.Issue.PullRequest != nil {
		opts.TargetBranch = strings.TrimSpace(req.Issue.PullRequest.BaseRef)
	}
	precheck, err := preparer.PrepareMerge(ctx, info, issue, opts)
	if err != nil {
		return RunResult{}, precheck, false, fmt.Errorf("merge fast-path precheck: %w", err)
	}
	switch precheck.Status {
	case workspace.MergePrepareStatusClean:
		r.logWorkerEvent(req.Issue, "worker_merge_fast_path_clean",
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			"workspace_path", info.Path,
			"workspace_branch", info.Branch,
		)
		return RunResult{
			FinalState:            FinalStateCompleted,
			Output:                RunOutputMergeFastPathClean,
			DiffStats:             diffStatsFromWorkspace(precheck.DiffStat),
			PullRequestHeadPushed: precheck.HeadChanged,
			ForgeWriteCompleted:   true,
		}, precheck, true, nil
	case workspace.MergePrepareStatusConflict, workspace.MergePrepareStatusDirty:
		r.logWorkerEvent(req.Issue, "worker_merge_fast_path_fallback",
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			"workspace_path", info.Path,
			"workspace_branch", info.Branch,
			"status", string(precheck.Status),
		)
		return RunResult{}, precheck, false, nil
	default:
		return RunResult{}, precheck, false, fmt.Errorf("merge fast-path precheck returned unknown status %q", precheck.Status)
	}
}

func mergePrecheckFromWorkspace(precheck workspace.MergePrepareResult) MergePrecheck {
	return MergePrecheck{
		Status:      string(precheck.Status),
		Message:     precheck.Message,
		DiffStats:   diffStatsFromWorkspace(precheck.DiffStat),
		HeadChanged: precheck.HeadChanged,
		HeadSHA:     precheck.HeadSHA,
	}
}

func mergeFallbackResult(output string) string {
	trimmed := strings.TrimSpace(output)
	resolvedMarker := "DETENT_MERGE_FALLBACK: resolved"
	reworkMarker := "DETENT_MERGE_FALLBACK: rework"
	if strings.Count(trimmed, resolvedMarker) == 1 &&
		strings.Count(trimmed, reworkMarker) == 0 &&
		strings.HasSuffix(trimmed, resolvedMarker) {
		return RunOutputMergeFallbackResolved
	}
	return RunOutputMergeFallbackRework
}

const mergeFallbackValidationTimeout = time.Hour

func (r *Runner) verifyMergeFallback(
	ctx context.Context,
	backend workspace.Backend,
	info workspace.Info,
	issue workspace.Issue,
	opts workspace.MergePrepareOptions,
	result RunResult,
) (RunResult, error) {
	result.MergeFallbackFindings = strings.TrimSpace(result.Output)
	result.Output = mergeFallbackResult(result.Output)
	if result.Output == RunOutputMergeFallbackRework {
		return result, nil
	}
	preparer, ok := backend.(workspace.MergePreparer)
	if !ok {
		result.Output = RunOutputMergeFallbackRework
		return result, nil
	}
	validationCtx, cancel := context.WithTimeout(ctx, mergeFallbackValidationTimeout)
	defer cancel()
	precheck, err := preparer.PrepareMerge(validationCtx, info, issue, opts)
	if err != nil {
		if ctx.Err() != nil {
			return result, fmt.Errorf("verify merge fallback: %w", errors.Join(ctx.Err(), err))
		}
		if !errors.Is(err, workspace.ErrMergeResolutionInvalid) && !errors.Is(validationCtx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("verify merge fallback: %w", err)
		}
		result.Output = RunOutputMergeFallbackRework
		result.MergeFallbackFindings += "\nDeterministic validation failed: " + err.Error()
		return result, nil
	}
	converted := mergePrecheckFromWorkspace(precheck)
	result.MergePrecheck = &converted
	if precheck.Status != workspace.MergePrepareStatusClean || strings.TrimSpace(precheck.HeadSHA) == "" {
		result.MergeFallbackFindings += fmt.Sprintf("\nDeterministic verification returned status %q, head %q: %s", precheck.Status, precheck.HeadSHA, precheck.Message)
		result.Output = RunOutputMergeFallbackRework
		return result, nil
	}
	result.PullRequestHeadPushed = result.PullRequestHeadPushed || precheck.HeadChanged
	result.ForgeWriteCompleted = true
	return result, nil
}

func cloneMergePrecheck(precheck *MergePrecheck) *MergePrecheck {
	if precheck == nil {
		return nil
	}
	cloned := *precheck
	return &cloned
}

func mergeFastPathCheckedHead(issue connector.Issue) bool {
	if issue.PullRequest == nil {
		return false
	}
	pullRequest := issue.PullRequest
	if !strings.EqualFold(strings.TrimSpace(pullRequest.State), "open") || pullRequest.Draft {
		return false
	}
	mergeable := strings.ToLower(strings.TrimSpace(pullRequest.MergeableState))
	if mergeable != "clean" {
		return false
	}
	if pullRequest.HydrationUnavailableReason != "" || pullRequest.HydrationDegradedReason != "" || len(pullRequest.RequiredCheckFailures) > 0 || pullRequest.MergeQueueEntry != nil {
		return false
	}
	if strings.TrimSpace(pullRequest.HeadSHA) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(pullRequest.CIStatus)) {
	case "success", "green", "pass", "passed":
		return true
	default:
		return false
	}
}

func (r *Runner) publishWorkspaceCreateStarted(req RunRequest) error {
	if req.OnUsageUpdate == nil {
		return nil
	}
	at := r.now().UTC()
	event := telemetry.ActivityEvent{
		At:      at,
		Event:   "workspace_create_started",
		Message: "workspace creation started",
	}
	return req.OnUsageUpdate(UsageUpdate{
		LastEventAt:       at,
		LastEvent:         event.Event,
		RecentEvents:      []telemetry.ActivityEvent{event},
		WorkerGitHubActor: req.workerGitHubActor,
	})
}

type agentTurnExecution struct {
	turnResult  AgentTurnResult
	result      RunResult
	err         error
	cleanupErr  error
	turnStarted bool
	turnCount   int
}

func runAgentBackendTurn(
	ctx context.Context,
	backend AgentBackend,
	request AgentTurnRequest,
	onUpdate AgentUpdateHandler,
) (AgentTurnResult, error, error) {
	var contextualUpdateHandler agentContextUpdateHandler
	if onUpdate != nil {
		contextualUpdateHandler = func(_ context.Context, update AgentUpdate) error {
			return onUpdate(update)
		}
	}
	return runAgentBackendTurnWithTools(ctx, backend, request, nil, nil, contextualUpdateHandler)
}

type agentContextUpdateHandler func(context.Context, AgentUpdate) error

func runAgentBackendTurnWithTools(
	ctx context.Context,
	backend AgentBackend,
	request AgentTurnRequest,
	tools []AgentTool,
	toolHandler AgentToolHandler,
	onUpdate agentContextUpdateHandler,
) (AgentTurnResult, error, error) {
	return runAgentBackendTurnWithToolsUsingLimit(
		ctx,
		backend,
		request,
		tools,
		toolHandler,
		onUpdate,
		withAgentDurationLimit,
	)
}

func runAgentBackendTurnWithToolsUsingLimit(
	ctx context.Context,
	backend AgentBackend,
	request AgentTurnRequest,
	tools []AgentTool,
	toolHandler AgentToolHandler,
	onUpdate agentContextUpdateHandler,
	turnLimit durationLimitContextFactory,
) (AgentTurnResult, error, error) {
	result, cleanupScratch, runErr := runAgentBackendTurnWithToolsUsingLimitPreservingScratch(
		ctx,
		backend,
		request,
		tools,
		toolHandler,
		onUpdate,
		turnLimit,
	)
	return result, runErr, runWorkerScratchCleanup(cleanupScratch)
}

func runAgentBackendTurnWithToolsUsingLimitPreservingScratch(
	ctx context.Context,
	backend AgentBackend,
	request AgentTurnRequest,
	tools []AgentTool,
	toolHandler AgentToolHandler,
	onUpdate agentContextUpdateHandler,
	turnLimit durationLimitContextFactory,
) (AgentTurnResult, func() error, error) {
	if turnLimit == nil {
		turnLimit = withAgentDurationLimit
	}
	run := func(ctx context.Context, request AgentTurnRequest) (AgentTurnResult, error) {
		limitCtx, cancelLimit := turnLimit(
			ctx,
			request.MaxDuration,
			ErrTurnDurationExceeded,
		)
		defer cancelLimit()
		turnCtx, cancelTurn := context.WithCancelCause(limitCtx)
		defer cancelTurn(nil)

		var boundedUpdateHandler AgentUpdateHandler
		var updateMu sync.Mutex
		var memoryGovernor sync.Once
		var memoryGovernorWG sync.WaitGroup
		if onUpdate != nil || request.MaxRSSBytes > 0 {
			boundedUpdateHandler = func(update AgentUpdate) error {
				updateMu.Lock()
				defer updateMu.Unlock()
				if onUpdate != nil {
					if err := onUpdate(turnCtx, update); err != nil {
						return err
					}
				}
				if update.Type != AgentUpdateProcessStarted || request.MaxRSSBytes == 0 || update.WorkerProcess.PID <= 0 {
					return nil
				}
				if err := observeAgentRSS(turnCtx, request, update.WorkerProcess, onUpdate); err != nil {
					cancelTurn(err)
					return err
				}
				memoryGovernor.Do(func() {
					memoryGovernorWG.Add(1)
					go func() {
						defer memoryGovernorWG.Done()
						interval := request.RSSPollInterval
						if interval <= 0 {
							interval = time.Second
						}
						ticker := time.NewTicker(interval)
						defer ticker.Stop()
						for {
							select {
							case <-turnCtx.Done():
								return
							case <-ticker.C:
								updateMu.Lock()
								err := observeAgentRSS(turnCtx, request, update.WorkerProcess, onUpdate)
								updateMu.Unlock()
								if err != nil {
									cancelTurn(err)
									return
								}
							}
						}
					}()
				})
				return nil
			}
		}
		governedCtx, stopGovernor, err := startWorkerGitHubGovernor(turnCtx, request.workerGitHub, boundedUpdateHandler)
		if err != nil {
			return AgentTurnResult{}, err
		}
		var result AgentTurnResult
		var runErr error
		if len(tools) > 0 {
			if toolBackend, ok := backend.(AgentToolBackend); ok {
				result, runErr = toolBackend.RunTurnWithTools(governedCtx, request, tools, toolHandler, boundedUpdateHandler)
			} else {
				result, runErr = backend.RunTurn(governedCtx, request, boundedUpdateHandler)
			}
		} else {
			result, runErr = backend.RunTurn(governedCtx, request, boundedUpdateHandler)
		}
		cancelTurn(nil)
		memoryGovernorWG.Wait()
		runErr = errors.Join(runErr, stopGovernor())
		if cause := context.Cause(turnCtx); errors.Is(cause, ErrSessionMemoryCeilingExceeded) {
			return result, errors.Join(cause, runErr)
		}
		if cause := context.Cause(turnCtx); errors.Is(cause, ErrTurnDurationExceeded) {
			return result, errors.Join(cause, runErr)
		}
		return result, runErr
	}
	workspacePath := strings.TrimSpace(request.Workspace)
	if workspacePath == "" {
		result, err := run(ctx, request)
		return result, nil, err
	}

	tempDir, err := workspace.PrepareWorkerScratch(ctx, workspacePath)
	if workspace.IsMissingWorkspaceError(err) {
		result, runErr := run(ctx, request)
		return result, nil, runErr
	}
	if err != nil {
		return AgentTurnResult{}, nil, fmt.Errorf("prepare worker scratch: %w", err)
	}
	request.TempDir = tempDir
	cleanupScratch := func() error {
		if cleanupErr := workspace.CleanupWorkerScratch(workspacePath); cleanupErr != nil {
			return fmt.Errorf("cleanup worker scratch: %w", cleanupErr)
		}
		return nil
	}
	if err := configureWorkerGitHubEnvironment(&request); err != nil {
		return AgentTurnResult{}, cleanupScratch, fmt.Errorf("prepare worker github environment: %w", err)
	}
	if err := configureWorkerCache(&request); err != nil {
		return AgentTurnResult{}, cleanupScratch, fmt.Errorf("prepare worker cache: %w", err)
	}
	result, runErr := run(ctx, request)
	return result, cleanupScratch, runErr
}

func runWorkerScratchCleanup(cleanup func() error) error {
	if cleanup == nil {
		return nil
	}
	return cleanup()
}

func cleanupWorkerScratchAfterProcessReap(cleanup func() error, reapErr error) error {
	if reapErr != nil {
		return nil
	}
	return runWorkerScratchCleanup(cleanup)
}

func observeAgentRSS(
	ctx context.Context,
	request AgentTurnRequest,
	identity procgroup.Identity,
	onUpdate agentContextUpdateHandler,
) error {
	if request.MaxRSSBytes == 0 || request.processRSS == nil {
		return nil
	}
	rssBytes, err := request.processRSS(ctx, identity)
	if err != nil {
		return nil
	}
	if onUpdate != nil {
		if err := onUpdate(ctx, AgentUpdate{
			Type:            AgentUpdateResourceUsage,
			WorkerProcess:   identity,
			RSSBytes:        rssBytes,
			RSSCeilingBytes: request.MaxRSSBytes,
		}); err != nil {
			return err
		}
	}
	if rssBytes <= request.MaxRSSBytes {
		return nil
	}
	return &SessionMemoryCeilingError{RSSBytes: rssBytes, CeilingBytes: request.MaxRSSBytes}
}

func withAgentDurationLimit(ctx context.Context, duration time.Duration, limit error) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if duration <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, duration, &agentDurationLimitError{
		limit:    limit,
		duration: duration,
	})
}

func durationLimitError(err error) bool {
	return errors.Is(err, ErrTurnDurationExceeded) ||
		errors.Is(err, ErrSessionDurationExceeded) ||
		errors.Is(err, ErrMergeFallbackBudgetExceeded) ||
		errors.Is(err, ErrSessionTurnLimitExceeded) ||
		errors.Is(err, ErrSessionNoProgress)
}

func (r *Runner) runAgentTurn(
	ctx context.Context,
	backend AgentBackend,
	turnRequest AgentTurnRequest,
	runRequest RunRequest,
	info workspace.Info,
	workspaceIssue workspace.Issue,
	agentConfig config.Agent,
	ciTriggerLabel string,
	targetRefObserver func(context.Context) *DeliverableTargetRefEvidence,
	runStartedAt time.Time,
	detentSessionID int64,
	initialIdentity agentidentity.Identity,
	budgetProjection *dispatchBudgetProjection,
	budgetCostOffsetUSD float64,
	sessionModel string,
	backendKind string,
) agentTurnExecution {
	if runRequest.Execution != nil {
		if err := runRequest.Execution.Validate(ctx); err != nil {
			return agentTurnExecution{err: err}
		}
	}
	result := RunResult{
		FinalState:       FinalStateCompleted,
		RuntimeIdentity:  initialIdentity.Normalize(),
		budgetProjection: budgetProjection,
	}
	progress := newAgentRunProgress(
		runtimeoutput.Policy{MaxBytes: agentConfig.OutputTruncation.MaxBytes},
		ciTriggerLabel,
		strings.TrimSpace(runRequest.Issue.PRRepository),
		ciTriggerPullRequestNumber(runRequest.Issue),
		runRequest.deliverableRecoveryBranch,
		runRequest.sessionTurnOffset,
	)
	usage := newSessionTokenUsage(!agentResumeEmpty(turnRequest.Resume))
	if !result.RuntimeIdentity.IsZero() {
		eventAt := r.now()
		progress.apply(AgentUpdate{Type: AgentUpdateRuntimeIdentity, RuntimeIdentity: result.RuntimeIdentity}, eventAt)
		if err := r.publishRunUpdate(ctx, runRequest, info, workspaceIssue, progress, result, eventAt, runStartedAt, detentSessionID); err != nil {
			return agentTurnExecution{result: result, err: err}
		}
	}
	turnStarted := false
	workerProcessObserved := false
	turnResult, cleanupScratch, turnErr := runAgentBackendTurnWithToolsUsingLimitPreservingScratch(ctx, backend, turnRequest, runRequest.AgentTools, runRequest.AgentToolHandler, func(updateCtx context.Context, update AgentUpdate) error {
		eventAt := r.now()
		if update.Type == AgentUpdateTokenUsage {
			update.Tokens = usage.normalize(update.Tokens)
		}
		if !update.RuntimeIdentity.IsZero() {
			update.RuntimeIdentity = update.RuntimeIdentity.ObserveAt(eventAt)
		}
		if !update.AuxiliaryTurn && (update.Type == AgentUpdateTurnStarted || strings.TrimSpace(update.TurnID) != "") {
			turnStarted = true
		}
		if update.Type == AgentUpdateProcessStarted && update.WorkerProcess.PID > 0 {
			workerProcessObserved = true
		}
		r.logAgentUpdate(runRequest, detentSessionID, update)
		if artifacts, ok := runRequest.Execution.(ArtifactExecution); ok && update.Delta != "" {
			if err := artifacts.ArtifactLog(updateCtx, update.Delta); err != nil {
				r.logger.Warn("artifact log upload deferred", "issue_id", runRequest.Issue.ID)
			}
		}
		if err := r.persistSessionWorkerProcess(updateCtx, detentSessionID, update, info.Path, filepath.Join(info.Path, ".detent", "tmp")); err != nil {
			return err
		}
		if err := r.persistSessionProviderIdentity(updateCtx, detentSessionID, update); err != nil {
			return err
		}
		if err := publishAgentActivity(runRequest, detentSessionID, update, eventAt); err != nil {
			return err
		}
		previousIdentity := result.RuntimeIdentity
		applyAgentUpdate(&result, update)
		if !previousIdentity.MateriallyEqual(result.RuntimeIdentity) {
			if err := r.persistSessionIdentity(updateCtx, detentSessionID, result.RuntimeIdentity); err != nil {
				return err
			}
			if result.RuntimeIdentity.HasRuntimeValues() {
				r.logRuntimeIdentity(runRequest, detentSessionID, update, previousIdentity, result.RuntimeIdentity)
			}
		}
		progress.apply(update, eventAt)
		if targetRefObserver != nil && update.Type == AgentUpdateToolCompleted && failedAgentToolStatus(update.Status) {
			if deliverableErr := progress.deliverableFailure(update.ItemID, "push"); deliverableErr != nil {
				deliverableErr.TargetRef = cloneDeliverableTargetRefEvidence(targetRefObserver(updateCtx))
			}
		}
		if err := r.publishRunUpdate(updateCtx, runRequest, info, workspaceIssue, progress, result, eventAt, runStartedAt, detentSessionID); err != nil {
			return err
		}
		if err := r.enforceSessionTokenCeiling(agentConfig, runRequest.Issue, info.Path, update, eventAt); err != nil {
			return err
		}
		observedModel := effectiveModel(result.RuntimeIdentity.ResolvedModel.Value, result.Model, sessionModel)
		if err := r.enforceSessionBudgetProjection(budgetProjection, budgetCostOffsetUSD, observedModel, backendKind, update); err != nil {
			return err
		}
		if err := runRequest.sessionBrake.observe(updateCtx, progress.turnCount(), result.Tokens.TotalTokens); err != nil {
			return err
		}
		return nil
	}, r.turnLimit)
	processReapErr := r.reapSessionWorkerProcess(
		ctx,
		detentSessionID,
		runRequest.Issue,
		workerProcessReapReason(ctx, turnErr),
	)
	workspaceReapErr := r.reapWorkspaceProcessesAfterTurn(
		ctx,
		info.Path,
		detentSessionID,
		runRequest.WorkAttemptID,
		runRequest.Issue,
		turnErr,
		workerProcessObserved,
	)
	workerReapErr := errors.Join(processReapErr, workspaceReapErr)
	scratchCleanupErr := cleanupWorkerScratchAfterProcessReap(cleanupScratch, workerReapErr)
	if workerReapErr != nil {
		turnErr = errors.Join(turnErr, fmt.Errorf("%w: %w", ErrWorkerProcessReap, workerReapErr))
	}
	if cause := context.Cause(ctx); cooperativeStopError(cause) || durationLimitError(cause) {
		if workerReapErr != nil {
			turnErr = errors.Join(cause, fmt.Errorf("%w: %w", ErrWorkerProcessReap, workerReapErr))
		} else {
			turnErr = cause
		}
	}
	var memoryErr *SessionMemoryCeilingError
	if errors.As(turnErr, &memoryErr) {
		if brake := runRequest.sessionBrake.memoryCeiling(memoryErr, r.now()); brake != nil {
			turnErr = brake
		}
	}
	result.Output = progress.outputText()
	result.SkillDraftProposed = skillDraftProposed(result.Output)
	result.PullRequestUpdated = progress.pullRequestUpdated()
	result.PullRequestHeadPushed = progress.pullRequestHeadPushed()
	result.CITriggerLabelReapplied = progress.ciTriggerLabelReapplied()
	result.ForgeWriteCompleted = progress.forgeWriteCompleted()
	if turnErr == nil {
		if deliverableErr := progress.deliverableError(); deliverableErr != nil {
			turnErr = deliverableErr
			result.FinalState = FinalStateFailed
		} else if credentialErr := workerCredentialBlockerError(progress.finalMessage()); credentialErr != nil {
			turnErr = credentialErr
			result.FinalState = FinalStateFailed
		}
	}
	result.DeliverableCommands = deliverableCommandEvidenceFromError(turnErr)
	cleanupErr := agentTurnCleanupError(turnErr, turnResult)
	if cleanupErr != nil {
		turnErr = nil
	}
	cleanupErr = errors.Join(cleanupErr, scratchCleanupErr)
	if turnErr != nil {
		result.FinalState = finalStateForTurnError(turnErr)
	}
	result.TurnStarted = turnStarted
	return agentTurnExecution{
		turnResult:  turnResult,
		result:      result,
		err:         turnErr,
		cleanupErr:  cleanupErr,
		turnStarted: turnStarted,
		turnCount:   progress.turnCount(),
	}
}

func (r *Runner) agentResumeState(
	ctx context.Context,
	cfg config.Agent,
	req RunRequest,
	model string,
	backendID string,
	backendKind string,
	agentRole string,
) store.AgentResumeState {
	lookup, ok := automaticAgentResumeLookup(r.projectID, cfg, req, model, backendID, backendKind, agentRole)
	if !ok {
		return store.AgentResumeState{}
	}
	resumeStore, ok := r.store.(store.AgentResumeStore)
	if !ok {
		return store.AgentResumeState{}
	}
	state, err := resumeStore.LatestCompletedAgentResumeState(ctx, lookup)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			r.logger.Warn(
				"agent resume state lookup failed",
				slog.String("issue_id", req.Issue.ID),
				slog.String("issue_identifier", req.Issue.Identifier),
				slog.String("model", lookup.RequestedModel),
				slog.String("backend_id", lookup.AgentBackendID),
				slog.String("backend_kind", lookup.AgentBackendKind),
				slog.String("agent_role", lookup.AgentRole),
				slog.Any("error", err),
			)
		}
		return store.AgentResumeState{}
	}
	if err := r.checkResumePolicy(ctx, req, state); err != nil {
		r.logger.Warn("automatic thread resume rejected", "issue_id", req.Issue.ID, "error", err)
		return store.AgentResumeState{}
	}
	r.logWorkerEvent(req.Issue, "worker_thread_resume_selected",
		telemetry.WorkAttemptIDKey, req.WorkAttemptID,
		"resumed_from_session_id", state.DetentSessionID,
		"pr_number", lookup.PRNumber,
		"pr_head_sha", lookup.PRHeadSHA,
		"pr_base_sha", lookup.PRBaseSHA,
	)
	return state
}

func automaticAgentResumeLookup(
	projectID string,
	cfg config.Agent,
	req RunRequest,
	model string,
	backendID string,
	backendKind string,
	agentRole string,
) (store.AgentResumeLookup, bool) {
	if !cfg.ExperimentalThreadResume || normalizeRunMode(req.Mode) != RunModeImplement {
		return store.AgentResumeLookup{}, false
	}
	reworkState := strings.TrimSpace(cfg.AutoPromote.ReworkState)
	if reworkState == "" {
		reworkState = "Rework"
	}
	dispatchSourceState := strings.TrimSpace(req.DispatchSourceState)
	if dispatchSourceState == "" {
		dispatchSourceState = strings.TrimSpace(req.Issue.State)
	}
	if !strings.EqualFold(dispatchSourceState, reworkState) || req.Issue.PullRequest == nil {
		return store.AgentResumeLookup{}, false
	}
	prNumber := int64(req.Issue.PullRequest.Number)
	if req.Issue.PRNumber != nil && *req.Issue.PRNumber > 0 {
		prNumber = int64(*req.Issue.PRNumber)
	}
	lookup := store.AgentResumeLookup{
		ProjectID:        strings.TrimSpace(projectID),
		IssueID:          strings.TrimSpace(req.Issue.ID),
		Identifier:       strings.TrimSpace(req.Issue.Identifier),
		IssueURL:         strings.TrimSpace(req.Issue.URL),
		PRNumber:         prNumber,
		PRHeadSHA:        strings.TrimSpace(req.Issue.PullRequest.HeadSHA),
		PRBaseSHA:        strings.TrimSpace(req.Issue.PullRequest.BaseSHA),
		RequestedModel:   strings.TrimSpace(model),
		AgentBackendID:   strings.TrimSpace(backendID),
		AgentBackendKind: strings.TrimSpace(backendKind),
		AgentRole:        strings.TrimSpace(agentRole),
	}
	if lookup.ProjectID == "" || lookup.PRNumber <= 0 || lookup.PRHeadSHA == "" || lookup.PRBaseSHA == "" ||
		lookup.RequestedModel == "" || lookup.AgentBackendID == "" || lookup.AgentBackendKind == "" || lookup.AgentRole == "" ||
		(lookup.IssueID == "" && lookup.Identifier == "" && lookup.IssueURL == "") {
		return store.AgentResumeLookup{}, false
	}
	return lookup, true
}

func (r *Runner) runRequestResumeState(
	ctx context.Context,
	cfg config.Agent,
	req RunRequest,
	model string,
	backendID string,
	backendKind string,
	agentRole string,
) (store.AgentResumeState, error) {
	switch req.RetryMode {
	case RetryModeFresh:
		return store.AgentResumeState{}, nil
	case RetryModeResume:
		if agentResumeStateEmpty(req.ResumeState) {
			return store.AgentResumeState{}, errors.New("resume retry requested without resume state")
		}
		return req.ResumeState, nil
	default:
		return r.agentResumeState(ctx, cfg, req, model, backendID, backendKind, agentRole), nil
	}
}

func agentResumeFromState(state store.AgentResumeState) AgentResume {
	return AgentResume{
		ThreadID:  strings.TrimSpace(state.ProviderThreadID),
		SessionID: strings.TrimSpace(state.ProviderSessionID),
	}
}

func agentResumeStateEmpty(state store.AgentResumeState) bool {
	return strings.TrimSpace(state.ProviderThreadID) == "" && strings.TrimSpace(state.ProviderSessionID) == ""
}

func agentResumeStateMatches(state store.AgentResumeState, model string, backendID string, backendKind string, agentRole string) bool {
	return strings.EqualFold(strings.TrimSpace(state.RequestedModel), strings.TrimSpace(model)) &&
		strings.EqualFold(strings.TrimSpace(state.AgentBackendID), strings.TrimSpace(backendID)) &&
		strings.EqualFold(strings.TrimSpace(state.AgentBackendKind), strings.TrimSpace(backendKind)) &&
		strings.EqualFold(strings.TrimSpace(state.AgentRole), strings.TrimSpace(agentRole))
}

func agentResumeEmpty(resume AgentResume) bool {
	return strings.TrimSpace(resume.ThreadID) == "" && strings.TrimSpace(resume.SessionID) == ""
}

func verifyAgentResume(ctx context.Context, backend AgentBackend, resume AgentResume) error {
	verifier, ok := backend.(AgentResumeVerifier)
	if !ok {
		return ErrAgentResumeUnsupported
	}
	return verifier.VerifyResume(ctx, resume)
}

func (r *Runner) run(ctx context.Context, req RunRequest) (RunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	workflow, agentRuntime, budgetChecker, dispatchEstimator := r.runtimeSnapshot()
	if req.Policy.ID != "" || workflow.Config.Policy.ID != "" {
		if err := req.Policy.Match(workflow.Config.Policy); err != nil {
			return RunResult{}, err
		}
	}
	if err := r.checkResumePolicy(ctx, req, req.ResumeState); err != nil {
		return RunResult{}, err
	}
	forgeHost := forgeavailability.HostFromEndpoint(workflow.Config.Tracker.Endpoint)
	mode := normalizeRunMode(req.Mode)
	if mode == RunModeMerge && mergeFastPathCheckedHead(req.Issue) && req.MergeRefreshHeadSHA != req.Issue.PullRequest.HeadSHA {
		r.logWorkerEvent(req.Issue, "worker_merge_fast_path_checked_head",
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			"workspace_branch", strings.TrimSpace(req.Issue.BranchName),
			"head_sha", req.Issue.PullRequest.HeadSHA,
			"base_sha", req.Issue.PullRequest.BaseSHA,
			"reason", "preserve_green_head_for_merge_api",
			"prior_validation_invalidated", false,
		)
		return RunResult{
			FinalState: FinalStateCompleted,
			Output:     RunOutputMergeFastPathCheckedHead,
		}, nil
	}
	workerGitHub, err := r.workerGitHubPolicy(ctx, workflow.Config, req.Issue.Identifier)
	if err != nil {
		r.logWorkerGitHubPolicyError(req.Issue, err, telemetry.WorkAttemptIDKey, req.WorkAttemptID)
		return RunResult{}, err
	}
	req.workerGitHubActor = workerGitHub.Principal

	runWorkspace := r.workspace
	if req.Admission != nil {
		runWorkspace = &admissionWorkspace{logger: r.logger, leaks: &r.admissionLeaks}
	}
	workspaceIssue := workspaceIssue(r.projectID, req.Issue)
	r.logWorkerEvent(req.Issue, "worker_workspace_create_started", telemetry.WorkAttemptIDKey, req.WorkAttemptID)
	if err := r.publishWorkspaceCreateStarted(req); err != nil {
		return RunResult{}, err
	}
	info, err := runWorkspace.Create(ctx, workspaceIssue)
	if err != nil {
		if heldErr, held := workspaceBranchHeldError(err, req.Issue); held {
			return RunResult{}, heldErr
		}
		classifiedErr := classifyForgeOperationError(fmt.Errorf("create workspace: %w", err), "git fetch", forgeHost)
		return RunResult{}, fmt.Errorf("%w: %w", ErrWorkspacePreparation, classifiedErr)
	}
	r.logWorkerEvent(req.Issue, "worker_workspace_created",
		telemetry.WorkAttemptIDKey, req.WorkAttemptID,
		"workspace_path", info.Path,
		"workspace_branch", info.Branch,
	)

	if req.Execution != nil {
		if err := req.Execution.Validate(ctx); err != nil {
			return RunResult{}, err
		}
	}
	if err := runWorkspace.BeforeRun(ctx, info, workspaceIssue); err != nil {
		return RunResult{}, fmt.Errorf("workspace before_run: %w", err)
	}
	r.logWorkerEvent(req.Issue, "worker_before_run_finished",
		telemetry.WorkAttemptIDKey, req.WorkAttemptID,
		"workspace_path", info.Path,
	)

	afterRunPending := true
	defer func() {
		if afterRunPending {
			if err := r.afterExecution(ctx, req, runWorkspace, info, workspaceIssue); err != nil {
				r.logger.Warn("native execution epilogue deferred", "issue_id", req.Issue.ID, "error", err)
			}
		}
	}()

	mergePrecheck := MergePrecheck{}
	mergeFallback := false
	if mode == RunModeMerge {
		if req.MergePrecheck != nil {
			mergePrecheck = *req.MergePrecheck
		} else {
			precheckResult, precheck, handled, err := r.prepareMergeFastPath(ctx, req, info, workspaceIssue)
			if err != nil {
				operation := "git fetch"
				if strings.Contains(strings.ToLower(err.Error()), "git push") {
					operation = "git push"
				}
				return RunResult{}, classifyForgeOperationError(err, operation, forgeHost)
			}
			mergePrecheck = mergePrecheckFromWorkspace(precheck)
			if handled {
				afterRunPending = false
				if err := r.afterExecution(ctx, req, runWorkspace, info, workspaceIssue); err != nil {
					return precheckResult, err
				}
				r.logWorkerEvent(req.Issue, "worker_after_run_finished",
					telemetry.WorkAttemptIDKey, req.WorkAttemptID,
					"workspace_path", info.Path,
				)
				return precheckResult, nil
			}
		}
		mergeFallback = true
	}

	attempt := req.Attempt
	var availableSkills []skills.Skill
	if req.Admission == nil {
		availableSkills, err = r.availableSkills(workflow, info.Path)
		if err != nil {
			return RunResult{}, err
		}
	}
	var recoveryState *workspace.RecoveryState
	initialDeliverableState := workspaceDeliverableStateObservation{}
	initialArtifactEvidence := workspaceArtifactEvidenceObservation{}
	var targetRefObserver func(context.Context) *DeliverableTargetRefEvidence
	if mode == RunModeImplement {
		recoveryState = r.workspaceRecoveryState(runWorkspace, ctx, info, workspaceIssue, "initial")
		initialDeliverableState = r.observeWorkspaceDeliverableState(runWorkspace, ctx, info, workspaceIssue, "initial")
		if workflow.Config.Deliverable.Kind == config.DeliverableArtifact {
			initialArtifactEvidence = r.observeWorkspaceArtifactEvidence(runWorkspace, ctx, info, workspaceIssue, "initial")
		}
		r.publishDispatchLoopStart(req, recoveryState)
		targetRefObserver = func(observerCtx context.Context) *DeliverableTargetRefEvidence {
			postCommand := r.observeWorkspaceDeliverableState(runWorkspace, observerCtx, info, workspaceIssue, "post_command")
			return deliverableTargetRefEvidence(info.Branch, initialDeliverableState, postCommand)
		}
	}
	promptOptions := PromptOptions{
		Attempt:              &attempt,
		WorkAttemptID:        req.WorkAttemptID,
		Generation:           req.Generation,
		PlanOnly:             mode == RunModePlan,
		MergeFallback:        mergeFallback,
		MergePrecheckStatus:  mergePrecheck.Status,
		MergePrecheckMessage: mergePrecheck.Message,
		WorkspacePath:        info.Path,
		Branch:               info.Branch,
		DispatchSourceState:  req.DispatchSourceState,
		DispatchTargetState:  req.DispatchTargetState,
		AvailableSkills:      availableSkills,
		PriorAttempt:         req.PriorAttempt,
		RecoveryState:        recoveryState,
	}
	var prompt string
	if mode == RunModeRoutine && req.Admission != nil {
		prompt, err = BuildAdmissionPrompt(req.Issue, *req.Admission, promptOptions)
	} else if mode == RunModeRoutine && req.Routine != nil {
		prompt, err = BuildRoutinePrompt(workflow, req.Issue, *req.Routine, promptOptions)
	} else {
		prompt, err = BuildPrompt(workflow, req.Issue, promptOptions)
	}
	if err != nil {
		return RunResult{}, fmt.Errorf("build prompt: %w", err)
	}
	if req.ForgeRetry != nil && !strings.Contains(strings.ToLower(req.ForgeRetry.Operation), "git fetch") {
		prompt = forgeRetryPrompt(*req.ForgeRetry, req.Issue)
		if strings.TrimSpace(req.ForgeRetry.Branch) != "" && req.Issue.PullRequest == nil {
			req.deliverableRecoveryBranch = strings.TrimSpace(req.ForgeRetry.Branch)
		}
	}
	role := runRole(req.Mode, req.Issue)
	recoveryPrompt, err := nativeRecoveryPrompt(req.Execution)
	if err != nil {
		return RunResult{}, err
	}
	prompt += recoveryPrompt
	routeRole := agentRuntime.effectiveRunRole(role)
	selection, backend, backendConfig, err := agentRuntime.selectRequestBackend(req, selectorContext(req.SelectorContext, workflow), routeRole)
	if err != nil {
		return RunResult{}, err
	}

	startedAt := req.StartedAt
	if startedAt.IsZero() {
		startedAt = r.now().UTC()
	}
	runStartedAt := r.now()
	modelProvider, serviceTier, configuredEffort := agentTurnIdentityOptions(backendConfig)
	baseModel := effectiveModel("", selection.Model, agentRuntime.defaultModelForRole(role))
	resolvedOverride := resolveRequestAgentSelection(ctx, req, info.Path, baseModel, role, workflow.Config, backendConfig, backend)
	selectedModel := resolvedOverride.Model
	effort := configuredEffort
	if resolvedOverride.Effort != "" {
		effort = resolvedOverride.Effort
	}
	if len(resolvedOverride.Rejections) > 0 && req.OnOverrideRejected != nil {
		if err := req.OnOverrideRejected(resolvedOverride.Rejections); err != nil {
			r.logger.Warn("report detent-agent override rejection failed", "issue_id", req.Issue.ID, "identifier", req.Issue.Identifier, "error", err)
		}
	}
	if resolvedOverride.Err != nil {
		return RunResult{}, resolvedOverride.Err
	}
	sessionModel := effectiveModel("", selectedModel, agentRuntime.defaultModelForRole(role))
	executionIdentity := tracker.NativeExecutionIdentity{Role: role, Backend: selection.BackendID, Model: sessionModel}
	if executionIdentity.Model == "" {
		executionIdentity.Model = "provider_default"
	}
	runtimeIdentity := configuredRuntimeIdentity(selection, backendConfig, role, sessionModel, startedAt)
	runtimeIdentity.Selection = resolvedOverride.Selection
	runtimeIdentity.Selection.BackendSource = workflow.Config.Agents.Sources["backends."+selection.BackendID]
	runtimeIdentity.Selection.RouteSource = workflow.Config.Agents.Sources["routes."+selection.RouteName]
	if effort != "" {
		runtimeIdentity.ReasoningEffort = agentidentity.NewValue(effort, agentidentity.ProvenanceConfigured)
	}
	if hasResumeIdentity(req) {
		runtimeIdentity = req.ResumeState.RuntimeIdentity.ObserveAt(startedAt)
		effort = runtimeIdentity.ReasoningEffort.Value
		modelProvider = runtimeIdentity.Provider.Value
		serviceTier = runtimeIdentity.ServiceTier.Value
	}
	var budgetProjection *dispatchBudgetProjection
	if workflow.Config.Budget.EffectiveBillingMode() != config.BillingModeSubscription {
		if admission, refused, err := r.checkDispatchBudget(ctx, budgetChecker, dispatchEstimator, req.Issue, sessionModel, startedAt); err != nil {
			return RunResult{}, err
		} else if refused {
			r.logWorkerEvent(req.Issue, "worker_budget_refused",
				telemetry.WorkAttemptIDKey, req.WorkAttemptID,
				"workspace_path", info.Path,
				"backend_id", selection.BackendID,
				"route", selection.RouteName,
				"role", role,
				"model", sessionModel,
				"code", admission.BudgetRefusal.Code,
			)
			return admission, nil
		} else {
			budgetProjection = admission.budgetProjection
		}
	}
	resumeState := store.AgentResumeState{}
	if mode != RunModeRoutine {
		resumeState, err = r.runRequestResumeState(ctx, workflow.Config.Agent, req, sessionModel, selection.BackendID, backendConfig.Kind, role)
		if err != nil {
			return RunResult{}, err
		}
	}
	resumeState, err = r.nativeResume(ctx, req, backend, recoveryState, resumeState, executionIdentity)
	if err != nil {
		return RunResult{}, err
	}
	if req.Execution != nil {
		if err := req.Execution.Start(ctx, executionIdentity); err != nil {
			return RunResult{}, err
		}
		if artifacts, ok := req.Execution.(ArtifactExecution); ok {
			if err := artifacts.PrepareArtifacts(ctx, info.Path); err != nil {
				return RunResult{}, err
			}
		}
		checkpoint := executionCheckpoint(recoveryState)
		checkpoint.WorktreeState = "unknown"
		if err := req.Execution.Checkpoint(ctx, checkpoint); err != nil {
			return RunResult{}, err
		}
	}
	orphanRecovery := resumeState.Orphaned
	orphanRecoveryOutcome := ""
	orphanRecoveryFallbackReason := ""
	if orphanRecovery {
		orphanRecoveryOutcome = store.OrphanRecoveryResumed
		var verifyErr error
		if !agentResumeStateMatches(resumeState, sessionModel, selection.BackendID, backendConfig.Kind, role) {
			verifyErr = errors.New("orphaned session runtime identity no longer matches selected backend, model, and role")
		} else {
			verifyErr = verifyAgentResume(ctx, backend, agentResumeFromState(resumeState))
		}
		if verifyErr != nil {
			orphanRecoveryFallbackReason = errorString(verifyErr)
			r.logWorkerEvent(req.Issue, "worker_orphan_resume_preflight_failed_fallback",
				telemetry.WorkAttemptIDKey, req.WorkAttemptID,
				"workspace_path", info.Path,
				"backend_id", selection.BackendID,
				"route", selection.RouteName,
				"role", role,
				"thread_id", resumeState.ProviderThreadID,
				"provider_session_id", resumeState.ProviderSessionID,
				"error", errorString(verifyErr),
			)
			resumeState = store.AgentResumeState{}
			orphanRecoveryOutcome = store.OrphanRecoveryFresh
		}
	}
	if req.AcquireModelPermit != nil {
		if err := req.AcquireModelPermit(ctx); err != nil {
			result := RunResult{
				Output:        RunOutputMergeFallbackDeferred,
				DiffStats:     mergePrecheck.DiffStats,
				MergePrecheck: cloneMergePrecheck(&mergePrecheck),
			}
			if errors.Is(err, ErrModelPermitUnavailable) {
				r.logWorkerEvent(req.Issue, "worker_merge_fallback_deferred",
					telemetry.WorkAttemptIDKey, req.WorkAttemptID,
					"workspace_path", info.Path,
					"status", mergePrecheck.Status,
				)
				return result, err
			}
			return result, fmt.Errorf("acquire model permit: %w", err)
		}
	}
	sessionID, sessionStarted, err := r.startSession(ctx, req, startedAt, runtimeIdentity, resumeState, orphanRecoveryOutcome, orphanRecoveryFallbackReason)
	if err != nil {
		return RunResult{}, err
	}
	sessionDuration := durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS)
	sessionLimitError := ErrSessionDurationExceeded
	if mergeFallback {
		sessionDuration = durationFromMillis(workflow.Config.Agent.MergeFallbackMaxDurationMS)
		if sessionDuration <= 0 {
			sessionDuration = durationFromMillis(config.DefaultMergeFallbackMaxDurationMS)
		}
		sessionLimitError = ErrMergeFallbackBudgetExceeded
	}
	sessionCtx, cancelSession := r.sessionLimit(
		ctx,
		sessionDuration,
		sessionLimitError,
	)
	defer cancelSession()
	sessionCtx, cancelSessionBrake := context.WithCancelCause(sessionCtx)
	defer cancelSessionBrake(context.Canceled)
	sessionBrake := newSessionBrakeController(
		sessionCtx,
		runStartedAt,
		sessionDuration,
		workflow.Config.Agent.MaxTurns,
		durationFromMillis(workflow.Config.Agent.NoProgressTimeoutMS),
		cancelSessionBrake,
		func(probeCtx context.Context) (sessionProgressSnapshot, error) {
			return r.sessionProgressSnapshot(probeCtx, runWorkspace, info, workspaceIssue, req.ProgressProbe)
		},
		r.now,
		r.progressTicker,
		r.logger,
		req.Issue,
	)
	defer sessionBrake.Stop()
	req.sessionBrake = sessionBrake

	commandStartedAttrs := []any{
		"workspace_path", info.Path,
		"work_attempt_id", req.WorkAttemptID,
		"detent_session_id", sessionID,
		"mode", mode,
		"github_credential_policy", workerGitHubPolicyName(workerGitHub),
		"github_credential_identity", workerGitHub.CredentialIdentity,
	}
	commandStartedAttrs = append(commandStartedAttrs, runtimeIdentityLogAttrs(runtimeIdentity)...)
	r.logWorkerEvent(req.Issue, "worker_command_started", commandStartedAttrs...)
	turnPrompt := prompt
	if req.ForgeRetry == nil && orphanRecovery && !agentResumeStateEmpty(resumeState) {
		turnPrompt = orphanResumePrompt
	}
	var extraWritableRoots []string
	if req.Admission == nil {
		extraWritableRoots = extraWritableRootsForWorkspace(sessionCtx, workflow.Config.Workspace.Kind, info.Path, r.logger)
	}
	deliverableKind, deliverableRepository := agentTurnDeliverable(workflow.Config, req.Issue, mode)
	turnRequest := AgentTurnRequest{
		Workspace:             info.Path,
		Prompt:                turnPrompt,
		ReadOnly:              mode == RunModeRoutine,
		SupplementalTools:     len(req.AgentTools) > 0,
		Model:                 selectedModel,
		ModelProvider:         modelProvider,
		ServiceTier:           serviceTier,
		ReasoningEffort:       effort,
		Resume:                agentResumeFromState(resumeState),
		MaxTurns:              workflow.Config.Agent.MaxTurns,
		MaxDuration:           durationFromMillis(workflow.Config.Agent.MaxTurnDurationMS),
		ExtraWritableRoots:    extraWritableRoots,
		DeliverableKind:       deliverableKind,
		DeliverableRepository: deliverableRepository,
		IssueRepository:       agentTurnIssueRepository(workflow.Config, req.Issue),
		MaxRSSBytes:           r.maxAgentRSSBytes,
		RSSPollInterval:       r.rssPollInterval,
		cacheStrategy:         workflow.Config.Workspace.CacheStrategy,
		projectID:             r.projectID,
		workerGitHub:          workerGitHub,
		processRSS:            r.processRSS,
	}
	if mergeFallback && (turnRequest.MaxDuration <= 0 || sessionDuration < turnRequest.MaxDuration) {
		turnRequest.MaxDuration = sessionDuration
	}
	if mode == RunModeRoutine {
		turnRequest.ToolInstructions = routineToolInstructions
		if req.Admission != nil {
			turnRequest.ToolInstructions = admissionToolInstructions
		}
	}
	execution := r.runAgentTurn(sessionCtx, backend, turnRequest, req, info, workspaceIssue, workflow.Config.Agent, workflow.Config.Gate.CITriggerLabel, targetRefObserver, runStartedAt, sessionID, runtimeIdentity, budgetProjection, 0, sessionModel, backendConfig.Kind)
	execution.err = sessionBrake.wrapTurnLimit(ctx, execution.err)
	execution.err = sessionBrake.wrapDuration(ctx, execution.err, durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS))
	execution.err = classifyAgentCapacityError(backend, selection, backendConfig, execution.result.RuntimeIdentity, execution.err, execution.result.RateLimits, runStartedAt)
	if execution.err != nil && !IsCapacityError(execution.err) && !durationLimitError(execution.err) && !errors.Is(execution.err, ErrWorkerProcessReap) && !agentResumeEmpty(turnRequest.Resume) && !execution.turnStarted {
		r.logWorkerEvent(req.Issue, "worker_resume_failed_fallback",
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			telemetry.DetentSessionIDKey, sessionID,
			"workspace_path", info.Path,
			"backend_id", selection.BackendID,
			"route", selection.RouteName,
			"role", role,
			"thread_id", turnRequest.Resume.ThreadID,
			"provider_session_id", turnRequest.Resume.SessionID,
			"error", errorString(execution.err),
		)
		turnRequest.Resume = AgentResume{}
		turnRequest.Prompt = prompt
		fallbackOutcome := ""
		if orphanRecovery {
			fallbackOutcome = store.OrphanRecoveryFresh
		}
		if updateErr := r.updateSessionResumeState(sessionCtx, sessionID, 0, fallbackOutcome, errorString(execution.err)); updateErr != nil {
			r.logger.Warn("clear agent session resume state failed", "detent_session_id", sessionID, "issue_id", req.Issue.ID, "error", updateErr)
		}
		resumeState = store.AgentResumeState{}
		if targetRefObserver != nil {
			initialDeliverableState = r.observeWorkspaceDeliverableState(runWorkspace, sessionCtx, info, workspaceIssue, "resume_fallback_initial")
		}
		execution = r.runAgentTurn(sessionCtx, backend, turnRequest, req, info, workspaceIssue, workflow.Config.Agent, workflow.Config.Gate.CITriggerLabel, targetRefObserver, runStartedAt, sessionID, runtimeIdentity, budgetProjection, 0, sessionModel, backendConfig.Kind)
		execution.err = sessionBrake.wrapTurnLimit(ctx, execution.err)
		execution.err = sessionBrake.wrapDuration(ctx, execution.err, durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS))
		execution.err = classifyAgentCapacityError(backend, selection, backendConfig, execution.result.RuntimeIdentity, execution.err, execution.result.RateLimits, runStartedAt)
	}
	if req.ForgeRetry != nil && strings.Contains(strings.ToLower(req.ForgeRetry.Operation), "git fetch") {
		execution.result.ForgeWriteCompleted = true
	}
	if req.ForgeRetry != nil && req.ForgeRetry.WorkProductPushed {
		execution.result.PullRequestHeadPushed = true
	}
	if mode == RunModeImplement {
		execution = r.reconcileFailedPushPublication(sessionCtx, runWorkspace, info, workspaceIssue, initialDeliverableState, execution, req)
	}
	turns := int64(max(execution.turnCount, 1))
	if deliverableErr, ok := recoverablePullRequestDeliverable(execution); ok {
		branch := strings.TrimSpace(info.Branch)
		if branch == "" {
			branch = strings.TrimSpace(req.Issue.BranchName)
		}
		r.logWorkerEvent(req.Issue, "worker_deliverable_recovery_started",
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			telemetry.DetentSessionIDKey, sessionID,
			"workspace_branch", branch,
			"deliverable_command", deliverableErr.Operation,
		)
		recoveryRequest := turnRequest
		recoveryRequest.Prompt = deliverableRecoveryPrompt(branch, req.Issue.PRRepository, deliverableErr)
		recoveryRequest.Resume = AgentResume{
			ThreadID:  execution.turnResult.ThreadID,
			SessionID: execution.turnResult.SessionID,
		}
		recoveryRunRequest := req
		recoveryRunRequest.deliverableRecoveryBranch = branch
		recoveryRunRequest.sessionTurnOffset = execution.turnCount
		budgetCostOffsetUSD := r.usageCostUSD(sessionModel, execution.result.Tokens.InputTokens, execution.result.Tokens.CachedInputTokens, execution.result.Tokens.OutputTokens, backendConfig.Kind)
		if targetRefObserver != nil {
			initialDeliverableState = r.observeWorkspaceDeliverableState(runWorkspace, sessionCtx, info, workspaceIssue, "deliverable_recovery_initial")
		}
		recovery := r.runAgentTurn(sessionCtx, backend, recoveryRequest, recoveryRunRequest, info, workspaceIssue, workflow.Config.Agent, workflow.Config.Gate.CITriggerLabel, targetRefObserver, runStartedAt, sessionID, execution.result.RuntimeIdentity, budgetProjection, budgetCostOffsetUSD, sessionModel, backendConfig.Kind)
		recovery.err = sessionBrake.wrapTurnLimit(ctx, recovery.err)
		recovery.err = sessionBrake.wrapDuration(ctx, recovery.err, durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS))
		recovery.err = classifyAgentCapacityError(backend, selection, backendConfig, recovery.result.RuntimeIdentity, recovery.err, recovery.result.RateLimits, runStartedAt)
		initialErr := execution.err
		execution = mergeAgentTurnExecutions(execution, recovery)
		turns = int64(max(execution.turnCount, 1))
		if recovery.err != nil {
			if recoveryFailure, exhausted := pullRequestDeliverableFailure(recovery.err); exhausted {
				execution.err = &DeliverableRecoveryError{Branch: branch, Err: errors.Join(initialErr, recovery.err)}
				execution.result.FinalState = FinalStateNeedsHumanAttention
				r.logWorkerEventLevel(slog.LevelWarn, req.Issue, "worker_deliverable_recovery_failed",
					telemetry.WorkAttemptIDKey, req.WorkAttemptID,
					telemetry.DetentSessionIDKey, sessionID,
					"workspace_branch", branch,
					"deliverable_command", recoveryFailure.Operation,
					"error", execution.err,
				)
			} else {
				execution.err = recovery.err
				execution.result.FinalState = finalStateForTurnError(recovery.err)
				r.logWorkerEventLevel(slog.LevelWarn, req.Issue, "worker_deliverable_recovery_interrupted",
					telemetry.WorkAttemptIDKey, req.WorkAttemptID,
					telemetry.DetentSessionIDKey, sessionID,
					"workspace_branch", branch,
					"deliverable_command", deliverableErr.Operation,
					"error", execution.err,
				)
			}
		} else {
			execution.err = nil
			execution.result.FinalState = FinalStateCompleted
			r.logWorkerEvent(req.Issue, "worker_deliverable_recovery_succeeded",
				telemetry.WorkAttemptIDKey, req.WorkAttemptID,
				telemetry.DetentSessionIDKey, sessionID,
				"workspace_branch", branch,
				"deliverable_command", deliverableErr.Operation,
			)
		}
	}
	sessionBrake.Stop()
	turnResult := execution.turnResult
	turnErr := execution.err
	cleanupErr := execution.cleanupErr
	result := execution.result
	result.WorkspaceBranch = strings.TrimSpace(info.Branch)
	if mergeFallback && turnErr == nil {
		targetBranch := ""
		if req.Issue.PullRequest != nil {
			targetBranch = req.Issue.PullRequest.BaseRef
		}
		cancelSession()
		expectedRemoteHead := ""
		if req.Issue.PullRequest != nil {
			expectedRemoteHead = req.Issue.PullRequest.HeadSHA
		}
		result, turnErr = r.verifyMergeFallback(ctx, runWorkspace, info, workspaceIssue, workspace.MergePrepareOptions{
			TargetBranch:       targetBranch,
			VerifyResolution:   true,
			ValidationCommand:  gate.Effective(workflow.Config.Gate).Run,
			ExpectedRemoteHead: expectedRemoteHead,
		}, result)
		if turnErr != nil {
			result.FinalState = finalStateForTurnError(turnErr)
		}
	}
	turnErr = classifyForgeDeliverableError(turnErr, forgeHost, result.PullRequestHeadPushed)
	result.budgetProjection = budgetProjection
	if brakeDiff := sessionBrake.resultDiffStats(); !diffStatsEmpty(brakeDiff) {
		result.DiffStats = brakeDiff
	}
	var deliverableState *workspace.DeliverableState
	var exhaustedDeliverable *DeliverableRecoveryError
	if mode == RunModeImplement && result.PullRequestHeadPushed && errors.As(turnErr, &exhaustedDeliverable) {
		if recoveryState := r.workspaceRecoveryState(runWorkspace, ctx, info, workspaceIssue, "deliverable_recovery"); recoveryState != nil {
			result.DiffStats = diffStatsFromWorkspace(recoveryState.DiffStat)
			applyRecoveryState(&result.DiffStats, recoveryState)
		}
		deliverableState = r.workspaceDeliverableState(runWorkspace, ctx, info, workspaceIssue)
		applyDeliverableState(&result.DiffStats, deliverableState)
	}
	commandFinishedAttrs := []any{
		"workspace_path", info.Path,
		"work_attempt_id", req.WorkAttemptID,
		"detent_session_id", sessionID,
		"provider_thread_id", turnResult.ThreadID,
		"provider_session_id", turnResult.SessionID,
		"outcome", workerRunOutcome(turnErr, result.FinalState),
		"error", errorString(turnErr),
	}
	commandFinishedAttrs = append(commandFinishedAttrs, runtimeIdentityLogAttrs(result.RuntimeIdentity)...)
	commandFinishedAttrs = append(commandFinishedAttrs, backendErrorAttrs(turnErr)...)
	if cleanupErr != nil {
		commandFinishedAttrs = append(commandFinishedAttrs,
			"cleanup_error", errorString(cleanupErr),
			"cleanup_class", "agent_turn_cleanup",
		)
	}
	if turnErr != nil || cleanupErr != nil {
		r.logWorkerEventLevel(slog.LevelWarn, req.Issue, "worker_command_finished", commandFinishedAttrs...)
	} else {
		r.logWorkerEvent(req.Issue, "worker_command_finished", commandFinishedAttrs...)
	}
	failureNoteRecorded := false
	if strings.EqualFold(strings.TrimSpace(result.FinalState), FinalStateFailed) {
		r.recordFailedRunNote(info.Path, req.Issue, result, turnErr, r.now().UTC())
		failureNoteRecorded = true
	}

	afterRunPending = false
	if err := r.afterExecution(ctx, req, runWorkspace, info, workspaceIssue); err != nil {
		return result, errors.Join(turnErr, err)
	}
	r.logWorkerEvent(req.Issue, "worker_after_run_finished",
		telemetry.WorkAttemptIDKey, req.WorkAttemptID,
		telemetry.DetentSessionIDKey, sessionID,
		"workspace_path", info.Path,
	)
	if mode == RunModeImplement && workflow.Config.Deliverable.Kind == config.DeliverableArtifact {
		finalArtifactEvidence := r.observeWorkspaceArtifactEvidence(runWorkspace, context.WithoutCancel(ctx), info, workspaceIssue, "final")
		result.ArtifactEvidence = artifactProgressEvidence(initialArtifactEvidence, finalArtifactEvidence)
	}

	if turnErr != nil {
		finishedAt := r.now().UTC()
		result.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, finishedAt)
		finishContext := ctx
		if cooperativeStopError(turnErr) {
			finishContext = context.WithoutCancel(ctx)
		}
		return result, errors.Join(
			fmt.Errorf("run agent turn: %w", turnErr),
			r.finishSession(finishContext, sessionID, sessionStarted, req.WorkAttemptID, req.Issue, startedAt, finishedAt, result, sessionModel, backendConfig.Kind, turns, turnResult, resumeState.DetentSessionID),
		)
	}

	diffStat, err := runWorkspace.DiffStat(ctx, info, workspaceIssue)
	if err != nil {
		if workspace.IsMissingWorkspaceError(err) {
			r.logger.Info(
				"workspace final diff stat skipped",
				slog.String("issue_id", workspaceIssue.ID),
				slog.String("issue_identifier", workspaceIssue.Identifier),
				slog.String("workspace_path", info.Path),
				slog.String("phase", "final"),
				slog.String("error", err.Error()),
			)
			finishedAt := r.now().UTC()
			result.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, finishedAt)
			if err := r.finishSession(ctx, sessionID, sessionStarted, req.WorkAttemptID, req.Issue, startedAt, finishedAt, result, sessionModel, backendConfig.Kind, turns, turnResult, resumeState.DetentSessionID); err != nil {
				return result, err
			}
			return result, nil
		}
		result.FinalState = FinalStateFailed
		if !failureNoteRecorded {
			r.recordFailedRunNote(info.Path, req.Issue, result, err, r.now().UTC())
		}
		finishedAt := r.now().UTC()
		result.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, finishedAt)
		return result, errors.Join(
			classifyForgeOperationError(fmt.Errorf("workspace diff stat: %w", err), "git fetch", forgeHost),
			r.finishSession(ctx, sessionID, sessionStarted, req.WorkAttemptID, req.Issue, startedAt, finishedAt, result, sessionModel, backendConfig.Kind, turns, turnResult, resumeState.DetentSessionID),
		)
	}

	result.DiffStats = diffStatsFromWorkspace(diffStat)
	if mode == RunModeImplement {
		_, result.DiffStats.RecoveryStateExpected = runWorkspace.(workspace.RecoveryStateProvider)
		if recoveryState := r.workspaceRecoveryState(runWorkspace, ctx, info, workspaceIssue, "final"); recoveryState != nil {
			applyRecoveryState(&result.DiffStats, recoveryState)
		}
	}
	applyDeliverableState(&result.DiffStats, deliverableState)
	finishedAt := r.now().UTC()
	result.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, finishedAt)
	if err := r.finishSession(ctx, sessionID, sessionStarted, req.WorkAttemptID, req.Issue, startedAt, finishedAt, result, sessionModel, backendConfig.Kind, turns, turnResult, resumeState.DetentSessionID); err != nil {
		return result, err
	}
	return result, nil
}

func recoverablePullRequestDeliverable(execution agentTurnExecution) (*DeliverableCommandError, bool) {
	if execution.err == nil || !execution.result.PullRequestHeadPushed {
		return nil, false
	}
	return pullRequestDeliverableFailure(execution.err)
}

func (r *Runner) reconcileFailedPushPublication(
	ctx context.Context,
	backend workspace.Backend,
	info workspace.Info,
	issue workspace.Issue,
	initial workspaceDeliverableStateObservation,
	execution agentTurnExecution,
	req RunRequest,
) agentTurnExecution {
	errorsFound, onlyDeliverableErrors := deliverableCommandErrors(execution.err)
	var pushErrors []*DeliverableCommandError
	for _, deliverableErr := range errorsFound {
		if deliverableErr != nil && deliverableErr.OperationClass == "push" {
			pushErrors = append(pushErrors, deliverableErr)
		}
	}
	if len(pushErrors) == 0 {
		return execution
	}

	targetRef := cloneDeliverableTargetRefEvidence(pushErrors[0].TargetRef)
	if targetRef == nil {
		targetRef = deliverableTargetRefEvidence(info.Branch, initial, workspaceDeliverableStateObservation{})
	}
	finalState := r.observeWorkspaceDeliverableState(backend, ctx, info, issue, "final_command_reconciliation")
	targetRef = finalizeDeliverableTargetRefEvidence(targetRef, finalState)
	if len(execution.result.DeliverableCommands) == 0 {
		execution.result.DeliverableCommands = deliverableCommandEvidenceFromError(execution.err)
	}
	for index := range execution.result.DeliverableCommands {
		if execution.result.DeliverableCommands[index].OperationClass == "push" {
			execution.result.DeliverableCommands[index].TargetRef = cloneDeliverableTargetRefEvidence(targetRef)
		}
	}
	if !targetRef.AdvancedToLocalHead || !onlyDeliverableErrors {
		return execution
	}

	reconciled := make([]error, 0, len(errorsFound))
	reconciliationOutcome := "published"
	for _, deliverableErr := range errorsFound {
		if deliverableErr == nil {
			continue
		}
		if deliverableErr.OperationClass != "push" {
			reconciled = append(reconciled, deliverableErr)
			continue
		}
		laterCommand := commandAfterLastGitPush(deliverableErr.Command)
		outcome := "published"
		if laterCommand != "" && (deliverableErr.ExitCode == nil || *deliverableErr.ExitCode != 0) {
			postPushErr := *deliverableErr
			postPushErr.OperationClass = "post_push"
			postPushErr.Operation = "post-push command"
			postPushErr.Arguments = summarizeDeliverableCommand(laterCommand)
			reconciled = append(reconciled, &postPushErr)
			outcome = "published_post_push_failed"
			reconciliationOutcome = outcome
		}
		for index := range execution.result.DeliverableCommands {
			evidence := &execution.result.DeliverableCommands[index]
			if evidence.ItemID != deliverableErr.ItemID || evidence.OperationClass != "push" {
				continue
			}
			evidence.Outcome = outcome
			if outcome == "published_post_push_failed" {
				evidence.OperationClass = "post_push"
				evidence.Operation = "post-push command"
			}
		}
	}

	execution.err = errors.Join(reconciled...)
	execution.result.PullRequestHeadPushed = true
	execution.result.ForgeWriteCompleted = true
	if execution.err == nil {
		execution.result.FinalState = FinalStateCompleted
	} else {
		execution.result.FinalState = finalStateForTurnError(execution.err)
	}
	attrs := []any{
		telemetry.WorkAttemptIDKey, req.WorkAttemptID,
		"workspace_branch", strings.TrimSpace(info.Branch),
		"remote", targetRef.Remote,
		"target_ref", targetRef.Ref,
		"initial_remote_head_sha", targetRef.InitialRemoteHeadSHA,
		"post_command_remote_head_sha", targetRef.PostCommandRemoteHeadSHA,
		"post_command_local_head_sha", targetRef.PostCommandLocalHeadSHA,
		"command_item_id", strings.TrimSpace(pushErrors[0].ItemID),
		"outcome", reconciliationOutcome,
	}
	if pushErrors[0].ExitCode != nil {
		attrs = append(attrs, "exit_code", *pushErrors[0].ExitCode)
	}
	r.logWorkerEventLevel(slog.LevelInfo, req.Issue, "worker_push_publication_reconciled", attrs...)
	return execution
}

func classifyForgeDeliverableError(err error, fallbackHost string, workProductPushed bool) error {
	if err == nil {
		return nil
	}
	if availabilityErr, ok := forgeavailability.As(err); ok && availabilityErr != nil {
		return err
	}
	errorsFound, _ := deliverableCommandErrors(err)
	for _, deliverableErr := range errorsFound {
		if deliverableErr == nil {
			continue
		}
		if workProductPushed && strings.Contains(strings.ToLower(deliverableErr.Arguments), "ci-trigger-label") {
			continue
		}
		operation := strings.TrimSpace(deliverableErr.Operation)
		if deliverableErr.OperationClass == "push" && !strings.Contains(strings.ToLower(operation), "git push") {
			operation = "git push"
		}
		class, unavailable := forgeavailability.Classify(operation, deliverableErr.Error())
		if !unavailable {
			continue
		}
		host := forgeavailability.HostFromText(deliverableErr.Arguments + " " + deliverableErr.Error())
		if host == "" {
			host = fallbackHost
		}
		return forgeavailability.NewError(forgeavailability.Scope{Host: host, Operation: operation}, class, err)
	}
	return err
}

func IsDeliverableConfigurationError(err error) bool {
	errorsFound, _ := deliverableCommandErrors(err)
	for _, deliverableErr := range errorsFound {
		if deliverableErr == nil {
			continue
		}
		if deliverableCredentialFailureDetail(deliverableErr.Message + "\n" + deliverableErr.Body) {
			return true
		}
	}
	return false
}

func workerCredentialBlockerError(message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		return nil
	}
	firstLine, _, _ := strings.Cut(message, "\n")
	firstLine = strings.TrimSpace(strings.TrimLeft(firstLine, "#>*_- "))
	blocked := strings.HasPrefix(strings.ToLower(firstLine), "blocked") || strings.HasPrefix(strings.ToLower(firstLine), "work is blocked")
	if !blocked || !workerGitHubCredentialFailureDetail(message) {
		return nil
	}
	return &DeliverableCommandError{
		OperationClass: "pull_request",
		Operation:      "read GitHub issue and pull request",
		Status:         "blocked",
		Message:        truncateDeliverableDetail(firstLine),
		Body:           truncateDeliverableDetail(message),
	}
}

func workerGitHubCredentialFailureDetail(detail string) bool {
	detail = strings.ToLower(strings.TrimSpace(detail))
	if !githubCredentialFailureDetail(detail) {
		return false
	}
	for _, marker := range []string{
		"github",
		"gh auth",
		"gh issue",
		"gh pr",
		"gh api",
		"git clone",
		"git fetch",
		"git push",
		"could not read username",
		"permission denied (publickey)",
	} {
		if strings.Contains(detail, marker) {
			return true
		}
	}
	return false
}

func deliverableCredentialFailureDetail(detail string) bool {
	if githubCredentialFailureDetail(detail) {
		return true
	}
	detail = strings.ToLower(strings.TrimSpace(detail))
	for _, phrase := range []string{
		"executable file not found",
	} {
		if strings.Contains(detail, phrase) {
			return true
		}
	}
	return false
}

func githubCredentialFailureDetail(detail string) bool {
	detail = strings.ToLower(strings.TrimSpace(detail))
	for _, phrase := range []string{
		"gh auth login",
		"populate gh_token",
		"not logged into any github hosts",
		"no credentials provided",
		"bad credentials",
		"authentication failed",
		"authentication required",
		"could not read username",
		"permission denied (publickey)",
		"github token is not configured",
		"gh_token environment variable is empty",
		"missing github authentication",
		"missing github credentials",
		"github authentication is unavailable",
		"github credentials are unavailable",
		"github credential injection is disabled",
		"github credential policy is disabled",
		"github_credential_policy: isolated_disabled",
	} {
		if strings.Contains(detail, phrase) {
			return true
		}
	}
	return false
}

func classifyForgeOperationError(err error, operation string, host string) error {
	if err == nil {
		return nil
	}
	class, unavailable := forgeavailability.Classify(operation, err.Error())
	if !unavailable {
		return err
	}
	if observedHost := forgeavailability.HostFromText(err.Error()); observedHost != "" {
		host = observedHost
	}
	return forgeavailability.NewError(forgeavailability.Scope{Host: host, Operation: operation}, class, err)
}

func pullRequestDeliverableFailure(err error) (*DeliverableCommandError, bool) {
	errorsFound, onlyDeliverableErrors := deliverableCommandErrors(err)
	if !onlyDeliverableErrors || len(errorsFound) == 0 {
		return nil, false
	}
	for _, deliverableErr := range errorsFound {
		if deliverableErr == nil || deliverableErr.OperationClass != "pull_request" {
			return nil, false
		}
	}
	return errorsFound[0], true
}

func deliverableCommandErrors(err error) ([]*DeliverableCommandError, bool) {
	if err == nil {
		return nil, true
	}
	var result []*DeliverableCommandError
	var visit func(error) bool
	visit = func(current error) bool {
		if current == nil {
			return true
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			for _, nested := range joined.Unwrap() {
				if !visit(nested) {
					return false
				}
			}
			return true
		}
		if wrapped := errors.Unwrap(current); wrapped != nil {
			return visit(wrapped)
		}
		var deliverableErr *DeliverableCommandError
		if errors.As(current, &deliverableErr) {
			result = append(result, deliverableErr)
			return true
		}
		return false
	}
	return result, visit(err)
}

func deliverableCommandEvidenceFromError(err error) []DeliverableCommandEvidence {
	errorsFound, _ := deliverableCommandErrors(err)
	evidence := make([]DeliverableCommandEvidence, 0, len(errorsFound))
	for _, deliverableErr := range errorsFound {
		if deliverableErr == nil {
			continue
		}
		evidence = append(evidence, DeliverableCommandEvidence{
			ItemID:         strings.TrimSpace(deliverableErr.ItemID),
			OperationClass: strings.TrimSpace(deliverableErr.OperationClass),
			Operation:      strings.TrimSpace(deliverableErr.Operation),
			Command:        summarizeDeliverableCommand(deliverableErr.Command),
			Status:         strings.TrimSpace(deliverableErr.Status),
			ExitCode:       cloneIntPointer(deliverableErr.ExitCode),
			Outcome:        "failed",
			TargetRef:      cloneDeliverableTargetRefEvidence(deliverableErr.TargetRef),
		})
	}
	return evidence
}

func summarizeDeliverableCommand(command string) string {
	segments := shellCommandSegments(command)
	summary := make([]string, 0, len(segments))
	for _, segment := range segments {
		if gitPushCommand(segment) {
			summary = append(summary, "git push")
			continue
		}
		summary = append(summary, "<redacted>")
	}
	return strings.Join(summary, " && ")
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func deliverableRecoveryPrompt(branch string, repository string, deliverableErr *DeliverableCommandError) string {
	branch = strings.TrimSpace(branch)
	repository = strings.TrimSpace(repository)
	operation := "pull request creation"
	arguments := "{}"
	if deliverableErr != nil {
		if value := strings.TrimSpace(deliverableErr.Operation); value != "" {
			operation = value
		}
		if value := strings.TrimSpace(deliverableErr.Arguments); value != "" {
			arguments = value
		}
	}
	var prompt strings.Builder
	prompt.WriteString("The implementation is complete and the branch is already pushed. Recover only the pull request deliverable.\n\n")
	prompt.WriteString("Search first for an existing open pull request whose head branch is exactly `")
	prompt.WriteString(branch)
	prompt.WriteString("`")
	if repository != "" {
		prompt.WriteString(" in `")
		prompt.WriteString(repository)
		prompt.WriteString("`")
	}
	prompt.WriteString(". Adopt it only after a tool result explicitly reports that exact head branch; if a search result omits the head, inspect the pull request before adopting it. If no exact-head pull request exists, retry `")
	prompt.WriteString(operation)
	prompt.WriteString("` with these attempted arguments:\n\n")
	prompt.WriteString(arguments)
	prompt.WriteString("\n\nDo not edit files, change commits, push the branch, rerun implementation, or perform unrelated work.")
	return prompt.String()
}

func forgeRetryPrompt(retry ForgeRetry, issue connector.Issue) string {
	branch := strings.TrimSpace(retry.Branch)
	operation := strings.TrimSpace(retry.Operation)
	if operation == "" {
		operation = "the failed forge write"
	}
	var prompt strings.Builder
	prompt.WriteString("The prior attempt completed its implementation work but the forge was unavailable. Retry only the deliverable write; do not edit files, change commits, rerun implementation, or perform unrelated work.\n\n")
	if retry.WorkProductPushed {
		prompt.WriteString("The branch is already pushed. Preserve it and recover the pull request create or update for branch `")
		prompt.WriteString(branch)
		prompt.WriteString("`.")
	} else {
		prompt.WriteString("Retry `")
		prompt.WriteString(operation)
		prompt.WriteString("` for branch `")
		prompt.WriteString(branch)
		prompt.WriteString("`. After the push succeeds, create or update the required pull request if one is not already open.")
	}
	if repository := strings.TrimSpace(issue.PRRepository); repository != "" {
		prompt.WriteString(" Use repository `")
		prompt.WriteString(repository)
		prompt.WriteString("`.")
	}
	if arguments := strings.TrimSpace(retry.Arguments); arguments != "" {
		prompt.WriteString("\n\nThe failed operation used these arguments:\n\n")
		prompt.WriteString(arguments)
	}
	prompt.WriteString("\n\nComplete the existing Detent Workpad and handoff protocol after delivery succeeds.")
	return prompt.String()
}

func mergeAgentTurnExecutions(initial agentTurnExecution, recovery agentTurnExecution) agentTurnExecution {
	result := recovery.result
	result.Output = joinAgentOutputs(initial.result.Output, recovery.result.Output)
	result.Model = effectiveModel(recovery.result.Model, initial.result.Model)
	result.RuntimeIdentity = initial.result.RuntimeIdentity.Merge(recovery.result.RuntimeIdentity)
	result.Tokens = addAgentTokenTotals(initial.result.Tokens, recovery.result.Tokens)
	result.RateLimits = mergeAgentRateLimits(initial.result.RateLimits, recovery.result.RateLimits)
	result.SkillDraftProposed = initial.result.SkillDraftProposed || recovery.result.SkillDraftProposed
	result.PullRequestUpdated = initial.result.PullRequestUpdated || recovery.result.PullRequestUpdated
	result.PullRequestHeadPushed = initial.result.PullRequestHeadPushed || recovery.result.PullRequestHeadPushed
	result.CITriggerLabelReapplied = initial.result.CITriggerLabelReapplied || recovery.result.CITriggerLabelReapplied
	result.ForgeWriteCompleted = initial.result.ForgeWriteCompleted || recovery.result.ForgeWriteCompleted
	if diffStatsEmpty(result.DiffStats) {
		result.DiffStats = initial.result.DiffStats
	}
	return agentTurnExecution{
		turnResult:  recovery.turnResult,
		result:      result,
		err:         recovery.err,
		cleanupErr:  errors.Join(initial.cleanupErr, recovery.cleanupErr),
		turnStarted: initial.turnStarted || recovery.turnStarted,
		turnCount:   max(initial.turnCount, recovery.turnCount),
	}
}

func joinAgentOutputs(initial string, recovery string) string {
	initial = strings.TrimSpace(initial)
	recovery = strings.TrimSpace(recovery)
	if initial == "" {
		return recovery
	}
	if recovery == "" {
		return initial
	}
	return initial + "\n" + recovery
}

func addAgentTokenTotals(initial TokenTotals, recovery TokenTotals) TokenTotals {
	result := TokenTotals{
		InputTokens:           initial.InputTokens + recovery.InputTokens,
		CachedInputTokens:     initial.CachedInputTokens + recovery.CachedInputTokens,
		OutputTokens:          initial.OutputTokens + recovery.OutputTokens,
		ReasoningOutputTokens: initial.ReasoningOutputTokens + recovery.ReasoningOutputTokens,
		TotalTokens:           initial.TotalTokens + recovery.TotalTokens,
		RuntimeSeconds:        initial.RuntimeSeconds + recovery.RuntimeSeconds,
		Last:                  cloneAgentTokenCounts(recovery.Last),
		ModelContextWindow:    recovery.ModelContextWindow,
	}
	if result.Last == nil {
		result.Last = cloneAgentTokenCounts(initial.Last)
	}
	if result.ModelContextWindow == nil {
		result.ModelContextWindow = initial.ModelContextWindow
	}
	return result
}

func (r *Runner) workspaceRecoveryState(
	backend workspace.Backend,
	ctx context.Context,
	info workspace.Info,
	issue workspace.Issue,
	phase string,
) *workspace.RecoveryState {
	provider, ok := backend.(workspace.RecoveryStateProvider)
	if !ok {
		return nil
	}
	state, err := provider.RecoveryState(ctx, info, issue)
	if err == nil {
		return &state
	}
	r.logger.Warn(
		"workspace recovery state failed",
		slog.String("issue_id", issue.ID),
		slog.String("issue_identifier", issue.Identifier),
		slog.String("workspace_path", info.Path),
		slog.String("phase", phase),
		slog.String("error", err.Error()),
	)
	return nil
}

type workspaceArtifactEvidenceObservation struct {
	evidence  workspace.ArtifactEvidence
	supported bool
	observed  bool
	err       string
}

func (r *Runner) observeWorkspaceArtifactEvidence(
	backend workspace.Backend,
	ctx context.Context,
	info workspace.Info,
	issue workspace.Issue,
	phase string,
) workspaceArtifactEvidenceObservation {
	provider, ok := backend.(workspace.ArtifactEvidenceProvider)
	if !ok {
		return workspaceArtifactEvidenceObservation{}
	}
	observation := workspaceArtifactEvidenceObservation{supported: true}
	evidence, err := provider.ArtifactEvidence(ctx, info, issue)
	if err == nil {
		observation.evidence = evidence
		observation.observed = evidence.Available
		return observation
	}
	observation.err = err.Error()
	r.logger.Warn(
		"workspace artifact evidence failed",
		slog.String("issue_id", issue.ID),
		slog.String("issue_identifier", issue.Identifier),
		slog.String("workspace_path", info.Path),
		slog.String("phase", phase),
		slog.String("error", err.Error()),
	)
	return observation
}

func artifactProgressEvidence(initial, current workspaceArtifactEvidenceObservation) ArtifactProgressEvidence {
	evidence := ArtifactProgressEvidence{
		InitialFiles:       initial.evidence.Files,
		CurrentFiles:       current.evidence.Files,
		InitialFingerprint: strings.TrimSpace(initial.evidence.Fingerprint),
		CurrentFingerprint: strings.TrimSpace(current.evidence.Fingerprint),
	}
	if initial.observed && current.observed {
		evidence.Available = true
		return evidence
	}
	warnings := make([]string, 0, 2)
	if initial.err != "" {
		warnings = append(warnings, "initial artifact output evidence: "+initial.err)
	}
	if current.err != "" {
		warnings = append(warnings, "final artifact output evidence: "+current.err)
	}
	if len(warnings) == 0 && (initial.supported || current.supported) {
		warnings = append(warnings, "artifact output root is not configured")
	}
	evidence.Warning = strings.Join(warnings, "; ")
	return evidence
}

func (r *Runner) publishDispatchLoopStart(req RunRequest, recovery *workspace.RecoveryState) {
	if req.OnUsageUpdate == nil {
		return
	}
	snapshot := DispatchLoopStartSnapshot{}
	if recovery != nil {
		snapshot.WorkspaceDiffAvailable = true
		snapshot.WorkspaceHeadAvailable = strings.TrimSpace(recovery.HeadSHA) != ""
		snapshot.DiffStats = diffStatsFromWorkspace(recovery.DiffStat)
		applyRecoveryState(&snapshot.DiffStats, recovery)
	}
	if err := req.OnUsageUpdate(UsageUpdate{DispatchLoopStart: &snapshot}); err != nil && r.logger != nil {
		r.logger.Warn("dispatch loop start snapshot publish failed", "issue_id", req.Issue.ID, "identifier", req.Issue.Identifier, "error", err)
	}
}

func (r *Runner) workspaceDeliverableState(
	backend workspace.Backend,
	ctx context.Context,
	info workspace.Info,
	issue workspace.Issue,
) *workspace.DeliverableState {
	observation := r.observeWorkspaceDeliverableState(backend, ctx, info, issue, "deliverable_recovery")
	if !observation.observed {
		return nil
	}
	state := observation.state
	return &state
}

type workspaceDeliverableStateObservation struct {
	state     workspace.DeliverableState
	supported bool
	observed  bool
	err       string
}

func (r *Runner) observeWorkspaceDeliverableState(
	backend workspace.Backend,
	ctx context.Context,
	info workspace.Info,
	issue workspace.Issue,
	phase string,
) workspaceDeliverableStateObservation {
	provider, ok := backend.(workspace.DeliverableStateProvider)
	if !ok {
		return workspaceDeliverableStateObservation{}
	}
	state, err := provider.DeliverableState(ctx, info, issue)
	if err == nil {
		return workspaceDeliverableStateObservation{state: state, supported: true, observed: true}
	}
	r.logger.Warn(
		"workspace deliverable state failed",
		slog.String("issue_id", issue.ID),
		slog.String("issue_identifier", issue.Identifier),
		slog.String("workspace_path", info.Path),
		slog.String("phase", strings.TrimSpace(phase)),
		slog.String("error", err.Error()),
	)
	return workspaceDeliverableStateObservation{supported: true, err: err.Error()}
}

func deliverableTargetRefEvidence(branch string, initial workspaceDeliverableStateObservation, postCommand workspaceDeliverableStateObservation) *DeliverableTargetRefEvidence {
	evidence := &DeliverableTargetRefEvidence{
		Remote:              "origin",
		Ref:                 "refs/heads/" + strings.TrimSpace(branch),
		InitialObserved:     initial.observed,
		PostCommandObserved: postCommand.observed,
	}
	if initial.observed {
		evidence.Remote = firstNonBlankString(initial.state.Remote, evidence.Remote)
		evidence.Ref = firstNonBlankString(initial.state.RemoteRef, evidence.Ref)
		evidence.InitialRemoteHeadSHA = strings.TrimSpace(initial.state.RemoteHeadSHA)
		evidence.InitialRemoteRefExists = initial.state.RemoteBranchExists
	}
	if postCommand.observed {
		evidence.Remote = firstNonBlankString(postCommand.state.Remote, evidence.Remote)
		evidence.Ref = firstNonBlankString(postCommand.state.RemoteRef, evidence.Ref)
		evidence.PostCommandLocalHeadSHA = strings.TrimSpace(postCommand.state.LocalHeadSHA)
		evidence.PostCommandRemoteHeadSHA = strings.TrimSpace(postCommand.state.RemoteHeadSHA)
		evidence.PostCommandRemoteRefExists = postCommand.state.RemoteBranchExists
	}
	errorsFound := make([]string, 0, 2)
	if initial.err != "" {
		errorsFound = append(errorsFound, "initial: "+initial.err)
	} else if !initial.supported {
		errorsFound = append(errorsFound, "initial: workspace backend does not provide deliverable state")
	}
	if postCommand.err != "" {
		errorsFound = append(errorsFound, "post-command: "+postCommand.err)
	} else if !postCommand.supported {
		errorsFound = append(errorsFound, "post-command: workspace backend does not provide deliverable state")
	}
	evidence.CheckError = strings.Join(errorsFound, "; ")
	evidence.AdvancedToLocalHead = evidence.InitialObserved &&
		evidence.PostCommandObserved &&
		evidence.PostCommandRemoteRefExists &&
		evidence.PostCommandLocalHeadSHA != "" &&
		evidence.PostCommandRemoteHeadSHA == evidence.PostCommandLocalHeadSHA &&
		(!evidence.InitialRemoteRefExists || evidence.InitialRemoteHeadSHA != evidence.PostCommandRemoteHeadSHA)
	return evidence
}

func finalizeDeliverableTargetRefEvidence(evidence *DeliverableTargetRefEvidence, finalState workspaceDeliverableStateObservation) *DeliverableTargetRefEvidence {
	evidence = cloneDeliverableTargetRefEvidence(evidence)
	if evidence == nil {
		evidence = &DeliverableTargetRefEvidence{}
	}
	evidence.FinalObserved = finalState.observed
	if finalState.observed {
		evidence.FinalLocalHeadSHA = strings.TrimSpace(finalState.state.LocalHeadSHA)
		evidence.FinalRemoteHeadSHA = strings.TrimSpace(finalState.state.RemoteHeadSHA)
		evidence.FinalRemoteRefExists = finalState.state.RemoteBranchExists
	}
	finalError := ""
	if finalState.err != "" {
		finalError = "final: " + finalState.err
	} else if !finalState.supported {
		finalError = "final: workspace backend does not provide deliverable state"
	}
	evidence.CheckError = strings.Join(nonBlankStrings(evidence.CheckError, finalError), "; ")
	evidence.AdvancedToLocalHead = evidence.AdvancedToLocalHead &&
		evidence.FinalObserved &&
		evidence.FinalRemoteRefExists &&
		evidence.FinalLocalHeadSHA == evidence.PostCommandRemoteHeadSHA &&
		evidence.FinalRemoteHeadSHA == evidence.PostCommandRemoteHeadSHA
	return evidence
}

func cloneDeliverableTargetRefEvidence(evidence *DeliverableTargetRefEvidence) *DeliverableTargetRefEvidence {
	if evidence == nil {
		return nil
	}
	clone := *evidence
	return &clone
}

func firstNonBlankString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func nonBlankStrings(values ...string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func applyDeliverableState(diffStats *DiffStats, state *workspace.DeliverableState) {
	if diffStats == nil || state == nil {
		return
	}
	diffStats.CommitsAhead = state.CommitsAhead
	diffStats.RemoteBranchExists = state.RemoteBranchExists
	diffStats.DeliveryStateChecked = true
}

func (r *Runner) checkDispatchBudget(ctx context.Context, checker BudgetChecker, estimator DispatchEstimator, issue connector.Issue, model string, now time.Time) (RunResult, bool, error) {
	if checker == nil {
		return RunResult{}, false, nil
	}

	estimate := budget.TokenEstimate{}
	if estimator != nil {
		var err error
		estimate, err = estimator.EstimateDispatch(ctx, model)
		if err != nil {
			return RunResult{}, false, fmt.Errorf("estimate dispatch budget: %w", err)
		}
	}
	decision, err := checker.CheckDispatch(ctx, budget.DispatchRequest{
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		IssueURL:   issue.URL,
		Model:      model,
		Now:        now,
		Estimate:   estimate,
	})
	if err != nil {
		return RunResult{}, false, fmt.Errorf("check dispatch budget: %w", err)
	}
	if decision.Allowed || decision.Refusal == nil {
		return runResultWithBudgetProjection(decision.Projection), false, nil
	}
	result := runResultWithBudgetProjection(decision.Projection)
	result.FinalState = FinalStateCompleted
	result.BudgetRefusal = budgetRefusalFromDecision(issue, *decision.Refusal)
	return result, true, nil
}

func runResultWithBudgetProjection(projection *budget.Projection) RunResult {
	if projection == nil {
		return RunResult{}
	}
	return RunResult{budgetProjection: &dispatchBudgetProjection{
		CostUSD:        projection.CostUSD,
		EstimateSource: projection.EstimateSource(),
	}}
}

func budgetRefusalFromDecision(issue connector.Issue, refusal budget.Refusal) *BudgetRefusal {
	var maxUSD *float64
	if refusal.MaxUSD != nil {
		value := *refusal.MaxUSD
		maxUSD = &value
	}
	var resetAt *time.Time
	if refusal.ResetAt != nil {
		value := *refusal.ResetAt
		resetAt = &value
	}
	return &BudgetRefusal{
		Issue:            issue,
		Code:             string(refusal.Code),
		Message:          refusal.Message,
		Comment:          refusal.Comment(),
		CurrentSpendUSD:  refusal.CurrentSpendUSD,
		ProjectedCostUSD: refusal.ProjectedCostUSD,
		MaxUSD:           maxUSD,
		ResetAt:          resetAt,
		RefusedAt:        refusal.RefusedAt,
	}
}

func (r *Runner) Validate(ctx context.Context, req ValidatorRequest) (gate.ValidatorResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	workflow, agentRuntime, _, _ := r.runtimeSnapshot()
	workerGitHub, err := r.workerGitHubPolicy(ctx, workflow.Config, req.Issue.Identifier)
	if err != nil {
		r.logWorkerGitHubPolicyError(req.Issue, err)
		return gate.ValidatorResult{}, err
	}

	workspaceIssue := workspaceIssue(r.projectID, req.Issue)
	r.logWorkerEvent(req.Issue, "worker_check_workspace_create_started")
	info, err := r.workspace.Create(ctx, workspaceIssue)
	if err != nil {
		return gate.ValidatorResult{}, fmt.Errorf("create workspace: %w", err)
	}
	r.logWorkerEvent(req.Issue, "worker_check_workspace_created",
		"workspace_path", info.Path,
		"workspace_branch", info.Branch,
	)

	if err := r.workspace.BeforeRun(ctx, info, workspaceIssue); err != nil {
		return gate.ValidatorResult{}, fmt.Errorf("workspace before_run: %w", err)
	}
	r.logWorkerEvent(req.Issue, "worker_check_before_run_finished",
		"workspace_path", info.Path,
	)

	afterRunPending := true
	defer func() {
		if afterRunPending {
			r.afterRun(r.workspace, info, workspaceIssue)
		}
	}()

	validator := gate.Effective(workflow.Config.Gate).Validator
	promptOptions := r.validatorPromptOptions(ctx, info, workspaceIssue, validatorMaxInlineDiffBytes(validator))
	prompt := BuildValidatorPrompt(workflow, req.Issue, promptOptions)
	selection, backend, backendConfig, err := agentRuntime.selectBackendForRole(req.Issue, selectorContext(req.SelectorContext, workflow), RoleValidator)
	if err != nil {
		return gate.ValidatorResult{}, err
	}

	selectedModel := selection.Model
	if override := strings.TrimSpace(validator.Model); override != "" {
		selectedModel = override
	}
	baseModel := effectiveModel("", selectedModel, agentRuntime.defaultModelForRole(RoleValidator))
	resolvedSelection := resolveAgentSelection(ctx, req.Issue, info.Path, baseModel, RoleValidator, workflow.Config, backendConfig, backend)
	if resolvedSelection.Err != nil {
		return gate.ValidatorResult{}, resolvedSelection.Err
	}
	selectedModel = resolvedSelection.Model
	sessionModel := effectiveModel("", selectedModel, agentRuntime.defaultModelForRole(RoleValidator))

	startedAt := req.StartedAt
	if startedAt.IsZero() {
		startedAt = r.now().UTC()
	}
	runStartedAt := r.now()
	runtimeIdentity := configuredRuntimeIdentity(selection, backendConfig, RoleValidator, sessionModel, startedAt)
	runtimeIdentity.Selection = resolvedSelection.Selection
	runtimeIdentity.Selection.BackendSource = workflow.Config.Agents.Sources["backends."+selection.BackendID]
	runtimeIdentity.Selection.RouteSource = workflow.Config.Agents.Sources["routes."+selection.RouteName]
	if resolvedSelection.Effort != "" {
		runtimeIdentity.ReasoningEffort = agentidentity.NewValue(resolvedSelection.Effort, agentidentity.ProvenanceConfigured)
	}
	runReq := RunRequest{
		Issue:            req.Issue,
		StartedAt:        req.StartedAt,
		SelectorContext:  req.SelectorContext,
		OnUsageUpdate:    req.OnUsageUpdate,
		OnActivityUpdate: req.OnActivityUpdate,
	}
	sessionID, sessionStarted, err := r.startSession(ctx, runReq, startedAt, runtimeIdentity, store.AgentResumeState{}, "", "")
	if err != nil {
		return gate.ValidatorResult{}, err
	}
	sessionCtx, cancelSession := r.sessionLimit(
		ctx,
		durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS),
		ErrSessionDurationExceeded,
	)
	defer cancelSession()
	sessionCtx, cancelSessionBrake := context.WithCancelCause(sessionCtx)
	defer cancelSessionBrake(context.Canceled)
	sessionBrake := newSessionBrakeController(
		sessionCtx,
		runStartedAt,
		durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS),
		workflow.Config.Agent.MaxTurns,
		durationFromMillis(workflow.Config.Agent.NoProgressTimeoutMS),
		cancelSessionBrake,
		func(probeCtx context.Context) (sessionProgressSnapshot, error) {
			return r.sessionProgressSnapshot(probeCtx, r.workspace, info, workspaceIssue, nil)
		},
		r.now,
		r.progressTicker,
		r.logger,
		req.Issue,
	)
	defer sessionBrake.Stop()
	runReq.sessionBrake = sessionBrake

	checkStartedAttrs := []any{
		"workspace_path", info.Path,
		"detent_session_id", sessionID,
		"github_credential_policy", workerGitHubPolicyName(workerGitHub),
		"github_credential_identity", workerGitHub.CredentialIdentity,
	}
	checkStartedAttrs = append(checkStartedAttrs, runtimeIdentityLogAttrs(runtimeIdentity)...)
	r.logWorkerEvent(req.Issue, "worker_check_started", checkStartedAttrs...)
	runResult := RunResult{FinalState: FinalStateCompleted, RuntimeIdentity: runtimeIdentity}
	progress := newAgentRunProgress(runtimeoutput.Policy{MaxBytes: workflow.Config.Agent.OutputTruncation.MaxBytes}, "", "", 0, "", 0)
	eventAt := r.now()
	progress.apply(AgentUpdate{Type: AgentUpdateRuntimeIdentity, RuntimeIdentity: runtimeIdentity}, eventAt)
	if err := r.publishRunUpdate(sessionCtx, runReq, info, workspaceIssue, progress, runResult, eventAt, runStartedAt, sessionID); err != nil {
		return gate.ValidatorResult{}, err
	}
	var output strings.Builder
	usage := newSessionTokenUsage(false)
	workerProcessObserved := false
	modelProvider, serviceTier, effort := agentTurnIdentityOptions(backendConfig)
	if resolvedSelection.Effort != "" {
		effort = resolvedSelection.Effort
	}
	turnResult, cleanupScratch, turnErr := runAgentBackendTurnWithToolsUsingLimitPreservingScratch(sessionCtx, backend, AgentTurnRequest{
		Workspace:          info.Path,
		Prompt:             prompt,
		Model:              selectedModel,
		ModelProvider:      modelProvider,
		ServiceTier:        serviceTier,
		ReasoningEffort:    effort,
		MaxTurns:           workflow.Config.Agent.MaxTurns,
		TurnTimeout:        durationFromMillis(validator.TurnTimeoutMS),
		MaxDuration:        durationFromMillis(workflow.Config.Agent.MaxTurnDurationMS),
		ExtraWritableRoots: extraWritableRootsForWorkspace(sessionCtx, workflow.Config.Workspace.Kind, info.Path, r.logger),
		MaxRSSBytes:        r.maxAgentRSSBytes,
		RSSPollInterval:    r.rssPollInterval,
		cacheStrategy:      workflow.Config.Workspace.CacheStrategy,
		projectID:          r.projectID,
		workerGitHub:       workerGitHub,
		processRSS:         r.processRSS,
	}, nil, nil, func(updateCtx context.Context, update AgentUpdate) error {
		eventAt := r.now()
		if update.Type == AgentUpdateTokenUsage {
			update.Tokens = usage.normalize(update.Tokens)
		}
		if update.Type == AgentUpdateProcessStarted && update.WorkerProcess.PID > 0 {
			workerProcessObserved = true
		}
		if !update.RuntimeIdentity.IsZero() {
			update.RuntimeIdentity = update.RuntimeIdentity.ObserveAt(eventAt)
		}
		r.logAgentUpdate(runReq, sessionID, update)
		if err := r.persistSessionWorkerProcess(updateCtx, sessionID, update, info.Path, filepath.Join(info.Path, ".detent", "tmp")); err != nil {
			return err
		}
		if err := r.persistSessionProviderIdentity(updateCtx, sessionID, update); err != nil {
			return err
		}
		if err := publishAgentActivity(runReq, sessionID, update, eventAt); err != nil {
			return err
		}
		if update.Type == AgentUpdateMessageDelta {
			output.WriteString(update.Delta)
		}
		previousIdentity := runResult.RuntimeIdentity
		applyAgentUpdate(&runResult, update)
		if !previousIdentity.MateriallyEqual(runResult.RuntimeIdentity) {
			if err := r.persistSessionIdentity(updateCtx, sessionID, runResult.RuntimeIdentity); err != nil {
				return err
			}
			if runResult.RuntimeIdentity.HasRuntimeValues() {
				r.logRuntimeIdentity(runReq, sessionID, update, previousIdentity, runResult.RuntimeIdentity)
			}
		}
		progress.apply(update, eventAt)
		if err := r.publishRunUpdate(updateCtx, runReq, info, workspaceIssue, progress, runResult, eventAt, runStartedAt, sessionID); err != nil {
			return err
		}
		if err := r.enforceSessionTokenCeiling(workflow.Config.Agent, req.Issue, info.Path, update, eventAt); err != nil {
			return err
		}
		if err := sessionBrake.observe(updateCtx, progress.turnCount(), runResult.Tokens.TotalTokens); err != nil {
			return err
		}
		return nil
	}, r.turnLimit)
	processReapErr := r.reapSessionWorkerProcess(
		sessionCtx,
		sessionID,
		req.Issue,
		workerProcessReapReason(sessionCtx, turnErr),
	)
	workspaceReapErr := r.reapWorkspaceProcessesAfterTurn(
		sessionCtx,
		info.Path,
		sessionID,
		runReq.WorkAttemptID,
		req.Issue,
		turnErr,
		workerProcessObserved,
	)
	workerReapErr := errors.Join(processReapErr, workspaceReapErr)
	scratchCleanupErr := cleanupWorkerScratchAfterProcessReap(cleanupScratch, workerReapErr)
	if workerReapErr != nil {
		turnErr = errors.Join(turnErr, fmt.Errorf("%w: %w", ErrWorkerProcessReap, workerReapErr))
	}
	if cause := context.Cause(sessionCtx); cooperativeStopError(cause) || durationLimitError(cause) {
		if workerReapErr != nil {
			turnErr = errors.Join(cause, fmt.Errorf("%w: %w", ErrWorkerProcessReap, workerReapErr))
		} else {
			turnErr = cause
		}
	}
	turnErr = sessionBrake.wrapTurnLimit(ctx, turnErr)
	turnErr = sessionBrake.wrapDuration(ctx, turnErr, durationFromMillis(workflow.Config.Agent.MaxSessionDurationMS))
	sessionBrake.Stop()
	if brakeDiff := sessionBrake.resultDiffStats(); !diffStatsEmpty(brakeDiff) {
		runResult.DiffStats = brakeDiff
	}
	turnErr = errors.Join(turnErr, scratchCleanupErr)
	turnErr = classifyAgentCapacityError(backend, selection, backendConfig, runResult.RuntimeIdentity, turnErr, runResult.RateLimits, runStartedAt)
	checkFinishedAttrs := []any{
		"workspace_path", info.Path,
		"detent_session_id", sessionID,
		"provider_thread_id", turnResult.ThreadID,
		"provider_session_id", turnResult.SessionID,
		"outcome", workerRunOutcome(turnErr, runResult.FinalState),
		"error", errorString(turnErr),
	}
	checkFinishedAttrs = append(checkFinishedAttrs, runtimeIdentityLogAttrs(runResult.RuntimeIdentity)...)
	checkFinishedAttrs = append(checkFinishedAttrs, backendErrorAttrs(turnErr)...)
	if turnErr != nil {
		r.logWorkerEventLevel(slog.LevelWarn, req.Issue, "worker_check_finished", checkFinishedAttrs...)
	} else {
		r.logWorkerEvent(req.Issue, "worker_check_finished", checkFinishedAttrs...)
	}

	r.afterRun(r.workspace, info, workspaceIssue)
	afterRunPending = false
	r.logWorkerEvent(req.Issue, "worker_check_after_run_finished",
		telemetry.DetentSessionIDKey, sessionID,
		"workspace_path", info.Path,
	)

	finishedAt := r.now().UTC()
	runResult.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, finishedAt)
	if turnErr != nil {
		runResult.FinalState = finalStateForTurnError(turnErr)
		return gate.ValidatorResult{}, errors.Join(
			fmt.Errorf("run validator turn: %w", turnErr),
			r.finishSession(ctx, sessionID, sessionStarted, runReq.WorkAttemptID, req.Issue, startedAt, finishedAt, runResult, sessionModel, backendConfig.Kind, 1, turnResult, 0),
		)
	}

	validation, err := parseValidatorResult(output.String())
	if err != nil {
		runResult.FinalState = FinalStateFailed
		return gate.ValidatorResult{}, errors.Join(
			fmt.Errorf("parse validator result: %w", err),
			r.finishSession(ctx, sessionID, sessionStarted, runReq.WorkAttemptID, req.Issue, startedAt, finishedAt, runResult, sessionModel, backendConfig.Kind, 1, turnResult, 0),
		)
	}
	if err := r.finishSession(ctx, sessionID, sessionStarted, runReq.WorkAttemptID, req.Issue, startedAt, finishedAt, runResult, sessionModel, backendConfig.Kind, 1, turnResult, 0); err != nil {
		return gate.ValidatorResult{}, err
	}
	return validation, nil
}

func (r *Runner) validatorPromptOptions(ctx context.Context, info workspace.Info, issue workspace.Issue, maxInlineDiffBytes int) ValidatorPromptOptions {
	opts := ValidatorPromptOptions{
		WorkspacePath:      info.Path,
		Branch:             info.Branch,
		MaxInlineDiffBytes: maxInlineDiffBytes,
	}

	if provider, ok := r.workspace.(workspace.DiffProvider); ok {
		diff, err := provider.Diff(ctx, info, issue, maxInlineDiffBytes)
		if err == nil {
			opts.DiffStat = &diff.Stat
			opts.DiffPatch = diff.Patch
			opts.DiffTruncated = diff.Truncated
			return opts
		}
		opts.DiffError = err.Error()
		r.logValidatorDiffError(issue, info, "workspace diff failed", err)
	}

	stat, err := r.workspace.DiffStat(ctx, info, issue)
	if err != nil {
		if opts.DiffError == "" {
			opts.DiffError = err.Error()
		}
		r.logValidatorDiffError(issue, info, "workspace diff stat failed", err)
		return opts
	}
	opts.DiffStat = &stat
	return opts
}

func validatorMaxInlineDiffBytes(cfg gate.ValidatorConfig) int {
	if cfg.MaxInlineDiffBytes == nil {
		return gate.DefaultValidatorMaxInlineDiffBytes
	}
	return *cfg.MaxInlineDiffBytes
}

func (r *Runner) logValidatorDiffError(issue workspace.Issue, info workspace.Info, message string, err error) {
	if r == nil || r.logger == nil || err == nil {
		return
	}
	r.logger.Warn(
		message,
		slog.String("issue_id", issue.ID),
		slog.String("issue_identifier", issue.Identifier),
		slog.String("workspace_path", info.Path),
		slog.String("phase", "validator"),
		slog.String("error", err.Error()),
	)
}

type validatorJSONResult struct {
	Verdict    string                 `json:"verdict"`
	Score      float64                `json:"score"`
	Confidence float64                `json:"confidence"`
	TrustScore float64                `json:"trust_score"`
	Summary    string                 `json:"summary"`
	Findings   []validatorJSONFinding `json:"findings"`
}

type validatorJSONFinding struct {
	Severity string `json:"severity"`
	Body     string `json:"body"`
	Message  string `json:"message"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
}

func parseValidatorResult(output string) (gate.ValidatorResult, error) {
	payload, err := validatorJSONPayload(output)
	if err != nil {
		return gate.ValidatorResult{}, err
	}

	var decoded validatorJSONResult
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return gate.ValidatorResult{}, err
	}

	score := decoded.Score
	if score == 0 {
		switch {
		case decoded.TrustScore > 0:
			score = decoded.TrustScore
		case decoded.Confidence > 0:
			score = decoded.Confidence
		}
	}

	findings := make([]gate.Finding, 0, len(decoded.Findings))
	for _, finding := range decoded.Findings {
		body := strings.TrimSpace(finding.Body)
		if body == "" {
			body = strings.TrimSpace(finding.Message)
		}
		findings = append(findings, gate.Finding{
			Severity: strings.ToLower(strings.TrimSpace(finding.Severity)),
			Body:     body,
			Path:     strings.TrimSpace(finding.Path),
			Line:     finding.Line,
		})
	}

	return gate.ValidatorResult{
		Submitted: true,
		Verdict:   strings.TrimSpace(decoded.Verdict),
		Score:     score,
		Summary:   strings.TrimSpace(decoded.Summary),
		Findings:  findings,
	}, nil
}

func validatorJSONPayload(output string) (string, error) {
	output = strings.TrimSpace(output)
	start := strings.Index(output, "{")
	end := strings.LastIndex(output, "}")
	if start < 0 || end < start {
		return "", errors.New("validator output did not contain a JSON object")
	}
	return output[start : end+1], nil
}

func (r *Runner) ReapWorkspace(ctx context.Context, issue connector.Issue) (WorkspaceReapResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	workspaceIssue := workspaceIssue(r.projectID, issue)
	if cleaner, ok := r.workspace.(workspace.IssueCleaner); ok {
		result, err := cleaner.CleanupIssue(ctx, workspaceIssue)
		return WorkspaceReapResult{
			Path:      result.Path,
			Worktrees: result.Worktrees,
			Branches:  result.Branches,
			Processes: result.Processes,
		}, err
	}
	if err := r.workspace.Cleanup(ctx, issue.Identifier); err != nil {
		return WorkspaceReapResult{}, err
	}
	return WorkspaceReapResult{}, nil
}

func (r *Runner) ReconcileWorkspaces(ctx context.Context, activeIssues []connector.Issue) (WorkspaceReconcileResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reconciler, ok := r.workspace.(workspace.ResidualReconciler)
	if !ok {
		return WorkspaceReconcileResult{}, nil
	}
	active := make([]workspace.Issue, 0, len(activeIssues))
	for _, issue := range activeIssues {
		active = append(active, workspaceIssue(r.projectID, issue))
	}
	result, err := reconciler.ReconcileResiduals(ctx, active)
	failures := make([]WorkspaceCleanupFailure, 0, len(result.Failures))
	for _, failure := range result.Failures {
		failures = append(failures, WorkspaceCleanupFailure{Path: failure.Path, Error: failure.Error})
	}
	return WorkspaceReconcileResult{
		Removed:           result.Removed,
		ActiveSkipped:     result.ActiveSkipped,
		PreservedSkipped:  result.PreservedSkipped,
		RegisteredSkipped: result.RegisteredSkipped,
		UnownedSkipped:    result.UnownedSkipped,
		CompletedPaths:    result.CompletedPaths,
		Failures:          failures,
	}, err
}

func (r *Runner) runtimeSnapshot() (config.Workflow, agentRuntime, BudgetChecker, DispatchEstimator) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.workflow, r.agentRuntime, r.budgetChecker, r.dispatchEstimator
}

func (r *Runner) availableSkills(workflow config.Workflow, workspacePath string) ([]skills.Skill, error) {
	cfg := workflow.Config.Agent.Skills
	if !cfg.Enabled {
		return nil, nil
	}

	result, err := skills.Load(workspacePath, skills.Options{
		Path:              cfg.Path,
		MaxSkillsInPrompt: cfg.MaxSkillsInPrompt,
		Logger:            r.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("load skills: %w", err)
	}
	for _, drop := range result.Dropped {
		r.logger.Warn(
			"repo skill dropped",
			slog.String("path", drop.Path),
			slog.String("reason", string(drop.Reason)),
			slog.String("message", drop.Message),
		)
	}
	return result.Skills, nil
}

func (r *Runner) afterRun(backend workspace.Backend, info workspace.Info, issue workspace.Issue) {
	ctx, cancel := context.WithTimeout(context.Background(), r.afterRunTimeout)
	defer cancel()

	backend.AfterRun(ctx, info, issue)
}

func (r *Runner) recordFailedRunNote(workspacePath string, issue connector.Issue, result RunResult, runErr error, at time.Time) {
	notesPath, err := notes.WorkspacePath(workspacePath)
	if err != nil {
		r.logger.Warn("resolve failed run note path failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		return
	}
	if err := notes.Append(notesPath, notes.Entry{
		Title: "Failed run output tail",
		Body:  failedRunNoteBody(result, runErr),
	}, notes.AppendOptions{Now: at, MaxBytes: notes.DefaultMaxBytes}); err != nil {
		r.logger.Warn("record failed run note failed", "issue_id", issue.ID, "identifier", issue.Identifier, "path", notesPath, "error", err)
	}
}

func failedRunNoteBody(result RunResult, runErr error) string {
	var b strings.Builder
	finalState := strings.TrimSpace(result.FinalState)
	if finalState == "" {
		finalState = FinalStateFailed
	}
	b.WriteString("- final_state: ")
	b.WriteString(finalState)
	if runErr != nil {
		b.WriteString("\n- error: ")
		b.WriteString(strings.TrimSpace(runErr.Error()))
	}
	output := strings.TrimSpace(notes.Tail(result.Output, notes.DefaultTailBytes))
	if output != "" {
		b.WriteString("\n\nOutput tail:\n\n```text\n")
		b.WriteString(output)
		if !strings.HasSuffix(output, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("```")
	}
	return b.String()
}

func (r *Runner) startSession(
	ctx context.Context,
	req RunRequest,
	startedAt time.Time,
	identity agentidentity.Identity,
	resumeState store.AgentResumeState,
	orphanRecoveryOutcome string,
	orphanRecoveryFallbackReason string,
) (int64, bool, error) {
	if r.store == nil {
		return 0, false, nil
	}

	sessionID, err := r.store.StartSession(ctx, store.SessionStart{
		ProjectID:                    r.projectID,
		IssueID:                      req.Issue.ID,
		Identifier:                   req.Issue.Identifier,
		IssueURL:                     req.Issue.URL,
		WorkAttemptID:                req.WorkAttemptID,
		StartedAt:                    startedAt,
		Model:                        identity.ResolvedModel.Value,
		RequestedModel:               identity.RequestedModel.Value,
		AgentBackendID:               identity.BackendID,
		AgentBackendKind:             identity.BackendKind,
		AgentRole:                    identity.Role,
		RuntimeIdentity:              identity,
		ProviderThreadID:             resumeState.ProviderThreadID,
		ProviderSessionID:            resumeState.ProviderSessionID,
		ResumedFromSessionID:         resumeState.DetentSessionID,
		OrphanRecoveryOutcome:        orphanRecoveryOutcome,
		OrphanRecoveryFallbackReason: orphanRecoveryFallbackReason,
	})
	if err != nil {
		return 0, false, fmt.Errorf("start agent session: %w", err)
	}
	attrs := []any{"detent_session_id", sessionID, "work_attempt_id", req.WorkAttemptID}
	attrs = append(attrs, runtimeIdentityLogAttrs(identity)...)
	r.logWorkerEvent(req.Issue, "worker_session_started", attrs...)
	return sessionID, true, nil
}

func (r *Runner) persistSessionProviderIdentity(ctx context.Context, sessionID int64, update AgentUpdate) error {
	if sessionID <= 0 || update.AuxiliaryTurn {
		return nil
	}
	threadID := strings.TrimSpace(update.ThreadID)
	providerSessionID := strings.TrimSpace(update.ProviderSessionID)
	if threadID == "" && providerSessionID == "" {
		return nil
	}
	providerStore, ok := r.store.(sessionProviderStore)
	if !ok {
		return nil
	}
	if err := providerStore.UpdateSessionProviderIdentity(ctx, sessionID, store.SessionProviderIdentity{
		ThreadID:  threadID,
		SessionID: providerSessionID,
	}); err != nil {
		return fmt.Errorf("update agent session provider identity: %w", err)
	}
	return nil
}

func (r *Runner) persistSessionWorkerProcess(ctx context.Context, sessionID int64, update AgentUpdate, cleanupRoot string, cleanupPath string) error {
	identity := update.WorkerProcess
	if sessionID <= 0 || identity.PID <= 0 || identity.StartedAt.IsZero() {
		return nil
	}
	processStore, ok := r.store.(sessionWorkerProcessStore)
	if !ok {
		return nil
	}
	if err := processStore.UpdateSessionWorkerProcess(ctx, sessionID, store.WorkerProcessRegistration{
		WorkerProcessIdentity: store.WorkerProcessIdentity{
			PID:       identity.PID,
			GroupID:   identity.GroupID,
			StartedAt: identity.StartedAt,
		},
		CleanupRoot: strings.TrimSpace(cleanupRoot),
		CleanupPath: strings.TrimSpace(cleanupPath),
	}); err != nil {
		return fmt.Errorf("update agent session worker process: %w", err)
	}
	return nil
}

func (r *Runner) reapSessionWorkerProcess(ctx context.Context, sessionID int64, issue connector.Issue, reason string) error {
	if sessionID <= 0 {
		return nil
	}
	processStore, ok := r.store.(sessionWorkerProcessReaper)
	if !ok {
		return nil
	}
	processes, err := processStore.ListActiveWorkerProcesses(context.WithoutCancel(ctx))
	if err != nil {
		return fmt.Errorf("list agent session worker processes: %w", err)
	}
	for _, process := range processes {
		if process.SessionID != sessionID {
			continue
		}
		identity := procgroup.Identity{PID: process.PID, GroupID: process.GroupID, StartedAt: process.StartedAt}
		outcome, reapErr := r.reapWorkerProcess(context.WithoutCancel(ctx), identity, r.workerReapGrace)
		attrs := []any{
			telemetry.DetentSessionIDKey, sessionID,
			"pid", process.PID,
			"pgid", process.GroupID,
			"reason", strings.TrimSpace(reason),
			"decision", string(outcome),
		}
		if reapErr != nil {
			attrs = append(attrs, "error", reapErr)
			r.logWorkerEventLevel(slog.LevelInfo, issue, "worker_process_reap_decision", attrs...)
			return fmt.Errorf("reap agent session worker process: %w", reapErr)
		}
		r.logWorkerEventLevel(slog.LevelInfo, issue, "worker_process_reap_decision", attrs...)
		cleanupAttempts, cleanupErr := r.cleanupSessionWorkerArtifacts(context.WithoutCancel(ctx), process.CleanupRoot, process.CleanupPath)
		if cleanupErr != nil {
			r.logWorkerEventLevel(slog.LevelWarn, issue, "worker_artifact_cleanup_failed",
				telemetry.DetentSessionIDKey, sessionID,
				"pid", process.PID,
				"pgid", process.GroupID,
				"reason", strings.TrimSpace(reason),
				"path", strings.TrimSpace(process.CleanupPath),
				"attempts", cleanupAttempts,
				"error", cleanupErr,
			)
		}
		if err := processStore.MarkSessionWorkerProcessReaped(context.WithoutCancel(ctx), sessionID, store.WorkerProcessReap{
			ReapedAt: r.now().UTC(),
			Outcome:  string(outcome),
			Reason:   strings.TrimSpace(reason),
		}); err != nil {
			return fmt.Errorf("record agent session worker process reap: %w", err)
		}
	}
	return nil
}

func (r *Runner) reapWorkspaceProcessesAfterTurn(
	ctx context.Context,
	workspacePath string,
	sessionID int64,
	workAttemptID int64,
	issue connector.Issue,
	turnErr error,
	workerProcessObserved bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	combined := errors.Join(context.Cause(ctx), turnErr)
	if !workerProcessObserved && !workerSessionCanceled(combined) {
		return nil
	}

	reap := r.reapWorkspaceProcesses
	if reap == nil {
		reap = workspace.ReapProcesses
	}
	reaped, err := reap(context.WithoutCancel(ctx), workspacePath, r.workerReapGrace)
	attrs := []any{
		telemetry.WorkAttemptIDKey, workAttemptID,
		telemetry.DetentSessionIDKey, sessionID,
		"workspace_path", strings.TrimSpace(workspacePath),
		"reason", workerProcessReapReason(ctx, turnErr),
		"count", reaped,
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	r.logWorkerEventLevel(slog.LevelInfo, issue, "worker_orphan_processes_reaped", attrs...)
	if err != nil {
		return fmt.Errorf("reap orphaned workspace processes: %w", err)
	}
	return nil
}

func workerSessionCanceled(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		durationLimitError(err) ||
		errors.Is(err, ErrSessionMemoryCeilingExceeded)
}

func (r *Runner) cleanupSessionWorkerArtifacts(ctx context.Context, root string, path string) (int, error) {
	cleanup := r.cleanupWorkerArtifacts
	if cleanup == nil {
		cleanup = workspace.CleanupOwnedPath
	}
	wait := r.waitWorkerArtifactCleanup
	if wait == nil {
		wait = waitForPathRemovalRetry
	}
	return removePathWithRetry(ctx, path, func(string) error {
		return cleanup(root, path)
	}, wait)
}

func workerProcessReapReason(ctx context.Context, turnErr error) string {
	cause := context.Cause(ctx)
	combined := errors.Join(cause, turnErr)
	switch {
	case errors.Is(combined, ErrSessionDurationExceeded), errors.Is(combined, ErrMergeFallbackBudgetExceeded):
		return "maximum_session_lifetime_exceeded"
	case errors.Is(combined, ErrSessionNoProgress):
		return SessionBrakeReasonNoProgress
	case errors.Is(combined, ErrTurnDurationExceeded):
		return "maximum_turn_lifetime_exceeded"
	case errors.Is(combined, context.Canceled):
		return "session_cancelled"
	case turnErr != nil:
		return "turn_failed"
	default:
		return "turn_completed"
	}
}

func (r *Runner) updateSessionResumeState(ctx context.Context, sessionID int64, resumedFromSessionID int64, orphanRecoveryOutcome string, fallbackReason string) error {
	if sessionID <= 0 {
		return nil
	}
	resumeStore, ok := r.store.(sessionResumeStore)
	if !ok {
		return nil
	}
	return resumeStore.UpdateSessionResumeState(ctx, sessionID, store.SessionResumeState{
		ResumedFromSessionID:         resumedFromSessionID,
		OrphanRecoveryOutcome:        orphanRecoveryOutcome,
		OrphanRecoveryFallbackReason: fallbackReason,
	})
}

func (r *Runner) persistSessionIdentity(ctx context.Context, sessionID int64, identity agentidentity.Identity) error {
	if sessionID <= 0 || identity.IsZero() {
		return nil
	}
	identityStore, ok := r.store.(sessionIdentityStore)
	if !ok {
		return nil
	}
	if err := identityStore.UpdateSessionIdentity(ctx, sessionID, identity); err != nil {
		return fmt.Errorf("update agent session identity: %w", err)
	}
	return nil
}

func (r *Runner) finishSession(
	ctx context.Context,
	sessionID int64,
	started bool,
	workAttemptID int64,
	issue connector.Issue,
	startedAt time.Time,
	finishedAt time.Time,
	result RunResult,
	model string,
	backendKind string,
	turns int64,
	turnResult AgentTurnResult,
	resumedFromSessionID int64,
) error {
	if !started {
		return nil
	}
	if result.FinalState == "" {
		result.FinalState = FinalStateCompleted
	}
	requestedModel := strings.TrimSpace(model)
	resolvedModel := effectiveModel(result.RuntimeIdentity.ResolvedModel.Value, result.Model)
	usageModel := effectiveModel(resolvedModel, requestedModel)
	actualCostUSD := r.usageCostUSD(usageModel, result.Tokens.InputTokens, result.Tokens.CachedInputTokens, result.Tokens.OutputTokens, backendKind)
	projectedCostUSD, projectionOvershootUSD := budgetProjectionCosts(result.budgetProjection, actualCostUSD)

	if err := r.store.FinishSession(ctx, sessionID, store.SessionFinish{
		CompletedAt:           finishedAt,
		Turns:                 turns,
		InputTokens:           result.Tokens.InputTokens,
		CachedInputTokens:     result.Tokens.CachedInputTokens,
		OutputTokens:          result.Tokens.OutputTokens,
		ReasoningOutputTokens: result.Tokens.ReasoningOutputTokens,
		TotalTokens:           result.Tokens.TotalTokens,
		ModelContextWindow:    result.Tokens.ModelContextWindow,
		RuntimeSeconds:        int64(math.Round(result.Tokens.RuntimeSeconds)),
		FinalState:            result.FinalState,
		Model:                 resolvedModel,
		ProviderThreadID:      turnResult.ThreadID,
		ProviderSessionID:     turnResult.SessionID,
		ResumedFromSessionID:  resumedFromSessionID,
		RuntimeIdentity:       result.RuntimeIdentity,
		SkillDraftProposed:    result.SkillDraftProposed,
	}); err != nil {
		return fmt.Errorf("finish agent session: %w", err)
	}
	attrs := []any{
		telemetry.WorkAttemptIDKey, workAttemptID,
		"detent_session_id", sessionID,
		"provider_thread_id", turnResult.ThreadID,
		"provider_session_id", turnResult.SessionID,
		"final_state", result.FinalState,
		"turns", turns,
		"skill_draft_proposed", result.SkillDraftProposed,
	}
	attrs = append(attrs, runtimeIdentityLogAttrs(result.RuntimeIdentity)...)
	r.logWorkerEvent(issue, "worker_session_finished", attrs...)
	r.warnImplausibleCompletedSessionUsage(issue, workAttemptID, sessionID, turns, result)
	if projectionOvershootUSD > 0 && projectedCostUSD != nil {
		r.logWorkerEventLevel(slog.LevelWarn, issue, "worker_budget_projection_overshoot",
			telemetry.WorkAttemptIDKey, workAttemptID,
			telemetry.DetentSessionIDKey, sessionID,
			"model", usageModel,
			"estimate_source", result.budgetProjection.EstimateSource,
			"projected_cost_usd", *projectedCostUSD,
			"actual_cost_usd", actualCostUSD,
			"projection_overshoot_usd", projectionOvershootUSD,
		)
	}
	if _, err := r.store.RecordUsageEvent(ctx, store.UsageEvent{
		ProjectID:              r.projectID,
		SessionID:              sessionID,
		IssueID:                issue.ID,
		Identifier:             issue.Identifier,
		PRNumber:               pullRequestNumber(issue),
		Model:                  usageModel,
		InputTokens:            result.Tokens.InputTokens,
		CachedInputTokens:      result.Tokens.CachedInputTokens,
		OutputTokens:           result.Tokens.OutputTokens,
		ReasoningOutputTokens:  result.Tokens.ReasoningOutputTokens,
		TotalTokens:            result.Tokens.TotalTokens,
		ModelContextWindow:     result.Tokens.ModelContextWindow,
		CostUSD:                actualCostUSD,
		ProjectedCostUSD:       projectedCostUSD,
		ProjectionOvershootUSD: projectionOvershootUSD,
		RuntimeSeconds:         int64(math.Round(result.Tokens.RuntimeSeconds)),
		StartedAt:              startedAt,
		FinishedAt:             finishedAt,
		Outcome:                result.FinalState,
	}); err != nil {
		return fmt.Errorf("record usage event: %w", err)
	}
	if err := r.recordAgentSessionPhase(ctx, sessionID, issue, startedAt, finishedAt, result, backendKind); err != nil {
		return err
	}
	return nil
}

func budgetProjectionCosts(projection *dispatchBudgetProjection, actualCostUSD float64) (*float64, float64) {
	if projection == nil {
		return nil, 0
	}
	projectedCostUSD := max(0, projection.CostUSD)
	return &projectedCostUSD, max(0, actualCostUSD-projectedCostUSD)
}

func (r *Runner) warnImplausibleCompletedSessionUsage(issue connector.Issue, workAttemptID int64, sessionID int64, turns int64, result RunResult) {
	runtimeSeconds := int64(math.Round(result.Tokens.RuntimeSeconds))
	if result.FinalState != FinalStateCompleted || turns <= 0 || runtimeSeconds < implausibleUsageRuntimeSeconds || result.Tokens.OutputTokens >= implausibleUsageOutputTokens {
		return
	}
	r.logWorkerEventLevel(slog.LevelWarn, issue, "worker_session_usage_implausible",
		telemetry.WorkAttemptIDKey, workAttemptID,
		telemetry.DetentSessionIDKey, sessionID,
		"turns", turns,
		"runtime_seconds", runtimeSeconds,
		"input_tokens", result.Tokens.InputTokens,
		"cached_input_tokens", result.Tokens.CachedInputTokens,
		"output_tokens", result.Tokens.OutputTokens,
		"reasoning_output_tokens", result.Tokens.ReasoningOutputTokens,
		"total_tokens", result.Tokens.TotalTokens,
		"minimum_runtime_seconds", implausibleUsageRuntimeSeconds,
		"output_token_threshold", implausibleUsageOutputTokens,
	)
}

func (r *Runner) recordAgentSessionPhase(
	ctx context.Context,
	sessionID int64,
	issue connector.Issue,
	startedAt time.Time,
	finishedAt time.Time,
	result RunResult,
	backendKind string,
) error {
	phaseStore, ok := r.store.(workflowPhaseStore)
	if !ok {
		return nil
	}
	if result.FinalState == "" {
		result.FinalState = FinalStateCompleted
	}
	endpointFamily := strings.TrimSpace(backendKind)
	if _, err := phaseStore.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:             r.projectID,
		SessionID:             sessionID,
		IssueID:               issue.ID,
		Identifier:            issue.Identifier,
		IssueURL:              issue.URL,
		PRNumber:              pullRequestNumber(issue),
		PhaseType:             store.WorkflowPhaseTypeAgentSession,
		PhaseName:             "agent_active",
		Status:                result.FinalState,
		StartedAt:             startedAt,
		FinishedAt:            finishedAt,
		DurationSeconds:       int64(math.Round(result.Tokens.RuntimeSeconds)),
		Turns:                 1,
		InputTokens:           result.Tokens.InputTokens,
		CachedInputTokens:     result.Tokens.CachedInputTokens,
		OutputTokens:          result.Tokens.OutputTokens,
		ReasoningOutputTokens: result.Tokens.ReasoningOutputTokens,
		TotalTokens:           result.Tokens.TotalTokens,
		ModelContextWindow:    result.Tokens.ModelContextWindow,
		EndpointFamily:        endpointFamily,
	}); err != nil {
		return fmt.Errorf("record agent session phase: %w", err)
	}
	return nil
}

func (r *Runner) usageCostUSD(model string, inputTokens int64, cachedInputTokens int64, outputTokens int64, backendKind string) float64 {
	model = strings.TrimSpace(model)
	if model == "" {
		r.logger.Debug("usage event model unavailable; using fallback pricing", "backend_kind", strings.TrimSpace(backendKind))
	}
	cost, ok := budget.UsageCostUSD(r.pricing, model, inputTokens, cachedInputTokens, outputTokens)
	if !ok {
		r.logger.Warn("usage event model pricing not found", "model", model, "backend_kind", strings.TrimSpace(backendKind))
		return 0
	}
	return cost
}

type sessionTokenCeiling struct {
	tokens             int64
	source             string
	modelContextWindow int64
	contextMultiplier  float64
}

func (r *Runner) enforceSessionTokenCeiling(cfg config.Agent, issue connector.Issue, workspacePath string, update AgentUpdate, eventAt time.Time) error {
	if update.Type != AgentUpdateTokenUsage || sessionTokenCeilingBypassed(cfg, issue) {
		return nil
	}
	ceiling, ok := sessionTokenCeilingForUsage(cfg, update.Tokens)
	observedTokens := sessionTokenCeilingObservedTokens(ceiling, update.Tokens)
	if !ok || observedTokens <= ceiling.tokens {
		return nil
	}

	err := &SessionTokenCeilingError{
		TotalTokens:        observedTokens,
		CeilingTokens:      ceiling.tokens,
		Source:             ceiling.source,
		ModelContextWindow: ceiling.modelContextWindow,
		ContextMultiplier:  ceiling.contextMultiplier,
	}
	if appendErr := appendSessionTokenCeilingLesson(cfg.Lessons, issue, workspacePath, err, eventAt); appendErr != nil {
		r.logger.Warn("session token ceiling lesson append failed", "error", appendErr)
		return errors.Join(err, appendErr)
	}
	return err
}

func (r *Runner) enforceSessionBudgetProjection(
	projection *dispatchBudgetProjection,
	costOffsetUSD float64,
	model string,
	backendKind string,
	update AgentUpdate,
) error {
	if projection == nil || projection.EstimateSource != budget.EstimateSourceHistorical || projection.CostUSD <= 0 || update.Type != AgentUpdateTokenUsage {
		return nil
	}
	observedCostUSD := costOffsetUSD + r.usageCostUSD(
		model,
		update.Tokens.InputTokens,
		update.Tokens.CachedInputTokens,
		update.Tokens.OutputTokens,
		backendKind,
	)
	if observedCostUSD <= projection.CostUSD {
		return nil
	}
	return &SessionBudgetProjectionError{
		ObservedCostUSD:  observedCostUSD,
		ProjectedCostUSD: projection.CostUSD,
		Model:            model,
		EstimateSource:   projection.EstimateSource,
	}
}

func sessionTokenCeilingForUsage(cfg config.Agent, tokens AgentTokenUsage) (sessionTokenCeiling, bool) {
	candidates := make([]sessionTokenCeiling, 0, 2)
	if cfg.MaxSessionTokens > 0 {
		candidates = append(candidates, sessionTokenCeiling{
			tokens: cfg.MaxSessionTokens,
			source: TokenCeilingSourceAbsolute,
		})
	}
	if cfg.MaxSessionContextMultiplier > 0 && tokens.ModelContextWindow != nil && *tokens.ModelContextWindow > 0 {
		limit := int64(math.Ceil(float64(*tokens.ModelContextWindow) * cfg.MaxSessionContextMultiplier))
		if limit > 0 {
			candidates = append(candidates, sessionTokenCeiling{
				tokens:             limit,
				source:             TokenCeilingSourceContextWindow,
				modelContextWindow: *tokens.ModelContextWindow,
				contextMultiplier:  cfg.MaxSessionContextMultiplier,
			})
		}
	}
	if len(candidates) == 0 {
		return sessionTokenCeiling{}, false
	}
	selected := candidates[0]
	selectedBreached := sessionTokenCeilingObservedTokens(selected, tokens) > selected.tokens
	for _, candidate := range candidates[1:] {
		breached := sessionTokenCeilingObservedTokens(candidate, tokens) > candidate.tokens
		if breached && !selectedBreached || breached == selectedBreached && candidate.tokens < selected.tokens {
			selected = candidate
			selectedBreached = breached
		}
	}
	return selected, true
}

func sessionTokenCeilingObservedTokens(ceiling sessionTokenCeiling, tokens AgentTokenUsage) int64 {
	if ceiling.source == TokenCeilingSourceContextWindow && tokens.Last != nil {
		return tokens.Last.TotalTokens
	}
	return tokens.TotalTokens
}

func sessionTokenCeilingBypassed(cfg config.Agent, issue connector.Issue) bool {
	if label := strings.TrimSpace(cfg.MaxSessionTokenOverrideLabel); label != "" && issueHasLabel(issue, label) {
		return true
	}
	if field := strings.TrimSpace(cfg.MaxSessionTokenOverrideField); field != "" {
		value, ok := issueFieldValue(issue.Fields, field)
		return ok && tokenCeilingOverrideEnabled(value)
	}
	return false
}

func issueHasLabel(issue connector.Issue, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, label := range issue.Labels {
		if strings.ToLower(strings.TrimSpace(label)) == want {
			return true
		}
	}
	return false
}

func tokenCeilingOverrideEnabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on", "allow", "allowed", "bypass", "disabled":
		return true
	default:
		return false
	}
}

func appendSessionTokenCeilingLesson(cfg config.Lessons, issue connector.Issue, workspacePath string, ceilingErr *SessionTokenCeilingError, eventAt time.Time) error {
	if strings.TrimSpace(workspacePath) == "" {
		return nil
	}
	path := cfg.Path
	if strings.TrimSpace(path) == "" {
		path = lessons.DefaultPath
	}
	lessonPath, err := promptWorkspaceRelativePath(workspacePath, path)
	if err != nil {
		return err
	}
	return lessons.Append(lessonPath, lessons.Entry{
		IssueNumber: githubIssueNumber(issue.Identifier),
		IssueRef:    issue.Identifier,
		Title:       issue.Title,
		FailureKind: FinalStateTokenCeilingExceeded,
		Symptom:     fmt.Sprintf("session reached %d tokens, above configured ceiling %d", ceilingErr.TotalTokens, ceilingErr.CeilingTokens),
		Hypothesis:  "the agent session is consuming tokens faster than the configured per-session ceiling permits",
		Hint:        "retry with a narrower task split, stronger stop conditions, or a deliberate per-issue token ceiling override",
	}, lessons.AppendOptions{Date: eventAt.UTC(), MaxEntries: cfg.MaxEntries})
}

func finalStateForTurnError(err error) string {
	if errors.Is(err, ErrOperatorStopped) {
		return FinalStateOperatorStopped
	}
	if errors.Is(err, ErrMergeRevoked) {
		return FinalStateMergeRevoked
	}
	if errors.Is(err, ErrLaneRevoked) {
		return FinalStateLaneRevoked
	}
	if errors.Is(err, ErrCIUnavailable) {
		return FinalStateCIUnavailable
	}
	if errors.Is(err, ErrMergeWorkerDurationExceeded) {
		return FinalStateMergeDurationExceeded
	}
	if errors.Is(err, ErrMergeFallbackBudgetExceeded) {
		return FinalStateMergeFallbackExceeded
	}
	if errors.Is(err, ErrSessionTokenCeilingExceeded) {
		return FinalStateTokenCeilingExceeded
	}
	if errors.Is(err, ErrSessionBudgetProjectionExceeded) {
		return FinalStateBudgetProjectionExceeded
	}
	if errors.Is(err, ErrSessionMemoryCeilingExceeded) {
		return FinalStateMemoryCeilingExceeded
	}
	if errors.Is(err, ErrSessionDurationExceeded) {
		return FinalStateSessionDurationExceeded
	}
	if errors.Is(err, ErrSessionTurnLimitExceeded) {
		return FinalStateTurnLimitExceeded
	}
	if errors.Is(err, ErrSessionNoProgress) {
		return FinalStateNoProgress
	}
	return FinalStateFailed
}

func agentTurnCleanupError(err error, result AgentTurnResult) error {
	if err == nil || !errors.Is(err, ErrAgentTurnCleanup) {
		return nil
	}
	if strings.TrimSpace(result.ThreadID) == "" || strings.TrimSpace(result.TurnID) == "" {
		return nil
	}
	return err
}

func durationFromMillis(ms int) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func effectiveModel(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func workspaceIssue(projectID string, issue connector.Issue) workspace.Issue {
	baseRef := ""
	if issue.PullRequest != nil {
		baseRef = strings.TrimSpace(issue.PullRequest.BaseSHA)
	}
	return workspace.Issue{
		ProjectID:          projectID,
		ID:                 issue.ID,
		Identifier:         issue.Identifier,
		BranchName:         issue.BranchName,
		BaseRef:            baseRef,
		PullRequestHeadSHA: pullRequestHeadSHA(issue.PullRequest),
	}
}

func pullRequestHeadSHA(pullRequest *connector.PullRequest) string {
	if pullRequest == nil {
		return ""
	}
	return strings.TrimSpace(pullRequest.HeadSHA)
}

func applyAgentUpdate(result *RunResult, update AgentUpdate) {
	if !update.RuntimeIdentity.IsZero() {
		result.RuntimeIdentity = result.RuntimeIdentity.Merge(update.RuntimeIdentity)
	}
	if model := strings.TrimSpace(update.Model); model != "" {
		result.Model = model
		if result.RuntimeIdentity.ResolvedModel.Value == "" {
			result.RuntimeIdentity = result.RuntimeIdentity.Merge(agentidentity.RuntimeUpdate(model, "", "", "", time.Time{}))
		}
	}
	switch update.Type {
	case AgentUpdateTokenUsage:
		result.Tokens.InputTokens = update.Tokens.InputTokens
		result.Tokens.CachedInputTokens = update.Tokens.CachedInputTokens
		result.Tokens.OutputTokens = update.Tokens.OutputTokens
		result.Tokens.ReasoningOutputTokens = update.Tokens.ReasoningOutputTokens
		result.Tokens.TotalTokens = update.Tokens.TotalTokens
		result.Tokens.Last = cloneAgentTokenCounts(update.Tokens.Last)
		result.Tokens.ModelContextWindow = update.Tokens.ModelContextWindow
	case AgentUpdateRateLimits:
		result.RateLimits = mergeAgentRateLimits(result.RateLimits, update.RateLimits)
	}
}

type sessionTokenUsage struct {
	resumed  bool
	baseline *AgentTokenCounts
}

func newSessionTokenUsage(resumed bool) *sessionTokenUsage {
	return &sessionTokenUsage{resumed: resumed}
}

func (s *sessionTokenUsage) normalize(tokens AgentTokenUsage) AgentTokenUsage {
	if tokens.ThreadTotal == nil {
		return tokens
	}
	threadTotal := *tokens.ThreadTotal
	if !s.resumed {
		return agentTokenUsageWithCounts(tokens, threadTotal)
	}
	if s.baseline == nil {
		baseline := AgentTokenCounts{}
		if tokens.Last != nil {
			baseline = subtractAgentTokenCounts(threadTotal, *tokens.Last)
		}
		s.baseline = &baseline
	}
	return agentTokenUsageWithCounts(tokens, subtractAgentTokenCounts(threadTotal, *s.baseline))
}

func agentTokenUsageWithCounts(tokens AgentTokenUsage, counts AgentTokenCounts) AgentTokenUsage {
	tokens.InputTokens = counts.InputTokens
	tokens.CachedInputTokens = counts.CachedInputTokens
	tokens.OutputTokens = counts.OutputTokens
	tokens.ReasoningOutputTokens = counts.ReasoningOutputTokens
	tokens.TotalTokens = counts.TotalTokens
	return tokens
}

func subtractAgentTokenCounts(total AgentTokenCounts, baseline AgentTokenCounts) AgentTokenCounts {
	return AgentTokenCounts{
		InputTokens:           max(0, total.InputTokens-baseline.InputTokens),
		CachedInputTokens:     max(0, total.CachedInputTokens-baseline.CachedInputTokens),
		OutputTokens:          max(0, total.OutputTokens-baseline.OutputTokens),
		ReasoningOutputTokens: max(0, total.ReasoningOutputTokens-baseline.ReasoningOutputTokens),
		TotalTokens:           max(0, total.TotalTokens-baseline.TotalTokens),
	}
}

func cloneAgentTokenCounts(tokens *AgentTokenCounts) *AgentTokenCounts {
	if tokens == nil {
		return nil
	}
	clone := *tokens
	return &clone
}

type agentRunProgress struct {
	sessionID                 string
	processIdentity           string
	workerProcess             procgroup.Identity
	rssBytes                  uint64
	rssCeilingBytes           uint64
	rssObservedAt             time.Time
	turnIDs                   map[string]struct{}
	messages                  map[string]*runtimeoutput.Buffer
	messageOrder              []string
	outputPolicy              runtimeoutput.Policy
	output                    *runtimeoutput.Buffer
	lastEventAt               time.Time
	lastEvent                 string
	lastMessage               string
	lastMessageTruncation     *runtimeoutput.Truncation
	recentEvents              []telemetry.ActivityEvent
	diffStats                 DiffStats
	diffStatsCollected        bool
	diffStatsCheckedAt        time.Time
	toolInvocations           map[string]deliverableToolInvocation
	deliverableFailures       map[string]error
	deliverableSuccesses      map[string]bool
	ciTriggerLabel            string
	ciTriggerRepository       string
	ciTriggerPRNumber         int
	successfulPushes          int
	ciTriggerPushSequence     int
	ciTriggerLabelValid       bool
	deliverableRecoveryBranch string
	turnOffset                int
}

func newAgentRunProgress(outputPolicy runtimeoutput.Policy, ciTriggerLabel string, ciTriggerRepository string, ciTriggerPRNumber int, deliverableRecoveryBranch string, turnOffset int) *agentRunProgress {
	return &agentRunProgress{
		turnIDs:                   map[string]struct{}{},
		messages:                  map[string]*runtimeoutput.Buffer{},
		outputPolicy:              outputPolicy,
		output:                    runtimeoutput.NewBuffer(outputPolicy),
		toolInvocations:           map[string]deliverableToolInvocation{},
		deliverableFailures:       map[string]error{},
		deliverableSuccesses:      map[string]bool{},
		ciTriggerLabel:            strings.TrimSpace(ciTriggerLabel),
		ciTriggerRepository:       strings.TrimSpace(ciTriggerRepository),
		ciTriggerPRNumber:         ciTriggerPRNumber,
		deliverableRecoveryBranch: strings.TrimSpace(deliverableRecoveryBranch),
		turnOffset:                max(turnOffset, 0),
	}
}

func (p *agentRunProgress) apply(update AgentUpdate, eventAt time.Time) {
	if update.ProcessIdentity != "" {
		p.processIdentity = update.ProcessIdentity
	}
	if update.WorkerProcess.PID > 0 && !update.WorkerProcess.StartedAt.IsZero() {
		p.workerProcess = update.WorkerProcess
	}
	if update.Type == AgentUpdateResourceUsage {
		p.rssBytes = update.RSSBytes
		p.rssCeilingBytes = update.RSSCeilingBytes
		p.rssObservedAt = eventAt.UTC()
		return
	}
	if update.TurnID != "" && !update.AuxiliaryTurn {
		p.turnIDs[update.TurnID] = struct{}{}
		if update.ThreadID != "" {
			p.sessionID = update.ThreadID + "-" + update.TurnID
		}
	}
	if update.Type != "" {
		p.lastEvent = string(update.Type)
	} else {
		p.lastEvent = update.Method
	}
	p.lastEventAt = eventAt.UTC()

	eventMessage := ""
	eventTruncation := (*runtimeoutput.Truncation)(nil)
	switch update.Type {
	case AgentUpdateMessageDelta:
		key := update.ItemID
		if key == "" {
			key = update.TurnID
		}
		message, ok := p.messages[key]
		if !ok {
			p.messageOrder = append(p.messageOrder, key)
			message = runtimeoutput.NewBuffer(p.outputPolicy)
			p.messages[key] = message
		}
		message.Append(update.Delta)
		if p.output != nil {
			p.output.Append(update.Delta)
		}
		text := message.Text()
		p.lastMessage = strings.TrimSpace(text.Value)
		p.lastMessageTruncation = runtimeoutput.CloneTruncation(text.Truncation)
		eventMessage = p.lastMessage
		eventTruncation = runtimeoutput.CloneTruncation(text.Truncation)
	case AgentUpdateTurnStarted:
		p.lastMessageTruncation = nil
		p.lastMessage = "turn started"
		eventMessage = p.lastMessage
	case AgentUpdateTurnCompleted:
		p.lastMessageTruncation = nil
		status := update.Status
		if status == "" {
			status = "completed"
		}
		p.lastMessage = "turn " + status
		eventMessage = p.lastMessage
	case AgentUpdateTokenUsage:
		eventMessage = tokenUsageActivityMessage(update.Tokens)
	case AgentUpdateRateLimits:
		eventMessage = rateLimitsActivityMessage(update.RateLimits)
	case AgentUpdateProcessStarted:
		if p.processIdentity != "" {
			eventMessage = "process " + p.processIdentity + " started"
		} else {
			eventMessage = "process started"
		}
	case AgentUpdateModelUpdated:
		eventMessage = modelUpdatedActivityMessage(update.Model)
	case AgentUpdateRuntimeIdentity:
		eventMessage = "agent route selected"
	case AgentUpdateToolStarted:
		p.recordDeliverableToolStart(update)
	case AgentUpdateToolOutput:
		p.recordDeliverableToolOutput(update)
	case AgentUpdateToolCompleted:
		p.recordDeliverableToolCompletion(update)
	case AgentUpdateMCPElicitation:
		eventMessage = update.Delta
	}

	p.addRecentEvent(telemetry.ActivityEvent{
		At:         p.lastEventAt,
		Event:      p.lastEvent,
		Message:    eventMessage,
		Truncation: eventTruncation,
	})
}

type deliverableToolInvocation struct {
	class                 string
	command               string
	tool                  string
	output                string
	ciTriggerLabelMatches bool
	ciTriggerAfterPush    bool
}

func (p *agentRunProgress) recordDeliverableToolOutput(update AgentUpdate) {
	key := deliverableToolKey(update)
	invocation, ok := p.toolInvocations[key]
	if !ok {
		return
	}
	invocation.output = appendDeliverableDetail(invocation.output, update.Delta)
	p.toolInvocations[key] = invocation
}

func (p *agentRunProgress) recordDeliverableToolStart(update AgentUpdate) {
	command := strings.TrimSpace(update.Command)
	if command == "" {
		command = update.Delta
	}
	invocation := newDeliverableToolInvocation(update.Tool, command, p.ciTriggerLabel, p.ciTriggerRepository, p.ciTriggerPRNumber, p.deliverableRecoveryBranch)
	if invocation.class == "" {
		return
	}
	p.toolInvocations[deliverableToolKey(update)] = invocation
}

func (p *agentRunProgress) recordDeliverableToolCompletion(update AgentUpdate) {
	key := deliverableToolKey(update)
	invocation, ok := p.toolInvocations[key]
	if !ok {
		command := strings.TrimSpace(update.Command)
		if command == "" {
			command = update.Delta
		}
		invocation = newDeliverableToolInvocation(update.Tool, command, p.ciTriggerLabel, p.ciTriggerRepository, p.ciTriggerPRNumber, p.deliverableRecoveryBranch)
	}
	if invocation.class == "" {
		return
	}
	delete(p.toolInvocations, key)
	if deliverableToolStatusSucceeded(update.Status) {
		switch invocation.class {
		case "push":
			delete(p.deliverableFailures, "push")
			p.deliverableSuccesses["push"] = true
			p.deliverableSuccesses["forge_write"] = true
			if gitPushChanged(update, invocation.command, invocation.output) {
				p.successfulPushes++
				p.ciTriggerLabelValid = false
			}
		case "ci_trigger_label":
			delete(p.deliverableFailures, "ci_trigger_label")
			p.deliverableSuccesses["ci_trigger_label"] = true
			p.ciTriggerPushSequence = p.successfulPushes
			p.ciTriggerLabelValid = invocation.ciTriggerLabelMatches
		case "push_ci_trigger_label":
			delete(p.deliverableFailures, "push")
			delete(p.deliverableFailures, "ci_trigger_label")
			p.deliverableSuccesses["push"] = true
			p.deliverableSuccesses["ci_trigger_label"] = true
			p.deliverableSuccesses["forge_write"] = true
			if gitPushChanged(update, invocation.command, invocation.output) {
				p.successfulPushes++
			}
			p.ciTriggerPushSequence = p.successfulPushes
			p.ciTriggerLabelValid = invocation.ciTriggerLabelMatches && invocation.ciTriggerAfterPush
		case "pull_request":
			delete(p.deliverableFailures, "pull_request")
			p.deliverableSuccesses["pull_request"] = true
			p.deliverableSuccesses["forge_write"] = true
		case "pull_request_lookup":
			if pullRequestLookupAdopts(update, p.deliverableRecoveryBranch) {
				delete(p.deliverableFailures, "pull_request")
				p.deliverableSuccesses["pull_request"] = true
			}
		}
		return
	}
	if !deliverableToolStatusFailed(update.Status) {
		return
	}
	failureClass := invocation.class
	if invocation.class == "push_ci_trigger_label" {
		failureClass = "push"
	}
	if invocation.class == "pull_request_lookup" {
		failureClass = "pull_request"
	}
	delete(p.deliverableSuccesses, failureClass)
	if invocation.class == "ci_trigger_label" || invocation.class == "push_ci_trigger_label" {
		p.ciTriggerLabelValid = false
	}
	message := meaningfulDeliverableDetail(invocation.output)
	if message == "" {
		message = meaningfulDeliverableDetail(update.BackendErrorMessage)
	}
	body := meaningfulDeliverableDetail(update.BackendErrorBody)
	if message == "" && strings.TrimSpace(update.Delta) != strings.TrimSpace(invocation.command) {
		message = meaningfulDeliverableDetail(update.Delta)
	}
	if body == "" && strings.TrimSpace(update.Delta) != strings.TrimSpace(invocation.command) && strings.HasPrefix(strings.TrimSpace(update.Delta), "{") {
		body = meaningfulDeliverableDetail(update.Delta)
	}
	p.deliverableFailures[failureClass] = &DeliverableCommandError{
		OperationClass: failureClass,
		Operation:      deliverableInvocationOperation(invocation),
		Arguments:      deliverableInvocationArguments(invocation),
		ItemID:         strings.TrimSpace(update.ItemID),
		Command:        strings.TrimSpace(invocation.command),
		Status:         strings.TrimSpace(update.Status),
		ExitCode:       cloneIntPointer(update.ExitCode),
		Message:        truncateDeliverableDetail(message),
		Body:           truncateDeliverableDetail(body),
	}
}

func (p *agentRunProgress) pullRequestUpdated() bool {
	return p.deliverableSuccesses["pull_request"]
}

func (p *agentRunProgress) deliverableFailure(itemID string, class string) *DeliverableCommandError {
	var deliverableErr *DeliverableCommandError
	if !errors.As(p.deliverableFailures[strings.TrimSpace(class)], &deliverableErr) || deliverableErr == nil || strings.TrimSpace(deliverableErr.ItemID) != strings.TrimSpace(itemID) {
		return nil
	}
	return deliverableErr
}

func (p *agentRunProgress) pullRequestHeadPushed() bool {
	return p.successfulPushes > 0
}

func (p *agentRunProgress) ciTriggerLabelReapplied() bool {
	return p.successfulPushes > 0 && p.ciTriggerLabelValid && p.ciTriggerPushSequence == p.successfulPushes
}

func (p *agentRunProgress) forgeWriteCompleted() bool {
	return p.deliverableSuccesses["forge_write"]
}

func (p *agentRunProgress) deliverableError() error {
	errs := make([]error, 0, len(p.deliverableFailures))
	for _, class := range []string{"pull_request", "push"} {
		if err := p.deliverableFailures[class]; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 && p.deliverableRecoveryBranch != "" && !p.deliverableSuccesses["pull_request"] {
		errs = append(errs, &DeliverableCommandError{
			OperationClass: "pull_request",
			Operation:      "pull request recovery",
			Arguments:      `{"head":` + strconv.Quote(p.deliverableRecoveryBranch) + `}`,
			Status:         "failed",
			Message:        "recovery turn completed without creating or adopting a pull request",
		})
	}
	return errors.Join(errs...)
}

func deliverableToolKey(update AgentUpdate) string {
	if itemID := strings.TrimSpace(update.ItemID); itemID != "" {
		return itemID
	}
	return strings.TrimSpace(update.TurnID) + "\x00" + strings.TrimSpace(update.Tool)
}

func deliverableOperationClass(tool string, command string, deliverableRecoveryBranch string) string {
	lowerTool := strings.ToLower(strings.TrimSpace(tool))
	lowerCommand := strings.ToLower(strings.TrimSpace(command))
	push := gitPushCommand(lowerCommand)
	ciTrigger := ciTriggerLabelCommand(lowerCommand)
	if push && ciTrigger {
		return "push_ci_trigger_label"
	}
	if ciTrigger {
		return "ci_trigger_label"
	}
	if push {
		return "push"
	}
	if pullRequestLookupCommand(lowerTool, lowerCommand, deliverableRecoveryBranch) {
		return "pull_request_lookup"
	}
	if pullRequestCommand(lowerCommand) ||
		(strings.Contains(lowerTool, "pull_request") &&
			(strings.Contains(lowerTool, "create") || strings.Contains(lowerTool, "update") || strings.Contains(lowerTool, "edit"))) {
		return "pull_request"
	}
	return ""
}

func newDeliverableToolInvocation(tool string, command string, ciTriggerLabel string, ciTriggerRepository string, ciTriggerPRNumber int, deliverableRecoveryBranch string) deliverableToolInvocation {
	command = strings.TrimSpace(command)
	return deliverableToolInvocation{
		class:                 deliverableOperationClass(tool, command, deliverableRecoveryBranch),
		command:               command,
		tool:                  strings.TrimSpace(tool),
		ciTriggerLabelMatches: ciTriggerLabelCommandMatches(command, ciTriggerLabel, ciTriggerRepository, ciTriggerPRNumber),
		ciTriggerAfterPush:    ciTriggerLabelRunsAfterPush(command),
	}
}

func pullRequestLookupCommand(tool string, arguments string, branch string) bool {
	branch = strings.ToLower(strings.TrimSpace(branch))
	if branch == "" || !strings.Contains(arguments, branch) {
		return false
	}
	if strings.Contains(tool, "list_pull_requests") {
		return true
	}
	return strings.Contains(tool, "search_issues") && (strings.Contains(arguments, "is:pr") || strings.Contains(arguments, "pull_request"))
}

func pullRequestLookupAdopts(update AgentUpdate, branch string) bool {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return false
	}
	for _, raw := range []string{update.Delta, update.BackendErrorBody, update.BackendErrorMessage} {
		var payload any
		if json.Unmarshal([]byte(strings.TrimSpace(raw)), &payload) == nil && pullRequestPayloadMatchesHead(payload, branch) {
			return true
		}
	}
	return false
}

func pullRequestPayloadMatchesHead(payload any, branch string) bool {
	switch value := payload.(type) {
	case []any:
		for _, item := range value {
			if pullRequestPayloadMatchesHead(item, branch) {
				return true
			}
		}
	case map[string]any:
		if pullRequestObjectMatchesHead(value, branch) {
			return true
		}
		for _, nested := range value {
			if pullRequestPayloadMatchesHead(nested, branch) {
				return true
			}
		}
	}
	return false
}

func pullRequestObjectMatchesHead(value map[string]any, branch string) bool {
	if !pullRequestObject(value) {
		return false
	}
	for _, key := range []string{"head_ref_name", "headRefName", "head_branch", "headBranch", "source_branch", "sourceBranch"} {
		if stringValue(value[key]) == branch {
			return true
		}
	}
	switch head := value["head"].(type) {
	case string:
		return strings.TrimSpace(head) == branch
	case map[string]any:
		for _, key := range []string{"ref", "name", "branch"} {
			if stringValue(head[key]) == branch {
				return true
			}
		}
	}
	return false
}

func pullRequestObject(value map[string]any) bool {
	for _, key := range []string{"url", "html_url", "htmlUrl"} {
		if strings.Contains(strings.ToLower(stringValue(value[key])), "/pull/") {
			return true
		}
	}
	if strings.EqualFold(stringValue(value["type"]), "pull_request") || strings.EqualFold(stringValue(value["kind"]), "pull_request") {
		return true
	}
	_, ok := value["pull_request"]
	return ok
}

func stringValue(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func deliverableInvocationOperation(invocation deliverableToolInvocation) string {
	tool := strings.TrimSpace(invocation.tool)
	lowerTool := strings.ToLower(tool)
	if strings.Contains(lowerTool, "pull_request") || strings.Contains(lowerTool, "search_issues") || strings.Contains(lowerTool, "list_pull_requests") {
		return tool
	}
	lowerCommand := strings.ToLower(invocation.command)
	for _, operation := range []string{"gh pr create", "gh pr edit", "gh pr ready", "git push", "detent ci-trigger-label"} {
		if strings.Contains(lowerCommand, operation) {
			return operation
		}
	}
	if tool != "" {
		return tool
	}
	return "unknown"
}

func meaningfulDeliverableDetail(value string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "null") {
		return ""
	}
	return value
}

func truncateDeliverableDetail(value string) string {
	value = meaningfulDeliverableDetail(value)
	if len(value) > 4096 {
		return value[len(value)-4096:]
	}
	return value
}

func appendDeliverableDetail(current string, delta string) string {
	return truncateDeliverableDetail(current + delta)
}

func deliverableInvocationArguments(invocation deliverableToolInvocation) string {
	command := strings.TrimSpace(invocation.command)
	if json.Valid([]byte(command)) {
		return truncateDeliverableDetail(command)
	}
	return deliverableInvocationOperation(invocation)
}

func ciTriggerLabelCommand(command string) bool {
	_, ok := ciTriggerLabelCommandFields(command)
	return ok
}

func ciTriggerLabelCommandMatches(command string, label string, repository string, pullRequestNumber int) bool {
	label = strings.TrimSpace(label)
	repository = strings.TrimSpace(repository)
	fields, ok := ciTriggerLabelCommandFields(command)
	if label == "" || repository == "" || pullRequestNumber <= 0 || !ok {
		return false
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(label))
	labelValue, labelPresent := commandFlagValue(fields, "--label")
	encodedLabelValue, encodedLabelPresent := commandFlagValue(fields, "--label-base64")
	repositoryValue, repositoryPresent := commandFlagValue(fields, "--repository")
	pullRequestValue, pullRequestPresent := commandFlagValue(fields, "--pull-request")
	labelMatches := labelPresent && strings.EqualFold(labelValue, label)
	if encodedLabelPresent {
		labelMatches = encodedLabelValue == encoded
	}
	return labelMatches &&
		repositoryPresent && strings.EqualFold(repositoryValue, repository) &&
		pullRequestPresent && pullRequestValue == strconv.Itoa(pullRequestNumber)
}

func ciTriggerLabelRunsAfterPush(command string) bool {
	fields := strings.Fields(command)
	ciTriggerIndex := ciTriggerLabelCommandIndex(fields)
	pushIndexes := gitPushCommandIndexes(command)
	if ciTriggerIndex < 0 || len(pushIndexes) == 0 {
		return false
	}
	pushIndex := pushIndexes[len(pushIndexes)-1]
	return pushIndex >= 0 && pushIndex < ciTriggerIndex
}

func ciTriggerLabelCommandFields(command string) ([]string, bool) {
	fields := strings.Fields(command)
	index := ciTriggerLabelCommandIndex(fields)
	if index < 0 {
		return nil, false
	}
	return fields[index+2:], true
}

func ciTriggerLabelCommandIndex(fields []string) int {
	index := -1
	for fieldIndex := 1; fieldIndex < len(fields); fieldIndex++ {
		executable := strings.ToLower(strings.Trim(fields[fieldIndex-1], "'\";()"))
		subcommand := strings.ToLower(strings.Trim(fields[fieldIndex], "'\";()"))
		if (executable == "detent" || strings.HasSuffix(executable, "/detent")) && subcommand == "ci-trigger-label" {
			index = fieldIndex - 1
		}
	}
	return index
}

func commandFlagValue(fields []string, flag string) (string, bool) {
	for index, field := range fields {
		field = strings.Trim(field, "'\";()")
		switch {
		case strings.EqualFold(field, flag) && index+1 < len(fields):
			return strings.Trim(fields[index+1], "'\";()"), true
		case strings.HasPrefix(strings.ToLower(field), strings.ToLower(flag)+"="):
			return strings.Trim(field[len(flag)+1:], "'\";()"), true
		}
	}
	return "", false
}

func gitPushChanged(update AgentUpdate, command string, streamedOutput string) bool {
	if gitPushCommandCount(command) != 1 {
		return true
	}
	output := strings.ToLower(strings.Join([]string{streamedOutput, update.Delta, update.BackendErrorMessage, update.BackendErrorBody}, " "))
	return !strings.Contains(output, "everything up-to-date") && !strings.Contains(output, "everything up to date")
}

func gitPushCommand(command string) bool {
	return gitPushCommandCount(command) > 0
}

func commandAfterLastGitPush(command string) string {
	segments := shellCommandSegments(command)
	lastPush := -1
	for index, segment := range segments {
		if gitPushCommand(segment) {
			lastPush = index
		}
	}
	if lastPush < 0 || lastPush == len(segments)-1 {
		return ""
	}
	return strings.Join(segments[lastPush+1:], " && ")
}

func shellCommandSegments(command string) []string {
	segments := make([]string, 0, 4)
	start := 0
	quote := byte(0)
	escaped := false
	appendSegment := func(end int) {
		if segment := strings.TrimSpace(command[start:end]); segment != "" {
			segments = append(segments, segment)
		}
	}
	for index := 0; index < len(command); index++ {
		current := command[index]
		if escaped {
			escaped = false
			continue
		}
		if current == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if current == quote {
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' {
			quote = current
			continue
		}
		separatorLength := 0
		switch current {
		case ';', '\n':
			separatorLength = 1
		case '&':
			if index+1 < len(command) && command[index+1] == '&' {
				separatorLength = 2
			}
		case '|':
			separatorLength = 1
			if index+1 < len(command) && command[index+1] == '|' {
				separatorLength = 2
			}
		}
		if separatorLength == 0 {
			continue
		}
		appendSegment(index)
		index += separatorLength - 1
		start = index + 1
	}
	appendSegment(len(command))
	return segments
}

func gitPushCommandCount(command string) int {
	return len(gitPushCommandIndexes(command))
}

func gitPushCommandIndexes(command string) []int {
	fields := strings.Fields(command)
	indexes := make([]int, 0, 1)
	for index, field := range fields {
		field = strings.Trim(field, "'\";()")
		if field != "git" {
			continue
		}
		for candidateIndex, candidate := range fields[index+1:] {
			candidate = strings.Trim(candidate, "'\";()")
			if candidate == "push" {
				indexes = append(indexes, index+candidateIndex+1)
				break
			}
			if candidate == "git" || strings.ContainsAny(candidate, "|&") {
				break
			}
		}
	}
	return indexes
}

func pullRequestCommand(command string) bool {
	for _, operation := range []string{"gh pr create", "gh pr edit", "gh pr ready"} {
		if strings.Contains(command, operation) {
			return true
		}
	}
	return strings.Contains(command, "gh api") &&
		strings.Contains(command, "/pulls") &&
		(strings.Contains(command, "--method post") || strings.Contains(command, "--method patch") ||
			strings.Contains(command, "-x post") || strings.Contains(command, "-x patch"))
}

func deliverableToolStatusSucceeded(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "success", "succeeded":
		return true
	default:
		return false
	}
}

func deliverableToolStatusFailed(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "error", "cancelled", "canceled", "timed_out":
		return true
	default:
		return false
	}
}

func tokenUsageActivityMessage(tokens AgentTokenUsage) string {
	if tokens.TotalTokens > 0 && (tokens.InputTokens > 0 || tokens.OutputTokens > 0) {
		return fmt.Sprintf("%d total tokens (%d in, %d out)", tokens.TotalTokens, tokens.InputTokens, tokens.OutputTokens)
	}
	if tokens.TotalTokens > 0 {
		return fmt.Sprintf("%d total tokens", tokens.TotalTokens)
	}
	return "tokens updated"
}

func rateLimitsActivityMessage(snapshot *telemetry.RateLimits) string {
	if snapshot == nil {
		return "rate limits updated"
	}
	name := strings.TrimSpace(snapshot.LimitName)
	if name == "" {
		name = strings.TrimSpace(snapshot.LimitID)
	}
	if name == "" {
		return "rate limits updated"
	}
	return name + " rate limits updated"
}

func modelUpdatedActivityMessage(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return "model updated"
	}
	return "model updated to " + model
}

func (p *agentRunProgress) turnCount() int {
	return p.turnOffset + len(p.turnIDs)
}

func (p *agentRunProgress) addRecentEvent(event telemetry.ActivityEvent) {
	if event.Event == "" && event.Message == "" {
		return
	}
	p.recentEvents = append(p.recentEvents, event)
	if len(p.recentEvents) > recentActivityLimit {
		p.recentEvents = p.recentEvents[len(p.recentEvents)-recentActivityLimit:]
	}
}

func (p *agentRunProgress) recentActivity() []telemetry.ActivityEvent {
	if len(p.recentEvents) == 0 {
		return nil
	}
	out := make([]telemetry.ActivityEvent, len(p.recentEvents))
	copy(out, p.recentEvents)
	for index := range out {
		out[index].Truncation = runtimeoutput.CloneTruncation(out[index].Truncation)
	}
	return out
}

func (p *agentRunProgress) outputText() string {
	if p.output != nil {
		return p.output.String()
	}
	var out strings.Builder
	for _, key := range p.messageOrder {
		if message := p.messages[key]; message != nil {
			out.WriteString(message.String())
		}
	}
	return out.String()
}

func (p *agentRunProgress) finalMessage() string {
	for index := len(p.messageOrder) - 1; index >= 0; index-- {
		if message := p.messages[p.messageOrder[index]]; message != nil {
			return message.String()
		}
	}
	return ""
}

func (r *Runner) publishRunUpdate(
	ctx context.Context,
	req RunRequest,
	info workspace.Info,
	issue workspace.Issue,
	progress *agentRunProgress,
	result RunResult,
	eventAt time.Time,
	runStartedAt time.Time,
	detentSessionID int64,
) error {
	if req.OnUsageUpdate == nil {
		return nil
	}

	result.Tokens.RuntimeSeconds = runtimeSeconds(runStartedAt, eventAt)
	usage := UsageUpdate{
		DetentSessionID:       detentSessionID,
		SessionID:             progress.sessionID,
		ProcessIdentity:       progress.processIdentity,
		WorkerProcess:         progress.workerProcess,
		WorkspacePath:         info.Path,
		TurnCount:             progress.turnCount(),
		LastEventAt:           progress.lastEventAt,
		LastEvent:             progress.lastEvent,
		LastMessage:           progress.lastMessage,
		LastMessageTruncation: runtimeoutput.CloneTruncation(progress.lastMessageTruncation),
		RecentEvents:          progress.recentActivity(),
		RuntimeIdentity:       result.RuntimeIdentity,
		WorkerGitHubActor:     req.workerGitHubActor,
		WorkProductPushed:     progress.pullRequestHeadPushed() || progress.pullRequestUpdated(),
		Tokens:                result.Tokens,
		RateLimits:            result.RateLimits,
		RSSBytes:              progress.rssBytes,
		RSSCeilingBytes:       progress.rssCeilingBytes,
		RSSObservedAt:         progress.rssObservedAt,
	}
	if req.Admission == nil {
		diffStats, ok := r.liveDiffStats(ctx, info, issue, progress, eventAt)
		if ok {
			usage.DiffStats = diffStats
		}
	}
	return req.OnUsageUpdate(usage)
}

func publishAgentActivity(req RunRequest, detentSessionID int64, update AgentUpdate, at time.Time) error {
	if req.OnActivityUpdate == nil {
		return nil
	}
	content := update.Delta
	if update.Type == AgentUpdateToolCompleted && failedAgentToolStatus(update.Status) {
		if message := meaningfulDeliverableDetail(update.BackendErrorMessage); message != "" {
			content = message
		}
	}
	return req.OnActivityUpdate(AgentActivityUpdate{
		At:                at.UTC(),
		DetentSessionID:   detentSessionID,
		ProviderSessionID: strings.TrimSpace(update.ThreadID),
		TurnID:            strings.TrimSpace(update.TurnID),
		ItemID:            strings.TrimSpace(update.ItemID),
		Type:              update.Type,
		Tool:              strings.TrimSpace(update.Tool),
		Content:           content,
		Status:            strings.TrimSpace(update.Status),
		Model:             strings.TrimSpace(update.Model),
		TotalTokens:       update.Tokens.TotalTokens,
	})
}

func failedAgentToolStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "error", "cancelled", "canceled", "timed_out":
		return true
	default:
		return false
	}
}

func (r *Runner) liveDiffStats(
	ctx context.Context,
	info workspace.Info,
	issue workspace.Issue,
	progress *agentRunProgress,
	eventAt time.Time,
) (DiffStats, bool) {
	if !progress.shouldRefreshDiffStats(eventAt) {
		return progress.cachedDiffStats()
	}

	progress.diffStatsCheckedAt = eventAt
	stat, err := r.workspace.DiffStat(ctx, info, issue)
	if err != nil {
		if workspace.IsMissingWorkspaceError(err) {
			r.logger.Info(
				"workspace live diff stat skipped",
				slog.String("issue_id", issue.ID),
				slog.String("issue_identifier", issue.Identifier),
				slog.String("workspace_path", info.Path),
				slog.String("phase", "live"),
				slog.String("error", err.Error()),
			)
			return progress.cachedDiffStats()
		}
		r.logger.Warn(
			"workspace live diff stat failed",
			slog.String("issue_id", issue.ID),
			slog.String("issue_identifier", issue.Identifier),
			slog.String("workspace_path", info.Path),
			slog.String("phase", "live"),
			slog.String("error", err.Error()),
		)
		return progress.cachedDiffStats()
	}

	diffStats := diffStatsFromWorkspace(stat)
	diffStats.Status = "ok"
	progress.diffStats = diffStats
	progress.diffStatsCollected = true
	return diffStats, true
}

func (p *agentRunProgress) shouldRefreshDiffStats(eventAt time.Time) bool {
	if p.diffStatsCheckedAt.IsZero() {
		return true
	}
	return eventAt.Sub(p.diffStatsCheckedAt) >= liveDiffStatsInterval
}

func (p *agentRunProgress) cachedDiffStats() (DiffStats, bool) {
	if !p.diffStatsCollected {
		return DiffStats{}, false
	}
	return p.diffStats, true
}

func pullRequestNumber(issue connector.Issue) *int64 {
	if issue.PRNumber != nil && *issue.PRNumber > 0 {
		number := int64(*issue.PRNumber)
		return &number
	}

	value := strings.TrimSpace(issue.URL)
	const marker = "/pull/"
	index := strings.LastIndex(value, marker)
	if index == -1 {
		return nil
	}

	value = value[index+len(marker):]
	end := strings.IndexFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	})
	if end != -1 {
		value = value[:end]
	}

	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number <= 0 {
		return nil
	}
	return &number
}

func ciTriggerPullRequestNumber(issue connector.Issue) int {
	if number := pullRequestNumber(issue); number != nil {
		return int(*number)
	}
	if issue.PullRequest != nil {
		return issue.PullRequest.Number
	}
	return 0
}

func diffStatsFromWorkspace(stat workspace.DiffStat) DiffStats {
	status := "clean"
	if stat.Files != 0 || stat.Added != 0 || stat.Removed != 0 {
		status = "changed"
	}
	return DiffStats{
		FilesChanged: stat.Files,
		AddedLines:   stat.Added,
		RemovedLines: stat.Removed,
		Fingerprint:  strings.TrimSpace(stat.Fingerprint),
		Status:       status,
	}
}

func applyRecoveryState(diffStats *DiffStats, state *workspace.RecoveryState) {
	if diffStats == nil || state == nil {
		return
	}
	diffStats.UnpushedCommits = state.UnpushedCommits
	diffStats.UnpushedCommitRefs = append([]string(nil), state.UnpushedCommitRefs...)
	diffStats.TrackedPaths = append([]string(nil), state.TrackedPaths...)
	diffStats.UntrackedPaths = append([]string(nil), state.UntrackedPaths...)
	diffStats.CommitsNotInPullRequest = append([]string(nil), state.CommitsNotInPullRequest...)
	diffStats.PullRequestComparisonAvailable = state.PullRequestComparisonAvailable
	diffStats.RecoveryStateAvailable = true
	diffStats.HeadSHA = strings.TrimSpace(state.HeadSHA)
}

func diffStatsEmpty(diffStats DiffStats) bool {
	return diffStats.FilesChanged == 0 &&
		diffStats.AddedLines == 0 &&
		diffStats.RemovedLines == 0 &&
		diffStats.UnpushedCommits == 0 &&
		len(diffStats.UnpushedCommitRefs) == 0 &&
		len(diffStats.TrackedPaths) == 0 &&
		len(diffStats.UntrackedPaths) == 0 &&
		len(diffStats.CommitsNotInPullRequest) == 0 &&
		!diffStats.PullRequestComparisonAvailable &&
		!diffStats.RecoveryStateExpected &&
		!diffStats.RecoveryStateAvailable &&
		diffStats.CommitsAhead == 0 &&
		!diffStats.RemoteBranchExists &&
		!diffStats.DeliveryStateChecked &&
		strings.TrimSpace(diffStats.HeadSHA) == "" &&
		strings.TrimSpace(diffStats.Fingerprint) == "" &&
		strings.TrimSpace(diffStats.Status) == ""
}

func (r *Runner) logWorkerEvent(issue connector.Issue, event string, attrs ...any) {
	r.logWorkerEventLevel(slog.LevelDebug, issue, event, attrs...)
}

func (r *Runner) logWorkerEventLevel(level slog.Level, issue connector.Issue, event string, attrs ...any) {
	if r == nil || r.logger == nil {
		return
	}
	correlation, attrs := workerLifecycleCorrelation(r.projectID, issue, attrs)
	all := []any{
		"issue_state", strings.TrimSpace(issue.State),
	}
	all = append(all, attrs...)
	telemetry.LogLifecycle(r.logger, level, workerLifecycleClass(event), event, correlation, all...)
}

func (r *Runner) logAgentUpdate(req RunRequest, detentSessionID int64, update AgentUpdate) {
	event := strings.TrimSpace(string(update.Type))
	if event == "" {
		event = strings.TrimSpace(update.Method)
	}
	logEvent := func(level slog.Level, lifecycleEvent string, attrs ...any) {
		correlation := []any{
			telemetry.WorkAttemptIDKey, req.WorkAttemptID,
			telemetry.DetentSessionIDKey, detentSessionID,
			telemetry.ProviderSessionIDKey, agentUpdateProviderSessionID(update),
		}
		correlation = append(correlation, attrs...)
		r.logWorkerEventLevel(level, req.Issue, lifecycleEvent, correlation...)
	}
	switch update.Type {
	case AgentUpdateProcessStarted:
		logEvent(slog.LevelDebug, "worker_process_started",
			"process_identity", strings.TrimSpace(update.ProcessIdentity),
		)
	case AgentUpdateTurnStarted:
		logEvent(slog.LevelDebug, "worker_turn_started",
			"thread_id", strings.TrimSpace(update.ThreadID),
			"turn_id", strings.TrimSpace(update.TurnID),
		)
	case AgentUpdateTurnCompleted:
		attrs := []any{
			"thread_id", strings.TrimSpace(update.ThreadID),
			"turn_id", strings.TrimSpace(update.TurnID),
			"status", strings.TrimSpace(update.Status),
		}
		attrs = append(attrs, agentUpdateBackendErrorAttrs(update)...)
		if failedAgentTurnStatus(update.Status) {
			logEvent(slog.LevelWarn, "worker_turn_finished", attrs...)
		} else {
			logEvent(slog.LevelDebug, "worker_turn_finished", attrs...)
		}
	case AgentUpdateTokenUsage:
		threadTotal := AgentTokenCounts{
			InputTokens:           update.Tokens.InputTokens,
			CachedInputTokens:     update.Tokens.CachedInputTokens,
			OutputTokens:          update.Tokens.OutputTokens,
			ReasoningOutputTokens: update.Tokens.ReasoningOutputTokens,
			TotalTokens:           update.Tokens.TotalTokens,
		}
		if update.Tokens.ThreadTotal != nil {
			threadTotal = *update.Tokens.ThreadTotal
		}
		attrs := []any{
			"thread_id", strings.TrimSpace(update.ThreadID),
			"turn_id", strings.TrimSpace(update.TurnID),
			"total_tokens", update.Tokens.TotalTokens,
			"input_tokens", update.Tokens.InputTokens,
			"cached_input_tokens", update.Tokens.CachedInputTokens,
			"output_tokens", update.Tokens.OutputTokens,
			"reasoning_output_tokens", update.Tokens.ReasoningOutputTokens,
			"thread_total_tokens", threadTotal.TotalTokens,
			"thread_input_tokens", threadTotal.InputTokens,
			"thread_cached_input_tokens", threadTotal.CachedInputTokens,
			"thread_output_tokens", threadTotal.OutputTokens,
			"thread_reasoning_output_tokens", threadTotal.ReasoningOutputTokens,
		}
		if update.Tokens.Last != nil {
			attrs = append(attrs,
				"last_total_tokens", update.Tokens.Last.TotalTokens,
				"last_input_tokens", update.Tokens.Last.InputTokens,
				"last_cached_input_tokens", update.Tokens.Last.CachedInputTokens,
				"last_output_tokens", update.Tokens.Last.OutputTokens,
				"last_reasoning_output_tokens", update.Tokens.Last.ReasoningOutputTokens,
			)
		}
		logEvent(slog.LevelDebug, "worker_usage_updated", attrs...)
	case AgentUpdateRateLimits:
		logEvent(slog.LevelDebug, "worker_rate_limits_updated",
			"thread_id", strings.TrimSpace(update.ThreadID),
			"turn_id", strings.TrimSpace(update.TurnID),
		)
	case AgentUpdateModelUpdated:
		logEvent(slog.LevelDebug, "worker_model_updated",
			"thread_id", strings.TrimSpace(update.ThreadID),
			"turn_id", strings.TrimSpace(update.TurnID),
			"model", strings.TrimSpace(update.Model),
		)
	case AgentUpdateRuntimeIdentity:
		return
	default:
		if event != "" && update.Type != AgentUpdateMessageDelta {
			logEvent(slog.LevelDebug, "worker_agent_update",
				"agent_event", event,
				"thread_id", strings.TrimSpace(update.ThreadID),
				"turn_id", strings.TrimSpace(update.TurnID),
			)
		}
	}
}

func agentUpdateProviderSessionID(update AgentUpdate) string {
	if providerSessionID := strings.TrimSpace(update.ProviderSessionID); providerSessionID != "" {
		return providerSessionID
	}
	threadID := strings.TrimSpace(update.ThreadID)
	turnID := strings.TrimSpace(update.TurnID)
	if threadID == "" || turnID == "" {
		return ""
	}
	if threadID == turnID {
		return threadID
	}
	return threadID + "-" + turnID
}

func workerLifecycleClass(event string) telemetry.LifecycleClass {
	switch strings.TrimSpace(event) {
	case "worker_workspace_create_started", "worker_workspace_created", "worker_before_run_finished", "worker_after_run_finished",
		"worker_check_workspace_create_started", "worker_check_workspace_created", "worker_check_before_run_finished", "worker_check_after_run_finished":
		return telemetry.LifecycleWorkspace
	case "worker_session_started", "worker_session_finished", "worker_command_started", "worker_command_finished", "worker_check_started", "worker_check_finished":
		return telemetry.LifecycleDetentSession
	case "worker_process_started", "worker_turn_started", "worker_turn_finished", "worker_usage_updated", "worker_rate_limits_updated", "worker_model_updated", "worker_agent_update", "worker_runtime_identity_resolved", "worker_runtime_identity_changed":
		return telemetry.LifecycleProviderSession
	default:
		return telemetry.LifecycleWorkAttempt
	}
}

func workerLifecycleCorrelation(projectID string, issue connector.Issue, attrs []any) (telemetry.LifecycleCorrelation, []any) {
	correlation := telemetry.LifecycleCorrelation{
		ProjectID:       projectID,
		IssueID:         issue.ID,
		IssueIdentifier: issue.Identifier,
	}
	remaining := make([]any, 0, len(attrs))
	for index := 0; index < len(attrs); {
		if index+1 >= len(attrs) {
			remaining = append(remaining, attrs[index])
			break
		}
		key, ok := attrs[index].(string)
		if !ok {
			remaining = append(remaining, attrs[index], attrs[index+1])
			index += 2
			continue
		}
		value := attrs[index+1]
		switch key {
		case telemetry.WorkAttemptIDKey:
			correlation.WorkAttemptID = lifecycleInt64(value)
		case telemetry.DetentSessionIDKey:
			correlation.DetentSessionID = lifecycleInt64(value)
		case telemetry.ProviderSessionIDKey:
			if providerSessionID, ok := value.(string); ok {
				correlation.ProviderSessionID = providerSessionID
			}
		default:
			remaining = append(remaining, key, value)
		}
		index += 2
	}
	return correlation, remaining
}

func lifecycleInt64(value any) int64 {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	default:
		return 0
	}
}

func (r *Runner) logRuntimeIdentity(req RunRequest, detentSessionID int64, update AgentUpdate, previous agentidentity.Identity, current agentidentity.Identity) {
	event := "worker_runtime_identity_changed"
	if !previous.HasRuntimeValues() {
		event = "worker_runtime_identity_resolved"
	}
	attrs := []any{
		"work_attempt_id", req.WorkAttemptID,
		"detent_session_id", detentSessionID,
		"provider_thread_id", strings.TrimSpace(update.ThreadID),
		"provider_session_id", runtimeIdentityProviderSessionID(current.BackendKind, update.ThreadID, update.TurnID),
		"provider_turn_id", strings.TrimSpace(update.TurnID),
		"identity_source_event", strings.TrimSpace(update.Method),
	}
	attrs = append(attrs, runtimeIdentityLogAttrs(current)...)
	if event == "worker_runtime_identity_changed" {
		attrs = append(attrs,
			"old_provider", previous.Provider.Value,
			"new_provider", current.Provider.Value,
			"old_provider_provenance", previous.Provider.Provenance,
			"new_provider_provenance", current.Provider.Provenance,
			"old_resolved_model", previous.ResolvedModel.Value,
			"new_resolved_model", current.ResolvedModel.Value,
			"old_resolved_model_provenance", previous.ResolvedModel.Provenance,
			"new_resolved_model_provenance", current.ResolvedModel.Provenance,
			"old_reasoning_effort", previous.ReasoningEffort.Value,
			"new_reasoning_effort", current.ReasoningEffort.Value,
			"old_reasoning_effort_provenance", previous.ReasoningEffort.Provenance,
			"new_reasoning_effort_provenance", current.ReasoningEffort.Provenance,
			"old_service_tier", previous.ServiceTier.Value,
			"new_service_tier", current.ServiceTier.Value,
			"old_service_tier_provenance", previous.ServiceTier.Provenance,
			"new_service_tier_provenance", current.ServiceTier.Provenance,
		)
	}
	r.logWorkerEvent(req.Issue, event, attrs...)
}

func runtimeIdentityProviderSessionID(backendKind string, threadID string, turnID string) string {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	if strings.EqualFold(strings.TrimSpace(backendKind), config.AgentBackendClaudeCode) {
		return threadID
	}
	if threadID == "" || turnID == "" {
		return ""
	}
	return threadID + "-" + turnID
}

func runtimeIdentityLogAttrs(identity agentidentity.Identity) []any {
	identity = identity.Normalize()
	attrs := []any{
		"model_selection_policy", identity.Selection.Policy,
		"model_selection_policy_source", identity.Selection.PolicySource,
		"model_selection_reason", identity.Selection.Reason,
		"model_selection_requested", identity.Selection.RequestedModel,
		"model_selection_fallback_reason", identity.Selection.FallbackReason,
		"model_selection_model_source", identity.Selection.ModelSource,
		"model_selection_effort_source", identity.Selection.EffortSource,
		"backend_source", identity.Selection.BackendSource,
		"route_source", identity.Selection.RouteSource,
		"backend_id", identity.BackendID,
		"backend_kind", identity.BackendKind,
		"route", identity.Route,
		"role", identity.Role,
		"provider", identity.Provider.Value,
		"provider_provenance", identity.Provider.Provenance,
		"requested_model", identity.RequestedModel.Value,
		"requested_model_provenance", identity.RequestedModel.Provenance,
		"resolved_model", identity.ResolvedModel.Value,
		"resolved_model_provenance", identity.ResolvedModel.Provenance,
		"reasoning_effort", identity.ReasoningEffort.Value,
		"reasoning_effort_provenance", identity.ReasoningEffort.Provenance,
		"service_tier", identity.ServiceTier.Value,
		"service_tier_provenance", identity.ServiceTier.Provenance,
	}
	if identity.ObservedAt != nil {
		attrs = append(attrs, "identity_observed_at", *identity.ObservedAt)
	}
	return attrs
}

type backendErrorCarrier interface {
	BackendErrorBody() string
	BackendErrorMessage() string
}

func backendErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	var carrier backendErrorCarrier
	if !errors.As(err, &carrier) {
		return nil
	}
	return backendErrorStringsAttrs(carrier.BackendErrorBody(), carrier.BackendErrorMessage())
}

func agentUpdateBackendErrorAttrs(update AgentUpdate) []any {
	return backendErrorStringsAttrs(update.BackendErrorBody, update.BackendErrorMessage)
}

func backendErrorStringsAttrs(body string, message string) []any {
	attrs := []any{}
	if body = strings.TrimSpace(body); body != "" {
		attrs = append(attrs, "backend_error_body", body)
	}
	if message = strings.TrimSpace(message); message != "" {
		attrs = append(attrs, "backend_error_message", message)
	}
	return attrs
}

func failedAgentTurnStatus(status string) bool {
	status = strings.TrimSpace(status)
	return status != "" && !strings.EqualFold(status, "completed")
}

func workerRunOutcome(err error, finalState string) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timed_out"
	case errors.Is(err, ErrSessionTokenCeilingExceeded):
		return FinalStateTokenCeilingExceeded
	case errors.Is(err, ErrSessionBudgetProjectionExceeded):
		return FinalStateBudgetProjectionExceeded
	case errors.Is(err, ErrSessionMemoryCeilingExceeded):
		return FinalStateMemoryCeilingExceeded
	case errors.Is(err, ErrWorkspaceBranchHeld):
		return "deferred"
	case err != nil:
		return "failed"
	case strings.EqualFold(strings.TrimSpace(finalState), FinalStateCompleted), strings.TrimSpace(finalState) == "":
		return "succeeded"
	default:
		return strings.TrimSpace(finalState)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func runtimeSeconds(startedAt, completedAt time.Time) float64 {
	if startedAt.IsZero() || completedAt.IsZero() || completedAt.Before(startedAt) {
		return 0
	}
	return completedAt.Sub(startedAt).Seconds()
}
