package web

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/a-h/templ"
	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/activity"
	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/buildinfo"
	chatpkg "github.com/digitaldrywood/detent/internal/chat"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/healthnotify"
	"github.com/digitaldrywood/detent/internal/hub"
	kanbanstate "github.com/digitaldrywood/detent/internal/kanban"
	"github.com/digitaldrywood/detent/internal/mcp"
	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/operatortool"
	"github.com/digitaldrywood/detent/internal/pause"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/staleness"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

var (
	ErrMissingHub       = errors.New("web server requires hub")
	ErrMissingStore     = errors.New("web server requires store")
	ErrMissingRegistry  = errors.New("web server requires registry")
	ErrMissingConnector = errors.New("web server requires connector")
)

type Dependencies struct {
	RunnerFleet         RunnerFleet
	Hub                 *hub.Hub[telemetry.Snapshot]
	Store               store.Store
	Registry            *project.Registry
	Connector           connector.Connector
	StartupLifecycle    *StartupLifecycle
	Refresher           Refresher
	OperatorMoves       OperatorMoveReconciler
	Recovery            WorkAttemptRecovery
	RunStopper          RunStopper
	UpdateApplier       UpdateApplier
	Activity            *activity.Broker
	History             activity.HistoryReader
	MagicLinkSender     auth.Sender
	IdentityProvider    auth.IdentityProvider
	Chat                chatpkg.Provider
	IssueExplainer      IssueExplainer
	HealthNotifications healthnotify.FailureReader
	TickLiveness        TickLivenessSource
	WorkerProcesses     WorkerProcessStore
	StalenessWarnings   *staleness.Acknowledgements
	ObserveProcesses    func([]procgroup.Identity) ([]procgroup.Observation, error)
}

type WorkerProcessStore interface {
	ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error)
}

type TickLivenessSource interface {
	TickLiveness(time.Time) []telemetry.TickLiveness
}

type Mode string

const (
	ModeRunning    Mode = "running"
	ModeOnboarding Mode = "onboarding"
)

type StartupLifecycleState string

const (
	StartupLifecycleStarting StartupLifecycleState = "starting"
	StartupLifecycleReady    StartupLifecycleState = "ready"
	StartupLifecycleFailed   StartupLifecycleState = "failed"
)

type StartupLifecycle struct {
	state atomic.Uint32
}

func NewStartupLifecycle() *StartupLifecycle {
	return &StartupLifecycle{}
}

func (l *StartupLifecycle) State() StartupLifecycleState {
	if l == nil {
		return StartupLifecycleReady
	}
	switch l.state.Load() {
	case 0:
		return StartupLifecycleStarting
	case 1:
		return StartupLifecycleReady
	default:
		return StartupLifecycleFailed
	}
}

func (l *StartupLifecycle) MarkReady() {
	l.state.Store(1)
}

func (l *StartupLifecycle) MarkFailed() {
	l.state.Store(2)
}

const (
	defaultHTTPReadHeaderTimeout = 5 * time.Second
	defaultHTTPIdleTimeout       = 2 * time.Minute
	defaultSSEFragmentInterval   = 5 * time.Second
	defaultSSEHealthInterval     = 10 * time.Second
	defaultSSEMetricsInterval    = time.Minute
	sidebarStateCookieName       = "sidebar_state"
	themeCookieName              = "theme"
	densityCookieName            = "density"
)

type Config struct {
	Logger                *slog.Logger
	Mode                  Mode
	StaticDir             string
	SSETickInterval       time.Duration
	SSEFragmentInterval   time.Duration
	SSEHealthInterval     time.Duration
	SSEMetricsInterval    time.Duration
	HTTPReadHeaderTimeout time.Duration
	HTTPIdleTimeout       time.Duration
	WorkflowPath          string
	Version               string
	Build                 buildinfo.Info
	DashboardURL          string
	Pricing               budget.PricingTable
	GlobalConfig          globalconfig.Config
	GlobalConfigSource    func() globalconfig.Config
	LookupEnv             func(string) string
	Hostname              func() (string, error)
	ConfigPathRule        globalconfig.PathRule
	Kanban                workflowconfig.Kanban
	KanbanWorkflow        workflowconfig.Config
	GitHubWebhookSecret   string
	RuntimeDBPath         string
	RuntimeLogPath        string
	ServerAddress         string
	Demo                  DemoConfig
	Now                   func() time.Time
}

type Server struct {
	runnerFleet         RunnerFleet
	echo                *echo.Echo
	hub                 *hub.Hub[telemetry.Snapshot]
	store               store.Store
	registry            *project.Registry
	connector           connector.Connector
	startupLifecycle    *StartupLifecycle
	refresher           Refresher
	operatorMoves       OperatorMoveReconciler
	recovery            WorkAttemptRecovery
	runStopper          RunStopper
	updateApplier       UpdateApplier
	activity            *activity.Broker
	history             activity.HistoryReader
	logger              *slog.Logger
	mode                Mode
	tickEvery           time.Duration
	sseFragmentInterval time.Duration
	sseHealthInterval   time.Duration
	sseMetricsInterval  time.Duration
	workflow            string
	version             string
	build               buildinfo.Info
	dashboardURL        string
	pricing             budget.PricingTable
	globalConfig        globalconfig.Config
	globalConfigSource  func() globalconfig.Config
	lookupEnv           func(string) string
	hostname            func() (string, error)
	configRule          globalconfig.PathRule
	kanban              workflowconfig.Kanban
	kanbanWorkflow      workflowconfig.Config
	githubWebhookSecret string
	dbPath              string
	logPath             string
	serverAddr          string
	assets              staticAssets
	projects            *projectSmallMultipleRecorder
	snapshots           *snapshotEnrichmentCache
	workflowHistory     workflowHistoryCache
	stateEndpoints      *stateEndpointRecorder
	spendRegressions    *spendRegressionMonitor
	kanbanMutations     *kanbanstate.MutationTracker
	kanbanRefreshes     *kanbanRefreshFeedbackTracker
	kanbanRetryInFlight atomic.Bool
	refreshes           *manualRefreshTracker
	demo                *demoScenarioSet
	apiKeys             *apikey.Service
	ipLimiter           *apiRateLimiter
	keyLimiter          *apiRateLimiter
	asyncWrites         *asyncStoreWriter
	dashboardAuthSecret [32]byte
	afterFunc           func(time.Duration, func()) *time.Timer
	sessions            *auth.Service
	magicLinks          *auth.Service
	identityProvider    auth.IdentityProvider
	identityAllowlist   *auth.Allowlist
	chat                *chatpkg.Service
	issueExplainer      IssueExplainer
	healthNotifications healthnotify.FailureReader
	tickLiveness        TickLivenessSource
	workerProcesses     WorkerProcessStore
	stalenessWarnings   *staleness.Acknowledgements
	observeProcesses    func([]procgroup.Identity) ([]procgroup.Observation, error)
	now                 func() time.Time
	operatorTools       *operatortool.Executor
	mcpHTTP             *mcp.HTTPHandler
}

func NewServer(cfg Config, deps Dependencies) (*Server, error) {
	mode := cfg.mode()
	if mode == ModeRunning {
		if deps.Hub == nil {
			return nil, ErrMissingHub
		}
		if deps.Store == nil {
			return nil, ErrMissingStore
		}
		if deps.Registry == nil {
			return nil, ErrMissingRegistry
		}
		if deps.Connector == nil {
			return nil, ErrMissingConnector
		}
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Server.ReadHeaderTimeout = cfg.httpReadHeaderTimeout()
	e.Server.IdleTimeout = cfg.httpIdleTimeout()
	kanban := cfg.kanban()
	kanbanWorkflow := cfg.kanbanWorkflow(kanban)
	logger := cfg.logger()
	activityBroker := deps.Activity
	if activityBroker == nil {
		activityBroker = activity.NewBroker()
	}
	historyReader := deps.History
	if historyReader == nil {
		historyReader = activity.NewRolloutHistoryReader("", "")
	}
	dashboardAuthSecret, err := newDashboardAuthSecret()
	if err != nil {
		return nil, fmt.Errorf("dashboard auth secret: %w", err)
	}
	magicLinks, magicLinksEnabled, err := newMagicLinkService(cfg, deps.Store, deps.MagicLinkSender)
	if err != nil {
		return nil, err
	}
	identityProvider, oidcSessions, identityAllowlist, oidcEnabled, err := newOIDCService(context.Background(), cfg, deps.Store, deps.IdentityProvider)
	if err != nil {
		return nil, err
	}
	sessions := magicLinks
	if oidcEnabled {
		sessions = oidcSessions
	}
	tickLiveness := deps.TickLiveness
	if tickLiveness == nil && deps.Registry != nil {
		tickLiveness = deps.Registry
	}
	observeProcesses := deps.ObserveProcesses
	if observeProcesses == nil {
		observeProcesses = procgroup.Observe
	}
	startupLifecycle := deps.StartupLifecycle
	if startupLifecycle == nil {
		startupLifecycle = NewStartupLifecycle()
		startupLifecycle.MarkReady()
	}

	server := &Server{
		runnerFleet:         deps.RunnerFleet,
		echo:                e,
		hub:                 deps.Hub,
		store:               deps.Store,
		registry:            deps.Registry,
		connector:           deps.Connector,
		startupLifecycle:    startupLifecycle,
		refresher:           deps.Refresher,
		operatorMoves:       deps.OperatorMoves,
		recovery:            deps.Recovery,
		runStopper:          deps.RunStopper,
		updateApplier:       deps.UpdateApplier,
		activity:            activityBroker,
		history:             historyReader,
		logger:              logger,
		mode:                mode,
		tickEvery:           cfg.sseTickInterval(),
		sseFragmentInterval: cfg.sseFragmentInterval(),
		sseHealthInterval:   cfg.sseHealthInterval(),
		sseMetricsInterval:  cfg.sseMetricsInterval(),
		workflow:            cfg.workflowPath(),
		version:             strings.TrimSpace(cfg.Version),
		build:               cfg.Build,
		dashboardURL:        cfg.dashboardURL(),
		pricing:             cfg.pricing(),
		globalConfig:        cfg.GlobalConfig,
		globalConfigSource:  cfg.globalConfigSource(),
		lookupEnv:           cfg.lookupEnv(),
		hostname:            cfg.hostname(),
		configRule:          cfg.ConfigPathRule,
		kanban:              kanban,
		kanbanWorkflow:      kanbanWorkflow,
		githubWebhookSecret: cfg.githubWebhookSecret(kanbanWorkflow),
		dbPath:              strings.TrimSpace(cfg.RuntimeDBPath),
		logPath:             strings.TrimSpace(cfg.RuntimeLogPath),
		serverAddr:          strings.TrimSpace(cfg.ServerAddress),
		assets:              newStaticAssets(cfg.staticDir()),
		projects:            newProjectSmallMultipleRecorder(),
		snapshots:           newSnapshotEnrichmentCache(),
		stateEndpoints:      newStateEndpointRecorder(),
		spendRegressions:    newSpendRegressionMonitor(),
		kanbanMutations:     kanbanstate.NewMutationTracker(),
		kanbanRefreshes:     newKanbanRefreshFeedbackTracker(),
		refreshes:           newManualRefreshTracker(),
		demo:                newDemoScenarioSet(cfg.Demo),
		apiKeys:             apikey.NewService(deps.Store),
		ipLimiter:           newAPIRateLimiter(300, 60),
		keyLimiter:          newAPIRateLimiter(120, 30),
		asyncWrites:         newAsyncStoreWriter(256, logger),
		dashboardAuthSecret: dashboardAuthSecret,
		afterFunc:           time.AfterFunc,
		sessions:            sessions,
		magicLinks:          magicLinks,
		identityProvider:    identityProvider,
		identityAllowlist:   identityAllowlist,
		issueExplainer:      deps.IssueExplainer,
		healthNotifications: deps.HealthNotifications,
		tickLiveness:        tickLiveness,
		workerProcesses:     deps.WorkerProcesses,
		stalenessWarnings:   deps.StalenessWarnings,
		observeProcesses:    observeProcesses,
		now:                 cfg.now(),
	}
	if !magicLinksEnabled && !oidcEnabled {
		server.sessions = nil
	}
	chatProvider := deps.Chat
	if server.demo != nil {
		chatProvider = server.demoChatProvider()
	}
	server.operatorTools = server.newReadOnlyToolExecutor()
	server.mcpHTTP = mcp.NewHTTPHandler(server.operatorTools, server.version, mcp.HTTPConfig{
		Principal: func(req *http.Request) string {
			credential, _ := apiCredentialFromContext(req.Context())
			return credential.ID
		},
	})
	server.chat = chatpkg.NewService(chatProvider, server.newChatToolExecutor(), server)
	e.HTTPErrorHandler = server.handleHTTPError
	e.Use(server.privateDashboardAccess, server.uiAPICookie, server.sessionGate, server.nativeWorkReadBoundary)
	server.registerRoutes()
	server.warnIfAPITokenMissingOnNonLoopback()

	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.echo
}

func (s *Server) Echo() *echo.Echo {
	return s.echo
}

func (s *Server) Start(addr string) error {
	s.logger.Info("starting web server", "addr", addr)
	return s.echo.Start(addr)
}

func (s *Server) StartListener(listener net.Listener) error {
	s.logger.Info("starting web server", "addr", listener.Addr().String())
	return s.echo.Server.Serve(listener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("stopping web server")
	if s.ipLimiter != nil {
		s.ipLimiter.Stop()
	}
	if s.keyLimiter != nil {
		s.keyLimiter.Stop()
	}
	var err error
	if s.mcpHTTP != nil {
		err = s.mcpHTTP.Shutdown(ctx)
	}
	err = errors.Join(err, s.echo.Shutdown(ctx))
	if s.asyncWrites != nil {
		if closeErr := s.asyncWrites.Close(ctx); closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return err
}

func (s *Server) registerRoutes() {
	s.echo.GET("/static/*", s.assets.serve)
	s.echo.GET("/health", s.health)
	s.echo.GET(openAPIPath, s.openAPI)
	if s.magicLinks != nil || s.identityProvider != nil {
		s.echo.GET("/login", s.loginPage)
	}
	if s.magicLinks != nil {
		s.echo.POST("/login", s.requestMagicLink)
		s.echo.GET("/auth/magic-link", s.consumeMagicLink)
	}
	if s.identityProvider != nil {
		s.echo.GET("/auth/oidc/start", s.startOIDC)
		s.echo.GET("/auth/oidc/callback", s.completeOIDC)
	}
	if s.mode == ModeOnboarding {
		s.echo.GET("/", s.redirectToOnboarding)
		s.echo.GET("/onboarding", s.onboarding)
		s.echo.POST("/onboarding/tracker", s.onboardingTracker)
		s.echo.POST("/onboarding/credentials", s.onboardingCredentials)
		s.echo.POST("/onboarding/project", s.onboardingProject)
		s.echo.POST("/onboarding/agent", s.onboardingAgent)
		s.echo.POST("/onboarding/write", s.onboardingWrite)
		return
	}

	s.echo.GET("/", s.board)
	s.echo.GET("/live-session", s.boardLiveSessionPage)
	s.echo.GET("/fleet", s.dashboard)
	s.echo.GET("/fleet/runners", s.runnerFleetPage)
	s.echo.GET("/kanban", s.redirectToBoard)
	s.echo.GET("/health/ui", s.healthDashboard)
	s.echo.GET("/diagnostics", s.diagnosticsDashboard)
	s.echo.GET("/analytics", s.analyticsDashboard)
	s.echo.GET("/library", s.library)
	s.echo.GET("/projects/:project_id/issues/new", s.nativeIssueForm, s.nativeReadAccess)
	s.echo.GET("/projects/:project_id/issues/:issue_ref/edit", s.nativeIssueForm, s.nativeReadAccess)
	s.echo.GET("/projects/:project_id/issues/:issue_ref/export", s.nativeIssueExport, s.nativeReadAccess)
	s.echo.GET("/projects/:project_id/issues/:issue_ref", s.issueDetail, s.nativeReadAccess)
	s.echo.GET("/projects/:project_id/issues/:issue_ref/changes/:change", s.changeDetail, s.nativeReadAccess)
	s.echo.POST("/projects/:project_id/issues/:issue_ref/changes/:change/versions/:version/review", s.changeReviewAction, s.nativeFormAuth)
	s.echo.GET("/projects/:project_id/issues/:issue_ref/runs/:attempt", s.nativeRunDetail, s.nativeReadAccess)
	s.echo.POST("/projects/:project_id/issues/:issue_ref/artifacts/:artifact/access", s.artifactAccess, s.nativeFormAuth)
	s.echo.GET("/projects/*", s.projectDashboard)
	s.echo.GET("/settings", s.settings)
	s.echo.GET("/api-keys", s.apiKeysPage)
	s.echo.GET("/reports", s.reports)
	s.echo.GET("/events", s.events)
	s.echo.GET("/onboarding", s.redirectToDashboard)
	s.echo.POST("/onboarding/tracker", s.onboardingTracker)
	s.echo.POST("/onboarding/credentials", s.onboardingCredentials)
	s.echo.POST("/onboarding/project", s.onboardingProject)
	s.echo.POST("/onboarding/agent", s.onboardingAgent)
	s.echo.POST("/onboarding/write", s.onboardingWrite)
	apiReadAuth := s.apiAuth(false)
	mcpReadAuth := s.mcpAPIAuth()
	apiMutateAuth := s.apiAuth(true)
	apiDashboardReadAuth := s.apiAuthWithOptions(apiAuthOptions{allowUICookie: true, allowDashboardHTMX: true})
	apiDashboardSSEReadAuth := s.apiAuthWithOptions(apiAuthOptions{allowUICookie: true, allowDashboardSSE: true})
	apiDashboardMutateAuth := s.apiAuthWithOptions(apiAuthOptions{mutating: true, allowUICookie: true, allowDashboardHTMX: true})
	apiKeyDashboardReadAuth := s.apiAuthWithOptions(apiAuthOptions{allowUICookie: true, allowDashboardHTMX: true, requireDashboardManagementToken: true})
	apiKeyDashboardMutateAuth := s.apiAuthWithOptions(apiAuthOptions{mutating: true, allowUICookie: true, allowDashboardHTMX: true, requireDashboardManagementToken: true})
	apiReadScope := s.requireScope(apikey.ScopeRead)
	apiWriteScope := s.requireScope(apikey.ScopeWrite)
	apiAdminScope := s.requireScope(apikey.ScopeAdmin)
	apiProjectWriteScope := s.requireProjectScope(apikey.ScopeWrite, "project_id")
	s.echo.POST("/projects/:project_id/issues/new", s.nativeIssueSubmit, s.nativeFormAuth, apiProjectWriteScope)
	s.echo.POST("/projects/:project_id/issues/:issue_ref/edit", s.nativeIssueSubmit, s.nativeFormAuth, apiProjectWriteScope)
	s.echo.POST("/fleet/runners/:runner", s.updateFleetRunner, apiKeyDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/fleet/hosts/:machine", s.updateFleetHost, apiKeyDashboardMutateAuth, apiAdminScope)
	s.echo.GET("/api/v1/state", s.apiState, apiReadAuth, apiReadScope)
	s.echo.GET("/api/v1/demo/scenarios", s.apiDemoScenarios, apiReadAuth, apiReadScope)
	s.echo.GET("/api/v1/timeseries", s.apiTimeSeries, apiReadAuth, apiReadScope)
	s.echo.POST("/api/v1/operator-tools/:tool_name", s.apiOperatorTool, apiReadAuth, apiReadScope)
	s.echo.Any("/mcp", echo.WrapHandler(s.mcpHTTP), mcpReadAuth)
	s.echo.POST("/api/v1/projects/:project_id/work-items", s.apiCreateWorkItem, apiMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/security-audits/dispositions", s.apiSecurityAuditDisposition, apiMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/projects/:project_id/budget/override", s.apiBudgetOverrideSet, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.DELETE("/api/v1/projects/:project_id/budget/override", s.apiBudgetOverrideClear, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/projects/:project_id/work-attempts/:attempt_id", s.apiWorkAttemptReceipt, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/projects/:project_id/issues/explanation", s.apiIssueExplanation, apiReadAuth, apiReadScope)
	s.echo.POST("/api/v1/projects/:project_id/issues/explanation", s.apiIssueParkAcknowledgement, apiMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/issues/progress-credit", s.apiIssueProgressCredit, apiMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/staleness-warnings/acknowledge", s.apiStalenessWarningsAcknowledgement, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/staleness-warnings/:warning_id/acknowledge", s.apiStalenessWarningAcknowledgement, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/work-attempts/:attempt_id/recovery", s.apiWorkAttemptRecovery, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/projects/:project_id/runs/:attempt/stop", s.apiStopRunDialog, apiDashboardReadAuth, apiReadScope)
	s.echo.POST("/api/v1/projects/:project_id/runs/:attempt/stop", s.apiStopRun, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/projects/*", s.apiProject, apiReadAuth, apiReadScope)
	s.echo.GET("/api/v1/keys", s.apiKeysList, apiKeyDashboardReadAuth, apiAdminScope)
	s.echo.POST("/api/v1/keys", s.apiKeysCreate, apiKeyDashboardMutateAuth, apiAdminScope)
	s.echo.GET("/api/v1/keys/:id/rotate", s.apiKeysRotateDialog, apiKeyDashboardReadAuth, apiAdminScope)
	s.echo.POST("/api/v1/keys/:id/rotate", s.apiKeysRotate, apiKeyDashboardMutateAuth, apiAdminScope)
	s.echo.DELETE("/api/v1/keys/:id", s.apiKeysRevoke, apiKeyDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/refresh", s.apiRefresh, apiDashboardMutateAuth, apiWriteScope)
	s.echo.POST("/api/v1/update/apply", s.apiUpdateApply, apiDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/capacity/clear", s.apiCapacityClear, apiDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/tracker/availability/clear", s.apiTrackerAvailabilityClear, apiDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/forge/availability/clear", s.apiForgeAvailabilityClear, apiDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/failure-breaker/canary", s.apiFailureBreakerCanary, apiDashboardMutateAuth, apiAdminScope)
	s.echo.POST("/api/v1/webhooks/github", s.githubWebhook)
	s.echo.POST("/api/v1/intake/:project_id/:source", s.intakeWebhook)
	s.echo.GET("/api/v1/refresh", s.methodNotAllowed, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/usage", s.apiUsage, apiReadAuth, apiReadScope)
	s.echo.GET("/api/v1/workflow/timeline", s.apiWorkflowTimeline, apiReadAuth, apiReadScope)
	s.echo.GET("/api/v1/ai-debug", s.aiDebugPrompt, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/card", s.apiBoardCard, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/card/core", s.apiBoardCardCore, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/receipt", s.apiBoardReceipt, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/conversation", s.apiBoardConversation, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/activity", s.apiBoardActivity, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/activity/events", s.apiBoardActivityEvents, apiDashboardSSEReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/session", s.apiBoardSession, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/session/events", s.apiBoardSessionEvents, apiDashboardSSEReadAuth, apiReadScope)
	s.echo.GET("/api/v1/board/session/history", s.apiBoardSessionHistory, apiDashboardReadAuth, apiReadScope)
	s.echo.GET("/api/v1/chat", s.apiChatPanel, apiDashboardReadAuth, apiReadScope)
	s.echo.POST("/api/v1/chat/messages", s.apiChatMessage, apiDashboardMutateAuth, apiWriteScope)
	s.echo.POST("/api/v1/chat/actions/:action_id/confirm", s.apiChatConfirm, apiDashboardMutateAuth, apiWriteScope)
	s.echo.POST("/api/v1/chat/actions/:action_id/reject", s.apiChatReject, apiDashboardMutateAuth, apiWriteScope)
	s.echo.POST("/api/v1/projects/:project_id/issues/:issue_id/priority", s.apiIssuePriority, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/kanban/move", s.apiKanbanMoveDialog, apiDashboardReadAuth, apiReadScope)
	s.echo.POST("/api/v1/kanban/move", s.apiKanbanMove, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/kanban/remove", s.apiKanbanRemove, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/kanban/comment", s.apiKanbanCommentDialog, apiDashboardReadAuth, apiReadScope)
	s.echo.POST("/api/v1/kanban/comment", s.apiKanbanComment, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.POST("/api/v1/kanban/comment/edit", s.apiKanbanCommentEdit, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.DELETE("/api/v1/kanban/comment", s.apiKanbanCommentDelete, apiDashboardMutateAuth, apiProjectWriteScope)
	s.echo.GET("/api/v1/*", s.apiIssue, apiReadAuth, apiReadScope)
}

func (s *Server) dashboard(c echo.Context) error {
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok {
		return s.demoDashboard(c, scenario)
	}
	ctx := c.Request().Context()
	snapshot, enriched := s.latestBoardSnapshot()
	data := s.dashboardFirstPaintData(ctx, snapshot, !enriched)
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.FleetPage(data))
}

func (s *Server) board(c echo.Context) error {
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok {
		return s.demoBoard(c, scenario)
	}
	ctx := c.Request().Context()
	snapshot, enriched := s.latestBoardSnapshot()
	data := s.boardFirstPaintData(ctx, snapshot, !enriched)
	data = s.withKanbanRefreshFeedback(data)
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.BoardPage(data))
}

func (s *Server) redirectToBoard(c echo.Context) error {
	return c.Redirect(http.StatusFound, "/")
}

// apiBoardCard renders the session detail sheet for one board card into
// the body-level sheet host. The scope param mirrors the board that
// opened the sheet so its kanban actions post against the same scope
// and success responses return the matching board fragment.
func (s *Server) apiBoardCard(c echo.Context) error {
	projectID := strings.TrimSpace(c.QueryParam("project"))
	data, demo, err := s.boardCardDashboardData(c, true)
	if err != nil {
		return err
	}
	card, ok := templates.FindBoardCard(data, projectID, c.QueryParam("issue"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "Card not found")
	}
	boardActions := c.QueryParam("actions") == "board"
	expanded := c.QueryParam("expanded") == "1"
	conversation := templates.BoardCardConversationData(data, card, boardActions, expanded)
	conversation = s.kanbanConversationShellData(conversation, demo)
	activityRequest := boardActivityRequest{
		ProjectID: projectID,
		Issue:     c.QueryParam("issue"),
		Limit:     defaultBoardActivityLimit,
	}
	issue := boardActivityIssue(data.Snapshot, activityRequest)
	activityData := boardActivityBaseData(issue, activityRequest)
	activityData.Pending = true
	sessionData := boardSessionSnapshotData(data.Snapshot, issue, projectID)
	return render(c, templates.BoardCardSheet(data, card, boardActions, expanded, conversation, activityData, sessionData))
}

func (s *Server) apiBoardCardCore(c echo.Context) error {
	projectID := strings.TrimSpace(c.QueryParam("project"))
	data, _, err := s.boardCardDashboardData(c, true)
	if err != nil {
		return err
	}
	card, ok := templates.FindBoardCard(data, projectID, c.QueryParam("issue"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "Card not found")
	}
	return render(c, templates.BoardCardSheetCore(data, card, c.QueryParam("actions") == "board"))
}

func (s *Server) apiBoardReceipt(c echo.Context) error {
	ctx := c.Request().Context()
	projectID := strings.TrimSpace(c.QueryParam("project"))
	data, demo, err := s.boardCardDashboardData(c, true)
	if err != nil {
		return err
	}
	card, ok := templates.FindBoardCard(data, projectID, c.QueryParam("issue"))
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "Card not found")
	}
	path := c.Request().URL.RequestURI()
	if demo {
		for _, receipt := range data.EfficiencyReceipts {
			if receipt.IssueID == card.IssueID || (receipt.Identifier != "" && receipt.Identifier == card.Identifier) {
				return render(c, templates.BoardEfficiencyReceipt(receipt, true, path, ""))
			}
		}
		return render(c, templates.BoardEfficiencyReceipt(efficiency.Receipt{}, false, path, ""))
	}
	receipt, err := s.store.EfficiencyReceipt(ctx, projectID, card.IssueID, card.Identifier)
	if err == nil {
		return render(c, templates.BoardEfficiencyReceipt(receipt, true, path, ""))
	}
	if errors.Is(err, sql.ErrNoRows) {
		return render(c, templates.BoardEfficiencyReceipt(efficiency.Receipt{}, false, path, ""))
	}
	s.logger.WarnContext(ctx, "efficiency receipt query failed", slog.Any("error", err))
	return render(c, templates.BoardEfficiencyReceipt(efficiency.Receipt{}, false, path, "Efficiency receipt is temporarily unavailable."))
}

func (s *Server) boardCardDashboardData(c echo.Context, local bool) (templates.DashboardData, bool, error) {
	ctx := c.Request().Context()
	projectID := strings.TrimSpace(c.QueryParam("project"))
	projectScope := c.QueryParam("scope") == "project" && projectID != ""
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return templates.DashboardData{}, false, err
	} else if ok {
		data := s.demoDashboardData(ctx, scenario)
		if projectScope {
			projectScenario := scenario
			projectScenario.ProjectID = projectID
			if scoped, found := s.demoProjectDashboardData(ctx, projectScenario); found {
				data = scoped
			}
		}
		return data, true, nil
	}
	if local {
		snapshot, enriched := s.latestBoardSnapshot()
		data := s.boardFirstPaintData(ctx, snapshot, !enriched)
		if projectScope {
			if scoped, ok := s.projectFirstPaintData(ctx, projectID, snapshot, !enriched); ok {
				data = scoped
			}
		}
		return data, false, nil
	}
	snapshot := s.latestSnapshot(ctx)
	data := s.boardData(ctx, snapshot)
	if projectScope {
		if scoped, ok := s.projectDashboardData(ctx, projectID, snapshot); ok {
			data = scoped
		}
	}
	return data, false, nil
}

func (s *Server) healthDashboard(c echo.Context) error {
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok {
		return s.demoHealthDashboard(c, scenario)
	}
	ctx := c.Request().Context()
	data := s.healthDashboardData(ctx, s.latestSnapshot(ctx))
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.HealthPageV2(data))
}

func (s *Server) analyticsDashboard(c echo.Context) error {
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok {
		return s.demoAnalyticsDashboard(c, scenario)
	}
	ctx := c.Request().Context()
	data := s.analyticsDashboardData(ctx, s.latestSnapshot(ctx))
	data.AnalyticsKind = c.QueryParam("kind")
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.AnalyticsPageV2(data))
}

func (s *Server) diagnosticsDashboard(c echo.Context) error {
	ctx := c.Request().Context()
	data := s.diagnosticsDashboardData(ctx, s.latestSnapshot(ctx))
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.DiagnosticsPageV2(data))
}

func (s *Server) projectDashboard(c echo.Context) error {
	ctx := c.Request().Context()
	projectID, view := projectRouteViewParam(c)
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok {
		if strings.TrimSpace(scenario.ProjectID) == "" {
			scenario.ProjectID = projectID
		}
		return s.demoProjectDashboard(c, scenario, view)
	}
	snapshot, enriched := s.latestBoardSnapshot()
	data, ok := s.projectFirstPaintData(ctx, projectID, snapshot, !enriched)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "Project not found")
	}
	switch view {
	case "kanban":
		data.ActiveNav = "kanban"
		data.Title = s.projectPageTitle(data, "Work")
		data = s.withKanbanRefreshFeedback(data)
		applyDashboardPreferences(c.Request(), &data)
		return render(c, templates.ProjectBoardPage(data))
	case "runs":
		data.ActiveNav = "runs"
		data.Title = s.projectPageTitle(data, "Runs")
		applyDashboardPreferences(c.Request(), &data)
		return render(c, templates.ProjectRunsPageV2(data))
	case "diagnostics":
		data.ActiveNav = "diagnostics"
		data.Title = s.projectPageTitle(data, "Diagnostics")
		applyDashboardPreferences(c.Request(), &data)
		return render(c, templates.ProjectDiagnosticsPageV2(data))
	case "configuration":
		settingsData := s.settingsData(ctx, projectID)
		settingsData.ActiveNav = "configuration"
		settingsData.Title = s.projectPageTitle(data, "Configuration")
		applySettingsPreferences(c.Request(), &settingsData)
		return render(c, templates.Settings(settingsData))
	}
	data.ActiveNav = "overview"
	applyDashboardPreferences(c.Request(), &data)
	return render(c, templates.ProjectOverviewPage(data))
}

func dashboardSidebarCollapsed(r *http.Request) bool {
	cookie, err := r.Cookie(sidebarStateCookieName)
	if err != nil {
		return false
	}
	return strings.TrimSpace(cookie.Value) == "false"
}

func dashboardTheme(r *http.Request) string {
	cookie, err := r.Cookie(themeCookieName)
	if err != nil {
		return ""
	}
	if strings.TrimSpace(cookie.Value) == "light" {
		return "light"
	}
	return ""
}

func dashboardDensity(r *http.Request) string {
	cookie, err := r.Cookie(densityCookieName)
	if err != nil {
		return "cozy"
	}
	switch strings.TrimSpace(cookie.Value) {
	case "compact", "cozy", "comfy":
		return strings.TrimSpace(cookie.Value)
	default:
		return "cozy"
	}
}

func applyDashboardPreferences(r *http.Request, data *templates.DashboardData) {
	data.SidebarCollapsed = dashboardSidebarCollapsed(r)
	data.Theme = dashboardTheme(r)
	data.Density = dashboardDensity(r)
}

func applySettingsPreferences(r *http.Request, data *templates.SettingsData) {
	data.SidebarCollapsed = dashboardSidebarCollapsed(r)
	data.Theme = dashboardTheme(r)
	data.Density = dashboardDensity(r)
}

func applyReportsPreferences(r *http.Request, data *templates.ReportsData) {
	data.SidebarCollapsed = dashboardSidebarCollapsed(r)
	data.Theme = dashboardTheme(r)
	data.Density = dashboardDensity(r)
}

func applyLibraryPreferences(r *http.Request, data *templates.LibraryData) {
	data.SidebarCollapsed = dashboardSidebarCollapsed(r)
	data.Theme = dashboardTheme(r)
	data.Density = dashboardDensity(r)
}

func applyAPIKeysPreferences(r *http.Request, data *templates.APIKeysData) {
	data.SidebarCollapsed = dashboardSidebarCollapsed(r)
	data.Theme = dashboardTheme(r)
	data.Density = dashboardDensity(r)
}

func projectRouteParam(c echo.Context) string {
	return cleanProjectRouteParam(c.Param("*"))
}

func (s *Server) projectPageTitle(data templates.DashboardData, title string) string {
	name := strings.TrimSpace(data.ProjectName)
	if name == "" {
		name = strings.TrimSpace(data.ProjectID)
	}
	if name == "" {
		name = "Project"
	}
	return instancePageTitle(s.instanceName(), name+" "+strings.TrimSpace(title)+" - Detent")
}

func projectRouteViewParam(c echo.Context) (string, string) {
	projectID := strings.Trim(strings.TrimSpace(projectEscapedRouteParam(c)), "/")
	for _, view := range []string{"kanban", "runs", "diagnostics", "configuration"} {
		suffix := "/" + view
		if strings.HasSuffix(projectID, suffix) {
			return cleanProjectRouteParam(strings.Trim(strings.TrimSuffix(projectID, suffix), "/")), view
		}
	}
	return cleanProjectRouteParam(projectID), "overview"
}

func projectEscapedRouteParam(c echo.Context) string {
	const projectsPrefix = "/projects/"
	path := c.Request().URL.EscapedPath()
	if strings.HasPrefix(path, projectsPrefix) {
		return strings.TrimPrefix(path, projectsPrefix)
	}
	return c.Param("*")
}

func cleanProjectRouteParam(projectID string) string {
	projectID = strings.Trim(strings.TrimSpace(projectID), "/")
	if unescaped, err := url.PathUnescape(projectID); err == nil {
		return strings.Trim(strings.TrimSpace(unescaped), "/")
	}
	return projectID
}

func (s *Server) dashboardData(ctx context.Context, snapshot telemetry.Snapshot) templates.DashboardData {
	instanceName := s.instanceName()
	snapshot = s.fleetKanbanSnapshotWithPendingStates(snapshot)
	return templates.DashboardData{
		RunnerFleetEnabled: s.runnerFleet != nil,
		Title:              instancePageTitle(instanceName, "Detent"),
		ApplicationName:    applicationName(instanceName),
		InstanceName:       instanceName,
		Version:            s.version,
		Build:              s.build,
		ConnectorName:      s.connector.Name(),
		DashboardURL:       s.dashboardURL,
		Snapshot:           snapshot,
		Projects:           s.projectSmallMultiples(ctx, snapshot),
		Kanban:             s.dashboardKanbanData(ctx, "", snapshot),
		Assets:             s.assets.templatePaths(),
		ActiveNav:          "fleet",
	}
}

func (s *Server) boardData(ctx context.Context, snapshot telemetry.Snapshot) templates.DashboardData {
	data := s.dashboardData(ctx, snapshot)
	data.ActiveNav = "board"
	data.Title = instancePageTitle(s.instanceName(), "Detent")
	return data
}

func (s *Server) boardFirstPaintData(ctx context.Context, snapshot telemetry.Snapshot, pendingEnrichment bool) templates.DashboardData {
	data := s.dashboardFirstPaintData(ctx, snapshot, pendingEnrichment)
	data.ActiveNav = "board"
	return data
}

func (s *Server) dashboardFirstPaintData(ctx context.Context, snapshot telemetry.Snapshot, pendingEnrichment bool) templates.DashboardData {
	instanceName := s.instanceName()
	snapshot = s.fleetKanbanSnapshotWithPendingStates(snapshot)
	return templates.DashboardData{
		Title:              instancePageTitle(instanceName, "Detent"),
		ApplicationName:    applicationName(instanceName),
		InstanceName:       instanceName,
		Version:            s.version,
		Build:              s.build,
		ConnectorName:      s.connector.Name(),
		DashboardURL:       s.dashboardURL,
		Snapshot:           snapshot,
		Projects:           s.cachedProjectSmallMultiples(snapshot),
		Kanban:             s.dashboardKanbanData(ctx, "", snapshot),
		Assets:             s.assets.templatePaths(),
		ActiveNav:          "fleet",
		PendingEnrichment:  pendingEnrichment,
		RunnerFleetEnabled: s.runnerFleet != nil,
	}
}

func (s *Server) healthDashboardData(ctx context.Context, snapshot telemetry.Snapshot) templates.DashboardData {
	data := s.dashboardData(ctx, snapshot)
	data.ActiveNav = "health"
	data.Title = instancePageTitle(s.instanceName(), "Health - Detent")
	return data
}

func (s *Server) analyticsDashboardData(ctx context.Context, snapshot telemetry.Snapshot) templates.DashboardData {
	data := s.dashboardData(ctx, snapshot)
	data.ActiveNav = "analytics"
	data.Title = instancePageTitle(s.instanceName(), "Analytics - Detent")
	return data
}

func (s *Server) diagnosticsDashboardData(ctx context.Context, snapshot telemetry.Snapshot) templates.DashboardData {
	data := s.dashboardData(ctx, snapshot)
	data.ActiveNav = "diagnostics"
	data.Title = instancePageTitle(s.instanceName(), "Diagnostics - Detent")
	return data
}

func (s *Server) projectDashboardData(ctx context.Context, projectID string, snapshot telemetry.Snapshot) (templates.DashboardData, bool) {
	projects := s.projectSmallMultiples(ctx, snapshot)
	return s.projectDashboardDataFromProjects(ctx, projectID, snapshot, projects, true)
}

func (s *Server) projectFirstPaintData(
	ctx context.Context,
	projectID string,
	snapshot telemetry.Snapshot,
	pendingEnrichment bool,
) (templates.DashboardData, bool) {
	projects := s.cachedProjectSmallMultiples(snapshot)
	data, ok := s.projectDashboardDataFromProjects(ctx, projectID, snapshot, projects, false)
	data.PendingEnrichment = pendingEnrichment
	return data, ok
}

func (s *Server) projectDashboardDataFromProjects(
	ctx context.Context,
	projectID string,
	snapshot telemetry.Snapshot,
	projects []templates.ProjectSmallMultiple,
	loadDetails bool,
) (templates.DashboardData, bool) {
	project, ok := s.dashboardProject(projectID, projects, snapshot)
	if !ok {
		return templates.DashboardData{}, false
	}

	scopedSnapshot := projectScopedSnapshotForProject(snapshot, telemetry.Project{
		ID:          project.ID,
		DisplayName: project.Name,
		URL:         project.URL,
		Color:       project.Color,
		Pool:        project.Pool,
	})
	scopedSnapshot = applyProjectBudgetSnapshot(scopedSnapshot, project)
	if target, _, _ := s.kanbanActionTarget(project.ID); target.key != "" {
		scopedSnapshot = s.kanbanSnapshotWithPendingStates(target.key, project.ID, scopedSnapshot)
	}
	if loadDetails {
		scopedSnapshot.WorkflowMetrics = s.snapshotWorkflowMetrics(ctx, scopedSnapshot)
	}
	name := strings.TrimSpace(project.Name)
	if name == "" {
		name = strings.TrimSpace(project.ID)
	}
	instanceName := s.instanceName()
	data := templates.DashboardData{
		RunnerFleetEnabled:        s.runnerFleet != nil,
		Title:                     instancePageTitle(instanceName, name+" - Detent"),
		ApplicationName:           applicationName(instanceName),
		InstanceName:              instanceName,
		Version:                   s.version,
		Build:                     s.build,
		ConnectorName:             s.connector.Name(),
		DashboardURL:              s.dashboardURL,
		Snapshot:                  scopedSnapshot,
		Projects:                  projects,
		Kanban:                    s.dashboardKanbanData(ctx, project.ID, scopedSnapshot),
		Assets:                    s.assets.templatePaths(),
		ActiveNav:                 "project",
		ProjectID:                 strings.TrimSpace(project.ID),
		ProjectName:               name,
		ProjectPaused:             project.Paused,
		ProjectPauseReason:        project.PauseReason,
		ProjectPauseIssue:         project.PauseIssue,
		ProjectPauseUntil:         project.PauseUntil,
		ProjectPauseExitEvaluated: project.PauseExitEvaluated,
		ProjectPauseExitEvaluable: project.PauseExitEvaluable,
		ProjectPauseExitError:     project.PauseExitError,
		ProjectPauseExitResolver:  project.PauseExitResolver,
	}
	if loadDetails {
		receipts, err := s.store.ListEfficiencyReceipts(ctx, efficiency.Query{ProjectID: project.ID, Limit: 100, IncludeInProgress: true})
		if err != nil {
			s.logger.Warn("efficiency receipts query failed", slog.Any("error", err))
		} else {
			data.EfficiencyReceipts = receipts
		}
	}
	return data, true
}

func applyProjectBudgetSnapshot(snapshot telemetry.Snapshot, project templates.ProjectSmallMultiple) telemetry.Snapshot {
	if !project.BudgetEnabled {
		return snapshot
	}
	dayCap := project.PerDayMaxUSD
	issueCap := project.PerIssueMaxUSD
	snapshot.Budget.Enabled = true
	snapshot.Budget.PerDayMaxUSD = &dayCap
	snapshot.Budget.PerIssueMaxUSD = &issueCap
	snapshot.Budget.CurrentSpendUSD = project.CurrentSpendUSD
	snapshot.Budget.PeriodEnd = project.BudgetResetAt
	snapshot.Budget.PeriodStart = project.BudgetResetAt.AddDate(0, 0, -1)
	return snapshot
}

func (s *Server) withKanbanRefreshFeedback(data templates.DashboardData) templates.DashboardData {
	data = s.withKanbanRevertFeedback(data)
	if s == nil || s.kanbanRefreshes == nil {
		return data
	}
	data.Kanban = s.kanbanRefreshes.apply(kanbanRefreshFeedbackKey(data), data.Kanban, data.Snapshot)
	return data
}

func (s *Server) withKanbanRevertFeedback(data templates.DashboardData) templates.DashboardData {
	if s == nil || s.kanbanMutations == nil || strings.TrimSpace(data.Kanban.Feedback) != "" {
		return data
	}
	notices := s.kanbanRevertNotices(data)
	if len(notices) == 0 {
		return data
	}
	data.Kanban.Feedback = kanbanRevertFeedback(notices)
	data.Kanban.FeedbackKind = "error"
	return data
}

func (s *Server) kanbanRevertNotices(data templates.DashboardData) []kanbanstate.RevertNotice {
	if s == nil || s.kanbanMutations == nil {
		return nil
	}
	projectID := strings.TrimSpace(data.ProjectID)
	var notices []kanbanstate.RevertNotice
	if projectID != "" {
		notices = s.kanbanMutations.ConsumeRevertNotices("project:"+projectID, projectID)
	} else {
		notices = s.kanbanMutations.ConsumeRevertNotices("", "")
	}
	if s.logger == nil {
		return notices
	}
	for _, notice := range notices {
		s.logger.Warn("kanban move reverted",
			"project", notice.ProjectID,
			"issue_id", notice.IssueID,
			"identifier", notice.Identifier,
			"from_state", notice.From,
			"to_state", notice.To,
			"data_seq", notice.DataSeq,
			"contradiction_count", notice.Contradictions,
		)
	}
	return notices
}

func kanbanRevertFeedback(notices []kanbanstate.RevertNotice) string {
	messages := make([]string, 0, len(notices))
	for _, notice := range notices {
		identifier := strings.TrimSpace(notice.Identifier)
		if identifier == "" {
			identifier = "card"
		}
		from := strings.TrimSpace(notice.From)
		if from == "" {
			from = "the requested state"
		}
		to := strings.TrimSpace(notice.To)
		if to == "" {
			to = "the tracker state"
		}
		messages = append(messages, fmt.Sprintf("Move of %s to %s was not confirmed by the tracker; reverted to %s.", identifier, from, to))
	}
	return strings.Join(messages, " ")
}

func kanbanRefreshFeedbackKey(data templates.DashboardData) string {
	if projectID := strings.TrimSpace(data.ProjectID); projectID != "" {
		return "project:" + projectID
	}
	return "fleet"
}

func (s *Server) dashboardProject(selectedProjectID string, projects []templates.ProjectSmallMultiple, snapshot telemetry.Snapshot) (templates.ProjectSmallMultiple, bool) {
	selectedProjectID = strings.TrimSpace(selectedProjectID)
	if selectedProjectID == "" {
		return templates.ProjectSmallMultiple{}, false
	}
	for _, project := range projects {
		if strings.TrimSpace(project.ID) == selectedProjectID {
			return project, true
		}
	}
	if projectSnapshot, ok := projectSnapshotForID(snapshot, selectedProjectID); ok {
		return templates.ProjectSmallMultiple{
			ID:    projectID(projectSnapshot.Project),
			Name:  strings.TrimSpace(projectSnapshot.Project.DisplayName),
			URL:   strings.TrimSpace(projectSnapshot.Project.URL),
			Color: strings.TrimSpace(projectSnapshot.Project.Color),
		}, true
	}
	return templates.ProjectSmallMultiple{}, false
}

func (s *Server) sidebarProjectContext(selectedProjectID string, projects []templates.ProjectSmallMultiple, snapshot telemetry.Snapshot) (string, string, bool) {
	project, ok := s.dashboardProject(selectedProjectID, projects, snapshot)
	if !ok {
		return "", "", false
	}
	name := strings.TrimSpace(project.Name)
	if name == "" {
		name = strings.TrimSpace(project.ID)
	}
	return strings.TrimSpace(project.ID), name, true
}

func (s *Server) latestSnapshot(ctx context.Context) telemetry.Snapshot {
	snapshot, ok := s.hub.Latest()
	if !ok {
		return s.withManualRefresh(s.enrichSnapshot(ctx, telemetry.Snapshot{})).WithFreshness(s.now())
	}
	return s.withManualRefresh(s.cachedEnrichedSnapshot(ctx, snapshot)).WithFreshness(s.now())
}

func (s *Server) latestBoardSnapshot() (telemetry.Snapshot, bool) {
	snapshot, ok := s.hub.Latest()
	if !ok {
		return s.withManualRefresh(telemetry.Snapshot{}).WithFreshness(s.now()), false
	}
	if enriched, ok := s.snapshots.get(snapshot); ok {
		return s.withManualRefresh(enriched).WithFreshness(s.now()), true
	}
	return s.withManualRefresh(snapshot).WithFreshness(s.now()), false
}

func (s *Server) health(c echo.Context) error {
	if _, _, err := s.demoScenarioOrError(c); err != nil {
		return err
	}
	lifecycle := s.startupLifecycle.State()
	status := "ok"
	now := s.now().UTC()
	sessionsRemaining := 0
	updateStatus := telemetry.Update{}
	backendOutages := []telemetry.BackendOutage{}
	failureBreakers := []telemetry.FailureBreaker{}
	dispatchLoops := []telemetry.DispatchLoop{}
	trackerUnavailable := []telemetry.TrackerCondition{}
	forgeUnavailable := []telemetry.ForgeCondition{}
	ciUnavailable := []telemetry.CICondition{}
	stalenessWarnings := []telemetry.StalenessWarning{}
	strandedActiveIssues := []telemetry.StrandedIssue{}
	cleanupFaults := []telemetry.CleanupFault{}
	dispatch := telemetry.DispatchStatus{}
	dispatchStalls := []telemetry.DispatchStatus{}
	notificationFailures := []healthnotify.Failure{}
	refreshFailures := []telemetry.RefreshFailure{}
	refresh := telemetry.Refresh{}
	memoryPressure := telemetry.MemoryPressure{}
	ioPressure := telemetry.IOPressure{}
	cpuPressure := telemetry.CPUPressure{}
	agentMemory := []healthAgentMemory{}
	latestSnapshot := telemetry.Snapshot{}
	snapshotGeneratedAt := time.Time{}
	snapshotAgeSeconds := int64(0)
	if s.hub != nil {
		if snapshot, ok := s.hub.Latest(); ok {
			snapshot = snapshot.WithFreshness(now)
			latestSnapshot = snapshot
			refreshFailures = snapshot.RefreshFailures()
			snapshotGeneratedAt = snapshot.GeneratedAt
			snapshotAgeSeconds = snapshot.AgeSeconds(now)
			refresh = snapshot.Refresh
			memoryPressure = snapshot.MemoryPressure
			ioPressure = snapshot.IOPressure
			cpuPressure = snapshot.CPUPressure
			for _, running := range snapshot.Running {
				agentMemory = append(agentMemory, healthAgentMemory{
					ProjectID:       running.ProjectID,
					IssueIdentifier: running.Identifier,
					RSSBytes:        running.RSSBytes,
					RSSCeilingBytes: running.RSSCeilingBytes,
					ObservedAt:      running.RSSObservedAt,
				})
			}
			updateStatus = snapshot.Update
			backendOutages = append(backendOutages, snapshot.BackendOutages...)
			failureBreakers = append(failureBreakers, snapshot.FailureBreakers...)
			dispatchLoops = append(dispatchLoops, snapshot.DispatchLoops...)
			trackerUnavailable = append(trackerUnavailable, snapshot.TrackerUnavailable...)
			forgeUnavailable = append(forgeUnavailable, snapshot.ForgeUnavailable...)
			ciUnavailable = append(ciUnavailable, snapshot.CIUnavailable...)
			stalenessWarnings = append(stalenessWarnings, snapshot.StalenessWarnings...)
			strandedActiveIssues = append(strandedActiveIssues, snapshot.StrandedActiveIssues...)
			cleanupFaults = append(cleanupFaults, snapshot.CleanupFaults...)
			dispatch = snapshot.Dispatch
			dispatchStalls = append(dispatchStalls, snapshot.DispatchStalls...)
			if snapshot.Shutdown.Draining {
				status = "draining"
				sessionsRemaining = snapshot.Shutdown.SessionsRemaining
			}
		}
	}
	tickLiveness := []telemetry.TickLiveness{}
	if s.tickLiveness != nil {
		tickLiveness = s.tickLiveness.TickLiveness(now)
	}
	if s.healthNotifications != nil {
		failures, err := s.healthNotifications.Failures(c.Request().Context())
		if err != nil {
			s.logger.Warn("read health notification failures failed", "error", err)
		} else {
			notificationFailures = failures
		}
	}
	checks := map[string]string{
		"hub":       configuredStatus(s.hub),
		"store":     configuredStatus(s.store),
		"registry":  configuredStatus(s.registry),
		"connector": configuredStatus(s.connector),
	}
	orphanedProcesses, orphanErr := s.orphanedAgentProcesses(c.Request().Context(), latestSnapshot, now)
	if orphanErr != nil {
		checks["worker_processes"] = "unavailable: " + orphanErr.Error()
		s.logger.Warn("inspect orphaned agent processes failed", "error", orphanErr)
	} else {
		checks["worker_processes"] = configuredStatus(s.workerProcesses)
	}
	if s.demo != nil {
		checks["demo"] = DemoModeScreenshots
		checks["demo_clock"] = s.demo.clock
	}
	projectStatus, projectHealth := s.projectHealth()
	stateEndpoints := s.stateEndpoints.health()
	faultDispatchStalls := faultDispatchStatuses(dispatchStalls)
	actionableBreakers := faultFailureBreakers(failureBreakers)
	actionableOutages := faultBackendOutages(backendOutages)
	projectHealth = applyNeedsAttentionToProjectHealth(projectHealth, faultDispatchStalls, trackerUnavailable, forgeUnavailable, ciUnavailable, actionableBreakers, dispatchLoops, actionableOutages, tickLiveness, refreshFailures)
	var budgets []healthBudget
	var workflows []healthWorkflowSource
	if status != "draining" {
		budgets = s.enforcedBudgets()
		workflows = s.workflowSources()
		if len(trackerUnavailable) > 0 || len(forgeUnavailable) > 0 || len(ciUnavailable) > 0 || len(faultDispatchStalls) > 0 || len(actionableBreakers) > 0 || len(dispatchLoops) > 0 || len(actionableOutages) > 0 || len(faultStalenessWarnings(stalenessWarnings)) > 0 || len(strandedActiveIssues) > 0 || len(cleanupFaults) > 0 || strings.TrimSpace(updateStatus.LastError) != "" || tickLivenessNeedsAttention(tickLiveness) || len(refreshFailures) > 0 || memoryPressure.DispatchHeld || ioPressure.CapacityConstrained || ioPressure.DispatchHeld || cpuPressure.CapacityConstrained || cpuPressure.DispatchHeld || orphanedProcesses.Count > 0 {
			status = "needs_attention"
		}
		if pauseExitNeedsAttention(projectHealth) {
			status = "needs_attention"
		}
		if stateEndpoints.Fleet.Status == "degraded" || stateEndpoints.Project.Status == "degraded" {
			status = "needs_attention"
		}
	}
	updateStatus.State = updateStatus.DisplayState(now)
	httpStatus := http.StatusOK
	ready := lifecycle == StartupLifecycleReady
	if !ready {
		httpStatus = http.StatusServiceUnavailable
		status = "not_ready"
	}
	return c.JSON(httpStatus, healthResponse{
		Status:                 status,
		Ready:                  ready,
		Lifecycle:              lifecycle,
		Version:                s.build.Version,
		Commit:                 s.build.Commit,
		ProjectStatus:          projectStatus,
		Mode:                   string(s.mode),
		Connector:              s.connectorName(),
		SessionsRemaining:      sessionsRemaining,
		Update:                 updateStatus,
		Checks:                 checks,
		Environment:            healthEnvironment{Path: os.Getenv("PATH")},
		Budgets:                budgets,
		Workflows:              workflows,
		TrackerUnavailable:     trackerUnavailable,
		ForgeUnavailable:       forgeUnavailable,
		CIUnavailable:          ciUnavailable,
		BackendOutages:         backendOutages,
		FailureBreakers:        failureBreakers,
		DispatchLoops:          dispatchLoops,
		StalenessWarnings:      stalenessWarnings,
		StrandedIssues:         strandedActiveIssues,
		CleanupFaults:          cleanupFaults,
		Dispatch:               dispatch,
		DispatchStalls:         dispatchStalls,
		NotificationFailures:   notificationFailures,
		RefreshFailures:        refreshFailures,
		Projects:               projectHealth,
		Refresh:                refresh,
		MemoryPressure:         memoryPressure,
		IOPressure:             ioPressure,
		CPUPressure:            cpuPressure,
		AgentMemory:            agentMemory,
		SnapshotGeneratedAt:    snapshotGeneratedAt,
		SnapshotAgeSeconds:     snapshotAgeSeconds,
		StateEndpoints:         stateEndpoints,
		TickLiveness:           tickLiveness,
		OrphanedAgentProcesses: orphanedProcesses,
	})
}

func faultDispatchStatuses(statuses []telemetry.DispatchStatus) []telemetry.DispatchStatus {
	faults := make([]telemetry.DispatchStatus, 0, len(statuses))
	for _, status := range statuses {
		if observability.Normalize(status.Class, observability.Dispatch(status.Stalled, status.WaitReasonCode)) == observability.ClassFault {
			faults = append(faults, status)
		}
	}
	return faults
}

func faultFailureBreakers(breakers []telemetry.FailureBreaker) []telemetry.FailureBreaker {
	faults := make([]telemetry.FailureBreaker, 0, len(breakers))
	for _, breaker := range breakers {
		parkedItems := 0
		for _, item := range breaker.Items {
			if item.Parked {
				parkedItems++
			}
		}
		if observability.FailureBreaker(breaker.EligibleCandidateCount, len(breaker.Items), parkedItems) == observability.ClassFault {
			faults = append(faults, breaker)
		}
	}
	return faults
}

func faultBackendOutages(outages []telemetry.BackendOutage) []telemetry.BackendOutage {
	faults := make([]telemetry.BackendOutage, 0, len(outages))
	for _, outage := range outages {
		if observability.BackendOutage(outage.Kind) == observability.ClassFault {
			faults = append(faults, outage)
		}
	}
	return faults
}

func faultStalenessWarnings(warnings []telemetry.StalenessWarning) []telemetry.StalenessWarning {
	faults := make([]telemetry.StalenessWarning, 0, len(warnings))
	for _, warning := range warnings {
		if observability.Normalize(warning.Class, observability.Staleness(warning.WaitingOnHuman)) == observability.ClassFault {
			faults = append(faults, warning)
		}
	}
	return faults
}

func (s *Server) orphanedAgentProcesses(ctx context.Context, snapshot telemetry.Snapshot, now time.Time) (telemetry.OrphanedAgentProcesses, error) {
	summary := telemetry.OrphanedAgentProcesses{Processes: []telemetry.OrphanedAgentProcess{}}
	if s.workerProcesses == nil || s.observeProcesses == nil {
		return summary, nil
	}
	processes, err := s.workerProcesses.ListActiveWorkerProcesses(ctx)
	if err != nil {
		return summary, fmt.Errorf("list worker process registry: %w", err)
	}
	liveSessions := make(map[int64]struct{}, len(snapshot.Running))
	for _, running := range snapshot.Running {
		if running.DetentSessionID > 0 {
			liveSessions[running.DetentSessionID] = struct{}{}
		}
	}
	candidates := make([]store.WorkerProcess, 0, len(processes))
	identities := make([]procgroup.Identity, 0, len(processes))
	for _, process := range processes {
		if process.CompletedAt.IsZero() {
			if _, live := liveSessions[process.SessionID]; live {
				continue
			}
			if snapshot.GeneratedAt.IsZero() || snapshot.GeneratedAt.Before(process.StartedAt) {
				continue
			}
		}
		candidates = append(candidates, process)
		identities = append(identities, procgroup.Identity{PID: process.PID, GroupID: process.GroupID, StartedAt: process.StartedAt})
	}
	observations, err := s.observeProcesses(identities)
	if err != nil {
		return summary, fmt.Errorf("observe worker process registry: %w", err)
	}
	if len(observations) != len(candidates) {
		return summary, fmt.Errorf("observe worker process registry: got %d observations for %d processes", len(observations), len(candidates))
	}
	for index, observation := range observations {
		if !observation.Alive || observation.Stale {
			continue
		}
		process := candidates[index]
		ageSeconds := int64(0)
		if now.After(process.StartedAt) {
			ageSeconds = int64(now.Sub(process.StartedAt) / time.Second)
		}
		summary.Count += observation.ProcessCount
		summary.SessionCount++
		summary.TotalRSSBytes += observation.RSSBytes
		summary.Processes = append(summary.Processes, telemetry.OrphanedAgentProcess{
			SessionID:    process.SessionID,
			IssueID:      process.IssueID,
			Identifier:   process.Identifier,
			PID:          process.PID,
			GroupID:      process.GroupID,
			StartedAt:    process.StartedAt,
			AgeSeconds:   ageSeconds,
			RSSBytes:     observation.RSSBytes,
			ProcessCount: observation.ProcessCount,
			FinalState:   process.FinalState,
		})
	}
	return summary, nil
}

func applyNeedsAttentionToProjectHealth(projects []healthProject, stalls []telemetry.DispatchStatus, trackerUnavailable []telemetry.TrackerCondition, forgeUnavailable []telemetry.ForgeCondition, ciUnavailable []telemetry.CICondition, breakers []telemetry.FailureBreaker, loops []telemetry.DispatchLoop, outages []telemetry.BackendOutage, tickLiveness []telemetry.TickLiveness, refreshFailures []telemetry.RefreshFailure) []healthProject {
	needsAttention := make(map[string]struct{}, len(stalls)+len(trackerUnavailable)+len(forgeUnavailable)+len(ciUnavailable)+len(breakers)+len(loops)+len(outages)+len(tickLiveness))
	refreshByProject := make(map[string]telemetry.RefreshFailure, len(refreshFailures))
	for _, stall := range stalls {
		if projectID := strings.TrimSpace(stall.ProjectID); projectID != "" && stall.Stalled {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, condition := range ciUnavailable {
		if projectID := strings.TrimSpace(condition.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, condition := range trackerUnavailable {
		if projectID := strings.TrimSpace(condition.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, condition := range forgeUnavailable {
		if projectID := strings.TrimSpace(condition.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, breaker := range breakers {
		if projectID := strings.TrimSpace(breaker.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, loop := range loops {
		if projectID := strings.TrimSpace(loop.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, outage := range outages {
		if projectID := strings.TrimSpace(outage.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, liveness := range tickLiveness {
		if projectID := strings.TrimSpace(liveness.ProjectID); projectID != "" && liveness.Status == telemetry.TickLivenessStatusNeedsAttention {
			needsAttention[projectID] = struct{}{}
		}
	}
	for _, failure := range refreshFailures {
		if projectID := strings.TrimSpace(failure.ProjectID); projectID != "" {
			needsAttention[projectID] = struct{}{}
			refreshByProject[projectID] = failure
		}
	}
	for index := range projects {
		projectID := strings.TrimSpace(projects[index].ProjectID)
		if _, ok := needsAttention[projectID]; ok {
			projects[index].Status = "needs_human_attention"
		}
		if failure, ok := refreshByProject[projectID]; ok {
			projects[index].LastError = failure.LastError
			if failure.LastErrorAt != nil {
				projects[index].LastErrorAt = failure.LastErrorAt.UTC()
			}
		}
	}
	return projects
}

func tickLivenessNeedsAttention(values []telemetry.TickLiveness) bool {
	for _, value := range values {
		if value.Status == telemetry.TickLivenessStatusNeedsAttention {
			return true
		}
	}
	return false
}

func pauseExitNeedsAttention(projects []healthProject) bool {
	needsAttention := false
	for index := range projects {
		if projects[index].PauseExit != nil && projects[index].PauseExit.NeedsAttention {
			projects[index].Status = "needs_human_attention"
			needsAttention = true
		}
	}
	return needsAttention
}

func (s *Server) projectHealth() (string, []healthProject) {
	if s.registry == nil {
		return "unknown", nil
	}

	status := "ok"
	projectHealth := s.registry.Health()
	activeHoursByProject := make(map[string]telemetry.ActiveHours)
	now := s.now().UTC()
	for _, trackedProject := range s.registry.List() {
		if trackedProject == nil {
			continue
		}
		if activeHours, err := trackedProject.ActiveHoursStatus(now); err == nil {
			activeHoursByProject[string(trackedProject.ID())] = telemetry.ActiveHoursFromStatus(activeHours)
		}
	}
	projects := make([]healthProject, 0, len(projectHealth))
	for _, health := range projectHealth {
		switch health.Status {
		case project.HealthStatusDegraded:
			status = "degraded"
		case project.HealthStatusInitializing:
			if status == "ok" {
				status = "initializing"
			}
		}
		projects = append(projects, healthProject{
			ProjectID:    health.Project.ID,
			Status:       string(health.Status),
			LastError:    health.LastError,
			LastErrorAt:  health.LastErrorAt,
			NextRetryAt:  health.NextRetryAt,
			RetryStopped: health.RetryStopped,
			ActiveHours:  activeHoursByProject[health.Project.ID],
			PauseExit:    health.PauseExit,
		})
	}
	return status, projects
}

func (s *Server) workflowSources() []healthWorkflowSource {
	if s.registry == nil {
		return nil
	}

	projects := s.registry.List()
	sources := make([]healthWorkflowSource, 0, len(projects))
	for _, trackedProject := range projects {
		status := trackedProject.WorkflowSourceStatus()
		sources = append(sources, healthWorkflowSource{
			Policy:           trackedProject.Workflow().Config.Policy,
			ProjectID:        string(trackedProject.ID()),
			Path:             status.Path,
			SourceHash:       status.Hash,
			Revision:         status.Revision,
			Layout:           string(status.Layout),
			ModifiedAt:       status.ModifiedAt,
			LoadedAt:         status.LoadedAt,
			LastWatchEventAt: status.LastWatchEventAt,
			LastReconcileAt:  status.LastReconcileAt,
			WatcherArmed:     status.WatcherArmed,
			LastReloadError:  status.LastReloadError,
			ReloadFailedAt:   status.ReloadFailedAt,
		})
	}
	return sources
}

func (s *Server) enforcedBudgets() []healthBudget {
	if s.registry == nil {
		return nil
	}

	projects := s.registry.List()
	budgets := make([]healthBudget, 0, len(projects))
	for _, project := range projects {
		cfg, ok := project.EnforcedBudget()
		if !ok {
			continue
		}
		budgets = append(budgets, healthBudget{
			ProjectID:      string(project.ID()),
			Enabled:        cfg.Enabled,
			PerDayMaxUSD:   cfg.PerDayMaxUSD,
			PerIssueMaxUSD: cfg.PerIssueMaxUSD,
		})
	}
	return budgets
}

func (s *Server) redirectToOnboarding(c echo.Context) error {
	return c.Redirect(http.StatusFound, "/onboarding")
}

func (s *Server) redirectToDashboard(c echo.Context) error {
	if scenario, ok, err := s.demoScenarioOrError(c); err != nil {
		return err
	} else if ok && scenario.Page == "onboarding" {
		return s.demoOnboarding(c, scenario)
	}
	return c.Redirect(http.StatusFound, "/")
}

func render(c echo.Context, component templ.Component) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	if c.Response().Header().Get(echo.HeaderCacheControl) == "" {
		c.Response().Header().Set(echo.HeaderCacheControl, revalidateCacheControl)
	}
	return component.Render(c.Request().Context(), c.Response().Writer)
}

func (cfg Config) logger() *slog.Logger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	return slog.Default()
}

func (cfg Config) now() func() time.Time {
	if cfg.Now != nil {
		return cfg.Now
	}
	return time.Now
}

func (cfg Config) mode() Mode {
	if cfg.Mode == ModeOnboarding {
		return ModeOnboarding
	}
	return ModeRunning
}

func (cfg Config) staticDir() string {
	return cfg.StaticDir
}

func (cfg Config) sseTickInterval() time.Duration {
	if cfg.SSETickInterval > 0 {
		return cfg.SSETickInterval
	}
	return time.Second
}

func (cfg Config) sseFragmentInterval() time.Duration {
	if cfg.SSEFragmentInterval < 0 {
		return 0
	}
	if cfg.SSEFragmentInterval > 0 {
		return cfg.SSEFragmentInterval
	}
	return defaultSSEFragmentInterval
}

func (cfg Config) sseHealthInterval() time.Duration {
	if cfg.SSEHealthInterval < 0 {
		return 0
	}
	if cfg.SSEHealthInterval > 0 {
		return cfg.SSEHealthInterval
	}
	return defaultSSEHealthInterval
}

func (cfg Config) sseMetricsInterval() time.Duration {
	if cfg.SSEMetricsInterval < 0 {
		return 0
	}
	if cfg.SSEMetricsInterval > 0 {
		return cfg.SSEMetricsInterval
	}
	return defaultSSEMetricsInterval
}

func (cfg Config) httpReadHeaderTimeout() time.Duration {
	if cfg.HTTPReadHeaderTimeout > 0 {
		return cfg.HTTPReadHeaderTimeout
	}
	return defaultHTTPReadHeaderTimeout
}

func (cfg Config) httpIdleTimeout() time.Duration {
	if cfg.HTTPIdleTimeout > 0 {
		return cfg.HTTPIdleTimeout
	}
	return defaultHTTPIdleTimeout
}

func (cfg Config) workflowPath() string {
	if cfg.WorkflowPath != "" {
		return cfg.WorkflowPath
	}
	return "WORKFLOW.md"
}

func (cfg Config) dashboardURL() string {
	if dashboardURL := strings.TrimSpace(cfg.DashboardURL); dashboardURL != "" {
		return dashboardURL
	}
	return "http://localhost:4000"
}

func (cfg Config) lookupEnv() func(string) string {
	if cfg.LookupEnv != nil {
		return cfg.LookupEnv
	}
	return defaultLookupEnv
}

func (cfg Config) kanban() workflowconfig.Kanban {
	kanban := cfg.Kanban
	kanban.Normalize()
	return kanban
}

func (cfg Config) kanbanWorkflow(kanban workflowconfig.Kanban) workflowconfig.Config {
	workflow := cfg.KanbanWorkflow
	if workflow.Tracker.Kind == "" &&
		len(workflow.Tracker.ActiveStates) == 0 &&
		len(workflow.Tracker.ObservedStates) == 0 &&
		len(workflow.Tracker.TerminalStates) == 0 {
		workflow = workflowconfig.Default()
	}
	workflow.Server.Kanban = kanban
	return workflow
}

func (cfg Config) githubWebhookSecret(workflow workflowconfig.Config) string {
	if secret := strings.TrimSpace(cfg.GitHubWebhookSecret); secret != "" {
		return secret
	}
	return strings.TrimSpace(workflow.Tracker.GitHubWebhookSecret)
}

func (cfg Config) pricing() budget.PricingTable {
	if cfg.Pricing != nil {
		return cfg.Pricing
	}
	return budget.DefaultPricingTable()
}

func configuredStatus(value any) string {
	if value == nil {
		return "missing"
	}
	return "configured"
}

func (s *Server) connectorName() string {
	if s.connector == nil {
		return ""
	}
	return s.connector.Name()
}

type healthResponse struct {
	Status                 string                           `json:"status"`
	Ready                  bool                             `json:"ready"`
	Lifecycle              StartupLifecycleState            `json:"lifecycle"`
	Version                string                           `json:"version"`
	Commit                 string                           `json:"commit"`
	ProjectStatus          string                           `json:"project_status"`
	Mode                   string                           `json:"mode"`
	Connector              string                           `json:"connector"`
	SessionsRemaining      int                              `json:"sessions_remaining,omitempty"`
	Update                 telemetry.Update                 `json:"update,omitzero"`
	Checks                 map[string]string                `json:"checks"`
	Environment            healthEnvironment                `json:"environment"`
	Budgets                []healthBudget                   `json:"budgets,omitempty"`
	Workflows              []healthWorkflowSource           `json:"workflows,omitempty"`
	TrackerUnavailable     []telemetry.TrackerCondition     `json:"tracker_unavailable,omitempty"`
	ForgeUnavailable       []telemetry.ForgeCondition       `json:"forge_unavailable,omitempty"`
	CIUnavailable          []telemetry.CICondition          `json:"ci_unavailable,omitempty"`
	BackendOutages         []telemetry.BackendOutage        `json:"backend_outages,omitempty"`
	FailureBreakers        []telemetry.FailureBreaker       `json:"failure_breakers,omitempty"`
	DispatchLoops          []telemetry.DispatchLoop         `json:"dispatch_loops,omitempty"`
	StalenessWarnings      []telemetry.StalenessWarning     `json:"staleness_warnings,omitempty"`
	StrandedIssues         []telemetry.StrandedIssue        `json:"stranded_active_issues,omitempty"`
	CleanupFaults          []telemetry.CleanupFault         `json:"workspace_cleanup_failures,omitempty"`
	Dispatch               telemetry.DispatchStatus         `json:"dispatch"`
	DispatchStalls         []telemetry.DispatchStatus       `json:"dispatch_stalls,omitempty"`
	NotificationFailures   []healthnotify.Failure           `json:"health_notification_failures,omitempty"`
	RefreshFailures        []telemetry.RefreshFailure       `json:"refresh_failures,omitempty"`
	Projects               []healthProject                  `json:"projects,omitempty"`
	Refresh                telemetry.Refresh                `json:"refresh"`
	MemoryPressure         telemetry.MemoryPressure         `json:"memory_pressure"`
	IOPressure             telemetry.IOPressure             `json:"io_pressure"`
	CPUPressure            telemetry.CPUPressure            `json:"cpu_pressure"`
	AgentMemory            []healthAgentMemory              `json:"agent_memory"`
	SnapshotGeneratedAt    time.Time                        `json:"snapshot_generated_at,omitzero"`
	SnapshotAgeSeconds     int64                            `json:"snapshot_age_seconds"`
	StateEndpoints         healthStateEndpoints             `json:"state_endpoints"`
	TickLiveness           []telemetry.TickLiveness         `json:"tick_liveness"`
	OrphanedAgentProcesses telemetry.OrphanedAgentProcesses `json:"orphaned_agent_processes"`
}

type healthAgentMemory struct {
	ProjectID       string    `json:"project_id,omitempty"`
	IssueIdentifier string    `json:"issue_identifier,omitempty"`
	RSSBytes        uint64    `json:"rss_bytes"`
	RSSCeilingBytes uint64    `json:"rss_ceiling_bytes"`
	ObservedAt      time.Time `json:"observed_at,omitzero"`
}

type healthEnvironment struct {
	Path string `json:"path"`
}

type healthProject struct {
	ProjectID    string                `json:"project_id"`
	Status       string                `json:"status"`
	LastError    string                `json:"last_error,omitempty"`
	LastErrorAt  time.Time             `json:"last_error_at,omitzero"`
	NextRetryAt  time.Time             `json:"next_retry_at,omitzero"`
	RetryStopped bool                  `json:"retry_stopped"`
	ActiveHours  telemetry.ActiveHours `json:"active_hours,omitzero"`
	PauseExit    *pause.ExitStatus     `json:"pause_exit,omitempty"`
}

type healthWorkflowSource struct {
	Policy           policy.Descriptor `json:"policy,omitzero"`
	ProjectID        string            `json:"project_id"`
	Path             string            `json:"path"`
	SourceHash       string            `json:"source_hash"`
	Revision         string            `json:"revision,omitempty"`
	Layout           string            `json:"layout,omitempty"`
	ModifiedAt       time.Time         `json:"modified_at,omitzero"`
	LoadedAt         time.Time         `json:"loaded_at,omitzero"`
	LastWatchEventAt time.Time         `json:"last_watch_event_at,omitzero"`
	LastReconcileAt  time.Time         `json:"last_reconcile_at,omitzero"`
	WatcherArmed     bool              `json:"watcher_armed"`
	LastReloadError  string            `json:"last_reload_error,omitempty"`
	ReloadFailedAt   time.Time         `json:"reload_failed_at,omitzero"`
}

type healthBudget struct {
	ProjectID      string  `json:"project_id"`
	Enabled        bool    `json:"enabled"`
	PerDayMaxUSD   float64 `json:"per_day_max_usd"`
	PerIssueMaxUSD float64 `json:"per_issue_max_usd"`
}
