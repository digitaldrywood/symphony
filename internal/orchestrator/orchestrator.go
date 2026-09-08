package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/digitaldrywood/detent/internal/activity"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/hostpressure"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/providercapacity"
	releasepkg "github.com/digitaldrywood/detent/internal/release"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/staleness"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	defaultPollInterval                        = 30 * time.Second
	defaultRunningReconcileInterval            = 2 * time.Minute
	defaultWorkspaceCleanupIdleTTL             = 24 * time.Hour
	defaultWorkspaceCleanupSweep               = 10 * time.Minute
	gitHubGraphQLPauseRemaining                = 100
	gitHubGraphQLBackoffRemaining              = 500
	defaultGitHubGraphQLWarnRemaining          = 500
	defaultGitHubGraphQLMinReserve             = 1000
	defaultGitHubRESTMinReserve                = 1000
	defaultMaxConcurrentAgents                 = 1
	defaultMaxRetryBackoff                     = 5 * time.Minute
	defaultOverloadRetryDelay                  = 45 * time.Second
	defaultContinuationRetry                   = time.Second
	defaultFailureRetryBaseDelay               = 10 * time.Second
	defaultFailureBreakerSameClassLimit        = 5
	defaultFailureBreakerWindow                = time.Hour
	defaultFailureBreakerCooldown              = time.Hour
	maxMergeWorkerRunnerFailures               = 3
	mergeWorkerCurrentHeadCIWaitTimeout        = time.Hour
	instantFailureThreshold                    = 5
	instantFailureMaxDuration                  = 10 * time.Second
	instantFailureBlockedReasonPrefix          = "instant fail circuit breaker: "
	repeatedFailureThreshold                   = 5
	repeatedFailureBlockedReasonPrefix         = "repeated failure circuit breaker: "
	tokenCeilingBlockedReasonPrefix            = "token ceiling circuit breaker: "
	deliverableConfigurationFailureCause       = "deliverable_configuration_failure"
	budgetProjectionCeilingFailureCause        = "budget_projection_ceiling"
	continuationDispatchBackoff                = 100 * time.Millisecond
	runUpdateBufferSize                        = 128
	maxRecentEvents                            = 50
	blockedStatusState                         = "Blocked"
	blockedReasonDependency                    = "blocked by non-terminal dependency"
	mergeWorkerTerminalStateMissing            = "merge worker completed without reaching a terminal issue or pull request state"
	mergeWorkerRetryExhaustedReason            = "merge_worker_retry_exhausted"
	mergeWorkerCurrentHeadCIWaitExceededReason = scheduler.DecisionReasonMergeWorkerCurrentHeadCIExceeded
	mergeWorkerDurationExceededReason          = "merge_worker_duration_exceeded"
	mergeFallbackBudgetExceededReason          = "merge_fallback_budget_exceeded"
	mergeFallbackRequiresReworkReason          = "merge_fallback_requires_rework"
)

var (
	ErrMissingConnector       = errors.New("orchestrator connector is required")
	ErrSchedulingClaimLost    = errors.New("scheduling claim lost")
	ErrSchedulingUnavailable  = errors.New("scheduling source unavailable")
	ErrStopped                = errors.New("orchestrator stopped")
	ErrCapacityClearQueueFull = errors.New("capacity clear already pending")
)

type Config struct {
	Policy                        policy.Descriptor
	PollInterval                  time.Duration
	RefreshFailureThreshold       int
	MaxConcurrentAgents           int
	MaxConcurrentAgentsByState    map[string]int
	DispatchPriorityByState       []string
	DispatchPriorityByLabel       []string
	PrioritizeUnblockers          bool
	MergeFastPathEnabled          bool
	MergeFairnessAge              time.Duration
	MergeMethod                   string
	DeliverableKind               string
	MergeWorkerStartupTimeout     time.Duration
	MergeWorkerMaxDuration        time.Duration
	ResumeOrphanedSessions        bool
	StopRunTargetState            string
	StopRunPriorityNames          map[int]string
	MaxConcurrentAgentsPerHost    int
	MaxRetryBackoff               time.Duration
	Recovery                      workflowconfig.Recovery
	OverloadRetryDelay            time.Duration
	NoProgressTokenLimit          int64
	NoProgressSpendLimitUSD       float64
	LifetimeSessionLimit          int64
	LifetimeTokenLimit            int64
	LifetimeLimitCooldown         time.Duration
	LifetimeLimitOverrideLabel    string
	BillingMode                   string
	RateWindowPacing              workflowconfig.RateWindowPacing
	FailureBreaker                FailureBreakerConfig
	Project                       scheduler.ProjectCandidate
	SchedulingRepository          string
	Claiming                      ClaimingConfig
	AutoPromote                   AutoPromoteConfig
	Plan                          gate.PlanConfig
	DependencySource              string
	StatusLabelPrefix             string
	DependencyAutoUnblock         DependencyAutoUnblockConfig
	BlockedRecovery               BlockedRecoveryConfig
	BlockerAutoPromote            BlockerAutoPromoteConfig
	AdmissionTargetState          string
	ActiveStates                  []string
	ObservedStates                []string
	TerminalStates                []string
	Authorization                 selector.Selector
	SelectorContext               selector.Context
	WorkerHosts                   []string
	BudgetRefusalCooldown         time.Duration
	WorkspaceCleanupIdleTTL       time.Duration
	WorkspaceCleanupSweepInterval time.Duration
	ContinuationRetryDelay        time.Duration
	FailureRetryBaseDelay         time.Duration
	SelectorPersona               string
	GitHubGraphQLWarnRemaining    int64
	GitHubGraphQLMinReserve       int64
	GitHubRESTMinReserve          int64
	ForgeHost                     string
	ServiceIdentity               string
	OutputTruncationMaxBytes      int
	EfficiencyThresholds          efficiency.Thresholds
	Lessons                       LessonCaptureConfig
	Staleness                     staleness.Config
	StalenessDelivery             staleness.DeliveryConfig
	StrandedActiveThreshold       time.Duration
	DispatchStallThreshold        time.Duration
	MemoryPressureSomeAvg60Max    float64
	MemoryPressurePollInterval    time.Duration
	IOPressureFullAvg10Max        float64
	IOPressureDegradedMaxAgents   int
	IOPressurePollInterval        time.Duration
	CPUPressureSomeAvg10Max       float64
	CPUPressureDegradedMaxAgents  int
	CPUPressurePollInterval       time.Duration
}

type LessonCaptureConfig struct {
	Enabled    bool
	Path       string
	MaxEntries int
}

type FailureBreakerConfig struct {
	SameClassLimit int
	Window         time.Duration
	Cooldown       time.Duration
}

type ClaimingConfig struct {
	Enabled           bool
	OwnershipSet      bool
	OwnershipMode     string
	AssigneeRequired  bool
	Owner             string
	AssigneeLogin     string
	OwnerField        string
	LeaseField        string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
}

type SchedulingRequest struct {
	ProviderRequirement func(context.Context, connector.Issue) (providercapacity.Requirement, error)
	Policy              policy.Descriptor
	ProjectID           string
	Repository          string
	WorkflowStates      []string
	Filter              connector.IssueFilterHint
}

type SchedulingSource interface {
	HeartbeatInterval() time.Duration
	FetchCandidateIssues(context.Context, SchedulingRequest) ([]connector.Issue, error)
	AdoptClaim(context.Context, connector.Issue, time.Time) (Claimed, error)
	RenewClaim(context.Context, string, time.Time) (Claimed, error)
	ReleaseClaim(context.Context, string, string) error
}

type Dependencies struct {
	Connector            connector.Connector
	Scheduling           SchedulingSource
	Runner               Runner
	WorkspaceReaper      WorkspaceReaper
	WorkflowMetrics      WorkflowMetricsRecorder
	LaneMutations        store.LaneMutationStore
	Efficiency           efficiency.Recorder
	LifecycleExporter    efficiency.LifecycleExporter
	WorkAttempts         store.WorkAttemptStore
	MergeRequiredChecks  store.MergeRequiredCheckStore
	ProgressSpend        store.ProgressSpendStore
	LifetimeUsage        LifetimeUsageStore
	AgentResume          store.AgentResumeStore
	OrphanSessions       store.OrphanSessionStore
	ValidatorMemo        store.ValidatorMemoStore
	SecurityAudits       store.SecurityAuditStore
	StalenessWarnings    store.StalenessWarningStore
	Activity             *activity.Broker
	Release              releasepkg.Coordinator
	GlobalDispatchGate   scheduler.ProjectDispatchGate
	DispatchPacer        runpkg.DispatchPacer
	ReadMemoryPressure   func(context.Context) (hostpressure.Sample, error)
	ReadIOPressure       func(context.Context) (hostpressure.Sample, error)
	ReadCPUPressure      func(context.Context) (hostpressure.Sample, error)
	Now                  func() time.Time
	Logger               *slog.Logger
	Retrospector         Retrospector
	WorkerProcesses      WorkerProcessStore
	ReapWorkerProcess    WorkerProcessReapFunc
	WorkerReapGrace      time.Duration
	NewStalenessNotifier func(staleness.DeliveryConfig) (staleness.Notifier, error)
}

type WorkerProcessStore interface {
	ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error)
	MarkSessionWorkerProcessReaped(context.Context, int64, store.WorkerProcessReap) error
}

type WorkerProcessReapFunc func(context.Context, procgroup.Identity, time.Duration) (procgroup.TerminationOutcome, error)

type Retrospector interface {
	Trigger(string)
}

type runDurationLimitFactory func(context.Context, time.Duration, error) (context.Context, context.CancelFunc)

type mergeWorkerStartupTimer interface {
	Stop() bool
}

type mergeWorkerStartupTimerFactory func(time.Duration, func()) mergeWorkerStartupTimer

type WorkspaceReapResult = runpkg.WorkspaceReapResult

type WorkspaceReaper = runpkg.WorkspaceReaper

type WorkspaceReconciler = runpkg.WorkspaceReconciler

type WorkspaceReconcileResult = runpkg.WorkspaceReconcileResult

type RuntimeUpdate struct {
	Config         Config
	Connector      connector.Connector
	Release        releasepkg.Coordinator
	ReplaceRelease bool
}

type Orchestrator struct {
	cfg                     Config
	connector               connector.Connector
	scheduling              SchedulingSource
	workflowMetrics         WorkflowMetricsRecorder
	laneMutations           store.LaneMutationStore
	efficiency              efficiency.Recorder
	lifecycleExporter       efficiency.LifecycleExporter
	workAttempts            store.WorkAttemptStore
	mergeRequiredChecks     store.MergeRequiredCheckStore
	operatorStops           store.OperatorStopStore
	progressSpend           store.ProgressSpendStore
	lifetimeUsage           LifetimeUsageStore
	agentResume             store.AgentResumeStore
	orphanSessions          store.OrphanSessionStore
	supervisor              *runpkg.Supervisor
	validator               Validator
	securityAuditor         SecurityAuditor
	reaper                  WorkspaceReaper
	logger                  *slog.Logger
	globalDispatchGate      scheduler.ProjectDispatchGate
	readMemoryPressure      func(context.Context) (hostpressure.Sample, error)
	readIOPressure          func(context.Context) (hostpressure.Sample, error)
	readCPUPressure         func(context.Context) (hostpressure.Sample, error)
	validatorMu             sync.Mutex
	validatorWG             sync.WaitGroup
	validatorRuns           map[string]struct{}
	validatorResults        map[string]validatorStageResult
	validatorFailures       map[string]validatorStageFailure
	validatorMemo           store.ValidatorMemoStore
	securityAuditStore      store.SecurityAuditStore
	securityAuditMu         sync.Mutex
	securityAuditWG         sync.WaitGroup
	securityAuditRuns       map[string]struct{}
	stalenessWarningStore   store.StalenessWarningStore
	activity                *activity.Broker
	release                 releasepkg.Coordinator
	capacityController      runpkg.CapacityController
	providerCapacity        runpkg.ProviderCapacityResolver
	capacityStatus          runpkg.CapacityStatusController
	validatorCapacity       runpkg.ValidatorCapacityController
	recoveryInspector       runpkg.BlockedRecoveryInspector
	workspaceHoldInspector  runpkg.WorkspaceBranchHoldInspector
	dailyBudgetStatus       runpkg.DailyBudgetStatusProvider
	issueBudgetStatus       runpkg.IssueBudgetStatusProvider
	githubRESTBudgetProber  runpkg.GitHubRESTBudgetProber
	githubRESTBudgetProbes  map[string]time.Time
	now                     func() time.Time
	retrospector            Retrospector
	workerProcesses         WorkerProcessStore
	reapWorkerProcess       WorkerProcessReapFunc
	workerReapGrace         time.Duration
	newStalenessNotifier    func(staleness.DeliveryConfig) (staleness.Notifier, error)
	mergeWorkerLimit        runDurationLimitFactory
	mergeWorkerStartupTimer mergeWorkerStartupTimerFactory
	deliverableRecoveryWait func(context.Context, time.Duration) bool
	heartbeats              *heartbeatManager
	hydrationSkipStreaks    map[string]int
	hydrationWarned         bool
	ownershipStartupLogged  bool
	dispatchStartMu         sync.Mutex
	capacityClearMu         sync.Mutex
	dispatchStarts          int
	dispatchStartsDone      chan struct{}
	dispatchClosed          atomic.Bool
	projectID               string
	dispatchGateSampleMu    sync.Mutex
	dispatchGateSamples     map[dispatchGateSampleKey]time.Time
	ciTriggerLabelMu        sync.Mutex
	ciTriggerLabelHeads     map[string]ciTriggerLabelHead
	stateRequests           chan stateRequest
	refreshSignalMu         sync.Mutex
	refreshStarted          chan struct{}
	initialStateReady       chan struct{}
	initialStatePublished   sync.Once
	drainRequests           chan drainRequest
	forceRequests           chan forceRequest
	recoveryRequests        chan workAttemptRecoveryRequest
	operatorMoves           chan operatorMoveRequest
	configUpdates           chan configUpdateRequest
	refreshes               chan manualRefreshRequest
	reconciles              chan targetedRefreshRequest
	capacityClearRequests   chan capacityClearRequest
	credentialChanges       chan backendCredentialChangeRequest
	trackerClearRequests    chan trackerClearRequest
	forgeClearRequests      chan forgeClearRequest
	failureCanaryRequests   chan failureBreakerCanaryRequest
	stopRequests            chan stopRunRequest
	modelPermitRequests     chan modelPermitRequest
	runResults              chan runpkg.Completion
	runUpdates              chan runUpdate
	validatorCapacityEvents chan validatorCapacityEvent
	done                    chan struct{}
	pendingStops            map[string]*pendingStopRun
	pendingLaneRevocations  map[string]*pendingLaneRevocation
	pendingMergeRevocations map[string]mergeRevocation
	mergeRevocationComments map[string]*mergeRevocationCommentState
	completedStops          map[string]StopRunResult
	refreshSeq              atomic.Uint64
	workerGeneration        atomic.Uint64
	latestState             atomic.Pointer[State]
	latestRuntimeState      atomic.Pointer[runtimeState]
	refreshInProgress       atomic.Bool
	refreshProgress         atomic.Pointer[telemetry.RefreshProgress]
	tickWatchdog            *tickWatchdog
}

type runtimeState struct {
	WorkAttempts []telemetry.WorkAttempt
	Running      map[string]Running
	Claimed      map[string]Claimed
}

type validatorStageResult struct {
	Result    gate.ValidatorResult
	Commented bool
}

type validatorStageFailure struct {
	Attempt     int
	NextRetryAt time.Time
	Error       string
}

type stateRequest struct {
	reply chan State
}

type modelPermitRequest struct {
	issueID string
	reply   chan error
}

type drainRequest struct {
	at    time.Time
	reply chan struct{}
}

type forceRequest struct {
	ctx   context.Context //nolint:containedctx // ForceQuit carries caller cancellation through the event loop.
	at    time.Time
	reply chan error
}

type configUpdateRequest struct {
	update RuntimeUpdate
	reply  chan struct{}
}

type runUpdate struct {
	progress *workerProgress
	issueID  string
	usage    runpkg.UsageUpdate
	applied  chan struct{}
}

type capacityClearRequest struct {
	scope string
	at    time.Time
	reply chan capacityClearReply
}

type capacityClearReply struct {
	cleared []BackendOutage
}

type trackerClearRequest struct {
	at    time.Time
	reply chan trackerClearReply
}

type trackerClearReply struct {
	cleared []TrackerCondition
}

type forgeClearRequest struct {
	host  string
	at    time.Time
	reply chan forgeClearReply
}

type forgeClearReply struct {
	cleared []ForgeCondition
}

type failureBreakerCanaryRequest struct {
	at    time.Time
	reply chan FailureBreakerCanaryResult
}

func New(cfg Config, deps Dependencies) (*Orchestrator, error) {
	cfg = normalizeConfig(cfg)
	if deps.Connector == nil {
		return nil, ErrMissingConnector
	}

	runner := deps.Runner
	if runner == nil {
		runner = FakeRunner{}
	}
	reaper := deps.WorkspaceReaper
	if reaper == nil {
		if candidate, ok := runner.(WorkspaceReaper); ok {
			reaper = candidate
		}
	}
	var validator Validator
	if candidate, ok := runner.(Validator); ok {
		validator = candidate
	}
	var securityAuditor SecurityAuditor
	if candidate, ok := runner.(SecurityAuditor); ok {
		securityAuditor = candidate
	}
	var capacityController runpkg.CapacityController
	var providerCapacity runpkg.ProviderCapacityResolver
	if candidate, ok := runner.(runpkg.ProviderCapacityResolver); ok {
		providerCapacity = candidate
	}
	if candidate, ok := runner.(runpkg.CapacityController); ok {
		capacityController = candidate
	}
	var validatorCapacity runpkg.ValidatorCapacityController
	if candidate, ok := runner.(runpkg.ValidatorCapacityController); ok {
		validatorCapacity = candidate
	}
	var blockedRecoveryInspector runpkg.BlockedRecoveryInspector
	if candidate, ok := runner.(runpkg.BlockedRecoveryInspector); ok {
		blockedRecoveryInspector = candidate
	}
	var workspaceHoldInspector runpkg.WorkspaceBranchHoldInspector
	if candidate, ok := runner.(runpkg.WorkspaceBranchHoldInspector); ok {
		workspaceHoldInspector = candidate
	}
	var capacityStatus runpkg.CapacityStatusController
	if candidate, ok := runner.(runpkg.CapacityStatusController); ok {
		capacityStatus = candidate
	}
	var dailyBudgetStatus runpkg.DailyBudgetStatusProvider
	if candidate, ok := runner.(runpkg.DailyBudgetStatusProvider); ok {
		dailyBudgetStatus = candidate
	}
	var issueBudgetStatus runpkg.IssueBudgetStatusProvider
	if candidate, ok := runner.(runpkg.IssueBudgetStatusProvider); ok {
		issueBudgetStatus = candidate
	}
	var githubRESTBudgetProber runpkg.GitHubRESTBudgetProber
	if candidate, ok := runner.(runpkg.GitHubRESTBudgetProber); ok {
		githubRESTBudgetProber = candidate
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	validatorMemo := deps.ValidatorMemo
	if validatorMemo == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.ValidatorMemoStore); ok {
			validatorMemo = candidate
		}
	}
	securityAuditStore := deps.SecurityAudits
	if securityAuditStore == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.SecurityAuditStore); ok {
			securityAuditStore = candidate
		}
	}
	agentResume := deps.AgentResume
	if agentResume == nil {
		if candidate, ok := deps.WorkAttempts.(store.AgentResumeStore); ok {
			agentResume = candidate
		}
	}
	orphanSessions := deps.OrphanSessions
	if orphanSessions == nil {
		if candidate, ok := deps.WorkAttempts.(store.OrphanSessionStore); ok {
			orphanSessions = candidate
		}
	}
	if orphanSessions == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.OrphanSessionStore); ok {
			orphanSessions = candidate
		}
	}
	if agentResume == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.AgentResumeStore); ok {
			agentResume = candidate
		}
	}
	laneMutations := deps.LaneMutations
	if laneMutations == nil {
		if candidate, ok := deps.WorkAttempts.(store.LaneMutationStore); ok {
			laneMutations = candidate
		}
	}
	if laneMutations == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.LaneMutationStore); ok {
			laneMutations = candidate
		}
	}
	progressSpend := deps.ProgressSpend
	if progressSpend == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.ProgressSpendStore); ok {
			progressSpend = candidate
		}
	}
	lifetimeUsage := deps.LifetimeUsage
	if lifetimeUsage == nil {
		if candidate, ok := deps.WorkflowMetrics.(LifetimeUsageStore); ok {
			lifetimeUsage = candidate
		}
	}
	if lifetimeUsage == nil {
		if candidate, ok := deps.WorkAttempts.(LifetimeUsageStore); ok {
			lifetimeUsage = candidate
		}
	}
	mergeRequiredChecks := deps.MergeRequiredChecks
	if mergeRequiredChecks == nil {
		if candidate, ok := deps.WorkAttempts.(store.MergeRequiredCheckStore); ok {
			mergeRequiredChecks = candidate
		}
	}
	if mergeRequiredChecks == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.MergeRequiredCheckStore); ok {
			mergeRequiredChecks = candidate
		}
	}
	var operatorStops store.OperatorStopStore
	if candidate, ok := deps.WorkAttempts.(store.OperatorStopStore); ok {
		operatorStops = candidate
	}
	if operatorStops == nil {
		if candidate, ok := deps.WorkflowMetrics.(store.OperatorStopStore); ok {
			operatorStops = candidate
		}
	}
	workerProcesses := deps.WorkerProcesses
	if workerProcesses == nil {
		if candidate, ok := deps.WorkAttempts.(WorkerProcessStore); ok {
			workerProcesses = candidate
		}
	}
	if workerProcesses == nil {
		if candidate, ok := deps.WorkflowMetrics.(WorkerProcessStore); ok {
			workerProcesses = candidate
		}
	}
	reapWorkerProcess := deps.ReapWorkerProcess
	if reapWorkerProcess == nil {
		reapWorkerProcess = procgroup.Terminate
	}
	workerReapGrace := deps.WorkerReapGrace
	if workerReapGrace <= 0 {
		workerReapGrace = procgroup.DefaultTerminationGrace
	}
	newStalenessNotifier := deps.NewStalenessNotifier
	if newStalenessNotifier == nil {
		newStalenessNotifier = staleness.NewNotifier
	}
	readMemoryPressure := deps.ReadMemoryPressure
	if readMemoryPressure == nil {
		readMemoryPressure = hostpressure.ReadMemory
	}
	readIOPressure := deps.ReadIOPressure
	if readIOPressure == nil {
		readIOPressure = hostpressure.ReadIO
	}
	readCPUPressure := deps.ReadCPUPressure
	if readCPUPressure == nil {
		readCPUPressure = hostpressure.ReadCPU
	}

	supervisor, err := runpkg.NewSupervisor(runner, runpkg.SupervisorConfig{
		MaxRetryBackoff:       cfg.MaxRetryBackoff,
		FailureRetryBaseDelay: cfg.FailureRetryBaseDelay,
		OverloadRetryDelay:    cfg.OverloadRetryDelay,
		Now:                   now,
		Logger:                logger,
		DispatchPacer:         deps.DispatchPacer,
	})
	if err != nil {
		return nil, err
	}

	orchestrator := &Orchestrator{
		cfg:                     cfg,
		connector:               deps.Connector,
		scheduling:              deps.Scheduling,
		workflowMetrics:         deps.WorkflowMetrics,
		laneMutations:           laneMutations,
		efficiency:              deps.Efficiency,
		lifecycleExporter:       deps.LifecycleExporter,
		workAttempts:            deps.WorkAttempts,
		mergeRequiredChecks:     mergeRequiredChecks,
		operatorStops:           operatorStops,
		progressSpend:           progressSpend,
		lifetimeUsage:           lifetimeUsage,
		agentResume:             agentResume,
		orphanSessions:          orphanSessions,
		supervisor:              supervisor,
		validator:               validator,
		securityAuditor:         securityAuditor,
		reaper:                  reaper,
		logger:                  logger,
		globalDispatchGate:      deps.GlobalDispatchGate,
		readMemoryPressure:      readMemoryPressure,
		readIOPressure:          readIOPressure,
		readCPUPressure:         readCPUPressure,
		validatorRuns:           map[string]struct{}{},
		validatorResults:        map[string]validatorStageResult{},
		validatorFailures:       map[string]validatorStageFailure{},
		validatorMemo:           validatorMemo,
		securityAuditStore:      securityAuditStore,
		securityAuditRuns:       map[string]struct{}{},
		stalenessWarningStore:   deps.StalenessWarnings,
		activity:                deps.Activity,
		release:                 deps.Release,
		retrospector:            deps.Retrospector,
		workerProcesses:         workerProcesses,
		reapWorkerProcess:       reapWorkerProcess,
		workerReapGrace:         workerReapGrace,
		newStalenessNotifier:    newStalenessNotifier,
		projectID:               cfg.Project.ID,
		capacityController:      capacityController,
		providerCapacity:        providerCapacity,
		capacityStatus:          capacityStatus,
		validatorCapacity:       validatorCapacity,
		recoveryInspector:       blockedRecoveryInspector,
		workspaceHoldInspector:  workspaceHoldInspector,
		dailyBudgetStatus:       dailyBudgetStatus,
		issueBudgetStatus:       issueBudgetStatus,
		githubRESTBudgetProber:  githubRESTBudgetProber,
		githubRESTBudgetProbes:  map[string]time.Time{},
		now:                     now,
		dispatchGateSamples:     map[dispatchGateSampleKey]time.Time{},
		ciTriggerLabelHeads:     map[string]ciTriggerLabelHead{},
		stateRequests:           make(chan stateRequest),
		refreshStarted:          make(chan struct{}),
		initialStateReady:       make(chan struct{}),
		drainRequests:           make(chan drainRequest),
		forceRequests:           make(chan forceRequest),
		recoveryRequests:        make(chan workAttemptRecoveryRequest),
		operatorMoves:           make(chan operatorMoveRequest),
		configUpdates:           make(chan configUpdateRequest),
		refreshes:               make(chan manualRefreshRequest, 1),
		reconciles:              make(chan targetedRefreshRequest, 128),
		capacityClearRequests:   make(chan capacityClearRequest, 1),
		credentialChanges:       make(chan backendCredentialChangeRequest),
		trackerClearRequests:    make(chan trackerClearRequest),
		forgeClearRequests:      make(chan forgeClearRequest),
		failureCanaryRequests:   make(chan failureBreakerCanaryRequest),
		stopRequests:            make(chan stopRunRequest),
		modelPermitRequests:     make(chan modelPermitRequest),
		runResults:              make(chan runpkg.Completion, max(cfg.MaxConcurrentAgents, 1)),
		runUpdates:              make(chan runUpdate, runUpdateBufferSize),
		validatorCapacityEvents: make(chan validatorCapacityEvent, max(cfg.MaxConcurrentAgents, 1)),
		done:                    make(chan struct{}),
		pendingStops:            map[string]*pendingStopRun{},
		pendingLaneRevocations:  map[string]*pendingLaneRevocation{},
		pendingMergeRevocations: map[string]mergeRevocation{},
		mergeRevocationComments: map[string]*mergeRevocationCommentState{},
		completedStops:          map[string]StopRunResult{},
		tickWatchdog:            newTickWatchdog(cfg.Project.ID, cfg.PollInterval, logger),
	}
	orchestrator.heartbeats = newHeartbeatManager(cfg, deps.Connector, deps.WorkAttempts, now, logger, deps.Scheduling)
	return orchestrator, nil
}

type ciTriggerLabelHead struct {
	HeadSHA string
	Pending bool
}

func (o *Orchestrator) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		o.capacityClearMu.Lock()
		defer o.capacityClearMu.Unlock()
		close(o.done)
	}()
	defer o.markGlobalProjectIdle()
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		o.tickWatchdog.Run(watchdogCtx)
	}()
	defer func() {
		stopWatchdog()
		<-watchdogDone
	}()
	heartbeatCtx, stopHeartbeats := context.WithCancel(ctx)
	heartbeatsDone := make(chan struct{})
	var heartbeatResults <-chan heartbeatResult
	if o.heartbeats != nil {
		heartbeatResults = o.heartbeats.results
	}
	go func() {
		defer close(heartbeatsDone)
		o.heartbeats.Run(heartbeatCtx)
	}()
	defer func() {
		stopHeartbeats()
		<-heartbeatsDone
	}()

	ticker := time.NewTicker(o.cfg.PollInterval)
	defer ticker.Stop()

	state := newState(o.cfg)
	defer o.validatorWG.Wait()
	defer o.securityAuditWG.Wait()
	defer o.releaseRunningSlots(&state)
	o.startTick(&state, time.Now())
	o.publishState(&state)
	recoveryTiming := newRefreshTiming(o.logger, o.cfg.Project.ID, false)
	recoveryTiming.message = "project startup recovery timing"
	recoveryTiming.progress = &o.refreshProgress
	recoveryTiming.next("recovery")
	o.recoverDurableWorkAttempts(ctx, &state, time.Now())
	recoveryTiming.log(ctx, ctx.Err() == nil, &state)
	o.publishState(&state)
	if err := ctx.Err(); err != nil {
		return err
	}
	initialTickAt := time.Now()
	o.startTick(&state, initialTickAt)
	o.tick(ctx, &state, initialTickAt)
	o.finishTick(&state)
	o.publishState(&state)
	resetTicker(ticker, state.PollInterval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			state.syncWorkerProgress()
			o.startTick(&state, now)
			o.tick(ctx, &state, now)
			o.finishTick(&state)
			resetTicker(ticker, state.PollInterval)
		case request := <-o.refreshes:
			state.syncWorkerProgress()
			o.startTick(&state, time.Now())
			o.tickManual(ctx, &state, request)
			o.finishTick(&state)
			resetTicker(ticker, state.PollInterval)
		case request := <-o.reconciles:
			state.syncWorkerProgress()
			o.startTick(&state, time.Now())
			o.reconcileTarget(ctx, &state, request)
			o.finishTick(&state)
			resetTicker(ticker, state.PollInterval)
		case request := <-o.capacityClearRequests:
			state.syncWorkerProgress()
			cleared := o.clearBackendCapacity(&state, request.scope, request.at)
			if request.reply != nil {
				request.reply <- capacityClearReply{cleared: cleared}
			}
		case request := <-o.credentialChanges:
			state.syncWorkerProgress()
			scheduled := o.scheduleBackendCredentialProbe(&state, request.scope, request.at)
			request.reply <- scheduled
			if scheduled {
				o.startTick(&state, request.at)
				o.tick(ctx, &state, request.at)
				o.finishTick(&state)
				resetTicker(ticker, state.PollInterval)
			}
		case request := <-o.trackerClearRequests:
			state.syncWorkerProgress()
			request.reply <- trackerClearReply{cleared: o.clearTrackerAvailability(&state, request.at)}
		case request := <-o.forgeClearRequests:
			state.syncWorkerProgress()
			request.reply <- forgeClearReply{cleared: o.clearForgeAvailability(&state, request.host, request.at)}
		case request := <-o.failureCanaryRequests:
			state.syncWorkerProgress()
			result := o.requestProjectFailureBreakerCanary(&state, request.at)
			request.reply <- result
			if result.Requested {
				o.startTick(&state, time.Now())
				o.tick(ctx, &state, request.at)
				o.finishTick(&state)
				resetTicker(ticker, state.PollInterval)
			}
		case request := <-o.stopRequests:
			state.syncWorkerProgress()
			o.handleStopRunRequest(ctx, &state, request)
		case request := <-o.modelPermitRequests:
			state.syncWorkerProgress()
			request.reply <- o.handleModelPermitRequest(&state, request.issueID)
		case result := <-o.runResults:
			state.syncWorkerProgress()
			o.handleRunResult(ctx, &state, result)
		case update := <-o.runUpdates:
			state.syncWorkerProgress()
			o.handleRunUpdate(&state, update)
			if update.applied != nil {
				close(update.applied)
			}
		case result := <-heartbeatResults:
			state.syncWorkerProgress()
			o.handleHeartbeatResult(&state, result)
		case event := <-o.validatorCapacityEvents:
			state.syncWorkerProgress()
			o.handleValidatorCapacityEvent(&state, event)
		case request := <-o.drainRequests:
			state.syncWorkerProgress()
			o.startDrain(&state, request.at)
			request.reply <- struct{}{}
		case request := <-o.forceRequests:
			state.syncWorkerProgress()
			request.reply <- o.forceQuit(request.ctx, &state, request.at)
		case request := <-o.recoveryRequests:
			state.syncWorkerProgress()
			response, err := o.handleWorkAttemptRecovery(ctx, &state, request.request, request.at)
			request.reply <- workAttemptRecoveryReply{response: response, err: err}
		case request := <-o.operatorMoves:
			state.syncWorkerProgress()
			request.reply <- o.handleOperatorMove(&state, request.request, request.at)
		case update := <-o.configUpdates:
			state.syncWorkerProgress()
			o.applyRuntimeUpdate(&state, update.update, ticker)
			o.finishTick(&state)
			update.reply <- struct{}{}
		case request := <-o.stateRequests:
			state.syncWorkerProgress()
			request.reply <- state.clone()
		}
		o.publishState(&state)
	}
}

func (o *Orchestrator) TickLiveness(now time.Time) telemetry.TickLiveness {
	if o == nil || o.tickWatchdog == nil {
		return telemetry.TickLiveness{Status: telemetry.TickLivenessStatusInitializing}
	}
	return o.tickWatchdog.Snapshot(now)
}

func (o *Orchestrator) startTick(state *State, at time.Time) {
	if o == nil {
		return
	}
	state.syncWorkerProgress()
	o.refreshInProgress.Store(true)
	o.refreshSignalMu.Lock()
	if o.refreshStarted != nil {
		close(o.refreshStarted)
	}
	o.refreshStarted = make(chan struct{})
	o.refreshSignalMu.Unlock()
	if o.tickWatchdog == nil || state == nil {
		return
	}
	nextRefreshAt := time.Time{}
	if state.PollInterval > 0 {
		nextRefreshAt = at.Add(state.PollInterval)
	}
	o.tickWatchdog.Advance(at, nextRefreshAt, state.PollInterval)
}

func (o *Orchestrator) finishTick(state *State) {
	if o == nil {
		return
	}
	o.publishState(state)
	o.refreshProgress.Store(nil)
	o.refreshInProgress.Store(false)
	if o.tickWatchdog == nil || state == nil {
		return
	}
	o.tickWatchdog.Schedule(state.NextRefreshAt, state.PollInterval)
}

func (o *Orchestrator) ClearBackendCapacity(ctx context.Context, scope string) ([]BackendOutage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request := capacityClearRequest{
		scope: strings.TrimSpace(scope),
		at:    o.clockNow(),
		reply: make(chan capacityClearReply, 1),
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrStopped
	case o.capacityClearRequests <- request:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrStopped
	case reply := <-request.reply:
		return reply.cleared, nil
	}
}

func (o *Orchestrator) RequestBackendCapacityClear(ctx context.Context, scope string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	request := capacityClearRequest{
		scope: strings.TrimSpace(scope),
		at:    o.clockNow(),
	}
	o.capacityClearMu.Lock()
	defer o.capacityClearMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case o.capacityClearRequests <- request:
		return nil
	default:
		return ErrCapacityClearQueueFull
	}
}

func (o *Orchestrator) ClearTrackerAvailability(ctx context.Context) ([]TrackerCondition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request := trackerClearRequest{
		at:    o.clockNow(),
		reply: make(chan trackerClearReply, 1),
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrStopped
	case o.trackerClearRequests <- request:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrStopped
	case reply := <-request.reply:
		return reply.cleared, nil
	}
}

func (o *Orchestrator) RequestProjectFailureBreakerCanary(ctx context.Context) (FailureBreakerCanaryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request := failureBreakerCanaryRequest{
		at:    o.clockNow(),
		reply: make(chan FailureBreakerCanaryResult, 1),
	}
	select {
	case <-ctx.Done():
		return FailureBreakerCanaryResult{}, ctx.Err()
	case <-o.done:
		return FailureBreakerCanaryResult{}, ErrStopped
	case o.failureCanaryRequests <- request:
	}
	select {
	case <-ctx.Done():
		return FailureBreakerCanaryResult{}, ctx.Err()
	case <-o.done:
		return FailureBreakerCanaryResult{}, ErrStopped
	case result := <-request.reply:
		return result, nil
	}
}

func resetTicker(ticker *time.Ticker, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker.Reset(interval)
}

func (o *Orchestrator) UpdateConfig(ctx context.Context, cfg Config) error {
	return o.UpdateRuntime(ctx, RuntimeUpdate{Config: cfg})
}

func (o *Orchestrator) UpdateRuntime(ctx context.Context, update RuntimeUpdate) error {
	if ctx == nil {
		ctx = context.Background()
	}

	request := configUpdateRequest{
		update: update,
		reply:  make(chan struct{}, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case o.configUpdates <- request:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case <-request.reply:
		return nil
	}
}

func (o *Orchestrator) State(ctx context.Context) (State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return State{}, ctx.Err()
	case <-o.done:
		return State{}, ErrStopped
	default:
	}
	if o.latestState.Load() == nil {
		select {
		case <-ctx.Done():
			return State{}, ctx.Err()
		case <-o.done:
			return State{}, ErrStopped
		case <-o.initialStateReady:
		}
	}
	o.refreshSignalMu.Lock()
	refreshStarted := o.refreshStarted
	o.refreshSignalMu.Unlock()
	if o.refreshInProgress.Load() {
		return o.publishedState(), nil
	}
	request := stateRequest{reply: make(chan State, 1)}
	select {
	case <-ctx.Done():
		return State{}, ctx.Err()
	case <-o.done:
		return State{}, ErrStopped
	case <-refreshStarted:
		return o.publishedState(), nil
	case o.stateRequests <- request:
	}
	select {
	case <-ctx.Done():
		return State{}, ctx.Err()
	case <-o.done:
		return State{}, ErrStopped
	case <-refreshStarted:
		return o.publishedState(), nil
	case state := <-request.reply:
		return o.observableState(state), nil
	}
}

func (o *Orchestrator) publishedState() State {
	state := o.latestState.Load().clone()
	if runtime := o.latestRuntimeState.Load(); runtime != nil {
		state.Running = cloneRunning(runtime.Running)
		state.Claimed = cloneClaimed(runtime.Claimed)
		state.WorkAttempts = cloneTelemetryWorkAttempts(runtime.WorkAttempts)
	}
	return o.observableState(state)
}

func (o *Orchestrator) observableState(state State) State {
	pool := o.dispatchPoolSnapshot()
	state.PoolName = pool.Name
	state.PoolCapacity = pool.Capacity
	state.PoolAvailable = pool.Available
	state.PoolDraining = pool.Draining
	if progress := o.refreshProgress.Load(); progress != nil {
		state.RefreshProgress = *progress
	}
	for _, running := range state.Running {
		if running.progress != nil {
			if heartbeat := running.progress.persisted.Load(); heartbeat != nil {
				o.applyWorkAttemptHeartbeatSnapshot(&state, running.WorkAttemptID, *heartbeat, running.LastMessageTruncation)
			}
		}
	}
	return state
}

func (o *Orchestrator) publishState(state *State) {
	if o == nil || state == nil {
		return
	}
	cloned := state.clone()
	o.latestState.Store(&cloned)
	if o.initialStateReady != nil {
		o.initialStatePublished.Do(func() { close(o.initialStateReady) })
	}
	o.latestRuntimeState.Store(&runtimeState{
		WorkAttempts: cloned.WorkAttempts,
		Running:      cloned.Running,
		Claimed:      cloned.Claimed,
	})
}

func (o *Orchestrator) publishRuntimeState(state *State) {
	if o == nil || state == nil {
		return
	}
	o.latestRuntimeState.Store(&runtimeState{
		WorkAttempts: cloneTelemetryWorkAttempts(state.WorkAttempts),
		Running:      cloneRunning(state.Running),
		Claimed:      cloneClaimed(state.Claimed),
	})
}

func (o *Orchestrator) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	o.BeginDrain()
	if err := o.WaitForDispatchQuiesced(ctx); err != nil {
		return err
	}

	request := drainRequest{
		at:    time.Now().UTC(),
		reply: make(chan struct{}, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case o.drainRequests <- request:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case <-request.reply:
		return nil
	}
}

func (o *Orchestrator) ForceQuit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	o.BeginDrain()
	if err := o.WaitForDispatchQuiesced(ctx); err != nil {
		return err
	}

	request := forceRequest{
		ctx:   ctx,
		at:    time.Now().UTC(),
		reply: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case o.forceRequests <- request:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return ErrStopped
	case err := <-request.reply:
		return err
	}
}

func (o *Orchestrator) BeginDrain() {
	if o == nil {
		return
	}

	o.dispatchStartMu.Lock()
	if o.dispatchClosed.Swap(true) {
		o.dispatchStartMu.Unlock()
		return
	}
	o.dispatchStartMu.Unlock()
	if gate, ok := o.globalDispatchGate.(interface{ PauseDispatch() func() }); ok {
		gate.PauseDispatch()
	}
	if o.globalDispatchGate != nil {
		o.globalDispatchGate.MarkIdle(scheduler.ProjectCandidate{ID: o.projectID})
	}
}

func (o *Orchestrator) WaitForDispatchQuiesced(ctx context.Context) error {
	if o == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	o.dispatchStartMu.Lock()
	done := o.dispatchStartsDone
	o.dispatchStartMu.Unlock()
	if done == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (o *Orchestrator) beginDispatchStart() bool {
	o.dispatchStartMu.Lock()
	defer o.dispatchStartMu.Unlock()
	if o.dispatchQuiesced() {
		return false
	}
	if o.dispatchStarts == 0 {
		o.dispatchStartsDone = make(chan struct{})
	}
	o.dispatchStarts++
	return true
}

func (o *Orchestrator) finishDispatchStart() {
	o.dispatchStartMu.Lock()
	defer o.dispatchStartMu.Unlock()
	o.dispatchStarts--
	if o.dispatchStarts == 0 {
		close(o.dispatchStartsDone)
		o.dispatchStartsDone = nil
	}
}

func (o *Orchestrator) dispatchQuiesced() bool {
	return o == nil || o.dispatchClosed.Load()
}

func (o *Orchestrator) applyRuntimeUpdate(state *State, update RuntimeUpdate, ticker *time.Ticker) {
	cfg := normalizeConfig(update.Config)
	previousCfg := o.cfg
	if o.cfg.Claiming.OwnershipMode != cfg.Claiming.OwnershipMode ||
		o.cfg.Claiming.AssigneeRequired != cfg.Claiming.AssigneeRequired {
		o.ownershipStartupLogged = false
	}
	o.cfg = cfg
	if nativeMergeQueueCleanupRequired(cfg) {
		state.nativeQueueSweepAt = time.Time{}
	}
	now := time.Now
	if o.now != nil {
		now = o.now
	}
	updatedAt := now()
	o.reloadProjectFailureBreaker(state, cfg.FailureBreaker, updatedAt)
	if update.Connector != nil {
		o.connector = update.Connector
	}
	o.heartbeats.configure(cfg, o.connector, o.workAttempts)
	if update.ReplaceRelease {
		o.release = update.Release
		if update.Release == nil {
			state.Release = releasepkg.Status{}
		}
	}
	o.supervisor.UpdateConfig(runpkg.SupervisorConfig{
		MaxRetryBackoff:       cfg.MaxRetryBackoff,
		FailureRetryBaseDelay: cfg.FailureRetryBaseDelay,
		OverloadRetryDelay:    cfg.OverloadRetryDelay,
	})
	state.PollInterval = cfg.PollInterval
	state.RefreshFailureThreshold = cfg.RefreshFailureThreshold
	state.MaxConcurrentAgents = cfg.MaxConcurrentAgents
	state.BillingMode = cfg.BillingMode
	state.RateWindowPacing = cfg.RateWindowPacing
	state.StrandedActiveThreshold = cfg.StrandedActiveThreshold
	state.DispatchStallThreshold = cfg.DispatchStallThreshold
	refreshCachedHostPressure(state, previousCfg, cfg, updatedAt)
	state.AutoPromoteQuietDuration = cfg.AutoPromote.QuietDuration
	state.AutoPromote = cloneAutoPromoteConfig(cfg.AutoPromote)
	state.ActiveStates = append([]string(nil), cfg.ActiveStates...)
	state.TerminalStates = append([]string(nil), cfg.TerminalStates...)
	state.StopRunTargetState = cfg.StopRunTargetState
	state.PrioritizeUnblockers = cfg.PrioritizeUnblockers
	state.Instance = instanceSnapshot(cfg)
	state.Authorization = cloneSelector(cfg.Authorization)
	state.SelectorContext = cfg.SelectorContext
	if !state.LastRefreshAt.IsZero() && cfg.PollInterval > 0 {
		state.NextRefreshAt = state.LastRefreshAt.Add(cfg.PollInterval)
	}
	ticker.Reset(cfg.PollInterval)
}

func (o *Orchestrator) startDrain(state *State, now time.Time) {
	if state == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !state.Draining {
		state.Draining = true
		state.DrainStartedAt = now
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      now,
			Event:   "shutdown_drain_started",
			Message: "shutdown drain started",
		})
	}
	o.markGlobalProjectIdle()
}

func (o *Orchestrator) forceQuit(ctx context.Context, state *State, now time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if state == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	state.Draining = true
	if state.DrainStartedAt.IsZero() {
		state.DrainStartedAt = now
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "shutdown_force_requested",
		Message: "shutdown force requested",
	})

	var err error
	for _, issueID := range sortedKeys(state.Running) {
		o.cancelRunning(state, issueID)
		o.heartbeats.remove(issueID)
		err = errors.Join(err, o.abandonClaim(ctx, issueID))
		delete(state.Running, issueID)
		delete(state.Claimed, issueID)
		delete(state.Retry, issueID)
		delete(state.BudgetRefusals, issueID)
		delete(state.PriorAttempts, issueID)
	}
	o.markGlobalProjectIdle()
	return err
}

func (o *Orchestrator) abandonClaim(ctx context.Context, issueID string) error {
	if o.scheduling != nil {
		return o.scheduling.ReleaseClaim(ctx, issueID, "orchestrator_release")
	}
	if !o.cfg.Claiming.Enabled || strings.TrimSpace(o.cfg.Claiming.LeaseField) == "" {
		return nil
	}
	if strings.TrimSpace(issueID) == "" || o.connector == nil {
		return nil
	}
	if err := o.connector.SetField(ctx, issueID, o.cfg.Claiming.LeaseField, ""); err != nil {
		if o.logger != nil {
			o.logger.Warn("abandon claim lease failed", "issue_id", issueID, "error", err)
		}
		return err
	}
	return nil
}

func (o *Orchestrator) cleanupDrainedRun(ctx context.Context, state *State, issueID string) {
	if err := o.abandonClaim(ctx, issueID); err != nil && o.logger != nil {
		o.logger.Warn("abandon completed drain claim failed", "issue_id", issueID, "error", err)
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
	delete(state.PriorAttempts, issueID)
	delete(state.InstantFailures, issueID)
	delete(state.RepeatedFailures, issueID)
}
