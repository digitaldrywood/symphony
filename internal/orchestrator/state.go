package orchestrator

import (
	"context"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/provenance"
	releasepkg "github.com/digitaldrywood/detent/internal/release"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/staleness"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

type State struct {
	PollInterval             time.Duration
	RefreshFailureThreshold  int
	MaxConcurrentAgents      int
	BillingMode              string
	RateWindowPacing         workflowconfig.RateWindowPacing
	MaxAgentsByState         map[string]int
	PoolName                 string
	PoolCapacity             int
	PoolAvailable            int
	PoolDraining             bool
	StrandedActiveThreshold  time.Duration
	DispatchStallThreshold   time.Duration
	MemoryPressure           telemetry.MemoryPressure
	IOPressure               telemetry.IOPressure
	CPUPressure              telemetry.CPUPressure
	AutoPromoteQuietDuration time.Duration
	AutoPromote              AutoPromoteConfig
	ActiveStates             []string
	TerminalStates           []string
	StopRunTargetState       string
	PrioritizeUnblockers     bool
	Instance                 telemetry.Instance
	Authorization            selector.Selector
	SelectorContext          selector.Context
	Draining                 bool
	DrainStartedAt           time.Time
	DataSeq                  uint64
	LastRefreshAt            time.Time
	LastRefreshDuration      time.Duration
	RefreshProgress          telemetry.RefreshProgress
	NextRefreshAt            time.Time
	LastRefreshError         string
	LastRefreshErrorAt       time.Time
	RefreshSources           map[telemetry.RefreshSourceName]telemetry.RefreshSource
	ManualRefresh            telemetry.RefreshAttempt
	LastRunningReconcileAt   time.Time
	LastWorkspaceCleanupAt   time.Time
	CleanupFailures          map[string]string
	CleanupFailureAt         time.Time
	RecentEvents             []telemetry.ActivityEvent
	Auth                     connector.AuthHealth
	StatusDrift              connector.StatusDrift
	BoardIssues              []connector.Issue
	Pipeline                 []connector.Issue
	AutoPromoteDecisions     map[string]AutoPromoteDecision
	RequiredGates            map[string]telemetry.RequiredGate
	Running                  map[string]Running
	WorkAttempts             []telemetry.WorkAttempt
	SchedulerDecisions       []telemetry.SchedulerDecision
	DispatchEscalations      map[string]time.Time
	DispatchStatus           store.ProjectDispatchStatus
	Release                  releasepkg.Status
	Claimed                  map[string]Claimed
	Blocked                  map[string]Blocked
	Completed                map[string]Completed
	Retry                    map[string]Retry
	MergeTimings             map[string]MergeTiming
	mergeReservations        map[string]mergeReservation
	mergeRecoveryChecked     map[string]bool
	mergeSlotAcquisitions    []mergeSlotAcquisition
	mergeSlotWarnings        map[string]time.Time
	nativeMergeQueueEntries  map[string]nativeMergeQueueEntry
	nativeMergeQueueRepos    map[string]nativeMergeQueueRepository
	nativeMergeQueueDeferred map[string]struct{}
	nativeQueueRetries       map[string]connector.Issue
	nativeQueueSweepAt       time.Time
	TransientCheckRetries    map[string]TransientCheckRetry
	DependencyAutoUnblocks   map[string]DependencyAutoUnblockRecord
	BudgetRefusals           map[string]BudgetRefusal
	PriorAttempts            map[string]runpkg.PriorAttempt
	InstantFailures          map[string]InstantFailure
	RepeatedFailures         map[string]RepeatedFailure
	FailureBreaker           ProjectFailureBreaker
	DispatchRecoveries       map[string]DispatchRecovery
	StalenessWarnings        map[string]StalenessWarning
	stalenessReminders       map[string]time.Time
	TrackerUnavailable       *TrackerCondition
	trackerEvidence          map[string]trackerAvailabilityEvidence
	deferredCompletions      map[string]deferredCompletion
	ForgeUnavailable         map[string]ForgeCondition
	GitHubMonitors           map[string]GitHubMonitor
	CIUnavailable            *CICondition
	BackendOutages           map[string]BackendOutage
	BackendRecoveries        map[string]BackendRecovery
	DiffStats                map[string]DiffStats
	ReapedWorkspaces         map[string]time.Time
	TokenTotals              TokenTotals
	RateLimits               *telemetry.RateLimits
	graphQLUsageSamples      []graphQLUsageSample
	laneEntries              map[string]time.Time
	laneProvenance           map[string]provenance.Attribution
	planRework               map[string]struct{}
	epicTransitionWatch      []connector.Issue
	pendingEpicParentLookups map[string]connector.Issue
	tickTransitions          *issueStateSnapshotTransitions
}

type StalenessWarning struct {
	Warning               staleness.Warning
	Visible               bool
	DetectedAt            time.Time
	LastObservedAt        time.Time
	DeliveredAt           time.Time
	DeliveryAttempts      int
	LastDeliveryAttemptAt time.Time
	DeliveryError         string
}

type Running struct {
	progress                    *workerProgress
	Policy                      policy.Descriptor
	Issue                       connector.Issue
	Attempt                     int
	WorkAttemptID               int64
	Generation                  uint64
	Mode                        string
	DispatchSourceState         string
	DispatchTargetState         string
	DispatchWorkpadHash         string
	DispatchWorkpadRead         bool
	DispatchProgress            implementProgressArtifactSnapshot
	DispatchArtifactStatus      string
	ArtifactStatusField         string
	DeliverableKind             string
	DispatchLoopStart           dispatchLoopStartRecord
	StartedAt                   time.Time
	WorkerHost                  string
	ProcessIdentity             string
	WorkerProcess               procgroup.Identity
	WorkspacePath               string
	SessionID                   string
	DetentSessionID             int64
	RuntimeIdentity             agentidentity.Identity
	WorkerGitHubActor           connector.IssueActor
	TurnCount                   int
	LastEventAt                 time.Time
	LastEvent                   string
	LastCommand                 string
	LastMessage                 string
	LastMessageTruncation       *runtimeoutput.Truncation
	RecentEvents                []telemetry.ActivityEvent
	DiffStats                   DiffStats
	ArtifactEvidence            runpkg.ArtifactProgressEvidence
	WorkProductPushed           bool
	Tokens                      TokenTotals
	RSSBytes                    uint64
	RSSCeilingBytes             uint64
	RSSObservedAt               time.Time
	CapacityScope               backendcapacity.Scope
	CapacityProbe               bool
	ForgeProbeHost              string
	GitHubCredential            string
	ModelPermitExempt           bool
	CIStopRequested             bool
	CompletionOwnershipReleased bool
	CompletionLane              string
	CompletionWorkpadURL        string
	CompletionAcceptedAt        time.Time
	laneMutation                store.LaneMutationReceipt
	StopDestination             string
	StopPriorityOptions         []telemetry.StopRunPriorityOption
	globalSlot                  scheduler.Slot
	cancel                      context.CancelFunc
	stop                        context.CancelCauseFunc
	done                        <-chan struct{}
}

type Claimed struct {
	Issue          connector.Issue
	ClaimedAt      time.Time
	Owner          string
	LeaseRenewedAt time.Time
	LeaseExpiresAt time.Time
}

type BlockedSource = telemetry.BlockedSource

const (
	BlockedSourceDependency    = telemetry.BlockedSourceDependency
	BlockedSourceMergeDuration = telemetry.BlockedSourceMergeDuration
	BlockedSourceProjectStatus = telemetry.BlockedSourceProjectStatus
	BlockedSourceOperatorStop  = telemetry.BlockedSourceOperatorStop
	BlockedSourceOwnership     = telemetry.BlockedSourceOwnership
)

type Blocked struct {
	Issue                   connector.Issue
	Reason                  string
	AttemptError            string
	RecoveryAction          string
	RecoveryReason          string
	RecoveryTarget          string
	RecoveryRemedy          string
	RecoveryReachability    string
	RecoveryIntentResumable bool
	NeedsHumanAttention     bool
	BlockerEvidence         []telemetry.BlockerEvidence
	RecoveryRoot            *telemetry.BlockedRecoveryRoot
	BlockedAt               time.Time
	Source                  BlockedSource
	Attempt                 int
	WorkAttemptID           int64
	DetentSessionID         int64
	SessionID               string
	Destination             string
	Priority                int
	PriorityName            string
	StopReason              string
	Recovery                *workflowLaneBlockedRecoveryMetadata
}

type Completed struct {
	Issue            connector.Issue
	SessionID        string
	StartedAt        time.Time
	CompletedAt      time.Time
	FinalState       string
	CompletionKind   string
	GateWaitReason   string
	gateWaitEvidence connector.Issue
	Tokens           TokenTotals
	MergeTiming      MergeTiming
	RuntimeIdentity  agentidentity.Identity
}

type MergeTiming struct {
	EnteredMergingAt           time.Time
	MergeWorkerSlotAcquiredAt  time.Time
	MergeStartedAt             time.Time
	BaseRefreshStartedAt       time.Time
	BaseRefreshFinishedAt      time.Time
	CIWaitHeadSHA              string
	CIWaitStartedAt            time.Time
	CIWaitFinishedAt           time.Time
	MergedAt                   time.Time
	MergeFailedAt              time.Time
	MergeFailureReason         string
	QueueWaitSeconds           int64
	ActiveMergeDurationSeconds int64
	TotalMergingSeconds        int64
}

type Retry struct {
	Issue              connector.Issue
	Attempt            int
	DueAt              time.Time
	Error              string
	WorkerHost         string
	CapacityScope      backendcapacity.Scope
	RetryMode          runpkg.RetryMode
	ResumeState        store.AgentResumeState
	MergePrecheck      *runpkg.MergePrecheck
	CIUnavailable      bool
	TrackerUnavailable bool
	CompletionDeferred bool
	ForgeUnavailable   bool
	ForgeHost          string
	ForgeRetry         *runpkg.ForgeRetry
	GitHubMonitor      bool
	GitHubCredential   string
	Wait               RetryWait
}

type RetryWait struct {
	Kind                  string
	StartedAt             time.Time
	PollCount             int
	PendingChecks         []string
	WorkspaceBranch       string
	WorkspaceHolderPath   string
	WorkspacePRNumber     int
	WorkspaceCreateCount  int
	WorkspaceDestroyCount int
}

type InstantFailure struct {
	Issue          connector.Issue
	Error          string
	errorKey       string
	Count          int
	FirstFailureAt time.Time
	LastFailureAt  time.Time
}

// RepeatedFailure tracks consecutive worker failures of any duration for one
// issue. Unlike InstantFailure it does not require matching error text: token
// counts and other attempt-specific details vary between otherwise identical
// failures, and each retry of a long-running failure spends real money.
type RepeatedFailure struct {
	Issue                    connector.Issue
	Error                    string
	Count                    int
	GitHubRESTBudgetFailures int
	FirstFailureAt           time.Time
	LastFailureAt            time.Time
}

type ProjectFailureBreaker struct {
	Config         FailureBreakerConfig
	Failures       map[string][]ProjectFailure
	Class          string
	Count          int
	FirstFailureAt time.Time
	TrippedAt      time.Time
	ResumeAt       time.Time
	CanaryIssueID  string
}

type ProjectFailure struct {
	IssueID      string
	Identifier   string
	IssueURL     string
	Title        string
	ErrorMessage string
	Cause        string
	BackendID    string
	BackendKind  string
	Provider     string
	At           time.Time
}

type TransientCheckRetry struct {
	IssueID       string
	HeadSHA       string
	CheckName     string
	CheckID       int64
	WorkflowRunID int64
	Attempts      int
	RetriedAt     time.Time
}

type DependencyAutoUnblockRecord struct {
	BlockerSet  string
	UnblockedAt time.Time
}

func newState(cfg Config) State {
	return State{
		PollInterval:            cfg.PollInterval,
		RefreshFailureThreshold: cfg.RefreshFailureThreshold,
		MaxConcurrentAgents:     cfg.MaxConcurrentAgents,
		BillingMode:             cfg.BillingMode,
		RateWindowPacing:        cfg.RateWindowPacing,
		MaxAgentsByState:        cloneStateLimits(cfg.MaxConcurrentAgentsByState),
		StrandedActiveThreshold: cfg.StrandedActiveThreshold,
		DispatchStallThreshold:  cfg.DispatchStallThreshold,
		MemoryPressure: telemetry.MemoryPressure{
			SomeAvg60Max: cfg.MemoryPressureSomeAvg60Max,
		},
		IOPressure: telemetry.IOPressure{
			FullAvg10Max:                cfg.IOPressureFullAvg10Max,
			DegradedMaxConcurrentAgents: cfg.IOPressureDegradedMaxAgents,
		},
		CPUPressure: telemetry.CPUPressure{
			SomeAvg10Max:                cfg.CPUPressureSomeAvg10Max,
			DegradedMaxConcurrentAgents: cfg.CPUPressureDegradedMaxAgents,
		},
		AutoPromoteQuietDuration: cfg.AutoPromote.QuietDuration,
		AutoPromote:              cloneAutoPromoteConfig(cfg.AutoPromote),
		ActiveStates:             append([]string(nil), cfg.ActiveStates...),
		TerminalStates:           append([]string(nil), cfg.TerminalStates...),
		StopRunTargetState:       cfg.StopRunTargetState,
		PrioritizeUnblockers:     cfg.PrioritizeUnblockers,
		Instance:                 instanceSnapshot(cfg),
		Authorization:            cloneSelector(cfg.Authorization),
		SelectorContext:          cfg.SelectorContext,
		AutoPromoteDecisions:     map[string]AutoPromoteDecision{},
		RefreshSources:           map[telemetry.RefreshSourceName]telemetry.RefreshSource{},
		Running:                  map[string]Running{},
		Claimed:                  map[string]Claimed{},
		Blocked:                  map[string]Blocked{},
		DispatchEscalations:      map[string]time.Time{},
		Completed:                map[string]Completed{},
		Retry:                    map[string]Retry{},
		MergeTimings:             map[string]MergeTiming{},
		mergeSlotWarnings:        map[string]time.Time{},
		nativeMergeQueueEntries:  map[string]nativeMergeQueueEntry{},
		nativeMergeQueueRepos:    map[string]nativeMergeQueueRepository{},
		nativeMergeQueueDeferred: map[string]struct{}{},
		nativeQueueRetries:       map[string]connector.Issue{},
		TransientCheckRetries:    map[string]TransientCheckRetry{},
		DependencyAutoUnblocks:   map[string]DependencyAutoUnblockRecord{},
		BudgetRefusals:           map[string]BudgetRefusal{},
		PriorAttempts:            map[string]runpkg.PriorAttempt{},
		InstantFailures:          map[string]InstantFailure{},
		RepeatedFailures:         map[string]RepeatedFailure{},
		FailureBreaker:           newProjectFailureBreaker(cfg.FailureBreaker),
		DispatchRecoveries:       map[string]DispatchRecovery{},
		StalenessWarnings:        map[string]StalenessWarning{},
		stalenessReminders:       map[string]time.Time{},
		trackerEvidence:          map[string]trackerAvailabilityEvidence{},
		deferredCompletions:      map[string]deferredCompletion{},
		ForgeUnavailable:         map[string]ForgeCondition{},
		GitHubMonitors:           map[string]GitHubMonitor{},
		BackendOutages:           map[string]BackendOutage{},
		BackendRecoveries:        map[string]BackendRecovery{},
		DiffStats:                map[string]DiffStats{},
		ReapedWorkspaces:         map[string]time.Time{},
		CleanupFailures:          map[string]string{},
		laneEntries:              map[string]time.Time{},
		laneProvenance:           map[string]provenance.Attribution{},
		planRework:               map[string]struct{}{},
		pendingEpicParentLookups: map[string]connector.Issue{},
	}
}

func (s State) clone() State {
	cloned := State{
		PollInterval:             s.PollInterval,
		RefreshFailureThreshold:  s.RefreshFailureThreshold,
		MaxConcurrentAgents:      s.MaxConcurrentAgents,
		BillingMode:              s.BillingMode,
		RateWindowPacing:         s.RateWindowPacing,
		MaxAgentsByState:         cloneStateLimits(s.MaxAgentsByState),
		PoolName:                 s.PoolName,
		PoolCapacity:             s.PoolCapacity,
		PoolAvailable:            s.PoolAvailable,
		PoolDraining:             s.PoolDraining,
		StrandedActiveThreshold:  s.StrandedActiveThreshold,
		DispatchStallThreshold:   s.DispatchStallThreshold,
		MemoryPressure:           s.MemoryPressure,
		IOPressure:               s.IOPressure,
		CPUPressure:              s.CPUPressure,
		AutoPromoteQuietDuration: s.AutoPromoteQuietDuration,
		AutoPromote:              cloneAutoPromoteConfig(s.AutoPromote),
		ActiveStates:             append([]string(nil), s.ActiveStates...),
		TerminalStates:           append([]string(nil), s.TerminalStates...),
		StopRunTargetState:       s.StopRunTargetState,
		PrioritizeUnblockers:     s.PrioritizeUnblockers,
		Instance:                 s.Instance,
		Authorization:            cloneSelector(s.Authorization),
		SelectorContext:          s.SelectorContext,
		Draining:                 s.Draining,
		DrainStartedAt:           s.DrainStartedAt,
		DataSeq:                  s.DataSeq,
		LastRefreshAt:            s.LastRefreshAt,
		LastRefreshDuration:      s.LastRefreshDuration,
		RefreshProgress:          s.RefreshProgress,
		NextRefreshAt:            s.NextRefreshAt,
		LastRefreshError:         s.LastRefreshError,
		LastRefreshErrorAt:       s.LastRefreshErrorAt,
		RefreshSources:           cloneRefreshSources(s.RefreshSources),
		ManualRefresh:            cloneRefreshAttempt(s.ManualRefresh),
		LastRunningReconcileAt:   s.LastRunningReconcileAt,
		LastWorkspaceCleanupAt:   s.LastWorkspaceCleanupAt,
		CleanupFailures:          maps.Clone(s.CleanupFailures),
		CleanupFailureAt:         s.CleanupFailureAt,
		RecentEvents:             cloneActivityEvents(s.RecentEvents),
		Auth:                     s.Auth,
		StatusDrift:              cloneStatusDrift(s.StatusDrift),
		BoardIssues:              cloneIssues(s.BoardIssues),
		Pipeline:                 cloneIssues(s.Pipeline),
		AutoPromoteDecisions:     cloneAutoPromoteDecisions(s.AutoPromoteDecisions),
		RequiredGates:            maps.Clone(s.RequiredGates),
		WorkAttempts:             cloneTelemetryWorkAttempts(s.WorkAttempts),
		SchedulerDecisions:       cloneTelemetrySchedulerDecisions(s.SchedulerDecisions),
		DispatchEscalations:      maps.Clone(s.DispatchEscalations),
		DispatchStatus:           cloneProjectDispatchStatus(s.DispatchStatus),
		Release:                  s.Release,
		Running:                  make(map[string]Running, len(s.Running)),
		Claimed:                  make(map[string]Claimed, len(s.Claimed)),
		Blocked:                  make(map[string]Blocked, len(s.Blocked)),
		Completed:                make(map[string]Completed, len(s.Completed)),
		Retry:                    make(map[string]Retry, len(s.Retry)),
		MergeTimings:             maps.Clone(s.MergeTimings),
		mergeReservations:        maps.Clone(s.mergeReservations),
		mergeRecoveryChecked:     maps.Clone(s.mergeRecoveryChecked),
		mergeSlotAcquisitions:    append([]mergeSlotAcquisition(nil), s.mergeSlotAcquisitions...),
		mergeSlotWarnings:        maps.Clone(s.mergeSlotWarnings),
		nativeMergeQueueEntries:  cloneNativeMergeQueueEntries(s.nativeMergeQueueEntries),
		nativeMergeQueueRepos:    maps.Clone(s.nativeMergeQueueRepos),
		nativeMergeQueueDeferred: maps.Clone(s.nativeMergeQueueDeferred),
		nativeQueueRetries:       cloneIssueMap(s.nativeQueueRetries),
		nativeQueueSweepAt:       s.nativeQueueSweepAt,
		TransientCheckRetries:    maps.Clone(s.TransientCheckRetries),
		DependencyAutoUnblocks:   maps.Clone(s.DependencyAutoUnblocks),
		BudgetRefusals:           make(map[string]BudgetRefusal, len(s.BudgetRefusals)),
		PriorAttempts:            clonePriorAttempts(s.PriorAttempts),
		InstantFailures:          make(map[string]InstantFailure, len(s.InstantFailures)),
		RepeatedFailures:         make(map[string]RepeatedFailure, len(s.RepeatedFailures)),
		FailureBreaker:           cloneProjectFailureBreaker(s.FailureBreaker),
		DispatchRecoveries:       cloneDispatchRecoveries(s.DispatchRecoveries),
		StalenessWarnings:        maps.Clone(s.StalenessWarnings),
		stalenessReminders:       maps.Clone(s.stalenessReminders),
		TrackerUnavailable:       cloneTrackerCondition(s.TrackerUnavailable),
		trackerEvidence:          maps.Clone(s.trackerEvidence),
		deferredCompletions:      cloneDeferredCompletions(s.deferredCompletions),
		ForgeUnavailable:         maps.Clone(s.ForgeUnavailable),
		GitHubMonitors:           maps.Clone(s.GitHubMonitors),
		CIUnavailable:            cloneCICondition(s.CIUnavailable),
		BackendOutages:           maps.Clone(s.BackendOutages),
		BackendRecoveries:        cloneBackendRecoveries(s.BackendRecoveries),
		DiffStats:                make(map[string]DiffStats, len(s.DiffStats)),
		ReapedWorkspaces:         make(map[string]time.Time, len(s.ReapedWorkspaces)),
		TokenTotals:              s.TokenTotals,
		RateLimits:               cloneRateLimits(s.RateLimits),
		graphQLUsageSamples:      append([]graphQLUsageSample(nil), s.graphQLUsageSamples...),
		laneEntries:              maps.Clone(s.laneEntries),
		laneProvenance:           maps.Clone(s.laneProvenance),
		planRework:               make(map[string]struct{}, len(s.planRework)),
		epicTransitionWatch:      cloneIssues(s.epicTransitionWatch),
		pendingEpicParentLookups: cloneIssueMap(s.pendingEpicParentLookups),
	}

	for id, running := range s.Running {
		running = running.withProgress()
		running.Issue = cloneIssue(running.Issue)
		running.LastMessageTruncation = runtimeoutput.CloneTruncation(running.LastMessageTruncation)
		running.RecentEvents = cloneActivityEvents(running.RecentEvents)
		running.StopPriorityOptions = append([]telemetry.StopRunPriorityOption(nil), running.StopPriorityOptions...)
		running.globalSlot = scheduler.Slot{}
		running.cancel = nil
		running.stop = nil
		running.done = nil
		cloned.Running[id] = running
	}
	for id, claimed := range s.Claimed {
		claimed.Issue = cloneIssue(claimed.Issue)
		cloned.Claimed[id] = claimed
	}
	for id, blocked := range s.Blocked {
		blocked.Issue = cloneIssue(blocked.Issue)
		blocked.BlockerEvidence = cloneBlockerEvidence(blocked.BlockerEvidence)
		cloned.Blocked[id] = blocked
	}
	for id, completed := range s.Completed {
		completed.Issue = cloneIssue(completed.Issue)
		completed.gateWaitEvidence = cloneIssue(completed.gateWaitEvidence)
		cloned.Completed[id] = completed
	}
	for id, retry := range s.Retry {
		retry.Issue = cloneIssue(retry.Issue)
		retry.MergePrecheck = cloneMergePrecheck(retry.MergePrecheck)
		retry.ForgeRetry = cloneForgeRetry(retry.ForgeRetry)
		retry.Wait.PendingChecks = append([]string(nil), retry.Wait.PendingChecks...)
		cloned.Retry[id] = retry
	}
	for id, failure := range s.InstantFailures {
		failure.Issue = cloneIssue(failure.Issue)
		cloned.InstantFailures[id] = failure
	}
	for id, failure := range s.RepeatedFailures {
		failure.Issue = cloneIssue(failure.Issue)
		cloned.RepeatedFailures[id] = failure
	}
	for id, refusal := range s.BudgetRefusals {
		refusal.Issue = cloneIssue(refusal.Issue)
		if refusal.MaxUSD != nil {
			maxUSD := *refusal.MaxUSD
			refusal.MaxUSD = &maxUSD
		}
		if refusal.ResetAt != nil {
			resetAt := *refusal.ResetAt
			refusal.ResetAt = &resetAt
		}
		cloned.BudgetRefusals[id] = refusal
	}
	maps.Copy(cloned.DiffStats, s.DiffStats)
	maps.Copy(cloned.ReapedWorkspaces, s.ReapedWorkspaces)
	maps.Copy(cloned.planRework, s.planRework)

	return cloned
}

func cloneRunning(source map[string]Running) map[string]Running {
	state := State{Running: source}
	return state.clone().Running
}

func cloneClaimed(source map[string]Claimed) map[string]Claimed {
	state := State{Claimed: source}
	return state.clone().Claimed
}

func cloneBlockerEvidence(evidence []telemetry.BlockerEvidence) []telemetry.BlockerEvidence {
	cloned := append([]telemetry.BlockerEvidence(nil), evidence...)
	for index := range cloned {
		if evidence[index].RecordedAt != nil {
			recordedAt := *evidence[index].RecordedAt
			cloned[index].RecordedAt = &recordedAt
		}
		if evidence[index].ExpiresAt != nil {
			expiresAt := *evidence[index].ExpiresAt
			cloned[index].ExpiresAt = &expiresAt
		}
	}
	return cloned
}

func cloneMergePrecheck(precheck *runpkg.MergePrecheck) *runpkg.MergePrecheck {
	if precheck == nil {
		return nil
	}
	cloned := *precheck
	cloned.ConflictPaths = append([]string(nil), precheck.ConflictPaths...)
	return &cloned
}

func cloneForgeRetry(retry *runpkg.ForgeRetry) *runpkg.ForgeRetry {
	if retry == nil {
		return nil
	}
	cloned := *retry
	return &cloned
}

func cloneAutoPromoteConfig(cfg AutoPromoteConfig) AutoPromoteConfig {
	cfg.AllowedIssueLabels = append([]string(nil), cfg.AllowedIssueLabels...)
	cfg.TerminalStates = append([]string(nil), cfg.TerminalStates...)
	cfg.Gate = cloneGateConfig(cfg.Gate)
	return cfg
}

func cloneGateConfig(cfg gate.Config) gate.Config {
	cfg.RequiredStatusChecks = append([]string(nil), cfg.RequiredStatusChecks...)
	cfg.RequireAutomatedReview = cloneBoolPointer(cfg.RequireAutomatedReview)
	cfg.TransientCIRetryLimit = cloneIntPointer(cfg.TransientCIRetryLimit)
	cfg.Validator.BlockOn = append([]string(nil), cfg.Validator.BlockOn...)
	cfg.Validator.MaxInlineDiffBytes = cloneIntPointer(cfg.Validator.MaxInlineDiffBytes)
	cfg.Artifact.PassStatuses = append([]string(nil), cfg.Artifact.PassStatuses...)
	cfg.Artifact.WaitStatuses = append([]string(nil), cfg.Artifact.WaitStatuses...)
	cfg.Artifact.ReworkStatuses = append([]string(nil), cfg.Artifact.ReworkStatuses...)
	return cfg
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneSelector(in selector.Selector) selector.Selector {
	out := in
	out.AssigneeIn = cloneStringSlice(in.AssigneeIn)
	out.AuthorIn = cloneStringSlice(in.AuthorIn)
	out.PriorityIn = append([]int(nil), in.PriorityIn...)
	out.Labels.Include = cloneStringSlice(in.Labels.Include)
	out.Labels.Exclude = cloneStringSlice(in.Labels.Exclude)
	out.Fields = append([]selector.FieldEquals(nil), in.Fields...)
	out.And = cloneSelectors(in.And)
	out.Or = cloneSelectors(in.Or)
	return out
}

func cloneSelectors(in []selector.Selector) []selector.Selector {
	if len(in) == 0 {
		return nil
	}
	out := make([]selector.Selector, 0, len(in))
	for _, item := range in {
		out = append(out, cloneSelector(item))
	}
	return out
}

func clonePriorAttempts(in map[string]runpkg.PriorAttempt) map[string]runpkg.PriorAttempt {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]runpkg.PriorAttempt, len(in))
	for key, value := range in {
		value.Validator.Findings = append([]gate.Finding(nil), value.Validator.Findings...)
		out[key] = value
	}
	return out
}

func cloneStatusDrift(drift connector.StatusDrift) connector.StatusDrift {
	return connector.StatusDrift{
		UntrackedOpen: cloneIssues(drift.UntrackedOpen),
		OpenTerminal:  cloneIssues(drift.OpenTerminal),
		ClosedActive:  cloneIssues(drift.ClosedActive),
	}
}

func cloneIssue(issue connector.Issue) connector.Issue {
	cloned := issue
	if issue.Priority != nil {
		priority := *issue.Priority
		cloned.Priority = &priority
	}
	if issue.PRNumber != nil {
		prNumber := *issue.PRNumber
		cloned.PRNumber = &prNumber
	}
	if issue.PullRequest != nil {
		pullRequest := *issue.PullRequest
		if issue.PullRequest.MergeQueueEntry != nil {
			entry := clonePullRequestMergeQueueEntry(*issue.PullRequest.MergeQueueEntry)
			pullRequest.MergeQueueEntry = &entry
		}
		if issue.PullRequest.ActivityAt != nil {
			activityAt := *issue.PullRequest.ActivityAt
			pullRequest.ActivityAt = &activityAt
		}
		if issue.PullRequest.CodexReviewSubmittedAt != nil {
			submittedAt := *issue.PullRequest.CodexReviewSubmittedAt
			pullRequest.CodexReviewSubmittedAt = &submittedAt
		}
		if issue.PullRequest.LatestCodexReviewSubmittedAt != nil {
			submittedAt := *issue.PullRequest.LatestCodexReviewSubmittedAt
			pullRequest.LatestCodexReviewSubmittedAt = &submittedAt
		}
		if issue.PullRequest.HydrationNextRetryAt != nil {
			nextRetryAt := *issue.PullRequest.HydrationNextRetryAt
			pullRequest.HydrationNextRetryAt = &nextRetryAt
		}
		pullRequest.Checks = append([]connector.PullRequestCheck(nil), issue.PullRequest.Checks...)
		pullRequest.SlowChecks = append([]connector.PullRequestCheck(nil), issue.PullRequest.SlowChecks...)
		pullRequest.RunningChecks = append([]string(nil), issue.PullRequest.RunningChecks...)
		pullRequest.UnstartedChecks = append([]connector.PullRequestCheck(nil), issue.PullRequest.UnstartedChecks...)
		pullRequest.StaleSuccessfulChecks = append([]connector.PullRequestCheck(nil), issue.PullRequest.StaleSuccessfulChecks...)
		pullRequest.RequiredCheckFailures = append([]connector.PullRequestCheck(nil), issue.PullRequest.RequiredCheckFailures...)
		pullRequest.TransientFailedChecks = append([]connector.PullRequestCheck(nil), issue.PullRequest.TransientFailedChecks...)
		pullRequest.UnresolvedReviewThreads = append([]connector.PullRequestReviewThread(nil), issue.PullRequest.UnresolvedReviewThreads...)
		pullRequest.CodexReviewFindings = append([]connector.PullRequestFinding(nil), issue.PullRequest.CodexReviewFindings...)
		pullRequest.Labels = cloneStringSlice(issue.PullRequest.Labels)
		cloned.PullRequest = &pullRequest
	}
	if issue.Deliverable != nil {
		deliverable := *issue.Deliverable
		deliverable.Metadata = cloneStringMap(issue.Deliverable.Metadata)
		cloned.Deliverable = &deliverable
	}
	if issue.CreatedAt != nil {
		createdAt := *issue.CreatedAt
		cloned.CreatedAt = &createdAt
	}
	if issue.UpdatedAt != nil {
		updatedAt := *issue.UpdatedAt
		cloned.UpdatedAt = &updatedAt
	}
	if issue.StageUpdatedAt != nil {
		stageUpdatedAt := *issue.StageUpdatedAt
		cloned.StageUpdatedAt = &stageUpdatedAt
	}
	cloned.BlockedBy = append([]connector.BlockedRef(nil), issue.BlockedBy...)
	cloned.ChildIssues = append([]connector.BlockedRef(nil), issue.ChildIssues...)
	cloned.WorkpadSignal = workpad.CloneSignal(issue.WorkpadSignal)
	cloned.Labels = cloneStringSlice(issue.Labels)
	cloned.Comments = cloneIssueComments(issue.Comments)
	cloned.Assignees = cloneStringSlice(issue.Assignees)
	cloned.Fields = cloneStringMap(issue.Fields)
	cloned.FieldUpdatedAt = cloneTimeMap(issue.FieldUpdatedAt)
	cloned.Metadata = cloneStringMap(issue.Metadata)
	return cloned
}

func cloneRefreshAttempt(attempt telemetry.RefreshAttempt) telemetry.RefreshAttempt {
	cloned := attempt
	cloned.RequestedAt = timePointerValue(attempt.RequestedAt)
	cloned.StartedAt = timePointerValue(attempt.StartedAt)
	cloned.CompletedAt = timePointerValue(attempt.CompletedAt)
	cloned.LastErrorAt = timePointerValue(attempt.LastErrorAt)
	cloned.RetryAt = timePointerValue(attempt.RetryAt)
	cloned.Operations = append([]string(nil), attempt.Operations...)
	return cloned
}

func timePointerValue(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func cloneIssueMap(issues map[string]connector.Issue) map[string]connector.Issue {
	if len(issues) == 0 {
		return nil
	}
	out := make(map[string]connector.Issue, len(issues))
	for key, issue := range issues {
		out[key] = cloneIssue(issue)
	}
	return out
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string{}, values...)
}

func cloneIssueComments(comments []connector.IssueComment) []connector.IssueComment {
	if comments == nil {
		return nil
	}
	out := make([]connector.IssueComment, len(comments))
	for index, comment := range comments {
		out[index] = comment
		out[index].CreatedAt = timePointerValue(comment.CreatedAt)
		out[index].UpdatedAt = timePointerValue(comment.UpdatedAt)
	}
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	maps.Copy(out, values)
	return out
}

func cloneTimeMap(values map[string]time.Time) map[string]time.Time {
	out := make(map[string]time.Time, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneActivityEvents(events []telemetry.ActivityEvent) []telemetry.ActivityEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]telemetry.ActivityEvent, len(events))
	copy(out, events)
	for index := range out {
		out[index].Truncation = runtimeoutput.CloneTruncation(out[index].Truncation)
	}
	return out
}

func cloneRateLimits(rateLimits *telemetry.RateLimits) *telemetry.RateLimits {
	if rateLimits == nil {
		return nil
	}

	cloned := *rateLimits
	cloned.Primary = cloneRateLimitBucket(rateLimits.Primary)
	cloned.Secondary = cloneRateLimitBucket(rateLimits.Secondary)
	cloned.Credits = cloneRateLimitBucket(rateLimits.Credits)
	cloned.GitHubGraphQL = cloneRateLimitBucket(rateLimits.GitHubGraphQL)
	cloned.GitHubREST = cloneRateLimitBucket(rateLimits.GitHubREST)
	cloned.GitHubRESTBudgets = cloneRESTBudgets(rateLimits.GitHubRESTBudgets)
	cloned.GraphQLCost = cloneGraphQLCost(rateLimits.GraphQLCost)
	cloned.RESTUsage = cloneRESTUsage(rateLimits.RESTUsage)
	return &cloned
}

func mergeRateLimits(current *telemetry.RateLimits, incoming *telemetry.RateLimits) *telemetry.RateLimits {
	merged := cloneRateLimits(incoming)
	if merged == nil {
		return cloneRateLimits(current)
	}
	if current != nil && current.GitHubGraphQL != nil && merged.GitHubGraphQL == nil {
		merged.GitHubGraphQL = cloneRateLimitBucket(current.GitHubGraphQL)
	}
	if current != nil && current.GraphQLCost != nil && merged.GraphQLCost == nil {
		merged.GraphQLCost = cloneGraphQLCost(current.GraphQLCost)
	}
	if current != nil && current.GitHubREST != nil && merged.GitHubREST == nil {
		merged.GitHubREST = cloneRateLimitBucket(current.GitHubREST)
	}
	if current != nil {
		merged.GitHubRESTBudgets = mergeRESTBudgets(current.GitHubRESTBudgets, merged.GitHubRESTBudgets)
	}
	if current != nil && current.RESTUsage != nil && merged.RESTUsage == nil {
		merged.RESTUsage = cloneRESTUsage(current.RESTUsage)
	}
	return merged
}

func cloneRateLimitBucket(bucket *telemetry.RateLimitBucket) *telemetry.RateLimitBucket {
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

func cloneGraphQLCost(cost *telemetry.GraphQLCost) *telemetry.GraphQLCost {
	if cost == nil {
		return nil
	}

	cloned := *cost
	if len(cost.Contributors) > 0 {
		cloned.Contributors = append([]telemetry.GraphQLCostContributor(nil), cost.Contributors...)
	}
	return &cloned
}

func cloneRESTUsage(usage *telemetry.RESTUsage) *telemetry.RESTUsage {
	if usage == nil {
		return nil
	}

	cloned := *usage
	if usage.BackoffUntil != nil {
		backoffUntil := *usage.BackoffUntil
		cloned.BackoffUntil = &backoffUntil
	}
	if len(usage.Contributors) > 0 {
		cloned.Contributors = append([]telemetry.RESTUsageContributor(nil), usage.Contributors...)
		for index := range cloned.Contributors {
			if usage.Contributors[index].ResetAt == nil {
				continue
			}
			resetAt := *usage.Contributors[index].ResetAt
			cloned.Contributors[index].ResetAt = &resetAt
		}
	}
	if len(usage.Divergences) > 0 {
		cloned.Divergences = append([]telemetry.RESTUsageDivergence(nil), usage.Divergences...)
		for index := range cloned.Divergences {
			cloned.Divergences[index].WindowStartedAt = cloneTimePointer(usage.Divergences[index].WindowStartedAt)
			cloned.Divergences[index].LastObservedAt = cloneTimePointer(usage.Divergences[index].LastObservedAt)
			cloned.Divergences[index].ResetAt = cloneTimePointer(usage.Divergences[index].ResetAt)
		}
	}
	return &cloned
}

func cloneRESTBudgets(budgets []telemetry.RESTBudget) []telemetry.RESTBudget {
	if len(budgets) == 0 {
		return nil
	}
	cloned := append([]telemetry.RESTBudget(nil), budgets...)
	for index := range cloned {
		cloned[index] = cloneRESTBudget(budgets[index])
	}
	return cloned
}

func cloneRESTBudget(budget telemetry.RESTBudget) telemetry.RESTBudget {
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

func mergeRESTBudgets(current []telemetry.RESTBudget, incoming []telemetry.RESTBudget) []telemetry.RESTBudget {
	if len(current) == 0 {
		return cloneRESTBudgets(incoming)
	}
	if len(incoming) == 0 {
		return cloneRESTBudgets(current)
	}
	merged := make(map[string]telemetry.RESTBudget, len(current)+len(incoming))
	for _, budget := range append(append([]telemetry.RESTBudget(nil), current...), incoming...) {
		consumer := strings.TrimSpace(budget.Consumer)
		if consumer == "" {
			consumer = telemetry.RESTConsumerOrchestrator
		}
		key := consumer + "\x00" + strings.TrimSpace(budget.CredentialIdentity) + "\x00" + strings.TrimSpace(budget.EndpointFamily) + "\x00" + strings.TrimSpace(budget.Resource)
		merged[key] = cloneRESTBudget(budget)
	}
	out := make([]telemetry.RESTBudget, 0, len(merged))
	for _, budget := range merged {
		out = append(out, budget)
	}
	sort.Slice(out, func(i, j int) bool {
		return restBudgetStateKey(out[i]) < restBudgetStateKey(out[j])
	})
	return out
}

func restBudgetStateKey(budget telemetry.RESTBudget) string {
	consumer := strings.TrimSpace(budget.Consumer)
	if consumer == "" {
		consumer = telemetry.RESTConsumerOrchestrator
	}
	return consumer + "\x00" + strings.TrimSpace(budget.CredentialIdentity) + "\x00" + strings.TrimSpace(budget.EndpointFamily) + "\x00" + strings.TrimSpace(budget.Resource)
}

func cloneTelemetryWorkAttempts(values []telemetry.WorkAttempt) []telemetry.WorkAttempt {
	if len(values) == 0 {
		return nil
	}
	return append([]telemetry.WorkAttempt(nil), values...)
}

func cloneTelemetrySchedulerDecisions(values []telemetry.SchedulerDecision) []telemetry.SchedulerDecision {
	if len(values) == 0 {
		return nil
	}
	return append([]telemetry.SchedulerDecision(nil), values...)
}

func cloneProjectDispatchStatus(status store.ProjectDispatchStatus) store.ProjectDispatchStatus {
	if status.AllSkippedSince != nil {
		value := *status.AllSkippedSince
		status.AllSkippedSince = &value
	}
	if status.LastSelectedAt != nil {
		value := *status.LastSelectedAt
		status.LastSelectedAt = &value
	}
	return status
}

func addTokenTotals(left, right TokenTotals) TokenTotals {
	return TokenTotals{
		InputTokens:           left.InputTokens + right.InputTokens,
		CachedInputTokens:     left.CachedInputTokens + right.CachedInputTokens,
		OutputTokens:          left.OutputTokens + right.OutputTokens,
		ReasoningOutputTokens: left.ReasoningOutputTokens + right.ReasoningOutputTokens,
		TotalTokens:           left.TotalTokens + right.TotalTokens,
		ModelContextWindow:    maxModelContextWindow(left.ModelContextWindow, right.ModelContextWindow),
		RuntimeSeconds:        left.RuntimeSeconds + right.RuntimeSeconds,
	}
}

func maxModelContextWindow(left *int64, right *int64) *int64 {
	switch {
	case left == nil && right == nil:
		return nil
	case left == nil:
		value := *right
		return &value
	case right == nil:
		value := *left
		return &value
	case *right > *left:
		value := *right
		return &value
	default:
		value := *left
		return &value
	}
}

func diffStatsPresent(diffStats DiffStats) bool {
	return diffStats.FilesChanged != 0 ||
		diffStats.AddedLines != 0 ||
		diffStats.RemovedLines != 0 ||
		diffStats.UnpushedCommits != 0 ||
		len(diffStats.UnpushedCommitRefs) != 0 ||
		len(diffStats.TrackedPaths) != 0 ||
		len(diffStats.UntrackedPaths) != 0 ||
		len(diffStats.CommitsNotInPullRequest) != 0 ||
		diffStats.PullRequestComparisonAvailable ||
		diffStats.RecoveryStateExpected ||
		diffStats.RecoveryStateAvailable ||
		diffStats.CommitsAhead != 0 ||
		diffStats.RemoteBranchExists ||
		diffStats.DeliveryStateChecked ||
		diffStats.HeadSHA != "" ||
		diffStats.Fingerprint != "" ||
		diffStats.Status != ""
}
