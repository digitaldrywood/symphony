package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	admissionmodel "github.com/digitaldrywood/detent/internal/admission/model"
	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/retro"
	routinemodel "github.com/digitaldrywood/detent/internal/routine/model"
	"github.com/digitaldrywood/detent/internal/schedulehealth"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
	"github.com/digitaldrywood/detent/internal/workflowmetrics"
)

type Backend string

const BackendSQLite Backend = "sqlite"

const (
	SessionStateRunning               = "running"
	SessionStateOrphaned              = "orphaned"
	OrphanRecoveryResumed             = "resumed"
	OrphanRecoveryFresh               = "fresh"
	WorkerProcessOutcomeTerminated    = "terminated"
	WorkerProcessOutcomeKilled        = "killed_after_timeout"
	WorkerProcessOutcomeAlreadyExited = "already_exited"
	WorkerProcessOutcomeStaleIdentity = "stale_identity"
)

const defaultBusyTimeout = 5 * time.Second

var (
	ErrNotFound        = errors.New("store record not found")
	ErrProjectRequired = errors.New("project_id is required")
)

type Config struct {
	Backend     Backend
	Path        string
	BusyTimeout time.Duration
}

type Store interface {
	auth.Store
	StatsStore
	FairShareStore
	BudgetCostStore
	ProgressSpendStore
	WorkflowMetricsStore
	ProvenanceStore
	WorkAttemptStore
	LaneMutationStore
	ProjectDispatchStatusStore
	HealthNotificationStateStore
	StalenessWarningStore
	WorkAttemptCapacityReleaseStore
	MergeRequiredCheckStore
	OperatorStopStore
	ValidatorMemoStore
	SecurityAuditStore
	RuntimeEvidenceStore
	AgentResumeStore
	OrphanSessionStore
	RetroStore
	RoutineStore
	AdmissionStore
	schedulehealth.Store
	efficiency.Recorder
	efficiency.Reader
	APIKeyStore
	Queries() *sqlc.Queries
	Close() error
}

type StatsStore interface {
	StartRun(context.Context, RunStart) (int64, error)
	UpdateRun(context.Context, int64, RunUpdate) error
	StopRun(context.Context, int64, RunStop) error
	StartSession(context.Context, SessionStart) (int64, error)
	UpdateSessionIdentity(context.Context, int64, agentidentity.Identity) error
	UpdateSessionProviderIdentity(context.Context, int64, SessionProviderIdentity) error
	UpdateSessionWorkerProcess(context.Context, int64, WorkerProcessRegistration) error
	ListActiveWorkerProcesses(context.Context) ([]WorkerProcess, error)
	MarkSessionWorkerProcessReaped(context.Context, int64, WorkerProcessReap) error
	UpdateSessionResumeState(context.Context, int64, SessionResumeState) error
	FinishSession(context.Context, int64, SessionFinish) error
	RecordUsageEvent(context.Context, UsageEvent) (int64, error)
	UsageReport(context.Context, UsageReportQuery) (UsageReport, error)
	DailyDigest(context.Context, []DailyDigestWindow) ([]DailyDigestDay, error)
	CycleTimeReport(context.Context) (CycleTimeReport, error)
	LifetimeTotals(context.Context) (LifetimeTotals, error)
	DailyTokenSpend(context.Context, time.Time) (TokenSpend, error)
	ProjectDailyTokenSpend(context.Context, string, time.Time) (TokenSpend, error)
	BackfillSessionProjectIDs(context.Context, []SessionProjectAttribution) (int64, error)
	IssueTokenSpend(context.Context, IssueIdentity) (TokenSpend, error)
	RecentModelTokenQuantiles(context.Context, ModelTokenQuantileQuery) (ModelTokenQuantiles, error)
}

type DailyDigestWindow struct {
	Date string
	From time.Time
	To   time.Time
}

type DailyDigestDay struct {
	Date                 string
	Sessions             int64
	InputTokens          int64
	CachedInputTokens    int64
	OutputTokens         int64
	TotalTokens          int64
	OrphanResumed        int64
	OrphanFresh          int64
	CapacityOutages      int64
	CapacitySeconds      int64
	CapacityRecoveryMode string
	BreakerTrips         int64
	FailedSessions       int64
	DominantErrorClass   string
	Models               []UsageReportModel
}

type FairShareStore interface {
	ListFairShareUsage(context.Context) ([]FairShareUsage, error)
	RecordFairShareDispatch(context.Context, FairShareDispatch) error
}

type BudgetCostStore interface {
	BudgetCostEvents(context.Context, BudgetCostQuery) ([]BudgetCostEvent, error)
}

type ProgressSpendStore interface {
	IssueSpendSince(context.Context, IssueSpendSinceQuery) (IssueSpendSince, error)
}

type ProgressCreditStore interface {
	CreditIssueProgress(context.Context, IssueIdentity, time.Time) (IssueProgressCredit, error)
	IssueProgressCredit(context.Context, IssueIdentity) (IssueProgressCredit, error)
}

type WorkflowMetricsStore interface {
	RecordWorkflowPhaseEvent(context.Context, WorkflowPhaseEvent) (int64, error)
	WorkflowMetricsReport(context.Context, WorkflowMetricsQuery) (WorkflowMetricsReport, error)
	IssueWorkflowTimeline(context.Context, IssueIdentity) (WorkflowTimeline, error)
}

type ProvenanceStore interface {
	ProvenanceAttributionTrustBoundary(context.Context) (time.Time, error)
}

type WorkAttemptStore interface {
	StartWorkAttempt(context.Context, WorkAttemptStart) (int64, error)
	WorkAttempt(context.Context, int64) (WorkAttempt, error)
	RecordWorkAttemptHeartbeat(context.Context, WorkAttemptHeartbeat) error
	CompleteWorkAttempt(context.Context, WorkAttemptCompletion) error
	ListActiveWorkAttempts(context.Context, WorkAttemptQuery) ([]WorkAttempt, error)
	ListRecentTerminalWorkAttempts(context.Context, WorkAttemptHistoryQuery) ([]WorkAttempt, error)
	TimeoutExpiredWorkAttempts(context.Context, WorkAttemptTimeout) ([]WorkAttempt, error)
	ReclaimActiveWorkAttempts(context.Context, WorkAttemptReclaim) ([]WorkAttempt, error)
	RecordSchedulerDecision(context.Context, SchedulerDecision) (int64, error)
	ListRecentSchedulerDecisions(context.Context, SchedulerDecisionQuery) ([]SchedulerDecision, error)
}

type LaneMutationStore interface {
	BeginLaneMutation(context.Context, LaneMutationStart) (LaneMutationReceipt, error)
	ResolveLaneMutation(context.Context, LaneMutationResolution) error
	LaneMutationReceipt(context.Context, LaneMutationLookup) (LaneMutationReceipt, error)
	ConsumeLaneMutation(context.Context, LaneMutationConsumption) (LaneMutationReceipt, error)
}

type ConcurrencyStore interface {
	ConcurrencyReport(context.Context, ConcurrencyQuery) (ConcurrencyReport, error)
}

type IssueSchedulerDecisionStore interface {
	ListIssueSchedulerDecisions(context.Context, IssueSchedulerDecisionQuery) ([]SchedulerDecision, error)
}

type ProjectDispatchStatusStore interface {
	RecordProjectDispatchStatus(context.Context, ProjectDispatchStatus) error
	ProjectDispatchStatus(context.Context, string) (ProjectDispatchStatus, error)
}

type HealthNotificationStateStore interface {
	ListHealthNotificationStates(context.Context) ([]HealthNotificationState, error)
	SaveHealthNotificationStates(context.Context, []HealthNotificationState) error
}

type StalenessWarningStore interface {
	ListStalenessWarningStates(context.Context, string) ([]StalenessWarningState, error)
	RecordStalenessWarningReminder(context.Context, string, string, time.Time) error
	AcknowledgeStalenessWarning(context.Context, string, string, time.Time) error
	AcknowledgeStalenessWarnings(context.Context, string, []string, time.Time) error
	ReconcileStalenessWarningStates(context.Context, string, []string, time.Time, time.Time) ([]StalenessWarningState, error)
}

type WorkAttemptCapacityReleaseStore interface {
	ListPendingWorkAttemptCapacityReleases(context.Context, string) ([]WorkAttempt, error)
	ClearWorkAttemptCapacityRelease(context.Context, int64) error
}

type MergeRequiredCheckStore interface {
	EvaluateMergeRequiredChecks(context.Context, MergeRequiredCheckEvaluation) ([]MergeRequiredCheckStreak, error)
	ClearMergeRequiredCheckStreaks(context.Context, string, string) error
}

type OperatorStopStore interface {
	UpdateOperatorStop(context.Context, OperatorStopUpdate) error
	ListPendingOperatorStops(context.Context, string) ([]WorkAttempt, error)
}

type ValidatorMemoStore interface {
	RecordValidatorVerdict(context.Context, ValidatorVerdict) error
	ValidatorVerdict(context.Context, ValidatorVerdictKey) (ValidatorVerdict, error)
	ListValidatorVerdicts(context.Context, ValidatorVerdictQuery) ([]ValidatorVerdict, error)
	MarkValidatorVerdictCommented(context.Context, ValidatorVerdictKey, time.Time) error
}

type SecurityAuditStore interface {
	RecordSecurityAuditRun(context.Context, securityaudit.Run) (securityaudit.Run, error)
	LatestSecurityAuditRun(context.Context, securityaudit.Key) (securityaudit.Run, error)
	LatestSecurityAuditRunForPullRequest(context.Context, string, string, int) (securityaudit.Run, error)
	RecordSecurityAuditDisposition(context.Context, securityaudit.Disposition) (securityaudit.Disposition, error)
	ListSecurityAuditDispositions(context.Context, int64) ([]securityaudit.Disposition, error)
}

type RuntimeEvidenceStore interface {
	RuntimeEvidence(context.Context, RuntimeEvidenceQuery) (RuntimeEvidence, error)
}

type ActivityStore interface {
	ListIssueActivity(context.Context, IssueActivityQuery) ([]IssueActivityEvent, error)
	LatestIssueAgentSession(context.Context, IssueIdentity) (IssueAgentSession, error)
}

type ParkSummaryStore interface {
	IssueParkSummary(context.Context, IssueIdentity) (ParkSummary, error)
	IssueParkSummaries(context.Context, []IssueIdentity) (map[IssueIdentity]ParkSummary, error)
	ListIssueParkSummaries(context.Context, string) ([]ParkSummary, error)
	AcknowledgeIssueParks(context.Context, IssueIdentity, int64, time.Time) error
}

type AgentResumeStore interface {
	LatestCompletedAgentResumeState(context.Context, AgentResumeLookup) (AgentResumeState, error)
	LatestIssueAgentResumeState(context.Context, IssueIdentity) (AgentResumeState, error)
}

type OrphanSessionStore interface {
	ListOrphanedAgentSessions(context.Context, string) ([]OrphanedAgentSession, error)
	MarkAgentSessionOrphaned(context.Context, int64, time.Time) error
}

type RetroStore interface {
	retro.TelemetryStore
}

type RoutineStore interface {
	LatestRoutineRun(context.Context, string, string) (routinemodel.RunRecord, bool, error)
	OpenRoutineIssueIDs(context.Context, string, string) ([]string, error)
	RecordRoutineIssue(context.Context, string, string, routinemodel.IssueRecord) error
	CloseRoutineIssues(context.Context, string, string, []string) error
	RecordRoutineRun(context.Context, routinemodel.RunRecord) error
}

type AdmissionStore interface {
	CreateAdmissionProposal(context.Context, admissionmodel.Proposal) (bool, error)
	CreateAdmissionDecline(context.Context, admissionmodel.Decline) (bool, error)
	AdmissionDecline(context.Context, string, string, string) (admissionmodel.Decline, bool, error)
	MarkAdmissionDeclineCommented(context.Context, string, time.Time) error
	OpenAdmissionProposals(context.Context, string, int) ([]admissionmodel.Proposal, error)
	AdmissionProposalHistory(context.Context, string, string) ([]admissionmodel.Proposal, error)
	CountOpenAdmissionProposals(context.Context, string) (int, error)
	ExpireAdmissionProposals(context.Context, string, time.Time) (int, error)
	TransitionAdmissionProposal(context.Context, string, admissionmodel.ProposalStatus, admissionmodel.ProposalStatus, time.Time) error
	ResolveAdmissionProposal(context.Context, admissionmodel.Decision) error
	AdmissionTargetTransitions(context.Context, admissionmodel.TargetTransitionQuery) ([]admissionmodel.TargetTransition, error)
	MarkAdmissionProposalCommented(context.Context, string, time.Time) error
	RefreshAdmissionOutcomes(context.Context, admissionmodel.OutcomeRefresh) error
	AdmissionDownstreamOutcomes(context.Context, string) ([]admissionmodel.DownstreamOutcome, error)
	RecordAdmissionRun(context.Context, admissionmodel.RunRecord) error
	LatestAdmissionRun(context.Context, string) (admissionmodel.RunRecord, bool, error)
	RecentAdmissionRuns(context.Context, string, int) ([]admissionmodel.RunRecord, error)
	RecordAdmissionMalformedResult(context.Context, admissionmodel.MalformedResult, int) (admissionmodel.MalformedResult, error)
	BlockedAdmissionMalformedResult(context.Context, string, string) (admissionmodel.MalformedResult, bool, error)
	ResolveAdmissionMalformedResults(context.Context, string, string, time.Time) error
}

type AdmissionProposalDecisionReader interface {
	AdmissionProposalsAwaitingDecision(context.Context, string, time.Time) ([]admissionmodel.Proposal, error)
}

type APIKeyStore interface {
	CreateAPIKey(context.Context, APIKeyCreate) (APIKey, error)
	APIKey(context.Context, string) (APIKey, error)
	APIKeyByHash(context.Context, string) (APIKey, error)
	ListAPIKeys(context.Context) ([]APIKey, error)
	CountActiveAPIKeys(context.Context) (int64, error)
	SetAPIKeyExpiresAt(context.Context, string, time.Time) error
	RevokeAPIKey(context.Context, string, time.Time) error
	MarkAPIKeyLastUsed(context.Context, string, time.Time) error
	RecordAPIUsageLog(context.Context, APIUsageLog) error
	CountAPIUsageLogsByKey(context.Context, string) (int64, error)
}

type RuntimeEvidenceQuery struct {
	ProjectID string
}

type RuntimeEvidence struct {
	Backend             Backend
	Path                string
	Healthy             bool
	MigrationStatus     string
	MigrationVersion    int64
	Tables              []RuntimeTableEvidence
	WorkflowPhaseEvents RuntimeWorkflowPhaseEventEvidence
}

type RuntimeTableEvidence struct {
	Name     string
	Scope    string
	RowCount int64
}

type RuntimeWorkflowPhaseEventEvidence struct {
	RowCount         int64
	OldestFinishedAt *time.Time
	NewestFinishedAt *time.Time
}

type WorkflowPhaseType = workflowmetrics.PhaseType

const (
	WorkflowPhaseTypeLane           = workflowmetrics.PhaseTypeLane
	WorkflowPhaseTypeAgentSession   = workflowmetrics.PhaseTypeAgentSession
	WorkflowPhaseTypeLocalCheck     = workflowmetrics.PhaseTypeLocalCheck
	WorkflowPhaseTypeCI             = workflowmetrics.PhaseTypeCI
	WorkflowPhaseTypeGitHubBackoff  = workflowmetrics.PhaseTypeGitHubBackoff
	WorkflowPhaseTypeReview         = workflowmetrics.PhaseTypeReview
	WorkflowPhaseTypeMergeQueue     = workflowmetrics.PhaseTypeMergeQueue
	WorkflowPhaseTypeRecovery       = workflowmetrics.PhaseTypeRecovery
	WorkflowPhaseTypeOperatorAction = workflowmetrics.PhaseTypeOperatorAction
)

type WorkAttemptStatus string

const (
	WorkAttemptStatusActive   WorkAttemptStatus = "active"
	WorkAttemptStatusTerminal WorkAttemptStatus = "terminal"
)

type WorkAttemptTerminalState string

const (
	WorkAttemptTerminalSuccess         WorkAttemptTerminalState = "success"
	WorkAttemptTerminalFailure         WorkAttemptTerminalState = "failure"
	WorkAttemptTerminalCancelled       WorkAttemptTerminalState = "cancelled"
	WorkAttemptTerminalTimedOut        WorkAttemptTerminalState = "timed_out"
	WorkAttemptTerminalSuperseded      WorkAttemptTerminalState = "superseded"
	WorkAttemptTerminalAbandoned       WorkAttemptTerminalState = "abandoned"
	WorkAttemptTerminalNoProgress      WorkAttemptTerminalState = "no_progress"
	WorkAttemptTerminalMemoryCeiling   WorkAttemptTerminalState = "memory_ceiling_exceeded"
	WorkAttemptTerminalCapacity        WorkAttemptTerminalState = "capacity"
	WorkAttemptTerminalOperatorStopped WorkAttemptTerminalState = "operator_stopped"
	WorkAttemptTerminalMergeRevoked    WorkAttemptTerminalState = "merge_revoked"
	WorkAttemptTerminalLaneRevoked     WorkAttemptTerminalState = "lane_revoked"
	WorkAttemptTerminalDelivered       WorkAttemptTerminalState = "delivered"
)

type SchedulerDecisionResult string

const (
	SchedulerDecisionResultSelected SchedulerDecisionResult = "selected"
	SchedulerDecisionResultSkipped  SchedulerDecisionResult = "skipped"
)

type RunStart struct {
	StartedAt            time.Time
	PeakConcurrentAgents int64
	SessionsLaunched     int64
	InputTokens          int64
	OutputTokens         int64
	TotalTokens          int64
	RuntimeSeconds       int64
}

type RunUpdate struct {
	PeakConcurrentAgents int64
	SessionsLaunched     int64
	InputTokens          int64
	OutputTokens         int64
	TotalTokens          int64
	RuntimeSeconds       int64
}

type RunStop struct {
	StoppedAt            time.Time
	RestartReason        string
	PeakConcurrentAgents int64
	SessionsLaunched     int64
	InputTokens          int64
	OutputTokens         int64
	TotalTokens          int64
	RuntimeSeconds       int64
}

type SessionStart struct {
	RunID                        int64
	WorkAttemptID                int64
	ProjectID                    string
	IssueID                      string
	Identifier                   string
	IssueURL                     string
	StartedAt                    time.Time
	Model                        string
	RequestedModel               string
	AgentBackendID               string
	AgentBackendKind             string
	AgentRole                    string
	RuntimeIdentity              agentidentity.Identity
	ProviderThreadID             string
	ProviderSessionID            string
	ResumedFromSessionID         int64
	OrphanRecoveryOutcome        string
	OrphanRecoveryFallbackReason string
}

type SessionProjectAttribution struct {
	ProjectID  string
	Repository string
}

type SessionProviderIdentity struct {
	ThreadID  string
	SessionID string
}

type WorkerProcessIdentity struct {
	PID       int
	GroupID   int
	StartedAt time.Time
}

type WorkerProcessRegistration struct {
	WorkerProcessIdentity
	CleanupRoot string
	CleanupPath string
}

type WorkerProcess struct {
	SessionID   int64
	IssueID     string
	Identifier  string
	IssueURL    string
	FinalState  string
	CompletedAt time.Time
	WorkerProcessIdentity
	CleanupRoot string
	CleanupPath string
}

type WorkerProcessReap struct {
	ReapedAt time.Time
	Outcome  string
	Reason   string
}

type SessionResumeState struct {
	ResumedFromSessionID         int64
	OrphanRecoveryOutcome        string
	OrphanRecoveryFallbackReason string
}

type SessionFinish struct {
	CompletedAt           time.Time
	Turns                 int64
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	ModelContextWindow    *int64
	RuntimeSeconds        int64
	FinalState            string
	Model                 string
	ProviderThreadID      string
	ProviderSessionID     string
	ResumedFromSessionID  int64
	RuntimeIdentity       agentidentity.Identity
	SkillDraftProposed    bool
}

type AgentResumeLookup struct {
	ProjectID        string
	IssueID          string
	Identifier       string
	IssueURL         string
	PRNumber         int64
	PRHeadSHA        string
	PRBaseSHA        string
	RequestedModel   string
	AgentBackendID   string
	AgentBackendKind string
	AgentRole        string
}

type AgentResumeState struct {
	RuntimeIdentity   agentidentity.Identity
	DetentSessionID   int64
	ProviderThreadID  string
	ProviderSessionID string
	RequestedModel    string
	Model             string
	AgentBackendID    string
	AgentBackendKind  string
	AgentRole         string
	CompletedAt       time.Time
	Orphaned          bool
}

type OrphanedAgentSession struct {
	ResumeState   AgentResumeState
	WorkAttemptID int64
	ProjectID     string
	IssueID       string
	Identifier    string
	IssueURL      string
	WorkerType    string
	WorkerHost    string
	Lane          string
	AttemptNumber int
	StartedAt     time.Time
}

type APIKeyCreate struct {
	ID          string
	Name        string
	PrefixLast4 string
	KeyHash     string
	Scopes      []string
	ProjectIDs  []string
	CreatedAt   time.Time
	ExpiresAt   *time.Time
}

type APIKey struct {
	ID          string
	Name        string
	PrefixLast4 string
	KeyHash     string
	Scopes      []string
	ProjectIDs  []string
	CreatedAt   time.Time
	ExpiresAt   *time.Time
	LastUsedAt  *time.Time
	RevokedAt   *time.Time
}

type APIUsageLog struct {
	APIKeyID   string
	Method     string
	Path       string
	StatusCode int
	LatencyMS  int
	IP         string
	UserAgent  string
	CreatedAt  time.Time
}

type UsageEvent struct {
	ProjectID              string
	RunID                  int64
	SessionID              int64
	IssueID                string
	Identifier             string
	PRNumber               *int64
	Model                  string
	InputTokens            int64
	CachedInputTokens      int64
	OutputTokens           int64
	ReasoningOutputTokens  int64
	TotalTokens            int64
	ModelContextWindow     *int64
	CostUSD                float64
	ProjectedCostUSD       *float64
	ProjectionOvershootUSD float64
	RuntimeSeconds         int64
	StartedAt              time.Time
	FinishedAt             time.Time
	Outcome                string
}

type WorkflowPhaseEvent = workflowmetrics.PhaseEvent

type ValidatorVerdictKey struct {
	ProjectID string
	IssueID   string
	HeadSHA   string
}

type ValidatorVerdictQuery struct {
	ProjectID string
	From      time.Time
	To        time.Time
}

type ValidatorFinding struct {
	Severity string `json:"severity,omitempty"`
	Body     string `json:"body,omitempty"`
	URL      string `json:"url,omitempty"`
	Path     string `json:"path,omitempty"`
	Line     int    `json:"line,omitempty"`
}

type ValidatorVerdict struct {
	ProjectID       string
	IssueID         string
	HeadSHA         string
	Identifier      string
	IssueURL        string
	PRNumber        *int64
	Submitted       bool
	Verdict         string
	Score           float64
	Summary         string
	Findings        []ValidatorFinding
	Commented       bool
	FailureAttempts int
	NextRetryAt     *time.Time
	RecordedAt      time.Time
	UpdatedAt       time.Time
}

type WorkAttempt struct {
	ID                     int64
	ProjectID              string
	IssueID                string
	Identifier             string
	IssueURL               string
	PRNumber               *int64
	Repo                   string
	WorkerType             string
	WorkerHost             string
	Lane                   string
	AttemptNumber          int
	Status                 WorkAttemptStatus
	StartedAt              time.Time
	LeaseExpiresAt         time.Time
	HeartbeatAt            time.Time
	CompletedAt            time.Time
	TerminalState          WorkAttemptTerminalState
	ErrorClass             string
	ErrorMessage           string
	Phase                  string
	StatusMessage          string
	CurrentStep            *int64
	TotalSteps             *int64
	ProgressPercent        *int64
	CurrentCommand         string
	WaitReason             string
	GitHubRateSnapshotJSON string
	CIState                string
	CapacitySnapshotJSON   string
	WorkerMetadataJSON     string
	MetricsJSON            string
	NextAction             string
	DetentSessionID        int64
	ProviderSessionID      string
	RuntimeIdentity        agentidentity.Identity
}

type LaneMutationDisposition string

const (
	LaneMutationPreserveOwnership LaneMutationDisposition = "preserve_ownership"
	LaneMutationAcceptCompletion  LaneMutationDisposition = "accept_completion"
	LaneMutationRevokeWorker      LaneMutationDisposition = "revoke_worker"
)

type LaneMutationTrackerResult string

const (
	LaneMutationTrackerPrepared   LaneMutationTrackerResult = "prepared"
	LaneMutationTrackerApplied    LaneMutationTrackerResult = "applied"
	LaneMutationTrackerBlocked    LaneMutationTrackerResult = "blocked"
	LaneMutationTrackerFailed     LaneMutationTrackerResult = "failed"
	LaneMutationTrackerSuperseded LaneMutationTrackerResult = "superseded"
)

type LaneMutationReceipt struct {
	ID            int64
	ProjectID     string
	IssueID       string
	WorkAttemptID int64
	Generation    uint64
	Disposition   LaneMutationDisposition
	FromState     string
	ToState       string
	Reason        string
	TrackerResult LaneMutationTrackerResult
	RequestedAt   time.Time
	ResolvedAt    time.Time
	ConsumedAt    time.Time
	ErrorMessage  string
}

type LaneMutationStart struct {
	ProjectID     string
	IssueID       string
	WorkAttemptID int64
	Generation    uint64
	Disposition   LaneMutationDisposition
	FromState     string
	ToState       string
	Reason        string
	RequestedAt   time.Time
}

type LaneMutationResolution struct {
	ReceiptID     int64
	WorkAttemptID int64
	Generation    uint64
	TrackerResult LaneMutationTrackerResult
	ResolvedAt    time.Time
	ErrorMessage  string
}

type LaneMutationLookup struct {
	ProjectID     string
	IssueID       string
	WorkAttemptID int64
	Generation    uint64
	ToState       string
}

type LaneMutationConsumption struct {
	ReceiptID     int64
	ProjectID     string
	IssueID       string
	WorkAttemptID int64
	Generation    uint64
	ToState       string
	ConsumedAt    time.Time
}

type WorkAttemptStart struct {
	ProjectID              string
	IssueID                string
	Identifier             string
	IssueURL               string
	PRNumber               *int64
	Repo                   string
	WorkerType             string
	WorkerHost             string
	Lane                   string
	AttemptNumber          int
	StartedAt              time.Time
	LeaseExpiresAt         time.Time
	Phase                  string
	StatusMessage          string
	CurrentStep            *int64
	TotalSteps             *int64
	ProgressPercent        *int64
	CurrentCommand         string
	WaitReason             string
	GitHubRateSnapshotJSON string
	CIState                string
	CapacitySnapshotJSON   string
	WorkerMetadataJSON     string
	MetricsJSON            string
	NextAction             string
	DetentSessionID        int64
	ProviderSessionID      string
	RuntimeIdentity        agentidentity.Identity
}

type WorkAttemptHeartbeat struct {
	AttemptID              int64
	HeartbeatAt            time.Time
	LeaseExpiresAt         time.Time
	Phase                  string
	StatusMessage          string
	CurrentStep            *int64
	TotalSteps             *int64
	ProgressPercent        *int64
	CurrentCommand         string
	WaitReason             string
	GitHubRateSnapshotJSON string
	CIState                string
	CapacitySnapshotJSON   string
	WorkerMetadataJSON     string
	MetricsJSON            string
	NextAction             string
	ErrorClass             string
	ErrorMessage           string
	DetentSessionID        int64
	ProviderSessionID      string
	RuntimeIdentity        agentidentity.Identity
}

type WorkAttemptCompletion struct {
	AttemptID              int64
	CompletedAt            time.Time
	Status                 WorkAttemptStatus
	TerminalState          WorkAttemptTerminalState
	SessionFinalState      string
	ErrorClass             string
	ErrorMessage           string
	Phase                  string
	StatusMessage          string
	WaitReason             string
	GitHubRateSnapshotJSON string
	CIState                string
	CapacitySnapshotJSON   string
	WorkerMetadataJSON     string
	MetricsJSON            string
	NextAction             string
	DetentSessionID        int64
	ProviderSessionID      string
	RuntimeIdentity        agentidentity.Identity
}

type WorkAttemptQuery struct {
	ProjectID string
}

type WorkAttemptHistoryQuery struct {
	ProjectID  string
	IssueID    string
	Identifier string
	IssueURL   string
	WorkerType string
	Limit      int
}

type ConcurrencyQuery struct {
	ProjectID string
	From      time.Time
	To        time.Time
	Bucket    time.Duration
}

type ConcurrencyReport struct {
	From         time.Time
	To           time.Time
	Bucket       time.Duration
	Series       []ConcurrencySeries
	AttemptCount int
}

type ConcurrencySeries struct {
	ProjectID string
	Buckets   []ConcurrencyBucket
}

type ConcurrencyBucket struct {
	Start         time.Time
	End           time.Time
	Median        int
	P90           int
	Max           int
	ActiveSeconds int64
}

type WorkAttemptTimeout struct {
	ProjectID     string
	Now           time.Time
	TerminalState WorkAttemptTerminalState
	ErrorClass    string
	ErrorMessage  string
}

type WorkAttemptReclaim struct {
	ProjectID     string
	Now           time.Time
	TerminalState WorkAttemptTerminalState
	ErrorClass    string
	ErrorMessage  string
}

type MergeRequiredCheckEvaluation struct {
	ProjectID                 string
	IssueID                   string
	Repository                string
	PRNumber                  int
	HeadSHA                   string
	RequiredChecksFingerprint string
	MissingChecks             []string
	EvaluatedAt               time.Time
}

type MergeRequiredCheckStreak struct {
	CheckName          string
	ConsecutiveMissing int
}

type OperatorStopUpdate struct {
	AttemptID          int64
	Phase              string
	StatusMessage      string
	WorkerMetadataJSON string
	NextAction         string
}

type SchedulerDecision struct {
	ID                     int64
	ProjectID              string
	IssueID                string
	Identifier             string
	IssueURL               string
	PRNumber               *int64
	Repo                   string
	Lane                   string
	QueuePosition          int
	Result                 SchedulerDecisionResult
	Reason                 string
	Selected               bool
	Retry                  bool
	AttemptNumber          int
	WorkerHost             string
	DecisionAt             time.Time
	WaitReason             string
	CapacitySnapshotJSON   string
	GitHubRateSnapshotJSON string
	MetadataJSON           string
}

type SchedulerDecisionQuery struct {
	ProjectID string
	Limit     int
}

type ProjectDispatchStatus struct {
	ProjectID              string
	CandidateCount         int
	EligibleCandidateCount int
	CandidateFingerprint   string
	SelectedCount          int
	SkippedCount           int
	WaitReason             string
	WaitReasonCode         string
	AllSkippedSince        *time.Time
	LastSelectedAt         *time.Time
	ObservedAt             time.Time
}

type HealthNotificationState struct {
	Identity  string
	StateJSON []byte
	UpdatedAt time.Time
}

type StalenessWarningState struct {
	ProjectID      string
	WarningID      string
	RemindedAt     *time.Time
	AcknowledgedAt *time.Time
	LastSeenAt     *time.Time
}

type IssueSchedulerDecisionQuery struct {
	Identity IssueIdentity
	Limit    int
}

type WorkflowMetricsQuery struct {
	ProjectID string
	From      time.Time
	To        time.Time
}

type WorkflowMetricsReport struct {
	Lanes      []WorkflowPhaseMetric
	SubPhases  []WorkflowPhaseMetric
	LaneTrends []WorkflowLaneTrend
}

type WorkflowPhaseMetric struct {
	ProjectID             string
	PhaseType             string
	PhaseName             string
	Count                 int64
	TotalSeconds          int64
	AverageSeconds        int64
	P50Seconds            int64
	P90Seconds            int64
	P95Seconds            int64
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	Turns                 int64
	EndpointFamily        string
	ActiveSeconds         int64
	WaitSeconds           int64
	ActivePercent         float64
	Representatives       []WorkflowRepresentativeRun
}

type WorkflowRepresentativeRun struct {
	RunID      int64
	SessionID  int64
	IssueID    string
	Identifier string
	IssueURL   string
	FinishedAt time.Time
}

type WorkflowLaneTrend struct {
	ProjectID  string
	PhaseName  string
	Points     []WorkflowLaneTrendPoint
	TotalCount int64
}

type WorkflowLaneTrendPoint struct {
	Label          string
	BucketEnd      time.Time
	Count          int64
	AverageSeconds int64
}

type WorkflowTimeline struct {
	Events []WorkflowPhaseEvent
}

type UsageReportGroup string

const (
	UsageReportByDay     UsageReportGroup = "day"
	UsageReportByProject UsageReportGroup = "project"
	UsageReportByIssue   UsageReportGroup = "issue"
	UsageReportByPR      UsageReportGroup = "pr"
	UsageReportByModel   UsageReportGroup = "model"
)

type UsageReportQuery struct {
	By   UsageReportGroup
	From time.Time
	To   time.Time
}

type UsageReport struct {
	By     UsageReportGroup
	From   string
	To     string
	Totals UsageReportTotals
	Rows   []UsageReportRow
}

type UsageReportTotals struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	ModelContextWindow    int64
	RuntimeSeconds        int64
	Events                int64
	Models                []UsageReportModel
}

type UsageReportRow struct {
	Key                   string
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	ModelContextWindow    int64
	RuntimeSeconds        int64
	Events                int64
	Models                []UsageReportModel
}

type UsageReportModel struct {
	Model                 string
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	ModelContextWindow    int64
	RuntimeSeconds        int64
	Events                int64
}

type CycleTimeReport struct {
	Issues         []CycleTimeIssue
	Buckets        []CycleTimeBucket
	AverageSeconds int64
}

type CycleTimeIssue struct {
	Key             string
	StartedAt       time.Time
	CompletedAt     time.Time
	DurationSeconds int64
	Sessions        int64
}

type CycleTimeBucket struct {
	Label      string
	MinSeconds int64
	MaxSeconds int64
	Count      int
}

type IssueIdentity struct {
	ProjectID  string
	IssueID    string
	Identifier string
	IssueURL   string
}

type IssueActivityQuery struct {
	ProjectID      string
	IssueID        string
	Identifier     string
	IssueURL       string
	IncludeVerbose bool
	Limit          int
	Offset         int
}

type IssueActivityEvent struct {
	ID            string
	Source        string
	Kind          string
	Name          string
	At            time.Time
	AttemptNumber int
	SessionID     int64
	Detail        string
	Reason        string
	Status        string
	Model         string
	Turns         int64
	TotalTokens   int64
	Verbose       bool
}

type IssueAgentSession struct {
	ProjectID         string
	DetentSessionID   int64
	ProviderThreadID  string
	ProviderSessionID string
	AgentBackendKind  string
	CompletedAt       time.Time
}

type TokenSpend struct {
	Date                  string
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	Sessions              int64
	ByModel               []ModelTokenSpend
}

type ModelTokenQuantileQuery struct {
	Model string
	Limit int64
}

type ModelTokenQuantiles struct {
	Model                string
	Sessions             int64
	P50InputTokens       int64
	P90InputTokens       int64
	P50CachedInputTokens int64
	P90CachedInputTokens int64
	P50OutputTokens      int64
	P90OutputTokens      int64
	P50TotalTokens       int64
	P90TotalTokens       int64
}

type LifetimeTotals struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	RuntimeSeconds        int64
	Sessions              int64
	Runs                  int64
	OrphanResumed         int64
	OrphanFresh           int64
	ResumedInputTokens    int64
	ResumedCachedTokens   int64
}

type BudgetCostQuery struct {
	ProjectIDs []string
	From       time.Time
	To         time.Time
}

type BudgetCostEvent struct {
	ProjectID string
	At        time.Time
	CostUSD   float64
}

type IssueSpendSinceQuery struct {
	ProjectID  string
	IssueID    string
	Identifier string
	Since      time.Time
}

type IssueSpendSince struct {
	CostUSD        float64
	TotalTokens    int64
	Sessions       int64
	FirstSessionAt time.Time
	LastSessionAt  time.Time
}

type IssueProgressCredit struct {
	ProjectID  string    `json:"project_id"`
	IssueID    string    `json:"issue_id,omitempty"`
	Identifier string    `json:"identifier,omitempty"`
	IssueURL   string    `json:"issue_url,omitempty"`
	CreditedAt time.Time `json:"credited_at"`
}

type ModelTokenSpend struct {
	Model                 string
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	Sessions              int64
}

type FairShareUsage struct {
	ProjectID      string
	Weight         int
	Dispatches     int64
	RuntimeSeconds int64
	UpdatedAt      time.Time
}

type FairShareDispatch struct {
	ProjectID      string
	Weight         int
	RuntimeSeconds int64
	DispatchedAt   time.Time
}

func Open(ctx context.Context, cfg Config) (Store, error) {
	switch cfg.Backend {
	case "", BackendSQLite:
		return openSQLite(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported store backend %q", cfg.Backend)
	}
}

func busyTimeoutMillis(timeout time.Duration) int64 {
	if timeout <= 0 {
		return defaultBusyTimeout.Milliseconds()
	}

	millis := timeout.Milliseconds()
	if millis < 1 {
		return 1
	}
	return millis
}
