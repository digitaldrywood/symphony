package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/budget"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workspace"
)

const (
	FinalStateCompleted                = "completed"
	FinalStateFailed                   = "failed"
	FinalStateTokenCeilingExceeded     = "token_ceiling_exceeded"
	FinalStateBudgetProjectionExceeded = "budget_projection_exceeded"
	FinalStateMemoryCeilingExceeded    = "memory_ceiling_exceeded"
	FinalStateOperatorStopped          = "operator_stopped"
	FinalStateMergeRevoked             = "merge_revoked"
	FinalStateLaneRevoked              = "lane_revoked"
	FinalStateCIUnavailable            = "ci_unavailable"
	FinalStateMergeDurationExceeded    = "merge_duration_exceeded"
	FinalStateMergeFallbackExceeded    = "merge_fallback_budget_exceeded"
	FinalStateNeedsHumanAttention      = "needs_human_attention"
	TokenCeilingSourceAbsolute         = "max_session_tokens"
	TokenCeilingSourceContextWindow    = "max_session_context_multiplier"

	RunModeImplement = "implement"
	RunModePlan      = "plan"
	RunModeMerge     = "merge"
	RunModeRoutine   = "routine"

	RunOutputMergeFastPathClean       = "merge_fast_path_clean"
	RunOutputMergeFastPathCheckedHead = "merge_fast_path_checked_head"
	RunOutputMergeFallbackDeferred    = "merge_fallback_deferred"
	RunOutputMergeFallbackResolved    = "merge_fallback_resolved"
	RunOutputMergeFallbackRework      = "merge_fallback_rework"
)

var (
	ErrSessionTokenCeilingExceeded     = errors.New("session token ceiling exceeded")
	ErrSessionBudgetProjectionExceeded = errors.New("session budget projection exceeded")
	ErrSessionMemoryCeilingExceeded    = errors.New("session memory ceiling exceeded")
	ErrTurnDurationExceeded            = errors.New("agent turn duration exceeded")
	ErrSessionDurationExceeded         = errors.New("agent session duration exceeded")
	ErrOperatorStopped                 = errors.New("operator stopped run")
	ErrMergeRevoked                    = errors.New("merge eligibility revoked")
	ErrLaneRevoked                     = errors.New("worker-owned lane revoked")
	ErrCIUnavailable                   = errors.New("CI unavailable")
	ErrMergeWorkerStartupTimeout       = errors.New("merge worker startup timed out")
	ErrMergeWorkerDurationExceeded     = errors.New("merge worker duration exceeded")
	ErrMergeFallbackBudgetExceeded     = errors.New("merge fallback budget exceeded")
	ErrModelPermitUnavailable          = errors.New("provider model permit unavailable")
	ErrAgentTurnCleanup                = errors.New("agent turn cleanup failed")
	ErrWorkerProcessReap               = errors.New("worker process reap failed")
	ErrWorkspacePreparation            = errors.New("workspace preparation failed")
	ErrWorkspaceBranchHeld             = errors.New("workspace branch held by worktree")
	ErrAgentResumeUnsupported          = errors.New("agent backend does not support resume verification")
	ErrDeliverableRecoveryExhausted    = errors.New("deliverable recovery exhausted")
	ErrSubscriptionAuthRequired        = errors.New("ChatGPT subscription authentication is required")
	ErrSecurityAuditToolUse            = errors.New("security audit attempted to use a tool")
)

type WorkspaceBranchHeldError struct {
	Branch       string
	WorktreePath string
	PRNumber     int
}

func (e *WorkspaceBranchHeldError) Error() string {
	if e == nil {
		return ErrWorkspaceBranchHeld.Error()
	}
	detail := fmt.Sprintf("branch held by worktree at %q", strings.TrimSpace(e.WorktreePath))
	if e.PRNumber > 0 {
		detail += fmt.Sprintf(" (PR #%d checkout)", e.PRNumber)
	}
	return detail + " — will resume when released"
}

func (e *WorkspaceBranchHeldError) Unwrap() error {
	return ErrWorkspaceBranchHeld
}

type WorkspaceBranchHold struct {
	Branch       string
	WorktreePath string
	PRNumber     int
	Held         bool
}

type WorkspaceBranchHoldInspector interface {
	InspectWorkspaceBranchHold(context.Context, connector.Issue) (WorkspaceBranchHold, error)
}

type DeliverableCommandError struct {
	OperationClass string
	Operation      string
	Arguments      string
	ItemID         string
	Command        string
	Status         string
	ExitCode       *int
	Message        string
	Body           string
	TargetRef      *DeliverableTargetRefEvidence
}

type DeliverableTargetRefEvidence struct {
	Remote                     string `json:"remote"`
	Ref                        string `json:"ref"`
	InitialRemoteHeadSHA       string `json:"initial_remote_head_sha,omitempty"`
	PostCommandLocalHeadSHA    string `json:"post_command_local_head_sha,omitempty"`
	PostCommandRemoteHeadSHA   string `json:"post_command_remote_head_sha,omitempty"`
	FinalLocalHeadSHA          string `json:"final_local_head_sha,omitempty"`
	FinalRemoteHeadSHA         string `json:"final_remote_head_sha,omitempty"`
	InitialRemoteRefExists     bool   `json:"initial_remote_ref_exists"`
	PostCommandRemoteRefExists bool   `json:"post_command_remote_ref_exists"`
	FinalRemoteRefExists       bool   `json:"final_remote_ref_exists"`
	InitialObserved            bool   `json:"initial_observed"`
	PostCommandObserved        bool   `json:"post_command_observed"`
	FinalObserved              bool   `json:"final_observed"`
	AdvancedToLocalHead        bool   `json:"advanced_to_local_head"`
	CheckError                 string `json:"check_error,omitempty"`
}

type DeliverableCommandEvidence struct {
	ItemID         string                        `json:"item_id,omitempty"`
	OperationClass string                        `json:"operation_class"`
	Operation      string                        `json:"operation"`
	Command        string                        `json:"command,omitempty"`
	Status         string                        `json:"status,omitempty"`
	ExitCode       *int                          `json:"exit_code,omitempty"`
	Outcome        string                        `json:"outcome"`
	TargetRef      *DeliverableTargetRefEvidence `json:"target_ref,omitempty"`
}

func (e *DeliverableCommandError) Error() string {
	if e == nil {
		return "deliverable command failed: no error detail returned"
	}
	operation := strings.TrimSpace(e.Operation)
	if operation == "" {
		operation = "unknown"
	}
	parts := []string{"deliverable command failed (" + operation + ")"}
	if status := strings.TrimSpace(e.Status); status != "" {
		parts = append(parts, "status="+status)
	}
	if arguments := strings.TrimSpace(e.Arguments); arguments != "" && !strings.EqualFold(arguments, "null") {
		parts = append(parts, "arguments="+arguments)
	}
	if e.ExitCode != nil {
		parts = append(parts, fmt.Sprintf("exit_code=%d", *e.ExitCode))
	}
	if message := strings.TrimSpace(e.Message); message != "" && !strings.EqualFold(message, "null") {
		parts = append(parts, "message="+message)
	}
	if body := strings.TrimSpace(e.Body); body != "" && !strings.EqualFold(body, "null") && body != strings.TrimSpace(e.Message) {
		parts = append(parts, "response="+body)
	}
	if len(parts) == 1 {
		parts = append(parts, "detail=no error detail returned")
	}
	return strings.Join(parts, ": ")
}

func (e *DeliverableCommandError) BackendErrorBody() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Body)
}

func (e *DeliverableCommandError) BackendErrorMessage() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

type DeliverableRecoveryError struct {
	Branch string
	Err    error
}

func (e *DeliverableRecoveryError) Error() string {
	if e == nil {
		return ErrDeliverableRecoveryExhausted.Error()
	}
	message := ErrDeliverableRecoveryExhausted.Error()
	if branch := strings.TrimSpace(e.Branch); branch != "" {
		message += " for pushed branch " + branch
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *DeliverableRecoveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *DeliverableRecoveryError) Is(target error) bool {
	return target == ErrDeliverableRecoveryExhausted
}

type Backend interface {
	Run(context.Context, RunRequest) (RunResult, error)
}

type BlockedRecoveryInspector interface {
	BlockedRecoverySnapshot(context.Context, RunRequest) BlockedRecoverySnapshot
}

type GitHubRESTBudgetProber interface {
	ProbeGitHubRESTBudget(context.Context, connector.Issue) (telemetry.RESTBudget, bool, error)
}

type BlockedRecoverySnapshot struct {
	ConfigFingerprint              string
	ToolingFingerprint             string
	BaseFingerprint                string
	HeadSHA                        string
	WorkspaceFingerprint           string
	WorkspaceStatus                string
	WorkspacePresent               bool
	WorkspaceFiles                 int
	UnpushedCommits                int
	UnpushedCommitRefs             []string
	TrackedPaths                   []string
	UntrackedPaths                 []string
	CommitsNotInPullRequest        []string
	PullRequestComparisonAvailable bool
	Health                         string
}

type Validator interface {
	Validate(context.Context, ValidatorRequest) (gate.ValidatorResult, error)
}

type SecurityAuditor interface {
	Audit(context.Context, SecurityAuditRequest) (SecurityAuditExecution, error)
}

type WorkspaceReaper interface {
	ReapWorkspace(context.Context, connector.Issue) (WorkspaceReapResult, error)
}

type WorkspaceReconciler interface {
	ReconcileWorkspaces(context.Context, []connector.Issue) (WorkspaceReconcileResult, error)
}

type DailyBudgetStatusProvider interface {
	DailyBudgetStatus(context.Context, time.Time) (DailyBudgetStatus, bool, error)
}

type IssueBudgetStatusProvider interface {
	IssueBudgetStatus(context.Context, connector.Issue) (IssueBudgetStatus, bool, error)
}

type DailyBudgetStatus struct {
	Active          bool
	CurrentSpendUSD float64
	MaxUSD          float64
}

type IssueBudgetStatus struct {
	Active          bool
	CurrentSpendUSD float64
	MaxUSD          float64
}

type WorkspaceReapResult struct {
	Path      string
	Worktrees int
	Branches  int
	Processes int
}

type WorkspaceCleanupFailure struct {
	Path  string
	Error string
}

type WorkspaceReconcileResult struct {
	Removed           int
	ActiveSkipped     int
	PreservedSkipped  int
	RegisteredSkipped int
	UnownedSkipped    int
	CompletedPaths    []string
	Failures          []WorkspaceCleanupFailure
}

type AgentBackend interface {
	RunTurn(context.Context, AgentTurnRequest, AgentUpdateHandler) (AgentTurnResult, error)
}

type AgentToolBackend interface {
	RunTurnWithTools(context.Context, AgentTurnRequest, []AgentTool, AgentToolHandler, AgentUpdateHandler) (AgentTurnResult, error)
}

type AgentTool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

type AgentToolCall struct {
	Name      string
	Arguments json.RawMessage
}

type AgentToolResult struct {
	Content string
	Success bool
}

type AgentToolHandler func(context.Context, AgentToolCall) (AgentToolResult, error)

type AgentResumeVerifier interface {
	VerifyResume(context.Context, AgentResume) error
}

type AgentModelCatalogProvider interface {
	ListModels(context.Context) ([]AgentModel, error)
}

type AgentDefaultModelProvider interface {
	DefaultModel(context.Context, string) (string, error)
}

type AgentModel struct {
	ID                        string
	Model                     string
	Default                   bool
	Upgrade                   string
	SupportedReasoningEfforts []string
}

type AgentTurnRequest struct {
	Workspace               string
	TempDir                 string
	Prompt                  string
	ToolInstructions        string
	SupplementalTools       bool
	ReadOnly                bool
	RequireSubscriptionAuth bool
	Model                   string
	ModelProvider           string
	ServiceTier             string
	ReasoningEffort         string
	Resume                  AgentResume
	MaxTurns                int
	TurnTimeout             time.Duration
	MaxDuration             time.Duration
	ExtraWritableRoots      []string
	DeliverableKind         string
	DeliverableRepository   string
	IssueRepository         string
	Environment             procgroup.Environment
	MaxRSSBytes             uint64
	RSSPollInterval         time.Duration
	cacheStrategy           string
	projectID               string
	workerGitHub            workerGitHubPolicy
	processRSS              func(context.Context, procgroup.Identity) (uint64, error)
}

type AgentResume struct {
	ThreadID  string
	SessionID string
}

type AgentTurnResult struct {
	ThreadID           string
	TurnID             string
	SessionID          string
	AuthenticationMode string
}

type AgentTurnCleanupError struct {
	Err error
}

func NewAgentTurnCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return &AgentTurnCleanupError{Err: err}
}

func (e *AgentTurnCleanupError) Error() string {
	if e == nil || e.Err == nil {
		return ErrAgentTurnCleanup.Error()
	}
	return fmt.Sprintf("%s: %v", ErrAgentTurnCleanup, e.Err)
}

func (e *AgentTurnCleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *AgentTurnCleanupError) Is(target error) bool {
	return target == ErrAgentTurnCleanup
}

type AgentUpdateHandler func(AgentUpdate) error

type AgentUpdateType string

const (
	AgentUpdateProcessStarted   AgentUpdateType = "process_started"
	AgentUpdateProviderIdentity AgentUpdateType = "provider_identity"
	AgentUpdateMessageDelta     AgentUpdateType = "agent_message_delta"
	AgentUpdateTokenUsage       AgentUpdateType = "token_usage"
	AgentUpdateRateLimits       AgentUpdateType = "rate_limits"
	AgentUpdateTurnStarted      AgentUpdateType = "turn_started"
	AgentUpdateTurnCompleted    AgentUpdateType = "turn_completed"
	AgentUpdateModelUpdated     AgentUpdateType = "model_updated"
	AgentUpdateRuntimeIdentity  AgentUpdateType = "runtime_identity"
	AgentUpdateToolStarted      AgentUpdateType = "tool_started"
	AgentUpdateToolOutput       AgentUpdateType = "tool_output"
	AgentUpdateToolCompleted    AgentUpdateType = "tool_completed"
	AgentUpdateMCPElicitation   AgentUpdateType = "mcp_elicitation"
	AgentUpdateResourceUsage    AgentUpdateType = "resource_usage"
)

type AgentUpdate struct {
	Type                AgentUpdateType
	Method              string
	ProcessIdentity     string
	WorkerProcess       procgroup.Identity
	ThreadID            string
	TurnID              string
	AuxiliaryTurn       bool
	ProviderSessionID   string
	ItemID              string
	Tool                string
	Command             string
	Delta               string
	Status              string
	ExitCode            *int
	Model               string
	RuntimeIdentity     agentidentity.Identity
	BackendErrorBody    string
	BackendErrorMessage string
	Tokens              AgentTokenUsage
	RateLimits          *telemetry.RateLimits
	RSSBytes            uint64
	RSSCeilingBytes     uint64
}

type AgentTokenUsage struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	ThreadTotal           *AgentTokenCounts
	Last                  *AgentTokenCounts
	ModelContextWindow    *int64
}

type AgentTokenCounts struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
}

type SessionTokenCeilingError struct {
	TotalTokens        int64
	CeilingTokens      int64
	Source             string
	ModelContextWindow int64
	ContextMultiplier  float64
}

type SessionBudgetProjectionError struct {
	ObservedCostUSD  float64
	ProjectedCostUSD float64
	Model            string
	EstimateSource   budget.EstimateSource
}

type SessionMemoryCeilingError struct {
	RSSBytes     uint64
	CeilingBytes uint64
}

func (e *SessionMemoryCeilingError) Error() string {
	if e == nil {
		return ErrSessionMemoryCeilingExceeded.Error()
	}
	return fmt.Sprintf("%s: rss_bytes=%d ceiling_bytes=%d", ErrSessionMemoryCeilingExceeded, e.RSSBytes, e.CeilingBytes)
}

func (e *SessionMemoryCeilingError) Unwrap() error {
	return ErrSessionMemoryCeilingExceeded
}

func (e *SessionTokenCeilingError) Error() string {
	source := e.Source
	if source == "" {
		source = "unknown"
	}
	message := fmt.Sprintf("%s: total_tokens=%d ceiling_tokens=%d source=%s", ErrSessionTokenCeilingExceeded, e.TotalTokens, e.CeilingTokens, source)
	if e.ModelContextWindow > 0 {
		message += fmt.Sprintf(" model_context_window=%d", e.ModelContextWindow)
	}
	if e.ContextMultiplier > 0 {
		message += fmt.Sprintf(" context_multiplier=%g", e.ContextMultiplier)
	}
	return message
}

func (e *SessionTokenCeilingError) Unwrap() error {
	return ErrSessionTokenCeilingExceeded
}

func (e *SessionBudgetProjectionError) Error() string {
	if e == nil {
		return ErrSessionBudgetProjectionExceeded.Error()
	}
	return fmt.Sprintf(
		"%s: observed_cost_usd=%.6f projected_cost_usd=%.6f model=%s estimate_source=%s",
		ErrSessionBudgetProjectionExceeded,
		e.ObservedCostUSD,
		e.ProjectedCostUSD,
		strings.TrimSpace(e.Model),
		e.EstimateSource,
	)
}

func (e *SessionBudgetProjectionError) Unwrap() error {
	return ErrSessionBudgetProjectionExceeded
}

type agentDurationLimitError struct {
	limit    error
	duration time.Duration
}

func (e *agentDurationLimitError) Error() string {
	return fmt.Sprintf("%s after %s", e.limit, e.duration)
}

func (e *agentDurationLimitError) Is(target error) bool {
	return target == e.limit || target == context.DeadlineExceeded
}

type RunRequest struct {
	Execution                 Execution
	Policy                    policy.Descriptor
	ProjectID                 string
	Issue                     connector.Issue
	Attempt                   int
	WorkAttemptID             int64
	Generation                uint64
	Mode                      string
	DispatchSourceState       string
	DispatchTargetState       string
	PriorAttempt              PriorAttempt
	StartedAt                 time.Time
	WorkerHost                string
	RetryMode                 RetryMode
	ResumeState               store.AgentResumeState
	SelectorContext           selector.Context
	OnUsageUpdate             UsageUpdateHandler
	OnActivityUpdate          AgentActivityUpdateHandler
	OnOverrideRejected        AgentOverrideRejectionHandler
	ProgressProbe             SessionProgressProbe
	CheckpointValidate        func(context.Context) error
	Routine                   *RoutineRequest
	Admission                 *AdmissionRequest
	AgentTools                []AgentTool
	AgentToolHandler          AgentToolHandler
	AcquireModelPermit        ModelPermitAcquirer
	MergePrecheck             *MergePrecheck
	MergeRefreshHeadSHA       string
	ForgeRetry                *ForgeRetry
	sessionBrake              *sessionBrakeController
	workerGitHubActor         connector.IssueActor
	deliverableRecoveryBranch string
	sessionTurnOffset         int
	sessionTokenOffset        int64
	retainCheckpoint          bool
}

type ForgeRetry struct {
	Host              string
	Operation         string
	Arguments         string
	Branch            string
	WorkProductPushed bool
}

type SessionProgressProbe func(context.Context) (string, error)

type ModelPermitAcquirer func(context.Context) error

type MergePrecheck struct {
	ConflictPaths []string
	HeadSHA       string
	Status        string
	Message       string
	DiffStats     DiffStats
	HeadChanged   bool
}

type RoutineRequest struct {
	Name     string
	Schedule string
	Prompt   string
}

type AdmissionRequest struct {
	Schedule        string
	TargetState     string
	CriteriaSection string
	CriteriaText    string
	Dimensions      []AdmissionDimension
	EffortSection   string
	EffortText      string
	AllowedEfforts  []string
	Candidates      []AdmissionCandidate
}

type AdmissionDimension struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

type AdmissionCandidate struct {
	Dependencies *AdmissionDependencies `json:"dependencies,omitempty"`
	ID           string                 `json:"id"`
	Identifier   string                 `json:"identifier"`
	Title        string                 `json:"title"`
	Description  string                 `json:"description"`
	State        string                 `json:"state"`
	AuthorID     string                 `json:"author_id,omitempty"`
	Labels       []string               `json:"labels,omitempty"`
}

type AdmissionDependencies struct {
	ObservedAt time.Time             `json:"observed_at"`
	Readiness  string                `json:"readiness"`
	Ready      bool                  `json:"ready"`
	References []AdmissionDependency `json:"references"`
}

type AdmissionDependency struct {
	Identifier       string `json:"identifier"`
	State            string `json:"state,omitempty"`
	Closed           bool   `json:"closed"`
	PullRequestState string `json:"pull_request_state,omitempty"`
	Ready            bool   `json:"ready"`
	Error            string `json:"error,omitempty"`
}

type AgentOverrideRejectionHandler func([]AgentOverrideRejection) error

type AgentOverrideRejection struct {
	Field  string
	Value  string
	Reason string
}

type RetryMode string

const (
	RetryModeFresh  RetryMode = "fresh"
	RetryModeResume RetryMode = "resume"
)

type ValidatorRequest struct {
	Issue            connector.Issue
	StartedAt        time.Time
	SelectorContext  selector.Context
	OnUsageUpdate    UsageUpdateHandler
	OnActivityUpdate AgentActivityUpdateHandler
}

type SecurityAuditRequest struct {
	Issue           connector.Issue
	Snapshot        securityaudit.Snapshot
	StartedAt       time.Time
	SelectorContext selector.Context
}

type SecurityAuditExecution struct {
	InvocationID       string
	AuthenticationMode string
	WorkerProcess      procgroup.Identity
	ProviderThreadID   string
	ProviderSessionID  string
	Output             string
	Result             securityaudit.Result
	StartedAt          time.Time
	CompletedAt        time.Time
}

type RunResult struct {
	Checkpoint              *workspace.CheckpointRecord
	FinalState              string
	Output                  string
	Model                   string
	TurnStarted             bool
	RuntimeIdentity         agentidentity.Identity
	Tokens                  TokenTotals
	DiffStats               DiffStats
	ArtifactEvidence        ArtifactProgressEvidence
	RateLimits              *telemetry.RateLimits
	BudgetRefusal           *BudgetRefusal
	SkillDraftProposed      bool
	PullRequestUpdated      bool
	PullRequestHeadPushed   bool
	CITriggerLabelReapplied bool
	ForgeWriteCompleted     bool
	DeliverableCommands     []DeliverableCommandEvidence
	WorkspaceBranch         string
	MergePrecheck           *MergePrecheck
	MergeFallbackFindings   string
	budgetProjection        *dispatchBudgetProjection
}

type ArtifactProgressEvidence struct {
	Available          bool
	InitialFiles       int
	CurrentFiles       int
	InitialFingerprint string
	CurrentFingerprint string
	Warning            string
}

type dispatchBudgetProjection struct {
	CostUSD        float64
	EstimateSource budget.EstimateSource
}

type UsageUpdateHandler func(UsageUpdate) error

type AgentActivityUpdateHandler func(AgentActivityUpdate) error

type AgentActivityUpdate struct {
	At                time.Time
	DetentSessionID   int64
	ProviderSessionID string
	TurnID            string
	ItemID            string
	Type              AgentUpdateType
	Tool              string
	Content           string
	Status            string
	Model             string
	TotalTokens       int64
}

type UsageUpdate struct {
	LastCommand           string
	DetentSessionID       int64
	SessionID             string
	ProcessIdentity       string
	WorkerProcess         procgroup.Identity
	WorkspacePath         string
	TurnCount             int
	LastEventAt           time.Time
	LastEvent             string
	LastMessage           string
	LastMessageTruncation *runtimeoutput.Truncation
	RecentEvents          []telemetry.ActivityEvent
	RuntimeIdentity       agentidentity.Identity
	WorkerGitHubActor     connector.IssueActor
	WorkProductPushed     bool
	Tokens                TokenTotals
	DiffStats             DiffStats
	RateLimits            *telemetry.RateLimits
	RSSBytes              uint64
	RSSCeilingBytes       uint64
	RSSObservedAt         time.Time
	DispatchLoopStart     *DispatchLoopStartSnapshot
}

type DispatchLoopStartSnapshot struct {
	WorkspaceDiffAvailable bool
	WorkspaceHeadAvailable bool
	DiffStats              DiffStats
}

type BudgetRefusal struct {
	Issue            connector.Issue
	Code             string
	Message          string
	Comment          string
	CurrentSpendUSD  float64
	ProjectedCostUSD float64
	MaxUSD           *float64
	ResetAt          *time.Time
	RefusedAt        time.Time
}

type DiffStats struct {
	FilesChanged                   int
	AddedLines                     int
	RemovedLines                   int
	UnpushedCommits                int
	UnpushedCommitRefs             []string
	TrackedPaths                   []string
	UntrackedPaths                 []string
	CommitsNotInPullRequest        []string
	PullRequestComparisonAvailable bool
	RecoveryStateExpected          bool
	RecoveryStateAvailable         bool
	CommitsAhead                   int
	RemoteBranchExists             bool
	DeliveryStateChecked           bool
	HeadSHA                        string
	Fingerprint                    string
	Status                         string
}

type TokenTotals struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	Last                  *AgentTokenCounts
	ModelContextWindow    *int64
	RuntimeSeconds        float64
}

type PriorAttempt struct {
	Source                  string
	Reason                  string
	ExplainBeforeRetry      bool
	MissingSignal           string
	ObservedTokens          int64
	NoProgressTokenLimit    int64
	ObservedSpendUSD        float64
	NoProgressSpendLimitUSD float64
	Validator               gate.ValidatorResult
}

type FakeRunner struct{}

func (FakeRunner) Run(context.Context, RunRequest) (RunResult, error) {
	return RunResult{FinalState: FinalStateCompleted}, nil
}
