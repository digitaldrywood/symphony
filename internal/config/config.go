package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/activehours"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/intake"
	"github.com/digitaldrywood/detent/internal/pathsafe"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/publication"
	"github.com/digitaldrywood/detent/internal/retro"
	"github.com/digitaldrywood/detent/internal/scheduleowner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	commandshell "github.com/digitaldrywood/detent/internal/shell"
)

const (
	TrackerGitHub      = "github"
	TrackerLinear      = "linear"
	TrackerMemory      = "memory"
	TrackerLocalSQLite = "local_sqlite"
	TrackerGitHubLocal = "github_local"
	TrackerHubNative   = "hub_native"

	GitHubStatusSourceProjectV2  = "project_v2"
	GitHubStatusSourceIssueField = "issue_field"
	GitHubStatusSourceLabel      = "label"

	WorkspaceLocalGit      = "local_git"
	WorkspaceFilesystem    = "filesystem"
	WorkspaceCacheIsolated = "isolated"
	WorkspaceCacheShared   = "shared"

	DeliverablePullRequest  = "pull_request"
	DeliverableArtifact     = "artifact"
	MergeMethodSquash       = "squash"
	MergeMethodMerge        = "merge"
	MergeMethodRebase       = "rebase"
	BillingModeMetered      = "metered"
	BillingModeSubscription = "subscription"

	DependencyReadinessTerminal         = "terminal"
	DependencyReadinessTerminalOrMerged = "terminal_or_merged"
	DependencySourceMerged              = "merged"
	DependencySourceNativeOnly          = "native_only"

	defaultLinearEndpoint      = "https://api.linear.app/graphql"
	defaultGitHubEndpoint      = "https://api.github.com/graphql"
	defaultGitHubStatusPageURL = "https://www.githubstatus.com"
	defaultLinearStatusPageURL = "https://linearstatus.com"

	DefaultAgentBackendID                    = "codex"
	AgentBackendCodex                        = "codex"
	AgentBackendClaudeCode                   = "claude_code"
	DefaultKnowledgeMaxBytes                 = 64 * 1024
	DefaultReworkLimit                       = 3
	DefaultNoProgressLimit                   = 3
	DefaultNoProgressTokenLimit              = 25_000_000
	DefaultNoProgressSpendLimitUSD           = 3.0
	DefaultLifetimeSessionLimit              = 120
	DefaultLifetimeTokenLimit                = 750_000_000
	DefaultLifetimeLimitCooldownSeconds      = 3600
	DefaultLifetimeLimitOverrideLabel        = "allow-lifetime-limit"
	DefaultFailureBreakerSameClassLimit      = 5
	DefaultFailureBreakerWindowSeconds       = 3600
	DefaultFailureBreakerCooldownSeconds     = 3600
	DefaultAutoPromoteGateWaitTimeoutSeconds = 3600
	DefaultOverloadRetryDelayMS              = 45000
	DefaultMergeWorkerStartupTimeoutMS       = 4 * 60 * 1000
	DefaultMergeWorkerMaxDurationMS          = 6 * 60 * 60 * 1000
	DefaultMergeFallbackMaxDurationMS        = 20 * 60 * 1000
	DefaultMergeFairnessAgeSeconds           = 2 * 60 * 60
	DefaultMaxSessionDurationMS              = 2 * 60 * 60 * 1000
	DefaultNoProgressTimeoutMS               = 90 * 60 * 1000

	DefaultPollingIntervalMS               = 120000
	DefaultRefreshFailureThreshold         = 3
	MinPollingIntervalMS                   = 60000
	DefaultShutdownDrainTimeoutMS          = 75000
	DefaultBoardSnapshotStaleAfterSeconds  = 15 * 60
	DefaultStalenessNoCompletionHours      = 24
	DefaultStalenessNoMergeHours           = 12
	DefaultStalenessHumanGateRearmHours    = 168
	DefaultStalenessLaneReentryWindowHours = 168
	DefaultStalenessRepeatedCount          = 20
	DefaultStalenessRepeatedWindowHours    = 24
	DefaultStalenessWebhookTimeoutMS       = 5000
	DefaultStrandedActiveThresholdSeconds  = 10 * 60
	DefaultDispatchStallThresholdSeconds   = 2 * 60 * 60
	DefaultParkReviewThreshold             = 3
	DefaultGitHubUnstartedSeconds          = 15 * 60
	MaxStalenessRepeatedCount              = 500

	defaultCodexProtocol                      = "app-server"
	defaultClaudeCodeProtocol                 = "headless"
	defaultClaudeCodeCommand                  = "claude"
	defaultClaudeCodePermissionMode           = "bypassPermissions"
	claudeCodePermissionModeDefault           = "default"
	claudeCodePermissionModeAcceptEdits       = "acceptEdits"
	claudeCodePermissionModeBypassPermissions = "bypassPermissions"
	claudeCodePermissionModePlan              = "plan"
	claudeCodeEffortLow                       = "low"
	claudeCodeEffortMedium                    = "medium"
	claudeCodeEffortHigh                      = "high"
	claudeCodeEffortXHigh                     = "xhigh"
	claudeCodeEffortMax                       = "max"
	claudeCodeEffortUltracode                 = "ultracode"

	IdentityOwnershipAssignee = "assignee"
	IdentityOwnershipField    = "field"

	KanbanModeReadOnly    = "read_only"
	KanbanModeIntegration = "integration"

	AutoPromoteGateWaitStateSource = "source"
	AutoPromoteGateWaitStateReview = "review"

	AutoPromoteGateWaitTimeoutActionMerge       = "merge"
	AutoPromoteGateWaitTimeoutActionHumanReview = "human_review"
)

type Workflow struct {
	Config       Config
	Prompt       string
	SharedPrompt string
	AgentsPrompt string
	SourceHash   string
	Overlay      WorkflowOverlay
	Definition   ProjectDefinition
}

type WorkflowOverlay struct {
	Path           string
	OverriddenKeys []string
}

type Config struct {
	Policy            policy.Descriptor    `yaml:"-" json:"-"`
	Runners           Runners              `yaml:"runners,omitempty"`
	Identity          Identity             `yaml:"identity,omitempty"`
	ActiveHours       activehours.Config   `yaml:"active_hours,omitempty"`
	Tracker           Tracker              `yaml:"tracker"`
	Polling           Polling              `yaml:"polling"`
	Workspace         Workspace            `yaml:"workspace"`
	Workpad           Workpad              `yaml:"workpad,omitempty"`
	Deliverable       Deliverable          `yaml:"deliverable,omitempty"`
	Dependencies      Dependencies         `yaml:"dependencies,omitempty"`
	Worker            Worker               `yaml:"worker"`
	Agent             Agent                `yaml:"agent"`
	Agents            Agents               `yaml:"agents"`
	Codex             Codex                `yaml:"codex"`
	Gate              gate.Config          `yaml:"gate"`
	Plan              gate.PlanConfig      `yaml:"plan"`
	Server            Server               `yaml:"server"`
	Observability     Observability        `yaml:"observability"`
	Budget            Budget               `yaml:"budget"`
	Release           Release              `yaml:"release,omitempty"`
	Hooks             Hooks                `yaml:"hooks"`
	ScheduleOwnership scheduleowner.Config `yaml:"schedule_ownership,omitempty"`
	Intake            intake.Config        `yaml:"intake,omitempty"`
	Retro             retro.Config         `yaml:"retro,omitempty"`
	Routines          []Routine            `yaml:"routines,omitempty"`
	BacklogAdmission  BacklogAdmission     `yaml:"backlog_admission,omitempty"`

	configuredFields map[string]struct{}
}

type Tracker struct {
	Kind                        string                `yaml:"kind"`
	Endpoint                    string                `yaml:"endpoint"`
	APIKey                      string                `yaml:"api_key"`
	HTTPMaxIdleConns            int                   `yaml:"http_max_idle_conns"`
	HTTPMaxIdleConnsPerHost     int                   `yaml:"http_max_idle_conns_per_host"`
	HTTPIdleConnTimeoutMS       int                   `yaml:"http_idle_conn_timeout_ms"`
	GitHubGraphQLWarnRemaining  int                   `yaml:"github_graphql_warn_remaining"`
	GitHubGraphQLMinReserve     int                   `yaml:"github_graphql_min_remaining_reserve"`
	GitHubRESTMinReserve        int                   `yaml:"github_rest_min_remaining_reserve"`
	GitHubRESTFanoutMaxRequests int                   `yaml:"github_rest_fanout_max_requests"`
	GitHubRESTDebugLogging      bool                  `yaml:"github_rest_debug_logging,omitempty"`
	GitHubUnstartedSeconds      int                   `yaml:"github_unstarted_check_threshold_seconds"`
	GitHubAppID                 string                `yaml:"github_app_id"`
	GitHubAppPrivateKey         string                `yaml:"github_app_private_key"`
	GitHubAppPrivateKeyPath     string                `yaml:"github_app_private_key_path"`
	GitHubAppInstallationID     string                `yaml:"github_app_installation_id"`
	GitHubWebhookSecret         string                `yaml:"github_webhook_secret,omitempty"`
	GitHubStatusSource          string                `yaml:"github_status_source"`
	StatusPageURL               string                `yaml:"status_page_url,omitempty"`
	ProjectSlug                 string                `yaml:"project_slug"`
	Repository                  string                `yaml:"repository"`
	StatusField                 string                `yaml:"status_field"`
	StatusLabelPrefix           string                `yaml:"status_label_prefix,omitempty"`
	WriteProbeIssue             string                `yaml:"write_probe_issue,omitempty"`
	Assignee                    string                `yaml:"assignee"`
	ActiveStates                []string              `yaml:"active_states"`
	ObservedStates              []string              `yaml:"observed_states"`
	TerminalStates              []string              `yaml:"terminal_states"`
	StateMap                    StringOrMap           `yaml:"state_map"`
	PriorityMap                 StringOrMap           `yaml:"priority_map"`
	DependencyAutoUnblock       DependencyAutoUnblock `yaml:"dependency_auto_unblock"`
	BlockedRecovery             BlockedRecovery       `yaml:"blocked_recovery"`
	BlockerAutoPromote          BlockerAutoPromote    `yaml:"blocker_auto_promote"`
	AutoProvision               bool                  `yaml:"auto_provision"`
	Claims                      Claims                `yaml:"claims,omitempty"`
	Authorization               selector.Selector     `yaml:"authorization,omitempty"`
	LocalSQLite                 LocalSQLite           `yaml:"local_sqlite,omitempty"`
	Issues                      []connector.Issue     `yaml:"issues"`
	Publication                 publication.Policy    `yaml:"-"`

	gitHubStatusSourceSet bool
}

type LocalSQLite struct {
	Path      string `yaml:"path"`
	ProjectID string `yaml:"project_id,omitempty"`
}

type DependencyAutoUnblock struct {
	Enabled      bool     `yaml:"enabled"`
	SourceStates []string `yaml:"source_states"`
	TargetState  string   `yaml:"target_state"`
	Readiness    string   `yaml:"readiness"`
}

type BlockedRecovery struct {
	Enabled                bool     `yaml:"enabled"`
	SourceStates           []string `yaml:"source_states"`
	TargetState            string   `yaml:"target_state"`
	ReasonCodes            []string `yaml:"reason_codes"`
	BreakerCooldownSeconds int      `yaml:"breaker_cooldown_seconds"`
}

type Dependencies struct {
	Source string `yaml:"source"`
}

type BlockerAutoPromote struct {
	Enabled       bool     `yaml:"enabled"`
	SourceStates  []string `yaml:"source_states"`
	BlockerStates []string `yaml:"blocker_states"`
	TargetState   string   `yaml:"target_state"`
}

type Identity struct {
	Name             string `yaml:"name"`
	GitHubLogin      string `yaml:"github_login,omitempty"`
	OwnershipMode    string `yaml:"ownership_mode,omitempty"`
	AssigneeRequired bool   `yaml:"assignee_required,omitempty"`
	OwnerField       string `yaml:"owner_field,omitempty"`
}

type Polling struct {
	IntervalMS              int  `yaml:"interval_ms"`
	Conditional             bool `yaml:"conditional"`
	RefreshFailureThreshold int  `yaml:"refresh_failure_threshold"`
}

type Claims struct {
	Enabled          bool   `yaml:"enabled"`
	LeaseField       string `yaml:"lease_field,omitempty"`
	TTLSeconds       int    `yaml:"ttl_seconds,omitempty"`
	HeartbeatSeconds int    `yaml:"heartbeat_seconds,omitempty"`
}

type Workspace struct {
	Kind                   string `yaml:"kind"`
	Root                   string `yaml:"root"`
	SourceRoot             string `yaml:"source_root"`
	OutputRoot             string `yaml:"output_root"`
	CacheStrategy          string `yaml:"cache_strategy"`
	AutoBranch             bool   `yaml:"auto_branch"`
	CleanupIdleTTLMS       int    `yaml:"cleanup_idle_ttl_ms"`
	CleanupSweepIntervalMS int    `yaml:"cleanup_sweep_interval_ms"`
}

type Workpad struct {
	StructuredOnly bool `yaml:"structured_only"`
}

type Deliverable struct {
	Kind                  string `yaml:"kind"`
	MergeMethod           string `yaml:"merge_method,omitempty"`
	OutputRoot            string `yaml:"output_root,omitempty"`
	ReviewURL             string `yaml:"review_url,omitempty"`
	mergeMethodConfigured bool
}

type DeliverableElicitationRule struct {
	Server     string `yaml:"server"`
	Tool       string `yaml:"tool"`
	Repository string `yaml:"repository"`
}

type Worker struct {
	SSHHosts                       []string `yaml:"ssh_hosts"`
	MaxConcurrentAgentsPerHost     *int     `yaml:"max_concurrent_agents_per_host"`
	GitHubToken                    string   `yaml:"github_token,omitempty"`
	GitHubTokenResolutionTimeoutMS int      `yaml:"github_token_resolution_timeout_ms"`
	GitHubRESTMinReserve           int      `yaml:"github_rest_min_remaining_reserve"`
	GitHubRESTPollIntervalMS       int      `yaml:"github_rest_poll_interval_ms"`
}

type Agent struct {
	MaxConcurrentAgents          int                          `yaml:"max_concurrent_agents"`
	RateWindowPacing             RateWindowPacing             `yaml:"rate_window_pacing"`
	MaxTurns                     int                          `yaml:"max_turns"`
	MaxTurnDurationMS            int                          `yaml:"max_turn_duration_ms"`
	MaxSessionDurationMS         int                          `yaml:"max_session_duration_ms"`
	NoProgressTimeoutMS          int                          `yaml:"no_progress_timeout_ms"`
	MergeWorkerStartupTimeoutMS  int                          `yaml:"merge_worker_startup_timeout_ms"`
	MergeWorkerMaxDurationMS     int                          `yaml:"merge_worker_max_duration_ms"`
	MergeFallbackMaxDurationMS   int                          `yaml:"merge_fallback_max_duration_ms"`
	MaxRetryBackoffMS            int                          `yaml:"max_retry_backoff_ms"`
	OverloadRetryDelayMS         int                          `yaml:"overload_retry_delay_ms"`
	NoProgressTokenLimit         int64                        `yaml:"no_progress_token_limit"`
	NoProgressSpendLimitUSD      float64                      `yaml:"no_progress_spend_limit_usd"`
	LifetimeSessionLimit         int64                        `yaml:"lifetime_session_limit"`
	LifetimeTokenLimit           int64                        `yaml:"lifetime_token_limit"`
	LifetimeLimitCooldownSeconds int                          `yaml:"lifetime_limit_cooldown_seconds"`
	LifetimeLimitOverrideLabel   string                       `yaml:"lifetime_limit_override_label"`
	FailureBreaker               FailureBreaker               `yaml:"failure_breaker"`
	MaxSessionTokens             int64                        `yaml:"max_session_tokens"`
	MaxSessionContextMultiplier  float64                      `yaml:"max_session_context_multiplier"`
	MaxSessionTokenOverrideLabel string                       `yaml:"max_session_token_override_label"`
	MaxSessionTokenOverrideField string                       `yaml:"max_session_token_override_field"`
	ExperimentalThreadResume     bool                         `yaml:"experimental_thread_resume"`
	ResumeOrphanedSessions       bool                         `yaml:"resume_orphaned_sessions"`
	Effort                       AgentRoleEffort              `yaml:"effort"`
	Shutdown                     Shutdown                     `yaml:"shutdown"`
	StopRun                      StopRun                      `yaml:"stop_run,omitempty"`
	MaxConcurrentAgentsByState   map[string]int               `yaml:"max_concurrent_agents_by_state"`
	DispatchPriorityByState      []string                     `yaml:"dispatch_priority_by_state"`
	DispatchPriorityByLabel      []string                     `yaml:"dispatch_priority_by_label"`
	PrioritizeUnblockers         bool                         `yaml:"prioritize_unblockers"`
	MergeFastPath                MergeFastPath                `yaml:"merge_fast_path"`
	AutoPromote                  AutoPromote                  `yaml:"auto_promote"`
	OutputTruncation             OutputTruncation             `yaml:"output_truncation"`
	InstructionsByState          map[string]string            `yaml:"instructions_by_state,omitempty"`
	InstructionsByTransition     map[string]map[string]string `yaml:"instructions_by_transition,omitempty"`
	Budget                       Budget                       `yaml:"budget"`
	Lessons                      Lessons                      `yaml:"lessons"`
	Knowledge                    Knowledge                    `yaml:"knowledge"`
	Skills                       Skills                       `yaml:"skills"`
	Followups                    Followups                    `yaml:"followups"`
}

type StopRun struct {
	TargetState string `yaml:"target_state,omitempty"`
}

type FailureBreaker struct {
	SameClassLimit  int `yaml:"same_class_limit"`
	WindowSeconds   int `yaml:"window_seconds"`
	CooldownSeconds int `yaml:"cooldown_seconds"`
}

type AgentRoleEffort struct {
	Code   string `yaml:"code"`
	Rework string `yaml:"rework"`
	Merge  string `yaml:"merge"`
}

func (e AgentRoleEffort) Resolve(role string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "code":
		return strings.TrimSpace(e.Code), "agent.effort.code"
	case "rework":
		if effort := strings.TrimSpace(e.Rework); effort != "" {
			return effort, "agent.effort.rework"
		}
		return strings.TrimSpace(e.Code), "agent.effort.code"
	case "merge":
		return strings.TrimSpace(e.Merge), "agent.effort.merge"
	default:
		return "", ""
	}
}

type Shutdown struct {
	DrainTimeoutMS int `yaml:"drain_timeout_ms"`
}

type MergeFastPath struct {
	Enabled            bool `yaml:"enabled"`
	FairnessAgeSeconds int  `yaml:"fairness_age_seconds"`
}

type Agents struct {
	Backends       []AgentBackend    `yaml:"backends"`
	Routes         []AgentRoute      `yaml:"routes"`
	ModelSelection ModelSelection    `yaml:"model_selection"`
	Sources        map[string]string `yaml:"-"`
	local          *Agents
	pricingPath    string
}

type AgentBackend struct {
	Disabled bool           `yaml:"disabled,omitempty"`
	ID       string         `yaml:"id"`
	Kind     string         `yaml:"kind"`
	Protocol string         `yaml:"protocol"`
	Command  string         `yaml:"command"`
	Provider string         `yaml:"provider"`
	Options  BackendOptions `yaml:"options"`

	codexOptions         CodexOptions
	codexOptionsSet      bool
	claudeCodeOptions    ClaudeCodeOptions
	claudeCodeOptionsSet bool
	optionsProblem       string
}

type BackendOptions struct {
	node *yaml.Node
}

type CodexOptions struct {
	Shell                           string                       `yaml:"shell"`
	ModelProvider                   string                       `yaml:"model_provider"`
	ServiceTier                     string                       `yaml:"service_tier"`
	ApprovalPolicy                  StringOrMap                  `yaml:"approval_policy"`
	DeliverableElicitationAllowlist []DeliverableElicitationRule `yaml:"deliverable_elicitation_allowlist"`
	ThreadSandbox                   string                       `yaml:"thread_sandbox"`
	TurnSandboxPolicy               map[string]any               `yaml:"turn_sandbox_policy"`
	TurnTimeoutMS                   int                          `yaml:"turn_timeout_ms"`
	ReadTimeoutMS                   int                          `yaml:"read_timeout_ms"`
	StallTimeoutMS                  int                          `yaml:"stall_timeout_ms"`
}

type ClaudeCodeOptions struct {
	PermissionMode         string   `yaml:"permission_mode"`
	Effort                 string   `yaml:"effort"`
	AllowedTools           []string `yaml:"allowed_tools"`
	DisallowedTools        []string `yaml:"disallowed_tools"`
	IncludePartialMessages bool     `yaml:"include_partial_messages"`
	TurnTimeoutMS          int      `yaml:"turn_timeout_ms"`
	StallTimeoutMS         int      `yaml:"stall_timeout_ms"`
	Shell                  string   `yaml:"shell"`
	ExtraArgs              []string `yaml:"extra_args"`
}

type AgentRoute struct {
	Disabled   bool              `yaml:"disabled,omitempty"`
	Name       string            `yaml:"name"`
	Role       string            `yaml:"role"`
	Backend    string            `yaml:"backend"`
	Model      string            `yaml:"model"`
	ModelField string            `yaml:"model_field"`
	Default    bool              `yaml:"default"`
	Selector   selector.Selector `yaml:"selector"`
}

type AutoPromote struct {
	Enabled                bool     `yaml:"enabled"`
	QuietSeconds           int      `yaml:"quiet_seconds"`
	OptoutLabel            string   `yaml:"optout_label"`
	AllowedIssueLabels     []string `yaml:"allowed_issue_labels"`
	GateWaitState          string   `yaml:"gate_wait_state,omitempty"`
	GateWaitTimeoutSeconds int      `yaml:"gate_wait_timeout_seconds,omitempty"`
	GateWaitTimeoutAction  string   `yaml:"gate_wait_timeout_action,omitempty"`
	SourceState            string   `yaml:"source_state,omitempty"`
	PassState              string   `yaml:"pass_state,omitempty"`
	ReworkState            string   `yaml:"rework_state,omitempty"`
	ReworkLimit            int      `yaml:"rework_limit,omitempty"`
	NoProgressLimit        int      `yaml:"no_progress_limit,omitempty"`
}

type OutputTruncation struct {
	MaxBytes int `yaml:"max_bytes"`
}

type Lessons struct {
	Enabled             bool   `yaml:"enabled"`
	Path                string `yaml:"path"`
	MaxEntries          int    `yaml:"max_entries"`
	RecallN             int    `yaml:"recall_n"`
	PostmortemMaxTokens int    `yaml:"postmortem_max_tokens"`
}

type Knowledge struct {
	Enabled    bool              `yaml:"enabled"`
	MaxBytes   int               `yaml:"max_bytes"`
	Sources    []KnowledgeSource `yaml:"sources"`
	Configured bool              `yaml:"-"`
}

type KnowledgeSource struct {
	Name string `yaml:"name,omitempty"`
	Path string `yaml:"path"`
}

type Skills struct {
	Enabled           bool          `yaml:"enabled"`
	Path              string        `yaml:"path"`
	MaxSkillsInPrompt int           `yaml:"max_skills_in_prompt"`
	Creation          SkillCreation `yaml:"creation"`
}

type SkillCreation struct {
	Enabled         bool `yaml:"enabled"`
	MaxDraftsPerRun int  `yaml:"max_drafts_per_run"`
}

type Followups struct {
	Enabled bool `yaml:"enabled"`
}

type Budget struct {
	BillingMode                string  `yaml:"billing_mode,omitempty"`
	Enabled                    bool    `yaml:"enabled"`
	PerDayMaxUSD               float64 `yaml:"per_day_max_usd"`
	PerIssueMaxUSD             float64 `yaml:"per_issue_max_usd"`
	RefusalCooldownSeconds     int     `yaml:"refusal_cooldown_seconds"`
	PricingPath                string  `yaml:"pricing_path"`
	OverrideMaxDurationSeconds int     `yaml:"override_max_duration_seconds"`
	OverrideMaxMultiplier      float64 `yaml:"override_max_multiplier"`

	perDayMaxUSDConfigured   bool
	perIssueMaxUSDConfigured bool
}

type USDBrakes struct {
	BudgetCaps bool
	NoProgress bool
}

func (b Budget) EffectiveBillingMode() string {
	if strings.EqualFold(strings.TrimSpace(b.BillingMode), BillingModeMetered) {
		return BillingModeMetered
	}
	return BillingModeSubscription
}

func (b Budget) BillingModeConfigured() bool {
	return strings.TrimSpace(b.BillingMode) != ""
}

func (b Budget) USDEnforcementEnabled() bool {
	return b.Enabled && b.EffectiveBillingMode() == BillingModeMetered
}

func (c Config) USDBrakes() USDBrakes {
	metered := c.Budget.EffectiveBillingMode() == BillingModeMetered
	return USDBrakes{
		BudgetCaps: metered && c.Budget.Enabled,
		NoProgress: metered && c.Agent.NoProgressSpendLimitUSD > 0,
	}
}

func (c Config) ValidationWarnings() []string {
	var warnings []string
	if c.USDBrakes().NoProgress {
		warnings = append(warnings, "budget.billing_mode: metered with agent.no_progress_spend_limit_usd > 0 arms a notional USD progress brake; budget.enabled: false does not disarm it; use budget.billing_mode: subscription to make USD enforcement inert")
	}
	if token := strings.TrimSpace(c.Worker.GitHubToken); token != "" {
		if IsGitHubTokenSentinel(token) {
			warnings = append(warnings, "worker.github_token: ambient gh authentication is classified at launch; when it resolves to the orchestrator's GitHub principal it uses shared-budget mode, primary REST usage attribution is indeterminate, and workers brake before the orchestrator dispatch floor; use a different GitHub user or App installation for true rate-limit isolation")
		} else {
			warnings = append(warnings, "worker.github_token: credential principals are classified at launch; a token resolving to the orchestrator's GitHub principal uses shared-budget mode, primary REST usage attribution is indeterminate, and workers brake before the orchestrator dispatch floor; use a different GitHub user or App installation for true rate-limit isolation")
		}
	}
	return warnings
}

func IsGitHubTokenSentinel(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "gh", "gh-auth", "${gh auth token}", "$(gh auth token)":
		return true
	default:
		return false
	}
}

func (b *Budget) Normalize() {
	if b == nil {
		return
	}
	b.BillingMode = strings.ToLower(strings.TrimSpace(b.BillingMode))
}

func (b Budget) PerDayMaxUSDConfigured() bool {
	return b.perDayMaxUSDConfigured
}

func (b Budget) PerIssueMaxUSDConfigured() bool {
	return b.perIssueMaxUSDConfigured
}

type Codex struct {
	Command                         string                       `yaml:"command"`
	Shell                           string                       `yaml:"shell"`
	ModelProvider                   string                       `yaml:"model_provider"`
	ServiceTier                     string                       `yaml:"service_tier"`
	ApprovalPolicy                  StringOrMap                  `yaml:"approval_policy"`
	DeliverableElicitationAllowlist []DeliverableElicitationRule `yaml:"deliverable_elicitation_allowlist"`
	ThreadSandbox                   string                       `yaml:"thread_sandbox"`
	TurnSandboxPolicy               map[string]any               `yaml:"turn_sandbox_policy"`
	TurnTimeoutMS                   int                          `yaml:"turn_timeout_ms"`
	ReadTimeoutMS                   int                          `yaml:"read_timeout_ms"`
	StallTimeoutMS                  int                          `yaml:"stall_timeout_ms"`
}

func (c Config) AgentBackendConfigs() []AgentBackend {
	if len(c.Agents.Backends) > 0 {
		backends := make([]AgentBackend, len(c.Agents.Backends))
		copy(backends, c.Agents.Backends)
		for index := range backends {
			if backends[index].Kind == AgentBackendCodex {
				backends[index] = mergedCodexAgentBackend(c.Codex, backends[index])
			}
		}
		return backends
	}
	return []AgentBackend{CodexAgentBackend(c.Codex)}
}

func (c Config) AgentRouteConfigs() []AgentRoute {
	if len(c.Agents.Routes) > 0 {
		routes := make([]AgentRoute, len(c.Agents.Routes))
		copy(routes, c.Agents.Routes)
		return routes
	}

	backendID := DefaultAgentBackendID
	if backends := c.AgentBackendConfigs(); len(backends) > 0 {
		backendID = backends[0].ID
	}
	return []AgentRoute{{
		Name:    "default",
		Backend: backendID,
		Default: true,
	}}
}

func CodexAgentBackend(codex Codex) AgentBackend {
	options := CodexOptions{
		Shell:                           codex.Shell,
		ModelProvider:                   codex.ModelProvider,
		ServiceTier:                     codex.ServiceTier,
		ApprovalPolicy:                  codex.ApprovalPolicy,
		DeliverableElicitationAllowlist: append([]DeliverableElicitationRule(nil), codex.DeliverableElicitationAllowlist...),
		ThreadSandbox:                   codex.ThreadSandbox,
		TurnSandboxPolicy:               codex.TurnSandboxPolicy,
		TurnTimeoutMS:                   codex.TurnTimeoutMS,
		ReadTimeoutMS:                   codex.ReadTimeoutMS,
		StallTimeoutMS:                  codex.StallTimeoutMS,
	}
	options.normalize()
	return AgentBackend{
		ID:       DefaultAgentBackendID,
		Kind:     AgentBackendCodex,
		Protocol: defaultCodexProtocol,
		Command:  strings.TrimSpace(codex.Command),
		Provider: strings.TrimSpace(codex.ModelProvider),
		Options:  backendOptionsFrom(options),

		codexOptions:    options,
		codexOptionsSet: true,
	}
}

func mergedCodexAgentBackend(fallback Codex, backend AgentBackend) AgentBackend {
	cfg := fallback
	if strings.TrimSpace(backend.Command) != "" {
		cfg.Command = strings.TrimSpace(backend.Command)
	}
	options := backend.CodexOptions()
	if strings.TrimSpace(options.Shell) != "" {
		cfg.Shell = options.Shell
	}
	if strings.TrimSpace(options.ModelProvider) != "" {
		cfg.ModelProvider = options.ModelProvider
	}
	if strings.TrimSpace(options.ServiceTier) != "" {
		cfg.ServiceTier = options.ServiceTier
	}
	if options.ApprovalPolicy.IsString || options.ApprovalPolicy.IsMap {
		cfg.ApprovalPolicy = options.ApprovalPolicy
	}
	if options.DeliverableElicitationAllowlist != nil {
		cfg.DeliverableElicitationAllowlist = append([]DeliverableElicitationRule(nil), options.DeliverableElicitationAllowlist...)
	}
	if strings.TrimSpace(options.ThreadSandbox) != "" {
		cfg.ThreadSandbox = strings.TrimSpace(options.ThreadSandbox)
	}
	if options.TurnSandboxPolicy != nil {
		cfg.TurnSandboxPolicy = options.TurnSandboxPolicy
	}
	if options.TurnTimeoutMS > 0 {
		cfg.TurnTimeoutMS = options.TurnTimeoutMS
	}
	if options.ReadTimeoutMS > 0 {
		cfg.ReadTimeoutMS = options.ReadTimeoutMS
	}
	if options.StallTimeoutMS > 0 {
		cfg.StallTimeoutMS = options.StallTimeoutMS
	}
	effective := CodexAgentBackend(cfg)
	effective.ID = backend.ID
	effective.Kind = backend.Kind
	effective.Protocol = backend.Protocol
	effective.Provider = backend.Provider
	return effective
}

func (b AgentBackend) CodexOptions() CodexOptions {
	options, err := b.decodedCodexOptions()
	if err != nil {
		return CodexOptions{}
	}
	return options
}

func (b AgentBackend) decodedCodexOptions() (CodexOptions, error) {
	if b.optionsProblem != "" {
		return CodexOptions{}, errors.New(b.optionsProblem)
	}
	if b.codexOptionsSet {
		return b.codexOptions, nil
	}

	var options CodexOptions
	if err := b.Options.Decode(&options); err != nil {
		return CodexOptions{}, err
	}
	options.normalize()
	return options, nil
}

func (b AgentBackend) ClaudeCodeOptions() ClaudeCodeOptions {
	options, err := b.decodedClaudeCodeOptions()
	if err != nil {
		return ClaudeCodeOptions{}
	}
	return options
}

func (b AgentBackend) decodedClaudeCodeOptions() (ClaudeCodeOptions, error) {
	if b.optionsProblem != "" {
		return ClaudeCodeOptions{}, errors.New(b.optionsProblem)
	}
	if b.claudeCodeOptionsSet {
		return b.claudeCodeOptions, nil
	}

	var options ClaudeCodeOptions
	if err := b.Options.Decode(&options); err != nil {
		return ClaudeCodeOptions{}, err
	}
	options.normalize()
	return options, nil
}

func (b *AgentBackend) decodeOptions() {
	b.codexOptions = CodexOptions{}
	b.codexOptionsSet = false
	b.claudeCodeOptions = ClaudeCodeOptions{}
	b.claudeCodeOptionsSet = false
	b.optionsProblem = ""

	switch b.Kind {
	case AgentBackendCodex:
		options, err := b.decodedCodexOptions()
		if err != nil {
			b.optionsProblem = err.Error()
			return
		}
		b.codexOptions = options
		b.codexOptionsSet = true
	case AgentBackendClaudeCode:
		options, err := b.decodedClaudeCodeOptions()
		if err != nil {
			b.optionsProblem = err.Error()
			return
		}
		b.claudeCodeOptions = options
		b.claudeCodeOptionsSet = true
	}
}

type Server struct {
	Port                           *int   `yaml:"port"`
	Host                           string `yaml:"host"`
	BoardSnapshotStaleAfterSeconds int    `yaml:"board_snapshot_stale_after_seconds"`
	Kanban                         Kanban `yaml:"kanban,omitempty"`
}

type Kanban struct {
	Mode               string              `yaml:"mode,omitempty"`
	IssueStateFieldID  int                 `yaml:"issue_state_field_id,omitempty"`
	AllowedTransitions map[string][]string `yaml:"allowed_transitions,omitempty"`
	ShowBlockedAlerts  bool                `yaml:"show_blocked_alerts,omitempty"`
}

type Observability struct {
	DashboardEnabled               bool                    `yaml:"dashboard_enabled"`
	RefreshMS                      int                     `yaml:"refresh_ms"`
	RenderIntervalMS               int                     `yaml:"render_interval_ms"`
	StrandedActiveThresholdSeconds int                     `yaml:"stranded_active_threshold_seconds"`
	DispatchStallThresholdSeconds  int                     `yaml:"dispatch_stall_threshold_seconds"`
	ParkReviewThreshold            int                     `yaml:"park_review_threshold"`
	Efficiency                     EfficiencyObservability `yaml:"efficiency,omitempty"`
	OTLP                           OTLPObservability       `yaml:"otlp,omitempty"`
	Staleness                      StalenessObservability  `yaml:"staleness,omitempty"`
}

type EfficiencyObservability struct {
	AnomalyTokensMultiple   float64 `yaml:"anomaly_tokens_multiple,omitempty"`
	AnomalySessionsMultiple float64 `yaml:"anomaly_sessions_multiple,omitempty"`
	AnomalyDwellMultiple    float64 `yaml:"anomaly_dwell_multiple,omitempty"`
}

type OTLPObservability struct {
	Endpoint    string            `yaml:"endpoint,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty"`
	ServiceName string            `yaml:"service_name,omitempty"`
	TimeoutMS   int               `yaml:"timeout_ms,omitempty"`
}

type StalenessObservability struct {
	Enabled                       bool                     `yaml:"enabled"`
	Lanes                         []StalenessLaneThreshold `yaml:"lanes,omitempty"`
	HumanGateRearmHours           int                      `yaml:"human_gate_rearm_hours"`
	LaneReentryWindowHours        int                      `yaml:"lane_reentry_window_hours"`
	NoCompletionHours             int                      `yaml:"no_completion_hours"`
	NoMergeHours                  int                      `yaml:"no_merge_hours"`
	RepeatedDecisionCount         int                      `yaml:"repeated_decision_count"`
	RepeatedDecisionBenignReasons []string                 `yaml:"repeated_decision_benign_reasons"`
	RepeatedWindowHours           int                      `yaml:"repeated_window_hours"`
	Webhook                       StalenessWebhook         `yaml:"webhook,omitempty"`
}

type StalenessLaneThreshold struct {
	State          string `yaml:"state"`
	ThresholdHours int    `yaml:"threshold_hours"`
	HumanGate      bool   `yaml:"human_gate,omitempty"`
}

type StalenessWebhook struct {
	URL       string            `yaml:"url,omitempty"`
	Headers   map[string]string `yaml:"headers,omitempty"`
	TimeoutMS int               `yaml:"timeout_ms"`
}

type Release struct {
	Enabled         bool     `yaml:"enabled"`
	MinMergedIssues int      `yaml:"min_merged_issues"`
	MaxAgeHours     int      `yaml:"max_age_hours"`
	RequireGreenCI  bool     `yaml:"require_green_ci"`
	VersionBump     string   `yaml:"version_bump"`
	RerunFlakyOnce  bool     `yaml:"rerun_flaky_once,omitempty"`
	FlakyCheckNames []string `yaml:"flaky_check_names,omitempty"`
}

func (r *Release) normalize() {
	if r == nil {
		return
	}
	if r.MinMergedIssues == 0 {
		r.MinMergedIssues = 5
	}
	if r.MaxAgeHours == 0 {
		r.MaxAgeHours = 24
	}
	if strings.TrimSpace(r.VersionBump) == "" {
		r.VersionBump = "auto"
	}
	r.VersionBump = strings.ToLower(strings.TrimSpace(r.VersionBump))
	seen := make(map[string]struct{}, len(r.FlakyCheckNames))
	names := make([]string, 0, len(r.FlakyCheckNames))
	for _, name := range r.FlakyCheckNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, name)
	}
	r.FlakyCheckNames = names
}

func (r Release) validate(prefix string, problems *[]string) {
	r.normalize()
	if !r.Enabled {
		return
	}
	if r.MinMergedIssues <= 0 {
		*problems = append(*problems, prefix+".min_merged_issues must be greater than 0 when "+prefix+".enabled is true")
	}
	if r.MaxAgeHours <= 0 {
		*problems = append(*problems, prefix+".max_age_hours must be greater than 0 when "+prefix+".enabled is true")
	}
	if !r.RequireGreenCI {
		*problems = append(*problems, prefix+".require_green_ci must be true when "+prefix+".enabled is true")
	}
	if r.VersionBump != "auto" {
		*problems = append(*problems, prefix+".version_bump must be auto")
	}
	if r.RerunFlakyOnce && len(r.FlakyCheckNames) == 0 {
		*problems = append(*problems, prefix+".flaky_check_names must not be empty when "+prefix+".rerun_flaky_once is true")
	}
}

type Hooks struct {
	Shell        string `yaml:"shell"`
	AfterCreate  string `yaml:"after_create"`
	BeforeRun    string `yaml:"before_run"`
	AfterRun     string `yaml:"after_run"`
	BeforeRemove string `yaml:"before_remove"`
	TimeoutMS    int    `yaml:"timeout_ms"`
}

func (i Identity) Configured() bool {
	return strings.TrimSpace(i.Name) != "" ||
		strings.TrimSpace(i.GitHubLogin) != "" ||
		strings.TrimSpace(i.OwnershipMode) != "" ||
		i.AssigneeRequired ||
		strings.TrimSpace(i.OwnerField) != ""
}

func (i Identity) IsZero() bool {
	return !i.Configured()
}

func (i *Identity) Normalize() {
	if i == nil {
		return
	}
	i.Name = strings.TrimSpace(i.Name)
	i.GitHubLogin = strings.TrimSpace(i.GitHubLogin)
	i.OwnershipMode = strings.ToLower(strings.TrimSpace(i.OwnershipMode))
	i.OwnerField = strings.TrimSpace(i.OwnerField)
	if i.OwnershipMode == "" && i.Configured() {
		i.OwnershipMode = IdentityOwnershipAssignee
	}
}

func (i Identity) Validate(prefix string) []string {
	if !i.Configured() {
		return nil
	}

	identity := i
	identity.Normalize()

	var problems []string
	if identity.Name == "" {
		problems = append(problems, prefix+".name must not be blank")
	}
	switch identity.OwnershipMode {
	case IdentityOwnershipAssignee:
		if identity.OwnerField != "" {
			problems = append(problems, prefix+".owner_field must be blank when "+prefix+".ownership_mode is assignee")
		}
	case IdentityOwnershipField:
		if identity.OwnerField == "" {
			problems = append(problems, prefix+".owner_field is required when "+prefix+".ownership_mode is field")
		}
	default:
		problems = append(problems, prefix+".ownership_mode must be one of assignee, field")
	}
	return problems
}

func (c *Claims) Normalize() {
	if c == nil {
		return
	}
	c.LeaseField = strings.TrimSpace(c.LeaseField)
}

func (c Claims) Validate(prefix string) []string {
	if !c.Enabled {
		return nil
	}

	var problems []string
	if strings.TrimSpace(c.LeaseField) == "" {
		problems = append(problems, prefix+".lease_field must not be blank when "+prefix+".enabled is true")
	}
	if c.TTLSeconds <= 0 {
		problems = append(problems, prefix+".ttl_seconds must be greater than 0 when "+prefix+".enabled is true")
	}
	if c.HeartbeatSeconds <= 0 {
		problems = append(problems, prefix+".heartbeat_seconds must be greater than 0 when "+prefix+".enabled is true")
	}
	if c.TTLSeconds > 0 && c.HeartbeatSeconds > c.TTLSeconds {
		problems = append(problems, prefix+".heartbeat_seconds must be less than or equal to "+prefix+".ttl_seconds")
	}
	return problems
}

func (d *DependencyAutoUnblock) Normalize() {
	if d == nil {
		return
	}
	d.SourceStates = normalizeStateList(d.SourceStates)
	d.TargetState = strings.TrimSpace(d.TargetState)
	d.Readiness = strings.ToLower(strings.TrimSpace(d.Readiness))
	if d.Readiness == "" {
		d.Readiness = DependencyReadinessTerminalOrMerged
	}
}

func (d *Dependencies) Normalize() {
	if d == nil {
		return
	}
	d.Source = strings.ToLower(strings.TrimSpace(d.Source))
	if d.Source == "" {
		d.Source = DependencySourceMerged
	}
}

func (d Dependencies) Validate(prefix string) []string {
	switch strings.ToLower(strings.TrimSpace(d.Source)) {
	case "", DependencySourceMerged, DependencySourceNativeOnly:
		return nil
	default:
		return []string{prefix + ".source must be one of merged, native_only"}
	}
}

func (d DependencyAutoUnblock) Validate(prefix string) []string {
	d.Normalize()

	var problems []string
	validateStateList(prefix+".source_states", d.SourceStates, &problems)
	if d.Enabled {
		if len(d.SourceStates) == 0 {
			problems = append(problems, prefix+".source_states must not be empty when "+prefix+".enabled is true")
		}
		if strings.TrimSpace(d.TargetState) == "" {
			problems = append(problems, prefix+".target_state is required when "+prefix+".enabled is true")
		}
	}
	if d.Readiness != "" {
		switch d.Readiness {
		case DependencyReadinessTerminal, DependencyReadinessTerminalOrMerged:
		default:
			problems = append(problems, prefix+".readiness must be one of terminal, terminal_or_merged")
		}
	}
	return problems
}

func (b *BlockedRecovery) Normalize() {
	if b == nil {
		return
	}
	b.SourceStates = normalizeStateList(b.SourceStates)
	b.TargetState = strings.TrimSpace(b.TargetState)
	if b.BreakerCooldownSeconds == 0 {
		b.BreakerCooldownSeconds = 24 * 60 * 60
	}
	for index := range b.ReasonCodes {
		b.ReasonCodes[index] = normalizeBlockedRecoveryReasonCode(b.ReasonCodes[index])
	}
}

func (b BlockedRecovery) Validate(prefix string) []string {
	breakerCooldownSeconds := b.BreakerCooldownSeconds
	b.ReasonCodes = slices.Clone(b.ReasonCodes)
	b.Normalize()

	var problems []string
	if breakerCooldownSeconds < 0 {
		problems = append(problems, prefix+".breaker_cooldown_seconds must be greater than or equal to 0")
	}
	validateStateList(prefix+".source_states", b.SourceStates, &problems)
	if b.Enabled {
		if len(b.SourceStates) == 0 {
			problems = append(problems, prefix+".source_states must not be empty when "+prefix+".enabled is true")
		}
		if strings.TrimSpace(b.TargetState) == "" {
			problems = append(problems, prefix+".target_state is required when "+prefix+".enabled is true")
		}
		if len(b.ReasonCodes) == 0 {
			problems = append(problems, prefix+".reason_codes must not be empty when "+prefix+".enabled is true")
		}
	}
	seen := map[string]struct{}{}
	for _, reason := range b.ReasonCodes {
		switch reason {
		case "merge_conflict", "stale_base", "missing_current_head_ci":
		default:
			problems = append(problems, prefix+".reason_codes must contain only merge_conflict, stale_base, missing_current_head_ci")
			continue
		}
		if _, ok := seen[reason]; ok {
			problems = append(problems, prefix+".reason_codes must be unique")
			continue
		}
		seen[reason] = struct{}{}
	}
	return problems
}

func normalizeBlockedRecoveryReasonCode(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	reason = strings.ReplaceAll(reason, "-", "_")
	reason = strings.ReplaceAll(reason, " ", "_")
	if reason == "merge_conflicts" {
		return "merge_conflict"
	}
	return reason
}

func (b *BlockerAutoPromote) Normalize() {
	if b == nil {
		return
	}
	b.SourceStates = normalizeStateList(b.SourceStates)
	b.BlockerStates = normalizeStateList(b.BlockerStates)
	b.TargetState = strings.TrimSpace(b.TargetState)
}

func (b BlockerAutoPromote) Validate(prefix string) []string {
	b.Normalize()

	var problems []string
	validateStateList(prefix+".source_states", b.SourceStates, &problems)
	validateStateList(prefix+".blocker_states", b.BlockerStates, &problems)
	if b.Enabled && strings.TrimSpace(b.TargetState) == "" {
		problems = append(problems, prefix+".target_state is required when "+prefix+".enabled is true")
	}
	return problems
}

type StringOrMap struct {
	IsString bool
	String   string
	IsMap    bool
	Map      map[string]any
}

type ValidationError struct {
	Problems []string
}

func LoadWorkflow(path string) (Workflow, error) {
	return LoadProjectDefinition(path)
}

func ParseWorkflow(raw []byte) (Workflow, error) {
	root, prompt, err := parseWorkflowDocument(raw)
	if err != nil {
		return Workflow{}, err
	}
	cfg, err := decodeWorkflowConfig(root)
	if err != nil {
		return Workflow{}, err
	}

	sum := sha256.Sum256(raw)
	return Workflow{
		Config:       cfg,
		Prompt:       string(prompt),
		SharedPrompt: string(prompt),
		SourceHash:   hex.EncodeToString(sum[:]),
	}, nil
}

func LocalWorkflowPath(path string) string {
	extension := filepath.Ext(path)
	if extension == "" {
		return path + ".local"
	}
	return strings.TrimSuffix(path, extension) + ".local" + extension
}

func ParseWorkflowOverlay(sharedRaw []byte, localRaw []byte, localPath string) (Workflow, error) {
	sharedRoot, sharedPrompt, err := parseWorkflowDocument(sharedRaw)
	if err != nil {
		return Workflow{}, err
	}
	localRoot, localPrompt, err := parseWorkflowDocument(localRaw)
	if err != nil {
		return Workflow{}, fmt.Errorf("parse local workflow overlay: %w", err)
	}

	if sharedRoot == nil {
		sharedRoot = &yaml.Node{Kind: yaml.MappingNode}
	}
	if localRoot != nil {
		mergeWorkflowMappings(sharedRoot, localRoot)
	}
	cfg, err := decodeWorkflowConfig(sharedRoot)
	if err != nil {
		return Workflow{}, err
	}

	overridden := sortedConfiguredFieldPaths(localRoot)
	combined := make([]byte, 0, len(sharedRaw)+len(localRaw)+len("\x00workflow.local.md\x00"))
	combined = append(combined, sharedRaw...)
	combined = append(combined, "\x00workflow.local.md\x00"...)
	combined = append(combined, localRaw...)
	sum := sha256.Sum256(combined)
	return Workflow{
		Config:       cfg,
		Prompt:       mergeWorkflowPrompts(sharedPrompt, localPrompt),
		SharedPrompt: string(sharedPrompt),
		SourceHash:   hex.EncodeToString(sum[:]),
		Overlay: WorkflowOverlay{
			Path:           strings.TrimSpace(localPath),
			OverriddenKeys: overridden,
		},
	}, nil
}

func parseWorkflowDocument(raw []byte) (*yaml.Node, []byte, error) {
	frontmatter, prompt, err := splitFrontmatter(raw)
	if err != nil {
		return nil, nil, err
	}
	if len(bytes.TrimSpace(frontmatter)) == 0 {
		return nil, prompt, nil
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(frontmatter, &doc); err != nil {
		return nil, nil, fmt.Errorf("parse YAML frontmatter: %w", err)
	}
	root, err := documentRoot(&doc)
	if err != nil {
		return nil, nil, err
	}
	if root.Kind != yaml.MappingNode {
		return nil, nil, errors.New("workflow frontmatter must be a mapping")
	}
	return root, prompt, nil
}

func decodeWorkflowConfig(root *yaml.Node) (Config, error) {
	cfg := Default()
	if root != nil {
		normalizeTrackerIDFields(root)
		gitHubStatusSourceSet := trackerFieldSet(root, "github_status_source")
		knowledgeConfigured := nestedFieldSet(root, "agent", "knowledge")
		mergeMethodConfigured := nestedFieldSet(root, "deliverable", "merge_method")
		perDayMaxUSDConfigured := nestedFieldSet(root, "budget", "per_day_max_usd")
		perIssueMaxUSDConfigured := nestedFieldSet(root, "budget", "per_issue_max_usd")
		if err := root.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("decode YAML frontmatter: %w", err)
		}
		cfg.Tracker.gitHubStatusSourceSet = gitHubStatusSourceSet
		cfg.Agent.Knowledge.Configured = knowledgeConfigured
		cfg.Deliverable.mergeMethodConfigured = mergeMethodConfigured
		cfg.Budget.perDayMaxUSDConfigured = perDayMaxUSDConfigured
		cfg.Budget.perIssueMaxUSDConfigured = perIssueMaxUSDConfigured
		cfg.configuredFields = configuredFieldPaths(root)
	}
	cfg.normalize()
	return cfg, nil
}

func mergeWorkflowMappings(shared *yaml.Node, local *yaml.Node) {
	for index := 0; index+1 < len(local.Content); index += 2 {
		localKey := local.Content[index]
		localValue := local.Content[index+1]
		sharedIndex := mappingKeyIndex(shared, localKey.Value)
		if sharedIndex < 0 {
			shared.Content = append(shared.Content, localKey, localValue)
			continue
		}
		sharedValue := shared.Content[sharedIndex+1]
		if sharedValue.Kind == yaml.MappingNode && localValue.Kind == yaml.MappingNode {
			mergeWorkflowMappings(sharedValue, localValue)
			continue
		}
		shared.Content[sharedIndex] = localKey
		shared.Content[sharedIndex+1] = localValue
	}
}

func mappingKeyIndex(node *yaml.Node, key string) int {
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return index
		}
	}
	return -1
}

func sortedConfiguredFieldPaths(root *yaml.Node) []string {
	if root == nil {
		return nil
	}
	paths := configuredFieldPaths(root)
	configured := make([]string, 0, len(paths))
	for path := range paths {
		configured = append(configured, path)
	}
	sort.Strings(configured)
	return configured
}

func mergeWorkflowPrompts(shared []byte, local []byte) string {
	if len(bytes.TrimSpace(local)) == 0 {
		return string(shared)
	}
	sharedPrompt := strings.TrimRight(string(shared), "\n")
	localPrompt := strings.TrimLeft(string(local), "\n")
	if sharedPrompt == "" {
		return "## Machine-local workflow overlay\n\n" + localPrompt
	}
	return sharedPrompt + "\n\n---\n\n## Machine-local workflow overlay\n\n" + localPrompt
}

func (c Config) ConfiguredSubsettings(prefix string) []string {
	prefix = strings.Trim(strings.TrimSpace(prefix), ".")
	if prefix == "" {
		return nil
	}
	configured := make([]string, 0)
	for path := range c.configuredFields {
		if path == prefix+".enabled" || !strings.HasPrefix(path, prefix+".") {
			continue
		}
		configured = append(configured, path)
	}
	sort.Strings(configured)
	return configured
}

func Default() Config {
	budget := defaultBudget()

	return Config{
		Tracker: Tracker{
			Endpoint:                    defaultLinearEndpoint,
			HTTPMaxIdleConns:            100,
			HTTPMaxIdleConnsPerHost:     32,
			HTTPIdleConnTimeoutMS:       90000,
			GitHubGraphQLWarnRemaining:  500,
			GitHubGraphQLMinReserve:     1000,
			GitHubRESTMinReserve:        1000,
			GitHubRESTFanoutMaxRequests: 80,
			GitHubUnstartedSeconds:      DefaultGitHubUnstartedSeconds,
			GitHubStatusSource:          GitHubStatusSourceProjectV2,
			StatusField:                 "Status",
			StatusLabelPrefix:           "detent:",
			ActiveStates:                []string{"Todo", "In Progress"},
			ObservedStates:              []string{"Backlog", "Human Review", "Blocked"},
			TerminalStates:              []string{"Closed", "Cancelled", "Canceled", "Duplicate", "Done"},
			StateMap:                    MapValue(map[string]any{}),
			PriorityMap:                 MapValue(defaultPriorityMap()),
			DependencyAutoUnblock: DependencyAutoUnblock{
				SourceStates: []string{"Blocked"},
				TargetState:  "Todo",
				Readiness:    DependencyReadinessTerminalOrMerged,
			},
			BlockedRecovery: BlockedRecovery{
				SourceStates:           []string{"Blocked"},
				TargetState:            "Rework",
				ReasonCodes:            []string{"merge_conflict", "stale_base", "missing_current_head_ci"},
				BreakerCooldownSeconds: 24 * 60 * 60,
			},
			BlockerAutoPromote: BlockerAutoPromote{
				BlockerStates: []string{"Backlog", "Blocked", "Human Review"},
				TargetState:   "Todo",
			},
			AutoProvision: true,
		},
		Polling: Polling{
			IntervalMS:              DefaultPollingIntervalMS,
			Conditional:             true,
			RefreshFailureThreshold: DefaultRefreshFailureThreshold,
		},
		Workspace: Workspace{
			Kind:                   WorkspaceLocalGit,
			Root:                   filepath.Join(os.TempDir(), "detent_workspaces"),
			CacheStrategy:          WorkspaceCacheIsolated,
			AutoBranch:             true,
			CleanupIdleTTLMS:       86400000,
			CleanupSweepIntervalMS: 600000,
		},
		Deliverable: Deliverable{
			Kind:        DeliverablePullRequest,
			MergeMethod: MergeMethodSquash,
		},
		Dependencies: Dependencies{
			Source: DependencySourceMerged,
		},
		Worker: Worker{
			SSHHosts:                       []string{},
			GitHubTokenResolutionTimeoutMS: 15000,
			GitHubRESTMinReserve:           1250,
			GitHubRESTPollIntervalMS:       60000,
		},
		Agent: Agent{
			MaxConcurrentAgents:          10,
			RateWindowPacing:             DefaultRateWindowPacing(),
			MaxTurns:                     20,
			MaxSessionDurationMS:         DefaultMaxSessionDurationMS,
			NoProgressTimeoutMS:          DefaultNoProgressTimeoutMS,
			MergeWorkerStartupTimeoutMS:  DefaultMergeWorkerStartupTimeoutMS,
			MergeWorkerMaxDurationMS:     DefaultMergeWorkerMaxDurationMS,
			MergeFallbackMaxDurationMS:   DefaultMergeFallbackMaxDurationMS,
			MaxRetryBackoffMS:            300000,
			OverloadRetryDelayMS:         DefaultOverloadRetryDelayMS,
			NoProgressTokenLimit:         DefaultNoProgressTokenLimit,
			NoProgressSpendLimitUSD:      DefaultNoProgressSpendLimitUSD,
			LifetimeSessionLimit:         DefaultLifetimeSessionLimit,
			LifetimeTokenLimit:           DefaultLifetimeTokenLimit,
			LifetimeLimitCooldownSeconds: DefaultLifetimeLimitCooldownSeconds,
			LifetimeLimitOverrideLabel:   DefaultLifetimeLimitOverrideLabel,
			StopRun:                      StopRun{TargetState: "Blocked"},
			MergeFastPath: MergeFastPath{
				Enabled:            true,
				FairnessAgeSeconds: DefaultMergeFairnessAgeSeconds,
			},
			FailureBreaker: FailureBreaker{
				SameClassLimit:  DefaultFailureBreakerSameClassLimit,
				WindowSeconds:   DefaultFailureBreakerWindowSeconds,
				CooldownSeconds: DefaultFailureBreakerCooldownSeconds,
			},
			ExperimentalThreadResume:   true,
			ResumeOrphanedSessions:     true,
			Shutdown:                   Shutdown{DrainTimeoutMS: DefaultShutdownDrainTimeoutMS},
			MaxConcurrentAgentsByState: map[string]int{},
			DispatchPriorityByState:    []string{},
			DispatchPriorityByLabel:    []string{},
			PrioritizeUnblockers:       true,
			AutoPromote: AutoPromote{
				QuietSeconds:           600,
				OptoutLabel:            "requires-human-review",
				AllowedIssueLabels:     []string{},
				GateWaitState:          AutoPromoteGateWaitStateSource,
				GateWaitTimeoutSeconds: DefaultAutoPromoteGateWaitTimeoutSeconds,
				SourceState:            "Human Review",
				PassState:              "Merging",
				ReworkState:            "Rework",
				ReworkLimit:            DefaultReworkLimit,
				NoProgressLimit:        DefaultNoProgressLimit,
			},
			Budget:    budget,
			Lessons:   defaultLessons(),
			Knowledge: defaultKnowledge(),
			Skills:    defaultSkills(),
			Followups: Followups{Enabled: true},
		},
		Codex: Codex{
			Command: "codex app-server",
			Shell:   commandshell.Default(),
			ApprovalPolicy: MapValue(map[string]any{
				"reject": map[string]any{
					"sandbox_approval": true,
					"rules":            true,
					"mcp_elicitations": true,
				},
			}),
			ThreadSandbox:  "workspace-write",
			TurnTimeoutMS:  3600000,
			ReadTimeoutMS:  5000,
			StallTimeoutMS: 300000,
		},
		Gate: gate.DefaultConfig(),
		Plan: gate.DefaultPlanConfig(),
		Server: Server{
			Host:                           "127.0.0.1",
			BoardSnapshotStaleAfterSeconds: DefaultBoardSnapshotStaleAfterSeconds,
			Kanban:                         Kanban{Mode: KanbanModeReadOnly},
		},
		Observability: Observability{
			DashboardEnabled:               true,
			RefreshMS:                      1000,
			RenderIntervalMS:               16,
			StrandedActiveThresholdSeconds: DefaultStrandedActiveThresholdSeconds,
			DispatchStallThresholdSeconds:  DefaultDispatchStallThresholdSeconds,
			ParkReviewThreshold:            DefaultParkReviewThreshold,
			Efficiency: EfficiencyObservability{
				AnomalyTokensMultiple:   3,
				AnomalySessionsMultiple: 3,
				AnomalyDwellMultiple:    3,
			},
			OTLP: OTLPObservability{TimeoutMS: 5000, ServiceName: "detent"},
			Staleness: StalenessObservability{
				Enabled:                       true,
				HumanGateRearmHours:           DefaultStalenessHumanGateRearmHours,
				LaneReentryWindowHours:        DefaultStalenessLaneReentryWindowHours,
				NoCompletionHours:             DefaultStalenessNoCompletionHours,
				NoMergeHours:                  DefaultStalenessNoMergeHours,
				RepeatedDecisionCount:         DefaultStalenessRepeatedCount,
				RepeatedDecisionBenignReasons: defaultStalenessRepeatedDecisionBenignReasons(),
				RepeatedWindowHours:           DefaultStalenessRepeatedWindowHours,
				Lanes: []StalenessLaneThreshold{
					{State: "Human Review", ThresholdHours: 72, HumanGate: true},
					{State: "Blocked", ThresholdHours: 48},
					{State: "Merging", ThresholdHours: 2},
				},
				Webhook: StalenessWebhook{TimeoutMS: DefaultStalenessWebhookTimeoutMS},
			},
		},
		Release: Release{
			MinMergedIssues: 5,
			MaxAgeHours:     24,
			RequireGreenCI:  true,
			VersionBump:     "auto",
		},
		Budget: budget,
		ScheduleOwnership: scheduleowner.Config{
			Backend:             scheduleowner.BackendGitHubRef,
			Branch:              scheduleowner.DefaultBranch,
			LeaseSeconds:        scheduleowner.DefaultLeaseSeconds,
			HeartbeatSeconds:    scheduleowner.DefaultHeartbeatSeconds,
			RetrySeconds:        scheduleowner.DefaultRetrySeconds,
			MaxClockSkewSeconds: scheduleowner.DefaultMaxClockSkewSeconds,
		},
		BacklogAdmission: BacklogAdmission{
			Schedule:               DefaultBacklogAdmissionSchedule,
			EffortFile:             BacklogAdmissionEffortFileWorkflow,
			MaxCandidatesPerRun:    DefaultBacklogAdmissionMaxCandidatesPerRun,
			MaxProposalsPerRun:     DefaultBacklogAdmissionMaxProposalsPerRun,
			MaxOpenProposals:       DefaultBacklogAdmissionMaxOpenProposals,
			ProposalExpiryDays:     DefaultBacklogAdmissionProposalExpiryDays,
			AutoAdmitMinConfidence: DefaultBacklogAdmissionAutoAdmitMinConfidence,
		},
		Hooks: Hooks{
			Shell:     commandshell.Default(),
			TimeoutMS: 60000,
		},
	}
}

func MapValue(value map[string]any) StringOrMap {
	return StringOrMap{
		IsMap: true,
		Map:   value,
	}
}

func StringValue(value string) StringOrMap {
	return StringOrMap{
		IsString: true,
		String:   value,
	}
}

func (c *Config) Validate() error {
	var problems []string
	problems = append(problems, c.Runners.Validate()...)

	problems = append(problems, c.Identity.Validate("identity")...)
	problems = append(problems, c.ActiveHours.Validate("active_hours")...)
	c.validateTracker(&problems)
	problems = append(problems, c.Dependencies.Validate("dependencies")...)
	validatePollingInterval(c.Polling.IntervalMS, &problems)
	validatePositive("polling.refresh_failure_threshold", c.Polling.RefreshFailureThreshold, &problems)
	c.Workspace.validate(&problems)
	if c.Worker.MaxConcurrentAgentsPerHost != nil {
		validatePositive("worker.max_concurrent_agents_per_host", *c.Worker.MaxConcurrentAgentsPerHost, &problems)
	}
	validatePositive("worker.github_token_resolution_timeout_ms", c.Worker.GitHubTokenResolutionTimeoutMS, &problems)
	validatePositive("worker.github_rest_min_remaining_reserve", c.Worker.GitHubRESTMinReserve, &problems)
	if c.Worker.GitHubRESTPollIntervalMS < 60000 {
		problems = append(problems, "worker.github_rest_poll_interval_ms must be greater than or equal to 60000")
	}
	c.Agent.validate("agent", &problems)
	c.validateStopRun(&problems)
	c.validateAgentInstructions(&problems)
	c.validateAutoPromoteReworkLimit(&problems)
	c.validateAutoPromoteNoProgressLimit(&problems)
	c.Agents.validate(&problems)
	c.Codex.validate(&problems)
	problems = append(problems, gate.Validate("gate", c.Gate)...)
	if c.Gate.SecurityAudit.Enabled {
		if gate.NormalizeKind(c.Gate.Kind) != gate.KindCommand {
			problems = append(problems, "gate.security_audit.enabled requires gate.kind command")
		}
		if c.Tracker.Kind != TrackerGitHub && c.Tracker.Kind != TrackerGitHubLocal {
			problems = append(problems, "gate.security_audit.enabled requires tracker.kind github or github_local")
		}
	}
	problems = append(problems, gate.ValidatePlan("plan", c.Plan)...)
	c.Deliverable.validate(&problems)
	c.Server.validate(&problems)
	c.Observability.validate(&problems)
	c.Release.validate("release", &problems)
	if c.Release.Enabled && c.Tracker.Kind != TrackerGitHub && c.Tracker.Kind != TrackerGitHubLocal {
		problems = append(problems, "release.enabled requires tracker.kind github or github_local")
	}
	if c.Release.Enabled && !validRepositoryName(c.Tracker.Repository) {
		problems = append(problems, "tracker.repository must be owner/name when release.enabled is true")
	}
	c.Budget.validate("budget", &problems)
	c.Hooks.validate(&problems)
	coordinationEndpoint := ""
	if c.Tracker.Kind == TrackerGitHub || c.Tracker.Kind == TrackerGitHubLocal {
		coordinationEndpoint = c.Tracker.Endpoint
	}
	c.ScheduleOwnership = c.ScheduleOwnership.Normalized(c.Tracker.Repository, coordinationEndpoint)
	problems = append(problems, c.ScheduleOwnership.Validate("schedule_ownership")...)
	states := make([]string, 0, len(c.configuredWorkflowStates()))
	for _, state := range c.configuredWorkflowStates() {
		states = append(states, state)
	}
	problems = append(problems, c.Intake.Validate("intake", states)...)
	if c.Intake.Enabled() && c.Tracker.Kind != TrackerGitHub {
		problems = append(problems, "intake.sources requires tracker.kind github")
	}
	if c.Intake.Enabled() && !validRepositoryName(c.Tracker.Repository) {
		problems = append(problems, "tracker.repository is required for intake sources")
	}
	c.Retro.Normalize()
	problems = append(problems, c.Retro.Validate("retro", states)...)
	if c.Retro.Enabled && c.Tracker.Kind != TrackerGitHub && c.Tracker.Kind != TrackerMemory {
		problems = append(problems, "retro.enabled requires tracker.kind github or memory")
	}
	if c.Retro.Enabled && c.Tracker.Kind == TrackerGitHub && !validRepositoryName(c.Tracker.Repository) {
		problems = append(problems, "tracker.repository is required for retro")
	}
	problems = append(problems, ValidateRoutines("routines", c.Routines, states)...)
	if len(c.Routines) > 0 && c.Tracker.Kind != TrackerGitHub && c.Tracker.Kind != TrackerMemory {
		problems = append(problems, "routines requires tracker.kind github or memory")
	}
	if len(c.Routines) > 0 && c.Tracker.Kind == TrackerGitHub && !validRepositoryName(c.Tracker.Repository) {
		problems = append(problems, "tracker.repository is required for routines")
	}
	c.BacklogAdmission.Normalize()
	problems = append(problems, c.BacklogAdmission.Validate("backlog_admission", states, c.Tracker)...)
	if c.SchedulersEnabled() && !c.ScheduleOwnership.Enabled {
		problems = append(problems, "schedule_ownership.enabled must be true when scheduled work is configured; add a schedule_ownership block with enabled: true and a shared key")
	}

	if len(problems) > 0 {
		return ValidationError{Problems: problems}
	}

	return nil
}

func (e ValidationError) Error() string {
	return strings.Join(e.Problems, "; ")
}

func (s *StringOrMap) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		return nil
	}

	switch value.Kind {
	case yaml.ScalarNode:
		if value.Tag != "!!str" {
			return fmt.Errorf("must be a string or mapping, got %s", yamlKindName(value))
		}
		*s = StringValue(value.Value)
		return nil
	case yaml.MappingNode:
		decoded, err := decodeMapNode(value)
		if err != nil {
			return err
		}
		*s = MapValue(decoded)
		return nil
	default:
		return fmt.Errorf("must be a string or mapping, got %s", yamlKindName(value))
	}
}

func (s StringOrMap) MarshalYAML() (any, error) {
	if s.IsString {
		return s.String, nil
	}
	if s.IsMap {
		return s.Map, nil
	}
	return yamlNullNode(), nil
}

func (o *BackendOptions) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		*o = BackendOptions{}
		return nil
	}
	o.node = cloneYAMLNode(value)
	return nil
}

func (o BackendOptions) MarshalYAML() (any, error) {
	if o.node == nil {
		return yamlNullNode(), nil
	}
	return o.node, nil
}

func (o BackendOptions) Decode(out any) error {
	if o.node == nil {
		return nil
	}
	return o.node.Decode(out)
}

func backendOptionsFrom(value any) BackendOptions {
	var node yaml.Node
	if err := node.Encode(value); err != nil {
		return BackendOptions{}
	}
	return BackendOptions{node: &node}
}

func yamlNullNode() *yaml.Node {
	return &yaml.Node{
		Kind: yaml.ScalarNode,
		Tag:  "!!null",
	}
}

func cloneYAMLNode(in *yaml.Node) *yaml.Node {
	if in == nil {
		return nil
	}
	out := *in
	if len(in.Content) > 0 {
		out.Content = make([]*yaml.Node, len(in.Content))
		for index := range in.Content {
			out.Content[index] = cloneYAMLNode(in.Content[index])
		}
	}
	return &out
}

func (c *Config) normalize() {
	c.Identity.Normalize()
	c.ActiveHours = c.ActiveHours.Normalize()
	c.Tracker.Kind = strings.ToLower(strings.TrimSpace(c.Tracker.Kind))
	if (c.Tracker.Kind == TrackerGitHub || c.Tracker.Kind == TrackerGitHubLocal) && c.Tracker.Endpoint == defaultLinearEndpoint {
		c.Tracker.Endpoint = defaultGitHubEndpoint
	}
	c.Tracker.GitHubStatusSource = normalizeGitHubStatusSource(c.Tracker.GitHubStatusSource)
	c.Tracker.StatusPageURL = normalizeStatusPageURL(c.Tracker.Kind, c.Tracker.StatusPageURL)
	c.Tracker.GitHubWebhookSecret = strings.TrimSpace(c.Tracker.GitHubWebhookSecret)
	c.Tracker.Repository = strings.TrimSpace(c.Tracker.Repository)
	c.Tracker.StatusField = strings.TrimSpace(c.Tracker.StatusField)
	if c.Tracker.StatusField == "" {
		c.Tracker.StatusField = "Status"
	}
	c.Tracker.StatusLabelPrefix = strings.TrimSpace(c.Tracker.StatusLabelPrefix)
	if c.Tracker.StatusLabelPrefix == "" {
		c.Tracker.StatusLabelPrefix = "detent:"
	}
	c.Tracker.WriteProbeIssue = strings.TrimSpace(c.Tracker.WriteProbeIssue)
	c.Tracker.LocalSQLite.Normalize()
	c.Tracker.Claims.Normalize()
	c.Tracker.DependencyAutoUnblock.Normalize()
	c.Tracker.BlockedRecovery.Normalize()
	c.Tracker.BlockerAutoPromote.Normalize()
	c.Tracker.Authorization.Normalize()
	c.Dependencies.Normalize()
	c.Workspace.Normalize()
	c.Deliverable.Normalize()
	c.Release.normalize()
	c.Budget.Normalize()
	c.Agent.Budget.Normalize()
	c.Agent.StopRun.TargetState = strings.TrimSpace(c.Agent.StopRun.TargetState)
	if c.Agent.StopRun.TargetState == "" {
		c.Agent.StopRun.TargetState = "Blocked"
	}

	c.Agent.MaxConcurrentAgentsByState = normalizeStateLimits(c.Agent.MaxConcurrentAgentsByState)
	c.Agent.RateWindowPacing = c.Agent.RateWindowPacing.Normalized()
	if c.Agent.MergeWorkerStartupTimeoutMS == 0 {
		c.Agent.MergeWorkerStartupTimeoutMS = DefaultMergeWorkerStartupTimeoutMS
	}
	if c.Agent.MergeWorkerMaxDurationMS == 0 {
		c.Agent.MergeWorkerMaxDurationMS = DefaultMergeWorkerMaxDurationMS
	}
	if c.Agent.MergeFallbackMaxDurationMS == 0 {
		c.Agent.MergeFallbackMaxDurationMS = DefaultMergeFallbackMaxDurationMS
	}
	if c.Agent.MergeFastPath.FairnessAgeSeconds == 0 {
		c.Agent.MergeFastPath.FairnessAgeSeconds = DefaultMergeFairnessAgeSeconds
	}
	if c.Agent.FailureBreaker.SameClassLimit == 0 {
		c.Agent.FailureBreaker.SameClassLimit = DefaultFailureBreakerSameClassLimit
	}
	if c.Agent.FailureBreaker.WindowSeconds == 0 {
		c.Agent.FailureBreaker.WindowSeconds = DefaultFailureBreakerWindowSeconds
	}
	if c.Agent.FailureBreaker.CooldownSeconds == 0 {
		c.Agent.FailureBreaker.CooldownSeconds = DefaultFailureBreakerCooldownSeconds
	}
	c.Agent.DispatchPriorityByState = normalizeStateList(c.Agent.DispatchPriorityByState)
	c.Agent.DispatchPriorityByLabel = normalizeLabels(c.Agent.DispatchPriorityByLabel)
	c.Agent.LifetimeLimitOverrideLabel = normalizeLabel(c.Agent.LifetimeLimitOverrideLabel)
	c.Agent.MaxSessionTokenOverrideLabel = normalizeLabel(c.Agent.MaxSessionTokenOverrideLabel)
	c.Agent.MaxSessionTokenOverrideField = strings.TrimSpace(c.Agent.MaxSessionTokenOverrideField)
	c.Agent.AutoPromote.OptoutLabel = normalizeLabel(c.Agent.AutoPromote.OptoutLabel)
	c.Agent.AutoPromote.AllowedIssueLabels = normalizeLabels(c.Agent.AutoPromote.AllowedIssueLabels)
	c.Agent.AutoPromote.GateWaitState = normalizeAutoPromoteGateWaitState(c.Agent.AutoPromote.GateWaitState)
	c.Agent.AutoPromote.GateWaitTimeoutAction = normalizeAutoPromoteGateWaitTimeoutAction(c.Agent.AutoPromote.GateWaitTimeoutAction)
	if c.Agent.AutoPromote.GateWaitTimeoutSeconds == 0 {
		c.Agent.AutoPromote.GateWaitTimeoutSeconds = DefaultAutoPromoteGateWaitTimeoutSeconds
	}
	c.Agent.AutoPromote.SourceState = strings.TrimSpace(c.Agent.AutoPromote.SourceState)
	if c.Agent.AutoPromote.SourceState == "" {
		c.Agent.AutoPromote.SourceState = "Human Review"
	}
	c.Agent.AutoPromote.PassState = strings.TrimSpace(c.Agent.AutoPromote.PassState)
	if c.Agent.AutoPromote.PassState == "" {
		c.Agent.AutoPromote.PassState = "Merging"
	}
	c.Agent.AutoPromote.ReworkState = strings.TrimSpace(c.Agent.AutoPromote.ReworkState)
	if c.Agent.AutoPromote.ReworkState == "" {
		c.Agent.AutoPromote.ReworkState = "Rework"
	}
	c.Agent.Knowledge.Normalize()
	c.Agents.normalize()
	c.Codex.Shell = commandshell.Normalize(c.Codex.Shell)
	c.Codex.ModelProvider = strings.TrimSpace(c.Codex.ModelProvider)
	c.Codex.ServiceTier = strings.TrimSpace(c.Codex.ServiceTier)
	c.Codex.DeliverableElicitationAllowlist = normalizeDeliverableElicitationRules(c.Codex.DeliverableElicitationAllowlist)
	c.Gate = gate.Effective(c.Gate)
	c.Agent.AutoPromote.GateWaitTimeoutAction = EffectiveAutoPromoteGateWaitTimeoutAction(
		c.Agent.AutoPromote.GateWaitTimeoutAction,
		c.Gate,
	)
	c.Plan = gate.EffectivePlan(c.Plan)
	if c.Plan.Enabled {
		c.Tracker.ObservedStates = appendStateUnique(c.Tracker.ObservedStates, c.Plan.Stop)
	}
	c.Server.Normalize()
	c.Observability.Normalize()
	c.Hooks.Shell = commandshell.Normalize(c.Hooks.Shell)
	coordinationEndpoint := ""
	if c.Tracker.Kind == TrackerGitHub || c.Tracker.Kind == TrackerGitHubLocal {
		coordinationEndpoint = c.Tracker.Endpoint
	}
	c.ScheduleOwnership = c.ScheduleOwnership.Normalized(c.Tracker.Repository, coordinationEndpoint)
	c.Intake.Normalize()
	c.Retro.Normalize()
	c.Routines = NormalizeRoutines(c.Routines)
	c.BacklogAdmission.Normalize()
}

func (c Config) SchedulersEnabled() bool {
	if len(c.Routines) > 0 || c.BacklogAdmission.Enabled {
		return true
	}
	for _, source := range c.Intake.Sources {
		if source.Kind == intake.KindSchedule {
			return true
		}
	}
	return false
}

func (c *Config) validateTracker(problems *[]string) {
	switch c.Tracker.Kind {
	case "":
		*problems = append(*problems, "tracker.kind is required")
	case TrackerLinear:
		validateRequired("tracker.api_key", c.Tracker.APIKey, " for linear", problems)
		validateRequired("tracker.project_slug", c.Tracker.ProjectSlug, " for linear", problems)
	case TrackerGitHub:
		c.Tracker.validateGitHubAuth(problems)
		c.Tracker.validateGitHubStatusSource(problems)
	case TrackerGitHubLocal:
		c.Tracker.validateGitHubLocal(problems)
	case TrackerMemory:
	case TrackerHubNative:
	case TrackerLocalSQLite:
		*problems = append(*problems, c.Tracker.LocalSQLite.Validate("tracker.local_sqlite")...)
	default:
		*problems = append(*problems, "tracker.kind must be one of github, github_local, hub_native, linear, memory, local_sqlite")
	}

	validateStateList("tracker.active_states", c.Tracker.ActiveStates, problems)
	validateStateList("tracker.observed_states", c.Tracker.ObservedStates, problems)
	validateStateList("tracker.terminal_states", c.Tracker.TerminalStates, problems)
	validateStateMap("tracker.state_map", c.Tracker.StateMap, problems)
	validatePriorityMap("tracker.priority_map", c.Tracker.PriorityMap, problems)
	*problems = append(*problems, c.Tracker.DependencyAutoUnblock.Validate("tracker.dependency_auto_unblock")...)
	if c.Tracker.DependencyAutoUnblock.Enabled && !stateListContains(c.Tracker.ActiveStates, "Rework") {
		*problems = append(*problems, "tracker.active_states must include Rework when tracker.dependency_auto_unblock.enabled is true")
	}
	*problems = append(*problems, c.Tracker.BlockedRecovery.Validate("tracker.blocked_recovery")...)
	if c.Tracker.BlockedRecovery.Enabled && !stateListContains(c.Tracker.ActiveStates, c.Tracker.BlockedRecovery.TargetState) {
		*problems = append(*problems, "tracker.active_states must include tracker.blocked_recovery.target_state when tracker.blocked_recovery.enabled is true")
	}
	*problems = append(*problems, c.Tracker.BlockerAutoPromote.Validate("tracker.blocker_auto_promote")...)
	validatePositive("tracker.http_max_idle_conns", c.Tracker.HTTPMaxIdleConns, problems)
	validatePositive("tracker.http_max_idle_conns_per_host", c.Tracker.HTTPMaxIdleConnsPerHost, problems)
	validatePositive("tracker.http_idle_conn_timeout_ms", c.Tracker.HTTPIdleConnTimeoutMS, problems)
	validatePositive("tracker.github_graphql_warn_remaining", c.Tracker.GitHubGraphQLWarnRemaining, problems)
	validatePositive("tracker.github_graphql_min_remaining_reserve", c.Tracker.GitHubGraphQLMinReserve, problems)
	validatePositive("tracker.github_rest_min_remaining_reserve", c.Tracker.GitHubRESTMinReserve, problems)
	validateNonNegative("tracker.github_rest_fanout_max_requests", c.Tracker.GitHubRESTFanoutMaxRequests, problems)
	validatePositive("tracker.github_unstarted_check_threshold_seconds", c.Tracker.GitHubUnstartedSeconds, problems)
	validateStatusPageURL(c.Tracker.StatusPageURL, problems)
	*problems = append(*problems, c.Tracker.Claims.Validate("tracker.claims")...)
	*problems = append(*problems, c.Tracker.Authorization.Validate("tracker.authorization")...)
}

func normalizeStatusPageURL(kind string, value string) string {
	value = strings.TrimRight(strings.TrimSpace(value), "/")
	if value != "" {
		return value
	}
	switch kind {
	case TrackerGitHub, TrackerGitHubLocal:
		return defaultGitHubStatusPageURL
	case TrackerLinear:
		return defaultLinearStatusPageURL
	default:
		return ""
	}
}

func validateStatusPageURL(value string, problems *[]string) {
	if value == "" {
		return
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		*problems = append(*problems, "tracker.status_page_url must be an absolute http or https base URL without credentials, path, query, or fragment")
	}
}

func (c *Config) validateAgentInstructions(problems *[]string) {
	configuredStates := c.configuredWorkflowStates()
	validateInstructionsByState("agent.instructions_by_state", c.Agent.InstructionsByState, configuredStates, problems)
	validateInstructionsByTransition("agent.instructions_by_transition", c.Agent.InstructionsByTransition, configuredStates, problems)
}

func (c *Config) validateStopRun(problems *[]string) {
	target := strings.TrimSpace(c.Agent.StopRun.TargetState)
	if target == "" {
		*problems = append(*problems, "agent.stop_run.target_state is required")
		return
	}
	if stateListContains(c.Tracker.ActiveStates, target) {
		*problems = append(*problems, "agent.stop_run.target_state must not be an active state")
	}
	if stateListContains(c.Tracker.TerminalStates, target) {
		*problems = append(*problems, "agent.stop_run.target_state must not be a terminal state")
	}
	if !stateListContains(c.Tracker.ObservedStates, target) {
		*problems = append(*problems, "agent.stop_run.target_state must be included in tracker.observed_states")
	}
}

func (c Config) configuredWorkflowStates() map[string]string {
	states := make(map[string]string, len(c.Tracker.ActiveStates)+len(c.Tracker.ObservedStates)+len(c.Tracker.TerminalStates))
	add := func(values []string) {
		for _, state := range values {
			normalized := normalizeIssueState(state)
			if normalized == "" {
				continue
			}
			if _, ok := states[normalized]; !ok {
				states[normalized] = strings.TrimSpace(state)
			}
		}
	}
	add(c.Tracker.ActiveStates)
	add(c.Tracker.ObservedStates)
	add(c.Tracker.TerminalStates)
	return states
}

func (c *Config) validateAutoPromoteReworkLimit(problems *[]string) {
	if !c.Agent.AutoPromote.Enabled || c.Agent.AutoPromote.ReworkLimit <= 0 {
		return
	}
	if stateListContains(c.Tracker.ActiveStates, "Blocked") ||
		stateListContains(c.Tracker.ObservedStates, "Blocked") ||
		stateListContains(c.Tracker.TerminalStates, "Blocked") {
		return
	}
	*problems = append(*problems, "tracker.active_states, tracker.observed_states, or tracker.terminal_states must include Blocked when agent.auto_promote.rework_limit is greater than 0")
}

func (c *Config) validateAutoPromoteNoProgressLimit(problems *[]string) {
	if c.Agent.AutoPromote.NoProgressLimit <= 0 {
		return
	}
	if stateListContains(c.Tracker.ActiveStates, "Blocked") ||
		stateListContains(c.Tracker.ObservedStates, "Blocked") ||
		stateListContains(c.Tracker.TerminalStates, "Blocked") {
		return
	}
	*problems = append(*problems, "tracker.active_states, tracker.observed_states, or tracker.terminal_states must include Blocked when agent.auto_promote.no_progress_limit is greater than 0")
}

func (l *LocalSQLite) Normalize() {
	if l == nil {
		return
	}
	l.Path = strings.TrimSpace(l.Path)
	l.ProjectID = strings.TrimSpace(l.ProjectID)
}

func (l LocalSQLite) Validate(prefix string) []string {
	l.Normalize()
	if l.Path == "" {
		return []string{prefix + ".path is required for local_sqlite"}
	}
	return nil
}

func (t *Tracker) validateGitHubStatusSource(problems *[]string) {
	switch t.GitHubStatusSource {
	case GitHubStatusSourceProjectV2:
		validateRequired("tracker.project_slug", t.ProjectSlug, " for github project_v2", problems)
	case GitHubStatusSourceIssueField:
		validateRequired("tracker.repository", t.Repository, " for github issue_field", problems)
		if strings.TrimSpace(t.StatusField) == "" {
			*problems = append(*problems, "tracker.status_field is required for github issue_field")
		}
		if strings.TrimSpace(t.Repository) != "" && !validRepositoryName(t.Repository) {
			*problems = append(*problems, "tracker.repository must be owner/name")
		}
	case GitHubStatusSourceLabel:
		validateRequired("tracker.repository", t.Repository, " for github label", problems)
		if strings.TrimSpace(t.Repository) != "" && !validRepositoryName(t.Repository) {
			*problems = append(*problems, "tracker.repository must be owner/name")
		}
		if strings.TrimSpace(t.StatusLabelPrefix) == "" {
			*problems = append(*problems, "tracker.status_label_prefix is required for github label")
		}
	default:
		*problems = append(*problems, "tracker.github_status_source must be one of project_v2, issue_field, label")
	}
}

func (t *Tracker) validateGitHubAuth(problems *[]string) {
	t.validateGitHubAuthFor("github", problems)
}

func (t *Tracker) validateGitHubAuthFor(kind string, problems *[]string) {
	if strings.TrimSpace(t.APIKey) != "" || t.hasGitHubAppCredentials() {
		return
	}

	if strings.TrimSpace(t.GitHubAppID) == "" &&
		strings.TrimSpace(t.GitHubAppInstallationID) == "" &&
		strings.TrimSpace(t.GitHubAppPrivateKey) == "" &&
		strings.TrimSpace(t.GitHubAppPrivateKeyPath) == "" {
		*problems = append(*problems, "tracker.api_key or GitHub App credentials are required for "+kind)
		return
	}

	validateRequired("tracker.github_app_id", t.GitHubAppID, " for github app", problems)
	validateRequired("tracker.github_app_installation_id", t.GitHubAppInstallationID, " for github app", problems)
	if strings.TrimSpace(t.GitHubAppPrivateKey) == "" && strings.TrimSpace(t.GitHubAppPrivateKeyPath) == "" {
		*problems = append(*problems, "tracker.github_app_private_key or tracker.github_app_private_key_path is required for github app")
	}
}

func (t *Tracker) validateGitHubLocal(problems *[]string) {
	t.validateGitHubAuthFor(TrackerGitHubLocal, problems)
	validateRequired("tracker.repository", t.Repository, " for github_local", problems)
	if strings.TrimSpace(t.Repository) != "" && !validRepositoryName(t.Repository) {
		*problems = append(*problems, "tracker.repository must be owner/name")
	}
	*problems = append(*problems, t.LocalSQLite.Validate("tracker.local_sqlite")...)
	if t.gitHubStatusSourceSet {
		*problems = append(*problems, "tracker.github_status_source must be omitted when tracker.kind is github_local; Detent stores workflow status in tracker.local_sqlite")
	}
}

func (t *Tracker) hasGitHubAppCredentials() bool {
	return strings.TrimSpace(t.GitHubAppID) != "" &&
		strings.TrimSpace(t.GitHubAppInstallationID) != "" &&
		(strings.TrimSpace(t.GitHubAppPrivateKey) != "" || strings.TrimSpace(t.GitHubAppPrivateKeyPath) != "")
}

func normalizeGitHubStatusSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", GitHubStatusSourceProjectV2, "projectv2", "project":
		return GitHubStatusSourceProjectV2
	case GitHubStatusSourceIssueField, "issuefield", "issues":
		return GitHubStatusSourceIssueField
	case GitHubStatusSourceLabel, "labels", "issue_label", "issue_labels":
		return GitHubStatusSourceLabel
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func validRepositoryName(value string) bool {
	owner, repo, ok := strings.Cut(strings.TrimSpace(value), "/")
	return ok && strings.TrimSpace(owner) != "" && strings.TrimSpace(repo) != "" && !strings.Contains(repo, "/")
}

func (w *Workspace) validate(problems *[]string) {
	switch w.Kind {
	case "", WorkspaceLocalGit, WorkspaceFilesystem:
	default:
		*problems = append(*problems, "workspace.kind must be one of local_git, filesystem")
	}
	switch w.CacheStrategy {
	case "", WorkspaceCacheIsolated, WorkspaceCacheShared:
	default:
		*problems = append(*problems, "workspace.cache_strategy must be one of isolated, shared")
	}
	validatePositive("workspace.cleanup_idle_ttl_ms", w.CleanupIdleTTLMS, problems)
	validatePositive("workspace.cleanup_sweep_interval_ms", w.CleanupSweepIntervalMS, problems)
}

func (w *Workspace) Normalize() {
	if w == nil {
		return
	}
	w.Kind = normalizeWorkspaceKind(w.Kind)
	w.Root = strings.TrimSpace(w.Root)
	w.SourceRoot = strings.TrimSpace(w.SourceRoot)
	w.OutputRoot = strings.TrimSpace(w.OutputRoot)
	w.CacheStrategy = normalizeWorkspaceCacheStrategy(w.CacheStrategy)
}

func normalizeWorkspaceKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", WorkspaceLocalGit, "git", "localgit":
		return WorkspaceLocalGit
	case WorkspaceFilesystem, "file_system", "file-system", "files":
		return WorkspaceFilesystem
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func normalizeWorkspaceCacheStrategy(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "", WorkspaceCacheIsolated:
		return WorkspaceCacheIsolated
	case WorkspaceCacheShared:
		return WorkspaceCacheShared
	default:
		return strings.ToLower(strings.TrimSpace(strategy))
	}
}

func (d *Deliverable) Normalize() {
	if d == nil {
		return
	}
	d.Kind = normalizeDeliverableKind(d.Kind)
	d.MergeMethod = normalizeMergeMethod(d.MergeMethod)
	d.OutputRoot = strings.TrimSpace(d.OutputRoot)
	d.ReviewURL = strings.TrimSpace(d.ReviewURL)
}

func (d *Deliverable) validate(problems *[]string) {
	switch d.Kind {
	case "", DeliverablePullRequest, DeliverableArtifact:
	default:
		*problems = append(*problems, "deliverable.kind must be one of pull_request, artifact")
	}
	switch d.MergeMethod {
	case "", MergeMethodSquash, MergeMethodMerge, MergeMethodRebase:
	default:
		*problems = append(*problems, "deliverable.merge_method must be one of squash, merge, rebase")
	}
}

func normalizeDeliverableElicitationRules(rules []DeliverableElicitationRule) []DeliverableElicitationRule {
	if rules == nil {
		return nil
	}
	out := make([]DeliverableElicitationRule, len(rules))
	for index, rule := range rules {
		out[index] = DeliverableElicitationRule{
			Server:     strings.TrimSpace(rule.Server),
			Tool:       strings.TrimSpace(rule.Tool),
			Repository: strings.TrimSpace(rule.Repository),
		}
	}
	return out
}

func validateDeliverableElicitationRules(prefix string, rules []DeliverableElicitationRule, problems *[]string) {
	seen := make(map[string]struct{}, len(rules))
	for index, rule := range rules {
		field := fmt.Sprintf("%s[%d]", prefix, index)
		if rule.Server == "" {
			*problems = append(*problems, field+".server is required")
		}
		if rule.Tool == "" {
			*problems = append(*problems, field+".tool is required")
		}
		if !validRepositoryName(rule.Repository) {
			*problems = append(*problems, field+".repository must be owner/name")
		}
		key := rule.Server + "\x00" + rule.Tool + "\x00" + strings.ToLower(rule.Repository)
		if _, ok := seen[key]; ok {
			*problems = append(*problems, field+" duplicates an earlier rule")
		}
		seen[key] = struct{}{}
	}
}

func (d Deliverable) EffectiveMergeMethod() string {
	method := normalizeMergeMethod(d.MergeMethod)
	if method == "" {
		return MergeMethodSquash
	}
	return method
}

func (d Deliverable) MergeMethodConfigured() bool {
	return d.mergeMethodConfigured
}

func normalizeMergeMethod(method string) string {
	return strings.ToLower(strings.TrimSpace(method))
}

func normalizeDeliverableKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", DeliverablePullRequest, "pr", "pull-request":
		return DeliverablePullRequest
	case DeliverableArtifact, "artifacts", "file", "files":
		return DeliverableArtifact
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func (a *Agent) validate(prefix string, problems *[]string) {
	validatePositive(prefix+".max_concurrent_agents", a.MaxConcurrentAgents, problems)
	*problems = append(*problems, a.RateWindowPacing.Validate(prefix+".rate_window_pacing")...)
	validatePositive(prefix+".max_turns", a.MaxTurns, problems)
	if a.MaxTurnDurationMS < 0 {
		*problems = append(*problems, prefix+".max_turn_duration_ms must be greater than or equal to 0")
	}
	if a.MaxSessionDurationMS < 0 {
		*problems = append(*problems, prefix+".max_session_duration_ms must be greater than or equal to 0")
	}
	if a.NoProgressTimeoutMS < 0 {
		*problems = append(*problems, prefix+".no_progress_timeout_ms must be greater than or equal to 0")
	}
	validatePositive(prefix+".merge_worker_startup_timeout_ms", a.MergeWorkerStartupTimeoutMS, problems)
	validatePositive(prefix+".merge_worker_max_duration_ms", a.MergeWorkerMaxDurationMS, problems)
	validatePositive(prefix+".merge_fallback_max_duration_ms", a.MergeFallbackMaxDurationMS, problems)
	validatePositive(prefix+".max_retry_backoff_ms", a.MaxRetryBackoffMS, problems)
	validatePositive(prefix+".overload_retry_delay_ms", a.OverloadRetryDelayMS, problems)
	if a.MaxSessionTokens < 0 {
		*problems = append(*problems, prefix+".max_session_tokens must be greater than or equal to 0")
	}
	if a.MaxSessionContextMultiplier < 0 {
		*problems = append(*problems, prefix+".max_session_context_multiplier must be greater than or equal to 0")
	}
	if a.NoProgressTokenLimit < 0 {
		*problems = append(*problems, prefix+".no_progress_token_limit must be greater than or equal to 0")
	}
	if a.NoProgressSpendLimitUSD < 0 {
		*problems = append(*problems, prefix+".no_progress_spend_limit_usd must be greater than or equal to 0")
	}
	if a.LifetimeSessionLimit < 0 {
		*problems = append(*problems, prefix+".lifetime_session_limit must be greater than or equal to 0")
	}
	if a.LifetimeTokenLimit < 0 {
		*problems = append(*problems, prefix+".lifetime_token_limit must be greater than or equal to 0")
	}
	if (a.LifetimeSessionLimit > 0 || a.LifetimeTokenLimit > 0) && a.LifetimeLimitCooldownSeconds <= 0 {
		*problems = append(*problems, prefix+".lifetime_limit_cooldown_seconds must be greater than 0 when a lifetime limit is enabled")
	}
	validatePositive(prefix+".failure_breaker.same_class_limit", a.FailureBreaker.SameClassLimit, problems)
	validatePositive(prefix+".failure_breaker.window_seconds", a.FailureBreaker.WindowSeconds, problems)
	validatePositive(prefix+".failure_breaker.cooldown_seconds", a.FailureBreaker.CooldownSeconds, problems)
	a.Effort.validate(prefix+".effort", problems)
	a.Shutdown.validate(prefix+".shutdown", problems)
	validateStateLimits(prefix+".max_concurrent_agents_by_state", a.MaxConcurrentAgentsByState, problems)
	validateStateList(prefix+".dispatch_priority_by_state", a.DispatchPriorityByState, problems)
	validateLabelList(prefix+".dispatch_priority_by_label", a.DispatchPriorityByLabel, problems)
	validatePositive(prefix+".merge_fast_path.fairness_age_seconds", a.MergeFastPath.FairnessAgeSeconds, problems)
	a.AutoPromote.validate(prefix+".auto_promote", problems)
	a.OutputTruncation.validate(prefix+".output_truncation", problems)
	a.Budget.validate(prefix+".budget", problems)
	a.Lessons.validate(prefix+".lessons", problems)
	a.Knowledge.validate(prefix+".knowledge", problems)
	a.Skills.validate(prefix+".skills", problems)
}

func (e AgentRoleEffort) validate(prefix string, problems *[]string) {
	for _, roleEffort := range []struct {
		role   string
		effort string
	}{
		{role: "code", effort: e.Code},
		{role: "rework", effort: e.Rework},
		{role: "merge", effort: e.Merge},
	} {
		role := roleEffort.role
		effort := roleEffort.effort
		switch strings.ToLower(strings.TrimSpace(effort)) {
		case "", claudeCodeEffortLow, claudeCodeEffortMedium, claudeCodeEffortHigh, claudeCodeEffortXHigh, claudeCodeEffortMax, claudeCodeEffortUltracode:
		default:
			*problems = append(*problems, prefix+"."+role+" must be one of low, medium, high, xhigh, max, ultracode")
		}
	}
}

func (s Shutdown) validate(prefix string, problems *[]string) {
	if s.DrainTimeoutMS < 0 {
		*problems = append(*problems, prefix+".drain_timeout_ms must be greater than or equal to 0")
	}
}

func (a *Agents) normalize() {
	for index := range a.Backends {
		backend := &a.Backends[index]
		backend.ID = strings.TrimSpace(backend.ID)
		backend.Kind = strings.ToLower(strings.TrimSpace(backend.Kind))
		backend.Protocol = normalizeAgentProtocol(backend.Protocol)
		if backend.Protocol == "" && backend.Kind == AgentBackendCodex {
			backend.Protocol = defaultCodexProtocol
		}
		backend.Command = strings.TrimSpace(backend.Command)
		backend.Provider = strings.TrimSpace(backend.Provider)
		if backend.Protocol == "" && backend.Kind == AgentBackendClaudeCode {
			backend.Protocol = defaultClaudeCodeProtocol
		}
		if backend.Command == "" && backend.Kind == AgentBackendClaudeCode {
			backend.Command = defaultClaudeCodeCommand
		}
		backend.decodeOptions()
	}
	for index := range a.Routes {
		route := &a.Routes[index]
		route.Name = strings.TrimSpace(route.Name)
		route.Role = strings.ToLower(strings.TrimSpace(route.Role))
		route.Backend = strings.TrimSpace(route.Backend)
		route.Model = strings.TrimSpace(route.Model)
		route.ModelField = strings.TrimSpace(route.ModelField)
	}
}

func normalizeAgentProtocol(protocol string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "app_server", "appserver":
		return defaultCodexProtocol
	default:
		return strings.ToLower(strings.TrimSpace(protocol))
	}
}

func (a *Agents) validate(problems *[]string) {
	policy := a.ModelSelection
	if policy.Sources == nil {
		policy = ResolveModelSelection(ModelSelection{}, policy)
	}
	*problems = append(*problems, policy.Validate()...)
	backendIDs := make(map[string]struct{}, len(a.Backends))
	if len(a.Backends) == 0 {
		backendIDs[DefaultAgentBackendID] = struct{}{}
	}
	for _, backend := range a.Backends {
		if backend.Disabled {
			continue
		}
		if strings.TrimSpace(backend.ID) == "" {
			*problems = append(*problems, "agents.backends.id is required")
			continue
		}
		if _, ok := backendIDs[backend.ID]; ok {
			*problems = append(*problems, "agents.backends ids must be unique")
		}
		backendIDs[backend.ID] = struct{}{}

		switch backend.Kind {
		case "":
			*problems = append(*problems, "agents.backends.kind is required")
		case AgentBackendCodex, AgentBackendClaudeCode:
		default:
			*problems = append(*problems, "agents.backends.kind must be one of codex, claude_code")
		}
		switch backend.Kind {
		case AgentBackendCodex:
			if backend.Protocol != defaultCodexProtocol {
				*problems = append(*problems, "agents.backends.protocol must be app-server for codex")
			}
		case AgentBackendClaudeCode:
			if backend.Protocol != defaultClaudeCodeProtocol {
				*problems = append(*problems, "agents.backends.protocol must be headless for claude_code")
			}
		}
		validateRequired("agents.backends.command", backend.Command, "", problems)
		if backend.Provider != "" && !validAgentIdentityLabel(backend.Provider) {
			*problems = append(*problems, "agents.backends.provider must be a sanitized label containing only letters, numbers, dots, underscores, or hyphens")
		}
		backend.validateOptions("agents.backends.options", problems)
	}

	defaultRoutes := map[string]int{}
	for _, route := range a.Routes {
		if route.Disabled {
			continue
		}
		if strings.TrimSpace(route.Backend) == "" {
			*problems = append(*problems, "agents.routes.backend is required")
		} else if _, ok := backendIDs[route.Backend]; !ok {
			*problems = append(*problems, "agents.routes.backend must reference a configured backend")
		}
		if route.Default {
			defaultRoutes[normalizeAgentRouteRole(route.Role)]++
		}
		validatePriorityValues("agents.routes.selector.priority_in", route.Selector.PriorityIn, problems)
	}
	for _, count := range defaultRoutes {
		if count > 1 {
			*problems = append(*problems, "agents.routes must not define multiple default routes for the same role")
			break
		}
	}
}

func normalizeAgentRouteRole(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		return "code"
	}
	return role
}

func (b AgentBackend) validateOptions(prefix string, problems *[]string) {
	switch b.Kind {
	case AgentBackendCodex:
		options, err := b.decodedCodexOptions()
		if err != nil {
			*problems = append(*problems, prefix+" must decode for codex: "+err.Error())
			return
		}
		options.validate(prefix, problems)
	case AgentBackendClaudeCode:
		options, err := b.decodedClaudeCodeOptions()
		if err != nil {
			*problems = append(*problems, prefix+" must decode for claude_code: "+err.Error())
			return
		}
		options.validate(prefix, problems)
	}
}

func (o *CodexOptions) normalize() {
	o.Shell = strings.TrimSpace(o.Shell)
	o.ModelProvider = strings.TrimSpace(o.ModelProvider)
	o.ServiceTier = strings.TrimSpace(o.ServiceTier)
	if o.Shell != "" {
		o.Shell = commandshell.Normalize(o.Shell)
	}
	o.ThreadSandbox = strings.TrimSpace(o.ThreadSandbox)
	o.DeliverableElicitationAllowlist = normalizeDeliverableElicitationRules(o.DeliverableElicitationAllowlist)
}

func (o CodexOptions) validate(prefix string, problems *[]string) {
	validateDeliverableElicitationRules(prefix+".deliverable_elicitation_allowlist", o.DeliverableElicitationAllowlist, problems)
	if o.ModelProvider != "" && !validAgentIdentityLabel(o.ModelProvider) {
		*problems = append(*problems, prefix+".model_provider must be a sanitized label containing only letters, numbers, dots, underscores, or hyphens")
	}
	if o.TurnTimeoutMS < 0 {
		*problems = append(*problems, prefix+".turn_timeout_ms must be greater than or equal to 0")
	}
	if o.ReadTimeoutMS < 0 {
		*problems = append(*problems, prefix+".read_timeout_ms must be greater than or equal to 0")
	}
	if o.StallTimeoutMS < 0 {
		*problems = append(*problems, prefix+".stall_timeout_ms must be greater than or equal to 0")
	}
}

func (o *ClaudeCodeOptions) normalize() {
	o.PermissionMode = strings.TrimSpace(o.PermissionMode)
	o.Effort = strings.ToLower(strings.TrimSpace(o.Effort))
	if o.PermissionMode == "" {
		o.PermissionMode = defaultClaudeCodePermissionMode
	}
	o.Shell = strings.TrimSpace(o.Shell)
	if o.Shell != "" {
		o.Shell = commandshell.Normalize(o.Shell)
	}
}

func (o ClaudeCodeOptions) validate(prefix string, problems *[]string) {
	switch o.PermissionMode {
	case claudeCodePermissionModeDefault, claudeCodePermissionModeAcceptEdits, claudeCodePermissionModeBypassPermissions:
	case claudeCodePermissionModePlan:
		*problems = append(*problems, prefix+".permission_mode must not be plan for unattended workers")
	default:
		*problems = append(*problems, prefix+".permission_mode must be one of default, acceptEdits, bypassPermissions")
	}
	switch o.Effort {
	case "", claudeCodeEffortLow, claudeCodeEffortMedium, claudeCodeEffortHigh, claudeCodeEffortXHigh, claudeCodeEffortMax, claudeCodeEffortUltracode:
	default:
		*problems = append(*problems, prefix+".effort must be one of low, medium, high, xhigh, max, ultracode")
	}
	if o.TurnTimeoutMS < 0 {
		*problems = append(*problems, prefix+".turn_timeout_ms must be greater than or equal to 0")
	}
	if o.StallTimeoutMS < 0 {
		*problems = append(*problems, prefix+".stall_timeout_ms must be greater than or equal to 0")
	}
}

func validAgentIdentityLabel(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 64 {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if index > 0 && (char == '.' || char == '_' || char == '-') {
			continue
		}
		return false
	}
	return true
}

func validatePriorityValues(field string, priorities []int, problems *[]string) {
	for _, priority := range priorities {
		if priority < 1 || priority > 4 {
			*problems = append(*problems, field+" values must be integers 1 through 4")
			return
		}
	}
}

func (a *AutoPromote) validate(prefix string, problems *[]string) {
	if a.QuietSeconds < 0 {
		*problems = append(*problems, prefix+".quiet_seconds must be greater than or equal to 0")
	}
	if strings.TrimSpace(a.OptoutLabel) == "" {
		*problems = append(*problems, prefix+".optout_label must not be blank")
	}
	for _, label := range a.AllowedIssueLabels {
		if strings.TrimSpace(label) == "" {
			*problems = append(*problems, prefix+".allowed_issue_labels labels must not be blank")
			return
		}
	}
	switch normalizeAutoPromoteGateWaitState(a.GateWaitState) {
	case AutoPromoteGateWaitStateSource, AutoPromoteGateWaitStateReview:
	default:
		*problems = append(*problems, prefix+".gate_wait_state must be one of source, review")
	}
	if a.GateWaitTimeoutSeconds <= 0 {
		*problems = append(*problems, prefix+".gate_wait_timeout_seconds must be greater than 0")
	}
	switch normalizeAutoPromoteGateWaitTimeoutAction(a.GateWaitTimeoutAction) {
	case "", AutoPromoteGateWaitTimeoutActionMerge, AutoPromoteGateWaitTimeoutActionHumanReview:
	default:
		*problems = append(*problems, prefix+".gate_wait_timeout_action must be one of merge, human_review")
	}
	if strings.TrimSpace(a.SourceState) == "" {
		*problems = append(*problems, prefix+".source_state must not be blank")
	}
	if strings.TrimSpace(a.PassState) == "" {
		*problems = append(*problems, prefix+".pass_state must not be blank")
	}
	if strings.TrimSpace(a.ReworkState) == "" {
		*problems = append(*problems, prefix+".rework_state must not be blank")
	}
	if a.ReworkLimit < 0 {
		*problems = append(*problems, prefix+".rework_limit must be greater than or equal to 0")
	}
	if a.NoProgressLimit < 0 {
		*problems = append(*problems, prefix+".no_progress_limit must be greater than or equal to 0")
	}
}

func normalizeAutoPromoteGateWaitState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", AutoPromoteGateWaitStateSource:
		return AutoPromoteGateWaitStateSource
	case AutoPromoteGateWaitStateReview:
		return AutoPromoteGateWaitStateReview
	default:
		return strings.ToLower(strings.TrimSpace(state))
	}
}

func normalizeAutoPromoteGateWaitTimeoutAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "", AutoPromoteGateWaitTimeoutActionMerge, AutoPromoteGateWaitTimeoutActionHumanReview:
		return strings.ToLower(strings.TrimSpace(action))
	case "human-review", "humanreview":
		return AutoPromoteGateWaitTimeoutActionHumanReview
	default:
		return strings.ToLower(strings.TrimSpace(action))
	}
}

func EffectiveAutoPromoteGateWaitTimeoutAction(action string, gateCfg gate.Config) string {
	action = normalizeAutoPromoteGateWaitTimeoutAction(action)
	if action != "" {
		return action
	}
	if gate.AutomatedReviewMode(gateCfg) == gate.AutomatedReviewRequired {
		return AutoPromoteGateWaitTimeoutActionHumanReview
	}
	return AutoPromoteGateWaitTimeoutActionMerge
}

func (o OutputTruncation) validate(prefix string, problems *[]string) {
	if o.MaxBytes < 0 {
		*problems = append(*problems, prefix+".max_bytes must be greater than or equal to 0")
	}
}

func (l *Lessons) validate(prefix string, problems *[]string) {
	validateWorkspaceRelativePath(prefix+".path", l.Path, problems)
	validatePositive(prefix+".max_entries", l.MaxEntries, problems)
	if l.RecallN < 0 {
		*problems = append(*problems, prefix+".recall_n must be greater than or equal to 0")
	}
	validatePositive(prefix+".postmortem_max_tokens", l.PostmortemMaxTokens, problems)
}

func (k *Knowledge) Normalize() {
	if k == nil {
		return
	}
	if (k.Enabled || len(k.Sources) > 0) && k.MaxBytes <= 0 {
		k.MaxBytes = DefaultKnowledgeMaxBytes
	}
	for index := range k.Sources {
		k.Sources[index].Name = strings.Join(strings.Fields(k.Sources[index].Name), " ")
		k.Sources[index].Path = strings.TrimSpace(k.Sources[index].Path)
	}
}

func (k Knowledge) validate(prefix string, problems *[]string) {
	if k.Enabled || len(k.Sources) > 0 {
		validatePositive(prefix+".max_bytes", k.MaxBytes, problems)
	}
	for index, source := range k.Sources {
		sourcePrefix := prefix + ".sources[" + strconv.Itoa(index) + "]"
		if strings.TrimSpace(source.Path) == "" {
			*problems = append(*problems, sourcePrefix+".path must not be blank")
		}
		if strings.ContainsAny(source.Name, "\r\n") {
			*problems = append(*problems, sourcePrefix+".name must be a single line")
		}
		if strings.ContainsAny(source.Path, "\r\n") {
			*problems = append(*problems, sourcePrefix+".path must be a single line")
		}
	}
}

func KnowledgeWithSources(scopes ...Knowledge) Knowledge {
	out := defaultKnowledge()
	out.Sources = nil
	for _, scope := range scopes {
		if !scope.Enabled {
			continue
		}
		scope.Normalize()
		if len(scope.Sources) > 0 {
			out.Sources = append(out.Sources, scope.Sources...)
		}
		if scope.Configured || len(scope.Sources) > 0 {
			out.MaxBytes = scope.MaxBytes
		}
	}
	return out
}

func (s *Skills) validate(prefix string, problems *[]string) {
	validateWorkspaceRelativePath(prefix+".path", s.Path, problems)
	validatePositive(prefix+".max_skills_in_prompt", s.MaxSkillsInPrompt, problems)
	s.Creation.validate(prefix+".creation", problems)
}

func (s *SkillCreation) validate(prefix string, problems *[]string) {
	validatePositive(prefix+".max_drafts_per_run", s.MaxDraftsPerRun, problems)
}

func (b *Budget) validate(prefix string, problems *[]string) {
	switch b.BillingMode {
	case "", BillingModeMetered, BillingModeSubscription:
	default:
		*problems = append(*problems, prefix+".billing_mode must be one of metered, subscription")
	}
	validatePositiveFloat(prefix+".per_day_max_usd", b.PerDayMaxUSD, problems)
	validatePositiveFloat(prefix+".per_issue_max_usd", b.PerIssueMaxUSD, problems)
	if b.RefusalCooldownSeconds < 0 {
		*problems = append(*problems, prefix+".refusal_cooldown_seconds must be greater than or equal to 0")
	}
	if strings.TrimSpace(b.PricingPath) == "" {
		*problems = append(*problems, prefix+".pricing_path is required")
	}
	validatePositive(prefix+".override_max_duration_seconds", b.OverrideMaxDurationSeconds, problems)
	validatePositiveFloat(prefix+".override_max_multiplier", b.OverrideMaxMultiplier, problems)
}

func (c *Codex) validate(problems *[]string) {
	validateDeliverableElicitationRules("codex.deliverable_elicitation_allowlist", c.DeliverableElicitationAllowlist, problems)
	if strings.TrimSpace(c.Command) == "" {
		*problems = append(*problems, "codex.command is required")
	}
	validatePositive("codex.turn_timeout_ms", c.TurnTimeoutMS, problems)
	validatePositive("codex.read_timeout_ms", c.ReadTimeoutMS, problems)
	if c.StallTimeoutMS < 0 {
		*problems = append(*problems, "codex.stall_timeout_ms must be greater than or equal to 0")
	}
	if c.ModelProvider != "" && !validAgentIdentityLabel(c.ModelProvider) {
		*problems = append(*problems, "codex.model_provider must be a sanitized label containing only letters, numbers, dots, underscores, or hyphens")
	}
}

func (k *Kanban) Normalize() {
	if k == nil {
		return
	}
	k.Mode = strings.ToLower(strings.TrimSpace(k.Mode))
	if k.Mode == "" {
		k.Mode = KanbanModeReadOnly
	}
	k.AllowedTransitions = normalizeKanbanTransitions(k.AllowedTransitions)
}

func (k Kanban) Validate(prefix string) []string {
	k.Normalize()

	var problems []string
	switch k.Mode {
	case KanbanModeReadOnly, KanbanModeIntegration:
	default:
		problems = append(problems, prefix+".mode must be one of read_only, integration")
	}
	if k.IssueStateFieldID < 0 {
		problems = append(problems, prefix+".issue_state_field_id must be greater than 0 when set")
	}
	validateKanbanTransitions(prefix+".allowed_transitions", k.AllowedTransitions, &problems)
	return problems
}

func (c Config) KanbanTransitionAllowed(source string, target string) bool {
	source = strings.TrimSpace(source)
	target = strings.TrimSpace(target)
	if source == "" || target == "" {
		return false
	}
	if sameKanbanPolicyState(source, target) {
		return false
	}
	for _, allowed := range c.KanbanAllowedTransitionTargets(source) {
		if sameKanbanPolicyState(allowed, target) {
			return true
		}
	}
	return false
}

func (c Config) KanbanAllowedTransitionTargets(source string) []string {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil
	}

	states := c.KanbanStateNames()
	kanban := c.Server.Kanban
	kanban.Normalize()
	if targets, ok := kanbanTransitionTargetsForSource(kanban.AllowedTransitions, source); ok {
		return filterKanbanTransitionTargets(source, targets, states)
	}
	if len(states) == 0 {
		return nil
	}
	if c.defaultKanbanTransitionRestricted(source) {
		return filterKanbanTransitionTargets(source, defaultKanbanExceptionTargets(states), states)
	}
	return filterKanbanTransitionTargets(source, states, states)
}

func (c Config) KanbanStateNames() []string {
	states := make([]string, 0, len(c.Tracker.ObservedStates)+len(c.Tracker.ActiveStates)+len(c.Tracker.TerminalStates))
	seen := map[string]struct{}{}
	add := func(values ...string) {
		for _, value := range values {
			value = cleanKanbanPolicyState(value)
			if value == "" {
				continue
			}
			key := kanbanPolicyStateKey(value)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			states = append(states, value)
		}
	}
	add(c.Tracker.ObservedStates...)
	add(c.Tracker.ActiveStates...)
	add(c.Tracker.TerminalStates...)
	return states
}

func (s *Server) Normalize() {
	if s == nil {
		return
	}
	s.Host = strings.TrimSpace(s.Host)
	if s.BoardSnapshotStaleAfterSeconds == 0 {
		s.BoardSnapshotStaleAfterSeconds = DefaultBoardSnapshotStaleAfterSeconds
	}
	s.Kanban.Normalize()
}

func (s *Server) validate(problems *[]string) {
	if s.Port != nil && *s.Port < 0 {
		*problems = append(*problems, "server.port must be greater than or equal to 0")
	}
	if strings.TrimSpace(s.Host) == "" {
		*problems = append(*problems, "server.host is required")
	}
	validatePositive("server.board_snapshot_stale_after_seconds", s.BoardSnapshotStaleAfterSeconds, problems)
	*problems = append(*problems, s.Kanban.Validate("server.kanban")...)
}

func (o *Observability) validate(problems *[]string) {
	validatePositive("observability.refresh_ms", o.RefreshMS, problems)
	validatePositive("observability.render_interval_ms", o.RenderIntervalMS, problems)
	validatePositive("observability.stranded_active_threshold_seconds", o.StrandedActiveThresholdSeconds, problems)
	validatePositive("observability.dispatch_stall_threshold_seconds", o.DispatchStallThresholdSeconds, problems)
	validatePositive("observability.park_review_threshold", o.ParkReviewThreshold, problems)
	validatePositiveFloat("observability.efficiency.anomaly_tokens_multiple", o.Efficiency.AnomalyTokensMultiple, problems)
	validatePositiveFloat("observability.efficiency.anomaly_sessions_multiple", o.Efficiency.AnomalySessionsMultiple, problems)
	validatePositiveFloat("observability.efficiency.anomaly_dwell_multiple", o.Efficiency.AnomalyDwellMultiple, problems)
	o.OTLP.Endpoint = strings.TrimSpace(o.OTLP.Endpoint)
	o.OTLP.ServiceName = strings.TrimSpace(o.OTLP.ServiceName)
	o.Staleness.validate(problems)
	if o.OTLP.Endpoint == "" {
		return
	}
	parsed, err := url.Parse(o.OTLP.Endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		*problems = append(*problems, "observability.otlp.endpoint must be an absolute http or https URL")
	}
	validatePositive("observability.otlp.timeout_ms", o.OTLP.TimeoutMS, problems)
	if o.OTLP.ServiceName == "" {
		*problems = append(*problems, "observability.otlp.service_name is required when OTLP export is enabled")
	}
}

func (o *Observability) Normalize() {
	if o == nil {
		return
	}
	if o.ParkReviewThreshold == 0 {
		o.ParkReviewThreshold = DefaultParkReviewThreshold
	}
	o.OTLP.Endpoint = strings.TrimSpace(o.OTLP.Endpoint)
	o.OTLP.ServiceName = strings.TrimSpace(o.OTLP.ServiceName)
	o.Staleness.Normalize()
}

func (s *StalenessObservability) Normalize() {
	if s == nil {
		return
	}
	s.RepeatedDecisionBenignReasons = normalizeReasonList(s.RepeatedDecisionBenignReasons)
	for index := range s.Lanes {
		s.Lanes[index].State = strings.TrimSpace(s.Lanes[index].State)
	}
	s.Webhook.URL = strings.TrimSpace(s.Webhook.URL)
	headers := make(map[string]string, len(s.Webhook.Headers))
	for name, value := range s.Webhook.Headers {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		headers[name] = strings.TrimSpace(value)
	}
	s.Webhook.Headers = headers
}

func defaultStalenessRepeatedDecisionBenignReasons() []string {
	return []string{
		"already_running",
		"blocked_by_dependency",
		"github_rest_capacity_paused",
		"github_rest_recovery",
		"global_capacity_full",
		"outside_active_window",
		"provider_rate_window_backpressure",
		"reserved_for_higher_priority_project",
	}
}

func normalizeReasonList(reasons []string) []string {
	normalized := make([]string, 0, len(reasons))
	seen := make(map[string]struct{}, len(reasons))
	for _, reason := range reasons {
		reason = strings.ToLower(strings.TrimSpace(reason))
		if reason == "" {
			continue
		}
		if _, ok := seen[reason]; ok {
			continue
		}
		seen[reason] = struct{}{}
		normalized = append(normalized, reason)
	}
	return normalized
}

func (s StalenessObservability) validate(problems *[]string) {
	s.Lanes = slices.Clone(s.Lanes)
	s.Normalize()
	if !s.Enabled {
		return
	}
	validatePositive("observability.staleness.no_completion_hours", s.NoCompletionHours, problems)
	validatePositive("observability.staleness.no_merge_hours", s.NoMergeHours, problems)
	validatePositive("observability.staleness.human_gate_rearm_hours", s.HumanGateRearmHours, problems)
	validatePositive("observability.staleness.lane_reentry_window_hours", s.LaneReentryWindowHours, problems)
	validatePositive("observability.staleness.repeated_decision_count", s.RepeatedDecisionCount, problems)
	if s.RepeatedDecisionCount > MaxStalenessRepeatedCount {
		*problems = append(*problems, "observability.staleness.repeated_decision_count must be less than or equal to 500")
	}
	validatePositive("observability.staleness.repeated_window_hours", s.RepeatedWindowHours, problems)
	for _, reason := range s.RepeatedDecisionBenignReasons {
		if scheduler.IsEmittedDecisionReason(reason) {
			continue
		}
		*problems = append(*problems, fmt.Sprintf(
			"observability.staleness.repeated_decision_benign_reasons contains %q, which does not match a scheduler-emitted reason; emitted reasons include %q",
			reason,
			scheduler.EmittedDecisionReasonExamples(),
		))
	}
	seen := make(map[string]struct{}, len(s.Lanes))
	for index, lane := range s.Lanes {
		prefix := fmt.Sprintf("observability.staleness.lanes[%d]", index)
		if lane.State == "" {
			*problems = append(*problems, prefix+".state is required")
		}
		key := strings.ToLower(lane.State)
		if _, ok := seen[key]; key != "" && ok {
			*problems = append(*problems, prefix+".state must be unique")
		}
		seen[key] = struct{}{}
		validatePositive(prefix+".threshold_hours", lane.ThresholdHours, problems)
	}
	if s.Webhook.URL == "" {
		return
	}
	parsed, err := url.Parse(s.Webhook.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		*problems = append(*problems, "observability.staleness.webhook.url must be an absolute http or https URL")
	}
	validatePositive("observability.staleness.webhook.timeout_ms", s.Webhook.TimeoutMS, problems)
}

func (h *Hooks) validate(problems *[]string) {
	validatePositive("hooks.timeout_ms", h.TimeoutMS, problems)
}

func splitFrontmatter(raw []byte) ([]byte, []byte, error) {
	normalized := strings.ReplaceAll(strings.TrimPrefix(string(raw), "\ufeff"), "\r\n", "\n")
	if !strings.HasPrefix(normalized, "---\n") {
		return nil, nil, errors.New("missing YAML frontmatter")
	}

	body := normalized[len("---\n"):]
	if strings.HasPrefix(body, "---\n") {
		return []byte{}, []byte(body[len("---\n"):]), nil
	}
	if body == "---" {
		return []byte{}, []byte{}, nil
	}

	before, after, ok := strings.Cut(body, "\n---\n")
	if ok {
		return []byte(before), []byte(after), nil
	}

	if before, ok := strings.CutSuffix(body, "\n---"); ok {
		return []byte(before), []byte{}, nil
	}

	return nil, nil, errors.New("unterminated YAML frontmatter")
}

func documentRoot(doc *yaml.Node) (*yaml.Node, error) {
	if doc.Kind == 0 {
		return &yaml.Node{Kind: yaml.MappingNode}, nil
	}
	if doc.Kind == yaml.DocumentNode {
		if len(doc.Content) == 0 {
			return &yaml.Node{Kind: yaml.MappingNode}, nil
		}
		return doc.Content[0], nil
	}
	return nil, errors.New("workflow frontmatter must be a YAML document")
}

func normalizeTrackerIDFields(root *yaml.Node) {
	tracker := mappingValue(root, "tracker")
	if tracker == nil || tracker.Kind != yaml.MappingNode {
		return
	}

	normalizeScalarField(tracker, "github_app_id")
	normalizeScalarField(tracker, "github_app_installation_id")
}

func trackerFieldSet(root *yaml.Node, key string) bool {
	tracker := mappingValue(root, "tracker")
	if tracker == nil || tracker.Kind != yaml.MappingNode {
		return false
	}
	return mappingValue(tracker, key) != nil
}

func nestedFieldSet(root *yaml.Node, keys ...string) bool {
	current := root
	for _, key := range keys {
		if current == nil || current.Kind != yaml.MappingNode {
			return false
		}
		current = mappingValue(current, key)
	}
	return current != nil
}

func configuredFieldPaths(root *yaml.Node) map[string]struct{} {
	paths := map[string]struct{}{}
	activeAliases := map[*yaml.Node]bool{}
	var collect func(*yaml.Node, string)
	collect = func(node *yaml.Node, prefix string) {
		if node != nil && node.Kind == yaml.AliasNode {
			if node.Alias == nil || activeAliases[node.Alias] {
				return
			}
			if node.Alias.Kind != yaml.MappingNode && node.Alias.Kind != yaml.AliasNode {
				if prefix != "" {
					paths[prefix] = struct{}{}
				}
				return
			}
			activeAliases[node.Alias] = true
			collect(node.Alias, prefix)
			delete(activeAliases, node.Alias)
			return
		}
		if node == nil || node.Kind != yaml.MappingNode {
			return
		}
		for index := 0; index+1 < len(node.Content); index += 2 {
			key := strings.TrimSpace(node.Content[index].Value)
			if key == "" {
				continue
			}
			value := node.Content[index+1]
			if key == "<<" {
				if value.Kind == yaml.SequenceNode {
					for _, merged := range value.Content {
						collect(merged, prefix)
					}
				} else {
					collect(value, prefix)
				}
				continue
			}
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			if value.Kind == yaml.MappingNode || value.Kind == yaml.AliasNode {
				collect(value, path)
				continue
			}
			paths[path] = struct{}{}
		}
	}
	collect(root, "")
	return paths
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node.Kind != yaml.MappingNode {
		return nil
	}

	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}

	return nil
}

func normalizeScalarField(node *yaml.Node, key string) {
	value := mappingValue(node, key)
	if value == nil || value.Kind != yaml.ScalarNode || value.Tag == "!!null" {
		return
	}

	value.Tag = "!!str"
}

func decodeMapNode(node *yaml.Node) (map[string]any, error) {
	out := make(map[string]any, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		if keyNode.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("map keys must be scalars, got %s", yamlKindName(keyNode))
		}

		value, err := decodeAnyNode(node.Content[i+1])
		if err != nil {
			return nil, err
		}
		out[keyNode.Value] = value
	}

	return out, nil
}

func decodeAnyNode(node *yaml.Node) (any, error) {
	switch node.Kind {
	case yaml.ScalarNode:
		return decodeScalarNode(node)
	case yaml.SequenceNode:
		out := make([]any, 0, len(node.Content))
		for _, child := range node.Content {
			value, err := decodeAnyNode(child)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case yaml.MappingNode:
		return decodeMapNode(node)
	default:
		return nil, fmt.Errorf("unsupported YAML node %s", yamlKindName(node))
	}
}

func decodeScalarNode(node *yaml.Node) (any, error) {
	switch node.Tag {
	case "!!null":
		return nil, nil //nolint:nilnil // YAML null decodes to a nil scalar value with no error.
	case "!!bool":
		return strconv.ParseBool(node.Value)
	case "!!int":
		value, err := strconv.ParseInt(strings.ReplaceAll(node.Value, "_", ""), 0, 64)
		if err != nil {
			return nil, err
		}
		return int(value), nil
	case "!!float":
		return strconv.ParseFloat(strings.ReplaceAll(node.Value, "_", ""), 64)
	default:
		return node.Value, nil
	}
}

func yamlKindName(node *yaml.Node) string {
	if node.Tag != "" {
		return node.Tag
	}

	switch node.Kind {
	case yaml.DocumentNode:
		return "document"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.MappingNode:
		return "mapping"
	case yaml.ScalarNode:
		return "scalar"
	case yaml.AliasNode:
		return "alias"
	default:
		return "unknown"
	}
}

func defaultBudget() Budget {
	return Budget{
		PerDayMaxUSD:               50.0,
		PerIssueMaxUSD:             5.0,
		RefusalCooldownSeconds:     3600,
		PricingPath:                "priv/pricing/models.yaml",
		OverrideMaxDurationSeconds: 48 * 60 * 60,
		OverrideMaxMultiplier:      3,
	}
}

func defaultLessons() Lessons {
	return Lessons{
		Path:                ".detent/lessons.md",
		MaxEntries:          50,
		RecallN:             10,
		PostmortemMaxTokens: 1024,
	}
}

func defaultKnowledge() Knowledge {
	return Knowledge{
		Enabled:  true,
		MaxBytes: DefaultKnowledgeMaxBytes,
	}
}

func defaultSkills() Skills {
	return Skills{
		Enabled:           true,
		Path:              ".detent/skills",
		MaxSkillsInPrompt: 50,
		Creation: SkillCreation{
			Enabled:         true,
			MaxDraftsPerRun: 1,
		},
	}
}

func defaultPriorityMap() map[string]any {
	return map[string]any{
		"Urgent":      1,
		"High":        2,
		"Medium":      3,
		"Low":         4,
		"No priority": nil,
	}
}

func normalizeStateLimits(limits map[string]int) map[string]int {
	if limits == nil {
		return map[string]int{}
	}

	normalized := make(map[string]int, len(limits))
	for state, limit := range limits {
		normalized[normalizeIssueState(state)] = limit
	}
	return normalized
}

func normalizeStateList(states []string) []string {
	if states == nil {
		return []string{}
	}

	normalized := make([]string, 0, len(states))
	for _, state := range states {
		normalized = append(normalized, normalizeIssueState(state))
	}
	return normalized
}

func normalizeIssueState(state string) string {
	return strings.ToLower(strings.TrimSpace(state))
}

func stateListContains(states []string, want string) bool {
	want = normalizeIssueState(want)
	for _, state := range states {
		if normalizeIssueState(state) == want {
			return true
		}
	}
	return false
}

func appendStateUnique(states []string, state string) []string {
	state = strings.TrimSpace(state)
	if state == "" || stateListContains(states, state) {
		return states
	}
	return append(states, state)
}

func normalizeLabel(label string) string {
	return strings.ToLower(strings.TrimSpace(label))
}

func normalizeLabels(labels []string) []string {
	if labels == nil {
		return []string{}
	}

	normalized := make([]string, 0, len(labels))
	seen := map[string]struct{}{}
	for _, label := range labels {
		candidate := normalizeLabel(label)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		normalized = append(normalized, candidate)
	}
	return normalized
}

func validateRequired(field string, value string, suffix string, problems *[]string) {
	if strings.TrimSpace(value) == "" {
		*problems = append(*problems, field+" is required"+suffix)
	}
}

func validatePositive(field string, value int, problems *[]string) {
	if value <= 0 {
		*problems = append(*problems, field+" must be greater than 0")
	}
}

func validateNonNegative(field string, value int, problems *[]string) {
	if value < 0 {
		*problems = append(*problems, field+" must be greater than or equal to 0")
	}
}

func validatePollingInterval(value int, problems *[]string) {
	if value <= 0 {
		*problems = append(*problems, "polling.interval_ms must be greater than 0")
		return
	}
	if value < MinPollingIntervalMS {
		*problems = append(*problems, fmt.Sprintf("polling.interval_ms must be at least %d", MinPollingIntervalMS))
	}
}

func validatePositiveFloat(field string, value float64, problems *[]string) {
	if value <= 0 {
		*problems = append(*problems, field+" must be greater than 0")
	}
}

func validateStateList(field string, states []string, problems *[]string) {
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		if strings.TrimSpace(state) == "" {
			*problems = append(*problems, field+" state names must not be blank")
			return
		}
		normalized := normalizeIssueState(state)
		if _, ok := seen[normalized]; ok {
			*problems = append(*problems, field+" state names must be unique")
			return
		}
		seen[normalized] = struct{}{}
	}
}

func validateInstructionsByState(field string, instructions map[string]string, configuredStates map[string]string, problems *[]string) {
	blank := false
	duplicate := false
	seen := make(map[string]struct{}, len(instructions))
	for state := range instructions {
		normalized := normalizeIssueState(state)
		if normalized == "" {
			if !blank {
				*problems = append(*problems, field+" state names must not be blank")
				blank = true
			}
			continue
		}
		if _, ok := seen[normalized]; ok {
			if !duplicate {
				*problems = append(*problems, field+" state names must be unique")
				duplicate = true
			}
			continue
		}
		seen[normalized] = struct{}{}
		if _, ok := configuredStates[normalized]; !ok {
			*problems = append(*problems, field+" state "+strconv.Quote(strings.TrimSpace(state))+" must reference a configured workflow state")
		}
	}
}

func validateInstructionsByTransition(
	field string,
	instructions map[string]map[string]string,
	configuredStates map[string]string,
	problems *[]string,
) {
	sourceBlank := false
	sourceDuplicate := false
	targetBlank := false
	targetDuplicate := false
	seenSources := make(map[string]struct{}, len(instructions))
	for source, targets := range instructions {
		normalizedSource := normalizeIssueState(source)
		if normalizedSource == "" {
			if !sourceBlank {
				*problems = append(*problems, field+" source states must not be blank")
				sourceBlank = true
			}
			continue
		}
		if _, ok := seenSources[normalizedSource]; ok {
			if !sourceDuplicate {
				*problems = append(*problems, field+" source states must be unique")
				sourceDuplicate = true
			}
		} else {
			seenSources[normalizedSource] = struct{}{}
		}
		if _, ok := configuredStates[normalizedSource]; !ok {
			*problems = append(*problems, field+" source state "+strconv.Quote(strings.TrimSpace(source))+" must reference a configured workflow state")
		}

		seenTargets := make(map[string]struct{}, len(targets))
		for target := range targets {
			normalizedTarget := normalizeIssueState(target)
			if normalizedTarget == "" {
				if !targetBlank {
					*problems = append(*problems, field+" target states must not be blank")
					targetBlank = true
				}
				continue
			}
			if _, ok := seenTargets[normalizedTarget]; ok {
				if !targetDuplicate {
					*problems = append(*problems, field+" target states must be unique per source")
					targetDuplicate = true
				}
				continue
			}
			seenTargets[normalizedTarget] = struct{}{}
			if _, ok := configuredStates[normalizedTarget]; !ok {
				*problems = append(*problems, field+" target state "+strconv.Quote(strings.TrimSpace(target))+" must reference a configured workflow state")
			}
		}
	}
}

func validateLabelList(field string, labels []string, problems *[]string) {
	for _, label := range labels {
		if strings.TrimSpace(label) == "" {
			*problems = append(*problems, field+" labels must not be blank")
			return
		}
	}
}

func validateStateLimits(field string, limits map[string]int, problems *[]string) {
	for state, limit := range limits {
		if strings.TrimSpace(state) == "" {
			*problems = append(*problems, field+" state names must not be blank")
			return
		}
		if limit <= 0 {
			*problems = append(*problems, field+" limits must be positive integers")
			return
		}
	}
}

func validateKanbanTransitions(field string, transitions map[string][]string, problems *[]string) {
	sourceBlank := false
	targetBlank := false
	for source, targets := range transitions {
		if strings.TrimSpace(source) == "" {
			if !sourceBlank {
				*problems = append(*problems, field+" source states must not be blank")
				sourceBlank = true
			}
			continue
		}
		seenTargets := map[string]struct{}{}
		for _, target := range targets {
			if strings.TrimSpace(target) == "" {
				if !targetBlank {
					*problems = append(*problems, field+" target states must not be blank")
					targetBlank = true
				}
				continue
			}
			key := kanbanPolicyStateKey(target)
			if _, ok := seenTargets[key]; ok {
				*problems = append(*problems, field+" target states must be unique per source")
				return
			}
			seenTargets[key] = struct{}{}
		}
	}
}

func validateStateMap(field string, value StringOrMap, problems *[]string) {
	if !value.IsMap {
		return
	}
	for state, mapped := range value.Map {
		if strings.TrimSpace(state) == "" {
			*problems = append(*problems, field+" state names must not be blank")
			return
		}
		if _, ok := mapped.(string); !ok {
			*problems = append(*problems, field+" values must be strings")
			return
		}
	}
}

func validatePriorityMap(field string, value StringOrMap, problems *[]string) {
	if !value.IsMap {
		return
	}
	if len(value.Map) == 0 {
		*problems = append(*problems, field+" must not be empty")
		return
	}

	for name, rank := range value.Map {
		if strings.TrimSpace(name) == "" {
			*problems = append(*problems, field+" option names must not be blank")
		}
		if !validPriorityRank(rank) {
			*problems = append(*problems, field+" ranks must be integers 1 through 4 or null")
		}
	}
}

func validPriorityRank(value any) bool {
	switch rank := value.(type) {
	case nil:
		return true
	case int:
		return rank >= 1 && rank <= 4
	default:
		return false
	}
}

func validateWorkspaceRelativePath(field string, path string, problems *[]string) {
	if !pathsafe.IsWorkspaceRelative(path) {
		*problems = append(*problems, field+" must be a relative path inside the workspace")
	}
}

func normalizeKanbanTransitions(transitions map[string][]string) map[string][]string {
	if transitions == nil {
		return nil
	}

	normalized := make(map[string][]string, len(transitions))
	targetKeysBySource := make(map[string]map[string]struct{}, len(transitions))
	displaySourceByKey := make(map[string]string, len(transitions))
	for source, targets := range transitions {
		source = cleanKanbanPolicyState(source)
		sourceKey := kanbanPolicyStateKey(source)
		displaySource := source
		if existing, ok := displaySourceByKey[sourceKey]; ok {
			displaySource = existing
		} else {
			displaySourceByKey[sourceKey] = displaySource
		}
		if _, ok := targetKeysBySource[sourceKey]; !ok {
			targetKeysBySource[sourceKey] = map[string]struct{}{}
		}
		for _, target := range targets {
			target = cleanKanbanPolicyState(target)
			targetKey := kanbanPolicyStateKey(target)
			if _, ok := targetKeysBySource[sourceKey][targetKey]; ok {
				continue
			}
			targetKeysBySource[sourceKey][targetKey] = struct{}{}
			normalized[displaySource] = append(normalized[displaySource], target)
		}
		if _, ok := normalized[displaySource]; !ok {
			normalized[displaySource] = []string{}
		}
	}
	return normalized
}

func (c Config) defaultKanbanTransitionRestricted(source string) bool {
	if defaultKanbanExecutionState(source) {
		return true
	}
	for _, active := range c.Tracker.ActiveStates {
		if !sameKanbanPolicyState(active, source) {
			continue
		}
		return !sameKanbanPolicyState(active, "Todo")
	}
	return false
}

func defaultKanbanExecutionState(state string) bool {
	switch kanbanPolicyStateKey(state) {
	case "in progress", "rework", "merging":
		return true
	default:
		return false
	}
}

func defaultKanbanExceptionTargets(states []string) []string {
	targets := make([]string, 0, 2)
	for _, state := range states {
		switch kanbanPolicyStateKey(state) {
		case "blocked", "cancelled", "canceled":
			targets = append(targets, state)
		}
	}
	return targets
}

func kanbanTransitionTargetsForSource(transitions map[string][]string, source string) ([]string, bool) {
	sourceKey := kanbanPolicyStateKey(source)
	for configuredSource, targets := range transitions {
		if kanbanPolicyStateKey(configuredSource) == sourceKey {
			return append([]string(nil), targets...), true
		}
	}
	return nil, false
}

func filterKanbanTransitionTargets(source string, targets []string, states []string) []string {
	stateDisplayByKey := make(map[string]string, len(states))
	for _, state := range states {
		state = cleanKanbanPolicyState(state)
		if state == "" {
			continue
		}
		key := kanbanPolicyStateKey(state)
		if _, ok := stateDisplayByKey[key]; !ok {
			stateDisplayByKey[key] = state
		}
	}

	allowed := make([]string, 0, len(targets))
	seen := map[string]struct{}{}
	for _, target := range targets {
		key := kanbanPolicyStateKey(target)
		if key == "" || sameKanbanPolicyState(source, target) {
			continue
		}
		display := cleanKanbanPolicyState(target)
		if len(stateDisplayByKey) > 0 {
			var ok bool
			display, ok = stateDisplayByKey[key]
			if !ok {
				continue
			}
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		allowed = append(allowed, display)
	}
	return allowed
}

func sameKanbanPolicyState(left string, right string) bool {
	return kanbanPolicyStateKey(left) == kanbanPolicyStateKey(right)
}

func cleanKanbanPolicyState(state string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(state)), " ")
}

func kanbanPolicyStateKey(state string) string {
	return strings.ToLower(cleanKanbanPolicyState(state))
}
