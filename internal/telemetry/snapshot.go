package telemetry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/activehours"
	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
)

type Snapshot struct {
	LastKnown               bool                `json:"-"`
	LastKnownUntil          time.Time           `json:"-"`
	Tracker                 SnapshotSection     `json:"tracker,omitzero"`
	Runtime                 SnapshotSection     `json:"runtime,omitzero"`
	Seq                     uint64              `json:"seq,omitempty"`
	GeneratedAt             time.Time           `json:"generated_at"`
	Project                 Project             `json:"project"`
	Instance                Instance            `json:"instance"`
	Projects                []ProjectSnapshot   `json:"projects,omitempty"`
	AgentPools              []AgentPool         `json:"agent_pools,omitempty"`
	DashboardURL            string              `json:"dashboard_url,omitempty"`
	Auth                    AuthHealth          `json:"auth,omitzero"`
	Shutdown                Shutdown            `json:"shutdown"`
	Update                  Update              `json:"update,omitzero"`
	Refresh                 Refresh             `json:"refresh"`
	Events                  []ActivityEvent     `json:"events,omitempty"`
	Counts                  Counts              `json:"counts"`
	TrackerDrift            TrackerDrift        `json:"tracker_drift,omitzero"`
	BoardIssues             []Issue             `json:"board_issues,omitempty"`
	Pipeline                []Issue             `json:"pipeline,omitempty"`
	Running                 []Running           `json:"running"`
	WorkAttempts            []WorkAttempt       `json:"work_attempts,omitempty"`
	SchedulerDecisions      []SchedulerDecision `json:"scheduler_decisions,omitempty"`
	Dispatch                DispatchStatus      `json:"dispatch"`
	DispatchStalls          []DispatchStatus    `json:"dispatch_stalls,omitempty"`
	Release                 Release             `json:"release,omitzero"`
	Releases                []Release           `json:"releases,omitempty"`
	Queue                   []Queued            `json:"queue"`
	Blocked                 []Blocked           `json:"blocked"`
	Completed               []Completed         `json:"completed"`
	Budget                  Budget              `json:"budget"`
	RateLimits              *RateLimits         `json:"rate_limits"`
	TrackerUnavailable      []TrackerCondition  `json:"tracker_unavailable,omitempty"`
	ForgeUnavailable        []ForgeCondition    `json:"forge_unavailable,omitempty"`
	GitHubMonitors          []GitHubMonitor     `json:"worker_github_budget_monitor_unavailable,omitempty"`
	CIUnavailable           []CICondition       `json:"ci_unavailable,omitempty"`
	BackendOutages          []BackendOutage     `json:"backend_outages,omitempty"`
	FailureBreakers         []FailureBreaker    `json:"failure_breakers,omitempty"`
	DispatchLoops           []DispatchLoop      `json:"dispatch_loops,omitempty"`
	DispatchRecoveries      []DispatchRecovery  `json:"dispatch_recoveries,omitempty"`
	StalenessWarnings       []StalenessWarning  `json:"staleness_warnings,omitempty"`
	StrandedActiveIssues    []StrandedIssue     `json:"stranded_active_issues,omitempty"`
	CleanupFaults           []CleanupFault      `json:"workspace_cleanup_failures,omitempty"`
	AdmissionProposals      []AdmissionProposal `json:"admission_proposals_awaiting_decision,omitempty"`
	OverloadRetriesLastHour int                 `json:"overload_retries_last_hour,omitempty"`
	Tokens                  Tokens              `json:"tokens"`
	Throughput              TokenThroughput     `json:"throughput"`
	Concurrency             ConcurrencyHistory  `json:"concurrency"`
	LifetimeTotals          LifetimeTotals      `json:"lifetime_totals"`
	MemoryPressure          MemoryPressure      `json:"memory_pressure"`
	IOPressure              IOPressure          `json:"io_pressure"`
	CPUPressure             CPUPressure         `json:"cpu_pressure"`
	CycleTime               CycleTimeReport     `json:"cycle_time"`
	WorkflowMetrics         WorkflowMetrics     `json:"workflow_metrics"`
	TokenTrend              []TokenTrendPoint   `json:"token_trend,omitempty"`
}

type MemoryPressure struct {
	Supported    bool             `json:"supported"`
	Some         PressureAverages `json:"some"`
	Full         PressureAverages `json:"full"`
	SomeAvg60Max float64          `json:"some_avg60_max"`
	DispatchHeld bool             `json:"dispatch_held"`
	ObservedAt   time.Time        `json:"observed_at,omitzero"`
	LastError    string           `json:"last_error,omitempty"`
}

type IOPressure struct {
	Supported                    bool             `json:"supported"`
	Some                         PressureAverages `json:"some"`
	Full                         PressureAverages `json:"full"`
	FullAvg10Max                 float64          `json:"full_avg10_max"`
	DegradedMaxConcurrentAgents  int              `json:"degraded_max_concurrent_agents"`
	EffectiveMaxConcurrentAgents int              `json:"effective_max_concurrent_agents"`
	CapacityConstrained          bool             `json:"capacity_constrained"`
	DispatchHeld                 bool             `json:"dispatch_held"`
	ConstrainedSince             time.Time        `json:"constrained_since,omitzero"`
	ConstrainedForMS             int64            `json:"constrained_for_ms"`
	ObservedAt                   time.Time        `json:"observed_at,omitzero"`
	LastError                    string           `json:"last_error,omitempty"`
}

type CPUPressure struct {
	Supported                    bool             `json:"supported"`
	Some                         PressureAverages `json:"some"`
	Full                         PressureAverages `json:"full"`
	SomeAvg10Max                 float64          `json:"some_avg10_max"`
	DegradedMaxConcurrentAgents  int              `json:"degraded_max_concurrent_agents"`
	EffectiveMaxConcurrentAgents int              `json:"effective_max_concurrent_agents"`
	CapacityConstrained          bool             `json:"capacity_constrained"`
	DispatchHeld                 bool             `json:"dispatch_held"`
	ConstrainedSince             time.Time        `json:"constrained_since,omitzero"`
	ConstrainedForMS             int64            `json:"constrained_for_ms"`
	ObservedAt                   time.Time        `json:"observed_at,omitzero"`
	LastError                    string           `json:"last_error,omitempty"`
}

type PressureAverages struct {
	Avg10  float64 `json:"avg10"`
	Avg60  float64 `json:"avg60"`
	Avg300 float64 `json:"avg300"`
	Total  uint64  `json:"total"`
}

type OrphanedAgentProcesses struct {
	Count         int                    `json:"count"`
	SessionCount  int                    `json:"session_count"`
	TotalRSSBytes int64                  `json:"total_rss_bytes"`
	Processes     []OrphanedAgentProcess `json:"processes,omitempty"`
}

type OrphanedAgentProcess struct {
	SessionID    int64     `json:"session_id"`
	IssueID      string    `json:"issue_id,omitempty"`
	Identifier   string    `json:"identifier,omitempty"`
	PID          int       `json:"pid"`
	GroupID      int       `json:"pgid,omitempty"`
	StartedAt    time.Time `json:"started_at"`
	AgeSeconds   int64     `json:"age_seconds"`
	RSSBytes     int64     `json:"rss_bytes"`
	ProcessCount int       `json:"process_count"`
	FinalState   string    `json:"final_state,omitempty"`
}

type CleanupFault struct {
	ProjectID         string    `json:"project_id,omitempty"`
	AffectedPathCount int       `json:"affected_path_count"`
	LastError         string    `json:"last_error"`
	ObservedAt        time.Time `json:"observed_at"`
}

func (s Snapshot) AgeSeconds(now time.Time) int64 {
	if s.GeneratedAt.IsZero() || now.IsZero() || now.Before(s.GeneratedAt) {
		return 0
	}
	return int64(now.Sub(s.GeneratedAt) / time.Second)
}

func (s Snapshot) WithFreshness(now time.Time) Snapshot {
	if len(s.Projects) == 0 {
		s.Refresh = s.Refresh.WithFreshness(now)
		return s
	}
	s.Projects = append([]ProjectSnapshot(nil), s.Projects...)
	partial := s.Refresh.Partial()
	partialLastError := s.Refresh.LastError
	partialLastErrorAt := copyTimePointer(s.Refresh.LastErrorAt)
	staleAfterSeconds, observedSweepSeconds := refreshFleetStaleAfter(s.Refresh, s.Projects)
	if staleAfterSeconds > s.Refresh.StaleAfterSeconds {
		s.Refresh.StaleAfterSeconds = staleAfterSeconds
	}
	s.Refresh.ObservedSweepSeconds = observedSweepSeconds
	s.Refresh = s.Refresh.WithFreshness(now)
	for index := range s.Projects {
		if !refreshHasReadinessSignal(s.Projects[index].Refresh) {
			continue
		}
		s.Projects[index].Refresh = s.Projects[index].Refresh.WithFreshness(now)
	}
	if partial && hasReadyProjectRefresh(s.Projects) {
		s.Refresh.Status = RefreshStatusPartial
		s.Refresh.StalenessWindowExceeded = false
		s.Refresh.LastError = partialLastError
		s.Refresh.LastErrorAt = partialLastErrorAt
	}
	return s
}

func hasReadyProjectRefresh(projects []ProjectSnapshot) bool {
	for _, project := range projects {
		if project.Refresh.Ready() {
			return true
		}
	}
	return false
}

func refreshFleetStaleAfter(fleet Refresh, projects []ProjectSnapshot) (int64, int64) {
	staleAfterSeconds := fleet.StaleAfterSeconds
	var observedDurationSeconds int64
	var activeProjectCount int64
	var knownDurationCount int64
	for _, project := range projects {
		refresh := project.Refresh
		if !refreshHasReadinessSignal(refresh) {
			continue
		}
		activeProjectCount++
		if refresh.StaleAfterSeconds > staleAfterSeconds {
			staleAfterSeconds = refresh.StaleAfterSeconds
		}
		if refresh.LastDurationSeconds > 0 {
			observedDurationSeconds += refresh.LastDurationSeconds
			knownDurationCount++
		}
	}
	if activeProjectCount < 2 || knownDurationCount == 0 {
		return staleAfterSeconds, observedDurationSeconds
	}
	unknownDurationCount := activeProjectCount - knownDurationCount
	if unknownDurationCount > 0 {
		observedDurationSeconds += ((observedDurationSeconds + knownDurationCount - 1) / knownDurationCount) * unknownDurationCount
	}
	adaptiveStaleAfterSeconds := refreshSweepHeadroomMultiplier * observedDurationSeconds
	if adaptiveStaleAfterSeconds > staleAfterSeconds {
		staleAfterSeconds = adaptiveStaleAfterSeconds
	}
	return staleAfterSeconds, observedDurationSeconds
}

type CICondition struct {
	ProjectID           string    `json:"project_id,omitempty"`
	UnstartedCheckCount int       `json:"unstarted_check_count"`
	PullRequestCount    int       `json:"pull_request_count"`
	OldestQueueSeconds  int64     `json:"oldest_queue_seconds"`
	DetectedAt          time.Time `json:"detected_at"`
	LastObservedAt      time.Time `json:"last_observed_at"`
	ParkedAttemptCount  int       `json:"parked_attempt_count,omitempty"`
}

type TrackerCondition struct {
	ProjectID          string            `json:"project_id,omitempty"`
	Connector          string            `json:"connector"`
	ConnectorInstance  string            `json:"connector_instance"`
	Endpoint           string            `json:"endpoint,omitempty"`
	Operation          string            `json:"operation,omitempty"`
	ErrorClass         string            `json:"error_class"`
	CredentialIdentity string            `json:"credential_identity,omitempty"`
	RefreshSource      RefreshSourceName `json:"refresh_source,omitempty"`
	DetectedAt         time.Time         `json:"detected_at"`
	LastObservedAt     time.Time         `json:"last_observed_at"`
	NextProbeAt        time.Time         `json:"next_probe_at,omitzero"`
	LastProbeAt        time.Time         `json:"last_probe_at,omitzero"`
	LastProbeResult    string            `json:"last_probe_result,omitempty"`
	LastProbeDetail    string            `json:"last_probe_detail,omitempty"`
	ProbeAttempts      int               `json:"probe_attempts,omitempty"`
	LastError          string            `json:"last_error,omitempty"`
	ProviderStatus     *ProviderStatus   `json:"provider_status,omitempty"`
}

const (
	ProviderStatusPending      = "pending"
	ProviderStatusCorroborated = "corroborated"
	ProviderStatusNoMatch      = "no_matching_incident"
	ProviderStatusUnavailable  = "unavailable"
)

type ProviderStatus struct {
	Provider  string            `json:"provider"`
	SourceURL string            `json:"source_url"`
	State     string            `json:"state"`
	CheckedAt time.Time         `json:"checked_at,omitzero"`
	Incident  *ProviderIncident `json:"incident,omitempty"`
}

type ProviderIncident struct {
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Status     string    `json:"status"`
	Impact     string    `json:"impact,omitempty"`
	Components []string  `json:"components,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ForgeCondition struct {
	ProjectID       string    `json:"project_id,omitempty"`
	Host            string    `json:"host"`
	Operation       string    `json:"operation,omitempty"`
	ErrorClass      string    `json:"error_class"`
	DetectedAt      time.Time `json:"detected_at"`
	LastObservedAt  time.Time `json:"last_observed_at"`
	NextProbeAt     time.Time `json:"next_probe_at,omitzero"`
	LastProbeAt     time.Time `json:"last_probe_at,omitzero"`
	LastProbeResult string    `json:"last_probe_result,omitempty"`
	LastProbeDetail string    `json:"last_probe_detail,omitempty"`
	ProbeAttempts   int       `json:"probe_attempts,omitempty"`
	ProbeIssueID    string    `json:"probe_issue_id,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

type GitHubMonitor struct {
	ProjectID          string    `json:"project_id,omitempty"`
	CredentialIdentity string    `json:"credential_identity"`
	Consumer           string    `json:"consumer"`
	Operation          string    `json:"operation,omitempty"`
	DetectedAt         time.Time `json:"detected_at"`
	LastObservedAt     time.Time `json:"last_observed_at"`
	NextProbeAt        time.Time `json:"next_probe_at"`
	LastProbeAt        time.Time `json:"last_probe_at,omitzero"`
	LastProbeResult    string    `json:"last_probe_result,omitempty"`
	LastProbeDetail    string    `json:"last_probe_detail,omitempty"`
	ProbeAttempts      int       `json:"probe_attempts,omitempty"`
	ProbeIssueID       string    `json:"probe_issue_id,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
}

type AdmissionProposal struct {
	ID              string    `json:"id"`
	ProjectID       string    `json:"project_id"`
	IssueID         string    `json:"issue_id"`
	IssueIdentifier string    `json:"issue_identifier,omitempty"`
	IssueURL        string    `json:"issue_url,omitempty"`
	Confidence      float64   `json:"confidence"`
	CreatedAt       time.Time `json:"created_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

type FailureBreaker struct {
	ProjectID              string               `json:"project_id,omitempty"`
	Class                  string               `json:"class"`
	Count                  int                  `json:"count"`
	AttemptCount           int                  `json:"attempt_count"`
	DistinctItemCount      int                  `json:"distinct_item_count"`
	Cause                  string               `json:"cause,omitempty"`
	RepresentativeError    string               `json:"representative_error,omitempty"`
	BackendID              string               `json:"backend_id,omitempty"`
	BackendKind            string               `json:"backend_kind,omitempty"`
	Provider               string               `json:"provider,omitempty"`
	EligibleCandidateCount *int                 `json:"eligible_candidate_count,omitempty"`
	Items                  []FailureBreakerItem `json:"items,omitempty"`
	BackendOutage          *BackendOutage       `json:"backend_outage,omitempty"`
	WindowSeconds          int64                `json:"window_seconds"`
	CooldownSeconds        int64                `json:"cooldown_seconds"`
	FirstFailureAt         time.Time            `json:"first_failure_at"`
	TrippedAt              time.Time            `json:"tripped_at"`
	ResumeAt               time.Time            `json:"resume_at"`
	CanaryIssueID          string               `json:"canary_issue_id,omitempty"`
}

type FailureBreakerItem struct {
	IssueID                 string `json:"issue_id"`
	Identifier              string `json:"identifier,omitempty"`
	IssueURL                string `json:"issue_url,omitempty"`
	Title                   string `json:"title,omitempty"`
	CurrentState            string `json:"current_state,omitempty"`
	AttemptCount            int    `json:"attempt_count"`
	Parked                  bool   `json:"parked"`
	RecoveryAction          string `json:"recovery_action,omitempty"`
	RecoveryReason          string `json:"recovery_reason,omitempty"`
	RecoveryIntentResumable bool   `json:"recovery_intent_resumable,omitempty"`
}

type DispatchLoop struct {
	ProjectID             string     `json:"project_id,omitempty"`
	IssueID               string     `json:"issue_id,omitempty"`
	Identifier            string     `json:"identifier,omitempty"`
	IssueURL              string     `json:"issue_url,omitempty"`
	Title                 string     `json:"title,omitempty"`
	Lane                  string     `json:"lane,omitempty"`
	ConsecutiveDispatches int        `json:"consecutive_dispatches"`
	DispatchLimit         int        `json:"dispatch_limit"`
	Tripped               bool       `json:"tripped"`
	LastCompletedAt       *time.Time `json:"last_completed_at,omitempty"`
}

type DispatchRecovery struct {
	ProjectID     string    `json:"project_id,omitempty"`
	Pool          string    `json:"pool,omitempty"`
	Kind          string    `json:"kind"`
	Reason        string    `json:"reason,omitempty"`
	Status        string    `json:"status"`
	StartedAt     time.Time `json:"started_at"`
	ResumeAt      time.Time `json:"resume_at"`
	Limit         int       `json:"limit"`
	MaxConcurrent int       `json:"max_concurrent"`
	Admitted      int       `json:"admitted"`
	Progressed    int       `json:"progressed"`
}

type StalenessWarning struct {
	ID                    string              `json:"id"`
	Class                 observability.Class `json:"class"`
	ProjectID             string              `json:"project_id,omitempty"`
	Kind                  string              `json:"kind"`
	IssueID               string              `json:"issue_id,omitempty"`
	Identifier            string              `json:"identifier,omitempty"`
	IssueURL              string              `json:"issue_url,omitempty"`
	Title                 string              `json:"title,omitempty"`
	Lane                  string              `json:"lane,omitempty"`
	Reason                string              `json:"reason"`
	Detail                string              `json:"detail"`
	Since                 time.Time           `json:"since"`
	DetectedAt            time.Time           `json:"detected_at"`
	LastObservedAt        time.Time           `json:"last_observed_at"`
	AgeSeconds            int64               `json:"age_seconds"`
	ThresholdSeconds      int64               `json:"threshold_seconds"`
	Count                 int                 `json:"count,omitempty"`
	WaitingOnHuman        bool                `json:"waiting_on_human,omitempty"`
	HasRecoveryPredicate  bool                `json:"has_recovery_predicate,omitempty"`
	DeliveredAt           *time.Time          `json:"delivered_at,omitempty"`
	DeliveryAttempts      int                 `json:"delivery_attempts,omitempty"`
	LastDeliveryAttemptAt *time.Time          `json:"last_delivery_attempt_at,omitempty"`
	DeliveryError         string              `json:"delivery_error,omitempty"`
}

type StrandedIssue struct {
	ProjectID         string     `json:"project_id,omitempty"`
	IssueID           string     `json:"issue_id,omitempty"`
	Identifier        string     `json:"identifier,omitempty"`
	IssueURL          string     `json:"issue_url,omitempty"`
	Title             string     `json:"title,omitempty"`
	State             string     `json:"state,omitempty"`
	Since             time.Time  `json:"since"`
	DurationSeconds   int64      `json:"duration_seconds"`
	ThresholdSeconds  int64      `json:"threshold_seconds"`
	LastRefusalReason string     `json:"last_refusal_reason,omitempty"`
	LastRefusalAt     *time.Time `json:"last_refusal_at,omitempty"`
}

type Update struct {
	Enabled            bool       `json:"enabled"`
	AutoApplyEnabled   bool       `json:"auto_apply_enabled"`
	CheckIntervalHours int        `json:"check_interval_hours"`
	State              string     `json:"state,omitempty"`
	LastCheckAt        *time.Time `json:"last_check_at,omitempty"`
	LastAppliedVersion string     `json:"last_applied_version,omitempty"`
	NextCheckAt        *time.Time `json:"next_check_at,omitempty"`
	AvailableVersion   string     `json:"available_version,omitempty"`
	PendingSince       *time.Time `json:"pending_since,omitempty"`
	MaxDeferralHours   int        `json:"max_deferral_hours,omitempty"`
	Critical           bool       `json:"critical,omitempty"`
	LastError          string     `json:"last_error,omitempty"`
}

func (u Update) IsZero() bool {
	return !u.Enabled && !u.AutoApplyEnabled && u.CheckIntervalHours == 0 && u.State == "" && u.LastCheckAt == nil && u.LastAppliedVersion == "" && u.NextCheckAt == nil && u.AvailableVersion == "" && u.PendingSince == nil && u.MaxDeferralHours == 0 && !u.Critical && u.LastError == ""
}

func (u Update) DisplayState(now time.Time) string {
	if strings.TrimSpace(u.State) != "pending_idle" || u.PendingSince == nil || u.MaxDeferralHours <= 0 {
		return u.State
	}
	age := now.Sub(*u.PendingSince)
	if age < 0 {
		age = 0
	}
	return fmt.Sprintf("pending_idle (%s, max %dh)", compactUpdateDuration(age), u.MaxDeferralHours)
}

func compactUpdateDuration(duration time.Duration) string {
	if duration < time.Hour {
		return fmt.Sprintf("%dm", max(0, int(duration/time.Minute)))
	}
	return fmt.Sprintf("%dh", int(duration/time.Hour))
}

type Shutdown struct {
	Status            string     `json:"status,omitempty"`
	Draining          bool       `json:"draining"`
	SessionsRemaining int        `json:"sessions_remaining"`
	RequestedAt       *time.Time `json:"requested_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	Result            string     `json:"result,omitempty"`
}

type Project struct {
	ID          string      `json:"id,omitempty"`
	DisplayName string      `json:"display_name,omitempty"`
	URL         string      `json:"url,omitempty"`
	Color       string      `json:"color,omitempty"`
	Pool        string      `json:"pool,omitempty"`
	ActiveHours ActiveHours `json:"active_hours,omitzero"`
}

type ActiveHours struct {
	Configured     bool       `json:"configured"`
	WindowOpen     bool       `json:"window_open"`
	Open           bool       `json:"open"`
	OverrideActive bool       `json:"override_active"`
	Timezone       string     `json:"timezone,omitempty"`
	NextOpen       *time.Time `json:"next_open,omitempty"`
	NextClose      *time.Time `json:"next_close,omitempty"`
	OverrideUntil  *time.Time `json:"override_until,omitempty"`
}

func (a ActiveHours) IsZero() bool {
	return !a.Configured && !a.WindowOpen && !a.Open && !a.OverrideActive && a.Timezone == "" && a.NextOpen == nil && a.NextClose == nil && a.OverrideUntil == nil
}

func ActiveHoursFromStatus(status activehours.Status) ActiveHours {
	return ActiveHours{
		Configured:     status.Configured,
		WindowOpen:     status.WindowOpen,
		Open:           status.Open,
		OverrideActive: status.OverrideActive,
		Timezone:       status.Timezone,
		NextOpen:       activeHoursTimePointer(status.NextOpen),
		NextClose:      activeHoursTimePointer(status.NextClose),
		OverrideUntil:  activeHoursTimePointer(status.OverrideUntil),
	}
}

func activeHoursTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

type AgentPool struct {
	Name       string `json:"name"`
	Used       int    `json:"used"`
	Capacity   int    `json:"capacity"`
	Guaranteed int    `json:"guaranteed"`
	BurstTo    int    `json:"burst_to"`
	Borrowed   int    `json:"borrowed"`
	Available  int    `json:"available"`
	Draining   bool   `json:"draining,omitempty"`
	Reclaiming bool   `json:"reclaiming,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
}

type Instance struct {
	Name                    string `json:"name,omitempty"`
	GitHubLogin             string `json:"github_login,omitempty"`
	AuthorizationScope      string `json:"authorization_scope,omitempty"`
	AuthorizationConfigured bool   `json:"authorization_configured"`
}

type ProjectSnapshot struct {
	Project    Project         `json:"project"`
	Tracker    SnapshotSection `json:"tracker,omitzero"`
	Runtime    SnapshotSection `json:"runtime,omitzero"`
	Counts     Counts          `json:"counts"`
	Tokens     Tokens          `json:"tokens"`
	Throughput TokenThroughput `json:"throughput"`
	Auth       AuthHealth      `json:"auth,omitzero"`
	Refresh    Refresh         `json:"refresh,omitzero"`
	Dispatch   DispatchStatus  `json:"dispatch"`
}

type SnapshotSource string

const (
	SnapshotSourceUnknown SnapshotSource = "unknown"
	SnapshotSourceLive    SnapshotSource = "live"
	SnapshotSourceCached  SnapshotSource = "cached"
	SnapshotSourceMixed   SnapshotSource = "mixed"
)

type SnapshotSection struct {
	Source     SnapshotSource `json:"source"`
	ObservedAt time.Time      `json:"observed_at,omitzero"`
	Complete   bool           `json:"complete"`
}

func (s SnapshotSection) IsZero() bool {
	return s.Source == "" && s.ObservedAt.IsZero() && !s.Complete
}

func (s SnapshotSection) Available() bool {
	return s.Source == SnapshotSourceLive || s.Source == SnapshotSourceCached || s.Source == SnapshotSourceMixed
}

type DispatchStatus struct {
	ProjectID                string              `json:"project_id,omitempty"`
	CandidateCount           int                 `json:"candidate_count"`
	EligibleCandidateCount   int                 `json:"eligible_candidate_count"`
	SelectedCount            int                 `json:"selected_count"`
	SkippedCount             int                 `json:"skipped_count"`
	WaitReason               string              `json:"wait_reason,omitempty"`
	WaitReasonCode           string              `json:"wait_reason_code,omitempty"`
	AllSkippedSince          *time.Time          `json:"all_skipped_since,omitempty"`
	LastSelectedAt           *time.Time          `json:"last_selected_at,omitempty"`
	SecondsSinceLastSelected *int64              `json:"seconds_since_last_selected,omitempty"`
	StallDurationSeconds     int64               `json:"stall_duration_seconds,omitempty"`
	StallThresholdSeconds    int64               `json:"stall_threshold_seconds,omitempty"`
	ObservedAt               time.Time           `json:"observed_at,omitzero"`
	Stalled                  bool                `json:"stalled"`
	NeedsHumanAttention      bool                `json:"needs_human_attention"`
	Class                    observability.Class `json:"class,omitempty"`
	RateWindowPacing         RateWindowPacing    `json:"rate_window_pacing"`
}

const (
	RateWindowBucketFresh   = "fresh"
	RateWindowBucketMissing = "missing"
	RateWindowBucketStale   = "stale"
)

type RateWindowPacing struct {
	Mode                     string     `json:"mode"`
	FloorPercent             float64    `json:"floor_percent"`
	StaleAfterSeconds        int64      `json:"stale_after_seconds"`
	Applicable               bool       `json:"applicable"`
	BucketStatus             string     `json:"bucket_status"`
	ObservedRemainingPercent *float64   `json:"observed_remaining_percent,omitempty"`
	ObservedAt               *time.Time `json:"observed_at,omitempty"`
	PermitCeiling            int        `json:"permit_ceiling"`
	ScalingApplied           bool       `json:"scaling_applied"`
}

type Release struct {
	ProjectID        string     `json:"project_id,omitempty"`
	Enabled          bool       `json:"enabled"`
	State            string     `json:"state,omitempty"`
	LastRelease      string     `json:"last_release,omitempty"`
	LastReleaseAt    *time.Time `json:"last_release_at,omitempty"`
	UnreleasedMerges int        `json:"unreleased_merges"`
	NextTriggerAt    *time.Time `json:"next_trigger_at,omitempty"`
	CandidateSHA     string     `json:"candidate_sha,omitempty"`
	PendingTag       string     `json:"pending_tag,omitempty"`
	LastError        string     `json:"last_error,omitempty"`
}

func (r Release) IsZero() bool {
	return !r.Enabled && r.State == "" && r.LastRelease == "" && r.LastReleaseAt == nil && r.UnreleasedMerges == 0 && r.NextTriggerAt == nil && r.CandidateSHA == "" && r.PendingTag == "" && r.LastError == ""
}

type AuthStatus string

const (
	AuthStatusStale     AuthStatus = "stale"
	AuthStatusRecovered AuthStatus = "recovered"
)

type AuthHealth struct {
	Status          AuthStatus `json:"status,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	LastErrorAt     *time.Time `json:"last_error_at,omitempty"`
	LastRecoveredAt *time.Time `json:"last_recovered_at,omitempty"`
}

func (h AuthHealth) IsZero() bool {
	return h.Status == "" &&
		strings.TrimSpace(h.LastError) == "" &&
		h.LastErrorAt == nil &&
		h.LastRecoveredAt == nil
}

type RefreshStatus string

const refreshSweepHeadroomMultiplier = 2

const (
	RefreshStatusInitializing RefreshStatus = "initializing"
	RefreshStatusReady        RefreshStatus = "ready"
	RefreshStatusBehind       RefreshStatus = "behind"
	RefreshStatusPartial      RefreshStatus = "partial"
	RefreshStatusDegraded     RefreshStatus = "degraded"
)

type TickLivenessStatus string

const (
	TickLivenessStatusInitializing   TickLivenessStatus = "initializing"
	TickLivenessStatusReady          TickLivenessStatus = "ready"
	TickLivenessStatusNeedsAttention TickLivenessStatus = "needs_attention"
)

type TickLiveness struct {
	ProjectID             string             `json:"project_id,omitempty"`
	Status                TickLivenessStatus `json:"status"`
	LastTickAt            *time.Time         `json:"last_tick_at,omitempty"`
	NextRefreshAt         *time.Time         `json:"next_refresh_at,omitempty"`
	NextRefreshOverdue    bool               `json:"next_refresh_overdue"`
	FrozenAt              *time.Time         `json:"frozen_at,omitempty"`
	MissedIntervals       int64              `json:"missed_intervals"`
	WatchdogIntervalCount int64              `json:"watchdog_interval_count"`
}

type RefreshAttemptStatus string

const (
	RefreshAttemptStatusInProgress RefreshAttemptStatus = "in_progress"
	RefreshAttemptStatusCoalesced  RefreshAttemptStatus = "coalesced"
	RefreshAttemptStatusSucceeded  RefreshAttemptStatus = "succeeded"
	RefreshAttemptStatusFailed     RefreshAttemptStatus = "failed"
	RefreshAttemptStatusRefused    RefreshAttemptStatus = "refused"
)

type RefreshSourceName string

const (
	RefreshSourceCandidates RefreshSourceName = "candidates"
	RefreshSourceStatuses   RefreshSourceName = "statuses"
	RefreshSourceDrift      RefreshSourceName = "drift"
	RefreshSourceProject    RefreshSourceName = "project"
)

type RefreshProgress struct {
	Stage               string    `json:"stage"`
	StartedAt           time.Time `json:"started_at"`
	StageStartedAt      time.Time `json:"stage_started_at"`
	ElapsedSeconds      int64     `json:"elapsed_seconds"`
	StageElapsedSeconds int64     `json:"stage_elapsed_seconds"`
}

type Refresh struct {
	InFlight                *RefreshProgress `json:"in_flight,omitempty"`
	PollIntervalSeconds     int64            `json:"poll_interval_seconds,omitempty"`
	StaleAfterSeconds       int64            `json:"stale_after_seconds,omitempty"`
	LastDurationSeconds     int64            `json:"last_duration_seconds,omitempty"`
	ObservedSweepSeconds    int64            `json:"observed_sweep_seconds,omitempty"`
	BehindBySeconds         int64            `json:"behind_by_seconds,omitempty"`
	FailureThreshold        int              `json:"failure_threshold,omitempty"`
	DataSeq                 uint64           `json:"data_seq,omitempty"`
	Status                  RefreshStatus    `json:"status,omitempty"`
	LastRefreshAt           *time.Time       `json:"last_refresh_at,omitempty"`
	NextRefreshAt           *time.Time       `json:"next_refresh_at,omitempty"`
	NextRefreshOverdue      bool             `json:"next_refresh_overdue"`
	StalenessWindowExceeded bool             `json:"staleness_window_exceeded,omitempty"`
	LastError               string           `json:"last_error,omitempty"`
	LastErrorAt             *time.Time       `json:"last_error_at,omitempty"`
	Sources                 []RefreshSource  `json:"sources,omitempty"`
	Manual                  *RefreshAttempt  `json:"manual,omitempty"`
}

type RefreshFailureReason string

const (
	RefreshFailureReasonConsecutiveFailures RefreshFailureReason = "consecutive_failures"
	RefreshFailureReasonNeverRefreshed      RefreshFailureReason = "never_refreshed"
	RefreshFailureReasonStale               RefreshFailureReason = "stale"
)

type RefreshFailure struct {
	ProjectID        string               `json:"project_id"`
	Reason           RefreshFailureReason `json:"reason"`
	Source           RefreshSourceName    `json:"source,omitempty"`
	FailureStreak    int                  `json:"failure_streak"`
	FailureThreshold int                  `json:"failure_threshold"`
	LastError        string               `json:"last_error"`
	LastErrorAt      *time.Time           `json:"last_error_at,omitempty"`
	Condition        string               `json:"condition,omitempty"`
	Connector        string               `json:"connector,omitempty"`
}

type RefreshSource struct {
	ProjectID     string            `json:"project_id,omitempty"`
	Name          RefreshSourceName `json:"name"`
	LastSuccessAt *time.Time        `json:"last_success_at,omitempty"`
	Degraded      bool              `json:"degraded,omitempty"`
	FailureStreak int               `json:"failure_streak,omitempty"`
	LastError     string            `json:"last_error,omitempty"`
	LastErrorAt   *time.Time        `json:"last_error_at,omitempty"`
	Condition     string            `json:"condition,omitempty"`
	Connector     string            `json:"connector,omitempty"`
}

type RefreshAttempt struct {
	ID          string               `json:"id,omitempty"`
	Status      RefreshAttemptStatus `json:"status,omitempty"`
	RequestedAt *time.Time           `json:"requested_at,omitempty"`
	StartedAt   *time.Time           `json:"started_at,omitempty"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	Operations  []string             `json:"operations,omitempty"`
	Coalesced   bool                 `json:"coalesced,omitempty"`
	LastError   string               `json:"last_error,omitempty"`
	LastErrorAt *time.Time           `json:"last_error_at,omitempty"`
	RetryAt     *time.Time           `json:"retry_at,omitempty"`
}

func (a RefreshAttempt) IsZero() bool {
	return strings.TrimSpace(a.ID) == "" &&
		a.Status == "" &&
		a.RequestedAt == nil &&
		a.StartedAt == nil &&
		a.CompletedAt == nil &&
		len(a.Operations) == 0 &&
		!a.Coalesced &&
		strings.TrimSpace(a.LastError) == "" &&
		a.LastErrorAt == nil &&
		a.RetryAt == nil
}

func (r Refresh) ReadinessStatus() RefreshStatus {
	if RefreshStatus(strings.TrimSpace(string(r.Status))) == RefreshStatusPartial {
		return RefreshStatusPartial
	}
	if strings.TrimSpace(r.LastError) != "" || r.LastErrorAt != nil {
		return RefreshStatusDegraded
	}
	switch RefreshStatus(strings.TrimSpace(string(r.Status))) {
	case RefreshStatusInitializing:
		return RefreshStatusInitializing
	case RefreshStatusReady:
		if r.NextRefreshOverdue {
			return RefreshStatusBehind
		}
		return RefreshStatusReady
	case RefreshStatusBehind:
		return RefreshStatusBehind
	case RefreshStatusDegraded:
		return RefreshStatusDegraded
	}
	if r.LastRefreshAt != nil {
		if r.NextRefreshOverdue {
			return RefreshStatusBehind
		}
		return RefreshStatusReady
	}
	return RefreshStatusInitializing
}

func (r Refresh) WithFreshness(now time.Time) Refresh {
	if r.InFlight != nil {
		progress := *r.InFlight
		progress.ElapsedSeconds = max(0, int64(now.Sub(progress.StartedAt)/time.Second))
		progress.StageElapsedSeconds = max(0, int64(now.Sub(progress.StageStartedAt)/time.Second))
		r.InFlight = &progress
	}
	r.NextRefreshOverdue = r.NextRefreshAt != nil && !now.IsZero() && now.After(*r.NextRefreshAt)
	r.BehindBySeconds = 0
	if r.NextRefreshOverdue {
		r.BehindBySeconds = int64(now.Sub(*r.NextRefreshAt) / time.Second)
	}
	if !refreshHasReadinessSignal(r) {
		return r
	}
	readinessStatus := r.ReadinessStatus()
	wasStalenessWindowExceeded := r.StalenessWindowExceeded
	if readinessStatus == RefreshStatusDegraded && !wasStalenessWindowExceeded {
		r.Status = RefreshStatusDegraded
		return r
	}
	r.StalenessWindowExceeded = false
	if deadline, ok := r.stalenessDeadline(); ok && now.After(deadline) {
		r.Status = RefreshStatusDegraded
		r.StalenessWindowExceeded = true
		r.LastErrorAt = copyTimePointer(&deadline)
		if r.LastRefreshAt == nil {
			r.LastError = "refresh has never completed within the expected sweep window"
		} else {
			r.LastError = "refresh has not succeeded within the expected sweep window"
		}
		return r
	}
	if wasStalenessWindowExceeded {
		r.LastError = ""
		r.LastErrorAt = nil
		if r.LastRefreshAt == nil {
			readinessStatus = RefreshStatusInitializing
		} else {
			readinessStatus = RefreshStatusReady
		}
	}
	if readinessStatus == RefreshStatusPartial {
		r.Status = RefreshStatusPartial
		return r
	}
	if r.NextRefreshOverdue {
		r.Status = RefreshStatusBehind
		return r
	}
	if r.LastRefreshAt == nil {
		r.Status = readinessStatus
		return r
	}
	r.Status = RefreshStatusReady
	return r
}

func (r Refresh) stalenessDeadline() (time.Time, bool) {
	staleAfter := time.Duration(r.StaleAfterSeconds) * time.Second
	if staleAfter <= 0 {
		return time.Time{}, false
	}
	oldest := r.LastRefreshAt
	for index := range r.Sources {
		successAt := r.Sources[index].LastSuccessAt
		if successAt != nil && (oldest == nil || successAt.Before(*oldest)) {
			oldest = successAt
		}
	}
	if oldest != nil {
		return oldest.Add(staleAfter), true
	}
	if r.NextRefreshAt != nil {
		return r.NextRefreshAt.Add(staleAfter), true
	}
	return time.Time{}, false
}

func refreshHasReadinessSignal(r Refresh) bool {
	return strings.TrimSpace(string(r.Status)) != "" ||
		r.LastRefreshAt != nil ||
		r.NextRefreshAt != nil ||
		strings.TrimSpace(r.LastError) != "" ||
		r.LastErrorAt != nil ||
		len(r.Sources) > 0
}

func (r Refresh) Ready() bool {
	status := r.ReadinessStatus()
	return status == RefreshStatusReady || status == RefreshStatusBehind
}

func (r Refresh) Initializing() bool {
	return r.ReadinessStatus() == RefreshStatusInitializing
}

func (r Refresh) Degraded() bool {
	return r.ReadinessStatus() == RefreshStatusDegraded
}

func (r Refresh) Partial() bool {
	return r.ReadinessStatus() == RefreshStatusPartial
}

func (r Refresh) Behind() bool {
	return r.ReadinessStatus() == RefreshStatusBehind
}

func (r Refresh) Source(name RefreshSourceName) (RefreshSource, bool) {
	for _, source := range r.Sources {
		if source.Name == name {
			return source, true
		}
	}
	return RefreshSource{}, false
}

func (r Refresh) Stale(now time.Time) bool {
	if len(r.Sources) == 0 {
		return r.Degraded()
	}
	threshold := r.FailureThreshold
	if threshold <= 0 {
		threshold = 3
	}
	staleAfter := time.Duration(r.StaleAfterSeconds) * time.Second
	for _, source := range r.Sources {
		if source.Degraded {
			return true
		}
		if source.FailureStreak >= threshold {
			return true
		}
		if staleAfter <= 0 || source.LastSuccessAt == nil || now.IsZero() {
			continue
		}
		if now.After(source.LastSuccessAt.Add(staleAfter)) {
			return true
		}
	}
	return false
}

func (s Snapshot) RefreshFailures() []RefreshFailure {
	projects := s.Projects
	if len(projects) == 0 && (strings.TrimSpace(s.Project.ID) != "" || refreshHasReadinessSignal(s.Refresh)) {
		projects = []ProjectSnapshot{{Project: s.Project, Refresh: s.Refresh}}
	}
	failures := make([]RefreshFailure, 0, len(projects))
	for _, project := range projects {
		if failure, ok := project.Refresh.failure(strings.TrimSpace(project.Project.ID)); ok {
			failures = append(failures, failure)
		}
	}
	return failures
}

func (r Refresh) failure(projectID string) (RefreshFailure, bool) {
	threshold := r.FailureThreshold
	if threshold <= 0 {
		threshold = 3
	}
	source := mostRelevantFailedRefreshSource(r.Sources, threshold, r.LastRefreshAt == nil)
	if strings.TrimSpace(projectID) == "" {
		projectID = strings.TrimSpace(source.ProjectID)
	}
	lastError := strings.TrimSpace(r.LastError)
	lastErrorAt := r.LastErrorAt
	if lastError == "" {
		lastError = strings.TrimSpace(source.LastError)
	}
	if lastErrorAt == nil {
		lastErrorAt = source.LastErrorAt
	}
	if r.LastRefreshAt == nil && lastError != "" {
		return RefreshFailure{
			ProjectID:        projectID,
			Reason:           RefreshFailureReasonNeverRefreshed,
			Source:           source.Name,
			FailureStreak:    source.FailureStreak,
			FailureThreshold: threshold,
			LastError:        lastError,
			LastErrorAt:      copyTimePointer(lastErrorAt),
			Condition:        source.Condition,
			Connector:        source.Connector,
		}, true
	}
	if r.StalenessWindowExceeded {
		return RefreshFailure{
			ProjectID:        projectID,
			Reason:           RefreshFailureReasonStale,
			Source:           source.Name,
			FailureStreak:    source.FailureStreak,
			FailureThreshold: threshold,
			LastError:        lastError,
			LastErrorAt:      copyTimePointer(lastErrorAt),
			Condition:        source.Condition,
			Connector:        source.Connector,
		}, true
	}
	if source.FailureStreak < threshold {
		return RefreshFailure{}, false
	}
	return RefreshFailure{
		ProjectID:        projectID,
		Reason:           RefreshFailureReasonConsecutiveFailures,
		Source:           source.Name,
		FailureStreak:    source.FailureStreak,
		FailureThreshold: threshold,
		LastError:        lastError,
		LastErrorAt:      copyTimePointer(lastErrorAt),
		Condition:        source.Condition,
		Connector:        source.Connector,
	}, true
}

func mostRelevantFailedRefreshSource(sources []RefreshSource, threshold int, neverRefreshed bool) RefreshSource {
	var selected RefreshSource
	for _, source := range sources {
		eligible := source.FailureStreak >= threshold
		if neverRefreshed {
			eligible = source.FailureStreak > 0 || strings.TrimSpace(source.LastError) != ""
		}
		if !eligible {
			continue
		}
		if selected.Name == "" || source.FailureStreak > selected.FailureStreak ||
			(source.FailureStreak == selected.FailureStreak && refreshSourceErrorAt(source).After(refreshSourceErrorAt(selected))) {
			selected = source
		}
	}
	return selected
}

func refreshSourceErrorAt(source RefreshSource) time.Time {
	if source.LastErrorAt == nil {
		return time.Time{}
	}
	return source.LastErrorAt.UTC()
}

func copyTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

type Counts struct {
	Running   int `json:"running"`
	Queue     int `json:"queue"`
	Blocked   int `json:"blocked"`
	Completed int `json:"completed"`
}

func (s Snapshot) EffectiveCounts() Counts {
	return Counts{
		Running:   countOrLength(s.Counts.Running, len(s.Running)),
		Queue:     countOrLength(s.Counts.Queue, len(s.Queue)),
		Blocked:   len(s.Blocked),
		Completed: countOrLength(s.Counts.Completed, len(s.Completed)),
	}
}

func countOrLength(count int, length int) int {
	if count > 0 {
		return count
	}
	return length
}

type TrackerDrift struct {
	UntrackedOpen []Issue `json:"untracked_open,omitempty"`
	OpenTerminal  []Issue `json:"open_terminal,omitempty"`
	ClosedActive  []Issue `json:"closed_active,omitempty"`
}

func (d TrackerDrift) IsZero() bool {
	return len(d.UntrackedOpen) == 0 && len(d.OpenTerminal) == 0 && len(d.ClosedActive) == 0
}

type Issue struct {
	ID                    string                 `json:"issue_id"`
	Identifier            string                 `json:"identifier,omitempty"`
	Number                int                    `json:"number,omitempty"`
	ProjectID             string                 `json:"project_id,omitempty"`
	URL                   string                 `json:"url,omitempty"`
	Title                 string                 `json:"title,omitempty"`
	Description           string                 `json:"description,omitempty"`
	Priority              *int                   `json:"priority,omitempty"`
	PriorityName          string                 `json:"priority_name,omitempty"`
	UnblockerCount        int                    `json:"unblocker_count,omitempty"`
	State                 string                 `json:"state,omitempty"`
	AuthorID              string                 `json:"author_id,omitempty"`
	Origin                string                 `json:"origin,omitempty"`
	OriginActor           string                 `json:"origin_actor,omitempty"`
	OriginActorKind       string                 `json:"origin_actor_kind,omitempty"`
	Labels                []string               `json:"labels,omitempty"`
	Assignees             []string               `json:"assignees,omitempty"`
	Comments              []IssueComment         `json:"comments,omitempty"`
	BlockedBy             []BlockedRef           `json:"blocked_by,omitempty"`
	PullRequest           *PullRequest           `json:"pull_request,omitempty"`
	Deliverable           *Deliverable           `json:"deliverable,omitempty"`
	Metadata              map[string]string      `json:"metadata,omitempty"`
	MergeTiming           *MergeTiming           `json:"merge_timing,omitempty"`
	Owner                 string                 `json:"owner,omitempty"`
	LeaseRenewedAt        *time.Time             `json:"lease_renewed_at,omitempty"`
	LeaseExpiresAt        *time.Time             `json:"lease_expires_at,omitempty"`
	LeaseStale            bool                   `json:"lease_stale,omitempty"`
	GatePending           bool                   `json:"gate_pending,omitempty"`
	RequiredGate          *RequiredGate          `json:"required_gate,omitempty"`
	CreatedAt             *time.Time             `json:"created_at,omitempty"`
	UpdatedAt             *time.Time             `json:"updated_at,omitempty"`
	StageUpdatedAt        *time.Time             `json:"stage_updated_at,omitempty"`
	CurrentLaneEnteredAt  *time.Time             `json:"current_lane_entered_at,omitempty"`
	CurrentLaneAgeSeconds int64                  `json:"current_lane_age_seconds,omitempty"`
	RuntimeIdentity       agentidentity.Identity `json:"runtime_identity,omitzero"`
	ParkSummary           ParkSummary            `json:"park_summary,omitzero"`
	CompletionProgress    CompletionProgress     `json:"completion_progress,omitzero"`
}

type RequiredGate struct {
	State          string `json:"state"`
	Reason         string `json:"reason,omitempty"`
	PRNumber       int    `json:"pr_number,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	BaseSHA        string `json:"base_sha,omitempty"`
	CIState        string `json:"ci_state,omitempty"`
	MergeableState string `json:"mergeable_state,omitempty"`
	AuditRunID     int64  `json:"audit_run_id,omitempty"`
	AuditReason    string `json:"audit_reason,omitempty"`
	HumanAction    string `json:"human_action,omitempty"`
}

type CompletionProgress struct {
	Outcome               string   `json:"outcome,omitempty"`
	Reason                string   `json:"reason,omitempty"`
	Kinds                 []string `json:"kinds,omitempty"`
	CompletionKind        string   `json:"completion_kind,omitempty"`
	ConsecutiveNoProgress int      `json:"consecutive_no_progress,omitempty"`
	NoProgressLimit       int      `json:"no_progress_limit,omitempty"`
}

const CompletionProgressOutcomeNoProgress = "no_progress"

func (p CompletionProgress) IsZero() bool {
	return p.Outcome == "" && p.Reason == "" && len(p.Kinds) == 0 && p.CompletionKind == "" && p.ConsecutiveNoProgress == 0 && p.NoProgressLimit == 0
}

type ParkSummary struct {
	AttemptCount             int64              `json:"attempt_count"`
	ParkCount                int64              `json:"park_count"`
	AcknowledgedParkSequence int64              `json:"acknowledged_park_sequence,omitempty"`
	AcknowledgedAt           *time.Time         `json:"acknowledged_at,omitempty"`
	Causes                   []ParkCauseSummary `json:"causes,omitempty"`
	Tokens                   ParkTokenTotals    `json:"tokens"`
}

func (s ParkSummary) IsZero() bool {
	return s.AttemptCount == 0 && s.ParkCount == 0 && s.AcknowledgedParkSequence == 0 && s.AcknowledgedAt == nil && len(s.Causes) == 0 && s.Tokens == (ParkTokenTotals{})
}

type ParkCauseSummary struct {
	Cause   string    `json:"cause"`
	Count   int64     `json:"count"`
	FirstAt time.Time `json:"first_at"`
	LastAt  time.Time `json:"last_at"`
}

type ParkTokenTotals struct {
	InputTokens           int64 `json:"input_tokens"`
	CachedInputTokens     int64 `json:"cached_input_tokens"`
	OutputTokens          int64 `json:"output_tokens"`
	ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
}

type IssueComment struct {
	ID                string     `json:"id,omitempty"`
	Backend           string     `json:"backend,omitempty"`
	Body              string     `json:"body,omitempty"`
	URL               string     `json:"url,omitempty"`
	AuthorLogin       string     `json:"author_login,omitempty"`
	AuthorKind        string     `json:"author_kind,omitempty"`
	AuthorDisplayName string     `json:"author_display_name,omitempty"`
	CreatedAt         *time.Time `json:"created_at,omitempty"`
	UpdatedAt         *time.Time `json:"updated_at,omitempty"`
	Local             bool       `json:"local,omitempty"`
	CanEdit           bool       `json:"can_edit,omitempty"`
	CanDelete         bool       `json:"can_delete,omitempty"`
	TargetType        string     `json:"target_type,omitempty"`
}

type Deliverable struct {
	Kind             string            `json:"kind,omitempty"`
	Path             string            `json:"path,omitempty"`
	ReviewURL        string            `json:"review_url,omitempty"`
	ValidationStatus string            `json:"validation_status,omitempty"`
	ExternalID       string            `json:"external_id,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type BlockedRef struct {
	HumanOwned           bool   `json:"human_owned,omitempty"`
	HumanCompletionReady bool   `json:"human_completion_ready,omitempty"`
	ID                   string `json:"id,omitempty"`
	Identifier           string `json:"identifier"`
	State                string `json:"state,omitempty"`
	TrackerState         string `json:"tracker_state,omitempty"`
	Source               string `json:"source,omitempty"`
}

type PullRequest struct {
	Number                     int                         `json:"number,omitempty"`
	URL                        string                      `json:"url,omitempty"`
	BranchName                 string                      `json:"branch_name,omitempty"`
	State                      string                      `json:"state,omitempty"`
	MergeableState             string                      `json:"mergeable_state,omitempty"`
	HeadSHA                    string                      `json:"head_sha,omitempty"`
	BaseSHA                    string                      `json:"base_sha,omitempty"`
	HydrationUnavailableReason string                      `json:"hydration_unavailable_reason,omitempty"`
	HydrationDegradedReason    string                      `json:"hydration_degraded_reason,omitempty"`
	HydrationNextRetryAt       *time.Time                  `json:"hydration_next_retry_at,omitempty"`
	CIStatus                   string                      `json:"ci_status,omitempty"`
	CheckRunCount              int                         `json:"check_run_count,omitempty"`
	StatusContextCount         int                         `json:"status_context_count,omitempty"`
	CIQueueSeconds             int64                       `json:"ci_queue_seconds,omitempty"`
	CIDurationSeconds          int64                       `json:"ci_duration_seconds,omitempty"`
	QuietWaitSeconds           int64                       `json:"quiet_wait_seconds,omitempty"`
	SlowChecks                 []PullRequestCheck          `json:"slow_checks,omitempty"`
	RunningChecks              []string                    `json:"running_checks,omitempty"`
	UnstartedCheckCount        int                         `json:"unstarted_check_count,omitempty"`
	UnstartedChecks            []PullRequestCheck          `json:"unstarted_checks,omitempty"`
	RequiredCheckFailures      []PullRequestCheck          `json:"required_check_failures,omitempty"`
	CodexReviewState           string                      `json:"codex_review_state,omitempty"`
	CodexReviewSource          string                      `json:"codex_review_source,omitempty"`
	MergeQueueEntry            *PullRequestMergeQueueEntry `json:"merge_queue_entry,omitempty"`
}

type PullRequestMergeQueueEntry struct {
	ID                          string     `json:"id,omitempty"`
	State                       string     `json:"state,omitempty"`
	Position                    int        `json:"position,omitempty"`
	Depth                       int        `json:"depth,omitempty"`
	EstimatedTimeToMergeSeconds int64      `json:"estimated_time_to_merge_seconds,omitempty"`
	EnqueuedAt                  *time.Time `json:"enqueued_at,omitempty"`
	URL                         string     `json:"url,omitempty"`
}

type PullRequestCheck struct {
	Name            string `json:"name,omitempty"`
	Status          string `json:"status,omitempty"`
	Conclusion      string `json:"conclusion,omitempty"`
	QueueSeconds    int64  `json:"queue_seconds,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
}

type MergeTiming struct {
	EnteredMergingAt           *time.Time `json:"entered_merging_at,omitempty"`
	MergeWorkerSlotAcquiredAt  *time.Time `json:"merge_worker_slot_acquired_at,omitempty"`
	MergeStartedAt             *time.Time `json:"merge_started_at,omitempty"`
	BaseRefreshStartedAt       *time.Time `json:"base_refresh_started_at,omitempty"`
	BaseRefreshFinishedAt      *time.Time `json:"base_refresh_finished_at,omitempty"`
	CIWaitStartedAt            *time.Time `json:"ci_wait_started_at,omitempty"`
	CIWaitFinishedAt           *time.Time `json:"ci_wait_finished_at,omitempty"`
	MergedAt                   *time.Time `json:"merged_at,omitempty"`
	MergeFailedAt              *time.Time `json:"merge_failed_at,omitempty"`
	MergeFailureReason         string     `json:"merge_failure_reason,omitempty"`
	QueueWaitSeconds           int64      `json:"queue_wait_seconds,omitempty"`
	ActiveMergeDurationSeconds int64      `json:"active_merge_duration_seconds,omitempty"`
	TotalMergingSeconds        int64      `json:"total_merging_seconds,omitempty"`
	Repository                 string     `json:"repository,omitempty"`
	PullRequestNumber          int        `json:"pull_request_number,omitempty"`
	IssueNumber                int        `json:"issue_number,omitempty"`
	HeadSHA                    string     `json:"head_sha,omitempty"`
	BaseSHA                    string     `json:"base_sha,omitempty"`
}

type ActivityEvent struct {
	At         time.Time                 `json:"at"`
	Event      string                    `json:"event,omitempty"`
	Message    string                    `json:"message,omitempty"`
	Truncation *runtimeoutput.Truncation `json:"truncation,omitempty"`
}

type Running struct {
	Issue
	Attempt               int                       `json:"attempt"`
	WorkAttemptID         int64                     `json:"work_attempt_id,omitempty"`
	StopDestination       string                    `json:"stop_destination,omitempty"`
	StopPriorityOptions   []StopRunPriorityOption   `json:"stop_priority_options,omitempty"`
	DetentSessionID       int64                     `json:"detent_session_id,omitempty"`
	WorkerHost            string                    `json:"worker_host,omitempty"`
	ProcessIdentity       string                    `json:"process_identity,omitempty"`
	WorkspacePath         string                    `json:"workspace_path,omitempty"`
	SessionID             string                    `json:"session_id,omitempty"`
	TurnCount             int                       `json:"turn_count"`
	StartedAt             time.Time                 `json:"started_at"`
	LastEventAt           *time.Time                `json:"last_event_at,omitempty"`
	LastEvent             string                    `json:"last_event,omitempty"`
	LastMessage           string                    `json:"last_message,omitempty"`
	LastMessageTruncation *runtimeoutput.Truncation `json:"last_message_truncation,omitempty"`
	RecentEvents          []ActivityEvent           `json:"recent_events,omitempty"`
	RuntimeSeconds        float64                   `json:"runtime_seconds"`
	DiffAdded             int                       `json:"diff_added"`
	DiffRemoved           int                       `json:"diff_removed"`
	DiffFiles             int                       `json:"diff_files"`
	DiffStatus            string                    `json:"diff_status,omitempty"`
	Tokens                Tokens                    `json:"tokens"`
	RSSBytes              uint64                    `json:"rss_bytes"`
	RSSCeilingBytes       uint64                    `json:"rss_ceiling_bytes"`
	RSSObservedAt         time.Time                 `json:"rss_observed_at,omitzero"`
}

type StopRunPriorityOption struct {
	Rank int    `json:"rank"`
	Name string `json:"name"`
}

type WorkAttempt struct {
	AttemptID               int64                     `json:"attempt_id"`
	ProjectID               string                    `json:"project_id,omitempty"`
	IssueID                 string                    `json:"issue_id,omitempty"`
	Identifier              string                    `json:"identifier,omitempty"`
	IssueURL                string                    `json:"issue_url,omitempty"`
	PRNumber                *int64                    `json:"pr_number,omitempty"`
	Repo                    string                    `json:"repo,omitempty"`
	WorkerType              string                    `json:"worker_type,omitempty"`
	WorkerHost              string                    `json:"worker_host,omitempty"`
	Lane                    string                    `json:"lane,omitempty"`
	AttemptNumber           int                       `json:"attempt_number,omitempty"`
	Status                  string                    `json:"status,omitempty"`
	StartedAt               time.Time                 `json:"started_at,omitzero"`
	LeaseExpiresAt          *time.Time                `json:"lease_expires_at,omitempty"`
	HeartbeatAt             *time.Time                `json:"heartbeat_at,omitempty"`
	CompletedAt             *time.Time                `json:"completed_at,omitempty"`
	TerminalState           string                    `json:"terminal_state,omitempty"`
	ErrorClass              string                    `json:"error_class,omitempty"`
	ErrorMessage            string                    `json:"error_message,omitempty"`
	Phase                   string                    `json:"phase,omitempty"`
	StatusMessage           string                    `json:"status_message,omitempty"`
	StatusMessageTruncation *runtimeoutput.Truncation `json:"status_message_truncation,omitempty"`
	CurrentCommand          string                    `json:"current_command,omitempty"`
	WaitReason              string                    `json:"wait_reason,omitempty"`
	GitHubRateSnapshotJSON  string                    `json:"github_rate_snapshot_json,omitempty"`
	CIState                 string                    `json:"ci_state,omitempty"`
	CapacitySnapshotJSON    string                    `json:"capacity_snapshot_json,omitempty"`
	WorkerMetadataJSON      string                    `json:"worker_metadata_json,omitempty"`
	MetricsJSON             string                    `json:"metrics_json,omitempty"`
	NextAction              string                    `json:"next_action,omitempty"`
	DetentSessionID         int64                     `json:"detent_session_id,omitempty"`
	ProviderSessionID       string                    `json:"provider_session_id,omitempty"`
	RuntimeIdentity         agentidentity.Identity    `json:"runtime_identity,omitzero"`
	Stale                   bool                      `json:"stale,omitempty"`
}

type SchedulerDecision struct {
	ID                     int64     `json:"id,omitempty"`
	ProjectID              string    `json:"project_id,omitempty"`
	IssueID                string    `json:"issue_id,omitempty"`
	Identifier             string    `json:"identifier,omitempty"`
	IssueURL               string    `json:"issue_url,omitempty"`
	PRNumber               *int64    `json:"pr_number,omitempty"`
	Repo                   string    `json:"repo,omitempty"`
	Lane                   string    `json:"lane,omitempty"`
	QueuePosition          int       `json:"queue_position,omitempty"`
	Result                 string    `json:"result,omitempty"`
	Reason                 string    `json:"reason,omitempty"`
	Selected               bool      `json:"selected,omitempty"`
	Retry                  bool      `json:"retry,omitempty"`
	AttemptNumber          int       `json:"attempt_number,omitempty"`
	WorkerHost             string    `json:"worker_host,omitempty"`
	DecisionAt             time.Time `json:"decision_at,omitzero"`
	WaitReason             string    `json:"wait_reason,omitempty"`
	CapacitySnapshotJSON   string    `json:"capacity_snapshot_json,omitempty"`
	GitHubRateSnapshotJSON string    `json:"github_rate_snapshot_json,omitempty"`
}

type Queued struct {
	Issue
	Attempt               int        `json:"attempt"`
	DueAt                 *time.Time `json:"due_at,omitempty"`
	DueInMillis           int64      `json:"due_in_ms,omitempty"`
	Error                 string     `json:"error,omitempty"`
	WorkerHost            string     `json:"worker_host,omitempty"`
	WorkspacePath         string     `json:"workspace_path,omitempty"`
	ProjectedSpend        float64    `json:"projected_spend_usd,omitempty"`
	QueueState            string     `json:"queue_state,omitempty"`
	WaitStartedAt         *time.Time `json:"wait_started_at,omitempty"`
	PollCount             int        `json:"poll_count,omitempty"`
	PendingChecks         []string   `json:"pending_checks,omitempty"`
	WorkspaceCreateCount  int        `json:"workspace_create_count,omitempty"`
	WorkspaceDestroyCount int        `json:"workspace_destroy_count,omitempty"`
}

const (
	QueueStateRetrying         = "retrying"
	QueueStateWaitingOnCI      = "waiting_on_ci"
	QueueStateWaitingOnTracker = "waiting_on_tracker"
)

type BlockedSource string

const (
	BlockedSourceDependency    BlockedSource = "dependency"
	BlockedSourceMergeDuration BlockedSource = "merge_duration_reconciliation"
	BlockedSourceProjectStatus BlockedSource = "project_status"
	BlockedSourceOperatorStop  BlockedSource = "operator_stop_reconciliation"
	BlockedSourceOwnership     BlockedSource = "ownership"
)

type Blocked struct {
	Issue
	WorkerHost              string               `json:"worker_host,omitempty"`
	WorkspacePath           string               `json:"workspace_path,omitempty"`
	SessionID               string               `json:"session_id,omitempty"`
	Error                   string               `json:"error,omitempty"`
	AttemptError            string               `json:"attempt_error,omitempty"`
	Source                  BlockedSource        `json:"source,omitempty"`
	RecoveryAction          string               `json:"recovery_action,omitempty"`
	RecoveryReason          string               `json:"recovery_reason,omitempty"`
	RecoveryTarget          string               `json:"recovery_target,omitempty"`
	RecoveryRemedy          string               `json:"recovery_remedy,omitempty"`
	RecoveryReachability    string               `json:"recovery_reachability,omitempty"`
	RecoveryIntentResumable bool                 `json:"recovery_intent_resumable,omitempty"`
	NeedsHumanAttention     bool                 `json:"needs_human_attention,omitempty"`
	BlockerEvidence         []BlockerEvidence    `json:"blocker_evidence,omitempty"`
	RecoveryRoot            *BlockedRecoveryRoot `json:"recovery_root,omitempty"`
	BlockedAt               *time.Time           `json:"blocked_at,omitempty"`
	LastEventAt             *time.Time           `json:"last_event_at,omitempty"`
	LastEvent               string               `json:"last_event,omitempty"`
	LastMessage             string               `json:"last_message,omitempty"`
	Attempt                 int                  `json:"attempt,omitempty"`
	WorkAttemptID           int64                `json:"work_attempt_id,omitempty"`
	DetentSessionID         int64                `json:"detent_session_id,omitempty"`
	Destination             string               `json:"destination,omitempty"`
	Priority                int                  `json:"priority,omitempty"`
	PriorityName            string               `json:"priority_name,omitempty"`
	StopReason              string               `json:"stop_reason,omitempty"`
}

type BlockerEvidence struct {
	Type            string     `json:"type"`
	Owner           string     `json:"owner"`
	Status          string     `json:"status"`
	Reference       string     `json:"reference,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	Detail          string     `json:"detail,omitempty"`
	Unverifiable    bool       `json:"unverifiable,omitempty"`
	AgeSeconds      int64      `json:"age_seconds,omitempty"`
	RecordedAt      *time.Time `json:"recorded_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	RecheckInterval string     `json:"recheck_interval,omitempty"`
}

type BlockedRecoveryRoot struct {
	IssueID         string `json:"issue_id,omitempty"`
	IssueIdentifier string `json:"issue_identifier,omitempty"`
	IssueURL        string `json:"issue_url,omitempty"`
	Reason          string `json:"reason"`
	Remedy          string `json:"remedy,omitempty"`
}

type Completed struct {
	Issue
	SessionID      string    `json:"session_id,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	Turns          int       `json:"turns"`
	RuntimeSeconds float64   `json:"runtime_seconds"`
	FinalState     string    `json:"final_state,omitempty"`
	Model          string    `json:"model,omitempty"`
	Tokens         Tokens    `json:"tokens"`
}

type Budget struct {
	Enabled           bool               `json:"enabled"`
	DegradedReason    string             `json:"degraded_reason,omitempty"`
	PerDayMaxUSD      *float64           `json:"per_day_max_usd"`
	PerIssueMaxUSD    *float64           `json:"per_issue_max_usd"`
	CurrentSpendUSD   float64            `json:"current_spend_usd"`
	ProjectedCostUSD  float64            `json:"projected_cost_usd"`
	ProjectedSpendUSD float64            `json:"projected_spend_usd,omitempty"`
	PeriodStart       time.Time          `json:"period_start,omitzero"`
	PeriodEnd         time.Time          `json:"period_end,omitzero"`
	SpendPoints       []BudgetSpendPoint `json:"spend_points,omitempty"`
	Days              []BudgetDay        `json:"days,omitempty"`
	SpendRegression   *SpendRegression   `json:"spend_regression,omitempty"`
	Refusals          []BudgetRefusal    `json:"refusals,omitempty"`
}

type SpendRegression struct {
	Date              string  `json:"date"`
	PreviousSpendUSD  float64 `json:"previous_spend_usd"`
	ProjectedSpendUSD float64 `json:"projected_spend_usd"`
	DropPercent       float64 `json:"drop_percent"`
	ThresholdPercent  float64 `json:"threshold_percent"`
}

type BudgetOverride struct {
	ProjectID      string    `json:"project_id"`
	PerDayMaxUSD   *float64  `json:"per_day_max_usd"`
	PerIssueMaxUSD *float64  `json:"per_issue_max_usd"`
	ExpiresAt      time.Time `json:"expires_at"`
	CreatedAt      time.Time `json:"created_at"`
	Reason         string    `json:"reason"`
}

type BudgetSpendPoint struct {
	At       time.Time `json:"at"`
	SpendUSD float64   `json:"spend_usd"`
}

type BudgetDay struct {
	Date     string  `json:"date"`
	SpendUSD float64 `json:"spend_usd"`
}

type BudgetRefusal struct {
	IssueID          string     `json:"issue_id"`
	Identifier       string     `json:"identifier,omitempty"`
	Code             string     `json:"code"`
	Message          string     `json:"message"`
	CurrentSpendUSD  float64    `json:"current_spend_usd"`
	ProjectedCostUSD float64    `json:"projected_cost_usd"`
	MaxUSD           *float64   `json:"max_usd"`
	ResetAt          *time.Time `json:"reset_at,omitempty"`
	RefusedAt        time.Time  `json:"refused_at"`
	HardHold         bool       `json:"hard_hold"`
}

type RateLimits struct {
	LimitID           string           `json:"limit_id,omitempty"`
	LimitName         string           `json:"limit_name,omitempty"`
	ReachedType       string           `json:"reached_type,omitempty"`
	Primary           *RateLimitBucket `json:"primary,omitempty"`
	Secondary         *RateLimitBucket `json:"secondary,omitempty"`
	Credits           *RateLimitBucket `json:"credits,omitempty"`
	GitHubGraphQL     *RateLimitBucket `json:"github_graphql,omitempty"`
	GitHubREST        *RateLimitBucket `json:"github_rest,omitempty"`
	GitHubRESTBudgets []RESTBudget     `json:"github_rest_budgets,omitempty"`
	GraphQLCost       *GraphQLCost     `json:"graphql_cost,omitempty"`
	RESTUsage         *RESTUsage       `json:"rest_usage,omitempty"`
}

type BackendOutage struct {
	ProjectID       string     `json:"project_id,omitempty"`
	BackendID       string     `json:"backend_id"`
	BackendKind     string     `json:"backend_kind,omitempty"`
	Provider        string     `json:"provider,omitempty"`
	Kind            string     `json:"kind,omitempty"`
	Reason          string     `json:"reason"`
	Trigger         string     `json:"trigger,omitempty"`
	DetectedAt      time.Time  `json:"detected_at"`
	LastObservedAt  time.Time  `json:"last_observed_at"`
	ResetAt         *time.Time `json:"reset_at,omitempty"`
	ResumeAt        time.Time  `json:"resume_at"`
	NextProbeAt     *time.Time `json:"next_probe_at,omitempty"`
	LastProbeAt     *time.Time `json:"last_probe_at,omitempty"`
	LastProbeResult string     `json:"last_probe_result,omitempty"`
	LastProbeDetail string     `json:"last_probe_detail,omitempty"`
	ProbeAttempts   int        `json:"probe_attempts,omitempty"`
	ProbeIssueID    string     `json:"probe_issue_id,omitempty"`
}

const (
	RateLimitStatusUnknown   = "unknown"
	RateLimitStatusBackoff   = "backoff"
	RateLimitStatusExhausted = "exhausted"
)

type RateLimitBucket struct {
	Remaining      int64      `json:"remaining,omitempty"`
	Used           int64      `json:"used,omitempty"`
	Limit          int64      `json:"limit,omitempty"`
	Cost           int64      `json:"cost,omitempty"`
	Status         string     `json:"status,omitempty"`
	ResetAt        *time.Time `json:"reset_at,omitempty"`
	ObservedAt     *time.Time `json:"observed_at,omitempty"`
	ResetInSeconds int64      `json:"reset_in_seconds,omitempty"`
	HasCredits     bool       `json:"has_credits,omitempty"`
	Unlimited      bool       `json:"unlimited,omitempty"`
	Balance        string     `json:"balance,omitempty"`
}

type GraphQLCost struct {
	TotalQueries    int64                    `json:"total_queries,omitempty"`
	TotalCost       int64                    `json:"total_cost,omitempty"`
	LastHourQueries int64                    `json:"last_hour_queries,omitempty"`
	LastHourCost    int64                    `json:"last_hour_cost,omitempty"`
	Contributors    []GraphQLCostContributor `json:"contributors,omitempty"`
}

type GraphQLCostContributor struct {
	QueryType string `json:"query_type"`
	Count     int64  `json:"count"`
	Cost      int64  `json:"cost"`
}

type RESTUsage struct {
	TotalRequests       int64                  `json:"total_requests,omitempty"`
	ConditionalRequests int64                  `json:"conditional_requests,omitempty"`
	NotModifiedRequests int64                  `json:"not_modified_requests,omitempty"`
	BillableRequests    int64                  `json:"billable_requests,omitempty"`
	RateLimited         bool                   `json:"rate_limited,omitempty"`
	ReserveHeld         bool                   `json:"reserve_held,omitempty"`
	FanoutDeferred      bool                   `json:"fanout_deferred,omitempty"`
	BackoffUntil        *time.Time             `json:"backoff_until,omitempty"`
	Contributors        []RESTUsageContributor `json:"contributors,omitempty"`
	Divergences         []RESTUsageDivergence  `json:"divergences,omitempty"`
}

type RESTUsageDivergence struct {
	CredentialIdentity   string     `json:"credential_identity"`
	Resource             string     `json:"resource"`
	Attribution          string     `json:"attribution"`
	ObservedRequests     int64      `json:"observed_requests"`
	DetentRequests       int64      `json:"detent_requests"`
	AttributedRequests   int64      `json:"attributed_requests,omitempty"`
	UnattributedRequests int64      `json:"unattributed_requests,omitempty"`
	WindowStartedAt      *time.Time `json:"window_started_at,omitempty"`
	LastObservedAt       *time.Time `json:"last_observed_at,omitempty"`
	ResetAt              *time.Time `json:"reset_at,omitempty"`
	WarningEmitted       bool       `json:"warning_emitted,omitempty"`
}

const (
	RESTConsumerOrchestrator = "orchestrator"
	RESTConsumerWorker       = "worker"
	RESTConsumerSharedPool   = "shared_pool"
)

type RESTUsageContributor struct {
	Consumer           string     `json:"consumer,omitempty"`
	CredentialIdentity string     `json:"credential_identity,omitempty"`
	EndpointFamily     string     `json:"endpoint_family"`
	BudgetScope        string     `json:"budget_scope,omitempty"`
	BudgetGate         string     `json:"budget_gate,omitempty"`
	Count              int64      `json:"count"`
	Conditional        int64      `json:"conditional,omitempty"`
	NotModified        int64      `json:"not_modified,omitempty"`
	Billable           int64      `json:"billable,omitempty"`
	Remaining          int64      `json:"remaining,omitempty"`
	Limit              int64      `json:"limit,omitempty"`
	Resource           string     `json:"resource,omitempty"`
	ResetAt            *time.Time `json:"reset_at,omitempty"`
	RetryAfterMS       int64      `json:"retry_after_ms,omitempty"`
	RateLimited        bool       `json:"rate_limited,omitempty"`
	LastStatus         int        `json:"last_status,omitempty"`
}

type RESTBudget struct {
	Consumer            string     `json:"consumer,omitempty"`
	CredentialIdentity  string     `json:"credential_identity"`
	EndpointFamily      string     `json:"endpoint_family"`
	Resource            string     `json:"resource,omitempty"`
	Remaining           int64      `json:"remaining,omitempty"`
	Used                int64      `json:"used,omitempty"`
	Limit               int64      `json:"limit,omitempty"`
	MinRemainingReserve int64      `json:"min_remaining_reserve,omitempty"`
	ResetAt             *time.Time `json:"reset_at,omitempty"`
	ObservedAt          *time.Time `json:"observed_at,omitempty"`
}

type Tokens struct {
	Input              int64           `json:"input_tokens"`
	CachedInput        int64           `json:"cached_input_tokens,omitempty"`
	Output             int64           `json:"output_tokens"`
	ReasoningOutput    int64           `json:"reasoning_output_tokens,omitempty"`
	Total              int64           `json:"total_tokens"`
	Last               *TokenBreakdown `json:"last,omitempty"`
	ModelContextWindow *int64          `json:"model_context_window,omitempty"`
	RuntimeSeconds     float64         `json:"seconds_running,omitempty"`
}

type TokenBreakdown struct {
	Input           int64 `json:"input_tokens"`
	CachedInput     int64 `json:"cached_input_tokens,omitempty"`
	Output          int64 `json:"output_tokens"`
	ReasoningOutput int64 `json:"reasoning_output_tokens,omitempty"`
	Total           int64 `json:"total_tokens"`
}

type ContextPressureState string

const (
	ContextPressureNormal   ContextPressureState = "normal"
	ContextPressureWatch    ContextPressureState = "watch"
	ContextPressureWarning  ContextPressureState = "warning"
	ContextPressureCritical ContextPressureState = "critical"
)

type ContextPressure struct {
	TotalTokens        int64                `json:"total_tokens"`
	ContextLimitTokens int64                `json:"context_limit_tokens"`
	PercentUsed        float64              `json:"percent_used"`
	ThresholdState     ContextPressureState `json:"threshold_state"`
}

func (t Tokens) MarshalJSON() ([]byte, error) {
	type tokensJSON struct {
		Input              int64            `json:"input_tokens"`
		CachedInput        int64            `json:"cached_input_tokens,omitempty"`
		Output             int64            `json:"output_tokens"`
		ReasoningOutput    int64            `json:"reasoning_output_tokens,omitempty"`
		Total              int64            `json:"total_tokens"`
		Last               *TokenBreakdown  `json:"last,omitempty"`
		ModelContextWindow *int64           `json:"model_context_window,omitempty"`
		ContextPressure    *ContextPressure `json:"context_pressure,omitempty"`
		CacheReadFraction  float64          `json:"cache_read_fraction,omitempty"`
		RuntimeSeconds     float64          `json:"seconds_running,omitempty"`
	}

	var pressure *ContextPressure
	if value, ok := t.ContextPressure(); ok {
		pressure = &value
	}
	cacheReadFraction, _ := t.CacheReadFraction()

	return json.Marshal(tokensJSON{
		Input:              t.Input,
		CachedInput:        t.CachedInput,
		Output:             t.Output,
		ReasoningOutput:    t.ReasoningOutput,
		Total:              t.Total,
		Last:               t.Last,
		ModelContextWindow: t.ModelContextWindow,
		ContextPressure:    pressure,
		CacheReadFraction:  cacheReadFraction,
		RuntimeSeconds:     t.RuntimeSeconds,
	})
}

func (t Tokens) ContextPressure() (ContextPressure, bool) {
	if t.ModelContextWindow == nil || *t.ModelContextWindow <= 0 {
		return ContextPressure{}, false
	}
	limit := *t.ModelContextWindow
	total := t.Total
	if t.Last != nil {
		total = t.Last.Total
	}
	if total < 0 {
		total = 0
	}
	percent := float64(total) / float64(limit) * 100
	return ContextPressure{
		TotalTokens:        total,
		ContextLimitTokens: limit,
		PercentUsed:        percent,
		ThresholdState:     ContextPressureStateForPercent(percent),
	}, true
}

func (t Tokens) CacheReadFraction() (float64, bool) {
	input := t.Input
	cached := t.CachedInput
	if input <= 0 || cached <= 0 {
		return 0, false
	}
	if cached > input {
		cached = input
	}
	return float64(cached) / float64(input), true
}

func ContextPressureStateForPercent(percent float64) ContextPressureState {
	switch {
	case percent >= 95:
		return ContextPressureCritical
	case percent >= 85:
		return ContextPressureWarning
	case percent >= 70:
		return ContextPressureWatch
	default:
		return ContextPressureNormal
	}
}

type TokenThroughput struct {
	TokensPerSecond float64 `json:"tokens_per_second"`
	WindowSeconds   int64   `json:"window_seconds"`
	Tokens          int64   `json:"tokens"`
}

type ConcurrencyHistory struct {
	Available      bool                `json:"available"`
	DegradedReason string              `json:"degraded_reason,omitempty"`
	From           time.Time           `json:"from,omitzero"`
	To             time.Time           `json:"to,omitzero"`
	BucketSeconds  int64               `json:"bucket_seconds"`
	AttemptCount   int                 `json:"attempt_count"`
	Series         []ConcurrencySeries `json:"series,omitempty"`
}

type ConcurrencySeries struct {
	ProjectID string              `json:"project_id,omitempty"`
	Buckets   []ConcurrencyBucket `json:"buckets,omitempty"`
}

type ConcurrencyBucket struct {
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	Median        int       `json:"median"`
	P90           int       `json:"p90"`
	Max           int       `json:"max"`
	ActiveSeconds int64     `json:"active_seconds"`
}

type LifetimeTotals struct {
	Available             bool   `json:"available"`
	DegradedReason        string `json:"degraded_reason,omitempty"`
	InputTokens           int64  `json:"input_tokens"`
	CachedInputTokens     int64  `json:"cached_input_tokens,omitempty"`
	OutputTokens          int64  `json:"output_tokens"`
	ReasoningOutputTokens int64  `json:"reasoning_output_tokens,omitempty"`
	TotalTokens           int64  `json:"total_tokens"`
	RuntimeSeconds        int64  `json:"runtime_seconds"`
	Sessions              int64  `json:"sessions"`
	Runs                  int64  `json:"runs"`
	OrphanResumed         int64  `json:"orphan_continuations_resumed,omitempty"`
	OrphanFresh           int64  `json:"orphan_continuations_fresh,omitempty"`
	ResumedInputTokens    int64  `json:"resumed_first_turn_input_tokens,omitempty"`
	ResumedCachedTokens   int64  `json:"resumed_first_turn_cached_input_tokens,omitempty"`
}

func (t LifetimeTotals) ResumedCacheReadFraction() (float64, bool) {
	if t.ResumedInputTokens <= 0 || t.ResumedCachedTokens <= 0 {
		return 0, false
	}
	cached := min(t.ResumedCachedTokens, t.ResumedInputTokens)
	return float64(cached) / float64(t.ResumedInputTokens), true
}

type CycleTimeReport struct {
	Available      bool              `json:"available"`
	DegradedReason string            `json:"degraded_reason,omitempty"`
	Issues         []CycleTimeIssue  `json:"issues,omitempty"`
	Buckets        []CycleTimeBucket `json:"buckets,omitempty"`
	AverageSeconds int64             `json:"average_seconds"`
}

type CycleTimeIssue struct {
	Key             string    `json:"key"`
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`
	DurationSeconds int64     `json:"duration_seconds"`
	Sessions        int64     `json:"sessions"`
}

type CycleTimeBucket struct {
	Label      string `json:"label"`
	MinSeconds int64  `json:"min_seconds"`
	MaxSeconds int64  `json:"max_seconds,omitempty"`
	Count      int    `json:"count"`
}

type WorkflowMetrics struct {
	Available        bool                    `json:"available"`
	DegradedReason   string                  `json:"degraded_reason,omitempty"`
	RuntimeStore     RuntimeStoreEvidence    `json:"runtime_store,omitzero"`
	Windows          []WorkflowMetricsWindow `json:"windows,omitempty"`
	OldestCards      []WorkflowLaneAge       `json:"oldest_cards,omitempty"`
	ActiveBottleneck WorkflowBottleneck      `json:"active_bottleneck,omitzero"`
}

type RuntimeStoreEvidence struct {
	Backend             string                          `json:"backend,omitempty"`
	Status              string                          `json:"status,omitempty"`
	Healthy             bool                            `json:"healthy"`
	Path                string                          `json:"path,omitempty"`
	MigrationStatus     string                          `json:"migration_status,omitempty"`
	MigrationVersion    int64                           `json:"migration_version,omitempty"`
	Tables              []RuntimeStoreTableEvidence     `json:"tables,omitempty"`
	WorkflowPhaseEvents RuntimeStoreWorkflowPhaseEvents `json:"workflow_phase_events,omitzero"`
}

type RuntimeStoreTableEvidence struct {
	Name     string `json:"name"`
	Scope    string `json:"scope,omitempty"`
	RowCount int64  `json:"row_count"`
}

type RuntimeStoreWorkflowPhaseEvents struct {
	RowCount         int64      `json:"row_count"`
	OldestFinishedAt *time.Time `json:"oldest_finished_at,omitempty"`
	NewestFinishedAt *time.Time `json:"newest_finished_at,omitempty"`
}

type WorkflowMetricsWindow struct {
	Label      string                `json:"label"`
	From       time.Time             `json:"from"`
	To         time.Time             `json:"to"`
	Lanes      []WorkflowPhaseMetric `json:"lanes,omitempty"`
	SubPhases  []WorkflowPhaseMetric `json:"sub_phases,omitempty"`
	LaneTrends []WorkflowLaneTrend   `json:"lane_trends,omitempty"`
}

type WorkflowPhaseMetric struct {
	ProjectID             string                      `json:"project_id,omitempty"`
	PhaseType             string                      `json:"phase_type"`
	PhaseName             string                      `json:"phase_name"`
	Count                 int64                       `json:"count"`
	TotalSeconds          int64                       `json:"total_seconds"`
	AverageSeconds        int64                       `json:"average_seconds"`
	P50Seconds            int64                       `json:"p50_seconds"`
	P90Seconds            int64                       `json:"p90_seconds"`
	P95Seconds            int64                       `json:"p95_seconds"`
	InputTokens           int64                       `json:"input_tokens,omitempty"`
	CachedInputTokens     int64                       `json:"cached_input_tokens,omitempty"`
	OutputTokens          int64                       `json:"output_tokens,omitempty"`
	ReasoningOutputTokens int64                       `json:"reasoning_output_tokens,omitempty"`
	TotalTokens           int64                       `json:"total_tokens,omitempty"`
	Turns                 int64                       `json:"turns,omitempty"`
	EndpointFamily        string                      `json:"endpoint_family,omitempty"`
	ActiveSeconds         int64                       `json:"active_seconds,omitempty"`
	WaitSeconds           int64                       `json:"wait_seconds,omitempty"`
	ActivePercent         float64                     `json:"active_percent,omitempty"`
	Representatives       []WorkflowRepresentativeRun `json:"representative_runs,omitempty"`
	Bottleneck            bool                        `json:"bottleneck,omitempty"`
	Comparison            *WorkflowMetricComparison   `json:"comparison,omitempty"`
}

type WorkflowRepresentativeRun struct {
	RunID      int64     `json:"run_id,omitempty"`
	SessionID  int64     `json:"session_id,omitempty"`
	IssueID    string    `json:"issue_id,omitempty"`
	Identifier string    `json:"identifier,omitempty"`
	IssueURL   string    `json:"issue_url,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
}

type WorkflowLaneTrend struct {
	ProjectID  string                   `json:"project_id,omitempty"`
	PhaseName  string                   `json:"phase_name"`
	Points     []WorkflowLaneTrendPoint `json:"points,omitempty"`
	TotalCount int64                    `json:"total_count"`
}

type WorkflowLaneTrendPoint struct {
	Label          string    `json:"label"`
	BucketEnd      time.Time `json:"bucket_end,omitzero"`
	Count          int64     `json:"count"`
	AverageSeconds int64     `json:"average_seconds"`
}

type WorkflowMetricComparison struct {
	Label                  string    `json:"label"`
	PreviousFrom           time.Time `json:"previous_from"`
	PreviousTo             time.Time `json:"previous_to"`
	PreviousCount          int64     `json:"previous_count"`
	PreviousAverageSeconds int64     `json:"previous_average_seconds,omitempty"`
	DeltaSeconds           int64     `json:"delta_seconds"`
	DeltaPercent           float64   `json:"delta_percent,omitempty"`
	Direction              string    `json:"direction"`
}

type WorkflowLaneAge struct {
	ProjectID     string     `json:"project_id,omitempty"`
	IssueID       string     `json:"issue_id,omitempty"`
	Identifier    string     `json:"identifier,omitempty"`
	URL           string     `json:"url,omitempty"`
	Title         string     `json:"title,omitempty"`
	State         string     `json:"state,omitempty"`
	EnteredAt     *time.Time `json:"entered_at,omitempty"`
	AgeSeconds    int64      `json:"age_seconds"`
	BottleneckKey string     `json:"bottleneck_key,omitempty"`
}

type WorkflowBottleneck struct {
	Kind       string     `json:"kind,omitempty"`
	Label      string     `json:"label,omitempty"`
	Detail     string     `json:"detail,omitempty"`
	ProjectID  string     `json:"project_id,omitempty"`
	IssueID    string     `json:"issue_id,omitempty"`
	Identifier string     `json:"identifier,omitempty"`
	Seconds    int64      `json:"seconds,omitempty"`
	Count      int        `json:"count,omitempty"`
	Until      *time.Time `json:"until,omitempty"`
}

func (b WorkflowBottleneck) IsZero() bool {
	return b.Kind == "" &&
		b.Label == "" &&
		b.Detail == "" &&
		b.ProjectID == "" &&
		b.IssueID == "" &&
		b.Identifier == "" &&
		b.Seconds == 0 &&
		b.Count == 0 &&
		b.Until == nil
}

type TokenTrendPoint struct {
	At     time.Time `json:"at"`
	Input  int64     `json:"input_tokens"`
	Output int64     `json:"output_tokens"`
	Total  int64     `json:"total_tokens"`
}
