package templates

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/buildinfo"
	"github.com/digitaldrywood/detent/internal/dispatchpriority"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/projectcolor"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/staleness"
	"github.com/digitaldrywood/detent/internal/telemetry"
	webchart "github.com/digitaldrywood/detent/internal/web/chart"
	"github.com/digitaldrywood/detent/internal/web/ui/primitives"
	"github.com/digitaldrywood/detent/internal/workitem"
)

const (
	throughputTrendWindow        = 10 * time.Minute
	defaultThroughputWindow      = time.Minute
	boardRecoveryOverdueGrace    = time.Minute
	boardRecoveryFailureAttempts = 3
	prPipelineDoneTodayLimit     = 10
	prPipelineMergeSummaryWindow = 24 * time.Hour
	prPipelineActiveMergeTarget  = 5 * time.Minute
	prPipelineQueueWaitTarget    = 10 * time.Minute
	kanbanActionDialogID         = "kanban-action-dialog"
	kanbanDialogContentID        = "kanban-dialog-content"
)

const (
	gitHubAPIWarningRemaining = int64(500)
	gitHubAPIWarningRatio     = 0.1
	httpStatusTooManyRequests = 429
)

const (
	projectKanbanBlockedSourceMetadataKey                = "detent.blocked_source"
	projectKanbanBlockedReasonMetadataKey                = "detent.blocked_reason"
	projectKanbanBlockedRecoveryActionMetadataKey        = "detent.blocked_recovery_action"
	projectKanbanBlockedRecoveryReasonMetadataKey        = "detent.blocked_recovery_reason"
	projectKanbanBlockedRecoveryRemedyMetadataKey        = "detent.blocked_recovery_remedy"
	projectKanbanAutoPromoteActionMetadataKey            = "detent.auto_promote_action"
	projectKanbanAutoPromoteReasonMetadataKey            = "detent.auto_promote_reason"
	projectKanbanAutomatedReviewModeMetadataKey          = "detent.automated_review_mode"
	projectKanbanAutomatedReviewDeadlineMetadataKey      = "detent.automated_review_deadline"
	projectKanbanAutomatedReviewTimeoutActionMetadataKey = "detent.automated_review_timeout_action"
	projectKanbanDispatchSkipReasonMetadataKey           = "detent.dispatch_skip_reason"
	projectKanbanArtifactGateStatusMetadataKey           = "detent.artifact_gate_status"
	hubSyncStatusMetadataKey                             = "hub_sync_status"
	hubSourceSyncedAtMetadataKey                         = "hub_source_synced_at"
)

type DashboardData struct {
	RunnerFleetEnabled        bool
	Title                     string
	ApplicationName           string
	InstanceName              string
	Version                   string
	Build                     buildinfo.Info
	DashboardURL              string
	ConnectorName             string
	Snapshot                  telemetry.Snapshot
	EfficiencyReceipts        []efficiency.Receipt
	Projects                  []ProjectSmallMultiple
	Kanban                    KanbanData
	Assets                    AssetPaths
	ActiveNav                 string
	ProjectID                 string
	ProjectName               string
	ProjectPaused             bool
	ProjectPauseReason        string
	ProjectPauseIssue         string
	ProjectPauseUntil         string
	ProjectPauseExitEvaluated bool
	ProjectPauseExitEvaluable bool
	ProjectPauseExitError     string
	ProjectPauseExitResolver  string
	SidebarCollapsed          bool
	Theme                     string
	Density                   string
	AnalyticsKind             string
	PendingEnrichment         bool
}

type DashboardShellData struct {
	Hosted                 *HostedPageData
	Title                  string
	ApplicationName        string
	InstanceName           string
	Version                string
	Build                  buildinfo.Info
	DashboardURL           string
	ConnectorName          string
	Snapshot               telemetry.Snapshot
	Projects               []ProjectSmallMultiple
	Assets                 AssetPaths
	ActiveNav              string
	ProjectID              string
	ProjectName            string
	SidebarCollapsed       bool
	IncludeDashboardCharts bool
	Theme                  string
	Density                string
	AnalyticsKind          string
}

type Budget = telemetry.Budget

type RateLimits = telemetry.RateLimits

type KanbanData struct {
	Mode                        string
	ProjectID                   string
	TrackerKind                 string
	States                      []string
	ActiveStates                []string
	TerminalStates              []string
	TerminalStatesByProject     map[string][]string
	DispatchPriorityByLabel     []string
	AllowedTransitions          map[string][]string
	ShowBlockedAlerts           bool
	SupportsPullRequestComments bool
	CanMoveCards                bool
	CanRemoveCards              bool
	Projects                    map[string]KanbanProjectData
	Feedback                    string
	FeedbackKind                string
}

type KanbanProjectData struct {
	Mode                        string
	ProjectID                   string
	TrackerKind                 string
	States                      []string
	ActiveStates                []string
	TerminalStates              []string
	DispatchPriorityByLabel     []string
	AllowedTransitions          map[string][]string
	SupportsPullRequestComments bool
	CanMoveCards                bool
	CanRemoveCards              bool
}

type KanbanMoveDialogData struct {
	ProjectID    string
	Board        string
	IssueID      string
	Identifier   string
	IssueURL     string
	Title        string
	CurrentState string
	TargetState  string
	PRNumber     int
	States       []string
	Error        string
}

type KanbanCommentDialogData struct {
	ProjectID    string
	Target       string
	IssueID      string
	PRRepository string
	PRNumber     int
	Identifier   string
	IssueURL     string
	PRURL        string
	Title        string
	Body         string
	Error        string
}

type workAttemptRecoveryControl struct {
	Action         string
	Label          string
	Title          string
	Confirm        string
	ConfirmPayload bool
}

type rateLimitRow struct {
	Name        string
	Remaining   string
	Used        string
	Limit       string
	Reset       string
	UsedPercent int
}

type graphQLBudgetContributorRow struct {
	QueryType string
	Count     string
	Cost      string
	Percent   string
}

type restBudgetContributorRow struct {
	Consumer           string
	CredentialIdentity string
	EndpointFamily     string
	Resource           string
	Count              string
	Remaining          string
	Reset              string
	Status             string
}

type gitHubAPIHealthState string

const (
	gitHubAPIHealthStateUnknown   gitHubAPIHealthState = "unknown"
	gitHubAPIHealthStateAtRest    gitHubAPIHealthState = "at-rest"
	gitHubAPIHealthStateHealthy   gitHubAPIHealthState = "healthy"
	gitHubAPIHealthStateWarning   gitHubAPIHealthState = "warning"
	gitHubAPIHealthStateBackoff   gitHubAPIHealthState = "backoff"
	gitHubAPIHealthStateExhausted gitHubAPIHealthState = "exhausted"
)

type gitHubAPIHealthView struct {
	State     gitHubAPIHealthState
	Label     string
	Summary   string
	Detail    string
	Buckets   []gitHubAPIHealthBucketRow
	Endpoints []gitHubAPIHealthEndpointRow
	Refreshes []gitHubAPIHealthRefreshRow
}

type gitHubAPIHealthBucketRow struct {
	Name      string
	Remaining string
	Used      string
	Limit     string
	Reset     string
}

type gitHubAPIHealthEndpointRow struct {
	EndpointFamily string
	Count          string
	Status         string
	Retry          string
	RetryAfter     string
	Remaining      string
}

type gitHubAPIHealthRefreshRow struct {
	Label string
	Value string
}

type trackerDriftRow struct {
	Title      string
	CountLabel string
	Detail     string
	Issues     []trackerDriftIssueRow
}

type trackerDriftIssueRow struct {
	Number string
	Title  string
	URL    string
	State  string
	Labels string
}

type boardStateRow struct {
	State      string
	Count      int
	CountLabel string
	Percent    string
	DotClass   string
}

type cycleTimeBucketRow struct {
	Label string
	Count string
}

type workflowLaneMetricRow struct {
	Window     string
	Lane       string
	Count      string
	Average    string
	P50        string
	P90        string
	P95        string
	Comparison string
	Trend      string
	TrendClass string
	Delta      string
	Bottleneck bool
	RowClass   string
	Prompt     string
}

type workflowLaneTrendCard struct {
	Lane    string
	Window  string
	Summary string
	Chart   SeriesChartData
	HasData bool
}

type workflowLaneFlowRow struct {
	Lane               string
	Window             string
	Active             string
	Wait               string
	Total              string
	ActivePercent      string
	ActiveStyle        string
	WaitStyle          string
	ActiveSegmentTitle string
	WaitSegmentTitle   string
	HasData            bool
}

type workflowSubphaseMetricRow struct {
	Window string
	Phase  string
	Count  string
	Total  string
	Mean   string
	Detail string
}

type workflowOldestCardRow struct {
	Identifier string
	Title      string
	ProjectID  string
	State      string
	Age        string
	Key        string
	URL        string
}

type workflowBottleneckView struct {
	Label  string
	Detail string
	Value  string
	Count  string
}

type diagnosticsSummaryFact struct {
	Label              string
	Value              string
	Detail             string
	DetailPrefix       string
	DetailReference    string
	DetailReferenceURL string
	DetailSuffix       string
	Kind               primitives.Kind
}

type diagnosticsConditionRow struct {
	ID         string
	Class      observability.Class
	ClassLabel string
	Kind       primitives.Kind
	ProjectID  string
	Target     string
	TargetURL  string
	Summary    string
	Detail     string
	ObservedAt time.Time
}

type runtimeStoreTableRow struct {
	Name     string
	Scope    string
	RowCount string
}

type budgetHistoryBar struct {
	Style string
	Title string
}

type budgetBurnDownViewModel struct {
	Available       bool
	EmptyTitle      string
	EmptyDetail     string
	PeriodLabel     string
	CurrentLabel    string
	CapLabel        string
	ProjectionLabel string
	Chart           BudgetProjectionChartData
}

type ProjectSmallMultiple struct {
	ID                        string
	Name                      string
	URL                       string
	Color                     string
	Pool                      string
	Paused                    bool
	PauseReason               string
	PauseIssue                string
	PauseUntil                string
	PauseExitEvaluated        bool
	PauseExitEvaluable        bool
	PauseExitError            string
	PauseExitResolver         string
	ActiveHours               telemetry.ActiveHours
	Dispatch                  telemetry.DispatchStatus
	Refresh                   telemetry.Refresh
	Running                   int
	QueueCount                int
	Blocked                   int
	BoardLoad                 int
	BoardTodo                 int
	BoardActive               int
	BoardWaiting              int
	BoardBlocked              int
	BoardWorkloadIncomplete   bool
	Completed                 int
	TotalTokens               int64
	ThroughputTokensPerSecond float64
	CurrentSpendUSD           float64
	BudgetEnabled             bool
	PerDayMaxUSD              float64
	PerIssueMaxUSD            float64
	BudgetResetAt             time.Time
	BudgetObservedAt          time.Time
	BudgetOverride            *telemetry.BudgetOverride
	Samples                   []ProjectSmallMultipleSample
}

type ProjectSmallMultipleSample struct {
	At                        time.Time
	Running                   int
	TotalTokens               int64
	ThroughputTokensPerSecond float64
	SpendUSD                  float64
	QueueDepth                int
	Blocked                   int
	Completed                 int
}

type projectSmallMultipleCard struct {
	ID                         string
	Name                       string
	Href                       string
	ExternalURL                string
	ProjectColor               string
	ActivityLabel              string
	PauseDetail                string
	ActiveHoursVisible         bool
	ActiveHoursCozyLabel       string
	ActiveHoursCompactLabel    string
	ActiveHoursHelpTerm        string
	ActiveHoursHelpTitle       string
	ActiveHoursHelpDescription string
	RunningLabel               string
	QueueLabel                 string
	BlockedLabel               string
	CompletedLabel             string
	ThroughputLabel            string
	SpendLabel                 string
	ThroughputChart            SeriesChartData
	SpendChart                 SeriesChartData
	QueueChart                 SeriesChartData
}

type concurrencySeriesCard struct {
	Name    string
	Metrics []concurrencyMetricCard
}

type concurrencyMetricCard struct {
	Label string
	Value string
	Chart SeriesChartData
}

type sidebarProjectItem struct {
	ID           string
	Name         string
	Href         string
	StatusLabel  string
	DotClass     string
	ProjectColor string
	BadgeClass   string
	CountLabel   string
	RunningLabel string
	Breakdown    string
	DefaultIndex int
	Active       bool
	Current      bool
}

type agentTimelineRow struct {
	Identifier        string
	Identity          issueIdentityView
	Title             string
	State             string
	IssueURL          string
	PullRequestURL    string
	PullRequestNumber int
	StartedAt         string
	EndedAt           string
	Duration          string
	StartPercent      string
	EndPercent        string
	Segments          []agentTimelineSegment
}

type agentTimelineSegment struct {
	Label string
	Class string
	Style string
	Title string
	Width string
}

type agentTimelineEntry struct {
	issue   telemetry.Issue
	state   string
	start   time.Time
	end     time.Time
	running bool
}

type runningActivityRow struct {
	At      string
	Event   string
	Message string
}

type prPipelineLane struct {
	ID          string
	Title       string
	CountLabel  string
	DotClass    string
	EmptyTitle  string
	EmptyDetail string
	Cards       []prPipelineCard
}

type prPipelineCard struct {
	IssueNumber      string
	Identity         issueIdentityView
	IdentityToken    string
	Identifier       string
	ProjectID        string
	Title            string
	URL              string
	CIStatus         string
	CIClass          string
	CodexReviewState string
	CodexReviewClass string
	TimeInStage      string
	TimeInStageTitle string
	WaitDetail       string
	MergeLaneStatus  string
	MergeLaneDetail  string
	MergeLanePrefix  string
	MergeLaneRef     string
	MergeLaneRefURL  string
	MergeLaneSuffix  string
	MergeLaneClass   string
	Stage            string
	StageAt          time.Time
}

type prPipelineMergeMetrics struct {
	Available     bool
	Depth         string
	DrainETA      string
	ActiveElapsed string
	QueueWait     string
	RecentCount   string
	ActiveP50     string
	ActiveP90     string
	TotalP50      string
	TotalP90      string
	ActiveWarning bool
	QueueWarning  bool
}

type issueIdentityView struct {
	Repository        string
	IssueNumber       string
	IssueURL          string
	PullRequestNumber int
	PullRequestLabel  string
	PullRequestURL    string
	Label             string
}

type projectKanbanBoard struct {
	AllLanes             []projectKanbanLane
	Lanes                []projectKanbanLane
	EmptyLanes           []projectKanbanLane
	HiddenPopulatedLanes []projectKanbanLane
	TotalCardCount       int
	VisibleCardCount     int
	HiddenCardCount      int
	TotalLabel           string
	VisibleCountLabel    string
	HiddenCountLabel     string
	EmptyCountLabel      string
}

type projectKanbanLane struct {
	ID             string
	Title          string
	CountLabel     string
	DotClass       string
	Empty          bool
	DefaultVisible bool
	Cards          []projectKanbanCard
}

type projectKanbanCard struct {
	IssueNumber           string
	Identity              string
	Identifier            string
	ProjectID             string
	ProjectColor          string
	Title                 string
	Description           string
	URL                   string
	PullRequestLabel      string
	MergeableState        string
	ConflictReason        string
	CIStatus              string
	CIClass               string
	CodexReviewState      string
	CodexReviewClass      string
	TimeInStage           string
	TimeInStageTitle      string
	WaitDetail            string
	GatePending           bool
	BlockedSource         telemetry.BlockedSource
	BlockedReason         string
	BlockedRecoveryAction string
	BlockedRecoveryReason string
	BlockedRecoveryRemedy string
	AttentionLabel        string
	AttentionDetail       string
	MergeLaneStatus       string
	MergeLaneDetail       string
	MergeLaneClass        string
	MergeLaneKind         primitives.Kind
	Stage                 string
	StageAt               time.Time
	AuthorID              string
	Origin                string
	OriginActor           string
	Owner                 string
	LeaseRenewedAt        *time.Time
	LeaseExpiresAt        *time.Time
	UpdatedAt             *time.Time
	SyncStatus            string
	SourceSyncedAt        string
	PriorityRank          int
	PriorityName          string
	DispatchPriorityLabel string
	DispatchPriorityRank  int
	UnblockerCount        int
	Labels                []string
	Assignees             []string
	Comments              []telemetry.IssueComment
	HumanDependencyWait   string
	Blockers              []string
	ClearedBlockers       []string
	BlockerSummary        string
	HasPullRequest        bool
	IssueID               string
	PRNumber              int
	PRRepository          string
	PRURL                 string
	Movable               bool
	RecentCompletion      bool
	DisabledText          string
	RuntimeIdentity       agentidentity.Identity
	ParkSummary           telemetry.ParkSummary
	CompletionProgress    telemetry.CompletionProgress
}

const (
	githubLocalDivergenceMetadataKey         = "github_local_divergence"
	githubLocalDivergenceDetailMetadataKey   = "github_local_divergence_detail"
	githubLocalClosedUpstreamDivergence      = "closed_upstream_local_active"
	githubIssueNumberMetadataKey             = "github_issue_number"
	projectKanbanRecentCompletionMetadataKey = "detent.recent_completion"
	recentCompletionWindow                   = 48 * time.Hour
)

type projectOverviewCard struct {
	ID       string
	Title    string
	Href     string
	Value    string
	Detail   string
	DotClass string
}

type projectKanbanIssueCard struct {
	issue   telemetry.Issue
	state   string
	stageAt time.Time
	rank    int
	index   int
}

func DashboardShellDataFromDashboard(data DashboardData) DashboardShellData {
	return DashboardShellData{
		Title:                  data.Title,
		ApplicationName:        data.ApplicationName,
		InstanceName:           data.InstanceName,
		Version:                data.Version,
		Build:                  data.Build,
		DashboardURL:           data.DashboardURL,
		ConnectorName:          data.ConnectorName,
		Snapshot:               data.Snapshot,
		Projects:               data.Projects,
		Assets:                 data.Assets,
		ActiveNav:              data.ActiveNav,
		ProjectID:              data.ProjectID,
		ProjectName:            data.ProjectName,
		SidebarCollapsed:       data.SidebarCollapsed,
		IncludeDashboardCharts: true,
		Theme:                  data.Theme,
		Density:                data.Density,
		AnalyticsKind:          data.AnalyticsKind,
	}
}

func ProjectKanbanShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "kanban"
	shell.IncludeDashboardCharts = false
	return shell
}

func ProjectRunsShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "runs"
	shell.IncludeDashboardCharts = false
	return shell
}

func ProjectDiagnosticsShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "diagnostics"
	shell.IncludeDashboardCharts = true
	return shell
}

func DiagnosticsShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "diagnostics"
	shell.IncludeDashboardCharts = true
	return shell
}

func HealthShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "health"
	shell.IncludeDashboardCharts = false
	return shell
}

func AnalyticsShellDataFromDashboard(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "analytics"
	shell.IncludeDashboardCharts = true
	return shell
}

func pageTitle(data DashboardShellData) string {
	if data.Title != "" {
		return data.Title
	}
	return "Detent"
}

func versionLabel(data DashboardData) string {
	version := strings.TrimSpace(data.Version)
	if version == "" {
		return "dev"
	}
	return version
}

func buildLabel(data DashboardData) string {
	if buildinfo.IsZero(data.Build) {
		return ""
	}
	return "Build " + buildinfo.DisplayLabel(data.Build)
}

func dashboardBuildVersionLabel(data DashboardData) string {
	if build := buildLabel(data); build != "" {
		return build
	}
	return versionLabel(data)
}

func projectDisplayName(data DashboardData) string {
	name := strings.TrimSpace(data.ProjectName)
	if name != "" {
		return name
	}
	id := strings.TrimSpace(data.ProjectID)
	if id != "" {
		return id
	}
	return "Project"
}

func isProjectDashboard(data DashboardData) bool {
	return strings.TrimSpace(data.ProjectID) != ""
}

func projectExternalURL(data DashboardData) string {
	if !isProjectDashboard(data) {
		return ""
	}
	return strings.TrimSpace(data.Snapshot.Project.URL)
}

func projectExternalLinkLabel(data DashboardData) string {
	name := projectDisplayName(data)
	if name == "" {
		return "Open project issues"
	}
	return "Open " + name + " issues"
}

func chartEndpoint(data DashboardData) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return "/api/v1/projects/" + url.PathEscape(id) + "/timeseries"
	}
	return "/api/v1/timeseries"
}

func eventsPath(data DashboardShellData) string {
	if strings.TrimSpace(data.ProjectID) == "" && strings.TrimSpace(data.ActiveNav) == "board" {
		return "/events?view=board"
	}
	if strings.TrimSpace(data.ProjectID) == "" && strings.TrimSpace(data.ActiveNav) == "fleet" {
		return "/events?view=fleet"
	}
	if strings.TrimSpace(data.ProjectID) == "" && strings.TrimSpace(data.ActiveNav) == "kanban" {
		return "/events?view=kanban"
	}
	if strings.TrimSpace(data.ProjectID) == "" && strings.TrimSpace(data.ActiveNav) == "diagnostics" {
		return "/events?view=diagnostics"
	}
	if activeNav := staticSidebarNav(data.ActiveNav); activeNav != "" {
		values := url.Values{"nav": []string{activeNav}}
		if id := strings.TrimSpace(data.ProjectID); id != "" {
			values.Set("project", id)
		}
		if activeNav == "analytics" {
			if kind := strings.TrimSpace(data.AnalyticsKind); kind != "" {
				values.Set("kind", kind)
			}
		}
		return "/events?" + values.Encode()
	}
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		switch strings.TrimSpace(data.ActiveNav) {
		case "kanban":
			return projectKanbanEventsPath(data)
		case "runs":
			return projectRunsEventsPath(data)
		case "diagnostics":
			return projectDiagnosticsEventsPath(data)
		case "configuration":
			return projectConfigurationEventsPath(data)
		case "overview":
			return "/events?" + url.Values{"project": []string{id}, "view": []string{"overview"}}.Encode()
		}
		return "/events?project=" + url.QueryEscape(id)
	}
	return "/events"
}

func dashboardScopeLabel(data DashboardData) string {
	if isProjectDashboard(data) {
		return "Project: " + projectDisplayName(data)
	}
	return authorizationScopeLabel(data.Snapshot)
}

func dashboardScopeClass(data DashboardData) string {
	if isProjectDashboard(data) {
		return "border-accent/15 bg-accent/15 text-accent"
	}
	return authorizationScopeClass(data.Snapshot)
}

func sidebarFilterVisible(data DashboardShellData) bool {
	return len(sidebarProjectItems(data)) > 10
}

func sidebarFleetActive(data DashboardShellData) bool {
	activeNav := strings.TrimSpace(data.ActiveNav)
	return strings.TrimSpace(data.ProjectID) == "" && activeNav != "kanban" && staticSidebarNav(activeNav) == ""
}

func fleetKanbanNavVisible(data DashboardShellData) bool {
	return len(data.Projects) > 1
}

func fleetKanbanNavActive(data DashboardShellData) bool {
	return strings.TrimSpace(data.ProjectID) == "" && strings.TrimSpace(data.ActiveNav) == "kanban"
}

func fleetKanbanNavAttributes(data DashboardShellData) templ.Attributes {
	attrs := templ.Attributes{
		"data-dashboard-static-nav": "kanban",
		"aria-label":                "Fleet Kanban",
	}
	maps.Copy(attrs, sidebarAriaCurrent(fleetKanbanNavActive(data)))
	return attrs
}

func sidebarStaticNavActive(data DashboardShellData, id string) bool {
	if strings.TrimSpace(id) == "settings" && projectSidebarNavVisible(data) {
		return false
	}
	return strings.TrimSpace(data.ActiveNav) == id
}

func projectSidebarNavVisible(data DashboardShellData) bool {
	return strings.TrimSpace(data.ProjectID) != ""
}

func projectSidebarOverviewPath(data DashboardShellData) string {
	return projectDashboardPath(data.ProjectID)
}

func projectSidebarKanbanPath(data DashboardShellData) string {
	return projectKanbanPath(data.ProjectID)
}

func projectSidebarRunsPath(data DashboardShellData) string {
	return projectRunsPath(data.ProjectID)
}

func projectSidebarDiagnosticsPath(data DashboardShellData) string {
	return projectDiagnosticsPath(data.ProjectID)
}

func projectSidebarConfigurationPath(data DashboardShellData) string {
	return projectConfigurationPath(data.ProjectID)
}

func sidebarReportsPath(data DashboardShellData) string {
	return sidebarStaticPath(data, "/reports")
}

func sidebarAnalyticsPath(data DashboardShellData) string {
	return "/analytics"
}

func sidebarHealthPath(data DashboardShellData) string {
	return "/health/ui"
}

func sidebarSettingsPath(data DashboardShellData) string {
	return sidebarStaticPath(data, "/settings")
}

func sidebarStaticPath(data DashboardShellData, path string) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return path + "?project=" + url.QueryEscape(id)
	}
	return path
}

func projectSidebarOverviewActive(data DashboardShellData) bool {
	activeNav := strings.TrimSpace(data.ActiveNav)
	return projectSidebarNavVisible(data) && (activeNav == "" || activeNav == "project")
}

func projectSidebarKanbanActive(data DashboardShellData) bool {
	return projectSidebarNavVisible(data) && strings.TrimSpace(data.ActiveNav) == "kanban"
}

func projectSidebarRunsActive(data DashboardShellData) bool {
	return projectSidebarNavVisible(data) && strings.TrimSpace(data.ActiveNav) == "runs"
}

func projectSidebarConfigurationActive(data DashboardShellData) bool {
	activeNav := strings.TrimSpace(data.ActiveNav)
	return projectSidebarNavVisible(data) && (activeNav == "configuration" || activeNav == "settings")
}

func projectSidebarDiagnosticsActive(data DashboardShellData) bool {
	return projectSidebarNavVisible(data) && strings.TrimSpace(data.ActiveNav) == "diagnostics"
}

func projectSidebarViewActive(data DashboardShellData, view string) bool {
	switch strings.TrimSpace(view) {
	case "overview":
		return projectSidebarOverviewActive(data)
	case "kanban":
		return projectSidebarKanbanActive(data)
	case "runs":
		return projectSidebarRunsActive(data)
	case "configuration":
		return projectSidebarConfigurationActive(data)
	case "diagnostics":
		return projectSidebarDiagnosticsActive(data)
	default:
		return false
	}
}

func projectSidebarViewAttributes(data DashboardShellData, view string) templ.Attributes {
	attrs := templ.Attributes{
		"data-dashboard-view-nav": true,
		"data-dashboard-view":     strings.TrimSpace(view),
	}
	if projectSidebarViewActive(data, view) {
		attrs["aria-current"] = "page"
	}
	return attrs
}

func staticSidebarNav(activeNav string) string {
	switch strings.TrimSpace(activeNav) {
	case "health":
		return "health"
	case "analytics":
		return "analytics"
	case "reports":
		return "reports"
	case "settings":
		return "settings"
	default:
		return ""
	}
}

func sidebarAriaCurrent(active bool) templ.Attributes {
	if !active {
		return nil
	}
	return templ.Attributes{"aria-current": "page"}
}

func sidebarStaticNavAttributes(data DashboardShellData, id string) templ.Attributes {
	attrs := templ.Attributes{
		"data-dashboard-static-nav": strings.TrimSpace(id),
	}
	maps.Copy(attrs, sidebarAriaCurrent(sidebarStaticNavActive(data, id)))
	return attrs
}

func gitHubAPIHealthSidebarLabel(data DashboardShellData) string {
	return "Health: " + gitHubAPIHealthStateLabel(data.Snapshot) + ". " + gitHubAPIHealth(data.Snapshot).Label
}

func gitHubAPIHealthSidebarTargetAttributes(data DashboardShellData) templ.Attributes {
	return templ.Attributes{
		"id":                           "github-api-health",
		"sse-swap":                     "github-api-health",
		"hx-swap":                      "morph:outerHTML",
		"data-github-api-health-state": string(gitHubAPIHealth(data.Snapshot).State),
	}
}

func gitHubAPIHealthSidebarLinkAttributes(data DashboardShellData) templ.Attributes {
	label := gitHubAPIHealthSidebarLabel(data)
	attrs := templ.Attributes{
		"aria-label": label,
		"title":      label,
	}
	maps.Copy(attrs, sidebarStaticNavAttributes(data, "health"))
	return attrs
}

func sidebarProjectItemAttributes(item sidebarProjectItem) templ.Attributes {
	attrs := templ.Attributes{
		"data-dashboard-project-entry": true,
		"data-project-id":              item.ID,
		"data-project-name":            item.Name,
		"data-project-default-index":   strconv.Itoa(item.DefaultIndex),
	}
	return attrs
}

func sidebarProjectButtonAttributes(item sidebarProjectItem) templ.Attributes {
	attrs := templ.Attributes{}
	maps.Copy(attrs, sidebarAriaCurrent(item.Current))
	return attrs
}

func sidebarProjectOrderAvailable(data DashboardShellData) bool {
	return len(sidebarProjectItems(data)) > 1
}

func sidebarFleetTooltip(data DashboardShellData) string {
	return "Fleet - " + strings.Join([]string{
		runningCountLabel(data.Snapshot) + " running",
		formatCount(queueCount(data.Snapshot)) + " queued",
		formatCount(blockedCount(data.Snapshot)) + " blocked",
	}, ", ")
}

func sidebarProjectTooltip(item sidebarProjectItem) string {
	return item.Name + " - " + item.Breakdown
}

func sidebarProjectSearchLabel(data DashboardShellData) string {
	return "Filter " + formatCount(len(sidebarProjectItems(data))) + " projects"
}

func chartPanelTitle(data DashboardData) string {
	if isProjectDashboard(data) {
		return projectDisplayName(data) + " activity"
	}
	return "Fleet activity"
}

func chartPanelDescription(data DashboardData) string {
	if isProjectDashboard(data) {
		return "Project-scoped activity, token spend, and board flow over the selected window."
	}
	return "Running agents, token throughput, and completions across registered projects."
}

func connectorName(data DashboardData) string {
	if data.ConnectorName != "" {
		return data.ConnectorName
	}
	return "unknown"
}

func runningCount(snapshot telemetry.Snapshot) int {
	if snapshot.Counts.Running != 0 || len(snapshot.Running) == 0 {
		return snapshot.Counts.Running
	}
	return len(snapshot.Running)
}

func runningCountLabel(snapshot telemetry.Snapshot) string {
	count := runningCount(snapshot)
	if snapshot.Runtime.IsZero() {
		return formatCount(count)
	}
	if !snapshot.Runtime.Available() || !snapshot.Runtime.Complete && count == 0 {
		if refreshSnapshotPartiallyFailed(snapshot) && snapshot.Runtime.Available() {
			return formatCount(count) + "+"
		}
		return "unknown"
	}
	if !snapshot.Runtime.Complete {
		return formatCount(count) + "+"
	}
	return formatCount(count)
}

func runtimeCountComplete(snapshot telemetry.Snapshot) bool {
	return snapshot.Runtime.IsZero() || snapshot.Runtime.Available() && snapshot.Runtime.Complete
}

func queueCount(snapshot telemetry.Snapshot) int {
	if snapshot.Counts.Queue != 0 || len(snapshot.Queue) == 0 {
		return snapshot.Counts.Queue
	}
	return len(snapshot.Queue)
}

func blockedCount(snapshot telemetry.Snapshot) int {
	if snapshot.Counts.Blocked != 0 || len(snapshot.Blocked) == 0 {
		return snapshot.Counts.Blocked
	}
	return len(snapshot.Blocked)
}

func completedCount(snapshot telemetry.Snapshot) int {
	if snapshot.Counts.Completed != 0 || len(snapshot.Completed) == 0 {
		return snapshot.Counts.Completed
	}
	return len(snapshot.Completed)
}

func projectSmallMultipleCards(data DashboardData) []projectSmallMultipleCard {
	if len(data.Projects) == 0 {
		return nil
	}

	projects := append([]ProjectSmallMultiple(nil), data.Projects...)
	sortProjectSmallMultiples(projects)

	cards := make([]projectSmallMultipleCard, 0, len(projects))
	for _, project := range projects {
		name := projectSmallMultipleName(project)
		samples := projectSmallMultipleSamples(project)
		activeHoursVisible, activeHoursCozyLabel, activeHoursCompactLabel, activeHoursHelpDescription := projectActiveHoursIndicator(project)
		cards = append(cards, projectSmallMultipleCard{
			ID:                         strings.TrimSpace(project.ID),
			Name:                       name,
			Href:                       projectOpenPath(project.ID),
			ExternalURL:                strings.TrimSpace(project.URL),
			ProjectColor:               projectColorForProject(project),
			ActivityLabel:              projectSmallMultipleActivityLabel(project),
			PauseDetail:                projectSmallMultiplePauseDetail(project),
			ActiveHoursVisible:         activeHoursVisible,
			ActiveHoursCozyLabel:       activeHoursCozyLabel,
			ActiveHoursCompactLabel:    activeHoursCompactLabel,
			ActiveHoursHelpTerm:        "active-hours-" + boardCardSlug(project.ID),
			ActiveHoursHelpTitle:       "Active hours · " + name,
			ActiveHoursHelpDescription: activeHoursHelpDescription,
			RunningLabel:               formatCount(project.Running) + " running",
			QueueLabel:                 formatCount(project.QueueCount) + " queued",
			BlockedLabel:               formatCount(project.Blocked) + " blocked",
			CompletedLabel:             formatCount(project.Completed) + " sessions",
			ThroughputLabel:            formatDecimal(project.ThroughputTokensPerSecond) + " tps",
			SpendLabel:                 formatUSD(project.CurrentSpendUSD),
			ThroughputChart: projectSmallMultipleChart(name+" throughput", samples, "tps", "text-accent", func(sample ProjectSmallMultipleSample) float64 {
				return sample.ThroughputTokensPerSecond
			}),
			SpendChart: projectSmallMultipleChart(name+" notional USD", samples, "notional USD", "text-ok", func(sample ProjectSmallMultipleSample) float64 {
				return sample.SpendUSD
			}),
			QueueChart: projectSmallMultipleChart(name+" queue depth", samples, "queued", "text-warn", func(sample ProjectSmallMultipleSample) float64 {
				return float64(sample.QueueDepth)
			}),
		})
	}
	return cards
}

func projectActiveHoursIndicator(project ProjectSmallMultiple) (bool, string, string, string) {
	active := project.ActiveHours
	if !active.Configured || active.Open {
		return false, "", "", ""
	}
	if active.NextOpen == nil {
		return true, "Off hours", "Off", "Dispatch is outside the configured active-hours window. In-flight agents continue draining."
	}
	opening := projectActiveHoursTime(active.NextOpen, active.Timezone, "15:04 MST")
	detail := "Dispatch reopens at " + projectActiveHoursTime(active.NextOpen, active.Timezone, "Mon, Jan 2 at 15:04 MST") + " in " + active.Timezone + ". In-flight agents continue draining."
	return true, "Off hours · opens " + opening, projectActiveHoursTime(active.NextOpen, active.Timezone, "15:04"), detail
}

func projectActiveHoursTime(value *time.Time, timezone string, layout string) string {
	if value == nil {
		return "unavailable"
	}
	location, err := time.LoadLocation(strings.TrimSpace(timezone))
	if err != nil {
		return value.Format(layout)
	}
	return value.In(location).Format(layout)
}

func projectSmallMultiplePauseDetail(project ProjectSmallMultiple) string {
	if !project.Paused {
		return ""
	}
	detail := projectPauseDetail(project.PauseReason, project.PauseIssue, project.PauseUntil)
	evaluation := projectPauseExitEvaluationDetail(project.PauseIssue, project.PauseExitEvaluated, project.PauseExitEvaluable, project.PauseExitResolver, project.PauseExitError)
	if detail == "" {
		return evaluation
	}
	if evaluation == "" {
		return detail
	}
	return detail + " · " + evaluation
}

func projectPauseDetail(reason string, issue string, until string) string {
	parts := make([]string, 0, 3)
	if reason = strings.TrimSpace(reason); reason != "" {
		parts = append(parts, "Reason: "+reason)
	}
	if issue = strings.TrimSpace(issue); issue != "" {
		parts = append(parts, "Until issue closes: "+issue)
	}
	if until = strings.TrimSpace(until); until != "" {
		parts = append(parts, "Until: "+until)
	}
	return strings.Join(parts, " · ")
}

func projectPauseExitEvaluationDetail(issue string, evaluated bool, evaluable bool, resolver string, evaluationError string) string {
	if strings.TrimSpace(issue) == "" {
		return ""
	}
	if !evaluated {
		return "Evaluation: pending"
	}
	resolver = strings.TrimSpace(resolver)
	if evaluable {
		if resolver == "" {
			return "Evaluation: evaluable"
		}
		return "Evaluation: evaluable via " + resolver
	}
	detail := "Evaluation: unevaluable"
	if evaluationError = strings.TrimSpace(evaluationError); evaluationError != "" {
		detail += ": " + evaluationError
	}
	return detail
}

func sidebarProjectItems(data DashboardShellData) []sidebarProjectItem {
	if len(data.Projects) == 0 {
		return nil
	}

	projects := append([]ProjectSmallMultiple(nil), data.Projects...)
	sortProjectSmallMultiples(projects)
	items := make([]sidebarProjectItem, 0, len(projects))
	for _, project := range projects {
		id := strings.TrimSpace(project.ID)
		if id == "" {
			continue
		}
		status := projectSmallMultipleStatus(project)
		active := strings.TrimSpace(data.ProjectID) == id
		items = append(items, sidebarProjectItem{
			ID:           id,
			Name:         projectSmallMultipleName(project),
			Href:         projectOpenPath(id),
			StatusLabel:  status.Label,
			DotClass:     status.DotClass,
			ProjectColor: projectColorForProject(project),
			BadgeClass:   status.BadgeClass,
			CountLabel:   projectBoardWorkloadCountLabel(project),
			RunningLabel: projectBoardWorkloadCountLabel(project) + " board load",
			Breakdown:    projectWorkloadBreakdown(project),
			DefaultIndex: len(items),
			Active:       active,
			Current:      false,
		})
	}
	return items
}

func projectBoardWorkloadCountLabel(project ProjectSmallMultiple) string {
	if !project.BoardWorkloadIncomplete {
		return formatCount(project.BoardLoad)
	}
	if project.BoardLoad == 0 {
		return "unknown"
	}
	return formatCount(project.BoardLoad) + "+"
}

type projectStatusView struct {
	Label      string
	DotClass   string
	BadgeClass string
}

func sortProjectSmallMultiples(projects []ProjectSmallMultiple) {
	sort.SliceStable(projects, func(i, j int) bool {
		leftName := projectSmallMultipleName(projects[i])
		rightName := projectSmallMultipleName(projects[j])
		leftFolded := strings.ToLower(leftName)
		rightFolded := strings.ToLower(rightName)
		if leftFolded != rightFolded {
			return leftFolded < rightFolded
		}
		if leftName != rightName {
			return leftName < rightName
		}
		return projects[i].ID < projects[j].ID
	})
}

func projectSmallMultipleStatus(project ProjectSmallMultiple) projectStatusView {
	switch {
	case project.Dispatch.NeedsHumanAttention:
		return projectStatusView{Label: "needs attention", DotClass: "bg-err", BadgeClass: "bg-err/15 text-err"}
	case snapshotHasRefreshSignal(project.Refresh) && project.Refresh.Degraded():
		label := "stale"
		if !project.Refresh.StalenessWindowExceeded && strings.TrimSpace(project.Refresh.LastError) != "" {
			label = "refresh failed"
		}
		return projectStatusView{Label: label, DotClass: "bg-warn", BadgeClass: "bg-warn/15 text-warn"}
	case project.Paused:
		return projectStatusView{Label: "paused", DotClass: "bg-warn", BadgeClass: "bg-warn/15 text-warn"}
	case snapshotHasRefreshSignal(project.Refresh) && project.Refresh.Initializing():
		return projectStatusView{Label: "initializing", DotClass: "bg-dim", BadgeClass: "bg-elev text-sec"}
	case snapshotHasRefreshSignal(project.Refresh) && project.Refresh.Behind():
		return projectStatusView{Label: "behind", DotClass: "bg-warn", BadgeClass: "bg-warn/15 text-warn"}
	case project.ActiveHours.Configured && !project.ActiveHours.Open:
		return projectStatusView{Label: "off hours", DotClass: "bg-dim", BadgeClass: "bg-elev text-sec"}
	case project.BoardBlocked > 0:
		dotClass := "bg-dim"
		if project.Running > 0 {
			dotClass = "bg-ok dt-pulse"
		}
		return projectStatusView{Label: "blocked", DotClass: dotClass, BadgeClass: "bg-err/15 text-err"}
	case project.Running > 0:
		return projectStatusView{Label: "active", DotClass: "bg-ok dt-pulse", BadgeClass: "bg-elev text-sec"}
	case project.BoardLoad > 0:
		return projectStatusView{Label: "queued", DotClass: "bg-dim", BadgeClass: "bg-elev text-sec"}
	default:
		return projectStatusView{Label: "idle", DotClass: "bg-dim", BadgeClass: "bg-elev text-sec"}
	}
}

func sidebarProjectBadgeLabel(item sidebarProjectItem) string {
	if item.StatusLabel == "paused" || item.StatusLabel == "off hours" || item.StatusLabel == "needs attention" || item.StatusLabel == "stale" || item.StatusLabel == "refresh failed" || item.StatusLabel == "initializing" || item.StatusLabel == "behind" {
		return item.StatusLabel
	}
	return item.RunningLabel
}

func projectWorkloadBreakdown(project ProjectSmallMultiple) string {
	return strings.Join([]string{
		workloadCountLabel(project.BoardTodo, project.BoardWorkloadIncomplete) + " ready",
		workloadCountLabel(project.BoardActive, project.BoardWorkloadIncomplete) + " active",
		workloadCountLabel(project.BoardWaiting, project.BoardWorkloadIncomplete) + " waiting",
		workloadCountLabel(project.BoardBlocked, project.BoardWorkloadIncomplete) + " blocked",
	}, " · ")
}

func projectColorForProject(project ProjectSmallMultiple) string {
	return projectcolor.ColorFor(project.ID, project.Color)
}

func projectColorForID(projectID string, projects []ProjectSmallMultiple) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return ""
	}
	for _, project := range projects {
		if strings.TrimSpace(project.ID) == projectID {
			return projectColorForProject(project)
		}
	}
	return projectcolor.ColorForID(projectID)
}

func projectColorStyle(color string) string {
	color, ok := projectcolor.Normalize(color)
	if !ok {
		return ""
	}
	return "background-color: " + color
}

func projectColorAttributes(color string) templ.Attributes {
	color, ok := projectcolor.Normalize(color)
	if !ok {
		return nil
	}
	return templ.Attributes{
		"data-project-color": color,
		"style":              projectColorStyle(color),
	}
}

func projectDashboardPath(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "/"
	}
	return "/projects/" + url.PathEscape(projectID)
}

func projectOpenPath(projectID string) string {
	return projectKanbanPath(projectID)
}

func projectKanbanPath(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "/"
	}
	return "/projects/" + url.PathEscape(projectID) + "/kanban"
}

func projectRunsPath(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "/"
	}
	return "/projects/" + url.PathEscape(projectID) + "/runs"
}

func projectDiagnosticsPath(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "/"
	}
	return "/projects/" + url.PathEscape(projectID) + "/diagnostics"
}

func projectStateAPIPath(data DashboardData) string {
	projectID := strings.TrimSpace(data.ProjectID)
	if projectID == "" {
		projectID = strings.TrimSpace(data.Snapshot.Project.ID)
	}
	if projectID == "" {
		return ""
	}
	return "/api/v1/projects/" + url.PathEscape(projectID) + "/state"
}

func projectConfigurationPath(projectID string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return "/settings"
	}
	return "/projects/" + url.PathEscape(projectID) + "/configuration"
}

func projectKanbanEventsPath(data DashboardShellData) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return "/events?project=" + url.QueryEscape(id) + "&view=kanban"
	}
	return "/events?view=kanban"
}

func projectRunsEventsPath(data DashboardShellData) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return "/events?project=" + url.QueryEscape(id) + "&view=runs"
	}
	return "/events?view=runs"
}

func projectDiagnosticsEventsPath(data DashboardShellData) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return "/events?project=" + url.QueryEscape(id) + "&view=diagnostics"
	}
	return "/events?view=diagnostics"
}

func projectConfigurationEventsPath(data DashboardShellData) string {
	if id := strings.TrimSpace(data.ProjectID); id != "" {
		return "/events?project=" + url.QueryEscape(id) + "&view=configuration"
	}
	return "/events?view=configuration"
}

func projectSmallMultiplesGridClass(cards []projectSmallMultipleCard) string {
	if len(cards) <= 1 {
		return "mt-4 grid min-w-0 gap-2"
	}
	return "mt-4 grid min-w-0 gap-2"
}

func projectSmallMultipleName(project ProjectSmallMultiple) string {
	name := strings.TrimSpace(project.Name)
	if name != "" {
		return name
	}
	id := strings.TrimSpace(project.ID)
	if id != "" {
		return id
	}
	return "unknown project"
}

func projectSmallMultipleActivityLabel(project ProjectSmallMultiple) string {
	if project.Paused {
		return "paused / " + formatCount(project.Running) + " running / " +
			formatCount(project.QueueCount) + " queued / " +
			formatCount(project.Blocked) + " blocked"
	}
	return formatCount(project.Running) + " running / " +
		formatCount(project.QueueCount) + " queued / " +
		formatCount(project.Blocked) + " blocked"
}

func projectSmallMultipleSamples(project ProjectSmallMultiple) []ProjectSmallMultipleSample {
	if len(project.Samples) > 0 {
		return append([]ProjectSmallMultipleSample(nil), project.Samples...)
	}
	return []ProjectSmallMultipleSample{
		{
			Running:                   project.Running,
			TotalTokens:               project.TotalTokens,
			ThroughputTokensPerSecond: project.ThroughputTokensPerSecond,
			SpendUSD:                  project.CurrentSpendUSD,
			QueueDepth:                project.QueueCount,
			Blocked:                   project.Blocked,
			Completed:                 project.Completed,
		},
	}
}

func projectSmallMultipleChart(
	title string,
	samples []ProjectSmallMultipleSample,
	valueSuffix string,
	colorClass string,
	value func(ProjectSmallMultipleSample) float64,
) SeriesChartData {
	points := make([]webchart.Point, 0, len(samples))
	for _, sample := range samples {
		points = append(points, webchart.Point{
			Label: projectSmallMultipleSampleLabel(sample.At),
			Value: value(sample),
		})
	}
	return SeriesChartData{
		Title:       title,
		AriaLabel:   title + " sparkline",
		Points:      points,
		ValueSuffix: valueSuffix,
		Class:       "h-12",
		ColorClass:  colorClass,
		Height:      48,
	}
}

func projectSmallMultipleSampleLabel(at time.Time) string {
	if at.IsZero() {
		return "latest"
	}
	return localTimeToken(at, LocalTimeWithSeconds)
}

func generatedAtLabel(snapshot telemetry.Snapshot) string {
	if snapshot.GeneratedAt.IsZero() {
		return "Snapshot pending"
	}
	return "Updated " + localTimeToken(snapshot.GeneratedAt, LocalDateTimeSeconds)
}

func snapshotReadinessStatus(snapshot telemetry.Snapshot) telemetry.RefreshStatus {
	if !snapshotHasRefreshSignal(snapshot.Refresh) && (!snapshot.GeneratedAt.IsZero() || snapshotHasLoadedData(snapshot)) {
		return telemetry.RefreshStatusReady
	}
	return snapshot.Refresh.ReadinessStatus()
}

func snapshotHasRefreshSignal(refresh telemetry.Refresh) bool {
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

func snapshotHasLoadedData(snapshot telemetry.Snapshot) bool {
	return snapshot.Project != (telemetry.Project{}) ||
		len(snapshot.Projects) > 0 ||
		len(snapshot.BoardIssues) > 0 ||
		len(snapshot.Pipeline) > 0 ||
		len(snapshot.Running) > 0 ||
		len(snapshot.Queue) > 0 ||
		len(snapshot.Blocked) > 0 ||
		len(snapshot.Completed) > 0 ||
		snapshot.Counts != (telemetry.Counts{}) ||
		snapshot.Tokens != (telemetry.Tokens{}) ||
		snapshot.RateLimits != nil ||
		snapshot.LifetimeTotals.Available ||
		snapshot.CycleTime.Available
}

func diagnosticsSnapshotHasLoadedData(snapshot telemetry.Snapshot) bool {
	return len(snapshot.BoardIssues) > 0 ||
		len(snapshot.Pipeline) > 0 ||
		len(snapshot.Running) > 0 ||
		len(snapshot.Queue) > 0 ||
		len(snapshot.Blocked) > 0 ||
		len(snapshot.Completed) > 0 ||
		len(snapshot.Events) > 0 ||
		len(snapshot.WorkAttempts) > 0 ||
		len(snapshot.SchedulerDecisions) > 0 ||
		len(snapshot.StalenessWarnings) > 0 ||
		len(snapshot.DispatchStalls) > 0 ||
		len(snapshot.BackendOutages) > 0 ||
		len(snapshot.DispatchRecoveries) > 0 ||
		len(snapshot.AdmissionProposals) > 0 ||
		len(snapshot.FailureBreakers) > 0 ||
		len(snapshot.StrandedActiveIssues) > 0 ||
		len(snapshot.TokenTrend) > 0 ||
		snapshot.Counts != (telemetry.Counts{}) ||
		snapshot.Tokens != (telemetry.Tokens{}) ||
		snapshot.Throughput != (telemetry.TokenThroughput{}) ||
		snapshot.RateLimits != nil ||
		snapshotHasRefreshSignal(snapshot.Refresh) ||
		len(snapshot.TrackerUnavailable) > 0 ||
		len(snapshot.ForgeUnavailable) > 0 ||
		len(snapshot.CIUnavailable) > 0 ||
		!snapshot.Update.IsZero() ||
		!snapshot.Release.IsZero() ||
		diagnosticsBudgetHasLoadedData(snapshot.Budget) ||
		diagnosticsProjectSnapshotsHaveLoadedData(snapshot.Projects) ||
		snapshot.LifetimeTotals.Available ||
		snapshot.CycleTime.Available ||
		diagnosticsWorkflowMetricsHasLoadedData(snapshot.WorkflowMetrics)
}

func diagnosticsProjectSnapshotsHaveLoadedData(projects []telemetry.ProjectSnapshot) bool {
	for _, project := range projects {
		if project.Counts != (telemetry.Counts{}) ||
			project.Tokens != (telemetry.Tokens{}) ||
			project.Throughput != (telemetry.TokenThroughput{}) ||
			!project.Auth.IsZero() ||
			snapshotHasRefreshSignal(project.Refresh) {
			return true
		}
	}
	return false
}

func diagnosticsBudgetHasLoadedData(budget telemetry.Budget) bool {
	return budget.Enabled ||
		strings.TrimSpace(budget.DegradedReason) != "" ||
		budget.PerDayMaxUSD != nil ||
		budget.PerIssueMaxUSD != nil ||
		budget.CurrentSpendUSD != 0 ||
		budget.ProjectedCostUSD != 0 ||
		budget.ProjectedSpendUSD != 0 ||
		!budget.PeriodStart.IsZero() ||
		!budget.PeriodEnd.IsZero() ||
		len(budget.SpendPoints) > 0 ||
		len(budget.Days) > 0 ||
		len(budget.Refusals) > 0
}

func diagnosticsWorkflowMetricsHasLoadedData(report telemetry.WorkflowMetrics) bool {
	return report.Available ||
		strings.TrimSpace(report.DegradedReason) != "" ||
		diagnosticsRuntimeStoreHasLoadedData(report.RuntimeStore) ||
		len(report.Windows) > 0 ||
		len(report.OldestCards) > 0 ||
		!report.ActiveBottleneck.IsZero()
}

func diagnosticsRuntimeStoreHasLoadedData(store telemetry.RuntimeStoreEvidence) bool {
	return strings.TrimSpace(store.Backend) != "" ||
		strings.TrimSpace(store.Status) != "" ||
		store.Healthy ||
		strings.TrimSpace(store.Path) != "" ||
		strings.TrimSpace(store.MigrationStatus) != "" ||
		store.MigrationVersion != 0 ||
		len(store.Tables) > 0 ||
		store.WorkflowPhaseEvents.RowCount != 0 ||
		store.WorkflowPhaseEvents.OldestFinishedAt != nil ||
		store.WorkflowPhaseEvents.NewestFinishedAt != nil
}

func snapshotReady(snapshot telemetry.Snapshot) bool {
	if snapshot.LastKnown {
		return false
	}
	status := snapshotReadinessStatus(snapshot)
	return status == telemetry.RefreshStatusReady ||
		status == telemetry.RefreshStatusBehind ||
		status == telemetry.RefreshStatusPartial
}

func snapshotUsesStartupCache(snapshot telemetry.Snapshot) bool {
	return snapshot.LastKnown &&
		snapshot.Tracker.Source == telemetry.SnapshotSourceCached &&
		snapshot.Runtime.Source == telemetry.SnapshotSourceUnknown &&
		snapshotInitializing(snapshot)
}

func snapshotInitializing(snapshot telemetry.Snapshot) bool {
	return snapshotReadinessStatus(snapshot) == telemetry.RefreshStatusInitializing
}

func snapshotDegraded(snapshot telemetry.Snapshot) bool {
	return snapshotReadinessStatus(snapshot) == telemetry.RefreshStatusDegraded
}

func snapshotReadinessTitle(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		if snapshotHasPriorTrackerSnapshot(snapshot) {
			return "Tracker refresh degraded."
		}
		return "Tracker refresh failed."
	}
	return "Loading tracker state..."
}

func snapshotReadinessDetail(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		if snapshotHasPriorTrackerSnapshot(snapshot) {
			return snapshotDegradedRefreshDetail(snapshot)
		}
		return snapshotFirstRefreshFailureDetail(snapshot)
	}
	if snapshot.Refresh.NextRefreshAt != nil {
		return "Detent is waiting for the first successful tracker snapshot. Next refresh is scheduled for " + timeLabel(*snapshot.Refresh.NextRefreshAt) + "."
	}
	return "Detent is waiting for the first successful tracker snapshot before showing board counts or empty states."
}

func snapshotHasPriorTrackerSnapshot(snapshot telemetry.Snapshot) bool {
	if snapshot.Refresh.LastRefreshAt != nil ||
		len(snapshot.BoardIssues) > 0 ||
		len(snapshot.Pipeline) > 0 ||
		len(snapshot.Running) > 0 ||
		len(snapshot.Queue) > 0 ||
		len(snapshot.Blocked) > 0 ||
		len(snapshot.Completed) > 0 ||
		snapshot.Counts != (telemetry.Counts{}) {
		return true
	}
	for _, project := range snapshot.Projects {
		if projectSnapshotHasPriorTrackerData(project) {
			return true
		}
	}
	return false
}

func projectSnapshotHasPriorTrackerData(project telemetry.ProjectSnapshot) bool {
	return project.Refresh.LastRefreshAt != nil ||
		project.Refresh.Ready() ||
		project.Counts != (telemetry.Counts{})
}

func snapshotFirstRefreshFailureDetail(snapshot telemetry.Snapshot) string {
	parts := []string{"Detent could not load the first tracker snapshot."}
	if err := snapshotReadinessErrorSummary(snapshot.Refresh.LastError); err != "" {
		parts = append(parts, err)
	}
	if snapshot.Refresh.LastErrorAt != nil {
		parts = append(parts, "Last error at "+timeLabel(*snapshot.Refresh.LastErrorAt)+".")
	}
	return strings.Join(parts, " ")
}

func snapshotDegradedRefreshDetail(snapshot telemetry.Snapshot) string {
	parts := []string{}
	if err := snapshotReadinessErrorSummary(snapshot.Refresh.LastError); err != "" {
		parts = append(parts, err)
	} else {
		parts = append(parts, "The latest tracker refresh failed. Detent will retry on the next refresh.")
	}
	if snapshot.Refresh.LastRefreshAt != nil {
		parts = append(parts, "Last successful refresh: "+timeLabel(*snapshot.Refresh.LastRefreshAt)+".")
	}
	return strings.Join(parts, " ")
}

func snapshotReadinessErrorSummary(raw string) string {
	err := strings.TrimSpace(raw)
	if err == "" {
		return ""
	}
	if status, ok := githubTransientStatus(err); ok {
		return fmt.Sprintf("GitHub returned a transient %d while %s. Detent will retry on the next refresh.", status, githubTransientOperation(err))
	}
	return sanitizeReadinessError(err)
}

func githubTransientStatus(err string) (int, bool) {
	lower := strings.ToLower(err)
	marker := "github transient error: status "
	index := strings.Index(lower, marker)
	if index < 0 {
		return 0, false
	}
	rest := lower[index+len(marker):]
	status := 0
	for _, char := range rest {
		if char < '0' || char > '9' {
			break
		}
		status = status*10 + int(char-'0')
	}
	return status, status >= 500 && status <= 599
}

func githubTransientOperation(err string) string {
	lower := strings.ToLower(err)
	switch {
	case strings.Contains(lower, "workspace cleanup candidate fetch failed"):
		return "fetching workspace cleanup candidates"
	case strings.Contains(lower, "fetch github pull request reviews"):
		return "fetching GitHub pull request reviews"
	case strings.Contains(lower, "fetch github pull request"):
		return "fetching GitHub pull request data"
	case strings.Contains(lower, "fetch github issue"):
		return "fetching GitHub issue data"
	default:
		return "refreshing tracker data"
	}
}

func sanitizeReadinessError(err string) string {
	err = strings.Join(strings.Fields(err), " ")
	htmlIndex := firstHTMLIndex(err)
	if htmlIndex < 0 {
		return err
	}
	prefix := strings.TrimRight(strings.TrimSpace(err[:htmlIndex]), ": ")
	if prefix == "" {
		return "Upstream returned an HTML error page. Check logs for details."
	}
	return prefix + ". Check logs for details."
}

func firstHTMLIndex(value string) int {
	lower := strings.ToLower(value)
	first := -1
	for _, marker := range []string{"<!doctype html", "<html", "<head", "<body"} {
		index := strings.Index(lower, marker)
		if index >= 0 && (first < 0 || index < first) {
			first = index
		}
	}
	return first
}

func snapshotReadinessDotClass(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		return "bg-err"
	}
	return "bg-dim"
}

func snapshotReadinessClass(snapshot telemetry.Snapshot) string {
	base := "rounded-md border p-3 shadow-sm"
	if snapshotDegraded(snapshot) && snapshotHasPriorTrackerSnapshot(snapshot) {
		return base + " border-warn/15 bg-elev/40 text-text"
	}
	if snapshotDegraded(snapshot) {
		return base + " border-err/15 bg-err/15 text-err"
	}
	return base + " border-line bg-elev/40 text-text"
}

func snapshotReadinessDetailClass(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) && snapshotHasPriorTrackerSnapshot(snapshot) {
		return "text-sec"
	}
	if snapshotDegraded(snapshot) {
		return "text-err"
	}
	return "text-sec"
}

func manualRefreshVisible(snapshot telemetry.Snapshot) bool {
	return snapshotDegraded(snapshot)
}

func manualRefreshStatusVisible(attempt *telemetry.RefreshAttempt) bool {
	return attempt != nil && !attempt.IsZero()
}

func manualRefreshStatusLabel(attempt *telemetry.RefreshAttempt) string {
	if attempt == nil {
		return ""
	}
	switch attempt.Status {
	case telemetry.RefreshAttemptStatusInProgress:
		return "Retrying"
	case telemetry.RefreshAttemptStatusCoalesced:
		return "Already retrying"
	case telemetry.RefreshAttemptStatusSucceeded:
		return "Refresh succeeded"
	case telemetry.RefreshAttemptStatusFailed:
		return "Refresh failed"
	case telemetry.RefreshAttemptStatusRefused:
		return "Refresh refused"
	default:
		return "Refresh requested"
	}
}

func manualRefreshStatusDetail(attempt *telemetry.RefreshAttempt) string {
	if attempt == nil {
		return ""
	}
	switch attempt.Status {
	case telemetry.RefreshAttemptStatusInProgress:
		if attempt.RequestedAt != nil {
			return "Requested " + timeLabel(*attempt.RequestedAt) + "."
		}
		return "Request queued."
	case telemetry.RefreshAttemptStatusCoalesced:
		if attempt.RequestedAt != nil {
			return "Request joined the active refresh at " + timeLabel(*attempt.RequestedAt) + "."
		}
		return "Request joined the active refresh."
	case telemetry.RefreshAttemptStatusSucceeded:
		if attempt.CompletedAt != nil {
			return "Completed " + timeLabel(*attempt.CompletedAt) + "."
		}
		return "Latest tracker snapshot is ready."
	case telemetry.RefreshAttemptStatusFailed:
		if err := sanitizeReadinessError(attempt.LastError); err != "" {
			return err
		}
		return "The forced refresh failed."
	case telemetry.RefreshAttemptStatusRefused:
		if err := sanitizeReadinessError(attempt.LastError); err != "" {
			if attempt.RetryAt != nil && !attempt.RetryAt.IsZero() {
				return err + " Retry at " + localTimeToken(*attempt.RetryAt, LocalTimeOnly) + "."
			}
			return err
		}
		if attempt.RetryAt != nil && !attempt.RetryAt.IsZero() {
			return "Hard rate-limit backoff is active. Retry at " + localTimeToken(*attempt.RetryAt, LocalTimeOnly) + "."
		}
		return "Hard rate-limit backoff is active."
	default:
		return ""
	}
}

func manualRefreshStatusClass(attempt *telemetry.RefreshAttempt) string {
	base := "min-w-0 rounded-md border px-3 py-2 text-xs"
	if attempt == nil {
		return base + " border-line bg-elev text-sec"
	}
	switch attempt.Status {
	case telemetry.RefreshAttemptStatusSucceeded:
		return base + " border-ok bg-ok/15 text-ok"
	case telemetry.RefreshAttemptStatusFailed, telemetry.RefreshAttemptStatusRefused:
		return base + " border-err bg-err/15 text-err"
	case telemetry.RefreshAttemptStatusCoalesced:
		return base + " border-accent bg-accent/15 text-accent"
	default:
		return base + " border-warn bg-warn/15 text-warn"
	}
}

func issueIdentifier(issue telemetry.Issue) string {
	if issue.Identifier != "" {
		return issue.Identifier
	}
	if issue.ID != "" {
		return issue.ID
	}
	return "unknown"
}

func issueTitle(issue telemetry.Issue) string {
	if issue.Title != "" {
		return issue.Title
	}
	return "Untitled issue"
}

func issueProjectLabel(issue telemetry.Issue) string {
	projectID := strings.TrimSpace(issue.ProjectID)
	if projectID == "" {
		return ""
	}
	return projectID
}

func issueIdentity(issue telemetry.Issue) issueIdentityView {
	repository := issueRepositoryLabel(issue)
	issueNumber := issueReference(issue)
	prNumber := pullRequestNumber(issue)
	prLabel := ""
	if prNumber > 0 {
		prLabel = "PR #" + strconv.Itoa(prNumber)
	}

	label := issueNumber
	if repository != "" && issueNumber != "" {
		label = repository + " " + issueNumber
	} else if repository != "" {
		label = repository
	}
	if label == "" {
		label = issueIdentifier(issue)
	}
	if prLabel != "" {
		label += " · " + prLabel
	}

	return issueIdentityView{
		Repository:        repository,
		IssueNumber:       issueNumber,
		IssueURL:          issueURL(issue),
		PullRequestNumber: prNumber,
		PullRequestLabel:  prLabel,
		PullRequestURL:    pullRequestURL(issue),
		Label:             label,
	}
}

func issueReference(issue telemetry.Issue) string {
	if reference := issueDisplayReference(issue); reference != "" {
		return reference
	}
	return issueIdentifier(issue)
}

func issueDisplayReference(issue telemetry.Issue) string {
	identifier := issueIdentifier(issue)
	if index := strings.LastIndex(identifier, "#"); index >= 0 && index < len(identifier)-1 {
		return identifier[index:]
	}
	if number := issueMetadataInt(issue.Metadata, githubIssueNumberMetadataKey); number > 0 {
		return "#" + strconv.Itoa(number)
	}
	if issue.Number > 0 {
		return "#" + strconv.Itoa(issue.Number)
	}
	return ""
}

func issueMetadataInt(metadata map[string]string, key string) int {
	value := strings.TrimSpace(metadata[key])
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func issueRepositoryLabel(issue telemetry.Issue) string {
	if repository := issueRepository(issue.Identifier); repository != "" {
		return repository
	}
	if repository := repositoryFromRecordURL(issue.URL); repository != "" {
		return repository
	}
	return pullRequestRepository(issue)
}

func identityRepositoryClass(accent bool) string {
	if accent {
		return "text-accent"
	}
	return "text-text"
}

func identityIssueBadgeClass(accent bool) string {
	if accent {
		return "border-accent/15 bg-accent/15 text-accent"
	}
	return "border-line bg-elev text-text"
}

func identityPullRequestBadgeClass(accent bool) string {
	if accent {
		return "border-accent/15 bg-accent/15 text-accent"
	}
	return "border-accent/15 bg-surface text-accent"
}

func issueDescriptionPreview(issue telemetry.Issue) string {
	description := strings.Join(strings.Fields(issue.Description), " ")
	if description == "" {
		return ""
	}

	const limit = 180
	runes := []rune(description)
	if len(runes) <= limit {
		return description
	}
	return string(runes[:limit-3]) + "..."
}

func issueClaimSummary(issue telemetry.Issue) string {
	parts := make([]string, 0, 2)
	if strings.TrimSpace(issue.Owner) != "" {
		parts = append(parts, "Owner "+strings.TrimSpace(issue.Owner))
	}
	if issue.LeaseExpiresAt != nil {
		label := "Lease expires "
		if issue.LeaseStale {
			label = "Lease stale since "
		}
		parts = append(parts, label+timeLabel(*issue.LeaseExpiresAt))
	}
	return strings.Join(parts, " / ")
}

func issueDetailURL(issue telemetry.Issue) string {
	identifier := issueIdentifier(issue)
	if identifier == "" || identifier == "unknown" {
		return ""
	}
	return "/api/v1/" + url.PathEscape(identifier)
}

func issueState(issue telemetry.Issue, fallback string) string {
	if issue.State != "" {
		return issue.State
	}
	return fallback
}

func sessionLabel(sessionID string) string {
	if sessionID == "" {
		return "n/a"
	}
	if len(sessionID) <= 18 {
		return sessionID
	}
	return sessionID[:10] + "..." + sessionID[len(sessionID)-5:]
}

func runningRuntime(row telemetry.Running, generatedAt time.Time) string {
	return formatDuration(runningRuntimeSeconds(row, generatedAt)) + " / " + formatInt(int64(row.TurnCount)) + " turns"
}

func runningRuntimeSeconds(row telemetry.Running, generatedAt time.Time) float64 {
	seconds := row.RuntimeSeconds
	if seconds <= 0 && !row.StartedAt.IsZero() && !generatedAt.IsZero() {
		seconds = generatedAt.Sub(row.StartedAt).Seconds()
	}
	return seconds
}

func lastCodexUpdate(row telemetry.Running) string {
	if row.LastMessage != "" {
		return displayOutputText(row.LastMessage, row.LastMessageTruncation)
	}
	if row.LastEvent != "" {
		return row.LastEvent
	}
	return "No Codex update yet."
}

func lastCodexMeta(row telemetry.Running) string {
	if row.LastEvent == "" && row.LastEventAt == nil {
		return "n/a"
	}
	parts := make([]string, 0, 2)
	if row.LastEvent != "" {
		parts = append(parts, row.LastEvent)
	}
	if row.LastEventAt != nil {
		parts = append(parts, localTimeToken(*row.LastEventAt, LocalTimeWithSeconds))
	}
	return strings.Join(parts, " / ")
}

func runningActivityID(prefix string, row telemetry.Running) string {
	return projectKanbanLaneID(prefix) + "-activity-" + runningActivityKey(row)
}

func runningActivityDetailsID(prefix string, row telemetry.Running) string {
	return runningActivityID(prefix, row) + "-details"
}

func runningActivityKey(row telemetry.Running) string {
	for _, value := range []string{row.SessionID, row.ID, row.Identifier} {
		key := projectKanbanLaneID(value)
		if key != "unknown" {
			return key
		}
	}
	return "unknown"
}

func runningActivityRows(row telemetry.Running) []runningActivityRow {
	events := row.RecentEvents
	if len(events) == 0 && row.LastEventAt != nil {
		events = []telemetry.ActivityEvent{
			{
				At:      *row.LastEventAt,
				Event:   row.LastEvent,
				Message: lastCodexUpdate(row),
			},
		}
	}
	if len(events) == 0 {
		return nil
	}

	start := 0
	if len(events) > 5 {
		start = len(events) - 5
	}
	rows := make([]runningActivityRow, 0, len(events)-start)
	for i := len(events) - 1; i >= start; i-- {
		event := events[i]
		rows = append(rows, runningActivityRow{
			At:      activityTimeLabel(event.At),
			Event:   activityValue(event.Event, "event"),
			Message: displayOutputText(activityValue(event.Message, "No message recorded."), event.Truncation),
		})
	}
	return rows
}

func displayOutputText(value string, truncation *runtimeoutput.Truncation) string {
	if strings.TrimSpace(value) == "" || truncation == nil || !truncation.Truncated {
		return value
	}
	return value + " [truncated]"
}

func activityTimeLabel(at time.Time) string {
	if at.IsZero() {
		return "n/a"
	}
	return localTimeToken(at, LocalTimeWithSeconds)
}

func activityValue(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func projectKanbanBoardView(data DashboardData) projectKanbanBoard {
	cardsByState := projectKanbanCardsByState(data)
	states := projectKanbanStateOrder(data.Kanban.States, cardsByState)
	terminalStates := projectKanbanTerminalStateSet(data.Kanban.TerminalStates)
	allLanes := make([]projectKanbanLane, 0, len(states))
	visibleLanes := make([]projectKanbanLane, 0, len(states))
	emptyLanes := make([]projectKanbanLane, 0, len(states))
	hiddenPopulatedLanes := make([]projectKanbanLane, 0, len(states))
	total := 0
	visibleTotal := 0
	hiddenTotal := 0
	for _, state := range states {
		cards := cardsByState[projectKanbanStateKey(state)]
		defaultVisible := len(cards) > 0 && !projectKanbanTerminalState(state, terminalStates)
		lane := projectKanbanLane{
			ID:             projectKanbanLaneID(state),
			Title:          state,
			CountLabel:     formatCount(len(cards)),
			DotClass:       boardStateDotClass(state),
			Empty:          len(cards) == 0,
			DefaultVisible: defaultVisible,
			Cards:          cards,
		}
		allLanes = append(allLanes, lane)
		if lane.Empty {
			emptyLanes = append(emptyLanes, lane)
			continue
		}
		total += len(cards)
		if lane.DefaultVisible {
			visibleLanes = append(visibleLanes, lane)
			visibleTotal += len(cards)
		} else if len(cards) > 0 {
			hiddenPopulatedLanes = append(hiddenPopulatedLanes, lane)
			hiddenTotal += len(cards)
		}
	}
	return projectKanbanBoard{
		AllLanes:             allLanes,
		Lanes:                visibleLanes,
		EmptyLanes:           emptyLanes,
		HiddenPopulatedLanes: hiddenPopulatedLanes,
		TotalCardCount:       total,
		VisibleCardCount:     visibleTotal,
		HiddenCardCount:      hiddenTotal,
		TotalLabel:           formatCount(total),
		VisibleCountLabel:    formatCount(visibleTotal),
		HiddenCountLabel:     formatCount(hiddenTotal),
		EmptyCountLabel:      formatCount(len(emptyLanes)),
	}
}

func projectOverviewCards(data DashboardData) []projectOverviewCard {
	board := projectKanbanBoardView(data)
	return []projectOverviewCard{
		{
			ID:       "kanban",
			Title:    "Kanban",
			Href:     projectKanbanPath(data.ProjectID),
			Value:    board.TotalLabel + " cards",
			Detail:   projectOverviewKanbanDetail(board),
			DotClass: "bg-accent",
		},
		{
			ID:       "runs",
			Title:    "Runs",
			Href:     projectRunsPath(data.ProjectID),
			Value:    runningCountLabel(data.Snapshot) + " running",
			Detail:   projectOverviewRunsDetail(data.Snapshot),
			DotClass: projectOverviewRunsDotClass(data.Snapshot),
		},
		{
			ID:       "diagnostics",
			Title:    "Diagnostics",
			Href:     projectDiagnosticsPath(data.ProjectID),
			Value:    runtimeStatusLabel(data.Snapshot),
			Detail:   projectOverviewDiagnosticsDetail(data.Snapshot),
			DotClass: projectOverviewDiagnosticsDotClass(data.Snapshot),
		},
		{
			ID:       "reports",
			Title:    "Reports",
			Href:     sidebarReportsPath(DashboardShellDataFromDashboard(data)),
			Value:    budgetSpendTodayLabel(data.Snapshot.Budget),
			Detail:   formatTokens(data.Snapshot.Tokens) + " tracked",
			DotClass: "bg-ok",
		},
	}
}

func projectOverviewKanbanDetail(board projectKanbanBoard) string {
	if len(board.AllLanes) == 0 {
		return "No workflow lanes"
	}
	if board.HiddenCardCount > 0 {
		return board.VisibleCountLabel + " visible / " + board.TotalLabel + " total cards"
	}
	return formatCount(len(board.Lanes)) + " active / " + formatCount(len(board.AllLanes)) + " lanes"
}

func projectOverviewRunsDetail(snapshot telemetry.Snapshot) string {
	return formatCount(queueCount(snapshot)) + " queued / " + formatCount(blockedCount(snapshot)) + " blocked"
}

func projectOverviewRunsDotClass(snapshot telemetry.Snapshot) string {
	if blockedCount(snapshot) > 0 {
		return "bg-err"
	}
	if queueCount(snapshot) > 0 {
		return "bg-warn"
	}
	if runningCount(snapshot) > 0 {
		return "bg-accent"
	}
	return "bg-dim"
}

func projectOverviewDiagnosticsDetail(snapshot telemetry.Snapshot) string {
	return rateLimitName(snapshot.RateLimits) + " / " + budgetStatus(snapshot.Budget)
}

func projectOverviewDiagnosticsDotClass(snapshot telemetry.Snapshot) string {
	if strings.Contains(runtimeStatusClass(snapshot), "err") {
		return "bg-err"
	}
	if strings.Contains(runtimeStatusClass(snapshot), "warn") {
		return "bg-warn"
	}
	return "bg-ok"
}

func projectKanbanCardsByState(data DashboardData) map[string][]projectKanbanCard {
	issues := projectKanbanIssues(data)
	mergeStatuses := mergeLaneStatuses(data.Snapshot)
	configured := projectKanbanConfiguredStateMap(data.Kanban.States)
	cardsByState := map[string][]projectKanbanCard{}
	for _, entry := range issues {
		state := projectKanbanDisplayState(entry.state, configured)
		card := projectKanbanCardForIssue(data, entry.issue, state, entry.stageAt, pipelineNow(data.Snapshot))
		if status, ok := mergeStatuses[mergeLaneIssueKey(entry.issue)]; ok {
			card.MergeLaneStatus = status.Label
			card.MergeLaneDetail = status.Detail
			card.MergeLaneClass = status.Class
			card.MergeLaneKind = status.Kind
		}
		cardsByState[projectKanbanStateKey(state)] = append(cardsByState[projectKanbanStateKey(state)], card)
	}
	for key := range cardsByState {
		cards := cardsByState[key]
		sort.SliceStable(cards, func(i, j int) bool {
			if leftRank, rightRank := projectKanbanCardPriorityRank(cards[i]), projectKanbanCardPriorityRank(cards[j]); leftRank != rightRank {
				return leftRank < rightRank
			}
			if leftMatched, rightMatched := cards[i].DispatchPriorityRank > 0, cards[j].DispatchPriorityRank > 0; leftMatched != rightMatched {
				return leftMatched
			}
			if cards[i].DispatchPriorityRank != cards[j].DispatchPriorityRank {
				return cards[i].DispatchPriorityRank < cards[j].DispatchPriorityRank
			}
			if cards[i].DispatchPriorityRank == 0 && cards[i].UnblockerCount != cards[j].UnblockerCount {
				return cards[i].UnblockerCount > cards[j].UnblockerCount
			}
			left := cards[i].StageAt
			right := cards[j].StageAt
			if left.IsZero() || right.IsZero() {
				return !left.IsZero() && right.IsZero()
			}
			if !left.Equal(right) {
				return left.Before(right)
			}
			return cards[i].Identifier < cards[j].Identifier
		})
		cardsByState[key] = cards
	}
	return cardsByState
}

func projectKanbanCardPriorityRank(card projectKanbanCard) int {
	if card.PriorityRank < 1 || card.PriorityRank >= dispatchpriority.UnmappedPriorityRank {
		return dispatchpriority.UnmappedPriorityRank
	}
	return card.PriorityRank
}

func projectKanbanPriorityRank(priority *int) int {
	rank := dispatchpriority.Priority(priority)
	if rank == dispatchpriority.UnmappedPriorityRank {
		return 0
	}
	return rank
}

func projectKanbanDispatchPriority(data DashboardData, projectID string, labels []string) (string, int) {
	ranker := dispatchpriority.New(nil, projectKanbanDispatchPriorityLabels(data, projectID))
	match, ok := ranker.MatchLabel(labels)
	if !ok {
		return "", 0
	}
	return match.Label, match.Rank + 1
}

func projectKanbanDispatchPriorityLabels(data DashboardData, projectID string) []string {
	if isProjectDashboard(data) {
		return data.Kanban.DispatchPriorityByLabel
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = strings.TrimSpace(data.Snapshot.Project.ID)
	}
	if projectID != "" {
		if projectData, ok := data.Kanban.Projects[projectID]; ok {
			return projectData.DispatchPriorityByLabel
		}
		for configuredProjectID, projectData := range data.Kanban.Projects {
			if strings.EqualFold(strings.TrimSpace(configuredProjectID), projectID) {
				return projectData.DispatchPriorityByLabel
			}
		}
		return data.Kanban.DispatchPriorityByLabel
	}
	if len(data.Kanban.Projects) == 1 {
		for _, projectData := range data.Kanban.Projects {
			return projectData.DispatchPriorityByLabel
		}
	}
	return data.Kanban.DispatchPriorityByLabel
}

func projectKanbanIssues(data DashboardData) []projectKanbanIssueCard {
	snapshot := data.Snapshot
	byIssue := map[string]projectKanbanIssueCard{}
	configured := projectKanbanConfiguredStateMap(data.Kanban.States)
	nextIndex := 0
	appendIssue := func(issue telemetry.Issue, state string, stageAt time.Time, rank int, rawRuntimeState bool) {
		state = strings.TrimSpace(state)
		if state == "" {
			return
		}
		key := projectKanbanIssueKey(issue)
		if key == "" {
			key = "anonymous:" + strconv.Itoa(nextIndex)
		}
		current, ok := byIssue[key]
		if ok && rawRuntimeState {
			return
		}
		if ok && rank < current.rank {
			return
		}
		byIssue[key] = projectKanbanIssueCard{
			issue:   issue,
			state:   state,
			stageAt: stageAt.UTC(),
			rank:    rank,
			index:   nextIndex,
		}
		nextIndex++
	}
	appendSnapshotIssue := func(issue telemetry.Issue, fallback string, stageAt time.Time, rank int) {
		state, rawRuntimeState := projectKanbanSnapshotState(issue, fallback, configured)
		appendIssue(issue, state, stageAt, rank, rawRuntimeState)
	}
	for _, completion := range projectKanbanRecentCompletions(data) {
		appendSnapshotIssue(completion.issue, completion.state, completion.completedAt, 1)
	}

	for _, issue := range snapshot.BoardIssues {
		appendSnapshotIssue(issue, "", projectKanbanIssueStageTime(issue, time.Time{}), 5)
	}
	for _, issue := range snapshot.Pipeline {
		appendSnapshotIssue(issue, "", pipelineIssueStageTime(issue), 10)
	}
	for _, row := range snapshot.Queue {
		appendSnapshotIssue(row.Issue, "Todo", projectKanbanIssueStageTime(row.Issue, time.Time{}), 20)
	}
	for _, row := range snapshot.Running {
		appendSnapshotIssue(row.Issue, "In Progress", projectKanbanIssueStageTime(row.Issue, row.StartedAt), 30)
	}
	for _, row := range snapshot.Blocked {
		stageAt := projectKanbanIssueStageTime(row.Issue, time.Time{})
		if row.BlockedAt != nil {
			stageAt = *row.BlockedAt
		}
		issue := row.Issue
		issue.Metadata = maps.Clone(issue.Metadata)
		if issue.Metadata == nil {
			issue.Metadata = map[string]string{}
		}
		issue.Metadata[projectKanbanBlockedSourceMetadataKey] = string(row.Source)
		issue.Metadata[projectKanbanBlockedReasonMetadataKey] = row.Error
		issue.Metadata[projectKanbanBlockedRecoveryActionMetadataKey] = row.RecoveryAction
		issue.Metadata[projectKanbanBlockedRecoveryReasonMetadataKey] = row.RecoveryReason
		issue.Metadata[projectKanbanBlockedRecoveryRemedyMetadataKey] = row.RecoveryRemedy
		fallback := "Todo"
		if !telemetry.BlockedRowDependencyWaiting(row) {
			issue.State = "Blocked"
			fallback = "Blocked"
		}
		appendSnapshotIssue(issue, fallback, stageAt, 40)
	}

	issues := make([]projectKanbanIssueCard, 0, len(byIssue))
	for _, issue := range byIssue {
		issues = append(issues, issue)
	}
	sort.SliceStable(issues, func(i, j int) bool {
		return issues[i].index < issues[j].index
	})
	return issues
}

type projectKanbanRecentCompletion struct {
	issue       telemetry.Issue
	state       string
	completedAt time.Time
}

func projectKanbanRecentCompletions(data DashboardData) []projectKanbanRecentCompletion {
	now := pipelineNow(data.Snapshot)
	cutoff := now.Add(-recentCompletionWindow)
	rows := make([]projectKanbanRecentCompletion, 0, len(data.Snapshot.Completed)+len(data.Snapshot.WorkAttempts))
	seen := map[string]struct{}{}
	appendCompletion := func(issue telemetry.Issue, state string, completedAt time.Time) {
		if completedAt.IsZero() || completedAt.Before(cutoff) || completedAt.After(now) {
			return
		}
		projectID := strings.TrimSpace(issue.ProjectID)
		if selected := strings.TrimSpace(data.ProjectID); selected != "" && projectID != "" && !strings.EqualFold(projectID, selected) {
			return
		}
		if !projectKanbanTerminalState(state, projectKanbanTerminalStateSetForProject(data, projectID)) {
			return
		}
		key := recentCompletionKey(issue)
		if _, ok := seen[key]; key != "" && ok {
			return
		}
		if key != "" {
			seen[key] = struct{}{}
		}
		issue.State = state
		issue.Metadata = maps.Clone(issue.Metadata)
		if issue.Metadata == nil {
			issue.Metadata = map[string]string{}
		}
		issue.Metadata[projectKanbanRecentCompletionMetadataKey] = "true"
		rows = append(rows, projectKanbanRecentCompletion{issue: issue, state: state, completedAt: completedAt.UTC()})
	}

	for _, completed := range data.Snapshot.Completed {
		state := strings.TrimSpace(completed.State)
		appendCompletion(completed.Issue, state, completed.CompletedAt)
	}
	for _, attempt := range data.Snapshot.WorkAttempts {
		if !recentTerminalWorkAttempt(attempt) || attempt.CompletedAt == nil {
			continue
		}
		state := projectKanbanDoneStateForProject(data, attempt.ProjectID)
		issue := telemetry.Issue{
			ID:         strings.TrimSpace(attempt.IssueID),
			Identifier: strings.TrimSpace(attempt.Identifier),
			ProjectID:  strings.TrimSpace(attempt.ProjectID),
			URL:        strings.TrimSpace(attempt.IssueURL),
			Title:      recentWorkAttemptIssueTitle(attempt),
		}
		if attempt.PRNumber != nil {
			issue.PullRequest = &telemetry.PullRequest{Number: int(*attempt.PRNumber)}
		}
		appendCompletion(issue, state, *attempt.CompletedAt)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].completedAt.After(rows[j].completedAt)
	})
	return rows
}

func recentCompletionKey(issue telemetry.Issue) string {
	if id := strings.TrimSpace(issue.ID); id != "" {
		return "id:" + id
	}
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" {
		return "identifier:" + identifier
	}
	return ""
}

func recentTerminalWorkAttempt(attempt telemetry.WorkAttempt) bool {
	return strings.EqualFold(strings.TrimSpace(attempt.Status), "terminal") &&
		strings.EqualFold(strings.TrimSpace(attempt.TerminalState), "success") &&
		strings.EqualFold(strings.TrimSpace(attempt.Phase), "completed") &&
		strings.EqualFold(strings.TrimSpace(attempt.StatusMessage), "worker reached terminal state")
}

func recentWorkAttemptIssueTitle(attempt telemetry.WorkAttempt) string {
	var metadata struct {
		IssueTitle string `json:"issue_title"`
	}
	if json.Unmarshal([]byte(attempt.WorkerMetadataJSON), &metadata) == nil && strings.TrimSpace(metadata.IssueTitle) != "" {
		return strings.TrimSpace(metadata.IssueTitle)
	}
	if identifier := strings.TrimSpace(attempt.Identifier); identifier != "" {
		return "Completed " + identifier
	}
	return "Completed work"
}

func projectKanbanDoneStateForProject(data DashboardData, projectID string) string {
	states := data.Kanban.TerminalStates
	for configuredProjectID, projectStates := range data.Kanban.TerminalStatesByProject {
		if strings.EqualFold(strings.TrimSpace(configuredProjectID), strings.TrimSpace(projectID)) && len(projectStates) > 0 {
			states = projectStates
			break
		}
	}
	for _, state := range states {
		if strings.EqualFold(strings.TrimSpace(state), "Done") {
			return strings.TrimSpace(state)
		}
	}
	for _, state := range states {
		if strings.TrimSpace(state) != "" {
			return strings.TrimSpace(state)
		}
	}
	return "Done"
}

func projectKanbanSnapshotState(issue telemetry.Issue, fallback string, configured map[string]string) (string, bool) {
	state := strings.TrimSpace(issueState(issue, fallback))
	if !projectKanbanRawGitHubIssueState(state) {
		return state, false
	}
	key := projectKanbanStateKey(state)
	if configuredDisplay, ok := configured[key]; ok {
		return configuredDisplay, false
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback, true
	}
	for _, alias := range projectKanbanStateAliases(key) {
		if configuredDisplay, ok := configured[alias]; ok {
			return configuredDisplay, false
		}
	}
	return "", true
}

func projectKanbanIssueStageTime(issue telemetry.Issue, fallback time.Time) time.Time {
	if stageAt := pipelineIssueStageTime(issue); !stageAt.IsZero() {
		return stageAt
	}
	return fallback.UTC()
}

func projectKanbanIssueKey(issue telemetry.Issue) string {
	scope := strings.TrimSpace(issue.ProjectID)
	prefix := ""
	if scope != "" {
		prefix = "project:" + scope + ":"
	}
	if id := strings.TrimSpace(issue.ID); id != "" {
		return prefix + "id:" + id
	}
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" {
		return prefix + "identifier:" + identifier
	}
	return ""
}

func kanbanIntegrationEnabled(data DashboardData) bool {
	return strings.EqualFold(strings.TrimSpace(data.Kanban.Mode), "integration")
}

func projectKanbanBoardLoaded(data DashboardData) bool {
	return snapshotCarriesData(data)
}

// projectKanbanDragDropEnabled reports whether board lanes should render as
// drop targets. A project board needs its own integration mode; the
// all-project board is draggable when any configured project runs in
// integration mode — per-card allowed targets still gate each move by the
// owning project's transition policy.
func projectKanbanDragDropEnabled(data DashboardData) bool {
	if isProjectDashboard(data) {
		return kanbanIntegrationEnabled(data)
	}
	for _, project := range data.Kanban.Projects {
		if strings.EqualFold(strings.TrimSpace(project.Mode), "integration") {
			return true
		}
	}
	return false
}

// snapshotCarriesData reports whether the snapshot has data worth rendering:
// a ready refresh, or a degraded one that still carries prior tracker data.
// The redesigned snapshot views use it so a transient tracker/API failure
// keeps the last-known content visible instead of flashing skeletons.
func snapshotCarriesData(data DashboardData) bool {
	return data.Snapshot.LastKnown ||
		data.Snapshot.Tracker.Available() ||
		snapshotReady(data.Snapshot) ||
		(snapshotDegraded(data.Snapshot) && snapshotHasPriorTrackerSnapshot(data.Snapshot))
}

func projectKanbanCardKanbanData(data DashboardData, card projectKanbanCard) KanbanData {
	if isProjectDashboard(data) {
		return data.Kanban
	}
	projectID := strings.TrimSpace(card.ProjectID)
	if projectID == "" {
		return KanbanData{}
	}
	projectData, ok := data.Kanban.Projects[projectID]
	if !ok {
		return KanbanData{}
	}
	if configuredProjectID := strings.TrimSpace(projectData.ProjectID); configuredProjectID != "" {
		projectID = configuredProjectID
	}
	return KanbanData{
		Mode:                        projectData.Mode,
		ProjectID:                   projectID,
		TrackerKind:                 projectData.TrackerKind,
		States:                      projectData.States,
		ActiveStates:                projectData.ActiveStates,
		TerminalStates:              projectData.TerminalStates,
		DispatchPriorityByLabel:     projectData.DispatchPriorityByLabel,
		AllowedTransitions:          projectData.AllowedTransitions,
		SupportsPullRequestComments: projectData.SupportsPullRequestComments,
		CanMoveCards:                projectData.CanMoveCards,
		CanRemoveCards:              projectData.CanRemoveCards,
	}
}

func projectKanbanCardIntegrationEnabled(data DashboardData, card projectKanbanCard) bool {
	return strings.EqualFold(strings.TrimSpace(projectKanbanCardKanbanData(data, card).Mode), "integration")
}

func kanbanProjectID(data DashboardData) string {
	if projectID := strings.TrimSpace(data.Kanban.ProjectID); projectID != "" {
		return projectID
	}
	return strings.TrimSpace(data.ProjectID)
}

func projectKanbanCardProjectID(data DashboardData, card projectKanbanCard) string {
	if isProjectDashboard(data) {
		return kanbanProjectID(data)
	}
	return strings.TrimSpace(card.ProjectID)
}

func projectKanbanBoardScope(data DashboardData) string {
	if isProjectDashboard(data) {
		return "project"
	}
	return "fleet"
}

func kanbanDialogTargetSelector() string {
	return "#" + kanbanDialogContentID
}

func projectKanbanMoveDialogPath(data DashboardData, card projectKanbanCard) string {
	values := kanbanMoveDialogValues(projectKanbanCardProjectID(data, card), card.IssueID, card.Identifier, card.URL, card.Title, card.Stage, "", card.PRNumber, projectKanbanBoardScope(data))
	return "/api/v1/kanban/move?" + values.Encode()
}

func projectKanbanCommentDialogPath(data DashboardData, card projectKanbanCard, target string) string {
	values := kanbanCommentDialogValues(projectKanbanCardProjectID(data, card), target, card.IssueID, card.PRRepository, card.Identifier, card.URL, card.PRURL, card.Title, card.PRNumber)
	return "/api/v1/kanban/comment?" + values.Encode()
}

func kanbanMoveDialogValues(projectID string, issueID string, identifier string, issueURL string, title string, currentState string, targetState string, prNumber int, board string) url.Values {
	values := url.Values{}
	addQueryValue(values, "project_id", projectID)
	addQueryValue(values, "kanban_board", board)
	addQueryValue(values, "issue_id", issueID)
	addQueryValue(values, "identifier", identifier)
	addQueryValue(values, "issue_url", issueURL)
	addQueryValue(values, "title", title)
	addQueryValue(values, "current_state", currentState)
	addQueryValue(values, "target_state", targetState)
	if prNumber > 0 {
		values.Set("pr_number", strconv.Itoa(prNumber))
	}
	return values
}

func kanbanCommentDialogValues(projectID string, target string, issueID string, prRepository string, identifier string, issueURL string, prURL string, title string, prNumber int) url.Values {
	values := url.Values{}
	addQueryValue(values, "project_id", projectID)
	addQueryValue(values, "target", target)
	addQueryValue(values, "identifier", identifier)
	addQueryValue(values, "issue_url", issueURL)
	addQueryValue(values, "pr_url", prURL)
	addQueryValue(values, "title", title)
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "issue":
		addQueryValue(values, "issue_id", issueID)
	case "pr":
		addQueryValue(values, "pr_repository", prRepository)
		if prNumber > 0 {
			values.Set("pr_number", strconv.Itoa(prNumber))
		}
	}
	return values
}

func addQueryValue(values url.Values, key string, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		values.Set(key, value)
	}
}

func kanbanMoveDialogTargetState(data KanbanMoveDialogData) string {
	if target := strings.TrimSpace(data.TargetState); target != "" {
		return target
	}
	for _, state := range data.States {
		if state = strings.TrimSpace(state); state != "" {
			return state
		}
	}
	return strings.TrimSpace(data.CurrentState)
}

func kanbanCommentTargetLabel(target string) string {
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "pr":
		return "PR"
	default:
		return "issue"
	}
}

func projectKanbanStateOrder(configuredStates []string, cardsByState map[string][]projectKanbanCard) []string {
	configured := projectKanbanConfiguredStateMap(configuredStates)
	ordered := make([]string, 0, len(configured)+len(cardsByState))
	seen := map[string]struct{}{}
	for _, state := range detentKanbanStateOrder() {
		key := projectKanbanStateKey(state)
		display, ok := configured[key]
		if !ok {
			continue
		}
		ordered = append(ordered, display)
		seen[key] = struct{}{}
	}
	for _, state := range configuredStates {
		display := projectKanbanStateTitle(state)
		key := projectKanbanStateKey(display)
		if display == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		ordered = append(ordered, display)
		seen[key] = struct{}{}
	}

	extras := make([]string, 0, len(cardsByState))
	for key, cards := range cardsByState {
		if len(cards) == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		if projectKanbanRawGitHubIssueState(cards[0].Stage) {
			continue
		}
		extras = append(extras, cards[0].Stage)
	}
	sort.SliceStable(extras, func(i, j int) bool {
		return strings.ToLower(extras[i]) < strings.ToLower(extras[j])
	})
	for _, state := range extras {
		key := projectKanbanStateKey(state)
		if _, ok := seen[key]; ok {
			continue
		}
		ordered = append(ordered, state)
		seen[key] = struct{}{}
	}
	return ordered
}

func detentKanbanStateOrder() []string {
	return []string{
		"Backlog",
		"Todo",
		"In Progress",
		"Blocked",
		"Human Review",
		"Rework",
		"Merging",
		"Done",
		"Cancelled",
		"Canceled",
		"Closed",
		"Duplicate",
	}
}

func projectKanbanTerminalStateSet(states []string) map[string]struct{} {
	if len(states) == 0 {
		states = []string{"Done", "Cancelled", "Canceled", "Closed", "Duplicate"}
	}
	out := map[string]struct{}{}
	for _, state := range states {
		for _, key := range projectKanbanTerminalStateKeys(state) {
			out[key] = struct{}{}
		}
	}
	return out
}

func projectKanbanTerminalState(state string, terminals map[string]struct{}) bool {
	for _, key := range projectKanbanTerminalStateKeys(state) {
		if _, ok := terminals[key]; ok {
			return true
		}
	}
	return false
}

func projectKanbanTerminalStateKeys(state string) []string {
	display := projectKanbanStateTitle(state)
	if display == "" {
		return nil
	}
	key := projectKanbanStateKey(display)
	keys := []string{key}
	keys = append(keys, projectKanbanStateAliases(key)...)
	switch key {
	case "cancelled":
		keys = append(keys, "canceled")
	case "done":
		keys = append(keys, "complete", "completed", "closed")
	}
	return keys
}

func projectKanbanConfiguredStateMap(states []string) map[string]string {
	out := map[string]string{}
	for _, state := range states {
		display := projectKanbanStateTitle(state)
		if display == "" {
			continue
		}
		key := projectKanbanStateKey(display)
		if _, ok := out[key]; ok {
			continue
		}
		out[key] = display
	}
	return out
}

func projectKanbanDisplayState(state string, configured map[string]string) string {
	display := projectKanbanStateTitle(state)
	if display == "" {
		return ""
	}
	key := projectKanbanStateKey(display)
	if configuredDisplay, ok := configured[key]; ok {
		return configuredDisplay
	}
	for _, alias := range projectKanbanStateAliases(key) {
		if configuredDisplay, ok := configured[alias]; ok {
			return configuredDisplay
		}
	}
	switch key {
	case "running":
		return "In Progress"
	case "review", "inreview":
		return "Human Review"
	case "complete", "completed", "closed":
		return "Done"
	case "canceled":
		return "Cancelled"
	default:
		return display
	}
}

func projectKanbanRawGitHubIssueState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "open", "closed":
		return true
	default:
		return false
	}
}

func projectKanbanStateAliases(key string) []string {
	switch key {
	case "running":
		return []string{"inprogress"}
	case "review", "inreview":
		return []string{"humanreview"}
	case "complete", "completed", "closed":
		return []string{"done"}
	case "canceled":
		return []string{"cancelled"}
	default:
		return nil
	}
}

func projectKanbanStateTitle(state string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(state)), " ")
}

func projectKanbanStateKey(state string) string {
	state = strings.ToLower(strings.TrimSpace(state))
	replacer := strings.NewReplacer(" ", "", "-", "", "_", "")
	return replacer.Replace(state)
}

func projectKanbanLaneID(state string) string {
	key := projectKanbanStateKey(state)
	if key == "" {
		return "unknown"
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(state)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if builder.Len() > 0 && !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	id := strings.Trim(builder.String(), "-")
	if id == "" {
		return key
	}
	return id
}

func projectKanbanCardForIssue(data DashboardData, issue telemetry.Issue, state string, stageAt time.Time, now time.Time) projectKanbanCard {
	blockers, clearedBlockers := projectKanbanBlockerLabels(issue.BlockedBy, projectKanbanTerminalStateSetForIssue(data, issue), state)
	card := projectKanbanCard{
		IssueNumber:           projectKanbanIssueNumber(issue),
		Identity:              boardCardIdentityToken(issue.Identifier, issue.ID, projectKanbanIssueNumber(issue)),
		IssueID:               strings.TrimSpace(issue.ID),
		Identifier:            issueIdentifier(issue),
		ProjectID:             strings.TrimSpace(issue.ProjectID),
		ProjectColor:          projectColorForID(issue.ProjectID, data.Projects),
		Title:                 issueTitle(issue),
		Description:           issueDescriptionPreview(issue),
		URL:                   strings.TrimSpace(issue.URL),
		PullRequestLabel:      projectKanbanPullRequestLabel(issue),
		TimeInStage:           prPipelineAge(stageAt, now),
		TimeInStageTitle:      prPipelineAgeTitle(state, stageAt, now),
		WaitDetail:            prPipelineWaitDetail(issue),
		GatePending:           issue.GatePending,
		BlockedSource:         telemetry.BlockedSource(strings.TrimSpace(issue.Metadata[projectKanbanBlockedSourceMetadataKey])),
		BlockedReason:         strings.TrimSpace(issue.Metadata[projectKanbanBlockedReasonMetadataKey]),
		BlockedRecoveryAction: strings.TrimSpace(issue.Metadata[projectKanbanBlockedRecoveryActionMetadataKey]),
		BlockedRecoveryReason: strings.TrimSpace(issue.Metadata[projectKanbanBlockedRecoveryReasonMetadataKey]),
		BlockedRecoveryRemedy: strings.TrimSpace(issue.Metadata[projectKanbanBlockedRecoveryRemedyMetadataKey]),
		AttentionLabel:        projectKanbanAttentionLabel(issue),
		AttentionDetail:       projectKanbanAttentionDetail(issue),
		Stage:                 chartText(state, "n/a"),
		StageAt:               stageAt.UTC(),
		AuthorID:              strings.TrimSpace(issue.AuthorID),
		Origin:                strings.TrimSpace(issue.Origin),
		OriginActor:           strings.TrimSpace(issue.OriginActor),
		Owner:                 strings.TrimSpace(issue.Owner),
		LeaseRenewedAt:        issue.LeaseRenewedAt,
		LeaseExpiresAt:        issue.LeaseExpiresAt,
		UpdatedAt:             issue.UpdatedAt,
		SyncStatus:            strings.ToLower(strings.TrimSpace(issue.Metadata[hubSyncStatusMetadataKey])),
		SourceSyncedAt:        strings.TrimSpace(issue.Metadata[hubSourceSyncedAtMetadataKey]),
		PriorityRank:          projectKanbanPriorityRank(issue.Priority),
		PriorityName:          strings.TrimSpace(issue.PriorityName),
		UnblockerCount:        issue.UnblockerCount,
		Labels:                uniqueStrings(issue.Labels),
		Assignees:             uniqueStrings(issue.Assignees),
		Comments:              append([]telemetry.IssueComment(nil), issue.Comments...),
		HumanDependencyWait:   projectKanbanHumanDependencyWait(issue.BlockedBy),
		Blockers:              blockers,
		ClearedBlockers:       clearedBlockers,
		BlockerSummary:        strings.Join(append(append([]string(nil), blockers...), clearedBlockers...), " · "),
		HasPullRequest:        issue.PullRequest != nil,
		Movable:               strings.TrimSpace(issue.ID) != "" && issue.Metadata[projectKanbanRecentCompletionMetadataKey] != "true",
		RecentCompletion:      issue.Metadata[projectKanbanRecentCompletionMetadataKey] == "true",
		RuntimeIdentity:       issue.RuntimeIdentity,
		ParkSummary:           issue.ParkSummary,
		CompletionProgress:    issue.CompletionProgress,
	}
	if projectKanbanCardUsesInternalIssueView(data, card) {
		reference := card.Identifier
		if strings.EqualFold(projectKanbanCardKanbanData(data, card).TrackerKind, "hub_native") {
			reference = card.IssueID
		}
		card.URL = workitem.WorkItemURL(data.DashboardURL, projectKanbanCardProjectID(data, card), reference)
	}
	if issue.PullRequest != nil {
		ciStatus := prPipelineCIStatus(issue, projectKanbanLaneID(state))
		codexReview := prPipelineCodexReviewState(issue)
		card.CIStatus = ciStatus
		card.CIClass = prPipelineCIClass(ciStatus)
		card.CodexReviewState = codexReview
		card.CodexReviewClass = prPipelineCodexReviewClass(codexReview)
		card.PRNumber = issue.PullRequest.Number
		card.PRURL = strings.TrimSpace(issue.PullRequest.URL)
		card.PRRepository = pullRequestRepository(issue)
		card.MergeableState = strings.ToLower(strings.TrimSpace(issue.PullRequest.MergeableState))
		card.ConflictReason = projectKanbanPullRequestConflictReason(issue)
	}
	if !card.Movable && card.PRNumber > 0 {
		card.DisabledText = "Cannot move PR-only card"
	}
	card.DispatchPriorityLabel, card.DispatchPriorityRank = projectKanbanDispatchPriority(data, card.ProjectID, card.Labels)
	return card
}

func WithKanbanCardComments(card projectKanbanCard, comments []telemetry.IssueComment) projectKanbanCard {
	card.Comments = append([]telemetry.IssueComment(nil), comments...)
	return card
}

func projectKanbanIssueNumber(issue telemetry.Issue) string {
	if reference := issueDisplayReference(issue); reference != "" {
		return reference
	}
	return issueIdentifier(issue)
}

func projectKanbanAttentionLabel(issue telemetry.Issue) string {
	switch strings.TrimSpace(issue.Metadata[githubLocalDivergenceMetadataKey]) {
	case githubLocalClosedUpstreamDivergence:
		return "Upstream closed"
	default:
		return ""
	}
}

func projectKanbanAttentionDetail(issue telemetry.Issue) string {
	if detail := strings.TrimSpace(issue.Metadata[githubLocalDivergenceDetailMetadataKey]); detail != "" {
		return detail
	}
	switch strings.TrimSpace(issue.Metadata[githubLocalDivergenceMetadataKey]) {
	case githubLocalClosedUpstreamDivergence:
		return "closed upstream while locally active"
	default:
		return ""
	}
}

func projectKanbanCountLabel(count int, singular string, plural string) string {
	if count == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(count) + " " + plural
}

func projectKanbanPullRequestLabel(issue telemetry.Issue) string {
	if issue.PullRequest == nil {
		return "No linked PR"
	}
	if issue.PullRequest.Number > 0 {
		return "PR #" + strconv.Itoa(issue.PullRequest.Number)
	}
	return "Linked PR"
}

func projectKanbanPullRequestConflictReason(issue telemetry.Issue) string {
	if issue.PullRequest == nil {
		return ""
	}
	mergeableState := strings.ToLower(strings.TrimSpace(issue.PullRequest.MergeableState))
	switch mergeableState {
	case "dirty", "conflicting":
	default:
		return ""
	}
	return projectKanbanPullRequestLabel(issue) + " mergeStateStatus " + strings.ToUpper(mergeableState)
}

func projectKanbanBlockerLabels(refs []telemetry.BlockedRef, terminalStates map[string]struct{}, issueState string) ([]string, []string) {
	active := make([]string, 0, len(refs))
	cleared := make([]string, 0, len(refs))
	for _, ref := range refs {
		label := strings.TrimSpace(ref.Identifier)
		if label == "" {
			label = strings.TrimSpace(ref.ID)
		}
		if label == "" {
			continue
		}
		if ref.HumanOwned {
			if ref.HumanCompletionReady {
				cleared = append(cleared, "human prerequisite "+label+" (completion evidence recorded)")
			} else {
				active = append(active, "human prerequisite "+label+" (completion evidence required)")
			}
			continue
		}
		trackerState := strings.ToLower(strings.TrimSpace(ref.TrackerState))
		if trackerState != "" {
			switch trackerState {
			case "open":
				label += " (live)"
			case "closed":
				label += " (resolved)"
			default:
				label += " (" + trackerState + ")"
			}
		} else if state := strings.TrimSpace(ref.State); state != "" {
			label += " (" + state + ")"
		}
		if projectKanbanBlockedRefCleared(ref, terminalStates) {
			cleared = append(cleared, label)
			continue
		}
		if strings.TrimSpace(ref.State) == "" && !projectKanbanUnknownBlockerActive(issueState) {
			continue
		}
		active = append(active, label)
	}
	return uniqueStrings(active), uniqueStrings(cleared)
}

func projectKanbanBlockedRefCleared(ref telemetry.BlockedRef, terminalStates map[string]struct{}) bool {
	if ref.HumanOwned {
		return ref.HumanCompletionReady
	}
	if strings.EqualFold(strings.TrimSpace(ref.TrackerState), "closed") {
		return true
	}
	return projectKanbanTerminalState(ref.State, terminalStates)
}

func projectKanbanUnknownBlockerActive(issueState string) bool {
	return projectKanbanStateKey(issueState) == "blocked"
}

func projectKanbanTerminalStateSetForIssue(data DashboardData, issue telemetry.Issue) map[string]struct{} {
	return projectKanbanTerminalStateSetForProject(data, strings.TrimSpace(issue.ProjectID))
}

// projectKanbanTerminalStateSetForProject resolves terminal states for a
// specific project, falling back to the snapshot project then the global
// set. The fleet board mixes projects, so terminal treatment must be
// evaluated per card rather than from the default project's states alone.
func projectKanbanTerminalStateSetForProject(data DashboardData, projectID string) map[string]struct{} {
	states := data.Kanban.TerminalStates
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = strings.TrimSpace(data.Snapshot.Project.ID)
	}
	if projectID != "" {
		if projectStates, ok := data.Kanban.TerminalStatesByProject[projectID]; ok && len(projectStates) > 0 {
			states = projectStates
		} else {
			for configuredProjectID, projectStates := range data.Kanban.TerminalStatesByProject {
				if strings.EqualFold(strings.TrimSpace(configuredProjectID), projectID) && len(projectStates) > 0 {
					states = projectStates
					break
				}
			}
		}
	}
	return projectKanbanTerminalStateSet(states)
}

func projectKanbanCardCanMove(data DashboardData, card projectKanbanCard) bool {
	return projectKanbanCardMoveDisabledText(data, card) == ""
}

// projectKanbanCardMoveDisabledText resolves per card, so the all-project
// board is draggable too: each card's own project supplies the integration
// mode and transition policy (projectKanbanCardKanbanData), and cards from
// read-only or unresolvable projects stay inert with a reason chip.
func projectKanbanCardMoveDisabledText(data DashboardData, card projectKanbanCard) string {
	if data.Snapshot.LastKnown && !snapshotUsesStartupCache(data.Snapshot) {
		return "Tracker snapshot is not ready; moves are disabled until data is current."
	}
	if reason := projectKanbanCardRefreshDisabledText(data, card); reason != "" {
		return reason
	}
	kanban := projectKanbanCardKanbanData(data, card)
	if !strings.EqualFold(strings.TrimSpace(kanban.Mode), "integration") {
		return "This project board is read-only."
	}
	if !kanban.CanMoveCards {
		return "This project's tracker does not support moving cards."
	}
	if !card.Movable || strings.TrimSpace(card.IssueID) == "" {
		if card.PRNumber > 0 {
			return "No linked issue is available for this PR-only card."
		}
		return "No linked issue is available for this card."
	}
	if len(projectKanbanMoveTargetStates(data, card)) == 0 {
		state := strings.TrimSpace(card.Stage)
		if state == "" || state == "n/a" {
			return "No allowed transition is configured for this card."
		}
		return "No allowed transition is configured from " + state + "."
	}
	return ""
}

func projectKanbanCardRefreshDisabledText(data DashboardData, card projectKanbanCard) string {
	refresh := projectKanbanCardRefresh(data, card)
	if !snapshotHasRefreshSignal(refresh) {
		if !snapshotHasRefreshSignal(data.Snapshot.Refresh) && (!data.Snapshot.GeneratedAt.IsZero() || snapshotHasLoadedData(data.Snapshot)) {
			return ""
		}
		refresh = data.Snapshot.Refresh
	}
	if refresh.ReadinessStatus() == telemetry.RefreshStatusInitializing {
		return "Project is initializing; moves are disabled until tracker data is current."
	}

	kanban := projectKanbanCardKanbanData(data, card)
	sourceName := telemetry.RefreshSourceStatuses
	sourceLabel := "status"
	if projectKanbanStateIn(card.Stage, kanban.ActiveStates) {
		sourceName = telemetry.RefreshSourceCandidates
		sourceLabel = "candidate"
	}
	if source, ok := refresh.Source(sourceName); ok {
		if refreshSourceStale(refresh, source, data.Snapshot.GeneratedAt) {
			return "Tracker " + sourceLabel + " data for this card is stale; moves are disabled until it refreshes."
		}
		return ""
	}
	if refresh.ReadinessStatus() == telemetry.RefreshStatusDegraded {
		return "Tracker refresh is degraded; moves are disabled until a fresh snapshot is ready."
	}
	return ""
}

func projectKanbanCardRefresh(data DashboardData, card projectKanbanCard) telemetry.Refresh {
	projectID := strings.TrimSpace(projectKanbanCardProjectID(data, card))
	for _, project := range data.Snapshot.Projects {
		if strings.EqualFold(strings.TrimSpace(project.Project.ID), projectID) && snapshotHasRefreshSignal(project.Refresh) {
			return project.Refresh
		}
	}
	return data.Snapshot.Refresh
}

func projectKanbanStateIn(state string, states []string) bool {
	for _, candidate := range states {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(state)) {
			return true
		}
	}
	return false
}

func projectKanbanCardCanRemove(data DashboardData, card projectKanbanCard) bool {
	return snapshotReady(data.Snapshot) && isProjectDashboard(data) && kanbanIntegrationEnabled(data) && data.Kanban.CanRemoveCards && strings.TrimSpace(card.IssueID) != ""
}

func projectKanbanCardCanComment(data DashboardData, card projectKanbanCard) bool {
	if !projectKanbanCardCanUseComments(data, card) {
		return false
	}
	return strings.TrimSpace(card.IssueID) != "" || projectKanbanCardCanCommentOnPullRequest(data, card)
}

func projectKanbanCardCanCommentOnIssue(data DashboardData, card projectKanbanCard) bool {
	return projectKanbanCardCanUseComments(data, card) && strings.TrimSpace(card.IssueID) != ""
}

func projectKanbanCardCanCommentOnPullRequest(data DashboardData, card projectKanbanCard) bool {
	if !projectKanbanCardCanUseComments(data, card) || card.PRNumber <= 0 || strings.TrimSpace(card.PRRepository) == "" {
		return false
	}
	return projectKanbanCardKanbanData(data, card).SupportsPullRequestComments
}

func projectKanbanCardCanUseComments(data DashboardData, card projectKanbanCard) bool {
	return snapshotReady(data.Snapshot) && projectKanbanCardIntegrationEnabled(data, card)
}

func projectKanbanMoveTargetStates(data DashboardData, card projectKanbanCard) []string {
	return kanbanMoveTargets(projectKanbanCardKanbanData(data, card), card.Stage)
}

func projectKanbanMoveTargetKeys(data DashboardData, card projectKanbanCard) string {
	targets := projectKanbanMoveTargetStates(data, card)
	if len(targets) == 0 {
		return ""
	}
	keys := make([]string, 0, len(targets))
	seen := map[string]struct{}{}
	for _, target := range targets {
		key := projectKanbanStateKey(target)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return strings.Join(keys, " ")
}

func kanbanMoveTargets(data KanbanData, source string) []string {
	sourceKey := projectKanbanStateKey(source)
	if sourceKey == "" {
		return nil
	}
	if data.AllowedTransitions == nil {
		return kanbanMoveTargetsFromStates(data.States, source)
	}
	for configuredSource, targets := range data.AllowedTransitions {
		if projectKanbanStateKey(configuredSource) != sourceKey {
			continue
		}
		return kanbanMoveTargetsFromStates(targets, source)
	}
	return nil
}

func kanbanMoveTargetsFromStates(states []string, source string) []string {
	sourceKey := projectKanbanStateKey(source)
	targets := make([]string, 0, len(states))
	seen := map[string]struct{}{}
	for _, state := range states {
		state = projectKanbanStateTitle(state)
		key := projectKanbanStateKey(state)
		if key == "" || key == sourceKey {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, state)
	}
	return targets
}

func issueRepository(identifier string) string {
	repo, _, ok := strings.Cut(strings.TrimSpace(identifier), "#")
	if !ok {
		return ""
	}
	return strings.TrimSpace(repo)
}

func issueURL(issue telemetry.Issue) string {
	return strings.TrimSpace(issue.URL)
}

func issueOpenLabel(issue telemetry.Issue) string {
	return "Open issue " + issueIdentifier(issue)
}

func pullRequestNumber(issue telemetry.Issue) int {
	if issue.PullRequest == nil || issue.PullRequest.Number <= 0 {
		return 0
	}
	return issue.PullRequest.Number
}

func pullRequestURL(issue telemetry.Issue) string {
	if issue.PullRequest == nil {
		return ""
	}
	if prURL := strings.TrimSpace(issue.PullRequest.URL); prURL != "" {
		return prURL
	}
	if issue.PullRequest.Number <= 0 {
		return ""
	}
	baseURL := pullRequestRepositoryBaseURL(issue)
	if baseURL == "" {
		return ""
	}
	return baseURL + "/pull/" + strconv.Itoa(issue.PullRequest.Number)
}

func pullRequestOpenLabel(issue telemetry.Issue) string {
	return pullRequestOpenLabelForNumber(pullRequestNumber(issue))
}

func pullRequestOpenLabelForNumber(number int) string {
	if number > 0 {
		return "Open PR #" + strconv.Itoa(number)
	}
	return "Open linked PR"
}

func issueActionClass(compact bool) string {
	base := "issue-external inline-flex shrink-0 items-center justify-center rounded-md border border-line bg-surface text-sec hover:border-accent hover:text-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
	if compact {
		return base + " h-8 w-8"
	}
	return base + " h-10 w-10"
}

func issueActionIconClass(compact bool) string {
	if compact {
		return "size-3.5"
	}
	return "size-4"
}

func pullRequestRepositoryBaseURL(issue telemetry.Issue) string {
	if issue.PullRequest != nil {
		if baseURL := repositoryBaseURLFromRecordURL(issue.PullRequest.URL); baseURL != "" {
			return baseURL
		}
	}
	if baseURL := repositoryBaseURLFromRecordURL(issue.URL); baseURL != "" {
		return baseURL
	}
	if repository := pullRequestRepository(issue); repository != "" {
		return "https://github.com/" + repository
	}
	return ""
}

func repositoryBaseURLFromRecordURL(rawURL string) string {
	scheme, host, repository := recordURLRepositoryParts(rawURL)
	if repository == "" {
		return ""
	}
	return scheme + "://" + host + "/" + repository
}

func repositoryFromRecordURL(rawURL string) string {
	_, _, repository := recordURLRepositoryParts(rawURL)
	return repository
}

func recordURLRepositoryParts(rawURL string) (string, string, string) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", "", ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || (parts[2] != "issues" && parts[2] != "pull") {
		return "", "", ""
	}
	owner := strings.TrimSpace(parts[0])
	repo := strings.TrimSpace(parts[1])
	if owner == "" || repo == "" {
		return "", "", ""
	}
	return parsed.Scheme, parsed.Host, owner + "/" + repo
}

func pullRequestRepository(issue telemetry.Issue) string {
	if issue.PullRequest != nil {
		if repository := repositoryFromPullRequestURL(issue.PullRequest.URL); repository != "" {
			return repository
		}
	}
	return issueRepository(issue.Identifier)
}

func repositoryFromPullRequestURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return ""
	}
	owner := strings.TrimSpace(parts[0])
	repo := strings.TrimSpace(parts[1])
	if owner == "" || repo == "" {
		return ""
	}
	return owner + "/" + repo
}

func normalizeDashboardState(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

type mergeLaneCardStatus struct {
	Label     string
	Detail    string
	Prefix    string
	Reference string
	URL       string
	Suffix    string
	Class     string
	Kind      primitives.Kind
}

type mergeLaneIssueRecord struct {
	issue      telemetry.Issue
	key        string
	group      string
	active     bool
	stageAt    time.Time
	step       string
	phase      string
	fresh      bool
	queueError string
	rank       int
	index      int
}

func mergeLaneStatuses(snapshot telemetry.Snapshot) map[string]mergeLaneCardStatus {
	records := mergeLaneIssueRecords(snapshot)
	if len(records) == 0 {
		return nil
	}

	byGroup := map[string][]mergeLaneIssueRecord{}
	for _, record := range records {
		byGroup[record.group] = append(byGroup[record.group], record)
	}

	statuses := make(map[string]mergeLaneCardStatus, len(records))
	for _, groupRecords := range byGroup {
		sort.SliceStable(groupRecords, func(i, j int) bool {
			if groupRecords[i].active != groupRecords[j].active {
				return groupRecords[i].active
			}
			left := groupRecords[i].stageAt
			right := groupRecords[j].stageAt
			if left.IsZero() || right.IsZero() {
				return !left.IsZero() && right.IsZero()
			}
			if !left.Equal(right) {
				return left.Before(right)
			}
			if groupRecords[i].index != groupRecords[j].index {
				return groupRecords[i].index < groupRecords[j].index
			}
			return issueIdentifier(groupRecords[i].issue) < issueIdentifier(groupRecords[j].issue)
		})

		var holder mergeLaneIssueRecord
		for _, record := range groupRecords {
			if record.active {
				holder = record
				break
			}
		}
		notDraining := mergeLaneGroupNotDraining(snapshot, groupRecords)

		for i, record := range groupRecords {
			position := i + 1
			if native := nativeMergeQueueCardStatus(record); native != (mergeLaneCardStatus{}) {
				statuses[record.key] = native
				continue
			}
			if record.active {
				statuses[record.key] = mergeLaneActiveStatus(record)
				continue
			}
			statuses[record.key] = mergeLaneQueuedStatus(record, position, holder, notDraining)
		}
	}
	return statuses
}

func nativeMergeQueueCardStatus(record mergeLaneIssueRecord) mergeLaneCardStatus {
	if record.issue.PullRequest == nil || record.issue.PullRequest.MergeQueueEntry == nil {
		return mergeLaneCardStatus{}
	}
	entry := record.issue.PullRequest.MergeQueueEntry
	if strings.TrimSpace(entry.ID) == "" {
		return mergeLaneCardStatus{}
	}
	label := "Native queue"
	if entry.Position > 0 && entry.Depth > 0 {
		label = "Native #" + strconv.Itoa(entry.Position) + " of " + strconv.Itoa(entry.Depth)
	} else if entry.Position > 0 {
		label = "Native #" + strconv.Itoa(entry.Position)
	}
	if entry.EstimatedTimeToMergeSeconds > 0 {
		label += " · ~" + formatDuration(float64(entry.EstimatedTimeToMergeSeconds))
	}
	details := []string{"GitHub native merge queue"}
	if state := strings.TrimSpace(entry.State); state != "" {
		details = append(details, "state "+strings.ToLower(state))
	}
	if entry.Position > 0 && entry.Depth > 0 {
		details = append(details, "position "+strconv.Itoa(entry.Position)+" of "+strconv.Itoa(entry.Depth))
	}
	if entry.EstimatedTimeToMergeSeconds > 0 {
		details = append(details, "estimated drain "+formatDuration(float64(entry.EstimatedTimeToMergeSeconds)))
	}
	return mergeLaneCardStatus{
		Label:  label,
		Detail: strings.Join(details, "; "),
		Prefix: strings.Join(details, "; "),
		URL:    strings.TrimSpace(entry.URL),
		Class:  "border-accent/15 bg-accent/15 text-accent",
		Kind:   primitives.KindInfo,
	}
}

func mergeLaneIssueRecords(snapshot telemetry.Snapshot) []mergeLaneIssueRecord {
	recordsByKey := map[string]mergeLaneIssueRecord{}
	nextIndex := 0
	upsert := func(record mergeLaneIssueRecord) {
		if !mergeLaneIssue(record.issue) {
			return
		}
		record.key = mergeLaneIssueKey(record.issue)
		record.group = mergeLaneGroupKey(record.issue)
		if record.key == "" || record.group == "" {
			return
		}
		record.index = nextIndex
		nextIndex++
		current, ok := recordsByKey[record.key]
		if ok && current.rank > record.rank {
			return
		}
		if ok && current.rank == record.rank && current.active && !record.active {
			return
		}
		recordsByKey[record.key] = record
	}

	for _, issue := range snapshot.BoardIssues {
		upsert(mergeLaneIssueRecord{issue: issue, stageAt: projectKanbanIssueStageTime(issue, time.Time{}), rank: 5})
	}
	for _, issue := range snapshot.Pipeline {
		upsert(mergeLaneIssueRecord{issue: issue, stageAt: pipelineIssueStageTime(issue), rank: 10})
	}
	for _, row := range snapshot.Queue {
		stageAt := projectKanbanIssueStageTime(row.Issue, time.Time{})
		if stageAt.IsZero() && row.DueAt != nil {
			stageAt = row.DueAt.UTC()
		}
		upsert(mergeLaneIssueRecord{
			issue:      row.Issue,
			stageAt:    stageAt,
			queueError: strings.TrimSpace(row.Error),
			rank:       20,
		})
	}
	for _, row := range snapshot.Running {
		attempt, hasAttempt := mergeLaneRunningAttempt(snapshot, row)
		phase := ""
		fresh := false
		if hasAttempt {
			phase = strings.TrimSpace(attempt.Phase)
			fresh = mergeLaneAttemptFresh(snapshot.GeneratedAt, attempt)
		}
		upsert(mergeLaneIssueRecord{
			issue:   row.Issue,
			active:  true,
			stageAt: row.StartedAt.UTC(),
			step:    mergeLaneActiveStep(row),
			phase:   phase,
			fresh:   fresh,
			rank:    30,
		})
	}

	records := make([]mergeLaneIssueRecord, 0, len(recordsByKey))
	for _, record := range recordsByKey {
		records = append(records, record)
	}
	return records
}

func mergeLaneRunningAttempt(snapshot telemetry.Snapshot, row telemetry.Running) (telemetry.WorkAttempt, bool) {
	for _, attempt := range snapshot.WorkAttempts {
		if row.WorkAttemptID > 0 && attempt.AttemptID == row.WorkAttemptID {
			return attempt, true
		}
	}
	var fallback telemetry.WorkAttempt
	found := false
	for _, attempt := range snapshot.WorkAttempts {
		if !mergeLaneAttemptMatchesIssue(attempt, row.Issue) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(attempt.Status), "active") {
			return attempt, true
		}
		if !found {
			fallback = attempt
			found = true
		}
	}
	return fallback, found
}

func mergeLaneAttemptMatchesIssue(attempt telemetry.WorkAttempt, issue telemetry.Issue) bool {
	if projectID := strings.TrimSpace(issue.ProjectID); projectID != "" && !strings.EqualFold(projectID, strings.TrimSpace(attempt.ProjectID)) {
		return false
	}
	if issueID := strings.TrimSpace(issue.ID); issueID != "" && issueID == strings.TrimSpace(attempt.IssueID) {
		return true
	}
	identifier := strings.TrimSpace(issue.Identifier)
	return identifier != "" && strings.EqualFold(identifier, strings.TrimSpace(attempt.Identifier))
}

func mergeLaneAttemptFresh(generatedAt time.Time, attempt telemetry.WorkAttempt) bool {
	if !strings.EqualFold(strings.TrimSpace(attempt.Status), "active") || attempt.HeartbeatAt == nil || attempt.Stale {
		return false
	}
	return generatedAt.IsZero() || attempt.LeaseExpiresAt == nil || !attempt.LeaseExpiresAt.Before(generatedAt)
}

func mergeLaneGroupNotDraining(snapshot telemetry.Snapshot, records []mergeLaneIssueRecord) bool {
	for _, warning := range snapshot.StalenessWarnings {
		if warning.Kind != staleness.KindMergeLiveness {
			continue
		}
		for _, record := range records {
			if strings.EqualFold(strings.TrimSpace(warning.ProjectID), strings.TrimSpace(record.issue.ProjectID)) {
				return true
			}
		}
	}
	return false
}

func mergeLaneIssue(issue telemetry.Issue) bool {
	return prPipelineLaneID(issueState(issue, "")) == "merging"
}

func mergeLaneIssueKey(issue telemetry.Issue) string {
	scope := strings.TrimSpace(issue.ProjectID)
	prefix := ""
	if scope != "" {
		prefix = "project:" + scope + ":"
	}
	if id := strings.TrimSpace(issue.ID); id != "" {
		return prefix + "id:" + id
	}
	if identifier := strings.TrimSpace(issue.Identifier); identifier != "" {
		return prefix + "identifier:" + identifier
	}
	if issue.PullRequest != nil && issue.PullRequest.Number > 0 {
		repository := strings.ToLower(strings.TrimSpace(pullRequestRepository(issue)))
		if repository != "" {
			return "pr:" + repository + "#" + strconv.Itoa(issue.PullRequest.Number)
		}
	}
	return ""
}

func mergeLaneGroupKey(issue telemetry.Issue) string {
	if repository := strings.ToLower(strings.TrimSpace(pullRequestRepository(issue))); repository != "" {
		return "repo:" + repository
	}
	if projectID := strings.ToLower(strings.TrimSpace(issue.ProjectID)); projectID != "" {
		return "project:" + projectID
	}
	return mergeLaneIssueKey(issue)
}

func mergeLaneActiveStep(row telemetry.Running) string {
	if event := strings.TrimSpace(row.LastEvent); event != "" {
		return event
	}
	if message := strings.TrimSpace(row.LastMessage); message != "" {
		return message
	}
	for i := len(row.RecentEvents) - 1; i >= 0; i-- {
		event := row.RecentEvents[i]
		if message := strings.TrimSpace(event.Message); message != "" {
			return message
		}
		if name := strings.TrimSpace(event.Event); name != "" {
			return name
		}
	}
	return ""
}

func mergeLaneActiveStatus(record mergeLaneIssueRecord) mergeLaneCardStatus {
	detail := "Active merge worker"
	status := mergeLaneCardStatus{
		Label:  "Merging now",
		Detail: detail,
		Class:  "border-accent/15 bg-accent/15 text-accent",
		Kind:   primitives.KindInfo,
	}
	if number := pullRequestNumber(record.issue); number > 0 {
		status.Prefix = detail + " for "
		status.Reference = "PR #" + strconv.Itoa(number)
		status.URL = pullRequestURL(record.issue)
		detail += " for " + status.Reference
	}
	if step := strings.TrimSpace(record.step); step != "" {
		status.Suffix = "; " + step
		detail += status.Suffix
	}
	status.Detail = detail
	if status.Reference == "" {
		status.Prefix = detail
		status.Suffix = ""
	}
	return status
}

func mergeLaneQueuedStatus(record mergeLaneIssueRecord, position int, holder mergeLaneIssueRecord, notDraining bool) mergeLaneCardStatus {
	details := []string{}
	if queueError := strings.TrimSpace(record.queueError); queueError != "" {
		details = append(details, "Waiting: "+queueError)
	}
	details = append(details, mergeLaneOrdinal(position)+" in merge queue")
	waiting := "waiting for repo merge lane"
	status := mergeLaneCardStatus{
		Label: "Queued #" + strconv.Itoa(position),
		Class: "border-warn/15 bg-warn/15 text-warn",
		Kind:  primitives.KindWarn,
	}
	if notDraining {
		status.Label = "Not draining #" + strconv.Itoa(position)
		status.Class = "border-err/15 bg-err/15 text-err"
		status.Kind = primitives.KindErr
		waiting = "merge queue is not advancing"
	} else if mergeLaneCapacityQueued(record) && holder.fresh {
		status.Label = "Draining #" + strconv.Itoa(position)
		status.Class = "border-ok/15 bg-ok/15 text-ok"
		status.Kind = primitives.KindOK
		waiting = "lane draining"
	}
	if holder.active {
		status.Reference, status.URL, status.Suffix = mergeLaneHolderAttribution(holder)
		if status.Reference != "" {
			status.Prefix = strings.Join(append(details, waiting+" behind "), "; ")
			waiting += " behind " + status.Reference + status.Suffix
		}
	}
	details = append(details, waiting)
	status.Detail = strings.Join(details, "; ")
	if status.Reference == "" {
		status.Prefix = status.Detail
	}
	return status
}

func mergeLaneCapacityQueued(record mergeLaneIssueRecord) bool {
	switch strings.TrimSpace(record.queueError) {
	case "lane_capacity_full", "project_state_capacity_full", "local_slot_unavailable":
		return true
	default:
		return false
	}
}

func mergeLaneHolderAttribution(holder mergeLaneIssueRecord) (string, string, string) {
	reference := strings.TrimSpace(issueIdentifier(holder.issue))
	url := strings.TrimSpace(holder.issue.URL)
	suffix := ""
	if number := pullRequestNumber(holder.issue); number > 0 {
		prReference := "PR #" + strconv.Itoa(number)
		if reference == "" {
			reference = prReference
			url = pullRequestURL(holder.issue)
		} else {
			suffix = " / " + prReference
		}
	}
	phase := strings.TrimSpace(holder.phase)
	if phase == "" {
		phase = strings.TrimSpace(holder.step)
	}
	if phase != "" {
		suffix += "; phase " + phase
	}
	return reference, url, suffix
}

func mergeLaneOrdinal(position int) string {
	if position <= 0 {
		return "0th"
	}
	lastTwo := position % 100
	if lastTwo >= 11 && lastTwo <= 13 {
		return strconv.Itoa(position) + "th"
	}
	switch position % 10 {
	case 1:
		return strconv.Itoa(position) + "st"
	case 2:
		return strconv.Itoa(position) + "nd"
	case 3:
		return strconv.Itoa(position) + "rd"
	default:
		return strconv.Itoa(position) + "th"
	}
}

func prPipelineLanes(snapshot telemetry.Snapshot) []prPipelineLane {
	cardsByLane := map[string][]prPipelineCard{
		"human-review": {},
		"merging":      {},
		"done-today":   {},
	}
	mergeStatuses := mergeLaneStatuses(snapshot)
	seen := map[string]struct{}{}
	now := pipelineNow(snapshot)

	for _, issue := range snapshot.Pipeline {
		appendPRPipelineCard(cardsByLane, seen, mergeStatuses, issue, issue.State, pipelineIssueStageTime(issue), now)
	}
	for _, row := range snapshot.Running {
		appendPRPipelineCard(cardsByLane, seen, mergeStatuses, row.Issue, issueState(row.Issue, "Running"), row.StartedAt, now)
	}
	for _, row := range snapshot.Queue {
		stageAt := time.Time{}
		if row.DueAt != nil {
			stageAt = *row.DueAt
		}
		appendPRPipelineCard(cardsByLane, seen, mergeStatuses, row.Issue, issueState(row.Issue, "Todo"), stageAt, now)
	}
	for _, row := range snapshot.Blocked {
		stageAt := time.Time{}
		if row.BlockedAt != nil {
			stageAt = *row.BlockedAt
		}
		appendPRPipelineCard(cardsByLane, seen, mergeStatuses, row.Issue, issueState(row.Issue, "Blocked"), stageAt, now)
	}

	prunePRPipelineCards(cardsByLane)

	return []prPipelineLane{
		{
			ID:          "human-review",
			Title:       "Human Review",
			CountLabel:  formatCount(len(cardsByLane["human-review"])),
			DotClass:    "bg-ok",
			EmptyTitle:  "No PRs waiting for review.",
			EmptyDetail: "Ready pull requests will appear here after Detent hands them to reviewers.",
			Cards:       cardsByLane["human-review"],
		},
		{
			ID:          "merging",
			Title:       "Merging",
			CountLabel:  formatCount(len(cardsByLane["merging"])),
			DotClass:    "bg-accent",
			EmptyTitle:  "Nothing is merging.",
			EmptyDetail: "Approved pull requests enter this lane while the final integration run is active.",
			Cards:       cardsByLane["merging"],
		},
		{
			ID:          "done-today",
			Title:       "Done today",
			CountLabel:  formatCount(len(cardsByLane["done-today"])),
			DotClass:    "bg-dim",
			EmptyTitle:  "No PRs finished today.",
			EmptyDetail: "Merged pull requests land here for the current day.",
			Cards:       cardsByLane["done-today"],
		},
	}
}

func prPipelineTotalLabel(snapshot telemetry.Snapshot) string {
	total := 0
	for _, lane := range prPipelineLanes(snapshot) {
		total += len(lane.Cards)
	}
	return formatCount(total)
}

func prPipelineMergeSummary(snapshot telemetry.Snapshot) prPipelineMergeMetrics {
	now := pipelineNow(snapshot)
	depth := len(mergeLaneIssueRecords(snapshot))
	nativeQueueDepths := map[string]int{}
	nativeUnknownDepth := 0
	activeSeconds := int64(0)
	queueSeconds := int64(0)
	nativeDrainSeconds := int64(0)
	collectLive := func(issue telemetry.Issue) {
		if normalizeDashboardState(issue.State) != "merging" {
			return
		}
		if issue.PullRequest != nil && issue.PullRequest.MergeQueueEntry != nil {
			entry := issue.PullRequest.MergeQueueEntry
			nativeDrainSeconds = max(nativeDrainSeconds, entry.EstimatedTimeToMergeSeconds)
			if entry.Depth > 0 {
				if queueURL := strings.TrimSpace(entry.URL); queueURL != "" {
					nativeQueueDepths[queueURL] = max(nativeQueueDepths[queueURL], entry.Depth)
				} else {
					nativeUnknownDepth = max(nativeUnknownDepth, entry.Depth)
				}
			}
		}
		if issue.MergeTiming == nil {
			return
		}
		timing := issue.MergeTiming
		if timing.MergeStartedAt != nil && timing.MergedAt == nil && timing.MergeFailedAt == nil {
			activeSeconds = max(activeSeconds, liveActiveMergeSeconds(timing, now))
			return
		}
		queueSeconds = max(queueSeconds, liveMergeQueueSeconds(timing, now))
	}
	for _, issue := range snapshot.Pipeline {
		collectLive(issue)
	}
	for _, row := range snapshot.Running {
		collectLive(row.Issue)
	}
	for _, row := range snapshot.Queue {
		collectLive(row.Issue)
	}

	activeDurations := []int64{}
	totalDurations := []int64{}
	cutoff := now.Add(-prPipelineMergeSummaryWindow)
	for _, row := range snapshot.Completed {
		if row.MergeTiming == nil || row.CompletedAt.IsZero() || (!now.IsZero() && row.CompletedAt.Before(cutoff)) {
			continue
		}
		if row.MergeTiming.ActiveMergeDurationSeconds > 0 {
			activeDurations = append(activeDurations, row.MergeTiming.ActiveMergeDurationSeconds)
		}
		if row.MergeTiming.TotalMergingSeconds > 0 {
			totalDurations = append(totalDurations, row.MergeTiming.TotalMergingSeconds)
		}
	}
	for _, queueDepth := range nativeQueueDepths {
		nativeUnknownDepth += queueDepth
	}
	if nativeUnknownDepth > 0 {
		depth = nativeUnknownDepth
	}

	activeP50 := percentileDuration(activeDurations, 50)
	drainSeconds := nativeDrainSeconds
	if drainSeconds == 0 && activeP50 > 0 && depth > 0 {
		drainSeconds = activeP50 * int64(depth)
		if activeSeconds > 0 {
			drainSeconds -= min(activeSeconds, activeP50)
		}
	}
	summary := prPipelineMergeMetrics{
		Available:     depth > 0 || activeSeconds > 0 || queueSeconds > 0 || len(activeDurations) > 0 || len(totalDurations) > 0,
		Depth:         formatCount(depth),
		DrainETA:      formatOptionalDuration(drainSeconds),
		ActiveElapsed: formatOptionalDuration(activeSeconds),
		QueueWait:     formatOptionalDuration(queueSeconds),
		RecentCount:   formatCount(len(activeDurations)),
		ActiveP50:     formatOptionalDuration(activeP50),
		ActiveP90:     formatOptionalDuration(percentileDuration(activeDurations, 90)),
		TotalP50:      formatOptionalDuration(percentileDuration(totalDurations, 50)),
		TotalP90:      formatOptionalDuration(percentileDuration(totalDurations, 90)),
		ActiveWarning: time.Duration(activeSeconds)*time.Second > prPipelineActiveMergeTarget,
		QueueWarning:  time.Duration(queueSeconds)*time.Second > prPipelineQueueWaitTarget,
	}
	return summary
}

func liveActiveMergeSeconds(timing *telemetry.MergeTiming, now time.Time) int64 {
	if timing == nil {
		return 0
	}
	if timing.ActiveMergeDurationSeconds > 0 {
		return timing.ActiveMergeDurationSeconds
	}
	if timing.MergeStartedAt == nil || timing.MergeStartedAt.IsZero() || now.IsZero() || now.Before(*timing.MergeStartedAt) {
		return 0
	}
	return int64(now.Sub(*timing.MergeStartedAt) / time.Second)
}

func liveMergeQueueSeconds(timing *telemetry.MergeTiming, now time.Time) int64 {
	if timing == nil {
		return 0
	}
	if timing.QueueWaitSeconds > 0 {
		return timing.QueueWaitSeconds
	}
	if timing.EnteredMergingAt == nil || timing.EnteredMergingAt.IsZero() {
		return 0
	}
	queueEnd := now
	for _, candidate := range []*time.Time{timing.MergeWorkerSlotAcquiredAt, timing.MergeStartedAt, timing.MergedAt, timing.MergeFailedAt} {
		if candidate != nil && !candidate.IsZero() {
			queueEnd = *candidate
			break
		}
	}
	if queueEnd.IsZero() || queueEnd.Before(*timing.EnteredMergingAt) {
		return 0
	}
	return int64(queueEnd.Sub(*timing.EnteredMergingAt) / time.Second)
}

func percentileDuration(values []int64, percentile int) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	index := int(math.Ceil(float64(percentile)/100*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func formatOptionalDuration(seconds int64) string {
	if seconds <= 0 {
		return "n/a"
	}
	return formatDuration(float64(seconds))
}

func prPipelineMergeSummaryClass(warning bool) string {
	if warning {
		return "border-warn/15 bg-warn/15 text-warn"
	}
	return "border-line bg-surface text-text"
}

func appendPRPipelineCard(
	cardsByLane map[string][]prPipelineCard,
	seen map[string]struct{},
	mergeLaneStatuses map[string]mergeLaneCardStatus,
	issue telemetry.Issue,
	state string,
	stageAt time.Time,
	now time.Time,
) {
	laneID := prPipelineLaneID(state)
	if laneID == "" {
		return
	}
	if laneID == "done-today" && !pipelineSameUTCDay(stageAt, now) {
		return
	}

	identity := boardCardIdentityToken(issue.Identifier, issue.ID, issueNumber(issue))
	key := laneID + ":" + boardCardScopedIdentityToken(issue.ProjectID, identity)
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}

	card := prPipelineCardForIssue(issue, state, laneID, stageAt, now)
	if status, ok := mergeLaneStatuses[mergeLaneIssueKey(issue)]; ok {
		card.MergeLaneStatus = status.Label
		card.MergeLaneDetail = status.Detail
		card.MergeLanePrefix = status.Prefix
		card.MergeLaneRef = status.Reference
		card.MergeLaneRefURL = status.URL
		card.MergeLaneSuffix = status.Suffix
		card.MergeLaneClass = status.Class
	}
	cardsByLane[laneID] = append(cardsByLane[laneID], card)
}

func prunePRPipelineCards(cardsByLane map[string][]prPipelineCard) {
	for laneID, cards := range cardsByLane {
		sort.SliceStable(cards, func(i, j int) bool {
			left := cards[i].StageAt
			right := cards[j].StageAt
			if left.IsZero() || right.IsZero() {
				return !left.IsZero() && right.IsZero()
			}
			return left.After(right)
		})
		if laneID == "done-today" && len(cards) > prPipelineDoneTodayLimit {
			cards = cards[:prPipelineDoneTodayLimit]
		}
		cardsByLane[laneID] = cards
	}
}

func prPipelineLaneID(state string) string {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(state), " ", "")) {
	case "humanreview", "review", "inreview", "handoff", "pendingtrackerrefresh":
		return "human-review"
	case "merging":
		return "merging"
	case "done", "complete", "completed", "closed", "cancelled", "canceled":
		return "done-today"
	default:
		return ""
	}
}

func prPipelineCardForIssue(issue telemetry.Issue, state string, laneID string, stageAt time.Time, now time.Time) prPipelineCard {
	ciStatus := prPipelineCIStatus(issue, laneID)
	codexReview := prPipelineCodexReviewState(issue)
	return prPipelineCard{
		IssueNumber:      issueNumber(issue),
		Identity:         issueIdentity(issue),
		IdentityToken:    boardCardIdentityToken(issue.Identifier, issue.ID, issueNumber(issue)),
		Identifier:       issueIdentifier(issue),
		ProjectID:        strings.TrimSpace(issue.ProjectID),
		Title:            issueTitle(issue),
		URL:              prPipelineURL(issue),
		CIStatus:         ciStatus,
		CIClass:          prPipelineCIClass(ciStatus),
		CodexReviewState: codexReview,
		CodexReviewClass: prPipelineCodexReviewClass(codexReview),
		TimeInStage:      prPipelineAge(stageAt, now),
		TimeInStageTitle: prPipelineAgeTitle(state, stageAt, now),
		WaitDetail:       prPipelineWaitDetail(issue),
		Stage:            chartText(state, "n/a"),
		StageAt:          stageAt.UTC(),
	}
}

func prPipelineWaitDetail(issue telemetry.Issue) string {
	parts := []string{}
	mergeDetail := prPipelineMergeWaitDetail(issue)
	if reason := prPipelineDispatchSkipWaitReason(issue); reason != "" {
		parts = append(parts, reason)
	} else if reason := prPipelineAutoPromoteWaitReason(issue); reason != "" {
		parts = append(parts, reason)
	} else if reason := prPipelineHumanReviewWaitReason(issue); reason != "" {
		parts = append(parts, reason)
	}
	if issue.PullRequest == nil && issue.MergeTiming == nil {
		return strings.Join(parts, " / ")
	}
	if issue.PullRequest != nil {
		if hydration := pullRequestHydrationWaitDetail(
			issue.PullRequest.HydrationUnavailableReason,
			issue.PullRequest.HydrationDegradedReason,
			issue.PullRequest.HydrationNextRetryAt,
		); hydration != "" {
			parts = append(parts, hydration)
		}
		if issue.PullRequest.QuietWaitSeconds > 0 {
			parts = append(parts, "quiet "+formatDuration(float64(issue.PullRequest.QuietWaitSeconds)))
		}
		if issue.PullRequest.CIQueueSeconds > 0 {
			parts = append(parts, "queued "+formatDuration(float64(issue.PullRequest.CIQueueSeconds)))
		}
		if issue.PullRequest.CIDurationSeconds > 0 {
			parts = append(parts, "CI "+formatDuration(float64(issue.PullRequest.CIDurationSeconds)))
		}
		if slowChecks := prPipelineSlowChecks(issue.PullRequest.SlowChecks); slowChecks != "" {
			parts = append(parts, "slow "+slowChecks)
		}
		if unstartedChecks := prPipelineUnstartedChecks(issue.PullRequest.UnstartedChecks); unstartedChecks != "" && mergeDetail == "" {
			parts = append(parts, "unstarted "+unstartedChecks)
		}
		if runningChecks := prPipelineRunningChecks(issue.PullRequest.RunningChecks); runningChecks != "" && mergeDetail == "" {
			parts = append(parts, "running "+runningChecks)
		}
	}
	if mergeDetail != "" {
		parts = append(parts, mergeDetail)
	}
	return strings.Join(parts, " / ")
}

func prPipelineDispatchSkipWaitReason(issue telemetry.Issue) string {
	reason := strings.TrimSpace(issue.Metadata[projectKanbanDispatchSkipReasonMetadataKey])
	if reason == "" {
		return ""
	}
	if reason == "artifact_gate_wait_status" {
		if status := strings.TrimSpace(issue.Metadata[projectKanbanArtifactGateStatusMetadataKey]); status != "" {
			return "waiting on artifact gate status ('" + status + "')"
		}
	}
	return "dispatch skipped: " + reason
}

func prPipelineAutoPromoteWaitReason(issue telemetry.Issue) string {
	action := strings.TrimSpace(issue.Metadata[projectKanbanAutoPromoteActionMetadataKey])
	if action != "await_review" && action != "skip" {
		return ""
	}
	reason := strings.TrimSpace(issue.Metadata[projectKanbanAutoPromoteReasonMetadataKey])
	if reason == "" {
		return ""
	}
	if reason == "automated_review_missing" {
		mode := strings.TrimSpace(issue.Metadata[projectKanbanAutomatedReviewModeMetadataKey])
		timeoutAction := strings.TrimSpace(issue.Metadata[projectKanbanAutomatedReviewTimeoutActionMetadataKey])
		if prPipelineLaneID(issue.State) == "human-review" && timeoutAction == "human_review" {
			return "held for human review after " + mode + " automated review timed out"
		}
		if mode == "optional" && timeoutAction == "merge" {
			deadline, err := time.Parse(time.RFC3339, strings.TrimSpace(issue.Metadata[projectKanbanAutomatedReviewDeadlineMetadataKey]))
			if err == nil {
				return "waiting for optional automated review; will merge at " + localTimeToken(deadline, LocalTimeOnly)
			}
			return "waiting for optional automated review; will merge when the wait expires"
		}
	}
	return "auto-promote " + action + ": " + reason
}

func prPipelineHumanReviewWaitReason(issue telemetry.Issue) string {
	if prPipelineLaneID(issue.State) != "human-review" {
		return ""
	}
	if issue.PullRequest == nil {
		return "waiting for linked PR"
	}
	if pullRequestHydrationWaitDetail(
		issue.PullRequest.HydrationUnavailableReason,
		issue.PullRequest.HydrationDegradedReason,
		issue.PullRequest.HydrationNextRetryAt,
	) != "" {
		return ""
	}
	switch prPipelineCIStatus(issue, "human-review") {
	case "fail":
		return "CI failed; Rework routing pending"
	case "pending":
		return "waiting for CI"
	}
	switch strings.ToUpper(strings.TrimSpace(issue.PullRequest.CodexReviewState)) {
	case "":
		return "waiting for automated review"
	case "P1":
		return "P1 review finding blocks promotion"
	}
	if issue.PullRequest.QuietWaitSeconds > 0 {
		return ""
	}
	return "waiting for auto-promote"
}

func prPipelineMergeWaitDetail(issue telemetry.Issue) string {
	timing := issue.MergeTiming
	if timing == nil {
		return ""
	}
	if timing.MergedAt == nil && timing.MergeFailedAt == nil && prPipelineLaneID(issue.State) == "merging" {
		if timing.MergeWorkerSlotAcquiredAt == nil {
			return prPipelineMergeSubstate("waiting for merge worker slot", timing.EnteredMergingAt)
		}
		checks := prPipelineMergeBlockingChecks(issue.PullRequest)
		if checks != "" || issue.PullRequest != nil && prPipelineCIStatus(issue, "merging") == "pending" {
			state := prPipelineMergeSubstate(
				"waiting on current-head CI",
				timing.CIWaitStartedAt,
				timing.MergeStartedAt,
				timing.MergeWorkerSlotAcquiredAt,
			)
			if checks == "" {
				checks = "check name unavailable"
			}
			return state + ": " + checks
		}
		if issue.PullRequest != nil {
			switch strings.ToLower(strings.TrimSpace(issue.PullRequest.MergeableState)) {
			case "", "unknown":
				return prPipelineMergeSubstate(
					"waiting for GitHub mergeability",
					timing.CIWaitStartedAt,
					timing.MergeStartedAt,
					timing.MergeWorkerSlotAcquiredAt,
				)
			}
		}
		return prPipelineMergeSubstate("active merge", timing.MergeWorkerSlotAcquiredAt)
	}
	parts := []string{}
	if timing.QueueWaitSeconds > 0 {
		parts = append(parts, "merge queue "+formatDuration(float64(timing.QueueWaitSeconds)))
	}
	if timing.ActiveMergeDurationSeconds > 0 {
		parts = append(parts, "active merge "+formatDuration(float64(timing.ActiveMergeDurationSeconds)))
	}
	if timing.TotalMergingSeconds > 0 && (timing.MergedAt != nil || timing.MergeFailedAt != nil) {
		parts = append(parts, "total Merging "+formatDuration(float64(timing.TotalMergingSeconds)))
	}
	return strings.Join(parts, " / ")
}

func prPipelineMergeSubstate(state string, sinceCandidates ...*time.Time) string {
	for _, since := range sinceCandidates {
		if since != nil && !since.IsZero() {
			return state + " since " + localTimeToken(*since, LocalTimeOnly)
		}
	}
	return state
}

func prPipelineMergeBlockingChecks(pullRequest *telemetry.PullRequest) string {
	if pullRequest == nil {
		return ""
	}
	unstartedNames := make(map[string]struct{}, len(pullRequest.UnstartedChecks))
	for _, check := range pullRequest.UnstartedChecks {
		if name := strings.ToLower(strings.TrimSpace(check.Name)); name != "" {
			unstartedNames[name] = struct{}{}
		}
	}
	checks := make([]string, 0, len(pullRequest.UnstartedChecks)+len(pullRequest.RequiredCheckFailures)+len(pullRequest.RunningChecks))
	if unstartedChecks := prPipelineUnstartedChecks(pullRequest.UnstartedChecks); unstartedChecks != "" {
		checks = append(checks, unstartedChecks)
	}
	for _, check := range pullRequest.RequiredCheckFailures {
		_, unstarted := unstartedNames[strings.ToLower(strings.TrimSpace(check.Name))]
		if prPipelineCheckPending(check) && !unstarted {
			checks = append(checks, check.Name)
		}
	}
	for _, check := range pullRequest.RunningChecks {
		if _, unstarted := unstartedNames[strings.ToLower(strings.TrimSpace(check))]; !unstarted {
			checks = append(checks, check)
		}
	}
	return strings.Join(uniqueStrings(checks), ", ")
}

func prPipelineCheckPending(check telemetry.PullRequestCheck) bool {
	if strings.EqualFold(strings.TrimSpace(check.Conclusion), "missing") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(check.Status)) {
	case "missing", "pending", "queued", "waiting", "in_progress", "in progress", "requested", "expected":
		return true
	default:
		return false
	}
}

func pullRequestHydrationWaitDetail(unavailableReason string, degradedReason string, nextRetryAt *time.Time) string {
	detail := ""
	switch strings.TrimSpace(unavailableReason) {
	case "rate_limited":
		detail = "PR hydration rate-limited"
	case "secondary_throttled":
		detail = "PR hydration secondary throttled"
	case "primary_exhausted":
		detail = "PR hydration primary exhausted"
	case "rest_budget_reserved":
		detail = "PR hydration waiting for REST budget"
	case "checks_unavailable":
		detail = "PR checks unavailable: current head SHA not found"
	}
	if detail == "" && strings.TrimSpace(degradedReason) == "stale_cached_pull_request" {
		detail = "PR hydration using stale cached data"
	}
	if detail == "" {
		return ""
	}
	if nextRetryAt != nil && !nextRetryAt.IsZero() {
		detail += " until " + localTimeToken(*nextRetryAt, LocalTimeOnly)
	}
	return detail
}

func prPipelineSlowChecks(checks []telemetry.PullRequestCheck) string {
	labels := make([]string, 0, len(checks))
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		if check.DurationSeconds > 0 {
			name += " " + formatDuration(float64(check.DurationSeconds))
		}
		if check.QueueSeconds > 0 {
			name += " (queued " + formatDuration(float64(check.QueueSeconds)) + ")"
		}
		labels = append(labels, name)
	}
	return strings.Join(labels, ", ")
}

func prPipelineRunningChecks(checks []string) string {
	return strings.Join(uniqueStrings(checks), ", ")
}

func prPipelineUnstartedChecks(checks []telemetry.PullRequestCheck) string {
	labels := make([]string, 0, len(checks))
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		if check.QueueSeconds > 0 {
			name += " queued " + formatDuration(float64(check.QueueSeconds))
		}
		labels = append(labels, name+", never started")
	}
	return strings.Join(labels, "; ")
}

func uniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func pipelineNow(snapshot telemetry.Snapshot) time.Time {
	if !snapshot.GeneratedAt.IsZero() {
		return snapshot.GeneratedAt.UTC()
	}
	latest := time.Time{}
	for _, issue := range snapshot.Pipeline {
		if issue.StageUpdatedAt != nil && issue.StageUpdatedAt.After(latest) {
			latest = *issue.StageUpdatedAt
		}
		if issue.UpdatedAt != nil && issue.UpdatedAt.After(latest) {
			latest = *issue.UpdatedAt
		}
	}
	for _, row := range snapshot.Running {
		if row.StartedAt.After(latest) {
			latest = row.StartedAt
		}
	}
	for _, row := range snapshot.Completed {
		if row.CompletedAt.After(latest) {
			latest = row.CompletedAt
		}
	}
	return latest.UTC()
}

func pipelineIssueStageTime(issue telemetry.Issue) time.Time {
	if issue.CurrentLaneEnteredAt != nil && !issue.CurrentLaneEnteredAt.IsZero() {
		return issue.CurrentLaneEnteredAt.UTC()
	}
	if issue.StageUpdatedAt != nil && !issue.StageUpdatedAt.IsZero() {
		return issue.StageUpdatedAt.UTC()
	}
	if issue.UpdatedAt != nil && !issue.UpdatedAt.IsZero() {
		return issue.UpdatedAt.UTC()
	}
	if issue.CreatedAt != nil && !issue.CreatedAt.IsZero() {
		return issue.CreatedAt.UTC()
	}
	return time.Time{}
}

func pipelineSameUTCDay(stageAt time.Time, now time.Time) bool {
	if stageAt.IsZero() || now.IsZero() {
		return true
	}
	stageAt = stageAt.UTC()
	now = now.UTC()
	return stageAt.Year() == now.Year() && stageAt.YearDay() == now.YearDay()
}

func prPipelineCIStatus(issue telemetry.Issue, laneID string) string {
	if issue.PullRequest != nil {
		switch strings.ToLower(strings.TrimSpace(issue.PullRequest.CIStatus)) {
		case "pass", "passed", "success", "green":
			return "pass"
		case "fail", "failed", "failure", "error", "red":
			return "fail"
		case "pending", "expected", "queued", "waiting", "in_progress", "in progress":
			return "pending"
		}
		if strings.EqualFold(issue.PullRequest.State, "MERGED") {
			return "pass"
		}
		return "pending"
	}
	if laneID == "done-today" {
		return "pass"
	}
	return "pending"
}

func prPipelineCodexReviewState(issue telemetry.Issue) string {
	if issue.PullRequest != nil {
		switch strings.ToUpper(strings.TrimSpace(issue.PullRequest.CodexReviewState)) {
		case "P1":
			return "P1"
		case "P2":
			return "P2"
		case "CLEAN":
			return "clean"
		}
	}
	for _, label := range issue.Labels {
		switch strings.ToUpper(strings.TrimSpace(label)) {
		case "P1", "CODEX:P1", "CODEX-REVIEW:P1":
			return "P1"
		case "P2", "CODEX:P2", "CODEX-REVIEW:P2":
			return "P2"
		}
	}
	return "clean"
}

func prPipelineCIClass(status string) string {
	switch status {
	case "pass":
		return "border-ok/15 bg-ok/15 text-ok"
	case "fail":
		return "border-err/15 bg-err/15 text-err"
	default:
		return "border-warn/15 bg-warn/15 text-warn"
	}
}

func prPipelineCodexReviewClass(state string) string {
	switch state {
	case "P1":
		return "border-err/15 bg-err/15 text-err"
	case "P2":
		return "border-warn/15 bg-warn/15 text-warn"
	default:
		return "border-ok/15 bg-ok/15 text-ok"
	}
}

func prPipelineAge(stageAt time.Time, now time.Time) string {
	if stageAt.IsZero() || now.IsZero() {
		return "n/a"
	}
	if now.Before(stageAt) {
		return "0s"
	}
	return formatDuration(now.Sub(stageAt).Seconds())
}

func prPipelineAgeTitle(state string, stageAt time.Time, now time.Time) string {
	if stageAt.IsZero() {
		return "Stage start is unavailable."
	}
	return chartText(state, "Stage") + " since " + timeLabel(stageAt) + " (" + prPipelineAge(stageAt, now) + ")"
}

func issueNumber(issue telemetry.Issue) string {
	if issue.PullRequest != nil && issue.PullRequest.Number > 0 {
		return "#" + strconv.Itoa(issue.PullRequest.Number)
	}
	if reference := issueDisplayReference(issue); reference != "" {
		return reference
	}
	return issueIdentifier(issue)
}

func trackerDriftVisible(snapshot telemetry.Snapshot) bool {
	return len(snapshot.TrackerDrift.UntrackedOpen) > 0 || len(snapshot.TrackerDrift.OpenTerminal) > 0
}

func trackerDriftTotalLabel(snapshot telemetry.Snapshot) string {
	total := len(snapshot.TrackerDrift.UntrackedOpen) + len(snapshot.TrackerDrift.OpenTerminal)
	return projectKanbanCountLabel(total, "cleanup issue", "cleanup issues")
}

func trackerDriftRows(snapshot telemetry.Snapshot) []trackerDriftRow {
	rows := []trackerDriftRow{}
	if len(snapshot.TrackerDrift.UntrackedOpen) > 0 {
		rows = append(rows, trackerDriftRow{
			Title:      "Untracked open issues",
			CountLabel: projectKanbanCountLabel(len(snapshot.TrackerDrift.UntrackedOpen), "issue", "issues"),
			Detail:     "Open issues without a configured Detent status label.",
			Issues:     trackerDriftIssueRows(snapshot.TrackerDrift.UntrackedOpen),
		})
	}
	if len(snapshot.TrackerDrift.OpenTerminal) > 0 {
		rows = append(rows, trackerDriftRow{
			Title:      "Open terminal issues",
			CountLabel: projectKanbanCountLabel(len(snapshot.TrackerDrift.OpenTerminal), "issue", "issues"),
			Detail:     "Open issues carrying a terminal Detent status label.",
			Issues:     trackerDriftIssueRows(snapshot.TrackerDrift.OpenTerminal),
		})
	}
	return rows
}

func trackerDriftIssueRows(issues []telemetry.Issue) []trackerDriftIssueRow {
	rows := make([]trackerDriftIssueRow, 0, len(issues))
	for _, issue := range issues {
		rows = append(rows, trackerDriftIssueRow{
			Number: projectKanbanIssueNumber(issue),
			Title:  strings.TrimSpace(issue.Title),
			URL:    strings.TrimSpace(issue.URL),
			State:  strings.TrimSpace(issue.State),
			Labels: strings.Join(issue.Labels, ", "),
		})
	}
	return rows
}

func prPipelineURL(issue telemetry.Issue) string {
	if issue.PullRequest != nil && strings.TrimSpace(issue.PullRequest.URL) != "" {
		return strings.TrimSpace(issue.PullRequest.URL)
	}
	return issue.URL
}

func queuedDueLabel(row telemetry.Queued) string {
	if row.DueAt != nil {
		return timeLabel(*row.DueAt)
	}
	if row.DueInMillis > 0 {
		return "in " + formatDuration(float64(row.DueInMillis)/1000)
	}
	return "n/a"
}

func queuedStateLabel(row telemetry.Queued) string {
	if row.QueueState == telemetry.QueueStateWaitingOnCI {
		return "Waiting on CI"
	}
	return "Retrying"
}

func queuedStateClass(row telemetry.Queued) string {
	base := "inline-flex rounded-full px-2 py-1 font-medium "
	if row.QueueState == telemetry.QueueStateWaitingOnCI {
		return base + "bg-accent/15 text-accent"
	}
	return base + "bg-warn/15 text-warn"
}

func rowError(value string) string {
	if strings.TrimSpace(value) == "" {
		return "n/a"
	}
	return value
}

func blockedAtLabel(row telemetry.Blocked) string {
	if row.BlockedAt == nil {
		return "n/a"
	}
	return timeLabel(*row.BlockedAt)
}

func blockedLastUpdate(row telemetry.Blocked) string {
	if row.LastMessage != "" {
		return row.LastMessage
	}
	if row.LastEvent != "" {
		return row.LastEvent
	}
	return "n/a"
}

func blockedLastUpdateMeta(row telemetry.Blocked) string {
	if row.LastEvent == "" && row.LastEventAt == nil {
		return "n/a"
	}
	parts := make([]string, 0, 2)
	if row.LastEvent != "" {
		parts = append(parts, row.LastEvent)
	}
	if row.LastEventAt != nil {
		parts = append(parts, timeLabel(*row.LastEventAt))
	}
	return strings.Join(parts, " / ")
}

func blockedRecoverySummary(row telemetry.Blocked) string {
	evidenceDetail := boardBlockerEvidenceDetail(row, time.Time{})
	action := strings.ToLower(strings.TrimSpace(row.RecoveryAction))
	reason := strings.ReplaceAll(strings.TrimSpace(row.RecoveryReason), "_", " ")
	remedy := strings.TrimSpace(row.RecoveryRemedy)
	if action == "hold" {
		detail := "Needs human attention"
		if reason != "" {
			detail += ": " + reason
		}
		if remedy != "" {
			detail += ". " + remedy
		}
		if evidenceDetail != "" {
			detail += ". " + evidenceDetail
		}
		return detail
	}
	if action == "defer" {
		detail := "Automatic recovery deferred"
		if reason != "" {
			detail += ": " + reason
		}
		if row.RecoveryRoot != nil {
			root := strings.TrimSpace(row.RecoveryRoot.IssueIdentifier)
			if root == "" {
				root = strings.TrimSpace(row.RecoveryRoot.IssueID)
			}
			if root != "" {
				detail += "; held root " + root
			}
			if rootReason := strings.ReplaceAll(strings.TrimSpace(row.RecoveryRoot.Reason), "_", " "); rootReason != "" {
				detail += " (" + rootReason + ")"
			}
		}
		if evidenceDetail != "" {
			detail += "; " + evidenceDetail
		}
		return detail
	}
	return rowError(row.Error)
}

func completedAtLabel(row telemetry.Completed) string {
	if row.CompletedAt.IsZero() {
		return "n/a"
	}
	return timeLabel(row.CompletedAt)
}

func completedRuntime(row telemetry.Completed) string {
	return formatDuration(row.RuntimeSeconds) + " / " + formatInt(int64(row.Turns)) + " turns"
}

func completedState(row telemetry.Completed) string {
	if strings.TrimSpace(row.FinalState) == "" {
		return "completed"
	}
	return row.FinalState
}

func boardStateRows(snapshot telemetry.Snapshot) []boardStateRow {
	counts := telemetry.BoardStateCounts(snapshot)
	total := boardStateTotal(counts)
	rows := make([]boardStateRow, 0, len(counts))
	for _, count := range counts {
		percent := "0%"
		if total > 0 {
			percent = fmt.Sprintf("%.0f%%", float64(count.Count)/float64(total)*100)
		}
		rows = append(rows, boardStateRow{
			State:      count.State,
			Count:      count.Count,
			CountLabel: formatCount(count.Count),
			Percent:    percent,
			DotClass:   boardStateDotClass(count.State),
		})
	}
	return rows
}

func boardStateTotal(counts []telemetry.BoardStateCount) int {
	total := 0
	for _, count := range counts {
		total += count.Count
	}
	return total
}

func boardStateTotalLabel(snapshot telemetry.Snapshot) string {
	return formatCount(boardStateTotal(telemetry.BoardStateCounts(snapshot)))
}

func boardDistributionChart(snapshot telemetry.Snapshot) TimelineChartData {
	counts := telemetry.BoardStateCounts(snapshot)
	segments := make([]TimelineSegment, 0, len(counts))
	for _, count := range counts {
		segments = append(segments, TimelineSegment{
			Label: count.State,
			Value: float64(count.Count),
			Class: boardStateTextClass(count.State),
		})
	}
	return TimelineChartData{
		Title:       "Current issue state distribution",
		AriaLabel:   "Current issue state distribution",
		Segments:    segments,
		ValueSuffix: "issues",
		Class:       "h-9",
		Height:      36,
	}
}

func boardProgressChart(snapshot telemetry.Snapshot) SeriesChartData {
	points := telemetry.BoardProgressPoints(snapshot)
	chartPoints := make([]webchart.Point, 0, len(points))
	for _, point := range points {
		label := point.Label
		if !point.At.IsZero() {
			label = localTimeToken(point.At, LocalTimeOnly)
		}
		chartPoints = append(chartPoints, webchart.Point{
			Label: label,
			Value: float64(point.Count),
		})
	}
	return SeriesChartData{
		Title:       "Completed sessions over time",
		AriaLabel:   "Completed sessions over time",
		Points:      chartPoints,
		ValueSuffix: "sessions",
		ColorClass:  "text-ok",
	}
}

func boardProgressCount(snapshot telemetry.Snapshot) string {
	points := telemetry.BoardProgressPoints(snapshot)
	if len(points) == 0 {
		return "0"
	}
	return formatCount(points[len(points)-1].Count)
}

func cycleTimeHistogramChart(report telemetry.CycleTimeReport) BarChartData {
	bars := make([]webchart.Point, 0, len(report.Buckets))
	for _, bucket := range report.Buckets {
		bars = append(bars, webchart.Point{
			Label: bucket.Label,
			Value: float64(bucket.Count),
		})
	}
	return BarChartData{
		Title:       "Cycle time histogram",
		AriaLabel:   "Cycle time histogram",
		Bars:        bars,
		ValueSuffix: "issues",
		ColorClass:  "text-ok",
		Class:       "h-28",
		Height:      112,
	}
}

func cycleTimeAverageLabel(report telemetry.CycleTimeReport) string {
	return formatDuration(float64(report.AverageSeconds))
}

func cycleTimeCountLabel(report telemetry.CycleTimeReport) string {
	count := len(report.Issues)
	if count == 1 {
		return "1 completed"
	}
	return formatInt(int64(count)) + " completed"
}

func cycleTimeBucketRows(report telemetry.CycleTimeReport) []cycleTimeBucketRow {
	rows := make([]cycleTimeBucketRow, 0, len(report.Buckets))
	for _, bucket := range report.Buckets {
		rows = append(rows, cycleTimeBucketRow{
			Label: bucket.Label,
			Count: formatInt(int64(bucket.Count)),
		})
	}
	return rows
}

func cycleTimeUnavailableDetail(report telemetry.CycleTimeReport) string {
	if strings.TrimSpace(report.DegradedReason) != "" {
		return report.DegradedReason
	}
	return "Runtime store unavailable."
}

func workflowMetricsAvailable(report telemetry.WorkflowMetrics) bool {
	return report.Available && strings.TrimSpace(report.DegradedReason) == ""
}

func workflowMetricsUnavailableDetail(report telemetry.WorkflowMetrics) string {
	if strings.TrimSpace(report.DegradedReason) != "" {
		return report.DegradedReason
	}
	return "Runtime store unavailable."
}

func workflowMetricsSummaryLabel(report telemetry.WorkflowMetrics) string {
	count := int64(0)
	for _, window := range report.Windows {
		if window.Label != "24h" {
			continue
		}
		for _, metric := range window.Lanes {
			count += metric.Count
		}
		break
	}
	if count == 1 {
		return "1 lane event"
	}
	return formatInt(count) + " lane events"
}

func diagnosticsStorageName(data DashboardData) string {
	return "detent.ui.diagnostics.selectedTab." + diagnosticsStorageKey(data)
}

func diagnosticsStorageKey(data DashboardData) string {
	scope := strings.TrimSpace(data.ProjectID)
	if scope == "" {
		scope = strings.TrimSpace(data.Snapshot.Project.ID)
	}
	if scope == "" {
		return "fleet"
	}
	key := projectKanbanLaneID(scope)
	if key == "unknown" {
		return "project"
	}
	return key
}

func diagnosticsBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func diagnosticsTabIndex(active bool) string {
	if active {
		return "0"
	}
	return "-1"
}

func diagnosticsConditionRowClass(last bool) string {
	class := "grid grid-cols-[105px_140px_150px_minmax(0,1fr)_120px] items-start gap-3 px-4 py-3"
	if !last {
		class += " border-b border-line"
	}
	return class
}

func diagnosticsConditionRows(snapshot telemetry.Snapshot) []diagnosticsConditionRow {
	rows := make([]diagnosticsConditionRow, 0, len(snapshot.StalenessWarnings)+len(snapshot.DispatchStalls)+len(snapshot.BackendOutages)+len(snapshot.DispatchRecoveries)+len(snapshot.AdmissionProposals)+len(snapshot.Projects))
	for index, warning := range snapshot.StalenessWarnings {
		class := stalenessConditionClass(warning)
		target := boardFirstNonBlank(warning.Identifier, warning.IssueID, warning.Lane, "project")
		detail := strings.TrimSpace(warning.Detail)
		if warning.AgeSeconds > 0 {
			detail = strings.Trim(strings.TrimSpace(detail)+" · age "+formatDuration(float64(warning.AgeSeconds)), " ·")
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-staleness-" + boardAlertRowSlug(warning.ID, index),
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  diagnosticsConditionProject(warning.ProjectID),
			Target:     target,
			TargetURL:  strings.TrimSpace(warning.IssueURL),
			Summary:    stalenessExceptionTitle(warning),
			Detail:     detail,
			ObservedAt: warning.LastObservedAt,
		})
	}
	for index, stall := range snapshot.DispatchStalls {
		class := dispatchConditionClass(stall)
		if class == "" {
			continue
		}
		detail := boardCountLabel(stall.CandidateCount, "candidate", "candidates") + " skipped for " + formatDuration(float64(stall.StallDurationSeconds))
		if reason := strings.TrimSpace(stall.WaitReason); reason != "" {
			detail += " · " + reason
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-dispatch-" + boardAlertRowSlug(stall.ProjectID, index),
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  diagnosticsConditionProject(stall.ProjectID),
			Target:     "Dispatch",
			Summary:    diagnosticsDispatchConditionSummary(stall, class),
			Detail:     detail,
			ObservedAt: stall.ObservedAt,
		})
	}
	for index, outage := range snapshot.BackendOutages {
		class := observability.BackendOutage(outage.Kind)
		detail, detailAt, showDetailAt := backendCapacityOutageDetailParts(outage, snapshot.GeneratedAt)
		if showDetailAt && !detailAt.IsZero() {
			detail += " " + localTimeToken(detailAt, LocalDateTimeZone)
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-backend-" + boardAlertRowSlug(boardAlertBackendCapacityRowKey(outage), index),
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  diagnosticsConditionProject(outage.ProjectID),
			Target:     backendCapacityBackendID(outage),
			Summary:    backendCapacityOutageTitle(outage),
			Detail:     detail,
			ObservedAt: outage.LastObservedAt,
		})
	}
	for index, breaker := range snapshot.FailureBreakers {
		class := failureBreakerConditionClass(breaker)
		detail, resumeAt, showResumeAt := failureBreakerDetailParts(breaker, snapshot.GeneratedAt)
		if showResumeAt && !resumeAt.IsZero() {
			detail += " " + localTimeToken(resumeAt, LocalDateTimeZone)
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-breaker-" + boardAlertRowSlug(breaker.ProjectID+breaker.Class, index),
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  diagnosticsConditionProject(breaker.ProjectID),
			Target:     "Failure breaker",
			Summary:    failureBreakerCauseLabel(breaker),
			Detail:     detail,
			ObservedAt: breaker.TrippedAt,
		})
	}
	for index, issue := range snapshot.StrandedActiveIssues {
		target := boardFirstNonBlank(issue.Identifier, issue.IssueID, "issue")
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-stranded-" + boardAlertRowSlug(target, index),
			Class:      observability.ClassFault,
			ClassLabel: diagnosticsConditionClassLabel(observability.ClassFault),
			Kind:       diagnosticsConditionKind(observability.ClassFault),
			ProjectID:  diagnosticsConditionProject(issue.ProjectID),
			Target:     target,
			TargetURL:  strings.TrimSpace(issue.IssueURL),
			Summary:    "Active work has no live worker",
			Detail:     strings.Trim(formatDuration(float64(issue.DurationSeconds))+" · "+strings.TrimSpace(issue.LastRefusalReason), " ·"),
			ObservedAt: diagnosticsConditionObservedAt(issue.LastRefusalAt, issue.Since),
		})
	}
	for index, condition := range snapshot.TrackerUnavailable {
		rows = append(rows, diagnosticsFaultRow(
			"tracker-"+boardAlertRowSlug(condition.ProjectID, index),
			condition.ProjectID,
			"Tracker",
			"Tracker is unavailable",
			strings.Trim(strings.TrimSpace(condition.Operation)+" · "+strings.TrimSpace(condition.LastError), " ·"),
			condition.LastObservedAt,
		))
	}
	for index, condition := range snapshot.ForgeUnavailable {
		rows = append(rows, diagnosticsFaultRow(
			"forge-"+boardAlertRowSlug(condition.ProjectID, index),
			condition.ProjectID,
			"Forge",
			"Forge writes are unavailable",
			strings.Trim(strings.TrimSpace(condition.Operation)+" · "+strings.TrimSpace(condition.LastError), " ·"),
			condition.LastObservedAt,
		))
	}
	for index, condition := range snapshot.CIUnavailable {
		rows = append(rows, diagnosticsFaultRow(
			"ci-"+boardAlertRowSlug(condition.ProjectID, index),
			condition.ProjectID,
			"CI",
			"Required checks are unavailable",
			boardCountLabel(condition.UnstartedCheckCount, "check", "checks")+" did not start",
			condition.LastObservedAt,
		))
	}
	for index, failure := range snapshot.RefreshFailures() {
		rows = append(rows, diagnosticsFaultRow(
			"refresh-"+boardAlertRowSlug(failure.ProjectID+string(failure.Source), index),
			failure.ProjectID,
			"Tracker refresh",
			"Live tracker refresh failed",
			refreshFailureDetail(failure),
			diagnosticsConditionObservedAt(failure.LastErrorAt, time.Time{}),
		))
	}
	for index, recovery := range snapshot.DispatchRecoveries {
		class := observability.DispatchRecovery(recovery.Status, recovery.ResumeAt, snapshot.GeneratedAt)
		detail, detailAt, showDetailAt := dispatchRecoveryDetailParts(recovery, snapshot.GeneratedAt)
		if showDetailAt && !detailAt.IsZero() {
			detail += " " + localTimeToken(detailAt, LocalDateTimeZone)
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-recovery-" + boardAlertRowSlug(recovery.ProjectID+recovery.Kind, index),
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  diagnosticsConditionProject(recovery.ProjectID),
			Target:     "Dispatch recovery",
			Summary:    dispatchRecoveryKindLabel(recovery.Kind) + " · " + strings.TrimSpace(recovery.Status),
			Detail:     detail,
			ObservedAt: recovery.StartedAt,
		})
	}
	for index, proposal := range snapshot.AdmissionProposals {
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-admission-" + boardAlertRowSlug(proposal.ID, index),
			Class:      observability.ClassReviewQueue,
			ClassLabel: diagnosticsConditionClassLabel(observability.ClassReviewQueue),
			Kind:       diagnosticsConditionKind(observability.ClassReviewQueue),
			ProjectID:  diagnosticsConditionProject(proposal.ProjectID),
			Target:     admissionProposalTarget(proposal),
			TargetURL:  strings.TrimSpace(proposal.IssueURL),
			Summary:    "Admission decision",
			Detail:     admissionProposalTiming(proposal, snapshot.GeneratedAt),
			ObservedAt: proposal.CreatedAt,
		})
	}
	rows = append(rows, diagnosticsPacingRows(snapshot)...)
	if snapshot.OverloadRetriesLastHour > 0 {
		class := observability.ClassDiagnostic
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-overload-retries",
			Class:      class,
			ClassLabel: diagnosticsConditionClassLabel(class),
			Kind:       diagnosticsConditionKind(class),
			ProjectID:  "Fleet",
			Target:     "Backend capacity",
			Summary:    formatCount(snapshot.OverloadRetriesLastHour) + " overload retries last hour",
			Detail:     "Transient overload retries are handled automatically.",
			ObservedAt: snapshot.GeneratedAt,
		})
	}
	if detail := strings.TrimSpace(snapshot.Update.LastError); detail != "" {
		rows = append(rows, diagnosticsFaultRow(
			"update",
			"",
			"Detent update",
			"Automatic update failed",
			detail,
			diagnosticsConditionObservedAt(snapshot.Update.LastCheckAt, time.Time{}),
		))
	} else if detentUpdatePending(snapshot.Update) {
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-update",
			Class:      observability.ClassDiagnostic,
			ClassLabel: diagnosticsConditionClassLabel(observability.ClassDiagnostic),
			Kind:       diagnosticsConditionKind(observability.ClassDiagnostic),
			ProjectID:  "Fleet",
			Target:     "Detent update",
			Summary:    "Update is waiting for active work to drain",
			Detail:     detentPendingUpdateVersion(snapshot.Update),
			ObservedAt: diagnosticsConditionObservedAt(snapshot.Update.PendingSince, time.Time{}),
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if diagnosticsConditionRank(rows[i].Class) != diagnosticsConditionRank(rows[j].Class) {
			return diagnosticsConditionRank(rows[i].Class) < diagnosticsConditionRank(rows[j].Class)
		}
		if rows[i].ProjectID != rows[j].ProjectID {
			return rows[i].ProjectID < rows[j].ProjectID
		}
		if !rows[i].ObservedAt.Equal(rows[j].ObservedAt) {
			return rows[i].ObservedAt.After(rows[j].ObservedAt)
		}
		return rows[i].ID < rows[j].ID
	})
	return rows
}

func diagnosticsFaultRow(id string, projectID string, target string, summary string, detail string, observedAt time.Time) diagnosticsConditionRow {
	return diagnosticsConditionRow{
		ID:         "diagnostics-condition-" + id,
		Class:      observability.ClassFault,
		ClassLabel: diagnosticsConditionClassLabel(observability.ClassFault),
		Kind:       diagnosticsConditionKind(observability.ClassFault),
		ProjectID:  diagnosticsConditionProject(projectID),
		Target:     target,
		Summary:    summary,
		Detail:     detail,
		ObservedAt: observedAt,
	}
}

func diagnosticsConditionObservedAt(value *time.Time, fallback time.Time) time.Time {
	if value != nil && !value.IsZero() {
		return value.UTC()
	}
	return fallback
}

func failureBreakerConditionClass(breaker telemetry.FailureBreaker) observability.Class {
	if len(actionableBoardFailureBreakers([]telemetry.FailureBreaker{breaker})) == 1 {
		return observability.ClassFault
	}
	return observability.ClassDiagnostic
}

func diagnosticsPacingRows(snapshot telemetry.Snapshot) []diagnosticsConditionRow {
	type pacingScope struct {
		projectID string
		pacing    telemetry.RateWindowPacing
	}
	scopes := make([]pacingScope, 0, len(snapshot.Projects)+1)
	if snapshot.Dispatch.RateWindowPacing.Applicable || snapshot.Dispatch.RateWindowPacing.ScalingApplied {
		scopes = append(scopes, pacingScope{projectID: snapshot.Dispatch.ProjectID, pacing: snapshot.Dispatch.RateWindowPacing})
	}
	for _, project := range snapshot.Projects {
		if project.Dispatch.RateWindowPacing.Applicable || project.Dispatch.RateWindowPacing.ScalingApplied {
			scopes = append(scopes, pacingScope{projectID: project.Project.ID, pacing: project.Dispatch.RateWindowPacing})
		}
	}
	rows := make([]diagnosticsConditionRow, 0, len(scopes))
	seen := map[string]struct{}{}
	for index, scope := range scopes {
		projectID := diagnosticsConditionProject(scope.projectID)
		if _, ok := seen[projectID]; ok {
			continue
		}
		seen[projectID] = struct{}{}
		detail := "permit ceiling " + formatCount(scope.pacing.PermitCeiling) + " · bucket " + strings.TrimSpace(scope.pacing.BucketStatus)
		if scope.pacing.ObservedRemainingPercent != nil {
			detail += " · " + formatContextPercent(*scope.pacing.ObservedRemainingPercent) + " remaining"
		}
		rows = append(rows, diagnosticsConditionRow{
			ID:         "diagnostics-condition-pacing-" + boardAlertRowSlug(projectID, index),
			Class:      observability.ClassDiagnostic,
			ClassLabel: diagnosticsConditionClassLabel(observability.ClassDiagnostic),
			Kind:       diagnosticsConditionKind(observability.ClassDiagnostic),
			ProjectID:  projectID,
			Target:     "Provider pacing",
			Summary:    strings.TrimSpace(scope.pacing.Mode),
			Detail:     detail,
		})
	}
	return rows
}

func diagnosticsConditionClassLabel(class observability.Class) string {
	switch class {
	case observability.ClassFault:
		return "Fault"
	case observability.ClassReviewQueue:
		return "Review queue"
	default:
		return "Diagnostic"
	}
}

func diagnosticsConditionKind(class observability.Class) primitives.Kind {
	switch class {
	case observability.ClassFault:
		return primitives.KindErr
	case observability.ClassDiagnostic:
		return primitives.KindInfo
	default:
		return primitives.KindNeutral
	}
}

func diagnosticsConditionRank(class observability.Class) int {
	switch class {
	case observability.ClassFault:
		return 0
	case observability.ClassDiagnostic:
		return 1
	default:
		return 2
	}
}

func diagnosticsConditionProject(projectID string) string {
	if projectID = strings.TrimSpace(projectID); projectID != "" {
		return projectID
	}
	return "Fleet"
}

func diagnosticsDispatchConditionSummary(stall telemetry.DispatchStatus, class observability.Class) string {
	if class == observability.ClassFault && strings.TrimSpace(stall.WaitReasonCode) == "authorization_selector_declined" {
		return "Authorization selector excludes every candidate"
	}
	if class == observability.ClassReviewQueue {
		return "Waiting on review by design"
	}
	return "Dispatch pacing or cooldown"
}

func diagnosticsSummaryFacts(data DashboardData) []diagnosticsSummaryFact {
	snapshot := data.Snapshot
	bottleneck := workflowBottleneck(snapshot.WorkflowMetrics)
	return []diagnosticsSummaryFact{
		{
			Label:  "Overall health",
			Value:  diagnosticsHealthLabel(snapshot),
			Detail: diagnosticsHealthDetail(snapshot),
			Kind:   diagnosticsHealthKind(snapshot),
		},
		{
			Label:  "Active bottleneck",
			Value:  bottleneck.Label,
			Detail: bottleneck.Detail,
			Kind:   primitives.KindWarn,
		},
		diagnosticsForwardProgressFact(snapshot),
		{
			Label:  "Data freshness",
			Value:  diagnosticsDataFreshnessValue(snapshot),
			Detail: diagnosticsDataFreshnessDetail(snapshot),
			Kind:   runtimeStoreStatusKind(snapshot.WorkflowMetrics.RuntimeStore),
		},
		{
			Label:  "API pressure",
			Value:  gitHubAPIHealth(snapshot).Label,
			Detail: gitHubAPIHealth(snapshot).Summary,
			Kind:   gitHubAPIHealthKind(snapshot),
		},
	}
}

func diagnosticsHealthLabel(snapshot telemetry.Snapshot) string {
	if snapshot.Shutdown.Draining {
		return "Draining"
	}
	if snapshotDegraded(snapshot) || strings.TrimSpace(snapshot.WorkflowMetrics.DegradedReason) != "" || strings.TrimSpace(snapshot.Budget.DegradedReason) != "" {
		return "Degraded"
	}
	if !runtimeCountComplete(snapshot) {
		return "Starting"
	}
	if runningCount(snapshot) == 0 && queueCount(snapshot)+blockedCount(snapshot) > 0 {
		return "Stalled"
	}
	return "Running"
}

func diagnosticsHealthDetail(snapshot telemetry.Snapshot) string {
	if snapshot.Shutdown.Draining {
		return formatCount(snapshot.Shutdown.SessionsRemaining) + " sessions remaining"
	}
	if snapshotDegraded(snapshot) {
		return snapshotDegradedRefreshDetail(snapshot)
	}
	if reason := strings.TrimSpace(snapshot.WorkflowMetrics.DegradedReason); reason != "" {
		return reason
	}
	if reason := strings.TrimSpace(snapshot.Budget.DegradedReason); reason != "" {
		return reason
	}
	if !runtimeCountComplete(snapshot) {
		return "Runtime state is still initializing; active worker count is not yet complete."
	}
	if runningCount(snapshot) == 0 && queueCount(snapshot)+blockedCount(snapshot) > 0 {
		return "No active workers while work is queued or blocked."
	}
	return "Tracker data and runtime telemetry are available."
}

func diagnosticsHealthKind(snapshot telemetry.Snapshot) primitives.Kind {
	switch diagnosticsHealthLabel(snapshot) {
	case "Running":
		return primitives.KindOK
	case "Draining", "Stalled":
		return primitives.KindWarn
	case "Degraded":
		return primitives.KindErr
	default:
		return primitives.KindNeutral
	}
}

func diagnosticsForwardProgressValue(snapshot telemetry.Snapshot) string {
	parts := []string{
		runningCountLabel(snapshot) + " active",
		formatCount(queueCount(snapshot)) + " queued",
	}
	return strings.Join(parts, " / ")
}

func diagnosticsForwardProgressFact(snapshot telemetry.Snapshot) diagnosticsSummaryFact {
	fact := diagnosticsSummaryFact{
		Label: "Forward progress",
		Value: diagnosticsForwardProgressValue(snapshot),
		Kind:  primitives.KindInfo,
	}
	if transition := diagnosticsLastTransition(snapshot); transition != "" {
		fact.Detail = "Last transition: " + transition + ". "
	}
	if latest := diagnosticsLatestCompleted(snapshot); latest != nil {
		fact.DetailPrefix = "Last merge: "
		fact.DetailReference = issueIdentifier(latest.Issue)
		fact.DetailReferenceURL = issueURL(latest.Issue)
		fact.DetailSuffix = " at " + timeLabel(latest.CompletedAt) + "."
		return fact
	}
	if fact.Detail == "" {
		fact.Detail = "No completed transition or merge is visible in this snapshot."
	}
	return fact
}

func diagnosticsLastTransition(snapshot telemetry.Snapshot) string {
	var latest *telemetry.ActivityEvent
	for i := range snapshot.Events {
		event := snapshot.Events[i]
		if event.At.IsZero() {
			continue
		}
		if latest == nil || event.At.After(latest.At) {
			latest = &event
		}
	}
	if latest == nil {
		return ""
	}
	label := strings.TrimSpace(latest.Event)
	if label == "" {
		label = strings.TrimSpace(latest.Message)
	}
	if label == "" {
		label = "event"
	}
	return label + " at " + timeLabel(latest.At)
}

func diagnosticsLatestCompleted(snapshot telemetry.Snapshot) *telemetry.Completed {
	var latest *telemetry.Completed
	for i := range snapshot.Completed {
		completed := snapshot.Completed[i]
		if completed.CompletedAt.IsZero() {
			continue
		}
		if latest == nil || completed.CompletedAt.After(latest.CompletedAt) {
			latest = &completed
		}
	}
	return latest
}

func diagnosticsDataFreshnessValue(snapshot telemetry.Snapshot) string {
	store := snapshot.WorkflowMetrics.RuntimeStore
	if store.Status == "healthy" {
		return "SQLite-backed history"
	}
	if store.Status == "not_configured" {
		return "No runtime store"
	}
	if strings.TrimSpace(snapshot.WorkflowMetrics.DegradedReason) != "" {
		return "Metrics degraded"
	}
	return "Runtime evidence pending"
}

func diagnosticsDataFreshnessDetail(snapshot telemetry.Snapshot) string {
	parts := []string{}
	if snapshot.Refresh.LastRefreshAt != nil {
		parts = append(parts, "Tracker refresh "+timeLabel(*snapshot.Refresh.LastRefreshAt)+".")
	}
	if newest := snapshot.WorkflowMetrics.RuntimeStore.WorkflowPhaseEvents.NewestFinishedAt; newest != nil {
		parts = append(parts, "Newest metrics event "+timeLabel(*newest)+".")
	}
	if status := runtimeStoreStatusLabel(snapshot.WorkflowMetrics.RuntimeStore); status != "" {
		parts = append(parts, status+".")
	}
	if len(parts) == 0 {
		return "No tracker refresh or runtime metrics event has been recorded yet."
	}
	return strings.Join(parts, " ")
}

func runtimeStoreStatusLabel(store telemetry.RuntimeStoreEvidence) string {
	switch strings.TrimSpace(store.Status) {
	case "healthy":
		return "SQLite-backed history healthy"
	case "not_configured":
		return "SQLite runtime store not configured"
	case "degraded":
		return "SQLite runtime store degraded"
	default:
		if strings.TrimSpace(store.Backend) == "" {
			return "Runtime store evidence unavailable"
		}
		return workflowPhaseLabel(store.Backend) + " runtime store status unknown"
	}
}

func runtimeStoreStatusKind(store telemetry.RuntimeStoreEvidence) primitives.Kind {
	switch strings.TrimSpace(store.Status) {
	case "healthy":
		return primitives.KindOK
	case "not_configured", "":
		return primitives.KindNeutral
	default:
		return primitives.KindErr
	}
}

func runtimeStorePathLabel(store telemetry.RuntimeStoreEvidence) string {
	if path := strings.TrimSpace(store.Path); path != "" {
		return path
	}
	return "path unavailable"
}

func runtimeStoreMigrationLabel(store telemetry.RuntimeStoreEvidence) string {
	if label := strings.TrimSpace(store.MigrationStatus); label != "" {
		return label
	}
	if store.MigrationVersion > 0 {
		return "applied through " + formatInt(store.MigrationVersion)
	}
	return "migration status unavailable"
}

func runtimeStoreTableRows(store telemetry.RuntimeStoreEvidence) []runtimeStoreTableRow {
	rows := make([]runtimeStoreTableRow, 0, len(store.Tables))
	for _, table := range store.Tables {
		scope := strings.TrimSpace(table.Scope)
		if scope == "" {
			scope = "fleet"
		}
		rows = append(rows, runtimeStoreTableRow{
			Name:     strings.TrimSpace(table.Name),
			Scope:    scope,
			RowCount: runtimeStoreRowCountLabel(table.RowCount, scope),
		})
	}
	return rows
}

func runtimeStoreRowCountLabel(count int64, scope string) string {
	rowLabel := "rows"
	if count == 1 {
		rowLabel = "row"
	}
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return formatInt(count) + " " + rowLabel
	}
	return formatInt(count) + " " + scope + " " + rowLabel
}

func runtimeWorkflowEventNewestLabel(store telemetry.RuntimeStoreEvidence) string {
	if store.WorkflowPhaseEvents.NewestFinishedAt == nil {
		return "No completed workflow phase event"
	}
	return timeLabel(*store.WorkflowPhaseEvents.NewestFinishedAt)
}

func runtimeWorkflowEventOldestLabel(store telemetry.RuntimeStoreEvidence) string {
	if store.WorkflowPhaseEvents.OldestFinishedAt == nil {
		return "No completed workflow phase event"
	}
	return timeLabel(*store.WorkflowPhaseEvents.OldestFinishedAt)
}

func runtimeWorkflowEventCountLabel(store telemetry.RuntimeStoreEvidence) string {
	return runtimeStoreRowCountLabel(store.WorkflowPhaseEvents.RowCount, runtimeWorkflowEventScope(store))
}

func runtimeWorkflowEventScope(store telemetry.RuntimeStoreEvidence) string {
	for _, table := range store.Tables {
		if table.Name == "workflow_phase_events" {
			return table.Scope
		}
	}
	return "fleet"
}

func workflowMetricsEmptyHistoryTitle(report telemetry.WorkflowMetrics) string {
	if report.RuntimeStore.Status == "healthy" && report.RuntimeStore.WorkflowPhaseEvents.RowCount == 0 {
		return "SQLite history is empty."
	}
	return "No workflow timing events yet."
}

func workflowMetricsEmptyHistoryDetail(report telemetry.WorkflowMetrics) string {
	if report.RuntimeStore.Status == "healthy" && report.RuntimeStore.WorkflowPhaseEvents.RowCount == 0 {
		return "Lane averages appear after Detent records lane exits. New state transitions will populate the current query windows."
	}
	return "Lane and sub-phase aggregates appear after Detent records state transitions or observable work phases."
}

func workflowMetricsWindowLabels(report telemetry.WorkflowMetrics) string {
	labels := make([]string, 0, len(report.Windows))
	for _, window := range report.Windows {
		if strings.TrimSpace(window.Label) != "" {
			labels = append(labels, strings.TrimSpace(window.Label))
		}
	}
	if len(labels) == 0 {
		return "24h / 7d / 30d"
	}
	return strings.Join(labels, " / ")
}

func workflowLaneTrendCards(report telemetry.WorkflowMetrics) []workflowLaneTrendCard {
	window, ok := workflowChartWindow(report)
	if !ok {
		return []workflowLaneTrendCard{}
	}

	trendsByLane := make(map[string]telemetry.WorkflowLaneTrend, len(window.LaneTrends))
	for _, trend := range window.LaneTrends {
		if lane, ok := workflowTrackedLaneName(trend.PhaseName); ok {
			trend.PhaseName = lane
			trendsByLane[lane] = trend
		}
	}

	cards := make([]workflowLaneTrendCard, 0, len(workflowTrackedLaneNames()))
	for _, lane := range workflowTrackedLaneNames() {
		trend := trendsByLane[lane]
		points := make([]webchart.Point, 0, len(trend.Points))
		latestAverage := int64(0)
		for _, point := range trend.Points {
			points = append(points, webchart.Point{Label: workflowLaneTrendPointLabel(point, window), Value: float64(point.AverageSeconds)})
			if point.Count > 0 {
				latestAverage = point.AverageSeconds
			}
		}
		hasData := trend.TotalCount > 0
		summary := "No samples"
		if hasData {
			summary = formatDuration(float64(latestAverage)) + " latest"
		}
		cards = append(cards, workflowLaneTrendCard{
			Lane:    lane,
			Window:  window.Label,
			Summary: summary,
			HasData: hasData,
			Chart: SeriesChartData{
				Title:       lane + " average lane time",
				AriaLabel:   lane + " average lane time trend for " + window.Label,
				Points:      points,
				ValueSuffix: "s",
				ColorClass:  workflowLaneTrendColor(lane),
				Class:       "h-24 border-line/70 bg-surface/60",
				Height:      120,
			},
		})
	}
	return cards
}

func workflowLaneTrendPointLabel(point telemetry.WorkflowLaneTrendPoint, window telemetry.WorkflowMetricsWindow) string {
	if point.BucketEnd.IsZero() {
		return point.Label
	}
	span := window.To.Sub(window.From)
	switch {
	case span <= 48*time.Hour:
		return localTimeToken(point.BucketEnd, LocalTimeOnly)
	case span <= 14*24*time.Hour:
		return localTimeToken(point.BucketEnd, LocalDateTime)
	default:
		return localTimeToken(point.BucketEnd, LocalDateOnly)
	}
}

func workflowLaneFlowRows(report telemetry.WorkflowMetrics) []workflowLaneFlowRow {
	window, ok := workflowChartWindow(report)
	if !ok {
		return []workflowLaneFlowRow{}
	}

	metricsByLane := make(map[string]telemetry.WorkflowPhaseMetric, len(window.Lanes))
	for _, metric := range window.Lanes {
		if lane, ok := workflowTrackedLaneName(metric.PhaseName); ok {
			metric.PhaseName = lane
			metricsByLane[lane] = metric
		}
	}

	rows := make([]workflowLaneFlowRow, 0, len(workflowTrackedLaneNames()))
	for _, lane := range workflowTrackedLaneNames() {
		metric := metricsByLane[lane]
		activeSeconds := metric.ActiveSeconds
		waitSeconds := metric.WaitSeconds
		totalSeconds := activeSeconds + waitSeconds
		if totalSeconds == 0 && metric.TotalSeconds > 0 {
			totalSeconds = metric.TotalSeconds
			waitSeconds = metric.TotalSeconds
		}
		activePercent := workflowFlowActivePercent(activeSeconds, totalSeconds)
		activePercentInt := int(math.Round(activePercent))
		waitPercentInt := 100 - activePercentInt
		if waitPercentInt < 0 {
			waitPercentInt = 0
		}
		hasData := metric.Count > 0 || totalSeconds > 0
		rows = append(rows, workflowLaneFlowRow{
			Lane:               lane,
			Window:             window.Label,
			Active:             formatDuration(float64(activeSeconds)),
			Wait:               formatDuration(float64(waitSeconds)),
			Total:              formatDuration(float64(totalSeconds)),
			ActivePercent:      strconv.Itoa(activePercentInt) + "% active",
			ActiveStyle:        percentStyle(activePercentInt),
			WaitStyle:          percentStyle(waitPercentInt),
			ActiveSegmentTitle: lane + " active: " + formatDuration(float64(activeSeconds)),
			WaitSegmentTitle:   lane + " wait: " + formatDuration(float64(waitSeconds)),
			HasData:            hasData,
		})
	}
	return rows
}

func workflowChartWindow(report telemetry.WorkflowMetrics) (telemetry.WorkflowMetricsWindow, bool) {
	for _, window := range report.Windows {
		if len(window.LaneTrends) > 0 || workflowWindowHasTrackedLane(window) {
			return window, true
		}
	}
	if len(report.Windows) == 0 {
		return telemetry.WorkflowMetricsWindow{}, false
	}
	return report.Windows[0], true
}

func workflowWindowHasTrackedLane(window telemetry.WorkflowMetricsWindow) bool {
	for _, metric := range window.Lanes {
		if _, ok := workflowTrackedLaneName(metric.PhaseName); ok {
			return true
		}
	}
	return false
}

func workflowTrackedLaneNames() []string {
	return []string{"In Progress", "Human Review", "Merging", "Rework"}
}

func workflowTrackedLaneName(phaseName string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(phaseName)) {
	case "in progress":
		return "In Progress", true
	case "human review":
		return "Human Review", true
	case "merging":
		return "Merging", true
	case "rework":
		return "Rework", true
	default:
		return "", false
	}
}

func workflowLaneTrendColor(lane string) string {
	switch lane {
	case "In Progress":
		return "text-accent"
	case "Human Review":
		return "text-warn"
	case "Merging":
		return "text-ok"
	case "Rework":
		return "text-err"
	default:
		return "text-accent"
	}
}

func workflowFlowActivePercent(activeSeconds int64, totalSeconds int64) float64 {
	if totalSeconds <= 0 {
		return 0
	}
	return float64(activeSeconds) / float64(totalSeconds) * 100
}

func workflowLaneMetricRows(report telemetry.WorkflowMetrics, project telemetry.Project) []workflowLaneMetricRow {
	rows := []workflowLaneMetricRow{}
	for _, window := range report.Windows {
		for _, metric := range window.Lanes {
			lane := workflowPhaseLabel(metric.PhaseName)
			prompt := ""
			if trackedLane, ok := workflowTrackedLaneName(metric.PhaseName); ok {
				metric.PhaseName = trackedLane
				lane = trackedLane
				prompt = workflowDiagnosticPrompt(project, report, window, metric)
			}
			rows = append(rows, workflowLaneMetricRow{
				Window:     window.Label,
				Lane:       lane,
				Count:      formatInt(metric.Count),
				Average:    formatDuration(float64(metric.AverageSeconds)),
				P50:        formatDuration(float64(metric.P50Seconds)),
				P90:        formatDuration(float64(metric.P90Seconds)),
				P95:        formatDuration(float64(metric.P95Seconds)),
				Comparison: workflowMetricComparisonLabel(metric),
				Trend:      workflowMetricTrendLabel(metric.Comparison),
				TrendClass: workflowMetricTrendClass(metric.Comparison),
				Delta:      workflowMetricDeltaLabel(metric.Comparison),
				Bottleneck: metric.Bottleneck,
				RowClass:   workflowLaneMetricRowClass(metric),
				Prompt:     prompt,
			})
		}
	}
	return rows
}

func workflowMetricComparisonLabel(metric telemetry.WorkflowPhaseMetric) string {
	if metric.Comparison == nil || strings.TrimSpace(metric.Comparison.Label) == "" {
		return "No comparison"
	}
	return strings.TrimSpace(metric.Comparison.Label)
}

func workflowMetricTrendLabel(comparison *telemetry.WorkflowMetricComparison) string {
	if comparison == nil {
		return "No prior"
	}
	switch strings.TrimSpace(comparison.Direction) {
	case "faster":
		return "Faster"
	case "slower":
		return "Slower"
	case "unchanged":
		return "Unchanged"
	default:
		return "No prior"
	}
}

func workflowMetricTrendClass(comparison *telemetry.WorkflowMetricComparison) string {
	base := "inline-flex rounded-full px-2 py-1 text-xs font-semibold "
	if comparison == nil {
		return base + "bg-elev text-sec"
	}
	switch strings.TrimSpace(comparison.Direction) {
	case "faster":
		return base + "bg-ok/15 text-ok"
	case "slower":
		return base + "bg-err/15 text-err"
	case "unchanged":
		return base + "bg-accent/15 text-accent"
	default:
		return base + "bg-elev text-sec"
	}
}

func workflowMetricDeltaLabel(comparison *telemetry.WorkflowMetricComparison) string {
	if comparison == nil || strings.TrimSpace(comparison.Direction) == "insufficient_history" {
		return "No prior"
	}
	return formatSignedDuration(comparison.DeltaSeconds)
}

func workflowLaneMetricRowClass(metric telemetry.WorkflowPhaseMetric) string {
	if metric.Bottleneck {
		return "bg-warn/15"
	}
	return ""
}

func workflowSubphaseMetricRows(report telemetry.WorkflowMetrics) []workflowSubphaseMetricRow {
	rows := []workflowSubphaseMetricRow{}
	for _, window := range report.Windows {
		for _, metric := range window.SubPhases {
			rows = append(rows, workflowSubphaseMetricRow{
				Window: window.Label,
				Phase:  workflowPhaseLabel(metric.PhaseName),
				Count:  formatInt(metric.Count),
				Total:  formatDuration(float64(metric.TotalSeconds)),
				Mean:   formatDuration(float64(metric.AverageSeconds)),
				Detail: workflowSubphaseDetail(metric),
			})
		}
	}
	return rows
}

func workflowOldestCardRows(report telemetry.WorkflowMetrics) []workflowOldestCardRow {
	rows := make([]workflowOldestCardRow, 0, len(report.OldestCards))
	for _, card := range report.OldestCards {
		rows = append(rows, workflowOldestCardRow{
			Identifier: workflowCardIdentifier(card),
			Title:      workflowCardTitle(card),
			ProjectID:  strings.TrimSpace(card.ProjectID),
			State:      workflowPhaseLabel(card.State),
			Age:        formatDuration(float64(card.AgeSeconds)),
			Key:        workflowBottleneckLabel(card.BottleneckKey),
			URL:        strings.TrimSpace(card.URL),
		})
	}
	return rows
}

func workflowBottleneck(report telemetry.WorkflowMetrics) workflowBottleneckView {
	bottleneck := report.ActiveBottleneck
	label := strings.TrimSpace(bottleneck.Label)
	if label == "" {
		label = "No active bottleneck"
	}
	detail := strings.TrimSpace(bottleneck.Detail)
	if detail == "" {
		detail = "No live queue pressure detected."
	}
	count := ""
	if bottleneck.Count > 0 {
		count = formatInt(int64(bottleneck.Count)) + " cards"
	}
	value := formatDuration(float64(bottleneck.Seconds))
	if bottleneck.Seconds <= 0 {
		value = "now"
	}
	return workflowBottleneckView{
		Label:  label,
		Detail: detail,
		Value:  value,
		Count:  count,
	}
}

func workflowBottleneckDiagnosticPrompt(project telemetry.Project, report telemetry.WorkflowMetrics) string {
	window, metric, ok := workflowBottleneckDiagnosticMetric(report)
	if !ok {
		return ""
	}
	return workflowDiagnosticPrompt(project, report, window, metric)
}

func workflowBottleneckDiagnosticMetric(report telemetry.WorkflowMetrics) (telemetry.WorkflowMetricsWindow, telemetry.WorkflowPhaseMetric, bool) {
	window, ok := workflowChartWindow(report)
	if !ok {
		return telemetry.WorkflowMetricsWindow{}, telemetry.WorkflowPhaseMetric{}, false
	}
	if lane := workflowActiveBottleneckLane(report); lane != "" {
		if metric, ok := workflowWindowLaneMetric(window, lane); ok {
			return window, metric, true
		}
		return window, telemetry.WorkflowPhaseMetric{PhaseType: "lane", PhaseName: lane}, true
	}
	for _, metric := range window.Lanes {
		if !metric.Bottleneck {
			continue
		}
		if lane, ok := workflowTrackedLaneName(metric.PhaseName); ok {
			metric.PhaseName = lane
			return window, metric, true
		}
	}
	for _, lane := range workflowTrackedLaneNames() {
		if metric, ok := workflowWindowLaneMetric(window, lane); ok {
			return window, metric, true
		}
	}
	return telemetry.WorkflowMetricsWindow{}, telemetry.WorkflowPhaseMetric{}, false
}

func workflowWindowLaneMetric(window telemetry.WorkflowMetricsWindow, lane string) (telemetry.WorkflowPhaseMetric, bool) {
	for _, metric := range window.Lanes {
		if trackedLane, ok := workflowTrackedLaneName(metric.PhaseName); ok && trackedLane == lane {
			metric.PhaseName = trackedLane
			return metric, true
		}
	}
	return telemetry.WorkflowPhaseMetric{}, false
}

func workflowActiveBottleneckLane(report telemetry.WorkflowMetrics) string {
	bottleneck := report.ActiveBottleneck
	for _, card := range report.OldestCards {
		if !workflowBottleneckMatchesCard(bottleneck, card) {
			continue
		}
		if lane, ok := workflowTrackedLaneName(card.State); ok {
			return lane
		}
	}
	if lane, ok := workflowTrackedLaneName(bottleneck.Detail); ok {
		return lane
	}
	switch strings.TrimSpace(bottleneck.Kind) {
	case "ai_active":
		return "In Progress"
	case "ci_wait", "merge_queue", "rate_limited":
		return "Merging"
	default:
		return ""
	}
}

func workflowBottleneckMatchesCard(bottleneck telemetry.WorkflowBottleneck, card telemetry.WorkflowLaneAge) bool {
	if strings.TrimSpace(bottleneck.ProjectID) != "" &&
		strings.TrimSpace(card.ProjectID) != "" &&
		!workflowNonEmptyStringEqual(bottleneck.ProjectID, card.ProjectID) {
		return false
	}
	return workflowNonEmptyStringEqual(bottleneck.IssueID, card.IssueID) ||
		workflowNonEmptyStringEqual(bottleneck.Identifier, card.Identifier)
}

func workflowNonEmptyStringEqual(a string, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	return a != "" && b != "" && a == b
}

func workflowDiagnosticPrompt(project telemetry.Project, report telemetry.WorkflowMetrics, window telemetry.WorkflowMetricsWindow, metric telemetry.WorkflowPhaseMetric) string {
	lane, ok := workflowTrackedLaneName(metric.PhaseName)
	if !ok {
		lane = workflowPhaseLabel(metric.PhaseName)
	}
	var b strings.Builder
	b.WriteString("Detent workflow lane diagnostic request\n\n")
	b.WriteString("Context\n")
	b.WriteString("- Project: " + workflowDiagnosticProjectLabel(project, metric) + "\n")
	b.WriteString("- Lane: " + lane + "\n")
	b.WriteString("- Selected window: " + workflowDiagnosticWindowLabel(window) + "\n\n")
	b.WriteString("Timing\n")
	b.WriteString("- Count: " + formatInt(metric.Count) + " lane exits\n")
	b.WriteString("- Average: " + formatDuration(float64(metric.AverageSeconds)) + "\n")
	b.WriteString("- P50: " + formatDuration(float64(metric.P50Seconds)) + "\n")
	b.WriteString("- P90: " + formatDuration(float64(metric.P90Seconds)) + "\n")
	b.WriteString("- P95: " + formatDuration(float64(metric.P95Seconds)) + "\n")
	b.WriteString("- Trend delta: " + workflowDiagnosticTrend(metric) + "\n")
	b.WriteString("- Wait vs active: " + workflowDiagnosticWaitActive(metric) + "\n\n")
	b.WriteString("Sub-phase breakdown\n")
	for _, row := range workflowDiagnosticSubphaseRows(window) {
		b.WriteString("- " + row + "\n")
	}
	b.WriteString("\nOldest/currently stuck cards in " + lane + "\n")
	for _, row := range workflowDiagnosticOldestCardRows(report, lane) {
		b.WriteString("- " + row + "\n")
	}
	b.WriteString("\nRepresentative run identifiers\n")
	for _, row := range workflowDiagnosticRepresentativeRunRows(metric) {
		b.WriteString("- " + row + "\n")
	}
	b.WriteString("\nInstruction\n")
	b.WriteString("Diagnose why this lane is slow. Examine skill and workflow selection, the sub-phase time distribution, wait-vs-active split, CI waits, GitHub backoff, and merge-queue waits. Propose concrete prioritized fixes Detent operators should make next.\n")
	return b.String()
}

func workflowDiagnosticProjectLabel(project telemetry.Project, metric telemetry.WorkflowPhaseMetric) string {
	name := strings.TrimSpace(project.DisplayName)
	id := strings.TrimSpace(project.ID)
	if id == "" {
		id = strings.TrimSpace(metric.ProjectID)
	}
	switch {
	case name != "" && id != "":
		return name + " (" + id + ")"
	case name != "":
		return name
	case id != "":
		return id
	default:
		return "unknown"
	}
}

func workflowDiagnosticWindowLabel(window telemetry.WorkflowMetricsWindow) string {
	label := strings.TrimSpace(window.Label)
	if label == "" {
		label = "unknown"
	}
	if window.From.IsZero() || window.To.IsZero() {
		return label
	}
	return label + " (" + localTimeISOString(window.From) + " to " + localTimeISOString(window.To) + ")"
}

func workflowDiagnosticTrend(metric telemetry.WorkflowPhaseMetric) string {
	if metric.Comparison == nil {
		return "No prior comparison"
	}
	trend := workflowMetricTrendLabel(metric.Comparison)
	delta := workflowMetricDeltaLabel(metric.Comparison)
	comparison := workflowMetricComparisonLabel(metric)
	if delta == "No prior" {
		return trend + " (" + comparison + ")"
	}
	return trend + " " + delta + " (" + comparison + ")"
}

func workflowDiagnosticWaitActive(metric telemetry.WorkflowPhaseMetric) string {
	activeSeconds := metric.ActiveSeconds
	waitSeconds := metric.WaitSeconds
	totalSeconds := activeSeconds + waitSeconds
	if totalSeconds == 0 && metric.TotalSeconds > 0 {
		totalSeconds = metric.TotalSeconds
		waitSeconds = metric.TotalSeconds
	}
	activePercent := int(math.Round(workflowFlowActivePercent(activeSeconds, totalSeconds)))
	return formatDuration(float64(waitSeconds)) + " wait / " +
		formatDuration(float64(activeSeconds)) + " active / " +
		formatDuration(float64(totalSeconds)) + " total (" +
		strconv.Itoa(activePercent) + "% active)"
}

func workflowDiagnosticSubphaseRows(window telemetry.WorkflowMetricsWindow) []string {
	metricsByCategory := map[string]telemetry.WorkflowPhaseMetric{}
	for _, metric := range window.SubPhases {
		category := workflowDiagnosticSubphaseCategory(metric)
		if category == "" {
			continue
		}
		current := metricsByCategory[category]
		current.PhaseName = category
		current.Count += metric.Count
		current.TotalSeconds += metric.TotalSeconds
		current.InputTokens += metric.InputTokens
		current.OutputTokens += metric.OutputTokens
		current.TotalTokens += metric.TotalTokens
		current.Turns += metric.Turns
		metricsByCategory[category] = current
	}
	categories := []string{"AI active time/tokens", "Local checks", "CI wait", "GitHub backoff", "Merge-queue wait"}
	rows := make([]string, 0, len(categories))
	for _, category := range categories {
		metric := metricsByCategory[category]
		if metric.Count == 0 && metric.TotalSeconds == 0 && metric.TotalTokens == 0 && metric.Turns == 0 {
			rows = append(rows, category+": no samples")
			continue
		}
		averageSeconds := int64(0)
		if metric.Count > 0 {
			averageSeconds = metric.TotalSeconds / metric.Count
		}
		parts := []string{
			category + ": " + formatInt(metric.Count) + " events",
			formatDuration(float64(metric.TotalSeconds)) + " total",
			formatDuration(float64(averageSeconds)) + " avg",
		}
		if metric.TotalTokens > 0 {
			parts = append(parts, formatInt(metric.TotalTokens)+" tokens")
		}
		if metric.Turns > 0 {
			parts = append(parts, formatInt(metric.Turns)+" turns")
		}
		rows = append(rows, strings.Join(parts, ", "))
	}
	return rows
}

func workflowDiagnosticSubphaseCategory(metric telemetry.WorkflowPhaseMetric) string {
	phaseType := strings.TrimSpace(metric.PhaseType)
	phaseName := strings.TrimSpace(metric.PhaseName)
	switch {
	case phaseType == "agent_session" || phaseName == "agent_active":
		return "AI active time/tokens"
	case phaseType == "local_check":
		return "Local checks"
	case phaseType == "ci" || phaseName == "ci":
		return "CI wait"
	case phaseType == "github_backoff" || phaseName == "github_backoff":
		return "GitHub backoff"
	case phaseType == "merge_queue" || phaseName == "merge_queue":
		return "Merge-queue wait"
	default:
		return ""
	}
}

func workflowDiagnosticOldestCardRows(report telemetry.WorkflowMetrics, lane string) []string {
	rows := []string{}
	for _, card := range report.OldestCards {
		cardLane, ok := workflowTrackedLaneName(card.State)
		if !ok || cardLane != lane {
			continue
		}
		parts := []string{
			workflowCardIdentifier(card),
			formatDuration(float64(card.AgeSeconds)) + " old",
		}
		if strings.TrimSpace(card.URL) != "" {
			parts = append(parts, strings.TrimSpace(card.URL))
		}
		rows = append(rows, strings.Join(parts, " / "))
	}
	if len(rows) == 0 {
		return []string{"No oldest-card samples for this lane."}
	}
	return rows
}

func workflowDiagnosticRepresentativeRunRows(metric telemetry.WorkflowPhaseMetric) []string {
	if len(metric.Representatives) == 0 {
		return []string{"No representative run identifiers recorded for this lane."}
	}
	rows := make([]string, 0, len(metric.Representatives))
	for _, representative := range metric.Representatives {
		parts := []string{}
		if representative.RunID > 0 {
			parts = append(parts, "run_id="+strconv.FormatInt(representative.RunID, 10))
		}
		if representative.SessionID > 0 {
			parts = append(parts, "session_id="+strconv.FormatInt(representative.SessionID, 10))
		}
		if strings.TrimSpace(representative.Identifier) != "" {
			parts = append(parts, "identifier="+strings.TrimSpace(representative.Identifier))
		} else if strings.TrimSpace(representative.IssueID) != "" {
			parts = append(parts, "issue_id="+strings.TrimSpace(representative.IssueID))
		}
		if strings.TrimSpace(representative.IssueURL) != "" {
			parts = append(parts, "url="+strings.TrimSpace(representative.IssueURL))
		}
		if !representative.FinishedAt.IsZero() {
			parts = append(parts, "finished_at="+localTimeISOString(representative.FinishedAt))
		}
		if len(parts) == 0 {
			parts = append(parts, "unknown representative run")
		}
		rows = append(rows, strings.Join(parts, " / "))
	}
	return rows
}

func workflowPhaseLabel(value string) string {
	switch strings.TrimSpace(value) {
	case "agent_active":
		return "AI Active"
	case "ci_wait":
		return "CI Wait"
	}
	value = strings.TrimSpace(strings.ReplaceAll(value, "_", " "))
	if value == "" {
		return "Unknown"
	}
	words := strings.Fields(value)
	for i, word := range words {
		if len(word) == 0 {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

func workflowSubphaseDetail(metric telemetry.WorkflowPhaseMetric) string {
	parts := []string{}
	if strings.TrimSpace(metric.EndpointFamily) != "" {
		parts = append(parts, metric.EndpointFamily)
	}
	if metric.Turns > 0 {
		parts = append(parts, formatInt(metric.Turns)+" turns")
	}
	if metric.TotalTokens > 0 {
		parts = append(parts, formatInt(metric.TotalTokens)+" tokens")
	}
	if len(parts) == 0 {
		return "observed"
	}
	return strings.Join(parts, " / ")
}

func workflowCardIdentifier(card telemetry.WorkflowLaneAge) string {
	if strings.TrimSpace(card.Identifier) != "" {
		return strings.TrimSpace(card.Identifier)
	}
	if strings.TrimSpace(card.IssueID) != "" {
		return strings.TrimSpace(card.IssueID)
	}
	return "unknown"
}

func workflowCardTitle(card telemetry.WorkflowLaneAge) string {
	if strings.TrimSpace(card.Title) != "" {
		return strings.TrimSpace(card.Title)
	}
	return "Untitled issue"
}

func workflowBottleneckLabel(key string) string {
	switch strings.TrimSpace(key) {
	case "ci_wait":
		return "CI wait"
	case "merge_queue":
		return "Merge queue"
	case "lane_age":
		return "Lane age"
	default:
		return "Lane age"
	}
}

func boardStateDotClass(state string) string {
	switch normalizeTimelineState(state) {
	case "todo", "rework":
		return "bg-warn"
	case "review", "done":
		return "bg-ok"
	case "blocked":
		return "bg-err"
	case "backlog":
		return "bg-dim"
	default:
		return "bg-accent"
	}
}

func boardStateTextClass(state string) string {
	switch normalizeTimelineState(state) {
	case "todo", "rework":
		return "text-warn"
	case "review", "done":
		return "text-ok"
	case "blocked":
		return "text-err"
	case "backlog":
		return "text-sec"
	default:
		return "text-accent"
	}
}

func completedModel(row telemetry.Completed) string {
	if strings.TrimSpace(row.Model) == "" {
		return "n/a"
	}
	return row.Model
}

func timeLabel(value time.Time) string {
	if value.IsZero() {
		return "n/a"
	}
	return localTimeToken(value, LocalDateTimeSeconds)
}

func agentTimelineRows(snapshot telemetry.Snapshot) []agentTimelineRow {
	entries := agentTimelineEntries(snapshot)
	if len(entries) == 0 {
		return nil
	}

	sortAgentTimelineEntries(entries)
	start, end := agentTimelineRange(entries)
	span := end.Sub(start).Seconds()
	if span <= 0 {
		span = 1
	}

	rows := make([]agentTimelineRow, 0, len(entries))
	for _, entry := range entries {
		startPercent := timelinePercent(entry.start, start, span)
		endPercent := timelinePercent(entry.end, start, span)
		width := endPercent - startPercent
		if width < 0 {
			width = 0
		}

		state := chartText(entry.state, "running")
		endLabel := timeLabel(entry.end)
		if entry.running {
			endLabel = "Live now"
		}

		identifier := issueIdentifier(entry.issue)
		title := issueTitle(entry.issue)
		segmentLabel := title
		if segmentLabel == "Untitled issue" {
			segmentLabel = identifier
		}
		segmentTitle := segmentLabel + ": " + state + " from " + timeLabel(entry.start) + " to " + endLabel

		rows = append(rows, agentTimelineRow{
			Identifier:        identifier,
			Identity:          issueIdentity(entry.issue),
			Title:             title,
			State:             state,
			IssueURL:          issueURL(entry.issue),
			PullRequestURL:    pullRequestURL(entry.issue),
			PullRequestNumber: pullRequestNumber(entry.issue),
			StartedAt:         timeLabel(entry.start),
			EndedAt:           endLabel,
			Duration:          formatDuration(entry.end.Sub(entry.start).Seconds()),
			StartPercent:      percentLabel(startPercent),
			EndPercent:        percentLabel(endPercent),
			Segments: []agentTimelineSegment{
				{
					Label: state,
					Class: agentTimelineStateClass(state),
					Style: "left: " + percentLabel(startPercent) + "; width: " + percentLabel(width) + ";",
					Title: segmentTitle,
					Width: percentLabel(width),
				},
			},
		})
	}

	return rows
}

func agentTimelineEntries(snapshot telemetry.Snapshot) []agentTimelineEntry {
	now, hasNow := agentTimelineNow(snapshot)
	entries := make([]agentTimelineEntry, 0, len(snapshot.Running)+len(snapshot.Completed))
	for _, row := range snapshot.Running {
		start, ok := agentTimelineStart(row.StartedAt, now, hasNow, row.RuntimeSeconds)
		if !ok {
			continue
		}

		end := now
		if !hasNow {
			end = start
			if row.RuntimeSeconds > 0 {
				end = start.Add(time.Duration(math.Round(row.RuntimeSeconds)) * time.Second)
			}
		}
		if end.Before(start) {
			end = start
		}

		entries = append(entries, agentTimelineEntry{
			issue:   row.Issue,
			state:   issueState(row.Issue, "Running"),
			start:   start.UTC(),
			end:     end.UTC(),
			running: true,
		})
	}

	for _, row := range snapshot.Completed {
		if row.CompletedAt.IsZero() {
			continue
		}
		end := row.CompletedAt.UTC()
		start := row.StartedAt
		if start.IsZero() && row.RuntimeSeconds > 0 {
			start = end.Add(-time.Duration(math.Round(row.RuntimeSeconds)) * time.Second)
		}
		if start.IsZero() {
			continue
		}
		if end.Before(start) {
			end = start
		}

		entries = append(entries, agentTimelineEntry{
			issue: row.Issue,
			state: completedState(row),
			start: start.UTC(),
			end:   end.UTC(),
		})
	}

	return entries
}

func agentTimelineNow(snapshot telemetry.Snapshot) (time.Time, bool) {
	if !snapshot.GeneratedAt.IsZero() {
		return snapshot.GeneratedAt.UTC(), true
	}

	var latest time.Time
	for _, row := range snapshot.Running {
		if row.LastEventAt != nil && row.LastEventAt.After(latest) {
			latest = *row.LastEventAt
		}
		if row.StartedAt.After(latest) {
			latest = row.StartedAt
		}
	}
	for _, row := range snapshot.Completed {
		if row.CompletedAt.After(latest) {
			latest = row.CompletedAt
		}
	}
	if latest.IsZero() {
		return time.Time{}, false
	}
	return latest.UTC(), true
}

func agentTimelineStart(start time.Time, now time.Time, hasNow bool, runtimeSeconds float64) (time.Time, bool) {
	if !start.IsZero() {
		return start.UTC(), true
	}
	if hasNow && runtimeSeconds > 0 {
		return now.Add(-time.Duration(math.Round(runtimeSeconds)) * time.Second).UTC(), true
	}
	return time.Time{}, false
}

func sortAgentTimelineEntries(entries []agentTimelineEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].start.Equal(entries[j].start) {
			return entries[i].start.Before(entries[j].start)
		}
		return issueIdentifier(entries[i].issue) < issueIdentifier(entries[j].issue)
	})
}

func agentTimelineRange(entries []agentTimelineEntry) (time.Time, time.Time) {
	start := entries[0].start
	end := entries[0].end
	for _, entry := range entries[1:] {
		if entry.start.Before(start) {
			start = entry.start
		}
		if entry.end.After(end) {
			end = entry.end
		}
	}
	if !end.After(start) {
		end = start.Add(time.Second)
	}
	return start, end
}

func timelinePercent(value time.Time, start time.Time, spanSeconds float64) float64 {
	if spanSeconds <= 0 {
		return 0
	}
	return clampPercent(value.Sub(start).Seconds() / spanSeconds * 100)
}

func clampPercent(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func percentLabel(value float64) string {
	return fmt.Sprintf("%.2f%%", clampPercent(value))
}

func agentTimelineStateClass(state string) string {
	switch normalizeTimelineState(state) {
	case "completed", "complete", "done", "human review":
		return "bg-ok"
	case "blocked", "failed", "failure", "cancelled", "canceled":
		return "bg-err"
	case "backlog", "queued", "queue", "retry", "retrying", "todo":
		return "bg-warn"
	default:
		return "bg-accent"
	}
}

func normalizeTimelineState(state string) string {
	return strings.ToLower(strings.TrimSpace(state))
}

func formatDiffStat(row telemetry.Running) string {
	if row.DiffStatus == "ok" {
		return "+" + formatInt(int64(row.DiffAdded)) + " -" + formatInt(int64(row.DiffRemoved)) + " (" + formatInt(int64(row.DiffFiles)) + " files)"
	}
	if row.DiffStatus != "" {
		return row.DiffStatus
	}
	return "pending"
}

func formatCount(value int) string {
	return formatInt(int64(value))
}

func formatTokens(tokens telemetry.Tokens) string {
	return formatInt(tokens.Total)
}

func formatTokenBreakdown(tokens telemetry.Tokens) string {
	parts := []string{"In " + formatInt(tokens.Input), "Out " + formatInt(tokens.Output)}
	if fraction, ok := tokens.CacheReadFraction(); ok {
		parts = append(parts, "Cache "+formatContextPercent(fraction*100))
	}
	return strings.Join(parts, " / ")
}

func contextPressureLabel(tokens telemetry.Tokens) string {
	pressure, ok := tokens.ContextPressure()
	if !ok {
		return "—"
	}
	return formatContextPercent(pressure.PercentUsed)
}

func contextPressureText(tokens telemetry.Tokens) string {
	pressure, ok := tokens.ContextPressure()
	if !ok {
		return ""
	}
	return formatContextPercent(pressure.PercentUsed) + " context"
}

func contextPressureTitle(tokens telemetry.Tokens) string {
	pressure, ok := tokens.ContextPressure()
	if !ok {
		return ""
	}
	return formatInt(pressure.TotalTokens) + " of " + formatInt(pressure.ContextLimitTokens) + " context tokens · " + string(pressure.ThresholdState)
}

func contextPressureKind(tokens telemetry.Tokens) primitives.Kind {
	pressure, ok := tokens.ContextPressure()
	if !ok {
		return primitives.KindNeutral
	}
	return contextPressureStateKind(pressure.ThresholdState)
}

func contextPressureStateKind(state telemetry.ContextPressureState) primitives.Kind {
	switch state {
	case telemetry.ContextPressureCritical:
		return primitives.KindErr
	case telemetry.ContextPressureWarning, telemetry.ContextPressureWatch:
		return primitives.KindWarn
	case telemetry.ContextPressureNormal:
		return primitives.KindOK
	default:
		return primitives.KindNeutral
	}
}

func contextPressureMeterClass(kind primitives.Kind) string {
	switch kind {
	case primitives.KindErr:
		return "h-full bg-err"
	case primitives.KindWarn:
		return "h-full bg-warn"
	case primitives.KindOK:
		return "h-full bg-ok"
	default:
		return "h-full bg-sec"
	}
}

func formatContextPercent(percent float64) string {
	if math.IsNaN(percent) || math.IsInf(percent, 0) || percent < 0 {
		percent = 0
	}
	return formatInt(int64(math.Round(percent))) + "%"
}

func formatUSD(value float64) string {
	return fmt.Sprintf("$%.2f", value)
}

func optionalUSD(value *float64) string {
	if value == nil {
		return "off"
	}
	return formatUSD(*value)
}

func budgetStatus(budget telemetry.Budget) string {
	if strings.TrimSpace(budget.DegradedReason) != "" {
		return "Budget unavailable"
	}
	if budget.Enabled {
		return "Budget enabled"
	}
	return "Budget disabled"
}

func budgetDisabled(budget telemetry.Budget) bool {
	return !budget.Enabled && strings.TrimSpace(budget.DegradedReason) == ""
}

func budgetCardClass(budget telemetry.Budget) string {
	if budgetDisabled(budget) {
		return "dashboard-panel min-w-0 rounded-md border border-line bg-surface px-4 py-3 shadow-sm sm:px-5"
	}
	return "dashboard-panel min-w-0 rounded-md border border-line bg-surface p-4 shadow-sm sm:p-5"
}

func budgetRefusalLabel(refusal telemetry.BudgetRefusal) string {
	if label := strings.TrimSpace(refusal.Identifier); label != "" {
		return label
	}
	return strings.TrimSpace(refusal.IssueID)
}

func budgetRefusalDisposition(refusal telemetry.BudgetRefusal) string {
	if refusal.HardHold {
		return "Hard hold"
	}
	return "Temporary cooldown"
}

func budgetRefusalBadgeClass(refusal telemetry.BudgetRefusal) string {
	if refusal.HardHold {
		return "shrink-0 rounded-full bg-danger/15 px-2 py-1 text-xs font-medium text-danger"
	}
	return "shrink-0 rounded-full bg-warn/15 px-2 py-1 text-xs font-medium text-warn"
}

func budgetRefusalDetail(refusal telemetry.BudgetRefusal) string {
	if refusal.HardHold {
		return "Needs an operator decision; it will not retry automatically."
	}
	if refusal.ResetAt != nil {
		return "Retries automatically after " + localTimeToken(*refusal.ResetAt, LocalDateTimeSeconds) + "."
	}
	return "Retries automatically after its cooldown."
}

func budgetSpendTodayLabel(budget telemetry.Budget) string {
	if strings.TrimSpace(budget.DegradedReason) != "" && budget.CurrentSpendUSD <= 0 && len(budget.SpendPoints) == 0 {
		return "unavailable / " + budgetDailyCapLabel(budget)
	}
	return formatUSD(budget.CurrentSpendUSD) + " / " + budgetDailyCapLabel(budget)
}

func budgetDailyCapLabel(budget telemetry.Budget) string {
	if !budget.Enabled {
		return "off"
	}
	return optionalUSD(budget.PerDayMaxUSD)
}

func budgetDailyUsageStyle(budget telemetry.Budget) string {
	if budget.PerDayMaxUSD == nil || *budget.PerDayMaxUSD <= 0 {
		return percentStyle(0)
	}
	return percentStyle(int(math.Round(budget.CurrentSpendUSD / *budget.PerDayMaxUSD * 100)))
}

func budgetBurnDownView(snapshot telemetry.Snapshot) budgetBurnDownViewModel {
	budget := snapshot.Budget
	if strings.TrimSpace(budget.DegradedReason) != "" {
		return budgetBurnDownViewModel{
			EmptyTitle:      "Budget data unavailable.",
			EmptyDetail:     budgetUnavailableDetail(budget),
			CurrentLabel:    formatUSD(budget.CurrentSpendUSD),
			CapLabel:        optionalUSD(budget.PerDayMaxUSD),
			ProjectionLabel: budgetProjectionLabel(budget),
		}
	}
	if !budget.Enabled {
		return budgetBurnDownViewModel{
			EmptyTitle:      "Budget disabled.",
			EmptyDetail:     "Enable a daily budget cap to show notional USD burn-down.",
			CurrentLabel:    formatUSD(budget.CurrentSpendUSD),
			CapLabel:        optionalUSD(budget.PerDayMaxUSD),
			ProjectionLabel: formatUSD(budget.ProjectedCostUSD),
		}
	}

	now := snapshot.GeneratedAt.UTC()
	if now.IsZero() {
		now = latestBudgetPointAt(budget.SpendPoints)
	}
	if now.IsZero() {
		now = time.Now().UTC().Truncate(time.Second)
	}

	periodStart, periodEnd := budgetPeriod(budget, now)
	currentSpend := budgetCurrentSpendUSD(budget)
	projectedSpend := budget.ProjectedSpendUSD
	if projectedSpend <= 0 {
		projectedSpend = budgetProjectedSpendUSD(periodStart, periodEnd, now, currentSpend)
	}

	actualPoints := budgetActualPoints(budget.SpendPoints, periodStart, periodEnd, now, currentSpend)
	if currentSpend <= 0 && len(actualPoints) <= 1 {
		return budgetBurnDownViewModel{
			EmptyTitle:      "No notional USD yet.",
			EmptyDetail:     "Cumulative notional USD and its projection will appear after usage is recorded.",
			CurrentLabel:    formatUSD(currentSpend),
			CapLabel:        optionalUSD(budget.PerDayMaxUSD),
			ProjectionLabel: formatUSD(projectedSpend),
		}
	}

	lastActual := actualPoints[len(actualPoints)-1]
	return budgetBurnDownViewModel{
		Available:       true,
		PeriodLabel:     budgetPeriodLabel(periodStart, periodEnd),
		CurrentLabel:    formatUSD(currentSpend),
		CapLabel:        optionalUSD(budget.PerDayMaxUSD),
		ProjectionLabel: formatUSD(projectedSpend),
		Chart: BudgetProjectionChartData{
			Title:        "Notional USD burn-down",
			AriaLabel:    "Cumulative notional USD burn-down with budget cap and projected period-end value",
			ActualPoints: actualPoints,
			ProjectionPoints: []BudgetProjectionPoint{
				{
					Label: "Current notional USD",
					At:    lastActual.At,
					Value: lastActual.Value,
				},
				{
					Label: "Projected period end",
					At:    periodEnd,
					Value: projectedSpend,
				},
			},
			PeriodStart: periodStart,
			PeriodEnd:   periodEnd,
			Cap:         budgetCapValue(budget.PerDayMaxUSD),
		},
	}
}

func budgetUnavailableDetail(budget telemetry.Budget) string {
	if reason := strings.TrimSpace(budget.DegradedReason); reason != "" {
		return reason
	}
	return "Budget notional USD data unavailable."
}

func budgetProjectionLabel(budget telemetry.Budget) string {
	if budget.ProjectedCostUSD > 0 {
		return formatUSD(budget.ProjectedCostUSD)
	}
	if budget.ProjectedSpendUSD > 0 {
		return formatUSD(budget.ProjectedSpendUSD)
	}
	return "unavailable"
}

func budgetProjectedSpendUSD(periodStart time.Time, periodEnd time.Time, now time.Time, currentSpend float64) float64 {
	if currentSpend <= 0 {
		return 0
	}
	if periodStart.IsZero() || !periodEnd.After(periodStart) {
		return currentSpend
	}
	elapsed := now.Sub(periodStart).Seconds()
	if elapsed <= 0 {
		return currentSpend
	}
	total := periodEnd.Sub(periodStart).Seconds()
	if total <= 0 {
		return currentSpend
	}
	projected := currentSpend * total / elapsed
	if projected < currentSpend {
		return currentSpend
	}
	return projected
}

func budgetPeriod(budget telemetry.Budget, now time.Time) (time.Time, time.Time) {
	start := budget.PeriodStart.UTC()
	end := budget.PeriodEnd.UTC()
	if !start.IsZero() && end.After(start) {
		return start, end
	}
	year, month, day := now.UTC().Date()
	start = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 0, 1)
}

func budgetCurrentSpendUSD(budget telemetry.Budget) float64 {
	current := budget.CurrentSpendUSD
	for _, point := range budget.SpendPoints {
		if point.SpendUSD > current {
			current = point.SpendUSD
		}
	}
	if current < 0 {
		return 0
	}
	return current
}

func budgetActualPoints(points []telemetry.BudgetSpendPoint, periodStart time.Time, periodEnd time.Time, now time.Time, currentSpend float64) []BudgetProjectionPoint {
	filtered := make([]telemetry.BudgetSpendPoint, 0, len(points))
	for _, point := range points {
		at := point.At.UTC()
		if at.IsZero() || at.Before(periodStart) || !at.Before(periodEnd) {
			continue
		}
		if point.SpendUSD < 0 {
			continue
		}
		filtered = append(filtered, telemetry.BudgetSpendPoint{At: at, SpendUSD: point.SpendUSD})
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].At.Before(filtered[j].At)
	})

	out := []BudgetProjectionPoint{
		{
			Label: "Period start",
			At:    periodStart,
			Value: 0,
		},
	}
	lastSpend := 0.0
	for _, point := range filtered {
		spend := point.SpendUSD
		if spend < lastSpend {
			spend = lastSpend
		}
		lastSpend = spend
		out = append(out, BudgetProjectionPoint{
			Label: budgetPointLabel(point.At),
			At:    point.At,
			Value: spend,
		})
	}

	if currentSpend < lastSpend {
		currentSpend = lastSpend
	}
	if currentSpend > lastSpend {
		at := now.UTC()
		if at.Before(periodStart) {
			at = periodStart
		}
		if !at.Before(periodEnd) {
			at = periodEnd
		}
		out = append(out, BudgetProjectionPoint{
			Label: "Current notional USD",
			At:    at,
			Value: currentSpend,
		})
	}
	return out
}

func latestBudgetPointAt(points []telemetry.BudgetSpendPoint) time.Time {
	var latest time.Time
	for _, point := range points {
		at := point.At.UTC()
		if at.After(latest) {
			latest = at
		}
	}
	return latest
}

func budgetPeriodLabel(periodStart time.Time, periodEnd time.Time) string {
	return localTimeToken(periodStart, LocalDateTime) + " - " + localTimeToken(periodEnd, LocalDateTime)
}

func budgetPointLabel(at time.Time) string {
	if at.IsZero() {
		return "Notional USD"
	}
	at = at.UTC()
	if at.Second() == 0 {
		return localTimeToken(at, LocalTimeOnly)
	}
	return localTimeToken(at, LocalTimeWithSeconds)
}

func budgetCapValue(cap *float64) float64 {
	if cap == nil || *cap <= 0 {
		return 0
	}
	return *cap
}

func budgetHistoryBars(budget telemetry.Budget) []budgetHistoryBar {
	days := budgetHistoryDays(budget.Days)
	if len(days) == 0 {
		return nil
	}

	maxSpend := 0.0
	for _, day := range days {
		if day.SpendUSD > maxSpend {
			maxSpend = day.SpendUSD
		}
	}

	bars := make([]budgetHistoryBar, 0, len(days))
	for _, day := range days {
		bars = append(bars, budgetHistoryBar{
			Style: budgetHistoryHeightStyle(day.SpendUSD, maxSpend),
			Title: budgetDayLabel(day) + ": " + formatUSD(day.SpendUSD),
		})
	}
	return bars
}

func budgetHistoryDays(days []telemetry.BudgetDay) []telemetry.BudgetDay {
	const maxBudgetHistoryDays = 7
	if len(days) <= maxBudgetHistoryDays {
		return days
	}
	return days[len(days)-maxBudgetHistoryDays:]
}

func budgetHistoryCount(budget telemetry.Budget) string {
	count := len(budgetHistoryDays(budget.Days))
	switch count {
	case 0:
		return "No history"
	case 1:
		return "1 day"
	default:
		return formatInt(int64(count)) + " days"
	}
}

func budgetHistoryHeightStyle(spend float64, maxSpend float64) string {
	percent := 12
	if spend > 0 && maxSpend > 0 {
		percent = max(int(math.Round(spend/maxSpend*100)), 12)
	}
	if percent > 100 {
		percent = 100
	}
	return fmt.Sprintf("height: %d%%;", percent)
}

func budgetDayLabel(day telemetry.BudgetDay) string {
	date := strings.TrimSpace(day.Date)
	if date == "" {
		return "n/a"
	}
	return date
}

func runtimeStatusLabel(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		return "Degraded"
	}
	if snapshotInitializing(snapshot) {
		return "Starting"
	}
	if snapshot.Shutdown.Draining {
		return "Draining"
	}
	return "Live"
}

func runtimeStatusClass(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		return "border-err/15 bg-err/15 text-err"
	}
	if snapshotInitializing(snapshot) {
		return "border-line bg-elev text-sec"
	}
	if snapshot.Shutdown.Draining {
		return "border-warn/15 bg-warn/15 text-warn"
	}
	return "border-ok/15 bg-ok/15 text-ok"
}

func statsStatusLabel(snapshot telemetry.Snapshot) string {
	if !snapshotReady(snapshot) {
		return "Stats pending"
	}
	if snapshot.LifetimeTotals.Available {
		return "Stats healthy"
	}
	return "Stats degraded"
}

func statsStatusClass(snapshot telemetry.Snapshot) string {
	if snapshotDegraded(snapshot) {
		return "border-err/15 bg-err/15 text-err"
	}
	if snapshotInitializing(snapshot) {
		return "border-line bg-elev text-sec"
	}
	if snapshot.LifetimeTotals.Available {
		return "border-ok/15 bg-ok/15 text-ok"
	}
	return "border-err/15 bg-err/15 text-err"
}

func statsStatusTitle(snapshot telemetry.Snapshot) string {
	if !snapshotReady(snapshot) {
		return snapshotReadinessDetail(snapshot)
	}
	if snapshot.LifetimeTotals.Available {
		return "Runtime statistics are available."
	}
	return lifetimeDegradedReason(snapshot.LifetimeTotals)
}

func instanceLabel(snapshot telemetry.Snapshot) string {
	name := strings.TrimSpace(snapshot.Instance.Name)
	login := strings.TrimSpace(snapshot.Instance.GitHubLogin)
	switch {
	case name != "" && login != "":
		return name + " (" + login + ")"
	case name != "":
		return name
	case login != "":
		return login
	default:
		return "not configured"
	}
}

func authorizationScopeLabel(snapshot telemetry.Snapshot) string {
	scope := strings.TrimSpace(snapshot.Instance.AuthorizationScope)
	if scope != "" {
		return scope
	}
	return "All issues"
}

func authorizationScopeClass(snapshot telemetry.Snapshot) string {
	if snapshot.Instance.AuthorizationConfigured {
		return "border-accent/15 bg-accent/15 text-accent"
	}
	return "border-line bg-elev text-sec"
}

func rateLimitRows(limits *telemetry.RateLimits) []rateLimitRow {
	if limits == nil {
		return nil
	}

	rows := make([]rateLimitRow, 0, 4)
	appendBucket := func(name string, bucket *telemetry.RateLimitBucket, providerWindow bool) {
		if bucket == nil {
			return
		}
		row := rateLimitRow{
			Name:        name,
			Remaining:   rateLimitRemainingLabel(bucket),
			Used:        rateLimitUsedLabel(bucket),
			Limit:       rateLimitLimitLabel(bucket),
			Reset:       resetLabel(bucket),
			UsedPercent: usedPercent(bucket),
		}
		if providerWindow {
			row.Remaining = formatInt(int64(100-row.UsedPercent)) + "% left"
			row.Used = formatInt(int64(row.UsedPercent)) + "% used"
			row.Limit = "rolling window"
		}
		rows = append(rows, row)
	}

	appendBucket("Primary", limits.Primary, true)
	appendBucket("Secondary", limits.Secondary, true)
	appendBucket("GitHub GraphQL", limits.GitHubGraphQL, false)
	appendBucket("GitHub REST", limits.GitHubREST, false)
	if limits.Credits != nil {
		rows = append(rows, creditRateLimitRow(limits.Credits))
	}
	return rows
}

func rateLimitRemainingLabel(bucket *telemetry.RateLimitBucket) string {
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return "unknown"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "exhausted"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusBackoff && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "backoff"
	}
	return formatInt(bucket.Remaining) + " left"
}

func rateLimitUsedLabel(bucket *telemetry.RateLimitBucket) string {
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return "usage unknown"
	}
	label := formatInt(bucket.Used) + " used"
	if bucket.Cost > 0 {
		label += " / cost " + formatInt(bucket.Cost)
	}
	return label
}

func rateLimitLimitLabel(bucket *telemetry.RateLimitBucket) string {
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return "limit unknown"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted && bucket.Limit <= 0 {
		return "limit unknown"
	}
	return formatLimit(bucket.Limit) + " limit"
}

func creditRateLimitRow(bucket *telemetry.RateLimitBucket) rateLimitRow {
	row := rateLimitRow{
		Name:        "Credits",
		Remaining:   formatInt(bucket.Remaining) + " left",
		Used:        formatInt(bucket.Used) + " used",
		Limit:       formatLimit(bucket.Limit) + " limit",
		Reset:       resetLabel(bucket),
		UsedPercent: usedPercent(bucket),
	}

	switch {
	case bucket.Unlimited:
		row.Remaining = "unlimited credits"
		row.Used = "available"
		row.Limit = "n/a limit"
		row.UsedPercent = 0
	case bucket.HasCredits && strings.TrimSpace(bucket.Balance) != "":
		row.Remaining = strings.TrimSpace(bucket.Balance) + " credits"
		row.Used = "available"
		row.Limit = "n/a limit"
		row.UsedPercent = 0
	case bucket.HasCredits:
		row.Remaining = "available credits"
		row.Used = "available"
		row.Limit = "n/a limit"
		row.UsedPercent = 0
	case bucket.Limit == 0 && bucket.Remaining == 0 && bucket.Used == 0:
		row.Remaining = "no credits"
		row.Used = "unavailable"
		row.Limit = "n/a limit"
		row.UsedPercent = 0
	}

	return row
}

func rateLimitName(limits *telemetry.RateLimits) string {
	if limits == nil || limits.LimitName == "" {
		if limits != nil && limits.GitHubGraphQL != nil {
			return "GitHub GraphQL"
		}
		if limits != nil && limits.GitHubREST != nil {
			return "GitHub REST"
		}
		return "Latest snapshot"
	}
	return limits.LimitName
}

func backendCapacityOutageTitle(outage telemetry.BackendOutage) string {
	if strings.TrimSpace(outage.Kind) == "github_rest_rate_limit" {
		return "GitHub REST dispatch paused"
	}
	if strings.EqualFold(strings.TrimSpace(outage.Reason), "subscription window exhausted") {
		return "Backend " + backendCapacityBackendID(outage) + ": subscription window exhausted"
	}
	return "Backend " + backendCapacityBackendID(outage) + " at usage limit"
}

func failureBreakerDetailParts(breaker telemetry.FailureBreaker, now time.Time) (string, time.Time, bool) {
	attemptCount := breaker.AttemptCount
	if attemptCount == 0 {
		attemptCount = breaker.Count
	}
	detail := boardCountLabel(attemptCount, "failed attempt", "failed attempts")
	itemCount := breaker.DistinctItemCount
	if itemCount == 0 && len(breaker.Items) > 0 {
		itemCount = len(breaker.Items)
	}
	if itemCount > 0 {
		detail += " across " + boardCountLabel(itemCount, "item", "items")
	}
	detail += " in " + formatDurationWindow(time.Duration(breaker.WindowSeconds)*time.Second) + "."
	if projectID := strings.TrimSpace(breaker.ProjectID); projectID != "" {
		detail += " Pause scope: project " + projectID + " only."
	}
	if backend := failureBreakerBackendLabel(breaker); backend != "" {
		detail += " Backend: " + backend + "."
	}
	if cause := failureBreakerCauseLabel(breaker); cause != "" {
		detail += " Cause: " + cause + "."
	}
	if message := strings.TrimSpace(breaker.RepresentativeError); message != "" && !strings.EqualFold(message, strings.TrimSpace(breaker.Cause)) {
		detail += " Representative error: " + message + "."
	}
	if class := strings.TrimSpace(breaker.Class); class != "" {
		detail += " Diagnostic class: " + class + "."
	}
	if outage := breaker.BackendOutage; outage != nil {
		detail += " A backend capacity pause is also active"
		if outage.ResetAt != nil && !outage.ResetAt.IsZero() {
			detail += "; provider reset " + localTimeToken(*outage.ResetAt, LocalDateTimeZone)
			if now.Before(*outage.ResetAt) {
				detail += " (in " + formatDuration(outage.ResetAt.Sub(now).Seconds()) + ")"
			}
		}
		detail += "."
	}
	parkedCopy := failureBreakerParkedCopy(breaker)
	if strings.TrimSpace(breaker.CanaryIssueID) != "" {
		return detail + " The project is dispatching one canary candidate." + parkedCopy, time.Time{}, false
	}
	if !breaker.ResumeAt.IsZero() && breaker.ResumeAt.After(now) {
		copy := detail + " The project may dispatch one eligible candidate after the project cooldown at"
		if breaker.EligibleCandidateCount != nil && *breaker.EligibleCandidateCount == 0 {
			copy += "; no candidate is currently eligible"
		}
		return copy + parkedCopy, breaker.ResumeAt, true
	}
	if breaker.EligibleCandidateCount != nil && *breaker.EligibleCandidateCount == 0 {
		return detail + " Project canary dispatch is ready, but no eligible candidate is currently available." + parkedCopy, time.Time{}, false
	}
	return detail + " The project may dispatch one eligible candidate now." + parkedCopy, time.Time{}, false
}

func failureBreakerCauseLabel(breaker telemetry.FailureBreaker) string {
	if cause := strings.TrimSpace(breaker.Cause); cause != "" {
		return failureBreakerSentenceCase(cause)
	}
	class := strings.TrimSpace(breaker.Class)
	prefix, suffix, _ := strings.Cut(class, ":")
	switch prefix {
	case "session_token_ceiling":
		return "Agent session token ceiling reached"
	case "deliverable_command_failure":
		if suffix != "" {
			return "Deliverable command failed: " + strings.ReplaceAll(suffix, "_", " ")
		}
		return "Deliverable command failed"
	case "no_progress":
		return "Agent made no work-product progress"
	case "runner_final_state":
		return "Runner ended in a failed state"
	case "backend_error":
		return "Agent backend returned an error"
	case "runner_error":
		return "Runner failed"
	}
	return failureBreakerSentenceCase(strings.ReplaceAll(class, "_", " "))
}

func failureBreakerSentenceCase(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

func failureBreakerBackendLabel(breaker telemetry.FailureBreaker) string {
	parts := make([]string, 0, 3)
	if value := strings.TrimSpace(breaker.BackendID); value != "" {
		parts = append(parts, value)
	}
	if value := strings.TrimSpace(breaker.BackendKind); value != "" && !strings.EqualFold(value, strings.TrimSpace(breaker.BackendID)) {
		parts = append(parts, "kind "+value)
	}
	if value := strings.TrimSpace(breaker.Provider); value != "" {
		parts = append(parts, "provider "+value)
	}
	return strings.Join(parts, " · ")
}

func failureBreakerParkedCopy(breaker telemetry.FailureBreaker) string {
	parked := 0
	for _, item := range breaker.Items {
		if item.Parked || strings.EqualFold(strings.TrimSpace(item.CurrentState), "Blocked") {
			parked++
		}
	}
	if parked == 0 {
		return ""
	}
	if parked == 1 {
		return " The affected Blocked item will not retry merely because the project canary is eligible; it needs recovery or operator action."
	}
	return " The " + formatCount(parked) + " affected Blocked items will not retry merely because the project canary is eligible; they need recovery or operator action."
}

func failureBreakerItemLabel(item telemetry.FailureBreakerItem) string {
	title := strings.TrimSpace(item.Title)
	identifier := strings.TrimSpace(item.Identifier)
	switch {
	case title != "" && identifier != "":
		return title + " — " + identifier
	case title != "":
		return title
	case identifier != "":
		return identifier
	default:
		return strings.TrimSpace(item.IssueID)
	}
}

func dispatchRecoveryTitle(recovery telemetry.DispatchRecovery) string {
	if strings.TrimSpace(recovery.Status) == "waiting" {
		return "Dispatch waiting on " + dispatchRecoveryKindLabel(recovery.Kind)
	}
	return "Dispatch recovery ramp active"
}

func dispatchRecoveryDetailParts(recovery telemetry.DispatchRecovery, now time.Time) (string, time.Time, bool) {
	kind := dispatchRecoveryKindLabel(recovery.Kind)
	if strings.TrimSpace(recovery.Status) == "waiting" {
		detail := kind + " is delaying dispatch."
		if reason := strings.TrimSpace(recovery.Reason); reason != "" {
			detail = kind + " is delaying dispatch: " + reason + "."
		}
		if !recovery.ResumeAt.IsZero() && recovery.ResumeAt.After(now) {
			return detail + " Automatic retry scheduled", recovery.ResumeAt, true
		}
		return detail, time.Time{}, false
	}
	limit := max(recovery.Limit, 1)
	configured := max(recovery.MaxConcurrent, limit)
	detail := kind + " cleared; recovery is admitting up to " + formatCount(limit) + " of " + formatCount(configured) + " configured workers after successful startup progress."
	if !recovery.ResumeAt.IsZero() && recovery.ResumeAt.After(now) {
		return detail + " Next canary becomes eligible", recovery.ResumeAt, true
	}
	return detail, time.Time{}, false
}

func dispatchRecoveryKindLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "tracker_unavailable":
		return "tracker availability"
	case "forge_unavailable":
		return "forge write availability"
	case "github_rest":
		return "GitHub REST capacity"
	case "pull_request_hydration":
		return "pull-request hydration"
	case "backend_capacity":
		return "backend capacity"
	case "project_failure_breaker":
		return "project failure breaker"
	default:
		return "dispatch dependency"
	}
}

type boardBannerSummary struct {
	ID    string
	Title string
}

func boardFailureBreakerSummary(breakers []telemetry.FailureBreaker) (boardBannerSummary, bool) {
	if len(breakers) == 0 {
		return boardBannerSummary{}, false
	}
	count := boardAffectedProjectCount(len(breakers), func(yield func(string)) {
		for _, breaker := range breakers {
			yield(breaker.ProjectID)
		}
	})
	return boardBannerSummary{
		ID:    "board-failure-breaker-summary",
		Title: boardBannerProjectTitle("Project failure breaker active", count),
	}, true
}

func boardDispatchRecoverySummaries(recoveries []telemetry.DispatchRecovery, now time.Time) []boardBannerSummary {
	return dispatchRecoverySummaries(recoveries, func(recovery telemetry.DispatchRecovery) (string, bool) {
		return boardDispatchRecoveryAlertTitle(recovery, now)
	})
}

func dispatchRecoverySummaries(recoveries []telemetry.DispatchRecovery, selectTitle func(telemetry.DispatchRecovery) (string, bool)) []boardBannerSummary {
	type group struct {
		title    string
		projects map[string]struct{}
		missing  int
	}
	groups := make(map[string]*group)
	order := make([]string, 0)
	for _, recovery := range recoveries {
		title, selected := selectTitle(recovery)
		if !selected {
			continue
		}
		key := strings.TrimSpace(recovery.Kind) + "\x00" + title
		if _, ok := groups[key]; !ok {
			groups[key] = &group{
				title:    title,
				projects: make(map[string]struct{}),
			}
			order = append(order, key)
		}
		if projectID := strings.TrimSpace(recovery.ProjectID); projectID != "" {
			groups[key].projects[projectID] = struct{}{}
		} else {
			groups[key].missing++
		}
	}
	summaries := make([]boardBannerSummary, 0, len(order))
	for _, key := range order {
		group, ok := groups[key]
		if !ok {
			continue
		}
		count := len(group.projects) + group.missing
		summaries = append(summaries, boardBannerSummary{
			ID:    "board-dispatch-recovery-summary-" + boardCardSlug(key),
			Title: boardBannerProjectTitle(group.title, count),
		})
	}
	return summaries
}

func boardBackendCapacitySummaries(outages []telemetry.BackendOutage, now time.Time) []boardBannerSummary {
	type group struct {
		title    string
		projects map[string]struct{}
		missing  int
	}
	groups := make(map[string]*group)
	order := make([]string, 0)
	for _, outage := range backendCapacityOutageDetails(outages) {
		title, selected := boardBackendCapacityTitle(outage, now)
		if !selected {
			continue
		}
		if _, ok := groups[title]; !ok {
			groups[title] = &group{title: title, projects: make(map[string]struct{})}
			order = append(order, title)
		}
		if projectID := strings.TrimSpace(outage.ProjectID); projectID != "" {
			groups[title].projects[projectID] = struct{}{}
		} else {
			groups[title].missing++
		}
	}
	summaries := make([]boardBannerSummary, 0, len(order))
	for _, key := range order {
		group, ok := groups[key]
		if !ok {
			continue
		}
		count := len(group.projects) + group.missing
		summaries = append(summaries, boardBannerSummary{
			ID:    "board-backend-capacity-summary-" + boardCardSlug(key),
			Title: boardBannerProjectTitle(group.title, count),
		})
	}
	return summaries
}

func boardBackendCapacityTitle(outage telemetry.BackendOutage, now time.Time) (string, bool) {
	if strings.TrimSpace(outage.ProbeIssueID) != "" {
		return "", false
	}
	backend := backendCapacityBackendID(outage)
	if outage.ProbeAttempts >= boardRecoveryFailureAttempts {
		return "Backend " + backend + " recovery failed repeatedly", true
	}
	resumeAt := outage.ResumeAt
	if outage.NextProbeAt != nil && !outage.NextProbeAt.IsZero() {
		resumeAt = *outage.NextProbeAt
	}
	if automaticRecoveryPending(resumeAt, now) {
		return "", false
	}
	if automaticRecoveryOverdue(resumeAt, now) {
		return "Backend " + backend + " recovery overdue", true
	}
	return backendCapacityOutageTitle(outage), true
}

func automaticRecoveryPending(resumeAt time.Time, now time.Time) bool {
	return !resumeAt.IsZero() && !now.IsZero() && now.Before(resumeAt.Add(boardRecoveryOverdueGrace))
}

func automaticRecoveryOverdue(resumeAt time.Time, now time.Time) bool {
	return !resumeAt.IsZero() && !now.IsZero() && !now.Before(resumeAt.Add(boardRecoveryOverdueGrace))
}

func boardAffectedProjectCount(entries int, projectIDs func(func(string))) int {
	projects := make(map[string]struct{}, entries)
	missing := 0
	projectIDs(func(projectID string) {
		projectID = strings.TrimSpace(projectID)
		if projectID == "" {
			missing++
			return
		}
		projects[projectID] = struct{}{}
	})
	return len(projects) + missing
}

func boardBannerProjectTitle(title string, count int) string {
	return title + " — " + boardCountLabel(count, "project", "projects")
}

func backendCapacityBackendID(outage telemetry.BackendOutage) string {
	backend := strings.TrimSpace(outage.BackendID)
	if backend == "" {
		backend = strings.TrimSpace(outage.BackendKind)
	}
	if backend == "" {
		backend = "agent backend"
	}
	return backend
}

func backendCapacityOutages(outages []telemetry.BackendOutage) []telemetry.BackendOutage {
	return uniqueBackendCapacityOutages(outages, false)
}

func backendCapacityOutageDetails(outages []telemetry.BackendOutage) []telemetry.BackendOutage {
	return uniqueBackendCapacityOutages(outages, true)
}

func uniqueBackendCapacityOutages(outages []telemetry.BackendOutage, byProject bool) []telemetry.BackendOutage {
	unique := make([]telemetry.BackendOutage, 0, len(outages))
	seen := make(map[string]struct{}, len(outages))
	for _, outage := range outages {
		resetAt := time.Time{}
		if outage.ResetAt != nil {
			resetAt = *outage.ResetAt
		}
		if resetAt.IsZero() {
			resetAt = outage.ResumeAt
		}
		key := backendCapacityBackendID(outage) + "\x00" + localTimeISOString(resetAt)
		if byProject {
			key = strings.TrimSpace(outage.ProjectID) + "\x00" + key
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, outage)
	}
	return unique
}

func backendCapacityOutageDetailParts(outage telemetry.BackendOutage, now time.Time) (string, time.Time, bool) {
	if strings.TrimSpace(outage.Kind) == "github_rest_rate_limit" {
		detail := strings.TrimSpace(outage.Reason)
		if detail == "" {
			detail = "The shared tracker account is at its REST dispatch floor"
		}
		if !outage.ResumeAt.IsZero() && outage.ResumeAt.After(now) {
			return detail + "; resuming at", outage.ResumeAt, true
		}
		return detail + "; dispatch can resume now", time.Time{}, false
	}
	provider := strings.TrimSpace(outage.Provider)
	detail := "Dispatch is paused"
	if reason := strings.TrimSpace(outage.Reason); reason != "" && !strings.EqualFold(reason, "provider usage limit reached") {
		detail = reason + ". " + detail
	}
	if provider != "" {
		detail += " for " + provider
	}
	if strings.TrimSpace(outage.ProbeIssueID) != "" {
		return detail + "; capacity probe in progress", time.Time{}, false
	}
	if outage.NextProbeAt != nil {
		if outage.NextProbeAt.After(now) {
			return detail + "; next canary at", *outage.NextProbeAt, true
		}
		return detail + "; capacity canary due now", time.Time{}, false
	}
	return detail + "; waiting for a low-frequency capacity probe", time.Time{}, false
}

func backendCapacityProbeResult(outage telemetry.BackendOutage) string {
	result := strings.ReplaceAll(strings.TrimSpace(outage.LastProbeResult), "_", " ")
	if result == "" {
		return "not yet probed"
	}
	return result
}

func backendCapacityHealthVerdict(snapshot telemetry.Snapshot) string {
	outages := backendCapacityOutages(snapshot.BackendOutages)
	if len(outages) == 0 {
		return ""
	}
	if len(outages) == 1 {
		return backendCapacityOutageTitle(outages[0]) + "."
	}
	return formatCount(len(outages)) + " agent backends are at usage limits."
}

func hasGraphQLBudget(limits *telemetry.RateLimits) bool {
	return limits != nil && (limits.GitHubGraphQL != nil || limits.GraphQLCost != nil)
}

func hasRESTBudget(limits *telemetry.RateLimits) bool {
	return limits != nil && (limits.GitHubREST != nil || len(limits.GitHubRESTBudgets) > 0 || limits.RESTUsage != nil)
}

func graphQLBudgetRemaining(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GitHubGraphQL == nil {
		return "n/a"
	}
	bucket := limits.GitHubGraphQL
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return "unknown"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "exhausted"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusBackoff && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "backoff"
	}
	if bucket.Limit > 0 {
		return formatInt(bucket.Remaining) + " / " + formatInt(bucket.Limit)
	}
	return formatInt(bucket.Remaining) + " left"
}

func graphQLBudgetReset(limits *telemetry.RateLimits, now time.Time) string {
	if limits == nil || limits.GitHubGraphQL == nil {
		return "n/a"
	}
	bucket := limits.GitHubGraphQL
	if bucket.ResetInSeconds > 0 {
		return formatDuration(float64(bucket.ResetInSeconds)) + " to reset"
	}
	if bucket.ResetAt != nil {
		if !now.IsZero() && bucket.ResetAt.After(now) {
			return formatDuration(bucket.ResetAt.Sub(now).Seconds()) + " to reset"
		}
		return localTimeToken(*bucket.ResetAt, LocalTimeOnly)
	}
	return "n/a"
}

func graphQLBudgetResetAt(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GitHubGraphQL == nil || limits.GitHubGraphQL.ResetAt == nil {
		return "reset time n/a"
	}
	return "resets " + localTimeToken(*limits.GitHubGraphQL.ResetAt, LocalTimeOnly)
}

func graphQLBudgetCycleCost(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GraphQLCost == nil {
		return "0 points"
	}
	return formatInt(limits.GraphQLCost.TotalCost) + " points"
}

func graphQLBudgetQueryCount(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GraphQLCost == nil {
		return "0 queries"
	}
	return formatInt(limits.GraphQLCost.TotalQueries) + " " + pluralize("query", limits.GraphQLCost.TotalQueries)
}

func graphQLBudgetLastHourQueryCount(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GraphQLCost == nil {
		return "0 queries"
	}
	count := limits.GraphQLCost.LastHourQueries
	return formatInt(count) + " " + pluralize("query", count)
}

func graphQLBudgetContributorRows(limits *telemetry.RateLimits) []graphQLBudgetContributorRow {
	if limits == nil || limits.GraphQLCost == nil || len(limits.GraphQLCost.Contributors) == 0 {
		return nil
	}

	total := limits.GraphQLCost.TotalCost
	rows := make([]graphQLBudgetContributorRow, 0, len(limits.GraphQLCost.Contributors))
	for _, contributor := range limits.GraphQLCost.Contributors {
		rows = append(rows, graphQLBudgetContributorRow{
			QueryType: strings.TrimSpace(contributor.QueryType),
			Count:     formatInt(contributor.Count) + " " + pluralize("query", contributor.Count),
			Cost:      formatInt(contributor.Cost) + " " + pluralize("point", contributor.Cost),
			Percent:   graphQLCostPercent(contributor.Cost, total),
		})
	}
	return rows
}

func restBudgetRemaining(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GitHubREST == nil {
		return "n/a"
	}
	bucket := limits.GitHubREST
	if bucket.Limit > 0 {
		return formatInt(bucket.Remaining) + " / " + formatInt(bucket.Limit)
	}
	return formatInt(bucket.Remaining) + " left"
}

func restBudgetReset(limits *telemetry.RateLimits, now time.Time) string {
	if limits == nil || limits.GitHubREST == nil {
		return "n/a"
	}
	bucket := limits.GitHubREST
	if bucket.ResetInSeconds > 0 {
		return formatDuration(float64(bucket.ResetInSeconds)) + " to reset"
	}
	if bucket.ResetAt != nil {
		if !now.IsZero() && bucket.ResetAt.After(now) {
			return formatDuration(bucket.ResetAt.Sub(now).Seconds()) + " to reset"
		}
		return localTimeToken(*bucket.ResetAt, LocalTimeOnly)
	}
	return "n/a"
}

func restBudgetResetAt(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GitHubREST == nil || limits.GitHubREST.ResetAt == nil {
		return "reset time n/a"
	}
	return "resets " + localTimeToken(*limits.GitHubREST.ResetAt, LocalTimeOnly)
}

func restBudgetRequestCount(limits *telemetry.RateLimits) string {
	if limits == nil || limits.RESTUsage == nil {
		return "0 requests"
	}
	return formatInt(limits.RESTUsage.TotalRequests) + " " + pluralize("request", limits.RESTUsage.TotalRequests)
}

func restBudgetContributorRows(limits *telemetry.RateLimits) []restBudgetContributorRow {
	if limits != nil && len(limits.GitHubRESTBudgets) > 0 {
		contributors := make(map[string]telemetry.RESTUsageContributor)
		if limits.RESTUsage != nil {
			for _, contributor := range limits.RESTUsage.Contributors {
				contributors[restBudgetRowKey(contributor.Consumer, contributor.CredentialIdentity, contributor.EndpointFamily, contributor.Resource)] = contributor
			}
		}
		rows := make([]restBudgetContributorRow, 0, len(limits.GitHubRESTBudgets))
		for _, budget := range limits.GitHubRESTBudgets {
			consumer := restBudgetConsumerLabel(budget.Consumer)
			contributor := contributors[restBudgetRowKey(consumer, budget.CredentialIdentity, budget.EndpointFamily, budget.Resource)]
			rows = append(rows, restBudgetContributorRow{
				Consumer:           consumer,
				CredentialIdentity: strings.TrimSpace(budget.CredentialIdentity),
				EndpointFamily:     strings.TrimSpace(budget.EndpointFamily),
				Resource:           strings.TrimSpace(budget.Resource),
				Count:              restBudgetRowCount(budget, contributor),
				Remaining:          restBudgetRowRemaining(budget),
				Reset:              restBudgetRowReset(budget),
				Status:             restBudgetRowStatus(budget, contributor),
			})
		}
		return rows
	}
	if limits == nil || limits.RESTUsage == nil || len(limits.RESTUsage.Contributors) == 0 {
		return nil
	}
	rows := make([]restBudgetContributorRow, 0, len(limits.RESTUsage.Contributors))
	for _, contributor := range limits.RESTUsage.Contributors {
		rows = append(rows, restBudgetContributorRow{
			Consumer:           restBudgetConsumerLabel(contributor.Consumer),
			CredentialIdentity: strings.TrimSpace(contributor.CredentialIdentity),
			EndpointFamily:     strings.TrimSpace(contributor.EndpointFamily),
			Resource:           strings.TrimSpace(contributor.Resource),
			Count:              formatInt(contributor.Count) + " " + pluralize("request", contributor.Count),
			Remaining:          restContributorRemaining(contributor),
			Reset:              restContributorReset(contributor),
			Status:             restContributorStatus(contributor),
		})
	}
	return rows
}

func restBudgetRowKey(consumer string, credentialIdentity string, endpointFamily string, resource string) string {
	return restBudgetConsumerLabel(consumer) + "\x00" + strings.TrimSpace(credentialIdentity) + "\x00" + strings.TrimSpace(endpointFamily) + "\x00" + strings.TrimSpace(resource)
}

func restBudgetConsumerLabel(consumer string) string {
	consumer = strings.TrimSpace(consumer)
	if consumer == "" {
		return telemetry.RESTConsumerOrchestrator
	}
	return consumer
}

func restBudgetRowCount(budget telemetry.RESTBudget, contributor telemetry.RESTUsageContributor) string {
	if restBudgetConsumerLabel(budget.Consumer) == telemetry.RESTConsumerSharedPool {
		return "usage indeterminate"
	}
	if restBudgetConsumerLabel(budget.Consumer) == telemetry.RESTConsumerWorker {
		return formatInt(budget.Used) + " used"
	}
	return formatInt(contributor.Count) + " " + pluralize("request", contributor.Count)
}

func restBudgetRowStatus(budget telemetry.RESTBudget, contributor telemetry.RESTUsageContributor) string {
	if budget.MinRemainingReserve > 0 && budget.Remaining <= budget.MinRemainingReserve {
		return "reserved"
	}
	if restBudgetConsumerLabel(budget.Consumer) == telemetry.RESTConsumerWorker {
		return "governed"
	}
	if restBudgetConsumerLabel(budget.Consumer) == telemetry.RESTConsumerSharedPool {
		return "governed shared"
	}
	return restContributorStatus(contributor)
}

func restBudgetRowRemaining(budget telemetry.RESTBudget) string {
	if budget.Limit > 0 {
		return formatInt(budget.Remaining) + " / " + formatInt(budget.Limit)
	}
	return formatInt(budget.Remaining) + " left"
}

func restBudgetRowReset(budget telemetry.RESTBudget) string {
	if budget.ResetAt == nil {
		return "reset n/a"
	}
	return localTimeToken(*budget.ResetAt, LocalTimeOnly)
}

func restBudgetCredentialCount(limits *telemetry.RateLimits) int64 {
	if limits == nil {
		return 0
	}
	identities := make(map[string]struct{}, len(limits.GitHubRESTBudgets))
	for _, budget := range limits.GitHubRESTBudgets {
		identities[strings.TrimSpace(budget.CredentialIdentity)] = struct{}{}
	}
	return int64(len(identities))
}

func restContributorRemaining(contributor telemetry.RESTUsageContributor) string {
	if contributor.Limit > 0 {
		return formatInt(contributor.Remaining) + " / " + formatInt(contributor.Limit)
	}
	if contributor.Remaining > 0 {
		return formatInt(contributor.Remaining) + " left"
	}
	return "remaining n/a"
}

func restContributorReset(contributor telemetry.RESTUsageContributor) string {
	if contributor.RetryAfterMS > 0 {
		return formatDuration(float64(contributor.RetryAfterMS)/1000) + " retry"
	}
	if contributor.ResetAt != nil {
		return localTimeToken(*contributor.ResetAt, LocalTimeOnly)
	}
	return "reset n/a"
}

func restContributorStatus(contributor telemetry.RESTUsageContributor) string {
	if contributor.RateLimited {
		return "rate limited"
	}
	if contributor.BudgetGate == "fanout_cap" {
		return "fanout deferred"
	}
	if contributor.BudgetGate == "reserve" {
		return "reserve held"
	}
	if contributor.LastStatus > 0 {
		return formatInt(int64(contributor.LastStatus))
	}
	return "ok"
}

func gitHubAPIHealth(snapshot telemetry.Snapshot) gitHubAPIHealthView {
	view := gitHubAPIHealthView{
		State:     gitHubAPIHealthStateUnknown,
		Label:     "GitHub API unknown",
		Summary:   "No GitHub rate-limit snapshot",
		Detail:    "No GitHub rate-limit snapshot is available in the latest tracker state.",
		Buckets:   gitHubAPIHealthBucketRows(snapshot.RateLimits),
		Endpoints: gitHubAPIHealthEndpointRows(snapshot),
		Refreshes: gitHubAPIHealthRefreshRows(snapshot.Refresh),
	}
	if !gitHubAPIHasSnapshot(snapshot.RateLimits) {
		if gitHubAPITrackerDegraded(snapshot) {
			view.State = gitHubAPIHealthStateWarning
			view.Label = "GitHub tracker degraded"
			view.Detail = gitHubAPITrackerDegradedDetail(snapshot)
		}
		return view
	}

	primarySummary := gitHubAPIPrimarySummary(snapshot.RateLimits)
	if primarySummary == "" {
		primarySummary = "Primary quota unavailable"
	}

	if gitHubAPIPrimaryExhausted(snapshot.RateLimits) {
		view.State = gitHubAPIHealthStateExhausted
		view.Label = "GitHub primary quota exhausted"
		view.Summary = primarySummary
		view.Detail = "Primary REST or GraphQL quota exhausted; " + gitHubAPIExhaustedReset(snapshot.RateLimits) + "."
		return view
	}
	if gitHubAPIGraphQLBackoff(snapshot) {
		view.State = gitHubAPIHealthStateBackoff
		view.Label = "GitHub GraphQL backoff active"
		view.Summary = primarySummary
		view.Detail = "GitHub GraphQL requests are in backoff; " + gitHubAPIGraphQLBackoffReset(snapshot.RateLimits) + "."
		return view
	}
	if gitHubAPIInBackoff(snapshot) {
		families := gitHubAPIBackoffFamilyLabel(snapshot, snapshot.RateLimits.RESTUsage)
		primaryContext := gitHubAPIRESTPrimaryContext(snapshot.RateLimits)
		retrySentence := gitHubAPIBackoffRetrySentence(snapshot)
		view.State = gitHubAPIHealthStateBackoff
		view.Label = "GitHub secondary throttle active for " + families
		view.Summary = primaryContext + ". " + retrySentence + "."
		view.Detail = "GitHub secondary endpoint throttle is active for " + families + ". " + primaryContext + ". " + retrySentence + "."
		return view
	}
	if localDeferral := gitHubAPILocalRESTDeferral(snapshot.RateLimits); localDeferral != "" {
		view.State = gitHubAPIHealthStateWarning
		view.Label = localDeferral
		view.Summary = gitHubAPIRESTPrimaryContext(snapshot.RateLimits) + "."
		view.Detail = gitHubAPILocalRESTDeferralDetail(snapshot.RateLimits)
		return view
	}
	if gitHubAPITrackerDegraded(snapshot) {
		view.State = gitHubAPIHealthStateWarning
		view.Label = "GitHub tracker degraded"
		view.Summary = primarySummary
		view.Detail = gitHubAPITrackerDegradedDetail(snapshot)
		return view
	}
	if gitHubAPIPrimaryWarning(snapshot.RateLimits) {
		view.State = gitHubAPIHealthStateWarning
		view.Label = "GitHub primary quota low"
		view.Summary = primarySummary
		view.Detail = "Primary quota is below the warning threshold; " + gitHubAPIWarningReset(snapshot.RateLimits) + "."
		return view
	}
	if gitHubAPIGraphQLAtRest(snapshot.RateLimits) {
		view.State = gitHubAPIHealthStateAtRest
		view.Label = "GitHub API at rest"
		view.Summary = gitHubAPIAtRestSummary(snapshot.RateLimits)
		view.Detail = "GraphQL quota is reported from live GraphQL traffic; none has occurred in this session. This is expected while boards are idle or when the status source is label-backed."
		return view
	}
	if gitHubAPIGraphQLPrimaryUnknown(snapshot.RateLimits) {
		restContext := gitHubAPIRESTPrimaryContext(snapshot.RateLimits)
		view.State = gitHubAPIHealthStateUnknown
		view.Label = "GitHub API GraphQL unknown"
		view.Summary = restContext + ". GraphQL primary quota unavailable."
		view.Detail = "REST primary quota is visible, but GraphQL primary quota could not be determined after an observation attempt."
		return view
	}

	view.State = gitHubAPIHealthStateHealthy
	view.Label = "GitHub API healthy"
	view.Summary = primarySummary
	view.Detail = gitHubAPIHealthyDetail(snapshot)
	return view
}

func gitHubAPILocalRESTDeferral(limits *telemetry.RateLimits) string {
	if limits == nil || limits.RESTUsage == nil {
		return ""
	}
	switch {
	case limits.RESTUsage.FanoutDeferred && limits.RESTUsage.ReserveHeld:
		return "GitHub REST local deferrals active"
	case limits.RESTUsage.FanoutDeferred:
		return "GitHub REST fanout deferred"
	case limits.RESTUsage.ReserveHeld:
		return "GitHub REST reserve held"
	default:
		return ""
	}
}

func gitHubAPILocalRESTDeferralDetail(limits *telemetry.RateLimits) string {
	primary := gitHubAPIRESTPrimaryContext(limits)
	if limits == nil || limits.RESTUsage == nil {
		return primary + "."
	}
	switch {
	case limits.RESTUsage.FanoutDeferred && limits.RESTUsage.ReserveHeld:
		return "Internal REST fanout capacity and the provider quota reserve floor deferred local work. " + primary + "."
	case limits.RESTUsage.FanoutDeferred:
		return "The internal REST fanout cap deferred local work without a provider rate-limit response. " + primary + "."
	default:
		return "The provider quota reserve floor deferred local work before exhaustion. " + primary + "."
	}
}

func gitHubAPITrackerDegraded(snapshot telemetry.Snapshot) bool {
	return snapshot.Refresh.Degraded()
}

func gitHubAPITrackerDegradedDetail(snapshot telemetry.Snapshot) string {
	if snapshotHasPriorTrackerSnapshot(snapshot) {
		return "Tracker refresh degraded. " + snapshotDegradedRefreshDetail(snapshot)
	}
	return "Tracker refresh failed. " + snapshotFirstRefreshFailureDetail(snapshot)
}

func gitHubAPIHealthKind(snapshot telemetry.Snapshot) primitives.Kind {
	switch gitHubAPIHealth(snapshot).State {
	case gitHubAPIHealthStateHealthy:
		return primitives.KindOK
	case gitHubAPIHealthStateAtRest:
		return primitives.KindInfo
	case gitHubAPIHealthStateWarning, gitHubAPIHealthStateBackoff:
		return primitives.KindWarn
	case gitHubAPIHealthStateExhausted:
		return primitives.KindErr
	default:
		return primitives.KindNeutral
	}
}

func gitHubAPIHealthDotClass(snapshot telemetry.Snapshot) string {
	switch gitHubAPIHealth(snapshot).State {
	case gitHubAPIHealthStateHealthy:
		return "bg-ok"
	case gitHubAPIHealthStateAtRest:
		return "bg-accent"
	case gitHubAPIHealthStateWarning, gitHubAPIHealthStateBackoff:
		return "bg-warn"
	case gitHubAPIHealthStateExhausted:
		return "bg-err"
	default:
		return "bg-dim"
	}
}

func gitHubAPIHealthBadgeClass(snapshot telemetry.Snapshot) string {
	switch gitHubAPIHealth(snapshot).State {
	case gitHubAPIHealthStateHealthy:
		return "border-ok/15 bg-ok/15 text-ok"
	case gitHubAPIHealthStateAtRest:
		return "border-accent/15 bg-accent/15 text-accent"
	case gitHubAPIHealthStateWarning, gitHubAPIHealthStateBackoff:
		return "border-warn/15 bg-warn/15 text-warn"
	case gitHubAPIHealthStateExhausted:
		return "border-err/15 bg-err/15 text-err"
	default:
		return "border-line bg-elev text-text/70"
	}
}

func gitHubAPIHealthStateLabel(snapshot telemetry.Snapshot) string {
	switch gitHubAPIHealth(snapshot).State {
	case gitHubAPIHealthStateHealthy:
		return "Healthy"
	case gitHubAPIHealthStateAtRest:
		return "At rest"
	case gitHubAPIHealthStateWarning:
		return "Warning"
	case gitHubAPIHealthStateBackoff:
		return "Backoff"
	case gitHubAPIHealthStateExhausted:
		return "Exhausted"
	default:
		return "Unknown"
	}
}

func gitHubAPIHealthBackoffLabel(snapshot telemetry.Snapshot) string {
	switch {
	case gitHubAPIGraphQLBackoff(snapshot):
		return "GraphQL backoff active"
	case gitHubAPIInBackoff(snapshot):
		return "Secondary REST backoff active"
	case gitHubAPIPrimaryExhausted(snapshot.RateLimits):
		return "Primary quota exhausted"
	default:
		return "No active backoff"
	}
}

func gitHubAPIHealthBackoffDetail(snapshot telemetry.Snapshot) string {
	switch {
	case gitHubAPIGraphQLBackoff(snapshot), gitHubAPIInBackoff(snapshot), gitHubAPIPrimaryExhausted(snapshot.RateLimits):
		return gitHubAPIHealth(snapshot).Detail
	default:
		return "No GitHub GraphQL or secondary REST backoff is active."
	}
}

func gitHubAPIHealthPrimaryQuotaLabel(snapshot telemetry.Snapshot) string {
	summary := gitHubAPIPrimarySummary(snapshot.RateLimits)
	if summary == "" {
		return "Primary quota unavailable"
	}
	return summary
}

func gitHubAPIHealthPrimaryQuotaDetail(snapshot telemetry.Snapshot) string {
	if gitHubAPIGraphQLAtRest(snapshot.RateLimits) {
		return "REST primary quota is shown below. GraphQL quota will appear after live GraphQL traffic is observed."
	}
	if gitHubAPIHasSnapshot(snapshot.RateLimits) {
		return "REST and GraphQL primary buckets are shown below with remaining quota, usage, limits, and reset timing."
	}
	return "Primary REST and GraphQL quota data has not arrived in the latest tracker snapshot."
}

func gitHubAPIHealthRetryLabel(snapshot telemetry.Snapshot) string {
	switch {
	case gitHubAPIGraphQLBackoff(snapshot):
		return gitHubAPIGraphQLBackoffReset(snapshot.RateLimits)
	case gitHubAPIInBackoff(snapshot):
		return gitHubAPIBackoffRetrySentence(snapshot)
	case gitHubAPIPrimaryExhausted(snapshot.RateLimits):
		return gitHubAPIExhaustedReset(snapshot.RateLimits)
	case gitHubAPIPrimaryWarning(snapshot.RateLimits):
		return gitHubAPIWarningReset(snapshot.RateLimits)
	default:
		return "No retry waiting"
	}
}

func gitHubAPIHealthRetryDetail(snapshot telemetry.Snapshot) string {
	if gitHubAPIHealthRetryLabel(snapshot) == "No retry waiting" {
		return "No GitHub throttle retry or exhausted-quota reset is currently gating tracker work."
	}
	return "Retry and reset timing is derived from active backoff, retry-after, and primary bucket reset data."
}

func gitHubAPIHasSnapshot(limits *telemetry.RateLimits) bool {
	return limits != nil &&
		(gitHubAPIBucketKnown(limits.GitHubREST) ||
			gitHubAPIBucketKnown(limits.GitHubGraphQL) ||
			limits.RESTUsage != nil)
}

func gitHubAPIBucketKnown(bucket *telemetry.RateLimitBucket) bool {
	return bucket != nil &&
		(rateLimitBucketStatus(bucket) != "" ||
			bucket.Limit > 0 ||
			bucket.Remaining > 0 ||
			bucket.Used > 0 ||
			bucket.ResetAt != nil ||
			bucket.ObservedAt != nil ||
			bucket.ResetInSeconds > 0)
}

func gitHubAPIPrimaryExhausted(limits *telemetry.RateLimits) bool {
	if limits == nil {
		return false
	}
	for _, bucket := range []*telemetry.RateLimitBucket{limits.GitHubREST, limits.GitHubGraphQL} {
		if gitHubAPIBucketExhausted(bucket) {
			return true
		}
	}
	return false
}

func gitHubAPIBucketExhausted(bucket *telemetry.RateLimitBucket) bool {
	if bucket == nil {
		return false
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted {
		return true
	}
	return bucket.Limit > 0 && bucket.Remaining <= 0
}

func gitHubAPIPrimaryWarning(limits *telemetry.RateLimits) bool {
	if limits == nil {
		return false
	}
	for _, bucket := range []*telemetry.RateLimitBucket{limits.GitHubREST, limits.GitHubGraphQL} {
		if gitHubAPIBucketWarning(bucket) {
			return true
		}
	}
	return false
}

func gitHubAPIBucketWarning(bucket *telemetry.RateLimitBucket) bool {
	if bucket == nil || gitHubAPIBucketExhausted(bucket) || bucket.Limit <= 0 || bucket.Remaining <= 0 {
		return false
	}
	if bucket.Remaining <= gitHubAPIWarningRemaining {
		return true
	}
	return float64(bucket.Remaining)/float64(bucket.Limit) <= gitHubAPIWarningRatio
}

func gitHubAPIGraphQLAtRest(limits *telemetry.RateLimits) bool {
	return limits != nil &&
		gitHubAPIBucketKnown(limits.GitHubREST) &&
		!gitHubAPIBucketWarning(limits.GitHubREST) &&
		!gitHubAPIBucketExhausted(limits.GitHubREST) &&
		!gitHubAPIBucketKnown(limits.GitHubGraphQL) &&
		(limits.GraphQLCost == nil || limits.GraphQLCost.TotalQueries == 0)
}

func gitHubAPIInBackoff(snapshot telemetry.Snapshot) bool {
	if snapshot.RateLimits == nil || snapshot.RateLimits.RESTUsage == nil {
		return false
	}
	usage := snapshot.RateLimits.RESTUsage
	if usage.RateLimited && gitHubAPIDeadlineActive(snapshot.GeneratedAt, usage.BackoffUntil) {
		return true
	}
	if usage.RateLimited && usage.BackoffUntil == nil {
		return true
	}
	for _, contributor := range usage.Contributors {
		if gitHubAPIContributorBackedOff(snapshot, contributor, usage.BackoffUntil) {
			return true
		}
	}
	return false
}

func gitHubAPIGraphQLBackoff(snapshot telemetry.Snapshot) bool {
	if snapshot.RateLimits == nil {
		return false
	}
	bucket := snapshot.RateLimits.GitHubGraphQL
	if bucket == nil || rateLimitBucketStatus(bucket) != telemetry.RateLimitStatusBackoff {
		return false
	}
	if bucket.ResetAt == nil {
		return true
	}
	return gitHubAPIDeadlineActive(snapshot.GeneratedAt, bucket.ResetAt)
}

func gitHubAPIContributorBackedOff(snapshot telemetry.Snapshot, contributor telemetry.RESTUsageContributor, backoffUntil *time.Time) bool {
	if !gitHubAPIContributorHasBackoffSignal(contributor) {
		return false
	}
	if backoffUntil != nil {
		return gitHubAPIDeadlineActive(snapshot.GeneratedAt, backoffUntil)
	}
	if contributor.ResetAt != nil {
		return gitHubAPIDeadlineActive(snapshot.GeneratedAt, contributor.ResetAt)
	}
	return true
}

func gitHubAPIContributorHasBackoffSignal(contributor telemetry.RESTUsageContributor) bool {
	if contributor.BudgetGate != "" {
		return false
	}
	return contributor.RateLimited || contributor.LastStatus == httpStatusTooManyRequests || contributor.RetryAfterMS > 0
}

func gitHubAPIDeadlineActive(generatedAt time.Time, deadline *time.Time) bool {
	return deadline != nil && (generatedAt.IsZero() || deadline.After(generatedAt))
}

func gitHubAPIGraphQLPrimaryUnknown(limits *telemetry.RateLimits) bool {
	return limits != nil &&
		gitHubAPIBucketKnown(limits.GitHubREST) &&
		rateLimitBucketStatus(limits.GitHubGraphQL) == telemetry.RateLimitStatusUnknown
}

func gitHubAPIPrimarySummary(limits *telemetry.RateLimits) string {
	if limits == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	appendBucket := func(name string, bucket *telemetry.RateLimitBucket) {
		if !gitHubAPIBucketKnown(bucket) {
			return
		}
		parts = append(parts, gitHubAPIPrimaryBucketSummary(name+" primary", bucket))
	}
	appendBucket("REST", limits.GitHubREST)
	appendBucket("GraphQL", limits.GitHubGraphQL)
	return strings.Join(parts, "; ")
}

func gitHubAPIPrimaryBucketSummary(label string, bucket *telemetry.RateLimitBucket) string {
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return label + ": unknown"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return label + ": exhausted"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusBackoff && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return label + ": backoff"
	}
	remaining := formatInt(bucket.Remaining) + " remaining"
	if bucket.Limit > 0 {
		return label + ": " + remaining + " / " + formatInt(bucket.Limit) + " total (" + formatInt(gitHubAPIPrimaryBucketUsed(bucket)) + " used)"
	}
	if bucket.Used > 0 {
		return label + ": " + remaining + " (" + formatInt(bucket.Used) + " used)"
	}
	return label + ": " + remaining
}

func gitHubAPIHealthyDetail(snapshot telemetry.Snapshot) string {
	detail := "Primary quota is available and no secondary REST backoff is active."
	freshness := gitHubAPIGraphQLObservationFreshness(snapshot)
	if freshness == "" {
		return detail
	}
	return detail + " GraphQL quota " + freshness + "."
}

func gitHubAPIAtRestSummary(limits *telemetry.RateLimits) string {
	return "REST primary: " + gitHubAPIPrimaryBucketCompact(limits.GitHubREST) + ". No GraphQL usage this session."
}

func gitHubAPIGraphQLObservationFreshness(snapshot telemetry.Snapshot) string {
	if snapshot.RateLimits == nil || snapshot.RateLimits.GitHubGraphQL == nil || snapshot.RateLimits.GitHubGraphQL.ObservedAt == nil {
		return ""
	}
	observedAt := snapshot.RateLimits.GitHubGraphQL.ObservedAt.UTC()
	if snapshot.GeneratedAt.IsZero() || observedAt.After(snapshot.GeneratedAt) {
		return "observed " + timeLabel(observedAt)
	}
	return "observed " + formatDuration(snapshot.GeneratedAt.Sub(observedAt).Seconds()) + " ago"
}

func gitHubAPIPrimaryBucketUsed(bucket *telemetry.RateLimitBucket) int64 {
	if bucket.Used > 0 || bucket.Limit <= 0 {
		return bucket.Used
	}
	if bucket.Remaining >= 0 && bucket.Remaining <= bucket.Limit {
		return bucket.Limit - bucket.Remaining
	}
	return bucket.Used
}

func gitHubAPIBackoffFamilyLabel(snapshot telemetry.Snapshot, usage *telemetry.RESTUsage) string {
	families := gitHubAPIBackoffFamilies(snapshot, usage)
	if len(families) == 0 {
		return "REST endpoints"
	}
	return strings.Join(families, "/")
}

func gitHubAPIBackoffFamilies(snapshot telemetry.Snapshot, usage *telemetry.RESTUsage) []string {
	if usage == nil {
		return nil
	}
	families := make([]string, 0, len(usage.Contributors))
	seen := map[string]struct{}{}
	for _, contributor := range usage.Contributors {
		if !gitHubAPIContributorBackedOff(snapshot, contributor, usage.BackoffUntil) {
			continue
		}
		family := strings.TrimSpace(contributor.EndpointFamily)
		if family == "" {
			family = "REST endpoints"
		}
		if _, ok := seen[family]; ok {
			continue
		}
		seen[family] = struct{}{}
		families = append(families, family)
	}
	return families
}

func gitHubAPIBackoffRetrySentence(snapshot telemetry.Snapshot) string {
	if snapshot.RateLimits == nil || snapshot.RateLimits.RESTUsage == nil {
		return "Retry time unavailable"
	}
	usage := snapshot.RateLimits.RESTUsage
	if gitHubAPIDeadlineActive(snapshot.GeneratedAt, usage.BackoffUntil) {
		return "Retrying at " + localTimeToken(*usage.BackoffUntil, LocalTimeOnly)
	}
	for _, contributor := range usage.Contributors {
		if !gitHubAPIContributorBackedOff(snapshot, contributor, usage.BackoffUntil) {
			continue
		}
		if contributor.ResetAt != nil && gitHubAPIDeadlineActive(snapshot.GeneratedAt, contributor.ResetAt) {
			return "Retrying at " + localTimeToken(*contributor.ResetAt, LocalTimeOnly)
		}
		if contributor.RetryAfterMS > 0 {
			return "Retrying after " + formatDuration(float64(contributor.RetryAfterMS)/1000)
		}
	}
	return "Retry time unavailable"
}

func gitHubAPIRESTPrimaryContext(limits *telemetry.RateLimits) string {
	if limits == nil || !gitHubAPIBucketKnown(limits.GitHubREST) {
		return "Primary REST quota health is unavailable"
	}
	bucket := limits.GitHubREST
	status := "healthy"
	if gitHubAPIBucketExhausted(bucket) {
		status = "exhausted"
	} else if gitHubAPIBucketWarning(bucket) {
		status = "low"
	}
	return "Primary REST quota is " + status + ": " + gitHubAPIPrimaryBucketCompact(bucket)
}

func gitHubAPIPrimaryBucketCompact(bucket *telemetry.RateLimitBucket) string {
	if bucket.Limit > 0 {
		return formatInt(bucket.Remaining) + "/" + formatInt(bucket.Limit) + " remaining"
	}
	return formatInt(bucket.Remaining) + " remaining"
}

func gitHubAPIExhaustedReset(limits *telemetry.RateLimits) string {
	if limits == nil {
		return "reset time n/a"
	}
	reset := gitHubAPIEarliestExhaustedReset(limits.GitHubREST, limits.GitHubGraphQL)
	if reset != nil {
		return "reset " + localTimeToken(*reset, LocalTimeOnly)
	}
	return "reset time n/a"
}

func gitHubAPIEarliestExhaustedReset(buckets ...*telemetry.RateLimitBucket) *time.Time {
	exhausted := make([]*telemetry.RateLimitBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if gitHubAPIBucketExhausted(bucket) {
			exhausted = append(exhausted, bucket)
		}
	}
	return gitHubAPIEarliestReset(exhausted...)
}

func gitHubAPIGraphQLBackoffReset(limits *telemetry.RateLimits) string {
	if limits == nil || limits.GitHubGraphQL == nil {
		return "retry time n/a"
	}
	bucket := limits.GitHubGraphQL
	if bucket.ResetAt != nil {
		return "retry " + localTimeToken(*bucket.ResetAt, LocalTimeOnly)
	}
	if bucket.ResetInSeconds > 0 {
		return "retry in " + formatDuration(float64(bucket.ResetInSeconds))
	}
	return "retry time n/a"
}

func gitHubAPIWarningReset(limits *telemetry.RateLimits) string {
	if limits == nil {
		return "reset time n/a"
	}
	reset := gitHubAPIEarliestReset(limits.GitHubREST, limits.GitHubGraphQL)
	if reset != nil {
		return "next reset " + localTimeToken(*reset, LocalTimeOnly)
	}
	return "reset time n/a"
}

func gitHubAPIEarliestReset(buckets ...*telemetry.RateLimitBucket) *time.Time {
	var earliest *time.Time
	for _, bucket := range buckets {
		if bucket == nil || bucket.ResetAt == nil {
			continue
		}
		reset := bucket.ResetAt.UTC()
		if earliest == nil || reset.Before(*earliest) {
			earliest = &reset
		}
	}
	return earliest
}

func gitHubAPIHealthBucketRows(limits *telemetry.RateLimits) []gitHubAPIHealthBucketRow {
	if limits == nil {
		return nil
	}
	rows := make([]gitHubAPIHealthBucketRow, 0, 2)
	appendBucket := func(name string, bucket *telemetry.RateLimitBucket) {
		if !gitHubAPIBucketKnown(bucket) {
			return
		}
		rows = append(rows, gitHubAPIHealthBucketRow{
			Name:      name,
			Remaining: gitHubAPIHealthBucketRemaining(bucket),
			Used:      formatInt(bucket.Used) + " used",
			Limit:     formatLimit(bucket.Limit) + " limit",
			Reset:     gitHubAPIHealthBucketReset(bucket),
		})
	}
	appendBucket("REST primary", limits.GitHubREST)
	appendBucket("GraphQL primary", limits.GitHubGraphQL)
	return rows
}

func gitHubAPIHealthBucketRemaining(bucket *telemetry.RateLimitBucket) string {
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusUnknown {
		return "unknown"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusExhausted && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "exhausted"
	}
	if rateLimitBucketStatus(bucket) == telemetry.RateLimitStatusBackoff && bucket.Limit <= 0 && bucket.Remaining == 0 {
		return "backoff"
	}
	if bucket.Limit > 0 {
		return formatInt(bucket.Remaining) + " / " + formatInt(bucket.Limit) + " remaining"
	}
	return formatInt(bucket.Remaining) + " remaining"
}

func rateLimitBucketStatus(bucket *telemetry.RateLimitBucket) string {
	if bucket == nil {
		return ""
	}
	return strings.TrimSpace(bucket.Status)
}

func gitHubAPIHealthBucketReset(bucket *telemetry.RateLimitBucket) string {
	if bucket.ResetAt != nil {
		return "reset " + localTimeToken(*bucket.ResetAt, LocalTimeOnly)
	}
	if bucket.ResetInSeconds > 0 {
		return "reset in " + formatDuration(float64(bucket.ResetInSeconds))
	}
	return "reset time n/a"
}

func gitHubAPIHealthEndpointRows(snapshot telemetry.Snapshot) []gitHubAPIHealthEndpointRow {
	if snapshot.RateLimits == nil || snapshot.RateLimits.RESTUsage == nil {
		return nil
	}
	usage := snapshot.RateLimits.RESTUsage
	rows := make([]gitHubAPIHealthEndpointRow, 0, len(usage.Contributors))
	for _, contributor := range usage.Contributors {
		if !gitHubAPIContributorBackedOff(snapshot, contributor, usage.BackoffUntil) && contributor.BudgetGate == "" {
			continue
		}
		rows = append(rows, gitHubAPIHealthEndpointRow{
			EndpointFamily: gitHubAPIEndpointFamily(contributor),
			Count:          formatInt(contributor.Count) + " " + pluralize("request", contributor.Count),
			Status:         gitHubAPIEndpointStatus(contributor),
			Retry:          gitHubAPIEndpointRetry(snapshot, contributor, usage.BackoffUntil),
			RetryAfter:     gitHubAPIEndpointRetryAfter(contributor),
			Remaining:      restContributorRemaining(contributor),
		})
	}
	return rows
}

func gitHubAPIEndpointFamily(contributor telemetry.RESTUsageContributor) string {
	family := strings.TrimSpace(contributor.EndpointFamily)
	if family == "" {
		family = "REST endpoints"
	}
	if scope := strings.TrimSpace(contributor.BudgetScope); scope != "" {
		return scope + ": " + family
	}
	return family
}

func gitHubAPIEndpointStatus(contributor telemetry.RESTUsageContributor) string {
	if contributor.BudgetGate != "" {
		return restContributorStatus(contributor)
	}
	if contributor.LastStatus > 0 {
		return formatInt(int64(contributor.LastStatus))
	}
	if contributor.RateLimited {
		return "rate limited"
	}
	return "n/a"
}

func gitHubAPIEndpointRetry(snapshot telemetry.Snapshot, contributor telemetry.RESTUsageContributor, backoffUntil *time.Time) string {
	if gitHubAPIDeadlineActive(snapshot.GeneratedAt, backoffUntil) {
		return "retry " + localTimeToken(*backoffUntil, LocalTimeOnly)
	}
	if gitHubAPIDeadlineActive(snapshot.GeneratedAt, contributor.ResetAt) {
		return "reset " + localTimeToken(*contributor.ResetAt, LocalTimeOnly)
	}
	return "retry time n/a"
}

func gitHubAPIEndpointRetryAfter(contributor telemetry.RESTUsageContributor) string {
	if contributor.RetryAfterMS <= 0 {
		return "retry-after n/a"
	}
	return "retry-after " + formatDuration(float64(contributor.RetryAfterMS)/1000)
}

func gitHubAPIHealthRefreshRows(refresh telemetry.Refresh) []gitHubAPIHealthRefreshRow {
	rows := []gitHubAPIHealthRefreshRow{
		{Label: "Last tracker refresh", Value: "n/a"},
		{Label: "Next tracker refresh", Value: "n/a"},
	}
	if refresh.LastRefreshAt != nil {
		rows[0].Value = timeLabel(*refresh.LastRefreshAt)
	}
	if refresh.NextRefreshAt != nil {
		rows[1].Value = timeLabel(*refresh.NextRefreshAt)
	}
	if manualRefreshStatusVisible(refresh.Manual) {
		rows = append(rows, gitHubAPIHealthRefreshRow{Label: "Manual refresh", Value: manualRefreshStatusLabel(refresh.Manual)})
		if detail := manualRefreshStatusDetail(refresh.Manual); detail != "" {
			rows = append(rows, gitHubAPIHealthRefreshRow{Label: "Manual detail", Value: detail})
		}
	}
	return rows
}

func graphQLCostPercent(cost int64, total int64) string {
	if cost <= 0 || total <= 0 {
		return "0%"
	}
	return formatInt(int64(math.Round(float64(cost)/float64(total)*100))) + "%"
}

func pluralize(word string, count int64) string {
	if count == 1 {
		return word
	}
	if before, ok := strings.CutSuffix(word, "y"); ok {
		return before + "ies"
	}
	return word + "s"
}

func percentStyle(percent int) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	return fmt.Sprintf("width: %d%%;", percent)
}

func tokenTrendChart(snapshot telemetry.Snapshot) SplitSeriesChartData {
	points := tokenTrendPoints(snapshot)
	chartPoints := make([]SplitSeriesPoint, 0, len(points))
	for _, point := range points {
		chartPoints = append(chartPoints, SplitSeriesPoint{
			Label:  tokenTrendLabel(point),
			Input:  float64(point.Input),
			Output: float64(point.Output),
		})
	}
	return SplitSeriesChartData{
		Title:       "Token trend",
		AriaLabel:   "Token trend",
		InputLabel:  "Input",
		OutputLabel: "Output",
		Points:      chartPoints,
		ValueSuffix: "tokens",
	}
}

func throughputTrendChart(data DashboardData) SeriesChartData {
	return SeriesChartData{
		Title:       "Token throughput trend",
		AriaLabel:   "Rolling token throughput trend",
		Points:      throughputTrendPoints(data.Snapshot),
		ValueSuffix: "tps",
		ColorClass:  "text-accent",
	}
}

func throughputRate(snapshot telemetry.Snapshot) string {
	return formatDecimal(snapshot.Throughput.TokensPerSecond) + " tps"
}

func throughputWindowLabel(snapshot telemetry.Snapshot) string {
	window := time.Duration(snapshot.Throughput.WindowSeconds) * time.Second
	if window <= 0 {
		window = defaultThroughputWindow
	}
	return "Last " + formatDurationWindow(window) + " token throughput"
}

func concurrencyWindowLabel(history telemetry.ConcurrencyHistory) string {
	if history.From.IsZero() || history.To.IsZero() || !history.From.Before(history.To) {
		return "Rolling history"
	}
	return "Rolling " + formatDurationWindow(history.To.Sub(history.From)) + " from recorded work attempts"
}

func concurrencySeriesCards(history telemetry.ConcurrencyHistory) []concurrencySeriesCard {
	if !history.Available || history.AttemptCount == 0 {
		return nil
	}
	cards := make([]concurrencySeriesCard, 0, len(history.Series))
	for _, series := range history.Series {
		name := strings.TrimSpace(series.ProjectID)
		if name == "" {
			name = "Fleet-wide"
		}
		cards = append(cards, concurrencySeriesCard{
			Name: name,
			Metrics: []concurrencyMetricCard{
				concurrencyMetric("Median", "text-sec", series.Buckets, func(bucket telemetry.ConcurrencyBucket) int { return bucket.Median }),
				concurrencyMetric("p90", "text-warn", series.Buckets, func(bucket telemetry.ConcurrencyBucket) int { return bucket.P90 }),
				concurrencyMetric("Max", "text-accent", series.Buckets, func(bucket telemetry.ConcurrencyBucket) int { return bucket.Max }),
			},
		})
	}
	return cards
}

func concurrencyMetric(
	label string,
	colorClass string,
	buckets []telemetry.ConcurrencyBucket,
	value func(telemetry.ConcurrencyBucket) int,
) concurrencyMetricCard {
	points := make([]webchart.Point, 0, len(buckets))
	latest := 0
	for _, bucket := range buckets {
		latest = value(bucket)
		points = append(points, webchart.Point{
			Label: localTimeToken(bucket.Start, LocalDateTime),
			Value: float64(latest),
		})
	}
	return concurrencyMetricCard{
		Label: label,
		Value: formatCount(latest),
		Chart: SeriesChartData{
			Title:       label + " hourly concurrency",
			AriaLabel:   label + " hourly concurrency over the rolling window",
			Points:      points,
			ValueSuffix: "agents",
			Class:       "h-10",
			ColorClass:  colorClass,
			Height:      40,
		},
	}
}

func runtimeLabel(snapshot telemetry.Snapshot) string {
	return formatDuration(snapshot.Tokens.RuntimeSeconds)
}

func tokenRate(snapshot telemetry.Snapshot) string {
	if snapshot.Tokens.Total <= 0 || snapshot.Tokens.RuntimeSeconds <= 0 {
		return "n/a"
	}
	perMinute := int64(math.Round(float64(snapshot.Tokens.Total) / snapshot.Tokens.RuntimeSeconds * 60))
	return formatInt(perMinute) + " tokens/min"
}

func lifetimeStatus(totals telemetry.LifetimeTotals) string {
	if totals.Available {
		return "available"
	}
	return "unavailable"
}

func lifetimeDegradedReason(totals telemetry.LifetimeTotals) string {
	if strings.TrimSpace(totals.DegradedReason) != "" {
		return totals.DegradedReason
	}
	return "runtime store unavailable"
}

func lifetimeRuntime(totals telemetry.LifetimeTotals) string {
	return formatDuration(float64(totals.RuntimeSeconds))
}

func lifetimeSessions(totals telemetry.LifetimeTotals) string {
	return formatInt(totals.Sessions)
}

func lifetimeRuns(totals telemetry.LifetimeTotals) string {
	return formatInt(totals.Runs)
}

func lifetimeOrphanContinuations(totals telemetry.LifetimeTotals) string {
	return formatInt(totals.OrphanResumed) + " / " + formatInt(totals.OrphanFresh)
}

func lifetimeResumedCacheShare(totals telemetry.LifetimeTotals) string {
	fraction, ok := totals.ResumedCacheReadFraction()
	if !ok {
		return "n/a"
	}
	return formatContextPercent(fraction * 100)
}

func throughputTrendPoints(snapshot telemetry.Snapshot) []webchart.Point {
	points := tokenTrendPoints(snapshot)
	if len(points) < 2 {
		return nil
	}

	latest := points[len(points)-1].At.UTC()
	windowStart := latest.Add(-throughputTrendWindow)
	chartPoints := make([]webchart.Point, 0, len(points)-1)
	for index := 1; index < len(points); index++ {
		previous := points[index-1]
		current := points[index]
		if current.At.IsZero() || previous.At.IsZero() || current.At.Before(windowStart) {
			continue
		}
		elapsed := current.At.Sub(previous.At).Seconds()
		if elapsed <= 0 {
			continue
		}
		tokens := current.Total - previous.Total
		if tokens <= 0 {
			continue
		}
		chartPoints = append(chartPoints, webchart.Point{
			Label: throughputTrendLabel(current.At),
			Value: float64(tokens) / elapsed,
		})
	}
	return chartPoints
}

func throughputTrendLabel(at time.Time) string {
	if at.IsZero() {
		return "Latest"
	}
	at = at.UTC()
	if at.Second() == 0 {
		return localTimeToken(at, LocalTimeOnly)
	}
	return localTimeToken(at, LocalTimeWithSeconds)
}

func formatDuration(seconds float64) string {
	if seconds <= 0 {
		return "0s"
	}

	duration := time.Duration(math.Round(seconds)) * time.Second
	hours := int(duration / time.Hour)
	duration -= time.Duration(hours) * time.Hour
	minutes := int(duration / time.Minute)
	duration -= time.Duration(minutes) * time.Minute
	secs := int(duration / time.Second)

	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

func formatSignedDuration(seconds int64) string {
	if seconds < 0 {
		return "-" + formatDuration(float64(-seconds))
	}
	if seconds > 0 {
		return "+" + formatDuration(float64(seconds))
	}
	return "0s"
}

func formatDurationWindow(duration time.Duration) string {
	if duration <= 0 {
		return "0s"
	}
	if duration%time.Hour == 0 {
		return formatInt(int64(duration/time.Hour)) + "h"
	}
	if duration%time.Minute == 0 {
		return formatInt(int64(duration/time.Minute)) + "m"
	}
	return formatDuration(duration.Seconds())
}

func formatInt(value int64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	raw := strconv.FormatInt(value, 10)
	if len(raw) <= 3 {
		return sign + raw
	}

	first := len(raw) % 3
	if first == 0 {
		first = 3
	}

	var out strings.Builder
	out.Grow(len(sign) + len(raw) + (len(raw)-1)/3)
	out.WriteString(sign)
	out.WriteString(raw[:first])
	for i := first; i < len(raw); i += 3 {
		out.WriteByte(',')
		out.WriteString(raw[i : i+3])
	}
	return out.String()
}

func schedulerRuntimeCount(snapshot telemetry.Snapshot) string {
	return formatCount(len(schedulerWorkAttemptRows(snapshot)) + len(schedulerDecisionRows(snapshot)))
}

func schedulerWorkAttemptRows(snapshot telemetry.Snapshot) []telemetry.WorkAttempt {
	return limitWorkAttemptRows(snapshot.WorkAttempts, 6)
}

func schedulerDecisionRows(snapshot telemetry.Snapshot) []telemetry.SchedulerDecision {
	return limitSchedulerDecisionRows(snapshot.SchedulerDecisions, 8)
}

func limitWorkAttemptRows(rows []telemetry.WorkAttempt, limit int) []telemetry.WorkAttempt {
	if len(rows) == 0 || limit <= 0 {
		return nil
	}
	if len(rows) <= limit {
		return rows
	}
	return rows[:limit]
}

func limitSchedulerDecisionRows(rows []telemetry.SchedulerDecision, limit int) []telemetry.SchedulerDecision {
	if len(rows) == 0 || limit <= 0 {
		return nil
	}
	if len(rows) <= limit {
		return rows
	}
	return rows[:limit]
}

func workAttemptIssueLabel(row telemetry.WorkAttempt) string {
	if label := strings.TrimSpace(row.Identifier); label != "" {
		return label
	}
	if label := strings.TrimSpace(row.IssueID); label != "" {
		return label
	}
	return fmt.Sprintf("attempt-%d", row.AttemptID)
}

func workAttemptWorkerLabel(row telemetry.WorkAttempt) string {
	parts := []string{}
	if row.WorkerType != "" {
		parts = append(parts, row.WorkerType)
	}
	if row.WorkerHost != "" {
		parts = append(parts, row.WorkerHost)
	}
	if row.AttemptNumber > 0 {
		parts = append(parts, "attempt "+formatInt(int64(row.AttemptNumber)))
	}
	if len(parts) == 0 {
		return "worker pending"
	}
	return strings.Join(parts, " / ")
}

func workAttemptStatusClass(row telemetry.WorkAttempt) string {
	base := "shrink-0 rounded-full px-2 py-1 font-mono text-xs font-medium "
	if row.Stale {
		return base + "bg-err/15 text-err"
	}
	switch strings.TrimSpace(row.TerminalState) {
	case "success", "delivered":
		return base + "bg-ok/15 text-ok"
	case "failure", "timed_out", "abandoned", "cancelled":
		return base + "bg-err/15 text-err"
	}
	if strings.TrimSpace(row.Status) == "active" {
		return base + "bg-accent/15 text-accent"
	}
	return base + "bg-elev text-sec"
}

func workAttemptStatusLabel(row telemetry.WorkAttempt) string {
	if row.Stale {
		return "stale"
	}
	if state := strings.TrimSpace(row.TerminalState); state != "" {
		return state
	}
	if status := strings.TrimSpace(row.Status); status != "" {
		return status
	}
	return "unknown"
}

func workAttemptPhaseLabel(row telemetry.WorkAttempt) string {
	if value := strings.TrimSpace(row.Phase); value != "" {
		return value
	}
	return "pending"
}

func workAttemptWaitLabel(row telemetry.WorkAttempt) string {
	if value := strings.TrimSpace(row.WaitReason); value != "" {
		return value
	}
	return "none"
}

func workAttemptLeaseLabel(row telemetry.WorkAttempt, generatedAt time.Time) string {
	if row.LeaseExpiresAt == nil {
		if row.CompletedAt != nil {
			return "released"
		}
		return "none"
	}
	if generatedAt.IsZero() {
		return timeLabel(*row.LeaseExpiresAt)
	}
	if row.LeaseExpiresAt.Before(generatedAt) {
		return "expired " + formatDuration(generatedAt.Sub(*row.LeaseExpiresAt).Seconds()) + " ago"
	}
	return "expires in " + formatDuration(row.LeaseExpiresAt.Sub(generatedAt).Seconds())
}

func workAttemptNextActionLabel(row telemetry.WorkAttempt) string {
	if value := strings.TrimSpace(row.NextAction); value != "" {
		return value
	}
	return "none"
}

func workAttemptReceiptPath(row telemetry.WorkAttempt) string {
	projectID := strings.Trim(strings.TrimSpace(row.ProjectID), "/")
	if projectID == "" {
		projectID = "default"
	}
	return "/api/v1/projects/" + url.PathEscape(projectID) + "/work-attempts/" + strconv.FormatInt(row.AttemptID, 10)
}

func workAttemptRecoveryPath(row telemetry.WorkAttempt) string {
	return workAttemptReceiptPath(row) + "/recovery"
}

func workAttemptRecoveryFeedbackID(row telemetry.WorkAttempt) string {
	return "work-attempt-recovery-" + strconv.FormatInt(row.AttemptID, 10)
}

func workAttemptRecoveryTarget(row telemetry.WorkAttempt) string {
	return "#" + workAttemptRecoveryFeedbackID(row)
}

func workAttemptRecoveryControls(row telemetry.WorkAttempt) []workAttemptRecoveryControl {
	controls := []workAttemptRecoveryControl{}
	if strings.TrimSpace(row.Status) == "active" {
		return append(controls, workAttemptRecoveryControl{
			Action:         "abandon",
			Label:          "Abandon",
			Title:          "Mark this active attempt abandoned",
			Confirm:        "Mark this active attempt abandoned and clear live worker state?",
			ConfirmPayload: true,
		})
	}
	if strings.TrimSpace(row.Status) != "terminal" {
		return controls
	}
	if workAttemptRetryable(row) {
		controls = append(controls,
			workAttemptRecoveryControl{
				Action: "retry_fresh",
				Label:  "Retry",
				Title:  "Queue a fresh retry for this attempt",
			},
			workAttemptRecoveryControl{
				Action: "retry_resume",
				Label:  "Resume",
				Title:  "Queue a retry with resume when an eligible completed session exists",
			},
		)
	}
	return append(controls, workAttemptRecoveryControl{
		Action:         "cleanup_workspace",
		Label:          "Clean",
		Title:          "Rerun workspace cleanup for this attempt",
		Confirm:        "Rerun workspace cleanup? This can delete worktrees, branches, or processes.",
		ConfirmPayload: true,
	})
}

func workAttemptRetryable(row telemetry.WorkAttempt) bool {
	switch strings.TrimSpace(row.TerminalState) {
	case "failure", "cancelled", "timed_out", "abandoned", "no_progress":
		return true
	default:
		return false
	}
}

func workAttemptRecoveryConfirmAttributes(control workAttemptRecoveryControl) templ.Attributes {
	if strings.TrimSpace(control.Confirm) == "" {
		return templ.Attributes{}
	}
	return templ.Attributes{"hx-confirm": control.Confirm}
}

func workAttemptRecoveryButtonClass(control workAttemptRecoveryControl) string {
	base := "inline-flex min-h-8 items-center rounded-md border px-2.5 py-1 text-xs font-medium transition-colors "
	switch control.Action {
	case "abandon", "cleanup_workspace":
		return base + "border-err/30 bg-err/10 text-err hover:bg-err/15"
	default:
		return base + "border-line bg-surface text-text hover:bg-elev"
	}
}

func schedulerDecisionIssueLabel(row telemetry.SchedulerDecision) string {
	if label := strings.TrimSpace(row.Identifier); label != "" {
		return label
	}
	if label := strings.TrimSpace(row.IssueID); label != "" {
		return label
	}
	return "candidate"
}

func schedulerDecisionLaneLabel(row telemetry.SchedulerDecision) string {
	parts := []string{}
	if row.Lane != "" {
		parts = append(parts, row.Lane)
	}
	if row.WorkerHost != "" {
		parts = append(parts, row.WorkerHost)
	}
	if row.Retry {
		parts = append(parts, "retry")
	}
	if len(parts) == 0 {
		return "lane unknown"
	}
	return strings.Join(parts, " / ")
}

func schedulerDecisionStatusClass(row telemetry.SchedulerDecision) string {
	base := "shrink-0 rounded-full px-2 py-1 font-mono text-xs font-medium "
	if row.Selected || strings.TrimSpace(row.Result) == "selected" {
		return base + "bg-ok/15 text-ok"
	}
	return base + "bg-warn/15 text-warn"
}

func schedulerDecisionStatusLabel(row telemetry.SchedulerDecision) string {
	if result := strings.TrimSpace(row.Result); result != "" {
		return result
	}
	if row.Selected {
		return "selected"
	}
	return "skipped"
}

func schedulerDecisionReasonLabel(row telemetry.SchedulerDecision) string {
	if reason := strings.TrimSpace(row.Reason); reason != "" {
		return reason
	}
	return schedulerDecisionStatusLabel(row)
}

func schedulerDecisionWaitLabel(row telemetry.SchedulerDecision) string {
	if wait := strings.TrimSpace(row.WaitReason); wait != "" {
		return wait
	}
	return "none"
}

func schedulerDecisionQueueLabel(row telemetry.SchedulerDecision) string {
	if row.QueuePosition <= 0 {
		return "not queued"
	}
	return "#" + formatInt(int64(row.QueuePosition))
}

func schedulerDecisionTimeLabel(row telemetry.SchedulerDecision) string {
	if row.DecisionAt.IsZero() {
		return "unknown"
	}
	return timeLabel(row.DecisionAt)
}

func formatDecimal(value float64) string {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return "0"
	}

	rounded := math.Round(value*10) / 10
	if math.Abs(rounded-math.Round(rounded)) < 0.000001 {
		return formatInt(int64(math.Round(rounded)))
	}
	return strconv.FormatFloat(rounded, 'f', 1, 64)
}

func formatLimit(value int64) string {
	if value <= 0 {
		return "n/a"
	}
	return formatInt(value)
}

func resetLabel(bucket *telemetry.RateLimitBucket) string {
	if bucket.ResetAt != nil {
		return localTimeToken(*bucket.ResetAt, LocalTimeOnly)
	}
	if bucket.ResetInSeconds > 0 {
		return formatDuration(float64(bucket.ResetInSeconds))
	}
	return "n/a"
}

func usedPercent(bucket *telemetry.RateLimitBucket) int {
	if bucket.Limit > 0 {
		return int(math.Round(float64(bucket.Used) / float64(bucket.Limit) * 100))
	}
	total := bucket.Used + bucket.Remaining
	if total > 0 {
		return int(math.Round(float64(bucket.Used) / float64(total) * 100))
	}
	return 0
}

func tokenTrendPoints(snapshot telemetry.Snapshot) []telemetry.TokenTrendPoint {
	if len(snapshot.TokenTrend) > 0 {
		points := make([]telemetry.TokenTrendPoint, 0, len(snapshot.TokenTrend))
		for _, point := range snapshot.TokenTrend {
			if point.Input <= 0 && point.Output <= 0 && point.Total <= 0 {
				continue
			}
			if point.Total <= 0 {
				point.Total = point.Input + point.Output
			}
			points = append(points, point)
		}
		return points
	}

	if snapshot.Tokens.Input <= 0 && snapshot.Tokens.Output <= 0 && snapshot.Tokens.Total <= 0 {
		return nil
	}
	return []telemetry.TokenTrendPoint{
		{
			At:     snapshot.GeneratedAt,
			Input:  snapshot.Tokens.Input,
			Output: snapshot.Tokens.Output,
			Total:  snapshot.Tokens.Total,
		},
	}
}

func tokenTrendLabel(point telemetry.TokenTrendPoint) string {
	if point.At.IsZero() {
		return "Latest"
	}
	return localTimeToken(point.At, LocalTimeOnly)
}
