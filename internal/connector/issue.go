package connector

import (
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/workpad"
)

type AuthorAssociation string

const (
	AuthorAssociationOwner                AuthorAssociation = "OWNER"
	AuthorAssociationMember               AuthorAssociation = "MEMBER"
	AuthorAssociationCollaborator         AuthorAssociation = "COLLABORATOR"
	AuthorAssociationContributor          AuthorAssociation = "CONTRIBUTOR"
	AuthorAssociationFirstTimeContributor AuthorAssociation = "FIRST_TIME_CONTRIBUTOR"
	AuthorAssociationNone                 AuthorAssociation = "NONE"
)

func NormalizeAuthorAssociation(value string) AuthorAssociation {
	return AuthorAssociation(strings.ToUpper(strings.TrimSpace(value)))
}

func (a AuthorAssociation) Valid() bool {
	switch a {
	case AuthorAssociationOwner,
		AuthorAssociationMember,
		AuthorAssociationCollaborator,
		AuthorAssociationContributor,
		AuthorAssociationFirstTimeContributor,
		AuthorAssociationNone:
		return true
	default:
		return false
	}
}

type Issue struct {
	ID                string               `json:"id,omitempty" yaml:"id,omitempty"`
	Identifier        string               `json:"identifier,omitempty" yaml:"identifier,omitempty"`
	Number            int                  `json:"number,omitempty" yaml:"number,omitempty"`
	Title             string               `json:"title,omitempty" yaml:"title,omitempty"`
	Description       string               `json:"description,omitempty" yaml:"description,omitempty"`
	Priority          *int                 `json:"priority,omitempty" yaml:"priority,omitempty"`
	PriorityName      string               `json:"priority_name,omitempty" yaml:"priority_name,omitempty"`
	UnblockerCount    int                  `json:"unblocker_count,omitempty" yaml:"unblocker_count,omitempty"`
	State             string               `json:"state,omitempty" yaml:"state,omitempty"`
	BranchName        string               `json:"branch_name,omitempty" yaml:"branch_name,omitempty"`
	URL               string               `json:"url,omitempty" yaml:"url,omitempty"`
	Closed            bool                 `json:"closed,omitempty" yaml:"closed,omitempty"`
	ClosedReason      string               `json:"closed_reason,omitempty" yaml:"closed_reason,omitempty"`
	PRNumber          *int                 `json:"pr_number,omitempty" yaml:"pr_number,omitempty"`
	PRRepository      string               `json:"pr_repository,omitempty" yaml:"pr_repository,omitempty"`
	PRSource          string               `json:"pr_association_source,omitempty" yaml:"pr_association_source,omitempty"`
	PRVerifiedAt      time.Time            `json:"pr_association_checked_at,omitzero" yaml:"pr_association_checked_at,omitempty"`
	PullRequest       *PullRequest         `json:"pull_request,omitempty" yaml:"pull_request,omitempty"`
	AuthorID          string               `json:"author_id,omitempty" yaml:"author_id,omitempty"`
	AuthorAssociation AuthorAssociation    `json:"author_association,omitempty" yaml:"author_association,omitempty"`
	AssigneeID        string               `json:"assignee_id,omitempty" yaml:"assignee_id,omitempty"`
	Assignees         []string             `json:"assignees,omitempty" yaml:"assignees,omitempty"`
	BlockedBy         []BlockedRef         `json:"blocked_by" yaml:"blocked_by"`
	ChildIssues       []BlockedRef         `json:"child_issues,omitempty" yaml:"child_issues,omitempty"`
	BlockerReason     string               `json:"blocker_reason,omitempty" yaml:"blocker_reason,omitempty"`
	WorkpadSignal     *workpad.Signal      `json:"workpad_signal,omitempty" yaml:"workpad_signal,omitempty"`
	Labels            []string             `json:"labels" yaml:"labels"`
	Comments          []IssueComment       `json:"comments,omitempty" yaml:"comments,omitempty"`
	Fields            map[string]string    `json:"fields,omitempty" yaml:"fields,omitempty"`
	FieldUpdatedAt    map[string]time.Time `json:"field_updated_at,omitempty" yaml:"field_updated_at,omitempty"`
	Metadata          map[string]string    `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Deliverable       *Deliverable         `json:"deliverable,omitempty" yaml:"deliverable,omitempty"`
	AssignedToWorker  bool                 `json:"assigned_to_worker" yaml:"assigned_to_worker"`
	CreatedAt         *time.Time           `json:"created_at,omitempty" yaml:"created_at,omitempty"`
	UpdatedAt         *time.Time           `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
	StageUpdatedAt    *time.Time           `json:"stage_updated_at,omitempty" yaml:"stage_updated_at,omitempty"`
	StageUpdatedActor IssueActor           `json:"stage_updated_actor,omitzero" yaml:"stage_updated_actor,omitempty"`
	ModelOverride     string               `json:"model_override" yaml:"model_override"`
}

type BlockedRef struct {
	HumanOwned           bool   `json:"human_owned,omitempty" yaml:"human_owned,omitempty"`
	HumanCompletionReady bool   `json:"human_completion_ready,omitempty" yaml:"human_completion_ready,omitempty"`
	ID                   string `json:"id,omitempty" yaml:"id,omitempty"`
	Identifier           string `json:"identifier" yaml:"identifier"`
	State                string `json:"state,omitempty" yaml:"state,omitempty"`
	TrackerState         string `json:"tracker_state,omitempty" yaml:"-"`
	Source               string `json:"source,omitempty" yaml:"source,omitempty"`
}

const (
	BlockedRefSourceNative       = "native"
	BlockedRefSourceProse        = "prose"
	BlockedRefSourceWorkpad      = "workpad"
	BlockedRefTrackerStateOpen   = "open"
	BlockedRefTrackerStateClosed = "closed"
)

type DependencyCapability struct {
	Repository      string `json:"repository,omitempty" yaml:"repository,omitempty"`
	NativeBlockedBy string `json:"native_blocked_by,omitempty" yaml:"native_blocked_by,omitempty"`
	Source          string `json:"source,omitempty" yaml:"source,omitempty"`
	Detail          string `json:"detail,omitempty" yaml:"detail,omitempty"`
}

type DependencyCapabilityReporter interface {
	DependencyCapabilities() []DependencyCapability
}

type PullRequest struct {
	NodeID                       string                      `json:"node_id,omitempty" yaml:"node_id,omitempty"`
	Number                       int                         `json:"number,omitempty" yaml:"number,omitempty"`
	URL                          string                      `json:"url,omitempty" yaml:"url,omitempty"`
	BranchName                   string                      `json:"branch_name,omitempty" yaml:"branch_name,omitempty"`
	BaseRef                      string                      `json:"base_ref,omitempty" yaml:"base_ref,omitempty"`
	State                        string                      `json:"state,omitempty" yaml:"state,omitempty"`
	MergeableState               string                      `json:"mergeable_state,omitempty" yaml:"mergeable_state,omitempty"`
	Draft                        bool                        `json:"draft,omitempty" yaml:"draft,omitempty"`
	Labels                       []string                    `json:"labels,omitempty" yaml:"labels,omitempty"`
	ActivityAt                   *time.Time                  `json:"activity_at,omitempty" yaml:"activity_at,omitempty"`
	HeadSHA                      string                      `json:"head_sha,omitempty" yaml:"head_sha,omitempty"`
	BaseSHA                      string                      `json:"base_sha,omitempty" yaml:"base_sha,omitempty"`
	DiffFingerprint              string                      `json:"diff_fingerprint,omitempty" yaml:"diff_fingerprint,omitempty"`
	HydrationUnavailableReason   string                      `json:"hydration_unavailable_reason,omitempty" yaml:"hydration_unavailable_reason,omitempty"`
	HydrationDegradedReason      string                      `json:"hydration_degraded_reason,omitempty" yaml:"hydration_degraded_reason,omitempty"`
	HydrationNextRetryAt         *time.Time                  `json:"hydration_next_retry_at,omitempty" yaml:"hydration_next_retry_at,omitempty"`
	CIStatus                     string                      `json:"ci_status,omitempty" yaml:"ci_status,omitempty"`
	Checks                       []PullRequestCheck          `json:"checks,omitempty" yaml:"checks,omitempty"`
	CheckRunCount                int                         `json:"check_run_count,omitempty" yaml:"check_run_count,omitempty"`
	StatusContextCount           int                         `json:"status_context_count,omitempty" yaml:"status_context_count,omitempty"`
	CIQueueSeconds               int64                       `json:"ci_queue_seconds,omitempty" yaml:"ci_queue_seconds,omitempty"`
	CIDurationSeconds            int64                       `json:"ci_duration_seconds,omitempty" yaml:"ci_duration_seconds,omitempty"`
	SlowChecks                   []PullRequestCheck          `json:"slow_checks,omitempty" yaml:"slow_checks,omitempty"`
	RunningChecks                []string                    `json:"running_checks,omitempty" yaml:"running_checks,omitempty"`
	UnstartedCheckCount          int                         `json:"unstarted_check_count,omitempty" yaml:"unstarted_check_count,omitempty"`
	UnstartedChecks              []PullRequestCheck          `json:"unstarted_checks,omitempty" yaml:"unstarted_checks,omitempty"`
	StaleSuccessfulChecks        []PullRequestCheck          `json:"stale_successful_checks,omitempty" yaml:"stale_successful_checks,omitempty"`
	RequiredCheckFailures        []PullRequestCheck          `json:"required_check_failures,omitempty" yaml:"required_check_failures,omitempty"`
	TransientFailedChecks        []PullRequestCheck          `json:"transient_failed_checks,omitempty" yaml:"transient_failed_checks,omitempty"`
	UnresolvedReviewThreads      []PullRequestReviewThread   `json:"unresolved_review_threads,omitempty" yaml:"unresolved_review_threads,omitempty"`
	CodexReviewState             string                      `json:"codex_review_state,omitempty" yaml:"codex_review_state,omitempty"`
	CodexReviewSource            string                      `json:"codex_review_source,omitempty" yaml:"codex_review_source,omitempty"`
	CodexReviewAPIState          string                      `json:"codex_review_api_state,omitempty" yaml:"codex_review_api_state,omitempty"`
	CodexReviewBodySeverity      string                      `json:"codex_review_body_severity,omitempty" yaml:"codex_review_body_severity,omitempty"`
	CodexReviewSubmittedAt       *time.Time                  `json:"codex_review_submitted_at,omitempty" yaml:"codex_review_submitted_at,omitempty"`
	CodexReviewFindings          []PullRequestFinding        `json:"codex_review_findings,omitempty" yaml:"codex_review_findings,omitempty"`
	LatestCodexReviewState       string                      `json:"latest_codex_review_state,omitempty" yaml:"latest_codex_review_state,omitempty"`
	LatestCodexReviewCommitSHA   string                      `json:"latest_codex_review_commit_sha,omitempty" yaml:"latest_codex_review_commit_sha,omitempty"`
	LatestCodexReviewSubmittedAt *time.Time                  `json:"latest_codex_review_submitted_at,omitempty" yaml:"latest_codex_review_submitted_at,omitempty"`
	MergeQueueEntry              *PullRequestMergeQueueEntry `json:"merge_queue_entry,omitempty" yaml:"merge_queue_entry,omitempty"`
}

const (
	PullRequestReviewSourceFormal         = "pull_request_review"
	PullRequestReviewSourceSummaryComment = "summary_comment"
)

const (
	PullRequestHydrationReasonRateLimited         = "rate_limited"
	PullRequestHydrationReasonSecondaryThrottled  = "secondary_throttled"
	PullRequestHydrationReasonPrimaryExhausted    = "primary_exhausted"
	PullRequestHydrationReasonRESTFanoutDeferred  = "rest_fanout_deferred"
	PullRequestHydrationReasonRESTBudgetReserved  = "rest_budget_reserved"
	PullRequestHydrationReasonChecksUnavailable   = "checks_unavailable"
	PullRequestHydrationReasonStaleCachedPullData = "stale_cached_pull_request"
)

type PullRequestCheck struct {
	FailureDetail   string `json:"failure_detail,omitempty" yaml:"failure_detail,omitempty"`
	ID              int64  `json:"id,omitempty" yaml:"id,omitempty"`
	WorkflowRunID   int64  `json:"workflow_run_id,omitempty" yaml:"workflow_run_id,omitempty"`
	Name            string `json:"name,omitempty" yaml:"name,omitempty"`
	Status          string `json:"status,omitempty" yaml:"status,omitempty"`
	Conclusion      string `json:"conclusion,omitempty" yaml:"conclusion,omitempty"`
	DetailsURL      string `json:"details_url,omitempty" yaml:"details_url,omitempty"`
	QueueSeconds    int64  `json:"queue_seconds,omitempty" yaml:"queue_seconds,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty" yaml:"duration_seconds,omitempty"`
}

type PullRequestFinding struct {
	Body string `json:"body,omitempty" yaml:"body,omitempty"`
	URL  string `json:"url,omitempty" yaml:"url,omitempty"`
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	Line int    `json:"line,omitempty" yaml:"line,omitempty"`
}

type PullRequestReviewThread struct {
	Body string `json:"body,omitempty" yaml:"body,omitempty"`
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	Line int    `json:"line,omitempty" yaml:"line,omitempty"`
}

const (
	IssueCommentTargetIssue       = "issue"
	IssueCommentTargetPullRequest = "pull_request"
)

type IssueComment struct {
	ID                string     `json:"id,omitempty" yaml:"id,omitempty"`
	Backend           string     `json:"backend,omitempty" yaml:"backend,omitempty"`
	Body              string     `json:"body,omitempty" yaml:"body,omitempty"`
	URL               string     `json:"url,omitempty" yaml:"url,omitempty"`
	AuthorLogin       string     `json:"author_login,omitempty" yaml:"author_login,omitempty"`
	AuthorKind        string     `json:"author_kind,omitempty" yaml:"author_kind,omitempty"`
	AuthorDisplayName string     `json:"author_display_name,omitempty" yaml:"author_display_name,omitempty"`
	AuthorAuthorized  bool       `json:"author_authorized,omitempty" yaml:"author_authorized,omitempty"`
	CreatedAt         *time.Time `json:"created_at,omitempty" yaml:"created_at,omitempty"`
	UpdatedAt         *time.Time `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
	Local             bool       `json:"local,omitempty" yaml:"local,omitempty"`
	CanEdit           bool       `json:"can_edit,omitempty" yaml:"can_edit,omitempty"`
	CanDelete         bool       `json:"can_delete,omitempty" yaml:"can_delete,omitempty"`
	TargetType        string     `json:"target_type,omitempty" yaml:"target_type,omitempty"`
}

type IssueEvent struct {
	ID        string            `json:"id,omitempty" yaml:"id,omitempty"`
	Kind      string            `json:"kind,omitempty" yaml:"kind,omitempty"`
	State     string            `json:"state,omitempty" yaml:"state,omitempty"`
	Body      string            `json:"body,omitempty" yaml:"body,omitempty"`
	Actor     IssueActor        `json:"actor,omitzero" yaml:"actor,omitempty"`
	Fields    map[string]string `json:"fields,omitempty" yaml:"fields,omitempty"`
	CreatedAt *time.Time        `json:"created_at,omitempty" yaml:"created_at,omitempty"`
}

type Deliverable struct {
	Kind             string            `json:"kind,omitempty" yaml:"kind,omitempty"`
	Path             string            `json:"path,omitempty" yaml:"path,omitempty"`
	ReviewURL        string            `json:"review_url,omitempty" yaml:"review_url,omitempty"`
	ValidationStatus string            `json:"validation_status,omitempty" yaml:"validation_status,omitempty"`
	ExternalID       string            `json:"external_id,omitempty" yaml:"external_id,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

func NewIssue() Issue {
	return Issue{
		BlockedBy:        []BlockedRef{},
		Labels:           []string{},
		Assignees:        []string{},
		Fields:           map[string]string{},
		FieldUpdatedAt:   map[string]time.Time{},
		Metadata:         map[string]string{},
		AssignedToWorker: true,
	}
}

func (i *Issue) UnmarshalYAML(value *yaml.Node) error {
	type issue Issue

	defaults := issue(NewIssue())
	if err := value.Decode(&defaults); err != nil {
		return err
	}

	*i = Issue(defaults)
	return nil
}
