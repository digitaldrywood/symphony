package github

import (
	"regexp"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
)

const (
	projectItemsPageSize                      = 100
	projectCandidatePageLimit                 = 100
	projectItemsPerIssue                      = 100
	projectItemFieldValuesPageSize            = 100
	linkedIssuePageSize                       = 20
	linkedIssueProjectItemsPageSize           = 10
	linkedIssueProjectItemFieldValuesPageSize = 20
	bodyParentSearchPageSize                  = 100
	pullRequestsPageSize                      = 100
	pullRequestsPageLimit                     = 3
	pullRequestSlowCheckLimit                 = 3
	pullRequestRunningCheckLimit              = 5
	pullRequestUnstartedCheckLimit            = 5
	defaultUnstartedCheckThreshold            = 15 * time.Minute
	defaultProjectItemStatusState             = "Backlog"
	defaultProjectItemStatusWriteParallelism  = 4
	defaultProjectItemStatusWriteTimeout      = 2 * time.Minute
)

var (
	modelOverridePattern = regexp.MustCompile(`(?i)<!--\s*model:\s*(\S+?)\s*-->`)
	issueRefPattern      = regexp.MustCompile(`(?:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+))?#(\d+)`)
	issueURLPattern      = regexp.MustCompile(`https?://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/issues/(\d+)`)
	numberedListPattern  = regexp.MustCompile(`^\d+[.)]\s+`)
	branchKeyPattern     = regexp.MustCompile(`[^A-Za-z0-9._-]`)
	actionRunURLPattern  = regexp.MustCompile(`/actions/runs/([0-9]+)(?:/|$)`)
)

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type projectItemsConnection struct {
	TotalCount int               `json:"totalCount"`
	PageInfo   pageInfo          `json:"pageInfo"`
	Nodes      []projectItemNode `json:"nodes"`
}

type projectItemNode struct {
	ID            string                            `json:"id"`
	Content       *githubIssueNode                  `json:"content"`
	Project       *projectRef                       `json:"project"`
	StatusValue   *singleSelectValue                `json:"statusValue"`
	PriorityValue *singleSelectValue                `json:"priorityValue"`
	FieldValues   nodeConnection[projectFieldValue] `json:"fieldValues"`
}

type githubIssueNode struct {
	TypeName                       string                       `json:"__typename"`
	ID                             string                       `json:"id"`
	Number                         int                          `json:"number"`
	Title                          string                       `json:"title"`
	Body                           string                       `json:"body"`
	State                          string                       `json:"state"`
	StateReason                    string                       `json:"stateReason"`
	URL                            string                       `json:"url"`
	CreatedAt                      *string                      `json:"createdAt"`
	UpdatedAt                      *string                      `json:"updatedAt"`
	Author                         *actor                       `json:"author"`
	AuthorAssociation              string                       `json:"authorAssociation"`
	Assignees                      nodeConnection[assignee]     `json:"assignees"`
	Labels                         nodeConnection[label]        `json:"labels"`
	Comments                       nodeConnection[issueComment] `json:"comments"`
	Repository                     repository                   `json:"repository"`
	ClosedByPullRequestsReferences nodeConnection[pullRequest]  `json:"closedByPullRequestsReferences"`
	TimelineItems                  nodeConnection[timelineItem] `json:"timelineItems"`
	ProjectItems                   *projectItemsConnection      `json:"projectItems"`
	SubIssues                      linkedIssuesConnection       `json:"subIssues"`
	TrackedIssues                  linkedIssuesConnection       `json:"trackedIssues"`
}

type timelineItem struct {
	TypeName  string  `json:"__typename"`
	CreatedAt *string `json:"createdAt"`
	Label     *label  `json:"label"`
	Actor     *actor  `json:"actor"`
}

type linkedIssuesConnection struct {
	PageInfo pageInfo      `json:"pageInfo"`
	Nodes    []linkedIssue `json:"nodes"`
}

type issueNodesConnection struct {
	PageInfo pageInfo          `json:"pageInfo"`
	Nodes    []githubIssueNode `json:"nodes"`
}

type issueParentsNode struct {
	ID              string               `json:"id"`
	Number          int                  `json:"number"`
	Repository      repository           `json:"repository"`
	Parent          *githubIssueNode     `json:"parent"`
	TrackedInIssues issueNodesConnection `json:"trackedInIssues"`
}

type linkedIssue struct {
	ID           string                  `json:"id"`
	Number       int                     `json:"number"`
	Title        string                  `json:"title"`
	State        string                  `json:"state"`
	URL          string                  `json:"url"`
	Labels       nodeConnection[label]   `json:"labels"`
	Repository   repository              `json:"repository"`
	ProjectItems *projectItemsConnection `json:"projectItems"`
}

type nodeConnection[T any] struct {
	PageInfo pageInfo `json:"pageInfo"`
	Nodes    []T      `json:"nodes"`
}

type assignee struct {
	ID    string `json:"id"`
	Login string `json:"login"`
}

type label struct {
	Name string `json:"name"`
}

type issueComment struct {
	ID                string  `json:"id"`
	Body              string  `json:"body"`
	URL               string  `json:"url"`
	Author            *actor  `json:"author"`
	AuthorAssociation string  `json:"authorAssociation"`
	CreatedAt         *string `json:"createdAt"`
	UpdatedAt         *string `json:"updatedAt"`
}

type pullRequest struct {
	Number     int        `json:"number"`
	URL        string     `json:"url"`
	State      string     `json:"state"`
	UpdatedAt  *string    `json:"updatedAt"`
	Repository repository `json:"repository"`
}

type pullRequestNode struct {
	NodeID                     string                              `json:"id"`
	Number                     int                                 `json:"number"`
	URL                        string                              `json:"url"`
	State                      string                              `json:"state"`
	MergeableState             string                              `json:"mergeableState"`
	Draft                      bool                                `json:"draft"`
	Labels                     []string                            `json:"labels"`
	ActivityAt                 *time.Time                          `json:"activityAt"`
	HeadRefName                string                              `json:"headRefName"`
	BaseRefName                string                              `json:"baseRefName"`
	HeadSHA                    string                              `json:"headSHA"`
	BaseSHA                    string                              `json:"baseRefOid"`
	HydrationUnavailableReason string                              `json:"-"`
	HydrationDegradedReason    string                              `json:"-"`
	HydrationNextRetryAt       *time.Time                          `json:"-"`
	Commits                    nodeConnection[pullRequestCommit]   `json:"commits"`
	LatestReviews              nodeConnection[pullRequestReview]   `json:"latestReviews"`
	UnresolvedReviewThreads    []connector.PullRequestReviewThread `json:"-"`
	CodexReviews               pullRequestCodexReviews             `json:"-"`
	CI                         pullRequestCI                       `json:"-"`
}

type pullRequestCommit struct {
	Commit commitNode `json:"commit"`
}

type commitNode struct {
	StatusCheckRollup *statusCheckRollup `json:"statusCheckRollup"`
}

type statusCheckRollup struct {
	State string `json:"state"`
}

type pullRequestReview struct {
	Body        string     `json:"body"`
	URL         string     `json:"url"`
	State       string     `json:"state"`
	Source      string     `json:"-"`
	Author      *actor     `json:"author"`
	CommitID    string     `json:"commitId"`
	SubmittedAt *time.Time `json:"submittedAt"`
}

type pullRequestReviewThread struct {
	Comments nodeConnection[struct {
		Body string `json:"body"`
	}] `json:"comments"`
	IsResolved   bool   `json:"isResolved"`
	IsOutdated   bool   `json:"isOutdated"`
	Path         string `json:"path"`
	Line         int    `json:"line"`
	OriginalLine int    `json:"originalLine"`
}

type pullRequestCodexReviews struct {
	CurrentHead []pullRequestReview
	Latest      []pullRequestReview
}

type actor struct {
	Login    string `json:"login"`
	Type     string `json:"type"`
	TypeName string `json:"__typename"`
}

type restIssue struct {
	ID                int            `json:"id"`
	NodeID            string         `json:"node_id"`
	Number            int            `json:"number"`
	Title             string         `json:"title"`
	Body              *string        `json:"body"`
	State             string         `json:"state"`
	StateReason       string         `json:"state_reason"`
	HTMLURL           string         `json:"html_url"`
	CreatedAt         *time.Time     `json:"created_at"`
	UpdatedAt         *time.Time     `json:"updated_at"`
	User              *actor         `json:"user"`
	AuthorAssociation string         `json:"author_association"`
	Assignees         []restAssignee `json:"assignees"`
	Labels            []label        `json:"labels"`
	PullRequest       *struct{}      `json:"pull_request"`
}

type restIssueSearchResponse struct {
	TotalCount int         `json:"total_count"`
	Items      []restIssue `json:"items"`
}

type restRepository struct {
	FullName      string          `json:"full_name"`
	DefaultBranch string          `json:"default_branch"`
	Permissions   map[string]bool `json:"permissions"`
}

type restIssueDependency struct {
	Body          string     `json:"body"`
	Labels        []label    `json:"labels"`
	ID            int        `json:"id"`
	NodeID        string     `json:"node_id"`
	Number        int        `json:"number"`
	Title         string     `json:"title"`
	State         string     `json:"state"`
	StateReason   string     `json:"state_reason"`
	HTMLURL       string     `json:"html_url"`
	URL           string     `json:"url"`
	RepositoryURL string     `json:"repository_url"`
	UpdatedAt     *time.Time `json:"updated_at"`
}

type nativeDependencyCapability struct {
	Status string
	Detail string
}

type restAssignee struct {
	NodeID string `json:"node_id"`
	Login  string `json:"login"`
}

type restPullRequest struct {
	NodeID         string     `json:"node_id"`
	Number         int        `json:"number"`
	Title          string     `json:"title"`
	Body           *string    `json:"body"`
	HTMLURL        string     `json:"html_url"`
	State          string     `json:"state"`
	MergeableState string     `json:"mergeable_state"`
	Draft          bool       `json:"draft"`
	Labels         []label    `json:"labels"`
	Head           restHead   `json:"head"`
	Base           restHead   `json:"base"`
	UpdatedAt      *time.Time `json:"updated_at"`
	MergedAt       *string    `json:"merged_at"`
}

type restPullRequestFile struct {
	Filename         string `json:"filename"`
	PreviousFilename string `json:"previous_filename"`
	Status           string `json:"status"`
	SHA              string `json:"sha"`
	Additions        int    `json:"additions"`
	Deletions        int    `json:"deletions"`
	Changes          int    `json:"changes"`
	Patch            string `json:"patch"`
}

type restPullRequestMergeResponse struct {
	SHA     string `json:"sha"`
	Merged  bool   `json:"merged"`
	Message string `json:"message"`
}

type restHead struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type restReview struct {
	Body        string     `json:"body"`
	HTMLURL     string     `json:"html_url"`
	State       string     `json:"state"`
	User        *actor     `json:"user"`
	CommitID    string     `json:"commit_id"`
	SubmittedAt *time.Time `json:"submitted_at"`
}

type restCheckRuns struct {
	CheckRuns []restCheckRun `json:"check_runs"`
}

type restCheckRun struct {
	FailureDetail string         `json:"-"`
	ID            int64          `json:"id"`
	Status        string         `json:"status"`
	Conclusion    string         `json:"conclusion"`
	Name          string         `json:"name"`
	DetailsURL    string         `json:"details_url"`
	HTMLURL       string         `json:"html_url"`
	Output        checkRunOutput `json:"output"`
	CreatedAt     *time.Time     `json:"created_at"`
	StartedAt     *time.Time     `json:"started_at"`
	CompletedAt   *time.Time     `json:"completed_at"`
}

type checkRunOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Text    string `json:"text"`
}

type restCheckRunAnnotation struct {
	Path            string `json:"path"`
	AnnotationLevel string `json:"annotation_level"`
	Message         string `json:"message"`
	RawDetails      string `json:"raw_details"`
}

type restWorkflowRun struct {
	ID           int64      `json:"id"`
	CreatedAt    *time.Time `json:"created_at"`
	RunStartedAt *time.Time `json:"run_started_at"`
}

type restCommitStatus struct {
	Context   string     `json:"context"`
	State     string     `json:"state"`
	CreatedAt *time.Time `json:"created_at"`
}

type pullRequestCI struct {
	State                 string
	Checks                []connector.PullRequestCheck
	CheckRunCount         int
	StatusContextCount    int
	CIQueueSeconds        int64
	CIDurationSeconds     int64
	SlowChecks            []connector.PullRequestCheck
	RunningChecks         []string
	UnstartedCheckCount   int
	UnstartedChecks       []connector.PullRequestCheck
	StaleSuccessfulChecks []connector.PullRequestCheck
	RequiredFailures      []connector.PullRequestCheck
	TransientFailures     []connector.PullRequestCheck
}

type restComment struct {
	ID                int64      `json:"id"`
	NodeID            string     `json:"node_id"`
	Body              string     `json:"body"`
	HTMLURL           string     `json:"html_url"`
	User              *actor     `json:"user"`
	AuthorAssociation string     `json:"author_association"`
	CreatedAt         *time.Time `json:"created_at"`
	UpdatedAt         *time.Time `json:"updated_at"`
}

type restCollaboratorPermission struct {
	Permission string `json:"permission"`
}

type restIssueTimelineEvent struct {
	Event     string     `json:"event"`
	CreatedAt *time.Time `json:"created_at"`
	Label     *label     `json:"label"`
	Actor     *actor     `json:"actor"`
}

type repository struct {
	NameWithOwner string `json:"nameWithOwner"`
}

type projectRef struct {
	ID string `json:"id"`
}

type singleSelectValue struct {
	Name      string  `json:"name"`
	UpdatedAt *string `json:"updatedAt"`
}

type projectFieldValue struct {
	TypeName  string       `json:"__typename"`
	Field     projectField `json:"field"`
	Name      string       `json:"name"`
	Text      string       `json:"text"`
	Number    *float64     `json:"number"`
	UpdatedAt *string      `json:"updatedAt"`
}

type projectField struct {
	Name string `json:"name"`
}
