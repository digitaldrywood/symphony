package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/selector"
)

func TestPullRequestCodexReviewStateFromReviewsUsesExplicitBodySeverity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		api  string
		want string
	}{
		{
			name: "approved narrative p1 remains approved",
			body: "No P1 issues found — approved.",
			api:  "APPROVED",
			want: "APPROVED",
		},
		{
			name: "approved bracket p1 escalates",
			body: "[P1] Missing acceptance coverage.",
			api:  "APPROVED",
			want: "P1",
		},
		{
			name: "commented line anchored p2 escalates",
			body: "P2: Minor follow-up.",
			api:  "COMMENTED",
			want: "P2",
		},
		{
			name: "mid sentence p1 remains api state",
			body: "The P1 fix from last week is already present.",
			api:  "APPROVED",
			want: "APPROVED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := pullRequestCodexReviewStateFromReviews([]pullRequestReview{{
				Body:  tt.body,
				State: tt.api,
			}})
			if got != tt.want {
				t.Fatalf("pullRequestCodexReviewStateFromReviews() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPullRequestCodexReviewStateInputsFromReviews(t *testing.T) {
	t.Parallel()

	apiState, bodySeverity := pullRequestCodexReviewStateInputsFromReviews([]pullRequestReview{{
		Body:  "[P1] Missing acceptance coverage.",
		State: "APPROVED",
	}})
	if apiState != "APPROVED" || bodySeverity != "P1" {
		t.Fatalf("state inputs = API %q body %q, want APPROVED/P1", apiState, bodySeverity)
	}
}

func TestPullRequestCodexReviewFindingsUseExplicitBodySeverity(t *testing.T) {
	t.Parallel()

	pullRequest := pullRequestNode{
		LatestReviews: nodeConnection[pullRequestReview]{Nodes: []pullRequestReview{
			{
				Body: "No P1 issues found — approved.",
				URL:  "https://github.test/review/narrative",
			},
			{
				Body: "[P1] Missing acceptance coverage.",
				URL:  "https://github.test/review/finding",
			},
		}},
	}

	got := pullRequestCodexReviewFindings(pullRequest)
	if len(got) != 1 || got[0].Body != "[P1] Missing acceptance coverage." || got[0].URL != "https://github.test/review/finding" {
		t.Fatalf("pullRequestCodexReviewFindings() = %#v, want only explicit P1 finding", got)
	}
}

func TestLatestCodexReviewRequiresExactCurrentHead(t *testing.T) {
	t.Parallel()

	headSHA := strings.Repeat("a", 40)
	tests := []struct {
		name     string
		commitID string
		want     bool
	}{
		{name: "exact head", commitID: headSHA, want: true},
		{name: "missing commit"},
		{name: "abbreviated commit", commitID: headSHA[:10]},
		{name: "stale commit", commitID: strings.Repeat("b", 40)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, got := latestCodexReview([]restReview{{
				State:    "COMMENTED",
				User:     &actor{Login: "chatgpt-codex-connector[bot]", Type: "Bot"},
				CommitID: tt.commitID,
			}}, headSHA)
			if got != tt.want {
				t.Fatalf("latestCodexReview() ok = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnectorFetchPullRequestReviewsUsesTrustedCurrentHeadSummary(t *testing.T) {
	t.Parallel()

	const headSHA = "79f9eb2d4ad5317af5ff46f29aba3b91f4b413a0"
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/gopherguides/gopher-ai/pulls/389/reviews?per_page=100",
			body:   `[{"body":"[P1] Prior-head finding.","html_url":"https://github.com/gopherguides/gopher-ai/pull/389#pullrequestreview-1","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]","type":"Bot"},"commit_id":"f236cc46fbb1a0e6821f16001e97ce07e0d483cd","submitted_at":"2026-09-02T22:18:56Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/gopherguides/gopher-ai/issues/389/comments?per_page=100",
			body:   `[{"node_id":"IC_summary","body":"<!-- codex-pull-request-review-summary -->\n\n## Codex Review Summary\n\n| Review | Status | Commit | Review trigger |\n| --- | --- | --- | --- |\n| 📝 **Code Review** | ✅ **Completed** <relative-time datetime=\"2026-09-02T22:35:58.561318Z\">2026-09-02T22:35:58.561318Z</relative-time> | ` + "`79f9eb2`" + ` | Manual request |","html_url":"https://github.com/gopherguides/gopher-ai/pull/389#issuecomment-1","user":{"login":"chatgpt-codex-connector[bot]","type":"Bot"},"created_at":"2026-09-02T16:27:27Z","updated_at":"2026-09-02T22:35:59Z"}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{})
	reviews, err := c.fetchPullRequestReviews(context.Background(), pullRequestRepo{Owner: "gopherguides", Name: "gopher-ai"}, 389, headSHA)
	if err != nil {
		t.Fatalf("fetchPullRequestReviews() error = %v", err)
	}
	if len(reviews.CurrentHead) != 1 {
		t.Fatalf("CurrentHead = %#v, want trusted summary review", reviews.CurrentHead)
	}
	if got := pullRequestCodexReviewStateFromReviews(reviews.CurrentHead); got != "COMMENTED" {
		t.Fatalf("current-head state = %q, want COMMENTED", got)
	}
	if got := reviews.CurrentHead[0].Source; got != connector.PullRequestReviewSourceSummaryComment {
		t.Fatalf("current-head source = %q, want %q", got, connector.PullRequestReviewSourceSummaryComment)
	}
	if got := reviewBodySeverity(reviews.CurrentHead[0].Body); got != "" {
		t.Fatalf("current-head severity = %q, want clean", got)
	}
	wantSubmittedAt := time.Date(2026, 9, 2, 22, 35, 58, 561318000, time.UTC)
	if got := reviews.CurrentHead[0].SubmittedAt; got == nil || !got.Equal(wantSubmittedAt) {
		t.Fatalf("current-head submitted at = %v, want %v", got, wantSubmittedAt)
	}
	if len(reviews.Latest) != 1 || reviews.Latest[0].CommitID != "f236cc46fbb1a0e6821f16001e97ce07e0d483cd" {
		t.Fatalf("Latest = %#v, want prior-head formal review preserved", reviews.Latest)
	}
	if got := pullRequestCodexReviewStateFromReviews(reviews.Latest); got != "P1" {
		t.Fatalf("latest formal state = %q, want stale P1 to remain observable", got)
	}
}

func TestLatestCodexSummaryReviewFailsClosed(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 9, 2, 22, 35, 58, 561318000, time.UTC)
	createdAt := completedAt.Add(-time.Hour)
	updatedAt := completedAt.Add(time.Second)
	headSHA := strings.Repeat("a", 40)
	trustedAuthor := &actor{Login: "chatgpt-codex-connector[bot]", Type: "Bot"}
	validComment := restComment{
		ID:        1,
		Body:      testCodexReviewSummaryBody("✅ **Completed** <relative-time datetime=\"2026-09-02T22:35:58.561318Z\">2026-09-02T22:35:58.561318Z</relative-time>", headSHA[:7]),
		HTMLURL:   "https://github.test/pull/1#issuecomment-1",
		User:      trustedAuthor,
		CreatedAt: &createdAt,
		UpdatedAt: &updatedAt,
	}

	tests := []struct {
		name          string
		comments      []restComment
		formalReviews []restReview
		want          bool
	}{
		{name: "trusted completed current head", comments: []restComment{validComment}, want: true},
		{
			name: "untrusted user",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.User = &actor{Login: "chatgpt-codex-connector[bot]", Type: "User"}
				return comment
			}()},
		},
		{
			name: "untrusted bot",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.User = &actor{Login: "codex-helper[bot]", Type: "Bot"}
				return comment
			}()},
		},
		{
			name: "stale head",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.Body = testCodexReviewSummaryBody("✅ **Completed** <relative-time datetime=\"2026-09-02T22:35:58.561318Z\">2026-09-02T22:35:58.561318Z</relative-time>", strings.Repeat("b", 7))
				return comment
			}()},
		},
		{
			name: "incomplete review",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.Body = testCodexReviewSummaryBody("🔄 **In progress**", headSHA[:7])
				return comment
			}()},
		},
		{
			name: "malformed table",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.Body = strings.Replace(comment.Body, codexReviewSummaryTableSeparator, "| --- | --- |", 1)
				return comment
			}()},
		},
		{
			name: "short commit prefix",
			comments: []restComment{func() restComment {
				comment := validComment
				comment.Body = testCodexReviewSummaryBody("✅ **Completed** <relative-time datetime=\"2026-09-02T22:35:58.561318Z\">2026-09-02T22:35:58.561318Z</relative-time>", headSHA[:6])
				return comment
			}()},
		},
		{
			name:     "ambiguous commit prefix",
			comments: []restComment{validComment},
			formalReviews: []restReview{{
				CommitID: headSHA[:7] + strings.Repeat("b", 33),
			}},
		},
		{
			name: "newer incomplete edit supersedes completed summary",
			comments: []restComment{validComment, func() restComment {
				comment := validComment
				comment.ID = 2
				comment.UpdatedAt = new(updatedAt.Add(time.Minute))
				comment.Body = testCodexReviewSummaryBody("🔄 **In progress**", headSHA[:7])
				return comment
			}()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, got := latestCodexSummaryReview(tt.comments, tt.formalReviews, headSHA)
			if got != tt.want {
				t.Fatalf("latestCodexSummaryReview() ok = %v, want %v", got, tt.want)
			}
		})
	}
}

func testCodexReviewSummaryBody(status string, commit string) string {
	return codexReviewSummaryMarker + "\n\n" + codexReviewSummaryHeading + "\n\n" +
		codexReviewSummaryTableHeader + "\n" + codexReviewSummaryTableSeparator + "\n" +
		"| 📝 **Code Review** | " + status + " | `" + commit + "` | Manual request |"
}

func TestConnectorFetchCandidateIssuesNormalizesProjectItems(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":26,"title":"GitHub adapter","state":"CLOSED","stateReason":"COMPLETED","url":"https://github.com/digitaldrywood/detent/issues/26","labels":{"nodes":[{"name":"Bug"},{"name":" enhancement "}]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Ready"},"priorityValue":{"name":"P0"}},{"id":"PVTI_2","content":{"__typename":"Issue","id":"I_kw2","number":27,"title":"Backlog item","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/27","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Backlog"},"priorityValue":{"name":"No priority"}}]}}}}`,
	}, {
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
		body:   `[]`,
	}})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
		StateMap:     map[string]string{"Todo": "Ready"},
		PriorityMap:  map[string]*int{"P0": new(1), "No priority": nil},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}

	priority := 1
	want := connector.Issue{
		ID:               "I_kw1",
		Identifier:       "digitaldrywood/detent#26",
		Title:            "GitHub adapter",
		Priority:         &priority,
		PriorityName:     "P0",
		State:            "Todo",
		URL:              "https://github.com/digitaldrywood/detent/issues/26",
		Closed:           true,
		ClosedReason:     "COMPLETED",
		BlockedBy:        []connector.BlockedRef{},
		Labels:           []string{"bug", "enhancement"},
		Assignees:        []string{},
		Fields:           map[string]string{},
		AssignedToWorker: true,
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("FetchCandidateIssues()[0] = %#v, want %#v", got[0], want)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	variables := requests[0]["variables"].(map[string]any)
	if variables["projectId"] != "PVT_1" {
		t.Fatalf("projectId = %v, want PVT_1", variables["projectId"])
	}
	if variables["first"] != float64(100) {
		t.Fatalf("first = %v, want 100", variables["first"])
	}
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
	query := requests[0]["query"].(string)
	for _, forbidden := range []string{
		"body",
		"closedByPullRequestsReferences",
		"subIssues(",
		"trackedIssues(",
		"fieldValues(",
	} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("project query contains %q:\n%s", forbidden, query)
		}
	}
	if !strings.Contains(query, "labels(first: 20)") {
		t.Fatalf("project query missing labels:\n%s", query)
	}
	if requests[1]["method"] != http.MethodGet || !strings.HasPrefix(requests[1]["path"].(string), "/repos/digitaldrywood/detent/pulls?") {
		t.Fatalf("pull request request = %#v, want REST pulls list", requests[1])
	}
}

func TestConnectorFetchRefreshIssuesBoundsLargeProjectScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		total            int
		firstPageCount   int
		secondPageCount  int
		wantGraphQLCalls int
	}{
		{name: "single page", total: 99, firstPageCount: 99, wantGraphQLCalls: 1},
		{name: "pyroapex scale", total: 186, firstPageCount: 100, secondPageCount: 86, wantGraphQLCalls: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			responses := []graphqlTestResponse{{
				body: projectItemsPageResponseWithTotal(
					tt.total,
					tt.secondPageCount > 0,
					"cursor-1",
					projectIssueNodes(tt.firstPageCount, "Todo"),
				),
			}}
			if tt.secondPageCount > 0 {
				responses = append(responses, graphqlTestResponse{
					body: projectItemsPageResponseWithTotal(
						tt.total,
						false,
						"",
						projectIssueNodes(tt.secondPageCount, "Done"),
					),
				})
			}
			responses = append(responses, graphqlTestResponse{
				method: http.MethodGet,
				path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
				body:   `[]`,
			})

			server := newGraphQLTestServer(t, responses)
			c := newGitHubTestConnector(t, server, Config{
				ProjectSlug:  "PVT_1",
				ActiveStates: []string{"Todo"},
			})

			result := c.FetchRefreshIssues(
				context.Background(),
				[]string{"Todo"},
				[]string{"Done"},
				connector.IssueFilterHint{},
			)
			if result.CandidateError != nil || result.StatusError != nil {
				t.Fatalf("FetchRefreshIssues() errors = %v, %v", result.CandidateError, result.StatusError)
			}
			if len(result.Candidates) != tt.firstPageCount || len(result.Statuses) != tt.secondPageCount {
				t.Fatalf(
					"FetchRefreshIssues() counts = %d candidates, %d statuses; want %d, %d",
					len(result.Candidates),
					len(result.Statuses),
					tt.firstPageCount,
					tt.secondPageCount,
				)
			}

			requests := waitForGraphQLRequests(t, server, tt.wantGraphQLCalls+1)
			graphqlCalls := 0
			for _, request := range requests {
				if request["method"] == http.MethodPost {
					graphqlCalls++
				}
			}
			if graphqlCalls != tt.wantGraphQLCalls {
				t.Fatalf("GraphQL calls = %d, want %d", graphqlCalls, tt.wantGraphQLCalls)
			}
		})
	}
}

func TestConnectorFetchCandidateIssuesWithFilterIgnoresProjectV2(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":26,"title":"GitHub adapter","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/26","labels":{"nodes":[{"name":"enhancement"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Ready"}}]}}}}`,
	}, {
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
		body:   `[]`,
	}})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
		StateMap:     map[string]string{"Todo": "Ready"},
	})

	got, err := c.FetchCandidateIssuesByStatesWithFilter(context.Background(), []string{"Todo"}, connector.IssueFilterHint{
		Authors:      []string{"alice"},
		Assignees:    []string{"worker-1"},
		LabelInclude: []string{"ready"},
		LabelExclude: []string{"blocked"},
	})
	if err != nil {
		t.Fatalf("FetchCandidateIssuesByStatesWithFilter() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssuesByStatesWithFilter() len = %d, want 1", len(got))
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want project query and pull request list", len(requests))
	}
	variables := requests[0]["variables"].(map[string]any)
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
}

func TestConnectorProjectV2FetchesHydrateAuthorizationFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    string
		responses []graphqlTestResponse
		fetch     func(context.Context, *Connector) ([]connector.Issue, error)
	}{
		{
			name:   "candidate issues",
			status: "Todo",
			responses: []graphqlTestResponse{{}, {
				method: http.MethodGet,
				path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
				body:   `[]`,
			}},
			fetch: func(ctx context.Context, c *Connector) ([]connector.Issue, error) {
				return c.FetchCandidateIssues(ctx)
			},
		},
		{
			name:      "observed issues",
			status:    "In Progress",
			responses: []graphqlTestResponse{{}},
			fetch: func(ctx context.Context, c *Connector) ([]connector.Issue, error) {
				return c.FetchIssuesByStates(ctx, []string{"In Progress"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			responses := append([]graphqlTestResponse(nil), tt.responses...)
			responses[0].body = projectAuthorizationItemsResponse(tt.status)
			server := newGraphQLTestServer(t, responses)
			c := newGitHubTestConnector(t, server, Config{
				ProjectSlug:    "PVT_1",
				ActiveStates:   []string{"Todo"},
				ObservedStates: []string{"In Progress"},
			})

			issues, err := tt.fetch(context.Background(), c)
			if err != nil {
				t.Fatalf("fetch ProjectV2 issues: %v", err)
			}
			if len(issues) != 2 {
				t.Fatalf("fetched issues = %d, want 2", len(issues))
			}

			selectors := []struct {
				name          string
				authorization selector.Selector
				wantIDs       []string
			}{
				{name: "author", authorization: selector.Selector{AuthorIn: []string{"alice"}}, wantIDs: []string{"I_authorized"}},
				{name: "assignee", authorization: selector.Selector{AssigneeIn: []string{"worker-1"}}, wantIDs: []string{"I_authorized"}},
				{name: "label", authorization: selector.Selector{Labels: selector.Labels{Include: []string{"ready"}}}, wantIDs: []string{"I_authorized"}},
				{name: "unconfigured", authorization: selector.Selector{}, wantIDs: []string{"I_authorized", "I_foreign"}},
			}
			for _, selectorTest := range selectors {
				t.Run(selectorTest.name, func(t *testing.T) {
					t.Parallel()

					matchedIDs := []string{}
					for _, issue := range issues {
						if selector.Match(issue, selectorTest.authorization, selector.Context{}) {
							matchedIDs = append(matchedIDs, issue.ID)
						}
					}
					if !reflect.DeepEqual(matchedIDs, selectorTest.wantIDs) {
						t.Fatalf("matched issue IDs = %#v, want %#v", matchedIDs, selectorTest.wantIDs)
					}
				})
			}

			query := server.requests()[0]["query"].(string)
			for _, selection := range []string{
				"author { login }",
				"assignees(first: 10) { nodes { login } }",
				"labels(first: 20) { nodes { name } }",
			} {
				if !strings.Contains(query, selection) {
					t.Fatalf("ProjectV2 query missing %q:\n%s", selection, query)
				}
			}
		})
	}
}

func TestAllAssigneeLogins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		assignees nodeConnection[assignee]
		want      []string
	}{
		{
			name:      "returns all nonblank logins in order",
			assignees: nodeConnection[assignee]{Nodes: []assignee{{Login: " worker-1 "}, {Login: ""}, {Login: "worker-2"}}},
			want:      []string{"worker-1", "worker-2"},
		},
		{
			name:      "empty connection returns empty slice",
			assignees: nodeConnection[assignee]{},
			want:      []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := allAssigneeLogins(tt.assignees); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("allAssigneeLogins() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestProjectFieldValues(t *testing.T) {
	t.Parallel()

	number := 42.5
	tests := []struct {
		name   string
		values nodeConnection[projectFieldValue]
		want   map[string]string
	}{
		{
			name: "captures supported field values",
			values: nodeConnection[projectFieldValue]{Nodes: []projectFieldValue{
				{TypeName: "ProjectV2ItemFieldSingleSelectValue", Name: "In Progress", Field: projectField{Name: "Status"}},
				{TypeName: "ProjectV2ItemFieldTextValue", Text: "owner notes", Field: projectField{Name: "Notes"}},
				{TypeName: "ProjectV2ItemFieldNumberValue", Number: &number, Field: projectField{Name: "Rank"}},
				{TypeName: "ProjectV2ItemFieldDateValue", Field: projectField{Name: "Due"}},
				{TypeName: "ProjectV2ItemFieldTextValue", Text: "missing field"},
			}},
			want: map[string]string{"Notes": "owner notes", "Rank": "42.5", "Status": "In Progress"},
		},
		{
			name:   "empty values return empty map",
			values: nodeConnection[projectFieldValue]{},
			want:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := projectFieldValues(tt.values); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("projectFieldValues() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestProjectFieldUpdatedAtFlowsIntoIssue(t *testing.T) {
	t.Parallel()

	updatedAt := "2026-07-10T01:36:00Z"
	fields := projectFieldValues(nodeConnection[projectFieldValue]{Nodes: []projectFieldValue{{
		TypeName:  "ProjectV2ItemFieldSingleSelectValue",
		Name:      "pending_review",
		UpdatedAt: &updatedAt,
		Field:     projectField{Name: "render_status"},
	}}})

	issue := (&Connector{}).buildIssue(githubIssueNode{}, "Todo", "", nil, fields)
	want := time.Date(2026, 7, 10, 1, 36, 0, 0, time.UTC)
	if issue.Fields["render_status"] != "pending_review" {
		t.Fatalf("render_status = %q, want pending_review", issue.Fields["render_status"])
	}
	if got := issue.FieldUpdatedAt["render_status"]; !got.Equal(want) {
		t.Fatalf("render_status updated at = %v, want %v", got, want)
	}
}

func TestLinkedIssueProjectQueriesStayUnderGitHubNodeLimit(t *testing.T) {
	t.Parallel()

	const githubStaticNodeLimit = 500000
	tests := []struct {
		name     string
		query    string
		required []string
		budget   int
	}{
		{
			name:  "sub issue project fields",
			query: issueSubIssuesQuery,
			required: []string{
				"subIssues(first: $linkedIssuesFirst, after: $after)",
				"projectItems(first: $linkedProjectItemsFirst)",
				"fieldValues(first: $linkedProjectItemFieldValuesFirst)",
			},
			budget: linkedIssuePageSize * linkedIssueProjectItemsPageSize * linkedIssueProjectItemFieldValuesPageSize,
		},
		{
			name:  "tracked issue project fields",
			query: issueTrackedIssuesQuery,
			required: []string{
				"trackedIssues(first: $linkedIssuesFirst, after: $after)",
				"projectItems(first: $linkedProjectItemsFirst)",
				"fieldValues(first: $linkedProjectItemFieldValuesFirst)",
			},
			budget: linkedIssuePageSize * linkedIssueProjectItemsPageSize * linkedIssueProjectItemFieldValuesPageSize,
		},
		{
			name:  "tracked in issue project fields",
			query: issueParentsQuery,
			required: []string{
				"trackedInIssues(first: $linkedIssuesFirst, after: $trackedInAfter)",
				"projectItems(first: $projectItemsFirst)",
				"fieldValues(first: $projectItemFieldValuesFirst)",
			},
			budget: linkedIssuePageSize * projectItemsPerIssue * projectItemFieldValuesPageSize,
		},
		{
			name:  "tracked in issue linked children",
			query: issueParentsQuery,
			required: []string{
				"trackedInIssues(first: $linkedIssuesFirst, after: $trackedInAfter)",
				"subIssues(first: $linkedIssuesFirst)",
				"trackedIssues(first: $linkedIssuesFirst)",
				"projectItems(first: $linkedProjectItemsFirst)",
				"fieldValues(first: $linkedProjectItemFieldValuesFirst)",
			},
			budget: linkedIssuePageSize * linkedIssuePageSize * linkedIssueProjectItemsPageSize * linkedIssueProjectItemFieldValuesPageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.budget >= githubStaticNodeLimit {
				t.Fatalf("GraphQL static node budget = %d, want < %d", tt.budget, githubStaticNodeLimit)
			}
			for _, want := range tt.required {
				if !strings.Contains(tt.query, want) {
					t.Fatalf("query missing %q:\n%s", want, tt.query)
				}
			}
		})
	}
}

func TestConnectorFetchCandidateIssuesRequestsRateLimitSnapshot(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
	}})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
	})

	if _, err := c.FetchCandidateIssues(context.Background()); err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	query := requests[0]["query"].(string)
	for _, want := range []string{"rateLimit", "remaining", "resetAt", "cost"} {
		if !strings.Contains(query, want) {
			t.Fatalf("project items query missing %q:\n%s", want, query)
		}
	}
}

func TestConnectorFetchIssuesByStatesDefaultsBlankProjectStatusesToBacklog(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_blank","content":{"__typename":"Issue","id":"I_blank","number":30,"title":"Blank status","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/30","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":null,"priorityValue":null},{"id":"PVTI_todo","content":{"__typename":"Issue","id":"I_todo","number":31,"title":"Ready status","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/31","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
		},
		{
			body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"},{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
		{
			body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_blank"}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ActiveStates:   []string{"Todo"},
		ObservedStates: []string{"Backlog"},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Backlog"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_blank" || got[0].State != "Backlog" {
		t.Fatalf("defaulted issue = %#v, want blank issue in Backlog", got[0])
	}

	requests := waitForGraphQLRequests(t, server, 3)
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want exhaustive project scan and default write", len(requests))
	}
	variables := requestVariables(t, requests[0])
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
	updateVariables := requestVariables(t, requests[2])
	if updateVariables["projectId"] != "PVT_1" ||
		updateVariables["itemId"] != "PVTI_blank" ||
		updateVariables["fieldId"] != "PVTSSF_status" ||
		updateVariables["optionId"] != "OPT_backlog" {
		t.Fatalf("update variables = %#v, want blank item moved to Backlog", updateVariables)
	}
}

func TestConnectorFetchIssueStateProbeFindsBlankBacklogAfterFirstProjectPage(t *testing.T) {
	t.Parallel()

	firstPageNodes := projectIssueNodes(projectItemsPageSize, "Todo")
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponse(true, "cursor-1", firstPageNodes),
		},
		{
			body: projectItemsPageResponse(false, "", []string{
				projectIssueNode("PVTI_blank_late", "I_blank_late", 81, "Late blank status", ""),
			}),
		},
		{
			body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"},{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
		{
			body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_blank_late"}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Backlog"},
	})

	got, err := c.FetchIssueStateProbe(context.Background(), []string{"Backlog"}, 1)
	if err != nil {
		t.Fatalf("FetchIssueStateProbe() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueStateProbe() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_blank_late" || got[0].State != "Backlog" {
		t.Fatalf("probe issue = %#v, want late blank issue in Backlog", got[0])
	}

	requests := waitForGraphQLRequests(t, server, 4)
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want two project pages and default write", len(requests))
	}
	variables := requestVariables(t, requests[0])
	if variables["query"] != nil || variables["after"] != nil {
		t.Fatalf("first blank scan variables = %#v, want unfiltered first page", variables)
	}
	variables = requestVariables(t, requests[1])
	if variables["query"] != nil || variables["after"] != "cursor-1" {
		t.Fatalf("second blank scan variables = %#v, want unfiltered second page", variables)
	}
	updateVariables := requestVariables(t, requests[3])
	if updateVariables["itemId"] != "PVTI_blank_late" || updateVariables["optionId"] != "OPT_backlog" {
		t.Fatalf("update variables = %#v, want late blank item moved to Backlog", updateVariables)
	}
}

func TestConnectorFetchIssueStateProbeFindsAnyRequestedStateInSingleScan(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponse(false, "", []string{
				projectIssueNode("PVTI_review", "I_review", 82, "Review issue", "Human Review"),
			}),
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Backlog", "Human Review"},
	})

	got, err := c.FetchIssueStateProbe(context.Background(), []string{"Backlog", "Human Review"}, 1)
	if err != nil {
		t.Fatalf("FetchIssueStateProbe() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueStateProbe() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_review" || got[0].State != "Human Review" {
		t.Fatalf("probe issue = %#v, want Human Review issue", got[0])
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want one exhaustive project scan", len(requests))
	}
	variables := requestVariables(t, requests[0])
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
}

func TestConnectorFetchIssueStateProbeDoesNotRepairUnrequestedBlankStatus(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponse(false, "", []string{
				projectIssueNode("PVTI_blank", "I_blank", 83, "Blank status issue", ""),
			}),
		},
		{
			body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"}]}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Human Review"},
	})

	got, err := c.FetchIssueStateProbe(context.Background(), []string{"Human Review"}, 1)
	if err != nil {
		t.Fatalf("FetchIssueStateProbe() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FetchIssueStateProbe() = %#v, want no Human Review issues", got)
	}

	select {
	case <-server.requestSeen:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for project item scan")
	}
	select {
	case <-server.requestSeen:
		t.Fatalf("requests = %#v, want no blank-status repair for an unrequested state", server.requests())
	case <-time.After(100 * time.Millisecond):
	}
}

func TestConnectorFetchIssuesByStatesReturnsBlankBacklogAfterFirstProjectPage(t *testing.T) {
	t.Parallel()

	firstPageNodes := projectIssueNodes(projectItemsPageSize, "Todo")
	firstPageNodes = append([]string{
		projectIssueNode("PVTI_backlog", "I_backlog", 40, "Explicit backlog", "Backlog"),
	}, firstPageNodes...)
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponse(true, "cursor-1", firstPageNodes),
		},
		{
			body: projectItemsPageResponse(false, "", []string{
				projectIssueNode("PVTI_blank_late", "I_blank_late", 81, "Late blank status", ""),
			}),
		},
		{
			body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"},{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
		{
			body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_blank_late"}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Backlog"},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Backlog"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 2", len(got))
	}
	if got[0].ID != "I_backlog" || got[1].ID != "I_blank_late" {
		t.Fatalf("FetchIssuesByStates() ids = [%s %s], want [I_backlog I_blank_late]", got[0].ID, got[1].ID)
	}
	if got[1].State != "Backlog" {
		t.Fatalf("late blank issue state = %q, want Backlog", got[1].State)
	}

	requests := waitForGraphQLRequests(t, server, 4)
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want two project pages and default write", len(requests))
	}
	variables := requestVariables(t, requests[0])
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
	variables = requestVariables(t, requests[1])
	if variables["query"] != nil || variables["after"] != "cursor-1" {
		t.Fatalf("second blank scan variables = %#v, want unfiltered second page", variables)
	}
	updateVariables := requestVariables(t, requests[3])
	if updateVariables["itemId"] != "PVTI_blank_late" || updateVariables["optionId"] != "OPT_backlog" {
		t.Fatalf("update variables = %#v, want late blank item moved to Backlog", updateVariables)
	}
}

func TestConnectorFetchIssuesByStatesScansBacklogAndObservedStatesExhaustively(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_backlog","content":{"__typename":"Issue","id":"I_backlog","number":40,"title":"Explicit backlog","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/40","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Backlog"},"priorityValue":null}]}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Backlog", "Human Review"},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Backlog", "Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_backlog" || got[0].State != "Backlog" {
		t.Fatalf("explicit backlog issue = %#v, want explicit Backlog issue", got[0])
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want project scan without unrelated pull request lookup", len(requests))
	}
	variables := requestVariables(t, requests[0])
	if _, ok := variables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
	}
	query := requests[0]["query"].(string)
	if !strings.Contains(query, "closedByPullRequestsReferences") {
		t.Fatalf("observed project query missing linked pull request refs:\n%s", query)
	}
}

func TestConnectorBoundedBacklogFetchesUseUnfilteredLightweightQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		fetch                 func(context.Context, *Connector) ([]connector.Issue, error)
		linkedPullRequestRefs bool
	}{
		{
			name:                  "FetchIssuesByStatesLimit",
			linkedPullRequestRefs: true,
			fetch: func(ctx context.Context, c *Connector) ([]connector.Issue, error) {
				return c.FetchIssuesByStatesLimit(ctx, []string{"Backlog", "Human Review"}, 1)
			},
		},
		{
			name: "FetchIssueStateProbe",
			fetch: func(ctx context.Context, c *Connector) ([]connector.Issue, error) {
				return c.FetchIssueStateProbe(ctx, []string{"Backlog", "Human Review"}, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{{
				body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_backlog","content":{"__typename":"Issue","id":"I_backlog","number":40,"title":"Explicit backlog","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/40","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Backlog"},"priorityValue":null}]}}}}`,
			}})
			c := newGitHubTestConnector(t, server, Config{
				ProjectSlug:    "PVT_1",
				ObservedStates: []string{"Backlog", "Human Review"},
			})

			got, err := tt.fetch(context.Background(), c)
			if err != nil {
				t.Fatalf("%s() error = %v", tt.name, err)
			}
			if len(got) != 1 || got[0].ID != "I_backlog" || got[0].State != "Backlog" {
				t.Fatalf("%s() = %#v, want explicit Backlog issue", tt.name, got)
			}

			requests := server.requests()
			if len(requests) != 1 {
				t.Fatalf("request count = %d, want project scan without unrelated pull request lookup", len(requests))
			}
			variables := requestVariables(t, requests[0])
			if _, ok := variables["query"]; ok {
				t.Fatalf("query = %v, want unfiltered ProjectV2 items", variables["query"])
			}
			query := requests[0]["query"].(string)
			if got := strings.Contains(query, "closedByPullRequestsReferences"); got != tt.linkedPullRequestRefs {
				t.Fatalf("project query linked pull request refs = %t, want %t:\n%s", got, tt.linkedPullRequestRefs, query)
			}
		})
	}
}

func TestConnectorFetchCandidateIssuesDoesNotBlockOnBlankProjectStatusDefaulting(t *testing.T) {
	t.Parallel()

	releaseDefaultWrite := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseDefaultWrite)
		})
	}
	defer release()
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_blank","content":{"__typename":"Issue","id":"I_blank","number":30,"title":"Blank status","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/30","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":null,"priorityValue":null}]}}}}`,
		},
		{
			release: releaseDefaultWrite,
			body:    `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"},{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
		{
			body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_blank"}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
	})

	type result struct {
		issues []connector.Issue
		err    error
	}
	results := make(chan result, 1)
	go func() {
		issues, err := c.FetchCandidateIssues(context.Background())
		results <- result{issues: issues, err: err}
	}()

	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("FetchCandidateIssues() error = %v", result.err)
		}
		if len(result.issues) != 0 {
			t.Fatalf("FetchCandidateIssues() len = %d, want 0", len(result.issues))
		}
	case <-time.After(200 * time.Millisecond):
		release()
		result := <-results
		t.Fatalf("FetchCandidateIssues() blocked on default status write; issues = %#v error = %v", result.issues, result.err)
	}

	release()
	requests := waitForGraphQLRequests(t, server, 3)
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(requests))
	}
	updateVariables := requestVariables(t, requests[2])
	if updateVariables["itemId"] != "PVTI_blank" || updateVariables["optionId"] != "OPT_backlog" {
		t.Fatalf("update variables = %#v, want blank item moved to Backlog", updateVariables)
	}
}

func TestConnectorFetchCandidateIssuesDefaultStatusWriteSurvivesParentCancellation(t *testing.T) {
	t.Parallel()

	type traceContextKey struct{}
	traceKey := traceContextKey{}
	const traceValue = "trace-1"
	contextValues := make(chan any, 3)
	releaseDefaultWrite := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseDefaultWrite)
		})
	}
	defer release()
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_blank","content":{"__typename":"Issue","id":"I_blank","number":30,"title":"Blank status","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/30","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":null,"priorityValue":null}]}}}}`,
		},
		{
			release: releaseDefaultWrite,
			body:    `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_backlog","name":"Backlog"},{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
		{
			body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_blank"}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
		HTTPClient: contextValueCaptureClient{
			base:   server.Client(),
			key:    traceKey,
			values: contextValues,
		},
	})

	parentCtx, cancel := context.WithCancel(context.WithValue(context.Background(), traceKey, traceValue))
	got, err := c.FetchCandidateIssues(parentCtx)
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 0", len(got))
	}

	requests := waitForGraphQLRequests(t, server, 2)
	if len(requests) != 2 {
		t.Fatalf("request count before cancellation = %d, want 2", len(requests))
	}
	cancel()
	release()

	requests = waitForGraphQLRequests(t, server, 3)
	updateVariables := requestVariables(t, requests[2])
	if updateVariables["itemId"] != "PVTI_blank" || updateVariables["optionId"] != "OPT_backlog" {
		t.Fatalf("update variables = %#v, want blank item moved to Backlog", updateVariables)
	}

	for i := range 3 {
		select {
		case got := <-contextValues:
			if got != traceValue {
				t.Fatalf("request context value %d = %#v, want %q", i, got, traceValue)
			}
		case <-time.After(time.Second):
			t.Fatalf("request context value %d was not captured", i)
		}
	}
}

func TestConnectorFetchIssuesByStatesIgnoresBlankStatusDefaultWriteFailure(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_blank","content":{"__typename":"Issue","id":"I_blank","number":30,"title":"Blank status","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/30","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":null,"priorityValue":null}]}}}}`,
		},
		{
			body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_todo","name":"Todo"}]}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:    "PVT_1",
		ObservedStates: []string{"Backlog"},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Backlog"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_blank" || got[0].State != "Backlog" {
		t.Fatalf("defaulted issue = %#v, want blank issue in Backlog", got[0])
	}

	requests := waitForGraphQLRequests(t, server, 2)
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want project scan and failed default lookup", len(requests))
	}
}

func TestConnectorFetchCandidateIssuesLeavesDependencyResolutionForHydration(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_candidate","number":26,"title":"Candidate","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/26","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Ready"},"priorityValue":null},{"id":"PVTI_2","content":{"__typename":"Issue","id":"I_done","number":24,"title":"Done blocker","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/24","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Done"},"priorityValue":null},{"id":"PVTI_3","content":{"__typename":"Issue","id":"I_progress","number":25,"title":"Active blocker","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/25","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Working"},"priorityValue":null}]}}}}`,
	}, {
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
		body:   `[]`,
	}})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
		StateMap: map[string]string{
			"Todo":        "Ready",
			"In Progress": "Working",
		},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}

	if len(got[0].BlockedBy) != 0 {
		t.Fatalf("BlockedBy = %#v, want no dependency graph from lightweight poll", got[0].BlockedBy)
	}
}

func TestParseBlockedByRecognizesIssueReferences(t *testing.T) {
	t.Parallel()

	body := strings.Join([]string{
		"Depends on #24",
		"Blocked by #25",
		"Depends on: #26",
		"depends-on digitaldrywood/agent-runtime#27",
		"Depends on: https://github.com/digitaldrywood/detent/issues/28",
		"Depends on: https://github.com/digitaldrywood/detent/issues/28 and #24",
		"Mention only: #29",
	}, "\n")

	got := parseBlockedBy(body, "digitaldrywood/detent")
	want := []connector.BlockedRef{
		{Identifier: "digitaldrywood/detent#24", Source: connector.BlockedRefSourceProse},
		{Identifier: "digitaldrywood/detent#25", Source: connector.BlockedRefSourceProse},
		{Identifier: "digitaldrywood/detent#26", Source: connector.BlockedRefSourceProse},
		{Identifier: "digitaldrywood/agent-runtime#27", Source: connector.BlockedRefSourceProse},
		{Identifier: "digitaldrywood/detent#28", Source: connector.BlockedRefSourceProse},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseBlockedBy() = %#v, want %#v", got, want)
	}
}

func TestConnectorHydrateIssueBlockedByRefsUsesNativeDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		dependencySource string
		initial          []connector.BlockedRef
		status           int
		body             string
		want             []connector.BlockedRef
		wantCapability   connector.DependencyCapability
	}{
		{
			name:   "native only",
			status: http.StatusOK,
			body:   `[{"id":100,"node_id":"I_100","number":100,"state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/100","repository_url":"https://api.github.com/repos/digitaldrywood/detent"}]`,
			want: []connector.BlockedRef{{
				ID:         "I_100",
				Identifier: "digitaldrywood/detent#100",
				State:      "Open",
				Source:     connector.BlockedRefSourceNative,
			}},
			wantCapability: connector.DependencyCapability{
				Repository:      "digitaldrywood/detent",
				NativeBlockedBy: nativeDependencyStatusAvailable,
				Source:          dependencySourceMerged,
			},
		},
		{
			name:    "prose only when native list is empty",
			initial: []connector.BlockedRef{{Identifier: "digitaldrywood/detent#101", Source: connector.BlockedRefSourceProse}},
			status:  http.StatusOK,
			body:    `[]`,
			want: []connector.BlockedRef{{
				Identifier: "digitaldrywood/detent#101",
				Source:     connector.BlockedRefSourceProse,
			}},
			wantCapability: connector.DependencyCapability{
				Repository:      "digitaldrywood/detent",
				NativeBlockedBy: nativeDependencyStatusAvailable,
				Source:          dependencySourceMerged,
			},
		},
		{
			name: "merged deduplicates with native winning",
			initial: []connector.BlockedRef{
				{Identifier: "digitaldrywood/detent#100", Source: connector.BlockedRefSourceProse},
				{Identifier: "digitaldrywood/detent#101", Source: connector.BlockedRefSourceProse},
			},
			status: http.StatusOK,
			body:   `[{"id":100,"node_id":"I_100","number":100,"state":"closed","html_url":"https://github.com/digitaldrywood/detent/issues/100","repository_url":"https://api.github.com/repos/digitaldrywood/detent"}]`,
			want: []connector.BlockedRef{{
				ID:         "I_100",
				Identifier: "digitaldrywood/detent#100",
				State:      "Done",
				Source:     connector.BlockedRefSourceNative,
			}, {
				Identifier: "digitaldrywood/detent#101",
				Source:     connector.BlockedRefSourceProse,
			}},
			wantCapability: connector.DependencyCapability{
				Repository:      "digitaldrywood/detent",
				NativeBlockedBy: nativeDependencyStatusAvailable,
				Source:          dependencySourceMerged,
			},
		},
		{
			name:    "probe not found falls back to prose",
			initial: []connector.BlockedRef{{Identifier: "digitaldrywood/detent#101", Source: connector.BlockedRefSourceProse}},
			status:  http.StatusNotFound,
			body:    `{"message":"not found"}`,
			want: []connector.BlockedRef{{
				Identifier: "digitaldrywood/detent#101",
				Source:     connector.BlockedRefSourceProse,
			}},
			wantCapability: connector.DependencyCapability{
				Repository:      "digitaldrywood/detent",
				NativeBlockedBy: nativeDependencyStatusUnavailable,
				Source:          dependencySourceMerged,
				Detail:          "status 404",
			},
		},
		{
			name:             "native only ignores prose on capable repo",
			dependencySource: dependencySourceNativeOnly,
			initial:          []connector.BlockedRef{{Identifier: "digitaldrywood/detent#101", Source: connector.BlockedRefSourceProse}},
			status:           http.StatusOK,
			body:             `[{"id":100,"node_id":"I_100","number":100,"state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/100","repository_url":"https://api.github.com/repos/digitaldrywood/detent"}]`,
			want: []connector.BlockedRef{{
				ID:         "I_100",
				Identifier: "digitaldrywood/detent#100",
				State:      "Open",
				Source:     connector.BlockedRefSourceNative,
			}},
			wantCapability: connector.DependencyCapability{
				Repository:      "digitaldrywood/detent",
				NativeBlockedBy: nativeDependencyStatusAvailable,
				Source:          dependencySourceNativeOnly,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{{
				method: http.MethodGet,
				path:   "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by?per_page=100",
				status: tt.status,
				body:   tt.body,
			}})
			c := newGitHubTestConnector(t, server, Config{DependencySource: tt.dependencySource})
			issue := connector.NewIssue()
			issue.ID = "I_1073"
			issue.Identifier = "digitaldrywood/detent#1073"
			issue.BlockedBy = append([]connector.BlockedRef(nil), tt.initial...)

			c.hydrateIssueBlockedByRefs(context.Background(), &issue)

			if !reflect.DeepEqual(issue.BlockedBy, tt.want) {
				t.Fatalf("BlockedBy = %#v, want %#v", issue.BlockedBy, tt.want)
			}
			capabilities := c.DependencyCapabilities()
			if len(capabilities) != 1 || capabilities[0] != tt.wantCapability {
				t.Fatalf("DependencyCapabilities() = %#v, want %#v", capabilities, []connector.DependencyCapability{tt.wantCapability})
			}
		})
	}
}

func TestConnectorHydrateIssueBlockedByRefsPaginatesNativeDependencies(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method:  http.MethodGet,
			path:    "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by?per_page=100",
			headers: map[string]string{"Link": `</repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by?per_page=100&page=2>; rel="next"`},
			body:    `[{"id":100,"node_id":"I_100","number":100,"state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/100","repository_url":"https://api.github.com/repos/digitaldrywood/detent"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by?per_page=100&page=2",
			body:   `[{"id":101,"node_id":"I_101","number":101,"state":"closed","html_url":"https://github.com/digitaldrywood/detent/issues/101","repository_url":"https://api.github.com/repos/digitaldrywood/detent"}]`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})
	issue := connector.NewIssue()
	issue.ID = "I_1073"
	issue.Identifier = "digitaldrywood/detent#1073"

	c.hydrateIssueBlockedByRefs(context.Background(), &issue)

	want := []connector.BlockedRef{
		{
			ID:         "I_100",
			Identifier: "digitaldrywood/detent#100",
			State:      "Open",
			Source:     connector.BlockedRefSourceNative,
		},
		{
			ID:         "I_101",
			Identifier: "digitaldrywood/detent#101",
			State:      "Done",
			Source:     connector.BlockedRefSourceNative,
		},
	}
	if !reflect.DeepEqual(issue.BlockedBy, want) {
		t.Fatalf("BlockedBy = %#v, want paginated native refs %#v", issue.BlockedBy, want)
	}
}

func TestConnectorHydrateIssueBlockedByRefsDoesNotCacheRateLimitAsUnavailable(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by?per_page=100",
		status: http.StatusForbidden,
		headers: map[string]string{
			"Retry-After":           "120",
			"X-RateLimit-Limit":     "5000",
			"X-RateLimit-Remaining": "4999",
			"X-RateLimit-Used":      "1",
			"X-RateLimit-Resource":  "core",
		},
		body: `{"message":"secondary rate limit"}`,
	}})
	c := newGitHubTestConnector(t, server, Config{})
	issue := connector.NewIssue()
	issue.ID = "I_1073"
	issue.Identifier = "digitaldrywood/detent#1073"
	issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#101"}}

	c.hydrateIssueBlockedByRefs(context.Background(), &issue)

	want := []connector.BlockedRef{{
		Identifier: "digitaldrywood/detent#101",
		Source:     connector.BlockedRefSourceProse,
	}}
	if !reflect.DeepEqual(issue.BlockedBy, want) {
		t.Fatalf("BlockedBy = %#v, want prose fallback %#v", issue.BlockedBy, want)
	}
	if capabilities := c.DependencyCapabilities(); len(capabilities) != 0 {
		t.Fatalf("DependencyCapabilities() = %#v, want no cached capability for retryable rate limit", capabilities)
	}
}

func TestConnectorIssueDependencyWriterUsesNativeRESTEndpoints(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/100",
			body:   `{"id":1000,"node_id":"I_100","number":100,"title":"Blocker","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/100","labels":[]}`,
		},
		{
			method: http.MethodPost,
			path:   "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by",
			body:   `{"id":1073,"node_id":"I_1073","number":1073,"title":"Blocked","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/1073","labels":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/100",
			body:   `{"id":1000,"node_id":"I_100","number":100,"title":"Blocker","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/100","labels":[]}`,
		},
		{
			method: http.MethodDelete,
			path:   "/repos/digitaldrywood/detent/issues/1073/dependencies/blocked_by/1000",
			body:   `{"id":1073,"node_id":"I_1073","number":1073,"title":"Blocked","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/1073","labels":[]}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})

	if err := c.AddIssueBlockedByDependency(context.Background(), "digitaldrywood/detent#1073", "digitaldrywood/detent#100"); err != nil {
		t.Fatalf("AddIssueBlockedByDependency() error = %v", err)
	}
	if err := c.RemoveIssueBlockedByDependency(context.Background(), "digitaldrywood/detent#1073", "digitaldrywood/detent#100"); err != nil {
		t.Fatalf("RemoveIssueBlockedByDependency() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want 4", len(requests))
	}
	body, ok := requests[1]["body"].(map[string]any)
	if !ok || body["issue_id"] != float64(1000) {
		t.Fatalf("POST body = %#v, want issue_id 1000", requests[1]["body"])
	}
}

func TestConnectorFetchCandidateIssuesCapturesLinkedChildIssues(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_epic","content":{"__typename":"Issue","id":"I_epic","number":258,"title":"Epic: release readiness","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/258","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
	}, {
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
		body:   `[]`,
	}})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}

	if got[0].ChildIssues != nil {
		t.Fatalf("ChildIssues = %#v, want nil from lightweight poll", got[0].ChildIssues)
	}

	query := server.requests()[0]["query"].(string)
	for _, forbidden := range []string{
		"subIssues(",
		"trackedIssues(",
		"fieldValues(",
	} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("project query contains %q:\n%s", forbidden, query)
		}
	}
	if !strings.Contains(query, "labels(first: 20)") {
		t.Fatalf("project query missing labels:\n%s", query)
	}
}

func TestConnectorFetchCandidateIssuesAttachesPullRequestByBranchPrefix(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"First issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null},{"id":"PVTI_18","content":{"__typename":"Issue","id":"I_18","number":18,"title":"Prefix neighbor","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/18","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","updated_at":"2026-06-05T11:30:00Z","head":{"ref":"detent/digitaldrywood_detent_182_followup","sha":"sha-187"}},{"number":188,"html_url":"https://github.com/digitaldrywood/detent/pull/188","state":"closed","head":{"ref":"detent/digitaldrywood_detent_181","sha":"sha-188"},"merged_at":"2026-06-01T00:00:00Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/187",
			body:   `{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","updated_at":"2026-06-05T11:30:00Z","head":{"ref":"detent/digitaldrywood_detent_182_followup","sha":"sha-187"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100",
			body:   `{"check_runs":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-187/statuses?per_page=100",
			body:   `[{"context":"ci/build","state":"success","created_at":"2026-06-05T11:00:00Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/187/reviews?per_page=100",
			body:   `[{"body":"[P1] Stale finding on the previous review.","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"sha-187","submitted_at":"2026-06-05T10:00:00Z"},{"body":"No blocking findings on the current head.","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"sha-187","submitted_at":"2026-06-05T11:00:00Z"}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 2", len(got))
	}

	byID := map[string]connector.Issue{}
	for _, issue := range got {
		byID[issue.ID] = issue
	}
	pr := byID["I_182"].PullRequest
	if pr == nil {
		t.Fatal("I_182 PullRequest = nil, want matching open PR")
		return
	}
	if pr.Number != 187 || pr.State != "OPEN" || pr.BranchName != "detent/digitaldrywood_detent_182_followup" || pr.CIStatus != "pass" || pr.CodexReviewState != "COMMENTED" {
		t.Fatalf("I_182 PullRequest = %#v, want PR 187 open followup", pr)
	}
	wantActivityAt := time.Date(2026, 6, 5, 11, 30, 0, 0, time.UTC)
	if pr.ActivityAt == nil || !pr.ActivityAt.Equal(wantActivityAt) {
		t.Fatalf("I_182 PullRequest.ActivityAt = %v, want %v", pr.ActivityAt, wantActivityAt)
	}
	wantReviewSubmittedAt := time.Date(2026, 6, 5, 11, 0, 0, 0, time.UTC)
	if pr.CodexReviewSubmittedAt == nil || !pr.CodexReviewSubmittedAt.Equal(wantReviewSubmittedAt) {
		t.Fatalf("I_182 PullRequest.CodexReviewSubmittedAt = %v, want %v", pr.CodexReviewSubmittedAt, wantReviewSubmittedAt)
	}
	if len(pr.CodexReviewFindings) != 0 {
		t.Fatalf("I_182 PullRequest.CodexReviewFindings = %#v, want none", pr.CodexReviewFindings)
	}
	if byID["I_18"].PullRequest != nil {
		t.Fatalf("I_18 PullRequest = %#v, want nil", byID["I_18"].PullRequest)
	}
}

func TestConnectorFetchIssuesByStatesAttachesLinkedPullRequestBeforeBranchPrefix(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_370","content":{"__typename":"Issue","id":"I_370","number":370,"title":"Linked PR issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/370","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"bug"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":375,"url":"https://github.com/digitaldrywood/detent/pull/375","state":"CLOSED","repository":{"nameWithOwner":"digitaldrywood/detent"}},{"number":376,"url":"https://github.com/corylanou/detent/pull/376","state":"OPEN","repository":{"nameWithOwner":"corylanou/detent"}}]}},"statusValue":{"name":"Reviewing"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/corylanou/detent/pulls/376",
			body:   `{"number":376,"html_url":"https://github.com/corylanou/detent/pull/376","state":"open","head":{"ref":"detent/detent-digitaldrywood_detent_370-e71678a9ca7e","sha":"sha-376"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/corylanou/detent/commits/sha-376/check-runs?per_page=100",
			body:   `{"check_runs":[{"status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/corylanou/detent/commits/sha-376/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/corylanou/detent/pulls/376/reviews?per_page=100",
			body:   `[{"body":"No blocking findings.","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"sha-376","submitted_at":"2026-06-05T11:00:00Z"}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap: map[string]string{
			"Human Review": "Reviewing",
		},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want linked PR")
		return
	}
	if pr.Number != 376 || pr.URL != "https://github.com/corylanou/detent/pull/376" || pr.State != "OPEN" || pr.BranchName != "detent/detent-digitaldrywood_detent_370-e71678a9ca7e" || pr.CIStatus != "pass" || pr.CodexReviewState != "COMMENTED" {
		t.Fatalf("PullRequest = %#v, want linked PR 376 with hydrated status", pr)
	}
	if got[0].PRRepository != "corylanou/detent" {
		t.Fatalf("PRRepository = %q, want corylanou/detent", got[0].PRRepository)
	}

	requests := server.requests()
	if len(requests) != 5 {
		t.Fatalf("request count = %d, want observed query plus linked PR status requests", len(requests))
	}
	query := requests[0]["query"].(string)
	if !strings.Contains(query, "closedByPullRequestsReferences") {
		t.Fatalf("observed status query does not request linked pull requests:\n%s", query)
	}
	if !strings.Contains(query, "nodes { number url state updatedAt repository { nameWithOwner } }") {
		t.Fatalf("observed status query does not request linked pull request states:\n%s", query)
	}
	for _, request := range requests {
		path, _ := request["path"].(string)
		if strings.Contains(path, "/pulls?") {
			t.Fatalf("request path = %q, want linked PR path without repository-wide pull list", path)
		}
	}
}

func TestConnectorFetchIssuesByStatesPrefersMergedLinkedPullRequestOverClosedUnmerged(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_179","content":{"__typename":"Issue","id":"I_179","number":179,"title":"Issue closed by external PR","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/creswoodcorners-phone/issues/179","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"bug"}]},"repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"},"closedByPullRequestsReferences":{"nodes":[{"number":185,"url":"https://github.com/digitaldrywood/creswoodcorners-phone/pull/185","state":"CLOSED","updatedAt":"2026-07-06T18:57:20Z","repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"}},{"number":186,"url":"https://github.com/digitaldrywood/creswoodcorners-phone/pull/186","state":"MERGED","updatedAt":"2026-07-07T12:00:00Z","repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"}},{"number":191,"url":"https://github.com/digitaldrywood/creswoodcorners-phone/pull/191","state":"MERGED","updatedAt":"2026-07-08T12:00:00Z","repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/creswoodcorners-phone/pulls/191",
			body:   `{"number":191,"html_url":"https://github.com/digitaldrywood/creswoodcorners-phone/pull/191","state":"closed","merged_at":"2026-07-08T12:00:00Z","mergeable_state":"clean","updated_at":"2026-07-08T12:00:00Z","head":{"ref":"issue-179-human-fix","sha":"sha-191"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/creswoodcorners-phone/commits/sha-191/check-runs?per_page=100",
			body:   `{"check_runs":[{"status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/creswoodcorners-phone/commits/sha-191/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/creswoodcorners-phone/pulls/191/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("digitaldrywood/creswoodcorners-phone", 191),
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})
	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	if got[0].PRNumber == nil || *got[0].PRNumber != 191 {
		t.Fatalf("PRNumber = %v, want merged linked PR 191", got[0].PRNumber)
	}
	if got[0].PRRepository != "digitaldrywood/creswoodcorners-phone" {
		t.Fatalf("PRRepository = %q, want digitaldrywood/creswoodcorners-phone", got[0].PRRepository)
	}
	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want merged linked PR")
		return
	}
	if pr.Number != 191 || pr.State != "MERGED" || pr.BranchName != "issue-179-human-fix" || pr.CIStatus != "pass" {
		t.Fatalf("PullRequest = %#v, want hydrated merged PR 191", pr)
	}

	for _, request := range server.requests() {
		path, _ := request["path"].(string)
		if strings.Contains(path, "/pulls/185") {
			t.Fatalf("request path = %q, want no hydration of closed unmerged PR 185", path)
		}
	}
}

func TestConnectorFetchCandidateIssuesPaginatesPullRequestStatusRESTEndpoints(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"First issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/187",
			body:   `{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}}`,
		},
		{
			method:  http.MethodGet,
			path:    "/repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100",
			headers: map[string]string{"Link": `</repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100&page=2>; rel="next"`},
			body:    `{"check_runs":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100&page=2",
			body:   `{"check_runs":[{"status":"completed","conclusion":"success"}]}`,
		},
		{
			method:  http.MethodGet,
			path:    "/repos/digitaldrywood/detent/commits/sha-187/statuses?per_page=100",
			headers: map[string]string{"Link": `</repos/digitaldrywood/detent/commits/sha-187/statuses?per_page=100&page=2>; rel="next"`},
			body:    `[{"context":"ci/build","state":"success","created_at":"2026-06-05T11:00:00Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-187/statuses?per_page=100&page=2",
			body:   `[{"context":"ci/build","state":"failure","created_at":"2026-06-05T12:00:00Z"}]`,
		},
		{
			method:  http.MethodGet,
			path:    "/repos/digitaldrywood/detent/pulls/187/reviews?per_page=100",
			headers: map[string]string{"Link": `</repos/digitaldrywood/detent/pulls/187/reviews?per_page=100&page=2>; rel="next"`},
			body:    `[{"body":"No blocking findings.","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"sha-187","submitted_at":"2026-06-05T10:00:00Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/187/reviews?per_page=100&page=2",
			body:   `[{"body":"[P1] Later paged finding.","html_url":"https://github.com/digitaldrywood/detent/pull/187#pullrequestreview-1","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"sha-187","submitted_at":"2026-06-05T12:00:00Z"}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}

	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want matching PR")
		return
	}
	if pr.CIStatus != "fail" || pr.CodexReviewState != "P1" {
		t.Fatalf("PullRequest status = CI %q review %q, want fail/P1", pr.CIStatus, pr.CodexReviewState)
	}
	if len(pr.CodexReviewFindings) != 1 ||
		pr.CodexReviewFindings[0].Body != "[P1] Later paged finding." ||
		pr.CodexReviewFindings[0].URL != "https://github.com/digitaldrywood/detent/pull/187#pullrequestreview-1" {
		t.Fatalf("PullRequest.CodexReviewFindings = %#v, want P1 review finding", pr.CodexReviewFindings)
	}

	requests := server.requests()
	if len(requests) != 9 {
		t.Fatalf("request count = %d, want project query plus paged PR REST requests", len(requests))
	}
}

func TestConnectorCachesPullRequestStatusByHeadSHA(t *testing.T) {
	t.Parallel()

	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"First issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`
	pullsBody := `[{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}}]`
	pullBody := `{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all", body: pullsBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/187", body: pullBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"success"}]}`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/sha-187/statuses?per_page=100", body: `[]`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/187/reviews?per_page=100", body: `[]`},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 187),
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all", body: pullsBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/187", body: pullBody},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
	})

	for range 2 {
		got, err := c.FetchCandidateIssues(context.Background())
		if err != nil {
			t.Fatalf("FetchCandidateIssues() error = %v", err)
		}
		if len(got) != 1 || got[0].PullRequest == nil || got[0].PullRequest.CIStatus != "pass" {
			t.Fatalf("FetchCandidateIssues() = %#v, want cached hydrated PR", got)
		}
	}

	requests := server.requests()
	if len(requests) != 10 {
		t.Fatalf("request count = %d, want second fetch to reuse PR status cache", len(requests))
	}
	for _, pattern := range []string{"/check-runs", "/statuses", "/reviews"} {
		count := 0
		for _, request := range requests {
			path, _ := request["path"].(string)
			if strings.Contains(path, pattern) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%s request count = %d, want 1; requests = %#v", pattern, count, requests)
		}
	}
}

func TestConnectorFetchCandidateIssuesMarksBranchPullRequestHydrationUnavailableWhenRESTBudgetReserved(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/rate_limit",
			headers: map[string]string{
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Remaining": "900",
				"X-RateLimit-Used":      "4100",
				"X-RateLimit-Resource":  "core",
			},
			body: `{}`,
		},
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"First issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:             "PVT_1",
		ActiveStates:            []string{"Todo"},
		RESTMinRemainingReserve: 1000,
	})
	if err := c.client.REST(context.Background(), http.MethodGet, "/rate_limit", nil, nil); err != nil {
		t.Fatalf("REST() seed rate limit error = %v", err)
	}

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}
	if got[0].PullRequest == nil {
		t.Fatal("PullRequest = nil, want hydration-unavailable marker")
	}
	if got[0].PullRequest.HydrationUnavailableReason != "rest_budget_reserved" {
		t.Fatalf("HydrationUnavailableReason = %q, want rest_budget_reserved", got[0].PullRequest.HydrationUnavailableReason)
	}
	if got[0].PullRequest.Number != 0 {
		t.Fatalf("PullRequest.Number = %d, want unknown PR number while branch matching is skipped", got[0].PullRequest.Number)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want rate limit seed plus project query", len(requests))
	}
	for _, request := range requests {
		path, _ := request["path"].(string)
		if strings.Contains(path, "/pulls?") {
			t.Fatalf("request path = %q, want pull request list skipped", path)
		}
	}
	usage := c.client.FlushRESTRateLimitUsage()
	if usage.RateLimited || !usage.ReserveHeld || usage.FanoutDeferred {
		t.Fatalf("RESTRateLimitUsage = %#v, want reserve-floor hold only", usage)
	}
	if got := restEndpointUsageCount(usage.Requests, "pull requests"); got != 1 {
		t.Fatalf("pull requests usage count = %d, want synthetic throttle; usage = %#v", got, usage.Requests)
	}
}

func TestConnectorAttachPullRequestsRotatesAfterRESTFanoutDeferral(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var pullRequests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/pulls/") && !strings.HasSuffix(path, "/reviews"):
			number := path[strings.LastIndex(path, "/")+1:]
			mu.Lock()
			pullRequests = append(pullRequests, number)
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"number":%s,"html_url":"https://github.com/digitaldrywood/detent/pull/%s","state":"open","head":{"ref":"detent/issue-%s","sha":"sha-%s"}}`, number, number, number, number)
		case strings.HasSuffix(path, "/check-runs"):
			_, _ = w.Write([]byte(`{"check_runs":[]}`))
		case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/reviews"), strings.HasSuffix(path, "/comments"):
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected REST path %s", r.URL.RequestURI())
		}
	}))
	t.Cleanup(server.Close)

	c, err := NewConnector(Config{
		Endpoint:                   server.URL,
		APIKey:                     "token",
		HTTPClient:                 server.Client(),
		RESTFanoutMaxRequests:      5,
		DisableConditionalRequests: true,
	})
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}

	newIssues := func() []connector.Issue {
		first := 101
		second := 102
		return []connector.Issue{
			{Identifier: "digitaldrywood/detent#1", PRNumber: &first},
			{Identifier: "digitaldrywood/detent#2", PRNumber: &second},
		}
	}

	first := newIssues()
	if err := c.attachPullRequests(context.Background(), first); err != nil {
		t.Fatalf("attachPullRequests() first error = %v", err)
	}
	if first[0].PullRequest == nil || first[0].PullRequest.HydrationUnavailableReason != "" {
		t.Fatalf("first pass issue 1 PullRequest = %#v, want hydrated", first[0].PullRequest)
	}
	if first[1].PullRequest == nil || first[1].PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonRESTFanoutDeferred {
		t.Fatalf("first pass issue 2 PullRequest = %#v, want fanout deferral", first[1].PullRequest)
	}
	c.FlushRESTRateLimitUsage()

	second := newIssues()
	if err := c.attachPullRequests(context.Background(), second); err != nil {
		t.Fatalf("attachPullRequests() second error = %v", err)
	}
	if second[1].PullRequest == nil || second[1].PullRequest.HydrationUnavailableReason != "" {
		t.Fatalf("second pass issue 2 PullRequest = %#v, want rotated hydration", second[1].PullRequest)
	}
	if second[0].PullRequest == nil || second[0].PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonRESTFanoutDeferred {
		t.Fatalf("second pass issue 1 PullRequest = %#v, want rotated fanout deferral", second[0].PullRequest)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := pullRequests, []string{"101", "102"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("hydrated pull requests = %v, want %v", got, want)
	}
}

func TestConnectorAttachPullRequestsPrioritizesSkippedBranchCandidate(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var detailedPullRequests []string
	var pullRequestLists int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case path == "/repos/digitaldrywood/detent/pulls":
			mu.Lock()
			pullRequestLists++
			mu.Unlock()
			_, _ = w.Write([]byte(`[{"number":202,"html_url":"https://github.com/digitaldrywood/detent/pull/202","state":"open","head":{"ref":"detent/digitaldrywood_detent_2","sha":"sha-202"}}]`))
		case strings.Contains(path, "/pulls/") && !strings.HasSuffix(path, "/reviews"):
			number := path[strings.LastIndex(path, "/")+1:]
			mu.Lock()
			detailedPullRequests = append(detailedPullRequests, number)
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"number":%s,"html_url":"https://github.com/digitaldrywood/detent/pull/%s","state":"open","head":{"ref":"detent/digitaldrywood_detent_%s","sha":"sha-%s"}}`, number, number, number, number)
		case strings.HasSuffix(path, "/check-runs"):
			_, _ = w.Write([]byte(`{"check_runs":[]}`))
		case strings.HasSuffix(path, "/statuses"), strings.HasSuffix(path, "/reviews"), strings.HasSuffix(path, "/comments"):
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected REST path %s", r.URL.RequestURI())
		}
	}))
	t.Cleanup(server.Close)

	c, err := NewConnector(Config{
		Endpoint:                   server.URL,
		APIKey:                     "token",
		HTTPClient:                 server.Client(),
		RESTFanoutMaxRequests:      6,
		DisableConditionalRequests: true,
	})
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}

	newIssues := func() []connector.Issue {
		linked := 101
		return []connector.Issue{
			{Identifier: "digitaldrywood/detent#1", PRNumber: &linked},
			{Identifier: "digitaldrywood/detent#2"},
		}
	}

	first := newIssues()
	if err := c.attachPullRequests(context.Background(), first); err != nil {
		t.Fatalf("attachPullRequests() first error = %v", err)
	}
	if first[1].PullRequest == nil || first[1].PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonRESTFanoutDeferred {
		t.Fatalf("first pass branch issue PullRequest = %#v, want fanout deferral", first[1].PullRequest)
	}
	c.FlushRESTRateLimitUsage()

	second := newIssues()
	if err := c.attachPullRequests(context.Background(), second); err != nil {
		t.Fatalf("attachPullRequests() second error = %v", err)
	}
	if second[1].PullRequest == nil || second[1].PullRequest.HydrationUnavailableReason != "" || second[1].PullRequest.Number != 202 {
		t.Fatalf("second pass branch issue PullRequest = %#v, want prioritized hydration", second[1].PullRequest)
	}
	if second[0].PullRequest == nil || second[0].PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonRESTFanoutDeferred {
		t.Fatalf("second pass linked issue PullRequest = %#v, want rotated fanout deferral", second[0].PullRequest)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := detailedPullRequests, []string{"101", "202"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("detailed pull requests = %v, want %v", got, want)
	}
	if pullRequestLists != 2 {
		t.Fatalf("pull request list calls = %d, want 2", pullRequestLists)
	}
}

func TestConnectorFetchCandidateIssuesStopsBranchPullRequestHydrationAfterSecondaryThrottle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 13, 0, 0, 0, time.UTC)
	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"First issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null},{"id":"PVTI_183","content":{"__typename":"Issue","id":"I_183","number":183,"title":"Second issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/183","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}},{"number":188,"html_url":"https://github.com/digitaldrywood/detent/pull/188","state":"open","head":{"ref":"detent/digitaldrywood_detent_183","sha":"sha-188"}}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/187",
			body:   `{"number":187,"html_url":"https://github.com/digitaldrywood/detent/pull/187","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-187"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-187/check-runs?per_page=100",
			status: http.StatusTooManyRequests,
			headers: map[string]string{
				"Retry-After":           "120",
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Used":      "264",
				"X-RateLimit-Remaining": "4736",
				"X-RateLimit-Resource":  "core",
			},
			body: `{"message":"secondary rate limit"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
		Now: func() time.Time {
			return now
		},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 2", len(got))
	}

	byID := map[string]connector.Issue{}
	for _, issue := range got {
		byID[issue.ID] = issue
	}
	for _, id := range []string{"I_182", "I_183"} {
		pr := byID[id].PullRequest
		if pr == nil {
			t.Fatalf("%s PullRequest = nil, want hydration marker", id)
			continue
		}
		if pr.HydrationUnavailableReason != connector.PullRequestHydrationReasonSecondaryThrottled {
			t.Fatalf("%s HydrationUnavailableReason = %q, want secondary_throttled", id, pr.HydrationUnavailableReason)
		}
		if pr.HydrationNextRetryAt == nil || !pr.HydrationNextRetryAt.After(now.Add(120*time.Second)) {
			t.Fatalf("%s HydrationNextRetryAt = %v, want retry-after plus jitter", id, pr.HydrationNextRetryAt)
		}
	}
	if byID["I_182"].PullRequest.Number != 187 {
		t.Fatalf("I_182 PullRequest.Number = %d, want hydrated PR number 187", byID["I_182"].PullRequest.Number)
	}
	if byID["I_183"].PullRequest.Number != 0 {
		t.Fatalf("I_183 PullRequest.Number = %d, want no second PR hydration after circuit trip", byID["I_183"].PullRequest.Number)
	}

	requests := server.requests()
	checkRunRequests := 0
	for _, request := range requests {
		path, _ := request["path"].(string)
		if strings.Contains(path, "/pulls/188") || strings.Contains(path, "sha-188") {
			t.Fatalf("unexpected request after circuit trip: %#v", request)
		}
		if strings.Contains(path, "/check-runs") {
			checkRunRequests++
		}
	}
	if checkRunRequests != 1 {
		t.Fatalf("check-runs requests = %d, want only first PR status attempt; requests = %#v", checkRunRequests, requests)
	}
}

func TestConnectorFetchFreshIssuesByStatesRechecksPullRequestStatusForPromotion(t *testing.T) {
	t.Parallel()

	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_401","content":{"__typename":"Issue","id":"I_401","number":401,"title":"Human review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/401","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":411,"url":"https://github.com/digitaldrywood/detent/pull/411","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`
	pullBody := `{"number":411,"html_url":"https://github.com/digitaldrywood/detent/pull/411","state":"open","head":{"ref":"detent/digitaldrywood_detent_401","sha":"head-current"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"success"}]}`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100", body: `[]`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411/reviews?per_page=100", body: `[]`},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 411),
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"failure"}]}`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100", body: `[]`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411/reviews?per_page=100", body: `[{"body":"[P1] New blocking finding.","html_url":"https://github.com/digitaldrywood/detent/pull/411#pullrequestreview-3","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"head-current","submitted_at":"2026-06-24T22:30:00Z"}]`},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	first, err := c.FetchFreshIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchFreshIssuesByStates() first error = %v", err)
	}
	if len(first) != 1 || first[0].PullRequest == nil {
		t.Fatalf("FetchFreshIssuesByStates() first = %#v, want hydrated PR", first)
	}
	if first[0].PullRequest.CIStatus != "pass" || first[0].PullRequest.CodexReviewState != "" {
		t.Fatalf("first PullRequest = %#v, want pass with no review finding", first[0].PullRequest)
	}

	second, err := c.FetchFreshIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchFreshIssuesByStates() second error = %v", err)
	}
	if len(second) != 1 || second[0].PullRequest == nil {
		t.Fatalf("FetchFreshIssuesByStates() second = %#v, want hydrated PR", second)
	}
	pr := second[0].PullRequest
	if pr.CIStatus != "fail" || pr.CodexReviewState != "P1" {
		t.Fatalf("second PullRequest status = CI %q review %q, want fail/P1", pr.CIStatus, pr.CodexReviewState)
	}
	if len(pr.CodexReviewFindings) != 1 || pr.CodexReviewFindings[0].Body != "[P1] New blocking finding." {
		t.Fatalf("second CodexReviewFindings = %#v, want new P1 finding", pr.CodexReviewFindings)
	}

	requests := server.requests()
	for _, pattern := range []string{"/check-runs", "/statuses", "/reviews"} {
		count := 0
		for _, request := range requests {
			path, _ := request["path"].(string)
			if strings.Contains(path, pattern) {
				count++
			}
		}
		if count != 2 {
			t.Fatalf("%s request count = %d, want fresh status fetch each call; requests = %#v", pattern, count, requests)
		}
	}
}

func TestConnectorFreshPullRequestStatusReconcilesDeletedChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		secondCheckState int
		wantError        bool
		wantCIStatus     string
		wantPublishedCI  string
		wantRunning      []string
	}{
		{
			name:            "authoritative snapshot removes deleted pending check",
			wantCIStatus:    "success",
			wantPublishedCI: "pass",
		},
		{
			name:             "failed snapshot retains pending check",
			secondCheckState: http.StatusServiceUnavailable,
			wantError:        true,
			wantCIStatus:     "pending",
			wantPublishedCI:  "pending",
			wantRunning:      []string{"Request Codex security review"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var checkRunPageOneRequests atomic.Int64
			var checkRunPageTwoRequests atomic.Int64
			var logs bytes.Buffer
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100":
					if checkRunPageOneRequests.Add(1) == 1 {
						w.Header().Set("ETag", `"checks-page-1-v1"`)
						w.Header().Set("Link", `<`+server.URL+`/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100&page=2>; rel="next"`)
						_, _ = w.Write([]byte(`{"check_runs":[{"id":2,"name":"Unit tests","status":"completed","conclusion":"success"}]}`))
						return
					}
					if tt.secondCheckState != 0 {
						w.WriteHeader(tt.secondCheckState)
						_, _ = w.Write([]byte(`{"message":"temporary upstream failure"}`))
						return
					}
					if r.Header.Get("If-None-Match") != "" {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					w.Header().Set("ETag", `"checks-page-1-v2"`)
					w.Header().Set("Link", `<`+server.URL+`/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100&page=2>; rel="next"`)
					_, _ = w.Write([]byte(`{"check_runs":[{"id":2,"name":"Unit tests","status":"completed","conclusion":"success"}]}`))
				case "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100&page=2":
					if checkRunPageTwoRequests.Add(1) == 1 {
						w.Header().Set("ETag", `"checks-page-2-v1"`)
						_, _ = w.Write([]byte(`{"check_runs":[{"id":1,"name":"Request Codex security review","status":"queued"}]}`))
						return
					}
					if r.Header.Get("If-None-Match") != "" {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					w.Header().Set("ETag", `"checks-page-2-v2"`)
					_, _ = w.Write([]byte(`{"check_runs":[]}`))
				case "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100",
					"/repos/digitaldrywood/detent/pulls/2028/reviews?per_page=100",
					"/repos/digitaldrywood/detent/issues/2028/comments?per_page=100":
					_, _ = w.Write([]byte(`[]`))
				default:
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"message":"not found"}`))
				}
			}))
			t.Cleanup(server.Close)

			githubConnector, err := NewConnector(Config{
				Endpoint:   server.URL,
				APIKey:     "token",
				HTTPClient: server.Client(),
				Logger:     slog.New(slog.NewTextHandler(&logs, nil)),
			})
			if err != nil {
				t.Fatalf("NewConnector() error = %v", err)
			}
			pullRequest := pullRequestNode{Number: 2028, HeadSHA: "head-current"}
			repo := pullRequestRepo{Owner: "digitaldrywood", Name: "detent"}

			if err := githubConnector.populatePullRequestStatus(context.Background(), repo, &pullRequest, false); err != nil {
				t.Fatalf("populatePullRequestStatus() first error = %v", err)
			}
			if pullRequest.CI.State != "pending" || !slices.Equal(pullRequest.CI.RunningChecks, []string{"Request Codex security review"}) {
				t.Fatalf("first CI = %#v, want pending security review", pullRequest.CI)
			}

			err = githubConnector.populatePullRequestStatus(context.Background(), repo, &pullRequest, false)
			if tt.wantError && err == nil {
				t.Fatal("populatePullRequestStatus() second error = nil, want error")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("populatePullRequestStatus() second error = %v", err)
			}
			if pullRequest.CI.State != tt.wantCIStatus {
				t.Fatalf("second CI state = %q, want %q", pullRequest.CI.State, tt.wantCIStatus)
			}
			if !slices.Equal(pullRequest.CI.RunningChecks, tt.wantRunning) {
				t.Fatalf("second RunningChecks = %#v, want %#v", pullRequest.CI.RunningChecks, tt.wantRunning)
			}
			issue := connector.Issue{}
			attachPullRequestToIssue(&issue, repo, pullRequest)
			if issue.PullRequest.CIStatus != tt.wantPublishedCI || !slices.Equal(issue.PullRequest.RunningChecks, tt.wantRunning) {
				t.Fatalf("published PullRequest = %#v, want CI %q and running %#v", issue.PullRequest, tt.wantPublishedCI, tt.wantRunning)
			}
			if tt.secondCheckState == 0 {
				for _, fragment := range []string{
					"github pull request checks reconciled",
					"request_purpose=reconcile_current_head_checks",
					`removed_running_checks="[Request Codex security review]"`,
				} {
					if !strings.Contains(logs.String(), fragment) {
						t.Fatalf("logs missing %q:\n%s", fragment, logs.String())
					}
				}
			}
		})
	}
}

func TestConnectorFetchFreshIssuesByStatesUsesCachedPullRequestStatusAfterRateLimit(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.UTC)
	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_401","content":{"__typename":"Issue","id":"I_401","number":401,"title":"Human review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/401","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":411,"url":"https://github.com/digitaldrywood/detent/pull/411","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`
	pullBody := `{"number":411,"html_url":"https://github.com/digitaldrywood/detent/pull/411","state":"open","head":{"ref":"detent/digitaldrywood_detent_401","sha":"head-current"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"success"}]}`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100", body: `[]`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411/reviews?per_page=100", body: `[]`},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 411),
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100",
			status: http.StatusTooManyRequests,
			headers: map[string]string{
				"Retry-After":           "120",
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Used":      "264",
				"X-RateLimit-Remaining": "4736",
				"X-RateLimit-Resource":  "core",
			},
			body: `{"message":"secondary rate limit"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		Now: func() time.Time {
			return now
		},
	})

	first, err := c.FetchFreshIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchFreshIssuesByStates() first error = %v", err)
	}
	if len(first) != 1 || first[0].PullRequest == nil || first[0].PullRequest.CIStatus != "pass" {
		t.Fatalf("FetchFreshIssuesByStates() first = %#v, want cached hydrated PR", first)
	}

	second, err := c.FetchFreshIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchFreshIssuesByStates() second error = %v", err)
	}
	if len(second) != 1 || second[0].PullRequest == nil {
		t.Fatalf("FetchFreshIssuesByStates() second = %#v, want hydrated PR", second)
	}
	pr := second[0].PullRequest
	if pr.HydrationUnavailableReason != "" {
		t.Fatalf("HydrationUnavailableReason = %q, want cached status without unavailable marker", pr.HydrationUnavailableReason)
	}
	if pr.HydrationDegradedReason != connector.PullRequestHydrationReasonStaleCachedPullData {
		t.Fatalf("HydrationDegradedReason = %q, want stale cached pull request marker", pr.HydrationDegradedReason)
	}
	if pr.HydrationNextRetryAt == nil || !pr.HydrationNextRetryAt.After(now.Add(120*time.Second)) {
		t.Fatalf("HydrationNextRetryAt = %v, want retry-after plus jitter", pr.HydrationNextRetryAt)
	}
	if pr.CIStatus != "pass" {
		t.Fatalf("CIStatus = %q, want cached pass", pr.CIStatus)
	}
}

func TestConnectorFetchIssuesByStatesCircuitBreaksPullRequestHydrationAfterSecondaryThrottle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 11, 0, 0, 0, time.UTC)
	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_401","content":{"__typename":"Issue","id":"I_401","number":401,"title":"Human review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/401","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":411,"url":"https://github.com/digitaldrywood/detent/pull/411","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`
	pullBody := `{"number":411,"html_url":"https://github.com/digitaldrywood/detent/pull/411","state":"open","head":{"ref":"detent/digitaldrywood_detent_401","sha":"head-current"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100",
			status: http.StatusTooManyRequests,
			headers: map[string]string{
				"Retry-After":           "120",
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Used":      "264",
				"X-RateLimit-Remaining": "4736",
				"X-RateLimit-Resource":  "core",
			},
			body: `{"message":"secondary rate limit"}`,
		},
		{body: projectBody},
		{body: projectBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411", body: pullBody},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"success"}]}`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100", body: `[]`},
		{method: http.MethodGet, path: "/repos/digitaldrywood/detent/pulls/411/reviews?per_page=100", body: `[]`},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 411),
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		Now: func() time.Time {
			return now
		},
	})

	first, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() first error = %v", err)
	}
	if len(first) != 1 || first[0].PullRequest == nil {
		t.Fatalf("FetchIssuesByStates() first = %#v, want PR hydration marker", first)
	}
	pr := first[0].PullRequest
	if pr.HydrationUnavailableReason != "secondary_throttled" {
		t.Fatalf("HydrationUnavailableReason = %q, want secondary_throttled", pr.HydrationUnavailableReason)
	}
	if pr.HydrationNextRetryAt == nil {
		t.Fatal("HydrationNextRetryAt = nil, want retry deadline")
	}
	retryAt := *pr.HydrationNextRetryAt
	if !retryAt.After(now.Add(120*time.Second)) || retryAt.After(now.Add(151*time.Second)) {
		t.Fatalf("HydrationNextRetryAt = %v, want retry-after plus jitter", retryAt)
	}

	beforeCooldownRequests := len(server.requests())
	second, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() second error = %v", err)
	}
	if len(second) != 1 || second[0].PullRequest == nil {
		t.Fatalf("FetchIssuesByStates() second = %#v, want PR hydration marker", second)
	}
	if second[0].PullRequest.HydrationUnavailableReason != "secondary_throttled" {
		t.Fatalf("second HydrationUnavailableReason = %q, want secondary_throttled", second[0].PullRequest.HydrationUnavailableReason)
	}
	if len(server.requests()) != beforeCooldownRequests+1 {
		t.Fatalf("request count after cooldown skip = %d, want one project refresh; requests = %#v", len(server.requests()), server.requests())
	}

	now = retryAt.Add(time.Second)
	third, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() third error = %v", err)
	}
	if len(third) != 1 || third[0].PullRequest == nil {
		t.Fatalf("FetchIssuesByStates() third = %#v, want hydrated PR", third)
	}
	pr = third[0].PullRequest
	if pr.HydrationUnavailableReason != "" || pr.HydrationNextRetryAt != nil {
		t.Fatalf("third hydration state = reason %q retry %v, want cleared", pr.HydrationUnavailableReason, pr.HydrationNextRetryAt)
	}
	if pr.CIStatus != "pass" {
		t.Fatalf("third CIStatus = %q, want pass", pr.CIStatus)
	}

	requests := server.requests()
	checkRunRequests := 0
	for _, request := range requests {
		path, _ := request["path"].(string)
		if strings.Contains(path, "/check-runs") {
			checkRunRequests++
		}
	}
	if checkRunRequests != 2 {
		t.Fatalf("check-runs requests = %d, want one failing call and one retry after cooldown; requests = %#v", checkRunRequests, requests)
	}
}

func TestConnectorFetchIssuesByStatesMarksLinkedPullRequestHydrationSecondaryThrottled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	projectBody := `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_401","content":{"__typename":"Issue","id":"I_401","number":401,"title":"Human review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/401","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":411,"url":"https://github.com/digitaldrywood/detent/pull/411","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: projectBody},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/411",
			status: http.StatusTooManyRequests,
			headers: map[string]string{
				"Retry-After":           "120",
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Used":      "264",
				"X-RateLimit-Remaining": "4736",
				"X-RateLimit-Resource":  "core",
			},
			body: `{"message":"secondary rate limit"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		Now: func() time.Time {
			return now
		},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want retained linked PR shell")
		return
	}
	if pr.Number != 411 {
		t.Fatalf("PullRequest.Number = %d, want 411", pr.Number)
	}
	if pr.HydrationUnavailableReason != connector.PullRequestHydrationReasonSecondaryThrottled {
		t.Fatalf("HydrationUnavailableReason = %q, want secondary_throttled", pr.HydrationUnavailableReason)
	}
	if pr.HydrationNextRetryAt == nil || !pr.HydrationNextRetryAt.After(now.Add(120*time.Second)) {
		t.Fatalf("HydrationNextRetryAt = %v, want retry-after plus jitter", pr.HydrationNextRetryAt)
	}
	if got[0].PRNumber == nil || *got[0].PRNumber != 411 {
		t.Fatalf("PRNumber = %v, want 411", got[0].PRNumber)
	}
}

func TestConnectorFetchIssuesByStatesAttachesPipelinePullRequest(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_182","content":{"__typename":"Issue","id":"I_182","number":182,"title":"Review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/182","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":{"name":"Reviewing"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[{"number":190,"html_url":"https://github.com/digitaldrywood/detent/pull/190","state":"open","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-190"}}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/190",
			body:   `{"number":190,"html_url":"https://github.com/digitaldrywood/detent/pull/190","state":"open","mergeable_state":"dirty","head":{"ref":"detent/digitaldrywood_detent_182","sha":"sha-190"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-190/check-runs?per_page=100",
			body:   `{"check_runs":[{"name":"Verify (ubuntu-latest)","status":"completed","conclusion":"success","created_at":"2026-06-05T10:59:00Z","started_at":"2026-06-05T11:00:00Z","completed_at":"2026-06-05T11:03:00Z"},{"name":"GoReleaser Snapshot","status":"completed","conclusion":"failure","created_at":"2026-06-05T11:00:30Z","started_at":"2026-06-05T11:03:30Z","completed_at":"2026-06-05T11:11:30Z"},{"name":"Portability Verify","status":"queued","conclusion":"","created_at":"2026-06-05T11:04:00Z","started_at":null,"completed_at":null},{"name":"Windows Core","status":"in_progress","conclusion":"","created_at":"2026-06-05T11:04:00Z","started_at":"2026-06-05T11:05:00Z","completed_at":null}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-190/statuses?per_page=100",
			body:   `[{"context":"ci/build","state":"success","created_at":"2026-06-05T11:00:00Z"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/190/reviews?per_page=100",
			body:   `[{"body":"[P1] Unsafe migration.","html_url":"https://github.com/digitaldrywood/detent/pull/190#pullrequestreview-2","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]","type":"Bot"},"commit_id":"sha-190","submitted_at":"2026-06-05T11:00:00Z"}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		Now:         func() time.Time { return time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC) },
		StateMap: map[string]string{
			"Human Review": "Reviewing",
		},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	pr := got[0].PullRequest
	if pr == nil || pr.Number != 190 || pr.CIStatus != "pending" || pr.CodexReviewState != "P1" {
		t.Fatalf("PullRequest = %#v, want PR 190 with pending CI and P1 review", pr)
	}
	if pr.CodexReviewSource != connector.PullRequestReviewSourceFormal {
		t.Fatalf("CodexReviewSource = %q, want formal review", pr.CodexReviewSource)
	}
	if pr.MergeableState != "dirty" {
		t.Fatalf("MergeableState = %q, want dirty from hydrated PR", pr.MergeableState)
	}
	if pr.CIDurationSeconds != 0 {
		t.Fatalf("CIDurationSeconds = %d, want 0 while checks are running", pr.CIDurationSeconds)
	}
	if pr.CIQueueSeconds != 60 {
		t.Fatalf("CIQueueSeconds = %d, want 60", pr.CIQueueSeconds)
	}
	if len(pr.SlowChecks) != 2 {
		t.Fatalf("SlowChecks len = %d, want 2: %#v", len(pr.SlowChecks), pr.SlowChecks)
	}
	if pr.SlowChecks[0].Name != "GoReleaser Snapshot" || pr.SlowChecks[0].DurationSeconds != 480 || pr.SlowChecks[0].QueueSeconds != 180 {
		t.Fatalf("SlowChecks[0] = %#v, want GoReleaser Snapshot 480s active and 180s queued", pr.SlowChecks[0])
	}
	if pr.SlowChecks[1].Name != "Verify (ubuntu-latest)" || pr.SlowChecks[1].DurationSeconds != 180 || pr.SlowChecks[1].QueueSeconds != 60 {
		t.Fatalf("SlowChecks[1] = %#v, want Verify 180s active and 60s queued", pr.SlowChecks[1])
	}
	if len(pr.RunningChecks) != 1 || pr.RunningChecks[0] != "Windows Core" {
		t.Fatalf("RunningChecks = %#v, want Windows Core", pr.RunningChecks)
	}
	if len(pr.UnstartedChecks) != 1 || pr.UnstartedChecks[0].Name != "Portability Verify" || pr.UnstartedChecks[0].QueueSeconds != 56*60 {
		t.Fatalf("UnstartedChecks = %#v, want Portability Verify queued 56 minutes", pr.UnstartedChecks)
	}
	if pr.UnstartedCheckCount != 1 {
		t.Fatalf("UnstartedCheckCount = %d, want 1", pr.UnstartedCheckCount)
	}
	if len(pr.CodexReviewFindings) != 1 ||
		pr.CodexReviewFindings[0].Body != "[P1] Unsafe migration." ||
		pr.CodexReviewFindings[0].URL != "https://github.com/digitaldrywood/detent/pull/190#pullrequestreview-2" {
		t.Fatalf("PullRequest.CodexReviewFindings = %#v, want P1 review finding", pr.CodexReviewFindings)
	}
}

func TestCheckRunTelemetryReportsQueueAndCompletedSpan(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 6, 5, 10, 58, 0, 0, time.UTC)
	start := time.Date(2026, 6, 5, 11, 0, 0, 0, time.UTC)
	verifyDone := start.Add(3 * time.Minute)
	snapshotCreated := start.Add(time.Minute)
	snapshotStart := verifyDone.Add(30 * time.Second)
	snapshotDone := snapshotStart.Add(8 * time.Minute)

	summary := checkRunTelemetry([]restCheckRun{
		{Name: "Verify (ubuntu-latest)", Status: "completed", Conclusion: "success", CreatedAt: &created, StartedAt: &start, CompletedAt: &verifyDone},
		{Name: "GoReleaser Snapshot", Status: "completed", Conclusion: "success", CreatedAt: &snapshotCreated, StartedAt: &snapshotStart, CompletedAt: &snapshotDone},
	}, nil, time.Time{}, defaultUnstartedCheckThreshold)

	if summary.QueueSeconds != 120 {
		t.Fatalf("QueueSeconds = %d, want 120", summary.QueueSeconds)
	}
	if summary.DurationSeconds != 690 {
		t.Fatalf("DurationSeconds = %d, want 690", summary.DurationSeconds)
	}
	if len(summary.SlowChecks) != 2 || summary.SlowChecks[0].Name != "GoReleaser Snapshot" || summary.SlowChecks[0].QueueSeconds != 150 {
		t.Fatalf("SlowChecks = %#v, want snapshot first with queued runtime", summary.SlowChecks)
	}
	if len(summary.RunningChecks) != 0 {
		t.Fatalf("RunningChecks = %#v, want none", summary.RunningChecks)
	}
}

func TestCheckRunTelemetryDistinguishesUnstartedChecks(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 8, 20, 0, 0, 0, time.UTC)
	threshold := 15 * time.Minute
	oldQueuedAt := now.Add(-47 * time.Minute)
	recentQueuedAt := now.Add(-5 * time.Minute)
	oldStartedAt := now.Add(-2 * time.Hour)
	recentStartedAt := now.Add(-time.Minute)
	tests := []struct {
		name          string
		checkRuns     []restCheckRun
		wantRunning   []string
		wantUnstarted []connector.PullRequestCheck
		wantCount     int
	}{
		{
			name: "all checks in progress",
			checkRuns: []restCheckRun{
				{Name: "Portability Verify", Status: "in_progress", CreatedAt: &oldQueuedAt, StartedAt: &oldStartedAt},
				{Name: "Test", Status: "in_progress", CreatedAt: &oldQueuedAt, StartedAt: &recentStartedAt},
			},
			wantRunning: []string{"Portability Verify", "Test"},
		},
		{
			name: "all checks queued past threshold",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Portability Verify", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 2, Name: "Test", Status: "queued", CreatedAt: &oldQueuedAt},
			},
			wantUnstarted: []connector.PullRequestCheck{
				{ID: 1, Name: "Portability Verify", Status: "queued", QueueSeconds: 47 * 60},
				{ID: 2, Name: "Test", Status: "queued", QueueSeconds: 47 * 60},
			},
		},
		{
			name:        "queued under threshold",
			checkRuns:   []restCheckRun{{Name: "Portability Verify", Status: "queued", CreatedAt: &recentQueuedAt}},
			wantRunning: []string{"Portability Verify"},
		},
		{
			name: "mixed queued and running",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Portability Verify", Status: "queued", CreatedAt: &oldQueuedAt},
				{Name: "Test", Status: "in_progress", CreatedAt: &oldQueuedAt, StartedAt: &recentStartedAt},
			},
			wantRunning: []string{"Test"},
			wantUnstarted: []connector.PullRequestCheck{
				{ID: 1, Name: "Portability Verify", Status: "queued", QueueSeconds: 47 * 60},
			},
		},
		{
			name:        "queued check that later starts",
			checkRuns:   []restCheckRun{{Name: "Portability Verify", Status: "in_progress", CreatedAt: &oldQueuedAt, StartedAt: &recentStartedAt}},
			wantRunning: []string{"Portability Verify"},
		},
		{
			name: "unstarted checks are sorted and limited",
			checkRuns: []restCheckRun{
				{ID: 6, Name: "F", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 1, Name: "A", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 5, Name: "E", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 2, Name: "B", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 4, Name: "D", Status: "queued", CreatedAt: &oldQueuedAt},
				{ID: 3, Name: "C", Status: "queued", CreatedAt: &oldQueuedAt},
			},
			wantUnstarted: []connector.PullRequestCheck{
				{ID: 1, Name: "A", Status: "queued", QueueSeconds: 47 * 60},
				{ID: 2, Name: "B", Status: "queued", QueueSeconds: 47 * 60},
				{ID: 3, Name: "C", Status: "queued", QueueSeconds: 47 * 60},
				{ID: 4, Name: "D", Status: "queued", QueueSeconds: 47 * 60},
				{ID: 5, Name: "E", Status: "queued", QueueSeconds: 47 * 60},
			},
			wantCount: 6,
		},
		{name: "empty check run list"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			summary := checkRunTelemetry(tt.checkRuns, nil, now, threshold)
			if !slices.Equal(summary.RunningChecks, tt.wantRunning) {
				t.Fatalf("RunningChecks = %#v, want %#v", summary.RunningChecks, tt.wantRunning)
			}
			if !slices.Equal(summary.UnstartedChecks, tt.wantUnstarted) {
				t.Fatalf("UnstartedChecks = %#v, want %#v", summary.UnstartedChecks, tt.wantUnstarted)
			}
			wantCount := tt.wantCount
			if wantCount == 0 {
				wantCount = len(tt.wantUnstarted)
			}
			if summary.UnstartedCount != wantCount {
				t.Fatalf("UnstartedCount = %d, want %d", summary.UnstartedCount, wantCount)
			}
		})
	}
}

func TestCheckRunsStateTreatsStaleSuccessfulCheckRunAsSuccess(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 7, 0, 45, 0, 0, time.UTC)
	completed := time.Date(2026, 7, 7, 0, 49, 24, 0, time.UTC)

	checkRuns := []restCheckRun{{
		Name:        "Installer Smoke (ubuntu-latest)",
		Status:      "in_progress",
		Conclusion:  "success",
		StartedAt:   &started,
		CompletedAt: &completed,
	}}

	if got := checkRunsState(checkRuns); got != "success" {
		t.Fatalf("checkRunsState() = %q, want success", got)
	}
	telemetry := checkRunTelemetry(checkRuns, nil, time.Time{}, defaultUnstartedCheckThreshold)
	if len(telemetry.RunningChecks) != 0 {
		t.Fatalf("RunningChecks = %#v, want none", telemetry.RunningChecks)
	}
	if telemetry.DurationSeconds != 264 {
		t.Fatalf("DurationSeconds = %d, want 264", telemetry.DurationSeconds)
	}
}

func TestCheckRunsStateUsesSettledContextResults(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	tests := []struct {
		name      string
		checkRuns []restCheckRun
		want      string
	}{
		{
			name: "pending current cycle supersedes settled failure",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &older},
				{ID: 2, Name: "Checks", Status: "in_progress", StartedAt: &newer},
			},
			want: "pending",
		},
		{
			name: "newer settled success supersedes pending run",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "in_progress", StartedAt: &older},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &newer},
			},
			want: "success",
		},
		{
			name: "newer settled failure supersedes pending run",
			checkRuns: []restCheckRun{
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &newer},
				{ID: 1, Name: "Checks", Status: "queued", StartedAt: &older},
			},
			want: "failure",
		},
		{
			name: "pending context keeps a different failure unsettled",
			checkRuns: []restCheckRun{
				{Name: "Checks", Status: "completed", Conclusion: "failure"},
				{Name: "Test", Status: "queued"},
			},
			want: "pending",
		},
		{
			name: "cancelled artifact does not shadow success",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Changed-file coverage", Status: "completed", Conclusion: "success", StartedAt: &older},
				{ID: 2, Name: "Changed-file coverage", Status: "completed", Conclusion: "cancelled", StartedAt: &newer},
			},
			want: "success",
		},
		{
			name: "skipped artifact does not shadow success",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Test", Status: "completed", Conclusion: "success", StartedAt: &older},
				{ID: 2, Name: "Test", Status: "completed", Conclusion: "skipped", StartedAt: &newer},
			},
			want: "success",
		},
		{
			name: "newer success supersedes ignored workflow runs",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "cancelled", StartedAt: &older},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "skipped", StartedAt: &older},
				{ID: 3, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &newer},
			},
			want: "success",
		},
		{
			name: "newer settled success supersedes failure",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &older},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &newer},
			},
			want: "success",
		},
		{
			name: "newer settled failure supersedes success",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &older},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &newer},
			},
			want: "failure",
		},
		{
			name: "only ignored conclusions are non-blocking",
			checkRuns: []restCheckRun{
				{Name: "Checks", Status: "completed", Conclusion: "cancelled"},
				{Name: "Test", Status: "completed", Conclusion: "skipped"},
			},
			want: "success",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := checkRunsState(tt.checkRuns); got != tt.want {
				t.Fatalf("checkRunsState() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompletedFailedCheckRunIgnoresNonFailureArtifacts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		conclusion string
		want       bool
	}{
		{conclusion: "failure", want: true},
		{conclusion: "timed_out", want: true},
		{conclusion: "cancelled"},
		{conclusion: "canceled"},
		{conclusion: "skipped"},
	}

	for _, tt := range tests {
		t.Run(tt.conclusion, func(t *testing.T) {
			t.Parallel()

			checkRun := restCheckRun{Status: "completed", Conclusion: tt.conclusion}
			if got := completedFailedCheckRun(checkRun); got != tt.want {
				t.Fatalf("completedFailedCheckRun() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestCIStateWaitsForAllContextsToSettle(t *testing.T) {
	t.Parallel()

	statuses := []restCommitStatus{
		{Context: "Checks", State: "failure"},
		{Context: "Test", State: "pending"},
	}
	if got := commitStatusesState(statuses); got != "pending" {
		t.Fatalf("commitStatusesState() = %q, want pending", got)
	}
	if got := combinedCIState("failure", "pending"); got != "pending" {
		t.Fatalf("combinedCIState() = %q, want pending", got)
	}
}

func TestCheckRunTelemetryExcludesSupersededFailures(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	olderDone := older.Add(10 * time.Minute)
	newer := older.Add(time.Minute)
	newerDone := newer.Add(time.Minute)
	summary := checkRunTelemetry([]restCheckRun{
		{ID: 1, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &older, CompletedAt: &olderDone},
		{ID: 2, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &newer, CompletedAt: &newerDone},
	}, nil, time.Time{}, defaultUnstartedCheckThreshold)

	if len(summary.SlowChecks) != 1 || summary.SlowChecks[0].Conclusion != "success" {
		t.Fatalf("SlowChecks = %#v, want only latest settled success", summary.SlowChecks)
	}
}

func TestTransientCheckRunFailuresExcludeSupersededFailures(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	checkRuns := []restCheckRun{
		{ID: 1, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: &older, Output: checkRunOutput{Text: "signal: killed"}},
		{ID: 2, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: &newer},
	}
	c := &Connector{}

	got, err := c.transientCheckRunFailures(context.Background(), pullRequestRepo{}, checkRuns)
	if err != nil {
		t.Fatalf("transientCheckRunFailures() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("transientCheckRunFailures() = %#v, want none", got)
	}
}

func TestCheckRunTelemetryUsesWorkflowRunTimingForQueue(t *testing.T) {
	t.Parallel()

	runCreated := time.Date(2026, 6, 5, 10, 58, 0, 0, time.UTC)
	runStarted := runCreated.Add(90 * time.Second)
	checkStarted := time.Date(2026, 6, 5, 11, 0, 0, 0, time.UTC)
	checkCompleted := checkStarted.Add(3 * time.Minute)

	summary := checkRunTelemetry([]restCheckRun{
		{Name: "Verify (ubuntu-latest)", Status: "completed", Conclusion: "success", StartedAt: &checkStarted, CompletedAt: &checkCompleted},
	}, []restWorkflowRun{
		{ID: 28196652213, CreatedAt: &runCreated, RunStartedAt: &runStarted},
	}, time.Time{}, defaultUnstartedCheckThreshold)

	if summary.QueueSeconds != 90 {
		t.Fatalf("QueueSeconds = %d, want 90", summary.QueueSeconds)
	}
	if summary.DurationSeconds != 180 {
		t.Fatalf("DurationSeconds = %d, want 180", summary.DurationSeconds)
	}
}

func TestRequiredStatusCheckFailures(t *testing.T) {
	t.Parallel()

	staleCompleted := time.Date(2026, 7, 7, 0, 49, 24, 0, time.UTC)
	tests := []struct {
		name       string
		checkRuns  []restCheckRun
		statuses   []restCommitStatus
		required   []string
		wantState  string
		wantChecks []connector.PullRequestCheck
	}{
		{
			name:      "all required check runs succeeded",
			checkRuns: []restCheckRun{{Name: "Lint", Status: "completed", Conclusion: "success"}},
			required:  []string{"Lint"},
		},
		{
			name: "stale successful required check run succeeded",
			checkRuns: []restCheckRun{{
				Name:        "Installer Smoke (ubuntu-latest)",
				Status:      "in_progress",
				Conclusion:  "success",
				CompletedAt: &staleCompleted,
			}},
			required: []string{"Installer Smoke (ubuntu-latest)"},
		},
		{
			name:      "missing required check blocks as pending",
			checkRuns: []restCheckRun{{Name: "Lint", Status: "completed", Conclusion: "success"}},
			required:  []string{"Lint", "Windows Core"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{{
				Name:       "Windows Core",
				Status:     "missing",
				Conclusion: "missing",
			}},
		},
		{
			name:      "running required check blocks as pending",
			checkRuns: []restCheckRun{{Name: "Windows Core", Status: "in_progress"}},
			required:  []string{"Windows Core"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{{
				Name:   "Windows Core",
				Status: "in_progress",
			}},
		},
		{
			name:      "skipped required check remains pending",
			checkRuns: []restCheckRun{{Name: "Windows Core", Status: "completed", Conclusion: "skipped"}},
			required:  []string{"Windows Core"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{{
				Name:   "Windows Core",
				Status: "pending",
			}},
		},
		{
			name:      "skipped required check overrides successful legacy status",
			checkRuns: []restCheckRun{{Name: "Windows Core", Status: "completed", Conclusion: "skipped"}},
			statuses:  []restCommitStatus{{Context: "Windows Core", State: "success"}},
			required:  []string{"Windows Core"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{{
				Name:   "Windows Core",
				Status: "pending",
			}},
		},
		{
			name: "pending required check wins over settled failure",
			checkRuns: []restCheckRun{
				{Name: "Checks", Status: "completed", Conclusion: "failure"},
				{Name: "Test", Status: "in_progress"},
			},
			required:  []string{"Checks", "Test"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{
				{Name: "Checks", Status: "completed", Conclusion: "failure"},
				{Name: "Test", Status: "in_progress"},
			},
		},
		{
			name: "latest skipped required run blocks despite older success",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "success"},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "skipped"},
			},
			required:  []string{"Checks"},
			wantState: "pending",
			wantChecks: []connector.PullRequestCheck{{
				Name:   "Checks",
				Status: "pending",
			}},
		},
		{
			name: "latest success replaces older ignored required runs",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "cancelled"},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "skipped"},
				{ID: 3, Name: "Checks", Status: "completed", Conclusion: "success"},
			},
			required: []string{"Checks"},
		},
		{
			name: "latest settled result wins regardless of response order",
			checkRuns: []restCheckRun{
				{ID: 1, Name: "Checks", Status: "completed", Conclusion: "failure", StartedAt: new(time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC))},
				{ID: 2, Name: "Checks", Status: "completed", Conclusion: "success", StartedAt: new(time.Date(2026, 7, 16, 18, 1, 0, 0, time.UTC))},
			},
			required: []string{"Checks"},
		},
		{
			name:      "neutral required check fails",
			checkRuns: []restCheckRun{{Name: "GoReleaser Snapshot", Status: "completed", Conclusion: "neutral"}},
			required:  []string{"GoReleaser Snapshot"},
			wantState: "failure",
			wantChecks: []connector.PullRequestCheck{{
				Name:       "GoReleaser Snapshot",
				Status:     "completed",
				Conclusion: "neutral",
			}},
		},
		{
			name:      "required commit status context fails",
			statuses:  []restCommitStatus{{Context: "release/check", State: "failure"}},
			required:  []string{"release/check"},
			wantState: "failure",
			wantChecks: []connector.PullRequestCheck{{
				Name:       "release/check",
				Status:     "failure",
				Conclusion: "failure",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotChecks := requiredStatusCheckFailures(tt.checkRuns, tt.statuses, tt.required)
			if !reflect.DeepEqual(gotChecks, tt.wantChecks) {
				t.Fatalf("requiredStatusCheckFailures() = %#v, want %#v", gotChecks, tt.wantChecks)
			}
			if gotState := requiredStatusCheckState(gotChecks); gotState != tt.wantState {
				t.Fatalf("requiredStatusCheckState() = %q, want %q", gotState, tt.wantState)
			}
		})
	}
}

func TestCheckRunWorkflowRunIDs(t *testing.T) {
	t.Parallel()

	got := checkRunWorkflowRunIDs([]restCheckRun{
		{DetailsURL: "https://github.com/digitaldrywood/detent/actions/runs/28196652213/job/83525095026"},
		{DetailsURL: "https://github.com/digitaldrywood/detent/actions/runs/28196652213/job/83525095027"},
		{DetailsURL: "https://github.com/digitaldrywood/detent/actions/runs/28196652214"},
		{DetailsURL: "https://example.com/not-actions"},
	})

	want := []int64{28196652213, 28196652214}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkRunWorkflowRunIDs() = %#v, want %#v", got, want)
	}
}

func TestConnectorPullRequestStatusCacheDebugLog(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	var logs bytes.Buffer
	c := &Connector{
		pullRequests: newPullRequestStatusCache(time.Minute, func() time.Time { return now }),
		logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
	repo := pullRequestRepo{Owner: "digitaldrywood", Name: "detent"}
	status := pullRequestStatus{
		ci: pullRequestCI{
			State:         "SUCCESS",
			CheckRunCount: 2,
		},
	}
	c.pullRequests.Set(repo, 726, "head-sha", status)
	pullRequest := pullRequestNode{Number: 726, HeadSHA: "head-sha"}

	if err := c.populatePullRequestStatus(context.Background(), repo, &pullRequest, true); err != nil {
		t.Fatalf("populatePullRequestStatus() error = %v", err)
	}

	logText := logs.String()
	for _, fragment := range []string{
		"github pull request status cache",
		"endpoint_family=pull_request_status_cache",
		"request_purpose=hydrate_pull_request_status",
		"repository=digitaldrywood/detent",
		"pr_number=726",
		"cache_hit=true",
		"avoidable_request=true",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logText)
		}
	}
	if pullRequest.CI.CheckRunCount != 2 {
		t.Fatalf("CheckRunCount = %d, want cached status", pullRequest.CI.CheckRunCount)
	}
}

func TestConnectorPullRequestStatusCacheRefreshesEditedCodexSummary(t *testing.T) {
	t.Parallel()

	const headSHA = "79f9eb2d4ad5317af5ff46f29aba3b91f4b413a0"
	completedBody := testCodexReviewSummaryBody(
		"✅ **Completed** <relative-time datetime=\"2026-09-02T22:35:58.561318Z\">2026-09-02T22:35:58.561318Z</relative-time>",
		headSHA[:7],
	)
	incompleteBody := testCodexReviewSummaryBody("🔄 **In progress**", headSHA[:7])
	commentResponse := func(body string, updatedAt string) string {
		return fmt.Sprintf(`[{"id":1,"body":%q,"html_url":"https://github.test/pull/389#issuecomment-1","user":{"login":"chatgpt-codex-connector[bot]","type":"Bot"},"created_at":"2026-09-02T16:27:27Z","updated_at":%q}]`, body, updatedAt)
	}
	statusResponses := func(commentBody string, commentETag string) []graphqlTestResponse {
		return []graphqlTestResponse{
			{method: http.MethodGet, path: "/repos/gopherguides/gopher-ai/commits/" + headSHA + "/check-runs?per_page=100", body: `{"check_runs":[{"status":"completed","conclusion":"success"}]}`},
			{method: http.MethodGet, path: "/repos/gopherguides/gopher-ai/commits/" + headSHA + "/statuses?per_page=100", body: `[]`},
			{method: http.MethodGet, path: "/repos/gopherguides/gopher-ai/pulls/389/reviews?per_page=100", body: `[]`},
			{method: http.MethodGet, path: "/repos/gopherguides/gopher-ai/issues/389/comments?per_page=100", headers: map[string]string{"ETag": commentETag}, body: commentBody},
		}
	}
	responses := statusResponses(commentResponse(completedBody, "2026-09-02T22:35:59Z"), `"summary-v1"`)
	responses = append(responses, statusResponses(commentResponse(incompleteBody, "2026-09-02T22:40:00Z"), `"summary-v2"`)...)
	server := newGraphQLTestServer(t, responses)

	now := time.Date(2026, 9, 2, 22, 36, 0, 0, time.UTC)
	c := newGitHubTestConnector(t, server, Config{Now: func() time.Time { return now }})
	repo := pullRequestRepo{Owner: "gopherguides", Name: "gopher-ai"}

	first := pullRequestNode{Number: 389, HeadSHA: headSHA}
	if err := c.populatePullRequestStatus(context.Background(), repo, &first, true); err != nil {
		t.Fatalf("first populatePullRequestStatus() error = %v", err)
	}
	if got := pullRequestCodexReviewSource(first); got != connector.PullRequestReviewSourceSummaryComment {
		t.Fatalf("first review source = %q, want summary comment", got)
	}

	cached := pullRequestNode{Number: 389, HeadSHA: headSHA}
	if err := c.populatePullRequestStatus(context.Background(), repo, &cached, true); err != nil {
		t.Fatalf("cached populatePullRequestStatus() error = %v", err)
	}
	if got := pullRequestCodexReviewState(cached); got != "COMMENTED" {
		t.Fatalf("cached review state = %q, want COMMENTED", got)
	}

	now = now.Add(githubCacheTTL + time.Second)
	refreshed := pullRequestNode{Number: 389, HeadSHA: headSHA}
	if err := c.populatePullRequestStatus(context.Background(), repo, &refreshed, true); err != nil {
		t.Fatalf("refreshed populatePullRequestStatus() error = %v", err)
	}
	if got := pullRequestCodexReviewState(refreshed); got != "" {
		t.Fatalf("refreshed review state = %q, want incomplete edit to invalidate evidence", got)
	}

	commentRequests := 0
	secondCommentETag := ""
	for _, request := range server.requests() {
		if request["path"] == "/repos/gopherguides/gopher-ai/issues/389/comments?per_page=100" {
			commentRequests++
			if commentRequests == 2 {
				secondCommentETag, _ = request["if_none_match"].(string)
			}
		}
	}
	if commentRequests != 2 {
		t.Fatalf("comment requests = %d, want one initial request and one post-expiry refresh", commentRequests)
	}
	if secondCommentETag != `"summary-v1"` {
		t.Fatalf("second comment If-None-Match = %q, want cached summary ETag", secondCommentETag)
	}
}

func TestConnectorFetchIssuesByStatesSurfacesStaleCodexReview(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_401","content":{"__typename":"Issue","id":"I_401","number":401,"title":"Human review issue","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/401","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"enhancement"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":411,"url":"https://github.com/digitaldrywood/detent/pull/411","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/411",
			body:   `{"number":411,"html_url":"https://github.com/digitaldrywood/detent/pull/411","state":"open","head":{"ref":"detent/digitaldrywood_detent_401","sha":"head-current"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100",
			body:   `{"check_runs":[{"status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/411/reviews?per_page=100",
			body:   `[{"body":"No blocking findings on an older head.","state":"COMMENTED","user":{"login":"chatgpt-codex-connector[bot]"},"commit_id":"head-previous","submitted_at":"2026-06-12T11:40:00Z"}]`,
		},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 411),
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})
	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want linked PR")
		return
	}
	if pr.HeadSHA != "head-current" {
		t.Fatalf("HeadSHA = %q, want head-current", pr.HeadSHA)
	}
	if pr.CIStatus != "pass" {
		t.Fatalf("CIStatus = %q, want pass", pr.CIStatus)
	}
	if pr.CodexReviewState != "" || pr.CodexReviewSubmittedAt != nil {
		t.Fatalf("current-head Codex review = %q at %v, want none", pr.CodexReviewState, pr.CodexReviewSubmittedAt)
	}
	if pr.LatestCodexReviewState != "COMMENTED" || pr.LatestCodexReviewCommitSHA != "head-previous" {
		t.Fatalf("latest Codex review = state %q commit %q, want COMMENTED/head-previous", pr.LatestCodexReviewState, pr.LatestCodexReviewCommitSHA)
	}
	wantSubmittedAt := time.Date(2026, 6, 12, 11, 40, 0, 0, time.UTC)
	if pr.LatestCodexReviewSubmittedAt == nil || !pr.LatestCodexReviewSubmittedAt.Equal(wantSubmittedAt) {
		t.Fatalf("LatestCodexReviewSubmittedAt = %v, want %v", pr.LatestCodexReviewSubmittedAt, wantSubmittedAt)
	}
}

func TestConnectorFetchIssuesByStatesLimitExhaustsProjectItemsBeforeSampling(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":true,"endCursor":"next"},"nodes":[{"id":"PVTI_370","content":{"__typename":"Issue","id":"I_370","number":370,"title":"Review issue","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/370","repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":371,"url":"https://github.com/digitaldrywood/detent/pull/371"}]}},"statusValue":{"name":"Human Review"},"priorityValue":null},{"id":"PVTI_387","content":{"__typename":"Issue","id":"I_387","number":387,"title":"Review issue 2","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/387","repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":{"name":"Human Review"},"priorityValue":null}]}}}}`,
		},
		{
			body: projectItemsPageResponse(false, "", nil),
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/371",
			body:   `{"number":371,"html_url":"https://github.com/digitaldrywood/detent/pull/371","state":"open","head":{"ref":"detent/digitaldrywood_detent_370","sha":"sha-371"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-371/check-runs?per_page=100",
			body:   `{"check_runs":[{"status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/sha-371/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/371/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 371),
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStatesLimit(context.Background(), []string{"Human Review"}, 1)
	if err != nil {
		t.Fatalf("FetchIssuesByStatesLimit() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStatesLimit() len = %d, want 1", len(got))
	}
	if got[0].PRNumber == nil || *got[0].PRNumber != 371 {
		t.Fatalf("PRNumber = %v, want linked PR 371", got[0].PRNumber)
	}
	pr := got[0].PullRequest
	if pr == nil || pr.Number != 371 || pr.CIStatus != "pass" {
		t.Fatalf("PullRequest = %#v, want hydrated linked PR 371 with passing CI", pr)
	}

	requests := server.requests()
	if len(requests) != 7 {
		t.Fatalf("request count = %d, want two project pages and linked PR status requests", len(requests))
	}
	if requests[0]["variables"].(map[string]any)["after"] != nil {
		t.Fatalf("first project request after = %v, want nil", requests[0]["variables"].(map[string]any)["after"])
	}
	if requests[1]["variables"].(map[string]any)["after"] != "next" {
		t.Fatalf("second project request after = %v, want next", requests[1]["variables"].(map[string]any)["after"])
	}
}

func TestConnectorFetchIssuesByStatesFindsAutoPromoteCandidateBeyondFirstProjectPage(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponse(true, "cursor-1", []string{
				projectIssueNode("PVTI_done", "I_done", 1491, "Older completed issue", "Done"),
			}),
		},
		{
			body: projectItemsPageResponse(false, "", []string{
				projectIssueNode("PVTI_review", "I_review", 1492, "Deep review issue", "Human Review"),
			}),
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[]`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "I_review" {
		t.Fatalf("FetchIssuesByStates() = %#v, want deep Human Review issue", got)
	}

	requests := server.requests()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want two project pages and pull request lookup", len(requests))
	}
	for index, request := range requests[:2] {
		variables := requestVariables(t, request)
		if _, ok := variables["query"]; ok {
			t.Fatalf("project request %d query = %v, want unfiltered pagination", index+1, variables["query"])
		}
	}
	if after := requestVariables(t, requests[1])["after"]; after != "cursor-1" {
		t.Fatalf("second project request after = %v, want cursor-1", after)
	}
}

func TestConnectorFetchIssuesByStatesScanCountsBeyondReturnedSample(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponseWithTotal(3, true, "cursor-1", []string{
				projectIssueNode("PVTI_done", "I_done", 1491, "Older completed issue", "Done"),
				projectIssueNode("PVTI_review_1", "I_review_1", 1492, "First review issue", "Human Review"),
			}),
		},
		{
			body: projectItemsPageResponseWithTotal(3, false, "", []string{
				projectIssueNode("PVTI_review_2", "I_review_2", 1493, "Second review issue", "Human Review"),
			}),
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[]`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	scan, err := c.FetchIssuesByStatesScan(context.Background(), []string{"Human Review"}, 1)
	if err != nil {
		t.Fatalf("FetchIssuesByStatesScan() error = %v", err)
	}
	if len(scan.Issues) != 1 || scan.Issues[0].ID != "I_review_1" {
		t.Fatalf("Issues = %#v, want one returned sample", scan.Issues)
	}
	if scan.ItemsFetched != 3 || scan.TotalItems != 3 {
		t.Fatalf("item counts = fetched %d total %d, want 3/3", scan.ItemsFetched, scan.TotalItems)
	}
	if scan.BoardCounts["Human Review"] != 2 || scan.BoardCounts["Done"] != 1 {
		t.Fatalf("BoardCounts = %#v, want Human Review=2 and Done=1", scan.BoardCounts)
	}
	if scan.EnumeratedCounts["Human Review"] != 2 {
		t.Fatalf("EnumeratedCounts = %#v, want Human Review=2", scan.EnumeratedCounts)
	}
}

func TestConnectorFetchIssuesByStatesReportsTruncatedProjectItems(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"totalCount":2,"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_done","content":{"__typename":"Issue","id":"I_done","number":1491,"title":"Older completed issue","state":"CLOSED","url":"https://github.com/digitaldrywood/detent/issues/1491","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Done"}}]}}}}`,
	}})
	var logs bytes.Buffer
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})

	_, err := c.FetchIssuesByStates(context.Background(), []string{"Human Review"})
	if !errors.Is(err, ErrProjectItemsTruncated) {
		t.Fatalf("FetchIssuesByStates() error = %v, want ErrProjectItemsTruncated", err)
	}
	for _, want := range []string{"github project item scan truncated", "fetched=1", "total=2"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs = %q, want containing %q", logs.String(), want)
		}
	}
}

func TestBranchMatchesIssuePrefixAcceptsCurrentAgentBranchShape(t *testing.T) {
	t.Parallel()

	prefix := detentIssueBranchPrefix("digitaldrywood/detent#506")
	if prefix != "detent/digitaldrywood_detent_506" {
		t.Fatalf("detentIssueBranchPrefix() = %q, want detent/digitaldrywood_detent_506", prefix)
	}

	for _, branch := range []string{
		"detent/digitaldrywood_detent_506",
		"detent/digitaldrywood_detent_506-fix",
		"detent/detent-digitaldrywood_detent_506-6bd1bec3c6d3",
		"detent/digitaldrywood-digitaldrywood_detent_506-6bd1bec3c6d3",
		"detent/506",
		"detent/506-fix",
	} {
		if !branchMatchesIssuePrefix(branch, prefix) {
			t.Fatalf("branchMatchesIssuePrefix(%q, %q) = false, want true", branch, prefix)
		}
	}

	for _, branch := range []string{
		"detent/digitaldrywood-digitaldrywood_detent_5060-6bd1bec3c6d3",
		"detent/digitaldrywood-digitaldrywood_detent_50-6bd1bec3c6d3",
		"detent/foo-digitaldrywood_detent_506-digitaldrywood_detent_123-6bd1bec3c6d3",
		"detent/digitaldrywood_detent_5060",
	} {
		if branchMatchesIssuePrefix(branch, prefix) {
			t.Fatalf("branchMatchesIssuePrefix(%q, %q) = true, want false", branch, prefix)
		}
	}
}

func TestConnectorFetchCandidateIssuesLimitsPullRequestPagination(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_1","number":1,"title":"Candidate","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/1","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Todo"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug:  "PVT_1",
		ActiveStates: []string{"Todo"},
	})

	got, err := c.FetchCandidateIssues(context.Background())
	if err != nil {
		t.Fatalf("FetchCandidateIssues() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchCandidateIssues() len = %d, want 1", len(got))
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want project query plus pull request page", len(requests))
	}
	if requests[1]["method"] != http.MethodGet || !strings.HasPrefix(requests[1]["path"].(string), "/repos/digitaldrywood/detent/pulls?") {
		t.Fatalf("pull request request = %#v, want REST pulls list", requests[1])
	}
}

func TestConnectorFetchIssuesByStatesFiltersMappedStates(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":1,"title":"Ready issue","body":"","state":"OPEN","url":"https://github.com/example/repo/issues/1","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Ready"},"priorityValue":null},{"id":"PVTI_2","content":{"__typename":"Issue","id":"I_kw2","number":2,"title":"Review issue","body":"","state":"OPEN","url":"https://github.com/example/repo/issues/2","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Reviewing"},"priorityValue":null}]}}}}`,
	}})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap: map[string]string{
			"Todo":         "Ready",
			"Human Review": "Reviewing",
		},
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"todo"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if ids := githubIssueIDs(got); !reflect.DeepEqual(ids, []string{"I_kw1"}) {
		t.Fatalf("FetchIssuesByStates() ids = %#v, want [I_kw1]", ids)
	}
	requests := server.requests()
	queryVariables := requests[0]["variables"].(map[string]any)
	if _, ok := queryVariables["query"]; ok {
		t.Fatalf("query = %v, want unfiltered ProjectV2 items", queryVariables["query"])
	}

	requestsBeforeEmpty := len(server.requests())
	got, err = c.FetchIssuesByStates(context.Background(), nil)
	if err != nil {
		t.Fatalf("FetchIssuesByStates(nil) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FetchIssuesByStates(nil) len = %d, want 0", len(got))
	}
	if len(server.requests()) != requestsBeforeEmpty {
		t.Fatalf("FetchIssuesByStates(nil) made a request")
	}
}

func TestConnectorFetchIssuesByStatesUsesStatusUpdatedAtForStage(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":1,"title":"Done issue","state":"OPEN","url":"https://github.com/example/repo/issues/1","repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Done","updatedAt":"2026-06-01T12:30:00Z"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Done"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	stageUpdatedAt := time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)
	if got[0].UpdatedAt != nil {
		t.Fatalf("UpdatedAt = %v, want nil from lightweight poll", got[0].UpdatedAt)
	}
	if got[0].StageUpdatedAt == nil || !got[0].StageUpdatedAt.Equal(stageUpdatedAt) {
		t.Fatalf("StageUpdatedAt = %v, want status updatedAt %v", got[0].StageUpdatedAt, stageUpdatedAt)
	}
}

func TestConnectorFetchIssuesByStatesExtractsWorkpadHumanActionNeeded(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw98","number":98,"title":"Homebrew tap","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/98","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Blocked"},"priorityValue":null}]}}}}`,
		},
		{
			method:  http.MethodGet,
			path:    "/repos/digitaldrywood/detent/issues/98/comments?per_page=100",
			headers: map[string]string{"Link": `</repos/digitaldrywood/detent/issues/98/comments?per_page=100&page=2>; rel="next"`},
			body:    `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/98/comments?per_page=100&page=2",
			body:   `[{"body":"## Codex Workpad\n\n### Plan\n- Check prerequisites.\n\n### Human Action Needed\n- Create public repository ` + "`" + `digitaldrywood/homebrew-tap` + "`" + `.\n- Add repository Actions secret ` + "`" + `HOMEBREW_TAP_GITHUB_TOKEN` + "`" + `.\n\n### Validation Evidence\n- Not run."}]`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/98/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
	})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Blocked"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	want := "Create public repository `digitaldrywood/homebrew-tap`.; Add repository Actions secret `HOMEBREW_TAP_GITHUB_TOKEN`."
	if got[0].BlockerReason != want {
		t.Fatalf("BlockerReason = %q, want %q", got[0].BlockerReason, want)
	}

	requests := server.requests()
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want 4", len(requests))
	}
	if strings.Contains(requests[0]["query"].(string), "comments") {
		t.Fatalf("project query = %q, want no comments", requests[0]["query"])
	}
	if requests[1]["method"] != http.MethodGet || requests[1]["path"] != "/repos/digitaldrywood/detent/issues/98/comments?per_page=100" {
		t.Fatalf("comments request = %#v, want REST issue comments", requests[1])
	}
}

func TestParseBlockerReasonUsesStructuredWorkpadFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantReason  string
		wantSource  string
		wantInvalid string
	}{
		{
			name:       "valid structured block suppresses prose",
			body:       "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: blocked\nblockers:\n  - ref: \"#1462\"\n    reason: \"needs migration\"\nhuman_action: null\n```\n\n### Human Action Needed\n- Blocked by: #999",
			wantReason: "digitaldrywood/detent#1462: needs migration",
			wantSource: "structured",
		},
		{
			name:       "structured reason code authorizes recovery park",
			body:       "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: blocked\nreason_code: merge_conflict\nblockers: []\nhuman_action: null\n```",
			wantReason: "merge_conflict",
			wantSource: "structured",
		},
		{
			name:        "invalid structured block suppresses prose",
			body:        "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: blocked\nblockers: []\nhuman_action: null\n```\n\n### Human Action Needed\n- Blocked by: #999",
			wantSource:  "structured",
			wantInvalid: "status blocked requires",
		},
		{
			name:       "no structured block preserves prose",
			body:       "## Codex Workpad\n\n### Human Action Needed\n- Need owner approval.",
			wantReason: "Need owner approval.",
			wantSource: "prose_section",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := githubIssueNode{
				Number:     1069,
				Repository: repository{NameWithOwner: "digitaldrywood/detent"},
				Comments: nodeConnection[issueComment]{Nodes: []issueComment{{
					Body: tt.body,
					URL:  "https://github.test/comment/workpad",
				}}},
			}
			signal := parseWorkpadSignal(issue)
			if signal == nil {
				t.Fatal("parseWorkpadSignal() = nil, want signal")
				return
			}
			if signal.Source != tt.wantSource {
				t.Fatalf("Signal.Source = %q, want %q", signal.Source, tt.wantSource)
			}
			if signal.CommentURL != "https://github.test/comment/workpad" {
				t.Fatalf("Signal.CommentURL = %q, want comment URL", signal.CommentURL)
			}
			if tt.wantInvalid != "" {
				if signal.Invalid == nil || !strings.Contains(signal.Invalid.Message, tt.wantInvalid) {
					t.Fatalf("Signal.Invalid = %#v, want message containing %q", signal.Invalid, tt.wantInvalid)
				}
				if reason := parseBlockerReason(issue); reason != "" {
					t.Fatalf("parseBlockerReason() = %q, want empty for invalid structured block", reason)
				}
				return
			}
			if signal.Invalid != nil {
				t.Fatalf("Signal.Invalid = %#v, want nil", signal.Invalid)
			}
			if reason := parseBlockerReason(issue); reason != tt.wantReason {
				t.Fatalf("parseBlockerReason() = %q, want %q", reason, tt.wantReason)
			}
		})
	}
}

func TestConnectorFetchIssuesByStatesExtractsWorkpadBlockedByRefs(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw416","number":416,"title":"Blocked workpad dependency","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/416","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Blocked"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/416/comments?per_page=100",
			body:   `[{"body":"## Codex Workpad\n\n### Blockers\n- Blocked by: #415\n- Human action needed: merge #415, then move #416 back to Todo.\n\n### Validation\n- Pending."}]`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/416/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/415",
			body:   `{"node_id":"I_kw415","number":415,"title":"Closed dependency","body":"","state":"CLOSED","html_url":"https://github.com/digitaldrywood/detent/issues/415","labels":[]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Blocked"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	want := []connector.BlockedRef{
		{ID: "I_kw415", Identifier: "digitaldrywood/detent#415", State: "Done", Source: connector.BlockedRefSourceProse},
	}
	if !reflect.DeepEqual(got[0].BlockedBy, want) {
		t.Fatalf("BlockedBy = %#v, want %#v", got[0].BlockedBy, want)
	}
}

func TestParseBlockedByFromIssueTextIgnoresRemovedWorkpadBlockedByProse(t *testing.T) {
	t.Parallel()

	issue := githubIssueNode{
		Number:     1476,
		Repository: repository{NameWithOwner: "digitaldrywood/pyroapex"},
		Comments: nodeConnection[issueComment]{Nodes: []issueComment{{
			Body: "## Codex Workpad\n\n### Blockers\n- Dependency blocker #1462 merged via PR #1482 and issue #1462 is closed/Done; #1463 was already closed. Removed the stale `Blocked by: #1462` line from #1476.\n\n### Validation\n- make check passed.",
		}}},
	}

	if got := parseBlockedByFromIssueText(issue, "digitaldrywood/pyroapex"); len(got) != 0 {
		t.Fatalf("parseBlockedByFromIssueText() = %#v, want no active dependency refs", got)
	}
}

func TestConnectorFetchIssuesByStatesResolvesBodyDependencyMissingFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		restState string
		wantState string
	}{
		{name: "closed dependency clears", restState: "CLOSED", wantState: "Done"},
		{name: "open dependency stays active", restState: "OPEN", wantState: "Open"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{
				{
					body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_163","content":{"__typename":"Issue","id":"I_163","number":163,"title":"Running with body dependency","body":"Depends on: #162","state":"OPEN","url":"https://github.com/digitaldrywood/creswoodcorners-phone/issues/163","repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"}},"statusValue":{"name":"In Progress"},"priorityValue":null}]}}}}`,
				},
				{
					method: http.MethodGet,
					path:   "/repos/digitaldrywood/creswoodcorners-phone/issues/162",
					body:   `{"node_id":"I_162","number":162,"title":"Dependency","body":"","state":"` + tt.restState + `","html_url":"https://github.com/digitaldrywood/creswoodcorners-phone/issues/162","labels":[]}`,
				},
				{
					body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
				},
			})
			c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

			got, err := c.FetchIssuesByStates(context.Background(), []string{"In Progress"})
			if err != nil {
				t.Fatalf("FetchIssuesByStates() error = %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
			}
			if len(got[0].BlockedBy) != 1 {
				t.Fatalf("BlockedBy len = %d, want 1; got %#v", len(got[0].BlockedBy), got[0].BlockedBy)
			}

			want := connector.BlockedRef{
				ID:         "I_162",
				Identifier: "digitaldrywood/creswoodcorners-phone#162",
				State:      tt.wantState,
				Source:     connector.BlockedRefSourceProse,
			}
			if got[0].BlockedBy[0] != want {
				t.Fatalf("BlockedBy[0] = %#v, want %#v", got[0].BlockedBy[0], want)
			}
		})
	}
}

func TestConnectorFetchIssuesByStatesKeepsBodyDependencyWhenHydrationFails(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_163","content":{"__typename":"Issue","id":"I_163","number":163,"title":"Running with body dependency","body":"Depends on: #162","state":"OPEN","url":"https://github.com/digitaldrywood/creswoodcorners-phone/issues/163","repository":{"nameWithOwner":"digitaldrywood/creswoodcorners-phone"}},"statusValue":{"name":"In Progress"},"priorityValue":null}]}}}}`,
		},
		{
			status: http.StatusInternalServerError,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/creswoodcorners-phone/issues/162",
			body:   `{"message":"temporary github failure"}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"In Progress"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}

	want := []connector.BlockedRef{{Identifier: "digitaldrywood/creswoodcorners-phone#162", Source: connector.BlockedRefSourceProse}}
	if !reflect.DeepEqual(got[0].BlockedBy, want) {
		t.Fatalf("BlockedBy = %#v, want %#v", got[0].BlockedBy, want)
	}
}

func TestConnectorFetchIssuesByStatesIgnoresHumanActionIssueMentions(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw417","number":417,"title":"Human blocked reference","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/417","repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":"Blocked"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/417/comments?per_page=100",
			body:   `[{"body":"## Codex Workpad\n\n### Human Action Needed\n- Need product approval based on #123 before continuing.\n\n### Validation\n- Pending."}]`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/417/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Blocked"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	if len(got[0].BlockedBy) != 0 {
		t.Fatalf("BlockedBy = %#v, want no dependency refs from Human Action Needed prose", got[0].BlockedBy)
	}
}

func TestConnectorFetchIssuesByStatesAttachesBlockedPullRequest(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_396","content":{"__typename":"Issue","id":"I_396","number":396,"title":"Blocked PR maintenance","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/396","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"bug"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[{"number":426,"url":"https://github.com/digitaldrywood/detent/pull/426","state":"OPEN","repository":{"nameWithOwner":"digitaldrywood/detent"}}]}},"statusValue":{"name":"Blocked"},"priorityValue":null}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/396/comments?per_page=100",
			body:   `[{"body":"## Codex Workpad\n\n### Human Action Needed\n- PR #426 latest head has no check-runs and conflicts with main."}]`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/396/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/426",
			body:   `{"number":426,"html_url":"https://github.com/digitaldrywood/detent/pull/426","state":"open","mergeable_state":"dirty","head":{"ref":"detent/digitaldrywood_detent_396","sha":"head-current"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/check-runs?per_page=100",
			body:   `{"check_runs":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/commits/head-current/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls/426/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("digitaldrywood/detent", 426),
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssuesByStates(context.Background(), []string{"Blocked"})
	if err != nil {
		t.Fatalf("FetchIssuesByStates() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssuesByStates() len = %d, want 1", len(got))
	}
	pr := got[0].PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want linked blocked PR")
		return
	}
	if pr.Number != 426 || pr.State != "OPEN" || pr.HeadSHA != "head-current" || pr.MergeableState != "dirty" || pr.CIStatus != "" || pr.CheckRunCount != 0 {
		t.Fatalf("PullRequest = %#v, want dirty PR with no current-head checks", pr)
	}
}

func TestConnectorFetchIssueStatesByIDsUsesProjectStatusAndRequestOrder(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponseWithTotal(2, false, "", []string{
				`{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":1,"title":"First","state":"OPEN","url":"https://github.com/example/repo/issues/1","repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Ready"},"priorityValue":{"name":"P1"},"fieldValues":{"nodes":[]}}`,
				`{"id":"PVTI_2","content":{"__typename":"Issue","id":"I_kw2","number":2,"title":"Second","state":"OPEN","url":"https://github.com/example/repo/issues/2","repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Reviewing"},"priorityValue":{"name":"No priority"},"fieldValues":{"nodes":[]}}`,
			}),
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/2",
			body:   `{"node_id":"I_kw2","number":2,"title":"Second","body":"","state":"open","html_url":"https://github.com/example/repo/issues/2","assignees":[],"labels":[]}`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/2/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1",
			body:   `{"node_id":"I_kw1","number":1,"title":"First","body":"","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[],"labels":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/2",
			body:   `{"node_id":"I_kw2","number":2,"title":"Second","body":"","state":"open","html_url":"https://github.com/example/repo/issues/2","assignees":[],"labels":[]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1",
			body:   `{"node_id":"I_kw1","number":1,"title":"First","body":"","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[],"labels":[]}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap: map[string]string{
			"Todo":         "Ready",
			"Human Review": "Reviewing",
		},
		PriorityMap: map[string]*int{"P1": new(2), "No priority": nil},
	})

	got, err := c.FetchIssueStatesByIDs(context.Background(), []string{"I_kw2", "I_kw1"})
	if err != nil {
		t.Fatalf("FetchIssueStatesByIDs() error = %v", err)
	}
	if ids := githubIssueIDs(got); !reflect.DeepEqual(ids, []string{"I_kw2", "I_kw1"}) {
		t.Fatalf("FetchIssueStatesByIDs() ids = %#v, want [I_kw2 I_kw1]", ids)
	}
	if got[0].State != "Human Review" {
		t.Fatalf("first State = %q, want Human Review", got[0].State)
	}
	if got[1].Priority == nil || *got[1].Priority != 2 {
		t.Fatalf("second Priority = %v, want 2", got[1].Priority)
	}
	if got[1].PriorityName != "P1" {
		t.Fatalf("second PriorityName = %q, want P1", got[1].PriorityName)
	}
	warm, err := c.FetchIssueStatesByIDs(context.Background(), []string{"I_kw2", "I_kw1"})
	if err != nil {
		t.Fatalf("warm FetchIssueStatesByIDs() error = %v", err)
	}
	if len(warm) != 2 || warm[0].State != "Human Review" || warm[1].State != "Todo" {
		t.Fatalf("warm FetchIssueStatesByIDs() = %#v, want cached project states", warm)
	}
	requests := server.requests()
	if len(requests) != 6 {
		t.Fatalf("request count = %d, want one project scan, four REST issues, and one dependency capability probe", len(requests))
	}
	var projectQueries int
	for _, request := range requests {
		query, _ := request["query"].(string)
		if strings.Contains(query, "DetentGitHubProjectItemForIssue") {
			t.Fatalf("query = %q, want no per-issue project-item lookup", query)
		}
		if strings.Contains(query, "query DetentGitHubProjectItems") {
			projectQueries++
		}
	}
	if projectQueries != 1 {
		t.Fatalf("project query count = %d, want one query across cold and warm passes", projectQueries)
	}
}

func TestConnectorFetchIssueStatesByIDsCapturesIssueMetadata(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponseWithTotal(1, false, "", []string{
				`{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":1,"title":"First","state":"CLOSED","stateReason":"not_planned","url":"https://github.com/example/repo/issues/1","author":{"login":"author-1"},"assignees":{"nodes":[{"login":"worker-1"},{"login":"worker-2"}]},"repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Ready"},"priorityValue":{"name":"P1"},"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"Ready","field":{"name":"Status"}},{"__typename":"ProjectV2ItemFieldTextValue","text":"team-a","field":{"name":"Owner"}},{"__typename":"ProjectV2ItemFieldNumberValue","number":3,"field":{"name":"Weight"}}]}}`,
			}),
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1",
			body:   `{"node_id":"I_kw1","number":1,"title":"First","body":"","state":"closed","state_reason":"not_planned","html_url":"https://github.com/example/repo/issues/1","user":{"login":"author-1"},"assignees":[{"node_id":"U_1","login":"worker-1"},{"node_id":"U_2","login":"worker-2"}],"labels":[]}`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap:    map[string]string{"Todo": "Ready"},
	})

	got, err := c.FetchIssueStatesByIDs(context.Background(), []string{"I_kw1"})
	if err != nil {
		t.Fatalf("FetchIssueStatesByIDs() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueStatesByIDs() len = %d, want 1", len(got))
	}

	issue := got[0]
	if issue.AuthorID != "author-1" {
		t.Fatalf("AuthorID = %q, want author-1", issue.AuthorID)
	}
	if !issue.Closed || issue.ClosedReason != "not_planned" {
		t.Fatalf("closed metadata = (%v, %q), want closed not_planned", issue.Closed, issue.ClosedReason)
	}
	if !reflect.DeepEqual(issue.Assignees, []string{"worker-1", "worker-2"}) {
		t.Fatalf("Assignees = %#v, want worker-1 and worker-2", issue.Assignees)
	}
	wantFields := map[string]string{"Owner": "team-a", "Status": "Ready", "Weight": "3"}
	if !reflect.DeepEqual(issue.Fields, wantFields) {
		t.Fatalf("Fields = %#v, want %#v", issue.Fields, wantFields)
	}
	if issue.AssigneeID != "worker-1" {
		t.Fatalf("AssigneeID = %q, want worker-1", issue.AssigneeID)
	}
}

func TestConnectorRefreshPullRequestReferenceCapturesClosingReference(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"nodes":[{"__typename":"Issue","id":"I_kw1","closedByPullRequestsReferences":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"number":42,"url":"https://github.com/example/repo/pull/42","state":"OPEN","updatedAt":"2026-08-28T00:38:50Z","repository":{"nameWithOwner":"example/repo"}}]}}]}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.RefreshPullRequestReference(t.Context(), connector.Issue{ID: "I_kw1", Identifier: "example/repo#1"})
	if err != nil {
		t.Fatalf("RefreshPullRequestReference() error = %v", err)
	}
	if got.PRNumber == nil || *got.PRNumber != 42 || got.PRRepository != "example/repo" {
		t.Fatalf("pull request reference = (%v, %q), want (42, example/repo)", got.PRNumber, got.PRRepository)
	}
}

func TestConnectorFetchIssueStatesByIDsPaginatesProjectItems(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: projectItemsPageResponseWithTotal(2, true, "cursor-1", []string{
				`{"id":"PVTI_other","content":{"__typename":"Issue","id":"I_other","number":2,"title":"Other","state":"OPEN","url":"https://github.com/example/repo/issues/2","repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Open"},"priorityValue":{"name":"P1"},"fieldValues":{"nodes":[]}}`,
			}),
		},
		{
			body: projectItemsPageResponseWithTotal(2, false, "", []string{
				`{"id":"PVTI_1","content":{"__typename":"Issue","id":"I_kw1","number":1,"title":"Later project","state":"OPEN","url":"https://github.com/example/repo/issues/1","repository":{"nameWithOwner":"example/repo"}},"statusValue":{"name":"Reviewing"},"priorityValue":{"name":"P2"},"fieldValues":{"nodes":[]}}`,
			}),
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1",
			body:   `{"node_id":"I_kw1","number":1,"title":"Later project","body":"","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[],"labels":[]}`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/1/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap: map[string]string{
			"Human Review": "Reviewing",
		},
		PriorityMap: map[string]*int{"P2": new(3)},
	})

	got, err := c.FetchIssueStatesByIDs(context.Background(), []string{"I_kw1"})
	if err != nil {
		t.Fatalf("FetchIssueStatesByIDs() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueStatesByIDs() len = %d, want 1", len(got))
	}
	if got[0].State != "Human Review" {
		t.Fatalf("State = %q, want Human Review", got[0].State)
	}
	if got[0].Priority == nil || *got[0].Priority != 3 {
		t.Fatalf("Priority = %v, want 3", got[0].Priority)
	}

	requests := server.requests()
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want 2 project scan pages, REST issue, and dependency probe", len(requests))
	}
	variables := requests[1]["variables"].(map[string]any)
	if variables["after"] != "cursor-1" {
		t.Fatalf("after = %v, want cursor-1", variables["after"])
	}
	if variables["projectId"] != "PVT_1" {
		t.Fatalf("projectId = %v, want PVT_1", variables["projectId"])
	}
}

func TestConnectorFetchIssueStatesByIdentifiersResolvesDependencyReadinessSignals(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/251",
			body:   `{"node_id":"I_closed","number":251,"title":"Closed child","body":"","state":"closed","html_url":"https://github.com/digitaldrywood/detent/issues/251","assignees":[],"labels":[]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[]}}}}`,
		},
		{
			status: http.StatusNotFound,
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/251/dependencies/blocked_by?per_page=100",
			body:   `{"message":"not found"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/252",
			body:   `{"node_id":"I_done","number":252,"title":"Done child","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/252","assignees":[],"labels":[]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_done","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/253",
			body:   `{"node_id":"I_merged_pr","number":253,"title":"Merged PR child","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/253","assignees":[],"labels":[]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_merged_pr","project":{"id":"PVT_1"},"statusValue":{"name":"In Progress"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all",
			body:   `[{"number":254,"html_url":"https://github.com/digitaldrywood/detent/pull/254","state":"closed","merged_at":"2026-06-12T16:00:00Z","head":{"ref":"detent/digitaldrywood_detent_253-autounblock","sha":"abc123"}}]`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueStatesByIdentifiers(context.Background(), []string{"digitaldrywood/detent#251", "digitaldrywood/detent#252", "digitaldrywood/detent#253"})
	if err != nil {
		t.Fatalf("FetchIssueStatesByIdentifiers() error = %v", err)
	}
	if ids := githubIssueIDs(got); !reflect.DeepEqual(ids, []string{"I_closed", "I_done", "I_merged_pr"}) {
		t.Fatalf("FetchIssueStatesByIdentifiers() ids = %#v, want [I_closed I_done I_merged_pr]", ids)
	}
	if !got[0].Closed || got[0].State != "Done" {
		t.Fatalf("closed child = %#v, want Closed true and State Done", got[0])
	}
	if got[1].Closed || got[1].State != "Done" {
		t.Fatalf("project done child = %#v, want open issue with State Done", got[1])
	}
	if got[2].Closed || got[2].State != "In Progress" {
		t.Fatalf("merged PR child = %#v, want open issue still In Progress", got[2])
	}
	if got[2].PullRequest == nil || got[2].PullRequest.State != "MERGED" || got[2].PullRequest.Number != 254 {
		t.Fatalf("merged PR child PullRequest = %#v, want merged PR 254", got[2].PullRequest)
	}

	requests := server.requests()
	if len(requests) != 8 {
		t.Fatalf("request count = %d, want REST issue and project field reads, dependency probe, and PR list", len(requests))
	}
	if requests[0]["method"] != http.MethodGet || requests[0]["path"] != "/repos/digitaldrywood/detent/issues/251" {
		t.Fatalf("first request = %#v, want REST issue lookup", requests[0])
	}
	if requests[7]["method"] != http.MethodGet || requests[7]["path"] != "/repos/digitaldrywood/detent/pulls?direction=desc&page=1&per_page=100&sort=updated&state=all" {
		t.Fatalf("PR request = %#v, want REST pull request list", requests[7])
	}
}

func TestConnectorFetchIssueChildrenPaginatesLinkedIssues(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"subIssues":{"pageInfo":{"hasNextPage":true,"endCursor":"sub-cursor-1"},"nodes":[{"id":"I_sub_1","number":251,"title":"Sub child","state":"CLOSED","url":"https://github.com/digitaldrywood/detent/issues/251","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[]}}]}}}}`,
		},
		{
			body: `{"data":{"node":{"subIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"I_sub_2","number":252,"title":"Sub child 2","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/252","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_sub_2","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]}}}}`,
		},
		{
			body: `{"data":{"node":{"trackedIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"I_tracked","number":253,"title":"Tracked child","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/253","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_tracked","project":{"id":"PVT_1"},"statusValue":{"name":"In Progress"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueChildren(context.Background(), "I_epic")
	if err != nil {
		t.Fatalf("FetchIssueChildren() error = %v", err)
	}
	want := []connector.BlockedRef{
		{ID: "I_sub_1", Identifier: "digitaldrywood/detent#251", State: "Done"},
		{ID: "I_sub_2", Identifier: "digitaldrywood/detent#252", State: "Done"},
		{ID: "I_tracked", Identifier: "digitaldrywood/detent#253", State: "In Progress"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchIssueChildren() = %#v, want %#v", got, want)
	}

	requests := server.requests()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(requests))
	}
	firstVariables := requests[0]["variables"].(map[string]any)
	if firstVariables["linkedIssuesFirst"] != float64(linkedIssuePageSize) {
		t.Fatalf("linkedIssuesFirst = %v, want %d", firstVariables["linkedIssuesFirst"], linkedIssuePageSize)
	}
	if firstVariables["linkedProjectItemsFirst"] != float64(linkedIssueProjectItemsPageSize) {
		t.Fatalf("linkedProjectItemsFirst = %v, want %d", firstVariables["linkedProjectItemsFirst"], linkedIssueProjectItemsPageSize)
	}
	if firstVariables["linkedProjectItemFieldValuesFirst"] != float64(linkedIssueProjectItemFieldValuesPageSize) {
		t.Fatalf("linkedProjectItemFieldValuesFirst = %v, want %d", firstVariables["linkedProjectItemFieldValuesFirst"], linkedIssueProjectItemFieldValuesPageSize)
	}
	secondVariables := requests[1]["variables"].(map[string]any)
	if secondVariables["after"] != "sub-cursor-1" {
		t.Fatalf("second after = %v, want sub-cursor-1", secondVariables["after"])
	}
	if !strings.Contains(requests[2]["query"].(string), "trackedIssues") {
		t.Fatalf("third query = %q, want trackedIssues", requests[2]["query"])
	}
}

func TestConnectorFetchIssueParentsReturnsParentAndTrackedInIssues(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"parent":{"__typename":"Issue","id":"I_parent","number":258,"title":"Epic: Parent","body":"- [ ] #251","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/258","createdAt":null,"updatedAt":null,"author":{"login":"corylanou"},"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"epic"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]},"subIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"I_child","number":251,"title":"Child","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/251","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_child","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]},"trackedIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_parent","project":{"id":"PVT_1"},"statusValue":{"name":"Todo","updatedAt":"2026-06-02T16:00:00Z"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}},"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"__typename":"Issue","id":"I_tracked_parent","number":259,"title":"Epic: Tracked parent","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/259","createdAt":null,"updatedAt":null,"author":{"login":"corylanou"},"assignees":{"nodes":[]},"labels":{"nodes":[{"name":"epic"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]},"subIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]},"trackedIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"I_child","number":251,"title":"Child","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/251","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_child","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_tracked_parent","project":{"id":"PVT_1"},"statusValue":{"name":"In Progress","updatedAt":"2026-06-02T16:01:00Z"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]}}}}`,
	}})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FetchIssueParents() len = %d, want 2", len(got))
	}
	if got[0].ID != "I_parent" || got[0].Identifier != "digitaldrywood/detent#258" || got[0].State != "Todo" {
		t.Fatalf("first parent = %#v", got[0])
	}
	if got[1].ID != "I_tracked_parent" || got[1].Identifier != "digitaldrywood/detent#259" || got[1].State != "In Progress" {
		t.Fatalf("second parent = %#v", got[1])
	}
	if got[0].ChildIssues[0] != (connector.BlockedRef{ID: "I_child", Identifier: "digitaldrywood/detent#251", State: "Done"}) {
		t.Fatalf("first parent child issues = %#v", got[0].ChildIssues)
	}
	if got[1].ChildIssues[0] != (connector.BlockedRef{ID: "I_child", Identifier: "digitaldrywood/detent#251", State: "Done"}) {
		t.Fatalf("second parent child issues = %#v", got[1].ChildIssues)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	variables := requests[0]["variables"].(map[string]any)
	if variables["issueId"] != "I_child" {
		t.Fatalf("issueId = %v, want I_child", variables["issueId"])
	}
	query := requests[0]["query"].(string)
	for _, want := range []string{"parent", "trackedInIssues", "subIssues(first: $linkedIssuesFirst)", "trackedIssues(first: $linkedIssuesFirst)"} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q:\n%s", want, query)
		}
	}
	if variables["linkedIssuesFirst"] != float64(linkedIssuePageSize) {
		t.Fatalf("linkedIssuesFirst = %v, want %d", variables["linkedIssuesFirst"], linkedIssuePageSize)
	}
}

func TestConnectorFetchIssueParentsReturnsBodyReferencedEpic(t *testing.T) {
	t.Parallel()

	body := "- [ ] #251"
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"id":"I_child","number":251,"repository":{"nameWithOwner":"digitaldrywood/detent"},"parent":null,"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
		},
		{
			method: http.MethodGet,
			body:   `{"items":[{"number":258}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/258",
			body:   `{"node_id":"I_epic","number":258,"title":"Epic: Parent","body":"` + body + `","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/258","assignees":[],"labels":[{"name":"epic"}]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_parent","project":{"id":"PVT_1"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueParents() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_epic" || got[0].Identifier != "digitaldrywood/detent#258" || got[0].State != "Todo" {
		t.Fatalf("body parent = %#v", got[0])
	}
	if got[0].Description != body {
		t.Fatalf("body parent description = %q, want %q", got[0].Description, body)
	}

	requests := server.requests()
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want parent lookup, search, REST issue, project item", len(requests))
	}
	if requests[1]["method"] != http.MethodGet || !strings.HasPrefix(requests[1]["path"].(string), "/search/issues?") {
		t.Fatalf("search request = %#v, want REST issue search", requests[1])
	}
	if !strings.Contains(requests[1]["path"].(string), "251") {
		t.Fatalf("search path = %q, want child issue number", requests[1]["path"])
	}
}

func TestConnectorFetchIssueParentsReturnsCrossRepoBodyReferencedEpic(t *testing.T) {
	t.Parallel()

	body := "Depends on: digitaldrywood/agent-runtime#251"
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"id":"I_child","number":251,"repository":{"nameWithOwner":"digitaldrywood/agent-runtime"},"parent":null,"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
		},
		{
			method: http.MethodGet,
			body:   `{"total_count":1,"items":[{"number":258,"html_url":"https://github.com/digitaldrywood/detent/issues/258"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/258",
			body:   `{"node_id":"I_epic","number":258,"title":"Epic: Parent","body":"` + body + `","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/258","assignees":[],"labels":[{"name":"epic"}]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_parent","project":{"id":"PVT_1"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueParents() len = %d, want 1", len(got))
	}
	if got[0].ID != "I_epic" || got[0].Identifier != "digitaldrywood/detent#258" || got[0].Description != body {
		t.Fatalf("cross-repo body parent = %#v", got[0])
	}

	requests := server.requests()
	if len(requests) != 4 {
		t.Fatalf("request count = %d, want parent lookup, search, REST issue, project item", len(requests))
	}
	searchPath := requests[1]["path"].(string)
	if !strings.Contains(searchPath, "user%3Adigitaldrywood") || strings.Contains(searchPath, "repo%3A") {
		t.Fatalf("search path = %q, want owner-scoped search", searchPath)
	}
	if requests[2]["path"] != "/repos/digitaldrywood/detent/issues/258" {
		t.Fatalf("REST issue path = %#v, want cross-repo epic issue", requests[2])
	}
}

func TestConnectorFetchIssueParentsPaginatesBodyReferencedEpicSearch(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"id":"I_child","number":251,"repository":{"nameWithOwner":"digitaldrywood/detent"},"parent":null,"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
		},
		{
			method: http.MethodGet,
			body:   `{"total_count":101,"items":[{"number":251}]}`,
		},
		{
			method: http.MethodGet,
			body:   `{"total_count":101,"items":[{"number":258}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/digitaldrywood/detent/issues/258",
			body:   `{"node_id":"I_epic","number":258,"title":"Epic: Parent","body":"Depends on: #251","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/258","assignees":[],"labels":[]}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_parent","project":{"id":"PVT_1"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "I_epic" {
		t.Fatalf("FetchIssueParents() = %#v, want body referenced epic", got)
	}

	requests := server.requests()
	if len(requests) != 5 {
		t.Fatalf("request count = %d, want parent lookup, 2 search pages, REST issue, project item", len(requests))
	}
	firstSearch := requests[1]["path"].(string)
	secondSearch := requests[2]["path"].(string)
	if !strings.Contains(firstSearch, "page=1") || !strings.Contains(secondSearch, "page=2") {
		t.Fatalf("search paths = %q, %q; want page 1 then page 2", firstSearch, secondSearch)
	}
}

func TestConnectorFetchIssueParentsPaginatesLinkedChildProjectItems(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"node":{"parent":{"__typename":"Issue","id":"I_parent","number":258,"title":"Epic: Parent","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/258","repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]},"subIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"I_child","number":251,"title":"Child","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/251","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":true,"endCursor":"project-cursor-1"},"nodes":[{"id":"PVTI_other","project":{"id":"PVT_other"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}]},"trackedIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_parent","project":{"id":"PVT_1"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}},"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
		},
		{
			body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_child","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}}}}`,
		},
	})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueParents() len = %d, want 1", len(got))
	}
	want := connector.BlockedRef{ID: "I_child", Identifier: "digitaldrywood/detent#251", State: "Done"}
	if got[0].ChildIssues[0] != want {
		t.Fatalf("parent child issue = %#v, want %#v", got[0].ChildIssues[0], want)
	}
	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want parent lookup and linked child project pagination", len(requests))
	}
}

func TestConnectorFetchIssueParentsSkipsParentsOutsideProject(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		body: `{"data":{"node":{"parent":{"__typename":"Issue","id":"I_outside_parent","number":260,"title":"Outside epic","body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/260","repository":{"nameWithOwner":"digitaldrywood/detent"},"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_other","project":{"id":"PVT_other"},"statusValue":{"name":"Todo"},"priorityValue":null,"fieldValues":{"nodes":[]}}]}},"trackedInIssues":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}`,
	}})

	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	got, err := c.FetchIssueParents(context.Background(), "I_child")
	if err != nil {
		t.Fatalf("FetchIssueParents() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FetchIssueParents() = %#v, want no out-of-project parents", got)
	}
}

func TestConnectorFetchIssueCommentsMapsRESTMetadata(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodGet,
		path:   "/repos/example/repo/issues/1/comments?per_page=100",
		body:   `[{"id":101,"node_id":"IC_kw1","body":"First note","html_url":"https://github.com/example/repo/issues/1#issuecomment-101","user":{"login":"alice"},"author_association":"MEMBER","created_at":"2026-07-06T12:00:00Z","updated_at":"2026-07-06T12:05:00Z"}]`,
	}})
	c := newGitHubTestConnector(t, server, Config{})

	got, err := c.FetchIssueComments(context.Background(), connector.Issue{Identifier: "example/repo#1"})
	if err != nil {
		t.Fatalf("FetchIssueComments() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchIssueComments() len = %d, want 1", len(got))
	}

	comment := got[0]
	if comment.ID != "IC_kw1" ||
		comment.Backend != connector.BackendGitHub.String() ||
		comment.Body != "First note" ||
		comment.URL != "https://github.com/example/repo/issues/1#issuecomment-101" ||
		comment.AuthorLogin != "alice" ||
		!comment.AuthorAuthorized ||
		comment.Local ||
		comment.TargetType != connector.IssueCommentTargetIssue {
		t.Fatalf("FetchIssueComments()[0] = %#v, want normalized GitHub metadata", comment)
	}
	createdAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 7, 6, 12, 5, 0, 0, time.UTC)
	if comment.CreatedAt == nil || !comment.CreatedAt.Equal(createdAt) {
		t.Fatalf("CreatedAt = %v, want %v", comment.CreatedAt, createdAt)
	}
	if comment.UpdatedAt == nil || !comment.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("UpdatedAt = %v, want %v", comment.UpdatedAt, updatedAt)
	}
}

func TestConnectorAuthorizesIssueCommentByRepositoryPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		permission string
		want       bool
	}{
		{permission: "admin", want: true},
		{permission: "maintain", want: true},
		{permission: "write", want: true},
		{permission: "read", want: false},
		{permission: "triage", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.permission, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{{
				method: http.MethodGet,
				path:   "/repos/example/repo/collaborators/alice/permission",
				body:   `{"permission":"` + tt.permission + `"}`,
			}})
			c := newGitHubTestConnector(t, server, Config{})

			got, err := c.IsIssueCommentAuthorAuthorized(
				t.Context(),
				connector.Issue{Identifier: "example/repo#1"},
				connector.IssueComment{AuthorLogin: "alice"},
			)
			if err != nil || got != tt.want {
				t.Fatalf("IsIssueCommentAuthorAuthorized() = %t, %v, want %t", got, err, tt.want)
			}
		})
	}
}

func TestConnectorFetchPullRequestCommentsMapsRESTMetadata(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodGet,
		path:   "/repos/example/repo/issues/42/comments?per_page=100",
		body:   `[{"id":202,"node_id":"IC_pr","body":"Review note","html_url":"https://github.com/example/repo/pull/42#issuecomment-202","user":{"login":"reviewer"},"created_at":"2026-07-06T13:00:00Z","updated_at":"2026-07-06T13:10:00Z"}]`,
	}})
	c := newGitHubTestConnector(t, server, Config{})

	got, err := c.FetchPullRequestComments(context.Background(), "example/repo", 42)
	if err != nil {
		t.Fatalf("FetchPullRequestComments() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FetchPullRequestComments() len = %d, want 1", len(got))
	}

	comment := got[0]
	if comment.ID != "IC_pr" ||
		comment.Backend != connector.BackendGitHub.String() ||
		comment.Body != "Review note" ||
		comment.URL != "https://github.com/example/repo/pull/42#issuecomment-202" ||
		comment.AuthorLogin != "reviewer" ||
		comment.Local ||
		comment.TargetType != connector.IssueCommentTargetPullRequest {
		t.Fatalf("FetchPullRequestComments()[0] = %#v, want normalized GitHub PR metadata", comment)
	}
	createdAt := time.Date(2026, 7, 6, 13, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 7, 6, 13, 10, 0, 0, time.UTC)
	if comment.CreatedAt == nil || !comment.CreatedAt.Equal(createdAt) {
		t.Fatalf("CreatedAt = %v, want %v", comment.CreatedAt, createdAt)
	}
	if comment.UpdatedAt == nil || !comment.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("UpdatedAt = %v, want %v", comment.UpdatedAt, updatedAt)
	}
}

func TestConnectorCreateCommentCallsAddComment(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodPost,
		path:   "/repos/example/repo/issues/1/comments",
		body:   `{"node_id":"IC_kw1"}`,
	}})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.CreateComment(context.Background(), "I_kw1", "hello"); err != nil {
		t.Fatalf("CreateComment() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	if requests[0]["method"] != http.MethodPost || requests[0]["path"] != "/repos/example/repo/issues/1/comments" {
		t.Fatalf("comment request = %#v, want REST issue comment", requests[0])
	}
	body := requests[0]["body"].(map[string]any)
	if body["body"] != "hello" {
		t.Fatalf("body = %v, want hello", body["body"])
	}
}

func TestConnectorCreateIssueUsesREST(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodPost,
		path:   "/repos/example/repo/issues",
		body:   `{"node_id":"I_kw2","number":2,"title":"Governed proposal","body":"proposal body","state":"open","html_url":"https://github.com/example/repo/issues/2","labels":[{"name":"enhancement"}]}`,
	}})
	c := newGitHubTestConnector(t, server, Config{Repository: "example/repo"})

	issue, err := c.CreateIssue(context.Background(), connector.IssueDraft{
		Title:  " Governed proposal ",
		Body:   " proposal body ",
		Labels: []string{"enhancement", "Enhancement", ""},
	})
	if err != nil {
		t.Fatalf("CreateIssue() error = %v", err)
	}
	if issue.ID != "I_kw2" || issue.Identifier != "example/repo#2" || issue.URL != "https://github.com/example/repo/issues/2" {
		t.Fatalf("CreateIssue() issue = %#v", issue)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	body := requests[0]["body"].(map[string]any)
	if body["title"] != "Governed proposal" || body["body"] != "proposal body" {
		t.Fatalf("request body = %#v", body)
	}
	labels, ok := body["labels"].([]any)
	if !ok || len(labels) != 1 || labels[0] != "enhancement" {
		t.Fatalf("labels = %#v, want trimmed non-empty labels", body["labels"])
	}
}

func TestConnectorCreatePullRequestCommentUsesIssueCommentsEndpoint(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodPost,
		path:   "/repos/example/repo/issues/42/comments",
		body:   `{"node_id":"IC_pr"}`,
	}})
	c := newGitHubTestConnector(t, server, Config{})

	if err := c.CreatePullRequestComment(context.Background(), "example/repo", 42, "ship it"); err != nil {
		t.Fatalf("CreatePullRequestComment() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	body := requests[0]["body"].(map[string]any)
	if body["body"] != "ship it" {
		t.Fatalf("body = %v, want ship it", body["body"])
	}
}

func TestConnectorMergePullRequestClassifiesBaseRefusal(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		status  int
		message string
		want    bool
	}{
		{"strict protection", 405, "Head branch is out of date. Review and try the merge again.", true},
		{"base race", 405, "Base branch was modified. Review and try the merge again.", true},
		{"required failure", 405, "Required status check Test is failing.", false},
		{"native queue", 405, "Pull request must be merged using the merge queue.", false},
		{"conflict", 405, "Pull Request is not mergeable", false},
		{"changed head", 409, "Head branch is out of date.", false},
		{"permission", 403, "Head branch is out of date.", false},
		{"transient", 502, "Head branch is out of date.", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body, err := json.Marshal(map[string]string{"message": tt.message})
			if err != nil {
				t.Fatal(err)
			}
			server := newGraphQLTestServer(t, []graphqlTestResponse{{method: http.MethodPut, path: "/repos/example/repo/pulls/42/merge", status: tt.status, body: string(body)}})
			c := newGitHubTestConnector(t, server, Config{})
			err = c.MergePullRequest(t.Context(), "example/repo", 42, "checked-head", "squash")
			if errors.Is(err, connector.ErrPullRequestBaseOutOfDate) != tt.want {
				t.Fatalf("error = %v, want base refusal %t", err, tt.want)
			}
			var status *StatusError
			if !errors.As(err, &status) || status.StatusCode != tt.status {
				t.Fatalf("lost original status: %v", err)
			}
		})
	}
}

func TestConnectorMergePullRequestUsesConfiguredMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		want         string
		wantErr      string
		wantRequests int
	}{
		{name: "default", want: "squash", wantRequests: 1},
		{name: "squash", method: "squash", want: "squash", wantRequests: 1},
		{name: "merge", method: "merge", want: "merge", wantRequests: 1},
		{name: "rebase", method: "rebase", want: "rebase", wantRequests: 1},
		{name: "normalizes case and whitespace", method: " ReBaSe ", want: "rebase", wantRequests: 1},
		{name: "rejects invalid method", method: "octopus", wantErr: "merge method must be one of squash, merge, rebase"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{{
				method: http.MethodPut,
				path:   "/repos/example/repo/pulls/42/merge",
				body:   `{"sha":"merge-sha","merged":true,"message":"Pull Request successfully merged"}`,
			}})
			c := newGitHubTestConnector(t, server, Config{})

			err := c.MergePullRequest(context.Background(), "example/repo", 42, "head-sha", tt.method)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("MergePullRequest() error = %v, want %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("MergePullRequest() error = %v", err)
			}

			requests := server.requests()
			if got := len(requests); got != tt.wantRequests {
				t.Fatalf("request count = %d, want %d", got, tt.wantRequests)
			}
			if tt.wantRequests == 0 {
				return
			}
			if requests[0]["method"] != http.MethodPut || requests[0]["path"] != "/repos/example/repo/pulls/42/merge" {
				t.Fatalf("merge request = %#v, want REST pull request merge", requests[0])
			}
			body := requests[0]["body"].(map[string]any)
			if body["merge_method"] != tt.want || body["sha"] != "head-sha" {
				t.Fatalf("merge body = %#v, want %s merge with head sha", body, tt.want)
			}
		})
	}
}

func TestConnectorReapplyPullRequestLabelRemovesAndAddsExistingLabel(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/issues/42",
			body:   `{"node_id":"PR_42","number":42,"labels":[{"name":"bug"},{"name":"ci:ready"}]}`,
		},
		{
			method: http.MethodDelete,
			path:   "/repos/example/repo/issues/42/labels/ci:ready",
			body:   `[{"name":"bug"}]`,
		},
		{
			method: http.MethodPost,
			path:   "/repos/example/repo/issues/42/labels",
			body:   `[{"name":"bug"},{"name":"ci:ready"}]`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})
	c.triggerLabelDir = t.TempDir()

	if err := c.ReapplyPullRequestLabel(context.Background(), "example/repo", 42, "ci:ready", 15*time.Second); err != nil {
		t.Fatalf("ReapplyPullRequestLabel() error = %v", err)
	}

	requests := server.requests()
	if got := len(requests); got != 3 {
		t.Fatalf("request count = %d, want 3", got)
	}
	if requests[1]["method"] != http.MethodDelete || requests[2]["method"] != http.MethodPost {
		t.Fatalf("requests = %#v, want GET, DELETE, POST", requests)
	}
	body := requests[2]["body"].(map[string]any)
	if got := body["labels"]; !reflect.DeepEqual(got, []any{"ci:ready"}) {
		t.Fatalf("labels body = %#v, want ci:ready", got)
	}
}

func TestConnectorHydratePullRequestRefreshesCurrentStatus(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"node_id":"PR_42","number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","draft":false,"labels":[{"name":"Ready to Merge"},{"name":"bug"}],"head":{"ref":"detent/example_repo_1","sha":"head-sha"},"base":{"ref":"main","sha":"base-sha"},"updated_at":"2026-06-26T13:00:00Z"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"name":"Verify","status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	if got.PullRequest == nil {
		t.Fatalf("HydratePullRequest().PullRequest = nil, want hydrated pull request")
	}
	pr := got.PullRequest
	if pr.NodeID != "PR_42" || pr.Number != 42 || pr.State != "OPEN" || pr.MergeableState != "clean" || pr.HeadSHA != "head-sha" || pr.BaseRef != "main" || pr.BaseSHA != "base-sha" {
		t.Fatalf("hydrated pull request = %#v, want current clean pull request details", pr)
	}
	if pr.CIStatus != "pass" || pr.CheckRunCount != 1 {
		t.Fatalf("hydrated CI = status %q check runs %d, want pass with one check run", pr.CIStatus, pr.CheckRunCount)
	}
	if !reflect.DeepEqual(pr.Labels, []string{"ready to merge", "bug"}) {
		t.Fatalf("hydrated labels = %#v, want ready to merge and bug", pr.Labels)
	}
	if got.PRRepository != "example/repo" {
		t.Fatalf("PRRepository = %q, want example/repo", got.PRRepository)
	}
}

func TestConnectorFetchPullRequestReviewThreads(t *testing.T) {
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodPost,
			path:   "/",
			body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"head-sha","reviewThreads":{"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"},"nodes":[{"isResolved":false,"path":"internal/orchestrator/autopromote.go","line":181,"originalLine":180,"comments":{"nodes":[{"body":"Preserve cancellation evidence."}]}},{"isResolved":true,"path":"internal/connector/issue.go","line":107,"originalLine":107}]}}}}}`,
		},
		{
			method: http.MethodPost,
			path:   "/",
			body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"head-sha","reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"isResolved":false,"isOutdated":true,"path":"internal/connector/github/pull_requests.go","line":null,"originalLine":1092}]}}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})

	got, err := c.fetchPullRequestReviewThreads(t.Context(), pullRequestRepo{Owner: "example", Name: "repo"}, 42, "head-sha")
	if err != nil {
		t.Fatalf("fetchPullRequestReviewThreads() error = %v", err)
	}
	want := []connector.PullRequestReviewThread{
		{Path: "internal/orchestrator/autopromote.go", Line: 181, Body: "Preserve cancellation evidence."},
		{Path: "internal/connector/github/pull_requests.go", Line: 1092},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fetchPullRequestReviewThreads() = %#v, want %#v", got, want)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if variables := requests[1]["variables"].(map[string]any); variables["after"] != "cursor-1" {
		t.Fatalf("second request after = %v, want cursor-1", variables["after"])
	}
}

func TestConnectorHydratePullRequestReviewThreadsRefreshesMutableState(t *testing.T) {
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodPost,
			path:   "/",
			body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"head-one","reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}}`,
		},
		{
			method: http.MethodPost,
			path:   "/",
			body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"head-one","reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"isResolved":false,"path":"internal/orchestrator/autopromote.go","line":181,"originalLine":181}]}}}}}`,
		},
		{
			method: http.MethodPost,
			path:   "/",
			body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"head-two","reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})
	repo := pullRequestRepo{Owner: "example", Name: "repo"}
	c.pullRequests.Set(repo, 42, "head-one", pullRequestStatus{ci: pullRequestCI{State: "SUCCESS"}})
	c.pullRequests.Set(repo, 42, "head-two", pullRequestStatus{ci: pullRequestCI{State: "SUCCESS"}})
	issue := connector.Issue{
		Identifier:   "example/repo#41",
		PRRepository: "example/repo",
		PullRequest: &connector.PullRequest{
			Number:  42,
			State:   "OPEN",
			HeadSHA: "head-one",
		},
	}

	first, err := c.HydratePullRequestReviewThreads(t.Context(), issue)
	if err != nil {
		t.Fatalf("first HydratePullRequestReviewThreads() error = %v", err)
	}
	second, err := c.HydratePullRequestReviewThreads(t.Context(), issue)
	if err != nil {
		t.Fatalf("second HydratePullRequestReviewThreads() error = %v", err)
	}
	if len(first.PullRequest.UnresolvedReviewThreads) != 0 {
		t.Fatalf("first unresolved review threads = %#v, want none", first.PullRequest.UnresolvedReviewThreads)
	}
	want := []connector.PullRequestReviewThread{{Path: "internal/orchestrator/autopromote.go", Line: 181}}
	if !reflect.DeepEqual(second.PullRequest.UnresolvedReviewThreads, want) {
		t.Fatalf("second unresolved review threads = %#v, want %#v", second.PullRequest.UnresolvedReviewThreads, want)
	}

	changedHead := issue
	pullRequest := *issue.PullRequest
	pullRequest.HeadSHA = "head-two"
	changedHead.PullRequest = &pullRequest
	resolved, err := c.HydratePullRequestReviewThreads(t.Context(), changedHead)
	if err != nil {
		t.Fatalf("changed-head HydratePullRequestReviewThreads() error = %v", err)
	}
	if len(resolved.PullRequest.UnresolvedReviewThreads) != 0 {
		t.Fatalf("changed-head unresolved review threads = %#v, want none", resolved.PullRequest.UnresolvedReviewThreads)
	}
	if got := len(server.requests()); got != 3 {
		t.Fatalf("request count = %d, want one fresh query per hydration", got)
	}
	for _, headSHA := range []string{"head-one", "head-two"} {
		status, ok := c.pullRequests.Get(repo, 42, headSHA)
		if !ok || status.ci.State != "SUCCESS" {
			t.Fatalf("cached status for %s = %#v, %t", headSHA, status, ok)
		}
	}
}

func TestConnectorFetchPullRequestReviewThreadsRejectsChangedHead(t *testing.T) {
	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodPost,
		path:   "/",
		body:   `{"data":{"repository":{"pullRequest":{"headRefOid":"new-head","reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}}}`,
	}})
	c := newGitHubTestConnector(t, server, Config{})

	_, err := c.fetchPullRequestReviewThreads(t.Context(), pullRequestRepo{Owner: "example", Name: "repo"}, 42, "old-head")
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("fetchPullRequestReviewThreads() error = %v, want ErrInvalidResponse", err)
	}
}

func TestConnectorLookupPullRequestByHeadUsesExactCurrentHead(t *testing.T) {
	t.Parallel()

	const (
		branch  = "detent/acme_widgets_18"
		headSHA = "current-head"
		path    = "/repos/example/repo/pulls?direction=desc&head=example%3Adetent%2Facme_widgets_18&per_page=100&sort=updated&state=all"
	)
	tests := []struct {
		name      string
		body      string
		wantFound bool
		wantState string
	}{
		{
			name:      "open exact head",
			body:      `[{"node_id":"PR_42","number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","head":{"ref":"detent/acme_widgets_18","sha":"current-head"},"base":{"ref":"main","sha":"base-head"}}]`,
			wantFound: true,
			wantState: "OPEN",
		},
		{
			name:      "merged exact head",
			body:      `[{"node_id":"PR_42","number":42,"html_url":"https://github.com/example/repo/pull/42","state":"closed","merged_at":"2026-08-12T18:00:00Z","head":{"ref":"detent/acme_widgets_18","sha":"current-head"},"base":{"ref":"main","sha":"base-head"}}]`,
			wantFound: true,
			wantState: "MERGED",
		},
		{
			name:      "closed unmerged exact head",
			body:      `[{"node_id":"PR_42","number":42,"html_url":"https://github.com/example/repo/pull/42","state":"closed","head":{"ref":"detent/acme_widgets_18","sha":"current-head"},"base":{"ref":"main","sha":"base-head"}}]`,
			wantFound: true,
			wantState: "CLOSED",
		},
		{
			name: "stale head does not match",
			body: `[{"node_id":"PR_42","number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","head":{"ref":"detent/acme_widgets_18","sha":"stale-head"},"base":{"ref":"main","sha":"base-head"}}]`,
		},
		{
			name: "no pull request",
			body: `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, []graphqlTestResponse{{
				method: http.MethodGet,
				path:   path,
				body:   tt.body,
			}})
			c := newGitHubTestConnector(t, server, Config{})

			pullRequest, found, err := c.LookupPullRequestByHead(t.Context(), "example/repo", branch, headSHA)
			if err != nil {
				t.Fatalf("LookupPullRequestByHead() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("LookupPullRequestByHead() = %#v, found = %v, want %v", pullRequest, found, tt.wantFound)
			}
			if !tt.wantFound {
				return
			}
			if pullRequest.Number != 42 || pullRequest.State != tt.wantState || pullRequest.BranchName != branch || pullRequest.HeadSHA != headSHA {
				t.Fatalf("LookupPullRequestByHead() = %#v, want PR 42 state %s on exact head", pullRequest, tt.wantState)
			}
		})
	}
}

func TestConnectorPullRequestDiffFingerprintIsContentStableAndCached(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodGet,
		path:   "/repos/example/repo/pulls/42/files?per_page=100",
		body:   `[{"filename":"z.go","status":"modified","sha":"blob-z"},{"filename":"a.go","previous_filename":"old.go","status":"renamed","sha":"blob-a"}]`,
	}})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{
		Identifier:   "example/repo#1",
		PRNumber:     &prNumber,
		PRRepository: "example/repo",
		PullRequest: &connector.PullRequest{
			Number:  prNumber,
			HeadSHA: "head-sha",
			BaseSHA: "base-sha",
		},
	}

	first, err := c.PullRequestDiffFingerprint(context.Background(), issue)
	if err != nil {
		t.Fatalf("PullRequestDiffFingerprint() error = %v", err)
	}
	second, err := c.PullRequestDiffFingerprint(context.Background(), issue)
	if err != nil {
		t.Fatalf("cached PullRequestDiffFingerprint() error = %v", err)
	}
	if first == "" || second != first {
		t.Fatalf("fingerprints = %q and %q, want identical non-empty values", first, second)
	}

	reordered := pullRequestDiffFingerprint([]restPullRequestFile{
		{Filename: "a.go", PreviousFilename: "old.go", Status: "renamed", SHA: "blob-a"},
		{Filename: "z.go", Status: "modified", SHA: "blob-z"},
	})
	if reordered != first {
		t.Fatalf("reordered fingerprint = %q, want %q", reordered, first)
	}
	changed := pullRequestDiffFingerprint([]restPullRequestFile{
		{Filename: "a.go", PreviousFilename: "old.go", Status: "renamed", SHA: "blob-a-changed"},
		{Filename: "z.go", Status: "modified", SHA: "blob-z"},
	})
	if changed == first {
		t.Fatalf("changed fingerprint = %q, want a new value", changed)
	}
}

func TestConnectorSecurityAuditSnapshotUsesStableMetadataAndTextualDiff(t *testing.T) {
	t.Parallel()

	metadata := `{"number":42,"title":"Trusted audit","body":"Review this change","head":{"ref":"detent/audit","sha":"head-sha"},"base":{"ref":"main","sha":"base-sha"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: metadata},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", accept: "application/vnd.github.diff", body: "diff --git a/a.go b/a.go\n@@ -0,0 +1 @@\n+safe\ndiff --git a/z.go b/z.go\n@@ -1 +1 @@\n-old\n+new\n"},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: metadata},
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{
		ID:           "issue-1",
		Identifier:   "example/repo#1",
		Title:        "Security issue",
		Description:  "Acceptance criteria",
		URL:          "https://github.test/example/repo/issues/1",
		PRNumber:     &prNumber,
		PRRepository: "example/repo",
		PullRequest:  &connector.PullRequest{Number: prNumber},
	}

	snapshot, err := c.SecurityAuditSnapshot(t.Context(), issue, 4096)
	if err != nil {
		t.Fatalf("SecurityAuditSnapshot() error = %v", err)
	}
	if snapshot.Repository != "example/repo" || snapshot.PRNumber != 42 || snapshot.BaseSHA != "base-sha" || snapshot.HeadSHA != "head-sha" || snapshot.DiffTruncated {
		t.Fatalf("SecurityAuditSnapshot() = %#v", snapshot)
	}
	if !strings.Contains(snapshot.Diff, "diff --git a/a.go b/a.go") || !strings.Contains(snapshot.Diff, "+safe") {
		t.Fatalf("SecurityAuditSnapshot().Diff = %q", snapshot.Diff)
	}
}

func TestConnectorSecurityAuditSnapshotRejectsHeadChangeDuringCollection(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: `{"number":42,"head":{"sha":"old-head"},"base":{"sha":"base"}}`},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", accept: "application/vnd.github.diff", body: "diff --git a/main.go b/main.go\n@@ -1 +1 @@\n-old\n+new\n"},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: `{"number":42,"head":{"sha":"new-head"},"base":{"sha":"base"}}`},
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{Identifier: "example/repo#1", PRNumber: &prNumber, PRRepository: "example/repo", PullRequest: &connector.PullRequest{Number: 42}}

	if _, err := c.SecurityAuditSnapshot(t.Context(), issue, 4096); err == nil || !strings.Contains(err.Error(), "head changed") {
		t.Fatalf("SecurityAuditSnapshot() error = %v", err)
	}
}

func TestConnectorSecurityAuditSnapshotMarksOversizedRawDiff(t *testing.T) {
	t.Parallel()

	metadata := `{"number":42,"head":{"sha":"head"},"base":{"sha":"base"}}`
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: metadata},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", accept: "application/vnd.github.diff", body: "12345"},
		{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: metadata},
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{Identifier: "example/repo#1", PRNumber: &prNumber, PRRepository: "example/repo", PullRequest: &connector.PullRequest{Number: 42}}

	snapshot, err := c.SecurityAuditSnapshot(t.Context(), issue, 4)
	if err != nil {
		t.Fatalf("SecurityAuditSnapshot() error = %v", err)
	}
	if snapshot.Diff != "1234" || !snapshot.DiffTruncated {
		t.Fatalf("SecurityAuditSnapshot() diff = %q truncated=%t", snapshot.Diff, snapshot.DiffTruncated)
	}
}

func TestConnectorHydratePullRequestNormalizesStaleSuccessfulWorkflowCheckRun(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/970",
			body:   `{"number":970,"html_url":"https://github.com/example/repo/pull/970","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/example_repo_970","sha":"head-sha"},"base":{"sha":"base-sha"},"updated_at":"2026-07-07T00:50:00Z"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"id":97001,"name":"Installer Smoke (ubuntu-latest)","status":"in_progress","conclusion":"success","details_url":"https://github.com/example/repo/actions/runs/28833549023/job/97001","started_at":"2026-07-07T00:45:00Z","completed_at":"2026-07-07T00:49:24Z"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/actions/runs/28833549023",
			body:   `{"id":28833549023,"status":"completed","conclusion":"success","created_at":"2026-07-07T00:44:00Z","run_started_at":"2026-07-07T00:45:00Z","updated_at":"2026-07-07T00:49:24Z"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/970/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 970),
	})
	c := newGitHubTestConnector(t, server, Config{
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	prNumber := 970
	issue := connector.Issue{
		ID:         "I_kw970",
		Identifier: "example/repo#970",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	pr := got.PullRequest
	if pr == nil {
		t.Fatalf("PullRequest = nil, want hydrated pull request")
		return
	}
	if pr.CIStatus != "pass" {
		t.Fatalf("CIStatus = %q, want pass", pr.CIStatus)
	}
	if len(pr.RunningChecks) != 0 {
		t.Fatalf("RunningChecks = %#v, want none", pr.RunningChecks)
	}
	if len(pr.StaleSuccessfulChecks) != 1 || pr.StaleSuccessfulChecks[0].Name != "Installer Smoke (ubuntu-latest)" {
		t.Fatalf("StaleSuccessfulChecks = %#v, want Installer Smoke anomaly", pr.StaleSuccessfulChecks)
	}
	for _, fragment := range []string{
		"stale_successful_check_run",
		"Installer Smoke (ubuntu-latest)",
		"action=normalize_success",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestConnectorHydratePullRequestUsesEffectiveCheckRunsForWorkflowTelemetry(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/971",
			body:   `{"number":971,"html_url":"https://github.com/example/repo/pull/971","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/example_repo_971","sha":"head-sha"},"base":{"sha":"base-sha"},"updated_at":"2026-07-16T18:10:00Z"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"id":97101,"name":"Verify","status":"completed","conclusion":"cancelled","details_url":"https://github.com/example/repo/actions/runs/7001/job/97101","created_at":"2026-07-16T18:00:00Z","started_at":"2026-07-16T18:01:00Z","completed_at":"2026-07-16T18:02:00Z"},{"id":97102,"name":"Verify","status":"completed","conclusion":"success","details_url":"https://github.com/example/repo/actions/runs/7002/job/97102","created_at":"2026-07-16T18:05:00Z","started_at":"2026-07-16T18:06:00Z","completed_at":"2026-07-16T18:10:00Z"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/actions/runs/7002",
			body:   `{"id":7002,"status":"completed","conclusion":"success","created_at":"2026-07-16T18:05:00Z","run_started_at":"2026-07-16T18:06:00Z","updated_at":"2026-07-16T18:10:00Z"}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/971/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 971),
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 971
	issue := connector.Issue{
		ID:         "I_kw971",
		Identifier: "example/repo#971",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	pr := got.PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want hydrated pull request")
		return
	}
	if pr.CIStatus != "pass" || pr.CIQueueSeconds != 60 || pr.CIDurationSeconds != 240 {
		t.Fatalf("PullRequest CI = %#v, want pass with 60s queue and 240s duration", pr)
	}
	for _, request := range server.requests() {
		if request["path"] == "/repos/example/repo/actions/runs/7001" {
			t.Fatalf("request path = %q, want superseded workflow run excluded", request["path"])
		}
	}
}

func TestConnectorHydratePullRequestIgnoresOptionalSkippedCheckRun(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/pyroapex/pulls/1648",
			body:   `{"number":1648,"html_url":"https://github.com/example/pyroapex/pull/1648","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/1647","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/pyroapex/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"id":1,"name":"Checks","status":"completed","conclusion":"success"},{"id":2,"name":"Changed-file coverage","status":"completed","conclusion":"success"},{"id":3,"name":"Race Tests","status":"completed","conclusion":"skipped"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/pyroapex/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/pyroapex/pulls/1648/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/pyroapex", 1648),
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 1648
	issue := connector.Issue{
		ID:           "I_kw1647",
		Identifier:   "example/pyroapex#1647",
		PRNumber:     &prNumber,
		PRRepository: "example/pyroapex",
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	pr := got.PullRequest
	if pr == nil {
		t.Fatal("PullRequest = nil, want hydrated pull request")
		return
	}
	if pr.MergeableState != "clean" || pr.CIStatus != "pass" {
		t.Fatalf("PullRequest = %#v, want clean with passing CI", pr)
	}
	if len(pr.RunningChecks) != 0 {
		t.Fatalf("RunningChecks = %#v, want none", pr.RunningChecks)
	}
}

func TestConnectorHydratePullRequestTreatsSkippedRequiredStatusCheckAsPending(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/example_repo_1","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"name":"Lint","status":"completed","conclusion":"success"},{"name":"Windows Core","status":"completed","conclusion":"skipped"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{RequiredStatusChecks: []string{"Lint", "Windows Core"}})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	pr := got.PullRequest
	if pr == nil {
		t.Fatalf("PullRequest = nil, want hydrated pull request")
		return
	}
	if pr.CIStatus != "pending" {
		t.Fatalf("CIStatus = %q, want pending for skipped required check", pr.CIStatus)
	}
	want := []connector.PullRequestCheck{{
		Name:   "Windows Core",
		Status: "pending",
	}}
	if !reflect.DeepEqual(pr.RequiredCheckFailures, want) {
		t.Fatalf("RequiredCheckFailures = %#v, want %#v", pr.RequiredCheckFailures, want)
	}
}

func TestConnectorHydratePullRequestWaitsForRunningRequiredCheckDespiteCancelledShadow(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/example_repo_1","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"name":"Changed-file coverage","status":"completed","conclusion":"success","started_at":"2026-07-16T18:21:41Z"},{"name":"Changed-file coverage","status":"completed","conclusion":"cancelled","started_at":"2026-07-16T18:20:00Z"},{"name":"Checks","status":"completed","conclusion":"failure","started_at":"2026-07-16T18:00:00Z"},{"id":5,"name":"Checks","status":"in_progress","started_at":"2026-07-16T18:21:40Z"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{RequiredStatusChecks: []string{"Changed-file coverage", "Checks"}})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}
	if got.PullRequest == nil {
		t.Fatal("PullRequest = nil, want hydrated pull request")
	}
	if got.PullRequest.CIStatus != "pending" {
		t.Fatalf("CIStatus = %q, want pending", got.PullRequest.CIStatus)
	}
	want := []connector.PullRequestCheck{{
		ID:     5,
		Name:   "Checks",
		Status: "in_progress",
	}}
	if !reflect.DeepEqual(got.PullRequest.RequiredCheckFailures, want) {
		t.Fatalf("RequiredCheckFailures = %#v, want %#v", got.PullRequest.RequiredCheckFailures, want)
	}
}

func TestConnectorHydratePullRequestBlocksMissingRequiredStatusCheck(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","draft":false,"head":{"ref":"detent/example_repo_1","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"name":"Lint","status":"completed","conclusion":"success"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{RequiredStatusChecks: []string{"Lint", "Windows Core"}})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}

	pr := got.PullRequest
	if pr == nil {
		t.Fatalf("PullRequest = nil, want hydrated pull request")
		return
	}
	if pr.CIStatus != "pending" {
		t.Fatalf("CIStatus = %q, want pending for missing required check", pr.CIStatus)
	}
	want := []connector.PullRequestCheck{{
		Name:       "Windows Core",
		Status:     "missing",
		Conclusion: "missing",
	}}
	if !reflect.DeepEqual(pr.RequiredCheckFailures, want) {
		t.Fatalf("RequiredCheckFailures = %#v, want %#v", pr.RequiredCheckFailures, want)
	}
}

func TestConnectorHydratePullRequestDetectsTransientKilledCheck(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","head":{"ref":"detent/example","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"id":9001,"name":"Checks","status":"completed","conclusion":"failure","details_url":"https://github.com/example/repo/actions/runs/8001/job/9001"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/actions/runs/8001",
			body:   `{"id":8001}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/check-runs/9001/annotations?per_page=100",
			body:   `[{"path":"internal/web/page_templ.go","annotation_level":"failure","message":"compile: signal: killed (typecheck)"}]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}
	if got.PullRequest == nil {
		t.Fatal("PullRequest = nil, want hydrated pull request")
	}
	checks := got.PullRequest.TransientFailedChecks
	if len(checks) != 1 {
		t.Fatalf("TransientFailedChecks = %#v, want one killed check", checks)
	}
	if checks[0].ID != 9001 || checks[0].WorkflowRunID != 8001 || checks[0].Name != "Checks" {
		t.Fatalf("TransientFailedChecks[0] = %#v, want check and workflow run IDs", checks[0])
	}
}

func TestConnectorHydratePullRequestIncludesWorkflowAndAnnotationsInRESTFanoutCap(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42",
			body:   `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","mergeable_state":"clean","head":{"ref":"detent/example","sha":"head-sha"},"base":{"sha":"base-sha"}}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/check-runs?per_page=100",
			body:   `{"check_runs":[{"id":9001,"name":"Checks","status":"completed","conclusion":"failure","details_url":"https://github.com/example/repo/actions/runs/8001/job/9001"}]}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/actions/runs/8001",
			body:   `{"id":8001}`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/commits/head-sha/statuses?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/check-runs/9001/annotations?per_page=100",
			body:   `[]`,
		},
		{
			method: http.MethodGet,
			path:   "/repos/example/repo/pulls/42/reviews?per_page=100",
			body:   `[]`,
		},
		emptyPullRequestCommentsResponse("example/repo", 42),
	})
	c := newGitHubTestConnector(t, server, Config{
		RESTFanoutMaxRequests:      4,
		DisableConditionalRequests: true,
	})
	prNumber := 42
	issue := connector.Issue{
		ID:         "I_kw42",
		Identifier: "example/repo#1",
		PRNumber:   &prNumber,
	}

	got, err := c.HydratePullRequest(context.Background(), issue)
	if err != nil {
		t.Fatalf("HydratePullRequest() error = %v", err)
	}
	if got.PullRequest == nil || got.PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonRESTFanoutDeferred {
		t.Fatalf("PullRequest = %#v, want REST fanout deferral", got.PullRequest)
	}
	if requests := server.requests(); len(requests) != 4 {
		t.Fatalf("outbound REST requests = %d, want fanout cap 4; requests = %#v", len(requests), requests)
	}

	usage := c.FlushRESTRateLimitUsage()
	if got := restEndpointUsageCount(usage.Requests, "other"); got != 0 {
		t.Fatalf("other usage count = %d, want no unclassified hydration requests; usage = %#v", got, usage.Requests)
	}
	if got := restEndpointUsageCount(usage.Requests, "workflow runs"); got != 1 {
		t.Fatalf("workflow runs usage count = %d, want 1; usage = %#v", got, usage.Requests)
	}
	if got := restEndpointUsageCount(usage.Requests, "check run annotations"); got != 1 {
		t.Fatalf("check run annotations usage count = %d, want throttled synthetic request; usage = %#v", got, usage.Requests)
	}
}

func emptyPullRequestCommentsResponse(repository string, number int) graphqlTestResponse {
	return graphqlTestResponse{
		method: http.MethodGet,
		path:   fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repository, number),
		body:   `[]`,
	}
}

func TestConnectorHydratePullRequestPropagatesCIDetailThrottles(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	throttleResponse := func(path string) graphqlTestResponse {
		return graphqlTestResponse{
			method: http.MethodGet,
			path:   path,
			status: http.StatusTooManyRequests,
			headers: map[string]string{
				"Retry-After":           "120",
				"X-RateLimit-Limit":     "5000",
				"X-RateLimit-Used":      "264",
				"X-RateLimit-Remaining": "4736",
				"X-RateLimit-Resource":  "core",
			},
			body: `{"message":"secondary rate limit"}`,
		}
	}
	tests := []struct {
		name         string
		responses    []graphqlTestResponse
		wantRequests int
	}{
		{
			name: "workflow run",
			responses: []graphqlTestResponse{
				{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","head":{"ref":"detent/example","sha":"head-sha"}}`},
				{method: http.MethodGet, path: "/repos/example/repo/commits/head-sha/check-runs?per_page=100", body: `{"check_runs":[{"id":9001,"name":"Checks","status":"completed","conclusion":"failure","details_url":"https://github.com/example/repo/actions/runs/8001/job/9001"}]}`},
				throttleResponse("/repos/example/repo/actions/runs/8001"),
				{method: http.MethodGet, path: "/repos/example/repo/commits/head-sha/statuses?per_page=100", body: `[]`},
				{method: http.MethodGet, path: "/repos/example/repo/check-runs/9001/annotations?per_page=100", body: `[]`},
				{method: http.MethodGet, path: "/repos/example/repo/pulls/42/reviews?per_page=100", body: `[]`},
			},
			wantRequests: 3,
		},
		{
			name: "check run annotations",
			responses: []graphqlTestResponse{
				{method: http.MethodGet, path: "/repos/example/repo/pulls/42", body: `{"number":42,"html_url":"https://github.com/example/repo/pull/42","state":"open","head":{"ref":"detent/example","sha":"head-sha"}}`},
				{method: http.MethodGet, path: "/repos/example/repo/commits/head-sha/check-runs?per_page=100", body: `{"check_runs":[{"id":9001,"name":"Checks","status":"completed","conclusion":"failure"}]}`},
				{method: http.MethodGet, path: "/repos/example/repo/commits/head-sha/statuses?per_page=100", body: `[]`},
				throttleResponse("/repos/example/repo/check-runs/9001/annotations?per_page=100"),
				{method: http.MethodGet, path: "/repos/example/repo/pulls/42/reviews?per_page=100", body: `[]`},
			},
			wantRequests: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newGraphQLTestServer(t, test.responses)
			c := newGitHubTestConnector(t, server, Config{
				Now: func() time.Time {
					return now
				},
			})
			prNumber := 42
			issue := connector.Issue{
				ID:         "I_kw42",
				Identifier: "example/repo#1",
				PRNumber:   &prNumber,
			}

			got, err := c.HydratePullRequest(context.Background(), issue)
			if err != nil {
				t.Fatalf("HydratePullRequest() error = %v", err)
			}
			if got.PullRequest == nil {
				t.Fatal("PullRequest = nil, want hydration throttle marker")
			}
			if got.PullRequest.HydrationUnavailableReason != connector.PullRequestHydrationReasonSecondaryThrottled {
				t.Fatalf("HydrationUnavailableReason = %q, want secondary_throttled", got.PullRequest.HydrationUnavailableReason)
			}
			if got.PullRequest.HydrationNextRetryAt == nil || !got.PullRequest.HydrationNextRetryAt.After(now.Add(120*time.Second)) {
				t.Fatalf("HydrationNextRetryAt = %v, want retry-after plus jitter", got.PullRequest.HydrationNextRetryAt)
			}
			if requests := server.requests(); len(requests) != test.wantRequests {
				t.Fatalf("outbound REST requests = %d, want %d; requests = %#v", len(requests), test.wantRequests, requests)
			}
		})
	}
}

func TestConnectorRerunPullRequestChecksUsesWorkflowRun(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			method: http.MethodPost,
			path:   "/repos/example/repo/actions/runs/8001/rerun-failed-jobs",
			body:   `{}`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})
	prNumber := 42
	issue := connector.Issue{
		ID:           "I_kw42",
		Identifier:   "example/repo#1",
		PRNumber:     &prNumber,
		PRRepository: "example/repo",
	}

	err := c.RerunPullRequestChecks(context.Background(), issue, []connector.PullRequestCheck{{
		ID:            9001,
		WorkflowRunID: 8001,
		Name:          "Checks",
	}})
	if err != nil {
		t.Fatalf("RerunPullRequestChecks() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("requests len = %d, want 1", len(requests))
	}
	if requests[0]["method"] != http.MethodPost || requests[0]["path"] != "/repos/example/repo/actions/runs/8001/rerun-failed-jobs" {
		t.Fatalf("request = %#v, want workflow rerun", requests[0])
	}
}

func TestConnectorSetIssueFieldUsesIssueFieldEndpoint(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"nodes":[{"__typename":"Issue","id":"I_kw28","number":28,"repository":{"nameWithOwner":"digitaldrywood/detent"}}]}}`,
		},
		{
			method: http.MethodPost,
			path:   "/repos/digitaldrywood/detent/issues/28/issue-field-values",
			body:   `[{"issue_field_id":123,"node_id":"IFSS_status","data_type":"single_select","value":1,"single_select_option":{"id":1,"name":"In Progress","color":"blue"}}]`,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})

	if err := c.SetIssueField(context.Background(), "I_kw28", 123, "In Progress"); err != nil {
		t.Fatalf("SetIssueField() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if requests[1]["method"] != http.MethodPost || requests[1]["path"] != "/repos/digitaldrywood/detent/issues/28/issue-field-values" {
		t.Fatalf("issue field request = %#v, want REST issue field update", requests[1])
	}
	body := requests[1]["body"].(map[string]any)
	values := body["issue_field_values"].([]any)
	if len(values) != 1 {
		t.Fatalf("issue_field_values len = %d, want 1", len(values))
	}
	value := values[0].(map[string]any)
	if value["field_id"] != float64(123) || value["value"] != "In Progress" {
		t.Fatalf("issue field value = %#v, want field_id 123 value In Progress", value)
	}
}

func TestConnectorClearIssueFieldUsesIssueFieldEndpoint(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{
			body: `{"data":{"nodes":[{"__typename":"Issue","id":"I_kw28","number":28,"repository":{"nameWithOwner":"digitaldrywood/detent"}}]}}`,
		},
		{
			method: http.MethodDelete,
			path:   "/repos/digitaldrywood/detent/issues/28/issue-field-values/123",
			status: http.StatusNoContent,
		},
	})
	c := newGitHubTestConnector(t, server, Config{})

	if err := c.ClearIssueField(context.Background(), "I_kw28", 123); err != nil {
		t.Fatalf("ClearIssueField() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if requests[1]["method"] != http.MethodDelete || requests[1]["path"] != "/repos/digitaldrywood/detent/issues/28/issue-field-values/123" {
		t.Fatalf("issue field request = %#v, want REST issue field delete", requests[1])
	}
}

func TestConnectorCloseIssueCallsCloseIssue(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{{
		method: http.MethodPatch,
		path:   "/repos/example/repo/issues/1",
		body:   `{"node_id":"I_kw1","state":"closed"}`,
	}})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.CloseIssue(context.Background(), " I_kw1 "); err != nil {
		t.Fatalf("CloseIssue() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	if requests[0]["method"] != http.MethodPatch || requests[0]["path"] != "/repos/example/repo/issues/1" {
		t.Fatalf("close request = %#v, want REST issue patch", requests[0])
	}
	body := requests[0]["body"].(map[string]any)
	if body["state"] != "closed" || body["state_reason"] != "completed" {
		t.Fatalf("close body = %#v, want closed/completed", body)
	}
}

func TestConnectorUpdateIssueStateWritesStatusOptionID(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"}}]}}}}`},
		{body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_ready","name":"Ready"},{"id":"OPT_todo","name":"Todo"}]}}}}`},
		{body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_1"}}}}`},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap:    map[string]string{"Todo": "Ready"},
	})

	if err := c.UpdateIssueState(context.Background(), "I_kw1", "Todo"); err != nil {
		t.Fatalf("UpdateIssueState() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(requests))
	}
	updateQuery := requests[2]["query"].(string)
	if !strings.Contains(updateQuery, "updateProjectV2ItemFieldValue") {
		t.Fatalf("query = %q, want updateProjectV2ItemFieldValue", updateQuery)
	}
	if strings.Contains(updateQuery, "rateLimit") {
		t.Fatalf("query = %q, want no rateLimit on mutation root", updateQuery)
	}
	variables := requests[2]["variables"].(map[string]any)
	want := map[string]any{
		"projectId": "PVT_1",
		"itemId":    "PVTI_1",
		"fieldId":   "PVTSSF_status",
		"optionId":  "OPT_ready",
	}
	for key, value := range want {
		if variables[key] != value {
			t.Fatalf("%s = %v, want %v", key, variables[key], value)
		}
	}
	if variables["optionId"] == "Ready" {
		t.Fatal("optionId used the option name, want option id")
	}
}

func TestConnectorRemoveIssueFromProjectDeletesProjectV2Item(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"},"statusValue":{"name":"Todo"}}]}}}}`},
		{body: `{"data":{"deleteProjectV2Item":{"deletedItemId":"PVTI_1"}}}`},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
	})

	if err := c.RemoveIssueFromProject(context.Background(), "I_kw1"); err != nil {
		t.Fatalf("RemoveIssueFromProject() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want project item lookup and delete", len(requests))
	}
	deleteQuery := requests[1]["query"].(string)
	if !strings.Contains(deleteQuery, "deleteProjectV2Item") {
		t.Fatalf("query = %q, want deleteProjectV2Item", deleteQuery)
	}
	if strings.Contains(deleteQuery, "rateLimit") {
		t.Fatalf("query = %q, want no rateLimit on mutation root", deleteQuery)
	}
	variables := requests[1]["variables"].(map[string]any)
	want := map[string]any{
		"projectId": "PVT_1",
		"itemId":    "PVTI_1",
	}
	for key, value := range want {
		if variables[key] != value {
			t.Fatalf("%s = %v, want %v", key, variables[key], value)
		}
	}
}

func TestConnectorVerifyStatusOptionsChecksMappedStatusOptions(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_review","name":"Reviewing"}]}}}}`},
	})
	c := newGitHubTestConnector(t, server, Config{
		ProjectSlug: "PVT_1",
		StateMap:    map[string]string{"Human Review": "Reviewing"},
	})

	err := c.VerifyStatusOptions(context.Background(), []string{"Human Review", "Merging"})
	if err == nil {
		t.Fatal("VerifyStatusOptions() error = nil, want ErrStatusOptionNotFound")
		return
	}
	if !errors.Is(err, ErrStatusOptionNotFound) {
		t.Fatalf("VerifyStatusOptions() error = %v, want ErrStatusOptionNotFound", err)
	}
	if !strings.Contains(err.Error(), "Merging") {
		t.Fatalf("VerifyStatusOptions() error = %q, want missing Merging detail", err.Error())
	}

	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	if strings.Contains(requests[0]["query"].(string), "updateProjectV2ItemFieldValue") {
		t.Fatalf("VerifyStatusOptions issued mutation: %q", requests[0]["query"])
	}
}

func TestConnectorUpdateIssueStateTerminalTransitionRules(t *testing.T) {
	t.Parallel()

	type transitionScenario struct {
		name          string
		currentStatus string
		targetState   string
		wantBlocked   bool
	}

	scenarios := []transitionScenario{
		{
			name:          "terminal to non-terminal",
			currentStatus: "Closed",
			targetState:   "In Progress",
			wantBlocked:   true,
		},
		{
			name:          "terminal to terminal",
			currentStatus: "Closed",
			targetState:   "Cancelled",
		},
		{
			name:          "non-terminal to terminal",
			currentStatus: "Working",
			targetState:   "Done",
		},
	}

	modes := []struct {
		name           string
		issueID        string
		newConnector   func(*testing.T, *graphqlTestServer, transitionScenario) *Connector
		responses      func(transitionScenario) []graphqlTestResponse
		mutationIssued func([]map[string]any) bool
	}{
		{
			name:    GitHubStatusSourceLabel,
			issueID: "I_1",
			newConnector: func(t *testing.T, server *graphqlTestServer, _ transitionScenario) *Connector {
				t.Helper()
				c := newGitHubTestConnector(t, server, Config{
					GitHubStatusSource: GitHubStatusSourceLabel,
					Repository:         "digitaldrywood/detent",
					ActiveStates:       []string{"In Progress"},
					TerminalStates:     []string{"Done", "Cancelled"},
					StateMap:           githubTransitionStateMap(),
				})
				c.projectCache.SetIssueRef("I_1", issueRef{Owner: "digitaldrywood", Name: "detent", Number: 1})
				return c
			},
			responses: func(scenario transitionScenario) []graphqlTestResponse {
				current := githubTransitionLabelIssueResponse("I_1", scenario.currentStatus)
				if scenario.wantBlocked {
					return []graphqlTestResponse{current}
				}
				target := githubTransitionLabelIssueResponse("I_1", scenario.currentStatus)
				return []graphqlTestResponse{
					current,
					target,
					{
						method: http.MethodPut,
						path:   "/repos/digitaldrywood/detent/issues/1/labels",
						body:   githubTransitionLabelUpdateResponse(scenario.targetState),
					},
				}
			},
			mutationIssued: func(requests []map[string]any) bool {
				for _, request := range requests {
					if request["method"] == http.MethodPut && request["path"] == "/repos/digitaldrywood/detent/issues/1/labels" {
						return true
					}
				}
				return false
			},
		},
		{
			name:    GitHubStatusSourceIssueField,
			issueID: "I_1",
			newConnector: func(t *testing.T, server *graphqlTestServer, _ transitionScenario) *Connector {
				t.Helper()
				c := newGitHubTestConnector(t, server, Config{
					GitHubStatusSource: GitHubStatusSourceIssueField,
					Repository:         "digitaldrywood/detent",
					StatusField:        "Status",
					ActiveStates:       []string{"In Progress"},
					TerminalStates:     []string{"Done", "Cancelled"},
					StateMap:           githubTransitionStateMap(),
				})
				c.projectCache.SetIssueRef("I_1", issueRef{Owner: "digitaldrywood", Name: "detent", Number: 1})
				return c
			},
			responses: func(scenario transitionScenario) []graphqlTestResponse {
				responses := []graphqlTestResponse{
					githubTransitionIssueFieldValuesResponse(scenario.currentStatus),
					githubTransitionIssueFieldMetadataResponse(),
				}
				if scenario.wantBlocked {
					return responses
				}
				return append(responses, githubTransitionIssueFieldUpdateResponse(scenario.targetState))
			},
			mutationIssued: func(requests []map[string]any) bool {
				for _, request := range requests {
					if request["method"] == http.MethodPost && request["path"] == "/repos/digitaldrywood/detent/issues/1/issue-field-values" {
						return true
					}
				}
				return false
			},
		},
		{
			name:    GitHubStatusSourceProjectV2,
			issueID: "I_kw1",
			newConnector: func(t *testing.T, server *graphqlTestServer, _ transitionScenario) *Connector {
				t.Helper()
				return newGitHubTestConnector(t, server, Config{
					ProjectSlug:    "PVT_1",
					ActiveStates:   []string{"In Progress"},
					TerminalStates: []string{"Done", "Cancelled"},
					StateMap:       githubTransitionStateMap(),
				})
			},
			responses: func(scenario transitionScenario) []graphqlTestResponse {
				responses := []graphqlTestResponse{githubTransitionProjectItemResponse(scenario.currentStatus)}
				if scenario.wantBlocked {
					return responses
				}
				return append(responses,
					githubTransitionProjectMetadataResponse(),
					graphqlTestResponse{body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_1"}}}}`},
				)
			},
			mutationIssued: func(requests []map[string]any) bool {
				for _, request := range requests {
					query, ok := request["query"].(string)
					if ok && strings.Contains(query, "updateProjectV2ItemFieldValue") {
						return true
					}
				}
				return false
			},
		},
	}

	for _, mode := range modes {
		mode := mode
		for _, scenario := range scenarios {
			scenario := scenario
			t.Run(mode.name+"/"+scenario.name, func(t *testing.T) {
				t.Parallel()

				server := newGraphQLTestServer(t, mode.responses(scenario))
				c := mode.newConnector(t, server, scenario)

				err := c.UpdateIssueState(context.Background(), mode.issueID, scenario.targetState)
				if scenario.wantBlocked {
					assertStateUpdateBlocked(t, err, mode.issueID, "Done", scenario.targetState)
				} else if err != nil {
					t.Fatalf("UpdateIssueState() error = %v", err)
				}

				requests := server.requests()
				if len(requests) != len(mode.responses(scenario)) {
					t.Fatalf("request count = %d, want %d", len(requests), len(mode.responses(scenario)))
				}
				mutationIssued := mode.mutationIssued(requests)
				if scenario.wantBlocked && mutationIssued {
					t.Fatalf("blocked transition issued mutation: %#v", requests)
				}
				if !scenario.wantBlocked && !mutationIssued {
					t.Fatalf("allowed transition did not issue mutation: %#v", requests)
				}
			})
		}
	}
}

func assertStateUpdateBlocked(t *testing.T, err error, issueID string, currentState string, targetState string) {
	t.Helper()

	if !errors.Is(err, connector.ErrStateUpdateBlocked) {
		t.Fatalf("UpdateIssueState() error = %v, want ErrStateUpdateBlocked", err)
	}
	var blocked *connector.StateUpdateBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("UpdateIssueState() error = %T, want StateUpdateBlockedError", err)
	}
	if blocked.IssueID != issueID || blocked.CurrentState != currentState || blocked.TargetState != targetState {
		t.Fatalf("StateUpdateBlockedError = %#v, want issue_id=%q current_state=%q target_state=%q", blocked, issueID, currentState, targetState)
	}
}

func githubTransitionStateMap() map[string]string {
	return map[string]string{
		"Done":        "Closed",
		"Cancelled":   "Archived",
		"In Progress": "Working",
	}
}

func githubTransitionExternalState(stateName string) string {
	if mapped, ok := githubTransitionStateMap()[stateName]; ok {
		return mapped
	}
	return stateName
}

func githubTransitionLabelIssueResponse(issueID string, status string) graphqlTestResponse {
	return graphqlTestResponse{
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/issues/1",
		body: fmt.Sprintf(
			`{"node_id":%q,"number":1,"title":"Transition issue","body":"","state":"open","html_url":"https://github.com/digitaldrywood/detent/issues/1","assignees":[],"labels":[{"name":%q},{"name":"bug"}]}`,
			issueID,
			"detent:"+statusLabelSlug(status),
		),
	}
}

func githubTransitionLabelUpdateResponse(targetState string) string {
	return fmt.Sprintf(`[{"name":"bug"},{"name":%q}]`, "detent:"+statusLabelSlug(githubTransitionExternalState(targetState)))
}

func githubTransitionIssueFieldValuesResponse(status string) graphqlTestResponse {
	return graphqlTestResponse{
		method: http.MethodGet,
		path:   "/repos/digitaldrywood/detent/issues/1/issue-field-values?per_page=100",
		body: fmt.Sprintf(
			`[{"issue_field_id":10,"node_id":"IFV_1","data_type":"single_select","single_select_option":{"id":1,"name":%q,"color":"gray"}}]`,
			status,
		),
	}
}

func githubTransitionIssueFieldMetadataResponse() graphqlTestResponse {
	return graphqlTestResponse{
		method: http.MethodGet,
		path:   "/orgs/digitaldrywood/issue-fields?per_page=100",
		body:   `[{"id":10,"node_id":"IFSS_status","name":"Status","data_type":"single_select","options":[{"id":1,"name":"Closed","color":"purple"},{"id":2,"name":"Archived","color":"gray"},{"id":3,"name":"Working","color":"yellow"}]}]`,
	}
}

func githubTransitionIssueFieldUpdateResponse(targetState string) graphqlTestResponse {
	return graphqlTestResponse{
		method: http.MethodPost,
		path:   "/repos/digitaldrywood/detent/issues/1/issue-field-values",
		body: fmt.Sprintf(
			`[{"issue_field_id":10,"node_id":"IFV_1","data_type":"single_select","single_select_option":{"id":2,"name":%q,"color":"gray"}}]`,
			githubTransitionExternalState(targetState),
		),
	}
}

func githubTransitionProjectItemResponse(status string) graphqlTestResponse {
	return graphqlTestResponse{
		body: fmt.Sprintf(
			`{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"},"statusValue":{"name":%q}}]}}}}`,
			status,
		),
	}
}

func githubTransitionProjectMetadataResponse() graphqlTestResponse {
	return graphqlTestResponse{
		body: `{"data":{"node":{"field":{"id":"PVTSSF_status","options":[{"id":"OPT_closed","name":"Closed"},{"id":"OPT_archived","name":"Archived"},{"id":"OPT_working","name":"Working"}]}}}}`,
	}
}

func TestConnectorSetAssigneeAddsUserByLogin(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/issues/1", body: `{"node_id":"I_kw1","number":1,"title":"Issue","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[],"labels":[]}`},
		{method: http.MethodPost, path: "/repos/example/repo/issues/1/assignees", body: `{"node_id":"I_kw1"}`},
	})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.SetAssignee(context.Background(), " I_kw1 ", " worker-1 "); err != nil {
		t.Fatalf("SetAssignee() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	assignBody := requests[1]["body"].(map[string]any)
	assignees, ok := assignBody["assignees"].([]any)
	if !ok || len(assignees) != 1 || assignees[0] != "worker-1" {
		t.Fatalf("assignees = %#v, want [worker-1]", assignBody["assignees"])
	}
}

func TestConnectorSetAssigneeReplacesExistingAssignees(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/issues/1", body: `{"node_id":"I_kw1","number":1,"title":"Issue","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[{"node_id":"U_old","login":"old-owner"},{"node_id":"U_worker","login":"worker-1"}],"labels":[]}`},
		{method: http.MethodDelete, path: "/repos/example/repo/issues/1/assignees", body: `{"node_id":"I_kw1"}`},
	})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.SetAssignee(context.Background(), "I_kw1", "worker-1"); err != nil {
		t.Fatalf("SetAssignee() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	removeBody := requests[1]["body"].(map[string]any)
	assignees, ok := removeBody["assignees"].([]any)
	if !ok || len(assignees) != 1 || assignees[0] != "old-owner" {
		t.Fatalf("removed assignees = %#v, want [old-owner]", removeBody["assignees"])
	}
}

func TestConnectorSetAssigneeAddsReplacementBeforeRemovingExistingAssignees(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/issues/1", body: `{"node_id":"I_kw1","number":1,"title":"Issue","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[{"node_id":"U_old","login":"old-owner"}],"labels":[]}`},
		{method: http.MethodPost, path: "/repos/example/repo/issues/1/assignees", body: `{"node_id":"I_kw1"}`},
		{method: http.MethodDelete, path: "/repos/example/repo/issues/1/assignees", body: `{"node_id":"I_kw1"}`},
	})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.SetAssignee(context.Background(), "I_kw1", "worker-1"); err != nil {
		t.Fatalf("SetAssignee() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(requests))
	}
	if requests[1]["method"] != http.MethodPost {
		t.Fatalf("second request = %#v, want assignee add", requests[1])
	}
	if requests[2]["method"] != http.MethodDelete {
		t.Fatalf("third request = %#v, want assignee remove", requests[2])
	}
}

func TestConnectorSetAssigneeDoesNotRemoveExistingAssigneesWhenAddFails(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{method: http.MethodGet, path: "/repos/example/repo/issues/1", body: `{"node_id":"I_kw1","number":1,"title":"Issue","state":"open","html_url":"https://github.com/example/repo/issues/1","assignees":[{"node_id":"U_old","login":"old-owner"}],"labels":[]}`},
		{method: http.MethodPost, path: "/repos/example/repo/issues/1/assignees", status: http.StatusUnprocessableEntity, body: `{"message":"not assignable"}`},
	})
	c := newGitHubTestConnector(t, server, Config{})
	c.projectCache.SetIssueRef("I_kw1", issueRef{Owner: "example", Name: "repo", Number: 1})

	if err := c.SetAssignee(context.Background(), "I_kw1", "worker-1"); err == nil {
		t.Fatal("SetAssignee() error = nil, want error")
	}

	requests := server.requests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if requests[1]["method"] != http.MethodPost {
		t.Fatalf("second request = %#v, want assignee add", requests[1])
	}
}

func TestConnectorSetFieldProvisionsOwnerOptionAndWritesProjectValue(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"}}]}}}}`},
		{body: `{"data":{"node":{"__typename":"ProjectV2","field":{"__typename":"ProjectV2SingleSelectField","id":"PVTSSF_owner","options":[{"id":"OPT_other","name":"worker-0","color":"BLUE","description":"Existing owner."}]}}}}`},
		{body: `{"data":{"node":{"__typename":"ProjectV2","field":{"__typename":"ProjectV2SingleSelectField","id":"PVTSSF_owner","options":[{"id":"OPT_other","name":"worker-0","color":"BLUE","description":"Existing owner."},{"id":"OPT_concurrent","name":"worker-2","color":"BLUE","description":"Concurrent owner."}]}}}}`},
		{body: `{"data":{"updateProjectV2Field":{"projectV2Field":{"options":[{"id":"OPT_other","name":"worker-0","color":"BLUE","description":"Existing owner."},{"id":"OPT_concurrent","name":"worker-2","color":"BLUE","description":"Concurrent owner."},{"id":"OPT_worker","name":"worker-1","color":"BLUE","description":"Detent ownership identity."}]}}}}`},
		{body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_1"}}}}`},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	if err := c.SetField(context.Background(), "I_kw1", " Owner ", " worker-1 "); err != nil {
		t.Fatalf("SetField() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 5 {
		t.Fatalf("request count = %d, want 5", len(requests))
	}
	fieldVariables := requests[1]["variables"].(map[string]any)
	if fieldVariables["fieldName"] != "Owner" {
		t.Fatalf("fieldName = %v, want Owner", fieldVariables["fieldName"])
	}
	refetchVariables := requests[2]["variables"].(map[string]any)
	if refetchVariables["fieldName"] != "Owner" {
		t.Fatalf("refetch fieldName = %v, want Owner", refetchVariables["fieldName"])
	}
	input := graphQLInput(t, requests[3])
	if input["fieldId"] != "PVTSSF_owner" {
		t.Fatalf("fieldId = %v, want PVTSSF_owner", input["fieldId"])
	}
	options := graphQLOptions(t, input)
	if got := optionNames(options); !reflect.DeepEqual(got, []string{"worker-0", "worker-2", "worker-1"}) {
		t.Fatalf("option names = %#v, want worker-0, worker-2, worker-1", got)
	}
	updateVariables := requests[4]["variables"].(map[string]any)
	want := map[string]any{
		"projectId": "PVT_1",
		"itemId":    "PVTI_1",
		"fieldId":   "PVTSSF_owner",
		"optionId":  "OPT_worker",
	}
	for key, value := range want {
		if updateVariables[key] != value {
			t.Fatalf("%s = %v, want %v", key, updateVariables[key], value)
		}
	}
	if !strings.Contains(requests[4]["query"].(string), "updateProjectV2ItemFieldValue") {
		t.Fatalf("update query = %q, want updateProjectV2ItemFieldValue", requests[4]["query"])
	}
}

func TestConnectorSetFieldWritesTextProjectValue(t *testing.T) {
	t.Parallel()

	server := newGraphQLTestServer(t, []graphqlTestResponse{
		{body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"}}]}}}}`},
		{body: `{"data":{"node":{"__typename":"ProjectV2","field":{"__typename":"ProjectV2Field","id":"PVTF_lease","dataType":"TEXT"}}}}`},
		{body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_1"}}}}`},
	})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})

	if err := c.SetField(context.Background(), "I_kw1", "Detent Lease", "2026-06-02T15:00:00Z"); err != nil {
		t.Fatalf("SetField() error = %v", err)
	}

	requests := server.requests()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3", len(requests))
	}
	fieldVariables := requests[1]["variables"].(map[string]any)
	if fieldVariables["fieldName"] != "Detent Lease" {
		t.Fatalf("fieldName = %v, want Detent Lease", fieldVariables["fieldName"])
	}
	updateVariables := requests[2]["variables"].(map[string]any)
	want := map[string]any{
		"projectId": "PVT_1",
		"itemId":    "PVTI_1",
		"fieldId":   "PVTF_lease",
		"text":      "2026-06-02T15:00:00Z",
	}
	for key, value := range want {
		if updateVariables[key] != value {
			t.Fatalf("%s = %v, want %v", key, updateVariables[key], value)
		}
	}
	if !strings.Contains(requests[2]["query"].(string), "text") {
		t.Fatalf("update query = %q, want text field mutation", requests[2]["query"])
	}
}

type graphqlTestServer struct {
	*httptest.Server
	t           *testing.T
	mu          sync.Mutex
	responses   []graphqlTestResponse
	seen        []map[string]any
	requestSeen chan struct{}
}

type graphqlTestResponse struct {
	status  int
	method  string
	path    string
	accept  string
	headers map[string]string
	body    string
	release <-chan struct{}
}

func newGraphQLTestServer(t *testing.T, responses []graphqlTestResponse) *graphqlTestServer {
	t.Helper()

	server := &graphqlTestServer{
		t:           t,
		responses:   responses,
		requestSeen: make(chan struct{}, len(responses)+1),
	}
	server.Server = httptest.NewServer(http.HandlerFunc(server.serveHTTP))
	t.Cleanup(server.Close)
	return server
}

func (s *graphqlTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	payload := map[string]any{
		"method": r.Method,
		"path":   r.URL.RequestURI(),
	}
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		payload["if_none_match"] = ifNoneMatch
	}
	if r.Method == http.MethodPost && r.URL.Path == "/" {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			s.t.Fatalf("Decode() error = %v", err)
		}
		payload["method"] = r.Method
		payload["path"] = r.URL.RequestURI()
	} else {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			s.t.Fatalf("ReadAll() error = %v", err)
		}
		if len(raw) > 0 {
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				s.t.Fatalf("Unmarshal() error = %v", err)
			}
			payload["body"] = body
		}
	}

	s.mu.Lock()
	s.seen = append(s.seen, payload)
	select {
	case s.requestSeen <- struct{}{}:
	default:
	}
	if len(s.responses) == 0 {
		s.mu.Unlock()
		s.t.Fatalf("unexpected GraphQL request: %v", payload)
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	s.mu.Unlock()
	if response.method != "" && response.method != r.Method {
		s.t.Fatalf("method = %s, want %s", r.Method, response.method)
	}
	if response.path != "" && response.path != r.URL.RequestURI() {
		s.t.Fatalf("path = %s, want %s", r.URL.RequestURI(), response.path)
	}
	if response.accept != "" && response.accept != r.Header.Get("Accept") {
		s.t.Fatalf("accept = %s, want %s", r.Header.Get("Accept"), response.accept)
	}

	if response.release != nil {
		<-response.release
	}

	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	for key, value := range response.headers {
		w.Header().Set(key, value)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(response.body))
}

func (s *graphqlTestServer) requests() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]map[string]any, len(s.seen))
	copy(out, s.seen)
	return out
}

func waitForGraphQLRequests(t *testing.T, server *graphqlTestServer, want int) []map[string]any {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		requests := server.requests()
		if len(requests) >= want {
			return requests
		}
		time.Sleep(10 * time.Millisecond)
	}
	requests := server.requests()
	t.Fatalf("request count = %d, want at least %d", len(requests), want)
	return nil
}

func projectItemsPageResponse(hasNextPage bool, endCursor string, nodes []string) string {
	cursor := "null"
	if endCursor != "" {
		cursor = fmt.Sprintf("%q", endCursor)
	}
	return fmt.Sprintf(
		`{"data":{"node":{"items":{"pageInfo":{"hasNextPage":%t,"endCursor":%s},"nodes":[%s]}}}}`,
		hasNextPage,
		cursor,
		strings.Join(nodes, ","),
	)
}

func projectItemsPageResponseWithTotal(totalCount int, hasNextPage bool, endCursor string, nodes []string) string {
	cursor := "null"
	if endCursor != "" {
		cursor = fmt.Sprintf("%q", endCursor)
	}
	return fmt.Sprintf(
		`{"data":{"node":{"items":{"totalCount":%d,"pageInfo":{"hasNextPage":%t,"endCursor":%s},"nodes":[%s]}}}}`,
		totalCount,
		hasNextPage,
		cursor,
		strings.Join(nodes, ","),
	)
}

func projectAuthorizationItemsResponse(status string) string {
	return fmt.Sprintf(
		`{"data":{"node":{"items":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"PVTI_authorized","content":{"__typename":"Issue","id":"I_authorized","number":1121,"title":"Authorized issue","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/1121","author":{"login":"alice"},"assignees":{"nodes":[{"login":"worker-1"}]},"labels":{"nodes":[{"name":"ready"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":%q}},{"id":"PVTI_foreign","content":{"__typename":"Issue","id":"I_foreign","number":1122,"title":"Foreign issue","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/1122","author":{"login":"bob"},"assignees":{"nodes":[{"login":"worker-2"}]},"labels":{"nodes":[{"name":"blocked"}]},"repository":{"nameWithOwner":"digitaldrywood/detent"}},"statusValue":{"name":%q}}]}}}}`,
		status,
		status,
	)
}

func projectIssueNodes(count int, status string) []string {
	nodes := make([]string, 0, count)
	for i := range count {
		number := 1000 + i
		nodes = append(nodes, projectIssueNode(
			fmt.Sprintf("PVTI_%d", number),
			fmt.Sprintf("I_%d", number),
			number,
			fmt.Sprintf("Issue %d", number),
			status,
		))
	}
	return nodes
}

func projectIssueNode(itemID string, issueID string, number int, title string, status string) string {
	statusValue := "null"
	if status != "" {
		statusValue = fmt.Sprintf(`{"name":%q}`, status)
	}
	return fmt.Sprintf(
		`{"id":%q,"content":{"__typename":"Issue","id":%q,"number":%d,"title":%q,"body":"","state":"OPEN","url":"https://github.com/digitaldrywood/detent/issues/%d","createdAt":null,"updatedAt":null,"assignees":{"nodes":[]},"labels":{"nodes":[]},"repository":{"nameWithOwner":"digitaldrywood/detent"},"closedByPullRequestsReferences":{"nodes":[]}},"statusValue":%s,"priorityValue":null}`,
		itemID,
		issueID,
		number,
		title,
		number,
		statusValue,
	)
}

func newGitHubTestConnector(t *testing.T, server *graphqlTestServer, cfg Config) *Connector {
	t.Helper()

	cfg.Endpoint = server.URL
	cfg.APIKey = "token"
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = server.Client()
	}
	cfg.GHToken = func(context.Context, string) (string, error) {
		t.Fatal("gh token fallback should not run")
		return "", nil
	}
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	c.client.restBackoffs = newRESTBackoffRegistry()
	return c
}

func TestPullRequestCheckInventoryRetainsSettledNames(t *testing.T) {
	t.Parallel()

	checks := pullRequestCheckInventory(
		[]restCheckRun{{ID: 1, Name: "Test", Status: "completed", Conclusion: "success"}},
		[]restCommitStatus{{Context: "Deploy", State: "pending"}, {Context: "test", State: "success"}},
	)
	if len(checks) != 2 {
		t.Fatalf("checks = %#v, want check run and distinct status context", checks)
	}
	if checks[0].Name != "Test" || checks[0].Status != "completed" || checks[0].Conclusion != "success" {
		t.Fatalf("check run = %#v", checks[0])
	}
	if checks[1].Name != "Deploy" || checks[1].Status != "pending" {
		t.Fatalf("status context = %#v", checks[1])
	}
}

func TestPullRequestCheckInventoryRetainsIgnoredNames(t *testing.T) {
	t.Parallel()

	tests := []string{"cancelled", "canceled", "skipped"}
	for _, conclusion := range tests {
		t.Run(conclusion, func(t *testing.T) {
			t.Parallel()

			checks := pullRequestCheckInventory(
				[]restCheckRun{{ID: 1, Name: "Optional", Status: "completed", Conclusion: conclusion}},
				nil,
			)
			if len(checks) != 1 || checks[0].Name != "Optional" || checks[0].Conclusion != conclusion {
				t.Fatalf("checks = %#v, want ignored check name retained", checks)
			}
		})
	}
}

type contextValueCaptureClient struct {
	base   HTTPClient
	key    any
	values chan<- any
}

func (c contextValueCaptureClient) Do(req *http.Request) (*http.Response, error) {
	select {
	case c.values <- req.Context().Value(c.key):
	default:
	}
	return c.base.Do(req)
}

func githubIssueIDs(issues []connector.Issue) []string {
	ids := make([]string, 0, len(issues))
	for _, issue := range issues {
		ids = append(ids, issue.ID)
	}
	return ids
}

func TestProjectFieldsRefreshUsesTargetedQueryOnlyForChangedCard(t *testing.T) {
	t.Parallel()
	server := newGraphQLTestServer(t, []graphqlTestResponse{{body: `{"data":{"node":{"projectItems":{"pageInfo":{"hasNextPage":false},"nodes":[{"id":"PVTI_1","project":{"id":"PVT_1"},"statusValue":{"name":"Done"},"fieldValues":{"nodes":[]}}]}}}}`}})
	c := newGitHubTestConnector(t, server, Config{ProjectSlug: "PVT_1"})
	c.projectCache.ReplaceProjectFields("PVT_1", map[string]projectItemFields{
		"I_1": {itemID: "PVTI_1", statusName: "Todo"},
		"I_2": {itemID: "PVTI_2", statusName: "Todo"},
	}, c.projectCache.Revision("PVT_1"))
	c.projectCache.SetItemID("PVT_1", "I_1", "PVTI_1")
	c.projectCache.InvalidateProjectFields("PVT_1", "I_1")
	for range 2 {
		if err := c.ensureProjectFieldsCached(t.Context(), []string{"I_1", "I_2"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct{ id, status string }{{"I_1", "Done"}, {"I_2", "Todo"}} {
		t.Run(tt.id, func(t *testing.T) {
			fields, present, known := c.projectCache.GetProjectFields("PVT_1", tt.id)
			if !present || !known || fields.statusName != tt.status {
				t.Fatalf("fields = %#v, present %t known %t", fields, present, known)
			}
		})
	}
	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want one targeted refresh", len(requests))
	}
	query, _ := requests[0]["query"].(string)
	if !strings.Contains(query, "DetentGitHubProjectItemForIssue") {
		t.Fatalf("query = %q, want targeted refresh", query)
	}
}

func TestCheckRunFailureEvidence(t *testing.T) {
	for _, tt := range []struct {
		name, annotations, text, want string
	}{
		{name: "first assertion", annotations: `[{"annotation_level":"warning","message":"not a failure"},{"annotation_level":"failure","path":"worker_test.go","message":"want released slot"},{"annotation_level":"failure","message":"later assertion"}]`, want: "worker_test.go: want released slot"},
		{name: "output fallback", annotations: `[]`, text: "Run make check: TestWorker failed", want: "Run make check: TestWorker failed"},
		{name: "no evidence", annotations: `[]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := newGraphQLTestServer(t, []graphqlTestResponse{{method: http.MethodGet, path: "/repos/example/repo/check-runs/1/annotations?per_page=100", body: tt.annotations}})
			c := newGitHubTestConnector(t, server, Config{})
			runs := []restCheckRun{{ID: 1, Name: "test", Status: "completed", Conclusion: "failure", Output: checkRunOutput{Text: tt.text}}}
			if _, err := c.transientCheckRunFailures(t.Context(), pullRequestRepo{Owner: "example", Name: "repo"}, runs); err != nil {
				t.Fatal(err)
			}
			inventory := pullRequestCheckInventory(runs, nil)
			if len(inventory) != 1 || inventory[0].FailureDetail != tt.want {
				t.Fatalf("failure evidence = %#v, want %q", inventory, tt.want)
			}
		})
	}
}
