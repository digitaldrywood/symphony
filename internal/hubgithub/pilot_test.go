package hubgithub

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	connectorgithub "github.com/digitaldrywood/detent/internal/connector/github"
	"github.com/digitaldrywood/detent/internal/hubserver"
)

func TestPilotGitHubImportBudget(t *testing.T) {
	t.Parallel()
	client := &scriptedRESTClient{t: t, steps: []restStep{
		{method: http.MethodGet, path: "/repos/pilot/repo/issues/1/comments?per_page=100", response: json.RawMessage(`[]`), next: "/repos/pilot/repo/issues/1/comments?per_page=100&page=2"},
		{method: http.MethodGet, path: "/repos/pilot/repo/issues/1/comments?per_page=100&page=2", response: json.RawMessage(`[]`)},
	}}
	transport := NewTransport(nil)
	transport.client = client
	request := hubserver.GitHubImportRequest{Profile: "native", Repository: "pilot/repo", IssueNumber: 1, Stage: "comments"}
	for _, wantCursor := range []bool{true, false} {
		page, err := NewImporter(transport).FetchImportPage(t.Context(), request)
		if err != nil || (page.NextCursor != "") != wantCursor {
			t.Fatalf("import page cursor=%q: %v", page.NextCursor, err)
		}
		request.Cursor = page.NextCursor
	}
	client.assertDone()
	counts := transport.Counts()
	if len(counts) != 1 || counts[0].Profile != "native" || counts[0].Operation != "import.issue" || counts[0].Requests != 2 || counts[0].Errors != 0 {
		t.Fatalf("explicit import accounting = %+v", counts)
	}
	t.Log("PILOT explicit_import comment_pages=2 issue_requests=2 steady_state_issue_requests=separate")
}

func TestPilotGitHubRequestBudgets(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	pull := reconcilePullRequest{ID: 1, NodeID: "PR_pilot", Number: 1, Title: "Pilot change", HTMLURL: "https://github.test/pilot/repo/pull/1", State: "open", Head: restRef{Ref: "work", SHA: "head"}, Base: restRef{Ref: "main", SHA: "base"}, CreatedAt: now, UpdatedAt: now}
	for _, test := range []struct {
		name  string
		mode  hubserver.ReconcileMode
		pulls []reconcilePullRequest
		want  map[string]int64
	}{
		{"idle repository", hubserver.ReconcileIncremental, nil, map[string]int64{"reconcile.repository": 1, "reconcile.pull_request": 1}},
		{"active repository", hubserver.ReconcileIncremental, []reconcilePullRequest{pull}, map[string]int64{"reconcile.repository": 1, "reconcile.pull_request": 1}},
		{"full repair", hubserver.ReconcileFullRepair, []reconcilePullRequest{pull}, map[string]int64{"reconcile.repository": 1, "reconcile.pull_request": 3, "reconcile.ci": 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := &scriptedRESTClient{t: t, steps: []restStep{
				{method: http.MethodGet, path: "/repos/pilot/repo", response: reconcileRepository{NodeID: "R_pilot", Name: "repo", Owner: restActor{Login: "pilot"}, UpdatedAt: now}},
				{method: http.MethodGet, path: "/repos/pilot/repo/pulls?direction=asc&per_page=100&sort=updated&state=all", response: test.pulls},
			}}
			if test.mode == hubserver.ReconcileFullRepair {
				client.steps = append(client.steps,
					restStep{method: http.MethodGet, path: "/repos/pilot/repo/pulls/1", response: pull},
					restStep{method: http.MethodGet, path: "/repos/pilot/repo/commits/head/check-runs?per_page=100", response: restCheckRuns{}},
					restStep{method: http.MethodGet, path: "/repos/pilot/repo/commits/head/statuses?per_page=100", response: []restStatus{}},
					restStep{method: http.MethodGet, path: "/repos/pilot/repo/pulls/1/reviews?per_page=100", response: []restReview{}},
				)
			}
			transport := NewTransport(nil)
			transport.client = client
			snapshot, err := NewReconciler(transport).Reconcile(t.Context(), hubserver.ReconcileRequest{Profile: "native", Mode: test.mode, Repository: hubserver.RepositoryTarget{Owner: "pilot", Name: "repo"}})
			if err != nil {
				t.Fatal(err)
			}
			client.assertDone()
			got := make(map[string]int64)
			for _, count := range transport.Counts() {
				if count.Profile != "native" || count.Errors != 0 {
					t.Fatalf("unexpected request accounting: %+v", count)
				}
				got[count.Operation] = count.Requests
			}
			if !reflect.DeepEqual(got, test.want) || len(snapshot.Issues) != 0 || len(snapshot.PullRequests) != len(test.pulls) {
				t.Fatalf("requests=%v want=%v; issues=%d pulls=%d", got, test.want, len(snapshot.Issues), len(snapshot.PullRequests))
			}
			t.Logf("PILOT github scenario=%q requests=%v issue_api=0 git_transport=outside_http_fixture", test.name, got)
		})
	}
}

func TestPilotGitHubBackoffAndOperationBound(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		status int
		retry  time.Duration
		reset  time.Duration
		wait   time.Duration
	}{
		{"primary reset", http.StatusForbidden, time.Second, time.Hour, time.Hour},
		{"secondary retry", http.StatusTooManyRequests, 2 * time.Minute, 0, 2 * time.Minute},
		{"secondary minimum", http.StatusForbidden, 0, 0, time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
			upstream := &connectorgithub.StatusError{StatusCode: test.status, RetryAfter: test.retry, ResetAt: now.Add(test.reset), Err: connectorgithub.ErrRateLimited}
			client := &scriptedRESTClient{t: t, steps: []restStep{
				{method: http.MethodGet, path: "/repos/pilot/repo/pulls", err: upstream},
				{method: http.MethodGet, path: "/repos/pilot/repo/pulls", response: []restPullRequest{}},
			}}
			transport := NewTransport(nil)
			transport.client, transport.now = client, func() time.Time { return now }
			ctx := scopedRequests(t.Context(), "native", "reconcile")
			if err := transport.REST(ctx, http.MethodGet, "/repos/pilot/repo/pulls", nil, nil); !errors.Is(err, upstream) {
				t.Fatalf("initial limit: %v", err)
			}
			now = now.Add(test.wait - time.Nanosecond)
			for range 16 {
				err := transport.REST(scopedRequests(t.Context(), "native", "import"), http.MethodGet, "/repos/pilot/repo/issues", nil, nil)
				var status *connectorgithub.StatusError
				if !errors.As(err, &status) || status.RetryAfter <= 0 {
					t.Fatalf("shared retry escaped backoff: %v", err)
				}
			}
			now = now.Add(time.Nanosecond)
			if err := transport.REST(ctx, http.MethodGet, "/repos/pilot/repo/pulls", nil, nil); err != nil {
				t.Fatal(err)
			}
			client.assertDone()
			counts := transport.Counts()
			if len(counts) != 1 || counts[0].Requests != 2 || counts[0].Errors != 1 {
				t.Fatalf("backoff counted unsent requests: %+v", counts)
			}
			t.Logf("PILOT backoff scenario=%q wait_seconds=%g sent=2 errors=1 suppressed=16", test.name, test.wait.Seconds())
		})
	}
	t.Run("paginated operation bound", func(t *testing.T) {
		t.Parallel()
		client := &scriptedRESTClient{t: t}
		for range 500 {
			client.steps = append(client.steps, restStep{method: http.MethodGet, path: "/repos/pilot/repo/pulls", next: "/repos/pilot/repo/pulls"})
		}
		transport := NewTransport(nil)
		transport.client = client
		_, err := fetchRESTList[restPullRequest](scopedRequests(t.Context(), "native", "reconcile"), transport, "/repos/pilot/repo/pulls")
		if err == nil {
			t.Fatal("unbounded pagination succeeded")
		}
		client.assertDone()
		counts := transport.Counts()
		if len(counts) != 1 || counts[0].Requests != 500 {
			t.Fatalf("operation bound = %+v", counts)
		}
		t.Log("PILOT pagination sent=500 next_request=blocked checkpoint=not_successful")
	})
}
