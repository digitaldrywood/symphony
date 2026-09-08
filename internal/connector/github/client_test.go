package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
)

func TestClientGraphQLSendsBearerRequest(t *testing.T) {
	t.Parallel()

	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want bearer token", got)
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Fatalf("Accept = %q, want GitHub JSON media type", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if got := r.Header.Get("X-GitHub-Api-Version"); got != "2026-03-10" {
			t.Fatalf("X-GitHub-Api-Version = %q, want 2026-03-10", got)
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- payload

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"}}}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var got struct {
		Viewer struct {
			Login string `json:"login"`
		} `json:"viewer"`
	}
	err = client.GraphQL(context.Background(), "query Viewer($id: ID!) { viewer { login } }", map[string]any{"id": "PVT_1"}, &got)
	if err != nil {
		t.Fatalf("GraphQL() error = %v", err)
	}
	if got.Viewer.Login != "octocat" {
		t.Fatalf("viewer.login = %q, want octocat", got.Viewer.Login)
	}

	payload := <-requests
	if payload["query"] == "" {
		t.Fatal("query payload is blank")
	}
	variables := payload["variables"].(map[string]any)
	if variables["id"] != "PVT_1" {
		t.Fatalf("variables.id = %v, want PVT_1", variables["id"])
	}
}

func TestClientGraphQLStopsLookupsAfterRateLimitResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		statusCode        int
		headers           http.Header
		body              string
		reserve           int64
		followupQueryType string
	}{
		{
			name:       "rate limit response",
			statusCode: http.StatusTooManyRequests,
			headers:    http.Header{"Retry-After": []string{"30"}},
			body:       `{"message":"API rate limit already exceeded"}`,
		},
		{
			name:       "forbidden exhausted",
			statusCode: http.StatusForbidden,
			headers: http.Header{
				"X-RateLimit-Limit":     []string{"5000"},
				"X-RateLimit-Remaining": []string{"0"},
				"X-RateLimit-Reset":     []string{"2082758400"},
			},
			body: `{"message":"API rate limit already exceeded"}`,
		},
		{
			name:       "header remaining below reserve",
			statusCode: http.StatusOK,
			headers: http.Header{
				"X-RateLimit-Limit":     []string{"5000"},
				"X-RateLimit-Remaining": []string{"1000"},
				"X-RateLimit-Used":      []string{"4000"},
				"X-RateLimit-Reset":     []string{"2082758400"},
			},
			body:    `{"data":{"viewer":{"login":"octocat"}}}`,
			reserve: 1000,
		},
		{
			name:              "merge queue lookup",
			statusCode:        http.StatusTooManyRequests,
			headers:           http.Header{"Retry-After": []string{"30"}},
			body:              `{"message":"API rate limit already exceeded"}`,
			followupQueryType: graphQLQueryMergeQueue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				for key, values := range tt.headers {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)

			client, err := NewClient(ClientConfig{
				Endpoint:                   server.URL,
				TokenSource:                StaticTokenSource("test-token"),
				HTTPClient:                 server.Client(),
				GraphQLMinRemainingReserve: tt.reserve,
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			for call := range 2 {
				queryType := ""
				if call > 0 {
					queryType = tt.followupQueryType
				}
				err = client.GraphQLWithType(context.Background(), queryType, "query { viewer { login } rateLimit { limit used remaining cost resetAt } }", nil, nil)
				if call == 0 && tt.statusCode == http.StatusOK {
					if err != nil {
						t.Fatalf("first GraphQL() error = %v", err)
					}
					continue
				}
				if !errors.Is(err, ErrRateLimited) {
					t.Fatalf("GraphQL() call %d error = %v, want ErrRateLimited", call+1, err)
				}
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("GraphQL HTTP calls = %d, want 1 after rate limit signal", got)
			}
		})
	}
}

func TestClientStopsLookupsAfterHeaderlessForbiddenRateLimitResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(context.Context, *Client) error
	}{
		{
			name: "graphql",
			call: func(ctx context.Context, client *Client) error {
				return client.GraphQL(ctx, "query { viewer { login } }", nil, nil)
			},
		},
		{
			name: "rest",
			call: func(ctx context.Context, client *Client) error {
				return client.REST(ctx, http.MethodGet, "/repos/digitaldrywood/detent/issues/1/comments", nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"API rate limit already exceeded"}`))
			}))
			t.Cleanup(server.Close)

			client, err := NewClient(ClientConfig{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient:  server.Client(),
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}
			client.restBackoffs = newRESTBackoffRegistry()

			for call := range 2 {
				err := tt.call(context.Background(), client)
				if !errors.Is(err, ErrRateLimited) {
					t.Fatalf("call %d error = %v, want ErrRateLimited", call+1, err)
				}
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("HTTP calls = %d, want 1 after rate limit signal", got)
			}
		})
	}
}

func TestConnectorRateLimitProbesRequireFreshResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seed      func(context.Context, *Connector) error
		probe     func(context.Context, *Connector) error
		backedOff func(*Connector) bool
	}{
		{
			name: "graphql",
			seed: func(ctx context.Context, conn *Connector) error {
				return conn.client.GraphQL(ctx, "query { viewer { login } }", nil, nil)
			},
			probe: func(ctx context.Context, conn *Connector) error {
				_, err := conn.ProbeGraphQLRateLimit(ctx)
				return err
			},
			backedOff: func(conn *Connector) bool {
				return conn.GraphQLRateLimitStatus() == connector.GraphQLRateLimitStatusBackoff
			},
		},
		{
			name: "rest",
			seed: func(ctx context.Context, conn *Connector) error {
				return conn.client.REST(ctx, http.MethodGet, "/user", nil, nil)
			},
			probe: func(ctx context.Context, conn *Connector) error {
				_, err := conn.ProbeRESTRateLimit(ctx, 1000)
				return err
			},
			backedOff: func(conn *Connector) bool {
				return conn.RESTRateLimitStatus().RateLimited
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "60")
					w.Header().Set("X-RateLimit-Limit", "5000")
					w.Header().Set("X-RateLimit-Remaining", "4000")
					w.Header().Set("X-RateLimit-Used", "1000")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"message":"API rate limit already exceeded"}`))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(server.Close)

			conn, err := NewConnector(Config{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient:  server.Client(),
			})
			if err != nil {
				t.Fatalf("NewConnector() error = %v", err)
			}
			conn.client.restBackoffs = newRESTBackoffRegistry()
			if err := tt.seed(context.Background(), conn); !errors.Is(err, ErrRateLimited) {
				t.Fatalf("seed error = %v, want ErrRateLimited", err)
			}
			if err := tt.probe(context.Background(), conn); !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("probe error = %v, want ErrInvalidResponse", err)
			}
			if !tt.backedOff(conn) {
				t.Fatal("rate limit backoff cleared without a fresh quota response")
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("HTTP calls = %d, want seed and probe", got)
			}
		})
	}
}

func TestClientGraphQLSuccessfulMutationClearsRateLimitResponse(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"API rate limit already exceeded"}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"item-1"}},"viewer":{"login":"octocat"}}}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if err := client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("first GraphQL() error = %v, want ErrRateLimited", err)
	}
	if err := client.GraphQLWithType(context.Background(), graphQLQueryUpdateField, "mutation { updateProjectV2ItemFieldValue { projectV2Item { id } } }", nil, nil); err != nil {
		t.Fatalf("mutation GraphQL() error = %v", err)
	}
	if err := client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil); err != nil {
		t.Fatalf("GraphQL() after successful mutation error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("GraphQL HTTP calls = %d, want rate limit, mutation, recovered lookup", got)
	}
}

func TestConnectorProbeRESTRateLimitClearsRecoveredBackoff(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"API rate limit already exceeded"}`))
			return
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "2000")
		w.Header().Set("X-RateLimit-Used", "3000")
		w.Header().Set("X-RateLimit-Reset", "2082758400")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)

	conn, err := NewConnector(Config{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	conn.client.restBackoffs = newRESTBackoffRegistry()
	if err := conn.client.REST(context.Background(), http.MethodGet, "/user", nil, nil); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("REST() error = %v, want ErrRateLimited", err)
	}
	rateLimit, err := conn.ProbeRESTRateLimit(context.Background(), 1000)
	if err != nil {
		t.Fatalf("ProbeRESTRateLimit() error = %v", err)
	}
	if rateLimit.Remaining != 2000 {
		t.Fatalf("ProbeRESTRateLimit().Remaining = %d, want 2000", rateLimit.Remaining)
	}
	if err := conn.client.REST(context.Background(), http.MethodGet, "/user", nil, nil); err != nil {
		t.Fatalf("REST() after recovery error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("HTTP calls = %d, want rate limit, probe, recovered request", got)
	}
}

func TestClientTrackerAvailabilityClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		queryType        string
		query            string
		status           int
		transportErr     error
		wantAvailability bool
		wantClass        string
	}{
		{name: "server error", query: "query DetentCandidates { viewer { login } }", status: http.StatusServiceUnavailable, wantAvailability: true, wantClass: connector.TrackerAvailabilityClassServer},
		{name: "timeout", query: "query DetentCandidates { viewer { login } }", transportErr: context.DeadlineExceeded, wantAvailability: true, wantClass: connector.TrackerAvailabilityClassTimeout},
		{name: "dns", query: "query DetentCandidates { viewer { login } }", transportErr: &net.DNSError{Err: "no such host", Name: "api.github.test"}, wantAvailability: true, wantClass: connector.TrackerAvailabilityClassTransport},
		{name: "transport", query: "query DetentCandidates { viewer { login } }", transportErr: errors.New("tls handshake failed"), wantAvailability: true, wantClass: connector.TrackerAvailabilityClassTransport},
		{name: "tracker status pull request references", queryType: graphQLQueryPullRequests, query: "query DetentPullRequestReferences { viewer { login } }", status: http.StatusServiceUnavailable, wantAvailability: true, wantClass: connector.TrackerAvailabilityClassServer},
		{name: "rate limit", query: "query DetentCandidates { viewer { login } }", status: http.StatusTooManyRequests},
		{name: "forbidden", query: "query DetentCandidates { viewer { login } }", status: http.StatusForbidden},
		{name: "not found", query: "query DetentCandidates { viewer { login } }", status: http.StatusNotFound},
		{name: "mutation server error", query: "mutation DetentMove { updateIssue(input: {}) { id } }", status: http.StatusServiceUnavailable},
		{name: "merge queue server error", queryType: graphQLQueryMergeQueue, query: "query DetentMergeQueue { viewer { login } }", status: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, err := NewClient(ClientConfig{
				Endpoint:    "https://api.github.test/graphql",
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient: staticHTTPClient{do: func(req *http.Request) (*http.Response, error) {
					if tt.transportErr != nil {
						return nil, tt.transportErr
					}
					return jsonResponse(req, tt.status, `{"message":"unavailable"}`, nil), nil
				}},
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			queryType := tt.queryType
			if queryType == "" {
				queryType = graphQLQueryCandidateIssues
			}
			err = client.GraphQLWithType(context.Background(), queryType, tt.query, nil, nil)
			availabilityErr, gotAvailability := connector.AsTrackerAvailability(err)
			if gotAvailability != tt.wantAvailability {
				t.Fatalf("tracker availability = %v, want %v; error = %v", gotAvailability, tt.wantAvailability, err)
			}
			if gotAvailability && availabilityErr.Class != tt.wantClass {
				t.Fatalf("class = %q, want %q", availabilityErr.Class, tt.wantClass)
			}
		})
	}
}

func TestClientReportsResponseProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{
			name: "GraphQL",
			run: func(ctx context.Context, client *Client) error {
				return client.GraphQL(ctx, "query { viewer { login } }", nil, nil)
			},
		},
		{
			name: "REST",
			run: func(ctx context.Context, client *Client) error {
				return client.REST(ctx, http.MethodGet, "/user", nil, nil)
			},
		},
		{
			name: "REST probe",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.restProbe(ctx, http.MethodGet, "/user", nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/" {
					_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"}}}`))
					return
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(server.Close)

			client, err := NewClient(ClientConfig{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient:  server.Client(),
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			calls := 0
			ctx := connector.WithProgressReporter(context.Background(), func() {
				calls++
			})
			if err := tt.run(ctx, client); err != nil {
				t.Fatalf("request error = %v", err)
			}
			if calls != 1 {
				t.Fatalf("progress reports = %d, want 1", calls)
			}
		})
	}
}

func TestClientGraphQLClassifiesFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		headers    map[string]string
		body       string
		want       error
	}{
		{
			name:       "unauthorized",
			statusCode: http.StatusUnauthorized,
			body:       `{"message":"bad credentials"}`,
			want:       ErrAuthenticationFailed,
		},
		{
			name:       "forbidden rate limit",
			statusCode: http.StatusForbidden,
			headers:    map[string]string{"X-RateLimit-Remaining": "0"},
			body:       `{"message":"rate limit"}`,
			want:       ErrRateLimited,
		},
		{
			name:       "secondary rate limit",
			statusCode: http.StatusForbidden,
			headers:    map[string]string{"Retry-After": "120"},
			body:       `{"message":"secondary rate limit"}`,
			want:       ErrRateLimited,
		},
		{
			name:       "issue comment cap",
			statusCode: http.StatusForbidden,
			body:       `{"message":"Commenting is disabled on issues with more than 2500 comments"}`,
			want:       ErrResourceExhausted,
		},
		{
			name:       "not found",
			statusCode: http.StatusNotFound,
			body:       `{"message":"not found"}`,
			want:       ErrNotFound,
		},
		{
			name:       "server error",
			statusCode: http.StatusBadGateway,
			body:       `{"message":"bad gateway"}`,
			want:       ErrTransient,
		},
		{
			name:       "graphql rate limit",
			statusCode: http.StatusOK,
			body:       `{"errors":[{"type":"RATE_LIMITED","message":"slow down"}]}`,
			want:       ErrRateLimited,
		},
		{
			name:       "graphql generic",
			statusCode: http.StatusOK,
			body:       `{"errors":[{"message":"field error"}]}`,
			want:       ErrGraphQLErrors,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, value := range tt.headers {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)

			client, err := NewClient(ClientConfig{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient:  server.Client(),
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			err = client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil)
			if !errors.Is(err, tt.want) {
				t.Fatalf("GraphQL() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestClassifyStatusMapsCommentCapToResourceExhaustion(t *testing.T) {
	t.Parallel()

	err := classifyStatus(
		http.StatusForbidden,
		nil,
		[]byte(`{"message":"Commenting is disabled on issues with more than 2500 comments"}`),
	)

	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("classifyStatus() error = %v, want ErrResourceExhausted", err)
	}
	if errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("classifyStatus() error = %v, do not want ErrAuthenticationFailed", err)
	}
	if !strings.Contains(err.Error(), "github resource exhausted") {
		t.Fatalf("classifyStatus() error = %v, want resource exhaustion message", err)
	}
}

func TestClientGraphQLRefreshesTokenAfterAuthFailure(t *testing.T) {
	t.Parallel()

	source := newRefreshingTokenTestSource("stale-token", "fresh-token")
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		requests <- token
		switch token {
		case "Bearer stale-token":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
		case "Bearer fresh-token":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"}}}`))
		default:
			t.Fatalf("Authorization = %q, want stale then fresh token", token)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: source,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var got struct {
		Viewer struct {
			Login string `json:"login"`
		} `json:"viewer"`
	}
	if err := client.GraphQL(context.Background(), "query { viewer { login } }", nil, &got); err != nil {
		t.Fatalf("GraphQL() error = %v", err)
	}
	if got.Viewer.Login != "octocat" {
		t.Fatalf("Viewer.Login = %q, want octocat", got.Viewer.Login)
	}
	if source.refreshes.Load() != 1 {
		t.Fatalf("RefreshToken() calls = %d, want 1", source.refreshes.Load())
	}
	if first, second := <-requests, <-requests; first != "Bearer stale-token" || second != "Bearer fresh-token" {
		t.Fatalf("Authorization sequence = %q, %q; want stale then fresh", first, second)
	}
	health, ok := client.AuthHealth()
	if !ok {
		t.Fatal("AuthHealth() ok = false, want true")
	}
	if health.Status != connector.AuthStatusRecovered {
		t.Fatalf("AuthHealth().Status = %q, want %q", health.Status, connector.AuthStatusRecovered)
	}
	if health.LastError == "" || health.LastErrorAt.IsZero() || health.LastRecoveredAt.IsZero() {
		t.Fatalf("AuthHealth() missing recovery detail: %#v", health)
	}
}

func TestClientRESTRefreshesTokenAfterAuthFailure(t *testing.T) {
	t.Parallel()

	source := newRefreshingTokenTestSource("stale-token", "fresh-token")
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		requests <- token
		switch token {
		case "Bearer stale-token":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
		case "Bearer fresh-token":
			if r.URL.Path != "/repos/digitaldrywood/detent/issues" {
				t.Fatalf("path = %s, want REST request path", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("Authorization = %q, want stale then fresh token", token)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: source,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var got struct {
		OK bool `json:"ok"`
	}
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues", nil, &got); err != nil {
		t.Fatalf("REST() error = %v", err)
	}
	if !got.OK {
		t.Fatal("REST() response OK = false, want true")
	}
	if source.refreshes.Load() != 1 {
		t.Fatalf("RefreshToken() calls = %d, want 1", source.refreshes.Load())
	}
	if first, second := <-requests, <-requests; first != "Bearer stale-token" || second != "Bearer fresh-token" {
		t.Fatalf("Authorization sequence = %q, %q; want stale then fresh", first, second)
	}
}

func TestClientRESTDebugLogsEndpointPurposeWithoutSecrets(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	httpClient := staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/repos/digitaldrywood/detent/commits/abc/check-runs" {
			t.Fatalf("path = %s, want check-runs path", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer super-secret-token" {
			t.Fatalf("Authorization = %q, want bearer token", got)
		}
		return jsonResponse(r, http.StatusOK, `{"check_runs":[],"body_secret":"do-not-log-response-body"}`, nil), nil
	}}

	client, err := NewClient(ClientConfig{
		Endpoint:         "https://api.github.test/graphql",
		TokenSource:      StaticTokenSource("super-secret-token"),
		HTTPClient:       httpClient,
		RESTDebugLogging: true,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/check-runs", nil, nil); err != nil {
		t.Fatalf("REST() error = %v", err)
	}

	logText := logs.String()
	for _, fragment := range []string{
		"github rest request",
		"github rest response",
		`endpoint_family="check runs"`,
		"request_purpose=hydrate_pull_request_checks",
		"body_present=false",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logText)
		}
	}
	for _, leaked := range []string{"super-secret-token", "do-not-log-response-body"} {
		if strings.Contains(logText, leaked) {
			t.Fatalf("logs leaked %q:\n%s", leaked, logText)
		}
	}
}

func TestClientRESTDebugLoggingOffByDefault(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	httpClient := staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/repos/digitaldrywood/detent/commits/abc/check-runs" {
			t.Fatalf("path = %s, want check-runs path", r.URL.Path)
		}
		return jsonResponse(r, http.StatusOK, `{"check_runs":[]}`, nil), nil
	}}
	client, err := NewClient(ClientConfig{
		Endpoint:    "https://api.github-debug-off.test/graphql",
		TokenSource: StaticTokenSource("debug-off-token"),
		HTTPClient:  httpClient,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/check-runs", nil, nil); err != nil {
		t.Fatalf("REST() error = %v", err)
	}

	logText := logs.String()
	for _, fragment := range []string{"github rest request", "github rest response"} {
		if strings.Contains(logText, fragment) {
			t.Fatalf("logs contained %q with RESTDebugLogging=false:\n%s", fragment, logText)
		}
	}
}

func TestClientRESTLogsRateLimitFailuresByDefault(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	httpClient := staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
		headers := http.Header{
			"X-Ratelimit-Limit":     []string{"5000"},
			"X-Ratelimit-Remaining": []string{"0"},
			"X-Ratelimit-Reset":     []string{strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10)},
		}
		return jsonResponse(r, http.StatusForbidden, `{"message":"API rate limit exceeded"}`, headers), nil
	}}
	client, err := NewClient(ClientConfig{
		Endpoint:    "https://api.github-rate-limit.test/graphql",
		TokenSource: StaticTokenSource("rate-limit-token"),
		HTTPClient:  httpClient,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.restBackoffs = newRESTBackoffRegistry()

	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("REST() error = %v, want %v", err, ErrRateLimited)
	}
	if _, ok := connector.AsTrackerAvailability(err); ok {
		t.Fatalf("REST() error = %v, do not want tracker availability for 403", err)
	}

	logText := logs.String()
	for _, fragment := range []string{
		"github rest shared backoff recorded",
		"github rest request failed",
		"rate_limited=true",
		"status=403",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logText)
		}
	}
	for _, fragment := range []string{`msg="github rest request"`, `msg="github rest response"`} {
		if strings.Contains(logText, fragment) {
			t.Fatalf("logs contained per-request debug %q:\n%s", fragment, logText)
		}
	}
}

func TestClientRESTDoesNotWarnOnNotFoundByDefault(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	httpClient := staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, http.StatusNotFound, `{"message":"Not Found"}`, nil), nil
	}}
	client, err := NewClient(ClientConfig{
		Endpoint:    "https://api.github-not-found.test/graphql",
		TokenSource: StaticTokenSource("not-found-token"),
		HTTPClient:  httpClient,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues/404", nil, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("REST() error = %v, want %v", err, ErrNotFound)
	}

	logText := logs.String()
	if strings.Contains(logText, "github rest request failed") {
		t.Fatalf("logs contained default not-found warning:\n%s", logText)
	}
}

func TestClientRESTLogsUnexpectedFailuresByDefault(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	httpClient := staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, http.StatusInternalServerError, `{"message":"server error"}`, nil), nil
	}}
	client, err := NewClient(ClientConfig{
		Endpoint:    "https://api.github-unexpected.test/graphql",
		TokenSource: StaticTokenSource("unexpected-token"),
		HTTPClient:  httpClient,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues", nil, nil)
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("REST() error = %v, want %v", err, ErrTransient)
	}
	availabilityErr, ok := connector.AsTrackerAvailability(err)
	if !ok || availabilityErr.Class != connector.TrackerAvailabilityClassServer {
		t.Fatalf("REST() availability = %#v, want server class", availabilityErr)
	}

	logText := logs.String()
	for _, fragment := range []string{
		"github rest request failed",
		"rate_limited=false",
		"status=500",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logText)
		}
	}
}

func TestClientRESTAggregatesUsageAndBacksOffAfterRateLimit(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch call := calls.Add(1); call {
		case 1:
			if r.URL.Path != "/repos/digitaldrywood/detent/issues" {
				t.Fatalf("first path = %s, want label issues path", r.URL.Path)
			}
			if got := r.URL.Query().Get("labels"); got != "detent:todo" {
				t.Fatalf("labels query = %q, want detent:todo", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Used", "121")
			w.Header().Set("X-RateLimit-Remaining", "4879")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
			w.Header().Set("X-RateLimit-Resource", "core")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		case 2:
			if r.URL.Path != "/repos/digitaldrywood/detent/issues/666/comments" {
				t.Fatalf("second path = %s, want issue comments path", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "120")
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Used", "5000")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
			w.Header().Set("X-RateLimit-Resource", "core")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
		default:
			t.Fatalf("unexpected REST call %d to %s", call, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.restBackoffs = newRESTBackoffRegistry()

	var issues []restIssue
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues?labels=detent%3Atodo", nil, &issues); err != nil {
		t.Fatalf("REST() label issues error = %v", err)
	}
	var comments []restComment
	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues/666/comments", nil, &comments)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("REST() comments error = %v, want ErrRateLimited", err)
	}
	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/check-runs", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("REST() during backoff error = %v, want ErrRateLimited", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("REST calls = %d, want 2 before local backoff", calls.Load())
	}
	status := client.RESTRateLimitStatus()
	if !status.RateLimited || status.RateLimit.Remaining != 0 || status.BackoffUntil.IsZero() {
		t.Fatalf("RESTRateLimitStatus() = %#v, want active exhausted backoff", status)
	}

	usage := client.FlushRESTRateLimitUsage()
	if !usage.HasRateLimit {
		t.Fatal("FlushRESTRateLimitUsage().HasRateLimit = false, want true")
	}
	if usage.TotalRequests != 2 || !usage.RateLimited {
		t.Fatalf("FlushRESTRateLimitUsage() totals = requests %d rate_limited %v, want 2 true", usage.TotalRequests, usage.RateLimited)
	}
	if usage.RateLimit.Remaining != 0 || usage.RateLimit.RetryAfter != 2*time.Minute || usage.RateLimit.Resource != "core" {
		t.Fatalf("FlushRESTRateLimitUsage().RateLimit = %#v, want remaining 0 retry-after 2m core", usage.RateLimit)
	}
	if usage.BackoffUntil.IsZero() {
		t.Fatal("FlushRESTRateLimitUsage().BackoffUntil is zero, want backoff deadline")
	}
	if got := restEndpointUsageCount(usage.Requests, "label issues"); got != 1 {
		t.Fatalf("label issues usage count = %d, want 1; usage = %#v", got, usage.Requests)
	}
	if got := restEndpointUsageCount(usage.Requests, "issue comments"); got != 1 {
		t.Fatalf("issue comments usage count = %d, want 1; usage = %#v", got, usage.Requests)
	}
}

func TestClientRESTDoesNotGloballyBackOffAfterSecondaryFanoutThrottle(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch call := calls.Add(1); call {
		case 1:
			if r.URL.Path != "/repos/digitaldrywood/detent/commits/abc/check-runs" {
				t.Fatalf("first path = %s, want check-runs path", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "120")
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Used", "122")
			w.Header().Set("X-RateLimit-Remaining", "4878")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
			w.Header().Set("X-RateLimit-Resource", "core")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
		case 2:
			if r.URL.Path != "/repos/digitaldrywood/detent/issues" {
				t.Fatalf("second path = %s, want label issues path", r.URL.Path)
			}
			if got := r.URL.Query().Get("labels"); got != "detent:todo" {
				t.Fatalf("labels query = %q, want detent:todo", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Used", "123")
			w.Header().Set("X-RateLimit-Remaining", "4877")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
			w.Header().Set("X-RateLimit-Resource", "core")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected REST call %d to %s", call, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/check-runs", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("REST() check-runs error = %v, want ErrRateLimited", err)
	}
	var issues []restIssue
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues?labels=detent%3Atodo", nil, &issues); err != nil {
		t.Fatalf("REST() label issues after secondary throttle error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("REST calls = %d, want secondary throttle not to block label issues", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.RateLimit.RetryAfter != 0 {
		t.Fatalf("RateLimit.RetryAfter = %s, want no global REST retry-after", usage.RateLimit.RetryAfter)
	}
	if !usage.BackoffUntil.IsZero() {
		t.Fatalf("BackoffUntil = %v, want no global REST backoff", usage.BackoffUntil)
	}
	checkRuns := restEndpointUsage(usage.Requests, "check runs")
	if !checkRuns.RateLimited || checkRuns.RetryAfter != 2*time.Minute {
		t.Fatalf("check runs usage = %#v, want endpoint retry-after", checkRuns)
	}
}

func TestClientRESTStopsFanoutAtRequestCap(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	var logs bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Fatalf("unexpected REST call to %s", r.URL.Path)
		}
		if r.URL.Path != "/repos/digitaldrywood/detent/pulls" {
			t.Fatalf("path = %s, want pull requests path", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "100")
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{FanoutMaxRequests: 1},
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var pulls []restPullRequest
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/pulls?state=all", nil, &pulls); err != nil {
		t.Fatalf("REST() pull requests error = %v", err)
	}
	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/check-runs", nil, nil)
	if !errors.Is(err, ErrRESTFanoutDeferred) {
		t.Fatalf("REST() capped fanout error = %v, want ErrRESTFanoutDeferred", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("REST calls = %d, want only first request sent", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.TotalRequests != 2 || usage.RateLimited || !usage.FanoutDeferred || usage.ReserveHeld {
		t.Fatalf("FlushRESTRateLimitUsage() = %#v, want local fanout deferral only", usage)
	}
	if got := restEndpointUsageCount(usage.Requests, "pull requests"); got != 1 {
		t.Fatalf("pull requests usage count = %d, want 1; usage = %#v", got, usage.Requests)
	}
	if got := restEndpointUsageCount(usage.Requests, "check runs"); got != 1 {
		t.Fatalf("check runs usage count = %d, want throttled synthetic request; usage = %#v", got, usage.Requests)
	}
	for _, want := range []string{`msg="github rest fanout deferred"`, "gate_branch=fanout_cap", "budget_scope=refresh", "fanout_count=1", "snapshot_age="} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("throttle log missing %q:\n%s", want, logs.String())
		}
	}
}

func TestClientRESTFanoutBudgetsIsolateProjectOperations(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "390")
		w.Header().Set("X-RateLimit-Remaining", "4610")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{FanoutMaxRequests: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	hydration := connector.WithRESTFanoutBudget(context.Background(), "fastestbanners_dependency_hydration")
	if err := client.REST(hydration, http.MethodGet, "/repos/fastestbanners/app/issues/1/dependencies/blocked_by", nil, nil); err != nil {
		t.Fatalf("dependency hydration error = %v", err)
	}
	err = client.REST(hydration, http.MethodGet, "/repos/fastestbanners/app/issues/2/dependencies/blocked_by", nil, nil)
	if !errors.Is(err, ErrRESTFanoutDeferred) {
		t.Fatalf("dependency hydration cap error = %v, want ErrRESTFanoutDeferred", err)
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		t.Fatalf("dependency hydration cap error = %#v, want no HTTP status", statusErr)
	}
	deferral, ok := connector.ErrorLocalDeferral(err)
	if !ok || deferral.Reason != "github_rest_fanout_cap" || deferral.Scope != "fastestbanners_dependency_hydration" || deferral.RetryAfter != restFanoutDeferralRetry {
		t.Fatalf("ErrorLocalDeferral() = %#v, %t", deferral, ok)
	}

	admission := connector.WithRESTFanoutBudget(context.Background(), "leadpipe_backlog_admission")
	if err := client.REST(admission, http.MethodGet, "/repos/leadpipe/app/issues?state=open", nil, nil); err != nil {
		t.Fatalf("independently budgeted admission error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("outbound REST calls = %d, want hydration plus admission", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.RateLimited || usage.ReserveHeld || !usage.FanoutDeferred || usage.RateLimit.Remaining != 4610 {
		t.Fatalf("FlushRESTRateLimitUsage() = %#v, want healthy provider plus local deferral", usage)
	}
}

func TestSortedRESTEndpointUsagesUsesBudgetTieBreakers(t *testing.T) {
	tests := []struct {
		name   string
		usages map[string]connector.RESTEndpointUsage
		want   string
	}{
		{
			name: "budget scope",
			usages: map[string]connector.RESTEndpointUsage{
				"second": {CredentialIdentity: "credential", EndpointFamily: "issues", BudgetScope: "refresh"},
				"first":  {CredentialIdentity: "credential", EndpointFamily: "issues", BudgetScope: "admission"},
			},
			want: "admission/|refresh/",
		},
		{
			name: "budget gate",
			usages: map[string]connector.RESTEndpointUsage{
				"second": {CredentialIdentity: "credential", EndpointFamily: "issues", BudgetScope: "admission", BudgetGate: "reserve"},
				"first":  {CredentialIdentity: "credential", EndpointFamily: "issues", BudgetScope: "admission", BudgetGate: "fanout_cap"},
			},
			want: "admission/fanout_cap|admission/reserve",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sortedRESTEndpointUsages(tt.usages)
			keys := make([]string, 0, len(got))
			for _, usage := range got {
				keys = append(keys, usage.BudgetScope+"/"+usage.BudgetGate)
			}
			if joined := strings.Join(keys, "|"); joined != tt.want {
				t.Fatalf("sorted budget keys = %q, want %q", joined, tt.want)
			}
		})
	}
}

func TestClientRESTCountsRepositoryIssuesAsFanout(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Fatalf("unexpected REST call to %s", r.URL.Path)
		}
		if r.URL.Path != "/repos/digitaldrywood/detent/issues" {
			t.Fatalf("path = %s, want repository issues path", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "100")
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{FanoutMaxRequests: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var issues []restIssue
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues?state=open", nil, &issues); err != nil {
		t.Fatalf("REST() repository issues error = %v", err)
	}
	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/pulls?state=all", nil, nil)
	if !errors.Is(err, ErrRESTFanoutDeferred) {
		t.Fatalf("REST() capped fanout error = %v, want ErrRESTFanoutDeferred", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("REST calls = %d, want only repository issues request sent", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if got := restEndpointUsageCount(usage.Requests, "repository issues"); got != 1 {
		t.Fatalf("repository issues usage count = %d, want 1; usage = %#v", got, usage.Requests)
	}
	if got := restEndpointUsageCount(usage.Requests, "pull requests"); got != 1 {
		t.Fatalf("pull requests usage count = %d, want throttled synthetic request; usage = %#v", got, usage.Requests)
	}
}

func TestRESTFanoutEndpointFamilyIncludesBulkHydrationReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantFamily string
	}{
		{name: "issue read", path: "/repos/digitaldrywood/detent/issues/1384", wantFamily: "issue reads"},
		{name: "issue comments", path: "/repos/digitaldrywood/detent/issues/1384/comments?per_page=100", wantFamily: "issue comments"},
		{name: "issue dependencies", path: "/repos/digitaldrywood/detent/issues/1384/dependencies/blocked_by?per_page=100", wantFamily: "issue dependencies"},
		{name: "issue field values", path: "/repos/digitaldrywood/detent/issues/1384/issue-field-values?per_page=100", wantFamily: "issue field values"},
		{name: "workflow run", path: "/repos/digitaldrywood/detent/actions/runs/8001", wantFamily: "workflow runs"},
		{name: "check run annotations", path: "/repos/digitaldrywood/detent/check-runs/9001/annotations?per_page=100", wantFamily: "check run annotations"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			family := restEndpointFamily(http.MethodGet, test.path)
			if family != test.wantFamily {
				t.Fatalf("restEndpointFamily() = %q, want %q", family, test.wantFamily)
			}
			if !restFanoutEndpointFamily(family) {
				t.Fatalf("restFanoutEndpointFamily(%q) = false, want true", family)
			}
		})
	}
}

func TestClientRESTStopsFanoutBelowReserve(t *testing.T) {
	t.Parallel()

	resetAt := time.Now().UTC().Add(time.Hour)
	var calls atomic.Int64
	var logs bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Fatalf("unexpected REST call to %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "4100")
		w.Header().Set("X-RateLimit-Remaining", "900")
		w.Header().Set("X-RateLimit-Resource", "core")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{MinRemainingReserve: 1000},
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var pulls []restPullRequest
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/pulls?state=all", nil, &pulls); err != nil {
		t.Fatalf("REST() pull requests error = %v", err)
	}
	err = client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/statuses", nil, nil)
	if !errors.Is(err, ErrRESTBudgetReserved) {
		t.Fatalf("REST() reserve fanout error = %v, want ErrRESTBudgetReserved", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("REST calls = %d, want reserve to stop second request", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.RateLimit.Remaining != 900 {
		t.Fatalf("RateLimit.Remaining = %d, want 900", usage.RateLimit.Remaining)
	}
	if got := restEndpointUsageCount(usage.Requests, "commit statuses"); got != 1 {
		t.Fatalf("commit statuses usage count = %d, want throttled synthetic request; usage = %#v", got, usage.Requests)
	}
	for _, want := range []string{"gate_branch=reserve", "fanout_count=1", "snapshot_age="} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("throttle log missing %q:\n%s", want, logs.String())
		}
	}
}

func TestClientRESTAllowsFanoutAfterReserveSnapshotReset(t *testing.T) {
	t.Parallel()

	resetAt := time.Now().UTC().Add(-time.Minute)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Resource", "core")
		if call == 1 {
			w.Header().Set("X-RateLimit-Used", "4100")
			w.Header().Set("X-RateLimit-Remaining", "900")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		} else {
			w.Header().Set("X-RateLimit-Used", "100")
			w.Header().Set("X-RateLimit-Remaining", "4900")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Add(time.Hour).Unix(), 10))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{MinRemainingReserve: 1000},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var pulls []restPullRequest
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/pulls?state=all", nil, &pulls); err != nil {
		t.Fatalf("REST() pull requests error = %v", err)
	}
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/commits/abc/statuses", nil, nil); err != nil {
		t.Fatalf("REST() after reset error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("REST calls = %d, want post-reset request sent", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.RateLimited || usage.RateLimit.Remaining != 4900 {
		t.Fatalf("REST usage = %#v, want refreshed budget with 4900 remaining", usage)
	}
}

func TestClientRESTAllowsCoreFanoutAfterSearchPoolBelowReserve(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch call := calls.Add(1); call {
		case 1:
			if r.URL.Path != "/search/issues" {
				t.Fatalf("first path = %s, want search issues path", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-RateLimit-Limit", "30")
			w.Header().Set("X-RateLimit-Used", "2")
			w.Header().Set("X-RateLimit-Remaining", "28")
			w.Header().Set("X-RateLimit-Resource", "search")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"total_count":0,"incomplete_results":false,"items":[]}`))
		case 2:
			if r.URL.Path != "/repos/digitaldrywood/detent/pulls" {
				t.Fatalf("second path = %s, want pull requests path", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Used", "19")
			w.Header().Set("X-RateLimit-Remaining", "4981")
			w.Header().Set("X-RateLimit-Resource", "core")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected REST call %d to %s", call, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
		RESTPolicy:  RESTBudgetPolicy{MinRemainingReserve: 1000},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var search restIssueSearchResponse
	if err := client.REST(context.Background(), http.MethodGet, "/search/issues?q=repo%3Adigitaldrywood%2Fdetent", nil, &search); err != nil {
		t.Fatalf("REST() issue search error = %v", err)
	}
	var pulls []restPullRequest
	if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/pulls?state=all", nil, &pulls); err != nil {
		t.Fatalf("REST() pull requests after search budget error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("REST calls = %d, want search and pull requests sent", calls.Load())
	}

	usage := client.FlushRESTRateLimitUsage()
	if usage.RateLimited {
		t.Fatalf("RESTRateLimitUsage.RateLimited = true, want no reserved budget throttle")
	}
	if usage.RateLimit.Resource != "core" || usage.RateLimit.Remaining != 4981 {
		t.Fatalf("RateLimit = %#v, want latest core pull request snapshot", usage.RateLimit)
	}
}

func TestClientFlushRESTRateLimitUsagePrefersCoreBudget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 22, 0, 0, 0, time.UTC)
	client := &Client{
		hasRestRateLimit: true,
		restRateLimit: connector.RESTRateLimit{
			Limit: 30, Used: 30, Remaining: 0, Resource: "search", ResetAt: now.Add(time.Minute), UpdatedAt: now,
		},
		restRateLimits: map[string]connector.RESTRateLimit{
			"core": {
				Limit: 5000, Used: 4200, Remaining: 800, Resource: "core", ResetAt: now.Add(time.Hour), UpdatedAt: now,
			},
			"search": {
				Limit: 30, Used: 30, Remaining: 0, Resource: "search", ResetAt: now.Add(time.Minute), UpdatedAt: now,
			},
		},
	}

	usage := client.FlushRESTRateLimitUsage()

	if !usage.HasRateLimit || usage.RateLimit.Resource != "core" || usage.RateLimit.Remaining != 800 {
		t.Fatalf("RateLimit = %#v, want core budget with 800 remaining", usage.RateLimit)
	}
}

func TestClientFlushRESTRateLimitUsageAttributesEndpointBudgets(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		path      string
		resource  string
		remaining string
		resetAt   time.Time
	}{
		{path: "/repos/digitaldrywood/detent/issues", resource: "core", remaining: "4800", resetAt: now.Add(time.Hour)},
		{path: "/search/issues?q=repo%3Adigitaldrywood%2Fdetent", resource: "search", remaining: "20", resetAt: now.Add(2 * time.Minute)},
	}
	responses := make(map[string]struct {
		resource  string
		remaining string
		resetAt   time.Time
	}, len(tests))
	for _, test := range tests {
		responses[test.path] = struct {
			resource  string
			remaining string
			resetAt   time.Time
		}{resource: test.resource, remaining: test.remaining, resetAt: test.resetAt}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := responses[r.URL.RequestURI()]
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "200")
		w.Header().Set("X-RateLimit-Remaining", response.remaining)
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(response.resetAt.Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", response.resource)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("attribution-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	for _, test := range tests {
		if err := client.REST(context.Background(), http.MethodGet, test.path, nil, &[]json.RawMessage{}); err != nil {
			t.Fatalf("REST(%q) error = %v", test.path, err)
		}
	}

	usage := client.FlushRESTRateLimitUsage()
	if len(usage.Budgets) != len(tests) {
		t.Fatalf("Budgets len = %d, want %d: %#v", len(usage.Budgets), len(tests), usage.Budgets)
	}
	credentialIdentity := usage.Budgets[0].CredentialIdentity
	if !strings.HasPrefix(credentialIdentity, "github-rest:") {
		t.Fatalf("CredentialIdentity = %q, want redacted github-rest identity", credentialIdentity)
	}
	for _, test := range tests {
		t.Run(test.resource, func(t *testing.T) {
			family := restEndpointFamily(http.MethodGet, test.path)
			for _, budget := range usage.Budgets {
				if budget.EndpointFamily != family {
					continue
				}
				if budget.CredentialIdentity != credentialIdentity || budget.RateLimit.Resource != test.resource || budget.RateLimit.ResetAt.Unix() != test.resetAt.Unix() {
					t.Fatalf("budget = %#v, want credential %q resource %q reset %v", budget, credentialIdentity, test.resource, test.resetAt)
				}
				return
			}
			t.Fatalf("Budgets = %#v, want endpoint family %q", usage.Budgets, family)
		})
	}
}

func TestClientRESTCredentialIdentityStaysStableAcrossInstallationTokenRotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source func(*InstallationTokenSource) TokenSource
	}{
		{name: "installation source", source: func(source *InstallationTokenSource) TokenSource { return source }},
		{name: "token resolver", source: func(source *InstallationTokenSource) TokenSource { return &TokenResolver{app: source} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := &InstallationTokenSource{installationID: "4242", cachedToken: "first-token"}
			client := &Client{restEndpoint: "https://api.github.com", tokenSource: test.source(source)}
			first := client.restCredentialIdentity("first-token")
			source.mu.Lock()
			source.cachedToken = "second-token"
			source.mu.Unlock()
			second := client.restCredentialIdentity("second-token")
			if first != "github-app-installation:4242" || second != first {
				t.Fatalf("identities = %q, %q, want stable installation identity", first, second)
			}
		})
	}
}

func TestClientDoesNotWarnOnExpectedSharedRESTBudgetUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		remaining []string
	}{
		{name: "detent request explains drop", remaining: []string{"4999", "4998"}},
		{name: "small external drop stays quiet", remaining: []string{"4999", "4990"}},
		{name: "worker consumption is expected telemetry", remaining: []string{"4999", "4988"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				index := int(calls.Add(1)) - 1
				w.Header().Set("X-RateLimit-Limit", "5000")
				w.Header().Set("X-RateLimit-Used", strconv.Itoa(5000-mustAtoi(t, test.remaining[index])))
				w.Header().Set("X-RateLimit-Remaining", test.remaining[index])
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
				w.Header().Set("X-RateLimit-Resource", "core")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[]`))
			}))
			t.Cleanup(server.Close)

			var logs bytes.Buffer
			client, err := NewClient(ClientConfig{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("divergence-" + test.name),
				HTTPClient:  server.Client(),
				Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}
			for range test.remaining {
				if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues", nil, &[]json.RawMessage{}); err != nil {
					t.Fatalf("REST() error = %v", err)
				}
			}

			output := logs.String()
			if strings.Contains(output, "level=WARN msg=\"github rest usage divergence coalesced\"") {
				t.Fatalf("expected shared usage emitted warning; logs = %s", output)
			}
		})
	}
}

func TestClientCoalescesExpectedSharedRESTBudgetDivergence(t *testing.T) {
	t.Parallel()

	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	remaining := []string{"4999", "4988", "4977"}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		index := int(calls.Add(1)) - 1
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", strconv.Itoa(5000-mustAtoi(t, remaining[index])))
		w.Header().Set("X-RateLimit-Remaining", remaining[index])
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", "core")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	var logs bytes.Buffer
	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("shared-divergence-token"),
		HTTPClient:  server.Client(),
		Logger:      slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	for range remaining {
		if err := client.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues", nil, &[]json.RawMessage{}); err != nil {
			t.Fatalf("REST() error = %v", err)
		}
	}

	usage := client.FlushRESTRateLimitUsage()
	if len(usage.Divergences) != 1 || usage.Divergences[0].AttributedRequests != 20 || usage.Divergences[0].UnattributedRequests != 0 {
		t.Fatalf("Divergences = %#v, want 20 attributed requests", usage.Divergences)
	}

	output := logs.String()
	if got := strings.Count(output, "level=WARN msg=\"github rest usage divergence coalesced\""); got != 0 {
		t.Fatalf("warning count = %d, want 0; logs = %s", got, output)
	}
	if got := strings.Count(output, "level=DEBUG msg=\"github rest usage divergence coalesced\""); got != 1 {
		t.Fatalf("debug report count = %d, want 1; logs = %s", got, output)
	}
}

func TestClientRESTBudgetDivergenceEscalationAndRollover(t *testing.T) {
	t.Parallel()

	type observation struct {
		client    int
		remaining int64
		window    int
	}
	tests := []struct {
		name               string
		credentialIdentity string
		reserve            int64
		observations       []observation
		wantAttributed     int64
		wantUnattributed   int64
		wantWarnings       int
		wantDebug          int
		wantWindow         int
		wantLogFields      []string
	}{
		{
			name:               "unexplained usage stays quiet below threshold",
			credentialIdentity: "github-app-installation:42",
			observations:       []observation{{remaining: 4999}, {remaining: 4993}},
			wantUnattributed:   5,
		},
		{
			name:               "unexplained usage warns once at accumulated threshold",
			credentialIdentity: "github-app-installation:43",
			observations:       []observation{{remaining: 4999}, {remaining: 4993}, {remaining: 4987}, {remaining: 4981}},
			wantUnattributed:   15,
			wantWarnings:       1,
			wantLogFields:      []string{"report_reason=unattributed_threshold", "observed_requests=12", "detent_requests=2", "unattributed_requests=10", "window_started_at=", "last_observed_at=", "reset_at="},
		},
		{
			name:               "shared usage warns once when reserve is threatened",
			credentialIdentity: "github-rest:reserve",
			reserve:            10,
			observations:       []observation{{remaining: 20}, {remaining: 9}, {remaining: 3}},
			wantAttributed:     15,
			wantWarnings:       1,
			wantLogFields:      []string{"report_reason=reserve_threat", "attribution=expected_shared_credential", "reserve=10"},
		},
		{
			name:               "shared usage coalesces across clients",
			credentialIdentity: "github-rest:clients",
			observations:       []observation{{remaining: 4999}, {client: 1, remaining: 4988}, {remaining: 4977}},
			wantAttributed:     20,
			wantDebug:          1,
		},
		{
			name:               "provider reset starts a new window",
			credentialIdentity: "github-rest:rollover",
			observations:       []observation{{remaining: 4999}, {remaining: 4988}, {remaining: 4999, window: 1}, {remaining: 4988, window: 1}},
			wantAttributed:     10,
			wantDebug:          2,
			wantWindow:         1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			start := time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)
			registry := newRESTDivergenceRegistry()
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			clients := []*Client{
				{restEndpoint: "https://api.github.test", restPolicy: RESTBudgetPolicy{MinRemainingReserve: test.reserve}, restDivergences: registry, logger: logger},
				{restEndpoint: "https://api.github.test", restPolicy: RESTBudgetPolicy{MinRemainingReserve: test.reserve}, restDivergences: registry, logger: logger},
			}
			paths := []string{"/repos/example/repo/issues", "/repos/example/repo/issues/1/comments", "/repos/example/repo/pulls"}
			for index, current := range test.observations {
				at := start.Add(time.Duration(index) * time.Minute)
				resetAt := start.Add(time.Duration(current.window+1) * time.Hour)
				headers := http.Header{
					"X-Ratelimit-Limit":     []string{"5000"},
					"X-Ratelimit-Used":      []string{strconv.FormatInt(5000-current.remaining, 10)},
					"X-Ratelimit-Remaining": []string{strconv.FormatInt(current.remaining, 10)},
					"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt.Unix(), 10)},
					"X-Ratelimit-Resource":  []string{"core"},
				}
				clients[current.client].recordRESTRateLimitFromHeaders(context.Background(), "", test.credentialIdentity, http.MethodGet, paths[index%len(paths)], http.StatusOK, headers, nil, at, false)
			}

			usage := clients[0].FlushRESTRateLimitUsage()
			if len(usage.Divergences) != 1 {
				t.Fatalf("Divergences = %#v, want one active provider window", usage.Divergences)
			}
			divergence := usage.Divergences[0]
			if divergence.AttributedRequests != test.wantAttributed || divergence.UnattributedRequests != test.wantUnattributed {
				t.Fatalf("divergence = %#v, want attributed %d unattributed %d", divergence, test.wantAttributed, test.wantUnattributed)
			}
			wantReset := start.Add(time.Duration(test.wantWindow+1) * time.Hour)
			if !divergence.ResetAt.Equal(wantReset) {
				t.Fatalf("ResetAt = %s, want %s", divergence.ResetAt, wantReset)
			}

			output := logs.String()
			if got := strings.Count(output, "level=WARN msg=\"github rest usage divergence coalesced\""); got != test.wantWarnings {
				t.Fatalf("warning count = %d, want %d; logs = %s", got, test.wantWarnings, output)
			}
			if got := strings.Count(output, "level=DEBUG msg=\"github rest usage divergence coalesced\""); got != test.wantDebug {
				t.Fatalf("debug count = %d, want %d; logs = %s", got, test.wantDebug, output)
			}
			for _, field := range test.wantLogFields {
				if !strings.Contains(output, field) {
					t.Fatalf("logs = %s, want field %q", output, field)
				}
			}
		})
	}
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("strconv.Atoi(%q) error = %v", value, err)
	}
	return parsed
}

func TestClientRESTBackoffAppliesAcrossClientsWithSharedToken(t *testing.T) {
	t.Parallel()

	token := StaticTokenSource(t.TempDir())
	resetAt := time.Now().UTC().Add(time.Hour)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "5000")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
		w.Header().Set("X-RateLimit-Resource", "core")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
	}))
	t.Cleanup(server.Close)

	clientA, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: token,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() clientA error = %v", err)
	}
	clientB, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: token,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() clientB error = %v", err)
	}

	err = clientA.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues/666/comments", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("clientA REST() error = %v, want ErrRateLimited", err)
	}
	err = clientB.REST(context.Background(), http.MethodGet, "/repos/digitaldrywood/detent/issues?labels=detent%3Atodo", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("clientB REST() error = %v, want ErrRateLimited from shared backoff", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("REST calls = %d, want only the first client to hit the server", calls.Load())
	}

	usage := clientB.FlushRESTRateLimitUsage()
	if !usage.BackoffUntil.After(time.Now()) {
		t.Fatalf("clientB BackoffUntil = %v, want shared future deadline", usage.BackoffUntil)
	}
}

func TestClientRESTBackoffLifetimeAcrossRecreatedClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		endpoint         string
		token            string
		isolatedRegistry bool
		wantBackoff      bool
	}{
		{name: "same identity retains backoff", wantBackoff: true},
		{name: "private registry isolates repeated fixture", isolatedRegistry: true},
		{name: "different endpoint isolates backoff", endpoint: "http://127.0.0.1:12346"},
		{name: "different token isolates backoff", token: "other-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registry := newRESTBackoffRegistry()
			cfg := ClientConfig{
				Endpoint:    "http://127.0.0.1:12345",
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient: staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
					return jsonResponse(r, http.StatusForbidden, `{"message":"API rate limit exceeded"}`, http.Header{
						"Retry-After": []string{"120"},
					}), nil
				}},
			}
			first, err := NewClient(cfg)
			if err != nil {
				t.Fatalf("NewClient() first error = %v", err)
			}
			first.restBackoffs = registry
			if err := first.REST(t.Context(), http.MethodGet, "/user", nil, nil); !errors.Is(err, ErrRateLimited) {
				t.Fatalf("first REST() error = %v, want ErrRateLimited", err)
			}

			var calls atomic.Int64
			cfg.HTTPClient = staticHTTPClient{do: func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(r, http.StatusOK, `{}`, nil), nil
			}}
			if tt.endpoint != "" {
				cfg.Endpoint = tt.endpoint
			}
			if tt.token != "" {
				cfg.TokenSource = StaticTokenSource(tt.token)
			}
			recreated, err := NewClient(cfg)
			if err != nil {
				t.Fatalf("NewClient() recreated error = %v", err)
			}
			recreated.restBackoffs = registry
			if tt.isolatedRegistry {
				recreated.restBackoffs = newRESTBackoffRegistry()
			}
			err = recreated.REST(t.Context(), http.MethodGet, "/user", nil, nil)
			wantCalls := int64(1)
			if tt.wantBackoff {
				wantCalls = 0
				var apiErr *StatusError
				if !errors.Is(err, ErrRateLimited) || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests {
					t.Fatalf("recreated REST() error = %v, want synthetic 429 backoff", err)
				}
			} else if err != nil {
				t.Fatalf("recreated REST() error = %v", err)
			}
			if got := calls.Load(); got != wantCalls {
				t.Fatalf("recreated HTTP calls = %d, want %d", got, wantCalls)
			}
		})
	}
}

func TestClientReportsStaleAuthWhenRefreshFails(t *testing.T) {
	t.Parallel()

	source := newRefreshingTokenTestSource("stale-token", "")
	source.refreshErr = errors.New("gh auth token failed")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: source,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil)
	if !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("GraphQL() error = %v, want ErrAuthenticationFailed", err)
	}
	health, ok := client.AuthHealth()
	if !ok {
		t.Fatal("AuthHealth() ok = false, want true")
	}
	if health.Status != connector.AuthStatusStale {
		t.Fatalf("AuthHealth().Status = %q, want %q", health.Status, connector.AuthStatusStale)
	}
	if health.LastError == "" || health.LastErrorAt.IsZero() {
		t.Fatalf("AuthHealth() missing stale auth detail: %#v", health)
	}
	if health.LastRecoveredAt.IsZero() == false {
		t.Fatalf("AuthHealth().LastRecoveredAt = %v, want zero", health.LastRecoveredAt)
	}
}

func restEndpointUsageCount(usages []connector.RESTEndpointUsage, family string) int64 {
	for _, usage := range usages {
		if usage.EndpointFamily == family {
			return usage.Count
		}
	}
	return 0
}

func restEndpointUsage(usages []connector.RESTEndpointUsage, family string) connector.RESTEndpointUsage {
	for _, usage := range usages {
		if usage.EndpointFamily == family {
			return usage
		}
	}
	return connector.RESTEndpointUsage{}
}

func TestClientGraphQLCapturesRateLimitSnapshot(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"},"rateLimit":{"limit":5000,"used":120,"remaining":4880,"cost":2,"resetAt":"` + resetAt.Format(time.RFC3339) + `"}}}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var got struct {
		Viewer struct {
			Login string `json:"login"`
		} `json:"viewer"`
	}
	if err := client.GraphQL(context.Background(), "query { viewer { login } rateLimit { remaining resetAt cost } }", nil, &got); err != nil {
		t.Fatalf("GraphQL() error = %v", err)
	}

	rateLimit, ok := client.GraphQLRateLimit()
	if !ok {
		t.Fatal("GraphQLRateLimit() ok = false, want true")
	}
	if rateLimit.Limit != 5000 || rateLimit.Used != 120 || rateLimit.Remaining != 4880 || rateLimit.Cost != 2 {
		t.Fatalf("GraphQLRateLimit() = %#v, want limit 5000 used 120 remaining 4880 cost 2", rateLimit)
	}
	if !rateLimit.ResetAt.Equal(resetAt) {
		t.Fatalf("GraphQLRateLimit().ResetAt = %v, want %v", rateLimit.ResetAt, resetAt)
	}
}

func TestClientGraphQLAggregatesRateLimitCostsByQueryType(t *testing.T) {
	t.Parallel()

	responses := make(chan string, 3)
	responses <- `{"data":{"rateLimit":{"limit":5000,"used":10,"remaining":4990,"cost":4,"resetAt":"2026-06-01T13:00:00Z"}}}`
	responses <- `{"data":{"rateLimit":{"limit":5000,"used":13,"remaining":4987,"cost":3,"resetAt":"2026-06-01T13:00:00Z"}}}`
	responses <- `{"data":{"rateLimit":{"limit":5000,"used":15,"remaining":4985,"cost":2,"resetAt":"2026-06-01T13:00:00Z"}}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(<-responses))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.GraphQLWithType(context.Background(), "candidate_issues", "query { rateLimit { cost } }", nil, nil); err != nil {
		t.Fatalf("first GraphQLWithType() error = %v", err)
	}
	if err := client.GraphQLWithType(context.Background(), "candidate_issues", "query { rateLimit { cost } }", nil, nil); err != nil {
		t.Fatalf("second GraphQLWithType() error = %v", err)
	}
	if err := client.GraphQLWithType(context.Background(), "running_states", "query { rateLimit { cost } }", nil, nil); err != nil {
		t.Fatalf("third GraphQLWithType() error = %v", err)
	}

	usage := client.FlushGraphQLRateLimitUsage()
	if !usage.HasRateLimit {
		t.Fatal("FlushGraphQLRateLimitUsage().HasRateLimit = false, want true")
	}
	if usage.TotalQueries != 3 || usage.TotalCost != 9 {
		t.Fatalf("FlushGraphQLRateLimitUsage() totals = queries %d cost %d, want queries 3 cost 9", usage.TotalQueries, usage.TotalCost)
	}
	if usage.RateLimit.Remaining != 4985 || usage.RateLimit.Cost != 2 {
		t.Fatalf("FlushGraphQLRateLimitUsage().RateLimit = %#v, want last snapshot remaining 4985 cost 2", usage.RateLimit)
	}
	want := []struct {
		queryType string
		count     int64
		cost      int64
	}{
		{queryType: "candidate_issues", count: 2, cost: 7},
		{queryType: "running_states", count: 1, cost: 2},
	}
	if len(usage.QueryCosts) != len(want) {
		t.Fatalf("QueryCosts len = %d, want %d: %#v", len(usage.QueryCosts), len(want), usage.QueryCosts)
	}
	for index, wantCost := range want {
		got := usage.QueryCosts[index]
		if got.QueryType != wantCost.queryType || got.Count != wantCost.count || got.Cost != wantCost.cost {
			t.Fatalf("QueryCosts[%d] = %#v, want %#v", index, got, wantCost)
		}
	}

	client.ResetGraphQLRateLimitUsage()
	usage = client.FlushGraphQLRateLimitUsage()
	if len(usage.QueryCosts) != 0 || usage.TotalQueries != 0 || usage.TotalCost != 0 {
		t.Fatalf("usage after reset = %#v, want no query costs", usage)
	}
}

func TestClientGraphQLRecordsRateLimitStatusWithoutSnapshot(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.GraphQLWithType(context.Background(), "issue_parent_metadata", "query { viewer { login } }", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("GraphQLWithType() error = %v, want ErrRateLimited", err)
	}

	usage := client.FlushGraphQLRateLimitUsage()
	if usage.HasRateLimit {
		t.Fatalf("FlushGraphQLRateLimitUsage().HasRateLimit = true, want false with no snapshot")
	}
	if usage.RateLimitStatus != connector.GraphQLRateLimitStatusExhausted {
		t.Fatalf("FlushGraphQLRateLimitUsage().RateLimitStatus = %q, want %q", usage.RateLimitStatus, connector.GraphQLRateLimitStatusExhausted)
	}

	usage = client.FlushGraphQLRateLimitUsage()
	if usage.RateLimitStatus != "" {
		t.Fatalf("second FlushGraphQLRateLimitUsage().RateLimitStatus = %q, want cleared", usage.RateLimitStatus)
	}
}

func TestClientGraphQLRateLimitFailureDoesNotPublishStaleSnapshot(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	responses := make(chan string, 2)
	responses <- `{"data":{"rateLimit":{"limit":5000,"used":120,"remaining":4880,"cost":2,"resetAt":"` + resetAt.Format(time.RFC3339) + `"}}}`
	responses <- `{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(<-responses))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.GraphQLWithType(context.Background(), "candidate_issues", "query { rateLimit { cost } }", nil, nil); err != nil {
		t.Fatalf("GraphQLWithType() healthy query error = %v", err)
	}
	usage := client.FlushGraphQLRateLimitUsage()
	if !usage.HasRateLimit || usage.RateLimit.Remaining != 4880 {
		t.Fatalf("FlushGraphQLRateLimitUsage() = %#v, want healthy current bucket", usage)
	}

	err = client.GraphQLWithType(context.Background(), "issue_parent_metadata", "query { viewer { login } }", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("GraphQLWithType() error = %v, want ErrRateLimited", err)
	}
	usage = client.FlushGraphQLRateLimitUsage()
	if usage.HasRateLimit {
		t.Fatalf("FlushGraphQLRateLimitUsage().HasRateLimit = true, want false after failure with no fresh bucket: %#v", usage)
	}
	if usage.RateLimit != (connector.GraphQLRateLimit{}) {
		t.Fatalf("FlushGraphQLRateLimitUsage().RateLimit = %#v, want zero stale bucket", usage.RateLimit)
	}
	if usage.RateLimitStatus != connector.GraphQLRateLimitStatusExhausted {
		t.Fatalf("FlushGraphQLRateLimitUsage().RateLimitStatus = %q, want %q", usage.RateLimitStatus, connector.GraphQLRateLimitStatusExhausted)
	}
}

func TestClientGraphQLInfersMutationCostsFromHeaders(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	type graphQLResponse struct {
		body    string
		headers map[string]string
	}
	responses := make(chan graphQLResponse, 3)
	responses <- graphQLResponse{
		body: `{"data":{"rateLimit":{"limit":5000,"used":10,"remaining":4990,"cost":4,"resetAt":"` + resetAt.Format(time.RFC3339) + `"}}}`,
	}
	responses <- graphQLResponse{
		body: `{"data":{"addComment":{"commentEdge":{"node":{"id":"IC_kw1"}}}}}`,
		headers: map[string]string{
			"X-RateLimit-Limit":     "5000",
			"X-RateLimit-Used":      "13",
			"X-RateLimit-Remaining": "4987",
			"X-RateLimit-Reset":     strconv.FormatInt(resetAt.Unix(), 10),
		},
	}
	responses <- graphQLResponse{
		body: `{"data":{"updateProjectV2ItemFieldValue":{"projectV2Item":{"id":"PVTI_kw1"}}}}`,
		headers: map[string]string{
			"X-RateLimit-Limit":     "5000",
			"X-RateLimit-Used":      "15",
			"X-RateLimit-Remaining": "4985",
			"X-RateLimit-Reset":     strconv.FormatInt(resetAt.Unix(), 10),
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := <-responses
		for key, value := range response.headers {
			w.Header().Set(key, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(response.body))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if err := client.GraphQLWithType(context.Background(), "candidate_issues", "query { rateLimit { cost } }", nil, nil); err != nil {
		t.Fatalf("GraphQLWithType() query error = %v", err)
	}
	if err := client.GraphQLWithType(context.Background(), "create_comment", "mutation { addComment(input: {}) { commentEdge { node { id } } } }", nil, nil); err != nil {
		t.Fatalf("GraphQLWithType() comment mutation error = %v", err)
	}
	if err := client.GraphQLWithType(context.Background(), "update_project_field", "mutation { updateProjectV2ItemFieldValue(input: {}) { projectV2Item { id } } }", nil, nil); err != nil {
		t.Fatalf("GraphQLWithType() field mutation error = %v", err)
	}

	usage := client.FlushGraphQLRateLimitUsage()
	want := []connector.GraphQLQueryCost{
		{QueryType: "candidate_issues", Count: 1, Cost: 4},
		{QueryType: "create_comment", Count: 1, Cost: 3},
		{QueryType: "update_project_field", Count: 1, Cost: 2},
	}
	if usage.TotalQueries != 3 || usage.TotalCost != 9 {
		t.Fatalf("FlushGraphQLRateLimitUsage() totals = queries %d cost %d, want queries 3 cost 9", usage.TotalQueries, usage.TotalCost)
	}
	if len(usage.QueryCosts) != len(want) {
		t.Fatalf("QueryCosts len = %d, want %d: %#v", len(usage.QueryCosts), len(want), usage.QueryCosts)
	}
	for index, wantCost := range want {
		if usage.QueryCosts[index] != wantCost {
			t.Fatalf("QueryCosts[%d] = %#v, want %#v", index, usage.QueryCosts[index], wantCost)
		}
	}
}

func TestClientGraphQLSerializesHeaderInferredMutationCosts(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(oldProcs)

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	resetAt := now.Add(time.Hour)
	client := &Client{}
	client.setRateLimit(connector.GraphQLRateLimit{
		Limit:     5000,
		Used:      10,
		Remaining: 4990,
		ResetAt:   resetAt,
		UpdatedAt: now,
	})

	const mutations = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 1; i <= mutations; i++ {
		used := int64(10 + i)
		remaining := int64(5000) - used
		wg.Go(func() {
			<-start
			headers := http.Header{}
			headers.Set("X-RateLimit-Limit", "5000")
			headers.Set("X-RateLimit-Used", strconv.FormatInt(used, 10))
			headers.Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
			headers.Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))

			snapshot := client.recordRateLimitFromHeaders(headers, now)
			client.recordGraphQLQueryCostFromHeaders("mutation", snapshot)
		})
	}
	close(start)
	wg.Wait()

	usage := client.FlushGraphQLRateLimitUsage()
	if usage.TotalCost != mutations {
		t.Fatalf("FlushGraphQLRateLimitUsage().TotalCost = %d, want %d", usage.TotalCost, mutations)
	}
	if len(usage.QueryCosts) != 1 {
		t.Fatalf("QueryCosts len = %d, want 1: %#v", len(usage.QueryCosts), usage.QueryCosts)
	}
	if usage.QueryCosts[0].QueryType != "mutation" || usage.QueryCosts[0].Cost != mutations {
		t.Fatalf("QueryCosts[0] = %#v, want mutation cost %d", usage.QueryCosts[0], mutations)
	}
	if usage.QueryCosts[0].Count != mutations {
		t.Fatalf("QueryCosts[0].Count = %d, want %d", usage.QueryCosts[0].Count, mutations)
	}
	if usage.RateLimit.Used != 10+mutations || usage.RateLimit.Remaining != 5000-10-mutations {
		t.Fatalf("RateLimit = %#v, want latest used %d remaining %d", usage.RateLimit, 10+mutations, 5000-10-mutations)
	}
}

func TestClientGraphQLCountsStaleHeaderInferredMutation(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	resetAt := now.Add(time.Hour)
	client := &Client{}
	client.setRateLimit(connector.GraphQLRateLimit{
		Limit:     5000,
		Used:      15,
		Remaining: 4985,
		ResetAt:   resetAt,
		UpdatedAt: now,
	})

	headers := http.Header{}
	headers.Set("X-RateLimit-Limit", "5000")
	headers.Set("X-RateLimit-Used", "13")
	headers.Set("X-RateLimit-Remaining", "4987")
	headers.Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))

	snapshot := client.recordRateLimitFromHeaders(headers, now.Add(time.Second))
	client.recordGraphQLQueryCostFromHeaders("mutation", snapshot)

	usage := client.FlushGraphQLRateLimitUsage()
	if usage.TotalQueries != 1 || usage.TotalCost != 0 {
		t.Fatalf("FlushGraphQLRateLimitUsage() totals = queries %d cost %d, want queries 1 cost 0", usage.TotalQueries, usage.TotalCost)
	}
	if len(usage.QueryCosts) != 1 {
		t.Fatalf("QueryCosts len = %d, want 1: %#v", len(usage.QueryCosts), usage.QueryCosts)
	}
	if usage.QueryCosts[0] != (connector.GraphQLQueryCost{QueryType: "mutation", Count: 1, Cost: 0}) {
		t.Fatalf("QueryCosts[0] = %#v, want mutation count 1 cost 0", usage.QueryCosts[0])
	}
	if usage.RateLimit.Used != 15 || usage.RateLimit.Remaining != 4985 {
		t.Fatalf("RateLimit = %#v, want previous snapshot preserved", usage.RateLimit)
	}
}

func TestClientGraphQLCapturesRetryAfterRateLimit(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "5000")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("GraphQL() error = %v, want ErrRateLimited", err)
	}

	rateLimit, ok := client.GraphQLRateLimit()
	if !ok {
		t.Fatal("GraphQLRateLimit() ok = false, want true")
	}
	if rateLimit.RetryAfter != 2*time.Minute || rateLimit.Remaining != 0 || rateLimit.Limit != 5000 {
		t.Fatalf("GraphQLRateLimit() = %#v, want retry-after 2m and exhausted headers", rateLimit)
	}
}

func TestClientGraphQLClearsRetryAfterOnHeaderRefresh(t *testing.T) {
	t.Parallel()

	responses := make(chan func(http.ResponseWriter), 2)
	responses <- func(w http.ResponseWriter) {
		w.Header().Set("Retry-After", "120")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "120")
		w.Header().Set("X-RateLimit-Remaining", "4880")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
	}
	responses <- func(w http.ResponseWriter) {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Used", "121")
		w.Header().Set("X-RateLimit-Remaining", "4879")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"viewer":{"login":"octocat"}}}`))
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handle := <-responses
		handle(w)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		Endpoint:    server.URL,
		TokenSource: StaticTokenSource("test-token"),
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.GraphQL(context.Background(), "query { viewer { login } }", nil, nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("GraphQL() error = %v, want ErrRateLimited", err)
	}
	rateLimit, ok := client.GraphQLRateLimit()
	if !ok {
		t.Fatal("GraphQLRateLimit() ok = false, want true")
	}
	if rateLimit.RetryAfter != 2*time.Minute {
		t.Fatalf("GraphQLRateLimit().RetryAfter = %s, want 2m", rateLimit.RetryAfter)
	}

	err = client.GraphQLWithType(context.Background(), graphQLQueryRateLimitProbe, "query { viewer { login } }", nil, nil)
	if err != nil {
		t.Fatalf("GraphQL() error = %v", err)
	}

	rateLimit, ok = client.GraphQLRateLimit()
	if !ok {
		t.Fatal("GraphQLRateLimit() ok = false, want true")
	}
	if rateLimit.RetryAfter != 0 {
		t.Fatalf("GraphQLRateLimit().RetryAfter = %s, want cleared", rateLimit.RetryAfter)
	}
	if rateLimit.Remaining != 4879 || rateLimit.Used != 121 || rateLimit.Limit != 5000 {
		t.Fatalf("GraphQLRateLimit() = %#v, want refreshed primary headers", rateLimit)
	}
}

func TestClientGraphQLRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "missing data", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)

			client, err := NewClient(ClientConfig{
				Endpoint:    server.URL,
				TokenSource: StaticTokenSource("test-token"),
				HTTPClient:  server.Client(),
			})
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			var out map[string]any
			err = client.GraphQL(context.Background(), "query { viewer { login } }", nil, &out)
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("GraphQL() error = %v, want ErrInvalidResponse", err)
			}
		})
	}
}

type staticHTTPClient struct {
	do func(*http.Request) (*http.Response, error)
}

func (c staticHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.do(req)
}

func jsonResponse(req *http.Request, status int, body string, headers http.Header) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

type refreshingTokenTestSource struct {
	mu           sync.Mutex
	token        string
	refreshToken string
	refreshErr   error
	refreshes    atomic.Int64
}

func newRefreshingTokenTestSource(token string, refreshToken string) *refreshingTokenTestSource {
	return &refreshingTokenTestSource{token: token, refreshToken: refreshToken}
}

func (s *refreshingTokenTestSource) Token(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, nil
}

func (s *refreshingTokenTestSource) RefreshToken(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.refreshes.Add(1)
	if s.refreshErr != nil {
		return "", s.refreshErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = s.refreshToken
	return s.token, nil
}
