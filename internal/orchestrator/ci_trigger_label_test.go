package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
)

func TestScheduleCITriggerLabelCurrentHeadChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		checks        []connector.PullRequestCheck
		failures      []connector.PullRequestCheck
		required      []string
		head          string
		hydrationErr  error
		unavailable   string
		afterHeadPush bool
		forceReapply  bool
		wantReapply   bool
		wantReason    string
	}{
		{
			name:       "green commit status after restart",
			checks:     []connector.PullRequestCheck{{Name: "Full CI", Status: "success", Conclusion: "success"}},
			wantReason: "required_checks_green",
		},
		{
			name:          "green check run after reported push",
			checks:        []connector.PullRequestCheck{{Name: "Full CI", Status: "completed", Conclusion: "success"}},
			afterHeadPush: true,
			wantReason:    "required_checks_green",
		},
		{
			name:         "force preserves label ordering repair",
			checks:       []connector.PullRequestCheck{{Name: "Full CI", Status: "success", Conclusion: "success"}},
			forceReapply: true, wantReapply: true, wantReason: "forced_reapply",
		},
		{
			name: "new head lacks old head success",
			head: "new-head", afterHeadPush: true, wantReapply: true, wantReason: "after_head_push",
		},
		{
			name: "missing required status", wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "failed status can retry",
			checks:      []connector.PullRequestCheck{{Name: "Full CI", Status: "failure", Conclusion: "failure"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "pending status is not green",
			checks:      []connector.PullRequestCheck{{Name: "Full CI", Status: "pending", Conclusion: "pending"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "skipped check is not green",
			checks:      []connector.PullRequestCheck{{Name: "Full CI", Status: "completed", Conclusion: "skipped"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "required failure overrides green inventory",
			checks:      []connector.PullRequestCheck{{Name: "Full CI", Status: "completed", Conclusion: "success"}},
			failures:    []connector.PullRequestCheck{{Name: "Full CI", Status: "in_progress"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "unrelated green check is insufficient",
			checks:      []connector.PullRequestCheck{{Name: "Lint", Status: "completed", Conclusion: "success"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:        "every configured check must pass",
			required:    []string{"Full CI", "Security"},
			checks:      []connector.PullRequestCheck{{Name: "Full CI", Status: "success", Conclusion: "success"}},
			wantReapply: true, wantReason: "required_checks_not_green",
		},
		{
			name:     "all configured checks pass",
			required: []string{"Full CI", "Security"},
			checks: []connector.PullRequestCheck{
				{Name: "Full CI", Status: "success", Conclusion: "success"},
				{Name: "Security", Status: "completed", Conclusion: "success"},
			},
			wantReason: "required_checks_green",
		},
		{
			name:         "hydration error does not spend CI",
			hydrationErr: errors.New("unavailable"), wantReason: "pull_request_refresh_failed",
		},
		{
			name:        "deferred hydration does not spend CI",
			unavailable: connector.PullRequestHydrationReasonRESTBudgetReserved, wantReason: "pull_request_hydration_unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				issue := connector.Issue{
					ID: "issue-2250", Identifier: "digitaldrywood/detent#2250", PRRepository: "digitaldrywood/detent",
					PullRequest: &connector.PullRequest{
						Number: 2250, HeadSHA: "old-head", State: "OPEN", CIStatus: "success",
						Checks: []connector.PullRequestCheck{{Name: "Full CI", Status: "success", Conclusion: "success"}},
					},
				}
				fresh := cloneIssue(issue)
				fresh.PullRequest.Checks = tt.checks
				fresh.PullRequest.RequiredCheckFailures = tt.failures
				fresh.PullRequest.HydrationUnavailableReason = tt.unavailable
				if tt.head != "" {
					fresh.PullRequest.HeadSHA = tt.head
				}
				tracker := &autoPromoteTickMergeConnector{
					hydratedIssues: []connector.Issue{fresh}, hydrateErr: tt.hydrationErr,
				}
				var logs mergeFastPathLockedBuffer
				required := tt.required
				if required == nil {
					required = []string{"Full CI"}
				}
				orch := &Orchestrator{
					cfg: Config{AutoPromote: AutoPromoteConfig{Gate: gate.Config{
						RequiredStatusChecks: required, CITriggerLabel: "run-full-ci",
					}}},
					connector: tracker, logger: slog.New(slog.NewTextHandler(&logs, nil)),
				}
				got := orch.scheduleCITriggerLabel(context.Background(), issue, []string{"Full CI"}, 1, tt.afterHeadPush, tt.forceReapply)
				synctest.Wait()
				if got != tt.wantReapply || (len(tracker.relabels) == 1) != tt.wantReapply {
					t.Fatalf("scheduled = %v, label events = %d, want reapply %v", got, len(tracker.relabels), tt.wantReapply)
				}
				if !strings.Contains(logs.String(), "reason="+tt.wantReason) {
					t.Fatalf("logs = %s, want reason %s", logs.String(), tt.wantReason)
				}
				if tt.wantReapply && !strings.Contains(logs.String(), "required_check_states=") {
					t.Fatalf("logs = %s, want current required-check states", logs.String())
				}
			})
		})
	}
}
