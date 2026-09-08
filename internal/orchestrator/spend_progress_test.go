package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

func TestEvaluateSpendProgress(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	createdAt := base.Add(-time.Hour)
	acceptedAt := base.Add(-20 * time.Minute)
	artifactReceiptBody := "## Codex Workpad\n\nCompleted timecode repairs with verification evidence.\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```"
	artifactDeliverableFingerprint := workpad.ContentHash(`{"kind":"artifact"}`)
	acceptedAttempt := store.WorkAttempt{
		CompletedAt: acceptedAt,
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			spendProgressMetadataKey: spendProgressRecord{
				AcceptedStateChange: true,
				AcceptedReason:      "signature_changed",
				LimitUSD:            5,
			},
		}),
	}

	tests := []struct {
		name             string
		billingMode      string
		tokenLimit       int64
		limit            float64
		spend            store.IssueSpendSince
		history          []store.WorkAttempt
		issue            connector.Issue
		effort           string
		accepted         bool
		acceptedReason   string
		deliverableKind  string
		artifactEvidence runpkg.ArtifactProgressEvidence
		wantBlock        bool
		wantBlockedBy    string
		wantAccepted     bool
		wantReason       string
		wantLimit        float64
		wantSpendCalls   int
		wantHistoryCalls int
		wantSince        time.Time
		wantCase         string
		creditAt         time.Time
	}{
		{name: "disabled avoids tracking", limit: 0, spend: store.IssueSpendSince{CostUSD: 100}, wantLimit: 0, wantSpendCalls: 0, wantHistoryCalls: 0},
		{name: "normal three retry sessions stay below default", limit: 3, spend: store.IssueSpendSince{CostUSD: 2.7, Sessions: 3}, wantLimit: 3, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "below threshold", limit: 5, spend: store.IssueSpendSince{CostUSD: 4.99, Sessions: 3}, wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "at threshold", limit: 5, spend: store.IssueSpendSince{CostUSD: 5, Sessions: 4}, wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "metered blocks above threshold", billingMode: "metered", limit: 5, spend: store.IssueSpendSince{CostUSD: 5.01, Sessions: 4}, wantBlock: true, wantBlockedBy: "usd", wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "subscription leaves USD breaker inert", billingMode: "subscription", limit: 5, spend: store.IssueSpendSince{CostUSD: 5.01, Sessions: 4}},
		{name: "subscription token breaker blocks at threshold", billingMode: "subscription", tokenLimit: 25_000_000, limit: 5, spend: store.IssueSpendSince{CostUSD: 5.01, TotalTokens: 25_000_000, Sessions: 4}, wantBlock: true, wantBlockedBy: "tokens", wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "subscription token breaker stays below threshold", billingMode: "subscription", tokenLimit: 25_000_000, spend: store.IssueSpendSince{TotalTokens: 24_999_999, Sessions: 3}, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "July telemetry replay", limit: 5, spend: store.IssueSpendSince{CostUSD: 6.75, Sessions: 5}, wantBlock: true, wantBlockedBy: "usd", wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
		{name: "old sessions reset after accepted change", limit: 5, spend: store.IssueSpendSince{CostUSD: 1.25, Sessions: 1}, history: []store.WorkAttempt{acceptedAttempt}, wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: acceptedAt},
		{name: "operator credit resets old sessions", limit: 5, spend: store.IssueSpendSince{CostUSD: 1.25, Sessions: 1}, creditAt: base.Add(-5 * time.Minute), wantLimit: 5, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: base.Add(-5 * time.Minute)},
		{name: "current accepted change resets without spend lookup", limit: 5, spend: store.IssueSpendSince{CostUSD: 100}, accepted: true, acceptedReason: "signature_changed", wantAccepted: true, wantReason: "signature_changed", wantLimit: 5, wantSpendCalls: 0, wantHistoryCalls: 0, wantSince: base},
		{
			name:  "dirty to clean pull request resets spend",
			limit: 5,
			spend: store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
			issue: spendProgressIssueWithPR("same-head", "clean", "failure"),
			history: []store.WorkAttempt{{
				CompletedAt: base.Add(-10 * time.Minute),
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: map[string]any{
						"limit_usd":      5,
						"pr_fingerprint": map[string]any{"number": 214, "head_sha": "same-head", "mergeable_state": "dirty", "ci_status": "failure"},
					},
				}),
			}},
			wantAccepted:     true,
			wantReason:       "pull_request_mergeable",
			wantLimit:        5,
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:  "failing to passing pull request resets spend",
			limit: 5,
			spend: store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
			issue: spendProgressIssueWithPR("same-head", "dirty", "pass"),
			history: []store.WorkAttempt{{
				CompletedAt: base.Add(-10 * time.Minute),
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: map[string]any{
						"limit_usd":      5,
						"pr_fingerprint": map[string]any{"number": 214, "head_sha": "same-head", "mergeable_state": "dirty", "ci_status": "fail"},
					},
				}),
			}},
			wantAccepted:     true,
			wantReason:       "pull_request_ci_passing",
			wantLimit:        5,
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:  "new pull request head resets spend",
			limit: 5,
			spend: store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
			issue: spendProgressIssueWithPR("new-head", "dirty", "failure"),
			history: []store.WorkAttempt{{
				CompletedAt: base.Add(-10 * time.Minute),
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: map[string]any{
						"limit_usd":      5,
						"pr_fingerprint": map[string]any{"number": 214, "head_sha": "old-head", "mergeable_state": "dirty", "ci_status": "failure"},
					},
				}),
			}},
			wantAccepted:     true,
			wantReason:       "pull_request_head_changed",
			wantLimit:        5,
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:  "byte identical pull request still parks",
			limit: 5,
			spend: store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
			issue: spendProgressIssueWithPR("same-head", "dirty", "failure"),
			history: []store.WorkAttempt{{
				CompletedAt: base.Add(-10 * time.Minute),
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: map[string]any{
						"limit_usd":      5,
						"pr_fingerprint": map[string]any{"number": 214, "head_sha": "same-head", "mergeable_state": "dirty", "ci_status": "failure"},
					},
				}),
			}},
			wantBlock:        true,
			wantBlockedBy:    "usd",
			wantLimit:        5,
			wantSpendCalls:   1,
			wantHistoryCalls: 1,
			wantSince:        createdAt,
		},
		{
			name:        "artifact completion receipt resets spend",
			billingMode: "subscription",
			tokenLimit:  25_000_000,
			spend:       store.IssueSpendSince{TotalTokens: 33_152_887, Sessions: 6},
			issue: connector.Issue{
				Deliverable: &connector.Deliverable{Kind: "artifact"},
				WorkpadSignal: &workpad.Signal{
					Source: workpad.SourceStructured,
					Status: workpad.StatusComplete,
				},
				Comments: []connector.IssueComment{{Body: artifactReceiptBody}},
			},
			wantAccepted:     true,
			wantReason:       "artifact_receipt_changed",
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:            "static artifact receipt still parks",
			billingMode:     "subscription",
			tokenLimit:      25_000_000,
			spend:           store.IssueSpendSince{TotalTokens: 25_000_000, Sessions: 5},
			deliverableKind: workflowconfig.DeliverableArtifact,
			issue: connector.Issue{
				Deliverable: &connector.Deliverable{Kind: "artifact"},
				Comments:    []connector.IssueComment{{Body: artifactReceiptBody}},
			},
			history: []store.WorkAttempt{{
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: spendProgressRecord{
						TokenLimit: 25_000_000,
						ArtifactFingerprint: &spendProgressArtifactFingerprint{
							ReceiptHash:            workpad.ContentHash(artifactReceiptBody),
							StatusField:            gate.DefaultArtifactStatusField,
							DeliverableFingerprint: artifactDeliverableFingerprint,
						},
					},
				}),
			}},
			wantBlock:        true,
			wantBlockedBy:    "tokens",
			wantSpendCalls:   1,
			wantHistoryCalls: 1,
			wantSince:        createdAt,
			wantCase:         spendProgressCaseStaticArtifact,
		},
		{
			name:            "artifact status transition resets spend",
			billingMode:     "subscription",
			tokenLimit:      25_000_000,
			deliverableKind: workflowconfig.DeliverableArtifact,
			issue: connector.Issue{
				Deliverable: &connector.Deliverable{Kind: "artifact"},
				Fields:      map[string]string{gate.DefaultArtifactStatusField: "pending_review"},
			},
			history: []store.WorkAttempt{{
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: spendProgressRecord{
						TokenLimit: 25_000_000,
						ArtifactFingerprint: &spendProgressArtifactFingerprint{
							StatusField:            gate.DefaultArtifactStatusField,
							Status:                 "recut",
							DeliverableFingerprint: artifactDeliverableFingerprint,
						},
					},
				}),
			}},
			wantAccepted:     true,
			wantReason:       "artifact_status_changed",
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:             "artifact output transition resets spend",
			billingMode:      "subscription",
			tokenLimit:       25_000_000,
			deliverableKind:  workflowconfig.DeliverableArtifact,
			artifactEvidence: runpkg.ArtifactProgressEvidence{Available: true, CurrentFiles: 2, CurrentFingerprint: "new-output"},
			history: []store.WorkAttempt{{
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: spendProgressRecord{
						TokenLimit: 25_000_000,
						ArtifactFingerprint: &spendProgressArtifactFingerprint{
							StatusField:       gate.DefaultArtifactStatusField,
							OutputFiles:       1,
							OutputFingerprint: "old-output",
						},
					},
				}),
			}},
			wantAccepted:     true,
			wantReason:       "artifact_output_changed",
			wantHistoryCalls: 1,
			wantSince:        base,
		},
		{
			name:             "artifact without evidence still parks",
			billingMode:      "subscription",
			tokenLimit:       25_000_000,
			deliverableKind:  workflowconfig.DeliverableArtifact,
			spend:            store.IssueSpendSince{TotalTokens: 25_000_000, Sessions: 5},
			wantBlock:        true,
			wantBlockedBy:    "tokens",
			wantSpendCalls:   1,
			wantHistoryCalls: 1,
			wantSince:        createdAt,
			wantCase:         spendProgressCaseNoArtifact,
		},
		{name: "xhigh threshold allows one expensive session", limit: 3, effort: "xhigh", spend: store.IssueSpendSince{CostUSD: 17.99, Sessions: 1}, wantLimit: 18, wantSpendCalls: 1, wantHistoryCalls: 1, wantSince: createdAt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spend := &spendProgressStore{result: tt.spend}
			if !tt.creditAt.IsZero() {
				spend.credit = store.IssueProgressCredit{CreditedAt: tt.creditAt}
			}
			attempts := &implementProgressAttemptStore{history: tt.history}
			billingMode := tt.billingMode
			if billingMode == "" {
				billingMode = "metered"
			}
			orch := &Orchestrator{
				cfg: Config{
					Project:                 scheduler.ProjectCandidate{ID: "detent"},
					BillingMode:             billingMode,
					NoProgressTokenLimit:    tt.tokenLimit,
					NoProgressSpendLimitUSD: tt.limit,
					DeliverableKind:         tt.deliverableKind,
				},
				progressSpend: spend,
				workAttempts:  attempts,
			}
			issue := tt.issue
			issue.ID = "issue-214"
			issue.Identifier = "gopherguides/gopher-ai#214"
			issue.CreatedAt = &createdAt
			running := Running{Issue: issue, DeliverableKind: tt.deliverableKind, ArtifactEvidence: tt.artifactEvidence}
			running.RuntimeIdentity.ReasoningEffort.Value = tt.effort
			decision := orch.evaluateSpendProgress(context.Background(), running, base, tt.accepted, tt.acceptedReason)

			if decision.Block != tt.wantBlock {
				t.Fatalf("Block = %t, want %t", decision.Block, tt.wantBlock)
			}
			if decision.BlockedBy != tt.wantBlockedBy {
				t.Fatalf("BlockedBy = %q, want %q", decision.BlockedBy, tt.wantBlockedBy)
			}
			if decision.AcceptedStateChange != tt.wantAccepted {
				t.Fatalf("AcceptedStateChange = %t, want %t", decision.AcceptedStateChange, tt.wantAccepted)
			}
			if decision.AcceptedReason != tt.wantReason {
				t.Fatalf("AcceptedReason = %q, want %q", decision.AcceptedReason, tt.wantReason)
			}
			if tt.wantCase != "" && decision.Case != tt.wantCase {
				t.Fatalf("Case = %q, want %q", decision.Case, tt.wantCase)
			}
			if math.Abs(decision.LimitUSD-tt.wantLimit) > 0.000001 {
				t.Fatalf("LimitUSD = %f, want %f", decision.LimitUSD, tt.wantLimit)
			}
			if spend.calls != tt.wantSpendCalls {
				t.Fatalf("spend calls = %d, want %d", spend.calls, tt.wantSpendCalls)
			}
			if attempts.historyCalls != tt.wantHistoryCalls {
				t.Fatalf("history calls = %d, want %d", attempts.historyCalls, tt.wantHistoryCalls)
			}
			if !decision.Since.Equal(tt.wantSince) {
				t.Fatalf("Since = %s, want %s", decision.Since, tt.wantSince)
			}
			if spend.calls == 1 && !spend.query.Since.Equal(tt.wantSince) {
				t.Fatalf("query since = %s, want %s", spend.query.Since, tt.wantSince)
			}
		})
	}
}

func TestSpendProgressBaselineIgnoresLaneTransitions(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)
	stageUpdatedAt := createdAt.Add(30 * time.Minute)
	acceptedAt := createdAt.Add(45 * time.Minute)
	tests := []struct {
		name     string
		attempts []store.WorkAttempt
		want     time.Time
	}{
		{name: "lane transition only", want: createdAt},
		{
			name: "accepted work product progress",
			attempts: []store.WorkAttempt{{
				CompletedAt: acceptedAt,
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
					spendProgressMetadataKey: spendProgressRecord{AcceptedStateChange: true},
				}),
			}},
			want: acceptedAt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := connector.Issue{CreatedAt: &createdAt, StageUpdatedAt: &stageUpdatedAt}
			if got := spendProgressBaseline(issue, tt.attempts); !got.Equal(tt.want) {
				t.Fatalf("spendProgressBaseline() = %s, want %s", got, tt.want)
			}
		})
	}
}

func spendProgressIssueWithPR(headSHA string, mergeableState string, ciStatus string) connector.Issue {
	number := 214
	return connector.Issue{
		PRNumber: &number,
		PullRequest: &connector.PullRequest{
			Number:         214,
			HeadSHA:        headSHA,
			MergeableState: mergeableState,
			CIStatus:       ciStatus,
		},
	}
}

func TestSpendProgressCommentNamesEvidenceCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		decision     spendProgressDecision
		wantContains []string
	}{
		{
			name: "without PR evidence",
			decision: spendProgressDecision{
				Spend:     store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
				LimitUSD:  5,
				Case:      spendProgressCaseNoPR,
				BlockedBy: "usd",
			},
			wantContains: []string{"resource consumption continued without any PR evidence", "case: spend_without_pr_evidence", "pr_evidence_checked: issue PR linkage, hydrated PR metadata, and tracker closing references including Fixes #N", "Shrink the task"},
		},
		{
			name: "static PR evidence",
			decision: spendProgressDecision{
				Spend:         store.IssueSpendSince{CostUSD: 6.75, Sessions: 2},
				LimitUSD:      5,
				Case:          spendProgressCaseStatic,
				BlockedBy:     "usd",
				PRFingerprint: &spendProgressPRFingerprint{Number: 214, HeadSHA: "same-head", MergeableState: "dirty", CIStatus: "failure"},
			},
			wantContains: []string{"resource consumption continued while a linked PR existed but could not merge", "case: spend_with_static_pr_evidence", "pr_evidence_checked: issue PR linkage, hydrated PR metadata, and tracker closing references including Fixes #N", "merge-train capacity", "pr_head_sha: same-head"},
		},
		{
			name: "static artifact evidence",
			decision: spendProgressDecision{
				Spend:           store.IssueSpendSince{TotalTokens: 33_152_887, Sessions: 6},
				TokenLimit:      25_000_000,
				Case:            spendProgressCaseStaticArtifact,
				BlockedBy:       "tokens",
				DeliverableKind: workflowconfig.DeliverableArtifact,
				EvidenceChecked: []string{"completion receipt", "artifact status field render_status", "files under the configured artifact output root"},
				ArtifactFingerprint: &spendProgressArtifactFingerprint{
					ReceiptHash:       "receipt-hash",
					StatusField:       "render_status",
					Status:            "pending_review",
					OutputFiles:       2,
					OutputFingerprint: "output-hash",
				},
			},
			wantContains: []string{"resource consumption continued while artifact evidence stayed static", "case: spend_with_static_artifact_evidence", "artifact_evidence_checked: completion receipt, artifact status field render_status, files under the configured artifact output root", "artifact_status_field: render_status", "artifact_receipt_hash: receipt-hash", "artifact_output_fingerprint: output-hash"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			comment := spendProgressComment(connector.Issue{Identifier: "digitaldrywood/detent#1276"}, tt.decision)
			for _, want := range tt.wantContains {
				if !strings.Contains(comment, want) {
					t.Fatalf("comment missing %q:\n%s", want, comment)
				}
			}
			if tt.decision.DeliverableKind == workflowconfig.DeliverableArtifact && strings.Contains(comment, "pr_evidence_checked:") {
				t.Fatalf("artifact comment contains PR evidence signal:\n%s", comment)
			}
			if recovery := spendProgressRecoveryReason(tt.decision); recovery == "" {
				t.Fatal("recovery reason is empty")
			}
			if handoff := spendProgressRetryHandoff(tt.decision); handoff.MissingSignal == "" {
				t.Fatal("retry handoff missing signal")
			}
		})
	}
}

func TestRefreshSpendProgressIssue(t *testing.T) {
	t.Parallel()

	baseIssue := connector.Issue{ID: "issue-1276", Identifier: "digitaldrywood/detent#1276"}
	linkedIssue := spendProgressIssueWithPR("head", "dirty", "failure")
	linkedIssue.ID = baseIssue.ID
	linkedIssue.Identifier = baseIssue.Identifier
	linkedIssue.PRRepository = "digitaldrywood/detent"
	degradedIssue := cloneIssue(linkedIssue)
	degradedIssue.PullRequest.HydrationDegradedReason = connector.PullRequestHydrationReasonStaleCachedPullData
	fallbackTracker := &implementProgressConnector{refreshed: linkedIssue, hydrated: linkedIssue}

	tests := []struct {
		name        string
		orch        *Orchestrator
		issue       connector.Issue
		wantWarning string
		wantPR      bool
		wantHead    string
	}{
		{name: "missing connector", orch: &Orchestrator{}, issue: baseIssue, wantWarning: "refresh unavailable"},
		{name: "reference refresh failure", orch: &Orchestrator{connector: &implementProgressConnector{referenceErr: errors.New("github unavailable")}}, issue: baseIssue, wantWarning: "reference refresh failed"},
		{name: "fallback refresh failure", orch: &Orchestrator{connector: connectorOnly{Connector: &implementProgressConnector{refreshErr: errors.New("github unavailable")}}}, issue: baseIssue, wantWarning: "evidence refresh failed"},
		{name: "refresh confirms no PR", orch: &Orchestrator{connector: &implementProgressConnector{refreshed: baseIssue}}, issue: baseIssue},
		{
			name: "fallback refresh discovers and hydrates PR",
			orch: &Orchestrator{connector: spendProgressHydratingConnector{
				Connector: fallbackTracker,
				hydrator:  fallbackTracker,
			}},
			issue:    baseIssue,
			wantPR:   true,
			wantHead: "head",
		},
		{
			name: "closing reference discovers and hydrates PR",
			orch: &Orchestrator{connector: &implementProgressConnector{
				refreshed:  baseIssue,
				referenced: linkedIssue,
				hydrated:   linkedIssue,
			}},
			issue:    baseIssue,
			wantPR:   true,
			wantHead: "head",
		},
		{
			name:        "linked PR without hydrator",
			orch:        &Orchestrator{connector: connectorOnly{Connector: &implementProgressConnector{}}},
			issue:       linkedIssue,
			wantWarning: "hydrator unavailable",
			wantPR:      true,
			wantHead:    "head",
		},
		{
			name:        "hydration failure",
			orch:        &Orchestrator{connector: &implementProgressConnector{hydrateErr: errors.New("github unavailable")}},
			issue:       linkedIssue,
			wantWarning: "hydration failed",
			wantPR:      true,
			wantHead:    "head",
		},
		{
			name:        "degraded hydration",
			orch:        &Orchestrator{connector: &implementProgressConnector{hydrated: degradedIssue}},
			issue:       linkedIssue,
			wantWarning: connector.PullRequestHydrationReasonStaleCachedPullData,
			wantPR:      true,
			wantHead:    "head",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue, warning := tt.orch.refreshSpendProgressIssue(t.Context(), tt.issue)
			if !strings.Contains(warning, tt.wantWarning) {
				t.Fatalf("warning = %q, want containing %q", warning, tt.wantWarning)
			}
			if got := issue.PullRequest != nil; got != tt.wantPR {
				t.Fatalf("PR present = %t, want %t", got, tt.wantPR)
			}
			if tt.wantHead != "" && issue.PullRequest.HeadSHA != tt.wantHead {
				t.Fatalf("head = %q, want %q", issue.PullRequest.HeadSHA, tt.wantHead)
			}
			if tt.wantPR && issue.PRRepository == "" {
				t.Fatal("PRRepository is empty")
			}
		})
	}
}

type connectorOnly struct {
	connector.Connector
}

type spendProgressHydratingConnector struct {
	connector.Connector
	hydrator connector.PullRequestHydrator
}

func (c spendProgressHydratingConnector) HydratePullRequest(ctx context.Context, issue connector.Issue) (connector.Issue, error) {
	return c.hydrator.HydratePullRequest(ctx, issue)
}

func TestSpendProgressArtifactAdvance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous spendProgressArtifactFingerprint
		current  spendProgressArtifactFingerprint
		want     string
	}{
		{
			name:     "status transition",
			previous: spendProgressArtifactFingerprint{Status: "recut"},
			current:  spendProgressArtifactFingerprint{Status: "pending_review"},
			want:     "artifact_status_changed",
		},
		{
			name:     "status casing only",
			previous: spendProgressArtifactFingerprint{Status: "approved"},
			current:  spendProgressArtifactFingerprint{Status: "Approved"},
		},
		{
			name:     "all outputs deleted",
			previous: spendProgressArtifactFingerprint{OutputFiles: 1, OutputFingerprint: "with-output"},
			current:  spendProgressArtifactFingerprint{OutputFingerprint: "empty-output"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := spendProgressArtifactAdvance(&tt.previous, &tt.current); got != tt.want {
				t.Fatalf("spendProgressArtifactAdvance() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestImplementAcceptedStateChangeRequiresWorkProductProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		running    Running
		decision   implementCompletionProgressDecision
		want       bool
		wantReason string
	}{
		{name: "retry lane transition", running: Running{DispatchSourceState: "Todo", DispatchTargetState: "In Progress"}},
		{name: "rework lane transition", running: Running{DispatchSourceState: "Rework", DispatchTargetState: "In Progress"}},
		{name: "park lane transition", running: Running{DispatchSourceState: "In Progress", DispatchTargetState: "Blocked"}},
		{name: "pull request update", decision: implementCompletionProgressDecision{Reason: "pull_request_created_or_updated"}, want: true, wantReason: "pull_request_created_or_updated"},
		{name: "signature change", decision: implementCompletionProgressDecision{Reason: "signature_changed"}, want: true, wantReason: "signature_changed"},
		{name: "merged completion", decision: implementCompletionProgressDecision{Reason: implementMergedCompletionReason}, want: true, wantReason: implementMergedCompletionReason},
		{name: "operational reason without accepted kind", decision: implementCompletionProgressDecision{Reason: implementOperationalCompletion}},
		{name: "operational completion", decision: implementCompletionProgressDecision{Reason: string(AutoPromoteReasonOperationalCompletion), CompletionKind: workpad.CompletionOperational}, want: true, wantReason: string(AutoPromoteReasonOperationalCompletion)},
		{name: "artifact receipt", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact}, decision: implementCompletionProgressDecision{ProgressKinds: []string{"artifact_receipt"}}, want: true, wantReason: "artifact_receipt_changed"},
		{name: "artifact status", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact, DispatchArtifactStatus: "recut", ArtifactStatusField: "render_status"}, decision: implementCompletionProgressDecision{Issue: connector.Issue{Fields: map[string]string{"render_status": "pending_review"}}}, want: true, wantReason: "artifact_status_changed"},
		{name: "artifact status initialized", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact, ArtifactStatusField: "render_status"}, decision: implementCompletionProgressDecision{Issue: connector.Issue{Fields: map[string]string{"render_status": "recut"}}}, want: true, wantReason: "artifact_status_changed"},
		{name: "artifact output", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact, ArtifactEvidence: runpkg.ArtifactProgressEvidence{Available: true, InitialFiles: 1, CurrentFiles: 1, InitialFingerprint: "before", CurrentFingerprint: "after"}}, want: true, wantReason: "artifact_output_changed"},
		{name: "static artifact output", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact, ArtifactEvidence: runpkg.ArtifactProgressEvidence{Available: true, InitialFingerprint: "same", CurrentFingerprint: "same"}}},
		{name: "all artifact outputs deleted", running: Running{DeliverableKind: workflowconfig.DeliverableArtifact, ArtifactEvidence: runpkg.ArtifactProgressEvidence{Available: true, InitialFiles: 1, InitialFingerprint: "before", CurrentFingerprint: "empty-output"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, reason := implementAcceptedStateChange(tt.running, tt.decision)
			if got != tt.want {
				t.Fatalf("accepted = %t, want %t", got, tt.want)
			}
			if reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tt.wantReason)
			}
		})
	}
}

func TestOperationalCompletionSpendBreakerContract(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name            string
		authorized      bool
		authorizedLate  bool
		wantTerminal    store.WorkAttemptTerminalState
		wantState       string
		wantBlocked     bool
		wantAccepted    bool
		wantCompletion  string
		wantSpendCase   string
		wantAcceptedWhy string
	}{
		{
			name:            "preauthorized operational completion bypasses no PR breaker",
			authorized:      true,
			wantTerminal:    store.WorkAttemptTerminalSuccess,
			wantState:       "Done",
			wantAccepted:    true,
			wantCompletion:  workpad.CompletionOperational,
			wantAcceptedWhy: string(AutoPromoteReasonOperationalCompletion),
		},
		{
			name:          "undeclared operational assertion trips existing breaker",
			wantTerminal:  store.WorkAttemptTerminalNoProgress,
			wantState:     blockedStatusState,
			wantBlocked:   true,
			wantSpendCase: spendProgressCaseNoPR,
		},
		{
			name:           "authorization added at completion trips existing breaker",
			authorizedLate: true,
			wantTerminal:   store.WorkAttemptTerminalNoProgress,
			wantState:      blockedStatusState,
			wantBlocked:    true,
			wantSpendCase:  spendProgressCaseNoPR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			createdAt := base.Add(-time.Hour)
			recordedAt := base.Add(-time.Minute)
			issue := implementProgressIssueWithoutPR()
			issue.ID = "issue-operational"
			issue.Identifier = "digitaldrywood/leadpipe#62"
			issue.CreatedAt = &createdAt
			if tt.authorized {
				issue.Description = operationalCompletionAuthorizationBody()
			}
			issue.Comments = []connector.IssueComment{{Body: implementProgressStructuredWorkpad("in_progress", "", nil)}}
			refreshed := cloneIssue(issue)
			if tt.authorizedLate {
				refreshed.Description = operationalCompletionAuthorizationBody()
			}
			refreshed.Comments = []connector.IssueComment{{
				Body:      operationalCompletionWorkpadBody("Classifier-v4 host backfill completed and verified."),
				CreatedAt: &recordedAt,
			}}
			tracker := &implementProgressConnector{refreshed: refreshed}
			attempts := &implementProgressAttemptStore{}
			cfg := normalizeConfig(Config{
				Project:              scheduler.ProjectCandidate{ID: "leadpipe"},
				BillingMode:          "subscription",
				NoProgressTokenLimit: 25_000_000,
				AutoPromote: AutoPromoteConfig{
					Enabled:         true,
					NoProgressLimit: 0,
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				ObservedStates: []string{"Human Review", "Blocked"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: attempts,
				progressSpend: &spendProgressStore{result: store.IssueSpendSince{
					TotalTokens: 25_604_036,
					Sessions:    3,
				}},
			}
			state := newState(cfg)
			running := Running{
				Issue:            issue,
				Attempt:          3,
				WorkAttemptID:    7187,
				Mode:             runpkg.RunModeImplement,
				StartedAt:        base.Add(-time.Minute),
				DiffStats:        DiffStats{Status: "clean"},
				DispatchProgress: implementProgressArtifactSnapshotFromIssue(issue, true),
			}
			state.Running[issue.ID] = running
			state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: running.StartedAt}

			orch.handleRunResult(t.Context(), &state, runpkg.Completion{
				IssueID:     issue.ID,
				CompletedAt: base,
				Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
				Result: runpkg.RunResult{
					FinalState: FinalStateCompleted,
					DiffStats:  DiffStats{Status: "clean"},
				},
			})
			if tt.authorized {
				orch.transitionCompletedActiveIssuesToReview(t.Context(), &state, []connector.Issue{refreshed}, base.Add(time.Second))
			}

			if len(attempts.completions) != 1 {
				t.Fatalf("completions = %#v, want one", attempts.completions)
			}
			completion := attempts.completions[0]
			if completion.TerminalState != tt.wantTerminal {
				t.Fatalf("terminal state = %q, want %q", completion.TerminalState, tt.wantTerminal)
			}
			if len(tracker.updates) != 1 || tracker.updates[0].state != tt.wantState {
				t.Fatalf("updates = %#v, want state %q", tracker.updates, tt.wantState)
			}
			if _, blocked := state.Blocked[issue.ID]; blocked != tt.wantBlocked {
				t.Fatalf("blocked = %t, want %t", blocked, tt.wantBlocked)
			}
			progress := implementProgressRecordFromCompletion(t, completion)
			if progress.CompletionKind != tt.wantCompletion {
				t.Fatalf("completion kind = %q, want %q", progress.CompletionKind, tt.wantCompletion)
			}
			spend, ok := spendProgressRecordFromAttempt(store.WorkAttempt{WorkerMetadataJSON: completion.WorkerMetadataJSON})
			if !ok {
				t.Fatal("spend progress metadata missing")
			}
			if spend.AcceptedStateChange != tt.wantAccepted || spend.AcceptedReason != tt.wantAcceptedWhy || spend.Case != tt.wantSpendCase {
				t.Fatalf("spend progress = %#v", spend)
			}
		})
	}
}

func TestHandleRunResultTripsTokenProgressBreakerOnSubscription(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	createdAt := base.Add(-30 * time.Minute)
	issue := implementProgressIssueWithoutPR()
	issue.CreatedAt = &createdAt
	stageUpdatedAt := base.Add(-5 * time.Minute)
	issue.StageUpdatedAt = &stageUpdatedAt
	tracker := &implementProgressConnector{}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project:                 scheduler.ProjectCandidate{ID: "gopher-ai"},
		BillingMode:             "subscription",
		NoProgressTokenLimit:    25_000_000,
		NoProgressSpendLimitUSD: 5,
		AutoPromote:             AutoPromoteConfig{NoProgressLimit: 0},
		ActiveStates:            []string{"Todo", "In Progress", "Rework"},
		ObservedStates:          []string{"Blocked"},
		TerminalStates:          []string{"Done"},
	})
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		progressSpend: &spendProgressStore{result: store.IssueSpendSince{
			CostUSD:        6.75,
			TotalTokens:    25_000_000,
			Sessions:       5,
			FirstSessionAt: base.Add(-14 * time.Minute),
			LastSessionAt:  base,
		}},
	}
	state := newState(cfg)
	running := Running{
		Issue:               issue,
		Attempt:             5,
		WorkAttemptID:       42,
		Mode:                runpkg.RunModeImplement,
		StartedAt:           base.Add(-2 * time.Minute),
		DiffStats:           DiffStats{FilesChanged: 2, AddedLines: 10, RemovedLines: 3, Status: "dirty"},
		DispatchSourceState: "Rework",
		DispatchTargetState: "In Progress",
	}
	state.Running[issue.ID] = running
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: running.StartedAt}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: base,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			DiffStats:  running.DiffStats,
		},
	})

	blocked, ok := state.Blocked[issue.ID]
	if !ok || blocked.Reason != spendProgressReason {
		t.Fatalf("Blocked[%q] = %#v, want spend breaker", issue.ID, blocked)
	}
	if len(tracker.comments) != 3 {
		t.Fatalf("comments = %#v, want pending and applied park markers and explanation", tracker.comments)
	}
	for _, want := range []string{"blocked_by: tokens", "25000000", "usd_breaker: inert", "Shrink the task", "first tool action"} {
		if !strings.Contains(tracker.comments[2].body, want) {
			t.Fatalf("comment missing %q:\n%s", want, tracker.comments[2].body)
		}
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("completions = %#v, want one", attempts.completions)
	}
	completion := attempts.completions[0]
	if completion.TerminalState != store.WorkAttemptTerminalNoProgress || completion.ErrorClass != spendProgressReason {
		t.Fatalf("completion = %#v, want spend no-progress terminal", completion)
	}
	record, ok := spendProgressRecordFromAttempt(store.WorkAttempt{WorkerMetadataJSON: completion.WorkerMetadataJSON})
	if !ok || record.BlockReason != spendProgressReason || record.BlockedBy != "tokens" || record.TotalTokens != 25_000_000 {
		t.Fatalf("spend metadata = %#v, ok=%t", record, ok)
	}
	if !state.PriorAttempts[issue.ID].ExplainBeforeRetry {
		t.Fatalf("PriorAttempts[%q] = %#v, want explain-before-retry", issue.ID, state.PriorAttempts[issue.ID])
	}
}

func TestHandleRunResultAcceptsPRAdvanceBeforeWorkerError(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 11, 16, 0, 0, 0, time.UTC)
	createdAt := base.Add(-time.Hour)
	runningIssue := spendProgressIssueWithPR("old-head", "dirty", "failure")
	runningIssue.ID = "issue-1276"
	runningIssue.Identifier = "digitaldrywood/detent#1276"
	runningIssue.State = "In Progress"
	runningIssue.CreatedAt = &createdAt
	hydratedIssue := cloneIssue(runningIssue)
	hydratedIssue.PullRequest.HeadSHA = "new-head"
	tracker := &implementProgressConnector{hydrated: hydratedIssue}
	attempts := &implementProgressAttemptStore{history: []store.WorkAttempt{{
		CompletedAt: base.Add(-20 * time.Minute),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			spendProgressMetadataKey: map[string]any{
				"limit_usd":      5,
				"pr_fingerprint": map[string]any{"number": 214, "head_sha": "old-head", "mergeable_state": "dirty", "ci_status": "failure"},
			},
		}),
	}}}
	cfg := normalizeConfig(Config{
		Project:                 scheduler.ProjectCandidate{ID: "detent"},
		BillingMode:             "metered",
		NoProgressSpendLimitUSD: 5,
		ActiveStates:            []string{"In Progress"},
		ObservedStates:          []string{"Blocked"},
		TerminalStates:          []string{"Done"},
	})
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     tracker,
		workAttempts:  attempts,
		progressSpend: &spendProgressStore{result: store.IssueSpendSince{CostUSD: 6.75, Sessions: 2}},
	}
	state := newState(cfg)
	running := Running{
		Issue:               runningIssue,
		Attempt:             2,
		WorkAttemptID:       42,
		Mode:                runpkg.RunModeImplement,
		StartedAt:           base.Add(-5 * time.Minute),
		DispatchSourceState: "Rework",
		DispatchTargetState: "In Progress",
	}
	state.Running[runningIssue.ID] = running
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: running.StartedAt}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: base,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Err:         errors.New("session token ceiling exceeded"),
	})

	if _, blocked := state.Blocked[runningIssue.ID]; blocked {
		t.Fatalf("Blocked[%q] present after PR head advanced", runningIssue.ID)
	}
	if tracker.hydrations != 1 {
		t.Fatalf("hydrations = %d, want 1", tracker.hydrations)
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("completions = %#v, want one", attempts.completions)
	}
	record, ok := spendProgressRecordFromAttempt(store.WorkAttempt{WorkerMetadataJSON: attempts.completions[0].WorkerMetadataJSON})
	if !ok || !record.AcceptedStateChange || record.AcceptedReason != "pull_request_head_changed" {
		t.Fatalf("spend metadata = %#v, ok=%t", record, ok)
	}
}

func TestHandleRunResultDiscoversWorkerOpenedPRBeforeWorkerError(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	createdAt := base.Add(-8 * 24 * time.Hour)
	runningIssue := implementProgressIssueWithoutPR()
	runningIssue.CreatedAt = &createdAt
	linkedIssue := spendProgressIssueWithPR("new-head", "dirty", "pending")
	linkedIssue.ID = runningIssue.ID
	linkedIssue.Identifier = runningIssue.Identifier
	linkedIssue.PRRepository = "digitaldrywood/detent"
	linkedIssue.Title = runningIssue.Title
	linkedIssue.State = runningIssue.State
	linkedIssue.URL = runningIssue.URL
	linkedIssue.CreatedAt = runningIssue.CreatedAt
	tracker := &implementProgressConnector{
		refreshed:  runningIssue,
		referenced: linkedIssue,
		hydrated:   linkedIssue,
	}
	attempts := &implementProgressAttemptStore{}
	cfg := normalizeConfig(Config{
		Project:              scheduler.ProjectCandidate{ID: "detent"},
		BillingMode:          "subscription",
		NoProgressTokenLimit: 25_000_000,
		ActiveStates:         []string{"In Progress"},
		ObservedStates:       []string{"Blocked"},
		TerminalStates:       []string{"Done"},
	})
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     tracker,
		workAttempts:  attempts,
		progressSpend: &spendProgressStore{result: store.IssueSpendSince{TotalTokens: 46_465_472, Sessions: 4}},
	}
	state := newState(cfg)
	running := Running{
		Issue:               runningIssue,
		Attempt:             4,
		WorkAttemptID:       42,
		Mode:                runpkg.RunModeImplement,
		StartedAt:           base.Add(-10 * time.Minute),
		DispatchSourceState: "Rework",
		DispatchTargetState: "In Progress",
	}
	state.Running[runningIssue.ID] = running
	state.Claimed[runningIssue.ID] = Claimed{Issue: runningIssue, ClaimedAt: running.StartedAt}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: base,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Err:         errors.New("session token ceiling exceeded"),
	})

	if _, blocked := state.Blocked[runningIssue.ID]; blocked {
		t.Fatalf("Blocked[%q] present after linked PR discovery", runningIssue.ID)
	}
	if tracker.referenceRefreshes != 1 || tracker.hydrations != 1 {
		t.Fatalf("reference refreshes = %d, hydrations = %d, want 1 each", tracker.referenceRefreshes, tracker.hydrations)
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("completions = %#v, want one", attempts.completions)
	}
	record, ok := spendProgressRecordFromAttempt(store.WorkAttempt{WorkerMetadataJSON: attempts.completions[0].WorkerMetadataJSON})
	if !ok || !record.AcceptedStateChange || record.AcceptedReason != "pull_request_created" {
		t.Fatalf("spend metadata = %#v, ok=%t", record, ok)
	}
}

func TestSpendProgressUSDMessagesLabelNotionalValue(t *testing.T) {
	t.Parallel()

	decision := spendProgressDecision{
		BlockedBy: "usd",
		Spend:     store.IssueSpendSince{CostUSD: 6.75},
		LimitUSD:  5,
	}
	for name, value := range map[string]string{
		"block":   spendProgressBlockMessage(decision),
		"summary": spendProgressUsageSummary(decision),
	} {
		if !strings.Contains(value, "notional USD") {
			t.Fatalf("%s message = %q, want notional USD label", name, value)
		}
	}
}

func TestSpendProgressPriorAttemptRestoresExplainBeforeRetry(t *testing.T) {
	t.Parallel()

	attempt := store.WorkAttempt{WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
		spendProgressMetadataKey: spendProgressRecord{
			TotalTokens: 25_000_000,
			SpendUSD:    7.25,
			Sessions:    6,
			TokenLimit:  25_000_000,
			LimitUSD:    5,
			BlockedBy:   "tokens",
			BlockReason: spendProgressReason,
		},
	})}
	orch := &Orchestrator{
		cfg:          Config{Project: scheduler.ProjectCandidate{ID: "detent"}, BillingMode: "subscription", NoProgressTokenLimit: 25_000_000, NoProgressSpendLimitUSD: 5},
		workAttempts: &implementProgressAttemptStore{history: []store.WorkAttempt{attempt}},
	}
	prior, ok := orch.spendProgressPriorAttempt(context.Background(), connector.Issue{ID: "issue-1"})
	if !ok || !prior.ExplainBeforeRetry || prior.ObservedTokens != 25_000_000 || prior.NoProgressTokenLimit != 25_000_000 {
		t.Fatalf("prior = %#v, ok=%t", prior, ok)
	}
}

func TestEvaluateSpendProgressFailsOpen(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	issue := connector.Issue{ID: "issue-1"}
	tests := []struct {
		name     string
		attempts *implementProgressAttemptStore
		spend    *spendProgressStore
		missing  bool
		want     string
	}{
		{name: "missing spend store", attempts: &implementProgressAttemptStore{}, missing: true, want: "progress usage store unavailable"},
		{name: "history lookup failure", attempts: &implementProgressAttemptStore{historyErr: errors.New("history unavailable")}, spend: &spendProgressStore{}, want: "history unavailable"},
		{name: "credit lookup failure", attempts: &implementProgressAttemptStore{}, spend: &spendProgressStore{creditErr: errors.New("credit unavailable")}, want: "credit unavailable"},
		{name: "spend lookup failure", attempts: &implementProgressAttemptStore{}, spend: &spendProgressStore{err: errors.New("spend unavailable")}, want: "spend unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			orch := &Orchestrator{
				cfg:          Config{NoProgressTokenLimit: 25_000_000},
				workAttempts: tt.attempts,
				logger:       slog.New(slog.NewTextHandler(&logs, nil)),
			}
			if !tt.missing {
				orch.progressSpend = tt.spend
			}
			decision := orch.evaluateSpendProgress(t.Context(), Running{Issue: issue}, base, false, "")
			if decision.Block || !strings.Contains(decision.Warning, tt.want) || !strings.Contains(logs.String(), tt.want) {
				t.Fatalf("decision = %#v logs = %q, want warning %q", decision, logs.String(), tt.want)
			}
		})
	}
}

func TestSpendProgressAttemptAcceptedCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		record map[string]any
		want   bool
	}{
		{name: "native accepted record", record: map[string]any{spendProgressMetadataKey: spendProgressRecord{AcceptedStateChange: true}}, want: true},
		{name: "legacy pull request change", record: map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: string(store.WorkAttemptTerminalSuccess), Reason: "pull_request_created_or_updated"}}, want: true},
		{name: "legacy signature change", record: map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: string(store.WorkAttemptTerminalSuccess), Reason: "signature_changed"}}, want: true},
		{name: "operational completion", record: map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: string(store.WorkAttemptTerminalSuccess), Reason: implementOperationalCompletion, CompletionKind: workpad.CompletionOperational}}, want: true},
		{name: "operational reason without accepted kind", record: map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: string(store.WorkAttemptTerminalSuccess), Reason: implementOperationalCompletion}}},
		{name: "unaccepted record", record: map[string]any{implementProgressMetadataKey: implementProgressRecord{Outcome: string(store.WorkAttemptTerminalSuccess), Reason: "unchanged_signature_clean_diff"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			attempt := store.WorkAttempt{
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: marshalWorkAttemptJSON(tt.record),
			}
			if got := spendProgressAttemptAccepted(attempt); got != tt.want {
				t.Fatalf("spendProgressAttemptAccepted() = %t, want %t", got, tt.want)
			}
		})
	}
}

type spendProgressStore struct {
	result    store.IssueSpendSince
	err       error
	query     store.IssueSpendSinceQuery
	calls     int
	credit    store.IssueProgressCredit
	creditErr error
}

func (s *spendProgressStore) IssueSpendSince(_ context.Context, query store.IssueSpendSinceQuery) (store.IssueSpendSince, error) {
	s.calls++
	s.query = query
	return s.result, s.err
}

func (s *spendProgressStore) CreditIssueProgress(_ context.Context, identity store.IssueIdentity, at time.Time) (store.IssueProgressCredit, error) {
	return store.IssueProgressCredit{
		ProjectID:  identity.ProjectID,
		IssueID:    identity.IssueID,
		Identifier: identity.Identifier,
		IssueURL:   identity.IssueURL,
		CreditedAt: at,
	}, s.creditErr
}

func (s *spendProgressStore) IssueProgressCredit(context.Context, store.IssueIdentity) (store.IssueProgressCredit, error) {
	if s.creditErr != nil {
		return store.IssueProgressCredit{}, s.creditErr
	}
	if s.credit.CreditedAt.IsZero() {
		return store.IssueProgressCredit{}, store.ErrNotFound
	}
	return s.credit, nil
}
