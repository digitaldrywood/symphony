package orchestrator

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/explain"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestDependencyDeferralRefusalDetails(t *testing.T) {
	t.Parallel()
	const ref = "owner/repo#10"
	human := humanDependencyIssue("", true)
	readyHuman := humanDependencyIssue("Verified authentication", true)
	for _, tt := range []struct {
		name       string
		tracker    connector.Connector
		historyErr error
		ref        string
		want       []string
	}{
		{name: "ordinary blocker", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, State: "Rework"}}}, ref: ref, want: []string{"waiting on dependency " + ref}},
		{name: "missing resolver", tracker: struct{ connector.Connector }{memory.New(memory.Config{})}, ref: ref, want: []string{"dependency resolution unavailable", ref, "issue reference resolver unavailable"}},
		{name: "failed lookup", tracker: dependencyFailureTracker{Connector: memory.New(memory.Config{}), resolveErr: errors.New("tracker unavailable")}, ref: ref, want: []string{"dependency resolution unavailable", ref, "tracker unavailable"}},
		{name: "missing result", tracker: hydratingDispatchConnector{}, ref: ref, want: []string{"missing blocker result for " + ref}},
		{name: "missing identifier", tracker: hydratingDispatchConnector{}, want: []string{"no blocker identifiers"}},
		{name: "history unavailable", historyErr: errors.New("history offline"), want: []string{"dependency deferral history unavailable: history offline"}},
		{name: "human pending", tracker: hydratingDispatchConnector{blockers: []connector.Issue{human}}, ref: ref, want: []string{"waiting on human prerequisite " + ref, "closure and completion evidence required"}},
		{name: "human ready", tracker: hydratingDispatchConnector{blockers: []connector.Issue{readyHuman}}, ref: ref},
		{name: "terminal", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, State: "Done"}}}, ref: ref},
		{name: "closed", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, Closed: true}}}, ref: ref},
		{name: "merged", tracker: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: ref, PullRequest: &connector.PullRequest{State: "merged"}}}}, ref: ref},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := implementProgressIssueWithoutPR()
			attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{implementProgressDependencyDeferralHistoryAttempt(1, tt.ref, "Todo")}, historyErr: tt.historyErr}
			o := &Orchestrator{cfg: normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}, TerminalStates: []string{"Done"}}), connector: tt.tracker, workAttempts: attempts}
			filtered := o.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue})
			if len(tt.want) == 0 {
				if len(filtered) != 1 || len(attempts.decisions) != 0 {
					t.Fatalf("ready dependency: candidates=%+v refusals=%+v", filtered, attempts.decisions)
				}
				return
			}
			if len(filtered) != 0 || len(attempts.decisions) != 1 {
				t.Fatalf("suppression: candidates=%+v refusals=%+v", filtered, attempts.decisions)
			}
			decision := attempts.decisions[0]
			if decision.Reason != dispatchSkipBlockedByDependency || decision.Result != store.SchedulerDecisionResultSkipped || decision.Selected || decision.IssueID != issue.ID {
				t.Fatalf("refusal=%+v", decision)
			}
			for _, detail := range tt.want {
				if !strings.Contains(decision.WaitReason, detail) {
					t.Fatalf("detail=%q, want %q", decision.WaitReason, detail)
				}
			}
		})
	}
}

func TestDependencyDeferralEvidenceAndReleaseAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "detent.db")
	cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}, TerminalStates: []string{"Done"}})
	issue := implementProgressIssueWithoutPR()
	history := implementProgressDependencyDeferralHistoryAttempt(1, "digitaldrywood/detent#134", "Todo")
	backend, err := store.Open(ctx, store.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	attempts := backend.(store.WorkAttemptStore)
	id, err := attempts.StartWorkAttempt(ctx, store.WorkAttemptStart{ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, WorkerType: "agent", Lane: issue.State, StartedAt: history.CompletedAt.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if err := attempts.CompleteWorkAttempt(ctx, store.WorkAttemptCompletion{AttemptID: id, CompletedAt: history.CompletedAt, Status: store.WorkAttemptStatusTerminal, TerminalState: store.WorkAttemptTerminalSuccess, WorkerMetadataJSON: history.WorkerMetadataJSON}); err != nil {
		t.Fatal(err)
	}
	o := &Orchestrator{cfg: cfg, workAttempts: attempts}
	o.recordSchedulerDecision(ctx, nil, history.CompletedAt.Add(time.Minute), dispatchPlanDecision{Issue: issue, Selected: true}, string(store.SchedulerDecisionResultSelected), "selected")
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, state   string
		wantCandidate bool
	}{
		{name: "restart suppresses open dependency", state: "Todo"},
		{name: "restart releases completed dependency", state: "Done", wantCandidate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reopened, err := store.Open(ctx, store.Config{Path: path})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			o := &Orchestrator{cfg: cfg, workAttempts: reopened.(store.WorkAttemptStore), connector: hydratingDispatchConnector{blockers: []connector.Issue{{Identifier: "digitaldrywood/detent#134", State: tt.state}}}}
			reader := reopened.(store.IssueSchedulerDecisionStore)
			service := explain.New(explain.Dependencies{Scheduler: reader})
			query := explain.Query{ProjectID: "detent", IssueID: issue.ID}
			if tt.wantCandidate {
				explanation, err := service.Explain(ctx, query)
				if err != nil || explanation.Eligibility.State != explain.EligibilityRefused {
					t.Fatalf("persisted refusal after restart=%+v, error=%v", explanation.Eligibility, err)
				}
			}
			filtered := o.filterImplementDependencyDeferrals(ctx, []connector.Issue{issue})
			if (len(filtered) == 1) != tt.wantCandidate {
				t.Fatalf("candidates=%+v", filtered)
			}
			decisions, err := reader.ListIssueSchedulerDecisions(ctx, store.IssueSchedulerDecisionQuery{Identity: store.IssueIdentity{ProjectID: "detent", IssueID: issue.ID}})
			if err != nil || len(decisions) != 2 {
				t.Fatalf("decisions=%+v, error=%v", decisions, err)
			}
			explanation, err := service.Explain(ctx, query)
			if err != nil || explanation.Eligibility.State != explain.EligibilityRefused || explanation.Eligibility.Latest == nil {
				t.Fatalf("explanation=%+v, error=%v", explanation.Eligibility, err)
			}
			latest := explanation.Eligibility.Latest
			if latest.EvidenceID != fmt.Sprintf("scheduler:%d", decisions[0].ID) || latest.Reason != "waiting on dependency digitaldrywood/detent#134" {
				t.Fatalf("latest=%+v, decisions=%+v", latest, decisions)
			}
		})
	}
}
