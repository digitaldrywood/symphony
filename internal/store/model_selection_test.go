package store

import (
	"context"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
)

func TestSessionModelSelectionRoundTrip(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "fallback"}[fallback], func(t *testing.T) {
			ctx := context.Background()
			backend := openTestStore(t, ctx)
			now := time.Now().UTC()
			identity := agentidentity.Configured("codex", "codex", "default", "code", "gpt-5.6-sol", "", "medium", "", now)
			identity.Selection = agentidentity.Selection{Policy: "sol_first", Reason: "default_complexity", RequestedModel: "gpt-5.6-sol", ModelSource: "preset:normal_model", EffortSource: "preset:levels.normal.effort"}
			if fallback {
				identity.Selection.RequestedModel = "gpt-6-astra"
				identity.Selection.FallbackReason = "automatic model unavailable or retired"
			}
			id, err := backend.StartSession(ctx, SessionStart{ProjectID: "alpha", IssueID: "123", Identifier: "example/repo#123", StartedAt: now, RuntimeIdentity: identity})
			if err != nil {
				t.Fatal(err)
			}
			identity = identity.Merge(agentidentity.RuntimeUpdate("gpt-5.6-sol", "openai", "medium", "default", now))
			if err := backend.FinishSession(ctx, id, SessionFinish{CompletedAt: now.Add(time.Second), FinalState: "completed", ProviderThreadID: "thread-1", RuntimeIdentity: identity}); err != nil {
				t.Fatal(err)
			}
			resume, err := backend.LatestIssueAgentResumeState(ctx, IssueIdentity{ProjectID: "alpha", IssueID: "123"})
			if err != nil {
				t.Fatal(err)
			}
			if !resume.RuntimeIdentity.MateriallyEqual(identity) {
				t.Fatalf("resume identity = %+v, want %+v", resume.RuntimeIdentity, identity)
			}
			sessions, err := backend.(AIDebugSessionReader).ListIssueAIDebugSessions(ctx, IssueIdentity{ProjectID: "alpha", IssueID: "123"})
			if err != nil {
				t.Fatal(err)
			}
			if len(sessions) != 1 || sessions[0].RuntimeIdentity.Selection != identity.Selection {
				t.Fatalf("session provenance = %+v", sessions)
			}
		})
	}
}
