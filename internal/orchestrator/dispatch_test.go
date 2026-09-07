package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/budget"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/procgroup"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/staleness"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestRunningWorkAttemptMetadataJSONPersistsPullRequestFingerprint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		request  *connector.PullRequest
		wantPR   int64
		wantHead string
		wantBase string
	}{
		{name: "current pull request", request: &connector.PullRequest{Number: 42, HeadSHA: " head-current ", BaseSHA: " base-current "}, wantPR: 42, wantHead: "head-current", wantBase: "base-current"},
		{name: "no pull request"},
		{name: "missing base", request: &connector.PullRequest{HeadSHA: "head-current"}, wantHead: "head-current"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded := runningWorkAttemptMetadataJSON(
				Running{Issue: connector.Issue{PullRequest: tt.request}},
				map[string]any{"pr_number": 99, "pr_head_sha": "untrusted-head", "pr_base_sha": "untrusted-base"},
			)
			var metadata struct {
				PRNumber  int64  `json:"pr_number"`
				PRHeadSHA string `json:"pr_head_sha"`
				PRBaseSHA string `json:"pr_base_sha"`
			}
			if err := json.Unmarshal([]byte(encoded), &metadata); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if metadata.PRNumber != tt.wantPR || metadata.PRHeadSHA != tt.wantHead || metadata.PRBaseSHA != tt.wantBase {
				t.Fatalf("pull request fingerprint = %#v, want PR %d head %q base %q", metadata, tt.wantPR, tt.wantHead, tt.wantBase)
			}
		})
	}
}

func TestHandleRunUpdatePersistsRuntimeIdentityHeartbeat(t *testing.T) {
	t.Parallel()

	attempts := &recordingWorkAttemptStore{}
	o := &Orchestrator{cfg: normalizeConfig(Config{}), workAttempts: attempts}
	state := newState(normalizeConfig(Config{}))
	issue := connector.Issue{ID: "issue-1118", Identifier: "digitaldrywood/detent#1118", State: "In Progress"}
	state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: 42}
	state.WorkAttempts = []telemetry.WorkAttempt{{AttemptID: 42}}
	identity := agentidentity.Configured("codex-high", "codex", "high", "code", "gpt-5.5", "", "", "", time.Time{}).
		Merge(agentidentity.RuntimeUpdate("gpt-5.6-sol", "openai", "xhigh", "priority", time.Time{}))
	at := time.Date(2026, 7, 9, 20, 0, 0, 0, time.UTC)
	processStartedAt := at.Add(-time.Second)

	o.handleRunUpdate(&state, runUpdate{
		issueID: issue.ID,
		usage: runpkg.UsageUpdate{
			DetentSessionID: 1118,
			SessionID:       "thread-1118-turn-1",
			WorkerProcess: procgroup.Identity{
				PID:       4242,
				GroupID:   4242,
				StartedAt: processStartedAt,
			},
			LastEventAt:       at,
			LastEvent:         "runtime_identity",
			RuntimeIdentity:   identity,
			WorkProductPushed: true,
		},
	})

	if len(attempts.heartbeats) != 1 {
		t.Fatalf("heartbeats = %#v, want immediate durable identity heartbeat", attempts.heartbeats)
	}
	heartbeat := attempts.heartbeats[0]
	if heartbeat.DetentSessionID != 1118 || heartbeat.ProviderSessionID != "thread-1118-turn-1" || !heartbeat.RuntimeIdentity.MateriallyEqual(identity) {
		t.Fatalf("heartbeat = %#v, want correlated runtime identity", heartbeat)
	}
	if !state.WorkAttempts[0].RuntimeIdentity.MateriallyEqual(identity) {
		t.Fatalf("snapshot work attempt identity = %#v, want runtime identity", state.WorkAttempts[0].RuntimeIdentity)
	}
	if running := state.Running[issue.ID]; running.WorkerProcess.PID != 4242 || !running.WorkerProcess.StartedAt.Equal(processStartedAt) {
		t.Fatalf("running worker process = %#v, want persisted process identity", running.WorkerProcess)
	}
	if !terminalRetryMetadataPushed(heartbeat.WorkerMetadataJSON) {
		t.Fatalf("heartbeat WorkerMetadataJSON = %q, want pushed work product", heartbeat.WorkerMetadataJSON)
	}
}

func TestRecoverDurableWorkAttemptsRestoresMostRecentRuntimeIdentity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 22, 0, 0, 0, time.UTC)
	issue := connector.Issue{ID: "issue-1118", Identifier: "digitaldrywood/detent#1118", State: "Human Review"}
	newest := agentidentity.Configured("claude-local", "claude_code", "local", "validator", "fable", "ollama", "high", "", now.Add(-time.Hour)).
		Merge(agentidentity.RuntimeUpdate("qwen3-coder", "", "", "", now.Add(-time.Hour)))
	older := agentidentity.Configured("codex-old", "codex", "default", "code", "gpt-5.5", "", "", "", now.Add(-2*time.Hour))
	attempts := &recordingWorkAttemptStore{recent: []store.WorkAttempt{
		{ID: 2, ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, Status: store.WorkAttemptStatusTerminal, RuntimeIdentity: newest},
		{ID: 1, ProjectID: "detent", IssueID: issue.ID, Identifier: issue.Identifier, Status: store.WorkAttemptStatusTerminal, RuntimeIdentity: older},
	}}
	cfg := normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}})
	o := &Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	state.BoardIssues = []connector.Issue{issue}

	o.recoverDurableWorkAttempts(context.Background(), &state, now)

	if len(state.WorkAttempts) != 2 || state.WorkAttempts[0].AttemptID != 2 || state.WorkAttempts[1].AttemptID != 1 {
		t.Fatalf("recovered attempts = %#v, want newest first", state.WorkAttempts)
	}
	snapshot := state.Snapshot(now)
	if len(snapshot.BoardIssues) != 1 || !snapshot.BoardIssues[0].RuntimeIdentity.MateriallyEqual(newest) {
		t.Fatalf("recovered board identity = %#v, want newest persisted identity", snapshot.BoardIssues)
	}
}

func TestRecoverDurableWorkAttemptsReleasesLegacyTerminalCapacityActions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	issue := claimTestIssue("issue-1430")
	issue.Fields["Detent Lease"] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	claimStore := newClaimTestStore([]connector.Issue{issue})
	attempts := &recordingWorkAttemptStore{pendingCapacityReleases: []store.WorkAttempt{{
		ID:            1430,
		ProjectID:     "detent",
		IssueID:       issue.ID,
		Identifier:    issue.Identifier,
		Status:        store.WorkAttemptStatusTerminal,
		CompletedAt:   now.Add(-30 * time.Minute),
		TerminalState: store.WorkAttemptTerminalSuccess,
		NextAction:    "release capacity",
	}}}
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))
	cfg.Project = scheduler.ProjectCandidate{ID: "detent"}
	o := &Orchestrator{
		cfg:          cfg,
		connector:    claimTestConnector{store: claimStore, login: "alpha"},
		workAttempts: attempts,
	}
	state := newState(cfg)

	o.recoverDurableWorkAttempts(t.Context(), &state, now)

	if got := claimStore.issue(issue.ID).Fields["Detent Lease"]; got != "" {
		t.Fatalf("Detent Lease = %q, want released", got)
	}
	if len(attempts.clearedCapacityReleases) != 1 || attempts.clearedCapacityReleases[0] != 1430 {
		t.Fatalf("cleared capacity releases = %v, want [1430]", attempts.clearedCapacityReleases)
	}
}

func TestRecoverDurableWorkAttemptsRetainsCapacityActionWhenReleaseFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	issue := claimTestIssue("issue-1430-failure")
	wantLease := now.Add(-time.Hour).Format(time.RFC3339Nano)
	issue.Fields["Detent Lease"] = wantLease
	claimStore := newClaimTestStore([]connector.Issue{issue})
	attempts := &recordingWorkAttemptStore{pendingCapacityReleases: []store.WorkAttempt{{
		ID:            1431,
		ProjectID:     "detent",
		IssueID:       issue.ID,
		Identifier:    issue.Identifier,
		Status:        store.WorkAttemptStatusTerminal,
		CompletedAt:   now.Add(-30 * time.Minute),
		TerminalState: store.WorkAttemptTerminalSuccess,
		NextAction:    "release capacity",
	}}}
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))
	cfg.Project = scheduler.ProjectCandidate{ID: "detent"}
	o := &Orchestrator{
		cfg: cfg,
		connector: failingCapacityReleaseConnector{
			claimTestConnector: claimTestConnector{store: claimStore, login: "alpha"},
			err:                errors.New("tracker unavailable"),
		},
		workAttempts: attempts,
	}
	state := newState(cfg)

	o.recoverDurableWorkAttempts(t.Context(), &state, now)

	if got := claimStore.issue(issue.ID).Fields["Detent Lease"]; got != wantLease {
		t.Fatalf("Detent Lease = %q, want pending lease %q", got, wantLease)
	}
	if len(attempts.clearedCapacityReleases) != 0 {
		t.Fatalf("cleared capacity releases = %v, want none", attempts.clearedCapacityReleases)
	}
	foundFailure := false
	for _, event := range state.RecentEvents {
		if event.Event == "work_attempt_capacity_release_failed" {
			foundFailure = true
			break
		}
	}
	if !foundFailure {
		t.Fatalf("RecentEvents = %#v, want capacity release failure", state.RecentEvents)
	}
}

func TestRecoverDurableWorkAttemptsPreservesNewerClaimLease(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	issue := claimTestIssue("issue-1430-newer-lease")
	wantLease := now.Add(-10 * time.Minute).Format(time.RFC3339Nano)
	issue.Fields["Detent Lease"] = wantLease
	claimStore := newClaimTestStore([]connector.Issue{issue})
	attempts := &recordingWorkAttemptStore{pendingCapacityReleases: []store.WorkAttempt{{
		ID:            1432,
		ProjectID:     "detent",
		IssueID:       issue.ID,
		Identifier:    issue.Identifier,
		Status:        store.WorkAttemptStatusTerminal,
		CompletedAt:   now.Add(-30 * time.Minute),
		TerminalState: store.WorkAttemptTerminalSuccess,
		NextAction:    "release capacity",
	}}}
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))
	cfg.Project = scheduler.ProjectCandidate{ID: "detent"}
	o := &Orchestrator{
		cfg:          cfg,
		connector:    claimTestConnector{store: claimStore, login: "alpha"},
		workAttempts: attempts,
	}
	state := newState(cfg)

	o.recoverDurableWorkAttempts(t.Context(), &state, now)

	if got := claimStore.issue(issue.ID).Fields["Detent Lease"]; got != wantLease {
		t.Fatalf("Detent Lease = %q, want newer lease %q preserved", got, wantLease)
	}
	if len(attempts.clearedCapacityReleases) != 1 || attempts.clearedCapacityReleases[0] != 1432 {
		t.Fatalf("cleared capacity releases = %v, want stale action [1432] acknowledged", attempts.clearedCapacityReleases)
	}
}

func TestConfigFromWorkflowIncludesDispatchControls(t *testing.T) {
	t.Parallel()

	perHost := 2
	cfg := workflowconfig.Default()
	cfg.Worker.SSHHosts = []string{"worker-a", "worker-b"}
	cfg.Worker.MaxConcurrentAgentsPerHost = &perHost
	cfg.Workspace.CleanupIdleTTLMS = 7200000
	cfg.Workspace.CleanupSweepIntervalMS = 120000
	cfg.Budget.RefusalCooldownSeconds = 45
	cfg.Budget.BillingMode = workflowconfig.BillingModeSubscription
	cfg.Agent.AutoPromote.Enabled = true
	cfg.Agent.AutoPromote.QuietSeconds = 30
	cfg.Agent.AutoPromote.OptoutLabel = " Requires-Human-Review "
	cfg.Agent.AutoPromote.AllowedIssueLabels = []string{" Docs ", "docs", "Chore"}
	cfg.Agent.AutoPromote.GateWaitState = " Review "
	cfg.Agent.AutoPromote.GateWaitTimeoutSeconds = 900
	cfg.Agent.AutoPromote.ReworkLimit = 2
	cfg.Agent.OverloadRetryDelayMS = 60000
	cfg.Agent.MergeWorkerStartupTimeoutMS = 180000
	cfg.Agent.MergeWorkerMaxDurationMS = 7200000
	cfg.Agent.MergeFastPath.FairnessAgeSeconds = 5400
	cfg.Observability.StrandedActiveThresholdSeconds = 42
	cfg.Deliverable.MergeMethod = workflowconfig.MergeMethodRebase
	cfg.Deliverable.Kind = workflowconfig.DeliverableArtifact
	cfg.Agent.OutputTruncation.MaxBytes = 4096
	cfg.Agent.LifetimeSessionLimit = 21
	cfg.Agent.LifetimeTokenLimit = 52_000_000
	cfg.Agent.LifetimeLimitCooldownSeconds = 7200
	cfg.Agent.LifetimeLimitOverrideLabel = " Allow-Hard-Issue "
	cfg.Identity.Name = "release-captain"
	cfg.Identity.GitHubLogin = "detent-bot"
	cfg.Identity.AssigneeRequired = true
	cfg.Tracker.Authorization = selector.Selector{
		AssigneeIn: []string{"@me"},
	}
	cfg.Tracker.Claims.Enabled = true
	cfg.Tracker.Claims.LeaseField = "Detent Lease"
	cfg.Tracker.Claims.TTLSeconds = 300
	cfg.Tracker.Claims.HeartbeatSeconds = 45
	cfg.Tracker.BlockedRecovery = workflowconfig.BlockedRecovery{
		Enabled:                true,
		SourceStates:           []string{"Blocked", "Parked"},
		TargetState:            "Repair",
		ReasonCodes:            []string{"merge_conflict", "stale_base"},
		BreakerCooldownSeconds: 12 * 60 * 60,
	}
	cfg.Tracker.PriorityMap = workflowconfig.StringOrMap{IsMap: true, Map: map[string]any{"Critical": 1, "Normal": 3, "No priority": nil}}
	staggerSeconds := 20
	cfg.Gate = gate.Config{
		Kind:                         gate.KindHumanReview,
		ApprovalLabel:                " Approved-By-Human ",
		CITriggerLabel:               " CI:Ready ",
		CITriggerLabelStaggerSeconds: &staggerSeconds,
	}

	got := ConfigFromWorkflow(cfg)
	if got.DeliverableKind != workflowconfig.DeliverableArtifact {
		t.Fatalf("DeliverableKind = %q, want artifact", got.DeliverableKind)
	}
	if got.StopRunPriorityNames[1] != "Critical" || got.StopRunPriorityNames[3] != "Normal" || len(got.StopRunPriorityNames) != 2 {
		t.Fatalf("StopRunPriorityNames = %#v, want configured ranked options", got.StopRunPriorityNames)
	}

	if got.MaxConcurrentAgentsPerHost != 2 {
		t.Fatalf("MaxConcurrentAgentsPerHost = %d, want 2", got.MaxConcurrentAgentsPerHost)
	}
	if got.OverloadRetryDelay != time.Minute {
		t.Fatalf("OverloadRetryDelay = %s, want 1m", got.OverloadRetryDelay)
	}
	if got.MergeWorkerStartupTimeout != 3*time.Minute {
		t.Fatalf("MergeWorkerStartupTimeout = %s, want 3m", got.MergeWorkerStartupTimeout)
	}
	if got.MergeWorkerMaxDuration != 2*time.Hour {
		t.Fatalf("MergeWorkerMaxDuration = %s, want 2h", got.MergeWorkerMaxDuration)
	}
	if got.StrandedActiveThreshold != 42*time.Second {
		t.Fatalf("StrandedActiveThreshold = %s, want 42s", got.StrandedActiveThreshold)
	}
	if got.NoProgressTokenLimit != workflowconfig.DefaultNoProgressTokenLimit {
		t.Fatalf("NoProgressTokenLimit = %d, want %d", got.NoProgressTokenLimit, workflowconfig.DefaultNoProgressTokenLimit)
	}
	if len(got.WorkerHosts) != 2 || got.WorkerHosts[0] != "worker-a" || got.WorkerHosts[1] != "worker-b" {
		t.Fatalf("WorkerHosts = %#v, want worker-a and worker-b", got.WorkerHosts)
	}
	if got.BudgetRefusalCooldown != 45*time.Second {
		t.Fatalf("BudgetRefusalCooldown = %s, want 45s", got.BudgetRefusalCooldown)
	}
	if got.BillingMode != workflowconfig.BillingModeSubscription {
		t.Fatalf("BillingMode = %q, want subscription", got.BillingMode)
	}
	if !got.BlockedRecovery.Enabled ||
		!slices.Equal(got.BlockedRecovery.SourceStates, []string{"blocked", "parked"}) ||
		got.BlockedRecovery.TargetState != "Repair" ||
		!slices.Equal(got.BlockedRecovery.ReasonCodes, []string{"merge_conflict", "stale_base"}) ||
		got.BlockedRecovery.BreakerCooldown != 12*time.Hour {
		t.Fatalf("BlockedRecovery = %#v, want configured recovery policy", got.BlockedRecovery)
	}
	if got.WorkspaceCleanupIdleTTL != 2*time.Hour {
		t.Fatalf("WorkspaceCleanupIdleTTL = %s, want 2h0m0s", got.WorkspaceCleanupIdleTTL)
	}
	if got.WorkspaceCleanupSweepInterval != 2*time.Minute {
		t.Fatalf("WorkspaceCleanupSweepInterval = %s, want 2m0s", got.WorkspaceCleanupSweepInterval)
	}
	if !got.AutoPromote.Enabled {
		t.Fatal("AutoPromote.Enabled = false, want true")
	}
	if got.AutoPromote.QuietDuration != 30*time.Second {
		t.Fatalf("AutoPromote.QuietDuration = %s, want 30s", got.AutoPromote.QuietDuration)
	}
	if got.AutoPromote.OptoutLabel != "requires-human-review" {
		t.Fatalf("AutoPromote.OptoutLabel = %q, want requires-human-review", got.AutoPromote.OptoutLabel)
	}
	if len(got.AutoPromote.AllowedIssueLabels) != 2 ||
		got.AutoPromote.AllowedIssueLabels[0] != "docs" ||
		got.AutoPromote.AllowedIssueLabels[1] != "chore" {
		t.Fatalf("AutoPromote.AllowedIssueLabels = %#v, want docs and chore", got.AutoPromote.AllowedIssueLabels)
	}
	if got.AutoPromote.GateWaitState != autoPromoteGateWaitReview {
		t.Fatalf("AutoPromote.GateWaitState = %q, want review", got.AutoPromote.GateWaitState)
	}
	if got.AutoPromote.GateWaitTimeout != 15*time.Minute {
		t.Fatalf("AutoPromote.GateWaitTimeout = %s, want 15m0s", got.AutoPromote.GateWaitTimeout)
	}
	if got.AutoPromote.ReworkLimit != 2 {
		t.Fatalf("AutoPromote.ReworkLimit = %d, want 2", got.AutoPromote.ReworkLimit)
	}
	if !got.MergeFastPathEnabled {
		t.Fatal("MergeFastPathEnabled = false, want true")
	}
	if got.MergeFairnessAge != 90*time.Minute {
		t.Fatalf("MergeFairnessAge = %s, want 1h30m0s", got.MergeFairnessAge)
	}
	if got.MergeMethod != workflowconfig.MergeMethodRebase {
		t.Fatalf("MergeMethod = %q, want rebase", got.MergeMethod)
	}
	if got.OutputTruncationMaxBytes != 4096 {
		t.Fatalf("OutputTruncationMaxBytes = %d, want 4096", got.OutputTruncationMaxBytes)
	}
	if got.LifetimeSessionLimit != 21 || got.LifetimeTokenLimit != 52_000_000 {
		t.Fatalf("lifetime limits = %d sessions/%d tokens, want 21/52000000", got.LifetimeSessionLimit, got.LifetimeTokenLimit)
	}
	if got.LifetimeLimitCooldown != 2*time.Hour {
		t.Fatalf("LifetimeLimitCooldown = %s, want 2h", got.LifetimeLimitCooldown)
	}
	if got.LifetimeLimitOverrideLabel != "allow-hard-issue" {
		t.Fatalf("LifetimeLimitOverrideLabel = %q, want allow-hard-issue", got.LifetimeLimitOverrideLabel)
	}
	if got.SelectorContext.InstanceLogin != "detent-bot" {
		t.Fatalf("SelectorContext.InstanceLogin = %q, want detent-bot", got.SelectorContext.InstanceLogin)
	}
	if got.SelectorContext.Persona != "release-captain" {
		t.Fatalf("SelectorContext.Persona = %q, want release-captain", got.SelectorContext.Persona)
	}
	if len(got.Authorization.AssigneeIn) != 1 || got.Authorization.AssigneeIn[0] != "@me" {
		t.Fatalf("Authorization.AssigneeIn = %#v, want @me", got.Authorization.AssigneeIn)
	}
	if !got.Claiming.Enabled {
		t.Fatal("Claiming.Enabled = false, want true")
	}
	if !got.Claiming.OwnershipSet {
		t.Fatal("Claiming.OwnershipSet = false, want true")
	}
	if got.Claiming.OwnershipMode != workflowconfig.IdentityOwnershipAssignee {
		t.Fatalf("Claiming.OwnershipMode = %q, want assignee", got.Claiming.OwnershipMode)
	}
	if !got.Claiming.AssigneeRequired {
		t.Fatal("Claiming.AssigneeRequired = false, want true")
	}
	if got.Claiming.Owner != "release-captain" {
		t.Fatalf("Claiming.Owner = %q, want release-captain", got.Claiming.Owner)
	}
	if got.Claiming.AssigneeLogin != "detent-bot" {
		t.Fatalf("Claiming.AssigneeLogin = %q, want detent-bot", got.Claiming.AssigneeLogin)
	}
	if got.Claiming.LeaseField != "Detent Lease" {
		t.Fatalf("Claiming.LeaseField = %q, want Detent Lease", got.Claiming.LeaseField)
	}
	if got.Claiming.LeaseTTL != 300*time.Second {
		t.Fatalf("Claiming.LeaseTTL = %s, want 5m0s", got.Claiming.LeaseTTL)
	}
	if got.Claiming.HeartbeatInterval != 45*time.Second {
		t.Fatalf("Claiming.HeartbeatInterval = %s, want 45s", got.Claiming.HeartbeatInterval)
	}
	if got.AutoPromote.Gate.Kind != gate.KindHumanReview || got.AutoPromote.Gate.ApprovalLabel != "approved-by-human" {
		t.Fatalf("AutoPromote.Gate = %#v, want human_review approved-by-human", got.AutoPromote.Gate)
	}
	if got.AutoPromote.Gate.CITriggerLabel != "ci:ready" || got.AutoPromote.Gate.CITriggerLabelStaggerSeconds == nil || *got.AutoPromote.Gate.CITriggerLabelStaggerSeconds != 20 {
		t.Fatalf("AutoPromote.Gate trigger label = %#v, want ci:ready with 20s stagger", got.AutoPromote.Gate)
	}
}

func TestPlanDispatchSubscriptionRateWindowBackpressure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		billingMode string
		pacing      workflowconfig.RateWindowPacing
		limits      *telemetry.RateLimits
		want        int
	}{
		{name: "metered ignores provider window", billingMode: workflowconfig.BillingModeMetered, limits: providerRateLimits(20, 100), want: 10},
		{name: "subscription scales to primary remaining", billingMode: workflowconfig.BillingModeSubscription, limits: providerRateLimits(50, 100), want: 5},
		{name: "subscription uses lower secondary remaining", billingMode: workflowconfig.BillingModeSubscription, limits: providerPrimarySecondaryRateLimits(80, 30), want: 3},
		{name: "subscription preserves one soft slot at exhaustion", billingMode: workflowconfig.BillingModeSubscription, limits: providerRateLimits(0, 100), want: 1},
		{name: "subscription without snapshot uses configured capacity", billingMode: workflowconfig.BillingModeSubscription, want: 10},
		{name: "off ignores provider window", billingMode: workflowconfig.BillingModeSubscription, pacing: workflowconfig.RateWindowPacing{Mode: workflowconfig.RateWindowPacingOff}, limits: providerRateLimits(10, 100), want: 10},
		{name: "floor preserves capacity above threshold", billingMode: workflowconfig.BillingModeSubscription, pacing: workflowconfig.RateWindowPacing{Mode: workflowconfig.RateWindowPacingFloor, FloorPercent: 25}, limits: providerRateLimits(30, 100), want: 10},
		{name: "floor scales below threshold", billingMode: workflowconfig.BillingModeSubscription, pacing: workflowconfig.RateWindowPacing{Mode: workflowconfig.RateWindowPacingFloor, FloorPercent: 25}, limits: providerRateLimits(20, 100), want: 2},
		{name: "stale snapshot uses configured capacity", billingMode: workflowconfig.BillingModeSubscription, limits: staleProviderRateLimits(10, 100), want: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				BillingMode:         tt.billingMode,
				RateWindowPacing:    tt.pacing,
				MaxConcurrentAgents: 10,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
			})
			state := newState(cfg)
			state.RateLimits = tt.limits
			issues := make([]connector.Issue, 10)
			for index := range issues {
				issues[index] = dispatchTestIssue(fmt.Sprintf("issue-%02d", index), "Todo")
			}

			plan := PlanDispatch(cfg, state, issues, time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC))
			if got := len(plan.Dispatches); got != tt.want {
				t.Fatalf("dispatches = %d, want %d", got, tt.want)
			}
		})
	}
}

func providerRateLimits(remaining, limit int64) *telemetry.RateLimits {
	observedAt := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	return &telemetry.RateLimits{Primary: &telemetry.RateLimitBucket{Remaining: remaining, Limit: limit, ObservedAt: &observedAt}}
}

func staleProviderRateLimits(remaining, limit int64) *telemetry.RateLimits {
	observedAt := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return &telemetry.RateLimits{Primary: &telemetry.RateLimitBucket{Remaining: remaining, Limit: limit, ObservedAt: &observedAt}}
}

func providerPrimarySecondaryRateLimits(primary, secondary int64) *telemetry.RateLimits {
	observedAt := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	return &telemetry.RateLimits{
		Primary:   &telemetry.RateLimitBucket{Remaining: primary, Limit: 100, ObservedAt: &observedAt},
		Secondary: &telemetry.RateLimitBucket{Remaining: secondary, Limit: 100, ObservedAt: &observedAt},
	}
}

func TestSubscriptionPrunesLegacyUSDBudgetRefusals(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{BillingMode: workflowconfig.BillingModeSubscription})
	planner := newDispatchPlanner(cfg)
	state := newState(cfg)
	state.BudgetRefusals["daily"] = BudgetRefusal{Code: string(budget.ReasonPerDayMaxUSD)}
	state.BudgetRefusals["issue"] = BudgetRefusal{Code: string(budget.ReasonPerIssueMaxUSD)}

	planner.pruneBudgetRefusals(&state, time.Now(), nil, nil)

	if len(state.BudgetRefusals) != 0 {
		t.Fatalf("BudgetRefusals = %#v, want cleared in subscription mode", state.BudgetRefusals)
	}
}

func TestDispatchableFiltersIneligibleCandidates(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    2,
		BillingMode:            workflowconfig.BillingModeMetered,
		ActiveStates:           []string{"Todo", "In Progress"},
		TerminalStates:         []string{"Done", "Cancelled"},
		BudgetRefusalCooldown:  time.Hour,
		ContinuationRetryDelay: time.Second,
	})
	orch := Orchestrator{cfg: cfg}

	tests := []struct {
		name  string
		issue connector.Issue
		state func(State)
		want  bool
	}{
		{
			name:  "active issue",
			issue: dispatchTestIssue("issue-active", "Todo"),
			want:  true,
		},
		{
			name:  "terminal issue",
			issue: dispatchTestIssue("issue-terminal", "Done"),
			want:  false,
		},
		{
			name:  "inactive issue",
			issue: dispatchTestIssue("issue-inactive", "Backlog"),
			want:  false,
		},
		{
			name: "todo blocked by non-terminal dependency",
			issue: func() connector.Issue {
				issue := dispatchTestIssue("issue-blocked-dependency", "Todo")
				issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#10", State: "In Progress"}}
				return issue
			}(),
			want: false,
		},
		{
			name: "todo unblocked by terminal dependency",
			issue: func() connector.Issue {
				issue := dispatchTestIssue("issue-terminal-dependency", "Todo")
				issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#10", State: "Done"}}
				return issue
			}(),
			want: true,
		},
		{
			name: "todo unblocked by unknown dependency state",
			issue: func() connector.Issue {
				issue := dispatchTestIssue("issue-unknown-dependency", "Todo")
				issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#10"}}
				return issue
			}(),
			want: true,
		},
		{
			name:  "already running",
			issue: dispatchTestIssue("issue-running", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-running", "Todo")
				state.Running[issue.ID] = Running{Issue: issue}
			},
			want: false,
		},
		{
			name:  "already claimed",
			issue: dispatchTestIssue("issue-claimed", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-claimed", "Todo")
				state.Claimed[issue.ID] = Claimed{Issue: issue}
			},
			want: false,
		},
		{
			name:  "scheduled retry",
			issue: dispatchTestIssue("issue-retry", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-retry", "Todo")
				state.Retry[issue.ID] = Retry{Issue: issue, DueAt: now.Add(time.Minute)}
			},
			want: false,
		},
		{
			name:  "already blocked",
			issue: dispatchTestIssue("issue-blocked", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-blocked", "Todo")
				state.Blocked[issue.ID] = Blocked{Issue: issue}
			},
			want: false,
		},
		{
			name:  "budget cooldown active",
			issue: dispatchTestIssue("issue-budget", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-budget", "Todo")
				state.BudgetRefusals[issue.ID] = BudgetRefusal{
					Issue:     issue,
					RefusedAt: now.Add(-time.Minute),
				}
			},
			want: false,
		},
		{
			name:  "budget cooldown expired",
			issue: dispatchTestIssue("issue-budget-expired", "Todo"),
			state: func(state State) {
				issue := dispatchTestIssue("issue-budget-expired", "Todo")
				state.BudgetRefusals[issue.ID] = BudgetRefusal{
					Issue:     issue,
					RefusedAt: now.Add(-2 * time.Hour),
				}
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(cfg)
			if tt.state != nil {
				tt.state(state)
			}

			got := orch.dispatchable(tt.issue, &state, now)
			if got != tt.want {
				t.Fatalf("dispatchable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDispatchableBudgetRefusalWaitReasons(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	resetAt := now.Add(12 * time.Hour)
	tests := []struct {
		name       string
		code       budget.ReasonCode
		refusedAt  time.Time
		resetAt    *time.Time
		wantReason string
	}{
		{
			name:       "daily refusal is a cooldown",
			code:       budget.ReasonPerDayMaxUSD,
			refusedAt:  now.Add(-time.Minute),
			resetAt:    &resetAt,
			wantReason: dispatchSkipBudgetCooldown,
		},
		{
			name:       "per issue refusal is a hard hold",
			code:       budget.ReasonPerIssueMaxUSD,
			refusedAt:  now.Add(-time.Minute),
			wantReason: dispatchSkipBudgetHardHold,
		},
		{
			name:       "per issue hold survives cooldown expiry",
			code:       budget.ReasonPerIssueMaxUSD,
			refusedAt:  now.Add(-48 * time.Hour),
			wantReason: dispatchSkipBudgetHardHold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents:   1,
				BillingMode:           workflowconfig.BillingModeMetered,
				ActiveStates:          []string{"Todo"},
				BudgetRefusalCooldown: time.Hour,
			})
			issue := dispatchTestIssue("issue-budget", "Todo")
			state := newState(cfg)
			state.BudgetRefusals[issue.ID] = BudgetRefusal{
				Issue:     issue,
				Code:      string(tt.code),
				RefusedAt: tt.refusedAt,
				ResetAt:   tt.resetAt,
			}

			decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, now, "")
			if decision.dispatchable || decision.reason != tt.wantReason {
				t.Fatalf("dispatch decision = %#v, want skipped for %q", decision, tt.wantReason)
			}
		})
	}
}

func TestDispatchableIssueDecisionReportsEarlierGateBeforeFailureBreaker(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 15, 22, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	})
	issue := dispatchTestIssue("issue-dependent", "Todo")
	issue.BlockedBy = []connector.BlockedRef{{Identifier: "owner/repo#1", State: "Todo"}}
	state := newState(cfg)
	state.FailureBreaker.Class = "runner_error:opaque"
	state.FailureBreaker.ResumeAt = now.Add(time.Hour)

	decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, now, "")
	if decision.dispatchable || decision.reason != dispatchSkipBlockedByDependency {
		t.Fatalf("dispatch decision = %#v, want skipped for %q", decision, dispatchSkipBlockedByDependency)
	}
}

func TestPruneBudgetRefusalsReevaluatesDailyCap(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	resetAt := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		refusal        BudgetRefusal
		dailyStatus    DailyBudgetStatus
		dailyStatusErr error
		issueStatus    IssueBudgetStatus
		issueKnown     bool
		issueStatusErr error
		wantActive     bool
	}{
		{
			name: "cap raise clears resolved daily refusal",
			refusal: BudgetRefusal{
				Code:             "per_day_max_usd",
				ProjectedCostUSD: 10,
				ResetAt:          &resetAt,
				RefusedAt:        now,
			},
			dailyStatus: DailyBudgetStatus{Active: true, CurrentSpendUSD: 100, MaxUSD: 250},
		},
		{
			name: "cap raise keeps daily refusal when projected spend remains over cap",
			refusal: BudgetRefusal{
				Code:             "per_day_max_usd",
				ProjectedCostUSD: 10,
				ResetAt:          &resetAt,
				RefusedAt:        now,
			},
			dailyStatus: DailyBudgetStatus{Active: true, CurrentSpendUSD: 245, MaxUSD: 250},
			wantActive:  true,
		},
		{
			name: "unchanged cap keeps daily refusal until midnight",
			refusal: BudgetRefusal{
				Code:             "per_day_max_usd",
				ProjectedCostUSD: 10,
				ResetAt:          &resetAt,
				RefusedAt:        now,
			},
			dailyStatus: DailyBudgetStatus{Active: true, CurrentSpendUSD: 100, MaxUSD: 100},
			wantActive:  true,
		},
		{
			name: "disabled daily cap clears daily refusal",
			refusal: BudgetRefusal{
				Code:             "per_day_max_usd",
				ProjectedCostUSD: 10,
				ResetAt:          &resetAt,
				RefusedAt:        now,
			},
		},
		{
			name: "status lookup failure preserves daily refusal",
			refusal: BudgetRefusal{
				Code:             "per_day_max_usd",
				ProjectedCostUSD: 10,
				ResetAt:          &resetAt,
				RefusedAt:        now,
			},
			dailyStatusErr: errors.New("lookup failed"),
			wantActive:     true,
		},
		{
			name: "per issue hold persists across cooldown boundaries",
			refusal: BudgetRefusal{
				Code:             "per_issue_max_usd",
				ProjectedCostUSD: 10,
				RefusedAt:        now.Add(-48 * time.Hour),
			},
			issueStatus: IssueBudgetStatus{Active: true, CurrentSpendUSD: 95, MaxUSD: 100},
			issueKnown:  true,
			wantActive:  true,
		},
		{
			name: "per issue cap raise clears resolved hold",
			refusal: BudgetRefusal{
				Code:             "per_issue_max_usd",
				ProjectedCostUSD: 10,
				RefusedAt:        now.Add(-48 * time.Hour),
			},
			issueStatus: IssueBudgetStatus{Active: true, CurrentSpendUSD: 95, MaxUSD: 125},
			issueKnown:  true,
		},
		{
			name: "disabled per issue cap clears hold",
			refusal: BudgetRefusal{
				Code:             "per_issue_max_usd",
				ProjectedCostUSD: 10,
				RefusedAt:        now.Add(-48 * time.Hour),
			},
			issueKnown: true,
		},
		{
			name: "per issue status failure preserves hold",
			refusal: BudgetRefusal{
				Code:             "per_issue_max_usd",
				ProjectedCostUSD: 10,
				RefusedAt:        now.Add(-48 * time.Hour),
			},
			issueKnown:     true,
			issueStatusErr: errors.New("lookup failed"),
			wantActive:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{BillingMode: workflowconfig.BillingModeMetered, BudgetRefusalCooldown: time.Hour})
			state := newState(cfg)
			issue := dispatchTestIssue("issue", "Todo")
			tt.refusal.Issue = issue
			state.BudgetRefusals["issue"] = tt.refusal
			orch := Orchestrator{
				cfg: cfg,
				dailyBudgetStatus: fakeDailyBudgetStatusProvider{
					status: tt.dailyStatus,
					err:    tt.dailyStatusErr,
				},
				issueBudgetStatus: fakeIssueBudgetStatusProvider{
					status: tt.issueStatus,
					known:  tt.issueKnown,
					err:    tt.issueStatusErr,
				},
			}

			orch.dispatchPlanner().pruneInactiveIssueBudgetRefusals(&state, []connector.Issue{issue})
			orch.pruneBudgetRefusals(context.Background(), &state, now)
			_, gotActive := state.BudgetRefusals["issue"]
			if gotActive != tt.wantActive {
				t.Fatalf("budget refusal active = %t, want %t", gotActive, tt.wantActive)
			}
		})
	}
}

func TestPruneInactiveIssueBudgetRefusals(t *testing.T) {
	t.Parallel()

	issue := dispatchTestIssue("issue-held", "Todo")
	tests := []struct {
		name       string
		code       string
		candidates []connector.Issue
		wantHeld   bool
	}{
		{
			name:       "active candidate keeps per issue hold",
			code:       string(budget.ReasonPerIssueMaxUSD),
			candidates: []connector.Issue{issue},
			wantHeld:   true,
		},
		{
			name: "missing candidate clears per issue hold",
			code: string(budget.ReasonPerIssueMaxUSD),
		},
		{
			name:     "missing candidate keeps daily cooldown",
			code:     string(budget.ReasonPerDayMaxUSD),
			wantHeld: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(normalizeConfig(Config{}))
			state.BudgetRefusals[issue.ID] = BudgetRefusal{Issue: issue, Code: tt.code}
			newDispatchPlanner(Config{}).pruneInactiveIssueBudgetRefusals(&state, tt.candidates)
			_, gotHeld := state.BudgetRefusals[issue.ID]
			if gotHeld != tt.wantHeld {
				t.Fatalf("budget refusal held = %t, want %t", gotHeld, tt.wantHeld)
			}
		})
	}
}

type fakeDailyBudgetStatusProvider struct {
	status DailyBudgetStatus
	err    error
}

type fakeIssueBudgetStatusProvider struct {
	status IssueBudgetStatus
	known  bool
	err    error
}

func (p fakeIssueBudgetStatusProvider) IssueBudgetStatus(context.Context, connector.Issue) (IssueBudgetStatus, bool, error) {
	return p.status, p.known, p.err
}

func (p fakeDailyBudgetStatusProvider) DailyBudgetStatus(context.Context, time.Time) (DailyBudgetStatus, bool, error) {
	return p.status, true, p.err
}

func TestDispatchableSkipsAutoPromoteGatePendingActiveIssue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 13, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 0,
			Gate:          gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := dispatchTestIssueWithPullRequest("issue-gate-pending", "In Progress", "OPEN")
	issue.PullRequest.CIStatus = "pending"
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{Issue: issue, FinalState: FinalStateCompleted}
	orch := Orchestrator{cfg: cfg}

	decision := orch.dispatchPlanner().dispatchableIssueDecision(issue, &state, false, now, "")
	if decision.dispatchable {
		t.Fatal("dispatchable gate-pending active issue = true, want false")
	}
	if decision.reason != dispatchSkipAwaitingGate {
		t.Fatalf("dispatchable reason = %q, want %q", decision.reason, dispatchSkipAwaitingGate)
	}
}

func TestDispatchPlannerSkipsCompletedGateWaitRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 18, 44, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 0,
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := dispatchTestIssueWithPullRequest("issue-completed-gate-wait", "In Progress", "OPEN")
	issue.PullRequest.CIStatus = "pending"
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{
		Issue:       issue,
		CompletedAt: now.Add(-3 * time.Minute),
		FinalState:  FinalStateCompleted,
	}
	planner := newDispatchPlanner(cfg)
	planner.scheduleRetryAfter(&state, issue, 1, now, 0, "", "")
	var decisions []dispatchPlanDecision

	plan := planner.plan(&state, []connector.Issue{issue}, now, dispatchPlanHooks{
		decision: func(decision dispatchPlanDecision) {
			decisions = append(decisions, decision)
		},
	})

	if len(plan.Dispatches) != 0 {
		t.Fatalf("dispatches = %#v, want completed gate-wait retry skipped", plan.Dispatches)
	}
	if len(decisions) != 1 || decisions[0].SkipReason != "awaiting_gate" {
		t.Fatalf("decisions = %#v, want awaiting_gate skip", decisions)
	}
	if _, ok := state.Completed[issue.ID]; !ok {
		t.Fatalf("Completed[%q] missing after gate-wait skip", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after gate-wait skip", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after gate-wait skip", issue.ID)
	}

	attempts := &recordingWorkAttemptStore{}
	orch := Orchestrator{cfg: cfg, workAttempts: attempts}
	loggedState := newState(cfg)
	loggedState.Completed[issue.ID] = Completed{
		Issue:       issue,
		CompletedAt: now.Add(-3 * time.Minute),
		FinalState:  FinalStateCompleted,
	}
	orch.dispatchReadyIssues(t.Context(), &loggedState, []connector.Issue{issue}, now)
	if len(attempts.decisions) != 1 || attempts.decisions[0].Reason != dispatchSkipAwaitingGate {
		t.Fatalf("scheduler decisions = %#v, want awaiting_gate skip", attempts.decisions)
	}
}

func TestDispatchableSkipsQuietWindowActiveIssueWithOpenPullRequest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 13, 5, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := dispatchTestIssueWithPullRequest("issue-quiet-gate-pending", "In Progress", "OPEN")
	issue.PullRequest.CIStatus = "pending"
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{Issue: issue, FinalState: FinalStateCompleted}
	orch := Orchestrator{cfg: cfg}

	decision := orch.dispatchPlanner().dispatchableIssueDecision(issue, &state, false, now, "")
	if decision.dispatchable {
		t.Fatal("dispatchable quiet-window active issue with open PR = true, want false")
	}
	if decision.reason != dispatchSkipAwaitingGate {
		t.Fatalf("dispatchable reason = %q, want %q", decision.reason, dispatchSkipAwaitingGate)
	}
}

func TestDispatchableCompletedReworkGateWaitEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 18, 30, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tests := []struct {
		name            string
		prepareComplete func(*connector.Issue)
		prepareCurrent  func(*connector.Issue)
		wantDispatch    bool
	}{
		{name: "unchanged exact head remains waiting"},
		{
			name: "new pull request head permits dispatch",
			prepareCurrent: func(issue *connector.Issue) {
				issue.PullRequest.HeadSHA = "new-head"
			},
			wantDispatch: true,
		},
		{
			name: "new failing check permits dispatch",
			prepareCurrent: func(issue *connector.Issue) {
				issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{Name: "Test", Status: "completed", Conclusion: "failure"}}
			},
			wantDispatch: true,
		},
		{
			name: "new P1 review permits dispatch",
			prepareCurrent: func(issue *connector.Issue) {
				issue.PullRequest.CodexReviewState = "P1"
				issue.PullRequest.CodexReviewFindings = []connector.PullRequestFinding{{Body: "Fix the race.", Path: "internal/orchestrator/run_completion.go", Line: 577}}
			},
			wantDispatch: true,
		},
		{
			name: "cleared failing check remains waiting for promotion",
			prepareComplete: func(issue *connector.Issue) {
				issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{Name: "Test", Status: "completed", Conclusion: "failure"}}
			},
			prepareCurrent: func(issue *connector.Issue) {
				issue.PullRequest.RequiredCheckFailures = nil
			},
		},
		{
			name: "lane movement permits dispatch",
			prepareCurrent: func(issue *connector.Issue) {
				issue.State = "In Progress"
			},
			wantDispatch: true,
		},
		{
			name: "later Rework entry permits dispatch",
			prepareCurrent: func(issue *connector.Issue) {
				enteredAt := now.Add(-time.Minute)
				issue.StageUpdatedAt = &enteredAt
			},
			wantDispatch: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			completedIssue := dispatchTestIssueWithPullRequest("issue-rework-gate-wait", "Rework", "OPEN")
			completedIssue.PullRequest.HeadSHA = "same-head"
			completedIssue.PullRequest.MergeableState = "clean"
			completedIssue.PullRequest.CIStatus = "success"
			if tt.prepareComplete != nil {
				tt.prepareComplete(&completedIssue)
			}
			currentIssue := cloneIssue(completedIssue)
			if tt.prepareCurrent != nil {
				tt.prepareCurrent(&currentIssue)
			}
			state := newState(cfg)
			state.Completed[currentIssue.ID] = Completed{
				Issue:          completedIssue,
				CompletedAt:    now.Add(-2 * time.Minute),
				FinalState:     FinalStateCompleted,
				GateWaitReason: completedReworkGateWaitReason,
			}

			decision := newDispatchPlanner(cfg).dispatchableIssueDecision(currentIssue, &state, false, now, "")
			if decision.dispatchable != tt.wantDispatch {
				t.Fatalf("dispatchable = %v, want %v; reason = %q", decision.dispatchable, tt.wantDispatch, decision.reason)
			}
			if !tt.wantDispatch && decision.reason != dispatchSkipAwaitingGate {
				t.Fatalf("reason = %q, want %q", decision.reason, dispatchSkipAwaitingGate)
			}
		})
	}
}

func TestTargetedReconcilePreservesCompletedReworkGateWaitEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 19, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	completedIssue := dispatchTestIssueWithPullRequest("issue-rework-gate-refresh", "Rework", "OPEN")
	completedIssue.PullRequest.HeadSHA = "completed-head"
	completedIssue.PullRequest.MergeableState = "clean"
	completedIssue.PullRequest.CIStatus = "success"
	refreshedIssue := cloneIssue(completedIssue)
	refreshedIssue.PullRequest.HeadSHA = "replacement-head"

	state := newState(cfg)
	state.Completed[completedIssue.ID] = Completed{
		Issue:            cloneIssue(completedIssue),
		CompletedAt:      now.Add(-2 * time.Minute),
		FinalState:       FinalStateCompleted,
		GateWaitReason:   completedReworkGateWaitReason,
		gateWaitEvidence: completionGateWaitEvidence(completedReworkGateWaitReason, completedIssue),
	}
	orch := Orchestrator{cfg: cfg}
	orch.updateTargetedIssueEntries(&state, refreshedIssue)

	completed := state.Completed[completedIssue.ID]
	if got := completed.Issue.PullRequest.HeadSHA; got != "replacement-head" {
		t.Fatalf("Completed issue head = %q, want refreshed head", got)
	}
	if got := completed.gateWaitEvidence.PullRequest.HeadSHA; got != "completed-head" {
		t.Fatalf("gate-wait evidence head = %q, want completion-time head", got)
	}
	if autoPromoteActiveGatePendingIssue(refreshedIssue, &state, cfg, cfg.AutoPromote) {
		t.Fatal("replacement head remained pending the completed-head gate")
	}
	decision := newDispatchPlanner(cfg).dispatchableIssueDecision(refreshedIssue, &state, false, now, "")
	if !decision.dispatchable {
		t.Fatalf("replacement-head dispatch decision = %#v, want dispatchable", decision)
	}
}

func TestDispatchableArtifactGateWaitStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 15, 40, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			SourceState: "Review",
			PassState:   "Ready for Pickup",
			ReworkState: "Rework",
			Gate: gate.Config{
				Kind: gate.KindArtifact,
				Artifact: gate.ArtifactConfig{
					StatusField:    "render_status",
					PassStatuses:   []string{"approved", "valid"},
					WaitStatuses:   []string{"queued", "rendering", "pending_review"},
					ReworkStatuses: []string{"recut", "invalid", "missing_assets"},
				},
			},
		},
		ActiveStates:   []string{"Todo", "Production", "Rework"},
		ObservedStates: []string{"Backlog", "Review", "Blocked"},
		TerminalStates: []string{"Ready for Pickup", "Done", "Cancelled"},
	})
	tests := []struct {
		name           string
		state          string
		status         string
		stageUpdatedAt *time.Time
		updatedAt      *time.Time
		fieldUpdatedAt *time.Time
		wantDispatch   bool
		wantReason     string
	}{
		{
			name:           "fresh item with seeded wait status dispatches",
			state:          "Todo",
			status:         "queued",
			stageUpdatedAt: timePointer(now),
			updatedAt:      timePointer(now),
			fieldUpdatedAt: timePointer(now),
			wantDispatch:   true,
		},
		{
			name:           "post run wait item stays skipped",
			state:          "Production",
			status:         "pending_review",
			stageUpdatedAt: timePointer(now.Add(-2 * time.Minute)),
			updatedAt:      timePointer(now.Add(-time.Minute)),
			fieldUpdatedAt: timePointer(now.Add(-time.Minute)),
			wantReason:     dispatchSkipArtifactGateWaitStatus,
		},
		{
			name:           "human restarted round dispatches",
			state:          "Todo",
			status:         "pending_review",
			stageUpdatedAt: timePointer(now),
			updatedAt:      timePointer(now),
			fieldUpdatedAt: timePointer(now.Add(-time.Minute)),
			wantDispatch:   true,
		},
		{
			name:           "newer state wins stale status race",
			state:          "Rework",
			status:         "pending_review",
			stageUpdatedAt: timePointer(now),
			updatedAt:      timePointer(now),
			fieldUpdatedAt: timePointer(now.Add(-time.Nanosecond)),
			wantDispatch:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := dispatchTestIssue("issue-artifact-status", tt.state)
			issue.Fields = map[string]string{"render_status": tt.status}
			issue.StageUpdatedAt = tt.stageUpdatedAt
			issue.UpdatedAt = tt.updatedAt
			if tt.fieldUpdatedAt != nil {
				issue.FieldUpdatedAt = map[string]time.Time{"render_status": *tt.fieldUpdatedAt}
			}
			state := newState(cfg)
			orch := Orchestrator{cfg: cfg}

			decision := orch.dispatchPlanner().dispatchableIssueDecision(issue, &state, false, now, "")
			if decision.dispatchable != tt.wantDispatch {
				t.Fatalf("dispatchable = %t, want %t", decision.dispatchable, tt.wantDispatch)
			}
			if decision.reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", decision.reason, tt.wantReason)
			}
		})
	}

	issue := dispatchTestIssue("issue-artifact-pending-review", "Production")
	issue.Fields = map[string]string{"render_status": "pending_review"}
	issue.StageUpdatedAt = timePointer(now.Add(-2 * time.Minute))
	issue.UpdatedAt = timePointer(now.Add(-time.Minute))
	issue.FieldUpdatedAt = map[string]time.Time{"render_status": now.Add(-time.Minute)}
	state := newState(cfg)
	orch := Orchestrator{cfg: cfg}
	attempts := &recordingWorkAttemptStore{}
	orch.workAttempts = attempts
	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)
	if len(attempts.decisions) != 1 {
		t.Fatalf("scheduler decisions len = %d, want 1", len(attempts.decisions))
	}
	if got := attempts.decisions[0].Reason; got != dispatchSkipArtifactGateWaitStatus {
		t.Fatalf("scheduler decision reason = %q, want %q", got, dispatchSkipArtifactGateWaitStatus)
	}
}

func TestHandleRunResultRecordsBudgetRefusalAndComment(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		BillingMode:            workflowconfig.BillingModeMetered,
		ActiveStates:           []string{"Todo"},
		TerminalStates:         []string{"Done"},
		BudgetRefusalCooldown:  time.Hour,
		ContinuationRetryDelay: time.Second,
	})
	commentConnector := &budgetRefusalCommentConnector{}
	orch := Orchestrator{
		cfg:       cfg,
		connector: commentConnector,
	}
	state := newState(cfg)
	issue := dispatchTestIssue("issue-budget-refused", "Todo")
	state.Running[issue.ID] = Running{
		Issue:      issue,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "local",
	}
	maxUSD := 2.5

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			BudgetRefusal: &runpkg.BudgetRefusal{
				Code:             "per_day_max_usd",
				Message:          "daily budget exceeded",
				Comment:          "Detent refused to dispatch this issue because the projected dispatch would exceed the daily budget.",
				CurrentSpendUSD:  2.40,
				ProjectedCostUSD: 0.20,
				MaxUSD:           &maxUSD,
				RefusedAt:        now,
			},
		},
	})

	refusal, ok := state.BudgetRefusals[issue.ID]
	if !ok {
		t.Fatal("BudgetRefusals missing issue, want recorded refusal")
	}
	if refusal.Issue.ID != issue.ID || refusal.Code != "per_day_max_usd" || refusal.ProjectedCostUSD != 0.20 {
		t.Fatalf("BudgetRefusal = %#v, want recorded issue and spend", refusal)
	}
	if orch.dispatchable(issue, &state, now.Add(time.Minute)) {
		t.Fatal("dispatchable during budget cooldown = true, want false")
	}
	if len(commentConnector.comments) != 1 {
		t.Fatalf("comments len = %d, want 1", len(commentConnector.comments))
	}
	if commentConnector.comments[0].issueID != issue.ID || !strings.Contains(commentConnector.comments[0].body, "projected dispatch would exceed the daily budget") {
		t.Fatalf("comment = %#v, want budget refusal comment", commentConnector.comments[0])
	}
}

func TestHandleRunResultCreatesPerIssueHardHoldWithoutRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		BillingMode:            workflowconfig.BillingModeMetered,
		ActiveStates:           []string{"Todo"},
		TerminalStates:         []string{"Done"},
		BudgetRefusalCooldown:  time.Hour,
		ContinuationRetryDelay: time.Second,
	})
	orch := Orchestrator{cfg: cfg, connector: &budgetRefusalCommentConnector{}}
	state := newState(cfg)
	issue := dispatchTestIssue("issue-hard-budget-hold", "Todo")
	state.Running[issue.ID] = Running{Issue: issue, StartedAt: now.Add(-time.Minute), WorkerHost: "local"}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
	maxUSD := 5.0

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			BudgetRefusal: &runpkg.BudgetRefusal{
				Code:             string(budget.ReasonPerIssueMaxUSD),
				Message:          "per-issue budget exceeded",
				Comment:          "hard budget hold",
				CurrentSpendUSD:  4.75,
				ProjectedCostUSD: 1,
				MaxUSD:           &maxUSD,
				RefusedAt:        now,
			},
		},
	})

	if _, ok := state.BudgetRefusals[issue.ID]; !ok {
		t.Fatal("BudgetRefusals missing per-issue hard hold")
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatal("Retry contains per-issue hard hold, want no scheduled retry")
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatal("Claimed contains per-issue hard hold, want claim released")
	}
	for _, boundary := range []time.Duration{time.Hour, 2 * time.Hour, 24 * time.Hour} {
		if orch.dispatchable(issue, &state, now.Add(boundary)) {
			t.Fatalf("dispatchable after %s = true, want hard hold", boundary)
		}
	}
}

func TestDispatchableFiltersUnauthorizedCandidates(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Authorization: selector.Selector{
			AssigneeIn: []string{"@me"},
		},
		SelectorContext: selector.Context{
			InstanceLogin: "worker-1",
			Persona:       "release-captain",
		},
	})
	orch := Orchestrator{cfg: cfg}

	tests := []struct {
		name  string
		issue connector.Issue
		want  bool
	}{
		{
			name: "matching assignee is dispatchable",
			issue: func() connector.Issue {
				issue := dispatchTestIssue("issue-authorized", "Todo")
				issue.Assignees = []string{"worker-1"}
				return issue
			}(),
			want: true,
		},
		{
			name: "nonmatching assignee is skipped",
			issue: func() connector.Issue {
				issue := dispatchTestIssue("issue-unauthorized", "Todo")
				issue.Assignees = []string{"worker-2"}
				return issue
			}(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(cfg)
			if got := orch.dispatchable(tt.issue, &state, now); got != tt.want {
				t.Fatalf("dispatchable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDispatchableAuthorizationDecisionCarriesSelectorDetail(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Authorization: selector.Selector{
			Labels: selector.Labels{Include: []string{"detent"}},
		},
	})
	issue := dispatchTestIssue("issue-selector-declined", "Todo")
	issue.Labels = []string{"detent:todo"}

	state := newState(cfg)
	decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, time.Now(), "")
	if decision.dispatchable || decision.reason != dispatchSkipAuthorizationSelector {
		t.Fatalf("dispatchableIssueDecision() = %#v, want authorization selector decline", decision)
	}
	if decision.authorization == nil || decision.authorization.Rule != selector.RuleLabelInclude || decision.authorization.Value != "detent" {
		t.Fatalf("authorization decision = %#v, want missing include label detail", decision.authorization)
	}
	if decision.detail != "issue does not match authorization selector: missing required label `detent`" {
		t.Fatalf("decision detail = %q", decision.detail)
	}
}

func TestAuthorizationSelectorDetailReachesDispatchTelemetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 20, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:    1,
		ActiveStates:           []string{"Todo"},
		TerminalStates:         []string{"Done"},
		DispatchStallThreshold: time.Hour,
		Authorization: selector.Selector{
			Labels: selector.Labels{Include: []string{"detent"}},
		},
	})
	cfg.Project.ID = "corp"
	issue := dispatchTestIssue("issue-532", "Todo")
	issue.Labels = []string{"detent:todo"}
	attempts := &recordingWorkAttemptStore{}
	orch := &Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	decisions := make([]dispatchPlanDecision, 0, 1)

	newDispatchPlanner(cfg).plan(&state, []connector.Issue{issue}, now, dispatchPlanHooks{
		decision: func(decision dispatchPlanDecision) {
			decisions = append(decisions, decision)
			orch.logDispatchPlanDecision(t.Context(), &state, now, decision)
		},
	})

	if len(decisions) != 1 || len(attempts.decisions) != 1 {
		t.Fatalf("plan decisions = %#v, persisted = %#v", decisions, attempts.decisions)
	}
	const wantDetail = "issue does not match authorization selector: missing required label `detent`"
	persisted := attempts.decisions[0]
	if persisted.Reason != dispatchSkipAuthorizationSelector || persisted.WaitReason != wantDetail {
		t.Fatalf("persisted decision = %#v", persisted)
	}
	for _, want := range []string{`"rule":"label_include"`, `"value":"detent"`, `"detail":"` + wantDetail + `"`} {
		if !strings.Contains(persisted.MetadataJSON, want) {
			t.Fatalf("metadata = %q, want containing %q", persisted.MetadataJSON, want)
		}
	}
	status := projectDispatchStatusFromCycle(store.ProjectDispatchStatus{}, "corp", []connector.Issue{issue}, decisions, nil, now)
	snapshot := dispatchStatusSnapshot(status, cfg.DispatchStallThreshold, now.Add(2*time.Hour))
	if snapshot.WaitReason != wantDetail {
		t.Fatalf("dispatch payload wait_reason = %q, want %q", snapshot.WaitReason, wantDetail)
	}
}

func TestDispatchPlanOwnershipEligibility(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		ownershipMode string
		assigneeGate  bool
		assignees     []string
		wantDispatch  bool
		wantAttention bool
	}{
		{name: "upgrade grace dispatches unassigned issue", ownershipMode: workflowconfig.IdentityOwnershipAssignee, wantDispatch: true},
		{name: "acknowledged rule blocks unassigned issue", ownershipMode: workflowconfig.IdentityOwnershipAssignee, assigneeGate: true, wantAttention: true},
		{name: "assigned steady state dispatches", ownershipMode: workflowconfig.IdentityOwnershipAssignee, assigneeGate: true, assignees: []string{"operator"}, wantDispatch: true},
		{name: "assignee absent in field mode dispatches", ownershipMode: workflowconfig.IdentityOwnershipField, wantDispatch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				Claiming: ClaimingConfig{
					OwnershipSet:     true,
					OwnershipMode:    tt.ownershipMode,
					AssigneeRequired: tt.assigneeGate,
				},
			})
			state := newState(cfg)
			issue := dispatchTestIssue("issue-ownership", "Todo")
			issue.Assignees = append([]string(nil), tt.assignees...)

			plan := newDispatchPlanner(cfg).plan(&state, []connector.Issue{issue}, now, dispatchPlanHooks{})
			if got := len(plan.Dispatches) == 1; got != tt.wantDispatch {
				t.Fatalf("dispatch = %t, want %t; plan = %#v", got, tt.wantDispatch, plan)
			}
			blocked, gotAttention := state.Blocked[issue.ID]
			if gotAttention != tt.wantAttention {
				t.Fatalf("ownership attention = %t, want %t; blocked = %#v", gotAttention, tt.wantAttention, state.Blocked)
			}
			if tt.wantAttention {
				if blocked.RecoveryReason != "human_blocker" || !strings.Contains(blocked.Reason, "needs an assignee") {
					t.Fatalf("ownership attention = %#v, want human blocker with assignee remedy", blocked)
				}
			}
		})
	}
}

func TestOwnershipEligibilityStartupLogOncePerProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enforced    bool
		wantMessage string
	}{
		{name: "upgrade grace", wantMessage: "ownership eligibility compatibility grace active"},
		{name: "acknowledged enforcement", enforced: true, wantMessage: "ownership eligibility enforcement active"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer
			cfg := normalizeConfig(Config{
				Project:        scheduler.ProjectCandidate{ID: "detent.build"},
				ActiveStates:   []string{"Todo"},
				TerminalStates: []string{"Done"},
				Claiming: ClaimingConfig{
					OwnershipSet:     true,
					OwnershipMode:    workflowconfig.IdentityOwnershipAssignee,
					AssigneeRequired: tt.enforced,
				},
			})
			orch := Orchestrator{
				cfg:       cfg,
				projectID: "detent.build",
				logger:    slog.New(slog.NewTextHandler(&logs, nil)),
			}
			issue := dispatchTestIssue("issue-10", "Todo")

			orch.logOwnershipEligibilityStartup(newDispatchPlanner(cfg), []connector.Issue{issue})
			orch.logOwnershipEligibilityStartup(newDispatchPlanner(cfg), []connector.Issue{issue})

			output := logs.String()
			if got := strings.Count(output, tt.wantMessage); got != 1 {
				t.Fatalf("startup log count = %d, want 1; logs = %q", got, output)
			}
			for _, want := range []string{
				"project_id=detent.build",
				"config_key=identity.assignee_required",
				fmt.Sprintf("enforced=%t", tt.enforced),
				"blocked_issue_count=1",
				"affected_issues=digitaldrywood/detent#issue-10",
			} {
				if !strings.Contains(output, want) {
					t.Fatalf("startup log = %q, want %q", output, want)
				}
			}
		})
	}
}

func TestOwnershipAttentionEscalatesOnce(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Claiming: ClaimingConfig{
			OwnershipSet:     true,
			OwnershipMode:    workflowconfig.IdentityOwnershipAssignee,
			AssigneeRequired: true,
		},
		Staleness: staleness.Config{RepeatedDecisionCount: 3},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	issue := dispatchTestIssue("issue-unassigned", "Todo")
	state.BoardIssues = []connector.Issue{issue}

	for tick := range 10 {
		newDispatchPlanner(cfg).plan(&state, []connector.Issue{issue}, now.Add(time.Duration(tick)*time.Minute), dispatchPlanHooks{
			decision: func(decision dispatchPlanDecision) {
				orch.logDispatchPlanDecision(t.Context(), &state, now.Add(time.Duration(tick)*time.Minute), decision)
			},
		})
	}

	if len(attempts.decisions) != 3 {
		t.Fatalf("recorded ownership decisions = %d, want threshold 3", len(attempts.decisions))
	}
	escalations := 0
	for _, event := range state.RecentEvents {
		if event.Event == "scheduler_dispatch_needs_human_attention" {
			escalations++
		}
	}
	if escalations != 1 {
		t.Fatalf("ownership escalations = %d, want 1; events = %#v", escalations, state.RecentEvents)
	}
	snapshot := state.Snapshot(now.Add(10 * time.Minute))
	if snapshot.Counts.Blocked != 1 {
		t.Fatalf("dashboard blocked count = %d, want 1", snapshot.Counts.Blocked)
	}
	counts := telemetry.BoardStateCounts(snapshot)
	if len(counts) != 1 || counts[0].State != "Blocked" || counts[0].Count != 1 {
		t.Fatalf("dashboard lane counts = %#v, want one Blocked issue", counts)
	}

	issue.Assignees = []string{"operator"}
	plan := newDispatchPlanner(cfg).plan(&state, []connector.Issue{issue}, now.Add(11*time.Minute), dispatchPlanHooks{})
	if len(plan.Dispatches) != 1 {
		t.Fatalf("dispatches after assignee remedy = %#v, want one", plan.Dispatches)
	}

	delete(state.Claimed, issue.ID)
	delete(state.Running, issue.ID)
	issue.Assignees = nil
	for tick := range 3 {
		newDispatchPlanner(cfg).plan(&state, []connector.Issue{issue}, now.Add(time.Duration(12+tick)*time.Minute), dispatchPlanHooks{
			decision: func(decision dispatchPlanDecision) {
				orch.logDispatchPlanDecision(t.Context(), &state, now.Add(time.Duration(12+tick)*time.Minute), decision)
			},
		})
		if tick == 0 && correctableDispatchEscalated(&state, issue.ID, dispatchSkipOwnershipAssigneeRequired) {
			t.Fatal("new ownership incident escalated before reaching threshold")
		}
	}
	if len(attempts.decisions) != 6 {
		t.Fatalf("recorded ownership decisions across incidents = %d, want 6", len(attempts.decisions))
	}
	escalations = 0
	for _, event := range state.RecentEvents {
		if event.Event == "scheduler_dispatch_needs_human_attention" {
			escalations++
		}
	}
	if escalations != 2 {
		t.Fatalf("ownership escalations across two incidents = %d, want 2; events = %#v", escalations, state.RecentEvents)
	}
}

func TestAuthorizationFilterHintUsesTopLevelSelectorFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth selector.Selector
		ctx  selector.Context
		want connector.IssueFilterHint
	}{
		{
			name: "resolves identities and labels",
			auth: selector.Selector{
				AuthorIn:   []string{" alice ", "@me", "ALICE"},
				AssigneeIn: []string{" team-a ", "@me"},
				Labels: selector.Labels{
					Include: []string{" ready ", "READY"},
					Exclude: []string{" blocked "},
				},
			},
			ctx: selector.Context{
				InstanceLogin: "worker-1",
				Persona:       "release-captain",
			},
			want: connector.IssueFilterHint{
				Authors:      []string{"alice", "worker-1", "release-captain"},
				Assignees:    []string{"team-a", "worker-1", "release-captain"},
				LabelInclude: []string{"ready"},
				LabelExclude: []string{"blocked"},
			},
		},
		{
			name: "ignores nested selectors",
			auth: selector.Selector{
				And: []selector.Selector{{
					AuthorIn: []string{"nested-author"},
					Labels:   selector.Labels{Include: []string{"nested-label"}},
				}},
				Or: []selector.Selector{{
					AssigneeIn: []string{"nested-assignee"},
					Labels:     selector.Labels{Exclude: []string{"nested-exclude"}},
				}},
			},
			want: connector.IssueFilterHint{},
		},
		{
			name: "drops unresolved me token",
			auth: selector.Selector{
				AuthorIn:   []string{"@me"},
				AssigneeIn: []string{"@me"},
			},
			want: connector.IssueFilterHint{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := authorizationFilterHint(tt.auth, tt.ctx)
			assertIssueFilterHint(t, got, tt.want)
		})
	}
}

func TestFetchCandidateIssuesForTickPassesAuthorizationFilterHint(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		ActiveStates: []string{"Todo", "In Progress"},
		Authorization: selector.Selector{
			AuthorIn:   []string{"@me", "alice"},
			AssigneeIn: []string{"worker-2"},
			Labels: selector.Labels{
				Include: []string{"ready"},
				Exclude: []string{"blocked"},
			},
		},
		SelectorContext: selector.Context{
			InstanceLogin: "worker-1",
			Persona:       "release-captain",
		},
	})
	tracker := &filterFetchConnector{
		issues: []connector.Issue{dispatchTestIssue("issue-authorized", "Todo")},
	}
	orch := Orchestrator{cfg: cfg, connector: tracker}
	state := newState(cfg)

	got, err := orch.fetchCandidateIssuesForTick(context.Background(), &state)
	if err != nil {
		t.Fatalf("fetchCandidateIssuesForTick() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "issue-authorized" {
		t.Fatalf("fetchCandidateIssuesForTick() = %#v, want authorized issue", got)
	}
	if tracker.baseFetches != 0 {
		t.Fatalf("FetchCandidateIssues calls = %d, want 0", tracker.baseFetches)
	}
	if !slices.Equal(tracker.states, []string{"todo", "in progress"}) {
		t.Fatalf("states = %#v, want todo/in progress", tracker.states)
	}
	assertIssueFilterHint(t, tracker.hint, connector.IssueFilterHint{
		Authors:      []string{"worker-1", "release-captain", "alice"},
		Assignees:    []string{"worker-2"},
		LabelInclude: []string{"ready"},
		LabelExclude: []string{"blocked"},
	})
}

func TestDispatchPlanMatchesWithAndWithoutAuthorizationPushdown(t *testing.T) {
	t.Parallel()

	authorMatch := dispatchTestIssue("issue-author-match", "Todo")
	authorMatch.AuthorID = "alice"
	authorMiss := dispatchTestIssue("issue-author-miss", "Todo")
	authorMiss.AuthorID = "bob"

	combinedMatch := dispatchTestIssue("issue-combined-match", "Todo")
	combinedMatch.AuthorID = "worker-1"
	combinedMatch.Assignees = []string{"release-captain"}
	combinedMatch.Labels = []string{"ready", "team-a"}
	combinedMiss := dispatchTestIssue("issue-combined-miss", "Todo")
	combinedMiss.AuthorID = "worker-1"
	combinedMiss.Assignees = []string{"release-captain"}
	combinedMiss.Labels = []string{"blocked", "ready"}

	tests := []struct {
		name       string
		auth       selector.Selector
		ctx        selector.Context
		all        []connector.Issue
		pushedDown []connector.Issue
		want       []string
	}{
		{
			name:       "author hint",
			auth:       selector.Selector{AuthorIn: []string{"alice"}},
			all:        []connector.Issue{authorMatch, authorMiss},
			pushedDown: []connector.Issue{authorMatch},
			want:       []string{authorMatch.ID},
		},
		{
			name: "combined top-level hint",
			auth: selector.Selector{
				AuthorIn:   []string{"@me"},
				AssigneeIn: []string{"release-captain"},
				Labels: selector.Labels{
					Include: []string{"ready"},
					Exclude: []string{"blocked"},
				},
			},
			ctx: selector.Context{
				InstanceLogin: "worker-1",
				Persona:       "release-captain",
			},
			all:        []connector.Issue{combinedMatch, combinedMiss},
			pushedDown: []connector.Issue{combinedMatch},
			want:       []string{combinedMatch.ID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{
				MaxConcurrentAgents: 10,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				Authorization:       tt.auth,
				SelectorContext:     tt.ctx,
			}
			withoutPushdown := dispatchPlanIssueIDs(cfg, tt.all)
			withPushdown := dispatchPlanIssueIDs(cfg, tt.pushedDown)
			if !slices.Equal(withoutPushdown, withPushdown) {
				t.Fatalf("dispatch IDs without pushdown = %#v, with pushdown = %#v", withoutPushdown, withPushdown)
			}
			if !slices.Equal(withPushdown, tt.want) {
				t.Fatalf("dispatch IDs = %#v, want %#v", withPushdown, tt.want)
			}
		})
	}
}

func TestMemoryConnectorOrchestratorsPartitionSharedIssuesByAuthorization(t *testing.T) {
	t.Parallel()

	alpha := dispatchTestIssue("issue-alpha", "Todo")
	alpha.Fields = map[string]string{"Owner": "alpha"}
	beta := dispatchTestIssue("issue-beta", "Todo")
	beta.Fields = map[string]string{"Owner": "beta"}
	sharedIssues := []connector.Issue{alpha, beta}

	tests := []struct {
		name      string
		owner     string
		wantIssue string
	}{
		{name: "alpha instance", owner: "alpha", wantIssue: alpha.ID},
		{name: "beta instance", owner: "beta", wantIssue: beta.ID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := newWorkerHostRunner()
			orch, err := New(Config{
				PollInterval:        time.Hour,
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				Authorization: selector.Selector{
					Fields: []selector.FieldEquals{
						{Name: "Owner", Value: tt.owner},
					},
				},
			}, Dependencies{
				Connector: memory.New(memory.Config{Issues: sharedIssues}),
				Runner:    runner,
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- orch.Run(ctx)
			}()

			request := receiveWorkerHostRunRequest(t, runner.started)
			if request.Issue.ID != tt.wantIssue {
				t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, tt.wantIssue)
			}

			select {
			case request := <-runner.started:
				t.Fatalf("unexpected extra dispatch = %#v", request)
			default:
			}

			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() error = %v, want context canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for orchestrator shutdown")
			}
		})
	}
}

func TestDispatchableChecksSlots(t *testing.T) {
	t.Parallel()

	now := time.Now()
	issue := dispatchTestIssue("issue-candidate", "Todo")

	tests := []struct {
		name  string
		cfg   Config
		state func(State)
		want  bool
	}{
		{
			name: "global cap full",
			cfg: Config{
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
			},
			state: func(state State) {
				running := dispatchTestIssue("issue-running", "In Progress")
				state.Running[running.ID] = Running{Issue: running}
			},
			want: false,
		},
		{
			name: "per-state cap full",
			cfg: Config{
				MaxConcurrentAgents:        2,
				MaxConcurrentAgentsByState: map[string]int{"Todo": 1},
				ActiveStates:               []string{"Todo", "In Progress"},
				TerminalStates:             []string{"Done"},
			},
			state: func(state State) {
				running := dispatchTestIssue("issue-running", "Todo")
				state.Running[running.ID] = Running{Issue: running}
			},
			want: false,
		},
		{
			name: "per-state falls back to global cap",
			cfg: Config{
				MaxConcurrentAgents: 2,
				ActiveStates:        []string{"Todo", "In Progress"},
				TerminalStates:      []string{"Done"},
			},
			state: func(state State) {
				running := dispatchTestIssue("issue-running", "In Progress")
				state.Running[running.ID] = Running{Issue: running}
			},
			want: true,
		},
		{
			name: "per-host cap full",
			cfg: Config{
				MaxConcurrentAgents:        2,
				ActiveStates:               []string{"Todo"},
				TerminalStates:             []string{"Done"},
				WorkerHosts:                []string{"worker-a"},
				MaxConcurrentAgentsPerHost: 1,
			},
			state: func(state State) {
				running := dispatchTestIssue("issue-running", "Todo")
				state.Running[running.ID] = Running{Issue: running, WorkerHost: "worker-a"}
			},
			want: false,
		},
		{
			name: "alternate host has capacity",
			cfg: Config{
				MaxConcurrentAgents:        3,
				ActiveStates:               []string{"Todo"},
				TerminalStates:             []string{"Done"},
				WorkerHosts:                []string{"worker-a", "worker-b"},
				MaxConcurrentAgentsPerHost: 1,
			},
			state: func(state State) {
				running := dispatchTestIssue("issue-running", "Todo")
				state.Running[running.ID] = Running{Issue: running, WorkerHost: "worker-a"}
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(tt.cfg)
			orch := Orchestrator{cfg: cfg}
			state := newState(cfg)
			if tt.state != nil {
				tt.state(state)
			}

			got := orch.dispatchable(issue, &state, now)
			if got != tt.want {
				t.Fatalf("dispatchable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDispatchableIssueDecisionCapacityReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		cfg   Config
		state func(*State)
		want  string
	}{
		{
			name: "project cap",
			cfg: Config{
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
			},
			state: func(state *State) {
				running := dispatchTestIssue("running", "Todo")
				state.Running[running.ID] = Running{Issue: running}
			},
			want: dispatchSkipGlobalCapacityFull,
		},
		{
			name: "lane cap",
			cfg: Config{
				MaxConcurrentAgents:        2,
				MaxConcurrentAgentsByState: map[string]int{"Todo": 1},
				ActiveStates:               []string{"Todo"},
				TerminalStates:             []string{"Done"},
			},
			state: func(state *State) {
				running := dispatchTestIssue("running", "Todo")
				state.Running[running.ID] = Running{Issue: running}
			},
			want: dispatchSkipLocalSlotUnavailable,
		},
		{
			name: "worker host cap",
			cfg: Config{
				MaxConcurrentAgents:        2,
				ActiveStates:               []string{"Todo"},
				TerminalStates:             []string{"Done"},
				WorkerHosts:                []string{"worker-a"},
				MaxConcurrentAgentsPerHost: 1,
			},
			state: func(state *State) {
				running := dispatchTestIssue("running", "Todo")
				state.Running[running.ID] = Running{Issue: running, WorkerHost: "worker-a"}
			},
			want: dispatchSkipWorkerHostUnavailable,
		},
		{
			name: "provider rate window",
			cfg: Config{
				MaxConcurrentAgents: 10,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				BillingMode:         workflowconfig.BillingModeSubscription,
			},
			state: func(state *State) {
				state.RateLimits = providerRateLimits(50, 100)
				for index := range 5 {
					running := dispatchTestIssue(fmt.Sprintf("running-%d", index), "Todo")
					state.Running[running.ID] = Running{Issue: running}
				}
			},
			want: dispatchSkipRateWindowBackpressure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(tt.cfg)
			state := newState(cfg)
			tt.state(&state)
			issue := dispatchTestIssue("candidate", "Todo")
			decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, time.Now(), "")
			if decision.dispatchable || decision.reason != tt.want {
				t.Fatalf("dispatchable decision = %#v, want skipped for %q", decision, tt.want)
			}
		})
	}
}

func TestDispatchableSkipsDuplicatePullRequestWork(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 3,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done"},
	})
	orch := Orchestrator{cfg: cfg}

	tests := []struct {
		name  string
		issue connector.Issue
		want  bool
	}{
		{
			name:  "todo without pull request dispatches",
			issue: dispatchTestIssue("issue-no-pr", "Todo"),
			want:  true,
		},
		{
			name:  "todo with open pull request skips",
			issue: dispatchTestIssueWithPullRequest("issue-todo-open-pr", "Todo", "OPEN"),
			want:  false,
		},
		{
			name:  "todo with unavailable pull request hydration skips",
			issue: dispatchTestIssueWithUnavailablePullRequestHydration("issue-todo-pr-rate-limited", "Todo"),
			want:  false,
		},
		{
			name:  "todo with unknown unavailable pull request hydration dispatches",
			issue: dispatchTestIssueWithUnknownUnavailablePullRequestHydration("issue-todo-pr-unknown", "Todo"),
			want:  true,
		},
		{
			name:  "in progress with open pull request dispatches",
			issue: dispatchTestIssueWithPullRequest("issue-progress-open-pr", "In Progress", "OPEN"),
			want:  true,
		},
		{
			name:  "rework with open pull request dispatches",
			issue: dispatchTestIssueWithPullRequest("issue-rework-open-pr", "Rework", "OPEN"),
			want:  true,
		},
		{
			name:  "rework with unavailable pull request hydration skips",
			issue: dispatchTestIssueWithUnavailablePullRequestHydration("issue-rework-pr-rate-limited", "Rework"),
			want:  false,
		},
		{
			name:  "merging with open pull request dispatches",
			issue: dispatchTestIssueWithPullRequest("issue-merging-open-pr", "Merging", "OPEN"),
			want:  true,
		},
		{
			name: "merging with degraded pull request hydration skips",
			issue: func() connector.Issue {
				issue := dispatchTestIssueWithPullRequest("issue-merging-pr-degraded", "Merging", "OPEN")
				issue.PullRequest.HydrationDegradedReason = connector.PullRequestHydrationReasonStaleCachedPullData
				return issue
			}(),
			want: false,
		},
		{
			name:  "todo with merged pull request skips",
			issue: dispatchTestIssueWithPullRequest("issue-todo-merged-pr", "Todo", "MERGED"),
			want:  false,
		},
		{
			name:  "rework with merged pull request skips",
			issue: dispatchTestIssueWithPullRequest("issue-rework-merged-pr", "Rework", "MERGED"),
			want:  false,
		},
		{
			name: "rework with failed merged pull request dispatches",
			issue: func() connector.Issue {
				issue := dispatchTestIssueWithPullRequest("issue-rework-merged-pr-failed", "Rework", "MERGED")
				issue.PullRequest.CIStatus = "fail"
				issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{
					Name:       "Test",
					Status:     "completed",
					Conclusion: "failure",
				}}
				return issue
			}(),
			want: true,
		},
		{
			name:  "todo with closed unmerged pull request dispatches",
			issue: dispatchTestIssueWithPullRequest("issue-todo-closed-pr", "Todo", "CLOSED"),
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(cfg)
			got := orch.dispatchable(tt.issue, &state, now)
			if got != tt.want {
				t.Fatalf("dispatchable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDispatchPlanReportsMergedPullRequestReconciliationPending(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 7, 15, 30, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 3,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done"},
	})
	issues := []connector.Issue{
		dispatchTestIssueWithPullRequest("issue-todo-merged-pr", "Todo", "MERGED"),
		func() connector.Issue {
			issue := dispatchTestIssueWithPullRequest("issue-todo-merged-pr-failed", "Todo", "MERGED")
			issue.PullRequest.CIStatus = "fail"
			issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{
				Name:       "Tier-1 Race Tests",
				Status:     "completed",
				Conclusion: "failure",
			}}
			return issue
		}(),
	}
	state := newState(cfg)
	decisions := make(map[string]dispatchPlanDecision)

	newDispatchPlanner(cfg).plan(&state, issues, now, dispatchPlanHooks{
		decision: func(decision dispatchPlanDecision) {
			decisions[decision.Issue.ID] = decision
		},
	})

	for _, issue := range issues {
		decision, ok := decisions[issue.ID]
		if !ok {
			t.Fatalf("decision for %s missing", issue.ID)
		}
		if decision.Selected {
			t.Fatalf("decision for %s selected = true, want reconciliation skip", issue.ID)
		}
		if decision.SkipReason != dispatchSkipMergedPullRequest {
			t.Fatalf("decision for %s skip reason = %q, want %q", issue.ID, decision.SkipReason, dispatchSkipMergedPullRequest)
		}
	}
}

func TestDispatchModeMergingFastPathFlag(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done"},
	})
	state := newState(cfg)
	issue := dispatchTestIssueWithPullRequest("issue-merging", "Merging", "OPEN")

	off := Orchestrator{cfg: cfg}
	if got := off.dispatchMode(context.Background(), &state, issue); got != runpkg.RunModeImplement {
		t.Fatalf("flag off dispatchMode = %q, want implement", got)
	}

	cfg.MergeFastPathEnabled = true
	on := Orchestrator{cfg: cfg}
	if got := on.dispatchMode(context.Background(), &state, issue); got != runpkg.RunModeMerge {
		t.Fatalf("flag on dispatchMode = %q, want merge", got)
	}
}

func TestDispatchCandidatesClaimsDuplicateIssueWithinCycle(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()
	candidate := dispatchTestIssue("issue-duplicate", "Todo")

	ctx := t.Context()

	orch.dispatchCandidates(ctx, &state, []connector.Issue{candidate, candidate}, now)
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != candidate.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, candidate.ID)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected duplicate dispatch = %#v", request)
	default:
	}
	if len(state.Running) != 1 {
		t.Fatalf("Running len = %d, want 1", len(state.Running))
	}
	if len(state.Claimed) != 1 {
		t.Fatalf("Claimed len = %d, want 1", len(state.Claimed))
	}
}

func TestDispatchReadyIssuesPassesPriorAttemptToRunner(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Rework"},
		TerminalStates:      []string{"Done"},
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Date(2026, 7, 2, 22, 0, 0, 0, time.UTC)
	issue := dispatchTestIssue("issue-prior-attempt", "Rework")
	state.PriorAttempts[issue.ID] = runpkg.PriorAttempt{
		Source: "auto_promote",
		Reason: "validator_rework",
		Validator: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictRework,
			Findings: []gate.Finding{{
				Severity: "p1",
				Body:     "Missing handoff.",
				Path:     "internal/runner/prompt.go",
				Line:     44,
			}},
		},
	}

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.PriorAttempt.Reason != "validator_rework" {
		t.Fatalf("PriorAttempt = %#v, want validator_rework", request.PriorAttempt)
	}
	if len(request.PriorAttempt.Validator.Findings) != 1 || request.PriorAttempt.Validator.Findings[0].Line != 44 {
		t.Fatalf("PriorAttempt.Validator.Findings = %#v", request.PriorAttempt.Validator.Findings)
	}
}

func TestDispatchReadyIssuesUsesLatestLaneTransitionContext(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Research", "Draft", "Review", "Package"},
		TerminalStates:      []string{"Publish"},
	})
	runner := newWorkerHostRunner()
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	orch := Orchestrator{
		cfg:             cfg,
		supervisor:      newTestSupervisor(t, runner, cfg),
		runResults:      make(chan runpkg.Completion),
		workflowMetrics: metrics,
	}
	state := newState(cfg)
	now := time.Date(2026, 7, 9, 15, 0, 0, 0, time.UTC)
	issue := dispatchTestIssue("issue-package", "Package")
	if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
		IssueID:           issue.ID,
		Identifier:        issue.Identifier,
		PhaseType:         store.WorkflowPhaseTypeLane,
		PhaseName:         "Package",
		PreviousPhaseName: "Review",
		Status:            "entered",
		StartedAt:         now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.DispatchSourceState != "Review" || request.DispatchTargetState != "Package" {
		t.Fatalf("RunRequest dispatch transition = %q -> %q, want Review -> Package", request.DispatchSourceState, request.DispatchTargetState)
	}
}

func TestDispatchReadyIssuesRechecksStartTransitionStateCapacity(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"In Progress": 1,
		},
		ActiveStates:   []string{"Todo", "In Progress"},
		TerminalStates: []string{"Done"},
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		connector:  hydratingDispatchConnector{},
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()
	first := dispatchTestIssue("issue-first", "Todo")
	first.Fields = map[string]string{"Status": "Todo"}
	second := dispatchTestIssue("issue-second", "Todo")
	second.Fields = map[string]string{"Status": "Todo"}

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{first, second}, now)

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != first.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, first.ID)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected second dispatch = %#v", request)
	default:
	}
	if len(state.Running) != 1 {
		t.Fatalf("Running len = %d, want 1", len(state.Running))
	}
	if got := state.Running[first.ID].Issue.State; got != "In Progress" {
		t.Fatalf("Running[%q].Issue.State = %q, want In Progress", first.ID, got)
	}
}

func TestDispatchIssueRequiresSharedGlobalSlot(t *testing.T) {
	t.Parallel()

	global := scheduler.NewRoundRobin(scheduler.Config{Capacity: 1})
	globalGate := scheduler.NewGlobalDispatchGate(global)
	now := time.Now()
	ctx := t.Context()

	alphaRunner := newWorkerHostRunner()
	alphaCfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "alpha", Weight: 1},
	})
	alpha := Orchestrator{
		cfg:                alphaCfg,
		supervisor:         newTestSupervisor(t, alphaRunner, alphaCfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
	}
	alphaState := newState(alphaCfg)
	alphaIssue := dispatchTestIssue("issue-alpha", "Todo")

	if !alpha.dispatchIssue(ctx, &alphaState, alphaIssue, 0, now, "") {
		t.Fatal("alpha dispatchIssue() = false, want true")
	}
	alphaRequest := receiveWorkerHostRunRequest(t, alphaRunner.started)
	if alphaRequest.Issue.ID != alphaIssue.ID {
		t.Fatalf("alpha RunRequest.Issue.ID = %q, want %q", alphaRequest.Issue.ID, alphaIssue.ID)
	}

	bravoRunner := newWorkerHostRunner()
	bravoCfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "bravo", Weight: 1},
	})
	bravo := Orchestrator{
		cfg:                bravoCfg,
		supervisor:         newTestSupervisor(t, bravoRunner, bravoCfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
	}
	bravoState := newState(bravoCfg)
	bravoIssue := dispatchTestIssue("issue-bravo", "Todo")

	if bravo.dispatchIssue(ctx, &bravoState, bravoIssue, 0, now, "") {
		t.Fatal("bravo dispatchIssue() = true while global slot is held, want false")
	}
	select {
	case request := <-bravoRunner.started:
		t.Fatalf("unexpected bravo dispatch while global slot is held = %#v", request)
	default:
	}

	alpha.handleRunResult(ctx, &alphaState, runpkg.Completion{
		IssueID:     alphaIssue.ID,
		CompletedAt: now.Add(time.Second),
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if !bravo.dispatchIssue(ctx, &bravoState, bravoIssue, 0, now.Add(2*time.Second), "") {
		t.Fatal("bravo dispatchIssue() after alpha completion = false, want true")
	}
	bravoRequest := receiveWorkerHostRunRequest(t, bravoRunner.started)
	if bravoRequest.Issue.ID != bravoIssue.ID {
		t.Fatalf("bravo RunRequest.Issue.ID = %q, want %q", bravoRequest.Issue.ID, bravoIssue.ID)
	}
}

func TestDispatchReadyIssuesAllowsOneMergeWorkerPerProject(t *testing.T) {
	t.Parallel()

	globalGate := scheduler.NewGlobalDispatchGate(scheduler.NewWeightedFair(scheduler.Config{Capacity: 2}))
	now := time.Date(2026, 6, 25, 13, 0, 0, 0, time.UTC)
	ctx := t.Context()

	alphaRunner := newWorkerHostRunner()
	alphaCfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		DispatchPriorityByState: []string{"Merging", "Rework", "Todo"},
		ActiveStates:            []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:          []string{"Done"},
		Project:                 scheduler.ProjectCandidate{ID: "alpha", Weight: 1},
	})
	alpha := Orchestrator{
		cfg:                alphaCfg,
		supervisor:         newTestSupervisor(t, alphaRunner, alphaCfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
	}
	alphaState := newState(alphaCfg)
	alphaFirst := dispatchTestIssueWithPullRequest("issue-alpha-first", "Merging", "OPEN")
	alphaSecond := dispatchTestIssueWithPullRequest("issue-alpha-second", "Merging", "OPEN")

	alpha.dispatchReadyIssues(ctx, &alphaState, []connector.Issue{alphaFirst, alphaSecond}, now)
	alphaRequest := receiveWorkerHostRunRequest(t, alphaRunner.started)
	if alphaRequest.Issue.ID != alphaFirst.ID {
		t.Fatalf("alpha RunRequest.Issue.ID = %q, want %q", alphaRequest.Issue.ID, alphaFirst.ID)
	}
	if len(alphaState.Running) != 1 {
		t.Fatalf("alpha Running len = %d, want 1", len(alphaState.Running))
	}

	bravoRunner := newWorkerHostRunner()
	bravoCfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		DispatchPriorityByState: []string{"Merging", "Rework", "Todo"},
		ActiveStates:            []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:          []string{"Done"},
		Project:                 scheduler.ProjectCandidate{ID: "bravo", Weight: 1},
	})
	bravo := Orchestrator{
		cfg:                bravoCfg,
		supervisor:         newTestSupervisor(t, bravoRunner, bravoCfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
	}
	bravoState := newState(bravoCfg)
	bravoIssue := dispatchTestIssueWithPullRequest("issue-bravo", "Merging", "OPEN")

	bravo.dispatchReadyIssues(ctx, &bravoState, []connector.Issue{bravoIssue}, now.Add(time.Second))
	bravoRequest := receiveWorkerHostRunRequest(t, bravoRunner.started)
	if bravoRequest.Issue.ID != bravoIssue.ID {
		t.Fatalf("bravo RunRequest.Issue.ID = %q, want %q", bravoRequest.Issue.ID, bravoIssue.ID)
	}
	if len(bravoState.Running) != 1 {
		t.Fatalf("bravo Running len = %d, want 1", len(bravoState.Running))
	}

	for _, running := range alphaState.Running {
		if running.cancel != nil {
			running.cancel()
		}
	}
	for _, running := range bravoState.Running {
		if running.cancel != nil {
			running.cancel()
		}
	}
}

func TestDispatchReadyIssuesLogsMergeSlotDecisionAndStopsAfterGlobalWait(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 14, 0, 0, 0, time.UTC)
	ctx := t.Context()
	globalGate := scheduler.NewGlobalDispatchGate(scheduler.NewWeightedFair(scheduler.Config{Capacity: 1}))
	lowerPriorityProject := scheduler.ProjectCandidate{ID: "alpha", Weight: 1}
	lowerSlot, ok, decision, err := globalGate.TryAcquireWithDecision(ctx, lowerPriorityProject, scheduler.SlotRequest{
		State:    "Todo",
		Priority: 2,
	}, now)
	if err != nil {
		t.Fatalf("lower-priority TryAcquireWithDecision() error = %v", err)
	}
	if !ok {
		t.Fatalf("lower-priority TryAcquireWithDecision() ok = false, want true; decision = %#v", decision)
	}
	t.Cleanup(func() {
		if err := globalGate.Release(lowerSlot); err != nil && !errors.Is(err, scheduler.ErrSlotNotHeld) {
			t.Fatalf("Release() error = %v", err)
		}
	})

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		DispatchPriorityByState: []string{"Merging", "Rework", "Todo"},
		ActiveStates:            []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:          []string{"Done"},
		Project:                 scheduler.ProjectCandidate{ID: "zulu", Weight: 1},
	})
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := Orchestrator{
		cfg:                cfg,
		supervisor:         newTestSupervisor(t, runner, cfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
		logger:             slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	first := dispatchTestIssueWithPullRequest("issue-merge-first", "Merging", "OPEN")
	second := dispatchTestIssueWithPullRequest("issue-merge-second", "Merging", "OPEN")

	orch.dispatchReadyIssues(ctx, &state, []connector.Issue{first, second}, now.Add(time.Second))

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected merge dispatch while global slot is held = %#v", request)
	default:
	}
	if len(state.Running) != 0 {
		t.Fatalf("Running len = %d, want 0", len(state.Running))
	}
	logText := logs.String()
	if count := strings.Count(logText, "merge_worker_slot_wait"); count != 1 {
		t.Fatalf("merge_worker_slot_wait count = %d, want 1; logs = %q", count, logText)
	}
	for _, fragment := range []string{
		"reason=global_capacity_full",
		"pool=default",
		"global_capacity=1",
		"global_used=1",
		"global_available=0",
		"project_state_capacity=1",
		"project_state_used=0",
		"project_state_available=1",
		"lower_priority_running=1",
		"selected_project_id=zulu",
		"selected_state=merging",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs %q missing fragment %q", logText, fragment)
		}
	}
	if strings.Contains(logText, "merge_worker_failure") {
		t.Fatalf("logs %q contain merge_worker_failure, want merge slot wait telemetry instead", logText)
	}
}

func TestDispatchReadyIssuesRecordsNonMergeSlotWaitTelemetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 10, 0, 0, 0, time.UTC)
	ctx := t.Context()
	globalGate := scheduler.NewGlobalDispatchGate(scheduler.NewWeightedFair(scheduler.Config{Capacity: 1}))
	lowerSlot, ok, decision, err := globalGate.TryAcquireWithDecision(ctx, scheduler.ProjectCandidate{ID: "alpha", Weight: 1}, scheduler.SlotRequest{
		State:    "Todo",
		Priority: 2,
	}, now)
	if err != nil {
		t.Fatalf("lower-priority TryAcquireWithDecision() error = %v", err)
	}
	if !ok {
		t.Fatalf("lower-priority TryAcquireWithDecision() ok = false, want true; decision = %#v", decision)
	}
	t.Cleanup(func() {
		if err := globalGate.Release(lowerSlot); err != nil && !errors.Is(err, scheduler.ErrSlotNotHeld) {
			t.Fatalf("Release() error = %v", err)
		}
	})

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:     2,
		DispatchPriorityByState: []string{"Merging", "Rework", "Todo"},
		ActiveStates:            []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:          []string{"Done"},
		Project:                 scheduler.ProjectCandidate{ID: "zulu", Weight: 1},
	})
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := Orchestrator{
		cfg:                cfg,
		supervisor:         newTestSupervisor(t, runner, cfg),
		runResults:         make(chan runpkg.Completion),
		globalDispatchGate: globalGate,
		logger:             slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	candidate := dispatchTestIssue("issue-rework", "Rework")

	orch.dispatchReadyIssues(ctx, &state, []connector.Issue{candidate}, now.Add(time.Second))

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected rework dispatch while global slot is held = %#v", request)
	default:
	}
	if len(state.Running) != 0 {
		t.Fatalf("Running len = %d, want 0", len(state.Running))
	}
	logText := logs.String()
	for _, fragment := range []string{
		"dispatch_slot_wait",
		"issue_id=issue-rework",
		"state=Rework",
		"reason=global_capacity_full",
		"global_capacity=1",
		"global_used=1",
		"global_available=0",
		"project_state_capacity=2",
		"project_state_used=0",
		"project_state_available=2",
		"selected_project_id=zulu",
		"selected_state=rework",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs %q missing fragment %q", logText, fragment)
		}
	}
	if strings.Contains(logText, "merge_worker_failure") {
		t.Fatalf("logs %q contain merge_worker_failure, want dispatch slot wait telemetry instead", logText)
	}
	if len(state.RecentEvents) != 1 {
		t.Fatalf("RecentEvents len = %d, want 1", len(state.RecentEvents))
	}
	event := state.RecentEvents[0]
	if event.Event != "dispatch_slot_wait" {
		t.Fatalf("RecentEvents[0].Event = %q, want dispatch_slot_wait", event.Event)
	}
	for _, fragment := range []string{
		"digitaldrywood/detent#issue-rework",
		"state=Rework",
		"reason=global_capacity_full",
		"global_available=0",
		"project_state_available=2",
		"selected_project_id=zulu",
	} {
		if !strings.Contains(event.Message, fragment) {
			t.Fatalf("RecentEvents[0].Message %q missing fragment %q", event.Message, fragment)
		}
	}
}

func TestRecordDispatchGateRefusalPersistsPoolArbitrationReasons(t *testing.T) {
	t.Parallel()

	reasons := []string{
		scheduler.DispatchGateReasonGlobalCapacityFull,
		scheduler.DispatchGateReasonReservedForHigherPriorityProject,
		scheduler.DispatchGateReasonReservedForHigherPriority,
		scheduler.DispatchGateReasonSelectedProjectWaiting,
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				Project:             scheduler.ProjectCandidate{ID: "cloud"},
			})
			attempts := &recordingWorkAttemptStore{}
			orch := Orchestrator{cfg: cfg, workAttempts: attempts}
			state := newState(cfg)
			issue := dispatchTestIssue("issue-cloud", "Todo")
			now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

			orch.recordDispatchGateRefusal(
				t.Context(),
				&state,
				issue,
				2,
				"worker-a",
				now,
				scheduler.DispatchGateDecision{
					PoolName:          "shared",
					Holders:           []string{"local"},
					Reason:            reason,
					GlobalCapacity:    5,
					GlobalUsed:        5,
					GlobalAvailable:   0,
					SelectedProjectID: "local",
					SelectedState:     "Merging",
				},
				projectStateSlotStats{capacity: 2, used: 1, available: 1},
			)

			if len(attempts.decisions) != 1 {
				t.Fatalf("scheduler decisions = %#v, want one gate refusal", attempts.decisions)
			}
			got := attempts.decisions[0]
			if got.Result != store.SchedulerDecisionResultSkipped ||
				got.Reason != reason ||
				got.WaitReason != reason ||
				got.ProjectID != "cloud" ||
				got.AttemptNumber != 2 ||
				got.WorkerHost != "worker-a" {
				t.Fatalf("scheduler decision = %#v, want durable %q refusal", got, reason)
			}
			for _, fragment := range []string{
				`"pool":"shared"`,
				`"holders":["local"]`,
				`"global_capacity":5`,
				`"global_used":5`,
				`"global_available":0`,
				`"selected_project_id":"local"`,
			} {
				if !strings.Contains(got.CapacitySnapshotJSON, fragment) {
					t.Fatalf("capacity snapshot %q missing %q", got.CapacitySnapshotJSON, fragment)
				}
			}
			if !strings.Contains(got.MetadataJSON, `"sample_interval_seconds":300`) {
				t.Fatalf("metadata = %q, want documented sampling interval", got.MetadataJSON)
			}
		})
	}
}

func TestRecordDispatchGateRefusalPersistsPressureCapacity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		configure  func(*State)
		wantReason string
	}{
		{
			name: "IO degraded capacity",
			configure: func(state *State) {
				state.IOPressure = telemetry.IOPressure{
					CapacityConstrained: true, EffectiveMaxConcurrentAgents: 1,
					ConstrainedSince: now.Add(-5 * time.Minute), ConstrainedForMS: 300000,
				}
			},
			wantReason: "I/O pressure has limited admission to 1 concurrent agent for 5m0s",
		},
		{
			name: "CPU degraded capacity",
			configure: func(state *State) {
				state.CPUPressure = telemetry.CPUPressure{
					CapacityConstrained: true, EffectiveMaxConcurrentAgents: 2,
					ConstrainedSince: now.Add(-time.Minute), ConstrainedForMS: 60000,
				}
			},
			wantReason: "CPU pressure has limited admission to 2 concurrent agents for 1m0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{MaxConcurrentAgents: 4, Project: scheduler.ProjectCandidate{ID: "detent"}})
			attempts := &recordingWorkAttemptStore{}
			orch := Orchestrator{cfg: cfg, workAttempts: attempts}
			state := newState(cfg)
			tt.configure(&state)
			orch.recordDispatchGateRefusal(
				t.Context(),
				&state,
				dispatchTestIssue("issue-pressure", "Todo"),
				1,
				"",
				now,
				scheduler.DispatchGateDecision{
					PoolName: scheduler.DefaultPoolName, Reason: scheduler.DispatchGateReasonPressureCapacityFull,
					GlobalCapacity: 4, GlobalUsed: 1, GlobalAvailable: 3,
					PressureCapacity: 1, PressureUsed: 1,
				},
				projectStateSlotStats{capacity: 4},
			)

			if len(attempts.decisions) != 1 {
				t.Fatalf("scheduler decisions = %#v, want one", attempts.decisions)
			}
			decision := attempts.decisions[0]
			if decision.Reason != scheduler.DispatchGateReasonPressureCapacityFull || decision.WaitReason != tt.wantReason {
				t.Fatalf("pressure decision = %#v", decision)
			}
			for _, fragment := range []string{
				`"pressure_capacity":1`,
				`"pressure_used":1`,
				`"pressure_available":0`,
				`"pressure_constrained_for_ms":`,
				`"pressure_reason":`,
			} {
				if !strings.Contains(decision.CapacitySnapshotJSON, fragment) {
					t.Fatalf("capacity snapshot %q missing %q", decision.CapacitySnapshotJSON, fragment)
				}
			}
		})
	}
}

func TestRecordDispatchGateRefusalSamplesEquivalentCandidates(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "cloud"},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	decision := scheduler.DispatchGateDecision{
		PoolName:        "shared",
		Reason:          scheduler.DispatchGateReasonGlobalCapacityFull,
		GlobalCapacity:  5,
		GlobalUsed:      5,
		GlobalAvailable: 0,
		Holders:         []string{"detent"},
	}
	projectStats := projectStateSlotStats{capacity: 2, used: 1, available: 1}

	orch.recordDispatchGateRefusal(t.Context(), &state, dispatchTestIssue("issue-a", "Todo"), 0, "", now, decision, projectStats)
	orch.recordDispatchGateRefusal(t.Context(), &state, dispatchTestIssue("issue-b", "Todo"), 0, "", now.Add(time.Minute), decision, projectStats)
	changedHolders := decision
	changedHolders.Holders = []string{"podcast"}
	orch.recordDispatchGateRefusal(t.Context(), &state, dispatchTestIssue("issue-c", "Todo"), 0, "", now.Add(2*time.Minute), changedHolders, projectStats)
	orch.recordDispatchGateRefusal(t.Context(), &state, dispatchTestIssue("issue-b", "Todo"), 0, "", now.Add(dispatchGateSampleInterval), decision, projectStats)

	if len(attempts.decisions) != 3 {
		t.Fatalf("scheduler decisions = %#v, want one sample per holder set and five-minute condition window", attempts.decisions)
	}
	if attempts.decisions[0].IssueID != "issue-a" ||
		attempts.decisions[1].IssueID != "issue-c" ||
		attempts.decisions[2].IssueID != "issue-b" {
		t.Fatalf(
			"sampled issues = %q/%q/%q, want holder change and representative candidate across windows",
			attempts.decisions[0].IssueID,
			attempts.decisions[1].IssueID,
			attempts.decisions[2].IssueID,
		)
	}
}

func TestDispatchReadyIssuesHydratesLightweightCandidateBeforeDependencyGate(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	})
	runner := newWorkerHostRunner()
	candidate := dispatchTestIssue("issue-lightweight", "Todo")
	candidate.Fields = map[string]string{}
	candidate.BlockedBy = nil
	hydrated := candidate
	hydrated.Fields = map[string]string{}
	hydrated.BlockedBy = []connector.BlockedRef{{
		Identifier: "digitaldrywood/detent#issue-blocker",
		State:      "In Progress",
	}}
	orch := Orchestrator{
		cfg:        cfg,
		connector:  hydratingDispatchConnector{issue: hydrated},
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()

	ctx := t.Context()

	orch.dispatchReadyIssues(ctx, &state, []connector.Issue{candidate}, now)
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected dispatch for hydrated blocked candidate = %#v", request)
	default:
	}
	blocked, ok := state.Blocked[candidate.ID]
	if !ok {
		t.Fatalf("Blocked[%q] missing after hydrated dependency gate", candidate.ID)
	}
	if blocked.Issue.BlockedBy[0].State != "In Progress" {
		t.Fatalf("blocked dependency state = %q, want In Progress", blocked.Issue.BlockedBy[0].State)
	}
}

func TestFilterImplementDependencyDeferralsUsesDurableAttemptHistory(t *testing.T) {
	t.Parallel()

	issue := implementProgressIssueWithoutPR()
	issue.Fields = map[string]string{"Status": "In Progress"}
	history := []store.WorkAttempt{implementProgressDependencyDeferralHistoryAttempt(1, "digitaldrywood/detent#134", "Todo")}
	tests := []struct {
		name          string
		blockerState  string
		historyErr    error
		wantCandidate bool
		wantDetail    string
	}{
		{name: "unresolved blocker suppresses worker after restart", blockerState: "Todo", wantDetail: "waiting on dependency digitaldrywood/detent#134"},
		{name: "terminal blocker releases worker after restart", blockerState: "Done", wantCandidate: true},
		{name: "history lookup failure suppresses worker", blockerState: "Todo", historyErr: errors.New("attempt store unavailable"), wantDetail: "dependency deferral history unavailable: attempt store unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1,
				Project:             scheduler.ProjectCandidate{ID: "detent"},
				ActiveStates:        []string{"Todo", "In Progress"},
				TerminalStates:      []string{"Done", "Cancelled"},
			})
			tracker := hydratingDispatchConnector{
				issue:    issue,
				blockers: []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: tt.blockerState}},
			}
			orch := Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: &recordingWorkAttemptStore{history: history, historyErr: tt.historyErr},
			}

			filtered := orch.filterImplementDependencyDeferrals(t.Context(), []connector.Issue{issue})
			if got := len(filtered) == 1; got != tt.wantCandidate {
				t.Fatalf("candidate retained = %v, want %v", got, tt.wantCandidate)
			}
			decisions := orch.workAttempts.(*recordingWorkAttemptStore).decisions
			if tt.wantCandidate {
				if len(decisions) != 0 {
					t.Fatalf("released candidate has refusals: %+v", decisions)
				}
			} else if len(decisions) != 1 || decisions[0].Reason != dispatchSkipBlockedByDependency || decisions[0].WaitReason != tt.wantDetail {
				t.Fatalf("dependency refusal = %+v, want detail %q", decisions, tt.wantDetail)
			}
		})
	}
}

func TestDispatchReadyIssuesDoesNotLaunchDurableDependencyDeferral(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 8, 19, 0, 0, 0, time.UTC)
	issue := implementProgressIssueWithoutPR()
	issue.AssignedToWorker = true
	issue.Fields = map[string]string{"Status": "In Progress"}
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		Project:             scheduler.ProjectCandidate{ID: "detent"},
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg: cfg,
		connector: hydratingDispatchConnector{
			issue:    issue,
			blockers: []connector.Issue{{ID: "blocker-134", Identifier: "digitaldrywood/detent#134", State: "Todo"}},
		},
		workAttempts: &recordingWorkAttemptStore{history: []store.WorkAttempt{
			implementProgressDependencyDeferralHistoryAttempt(1, "digitaldrywood/detent#134", "Todo"),
		}},
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
	}
	state := newState(cfg)
	if !orch.dispatchable(issue, &state, now) {
		t.Fatal("control candidate is not independently dispatchable")
	}

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected worker launch for durable dependency deferral: %#v", request)
	default:
	}
	if _, blocked := state.Blocked[issue.ID]; blocked {
		t.Fatalf("Blocked[%q] present for dependency deferral", issue.ID)
	}
}

func TestDispatchReadyIssuesLogsDebugDecisionAndWorkerLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "detent", Weight: 2, Priority: 10},
	})
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := Orchestrator{
		cfg:        cfg,
		connector:  hydratingDispatchConnector{},
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
	state := newState(cfg)
	running := dispatchTestIssueWithPullRequest("issue-running", "In Progress", "OPEN")
	running.Fields = map[string]string{"Status": "In Progress"}
	running.PullRequest.CIStatus = "pass"
	running.PullRequest.CheckRunCount = 3
	state.Running[running.ID] = Running{Issue: running, StartedAt: now.Add(-time.Minute)}
	selected := dispatchTestIssue("issue-selected", "Todo")
	selected.Fields = map[string]string{"Status": "Todo"}
	selected.UpdatedAt = timePointer(now.Add(-2 * time.Minute))
	orch.connector = hydratingDispatchConnector{issue: selected}

	ctx := t.Context()
	orch.dispatchReadyIssues(ctx, &state, []connector.Issue{running, selected}, now)

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != selected.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, selected.ID)
	}
	if state.Running[selected.ID].cancel == nil {
		t.Fatalf("Running[%q].cancel = nil, want cancellation hook", selected.ID)
	}
	state.Running[selected.ID].cancel()
	orch.handleRunResult(ctx, &state, runpkg.Completion{
		IssueID:     selected.ID,
		Request:     request,
		Err:         context.Canceled,
		CompletedAt: now.Add(time.Second),
	})

	logText := logs.String()
	for _, fragment := range []string{
		"scheduler_dispatch_decision",
		"skip_reason=already_running",
		"result=selected",
		"queue_position=2",
		"pr_ci_status=pass",
		"pr_check_run_count=3",
		"scheduler_dispatch_slot_decision",
		"outcome=acquired",
		"worker_slot_acquired",
		"worker_attempt_started",
		"worker_capacity_released",
		"worker_cancelled",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logText)
		}
	}
}

func TestDispatchIssueAcquiresDurableAttemptBeforeWorker(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 14, 45, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "detent"},
		WorkerHosts:         []string{"worker-a"},
	})
	runner := newWorkerHostRunner()
	attempts := &recordingWorkAttemptStore{nextID: 42}
	orch := Orchestrator{
		cfg:          cfg,
		supervisor:   newTestSupervisor(t, runner, cfg),
		runResults:   make(chan runpkg.Completion, 1),
		workAttempts: attempts,
	}
	state := newState(cfg)
	issue := dispatchTestIssue("issue-durable-attempt", "Todo")
	issue.Identifier = "digitaldrywood/detent#737"
	issue.URL = "https://github.com/digitaldrywood/detent/issues/737"

	if !orch.dispatchIssue(t.Context(), &state, issue, 2, now, "worker-a") {
		t.Fatal("dispatchIssue() = false, want true")
	}
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.WorkAttemptID != 42 {
		t.Fatalf("RunRequest.WorkAttemptID = %d, want 42", request.WorkAttemptID)
	}
	running := state.Running[issue.ID]
	if running.WorkAttemptID != 42 {
		t.Fatalf("Running.WorkAttemptID = %d, want 42", running.WorkAttemptID)
	}
	if len(attempts.starts) != 1 {
		t.Fatalf("work attempt starts len = %d, want 1", len(attempts.starts))
	}
	start := attempts.starts[0]
	if start.ProjectID != "detent" || start.IssueID != issue.ID || start.Identifier != issue.Identifier {
		t.Fatalf("work attempt start identity = %#v, want detent issue", start)
	}
	if start.WorkerType != "agent" || start.WorkerHost != "worker-a" || start.AttemptNumber != 2 {
		t.Fatalf("work attempt start worker = %#v, want agent worker-a attempt 2", start)
	}
	if !start.StartedAt.Equal(now) || !start.LeaseExpiresAt.After(now) {
		t.Fatalf("work attempt times = started %s lease %s, want started now and future lease", start.StartedAt, start.LeaseExpiresAt)
	}
	state.Running[issue.ID].cancel()
}

func TestHandleRunResultCompletesDurableAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 15, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "detent"},
	})
	attempts := &recordingWorkAttemptStore{}
	retrospector := &recordingRetrospector{}
	orch := Orchestrator{
		cfg:          cfg,
		runResults:   make(chan runpkg.Completion, 1),
		workAttempts: attempts,
		retrospector: retrospector,
	}
	state := newState(cfg)
	issue := dispatchTestIssue("issue-failed-attempt", "Todo")
	state.Running[issue.ID] = Running{
		Issue:         issue,
		Attempt:       2,
		StartedAt:     now.Add(-time.Minute),
		WorkerHost:    "worker-a",
		WorkAttemptID: 77,
	}
	errRun := errors.New("runner eof")

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:      issue.ID,
		Request:      RunRequest{Issue: issue, Attempt: 2, WorkAttemptID: 77},
		Err:          errRun,
		CompletedAt:  now,
		Retryable:    true,
		RetryAttempt: 3,
		RetryDelay:   time.Second,
	})

	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions len = %d, want 1", len(attempts.completions))
	}
	completion := attempts.completions[0]
	if completion.AttemptID != 77 || completion.TerminalState != store.WorkAttemptTerminalFailure {
		t.Fatalf("completion = %#v, want failed attempt 77", completion)
	}
	if completion.ErrorClass != "runner_error" || !strings.Contains(completion.ErrorMessage, "runner eof") {
		t.Fatalf("completion error = %q/%q, want runner_error with message", completion.ErrorClass, completion.ErrorMessage)
	}
	if _, ok := state.Retry[issue.ID]; !ok {
		t.Fatalf("Retry[%q] missing after failed durable attempt", issue.ID)
	}
	if !slices.Equal(retrospector.triggers, []string{"completion"}) {
		t.Fatalf("retrospector triggers = %v, want completion", retrospector.triggers)
	}
}

func TestDispatchReadyIssuesPersistsEveryCapacitySkip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 15, 15, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "detent"},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := Orchestrator{
		cfg:          cfg,
		runResults:   make(chan runpkg.Completion, 1),
		workAttempts: attempts,
	}
	state := newState(cfg)
	running := dispatchTestIssue("issue-running-capacity", "Todo")
	state.Running[running.ID] = Running{Issue: running, StartedAt: now.Add(-time.Minute)}
	first := dispatchTestIssue("issue-waiting-a", "Todo")
	second := dispatchTestIssue("issue-waiting-b", "Todo")

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{first, second}, now)

	if len(attempts.decisions) != 2 {
		t.Fatalf("decisions len = %d, want every skipped candidate: %#v", len(attempts.decisions), attempts.decisions)
	}
	for _, decision := range attempts.decisions {
		if decision.Result != store.SchedulerDecisionResultSkipped || decision.Reason != dispatchSkipGlobalCapacityFull {
			t.Fatalf("decision = %#v, want skipped global capacity", decision)
		}
	}
}

func TestDispatchReadyIssuesRecordsPostSelectionRefusal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 4, 19, 1, 6, 0, time.UTC)
	tests := []struct {
		name      string
		configure func(*Config, *State)
		want      string
	}{
		{
			name: "CPU pressure",
			configure: func(_ *Config, state *State) {
				state.CPUPressure = telemetry.CPUPressure{
					Supported:        true,
					Some:             telemetry.PressureAverages{Avg10: 84.78},
					SomeAvg10Max:     80,
					DispatchHeld:     true,
					ConstrainedSince: now.Add(-time.Minute),
				}
			},
			want: dispatchIssueFailureCPUPressure,
		},
		{
			name: "claim rejected",
			configure: func(cfg *Config, _ *State) {
				cfg.Claiming = ClaimingConfig{Enabled: true, LeaseField: "lease"}
			},
			want: dispatchIssueFailureClaimFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1,
				ActiveStates:        []string{"Todo"},
				TerminalStates:      []string{"Done"},
				Project:             scheduler.ProjectCandidate{ID: "detent"},
			})
			state := newState(cfg)
			tt.configure(&cfg, &state)
			attempts := &recordingWorkAttemptStore{}
			orch := Orchestrator{cfg: cfg, workAttempts: attempts, now: func() time.Time { return now }}
			issue := dispatchTestIssue("issue-post-selection-refusal", "Todo")

			orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)

			if len(attempts.starts) != 0 {
				t.Fatalf("work attempt starts = %#v, want none", attempts.starts)
			}
			if len(attempts.decisions) != 2 {
				t.Fatalf("scheduler decisions = %#v, want selection and refusal", attempts.decisions)
			}
			selected := attempts.decisions[0]
			refused := attempts.decisions[1]
			if selected.Result != store.SchedulerDecisionResultSelected || !selected.Selected {
				t.Fatalf("selected decision = %#v", selected)
			}
			if refused.Result != store.SchedulerDecisionResultSkipped || refused.Selected || refused.Reason != tt.want {
				t.Fatalf("refusal decision = %#v, want skipped %q", refused, tt.want)
			}
			if !selected.DecisionAt.Equal(refused.DecisionAt) || selected.AttemptNumber != refused.AttemptNumber {
				t.Fatalf("decision correlation = selected %#v refused %#v", selected, refused)
			}
		})
	}
}

func TestDispatchReadyIssuesPreservesConcretePressureRefusal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 4, 19, 1, 6, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             scheduler.ProjectCandidate{ID: "detent"},
	})
	globalGate := scheduler.NewGlobalDispatchGate(
		scheduler.NewRoundRobin(scheduler.Config{Capacity: 2}),
		cfg.Project,
	)
	heldSlot, ok, decision, err := globalGate.TryAcquireWithDecision(
		t.Context(),
		cfg.Project,
		scheduler.SlotRequest{State: "Todo"},
		now.Add(-time.Minute),
	)
	if err != nil || !ok {
		t.Fatalf("TryAcquireWithDecision() = %#v, %v, want acquired slot; decision = %#v", heldSlot, err, decision)
	}
	t.Cleanup(func() {
		if err := globalGate.Release(heldSlot); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	})

	state := newState(cfg)
	state.CPUPressure = telemetry.CPUPressure{
		Supported:                    true,
		Some:                         telemetry.PressureAverages{Avg10: 84.78},
		SomeAvg10Max:                 80,
		CapacityConstrained:          true,
		EffectiveMaxConcurrentAgents: 1,
		ConstrainedSince:             now.Add(-time.Minute),
	}
	attempts := &recordingWorkAttemptStore{}
	orch := Orchestrator{
		cfg:                cfg,
		workAttempts:       attempts,
		globalDispatchGate: globalGate,
		now:                func() time.Time { return now },
	}
	issue := dispatchTestIssue("issue-pressure-capacity", "Todo")

	orch.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)

	if len(attempts.starts) != 0 {
		t.Fatalf("work attempt starts = %#v, want none", attempts.starts)
	}
	if len(attempts.decisions) != 3 {
		t.Fatalf("scheduler decisions = %#v, want selection, gate refusal, and concrete pressure refusal", attempts.decisions)
	}
	selected, gateRefusal, pressureRefusal := attempts.decisions[0], attempts.decisions[1], attempts.decisions[2]
	if selected.Result != store.SchedulerDecisionResultSelected || !selected.Selected {
		t.Fatalf("selected decision = %#v", selected)
	}
	if gateRefusal.Reason != scheduler.DispatchGateReasonPressureCapacityFull {
		t.Fatalf("gate refusal = %#v, want %q", gateRefusal, scheduler.DispatchGateReasonPressureCapacityFull)
	}
	if pressureRefusal.Result != store.SchedulerDecisionResultSkipped || pressureRefusal.Selected || pressureRefusal.Reason != dispatchIssueFailureCPUPressure {
		t.Fatalf("pressure refusal = %#v, want skipped %q", pressureRefusal, dispatchIssueFailureCPUPressure)
	}
	if !selected.DecisionAt.Equal(pressureRefusal.DecisionAt) || selected.AttemptNumber != pressureRefusal.AttemptNumber {
		t.Fatalf("decision correlation = selected %#v pressure refusal %#v", selected, pressureRefusal)
	}
}

func TestDispatchReadyIssuesStaggersContinuationDispatches(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		ActiveStates:        []string{"Todo", "In Progress"},
		TerminalStates:      []string{"Done"},
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()
	first := dispatchTestIssueWithPullRequest("issue-first", "In Progress", "OPEN")
	second := dispatchTestIssueWithPullRequest("issue-second", "In Progress", "OPEN")

	ctx := t.Context()

	done := make(chan struct{})
	go func() {
		defer close(done)
		orch.dispatchReadyIssues(ctx, &state, []connector.Issue{first, second}, now)
	}()

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != first.ID {
		t.Fatalf("first RunRequest.Issue.ID = %q, want %q", request.Issue.ID, first.ID)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected unstaggered continuation dispatch = %#v", request)
	default:
	}

	request = receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != second.ID {
		t.Fatalf("second RunRequest.Issue.ID = %q, want %q", request.Issue.ID, second.ID)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatchReadyIssues to finish")
	}
}

func TestContinuationDelayUsesConstantGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		index int
		want  time.Duration
	}{
		{index: -1, want: 0},
		{index: 0, want: 0},
		{index: 1, want: continuationDispatchBackoff},
		{index: 2, want: continuationDispatchBackoff},
		{index: 50, want: continuationDispatchBackoff},
	}

	for _, tt := range tests {
		got := continuationDelay(tt.index)
		if got != tt.want {
			t.Fatalf("continuationDelay(%d) = %s, want %s", tt.index, got, tt.want)
		}
	}
}

func TestDispatchCandidatesAssignsLeastLoadedWorkerHost(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:        3,
		ActiveStates:               []string{"Todo"},
		TerminalStates:             []string{"Done"},
		WorkerHosts:                []string{"worker-a", "worker-b"},
		MaxConcurrentAgentsPerHost: 1,
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()
	running := dispatchTestIssue("issue-running", "Todo")
	state.Running[running.ID] = Running{Issue: running, WorkerHost: "worker-a"}
	candidate := dispatchTestIssue("issue-candidate", "Todo")

	ctx := t.Context()

	orch.dispatchCandidates(ctx, &state, []connector.Issue{candidate}, now)
	request := receiveWorkerHostRunRequest(t, runner.started)

	if request.WorkerHost != "worker-b" {
		t.Fatalf("RunRequest.WorkerHost = %q, want worker-b", request.WorkerHost)
	}
	if got := state.Running[candidate.ID].WorkerHost; got != "worker-b" {
		t.Fatalf("Running[%q].WorkerHost = %q, want worker-b", candidate.ID, got)
	}
}

func TestDispatchIssueIncludesSelectorContext(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		SelectorPersona:     " persona-reviewer ",
	})
	runner := newWorkerHostRunner()
	orch := Orchestrator{
		cfg:        cfg,
		connector:  selectorContextConnector{login: "worker-1"},
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion),
	}
	state := newState(cfg)
	now := time.Now()
	issue := dispatchTestIssue("issue-selector-context", "Todo")

	ctx := t.Context()

	orch.dispatchIssue(ctx, &state, issue, 0, now, "")
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.SelectorContext.InstanceLogin != "worker-1" {
		t.Fatalf("SelectorContext.InstanceLogin = %q, want worker-1", request.SelectorContext.InstanceLogin)
	}
	if request.SelectorContext.Persona != "persona-reviewer" {
		t.Fatalf("SelectorContext.Persona = %q, want persona-reviewer", request.SelectorContext.Persona)
	}
}

func TestDispatchIssueClearsReapedWorkspaceMarker(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
	})
	orch := Orchestrator{
		cfg:        cfg,
		supervisor: newTestSupervisor(t, FakeRunner{}, cfg),
		runResults: make(chan runpkg.Completion, 1),
	}
	state := newState(cfg)
	now := time.Now()
	issue := dispatchTestIssue("issue-reopened", "Todo")
	state.ReapedWorkspaces[issue.ID] = now.Add(-time.Hour)

	if !orch.dispatchIssue(context.Background(), &state, issue, 0, now, "") {
		t.Fatal("dispatchIssue() = false, want true")
	}
	if _, ok := state.ReapedWorkspaces[issue.ID]; ok {
		t.Fatalf("ReapedWorkspaces[%q] present after dispatch", issue.ID)
	}
}

func TestSelectWorkerHostKeepsPreferredHostWhenAvailable(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:        3,
		ActiveStates:               []string{"Todo"},
		TerminalStates:             []string{"Done"},
		WorkerHosts:                []string{"worker-a", "worker-b"},
		MaxConcurrentAgentsPerHost: 2,
	})
	orch := Orchestrator{cfg: cfg}
	state := newState(cfg)
	running := dispatchTestIssue("issue-running", "Todo")
	state.Running[running.ID] = Running{Issue: running, WorkerHost: "worker-a"}

	host, ok := orch.selectWorkerHost(&state, "worker-a")
	if !ok {
		t.Fatal("selectWorkerHost() ok = false, want true")
	}
	if host != "worker-a" {
		t.Fatalf("selectWorkerHost() host = %q, want worker-a", host)
	}
}

func dispatchTestIssue(id, state string) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = "digitaldrywood/detent#" + id
	issue.Title = "Dispatch test issue"
	issue.State = state
	return issue
}

func dispatchTestIssueWithPullRequest(id, state, prState string) connector.Issue {
	issue := dispatchTestIssue(id, state)
	issue.PullRequest = &connector.PullRequest{
		Number:     187,
		URL:        "https://github.com/digitaldrywood/detent/pull/187",
		BranchName: "detent/digitaldrywood_detent_187",
		State:      prState,
	}
	return issue
}

func dispatchTestIssueWithUnavailablePullRequestHydration(id, state string) connector.Issue {
	issue := dispatchTestIssueWithPullRequest(id, state, "OPEN")
	issue.PullRequest.HydrationUnavailableReason = "rate_limited"
	return issue
}

func dispatchTestIssueWithUnknownUnavailablePullRequestHydration(id, state string) connector.Issue {
	issue := dispatchTestIssue(id, state)
	issue.PullRequest = &connector.PullRequest{
		HydrationUnavailableReason: "rest_budget_reserved",
	}
	return issue
}

func assertIssueFilterHint(t *testing.T, got connector.IssueFilterHint, want connector.IssueFilterHint) {
	t.Helper()

	if !slices.Equal(got.Authors, want.Authors) {
		t.Fatalf("Authors = %#v, want %#v", got.Authors, want.Authors)
	}
	if !slices.Equal(got.Assignees, want.Assignees) {
		t.Fatalf("Assignees = %#v, want %#v", got.Assignees, want.Assignees)
	}
	if !slices.Equal(got.LabelInclude, want.LabelInclude) {
		t.Fatalf("LabelInclude = %#v, want %#v", got.LabelInclude, want.LabelInclude)
	}
	if !slices.Equal(got.LabelExclude, want.LabelExclude) {
		t.Fatalf("LabelExclude = %#v, want %#v", got.LabelExclude, want.LabelExclude)
	}
}

func dispatchPlanIssueIDs(cfg Config, candidates []connector.Issue) []string {
	cfg = normalizeConfig(cfg)
	state := newState(cfg)
	plan := newDispatchPlanner(cfg).plan(&state, candidates, time.Now(), dispatchPlanHooks{})
	ids := make([]string, 0, len(plan.Dispatches))
	for _, dispatch := range plan.Dispatches {
		ids = append(ids, dispatch.IssueID)
	}
	return ids
}

type filterFetchConnector struct {
	issues      []connector.Issue
	states      []string
	hint        connector.IssueFilterHint
	baseFetches int
}

func (c *filterFetchConnector) Name() string {
	return "filter-fetch"
}

func (c *filterFetchConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	c.baseFetches++
	return cloneIssues(c.issues), nil
}

func (c *filterFetchConnector) FetchCandidateIssuesByStatesWithFilter(
	_ context.Context,
	states []string,
	hint connector.IssueFilterHint,
) ([]connector.Issue, error) {
	c.states = append([]string(nil), states...)
	c.hint = connector.IssueFilterHint{
		Authors:      append([]string(nil), hint.Authors...),
		Assignees:    append([]string(nil), hint.Assignees...),
		LabelInclude: append([]string(nil), hint.LabelInclude...),
		LabelExclude: append([]string(nil), hint.LabelExclude...),
	}
	return cloneIssues(c.issues), nil
}

func (c *filterFetchConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *filterFetchConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *filterFetchConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c *filterFetchConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c *filterFetchConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *filterFetchConnector) SetField(context.Context, string, string, string) error {
	return nil
}

type budgetRefusalComment struct {
	issueID string
	body    string
}

type budgetRefusalCommentConnector struct {
	comments []budgetRefusalComment
}

func (c *budgetRefusalCommentConnector) Name() string {
	return "budget-refusal-comment"
}

func (c *budgetRefusalCommentConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *budgetRefusalCommentConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *budgetRefusalCommentConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *budgetRefusalCommentConnector) CreateComment(_ context.Context, issueID string, body string) error {
	c.comments = append(c.comments, budgetRefusalComment{issueID: issueID, body: body})
	return nil
}

func (c *budgetRefusalCommentConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c *budgetRefusalCommentConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *budgetRefusalCommentConnector) SetField(context.Context, string, string, string) error {
	return nil
}

var _ connector.Connector = (*budgetRefusalCommentConnector)(nil)

type hydratingDispatchConnector struct {
	fetches  *int
	issue    connector.Issue
	blockers []connector.Issue
}

func (c hydratingDispatchConnector) Name() string {
	return "hydrating"
}

func (c hydratingDispatchConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return []connector.Issue{c.issue}, nil
}

func (c hydratingDispatchConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c hydratingDispatchConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	if c.fetches != nil {
		*c.fetches++
	}
	if slices.Contains(ids, c.issue.ID) {
		return []connector.Issue{c.issue}, nil
	}
	return nil, nil
}

func (c hydratingDispatchConnector) FetchIssueStatesByIdentifiers(context.Context, []string) ([]connector.Issue, error) {
	return cloneIssues(c.blockers), nil
}

func (c hydratingDispatchConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (c hydratingDispatchConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (c hydratingDispatchConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c hydratingDispatchConnector) SetField(context.Context, string, string, string) error {
	return nil
}

type workerHostRunner struct {
	started chan RunRequest
}

type selectorContextConnector struct {
	connector.Connector
	login string
}

func (c selectorContextConnector) InstanceLogin() string {
	return c.login
}

func newWorkerHostRunner() *workerHostRunner {
	return &workerHostRunner{started: make(chan RunRequest, 1)}
}

func (r *workerHostRunner) Run(ctx context.Context, request RunRequest) (RunResult, error) {
	select {
	case r.started <- request:
	case <-ctx.Done():
		return RunResult{}, ctx.Err()
	}

	<-ctx.Done()
	return RunResult{}, ctx.Err()
}

func receiveWorkerHostRunRequest(t *testing.T, requests <-chan RunRequest) RunRequest {
	t.Helper()

	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker host run request")
	}

	return RunRequest{}
}

type recordingWorkAttemptStore struct {
	nextID                  int64
	starts                  []store.WorkAttemptStart
	heartbeats              []store.WorkAttemptHeartbeat
	completions             []store.WorkAttemptCompletion
	completionErrors        []error
	decisions               []store.SchedulerDecision
	reclaimed               []store.WorkAttempt
	recent                  []store.WorkAttempt
	history                 []store.WorkAttempt
	historyErr              error
	historyQueries          []store.WorkAttemptHistoryQuery
	pendingCapacityReleases []store.WorkAttempt
	clearedCapacityReleases []int64
}

type failingCapacityReleaseConnector struct {
	claimTestConnector
	err error
}

func (c failingCapacityReleaseConnector) SetField(context.Context, string, string, string) error {
	return c.err
}

type recordingRetrospector struct {
	triggers []string
}

func (r *recordingRetrospector) Trigger(trigger string) {
	r.triggers = append(r.triggers, trigger)
}

func (s *recordingWorkAttemptStore) StartWorkAttempt(_ context.Context, attrs store.WorkAttemptStart) (int64, error) {
	s.starts = append(s.starts, attrs)
	if s.nextID <= 0 {
		s.nextID = 1
	}
	id := s.nextID
	s.nextID++
	return id, nil
}

func (s *recordingWorkAttemptStore) WorkAttempt(context.Context, int64) (store.WorkAttempt, error) {
	return store.WorkAttempt{}, store.ErrNotFound
}

func (s *recordingWorkAttemptStore) RecordWorkAttemptHeartbeat(_ context.Context, attrs store.WorkAttemptHeartbeat) error {
	s.heartbeats = append(s.heartbeats, attrs)
	return nil
}

func (s *recordingWorkAttemptStore) CompleteWorkAttempt(_ context.Context, attrs store.WorkAttemptCompletion) error {
	s.completions = append(s.completions, attrs)
	if len(s.completionErrors) > 0 {
		err := s.completionErrors[0]
		s.completionErrors = s.completionErrors[1:]
		return err
	}
	return nil
}

func (s *recordingWorkAttemptStore) TimeoutExpiredWorkAttempts(context.Context, store.WorkAttemptTimeout) ([]store.WorkAttempt, error) {
	return append([]store.WorkAttempt(nil), s.reclaimed...), nil
}

func (s *recordingWorkAttemptStore) ReclaimActiveWorkAttempts(context.Context, store.WorkAttemptReclaim) ([]store.WorkAttempt, error) {
	return append([]store.WorkAttempt(nil), s.reclaimed...), nil
}

func (s *recordingWorkAttemptStore) ListActiveWorkAttempts(context.Context, store.WorkAttemptQuery) ([]store.WorkAttempt, error) {
	return nil, nil
}

func (s *recordingWorkAttemptStore) ListPendingWorkAttemptCapacityReleases(context.Context, string) ([]store.WorkAttempt, error) {
	return append([]store.WorkAttempt(nil), s.pendingCapacityReleases...), nil
}

func (s *recordingWorkAttemptStore) ClearWorkAttemptCapacityRelease(_ context.Context, attemptID int64) error {
	s.clearedCapacityReleases = append(s.clearedCapacityReleases, attemptID)
	return nil
}

func (s *recordingWorkAttemptStore) ListRecentTerminalWorkAttempts(_ context.Context, query store.WorkAttemptHistoryQuery) ([]store.WorkAttempt, error) {
	s.historyQueries = append(s.historyQueries, query)
	if s.historyErr != nil {
		return nil, s.historyErr
	}
	if s.history == nil {
		return append([]store.WorkAttempt(nil), s.recent...), nil
	}
	return append([]store.WorkAttempt(nil), s.history...), nil
}

func (s *recordingWorkAttemptStore) RecordSchedulerDecision(_ context.Context, attrs store.SchedulerDecision) (int64, error) {
	s.decisions = append(s.decisions, attrs)
	return int64(len(s.decisions)), nil
}

func (s *recordingWorkAttemptStore) ListRecentSchedulerDecisions(context.Context, store.SchedulerDecisionQuery) ([]store.SchedulerDecision, error) {
	return nil, nil
}

func TestHydrateDispatchIssueSkipsPausedProvider(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		outage         bool
		knownScope     bool
		unrelatedScope bool
		resumeAt       time.Time
		probeAt        time.Time
		probeIssue     string
		wantFetches    int
	}{
		{name: "healthy", wantFetches: 1},
		{name: "unrelated recorded scope", outage: true, knownScope: true, unrelatedScope: true, probeAt: now.Add(time.Minute), wantFetches: 1},
		{name: "waiting for resume without probe time", outage: true, knownScope: true, resumeAt: now.Add(time.Minute)},
		{name: "unknown route with paused fallback", outage: true, probeAt: now.Add(time.Minute), wantFetches: 1},
		{name: "outage waiting", outage: true, knownScope: true, probeAt: now.Add(time.Minute)},
		{name: "probe already running", outage: true, knownScope: true, probeAt: now, probeIssue: "probe"},
		{name: "recovery probe due", outage: true, knownScope: true, probeAt: now, wantFetches: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
			issue := connector.Issue{ID: "issue", State: "Todo"}
			fetches := 0
			cfg := normalizeConfig(Config{})
			orch := Orchestrator{cfg: cfg, connector: hydratingDispatchConnector{issue: issue, fetches: &fetches}, capacityController: backendCapacityTestController{scope: scope}}
			state := newState(cfg)
			if tt.knownScope {
				retryScope := scope
				if tt.unrelatedScope {
					retryScope.BackendID = "healthy"
				}
				state.Retry[issue.ID] = Retry{Issue: issue, CapacityScope: retryScope}
			}
			if tt.outage {
				state.BackendOutages[scope.Key()] = BackendOutage{Scope: scope, NextProbeAt: tt.probeAt, ResumeAt: tt.resumeAt, ProbeIssueID: tt.probeIssue}
			}
			for range 3 {
				if _, ok := orch.hydrateDispatchIssue(t.Context(), &state, issue, now); !ok {
					t.Fatal("hydration failed")
				}
			}
			if fetches != tt.wantFetches*3 {
				t.Fatalf("fetches = %d, want %d", fetches, tt.wantFetches*3)
			}
		})
	}
}
