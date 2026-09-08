package orchestrator

import (
	"slices"
	"strings"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/efficiency"
	"github.com/digitaldrywood/detent/internal/forgeavailability"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/staleness"
)

func ConfigFromWorkflow(cfg workflowconfig.Config) Config {
	identity := cfg.Identity
	identity.Normalize()
	admissionTargetState := ""
	if cfg.BacklogAdmission.Enabled {
		admissionTargetState = cfg.BacklogAdmission.TargetState
	}

	return Config{
		Policy:                     cfg.Policy,
		PollInterval:               durationFromMillis(cfg.Polling.IntervalMS),
		RefreshFailureThreshold:    cfg.Polling.RefreshFailureThreshold,
		MaxConcurrentAgents:        cfg.Agent.MaxConcurrentAgents,
		MaxConcurrentAgentsByState: cloneStateLimits(cfg.Agent.MaxConcurrentAgentsByState),
		DispatchPriorityByState:    append([]string(nil), cfg.Agent.DispatchPriorityByState...),
		DispatchPriorityByLabel:    append([]string(nil), cfg.Agent.DispatchPriorityByLabel...),
		PrioritizeUnblockers:       cfg.Agent.PrioritizeUnblockers,
		MergeFastPathEnabled:       cfg.Agent.MergeFastPath.Enabled,
		MergeFairnessAge:           durationFromSeconds(cfg.Agent.MergeFastPath.FairnessAgeSeconds),
		MergeMethod:                cfg.Deliverable.EffectiveMergeMethod(),
		DeliverableKind:            cfg.Deliverable.Kind,
		MergeWorkerStartupTimeout:  durationFromMillis(cfg.Agent.MergeWorkerStartupTimeoutMS),
		MergeWorkerMaxDuration:     durationFromMillis(cfg.Agent.MergeWorkerMaxDurationMS),
		ResumeOrphanedSessions:     cfg.Agent.ResumeOrphanedSessions,
		StopRunTargetState:         cfg.Agent.StopRun.TargetState,
		StopRunPriorityNames:       stopRunPriorityNames(cfg.Tracker.PriorityMap),
		MaxConcurrentAgentsPerHost: positiveIntValue(cfg.Worker.MaxConcurrentAgentsPerHost),
		MaxRetryBackoff:            durationFromMillis(cfg.Agent.MaxRetryBackoffMS),
		Recovery:                   cfg.Recovery,
		OverloadRetryDelay:         durationFromMillis(cfg.Agent.OverloadRetryDelayMS),
		NoProgressTokenLimit:       cfg.Agent.NoProgressTokenLimit,
		NoProgressSpendLimitUSD:    cfg.Agent.NoProgressSpendLimitUSD,
		LifetimeSessionLimit:       cfg.Agent.LifetimeSessionLimit,
		LifetimeTokenLimit:         cfg.Agent.LifetimeTokenLimit,
		LifetimeLimitCooldown:      durationFromSeconds(cfg.Agent.LifetimeLimitCooldownSeconds),
		LifetimeLimitOverrideLabel: normalizeLabel(cfg.Agent.LifetimeLimitOverrideLabel),
		BillingMode:                cfg.Budget.EffectiveBillingMode(),
		RateWindowPacing:           cfg.Agent.RateWindowPacing.Normalized(),
		FailureBreaker: FailureBreakerConfig{
			SameClassLimit: cfg.Agent.FailureBreaker.SameClassLimit,
			Window:         durationFromSeconds(cfg.Agent.FailureBreaker.WindowSeconds),
			Cooldown:       durationFromSeconds(cfg.Agent.FailureBreaker.CooldownSeconds),
		},
		Claiming: ClaimingConfig{
			Enabled:           cfg.Tracker.Claims.Enabled,
			OwnershipSet:      identity.Configured(),
			OwnershipMode:     identity.OwnershipMode,
			AssigneeRequired:  identity.AssigneeRequired,
			Owner:             identity.Name,
			AssigneeLogin:     identity.GitHubLogin,
			OwnerField:        identity.OwnerField,
			LeaseField:        cfg.Tracker.Claims.LeaseField,
			LeaseTTL:          durationFromSeconds(cfg.Tracker.Claims.TTLSeconds),
			HeartbeatInterval: durationFromSeconds(cfg.Tracker.Claims.HeartbeatSeconds),
		},
		AutoPromote: normalizeAutoPromoteConfig(AutoPromoteConfig{
			Enabled:               cfg.Agent.AutoPromote.Enabled,
			QuietDuration:         durationFromSeconds(cfg.Agent.AutoPromote.QuietSeconds),
			OptoutLabel:           cfg.Agent.AutoPromote.OptoutLabel,
			AllowedIssueLabels:    append([]string(nil), cfg.Agent.AutoPromote.AllowedIssueLabels...),
			GateWaitState:         cfg.Agent.AutoPromote.GateWaitState,
			GateWaitTimeout:       durationFromSeconds(cfg.Agent.AutoPromote.GateWaitTimeoutSeconds),
			GateWaitTimeoutAction: cfg.Agent.AutoPromote.GateWaitTimeoutAction,
			SourceState:           cfg.Agent.AutoPromote.SourceState,
			PassState:             cfg.Agent.AutoPromote.PassState,
			ReworkState:           cfg.Agent.AutoPromote.ReworkState,
			ReworkLimit:           cfg.Agent.AutoPromote.ReworkLimit,
			TerminalStates:        append([]string(nil), cfg.Tracker.TerminalStates...),
			NoProgressLimit:       cfg.Agent.AutoPromote.NoProgressLimit,
			WorkpadStructuredOnly: cfg.Workpad.StructuredOnly,
			Gate:                  gate.Effective(cfg.Gate),
		}),
		Plan:              gate.EffectivePlan(cfg.Plan),
		DependencySource:  normalizeDependencySource(cfg.Dependencies.Source),
		StatusLabelPrefix: blockedCauseStatusLabelPrefix(cfg),
		DependencyAutoUnblock: normalizeDependencyAutoUnblockConfig(DependencyAutoUnblockConfig{
			Enabled:      cfg.Tracker.DependencyAutoUnblock.Enabled,
			SourceStates: append([]string(nil), cfg.Tracker.DependencyAutoUnblock.SourceStates...),
			TargetState:  cfg.Tracker.DependencyAutoUnblock.TargetState,
			Readiness:    cfg.Tracker.DependencyAutoUnblock.Readiness,
		}),
		BlockedRecovery: normalizeBlockedRecoveryConfig(BlockedRecoveryConfig{
			Enabled:         cfg.Tracker.BlockedRecovery.Enabled,
			SourceStates:    append([]string(nil), cfg.Tracker.BlockedRecovery.SourceStates...),
			TargetState:     cfg.Tracker.BlockedRecovery.TargetState,
			ReasonCodes:     append([]string(nil), cfg.Tracker.BlockedRecovery.ReasonCodes...),
			BreakerCooldown: durationFromSeconds(cfg.Tracker.BlockedRecovery.BreakerCooldownSeconds),
		}),
		BlockerAutoPromote: BlockerAutoPromoteConfig{
			Enabled:       cfg.Tracker.BlockerAutoPromote.Enabled,
			SourceStates:  append([]string(nil), cfg.Tracker.BlockerAutoPromote.SourceStates...),
			BlockerStates: append([]string(nil), cfg.Tracker.BlockerAutoPromote.BlockerStates...),
			TargetState:   cfg.Tracker.BlockerAutoPromote.TargetState,
		},
		AdmissionTargetState:          admissionTargetState,
		ActiveStates:                  append([]string(nil), cfg.Tracker.ActiveStates...),
		ObservedStates:                append([]string(nil), cfg.Tracker.ObservedStates...),
		TerminalStates:                append([]string(nil), cfg.Tracker.TerminalStates...),
		Authorization:                 cfg.Tracker.Authorization,
		SelectorContext:               selector.Context{InstanceLogin: identity.GitHubLogin, Persona: identity.Name},
		WorkerHosts:                   append([]string(nil), cfg.Worker.SSHHosts...),
		BudgetRefusalCooldown:         durationFromSeconds(cfg.Budget.RefusalCooldownSeconds),
		WorkspaceCleanupIdleTTL:       durationFromMillis(cfg.Workspace.CleanupIdleTTLMS),
		WorkspaceCleanupSweepInterval: durationFromMillis(cfg.Workspace.CleanupSweepIntervalMS),
		SelectorPersona:               cfg.Tracker.Assignee,
		GitHubGraphQLWarnRemaining:    int64(cfg.Tracker.GitHubGraphQLWarnRemaining),
		GitHubGraphQLMinReserve:       int64(cfg.Tracker.GitHubGraphQLMinReserve),
		GitHubRESTMinReserve:          int64(cfg.Tracker.GitHubRESTMinReserve),
		ForgeHost:                     forgeavailability.HostFromEndpoint(cfg.Tracker.Endpoint),
		OutputTruncationMaxBytes:      cfg.Agent.OutputTruncation.MaxBytes,
		Lessons: LessonCaptureConfig{
			Path:       cfg.Agent.Lessons.Path,
			MaxEntries: cfg.Agent.Lessons.MaxEntries,
		},
		EfficiencyThresholds: efficiency.Thresholds{
			TokensMultiple:   cfg.Observability.Efficiency.AnomalyTokensMultiple,
			SessionsMultiple: cfg.Observability.Efficiency.AnomalySessionsMultiple,
			DwellMultiple:    cfg.Observability.Efficiency.AnomalyDwellMultiple,
		},
		Staleness: stalenessConfigFromWorkflow(cfg.Observability.Staleness, cfg.Tracker.TerminalStates),
		StalenessDelivery: staleness.DeliveryConfig{
			WebhookURL: cfg.Observability.Staleness.Webhook.URL,
			Headers:    cloneStringMap(cfg.Observability.Staleness.Webhook.Headers),
			Timeout:    durationFromMillis(cfg.Observability.Staleness.Webhook.TimeoutMS),
		},
		StrandedActiveThreshold: durationFromSeconds(cfg.Observability.StrandedActiveThresholdSeconds),
		DispatchStallThreshold:  durationFromSeconds(cfg.Observability.DispatchStallThresholdSeconds),
	}
}

func stalenessConfigFromWorkflow(cfg workflowconfig.StalenessObservability, terminalStates []string) staleness.Config {
	lanes := make([]staleness.LaneThreshold, 0, len(cfg.Lanes))
	for _, lane := range cfg.Lanes {
		lanes = append(lanes, staleness.LaneThreshold{
			State:     lane.State,
			Threshold: time.Duration(lane.ThresholdHours) * time.Hour,
			HumanGate: lane.HumanGate,
		})
	}
	return staleness.Config{
		Enabled:                       cfg.Enabled,
		Lanes:                         lanes,
		HumanGateRearm:                time.Duration(cfg.HumanGateRearmHours) * time.Hour,
		LaneReentryWindow:             time.Duration(cfg.LaneReentryWindowHours) * time.Hour,
		NoCompletionThreshold:         time.Duration(cfg.NoCompletionHours) * time.Hour,
		NoMergeThreshold:              time.Duration(cfg.NoMergeHours) * time.Hour,
		RepeatedDecisionCount:         cfg.RepeatedDecisionCount,
		RepeatedDecisionWindow:        time.Duration(cfg.RepeatedWindowHours) * time.Hour,
		RepeatedDecisionBenignReasons: append([]string(nil), cfg.RepeatedDecisionBenignReasons...),
		TerminalStates:                append([]string(nil), terminalStates...),
	}
}

func normalizeConfig(cfg Config) Config {
	cfg.ForgeHost = forgeavailability.NormalizeHost(cfg.ForgeHost)
	cfg.ServiceIdentity = strings.TrimSpace(cfg.ServiceIdentity)
	if cfg.ServiceIdentity == "" && strings.TrimSpace(cfg.Project.ID) != "" {
		cfg.ServiceIdentity = securityaudit.ServiceIdentity(cfg.Project.ID)
	}
	cfg.BillingMode = strings.ToLower(strings.TrimSpace(cfg.BillingMode))
	cfg.RateWindowPacing = cfg.RateWindowPacing.Normalized()
	if cfg.BillingMode == "" {
		cfg.BillingMode = workflowconfig.BillingModeSubscription
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.RefreshFailureThreshold <= 0 {
		cfg.RefreshFailureThreshold = workflowconfig.DefaultRefreshFailureThreshold
	}
	if cfg.MaxConcurrentAgents <= 0 {
		cfg.MaxConcurrentAgents = defaultMaxConcurrentAgents
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	if cfg.OverloadRetryDelay <= 0 {
		cfg.OverloadRetryDelay = defaultOverloadRetryDelay
	}
	if (cfg.LifetimeSessionLimit > 0 || cfg.LifetimeTokenLimit > 0) && cfg.LifetimeLimitCooldown <= 0 {
		cfg.LifetimeLimitCooldown = time.Duration(workflowconfig.DefaultLifetimeLimitCooldownSeconds) * time.Second
	}
	cfg.LifetimeLimitOverrideLabel = normalizeLabel(cfg.LifetimeLimitOverrideLabel)
	if cfg.StrandedActiveThreshold <= 0 {
		cfg.StrandedActiveThreshold = durationFromSeconds(workflowconfig.DefaultStrandedActiveThresholdSeconds)
	}
	if cfg.DispatchStallThreshold <= 0 {
		cfg.DispatchStallThreshold = durationFromSeconds(workflowconfig.DefaultDispatchStallThresholdSeconds)
	}
	if cfg.Staleness.HumanGateRearm <= 0 {
		cfg.Staleness.HumanGateRearm = time.Duration(workflowconfig.DefaultStalenessHumanGateRearmHours) * time.Hour
	}
	if cfg.Staleness.LaneReentryWindow <= 0 {
		cfg.Staleness.LaneReentryWindow = time.Duration(workflowconfig.DefaultStalenessLaneReentryWindowHours) * time.Hour
	}
	if cfg.MergeWorkerStartupTimeout <= 0 {
		cfg.MergeWorkerStartupTimeout = durationFromMillis(workflowconfig.DefaultMergeWorkerStartupTimeoutMS)
	}
	if cfg.MergeWorkerMaxDuration <= 0 {
		cfg.MergeWorkerMaxDuration = durationFromMillis(workflowconfig.DefaultMergeWorkerMaxDurationMS)
	}
	if cfg.MergeFairnessAge <= 0 {
		cfg.MergeFairnessAge = durationFromSeconds(workflowconfig.DefaultMergeFairnessAgeSeconds)
	}
	if cfg.ContinuationRetryDelay < 0 {
		cfg.ContinuationRetryDelay = 0
	}
	if cfg.ContinuationRetryDelay == 0 {
		cfg.ContinuationRetryDelay = defaultContinuationRetry
	}
	if cfg.FailureRetryBaseDelay <= 0 {
		cfg.FailureRetryBaseDelay = defaultFailureRetryBaseDelay
	}
	if cfg.FailureBreaker.SameClassLimit <= 0 {
		cfg.FailureBreaker.SameClassLimit = defaultFailureBreakerSameClassLimit
	}
	if cfg.FailureBreaker.Window <= 0 {
		cfg.FailureBreaker.Window = defaultFailureBreakerWindow
	}
	if cfg.FailureBreaker.Cooldown <= 0 {
		cfg.FailureBreaker.Cooldown = defaultFailureBreakerCooldown
	}
	if cfg.GitHubGraphQLWarnRemaining <= 0 {
		cfg.GitHubGraphQLWarnRemaining = defaultGitHubGraphQLWarnRemaining
	}
	if cfg.GitHubGraphQLMinReserve <= 0 {
		cfg.GitHubGraphQLMinReserve = defaultGitHubGraphQLMinReserve
	}
	if cfg.GitHubRESTMinReserve <= 0 {
		cfg.GitHubRESTMinReserve = defaultGitHubRESTMinReserve
	}
	if len(cfg.ActiveStates) == 0 {
		cfg.ActiveStates = []string{"Todo", "In Progress"}
	}
	if len(cfg.TerminalStates) == 0 {
		cfg.TerminalStates = []string{"Done", "Cancelled", "Canceled", "Closed"}
	}
	if strings.TrimSpace(cfg.StopRunTargetState) == "" {
		cfg.StopRunTargetState = blockedStatusState
	}
	if len(cfg.StopRunPriorityNames) == 0 {
		cfg.StopRunPriorityNames = map[int]string{1: "Urgent", 2: "High", 3: "Medium", 4: "Low"}
	} else {
		cfg.StopRunPriorityNames = cloneStopRunPriorityNames(cfg.StopRunPriorityNames)
	}
	if cfg.WorkspaceCleanupIdleTTL <= 0 {
		cfg.WorkspaceCleanupIdleTTL = defaultWorkspaceCleanupIdleTTL
	}
	if cfg.WorkspaceCleanupSweepInterval <= 0 {
		cfg.WorkspaceCleanupSweepInterval = defaultWorkspaceCleanupSweep
	}

	cfg.ActiveStates = normalizedStates(cfg.ActiveStates)
	cfg.ObservedStates = normalizedStates(cfg.ObservedStates)
	cfg.TerminalStates = normalizedStates(cfg.TerminalStates)
	if len(cfg.AutoPromote.TerminalStates) == 0 {
		cfg.AutoPromote.TerminalStates = append([]string(nil), cfg.TerminalStates...)
	}
	cfg.MaxConcurrentAgentsByState = cloneStateLimits(cfg.MaxConcurrentAgentsByState)
	cfg.DispatchPriorityByState = normalizedStates(cfg.DispatchPriorityByState)
	cfg.DispatchPriorityByLabel = normalizeLabels(cfg.DispatchPriorityByLabel)
	cfg.MergeMethod = workflowconfig.Deliverable{MergeMethod: cfg.MergeMethod}.EffectiveMergeMethod()
	cfg.DeliverableKind = strings.ToLower(strings.TrimSpace(cfg.DeliverableKind))
	if cfg.DeliverableKind == "" {
		cfg.DeliverableKind = workflowconfig.DeliverablePullRequest
	}
	cfg.Claiming = normalizeClaimingConfig(cfg.Claiming)
	cfg.AutoPromote = normalizeAutoPromoteConfig(cfg.AutoPromote)
	cfg.Plan = gate.EffectivePlan(cfg.Plan)
	cfg.DependencySource = normalizeDependencySource(cfg.DependencySource)
	cfg.StatusLabelPrefix = strings.ToLower(strings.TrimSpace(cfg.StatusLabelPrefix))
	cfg.DependencyAutoUnblock = normalizeDependencyAutoUnblockConfig(cfg.DependencyAutoUnblock)
	cfg.BlockedRecovery = normalizeBlockedRecoveryConfig(cfg.BlockedRecovery)
	cfg.BlockerAutoPromote = normalizeBlockerAutoPromoteConfig(cfg.BlockerAutoPromote, cfg.ActiveStates, cfg.DependencyAutoUnblock)
	cfg.Authorization.Normalize()
	cfg.SelectorContext.InstanceLogin = strings.TrimSpace(cfg.SelectorContext.InstanceLogin)
	cfg.SelectorContext.Persona = strings.TrimSpace(cfg.SelectorContext.Persona)
	cfg.WorkerHosts = normalizeWorkerHosts(cfg.WorkerHosts)
	cfg.SelectorPersona = strings.TrimSpace(cfg.SelectorPersona)
	cfg.Staleness.Lanes = append([]staleness.LaneThreshold(nil), cfg.Staleness.Lanes...)
	cfg.StalenessDelivery.WebhookURL = strings.TrimSpace(cfg.StalenessDelivery.WebhookURL)
	cfg.StalenessDelivery.Headers = cloneStringMap(cfg.StalenessDelivery.Headers)
	if cfg.MaxConcurrentAgentsPerHost < 0 {
		cfg.MaxConcurrentAgentsPerHost = 0
	}
	if cfg.OutputTruncationMaxBytes < 0 {
		cfg.OutputTruncationMaxBytes = 0
	}

	return cfg
}

func blockedCauseStatusLabelPrefix(cfg workflowconfig.Config) string {
	if cfg.Tracker.GitHubStatusSource != workflowconfig.GitHubStatusSourceLabel {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cfg.Tracker.StatusLabelPrefix))
}

func stopRunPriorityNames(value workflowconfig.StringOrMap) map[int]string {
	if !value.IsMap {
		return nil
	}
	result := make(map[int]string, 4)
	for name, value := range value.Map {
		rank, ok := value.(int)
		name = strings.TrimSpace(name)
		if !ok || rank < 1 || rank > 4 || name == "" {
			continue
		}
		if current := result[rank]; current == "" || name < current {
			result[rank] = name
		}
	}
	return result
}

func cloneStopRunPriorityNames(values map[int]string) map[int]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[int]string, len(values))
	for rank, name := range values {
		result[rank] = strings.TrimSpace(name)
	}
	return result
}

func (cfg Config) subscriptionBilling() bool {
	return !strings.EqualFold(strings.TrimSpace(cfg.BillingMode), workflowconfig.BillingModeMetered)
}

func normalizeClaimingConfig(cfg ClaimingConfig) ClaimingConfig {
	cfg.OwnershipMode = strings.ToLower(strings.TrimSpace(cfg.OwnershipMode))
	if cfg.OwnershipMode == "" {
		cfg.OwnershipMode = workflowconfig.IdentityOwnershipAssignee
	}
	cfg.Owner = strings.TrimSpace(cfg.Owner)
	cfg.AssigneeLogin = strings.TrimSpace(cfg.AssigneeLogin)
	cfg.OwnerField = strings.TrimSpace(cfg.OwnerField)
	cfg.LeaseField = strings.TrimSpace(cfg.LeaseField)
	if cfg.LeaseTTL < 0 {
		cfg.LeaseTTL = 0
	}
	if cfg.HeartbeatInterval < 0 {
		cfg.HeartbeatInterval = 0
	}
	return cfg
}

func durationFromMillis(ms int) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func durationFromSeconds(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func positiveIntValue(value *int) int {
	if value == nil || *value <= 0 {
		return 0
	}
	return *value
}

func cloneStateLimits(limits map[string]int) map[string]int {
	cloned := make(map[string]int, len(limits))
	for state, limit := range limits {
		if limit > 0 {
			cloned[normalizeState(state)] = limit
		}
	}
	return cloned
}

func cloneIssues(issues []connector.Issue) []connector.Issue {
	cloned := make([]connector.Issue, len(issues))
	for i, issue := range issues {
		cloned[i] = cloneIssue(issue)
	}
	return cloned
}

func normalizePullRequestState(state string) string {
	return strings.ToLower(strings.TrimSpace(state))
}

func nextAttempt(attempt int) int {
	if attempt < 1 {
		return 1
	}
	return attempt + 1
}

func stateIn(state string, states []string) bool {
	normalized := normalizeState(state)
	return slices.Contains(states, normalized)
}

func normalizedStates(states []string) []string {
	normalized := make([]string, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		state = normalizeState(state)
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		normalized = append(normalized, state)
	}
	return normalized
}

func normalizeState(state string) string {
	return strings.ToLower(strings.TrimSpace(state))
}
