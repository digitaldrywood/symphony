package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/securityaudit"
	"github.com/digitaldrywood/detent/internal/store"
)

func (r *Runner) Audit(ctx context.Context, req SecurityAuditRequest) (execution SecurityAuditExecution, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	workflow, agentRuntime, _, _ := r.runtimeSnapshot()
	auditConfig := gateSecurityAuditConfig(workflow.Config)
	prompt, err := securityaudit.BuildPrompt(req.Snapshot, securityAuditMaxDiffBytes(auditConfig))
	if err != nil {
		return execution, err
	}
	selection, backend, backendConfig, err := agentRuntime.selectBackendForRole(req.Issue, selectorContext(req.SelectorContext, workflow), RoleSecurityAudit)
	if err != nil {
		return execution, err
	}
	if backendConfig.Kind != config.AgentBackendCodex {
		return execution, errors.New("security audit requires a Codex app-server backend")
	}
	options := backendConfig.CodexOptions()
	if strings.TrimSpace(options.ModelProvider) != "" {
		return execution, errors.New("security audit refuses a configured metered model provider")
	}

	auditWorkspace, err := r.prepareSecurityAuditWorkspace()
	if err != nil {
		return execution, err
	}
	removeAuditWorkspace := true
	defer func() {
		if !removeAuditWorkspace {
			return
		}
		if cleanupErr := os.RemoveAll(auditWorkspace); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove security audit workspace: %w", cleanupErr))
		}
	}()

	startedAt := req.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = r.now().UTC()
	}
	execution.InvocationID = uuid.NewString()
	execution.StartedAt = startedAt
	selectedModel := selection.Model
	if configuredModel := strings.TrimSpace(auditConfig.Model); configuredModel != "" {
		selectedModel = configuredModel
	}
	baseModel := effectiveModel("", selectedModel, agentRuntime.defaultModelForRole(RoleSecurityAudit))
	selectionIssue := req.Issue
	selectionIssue.Description = ""
	resolvedSelection := resolveAgentSelection(ctx, selectionIssue, auditWorkspace, baseModel, RoleSecurityAudit, workflow.Config, backendConfig, backend)
	if resolvedSelection.Err != nil {
		return execution, resolvedSelection.Err
	}
	selectedModel = resolvedSelection.Model
	sessionModel := effectiveModel("", selectedModel, agentRuntime.defaultModelForRole(RoleSecurityAudit))
	runtimeIdentity := configuredRuntimeIdentity(selection, backendConfig, RoleSecurityAudit, sessionModel, startedAt)
	runtimeIdentity.Selection = resolvedSelection.Selection
	if resolvedSelection.Effort != "" {
		runtimeIdentity.ReasoningEffort = agentidentity.NewValue(resolvedSelection.Effort, agentidentity.ProvenanceConfigured)
	}
	runReq := RunRequest{Issue: req.Issue, StartedAt: startedAt, SelectorContext: req.SelectorContext}
	sessionID, sessionStarted, err := r.startSession(ctx, runReq, startedAt, runtimeIdentity, store.AgentResumeState{}, "", "")
	if err != nil {
		return execution, err
	}

	runResult := RunResult{FinalState: FinalStateCompleted, RuntimeIdentity: runtimeIdentity}
	var output strings.Builder
	modelProvider, serviceTier, effort := agentTurnIdentityOptions(backendConfig)
	if resolvedSelection.Effort != "" {
		effort = resolvedSelection.Effort
	}
	removeAuditWorkspace = false
	turnResult, cleanupScratch, turnErr := runAgentBackendTurnWithToolsUsingLimitPreservingScratch(ctx, backend, AgentTurnRequest{
		Workspace:               auditWorkspace,
		Prompt:                  prompt,
		ToolInstructions:        securityaudit.ToolInstructions(),
		ReadOnly:                true,
		RequireSubscriptionAuth: true,
		Model:                   selectedModel,
		ModelProvider:           modelProvider,
		ServiceTier:             serviceTier,
		ReasoningEffort:         effort,
		TurnTimeout:             durationFromMillis(auditConfig.TurnTimeoutMS),
		MaxDuration:             durationFromMillis(auditConfig.TurnTimeoutMS),
		Environment: procgroup.Environment{Variables: map[string]string{
			"OPENAI_API_KEY":       "",
			"AZURE_OPENAI_API_KEY": "",
			"GH_TOKEN":             "",
			"GITHUB_TOKEN":         "",
		}},
		MaxRSSBytes:     r.maxAgentRSSBytes,
		RSSPollInterval: r.rssPollInterval,
		projectID:       r.projectID,
		processRSS:      r.processRSS,
	}, nil, nil, func(updateCtx context.Context, update AgentUpdate) error {
		if update.Type == AgentUpdateToolStarted || update.Type == AgentUpdateToolOutput || update.Type == AgentUpdateToolCompleted {
			return ErrSecurityAuditToolUse
		}
		if update.WorkerProcess.PID > 0 {
			execution.WorkerProcess = update.WorkerProcess
		}
		if err := r.persistSessionWorkerProcess(updateCtx, sessionID, update, r.securityAuditRoot, auditWorkspace); err != nil {
			return err
		}
		if err := r.persistSessionProviderIdentity(updateCtx, sessionID, update); err != nil {
			return err
		}
		if update.Type == AgentUpdateMessageDelta {
			output.WriteString(update.Delta)
		}
		previousIdentity := runResult.RuntimeIdentity
		applyAgentUpdate(&runResult, update)
		if !previousIdentity.MateriallyEqual(runResult.RuntimeIdentity) {
			if err := r.persistSessionIdentity(updateCtx, sessionID, runResult.RuntimeIdentity); err != nil {
				return err
			}
		}
		return nil
	}, r.turnLimit)
	reapErr := r.reapSessionWorkerProcess(ctx, sessionID, req.Issue, workerProcessReapReason(ctx, turnErr))
	removeAuditWorkspace = reapErr == nil
	turnErr = errors.Join(turnErr, cleanupWorkerScratchAfterProcessReap(cleanupScratch, reapErr))
	if reapErr != nil {
		turnErr = errors.Join(turnErr, fmt.Errorf("%w: %w", ErrWorkerProcessReap, reapErr))
	}

	execution.AuthenticationMode = turnResult.AuthenticationMode
	if errors.Is(turnErr, ErrSubscriptionAuthRequired) {
		execution.AuthenticationMode = securityaudit.AuthenticationRejected
	}
	execution.ProviderThreadID = turnResult.ThreadID
	execution.ProviderSessionID = turnResult.SessionID
	execution.Output = output.String()
	execution.CompletedAt = r.now().UTC()
	runResult.Tokens.RuntimeSeconds = runtimeSeconds(startedAt, execution.CompletedAt)
	if turnErr != nil {
		runResult.FinalState = finalStateForTurnError(turnErr)
		finishErr := r.finishSession(context.WithoutCancel(ctx), sessionID, sessionStarted, 0, req.Issue, startedAt, execution.CompletedAt, runResult, sessionModel, backendConfig.Kind, 1, turnResult, 0)
		return execution, errors.Join(fmt.Errorf("run security audit turn: %w", turnErr), finishErr)
	}

	execution.Result, err = securityaudit.ParseOutput(execution.Output)
	if err != nil {
		runResult.FinalState = FinalStateFailed
	}
	finishErr := r.finishSession(context.WithoutCancel(ctx), sessionID, sessionStarted, 0, req.Issue, startedAt, execution.CompletedAt, runResult, sessionModel, backendConfig.Kind, 1, turnResult, 0)
	if err != nil {
		return execution, errors.Join(err, finishErr)
	}
	return execution, finishErr
}

func (r *Runner) prepareSecurityAuditWorkspace() (string, error) {
	root := filepath.Clean(r.securityAuditRoot)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("create security audit root: %w", err)
	}
	path, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return "", fmt.Errorf("create security audit workspace: %w", err)
	}
	return path, nil
}

func securityAuditMaxDiffBytes(cfg gate.SecurityAuditConfig) int {
	if cfg.MaxDiffBytes == nil {
		return securityaudit.DefaultMaxDiffBytes
	}
	return *cfg.MaxDiffBytes
}

func gateSecurityAuditConfig(cfg config.Config) gate.SecurityAuditConfig {
	return gate.Effective(cfg.Gate).SecurityAudit
}
