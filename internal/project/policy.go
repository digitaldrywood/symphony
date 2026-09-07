package project

import (
	"context"
	"fmt"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/policy"
)

type policyChecker interface {
	CheckProjectPolicy(context.Context, string, string, policy.Descriptor) error
}

func ResolvePolicy(cfg globalconfig.Project, workflow workflowconfig.Workflow) (policy.Descriptor, error) {
	workflow.Config = workflow.Config.WithAgentDefaults(cfg.GlobalAgents, cfg.GlobalBudget)
	workflow.Config = workflowConfigWithProjectIdentity(cfg, workflow.Config)
	return workflowconfig.ResolvePolicy(workflow)
}

func configureProjectPolicy(ctx context.Context, cfg globalconfig.Project, workflow *workflowconfig.Workflow, scheduling orchestrator.SchedulingSource) error {
	checker, ok := scheduling.(policyChecker)
	if !ok {
		return nil
	}
	descriptor, err := ResolvePolicy(cfg, *workflow)
	if err != nil {
		return fmt.Errorf("resolve repository policy: %w", err)
	}
	if err := checker.CheckProjectPolicy(ctx, cfg.ID, workflow.Config.Tracker.Repository, descriptor); err != nil {
		return err
	}
	workflow.Config.Policy = descriptor
	return nil
}
