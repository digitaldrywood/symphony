package runner

import (
	"context"

	"github.com/digitaldrywood/detent/internal/providercapacity"
)

type ProviderCapacityResolver interface {
	DispatchCapacity(context.Context, RunRequest) (providercapacity.Requirement, error)
}

func (r *Runner) DispatchCapacity(ctx context.Context, req RunRequest) (providercapacity.Requirement, error) {
	workflow, runtime, _, _ := r.runtimeSnapshot()
	role := runRole(req.Mode, req.Issue)
	selection, backend, backendConfig, err := runtime.selectRequestBackend(req, selectorContext(req.SelectorContext, workflow), role)
	if err != nil {
		return providercapacity.Requirement{}, err
	}
	baseModel := effectiveModel("", selection.Model, runtime.defaultModelForRole(role))
	override := resolveRequestAgentSelection(ctx, req, "", baseModel, role, workflow.Config, backendConfig, backend)
	if override.Err != nil {
		return providercapacity.Requirement{}, override.Err
	}
	model := effectiveModel("", override.Model, runtime.defaultModelForRole(role))
	if model == "" {
		model = "provider_default"
	}
	result := providercapacity.Requirement{Role: role, Backend: selection.BackendID, Model: model}
	return result, result.Validate()
}
