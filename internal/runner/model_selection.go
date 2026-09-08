package runner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/agentoverride"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/selector"
)

type agentSelection struct {
	resolvedAgentOverride
	Selection agentidentity.Selection
	Err       error
}

func hasResumeIdentity(req RunRequest) bool {
	return req.RetryMode == RetryModeResume && !agentResumeStateEmpty(req.ResumeState) && !req.ResumeState.RuntimeIdentity.IsZero()
}

func (r agentRuntime) selectRequestBackend(req RunRequest, ctx selector.Context, role string) (RouteSelection, AgentBackend, config.AgentBackend, error) {
	if !hasResumeIdentity(req) {
		return r.selectBackendForRole(req.Issue, ctx, r.effectiveRunRole(role))
	}
	identity := req.ResumeState.RuntimeIdentity
	backend, ok := r.backends[identity.BackendID]
	backendConfig := r.backendConfigs[identity.BackendID]
	if !ok || backendConfig.Kind != identity.BackendKind {
		return RouteSelection{}, nil, config.AgentBackend{}, errors.New("resumed agent backend is no longer configured; restore the established backend or start a fresh attempt")
	}
	model := effectiveModel("", identity.RequestedModel.Value, identity.ResolvedModel.Value)
	return RouteSelection{BackendID: identity.BackendID, Model: model, RouteName: identity.Route}, backend, backendConfig, nil
}

func resolveRequestAgentSelection(ctx context.Context, req RunRequest, workspace, baseModel, role string, cfg config.Config, backendConfig config.AgentBackend, backend AgentBackend) agentSelection {
	if hasResumeIdentity(req) {
		identity := req.ResumeState.RuntimeIdentity
		model := effectiveModel("", identity.RequestedModel.Value, identity.ResolvedModel.Value)
		result := agentSelection{resolvedAgentOverride: resolvedAgentOverride{Model: model, Effort: identity.ReasoningEffort.Value}, Selection: identity.Selection}
		policy := cfg.EffectiveModelSelection()
		if !policy.Active() || policy.BackendKinds == nil || !slices.Contains(*policy.BackendKinds, backendConfig.Kind) {
			return result
		}
		override, _, err := agentoverride.FromIssueBody(req.Issue.Description)
		if err != nil {
			return result.reject("block", "", err.Error())
		}
		effort, field := override.EffortForRole(role)
		roleEffort := effort != ""
		if effort == "" {
			effort, field = override.Effort, "effort"
		}
		if effort != "" && selectionEffortRank(effort) < selectionEffortRank(result.Effort) {
			result.Effort = effort
			result.Selection.EffortSource = "issue." + field
		}
		explicitModel, _ := override.ModelForRole(role)
		level, _ := selectionComplexity(policy, req.Issue, role, result.Effort, roleEffort, explicitModel == "")
		result = boundSelectionEffort(result, policy, level, role)
		if result.Effort == identity.ReasoningEffort.Value {
			return result
		}
		provider, ok := backend.(AgentModelCatalogProvider)
		if !ok {
			return result.reject("effort", result.Effort, "changed resume effort requires a backend model catalog")
		}
		models, err := provider.ListModels(ctx)
		if err != nil {
			return result.reject("effort", result.Effort, "model catalog unavailable while validating changed resume effort")
		}
		selected, ok := findAgentModel(models, model)
		if !ok {
			return result.reject("model", model, "resumed model is unavailable while validating changed effort")
		}
		if effort, ok := supportedAgentEffort(selected, result.Effort); ok {
			result.Effort = effort
		} else {
			return result.reject("effort", result.Effort, "changed resume effort is unsupported by the resumed model")
		}
		return result
	}
	return resolveAgentSelection(ctx, req.Issue, workspace, baseModel, role, cfg, backendConfig, backend)
}

func resolveAgentSelection(ctx context.Context, issue connector.Issue, workspace, baseModel, role string, cfg config.Config, backendConfig config.AgentBackend, backend AgentBackend) agentSelection {
	policy := cfg.EffectiveModelSelection()
	projectEffort, field := cfg.Agent.Effort.Resolve(role)
	if !policy.Active() || policy.BackendKinds == nil || !slices.Contains(*policy.BackendKinds, backendConfig.Kind) {
		return agentSelection{resolvedAgentOverride: resolveAgentOverride(ctx, issue, workspace, baseModel, role, agentEffortCandidate{Field: field, Effort: projectEffort}, backend)}
	}
	result := agentSelection{Selection: agentidentity.Selection{Policy: "automatic", PolicySource: policy.Sources["enabled"]}}
	if policy.Preset != nil && *policy.Preset != "" {
		result.Selection.Policy = *policy.Preset
	}
	override, _, err := agentoverride.FromIssueBody(issue.Description)
	if err != nil {
		return result.reject("block", "", err.Error())
	}
	explicitModel, modelField := override.ModelForRole(role)
	explicitEffort, effortField := override.EffortForRole(role)
	roleEffort := explicitEffort != ""
	if explicitEffort == "" {
		explicitEffort, effortField = override.Effort, "effort"
	}
	level, reason := selectionComplexity(policy, issue, role, explicitEffort, roleEffort, explicitModel == "")
	defaults := policy.Levels[level]
	stage := policy.Stages[role]
	result.Model = strings.TrimSpace(baseModel)
	result.Selection.Reason = reason
	result.Selection.Level = level
	result.Selection.ModelSource = "route"
	if explicitModel != "" {
		result.Model = explicitModel
		result.Selection.ModelSource = "issue." + modelField
	}
	automaticModel := result.Model == ""
	if automaticModel {
		model := defaults.Model
		result.Selection.ModelSource = "levels." + level + ".model"
		if stage.Model != nil && *stage.Model != "" {
			model = stage.Model
			result.Selection.ModelSource = "stages." + role + ".model"
		}
		result.Model = policy.Model(selectionValue(model))
		result.Selection.ModelSource = selectionSource(policy, result.Selection.ModelSource, selectionValue(model))
	}
	result.Effort = explicitEffort
	result.Selection.EffortSource = "issue." + effortField
	if result.Effort == "" {
		result.Effort = projectEffort
		result.Selection.EffortSource = field
	}
	if result.Effort == "" {
		_, _, result.Effort = agentTurnIdentityOptions(backendConfig)
		result.Selection.EffortSource = "backend"
	}
	if result.Effort == "" {
		result.Effort = selectionValue(defaults.Effort)
		result.Selection.EffortSource = selectionSource(policy, "levels."+level+".effort", "")
		if stage.Effort != nil && *stage.Effort != "" {
			result.Effort = *stage.Effort
			result.Selection.EffortSource = selectionSource(policy, "stages."+role+".effort", "")
		}
	}
	result = boundSelectionEffort(result, policy, level, role)
	result.Selection.RequestedModel = result.Model
	provider, ok := backend.(AgentModelCatalogProvider)
	if !ok {
		result.Err = errors.New("automatic model selection requires a backend model catalog; configure an eligible backend or disable the policy")
		return result
	}
	models, err := provider.ListModels(ctx)
	if err != nil {
		result.Err = errors.New("automatic model selection: model catalog unavailable; retry catalog discovery before dispatch")
		return result
	}
	model, available := availableSelectionModel(models, result.Model)
	if !available && !automaticModel {
		return result.reject(modelField, result.Model, "explicit model is unavailable or retired in the selected backend catalog")
	}
	if !available && policy.Unavailable != nil && *policy.Unavailable == "fallback" && policy.FallbackOrder != nil {
		for _, candidate := range *policy.FallbackOrder {
			if fallback, ok := availableSelectionModel(models, policy.Model(candidate)); ok {
				model, available = fallback, true
				result.Selection.FallbackReason = "automatic model unavailable or retired"
				break
			}
		}
	}
	if !available {
		result.Err = errors.New("automatic model selection: no configured model is available; update agents.model_selection models or fallback_order after checking the backend catalog")
		return result
	}
	result.Model = canonicalAgentModel(model, result.Model)
	if effort, ok := supportedAgentEffort(model, result.Effort); ok {
		result.Effort = effort
	} else {
		if explicitEffort != "" {
			return result.reject(effortField, explicitEffort, "explicit effort is unsupported by the selected model")
		}
		result.Err = fmt.Errorf("automatic model selection: effort default %q is unsupported by model %q; configure a supported effort", result.Effort, result.Model)
	}
	return result
}

func (s agentSelection) reject(field, value, reason string) agentSelection {
	s.Rejections = append(s.Rejections, AgentOverrideRejection{Field: field, Value: value, Reason: reason})
	s.Err = fmt.Errorf("agent override rejected: %s: %s", field, reason)
	return s
}

func boundSelectionEffort(result agentSelection, policy config.ModelSelection, level, role string) agentSelection {
	ceiling := -1
	for _, defaults := range policy.Levels {
		ceiling = max(ceiling, selectionEffortRank(selectionValue(defaults.Effort)))
	}
	stage := policy.Stages[role]
	ceiling = max(ceiling, selectionEffortRank(selectionValue(stage.Effort)))
	if ceiling < 0 || selectionEffortRank(result.Effort) <= ceiling {
		return result
	}
	result.Effort = selectionValue(policy.Levels[level].Effort)
	field := "levels." + level + ".effort"
	if stage.Effort != nil && *stage.Effort != "" {
		result.Effort = *stage.Effort
		field = "stages." + role + ".effort"
	}
	result.Selection.EffortSource = selectionSource(policy, field, "") + ":ceiling"
	return result
}

func selectionEffortRank(effort string) int {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if effort == "ultracode" {
		effort = "max"
	}
	return slices.Index([]string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}, effort)
}

func selectionValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func availableSelectionModel(models []AgentModel, name string) (AgentModel, bool) {
	if strings.TrimSpace(name) == "" {
		return AgentModel{}, false
	}
	model, found := findAgentModel(models, name)
	return model, found && strings.TrimSpace(model.Upgrade) == ""
}

func selectionSource(policy config.ModelSelection, field, model string) string {
	if model == "normal" || model == "complex" {
		field = model + "_model"
	}
	return policy.Sources[field] + ":" + field
}

func selectionComplexity(policy config.ModelSelection, issue connector.Issue, role, effort string, roleEffort, modelOmitted bool) (string, string) {
	level := selectionValue(policy.DefaultLevel)
	stage := policy.Stages[role]
	if stage.Level != nil && *stage.Level != "" {
		return *stage.Level, "stage_complexity"
	}
	if policy.Rules != nil {
		for _, rule := range *policy.Rules {
			if rule.Disabled {
				continue
			}
			if len(rule.Roles) > 0 {
				if !slices.Contains(rule.Roles, role) {
					continue
				}
			} else if stage.IssueComplexity == nil || !*stage.IssueComplexity {
				if !roleEffort || len(rule.Efforts) == 0 {
					continue
				}
			}
			if len(rule.Efforts) > 0 && (!modelOmitted || !slices.Contains(rule.Efforts, strings.ToLower(effort))) {
				continue
			}
			if !selector.Match(issue, rule.Selector, selector.Context{}) {
				continue
			}
			return rule.Level, "rule:" + rule.Name
		}
	}
	return level, "default_complexity"
}
