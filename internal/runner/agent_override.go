package runner

import (
	"context"
	"fmt"
	"strings"

	"github.com/digitaldrywood/detent/internal/agentoverride"
	"github.com/digitaldrywood/detent/internal/connector"
)

type resolvedAgentOverride struct {
	Model      string
	Effort     string
	Rejections []AgentOverrideRejection
}

type agentEffortCandidate struct {
	Field           string
	Effort          string
	RequiresCatalog bool
}

func resolveAgentOverride(
	ctx context.Context,
	issue connector.Issue,
	workspace string,
	baseModel string,
	role string,
	projectEffort agentEffortCandidate,
	backend AgentBackend,
) resolvedAgentOverride {
	baseModel = strings.TrimSpace(baseModel)
	result := resolvedAgentOverride{Model: baseModel}
	override, found, err := agentoverride.FromIssueBody(issue.Description)
	if found && err != nil {
		result.Rejections = append(result.Rejections, AgentOverrideRejection{
			Field:  "block",
			Reason: err.Error(),
		})
		override = agentoverride.Override{}
	}
	override.Model, _ = override.ModelForRole(role)
	efforts := agentEffortCandidates(override, role, projectEffort)
	if override.Model == "" && len(efforts) == 0 {
		return result
	}

	provider, ok := backend.(AgentModelCatalogProvider)
	if !ok {
		return resolveWithoutAgentCatalog(result, override.Model, efforts, "selected backend does not advertise a model catalog")
	}
	models, err := provider.ListModels(ctx)
	if err != nil {
		return rejectUnavailableCatalog(result, override.Model, efforts, "model catalog unavailable: "+err.Error())
	}

	var effectiveModel AgentModel
	modelFound := false
	if override.Model != "" {
		requested, ok := findAgentModel(models, override.Model)
		if ok && strings.TrimSpace(requested.Upgrade) == "" {
			effectiveModel = requested
			modelFound = true
			result.Model = canonicalAgentModel(requested, override.Model)
		} else {
			reason := "model is not available from the selected backend"
			if ok {
				reason = fmt.Sprintf("model is retired; use %q", strings.TrimSpace(requested.Upgrade))
			}
			result.Rejections = append(result.Rejections, AgentOverrideRejection{
				Field:  "model",
				Value:  override.Model,
				Reason: reason,
			})
		}
	}

	if len(efforts) == 0 {
		return result
	}
	if !modelFound {
		effectiveBaseModel := baseModel
		if effectiveBaseModel == "" {
			defaultProvider, ok := backend.(AgentDefaultModelProvider)
			if !ok {
				rejectEffortCandidates(&result, efforts, "selected backend does not advertise its effective default model")
				return result
			}
			effectiveBaseModel, err = defaultProvider.DefaultModel(ctx, workspace)
			if err != nil {
				rejectEffortCandidates(&result, efforts, "effective default model unavailable: "+err.Error())
				return result
			}
		}
		effectiveModel, modelFound = findAgentModel(models, effectiveBaseModel)
	}
	if !modelFound {
		rejectEffortCandidates(&result, efforts, "effective model is not available in the selected backend catalog")
		return result
	}

	model := canonicalAgentModel(effectiveModel, result.Model)
	for _, candidate := range efforts {
		if effort, ok := supportedAgentEffort(effectiveModel, candidate.Effort); ok {
			result.Effort = effort
			return result
		}
		result.Rejections = append(result.Rejections, AgentOverrideRejection{
			Field:  candidate.Field,
			Value:  candidate.Effort,
			Reason: fmt.Sprintf("effort is not supported by model %q", model),
		})
	}
	return result
}

func agentEffortCandidates(override agentoverride.Override, role string, project agentEffortCandidate) []agentEffortCandidate {
	candidates := make([]agentEffortCandidate, 0, 3)
	if effort, field := override.EffortForRole(role); effort != "" {
		candidates = append(candidates, agentEffortCandidate{Field: field, Effort: effort, RequiresCatalog: true})
	}
	if override.Effort != "" {
		candidates = append(candidates, agentEffortCandidate{Field: "effort", Effort: override.Effort, RequiresCatalog: true})
	}
	project.Effort = strings.TrimSpace(project.Effort)
	if project.Effort != "" {
		candidates = append(candidates, project)
	}
	return candidates
}

func resolveWithoutAgentCatalog(result resolvedAgentOverride, model string, efforts []agentEffortCandidate, reason string) resolvedAgentOverride {
	if model != "" {
		result.Rejections = append(result.Rejections, AgentOverrideRejection{
			Field:  "model",
			Value:  model,
			Reason: reason,
		})
	}
	for _, effort := range efforts {
		if effort.RequiresCatalog {
			result.Rejections = append(result.Rejections, AgentOverrideRejection{
				Field:  effort.Field,
				Value:  effort.Effort,
				Reason: reason,
			})
			continue
		}
		result.Effort = strings.ToLower(strings.TrimSpace(effort.Effort))
		return result
	}
	return result
}

func rejectUnavailableCatalog(result resolvedAgentOverride, model string, efforts []agentEffortCandidate, reason string) resolvedAgentOverride {
	if model != "" {
		result.Rejections = append(result.Rejections, AgentOverrideRejection{
			Field:  "model",
			Value:  model,
			Reason: reason,
		})
	}
	rejectEffortCandidates(&result, efforts, reason)
	return result
}

func rejectEffortCandidates(result *resolvedAgentOverride, efforts []agentEffortCandidate, reason string) {
	for _, effort := range efforts {
		result.Rejections = append(result.Rejections, AgentOverrideRejection{
			Field:  effort.Field,
			Value:  effort.Effort,
			Reason: reason,
		})
	}
}

func findAgentModel(models []AgentModel, want string) (AgentModel, bool) {
	want = strings.TrimSpace(want)
	if want == "" {
		for _, model := range models {
			if model.Default {
				return model, true
			}
		}
		return AgentModel{}, false
	}
	for _, model := range models {
		if strings.TrimSpace(model.ID) == want || strings.TrimSpace(model.Model) == want {
			return model, true
		}
	}
	return AgentModel{}, false
}

func canonicalAgentModel(model AgentModel, fallback string) string {
	if value := strings.TrimSpace(model.Model); value != "" {
		return value
	}
	if value := strings.TrimSpace(model.ID); value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func supportedAgentEffort(model AgentModel, want string) (string, bool) {
	want = strings.TrimSpace(want)
	for _, effort := range model.SupportedReasoningEfforts {
		if strings.EqualFold(strings.TrimSpace(effort), want) {
			return strings.TrimSpace(effort), true
		}
	}
	return "", false
}
