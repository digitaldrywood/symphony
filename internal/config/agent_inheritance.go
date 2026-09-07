package config

import (
	"slices"
	"strings"
)

type AgentBudgetDefaults struct {
	PricingPath *string `yaml:"pricing_path,omitempty"`
}

func (c Config) WithAgentDefaults(instance Agents, budget AgentBudgetDefaults) Config {
	if len(instance.Backends) == 0 && len(instance.Routes) == 0 && !instance.ModelSelection.Configured() && budget.PricingPath == nil && c.Agents.local == nil {
		return c
	}
	local := c.Agents
	if local.local != nil {
		local = *local.local
	} else {
		local.pricingPath = c.Budget.PricingPath
	}
	c.Budget.PricingPath = local.pricingPath
	sources := map[string]string{}
	backends := []AgentBackend{}
	if local.Backends == nil || len(local.Backends) > 0 {
		for _, backend := range instance.Backends {
			backends = append(backends, backend)
			sources["backends."+backend.ID] = "instance"
		}
	}
	projectBackends := local.Backends
	if len(projectBackends) == 0 && (!slices.ContainsFunc(backends, func(value AgentBackend) bool { return value.ID == DefaultAgentBackendID }) || len(c.ConfiguredSubsettings("codex")) > 0) {
		projectBackends = []AgentBackend{CodexAgentBackend(c.Codex)}
	}
	for _, backend := range projectBackends {
		index := slices.IndexFunc(backends, func(value AgentBackend) bool { return value.ID == backend.ID })
		if index < 0 {
			backends = append(backends, backend)
		} else {
			backends[index] = backend
		}
		sources["backends."+backend.ID] = "project"
	}
	backends = slices.DeleteFunc(backends, func(value AgentBackend) bool { return value.Disabled })
	routes := slices.Clone(local.Routes)
	projectDefaults := map[string]bool{}
	for _, route := range local.Routes {
		sources["routes."+route.Name] = "project"
		if route.Default && !route.Disabled {
			projectDefaults[normalizeAgentRouteRole(route.Role)] = true
		}
	}
	if local.Routes == nil || len(local.Routes) > 0 {
		for _, route := range instance.Routes {
			if slices.ContainsFunc(local.Routes, func(value AgentRoute) bool { return value.Name == route.Name }) {
				continue
			}
			if route.Default && projectDefaults[normalizeAgentRouteRole(route.Role)] {
				continue
			}
			routes = append(routes, route)
			sources["routes."+route.Name] = "instance"
		}
	}
	routes = slices.DeleteFunc(routes, func(value AgentRoute) bool { return value.Disabled })
	if !slices.ContainsFunc(local.Routes, func(value AgentRoute) bool { return value.Name == "default" && value.Disabled }) && !slices.ContainsFunc(routes, func(value AgentRoute) bool { return value.Default && normalizeAgentRouteRole(value.Role) == "code" }) {
		backendID := DefaultAgentBackendID
		if len(projectBackends) > 0 {
			backendID = projectBackends[0].ID
		}
		routes = append(routes, AgentRoute{Name: "default", Backend: backendID, Default: true})
		sources["routes.default"] = "project"
	}
	c.Agents = Agents{
		Backends: backends, Routes: routes,
		ModelSelection: ResolveModelSelection(instance.ModelSelection, local.ModelSelection),
		Sources:        sources, local: &local,
	}
	c.Agents.normalize()
	_, pricingConfigured := c.configuredFields["budget.pricing_path"]
	pricingConfigured = pricingConfigured || (local.pricingPath != "" && local.pricingPath != Default().Budget.PricingPath)
	if !pricingConfigured && budget.PricingPath != nil {
		c.Budget.PricingPath = strings.TrimSpace(*budget.PricingPath)
		sources["budget.pricing_path"] = "instance"
	} else {
		sources["budget.pricing_path"] = "preset"
		if pricingConfigured {
			sources["budget.pricing_path"] = "project"
		}
	}
	if c.Budget.PricingPath == "" {
		c.Budget.PricingPath = Default().Budget.PricingPath
	}
	return c
}

func (c Config) EffectiveModelSelection() ModelSelection {
	if c.Agents.ModelSelection.Sources != nil {
		return c.Agents.ModelSelection
	}
	return ResolveModelSelection(ModelSelection{}, c.Agents.ModelSelection)
}
