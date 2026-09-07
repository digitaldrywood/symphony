package config

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/selector"
)

type ModelSelection struct {
	Enabled       *bool                             `yaml:"enabled,omitempty"`
	Preset        *string                           `yaml:"preset,omitempty"`
	NormalModel   *string                           `yaml:"normal_model,omitempty"`
	ComplexModel  *string                           `yaml:"complex_model,omitempty"`
	BackendKinds  *[]string                         `yaml:"backend_kinds,omitempty"`
	DefaultLevel  *string                           `yaml:"default_level,omitempty"`
	Levels        map[string]ModelSelectionDefaults `yaml:"levels,omitempty"`
	Stages        map[string]ModelSelectionStage    `yaml:"stages,omitempty"`
	Rules         *[]ModelSelectionRule             `yaml:"rules,omitempty"`
	Unavailable   *string                           `yaml:"unavailable,omitempty"`
	FallbackOrder *[]string                         `yaml:"fallback_order,omitempty"`
	Sources       map[string]string                 `yaml:"-"`
}

type ModelSelectionDefaults struct {
	Model  *string `yaml:"model,omitempty"`
	Effort *string `yaml:"effort,omitempty"`
}

type ModelSelectionStage struct {
	Model           *string `yaml:"model,omitempty"`
	Effort          *string `yaml:"effort,omitempty"`
	Level           *string `yaml:"level,omitempty"`
	IssueComplexity *bool   `yaml:"issue_complexity,omitempty"`
}

type ModelSelectionRule struct {
	Name     string            `yaml:"name"`
	Disabled bool              `yaml:"disabled,omitempty"`
	Level    string            `yaml:"level"`
	Roles    []string          `yaml:"roles,omitempty"`
	Selector selector.Selector `yaml:"selector,omitempty"`
	Efforts  []string          `yaml:"efforts,omitempty"`
}

func (p ModelSelection) MarshalYAML() (any, error) {
	type plain ModelSelection
	var node yaml.Node
	if err := node.Encode(plain(p)); err != nil {
		return nil, err
	}
	for _, field := range []struct {
		name  string
		empty bool
	}{{"levels", p.Levels != nil && len(p.Levels) == 0}, {"stages", p.Stages != nil && len(p.Stages) == 0}} {
		if field.empty {
			node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: field.name}, &yaml.Node{Kind: yaml.MappingNode})
		}
	}
	return &node, nil
}

func SolFirstModelSelection() ModelSelection {
	return ModelSelection{
		Enabled: new(true), Preset: new("sol_first"),
		NormalModel: new("gpt-5.6-sol"), ComplexModel: new("gpt-6-astra"),
		BackendKinds: &[]string{AgentBackendCodex}, DefaultLevel: new("normal"),
		Levels: map[string]ModelSelectionDefaults{
			"normal":       {Model: new("normal"), Effort: new("medium")},
			"complex":      {Model: new("complex"), Effort: new("medium")},
			"very_complex": {Model: new("complex"), Effort: new("high")},
		},
		Stages: map[string]ModelSelectionStage{
			"code":           {IssueComplexity: new(true)},
			"plan":           {IssueComplexity: new(true)},
			"rework":         {IssueComplexity: new(true)},
			"merge":          {IssueComplexity: new(false)},
			"routine":        {IssueComplexity: new(false)},
			"validator":      {IssueComplexity: new(false)},
			"security_audit": {IssueComplexity: new(false)},
		},
		Rules: &[]ModelSelectionRule{
			{Name: "very_complex", Level: "very_complex", Selector: selector.Selector{Labels: selector.Labels{Include: []string{"complexity:very-complex"}}}},
			{Name: "explicit_effort", Level: "complex", Efforts: []string{"xhigh", "max"}},
			{Name: "complex", Level: "complex", Selector: selector.Selector{Labels: selector.Labels{Include: []string{"complexity:complex"}}}},
		},
		Unavailable: new("fallback"), FallbackOrder: &[]string{"normal"},
	}
}

func (p ModelSelection) Active() bool {
	return p.Enabled != nil && *p.Enabled
}

func (p ModelSelection) Configured() bool {
	return p.Enabled != nil || p.Preset != nil || p.NormalModel != nil || p.ComplexModel != nil || p.BackendKinds != nil || p.DefaultLevel != nil || p.Levels != nil || p.Stages != nil || p.Rules != nil || p.Unavailable != nil || p.FallbackOrder != nil
}

func (p ModelSelection) Model(value string) string {
	switch value {
	case "normal":
		return selectionString(p.NormalModel)
	case "complex":
		return selectionString(p.ComplexModel)
	default:
		return strings.TrimSpace(value)
	}
}

func selectionString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func ResolveModelSelection(instance, project ModelSelection) ModelSelection {
	preset := instance.Preset
	if project.Preset != nil {
		preset = project.Preset
	}
	result := ModelSelection{Sources: map[string]string{}}
	if selectionString(preset) == "sol_first" {
		result.overlay(SolFirstModelSelection(), "preset")
	}
	result.overlay(instance, "instance")
	result.overlay(project, "project")
	return result
}

func selectionOverride[T any](target **T, value *T, sources map[string]string, key, source string) {
	if value != nil {
		*target = value
		sources[key] = source
	}
}

func (p *ModelSelection) overlay(v ModelSelection, source string) {
	selectionOverride(&p.Enabled, v.Enabled, p.Sources, "enabled", source)
	selectionOverride(&p.Preset, v.Preset, p.Sources, "preset", source)
	selectionOverride(&p.NormalModel, v.NormalModel, p.Sources, "normal_model", source)
	selectionOverride(&p.ComplexModel, v.ComplexModel, p.Sources, "complex_model", source)
	selectionOverride(&p.BackendKinds, v.BackendKinds, p.Sources, "backend_kinds", source)
	selectionOverride(&p.DefaultLevel, v.DefaultLevel, p.Sources, "default_level", source)
	selectionOverride(&p.Unavailable, v.Unavailable, p.Sources, "unavailable", source)
	selectionOverride(&p.FallbackOrder, v.FallbackOrder, p.Sources, "fallback_order", source)
	if v.Levels != nil {
		p.Levels = maps.Clone(p.Levels)
		if len(v.Levels) == 0 || p.Levels == nil {
			p.Levels = map[string]ModelSelectionDefaults{}
			maps.DeleteFunc(p.Sources, func(key, _ string) bool { return strings.HasPrefix(key, "levels.") })
		}
		p.Sources["levels"] = source
		for name, value := range v.Levels {
			level := p.Levels[name]
			selectionOverride(&level.Model, value.Model, p.Sources, "levels."+name+".model", source)
			selectionOverride(&level.Effort, value.Effort, p.Sources, "levels."+name+".effort", source)
			p.Levels[name] = level
		}
	}
	if v.Stages != nil {
		p.Stages = maps.Clone(p.Stages)
		if len(v.Stages) == 0 || p.Stages == nil {
			p.Stages = map[string]ModelSelectionStage{}
			maps.DeleteFunc(p.Sources, func(key, _ string) bool { return strings.HasPrefix(key, "stages.") })
		}
		p.Sources["stages"] = source
		for name, value := range v.Stages {
			stage := p.Stages[name]
			selectionOverride(&stage.Model, value.Model, p.Sources, "stages."+name+".model", source)
			selectionOverride(&stage.Effort, value.Effort, p.Sources, "stages."+name+".effort", source)
			selectionOverride(&stage.Level, value.Level, p.Sources, "stages."+name+".level", source)
			selectionOverride(&stage.IssueComplexity, value.IssueComplexity, p.Sources, "stages."+name+".issue_complexity", source)
			p.Stages[name] = stage
		}
	}
	if v.Rules != nil {
		if len(*v.Rules) == 0 {
			maps.DeleteFunc(p.Sources, func(key, _ string) bool { return strings.HasPrefix(key, "rules.") })
		}
		rules := []ModelSelectionRule{}
		if p.Rules != nil && len(*v.Rules) > 0 {
			rules = slices.Clone(*p.Rules)
		}
		for _, rule := range *v.Rules {
			index := slices.IndexFunc(rules, func(existing ModelSelectionRule) bool { return existing.Name == rule.Name })
			if index < 0 {
				rules = append(rules, rule)
			} else {
				rules[index] = rule
			}
			p.Sources["rules."+rule.Name] = source
		}
		p.Rules = &rules
		p.Sources["rules"] = source
	}
}

func (p ModelSelection) Validate() []string {
	var problems []string
	add := func(field, reason string) { problems = append(problems, "agents.model_selection."+field+" "+reason) }
	if preset := selectionString(p.Preset); preset != "" && preset != "sol_first" {
		add("preset", "must be sol_first or empty")
	}
	if !p.Active() {
		return problems
	}
	for _, field := range []struct {
		name  string
		value *string
	}{
		{"normal_model", p.NormalModel}, {"complex_model", p.ComplexModel}, {"default_level", p.DefaultLevel},
	} {
		if selectionString(field.value) == "" {
			add(field.name, "is required when enabled")
		}
	}
	if p.Unavailable == nil || (*p.Unavailable != "fallback" && *p.Unavailable != "fail") {
		add("unavailable", "must be fallback or fail")
	}
	if _, ok := p.Levels[selectionString(p.DefaultLevel)]; !ok {
		add("default_level", "must reference a configured level")
	}
	for name, level := range p.Levels {
		if p.Model(selectionString(level.Model)) == "" {
			add("levels."+name+".model", "is required")
		}
		if selectionString(level.Effort) == "" {
			add("levels."+name+".effort", "is required")
		}
		if selectionString(level.Effort) == "max" {
			add("levels."+name+".effort", "must not automatically assign max")
		}
	}
	for name, stage := range p.Stages {
		if stage.Level != nil && *stage.Level != "" {
			if _, ok := p.Levels[*stage.Level]; !ok {
				add("stages."+name+".level", "must reference a configured level")
			}
		}
		if selectionString(stage.Effort) == "max" {
			add("stages."+name+".effort", "must not automatically assign max")
		}
	}
	if p.Rules != nil {
		seen := map[string]bool{}
		for _, rule := range *p.Rules {
			if len(rule.Name) > 80 || !validAgentIdentityLabel(rule.Name) {
				add("rules", "names must be sanitized labels of at most 80 characters")
			}
			if rule.Name == "" || seen[rule.Name] {
				add("rules", "must have unique nonempty names")
			}
			seen[rule.Name] = true
			if rule.Disabled {
				continue
			}
			if _, ok := p.Levels[rule.Level]; !ok {
				add("rules."+rule.Name+".level", "must reference a configured level")
			}
			problems = append(problems, rule.Selector.Validate("agents.model_selection.rules."+rule.Name+".selector")...)
			if !rule.Selector.Configured() && len(rule.Efforts) == 0 {
				add("rules."+rule.Name, "requires a selector or explicit efforts")
			}
		}
	}
	return problems
}

func (a Agents) ValidateDefaults() error {
	a.Backends = slices.Clone(a.Backends)
	a.Routes = slices.Clone(a.Routes)
	a.normalize()
	var problems []string
	for _, route := range a.Routes {
		if route.Name == "" {
			problems = append(problems, "inherited agent routes require a name")
		}
	}
	a.validate(&problems)
	if len(problems) > 0 {
		return fmt.Errorf("invalid agent defaults: %s", strings.Join(problems, "; "))
	}
	return nil
}
