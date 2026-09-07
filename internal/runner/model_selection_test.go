package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func selectionCatalog() []AgentModel {
	return []AgentModel{
		{ID: "gpt-5.6-sol", Model: "gpt-5.6-sol", SupportedReasoningEfforts: []string{"low", "medium", "high", "xhigh", "max"}},
		{ID: "gpt-6-astra", Model: "gpt-6-astra", Default: true, SupportedReasoningEfforts: []string{"low", "medium", "high", "xhigh", "max"}},
	}
}

func TestSelectionReloadAndResume(t *testing.T) {
	backend := &catalogAgentBackend{models: selectionCatalog()}
	cfg := config.Default()
	cfg.Agents.ModelSelection = config.ModelSelection{Preset: new("sol_first")}
	runner, err := NewRunner(Dependencies{Workflow: config.Workflow{Config: cfg}, Workspace: &fakeWorkspaceBackend{info: workspace.Info{Path: t.TempDir()}}, AgentBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	initial, oldRuntime, _, _ := runner.runtimeSnapshot()
	identity := agentidentity.Configured("codex", "codex", "default", RoleCode, "gpt-5.6-sol", "", "medium", "", time.Now())
	identity.Selection = agentidentity.Selection{Policy: "sol_first", Reason: "default_complexity"}
	resume := RunRequest{RetryMode: RetryModeResume, ResumeState: store.AgentResumeState{ProviderThreadID: "thread-1", RuntimeIdentity: identity}}
	cfg.Agents.ModelSelection.NormalModel = new("gpt-6-astra")
	if err := runner.UpdateWorkflowChecked(config.Workflow{Config: cfg}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		req  RunRequest
		want string
	}{
		{name: "new dispatch", want: "gpt-6-astra"},
		{name: "resumed dispatch", req: resume, want: "gpt-5.6-sol"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			capacity, err := runner.DispatchCapacity(context.Background(), tt.req)
			if err != nil || capacity.Model != tt.want {
				t.Fatalf("capacity=%+v error=%v", capacity, err)
			}
		})
	}
	if initial.Config.EffectiveModelSelection().Model("normal") != "gpt-5.6-sol" || oldRuntime.backends["codex"] != backend {
		t.Fatal("active snapshot changed")
	}
	backend.err = errors.New("catalog unavailable")
	got := resolveRequestAgentSelection(context.Background(), resume, "", "gpt-6-astra", RoleCode, cfg, config.AgentBackend{Kind: config.AgentBackendCodex}, backend)
	if got.Err != nil || got.Model != "gpt-5.6-sol" || got.Effort != "medium" {
		t.Fatalf("resume = %+v", got)
	}
	_, _, _, err = (agentRuntime{}).selectRequestBackend(resume, selector.Context{}, RoleCode)
	if err == nil {
		t.Fatal("missing resume backend accepted")
	}
	bad := cfg
	bad.Agents.ModelSelection.DefaultLevel = new("missing")
	if err := runner.UpdateWorkflowChecked(config.Workflow{Config: bad}); err == nil {
		t.Fatal("invalid reload accepted")
	}
	lastGood, _, _, _ := runner.runtimeSnapshot()
	if *lastGood.Config.EffectiveModelSelection().DefaultLevel != "normal" {
		t.Fatal("invalid reload replaced last known good config")
	}
}

func TestCustomSelectionSettings(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		mutate                    func(*config.ModelSelection)
		kind, role, model, effort string
	}{
		{name: "disabled", mutate: func(p *config.ModelSelection) { p.Enabled = new(false) }, model: "", effort: ""},
		{name: "cleared eligible backends", mutate: func(p *config.ModelSelection) { p.BackendKinds = &[]string{} }, model: "", effort: ""},
		{name: "custom backend excluded", kind: config.AgentBackendClaudeCode, model: "", effort: ""},
		{name: "cleared fallback", mutate: func(p *config.ModelSelection) { p.FallbackOrder = &[]string{} }, model: "gpt-5.6-sol", effort: "medium"},
		{name: "stage defaults", mutate: func(p *config.ModelSelection) {
			p.Stages = map[string]config.ModelSelectionStage{RoleMerge: {Model: new("complex"), Effort: new("high")}}
		}, role: RoleMerge, model: "gpt-6-astra", effort: "high"},
		{name: "role rule", mutate: func(p *config.ModelSelection) {
			p.Rules = &[]config.ModelSelectionRule{{Name: "merge-complex", Roles: []string{RoleMerge}, Level: "complex", Selector: selector.Selector{Fields: []selector.FieldEquals{{Name: "Complex", Value: "yes"}}}}}
		}, role: RoleMerge, model: "gpt-6-astra", effort: "medium"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Agents.ModelSelection.Preset = new("sol_first")
			if tt.mutate != nil {
				tt.mutate(&cfg.Agents.ModelSelection)
			}
			kind, role := tt.kind, tt.role
			if kind == "" {
				kind = config.AgentBackendCodex
			}
			if role == "" {
				role = RoleCode
			}
			backend := &catalogAgentBackend{models: selectionCatalog()}
			got := resolveAgentSelection(context.Background(), connector.Issue{Fields: map[string]string{"Complex": "yes"}}, "", "", role, cfg, config.AgentBackend{Kind: kind}, backend)
			if got.Err != nil || got.Model != tt.model || got.Effort != tt.effort {
				t.Fatalf("selection=%+v", got)
			}
		})
	}
}

func TestAutomaticModelSelection(t *testing.T) {
	for _, tt := range []struct {
		name, body, role, label, base, projectEffort, model, effort string
	}{
		{name: "missing metadata", model: "gpt-5.6-sol", effort: "medium"},
		{name: "generic enhancement", label: "enhancement", model: "gpt-5.6-sol", effort: "medium"},
		{name: "high effort", body: "effort: high", model: "gpt-5.6-sol", effort: "high"},
		{name: "complex metadata", label: "complexity:complex", model: "gpt-6-astra", effort: "medium"},
		{name: "very complex", label: "complexity:very-complex", model: "gpt-6-astra", effort: "high"},
		{name: "model only", body: "model: gpt-6-astra", model: "gpt-6-astra", effort: "medium"},
		{name: "explicit low", body: "effort: low", label: "complexity:very-complex", model: "gpt-6-astra", effort: "low"},
		{name: "xhigh signal", body: "effort: xhigh", model: "gpt-6-astra", effort: "xhigh"},
		{name: "max signal", body: "effort: max", model: "gpt-6-astra", effort: "max"},
		{name: "both explicit", body: "model: gpt-5.6-sol\neffort: max", label: "complexity:very-complex", model: "gpt-5.6-sol", effort: "max"},
		{name: "role fields", body: "model: gpt-6-astra\neffort: high\ncode:\n  model: gpt-5.6-sol\n  effort: low", model: "gpt-5.6-sol", effort: "low"},
		{name: "role model issue effort", body: "effort: xhigh\ncode:\n  model: gpt-5.6-sol", model: "gpt-5.6-sol", effort: "xhigh"},
		{name: "role effort issue model", body: "model: gpt-5.6-sol\ncode:\n  effort: xhigh", model: "gpt-5.6-sol", effort: "xhigh"},
		{name: "code inherited in rework", role: RoleRework, body: "code:\n  model: gpt-6-astra\n  effort: low", model: "gpt-6-astra", effort: "low"},
		{name: "entering rework", role: RoleRework, model: "gpt-5.6-sol", effort: "medium"},
		{name: "routine stays normal", role: RoleRoutine, label: "complexity:very-complex", body: "effort: xhigh", model: "gpt-5.6-sol", effort: "xhigh"},
		{name: "merge stays normal", role: RoleMerge, label: "complexity:very-complex", model: "gpt-5.6-sol", effort: "medium"},
		{name: "validator stays normal", role: RoleValidator, label: "complexity:complex", model: "gpt-5.6-sol", effort: "medium"},
		{name: "role specific merge signal", role: RoleMerge, body: "merge:\n  effort: xhigh", model: "gpt-6-astra", effort: "xhigh"},
		{name: "route pin", base: "gpt-5.6-sol", label: "complexity:very-complex", model: "gpt-5.6-sol", effort: "high"},
		{name: "issue wins route", base: "gpt-6-astra", body: "model: gpt-5.6-sol", model: "gpt-5.6-sol", effort: "medium"},
		{name: "project effort not complexity", projectEffort: "xhigh", model: "gpt-5.6-sol", effort: "xhigh"},
		{name: "issue effort wins project", projectEffort: "high", body: "effort: low", model: "gpt-5.6-sol", effort: "low"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Agents.ModelSelection = config.ModelSelection{Preset: new("sol_first")}
			cfg.Agent.Effort.Code = tt.projectEffort
			issue := connector.Issue{Priority: new(1), State: "Rework"}
			if tt.body != "" {
				issue.Description = "```detent-agent\nschema: 1\n" + tt.body + "\n```"
			}
			if tt.label != "" {
				issue.Labels = []string{tt.label}
			}
			backend := &catalogAgentBackend{models: selectionCatalog(), defaultModel: "gpt-6-astra"}
			role := tt.role
			if role == "" {
				role = RoleCode
			}
			got := resolveAgentSelection(context.Background(), issue, t.TempDir(), tt.base, role, cfg, config.AgentBackend{Kind: config.AgentBackendCodex}, backend)
			if got.Err != nil || got.Model != tt.model || got.Effort != tt.effort {
				t.Fatalf("selection = %+v, want %s %s", got, tt.model, tt.effort)
			}
			if backend.defaultCalls != 0 || backend.calls != 1 {
				t.Fatalf("catalog/default calls = %d/%d", backend.calls, backend.defaultCalls)
			}
			if got.Selection.Reason == "" || got.Selection.ModelSource == "" || got.Selection.EffortSource == "" {
				t.Fatalf("missing provenance: %+v", got.Selection)
			}
		})
	}
}

func TestAutomaticModelSelectionFailures(t *testing.T) {
	for _, tt := range []struct {
		name, body, unavailable     string
		catalog                     []AgentModel
		catalogErr                  error
		fallback, failure, rejected bool
	}{
		{name: "Astra unavailable", catalog: selectionCatalog()[:1], fallback: true},
		{name: "Astra retired", catalog: []AgentModel{selectionCatalog()[0], {ID: "gpt-6-astra", Upgrade: "replacement"}}, fallback: true},
		{name: "neither available", failure: true},
		{name: "catalog unavailable", catalogErr: errors.New("secret catalog transport details"), failure: true},
		{name: "fail configured", catalog: selectionCatalog()[:1], unavailable: "fail", failure: true},
		{name: "invalid explicit model", catalog: selectionCatalog(), body: "model: absent", failure: true, rejected: true},
		{name: "invalid explicit effort", catalog: selectionCatalog(), body: "effort: absent", failure: true, rejected: true},
		{name: "invalid role does not downgrade", catalog: selectionCatalog(), body: "effort: low\ncode:\n  effort: absent", failure: true, rejected: true},
		{name: "malformed override", catalog: selectionCatalog(), body: "unknown: value", failure: true, rejected: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Agents.ModelSelection = config.ModelSelection{Preset: new("sol_first")}
			if tt.unavailable != "" {
				cfg.Agents.ModelSelection.Unavailable = &tt.unavailable
			}
			issue := connector.Issue{Labels: []string{"complexity:complex"}}
			if tt.body != "" {
				issue.Description = "```detent-agent\nschema: 1\n" + tt.body + "\n```"
			}
			backend := &catalogAgentBackend{models: tt.catalog, err: tt.catalogErr}
			got := resolveAgentSelection(context.Background(), issue, "", "", RoleCode, cfg, config.AgentBackend{Kind: config.AgentBackendCodex}, backend)
			if (got.Err != nil) != tt.failure || (len(got.Rejections) > 0) != tt.rejected {
				t.Fatalf("selection = %+v", got)
			}
			if (got.Selection.FallbackReason != "") != tt.fallback {
				t.Fatalf("fallback = %+v", got.Selection)
			}
			if tt.fallback && (got.Model != "gpt-5.6-sol" || got.Selection.RequestedModel != "gpt-6-astra") {
				t.Fatalf("fallback identity = %+v", got)
			}
			if got.Err != nil && strings.Contains(got.Err.Error(), "secret") {
				t.Fatal("catalog details leaked")
			}
		})
	}
}
