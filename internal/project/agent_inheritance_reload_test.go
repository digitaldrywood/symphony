package project_test

import (
	"context"
	"testing"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/project"
)

func TestManagerReloadsInheritedAgentSelection(t *testing.T) {
	ctx := context.Background()
	global := globalconfig.Config{
		Global:   globalconfig.Settings{Agents: workflowconfig.Agents{ModelSelection: workflowconfig.ModelSelection{Preset: new("sol_first")}}},
		Projects: []globalconfig.Project{{ID: "alpha", Weight: 1}, {ID: "beta", Weight: 1}},
	}
	created := 0
	manager, err := project.NewManager(project.ManagerConfigFromGlobal(global), project.ManagerDependencies{
		ProjectFactory: func(cfg globalconfig.Project) (*project.Project, error) {
			created++
			workflow := workflowConfig("memory")
			if cfg.ID == "beta" {
				workflow.Agents.ModelSelection.ComplexModel = new("beta-complex")
			}
			return project.New(project.Config{Project: cfg, Workflow: workflowconfig.Workflow{Config: workflow}}, project.Dependencies{Runner: blockingRunner{}})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, item := range manager.Registry().List() {
			if err := item.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	before, _ := manager.Registry().Get("alpha")
	for _, tt := range []struct {
		name    string
		invalid bool
	}{{name: "valid reload"}, {name: "invalid reference", invalid: true}, {name: "future registration"}} {
		t.Run(tt.name, func(t *testing.T) {
			next := global
			next.Global.Agents.ModelSelection.NormalModel = new("host-normal")
			if tt.invalid {
				next.Global.Agents.Routes = []workflowconfig.AgentRoute{{Name: "broken", Backend: "missing"}}
			}
			if tt.name == "future registration" {
				next.Projects = append(append([]globalconfig.Project(nil), global.Projects...), globalconfig.Project{ID: "future", Weight: 1})
			}
			_, err := manager.Reconcile(ctx, project.ManagerConfigFromGlobal(next))
			if tt.invalid {
				if err == nil {
					t.Fatal("invalid reload accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			after, _ := manager.Registry().Get("alpha")
			if before != after {
				t.Fatal("host policy reload restarted project")
			}
			if model := after.Workflow().Config.EffectiveModelSelection().Model("normal"); model != "host-normal" {
				t.Fatalf("normal = %s", model)
			}
			beta, _ := manager.Registry().Get("beta")
			if model := beta.Workflow().Config.EffectiveModelSelection().Model("complex"); model != "beta-complex" {
				t.Fatalf("complex = %s", model)
			}
			if tt.name == "future registration" {
				future, ok := manager.Registry().Get("future")
				if !ok || future.Workflow().Config.EffectiveModelSelection().Model("normal") != "host-normal" {
					t.Fatal("future project did not inherit")
				}
			}
		})
	}
	if created != 3 {
		t.Fatalf("created = %d, want 3", created)
	}
}
