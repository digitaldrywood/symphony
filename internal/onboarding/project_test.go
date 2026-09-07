package onboarding

import (
	"testing"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/runnerauth"
)

func TestProjectReadiness(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                                      string
		policy, doctor, provider, runner, binding bool
		artifacts                                 string
		want                                      bool
	}{
		{name: "interrupted"},
		{name: "missing policy", doctor: true, provider: true, runner: true, artifacts: "local"},
		{name: "missing provider", policy: true, doctor: true, runner: true, artifacts: "local"},
		{name: "missing runner", policy: true, doctor: true, provider: true, artifacts: "local"},
		{name: "missing gateway", policy: true, doctor: true, provider: true, runner: true, artifacts: "customer"},
		{name: "local ready", policy: true, doctor: true, provider: true, runner: true, artifacts: "local", want: true},
		{name: "customer configured", policy: true, doctor: true, provider: true, runner: true, binding: true, artifacts: "customer", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := Project{Progress: Progress{Repository: "existing", Doctor: tt.doctor, Provider: tt.provider, Artifacts: tt.artifacts}}
			if tt.policy {
				p.Policy = &policy.Approval{}
			}
			if tt.runner {
				p.Runners = []runnerauth.Eligibility{{}}
			}
			if tt.binding {
				p.Artifacts = []artifact.Binding{{Mode: "customer"}}
			}
			p.Evaluate()
			p.Evaluate()
			if p.Ready != tt.want || len(p.Steps) != 4 {
				t.Fatalf("readiness = %+v", p)
			}
		})
	}
}

func TestProgressValidation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		repository, artifacts string
		valid                 bool
	}{
		{"", "", true}, {"existing", "local", true}, {"generate", "customer", true}, {"workflow source", "local", false}, {"existing", "secret", false},
	} {
		t.Run(tt.repository+tt.artifacts, func(t *testing.T) {
			err := (Progress{Repository: tt.repository, Artifacts: tt.artifacts}).Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}
