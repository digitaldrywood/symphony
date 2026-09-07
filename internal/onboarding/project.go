package onboarding

import (
	"errors"
	"slices"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type Progress struct {
	Revision   tracker.Revision `json:"revision,string"`
	Repository string           `json:"repository"`
	Doctor     bool             `json:"doctor"`
	Provider   bool             `json:"provider"`
	Artifacts  string           `json:"artifacts"`
	UpdatedAt  string           `json:"updated_at,omitempty"`
}

func (p Progress) Validate() error {
	if !slices.Contains([]string{"", "existing", "generate"}, p.Repository) || !slices.Contains([]string{"", "local", "customer"}, p.Artifacts) {
		return errors.New("select an existing or new repository configuration and local or customer artifact storage")
	}
	return nil
}

type Step struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail"`
}

type Project struct {
	LatestRun string                   `json:"latest_run,omitempty"`
	Progress  Progress                 `json:"progress"`
	Policy    *policy.Approval         `json:"policy,omitempty"`
	Runners   []runnerauth.Eligibility `json:"runners"`
	Artifacts []artifact.Binding       `json:"artifact_services"`
	Steps     []Step                   `json:"steps"`
	Ready     bool                     `json:"ready"`
}

func (p *Project) Evaluate() {
	p.Steps = nil
	add := func(name string, ready bool, detail string) {
		state := "action_required"
		if ready {
			state = "ready"
		}
		p.Steps = append(p.Steps, Step{Name: name, State: state, Detail: detail})
	}
	add("Repository configuration", p.Policy != nil, "Inspect detent.yaml and WORKFLOW.md on the customer host, then explicitly approve the resolved policy descriptor.")
	add("Local validation", p.Progress.Doctor && p.Progress.Provider, "User-reported: run detent doctor and sign in to the selected provider on the execution host. Credentials stay local.")
	matching := slices.ContainsFunc(p.Runners, func(r runnerauth.Eligibility) bool { return len(r.Exclusions) == 0 })
	add("Execution runner", p.Policy != nil && matching, "Requires approved project access, matching tags and host selectors, a fresh heartbeat and available capacity.")
	artifactDetail := "Choose local history or configure the customer S3-compatible service and independent gateway. A binding does not verify storage or promise offline access."
	add("Artifact history", p.Progress.Artifacts == "local" || p.Progress.Artifacts == "customer" && len(p.Artifacts) > 0, artifactDetail)
	p.Ready = !slices.ContainsFunc(p.Steps, func(s Step) bool { return s.State != "ready" })
}
