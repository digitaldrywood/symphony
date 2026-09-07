package cli

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
)

func checkDoctorModelSelection(id string, cfg workflowconfig.Config) doctorCheck {
	policy := cfg.EffectiveModelSelection()
	check := doctorCheck{Name: "Project " + id + " agent selection", Status: doctorOK}
	if problems := policy.Validate(); len(problems) > 0 {
		check.Status = doctorFail
		check.Detail = strings.Join(problems, "; ")
		return check
	}
	state := "disabled"
	if policy.Active() {
		state = "enabled"
	}
	details := []string{"automatic model selection: " + state}
	for _, backend := range cfg.AgentBackendConfigs() {
		source := cfg.Agents.Sources["backends."+backend.ID]
		if source == "" {
			source = "project"
		}
		details = append(details, fmt.Sprintf("backend %s (%s): %s", backend.ID, backend.Kind, source))
	}
	for _, route := range cfg.AgentRouteConfigs() {
		source := cfg.Agents.Sources["routes."+route.Name]
		if source == "" {
			source = "project"
		}
		details = append(details, fmt.Sprintf("route %s: %s, backend=%s, model=%s", route.Name, source, route.Backend, route.Model))
	}
	keys := make([]string, 0, len(policy.Sources))
	for key := range policy.Sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		details = append(details, key+"="+policy.Sources[key])
	}
	if policy.Configured() {
		encoded, err := json.Marshal(policy)
		if err != nil {
			check.Status = doctorFail
			check.Detail = "encode effective model policy: " + err.Error()
			return check
		}
		details = append(details, "effective policy: "+string(encoded))
	}
	details = append(details, "pricing: standard API USD estimates; subscription charges, credits, Fast mode, long context, and cache-write premiums can differ")
	check.Detail = strings.Join(details, "; ")
	return check
}
