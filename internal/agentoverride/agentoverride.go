package agentoverride

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

type Override struct {
	Model         string
	Effort        string
	Code          RoleOverride
	Rework        RoleOverride
	Merge         RoleOverride
	Plan          RoleOverride
	Routine       RoleOverride
	Validator     RoleOverride
	SecurityAudit RoleOverride
}

type RoleOverride struct {
	Model  string `yaml:"model"`
	Effort string `yaml:"effort"`
}

type RoleEffort struct {
	Role      string
	Field     string
	Effort    string
	Inherited bool
}

type blockYAML struct {
	Schema        int          `yaml:"schema"`
	Model         string       `yaml:"model"`
	Effort        string       `yaml:"effort"`
	Code          RoleOverride `yaml:"code"`
	Rework        RoleOverride `yaml:"rework"`
	Merge         RoleOverride `yaml:"merge"`
	Plan          RoleOverride `yaml:"plan"`
	Routine       RoleOverride `yaml:"routine"`
	Validator     RoleOverride `yaml:"validator"`
	SecurityAudit RoleOverride `yaml:"security_audit"`
}

func FromIssueBody(body string) (Override, bool, error) {
	content, ok := lastBlock(body)
	if !ok {
		return Override{}, false, nil
	}

	var raw blockYAML
	decoder := yaml.NewDecoder(strings.NewReader(content))
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return Override{}, true, fmt.Errorf("parse detent-agent YAML: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return Override{}, true, errors.New("parse detent-agent YAML: multiple YAML documents are not supported")
	} else if !errors.Is(err, io.EOF) {
		return Override{}, true, errors.New("parse detent-agent YAML: multiple YAML documents are not supported")
	}
	if raw.Schema != 1 {
		return Override{}, true, errors.New("detent-agent schema must be 1")
	}

	return Override{
		Model:         strings.TrimSpace(raw.Model),
		Effort:        strings.TrimSpace(raw.Effort),
		Code:          normalizeRoleOverride(raw.Code),
		Rework:        normalizeRoleOverride(raw.Rework),
		Merge:         normalizeRoleOverride(raw.Merge),
		Plan:          normalizeRoleOverride(raw.Plan),
		Routine:       normalizeRoleOverride(raw.Routine),
		Validator:     normalizeRoleOverride(raw.Validator),
		SecurityAudit: normalizeRoleOverride(raw.SecurityAudit),
	}, true, nil
}

func normalizeRoleOverride(override RoleOverride) RoleOverride {
	override.Model = strings.TrimSpace(override.Model)
	override.Effort = strings.TrimSpace(override.Effort)
	return override
}

func (o Override) EffortForRole(role string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "code":
		return o.Code.Effort, "code.effort"
	case "rework":
		if o.Rework.Effort != "" {
			return o.Rework.Effort, "rework.effort"
		}
		return o.Code.Effort, "code.effort"
	case "merge":
		return o.Merge.Effort, "merge.effort"
	case "plan":
		if o.Plan.Effort != "" {
			return o.Plan.Effort, "plan.effort"
		}
	case "routine":
		if o.Routine.Effort != "" {
			return o.Routine.Effort, "routine.effort"
		}
	case "validator":
		if o.Validator.Effort != "" {
			return o.Validator.Effort, "validator.effort"
		}
	case "security_audit":
		if o.SecurityAudit.Effort != "" {
			return o.SecurityAudit.Effort, "security_audit.effort"
		}
	default:
		return "", ""
	}
	return "", ""
}

func (o Override) ModelForRole(role string) (string, string) {
	var model string
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "code":
		model = o.Code.Model
	case "rework":
		model = o.Rework.Model
		if model == "" && o.Code.Model != "" {
			return o.Code.Model, "code.model"
		}
	case "merge":
		model = o.Merge.Model
	case "plan":
		model = o.Plan.Model
	case "routine":
		model = o.Routine.Model
	case "validator":
		model = o.Validator.Model
	case "security_audit":
		model = o.SecurityAudit.Model
	}
	if model != "" {
		return model, role + ".model"
	}
	return o.Model, "model"
}

func (o Override) RoleEfforts() []RoleEffort {
	rework := RoleEffort{Role: "rework", Field: "rework.effort", Effort: o.Rework.Effort}
	if rework.Effort == "" && o.Code.Effort != "" {
		rework.Field = "code.effort"
		rework.Effort = o.Code.Effort
		rework.Inherited = true
	}
	return []RoleEffort{
		{Role: "code", Field: "code.effort", Effort: o.Code.Effort},
		rework,
		{Role: "merge", Field: "merge.effort", Effort: o.Merge.Effort},
	}
}

func lastBlock(body string) (string, bool) {
	var last string
	found := false
	inFence := false
	fenceChar := byte(0)
	fenceLen := 0
	lines := []string{}

	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if !inFence {
			char, length, ok := fenceOpening(trimmed)
			if !ok {
				continue
			}
			inFence = true
			fenceChar = char
			fenceLen = length
			lines = lines[:0]
			continue
		}
		if fenceClosing(trimmed, fenceChar, fenceLen) {
			last = strings.Join(lines, "\n")
			found = true
			inFence = false
			continue
		}
		lines = append(lines, line)
	}

	return last, found
}

func fenceOpening(line string) (byte, int, bool) {
	if len(line) < 3 || (line[0] != '`' && line[0] != '~') {
		return 0, 0, false
	}
	char := line[0]
	length := 0
	for length < len(line) && line[length] == char {
		length++
	}
	if length < 3 {
		return 0, 0, false
	}
	fields := strings.Fields(strings.TrimSpace(line[length:]))
	if len(fields) == 0 || fields[0] != "detent-agent" {
		return 0, 0, false
	}
	return char, length, true
}

func fenceClosing(line string, char byte, length int) bool {
	if len(line) < length {
		return false
	}
	index := 0
	for index < len(line) && line[index] == char {
		index++
	}
	return index >= length && strings.TrimSpace(line[index:]) == ""
}
