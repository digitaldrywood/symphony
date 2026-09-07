package agentidentity

import (
	"strings"
	"time"
)

type Provenance string

const (
	ProvenanceUnknown        Provenance = "unknown"
	ProvenanceConfigured     Provenance = "configured"
	ProvenanceRuntime        Provenance = "runtime"
	ProvenanceCatalogDefault Provenance = "catalog_default"
)

type Value struct {
	Value      string     `json:"value,omitempty"`
	Provenance Provenance `json:"provenance,omitempty"`
}

func NewValue(value string, provenance Provenance) Value {
	value = strings.TrimSpace(value)
	if value == "" {
		return Value{}
	}
	provenance = normalizeProvenance(provenance)
	if provenance == "" {
		provenance = ProvenanceUnknown
	}
	return Value{Value: value, Provenance: provenance}
}

func UnknownValue() Value {
	return Value{Provenance: ProvenanceUnknown}
}

func (v Value) IsZero() bool {
	return strings.TrimSpace(v.Value) == "" && normalizeProvenance(v.Provenance) == ""
}

func (v Value) Known() bool {
	return strings.TrimSpace(v.Value) != ""
}

func (v Value) Normalize() Value {
	v.Value = strings.TrimSpace(v.Value)
	v.Provenance = normalizeProvenance(v.Provenance)
	if v.Value == "" && v.Provenance != ProvenanceUnknown {
		v.Provenance = ""
	}
	if v.Value != "" && v.Provenance == "" {
		v.Provenance = ProvenanceUnknown
	}
	return v
}

type Identity struct {
	Selection       Selection  `json:"selection,omitzero"`
	BackendID       string     `json:"backend_id,omitempty"`
	BackendKind     string     `json:"backend_kind,omitempty"`
	Route           string     `json:"route,omitempty"`
	Role            string     `json:"role,omitempty"`
	Provider        Value      `json:"provider,omitzero"`
	RequestedModel  Value      `json:"requested_model,omitzero"`
	ResolvedModel   Value      `json:"resolved_model,omitzero"`
	ReasoningEffort Value      `json:"reasoning_effort,omitzero"`
	ServiceTier     Value      `json:"service_tier,omitzero"`
	ObservedAt      *time.Time `json:"observed_at,omitempty"`
}

type Selection struct {
	Policy         string `json:"policy,omitempty"`
	PolicySource   string `json:"policy_source,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Level          string `json:"level,omitempty"`
	RequestedModel string `json:"requested_model,omitempty"`
	ModelSource    string `json:"model_source,omitempty"`
	EffortSource   string `json:"effort_source,omitempty"`
	FallbackReason string `json:"fallback_reason,omitempty"`
	BackendSource  string `json:"backend_source,omitempty"`
	RouteSource    string `json:"route_source,omitempty"`
}

func Configured(backendID string, backendKind string, route string, role string, requestedModel string, provider string, effort string, serviceTier string, observedAt time.Time) Identity {
	identity := Identity{
		BackendID:       backendID,
		BackendKind:     backendKind,
		Route:           route,
		Role:            role,
		Provider:        NewValue(provider, ProvenanceConfigured),
		RequestedModel:  NewValue(requestedModel, ProvenanceConfigured),
		ReasoningEffort: NewValue(effort, ProvenanceConfigured),
		ServiceTier:     NewValue(serviceTier, ProvenanceConfigured),
	}
	if !observedAt.IsZero() {
		at := observedAt.UTC()
		identity.ObservedAt = &at
	}
	return identity.Normalize()
}

func RuntimeUpdate(model string, provider string, effort string, serviceTier string, observedAt time.Time) Identity {
	identity := Identity{
		Provider:        NewValue(provider, ProvenanceRuntime),
		ResolvedModel:   NewValue(model, ProvenanceRuntime),
		ReasoningEffort: NewValue(effort, ProvenanceRuntime),
		ServiceTier:     NewValue(serviceTier, ProvenanceRuntime),
	}
	if !observedAt.IsZero() {
		at := observedAt.UTC()
		identity.ObservedAt = &at
	}
	return identity.Normalize()
}

func (i Identity) IsZero() bool {
	i = i.Normalize()
	return i.Selection == (Selection{}) && i.BackendID == "" &&
		i.BackendKind == "" &&
		i.Route == "" &&
		i.Role == "" &&
		i.Provider.IsZero() &&
		i.RequestedModel.IsZero() &&
		i.ResolvedModel.IsZero() &&
		i.ReasoningEffort.IsZero() &&
		i.ServiceTier.IsZero() &&
		i.ObservedAt == nil
}

func (i Identity) Normalize() Identity {
	i.BackendID = strings.TrimSpace(i.BackendID)
	i.BackendKind = strings.TrimSpace(i.BackendKind)
	i.Route = strings.TrimSpace(i.Route)
	i.Role = strings.TrimSpace(i.Role)
	i.Provider = i.Provider.Normalize()
	i.RequestedModel = i.RequestedModel.Normalize()
	i.ResolvedModel = i.ResolvedModel.Normalize()
	i.ReasoningEffort = i.ReasoningEffort.Normalize()
	i.ServiceTier = i.ServiceTier.Normalize()
	if i.ObservedAt != nil {
		at := i.ObservedAt.UTC()
		i.ObservedAt = &at
	}
	return i
}

func (i Identity) Merge(update Identity) Identity {
	out := i.Normalize()
	update = update.Normalize()
	if update.Selection != (Selection{}) {
		out.Selection = update.Selection
	}
	if update.BackendID != "" {
		out.BackendID = update.BackendID
	}
	if update.BackendKind != "" {
		out.BackendKind = update.BackendKind
	}
	if update.Route != "" {
		out.Route = update.Route
	}
	if update.Role != "" {
		out.Role = update.Role
	}
	if !update.Provider.IsZero() {
		out.Provider = update.Provider
	}
	if !update.RequestedModel.IsZero() {
		out.RequestedModel = update.RequestedModel
	}
	if !update.ResolvedModel.IsZero() {
		out.ResolvedModel = update.ResolvedModel
	}
	if !update.ReasoningEffort.IsZero() {
		out.ReasoningEffort = update.ReasoningEffort
	}
	if !update.ServiceTier.IsZero() {
		out.ServiceTier = update.ServiceTier
	}
	if update.ObservedAt != nil {
		at := update.ObservedAt.UTC()
		out.ObservedAt = &at
	}
	return out.Normalize()
}

func (i Identity) Model() string {
	i = i.Normalize()
	if i.ResolvedModel.Value != "" {
		return i.ResolvedModel.Value
	}
	return i.RequestedModel.Value
}

func (i Identity) RequestedDiffers() bool {
	i = i.Normalize()
	return i.RequestedModel.Value != "" && i.ResolvedModel.Value != "" && i.RequestedModel.Value != i.ResolvedModel.Value
}

func (i Identity) ObserveAt(observedAt time.Time) Identity {
	i = i.Normalize()
	if observedAt.IsZero() {
		return i
	}
	at := observedAt.UTC()
	i.ObservedAt = &at
	return i
}

func (i Identity) MateriallyEqual(other Identity) bool {
	i = i.Normalize()
	other = other.Normalize()
	i.ObservedAt = nil
	other.ObservedAt = nil
	return i == other
}

func (i Identity) HasRuntimeValues() bool {
	i = i.Normalize()
	for _, value := range []Value{i.Provider, i.RequestedModel, i.ResolvedModel, i.ReasoningEffort, i.ServiceTier} {
		if value.Provenance == ProvenanceRuntime {
			return true
		}
	}
	return false
}

func normalizeProvenance(provenance Provenance) Provenance {
	switch Provenance(strings.ToLower(strings.TrimSpace(string(provenance)))) {
	case ProvenanceUnknown:
		return ProvenanceUnknown
	case ProvenanceConfigured:
		return ProvenanceConfigured
	case ProvenanceRuntime:
		return ProvenanceRuntime
	case ProvenanceCatalogDefault:
		return ProvenanceCatalogDefault
	default:
		return ""
	}
}
