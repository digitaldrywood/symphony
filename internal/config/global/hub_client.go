package global

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultHubTokenEnvironment     = "DETENT_HUB_TOKEN"
	DefaultHubHeartbeatSeconds     = 30
	DefaultHubLeaseTTLSeconds      = 90
	DefaultHubRequestTimeoutMillis = 10000
)

type HubClient struct {
	ArtifactServiceID        string            `yaml:"artifact_service_id,omitempty"`
	ArtifactBytes            int64             `yaml:"artifact_bytes,omitempty"`
	ProviderCapacityFile     string            `yaml:"provider_capacity_file,omitempty"`
	OrganizationID           string            `yaml:"organization_id,omitempty"`
	NativeProjects           map[string]string `yaml:"native_projects,omitempty"`
	URL                      string            `yaml:"hub_url,omitempty"`
	TokenEnvironment         string            `yaml:"token_env,omitempty"`
	IdentityFile             string            `yaml:"identity_file,omitempty"`
	MachineID                string            `yaml:"machine_id,omitempty"`
	DisplayName              string            `yaml:"display_name,omitempty"`
	Capacity                 int               `yaml:"capacity,omitempty"`
	HeartbeatIntervalSeconds int               `yaml:"heartbeat_interval_seconds,omitempty"`
	LeaseTTLSeconds          int               `yaml:"lease_ttl_seconds,omitempty"`
	RequestTimeoutMS         int               `yaml:"request_timeout_ms,omitempty"`
}

func (c HubClient) IsZero() bool {
	if c.ArtifactServiceID != "" || c.ArtifactBytes != 0 {
		return false
	}
	return strings.TrimSpace(c.ProviderCapacityFile) == "" && strings.TrimSpace(c.IdentityFile) == "" && strings.TrimSpace(c.OrganizationID) == "" && len(c.NativeProjects) == 0 && strings.TrimSpace(c.URL) == "" && strings.TrimSpace(c.TokenEnvironment) == "" &&
		strings.TrimSpace(c.MachineID) == "" && strings.TrimSpace(c.DisplayName) == "" && c.Capacity == 0 &&
		c.HeartbeatIntervalSeconds == 0 && c.LeaseTTLSeconds == 0 && c.RequestTimeoutMS == 0
}

func (c HubClient) Configured() bool {
	return strings.TrimSpace(c.URL) != ""
}

func (c HubClient) Normalized() HubClient {
	c.ProviderCapacityFile = strings.TrimSpace(c.ProviderCapacityFile)
	c.OrganizationID = strings.TrimSpace(c.OrganizationID)
	c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
	c.TokenEnvironment = strings.TrimSpace(c.TokenEnvironment)
	c.IdentityFile = strings.TrimSpace(c.IdentityFile)
	if c.TokenEnvironment == "" && c.IdentityFile == "" {
		c.TokenEnvironment = DefaultHubTokenEnvironment
	}
	c.MachineID = strings.TrimSpace(c.MachineID)
	c.DisplayName = strings.TrimSpace(c.DisplayName)
	if c.HeartbeatIntervalSeconds <= 0 {
		c.HeartbeatIntervalSeconds = DefaultHubHeartbeatSeconds
	}
	if c.LeaseTTLSeconds <= 0 {
		c.LeaseTTLSeconds = DefaultHubLeaseTTLSeconds
	}
	if c.RequestTimeoutMS <= 0 {
		c.RequestTimeoutMS = DefaultHubRequestTimeoutMillis
	}
	return c
}

func (c HubClient) HeartbeatInterval() time.Duration {
	return time.Duration(c.Normalized().HeartbeatIntervalSeconds) * time.Second
}

func (c HubClient) LeaseTTL() time.Duration {
	return time.Duration(c.Normalized().LeaseTTLSeconds) * time.Second
}

func (c HubClient) RequestTimeout() time.Duration {
	return time.Duration(c.Normalized().RequestTimeoutMS) * time.Millisecond
}

func (c HubClient) Validate() []string {
	if !c.Configured() {
		if !c.IsZero() {
			return []string{"client.hub_url is required when Hub client settings are configured"}
		}
		return nil
	}
	var problems []string
	serviceID := strings.TrimPrefix(c.ArtifactServiceID, "service_")
	_, idErr := hex.DecodeString(serviceID)
	if c.ArtifactServiceID != "" && (!strings.HasPrefix(c.ArtifactServiceID, "service_") || len(serviceID) != 32 || strings.ToLower(serviceID) != serviceID || idErr != nil || c.ArtifactBytes <= 1<<20 || c.ArtifactBytes > 256<<20) || c.ArtifactServiceID == "" && c.ArtifactBytes != 0 {
		problems = append(problems, "client.artifact_service_id requires an opaque service ID and finite artifact_bytes greater than 1048576 and at most 268435456")
	}
	if c.ProviderCapacityFile != "" && (!filepath.IsAbs(c.ProviderCapacityFile) || c.IdentityFile == "") {
		problems = append(problems, "client.provider_capacity_file requires an absolute report path and an enrolled identity_file")
	}
	if c.IdentityFile != "" {
		if !filepath.IsAbs(c.IdentityFile) {
			problems = append(problems, "client.identity_file must be an absolute private path")
		}
		if c.TokenEnvironment != "" {
			problems = append(problems, "client.identity_file and client.token_env are mutually exclusive")
		}
		if len(c.NativeProjects) == 0 {
			problems = append(problems, "client.identity_file requires explicit native_projects")
		}
	}
	if len(c.NativeProjects) > 0 && !strings.HasPrefix(c.OrganizationID, "org_") {
		problems = append(problems, "client.organization_id is required for native projects")
	}
	for name, id := range c.NativeProjects {
		if strings.TrimSpace(name) == "" || !strings.HasPrefix(id, "prj_") || strings.ContainsAny(id+c.OrganizationID, "/?#%\\") {
			problems = append(problems, "client.native_projects must map local project names to native project IDs")
		}
	}
	parsed, err := url.Parse(strings.TrimSpace(c.URL))
	if err != nil || !parsed.IsAbs() || strings.TrimSpace(parsed.Host) == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		problems = append(problems, "client.hub_url must be an absolute http or https URL")
	}
	if strings.ContainsAny(c.TokenEnvironment, "\r\n") || (strings.TrimSpace(c.TokenEnvironment) != "" && !validEnvironmentName(c.TokenEnvironment)) {
		problems = append(problems, "client.token_env must be an environment variable name")
	}
	if strings.ContainsAny(c.MachineID, "\r\n") {
		problems = append(problems, "client.machine_id must be a single line")
	}
	if c.Capacity < 0 {
		problems = append(problems, "client.capacity must be greater than 0")
	}
	if c.HeartbeatIntervalSeconds < 0 {
		problems = append(problems, "client.heartbeat_interval_seconds must be greater than 0")
	}
	if c.LeaseTTLSeconds < 0 {
		problems = append(problems, "client.lease_ttl_seconds must be greater than 0")
	}
	if c.RequestTimeoutMS < 0 {
		problems = append(problems, "client.request_timeout_ms must be greater than 0")
	}
	normalized := c.Normalized()
	if normalized.HeartbeatIntervalSeconds >= normalized.LeaseTTLSeconds {
		problems = append(problems, "client.heartbeat_interval_seconds must be shorter than client.lease_ttl_seconds")
	}
	return problems
}

func hubClientRawErrors(value any) []string {
	if value == nil {
		return nil
	}
	attrs, ok := value.(map[string]any)
	if !ok {
		return []string{"client: must be a mapping"}
	}
	var problems []string
	for _, name := range []string{"hub_url", "token_env", "identity_file", "provider_capacity_file", "machine_id", "display_name", "organization_id"} {
		problems = append(problems, optionalStringTypeError(attrs, name)...)
	}
	for _, field := range []struct{ name, path string }{
		{"capacity", "client.capacity"},
		{"heartbeat_interval_seconds", "client.heartbeat_interval_seconds"},
		{"lease_ttl_seconds", "client.lease_ttl_seconds"},
		{"request_timeout_ms", "client.request_timeout_ms"},
	} {
		if value, configured := attrs[field.name]; configured && !positiveInteger(value) {
			problems = append(problems, field.path+": must be a positive integer")
		}
	}
	return problems
}

func buildHubClient(value any) (HubClient, error) {
	if value == nil {
		return HubClient{}, nil
	}
	if _, err := mapValue(value, "client"); err != nil {
		return HubClient{}, err
	}
	var client HubClient
	if err := decodeYAMLValue(value, &client); err != nil {
		return HubClient{}, fmt.Errorf("client: %w", err)
	}
	return client, nil
}

func validEnvironmentName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for index, char := range value {
		if (char >= 'A' && char <= 'Z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}
