package global

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/activehours"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	intakeconfig "github.com/digitaldrywood/detent/internal/intake"
	"github.com/digitaldrywood/detent/internal/projectcolor"
	"github.com/digitaldrywood/detent/internal/selector"
)

const (
	APIVersion                      = "detent/v1"
	Kind                            = "GlobalConfig"
	DefaultUpdateCheckIntervalHours = 6
	DefaultUpdateMaxDeferralHours   = 6

	SchedulingWeighted                            = "weighted"
	SchedulingStrict                              = "strict"
	SchedulingRoundRobin                          = "round_robin"
	SchedulingFairShare                           = "fair_share"
	DefaultAgentPoolName                          = "default"
	DashboardAccessModePrivateToken               = "private_token"
	DefaultMaxAgentRSSBytes                 int64 = 8 * 1024 * 1024 * 1024
	DefaultMemoryPressureSomeAvg60Threshold       = 10.0
	DefaultMemoryPollIntervalMS                   = 1000
	DefaultIOPressureFullAvg10Threshold           = 5.0
	DefaultIOPressurePollIntervalMS               = 1000
	DefaultCPUPressureSomeAvg10Threshold          = 80.0
	DefaultCPUPressurePollIntervalMS              = 1000

	configFileMode = 0o600
)

const plainWorkflowPathRequirement = "must be absolute or home-relative when workflow_ref is empty"

var schedulingModes = []string{
	SchedulingWeighted,
	SchedulingStrict,
	SchedulingRoundRobin,
	SchedulingFairShare,
}

type PathRule string

const (
	PathRuleFlag                PathRule = "--config"
	PathRuleEnvConfig           PathRule = "CONFIG"
	PathRuleDeprecatedEnvConfig PathRule = "DETENT_CONFIG"
	PathRuleEnvHome             PathRule = "CONFIG_HOME"
	PathRuleDeprecatedEnvHome   PathRule = "DETENT_HOME"
	PathRuleUserConfigDir       PathRule = "os.UserConfigDir()"
	PathRuleLegacyHome          PathRule = "~/.detent"
)

type PathResolution struct {
	Path string
	Rule PathRule
}

type Config struct {
	Path                  string          `yaml:"-"`
	APIVersion            string          `yaml:"apiVersion"`
	Kind                  string          `yaml:"kind"`
	Env                   string          `yaml:"env,omitempty"`
	LogLevel              string          `yaml:"log_level,omitempty"`
	LogMaxSizeBytes       *int            `yaml:"log_max_size_bytes,omitempty"`
	LogMaxBackups         *int            `yaml:"log_max_backups,omitempty"`
	GitHubToken           string          `yaml:"github_token,omitempty"`
	APIToken              string          `yaml:"api_token,omitempty"`
	TrustLoopbackPeerRead bool            `yaml:"trust_loopback_peer_read,omitempty"`
	DashboardAccess       DashboardAccess `yaml:"dashboard_access,omitempty"`
	Client                HubClient       `yaml:"client,omitempty"`
	Ops                   Ops             `yaml:"ops,omitempty"`
	Port                  *int            `yaml:"port,omitempty"`
	InstanceName          string          `yaml:"instance_name,omitempty"`
	Notifications         Notifications   `yaml:"notifications,omitempty"`
	Update                Update          `yaml:"update,omitempty"`
	Auth                  Auth            `yaml:"auth,omitempty"`
	Global                Settings        `yaml:"global"`
	Projects              []Project       `yaml:"projects"`
}

type DashboardAccess struct {
	Mode       string `yaml:"mode,omitempty"`
	Token      string `yaml:"token,omitempty"`
	AllowWrite bool   `yaml:"allow_write,omitempty"`
}

type Ops struct {
	TmuxWindowStatus *bool `yaml:"tmux_window_status,omitempty"`
}

func (o Ops) IsZero() bool {
	return o.TmuxWindowStatus == nil
}

func (a DashboardAccess) IsZero() bool {
	return strings.TrimSpace(a.Mode) == "" && strings.TrimSpace(a.Token) == "" && !a.AllowWrite
}

type Update struct {
	AutoCheckEnabled   bool `yaml:"auto_check_enabled,omitempty"`
	CheckIntervalHours int  `yaml:"check_interval_hours,omitempty"`
	AutoApplyEnabled   bool `yaml:"auto_apply_enabled,omitempty"`
	MaxDeferralHours   int  `yaml:"max_deferral_hours,omitempty"`
}

func (u Update) IsZero() bool {
	return !u.AutoCheckEnabled && u.CheckIntervalHours == 0 && !u.AutoApplyEnabled && u.MaxDeferralHours == 0
}

func (u Update) NormalizedCheckIntervalHours() int {
	if u.CheckIntervalHours > 0 {
		return u.CheckIntervalHours
	}
	return DefaultUpdateCheckIntervalHours
}

func (u Update) NormalizedMaxDeferralHours() int {
	if u.MaxDeferralHours > 0 {
		return u.MaxDeferralHours
	}
	return DefaultUpdateMaxDeferralHours
}

type Settings struct {
	Agents              workflowconfig.Agents              `yaml:"agents,omitempty"`
	Budget              workflowconfig.AgentBudgetDefaults `yaml:"budget,omitempty"`
	MaxConcurrentAgents int                                `yaml:"max_concurrent_agents"`
	RateWindowPacing    workflowconfig.RateWindowPacing    `yaml:"rate_window_pacing"`
	Scheduling          string                             `yaml:"scheduling"`
	AgentPools          []AgentPool                        `yaml:"agent_pools,omitempty"`
	ActiveHours         *activehours.Config                `yaml:"active_hours,omitempty"`
	Identity            Identity                           `yaml:"identity,omitempty"`
	Knowledge           Knowledge                          `yaml:"knowledge,omitempty"`
	FairShare           map[string]any                     `yaml:"fair_share,omitempty"`
	Startup             map[string]any                     `yaml:"startup,omitempty"`
	Memory              Memory                             `yaml:"memory,omitempty"`
	IO                  IO                                 `yaml:"io,omitempty"`
	CPU                 CPU                                `yaml:"cpu,omitempty"`
}

type Memory struct {
	MaxAgentRSSBytes           int64   `yaml:"max_agent_rss_bytes"`
	PressureSomeAvg60Threshold float64 `yaml:"pressure_some_avg60_threshold"`
	PollIntervalMS             int     `yaml:"poll_interval_ms"`
}

type ProjectMemory struct {
	MaxAgentRSSBytes *int64 `yaml:"max_agent_rss_bytes,omitempty"`
}

type IO struct {
	PressureFullAvg10Threshold  float64 `yaml:"pressure_full_avg10_threshold"`
	DegradedMaxConcurrentAgents int     `yaml:"degraded_max_concurrent_agents,omitempty"`
	PollIntervalMS              int     `yaml:"poll_interval_ms"`
}

type CPU struct {
	PressureSomeAvg10Threshold  float64 `yaml:"pressure_some_avg10_threshold"`
	DegradedMaxConcurrentAgents int     `yaml:"degraded_max_concurrent_agents,omitempty"`
	PollIntervalMS              int     `yaml:"poll_interval_ms"`
}

func (m Memory) Normalized() Memory {
	if m.MaxAgentRSSBytes <= 0 {
		m.MaxAgentRSSBytes = DefaultMaxAgentRSSBytes
	}
	if m.PressureSomeAvg60Threshold <= 0 {
		m.PressureSomeAvg60Threshold = DefaultMemoryPressureSomeAvg60Threshold
	}
	if m.PollIntervalMS <= 0 {
		m.PollIntervalMS = DefaultMemoryPollIntervalMS
	}
	return m
}

func (p IO) Normalized() IO {
	if p.PressureFullAvg10Threshold <= 0 {
		p.PressureFullAvg10Threshold = DefaultIOPressureFullAvg10Threshold
	}
	if p.PollIntervalMS <= 0 {
		p.PollIntervalMS = DefaultIOPressurePollIntervalMS
	}
	return p
}

func (p CPU) Normalized() CPU {
	if p.PressureSomeAvg10Threshold <= 0 {
		p.PressureSomeAvg10Threshold = DefaultCPUPressureSomeAvg10Threshold
	}
	if p.PollIntervalMS <= 0 {
		p.PollIntervalMS = DefaultCPUPressurePollIntervalMS
	}
	return p
}

type AgentPool struct {
	Name                string `yaml:"name"`
	MaxConcurrentAgents int    `yaml:"max_concurrent_agents"`
	BurstTo             int    `yaml:"burst_to,omitempty"`
	Scheduling          string `yaml:"scheduling,omitempty"`
}

type Project struct {
	GlobalAgents             workflowconfig.Agents              `yaml:"-"`
	GlobalBudget             workflowconfig.AgentBudgetDefaults `yaml:"-"`
	ID                       string                             `yaml:"id"`
	Pool                     string                             `yaml:"pool,omitempty"`
	Workflow                 string                             `yaml:"workflow"`
	WorkflowRef              string                             `yaml:"workflow_ref,omitempty"`
	Workdir                  string                             `yaml:"workdir"`
	Color                    string                             `yaml:"color,omitempty"`
	Knowledge                Knowledge                          `yaml:"knowledge,omitempty"`
	Weight                   int                                `yaml:"weight"`
	Priority                 int                                `yaml:"priority"`
	Paused                   bool                               `yaml:"paused,omitempty"`
	PausedReason             string                             `yaml:"paused_reason,omitempty"`
	PausedAt                 string                             `yaml:"paused_at,omitempty"`
	PausedUntilIssue         string                             `yaml:"paused_until_issue,omitempty"`
	PausedUntil              string                             `yaml:"paused_until,omitempty"`
	ActiveHours              *activehours.Config                `yaml:"active_hours,omitempty"`
	ActiveHoursOverrideUntil string                             `yaml:"active_hours_override_until,omitempty"`
	CredentialRef            string                             `yaml:"credential_ref,omitempty"`
	Authorization            selector.Selector                  `yaml:"authorization,omitempty"`
	Intake                   intakeconfig.Config                `yaml:"intake,omitempty"`
	Memory                   ProjectMemory                      `yaml:"memory,omitempty"`
	Identity                 Identity                           `yaml:"-"`
	GlobalKnowledge          Knowledge                          `yaml:"-"`
	GlobalActiveHours        *activehours.Config                `yaml:"-"`
	GlobalRateWindowPacing   workflowconfig.RateWindowPacing    `yaml:"-"`
	GlobalMemory             Memory                             `yaml:"-"`
	GlobalIO                 IO                                 `yaml:"-"`
	GlobalCPU                CPU                                `yaml:"-"`
	IntakeConfigured         bool                               `yaml:"-"`
}

func (p Project) EffectiveMemory() Memory {
	memory := p.GlobalMemory.Normalized()
	if p.Memory.MaxAgentRSSBytes != nil {
		memory.MaxAgentRSSBytes = *p.Memory.MaxAgentRSSBytes
	}
	return memory
}

type Identity = workflowconfig.Identity
type Knowledge = workflowconfig.Knowledge
type KnowledgeSource = workflowconfig.KnowledgeSource

type Option func(*options)

type MissingFileError struct {
	Path string
	Err  error
}

type ParseError struct {
	Path string
	Err  error
}

type ValidationError struct {
	Path     string
	Problems []string
}

type options struct {
	home                      string
	relativeTo                string
	projectPathLiterals       bool
	allowMissingWorkflowFiles bool
}

type pathOptions struct {
	config        options
	lookupEnv     func(string) string
	userConfigDir func() (string, error)
	stat          func(string) (os.FileInfo, error)
}

func WithHome(home string) Option {
	return func(opts *options) {
		opts.home = home
	}
}

func WithRelativeTo(path string) Option {
	return func(opts *options) {
		opts.relativeTo = path
	}
}

func WithProjectPathLiterals() Option {
	return func(opts *options) {
		opts.projectPathLiterals = true
	}
}

func WithMissingWorkflowFiles() Option {
	return func(opts *options) {
		opts.allowMissingWorkflowFiles = true
	}
}

func ResolvePath(configPath string) (PathResolution, error) {
	return resolvePath(configPath, defaultPathOptions())
}

func DefaultPath() (string, error) {
	resolution, err := ResolvePath("")
	if err != nil {
		return "", err
	}
	return resolution.Path, nil
}

func Default() (Config, error) {
	path, err := DefaultPath()
	if err != nil {
		return Config{}, err
	}
	return defaultConfig(path), nil
}

func DefaultAt(path string, opts ...Option) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, errors.New("global config path is required")
	}

	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	expandedPath, err := expandPath(path, readOptions)
	if err != nil {
		return Config{}, err
	}
	return defaultConfig(expandedPath), nil
}

func Read(path string, opts ...Option) (Config, error) {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	expandedPath, err := expandPath(path, readOptions)
	if err != nil {
		return Config{}, err
	}

	raw, err := os.ReadFile(expandedPath)
	if err != nil {
		return Config{}, MissingFileError{Path: expandedPath, Err: err}
	}

	return Parse(raw, expandedPath, opts...)
}

func ReadProject(path string, projectID string, opts ...Option) (Config, []string, error) {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	expandedPath, err := expandPath(path, readOptions)
	if err != nil {
		return Config{}, nil, err
	}

	raw, err := os.ReadFile(expandedPath)
	if err != nil {
		return Config{}, nil, MissingFileError{Path: expandedPath, Err: err}
	}

	return parseProject(raw, expandedPath, projectID, opts...)
}

func Write(path string, cfg Config, opts ...Option) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("global config path is required")
	}

	writeOptions := defaultOptions()
	for _, opt := range opts {
		opt(&writeOptions)
	}

	expandedPath, err := expandPath(path, writeOptions)
	if err != nil {
		return err
	}

	cfg.Path = expandedPath
	cfg.Global.Memory = cfg.Global.Memory.Normalized()
	cfg.Global.IO = cfg.Global.IO.Normalized()
	cfg.Global.CPU = cfg.Global.CPU.Normalized()
	if err := cfg.Validate(opts...); err != nil {
		return err
	}

	raw, err := marshalConfigPreservingComments(expandedPath, cfg)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(expandedPath), 0o755); err != nil {
		return fmt.Errorf("create global config directory %s: %w", filepath.Dir(expandedPath), err)
	}
	if err := os.WriteFile(expandedPath, raw, configFileMode); err != nil {
		return fmt.Errorf("write global config %s: %w", expandedPath, err)
	}
	if err := os.Chmod(expandedPath, configFileMode); err != nil {
		return fmt.Errorf("restrict global config permissions %s: %w", expandedPath, err)
	}

	return nil
}

func marshalConfigPreservingComments(path string, cfg Config) ([]byte, error) {
	raw, err := yaml.Marshal(cfg) // #nosec G117 -- the operator-provided API token must be persisted in the permission-restricted config file.
	if err != nil {
		return nil, fmt.Errorf("marshal global config %s: %w", path, err)
	}

	existingRaw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raw, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read global config %s for update: %w", path, err)
	}

	var existing yaml.Node
	if err := yaml.Unmarshal(existingRaw, &existing); err != nil {
		return nil, fmt.Errorf("parse global config %s for update: %w", path, err)
	}
	var desired yaml.Node
	if err := yaml.Unmarshal(raw, &desired); err != nil {
		return nil, fmt.Errorf("parse marshaled global config %s: %w", path, err)
	}
	mergeYAMLNode(&existing, &desired)

	merged, err := yaml.Marshal(&existing)
	if err != nil {
		return nil, fmt.Errorf("marshal updated global config %s: %w", path, err)
	}
	return merged, nil
}

func mergeYAMLNode(existing *yaml.Node, desired *yaml.Node) {
	if existing == nil || desired == nil {
		return
	}
	if existing.Kind != desired.Kind {
		replaceYAMLNode(existing, desired)
		return
	}

	switch desired.Kind {
	case yaml.DocumentNode:
		if len(existing.Content) == 1 && len(desired.Content) == 1 {
			mergeYAMLNode(existing.Content[0], desired.Content[0])
			return
		}
		replaceYAMLNode(existing, desired)
	case yaml.MappingNode:
		mergeYAMLMapping(existing, desired)
	case yaml.SequenceNode:
		mergeYAMLSequence(existing, desired)
	default:
		replaceYAMLNode(existing, desired)
	}
}

func mergeYAMLMapping(existing *yaml.Node, desired *yaml.Node) {
	desiredValues := make(map[string]int, len(desired.Content)/2)
	for index := 0; index+1 < len(desired.Content); index += 2 {
		desiredValues[desired.Content[index].Value] = index
	}

	merged := make([]*yaml.Node, 0, len(desired.Content))
	used := make(map[string]struct{}, len(desiredValues))
	for index := 0; index+1 < len(existing.Content); index += 2 {
		key := existing.Content[index]
		value := existing.Content[index+1]
		desiredIndex, ok := desiredValues[key.Value]
		if !ok {
			continue
		}
		mergeYAMLNode(value, desired.Content[desiredIndex+1])
		merged = append(merged, key, value)
		used[key.Value] = struct{}{}
	}
	for index := 0; index+1 < len(desired.Content); index += 2 {
		key := desired.Content[index]
		if _, ok := used[key.Value]; ok {
			continue
		}
		merged = append(merged, key, desired.Content[index+1])
	}
	existing.Content = merged
}

func mergeYAMLSequence(existing *yaml.Node, desired *yaml.Node) {
	existingByKey := make(map[string]*yaml.Node, len(existing.Content))
	existingKeyCounts := make(map[string]int, len(existing.Content))
	for _, item := range existing.Content {
		if key := yamlSequenceMappingKey(item); key != "" {
			existingByKey[key] = item
			existingKeyCounts[key]++
		}
	}
	desiredKeyCounts := make(map[string]int, len(desired.Content))
	for _, item := range desired.Content {
		if key := yamlSequenceMappingKey(item); key != "" {
			desiredKeyCounts[key]++
		}
	}

	merged := make([]*yaml.Node, 0, len(desired.Content))
	for index, desiredItem := range desired.Content {
		var existingItem *yaml.Node
		key := yamlSequenceMappingKey(desiredItem)
		if key != "" && existingKeyCounts[key] == 1 && desiredKeyCounts[key] == 1 {
			existingItem = existingByKey[key]
		} else if index < len(existing.Content) {
			existingItem = existing.Content[index]
		}
		if existingItem == nil {
			merged = append(merged, desiredItem)
			continue
		}
		mergeYAMLNode(existingItem, desiredItem)
		merged = append(merged, existingItem)
	}
	existing.Content = merged
}

func yamlSequenceMappingKey(node *yaml.Node) string {
	for _, key := range []string{"id", "name"} {
		if value := yamlMappingScalar(node, key); value != "" {
			return key + ":" + value
		}
	}
	return ""
}

func yamlMappingScalar(node *yaml.Node, key string) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key && node.Content[index+1].Kind == yaml.ScalarNode {
			return strings.TrimSpace(node.Content[index+1].Value)
		}
	}
	return ""
}

func replaceYAMLNode(existing *yaml.Node, desired *yaml.Node) {
	headComment := existing.HeadComment
	lineComment := existing.LineComment
	footComment := existing.FootComment
	style := existing.Style
	unchangedScalar := existing.Kind == yaml.ScalarNode &&
		desired.Kind == yaml.ScalarNode &&
		existing.Tag == desired.Tag &&
		existing.Value == desired.Value
	*existing = *desired
	existing.HeadComment = headComment
	existing.LineComment = lineComment
	existing.FootComment = footComment
	if unchangedScalar {
		existing.Style = style
	}
}

func ReadOrDefault(path string, opts ...Option) (Config, error) {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	expandedPath, err := expandPath(path, readOptions)
	if err != nil {
		return Config{}, err
	}

	cfg, err := Read(expandedPath, opts...)
	if err == nil {
		return cfg, nil
	}

	var missing MissingFileError
	if errors.As(err, &missing) && errors.Is(missing.Err, os.ErrNotExist) {
		return defaultConfig(expandedPath), nil
	}

	return Config{}, err
}

func Parse(raw []byte, path string, opts ...Option) (Config, error) {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	root, err := decodeConfigRoot(raw, path)
	if err != nil {
		return Config{}, err
	}

	return parseConfigRoot(root, path, readOptions, opts...)
}

func parseProject(raw []byte, path string, projectID string, opts ...Option) (Config, []string, error) {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	root, err := decodeConfigRoot(raw, path)
	if err != nil {
		return Config{}, nil, err
	}

	skippedProjects := scopeRawProjects(root, projectID)
	cfg, err := parseConfigRoot(root, path, readOptions, opts...)
	if err != nil {
		return Config{}, skippedProjects, err
	}
	return cfg, skippedProjects, nil
}

func decodeConfigRoot(raw []byte, path string) (map[string]any, error) {
	var decoded any
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return nil, ParseError{Path: path, Err: err}
	}

	root, ok := normalizeYAML(decoded).(map[string]any)
	if !ok {
		return nil, ValidationError{Path: path, Problems: []string{"root: must be a mapping"}}
	}
	return root, nil
}

func parseConfigRoot(root map[string]any, path string, readOptions options, opts ...Option) (Config, error) {
	if problems := validateRaw(root, readOptions); len(problems) > 0 {
		return Config{}, ValidationError{Path: path, Problems: problems}
	}

	cfg, err := build(root, path, readOptions)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(opts...); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func scopeRawProjects(root map[string]any, projectID string) []string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil
	}
	projects, ok := root["projects"].([]any)
	if !ok {
		return nil
	}

	scopedProjects := make([]any, 0, len(projects))
	skippedProjects := make([]string, 0, len(projects))
	for _, item := range projects {
		id := rawProjectID(item)
		if id == projectID {
			scopedProjects = append(scopedProjects, item)
			continue
		}
		if id == "" {
			id = "project"
		}
		skippedProjects = append(skippedProjects, id)
	}
	root["projects"] = scopedProjects
	return skippedProjects
}

func rawProjectID(value any) string {
	project, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	id, ok := project["id"].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(id)
}

func (c Config) Validate(opts ...Option) error {
	readOptions := defaultOptions()
	for _, opt := range opts {
		opt(&readOptions)
	}

	var problems []string
	if strings.TrimSpace(c.APIVersion) == "" {
		problems = append(problems, "apiVersion: is required")
	} else if c.APIVersion != APIVersion {
		problems = append(problems, "apiVersion: must equal "+APIVersion)
	}
	if strings.TrimSpace(c.Kind) == "" {
		problems = append(problems, "kind: is required")
	} else if c.Kind != Kind {
		problems = append(problems, "kind: must equal "+Kind)
	}
	if c.Port != nil && *c.Port < 0 {
		problems = append(problems, "port: must be greater than or equal to 0")
	}
	if c.LogMaxSizeBytes != nil && *c.LogMaxSizeBytes < 0 {
		problems = append(problems, "log_max_size_bytes: must be greater than or equal to 0")
	}
	if c.LogMaxBackups != nil && *c.LogMaxBackups < 0 {
		problems = append(problems, "log_max_backups: must be greater than or equal to 0")
	}
	if strings.ContainsAny(c.InstanceName, "\r\n") {
		problems = append(problems, "instance_name: must be a single line")
	}
	if strings.ContainsAny(c.APIToken, "\r\n") {
		problems = append(problems, "api_token: must be a single line")
	}
	problems = append(problems, c.Notifications.Validate()...)
	problems = append(problems, dashboardAccessProblems(c.DashboardAccess)...)
	problems = append(problems, c.Client.Validate()...)
	if c.Update.CheckIntervalHours < 0 {
		problems = append(problems, "update.check_interval_hours: must be a positive integer")
	}
	if c.Update.MaxDeferralHours < 0 {
		problems = append(problems, "update.max_deferral_hours: must be a positive integer")
	}
	problems = append(problems, c.Auth.validate("auth")...)

	if c.Global.MaxConcurrentAgents <= 0 {
		problems = append(problems, "global.max_concurrent_agents: must be a positive integer")
	}
	problems = append(problems, c.Global.RateWindowPacing.Validate("global.rate_window_pacing")...)
	if err := c.Global.Agents.ValidateDefaults(); err != nil {
		problems = append(problems, err.Error())
	}
	if !validSchedulingMode(c.Global.Scheduling) {
		problems = append(problems, "global.scheduling: must be one of "+strings.Join(schedulingModes, ", "))
	}
	problems = append(problems, agentPoolProblems(c.Global.AgentPools)...)
	if c.Global.ActiveHours != nil {
		problems = append(problems, c.Global.ActiveHours.Validate("global.active_hours")...)
	}
	problems = append(problems, c.Global.Identity.Validate("global.identity")...)
	problems = append(problems, startupErrors(c.Global.Startup, "global.startup")...)
	problems = append(problems, memoryProblems(c.Global.Memory, "global.memory")...)
	problems = append(problems, ioPressureProblems(c.Global.IO, "global.io")...)
	problems = append(problems, cpuPressureProblems(c.Global.CPU, "global.cpu")...)

	if c.Projects == nil {
		problems = append(problems, "projects: is required")
	}
	for index, project := range c.Projects {
		prefix := fmt.Sprintf("projects[%d]", index)
		if strings.TrimSpace(project.ID) == "" {
			problems = append(problems, prefix+".id: must not be blank")
		}
		if strings.TrimSpace(project.Workflow) == "" {
			problems = append(problems, prefix+".workflow: must not be blank")
		} else if strings.TrimSpace(project.WorkflowRef) == "" {
			problems = append(problems, plainWorkflowPathErrors(project.Workflow, prefix+".workflow", readOptions, wantFile)...)
		}
		if strings.ContainsAny(project.WorkflowRef, "\r\n") {
			problems = append(problems, prefix+".workflow_ref: must be a single line")
		}
		if strings.TrimSpace(project.Color) != "" {
			if _, ok := projectcolor.Normalize(project.Color); !ok {
				problems = append(problems, prefix+".color: must be an opaque CSS hex color like #1192e8")
			}
		}
		problems = append(problems, projectPathErrors(project.Workdir, prefix+".workdir", readOptions, wantDirectory)...)
		if project.Weight <= 0 {
			problems = append(problems, prefix+".weight: must be a positive integer")
		}
		if project.CredentialRef != "" && strings.TrimSpace(project.CredentialRef) == "" {
			problems = append(problems, prefix+".credential_ref: must not be blank")
		}
		problems = append(problems, pauseProjectProblems(project, prefix)...)
		if project.ActiveHours != nil {
			problems = append(problems, project.ActiveHours.Validate(prefix+".active_hours")...)
		}
		if value := strings.TrimSpace(project.ActiveHoursOverrideUntil); value != "" {
			if _, err := time.Parse(time.RFC3339, value); err != nil {
				problems = append(problems, prefix+".active_hours_override_until: must be an RFC 3339 timestamp")
			}
		}
		problems = append(problems, project.Authorization.Validate(prefix+".authorization")...)
		if project.Memory.MaxAgentRSSBytes != nil && *project.Memory.MaxAgentRSSBytes <= 0 {
			problems = append(problems, prefix+".memory.max_agent_rss_bytes: must be a positive integer")
		}
	}
	problems = append(problems, duplicateProjectIDErrorsFromProjects(c.Projects)...)
	problems = append(problems, projectAgentPoolProblems(c.Global.AgentPools, c.Projects)...)

	if len(problems) > 0 {
		return ValidationError{Path: c.Path, Problems: problems}
	}

	return nil
}

func pauseProjectProblems(project Project, prefix string) []string {
	var problems []string
	values := []struct {
		field string
		value string
	}{
		{field: "paused_reason", value: project.PausedReason},
		{field: "paused_at", value: project.PausedAt},
		{field: "paused_until_issue", value: project.PausedUntilIssue},
		{field: "paused_until", value: project.PausedUntil},
	}
	for _, item := range values {
		if item.value != "" && strings.TrimSpace(item.value) == "" {
			problems = append(problems, prefix+"."+item.field+": must not be blank")
		}
	}
	for _, item := range []struct {
		field string
		value string
	}{
		{field: "paused_at", value: project.PausedAt},
		{field: "paused_until", value: project.PausedUntil},
	} {
		value := strings.TrimSpace(item.value)
		if value == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			problems = append(problems, prefix+"."+item.field+": must be an RFC 3339 timestamp")
		}
	}
	if strings.TrimSpace(project.PausedUntilIssue) != "" && strings.TrimSpace(project.PausedUntil) != "" {
		problems = append(problems, prefix+".paused_until_issue and "+prefix+".paused_until: must not both be set")
	}
	return problems
}

func (e MissingFileError) Error() string {
	return fmt.Sprintf("read global config %s: %v", e.Path, e.Err)
}

func (e MissingFileError) Unwrap() error {
	return e.Err
}

func (e ParseError) Error() string {
	return fmt.Sprintf("parse global config %s: %v", e.Path, e.Err)
}

func (e ParseError) Unwrap() error {
	return e.Err
}

func (e ValidationError) Error() string {
	if e.Path == "" {
		return "invalid global config: " + strings.Join(e.Problems, "; ")
	}
	return "invalid global config at " + e.Path + ": " + strings.Join(e.Problems, "; ")
}

func defaultOptions() options {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}

	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}

	return options{
		home:       home,
		relativeTo: cwd,
	}
}

func defaultPathOptions() pathOptions {
	return pathOptions{
		config:        defaultOptions(),
		lookupEnv:     os.Getenv,
		userConfigDir: os.UserConfigDir,
		stat:          os.Stat,
	}
}

func resolvePath(configPath string, opts pathOptions) (PathResolution, error) {
	opts = normalizePathOptions(opts)

	if strings.TrimSpace(configPath) != "" {
		return pathResolution(configPath, PathRuleFlag, opts.config)
	}
	if envPath, rule := lookupPathEnv(opts.lookupEnv, pathEnvCandidates{
		{name: string(PathRuleEnvConfig), rule: PathRuleEnvConfig},
	}); envPath != "" {
		return pathResolution(envPath, rule, opts.config)
	}
	if configHome, rule := lookupPathEnv(opts.lookupEnv, pathEnvCandidates{
		{name: string(PathRuleEnvHome), rule: PathRuleEnvHome},
	}); configHome != "" {
		return configHomeResolution(configHome, rule, opts.config)
	}
	if envPath, rule := lookupPathEnv(opts.lookupEnv, pathEnvCandidates{
		{name: string(PathRuleDeprecatedEnvConfig), rule: PathRuleDeprecatedEnvConfig},
	}); envPath != "" {
		return pathResolution(envPath, rule, opts.config)
	}
	if configHome, rule := lookupPathEnv(opts.lookupEnv, pathEnvCandidates{
		{name: string(PathRuleDeprecatedEnvHome), rule: PathRuleDeprecatedEnvHome},
	}); configHome != "" {
		return configHomeResolution(configHome, rule, opts.config)
	}

	nativePath, nativeErr := userConfigPath(opts)
	legacyPath, legacyErr := legacyConfigPath(opts.config)
	switch {
	case nativeErr == nil && existingConfigFile(nativePath, opts):
		return PathResolution{Path: nativePath, Rule: PathRuleUserConfigDir}, nil
	case legacyErr == nil && existingConfigFile(legacyPath, opts):
		return PathResolution{Path: legacyPath, Rule: PathRuleLegacyHome}, nil
	case nativeErr == nil:
		return PathResolution{Path: nativePath, Rule: PathRuleUserConfigDir}, nil
	case legacyErr == nil:
		return PathResolution{Path: legacyPath, Rule: PathRuleLegacyHome}, nil
	default:
		return PathResolution{}, nativeErr
	}
}

type pathEnvCandidate struct {
	name string
	rule PathRule
}

type pathEnvCandidates []pathEnvCandidate

func lookupPathEnv(lookupEnv func(string) string, candidates pathEnvCandidates) (string, PathRule) {
	for _, candidate := range candidates {
		if value := strings.TrimSpace(lookupEnv(candidate.name)); value != "" {
			return value, candidate.rule
		}
	}
	return "", ""
}

func normalizePathOptions(opts pathOptions) pathOptions {
	if opts.config.home == "" && opts.config.relativeTo == "" {
		opts.config = defaultOptions()
	}
	if opts.lookupEnv == nil {
		opts.lookupEnv = os.Getenv
	}
	if opts.userConfigDir == nil {
		opts.userConfigDir = os.UserConfigDir
	}
	if opts.stat == nil {
		opts.stat = os.Stat
	}
	return opts
}

func pathResolution(path string, rule PathRule, opts options) (PathResolution, error) {
	expanded, err := expandPath(strings.TrimSpace(path), opts)
	if err != nil {
		return PathResolution{}, err
	}
	return PathResolution{Path: expanded, Rule: rule}, nil
}

func configHomeResolution(configHome string, rule PathRule, opts options) (PathResolution, error) {
	expanded, err := expandPath(configHome, opts)
	if err != nil {
		return PathResolution{}, err
	}
	return PathResolution{Path: filepath.Join(expanded, "global.yaml"), Rule: rule}, nil
}

func userConfigPath(opts pathOptions) (string, error) {
	dir, err := opts.userConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(dir, "detent", "global.yaml"), nil
}

func legacyConfigPath(opts options) (string, error) {
	if opts.home == "" {
		return "", errors.New("home directory is not available")
	}
	return filepath.Join(opts.home, ".detent", "global.yaml"), nil
}

func existingConfigFile(path string, opts pathOptions) bool {
	info, err := opts.stat(path)
	return err == nil && info.Mode().IsRegular()
}

func defaultConfig(path string) Config {
	return Config{
		Path:       path,
		APIVersion: APIVersion,
		Kind:       Kind,
		Global:     defaultSettings(),
		Projects:   []Project{},
	}
}

func defaultSettings() Settings {
	return Settings{
		MaxConcurrentAgents: 8,
		RateWindowPacing:    workflowconfig.DefaultRateWindowPacing(),
		Scheduling:          SchedulingWeighted,
		FairShare: map[string]any{
			"half_life": "1h",
		},
		Startup: map[string]any{
			"jitter_seconds":       10,
			"max_spawn_per_second": 2,
		},
		Memory: Memory{
			MaxAgentRSSBytes:           DefaultMaxAgentRSSBytes,
			PressureSomeAvg60Threshold: DefaultMemoryPressureSomeAvg60Threshold,
			PollIntervalMS:             DefaultMemoryPollIntervalMS,
		},
		IO: IO{
			PressureFullAvg10Threshold: DefaultIOPressureFullAvg10Threshold,
			PollIntervalMS:             DefaultIOPressurePollIntervalMS,
		},
		CPU: CPU{
			PressureSomeAvg10Threshold: DefaultCPUPressureSomeAvg10Threshold,
			PollIntervalMS:             DefaultCPUPressurePollIntervalMS,
		},
	}
}

func validateRaw(attrs map[string]any, opts options) []string {
	var problems []string

	problems = append(problems, requiredErrors(attrs, []string{"apiVersion", "kind", "global", "projects"})...)
	problems = append(problems, versionErrors(attrs["apiVersion"])...)
	problems = append(problems, kindErrors(attrs["kind"])...)
	problems = append(problems, optionalStringTypeError(attrs, "env")...)
	problems = append(problems, optionalStringTypeError(attrs, "log_level")...)
	problems = append(problems, optionalNonNegativeIntegerError(attrs["log_max_size_bytes"], "log_max_size_bytes")...)
	problems = append(problems, optionalNonNegativeIntegerError(attrs["log_max_backups"], "log_max_backups")...)
	problems = append(problems, optionalStringTypeError(attrs, "github_token")...)
	problems = append(problems, optionalStringTypeError(attrs, "api_token")...)
	problems = append(problems, optionalSingleLineStringError(attrs, "api_token")...)
	if _, err := optionalBool(attrs["trust_loopback_peer_read"], "trust_loopback_peer_read"); err != nil {
		problems = append(problems, err.Error())
	}
	problems = append(problems, dashboardAccessRawErrors(attrs["dashboard_access"])...)
	problems = append(problems, hubClientRawErrors(attrs["client"])...)
	problems = append(problems, opsRawErrors(attrs["ops"])...)
	problems = append(problems, optionalStringTypeError(attrs, "instance_name")...)
	problems = append(problems, optionalSingleLineStringError(attrs, "instance_name")...)
	problems = append(problems, notificationsRawErrors(attrs["notifications"])...)
	problems = append(problems, optionalNonNegativeIntegerError(attrs["port"], "port")...)
	problems = append(problems, updateErrors(attrs["update"])...)
	problems = append(problems, authErrors(attrs["auth"])...)
	problems = append(problems, globalErrors(attrs["global"])...)
	problems = append(problems, projectsErrors(attrs["projects"], opts)...)

	return problems
}

func updateErrors(value any) []string {
	if value == nil {
		return nil
	}
	update, ok := value.(map[string]any)
	if !ok {
		return []string{"update: must be a mapping"}
	}
	var problems []string
	for _, field := range []string{"auto_check_enabled", "auto_apply_enabled"} {
		if configured, ok := update[field]; ok {
			if _, ok := configured.(bool); !ok {
				problems = append(problems, "update."+field+": must be a boolean")
			}
		}
	}
	if interval, ok := update["check_interval_hours"]; ok && !positiveInteger(interval) {
		problems = append(problems, "update.check_interval_hours: must be a positive integer")
	}
	if interval, ok := update["max_deferral_hours"]; ok && !positiveInteger(interval) {
		problems = append(problems, "update.max_deferral_hours: must be a positive integer")
	}
	return problems
}

func requiredErrors(attrs map[string]any, fields []string) []string {
	var problems []string
	for _, field := range fields {
		value, ok := attrs[field]
		if !ok || value == nil {
			problems = append(problems, field+": is required")
		}
	}
	return problems
}

func versionErrors(value any) []string {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok || text != APIVersion {
		return []string{"apiVersion: must equal " + APIVersion}
	}
	return nil
}

func kindErrors(value any) []string {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok || text != Kind {
		return []string{"kind: must equal " + Kind}
	}
	return nil
}

func globalErrors(value any) []string {
	if value == nil {
		return nil
	}

	global, ok := value.(map[string]any)
	if !ok {
		return []string{"global: must be a mapping"}
	}

	var problems []string
	problems = append(problems, prefixErrors(requiredErrors(global, []string{"max_concurrent_agents", "scheduling"}), "global")...)
	problems = append(problems, positiveIntegerError(global["max_concurrent_agents"], "global.max_concurrent_agents")...)
	problems = append(problems, rateWindowPacingErrors(global["rate_window_pacing"], "global.rate_window_pacing")...)
	problems = append(problems, schedulingErrors(global["scheduling"], "global.scheduling")...)
	problems = append(problems, agentPoolsErrors(global["agent_pools"])...)
	problems = append(problems, activeHoursErrors(global["active_hours"], "global.active_hours")...)
	problems = append(problems, optionalMapErrors(global, "identity")...)
	problems = append(problems, identityErrors(global["identity"], "global.identity")...)
	problems = append(problems, knowledgeErrors(global["knowledge"], "global.knowledge")...)
	problems = append(problems, optionalMapErrors(global, "fair_share")...)
	problems = append(problems, optionalMapErrors(global, "startup")...)
	problems = append(problems, memoryErrors(global["memory"], "global.memory", false)...)
	problems = append(problems, pressureErrors(global["io"], "global.io", "pressure_full_avg10_threshold")...)
	problems = append(problems, pressureErrors(global["cpu"], "global.cpu", "pressure_some_avg10_threshold")...)

	if startup, ok := global["startup"].(map[string]any); ok {
		problems = append(problems, startupErrors(startup, "global.startup")...)
	}

	return problems
}

func schedulingErrors(value any, field string) []string {
	if value == nil {
		return nil
	}
	mode, ok := value.(string)
	if ok && validSchedulingMode(mode) {
		return nil
	}
	return []string{field + ": must be one of " + strings.Join(schedulingModes, ", ")}
}

func rateWindowPacingErrors(value any, field string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(map[string]any); !ok {
		return []string{field + ": must be a mapping"}
	}
	var pacing workflowconfig.RateWindowPacing
	if err := decodeYAMLValue(value, &pacing); err != nil {
		return []string{field + ": " + err.Error()}
	}
	return pacing.Validate(field)
}

func validSchedulingMode(mode string) bool {
	return slices.Contains(schedulingModes, mode)
}

func agentPoolsErrors(value any) []string {
	if value == nil {
		return nil
	}
	pools, ok := value.([]any)
	if !ok {
		return []string{"global.agent_pools: must be a list"}
	}

	var problems []string
	names := make(map[string]int, len(pools))
	for index, value := range pools {
		prefix := fmt.Sprintf("global.agent_pools[%d]", index)
		pool, ok := value.(map[string]any)
		if !ok {
			problems = append(problems, prefix+": must be a mapping")
			continue
		}
		problems = append(problems, prefixErrors(requiredErrors(pool, []string{"name", "max_concurrent_agents"}), prefix)...)
		problems = append(problems, stringErrors(pool, "name", prefix)...)
		problems = append(problems, positiveIntegerError(pool["max_concurrent_agents"], prefix+".max_concurrent_agents")...)
		problems = append(problems, positiveIntegerError(pool["burst_to"], prefix+".burst_to")...)
		capacity, capacityOK := pool["max_concurrent_agents"].(int)
		burstTo, burstOK := pool["burst_to"].(int)
		if capacityOK && burstOK && burstTo < capacity {
			problems = append(problems, prefix+".burst_to: must be greater than or equal to max_concurrent_agents")
		}
		if scheduling, configured := pool["scheduling"]; configured {
			problems = append(problems, schedulingErrors(scheduling, prefix+".scheduling")...)
		}

		name, ok := pool["name"].(string)
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if name == DefaultAgentPoolName {
			problems = append(problems, prefix+".name: "+DefaultAgentPoolName+" is reserved")
		}
		if previous, exists := names[name]; exists {
			problems = append(problems, fmt.Sprintf("%s.name: duplicates global.agent_pools[%d].name %q", prefix, previous, name))
			continue
		}
		names[name] = index
	}
	return problems
}

func agentPoolProblems(pools []AgentPool) []string {
	var problems []string
	names := make(map[string]int, len(pools))
	for index, pool := range pools {
		prefix := fmt.Sprintf("global.agent_pools[%d]", index)
		name := strings.TrimSpace(pool.Name)
		if name == "" {
			problems = append(problems, prefix+".name: must not be blank")
		} else if name == DefaultAgentPoolName {
			problems = append(problems, prefix+".name: "+DefaultAgentPoolName+" is reserved")
		} else if previous, exists := names[name]; exists {
			problems = append(problems, fmt.Sprintf("%s.name: duplicates global.agent_pools[%d].name %q", prefix, previous, name))
		} else {
			names[name] = index
		}
		if pool.MaxConcurrentAgents <= 0 {
			problems = append(problems, prefix+".max_concurrent_agents: must be a positive integer")
		}
		if pool.BurstTo < 0 {
			problems = append(problems, prefix+".burst_to: must be a positive integer")
		} else if pool.BurstTo > 0 && pool.BurstTo < pool.MaxConcurrentAgents {
			problems = append(problems, prefix+".burst_to: must be greater than or equal to max_concurrent_agents")
		}
		if pool.Scheduling != "" && !validSchedulingMode(pool.Scheduling) {
			problems = append(problems, prefix+".scheduling: must be one of "+strings.Join(schedulingModes, ", "))
		}
	}
	return problems
}

func projectAgentPoolProblems(pools []AgentPool, projects []Project) []string {
	available := make(map[string]struct{}, len(pools)+1)
	available[DefaultAgentPoolName] = struct{}{}
	for _, pool := range pools {
		available[strings.TrimSpace(pool.Name)] = struct{}{}
	}

	var problems []string
	for index, project := range projects {
		pool := strings.TrimSpace(project.Pool)
		if pool == "" {
			pool = DefaultAgentPoolName
		}
		if _, ok := available[pool]; ok {
			continue
		}
		problems = append(problems, fmt.Sprintf(
			"projects[%d].pool: project %q references undefined agent pool %q",
			index,
			strings.TrimSpace(project.ID),
			pool,
		))
	}
	return problems
}

func optionalMapErrors(attrs map[string]any, field string) []string {
	value, ok := attrs[field]
	if !ok {
		return nil
	}
	if _, ok := value.(map[string]any); ok {
		return nil
	}
	return []string{"global." + field + ": must be a mapping"}
}

func startupErrors(startup map[string]any, prefix string) []string {
	if startup == nil {
		return nil
	}

	var problems []string
	if value, ok := startup["jitter_seconds"]; ok && !nonNegativeInteger(value) {
		problems = append(problems, prefix+".jitter_seconds: must be an integer greater than or equal to 0")
	}
	if value, ok := startup["max_spawn_per_second"]; ok && !positiveInteger(value) {
		problems = append(problems, prefix+".max_spawn_per_second: must be a positive integer")
	}
	if value, ok := startup["max_concurrent_starts"]; ok && !positiveInteger(value) {
		problems = append(problems, prefix+".max_concurrent_starts: must be a positive integer")
	}
	return problems
}

func memoryErrors(value any, prefix string, projectOverride bool) []string {
	if value == nil {
		return nil
	}
	memory, ok := value.(map[string]any)
	if !ok {
		return []string{prefix + ": must be a mapping"}
	}
	var problems []string
	if value, ok := memory["max_agent_rss_bytes"]; ok && !positiveInteger(value) {
		problems = append(problems, prefix+".max_agent_rss_bytes: must be a positive integer")
	}
	if projectOverride {
		return problems
	}
	if value, ok := memory["pressure_some_avg60_threshold"]; ok && !positiveNumber(value) {
		problems = append(problems, prefix+".pressure_some_avg60_threshold: must be a positive number")
	}
	if value, ok := memory["poll_interval_ms"]; ok && !positiveInteger(value) {
		problems = append(problems, prefix+".poll_interval_ms: must be a positive integer")
	}
	return problems
}

func memoryProblems(memory Memory, prefix string) []string {
	var problems []string
	if memory.MaxAgentRSSBytes <= 0 {
		problems = append(problems, prefix+".max_agent_rss_bytes: must be a positive integer")
	}
	if memory.PressureSomeAvg60Threshold <= 0 {
		problems = append(problems, prefix+".pressure_some_avg60_threshold: must be a positive number")
	}
	if memory.PollIntervalMS <= 0 {
		problems = append(problems, prefix+".poll_interval_ms: must be a positive integer")
	}
	return problems
}

func pressureErrors(value any, prefix string, thresholdKey string) []string {
	if value == nil {
		return nil
	}
	pressure, ok := value.(map[string]any)
	if !ok {
		return []string{prefix + ": must be a mapping"}
	}
	var problems []string
	if value, ok := pressure[thresholdKey]; ok && !positiveNumber(value) {
		problems = append(problems, prefix+"."+thresholdKey+": must be a positive number")
	}
	if value, ok := pressure["poll_interval_ms"]; ok && !positiveInteger(value) {
		problems = append(problems, prefix+".poll_interval_ms: must be a positive integer")
	}
	if value, ok := pressure["degraded_max_concurrent_agents"]; ok {
		problems = append(problems, optionalNonNegativeIntegerError(value, prefix+".degraded_max_concurrent_agents")...)
	}
	return problems
}

func ioPressureProblems(pressure IO, prefix string) []string {
	var problems []string
	if pressure.PressureFullAvg10Threshold <= 0 {
		problems = append(problems, prefix+".pressure_full_avg10_threshold: must be a positive number")
	}
	if pressure.PollIntervalMS <= 0 {
		problems = append(problems, prefix+".poll_interval_ms: must be a positive integer")
	}
	if pressure.DegradedMaxConcurrentAgents < 0 {
		problems = append(problems, prefix+".degraded_max_concurrent_agents: must be a non-negative integer")
	}
	return problems
}

func cpuPressureProblems(pressure CPU, prefix string) []string {
	var problems []string
	if pressure.PressureSomeAvg10Threshold <= 0 {
		problems = append(problems, prefix+".pressure_some_avg10_threshold: must be a positive number")
	}
	if pressure.PollIntervalMS <= 0 {
		problems = append(problems, prefix+".poll_interval_ms: must be a positive integer")
	}
	if pressure.DegradedMaxConcurrentAgents < 0 {
		problems = append(problems, prefix+".degraded_max_concurrent_agents: must be a non-negative integer")
	}
	return problems
}

func projectsErrors(value any, opts options) []string {
	if value == nil {
		return nil
	}

	projects, ok := value.([]any)
	if !ok {
		return []string{"projects: must be a list"}
	}

	var problems []string
	for index, project := range projects {
		problems = append(problems, projectErrors(project, index, opts)...)
	}
	problems = append(problems, duplicateProjectIDErrors(projects)...)
	return problems
}

func projectErrors(value any, index int, opts options) []string {
	project, ok := value.(map[string]any)
	prefix := fmt.Sprintf("projects[%d]", index)
	if !ok {
		return []string{prefix + ": must be a mapping"}
	}

	var problems []string
	problems = append(problems, prefixErrors(requiredErrors(project, []string{"id", "workflow", "workdir", "weight", "priority"}), prefix)...)
	problems = append(problems, stringErrors(project, "id", prefix)...)
	problems = append(problems, stringErrors(project, "pool", prefix)...)
	problems = append(problems, stringErrors(project, "workflow_ref", prefix)...)
	problems = append(problems, singleLineStringErrors(project, "workflow_ref", prefix)...)
	problems = append(problems, projectColorErrors(project, prefix)...)
	if strings.TrimSpace(projectStringValue(project, "workflow_ref")) == "" {
		problems = append(problems, plainWorkflowErrors(project, "workflow", prefix, opts, wantFile)...)
	} else {
		problems = append(problems, stringErrors(project, "workflow", prefix)...)
	}
	problems = append(problems, pathErrors(project, "workdir", prefix, opts, wantDirectory)...)
	problems = append(problems, positiveIntegerError(project["weight"], prefix+".weight")...)
	problems = append(problems, integerError(project["priority"], prefix+".priority")...)
	problems = append(problems, pausedErrors(project, prefix)...)
	problems = append(problems, pauseMetadataErrors(project, prefix)...)
	problems = append(problems, activeHoursErrors(project["active_hours"], prefix+".active_hours")...)
	problems = append(problems, pauseTimestampErrors(project, "active_hours_override_until", prefix)...)
	problems = append(problems, knowledgeErrors(project["knowledge"], prefix+".knowledge")...)
	problems = append(problems, credentialRefErrors(project, prefix)...)
	problems = append(problems, authorizationErrors(project["authorization"], prefix+".authorization")...)
	problems = append(problems, intakeErrors(project["intake"], prefix+".intake")...)
	problems = append(problems, memoryErrors(project["memory"], prefix+".memory", true)...)

	return problems
}

func identityErrors(value any, prefix string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(map[string]any); !ok {
		return nil
	}

	var identity Identity
	if err := decodeYAMLValue(value, &identity); err != nil {
		return []string{prefix + ": " + err.Error()}
	}
	identity.Normalize()
	return identity.Validate(prefix)
}

func knowledgeErrors(value any, prefix string) []string {
	if value == nil {
		return nil
	}
	knowledge, ok := value.(map[string]any)
	if !ok {
		return []string{prefix + ": must be a mapping"}
	}

	var problems []string
	if enabled, ok := knowledge["enabled"]; ok {
		if _, ok := enabled.(bool); !ok {
			problems = append(problems, prefix+".enabled: must be a boolean")
		}
	}
	if maxBytes, ok := knowledge["max_bytes"]; ok && !positiveInteger(maxBytes) {
		problems = append(problems, prefix+".max_bytes: must be a positive integer")
	}
	if sources, ok := knowledge["sources"]; ok {
		problems = append(problems, knowledgeSourceErrors(sources, prefix+".sources")...)
	}
	return problems
}

func knowledgeSourceErrors(value any, prefix string) []string {
	sources, ok := value.([]any)
	if !ok {
		return []string{prefix + ": must be a list"}
	}

	var problems []string
	for index, item := range sources {
		source, ok := item.(map[string]any)
		sourcePrefix := fmt.Sprintf("%s[%d]", prefix, index)
		if !ok {
			problems = append(problems, sourcePrefix+": must be a mapping")
			continue
		}
		problems = append(problems, prefixErrors(requiredErrors(source, []string{"path"}), sourcePrefix)...)
		problems = append(problems, stringErrors(source, "name", sourcePrefix)...)
		problems = append(problems, singleLineStringErrors(source, "name", sourcePrefix)...)
		problems = append(problems, stringErrors(source, "path", sourcePrefix)...)
		problems = append(problems, singleLineStringErrors(source, "path", sourcePrefix)...)
	}
	return problems
}

func authorizationErrors(value any, prefix string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(map[string]any); !ok {
		return []string{prefix + ": must be a mapping"}
	}

	var authorization selector.Selector
	if err := decodeYAMLValue(value, &authorization); err != nil {
		return []string{prefix + ": " + err.Error()}
	}
	authorization.Normalize()
	return authorization.Validate(prefix)
}

func prefixErrors(errors []string, prefix string) []string {
	out := make([]string, 0, len(errors))
	for _, err := range errors {
		out = append(out, prefix+"."+err)
	}
	return out
}

func stringErrors(attrs map[string]any, field string, prefix string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return []string{prefix + "." + field + ": must be a string"}
	}
	if strings.TrimSpace(text) == "" {
		return []string{prefix + "." + field + ": must not be blank"}
	}
	return nil
}

func singleLineStringErrors(attrs map[string]any, field string, prefix string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return nil
	}
	if strings.ContainsAny(text, "\r\n") {
		return []string{prefix + "." + field + ": must be a single line"}
	}
	return nil
}

func projectStringValue(attrs map[string]any, field string) string {
	value, ok := attrs[field]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

type pathExpectation int

const (
	wantFile pathExpectation = iota
	wantDirectory
)

func pathErrors(attrs map[string]any, field string, prefix string, opts options, expected pathExpectation) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return []string{prefix + "." + field + ": must be a string"}
	}
	return projectPathErrors(text, prefix+"."+field, opts, expected)
}

func plainWorkflowErrors(attrs map[string]any, field string, prefix string, opts options, expected pathExpectation) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return []string{prefix + "." + field + ": must be a string"}
	}
	return plainWorkflowPathErrors(text, prefix+"."+field, opts, expected)
}

func plainWorkflowPathErrors(path string, field string, opts options, expected pathExpectation) []string {
	if strings.TrimSpace(path) == "" {
		return []string{field + ": must not be blank"}
	}
	if relativeWorkflowPathLiteral(path) {
		return []string{field + ": " + plainWorkflowPathRequirement}
	}
	if opts.allowMissingWorkflowFiles {
		return nil
	}
	return projectPathErrors(path, field, opts, expected)
}

func projectPathErrors(path string, field string, opts options, expected pathExpectation) []string {
	if strings.TrimSpace(path) == "" {
		return []string{field + ": must not be blank"}
	}

	expanded, err := expandPath(path, opts)
	if err != nil {
		return []string{field + ": path does not exist"}
	}

	info, err := os.Stat(expanded) // #nosec G703 -- validation intentionally inspects the operator-configured project path after expansion.
	if err != nil {
		return []string{field + ": path does not exist"}
	}
	if expected == wantFile && !info.Mode().IsRegular() {
		return []string{field + ": path does not exist"}
	}
	if expected == wantDirectory && !info.IsDir() {
		return []string{field + ": path does not exist"}
	}
	return nil
}

func relativeWorkflowPathLiteral(path string) bool {
	path = strings.TrimSpace(path)
	return !filepath.IsAbs(path) && !homePathLiteral(path)
}

func homePathLiteral(path string) bool {
	return path == "~" || strings.HasPrefix(path, "~/")
}

func positiveIntegerError(value any, field string) []string {
	if value == nil {
		return nil
	}
	if positiveInteger(value) {
		return nil
	}
	return []string{field + ": must be a positive integer"}
}

func integerError(value any, field string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(int); ok {
		return nil
	}
	return []string{field + ": must be an integer"}
}

func positiveInteger(value any) bool {
	number, ok := value.(int)
	return ok && number > 0
}

func positiveNumber(value any) bool {
	switch number := value.(type) {
	case int:
		return number > 0
	case float64:
		return number > 0
	default:
		return false
	}
}

func nonNegativeInteger(value any) bool {
	number, ok := value.(int)
	return ok && number >= 0
}

func optionalStringTypeError(attrs map[string]any, field string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}
	if _, ok := value.(string); ok {
		return nil
	}
	return []string{field + ": must be a string"}
}

func optionalSingleLineStringError(attrs map[string]any, field string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return nil
	}
	if strings.ContainsAny(text, "\r\n") {
		return []string{field + ": must be a single line"}
	}
	return nil
}

func optionalNonNegativeIntegerError(value any, field string) []string {
	if value == nil {
		return nil
	}
	if nonNegativeInteger(value) {
		return nil
	}
	return []string{field + ": must be an integer greater than or equal to 0"}
}

func pausedErrors(attrs map[string]any, prefix string) []string {
	value, ok := attrs["paused"]
	if !ok {
		return nil
	}
	if _, ok := value.(bool); ok {
		return nil
	}
	return []string{prefix + ".paused: must be a boolean"}
}

func pauseMetadataErrors(attrs map[string]any, prefix string) []string {
	var problems []string
	for _, field := range []string{"paused_reason", "paused_until_issue"} {
		problems = append(problems, optionalPauseStringErrors(attrs, field, prefix)...)
	}
	for _, field := range []string{"paused_at", "paused_until"} {
		problems = append(problems, pauseTimestampErrors(attrs, field, prefix)...)
	}
	if pauseString(attrs["paused_until_issue"]) != "" && pauseTimestampString(attrs["paused_until"]) != "" {
		problems = append(problems, prefix+".paused_until_issue and "+prefix+".paused_until: must not both be set")
	}
	return problems
}

func optionalPauseStringErrors(attrs map[string]any, field string, prefix string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return []string{prefix + "." + field + ": must be a string"}
	}
	if strings.TrimSpace(text) == "" {
		return []string{prefix + "." + field + ": must not be blank"}
	}
	return nil
}

func pauseTimestampErrors(attrs map[string]any, field string, prefix string) []string {
	value, ok := attrs[field]
	if !ok || value == nil {
		return nil
	}
	text := pauseTimestampString(value)
	if text == "" {
		if _, ok := value.(string); ok {
			return []string{prefix + "." + field + ": must not be blank"}
		}
		return []string{prefix + "." + field + ": must be an RFC 3339 timestamp"}
	}
	if _, err := time.Parse(time.RFC3339, text); err != nil {
		return []string{prefix + "." + field + ": must be an RFC 3339 timestamp"}
	}
	return nil
}

func pauseString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func pauseTimestampString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339)
	default:
		return ""
	}
}

func credentialRefErrors(attrs map[string]any, prefix string) []string {
	value, ok := attrs["credential_ref"]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return []string{prefix + ".credential_ref: must be a string"}
	}
	if strings.TrimSpace(text) == "" {
		return []string{prefix + ".credential_ref: must not be blank"}
	}
	return nil
}

func projectColorErrors(attrs map[string]any, prefix string) []string {
	value, ok := attrs["color"]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return []string{prefix + ".color: must be a string"}
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}
	if _, ok := projectcolor.Normalize(text); !ok {
		return []string{prefix + ".color: must be an opaque CSS hex color like #1192e8"}
	}
	return nil
}

func duplicateProjectIDErrors(projects []any) []string {
	counts := make(map[string]int)
	for _, item := range projects {
		project, ok := item.(map[string]any)
		if !ok {
			continue
		}

		id, ok := project["id"].(string)
		if !ok {
			continue
		}

		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		counts[id]++
	}

	var problems []string
	for id, count := range counts {
		if count > 1 {
			problems = append(problems, "projects.id: duplicate id "+id)
		}
	}
	return problems
}

func duplicateProjectIDErrorsFromProjects(projects []Project) []string {
	counts := make(map[string]int)
	for _, project := range projects {
		id := strings.TrimSpace(project.ID)
		if id == "" {
			continue
		}
		counts[id]++
	}

	var problems []string
	for id, count := range counts {
		if count > 1 {
			problems = append(problems, "projects.id: duplicate id "+id)
		}
	}
	return problems
}

func build(attrs map[string]any, path string, opts options) (Config, error) {
	global, err := mapValue(attrs["global"], "global")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	projects, err := listValue(attrs["projects"], "projects")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	apiVersion, err := stringValue(attrs["apiVersion"], "apiVersion")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	kind, err := stringValue(attrs["kind"], "kind")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	env, err := optionalString(attrs["env"], "env")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	logLevel, err := optionalString(attrs["log_level"], "log_level")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	logMaxSizeBytes, err := optionalIntPointer(attrs["log_max_size_bytes"], "log_max_size_bytes")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	logMaxBackups, err := optionalIntPointer(attrs["log_max_backups"], "log_max_backups")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	githubToken, err := optionalString(attrs["github_token"], "github_token")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	apiToken, err := optionalString(attrs["api_token"], "api_token")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	trustLoopbackPeerRead, err := optionalBool(attrs["trust_loopback_peer_read"], "trust_loopback_peer_read")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	dashboardAccess, err := buildDashboardAccess(attrs["dashboard_access"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	client, err := buildHubClient(attrs["client"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	ops, err := buildOps(attrs["ops"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	instanceName, err := optionalString(attrs["instance_name"], "instance_name")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	notifications, err := buildNotifications(attrs["notifications"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	port, err := optionalIntPointer(attrs["port"], "port")
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	update, err := buildUpdate(attrs["update"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	auth, err := buildAuth(attrs["auth"])
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	settings, err := buildSettings(global, opts)
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}
	builtProjects, err := buildProjects(projects, opts)
	if err != nil {
		return Config{}, buildValidationError(path, err)
	}

	for index := range builtProjects {
		builtProjects[index].GlobalAgents = settings.Agents
		builtProjects[index].GlobalBudget = settings.Budget
	}
	return Config{
		Path:                  path,
		APIVersion:            apiVersion,
		Kind:                  kind,
		Env:                   env,
		LogLevel:              logLevel,
		LogMaxSizeBytes:       logMaxSizeBytes,
		LogMaxBackups:         logMaxBackups,
		GitHubToken:           githubToken,
		APIToken:              apiToken,
		TrustLoopbackPeerRead: trustLoopbackPeerRead,
		DashboardAccess:       dashboardAccess,
		Client:                client,
		Ops:                   ops,
		Port:                  port,
		InstanceName:          instanceName,
		Notifications:         notifications,
		Update:                update,
		Auth:                  auth,
		Global:                settings,
		Projects:              builtProjects,
	}, nil
}

func opsRawErrors(value any) []string {
	if value == nil {
		return nil
	}
	attrs, ok := value.(map[string]any)
	if !ok {
		return []string{"ops: must be a mapping"}
	}
	if _, err := optionalBool(attrs["tmux_window_status"], "ops.tmux_window_status"); err != nil {
		return []string{err.Error()}
	}
	return nil
}

func buildOps(value any) (Ops, error) {
	if value == nil {
		return Ops{}, nil
	}
	attrs, err := mapValue(value, "ops")
	if err != nil {
		return Ops{}, err
	}
	if _, configured := attrs["tmux_window_status"]; !configured {
		return Ops{}, nil
	}
	enabled, err := optionalBool(attrs["tmux_window_status"], "ops.tmux_window_status")
	if err != nil {
		return Ops{}, err
	}
	return Ops{TmuxWindowStatus: &enabled}, nil
}

func dashboardAccessRawErrors(value any) []string {
	if value == nil {
		return nil
	}
	attrs, ok := value.(map[string]any)
	if !ok {
		return []string{"dashboard_access: must be a mapping"}
	}
	var problems []string
	problems = append(problems, optionalStringTypeError(attrs, "mode")...)
	problems = append(problems, optionalStringTypeError(attrs, "token")...)
	problems = append(problems, optionalSingleLineStringError(attrs, "token")...)
	if allowWrite, configured := attrs["allow_write"]; configured {
		if _, ok := allowWrite.(bool); !ok {
			problems = append(problems, "dashboard_access.allow_write: must be a boolean")
		}
	}
	return problems
}

func buildDashboardAccess(value any) (DashboardAccess, error) {
	if value == nil {
		return DashboardAccess{}, nil
	}
	if _, err := mapValue(value, "dashboard_access"); err != nil {
		return DashboardAccess{}, err
	}
	var access DashboardAccess
	if err := decodeYAMLValue(value, &access); err != nil {
		return DashboardAccess{}, fmt.Errorf("dashboard_access: %w", err)
	}
	access.Mode = strings.ToLower(strings.TrimSpace(access.Mode))
	access.Token = strings.TrimSpace(access.Token)
	return access, nil
}

func dashboardAccessProblems(access DashboardAccess) []string {
	mode := strings.ToLower(strings.TrimSpace(access.Mode))
	token := strings.TrimSpace(access.Token)
	if mode == "" {
		if token != "" || access.AllowWrite {
			return []string{"dashboard_access.mode: must equal private_token when dashboard access settings are configured"}
		}
		return nil
	}
	if mode != DashboardAccessModePrivateToken {
		return []string{"dashboard_access.mode: must equal " + DashboardAccessModePrivateToken}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 {
		return []string{"dashboard_access.token: must be a 256-bit unpadded URL-safe base64 value"}
	}
	return nil
}

func buildUpdate(value any) (Update, error) {
	if value == nil {
		return Update{}, nil
	}
	if _, err := mapValue(value, "update"); err != nil {
		return Update{}, err
	}
	var update Update
	if err := decodeYAMLValue(value, &update); err != nil {
		return Update{}, fmt.Errorf("update: %w", err)
	}
	return update, nil
}

func buildSettings(attrs map[string]any, opts options) (Settings, error) {
	settings := defaultSettings()
	if attrs["agents"] != nil {
		if err := decodeYAMLValue(attrs["agents"], &settings.Agents); err != nil {
			return Settings{}, fmt.Errorf("global.agents: %w", err)
		}
		if err := settings.Agents.ValidateDefaults(); err != nil {
			return Settings{}, err
		}
	}
	if attrs["budget"] != nil {
		if err := decodeYAMLValue(attrs["budget"], &settings.Budget); err != nil {
			return Settings{}, fmt.Errorf("global.budget: %w", err)
		}
	}
	maxConcurrentAgents, err := intValue(attrs["max_concurrent_agents"], "global.max_concurrent_agents")
	if err != nil {
		return Settings{}, err
	}
	scheduling, err := stringValue(attrs["scheduling"], "global.scheduling")
	if err != nil {
		return Settings{}, err
	}
	fairShare, err := mergeMap(settings.FairShare, attrs["fair_share"], "global.fair_share")
	if err != nil {
		return Settings{}, err
	}
	startup, err := mergeMap(settings.Startup, attrs["startup"], "global.startup")
	if err != nil {
		return Settings{}, err
	}
	identity, err := buildIdentity(attrs["identity"], "global.identity")
	if err != nil {
		return Settings{}, err
	}
	knowledge, err := buildKnowledge(attrs["knowledge"], "global.knowledge", opts)
	if err != nil {
		return Settings{}, err
	}
	agentPools, err := buildAgentPools(attrs["agent_pools"])
	if err != nil {
		return Settings{}, err
	}
	var activeHours *activehours.Config
	if attrs["active_hours"] != nil {
		parsed, err := buildActiveHours(attrs["active_hours"], "global.active_hours")
		if err != nil {
			return Settings{}, err
		}
		activeHours = &parsed
	}
	rateWindowPacing := settings.RateWindowPacing
	if attrs["rate_window_pacing"] != nil {
		if err := decodeYAMLValue(attrs["rate_window_pacing"], &rateWindowPacing); err != nil {
			return Settings{}, fmt.Errorf("global.rate_window_pacing: %w", err)
		}
		rateWindowPacing = rateWindowPacing.Normalized()
	}
	memory := settings.Memory
	if attrs["memory"] != nil {
		if err := decodeYAMLValue(attrs["memory"], &memory); err != nil {
			return Settings{}, fmt.Errorf("global.memory: %w", err)
		}
	}
	ioPressure := settings.IO
	if attrs["io"] != nil {
		if err := decodeYAMLValue(attrs["io"], &ioPressure); err != nil {
			return Settings{}, fmt.Errorf("global.io: %w", err)
		}
	}
	cpuPressure := settings.CPU
	if attrs["cpu"] != nil {
		if err := decodeYAMLValue(attrs["cpu"], &cpuPressure); err != nil {
			return Settings{}, fmt.Errorf("global.cpu: %w", err)
		}
	}

	settings.MaxConcurrentAgents = maxConcurrentAgents
	settings.RateWindowPacing = rateWindowPacing
	settings.Scheduling = scheduling
	settings.AgentPools = agentPools
	settings.ActiveHours = activeHours
	settings.Identity = identity
	settings.Knowledge = knowledge
	settings.FairShare = fairShare
	settings.Startup = startup
	settings.Memory = memory
	settings.IO = ioPressure
	settings.CPU = cpuPressure
	return settings, nil
}

func buildAgentPools(value any) ([]AgentPool, error) {
	if value == nil {
		return nil, nil
	}
	items, err := listValue(value, "global.agent_pools")
	if err != nil {
		return nil, err
	}
	pools := make([]AgentPool, 0, len(items))
	for index, item := range items {
		prefix := fmt.Sprintf("global.agent_pools[%d]", index)
		pool, err := mapValue(item, prefix)
		if err != nil {
			return nil, err
		}
		name, err := stringValue(pool["name"], prefix+".name")
		if err != nil {
			return nil, err
		}
		capacity, err := intValue(pool["max_concurrent_agents"], prefix+".max_concurrent_agents")
		if err != nil {
			return nil, err
		}
		burstTo := 0
		if value, ok := pool["burst_to"]; ok {
			burstTo, err = intValue(value, prefix+".burst_to")
			if err != nil {
				return nil, err
			}
		}
		scheduling, err := optionalString(pool["scheduling"], prefix+".scheduling")
		if err != nil {
			return nil, err
		}
		pools = append(pools, AgentPool{
			Name:                strings.TrimSpace(name),
			MaxConcurrentAgents: capacity,
			BurstTo:             burstTo,
			Scheduling:          scheduling,
		})
	}
	return pools, nil
}

func buildProjects(projects []any, opts options) ([]Project, error) {
	out := make([]Project, 0, len(projects))
	for index, item := range projects {
		prefix := fmt.Sprintf("projects[%d]", index)
		project, err := mapValue(item, prefix)
		if err != nil {
			return nil, err
		}
		workflow, err := stringValue(project["workflow"], prefix+".workflow")
		if err != nil {
			return nil, err
		}
		workflowRef, err := optionalString(project["workflow_ref"], prefix+".workflow_ref")
		if err != nil {
			return nil, err
		}
		workdir, err := stringValue(project["workdir"], prefix+".workdir")
		if err != nil {
			return nil, err
		}
		color, err := optionalString(project["color"], prefix+".color")
		if err != nil {
			return nil, err
		}
		if color != "" {
			normalized, ok := projectcolor.Normalize(color)
			if !ok {
				return nil, fmt.Errorf("%s.color: must be an opaque CSS hex color like #1192e8", prefix)
			}
			color = normalized
		}
		if !opts.projectPathLiterals {
			if strings.TrimSpace(workflowRef) == "" {
				if relativeWorkflowPathLiteral(workflow) {
					return nil, fmt.Errorf("%s.workflow: %s", prefix, plainWorkflowPathRequirement)
				}
				expandedWorkflow, err := expandPath(workflow, opts)
				if err != nil {
					return nil, fmt.Errorf("%s.workflow: expand path: %w", prefix, err)
				}
				workflow = expandedWorkflow
			}
			expandedWorkdir, err := expandPath(workdir, opts)
			if err != nil {
				return nil, fmt.Errorf("%s.workdir: expand path: %w", prefix, err)
			}
			workdir = expandedWorkdir
		}
		id, err := stringValue(project["id"], prefix+".id")
		if err != nil {
			return nil, err
		}
		pool, err := optionalString(project["pool"], prefix+".pool")
		if err != nil {
			return nil, err
		}
		weight, err := intValue(project["weight"], prefix+".weight")
		if err != nil {
			return nil, err
		}
		priority, err := intValue(project["priority"], prefix+".priority")
		if err != nil {
			return nil, err
		}
		paused, err := optionalBool(project["paused"], prefix+".paused")
		if err != nil {
			return nil, err
		}
		pausedReason, err := optionalString(project["paused_reason"], prefix+".paused_reason")
		if err != nil {
			return nil, err
		}
		pausedAt := pauseTimestampString(project["paused_at"])
		pausedUntilIssue, err := optionalString(project["paused_until_issue"], prefix+".paused_until_issue")
		if err != nil {
			return nil, err
		}
		pausedUntil := pauseTimestampString(project["paused_until"])
		var activeHours *activehours.Config
		if project["active_hours"] != nil {
			parsed, err := buildActiveHours(project["active_hours"], prefix+".active_hours")
			if err != nil {
				return nil, err
			}
			activeHours = &parsed
		}
		activeHoursOverrideUntil := pauseTimestampString(project["active_hours_override_until"])
		credentialRef, err := optionalString(project["credential_ref"], prefix+".credential_ref")
		if err != nil {
			return nil, err
		}
		authorization, err := buildAuthorization(project["authorization"], prefix+".authorization")
		if err != nil {
			return nil, err
		}
		knowledge, err := buildKnowledge(project["knowledge"], prefix+".knowledge", opts)
		if err != nil {
			return nil, err
		}
		projectIntake, intakeConfigured, err := buildIntake(project, prefix)
		if err != nil {
			return nil, err
		}
		var memory ProjectMemory
		if project["memory"] != nil {
			if err := decodeYAMLValue(project["memory"], &memory); err != nil {
				return nil, fmt.Errorf("%s.memory: %w", prefix, err)
			}
		}

		out = append(out, Project{
			ID:                       strings.TrimSpace(id),
			Pool:                     pool,
			Workflow:                 strings.TrimSpace(workflow),
			WorkflowRef:              strings.TrimSpace(workflowRef),
			Workdir:                  workdir,
			Color:                    color,
			Knowledge:                knowledge,
			Weight:                   weight,
			Priority:                 priority,
			Paused:                   paused,
			PausedReason:             strings.TrimSpace(pausedReason),
			PausedAt:                 pausedAt,
			PausedUntilIssue:         strings.TrimSpace(pausedUntilIssue),
			PausedUntil:              pausedUntil,
			ActiveHours:              activeHours,
			ActiveHoursOverrideUntil: activeHoursOverrideUntil,
			CredentialRef:            credentialRef,
			Authorization:            authorization,
			Intake:                   projectIntake,
			Memory:                   memory,
			IntakeConfigured:         intakeConfigured,
		})
	}
	return out, nil
}

func activeHoursErrors(value any, prefix string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(map[string]any); !ok {
		return []string{prefix + ": must be a mapping"}
	}
	var config activehours.Config
	if err := decodeYAMLValue(value, &config); err != nil {
		return []string{prefix + ": " + err.Error()}
	}
	return config.Validate(prefix)
}

func buildActiveHours(value any, prefix string) (activehours.Config, error) {
	if _, err := mapValue(value, prefix); err != nil {
		return activehours.Config{}, err
	}
	var config activehours.Config
	if err := decodeYAMLValue(value, &config); err != nil {
		return activehours.Config{}, fmt.Errorf("%s: %w", prefix, err)
	}
	config = config.Normalize()
	if problems := config.Validate(prefix); len(problems) > 0 {
		return activehours.Config{}, errors.New(strings.Join(problems, "; "))
	}
	return config, nil
}

func intakeErrors(value any, prefix string) []string {
	if value == nil {
		return nil
	}
	if _, ok := value.(map[string]any); !ok {
		return []string{prefix + ": must be a mapping"}
	}
	var cfg intakeconfig.Config
	if err := decodeYAMLValue(value, &cfg); err != nil {
		return []string{prefix + ": " + err.Error()}
	}
	cfg.Normalize()
	return cfg.Validate(prefix, nil)
}

func buildIntake(project map[string]any, prefix string) (intakeconfig.Config, bool, error) {
	value, configured := project["intake"]
	if !configured || value == nil {
		return intakeconfig.Config{}, configured, nil
	}
	var cfg intakeconfig.Config
	if err := decodeYAMLValue(value, &cfg); err != nil {
		return intakeconfig.Config{}, configured, fmt.Errorf("%s.intake: %w", prefix, err)
	}
	cfg.Normalize()
	return cfg, configured, nil
}

func buildIdentity(value any, field string) (Identity, error) {
	var identity Identity
	if value == nil {
		return identity, nil
	}
	if _, err := mapValue(value, field); err != nil {
		return Identity{}, err
	}
	if err := decodeYAMLValue(value, &identity); err != nil {
		return Identity{}, fmt.Errorf("%s: %w", field, err)
	}
	identity.Normalize()
	if problems := identity.Validate(field); len(problems) > 0 {
		return Identity{}, errors.New(strings.Join(problems, "; "))
	}
	return identity, nil
}

func buildKnowledge(value any, field string, opts options) (Knowledge, error) {
	if value == nil {
		return Knowledge{}, nil
	}
	if _, err := mapValue(value, field); err != nil {
		return Knowledge{}, err
	}

	knowledge := Knowledge{
		Enabled:    true,
		MaxBytes:   workflowconfig.DefaultKnowledgeMaxBytes,
		Configured: true,
	}
	if err := decodeYAMLValue(value, &knowledge); err != nil {
		return Knowledge{}, fmt.Errorf("%s: %w", field, err)
	}
	if !opts.projectPathLiterals {
		for index := range knowledge.Sources {
			path := strings.TrimSpace(knowledge.Sources[index].Path)
			if path == "" {
				continue
			}
			expanded, err := expandPath(path, opts)
			if err != nil {
				return Knowledge{}, fmt.Errorf("%s.sources[%d].path: expand path: %w", field, index, err)
			}
			knowledge.Sources[index].Path = expanded
		}
	}
	knowledge.Normalize()
	return knowledge, nil
}

func buildAuthorization(value any, field string) (selector.Selector, error) {
	var authorization selector.Selector
	if value == nil {
		return authorization, nil
	}
	if _, err := mapValue(value, field); err != nil {
		return selector.Selector{}, err
	}
	if err := decodeYAMLValue(value, &authorization); err != nil {
		return selector.Selector{}, fmt.Errorf("%s: %w", field, err)
	}
	authorization.Normalize()
	if problems := authorization.Validate(field); len(problems) > 0 {
		return selector.Selector{}, errors.New(strings.Join(problems, "; "))
	}
	return authorization, nil
}

func decodeYAMLValue(value any, out any) error {
	raw, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(raw, out)
}

func mergeMap(defaults map[string]any, value any, field string) (map[string]any, error) {
	out := make(map[string]any, len(defaults))
	maps.Copy(out, defaults)

	if value == nil {
		return out, nil
	}

	source, err := mapValue(value, field)
	if err != nil {
		return nil, err
	}
	maps.Copy(out, source)
	return out, nil
}

func optionalBool(value any, field string) (bool, error) {
	if value == nil {
		return false, nil
	}
	paused, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s: must be a boolean", field)
	}
	return paused, nil
}

func optionalString(value any, field string) (string, error) {
	if value == nil {
		return "", nil
	}
	text, err := stringValue(value, field)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func optionalIntPointer(value any, field string) (*int, error) {
	if value == nil {
		return nil, nil //nolint:nilnil // Absent optional integer is represented as a nil pointer.
	}
	number, err := intValue(value, field)
	if err != nil {
		return nil, err
	}
	return &number, nil
}

func mapValue(value any, field string) (map[string]any, error) {
	typed, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be a mapping", field)
	}
	return typed, nil
}

func listValue(value any, field string) ([]any, error) {
	typed, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be a list", field)
	}
	return typed, nil
}

func stringValue(value any, field string) (string, error) {
	typed, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: must be a string", field)
	}
	return typed, nil
}

func intValue(value any, field string) (int, error) {
	typed, ok := value.(int)
	if !ok {
		return 0, fmt.Errorf("%s: must be an integer", field)
	}
	return typed, nil
}

func buildValidationError(path string, err error) error {
	return ValidationError{Path: path, Problems: []string{err.Error()}}
}

func expandPath(path string, opts options) (string, error) {
	switch {
	case path == "~" || path == "~/":
		if opts.home == "" {
			return "", errors.New("home directory is not available")
		}
		return filepath.Clean(opts.home), nil
	case strings.HasPrefix(path, "~/"):
		if opts.home == "" {
			return "", errors.New("home directory is not available")
		}
		return filepath.Join(opts.home, strings.TrimPrefix(path, "~/")), nil
	case filepath.IsAbs(path):
		return filepath.Clean(path), nil
	default:
		return filepath.Abs(filepath.Join(opts.relativeTo, path))
	}
}

func normalizeYAML(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nestedValue := range typed {
			out[key] = normalizeYAML(nestedValue)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, nestedValue := range typed {
			out[fmt.Sprint(key)] = normalizeYAML(nestedValue)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, nestedValue := range typed {
			out = append(out, normalizeYAML(nestedValue))
		}
		return out
	default:
		return value
	}
}
