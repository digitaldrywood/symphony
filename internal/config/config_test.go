package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	"github.com/digitaldrywood/detent/internal/selector"
)

func TestParseWorkflowOverlayPrecedence(t *testing.T) {
	t.Parallel()

	shared := []byte("---\ntracker:\n  kind: memory\n  assignee: shared\n  active_states: [Todo, In Progress]\npolling:\n  interval_ms: 60000\n  conditional: true\nagent:\n  max_turns: 20\n---\nShared direction.\n")
	tests := []struct {
		name             string
		local            string
		wantAssignee     string
		wantInterval     int
		wantConditional  bool
		wantActiveStates []string
		wantMaxTurns     int
		wantPrompt       string
		wantKeys         []string
	}{
		{
			name:             "nested scalar keys override independently",
			local:            "---\ntracker:\n  assignee: local\npolling:\n  interval_ms: 90000\n---\n",
			wantAssignee:     "local",
			wantInterval:     90000,
			wantConditional:  true,
			wantActiveStates: []string{"Todo", "In Progress"},
			wantMaxTurns:     20,
			wantPrompt:       "Shared direction.\n",
			wantKeys:         []string{"polling.interval_ms", "tracker.assignee"},
		},
		{
			name:             "sequences replace shared values",
			local:            "---\ntracker:\n  active_states: [Backlog]\n---\n",
			wantAssignee:     "shared",
			wantInterval:     60000,
			wantConditional:  true,
			wantActiveStates: []string{"Backlog"},
			wantMaxTurns:     20,
			wantPrompt:       "Shared direction.\n",
			wantKeys:         []string{"tracker.active_states"},
		},
		{
			name:             "local prose follows shared prose",
			local:            "---\nagent:\n  max_turns: 30\n---\nLocal direction.\n",
			wantAssignee:     "shared",
			wantInterval:     60000,
			wantConditional:  true,
			wantActiveStates: []string{"Todo", "In Progress"},
			wantMaxTurns:     30,
			wantPrompt:       "Shared direction.\n\n---\n\n## Machine-local workflow overlay\n\nLocal direction.\n",
			wantKeys:         []string{"agent.max_turns"},
		},
		{
			name:             "prose-only overlay leaves structured config intact",
			local:            "---\n---\nLocal direction.\n",
			wantAssignee:     "shared",
			wantInterval:     60000,
			wantConditional:  true,
			wantActiveStates: []string{"Todo", "In Progress"},
			wantMaxTurns:     20,
			wantPrompt:       "Shared direction.\n\n---\n\n## Machine-local workflow overlay\n\nLocal direction.\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := ParseWorkflowOverlay(shared, []byte(tt.local), "/repo/WORKFLOW.local.md")
			if err != nil {
				t.Fatalf("ParseWorkflowOverlay() error = %v", err)
			}
			if workflow.Config.Tracker.Kind != TrackerMemory {
				t.Fatalf("Tracker.Kind = %q, want %q", workflow.Config.Tracker.Kind, TrackerMemory)
			}
			if workflow.Config.Tracker.Assignee != tt.wantAssignee {
				t.Fatalf("Tracker.Assignee = %q, want %q", workflow.Config.Tracker.Assignee, tt.wantAssignee)
			}
			if workflow.Config.Polling.IntervalMS != tt.wantInterval {
				t.Fatalf("Polling.IntervalMS = %d, want %d", workflow.Config.Polling.IntervalMS, tt.wantInterval)
			}
			if workflow.Config.Polling.Conditional != tt.wantConditional {
				t.Fatalf("Polling.Conditional = %t, want %t", workflow.Config.Polling.Conditional, tt.wantConditional)
			}
			if !slices.Equal(workflow.Config.Tracker.ActiveStates, tt.wantActiveStates) {
				t.Fatalf("Tracker.ActiveStates = %#v, want %#v", workflow.Config.Tracker.ActiveStates, tt.wantActiveStates)
			}
			if workflow.Config.Agent.MaxTurns != tt.wantMaxTurns {
				t.Fatalf("Agent.MaxTurns = %d, want %d", workflow.Config.Agent.MaxTurns, tt.wantMaxTurns)
			}
			if workflow.Prompt != tt.wantPrompt {
				t.Fatalf("Prompt = %q, want %q", workflow.Prompt, tt.wantPrompt)
			}
			if workflow.Overlay.Path != "/repo/WORKFLOW.local.md" {
				t.Fatalf("Overlay.Path = %q, want local path", workflow.Overlay.Path)
			}
			if !slices.Equal(workflow.Overlay.OverriddenKeys, tt.wantKeys) {
				t.Fatalf("Overlay.OverriddenKeys = %#v, want %#v", workflow.Overlay.OverriddenKeys, tt.wantKeys)
			}
		})
	}
}

func TestWorkerGitHubPolicyConfiguration(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte("---\ntracker:\n  kind: memory\nworker:\n  github_token: $DETENT_WORKER_GITHUB_TOKEN\n  github_token_resolution_timeout_ms: 30000\n  github_rest_min_remaining_reserve: 1250\n  github_rest_poll_interval_ms: 90000\n---\nPrompt\n"))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if workflow.Config.Worker.GitHubToken != "$DETENT_WORKER_GITHUB_TOKEN" {
		t.Fatalf("Worker.GitHubToken = %q, want environment reference", workflow.Config.Worker.GitHubToken)
	}
	if workflow.Config.Worker.GitHubTokenResolutionTimeoutMS != 30000 {
		t.Fatalf("Worker.GitHubTokenResolutionTimeoutMS = %d, want 30000", workflow.Config.Worker.GitHubTokenResolutionTimeoutMS)
	}
	if workflow.Config.Worker.GitHubRESTMinReserve != 1250 {
		t.Fatalf("Worker.GitHubRESTMinReserve = %d, want 1250", workflow.Config.Worker.GitHubRESTMinReserve)
	}
	if workflow.Config.Worker.GitHubRESTPollIntervalMS != 90000 {
		t.Fatalf("Worker.GitHubRESTPollIntervalMS = %d, want 90000", workflow.Config.Worker.GitHubRESTPollIntervalMS)
	}
}

func TestRefreshFailureThresholdConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want int
	}{
		{name: "default", raw: "---\ntracker:\n  kind: memory\n---\nPrompt\n", want: DefaultRefreshFailureThreshold},
		{name: "override", raw: "---\ntracker:\n  kind: memory\npolling:\n  refresh_failure_threshold: 5\n---\nPrompt\n", want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.Polling.RefreshFailureThreshold; got != tt.want {
				t.Fatalf("Polling.RefreshFailureThreshold = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestWorkerGitHubDefaultReserveBrakesBeforeOrchestrator(t *testing.T) {
	t.Parallel()

	cfg := Default()
	if cfg.Worker.GitHubTokenResolutionTimeoutMS != 15000 {
		t.Fatalf("Worker.GitHubTokenResolutionTimeoutMS = %d, want 15000", cfg.Worker.GitHubTokenResolutionTimeoutMS)
	}
	if cfg.Worker.GitHubRESTMinReserve <= cfg.Tracker.GitHubRESTMinReserve {
		t.Fatalf("worker reserve = %d, want above orchestrator floor %d", cfg.Worker.GitHubRESTMinReserve, cfg.Tracker.GitHubRESTMinReserve)
	}
}

func TestWorkerGitHubPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mutate      func(*Config)
		wantProblem string
	}{
		{
			name: "token resolution timeout must be positive",
			mutate: func(cfg *Config) {
				cfg.Worker.GitHubTokenResolutionTimeoutMS = 0
			},
			wantProblem: "worker.github_token_resolution_timeout_ms must be greater than 0",
		},
		{
			name: "reserve must be positive",
			mutate: func(cfg *Config) {
				cfg.Worker.GitHubRESTMinReserve = 0
			},
			wantProblem: "worker.github_rest_min_remaining_reserve must be greater than 0",
		},
		{
			name: "poll interval protects REST budget",
			mutate: func(cfg *Config) {
				cfg.Worker.GitHubRESTPollIntervalMS = 59999
			},
			wantProblem: "worker.github_rest_poll_interval_ms must be greater than or equal to 60000",
		},
		{
			name: "ambient gh accepted with worker-first reserve",
			mutate: func(cfg *Config) {
				cfg.Worker.GitHubToken = "gh"
			},
		},
		{
			name: "ambient gh defers equal reserve validation until principal classification",
			mutate: func(cfg *Config) {
				cfg.Worker.GitHubToken = "gh"
				cfg.Worker.GitHubRESTMinReserve = cfg.Tracker.GitHubRESTMinReserve
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			cfg.Tracker.Kind = TrackerMemory
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantProblem == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantProblem) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.wantProblem)
			}
		})
	}
}

func TestWorkerGitHubPolicyValidationWarnings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		want  bool
	}{
		{name: "unset remains disabled"},
		{name: "ambient gh warns", token: "gh", want: true},
		{name: "configured token warns about same principal", token: "$DETENT_WORKER_GITHUB_TOKEN", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			cfg.Worker.GitHubToken = tt.token
			warnings := strings.Join(cfg.ValidationWarnings(), "; ")
			if got := warnings != ""; got != tt.want {
				t.Fatalf("ValidationWarnings() = %q, want warning=%t", warnings, tt.want)
			}
			if tt.want {
				for _, want := range []string{"shared-budget mode", "attribution is indeterminate", "workers brake before", "different GitHub user or App installation"} {
					if !strings.Contains(warnings, want) {
						t.Fatalf("ValidationWarnings() = %q, want containing %q", warnings, want)
					}
				}
			}
		})
	}
}

func TestParseWorkflowActiveHours(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		activeHours string
		wantZone    string
		wantProblem string
	}{
		{
			name: "valid",
			activeHours: `active_hours:
  timezone: America/Chicago
  windows:
    - Mon-Fri 22:00-06:00
`,
			wantZone: "America/Chicago",
		},
		{
			name: "missing timezone",
			activeHours: `active_hours:
  windows:
    - Mon-Fri 22:00-06:00
`,
			wantProblem: "active_hours.timezone: is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte("---\n" + test.activeHours + "tracker:\n  kind: memory\n---\n"))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if test.wantProblem != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantProblem) {
					t.Fatalf("ParseWorkflow() error = %v, want containing %q", err, test.wantProblem)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.ActiveHours.Timezone; got != test.wantZone {
				t.Fatalf("ActiveHours.Timezone = %q, want %q", got, test.wantZone)
			}
		})
	}
}

func TestLoadWorkflowWithoutOverlayPreservesExistingBehavior(t *testing.T) {
	t.Parallel()

	raw := []byte("---\ntracker:\n  kind: memory\n---\nShared direction.\n")
	path := filepath.Join(t.TempDir(), "WORKFLOW.md")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	workflow, err := LoadWorkflow(path)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	sum := sha256.Sum256(raw)
	if workflow.SourceHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("SourceHash = %q, want shared-only hash", workflow.SourceHash)
	}
	if workflow.Prompt != "Shared direction.\n" {
		t.Fatalf("Prompt = %q, want shared prompt", workflow.Prompt)
	}
	if workflow.Overlay.Path != "" || len(workflow.Overlay.OverriddenKeys) != 0 {
		t.Fatalf("Overlay = %#v, want inactive", workflow.Overlay)
	}
}

func TestLocalWorkflowPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want string
	}{
		{path: "/repo/WORKFLOW.md", want: "/repo/WORKFLOW.local.md"},
		{path: "/repo/workflow.md", want: "/repo/workflow.local.md"},
		{path: "/repo/workflow", want: "/repo/workflow.local"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			if got := LocalWorkflowPath(tt.path); got != tt.want {
				t.Fatalf("LocalWorkflowPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestShippedWorkflowTemplatesEnableBudgetCaps(t *testing.T) {
	t.Parallel()

	templateDir := filepath.Join("..", "..", "docs", "templates")
	entries, err := os.ReadDir(templateDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", templateDir, err)
	}

	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "WORKFLOW.") || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		checked++
		t.Run(entry.Name(), func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(templateDir, entry.Name())
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", path, err)
			}
			variant := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "WORKFLOW."), ".md")
			configPath := filepath.Join(templateDir, "detent."+variant+".yaml")
			configRaw, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", configPath, err)
			}
			workflow, err := ParseProjectDefinition(ProjectDefinitionSources{
				WorkflowPath: path,
				Workflow:     raw,
				ConfigPath:   configPath,
				Config:       configRaw,
				HasConfig:    true,
			})
			if err != nil {
				t.Fatalf("ParseProjectDefinition(%q) error = %v", path, err)
			}
			if !workflow.Config.Budget.Enabled {
				t.Fatalf("%s budget.enabled = false, want true", path)
			}
			if workflow.Config.Budget.BillingMode != BillingModeMetered {
				t.Fatalf("%s budget.billing_mode = %q, want metered", path, workflow.Config.Budget.BillingMode)
			}
			normalizedRaw := strings.ReplaceAll(string(configRaw), "\r\n", "\n")
			if workflow.Config.Deliverable.Kind == DeliverablePullRequest && !strings.Contains(normalizedRaw, "\n  merge_method: squash\n") {
				t.Fatalf("%s does not declare deliverable.merge_method: squash", path)
			}
		})
	}
	if checked == 0 {
		t.Fatal("no shipped WORKFLOW templates found")
	}
}

func TestParseWorkflowBudgetCapPresence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		budgetYAML   string
		wantPerDay   bool
		wantPerIssue bool
	}{
		{name: "budget omitted"},
		{name: "only enabled configured", budgetYAML: "  enabled: false\n"},
		{name: "daily cap configured", budgetYAML: "  per_day_max_usd: 50\n", wantPerDay: true},
		{name: "issue cap configured", budgetYAML: "  per_issue_max_usd: 5\n", wantPerIssue: true},
		{name: "both caps configured", budgetYAML: "  per_day_max_usd: 50\n  per_issue_max_usd: 5\n", wantPerDay: true, wantPerIssue: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			frontmatter := "---\n"
			if tt.budgetYAML != "" {
				frontmatter += "budget:\n" + tt.budgetYAML
			}
			workflow, err := ParseWorkflow([]byte(frontmatter + "---\n"))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.Budget.PerDayMaxUSDConfigured(); got != tt.wantPerDay {
				t.Fatalf("PerDayMaxUSDConfigured() = %t, want %t", got, tt.wantPerDay)
			}
			if got := workflow.Config.Budget.PerIssueMaxUSDConfigured(); got != tt.wantPerIssue {
				t.Fatalf("PerIssueMaxUSDConfigured() = %t, want %t", got, tt.wantPerIssue)
			}
		})
	}
}

func TestParseWorkflowBillingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      string
		want       string
		configured bool
		wantErr    string
	}{
		{name: "omitted defaults to subscription", want: BillingModeSubscription},
		{name: "metered", value: "metered", want: BillingModeMetered, configured: true},
		{name: "subscription", value: "subscription", want: BillingModeSubscription, configured: true},
		{name: "normalizes case and whitespace", value: " Subscription ", want: BillingModeSubscription, configured: true},
		{name: "rejects invalid mode", value: "credits", wantErr: "budget.billing_mode must be one of metered, subscription"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			frontmatter := "---\ntracker:\n  kind: memory\n"
			if tt.value != "" {
				frontmatter += "budget:\n  billing_mode: \"" + tt.value + "\"\n"
			}
			workflow, err := ParseWorkflow([]byte(frontmatter + "---\n"))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseWorkflow() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.Budget.EffectiveBillingMode(); got != tt.want {
				t.Fatalf("EffectiveBillingMode() = %q, want %q", got, tt.want)
			}
			if got := workflow.Config.Budget.BillingModeConfigured(); got != tt.configured {
				t.Fatalf("BillingModeConfigured() = %t, want %t", got, tt.configured)
			}
		})
	}
}

func TestUSDBrakes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		billingMode    string
		budgetEnabled  bool
		spendLimit     float64
		wantBudgetCaps bool
		wantProgress   bool
		wantWarning    bool
	}{
		{name: "subscription disabled without progress limit", billingMode: BillingModeSubscription},
		{name: "subscription disabled with progress limit", billingMode: BillingModeSubscription, spendLimit: 3},
		{name: "subscription enabled without progress limit", billingMode: BillingModeSubscription, budgetEnabled: true},
		{name: "subscription enabled with progress limit", billingMode: BillingModeSubscription, budgetEnabled: true, spendLimit: 3},
		{name: "metered disabled without progress limit", billingMode: BillingModeMetered},
		{name: "metered disabled with progress limit", billingMode: BillingModeMetered, spendLimit: 3, wantProgress: true, wantWarning: true},
		{name: "metered enabled without progress limit", billingMode: BillingModeMetered, budgetEnabled: true, wantBudgetCaps: true},
		{name: "metered enabled with progress limit", billingMode: BillingModeMetered, budgetEnabled: true, spendLimit: 3, wantBudgetCaps: true, wantProgress: true, wantWarning: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Budget: Budget{BillingMode: tt.billingMode, Enabled: tt.budgetEnabled},
				Agent:  Agent{NoProgressSpendLimitUSD: tt.spendLimit},
			}
			brakes := cfg.USDBrakes()
			if brakes.BudgetCaps != tt.wantBudgetCaps || brakes.NoProgress != tt.wantProgress {
				t.Fatalf("USDBrakes() = %#v, want budget caps=%t progress=%t", brakes, tt.wantBudgetCaps, tt.wantProgress)
			}
			warnings := cfg.ValidationWarnings()
			if got := len(warnings) > 0; got != tt.wantWarning {
				t.Fatalf("ValidationWarnings() = %#v, want warning=%t", warnings, tt.wantWarning)
			}
			if tt.wantWarning {
				warning := strings.Join(warnings, "; ")
				for _, want := range []string{"billing_mode: metered", "agent.no_progress_spend_limit_usd", "budget.enabled: false", "billing_mode: subscription"} {
					if !strings.Contains(warning, want) {
						t.Fatalf("ValidationWarnings() = %q, want %q", warning, want)
					}
				}
			}
		})
	}
}

func TestParseWorkflowMergeMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		value          string
		want           string
		wantConfigured bool
		wantErr        string
	}{
		{name: "omitted defaults to squash", want: MergeMethodSquash},
		{name: "squash", value: "squash", want: MergeMethodSquash, wantConfigured: true},
		{name: "merge", value: "merge", want: MergeMethodMerge, wantConfigured: true},
		{name: "rebase", value: "rebase", want: MergeMethodRebase, wantConfigured: true},
		{name: "normalizes case and whitespace", value: " ReBaSe ", want: MergeMethodRebase, wantConfigured: true},
		{name: "rejects invalid value", value: "octopus", wantErr: "deliverable.merge_method must be one of squash, merge, rebase"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw := "---\ntracker:\n  kind: memory\n"
			if tt.value != "" {
				raw += "deliverable:\n  merge_method: " + tt.value + "\n"
			}
			workflow, err := ParseWorkflow([]byte(raw + "---\nPrompt\n"))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("configuration error = %v", err)
			}
			if got := workflow.Config.Deliverable.MergeMethod; got != tt.want {
				t.Fatalf("Deliverable.MergeMethod = %q, want %q", got, tt.want)
			}
			if got := workflow.Config.Deliverable.MergeMethodConfigured(); got != tt.wantConfigured {
				t.Fatalf("Deliverable.MergeMethodConfigured() = %t, want %t", got, tt.wantConfigured)
			}
		})
	}
}

func TestParseWorkflowFollowups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		agent   string
		enabled bool
	}{
		{name: "defaults enabled", enabled: true},
		{name: "explicitly enabled", agent: "agent:\n  followups:\n    enabled: true\n", enabled: true},
		{name: "explicitly disabled", agent: "agent:\n  followups:\n    enabled: false\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := ParseWorkflow([]byte("---\n" + tt.agent + "---\nPrompt\n"))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Agent.Followups.Enabled != tt.enabled {
				t.Fatalf("Agent.Followups.Enabled = %t, want %t", workflow.Config.Agent.Followups.Enabled, tt.enabled)
			}
		})
	}
}

func TestNoProgressSpendLimitConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    float64
		wantErr string
	}{
		{name: "default enabled", raw: "---\ntracker:\n  kind: memory\n---\nPrompt\n", want: DefaultNoProgressSpendLimitUSD},
		{name: "explicitly disabled", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_spend_limit_usd: 0\n---\nPrompt\n", want: 0},
		{name: "custom threshold", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_spend_limit_usd: 8.5\n---\nPrompt\n", want: 8.5},
		{name: "negative rejected", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_spend_limit_usd: -1\n---\nPrompt\n", wantErr: "agent.no_progress_spend_limit_usd must be greater than or equal to 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if tt.wantErr != "" {
				if err == nil {
					err = workflow.Config.Validate()
				}
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Agent.NoProgressSpendLimitUSD != tt.want {
				t.Fatalf("NoProgressSpendLimitUSD = %g, want %g", workflow.Config.Agent.NoProgressSpendLimitUSD, tt.want)
			}
			if workflow.Config.Agent.AutoPromote.NoProgressLimit != DefaultNoProgressLimit {
				t.Fatalf("AutoPromote.NoProgressLimit = %d, want %d", workflow.Config.Agent.AutoPromote.NoProgressLimit, DefaultNoProgressLimit)
			}
		})
	}
}

func TestNoProgressTokenLimitConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr string
	}{
		{name: "default enabled", raw: "---\ntracker:\n  kind: memory\n---\nPrompt\n", want: DefaultNoProgressTokenLimit},
		{name: "explicitly disabled", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_token_limit: 0\n---\nPrompt\n"},
		{name: "custom threshold", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_token_limit: 40000000\n---\nPrompt\n", want: 40_000_000},
		{name: "negative rejected", raw: "---\ntracker:\n  kind: memory\nagent:\n  no_progress_token_limit: -1\n---\nPrompt\n", wantErr: "agent.no_progress_token_limit must be greater than or equal to 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if tt.wantErr != "" {
				if err == nil {
					err = workflow.Config.Validate()
				}
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Agent.NoProgressTokenLimit != tt.want {
				t.Fatalf("NoProgressTokenLimit = %d, want %d", workflow.Config.Agent.NoProgressTokenLimit, tt.want)
			}
		})
	}
}

func TestLifetimeLimitConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		raw          string
		wantSessions int64
		wantTokens   int64
		wantCooldown int
		wantOverride string
		wantErr      string
	}{
		{
			name:         "default enabled",
			raw:          "---\ntracker:\n  kind: memory\n---\nPrompt\n",
			wantSessions: DefaultLifetimeSessionLimit,
			wantTokens:   DefaultLifetimeTokenLimit,
			wantCooldown: DefaultLifetimeLimitCooldownSeconds,
			wantOverride: DefaultLifetimeLimitOverrideLabel,
		},
		{
			name:         "custom values normalize override label",
			raw:          "---\ntracker:\n  kind: memory\nagent:\n  lifetime_session_limit: 20\n  lifetime_token_limit: 50000000\n  lifetime_limit_cooldown_seconds: 7200\n  lifetime_limit_override_label: Allow-Hard-Issue\n---\nPrompt\n",
			wantSessions: 20,
			wantTokens:   50_000_000,
			wantCooldown: 7200,
			wantOverride: "allow-hard-issue",
		},
		{
			name:         "limits can be disabled",
			raw:          "---\ntracker:\n  kind: memory\nagent:\n  lifetime_session_limit: 0\n  lifetime_token_limit: 0\n  lifetime_limit_cooldown_seconds: 0\n---\nPrompt\n",
			wantOverride: DefaultLifetimeLimitOverrideLabel,
		},
		{
			name:    "negative sessions rejected",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  lifetime_session_limit: -1\n---\nPrompt\n",
			wantErr: "agent.lifetime_session_limit must be greater than or equal to 0",
		},
		{
			name:    "negative tokens rejected",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  lifetime_token_limit: -1\n---\nPrompt\n",
			wantErr: "agent.lifetime_token_limit must be greater than or equal to 0",
		},
		{
			name:    "enabled limit requires cooldown",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  lifetime_session_limit: 1\n  lifetime_limit_cooldown_seconds: 0\n---\nPrompt\n",
			wantErr: "agent.lifetime_limit_cooldown_seconds must be greater than 0 when a lifetime limit is enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if tt.wantErr != "" {
				if err == nil {
					err = workflow.Config.Validate()
				}
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			agent := workflow.Config.Agent
			if agent.LifetimeSessionLimit != tt.wantSessions || agent.LifetimeTokenLimit != tt.wantTokens || agent.LifetimeLimitCooldownSeconds != tt.wantCooldown || agent.LifetimeLimitOverrideLabel != tt.wantOverride {
				t.Fatalf("lifetime config = sessions %d tokens %d cooldown %d override %q", agent.LifetimeSessionLimit, agent.LifetimeTokenLimit, agent.LifetimeLimitCooldownSeconds, agent.LifetimeLimitOverrideLabel)
			}
		})
	}
}

func TestLifetimeLimitDefaultsSeparateHistoricalRunaways(t *testing.T) {
	t.Parallel()

	if DefaultLifetimeSessionLimit != 120 {
		t.Fatalf("DefaultLifetimeSessionLimit = %d, want 120", DefaultLifetimeSessionLimit)
	}
	if DefaultLifetimeTokenLimit != 750_000_000 {
		t.Fatalf("DefaultLifetimeTokenLimit = %d, want 750000000", DefaultLifetimeTokenLimit)
	}
}

func TestFailureBreakerConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    FailureBreaker
		wantErr string
	}{
		{
			name: "defaults enabled",
			raw:  "---\ntracker:\n  kind: memory\n---\nPrompt\n",
			want: FailureBreaker{
				SameClassLimit:  DefaultFailureBreakerSameClassLimit,
				WindowSeconds:   DefaultFailureBreakerWindowSeconds,
				CooldownSeconds: DefaultFailureBreakerCooldownSeconds,
			},
		},
		{
			name: "custom values",
			raw:  "---\ntracker:\n  kind: memory\nagent:\n  failure_breaker:\n    same_class_limit: 7\n    window_seconds: 900\n    cooldown_seconds: 120\n---\nPrompt\n",
			want: FailureBreaker{SameClassLimit: 7, WindowSeconds: 900, CooldownSeconds: 120},
		},
		{
			name:    "negative limit rejected",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  failure_breaker:\n    same_class_limit: -1\n---\nPrompt\n",
			wantErr: "agent.failure_breaker.same_class_limit must be greater than 0",
		},
		{
			name:    "negative window rejected",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  failure_breaker:\n    window_seconds: -1\n---\nPrompt\n",
			wantErr: "agent.failure_breaker.window_seconds must be greater than 0",
		},
		{
			name:    "negative cooldown rejected",
			raw:     "---\ntracker:\n  kind: memory\nagent:\n  failure_breaker:\n    cooldown_seconds: -1\n---\nPrompt\n",
			wantErr: "agent.failure_breaker.cooldown_seconds must be greater than 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("configuration error = %v", err)
			}
			if got := workflow.Config.Agent.FailureBreaker; got != tt.want {
				t.Fatalf("Agent.FailureBreaker = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestOverloadRetryDelayConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr string
	}{
		{name: "default", raw: "---\ntracker:\n  kind: memory\n---\nPrompt\n", want: DefaultOverloadRetryDelayMS},
		{name: "custom", raw: "---\ntracker:\n  kind: memory\nagent:\n  overload_retry_delay_ms: 60000\n---\nPrompt\n", want: 60000},
		{name: "zero rejected", raw: "---\ntracker:\n  kind: memory\nagent:\n  overload_retry_delay_ms: 0\n---\nPrompt\n", wantErr: "agent.overload_retry_delay_ms must be greater than 0"},
		{name: "negative rejected", raw: "---\ntracker:\n  kind: memory\nagent:\n  overload_retry_delay_ms: -1\n---\nPrompt\n", wantErr: "agent.overload_retry_delay_ms must be greater than 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.raw))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Agent.OverloadRetryDelayMS != tt.want {
				t.Fatalf("OverloadRetryDelayMS = %d, want %d", workflow.Config.Agent.OverloadRetryDelayMS, tt.want)
			}
		})
	}
}

func TestEffectiveNoProgressSpendLimitUSD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limit  float64
		effort string
		want   float64
	}{
		{name: "disabled", limit: 0, effort: "xhigh", want: 0},
		{name: "unknown uses base", limit: 3, want: 3},
		{name: "low uses base", limit: 3, effort: "low", want: 3},
		{name: "medium", limit: 3, effort: "medium", want: 4.5},
		{name: "high", limit: 3, effort: "high", want: 9},
		{name: "xhigh", limit: 3, effort: "xhigh", want: 18},
		{name: "max", limit: 3, effort: "max", want: 24},
		{name: "ultracode", limit: 3, effort: "ultracode", want: 24},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := EffectiveNoProgressSpendLimitUSD(tt.limit, tt.effort); got != tt.want {
				t.Fatalf("EffectiveNoProgressSpendLimitUSD(%g, %q) = %g, want %g", tt.limit, tt.effort, got, tt.want)
			}
		})
	}
}

func TestRESTFanoutMaxRequestsConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   int
		wantErr string
	}{
		{name: "disabled", value: 0},
		{name: "custom cap", value: 200},
		{name: "negative rejected", value: -1, wantErr: "tracker.github_rest_fanout_max_requests must be greater than or equal to 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw := []byte("---\ntracker:\n  kind: memory\n  github_rest_fanout_max_requests: " + strconv.Itoa(tt.value) + "\n---\nPrompt\n")
			workflow, err := ParseWorkflow(raw)
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Tracker.GitHubRESTFanoutMaxRequests != tt.value {
				t.Fatalf("GitHubRESTFanoutMaxRequests = %d, want %d", workflow.Config.Tracker.GitHubRESTFanoutMaxRequests, tt.value)
			}
		})
	}
}

func TestGitHubUnstartedCheckThresholdConfiguration(t *testing.T) {
	t.Parallel()

	intPointer := func(value int) *int { return &value }
	tests := []struct {
		name    string
		value   *int
		want    int
		wantErr string
	}{
		{name: "default", want: DefaultGitHubUnstartedSeconds},
		{name: "custom", value: intPointer(300), want: 300},
		{name: "zero rejected", value: intPointer(0), wantErr: "tracker.github_unstarted_check_threshold_seconds must be greater than 0"},
		{name: "negative rejected", value: intPointer(-1), wantErr: "tracker.github_unstarted_check_threshold_seconds must be greater than 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw := "---\ntracker:\n  kind: memory\n"
			if tt.value != nil {
				raw += "  github_unstarted_check_threshold_seconds: " + strconv.Itoa(*tt.value) + "\n"
			}
			raw += "---\nPrompt\n"
			workflow, err := ParseWorkflow([]byte(raw))
			if err == nil {
				err = workflow.Config.Validate()
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("configuration error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.Tracker.GitHubUnstartedSeconds; got != tt.want {
				t.Fatalf("GitHubUnstartedSeconds = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseWorkflowFrontmatter(t *testing.T) {
	t.Parallel()

	raw := []byte(`---
identity:
  name: release-captain
  github_login: detent-bot
  ownership_mode: field
  owner_field: Owner
tracker:
  kind: github
  api_key: $GITHUB_TOKEN
  project_slug: "PVT_project"
  write_probe_issue: " digitaldrywood/detent#1 "
  http_max_idle_conns: 120
  http_max_idle_conns_per_host: 40
  http_idle_conn_timeout_ms: 120000
  github_graphql_warn_remaining: 750
  github_graphql_min_remaining_reserve: 1750
  github_rest_min_remaining_reserve: 1500
  github_rest_fanout_max_requests: 42
  github_rest_debug_logging: true
  claims:
    enabled: true
    lease_field: Detent Lease
    ttl_seconds: 300
    heartbeat_seconds: 45
  authorization:
    assignee_in:
      - "@me"
    labels:
      include:
        - release
    fields:
      - name: Track
        value: multi-instance
  active_states:
    - Todo
    - In Progress
    - Rework
  state_map:
    Cancelled: Done
  priority_map:
    Urgent: 1
    No priority: null
  dependency_auto_unblock:
    enabled: true
    source_states:
      - Blocked
      - Waiting
    target_state: Todo
    readiness: terminal_or_merged
  blocked_recovery:
    enabled: true
    breaker_cooldown_seconds: 43200
    source_states:
      - Blocked
    target_state: Rework
    reason_codes:
      - merge-conflicts
      - stale base
      - missing_current_head_ci
  blocker_auto_promote:
    enabled: true
    source_states:
      - Blocked
      - Rework
    blocker_states:
      - Backlog
      - Human Review
    target_state: Todo
polling:
  interval_ms: 60000
  conditional: false
workspace:
  root: ~/code/detent-workspaces
  cache_strategy: shared
  auto_branch: false
  cleanup_idle_ttl_ms: 7200000
  cleanup_sweep_interval_ms: 120000
dependencies:
  source: native_only
workpad:
  structured_only: true
worker:
  ssh_hosts:
    - worker-1
  max_concurrent_agents_per_host: 2
agent:
  max_concurrent_agents: 5
  max_turn_duration_ms: 900000
  max_session_duration_ms: 3600000
  no_progress_timeout_ms: 1800000
  merge_worker_startup_timeout_ms: 180000
  merge_worker_max_duration_ms: 7200000
  merge_fallback_max_duration_ms: 1200000
  max_session_tokens: 10000000
  max_session_context_multiplier: 3.5
  max_session_token_override_label: Allow-Large-Session
  max_session_token_override_field: Token Override
  experimental_thread_resume: true
  shutdown:
    drain_timeout_ms: 300000
  max_concurrent_agents_by_state:
    Merging: 1
  dispatch_priority_by_state:
    - Merging
    - Rework
  dispatch_priority_by_label:
    - Bug
    - regression
    - enhancement
  prioritize_unblockers: false
  auto_promote:
    enabled: true
    quiet_seconds: 0
    optout_label: Requires-Human-Review
    gate_wait_state: review
    gate_wait_timeout_seconds: 900
    rework_limit: 3
    no_progress_limit: 4
    allowed_issue_labels:
      - enhancement
  instructions_by_state:
    Rework: |
      Address review comments before moving back to Human Review.
  instructions_by_transition:
    Todo:
      In Progress: |
        Confirm prerequisites before implementation.
  lessons:
    enabled: true
    path: ".detent/lessons.md"
    max_entries: 5
    recall_n: 2
    postmortem_max_tokens: 256
  knowledge:
    enabled: true
    max_bytes: 2048
    sources:
      - name: Team standards
        path: ../knowledge/team.md
  skills:
    enabled: true
    path: ".detent/skills"
    max_skills_in_prompt: 20
    creation:
      enabled: true
      max_drafts_per_run: 3
  followups:
    enabled: false
codex:
  command: codex app-server
  shell: bash
  approval_policy: never
  thread_sandbox: danger-full-access
  turn_sandbox_policy:
    type: dangerFullAccess
    networkAccess: true
  turn_timeout_ms: 600000
  read_timeout_ms: 1000
  stall_timeout_ms: 0
gate:
  kind: human_review
  approval_label: Approved-By-Human
plan:
  enabled: true
  review: both
  approval_label: Plan-Approved
  stop: " Plan Review "
server:
  host: 0.0.0.0
  port: 4001
  kanban:
    mode: integration
    show_blocked_alerts: true
    allowed_transitions:
      In Progress:
        - Blocked
        - Cancelled
      QA:
        - Done
observability:
  dashboard_enabled: false
  refresh_ms: 2000
  render_interval_ms: 32
budget:
  enabled: true
  per_day_max_usd: 25
  per_issue_max_usd: 5
  refusal_cooldown_seconds: 30
  pricing_path: priv/pricing/models.yaml
hooks:
  shell: bash
  after_create: git clone .
  before_run: echo before
  after_run: echo after
  before_remove: echo remove
  timeout_ms: 30000
---
Ticket prompt {{ issue.title }}
`)

	workflow, err := ParseWorkflow(raw)
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	cfg := workflow.Config
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if workflow.Prompt != "Ticket prompt {{ issue.title }}\n" {
		t.Fatalf("Prompt = %q", workflow.Prompt)
	}
	if cfg.Tracker.Kind != TrackerGitHub {
		t.Fatalf("Tracker.Kind = %q, want %q", cfg.Tracker.Kind, TrackerGitHub)
	}
	if cfg.Identity.Name != "release-captain" {
		t.Fatalf("Identity.Name = %q, want release-captain", cfg.Identity.Name)
	}
	if cfg.Identity.GitHubLogin != "detent-bot" {
		t.Fatalf("Identity.GitHubLogin = %q, want detent-bot", cfg.Identity.GitHubLogin)
	}
	if cfg.Identity.OwnershipMode != IdentityOwnershipField {
		t.Fatalf("Identity.OwnershipMode = %q, want %q", cfg.Identity.OwnershipMode, IdentityOwnershipField)
	}
	if cfg.Identity.OwnerField != "Owner" {
		t.Fatalf("Identity.OwnerField = %q, want Owner", cfg.Identity.OwnerField)
	}
	if cfg.Tracker.Endpoint != "https://api.github.com/graphql" {
		t.Fatalf("Tracker.Endpoint = %q", cfg.Tracker.Endpoint)
	}
	if cfg.Tracker.WriteProbeIssue != "digitaldrywood/detent#1" {
		t.Fatalf("Tracker.WriteProbeIssue = %q, want digitaldrywood/detent#1", cfg.Tracker.WriteProbeIssue)
	}
	if cfg.Tracker.HTTPMaxIdleConns != 120 {
		t.Fatalf("Tracker.HTTPMaxIdleConns = %d, want 120", cfg.Tracker.HTTPMaxIdleConns)
	}
	if cfg.Tracker.HTTPMaxIdleConnsPerHost != 40 {
		t.Fatalf("Tracker.HTTPMaxIdleConnsPerHost = %d, want 40", cfg.Tracker.HTTPMaxIdleConnsPerHost)
	}
	if cfg.Tracker.HTTPIdleConnTimeoutMS != 120000 {
		t.Fatalf("Tracker.HTTPIdleConnTimeoutMS = %d, want 120000", cfg.Tracker.HTTPIdleConnTimeoutMS)
	}
	if cfg.Workspace.CleanupIdleTTLMS != 7200000 {
		t.Fatalf("Workspace.CleanupIdleTTLMS = %d, want 7200000", cfg.Workspace.CleanupIdleTTLMS)
	}
	if cfg.Workspace.CacheStrategy != WorkspaceCacheShared {
		t.Fatalf("Workspace.CacheStrategy = %q, want %q", cfg.Workspace.CacheStrategy, WorkspaceCacheShared)
	}
	if cfg.Workspace.CleanupSweepIntervalMS != 120000 {
		t.Fatalf("Workspace.CleanupSweepIntervalMS = %d, want 120000", cfg.Workspace.CleanupSweepIntervalMS)
	}
	if cfg.Dependencies.Source != DependencySourceNativeOnly {
		t.Fatalf("Dependencies.Source = %q, want %q", cfg.Dependencies.Source, DependencySourceNativeOnly)
	}
	if cfg.Polling.Conditional {
		t.Fatal("Polling.Conditional = true, want explicit false")
	}
	if cfg.Tracker.GitHubGraphQLWarnRemaining != 750 {
		t.Fatalf("Tracker.GitHubGraphQLWarnRemaining = %d, want 750", cfg.Tracker.GitHubGraphQLWarnRemaining)
	}
	if cfg.Tracker.GitHubGraphQLMinReserve != 1750 {
		t.Fatalf("Tracker.GitHubGraphQLMinReserve = %d, want 1750", cfg.Tracker.GitHubGraphQLMinReserve)
	}
	if cfg.Tracker.GitHubRESTMinReserve != 1500 {
		t.Fatalf("Tracker.GitHubRESTMinReserve = %d, want 1500", cfg.Tracker.GitHubRESTMinReserve)
	}
	if cfg.Tracker.GitHubRESTFanoutMaxRequests != 42 {
		t.Fatalf("Tracker.GitHubRESTFanoutMaxRequests = %d, want 42", cfg.Tracker.GitHubRESTFanoutMaxRequests)
	}
	if !cfg.Tracker.GitHubRESTDebugLogging {
		t.Fatal("Tracker.GitHubRESTDebugLogging = false, want true")
	}
	if !cfg.Tracker.Claims.Enabled {
		t.Fatal("Tracker.Claims.Enabled = false, want true")
	}
	if cfg.Tracker.Claims.LeaseField != "Detent Lease" {
		t.Fatalf("Tracker.Claims.LeaseField = %q, want Detent Lease", cfg.Tracker.Claims.LeaseField)
	}
	if cfg.Tracker.Claims.TTLSeconds != 300 {
		t.Fatalf("Tracker.Claims.TTLSeconds = %d, want 300", cfg.Tracker.Claims.TTLSeconds)
	}
	if cfg.Tracker.Claims.HeartbeatSeconds != 45 {
		t.Fatalf("Tracker.Claims.HeartbeatSeconds = %d, want 45", cfg.Tracker.Claims.HeartbeatSeconds)
	}
	wantAuthorization := selector.Selector{
		AssigneeIn: []string{"@me"},
		Labels:     selector.Labels{Include: []string{"release"}},
		Fields:     []selector.FieldEquals{{Name: "Track", Value: "multi-instance"}},
	}
	if got := cfg.Tracker.Authorization; !reflect.DeepEqual(got, wantAuthorization) {
		t.Fatalf("Tracker.Authorization = %#v, want %#v", got, wantAuthorization)
	}
	if got := cfg.Tracker.StateMap.Map["Cancelled"]; got != "Done" {
		t.Fatalf("Tracker.StateMap[Cancelled] = %v, want Done", got)
	}
	if got := cfg.Tracker.PriorityMap.Map["No priority"]; got != nil {
		t.Fatalf("Tracker.PriorityMap[No priority] = %v, want nil", got)
	}
	if !cfg.Tracker.DependencyAutoUnblock.Enabled {
		t.Fatal("Tracker.DependencyAutoUnblock.Enabled = false, want true")
	}
	if got := cfg.Tracker.DependencyAutoUnblock.SourceStates; !reflect.DeepEqual(got, []string{"blocked", "waiting"}) {
		t.Fatalf("Tracker.DependencyAutoUnblock.SourceStates = %#v, want blocked/waiting", got)
	}
	if cfg.Tracker.DependencyAutoUnblock.TargetState != "Todo" {
		t.Fatalf("Tracker.DependencyAutoUnblock.TargetState = %q, want Todo", cfg.Tracker.DependencyAutoUnblock.TargetState)
	}
	if cfg.Tracker.DependencyAutoUnblock.Readiness != DependencyReadinessTerminalOrMerged {
		t.Fatalf("Tracker.DependencyAutoUnblock.Readiness = %q, want %q", cfg.Tracker.DependencyAutoUnblock.Readiness, DependencyReadinessTerminalOrMerged)
	}
	if !cfg.Tracker.BlockedRecovery.Enabled {
		t.Fatal("Tracker.BlockedRecovery.Enabled = false, want true")
	}
	if got := cfg.Tracker.BlockedRecovery.SourceStates; !reflect.DeepEqual(got, []string{"blocked"}) {
		t.Fatalf("Tracker.BlockedRecovery.SourceStates = %#v, want blocked", got)
	}
	if cfg.Tracker.BlockedRecovery.TargetState != "Rework" {
		t.Fatalf("Tracker.BlockedRecovery.TargetState = %q, want Rework", cfg.Tracker.BlockedRecovery.TargetState)
	}
	if got := cfg.Tracker.BlockedRecovery.ReasonCodes; !reflect.DeepEqual(got, []string{"merge_conflict", "stale_base", "missing_current_head_ci"}) {
		t.Fatalf("Tracker.BlockedRecovery.ReasonCodes = %#v, want canonical reason codes", got)
	}
	if cfg.Tracker.BlockedRecovery.BreakerCooldownSeconds != 43200 {
		t.Fatalf("Tracker.BlockedRecovery.BreakerCooldownSeconds = %d, want 43200", cfg.Tracker.BlockedRecovery.BreakerCooldownSeconds)
	}
	if !cfg.Tracker.BlockerAutoPromote.Enabled {
		t.Fatal("Tracker.BlockerAutoPromote.Enabled = false, want true")
	}
	if got := cfg.Tracker.BlockerAutoPromote.SourceStates; !reflect.DeepEqual(got, []string{"blocked", "rework"}) {
		t.Fatalf("Tracker.BlockerAutoPromote.SourceStates = %#v, want blocked/rework", got)
	}
	if got := cfg.Tracker.BlockerAutoPromote.BlockerStates; !reflect.DeepEqual(got, []string{"backlog", "human review"}) {
		t.Fatalf("Tracker.BlockerAutoPromote.BlockerStates = %#v, want backlog/human review", got)
	}
	if cfg.Tracker.BlockerAutoPromote.TargetState != "Todo" {
		t.Fatalf("Tracker.BlockerAutoPromote.TargetState = %q, want Todo", cfg.Tracker.BlockerAutoPromote.TargetState)
	}
	if got := cfg.Agent.MaxConcurrentAgentsByState["merging"]; got != 1 {
		t.Fatalf("Agent.MaxConcurrentAgentsByState[merging] = %d, want 1", got)
	}
	if cfg.Agent.MaxTurnDurationMS != 900000 {
		t.Fatalf("Agent.MaxTurnDurationMS = %d, want 900000", cfg.Agent.MaxTurnDurationMS)
	}
	if cfg.Agent.MaxSessionDurationMS != 3600000 {
		t.Fatalf("Agent.MaxSessionDurationMS = %d, want 3600000", cfg.Agent.MaxSessionDurationMS)
	}
	if cfg.Agent.NoProgressTimeoutMS != 1800000 {
		t.Fatalf("Agent.NoProgressTimeoutMS = %d, want 1800000", cfg.Agent.NoProgressTimeoutMS)
	}
	if cfg.Agent.MergeWorkerStartupTimeoutMS != 180000 {
		t.Fatalf("Agent.MergeWorkerStartupTimeoutMS = %d, want 180000", cfg.Agent.MergeWorkerStartupTimeoutMS)
	}
	if cfg.Agent.MergeWorkerMaxDurationMS != 7200000 {
		t.Fatalf("Agent.MergeWorkerMaxDurationMS = %d, want 7200000", cfg.Agent.MergeWorkerMaxDurationMS)
	}
	if cfg.Agent.MergeFallbackMaxDurationMS != 1200000 {
		t.Fatalf("Agent.MergeFallbackMaxDurationMS = %d, want 1200000", cfg.Agent.MergeFallbackMaxDurationMS)
	}
	if cfg.Agent.MaxSessionTokens != 10000000 {
		t.Fatalf("Agent.MaxSessionTokens = %d, want 10000000", cfg.Agent.MaxSessionTokens)
	}
	if cfg.Agent.MaxSessionContextMultiplier != 3.5 {
		t.Fatalf("Agent.MaxSessionContextMultiplier = %v, want 3.5", cfg.Agent.MaxSessionContextMultiplier)
	}
	if cfg.Agent.MaxSessionTokenOverrideLabel != "allow-large-session" {
		t.Fatalf("Agent.MaxSessionTokenOverrideLabel = %q, want allow-large-session", cfg.Agent.MaxSessionTokenOverrideLabel)
	}
	if cfg.Agent.MaxSessionTokenOverrideField != "Token Override" {
		t.Fatalf("Agent.MaxSessionTokenOverrideField = %q, want Token Override", cfg.Agent.MaxSessionTokenOverrideField)
	}
	if !cfg.Agent.ExperimentalThreadResume {
		t.Fatal("Agent.ExperimentalThreadResume = false, want true")
	}
	if !cfg.Agent.Knowledge.Enabled {
		t.Fatal("Agent.Knowledge.Enabled = false, want true")
	}
	if cfg.Agent.Knowledge.MaxBytes != 2048 {
		t.Fatalf("Agent.Knowledge.MaxBytes = %d, want 2048", cfg.Agent.Knowledge.MaxBytes)
	}
	if len(cfg.Agent.Knowledge.Sources) != 1 {
		t.Fatalf("Agent.Knowledge.Sources len = %d, want 1", len(cfg.Agent.Knowledge.Sources))
	}
	if source := cfg.Agent.Knowledge.Sources[0]; source.Name != "Team standards" || source.Path != "../knowledge/team.md" {
		t.Fatalf("Agent.Knowledge.Sources[0] = %#v, want team standards source", source)
	}
	if !cfg.Agent.Skills.Creation.Enabled {
		t.Fatal("Agent.Skills.Creation.Enabled = false, want true")
	}
	if cfg.Agent.Skills.Creation.MaxDraftsPerRun != 3 {
		t.Fatalf("Agent.Skills.Creation.MaxDraftsPerRun = %d, want 3", cfg.Agent.Skills.Creation.MaxDraftsPerRun)
	}
	if cfg.Agent.Followups.Enabled {
		t.Fatal("Agent.Followups.Enabled = true, want false")
	}
	if cfg.Agent.Shutdown.DrainTimeoutMS != 300000 {
		t.Fatalf("Agent.Shutdown.DrainTimeoutMS = %d, want 300000", cfg.Agent.Shutdown.DrainTimeoutMS)
	}
	if !cfg.Workpad.StructuredOnly {
		t.Fatal("Workpad.StructuredOnly = false, want true")
	}
	if got := cfg.Agent.DispatchPriorityByState; len(got) != 2 || got[0] != "merging" || got[1] != "rework" {
		t.Fatalf("Agent.DispatchPriorityByState = %#v", got)
	}
	if got := cfg.Agent.DispatchPriorityByLabel; !reflect.DeepEqual(got, []string{"bug", "regression", "enhancement"}) {
		t.Fatalf("Agent.DispatchPriorityByLabel = %#v, want bug/regression/enhancement", got)
	}
	if cfg.Agent.PrioritizeUnblockers {
		t.Fatal("Agent.PrioritizeUnblockers = true, want false")
	}
	if got := cfg.Agent.InstructionsByState["Rework"]; !strings.Contains(got, "Address review comments") {
		t.Fatalf("Agent.InstructionsByState[Rework] = %q, want review instructions", got)
	}
	if got := cfg.Agent.InstructionsByTransition["Todo"]["In Progress"]; !strings.Contains(got, "Confirm prerequisites") {
		t.Fatalf("Agent.InstructionsByTransition[Todo][In Progress] = %q, want transition instructions", got)
	}
	if cfg.Agent.AutoPromote.OptoutLabel != "requires-human-review" {
		t.Fatalf("Agent.AutoPromote.OptoutLabel = %q", cfg.Agent.AutoPromote.OptoutLabel)
	}
	if cfg.Agent.AutoPromote.ReworkLimit != 3 {
		t.Fatalf("Agent.AutoPromote.ReworkLimit = %d, want 3", cfg.Agent.AutoPromote.ReworkLimit)
	}
	if cfg.Agent.AutoPromote.GateWaitState != AutoPromoteGateWaitStateReview {
		t.Fatalf("Agent.AutoPromote.GateWaitState = %q, want review", cfg.Agent.AutoPromote.GateWaitState)
	}
	if cfg.Agent.AutoPromote.GateWaitTimeoutSeconds != 900 {
		t.Fatalf("Agent.AutoPromote.GateWaitTimeoutSeconds = %d, want 900", cfg.Agent.AutoPromote.GateWaitTimeoutSeconds)
	}
	if cfg.Agent.AutoPromote.NoProgressLimit != 4 {
		t.Fatalf("Agent.AutoPromote.NoProgressLimit = %d, want 4", cfg.Agent.AutoPromote.NoProgressLimit)
	}
	if !cfg.Codex.ApprovalPolicy.IsString || cfg.Codex.ApprovalPolicy.String != "never" {
		t.Fatalf("Codex.ApprovalPolicy = %#v, want string never", cfg.Codex.ApprovalPolicy)
	}
	if cfg.Gate.Kind != gate.KindHumanReview || cfg.Gate.ApprovalLabel != "approved-by-human" || cfg.Gate.Run != "" {
		t.Fatalf("Gate = %#v, want human_review with approved-by-human label", cfg.Gate)
	}
	if !cfg.Plan.Enabled || cfg.Plan.Review != gate.PlanReviewBoth || cfg.Plan.ApprovalLabel != gate.DefaultPlanApprovalLabel || cfg.Plan.Stop != gate.DefaultPlanStop {
		t.Fatalf("Plan = %#v, want enabled both review at Plan Review with plan-approved label", cfg.Plan)
	}
	if !stateListContains(cfg.Tracker.ObservedStates, gate.DefaultPlanStop) {
		t.Fatalf("Tracker.ObservedStates = %#v, want plan stop", cfg.Tracker.ObservedStates)
	}
	if cfg.Server.Kanban.Mode != KanbanModeIntegration {
		t.Fatalf("Server.Kanban.Mode = %q, want %q", cfg.Server.Kanban.Mode, KanbanModeIntegration)
	}
	if !cfg.Server.Kanban.ShowBlockedAlerts {
		t.Fatal("Server.Kanban.ShowBlockedAlerts = false, want true")
	}
	wantTransitions := map[string][]string{
		"In Progress": {"Blocked", "Cancelled"},
		"QA":          {"Done"},
	}
	if !reflect.DeepEqual(cfg.Server.Kanban.AllowedTransitions, wantTransitions) {
		t.Fatalf("Server.Kanban.AllowedTransitions = %#v, want %#v", cfg.Server.Kanban.AllowedTransitions, wantTransitions)
	}
	if cfg.Codex.Shell != "bash" {
		t.Fatalf("Codex.Shell = %q, want bash", cfg.Codex.Shell)
	}
	if got := cfg.Codex.TurnSandboxPolicy["networkAccess"]; got != true {
		t.Fatalf("Codex.TurnSandboxPolicy[networkAccess] = %v, want true", got)
	}
	if !cfg.Budget.Enabled {
		t.Fatal("Budget.Enabled = false, want true")
	}
	if cfg.Hooks.AfterCreate != "git clone ." {
		t.Fatalf("Hooks.AfterCreate = %q", cfg.Hooks.AfterCreate)
	}
	if cfg.Hooks.Shell != "bash" {
		t.Fatalf("Hooks.Shell = %q, want bash", cfg.Hooks.Shell)
	}
}

func TestParseWorkflowRetroDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
  active_states: [Todo]
  observed_states: [Backlog, Blocked]
retro:
  enabled: true
  target_state: Backlog
  allow_public_cross_project_details: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	retro := workflow.Config.Retro
	if !retro.Enabled || !retro.AllowPublicCrossProjectDetails || retro.DailyIssueCap != 3 || retro.LookbackDays != 7 || retro.MinOccurrences != 2 || retro.ProductRepository != "digitaldrywood/detent" || !reflect.DeepEqual(retro.Labels, []string{"retro"}) {
		t.Fatalf("Retro = %#v", retro)
	}
}

func TestParseWorkflowGitHubIssueFieldTracker(t *testing.T) {
	t.Parallel()

	raw := []byte(`---
tracker:
  kind: github
  api_key: $GITHUB_TOKEN
  github_status_source: issue_field
  repository: digitaldrywood/detent
  active_states:
    - Todo
---
Prompt
`)

	workflow, err := ParseWorkflow(raw)
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	cfg := workflow.Config
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.Tracker.GitHubStatusSource != GitHubStatusSourceIssueField {
		t.Fatalf("GitHubStatusSource = %q, want %q", cfg.Tracker.GitHubStatusSource, GitHubStatusSourceIssueField)
	}
	if cfg.Tracker.Repository != "digitaldrywood/detent" {
		t.Fatalf("Repository = %q, want digitaldrywood/detent", cfg.Tracker.Repository)
	}
	if cfg.Tracker.StatusField != "Status" {
		t.Fatalf("StatusField = %q, want Status", cfg.Tracker.StatusField)
	}
	if cfg.Tracker.ProjectSlug != "" {
		t.Fatalf("ProjectSlug = %q, want empty for issue_field source", cfg.Tracker.ProjectSlug)
	}
}

func TestParseWorkflowGitHubLabelTracker(t *testing.T) {
	t.Parallel()

	raw := []byte(`---
tracker:
  kind: github
  api_key: $GITHUB_TOKEN
  github_status_source: label
  repository: digitaldrywood/detent
  status_label_prefix: "detent:"
  active_states:
    - Todo
---
Prompt
`)

	workflow, err := ParseWorkflow(raw)
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	cfg := workflow.Config
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.Tracker.GitHubStatusSource != GitHubStatusSourceLabel {
		t.Fatalf("GitHubStatusSource = %q, want %q", cfg.Tracker.GitHubStatusSource, GitHubStatusSourceLabel)
	}
	if cfg.Tracker.Repository != "digitaldrywood/detent" {
		t.Fatalf("Repository = %q, want digitaldrywood/detent", cfg.Tracker.Repository)
	}
	if cfg.Tracker.StatusLabelPrefix != "detent:" {
		t.Fatalf("StatusLabelPrefix = %q, want detent:", cfg.Tracker.StatusLabelPrefix)
	}
	if cfg.Tracker.ProjectSlug != "" {
		t.Fatalf("ProjectSlug = %q, want empty for label source", cfg.Tracker.ProjectSlug)
	}
}

func TestValidateGitHubProjectV2StillRequiresProjectSlug(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.Kind = TrackerGitHub
	cfg.Tracker.APIKey = "token"
	cfg.Tracker.ProjectSlug = ""

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "tracker.project_slug") {
		t.Fatalf("Validate() error = %v, want project_slug requirement", err)
	}
}

func TestValidateGitHubIssueFieldRequiresRepository(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.Kind = TrackerGitHub
	cfg.Tracker.APIKey = "token"
	cfg.Tracker.GitHubStatusSource = GitHubStatusSourceIssueField
	cfg.Tracker.ProjectSlug = ""
	cfg.Tracker.Repository = ""

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "tracker.repository") {
		t.Fatalf("Validate() error = %v, want repository requirement", err)
	}
	if strings.Contains(err.Error(), "tracker.project_slug") {
		t.Fatalf("Validate() error = %v, want no project_slug requirement in issue_field mode", err)
	}
}

func TestValidateGitHubLabelRequiresRepository(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.Kind = TrackerGitHub
	cfg.Tracker.APIKey = "token"
	cfg.Tracker.GitHubStatusSource = GitHubStatusSourceLabel
	cfg.Tracker.ProjectSlug = ""
	cfg.Tracker.Repository = ""

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "tracker.repository") {
		t.Fatalf("Validate() error = %v, want repository requirement", err)
	}
	if strings.Contains(err.Error(), "tracker.project_slug") {
		t.Fatalf("Validate() error = %v, want no project_slug requirement in label mode", err)
	}
}

func TestNormalizeGitHubLabelStatusSourceAliases(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"label", "labels", "issue_label", "issue_labels"} {
		if got := normalizeGitHubStatusSource(value); got != GitHubStatusSourceLabel {
			t.Fatalf("normalizeGitHubStatusSource(%q) = %q, want %q", value, got, GitHubStatusSourceLabel)
		}
	}
}

func TestParseWorkflowDefaults(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte("---\ntracker:\n  kind: memory\n---\nPrompt\n"))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	cfg := workflow.Config
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if cfg.Tracker.Endpoint != "https://api.linear.app/graphql" {
		t.Fatalf("Tracker.Endpoint = %q", cfg.Tracker.Endpoint)
	}
	if cfg.Identity.Configured() {
		t.Fatalf("Identity = %#v, want omitted default", cfg.Identity)
	}
	if cfg.Tracker.Authorization.Configured() {
		t.Fatalf("Tracker.Authorization = %#v, want authorize all default", cfg.Tracker.Authorization)
	}
	if cfg.Tracker.DependencyAutoUnblock.Enabled {
		t.Fatal("Tracker.DependencyAutoUnblock.Enabled = true, want disabled by default")
	}
	if got := cfg.Tracker.DependencyAutoUnblock.SourceStates; !reflect.DeepEqual(got, []string{"blocked"}) {
		t.Fatalf("Tracker.DependencyAutoUnblock.SourceStates = %#v, want blocked", got)
	}
	if cfg.Tracker.DependencyAutoUnblock.TargetState != "Todo" {
		t.Fatalf("Tracker.DependencyAutoUnblock.TargetState = %q, want Todo", cfg.Tracker.DependencyAutoUnblock.TargetState)
	}
	if cfg.Tracker.DependencyAutoUnblock.Readiness != DependencyReadinessTerminalOrMerged {
		t.Fatalf("Tracker.DependencyAutoUnblock.Readiness = %q, want %q", cfg.Tracker.DependencyAutoUnblock.Readiness, DependencyReadinessTerminalOrMerged)
	}
	if cfg.Tracker.BlockedRecovery.Enabled {
		t.Fatal("Tracker.BlockedRecovery.Enabled = true, want disabled by default")
	}
	if got := cfg.Tracker.BlockedRecovery.SourceStates; !reflect.DeepEqual(got, []string{"blocked"}) {
		t.Fatalf("Tracker.BlockedRecovery.SourceStates = %#v, want blocked", got)
	}
	if cfg.Tracker.BlockedRecovery.TargetState != "Rework" {
		t.Fatalf("Tracker.BlockedRecovery.TargetState = %q, want Rework", cfg.Tracker.BlockedRecovery.TargetState)
	}
	if got := cfg.Tracker.BlockedRecovery.ReasonCodes; !reflect.DeepEqual(got, []string{"merge_conflict", "stale_base", "missing_current_head_ci"}) {
		t.Fatalf("Tracker.BlockedRecovery.ReasonCodes = %#v, want default allowlist", got)
	}
	if cfg.Tracker.BlockedRecovery.BreakerCooldownSeconds != 86400 {
		t.Fatalf("Tracker.BlockedRecovery.BreakerCooldownSeconds = %d, want 86400", cfg.Tracker.BlockedRecovery.BreakerCooldownSeconds)
	}
	if cfg.Tracker.BlockerAutoPromote.Enabled {
		t.Fatal("Tracker.BlockerAutoPromote.Enabled = true, want disabled by default")
	}
	if got := cfg.Tracker.BlockerAutoPromote.BlockerStates; !reflect.DeepEqual(got, []string{"backlog", "blocked", "human review"}) {
		t.Fatalf("Tracker.BlockerAutoPromote.BlockerStates = %#v, want backlog/blocked/human review", got)
	}
	if cfg.Tracker.BlockerAutoPromote.TargetState != "Todo" {
		t.Fatalf("Tracker.BlockerAutoPromote.TargetState = %q, want Todo", cfg.Tracker.BlockerAutoPromote.TargetState)
	}
	if cfg.Dependencies.Source != DependencySourceMerged {
		t.Fatalf("Dependencies.Source = %q, want %q", cfg.Dependencies.Source, DependencySourceMerged)
	}
	if cfg.Polling.IntervalMS != 120000 {
		t.Fatalf("Polling.IntervalMS = %d", cfg.Polling.IntervalMS)
	}
	if !cfg.Polling.Conditional {
		t.Fatal("Polling.Conditional = false, want true by default")
	}
	if cfg.Tracker.HTTPMaxIdleConns != 100 {
		t.Fatalf("Tracker.HTTPMaxIdleConns = %d, want 100", cfg.Tracker.HTTPMaxIdleConns)
	}
	if cfg.Tracker.HTTPMaxIdleConnsPerHost != 32 {
		t.Fatalf("Tracker.HTTPMaxIdleConnsPerHost = %d, want 32", cfg.Tracker.HTTPMaxIdleConnsPerHost)
	}
	if cfg.Tracker.HTTPIdleConnTimeoutMS != 90000 {
		t.Fatalf("Tracker.HTTPIdleConnTimeoutMS = %d, want 90000", cfg.Tracker.HTTPIdleConnTimeoutMS)
	}
	if cfg.Tracker.GitHubGraphQLMinReserve != 1000 {
		t.Fatalf("Tracker.GitHubGraphQLMinReserve = %d, want 1000", cfg.Tracker.GitHubGraphQLMinReserve)
	}
	if cfg.Tracker.GitHubRESTMinReserve != 1000 {
		t.Fatalf("Tracker.GitHubRESTMinReserve = %d, want 1000", cfg.Tracker.GitHubRESTMinReserve)
	}
	if cfg.Tracker.GitHubRESTFanoutMaxRequests != 80 {
		t.Fatalf("Tracker.GitHubRESTFanoutMaxRequests = %d, want 80", cfg.Tracker.GitHubRESTFanoutMaxRequests)
	}
	if cfg.Tracker.GitHubRESTDebugLogging {
		t.Fatal("Tracker.GitHubRESTDebugLogging = true, want false by default")
	}
	if cfg.Workspace.AutoBranch != true {
		t.Fatal("Workspace.AutoBranch = false, want true")
	}
	if cfg.Workspace.CacheStrategy != WorkspaceCacheIsolated {
		t.Fatalf("Workspace.CacheStrategy = %q, want %q", cfg.Workspace.CacheStrategy, WorkspaceCacheIsolated)
	}
	if cfg.Workspace.CleanupIdleTTLMS != 86400000 {
		t.Fatalf("Workspace.CleanupIdleTTLMS = %d, want 86400000", cfg.Workspace.CleanupIdleTTLMS)
	}
	if cfg.Workspace.CleanupSweepIntervalMS != 600000 {
		t.Fatalf("Workspace.CleanupSweepIntervalMS = %d, want 600000", cfg.Workspace.CleanupSweepIntervalMS)
	}
	if cfg.Agent.MaxConcurrentAgents != 10 {
		t.Fatalf("Agent.MaxConcurrentAgents = %d, want 10", cfg.Agent.MaxConcurrentAgents)
	}
	if cfg.Agent.MergeWorkerStartupTimeoutMS != DefaultMergeWorkerStartupTimeoutMS {
		t.Fatalf("Agent.MergeWorkerStartupTimeoutMS = %d, want %d", cfg.Agent.MergeWorkerStartupTimeoutMS, DefaultMergeWorkerStartupTimeoutMS)
	}
	if cfg.Agent.MergeWorkerMaxDurationMS != DefaultMergeWorkerMaxDurationMS {
		t.Fatalf("Agent.MergeWorkerMaxDurationMS = %d, want %d", cfg.Agent.MergeWorkerMaxDurationMS, DefaultMergeWorkerMaxDurationMS)
	}
	if cfg.Agent.MergeFallbackMaxDurationMS != DefaultMergeFallbackMaxDurationMS {
		t.Fatalf("Agent.MergeFallbackMaxDurationMS = %d, want %d", cfg.Agent.MergeFallbackMaxDurationMS, DefaultMergeFallbackMaxDurationMS)
	}
	if cfg.Agent.MaxSessionDurationMS != DefaultMaxSessionDurationMS {
		t.Fatalf("Agent.MaxSessionDurationMS = %d, want %d", cfg.Agent.MaxSessionDurationMS, DefaultMaxSessionDurationMS)
	}
	if cfg.Agent.NoProgressTimeoutMS != DefaultNoProgressTimeoutMS {
		t.Fatalf("Agent.NoProgressTimeoutMS = %d, want %d", cfg.Agent.NoProgressTimeoutMS, DefaultNoProgressTimeoutMS)
	}
	if cfg.Agent.MaxSessionTokens != 0 {
		t.Fatalf("Agent.MaxSessionTokens = %d, want disabled default", cfg.Agent.MaxSessionTokens)
	}
	if cfg.Agent.MaxSessionContextMultiplier != 0 {
		t.Fatalf("Agent.MaxSessionContextMultiplier = %v, want disabled default", cfg.Agent.MaxSessionContextMultiplier)
	}
	if !cfg.Agent.MergeFastPath.Enabled {
		t.Fatal("Agent.MergeFastPath.Enabled = false, want true default")
	}
	if cfg.Agent.MergeFastPath.FairnessAgeSeconds != DefaultMergeFairnessAgeSeconds {
		t.Fatalf("Agent.MergeFastPath.FairnessAgeSeconds = %d, want %d", cfg.Agent.MergeFastPath.FairnessAgeSeconds, DefaultMergeFairnessAgeSeconds)
	}
	if !cfg.Agent.ExperimentalThreadResume {
		t.Fatal("Agent.ExperimentalThreadResume = false, want enabled default")
	}
	if !cfg.Agent.ResumeOrphanedSessions {
		t.Fatal("Agent.ResumeOrphanedSessions = false, want enabled default")
	}
	if cfg.Agent.Shutdown.DrainTimeoutMS != DefaultShutdownDrainTimeoutMS {
		t.Fatalf("Agent.Shutdown.DrainTimeoutMS = %d, want %d", cfg.Agent.Shutdown.DrainTimeoutMS, DefaultShutdownDrainTimeoutMS)
	}
	if cfg.Agent.Lessons.Path != ".detent/lessons.md" {
		t.Fatalf("Agent.Lessons.Path = %q", cfg.Agent.Lessons.Path)
	}
	if cfg.Agent.Skills.Path != ".detent/skills" {
		t.Fatalf("Agent.Skills.Path = %q", cfg.Agent.Skills.Path)
	}
	if !cfg.Agent.Knowledge.Enabled {
		t.Fatal("Agent.Knowledge.Enabled = false, want true default")
	}
	if cfg.Agent.Knowledge.MaxBytes != DefaultKnowledgeMaxBytes {
		t.Fatalf("Agent.Knowledge.MaxBytes = %d, want %d", cfg.Agent.Knowledge.MaxBytes, DefaultKnowledgeMaxBytes)
	}
	if len(cfg.Agent.Knowledge.Sources) != 0 {
		t.Fatalf("Agent.Knowledge.Sources = %#v, want empty default", cfg.Agent.Knowledge.Sources)
	}
	if !cfg.Agent.Skills.Creation.Enabled {
		t.Fatal("Agent.Skills.Creation.Enabled = false, want true default")
	}
	if cfg.Agent.Skills.Creation.MaxDraftsPerRun != 1 {
		t.Fatalf("Agent.Skills.Creation.MaxDraftsPerRun = %d, want 1", cfg.Agent.Skills.Creation.MaxDraftsPerRun)
	}
	if !cfg.Agent.Followups.Enabled {
		t.Fatal("Agent.Followups.Enabled = false, want true default")
	}
	if cfg.Workpad.StructuredOnly {
		t.Fatal("Workpad.StructuredOnly = true, want false default")
	}
	if cfg.Agent.AutoPromote.ReworkLimit != DefaultReworkLimit {
		t.Fatalf("Agent.AutoPromote.ReworkLimit = %d, want %d", cfg.Agent.AutoPromote.ReworkLimit, DefaultReworkLimit)
	}
	if cfg.Agent.AutoPromote.GateWaitState != AutoPromoteGateWaitStateSource {
		t.Fatalf("Agent.AutoPromote.GateWaitState = %q, want source", cfg.Agent.AutoPromote.GateWaitState)
	}
	if cfg.Agent.AutoPromote.GateWaitTimeoutSeconds != DefaultAutoPromoteGateWaitTimeoutSeconds {
		t.Fatalf("Agent.AutoPromote.GateWaitTimeoutSeconds = %d, want %d", cfg.Agent.AutoPromote.GateWaitTimeoutSeconds, DefaultAutoPromoteGateWaitTimeoutSeconds)
	}
	if cfg.Agent.AutoPromote.NoProgressLimit != DefaultNoProgressLimit {
		t.Fatalf("Agent.AutoPromote.NoProgressLimit = %d, want %d", cfg.Agent.AutoPromote.NoProgressLimit, DefaultNoProgressLimit)
	}
	if cfg.Agent.OutputTruncation.MaxBytes != 0 {
		t.Fatalf("Agent.OutputTruncation.MaxBytes = %d, want disabled default", cfg.Agent.OutputTruncation.MaxBytes)
	}
	if !cfg.Codex.ApprovalPolicy.IsMap {
		t.Fatalf("Codex.ApprovalPolicy = %#v, want map default", cfg.Codex.ApprovalPolicy)
	}
	if strings.TrimSpace(cfg.Codex.Shell) == "" {
		t.Fatal("Codex.Shell is blank, want per-OS default")
	}
	if strings.TrimSpace(cfg.Hooks.Shell) == "" {
		t.Fatal("Hooks.Shell is blank, want per-OS default")
	}
	if cfg.Server.Host != "127.0.0.1" {
		t.Fatalf("Server.Host = %q", cfg.Server.Host)
	}
	if !cfg.Observability.DashboardEnabled {
		t.Fatal("Observability.DashboardEnabled = false, want true")
	}
	if cfg.Observability.StrandedActiveThresholdSeconds != DefaultStrandedActiveThresholdSeconds {
		t.Fatalf("Observability.StrandedActiveThresholdSeconds = %d, want %d", cfg.Observability.StrandedActiveThresholdSeconds, DefaultStrandedActiveThresholdSeconds)
	}
	if cfg.Observability.DispatchStallThresholdSeconds != DefaultDispatchStallThresholdSeconds {
		t.Fatalf("Observability.DispatchStallThresholdSeconds = %d, want %d", cfg.Observability.DispatchStallThresholdSeconds, DefaultDispatchStallThresholdSeconds)
	}
	if cfg.Observability.ParkReviewThreshold != DefaultParkReviewThreshold {
		t.Fatalf("Observability.ParkReviewThreshold = %d, want %d", cfg.Observability.ParkReviewThreshold, DefaultParkReviewThreshold)
	}
	if cfg.Observability.Efficiency.AnomalyTokensMultiple != 3 || cfg.Observability.Efficiency.AnomalySessionsMultiple != 3 || cfg.Observability.Efficiency.AnomalyDwellMultiple != 3 {
		t.Fatalf("Observability.Efficiency = %#v, want 3x defaults", cfg.Observability.Efficiency)
	}
	if cfg.Observability.OTLP.Endpoint != "" || cfg.Observability.OTLP.ServiceName != "detent" || cfg.Observability.OTLP.TimeoutMS != 5000 {
		t.Fatalf("Observability.OTLP = %#v, want disabled endpoint and defaults", cfg.Observability.OTLP)
	}
	if cfg.Budget.PricingPath != "priv/pricing/models.yaml" {
		t.Fatalf("Budget.PricingPath = %q", cfg.Budget.PricingPath)
	}
	if len(cfg.Agents.Backends) != 0 {
		t.Fatalf("Agents.Backends len = %d, want legacy empty config", len(cfg.Agents.Backends))
	}
	if len(cfg.Agents.Routes) != 0 {
		t.Fatalf("Agents.Routes len = %d, want legacy empty config", len(cfg.Agents.Routes))
	}
	if len(cfg.Agent.DispatchPriorityByLabel) != 0 {
		t.Fatalf("Agent.DispatchPriorityByLabel = %#v, want empty default", cfg.Agent.DispatchPriorityByLabel)
	}
	if !cfg.Agent.PrioritizeUnblockers {
		t.Fatal("Agent.PrioritizeUnblockers = false, want true default")
	}
	if cfg.Gate.Kind != gate.KindCommand || cfg.Gate.Run != gate.DefaultCommand {
		t.Fatalf("Gate = %#v, want default command gate", cfg.Gate)
	}
	if cfg.Plan.Enabled || cfg.Plan.Review != gate.PlanReviewHuman || cfg.Plan.ApprovalLabel != gate.DefaultPlanApprovalLabel || cfg.Plan.Stop != gate.DefaultPlanStop {
		t.Fatalf("Plan = %#v, want disabled human review plan default", cfg.Plan)
	}
}

func TestParseWorkflowCanDisableOrphanedSessionResume(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte("---\ntracker:\n  kind: memory\nagent:\n  resume_orphaned_sessions: false\n---\nPrompt\n"))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if workflow.Config.Agent.ResumeOrphanedSessions {
		t.Fatal("Agent.ResumeOrphanedSessions = true, want explicitly disabled")
	}
}

func TestParseWorkflowMarksAgentKnowledgeConfigured(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
agent:
  knowledge:
    max_bytes: 2048
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	if !workflow.Config.Agent.Knowledge.Configured {
		t.Fatal("Agent.Knowledge.Configured = false, want true")
	}
	if workflow.Config.Agent.Knowledge.MaxBytes != 2048 {
		t.Fatalf("Agent.Knowledge.MaxBytes = %d, want 2048", workflow.Config.Agent.Knowledge.MaxBytes)
	}
}

func TestConfiguredSubsettingsTracksExplicitFrontmatterLeaves(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
budget:
  enabled: false
  per_issue_max_usd: 12
agent:
  skills:
    enabled: false
  lessons:
    enabled: false
    recall_n: 4
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	tests := []struct {
		prefix string
		want   []string
	}{
		{prefix: "budget", want: []string{"budget.per_issue_max_usd"}},
		{prefix: "agent.skills"},
		{prefix: "agent.lessons", want: []string{"agent.lessons.recall_n"}},
		{prefix: "gate.validator"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			t.Parallel()
			got := workflow.Config.ConfiguredSubsettings(tt.prefix)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ConfiguredSubsettings(%q) = %#v, want %#v", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestConfiguredSubsettingsTracksAliasedFrontmatterLeaves(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
shared_skills: &disabled_skills
  enabled: false
  path: custom-skills
agent:
  skills: *disabled_skills
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	got := workflow.Config.ConfiguredSubsettings("agent.skills")
	want := []string{"agent.skills.path"}
	if !slices.Equal(got, want) {
		t.Fatalf("ConfiguredSubsettings(agent.skills) = %#v, want %#v", got, want)
	}
}

func TestConfiguredSubsettingsTracksScalarAliasLeaves(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
shared_skill_path: &skill_path custom-skills
agent:
  skills:
    enabled: false
    path: *skill_path
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	got := workflow.Config.ConfiguredSubsettings("agent.skills")
	want := []string{"agent.skills.path"}
	if !slices.Equal(got, want) {
		t.Fatalf("ConfiguredSubsettings(agent.skills) = %#v, want %#v", got, want)
	}
}

func TestConfiguredSubsettingsTracksMergedFrontmatterLeaves(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
shared_agent: &shared_agent
  skills:
    enabled: false
    path: merged-skills
agent:
  <<: *shared_agent
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	got := workflow.Config.ConfiguredSubsettings("agent.skills")
	want := []string{"agent.skills.path"}
	if !slices.Equal(got, want) {
		t.Fatalf("ConfiguredSubsettings(agent.skills) = %#v, want %#v", got, want)
	}
}

func TestKnowledgeWithSourcesAllowsSourceLessMaxBytesOverride(t *testing.T) {
	t.Parallel()

	workflowDefault := defaultKnowledge()
	got := KnowledgeWithSources(
		Knowledge{
			Enabled:  true,
			MaxBytes: 4096,
			Sources: []KnowledgeSource{{
				Name: "Global",
				Path: "global.md",
			}},
		},
		Knowledge{
			Enabled:    true,
			MaxBytes:   1024,
			Configured: true,
		},
		workflowDefault,
	)

	if got.MaxBytes != 1024 {
		t.Fatalf("MaxBytes = %d, want 1024", got.MaxBytes)
	}
	if len(got.Sources) != 1 || got.Sources[0].Path != "global.md" {
		t.Fatalf("Sources = %#v, want inherited global source", got.Sources)
	}
}

func TestValidatePlanRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Plan.Enabled = true
	cfg.Plan.Review = "committee"
	cfg.Plan.Stop = " "

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want plan validation errors")
	}
	for _, want := range []string{
		"plan.review must be one of human, automated, both",
		"plan.stop must not be blank when plan.enabled is true",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate() error = %v, want %q", err, want)
		}
	}
}

func TestParseWorkflowAgentMergeFastPath(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name               string
		enabled            string
		fairnessAgeSeconds int
		want               bool
	}{
		{name: "enabled", enabled: "true", fairnessAgeSeconds: 5400, want: true},
		{name: "disabled", enabled: "false", fairnessAgeSeconds: 900, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := ParseWorkflow([]byte(`---
agent:
  merge_fast_path:
    enabled: ` + tt.enabled + `
    fairness_age_seconds: ` + strconv.Itoa(tt.fairnessAgeSeconds) + `
---
Body
`))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if workflow.Config.Agent.MergeFastPath.Enabled != tt.want {
				t.Fatalf("Agent.MergeFastPath.Enabled = %t, want %t", workflow.Config.Agent.MergeFastPath.Enabled, tt.want)
			}
			if workflow.Config.Agent.MergeFastPath.FairnessAgeSeconds != tt.fairnessAgeSeconds {
				t.Fatalf("Agent.MergeFastPath.FairnessAgeSeconds = %d, want %d", workflow.Config.Agent.MergeFastPath.FairnessAgeSeconds, tt.fairnessAgeSeconds)
			}
		})
	}
}

func TestValidateMergeFastPathRejectsNegativeFairnessAge(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Agent.MergeFastPath.FairnessAgeSeconds = -1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "agent.merge_fast_path.fairness_age_seconds must be greater than 0") {
		t.Fatalf("Validate() error = %v, want fairness age validation", err)
	}
}

func TestParseWorkflowAgentOutputTruncation(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
agent:
  output_truncation:
    max_bytes: 4096
---
Body
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if got := workflow.Config.Agent.OutputTruncation.MaxBytes; got != 4096 {
		t.Fatalf("Agent.OutputTruncation.MaxBytes = %d, want 4096", got)
	}
}

func TestKanbanTransitionPolicyRestrictsActiveExecutionDefaults(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework", "Merging"}
	cfg.Tracker.ObservedStates = []string{"Backlog", "Blocked", "Human Review"}
	cfg.Tracker.TerminalStates = []string{"Done", "Cancelled"}

	tests := []struct {
		name   string
		source string
		target string
		want   bool
	}{
		{
			name:   "todo can move into execution",
			source: "Todo",
			target: "In Progress",
			want:   true,
		},
		{
			name:   "in progress can move to blocked",
			source: "In Progress",
			target: "Blocked",
			want:   true,
		},
		{
			name:   "in progress cannot bypass review",
			source: "In Progress",
			target: "Human Review",
			want:   false,
		},
		{
			name:   "rework cannot move straight to done",
			source: "Rework",
			target: "Done",
			want:   false,
		},
		{
			name:   "merging can move to cancelled",
			source: "Merging",
			target: "Cancelled",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := cfg.KanbanTransitionAllowed(tt.source, tt.target); got != tt.want {
				t.Fatalf("KanbanTransitionAllowed(%q, %q) = %v, want %v", tt.source, tt.target, got, tt.want)
			}
		})
	}
}

func TestKanbanTransitionPolicyAllowsConfiguredOverrides(t *testing.T) {
	t.Parallel()

	cfg := Default()
	if cfg.Server.Kanban.ShowBlockedAlerts {
		t.Fatal("Default Server.Kanban.ShowBlockedAlerts = true, want false")
	}
	cfg.Tracker.ActiveStates = []string{"Todo", "In Progress", "Rework", "Merging"}
	cfg.Tracker.ObservedStates = []string{"Backlog", "Blocked", "Human Review"}
	cfg.Tracker.TerminalStates = []string{"Done", "Cancelled"}
	cfg.Server.Kanban.AllowedTransitions = map[string][]string{
		"In Progress": {"Blocked", "Human Review"},
	}
	cfg.Server.Kanban.Normalize()

	if !cfg.KanbanTransitionAllowed("In Progress", "Human Review") {
		t.Fatal("KanbanTransitionAllowed(In Progress, Human Review) = false, want configured override")
	}
	if cfg.KanbanTransitionAllowed("In Progress", "Done") {
		t.Fatal("KanbanTransitionAllowed(In Progress, Done) = true, want configured source allowlist")
	}
}

func TestServerBoardSnapshotStaleness(t *testing.T) {
	t.Parallel()

	if got := Default().Server.BoardSnapshotStaleAfterSeconds; got != DefaultBoardSnapshotStaleAfterSeconds {
		t.Fatalf("Default Server.BoardSnapshotStaleAfterSeconds = %d, want %d", got, DefaultBoardSnapshotStaleAfterSeconds)
	}
	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
server:
  board_snapshot_stale_after_seconds: 120
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if got := workflow.Config.Server.BoardSnapshotStaleAfterSeconds; got != 120 {
		t.Fatalf("Server.BoardSnapshotStaleAfterSeconds = %d, want 120", got)
	}
	workflow.Config.Server.BoardSnapshotStaleAfterSeconds = -1
	if err := workflow.Config.Validate(); err == nil || !strings.Contains(err.Error(), "server.board_snapshot_stale_after_seconds must be greater than 0") {
		t.Fatalf("Validate() error = %v, want board snapshot staleness validation", err)
	}
}

func TestDefaultStalenessObservability(t *testing.T) {
	t.Parallel()
	cfg := Default()
	if !cfg.Observability.Staleness.Enabled {
		t.Fatal("Observability.Staleness.Enabled = false, want true")
	}
	if cfg.Observability.Staleness.NoCompletionHours != DefaultStalenessNoCompletionHours {
		t.Fatalf("NoCompletionHours = %d, want %d", cfg.Observability.Staleness.NoCompletionHours, DefaultStalenessNoCompletionHours)
	}
	if cfg.Observability.Staleness.HumanGateRearmHours != DefaultStalenessHumanGateRearmHours {
		t.Fatalf("HumanGateRearmHours = %d, want %d", cfg.Observability.Staleness.HumanGateRearmHours, DefaultStalenessHumanGateRearmHours)
	}
	if cfg.Observability.Staleness.LaneReentryWindowHours != DefaultStalenessLaneReentryWindowHours {
		t.Fatalf("LaneReentryWindowHours = %d, want %d", cfg.Observability.Staleness.LaneReentryWindowHours, DefaultStalenessLaneReentryWindowHours)
	}
	if len(cfg.Observability.Staleness.Lanes) != 3 {
		t.Fatalf("Staleness.Lanes = %#v, want three defaults", cfg.Observability.Staleness.Lanes)
	}
	if !cfg.Observability.Staleness.Lanes[0].HumanGate {
		t.Fatal("Human Review default is not marked as a human gate")
	}
	wantReasons := []string{
		"already_running",
		"blocked_by_dependency",
		"github_rest_capacity_paused",
		"github_rest_recovery",
		"global_capacity_full",
		"outside_active_window",
		"provider_rate_window_backpressure",
		"reserved_for_higher_priority_project",
	}
	if got := cfg.Observability.Staleness.RepeatedDecisionBenignReasons; !slices.Equal(got, wantReasons) {
		t.Fatalf("RepeatedDecisionBenignReasons = %#v, want %#v", got, wantReasons)
	}

	cfg.Observability.Staleness.RepeatedDecisionBenignReasons = []string{
		" Planned_Maintenance ",
		"planned_maintenance",
		"",
	}
	cfg.Observability.Staleness.Normalize()
	if got, want := cfg.Observability.Staleness.RepeatedDecisionBenignReasons, []string{"planned_maintenance"}; !slices.Equal(got, want) {
		t.Fatalf("normalized RepeatedDecisionBenignReasons = %#v, want %#v", got, want)
	}
	cfg = Default()
	cfg.Observability.Staleness.HumanGateRearmHours = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "observability.staleness.human_gate_rearm_hours must be greater than 0") {
		t.Fatalf("Validate() error = %v, want human gate rearm validation", err)
	}
	cfg = Default()
	cfg.Observability.Staleness.LaneReentryWindowHours = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "observability.staleness.lane_reentry_window_hours must be greater than 0") {
		t.Fatalf("Validate() error = %v, want lane reentry window validation", err)
	}
}

func TestStalenessRepeatedDecisionBenignReasonsOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
		want []string
	}{
		{
			name: "project override replaces defaults",
			yaml: `---
tracker:
  kind: memory
observability:
  staleness:
    repeated_decision_benign_reasons:
      - authorization_selector_declined
      - provider_rate_window_backpressure
---
Prompt
`,
			want: []string{"authorization_selector_declined", "provider_rate_window_backpressure"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow([]byte(tt.yaml))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if got := workflow.Config.Observability.Staleness.RepeatedDecisionBenignReasons; !slices.Equal(got, tt.want) {
				t.Fatalf("RepeatedDecisionBenignReasons = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestStalenessRepeatedDecisionBenignReasonsRejectUnknownSchedulerReason(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.Kind = TrackerMemory
	cfg.Observability.Staleness.RepeatedDecisionBenignReasons = []string{"unauthorized"}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want scheduler reason mismatch")
	}
	for _, want := range []string{
		`observability.staleness.repeated_decision_benign_reasons contains "unauthorized"`,
		`authorization_selector_declined`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate() error = %q, want %q", err, want)
		}
	}
}

func TestKanbanTransitionPolicyAllowsConfiguredOverridesWithoutStateLists(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Tracker.ActiveStates = nil
	cfg.Tracker.ObservedStates = nil
	cfg.Tracker.TerminalStates = nil
	cfg.Server.Kanban.AllowedTransitions = map[string][]string{
		"In Progress": {"Blocked"},
	}
	cfg.Server.Kanban.Normalize()

	if got, want := cfg.KanbanAllowedTransitionTargets("In Progress"), []string{"Blocked"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("KanbanAllowedTransitionTargets(In Progress) = %#v, want %#v", got, want)
	}
	if !cfg.KanbanTransitionAllowed("In Progress", "Blocked") {
		t.Fatal("KanbanTransitionAllowed(In Progress, Blocked) = false, want explicit override")
	}
	if cfg.KanbanTransitionAllowed("In Progress", "Human Review") {
		t.Fatal("KanbanTransitionAllowed(In Progress, Human Review) = true, want configured source allowlist")
	}
}

func TestValidateKanbanTransitionsRejectsBlankNames(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Server.Kanban.AllowedTransitions = map[string][]string{
		"":            {"Blocked"},
		"In Progress": {""},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want transition validation errors")
	}
	for _, want := range []string{
		"server.kanban.allowed_transitions source states must not be blank",
		"server.kanban.allowed_transitions target states must not be blank",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate() error = %v, want %q", err, want)
		}
	}
}

func TestAgentDispatchPriorityByLabelYAMLRoundTrip(t *testing.T) {
	t.Parallel()

	want := []string{"bug", "regression", "enhancement"}
	raw, err := yaml.Marshal(Agent{DispatchPriorityByLabel: want})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	var got Agent
	if err := yaml.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(got.DispatchPriorityByLabel, want) {
		t.Fatalf("DispatchPriorityByLabel = %#v, want %#v", got.DispatchPriorityByLabel, want)
	}
}

func TestParseWorkflowAgentsConfig(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
agents:
  backends:
    - id: codex-high
      kind: codex
      protocol: app-server
      command: codex app-server --profile high
      options:
        shell: bash
        approval_policy: never
        thread_sandbox: danger-full-access
        turn_sandbox_policy:
          type: dangerFullAccess
        turn_timeout_ms: 600000
        read_timeout_ms: 1000
        stall_timeout_ms: 0
  routes:
    - name: high-label
      backend: codex-high
      model: gpt-5-codex-high
      selector:
        labels:
          include:
            - tier:high
    - name: project-model
      backend: codex-high
      model_field: Model
    - name: urgent
      backend: codex-high
      model: gpt-5-codex
      selector:
        priority_in:
          - 1
    - name: default
      backend: codex-high
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	agents := workflow.Config.Agents
	if len(agents.Backends) != 1 {
		t.Fatalf("Agents.Backends len = %d, want 1", len(agents.Backends))
	}
	backend := agents.Backends[0]
	if backend.ID != "codex-high" || backend.Kind != "codex" || backend.Protocol != "app-server" {
		t.Fatalf("backend identity = %#v, want codex-high codex app-server", backend)
	}
	if backend.Command != "codex app-server --profile high" {
		t.Fatalf("backend Command = %q, want configured command", backend.Command)
	}
	options := backend.CodexOptions()
	if options.Shell != "bash" {
		t.Fatalf("backend shell = %q, want bash", options.Shell)
	}
	if !options.ApprovalPolicy.IsString || options.ApprovalPolicy.String != "never" {
		t.Fatalf("backend approval policy = %#v, want never", options.ApprovalPolicy)
	}
	if options.TurnSandboxPolicy["type"] != "dangerFullAccess" {
		t.Fatalf("backend turn sandbox policy = %#v, want dangerFullAccess", options.TurnSandboxPolicy)
	}
	var decoded CodexOptions
	if err := backend.Options.Decode(&decoded); err != nil {
		t.Fatalf("backend options Decode() error = %v", err)
	}
	if decoded.ReadTimeoutMS != 1000 {
		t.Fatalf("decoded read timeout = %d, want 1000", decoded.ReadTimeoutMS)
	}
	if len(agents.Routes) != 4 {
		t.Fatalf("Agents.Routes len = %d, want 4", len(agents.Routes))
	}
	if got := agents.Routes[0].Selector.Labels.Include; len(got) != 1 || got[0] != "tier:high" {
		t.Fatalf("route label selector = %#v, want tier:high", got)
	}
	if agents.Routes[1].ModelField != "Model" {
		t.Fatalf("route ModelField = %q, want Model", agents.Routes[1].ModelField)
	}
	if got := agents.Routes[2].Selector.PriorityIn; len(got) != 1 || got[0] != 1 {
		t.Fatalf("route priority selector = %#v, want priority 1", got)
	}
	if !agents.Routes[3].Default {
		t.Fatal("default route Default = false, want true")
	}
}

func TestParseWorkflowClaudeCodeAgentBackendConfig(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
agents:
  backends:
    - id: claude-worker
      kind: claude_code
      provider: local_ollama
      options:
        effort: ULTRACODE
        allowed_tools:
          - Bash
          - Edit
        disallowed_tools:
          - WebFetch
        include_partial_messages: true
        turn_timeout_ms: 600000
        stall_timeout_ms: 0
        shell: bash
        extra_args:
          - --model
          - claude-sonnet-4
  routes:
    - backend: claude-worker
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	backends := workflow.Config.AgentBackendConfigs()
	if len(backends) != 1 {
		t.Fatalf("AgentBackendConfigs() len = %d, want 1", len(backends))
	}
	backend := backends[0]
	if backend.ID != "claude-worker" || backend.Kind != AgentBackendClaudeCode || backend.Protocol != "headless" {
		t.Fatalf("backend identity = %#v, want claude-worker claude_code headless", backend)
	}
	if backend.Command != "claude" {
		t.Fatalf("backend Command = %q, want default claude", backend.Command)
	}
	if backend.Provider != "local_ollama" {
		t.Fatalf("backend Provider = %q, want local_ollama", backend.Provider)
	}

	options := backend.ClaudeCodeOptions()
	if options.PermissionMode != "bypassPermissions" {
		t.Fatalf("permission mode = %q, want bypassPermissions", options.PermissionMode)
	}
	if options.Effort != "ultracode" {
		t.Fatalf("effort = %q, want normalized ultracode", options.Effort)
	}
	if !reflect.DeepEqual(options.AllowedTools, []string{"Bash", "Edit"}) {
		t.Fatalf("allowed tools = %#v, want Bash/Edit", options.AllowedTools)
	}
	if !reflect.DeepEqual(options.DisallowedTools, []string{"WebFetch"}) {
		t.Fatalf("disallowed tools = %#v, want WebFetch", options.DisallowedTools)
	}
	if !options.IncludePartialMessages {
		t.Fatal("include partial messages = false, want true")
	}
	if options.TurnTimeoutMS != 600000 {
		t.Fatalf("turn timeout = %d, want 600000", options.TurnTimeoutMS)
	}
	if options.StallTimeoutMS != 0 {
		t.Fatalf("stall timeout = %d, want 0", options.StallTimeoutMS)
	}
	if options.Shell != "bash" {
		t.Fatalf("shell = %q, want bash", options.Shell)
	}
	if !reflect.DeepEqual(options.ExtraArgs, []string{"--model", "claude-sonnet-4"}) {
		t.Fatalf("extra args = %#v, want model args", options.ExtraArgs)
	}
}

func TestParseWorkflowAgentRoleEffort(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
agent:
  effort:
    code: xhigh
    rework: medium
    merge: high
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []struct {
		name       string
		role       string
		wantEffort string
		wantField  string
	}{
		{name: "code", role: "code", wantEffort: "xhigh", wantField: "agent.effort.code"},
		{name: "rework", role: "rework", wantEffort: "medium", wantField: "agent.effort.rework"},
		{name: "merge", role: "merge", wantEffort: "high", wantField: "agent.effort.merge"},
		{name: "unconfigured role", role: "plan"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			effort, field := workflow.Config.Agent.Effort.Resolve(tt.role)
			if effort != tt.wantEffort || field != tt.wantField {
				t.Fatalf("Resolve(%q) = %q, %q; want %q, %q", tt.role, effort, field, tt.wantEffort, tt.wantField)
			}
		})
	}
}

func TestAgentRoleEffortReworkInheritsCode(t *testing.T) {
	t.Parallel()

	effort, field := (AgentRoleEffort{Code: "high"}).Resolve("rework")
	if effort != "high" || field != "agent.effort.code" {
		t.Fatalf("Resolve(rework) = %q, %q; want high, agent.effort.code", effort, field)
	}
}

func TestAgentRoleEffortValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		effort      AgentRoleEffort
		wantProblem string
	}{
		{name: "supported values", effort: AgentRoleEffort{Code: "xhigh", Rework: "medium", Merge: "high"}},
		{name: "invalid merge value", effort: AgentRoleEffort{Merge: "extreme"}, wantProblem: "agent.effort.merge must be one of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var problems []string
			tt.effort.validate("agent.effort", &problems)
			if tt.wantProblem == "" {
				if len(problems) != 0 {
					t.Fatalf("problems = %#v, want none", problems)
				}
				return
			}
			if len(problems) != 1 || !strings.Contains(problems[0], tt.wantProblem) {
				t.Fatalf("problems = %#v, want one containing %q", problems, tt.wantProblem)
			}
		})
	}
}

func TestAgentBackendConfigsMergesLegacyCodexDefaults(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
codex:
  command: codex app-server
  shell: bash
  approval_policy:
    reject:
      sandbox_approval: true
  thread_sandbox: workspace-write
  turn_sandbox_policy:
    type: workspaceWrite
  turn_timeout_ms: 700000
  read_timeout_ms: 7000
  stall_timeout_ms: 70000
agents:
  backends:
    - id: codex-custom
      kind: codex
      protocol: app-server
      command: codex app-server --profile custom
      options:
        approval_policy: never
        read_timeout_ms: 1000
        stall_timeout_ms: 0
  routes:
    - backend: codex-custom
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	backends := workflow.Config.AgentBackendConfigs()
	if len(backends) != 1 {
		t.Fatalf("AgentBackendConfigs() len = %d, want 1", len(backends))
	}
	backend := backends[0]
	if backend.ID != "codex-custom" {
		t.Fatalf("backend ID = %q, want codex-custom", backend.ID)
	}
	if backend.Command != "codex app-server --profile custom" {
		t.Fatalf("backend Command = %q, want configured command", backend.Command)
	}
	options := backend.CodexOptions()
	if options.Shell != "bash" {
		t.Fatalf("backend shell = %q, want legacy default", options.Shell)
	}
	if !options.ApprovalPolicy.IsString || options.ApprovalPolicy.String != "never" {
		t.Fatalf("backend approval policy = %#v, want backend override", options.ApprovalPolicy)
	}
	if options.ThreadSandbox != "workspace-write" {
		t.Fatalf("backend thread sandbox = %q, want legacy default", options.ThreadSandbox)
	}
	if got := options.TurnSandboxPolicy["type"]; got != "workspaceWrite" {
		t.Fatalf("backend turn sandbox policy type = %v, want workspaceWrite", got)
	}
	if options.TurnTimeoutMS != 700000 {
		t.Fatalf("backend turn timeout = %d, want legacy default", options.TurnTimeoutMS)
	}
	if options.ReadTimeoutMS != 1000 {
		t.Fatalf("backend read timeout = %d, want backend override", options.ReadTimeoutMS)
	}
	if options.StallTimeoutMS != 70000 {
		t.Fatalf("backend stall timeout = %d, want legacy default for zero backend value", options.StallTimeoutMS)
	}
}

func TestParseWorkflowCommandGateDisablesAutomatedReviewRequirement(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  kind: command
  run: make check
  require_automated_review: false
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	cfg := workflow.Config.Gate
	if cfg.Kind != gate.KindCommand || cfg.Run != gate.DefaultCommand {
		t.Fatalf("Gate = %#v, want command make check", cfg)
	}
	if cfg.RequireAutomatedReview == nil {
		t.Fatal("Gate.RequireAutomatedReview = nil, want false")
	}
	if *cfg.RequireAutomatedReview {
		t.Fatal("Gate.RequireAutomatedReview = true, want false")
	}
}

func TestParseWorkflowAutomatedReviewModeAndTimeoutAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       string
		action     string
		wantAction string
	}{
		{name: "optional defaults to merge", mode: gate.AutomatedReviewOptional, wantAction: AutoPromoteGateWaitTimeoutActionMerge},
		{name: "required defaults to human review", mode: gate.AutomatedReviewRequired, wantAction: AutoPromoteGateWaitTimeoutActionHumanReview},
		{name: "optional can hold for human", mode: gate.AutomatedReviewOptional, action: AutoPromoteGateWaitTimeoutActionHumanReview, wantAction: AutoPromoteGateWaitTimeoutActionHumanReview},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw := fmt.Sprintf(`---
tracker:
  kind: memory
agent:
  auto_promote:
    gate_wait_timeout_action: %s
gate:
  kind: command
  automated_review: %s
---
Prompt
`, tt.action, tt.mode)
			workflow, err := ParseWorkflow([]byte(raw))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}
			if err := workflow.Config.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if got := workflow.Config.Gate.AutomatedReview; got != tt.mode {
				t.Fatalf("Gate.AutomatedReview = %q, want %q", got, tt.mode)
			}
			if got := workflow.Config.Agent.AutoPromote.GateWaitTimeoutAction; got != tt.wantAction {
				t.Fatalf("GateWaitTimeoutAction = %q, want %q", got, tt.wantAction)
			}
		})
	}
}

func TestParseWorkflowCommandGateCIFailureAction(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  kind: command
  run: make check
  ci_failure_action: rework
  transient_ci_retry_limit: 3
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	cfg := workflow.Config.Gate
	if cfg.CIFailureAction != gate.CIFailureActionRework {
		t.Fatalf("Gate.CIFailureAction = %q, want %q", cfg.CIFailureAction, gate.CIFailureActionRework)
	}
	if cfg.TransientCIRetryLimit == nil || *cfg.TransientCIRetryLimit != 3 {
		t.Fatalf("Gate.TransientCIRetryLimit = %v, want 3", cfg.TransientCIRetryLimit)
	}
}

func TestParseWorkflowGateRequiredStatusChecks(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  required_status_checks:
    - " Lint "
    - Windows Core
    - Lint
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	want := []string{"Lint", "Windows Core"}
	if !reflect.DeepEqual(workflow.Config.Gate.RequiredStatusChecks, want) {
		t.Fatalf("Gate.RequiredStatusChecks = %#v, want %#v", workflow.Config.Gate.RequiredStatusChecks, want)
	}
}

func TestParseWorkflowGateCITriggerLabel(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  ci_trigger_label: " CI:Ready "
  ci_trigger_label_stagger_seconds: 20
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if got := workflow.Config.Gate.CITriggerLabel; got != "ci:ready" {
		t.Fatalf("Gate.CITriggerLabel = %q, want ci:ready", got)
	}
	if got := workflow.Config.Gate.CITriggerLabelStaggerSeconds; got == nil || *got != 20 {
		t.Fatalf("Gate.CITriggerLabelStaggerSeconds = %v, want 20", got)
	}
}

func TestParseWorkflowGateCITriggerLabelRejectsZeroStagger(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  ci_trigger_label: ci:ready
  ci_trigger_label_stagger_seconds: 0
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err == nil || !strings.Contains(err.Error(), "gate.ci_trigger_label_stagger_seconds must be greater than 0") {
		t.Fatalf("Validate() error = %v, want positive stagger validation", err)
	}
}

func TestParseWorkflowGateTransientCIRetryLimitCanDisable(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  transient_ci_retry_limit: 0
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	cfg := workflow.Config.Gate
	if cfg.TransientCIRetryLimit == nil || *cfg.TransientCIRetryLimit != 0 {
		t.Fatalf("Gate.TransientCIRetryLimit = %v, want 0", cfg.TransientCIRetryLimit)
	}
}

func TestParseWorkflowGateValidatorConfig(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
gate:
  kind: command
  validator:
    enabled: true
    model: gpt-5-validator
    min_score: 0.85
    max_attempts: 4
    turn_timeout_ms: 120000
    max_inline_diff_bytes: 32768
    block_on:
      - P1
      - p2
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	validator := workflow.Config.Gate.Validator
	if !validator.Enabled {
		t.Fatal("Gate.Validator.Enabled = false, want true")
	}
	if validator.Model != "gpt-5-validator" {
		t.Fatalf("Gate.Validator.Model = %q, want gpt-5-validator", validator.Model)
	}
	if validator.MinScore != 0.85 {
		t.Fatalf("Gate.Validator.MinScore = %v, want 0.85", validator.MinScore)
	}
	if validator.MaxAttempts != 4 {
		t.Fatalf("Gate.Validator.MaxAttempts = %d, want 4", validator.MaxAttempts)
	}
	if validator.TurnTimeoutMS != 120000 {
		t.Fatalf("Gate.Validator.TurnTimeoutMS = %d, want 120000", validator.TurnTimeoutMS)
	}
	if validator.MaxInlineDiffBytes == nil || *validator.MaxInlineDiffBytes != 32768 {
		t.Fatalf("Gate.Validator.MaxInlineDiffBytes = %v, want 32768", validator.MaxInlineDiffBytes)
	}
	if got := validator.BlockOn; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
		t.Fatalf("Gate.Validator.BlockOn = %#v, want p1/p2", got)
	}
}

func TestParseWorkflowGateSecurityAuditConfig(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: github_local
  api_key: ghp_example
  repository: digitaldrywood/detent
  local_sqlite:
    path: .detent/work-items.db
gate:
  kind: command
  security_audit:
    enabled: true
    model: gpt-5-security
    max_attempts: 4
    turn_timeout_ms: 180000
    max_diff_bytes: 131072
    block_on: [p1, p2]
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	audit := workflow.Config.Gate.SecurityAudit
	if !audit.Enabled || audit.Model != "gpt-5-security" || audit.MaxAttempts != 4 || audit.TurnTimeoutMS != 180000 {
		t.Fatalf("Gate.SecurityAudit = %#v", audit)
	}
	if audit.MaxDiffBytes == nil || *audit.MaxDiffBytes != 131072 {
		t.Fatalf("Gate.SecurityAudit.MaxDiffBytes = %v", audit.MaxDiffBytes)
	}
	if got := audit.BlockOn; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
		t.Fatalf("Gate.SecurityAudit.BlockOn = %#v", got)
	}
}

func TestParseWorkflowAgentRoutesCanUseLegacyCodexBackend(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
codex:
  command: codex app-server
agents:
  routes:
    - name: project-model
      backend: codex
      model_field: Model
    - name: default
      backend: codex
      default: true
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestParseWorkflowMemoryTrackerIssues(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
  issues:
    - id: issue-1
      identifier: MT-1
      title: Memory adapter
      description: Load issues from config
      priority: 2
      state: Todo
      branch_name: detent/mt-1
      url: https://example.com/issues/1
      assignee_id: worker-1
      blocked_by:
        - id: issue-0
          identifier: MT-0
          state: Done
      labels:
        - stage:s1
      assigned_to_worker: true
      model_override: gpt-5-codex-high
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	got := workflow.Config.Tracker.Issues
	if len(got) != 1 {
		t.Fatalf("Tracker.Issues len = %d, want 1", len(got))
	}
	priority := 2
	want := connector.Issue{
		ID:               "issue-1",
		Identifier:       "MT-1",
		Title:            "Memory adapter",
		Description:      "Load issues from config",
		Priority:         &priority,
		State:            "Todo",
		BranchName:       "detent/mt-1",
		URL:              "https://example.com/issues/1",
		AssigneeID:       "worker-1",
		BlockedBy:        []connector.BlockedRef{{ID: "issue-0", Identifier: "MT-0", State: "Done"}},
		Labels:           []string{"stage:s1"},
		AssignedToWorker: true,
		ModelOverride:    "gpt-5-codex-high",
	}
	if got[0].ID != want.ID ||
		got[0].Identifier != want.Identifier ||
		got[0].Title != want.Title ||
		got[0].Description != want.Description ||
		got[0].Priority == nil ||
		*got[0].Priority != *want.Priority ||
		got[0].State != want.State ||
		got[0].BranchName != want.BranchName ||
		got[0].URL != want.URL ||
		got[0].AssigneeID != want.AssigneeID ||
		len(got[0].BlockedBy) != 1 ||
		got[0].BlockedBy[0] != want.BlockedBy[0] ||
		len(got[0].Labels) != 1 ||
		got[0].Labels[0] != want.Labels[0] ||
		!got[0].AssignedToWorker ||
		got[0].ModelOverride != want.ModelOverride {
		t.Fatalf("Tracker.Issues[0] = %#v, want %#v", got[0], want)
	}
}

func TestParseWorkflowMemoryTrackerIssueDefaults(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
  issues:
    - id: issue-1
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	got := workflow.Config.Tracker.Issues[0]
	if !got.AssignedToWorker {
		t.Fatal("AssignedToWorker = false, want true")
	}
	if len(got.BlockedBy) != 0 {
		t.Fatalf("BlockedBy len = %d, want 0", len(got.BlockedBy))
	}
	if len(got.Labels) != 0 {
		t.Fatalf("Labels len = %d, want 0", len(got.Labels))
	}
}

func TestParseWorkflowLocalSQLiteArtifactWorkflow(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: local_sqlite
  local_sqlite:
    path: .detent/work-items.db
    project_id: video-production
  active_states:
    - Todo
    - Production
  observed_states:
    - Review
    - Blocked
  terminal_states:
    - Done
workspace:
  kind: filesystem
  root: .detent/workspaces
  source_root: assets
  output_root: .detent/renders
deliverable:
  kind: artifact
  output_root: .detent/renders
  review_url: http://127.0.0.1:8080/review
agent:
  auto_promote:
    enabled: true
    source_state: Review
    pass_state: Done
    rework_state: Production
gate:
  kind: artifact
  artifact:
    status_field: render_status
    pass_statuses:
      - approved
    wait_statuses:
      - queued
    rework_statuses:
      - recut
server:
  kanban:
    mode: integration
---
Produce the video artifact.
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	cfg := workflow.Config
	if cfg.Tracker.Kind != TrackerLocalSQLite || cfg.Tracker.LocalSQLite.Path != ".detent/work-items.db" || cfg.Tracker.LocalSQLite.ProjectID != "video-production" {
		t.Fatalf("Tracker local sqlite config = %#v", cfg.Tracker)
	}
	if cfg.Workspace.Kind != WorkspaceFilesystem || cfg.Workspace.Root != ".detent/workspaces" || cfg.Workspace.OutputRoot != ".detent/renders" {
		t.Fatalf("Workspace = %#v", cfg.Workspace)
	}
	if cfg.Deliverable.Kind != DeliverableArtifact || cfg.Deliverable.OutputRoot != ".detent/renders" {
		t.Fatalf("Deliverable = %#v", cfg.Deliverable)
	}
	if cfg.Agent.AutoPromote.SourceState != "Review" || cfg.Agent.AutoPromote.PassState != "Done" || cfg.Agent.AutoPromote.ReworkState != "Production" {
		t.Fatalf("AutoPromote = %#v", cfg.Agent.AutoPromote)
	}
	if cfg.Gate.Kind != gate.KindArtifact ||
		cfg.Gate.Artifact.StatusField != "render_status" ||
		len(cfg.Gate.Artifact.PassStatuses) != 1 ||
		cfg.Gate.Artifact.PassStatuses[0] != "approved" {
		t.Fatalf("Gate = %#v", cfg.Gate)
	}
	if cfg.Server.Kanban.Mode != KanbanModeIntegration {
		t.Fatalf("Kanban mode = %q, want %q", cfg.Server.Kanban.Mode, KanbanModeIntegration)
	}
}

func TestParseWorkflowGitHubLocalTracker(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: github_local
  api_key: ghp_example
  repository: digitaldrywood/detent
  local_sqlite:
    path: .detent/work-items.db
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	cfg := workflow.Config
	if cfg.Tracker.Kind != TrackerGitHubLocal {
		t.Fatalf("Tracker.Kind = %q, want %q", cfg.Tracker.Kind, TrackerGitHubLocal)
	}
	if cfg.Tracker.Endpoint != defaultGitHubEndpoint {
		t.Fatalf("Tracker.Endpoint = %q, want %q", cfg.Tracker.Endpoint, defaultGitHubEndpoint)
	}
	if cfg.Tracker.GitHubStatusSource != GitHubStatusSourceProjectV2 {
		t.Fatalf("GitHubStatusSource default changed to %q", cfg.Tracker.GitHubStatusSource)
	}
}

func TestTrackerStatusPageDefaultsAndOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		kind       string
		configured string
		want       string
	}{
		{name: "github default", kind: TrackerGitHub, want: defaultGitHubStatusPageURL},
		{name: "github local default", kind: TrackerGitHubLocal, want: defaultGitHubStatusPageURL},
		{name: "linear default", kind: TrackerLinear, want: defaultLinearStatusPageURL},
		{name: "unknown connector has no source", kind: TrackerMemory},
		{name: "configured source overrides default", kind: TrackerGitHub, configured: "https://status.example.com/", want: "https://status.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			cfg.Tracker.Kind = tt.kind
			cfg.Tracker.StatusPageURL = tt.configured
			cfg.normalize()
			if cfg.Tracker.StatusPageURL != tt.want {
				t.Fatalf("Tracker.StatusPageURL = %q, want %q", cfg.Tracker.StatusPageURL, tt.want)
			}
		})
	}
}

func TestValidateStatusPageURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "empty optional", want: false},
		{name: "https", value: "https://status.example.com", want: false},
		{name: "http", value: "http://127.0.0.1:8080", want: false},
		{name: "relative", value: "/status", want: true},
		{name: "credentials", value: "https://user@example.com", want: true},
		{name: "path", value: "https://status.example.com/status", want: true},
		{name: "query", value: "https://status.example.com?tenant=one", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var problems []string
			validateStatusPageURL(tt.value, &problems)
			if got := len(problems) > 0; got != tt.want {
				t.Fatalf("validateStatusPageURL(%q) problems = %v, want error %t", tt.value, problems, tt.want)
			}
		})
	}
}

func TestParseWorkflowGitHubLocalRejectsGitHubStatusSource(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: github_local
  api_key: ghp_example
  repository: digitaldrywood/detent
  github_status_source: label
  local_sqlite:
    path: .detent/work-items.db
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	err = workflow.Config.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want github_status_source rejection")
	}
	if !strings.Contains(err.Error(), "tracker.github_status_source must be omitted when tracker.kind is github_local") {
		t.Fatalf("Validate() error = %q, want github_status_source rejection", err)
	}
}

func TestParseWorkflowNormalizesGitHubAppIDs(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: github
  api_key: token
  project_slug: PVT_project
  github_app_id: 12345
  github_app_installation_id: 67890
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	if workflow.Config.Tracker.GitHubAppID != "12345" {
		t.Fatalf("Tracker.GitHubAppID = %q, want 12345", workflow.Config.Tracker.GitHubAppID)
	}
	if workflow.Config.Tracker.GitHubAppInstallationID != "67890" {
		t.Fatalf("Tracker.GitHubAppInstallationID = %q, want 67890", workflow.Config.Tracker.GitHubAppInstallationID)
	}
}

func TestConfigValidateAcceptsGitHubAppCredentials(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: github
  project_slug: PVT_project
  github_app_id: 12345
  github_app_private_key_path: .detent/github-app.pem
  github_app_installation_id: 67890
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestStringOrMapFieldsAcceptScalarOrMapping(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
  state_map: $STATE_MAP_JSON
  priority_map:
    P0: 1
    P1: 2
codex:
  approval_policy:
    allow:
      - tool: shell
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}

	if !workflow.Config.Tracker.StateMap.IsString {
		t.Fatalf("Tracker.StateMap = %#v, want string", workflow.Config.Tracker.StateMap)
	}
	if workflow.Config.Tracker.StateMap.String != "$STATE_MAP_JSON" {
		t.Fatalf("Tracker.StateMap.String = %q", workflow.Config.Tracker.StateMap.String)
	}
	if got := workflow.Config.Tracker.PriorityMap.Map["P1"]; got != 2 {
		t.Fatalf("Tracker.PriorityMap[P1] = %v, want 2", got)
	}
	if got := workflow.Config.Codex.ApprovalPolicy.Map["allow"].([]any)[0].(map[string]any)["tool"]; got != "shell" {
		t.Fatalf("Codex.ApprovalPolicy allow tool = %v, want shell", got)
	}
}

func TestParseWorkflowDeliverableElicitationAllowlist(t *testing.T) {
	t.Parallel()

	workflow, err := ParseWorkflow([]byte(`---
tracker:
  kind: memory
codex:
  deliverable_elicitation_allowlist:
    - server: " codex_apps "
      tool: " github.create_pull_request "
      repository: " acme/widgets "
---
Prompt
`))
	if err != nil {
		t.Fatalf("ParseWorkflow() error = %v", err)
	}
	if err := workflow.Config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	rules := workflow.Config.Codex.DeliverableElicitationAllowlist
	want := []DeliverableElicitationRule{{
		Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
	}}
	if !reflect.DeepEqual(rules, want) {
		t.Fatalf("DeliverableElicitationAllowlist = %#v, want %#v", rules, want)
	}
	backendRules := workflow.Config.AgentBackendConfigs()[0].CodexOptions().DeliverableElicitationAllowlist
	if !reflect.DeepEqual(backendRules, want) {
		t.Fatalf("backend DeliverableElicitationAllowlist = %#v, want %#v", backendRules, want)
	}
}

func TestDeliverableElicitationAllowlistValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []DeliverableElicitationRule
		wantErr string
	}{
		{
			name: "valid tuple",
			rules: []DeliverableElicitationRule{{
				Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
			}},
		},
		{
			name:    "server only",
			rules:   []DeliverableElicitationRule{{Server: "codex_apps"}},
			wantErr: "codex.deliverable_elicitation_allowlist[0].tool is required",
		},
		{
			name: "missing server",
			rules: []DeliverableElicitationRule{{
				Tool: "github.create_pull_request", Repository: "acme/widgets",
			}},
			wantErr: "codex.deliverable_elicitation_allowlist[0].server is required",
		},
		{
			name: "invalid repository",
			rules: []DeliverableElicitationRule{{
				Server: "codex_apps", Tool: "github.create_pull_request", Repository: "widgets",
			}},
			wantErr: "codex.deliverable_elicitation_allowlist[0].repository must be owner/name",
		},
		{
			name: "duplicate tuple",
			rules: []DeliverableElicitationRule{
				{Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets"},
				{Server: "codex_apps", Tool: "github.create_pull_request", Repository: "ACME/WIDGETS"},
			},
			wantErr: "codex.deliverable_elicitation_allowlist[1] duplicates an earlier rule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Default()
			cfg.Tracker.Kind = TrackerMemory
			cfg.Codex.DeliverableElicitationAllowlist = tt.rules
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestObservabilityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "invalid OTLP endpoint",
			mutate: func(cfg *Config) {
				cfg.Observability.OTLP.Endpoint = "collector:4318"
			},
			wantErr: "observability.otlp.endpoint must be an absolute http or https URL",
		},
		{
			name: "nonpositive anomaly threshold",
			mutate: func(cfg *Config) {
				cfg.Observability.Efficiency.AnomalyTokensMultiple = 0
			},
			wantErr: "observability.efficiency.anomaly_tokens_multiple must be greater than 0",
		},
		{
			name: "nonpositive stranded active threshold",
			mutate: func(cfg *Config) {
				cfg.Observability.StrandedActiveThresholdSeconds = 0
			},
			wantErr: "observability.stranded_active_threshold_seconds must be greater than 0",
		},
		{
			name: "nonpositive dispatch stall threshold",
			mutate: func(cfg *Config) {
				cfg.Observability.DispatchStallThresholdSeconds = 0
			},
			wantErr: "observability.dispatch_stall_threshold_seconds must be greater than 0",
		},
		{
			name: "nonpositive park review threshold",
			mutate: func(cfg *Config) {
				cfg.Observability.ParkReviewThreshold = -1
			},
			wantErr: "observability.park_review_threshold must be greater than 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestBlockedRecoveryValidateConcurrentSharedStorage(t *testing.T) {
	t.Parallel()

	const validatorCount = 2
	config := BlockedRecovery{
		Enabled:      true,
		SourceStates: []string{" Blocked "},
		TargetState:  " Rework ",
		ReasonCodes:  []string{" Merge Conflicts ", " Stale Base ", " Missing Current Head CI "},
	}
	wantReasonCodes := slices.Clone(config.ReasonCodes)
	start := make(chan struct{})
	results := make(chan []string, validatorCount)
	var wait sync.WaitGroup
	wait.Add(validatorCount)

	for range validatorCount {
		configCopy := config
		go func() {
			defer wait.Done()
			<-start
			for range 1000 {
				if problems := configCopy.Validate("tracker.blocked_recovery"); len(problems) > 0 {
					results <- problems
					return
				}
			}
		}()
	}

	close(start)
	wait.Wait()
	close(results)

	for problems := range results {
		t.Errorf("Validate() problems = %v, want none", problems)
	}
	if !slices.Equal(config.ReasonCodes, wantReasonCodes) {
		t.Fatalf("Validate() changed ReasonCodes to %q, want %q", config.ReasonCodes, wantReasonCodes)
	}
}

func TestConfigValidateReportsInvalidSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "missing tracker kind",
			raw:  "---\ntracker: {}\n---\nPrompt\n",
			want: []string{"tracker.kind is required"},
		},
		{
			name: "unsupported tracker kind",
			raw:  "---\ntracker:\n  kind: jira\n---\nPrompt\n",
			want: []string{"tracker.kind must be one of"},
		},
		{
			name: "linear credentials",
			raw:  "---\ntracker:\n  kind: linear\n---\nPrompt\n",
			want: []string{"tracker.api_key is required for linear", "tracker.project_slug is required for linear"},
		},
		{
			name: "github credentials",
			raw:  "---\ntracker:\n  kind: github\n---\nPrompt\n",
			want: []string{"tracker.api_key or GitHub App credentials are required for github", "tracker.project_slug is required for github"},
		},
		{
			name: "dependency source",
			raw:  "---\ntracker:\n  kind: memory\ndependencies:\n  source: prose\n---\nPrompt\n",
			want: []string{"dependencies.source must be one of merged, native_only"},
		},
		{
			name: "workspace cache strategy",
			raw:  "---\ntracker:\n  kind: memory\nworkspace:\n  cache_strategy: ephemeral\n---\nPrompt\n",
			want: []string{"workspace.cache_strategy must be one of isolated, shared"},
		},
		{
			name: "partial github app credentials",
			raw: `---
tracker:
  kind: github
  project_slug: PVT_project
  github_app_id: 12345
---
Prompt
`,
			want: []string{
				"tracker.github_app_installation_id is required for github app",
				"tracker.github_app_private_key or tracker.github_app_private_key_path is required for github app",
			},
		},
		{
			name: "positive numbers and states",
			raw: `---
tracker:
  kind: memory
  http_max_idle_conns: 0
  http_max_idle_conns_per_host: 0
  http_idle_conn_timeout_ms: 0
  github_graphql_warn_remaining: 0
  github_graphql_min_remaining_reserve: 0
  github_rest_min_remaining_reserve: 0
  github_rest_fanout_max_requests: 0
  active_states: ["Todo", ""]
polling:
  interval_ms: 0
worker:
  max_concurrent_agents_per_host: 0
workspace:
  cleanup_idle_ttl_ms: 0
  cleanup_sweep_interval_ms: 0
agent:
  max_concurrent_agents: 0
  max_turn_duration_ms: -1
  max_session_duration_ms: -1
  no_progress_timeout_ms: -1
  merge_worker_startup_timeout_ms: -1
  merge_worker_max_duration_ms: -1
  merge_fallback_max_duration_ms: -1
  max_session_tokens: -1
  max_session_context_multiplier: -0.5
  output_truncation:
    max_bytes: -1
  max_concurrent_agents_by_state:
    Todo: 0
  dispatch_priority_by_state: ["Todo", "Todo"]
  dispatch_priority_by_label: [""]
codex:
  turn_timeout_ms: 0
hooks:
  timeout_ms: 0
observability:
  refresh_ms: 0
budget:
  per_day_max_usd: 0
server:
  port: -1
  kanban:
    mode: edit
    issue_state_field_id: -1
---
Prompt
`,
			want: []string{
				"tracker.active_states state names must not be blank",
				"tracker.http_max_idle_conns must be greater than 0",
				"tracker.http_max_idle_conns_per_host must be greater than 0",
				"tracker.http_idle_conn_timeout_ms must be greater than 0",
				"tracker.github_graphql_warn_remaining must be greater than 0",
				"tracker.github_graphql_min_remaining_reserve must be greater than 0",
				"tracker.github_rest_min_remaining_reserve must be greater than 0",
				"polling.interval_ms must be greater than 0",
				"worker.max_concurrent_agents_per_host must be greater than 0",
				"workspace.cleanup_idle_ttl_ms must be greater than 0",
				"workspace.cleanup_sweep_interval_ms must be greater than 0",
				"agent.max_concurrent_agents must be greater than 0",
				"agent.max_turn_duration_ms must be greater than or equal to 0",
				"agent.max_session_duration_ms must be greater than or equal to 0",
				"agent.no_progress_timeout_ms must be greater than or equal to 0",
				"agent.merge_worker_startup_timeout_ms must be greater than 0",
				"agent.merge_worker_max_duration_ms must be greater than 0",
				"agent.merge_fallback_max_duration_ms must be greater than 0",
				"agent.max_session_tokens must be greater than or equal to 0",
				"agent.max_session_context_multiplier must be greater than or equal to 0",
				"agent.output_truncation.max_bytes must be greater than or equal to 0",
				"agent.max_concurrent_agents_by_state limits must be positive integers",
				"agent.dispatch_priority_by_state state names must be unique",
				"agent.dispatch_priority_by_label labels must not be blank",
				"codex.turn_timeout_ms must be greater than 0",
				"hooks.timeout_ms must be greater than 0",
				"observability.refresh_ms must be greater than 0",
				"budget.per_day_max_usd must be greater than 0",
				"server.port must be greater than or equal to 0",
				"server.kanban.mode must be one of read_only, integration",
				"server.kanban.issue_state_field_id must be greater than 0 when set",
			},
		},
		{
			name: "invalid agent workflow instructions",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - In Progress
    - Rework
  observed_states:
    - Blocked
  terminal_states:
    - Done
agent:
  instructions_by_state:
    "": blank
    Todo: first
    " todo ": duplicate
    QA: unknown
  instructions_by_transition:
    "":
      Done: blank source
    Todo:
      "": blank target
      Done: first
      " done ": duplicate
      Archive: unknown target
    Review:
      Done: unknown source
    " todo ":
      Done: duplicate source
---
Prompt
`,
			want: []string{
				"agent.instructions_by_state state names must not be blank",
				"agent.instructions_by_state state names must be unique",
				"agent.instructions_by_state state \"QA\" must reference a configured workflow state",
				"agent.instructions_by_transition source states must not be blank",
				"agent.instructions_by_transition source states must be unique",
				"agent.instructions_by_transition source state \"Review\" must reference a configured workflow state",
				"agent.instructions_by_transition target states must not be blank",
				"agent.instructions_by_transition target states must be unique per source",
				"agent.instructions_by_transition target state \"Archive\" must reference a configured workflow state",
			},
		},
		{
			name: "polling interval floor",
			raw: `---
tracker:
  kind: memory
polling:
  interval_ms: 59999
---
Prompt
`,
			want: []string{"polling.interval_ms must be at least 60000"},
		},
		{
			name: "invalid dependency auto unblock config",
			raw: `---
tracker:
  kind: memory
  dependency_auto_unblock:
    enabled: true
    source_states: [""]
    target_state: ""
    readiness: sometimes
---
Prompt
`,
			want: []string{
				"tracker.dependency_auto_unblock.source_states state names must not be blank",
				"tracker.dependency_auto_unblock.target_state is required when tracker.dependency_auto_unblock.enabled is true",
				"tracker.dependency_auto_unblock.readiness must be one of terminal, terminal_or_merged",
				"tracker.active_states must include Rework when tracker.dependency_auto_unblock.enabled is true",
			},
		},
		{
			name: "dependency auto unblock requires active rework",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - In Progress
  dependency_auto_unblock:
    enabled: true
    source_states:
      - Blocked
    target_state: Todo
    readiness: terminal_or_merged
---
Prompt
`,
			want: []string{
				"tracker.active_states must include Rework when tracker.dependency_auto_unblock.enabled is true",
			},
		},
		{
			name: "invalid blocked recovery config",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - Rework
  blocked_recovery:
    enabled: true
    breaker_cooldown_seconds: -1
    source_states: [""]
    target_state: ""
    reason_codes:
      - sometimes
      - merge_conflict
      - merge-conflicts
---
Prompt
`,
			want: []string{
				"tracker.blocked_recovery.breaker_cooldown_seconds must be greater than or equal to 0",
				"tracker.blocked_recovery.source_states state names must not be blank",
				"tracker.blocked_recovery.target_state is required when tracker.blocked_recovery.enabled is true",
				"tracker.blocked_recovery.reason_codes must contain only merge_conflict, stale_base, missing_current_head_ci",
				"tracker.blocked_recovery.reason_codes must be unique",
			},
		},
		{
			name: "blocked recovery requires active target state",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - In Progress
  blocked_recovery:
    enabled: true
    source_states:
      - Blocked
    target_state: Rework
    reason_codes:
      - merge_conflict
---
Prompt
`,
			want: []string{
				"tracker.active_states must include tracker.blocked_recovery.target_state when tracker.blocked_recovery.enabled is true",
			},
		},
		{
			name: "invalid blocker auto promote config",
			raw: `---
tracker:
  kind: memory
  blocker_auto_promote:
    enabled: true
    source_states: [""]
    blocker_states: [""]
    target_state: ""
---
Prompt
`,
			want: []string{
				"tracker.blocker_auto_promote.source_states state names must not be blank",
				"tracker.blocker_auto_promote.blocker_states state names must not be blank",
				"tracker.blocker_auto_promote.target_state is required when tracker.blocker_auto_promote.enabled is true",
			},
		},
		{
			name: "invalid transient ci retry limit",
			raw: `---
tracker:
  kind: memory
gate:
  transient_ci_retry_limit: -1
---
Prompt
`,
			want: []string{
				"gate.transient_ci_retry_limit must be greater than or equal to 0",
			},
		},
		{
			name: "invalid auto promote rework limit",
			raw: `---
tracker:
  kind: memory
agent:
  auto_promote:
    rework_limit: -1
---
Prompt
`,
			want: []string{
				"agent.auto_promote.rework_limit must be greater than or equal to 0",
			},
		},
		{
			name: "invalid auto promote gate wait settings",
			raw: `---
tracker:
  kind: memory
agent:
  auto_promote:
    gate_wait_state: backlog
    gate_wait_timeout_seconds: -1
---
Prompt
`,
			want: []string{
				"agent.auto_promote.gate_wait_state must be one of source, review",
				"agent.auto_promote.gate_wait_timeout_seconds must be greater than 0",
			},
		},
		{
			name: "invalid automated review settings",
			raw: `---
tracker:
  kind: memory
agent:
  auto_promote:
    gate_wait_timeout_action: abandon
gate:
  automated_review: sometimes
---
Prompt
`,
			want: []string{
				"agent.auto_promote.gate_wait_timeout_action must be one of merge, human_review",
				"gate.automated_review must be one of required, optional, off",
			},
		},
		{
			name: "invalid auto promote no progress limit",
			raw: `---
tracker:
  kind: memory
agent:
  auto_promote:
    no_progress_limit: -1
---
Prompt
`,
			want: []string{
				"agent.auto_promote.no_progress_limit must be greater than or equal to 0",
			},
		},
		{
			name: "auto promote rework limit requires blocked state",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  observed_states:
    - Human Review
  terminal_states:
    - Done
agent:
  auto_promote:
    enabled: true
    rework_limit: 1
---
Prompt
`,
			want: []string{
				"tracker.active_states, tracker.observed_states, or tracker.terminal_states must include Blocked when agent.auto_promote.rework_limit is greater than 0",
			},
		},
		{
			name: "auto promote no progress limit requires blocked state",
			raw: `---
tracker:
  kind: memory
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  observed_states:
    - Human Review
  terminal_states:
    - Done
agent:
  auto_promote:
    no_progress_limit: 1
---
Prompt
`,
			want: []string{
				"tracker.active_states, tracker.observed_states, or tracker.terminal_states must include Blocked when agent.auto_promote.no_progress_limit is greater than 0",
			},
		},
		{
			name: "invalid paths and priority map",
			raw: `---
tracker:
  kind: memory
  priority_map:
    "": 1
    Bad: 5
agent:
  lessons:
    path: ../lessons.md
  knowledge:
    sources:
      - path: ""
  skills:
    path: /tmp/skills
    creation:
      max_drafts_per_run: 0
---
Prompt
`,
			want: []string{
				"tracker.priority_map option names must not be blank",
				"tracker.priority_map ranks must be integers 1 through 4 or null",
				"agent.lessons.path must be a relative path inside the workspace",
				"agent.knowledge.sources[0].path must not be blank",
				"agent.skills.path must be a relative path inside the workspace",
				"agent.skills.creation.max_drafts_per_run must be greater than 0",
			},
		},
		{
			name: "invalid agents config",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: codex
      kind: claude
      protocol: stream
      command: ""
  routes:
    - backend: missing
      default: true
    - backend: codex
      default: true
      selector:
        priority_in: [0]
---
Prompt
`,
			want: []string{
				"agents.backends.kind must be one of codex, claude_code",
				"agents.backends.command is required",
				"agents.routes.backend must reference a configured backend",
				"agents.routes.selector.priority_in values must be integers 1 through 4",
				"agents.routes must not define multiple default routes for the same role",
			},
		},
		{
			name: "invalid claude code protocol",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      protocol: app-server
  routes:
    - backend: claude
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.protocol must be headless for claude_code",
			},
		},
		{
			name: "invalid agent backend options decode",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: codex
      kind: codex
      protocol: app-server
      command: codex app-server
      options:
        approval_policy: [never]
  routes:
    - backend: codex
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.options must decode for codex",
			},
		},
		{
			name: "invalid claude code permission mode",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      options:
        permission_mode: ask
  routes:
    - backend: claude
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.options.permission_mode must be one of default, acceptEdits, bypassPermissions",
			},
		},
		{
			name: "invalid runtime identity labels and effort",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      provider: https://provider.example
      options:
        effort: extreme
  routes:
    - backend: claude
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.provider must be a sanitized label containing only letters, numbers, dots, underscores, or hyphens",
				"agents.backends.options.effort must be one of low, medium, high, xhigh, max, ultracode",
			},
		},
		{
			name: "invalid claude code plan permission mode",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      options:
        permission_mode: plan
  routes:
    - backend: claude
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.options.permission_mode must not be plan for unattended workers",
			},
		},
		{
			name: "invalid claude code option timeouts",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      options:
        turn_timeout_ms: -1
        stall_timeout_ms: -1
  routes:
    - backend: claude
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.options.turn_timeout_ms must be greater than or equal to 0",
				"agents.backends.options.stall_timeout_ms must be greater than or equal to 0",
			},
		},
		{
			name: "invalid agent backend option timeouts",
			raw: `---
tracker:
  kind: memory
agents:
  backends:
    - id: codex
      kind: codex
      protocol: app-server
      command: codex app-server
      options:
        turn_timeout_ms: -1
        read_timeout_ms: -1
        stall_timeout_ms: -1
  routes:
    - backend: codex
      default: true
---
Prompt
`,
			want: []string{
				"agents.backends.options.turn_timeout_ms must be greater than or equal to 0",
				"agents.backends.options.read_timeout_ms must be greater than or equal to 0",
				"agents.backends.options.stall_timeout_ms must be greater than or equal to 0",
			},
		},
		{
			name: "invalid identity and authorization",
			raw: `---
identity:
  github_login: detent-bot
  ownership_mode: field
tracker:
  kind: memory
  authorization:
    priority_in: [0]
    fields:
      - value: multi-instance
---
Prompt
`,
			want: []string{
				"identity.name must not be blank",
				"identity.owner_field is required when identity.ownership_mode is field",
				"tracker.authorization.priority_in values must be integers 1 through 4",
				"tracker.authorization.fields[0].name must not be blank",
			},
		},
		{
			name: "invalid claim lease config",
			raw: `---
identity:
  name: release-captain
  github_login: detent-bot
tracker:
  kind: memory
  claims:
    enabled: true
    lease_field: ""
    ttl_seconds: 30
    heartbeat_seconds: 60
---
Prompt
`,
			want: []string{
				"tracker.claims.lease_field must not be blank when tracker.claims.enabled is true",
				"tracker.claims.heartbeat_seconds must be less than or equal to tracker.claims.ttl_seconds",
			},
		},
		{
			name: "invalid gate config",
			raw: `---
tracker:
  kind: memory
gate:
  kind: checklist
  ci_failure_action: bounce
---
Prompt
`,
			want: []string{
				"gate.kind must be one of command, human_review, artifact",
				"gate.ci_failure_action must be one of skip, rework",
			},
		},
		{
			name: "invalid validator config",
			raw: `---
tracker:
  kind: memory
gate:
  validator:
    enabled: true
    min_score: 1.2
    turn_timeout_ms: -1
    max_inline_diff_bytes: -1
    block_on:
      - ""
---
Prompt
`,
			want: []string{
				"gate.validator.min_score must be greater than 0 and less than or equal to 1",
				"gate.validator.turn_timeout_ms must be greater than or equal to 0",
				"gate.validator.max_inline_diff_bytes must be greater than or equal to 0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			workflow, err := ParseWorkflow([]byte(tt.raw))
			if err != nil {
				t.Fatalf("ParseWorkflow() error = %v", err)
			}

			err = workflow.Config.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("Validate() error = %q, want substring %q", err, want)
				}
			}
		})
	}
}

func TestParseWorkflowReportsInvalidFrontmatter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "missing frontmatter", raw: "Prompt only\n", want: "missing YAML frontmatter"},
		{name: "unterminated frontmatter", raw: "---\ntracker:\n  kind: memory\n", want: "unterminated YAML frontmatter"},
		{name: "invalid yaml", raw: "---\ntracker: [\n---\nPrompt\n", want: "parse YAML frontmatter"},
		{name: "not a map", raw: "---\n- tracker\n---\nPrompt\n", want: "workflow frontmatter must be a mapping"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseWorkflow([]byte(tt.raw))
			if err == nil {
				t.Fatal("ParseWorkflow() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseWorkflow() error = %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestReleaseConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{
			name: "valid github policy",
			change: func(cfg *Config) {
				cfg.Tracker.Kind = TrackerGitHub
				cfg.Tracker.APIKey = "token"
				cfg.Tracker.GitHubStatusSource = GitHubStatusSourceLabel
				cfg.Tracker.Repository = "example/repo"
				cfg.Release.Enabled = true
			},
		},
		{
			name: "requires github tracker",
			change: func(cfg *Config) {
				cfg.Tracker.Kind = TrackerMemory
				cfg.Release.Enabled = true
			},
			want: "release.enabled requires tracker.kind github or github_local",
		},
		{
			name: "green ci cannot be waived",
			change: func(cfg *Config) {
				cfg.Tracker.Kind = TrackerGitHub
				cfg.Tracker.APIKey = "token"
				cfg.Tracker.GitHubStatusSource = GitHubStatusSourceLabel
				cfg.Tracker.Repository = "example/repo"
				cfg.Release.Enabled = true
				cfg.Release.RequireGreenCI = false
			},
			want: "release.require_green_ci must be true",
		},
		{
			name: "flaky rerun requires allowlist",
			change: func(cfg *Config) {
				cfg.Tracker.Kind = TrackerGitHub
				cfg.Tracker.APIKey = "token"
				cfg.Tracker.GitHubStatusSource = GitHubStatusSourceLabel
				cfg.Tracker.Repository = "example/repo"
				cfg.Release.Enabled = true
				cfg.Release.RerunFlakyOnce = true
			},
			want: "release.flaky_check_names must not be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			test.change(&cfg)
			err := cfg.Validate()
			if test.want == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCheckpointIntervalConfiguration(t *testing.T) {
	t.Parallel()
	for _, interval := range []int{-1, 0, 60000} {
		t.Run(strconv.Itoa(interval), func(t *testing.T) {
			t.Parallel()
			workflow, err := ParseWorkflow(fmt.Appendf(nil, "---\nagent:\n  checkpoint_interval_ms: %d\n---\n", interval))
			if err != nil {
				t.Fatal(err)
			}
			var problems []string
			workflow.Config.Agent.validate("agent", &problems)
			if interval < 0 {
				if !strings.Contains(strings.Join(problems, "\n"), "agent.checkpoint_interval_ms") {
					t.Fatalf("negative interval accepted: %v", problems)
				}
				return
			}
			if len(problems) != 0 || workflow.Config.Agent.CheckpointIntervalMS != interval {
				t.Fatalf("interval = %d, problems = %v", workflow.Config.Agent.CheckpointIntervalMS, problems)
			}
		})
	}
}
