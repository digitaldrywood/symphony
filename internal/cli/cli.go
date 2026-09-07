package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/digitaldrywood/detent/internal/buildinfo"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/pause"
	"github.com/digitaldrywood/detent/internal/project"
)

var (
	ErrConfigExists    = errors.New("global config already exists")
	ErrProjectExists   = errors.New("project already exists")
	ErrProjectNotFound = errors.New("project not found")
)

const (
	addProjectExampleCommand = "detent add-project --id api --workflow ~/code/api/WORKFLOW.md --workdir ~/code/api"
	configPathCommand        = "detent config path"
	forceInitCommand         = "detent init --force"
	ghAuthLoginCommand       = `gh auth login --scopes "repo,read:org,project"`
	portExampleCommand       = "detent --port 0"
	promoteExampleCommand    = "detent promote api --priority 10"
)

type HintedError struct {
	Err      error
	Message  string
	Hint     string
	Commands []string
}

func (e *HintedError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return ""
}

func (e *HintedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func HintFor(err error) (string, []string, bool) {
	var hinted *HintedError
	if !errors.As(err, &hinted) {
		return "", nil, false
	}
	return hinted.Hint, append([]string(nil), hinted.Commands...), true
}

type Operation string

const (
	OperationInit           Operation = "init"
	OperationAddProject     Operation = "add-project"
	OperationPauseProject   Operation = "pause"
	OperationUnpauseProject Operation = "unpause"
	OperationResumeProject  Operation = "resume"
	OperationPromoteProject Operation = "promote"
	OperationRemoveProject  Operation = "remove-project"
)

const examplesFirstUsageTemplate = `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

Available Commands:{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

Additional Commands:{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

type Signal struct {
	Operation Operation
	ProjectID string
	Project   globalconfig.Project
}

type initResult struct {
	Status string `json:"status"`
	Path   string `json:"path"`
	Rule   string `json:"rule"`
}

type configPathResult struct {
	Path string `json:"path"`
	Rule string `json:"rule"`
}

type projectResult struct {
	ID                    string                 `json:"id"`
	Workflow              string                 `json:"workflow"`
	WorkflowRef           string                 `json:"workflow_ref,omitempty"`
	Workdir               string                 `json:"workdir"`
	Weight                int                    `json:"weight"`
	Priority              int                    `json:"priority"`
	Paused                bool                   `json:"paused"`
	PausedReason          string                 `json:"paused_reason,omitempty"`
	PausedUntilIssue      string                 `json:"paused_until_issue,omitempty"`
	PausedUntil           string                 `json:"paused_until,omitempty"`
	CredentialRef         string                 `json:"credential_ref,omitempty"`
	GlobalConfigGitStatus *globalConfigGitStatus `json:"global_config_git,omitempty"`
}

type projectPausedResult struct {
	Status           string `json:"status"`
	Project          string `json:"project"`
	Paused           bool   `json:"paused"`
	PausedReason     string `json:"paused_reason,omitempty"`
	PausedUntilIssue string `json:"paused_until_issue,omitempty"`
	PausedUntil      string `json:"paused_until,omitempty"`
}

type projectResumedResult struct {
	Status                   string `json:"status"`
	Project                  string `json:"project"`
	ActiveHoursOverrideUntil string `json:"active_hours_override_until"`
}

type projectPriorityResult struct {
	Status   string `json:"status"`
	Project  string `json:"project"`
	Priority int    `json:"priority"`
}

type projectRemovedResult struct {
	Status  string `json:"status"`
	Project string `json:"project"`
	Removed bool   `json:"removed"`
}

type BootMode string

const (
	BootModeRunning    BootMode = "running"
	BootModeOnboarding BootMode = "onboarding"
)

type BootConfig struct {
	Mode             BootMode
	Global           globalconfig.Config
	ConfigPathRule   globalconfig.PathRule
	Runtime          RuntimeSettings
	LogLevel         *slog.LevelVar
	WorkflowPath     string
	Host             string
	Port             *int
	RuntimeDBPath    string
	RuntimeLogPath   string
	Isolated         *IsolatedRuntimeInfo
	Version          string
	Build            buildinfo.Info
	Headless         bool
	StdoutTTY        bool
	Output           io.Writer
	Shutdown         *ShutdownController
	Restart          *RestartRequest
	HardExit         func(int)
	Runner           orchestrator.Runner
	ConnectorFactory project.ConnectorFactory
	StartupRecovery  StartupRecovery
}

type IsolatedRuntimeInfo struct {
	Home          string
	ConfigPath    string
	WorkflowPath  string
	WorkspaceRoot string
	DBPath        string
	DBMode        string
	TrackerMode   string
	FixturePath   string
	Demo          string
	DemoClock     string
	ManifestPath  string
}

type BootFunc func(context.Context, BootConfig) error

type StartupRecovery interface {
	MarkHealthy(context.Context) error
	HandleFailure(context.Context, error)
}

type StartupRecoveryFactory func(context.Context, BootConfig) (StartupRecovery, error)

type SignalFunc func(context.Context, Signal) error

type LoggerFunc func(RuntimeSettings, io.Writer, io.Writer, bool) *slog.LevelVar

type CommandRunner func(context.Context, string, ...string) (string, error)

type ProjectManager interface {
	Add(context.Context, globalconfig.Project) error
	Remove(context.Context, project.ID) error
	Pause(context.Context, project.ID) error
	Unpause(context.Context, project.ID) error
}

type Option func(*options)

type options struct {
	resolvePath       func(string) (globalconfig.PathResolution, error)
	read              func(string) (globalconfig.Config, error)
	readProject       func(string, string) (globalconfig.Config, []string, error)
	readDoctor        func(string) (globalconfig.Config, error)
	readDoctorProject func(string, string) (globalconfig.Config, []string, error)
	readOrDefault     func(string) (globalconfig.Config, error)
	write             func(string, globalconfig.Config) error
	boot              BootFunc
	signal            SignalFunc
	lookupEnv         func(string) string
	ghAuthToken       func(context.Context) (string, error)
	runCommand        CommandRunner
	httpDo            func(*http.Request) (*http.Response, error)
	configureLog      LoggerFunc
	captureDemo       demoCaptureFunc
	version           string
	build             buildinfo.Info
	stdoutTTY         func() bool
	shutdown          *ShutdownController
	restart           *RestartRequest
	service           ServiceFactory
	serviceInjected   bool
	startupRecovery   StartupRecoveryFactory
}

func WithBootFunc(boot BootFunc) Option {
	return func(opts *options) {
		if boot != nil {
			opts.boot = boot
		}
	}
}

func WithSignalFunc(signal SignalFunc) Option {
	return func(opts *options) {
		if signal != nil {
			opts.signal = signal
		}
	}
}

func WithVersion(version string) Option {
	return func(opts *options) {
		opts.version = strings.TrimSpace(version)
	}
}

func WithBuild(build buildinfo.Info) Option {
	return func(opts *options) {
		opts.build = build
	}
}

func WithProjectManager(manager ProjectManager) Option {
	return func(opts *options) {
		opts.signal = ProjectManagerSignalFunc(manager)
	}
}

func WithStdoutTTY(stdoutTTY func() bool) Option {
	return func(opts *options) {
		if stdoutTTY != nil {
			opts.stdoutTTY = stdoutTTY
		}
	}
}

func WithCommandRunner(run CommandRunner) Option {
	return func(opts *options) {
		if run != nil {
			opts.runCommand = run
		}
	}
}

func WithShutdownController(controller *ShutdownController) Option {
	return func(opts *options) {
		opts.shutdown = controller
	}
}

func WithRestartRequest(restart *RestartRequest) Option {
	return func(opts *options) {
		opts.restart = restart
	}
}

func WithLoggerFunc(configure LoggerFunc) Option {
	return func(opts *options) {
		opts.configureLog = configure
	}
}

func WithServiceFactory(factory ServiceFactory) Option {
	return func(opts *options) {
		if factory != nil {
			opts.service = factory
			opts.serviceInjected = true
		}
	}
}

func WithStartupRecovery() Option {
	return func(opts *options) {
		opts.startupRecovery = newDefaultStartupRecovery
	}
}

func ProjectManagerSignalFunc(manager ProjectManager) SignalFunc {
	if manager == nil {
		return noSignal
	}

	return func(ctx context.Context, signal Signal) error {
		switch signal.Operation {
		case OperationAddProject:
			return manager.Add(ctx, signal.Project)
		case OperationRemoveProject:
			return manager.Remove(ctx, project.ID(signal.ProjectID))
		case OperationPauseProject:
			return manager.Pause(ctx, project.ID(signal.ProjectID))
		case OperationUnpauseProject:
			return manager.Unpause(ctx, project.ID(signal.ProjectID))
		default:
			return nil
		}
	}
}

func NewRootCommand(ctx context.Context, optFns ...Option) *cobra.Command {
	opts := defaultOptions()
	for _, opt := range optFns {
		opt(&opts)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var configPath string
	var env string
	var logLevel string
	var host string
	var port int
	var headless bool
	var format string
	cmd := &cobra.Command{
		Use:   "detent",
		Short: "Detent agent orchestrator",
		Long: strings.TrimSpace(`Detent is an agent orchestrator for tracker-backed work queues.

Output:
  --format pretty prints human-readable output.
  --format json prints machine-readable JSON.
  DETENT_FORMAT can set the default format.
  When neither is set, Detent prints pretty output on a TTY and JSON otherwise.

JSON errors:
  JSON-mode failures write one problem object to stderr.
  Fields: type, code, title, detail, exit_code, suggested_fix, did_you_mean, docs_url.
  Optional fields: suggested_fix, did_you_mean, docs_url.
  Stable codes: general, validation, unknown_command, unknown_flag, github_auth,
  config_exists, project_exists, project_not_found, doctor_failed, shutdown_forced,
  shutdown_timeout, dashboard_unreachable, dashboard_timeout,
  dashboard_unauthorized, dashboard_forbidden, ambiguous_reference,
  issue_not_found, unsupported_model_version, runtime_unavailable,
  dashboard_request_failed.

Exit codes:
  0  success
  1  general or unexpected failure
  2  auth or GitHub token problem
  3  validation, unknown command, or unknown flag
  4  not found or config conflict`),
		Example: strings.TrimSpace(`detent --config ~/.config/detent/global.yaml --headless
detent --format json config path`),
		Args:         suggestedNoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := OutputForCommand(cmd); err != nil {
				return err
			}
			flags := runtimeFlags{
				Env:      runtimeStringFlag{Value: env, Set: flagChanged(cmd, "env")},
				LogLevel: runtimeStringFlag{Value: logLevel, Set: flagChanged(cmd, "log-level")},
				Port:     runtimeIntFlag{Value: port, Set: flagChanged(cmd, "port")},
			}
			boot, err := resolveBootConfig(cmd.Context(), configPath, host, flags, opts)
			if err != nil {
				return err
			}
			stdoutTTY := opts.stdoutTTY()
			if opts.configureLog != nil {
				boot.LogLevel = opts.configureLog(boot.Runtime, cmd.OutOrStdout(), cmd.ErrOrStderr(), stdoutTTY)
			}
			boot.Headless = headless
			boot.StdoutTTY = stdoutTTY
			boot.Output = cmd.OutOrStdout()
			boot.Shutdown = opts.shutdown
			boot.Restart = opts.restart
			if !shouldLaunchTerminalDashboard(boot) {
				slog.Info("resolved global config", "path", boot.Global.Path, "rule", boot.ConfigPathRule)
				for _, warning := range boot.Runtime.Warnings {
					slog.Warn(warning.Detail, "check", warning.Name, "hint", warning.Hint)
				}
			}
			var recovery StartupRecovery
			if opts.startupRecovery != nil && boot.Mode == BootModeRunning {
				recovery, err = opts.startupRecovery(cmd.Context(), boot)
				if err != nil {
					slog.Warn("initialize startup recovery failed", "error", err)
				} else {
					boot.StartupRecovery = recovery
				}
			}
			bootErr := opts.boot(cmd.Context(), boot)
			if bootErr != nil && recovery != nil {
				recovery.HandleFailure(cmd.Context(), bootErr)
			}
			return bootErr
		},
	}
	cmd.SetContext(withCommandOutputOptions(ctx, commandOutputOptions{
		lookupEnv: opts.lookupEnv,
		stdoutTTY: opts.stdoutTTY,
	}))
	cmd.PersistentFlags().StringVar(&configPath, "config", "", "path to global.yaml")
	cmd.PersistentFlags().StringVar(&env, "env", "", "runtime environment")
	cmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "log level")
	cmd.PersistentFlags().StringVar(&host, "host", "", "web server host")
	cmd.PersistentFlags().IntVar(&port, "port", -1, "web server port, or 0 for an ephemeral port")
	cmd.PersistentFlags().BoolVar(&headless, "headless", false, "stream logs instead of launching the terminal dashboard")
	AddFormatFlag(cmd, &format)
	cmd.AddCommand(
		newStartCommand(&configPath, &host, &port, opts),
		newStatusCommand(&configPath, &host, &port, opts),
		newLogsCommand(&configPath, opts),
		newAuthCommand(&configPath, &host, &port, opts),
		newDoctorCommand(&configPath, &env, &logLevel, &host, &port, opts),
		newFixCommand(&configPath, opts),
		newCITriggerLabelCommand(opts),
		newDevRuntimeCommand(&host, &port, opts),
		newHubCommand(opts),
		newArtifactCommand(opts.lookupEnv),
		newInitCommand(&configPath, opts),
		newAddProjectCommand(&configPath, opts),
		newRefreshProjectCommand(&configPath, opts),
		newPauseProjectCommand(&configPath, opts),
		newEditProjectCommand(&configPath, opts, OperationUnpauseProject, "unpause", "Unpause a project", func(project *globalconfig.Project) error {
			clearProjectPause(project)
			return nil
		}),
		newResumeProjectCommand(&configPath, opts),
		newGitHubLocalCommand(&configPath, opts),
		newWorkItemCommand(&configPath, opts),
		newBudgetCommand(&configPath, opts),
		newAuditCommand(&configPath, &host, &port, opts),
		newKeyCommand(&configPath, opts),
		newConfigCommand(&configPath, opts),
		newCapacityCommand(&configPath, &host, &port, opts),
		newExposureCommand(&configPath, opts),
		newIssueCommand(&configPath, &host, &port, opts),
		newMCPCommand(&configPath, &host, &port, opts),
		newStateCommand(&configPath, &host, &port, opts),
		newOnboardingCommand(&configPath, opts),
		newPromoteCommand(&configPath, opts),
		newRemoveProjectCommand(&configPath, opts),
	)
	cmd.SetHelpCommand(newHelpCommand(cmd, opts))
	ConfigureExamplesFirstHelp(cmd)
	ConfigureCommandSuggestions(cmd)
	wrapHintedErrors(cmd, opts)

	return cmd
}

func defaultOptions() options {
	return options{
		resolvePath: globalconfig.ResolvePath,
		read: func(path string) (globalconfig.Config, error) {
			return globalconfig.Read(path)
		},
		readProject: func(path string, projectID string) (globalconfig.Config, []string, error) {
			return globalconfig.ReadProject(path, projectID)
		},
		readDoctor: func(path string) (globalconfig.Config, error) {
			return globalconfig.Read(path, globalconfig.WithMissingWorkflowFiles())
		},
		readDoctorProject: func(path string, projectID string) (globalconfig.Config, []string, error) {
			return globalconfig.ReadProject(path, projectID, globalconfig.WithMissingWorkflowFiles())
		},
		readOrDefault: func(path string) (globalconfig.Config, error) {
			return globalconfig.ReadOrDefault(path, globalconfig.WithProjectPathLiterals())
		},
		write: func(path string, cfg globalconfig.Config) error {
			return globalconfig.Write(path, cfg, globalconfig.WithProjectPathLiterals())
		},
		boot:        defaultBoot,
		signal:      noSignal,
		lookupEnv:   os.Getenv,
		ghAuthToken: defaultGHAuthToken,
		runCommand:  defaultCommandRunner,
		httpDo:      http.DefaultClient.Do,
		captureDemo: runDemoCapture,
		stdoutTTY:   stdoutIsTTY,
		service:     defaultServiceFactory,
	}
}

func defaultCommandRunner(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...) // #nosec G204 -- command runners receive internally selected executables and bypass a shell.
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(output), ctx.Err()
	}
	if err != nil {
		if detail := strings.TrimSpace(string(output)); detail != "" {
			return string(output), fmt.Errorf("%w: %s", err, detail)
		}
		return string(output), err
	}
	return string(output), nil
}

func newHelpCommand(root *cobra.Command, opts options) *cobra.Command {
	return &cobra.Command{
		Use:   "help [command]",
		Short: "Show help for a command",
		Example: strings.TrimSpace(`detent help add-project
detent --format json help`),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := commandOutputFormat(cmd, opts)
			if err != nil {
				return err
			}
			if format == OutputFormatJSON {
				return WriteJSON(cmd.OutOrStdout(), NewCommandCatalog(root))
			}

			target := root
			if len(args) > 0 {
				found, _, findErr := root.Find(args)
				if findErr != nil {
					return findErr
				}
				target = found
			}
			target.SetOut(cmd.OutOrStdout())
			target.SetErr(cmd.ErrOrStderr())
			return target.Help()
		},
	}
}

func ConfigureExamplesFirstHelp(cmd *cobra.Command) {
	if cmd == nil {
		return
	}
	cmd.SetUsageTemplate(examplesFirstUsageTemplate)
	cmd.SetHelpFunc(examplesFirstHelp)
	for _, child := range cmd.Commands() {
		ConfigureExamplesFirstHelp(child)
	}
}

func examplesFirstHelp(cmd *cobra.Command, _ []string) {
	format, err := helpOutputFormat(cmd)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}
	if format == OutputFormatJSON {
		if err := WriteJSON(cmd.OutOrStdout(), NewCommandCatalog(cmd.Root())); err != nil {
			cmd.PrintErrln(err)
		}
		return
	}
	if err := writeExamplesFirstHelp(cmd.OutOrStdout(), cmd); err != nil {
		cmd.PrintErrln(err)
	}
}

func helpOutputFormat(cmd *cobra.Command) (OutputFormat, error) {
	value, set := outputFormatFlagValue(cmd)
	return ResolveOutputFormat(value, set, os.Getenv(outputFormatEnv), true)
}

func writeExamplesFirstHelp(out io.Writer, cmd *cobra.Command) error {
	if strings.TrimSpace(cmd.Example) != "" {
		if _, err := fmt.Fprintf(out, "Examples:\n%s\n\n", strings.TrimRight(cmd.Example, "\n")); err != nil {
			return err
		}
	}

	description := strings.TrimSpace(cmd.Long)
	if description == "" {
		description = strings.TrimSpace(cmd.Short)
	}
	if description != "" {
		if _, err := fmt.Fprintf(out, "%s\n\n", description); err != nil {
			return err
		}
	}

	if cmd.Runnable() || cmd.HasSubCommands() {
		_, err := fmt.Fprint(out, cmd.UsageString())
		return err
	}
	return nil
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	if flag == nil {
		flag = cmd.InheritedFlags().Lookup(name)
	}
	return flag != nil && flag.Changed
}

func noSignal(context.Context, Signal) error {
	return nil
}

func stdoutIsTTY() bool {
	return WriterIsTTY(os.Stdout)
}

func hintedError(cause error, message string, hint string, commands ...string) error {
	return &HintedError{
		Err:      cause,
		Message:  message,
		Hint:     strings.TrimSpace(hint),
		Commands: append([]string(nil), commands...),
	}
}

func exampleHint(command string) string {
	return "e.g. " + command
}

func wrapHintedErrors(cmd *cobra.Command, opts options) {
	if cmd == nil {
		return
	}
	if cmd.RunE != nil {
		runE := cmd.RunE
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			err := runE(cmd, args)
			if err != nil {
				if cmd.Root().SilenceErrors {
					return err
				}
				if writeErr := writeErrorHint(cmd.ErrOrStderr(), err); writeErr != nil {
					return errors.Join(err, writeErr)
				}
			}
			return err
		}
	}
	for _, child := range cmd.Commands() {
		wrapHintedErrors(child, opts)
	}
}

func writeErrorHint(out io.Writer, err error) error {
	hint, _, ok := HintFor(err)
	if !ok || strings.TrimSpace(hint) == "" {
		return nil
	}
	_, writeErr := fmt.Fprintf(out, "Hint: %s\n", hint)
	return writeErr
}

func newInitCommand(configPath *string, opts options) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:     "init",
		Short:   "Create a default global config",
		Example: "detent init --config ~/.config/detent/global.yaml",
		Args:    NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			resolution, err := resolveConfigPathResolution(*configPath, opts)
			if err != nil {
				return err
			}
			path := resolution.Path

			cfg, err := globalconfig.DefaultAt(path)
			if err != nil {
				return err
			}
			path = cfg.Path
			if err := checkInitTarget(path, force); err != nil {
				return err
			}
			if err := opts.write(path, cfg); err != nil {
				return err
			}
			if err := opts.signal(cmd.Context(), Signal{
				Operation: OperationInit,
			}); err != nil {
				return err
			}
			return out.Write(nil, initResult{
				Status: "ok",
				Path:   path,
				Rule:   string(resolution.Rule),
			})
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "overwrite an existing global config")
	return cmd
}

func checkInitTarget(path string, force bool) error {
	_, err := os.Stat(path)
	if err == nil {
		if !force {
			return hintedError(
				ErrConfigExists,
				fmt.Sprintf("%s: %s", ErrConfigExists, path),
				"run detent init --force to overwrite it, or edit the file reported by detent config path",
				forceInitCommand,
				configPathCommand,
			)
		}
		if _, readErr := os.ReadFile(path); readErr != nil {
			return fmt.Errorf("read existing global config %s: %w", path, readErr)
		}
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("stat global config %s: %w", path, err)
}

func newAddProjectCommand(configPath *string, opts options) *cobra.Command {
	var cfg globalconfig.Project
	cmd := &cobra.Command{
		Use:     "add-project",
		Short:   "Add a project to global config",
		Example: "detent add-project --id api --workflow ~/code/api/WORKFLOW.md --workdir ~/code/api --weight 2",
		Args:    NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			path, err := resolveConfigPath(*configPath, opts)
			if err != nil {
				return err
			}
			cfg.ID = strings.TrimSpace(cfg.ID)
			cfg.WorkflowRef = strings.TrimSpace(cfg.WorkflowRef)
			cfg.CredentialRef = strings.TrimSpace(cfg.CredentialRef)
			cfg.PausedReason = strings.TrimSpace(cfg.PausedReason)
			cfg.PausedUntilIssue = strings.TrimSpace(cfg.PausedUntilIssue)
			cfg.PausedUntil = strings.TrimSpace(cfg.PausedUntil)
			if err := validateProjectFlags(cfg); err != nil {
				return err
			}
			if cfg.Paused {
				cfg.PausedAt = time.Now().UTC().Format(time.RFC3339)
			}

			global, err := opts.readOrDefault(path)
			if err != nil {
				return err
			}
			if projectIndex(global.Projects, cfg.ID) >= 0 {
				return projectExistsError(cfg.ID)
			}

			global.Projects = append(global.Projects, cfg)
			if err := opts.write(path, global); err != nil {
				return err
			}
			gitStatus := inspectGlobalConfigGit(cmd.Context(), path, opts.runCommand)
			if err := opts.signal(cmd.Context(), Signal{
				Operation: OperationAddProject,
				ProjectID: cfg.ID,
				Project:   cfg,
			}); err != nil {
				return err
			}
			result := newProjectResult(cfg)
			result.GlobalConfigGitStatus = gitStatus
			return out.Write(func(w io.Writer) error {
				return writeProjectRegistrationPretty(w, path, result)
			}, result)
		},
	}
	cmd.Flags().StringVar(&cfg.ID, "id", "", "project id")
	cmd.Flags().StringVar(&cfg.Workflow, "workflow", "", "project workflow path")
	cmd.Flags().StringVar(&cfg.WorkflowRef, "workflow-ref", "", "git ref for loading the workflow")
	cmd.Flags().StringVar(&cfg.Workdir, "workdir", "", "project worktree root")
	cmd.Flags().IntVar(&cfg.Weight, "weight", 1, "project scheduling weight")
	cmd.Flags().IntVar(&cfg.Priority, "priority", 0, "project dispatch priority")
	cmd.Flags().BoolVar(&cfg.Paused, "paused", false, "add the project in a paused state")
	cmd.Flags().StringVar(&cfg.PausedReason, "reason", "", "reason the project is paused")
	cmd.Flags().StringVar(&cfg.PausedUntilIssue, "until-issue", "", "auto-unpause after this tracker issue closes")
	cmd.Flags().StringVar(&cfg.PausedUntil, "until", "", "auto-unpause at this RFC 3339 timestamp")
	cmd.Flags().StringVar(&cfg.CredentialRef, "credential-ref", "", "project credential reference")
	return cmd
}

func validateProjectFlags(cfg globalconfig.Project) error {
	switch {
	case cfg.ID == "":
		return WrapValidation(hintedError(nil, "--id is required", exampleHint(addProjectExampleCommand), addProjectExampleCommand))
	case strings.TrimSpace(cfg.Workflow) == "":
		return WrapValidation(hintedError(nil, "--workflow is required", exampleHint(addProjectExampleCommand), addProjectExampleCommand))
	case strings.TrimSpace(cfg.Workdir) == "":
		return WrapValidation(hintedError(nil, "--workdir is required", exampleHint(addProjectExampleCommand), addProjectExampleCommand))
	case cfg.Weight <= 0:
		command := addProjectExampleCommand + " --weight 1"
		return WrapValidation(hintedError(nil, "--weight must be positive", exampleHint(command), command))
	case cfg.Paused && strings.TrimSpace(cfg.PausedReason) == "":
		command := addProjectExampleCommand + ` --paused --reason "maintenance"`
		return WrapValidation(hintedError(nil, "--reason is required with --paused", exampleHint(command), command))
	case !cfg.Paused && pauseMetadataConfigured(cfg):
		return WrapValidation(errors.New("--reason, --until-issue, and --until require --paused"))
	case strings.TrimSpace(cfg.PausedUntilIssue) != "" && strings.TrimSpace(cfg.PausedUntil) != "":
		return WrapValidation(errors.New("--until-issue and --until are mutually exclusive"))
	case strings.TrimSpace(cfg.PausedUntil) != "":
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(cfg.PausedUntil)); err != nil {
			return WrapValidation(errors.New("--until must be an RFC 3339 timestamp"))
		}
		return nil
	default:
		return nil
	}
}

type projectEdit func(*globalconfig.Project) error

type projectValidation func(context.Context, globalconfig.Config, int) error

func newPauseProjectCommand(configPath *string, opts options) *cobra.Command {
	var reason string
	var untilIssue string
	var until string
	cmd := &cobra.Command{
		Use:     "pause PROJECT_ID",
		Short:   "Pause a project",
		Example: `detent pause api --reason "maintenance" --until-issue digitaldrywood/api#42`,
		Args:    ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reason = strings.TrimSpace(reason)
			untilIssue = strings.TrimSpace(untilIssue)
			until = strings.TrimSpace(until)
			if err := validatePauseFlags(reason, untilIssue, until); err != nil {
				return err
			}
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			updated, err := updateProject(cmd.Context(), *configPath, opts, OperationPauseProject, args[0], func(project *globalconfig.Project) error {
				project.Paused = true
				project.PausedReason = reason
				project.PausedAt = time.Now().UTC().Format(time.RFC3339)
				project.PausedUntilIssue = untilIssue
				project.PausedUntil = until
				return nil
			}, func(ctx context.Context, cfg globalconfig.Config, index int) error {
				return validatePauseIssueReference(ctx, cfg, index)
			})
			if err != nil {
				return err
			}
			return out.Write(nil, projectEditResult(OperationPauseProject, updated))
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "reason the project is paused")
	cmd.Flags().StringVar(&untilIssue, "until-issue", "", "auto-unpause after this tracker issue closes")
	cmd.Flags().StringVar(&until, "until", "", "auto-unpause at this RFC 3339 timestamp")
	return cmd
}

func validatePauseFlags(reason string, untilIssue string, until string) error {
	switch {
	case strings.TrimSpace(reason) == "":
		return WrapValidation(errors.New("--reason is required"))
	case strings.TrimSpace(untilIssue) != "" && strings.TrimSpace(until) != "":
		return WrapValidation(errors.New("--until-issue and --until are mutually exclusive"))
	case strings.TrimSpace(until) != "":
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(until)); err != nil {
			return WrapValidation(errors.New("--until must be an RFC 3339 timestamp"))
		}
	}
	return nil
}

func newResumeProjectCommand(configPath *string, opts options) *cobra.Command {
	var duration string
	var until string
	cmd := &cobra.Command{
		Use:     "resume PROJECT_ID",
		Short:   "Temporarily allow dispatch outside active hours",
		Example: "detent resume api --for 2h",
		Args:    ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			now := time.Now().UTC()
			overrideUntil, err := activeHoursOverrideDeadline(now, duration, until)
			if err != nil {
				return err
			}
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			updated, err := updateProject(cmd.Context(), *configPath, opts, OperationResumeProject, args[0], func(project *globalconfig.Project) error {
				if project.Paused {
					return WrapValidation(errors.New("project is manually paused; run detent unpause before adding an active-hours override"))
				}
				project.ActiveHoursOverrideUntil = overrideUntil.Format(time.RFC3339)
				return nil
			})
			if err != nil {
				return err
			}
			return out.Write(nil, projectEditResult(OperationResumeProject, updated))
		},
	}
	cmd.Flags().StringVar(&duration, "for", "", "allow dispatch for this Go duration, such as 2h")
	cmd.Flags().StringVar(&until, "until", "", "allow dispatch until this RFC 3339 timestamp")
	return cmd
}

func activeHoursOverrideDeadline(now time.Time, duration string, until string) (time.Time, error) {
	duration = strings.TrimSpace(duration)
	until = strings.TrimSpace(until)
	if duration == "" && until == "" {
		return time.Time{}, WrapValidation(errors.New("one of --for or --until is required"))
	}
	if duration != "" && until != "" {
		return time.Time{}, WrapValidation(errors.New("--for and --until are mutually exclusive"))
	}
	if duration != "" {
		parsed, err := time.ParseDuration(duration)
		if err != nil || parsed <= 0 {
			return time.Time{}, WrapValidation(errors.New("--for must be a positive Go duration such as 2h"))
		}
		return now.Add(parsed), nil
	}
	parsed, err := time.Parse(time.RFC3339, until)
	if err != nil {
		return time.Time{}, WrapValidation(errors.New("--until must be an RFC 3339 timestamp"))
	}
	if !parsed.After(now) {
		return time.Time{}, WrapValidation(errors.New("--until must be in the future"))
	}
	return parsed, nil
}

func pauseMetadataConfigured(project globalconfig.Project) bool {
	return strings.TrimSpace(project.PausedReason) != "" ||
		strings.TrimSpace(project.PausedUntilIssue) != "" ||
		strings.TrimSpace(project.PausedUntil) != ""
}

func clearProjectPause(project *globalconfig.Project) {
	if project == nil {
		return
	}
	project.Paused = false
	project.PausedReason = ""
	project.PausedAt = ""
	project.PausedUntilIssue = ""
	project.PausedUntil = ""
}

func newEditProjectCommand(configPath *string, opts options, operation Operation, use string, short string, edit projectEdit) *cobra.Command {
	return &cobra.Command{
		Use:     use + " PROJECT_ID",
		Short:   short,
		Example: "detent " + use + " api",
		Args:    ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			updated, err := updateProject(cmd.Context(), *configPath, opts, operation, args[0], edit)
			if err != nil {
				return err
			}
			return out.Write(nil, projectEditResult(operation, updated))
		},
	}
}

func newConfigCommand(configPath *string, opts options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "config",
		Short:   "Inspect global config settings",
		Example: "detent config path",
	}
	cmd.AddCommand(newConfigPathCommand(configPath, opts))
	return cmd
}

func newConfigPathCommand(configPath *string, opts options) *cobra.Command {
	return &cobra.Command{
		Use:     "path",
		Short:   "Print the resolved global config path",
		Example: "detent --format json config path",
		Args:    NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			resolution, err := resolveConfigPathResolution(*configPath, opts)
			if err != nil {
				return err
			}
			return out.Write(func(out io.Writer) error {
				_, err := fmt.Fprintf(out, "path: %s\nrule: %s\n", resolution.Path, resolution.Rule)
				return err
			}, configPathResult{
				Path: resolution.Path,
				Rule: string(resolution.Rule),
			})
		},
	}
}

func newPromoteCommand(configPath *string, opts options) *cobra.Command {
	var priority int
	cmd := &cobra.Command{
		Use:     "promote PROJECT_ID",
		Short:   "Promote a project priority",
		Example: "detent promote api --priority 1",
		Args:    ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if priority <= 0 {
				return WrapValidation(hintedError(nil, "--priority must be positive", exampleHint(promoteExampleCommand), promoteExampleCommand))
			}
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			updated, err := updateProject(cmd.Context(), *configPath, opts, OperationPromoteProject, args[0], func(project *globalconfig.Project) error {
				project.Priority = priority
				return nil
			})
			if err != nil {
				return err
			}
			return out.Write(nil, projectPriorityResult{
				Status:   "ok",
				Project:  updated.ID,
				Priority: updated.Priority,
			})
		},
	}
	cmd.Flags().IntVar(&priority, "priority", 1, "project priority rank")
	return cmd
}

func updateProject(
	ctx context.Context,
	configPath string,
	opts options,
	operation Operation,
	id string,
	edit projectEdit,
	validations ...projectValidation,
) (globalconfig.Project, error) {
	path, err := resolveConfigPath(configPath, opts)
	if err != nil {
		return globalconfig.Project{}, err
	}
	cfg, err := opts.readOrDefault(path)
	if err != nil {
		return globalconfig.Project{}, err
	}

	id = strings.TrimSpace(id)
	index := projectIndex(cfg.Projects, id)
	if index < 0 {
		return globalconfig.Project{}, projectNotFoundError(id, cfg.Projects)
	}
	if err := edit(&cfg.Projects[index]); err != nil {
		return globalconfig.Project{}, err
	}
	for _, validate := range validations {
		if validate == nil {
			continue
		}
		if err := validate(ctx, cfg, index); err != nil {
			return globalconfig.Project{}, err
		}
	}
	if err := opts.write(path, cfg); err != nil {
		return globalconfig.Project{}, err
	}

	if err := opts.signal(ctx, Signal{
		Operation: operation,
		ProjectID: cfg.Projects[index].ID,
		Project:   cfg.Projects[index],
	}); err != nil {
		return globalconfig.Project{}, err
	}
	return cfg.Projects[index], nil
}

func validatePauseIssueReference(ctx context.Context, cfg globalconfig.Config, projectIndex int) error {
	project := cfg.Projects[projectIndex]
	reference := strings.TrimSpace(project.PausedUntilIssue)
	if reference == "" {
		return nil
	}

	deps := runtimeDeps{}.withDefaults()
	trackers := make([]pause.Tracker, 0, len(cfg.Projects))
	trackerKinds := make(map[string]string, len(cfg.Projects))
	for _, candidate := range cfg.Projects {
		workflow, err := loadRuntimeProjectWorkflow(ctx, candidate, deps)
		if err != nil {
			if strings.EqualFold(strings.TrimSpace(candidate.ID), strings.TrimSpace(project.ID)) {
				return pauseIssueValidationError(project.ID, reference, fmt.Errorf("load project workflow: %w", err))
			}
			continue
		}
		projectID := strings.TrimSpace(candidate.ID)
		trackers = append(trackers, pause.Tracker{
			ProjectID:  projectID,
			Kind:       workflow.Config.Tracker.Kind,
			Repository: workflow.Config.Tracker.Repository,
		})
		trackerKinds[strings.ToLower(projectID)] = workflow.Config.Tracker.Kind
	}

	resolution, err := pause.ResolveReference(project.ID, reference, trackers)
	if err != nil {
		return pauseIssueValidationError(project.ID, reference, err)
	}
	kind := trackerKinds[strings.ToLower(strings.TrimSpace(resolution.ProjectID))]
	if !pauseTrackerSupportsIssueReferences(kind) {
		return pauseIssueValidationError(
			project.ID,
			reference,
			fmt.Errorf("resolver project %s tracker kind %s cannot resolve issue references", resolution.ProjectID, kind),
		)
	}
	return nil
}

func pauseTrackerSupportsIssueReferences(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case workflowconfig.TrackerGitHub, workflowconfig.TrackerGitHubLocal, workflowconfig.TrackerLocalSQLite, workflowconfig.TrackerMemory:
		return true
	default:
		return false
	}
}

func pauseIssueValidationError(projectID string, reference string, err error) error {
	return WrapValidation(fmt.Errorf(
		"cannot pause project %q with --until-issue %q: %w",
		strings.TrimSpace(projectID),
		strings.TrimSpace(reference),
		err,
	))
}

func newRemoveProjectCommand(configPath *string, opts options) *cobra.Command {
	return &cobra.Command{
		Use:     "remove-project PROJECT_ID",
		Short:   "Remove a project from global config",
		Example: "detent remove-project api",
		Args:    ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := OutputForCommand(cmd)
			if err != nil {
				return err
			}
			path, err := resolveConfigPath(*configPath, opts)
			if err != nil {
				return err
			}
			cfg, err := opts.readOrDefault(path)
			if err != nil {
				return err
			}

			id := strings.TrimSpace(args[0])
			index := projectIndex(cfg.Projects, id)
			if index < 0 {
				return projectNotFoundError(id, cfg.Projects)
			}
			removed := cfg.Projects[index]
			cfg.Projects = append(cfg.Projects[:index], cfg.Projects[index+1:]...)
			if cfg.Projects == nil {
				cfg.Projects = []globalconfig.Project{}
			}
			if err := opts.write(path, cfg); err != nil {
				return err
			}

			if err := opts.signal(cmd.Context(), Signal{
				Operation: OperationRemoveProject,
				ProjectID: removed.ID,
				Project:   removed,
			}); err != nil {
				return err
			}
			return out.Write(nil, projectRemovedResult{
				Status:  "ok",
				Project: removed.ID,
				Removed: true,
			})
		},
	}
}

func newProjectResult(project globalconfig.Project) projectResult {
	return projectResult{
		ID:               project.ID,
		Workflow:         project.Workflow,
		WorkflowRef:      project.WorkflowRef,
		Workdir:          project.Workdir,
		Weight:           project.Weight,
		Priority:         project.Priority,
		Paused:           project.Paused,
		PausedReason:     project.PausedReason,
		PausedUntilIssue: project.PausedUntilIssue,
		PausedUntil:      project.PausedUntil,
		CredentialRef:    project.CredentialRef,
	}
}

func writeProjectRegistrationPretty(w io.Writer, path string, result projectResult) error {
	if result.GlobalConfigGitStatus == nil {
		return nil
	}
	status := result.GlobalConfigGitStatus
	lines := []string{
		fmt.Sprintf("project %s registered in %s", result.ID, path),
		"global_config_repository: " + status.RepositoryRoot,
		"global_config_status: " + status.trackedStatus(),
	}
	if status.Dirty {
		lines = append(lines, "warning: "+globalConfigGitDurabilityWarning(status.RepositoryRoot))
	}
	_, err := fmt.Fprintln(w, strings.Join(lines, "\n"))
	return err
}

func projectEditResult(operation Operation, project globalconfig.Project) any {
	switch operation {
	case OperationPauseProject, OperationUnpauseProject:
		return projectPausedResult{
			Status:           "ok",
			Project:          project.ID,
			Paused:           project.Paused,
			PausedReason:     project.PausedReason,
			PausedUntilIssue: project.PausedUntilIssue,
			PausedUntil:      project.PausedUntil,
		}
	case OperationResumeProject:
		return projectResumedResult{
			Status:                   "ok",
			Project:                  project.ID,
			ActiveHoursOverrideUntil: project.ActiveHoursOverrideUntil,
		}
	default:
		return newProjectResult(project)
	}
}

func resolveConfigPath(path string, opts options) (string, error) {
	resolution, err := resolveConfigPathResolution(path, opts)
	if err != nil {
		return "", err
	}
	return resolution.Path, nil
}

func resolveConfigPathResolution(path string, opts options) (globalconfig.PathResolution, error) {
	if opts.resolvePath == nil {
		opts.resolvePath = globalconfig.ResolvePath
	}
	return opts.resolvePath(path)
}

func projectIndex(projects []globalconfig.Project, id string) int {
	id = strings.TrimSpace(id)
	for index, project := range projects {
		if strings.TrimSpace(project.ID) == id {
			return index
		}
	}
	return -1
}

func projectExistsError(id string) error {
	id = strings.TrimSpace(id)
	return hintedError(
		ErrProjectExists,
		fmt.Sprintf("project %q already exists", id),
		fmt.Sprintf("project id %q is already taken; run detent config path to inspect current projects before choosing a new --id", id),
		configPathCommand,
	)
}

func projectNotFoundError(id string, projects []globalconfig.Project) error {
	id = strings.TrimSpace(id)
	ids := projectHintIDs(projects)
	if len(ids) == 0 {
		return hintedError(
			ErrProjectNotFound,
			fmt.Sprintf("project %q not found", id),
			"no projects are configured; run detent add-project to add one",
			addProjectExampleCommand,
		)
	}

	hint := "available: " + strings.Join(ids, ", ")
	if closest := closestProjectID(id, ids); closest != "" {
		hint += fmt.Sprintf("\ndid you mean %q? see `%s`, then retry", closest, configPathCommand)
	} else {
		hint += fmt.Sprintf("\nsee `%s`, then retry", configPathCommand)
	}
	return hintedError(ErrProjectNotFound, fmt.Sprintf("project %q not found", id), hint, configPathCommand)
}

func projectHintIDs(projects []globalconfig.Project) []string {
	ids := make([]string, 0, len(projects))
	for _, project := range projects {
		id := strings.TrimSpace(project.ID)
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func closestProjectID(target string, ids []string) string {
	target = strings.TrimSpace(target)
	bestID := ""
	bestDistance := 0
	for _, id := range ids {
		distance := levenshteinDistance(target, id)
		if bestID == "" || distance < bestDistance {
			bestID = id
			bestDistance = distance
		}
	}
	return bestID
}

func levenshteinDistance(a string, b string) int {
	ar := []rune(strings.ToLower(a))
	br := []rune(strings.ToLower(b))
	if len(ar) == 0 {
		return len(br)
	}
	if len(br) == 0 {
		return len(ar)
	}

	previous := make([]int, len(br)+1)
	for j := range previous {
		previous[j] = j
	}
	for i, ra := range ar {
		current := make([]int, len(br)+1)
		current[0] = i + 1
		for j, rb := range br {
			cost := 0
			if ra != rb {
				cost = 1
			}
			current[j+1] = minInt3(
				current[j]+1,
				previous[j+1]+1,
				previous[j]+cost,
			)
		}
		previous = current
	}
	return previous[len(br)]
}

func minInt3(a int, b int, c int) int {
	min := a
	if b < min {
		min = b
	}
	if c < min {
		min = c
	}
	return min
}
