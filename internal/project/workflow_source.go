package project

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	configwatcher "github.com/digitaldrywood/detent/internal/config/watcher"
)

var (
	errMissingWorkflowRefRoot = errors.New("workflow_ref requires project workdir")
	errRelativeWorkflowPath   = errors.New("workflow path must be absolute or home-relative when workflow_ref is empty")
	errUnsafeWorkflowPath     = errors.New("workflow path must stay inside the source root")
)

const workflowGitWaitDelay = time.Second

type workflowGitRefSource struct {
	sourceRoot string
	ref        string
	path       string
}

type gitRefWorkflowWatcher struct {
	currentProject func() globalconfig.Project
	agents         workflowconfig.Agents
	budget         workflowconfig.AgentBudgetDefaults
	source         workflowGitRefSource
	interval       time.Duration
	logger         *slog.Logger
}

func LoadWorkflow(cfg globalconfig.Project) (workflowconfig.Workflow, error) {
	return LoadWorkflowContext(context.Background(), cfg)
}

func LoadWorkflowContext(ctx context.Context, cfg globalconfig.Project) (workflow workflowconfig.Workflow, err error) {
	defer func() {
		if err == nil {
			workflow.Config = workflow.Config.WithAgentDefaults(cfg.GlobalAgents, cfg.GlobalBudget)
		}
	}()
	if strings.TrimSpace(cfg.WorkflowRef) == "" {
		workflowPath, err := plainWorkflowPath(cfg.Workflow)
		if err != nil {
			return workflowconfig.Workflow{}, err
		}
		return workflowconfig.LoadWorkflow(workflowPath)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	source, err := newWorkflowGitRefSource(cfg)
	if err != nil {
		return workflowconfig.Workflow{}, err
	}
	workflow, _, err = source.load(ctx)
	return workflow, err
}

func workflowSourceDisplayPath(cfg globalconfig.Project) string {
	if strings.TrimSpace(cfg.WorkflowRef) == "" {
		path, err := plainWorkflowPath(cfg.Workflow)
		if err != nil {
			return strings.TrimSpace(cfg.Workflow)
		}
		return path
	}
	source, err := newWorkflowGitRefSource(cfg)
	if err != nil {
		return strings.TrimSpace(cfg.Workflow)
	}
	return source.displayPath()
}

func workflowFileModifiedAt(cfg globalconfig.Project) time.Time {
	if strings.TrimSpace(cfg.WorkflowRef) != "" {
		return time.Time{}
	}
	path, err := plainWorkflowPath(cfg.Workflow)
	if err != nil {
		return time.Time{}
	}
	var modified time.Time
	for _, candidate := range []string{
		path,
		workflowconfig.DefinitionPath(path),
		workflowconfig.LocalWorkflowPath(path),
		workflowconfig.LocalDefinitionPath(path),
		filepath.Join(filepath.Dir(path), workflowconfig.BacklogAdmissionEffortFileAgents),
	} {
		info, err := os.Stat(candidate)
		if err == nil && info.ModTime().After(modified) {
			modified = info.ModTime()
		}
	}
	return modified.UTC()
}

func newWorkflowGitRefSource(cfg globalconfig.Project) (workflowGitRefSource, error) {
	sourceRoot := strings.TrimSpace(cfg.Workdir)
	if sourceRoot == "" {
		return workflowGitRefSource{}, errMissingWorkflowRefRoot
	}

	workflowPath, err := workflowRefPath(sourceRoot, cfg.Workflow)
	if err != nil {
		return workflowGitRefSource{}, err
	}

	ref := strings.TrimSpace(cfg.WorkflowRef)
	if ref == "" {
		return workflowGitRefSource{}, errors.New("workflow_ref must not be blank")
	}
	if strings.ContainsAny(ref, "\r\n") {
		return workflowGitRefSource{}, errors.New("workflow_ref must be a single line")
	}

	return workflowGitRefSource{
		sourceRoot: sourceRoot,
		ref:        ref,
		path:       workflowPath,
	}, nil
}

func plainWorkflowPath(workflowPath string) (string, error) {
	workflowPath = strings.TrimSpace(workflowPath)
	if workflowPath == "" {
		return "", errors.New("workflow path must not be blank")
	}
	if filepath.IsAbs(workflowPath) {
		return filepath.Clean(workflowPath), nil
	}

	if workflowPath == "~" || strings.HasPrefix(workflowPath, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home workflow path: %w", err)
		}
		if workflowPath == "~" {
			return filepath.Clean(home), nil
		}
		return filepath.Join(home, strings.TrimPrefix(workflowPath, "~/")), nil
	}

	return "", errRelativeWorkflowPath
}

func workflowRefPath(sourceRoot string, workflowPath string) (string, error) {
	workflowPath = strings.TrimSpace(workflowPath)
	if workflowPath == "" {
		return "", errors.New("workflow path must not be blank")
	}

	if filepath.IsAbs(workflowPath) {
		sourceRoot, err := filepath.Abs(sourceRoot)
		if err != nil {
			return "", fmt.Errorf("resolve source root: %w", err)
		}
		workflowPath, err = filepath.Abs(workflowPath)
		if err != nil {
			return "", fmt.Errorf("resolve workflow path: %w", err)
		}
		rel, err := filepath.Rel(sourceRoot, workflowPath)
		if err != nil {
			return "", fmt.Errorf("relativize workflow path: %w", err)
		}
		workflowPath = rel
	}

	workflowPath = filepath.Clean(workflowPath)
	if workflowPath == "." || workflowPath == ".." || strings.HasPrefix(workflowPath, ".."+string(filepath.Separator)) {
		return "", errUnsafeWorkflowPath
	}
	return filepath.ToSlash(workflowPath), nil
}

func (s workflowGitRefSource) load(ctx context.Context) (workflowconfig.Workflow, string, error) {
	revision, err := s.revision(ctx)
	if err != nil {
		return workflowconfig.Workflow{}, "", err
	}

	raw, err := runWorkflowGit(ctx, s.sourceRoot, "show", revision+":"+s.path)
	if err != nil {
		return workflowconfig.Workflow{}, revision, fmt.Errorf("load workflow from %s: %w", s.displayPath(), err)
	}
	configPath := path.Join(path.Dir(s.path), "detent.yaml")
	configRaw, hasConfig, err := s.loadOptionalRefFile(ctx, revision, configPath)
	if err != nil {
		return workflowconfig.Workflow{}, revision, err
	}
	agentsPath := path.Join(path.Dir(s.path), workflowconfig.BacklogAdmissionEffortFileAgents)
	agentsRaw, hasAgents, err := s.loadOptionalRefFile(ctx, revision, agentsPath)
	if err != nil {
		return workflowconfig.Workflow{}, revision, err
	}
	localWorkflowPath := s.localPath()
	localRaw, hasLocalWorkflow, err := readOptionalWorkflowSourceFile(localWorkflowPath)
	if err != nil {
		return workflowconfig.Workflow{}, revision, fmt.Errorf("read local workflow overlay: %w", err)
	}
	localConfigPath := s.localConfigPath()
	localConfigRaw, hasLocalConfig, err := readOptionalWorkflowSourceFile(localConfigPath)
	if err != nil {
		return workflowconfig.Workflow{}, revision, fmt.Errorf("read local project config: %w", err)
	}
	workflow, err := workflowconfig.ParseProjectDefinition(workflowconfig.ProjectDefinitionSources{
		WorkflowPath:      revision + ":" + s.path,
		Workflow:          raw,
		ConfigPath:        revision + ":" + configPath,
		Config:            configRaw,
		HasConfig:         hasConfig,
		LocalWorkflowPath: localWorkflowPath,
		LocalWorkflow:     localRaw,
		HasLocalWorkflow:  hasLocalWorkflow,
		LocalConfigPath:   localConfigPath,
		LocalConfig:       localConfigRaw,
		HasLocalConfig:    hasLocalConfig,
		AgentsPath:        revision + ":" + agentsPath,
		Agents:            agentsRaw,
		HasAgents:         hasAgents,
	})
	if err != nil {
		return workflowconfig.Workflow{}, revision, err
	}
	workflow.Definition.Revision = revision
	return workflow, revision, nil
}

func (s workflowGitRefSource) loadOptionalRefFile(ctx context.Context, revision string, refPath string) ([]byte, bool, error) {
	raw, err := runWorkflowGit(ctx, s.sourceRoot, "show", revision+":"+refPath)
	if err == nil {
		return raw, true, nil
	}
	if _, existsErr := runWorkflowGit(ctx, s.sourceRoot, "cat-file", "-e", revision+":"+refPath); existsErr != nil {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("load project config from %s:%s: %w", revision, refPath, err)
}

func readOptionalWorkflowSourceFile(path string) ([]byte, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s workflowGitRefSource) localPath() string {
	return filepath.Join(s.sourceRoot, filepath.FromSlash(workflowconfig.LocalWorkflowPath(s.path)))
}

func (s workflowGitRefSource) localConfigPath() string {
	return filepath.Join(s.sourceRoot, filepath.FromSlash(workflowconfig.LocalDefinitionPath(s.path)))
}

func (s workflowGitRefSource) revision(ctx context.Context) (string, error) {
	output, err := runWorkflowGit(ctx, s.sourceRoot, "rev-parse", "--verify", s.ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve workflow ref %s: %w", s.ref, err)
	}
	revision := strings.TrimSpace(string(output))
	if revision == "" {
		return "", fmt.Errorf("resolve workflow ref %s: empty revision", s.ref)
	}
	return revision, nil
}

func (s workflowGitRefSource) displayPath() string {
	return s.ref + ":" + s.path
}

func runWorkflowGit(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...) // #nosec G204 -- workflow refs and paths are operator config and are passed as git arguments, not shell.
	cmd.WaitDelay = workflowGitWaitDelay
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git -C %s %s: %w\n%s", dir, strings.Join(args, " "), err, output)
	}
	return output, nil
}

func newGitRefWorkflowWatcher(cfg globalconfig.Project, interval time.Duration, logger *slog.Logger) (*gitRefWorkflowWatcher, error) {
	source, err := newWorkflowGitRefSource(cfg)
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		interval = time.Duration(workflowconfig.DefaultPollingIntervalMS) * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &gitRefWorkflowWatcher{
		agents:   cfg.GlobalAgents,
		budget:   cfg.GlobalBudget,
		source:   source,
		interval: interval,
		logger:   logger,
	}, nil
}

func (w *gitRefWorkflowWatcher) Watch(ctx context.Context) (<-chan configwatcher.Update, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	localWatcher, err := configwatcher.NewFile(w.source.localPath(), func(string) (workflowconfig.Workflow, error) {
		workflow, _, err := w.source.load(ctx)
		if err == nil {
			agents, budget := w.agents, w.budget
			if w.currentProject != nil {
				project := w.currentProject()
				agents, budget = project.GlobalAgents, project.GlobalBudget
			}
			workflow.Config = workflow.Config.WithAgentDefaults(agents, budget)
			err = workflow.Config.Validate()
		}
		return workflow, err
	}, configwatcher.WithFileLogger(w.logger), configwatcher.WithFileWatchPaths(w.source.localConfigPath()))
	if err != nil {
		return nil, fmt.Errorf("create local workflow overlay watcher: %w", err)
	}
	localUpdates, err := localWatcher.Watch(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch local workflow overlay: %w", err)
	}

	updates := make(chan configwatcher.Update, 1)
	go w.run(ctx, updates, localUpdates)
	return updates, nil
}

func (w *gitRefWorkflowWatcher) run(
	ctx context.Context,
	updates chan<- configwatcher.Update,
	localUpdates <-chan configwatcher.FileUpdate[workflowconfig.Workflow],
) {
	defer close(updates)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	lastRevision, lastErr := w.seed(ctx, updates)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastRevision, lastErr = w.reload(ctx, updates, lastRevision, lastErr)
		case update, ok := <-localUpdates:
			if !ok {
				w.send(ctx, updates, configwatcher.Update{
					Path:       w.source.localPath(),
					Err:        errors.New("local workflow overlay watcher stopped"),
					WatcherErr: true,
					At:         time.Now(),
				})
				return
			}
			w.send(ctx, updates, configwatcher.Update{
				Path:       update.Path,
				Workflow:   update.Value,
				Err:        update.Err,
				WatcherErr: update.WatcherErr,
				At:         update.At,
			})
			if update.WatcherErr {
				return
			}
		}
	}
}

func (w *gitRefWorkflowWatcher) seed(ctx context.Context, updates chan<- configwatcher.Update) (string, string) {
	_, revision, err := w.source.load(ctx)
	if err != nil {
		message := err.Error()
		w.send(ctx, updates, configwatcher.Update{Path: w.source.displayPath(), Err: err, At: time.Now()})
		return "", message
	}
	return revision, ""
}

func (w *gitRefWorkflowWatcher) reload(
	ctx context.Context,
	updates chan<- configwatcher.Update,
	lastRevision string,
	lastErr string,
) (string, string) {
	workflow, revision, err := w.source.load(ctx)
	if err != nil {
		message := err.Error()
		if message != lastErr {
			w.send(ctx, updates, configwatcher.Update{Path: w.source.displayPath(), Err: err, At: time.Now()})
		}
		return lastRevision, message
	}
	if revision == lastRevision {
		return lastRevision, ""
	}
	w.send(ctx, updates, configwatcher.Update{
		Path:     w.source.displayPath(),
		Workflow: workflow,
		At:       time.Now(),
	})
	return revision, ""
}

func (w *gitRefWorkflowWatcher) send(ctx context.Context, updates chan<- configwatcher.Update, update configwatcher.Update) {
	select {
	case updates <- update:
	case <-ctx.Done():
	}
}
