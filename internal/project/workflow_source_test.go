package project

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	configwatcher "github.com/digitaldrywood/detent/internal/config/watcher"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/testenv"
)

const (
	workflowGitHelperModeEnv     = "DETENT_WORKFLOW_GIT_HELPER_MODE"
	workflowGitHelperAddressEnv  = "DETENT_WORKFLOW_GIT_HELPER_ADDRESS"
	workflowGitHelperExitCodeEnv = "DETENT_WORKFLOW_GIT_HELPER_EXIT_CODE"
)

func TestMain(m *testing.M) {
	if err := testenv.ClearGitEnvironment(); err != nil {
		panic(err)
	}
	switch os.Getenv(workflowGitHelperModeEnv) {
	case "git":
		os.Exit(runWorkflowGitParentHelper())
	case "descendant":
		os.Exit(runWorkflowGitDescendantHelper())
	default:
		os.Exit(m.Run())
	}
}

func TestRunWorkflowGitBoundsInheritedOutputPipe(t *testing.T) {
	binDir := installWorkflowGitHelper(t)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(workflowGitHelperModeEnv, "git")

	tests := []struct {
		name          string
		exitCode      int
		wantWaitDelay bool
	}{
		{name: "successful process", wantWaitDelay: true},
		{name: "failed process", exitCode: 23},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatalf("ListenTCP() error = %v", err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatalf("SetDeadline() error = %v", err)
			}
			t.Setenv(workflowGitHelperAddressEnv, listener.Addr().String())
			t.Setenv(workflowGitHelperExitCodeEnv, strconv.Itoa(tt.exitCode))

			type result struct {
				output []byte
				err    error
			}
			results := make(chan result, 1)
			runDone := make(chan struct{})
			go func() {
				defer close(runDone)
				output, err := runWorkflowGit(t.Context(), t.TempDir(), "show", "HEAD:WORKFLOW.md")
				results <- result{output: output, err: err}
			}()

			type acceptedConnection struct {
				connection *net.TCPConn
				err        error
			}
			accepted := make(chan acceptedConnection, 1)
			go func() {
				connection, err := listener.AcceptTCP()
				accepted <- acceptedConnection{connection: connection, err: err}
			}()
			var connection *net.TCPConn
			select {
			case acceptResult := <-accepted:
				if acceptResult.err != nil {
					t.Fatalf("AcceptTCP() error = %v", acceptResult.err)
				}
				connection = acceptResult.connection
			case earlyResult := <-results:
				t.Fatalf("runWorkflowGit() returned before helper readiness: output = %q, error = %v", earlyResult.output, earlyResult.err)
			}
			defer connection.Close()
			if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatalf("SetDeadline() error = %v", err)
			}
			reader := bufio.NewReader(connection)
			pidLine, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read helper PID: %v", err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(pidLine))
			if err != nil {
				t.Fatalf("parse helper PID %q: %v", pidLine, err)
			}
			t.Cleanup(func() {
				if !t.Failed() {
					return
				}
				terminateWorkflowGitHelper(t, pid)
				select {
				case <-runDone:
				case <-time.After(5 * time.Second):
					t.Error("runWorkflowGit() goroutine did not stop after helper termination")
				}
			})

			var got result
			select {
			case got = <-results:
			case <-time.After(5 * time.Second):
				t.Fatal("runWorkflowGit() did not return after the Git process exited with an inherited output pipe")
			}

			if _, err := connection.Write([]byte{1}); err != nil {
				t.Fatalf("release helper descendant: %v", err)
			}
			done, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("wait for helper descendant: %v", err)
			}
			if strings.TrimSpace(done) != "done" {
				t.Fatalf("helper completion = %q, want done", done)
			}
			waitForWorkflowGitHelperExit(t, pid)

			if got.err == nil {
				t.Fatal("runWorkflowGit() error = nil, want bounded pipe error")
			}
			if tt.wantWaitDelay && !errors.Is(got.err, exec.ErrWaitDelay) {
				t.Fatalf("runWorkflowGit() error = %v, want %v", got.err, exec.ErrWaitDelay)
			}
			if tt.exitCode != 0 {
				var exitErr *exec.ExitError
				if !errors.As(got.err, &exitErr) || exitErr.ExitCode() != tt.exitCode {
					t.Fatalf("runWorkflowGit() error = %v, want exit code %d", got.err, tt.exitCode)
				}
			}
			if !strings.Contains(got.err.Error(), "workflow git stdout") || !strings.Contains(got.err.Error(), "workflow git stderr") {
				t.Fatalf("runWorkflowGit() error = %q, want captured stdout and stderr", got.err)
			}
			if got.output != nil {
				t.Fatalf("runWorkflowGit() output = %q, want nil on error", got.output)
			}
		})
	}
}

func TestLoadWorkflowUsesWorkingTreeWhenWorkflowRefUnset(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workflowPath := filepath.Join(root, "WORKFLOW.md")
	writeWorkflowSourceFile(t, workflowPath, "working tree")

	workflow, err := LoadWorkflow(globalconfig.Project{Workflow: workflowPath})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	if got := strings.TrimSpace(workflow.Prompt); got != "working tree" {
		t.Fatalf("Prompt = %q, want working tree", got)
	}
}

func TestLoadWorkflowRejectsRelativeWorkflowWhenWorkflowRefUnset(t *testing.T) {
	root := t.TempDir()
	daemonDir := filepath.Join(root, "daemon")
	projectDir := filepath.Join(root, "project")
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	writeWorkflowSourceFile(t, filepath.Join(daemonDir, "WORKFLOW.md"), "daemon")
	writeWorkflowSourceFile(t, filepath.Join(projectDir, "WORKFLOW.md"), "project")
	t.Chdir(daemonDir)

	_, err := LoadWorkflow(globalconfig.Project{
		Workflow: "WORKFLOW.md",
		Workdir:  projectDir,
	})
	if !errors.Is(err, errRelativeWorkflowPath) {
		t.Fatalf("LoadWorkflow() error = %v, want %v", err, errRelativeWorkflowPath)
	}
}

func TestLoadWorkflowUsesConfiguredGitRef(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	writeWorkflowSourceFile(t, filepath.Join(repo, "WORKFLOW.md"), "from ref")
	commitWorkflowSourceRepo(t, repo, "initial workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
	writeWorkflowSourceFile(t, filepath.Join(repo, "WORKFLOW.md"), "working tree")

	workflow, err := LoadWorkflow(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	if got := strings.TrimSpace(workflow.Prompt); got != "from ref" {
		t.Fatalf("Prompt = %q, want from ref", got)
	}
}

func TestLoadWorkflowUsesSplitDefinitionFromConfiguredGitRef(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "WORKFLOW.md"), []byte("from split ref\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(WORKFLOW.md) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "detent.yaml"), []byte("schema: 1\ntracker:\n  kind: memory\npolling:\n  interval_ms: 90000\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(detent.yaml) error = %v", err)
	}
	runWorkflowSourceGit(t, repo, "add", "WORKFLOW.md", "detent.yaml")
	runWorkflowSourceGit(t, repo, "commit", "-m", "split project definition")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "WORKFLOW.md"), []byte("working tree\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(working tree) error = %v", err)
	}

	workflow, err := LoadWorkflow(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	if strings.TrimSpace(workflow.Prompt) != "from split ref" {
		t.Fatalf("Prompt = %q, want split ref", workflow.Prompt)
	}
	if workflow.Config.Polling.IntervalMS != 90000 {
		t.Fatalf("Polling.IntervalMS = %d, want 90000", workflow.Config.Polling.IntervalMS)
	}
	if workflow.Definition.Layout != workflowconfig.ProjectDefinitionSplit {
		t.Fatalf("Layout = %q, want split", workflow.Definition.Layout)
	}
	if workflow.Definition.Revision == "" || workflow.Definition.Revision == workflow.SourceHash {
		t.Fatalf("Revision = %q, want git commit revision", workflow.Definition.Revision)
	}
}

func TestLoadWorkflowUsesAdmissionEffortGuidanceFromConfiguredGitRef(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	workflowPrompt := "## Admission Criteria\n\n- **Alignment** — ready.\n"
	config := `schema: 1
tracker:
  kind: memory
backlog_admission:
  enabled: true
  sources:
    states: [Backlog]
  target_state: Todo
  criteria_section: Admission Criteria
  require_effort: true
  effort_file: AGENTS.md
  effort_section: Issue Effort Selection
`
	agents := "## Issue Effort Selection\n\n- `medium` — small.\n- `high` — standard.\n"
	for fileName, content := range map[string]string{
		"WORKFLOW.md": workflowPrompt,
		"detent.yaml": config,
		"AGENTS.md":   agents,
	} {
		if err := os.WriteFile(filepath.Join(repo, fileName), []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", fileName, err)
		}
	}
	runWorkflowSourceGit(t, repo, "add", "WORKFLOW.md", "detent.yaml", "AGENTS.md")
	runWorkflowSourceGit(t, repo, "commit", "-m", "configure admission effort source")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "AGENTS.md"), []byte("working tree guidance\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(AGENTS.md) error = %v", err)
	}

	workflow, err := LoadWorkflow(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	rubric, err := workflowconfig.ResolveWorkflowAdmissionEffortRubric(workflow)
	if err != nil {
		t.Fatalf("ResolveWorkflowAdmissionEffortRubric() error = %v", err)
	}
	if len(rubric.Efforts) != 2 || rubric.Efforts[0] != "medium" || strings.Contains(workflow.AgentsPrompt, "working tree guidance") {
		t.Fatalf("rubric = %#v agents prompt = %q, want configured-ref AGENTS.md", rubric, workflow.AgentsPrompt)
	}
}

func TestLoadWorkflowUsesLocalOverlayWithConfiguredGitRef(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	workflowPath := filepath.Join(repo, "WORKFLOW.md")
	writeWorkflowSourceFile(t, workflowPath, "from ref")
	commitWorkflowSourceRepo(t, repo, "initial workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "WORKFLOW.local.md"), []byte("---\npolling:\n  interval_ms: 90000\n---\nlocal direction\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	workflow, err := LoadWorkflow(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	if workflow.Config.Polling.IntervalMS != 90000 {
		t.Fatalf("Polling.IntervalMS = %d, want 90000", workflow.Config.Polling.IntervalMS)
	}
	if got := workflow.Prompt; !strings.Contains(got, "from ref") || !strings.Contains(got, "local direction") {
		t.Fatalf("Prompt = %q, want shared and local direction", got)
	}
	if workflow.Overlay.Path != filepath.Join(repo, "WORKFLOW.local.md") {
		t.Fatalf("Overlay.Path = %q, want working-tree overlay", workflow.Overlay.Path)
	}
}

func TestLoadWorkflowUsesAbsolutePathUnderWorkdirWithConfiguredGitRef(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	workflowPath := filepath.Join(repo, "WORKFLOW.md")
	writeWorkflowSourceFile(t, workflowPath, "from ref")
	commitWorkflowSourceRepo(t, repo, "initial workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
	writeWorkflowSourceFile(t, workflowPath, "working tree")

	workflow, err := LoadWorkflow(globalconfig.Project{
		Workflow:    workflowPath,
		WorkflowRef: "origin/main",
		Workdir:     repo,
	})
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}

	if got := strings.TrimSpace(workflow.Prompt); got != "from ref" {
		t.Fatalf("Prompt = %q, want from ref", got)
	}
}

func TestLoadWorkflowRejectsRefPathOutsideWorkdir(t *testing.T) {
	t.Parallel()

	_, err := LoadWorkflow(globalconfig.Project{
		Workflow:    filepath.Join(t.TempDir(), "WORKFLOW.md"),
		WorkflowRef: "origin/main",
		Workdir:     t.TempDir(),
	})
	if !errors.Is(err, errUnsafeWorkflowPath) {
		t.Fatalf("LoadWorkflow() error = %v, want %v", err, errUnsafeWorkflowPath)
	}
}

func TestGitRefWorkflowWatcherReloadsWhenRefAdvances(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	writeWorkflowSourceFile(t, filepath.Join(repo, "WORKFLOW.md"), "first")
	commitWorkflowSourceRepo(t, repo, "first workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")

	watcher, err := newGitRefWorkflowWatcher(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	}, 10*time.Millisecond, slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)))
	if err != nil {
		t.Fatalf("newGitRefWorkflowWatcher() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan configwatcher.Update, 1)

	lastRevision, lastErr := watcher.seed(ctx, updates)
	if lastErr != "" {
		t.Fatalf("seed() error = %s", lastErr)
	}
	if lastRevision == "" {
		t.Fatal("seed() revision = empty")
	}
	assertNoWorkflowSourceUpdate(t, updates)

	writeWorkflowSourceFile(t, filepath.Join(repo, "WORKFLOW.md"), "second")
	commitWorkflowSourceRepo(t, repo, "second workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")

	lastRevision, lastErr = watcher.reload(ctx, updates, lastRevision, lastErr)
	if lastErr != "" {
		t.Fatalf("reload() error = %s", lastErr)
	}
	if lastRevision == "" {
		t.Fatal("reload() revision = empty")
	}
	update := readWorkflowSourceUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("workflow update error = %v", update.Err)
	}
	if got := strings.TrimSpace(update.Workflow.Prompt); got != "second" {
		t.Fatalf("Prompt = %q, want second", got)
	}
}

func TestWorkflowWatchersUseReloadedHostBackends(t *testing.T) {
	for _, ref := range []string{"", "origin/main"} {
		t.Run("ref="+ref, func(t *testing.T) {
			repo := initWorkflowSourceRepo(t)
			path := filepath.Join(repo, "WORKFLOW.md")
			writeWorkflowSourceFile(t, path, "initial")
			commitWorkflowSourceRepo(t, repo, "initial")
			updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")
			project := &Project{cfg: globalconfig.Project{Workflow: path, WorkflowRef: ref, Workdir: repo}}
			factory := resolveWorkflowWatcherFactory(Dependencies{}, project.Config(), "", slog.New(slog.DiscardHandler), project.Config)
			watcher, err := factory(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			updates, err := watcher.Watch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			project.mu.Lock()
			project.cfg.GlobalAgents = workflowconfig.Agents{Backends: []workflowconfig.AgentBackend{{ID: "new-backend", Kind: workflowconfig.AgentBackendCodex, Command: "codex"}}}
			project.mu.Unlock()
			if ref != "" {
				path = workflowconfig.LocalWorkflowPath(path)
			}
			raw := "---\ntracker:\n  kind: memory\nagents:\n  routes:\n    - name: new-default\n      backend: new-backend\n      default: true\n---\nupdated\n"
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			update := readWorkflowSourceUpdate(t, updates)
			if update.Err != nil {
				t.Fatal(update.Err)
			}
			found := false
			for _, backend := range update.Workflow.Config.AgentBackendConfigs() {
				if backend.ID == "new-backend" {
					found = true
				}
			}
			if !found {
				t.Fatal("watcher used obsolete host backends")
			}
		})
	}
}

func TestGitRefWorkflowWatcherReloadsLocalOverlayLifecycle(t *testing.T) {
	t.Parallel()

	repo := initWorkflowSourceRepo(t)
	writeWorkflowSourceFile(t, filepath.Join(repo, "WORKFLOW.md"), "shared")
	commitWorkflowSourceRepo(t, repo, "initial workflow")
	updateWorkflowSourceRef(t, repo, "origin/main", "HEAD")

	watcher, err := newGitRefWorkflowWatcher(globalconfig.Project{
		Workflow:    "WORKFLOW.md",
		WorkflowRef: "origin/main",
		Workdir:     repo,
	}, time.Hour, slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)))
	if err != nil {
		t.Fatalf("newGitRefWorkflowWatcher() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	updates, err := watcher.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	localPath := filepath.Join(repo, "WORKFLOW.local.md")
	writeWorkflowSourceFile(t, localPath, "local")
	created := readWorkflowSourceUpdate(t, updates)
	if created.Err != nil {
		t.Fatalf("create update error = %v", created.Err)
	}
	if !strings.Contains(created.Workflow.Prompt, "shared") || !strings.Contains(created.Workflow.Prompt, "local") {
		t.Fatalf("create Prompt = %q, want shared and local", created.Workflow.Prompt)
	}

	removeDeadline := time.NewTimer(5 * time.Second)
	defer removeDeadline.Stop()
	removeRetry := time.NewTicker(10 * time.Millisecond)
	defer removeRetry.Stop()
	for {
		err := os.Remove(localPath)
		if err == nil {
			break
		}
		const windowsSharingViolation syscall.Errno = 32
		if runtime.GOOS != "windows" || !errors.Is(err, windowsSharingViolation) {
			t.Fatalf("Remove() error = %v", err)
		}
		select {
		case <-removeDeadline.C:
			t.Fatalf("Remove() still failing after retry deadline: %v", err)
		case <-removeRetry.C:
		}
	}
	deleted := readWorkflowSourceUpdate(t, updates)
	if deleted.Err != nil {
		t.Fatalf("delete update error = %v", deleted.Err)
	}
	if got := strings.TrimSpace(deleted.Workflow.Prompt); got != "shared" {
		t.Fatalf("delete Prompt = %q, want shared", got)
	}
	if deleted.Workflow.Overlay.Path != "" {
		t.Fatalf("delete Overlay = %#v, want inactive", deleted.Workflow.Overlay)
	}
}

func assertNoWorkflowSourceUpdate(t *testing.T, updates <-chan configwatcher.Update) {
	t.Helper()

	select {
	case update := <-updates:
		t.Fatalf("unexpected workflow update: %#v", update)
	default:
	}
}

func readWorkflowSourceUpdate(t *testing.T, updates <-chan configwatcher.Update) configwatcher.Update {
	t.Helper()

	select {
	case update := <-updates:
		return update
	case <-time.After(30 * time.Second):
		t.Fatal("deadlocked waiting for workflow update")
		return configwatcher.Update{}
	}
}

func initWorkflowSourceRepo(t *testing.T) string {
	t.Helper()

	root := t.TempDir()
	runWorkflowSourceCommand(t, "", "git", "init", root)
	runWorkflowSourceGit(t, root, "config", "user.email", "detent@example.com")
	runWorkflowSourceGit(t, root, "config", "user.name", "Detent Test")
	return root
}

func writeWorkflowSourceFile(t *testing.T, path string, prompt string) {
	t.Helper()

	content := "---\ntracker:\n  kind: memory\n---\n" + prompt + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func commitWorkflowSourceRepo(t *testing.T, repo string, message string) {
	t.Helper()

	runWorkflowSourceGit(t, repo, "add", "WORKFLOW.md")
	runWorkflowSourceGit(t, repo, "commit", "-m", message)
}

func updateWorkflowSourceRef(t *testing.T, repo string, ref string, value string) {
	t.Helper()

	runWorkflowSourceGit(t, repo, "update-ref", "refs/remotes/"+ref, value)
}

func runWorkflowSourceGit(t *testing.T, repo string, args ...string) string {
	t.Helper()

	return runWorkflowSourceCommand(t, repo, "git", args...)
}

func runWorkflowSourceCommand(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s error = %v\n%s", name, strings.Join(args, " "), err, output)
	}
	return string(output)
}

func installWorkflowGitHelper(t *testing.T) string {
	t.Helper()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable() error = %v", err)
	}
	binDir := t.TempDir()
	name := "git"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	destination := filepath.Join(binDir, name)
	if runtime.GOOS != "windows" {
		if err := os.Link(executable, destination); err == nil {
			return binDir
		}
	}

	source, err := os.Open(executable)
	if err != nil {
		t.Fatalf("Open(%s) error = %v", executable, err)
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatalf("OpenFile(%s) error = %v", destination, err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		t.Fatalf("copy helper executable: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("close helper executable: %v", err)
	}
	return binDir
}

func runWorkflowGitParentHelper() int {
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^$")
	cmd.Env = replaceWorkflowGitHelperMode(os.Environ(), "descendant")
	readiness, err := cmd.StdoutPipe()
	if err != nil {
		return 2
	}
	cmd.Stderr = os.Stderr
	procgroup.Configure(context.Background(), cmd)
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	ready, err := bufio.NewReader(readiness).ReadString('\n')
	if err != nil || strings.TrimSpace(ready) != "ready" {
		_ = procgroup.TerminateTree(cmd, procgroup.GroupID(cmd))
		return 4
	}
	fmt.Fprintln(os.Stdout, "workflow git stdout")
	fmt.Fprintln(os.Stderr, "workflow git stderr")
	exitCode, err := strconv.Atoi(os.Getenv(workflowGitHelperExitCodeEnv))
	if err != nil {
		return 5
	}
	return exitCode
}

func replaceWorkflowGitHelperMode(environment []string, mode string) []string {
	updated := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, workflowGitHelperModeEnv+"=") {
			updated = append(updated, entry)
		}
	}
	return append(updated, workflowGitHelperModeEnv+"="+mode)
}

func runWorkflowGitDescendantHelper() int {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	connection, err := dialer.DialContext(context.Background(), "tcp", os.Getenv(workflowGitHelperAddressEnv))
	if err != nil {
		return 6
	}
	defer connection.Close()
	if _, err := fmt.Fprintf(connection, "%d\n", os.Getpid()); err != nil {
		return 7
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		return 8
	}
	var release [1]byte
	if _, err := io.ReadFull(connection, release[:]); err != nil {
		return 9
	}
	if err := os.Stderr.Close(); err != nil {
		return 10
	}
	if _, err := fmt.Fprintln(connection, "done"); err != nil {
		return 11
	}
	return 0
}

func terminateWorkflowGitHelper(t *testing.T, pid int) {
	t.Helper()

	process, err := os.FindProcess(pid)
	if err != nil {
		t.Errorf("FindProcess(%d) error = %v", pid, err)
		return
	}
	if err := procgroup.TerminateTree(&exec.Cmd{Process: process}, pid); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("terminate helper process tree %d: %v", pid, err)
	}
}

func waitForWorkflowGitHelperExit(t *testing.T, pid int) {
	t.Helper()

	if runtime.GOOS != "windows" {
		return
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return
		}
		t.Fatalf("FindProcess(%d) error = %v", pid, err)
	}
	waited := make(chan error, 1)
	go func() {
		_, err := process.Wait()
		waited <- err
	}()
	select {
	case err := <-waited:
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Fatalf("wait for helper process %d: %v", pid, err)
		}
	case <-time.After(5 * time.Second):
		terminateWorkflowGitHelper(t, pid)
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("helper process %d did not stop after termination", pid)
		}
		t.Fatalf("helper process %d did not exit after release", pid)
	}
}
