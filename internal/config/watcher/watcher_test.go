package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
)

func TestWatchDebouncesWorkflowWrites(t *testing.T) {
	t.Parallel()

	runtime := newControlledFileRuntime()
	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "initial")

	w, err := New(path, withFileOptions(runtime.option()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeWorkflow(t, path, 61000, "first")
	runtime.sendEvent(t, path)
	runtime.waitForReset(t)
	writeWorkflow(t, path, 62000, "second")
	runtime.sendEvent(t, path)
	runtime.waitForReset(t)
	runtime.fireTimer(t)

	update := receiveUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Workflow.Config.Polling.IntervalMS != 62000 {
		t.Fatalf("Polling.IntervalMS = %d, want 62000", update.Workflow.Config.Polling.IntervalMS)
	}
	if update.Workflow.Prompt != "second\n" {
		t.Fatalf("Prompt = %q, want second", update.Workflow.Prompt)
	}

	assertNoUpdate(t, updates)
}

func TestWatchSuppressesDuplicateWorkflowUpdates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "initial")

	w, err := New(path, WithDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeWorkflow(t, path, 61000, "second")
	update := receiveUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Workflow.Prompt != "second\n" {
		t.Fatalf("Prompt = %q, want second", update.Workflow.Prompt)
	}

	writeWorkflow(t, path, 61000, "second")
	select {
	case extra := <-updates:
		t.Fatalf("duplicate update = %#v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchHandlesAtomicSaveRename(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "initial")

	w, err := New(path, WithDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	tmp := filepath.Join(dir, ".WORKFLOW.md.tmp")
	writeWorkflow(t, tmp, 63000, "renamed")
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}

	update := receiveUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Workflow.Config.Polling.IntervalMS != 63000 {
		t.Fatalf("Polling.IntervalMS = %d, want 63000", update.Workflow.Config.Polling.IntervalMS)
	}
	if update.Workflow.Prompt != "renamed\n" {
		t.Fatalf("Prompt = %q, want renamed", update.Workflow.Prompt)
	}
}

func TestWatchRetriesTransientReloadErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "initial")

	var attempts atomic.Int32
	w, err := New(path,
		WithDebounce(10*time.Millisecond),
		WithLoader(func(path string) (workflowconfig.Workflow, error) {
			if attempts.Add(1) == 1 {
				return workflowconfig.Workflow{}, errors.New("missing YAML frontmatter")
			}
			return workflowconfig.LoadWorkflow(path)
		}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeWorkflow(t, path, 62000, "second")

	update := receiveUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Workflow.Config.Polling.IntervalMS != 62000 {
		t.Fatalf("Polling.IntervalMS = %d, want 62000", update.Workflow.Config.Polling.IntervalMS)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("loader attempts = %d, want at least 2", got)
	}
}

func TestWatchReportsInvalidReload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "initial")

	w, err := New(path, WithDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	if err := os.WriteFile(path, []byte("---\ntracker: [\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	update := receiveUpdate(t, updates)
	if update.Err == nil {
		t.Fatal("update error = nil, want parse error")
	}
}

func TestWatchReloadsLocalWorkflowOverlayLifecycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	localPath := workflowconfig.LocalWorkflowPath(path)
	writeWorkflow(t, path, 60000, "shared")

	w, err := New(path, WithDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeWorkflow(t, localPath, 61000, "local first")
	created := receiveUpdate(t, updates)
	if created.Err != nil {
		t.Fatalf("create update error = %v", created.Err)
	}
	if created.Workflow.Config.Polling.IntervalMS != 61000 || !strings.Contains(created.Workflow.Prompt, "local first") {
		t.Fatalf("create workflow = %#v, want local overlay", created.Workflow)
	}

	writeWorkflow(t, localPath, 62000, "local second")
	edited := receiveUpdate(t, updates)
	if edited.Err != nil {
		t.Fatalf("edit update error = %v", edited.Err)
	}
	if edited.Workflow.Config.Polling.IntervalMS != 62000 || !strings.Contains(edited.Workflow.Prompt, "local second") {
		t.Fatalf("edit workflow = %#v, want updated local overlay", edited.Workflow)
	}

	if err := os.Remove(localPath); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	deleted := receiveUpdate(t, updates)
	if deleted.Err != nil {
		t.Fatalf("delete update error = %v", deleted.Err)
	}
	if deleted.Workflow.Config.Polling.IntervalMS != 60000 || deleted.Workflow.Prompt != "shared\n" {
		t.Fatalf("delete workflow = %#v, want shared workflow", deleted.Workflow)
	}
	if deleted.Workflow.Overlay.Path != "" {
		t.Fatalf("delete Overlay = %#v, want inactive", deleted.Workflow.Overlay)
	}
}

func TestWatchReconcilesOverlayDeletionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		deniedFor time.Duration
		wantErr   bool
	}{
		{name: "immediately absent"},
		{name: "settles between last retry and expiry", deniedFor: 11 * time.Millisecond},
		{name: "settles at expiry", deniedFor: 12 * time.Millisecond},
		{name: "persistent permission error", deniedFor: time.Hour, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				runtime := newControlledFileRuntime()
				path := filepath.Join(t.TempDir(), "WORKFLOW.md")
				localPath := workflowconfig.LocalWorkflowPath(path)
				writeWorkflow(t, path, 60000, "shared")
				var deniedUntil time.Time
				var deniedReads int
				w, err := New(path, WithDebounce(12*time.Millisecond),
					withFileOptions(runtime.option()),
					WithLoader(func(path string) (workflowconfig.Workflow, error) {
						if time.Now().Before(deniedUntil) {
							deniedReads++
							return workflowconfig.Workflow{}, fmt.Errorf("read local workflow overlay: %w", &os.PathError{
								Op: "open", Path: localPath, Err: os.ErrPermission,
							})
						}
						return workflowconfig.LoadWorkflow(path)
					}),
				)
				if err != nil {
					t.Fatalf("New() error = %v", err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				updates, err := w.Watch(ctx)
				if err != nil {
					t.Fatalf("Watch() error = %v", err)
				}
				steps := []struct {
					name     string
					interval int
					prompt   string
					remove   bool
				}{
					{name: "create", interval: 61000, prompt: "local first"},
					{name: "edit", interval: 62000, prompt: "local second"},
					{name: "delete", interval: 60000, prompt: "shared", remove: true},
					{name: "recreate", interval: 63000, prompt: "local third"},
				}
				for _, step := range steps {
					synctest.Wait()
					deniedUntil = time.Time{}
					if step.remove {
						if err := os.Remove(localPath); err != nil {
							t.Fatalf("Remove() error = %v", err)
						}
						deniedUntil = time.Now().Add(test.deniedFor)
					} else {
						writeWorkflow(t, localPath, step.interval, step.prompt)
					}
					runtime.sendEvent(t, localPath)
					runtime.waitForReset(t)
					runtime.fireTimer(t)
					update := receiveUpdate(t, updates)
					if step.remove && test.wantErr {
						if !errors.Is(update.Err, os.ErrPermission) {
							t.Fatalf("%s error = %v, want permission error", step.name, update.Err)
						}
					} else {
						if update.Err != nil {
							t.Fatalf("%s error = %v", step.name, update.Err)
						}
						if update.Workflow.Config.Polling.IntervalMS != step.interval || !strings.Contains(update.Workflow.Prompt, step.prompt+"\n") {
							t.Fatalf("%s workflow = %#v, want interval %d and prompt %q", step.name, update.Workflow, step.interval, step.prompt)
						}
						if step.remove && (update.Workflow.Overlay.Path != "" || update.Workflow.Prompt != "shared\n") {
							t.Fatalf("deleted workflow = %#v, want shared workflow with inactive overlay", update.Workflow)
						}
						if !step.remove && update.Workflow.Overlay.Path == "" {
							t.Fatalf("%s overlay is inactive", step.name)
						}
					}
					synctest.Wait()
					select {
					case extra := <-updates:
						t.Fatalf("%s extra update = %#v", step.name, extra)
					default:
					}
				}
				if test.deniedFor > 0 && deniedReads == 0 {
					t.Fatal("deletion did not exercise an access-denied read")
				}
				cancel()
				synctest.Wait()
				if _, ok := <-updates; ok {
					t.Fatal("updates remained open after cancellation")
				}
				runtime.waitForClosed(t)
				runtime.poll.waitForStopped(t)
			})
		})
	}
}

func TestFileWatcherLoadCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cancelAt  time.Duration
		wantReads int
	}{
		{name: "before initial read", wantReads: 0},
		{name: "during retry wait", cancelAt: 2 * time.Millisecond, wantReads: 1},
		{name: "before final reconciliation", cancelAt: 11 * time.Millisecond, wantReads: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if test.cancelAt == 0 {
					cancel()
				} else {
					timer := time.AfterFunc(test.cancelAt, cancel)
					defer timer.Stop()
				}
				reads := 0
				w, err := NewFile("WORKFLOW.md", func(string) (string, error) {
					reads++
					return "", os.ErrPermission
				}, WithFileDebounce(12*time.Millisecond))
				if err != nil {
					t.Fatalf("NewFile() error = %v", err)
				}
				if _, err := w.load(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("load() error = %v, want cancellation", err)
				}
				if reads != test.wantReads {
					t.Fatalf("loader reads = %d, want %d", reads, test.wantReads)
				}
			})
		})
	}
}

func TestWatchSurvivesSharedWorkflowDeletionAndCreation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "WORKFLOW.md")
	writeWorkflow(t, path, 60000, "first")

	w, err := New(path, WithDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	deleted := receiveUpdate(t, updates)
	if deleted.Err == nil {
		t.Fatal("delete update error = nil, want missing shared workflow error")
	}

	writeWorkflow(t, path, 63000, "recreated")
	recreated := receiveUpdate(t, updates)
	if recreated.Err != nil {
		t.Fatalf("recreate update error = %v", recreated.Err)
	}
	if recreated.Workflow.Config.Polling.IntervalMS != 63000 || recreated.Workflow.Prompt != "recreated\n" {
		t.Fatalf("recreated workflow = %#v, want recreated shared workflow", recreated.Workflow)
	}
}

func TestFileWatcherDebouncesGlobalConfigWrites(t *testing.T) {
	t.Parallel()

	runtime := newControlledFileRuntime()
	dir := t.TempDir()
	path := filepath.Join(dir, "global.yaml")
	writeGlobalConfig(t, path, 2)

	w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
		return globalconfig.Read(path)
	}, runtime.option())
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeGlobalConfig(t, path, 3)
	runtime.sendEvent(t, path)
	runtime.waitForReset(t)
	writeGlobalConfig(t, path, 4)
	runtime.sendEvent(t, path)
	runtime.waitForReset(t)
	runtime.fireTimer(t)

	update := receiveFileUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Value.Global.MaxConcurrentAgents != 4 {
		t.Fatalf("MaxConcurrentAgents = %d, want 4", update.Value.Global.MaxConcurrentAgents)
	}
	if update.Value.Path != path {
		t.Fatalf("Path = %q, want %q", update.Value.Path, path)
	}

	assertNoFileUpdate(t, updates)
}

func TestFileWatcherWatchesSymlinkTargetWrites(t *testing.T) {
	t.Parallel()

	linkDir := t.TempDir()
	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "global.yaml")
	linkPath := filepath.Join(linkDir, "global.yaml")
	writeGlobalConfig(t, targetPath, 2)
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	w, err := NewFile(linkPath, func(path string) (globalconfig.Config, error) {
		return globalconfig.Read(path)
	}, WithFileDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	tmpPath := filepath.Join(targetDir, ".global.yaml.tmp")
	writeGlobalConfig(t, tmpPath, 5)
	if err := os.Rename(tmpPath, targetPath); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}

	update := receiveFileUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Path != linkPath {
		t.Fatalf("Path = %q, want symlink path %q", update.Path, linkPath)
	}
	if update.Value.Path != linkPath {
		t.Fatalf("Value.Path = %q, want symlink path %q", update.Value.Path, linkPath)
	}
	if update.Value.Global.MaxConcurrentAgents != 5 {
		t.Fatalf("MaxConcurrentAgents = %d, want 5", update.Value.Global.MaxConcurrentAgents)
	}
}

func TestFileWatcherWatchesSymlinkTargetTouches(t *testing.T) {
	t.Parallel()

	linkDir := t.TempDir()
	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "global.yaml")
	linkPath := filepath.Join(linkDir, "global.yaml")
	writeGlobalConfig(t, targetPath, 2)
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	w, err := NewFile(linkPath, func(path string) (globalconfig.Config, error) {
		return globalconfig.Read(path)
	}, WithFileDebounce(10*time.Millisecond))
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	touchedAt := time.Now().Add(time.Minute)
	if err := os.Chtimes(targetPath, touchedAt, touchedAt); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	update := receiveFileUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Path != linkPath {
		t.Fatalf("Path = %q, want symlink path %q", update.Path, linkPath)
	}
	if update.Value.Global.MaxConcurrentAgents != 2 {
		t.Fatalf("MaxConcurrentAgents = %d, want 2", update.Value.Global.MaxConcurrentAgents)
	}
}

func TestFileWatcherRefreshesRetargetedSymlinkTarget(t *testing.T) {
	t.Parallel()

	runtime := newControlledFileRuntime()
	linkDir := t.TempDir()
	firstDir := t.TempDir()
	nextDir := t.TempDir()
	firstPath := filepath.Join(firstDir, "global.yaml")
	nextPath := filepath.Join(nextDir, "global.yaml")
	linkPath := filepath.Join(linkDir, "global.yaml")
	writeGlobalConfig(t, firstPath, 2)
	writeGlobalConfig(t, nextPath, 3)
	if err := os.Symlink(firstPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	w, err := NewFile(linkPath, func(path string) (globalconfig.Config, error) {
		return globalconfig.Read(path)
	}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	if err := os.Remove(linkPath); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if err := os.Symlink(nextPath, linkPath); err != nil {
		t.Fatalf("Symlink() retarget error = %v", err)
	}
	runtime.fireTicker(t, filePollTicker)
	runtime.waitForReset(t)
	runtime.fireTimer(t)

	update := receiveFileUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("retarget update error = %v", update.Err)
	}
	if update.Value.Global.MaxConcurrentAgents != 3 {
		t.Fatalf("MaxConcurrentAgents = %d, want 3", update.Value.Global.MaxConcurrentAgents)
	}
	resolvedNextPath, err := filepath.EvalSymlinks(nextPath)
	if err != nil {
		t.Fatalf("EvalSymlinks() error = %v", err)
	}
	runtime.waitForAdd(t, filepath.Dir(resolvedNextPath))
}

func TestFileWatcherReconcilesMissedEvents(t *testing.T) {
	t.Parallel()

	type step struct {
		mutate  func(*testing.T)
		ticker  fileTickerKind
		want    int
		wantErr bool
	}
	tests := []struct {
		name    string
		prepare func(*testing.T, *controlledFileRuntime) (string, []step)
	}{
		{
			name: "atomic replacement",
			prepare: func(t *testing.T, _ *controlledFileRuntime) (string, []step) {
				dir := t.TempDir()
				path := filepath.Join(dir, "global.yaml")
				writeGlobalConfig(t, path, 2)
				return path, []step{{
					mutate: func(t *testing.T) {
						tmpPath := filepath.Join(dir, ".global.yaml.tmp")
						writeGlobalConfig(t, tmpPath, 5)
						if err := os.Rename(tmpPath, path); err != nil {
							t.Fatalf("Rename() error = %v", err)
						}
					},
					ticker: filePollTicker,
					want:   5,
				}}
			},
		},
		{
			name: "absent parent creation",
			prepare: func(t *testing.T, runtime *controlledFileRuntime) (string, []step) {
				path := filepath.Join(t.TempDir(), "missing", "global.yaml")
				runtime.add = func(dir string) error {
					_, err := os.Stat(dir)
					return err
				}
				return path, []step{{
					mutate: func(t *testing.T) {
						if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
							t.Fatalf("MkdirAll() error = %v", err)
						}
						writeGlobalConfig(t, path, 4)
					},
					ticker: fileRetryTicker,
					want:   4,
				}}
			},
		},
		{
			name: "symlink target replacement",
			prepare: func(t *testing.T, _ *controlledFileRuntime) (string, []step) {
				linkDir := t.TempDir()
				targetDir := t.TempDir()
				targetPath := filepath.Join(targetDir, "global.yaml")
				linkPath := filepath.Join(linkDir, "global.yaml")
				writeGlobalConfig(t, targetPath, 2)
				if err := os.Symlink(targetPath, linkPath); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
				return linkPath, []step{{
					mutate: func(t *testing.T) {
						tmpPath := filepath.Join(targetDir, ".global.yaml.tmp")
						writeGlobalConfig(t, tmpPath, 6)
						if err := os.Rename(tmpPath, targetPath); err != nil {
							t.Fatalf("Rename() error = %v", err)
						}
					},
					ticker: filePollTicker,
					want:   6,
				}}
			},
		},
		{
			name: "delete and recreate",
			prepare: func(t *testing.T, _ *controlledFileRuntime) (string, []step) {
				dir := t.TempDir()
				path := filepath.Join(dir, "global.yaml")
				writeGlobalConfig(t, path, 2)
				return path, []step{
					{
						mutate: func(t *testing.T) {
							if err := os.Remove(path); err != nil {
								t.Fatalf("Remove() error = %v", err)
							}
						},
						ticker:  filePollTicker,
						wantErr: true,
					},
					{
						mutate: func(t *testing.T) {
							writeGlobalConfig(t, path, 7)
						},
						ticker: filePollTicker,
						want:   7,
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime := newControlledFileRuntime()
			path, steps := test.prepare(t, runtime)
			w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
				return globalconfig.Read(path)
			}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
			if err != nil {
				t.Fatalf("NewFile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			updates, err := w.Watch(ctx)
			if err != nil {
				t.Fatalf("Watch() error = %v", err)
			}

			for _, step := range steps {
				step.mutate(t)
				runtime.fireTicker(t, step.ticker)
				runtime.waitForReset(t)
				runtime.fireTimer(t)
				update := receiveFileUpdate(t, updates)
				if step.wantErr {
					if update.Err == nil {
						t.Fatal("update error = nil, want file error")
					}
					continue
				}
				if update.Err != nil {
					t.Fatalf("update error = %v", update.Err)
				}
				if update.Value.Global.MaxConcurrentAgents != step.want {
					t.Fatalf("MaxConcurrentAgents = %d, want %d", update.Value.Global.MaxConcurrentAgents, step.want)
				}
			}
		})
	}
}

func TestFileWatcherObservesContentChangesWithUnchangedMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replace bool
		poll    bool
	}{
		{name: "write event"},
		{name: "write poll", poll: true},
		{name: "replacement event", replace: true},
		{name: "replacement poll", replace: true, poll: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime := newControlledFileRuntime()
			path := filepath.Join(t.TempDir(), "global.yaml")
			writeGlobalConfig(t, path, 2)
			modifiedAt := time.Unix(1700000000, 0)
			if err := os.Chtimes(path, modifiedAt, modifiedAt); err != nil {
				t.Fatalf("Chtimes() error = %v", err)
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat() error = %v", err)
			}
			w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
				return globalconfig.Read(path)
			}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
			if err != nil {
				t.Fatalf("NewFile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			updates, err := w.Watch(ctx)
			if err != nil {
				t.Fatalf("Watch() error = %v", err)
			}

			writePath := path
			if test.replace {
				writePath += ".tmp"
			}
			writeGlobalConfig(t, writePath, 5)
			if err := os.Chtimes(writePath, modifiedAt, modifiedAt); err != nil {
				t.Fatalf("Chtimes() error = %v", err)
			}
			if test.replace {
				if err := os.Rename(writePath, path); err != nil {
					t.Fatalf("Rename() error = %v", err)
				}
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat() error = %v", err)
			}
			if before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
				t.Fatal("rewrite changed metadata")
			}
			if test.poll {
				runtime.fireTicker(t, filePollTicker)
			} else {
				runtime.sendEvent(t, path)
			}
			runtime.waitForReset(t)
			runtime.fireTimer(t)
			update := receiveFileUpdate(t, updates)
			if update.Err != nil {
				t.Fatalf("update error = %v", update.Err)
			}
			if update.Value.Global.MaxConcurrentAgents != 5 {
				t.Fatalf("MaxConcurrentAgents = %d, want 5", update.Value.Global.MaxConcurrentAgents)
			}

			runtime.sendEvent(t, path)
			runtime.sendEvent(t, path+".unwatched")
			select {
			case <-runtime.timer.resets:
				t.Fatal("duplicate observation reset debounce")
			default:
			}
			cancel()
			waitForFileUpdatesClosed(t, updates)
		})
	}
}

func TestFileWatcherDeduplicatesEventAndPollObservations(t *testing.T) {
	t.Parallel()

	runtime := newControlledFileRuntime()
	dir := t.TempDir()
	path := filepath.Join(dir, "global.yaml")
	writeGlobalConfig(t, path, 2)
	w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
		return globalconfig.Read(path)
	}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	updates, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	writeGlobalConfig(t, path, 8)
	runtime.fireTicker(t, filePollTicker)
	runtime.waitForReset(t)
	runtime.sendEvent(t, path)
	runtime.fireTimer(t)
	update := receiveFileUpdate(t, updates)
	if update.Err != nil {
		t.Fatalf("update error = %v", update.Err)
	}
	if update.Value.Global.MaxConcurrentAgents != 8 {
		t.Fatalf("MaxConcurrentAgents = %d, want 8", update.Value.Global.MaxConcurrentAgents)
	}
	assertNoFileUpdate(t, updates)
}

func TestFileWatcherCompletesInitialAttachmentBeforeReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		addErr  error
		wantErr bool
	}{
		{name: "attached"},
		{name: "missing directory remains pending", addErr: os.ErrNotExist},
		{name: "other attachment failure", addErr: errors.New("permission denied"), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime := newControlledFileRuntime()
			var addCalled atomic.Bool
			runtime.add = func(string) error {
				addCalled.Store(true)
				return test.addErr
			}
			path := filepath.Join(t.TempDir(), "global.yaml")
			writeGlobalConfig(t, path, 2)
			w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
				return globalconfig.Read(path)
			}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
			if err != nil {
				t.Fatalf("NewFile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			updates, err := w.Watch(ctx)
			if !addCalled.Load() {
				t.Fatal("Watch() returned before initial attachment attempt")
			}
			if test.wantErr {
				cancel()
				if err == nil {
					t.Fatal("Watch() error = nil, want attachment error")
				}
				runtime.waitForClosed(t)
				return
			}
			if err != nil {
				cancel()
				t.Fatalf("Watch() error = %v", err)
			}
			cancel()
			waitForFileUpdatesClosed(t, updates)
		})
	}
}

func TestFileWatcherCancellationStopsRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		missingParent bool
	}{
		{name: "attached"},
		{name: "pending attachment", missingParent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime := newControlledFileRuntime()
			path := filepath.Join(t.TempDir(), "global.yaml")
			if test.missingParent {
				path = filepath.Join(t.TempDir(), "missing", "global.yaml")
				runtime.add = func(dir string) error {
					_, err := os.Stat(dir)
					return err
				}
			} else {
				writeGlobalConfig(t, path, 2)
			}
			w, err := NewFile(path, func(path string) (globalconfig.Config, error) {
				return globalconfig.Read(path)
			}, runtime.option(), withFileIntervals(time.Hour, time.Hour))
			if err != nil {
				t.Fatalf("NewFile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			updates, err := w.Watch(ctx)
			if err != nil {
				t.Fatalf("Watch() error = %v", err)
			}
			cancel()
			waitForFileUpdatesClosed(t, updates)
			runtime.waitForClosed(t)
			runtime.poll.waitForStopped(t)
			if test.missingParent {
				runtime.retry.waitForStopped(t)
			}
			if runtime.timer.stops.Load() < 2 {
				t.Fatalf("debounce timer stops = %d, want at least 2", runtime.timer.stops.Load())
			}
		})
	}
}

func receiveUpdate(t *testing.T, updates <-chan Update) Update {
	t.Helper()

	select {
	case update, ok := <-updates:
		if !ok {
			t.Fatal("updates channel closed")
		}
		return update
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watcher update")
	}

	return Update{}
}

func receiveFileUpdate[T any](t *testing.T, updates <-chan FileUpdate[T]) FileUpdate[T] {
	t.Helper()

	select {
	case update, ok := <-updates:
		if !ok {
			t.Fatal("updates channel closed")
		}
		return update
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watcher update")
	}

	return FileUpdate[T]{}
}

func assertNoUpdate(t *testing.T, updates <-chan Update) {
	t.Helper()

	select {
	case extra := <-updates:
		t.Fatalf("extra update after debounce = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func assertNoFileUpdate[T any](t *testing.T, updates <-chan FileUpdate[T]) {
	t.Helper()

	select {
	case extra := <-updates:
		t.Fatalf("extra update after debounce = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

type controlledFileRuntime struct {
	events chan fsnotify.Event
	errors chan error
	timer  *controlledFileTimer
	poll   *controlledFileTicker
	retry  *controlledFileTicker
	added  chan string
	closed chan struct{}
	add    func(string) error
	close  sync.Once
}

type controlledFileTimer struct {
	ticks  chan time.Time
	resets chan time.Duration
	stops  atomic.Int32
}

type controlledFileTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	stop    sync.Once
}

func newControlledFileRuntime() *controlledFileRuntime {
	return &controlledFileRuntime{
		events: make(chan fsnotify.Event, 2),
		errors: make(chan error),
		timer: &controlledFileTimer{
			ticks:  make(chan time.Time, 1),
			resets: make(chan time.Duration, 8),
		},
		poll:   newControlledFileTicker(),
		retry:  newControlledFileTicker(),
		added:  make(chan string, 16),
		closed: make(chan struct{}),
	}
}

func newControlledFileTicker() *controlledFileTicker {
	return &controlledFileTicker{
		ticks:   make(chan time.Time, 1),
		stopped: make(chan struct{}),
	}
}

func (r *controlledFileRuntime) option() FileOption {
	return withFileRuntime(
		func() (fileEventWatcher, error) { return r, nil },
		func(time.Duration) fileTimer { return r.timer },
		func(kind fileTickerKind, _ time.Duration) fileTicker {
			if kind == fileRetryTicker {
				return r.retry
			}
			return r.poll
		},
	)
}

func (r *controlledFileRuntime) Add(path string) error {
	r.added <- path
	if r.add != nil {
		return r.add(path)
	}
	return nil
}

func (r *controlledFileRuntime) Close() error {
	r.close.Do(func() {
		close(r.closed)
	})
	return nil
}

func (r *controlledFileRuntime) Events() <-chan fsnotify.Event {
	return r.events
}

func (r *controlledFileRuntime) Errors() <-chan error {
	return r.errors
}

func (r *controlledFileRuntime) sendEvent(t *testing.T, path string) {
	t.Helper()

	select {
	case r.events <- fsnotify.Event{Name: path, Op: fsnotify.Write}:
	case <-time.After(time.Second):
		t.Fatal("timed out sending file event")
	}
}

func (r *controlledFileRuntime) waitForReset(t *testing.T) {
	t.Helper()

	select {
	case <-r.timer.resets:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for debounce reset")
	}
}

func (r *controlledFileRuntime) fireTimer(t *testing.T) {
	t.Helper()

	select {
	case r.timer.ticks <- time.Now():
	case <-time.After(time.Second):
		t.Fatal("timed out firing debounce timer")
	}
}

func (r *controlledFileRuntime) fireTicker(t *testing.T, kind fileTickerKind) {
	t.Helper()

	ticker := r.poll
	if kind == fileRetryTicker {
		ticker = r.retry
	}
	select {
	case ticker.ticks <- time.Now():
	case <-time.After(time.Second):
		t.Fatal("timed out firing file ticker")
	}
}

func (r *controlledFileRuntime) waitForAdd(t *testing.T, want string) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		select {
		case got := <-r.added:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting to watch directory %q", want)
		}
	}
}

func (r *controlledFileRuntime) waitForClosed(t *testing.T) {
	t.Helper()

	select {
	case <-r.closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for file watcher close")
	}
}

func (t *controlledFileTimer) C() <-chan time.Time {
	return t.ticks
}

func (t *controlledFileTimer) Reset(duration time.Duration) bool {
	t.resets <- duration
	return true
}

func (t *controlledFileTimer) Stop() bool {
	t.stops.Add(1)
	return true
}

func (t *controlledFileTicker) C() <-chan time.Time {
	return t.ticks
}

func (t *controlledFileTicker) Stop() {
	t.stop.Do(func() {
		close(t.stopped)
	})
}

func (t *controlledFileTicker) waitForStopped(testingT *testing.T) {
	testingT.Helper()

	select {
	case <-t.stopped:
	case <-time.After(time.Second):
		testingT.Fatal("timed out waiting for file ticker stop")
	}
}

func waitForFileUpdatesClosed[T any](t *testing.T, updates <-chan FileUpdate[T]) {
	t.Helper()

	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("updates channel remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for updates channel close")
	}
}

func writeWorkflow(t *testing.T, path string, intervalMS int, prompt string) {
	t.Helper()

	raw := []byte(`---
tracker:
  kind: memory
polling:
  interval_ms: ` + strconv.Itoa(intervalMS) + `
---
` + prompt + `
`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func writeGlobalConfig(t *testing.T, path string, maxConcurrentAgents int) {
	t.Helper()

	raw := []byte(`apiVersion: detent/v1
kind: GlobalConfig
global:
  max_concurrent_agents: ` + strconv.Itoa(maxConcurrentAgents) + `
  scheduling: weighted
projects: []
`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}
