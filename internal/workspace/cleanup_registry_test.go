package workspace

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestLocalGitCleanupRemovesHookArtifacts(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	publishCleanupSource(t, source)
	root := filepath.Join(t.TempDir(), "workspaces")
	hookCommand := "mkdir -p node_modules/pkg uploads .local/state && touch node_modules/pkg/index.js uploads/generated.bin .local/state/cache"
	if runtime.GOOS == "windows" {
		hookCommand = "mkdir node_modules\\pkg uploads .local\\state && type nul > node_modules\\pkg\\index.js && type nul > uploads\\generated.bin && type nul > .local\\state\\cache"
	}
	backend, err := NewLocalGit(LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks:      Hooks{AfterCreate: hookCommand},
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	issue := Issue{ProjectID: "detent", ID: "2140", Identifier: "digitaldrywood/detent#2140"}
	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := ensureGitInfoExcludes(t.Context(), info.Path, []string{"node_modules/", "uploads/", ".local/"}); err != nil {
		t.Fatal(err)
	}

	artifacts := []struct {
		name string
		path string
	}{
		{name: "node modules", path: "node_modules/pkg/index.js"},
		{name: "generated upload", path: "uploads/generated.bin"},
		{name: "local state", path: ".local/state/cache"},
	}
	for _, artifact := range artifacts {
		t.Run(artifact.name, func(t *testing.T) {
			if _, err := os.Stat(filepath.Join(info.Path, filepath.FromSlash(artifact.path))); err != nil {
				t.Fatalf("hook artifact stat error = %v", err)
			}
		})
	}

	if _, err := backend.CleanupIssue(t.Context(), issue); err != nil {
		t.Fatalf("CleanupIssue() error = %v", err)
	}
	if _, err := os.Stat(info.Path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("workspace exists after cleanup, stat error = %v", err)
	}
}

func TestLocalGitCleanupRetriesAfterGitDeregistration(t *testing.T) {
	skipWindows(t)

	enclosing := initSourceRepo(t)
	source := filepath.Join(enclosing, "source")
	initSourceRepoAt(t, source)
	publishCleanupSource(t, source)
	root := filepath.Join(enclosing, "workspaces")
	backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	issue := Issue{ProjectID: "detent", ID: "2140", Identifier: "digitaldrywood/detent#2140"}
	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := ensureGitInfoExcludes(t.Context(), info.Path, []string{"node_modules/"}); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(info.Path, "node_modules", "locked")
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatalf("create locked artifact directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(locked, "generated.js"), []byte("generated"), 0o400); err != nil {
		t.Fatalf("write locked artifact: %v", err)
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatalf("lock artifact directory: %v", err)
	}
	t.Cleanup(func() { restoreWritableTree(t, info.Path) })

	removeCalls := 0
	backend.removeOwnedPath = func(root string, path string) error {
		removeCalls++
		if removeCalls == 1 {
			return fs.ErrPermission
		}
		return removeWorkspacePath(root, path)
	}
	_, err = backend.CleanupIssue(t.Context(), issue)
	if err == nil {
		t.Fatal("first CleanupIssue() error = nil, want final removal failure")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("first CleanupIssue() error = %v, want permission denial", err)
	}
	registered, err := backend.sourceWorktreeRegistered(t.Context(), info.Path)
	if err != nil {
		t.Fatalf("sourceWorktreeRegistered() error = %v", err)
	}
	if registered {
		t.Fatal("workspace remains registered after partial Git removal")
	}
	if _, err := os.Stat(info.Path); err != nil {
		t.Fatalf("residual workspace stat error = %v", err)
	}
	if recorded, err := backend.cleanupOwnershipRecorded(t.Context(), info.Path); err != nil || !recorded {
		t.Fatalf("cleanup ownership recorded = %t, error = %v", recorded, err)
	}

	if _, err := backend.CleanupIssue(t.Context(), issue); err != nil {
		t.Fatalf("second CleanupIssue() error = %v", err)
	}
	if _, err := os.Stat(info.Path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("workspace exists after retry, stat error = %v", err)
	}
	if recorded, err := backend.cleanupOwnershipRecorded(t.Context(), info.Path); err != nil || recorded {
		t.Fatalf("cleanup ownership recorded after retry = %t, error = %v", recorded, err)
	}
}

func TestLocalGitReconcileResiduals(t *testing.T) {
	skipWindows(t)

	tests := []struct {
		name           string
		active         bool
		activeProcess  bool
		registered     bool
		unverified     bool
		wantRemoved    int
		wantActive     int
		wantRegistered int
		wantExists     bool
	}{
		{name: "removes owned residual", wantRemoved: 1},
		{name: "retains unverified residual", unverified: true, wantExists: true},
		{name: "skips active issue", active: true, wantActive: 1, wantExists: true},
		{name: "skips active process", activeProcess: true, wantActive: 1, wantExists: true},
		{name: "skips registered worktree", registered: true, wantRegistered: 1, wantExists: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enclosing := initSourceRepo(t)
			source := filepath.Join(enclosing, "source")
			initSourceRepoAt(t, source)
			publishCleanupSource(t, source)
			root := filepath.Join(enclosing, "workspaces")
			backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatalf("NewLocalGit() error = %v", err)
			}
			issue := Issue{ProjectID: "detent", ID: "2140", Identifier: "digitaldrywood/detent#2140"}
			var info Info
			if tt.registered {
				info, err = backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("Create() error = %v", err)
				}
				if err := backend.recordCleanupOwnership(t.Context(), info, issue, true); err != nil {
					t.Fatalf("recordCleanupOwnership() error = %v", err)
				}
			} else {
				info = strandCleanupWorkspace(t, backend, source, issue)
			}
			if tt.unverified {
				record, err := backend.readOwnershipRecord(cleanupOwnershipRecordRelativePath(info.Path))
				if err != nil {
					t.Fatal(err)
				}
				record.CleanupStarted = false
				if err := backend.writeOwnershipRecord(record); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { restoreWritableTree(t, info.Path) })
			if tt.activeProcess {
				backend.scanWorkspacePaths = func(context.Context, string) ([]int, error) {
					return []int{os.Getpid() + 1000}, nil
				}
			}
			var active []Issue
			if tt.active {
				active = []Issue{issue}
			}

			result, err := backend.ReconcileResiduals(t.Context(), active)
			if tt.unverified {
				if !errors.Is(err, ErrWorkspacePreserved) || result.PreservedSkipped != 1 {
					t.Fatalf("reconcile = %+v, %v; want preservation", result, err)
				}
			} else if err != nil {
				t.Fatalf("ReconcileResiduals() error = %v", err)
			}
			if result.Removed != tt.wantRemoved || result.ActiveSkipped != tt.wantActive || result.RegisteredSkipped != tt.wantRegistered {
				t.Fatalf("ReconcileResiduals() = %+v, want removed=%d active=%d registered=%d", result, tt.wantRemoved, tt.wantActive, tt.wantRegistered)
			}
			if got := len(result.CompletedPaths); got != tt.wantRemoved {
				t.Fatalf("ReconcileResiduals() completed paths = %d, want %d", got, tt.wantRemoved)
			}
			_, statErr := os.Stat(info.Path)
			if got := statErr == nil; got != tt.wantExists {
				t.Fatalf("residual workspace exists = %t, want %t, stat error = %v", got, tt.wantExists, statErr)
			}
		})
	}
}

func TestLocalGitReconcileResidualsRejectsInsufficientOwnership(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	foreign := filepath.Join(root, "detent-digitaldrywood_detent_9999-000000000000")
	if err := os.MkdirAll(filepath.Join(foreign, "node_modules"), 0o700); err != nil {
		t.Fatalf("create foreign directory: %v", err)
	}
	sourceCommonDir, err := gitCommonDir(t.Context(), source)
	if err != nil {
		t.Fatalf("gitCommonDir() error = %v", err)
	}
	record := cleanupOwnershipRecord{
		Schema:          cleanupOwnershipSchema,
		Path:            foreign,
		Key:             filepath.Base(foreign),
		SourceCommonDir: sourceCommonDir + "-other",
	}
	if err := backend.writeOwnershipRecord(record); err != nil {
		t.Fatalf("writeOwnershipRecord() error = %v", err)
	}

	result, err := backend.ReconcileResiduals(t.Context(), nil)
	if err != nil {
		t.Fatalf("ReconcileResiduals() error = %v", err)
	}
	if result.UnownedSkipped != 1 || result.Removed != 0 {
		t.Fatalf("ReconcileResiduals() = %+v, want one unowned skip", result)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("foreign directory was changed, stat error = %v", err)
	}
}

func strandCleanupWorkspace(t *testing.T, backend *LocalGit, source string, issue Issue) Info {
	t.Helper()

	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := backend.recordCleanupOwnership(t.Context(), info, issue, true); err != nil {
		t.Fatalf("recordCleanupOwnership() error = %v", err)
	}
	if err := ensureGitInfoExcludes(t.Context(), info.Path, []string{"node_modules/"}); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(info.Path, "node_modules", "locked")
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatalf("create locked artifact directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(locked, "generated.js"), []byte("generated"), 0o400); err != nil {
		t.Fatalf("write locked artifact: %v", err)
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatalf("lock artifact directory: %v", err)
	}
	if err := backend.beginWorkspaceCleanup(t.Context(), info, issue, true); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "git", "-C", source, "worktree", "remove", "--force", info.Path)
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("git worktree remove succeeded, want partial removal failure: %s", output)
	}
	if err := os.Chmod(locked, 0o700); err != nil {
		t.Fatalf("restore locked artifact directory: %v", err)
	}
	registered, err := backend.sourceWorktreeRegistered(t.Context(), info.Path)
	if err != nil {
		t.Fatalf("sourceWorktreeRegistered() error = %v", err)
	}
	if registered {
		t.Fatal("workspace remains registered after partial removal")
	}
	return info
}

func publishCleanupSource(t *testing.T, source string) {
	t.Helper()
	runGit(t, source, "remote", "add", "origin", initBareRemote(t))
	runGit(t, source, "push", "-u", "origin", "main")
}

func TestLocalGitReconcileUnrecordedWorkspaces(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		work          string
		active        bool
		process       bool
		wantRemoved   int
		wantPreserved int
		wantActive    int
	}{
		{name: "clean without registry", wantRemoved: 1},
		{name: "dirty without registry", work: "dirty", wantPreserved: 1},
		{name: "unpushed without registry", work: "unpushed", wantPreserved: 1},
		{name: "active issue", active: true, wantActive: 1},
		{name: "active process", process: true, wantActive: 1},
		{name: "uninspectable orphan", work: "broken", wantPreserved: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			publishCleanupSource(t, source)
			backend, err := NewLocalGit(LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{ProjectID: "detent", Identifier: "detent#2218"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			if tt.work == "dirty" || tt.work == "unpushed" {
				if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("unique work"), 0o600); err != nil {
					t.Fatal(err)
				}
				if tt.work == "unpushed" {
					runGit(t, info.Path, "add", "README.md")
					runGit(t, info.Path, "commit", "-m", "unique work")
				}
			}
			if tt.work == "broken" {
				if err := os.Rename(filepath.Join(info.Path, ".git"), filepath.Join(info.Path, "saved-git")); err != nil {
					t.Fatal(err)
				}
			}
			backend.scanWorkspacePaths = func(context.Context, string) ([]int, error) {
				if tt.process {
					return []int{os.Getpid() + 1000}, nil
				}
				return nil, nil
			}
			var active []Issue
			if tt.active {
				active = []Issue{issue}
			}
			result, err := backend.ReconcileResiduals(t.Context(), active)
			if (err != nil) != (tt.wantPreserved > 0) {
				t.Fatalf("reconcile error = %v", err)
			}
			if result.Removed != tt.wantRemoved || result.PreservedSkipped != tt.wantPreserved || result.ActiveSkipped != tt.wantActive {
				t.Fatalf("reconcile = %+v, want removed=%d preserved=%d active=%d", result, tt.wantRemoved, tt.wantPreserved, tt.wantActive)
			}
			if len(result.Failures) != tt.wantPreserved || len(result.CompletedPaths) != tt.wantRemoved {
				t.Fatalf("reconcile evidence = %+v", result)
			}
			_, statErr := os.Stat(info.Path)
			if tt.wantRemoved == 1 {
				if !errors.Is(statErr, fs.ErrNotExist) || branchExists(t, source, info.Branch) {
					t.Fatalf("clean workspace remains: %v", statErr)
				}
			} else if statErr != nil || !branchExists(t, source, info.Branch) {
				t.Fatalf("retained workspace missing: %v", statErr)
			}
		})
	}
}

func TestLocalGitOrphanDiscoveryKeepsSourceRepository(t *testing.T) {
	t.Parallel()
	for _, nested := range []bool{false, true} {
		t.Run(strconv.FormatBool(nested), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := root
			if nested {
				source = filepath.Join(root, "source")
			}
			initSourceRepoAt(t, source)
			publishCleanupSource(t, source)
			backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatal(err)
			}
			result, err := backend.ReconcileResiduals(t.Context(), nil)
			if err != nil || result.Removed != 0 {
				t.Fatalf("reconcile = %+v, %v", result, err)
			}
			if got := strings.TrimSpace(runGit(t, source, "rev-parse", "--is-inside-work-tree")); got != "true" {
				t.Fatalf("source repository changed: %s", got)
			}
		})
	}
}

func TestLocalGitReconcilePreservesManualWorktrees(t *testing.T) {
	t.Parallel()
	for _, branch := range []string{"feature/manual-hotfix", "detent/manual-hotfix", ""} {
		t.Run(branch, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			publishCleanupSource(t, source)
			root := filepath.Join(t.TempDir(), "workspaces")
			backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "detent-astra-effort-ceiling")
			if branch == "" {
				runGit(t, source, "worktree", "add", "--detach", path, "main")
			} else {
				runGit(t, source, "worktree", "add", "-b", branch, path, "main")
				runGit(t, path, "commit", "--allow-empty", "-m", "manual hotfix")
				runGit(t, path, "push", "-u", "origin", branch)
			}
			if got := strings.TrimSpace(runGit(t, path, "status", "--porcelain")); got != "" {
				t.Fatalf("manual worktree is dirty: %s", got)
			}
			if recorded, err := backend.cleanupOwnershipRecorded(t.Context(), path); err != nil || recorded {
				t.Fatalf("ownership = %t, %v; want no record", recorded, err)
			}
			backend.scanWorkspacePaths = func(context.Context, string) ([]int, error) {
				return nil, nil
			}
			result, err := backend.ReconcileResiduals(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("manual worktree removed: %v; result = %+v", err, result)
			}
			if registered, err := backend.sourceWorktreeRegistered(t.Context(), path); err != nil || !registered {
				t.Fatalf("manual worktree registration = %t, %v", registered, err)
			}
			if branch != "" && !branchExists(t, source, branch) {
				t.Fatal("manual branch removed")
			}
			if result.UnownedSkipped != 1 || result.Removed != 0 || len(result.CompletedPaths) != 0 || len(result.Failures) != 0 || result.PreservedSkipped != 0 || result.RegisteredSkipped != 0 || result.ActiveSkipped != 0 {
				t.Fatalf("reconcile = %+v; want one unowned skip only", result)
			}
			if recorded, err := backend.cleanupOwnershipRecorded(t.Context(), path); err != nil || recorded {
				t.Fatalf("manual worktree adopted: ownership = %t, %v", recorded, err)
			}
		})
	}
}
