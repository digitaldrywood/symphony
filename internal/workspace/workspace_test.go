package workspace

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/testenv"
)

const workspacePackageDefaultParallelism = "1"

func TestMain(m *testing.M) {
	if err := testenv.ClearGitEnvironment(); err != nil {
		panic(err)
	}
	if !hasExplicitTestParallelism(os.Args[1:]) {
		if err := flag.Set("test.parallel", workspacePackageDefaultParallelism); err != nil {
			fmt.Fprintf(os.Stderr, "configure workspace test parallelism: %v\n", err)
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func TestHasExplicitTestParallelism(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "default"},
		{name: "unrelated flag", args: []string{"-test.run=LocalGit"}},
		{name: "single dash value", args: []string{"-test.parallel=4"}, want: true},
		{name: "single dash separate value", args: []string{"-test.parallel", "4"}, want: true},
		{name: "double dash value", args: []string{"--test.parallel=4"}, want: true},
		{name: "double dash separate value", args: []string{"--test.parallel", "4"}, want: true},
		{name: "before terminator", args: []string{"-test.parallel=4", "--"}, want: true},
		{name: "after terminator", args: []string{"--", "-test.parallel=4"}},
		{name: "after positional argument", args: []string{"fixture", "-test.parallel=4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := hasExplicitTestParallelism(tt.args); got != tt.want {
				t.Fatalf("hasExplicitTestParallelism(%q) = %t, want %t", tt.args, got, tt.want)
			}
		})
	}
}

func hasExplicitTestParallelism(args []string) bool {
	for _, arg := range args {
		if arg == "--" || arg == "-" || !strings.HasPrefix(arg, "-") {
			return false
		}
		if arg == "-test.parallel" || arg == "--test.parallel" || strings.HasPrefix(arg, "-test.parallel=") || strings.HasPrefix(arg, "--test.parallel=") {
			return true
		}
	}
	return false
}

func TestLocalGitCreateCreatesWorktreeBranchAndRunsAfterCreateHook(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: "printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \"$PWD\" \"$(git branch --show-current)\" \"$ISSUE_IDENTIFIER\" \"$WORKSPACE_KEY\" \"$BRANCH\" \"$DETENT_ISSUE_IDENTIFIER\" \"$DETENT_WORKSPACE_KEY\" \"$DETENT_BRANCH\" >> " + shellQuote(tracePath),
			Timeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{ID: "issue-node", Identifier: "DD/19"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if !info.Created {
		t.Fatal("Create() Created = false, want true")
	}
	if info.Key != "DD_19" {
		t.Fatalf("Create() Key = %q, want DD_19", info.Key)
	}
	if filepath.Base(info.Path) != "DD_19" {
		t.Fatalf("Create() Path = %q, want basename DD_19", info.Path)
	}
	if info.Branch != "detent/dd_19" {
		t.Fatalf("Create() Branch = %q, want detent/dd_19", info.Branch)
	}
	if got := strings.TrimSpace(runGit(t, info.Path, "branch", "--show-current")); got != "detent/dd_19" {
		t.Fatalf("worktree branch = %q, want detent/dd_19", got)
	}
	if got := readFile(t, filepath.Join(info.Path, "README.md")); got != "source repo\n" {
		t.Fatalf("README.md = %q, want source repo", got)
	}

	trace := strings.TrimSpace(readFile(t, tracePath))
	fields := strings.Split(trace, "|")
	if len(fields) != 8 {
		t.Fatalf("after_create trace = %q, want eight fields", trace)
	}
	if fields[0] != info.Path {
		t.Fatalf("after_create cwd = %q, want %q", fields[0], info.Path)
	}
	if fields[1] != "detent/dd_19" {
		t.Fatalf("after_create branch = %q, want detent/dd_19", fields[1])
	}
	if fields[2] != "DD/19" {
		t.Fatalf("ISSUE_IDENTIFIER = %q, want DD/19", fields[2])
	}
	if fields[3] != "DD_19" {
		t.Fatalf("WORKSPACE_KEY = %q, want DD_19", fields[3])
	}
	if fields[4] != "detent/dd_19" {
		t.Fatalf("BRANCH = %q, want detent/dd_19", fields[4])
	}
	if fields[5] != "DD/19" {
		t.Fatalf("DETENT_ISSUE_IDENTIFIER = %q, want DD/19", fields[5])
	}
	if fields[6] != "DD_19" {
		t.Fatalf("DETENT_WORKSPACE_KEY = %q, want DD_19", fields[6])
	}
	if fields[7] != "detent/dd_19" {
		t.Fatalf("DETENT_BRANCH = %q, want detent/dd_19", fields[7])
	}
}

func TestLocalGitCreateSerializesAfterCreateHooksForSharedSource(t *testing.T) {
	skipWindows(t)

	source := initSourceRepo(t)
	alternateSource := filepath.Join(t.TempDir(), "source-worktree")
	runGit(t, source, "worktree", "add", "--detach", alternateSource, "HEAD")
	hookState := t.TempDir()
	lockPath := filepath.Join(hookState, "hook.lock")
	startedPath := filepath.Join(hookState, "first.started")
	releasePath := filepath.Join(hookState, "first.release")
	hookCommand := "set -eu\n" +
		"mkdir " + shellQuote(lockPath) + "\n" +
		"if [ \"$ISSUE_IDENTIFIER\" = \"DD-FIRST\" ]; then\n" +
		"  : > " + shellQuote(startedPath) + "\n" +
		"  while [ ! -f " + shellQuote(releasePath) + " ]; do sleep 0.01; done\n" +
		"fi\n" +
		"rmdir " + shellQuote(lockPath)

	newBackend := func(sourceRoot string) *LocalGit {
		backend, err := NewLocalGit(LocalGitOptions{
			Root:       filepath.Join(t.TempDir(), "workspaces"),
			SourceRoot: sourceRoot,
			AutoBranch: true,
			Hooks: Hooks{
				AfterCreate: hookCommand,
				Timeout:     5 * time.Second,
			},
		})
		if err != nil {
			t.Fatalf("NewLocalGit() error = %v", err)
		}
		return backend
	}
	firstBackend := newBackend(source)
	secondBackend := newBackend(alternateSource)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := firstBackend.Create(ctx, Issue{Identifier: "DD-FIRST"})
		firstDone <- err
	}()
	waitForFile(t, startedPath, 5*time.Second)

	secondDone := make(chan error, 1)
	go func() {
		_, err := secondBackend.Create(ctx, Issue{Identifier: "DD-SECOND"})
		secondDone <- err
	}()

	premature := false
	var prematureErr error
	select {
	case prematureErr = <-secondDone:
		premature = true
	case <-time.After(500 * time.Millisecond):
	}
	if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
		t.Fatalf("release first hook: %v", err)
	}
	if err := waitForError(t, firstDone, 5*time.Second); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if premature {
		t.Fatalf("second Create() completed while first hook held the source lock: %v", prematureErr)
	}
	if err := waitForError(t, secondDone, 5*time.Second); err != nil {
		t.Fatalf("second Create() error = %v", err)
	}
}

func TestLocalGitCreateRunsAfterCreateHooksConcurrentlyForDifferentSources(t *testing.T) {
	skipWindows(t)

	hookState := t.TempDir()
	startedPath := filepath.Join(hookState, "first.started")
	releasePath := filepath.Join(hookState, "first.release")
	hookCommand := "set -eu\n" +
		"if [ \"$ISSUE_IDENTIFIER\" = \"DD-FIRST\" ]; then\n" +
		"  : > " + shellQuote(startedPath) + "\n" +
		"  while [ ! -f " + shellQuote(releasePath) + " ]; do sleep 0.01; done\n" +
		"fi"
	newBackend := func(source string) *LocalGit {
		backend, err := NewLocalGit(LocalGitOptions{
			Root:       filepath.Join(t.TempDir(), "workspaces"),
			SourceRoot: source,
			AutoBranch: true,
			Hooks: Hooks{
				AfterCreate: hookCommand,
				Timeout:     5 * time.Second,
			},
		})
		if err != nil {
			t.Fatalf("NewLocalGit() error = %v", err)
		}
		return backend
	}
	firstBackend := newBackend(initSourceRepo(t))
	secondBackend := newBackend(initSourceRepo(t))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := firstBackend.Create(ctx, Issue{Identifier: "DD-FIRST"})
		firstDone <- err
	}()
	waitForFile(t, startedPath, 5*time.Second)

	secondDone := make(chan error, 1)
	go func() {
		_, err := secondBackend.Create(ctx, Issue{Identifier: "DD-SECOND"})
		secondDone <- err
	}()
	if err := waitForError(t, secondDone, 5*time.Second); err != nil {
		t.Fatalf("second Create() error = %v", err)
	}
	if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
		t.Fatalf("release first hook: %v", err)
	}
	if err := waitForError(t, firstDone, 5*time.Second); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
}

func TestLocalGitInfoForIssueNamespacesKeysByProjectID(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	backend := &LocalGit{
		root:       root,
		autoBranch: true,
	}

	tests := []struct {
		name          string
		issue         Issue
		wantKey       string
		wantKeyPrefix string
	}{
		{
			name:    "legacy issue identifier without project id",
			issue:   Issue{Identifier: "digitaldrywood/detent#42"},
			wantKey: "digitaldrywood_detent_42",
		},
		{
			name:    "reserved detent metadata key",
			issue:   Issue{Identifier: ".detent"},
			wantKey: "issue",
		},
		{
			name:          "alpha project",
			issue:         Issue{ProjectID: "alpha", Identifier: "digitaldrywood/detent#42"},
			wantKeyPrefix: "alpha-digitaldrywood_detent_42-",
		},
		{
			name:          "bravo project same identifier",
			issue:         Issue{ProjectID: "bravo", Identifier: "digitaldrywood/detent#42"},
			wantKeyPrefix: "bravo-digitaldrywood_detent_42-",
		},
		{
			name:          "project ids with same safe key",
			issue:         Issue{ProjectID: "foo/bar", Identifier: "baz"},
			wantKeyPrefix: "foo_bar-baz-",
		},
		{
			name:          "second project id with same safe key",
			issue:         Issue{ProjectID: "foo_bar", Identifier: "baz"},
			wantKeyPrefix: "foo_bar-baz-",
		},
		{
			name:          "separator ambiguity left",
			issue:         Issue{ProjectID: "foo", Identifier: "bar-baz"},
			wantKeyPrefix: "foo-bar-baz-",
		},
		{
			name:          "separator ambiguity right",
			issue:         Issue{ProjectID: "foo-bar", Identifier: "baz"},
			wantKeyPrefix: "foo-bar-baz-",
		},
	}

	keys := make(map[string]string, len(tests))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := backend.infoForIssue(tt.issue)
			if err != nil {
				t.Fatalf("infoForIssue() error = %v", err)
			}
			switch {
			case tt.wantKey != "" && info.Key != tt.wantKey:
				t.Fatalf("Key = %q, want %q", info.Key, tt.wantKey)
			case tt.wantKeyPrefix != "" && !strings.HasPrefix(info.Key, tt.wantKeyPrefix):
				t.Fatalf("Key = %q, want prefix %q", info.Key, tt.wantKeyPrefix)
			case tt.wantKeyPrefix != "" && len(info.Key) == len(tt.wantKeyPrefix):
				t.Fatalf("Key = %q, want digest suffix", info.Key)
			}
			if filepath.Base(info.Path) != info.Key {
				t.Fatalf("Path basename = %q, want %q", filepath.Base(info.Path), info.Key)
			}
			wantBranch := "detent/" + strings.ToLower(info.Key)
			if info.Branch != wantBranch {
				t.Fatalf("Branch = %q, want %q", info.Branch, wantBranch)
			}

			keys[tt.name] = info.Key
		})
	}

	for leftName, leftKey := range keys {
		for rightName, rightKey := range keys {
			if leftName >= rightName {
				continue
			}
			if leftKey == rightKey {
				t.Fatalf("%s and %s both produced key %q", leftName, rightName, leftKey)
			}
		}
	}
}

func TestLocalGitCreateAndCleanupWithoutHooks(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	publishCleanupSource(t, source)
	root := filepath.Join(t.TempDir(), "workspaces")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-NATIVE"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if !info.Created {
		t.Fatal("Create() Created = false, want true")
	}
	if got := strings.TrimSpace(runGit(t, info.Path, "branch", "--show-current")); got != "detent/dd-native" {
		t.Fatalf("worktree branch = %q, want detent/dd-native", got)
	}
	if got := readFile(t, filepath.Join(info.Path, "README.md")); got != "source repo\n" {
		t.Fatalf("README.md = %q, want source repo", got)
	}

	if err := backend.Cleanup(context.Background(), "DD-NATIVE"); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := os.Stat(info.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace exists after cleanup, stat error = %v", err)
	}
	if got := runGit(t, source, "worktree", "list", "--porcelain"); strings.Contains(got, info.Path) {
		t.Fatalf("git worktree list still contains removed path:\n%s", got)
	}
	if branchExists(t, source, "detent/dd-native") {
		t.Fatal("detent/dd-native branch still exists after cleanup")
	}
}

func TestLocalGitCreatePrunesMissingRegisteredWorktree(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewLocalGit(LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	issue := Issue{Identifier: "DD-STALE-REGISTRATION"}
	first, err := backend.Create(context.Background(), issue)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	movedPath := filepath.Join(t.TempDir(), "moved-workspace")
	if err := os.Rename(first.Path, movedPath); err != nil {
		t.Fatalf("move registered worktree: %v", err)
	}
	if got := runGit(t, source, "worktree", "list", "--porcelain"); !strings.Contains(got, filepath.ToSlash(first.Path)) {
		t.Fatalf("git worktree list does not contain missing registered path:\n%s", got)
	}

	second, err := backend.Create(context.Background(), issue)
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}
	if !second.Created {
		t.Fatal("second Create() Created = false, want true")
	}
	if second.Path != first.Path {
		t.Fatalf("second Create() Path = %q, want %q", second.Path, first.Path)
	}
	if got := strings.TrimSpace(runGit(t, second.Path, "branch", "--show-current")); got != first.Branch {
		t.Fatalf("second worktree branch = %q, want %q", got, first.Branch)
	}
}

func TestLocalGitCreateClassifiesOccupiedBranch(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewLocalGit(LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	occupiedPath := filepath.Join(t.TempDir(), "occupied-worktree")
	runGit(t, source, "worktree", "add", "-b", "detent/dd-conflict", occupiedPath, "HEAD")

	_, err = backend.Create(context.Background(), Issue{Identifier: "DD-CONFLICT"})
	if err == nil {
		t.Fatal("Create() error = nil, want occupied branch error")
	}
	var heldErr *BranchHeldError
	if !errors.As(err, &heldErr) {
		t.Fatalf("Create() error = %T, want *BranchHeldError", err)
	}
	if heldErr.Branch != "detent/dd-conflict" {
		t.Fatalf("BranchHeldError branch = %q, want detent/dd-conflict", heldErr.Branch)
	}
	requireSameFile(t, heldErr.Path, occupiedPath)
}

func TestRunWorktreeAddWithPrune(t *testing.T) {
	t.Parallel()

	initialPruneErr := errors.New("initial prune failed")
	retryPruneErr := errors.New("retry prune failed")
	nonRetryErr := errors.New("exit status 1")
	exit128Err := errors.New("exit status 128")
	retryAddErr := errors.New("retry add failed")
	nonRetryCommandErr := &CommandError{ExitCode: 1, Err: nonRetryErr}
	exit128CommandErr := &CommandError{ExitCode: 128, Err: exit128Err}

	tests := []struct {
		name        string
		pruneErrors []error
		addErrors   []error
		wantPrunes  int
		wantAdds    int
		wantErrors  []error
	}{
		{
			name:       "successful add",
			addErrors:  []error{nil},
			wantPrunes: 1,
			wantAdds:   1,
		},
		{
			name:        "initial prune fails",
			pruneErrors: []error{initialPruneErr},
			wantPrunes:  1,
			wantErrors:  []error{initialPruneErr},
		},
		{
			name:       "non-128 add is not retried",
			addErrors:  []error{nonRetryCommandErr},
			wantPrunes: 1,
			wantAdds:   1,
			wantErrors: []error{nonRetryErr},
		},
		{
			name:        "exit 128 succeeds after prune",
			pruneErrors: []error{nil, nil},
			addErrors:   []error{exit128CommandErr, nil},
			wantPrunes:  2,
			wantAdds:    2,
		},
		{
			name:        "retry prune fails",
			pruneErrors: []error{nil, retryPruneErr},
			addErrors:   []error{exit128CommandErr},
			wantPrunes:  2,
			wantAdds:    1,
			wantErrors:  []error{exit128Err, retryPruneErr},
		},
		{
			name:        "retry add fails without another retry",
			pruneErrors: []error{nil, nil},
			addErrors:   []error{exit128CommandErr, retryAddErr},
			wantPrunes:  2,
			wantAdds:    2,
			wantErrors:  []error{retryAddErr},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pruneCalls := 0
			addCalls := 0
			err := runWorktreeAddWithPrune(func() error {
				call := pruneCalls
				pruneCalls++
				if call >= len(tt.pruneErrors) {
					return nil
				}
				return tt.pruneErrors[call]
			}, func() error {
				call := addCalls
				addCalls++
				if call >= len(tt.addErrors) {
					return nil
				}
				return tt.addErrors[call]
			})

			if len(tt.wantErrors) == 0 && err != nil {
				t.Fatalf("runWorktreeAddWithPrune() error = %v, want nil", err)
			}
			if len(tt.wantErrors) > 0 && err == nil {
				t.Fatal("runWorktreeAddWithPrune() error = nil, want error")
			}
			for _, wantErr := range tt.wantErrors {
				if !errors.Is(err, wantErr) {
					t.Errorf("runWorktreeAddWithPrune() error = %v, want errors.Is(%v)", err, wantErr)
				}
			}
			if pruneCalls != tt.wantPrunes {
				t.Errorf("prune calls = %d, want %d", pruneCalls, tt.wantPrunes)
			}
			if addCalls != tt.wantAdds {
				t.Errorf("add calls = %d, want %d", addCalls, tt.wantAdds)
			}
		})
	}
}

func TestLocalGitCreateBasesNewBranchOnFetchedRemoteDefault(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	runGit(t, source, "switch", "-c", "dev")
	if err := os.WriteFile(filepath.Join(source, "dev.txt"), []byte("dev\n"), 0o600); err != nil {
		t.Fatalf("write dev file: %v", err)
	}
	runGit(t, source, "add", "dev.txt")
	runGit(t, source, "commit", "-m", "add dev branch")
	runGit(t, source, "push", "-u", "origin", "dev")
	runGit(t, remote, "symbolic-ref", "HEAD", "refs/heads/dev")

	runGit(t, source, "switch", "-c", "local-feature", "main")
	if err := os.WriteFile(filepath.Join(source, "local-only.txt"), []byte("local only\n"), 0o600); err != nil {
		t.Fatalf("write local-only file: %v", err)
	}
	runGit(t, source, "add", "local-only.txt")
	runGit(t, source, "commit", "-m", "add local-only change")

	publisher := filepath.Join(t.TempDir(), "publisher")
	runCommand(t, t.TempDir(), "git", "clone", remote, publisher)
	runGit(t, publisher, "config", "user.name", "Test Publisher")
	runGit(t, publisher, "config", "user.email", "publisher@example.com")
	if err := os.WriteFile(filepath.Join(publisher, "remote-latest.txt"), []byte("remote latest\n"), 0o600); err != nil {
		t.Fatalf("write remote-latest file: %v", err)
	}
	runGit(t, publisher, "add", "remote-latest.txt")
	runGit(t, publisher, "commit", "-m", "advance remote dev")
	runGit(t, publisher, "push", "origin", "dev")
	wantHead := strings.TrimSpace(runGit(t, publisher, "rev-parse", "HEAD"))

	backend, err := NewLocalGit(LocalGitOptions{
		Root:       filepath.Join(t.TempDir(), "workspaces"),
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-REMOTE-DEFAULT"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if got := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD")); got != wantHead {
		t.Fatalf("worktree HEAD = %q, want remote default HEAD %q", got, wantHead)
	}
	if got := readFile(t, filepath.Join(info.Path, "remote-latest.txt")); got != "remote latest\n" {
		t.Fatalf("remote-latest.txt = %q, want remote latest content", got)
	}
	if _, err := os.Stat(filepath.Join(info.Path, "local-only.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local-only.txt stat error = %v, want file absent", err)
	}
}

func TestLocalGitCreateDoesNotFallBackWhenOriginIsUnavailable(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	runGit(t, source, "remote", "add", "origin", filepath.Join(t.TempDir(), "missing.git"))
	backend, err := NewLocalGit(LocalGitOptions{
		Root:       filepath.Join(t.TempDir(), "workspaces"),
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	issue := Issue{Identifier: "DD-UNAVAILABLE-ORIGIN"}

	_, err = backend.Create(context.Background(), issue)
	if err == nil {
		t.Fatal("Create() error = nil, want unavailable origin error")
	}
	if !strings.Contains(err.Error(), "resolve origin default branch") {
		t.Fatalf("Create() error = %v, want remote default branch context", err)
	}
	info, infoErr := backend.infoForIssue(issue)
	if infoErr != nil {
		t.Fatalf("infoForIssue() error = %v", infoErr)
	}
	if _, statErr := os.Stat(info.Path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("workspace stat error = %v, want workspace absent", statErr)
	}
	if branchExists(t, source, info.Branch) {
		t.Fatalf("branch %q exists after failed Create()", info.Branch)
	}
}

func TestLocalGitCreateSerializesRemoteOperations(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	operationDir := t.TempDir()
	lockDir := filepath.Join(operationDir, "active")
	overlapPath := filepath.Join(operationDir, "overlap")
	uploadPackPath := filepath.Join(operationDir, "upload-pack")
	uploadPack := "#!/bin/sh\n" +
		"lock_dir=" + shellQuote(lockDir) + "\n" +
		"overlap_path=" + shellQuote(overlapPath) + "\n" +
		"owns_lock=\n" +
		"if mkdir \"$lock_dir\" 2>/dev/null; then\n" +
		"  owns_lock=1\n" +
		"else\n" +
		"  : > \"$overlap_path\"\n" +
		"fi\n" +
		"sleep 0.1\n" +
		"if [ -n \"$owns_lock\" ]; then\n" +
		"  rmdir \"$lock_dir\"\n" +
		"fi\n" +
		"exec git-upload-pack \"$@\"\n"
	if err := os.WriteFile(uploadPackPath, []byte(uploadPack), 0o700); err != nil {
		t.Fatalf("write upload-pack wrapper: %v", err)
	}
	runGit(t, source, "config", "remote.origin.uploadpack", uploadPackPath)

	backend, err := NewLocalGit(LocalGitOptions{
		Root:       filepath.Join(t.TempDir(), "workspaces"),
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}

	const creates = 6
	start := make(chan struct{})
	results := make(chan error, creates)
	for i := range creates {
		go func() {
			<-start
			_, err := backend.Create(context.Background(), Issue{Identifier: fmt.Sprintf("DD-CONCURRENT-%d", i)})
			results <- err
		}()
	}
	close(start)
	for range creates {
		if err := <-results; err != nil {
			t.Errorf("Create() error = %v", err)
		}
	}
	if _, err := os.Stat(overlapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remote operations overlapped, marker stat error = %v", err)
	}
}

func TestLocalGitPrepareMergeRebasesAndPushesCleanBranch(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	preparer, ok := backend.(MergePreparer)
	if !ok {
		t.Fatal("backend does not implement MergePreparer")
	}
	issue := Issue{Identifier: "DD-MERGE"}
	info, err := backend.Create(context.Background(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		t.Fatalf("write feature: %v", err)
	}
	runGit(t, info.Path, "add", "feature.txt")
	runGit(t, info.Path, "commit", "-m", "feature")
	runGit(t, info.Path, "push", "origin", "HEAD:"+info.Branch)

	if err := os.WriteFile(filepath.Join(source, "main.txt"), []byte("main\n"), 0o600); err != nil {
		t.Fatalf("write main: %v", err)
	}
	runGit(t, source, "add", "main.txt")
	runGit(t, source, "commit", "-m", "main change")
	runGit(t, source, "push", "origin", "main")

	result, err := preparer.PrepareMerge(context.Background(), info, issue, MergePrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareMerge() error = %v", err)
	}
	if result.Status != MergePrepareStatusClean {
		t.Fatalf("PrepareMerge() status = %q, want clean", result.Status)
	}
	if result.DiffStat != (DiffStat{}) {
		t.Fatalf("PrepareMerge() DiffStat = %#v, want zero", result.DiffStat)
	}
	if !result.HeadChanged {
		t.Fatal("PrepareMerge() HeadChanged = false, want true after rebase")
	}
	if got := readFile(t, filepath.Join(info.Path, "main.txt")); got != "main\n" {
		t.Fatalf("main.txt = %q, want rebased main file", got)
	}
	head := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD"))
	remoteHead := strings.Fields(runGit(t, source, "ls-remote", "origin", "refs/heads/"+info.Branch))
	if len(remoteHead) == 0 || remoteHead[0] != head {
		t.Fatalf("remote branch head = %#v, want %s", remoteHead, head)
	}

	result, err = preparer.PrepareMerge(context.Background(), info, issue, MergePrepareOptions{})
	if err != nil {
		t.Fatalf("second PrepareMerge() error = %v", err)
	}
	if result.HeadChanged {
		t.Fatal("second PrepareMerge() HeadChanged = true, want false for unchanged head")
	}
}

func TestLocalGitPrepareMergeUsesDevBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		remoteDefault string
		opts          MergePrepareOptions
	}{
		{name: "remote default", remoteDefault: "dev"},
		{name: "explicit target", remoteDefault: "main", opts: MergePrepareOptions{TargetBranch: "dev"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testLocalGitPrepareMergeUsesDevBranch(t, tt.remoteDefault, tt.opts)
		})
	}
}

func testLocalGitPrepareMergeUsesDevBranch(t *testing.T, remoteDefault string, opts MergePrepareOptions) {
	t.Helper()

	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	runGit(t, source, "switch", "-c", "dev")
	if err := os.WriteFile(filepath.Join(source, "dev.txt"), []byte("dev\n"), 0o600); err != nil {
		t.Fatalf("write dev file: %v", err)
	}
	runGit(t, source, "add", "dev.txt")
	runGit(t, source, "commit", "-m", "dev branch")
	runGit(t, source, "push", "-u", "origin", "dev")
	runGit(t, source, "config", "--replace-all", "remote.origin.fetch", "+refs/heads/main:refs/remotes/origin/main")
	runGit(t, source, "update-ref", "-d", "refs/remotes/origin/dev")
	runGit(t, remote, "symbolic-ref", "HEAD", "refs/heads/"+remoteDefault)

	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	preparer, ok := backend.(MergePreparer)
	if !ok {
		t.Fatal("backend does not implement MergePreparer")
	}
	issue := Issue{Identifier: "DD-DEFAULT-BRANCH"}
	info, err := backend.Create(context.Background(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		t.Fatalf("write feature: %v", err)
	}
	runGit(t, info.Path, "add", "feature.txt")
	runGit(t, info.Path, "commit", "-m", "feature")
	runGit(t, info.Path, "push", "origin", "HEAD:"+info.Branch)

	runGit(t, source, "switch", "main")
	if err := os.WriteFile(filepath.Join(source, "main-latest.txt"), []byte("main\n"), 0o600); err != nil {
		t.Fatalf("write latest main file: %v", err)
	}
	runGit(t, source, "add", "main-latest.txt")
	runGit(t, source, "commit", "-m", "main latest")
	runGit(t, source, "push", "origin", "main")
	runGit(t, source, "switch", "dev")
	if err := os.WriteFile(filepath.Join(source, "dev-latest.txt"), []byte("dev latest\n"), 0o600); err != nil {
		t.Fatalf("write latest dev file: %v", err)
	}
	runGit(t, source, "add", "dev-latest.txt")
	runGit(t, source, "commit", "-m", "dev latest")
	runGit(t, source, "push", "origin", "dev")

	result, err := preparer.PrepareMerge(context.Background(), info, issue, opts)
	if err != nil {
		t.Fatalf("PrepareMerge() error = %v", err)
	}
	if result.Status != MergePrepareStatusClean {
		t.Fatalf("PrepareMerge() status = %q, want clean", result.Status)
	}
	if got := readFile(t, filepath.Join(info.Path, "dev-latest.txt")); got != "dev latest\n" {
		t.Fatalf("dev-latest.txt = %q, want rebased dev file", got)
	}
	if _, err := os.Stat(filepath.Join(info.Path, "main-latest.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("main-latest.txt stat error = %v, want file absent", err)
	}
}

func TestLocalGitPrepareMergeAbortsConflictingRebase(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")

	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}
	preparer, ok := backend.(MergePreparer)
	if !ok {
		t.Fatal("backend does not implement MergePreparer")
	}
	issue := Issue{Identifier: "DD-CONFLICT"}
	info, err := backend.Create(context.Background(), issue)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("feature\n"), 0o600); err != nil {
		t.Fatalf("write feature README: %v", err)
	}
	runGit(t, info.Path, "add", "README.md")
	runGit(t, info.Path, "commit", "-m", "feature conflict")

	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("main\n"), 0o600); err != nil {
		t.Fatalf("write main README: %v", err)
	}
	runGit(t, source, "add", "README.md")
	runGit(t, source, "commit", "-m", "main conflict")
	runGit(t, source, "push", "origin", "main")

	result, err := preparer.PrepareMerge(context.Background(), info, issue, MergePrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareMerge() error = %v", err)
	}
	if result.Status != MergePrepareStatusConflict {
		t.Fatalf("PrepareMerge() status = %q, want conflict", result.Status)
	}
	if got := strings.TrimSpace(runGit(t, info.Path, "status", "--short")); got != "" {
		t.Fatalf("status after aborted rebase = %q, want clean", got)
	}
	if got := strings.TrimSpace(runGit(t, info.Path, "branch", "--show-current")); got != info.Branch {
		t.Fatalf("branch after aborted rebase = %q, want %q", got, info.Branch)
	}
	if got := readFile(t, filepath.Join(info.Path, "README.md")); got != "feature\n" {
		t.Fatalf("README after aborted rebase = %q, want feature branch content", got)
	}
	inProgress, err := rebaseInProgress(context.Background(), info.Path)
	if err != nil {
		t.Fatalf("rebaseInProgress() error = %v", err)
	}
	if inProgress {
		t.Fatal("rebase still in progress after PrepareMerge conflict")
	}
}

func TestGitMetadataWritableRootsForLinkedWorktree(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-GIT-ROOTS"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	roots, err := GitMetadataWritableRoots(context.Background(), info.Path)
	if err != nil {
		t.Fatalf("GitMetadataWritableRoots() error = %v", err)
	}

	commonDir := mustCanonicalExistingPath(t, filepath.Join(source, ".git"))
	wantRoots := []string{
		linkedWorktreeGitDir(t, info.Path),
		mustCanonicalExistingPath(t, filepath.Join(commonDir, "objects")),
		mustCanonicalExistingPath(t, filepath.Join(commonDir, "refs", "heads", "detent")),
		mustCanonicalExistingPath(t, filepath.Join(commonDir, "logs", "refs", "heads", "detent")),
	}
	for _, want := range wantRoots {
		if !containsString(roots, want) {
			t.Fatalf("GitMetadataWritableRoots() = %#v, missing %q", roots, want)
		}
	}
	if containsString(roots, commonDir) {
		t.Fatalf("GitMetadataWritableRoots() = %#v, should not allow entire common git dir %q", roots, commonDir)
	}

	if err := os.WriteFile(filepath.Join(info.Path, "agent.txt"), []byte("agent edit\n"), 0o600); err != nil {
		t.Fatalf("write agent edit: %v", err)
	}
	runGit(t, info.Path, "add", "agent.txt")
	runGit(t, info.Path, "commit", "-m", "agent commit")
	if got := strings.TrimSpace(runGit(t, info.Path, "log", "-1", "--pretty=%s")); got != "agent commit" {
		t.Fatalf("latest commit subject = %q, want agent commit", got)
	}
}

func TestGitMetadataWritableRootsRejectsEnclosingRepository(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	workspacePath := filepath.Join(source, "nested")
	if err := os.MkdirAll(workspacePath, 0o700); err != nil {
		t.Fatalf("mkdir nested workspace: %v", err)
	}

	if _, err := GitMetadataWritableRoots(context.Background(), workspacePath); err == nil {
		t.Fatal("GitMetadataWritableRoots() error = nil, want nested workspace rejection")
	}
}

func TestLocalGitHooksUseNonLoginShell(t *testing.T) {
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")
	argsPath := filepath.Join(t.TempDir(), "shell-args.trace")
	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	shellPath := filepath.Join(binDir, "sh")
	shellScript := "#!/bin/sh\nprintf '%s\\n' \"$1\" > " + shellQuote(argsPath) + "\nexec /bin/sh \"$@\"\n"
	if err := os.WriteFile(shellPath, []byte(shellScript), 0o700); err != nil {
		t.Fatalf("write shell wrapper: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: "printf 'ok\n' > " + shellQuote(tracePath),
			Timeout:     5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	if _, err := backend.Create(context.Background(), Issue{Identifier: "DD-SHELL"}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if got := readFile(t, argsPath); got != "-c\n" {
		t.Fatalf("hook shell first arg = %q, want -c", got)
	}
	if got := readFile(t, tracePath); got != "ok\n" {
		t.Fatalf("hook trace = %q, want ok", got)
	}
}

func TestLocalGitHooksUseConfiguredShell(t *testing.T) {
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")
	argsPath := filepath.Join(t.TempDir(), "shell-args.trace")
	shellPath := filepath.Join(t.TempDir(), "custom-sh")
	shellScript := "#!/bin/sh\nprintf '%s\\n' \"$0|$1|$2\" > " + shellQuote(argsPath) + "\nexec /bin/sh \"$@\"\n"
	if err := os.WriteFile(shellPath, []byte(shellScript), 0o700); err != nil {
		t.Fatalf("write shell wrapper: %v", err)
	}

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			Shell:       shellPath,
			AfterCreate: "printf 'ok\n' > " + shellQuote(tracePath),
			Timeout:     5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	if _, err := backend.Create(context.Background(), Issue{Identifier: "DD-SHELL-CFG"}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	gotArgs := strings.TrimSpace(readFile(t, argsPath))
	wantArgs := shellPath + "|-c|" + "printf 'ok\n' > " + shellQuote(tracePath)
	if gotArgs != wantArgs {
		t.Fatalf("hook shell args = %q, want %q", gotArgs, wantArgs)
	}
	if got := readFile(t, tracePath); got != "ok\n" {
		t.Fatalf("hook trace = %q, want ok", got)
	}
}

func TestRunGitAtWithEnvCancellationReturnsPromptly(t *testing.T) {
	skipWindows(t)

	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	gitPath := filepath.Join(binDir, "git")
	gitScript := "#!/bin/sh\nsleep 4 &\nwait\n"
	if err := os.WriteFile(gitPath, []byte(gitScript), 0o700); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := runGitAtWithEnv(ctx, t.TempDir(), nil, "status")
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("runGitAtWithEnv() error = nil, want cancellation error")
	}
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("runGitAtWithEnv() error = %T, want *CommandError", err)
	}
	if !errors.Is(commandErr.Err, context.DeadlineExceeded) {
		t.Fatalf("CommandError.Err = %v, want context deadline exceeded", commandErr.Err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("runGitAtWithEnv() elapsed = %s, want cancellation within wait delay", elapsed)
	}
}

func TestLocalGitHookCancellationReturnsPromptly(t *testing.T) {
	skipWindows(t)

	workspacePath := t.TempDir()
	startedPath := filepath.Join(workspacePath, "hook.started")
	completedPath := filepath.Join(workspacePath, "hook.completed")
	backend := &LocalGit{
		hooks:  Hooks{Timeout: time.Minute},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hookDone := make(chan error, 1)
	go func() {
		hookDone <- backend.runHook(
			ctx,
			"before_run",
			"(\n"+
				": > "+shellQuote(startedPath)+"\n"+
				"sleep 4\n"+
				": > "+shellQuote(completedPath)+"\n"+
				") &\n"+
				"wait",
			Info{Path: workspacePath, Key: "DD-HOOK", Branch: "detent/dd-hook"},
			Issue{Identifier: "DD-HOOK"},
		)
	}()

	waitForFile(t, startedPath, 10*time.Second)
	cancel()
	err := waitForError(t, hookDone, 10*time.Second)

	if err == nil {
		t.Fatal("runHook() error = nil, want cancellation error")
	}
	var hookErr *HookError
	if !errors.As(err, &hookErr) {
		t.Fatalf("runHook() error = %T, want *HookError", err)
	}
	if !errors.Is(hookErr.Err, context.Canceled) {
		t.Fatalf("HookError.Err = %v, want context canceled", hookErr.Err)
	}
	if _, err := os.Stat(completedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hook workload completed before cancellation returned, stat error = %v", err)
	}
}

func TestLocalGitHookAllowsDaemonizedSuccess(t *testing.T) {
	skipWindows(t)

	workspacePath := t.TempDir()
	backend := &LocalGit{
		hooks:  Hooks{Timeout: 3 * time.Second},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	started := time.Now()
	err := backend.runHook(
		context.Background(),
		"before_run",
		"sleep 4 &",
		Info{Path: workspacePath, Key: "DD-HOOK", Branch: "detent/dd-hook"},
		Issue{Identifier: "DD-HOOK"},
	)
	elapsed := time.Since(started)

	if err != nil {
		t.Fatalf("runHook() error = %v, want nil", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("runHook() elapsed = %s, want daemonized hook within wait delay", elapsed)
	}
}

func TestLocalGitCreateReusesExistingWorktreeWithoutAfterCreate(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: "printf 'after-create\n' >> " + shellQuote(tracePath),
			Timeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	first, err := backend.Create(context.Background(), Issue{Identifier: "DD-REUSE"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(first.Path, "local-progress.txt"), []byte("keep\n"), 0o600); err != nil {
		t.Fatalf("write local progress: %v", err)
	}

	second, err := backend.Create(context.Background(), Issue{Identifier: "DD-REUSE"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if second.Created {
		t.Fatal("second Create() Created = true, want false")
	}
	if second.Path != first.Path {
		t.Fatalf("second Create() Path = %q, want %q", second.Path, first.Path)
	}
	if got := readFile(t, filepath.Join(second.Path, "local-progress.txt")); got != "keep\n" {
		t.Fatalf("local-progress.txt = %q, want keep", got)
	}
	if got := strings.Count(readFile(t, tracePath), "after-create"); got != 1 {
		t.Fatalf("after_create runs = %d, want 1", got)
	}
}

func TestLocalGitCreateRecoversCleanDetachedWorktree(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: "printf 'after-create\n' >> " + shellQuote(tracePath),
			Timeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	first, err := backend.Create(context.Background(), Issue{Identifier: "DD-DETACHED"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	runGit(t, first.Path, "switch", "--detach", "HEAD")

	second, err := backend.Create(context.Background(), Issue{Identifier: "DD-DETACHED"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if !second.Created {
		t.Fatal("second Create() Created = false, want true")
	}
	if second.Path != first.Path {
		t.Fatalf("second Create() Path = %q, want %q", second.Path, first.Path)
	}
	if got := strings.TrimSpace(runGit(t, second.Path, "branch", "--show-current")); got != "detent/dd-detached" {
		t.Fatalf("worktree branch = %q, want detent/dd-detached", got)
	}
	if got := strings.Count(readFile(t, tracePath), "after-create"); got != 2 {
		t.Fatalf("after_create runs = %d, want 2", got)
	}
}

func TestLocalGitCreateRecoversCleanWrongBranchWorktree(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "after-create.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: "printf 'after-create\n' >> " + shellQuote(tracePath),
			Timeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-REUSE"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}

	runGit(t, source, "branch", "detent/other")
	runGit(t, info.Path, "switch", "detent/other")

	second, err := backend.Create(context.Background(), Issue{Identifier: "DD-REUSE"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if !second.Created {
		t.Fatal("second Create() Created = false, want true")
	}
	if second.Path != info.Path {
		t.Fatalf("second Create() Path = %q, want %q", second.Path, info.Path)
	}
	if got := strings.TrimSpace(runGit(t, second.Path, "branch", "--show-current")); got != "detent/dd-reuse" {
		t.Fatalf("worktree branch = %q, want detent/dd-reuse", got)
	}
	if got := strings.Count(readFile(t, tracePath), "after-create"); got != 2 {
		t.Fatalf("after_create runs = %d, want 2", got)
	}
}

func TestLocalGitCreateQuarantinesDirtyDetachedWorktree(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	var logs strings.Builder

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	first, err := backend.Create(context.Background(), Issue{Identifier: "DD-DIRTY-DETACHED"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	runGit(t, first.Path, "switch", "--detach", "HEAD")
	if err := os.WriteFile(filepath.Join(first.Path, "local-progress.txt"), []byte("keep\n"), 0o600); err != nil {
		t.Fatalf("write local progress: %v", err)
	}

	second, err := backend.Create(context.Background(), Issue{Identifier: "DD-DIRTY-DETACHED"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if !second.Created {
		t.Fatal("second Create() Created = false, want true")
	}
	if second.Path != first.Path {
		t.Fatalf("second Create() Path = %q, want %q", second.Path, first.Path)
	}
	if got := strings.TrimSpace(runGit(t, second.Path, "branch", "--show-current")); got != "detent/dd-dirty-detached" {
		t.Fatalf("worktree branch = %q, want detent/dd-dirty-detached", got)
	}
	if _, err := os.Stat(filepath.Join(second.Path, "local-progress.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local progress exists in fresh workspace, stat error = %v", err)
	}

	quarantineDir := filepath.Join(root, ".detent", "quarantine")
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		t.Fatalf("read quarantine dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("quarantine entries = %d, want 1", len(entries))
	}
	quarantinedPath := filepath.Join(quarantineDir, entries[0].Name())
	if got := readFile(t, filepath.Join(quarantinedPath, "local-progress.txt")); got != "keep\n" {
		t.Fatalf("quarantined local-progress.txt = %q, want keep", got)
	}
	if got := strings.TrimSpace(runGit(t, quarantinedPath, "branch", "--show-current")); got != "" {
		t.Fatalf("quarantined worktree branch = %q, want detached HEAD", got)
	}
	if got := logs.String(); !strings.Contains(got, "quarantined stale workspace") || !strings.Contains(got, quarantinedPath) {
		t.Fatalf("logs = %q, want quarantine report for %s", got, quarantinedPath)
	}
}

func TestLocalGitCreateQuarantinesCleanDetachedUnreferencedCommit(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	var logs strings.Builder

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	first, err := backend.Create(context.Background(), Issue{Identifier: "DD-DETACHED-COMMIT"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	runGit(t, first.Path, "switch", "--detach", "HEAD")
	if err := os.WriteFile(filepath.Join(first.Path, "local-commit.txt"), []byte("keep\n"), 0o600); err != nil {
		t.Fatalf("write local commit file: %v", err)
	}
	runGit(t, first.Path, "add", "local-commit.txt")
	runGit(t, first.Path, "commit", "-m", "local detached commit")
	if got := strings.TrimSpace(runGit(t, first.Path, "status", "--porcelain")); got != "" {
		t.Fatalf("detached worktree status = %q, want clean", got)
	}
	if got := strings.TrimSpace(runGit(t, first.Path, "branch", "--show-current")); got != "" {
		t.Fatalf("detached worktree branch = %q, want detached HEAD", got)
	}

	second, err := backend.Create(context.Background(), Issue{Identifier: "DD-DETACHED-COMMIT"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if !second.Created {
		t.Fatal("second Create() Created = false, want true")
	}
	if got := strings.TrimSpace(runGit(t, second.Path, "branch", "--show-current")); got != "detent/dd-detached-commit" {
		t.Fatalf("worktree branch = %q, want detent/dd-detached-commit", got)
	}
	if _, err := os.Stat(filepath.Join(second.Path, "local-commit.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local commit file exists in fresh workspace, stat error = %v", err)
	}

	quarantineDir := filepath.Join(root, ".detent", "quarantine")
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		t.Fatalf("read quarantine dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("quarantine entries = %d, want 1", len(entries))
	}
	quarantinedPath := filepath.Join(quarantineDir, entries[0].Name())
	if got := readFile(t, filepath.Join(quarantinedPath, "local-commit.txt")); got != "keep\n" {
		t.Fatalf("quarantined local-commit.txt = %q, want keep", got)
	}
	if got := strings.TrimSpace(runGit(t, quarantinedPath, "branch", "--show-current")); got != "" {
		t.Fatalf("quarantined worktree branch = %q, want detached HEAD", got)
	}
	if got := logs.String(); !strings.Contains(got, "quarantined stale workspace") || !strings.Contains(got, quarantinedPath) {
		t.Fatalf("logs = %q, want quarantine report for %s", got, quarantinedPath)
	}
}

func TestLocalGitCreateRepairsWorkspacePreparationFailures(t *testing.T) {
	skipWindows(t)

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "quarantine releases canonical admin name",
			run: func(t *testing.T) {
				source := initSourceRepo(t)
				root := filepath.Join(t.TempDir(), "workspaces")
				backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
				if err != nil {
					t.Fatalf("NewLocalGit() error = %v", err)
				}

				issue := Issue{Identifier: "DD-ADMIN-NAME"}
				first, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("first Create() error = %v", err)
				}
				runGit(t, first.Path, "switch", "--detach", "HEAD")
				if err := os.WriteFile(filepath.Join(first.Path, "local-progress.txt"), []byte("keep\n"), 0o600); err != nil {
					t.Fatalf("write local progress: %v", err)
				}

				second, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("second Create() error = %v", err)
				}
				entries, err := os.ReadDir(filepath.Join(root, ".detent", "quarantine"))
				if err != nil || len(entries) != 1 {
					t.Fatalf("quarantine entries = %d, error = %v, want one", len(entries), err)
				}
				quarantinedPath := filepath.Join(root, ".detent", "quarantine", entries[0].Name())
				if got, want := filepath.Base(linkedWorktreeGitDir(t, second.Path)), filepath.Base(second.Path); got != want {
					t.Fatalf("replacement admin name = %q, want %q", got, want)
				}
				if got, want := filepath.Base(linkedWorktreeGitDir(t, quarantinedPath)), filepath.Base(quarantinedPath); got != want {
					t.Fatalf("quarantine admin name = %q, want %q", got, want)
				}
			},
		},
		{
			name: "dangling gitdir is recreated",
			run: func(t *testing.T) {
				source := initSourceRepo(t)
				root := filepath.Join(t.TempDir(), "workspaces")
				backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
				if err != nil {
					t.Fatalf("NewLocalGit() error = %v", err)
				}

				issue := Issue{Identifier: "DD-DANGLING-GITDIR"}
				first, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("first Create() error = %v", err)
				}
				adminDir := linkedWorktreeGitDir(t, first.Path)
				orphanedAdminDir := adminDir + "-orphaned"
				if err := os.Rename(adminDir, orphanedAdminDir); err != nil {
					t.Fatalf("orphan admin directory: %v", err)
				}

				second, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("second Create() error = %v", err)
				}
				if !second.Created {
					t.Fatal("second Create() Created = false, want true")
				}
				if got, want := filepath.Base(linkedWorktreeGitDir(t, second.Path)), filepath.Base(second.Path); got != want {
					t.Fatalf("replacement admin name = %q, want %q", got, want)
				}
				if _, err := os.Stat(orphanedAdminDir); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("orphaned admin directory stat error = %v, want absent", err)
				}
			},
		},
		{
			name: "failed clean removal leaves recreatable path",
			run: func(t *testing.T) {
				source := initSourceRepo(t)
				root := filepath.Join(t.TempDir(), "workspaces")
				backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
				if err != nil {
					t.Fatalf("NewLocalGit() error = %v", err)
				}

				issue := Issue{Identifier: "DD-PARTIAL-REMOVE"}
				first, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("first Create() error = %v", err)
				}
				runGit(t, first.Path, "branch", "detent/other-partial-remove", "HEAD")
				runGit(t, first.Path, "switch", "detent/other-partial-remove")
				lockedDir := filepath.Join(first.Path, "locked")
				if err := os.MkdirAll(lockedDir, 0o700); err != nil {
					t.Fatalf("create locked directory: %v", err)
				}
				if err := os.WriteFile(filepath.Join(lockedDir, "tracked.txt"), []byte("tracked\n"), 0o600); err != nil {
					t.Fatalf("write tracked file: %v", err)
				}
				runGit(t, first.Path, "add", "locked/tracked.txt")
				runGit(t, first.Path, "commit", "-m", "add locked file")
				if err := os.Chmod(lockedDir, 0o500); err != nil {
					t.Fatalf("make tracked directory read-only: %v", err)
				}
				t.Cleanup(func() { restoreWritableTree(t, first.Path) })

				second, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatalf("second Create() error = %v", err)
				}
				if !second.Created {
					t.Fatal("second Create() Created = false, want true")
				}
				if _, err := os.Stat(filepath.Join(second.Path, "locked")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stale locked directory stat error = %v, want absent", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestLocalGitQuarantineWorkspaceCount(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	parent := filepath.Join(root, ".detent", "quarantine")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatalf("create quarantine directory: %v", err)
	}
	base := "detent-acme_widgets_42"
	stamp := "20260820T140000.000000000Z"
	for _, name := range []string{
		base + "-" + stamp,
		base + "-" + stamp + "-1",
		base + "-" + stamp + "-2",
		base + "-malformed",
		base + "-other-" + stamp,
		base + "0-" + stamp,
	} {
		if err := os.MkdirAll(filepath.Join(parent, name), 0o700); err != nil {
			t.Fatalf("create quarantine entry %q: %v", name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(parent, base+"-20260820T150000.000000000Z"), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create quarantine file: %v", err)
	}
	backend := &LocalGit{root: root}

	got, err := backend.quarantineWorkspaceCount(filepath.Join(root, base))
	if err != nil {
		t.Fatalf("quarantineWorkspaceCount() error = %v", err)
	}
	if got != 3 {
		t.Fatalf("quarantineWorkspaceCount() = %d, want 3", got)
	}
}

func TestLocalGitBeforeAndAfterRunHooks(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "run-hooks.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			BeforeRun: "printf 'before:%s:%s\n' \"$PWD\" \"$WORKSPACE_KEY\" >> " + shellQuote(tracePath),
			AfterRun:  "printf 'after:%s:%s\n' \"$PWD\" \"$WORKSPACE_KEY\" >> " + shellQuote(tracePath),
			Timeout:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-RUN"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := backend.BeforeRun(context.Background(), info, Issue{Identifier: "DD-RUN"}); err != nil {
		t.Fatalf("BeforeRun() error = %v", err)
	}
	backend.AfterRun(context.Background(), info, Issue{Identifier: "DD-RUN"})

	want := "before:" + info.Path + ":DD-RUN\nafter:" + info.Path + ":DD-RUN\n"
	if got := readFile(t, tracePath); got != want {
		t.Fatalf("hook trace = %q, want %q", got, want)
	}
}

func TestLocalGitHookFailureSurfaces(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	hookCommand := "printf 'out\\n'; printf 'err\\n' >&2; exit 17"

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			AfterCreate: hookCommand,
			Timeout:     time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	_, err = backend.Create(context.Background(), Issue{Identifier: "DD-FAIL"})
	if err == nil {
		t.Fatal("Create() error = nil, want hook error")
	}
	var hookErr *HookError
	if !errors.As(err, &hookErr) {
		t.Fatalf("Create() error = %T, want *HookError", err)
	}
	if hookErr.Hook != "after_create" {
		t.Fatalf("HookError.Hook = %q, want after_create", hookErr.Hook)
	}
	if hookErr.ExitCode != 17 {
		t.Fatalf("HookError.ExitCode = %d, want 17", hookErr.ExitCode)
	}
	for _, want := range []string{"out", "err"} {
		if !strings.Contains(hookErr.Output, want) {
			t.Fatalf("HookError.Output = %q, want %q", hookErr.Output, want)
		}
	}
	if hookErr.Command != hookCommand {
		t.Fatalf("HookError.Command = %q, want hook command", hookErr.Command)
	}
	if filepath.Base(hookErr.Dir) != "DD-FAIL" {
		t.Fatalf("HookError.Dir = %q, want DD-FAIL workspace", hookErr.Dir)
	}
	if hookErr.LogPath == "" {
		t.Fatal("HookError.LogPath is empty")
	}
	wantLogDir := filepath.Join(backend.(*LocalGit).root, ".detent", "hook-logs", "DD-FAIL")
	if !strings.HasPrefix(hookErr.LogPath, wantLogDir) {
		t.Fatalf("HookError.LogPath = %q, want under root hook logs", hookErr.LogPath)
	}
	errorDetail := err.Error()
	for _, want := range []string{
		fmt.Sprintf("command %q", hookCommand),
		"working directory",
		"exit status 17",
		"hook log",
		"output (last",
		"out",
		"err",
	} {
		if !strings.Contains(errorDetail, want) {
			t.Fatalf("Create() error = %q, want %q", errorDetail, want)
		}
	}
	logContent := readFile(t, hookErr.LogPath)
	for _, want := range []string{
		"hook: after_create\n",
		"command: " + hookCommand + "\n",
		"exit_status: 17\n",
		"output:\nout\nerr\n",
	} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("hook log = %q, want %q", logContent, want)
		}
	}
	if _, statErr := os.Stat(filepath.Join(root, "DD-FAIL")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed after_create workspace exists, stat error = %v", statErr)
	}
	quarantinedPath := singleQuarantinedWorkspace(t, root)
	if got := readFile(t, filepath.Join(quarantinedPath, "README.md")); got != "source repo\n" {
		t.Fatalf("quarantined README.md = %q, want source repo", got)
	}
	if got := strings.TrimSpace(runGit(t, quarantinedPath, "branch", "--show-current")); got != "" {
		t.Fatalf("quarantined worktree branch = %q, want detached HEAD", got)
	}
	if got := runGit(t, source, "worktree", "list", "--porcelain"); !strings.Contains(got, quarantinedPath) {
		t.Fatalf("git worktree list does not contain quarantined path:\n%s", got)
	}

	retryBackend, retryErr := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
	if retryErr != nil {
		t.Fatalf("NewLocalGit() retry error = %v", retryErr)
	}
	retried, retryErr := retryBackend.Create(context.Background(), Issue{Identifier: "DD-FAIL"})
	if retryErr != nil {
		t.Fatalf("retry Create() error = %v", retryErr)
	}
	if !retried.Created {
		t.Fatal("retry Create() Created = false, want true")
	}
}

func TestLocalGitPreserveFailedWorkspaceReleasesBranch(t *testing.T) {
	skipWindows(t)

	tests := []struct {
		name       string
		identifier string
		ctx        func() context.Context
		prepare    func(*testing.T, string, string)
		wantStatus string
	}{
		{
			name:       "canceled creation context",
			identifier: "DD-CANCELED-PRESERVE",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
		{
			name:       "unmerged index",
			identifier: "DD-UNMERGED-PRESERVE",
			ctx:        context.Background,
			prepare: func(t *testing.T, source string, workspace string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("workspace\n"), 0o600); err != nil {
					t.Fatalf("write workspace README.md: %v", err)
				}
				runGit(t, workspace, "add", "README.md")
				runGit(t, workspace, "commit", "-m", "workspace change")
				if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("source\n"), 0o600); err != nil {
					t.Fatalf("write source README.md: %v", err)
				}
				runGit(t, source, "add", "README.md")
				runGit(t, source, "commit", "-m", "source change")

				cmd := exec.CommandContext(t.Context(), "git", "-C", workspace, "merge", "main")
				output, err := cmd.CombinedOutput()
				if err == nil {
					t.Fatalf("git merge succeeded, want conflict:\n%s", output)
				}
			},
			wantStatus: "UU README.md",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := initSourceRepo(t)
			root := filepath.Join(t.TempDir(), "workspaces")
			backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatalf("NewLocalGit() error = %v", err)
			}
			info, err := backend.Create(context.Background(), Issue{Identifier: tt.identifier})
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if tt.prepare != nil {
				tt.prepare(t, source, info.Path)
			}

			cause := errors.New("synthetic workspace failure")
			err = backend.preserveFailedWorkspace(tt.ctx(), info.Path, cause)
			if !errors.Is(err, cause) {
				t.Fatalf("preserveFailedWorkspace() error = %v, want cause", err)
			}
			if _, statErr := os.Stat(info.Path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed workspace remains at original path, stat error = %v", statErr)
			}

			quarantinedPath := singleQuarantinedWorkspace(t, root)
			if got := runGit(t, source, "worktree", "list", "--porcelain"); !strings.Contains(got, quarantinedPath) || strings.Contains(got, info.Path+"\n") {
				t.Fatalf("git worktree list does not register only quarantined path:\n%s", got)
			}
			if got := strings.TrimSpace(runGit(t, quarantinedPath, "branch", "--show-current")); got != "" {
				t.Fatalf("quarantined worktree branch = %q, want detached HEAD", got)
			}
			if tt.wantStatus != "" {
				if got := runGit(t, quarantinedPath, "status", "--porcelain"); !strings.Contains(got, tt.wantStatus) {
					t.Fatalf("quarantined worktree status = %q, want %q", got, tt.wantStatus)
				}
			}

			retried, retryErr := backend.Create(context.Background(), Issue{Identifier: tt.identifier})
			if retryErr != nil {
				t.Fatalf("retry Create() error = %v", retryErr)
			}
			if !retried.Created {
				t.Fatal("retry Create() Created = false, want true")
			}
		})
	}
}

func TestLocalGitPreserveFailedWorkspaceSurfacesBranchReleaseFailure(t *testing.T) {
	skipWindows(t)

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-DETACH-FAILURE"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	installGitUpdateRefFailureWrapper(t)

	cause := errors.New("synthetic workspace failure")
	err = backend.preserveFailedWorkspace(context.Background(), info.Path, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("preserveFailedWorkspace() error = %v, want cause", err)
	}
	for _, want := range []string{"workspace quarantined at", "failed to release its branch", "synthetic update-ref failure"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("preserveFailedWorkspace() error = %q, want %q", err, want)
		}
	}
	if _, statErr := os.Stat(info.Path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed workspace remains at original path, stat error = %v", statErr)
	}

	quarantinedPath := singleQuarantinedWorkspace(t, root)
	if !strings.Contains(err.Error(), quarantinedPath) {
		t.Fatalf("preserveFailedWorkspace() error = %q, want quarantine path %q", err, quarantinedPath)
	}
	if got := strings.TrimSpace(runGit(t, quarantinedPath, "branch", "--show-current")); got != info.Branch {
		t.Fatalf("quarantined worktree branch = %q, want %q after release failure", got, info.Branch)
	}
}

func TestLocalGitCreateQuarantinesFailedCreationState(t *testing.T) {
	skipWindows(t)

	tests := []struct {
		name           string
		mode           string
		identifier     string
		wantError      []string
		wantStandalone bool
	}{
		{
			name:       "partial worktree add failure",
			mode:       "partial_failure",
			identifier: "DD-PARTIAL-CREATE",
			wantError: []string{
				"synthetic worktree add failure",
				"workspace quarantined at",
			},
		},
		{
			name:       "successful add yields standalone repository",
			mode:       "standalone_after_add",
			identifier: "DD-STANDALONE",
			wantError: []string{
				"workspace worktree invariant failed",
				"registered with source: false",
				"workspace quarantined at",
			},
			wantStandalone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := initSourceRepo(t)
			root := filepath.Join(t.TempDir(), "workspaces")
			target := filepath.Join(root, tt.identifier)
			tracePath := filepath.Join(t.TempDir(), "after-create.trace")
			installGitCreationWrapper(t, tt.mode, source, target)

			backend, err := NewLocalGit(LocalGitOptions{
				Root:       root,
				SourceRoot: source,
				AutoBranch: true,
				Hooks: Hooks{
					AfterCreate: "printf 'ran\\n' > " + shellQuote(tracePath),
					Timeout:     time.Second,
				},
			})
			if err != nil {
				t.Fatalf("NewLocalGit() error = %v", err)
			}

			_, err = backend.Create(context.Background(), Issue{Identifier: tt.identifier})
			if err == nil {
				t.Fatal("Create() error = nil, want creation failure")
			}
			if tt.wantStandalone && !errors.Is(err, ErrWorktreeInvariant) {
				t.Errorf("Create() error = %v, want ErrWorktreeInvariant", err)
			}
			for _, want := range tt.wantError {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Create() error = %q, want %q", err, want)
				}
			}
			if tt.wantStandalone {
				for _, want := range []string{filepath.Join(target, ".git"), filepath.Join(source, ".git")} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Create() error = %q, want common dir %q", err, want)
					}
				}
			}
			if _, statErr := os.Stat(tracePath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("after_create hook ran, stat error = %v", statErr)
			}
			if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed workspace remains at original path, stat error = %v", statErr)
			}

			quarantinedPath := singleQuarantinedWorkspace(t, root)
			if got := readFile(t, filepath.Join(quarantinedPath, "forensics.txt")); got != tt.mode+"\n" {
				t.Fatalf("quarantined forensics.txt = %q, want %q", got, tt.mode)
			}
			gitInfo, statErr := os.Stat(filepath.Join(quarantinedPath, ".git"))
			switch {
			case tt.wantStandalone && statErr != nil:
				t.Fatalf("stat quarantined .git: %v", statErr)
			case tt.wantStandalone && !gitInfo.IsDir():
				t.Fatalf("quarantined .git mode = %v, want directory", gitInfo.Mode())
			case !tt.wantStandalone && !errors.Is(statErr, os.ErrNotExist):
				t.Fatalf("partial quarantine .git stat error = %v, want absent", statErr)
			}
		})
	}
}

func TestLocalGitBranchHeldByWorktree(t *testing.T) {
	skipWindows(t)

	tests := []struct {
		name               string
		releaseBeforeRetry bool
		wantCreated        bool
	}{
		{name: "held branch is classified"},
		{name: "released branch creates workspace", releaseBeforeRetry: true, wantCreated: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := initSourceRepo(t)
			root := filepath.Join(t.TempDir(), "workspaces")
			holder := filepath.Join(t.TempDir(), "review-pr")
			branch := "detent/reviewer-held"
			runGit(t, source, "worktree", "add", "-b", branch, holder)

			backend, err := NewLocalGit(LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatalf("NewLocalGit() error = %v", err)
			}
			issue := Issue{Identifier: "DD-HELD", BranchName: branch}

			_, err = backend.Create(t.Context(), issue)
			var heldErr *BranchHeldError
			if !errors.As(err, &heldErr) {
				t.Fatalf("Create() error = %v, want BranchHeldError", err)
			}
			if heldErr.Branch != branch {
				t.Fatalf("BranchHeldError branch = %q, want %q", heldErr.Branch, branch)
			}
			requireSameFile(t, heldErr.Path, holder)
			hold, held, err := backend.BranchHold(t.Context(), issue)
			if err != nil || !held || hold.Branch != branch {
				t.Fatalf("BranchHold() = %#v, %v, %v", hold, held, err)
			}
			requireSameFile(t, hold.Path, holder)

			if !tt.releaseBeforeRetry {
				return
			}
			runGit(t, source, "worktree", "remove", holder)
			if hold, held, err = backend.BranchHold(t.Context(), issue); err != nil || held {
				t.Fatalf("BranchHold() after release = %#v, %v, %v", hold, held, err)
			}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatalf("Create() after release error = %v", err)
			}
			if info.Created != tt.wantCreated || info.Branch != branch {
				t.Fatalf("Create() after release = %#v, want Created=%v branch=%q", info, tt.wantCreated, branch)
			}
		})
	}
}

func requireSameFile(t *testing.T, got string, want string) {
	t.Helper()
	gotInfo, err := os.Stat(got)
	if err != nil {
		t.Fatalf("stat %q: %v", got, err)
	}
	wantInfo, err := os.Stat(want)
	if err != nil {
		t.Fatalf("stat %q: %v", want, err)
	}
	if !os.SameFile(gotInfo, wantInfo) {
		t.Fatalf("path %q does not identify %q", got, want)
	}
}

func TestHookErrorErrorIncludesBoundedOutputTail(t *testing.T) {
	t.Parallel()

	output := "prefix-" + strings.Repeat("x", hookOutputTailBytes) + "-tail"
	err := (&HookError{
		Hook:     "after_create",
		Command:  "bootstrap",
		Dir:      "/workspaces/DD-TAIL",
		ExitCode: 1,
		LogPath:  "/workspaces/.detent/hook-logs/DD-TAIL/hook.log",
		Output:   output,
		Err:      errors.New("exit status 1"),
	}).Error()

	for _, want := range []string{
		"command \"bootstrap\"",
		"working directory \"/workspaces/DD-TAIL\"",
		"exit status 1",
		"hook log \"/workspaces/.detent/hook-logs/DD-TAIL/hook.log\"",
		"output (last 16 KiB)",
		"truncated to last 16 KiB",
		"-tail",
	} {
		if !strings.Contains(err, want) {
			t.Fatalf("HookError.Error() = %q, want %q", err, want)
		}
	}
	if strings.Contains(err, "prefix-") {
		t.Fatalf("HookError.Error() includes truncated prefix: %q", err)
	}
}

func TestLocalGitCleanupRemovesOnlyTargetWorktree(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	publishCleanupSource(t, source)
	root := filepath.Join(t.TempDir(), "workspaces")
	tracePath := filepath.Join(t.TempDir(), "cleanup.trace")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			BeforeRemove: "printf '%s\n' \"$WORKSPACE_KEY\" >> " + shellQuote(tracePath),
			Timeout:      time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	target, err := backend.Create(context.Background(), Issue{Identifier: "DD-CLEAN"})
	if err != nil {
		t.Fatalf("target Create() error = %v", err)
	}
	other, err := backend.Create(context.Background(), Issue{Identifier: "DD-KEEP"})
	if err != nil {
		t.Fatalf("other Create() error = %v", err)
	}

	if err := backend.Cleanup(context.Background(), "DD-CLEAN"); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	if _, err := os.Stat(target.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target workspace exists after cleanup, stat error = %v", err)
	}
	if _, err := os.Stat(other.Path); err != nil {
		t.Fatalf("other workspace stat error = %v", err)
	}
	if got := strings.TrimSpace(readFile(t, tracePath)); got != "DD-CLEAN" {
		t.Fatalf("before_remove trace = %q, want DD-CLEAN", got)
	}
	if got := runGit(t, source, "worktree", "list", "--porcelain"); strings.Contains(got, target.Path) {
		t.Fatalf("git worktree list still contains removed path:\n%s", got)
	}
}

func TestLocalGitCleanupRemediatesGeneratedCachePermissions(t *testing.T) {
	skipWindows(t)
	t.Setenv("GIT_CEILING_DIRECTORIES", os.TempDir())

	enclosing := initSourceRepo(t)
	source := filepath.Join(enclosing, "source")
	initSourceRepoAt(t, source)
	publishCleanupSource(t, source)
	root := filepath.Join(enclosing, "workspaces")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	info, err := backend.Create(context.Background(), Issue{Identifier: "DD-CACHE-PERM"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	t.Cleanup(func() {
		restoreWritableTree(t, info.Path)
	})

	if err := ensureGitInfoExcludes(t.Context(), info.Path, []string{"tmp/"}); err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(info.Path, "tmp", "_validation-cache", "go-mod", "modernc.org", "libc@v1.73.4")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("mkdir cache dir: %v", err)
	}
	cacheFile := filepath.Join(cacheDir, "libc_amd64.go")
	if err := os.WriteFile(cacheFile, []byte("package libc\n"), 0o600); err != nil {
		t.Fatalf("write cache file: %v", err)
	}
	if err := os.Chmod(cacheFile, 0o444); err != nil {
		t.Fatalf("chmod cache file: %v", err)
	}
	if err := os.Chmod(cacheDir, 0o555); err != nil {
		t.Fatalf("chmod cache dir: %v", err)
	}

	if err := backend.Cleanup(context.Background(), "DD-CACHE-PERM"); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if _, err := os.Stat(info.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace exists after cleanup, stat error = %v", err)
	}
	if branchExists(t, source, "detent/dd-cache-perm") {
		t.Fatal("detent/dd-cache-perm branch still exists after cleanup")
	}
}

func TestLocalGitRepositoryDiscoveryStaysWithinCandidate(t *testing.T) {
	t.Parallel()

	enclosing := initSourceRepo(t)
	source := filepath.Join(enclosing, "source")
	initSourceRepoAt(t, source)
	sourceSubdir := filepath.Join(source, "subdir")
	if err := os.MkdirAll(sourceSubdir, 0o700); err != nil {
		t.Fatalf("mkdir source subdirectory: %v", err)
	}
	root := filepath.Join(enclosing, "workspaces")

	backend, err := NewLocalGit(LocalGitOptions{
		Root:       root,
		SourceRoot: sourceSubdir,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewLocalGit() error = %v", err)
	}
	managed, err := backend.Create(context.Background(), Issue{Identifier: "DD-MANAGED"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	foreign := filepath.Join(root, "DD-FOREIGN")
	initSourceRepoAt(t, foreign)
	unmanaged := filepath.Join(root, "DD-UNMANAGED")
	if err := os.MkdirAll(unmanaged, 0o700); err != nil {
		t.Fatalf("mkdir unmanaged workspace: %v", err)
	}

	tests := []struct {
		name       string
		path       string
		wantGit    bool
		wantSource bool
	}{
		{name: "managed worktree", path: managed.Path, wantGit: true, wantSource: true},
		{name: "foreign repository", path: foreign, wantGit: true, wantSource: false},
		{name: "unmanaged nested directory", path: unmanaged, wantGit: false, wantSource: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := backend.isGitWorkspace(context.Background(), test.path); got != test.wantGit {
				t.Errorf("isGitWorkspace() = %t, want %t", got, test.wantGit)
			}
			if got := backend.isSourceWorktree(context.Background(), test.path); got != test.wantSource {
				t.Errorf("isSourceWorktree() = %t, want %t", got, test.wantSource)
			}
		})
	}
}

func TestWorkerScratchLifecycleRemediatesGeneratedCachePermissions(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	workspacePath := t.TempDir()
	scratchPath, err := PrepareWorkerScratch(context.Background(), workspacePath)
	if err != nil {
		t.Fatalf("PrepareWorkerScratch() error = %v", err)
	}
	t.Cleanup(func() {
		restoreWritableTree(t, scratchPath)
	})
	canonicalWorkspace := mustCanonicalExistingPath(t, workspacePath)
	if !strings.HasPrefix(scratchPath, canonicalWorkspace+string(filepath.Separator)) {
		t.Fatalf("scratch path = %q, want path under %q", scratchPath, canonicalWorkspace)
	}

	cacheDir := filepath.Join(scratchPath, "go-mod", "modernc.org", "libc@v1.73.4")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("mkdir cache dir: %v", err)
	}
	cacheFile := filepath.Join(cacheDir, "libc_amd64.go")
	if err := os.WriteFile(cacheFile, []byte("package libc\n"), 0o444); err != nil {
		t.Fatalf("write cache file: %v", err)
	}
	if err := os.Chmod(cacheDir, 0o555); err != nil {
		t.Fatalf("chmod cache dir: %v", err)
	}

	if err := CleanupWorkerScratch(workspacePath); err != nil {
		t.Fatalf("CleanupWorkerScratch() error = %v", err)
	}
	if _, err := os.Stat(scratchPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker scratch exists after cleanup, stat error = %v", err)
	}
}

func TestCleanupOwnedPathConfinesRemovalToRegisteredRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       func(root string, outside string) string
		wantErr    bool
		wantExists bool
	}{
		{
			name: "registered descendant",
			path: func(root string, _ string) string {
				return filepath.Join(root, "run-2011")
			},
		},
		{
			name: "cleanup root",
			path: func(root string, _ string) string {
				return root
			},
			wantErr:    true,
			wantExists: true,
		},
		{
			name: "outside root",
			path: func(_ string, outside string) string {
				return outside
			},
			wantErr:    true,
			wantExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			outside := t.TempDir()
			path := tt.path(root, outside)
			if path != root && path != outside {
				if err := os.MkdirAll(filepath.Join(path, "cache"), 0o700); err != nil {
					t.Fatalf("create cleanup path: %v", err)
				}
			}
			err := CleanupOwnedPath(root, path)
			if got := err != nil; got != tt.wantErr {
				t.Fatalf("CleanupOwnedPath() error = %v, want error %v", err, tt.wantErr)
			}
			_, statErr := os.Stat(path)
			if got := statErr == nil; got != tt.wantExists {
				t.Fatalf("cleanup path exists = %v, want %v, stat error = %v", got, tt.wantExists, statErr)
			}
		})
	}
}

func TestPrepareWorkerScratchInstallsGitExcludeBeforeUse(t *testing.T) {
	t.Parallel()

	workspacePath := initSourceRepo(t)
	scratchPath, err := PrepareWorkerScratch(context.Background(), workspacePath)
	if err != nil {
		t.Fatalf("PrepareWorkerScratch() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(scratchPath, "cache"), []byte("temporary"), 0o600); err != nil {
		t.Fatalf("write worker scratch: %v", err)
	}

	status := runCommand(t, workspacePath, "git", "status", "--short", "--untracked-files=all")
	if strings.Contains(status, ".detent/tmp") {
		t.Fatalf("git status includes worker scratch:\n%s", status)
	}
	excludePath := strings.TrimSpace(runCommand(t, workspacePath, "git", "rev-parse", "--git-path", "info/exclude"))
	if !filepath.IsAbs(excludePath) {
		excludePath = filepath.Join(workspacePath, excludePath)
	}
	exclude, err := os.ReadFile(excludePath)
	if err != nil {
		t.Fatalf("read git exclude: %v", err)
	}
	if !strings.Contains(string(exclude), ".detent/tmp/") {
		t.Fatalf("git exclude = %q, want worker scratch pattern", exclude)
	}
}

func TestLocalGitCleanupRejectsForeignGitRepoWithoutBeforeRemove(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	foreign := filepath.Join(root, "DD-FOREIGN")
	tracePath := filepath.Join(t.TempDir(), "cleanup.trace")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatalf("mkdir foreign repo: %v", err)
	}
	runCommand(t, foreign, "git", "init", "-b", "main")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
		Hooks: Hooks{
			BeforeRemove: "printf 'ran\n' > " + shellQuote(tracePath),
			Timeout:      time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	err = backend.Cleanup(context.Background(), "DD-FOREIGN")
	if err == nil {
		t.Fatal("Cleanup() error = nil, want foreign repo error")
	}
	if !strings.Contains(err.Error(), "not managed by source") {
		t.Fatalf("Cleanup() error = %v, want not managed by source", err)
	}
	if _, err := os.Stat(tracePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("before_remove hook ran, stat error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(foreign, ".git")); err != nil {
		t.Fatalf("foreign repo was removed, stat error = %v", err)
	}
}

func TestLocalGitRejectsSymlinkEscape(t *testing.T) {
	t.Parallel()
	skipWindows(t)

	source := initSourceRepo(t)
	testRoot := t.TempDir()
	root := filepath.Join(testRoot, "workspaces")
	outside := filepath.Join(testRoot, "outside")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "DD-SYM")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	_, err = backend.Create(context.Background(), Issue{Identifier: "DD-SYM"})
	if !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("Create() error = %v, want ErrUnsafePath", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "DD-SYM")); err != nil {
		t.Fatalf("symlink stat error = %v", err)
	}
}

func TestLocalGitRejectsExistingGitRepoFromDifferentSource(t *testing.T) {
	t.Parallel()

	source := initSourceRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	foreign := filepath.Join(root, "DD-FOREIGN")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatalf("mkdir foreign repo: %v", err)
	}
	runCommand(t, foreign, "git", "init", "-b", "main")

	backend, err := NewBackend(KindLocalGit, LocalGitOptions{
		Root:       root,
		SourceRoot: source,
		AutoBranch: true,
	})
	if err != nil {
		t.Fatalf("NewBackend() error = %v", err)
	}

	_, err = backend.Create(context.Background(), Issue{Identifier: "DD-FOREIGN"})
	if err == nil {
		t.Fatal("Create() error = nil, want foreign repo error")
	}
	if !strings.Contains(err.Error(), "not managed by source") {
		t.Fatalf("Create() error = %v, want not managed by source", err)
	}
	if _, err := os.Stat(filepath.Join(foreign, ".git")); err != nil {
		t.Fatalf("foreign repo was removed, stat error = %v", err)
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s: %v", path, err)
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", path)
		}
	}
}

func waitForError(t *testing.T, result <-chan error, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func initSourceRepo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	initSourceRepoAt(t, dir)
	return dir
}

func initSourceRepoAt(t *testing.T, dir string) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir source repo: %v", err)
	}
	runCommand(t, dir, "git", "init", "-b", "main")
	runGit(t, dir, "config", "core.autocrlf", "false")
	runGit(t, dir, "config", "user.name", "Test User")
	runGit(t, dir, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("source repo\n"), 0o600); err != nil {
		t.Fatalf("write README.md: %v", err)
	}
	runGit(t, dir, "add", "README.md")
	runGit(t, dir, "commit", "-m", "initial")
}

func initBareRemote(t *testing.T) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "origin.git")
	runCommand(t, t.TempDir(), "git", "init", "--bare", "-b", "main", dir)
	return dir
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return runCommand(t, dir, "git", args...)
}

func linkedWorktreeGitDir(t *testing.T, workspacePath string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(workspacePath, ".git"))
	if err != nil {
		t.Fatalf("read linked worktree .git file: %v", err)
	}
	gitDir, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "gitdir: ")
	if !ok || strings.TrimSpace(gitDir) == "" {
		t.Fatalf("linked worktree .git file = %q, want gitdir path", data)
	}
	return mustCanonicalExistingPath(t, gitDir)
}

func branchExists(t *testing.T, dir string, branch string) bool {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "git", "-C", dir, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false
	}
	t.Fatalf("git show-ref failed: %v", err)
	return false
}

func runCommand(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, output)
	}
	return string(output)
}

func installGitCreationWrapper(t *testing.T, mode string, source string, target string) {
	t.Helper()

	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("locate git: %v", err)
	}
	wrapperDir := t.TempDir()
	wrapperPath := filepath.Join(wrapperDir, "git")
	var action string
	switch mode {
	case "partial_failure":
		action = "mkdir -p " + shellQuote(target) + "\n" +
			"printf 'partial_failure\\n' > " + shellQuote(filepath.Join(target, "forensics.txt")) + "\n" +
			"printf 'synthetic worktree add failure\\n' >&2\n" +
			"exit 23"
	case "standalone_after_add":
		action = shellQuote(realGit) + " \"$@\"\n" +
			"status=$?\n" +
			"if [ \"$status\" -ne 0 ]; then exit \"$status\"; fi\n" +
			shellQuote(realGit) + " -C " + shellQuote(source) + " worktree remove --force " + shellQuote(target) + "\n" +
			shellQuote(realGit) + " clone --quiet " + shellQuote(source) + " " + shellQuote(target) + "\n" +
			"printf 'standalone_after_add\\n' > " + shellQuote(filepath.Join(target, "forensics.txt")) + "\n" +
			"exit 0"
	default:
		t.Fatalf("unknown git wrapper mode %q", mode)
	}
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"-C\" ] && [ \"$3\" = \"worktree\" ] && [ \"$4\" = \"add\" ]; then\n" +
		action + "\n" +
		"fi\n" +
		"exec " + shellQuote(realGit) + " \"$@\"\n"
	if err := os.WriteFile(wrapperPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write git wrapper: %v", err)
	}
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func installGitUpdateRefFailureWrapper(t *testing.T) {
	t.Helper()

	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("locate git: %v", err)
	}
	wrapperDir := t.TempDir()
	wrapperPath := filepath.Join(wrapperDir, "git")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"-C\" ] && [ \"$3\" = \"update-ref\" ] && [ \"$4\" = \"--no-deref\" ] && [ \"$5\" = \"HEAD\" ]; then\n" +
		"printf 'synthetic update-ref failure\\n' >&2\n" +
		"exit 29\n" +
		"fi\n" +
		"exec " + shellQuote(realGit) + " \"$@\"\n"
	if err := os.WriteFile(wrapperPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write git wrapper: %v", err)
	}
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func singleQuarantinedWorkspace(t *testing.T, root string) string {
	t.Helper()

	parent := filepath.Join(root, ".detent", "quarantine")
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read quarantine directory: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("quarantine entries = %d, want 1", len(entries))
	}
	return filepath.Join(parent, entries[0].Name())
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func restoreWritableTree(t *testing.T, path string) {
	t.Helper()

	err := filepath.WalkDir(path, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return os.Chmod(path, 0o600)
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore writable tree: %v", err)
	}
}

func mustCanonicalExistingPath(t *testing.T, path string) string {
	t.Helper()

	canonical, err := canonicalExistingPath(path)
	if err != nil {
		t.Fatalf("canonicalExistingPath(%q) error = %v", path, err)
	}
	return canonical
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func skipWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("requires a UNIX test environment")
	}
}

func TestLocalGitPrepareMergeValidatesResolvedHead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		before     string
		gate       string
		wantError  string
		wantPushed bool
	}{
		{name: "resolved committed head", wantPushed: true},
		{name: "already pushed head", before: "push"},
		{name: "gate failure", gate: "git detent-invalid-gate", wantError: "gate failed"},
		{name: "dirty resolution", before: "dirty", wantError: "not source-clean"},
		{name: "stale target", before: "base", wantError: "does not contain"},
		{name: "replaced remote", before: "remote", wantError: "remote branch changed before"},
		{name: "gate changes head", gate: "git commit --allow-empty -m changed", wantError: "changed during"},
		{name: "gate dirties source", gate: "git rm README.md", wantError: "not source-clean"},
		{name: "gate changes branch", gate: "git checkout -b other", wantError: "owned workspace branch"},
		{name: "gate changes target", gate: "base", wantError: "does not contain"},
		{name: "gate replaces remote", gate: "remote", wantError: "changed during"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			remote := initBareRemote(t)
			runGit(t, source, "remote", "add", "origin", remote)
			runGit(t, source, "push", "-u", "origin", "main")
			backend, err := NewBackend(KindLocalGit, LocalGitOptions{
				Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{Identifier: "DD-RESOLVED"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			runGit(t, info.Path, "push", "origin", "HEAD:"+info.Branch)
			expectedRemote := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD"))
			runGit(t, info.Path, "commit", "--allow-empty", "-m", "resolved")
			head := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD"))
			mutateRemote := func(branch string) {
				runGit(t, source, "commit", "--allow-empty", "-m", "external")
				runGit(t, source, "push", "origin", "HEAD:"+branch)
			}
			switch tt.before {
			case "push":
				runGit(t, info.Path, "push", "origin", "HEAD:"+info.Branch)
			case "dirty":
				runGit(t, info.Path, "rm", "README.md")
			case "base":
				mutateRemote("main")
			case "remote":
				mutateRemote(info.Branch)
			}
			remoteBefore := strings.TrimSpace(runGit(t, source, "ls-remote", "origin", "refs/heads/"+info.Branch))
			command := "git config detent.validation passed"
			switch tt.gate {
			case "base", "remote":
				branch := "main"
				if tt.gate == "remote" {
					branch = info.Branch
				}
				relativeSource, err := filepath.Rel(info.Path, source)
				if err != nil {
					t.Fatal(err)
				}
				relativeSource = filepath.ToSlash(relativeSource)
				command += " && git -C " + relativeSource + " commit --allow-empty -m external && git -C " + relativeSource + " push origin HEAD:" + branch
			default:
				if tt.gate != "" {
					command += " && " + tt.gate
				}
			}
			result, err := backend.(MergePreparer).PrepareMerge(t.Context(), info, issue, MergePrepareOptions{
				TargetBranch: "main", VerifyResolution: true, ValidationCommand: command, ExpectedRemoteHead: expectedRemote,
			})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("PrepareMerge() = %#v, %v; want error containing %q", result, err, tt.wantError)
				}
				if tt.gate != "remote" {
					if got := strings.TrimSpace(runGit(t, source, "ls-remote", "origin", "refs/heads/"+info.Branch)); got != remoteBefore {
						t.Fatalf("remote changed on failed validation: %q, want %q", got, remoteBefore)
					}
				}
				return
			}
			if err != nil || result.Status != MergePrepareStatusClean || result.HeadSHA != head || result.HeadChanged != tt.wantPushed {
				t.Fatalf("PrepareMerge() = %#v, %v; want validated head %s, pushed %t", result, err, head, tt.wantPushed)
			}
			if got := strings.TrimSpace(runGit(t, info.Path, "config", "--get", "detent.validation")); got != "passed" {
				t.Fatalf("local gate evidence = %q", got)
			}
			if got := strings.Fields(runGit(t, source, "ls-remote", "origin", "refs/heads/"+info.Branch))[0]; got != head {
				t.Fatalf("remote head = %s, want %s", got, head)
			}
		})
	}
}
