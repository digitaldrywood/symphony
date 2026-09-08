package workspace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalProgressBase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		issue   Issue
		remote  bool
		wantErr bool
	}{
		{name: "no upstream"},
		{name: "named local base", issue: Issue{ProgressBaseRef: "main"}},
		{name: "named remote base", issue: Issue{ProgressBaseRef: "main"}, remote: true},
		{name: "cached default remote", remote: true},
		{name: "explicit base", issue: Issue{BaseRef: "main"}},
		{name: "missing named base", issue: Issue{ProgressBaseRef: "missing"}, wantErr: true},
		{name: "missing explicit base", issue: Issue{BaseRef: "missing"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			base := strings.TrimSpace(runGit(t, source, "rev-parse", "HEAD"))
			if tt.remote {
				runGit(t, source, "update-ref", "refs/remotes/origin/main", base)
				runGit(t, source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
			}
			local := &LocalGit{sourceRoot: source}
			got, err := local.localProgressBase(t.Context(), source, tt.issue)
			if tt.wantErr {
				if err == nil {
					t.Fatal("missing base accepted")
				}
				return
			}
			if err != nil || got != base {
				t.Fatalf("base=%q, %v, want %q", got, err, base)
			}
		})
	}
}

func TestLocalProgressRebaseSameFile(t *testing.T) {
	t.Parallel()
	source := initSourceRepo(t)
	path := filepath.Join(t.TempDir(), "worker")
	lines := strings.Repeat("unchanged\n", 15)
	if err := os.WriteFile(filepath.Join(source, "code.txt"), []byte("original\n"+lines+"end\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "add", "code.txt")
	runGit(t, source, "commit", "-m", "create source")
	runGit(t, source, "worktree", "add", "-b", "worker", path)
	if err := os.WriteFile(filepath.Join(path, "code.txt"), []byte("original\n"+lines+"implemented\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, path, "commit", "-am", "implement")
	oldBase := strings.TrimSpace(runGit(t, source, "rev-parse", "HEAD"))
	oldHead := strings.TrimSpace(runGit(t, path, "rev-parse", "HEAD"))
	before, err := gitProgressPatch(t.Context(), path, oldBase, oldHead)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "code.txt"), []byte("base added a line\noriginal\n"+lines+"end\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "commit", "-am", "advance base")
	runGit(t, path, "rebase", "main")
	after, err := gitProgressPatch(t.Context(), path, "main", "HEAD")
	if err != nil || after == "" || after != before {
		t.Fatalf("rebased patch=%q, %v, want %q", after, err, before)
	}
	if oldHead == strings.TrimSpace(runGit(t, path, "rev-parse", "HEAD")) {
		t.Fatal("rebase did not change head")
	}
}

func TestLocalProgressPatchFailures(t *testing.T) {
	t.Parallel()
	source := initSourceRepo(t)
	tests := []struct {
		name     string
		path     string
		revision string
		canceled bool
	}{
		{name: "missing workspace", path: filepath.Join(t.TempDir(), "missing"), revision: "HEAD"},
		{name: "missing revision", path: source, revision: "missing"},
		{name: "canceled read", path: source, revision: "HEAD", canceled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			if _, err := gitProgressPatch(ctx, tt.path, tt.revision); err == nil {
				t.Fatal("failed read accepted")
			}
		})
	}
}

func TestLocalProgressObservation(t *testing.T) {
	t.Parallel()
	source := initSourceRepo(t)
	root := t.TempDir()
	backend, err := NewBackend(KindLocalGit, LocalGitOptions{Root: root, SourceRoot: source, AutoBranch: true})
	if err != nil {
		t.Fatal(err)
	}
	issue := Issue{Identifier: "progress"}
	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatal(err)
	}
	local := backend.(LocalProgressProvider)
	before, err := local.LocalProgress(t.Context(), info, issue)
	if err != nil || before.HeadSHA == "" || before.CommitFingerprint != "" || before.TrackedFingerprint != "" {
		t.Fatalf("initial observation=%+v, %v", before, err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "binary.dat"), []byte{0, 1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, info.Path, "add", "binary.dat")
	runGit(t, info.Path, "commit", "-m", "add binary implementation")
	after, err := local.LocalProgress(t.Context(), info, issue)
	if err != nil || after.CommitFingerprint == "" || before.HeadSHA == after.HeadSHA {
		t.Fatalf("commit observation=%+v, %v", after, err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "binary.dat"), []byte{0, 1, 3, 4}, 0o600); err != nil {
		t.Fatal(err)
	}
	dirty, err := local.LocalProgress(t.Context(), info, issue)
	if err != nil || dirty.TrackedFingerprint == "" || dirty.CommitFingerprint != after.CommitFingerprint {
		t.Fatalf("tracked observation=%+v, %v", dirty, err)
	}
	tests := []struct {
		name  string
		info  Info
		issue Issue
	}{
		{name: "invalid path", info: Info{Path: filepath.Join(root, "..", "outside")}, issue: issue},
		{name: "missing workspace", info: Info{Path: filepath.Join(root, "missing")}, issue: issue},
		{name: "missing base", info: info, issue: Issue{Identifier: "progress", BaseRef: "missing"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := local.LocalProgress(t.Context(), tt.info, tt.issue); err == nil {
				t.Fatal("failed observation accepted")
			}
		})
	}
}
