package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/procgroup"
)

func checkpointFixture(t *testing.T) (*LocalGit, Issue, CheckpointPlan, string) {
	t.Helper()
	source := initSourceRepo(t)
	remote := initBareRemote(t)
	runGit(t, source, "remote", "add", "origin", remote)
	runGit(t, source, "push", "-u", "origin", "main")
	backend, err := NewLocalGit(LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true})
	if err != nil {
		t.Fatal(err)
	}
	issue := Issue{ID: "2313", Identifier: "example/repo#2313"}
	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := backend.PrepareCheckpoint(t.Context(), info, issue)
	if err != nil {
		t.Fatal(err)
	}
	return backend, issue, plan, remote
}

func checkpointWrite(t *testing.T, root, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointIntendedWork(t *testing.T) {
	t.Parallel()
	for _, work := range []string{"dirty", "unpushed", "already pushed", "deleted", "unchanged", "literal path"} {
		t.Run(work, func(t *testing.T) {
			t.Parallel()
			backend, _, plan, remote := checkpointFixture(t)
			path := "README.md"
			if work == "literal path" {
				path = "[intended].go"
			}
			if work != "unchanged" {
				checkpointWrite(t, plan.Info.Path, path, "intended issue work\n")
			}
			if work == "deleted" {
				if err := os.Remove(filepath.Join(plan.Info.Path, path)); err != nil {
					t.Fatal(err)
				}
			}
			if work == "unpushed" || work == "already pushed" {
				runGit(t, plan.Info.Path, "add", path)
				runGit(t, plan.Info.Path, "commit", "-m", "intended work")
			}
			if work == "already pushed" {
				runGit(t, plan.Info.Path, "push", "origin", "HEAD")
			}
			checkpointWrite(t, plan.Info.Path, "unrelated.txt", "keep unrelated staged work\n")
			runGit(t, plan.Info.Path, "add", "unrelated.txt")
			checkpointWrite(t, plan.Info.Path, ".env", "SECRET=private\n")
			selection := CheckpointSelection{Reviewed: true, HeadSHA: strings.TrimSpace(runGit(t, plan.Info.Path, "rev-parse", "HEAD")), Paths: []string{path}}
			guard := func(context.Context) error { return nil }
			head, err := backend.Checkpoint(t.Context(), plan, selection, guard, procgroup.Environment{})
			if err != nil {
				t.Fatal(err)
			}
			if work != "unchanged" {
				if got := strings.TrimSpace(runGit(t, remote, "rev-parse", "refs/heads/"+plan.Info.Branch)); got != head {
					t.Fatalf("remote = %s; head = %s", got, head)
				}
			}
			if got := strings.TrimSpace(runGit(t, plan.Info.Path, "diff", "--cached", "--name-only")); got != "unrelated.txt" {
				t.Fatalf("unrelated index changed: %q", got)
			}
			selection.HeadSHA = head
			again, err := backend.Checkpoint(t.Context(), plan, selection, guard, procgroup.Environment{})
			if err != nil || again != head {
				t.Fatalf("duplicate = %q, %v; want unchanged %s", again, err, head)
			}
			if got := runGit(t, plan.Info.Path, "show", "--format=", "--name-only", head); strings.Contains(got, "unrelated.txt") || strings.Contains(got, ".env") {
				t.Fatalf("unexpected published paths: %s", got)
			}
		})
	}
}

func TestCheckpointRefusesUnsafePublication(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"unreviewed", "stale head", "different branch", "secret path", "secret content", "secret in old commit", "unapproved commit", "directory", "symlink", "path traversal", "lost lease", "lost lease before commit", "lost lease before push", "cancelled", "network unavailable", "remote advanced", "concurrent push", "hook rejected"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			backend, _, plan, remote := checkpointFixture(t)
			checkpointWrite(t, plan.Info.Path, "README.md", "intended work\n")
			selection := CheckpointSelection{Reviewed: true, HeadSHA: plan.BaseSHA, Paths: []string{"README.md"}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			guard := func(context.Context) error { return nil }
			switch failure {
			case "unreviewed":
				selection.Reviewed = false
			case "stale head":
				selection.HeadSHA = strings.Repeat("1", 40)
			case "different branch":
				runGit(t, plan.Info.Path, "switch", "-c", "human-work")
			case "secret path":
				selection.Paths = []string{".env"}
				checkpointWrite(t, plan.Info.Path, ".env", "secret")
			case "secret content":
				checkpointWrite(t, plan.Info.Path, "README.md", "-----BEGIN PRIVATE KEY-----")
			case "secret in old commit", "unapproved commit":
				path, content := "README.md", "-----BEGIN PRIVATE KEY-----"
				if failure == "unapproved commit" {
					path, content = "unrelated.txt", "not issue work"
				}
				checkpointWrite(t, plan.Info.Path, path, content)
				runGit(t, plan.Info.Path, "add", path)
				runGit(t, plan.Info.Path, "commit", "-m", "unreviewed work")
				checkpointWrite(t, plan.Info.Path, path, "safe now")
				selection.HeadSHA = strings.TrimSpace(runGit(t, plan.Info.Path, "rev-parse", "HEAD"))
			case "directory":
				selection.Paths = []string{"."}
			case "symlink":
				if err := os.Symlink("README.md", filepath.Join(plan.Info.Path, "link")); err != nil {
					t.Skip(err)
				}
				selection.Paths = []string{"link"}
			case "path traversal":
				selection.Paths = []string{"../outside"}
			case "lost lease":
				guard = func(context.Context) error { return errors.New("lease lost") }
			case "lost lease before commit", "lost lease before push":
				calls := 0
				limit := 2
				if failure == "lost lease before push" {
					limit = 3
				}
				guard = func(context.Context) error {
					calls++
					if calls == limit {
						return errors.New("lease lost during checkpoint")
					}
					return nil
				}
			case "cancelled":
				cancel()
			case "network unavailable":
				plan.RemoteURL = filepath.Join(t.TempDir(), "missing.git")
			case "remote advanced":
				runGit(t, backend.sourceRoot, "commit", "--allow-empty", "-m", "concurrent remote work")
				runGit(t, backend.sourceRoot, "push", "origin", "HEAD:refs/heads/"+plan.Info.Branch)
			case "concurrent push":
				calls := 0
				guard = func(ctx context.Context) error {
					calls++
					if calls == 3 {
						if _, err := runGitAt(ctx, backend.sourceRoot, "commit", "--allow-empty", "-m", "racing remote work"); err != nil {
							return err
						}
						if _, err := runGitAt(ctx, backend.sourceRoot, "push", "origin", "HEAD:refs/heads/"+plan.Info.Branch); err != nil {
							return err
						}
					}
					return nil
				}
			case "hook rejected":
				if testing.Short() {
					t.Skip("git hook subprocess")
				}
				hooks := t.TempDir()
				checkpointWrite(t, hooks, "pre-commit", "#!/bin/sh\nexit 1\n")
				if err := os.Chmod(filepath.Join(hooks, "pre-commit"), 0o700); err != nil {
					t.Fatal(err)
				}
				runGit(t, plan.Info.Path, "config", "core.hooksPath", hooks)
			}
			_, err := backend.Checkpoint(ctx, plan, selection, guard, procgroup.Environment{})
			if err == nil {
				t.Fatal("unsafe checkpoint succeeded")
			}
			remoteHead, _, readErr := remoteBranchHead(t.Context(), plan.Info.Path, remote, plan.Info.Branch)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if failure != "remote advanced" && failure != "concurrent push" && remoteHead != "" {
				t.Fatalf("unexpected remote publication: %s", remoteHead)
			}
			if failure == "remote advanced" || failure == "concurrent push" {
				want := strings.TrimSpace(runGit(t, backend.sourceRoot, "rev-parse", "HEAD"))
				if remoteHead != want {
					t.Fatalf("concurrent remote work lost: %s != %s", remoteHead, want)
				}
			}
		})
	}
}

func TestCheckpointDurableEvidenceBeforeHardTermination(t *testing.T) {
	t.Parallel()
	backend, issue, plan, _ := checkpointFixture(t)
	checkpointWrite(t, plan.Info.Path, "README.md", "work after journal; worker killed without epilogue")
	data, err := os.ReadFile(plan.Journal)
	if err != nil {
		t.Fatal(err)
	}
	var record CheckpointRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "active" || record.Published || !strings.Contains(record.Detail, "No checkpoint handshake") {
		t.Fatalf("false recovery claim: %+v", record)
	}
	reopened, err := NewLocalGit(LocalGitOptions{Root: backend.root, SourceRoot: backend.sourceRoot, AutoBranch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CleanupIssue(t.Context(), issue); !errors.Is(err, ErrWorkspacePreserved) {
		t.Fatalf("recovery workspace removed: %v", err)
	}
}

func TestCheckpointGitReadFailures(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"missing blob", "unreadable entry", "unreadable blob", "symlink history", "invalid entry", "empty remote", "invalid remote", "unreadable remote", "unrelated base", "unreadable history", "unreadable diff", "empty commit"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			failure := errors.New("git unavailable")
			git := func(_ context.Context, args ...string) (string, error) {
				switch args[0] {
				case "ls-tree", "ls-files":
					switch scenario {
					case "missing blob":
						return "", nil
					case "unreadable entry":
						return "", failure
					case "symlink history":
						return "120000 blob hash\tfile.go\x00", nil
					case "invalid entry":
						return "invalid", nil
					}
					return "100644 blob hash\tfile.go\x00", nil
				case "show":
					return "", failure
				case "ls-remote":
					if scenario == "empty remote" {
						return "", nil
					}
					if scenario == "invalid remote" {
						return "unexpected ref", nil
					}
					return "", failure
				case "merge-base":
					if scenario == "unrelated base" {
						return "", failure
					}
					return "", nil
				case "rev-list":
					if scenario == "unreadable history" {
						return "", failure
					}
					return "commit", nil
				case "diff-tree":
					if scenario == "unreadable diff" {
						return "", failure
					}
					return "", nil
				}
				t.Fatalf("unexpected git command %v", args)
				return "", failure
			}
			var err error
			switch scenario {
			case "empty remote", "invalid remote", "unreadable remote":
				_, err = checkpointRemoteHead(t.Context(), git, CheckpointPlan{Info: Info{Branch: "owned"}, RemoteURL: "remote"})
			case "unrelated base", "unreadable history", "unreadable diff", "empty commit":
				err = checkpointHistory(t.Context(), git, "base", "head", []string{"file.go"})
			default:
				err = checkpointBlob(t.Context(), git, "commit:", "file.go")
			}
			wantOK := scenario == "missing blob" || scenario == "empty remote" || scenario == "empty commit"
			if (err == nil) != wantOK {
				t.Fatalf("Git failure handling = %v", err)
			}
		})
	}
}

func TestCheckpointPreparationRefusesUnownedWork(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"human branch", "other workspace", "not a worktree", "missing remote", "missing base", "invalid root"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			backend, issue, plan, _ := checkpointFixture(t)
			switch scenario {
			case "human branch":
				plan.Info.Branch = "human"
			case "other workspace":
				plan.Info.Path = t.TempDir()
			case "not a worktree":
				backend.sourceRoot = t.TempDir()
			case "missing remote":
				runGit(t, plan.Info.Path, "remote", "remove", "origin")
			case "missing base":
				issue.BaseRef = "missing-base"
			case "invalid root":
				backend.root = ""
			}
			if _, err := backend.PrepareCheckpoint(t.Context(), plan.Info, issue); err == nil {
				t.Fatal("unsafe preparation succeeded")
			}
		})
	}
}

func TestCheckpointJournalUnavailable(t *testing.T) {
	t.Parallel()
	for _, journal := range []string{"", filepath.Join(t.TempDir(), "missing", "journal.json")} {
		t.Run(journal, func(t *testing.T) {
			t.Parallel()
			if err := WriteCheckpointRecord(CheckpointPlan{Journal: journal}, CheckpointRecord{}); err == nil {
				t.Fatal("unavailable journal accepted")
			}
		})
	}
}

func TestCheckpointGuardCancelledBeforeGit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	git := func(context.Context, ...string) (string, error) { t.Fatal("cancelled guard ran Git"); return "", nil }
	if err := checkpointGuard(ctx, git, CheckpointPlan{}, "head", func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("guard = %v", err)
	}
}
