package workspace

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLocalGitCleanupChecksEveryWorkspace(t *testing.T) {
	t.Parallel()
	for _, record := range []string{"none", "ordinary", "preserved"} {
		for _, work := range []string{"clean", "tracked", "staged", "untracked", "unpushed", "detached", "missing", "broken", "different branch", "no remote"} {
			t.Run(record+"/"+work, func(t *testing.T) {
				t.Parallel()
				source := initSourceRepo(t)
				if work != "no remote" {
					runGit(t, source, "remote", "add", "origin", initBareRemote(t))
					runGit(t, source, "push", "-u", "origin", "main")
				}
				backend, err := NewLocalGit(LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true})
				if err != nil {
					t.Fatal(err)
				}
				issue := Issue{Identifier: "cleanup-safety"}
				info, err := backend.Create(t.Context(), issue)
				if err != nil {
					t.Fatal(err)
				}
				if record != "none" {
					if err := backend.recordCleanupOwnership(t.Context(), info, issue, true); err != nil {
						t.Fatal(err)
					}
				}
				if record == "preserved" {
					if _, err := backend.PreserveIssue(t.Context(), issue); err != nil {
						t.Fatal(err)
					}
				}
				if work == "detached" {
					runGit(t, info.Path, "checkout", "--detach")
				}
				if work != "clean" && work != "no remote" && work != "broken" {
					name := "README.md"
					if work == "untracked" {
						name = "implementation.go"
					}
					if err := os.WriteFile(filepath.Join(info.Path, name), []byte("unique work\n"), 0o600); err != nil {
						t.Fatal(err)
					}
					if work != "tracked" && work != "untracked" {
						runGit(t, info.Path, "add", name)
						if work != "staged" {
							runGit(t, info.Path, "commit", "-m", "unique work")
						}
					}
				}
				if work == "different branch" {
					runGit(t, info.Path, "checkout", "--detach", "origin/main")
				}
				head := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD"))
				status := runGit(t, info.Path, "status", "--porcelain")
				if work == "missing" {
					if err := os.Rename(info.Path, filepath.Join(t.TempDir(), "moved")); err != nil {
						t.Fatal(err)
					}
				}
				if work == "broken" {
					if err := os.Rename(filepath.Join(info.Path, ".git"), filepath.Join(info.Path, "saved-git")); err != nil {
						t.Fatal(err)
					}
				}
				result, err := backend.CleanupIssue(t.Context(), issue)
				if work == "clean" {
					if err != nil || result.Worktrees != 1 || result.Branches != 1 {
						t.Fatalf("cleanup = %+v, %v; want removed workspace and branch", result, err)
					}
					return
				}
				if !errors.Is(err, ErrWorkspacePreserved) || result.Worktrees != 0 || result.Branches != 0 {
					t.Fatalf("cleanup = %+v, %v; want retained work", result, err)
				}
				if !branchExists(t, source, info.Branch) {
					t.Fatal("cleanup deleted branch holding retained work")
				}
				if work != "missing" && work != "broken" {
					if got := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD")); got != head {
						t.Fatalf("head = %q, want %q", got, head)
					}
					if got := runGit(t, info.Path, "status", "--porcelain"); got != status {
						t.Fatalf("status = %q, want %q", got, status)
					}
				}
			})
		}
	}
}

func TestLocalGitCleanupChecksRemovalHooks(t *testing.T) {
	t.Parallel()
	skipWindows(t)
	for _, dirty := range []bool{false, true} {
		t.Run(strconv.FormatBool(dirty), func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			publishCleanupSource(t, source)
			trace := filepath.Join(t.TempDir(), "hook-ran")
			backend, err := NewLocalGit(LocalGitOptions{
				Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true,
				Hooks: Hooks{BeforeRemove: "touch " + shellQuote(trace) + " && printf 'hook work' > README.md"},
			})
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{Identifier: "cleanup-hook"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			if dirty {
				if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("worker work"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := backend.CleanupIssue(t.Context(), issue)
			if !errors.Is(err, ErrWorkspacePreserved) || result.Worktrees != 0 {
				t.Fatalf("cleanup = %+v, %v; want preservation", result, err)
			}
			_, err = os.Stat(trace)
			if dirty != errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("hook trace stat = %v, dirty = %t", err, dirty)
			}
			want := "hook work"
			if dirty {
				want = "worker work"
			}
			if got := readFile(t, filepath.Join(info.Path, "README.md")); got != want {
				t.Fatalf("retained work = %q, want %q", got, want)
			}
		})
	}
}

func TestLocalGitPreservesRevokedWorkAcrossCleanupAndRestart(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"tracked", "staged", "untracked", "unpushed", "pushed", "local only"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			remote := initBareRemote(t)
			if kind != "local only" {
				runGit(t, source, "remote", "add", "origin", remote)
				runGit(t, source, "push", "-u", "origin", "main")
			}
			opts := LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true}
			backend, err := NewLocalGit(opts)
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{ProjectID: "detent", ID: "2138", Identifier: "digitaldrywood/detent#2138"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			name := "README.md"
			if kind == "untracked" {
				name = "implementation.go"
			}
			content := []byte("completed worker implementation\n")
			if err := os.WriteFile(filepath.Join(info.Path, name), content, 0o600); err != nil {
				t.Fatal(err)
			}
			if kind == "staged" || kind == "unpushed" || kind == "pushed" || kind == "local only" {
				runGit(t, info.Path, "add", name)
			}
			if kind == "unpushed" || kind == "pushed" || kind == "local only" {
				runGit(t, info.Path, "commit", "-m", "completed implementation")
			}
			if kind == "pushed" {
				runGit(t, info.Path, "push", "-u", "origin", info.Branch)
			}
			head := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD"))
			status := runGit(t, info.Path, "status", "--porcelain")
			preserved, err := backend.PreserveIssue(t.Context(), issue)
			if err != nil || !preserved.Preserved || preserved.Path != info.Path || preserved.HeadSHA != head {
				t.Fatalf("preservation = %#v, error = %v", preserved, err)
			}
			backend, err = NewLocalGit(opts)
			if err != nil {
				t.Fatal(err)
			}
			_, cleanupErr := backend.CleanupIssue(t.Context(), issue)
			if kind != "pushed" {
				if !errors.Is(cleanupErr, ErrWorkspacePreserved) {
					t.Fatalf("cleanup error = %v, want retained workspace", cleanupErr)
				}
				actual, err := os.ReadFile(filepath.Join(info.Path, name))
				if err != nil || string(actual) != string(content) {
					t.Fatalf("retained file = %q, error = %v", actual, err)
				}
				if got := runGit(t, info.Path, "status", "--porcelain"); got != status {
					t.Fatalf("retained index/status = %q, want %q", got, status)
				}
				if got := strings.TrimSpace(runGit(t, info.Path, "rev-parse", "HEAD")); got != head {
					t.Fatalf("retained head = %q, want %q", got, head)
				}
				residual, err := backend.ReconcileResiduals(t.Context(), nil)
				if err != nil || residual.Removed != 0 || residual.PreservedSkipped != 1 {
					t.Fatalf("residual cleanup = %#v, error = %v", residual, err)
				}
				resumed, err := backend.Create(t.Context(), issue)
				if err != nil || resumed.Path != info.Path || resumed.Created {
					t.Fatalf("resumed workspace = %#v, error = %v", resumed, err)
				}
				if kind != "unpushed" && kind != "local only" {
					runGit(t, info.Path, "add", name)
					runGit(t, info.Path, "commit", "-m", "publish recovered implementation")
				}
				if kind == "local only" {
					runGit(t, source, "remote", "add", "origin", remote)
				}
				runGit(t, info.Path, "push", "-u", "origin", info.Branch)
				_, cleanupErr = backend.CleanupIssue(t.Context(), issue)
			}
			if cleanupErr != nil {
				t.Fatalf("cleanup after delivery: %v", cleanupErr)
			}
			if _, err := os.Stat(info.Path); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("delivered workspace remains: %v", err)
			}
		})
	}
}

func TestLocalGitPreservationVerifiesWorkEvidence(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"clean base", "dirty", "local commit", "pushed commit", "base branch pushed", "remote branch deleted"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			source := initSourceRepo(t)
			remote := initBareRemote(t)
			runGit(t, source, "remote", "add", "origin", remote)
			runGit(t, source, "push", "-u", "origin", "main")
			base := strings.TrimSpace(runGit(t, source, "rev-parse", "HEAD"))
			backend, err := NewLocalGit(LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true})
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{Identifier: "evidence", BaseRef: base}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			changed := kind == "dirty" || kind == "local commit" || kind == "pushed commit" || kind == "remote branch deleted"
			if changed {
				if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("worker implementation\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if kind != "dirty" {
					runGit(t, info.Path, "add", "README.md")
					runGit(t, info.Path, "commit", "-m", "implement change")
				}
			}
			if kind == "pushed commit" || kind == "base branch pushed" || kind == "remote branch deleted" {
				runGit(t, info.Path, "push", "origin", info.Branch)
			}
			if kind == "remote branch deleted" {
				runGit(t, remote, "update-ref", "-d", "refs/heads/"+info.Branch)
			}
			preserved, err := backend.PreserveIssue(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			if !preserved.LocalChangesVerified {
				t.Fatalf("unverified local evidence: %#v", preserved)
			}
			localWork := preserved.UnpushedCommits > 0 || len(preserved.TrackedPaths) > 0 || len(preserved.UntrackedPaths) > 0
			if localWork != (kind == "dirty" || kind == "local commit") {
				t.Fatalf("local evidence = %#v", preserved)
			}
			delivery := preserved.Delivery
			delivered := delivery != nil && delivery.RemoteBranchExists && delivery.CommitsAhead > 0 && delivery.LocalHeadSHA == delivery.RemoteHeadSHA
			if delivered != (kind == "pushed commit") {
				t.Fatalf("delivery evidence = %#v", preserved)
			}
		})
	}
}

func TestLocalGitPreservationInspectionFailureKeepsFiles(t *testing.T) {
	t.Parallel()
	backend, err := NewLocalGit(LocalGitOptions{Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: initSourceRepo(t), AutoBranch: true})
	if err != nil {
		t.Fatal(err)
	}
	issue := Issue{Identifier: "detent#2138"}
	info, err := backend.Create(t.Context(), issue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.PreserveIssue(t.Context(), issue); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(info.Path, ".git"), filepath.Join(info.Path, "saved-git-pointer")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.CleanupIssue(t.Context(), issue); !errors.Is(err, ErrWorkspacePreserved) {
		t.Fatalf("cleanup error = %v, want preservation on failed inspection", err)
	}
	result, err := backend.ReconcileResiduals(t.Context(), nil)
	if err != nil || result.PreservedSkipped != 1 || result.Removed != 0 {
		t.Fatalf("residual cleanup = %#v, error = %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(info.Path, "README.md")); err != nil {
		t.Fatalf("retained file missing: %v", err)
	}
}

func TestFilesystemPreservationSurvivesRestartAndResumption(t *testing.T) {
	t.Parallel()
	for _, artifact := range []bool{false, true} {
		name := "empty"
		if artifact {
			name = "artifact"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := FilesystemOptions{Root: filepath.Join(t.TempDir(), "workspaces")}
			backend, err := NewFilesystem(opts)
			if err != nil {
				t.Fatal(err)
			}
			issue := Issue{ProjectID: "video", ID: "2138", Identifier: "video#2138"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			artifactPath := filepath.Join(info.Path, "artifacts", "render.mp4")
			if artifact {
				if err := os.WriteFile(artifactPath, []byte("completed render"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := backend.DiffStat(t.Context(), info, issue)
			if err != nil {
				t.Fatal(err)
			}
			preserver, ok := any(backend).(IssuePreserver)
			if !ok {
				t.Fatal("filesystem backend does not support preservation")
			}
			preserved, err := preserver.PreserveIssue(t.Context(), issue)
			if err != nil || !preserved.Preserved || preserved.Path != info.Path {
				t.Fatalf("preservation = %#v, error = %v", preserved, err)
			}
			backend, err = NewFilesystem(opts)
			if err != nil {
				t.Fatal(err)
			}
			for _, resumed := range []bool{false, true} {
				if resumed {
					resumedInfo, err := backend.Create(t.Context(), issue)
					if err != nil || resumedInfo.Created || resumedInfo.Path != info.Path {
						t.Fatalf("resumption = %#v, error = %v", resumedInfo, err)
					}
				}
				if _, err := backend.CleanupIssue(t.Context(), issue); !errors.Is(err, ErrWorkspacePreserved) {
					t.Fatalf("cleanup error = %v, want preservation", err)
				}
			}
			after, err := backend.DiffStat(t.Context(), info, issue)
			if err != nil || after != before {
				t.Fatalf("retained artifact evidence = %#v, error = %v, want %#v", after, err, before)
			}
			if artifact {
				content, err := os.ReadFile(artifactPath)
				if err != nil || string(content) != "completed render" {
					t.Fatalf("retained artifact = %q, error = %v", content, err)
				}
			}
		})
	}
}
