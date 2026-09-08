package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestSessionLocalCommitProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		change       func(*testing.T, string, string)
		wantProgress bool
	}{
		{name: "new implementation commit", wantProgress: true, change: func(t *testing.T, path, _ string) {
			writeLocalProgressFile(t, path, "implementation.txt", "second implementation\n")
			runRunnerGit(t, path, "commit", "-am", "implement second change")
		}},
		{name: "amended implementation", wantProgress: true, change: func(t *testing.T, path, _ string) {
			writeLocalProgressFile(t, path, "implementation.txt", "amended implementation\n")
			runRunnerGit(t, path, "commit", "--amend", "-am", "amend implementation")
		}},
		{name: "whitespace implementation", wantProgress: true, change: func(t *testing.T, path, _ string) {
			writeLocalProgressFile(t, path, "implementation.txt", " first implementation\n")
			runRunnerGit(t, path, "commit", "-am", "adjust whitespace")
		}},
		{name: "tracked implementation edit", wantProgress: true, change: func(t *testing.T, path, _ string) {
			writeLocalProgressFile(t, path, "implementation.txt", "tracked implementation\n")
		}},
		{name: "committed handoff only", change: func(t *testing.T, path, _ string) {
			if err := os.MkdirAll(filepath.Join(path, ".detent"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeLocalProgressFile(t, path, ".detent/notes.md", "handoff\n")
			runRunnerGit(t, path, "add", "-f", ".detent/notes.md")
			runRunnerGit(t, path, "commit", "-m", "update handoff")
		}},
		{name: "unchanged observation", change: func(*testing.T, string, string) {}},
		{name: "empty commit", change: func(t *testing.T, path, _ string) {
			runRunnerGit(t, path, "commit", "--allow-empty", "-m", "empty")
		}},
		{name: "commit metadata churn", change: func(t *testing.T, path, _ string) {
			runRunnerGit(t, path, "commit", "--amend", "--date=2001-01-01T00:00:00Z", "-m", "renamed commit")
		}},
		{name: "untracked noise", change: func(t *testing.T, path, _ string) {
			writeLocalProgressFile(t, path, "unrelated.log", "unrelated output\n")
		}},
		{name: "base only rebase", change: func(t *testing.T, path, source string) {
			writeLocalProgressFile(t, source, "README.md", "upstream change\n")
			runRunnerGit(t, source, "commit", "-am", "update base")
			runRunnerGit(t, path, "rebase", "main")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			source := initRunnerSourceRepo(t)
			backend, err := workspace.NewBackend(workspace.KindLocalGit, workspace.LocalGitOptions{
				Root: filepath.Join(t.TempDir(), "workspaces"), SourceRoot: source, AutoBranch: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			issue := workspace.Issue{Identifier: "local-progress", BaseRef: "main"}
			info, err := backend.Create(t.Context(), issue)
			if err != nil {
				t.Fatal(err)
			}
			writeLocalProgressFile(t, info.Path, "implementation.txt", "first implementation\n")
			runRunnerGit(t, info.Path, "add", "implementation.txt")
			runRunnerGit(t, info.Path, "commit", "-m", "implement first change")
			runner := &Runner{}
			probe := func(ctx context.Context) (sessionProgressSnapshot, error) {
				return runner.sessionProgressSnapshot(ctx, backend, info, issue, nil)
			}
			before, err := probe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			tt.change(t, info.Path, source)
			started := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			controller := &sessionBrakeController{
				startedAt: started, lastProgressAt: started, noProgressTimeout: time.Minute,
				initial: before, current: before, probe: probe, cancelSession: cancel,
				now: func() time.Time { return started.Add(time.Minute) },
			}
			observation := sessionProgressObservation(before, started)
			controller.observation = &observation
			controller.checkProgress(ctx, started.Add(time.Minute))
			if got := controller.lastProgressAt.After(started); got != tt.wantProgress {
				t.Fatalf("progress = %t, want %t; before=%+v after=%+v", got, tt.wantProgress, before, controller.current)
			}
			controller.checkProgress(ctx, controller.lastProgressAt.Add(time.Minute))
			if controller.breach == nil {
				t.Fatal("unchanged observation did not expire")
			}
		})
	}
}

func writeLocalProgressFile(t *testing.T, path, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(path, name), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
