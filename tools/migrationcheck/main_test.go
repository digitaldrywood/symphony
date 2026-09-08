package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestCheckMigrations(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		files       []string
		directories []string
		want        string
	}{
		{"unique", []string{"hub/00017_identity.sql", "hub/00018_viewed.sql", "hub/00019_onboarding.sql"}, []string{"hub"}, ""},
		{"collision", []string{"hub/00018_viewed.sql", "hub/00018_onboarding.sql"}, []string{"hub"}, "duplicate migration version 18"},
		{"numeric collision", []string{"hub/18_viewed.sql", "hub/00018_onboarding.sql"}, []string{"hub"}, "duplicate migration version 18"},
		{"separate schemas", []string{"hub/00018_viewed.sql", "store/00018_session.sql"}, []string{"hub", "store"}, ""},
		{"ignore other files", []string{"hub/00018_viewed.sql", "hub/README.md", "hub/nested/00018_example.sql"}, []string{"hub"}, ""},
		{"invalid version", []string{"hub/name.sql"}, []string{"hub"}, "invalid migration version"},
		{"zero version", []string{"hub/00000_name.sql"}, []string{"hub"}, "invalid migration version"},
		{"overflow", []string{"hub/99999999999999999999_name.sql"}, []string{"hub"}, "invalid migration version"},
		{"empty", []string{"hub/README.md"}, []string{"hub"}, "no SQL migrations"},
		{"missing", nil, []string{"hub"}, "read migrations"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			files := fstest.MapFS{}
			for _, name := range test.files {
				files[name] = &fstest.MapFile{}
			}
			err := checkMigrations(files, test.directories)
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, directory := range []string{"internal/store/migrations", "internal/hubserver/migrations"} {
		writeMigration(t, root, directory+"/00001_initial.sql")
	}
	for _, test := range []struct {
		name string
		args []string
		want int
	}{
		{"success", []string{"-root", root}, 0},
		{"missing root", []string{"-root", filepath.Join(root, "missing")}, 1},
		{"unknown flag", []string{"-unknown"}, 2},
		{"positional argument", []string{"unexpected"}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if got := run(test.args, &stdout, &stderr); got != test.want {
				t.Fatalf("exit = %d, want %d: %s", got, test.want, stderr.String())
			}
			if stdout.Len()+stderr.Len() == 0 {
				t.Fatal("missing diagnostic")
			}
		})
	}
}

func TestIndividuallyValidBranchesRejectIntegratedCollision(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		command := exec.CommandContext(t.Context(), "git", append([]string{"-c", "user.name=Test User", "-c", "user.email=test@example.test", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=" + os.DevNull}, args...)...)
		command.Dir = root
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(strings.ToUpper(entry), "GIT_") {
				command.Env = append(command.Env, entry)
			}
		}
		command.Env = append(command.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	check := func() error { return checkMigrations(os.DirFS(root), []string{"hub"}) }
	git("init", "-b", "main")
	writeMigration(t, root, "hub/00017_identity.sql")
	git("add", ".")
	git("commit", "-m", "initial schema")
	git("checkout", "-b", "onboarding")
	writeMigration(t, root, "hub/00018_onboarding.sql")
	if err := check(); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-m", "onboarding schema")
	git("checkout", "main")
	writeMigration(t, root, "hub/00018_viewed.sql")
	if err := check(); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-m", "viewed schema")
	git("merge", "--no-edit", "onboarding")
	err := check()
	if err == nil || !strings.Contains(err.Error(), "duplicate migration version 18") || !strings.Contains(err.Error(), "00018_onboarding.sql") || !strings.Contains(err.Error(), "00018_viewed.sql") {
		t.Fatalf("integrated error = %v", err)
	}
}

func writeMigration(t *testing.T, root, name string) {
	t.Helper()
	filename := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte("-- +goose Up\nSELECT 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}
