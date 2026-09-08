package shell

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCommandWindowsConfiguredArguments(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args string
		want []string
	}{
		{name: "spaces", args: `"C:/path with spaces"`, want: []string{"C:/path with spaces"}},
		{name: "quotes", args: `"say \"hello\""`, want: []string{`say "hello"`}},
		{name: "percent", args: `"percent%value" "%DETENT_SHELL_VALUE%"`, want: []string{"percent%value", "expanded"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := Command(t.Context(), `"`+executable+`" -test.run=TestShellArgumentProcess -- `+tt.args, "cmd")
			cmd.Env = append(cmd.Environ(), "DETENT_SHELL_ARGUMENT_PROCESS=1", "DETENT_SHELL_VALUE=expanded", "GOCOVERDIR="+t.TempDir())
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("command failed: %v\n%s", err, output)
			}
			var got []string
			if err := json.Unmarshal(output, &got); err != nil {
				t.Fatalf("decode output %q: %v", output, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("child arguments = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestCommandWindowsQuotedGitDirectory(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"plain", "directory with spaces", "percent%value"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), name)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			script := `git -C "` + filepath.ToSlash(dir) + `" --version`
			legacy := exec.CommandContext(t.Context(), "cmd", "/C", script)
			output, err := legacy.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "cannot change to '\"") {
				t.Fatalf("legacy launch = %v, %s; want literal-quote directory failure", err, output)
			}
			for _, shellName := range []string{"", "cmd.exe", filepath.Join(os.Getenv("SystemRoot"), "System32", "cmd.exe")} {
				cmd := Command(t.Context(), script, shellName)
				output, err := cmd.CombinedOutput()
				if err != nil || !strings.HasPrefix(string(output), "git version ") {
					t.Fatalf("shell %q: %v, %s", shellName, err, output)
				}
			}
		})
	}
}

func TestCommandWindowsPreservesShellSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		want   string
	}{
		{name: "expansion", script: `echo %DETENT_SHELL_VALUE%`, want: "expanded"},
		{name: "sequence", script: `echo first&&echo second`, want: "first\r\nsecond"},
		{name: "pipeline", script: `echo piped|findstr piped`, want: "piped"},
		{name: "redirection", script: `echo redirected>result.txt&&type result.txt`, want: "redirected"},
		{name: "escaped operator", script: `echo first^&second`, want: "first&second"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := Command(t.Context(), tt.script, "cmd")
			cmd.Dir = t.TempDir()
			cmd.Env = append(cmd.Environ(), "DETENT_SHELL_VALUE=expanded")
			output, err := cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != tt.want {
				t.Fatalf("command = %v, %q; want %q", err, output, tt.want)
			}
		})
	}
}

func TestCommandWindowsLeavesOtherShellsUnchanged(t *testing.T) {
	t.Parallel()

	for _, shellName := range []string{"powershell.exe", "pwsh.exe", "bash.exe", "custom.exe"} {
		t.Run(shellName, func(t *testing.T) {
			t.Parallel()
			cmd := Command(t.Context(), `echo "quoted"`, shellName)
			if cmd.SysProcAttr != nil {
				t.Fatalf("SysProcAttr = %+v, want default serialization", cmd.SysProcAttr)
			}
		})
	}
}
