package shell

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"testing"
)

func TestShellArgumentProcess(t *testing.T) {
	if os.Getenv("DETENT_SHELL_ARGUMENT_PROCESS") != "1" {
		return
	}
	index := slices.Index(os.Args, "--")
	if index < 0 {
		t.Fatal("missing argument separator")
	}
	if err := json.NewEncoder(os.Stdout).Encode(os.Args[index+1:]); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func TestCommandWithArgsExecution(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(t.TempDir(), "argument helper"+filepath.Ext(executable))
	if err := os.WriteFile(helper, data, 0o700); err != nil {
		t.Fatal(err)
	}
	shells := []string{"sh"}
	if runtime.GOOS == "windows" {
		shells = []string{"cmd", "powershell", "pwsh", "bash"}
	}
	tests := []struct {
		name string
		args []string
	}{
		{name: "none"},
		{name: "spaces", args: []string{"path with spaces", "quoted'value"}},
		{name: "paths", args: []string{`C:/directory with spaces/file`, `C:\directory with spaces\`, ""}},
		{name: "quotes", args: []string{`say "hello"`, `quoted'value`, `a\"b`, `"&echo injected&"`}},
		{name: "percent", args: []string{"percent%value", "%PATH%", "100%%", "%DETENT_SHELL_VALUE%"}},
		{name: "operators", args: []string{"Bash(git *)", "a&b|c<d>e(f)^!", "--model", "fable"}},
	}
	for _, shellName := range shells {
		t.Run(shellName, func(t *testing.T) {
			t.Parallel()
			if _, err := exec.LookPath(shellName); err != nil {
				t.Skipf("shell unavailable: %v", err)
			}
			quote := argQuoterForOS(shellName, runtime.GOOS)
			command := quote(helper) + " -test.run=TestShellArgumentProcess -- configured"
			if runtime.GOOS == "windows" {
				if shellBase(shellName) == "cmd" {
					command = `"` + helper + `" -test.run=TestShellArgumentProcess -- configured`
				} else if isPowerShell(shellBase(shellName)) {
					command = "& " + command
				}
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					if shellName == "powershell" && (tt.name == "quotes" || tt.name == "paths") {
						t.Skip("Windows PowerShell uses legacy native argument parsing")
					}
					cmd := CommandWithArgs(t.Context(), command, shellName, tt.args)
					cmd.Env = append(cmd.Environ(), "DETENT_SHELL_ARGUMENT_PROCESS=1", "DETENT_SHELL_VALUE=expanded", "MSYS2_ARG_CONV_EXCL=*")
					output, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("command failed: %v\n%s", err, output)
					}
					var got []string
					if err := json.Unmarshal(output, &got); err != nil {
						t.Fatalf("decode output %q: %v", output, err)
					}
					want := append([]string{"configured"}, tt.args...)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("child arguments = %#v, want %#v", got, want)
					}
				})
			}
		})
	}
}
