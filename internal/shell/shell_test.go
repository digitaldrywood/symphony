package shell

import (
	"context"
	"reflect"
	"testing"
)

func TestCommandSpecUsesPerOSDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		goos     string
		command  string
		wantName string
		wantArgs []string
	}{
		{
			name:     "unix",
			goos:     "linux",
			command:  "printf ok",
			wantName: "sh",
			wantArgs: []string{"-c", "printf ok"},
		},
		{
			name:     "windows",
			goos:     "windows",
			command:  "echo ok",
			wantName: "cmd",
			wantArgs: []string{"/C", "echo ok"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := CommandSpecForOS(tt.command, "", tt.goos)
			if got.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", got.Name, tt.wantName)
			}
			if !reflect.DeepEqual(got.Args, tt.wantArgs) {
				t.Fatalf("Args = %#v, want %#v", got.Args, tt.wantArgs)
			}
		})
	}
}

func TestCommandSpecUsesConfiguredShell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		goos     string
		shell    string
		wantName string
		wantArgs []string
	}{
		{
			name:     "unix custom shell",
			goos:     "linux",
			shell:    "bash",
			wantName: "bash",
			wantArgs: []string{"-c", "echo ok"},
		},
		{
			name:     "windows cmd path",
			goos:     "windows",
			shell:    `C:\Windows\System32\cmd.exe`,
			wantName: `C:\Windows\System32\cmd.exe`,
			wantArgs: []string{"/C", "echo ok"},
		},
		{
			name:     "windows powershell",
			goos:     "windows",
			shell:    "pwsh",
			wantName: "pwsh",
			wantArgs: []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", "echo ok"},
		},
		{
			name:     "windows posix shell override",
			goos:     "windows",
			shell:    "bash",
			wantName: "bash",
			wantArgs: []string{"-c", "echo ok"},
		},
		{
			name:     "windows posix shell exe name",
			goos:     "windows",
			shell:    "bash.exe",
			wantName: "bash.exe",
			wantArgs: []string{"-c", "echo ok"},
		},
		{
			name:     "windows posix shell exe path",
			goos:     "windows",
			shell:    `C:\Program Files\Git\bin\bash.exe`,
			wantName: `C:\Program Files\Git\bin\bash.exe`,
			wantArgs: []string{"-c", "echo ok"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := CommandSpecForOS("echo ok", tt.shell, tt.goos)
			if got.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", got.Name, tt.wantName)
			}
			if !reflect.DeepEqual(got.Args, tt.wantArgs) {
				t.Fatalf("Args = %#v, want %#v", got.Args, tt.wantArgs)
			}
		})
	}
}

func TestCommandSpecWithArgsQuotesForConfiguredShell(t *testing.T) {
	t.Parallel()

	args := []string{"-p", "--model", "fable", "Bash(git *)", "quoted'value", "percent%value"}
	tests := []struct {
		name     string
		goos     string
		shell    string
		wantName string
		wantArgs []string
	}{
		{
			name:     "unix",
			goos:     "linux",
			wantName: "sh",
			wantArgs: []string{"-c", "claude '-p' '--model' 'fable' 'Bash(git *)' 'quoted'\\''value' 'percent%value'"},
		},
		{
			name:     "windows cmd",
			goos:     "windows",
			wantName: "cmd",
			wantArgs: []string{"/C", `claude ^"-p^" ^"--model^" ^"fable^" ^"Bash^(git^ ^*^)^" ^"quoted'value^" ^"percent^%value^"`},
		},
		{
			name:     "windows powershell",
			goos:     "windows",
			shell:    "pwsh",
			wantName: "pwsh",
			wantArgs: []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", "claude '-p' '--model' 'fable' 'Bash(git *)' 'quoted''value' 'percent%value'"},
		},
		{
			name:     "windows posix shell",
			goos:     "windows",
			shell:    "bash",
			wantName: "bash",
			wantArgs: []string{"-c", "claude '-p' '--model' 'fable' 'Bash(git *)' 'quoted'\\''value' 'percent%value'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := CommandSpecWithArgsForOS("claude", tt.shell, args, tt.goos)
			if got.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", got.Name, tt.wantName)
			}
			if !reflect.DeepEqual(got.Args, tt.wantArgs) {
				t.Fatalf("Args = %#v, want %#v", got.Args, tt.wantArgs)
			}
		})
	}
}

func TestCommandBuildsExecCommand(t *testing.T) {
	t.Parallel()

	cmd := CommandForOS(context.Background(), "echo ok", "bash", "linux")
	if !reflect.DeepEqual(cmd.Args, []string{"bash", "-c", "echo ok"}) {
		t.Fatalf("Args = %#v, want bash -c command", cmd.Args)
	}
}

func TestQuoteCmdArg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arg  string
		want string
	}{
		{name: "empty", want: `^"^"`},
		{name: "spaces", arg: `path with spaces`, want: `^"path^ with^ spaces^"`},
		{name: "embedded quote", arg: `say "hello"`, want: `^"say^ \^"hello\^"^"`},
		{name: "percent", arg: `%PATH%`, want: `^"^%PATH^%^"`},
		{name: "trailing backslash", arg: `C:\path with spaces\`, want: `^"C:\path^ with^ spaces\\^"`},
		{name: "backslash before quote", arg: `a\"b`, want: `^"a\\\^"b^"`},
		{name: "shell syntax", arg: `a&b|c<d>e(f)^!`, want: `^"a^&b^|c^<d^>e^(f^)^^^!^"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := quoteCmdArg(tt.arg); got != tt.want {
				t.Fatalf("quoteCmdArg(%q) = %q, want %q", tt.arg, got, tt.want)
			}
		})
	}
}
