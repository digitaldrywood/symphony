package cli

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/hubserver"
)

func TestHubRecoveryCommands(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "hub.db")
	service, err := hubserver.Open(t.Context(), hubserver.Config{DatabasePath: path, GitHubDisabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	restored := filepath.Join(t.TempDir(), "restored.db")
	const token = "example-fresh-administrator-token-for-recovery"
	for _, test := range []struct {
		name string
		args []string
		fail bool
	}{
		{"missing source", []string{"verify"}, true},
		{"missing output", []string{"backup", "--database", path}, true},
		{"verify", []string{"verify", "--database", path}, false},
		{"backup", []string{"backup", "--database", path, "--output", backup}, false},
		{"existing backup", []string{"backup", "--database", path, "--output", backup}, true},
		{"invalid environment", []string{"restore", "--database", backup, "--output", restored, "--admin-token-env", "BAD=ENV"}, true},
		{"restore", []string{"restore", "--database", backup, "--output", restored}, false},
		{"verify restore", []string{"verify", "--database", restored}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := newHubCommandWithRun("test", func(string) string { return token }, nil)
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs(test.args)
			err := cmd.ExecuteContext(t.Context())
			if (err != nil) != test.fail {
				t.Fatalf("error = %v, output = %s", err, &output)
			}
			if strings.Contains(output.String(), token) {
				t.Fatal("command exposed administrator token")
			}
			if !test.fail && test.args[0] != "backup" {
				var result hubserver.RecoveryResult
				if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.SchemaVersion == 0 {
					t.Fatalf("invalid recovery output: %s, %v", &output, err)
				}
			}
		})
	}
	service, err = hubserver.Open(t.Context(), hubserver.Config{DatabasePath: restored, GitHubDisabled: true, InitialAdminToken: []byte(token)})
	if err != nil {
		t.Fatalf("start restored Hub without an original bootstrap principal: %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}
