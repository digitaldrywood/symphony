package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/digitaldrywood/detent/internal/artifact"
)

func TestArtifactMaintenanceCommands(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, operation := range []string{"usage", "backup", "invalid config", "public listener"} {
		t.Run(operation, func(t *testing.T) {
			cfg := artifact.Config{ServiceID: artifact.NewID("service"), OrganizationID: artifact.NewID("org"), Mode: "customer", DatabasePath: filepath.Join(t.TempDir(), "catalog.db"), HubOrigin: "https://hub.example.com", PublishTokenEnv: "PUBLISHER", Policy: artifact.Policy{ID: "test", Limits: artifact.Limits{RetainedBytes: 4 << 20, ReservedBytes: 4 << 20, ArtifactBytes: 2 << 20, UploadBytes: 1 << 20, RetentionSeconds: 60}, AbandonedUploadSeconds: 30, DeletionRecordSeconds: 120, BackupSeconds: 60}, Storage: artifact.StorageConfig{Kind: "s3", Region: "us-east-1", Bucket: "example"}}
			if operation == "invalid config" {
				cfg.Policy.BackupSeconds = 0
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(t.TempDir(), "backup.db")
			args := []string{"--artifact-config", path}
			switch operation {
			case "backup":
				args = append(args, "backup", "--output", destination)
			case "public listener":
				args = append(args, "serve", "--listen", "0.0.0.0:7788")
			default:
				args = append(args, "usage")
			}
			cmd := newArtifactCommand(func(string) string { return "test" })
			cmd.SetArgs(args)
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			err = cmd.ExecuteContext(t.Context())
			if operation == "invalid config" || operation == "public listener" {
				if err == nil {
					t.Fatal("invalid configuration accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if operation == "usage" && out.String() != "{\"retained_bytes\":0,\"reserved_bytes\":0}\n" {
				t.Fatal(out.String())
			}
			if operation == "backup" {
				if _, err := os.Stat(destination); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
