package hubserver

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3"
)

func TestRecoveryInterruptedMigration(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"schema", "version"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "older.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			files, err := migrationFiles.ReadDir("migrations")
			if err != nil {
				t.Fatal(err)
			}
			migrations := fstest.MapFS{}
			for _, file := range files {
				if file.Name() >= "00016_" {
					continue
				}
				data, err := migrationFiles.ReadFile("migrations/" + file.Name())
				if err != nil {
					t.Fatal(err)
				}
				migrations[file.Name()] = &fstest.MapFile{Data: data}
			}
			provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrations, goose.WithDisableGlobalRegistry(true), goose.WithTableName(hubSchemaTable), goose.WithSlog(discardLogger()))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := provider.Up(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA application_id = %d", hubApplicationID)); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRecoveryMigrationProcess$")
			command.Env = append(os.Environ(), "DETENT_TEST_MIGRATION_PATH="+path, "DETENT_TEST_MIGRATION_STAGE="+stage)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
				t.Fatalf("interruption helper: %v: %s", err, output)
			}
			verified, err := VerifyDatabase(t.Context(), path)
			if err != nil || verified.SchemaVersion != 15 {
				t.Fatalf("interrupted migration did not roll back: %+v, %v", verified, err)
			}
			backup := filepath.Join(t.TempDir(), "pre-upgrade.db")
			if err := BackupDatabase(t.Context(), path, backup); err != nil {
				t.Fatal(err)
			}
			verified, err = VerifyDatabase(t.Context(), backup)
			if err != nil || verified.SchemaVersion != 15 {
				t.Fatalf("backup migrated the source: %+v, %v", verified, err)
			}
			restored := filepath.Join(t.TempDir(), "restored.db")
			if _, err := RestoreDatabase(t.Context(), backup, restored, []byte(recoveryTestToken)); err != nil {
				t.Fatal(err)
			}
			verified, err = VerifyDatabase(t.Context(), restored)
			if err != nil || verified.SchemaVersion != supportedSchemaVersion {
				t.Fatalf("migration retry: %+v, %v", verified, err)
			}
		})
	}
}

func TestRecoveryMigrationProcess(t *testing.T) {
	path := os.Getenv("DETENT_TEST_MIGRATION_PATH")
	if path == "" {
		return
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode = WAL"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := migrationFiles.ReadFile("migrations/00016_artifacts.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), string(data)); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("DETENT_TEST_MIGRATION_STAGE") == "version" {
		if _, err := tx.ExecContext(t.Context(), "INSERT INTO hub_schema_version(version_id,is_applied) VALUES(16,1)"); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(23)
}
