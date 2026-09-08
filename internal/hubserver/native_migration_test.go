package hubserver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3"
)

func TestHubMigrationPreservesExistingData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		version        int64
		hostedSessions string
		viewed         bool
		onboarding     bool
	}{
		{"artifacts", 16, "0", false, false},
		{"hosted identity", 17, "1", false, false},
		{"viewed files version 18", 18, "1", true, false},
		{"onboarding version 18", 18, "1", false, true},
		{"both tables version 18", 18, "1", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "hub.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA application_id = %d", hubApplicationID)); err != nil {
				t.Fatal(err)
			}
			files, err := migrationFiles.ReadDir("migrations")
			if err != nil {
				t.Fatal(err)
			}
			migrations := fstest.MapFS{}
			for _, file := range files {
				if file.Name() >= "00018_" && file.Name() != "00018_change_viewed_files.sql" {
					continue
				}
				if file.Name() == "00018_change_viewed_files.sql" && !test.viewed {
					continue
				}
				data, err := migrationFiles.ReadFile("migrations/" + file.Name())
				if err != nil {
					t.Fatal(err)
				}
				migrations[file.Name()] = &fstest.MapFile{Data: data}
			}
			if test.onboarding {
				data, err := os.ReadFile("testdata/migrations/00018_project_onboarding.sql")
				if err != nil {
					t.Fatal(err)
				}
				if test.viewed {
					migrations["00018_change_viewed_files.sql"].Data = append(migrations["00018_change_viewed_files.sql"].Data, []byte(strings.TrimPrefix(strings.Split(string(data), "-- +goose Down")[0], "-- +goose Up\n"))...)
				} else {
					migrations["00018_project_onboarding.sql"] = &fstest.MapFile{Data: data}
				}
			}
			provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrations, goose.WithDisableGlobalRegistry(true), goose.WithTableName(hubSchemaTable), goose.WithSlog(discardLogger()))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := provider.UpTo(t.Context(), 7); err != nil {
				t.Fatal(err)
			}
			_, issueID := seedProjection(t, db)
			if _, err := provider.UpTo(t.Context(), test.version); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `INSERT INTO change_requests (id, organization_id, project_id, work_item_id, record_json)
				SELECT 'existing-change', organization_id, project_id, native_id, '{}' FROM issues WHERE id = ?`, issueID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `INSERT INTO change_versions (id, change_id, number, record_json) VALUES ('existing-version', 'existing-change', 1, '{"head_sha":"preserved"}')`); err != nil {
				t.Fatal(err)
			}
			if test.version >= 17 {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO hosted_sessions (token_hash, email, identity_json, expires_at, created_at) VALUES ('existing-session', 'reviewer@example.test', '{}', ?, ?)`, testTimestamp, testTimestamp); err != nil {
					t.Fatal(err)
				}
			}
			if test.viewed {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO change_viewed_files (version_id, principal_id, manifest_sha256, file_sha256, viewed) VALUES ('existing-version', 'reviewer', ?, ?, 1)`, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
					t.Fatal(err)
				}
			}
			if test.onboarding {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO project_onboarding (organization_id, project_id, revision, progress_json, updated_at) SELECT organization_id, project_id, 7, '{"policy_approved":true}', ? FROM issues WHERE id = ?`, testTimestamp, issueID); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := Config{DatabasePath: path}
			service := openTestService(t, cfg)
			if !test.viewed {
				if _, err := service.database.db.ExecContext(t.Context(), `INSERT INTO change_viewed_files (version_id, principal_id, manifest_sha256, file_sha256, viewed) VALUES ('existing-version', 'reviewer', ?, ?, 1)`, strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
					t.Fatal(err)
				}
			}
			if !test.onboarding {
				if _, err := service.database.db.ExecContext(t.Context(), `INSERT INTO project_onboarding (organization_id, project_id, revision, progress_json, updated_at) SELECT organization_id, project_id, 7, '{"policy_approved":true}', ? FROM issues WHERE id = ?`, testTimestamp, issueID); err != nil {
					t.Fatal(err)
				}
			}
			if err := service.Close(); err != nil {
				t.Fatal(err)
			}
			service = openTestService(t, cfg)
			for _, check := range []struct{ name, query, want string }{
				{"schema version", "SELECT max(version_id) FROM hub_schema_version WHERE is_applied = 1", strconv.FormatInt(supportedSchemaVersion, 10)},
				{"onboarding migration", "SELECT count(*) FROM hub_schema_version WHERE version_id = 19 AND is_applied = 1", "1"},
				{"onboarding progress", "SELECT progress_json FROM project_onboarding", `{"policy_approved":true}`},
				{"onboarding revision", "SELECT revision FROM project_onboarding", "7"},
				{"onboarding timestamp", "SELECT updated_at FROM project_onboarding", testTimestamp},
				{"identity migration", "SELECT count(*) FROM hub_schema_version WHERE version_id = 17 AND is_applied = 1", "1"},
				{"version 18 recorded once", "SELECT count(*) FROM hub_schema_version WHERE version_id = 18 AND is_applied = 1", "1"},
				{"existing code version", "SELECT record_json FROM change_versions WHERE id = 'existing-version'", `{"head_sha":"preserved"}`},
				{"viewed state", "SELECT viewed FROM change_viewed_files WHERE version_id = 'existing-version' AND principal_id = 'reviewer'", "1"},
				{"hosted sessions", "SELECT count(*) FROM hosted_sessions WHERE token_hash = 'existing-session' AND email = 'reviewer@example.test'", test.hostedSessions},
				{"foreign keys", "SELECT count(*) FROM pragma_foreign_key_check", "0"},
				{"foreign key enforcement", "PRAGMA foreign_keys", "1"},
				{"integrity", "PRAGMA integrity_check", "ok"},
			} {
				t.Run(check.name, func(t *testing.T) {
					var got string
					if err := service.database.db.QueryRowContext(t.Context(), check.query).Scan(&got); err != nil {
						t.Fatal(err)
					}
					if got != check.want {
						t.Fatalf("got %q, want %q", got, check.want)
					}
				})
			}
		})
	}
}

func TestNativeMigrationPreservesCompatibilityIdentity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "hub.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA application_id = %d", hubApplicationID)); err != nil {
		t.Fatal(err)
	}
	files, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	migrations := fstest.MapFS{}
	for _, file := range files {
		if file.Name() >= "00008_" {
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
	repositoryID, issueID := seedProjection(t, db)
	for _, statement := range []string{
		`INSERT INTO machines (id, hostname, capacity, version, last_heartbeat_at, registered_at, updated_at) VALUES ('legacy-machine', 'host', 1, 'test', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z')`,
		fmt.Sprintf(`INSERT INTO leases (lease_id, issue_id, machine_id, session_id, expires_at, acquired_at, renewed_at, created_at, updated_at) VALUES ('legacy-lease', %d, 'legacy-machine', 'legacy-session', '2026-09-01T12:10:00Z', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z')`, issueID),
		fmt.Sprintf(`INSERT INTO work_events (issue_id, fencing_token, kind, payload_json, occurred_at, recorded_at) VALUES (%d, 1, 'legacy-event', '{"reference":"preserved"}', '2026-09-01T12:00:00Z', '2026-09-01T12:00:00Z')`, issueID),
	} {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	service := openTestService(t, Config{DatabasePath: path})
	var nativeID, organizationID, projectID string
	if err := service.database.db.QueryRowContext(t.Context(), "SELECT native_id, organization_id, project_id FROM issues WHERE id = ?", issueID).Scan(&nativeID, &organizationID, &projectID); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, query, want string }{
		{"repository", fmt.Sprintf("SELECT repository_id FROM issues WHERE id = %d", issueID), strconv.FormatInt(repositoryID, 10)},
		{"GitHub identity", fmt.Sprintf("SELECT github_node_id FROM issues WHERE id = %d", issueID), "I_issue"},
		{"number", fmt.Sprintf("SELECT number FROM issues WHERE id = %d", issueID), "1"},
		{"compatibility profile", "SELECT profile FROM projects WHERE id = '" + projectID + "'", "github_compatible"},
		{"lease", "SELECT issue_id FROM leases WHERE lease_id = 'legacy-lease'", strconv.FormatInt(issueID, 10)},
		{"fencing", "SELECT fencing_token FROM leases WHERE lease_id = 'legacy-lease'", "1"},
		{"history", "SELECT payload_json FROM work_events WHERE kind = 'legacy-event'", `{"reference":"preserved"}`},
		{"foreign keys", "SELECT count(*) FROM pragma_foreign_key_check", "0"},
		{"foreign key enforcement", "PRAGMA foreign_keys", "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			if err := service.database.db.QueryRowContext(t.Context(), test.query).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
	if _, err := service.database.db.ExecContext(t.Context(), "UPDATE issues SET native_id = 'changed' WHERE id = ?", issueID); err == nil {
		t.Fatal("native identity was mutable")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	service = openTestService(t, Config{DatabasePath: path})
	var restartedID string
	if err := service.database.db.QueryRowContext(t.Context(), "SELECT native_id FROM issues WHERE id = ?", issueID).Scan(&restartedID); err != nil {
		t.Fatal(err)
	}
	if restartedID != nativeID {
		t.Fatal("restart changed native identity")
	}
	fixture := newNativeFixture(t, service, "", "coexisting-native")
	fixture.create(t, "native work")
}
