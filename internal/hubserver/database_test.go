package hubserver

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/digitaldrywood/detent/internal/instancelock"
)

const (
	testTimestamp     = "2026-09-01T12:00:00Z"
	testHubAdminToken = "detent_test_hub_admin_token_00000000000000000000000000000000"
)

func TestOpenCreatesHubSchemaAndConfiguresSQLite(t *testing.T) {
	t.Parallel()

	service := openTestService(t, Config{
		DatabasePath: filepath.Join(t.TempDir(), "hub.db"),
		BusyTimeout:  2500 * time.Millisecond,
	})

	rows, err := service.database.db.QueryContext(t.Context(), "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatalf("query schema tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan schema table: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema tables: %v", err)
	}

	wantTables := []string{
		"artifact_services",
		"artifact_references",
		"artifact_grants",
		"change_evidence",
		"change_issue_links",
		"change_requests",
		"change_review_policies",
		"change_versions",
		"change_viewed_files",
		"github_cutovers",
		"github_import_records",
		"github_imports",
		"collaboration_events",
		"collaboration_versions",
		"hub_identity",
		"hosted_tenant",
		"hosted_members",
		"hosted_project_grants",
		"hosted_sessions",
		"hosted_transactions",
		"hosted_invitations",
		"hosted_audit",
		"native_commands",
		"native_comments",
		"organizations",
		"projects",
		"token_grants",
		"api_tokens",
		"github_hydration_requests",
		"github_outbox",
		"github_webhook_inbox",
		"github_webhook_payloads",
		"hub_schema_version",
		"issue_dependencies",
		"issues",
		"leases",
		"lease_policies",
		"lease_runners",
		"policy_revisions",
		"project_policies",
		"provider_reservations",
		"machines",
		"native_attempts",
		"native_attempt_events",
		"pull_requests",
		"queue_entries",
		"repositories",
		"runner_enrollment_projects",
		"runner_enrollments",
		"runner_identities",
		"runner_identity_events",
		"sync_checkpoints",
		"work_events",
		"workflow_states",
	}
	sort.Strings(wantTables)
	if strings.Join(tables, ",") != strings.Join(wantTables, ",") {
		t.Fatalf("tables = %v, want %v", tables, wantTables)
	}

	checks := []struct {
		name  string
		query string
		want  string
	}{
		{name: "journal mode", query: "PRAGMA journal_mode", want: "wal"},
		{name: "foreign keys", query: "PRAGMA foreign_keys", want: "1"},
		{name: "busy timeout", query: "PRAGMA busy_timeout", want: "2500"},
		{name: "locking mode", query: "PRAGMA locking_mode", want: "exclusive"},
		{name: "synchronous mode", query: "PRAGMA synchronous", want: "2"},
		{name: "application id", query: "PRAGMA application_id", want: strconv.Itoa(hubApplicationID)},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			var got string
			if err := service.database.db.QueryRowContext(t.Context(), check.query).Scan(&got); err != nil {
				t.Fatalf("query %s: %v", check.name, err)
			}
			if !strings.EqualFold(got, check.want) {
				t.Fatalf("%s = %q, want %q", check.name, got, check.want)
			}
		})
	}

	if service.database.schemaVersion != supportedSchemaVersion {
		t.Fatalf("schema version = %d, want %d", service.database.schemaVersion, supportedSchemaVersion)
	}
}

func TestOpenRejectsInvalidDatabasePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "memory", path: ":memory:"},
		{name: "URI", path: "file:hub.db"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, err := Open(t.Context(), Config{DatabasePath: test.path, Logger: discardLogger()})
			if err == nil {
				service.Close()
				t.Fatal("Open() error = nil, want validation error")
			}
		})
	}
}

func TestOpenRejectsNetworkFilesystemBeforeTakingOwnership(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	resolvedDirectory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	path := filepath.Join(directory, "hub.db")
	service, err := Open(t.Context(), Config{
		DatabasePath: path,
		Logger:       discardLogger(),
		validateDatabaseFilesystem: func(gotDirectory string) error {
			if gotDirectory != resolvedDirectory {
				t.Fatalf("filesystem directory = %q, want %q", gotDirectory, resolvedDirectory)
			}
			return ErrNetworkFilesystem
		},
	})
	if err == nil {
		service.Close()
		t.Fatal("Open() error = nil, want network filesystem error")
	}
	if !errors.Is(err, ErrNetworkFilesystem) {
		t.Fatalf("Open() error = %v, want ErrNetworkFilesystem", err)
	}
	for _, candidate := range []string{path, path + ".lock"} {
		if _, statErr := os.Stat(candidate); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("os.Stat(%q) error = %v, want file not to exist", candidate, statErr)
		}
	}
}

func TestOpenCreatesPrivateDatabaseFiles(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose POSIX file modes")
	}

	path := filepath.Join(t.TempDir(), "hub.db")
	openTestService(t, Config{DatabasePath: path})

	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(candidate)
		if errors.Is(err, os.ErrNotExist) && candidate != path {
			continue
		}
		if err != nil {
			t.Fatalf("os.Stat(%q) error = %v", candidate, err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Errorf("%s permissions = %o, want 600", filepath.Base(candidate), got)
		}
	}
}

func TestSQLiteDSNEncodesAbsolutePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantPrefix string
	}{
		{name: "Unix", path: "/var/lib/detent/hub.db", wantPrefix: "file:///var/lib/detent/hub.db?"},
		{name: "Windows uppercase drive", path: "C:/detent/hub.db", wantPrefix: "file:///C:/detent/hub.db?"},
		{name: "Windows lowercase drive", path: "d:/detent/hub.db", wantPrefix: "file:///d:/detent/hub.db?"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := sqliteDSN(test.path, defaultBusyTimeout)
			if !strings.HasPrefix(got, test.wantPrefix) {
				t.Fatalf("sqliteDSN() = %q, want prefix %q", got, test.wantPrefix)
			}
		})
	}
}

func TestOpenRejectsUnrecognizedDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*testing.T, *sql.DB)
	}{
		{
			name: "wrong application id",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), "PRAGMA application_id = 7"); err != nil {
					t.Fatalf("set application id: %v", err)
				}
			},
		},
		{
			name: "existing unrecognized table",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatalf("create unrelated table: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "database.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("sql.Open() error = %v", err)
			}
			test.setup(t, db)
			if err := db.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			service, err := Open(t.Context(), Config{DatabasePath: path, Logger: discardLogger()})
			if err == nil {
				service.Close()
				t.Fatal("Open() error = nil, want identity error")
			}
			if !errors.Is(err, ErrDatabaseIdentity) {
				t.Fatalf("Open() error = %v, want ErrDatabaseIdentity", err)
			}
		})
	}
}

func TestOpenRejectsNewerSchema(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hub.db")
	service := openTestService(t, Config{DatabasePath: path})
	if err := service.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO hub_schema_version (version_id, is_applied) VALUES (?, 1)", supportedSchemaVersion+1); err != nil {
		t.Fatalf("insert future schema version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	service, err = Open(t.Context(), Config{DatabasePath: path, Logger: discardLogger()})
	if err == nil {
		service.Close()
		t.Fatal("Open() error = nil, want unsupported schema error")
	}
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Open() error = %v, want ErrUnsupportedSchema", err)
	}
}

func TestOpenMigratesLegacyWebhookPayloads(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hub.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("PRAGMA application_id = %d", hubApplicationID)); err != nil {
		t.Fatalf("set application id: %v", err)
	}
	legacyMigrations := fstest.MapFS{}
	for _, name := range []string{
		"00001_create_github_projection.sql",
		"00002_create_execution_state.sql",
		"00003_create_github_delivery.sql",
	} {
		data, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		legacyMigrations[name] = &fstest.MapFile{Data: data}
	}
	provider, err := goose.NewProvider(
		goose.DialectSQLite3,
		db,
		legacyMigrations,
		goose.WithDisableGlobalRegistry(true),
		goose.WithTableName(hubSchemaTable),
		goose.WithSlog(discardLogger()),
	)
	if err != nil {
		t.Fatalf("create legacy migration provider: %v", err)
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO github_webhook_inbox (
			delivery_id, event_type, action, headers_json, payload_json,
			payload_sha256, payload_bytes, status, received_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "legacy-delivery", "push", "created", `{"user_agent":"GitHub-Hookshot"}`, `{"ref":"refs/heads/main"}`,
		"legacy-sha", 25, "pending", testTimestamp, testTimestamp, testTimestamp); err != nil {
		t.Fatalf("insert legacy webhook: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	service := openTestService(t, Config{
		DatabasePath: path,
		now: func() time.Time {
			return time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
		},
	})
	var eventType string
	var action string
	var headers string
	var body string
	var payloadSHA string
	var lastReceivedAt string
	if err := service.database.db.QueryRowContext(t.Context(), `
		SELECT i.event_type, i.action, i.headers_json, CAST(p.body AS TEXT), i.payload_sha256, i.last_received_at
		FROM github_webhook_inbox i
		JOIN github_webhook_payloads p ON p.inbox_id = i.id
		WHERE i.delivery_id = 'legacy-delivery'
	`).Scan(&eventType, &action, &headers, &body, &payloadSHA, &lastReceivedAt); err != nil {
		t.Fatalf("read migrated webhook: %v", err)
	}
	if eventType != "push" || action != "created" || headers != `{"user_agent":"GitHub-Hookshot"}` ||
		body != `{"ref":"refs/heads/main"}` || payloadSHA != "legacy-sha" || lastReceivedAt != testTimestamp {
		t.Fatalf("migrated webhook = event %q action %q headers %s body %s sha %q received %q", eventType, action, headers, body, payloadSHA, lastReceivedAt)
	}
}

func TestSchemaEnforcesIdentityAndAppendOnlyConstraints(t *testing.T) {
	t.Parallel()

	service := openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")})
	db := service.database.db
	repositoryID, issueID := seedProjection(t, db)

	duplicateTests := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name:  "repository GitHub node id",
			query: "INSERT INTO repositories (github_node_id, github_owner, github_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
			args:  []any{"R_repo", "other", "repo", testTimestamp, testTimestamp},
		},
		{
			name:  "issue GitHub node id",
			query: "INSERT INTO issues (repository_id, github_node_id, github_number, title, url, github_state, source_version, source_updated_at, synchronized_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			args:  []any{repositoryID, "I_issue", 2, "Other", "https://example.test/2", "open", "v2", testTimestamp, testTimestamp, testTimestamp, testTimestamp},
		},
		{
			name:  "workflow state GitHub node id",
			query: "INSERT INTO workflow_states (repository_id, github_node_id, source_name, detent_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			args:  []any{repositoryID, "WS_todo", "Other", "Other", testTimestamp, testTimestamp},
		},
		{
			name:  "pull request GitHub node id",
			query: "INSERT INTO pull_requests (repository_id, github_node_id, github_number, title, url, github_state, head_ref, head_sha, base_ref, source_version, source_updated_at, synchronized_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			args:  []any{repositoryID, "PR_one", 2, "Other", "https://example.test/pr/2", "open", "other", "def", "main", "v2", testTimestamp, testTimestamp, testTimestamp, testTimestamp},
		},
		{
			name:  "webhook delivery id",
			query: "INSERT INTO github_webhook_inbox (delivery_id, event_type, payload_sha256, payload_bytes, status, received_at, last_received_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			args:  []any{"delivery-one", "issues", "def", 2, "pending", testTimestamp, testTimestamp, testTimestamp, testTimestamp},
		},
	}
	for _, test := range duplicateTests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(t.Context(), test.query, test.args...); err == nil {
				t.Fatal("duplicate insert error = nil, want unique constraint error")
			}
		})
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO github_hydration_requests (
			repository_id, repository_full_name, object_kind, object_key,
			github_number, reason, requested_source_version,
			first_delivery_id, last_delivery_id, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, repositoryID, "digitaldrywood/detent", "repository", "detent", 1, "invalid", "v1",
		"delivery-one", "delivery-one", "pending", testTimestamp, testTimestamp); err == nil {
		t.Fatal("invalid hydration object kind error = nil, want constraint error")
	}

	if _, err := db.ExecContext(t.Context(), "INSERT INTO issues (repository_id, github_node_id, github_number, title, url, github_state, source_version, source_updated_at, synchronized_at, created_at, updated_at) VALUES (9999, 'I_missing', 9, 'Missing', 'https://example.test/9', 'open', 'v1', ?, ?, ?, ?)", testTimestamp, testTimestamp, testTimestamp, testTimestamp); err == nil {
		t.Fatal("foreign key insert error = nil, want constraint error")
	}

	if _, err := db.ExecContext(t.Context(), "INSERT INTO machines (id, hostname, capacity, version, last_heartbeat_at, registered_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)", "machine-one", "builder", 2, "v1", testTimestamp, testTimestamp, testTimestamp); err != nil {
		t.Fatalf("insert machine: %v", err)
	}
	firstLease, err := db.ExecContext(t.Context(), "INSERT INTO leases (lease_id, issue_id, machine_id, session_id, expires_at, acquired_at, renewed_at, released_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", "lease-one", issueID, "machine-one", "session-one", testTimestamp, testTimestamp, testTimestamp, testTimestamp, testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert first lease: %v", err)
	}
	firstToken, err := firstLease.LastInsertId()
	if err != nil {
		t.Fatalf("first lease token: %v", err)
	}
	secondLease, err := db.ExecContext(t.Context(), "INSERT INTO leases (lease_id, issue_id, machine_id, session_id, expires_at, acquired_at, renewed_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", "lease-two", issueID, "machine-one", "session-two", testTimestamp, testTimestamp, testTimestamp, testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert second lease: %v", err)
	}
	secondToken, err := secondLease.LastInsertId()
	if err != nil {
		t.Fatalf("second lease token: %v", err)
	}
	if secondToken <= firstToken {
		t.Fatalf("second fencing token = %d, want greater than %d", secondToken, firstToken)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO leases (lease_id, issue_id, machine_id, session_id, expires_at, acquired_at, renewed_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", "lease-three", issueID, "machine-one", "session-three", testTimestamp, testTimestamp, testTimestamp, testTimestamp, testTimestamp); err == nil {
		t.Fatal("second active lease error = nil, want unique constraint error")
	}

	event, err := db.ExecContext(t.Context(), "INSERT INTO work_events (issue_id, fencing_token, machine_id, session_id, kind, payload_json, occurred_at, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", issueID, secondToken, "machine-one", "session-two", "progress", "{}", testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert work event: %v", err)
	}
	eventID, err := event.LastInsertId()
	if err != nil {
		t.Fatalf("work event id: %v", err)
	}
	for _, mutation := range []string{
		"UPDATE work_events SET kind = 'changed' WHERE id = ?",
		"DELETE FROM work_events WHERE id = ?",
	} {
		if _, err := db.ExecContext(t.Context(), mutation, eventID); err == nil {
			t.Fatalf("%s error = nil, want append-only constraint error", mutation)
		}
	}
}

func TestOpenExcludesAnotherHubProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHubOwnerHelperProcess$")
	command.Env = append(os.Environ(), "DETENT_HUB_OWNER_HELPER="+path)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe() error = %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() error = %v", err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waited := false
	t.Cleanup(func() {
		stdin.Close()
		if !waited {
			command.Wait()
		}
	})

	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read helper readiness: %v, stderr = %s", err, stderr.String())
	}
	if strings.TrimSpace(ready) != "ready" {
		t.Fatalf("helper readiness = %q, want ready", ready)
	}

	service, err := Open(t.Context(), Config{DatabasePath: path, Logger: discardLogger()})
	if err == nil {
		service.Close()
		t.Fatal("Open() error = nil, want ownership error")
	}
	if !errors.Is(err, instancelock.ErrHeld) {
		t.Fatalf("Open() error = %v, want instance lock held", err)
	}

	if err := stdin.Close(); err != nil {
		t.Fatalf("close helper stdin: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("helper process error = %v, stderr = %s", err, stderr.String())
	}
	waited = true

	service = openTestService(t, Config{DatabasePath: path})
	if service.database.path == "" {
		t.Fatal("reopened database path is empty")
	}
}

func TestHubOwnerHelperProcess(t *testing.T) {
	path := os.Getenv("DETENT_HUB_OWNER_HELPER")
	if path == "" {
		return
	}
	service, err := Open(t.Context(), Config{DatabasePath: path, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatalf("write readiness: %v", err)
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatalf("wait for parent: %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestOnlineBackupPreservesSchemaAndData(t *testing.T) {
	t.Parallel()

	service := openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")})
	repositoryID, _ := seedProjection(t, service.database.db)
	backupPath := filepath.Join(t.TempDir(), "backups", "hub.db")
	if err := service.Backup(t.Context(), backupPath); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if err := service.database.health(t.Context()); err != nil {
		t.Fatalf("source health after backup: %v", err)
	}

	backup, err := sql.Open("sqlite", backupPath)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	t.Cleanup(func() {
		if err := backup.Close(); err != nil {
			t.Fatalf("close backup: %v", err)
		}
	})
	var integrity string
	if err := backup.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("backup integrity check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("backup integrity = %q, want ok", integrity)
	}
	var gotRepositoryID int64
	if err := backup.QueryRowContext(t.Context(), "SELECT id FROM repositories WHERE github_node_id = 'R_repo'").Scan(&gotRepositoryID); err != nil {
		t.Fatalf("query backed up repository: %v", err)
	}
	if gotRepositoryID != repositoryID {
		t.Fatalf("backed up repository id = %d, want %d", gotRepositoryID, repositoryID)
	}
	version, err := currentSchemaVersion(t.Context(), backup)
	if err != nil {
		t.Fatalf("backup schema version: %v", err)
	}
	if version != supportedSchemaVersion {
		t.Fatalf("backup schema version = %d, want %d", version, supportedSchemaVersion)
	}
	var applicationID int64
	if err := backup.QueryRowContext(t.Context(), "PRAGMA application_id").Scan(&applicationID); err != nil {
		t.Fatalf("backup application id: %v", err)
	}
	if applicationID != hubApplicationID {
		t.Fatalf("backup application id = %d, want %d", applicationID, hubApplicationID)
	}
}

func TestBackupRejectsUnsafeDestinations(t *testing.T) {
	t.Parallel()

	service := openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")})
	if err := service.Backup(t.Context(), service.database.path); !errors.Is(err, ErrBackupSource) {
		t.Fatalf("Backup(source) error = %v, want ErrBackupSource", err)
	}
	existing := filepath.Join(t.TempDir(), "existing.db")
	if err := os.WriteFile(existing, []byte("preserve"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := service.Backup(t.Context(), existing); !errors.Is(err, os.ErrExist) {
		t.Fatalf("Backup(existing) error = %v, want os.ErrExist", err)
	}
	content, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(content) != "preserve" {
		t.Fatalf("existing backup content = %q, want preserve", content)
	}
}

func TestBackupCancellationRemovesPartialDestination(t *testing.T) {
	t.Parallel()

	service := openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")})
	destination := filepath.Join(t.TempDir(), "backup.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Backup(ctx, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("Backup() error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup destination Stat() error = %v, want os.ErrNotExist", err)
	}
}

func openTestService(t *testing.T, cfg Config) *Service {
	t.Helper()
	cfg.Logger = discardLogger()
	if len(cfg.InitialAdminToken) == 0 {
		cfg.InitialAdminToken = []byte(testHubAdminToken)
	}
	service, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return service
}

func authorizeHubTestRequest(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+testHubAdminToken)
}

func seedProjection(t *testing.T, db *sql.DB) (int64, int64) {
	t.Helper()
	repository, err := db.ExecContext(t.Context(), "INSERT INTO repositories (github_node_id, github_owner, github_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", "R_repo", "digitaldrywood", "detent", testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert repository: %v", err)
	}
	repositoryID, err := repository.LastInsertId()
	if err != nil {
		t.Fatalf("repository id: %v", err)
	}
	workflow, err := db.ExecContext(t.Context(), "INSERT INTO workflow_states (repository_id, github_node_id, source_name, detent_state, dispatchable, created_at, updated_at) VALUES (?, ?, ?, ?, 1, ?, ?)", repositoryID, "WS_todo", "Todo", "Todo", testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert workflow state: %v", err)
	}
	workflowID, err := workflow.LastInsertId()
	if err != nil {
		t.Fatalf("workflow state id: %v", err)
	}
	issue, err := db.ExecContext(t.Context(), "INSERT INTO issues (repository_id, workflow_state_id, github_node_id, github_number, title, url, github_state, source_version, source_updated_at, synchronized_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", repositoryID, workflowID, "I_issue", 1, "Issue", "https://example.test/1", "open", "v1", testTimestamp, testTimestamp, testTimestamp, testTimestamp)
	if err != nil {
		t.Fatalf("insert issue: %v", err)
	}
	issueID, err := issue.LastInsertId()
	if err != nil {
		t.Fatalf("issue id: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO pull_requests (repository_id, issue_id, github_node_id, github_number, title, url, github_state, head_ref, head_sha, base_ref, source_version, source_updated_at, synchronized_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", repositoryID, issueID, "PR_one", 1, "PR", "https://example.test/pr/1", "open", "feature", "abc", "main", "v1", testTimestamp, testTimestamp, testTimestamp, testTimestamp); err != nil {
		t.Fatalf("insert pull request: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO github_webhook_inbox (delivery_id, repository_id, event_type, payload_sha256, payload_bytes, status, received_at, last_received_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", "delivery-one", repositoryID, "issues", "abc", 2, "pending", testTimestamp, testTimestamp, testTimestamp, testTimestamp); err != nil {
		t.Fatalf("insert webhook delivery: %v", err)
	}
	return repositoryID, issueID
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
