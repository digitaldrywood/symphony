package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
)

func TestOpenSQLiteAppliesMigrationsAndPragmas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")

	backend, err := Open(ctx, Config{
		Backend: BackendSQLite,
		Path:    dbPath,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	sqliteBackend, ok := backend.(*sqliteStore)
	if !ok {
		t.Fatalf("Open() returned %T, want *sqliteStore", backend)
	}

	if got := queryString(t, sqliteBackend.db, "PRAGMA journal_mode"); got != "wal" {
		t.Fatalf("journal_mode = %q, want wal", got)
	}
	if got := queryInt(t, sqliteBackend.db, "PRAGMA busy_timeout"); got != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", got)
	}
	if got := queryInt(t, sqliteBackend.db, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('detent_runs', 'codex_sessions', 'fair_share_usage', 'usage_events', 'workflow_phase_events', 'work_attempts', 'scheduler_decisions', 'merge_required_check_streaks', 'validator_verdicts', 'api_keys', 'api_usage_logs', 'efficiency_receipts', 'budget_overrides', 'auth_magic_links', 'auth_sessions', 'routine_runs', 'project_dispatch_status')"); got != 17 {
		t.Fatalf("migrated table count = %d, want 17", got)
	}
}

func TestProvenanceAttributionTrustBoundaryPersistsAcrossRestart(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	first, err := Open(ctx, Config{Backend: BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	boundary, err := first.ProvenanceAttributionTrustBoundary(ctx)
	if err != nil {
		t.Fatalf("ProvenanceAttributionTrustBoundary(first) error = %v", err)
	}
	if boundary.IsZero() {
		t.Fatal("ProvenanceAttributionTrustBoundary(first) is zero")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	restarted, err := Open(ctx, Config{Backend: BackendSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("Open(restarted) error = %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restartedBoundary, err := restarted.ProvenanceAttributionTrustBoundary(ctx)
	if err != nil {
		t.Fatalf("ProvenanceAttributionTrustBoundary(restarted) error = %v", err)
	}
	if !restartedBoundary.Equal(boundary) {
		t.Fatalf("restarted boundary = %v, want %v", restartedBoundary, boundary)
	}
}

func TestCostPerOutcomeIndexesMigrationUpDown(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "detent.db"))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 21); err != nil {
		t.Fatalf("goose.UpToContext(21) error = %v", err)
	}

	indexes := []string{
		"usage_events_finished_at_idx",
		"usage_events_project_finished_at_idx",
		"efficiency_receipts_completed_at_idx",
	}
	for _, index := range indexes {
		assertIndexAbsent(t, db, index)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 22); err != nil {
		t.Fatalf("goose.UpToContext(22) error = %v", err)
	}
	for _, index := range indexes {
		assertIndexPresent(t, db, index)
	}
	if err := goose.DownToContext(ctx, db, "migrations", 21); err != nil {
		t.Fatalf("goose.DownToContext(21) error = %v", err)
	}
	for _, index := range indexes {
		assertIndexAbsent(t, db, index)
	}
}

func TestDeliveredLaneRevocationMigrationUpDown(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "detent.db"))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 38); err != nil {
		t.Fatalf("goose.UpToContext(38) error = %v", err)
	}

	if _, err := db.ExecContext(ctx, `
INSERT INTO work_attempts (
  id, project_id, worker_type, status, started_at, completed_at,
  terminal_state, error_class, error_message, phase, status_message, worker_metadata_json
) VALUES
  (3295, 'corp', 'implementation', 'terminal', '2026-08-18T20:49:37Z', '2026-08-18T21:10:53Z',
   'lane_revoked', 'lane_revoked', 'tracker_lane_changed', 'lane_revoked', 'worker stopped after leaving a worker-owned lane',
   '{"work_product_pushed":true,"lane_revocation":{"work_discarded":true}}'),
  (3238, 'corp', 'implementation', 'terminal', '2026-08-18T15:59:18Z', '2026-08-18T16:06:21Z',
   'lane_revoked', 'lane_revoked', 'tracker_lane_changed', 'lane_revoked', 'worker stopped after leaving a worker-owned lane',
   '{"work_product_pushed":false,"lane_revocation":{"work_discarded":true}}'),
  (3300, 'corp', 'implementation', 'terminal', '2026-08-18T22:00:00Z', '2026-08-18T22:10:00Z',
   'lane_revoked', 'lane_revoked', 'operator_hold', 'lane_revoked', 'operator stopped the worker',
   '{"work_product_pushed":true}');
`); err != nil {
		t.Fatalf("seed lane revocations error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO codex_sessions (id, work_attempt_id, identifier, started_at, completed_at, final_state) VALUES
  (3337, 3295, 'gopherguides/corp#72', '2026-08-18T20:49:37Z', '2026-08-18T21:10:53Z', 'lane_revoked'),
  (3338, 3238, 'gopherguides/corp#72', '2026-08-18T15:59:18Z', '2026-08-18T16:06:21Z', 'lane_revoked');
`); err != nil {
		t.Fatalf("seed lane revocation sessions error = %v", err)
	}

	if err := goose.UpToContext(ctx, db, "migrations", 39); err != nil {
		t.Fatalf("goose.UpToContext(39) error = %v", err)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 3295"); got != "delivered" {
		t.Fatalf("delivered terminal state = %q, want delivered", got)
	}
	if got := queryString(t, db, "SELECT COALESCE(error_class, '') FROM work_attempts WHERE id = 3295"); got != "" {
		t.Fatalf("delivered error class = %q, want empty", got)
	}
	if got := queryString(t, db, "SELECT json_extract(worker_metadata_json, '$.historical_lane_revocation.classification') FROM work_attempts WHERE id = 3295"); got != "delivered_before_revocation" {
		t.Fatalf("historical classification = %q, want delivered_before_revocation", got)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 3238"); got != "lane_revoked" {
		t.Fatalf("undelivered attempt terminal state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 3300"); got != "lane_revoked" {
		t.Fatalf("operator revocation terminal state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 3337"); got != "delivered" {
		t.Fatalf("delivered session final state = %q, want delivered", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 3338"); got != "lane_revoked" {
		t.Fatalf("undelivered session final state = %q, want lane_revoked", got)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 38); err != nil {
		t.Fatalf("goose.DownToContext(38) error = %v", err)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 3295"); got != "lane_revoked" {
		t.Fatalf("restored terminal state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT error_message FROM work_attempts WHERE id = 3295"); got != "tracker_lane_changed" {
		t.Fatalf("restored error message = %q, want tracker_lane_changed", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 3337"); got != "lane_revoked" {
		t.Fatalf("restored session final state = %q, want lane_revoked", got)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM work_attempts WHERE json_extract(worker_metadata_json, '$.historical_lane_revocation.classification') IS NOT NULL"); got != 0 {
		t.Fatalf("historical classifications after down = %d, want 0", got)
	}
}

func TestLaneRevocationDeliveryReceiptMigrationUpDown(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "detent.db"))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 44); err != nil {
		t.Fatalf("goose.UpToContext(44) error = %v", err)
	}

	if _, err := db.ExecContext(ctx, `
INSERT INTO work_attempts (
  id, project_id, worker_type, status, started_at, completed_at,
  terminal_state, error_class, error_message, phase, status_message, worker_metadata_json
) VALUES
  (4001, 'detent', 'implementation', 'terminal', '2026-08-27T08:00:00Z', '2026-08-27T09:00:00Z',
   'lane_revoked', 'lane_revoked', 'tracker_lane_changed', 'lane_revoked', 'worker stopped after leaving a worker-owned lane',
   '{"work_product_pushed":true,"pr_number":2001,"pr_head_sha":"abc123","lane_revocation":{"work_discarded":true}}'),
  (4002, 'detent', 'implementation', 'terminal', '2026-08-27T08:00:00Z', '2026-08-27T09:00:00Z',
   'lane_revoked', 'lane_revoked', 'tracker_lane_changed', 'lane_revoked', 'worker stopped after leaving a worker-owned lane',
   '{"work_product_pushed":false,"lane_revocation":{"work_discarded":true}}'),
  (4003, 'detent', 'implementation', 'terminal', '2026-08-27T08:00:00Z', '2026-08-27T09:00:00Z',
   'lane_revoked', 'lane_revoked', 'operator_hold', 'lane_revoked', 'operator stopped the worker',
   '{"work_product_pushed":true,"lane_revocation":{"work_discarded":true}}');
INSERT INTO codex_sessions (id, work_attempt_id, identifier, started_at, completed_at, final_state) VALUES
  (4101, 4001, 'digitaldrywood/detent#1998', '2026-08-27T08:00:00Z', '2026-08-27T09:00:00Z', 'lane_revoked'),
  (4102, 4002, 'digitaldrywood/detent#1998', '2026-08-27T08:00:00Z', '2026-08-27T09:00:00Z', 'lane_revoked');
`); err != nil {
		t.Fatalf("seed post-migration lane revocations error = %v", err)
	}

	if err := goose.UpToContext(ctx, db, "migrations", 45); err != nil {
		t.Fatalf("goose.UpToContext(45) error = %v", err)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 4001"); got != "delivered" {
		t.Fatalf("pushed terminal state = %q, want delivered", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 4101"); got != "delivered" {
		t.Fatalf("pushed session final state = %q, want delivered", got)
	}
	if got := queryString(t, db, "SELECT json_extract(worker_metadata_json, '$.delivery_receipt.kind') FROM work_attempts WHERE id = 4001"); got != "pushed_work_product" {
		t.Fatalf("delivery receipt kind = %q, want pushed_work_product", got)
	}
	if got := queryInt(t, db, "SELECT json_extract(worker_metadata_json, '$.lane_revocation.work_discarded') FROM work_attempts WHERE id = 4001"); got != 0 {
		t.Fatalf("pushed work_discarded = %d, want false", got)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 4002"); got != "lane_revoked" {
		t.Fatalf("unpushed terminal state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 4102"); got != "lane_revoked" {
		t.Fatalf("unpushed session final state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 4003"); got != "lane_revoked" {
		t.Fatalf("operator hold terminal state = %q, want lane_revoked", got)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 44); err != nil {
		t.Fatalf("goose.DownToContext(44) error = %v", err)
	}
	if got := queryString(t, db, "SELECT terminal_state FROM work_attempts WHERE id = 4001"); got != "lane_revoked" {
		t.Fatalf("restored pushed terminal state = %q, want lane_revoked", got)
	}
	if got := queryString(t, db, "SELECT final_state FROM codex_sessions WHERE id = 4101"); got != "lane_revoked" {
		t.Fatalf("restored pushed session final state = %q, want lane_revoked", got)
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM work_attempts WHERE json_extract(worker_metadata_json, '$.delivery_receipt.kind') IS NOT NULL"); got != 0 {
		t.Fatalf("delivery receipts after down = %d, want 0", got)
	}
}

func TestCompleteWorkAttemptFinalizesLinkedSessionOutcome(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := openTestStore(t, ctx)
	now := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	attemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:     "detent",
		IssueID:       "issue-1998",
		Identifier:    "digitaldrywood/detent#1998",
		WorkerType:    "implementation",
		AttemptNumber: 1,
		StartedAt:     now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	sessionID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID: attemptID,
		ProjectID:     "detent",
		IssueID:       "issue-1998",
		Identifier:    "digitaldrywood/detent#1998",
		StartedAt:     now.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, sessionID, SessionFinish{
		CompletedAt: now,
		FinalState:  "lane_revoked",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}

	if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{
		AttemptID:         attemptID,
		CompletedAt:       now,
		TerminalState:     WorkAttemptTerminalDelivered,
		SessionFinalState: "delivered",
	}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}

	session, err := backend.Queries().GetCodexSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetCodexSession() error = %v", err)
	}
	if session.FinalState.String != "delivered" {
		t.Fatalf("session final_state = %q, want delivered", session.FinalState.String)
	}
}

func TestCachedTokenTelemetryMigrationUpDown(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 6); err != nil {
		t.Fatalf("goose.UpToContext(6) error = %v", err)
	}

	if _, err := db.ExecContext(ctx, `
INSERT INTO codex_sessions (started_at, input_tokens, output_tokens, total_tokens)
VALUES ('2026-05-31T13:00:00Z', 10, 2, 12);
INSERT INTO usage_events (project_id, model, input_tokens, output_tokens, total_tokens, runtime_seconds, started_at, finished_at, event_day, outcome, cost_usd)
VALUES ('detent', 'gpt-5', 10, 2, 12, 5, '2026-05-31T13:00:00Z', '2026-05-31T13:00:05Z', '2026-05-31', 'completed', 0.001);
INSERT INTO workflow_phase_events (project_id, phase_type, phase_name, started_at, duration_seconds, event_day, input_tokens, output_tokens, total_tokens)
VALUES ('detent', 'agent_session', 'agent_active', '2026-05-31T13:00:00Z', 5, '2026-05-31', 10, 2, 12);
`); err != nil {
		t.Fatalf("seed old schema rows error = %v", err)
	}
	for _, table := range []string{"codex_sessions", "usage_events", "workflow_phase_events"} {
		assertColumnAbsent(t, db, table, "cached_input_tokens")
	}

	if err := goose.UpToContext(ctx, db, "migrations", 8); err != nil {
		t.Fatalf("goose.UpToContext(8) error = %v", err)
	}
	for _, table := range []string{"codex_sessions", "usage_events", "workflow_phase_events"} {
		assertColumnPresent(t, db, table, "cached_input_tokens")
		assertColumnPresent(t, db, table, "reasoning_output_tokens")
		assertColumnPresent(t, db, table, "model_context_window")
		assertTelemetryColumnsNull(t, db, table)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 6); err != nil {
		t.Fatalf("goose.DownToContext(6) error = %v", err)
	}
	for _, table := range []string{"codex_sessions", "usage_events", "workflow_phase_events"} {
		assertColumnAbsent(t, db, table, "cached_input_tokens")
		assertColumnAbsent(t, db, table, "reasoning_output_tokens")
		assertColumnAbsent(t, db, table, "model_context_window")
	}
}

func TestSkillDraftTelemetryMigrationUpDown(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 12); err != nil {
		t.Fatalf("goose.UpToContext(12) error = %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO codex_sessions (started_at) VALUES (?)", "2026-07-10T00:00:00Z"); err != nil {
		t.Fatalf("seed codex session error = %v", err)
	}

	assertColumnAbsent(t, db, "codex_sessions", "skill_draft_proposed")
	if err := goose.UpToContext(ctx, db, "migrations", 13); err != nil {
		t.Fatalf("goose.UpToContext(13) error = %v", err)
	}
	assertColumnPresent(t, db, "codex_sessions", "skill_draft_proposed")
	if got := queryInt(t, db, "SELECT skill_draft_proposed FROM codex_sessions LIMIT 1"); got != 0 {
		t.Fatalf("skill_draft_proposed default = %d, want 0", got)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 12); err != nil {
		t.Fatalf("goose.DownToContext(12) error = %v", err)
	}
	assertColumnAbsent(t, db, "codex_sessions", "skill_draft_proposed")
}

func TestSessionProjectAttributionMigrationUpDown(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "detent.db"))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 16); err != nil {
		t.Fatalf("goose.UpToContext(16) error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO work_attempts (id, project_id, worker_type, status, started_at)
VALUES (1279, 'detent', 'implementation', 'complete', '2026-07-12T19:00:00Z');
INSERT INTO codex_sessions (work_attempt_id, identifier, started_at, completed_at)
VALUES (1279, 'digitaldrywood/detent#1279', '2026-07-12T19:00:00Z', '2026-07-12T20:00:00Z');
`); err != nil {
		t.Fatalf("seed pre-migration rows error = %v", err)
	}
	assertColumnAbsent(t, db, "codex_sessions", "project_id")

	if err := goose.UpToContext(ctx, db, "migrations", 18); err != nil {
		t.Fatalf("goose.UpToContext(18) error = %v", err)
	}
	assertColumnPresent(t, db, "codex_sessions", "project_id")
	if got := queryString(t, db, "SELECT project_id FROM codex_sessions WHERE work_attempt_id = 1279"); got != "detent" {
		t.Fatalf("backfilled project_id = %q, want detent", got)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 17); err != nil {
		t.Fatalf("goose.DownToContext(17) error = %v", err)
	}
	assertColumnAbsent(t, db, "codex_sessions", "project_id")
}

func TestAgentResumeStateMigrationUpDown(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatalf("configureSQLite() error = %v", err)
	}

	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("goose.SetDialect() error = %v", err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 8); err != nil {
		t.Fatalf("goose.UpToContext(8) error = %v", err)
	}

	for _, column := range []string{"requested_model", "agent_backend_id", "agent_backend_kind", "agent_role", "provider_thread_id", "provider_session_id", "resumed_from_session_id"} {
		assertColumnAbsent(t, db, "codex_sessions", column)
	}

	if err := goose.UpToContext(ctx, db, "migrations", 9); err != nil {
		t.Fatalf("goose.UpToContext(9) error = %v", err)
	}
	for _, column := range []string{"requested_model", "agent_backend_id", "agent_backend_kind", "agent_role", "provider_thread_id", "provider_session_id", "resumed_from_session_id"} {
		assertColumnPresent(t, db, "codex_sessions", column)
	}

	if err := goose.DownToContext(ctx, db, "migrations", 8); err != nil {
		t.Fatalf("goose.DownToContext(8) error = %v", err)
	}
	for _, column := range []string{"requested_model", "agent_backend_id", "agent_backend_kind", "agent_role", "provider_thread_id", "provider_session_id", "resumed_from_session_id"} {
		assertColumnAbsent(t, db, "codex_sessions", column)
	}
}

func TestRuntimeEvidenceReportsSQLiteTelemetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	backend, err := Open(ctx, Config{
		Backend: BackendSQLite,
		Path:    dbPath,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	finishedAt := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	if _, err := backend.RecordUsageEvent(ctx, UsageEvent{
		ProjectID:      "detent",
		Model:          "gpt-5-codex",
		StartedAt:      finishedAt.Add(-5 * time.Minute),
		FinishedAt:     finishedAt,
		RuntimeSeconds: 300,
		TotalTokens:    1200,
		Outcome:        "success",
	}); err != nil {
		t.Fatalf("RecordUsageEvent() error = %v", err)
	}
	if _, err := backend.RecordWorkflowPhaseEvent(ctx, WorkflowPhaseEvent{
		ProjectID:       "detent",
		IssueID:         "issue-755",
		Identifier:      "digitaldrywood/detent#755",
		PhaseType:       WorkflowPhaseTypeLane,
		PhaseName:       "In Progress",
		Status:          "completed",
		StartedAt:       finishedAt.Add(-30 * time.Minute),
		FinishedAt:      finishedAt,
		DurationSeconds: int64((30 * time.Minute) / time.Second),
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}

	evidence, err := backend.RuntimeEvidence(ctx, RuntimeEvidenceQuery{ProjectID: "detent"})
	if err != nil {
		t.Fatalf("RuntimeEvidence() error = %v", err)
	}

	if evidence.Backend != BackendSQLite || evidence.Path != dbPath || !evidence.Healthy {
		t.Fatalf("RuntimeEvidence() = %#v, want healthy sqlite evidence for %q", evidence, dbPath)
	}
	if evidence.MigrationVersion < 7 || evidence.MigrationStatus == "" {
		t.Fatalf("migration evidence = version %d status %q, want applied version", evidence.MigrationVersion, evidence.MigrationStatus)
	}
	if got := runtimeEvidenceTableCount(evidence.Tables, "usage_events"); got != 1 {
		t.Fatalf("usage_events row count = %d, want 1", got)
	}
	if got := runtimeEvidenceTableCount(evidence.Tables, "workflow_phase_events"); got != 1 {
		t.Fatalf("workflow_phase_events row count = %d, want 1", got)
	}
	if got := runtimeEvidenceTableCount(evidence.Tables, "scheduled_runs"); got != 0 {
		t.Fatalf("scheduled_runs row count = %d, want 0", got)
	}
	if got := runtimeEvidenceTableCount(evidence.Tables, "api_keys"); got != 0 {
		t.Fatalf("api_keys row count = %d, want 0", got)
	}
	if evidence.WorkflowPhaseEvents.RowCount != 1 {
		t.Fatalf("WorkflowPhaseEvents.RowCount = %d, want 1", evidence.WorkflowPhaseEvents.RowCount)
	}
	if evidence.WorkflowPhaseEvents.OldestFinishedAt == nil || !evidence.WorkflowPhaseEvents.OldestFinishedAt.Equal(finishedAt) {
		t.Fatalf("OldestFinishedAt = %#v, want %s", evidence.WorkflowPhaseEvents.OldestFinishedAt, finishedAt)
	}
	if evidence.WorkflowPhaseEvents.NewestFinishedAt == nil || !evidence.WorkflowPhaseEvents.NewestFinishedAt.Equal(finishedAt) {
		t.Fatalf("NewestFinishedAt = %#v, want %s", evidence.WorkflowPhaseEvents.NewestFinishedAt, finishedAt)
	}
}

func TestSQLiteValidatorVerdictRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, err := Open(ctx, Config{
		Backend: BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	recordedAt := time.Date(2026, 7, 2, 14, 0, 0, 0, time.UTC)
	key := ValidatorVerdictKey{ProjectID: "detent", IssueID: "issue-858", HeadSHA: "abc123"}
	if err := backend.RecordValidatorVerdict(ctx, ValidatorVerdict{
		ProjectID:  key.ProjectID,
		IssueID:    key.IssueID,
		HeadSHA:    key.HeadSHA,
		Identifier: "digitaldrywood/detent#858",
		IssueURL:   "https://github.test/digitaldrywood/detent/issues/858",
		PRNumber:   int64Pointer(875),
		Submitted:  true,
		Verdict:    "pass",
		Score:      0.93,
		Summary:    "acceptance criteria pass",
		Findings: []ValidatorFinding{{
			Severity: "p2",
			Body:     "minor follow-up",
			URL:      "https://github.test/digitaldrywood/detent/pull/875#discussion_r1",
			Path:     "internal/orchestrator/autopromote_tick.go",
			Line:     42,
		}},
		RecordedAt: recordedAt,
		UpdatedAt:  recordedAt,
	}); err != nil {
		t.Fatalf("RecordValidatorVerdict() error = %v", err)
	}

	got, err := backend.ValidatorVerdict(ctx, key)
	if err != nil {
		t.Fatalf("ValidatorVerdict() error = %v", err)
	}
	if got.ProjectID != key.ProjectID || got.IssueID != key.IssueID || got.HeadSHA != key.HeadSHA {
		t.Fatalf("verdict key = %#v, want %#v", got, key)
	}
	if !got.Submitted || got.Verdict != "pass" || got.Score != 0.93 || got.Summary != "acceptance criteria pass" {
		t.Fatalf("verdict result = %#v, want submitted pass score", got)
	}
	if got.PRNumber == nil || *got.PRNumber != 875 {
		t.Fatalf("PRNumber = %#v, want 875", got.PRNumber)
	}
	if len(got.Findings) != 1 || got.Findings[0].Severity != "p2" || got.Findings[0].Path != "internal/orchestrator/autopromote_tick.go" {
		t.Fatalf("Findings = %#v, want persisted finding", got.Findings)
	}
	if got.Commented {
		t.Fatalf("Commented = true, want false")
	}

	commentedAt := recordedAt.Add(time.Minute)
	if err := backend.MarkValidatorVerdictCommented(ctx, key, commentedAt); err != nil {
		t.Fatalf("MarkValidatorVerdictCommented() error = %v", err)
	}
	got, err = backend.ValidatorVerdict(ctx, key)
	if err != nil {
		t.Fatalf("ValidatorVerdict() after commented error = %v", err)
	}
	if !got.Commented || !got.UpdatedAt.Equal(commentedAt) {
		t.Fatalf("commented verdict = %#v, want commented at %s", got, commentedAt)
	}
}

func TestSQLiteListValidatorVerdictsFiltersAndSorts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, err := Open(ctx, Config{
		Backend: BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	base := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	verdicts := []ValidatorVerdict{
		{
			ProjectID:  "detent",
			IssueID:    "issue-1",
			HeadSHA:    "aaa111",
			Identifier: "digitaldrywood/detent#1",
			PRNumber:   int64Pointer(11),
			Verdict:    "pass",
			RecordedAt: base,
			UpdatedAt:  base,
		},
		{
			ProjectID:  "detent",
			IssueID:    "issue-2",
			HeadSHA:    "bbb222",
			Identifier: "digitaldrywood/detent#2",
			PRNumber:   int64Pointer(12),
			Verdict:    "rework",
			RecordedAt: base.Add(time.Hour),
			UpdatedAt:  base.Add(time.Hour),
		},
		{
			ProjectID:  "video",
			IssueID:    "render-1",
			HeadSHA:    "ccc333",
			Identifier: "video/render-1",
			Verdict:    "pass",
			RecordedAt: base.Add(2 * time.Hour),
			UpdatedAt:  base.Add(2 * time.Hour),
		},
	}
	for _, verdict := range verdicts {
		if err := backend.RecordValidatorVerdict(ctx, verdict); err != nil {
			t.Fatalf("RecordValidatorVerdict(%s) error = %v", verdict.IssueID, err)
		}
	}

	got, err := backend.ListValidatorVerdicts(ctx, ValidatorVerdictQuery{})
	if err != nil {
		t.Fatalf("ListValidatorVerdicts() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ListValidatorVerdicts() len = %d, want 3", len(got))
	}
	if got[0].IssueID != "render-1" || got[1].IssueID != "issue-2" || got[2].IssueID != "issue-1" {
		t.Fatalf("ListValidatorVerdicts() order = %#v", []string{got[0].IssueID, got[1].IssueID, got[2].IssueID})
	}

	filtered, err := backend.ListValidatorVerdicts(ctx, ValidatorVerdictQuery{
		ProjectID: "detent",
		From:      base.Add(30 * time.Minute),
		To:        base.Add(90 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ListValidatorVerdicts(filtered) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].IssueID != "issue-2" {
		t.Fatalf("filtered verdicts = %#v, want issue-2", filtered)
	}
}

func TestSQLiteQueriesRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detent.db")

	backend, err := Open(ctx, Config{
		Backend:     BackendSQLite,
		Path:        dbPath,
		BusyTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	run, err := backend.Queries().CreateDetentRun(ctx, sqlc.CreateDetentRunParams{
		StartedAt:            "2026-05-30T12:00:00Z",
		StoppedAt:            sql.NullString{},
		RestartReason:        sql.NullString{},
		PeakConcurrentAgents: 3,
		SessionsLaunched:     1,
		InputTokens:          120,
		OutputTokens:         30,
		TotalTokens:          150,
		RuntimeSeconds:       90,
	})
	if err != nil {
		t.Fatalf("CreateDetentRun() error = %v", err)
	}

	session, err := backend.Queries().CreateCodexSession(ctx, sqlc.CreateCodexSessionParams{
		RunID:          sql.NullInt64{Int64: run.ID, Valid: true},
		IssueID:        sql.NullString{String: "I_kwDOSskuwc8AAAABD42cNw", Valid: true},
		Identifier:     sql.NullString{String: "digitaldrywood/detent#5", Valid: true},
		IssueURL:       sql.NullString{String: "https://github.com/digitaldrywood/detent/issues/5", Valid: true},
		StartedAt:      sql.NullString{String: "2026-05-30T12:01:00Z", Valid: true},
		CompletedAt:    sql.NullString{String: "2026-05-30T12:02:00Z", Valid: true},
		Turns:          2,
		InputTokens:    100,
		OutputTokens:   20,
		TotalTokens:    120,
		RuntimeSeconds: 60,
		FinalState:     sql.NullString{String: "Human Review", Valid: true},
		Model:          sql.NullString{String: "gpt-5", Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateCodexSession() error = %v", err)
	}

	got, err := backend.Queries().GetCodexSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetCodexSession() error = %v", err)
	}

	if got.RunID.Int64 != run.ID {
		t.Fatalf("session run_id = %d, want %d", got.RunID.Int64, run.ID)
	}
	if got.Identifier.String != "digitaldrywood/detent#5" {
		t.Fatalf("session identifier = %q, want digitaldrywood/detent#5", got.Identifier.String)
	}
}

func runtimeEvidenceTableCount(tables []RuntimeTableEvidence, name string) int64 {
	for _, table := range tables {
		if table.Name == name {
			return table.RowCount
		}
	}
	return -1
}

func TestWorkAttemptStoreRoundTripDecisionsAndRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 6, 26, 14, 30, 0, 0, time.UTC)

	prNumber := int64(737)
	attemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:            " detent ",
		IssueID:              " issue-737 ",
		Identifier:           " digitaldrywood/detent#737 ",
		IssueURL:             " https://github.com/digitaldrywood/detent/issues/737 ",
		PRNumber:             &prNumber,
		Repo:                 " digitaldrywood/detent ",
		WorkerType:           " agent ",
		WorkerHost:           " worker-a ",
		Lane:                 " In Progress ",
		AttemptNumber:        2,
		StartedAt:            base,
		LeaseExpiresAt:       base.Add(5 * time.Minute),
		WorkerMetadataJSON:   `{"mode":"implement"}`,
		CapacitySnapshotJSON: `{"global_used":1}`,
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	if attemptID <= 0 {
		t.Fatalf("StartWorkAttempt() id = %d, want positive", attemptID)
	}

	if err := backend.RecordWorkAttemptHeartbeat(ctx, WorkAttemptHeartbeat{
		AttemptID:              attemptID,
		HeartbeatAt:            base.Add(time.Minute),
		LeaseExpiresAt:         base.Add(6 * time.Minute),
		Phase:                  "testing",
		StatusMessage:          "running focused tests",
		WaitReason:             "github_checks",
		GitHubRateSnapshotJSON: `{"rest_remaining":4878}`,
		CapacitySnapshotJSON:   `{"global_available":1}`,
		WorkerMetadataJSON:     `{"work_product_pushed":true}`,
		MetricsJSON:            `{"test_runs":1}`,
		NextAction:             "wait for CI",
		DetentSessionID:        737,
		ProviderSessionID:      "thread-737-turn-1",
		RuntimeIdentity: agentidentity.Configured(
			"codex-local",
			"codex",
			"local",
			"rework",
			"qwen-alias",
			"ollama",
			"",
			"",
			base,
		).Merge(agentidentity.RuntimeUpdate("qwen3-coder:30b", "local_ollama", "high", "", base.Add(time.Minute))),
	}); err != nil {
		t.Fatalf("RecordWorkAttemptHeartbeat() error = %v", err)
	}

	if _, err := backend.RecordSchedulerDecision(ctx, SchedulerDecision{
		ProjectID:            " detent ",
		IssueID:              " issue-738 ",
		Identifier:           " digitaldrywood/detent#738 ",
		Repo:                 " digitaldrywood/detent ",
		Lane:                 " Rework ",
		QueuePosition:        3,
		Result:               SchedulerDecisionResultSkipped,
		Reason:               " repo_merge_lock ",
		DecisionAt:           base.Add(2 * time.Minute),
		CapacitySnapshotJSON: `{"repo_merge_lock":"digitaldrywood/detent"}`,
	}); err != nil {
		t.Fatalf("RecordSchedulerDecision() error = %v", err)
	}

	active, err := backend.ListActiveWorkAttempts(ctx, WorkAttemptQuery{ProjectID: "detent"})
	if err != nil {
		t.Fatalf("ListActiveWorkAttempts() error = %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active attempts len = %d, want 1: %#v", len(active), active)
	}
	got := active[0]
	if got.ProjectID != "detent" || got.IssueID != "issue-737" || got.AttemptNumber != 2 || got.Phase != "testing" {
		t.Fatalf("active attempt = %#v, want normalized heartbeat", got)
	}
	if got.Status != WorkAttemptStatusActive {
		t.Fatalf("active status = %q, want %q", got.Status, WorkAttemptStatusActive)
	}
	if got.DetentSessionID != 737 || got.ProviderSessionID != "thread-737-turn-1" || got.RuntimeIdentity.Model() != "qwen3-coder:30b" || got.RuntimeIdentity.Role != "rework" {
		t.Fatalf("active runtime identity = %#v, want correlated rework session", got)
	}
	if got.WorkerMetadataJSON != `{"work_product_pushed":true}` {
		t.Fatalf("active WorkerMetadataJSON = %q", got.WorkerMetadataJSON)
	}
	receipt, err := backend.WorkAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.ID != attemptID || receipt.WorkerHost != "worker-a" || receipt.WaitReason != "github_checks" {
		t.Fatalf("WorkAttempt() = %#v, want receipt fields for attempt %d", receipt, attemptID)
	}

	decisions, err := backend.ListRecentSchedulerDecisions(ctx, SchedulerDecisionQuery{ProjectID: "detent", Limit: 5})
	if err != nil {
		t.Fatalf("ListRecentSchedulerDecisions() error = %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("decisions len = %d, want 1: %#v", len(decisions), decisions)
	}
	if decisions[0].Result != SchedulerDecisionResultSkipped || decisions[0].Reason != "repo_merge_lock" {
		t.Fatalf("decision = %#v, want skipped repo_merge_lock", decisions[0])
	}

	if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{
		AttemptID:          attemptID,
		CompletedAt:        base.Add(3 * time.Minute),
		Status:             WorkAttemptStatusTerminal,
		TerminalState:      WorkAttemptTerminalNoProgress,
		Phase:              "completed",
		WorkerMetadataJSON: `{"completion_progress":{"outcome":"no_progress","current_head_sha":"abc123"}}`,
	}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}
	active, err = backend.ListActiveWorkAttempts(ctx, WorkAttemptQuery{ProjectID: "detent"})
	if err != nil {
		t.Fatalf("ListActiveWorkAttempts() after complete error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active attempts after complete = %#v, want none", active)
	}
	history, err := backend.ListRecentTerminalWorkAttempts(ctx, WorkAttemptHistoryQuery{
		ProjectID:  "detent",
		IssueID:    "issue-737",
		WorkerType: "agent",
		Limit:      5,
	})
	if err != nil {
		t.Fatalf("ListRecentTerminalWorkAttempts() error = %v", err)
	}
	if len(history) != 1 || history[0].ID != attemptID || history[0].TerminalState != WorkAttemptTerminalNoProgress {
		t.Fatalf("history = %#v, want completed no_progress attempt %d", history, attemptID)
	}
	if history[0].WorkerMetadataJSON != `{"completion_progress":{"outcome":"no_progress","current_head_sha":"abc123"}}` {
		t.Fatalf("history WorkerMetadataJSON = %q", history[0].WorkerMetadataJSON)
	}
	if history[0].DetentSessionID != 737 || history[0].ProviderSessionID != "thread-737-turn-1" || history[0].RuntimeIdentity.Model() != "qwen3-coder:30b" {
		t.Fatalf("history runtime identity = %#v, want heartbeat identity preserved at completion", history[0])
	}

	staleID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:      "detent",
		IssueID:        "issue-stale",
		Identifier:     "digitaldrywood/detent#739",
		Repo:           "digitaldrywood/detent",
		WorkerType:     "merge",
		Lane:           "Merging",
		AttemptNumber:  1,
		StartedAt:      base.Add(-10 * time.Minute),
		LeaseExpiresAt: base.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() stale error = %v", err)
	}
	recovered, err := backend.TimeoutExpiredWorkAttempts(ctx, WorkAttemptTimeout{
		Now:           base,
		TerminalState: WorkAttemptTerminalTimedOut,
		ErrorClass:    "stale_lease",
		ErrorMessage:  "worker lease expired",
	})
	if err != nil {
		t.Fatalf("TimeoutExpiredWorkAttempts() error = %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != staleID || recovered[0].TerminalState != WorkAttemptTerminalTimedOut {
		t.Fatalf("recovered stale attempts = %#v, want stale timeout id %d", recovered, staleID)
	}
}

func TestWorkAttemptCapacityReleaseStore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		projectID   string
		nextAction  string
		terminal    bool
		wantPending bool
	}{
		{name: "terminal release for project", projectID: "detent", nextAction: " release CAPACITY ", terminal: true, wantPending: true},
		{name: "active release for project", projectID: "detent", nextAction: "release capacity"},
		{name: "unrelated terminal action", projectID: "detent", nextAction: "retry tracker transition", terminal: true},
		{name: "terminal release for another project", projectID: "video", nextAction: "release capacity", terminal: true},
	}

	var wantID int64
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startedAt := base.Add(time.Duration(index) * time.Minute)
			attemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
				ProjectID:  tt.projectID,
				IssueID:    fmt.Sprintf("issue-%d", index),
				WorkerType: "agent",
				StartedAt:  startedAt,
				NextAction: tt.nextAction,
			})
			if err != nil {
				t.Fatalf("StartWorkAttempt() error = %v", err)
			}
			if tt.terminal {
				if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{
					AttemptID:     attemptID,
					CompletedAt:   startedAt.Add(30 * time.Second),
					TerminalState: WorkAttemptTerminalSuccess,
					NextAction:    tt.nextAction,
				}); err != nil {
					t.Fatalf("CompleteWorkAttempt() error = %v", err)
				}
			}
			if tt.wantPending {
				wantID = attemptID
			}
		})
	}

	pending, err := backend.ListPendingWorkAttemptCapacityReleases(ctx, " detent ")
	if err != nil {
		t.Fatalf("ListPendingWorkAttemptCapacityReleases() error = %v", err)
	}
	if len(pending) != 1 || pending[0].ID != wantID {
		t.Fatalf("pending capacity releases = %#v, want attempt %d", pending, wantID)
	}
	if err := backend.ClearWorkAttemptCapacityRelease(ctx, wantID); err != nil {
		t.Fatalf("ClearWorkAttemptCapacityRelease() error = %v", err)
	}
	if err := backend.ClearWorkAttemptCapacityRelease(ctx, wantID); err != nil {
		t.Fatalf("ClearWorkAttemptCapacityRelease() repeated error = %v", err)
	}
	pending, err = backend.ListPendingWorkAttemptCapacityReleases(ctx, "detent")
	if err != nil {
		t.Fatalf("ListPendingWorkAttemptCapacityReleases() after clear error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending capacity releases after clear = %#v, want none", pending)
	}
	receipt, err := backend.WorkAttempt(ctx, wantID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.NextAction != "" {
		t.Fatalf("NextAction = %q, want cleared", receipt.NextAction)
	}
}

func TestStatsStoreRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  RunStart
	}{
		{
			name: "persists run and session stats",
			run: RunStart{
				StartedAt:            time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
				PeakConcurrentAgents: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			backend := openTestStore(t, ctx)

			runID, err := backend.StartRun(ctx, tt.run)
			if err != nil {
				t.Fatalf("StartRun() error = %v", err)
			}

			if err := backend.UpdateRun(ctx, runID, RunUpdate{
				PeakConcurrentAgents: 3,
				SessionsLaunched:     1,
				InputTokens:          100,
				OutputTokens:         25,
				TotalTokens:          125,
				RuntimeSeconds:       240,
			}); err != nil {
				t.Fatalf("UpdateRun() error = %v", err)
			}

			sessionID, err := backend.StartSession(ctx, SessionStart{
				RunID:      runID,
				ProjectID:  "detent",
				IssueID:    "I_kwDOSskuwc8AAAABD42c3Q",
				Identifier: "digitaldrywood/detent#6",
				IssueURL:   "https://github.com/digitaldrywood/detent/issues/6",
				StartedAt:  time.Date(2026, 5, 30, 12, 1, 0, 0, time.UTC),
				Model:      "gpt-5",
			})
			if err != nil {
				t.Fatalf("StartSession() error = %v", err)
			}
			modelContextWindow := int64(200000)

			if err := backend.FinishSession(ctx, sessionID, SessionFinish{
				CompletedAt:           time.Date(2026, 5, 30, 12, 5, 0, 0, time.UTC),
				Turns:                 2,
				InputTokens:           100,
				CachedInputTokens:     40,
				OutputTokens:          25,
				ReasoningOutputTokens: 7,
				TotalTokens:           125,
				ModelContextWindow:    &modelContextWindow,
				RuntimeSeconds:        240,
				FinalState:            "Human Review",
				Model:                 "gpt-5-resolved",
				SkillDraftProposed:    true,
			}); err != nil {
				t.Fatalf("FinishSession() error = %v", err)
			}

			if err := backend.StopRun(ctx, runID, RunStop{
				StoppedAt:            time.Date(2026, 5, 30, 12, 5, 0, 0, time.UTC),
				RestartReason:        "complete",
				PeakConcurrentAgents: 3,
				SessionsLaunched:     1,
				InputTokens:          100,
				OutputTokens:         25,
				TotalTokens:          125,
				RuntimeSeconds:       240,
			}); err != nil {
				t.Fatalf("StopRun() error = %v", err)
			}

			run, err := backend.Queries().GetDetentRun(ctx, runID)
			if err != nil {
				t.Fatalf("GetDetentRun() error = %v", err)
			}
			if run.StartedAt != "2026-05-30T12:00:00Z" {
				t.Fatalf("run started_at = %q, want 2026-05-30T12:00:00Z", run.StartedAt)
			}
			if run.StoppedAt.String != "2026-05-30T12:05:00Z" {
				t.Fatalf("run stopped_at = %q, want 2026-05-30T12:05:00Z", run.StoppedAt.String)
			}
			if run.TotalTokens != 125 {
				t.Fatalf("run total_tokens = %d, want 125", run.TotalTokens)
			}

			session, err := backend.Queries().GetCodexSession(ctx, sessionID)
			if err != nil {
				t.Fatalf("GetCodexSession() error = %v", err)
			}
			if session.RunID.Int64 != runID {
				t.Fatalf("session run_id = %d, want %d", session.RunID.Int64, runID)
			}
			if session.CompletedAt.String != "2026-05-30T12:05:00Z" {
				t.Fatalf("session completed_at = %q, want 2026-05-30T12:05:00Z", session.CompletedAt.String)
			}
			if session.FinalState.String != "Human Review" {
				t.Fatalf("session final_state = %q, want Human Review", session.FinalState.String)
			}
			if session.Model.String != "gpt-5-resolved" {
				t.Fatalf("session model = %q, want gpt-5-resolved", session.Model.String)
			}
			if !session.CachedInputTokens.Valid || session.CachedInputTokens.Int64 != 40 {
				t.Fatalf("session cached_input_tokens = %#v, want 40", session.CachedInputTokens)
			}
			if !session.ReasoningOutputTokens.Valid || session.ReasoningOutputTokens.Int64 != 7 {
				t.Fatalf("session reasoning_output_tokens = %#v, want 7", session.ReasoningOutputTokens)
			}
			if !session.ModelContextWindow.Valid || session.ModelContextWindow.Int64 != modelContextWindow {
				t.Fatalf("session model_context_window = %#v, want %d", session.ModelContextWindow, modelContextWindow)
			}
			if session.SkillDraftProposed != 1 {
				t.Fatalf("session skill_draft_proposed = %d, want 1", session.SkillDraftProposed)
			}

			spend, err := backend.DailyTokenSpend(ctx, time.Date(2026, 5, 30, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("DailyTokenSpend() error = %v", err)
			}
			if spend.InputTokens != 100 || spend.CachedInputTokens != 40 || spend.OutputTokens != 25 || spend.ReasoningOutputTokens != 7 || spend.TotalTokens != 125 || spend.Sessions != 1 {
				t.Fatalf("DailyTokenSpend() = %#v", spend)
			}
			if len(spend.ByModel) != 1 || spend.ByModel[0].Model != "gpt-5-resolved" || spend.ByModel[0].CachedInputTokens != 40 || spend.ByModel[0].ReasoningOutputTokens != 7 {
				t.Fatalf("DailyTokenSpend().ByModel = %#v", spend.ByModel)
			}

			issueSpend, err := backend.IssueTokenSpend(ctx, IssueIdentity{ProjectID: "detent", IssueID: "I_kwDOSskuwc8AAAABD42c3Q"})
			if err != nil {
				t.Fatalf("IssueTokenSpend() error = %v", err)
			}
			if issueSpend.InputTokens != 100 || issueSpend.CachedInputTokens != 40 || issueSpend.OutputTokens != 25 || issueSpend.ReasoningOutputTokens != 7 || issueSpend.TotalTokens != 125 || issueSpend.Sessions != 1 {
				t.Fatalf("IssueTokenSpend() = %#v", issueSpend)
			}
			if len(issueSpend.ByModel) != 1 || issueSpend.ByModel[0].Model != "gpt-5-resolved" || issueSpend.ByModel[0].CachedInputTokens != 40 || issueSpend.ByModel[0].ReasoningOutputTokens != 7 {
				t.Fatalf("IssueTokenSpend().ByModel = %#v", issueSpend.ByModel)
			}

			identifierSpend, err := backend.IssueTokenSpend(ctx, IssueIdentity{ProjectID: "detent", Identifier: "digitaldrywood/detent#6"})
			if err != nil {
				t.Fatalf("IssueTokenSpend(identifier) error = %v", err)
			}
			if identifierSpend.TotalTokens != 125 {
				t.Fatalf("IssueTokenSpend(identifier).TotalTokens = %d, want 125", identifierSpend.TotalTokens)
			}

			urlSpend, err := backend.IssueTokenSpend(ctx, IssueIdentity{ProjectID: "detent", IssueURL: "https://github.com/digitaldrywood/detent/issues/6"})
			if err != nil {
				t.Fatalf("IssueTokenSpend(url) error = %v", err)
			}
			if urlSpend.TotalTokens != 125 {
				t.Fatalf("IssueTokenSpend(url).TotalTokens = %d, want 125", urlSpend.TotalTokens)
			}

			lifetime, err := backend.LifetimeTotals(ctx)
			if err != nil {
				t.Fatalf("LifetimeTotals() error = %v", err)
			}
			if lifetime.InputTokens != 100 || lifetime.CachedInputTokens != 40 || lifetime.OutputTokens != 25 || lifetime.ReasoningOutputTokens != 7 || lifetime.TotalTokens != 125 || lifetime.RuntimeSeconds != 240 {
				t.Fatalf("LifetimeTotals() token/runtime totals = %#v", lifetime)
			}
			if lifetime.Sessions != 1 || lifetime.Runs != 1 {
				t.Fatalf("LifetimeTotals() sessions/runs = %#v, want 1/1", lifetime)
			}
		})
	}
}

func TestSessionRuntimeIdentityPersistsRequestedAndResolvedSeparately(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	configuredAt := time.Date(2026, 7, 9, 18, 0, 0, 0, time.UTC)
	runtimeAt := configuredAt.Add(time.Minute)
	configured := agentidentity.Configured("codex-local", "codex", "local", "code", "qwen-alias", "ollama", "", "", configuredAt)

	sessionID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID:   1118,
		IssueID:         "issue-1118",
		Identifier:      "digitaldrywood/detent#1118",
		StartedAt:       configuredAt,
		RequestedModel:  configured.RequestedModel.Value,
		RuntimeIdentity: configured,
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}

	started, err := backend.Queries().GetCodexSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetCodexSession(started) error = %v", err)
	}
	if started.Model.Valid || started.Model.String != "" {
		t.Fatalf("started model = %#v, want unresolved", started.Model)
	}
	if started.RequestedModel.String != "qwen-alias" || started.RequestedModelProvenance.String != string(agentidentity.ProvenanceConfigured) {
		t.Fatalf("started requested model = %#v/%#v", started.RequestedModel, started.RequestedModelProvenance)
	}
	if started.WorkAttemptID.Int64 != 1118 || started.AgentRoute.String != "local" {
		t.Fatalf("started attempt/route = %#v/%#v", started.WorkAttemptID, started.AgentRoute)
	}

	resolved := configured.Merge(agentidentity.RuntimeUpdate("qwen3-coder:30b", "local_ollama", "high", "flex", runtimeAt))
	if err := backend.UpdateSessionIdentity(ctx, sessionID, resolved); err != nil {
		t.Fatalf("UpdateSessionIdentity() error = %v", err)
	}
	if err := backend.FinishSession(ctx, sessionID, SessionFinish{
		CompletedAt:       runtimeAt.Add(time.Minute),
		FinalState:        "Human Review",
		Model:             resolved.ResolvedModel.Value,
		ProviderThreadID:  "thread-1118",
		ProviderSessionID: "thread-1118-turn-1",
		RuntimeIdentity:   resolved,
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}

	finished, err := backend.Queries().GetCodexSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetCodexSession(finished) error = %v", err)
	}
	if finished.RequestedModel.String != "qwen-alias" || finished.Model.String != "qwen3-coder:30b" {
		t.Fatalf("finished requested/resolved = %q/%q", finished.RequestedModel.String, finished.Model.String)
	}
	if finished.Provider.String != "local_ollama" || finished.ProviderProvenance.String != string(agentidentity.ProvenanceRuntime) {
		t.Fatalf("finished provider = %#v/%#v", finished.Provider, finished.ProviderProvenance)
	}
	if finished.ModelProvenance.String != string(agentidentity.ProvenanceRuntime) || finished.ReasoningEffort.String != "high" || finished.ServiceTier.String != "flex" {
		t.Fatalf("finished runtime values = %#v", finished)
	}
	if finished.IdentityObservedAt.String != runtimeAt.Format(time.RFC3339Nano) {
		t.Fatalf("identity observed at = %q, want %q", finished.IdentityObservedAt.String, runtimeAt.Format(time.RFC3339Nano))
	}
}

func TestDailyTokenSpendIsScopedToProject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	day := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	for _, session := range []struct {
		projectID  string
		identifier string
		tokens     int64
	}{
		{projectID: "detent", identifier: "digitaldrywood/detent#1279", tokens: 100},
		{projectID: "gopher-ai", identifier: "gopherguides/gopher-ai#42", tokens: 900},
	} {
		sessionID, err := backend.StartSession(ctx, SessionStart{
			ProjectID:  session.projectID,
			Identifier: session.identifier,
			StartedAt:  day.Add(-time.Minute),
			Model:      "gpt-5",
		})
		if err != nil {
			t.Fatalf("StartSession(%s) error = %v", session.identifier, err)
		}
		if err := backend.FinishSession(ctx, sessionID, SessionFinish{
			CompletedAt: day,
			InputTokens: session.tokens,
			TotalTokens: session.tokens,
			FinalState:  "complete",
			Model:       "gpt-5",
		}); err != nil {
			t.Fatalf("FinishSession(%s) error = %v", session.identifier, err)
		}
	}

	spend, err := backend.ProjectDailyTokenSpend(ctx, "detent", day)
	if err != nil {
		t.Fatalf("ProjectDailyTokenSpend() error = %v", err)
	}
	if spend.TotalTokens != 100 || spend.Sessions != 1 {
		t.Fatalf("ProjectDailyTokenSpend() = %#v, want detent session only", spend)
	}

	unknownID, err := backend.StartSession(ctx, SessionStart{
		Identifier: "example/legacy#1",
		StartedAt:  day.Add(-time.Minute),
		Model:      "gpt-5",
	})
	if err != nil {
		t.Fatalf("StartSession(unknown) error = %v", err)
	}
	if err := backend.FinishSession(ctx, unknownID, SessionFinish{
		CompletedAt: day,
		InputTokens: 50,
		TotalTokens: 50,
		FinalState:  "complete",
		Model:       "gpt-5",
	}); err != nil {
		t.Fatalf("FinishSession(unknown) error = %v", err)
	}
	spend, err = backend.ProjectDailyTokenSpend(ctx, "detent", day)
	if err != nil {
		t.Fatalf("ProjectDailyTokenSpend(unknown) error = %v", err)
	}
	if spend.TotalTokens != 150 || spend.Sessions != 2 {
		t.Fatalf("ProjectDailyTokenSpend(unknown) = %#v, want conservative unknown fallback", spend)
	}

	updated, err := backend.BackfillSessionProjectIDs(ctx, []SessionProjectAttribution{{ProjectID: "legacy", Repository: "example/legacy"}})
	if err != nil {
		t.Fatalf("BackfillSessionProjectIDs() error = %v", err)
	}
	if updated != 1 {
		t.Fatalf("BackfillSessionProjectIDs() = %d, want 1", updated)
	}
	spend, err = backend.ProjectDailyTokenSpend(ctx, "detent", day)
	if err != nil {
		t.Fatalf("ProjectDailyTokenSpend(backfilled) error = %v", err)
	}
	if spend.TotalTokens != 100 || spend.Sessions != 1 {
		t.Fatalf("ProjectDailyTokenSpend(backfilled) = %#v, want detent session only", spend)
	}

	wildcardID, err := backend.StartSession(ctx, SessionStart{
		IssueURL:  "https://github.com/owner/axb/issues/1",
		StartedAt: day.Add(-time.Minute),
		Model:     "gpt-5",
	})
	if err != nil {
		t.Fatalf("StartSession(wildcard) error = %v", err)
	}
	if err := backend.FinishSession(ctx, wildcardID, SessionFinish{CompletedAt: day, FinalState: "complete", Model: "gpt-5"}); err != nil {
		t.Fatalf("FinishSession(wildcard) error = %v", err)
	}
	updated, err = backend.BackfillSessionProjectIDs(ctx, []SessionProjectAttribution{{ProjectID: "wrong", Repository: "owner/a_b"}})
	if err != nil {
		t.Fatalf("BackfillSessionProjectIDs(wildcard) error = %v", err)
	}
	if updated != 0 {
		t.Fatalf("BackfillSessionProjectIDs(wildcard) = %d, want 0", updated)
	}
	updated, err = backend.BackfillSessionProjectIDs(ctx, []SessionProjectAttribution{{ProjectID: "right", Repository: "owner/axb"}})
	if err != nil {
		t.Fatalf("BackfillSessionProjectIDs(exact URL) error = %v", err)
	}
	if updated != 1 {
		t.Fatalf("BackfillSessionProjectIDs(exact URL) = %d, want 1", updated)
	}
}

func TestLatestCompletedAgentResumeStateMatchesIssueBackendAndModel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	startedAt := time.Date(2026, 7, 2, 17, 0, 0, 0, time.UTC)
	failedID, err := backend.StartSession(ctx, SessionStart{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		Identifier:       "digitaldrywood/detent#859",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/859",
		StartedAt:        startedAt,
		Model:            "gpt-5-codex",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("StartSession(failed) error = %v", err)
	}
	if err := backend.FinishSession(ctx, failedID, SessionFinish{
		CompletedAt:       startedAt.Add(time.Minute),
		FinalState:        "failed",
		Model:             "gpt-5-codex",
		ProviderThreadID:  "thread-failed",
		ProviderSessionID: "session-failed",
	}); err != nil {
		t.Fatalf("FinishSession(failed) error = %v", err)
	}

	firstID, err := backend.StartSession(ctx, SessionStart{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		Identifier:       "digitaldrywood/detent#859",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/859",
		StartedAt:        startedAt.Add(2 * time.Minute),
		Model:            "gpt-5-codex",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("StartSession(first) error = %v", err)
	}
	if err := backend.FinishSession(ctx, firstID, SessionFinish{
		CompletedAt:       startedAt.Add(3 * time.Minute),
		FinalState:        "completed",
		Model:             "gpt-5-codex-resolved",
		ProviderThreadID:  "thread-first",
		ProviderSessionID: "session-first",
	}); err != nil {
		t.Fatalf("FinishSession(first) error = %v", err)
	}

	resumeMetadata := `{"pr_number":42,"pr_head_sha":"head-current","pr_base_sha":"base-current"}`
	resumeAttemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:          "detent",
		IssueID:            "issue-859",
		Identifier:         "digitaldrywood/detent#859",
		IssueURL:           "https://github.com/digitaldrywood/detent/issues/859",
		Repo:               "digitaldrywood/detent",
		WorkerType:         "agent",
		Lane:               "Todo",
		AttemptNumber:      2,
		StartedAt:          startedAt.Add(4 * time.Minute),
		WorkerMetadataJSON: resumeMetadata,
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt(resume candidate) error = %v", err)
	}
	secondID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID:    resumeAttemptID,
		ProjectID:        "detent",
		IssueID:          "issue-859",
		Identifier:       "digitaldrywood/detent#859",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/859",
		StartedAt:        startedAt.Add(4 * time.Minute),
		Model:            "gpt-5-codex",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("StartSession(second) error = %v", err)
	}
	if err := backend.FinishSession(ctx, secondID, SessionFinish{
		CompletedAt:          startedAt.Add(5 * time.Minute),
		FinalState:           "completed",
		Model:                "gpt-5-codex-resolved",
		ProviderThreadID:     "thread-second",
		ProviderSessionID:    "session-second",
		ResumedFromSessionID: firstID,
	}); err != nil {
		t.Fatalf("FinishSession(second) error = %v", err)
	}
	if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{
		AttemptID:          resumeAttemptID,
		CompletedAt:        startedAt.Add(6 * time.Minute),
		Status:             WorkAttemptStatusTerminal,
		TerminalState:      WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: resumeMetadata,
	}); err != nil {
		t.Fatalf("CompleteWorkAttempt(resume candidate) error = %v", err)
	}

	validatorID, err := backend.StartSession(ctx, SessionStart{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		Identifier:       "digitaldrywood/detent#859",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/859",
		StartedAt:        startedAt.Add(6 * time.Minute),
		Model:            "gpt-5-codex",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "validator",
	})
	if err != nil {
		t.Fatalf("StartSession(validator) error = %v", err)
	}
	if err := backend.FinishSession(ctx, validatorID, SessionFinish{
		CompletedAt:       startedAt.Add(7 * time.Minute),
		FinalState:        "completed",
		Model:             "gpt-5-codex-resolved",
		ProviderThreadID:  "thread-validator",
		ProviderSessionID: "session-validator",
	}); err != nil {
		t.Fatalf("FinishSession(validator) error = %v", err)
	}

	got, err := backend.LatestCompletedAgentResumeState(ctx, AgentResumeLookup{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		Identifier:       "digitaldrywood/detent#859",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/859",
		PRNumber:         42,
		PRHeadSHA:        "head-current",
		PRBaseSHA:        "base-current",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("LatestCompletedAgentResumeState() error = %v", err)
	}
	if got.DetentSessionID != secondID || got.ProviderThreadID != "thread-second" || got.ProviderSessionID != "session-second" {
		t.Fatalf("resume state = %#v, want newest completed second session", got)
	}
	if got.RequestedModel != "gpt-5-codex" || got.Model != "gpt-5-codex-resolved" || got.AgentRole != "code" {
		t.Fatalf("resume models = %#v, want requested and resolved model", got)
	}
	latestForIssue, err := backend.LatestIssueAgentResumeState(ctx, IssueIdentity{
		ProjectID:  "detent",
		Identifier: "digitaldrywood/detent#859",
	})
	if err != nil {
		t.Fatalf("LatestIssueAgentResumeState() error = %v", err)
	}
	if latestForIssue.DetentSessionID != validatorID || latestForIssue.AgentRole != "validator" {
		t.Fatalf("LatestIssueAgentResumeState() = %#v, want newest completed validator session", latestForIssue)
	}

	_, err = backend.LatestCompletedAgentResumeState(ctx, AgentResumeLookup{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		PRNumber:         42,
		PRHeadSHA:        "head-current",
		PRBaseSHA:        "base-current",
		RequestedModel:   "gpt-5-codex-mini",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestCompletedAgentResumeState(model mismatch) error = %v, want ErrNotFound", err)
	}

	_, err = backend.LatestCompletedAgentResumeState(ctx, AgentResumeLookup{
		ProjectID:        "detent",
		IssueID:          "issue-859",
		PRNumber:         42,
		PRHeadSHA:        "head-current",
		PRBaseSHA:        "base-current",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "merge",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestCompletedAgentResumeState(role mismatch) error = %v, want ErrNotFound", err)
	}

	for _, tt := range []struct {
		name    string
		project string
		headSHA string
		baseSHA string
	}{
		{name: "project mismatch", project: "other", headSHA: "head-current", baseSHA: "base-current"},
		{name: "head mismatch", project: "detent", headSHA: "head-stale", baseSHA: "base-current"},
		{name: "base mismatch", project: "detent", headSHA: "head-current", baseSHA: "base-moved"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := backend.LatestCompletedAgentResumeState(ctx, AgentResumeLookup{
				ProjectID:        tt.project,
				IssueID:          "issue-859",
				PRNumber:         42,
				PRHeadSHA:        tt.headSHA,
				PRBaseSHA:        tt.baseSHA,
				RequestedModel:   "gpt-5-codex",
				AgentBackendID:   "codex",
				AgentBackendKind: "codex",
				AgentRole:        "code",
			})
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("LatestCompletedAgentResumeState() error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestOrphanedAgentSessionsJournalProviderIdentityAndExcludeCleanExits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	startedAt := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	attemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:     "detent",
		IssueID:       "issue-1155",
		Identifier:    "digitaldrywood/detent#1155",
		IssueURL:      "https://github.com/digitaldrywood/detent/issues/1155",
		WorkerType:    "agent",
		WorkerHost:    "local",
		Lane:          "In Progress",
		AttemptNumber: 2,
		StartedAt:     startedAt,
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}

	orphanID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID:    attemptID,
		IssueID:          "issue-1155",
		Identifier:       "digitaldrywood/detent#1155",
		IssueURL:         "https://github.com/digitaldrywood/detent/issues/1155",
		StartedAt:        startedAt,
		RequestedModel:   "gpt-5.6-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("StartSession(orphan) error = %v", err)
	}
	started, err := backend.Queries().GetCodexSession(ctx, orphanID)
	if err != nil {
		t.Fatalf("GetCodexSession(orphan) error = %v", err)
	}
	if started.FinalState.String != SessionStateRunning || started.CompletedAt.Valid {
		t.Fatalf("started session state = %#v, want running without completed_at", started)
	}
	if err := backend.UpdateSessionProviderIdentity(ctx, orphanID, SessionProviderIdentity{
		ThreadID:  "thread-1155",
		SessionID: "thread-1155-turn-1",
	}); err != nil {
		t.Fatalf("UpdateSessionProviderIdentity() error = %v", err)
	}
	fallbackID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID:         attemptID,
		IssueID:               "issue-fallback",
		Identifier:            "digitaldrywood/detent#1157",
		StartedAt:             startedAt.Add(30 * time.Second),
		ProviderThreadID:      "thread-stale",
		ProviderSessionID:     "thread-stale-turn-1",
		ResumedFromSessionID:  orphanID,
		OrphanRecoveryOutcome: OrphanRecoveryResumed,
	})
	if err != nil {
		t.Fatalf("StartSession(fallback) error = %v", err)
	}
	if err := backend.UpdateSessionResumeState(ctx, fallbackID, SessionResumeState{OrphanRecoveryOutcome: OrphanRecoveryFresh, OrphanRecoveryFallbackReason: "rollout file not found"}); err != nil {
		t.Fatalf("UpdateSessionResumeState(fallback) error = %v", err)
	}
	fallback, err := backend.Queries().GetCodexSession(ctx, fallbackID)
	if err != nil {
		t.Fatalf("GetCodexSession(fallback) error = %v", err)
	}
	if fallback.ResumedFromSessionID.Valid || fallback.ProviderThreadID.Valid || fallback.ProviderSessionID.Valid || fallback.OrphanRecoveryOutcome.String != OrphanRecoveryFresh {
		t.Fatalf("fallback session resume metadata = %#v, want cleared provider source and fresh outcome", fallback)
	}
	if fallback.OrphanRecoveryFallbackReason.String != "rollout file not found" {
		t.Fatalf("fallback reason = %q, want rollout file not found", fallback.OrphanRecoveryFallbackReason.String)
	}

	cleanID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID:    attemptID,
		IssueID:          "issue-clean",
		Identifier:       "digitaldrywood/detent#1156",
		StartedAt:        startedAt.Add(time.Minute),
		RequestedModel:   "gpt-5.6-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
		ProviderThreadID: "thread-clean",
	})
	if err != nil {
		t.Fatalf("StartSession(clean) error = %v", err)
	}
	if err := backend.FinishSession(ctx, cleanID, SessionFinish{
		CompletedAt:      startedAt.Add(2 * time.Minute),
		FinalState:       "completed",
		ProviderThreadID: "thread-clean",
	}); err != nil {
		t.Fatalf("FinishSession(clean) error = %v", err)
	}

	orphans, err := backend.ListOrphanedAgentSessions(ctx, "detent")
	if err != nil {
		t.Fatalf("ListOrphanedAgentSessions() error = %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("ListOrphanedAgentSessions() len = %d, want 1: %#v", len(orphans), orphans)
	}
	got := orphans[0]
	if got.ResumeState.DetentSessionID != orphanID || got.ResumeState.ProviderThreadID != "thread-1155" || !got.ResumeState.Orphaned {
		t.Fatalf("orphan resume state = %#v", got.ResumeState)
	}
	if got.WorkAttemptID != attemptID || got.AttemptNumber != 2 || got.WorkerHost != "local" {
		t.Fatalf("orphan work attempt = %#v", got)
	}

	if err := backend.MarkAgentSessionOrphaned(ctx, orphanID, startedAt.Add(3*time.Minute)); err != nil {
		t.Fatalf("MarkAgentSessionOrphaned() error = %v", err)
	}
	marked, err := backend.Queries().GetCodexSession(ctx, orphanID)
	if err != nil {
		t.Fatalf("GetCodexSession(marked) error = %v", err)
	}
	if marked.FinalState.String != SessionStateOrphaned || !marked.CompletedAt.Valid {
		t.Fatalf("marked session = %#v, want terminal orphaned", marked)
	}
}

func TestActiveWorkerProcessesAreJournaledAndReaped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	startedAt := time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)
	sessionID, err := backend.StartSession(ctx, SessionStart{
		IssueID:    "issue-1214",
		Identifier: "digitaldrywood/detent#1214",
		StartedAt:  startedAt,
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}

	processStartedAt := startedAt.Add(time.Second)
	cleanupRoot := t.TempDir()
	cleanupPath := filepath.Join(cleanupRoot, "run-1214")
	if err := backend.UpdateSessionWorkerProcess(ctx, sessionID, WorkerProcessRegistration{
		WorkerProcessIdentity: WorkerProcessIdentity{
			PID:       4242,
			GroupID:   4242,
			StartedAt: processStartedAt,
		},
		CleanupRoot: cleanupRoot,
		CleanupPath: cleanupPath,
	}); err != nil {
		t.Fatalf("UpdateSessionWorkerProcess() error = %v", err)
	}

	active, err := backend.ListActiveWorkerProcesses(ctx)
	if err != nil {
		t.Fatalf("ListActiveWorkerProcesses() error = %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("ListActiveWorkerProcesses() len = %d, want 1", len(active))
	}
	if got := active[0]; got.SessionID != sessionID || got.Identifier != "digitaldrywood/detent#1214" || got.PID != 4242 || got.GroupID != 4242 || !got.StartedAt.Equal(processStartedAt) || got.CleanupRoot != cleanupRoot || got.CleanupPath != cleanupPath {
		t.Fatalf("active worker process = %#v", got)
	}
	if err := backend.FinishSession(ctx, sessionID, SessionFinish{
		CompletedAt: startedAt.Add(90 * time.Second),
		FinalState:  "completed",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
	active, err = backend.ListActiveWorkerProcesses(ctx)
	if err != nil {
		t.Fatalf("ListActiveWorkerProcesses() after terminal session error = %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("ListActiveWorkerProcesses() after terminal session len = %d, want unreaped process retained", len(active))
	}

	reapedAt := startedAt.Add(2 * time.Second)
	if err := backend.MarkSessionWorkerProcessReaped(ctx, sessionID, WorkerProcessReap{
		ReapedAt: reapedAt,
		Outcome:  WorkerProcessOutcomeTerminated,
		Reason:   "terminal_completed",
	}); err != nil {
		t.Fatalf("MarkSessionWorkerProcessReaped() error = %v", err)
	}
	active, err = backend.ListActiveWorkerProcesses(ctx)
	if err != nil {
		t.Fatalf("ListActiveWorkerProcesses() after reap error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("ListActiveWorkerProcesses() after reap len = %d, want 0", len(active))
	}
	session, err := backend.Queries().GetCodexSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetCodexSession() error = %v", err)
	}
	if session.WorkerReapOutcome.String != WorkerProcessOutcomeTerminated || session.WorkerReapReason.String != "terminal_completed" {
		t.Fatalf("persisted reap = outcome %q reason %q", session.WorkerReapOutcome.String, session.WorkerReapReason.String)
	}
}

func TestLifetimeTotalsMeasureOrphanRecoveryAndResumedCacheShare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	startedAt := time.Date(2026, 7, 10, 16, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		outcome      string
		input        int64
		cached       int64
		resumeSource int64
	}{
		{name: "resumed", outcome: OrphanRecoveryResumed, input: 1000, cached: 850, resumeSource: 41},
		{name: "fresh", outcome: OrphanRecoveryFresh, input: 400, cached: 20},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionID, err := backend.StartSession(ctx, SessionStart{
				StartedAt:             startedAt.Add(time.Duration(index) * time.Minute),
				OrphanRecoveryOutcome: tt.outcome,
				ResumedFromSessionID:  tt.resumeSource,
			})
			if err != nil {
				t.Fatalf("StartSession() error = %v", err)
			}
			if err := backend.FinishSession(ctx, sessionID, SessionFinish{
				CompletedAt:          startedAt.Add(time.Duration(index+1) * time.Minute),
				FinalState:           "completed",
				InputTokens:          tt.input,
				CachedInputTokens:    tt.cached,
				ResumedFromSessionID: tt.resumeSource,
			}); err != nil {
				t.Fatalf("FinishSession() error = %v", err)
			}
		})
	}

	totals, err := backend.LifetimeTotals(ctx)
	if err != nil {
		t.Fatalf("LifetimeTotals() error = %v", err)
	}
	if totals.OrphanResumed != 1 || totals.OrphanFresh != 1 {
		t.Fatalf("orphan continuation totals = %d/%d, want 1/1", totals.OrphanResumed, totals.OrphanFresh)
	}
	if totals.ResumedInputTokens != 1000 || totals.ResumedCachedTokens != 850 {
		t.Fatalf("resumed token totals = %d/%d, want 1000/850", totals.ResumedInputTokens, totals.ResumedCachedTokens)
	}
}

func TestBudgetCostEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)

	events := []UsageEvent{
		{
			ProjectID:      "detent",
			Model:          "gpt-5",
			CostUSD:        1.25,
			StartedAt:      time.Date(2026, 6, 1, 5, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 1, 5, 1, 0, 0, time.UTC),
			Outcome:        "completed",
			RuntimeSeconds: 60,
		},
		{
			ProjectID:      "pyroapex",
			Model:          "gpt-5",
			CostUSD:        3.5,
			StartedAt:      time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 1, 6, 1, 0, 0, time.UTC),
			Outcome:        "completed",
			RuntimeSeconds: 60,
		},
		{
			ProjectID:      "detent",
			Model:          "gpt-5",
			CostUSD:        2.75,
			StartedAt:      time.Date(2026, 6, 1, 7, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 1, 7, 1, 0, 0, time.UTC),
			Outcome:        "completed",
			RuntimeSeconds: 60,
		},
		{
			ProjectID:      "detent",
			Model:          "gpt-5",
			CostUSD:        9,
			StartedAt:      time.Date(2026, 6, 2, 1, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 2, 1, 1, 0, 0, time.UTC),
			Outcome:        "completed",
			RuntimeSeconds: 60,
		},
	}

	for _, event := range events {
		if _, err := backend.RecordUsageEvent(ctx, event); err != nil {
			t.Fatalf("RecordUsageEvent() error = %v", err)
		}
	}

	got, err := backend.BudgetCostEvents(ctx, BudgetCostQuery{
		ProjectIDs: []string{"detent"},
		From:       time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		To:         time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BudgetCostEvents() error = %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("BudgetCostEvents() len = %d, want 2: %#v", len(got), got)
	}
	if got[0].ProjectID != "detent" || got[0].CostUSD != 1.25 || got[1].CostUSD != 2.75 {
		t.Fatalf("BudgetCostEvents() = %#v, want detent costs in time order", got)
	}
}

func TestIssueSpendSinceUsesAcceptedProgressBoundaryAndIssueIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	events := []UsageEvent{
		{ProjectID: "detent", IssueID: "issue-214", Identifier: "gopherguides/gopher-ai#214", CostUSD: 9, TotalTokens: 900, StartedAt: base.Add(-time.Minute), FinishedAt: base, Outcome: "completed"},
		{ProjectID: "detent", IssueID: "issue-214", Identifier: "gopherguides/gopher-ai#214", CostUSD: 40, TotalTokens: 4_000, StartedAt: base.Add(-time.Minute), FinishedAt: base.Add(5 * time.Minute), Outcome: "completed"},
		{ProjectID: "detent", IssueID: "issue-214", Identifier: "gopherguides/gopher-ai#214", CostUSD: 1.25, TotalTokens: 125, StartedAt: base.Add(time.Minute), FinishedAt: base.Add(2 * time.Minute), Outcome: "completed"},
		{ProjectID: "detent", IssueID: "issue-214", Identifier: "gopherguides/gopher-ai#214", CostUSD: 2.5, TotalTokens: 250, StartedAt: base.Add(3 * time.Minute), FinishedAt: base.Add(4 * time.Minute), Outcome: "completed"},
		{ProjectID: "detent", IssueID: "other", Identifier: "gopherguides/gopher-ai#999", CostUSD: 20, TotalTokens: 2_000, StartedAt: base.Add(time.Minute), FinishedAt: base.Add(2 * time.Minute), Outcome: "completed"},
		{ProjectID: "other-project", IssueID: "issue-214", Identifier: "gopherguides/gopher-ai#214", CostUSD: 30, TotalTokens: 3_000, StartedAt: base.Add(time.Minute), FinishedAt: base.Add(2 * time.Minute), Outcome: "completed"},
	}
	for _, event := range events {
		if _, err := backend.RecordUsageEvent(ctx, event); err != nil {
			t.Fatalf("RecordUsageEvent() error = %v", err)
		}
	}
	capacityAttemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{
		ProjectID:     "detent",
		IssueID:       "issue-214",
		Identifier:    "gopherguides/gopher-ai#214",
		WorkerType:    "implementation",
		AttemptNumber: 3,
		StartedAt:     base.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	capacitySessionID, err := backend.StartSession(ctx, SessionStart{
		WorkAttemptID: capacityAttemptID,
		ProjectID:     "detent",
		IssueID:       "issue-214",
		Identifier:    "gopherguides/gopher-ai#214",
		StartedAt:     base.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, capacitySessionID, SessionFinish{
		CompletedAt: base.Add(6 * time.Minute),
		FinalState:  "failed",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
	if _, err := backend.RecordUsageEvent(ctx, UsageEvent{
		ProjectID:   "detent",
		SessionID:   capacitySessionID,
		IssueID:     "issue-214",
		Identifier:  "gopherguides/gopher-ai#214",
		CostUSD:     50,
		TotalTokens: 5_000,
		StartedAt:   base.Add(5 * time.Minute),
		FinishedAt:  base.Add(6 * time.Minute),
		Outcome:     "failed",
	}); err != nil {
		t.Fatalf("RecordUsageEvent(capacity) error = %v", err)
	}
	if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{
		AttemptID:     capacityAttemptID,
		CompletedAt:   base.Add(6 * time.Minute),
		Status:        WorkAttemptStatusTerminal,
		TerminalState: WorkAttemptTerminalCapacity,
		ErrorClass:    "backend_capacity",
	}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}

	spend, err := backend.IssueSpendSince(ctx, IssueSpendSinceQuery{
		ProjectID: "detent",
		IssueID:   "issue-214",
		Since:     base,
	})
	if err != nil {
		t.Fatalf("IssueSpendSince() error = %v", err)
	}
	if math.Abs(spend.CostUSD-3.75) > 0.000001 || spend.TotalTokens != 375 || spend.Sessions != 2 {
		t.Fatalf("IssueSpendSince() = %#v, want $3.75 and 375 tokens across two sessions", spend)
	}
	if !spend.FirstSessionAt.Equal(base.Add(2*time.Minute)) || !spend.LastSessionAt.Equal(base.Add(4*time.Minute)) {
		t.Fatalf("session range = %s..%s", spend.FirstSessionAt, spend.LastSessionAt)
	}
}

func TestIssueSpendSinceExcludesOnlyCapacityAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		terminal WorkAttemptTerminalState
		wantCost float64
	}{
		{terminal: WorkAttemptTerminalCapacity},
		{terminal: WorkAttemptTerminalFailure, wantCost: 50},
		{terminal: WorkAttemptTerminalSuccess, wantCost: 50},
		{terminal: WorkAttemptTerminalTimedOut, wantCost: 50},
	}
	for _, tt := range tests {
		t.Run(string(tt.terminal), func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := openTestStore(t, ctx)
			startedAt := time.Date(2026, 9, 3, 19, 0, 0, 0, time.UTC)
			attemptID, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{ProjectID: "detent", IssueID: "issue", WorkerType: "implementation", AttemptNumber: 1, StartedAt: startedAt})
			if err != nil {
				t.Fatal(err)
			}
			sessionID, err := backend.StartSession(ctx, SessionStart{WorkAttemptID: attemptID, ProjectID: "detent", IssueID: "issue", StartedAt: startedAt})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := backend.RecordUsageEvent(ctx, UsageEvent{ProjectID: "detent", IssueID: "issue", SessionID: sessionID, CostUSD: 50, TotalTokens: 5000, StartedAt: startedAt, FinishedAt: startedAt.Add(time.Minute), Outcome: "failed"}); err != nil {
				t.Fatal(err)
			}
			if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{AttemptID: attemptID, CompletedAt: startedAt.Add(time.Minute), Status: WorkAttemptStatusTerminal, TerminalState: tt.terminal}); err != nil {
				t.Fatal(err)
			}
			spend, err := backend.IssueSpendSince(ctx, IssueSpendSinceQuery{ProjectID: "detent", IssueID: "issue", Since: startedAt.Add(-time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			if spend.CostUSD != tt.wantCost || spend.TotalTokens != int64(tt.wantCost*100) {
				t.Fatalf("no-progress spend = %#v, want cost %.2f", spend, tt.wantCost)
			}
			costs, err := backend.BudgetCostEvents(ctx, BudgetCostQuery{ProjectIDs: []string{"detent"}, From: startedAt.Add(-time.Second), To: startedAt.Add(time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if len(costs) != 1 || costs[0].CostUSD != 50 {
				t.Fatalf("capacity usage disappeared from cost reporting: %#v", costs)
			}
		})
	}
}

func TestRecentModelTokenQuantilesUsesRecentCompletedSessions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	seedQuantileSession(t, ctx, backend, "gpt-5", base, 1_000_000, 100_000, 100_000)
	for index, inputTokens := range []int64{10, 20, 30, 40, 50} {
		seedQuantileSession(
			t,
			ctx,
			backend,
			"gpt-5",
			base.Add(time.Duration(index+1)*time.Minute),
			inputTokens,
			inputTokens/10,
			inputTokens/5,
		)
	}
	seedQuantileSession(t, ctx, backend, "other-model", base.Add(10*time.Minute), 9_000, 900, 900)
	if _, err := backend.StartSession(ctx, SessionStart{
		StartedAt: base.Add(11 * time.Minute),
		Model:     "gpt-5",
	}); err != nil {
		t.Fatalf("StartSession() incomplete error = %v", err)
	}

	quantiles, err := backend.RecentModelTokenQuantiles(ctx, ModelTokenQuantileQuery{
		Model: " GPT-5 ",
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("RecentModelTokenQuantiles() error = %v", err)
	}
	if quantiles.Sessions != 5 {
		t.Fatalf("Sessions = %d, want 5", quantiles.Sessions)
	}
	if quantiles.P50InputTokens != 30 || quantiles.P90InputTokens != 50 {
		t.Fatalf("input quantiles = p50 %d p90 %d, want 30/50", quantiles.P50InputTokens, quantiles.P90InputTokens)
	}
	if quantiles.P50CachedInputTokens != 3 || quantiles.P90CachedInputTokens != 5 {
		t.Fatalf("cached quantiles = p50 %d p90 %d, want 3/5", quantiles.P50CachedInputTokens, quantiles.P90CachedInputTokens)
	}
	if quantiles.P50OutputTokens != 6 || quantiles.P90OutputTokens != 10 {
		t.Fatalf("output quantiles = p50 %d p90 %d, want 6/10", quantiles.P50OutputTokens, quantiles.P90OutputTokens)
	}
	if quantiles.P50TotalTokens != 36 || quantiles.P90TotalTokens != 60 {
		t.Fatalf("total quantiles = p50 %d p90 %d, want 36/60", quantiles.P50TotalTokens, quantiles.P90TotalTokens)
	}

	empty, err := backend.RecentModelTokenQuantiles(ctx, ModelTokenQuantileQuery{Model: "missing", Limit: 5})
	if err != nil {
		t.Fatalf("RecentModelTokenQuantiles(missing) error = %v", err)
	}
	if empty.Sessions != 0 || empty.P90InputTokens != 0 {
		t.Fatalf("missing quantiles = %#v, want zero values", empty)
	}
}

func TestCycleTimeReportFromCompletedSessions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)

	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-215",
		Identifier:  "digitaldrywood/detent#215",
		StartedAt:   base.Add(-time.Hour),
		CompletedAt: base.Add(-30 * time.Minute),
		FinalState:  "failed",
	})
	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-215",
		Identifier:  "digitaldrywood/detent#215",
		StartedAt:   base,
		CompletedAt: base.Add(90 * time.Minute),
		FinalState:  "completed",
	})
	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-216",
		Identifier:  "digitaldrywood/detent#216",
		StartedAt:   base.Add(30 * time.Minute),
		CompletedAt: base.Add(2 * time.Hour),
		FinalState:  "failed",
	})
	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-215",
		Identifier:  "digitaldrywood/detent#215",
		StartedAt:   base.Add(2 * time.Hour),
		CompletedAt: base.Add(3 * time.Hour),
		FinalState:  "completed",
	})
	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-217",
		Identifier:  "digitaldrywood/detent#217",
		StartedAt:   base.Add(-24 * time.Hour),
		CompletedAt: base.Add(24 * time.Hour),
		FinalState:  "Human Review",
	})
	seedCycleSession(t, ctx, backend, cycleSessionSeed{
		IssueID:     "issue-218",
		Identifier:  "digitaldrywood/detent#218",
		StartedAt:   base.Add(-time.Hour),
		CompletedAt: base.Add(30 * time.Minute),
		FinalState:  SessionStateOrphaned,
	})

	report, err := backend.CycleTimeReport(ctx)
	if err != nil {
		t.Fatalf("CycleTimeReport() error = %v", err)
	}

	if len(report.Issues) != 2 {
		t.Fatalf("CycleTimeReport().Issues len = %d, want 2: %#v", len(report.Issues), report.Issues)
	}
	if report.Issues[0].Key != "digitaldrywood/detent#217" || report.Issues[0].DurationSeconds != int64(48*time.Hour/time.Second) {
		t.Fatalf("first issue = %#v, want #217 at 48h", report.Issues[0])
	}
	if report.Issues[1].Key != "digitaldrywood/detent#215" || report.Issues[1].DurationSeconds != int64(4*time.Hour/time.Second) || report.Issues[1].Sessions != 3 {
		t.Fatalf("second issue = %#v, want #215 at 4h across 3 sessions", report.Issues[1])
	}
	if report.AverageSeconds != int64((48*time.Hour+4*time.Hour)/2/time.Second) {
		t.Fatalf("AverageSeconds = %d, want 93600", report.AverageSeconds)
	}
	if len(report.Buckets) != 5 || report.Buckets[2].Count != 1 || report.Buckets[4].Count != 1 {
		t.Fatalf("Buckets = %#v, want counts in 4-8h and 1-3d", report.Buckets)
	}
}

func TestCycleTimeSeconds(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		want  int64
		ok    bool
	}{
		{name: "same instant is zero seconds", start: base, end: base, want: 0, ok: true},
		{name: "whole seconds between timestamps", start: base, end: base.Add(90*time.Minute + 12*time.Second), want: 5412, ok: true},
		{name: "missing start is invalid", end: base, ok: false},
		{name: "missing end is invalid", start: base, ok: false},
		{name: "end before start is invalid", start: base, end: base.Add(-time.Second), ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := cycleTimeSeconds(tt.start, tt.end)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("cycleTimeSeconds() = %d, %v; want %d, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestCycleTimeBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		issues []CycleTimeIssue
		want   []CycleTimeBucket
	}{
		{name: "no durations returns no buckets"},
		{
			name: "assigns fixed lead time ranges and trims trailing empties",
			issues: []CycleTimeIssue{
				{Key: "fast", DurationSeconds: int64(30 * time.Minute / time.Second)},
				{Key: "medium", DurationSeconds: int64(2 * time.Hour / time.Second)},
				{Key: "same range", DurationSeconds: int64(3 * time.Hour / time.Second)},
				{Key: "slow", DurationSeconds: int64(9 * 24 * time.Hour / time.Second)},
			},
			want: []CycleTimeBucket{
				{Label: "<1h", MinSeconds: 0, MaxSeconds: 3600, Count: 1},
				{Label: "1-4h", MinSeconds: 3600, MaxSeconds: 14400, Count: 2},
				{Label: "4-8h", MinSeconds: 14400, MaxSeconds: 28800},
				{Label: "8-24h", MinSeconds: 28800, MaxSeconds: 86400},
				{Label: "1-3d", MinSeconds: 86400, MaxSeconds: 259200},
				{Label: "3-7d", MinSeconds: 259200, MaxSeconds: 604800},
				{Label: "7d+", MinSeconds: 604800, Count: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cycleTimeBuckets(tt.issues)
			if len(got) != len(tt.want) {
				t.Fatalf("cycleTimeBuckets() len = %d, want %d: %#v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("bucket %d = %#v, want %#v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestWorkflowMetricsStoreRoundTripAndAggregates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	events := []WorkflowPhaseEvent{
		{
			ProjectID:         " detent ",
			IssueID:           " issue-722 ",
			Identifier:        " digitaldrywood/detent#722 ",
			IssueURL:          " https://github.com/digitaldrywood/detent/issues/722 ",
			PhaseType:         WorkflowPhaseTypeLane,
			PhaseName:         " In Progress ",
			PreviousPhaseName: "Todo",
			Status:            " exited ",
			StartedAt:         base,
			FinishedAt:        base.Add(10 * time.Minute),
			Reason:            "transition_to:Human Review",
		},
		{
			ProjectID:  "detent",
			IssueID:    "issue-723",
			Identifier: "digitaldrywood/detent#723",
			PhaseType:  WorkflowPhaseTypeLane,
			PhaseName:  "In Progress",
			Status:     "exited",
			StartedAt:  base.Add(time.Hour),
			FinishedAt: base.Add(time.Hour + 20*time.Minute),
		},
		{
			ProjectID:      "detent",
			IssueID:        "issue-722",
			Identifier:     "digitaldrywood/detent#722",
			PhaseType:      WorkflowPhaseTypeAgentSession,
			PhaseName:      "agent_active",
			Status:         "completed",
			StartedAt:      base.Add(time.Minute),
			FinishedAt:     base.Add(9 * time.Minute),
			Turns:          3,
			InputTokens:    1000,
			OutputTokens:   250,
			TotalTokens:    1250,
			MetadataJSON:   `{"session_id":42}`,
			EndpointFamily: "codex",
		},
	}
	for _, event := range events {
		if _, err := backend.RecordWorkflowPhaseEvent(ctx, event); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}

	report, err := backend.WorkflowMetricsReport(ctx, WorkflowMetricsQuery{
		ProjectID: "detent",
		From:      base.Add(-time.Minute),
		To:        base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("WorkflowMetricsReport() error = %v", err)
	}

	if len(report.Lanes) != 1 {
		t.Fatalf("WorkflowMetricsReport().Lanes len = %d, want 1: %#v", len(report.Lanes), report.Lanes)
	}
	lane := report.Lanes[0]
	if lane.ProjectID != "detent" || lane.PhaseName != "In Progress" || lane.Count != 2 {
		t.Fatalf("lane metric = %#v, want detent In Progress count 2", lane)
	}
	if lane.AverageSeconds != 900 || lane.P50Seconds != 600 || lane.P90Seconds != 1200 || lane.P95Seconds != 1200 {
		t.Fatalf("lane durations = %#v, want average 900 p50 600 p90/p95 1200", lane)
	}

	if len(report.SubPhases) != 1 {
		t.Fatalf("WorkflowMetricsReport().SubPhases len = %d, want 1: %#v", len(report.SubPhases), report.SubPhases)
	}
	subphase := report.SubPhases[0]
	if subphase.PhaseType != string(WorkflowPhaseTypeAgentSession) || subphase.PhaseName != "agent_active" || subphase.TotalSeconds != 480 {
		t.Fatalf("subphase metric = %#v, want 480s agent_active", subphase)
	}

	timeline, err := backend.IssueWorkflowTimeline(ctx, IssueIdentity{ProjectID: "detent", Identifier: "digitaldrywood/detent#722"})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	if len(timeline.Events) != 2 {
		t.Fatalf("IssueWorkflowTimeline().Events len = %d, want 2: %#v", len(timeline.Events), timeline.Events)
	}
	if timeline.Events[0].ProjectID != "detent" || timeline.Events[0].PhaseName != "In Progress" || timeline.Events[0].DurationSeconds != 600 {
		t.Fatalf("timeline first event = %#v, want normalized In Progress lane", timeline.Events[0])
	}
	if timeline.Events[1].Turns != 3 || timeline.Events[1].TotalTokens != 1250 || timeline.Events[1].MetadataJSON != `{"session_id":42}` {
		t.Fatalf("timeline agent event = %#v, want turns/tokens/metadata", timeline.Events[1])
	}
}

func TestWorkflowMetricsReportComputesLaneFlowEfficiency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		events        []WorkflowPhaseEvent
		phaseName     string
		activeSeconds int64
		waitSeconds   int64
		activePercent float64
	}{
		{
			name:      "splits active intervals from uncovered lane time",
			phaseName: "In Progress",
			events: []WorkflowPhaseEvent{
				workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeLane, "In Progress", 0, 10*time.Minute),
				workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeAgentSession, "agent_active", 2*time.Minute, 2*time.Minute),
				workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeCI, "ci", 5*time.Minute, 3*time.Minute),
			},
			activeSeconds: 300,
			waitSeconds:   300,
			activePercent: 50,
		},
		{
			name:      "caps overlapping active intervals at their union",
			phaseName: "Rework",
			events: []WorkflowPhaseEvent{
				workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeLane, "Rework", 0, 10*time.Minute),
				workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeAgentSession, "agent_active", time.Minute, 5*time.Minute),
				workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeLocalCheck, "make check", 4*time.Minute, 5*time.Minute),
			},
			activeSeconds: 480,
			waitSeconds:   120,
			activePercent: 80,
		},
		{
			name:      "treats explicit wait and unrelated work as wait",
			phaseName: "Merging",
			events: []WorkflowPhaseEvent{
				workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeLane, "Merging", 0, 10*time.Minute),
				workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeGitHubBackoff, "github_backoff", time.Minute, 4*time.Minute),
				workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeCI, "ci", 7*time.Minute, 2*time.Minute),
				workflowMetricTestEvent("detent", "issue-other", WorkflowPhaseTypeAgentSession, "agent_active", time.Minute, 8*time.Minute),
			},
			activeSeconds: 120,
			waitSeconds:   480,
			activePercent: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			backend := openTestStore(t, ctx)
			for _, event := range tt.events {
				if _, err := backend.RecordWorkflowPhaseEvent(ctx, event); err != nil {
					t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
				}
			}

			report, err := backend.WorkflowMetricsReport(ctx, WorkflowMetricsQuery{
				ProjectID: "detent",
				From:      workflowMetricTestBase.Add(-time.Minute),
				To:        workflowMetricTestBase.Add(time.Hour),
			})
			if err != nil {
				t.Fatalf("WorkflowMetricsReport() error = %v", err)
			}

			lane := workflowMetricTestLane(t, report.Lanes, tt.phaseName)
			if lane.ActiveSeconds != tt.activeSeconds || lane.WaitSeconds != tt.waitSeconds {
				t.Fatalf("%s active/wait = %d/%d, want %d/%d", tt.phaseName, lane.ActiveSeconds, lane.WaitSeconds, tt.activeSeconds, tt.waitSeconds)
			}
			if math.Abs(lane.ActivePercent-tt.activePercent) > 0.01 {
				t.Fatalf("%s active percent = %.2f, want %.2f", tt.phaseName, lane.ActivePercent, tt.activePercent)
			}
		})
	}
}

func TestWorkflowMetricsReportIncludesFlowActiveEventsAcrossWindowBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		lane          WorkflowPhaseEvent
		active        WorkflowPhaseEvent
		from          time.Time
		to            time.Time
		activeSeconds int64
		waitSeconds   int64
	}{
		{
			name:          "active event finished before report window",
			lane:          workflowMetricTestEvent("detent", "issue-boundary-before", WorkflowPhaseTypeLane, "In Progress", 0, 10*time.Minute),
			active:        workflowMetricTestEvent("detent", "issue-boundary-before", WorkflowPhaseTypeAgentSession, "agent_active", 2*time.Minute, 2*time.Minute),
			from:          workflowMetricTestBase.Add(9 * time.Minute),
			to:            workflowMetricTestBase.Add(20 * time.Minute),
			activeSeconds: 120,
			waitSeconds:   480,
		},
		{
			name:          "active event finished after report window",
			lane:          workflowMetricTestEvent("detent", "issue-boundary-after", WorkflowPhaseTypeLane, "Merging", 8*time.Minute, 4*time.Minute),
			active:        workflowMetricTestEvent("detent", "issue-boundary-after", WorkflowPhaseTypeCI, "ci", 11*time.Minute, 3*time.Minute),
			from:          workflowMetricTestBase.Add(9 * time.Minute),
			to:            workflowMetricTestBase.Add(13 * time.Minute),
			activeSeconds: 60,
			waitSeconds:   180,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			backend := openTestStore(t, ctx)
			for _, event := range []WorkflowPhaseEvent{tt.lane, tt.active} {
				if _, err := backend.RecordWorkflowPhaseEvent(ctx, event); err != nil {
					t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
				}
			}

			report, err := backend.WorkflowMetricsReport(ctx, WorkflowMetricsQuery{
				ProjectID: "detent",
				From:      tt.from,
				To:        tt.to,
			})
			if err != nil {
				t.Fatalf("WorkflowMetricsReport() error = %v", err)
			}

			lane := workflowMetricTestLane(t, report.Lanes, tt.lane.PhaseName)
			if lane.ActiveSeconds != tt.activeSeconds || lane.WaitSeconds != tt.waitSeconds {
				t.Fatalf("%s active/wait = %d/%d, want %d/%d", tt.lane.PhaseName, lane.ActiveSeconds, lane.WaitSeconds, tt.activeSeconds, tt.waitSeconds)
			}
			if len(report.SubPhases) != 0 {
				t.Fatalf("WorkflowMetricsReport().SubPhases len = %d, want 0: %#v", len(report.SubPhases), report.SubPhases)
			}
		})
	}
}

func TestWorkflowMetricsReportIncludesRepresentativeRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	events := []WorkflowPhaseEvent{
		workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeLane, "In Progress", 0, 10*time.Minute),
		workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeAgentSession, "agent_active", time.Minute, 2*time.Minute),
		workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeLane, "In Progress", 10*time.Minute, 10*time.Minute),
		workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeAgentSession, "agent_active", 12*time.Minute, 2*time.Minute),
		workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeLane, "In Progress", 20*time.Minute, 10*time.Minute),
		workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeAgentSession, "agent_active", 22*time.Minute, 2*time.Minute),
		workflowMetricTestEvent("detent", "issue-4", WorkflowPhaseTypeLane, "In Progress", 30*time.Minute, 10*time.Minute),
		workflowMetricTestEvent("detent", "issue-4", WorkflowPhaseTypeAgentSession, "agent_active", 32*time.Minute, 2*time.Minute),
		workflowMetricTestEvent("detent", "issue-other", WorkflowPhaseTypeAgentSession, "agent_active", 35*time.Minute, 2*time.Minute),
	}
	for i := range events {
		if events[i].PhaseType == WorkflowPhaseTypeAgentSession {
			events[i].RunID = 100 + int64(i)
			events[i].SessionID = 200 + int64(i)
			events[i].TotalTokens = 1_000 + int64(i)
		}
		if _, err := backend.RecordWorkflowPhaseEvent(ctx, events[i]); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}

	report, err := backend.WorkflowMetricsReport(ctx, WorkflowMetricsQuery{
		ProjectID: "detent",
		From:      workflowMetricTestBase.Add(-time.Minute),
		To:        workflowMetricTestBase.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("WorkflowMetricsReport() error = %v", err)
	}

	lane := workflowMetricTestLane(t, report.Lanes, "In Progress")
	if len(lane.Representatives) != 3 {
		t.Fatalf("Representatives len = %d, want 3: %#v", len(lane.Representatives), lane.Representatives)
	}
	wantRunIDs := []int64{107, 105, 103}
	for i, want := range wantRunIDs {
		if lane.Representatives[i].RunID != want {
			t.Fatalf("Representatives[%d].RunID = %d, want %d: %#v", i, lane.Representatives[i].RunID, want, lane.Representatives)
		}
	}
	if lane.Representatives[0].Identifier != "digitaldrywood/detent#4" || lane.Representatives[0].SessionID != 207 {
		t.Fatalf("Representatives[0] = %#v, want issue-4 active session", lane.Representatives[0])
	}
}

func TestWorkflowMetricsReportBuildsTrackedLaneTrends(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	events := []WorkflowPhaseEvent{
		workflowMetricTestEvent("detent", "issue-1", WorkflowPhaseTypeLane, "In Progress", 10*time.Minute, 2*time.Minute),
		workflowMetricTestEvent("detent", "issue-2", WorkflowPhaseTypeLane, "In Progress", 70*time.Minute, 4*time.Minute),
		workflowMetricTestEvent("detent", "issue-3", WorkflowPhaseTypeLane, "Human Review", 130*time.Minute, 6*time.Minute),
		workflowMetricTestEvent("detent", "issue-4", WorkflowPhaseTypeLane, "Merging", 190*time.Minute, 8*time.Minute),
		workflowMetricTestEvent("detent", "issue-5", WorkflowPhaseTypeLane, "Rework", 250*time.Minute, 10*time.Minute),
		workflowMetricTestEvent("detent", "issue-6", WorkflowPhaseTypeLane, "Todo", 310*time.Minute, 12*time.Minute),
	}
	for _, event := range events {
		if _, err := backend.RecordWorkflowPhaseEvent(ctx, event); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}

	report, err := backend.WorkflowMetricsReport(ctx, WorkflowMetricsQuery{
		ProjectID: "detent",
		From:      workflowMetricTestBase,
		To:        workflowMetricTestBase.Add(8 * time.Hour),
	})
	if err != nil {
		t.Fatalf("WorkflowMetricsReport() error = %v", err)
	}

	if len(report.LaneTrends) != 4 {
		t.Fatalf("LaneTrends len = %d, want 4: %#v", len(report.LaneTrends), report.LaneTrends)
	}
	for _, phaseName := range []string{"In Progress", "Human Review", "Merging", "Rework"} {
		trend := workflowMetricTestTrend(t, report.LaneTrends, phaseName)
		if len(trend.Points) != 8 {
			t.Fatalf("%s trend points len = %d, want 8", phaseName, len(trend.Points))
		}
	}
	for _, trend := range report.LaneTrends {
		if trend.PhaseName == "Todo" {
			t.Fatalf("LaneTrends included Todo: %#v", report.LaneTrends)
		}
	}
}

var workflowMetricTestBase = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

func workflowMetricTestEvent(projectID string, issueID string, phaseType WorkflowPhaseType, phaseName string, offset time.Duration, duration time.Duration) WorkflowPhaseEvent {
	startedAt := workflowMetricTestBase.Add(offset)
	return WorkflowPhaseEvent{
		ProjectID:       projectID,
		IssueID:         issueID,
		Identifier:      "digitaldrywood/detent#" + strings.TrimPrefix(issueID, "issue-"),
		IssueURL:        "https://github.com/digitaldrywood/detent/issues/" + strings.TrimPrefix(issueID, "issue-"),
		PhaseType:       phaseType,
		PhaseName:       phaseName,
		Status:          "completed",
		StartedAt:       startedAt,
		FinishedAt:      startedAt.Add(duration),
		DurationSeconds: int64(duration / time.Second),
	}
}

func workflowMetricTestLane(t *testing.T, lanes []WorkflowPhaseMetric, phaseName string) WorkflowPhaseMetric {
	t.Helper()
	for _, lane := range lanes {
		if lane.PhaseName == phaseName {
			return lane
		}
	}
	t.Fatalf("missing lane %q in %#v", phaseName, lanes)
	return WorkflowPhaseMetric{}
}

func workflowMetricTestTrend(t *testing.T, trends []WorkflowLaneTrend, phaseName string) WorkflowLaneTrend {
	t.Helper()
	for _, trend := range trends {
		if trend.PhaseName == phaseName {
			return trend
		}
	}
	t.Fatalf("missing trend %q in %#v", phaseName, trends)
	return WorkflowLaneTrend{}
}

func TestFairShareStoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	dispatchedAt := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)

	if err := backend.RecordFairShareDispatch(ctx, FairShareDispatch{
		ProjectID:      " alpha ",
		Weight:         2,
		RuntimeSeconds: 30,
		DispatchedAt:   dispatchedAt,
	}); err != nil {
		t.Fatalf("RecordFairShareDispatch() first error = %v", err)
	}
	if err := backend.RecordFairShareDispatch(ctx, FairShareDispatch{
		ProjectID:      "alpha",
		Weight:         2,
		RuntimeSeconds: 45,
		DispatchedAt:   dispatchedAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("RecordFairShareDispatch() second error = %v", err)
	}

	usage, err := backend.ListFairShareUsage(ctx)
	if err != nil {
		t.Fatalf("ListFairShareUsage() error = %v", err)
	}
	if len(usage) != 1 {
		t.Fatalf("usage len = %d, want 1: %#v", len(usage), usage)
	}

	got := usage[0]
	if got.ProjectID != "alpha" {
		t.Fatalf("ProjectID = %q, want alpha", got.ProjectID)
	}
	if got.Weight != 2 {
		t.Fatalf("Weight = %d, want 2", got.Weight)
	}
	if got.Dispatches != 2 {
		t.Fatalf("Dispatches = %d, want 2", got.Dispatches)
	}
	if got.RuntimeSeconds != 75 {
		t.Fatalf("RuntimeSeconds = %d, want 75", got.RuntimeSeconds)
	}
	if !got.UpdatedAt.Equal(dispatchedAt.Add(time.Minute)) {
		t.Fatalf("UpdatedAt = %s, want %s", got.UpdatedAt, dispatchedAt.Add(time.Minute))
	}
}

func TestUsageLedgerRoundTrip(t *testing.T) {
	t.Parallel()

	modelContextWindow := int64(128000)
	projectedCost := 0.0008
	tests := []struct {
		name  string
		event UsageEvent
	}{
		{
			name: "persists usage event across reopen",
			event: UsageEvent{
				ProjectID:              " detent ",
				RunID:                  11,
				SessionID:              42,
				IssueID:                " I_kwDOSskuwc8AAAABD6psJQ ",
				Identifier:             " digitaldrywood/detent#117 ",
				PRNumber:               int64Ptr(91),
				Model:                  " gpt-5-codex ",
				InputTokens:            123,
				CachedInputTokens:      67,
				OutputTokens:           45,
				ReasoningOutputTokens:  9,
				TotalTokens:            168,
				ModelContextWindow:     &modelContextWindow,
				CostUSD:                0.00123,
				ProjectedCostUSD:       &projectedCost,
				ProjectionOvershootUSD: 0.00043,
				RuntimeSeconds:         73,
				StartedAt:              time.Date(2026, 5, 31, 13, 0, 0, 0, time.UTC),
				FinishedAt:             time.Date(2026, 5, 31, 13, 1, 13, 0, time.UTC),
				Outcome:                " completed ",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "detent.db")

			backend, err := Open(ctx, Config{
				Backend: BackendSQLite,
				Path:    dbPath,
			})
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			eventID, err := backend.RecordUsageEvent(ctx, tt.event)
			if err != nil {
				t.Fatalf("RecordUsageEvent() error = %v", err)
			}

			got, err := backend.Queries().GetUsageEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("GetUsageEvent() error = %v", err)
			}
			if got.ProjectID != "detent" {
				t.Fatalf("ProjectID = %q, want detent", got.ProjectID)
			}
			if got.RunID.Int64 != 11 || got.SessionID.Int64 != 42 {
				t.Fatalf("run/session = %d/%d, want 11/42", got.RunID.Int64, got.SessionID.Int64)
			}
			if got.IssueID.String != "I_kwDOSskuwc8AAAABD6psJQ" || got.Identifier.String != "digitaldrywood/detent#117" {
				t.Fatalf("issue identity = %q/%q", got.IssueID.String, got.Identifier.String)
			}
			if got.PrNumber.Int64 != 91 {
				t.Fatalf("pr_number = %d, want 91", got.PrNumber.Int64)
			}
			if got.Model != "gpt-5-codex" {
				t.Fatalf("model = %q, want gpt-5-codex", got.Model)
			}
			if got.InputTokens != 123 || got.OutputTokens != 45 || got.TotalTokens != 168 || got.RuntimeSeconds != 73 {
				t.Fatalf("tokens/runtime = %d/%d/%d/%d", got.InputTokens, got.OutputTokens, got.TotalTokens, got.RuntimeSeconds)
			}
			if !got.CachedInputTokens.Valid || got.CachedInputTokens.Int64 != 67 {
				t.Fatalf("cached_input_tokens = %#v, want 67", got.CachedInputTokens)
			}
			if !got.ReasoningOutputTokens.Valid || got.ReasoningOutputTokens.Int64 != 9 {
				t.Fatalf("reasoning_output_tokens = %#v, want 9", got.ReasoningOutputTokens)
			}
			if !got.ModelContextWindow.Valid || got.ModelContextWindow.Int64 != modelContextWindow {
				t.Fatalf("model_context_window = %#v, want %d", got.ModelContextWindow, modelContextWindow)
			}
			if got.CostUsd != 0.00123 {
				t.Fatalf("cost_usd = %.12f, want 0.001230000000", got.CostUsd)
			}
			if !got.ProjectedCostUsd.Valid || got.ProjectedCostUsd.Float64 != projectedCost {
				t.Fatalf("projected_cost_usd = %#v, want %.12f", got.ProjectedCostUsd, projectedCost)
			}
			if got.ProjectionOvershootUsd != 0.00043 {
				t.Fatalf("projection_overshoot_usd = %.12f, want 0.000430000000", got.ProjectionOvershootUsd)
			}
			if got.StartedAt != "2026-05-31T13:00:00Z" || got.FinishedAt != "2026-05-31T13:01:13Z" {
				t.Fatalf("timestamps = %q/%q", got.StartedAt, got.FinishedAt)
			}
			if got.EventDay != "2026-05-31" {
				t.Fatalf("event_day = %q, want 2026-05-31", got.EventDay)
			}
			if got.Outcome != "completed" {
				t.Fatalf("outcome = %q, want completed", got.Outcome)
			}

			if err := backend.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			reopened, err := Open(ctx, Config{
				Backend: BackendSQLite,
				Path:    dbPath,
			})
			if err != nil {
				t.Fatalf("reopen Open() error = %v", err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Fatalf("reopened Close() error = %v", err)
				}
			})

			persisted, err := reopened.Queries().GetUsageEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("GetUsageEvent() after reopen error = %v", err)
			}
			if persisted.TotalTokens != 168 {
				t.Fatalf("persisted total_tokens = %d, want 168", persisted.TotalTokens)
			}
			if !persisted.CachedInputTokens.Valid || persisted.CachedInputTokens.Int64 != 67 {
				t.Fatalf("persisted cached_input_tokens = %#v, want 67", persisted.CachedInputTokens)
			}
			if !persisted.ReasoningOutputTokens.Valid || persisted.ReasoningOutputTokens.Int64 != 9 {
				t.Fatalf("persisted reasoning_output_tokens = %#v, want 9", persisted.ReasoningOutputTokens)
			}
			if !persisted.ModelContextWindow.Valid || persisted.ModelContextWindow.Int64 != modelContextWindow {
				t.Fatalf("persisted model_context_window = %#v, want %d", persisted.ModelContextWindow, modelContextWindow)
			}
			if persisted.CostUsd != 0.00123 {
				t.Fatalf("persisted cost_usd = %.12f, want 0.001230000000", persisted.CostUsd)
			}
		})
	}
}

func TestUsageReportAggregates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	seedUsageReportEvents(t, ctx, backend)

	tests := []struct {
		name  string
		query UsageReportQuery
		want  []UsageReportRow
	}{
		{
			name:  "by day with inclusive range",
			query: UsageReportQuery{By: UsageReportByDay, From: dateOnly(2026, 5, 31), To: dateOnly(2026, 6, 1)},
			want: []UsageReportRow{
				{
					Key:                   "2026-05-31",
					InputTokens:           150,
					CachedInputTokens:     45,
					OutputTokens:          75,
					ReasoningOutputTokens: 15,
					TotalTokens:           225,
					ModelContextWindow:    200000,
					RuntimeSeconds:        45,
					Events:                2,
				},
				{
					Key:                   "2026-06-01",
					InputTokens:           70,
					CachedInputTokens:     70,
					OutputTokens:          30,
					ReasoningOutputTokens: 12,
					TotalTokens:           100,
					ModelContextWindow:    100000,
					RuntimeSeconds:        25,
					Events:                1,
				},
			},
		},
		{
			name:  "by project",
			query: UsageReportQuery{By: UsageReportByProject, From: dateOnly(2026, 5, 31), To: dateOnly(2026, 6, 1)},
			want: []UsageReportRow{
				{
					Key:                   "detent",
					InputTokens:           220,
					CachedInputTokens:     115,
					OutputTokens:          105,
					ReasoningOutputTokens: 27,
					TotalTokens:           325,
					ModelContextWindow:    200000,
					RuntimeSeconds:        70,
					Events:                3,
				},
			},
		},
		{
			name:  "by issue",
			query: UsageReportQuery{By: UsageReportByIssue},
			want: []UsageReportRow{
				{
					Key:                   "digitaldrywood/detent#117",
					InputTokens:           100,
					CachedInputTokens:     30,
					OutputTokens:          50,
					ReasoningOutputTokens: 10,
					TotalTokens:           150,
					ModelContextWindow:    200000,
					RuntimeSeconds:        30,
					Events:                1,
				},
				{
					Key:                   "digitaldrywood/detent#119",
					InputTokens:           120,
					CachedInputTokens:     85,
					OutputTokens:          55,
					ReasoningOutputTokens: 17,
					TotalTokens:           175,
					ModelContextWindow:    128000,
					RuntimeSeconds:        40,
					Events:                2,
				},
				{
					Key:            "unassigned",
					InputTokens:    5,
					OutputTokens:   2,
					TotalTokens:    7,
					RuntimeSeconds: 3,
					Events:         1,
				},
			},
		},
		{
			name:  "by PR",
			query: UsageReportQuery{By: UsageReportByPR},
			want: []UsageReportRow{
				{
					Key:                   "detent#133",
					InputTokens:           100,
					CachedInputTokens:     30,
					OutputTokens:          50,
					ReasoningOutputTokens: 10,
					TotalTokens:           150,
					ModelContextWindow:    200000,
					RuntimeSeconds:        30,
					Events:                1,
				},
				{
					Key:                   "detent#141",
					InputTokens:           120,
					CachedInputTokens:     85,
					OutputTokens:          55,
					ReasoningOutputTokens: 17,
					TotalTokens:           175,
					ModelContextWindow:    128000,
					RuntimeSeconds:        40,
					Events:                2,
				},
				{
					Key:            "pyroapex#141",
					InputTokens:    5,
					OutputTokens:   2,
					TotalTokens:    7,
					RuntimeSeconds: 3,
					Events:         1,
				},
			},
		},
		{
			name:  "by model",
			query: UsageReportQuery{By: UsageReportByModel, From: dateOnly(2026, 5, 31), To: dateOnly(2026, 6, 1)},
			want: []UsageReportRow{
				{
					Key:                   "gpt-5.4",
					InputTokens:           150,
					CachedInputTokens:     45,
					OutputTokens:          75,
					ReasoningOutputTokens: 15,
					TotalTokens:           225,
					ModelContextWindow:    200000,
					RuntimeSeconds:        45,
					Events:                2,
				},
				{
					Key:                   "gpt-5.4-mini",
					InputTokens:           70,
					CachedInputTokens:     70,
					OutputTokens:          30,
					ReasoningOutputTokens: 12,
					TotalTokens:           100,
					ModelContextWindow:    100000,
					RuntimeSeconds:        25,
					Events:                1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			report, err := backend.UsageReport(ctx, tt.query)
			if err != nil {
				t.Fatalf("UsageReport() error = %v", err)
			}
			assertUsageRows(t, report.Rows, tt.want)
		})
	}
}

func TestUsageReportRejectsInvalidRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)

	_, err := backend.UsageReport(ctx, UsageReportQuery{
		By:   UsageReportByDay,
		From: dateOnly(2026, 6, 2),
		To:   dateOnly(2026, 6, 1),
	})
	if err == nil {
		t.Fatal("UsageReport() error = nil, want invalid date range")
	}
}

func TestDailyDigestReconcilesRuntimeTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := openTestStore(t, ctx)
	from := time.Date(2026, 7, 10, 5, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)

	seedDigestSession(t, ctx, backend, SessionStart{StartedAt: from.Add(time.Hour), Model: "gpt-a", OrphanRecoveryOutcome: OrphanRecoveryResumed}, SessionFinish{
		CompletedAt: from.Add(2 * time.Hour), InputTokens: 1000, CachedInputTokens: 900, OutputTokens: 100, TotalTokens: 1100, FinalState: "completed", Model: "gpt-a",
	})
	seedDigestSession(t, ctx, backend, SessionStart{StartedAt: from.Add(3 * time.Hour), RequestedModel: "gpt-b", OrphanRecoveryOutcome: OrphanRecoveryFresh}, SessionFinish{
		CompletedAt: from.Add(4 * time.Hour), InputTokens: 500, CachedInputTokens: 400, OutputTokens: 50, TotalTokens: 550, FinalState: "failed", Model: "gpt-b",
	})
	seedDigestSession(t, ctx, backend, SessionStart{StartedAt: from.Add(-time.Hour), Model: "outside"}, SessionFinish{
		CompletedAt: from.Add(-time.Second), InputTokens: 9000, CachedInputTokens: 9000, OutputTokens: 9000, TotalTokens: 18000, FinalState: "failed", Model: "outside",
	})

	seedDigestAttempt(t, ctx, backend, "capacity", from.Add(-time.Hour), from.Add(time.Hour), WorkAttemptTerminalCapacity, "quota", "retry_resume")
	seedDigestAttempt(t, ctx, backend, "failure-a", from.Add(8*time.Hour), from.Add(9*time.Hour), WorkAttemptTerminalFailure, "auth", "")
	seedDigestAttempt(t, ctx, backend, "failure-b", from.Add(10*time.Hour), from.Add(11*time.Hour), WorkAttemptTerminalTimedOut, "auth", "")
	for _, identifier := range []string{"digitaldrywood/detent#1", "digitaldrywood/detent#1"} {
		if _, err := backend.RecordSchedulerDecision(ctx, SchedulerDecision{ProjectID: "detent", Identifier: identifier, Result: SchedulerDecisionResultSkipped, Reason: "repeated_failure_circuit_breaker", DecisionAt: from.Add(12 * time.Hour)}); err != nil {
			t.Fatalf("RecordSchedulerDecision() error = %v", err)
		}
	}
	if _, err := backend.RecordWorkflowPhaseEvent(ctx, WorkflowPhaseEvent{ProjectID: "detent", Identifier: "digitaldrywood/detent#1", PhaseType: WorkflowPhaseTypeLane, PhaseName: "Blocked", Reason: "repeated_failure_circuit_breaker", Status: "entered", StartedAt: from.Add(12 * time.Hour)}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}

	days, err := backend.DailyDigest(ctx, []DailyDigestWindow{
		{Date: "2026-07-09", From: from.Add(-24 * time.Hour), To: from},
		{Date: "2026-07-10", From: from, To: to},
	})
	if err != nil {
		t.Fatalf("DailyDigest() error = %v", err)
	}
	if len(days) != 2 {
		t.Fatalf("DailyDigest() len = %d, want 2", len(days))
	}
	previous, day := days[0], days[1]
	if previous.CapacityOutages != 1 || previous.CapacitySeconds != 3600 {
		t.Fatalf("previous capacity = %#v, want the first boundary-clipped outage hour", previous)
	}
	if day.Sessions != 2 || day.InputTokens != 1500 || day.CachedInputTokens != 1300 || day.OutputTokens != 150 || day.TotalTokens != 1650 {
		t.Fatalf("token totals = %#v, want exact in-window session sums", day)
	}
	if day.OrphanResumed != 1 || day.OrphanFresh != 1 || day.FailedSessions != 1 {
		t.Fatalf("session outcomes = %#v, want resumed/fresh/failed 1/1/1", day)
	}
	if day.CapacityOutages != 1 || day.CapacitySeconds != 3600 || day.CapacityRecoveryMode != "retry_resume" {
		t.Fatalf("capacity = %#v, want one boundary-clipped retry_resume outage hour", day)
	}
	if day.BreakerTrips != 1 || day.DominantErrorClass != "auth" {
		t.Fatalf("failure summary = %#v, want one distinct breaker and dominant auth", day)
	}
	if len(day.Models) != 2 || day.Models[0].Model != "gpt-a" || day.Models[1].Model != "gpt-b" {
		t.Fatalf("models = %#v, want exact gpt-a/gpt-b breakdown", day.Models)
	}
}

func seedDigestSession(t *testing.T, ctx context.Context, backend Store, start SessionStart, finish SessionFinish) {
	t.Helper()
	id, err := backend.StartSession(ctx, start)
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, id, finish); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
}

func seedDigestAttempt(t *testing.T, ctx context.Context, backend Store, issueID string, startedAt time.Time, completedAt time.Time, terminal WorkAttemptTerminalState, errorClass string, nextAction string) {
	t.Helper()
	id, err := backend.StartWorkAttempt(ctx, WorkAttemptStart{ProjectID: "detent", IssueID: issueID, Identifier: "digitaldrywood/detent#" + issueID, WorkerType: "agent", StartedAt: startedAt})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	if err := backend.CompleteWorkAttempt(ctx, WorkAttemptCompletion{AttemptID: id, CompletedAt: completedAt, Status: WorkAttemptStatusTerminal, TerminalState: terminal, ErrorClass: errorClass, NextAction: nextAction}); err != nil {
		t.Fatalf("CompleteWorkAttempt() error = %v", err)
	}
}

func TestOpenRejectsUnsupportedBackend(t *testing.T) {
	t.Parallel()

	_, err := Open(context.Background(), Config{
		Backend: Backend("postgres"),
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err == nil {
		t.Fatal("Open() error = nil, want unsupported backend error")
	}
}

func TestOpenUsesSQLiteBackendByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, err := Open(ctx, Config{
		Path:        filepath.Join(t.TempDir(), "detent.db"),
		BusyTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	sqliteBackend, ok := backend.(*sqliteStore)
	if !ok {
		t.Fatalf("Open() returned %T, want *sqliteStore", backend)
	}
	if got := queryInt(t, sqliteBackend.db, "PRAGMA busy_timeout"); got != 2500 {
		t.Fatalf("busy_timeout = %d, want 2500", got)
	}
}

func TestOpenSQLiteRejectsMissingPath(t *testing.T) {
	t.Parallel()

	_, err := Open(context.Background(), Config{
		Backend: BackendSQLite,
	})
	if err == nil {
		t.Fatal("Open() error = nil, want missing path error")
	}
}

func TestBusyTimeoutMillis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
		want    int64
	}{
		{name: "default for zero", timeout: 0, want: 5000},
		{name: "default for negative", timeout: -time.Second, want: 5000},
		{name: "minimum positive", timeout: time.Nanosecond, want: 1},
		{name: "configured duration", timeout: 3 * time.Second, want: 3000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := busyTimeoutMillis(tt.timeout); got != tt.want {
				t.Fatalf("busyTimeoutMillis(%s) = %d, want %d", tt.timeout, got, tt.want)
			}
		})
	}
}

func openTestStore(t *testing.T, ctx context.Context) Store {
	t.Helper()

	database, err := migratedTestDatabase()
	if err != nil {
		t.Fatalf("build migrated store fixture: %v", err)
	}
	path := filepath.Join(t.TempDir(), "detent.db")
	if err := os.WriteFile(path, database, 0o600); err != nil {
		t.Fatalf("write migrated store fixture: %v", err)
	}
	backend, err := Open(ctx, Config{
		Backend: BackendSQLite,
		Path:    path,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return backend
}

var migratedTestDatabase = sync.OnceValues(func() (_ []byte, returnErr error) {
	dir, err := os.MkdirTemp("", "detent-store-test-")
	if err != nil {
		return nil, err
	}
	defer func() {
		returnErr = errors.Join(returnErr, os.RemoveAll(dir))
	}()

	path := filepath.Join(dir, "detent.db")
	backend, err := Open(context.Background(), Config{Backend: BackendSQLite, Path: path})
	if err != nil {
		return nil, err
	}
	if err := backend.Close(); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
})

type cycleSessionSeed struct {
	IssueID     string
	Identifier  string
	StartedAt   time.Time
	CompletedAt time.Time
	FinalState  string
}

func seedCycleSession(t *testing.T, ctx context.Context, backend Store, seed cycleSessionSeed) {
	t.Helper()

	sessionID, err := backend.StartSession(ctx, SessionStart{
		IssueID:    seed.IssueID,
		Identifier: seed.Identifier,
		StartedAt:  seed.StartedAt,
		Model:      "gpt-5-codex",
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, sessionID, SessionFinish{
		CompletedAt:    seed.CompletedAt,
		RuntimeSeconds: int64(seed.CompletedAt.Sub(seed.StartedAt) / time.Second),
		FinalState:     seed.FinalState,
		Model:          "gpt-5-codex",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
}

func seedQuantileSession(
	t *testing.T,
	ctx context.Context,
	backend Store,
	model string,
	completedAt time.Time,
	inputTokens int64,
	cachedInputTokens int64,
	outputTokens int64,
) {
	t.Helper()

	sessionID, err := backend.StartSession(ctx, SessionStart{
		StartedAt: completedAt.Add(-time.Minute),
		Model:     model,
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := backend.FinishSession(ctx, sessionID, SessionFinish{
		CompletedAt:       completedAt,
		InputTokens:       inputTokens,
		CachedInputTokens: cachedInputTokens,
		OutputTokens:      outputTokens,
		TotalTokens:       inputTokens + outputTokens,
		FinalState:        "completed",
		Model:             model,
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
}

func seedUsageReportEvents(t *testing.T, ctx context.Context, backend Store) {
	t.Helper()

	contextWindow200K := int64(200000)
	contextWindow128K := int64(128000)
	contextWindow100K := int64(100000)
	events := []UsageEvent{
		{
			ProjectID:             "detent",
			IssueID:               "issue-117",
			Identifier:            "digitaldrywood/detent#117",
			PRNumber:              int64Ptr(133),
			Model:                 "gpt-5.4",
			InputTokens:           100,
			CachedInputTokens:     30,
			OutputTokens:          50,
			ReasoningOutputTokens: 10,
			TotalTokens:           150,
			ModelContextWindow:    &contextWindow200K,
			RuntimeSeconds:        30,
			StartedAt:             time.Date(2026, 5, 31, 9, 0, 0, 0, time.UTC),
			FinishedAt:            time.Date(2026, 5, 31, 9, 1, 0, 0, time.UTC),
			Outcome:               "completed",
		},
		{
			ProjectID:             "detent",
			IssueID:               "issue-119",
			Identifier:            "digitaldrywood/detent#119",
			PRNumber:              int64Ptr(141),
			Model:                 "gpt-5.4",
			InputTokens:           50,
			CachedInputTokens:     15,
			OutputTokens:          25,
			ReasoningOutputTokens: 5,
			TotalTokens:           75,
			ModelContextWindow:    &contextWindow128K,
			RuntimeSeconds:        15,
			StartedAt:             time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
			FinishedAt:            time.Date(2026, 5, 31, 10, 1, 0, 0, time.UTC),
			Outcome:               "completed",
		},
		{
			ProjectID:             "detent",
			IssueID:               "issue-119",
			Identifier:            "digitaldrywood/detent#119",
			PRNumber:              int64Ptr(141),
			Model:                 "gpt-5.4-mini",
			InputTokens:           70,
			CachedInputTokens:     70,
			OutputTokens:          30,
			ReasoningOutputTokens: 12,
			TotalTokens:           100,
			ModelContextWindow:    &contextWindow100K,
			RuntimeSeconds:        25,
			StartedAt:             time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC),
			FinishedAt:            time.Date(2026, 6, 1, 11, 1, 0, 0, time.UTC),
			Outcome:               "completed",
		},
		{
			ProjectID:      "pyroapex",
			PRNumber:       int64Ptr(141),
			Model:          "",
			InputTokens:    5,
			OutputTokens:   2,
			TotalTokens:    7,
			RuntimeSeconds: 3,
			StartedAt:      time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC),
			FinishedAt:     time.Date(2026, 6, 2, 12, 1, 0, 0, time.UTC),
			Outcome:        "completed",
		},
	}

	for _, event := range events {
		if _, err := backend.RecordUsageEvent(ctx, event); err != nil {
			t.Fatalf("RecordUsageEvent() error = %v", err)
		}
	}
}

func assertUsageRows(t *testing.T, got []UsageReportRow, want []UsageReportRow) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("rows len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Key != want[i].Key ||
			got[i].InputTokens != want[i].InputTokens ||
			got[i].CachedInputTokens != want[i].CachedInputTokens ||
			got[i].OutputTokens != want[i].OutputTokens ||
			got[i].ReasoningOutputTokens != want[i].ReasoningOutputTokens ||
			got[i].TotalTokens != want[i].TotalTokens ||
			got[i].ModelContextWindow != want[i].ModelContextWindow ||
			got[i].RuntimeSeconds != want[i].RuntimeSeconds ||
			got[i].Events != want[i].Events {
			t.Fatalf("row %d = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func int64Ptr(value int64) *int64 {
	return &value
}

func dateOnly(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func int64Pointer(value int64) *int64 {
	return &value
}

func queryString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()

	var value string
	if err := db.QueryRowContext(t.Context(), query).Scan(&value); err != nil {
		t.Fatalf("querying %q: %v", query, err)
	}
	return value
}

func queryInt(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()

	var value int64
	if err := db.QueryRowContext(t.Context(), query).Scan(&value); err != nil {
		t.Fatalf("querying %q: %v", query, err)
	}
	return value
}

func assertColumnPresent(t *testing.T, db *sql.DB, table string, column string) {
	t.Helper()

	if count := columnCount(t, db, table, column); count != 1 {
		t.Fatalf("%s.%s column count = %d, want 1", table, column, count)
	}
}

func assertColumnAbsent(t *testing.T, db *sql.DB, table string, column string) {
	t.Helper()

	if count := columnCount(t, db, table, column); count != 0 {
		t.Fatalf("%s.%s column count = %d, want 0", table, column, count)
	}
}

func assertIndexPresent(t *testing.T, db *sql.DB, index string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index).Scan(&count); err != nil {
		t.Fatalf("query index %q error = %v", index, err)
	}
	if count != 1 {
		t.Fatalf("index %q count = %d, want 1", index, count)
	}
}

func assertIndexAbsent(t *testing.T, db *sql.DB, index string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index).Scan(&count); err != nil {
		t.Fatalf("query index %q error = %v", index, err)
	}
	if count != 0 {
		t.Fatalf("index %q count = %d, want 0", index, count)
	}
}

func columnCount(t *testing.T, db *sql.DB, table string, column string) int64 {
	t.Helper()

	var count int64
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?", tableIdentifier(t, table), column).Scan(&count); err != nil {
		t.Fatalf("querying %s.%s column: %v", table, column, err)
	}
	return count
}

func assertTelemetryColumnsNull(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	query := "SELECT CASE WHEN cached_input_tokens IS NULL AND reasoning_output_tokens IS NULL AND model_context_window IS NULL THEN 1 ELSE 0 END FROM " + tableIdentifier(t, table) + " LIMIT 1"
	if got := queryInt(t, db, query); got != 1 {
		t.Fatalf("%s new telemetry columns null = %d, want 1", table, got)
	}
}

func tableIdentifier(t *testing.T, table string) string {
	t.Helper()

	switch table {
	case "codex_sessions", "usage_events", "workflow_phase_events":
		return table
	default:
		t.Fatalf("unexpected table %q", table)
		return ""
	}
}

func TestCompletionFenceRevocationMigrationAndAccounting(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "detent.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := configureSQLite(ctx, db, 0); err != nil {
		t.Fatal(err)
	}
	migrationMu.Lock()
	defer migrationMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpToContext(ctx, db, "migrations", 54); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, terminal, message, metadata string
		flagged, active                   bool
	}{
		{name: "legacy error", terminal: "lane_revoked", message: "completion_fence_unavailable", metadata: `{}`, flagged: true},
		{name: "delivered metadata", terminal: "delivered", metadata: `{"lane_revocation":{"reason":"completion_fence_unavailable"},"delivery_receipt":{"schema":1,"kind":"pushed_work_product"}}`, flagged: true},
		{name: "revoked metadata", terminal: "lane_revoked", metadata: `{"lane_revocation":{"reason":"completion_fence_unavailable"}}`, flagged: true},
		{name: "earlier migration", terminal: "delivered", metadata: `{"historical_lane_revocation":{"original_error_message":"completion_fence_unavailable"}}`, flagged: true},
		{name: "receipt backfill", terminal: "delivered", metadata: `{"lane_revocation_receipt_backfill":{"original_error_message":"completion_fence_unavailable"}}`, flagged: true},
		{name: "malformed legacy metadata", terminal: "lane_revoked", message: "completion_fence_unavailable", metadata: `{broken`, flagged: true},
		{name: "verified lane change", terminal: "lane_revoked", message: "tracker_lane_changed", metadata: `{}`},
		{name: "ordinary success", terminal: "success", metadata: `{}`},
		{name: "active completion wait", metadata: `{}`, active: true},
	}
	for i, tc := range cases {
		status, phase := "terminal", "lane_revoked"
		var completed any = "2026-09-07T11:00:00Z"
		if tc.active {
			status, phase, completed = "active", "completion_deferred", nil
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO work_attempts
		(id, project_id, issue_id, worker_type, status, started_at, completed_at, terminal_state, error_message, phase, worker_metadata_json)
		VALUES (?, 'detent', 'issue', 'implementation', ?, '2026-09-07T10:00:00Z', ?, ?, ?, ?, ?)`, i+1, status, completed, tc.terminal, tc.message, phase, tc.metadata); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO codex_sessions (id, work_attempt_id, project_id, issue_id, started_at, completed_at, final_state)
		VALUES (?, ?, 'detent', 'issue', '2026-09-07T10:00:00Z', '2026-09-07T11:00:00Z', 'completed')`, i+1, i+1); err != nil {
			t.Fatal(err)
		}
	}
	backend := &sqliteStore{db: db, queries: sqlc.New(db)}
	startedAt := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	for i := range cases {
		if _, err := backend.RecordUsageEvent(ctx, UsageEvent{ProjectID: "detent", IssueID: "issue", SessionID: int64(i + 1), CostUSD: 50, TotalTokens: 5000, StartedAt: startedAt, FinishedAt: startedAt.Add(time.Hour), Outcome: "completed"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := goose.UpToContext(ctx, db, "migrations", 55); err != nil {
		t.Fatal(err)
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var metadata string
			if err := db.QueryRowContext(ctx, "SELECT worker_metadata_json FROM work_attempts WHERE id = ?", i+1).Scan(&metadata); err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(metadata, `"historical_completion_fence"`); got != tc.flagged {
				t.Fatalf("metadata = %s, flagged = %v", metadata, tc.flagged)
			}
		})
	}
	spend, err := backend.IssueSpendSince(ctx, IssueSpendSinceQuery{ProjectID: "detent", IssueID: "issue", Since: startedAt.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if spend.CostUSD != 100 || spend.TotalTokens != 10000 || spend.Sessions != 2 {
		t.Fatalf("progress spend = %#v, want only ordinary attempts", spend)
	}
	history, err := backend.ListRecentTerminalWorkAttempts(ctx, WorkAttemptHistoryQuery{ProjectID: "detent", IssueID: "issue"})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("progress history = %#v, want two ordinary attempts", history)
	}
	cycles, err := backend.CycleTimeReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cycles.Issues) != 1 || cycles.Issues[0].Sessions != 2 {
		t.Fatalf("completed cycles = %#v", cycles)
	}
	costs, err := backend.BudgetCostEvents(ctx, BudgetCostQuery{ProjectIDs: []string{"detent"}, From: startedAt.Add(-time.Second), To: startedAt.Add(2 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(costs) != len(cases) {
		t.Fatalf("billing events = %d, want %d", len(costs), len(cases))
	}
	if got := queryInt(t, db, "SELECT COUNT(*) FROM codex_sessions WHERE final_state = 'completed'"); got != int64(len(cases)) {
		t.Fatal("migration rewrote session history")
	}
	if err := goose.DownToContext(ctx, db, "migrations", 54); err != nil {
		t.Fatal(err)
	}
	for i, tc := range cases {
		var metadata string
		if err := db.QueryRowContext(ctx, "SELECT worker_metadata_json FROM work_attempts WHERE id = ?", i+1).Scan(&metadata); err != nil {
			t.Fatal(err)
		}
		if metadata != tc.metadata {
			t.Fatalf("restored metadata = %s, want %s", metadata, tc.metadata)
		}
	}
}
