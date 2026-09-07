package hubserver

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/pressly/goose/v3"
)

const (
	hubSchemaTable         = "hub_schema_version"
	supportedSchemaVersion = int64(18)
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func runMigrations(ctx context.Context, db *sql.DB, logger *slog.Logger) (int64, error) {
	migrations, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return 0, fmt.Errorf("open hub migrations: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectSQLite3,
		db,
		migrations,
		goose.WithDisableGlobalRegistry(true),
		goose.WithTableName(hubSchemaTable),
		goose.WithSlog(logger),
	)
	if err != nil {
		return 0, fmt.Errorf("create hub migration provider: %w", err)
	}
	current, target, err := provider.GetVersions(ctx)
	if err != nil {
		return 0, fmt.Errorf("read hub schema versions: %w", err)
	}
	if target != supportedSchemaVersion {
		return 0, fmt.Errorf("embedded hub schema version is %d, want %d", target, supportedSchemaVersion)
	}
	if current > target {
		return 0, fmt.Errorf("%w: database=%d supported=%d", ErrUnsupportedSchema, current, target)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return 0, fmt.Errorf("prepare hub schema rebuild: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return 0, fmt.Errorf("apply hub migrations: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return 0, fmt.Errorf("restore hub foreign key enforcement: %w", err)
	}
	var violations int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_foreign_key_check").Scan(&violations); err != nil {
		return 0, fmt.Errorf("validate hub foreign keys: %w", err)
	}
	if violations != 0 {
		return 0, fmt.Errorf("hub migration has %d foreign key violations", violations)
	}
	current, target, err = provider.GetVersions(ctx)
	if err != nil {
		return 0, fmt.Errorf("verify hub schema versions: %w", err)
	}
	if current != target {
		return 0, fmt.Errorf("hub schema migration stopped at %d, want %d", current, target)
	}
	return current, nil
}

func currentSchemaVersion(ctx context.Context, db *sql.DB) (int64, error) {
	var version int64
	query := fmt.Sprintf("SELECT COALESCE(MAX(version_id), 0) FROM %s WHERE is_applied = 1", hubSchemaTable)
	if err := db.QueryRowContext(ctx, query).Scan(&version); err != nil {
		return 0, fmt.Errorf("read hub schema version: %w", err)
	}
	return version, nil
}
