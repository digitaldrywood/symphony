package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/instancelock"
)

type RecoveryResult struct {
	DatabasePath    string `json:"database_path"`
	SchemaVersion   int64  `json:"schema_version"`
	AdministratorID string `json:"administrator_id,omitempty"`
}

func BackupDatabase(ctx context.Context, source, destination string) (resultErr error) {
	db, err := openRecoverySource(ctx, source)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	return db.backup(ctx, destination)
}

func VerifyDatabase(ctx context.Context, source string) (result RecoveryResult, resultErr error) {
	db, err := openRecoverySource(ctx, source)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	return RecoveryResult{DatabasePath: db.path, SchemaVersion: db.schemaVersion}, nil
}

func openRecoverySource(ctx context.Context, source string) (*database, error) {
	cfg := (Config{DatabasePath: source}).normalized()
	path, err := canonicalDatabasePath(source)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect hub recovery source: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("hub recovery source must be an existing regular database file")
	}
	if err := cfg.validateDatabaseFilesystem(filepath.Dir(path)); err != nil {
		return nil, err
	}
	lock, err := instancelock.Acquire(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("stop the owning Hub before offline maintenance: %w", err)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, cfg.BusyTimeout)+"&mode=rw")
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	owner := &database{db: db, lock: lock, path: path}
	if err := owner.configure(ctx, cfg.BusyTimeout); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	if err := verifyRecoveryDatabase(ctx, db); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	version, err := currentSchemaVersion(ctx, db)
	if err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	if version < 1 || version > supportedSchemaVersion {
		return nil, errors.Join(fmt.Errorf("%w: database=%d supported=%d", ErrUnsupportedSchema, version, supportedSchemaVersion), owner.Close())
	}
	owner.schemaVersion = version
	return owner, nil
}

func verifyRecoveryDatabase(ctx context.Context, db *sql.DB) error {
	var identity int64
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&identity); err != nil {
		return err
	}
	if identity != hubApplicationID {
		return ErrDatabaseIdentity
	}
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		return errors.New("hub database integrity check failed")
	}
	var violations int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_foreign_key_check").Scan(&violations); err != nil {
		return err
	}
	if violations != 0 {
		return errors.New("hub database foreign key check failed")
	}
	return nil
}

func RestoreDatabase(ctx context.Context, source, destination string, adminToken []byte) (result RecoveryResult, resultErr error) {
	value := strings.TrimSpace(string(adminToken))
	if len(value) < 32 || len(value) > maxBearerTokenBytes || strings.ContainsAny(value, " \t\r\n") {
		return result, errors.New("restore requires a fresh administrator token of 32 to 4096 non-whitespace bytes")
	}
	path, err := canonicalDatabasePath(destination)
	if err != nil {
		return result, err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return result, errors.New("restore destination must not exist")
	}
	if err := validateLocalDatabaseFilesystem(filepath.Dir(path)); err != nil {
		return result, err
	}
	lock, err := instancelock.Acquire(path + ".lock")
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	staging, err := os.MkdirTemp(filepath.Dir(path), ".hub-restore-")
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(staging)) }()
	stagedPath := filepath.Join(staging, "hub.db")
	if err := BackupDatabase(ctx, source, stagedPath); err != nil {
		return result, err
	}
	db, err := openDatabase(ctx, (Config{DatabasePath: stagedPath}).normalized())
	if err != nil {
		return result, err
	}
	administratorID, prepareErr := db.prepareRestore(ctx, value)
	if err := errors.Join(prepareErr, db.Close()); err != nil {
		return result, err
	}
	if err := os.Link(stagedPath, path); err != nil {
		return result, fmt.Errorf("publish restored hub without replacing existing data: %w", err)
	}
	return RecoveryResult{DatabasePath: path, SchemaVersion: db.schemaVersion, AdministratorID: administratorID}, nil
}

func (d *database) prepareRestore(ctx context.Context, adminToken string) (string, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	now, err := d.currentTime()
	if err != nil {
		return "", err
	}
	timestamp := formatHubTime(now)
	for _, statement := range []string{
		"UPDATE api_tokens SET revoked_at = coalesce(revoked_at, ?), updated_at = ?",
		"UPDATE runner_enrollments SET revoked_at = coalesce(revoked_at, ?)",
		"UPDATE leases SET released_at = ?, updated_at = ? WHERE released_at IS NULL",
		"UPDATE hub_identity SET cursor_key = randomblob(32)",
	} {
		args := make([]any, strings.Count(statement, "?"))
		for i := range args {
			args[i] = timestamp
		}
		if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
			return "", err
		}
	}
	id := "restore-admin-" + uuid.NewString()
	var bootstrapExists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM api_tokens WHERE id = ?)", bootstrapTokenID).Scan(&bootstrapExists); err != nil {
		return "", err
	}
	if !bootstrapExists {
		id = bootstrapTokenID
	}
	hash := apikey.HashToken(adminToken)
	var tokenExists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM api_tokens WHERE token_hash = ?)", hash).Scan(&tokenExists); err != nil {
		return "", err
	}
	if tokenExists {
		return "", errors.New("recovery administrator token must not match any source credential")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO api_tokens
		(id, name, token_hash, token_fingerprint, scope, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'admin', ?, ?)`, id, id, hash, tokenFingerprint(hash), timestamp, timestamp); err != nil {
		return "", fmt.Errorf("create recovery administrator: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, verifyRecoveryDatabase(ctx, d.db)
}
