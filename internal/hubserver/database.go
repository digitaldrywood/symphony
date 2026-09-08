package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/digitaldrywood/detent/internal/instancelock"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const hubApplicationID = 0x44544842

type database struct {
	db                 *sql.DB
	lock               *instancelock.Lock
	path               string
	schemaVersion      int64
	hostedOrganization tracker.OrganizationID
	hostedPlans        *HostedPlansConfig
	now                func() time.Time
	newLeaseID         func() string
	closeOnce          sync.Once
	closeErr           error
}

func openDatabase(ctx context.Context, cfg Config) (*database, error) {
	path, err := canonicalDatabasePath(cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	if err := cfg.validateDatabaseFilesystem(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("validate hub database filesystem: %w", err)
	}

	lock, err := instancelock.Acquire(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("acquire hub database ownership: %w", err)
	}
	if err := createPrivateDatabaseFile(path); err != nil {
		return nil, errors.Join(err, lock.Close())
	}

	db, err := sql.Open("sqlite", sqliteDSN(path, cfg.BusyTimeout))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open hub database: %w", err), lock.Close())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &database{db: db, lock: lock, path: path, now: cfg.now, newLeaseID: cfg.newLeaseID}
	if err := store.configure(ctx, cfg.BusyTimeout); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if err := store.verifyIdentity(ctx); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if err := store.enableWAL(ctx); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	version, err := runMigrations(ctx, db, cfg.Logger)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	store.schemaVersion = version
	if err := store.bindHostedDatabase(ctx, cfg.Hosted); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if err := store.configureHostedPlans(ctx, cfg.Hosted); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	if err := store.health(ctx); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func canonicalDatabasePath(rawPath string) (string, error) {
	path := strings.TrimSpace(rawPath)
	if path == "" {
		return "", errors.New("hub database path is required")
	}
	if path == ":memory:" || strings.HasPrefix(strings.ToLower(path), "file:") {
		return "", errors.New("hub database must be a local filesystem path")
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve hub database path: %w", err)
	}
	parent := filepath.Dir(absolutePath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("create hub database directory: %w", err)
	}

	if _, err := os.Lstat(absolutePath); err == nil {
		resolvedPath, resolveErr := filepath.EvalSymlinks(absolutePath)
		if resolveErr != nil {
			return "", fmt.Errorf("resolve hub database symlink: %w", resolveErr)
		}
		return resolvedPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect hub database path: %w", err)
	}

	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("resolve hub database directory: %w", err)
	}
	return filepath.Join(resolvedParent, filepath.Base(absolutePath)), nil
}

func createPrivateDatabaseFile(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create private hub database: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close new hub database: %w", err)
	}
	return nil
}

func sqliteDSN(path string, busyTimeout time.Duration) string {
	databasePath := filepath.ToSlash(path)
	if isWindowsDrivePath(databasePath) {
		databasePath = "/" + databasePath
	}
	databaseURL := &url.URL{Scheme: "file", Path: databasePath}
	query := databaseURL.Query()
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMillis(busyTimeout)))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "locking_mode(EXCLUSIVE)")
	query.Add("_pragma", "synchronous(FULL)")
	databaseURL.RawQuery = query.Encode()
	return databaseURL.String()
}

func isWindowsDrivePath(path string) bool {
	if len(path) < 3 || path[1] != ':' || path[2] != '/' {
		return false
	}
	return path[0] >= 'A' && path[0] <= 'Z' || path[0] >= 'a' && path[0] <= 'z'
}

func (d *database) configure(ctx context.Context, busyTimeout time.Duration) error {
	wantBusyTimeout := busyTimeoutMillis(busyTimeout)
	var gotBusyTimeout int64
	if err := d.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&gotBusyTimeout); err != nil {
		return fmt.Errorf("read hub sqlite busy timeout: %w", err)
	}
	if gotBusyTimeout != wantBusyTimeout {
		return fmt.Errorf("hub sqlite busy timeout is %dms, want %dms", gotBusyTimeout, wantBusyTimeout)
	}
	var synchronous int
	if err := d.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("read hub sqlite synchronous mode: %w", err)
	}
	if synchronous != 2 {
		return fmt.Errorf("hub sqlite synchronous mode is %d, want FULL", synchronous)
	}

	var foreignKeys int
	if err := d.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read hub sqlite foreign keys: %w", err)
	}
	if foreignKeys != 1 {
		return errors.New("hub sqlite foreign keys are disabled")
	}

	var lockingMode string
	if err := d.db.QueryRowContext(ctx, "PRAGMA locking_mode = EXCLUSIVE").Scan(&lockingMode); err != nil {
		return fmt.Errorf("enable hub sqlite exclusive locking: %w", err)
	}
	if !strings.EqualFold(lockingMode, "exclusive") {
		return fmt.Errorf("hub sqlite locking mode is %q, want exclusive", lockingMode)
	}
	if _, err := d.db.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("acquire hub sqlite ownership: %w", err)
	}
	if _, err := d.db.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit hub sqlite ownership probe: %w", err)
	}

	return nil
}

func (d *database) enableWAL(ctx context.Context) error {
	var journalMode string
	if err := d.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable hub sqlite WAL: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("hub sqlite journal mode is %q, want wal", journalMode)
	}
	return nil
}

func (d *database) verifyIdentity(ctx context.Context) error {
	var applicationID int64
	if err := d.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return fmt.Errorf("read hub sqlite application id: %w", err)
	}
	if applicationID == hubApplicationID {
		return nil
	}
	if applicationID != 0 {
		return fmt.Errorf("%w: application id is %d", ErrDatabaseIdentity, applicationID)
	}

	var tableCount int
	if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect hub sqlite schema: %w", err)
	}
	if tableCount != 0 {
		return fmt.Errorf("%w: unrecognized tables already exist", ErrDatabaseIdentity)
	}

	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", hubApplicationID)); err != nil {
		return fmt.Errorf("set hub sqlite application id: %w", err)
	}
	if err := d.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return fmt.Errorf("verify hub sqlite application id: %w", err)
	}
	if applicationID != hubApplicationID {
		return fmt.Errorf("set hub sqlite application id: got %d", applicationID)
	}
	return nil
}

func (d *database) health(ctx context.Context) error {
	if err := d.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping hub database: %w", err)
	}
	version, err := currentSchemaVersion(ctx, d.db)
	if err != nil {
		return err
	}
	if version != d.schemaVersion {
		return fmt.Errorf("hub database schema version is %d, want %d", version, d.schemaVersion)
	}
	return nil
}

func (d *database) Close() error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		d.closeErr = errors.Join(d.db.Close(), d.lock.Close())
	})
	return d.closeErr
}

func busyTimeoutMillis(timeout time.Duration) int64 {
	if timeout <= 0 {
		return defaultBusyTimeout.Milliseconds()
	}
	milliseconds := timeout.Milliseconds()
	if milliseconds < 1 {
		return 1
	}
	return milliseconds
}
