package artifact

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pressly/goose/v3"
	"modernc.org/sqlite"

	"github.com/digitaldrywood/detent/internal/instancelock"
)

//go:embed migrations/*.sql
var catalogMigrations embed.FS

type catalog struct {
	db        *sql.DB
	lock      *instancelock.Lock
	path      string
	closeOnce sync.Once
	closeErr  error
}

func catalogPath(raw string) (string, error) {
	if raw == "" || raw == ":memory:" || strings.HasPrefix(strings.ToLower(raw), "file:") {
		return "", ErrInvalid
	}
	absolute, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", err
	}
	if _, err := os.Lstat(absolute); err == nil {
		return filepath.EvalSymlinks(absolute)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	return filepath.Join(parent, filepath.Base(absolute)), err
}

func openCatalog(ctx context.Context, raw string) (*catalog, error) {
	path, err := catalogPath(raw)
	if err != nil {
		return nil, err
	}
	if err := validateLocalDatabaseFilesystem(filepath.Dir(path)); err != nil {
		return nil, err
	}
	lock, err := instancelock.Acquire(path + ".lock")
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.Join(err, lock.Close())
	}
	if file != nil {
		if err := file.Close(); err != nil {
			return nil, errors.Join(err, lock.Close())
		}
	}
	db, err := sql.Open("sqlite", catalogDSN(path))
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	c := &catalog{db: db, lock: lock, path: path}
	if err := c.initialize(ctx); err != nil {
		return nil, errors.Join(err, c.Close())
	}
	return c, nil
}

func catalogDSN(path string) string {
	databasePath := filepath.ToSlash(path)
	if len(databasePath) >= 3 && databasePath[1] == ':' && databasePath[2] == '/' && (databasePath[0] >= 'A' && databasePath[0] <= 'Z' || databasePath[0] >= 'a' && databasePath[0] <= 'z') {
		databasePath = "/" + databasePath
	}
	u := url.URL{Scheme: "file", Path: databasePath}
	q := u.Query()
	for _, pragma := range []string{"busy_timeout(5000)", "foreign_keys(1)", "locking_mode(EXCLUSIVE)", "synchronous(FULL)"} {
		q.Add("_pragma", pragma)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *catalog) initialize(ctx context.Context) error {
	for _, check := range []struct {
		query string
		want  int
	}{{"PRAGMA busy_timeout", 5000}, {"PRAGMA foreign_keys", 1}, {"PRAGMA synchronous", 2}} {
		var got int
		if err := c.db.QueryRowContext(ctx, check.query).Scan(&got); err != nil {
			return err
		}
		if got != check.want {
			return errors.New("artifact SQLite configuration mismatch")
		}
	}
	var mode string
	if err := c.db.QueryRowContext(ctx, "PRAGMA locking_mode").Scan(&mode); err != nil {
		return err
	}
	if !strings.EqualFold(mode, "exclusive") {
		return errors.New("artifact catalog requires exclusive ownership")
	}
	if _, err := c.db.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	var identity int
	if err := c.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&identity); err != nil {
		return err
	}
	const applicationID = 0x44544152
	if identity != applicationID {
		var count int
		if err := c.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&count); err != nil {
			return err
		}
		if identity != 0 || count != 0 {
			return errors.New("database is not a Detent artifact catalog")
		}
		if _, err := c.db.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
			return err
		}
	}
	if err := c.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return err
	}
	if !strings.EqualFold(mode, "wal") {
		return errors.New("artifact catalog requires WAL")
	}
	migrations, err := fs.Sub(catalogMigrations, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, c.db, migrations, goose.WithDisableGlobalRegistry(true), goose.WithTableName("artifact_schema_version"))
	if err != nil {
		return err
	}
	current, target, err := provider.GetVersions(ctx)
	if err != nil {
		return err
	}
	if target != 1 || current > target {
		return errors.New("unsupported artifact catalog schema")
	}
	if _, err := provider.Up(ctx); err != nil {
		return err
	}
	current, _, err = provider.GetVersions(ctx)
	if err != nil {
		return err
	}
	if current != target {
		return errors.New("artifact catalog migration incomplete")
	}
	return nil
}

func (c *catalog) Close() error {
	c.closeOnce.Do(func() { c.closeErr = errors.Join(c.db.Close(), c.lock.Close()) })
	return c.closeErr
}

func (c *catalog) backup(ctx context.Context, destination string) (resultErr error) {
	path, err := catalogPath(destination)
	if err != nil {
		return err
	}
	if path == c.path {
		return ErrConflict
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	complete := false
	defer func() {
		if !complete {
			resultErr = errors.Join(resultErr, os.Remove(path))
		}
	}()
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	err = conn.Raw(func(driver any) error {
		backuper, ok := driver.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		})
		if !ok {
			return ErrUnsupported
		}
		backup, err := backuper.NewBackup(path)
		if err != nil {
			return err
		}
		var stepErr error
		for more := true; more && stepErr == nil; {
			if stepErr = ctx.Err(); stepErr != nil {
				break
			}
			more, stepErr = backup.Step(256)
		}
		return errors.Join(stepErr, backup.Finish())
	})
	if err = errors.Join(err, conn.Close()); err != nil {
		return err
	}
	complete = true
	return nil
}
