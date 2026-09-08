package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionProgressPersists(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "progress.db")
	db := openParkTestStore(t, path)
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	id, err := db.StartSession(t.Context(), SessionStart{StartedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SessionProgress(t.Context(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing observation error = %v", err)
	}
	for _, head := range []string{"first", "second"} {
		want := SessionProgress{HeadSHA: head, CommitFingerprint: "patch", Local: true, LastProgressAt: at}
		if err := db.SaveSessionProgress(t.Context(), id, want); err != nil {
			t.Fatal(err)
		}
		got, err := db.SessionProgress(t.Context(), id)
		if err != nil || got != want {
			t.Fatalf("progress = %+v, %v, want %+v", got, err, want)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openParkTestStore(t, path)
	got, err := db.SessionProgress(t.Context(), id)
	if err != nil || got.HeadSHA != "second" || !got.LastProgressAt.Equal(at) {
		t.Fatalf("restarted progress = %+v, %v", got, err)
	}
}

func TestSessionProgressFailures(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		check func(*testing.T, *sqliteStore, int64)
	}{
		{name: "invalid session", check: func(t *testing.T, db *sqliteStore, _ int64) {
			if err := db.SaveSessionProgress(t.Context(), 0, SessionProgress{LastProgressAt: at}); !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
		}},
		{name: "missing timestamp", check: func(t *testing.T, db *sqliteStore, id int64) {
			if err := db.SaveSessionProgress(t.Context(), id, SessionProgress{}); err == nil {
				t.Fatal("missing timestamp accepted")
			}
		}},
		{name: "invalid timestamp", check: func(t *testing.T, db *sqliteStore, id int64) {
			if err := db.SaveSessionProgress(t.Context(), id, SessionProgress{LastProgressAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}); err == nil {
				t.Fatal("invalid timestamp accepted")
			}
		}},
		{name: "missing parent session", check: func(t *testing.T, db *sqliteStore, id int64) {
			if err := db.SaveSessionProgress(t.Context(), id+100, SessionProgress{LastProgressAt: at}); err == nil {
				t.Fatal("missing session accepted")
			}
		}},
		{name: "closed database", check: func(t *testing.T, db *sqliteStore, id int64) {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := db.SessionProgress(t.Context(), id); err == nil {
				t.Fatal("read closed database")
			}
			if err := db.SaveSessionProgress(t.Context(), id, SessionProgress{LastProgressAt: at}); err == nil {
				t.Fatal("wrote closed database")
			}
		}},
		{name: "corrupt observation", check: func(t *testing.T, db *sqliteStore, id int64) {
			for _, data := range []string{"invalid", "{}"} {
				if _, err := db.db.ExecContext(t.Context(), "INSERT OR REPLACE INTO session_progress VALUES (?, ?)", id, data); err != nil {
					t.Fatal(err)
				}
				if _, err := db.SessionProgress(t.Context(), id); err == nil {
					t.Fatal("corrupt observation accepted")
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := openParkTestStore(t, filepath.Join(t.TempDir(), "progress.db"))
			id, err := db.StartSession(t.Context(), SessionStart{StartedAt: at})
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, db, id)
		})
	}
}
