package runner

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestSessionProgressResume(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "progress.db")
	db, err := store.Open(t.Context(), store.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sessionID, err := db.StartSession(t.Context(), store.SessionStart{StartedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sessionProgressSnapshot{LocalProgress: &workspace.LocalProgress{HeadSHA: "first", CommitFingerprint: "first-patch"}}
	runner := &Runner{store: db}
	controller := &sessionBrakeController{lastProgressAt: at, journal: runner.sessionProgressJournal(sessionID, 0)}
	if err := controller.observeSnapshotLocked(t.Context(), snapshot, at); err != nil {
		t.Fatal(err)
	}
	snapshot.LocalProgress = &workspace.LocalProgress{HeadSHA: "second", CommitFingerprint: "second-patch"}
	if err := controller.observeSnapshotLocked(t.Context(), snapshot, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(t.Context(), store.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	runner.store = db
	for _, name := range []string{"restart same session", "resume new session", "resume again"} {
		t.Run(name, func(t *testing.T) {
			resumeID := int64(0)
			if name != "restart same session" {
				resumeID = sessionID
				sessionID, err = db.StartSession(t.Context(), store.SessionStart{StartedAt: at.Add(2 * time.Minute), ResumedFromSessionID: resumeID})
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			controller = &sessionBrakeController{
				startedAt: at.Add(2 * time.Minute), lastProgressAt: at.Add(2 * time.Minute), noProgressTimeout: time.Minute,
				journal: runner.sessionProgressJournal(sessionID, resumeID), cancelSession: cancel,
				probe: func(context.Context) (sessionProgressSnapshot, error) { return snapshot, nil },
			}
			controller.checkProgress(ctx, at.Add(2*time.Minute))
			if controller.breach == nil || !controller.lastProgressAt.Equal(at.Add(time.Minute)) {
				t.Fatalf("restart recredited same head: last=%v breach=%v", controller.lastProgressAt, controller.breach)
			}
			got, err := db.(store.SessionProgressStore).SessionProgress(t.Context(), sessionID)
			if err != nil || got.HeadSHA != "second" || !got.LastProgressAt.Equal(at.Add(time.Minute)) {
				t.Fatalf("durable progress = %+v, %v", got, err)
			}
		})
	}
}

func TestSessionProgressObservationFailures(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	initial := sessionProgressSnapshot{LocalProgress: &workspace.LocalProgress{HeadSHA: "first", CommitFingerprint: "first-patch"}}
	changed := sessionProgressSnapshot{LocalProgress: &workspace.LocalProgress{HeadSHA: "second", CommitFingerprint: "second-patch"}}
	tests := []struct {
		name                       string
		readErr, saveErr, probeErr error
		missing                    bool
	}{
		{name: "journal read failure", readErr: errors.New("read failed")},
		{name: "journal write failure", saveErr: errors.New("write failed")},
		{name: "probe read failure", probeErr: errors.New("git read failed")},
		{name: "first successful probe establishes baseline", missing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			memory := &progressJournalStore{readErr: tt.readErr, saveErr: tt.saveErr}
			if !tt.missing {
				observation := sessionProgressObservation(initial, at)
				memory.progress = &observation
			}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			controller := &sessionBrakeController{
				startedAt: at, lastProgressAt: at, noProgressTimeout: time.Minute, cancelSession: cancel,
				journal: &sessionProgressJournal{store: memory, sessionID: 1},
				probe:   func(context.Context) (sessionProgressSnapshot, error) { return changed, tt.probeErr },
			}
			controller.checkProgress(ctx, at.Add(time.Minute))
			if controller.breach == nil || !controller.lastProgressAt.Equal(at) {
				t.Fatalf("failed read/write extended clock: %+v", controller)
			}
		})
	}
}

func TestSessionProgressAdvancement(t *testing.T) {
	t.Parallel()
	previous := store.SessionProgress{Local: true, HeadSHA: "first", CommitFingerprint: "patch", TrackedFingerprint: "dirty"}
	tests := []struct {
		name    string
		current store.SessionProgress
		want    bool
	}{
		{name: "same", current: previous},
		{name: "head only", current: store.SessionProgress{Local: true, HeadSHA: "second", CommitFingerprint: "patch", TrackedFingerprint: "dirty"}},
		{name: "committed", current: store.SessionProgress{Local: true, CommitFingerprint: "new"}, want: true},
		{name: "tracked edit", current: store.SessionProgress{Local: true, TrackedFingerprint: "new"}, want: true},
		{name: "cleared evidence", current: store.SessionProgress{Local: true}},
		{name: "new workpad", current: store.SessionProgress{Local: true, WorkpadFingerprint: "new"}, want: true},
		{name: "backend changed", current: store.SessionProgress{WorkspaceFingerprint: "new"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionProgressAdvanced(previous, tt.current); got != tt.want {
				t.Fatalf("advance=%t, want %t", got, tt.want)
			}
		})
	}
}

type progressJournalStore struct {
	progress *store.SessionProgress
	readErr  error
	saveErr  error
}

func (s *progressJournalStore) SessionProgress(context.Context, int64) (store.SessionProgress, error) {
	if s.readErr != nil {
		return store.SessionProgress{}, s.readErr
	}
	if s.progress == nil {
		return store.SessionProgress{}, store.ErrNotFound
	}
	return *s.progress, nil
}

func (s *progressJournalStore) SaveSessionProgress(_ context.Context, _ int64, progress store.SessionProgress) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.progress = &progress
	return nil
}
