package boardsnapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestFileStoreLoad(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		savedAt   time.Time
		write     bool
		wantFound bool
	}{
		{name: "fresh snapshot", savedAt: now.Add(-time.Minute), write: true, wantFound: true},
		{name: "snapshot at staleness cap", savedAt: now.Add(-15 * time.Minute), write: true, wantFound: true},
		{name: "expired snapshot", savedAt: now.Add(-15*time.Minute - time.Nanosecond), write: true},
		{name: "missing snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "board-snapshot.json")
			store, err := New(Config{Path: path, MaxAge: 15 * time.Minute, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if tt.write {
				writer, err := New(Config{Path: path, MaxAge: 15 * time.Minute, Now: func() time.Time { return tt.savedAt }})
				if err != nil {
					t.Fatalf("New(writer) error = %v", err)
				}
				if err := writer.Save(context.Background(), telemetry.Snapshot{
					GeneratedAt: tt.savedAt.Add(-time.Second),
					Counts:      telemetry.Counts{Running: 2},
					Shutdown:    telemetry.Shutdown{Status: "draining", Draining: true},
				}); err != nil {
					t.Fatalf("Save() error = %v", err)
				}
			}

			got, found, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("Load() found = %v, want %v", found, tt.wantFound)
			}
			if found && (!got.LastKnown || got.Counts.Running != 0 || !got.LastKnownUntil.Equal(tt.savedAt.Add(15*time.Minute))) {
				t.Fatalf("Load() snapshot = %#v, want startup-safe cached tracker snapshot", got)
			}
			if found && (got.Tracker.Source != telemetry.SnapshotSourceCached || got.Runtime.Source != telemetry.SnapshotSourceUnknown) {
				t.Fatalf("Load() provenance = tracker %#v, runtime %#v", got.Tracker, got.Runtime)
			}
			if found && got.Refresh.ReadinessStatus() != telemetry.RefreshStatusInitializing {
				t.Fatalf("Load() refresh = %#v, want initializing", got.Refresh)
			}
			if found && got.Shutdown != (telemetry.Shutdown{Status: "running"}) {
				t.Fatalf("Load() shutdown = %#v, want the new process running", got.Shutdown)
			}
		})
	}
}

func TestFileStoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "board-snapshot.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := New(Config{Path: path, MaxAge: time.Minute})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, found, err := store.Load(context.Background()); err == nil || found {
		t.Fatalf("Load() = found %v, error %v; want decode error", found, err)
	}
}

func TestFileStoreHonorsContext(t *testing.T) {
	t.Parallel()

	store, err := New(Config{Path: filepath.Join(t.TempDir(), "board-snapshot.json"), MaxAge: time.Minute})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want context canceled", err)
	}
	if err := store.Save(ctx, telemetry.Snapshot{GeneratedAt: time.Now()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save() error = %v, want context canceled", err)
	}
}

func TestEligible(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		snapshot telemetry.Snapshot
		want     bool
	}{
		{name: "ready", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusReady}}, want: true},
		{name: "loop behind", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusBehind}}, want: true},
		{name: "partial fleet", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusPartial}}, want: true},
		{name: "live snapshot without refresh signal", snapshot: telemetry.Snapshot{GeneratedAt: now, Projects: []telemetry.ProjectSnapshot{{Project: telemetry.Project{ID: "paused"}}}}, want: true},
		{name: "degraded with prior data", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusDegraded}, BoardIssues: []telemetry.Issue{{ID: "issue"}}}, want: true},
		{name: "composite with cached tracker", snapshot: telemetry.Snapshot{GeneratedAt: now, Tracker: telemetry.SnapshotSection{Source: telemetry.SnapshotSourceMixed}, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusDegraded}, Projects: []telemetry.ProjectSnapshot{{Project: telemetry.Project{ID: "alpha"}, Tracker: telemetry.SnapshotSection{Source: telemetry.SnapshotSourceCached}}, {Project: telemetry.Project{ID: "bravo"}, Tracker: telemetry.SnapshotSection{Source: telemetry.SnapshotSourceLive}}}, BoardIssues: []telemetry.Issue{{ID: "issue"}}}},
		{name: "initializing", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusInitializing}}},
		{name: "degraded without data", snapshot: telemetry.Snapshot{GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusDegraded}}},
		{name: "last known", snapshot: telemetry.Snapshot{LastKnown: true, GeneratedAt: now, Refresh: telemetry.Refresh{Status: telemetry.RefreshStatusReady}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Eligible(tt.snapshot); got != tt.want {
				t.Fatalf("Eligible() = %v, want %v", got, tt.want)
			}
		})
	}
}
