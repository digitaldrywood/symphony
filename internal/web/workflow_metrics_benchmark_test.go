package web

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func BenchmarkWorkflowSnapshotHeartbeat(b *testing.B) {
	backend, err := store.Open(b.Context(), store.Config{Backend: store.BackendSQLite, Path: filepath.Join(b.TempDir(), "history.db")})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Error(err)
		}
	})
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for i := range 2000 {
		for j := range 4 {
			phaseType, phaseName := store.WorkflowPhaseTypeAgentSession, "implement"
			if j == 0 {
				phaseType, phaseName = store.WorkflowPhaseTypeLane, "In Progress"
			}
			started := now.Add(-time.Duration(i%55)*24*time.Hour - time.Hour)
			_, err := backend.RecordWorkflowPhaseEvent(b.Context(), store.WorkflowPhaseEvent{
				ProjectID: fmt.Sprintf("project-%d", i%4), IssueID: fmt.Sprintf("issue-%d", i), PhaseType: phaseType, PhaseName: phaseName,
				StartedAt: started, FinishedAt: started.Add(time.Hour),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	clock := now
	server := &Server{store: backend, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: func() time.Time { return clock }}
	snapshot := telemetry.Snapshot{GeneratedAt: now}
	b.ReportAllocs()
	for b.Loop() {
		snapshot.Seq++
		clock = clock.Add(time.Second)
		snapshot.GeneratedAt = now.Add(time.Duration(snapshot.Seq%30) * time.Second)
		server.snapshotWorkflowMetrics(b.Context(), snapshot)
	}
}
