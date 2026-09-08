package store

import (
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestWorkflowIndexedCorrelationEquivalence(t *testing.T) {
	t.Parallel()
	for _, seed := range []uint64{1, 7, 42, 2326} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			random := rand.New(rand.NewPCG(seed, seed+1))
			values := []string{"", " ", "a", " a ", "b", "c"}
			rows := make([]workflowMetricRow, 0, 400)
			for i := range 400 {
				phase := []WorkflowPhaseType{WorkflowPhaseTypeLane, WorkflowPhaseTypeAgentSession, WorkflowPhaseTypeCI, WorkflowPhaseTypeLocalCheck, WorkflowPhaseTypeReview}[random.IntN(5)]
				event := WorkflowPhaseEvent{
					ID: int64(i % 11), ProjectID: values[random.IntN(len(values))],
					IssueID: values[random.IntN(len(values))], Identifier: values[random.IntN(len(values))], IssueURL: values[random.IntN(len(values))],
					PhaseType: phase, PhaseName: []string{"In Progress", "Merging", "Rework"}[random.IntN(3)],
					StartedAt:       workflowMetricTestBase.Add(time.Duration(random.IntN(10)) * time.Minute),
					FinishedAt:      workflowMetricTestBase.Add(time.Duration(random.IntN(15)) * time.Minute),
					DurationSeconds: int64(random.IntN(1000) - 1), RunID: int64(random.IntN(15)), SessionID: int64(random.IntN(15)),
				}
				if random.IntN(2) == 0 {
					event.PRNumber = new(int64(random.IntN(3)))
				}
				rows = append(rows, workflowMetricRow{event: event})
			}
			if got, want := workflowLaneFlowsIndexed(rows, newWorkflowEventIndex(rows)), referenceWorkflowLaneFlows(rows); !reflect.DeepEqual(got, want) {
				t.Fatalf("flows differ: got %#v want %#v", got, want)
			}
			for _, selected := range [][]workflowMetricRow{rows, rows[:100], nil} {
				if got, want := workflowLaneRepresentativeRunsIndexed(selected, newWorkflowEventIndex(rows)), referenceWorkflowLaneRepresentativeRuns(selected, rows); !reflect.DeepEqual(got, want) {
					t.Fatalf("representatives differ: got %#v want %#v", got, want)
				}
			}
		})
	}
}

func TestWorkflowHistoryRevision(t *testing.T) {
	t.Parallel()
	backend, err := openSQLite(t.Context(), Config{Path: filepath.Join(t.TempDir(), "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	event := workflowMetricTestEvent("project", "issue", WorkflowPhaseTypeLane, "In Progress", 0, time.Hour)
	var id int64
	tests := []struct {
		name   string
		mutate func() error
		want   int64
	}{
		{"initial", func() error { return nil }, 0},
		{"insert", func() error {
			var err error
			id, err = backend.RecordWorkflowPhaseEvent(t.Context(), event)
			return err
		}, 1},
		{"metadata correction", func() error { return backend.UpdateWorkflowPhaseEventMetadata(t.Context(), id, `{"corrected":true}`) }, 2},
		{"same count and timestamps correction", func() error {
			_, err := backend.db.ExecContext(t.Context(), "UPDATE workflow_phase_events SET duration_seconds = 42 WHERE id = ?", id)
			return err
		}, 3},
		{"rollback", func() error {
			tx, err := backend.db.BeginTx(t.Context(), nil)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(t.Context(), "DELETE FROM workflow_phase_events"); err != nil {
				t.Error(err)
			}
			return tx.Rollback()
		}, 3},
		{"delete", func() error {
			_, err := backend.db.ExecContext(t.Context(), "DELETE FROM workflow_phase_events WHERE id = ?", id)
			return err
		}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.mutate(); err != nil {
				t.Fatal(err)
			}
			got, err := backend.WorkflowHistoryRevision(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("revision = %d, want %d", got, tt.want)
			}
		})
	}
}
