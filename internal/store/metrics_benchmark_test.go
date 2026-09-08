package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func benchmarkWorkflowRows(issues int) []workflowMetricRow {
	rows := make([]workflowMetricRow, 0, issues*8)
	for i := range issues {
		project := fmt.Sprintf("project-%d", i%4)
		issue := fmt.Sprintf("issue-%d", i)
		offset := -time.Duration(i%55) * 24 * time.Hour
		for j := range 2 {
			lane := workflowMetricTestEvent(project, issue, WorkflowPhaseTypeLane, "In Progress", offset+time.Duration(j)*time.Hour, time.Hour)
			lane.ID = int64(len(rows) + 1)
			rows = append(rows, workflowMetricRow{event: lane})
			for k := range 3 {
				active := workflowMetricTestEvent(project, issue, WorkflowPhaseTypeAgentSession, "implement", offset+time.Duration(j)*time.Hour+time.Duration(k)*10*time.Minute, 20*time.Minute)
				active.ID = int64(len(rows) + 1)
				rows = append(rows, workflowMetricRow{event: active})
			}
		}
	}
	return rows
}

func BenchmarkWorkflowHistory(b *testing.B) {
	for _, issues := range []int{500, 2000} {
		b.Run(fmt.Sprintf("issues_%d", issues), func(b *testing.B) {
			rows := benchmarkWorkflowRows(issues)
			b.ReportAllocs()
			for b.Loop() {
				workflowMetricsReport(rows, rows, workflowMetricTestBase.Add(-60*24*time.Hour), workflowMetricTestBase.Add(24*time.Hour))
			}
		})
	}
}

func BenchmarkSQLiteWorkflowHistory(b *testing.B) {
	backend, err := openSQLite(b.Context(), Config{Path: filepath.Join(b.TempDir(), "history.db")})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Error(err)
		}
	})
	for _, row := range benchmarkWorkflowRows(2000) {
		if _, err := backend.RecordWorkflowPhaseEvent(b.Context(), row.event); err != nil {
			b.Fatal(err)
		}
	}
	query := WorkflowMetricsQuery{From: workflowMetricTestBase.Add(-60 * 24 * time.Hour), To: workflowMetricTestBase.Add(24 * time.Hour)}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := backend.WorkflowMetricsReport(b.Context(), query); err != nil {
			b.Fatal(err)
		}
	}
}
