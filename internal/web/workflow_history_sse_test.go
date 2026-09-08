package web_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web"
)

type workflowSSECountingStore struct {
	enrichmentQueryCountingStore
	revision atomic.Int64
}

func (s *workflowSSECountingStore) WorkflowHistoryRevision(context.Context) (int64, error) {
	return s.revision.Load(), nil
}

func TestWorkflowHistorySharedAcrossSSEClients(t *testing.T) {
	t.Parallel()
	backend := &workflowSSECountingStore{}
	deps := testDeps(t)
	deps.Store = backend
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	server, err := web.NewServer(web.Config{SSETickInterval: time.Hour, SSEFragmentInterval: -1, Now: func() time.Time { return now }}, deps)
	if err != nil {
		t.Fatal(err)
	}
	addr := startWebServer(t, server)
	type client struct {
		conn   net.Conn
		reader *bufio.Reader
	}
	clients := make([]client, 3)
	for i := range clients {
		clients[i].conn, clients[i].reader = openRawEventStream(t, addr)
	}
	for _, tt := range []struct {
		name, title string
		seq         uint64
		elapsed     time.Duration
		correct     bool
		want        int64
	}{
		{name: "initial", title: "Initial operational row", seq: 1, want: 6},
		{name: "heartbeat", title: "Updated operational row", seq: 2, elapsed: time.Second, want: 6},
		{name: "correction", title: "Corrected history row", seq: 3, elapsed: 2 * time.Second, correct: true, want: 12},
		{name: "moving window", title: "Window refreshed row", seq: 4, elapsed: 32 * time.Second, want: 18},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.correct {
				backend.revision.Add(1)
			}
			snapshot := telemetry.Snapshot{Seq: tt.seq, GeneratedAt: now.Add(tt.elapsed), Running: []telemetry.Running{{Issue: telemetry.Issue{ID: "live", Identifier: "LIVE-1", Title: tt.title, State: "In Progress"}}}}
			if err := deps.Hub.Publish(snapshot); err != nil {
				t.Fatal(err)
			}
			for _, client := range clients {
				event := readRawSSEEventNamed(t, client.conn, client.reader, "snapshot")
				if !strings.Contains(event.data, tt.title) {
					t.Fatalf("operational row missing %q", tt.title)
				}
			}
			if got := backend.workflowMetricsCalls.Load(); got != tt.want {
				t.Fatalf("history queries = %d, want %d", got, tt.want)
			}
		})
	}
}
