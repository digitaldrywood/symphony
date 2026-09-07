package templates

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestBoardViewBuildsOneSharedWorkItemPerCard(t *testing.T) {
	view := boardViewFromDashboard(boardTestData())
	want := 0
	for _, lane := range view.Lanes {
		want += len(lane.Cards)
		for _, card := range lane.Cards {
			if card.Work.Key == "" || card.Work.Identity != card.Identity {
				t.Fatalf("card %q has incomplete shared work metadata: %#v", card.Identity, card.Work)
			}
		}
	}
	if len(view.Items) != want {
		t.Fatalf("shared work items = %d, want %d board cards", len(view.Items), want)
	}
	for _, item := range view.Items {
		if item.Card.Work != item.Meta {
			t.Fatalf("list item %q does not share its board metadata", item.Meta.Identity)
		}
	}
}

func TestNativeWorkNavigationAndSource(t *testing.T) {
	for _, tt := range []struct{ kind, want string }{
		{"hub_native", "/projects/detent/issues/new"},
		{"github", "https://github.com/digitaldrywood/detent/issues/new"},
	} {
		t.Run(tt.kind, func(t *testing.T) {
			data := boardTestData()
			data.ProjectID = "detent"
			data.Snapshot.Project.URL = "https://github.com/digitaldrywood/detent"
			data.Kanban.TrackerKind = tt.kind
			if got := workNewIssueURL(data); got != tt.want {
				t.Fatalf("new issue URL = %q, want %q", got, tt.want)
			}
			if tt.kind == "hub_native" {
				label, key, detail, _ := workItemSync(data, projectKanbanCard{})
				if label != "Native" || key != "native" || strings.Contains(detail, "GitHub") {
					t.Fatalf("native source = %q %q %q", label, key, detail)
				}
			}
		})
	}
}

func TestWorkItemMetadataStates(t *testing.T) {
	now := time.Date(2026, 9, 3, 17, 0, 0, 0, time.UTC)
	leaseRenewedAt := now.Add(-90 * time.Second)
	leaseExpiresAt := now.Add(30 * time.Second)
	updatedAt := now.Add(-15 * time.Minute)
	tests := []struct {
		name          string
		data          DashboardData
		card          projectKanbanCard
		view          boardCardView
		wantReadiness string
		wantPriority  string
		wantLabel     string
		wantSync      string
		wantMachine   string
	}{
		{
			name: "running pending sync",
			data: DashboardData{Snapshot: telemetry.Snapshot{GeneratedAt: now}},
			card: projectKanbanCard{
				Stage: "In Progress", PriorityName: "urgent", PriorityRank: 1,
				Owner: "machine-a", LeaseRenewedAt: &leaseRenewedAt, LeaseExpiresAt: &leaseExpiresAt,
				UpdatedAt: &updatedAt, SyncStatus: "pending",
			},
			view:          boardCardView{Identity: "acme/widgets#1", Project: "widgets", Running: true},
			wantReadiness: "running", wantPriority: "urgent", wantLabel: "Urgent", wantSync: "pending", wantMachine: "machine-a",
		},
		{
			name: "blocked sync error",
			data: DashboardData{Snapshot: telemetry.Snapshot{GeneratedAt: now}},
			card: projectKanbanCard{
				Stage: "Blocked", PriorityName: "low", PriorityRank: 4,
				BlockedReason: "waiting for operator", SyncStatus: "error",
			},
			view:          boardCardView{Identity: "acme/widgets#2", Project: "widgets", ExtraChip: true, ExtraKind: "err"},
			wantReadiness: "blocked", wantPriority: "low", wantLabel: "Low", wantSync: "error", wantMachine: "Unclaimed",
		},
		{
			name:          "stale projection",
			data:          DashboardData{Snapshot: telemetry.Snapshot{GeneratedAt: now, LastKnown: true}},
			card:          projectKanbanCard{Stage: "Todo"},
			view:          boardCardView{Identity: "acme/widgets#3", Project: "widgets"},
			wantReadiness: "ready", wantPriority: "unset", wantLabel: "Unset", wantSync: "stale", wantMachine: "Unclaimed",
		},
		{
			name:          "tracker p0 priority",
			data:          DashboardData{Snapshot: telemetry.Snapshot{GeneratedAt: now}},
			card:          projectKanbanCard{Stage: "Todo", PriorityName: "P0", PriorityRank: 1},
			view:          boardCardView{Identity: "acme/widgets#4", Project: "widgets"},
			wantReadiness: "ready", wantPriority: "urgent", wantLabel: "P0", wantSync: "synced", wantMachine: "Unclaimed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := workItemMetadataFromCard(test.data, test.card, test.view)
			if metadata.ReadinessKey != test.wantReadiness || metadata.PriorityKey != test.wantPriority || metadata.Priority != test.wantLabel || metadata.SyncKey != test.wantSync || metadata.Machine != test.wantMachine {
				t.Fatalf("metadata = readiness %q priority %q label %q sync %q machine %q", metadata.ReadinessKey, metadata.PriorityKey, metadata.Priority, metadata.SyncKey, metadata.Machine)
			}
		})
	}
}

func TestBoardSnapshotRendersSharedWorkControlsAndRepresentations(t *testing.T) {
	html := renderBoardComponent(t, BoardSnapshot(boardTestData()))
	for _, want := range []string{
		`data-work-toolbar`,
		`data-work-view="board"`,
		`data-work-view="list"`,
		`data-work-search-input`,
		`data-work-filter="readiness"`,
		`data-work-health`,
		`href="/health/ui"`,
		`data-work-list-body`,
		`data-work-view-panel="board"`,
		`data-work-view-panel="list"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("shared Work surface missing %q", want)
		}
	}
	view := boardViewFromDashboard(boardTestData())
	if got := strings.Count(html, `data-work-representation="board"`); got != len(view.Items) {
		t.Fatalf("board representations = %d, want %d", got, len(view.Items))
	}
	if got := strings.Count(html, `data-work-representation="list"`); got != len(view.Items) {
		t.Fatalf("list representations = %d, want %d", got, len(view.Items))
	}
}

func TestWorkListSheetTriggerExcludesNestedControls(t *testing.T) {
	attrs := workListSheetOpenAttrs("detent", "digitaldrywood/detent#2076", "fleet")
	if got, _ := attrs["hx-trigger"].(string); got != "click[!event.target.closest('a,button,input,select,label')]" {
		t.Fatalf("list sheet trigger = %q, want nested controls excluded", got)
	}
}

func TestSharedWorkRepresentationsKeepIssueHealthContextual(t *testing.T) {
	data := boardTestData()
	now := data.Snapshot.GeneratedAt
	leaseRenewedAt := now.Add(-90 * time.Second)
	leaseExpiresAt := now.Add(30 * time.Second)
	updatedAt := now.Add(-15 * time.Minute)
	issue := &data.Snapshot.Running[0].Issue
	issue.Owner = "machine-a"
	issue.LeaseRenewedAt = &leaseRenewedAt
	issue.LeaseExpiresAt = &leaseExpiresAt
	issue.UpdatedAt = &updatedAt
	issue.Metadata = map[string]string{
		hubSyncStatusMetadataKey:     "error",
		hubSourceSyncedAtMetadataKey: now.Add(-5 * time.Minute).Format(time.RFC3339),
	}

	html := renderBoardComponent(t, BoardSnapshot(data))
	for _, want := range []string{
		`data-board-card-sync="error"`,
		`data-work-sync-status="error"`,
		`machine-a`,
		`renewed 1m 30s ago`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("shared Work representations missing contextual health %q", want)
		}
	}
	if strings.Contains(html, `id="board-alerts"`) {
		t.Fatal("issue sync errors must not render as a persistent page-wide alert")
	}

	card, ok := FindBoardCard(data, issue.ProjectID, issue.Identifier)
	if !ok {
		t.Fatalf("FindBoardCard() did not find %q", issue.Identifier)
	}
	sheet := renderBoardComponent(t, BoardCardSheetCore(data, card, true))
	for _, want := range []string{
		`data-detail-sync-status`,
		`GitHub sync`,
		`Error`,
		`Machine`,
		`machine-a`,
		`Lease`,
		`Updated`,
	} {
		if !strings.Contains(sheet, want) {
			t.Fatalf("detail sheet missing contextual health %q", want)
		}
	}
}

func TestWorkBoardRendersLargeFleetLanes(t *testing.T) {
	for _, backlogCount := range []int{50, 120} {
		t.Run(fmt.Sprintf("%d backlog cards", backlogCount), func(t *testing.T) {
			data := boardTestData()
			data.Snapshot.Blocked = nil
			data.Snapshot.BoardIssues = nil
			data.Snapshot.Running[0].State = "Rework"
			data.Kanban.States = []string{"Backlog", "Todo", "In Progress", "Rework"}
			for index := range backlogCount {
				data.Snapshot.BoardIssues = append(data.Snapshot.BoardIssues, telemetry.Issue{
					ID: fmt.Sprintf("backlog-%d", index), Identifier: fmt.Sprintf("digitaldrywood/detent#%d", index+1000),
					ProjectID: "detent", Title: "Backlog fixture", State: "Backlog",
				})
			}
			data.Snapshot.BoardIssues = append(data.Snapshot.BoardIssues, telemetry.Issue{
				ID: "queued", Identifier: "digitaldrywood/detent#3000", ProjectID: "detent", Title: "Queued fixture", State: "Todo",
			})
			view := boardViewFromDashboard(data)
			if len(view.Items) != backlogCount+2 {
				t.Fatalf("work items = %d, want %d", len(view.Items), backlogCount+2)
			}
			foundLanes := 0
			for _, lane := range view.Lanes {
				if lane.Title != "Todo" && lane.Title != "Rework" {
					continue
				}
				foundLanes++
				if len(lane.Cards) != 1 || !lane.DefaultVisible {
					t.Fatalf("%s lane must contain one card and be visible: %#v", lane.Title, lane)
				}
				wantReadiness := "ready"
				if lane.Title == "Rework" {
					wantReadiness = "running"
				}
				if got := lane.Cards[0].Work.ReadinessKey; got != wantReadiness {
					t.Fatalf("%s readiness = %q, want %q", lane.Title, got, wantReadiness)
				}
			}
			if foundLanes != 2 {
				t.Fatalf("queued and running lanes = %d, want 2", foundLanes)
			}
			html := renderBoardComponent(t, BoardSnapshot(data))
			for _, representation := range []string{"board", "list"} {
				if got := strings.Count(html, `data-work-representation="`+representation+`"`); got != backlogCount+2 {
					t.Fatalf("%s representations = %d, want %d", representation, got, backlogCount+2)
				}
			}
			if !strings.Contains(html, `data-work-list-only hidden`) {
				t.Fatal("list pagination must be hidden on the initial board render")
			}
		})
	}
}
