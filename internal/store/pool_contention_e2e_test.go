package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestPoolContentionTelemetryEndToEnd(t *testing.T) {
	t.Parallel()

	const synchronizationWatchdog = 2 * time.Minute
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "detent.db")
	runtimeStore, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    dbPath,
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtimeStore.Close(); err != nil {
			t.Fatalf("runtime store Close() error = %v", err)
		}
	})

	localProject := scheduler.ProjectCandidate{ID: "local", Weight: 1, Priority: 0}
	cloudProject := scheduler.ProjectCandidate{ID: "cloud", Weight: 1, Priority: 1}
	dispatchGate := scheduler.NewGlobalDispatchGate(
		scheduler.NewStrictPriority(scheduler.Config{Capacity: 1}),
		localProject,
		cloudProject,
	)
	localSlot, acquired, decision, err := dispatchGate.TryAcquireWithDecision(
		ctx,
		localProject,
		scheduler.SlotRequest{State: "Todo"},
		time.Now(),
	)
	if err != nil {
		t.Fatalf("local TryAcquireWithDecision() error = %v", err)
	}
	if !acquired {
		t.Fatalf("local TryAcquireWithDecision() acquired = false; decision = %#v", decision)
	}
	t.Cleanup(func() {
		if err := dispatchGate.Release(localSlot); err != nil && !errors.Is(err, scheduler.ErrSlotNotHeld) {
			t.Fatalf("dispatch gate Release() error = %v", err)
		}
	})

	issue := connector.NewIssue()
	issue.ID = "issue-cloud"
	issue.Identifier = "digitaldrywood/video#42"
	issue.Title = "render cloud video"
	issue.State = "Todo"
	issue.URL = "https://github.com/digitaldrywood/video/issues/42"

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo"},
		TerminalStates:      []string{"Done"},
		Project:             cloudProject,
	}, orchestrator.Dependencies{
		Connector:          memory.New(memory.Config{Issues: []connector.Issue{issue}}),
		WorkAttempts:       runtimeStore,
		GlobalDispatchGate: dispatchGate,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("orchestrator.New() error = %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() {
		errCh <- orch.Run(runCtx)
	}()

	stateCtx, cancelState := context.WithTimeout(ctx, synchronizationWatchdog)
	defer cancelState()
	stateTicker := time.NewTicker(time.Millisecond)
	defer stateTicker.Stop()
	for {
		state, err := orch.State(stateCtx)
		if err != nil {
			t.Fatalf("orchestrator State() error = %v", err)
		}
		if state.DataSeq > 0 {
			break
		}
		select {
		case <-stateCtx.Done():
			t.Fatal("initial scheduler decision did not complete")
		case <-stateTicker.C:
		}
	}
	decisions, err := runtimeStore.ListRecentSchedulerDecisions(
		ctx,
		store.SchedulerDecisionQuery{Limit: 100},
	)
	if err != nil {
		t.Fatalf("ListRecentSchedulerDecisions() error = %v", err)
	}
	refusal, ok := schedulerDecisionWithReason(
		decisions,
		scheduler.DispatchGateReasonReservedForHigherPriorityProject,
	)
	if !ok {
		t.Fatalf("scheduler decision %q not found; decisions = %#v", scheduler.DispatchGateReasonReservedForHigherPriorityProject, decisions)
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("orchestrator Run() error = %v, want context canceled", err)
		}
	case <-time.After(synchronizationWatchdog):
		t.Fatal("orchestrator did not stop before synchronization watchdog")
	}

	if refusal.ProjectID != cloudProject.ID || refusal.IssueID != issue.ID {
		t.Fatalf("refusal identity = %q/%q, want %q/%q", refusal.ProjectID, refusal.IssueID, cloudProject.ID, issue.ID)
	}
	if refusal.Result != store.SchedulerDecisionResultSkipped || refusal.WaitReason != refusal.Reason {
		t.Fatalf("refusal result/reason = %q/%q/%q, want skipped gate reason", refusal.Result, refusal.Reason, refusal.WaitReason)
	}
	var capacity map[string]any
	if err := json.Unmarshal([]byte(refusal.CapacitySnapshotJSON), &capacity); err != nil {
		t.Fatalf("capacity snapshot JSON error = %v", err)
	}
	// selected_project_id reports the project selected for the slot, which is now
	// the refused project itself. It previously named the reservation holder only
	// because the removed priority-reservation path stamped it. The holder is
	// still reported accurately below via capacity["holders"], and the refusal is
	// still genuine exhaustion: capacity 1, used 1.
	for key, want := range map[string]any{
		"pool":                scheduler.DefaultPoolName,
		"global_capacity":     float64(1),
		"global_used":         float64(1),
		"global_available":    float64(0),
		"selected_project_id": cloudProject.ID,
	} {
		if got := capacity[key]; got != want {
			t.Fatalf("capacity[%q] = %#v, want %#v; snapshot = %s", key, got, want, refusal.CapacitySnapshotJSON)
		}
	}
	holders, ok := capacity["holders"].([]any)
	if !ok || len(holders) != 1 || holders[0] != localProject.ID {
		t.Fatalf("capacity holders = %#v, want refusal-time holder %q", capacity["holders"], localProject.ID)
	}

	attempts, err := runtimeStore.ListActiveWorkAttempts(ctx, store.WorkAttemptQuery{ProjectID: cloudProject.ID})
	if err != nil {
		t.Fatalf("ListActiveWorkAttempts() error = %v", err)
	}
	if len(attempts) != 0 {
		t.Fatalf("active work attempts = %#v, want none for pre-acquisition wait", attempts)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("query database Close() error = %v", err)
		}
	})
	contention, err := store.QueryCrossClassPoolContention(ctx, db, store.PoolContentionQuery{
		Since: time.Now().Add(-time.Hour),
		ProjectClasses: map[string]string{
			localProject.ID: "local-heavy",
			cloudProject.ID: "cloud-only",
		},
	})
	if err != nil {
		t.Fatalf("QueryCrossClassPoolContention() error = %v", err)
	}
	if len(contention) != 1 || contention[0].WaitCount != 1 ||
		contention[0].WaitingClass != "cloud-only" ||
		contention[0].HoldingClass != "local-heavy" {
		t.Fatalf("contention = %#v, want one production-recorded cross-class wait", contention)
	}

	constraints, err := store.QueryCapacityConstraintWaits(ctx, db, store.CapacityConstraintQuery{
		Since: time.Now().Add(-time.Hour),
		ProjectClasses: map[string]string{
			localProject.ID: "local-heavy",
			cloudProject.ID: "cloud-only",
		},
	})
	if err != nil {
		t.Fatalf("QueryCapacityConstraintWaits() error = %v", err)
	}
	if len(constraints) != 1 ||
		constraints[0].ProjectID != cloudProject.ID ||
		constraints[0].WorkloadClass != "cloud-only" ||
		constraints[0].Reason != store.CapacityConstraintPool ||
		constraints[0].WaitCount != 1 {
		t.Fatalf("constraints = %#v, want one production-recorded cloud-only pool wait", constraints)
	}
}

func schedulerDecisionWithReason(decisions []store.SchedulerDecision, reason string) (store.SchedulerDecision, bool) {
	for _, decision := range decisions {
		if decision.Reason == reason {
			return decision, true
		}
	}
	return store.SchedulerDecision{}, false
}
