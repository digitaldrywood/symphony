package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

type checkpointAttemptStore struct {
	recordingWorkAttemptStore
	attempt store.WorkAttempt
	err     error
}

func (s *checkpointAttemptStore) WorkAttempt(context.Context, int64) (store.WorkAttempt, error) {
	return s.attempt, s.err
}

type checkpointLaneConnector struct {
	backendCapacityTestConnector
	read func() ([]connector.Issue, error)
}

func (c checkpointLaneConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return c.read()
}

func TestCheckpointValidator(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"owned", "cancelled", "no runtime", "no running", "wrong attempt", "wrong generation", "wrong mode", "no store", "no tracker", "store unavailable", "wrong issue", "terminal attempt", "expired lease", "tracker unavailable", "lane changed", "ownership lost during lookup", "runtime lost during lookup", "lease expires during lookup"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
			issue := connector.Issue{ID: "checkpoint", State: "In Progress"}
			attempts := &checkpointAttemptStore{attempt: store.WorkAttempt{IssueID: issue.ID, Status: store.WorkAttemptStatusActive, LeaseExpiresAt: now.Add(time.Minute)}}
			orch := &Orchestrator{workAttempts: attempts, now: func() time.Time { return now }}
			running := Running{Issue: issue, WorkAttemptID: 23, Generation: 5, Mode: runner.RunModeImplement}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			orch.connector = &checkpointLaneConnector{read: func() ([]connector.Issue, error) {
				switch scenario {
				case "tracker unavailable":
					return nil, errors.New("network unavailable")
				case "lane changed":
					issue.State = "Backlog"
				case "ownership lost during lookup":
					orch.latestRuntimeState.Store(&runtimeState{})
				case "runtime lost during lookup":
					orch.latestRuntimeState.Store(nil)
				case "lease expires during lookup":
					now = now.Add(time.Minute)
				}
				return []connector.Issue{issue}, nil
			}}
			switch scenario {
			case "cancelled":
				cancel()
			case "wrong attempt":
				running.WorkAttemptID++
			case "wrong generation":
				running.Generation++
			case "wrong mode":
				running.Mode = runner.RunModeRoutine
			case "no store":
				orch.workAttempts = nil
			case "no tracker":
				orch.connector = nil
			case "store unavailable":
				attempts.err = errors.New("store unavailable")
			case "wrong issue":
				attempts.attempt.IssueID = "other"
			case "terminal attempt":
				attempts.attempt.Status = store.WorkAttemptStatusTerminal
			case "expired lease":
				attempts.attempt.LeaseExpiresAt = now
			}
			state := &runtimeState{Running: map[string]Running{issue.ID: running}}
			if scenario == "no running" {
				state.Running = nil
			}
			if scenario != "no runtime" {
				orch.latestRuntimeState.Store(state)
			}
			err := orch.checkpointValidator(issue.ID, 23, 5)(ctx)
			if (err == nil) != (scenario == "owned") {
				t.Fatalf("checkpoint validation = %v", err)
			}
		})
	}
}
