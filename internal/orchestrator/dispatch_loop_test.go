package orchestrator

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestEvaluateDispatchLoopProgress(t *testing.T) {
	t.Parallel()

	clean := implementProgressDiffStats{HeadSHA: "same-workspace-head", Status: "clean"}
	dirty := implementProgressDiffStats{FilesChanged: 1, AddedLines: 4, HeadSHA: "same-workspace-head", Fingerprint: "same-diff", Status: "changed"}
	signature := autoPromoteReworkSignature{PRNumber: 42, HeadSHA: "same-head"}
	tests := []struct {
		name            string
		history         []store.WorkAttempt
		running         Running
		decision        implementCompletionProgressDecision
		wantCount       int
		wantBlock       bool
		wantReason      string
		wantBlockReason string
		limit           int
		omitStart       bool
	}{
		{
			name: "successful terminal states count",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 1),
			},
			running:         dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:        dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantCount:       3,
			wantBlock:       true,
			wantReason:      dispatchLoopDetectedReason,
			wantBlockReason: dispatchLoopDetectedReason,
		},
		{
			name: "failure terminal states count",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, clean, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, clean, nil, 1),
			},
			running:         dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:        dispatchLoopDecision("Rework", store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantCount:       3,
			wantBlock:       true,
			wantReason:      dispatchLoopDetectedReason,
			wantBlockReason: dispatchLoopDetectedReason,
		},
		{
			name: "unverified pull request updates count",
			history: []store.WorkAttempt{
				dispatchLoopReportedPRUpdateAttempt(2),
				dispatchLoopReportedPRUpdateAttempt(1),
			},
			running: dispatchLoopRunning("In Progress", DiffStats{Status: "clean"}),
			decision: func() implementCompletionProgressDecision {
				decision := dispatchLoopDecision("In Progress", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"})
				decision.Reason = "pull_request_created_or_updated"
				decision.ProgressKinds = []string{"pull_request"}
				return decision
			}(),
			wantCount:       3,
			wantBlock:       true,
			wantReason:      dispatchLoopDetectedReason,
			wantBlockReason: dispatchLoopDetectedReason,
		},
		{
			name:       "single completed run does not trip even with limit one",
			running:    dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:   dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantCount:  1,
			wantReason: implementDependencyDeferralReason,
			limit:      1,
		},
		{
			name:       "missing dispatch start fails open",
			running:    dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:   dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			omitStart:  true,
			wantReason: implementDependencyDeferralReason,
		},
		{
			name: "diff advancement resets",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, dirty, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, dirty, nil, 1),
			},
			running:    dispatchLoopRunning("Rework", DiffStats{FilesChanged: 2, AddedLines: 8, Fingerprint: "new-diff", Status: "changed"}),
			decision:   dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{FilesChanged: 2, AddedLines: 8, Fingerprint: "new-diff", Status: "changed"}),
			wantReason: implementDependencyDeferralReason,
		},
		{
			name: "commit advancement resets",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, implementProgressDiffStats{HeadSHA: "old-head", Status: "clean"}, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, implementProgressDiffStats{HeadSHA: "old-head", Status: "clean"}, nil, 1),
			},
			running:    dispatchLoopRunning("Rework", DiffStats{HeadSHA: "new-head", Status: "clean"}),
			decision:   dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{HeadSHA: "new-head", Status: "clean"}),
			wantReason: implementDependencyDeferralReason,
		},
		{
			name: "pull request advancement resets",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, signature, clean, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, signature, clean, nil, 1),
			},
			running:    dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:   dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{PRNumber: 42, HeadSHA: "new-head"}, DiffStats{Status: "clean"}),
			wantReason: implementDependencyDeferralReason,
		},
		{
			name: "lane advancement resets",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 1),
			},
			running:    dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:   dispatchLoopDecision("In Progress", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantReason: implementDependencyDeferralReason,
		},
		{
			name: "breaker park and release lane residency preserves count",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttemptInLane(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 2, "In Progress"),
				dispatchLoopHistoryAttemptInLane(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, nil, 1, "In Progress"),
			},
			running:         dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:        dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantCount:       3,
			wantBlock:       true,
			wantReason:      dispatchLoopDetectedReason,
			wantBlockReason: dispatchLoopDetectedReason,
		},
		{
			name: "workpad-only changes do not reset",
			history: []store.WorkAttempt{
				dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, []string{"audit_artifact"}, 2),
				dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, clean, []string{"workpad_predicate"}, 1),
			},
			running:         dispatchLoopRunning("Rework", DiffStats{Status: "clean"}),
			decision:        dispatchLoopDecision("Rework", store.WorkAttemptTerminalSuccess, autoPromoteReworkSignature{}, DiffStats{Status: "clean"}),
			wantCount:       3,
			wantBlock:       true,
			wantReason:      dispatchLoopDetectedReason,
			wantBlockReason: dispatchLoopDetectedReason,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.limit > 0 {
				tt.decision.NoProgressLimit = tt.limit
			}
			if !tt.omitStart {
				tt.running.DispatchLoopStart = dispatchLoopTestStart(
					tt.running.DispatchSourceState,
					tt.decision.CurrentSignature,
					implementProgressDiffStatsFromDiffStats(tt.decision.WorkspaceDiffStats),
				)
			}
			orch := &Orchestrator{
				cfg: Config{
					Project:                 scheduler.ProjectCandidate{ID: "detent"},
					NoProgressSpendLimitUSD: 0,
				},
				workAttempts: &implementProgressAttemptStore{history: tt.history},
			}

			got := orch.evaluateDispatchLoopProgress(t.Context(), tt.running, tt.decision)

			if got.ConsecutiveNoProgress != tt.wantCount || got.Block != tt.wantBlock || got.Reason != tt.wantReason || got.BlockReason != tt.wantBlockReason {
				t.Fatalf("evaluateDispatchLoopProgress() = count %d block %v reason %q block reason %q, want %d %v %q %q", got.ConsecutiveNoProgress, got.Block, got.Reason, got.BlockReason, tt.wantCount, tt.wantBlock, tt.wantReason, tt.wantBlockReason)
			}
		})
	}
}

func TestEvaluateDispatchLoopProgressCreditsWithinAttemptProgress(t *testing.T) {
	t.Parallel()

	currentSignature := autoPromoteReworkSignature{PRNumber: 42, HeadSHA: "current-pr-head"}
	currentDiff := implementProgressDiffStats{FilesChanged: 2, AddedLines: 8, HeadSHA: "current-workspace-head", Fingerprint: "current-diff", Status: "changed"}
	baseStart := dispatchLoopTestStart("Rework", currentSignature, currentDiff)
	tests := []struct {
		name   string
		mutate func(*dispatchLoopStartRecord)
	}{
		{name: "lane", mutate: func(start *dispatchLoopStartRecord) { start.Fingerprint.Lane = "todo" }},
		{name: "workspace diff", mutate: func(start *dispatchLoopStartRecord) {
			start.Fingerprint.FilesChanged = 0
			start.Fingerprint.AddedLines = 0
			start.Fingerprint.DiffFingerprint = ""
			start.Fingerprint.DiffStatus = "clean"
		}},
		{name: "workspace head", mutate: func(start *dispatchLoopStartRecord) { start.Fingerprint.WorkspaceHead = "previous-workspace-head" }},
		{name: "pull request head", mutate: func(start *dispatchLoopStartRecord) { start.Fingerprint.PRHeadSHA = "previous-pr-head" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			running := dispatchLoopRunning("Rework", DiffStats{
				FilesChanged: 2,
				AddedLines:   8,
				HeadSHA:      "current-workspace-head",
				Fingerprint:  "current-diff",
				Status:       "changed",
			})
			running.DispatchLoopStart = baseStart
			tt.mutate(&running.DispatchLoopStart)
			decision := dispatchLoopDecision("Rework", store.WorkAttemptTerminalFailure, currentSignature, running.DiffStats)
			orch := &Orchestrator{
				cfg: Config{Project: scheduler.ProjectCandidate{ID: "detent"}},
				workAttempts: &implementProgressAttemptStore{history: []store.WorkAttempt{
					dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalFailure, currentSignature, currentDiff, nil, 2),
					dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalFailure, currentSignature, currentDiff, nil, 1),
				}},
			}

			got := orch.evaluateDispatchLoopProgress(t.Context(), running, decision)

			if got.ConsecutiveNoProgress != 0 || got.Block || got.Reason != implementDependencyDeferralReason {
				t.Fatalf("evaluateDispatchLoopProgress() = count %d block %v reason %q, want reset", got.ConsecutiveNoProgress, got.Block, got.Reason)
			}
		})
	}
}

func TestEvaluateDispatchLoopProgressFailsOpenWithUnavailableCompletionEvidence(t *testing.T) {
	t.Parallel()

	running := dispatchLoopRunning("Rework", DiffStats{HeadSHA: "workspace-head", Status: "clean"})
	running.DispatchLoopStart = dispatchLoopTestStart(
		"Rework",
		autoPromoteReworkSignature{},
		implementProgressDiffStats{HeadSHA: "workspace-head", Status: "clean"},
	)
	decision := dispatchLoopDecision("Rework", store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, DiffStats{Status: "clean"})
	decision.WorkspaceDiffStats = DiffStats{}
	orch := &Orchestrator{
		cfg: Config{Project: scheduler.ProjectCandidate{ID: "detent"}},
		workAttempts: &implementProgressAttemptStore{history: []store.WorkAttempt{
			dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, implementProgressDiffStats{HeadSHA: "workspace-head", Status: "clean"}, nil, 2),
			dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, implementProgressDiffStats{HeadSHA: "workspace-head", Status: "clean"}, nil, 1),
		}},
	}

	got := orch.evaluateDispatchLoopProgress(t.Context(), running, decision)

	if got.ConsecutiveNoProgress != 0 || got.Block {
		t.Fatalf("evaluateDispatchLoopProgress() = count %d block %v, want fail-open reset", got.ConsecutiveNoProgress, got.Block)
	}
}

func TestEvaluateDispatchLoopProgressLateFailureBreaksSequence(t *testing.T) {
	t.Parallel()

	currentSignature := autoPromoteReworkSignature{PRNumber: 42, HeadSHA: "pushed-head"}
	currentDiff := implementProgressDiffStats{HeadSHA: "pushed-head", Status: "clean"}
	lateProgress := dispatchLoopHistoryAttemptWithStart(
		1,
		store.WorkAttemptTerminalFailure,
		dispatchLoopTestStart("Rework", autoPromoteReworkSignature{}, implementProgressDiffStats{HeadSHA: "base-head", Status: "clean"}),
		currentSignature,
		implementProgressDiffStats{},
		0,
	)
	unchangedRetry := dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalFailure, currentSignature, currentDiff, nil, 1)
	running := dispatchLoopRunning("Rework", DiffStats{HeadSHA: "pushed-head", Status: "clean"})
	running.DispatchLoopStart = dispatchLoopTestStart("Rework", currentSignature, currentDiff)
	decision := dispatchLoopDecision("Rework", store.WorkAttemptTerminalFailure, currentSignature, running.DiffStats)
	orch := &Orchestrator{
		cfg:          Config{Project: scheduler.ProjectCandidate{ID: "detent"}},
		workAttempts: &implementProgressAttemptStore{history: []store.WorkAttempt{unchangedRetry, lateProgress}},
	}

	got := orch.evaluateDispatchLoopProgress(t.Context(), running, decision)

	if got.ConsecutiveNoProgress != 2 || got.Block || got.Reason == dispatchLoopDetectedReason {
		t.Fatalf("evaluateDispatchLoopProgress() = count %d block %v reason %q, want late progress to exclude attempt 1", got.ConsecutiveNoProgress, got.Block, got.Reason)
	}
}

func TestDispatchLoopStartPersistence(t *testing.T) {
	t.Parallel()

	issue := implementProgressIssueWithoutPR()
	start := newDispatchLoopStartRecord(issue, runpkg.RunModeImplement)
	attempts := &implementProgressAttemptStore{}
	orch := &Orchestrator{
		cfg:          normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}}),
		workAttempts: attempts,
	}
	state := newState(orch.cfg)
	if _, ok := orch.startDurableWorkAttempt(t.Context(), &state, issue, 1, time.Now(), "local", runpkg.RunModeImplement, start); !ok {
		t.Fatal("startDurableWorkAttempt() = false")
	}
	if len(attempts.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(attempts.starts))
	}
	persistedStart := dispatchLoopStartFromMetadata(t, attempts.starts[0].WorkerMetadataJSON)
	if persistedStart.Captured || persistedStart.Persisted || !persistedStart.LaneAvailable || !persistedStart.PullRequestAvailable {
		t.Fatalf("initial dispatch loop start = %#v, want durable pre-workspace baseline", persistedStart)
	}

	state.Running[issue.ID] = Running{
		Issue:             issue,
		WorkAttemptID:     1,
		Mode:              runpkg.RunModeImplement,
		DispatchLoopStart: start,
	}
	orch.handleRunUpdate(&state, runUpdate{
		issueID: issue.ID,
		usage: runpkg.UsageUpdate{DispatchLoopStart: &runpkg.DispatchLoopStartSnapshot{
			WorkspaceDiffAvailable: true,
			WorkspaceHeadAvailable: true,
			DiffStats:              DiffStats{HeadSHA: "dispatch-head", Status: "clean"},
		}},
	})
	if len(attempts.heartbeats) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(attempts.heartbeats))
	}
	persistedStart = dispatchLoopStartFromMetadata(t, attempts.heartbeats[0].WorkerMetadataJSON)
	if !persistedStart.Captured || !persistedStart.Persisted || persistedStart.Fingerprint.WorkspaceHead != "dispatch-head" {
		t.Fatalf("heartbeat dispatch loop start = %#v, want complete persisted baseline", persistedStart)
	}
}

func TestDispatchLoopStartPersistenceFailureFailsOpen(t *testing.T) {
	t.Parallel()

	issue := implementProgressIssueWithoutPR()
	attempts := &implementProgressAttemptStore{heartbeatErr: errors.New("store unavailable")}
	orch := &Orchestrator{workAttempts: attempts}
	state := newState(Config{})
	state.Running[issue.ID] = Running{
		Issue:             issue,
		WorkAttemptID:     1,
		Mode:              runpkg.RunModeImplement,
		DispatchLoopStart: newDispatchLoopStartRecord(issue, runpkg.RunModeImplement),
	}

	orch.handleRunUpdate(&state, runUpdate{
		issueID: issue.ID,
		usage: runpkg.UsageUpdate{DispatchLoopStart: &runpkg.DispatchLoopStartSnapshot{
			WorkspaceDiffAvailable: true,
			WorkspaceHeadAvailable: true,
			DiffStats:              DiffStats{HeadSHA: "dispatch-head", Status: "clean"},
		}},
	})

	if state.Running[issue.ID].DispatchLoopStart.Persisted {
		t.Fatalf("dispatch loop start = %#v, want untrusted after persistence failure", state.Running[issue.ID].DispatchLoopStart)
	}
	decision := dispatchLoopDecision("In Progress", store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, DiffStats{HeadSHA: "dispatch-head", Status: "clean"})
	got := orch.evaluateDispatchLoopProgress(t.Context(), state.Running[issue.ID], decision)
	if got.ConsecutiveNoProgress != 0 || got.Block {
		t.Fatalf("evaluateDispatchLoopProgress() = count %d block %v, want persistence failure to fail open", got.ConsecutiveNoProgress, got.Block)
	}
}

func TestHandleRunResultTripsDispatchLoopAfterFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 15, 0, 0, 0, time.UTC)
	issue := implementProgressIssueWithoutPR()
	issue.State = "Rework"
	history := []store.WorkAttempt{
		dispatchLoopHistoryAttempt(2, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, implementProgressDiffStats{Status: "clean"}, nil, 2),
		dispatchLoopHistoryAttempt(1, store.WorkAttemptTerminalFailure, autoPromoteReworkSignature{}, implementProgressDiffStats{Status: "clean"}, nil, 1),
	}
	tracker := &implementProgressConnector{refreshed: issue}
	attempts := &implementProgressAttemptStore{history: history}
	cfg := normalizeConfig(Config{
		Project:        scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote:    AutoPromoteConfig{NoProgressLimit: 3},
		ActiveStates:   []string{"Rework"},
		ObservedStates: []string{"Blocked"},
		TerminalStates: []string{"Done"},
	})
	orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:               issue,
		Attempt:             1,
		WorkAttemptID:       42,
		Mode:                runpkg.RunModeImplement,
		DispatchSourceState: "Rework",
		StartedAt:           now.Add(-time.Minute),
		DiffStats:           dispatchLoopTestRunnerDiff(DiffStats{Status: "clean"}),
		DispatchLoopStart:   dispatchLoopTestStart("Rework", autoPromoteReworkSignature{}, implementProgressDiffStatsFromDiffStats(dispatchLoopTestRunnerDiff(DiffStats{Status: "clean"}))),
	}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Request:     runpkg.RunRequest{Mode: runpkg.RunModeImplement},
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateFailed, DiffStats: dispatchLoopTestRunnerDiff(DiffStats{Status: "clean"})},
		Err:         errors.New("worker failed without progress"),
	})

	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalNoProgress || attempts.completions[0].ErrorClass != dispatchLoopDetectedReason {
		t.Fatalf("completions = %#v, want dispatch-loop no-progress terminal", attempts.completions)
	}
	if blocked, ok := state.Blocked[issue.ID]; !ok || blocked.Reason != dispatchLoopDetectedReason {
		t.Fatalf("Blocked[%q] = %#v, %v", issue.ID, blocked, ok)
	}
	if len(tracker.comments) != 3 || tracker.comments[2].body == "" {
		t.Fatalf("comments = %#v, want loop-specific park comment", tracker.comments)
	}
}

func dispatchLoopRunning(lane string, diff DiffStats) Running {
	issue := connector.Issue{ID: "issue-loop", Identifier: "digitaldrywood/detent#1886", State: lane}
	return Running{Issue: issue, DispatchSourceState: lane, DiffStats: dispatchLoopTestRunnerDiff(diff), Mode: runpkg.RunModeImplement}
}

func dispatchLoopDecision(lane string, outcome store.WorkAttemptTerminalState, signature autoPromoteReworkSignature, diff DiffStats) implementCompletionProgressDecision {
	issue := connector.Issue{ID: "issue-loop", Identifier: "digitaldrywood/detent#1886", State: lane}
	diff = dispatchLoopTestRunnerDiff(diff)
	return implementCompletionProgressDecision{
		Issue:              issue,
		Outcome:            outcome,
		Reason:             implementDependencyDeferralReason,
		CurrentSignature:   signature,
		WorkspaceDiffStats: diff,
		TrackerState:       lane,
		NoProgressLimit:    3,
	}
}

func dispatchLoopHistoryAttempt(
	id int64,
	terminal store.WorkAttemptTerminalState,
	signature autoPromoteReworkSignature,
	diff implementProgressDiffStats,
	progressKinds []string,
	count int,
) store.WorkAttempt {
	return dispatchLoopHistoryAttemptInLane(id, terminal, signature, diff, progressKinds, count, "Rework")
}

func dispatchLoopHistoryAttemptInLane(
	id int64,
	terminal store.WorkAttemptTerminalState,
	signature autoPromoteReworkSignature,
	diff implementProgressDiffStats,
	progressKinds []string,
	count int,
	lane string,
) store.WorkAttempt {
	if strings.TrimSpace(diff.HeadSHA) == "" {
		diff.HeadSHA = "same-workspace-head"
	}
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "detent",
		IssueID:       "issue-loop",
		Identifier:    "digitaldrywood/detent#1886",
		WorkerType:    "agent",
		Lane:          lane,
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: terminal,
		CompletedAt:   time.Date(2026, 8, 18, 14, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			dispatchLoopStartMetadataKey: dispatchLoopTestStart(lane, signature, diff),
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:               string(terminal),
				Reason:                implementDependencyDeferralReason,
				CurrentSignature:      implementProgressSignatureRecordFromSignature(signature),
				WorkspaceDiffStats:    diff,
				TrackerState:          lane,
				ConsecutiveNoProgress: count,
				NoProgressLimit:       3,
				ProgressKinds:         progressKinds,
			},
		}),
	}
}

func dispatchLoopHistoryAttemptWithStart(
	id int64,
	terminal store.WorkAttemptTerminalState,
	start dispatchLoopStartRecord,
	signature autoPromoteReworkSignature,
	diff implementProgressDiffStats,
	count int,
) store.WorkAttempt {
	attempt := dispatchLoopHistoryAttemptInLane(id, terminal, signature, diff, nil, count, start.Fingerprint.Lane)
	attempt.WorkerMetadataJSON = marshalWorkAttemptJSON(map[string]any{
		dispatchLoopStartMetadataKey: start,
		implementProgressMetadataKey: implementProgressRecord{
			Outcome:               string(terminal),
			Reason:                "late_cleanup_failure",
			CurrentSignature:      implementProgressSignatureRecordFromSignature(signature),
			WorkspaceDiffStats:    diff,
			TrackerState:          start.Fingerprint.Lane,
			ConsecutiveNoProgress: count,
			NoProgressLimit:       3,
		},
	})
	return attempt
}

func dispatchLoopReportedPRUpdateAttempt(id int64) store.WorkAttempt {
	diff := implementProgressDiffStats{HeadSHA: "same-workspace-head", Status: "clean"}
	return store.WorkAttempt{
		ID:            id,
		ProjectID:     "detent",
		IssueID:       "issue-loop",
		Identifier:    "digitaldrywood/detent#1886",
		WorkerType:    "agent",
		Lane:          "In Progress",
		Status:        store.WorkAttemptStatusTerminal,
		TerminalState: store.WorkAttemptTerminalSuccess,
		CompletedAt:   time.Date(2026, 8, 18, 14, int(id), 0, 0, time.UTC),
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			dispatchLoopStartMetadataKey: dispatchLoopTestStart("In Progress", autoPromoteReworkSignature{}, diff),
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:            string(store.WorkAttemptTerminalSuccess),
				Reason:             "pull_request_created_or_updated",
				WorkspaceDiffStats: diff,
				ProgressKinds:      []string{"pull_request"},
			},
		}),
	}
}

func dispatchLoopTestRunnerDiff(diff DiffStats) DiffStats {
	if strings.TrimSpace(diff.HeadSHA) == "" {
		diff.HeadSHA = "same-workspace-head"
	}
	return diff
}

func dispatchLoopTestStart(lane string, signature autoPromoteReworkSignature, diff implementProgressDiffStats) dispatchLoopStartRecord {
	return dispatchLoopStartRecord{
		Fingerprint:            dispatchLoopFingerprintFromValues(lane, signature, diff),
		Captured:               true,
		Persisted:              true,
		LaneAvailable:          true,
		PullRequestAvailable:   true,
		WorkspaceDiffAvailable: true,
		WorkspaceHeadAvailable: strings.TrimSpace(diff.HeadSHA) != "",
	}
}

func dispatchLoopStartFromMetadata(t *testing.T, metadata string) dispatchLoopStartRecord {
	t.Helper()
	var root struct {
		DispatchLoopStart dispatchLoopStartRecord `json:"dispatch_loop_start"`
	}
	if err := json.Unmarshal([]byte(metadata), &root); err != nil {
		t.Fatalf("unmarshal dispatch loop start metadata: %v", err)
	}
	return root.DispatchLoopStart
}
