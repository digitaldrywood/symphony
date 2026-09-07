package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/tracker"
	"github.com/digitaldrywood/detent/internal/workspace"
)

type testExecution struct {
	recovery    tracker.NativeRecovery
	validateErr error
	checkpoint  *tracker.NativeCheckpoint
	finish      string
	started     bool
}

func (e *testExecution) Guard(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, e.validateErr
}

func (e *testExecution) Validate(context.Context) error { return e.validateErr }
func (e *testExecution) Start(context.Context, tracker.NativeExecutionIdentity) error {
	e.started = true
	return nil
}
func (e *testExecution) Checkpoint(_ context.Context, checkpoint tracker.NativeCheckpoint) error {
	e.checkpoint = &checkpoint
	return nil
}
func (e *testExecution) Finish(_ context.Context, outcome string) error {
	e.finish = outcome
	return nil
}
func (e *testExecution) Recovery() tracker.NativeRecovery { return e.recovery }

type retainedExecutionWorkspace struct {
	*fakeWorkspaceBackend
	retained bool
}

func (w *retainedExecutionWorkspace) PreserveIssue(context.Context, workspace.Issue) (workspace.Preservation, error) {
	w.retained = true
	return workspace.Preservation{Preserved: true}, nil
}

func TestNativeRecoveryDecision(t *testing.T) {
	t.Parallel()
	identity := tracker.NativeExecutionIdentity{Role: "implement", Backend: "codex", Model: "test"}
	for _, test := range []struct {
		name   string
		edit   func(*tracker.NativeRecovery, **workspace.RecoveryState, *bool)
		action string
		reason string
	}{
		{"verified session", func(*tracker.NativeRecovery, **workspace.RecoveryState, *bool) {}, "resume_session", "verified_local_session"},
		{"first run", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) { r.Attempts = nil }, "fresh_checkout", "no_prior_attempt"},
		{"missing checkpoint", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) { r.Attempts[0].Checkpoint = nil }, "fresh_checkout", "checkpoint_missing"},
		{"machine lost with dirty work", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) { r.Lease.MachineID = "other" }, "manual_recovery", "checkpoint_unavailable"},
		{"local workspace missing", func(_ *tracker.NativeRecovery, local **workspace.RecoveryState, _ *bool) { *local = nil }, "manual_recovery", "checkpoint_unavailable"},
		{"dirty checkpoint replaced", func(_ *tracker.NativeRecovery, local **workspace.RecoveryState, _ *bool) {
			(*local).WorkspaceFingerprint = "different"
		}, "manual_recovery", "local_checkpoint_changed"},
		{"inaccessible checkpoint", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.Availability = "inaccessible"
		}, "manual_recovery", "checkpoint_unavailable"},
		{"customer receipt unverified", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.Storage = "customer_store"
		}, "manual_recovery", "checkpoint_unavailable"},
		{"clean inaccessible checkpoint", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.Availability = "missing"
			r.Attempts[0].Checkpoint.WorktreeState = "clean"
		}, "fresh_checkout", "checkpoint_unavailable"},
		{"provider session missing", func(_ *tracker.NativeRecovery, _ **workspace.RecoveryState, available *bool) { *available = false }, "fresh_checkout", "session_restart_required"},
		{"policy changed", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) { r.Lease.PolicyID = "new-policy" }, "fresh_checkout", "session_restart_required"},
		{"backend changed", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Identity = &tracker.NativeExecutionIdentity{Role: "implement", Backend: "claude", Model: "test"}
		}, "fresh_checkout", "session_restart_required"},
		{"push ambiguity", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.ExternalEffect = "git_push"
			r.Attempts[0].Checkpoint.EffectState = "ambiguous"
		}, "manual_recovery", "external_effect_uncertain"},
		{"PR pending", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.ExternalEffect = "pr_create"
			r.Attempts[0].Checkpoint.EffectState = "pending"
		}, "manual_recovery", "external_effect_uncertain"},
		{"manual checkpoint", func(r *tracker.NativeRecovery, _ **workspace.RecoveryState, _ *bool) {
			r.Attempts[0].Checkpoint.Resume = "manual_recovery"
		}, "manual_recovery", "checkpoint_requires_recovery"},
	} {
		t.Run(test.name, func(t *testing.T) {
			local := &workspace.RecoveryState{HeadSHA: "head", WorkspaceFingerprint: "digest"}
			available := true
			recovery := tracker.NativeRecovery{Lease: tracker.NativeLease{MachineID: "machine", PolicyID: "policy"}, Attempts: []tracker.NativeAttempt{{
				NativeRunData: tracker.NativeRunData{Identity: &identity, MachineID: "machine", PolicyID: "policy"},
				Checkpoint:    &tracker.NativeCheckpoint{Resume: "resume_session", Availability: "available", Storage: "local_only", WorktreeState: "dirty", HeadSHA: "head", WorkspaceDigest: "digest", ExternalEffect: "none", EffectState: "none"},
			}}}
			test.edit(&recovery, &local, &available)
			action, reason := nativeRecoveryAction(recovery, local, available, identity)
			if action != test.action || reason != test.reason {
				t.Fatalf("recovery = %s/%s, want %s/%s", action, reason, test.action, test.reason)
			}
		})
	}
}

func TestNativeEpiloguePreservesBeforeCleanup(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		state    workspace.RecoveryState
		lost     bool
		retained bool
		after    bool
	}{
		{"clean", workspace.RecoveryState{HeadSHA: "head"}, false, false, true},
		{"dirty", workspace.RecoveryState{TrackedPaths: []string{"work.go"}}, false, true, false},
		{"unpushed", workspace.RecoveryState{UnpushedCommits: 2}, false, true, false},
		{"claim lost", workspace.RecoveryState{TrackedPaths: []string{"work.go"}}, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &retainedExecutionWorkspace{fakeWorkspaceBackend: &fakeWorkspaceBackend{recoveryStates: []workspace.RecoveryState{test.state}}}
			execution := &testExecution{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.lost {
				execution.validateErr = ErrExecutionAuthorityUnavailable
				cancel()
			}
			r := &Runner{workspace: backend, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), afterRunTimeout: time.Second}
			err := r.afterExecution(ctx, RunRequest{Execution: execution, Issue: connector.Issue{ID: "work"}}, backend, workspace.Info{}, workspace.Issue{})
			if errors.Is(err, ErrExecutionAuthorityUnavailable) != test.lost || backend.retained != test.retained || backend.afterRun != test.after {
				t.Fatalf("epilogue error=%v retained=%v hook=%v", err, backend.retained, backend.afterRun)
			}
			if test.lost && execution.checkpoint != nil {
				t.Fatal("lost owner wrote a checkpoint")
			}
			if !test.lost && execution.checkpoint == nil {
				t.Fatal("epilogue omitted checkpoint")
			}
		})
	}
}

func TestNativeGuardPreventsRun(t *testing.T) {
	t.Parallel()
	r := &Runner{}
	_, err := r.Run(t.Context(), RunRequest{Execution: &testExecution{validateErr: ErrExecutionAuthorityUnavailable}})
	if !errors.Is(err, ErrExecutionAuthorityUnavailable) {
		t.Fatalf("guard = %v", err)
	}
}

type executionUnavailableBackend struct{}

func (executionUnavailableBackend) Run(context.Context, RunRequest) (RunResult, error) {
	return RunResult{}, ErrExecutionAuthorityUnavailable
}

func TestNativeOutagePreservesFailureBudget(t *testing.T) {
	t.Parallel()
	supervisor, err := NewSupervisor(executionUnavailableBackend{}, SupervisorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	completion := supervisor.Run(t.Context(), RunRequest{Attempt: 4})
	if !completion.Retryable || completion.RetryAttempt != 4 || completion.RetryDelay != supervisor.OverloadRetryDelay() {
		t.Fatalf("outage consumed retry budget: %#v", completion)
	}
}

func TestNativeRecoveryPromptIncludesContext(t *testing.T) {
	t.Parallel()
	execution := &testExecution{recovery: tracker.NativeRecovery{Issue: tracker.NativeIssue{Title: "Native issue"}, Discussion: []tracker.NativeComment{{Body: "Prior discussion"}}, Attempts: []tracker.NativeAttempt{{Status: "interrupted"}}}}
	prompt, err := nativeRecoveryPrompt(execution)
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{"Native issue", "Prior discussion", "interrupted", "untrusted task content", "Do not fetch GitHub issue history"} {
		if !strings.Contains(prompt, content) {
			t.Errorf("prompt omitted %q", content)
		}
	}
}

func TestNativeRunnerPublishesOnlyAfterRecovery(t *testing.T) {
	t.Parallel()
	for _, blocked := range []bool{false, true} {
		name := "first run"
		if blocked {
			name = "lost checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			backend := &retainedExecutionWorkspace{fakeWorkspaceBackend: &fakeWorkspaceBackend{info: workspace.Info{Path: t.TempDir(), Key: "native", Branch: "native"}}}
			agent := &fakeCodexClient{}
			execution := &testExecution{}
			if blocked {
				execution.recovery = tracker.NativeRecovery{Lease: tracker.NativeLease{MachineID: "new-machine"}, Attempts: []tracker.NativeAttempt{{NativeRunData: tracker.NativeRunData{MachineID: "lost-machine"}, Checkpoint: &tracker.NativeCheckpoint{Storage: "local_only", WorktreeState: "dirty", Resume: "resume_session"}}}}
			}
			r, err := NewRunner(Dependencies{Workflow: config.Workflow{Config: config.Config{}, Prompt: "Complete the native issue"}, Workspace: backend, AgentBackend: agent})
			if err != nil {
				t.Fatal(err)
			}
			_, err = r.Run(t.Context(), RunRequest{Execution: execution, Issue: connector.Issue{ID: "native", Identifier: "native#1"}, Mode: RunModePlan})
			if errors.Is(err, ErrNativeRecoveryRequired) != blocked {
				t.Fatalf("run error = %v", err)
			}
			if blocked && execution.started {
				t.Fatal("unresolved checkpoint was superseded by a new attempt")
			}
			if !blocked && (!execution.started || execution.finish == "" || execution.checkpoint == nil) {
				t.Fatalf("native run omitted lifecycle: %#v", execution)
			}
		})
	}
}

type artifactExecutionProbe struct {
	testExecution
	failure   error
	finalized bool
}

func (*artifactExecutionProbe) PrepareArtifacts(context.Context, string) error { return nil }
func (*artifactExecutionProbe) ArtifactLog(context.Context, string) error      { return nil }
func (e *artifactExecutionProbe) FinalizeArtifacts(context.Context, string) error {
	e.finalized = true
	return e.failure
}

func TestArtifactsFinalizeBeforeWorkspaceCleanup(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(strconv.FormatBool(failed), func(t *testing.T) {
			backend := &retainedExecutionWorkspace{fakeWorkspaceBackend: &fakeWorkspaceBackend{recoveryStates: []workspace.RecoveryState{{HeadSHA: "head"}}}}
			execution := &artifactExecutionProbe{}
			if failed {
				execution.failure = errors.New("upload unavailable")
			}
			r := &Runner{workspace: backend, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), afterRunTimeout: time.Second}
			err := r.afterExecution(t.Context(), RunRequest{Execution: execution, Issue: connector.Issue{ID: "work"}}, backend, workspace.Info{}, workspace.Issue{})
			if !execution.finalized || backend.afterRun == failed || (err != nil) != failed {
				t.Fatal("cleanup preceded durable finalization", err, backend.afterRun)
			}
		})
	}
}
