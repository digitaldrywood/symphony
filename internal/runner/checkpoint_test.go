package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/workspace"
)

type checkpointBackendStub struct {
	calls       int
	err         error
	published   string
	environment procgroup.Environment
}

func (b *checkpointBackendStub) PrepareCheckpoint(context.Context, workspace.Info, workspace.Issue) (workspace.CheckpointPlan, error) {
	return workspace.CheckpointPlan{}, nil
}

func (b *checkpointBackendStub) Checkpoint(ctx context.Context, _ workspace.CheckpointPlan, selection workspace.CheckpointSelection, guard func(context.Context) error, environment procgroup.Environment) (string, error) {
	b.calls++
	b.environment = environment
	if err := guard(ctx); err != nil {
		return "", err
	}
	if !selection.Reviewed || len(selection.Paths) != 1 || selection.Paths[0] != "implementation.go" {
		return "", errors.New("wrong selection")
	}
	return b.published, b.err
}

type checkpointAgentStub struct{}

func (checkpointAgentStub) RunTurn(context.Context, AgentTurnRequest, AgentUpdateHandler) (AgentTurnResult, error) {
	return AgentTurnResult{}, nil
}
func (checkpointAgentStub) RunTurnWithTools(context.Context, AgentTurnRequest, []AgentTool, AgentToolHandler, AgentUpdateHandler) (AgentTurnResult, error) {
	return AgentTurnResult{}, nil
}

func checkpointRunnerFixture(t *testing.T) (*Runner, *workerCheckpoint, *checkpointBackendStub) {
	t.Helper()
	root := t.TempDir()
	backend := &checkpointBackendStub{published: strings.Repeat("b", 40)}
	return &Runner{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: time.Now}, &workerCheckpoint{
		backend: backend,
		plan:    workspace.CheckpointPlan{Info: workspace.Info{Path: root, Branch: "owned-issue"}, BaseSHA: strings.Repeat("a", 40), Journal: filepath.Join(root, "checkpoint.json")},
		request: RunRequest{Issue: connector.Issue{ID: "2313", Identifier: "example/repo#2313", Title: "Recover intended work"}, CheckpointValidate: func(context.Context) error { return nil }},
	}, backend
}

func submitCheckpoint(t *testing.T, ctx context.Context, req RunRequest) {
	t.Helper()
	arguments, err := json.Marshal(workspace.CheckpointSelection{HeadSHA: strings.Repeat("a", 40), Paths: []string{"implementation.go"}, Reviewed: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := req.AgentToolHandler(ctx, AgentToolCall{Name: "detent_checkpoint_selection", Arguments: arguments})
	if err != nil || !result.Success {
		t.Fatalf("selection = %+v, %v", result, err)
	}
}

func TestWorkerCheckpointHandshake(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"published", "unchanged", "cancelled", "lost ownership", "unsupported provider", "provider unavailable", "no selection", "network unavailable", "duplicate selection", "invalid selection", "cancel during handshake"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			runner, checkpoint, publication := checkpointRunnerFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var agent AgentBackend = checkpointAgentStub{}
			calls := 0
			if scenario == "cancelled" {
				cancel()
			}
			if scenario == "lost ownership" {
				checkpoint.request.CheckpointValidate = func(context.Context) error { return ErrExecutionAuthorityUnavailable }
			}
			if scenario == "unsupported provider" {
				agent = nonVerifyingAgentBackend{}
			}
			if scenario == "network unavailable" {
				publication.err = errors.New("network unavailable")
			}
			if scenario == "unchanged" {
				publication.published = checkpoint.plan.BaseSHA
			}
			run := func(ctx context.Context, turn AgentTurnRequest, req RunRequest) agentTurnExecution {
				calls++
				if !turn.ReadOnly || !turn.SupplementalTools || req.sessionBrake != nil || turn.MaxDuration != checkpointTimeout {
					t.Fatalf("unbounded or writable checkpoint: %+v", turn)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("checkpoint lacks deadline")
				}
				if scenario == "provider unavailable" {
					return agentTurnExecution{err: errors.New("provider unavailable")}
				}
				if scenario == "no selection" {
					return agentTurnExecution{}
				}
				if scenario == "invalid selection" {
					for _, input := range []string{`{}`, `{"reviewed":true} {}`, `{"unknown":true}`, `{"head_sha":"x","paths":[],"reviewed":true}`} {
						result, err := req.AgentToolHandler(ctx, AgentToolCall{Name: "detent_checkpoint_selection", Arguments: json.RawMessage(input)})
						if err != nil || result.Success {
							t.Fatalf("invalid selection accepted: %+v %v", result, err)
						}
					}
					return agentTurnExecution{}
				}
				submitCheckpoint(t, ctx, req)
				if scenario == "duplicate selection" {
					result, err := req.AgentToolHandler(ctx, AgentToolCall{Name: "detent_checkpoint_selection", Arguments: json.RawMessage(`{}`)})
					if result.Success || err != nil {
						t.Fatalf("duplicate accepted: %+v, %v", result, err)
					}
				}
				if scenario == "cancel during handshake" {
					cancel()
				}
				return agentTurnExecution{result: RunResult{FinalState: FinalStateCompleted}}
			}
			runner.workerCheckpoint(ctx, checkpoint, SessionBrakeReasonDuration, agent, AgentTurnRequest{}, run)
			wantPublished := scenario == "published" || scenario == "duplicate selection"
			if checkpoint.record == nil || checkpoint.record.Published != wantPublished {
				t.Fatalf("record = %+v; published = %v", checkpoint.record, wantPublished)
			}
			if scenario == "cancelled" || scenario == "lost ownership" || scenario == "unsupported provider" {
				if calls != 0 || publication.calls != 0 || !strings.Contains(checkpoint.record.Detail, "No checkpoint epilogue ran") {
					t.Fatalf("unexpected epilogue: calls=%d, publication=%d, record=%+v", calls, publication.calls, checkpoint.record)
				}
			}
			data, err := os.ReadFile(checkpoint.plan.Journal)
			if err != nil {
				t.Fatal(err)
			}
			var durable workspace.CheckpointRecord
			if err := json.Unmarshal(data, &durable); err != nil {
				t.Fatal(err)
			}
			if durable.Published != wantPublished || durable.Status != checkpoint.record.Status {
				t.Fatalf("durable record mismatch: %+v", durable)
			}
		})
	}
}

func TestCheckpointPullRequestAssociation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"existing ready PR", "new draft PR", "multiple PRs", "network failure", "invalid response", "lost lease", "unknown repository", "ownership lost before create"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			_, checkpoint, _ := checkpointRunnerFixture(t)
			calls := 0
			turn := AgentTurnRequest{DeliverableRepository: "example/repo"}
			if scenario == "unknown repository" {
				turn.DeliverableRepository = ""
			}
			if scenario == "lost lease" {
				checkpoint.request.CheckpointValidate = func(context.Context) error { return ErrExecutionAuthorityUnavailable }
			}
			command := func(_ context.Context, _ AgentTurnRequest, args ...string) (string, error) {
				calls++
				if calls == 1 {
					if args[1] != "list" {
						t.Fatalf("expected lookup: %v", args)
					}
					switch scenario {
					case "existing ready PR":
						return `[{"url":"https://example/pr/1"}]`, nil
					case "multiple PRs":
						return `[{},{}]`, nil
					case "invalid response":
						return `{`, nil
					case "network failure":
						return "", errors.New("network")
					case "ownership lost before create":
						checkpoint.request.CheckpointValidate = func(context.Context) error { return ErrExecutionAuthorityUnavailable }
					}
					return `[]`, nil
				}
				if args[1] != "create" || !strings.Contains(strings.Join(args, " "), "--draft") {
					t.Fatalf("expected draft creation: %v", args)
				}
				return "https://example/pr/2", nil
			}
			url, err := checkpointPullRequest(t.Context(), checkpoint, turn, command)
			wantOK := scenario == "existing ready PR" || scenario == "new draft PR"
			if wantOK != (err == nil) {
				t.Fatalf("PR = %s, %v", url, err)
			}
			if scenario == "existing ready PR" && calls != 1 {
				t.Fatal("existing PR changed")
			}
			if scenario == "new draft PR" && calls != 2 {
				t.Fatal("draft was not associated")
			}
		})
	}
}

func TestCheckpointBoundedPeriodicAndTerminalTurns(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"duration", "no progress", "cancelled", "hard reap failure", "periodic", "checkpoint timeout", "duration during checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			runner, checkpoint, publication := checkpointRunnerFixture(t)
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				session := parent
				cfg := config.Agent{}
				if scenario == "periodic" || scenario == "duration during checkpoint" {
					cfg.CheckpointIntervalMS = 1000
				}
				if scenario == "duration during checkpoint" {
					var stop context.CancelFunc
					session, stop = context.WithTimeoutCause(parent, 2*time.Second, ErrSessionDurationExceeded)
					defer stop()
				}
				calls, handshakes := 0, 0
				checkpoint.fingerprint = func(context.Context) (string, error) { return "identical-state", nil }
				run := func(ctx context.Context, turn AgentTurnRequest, req RunRequest) agentTurnExecution {
					if turn.ReadOnly {
						handshakes++
						if scenario == "checkpoint timeout" {
							<-ctx.Done()
							return agentTurnExecution{err: ctx.Err()}
						}
						if scenario == "duration during checkpoint" {
							time.Sleep(2 * time.Second)
						}
						submitCheckpoint(t, ctx, req)
						return agentTurnExecution{}
					}
					calls++
					switch scenario {
					case "duration", "checkpoint timeout":
						return agentTurnExecution{err: ErrSessionDurationExceeded, result: RunResult{FinalState: FinalStateSessionDurationExceeded}}
					case "no progress":
						return agentTurnExecution{err: ErrSessionNoProgress}
					case "cancelled":
						cancel()
						return agentTurnExecution{err: context.Canceled}
					case "hard reap failure":
						return agentTurnExecution{err: errors.Join(ErrSessionDurationExceeded, ErrWorkerProcessReap)}
					case "periodic":
						if calls == 3 {
							return agentTurnExecution{result: RunResult{FinalState: FinalStateCompleted}}
						}
					}
					<-ctx.Done()
					return agentTurnExecution{err: context.Cause(ctx)}
				}
				result := runner.runCheckpointedTurn(parent, session, checkpoint, cfg, checkpointAgentStub{}, AgentTurnRequest{}, checkpoint.request, run)
				if scenario == "periodic" {
					if result.err != nil || calls != 3 || handshakes != 1 || publication.calls != 1 {
						t.Fatalf("repeated checkpoint made progress: calls=%d handshake=%d publication=%d result=%+v", calls, handshakes, publication.calls, result)
					}
				} else if result.err == nil {
					t.Fatal("checkpoint converted failure into completion")
				}
				if scenario == "cancelled" || scenario == "hard reap failure" {
					if handshakes != 0 || publication.calls != 0 {
						t.Fatal("unreachable epilogue ran")
					}
				}
				if scenario == "checkpoint timeout" && publication.calls != 0 {
					t.Fatal("timed out checkpoint published")
				}
				if scenario == "duration during checkpoint" && !errors.Is(result.err, ErrSessionDurationExceeded) {
					t.Fatalf("session deadline extended: %v", result.err)
				}
			})
		})
	}
}

func TestCheckpointPublicationCredentialIsolation(t *testing.T) {
	t.Parallel()
	for _, unavailable := range []bool{false, true} {
		t.Run(strconv.FormatBool(unavailable), func(t *testing.T) {
			t.Parallel()
			r, checkpoint, publication := checkpointRunnerFixture(t)
			turn := AgentTurnRequest{workerGitHub: workerGitHubPolicy{Token: "selected-credential"}}
			run := func(ctx context.Context, _ AgentTurnRequest, req RunRequest) agentTurnExecution {
				submitCheckpoint(t, ctx, req)
				if unavailable {
					if err := os.WriteFile(filepath.Join(checkpoint.plan.Info.Path, ".detent"), []byte("unavailable scratch"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				return agentTurnExecution{}
			}
			r.workerCheckpoint(t.Context(), checkpoint, "duration", checkpointAgentStub{}, turn, run)
			if unavailable {
				if publication.calls != 0 || checkpoint.record.Published {
					t.Fatal("published without credential isolation")
				}
				return
			}
			if publication.environment.Variables["GH_TOKEN"] != "selected-credential" || publication.environment.Variables["GH_CONFIG_DIR"] == "" {
				t.Fatal("publication did not use isolated selected credentials")
			}
			if _, err := os.Stat(publication.environment.Variables["GH_CONFIG_DIR"]); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("credential environment retained: %v", err)
			}
		})
	}
}

func TestCheckpointResumePreservesTokenCeiling(t *testing.T) {
	t.Parallel()
	for _, offset := range []int64{0, 50} {
		t.Run(strconv.FormatInt(offset, 10), func(t *testing.T) {
			t.Parallel()
			r, _, _ := checkpointRunnerFixture(t)
			backend := &toolUpdateAgentBackend{updates: []AgentUpdate{{Type: AgentUpdateTokenUsage, Tokens: AgentTokenUsage{TotalTokens: 60}}}}
			execution := r.runAgentTurn(t.Context(), backend, AgentTurnRequest{}, RunRequest{sessionTokenOffset: offset}, workspace.Info{}, workspace.Issue{}, config.Agent{MaxSessionTokens: 100}, "", nil, time.Now(), 0, agentidentity.Identity{}, nil, 0, "", "")
			var ceiling *SessionTokenCeilingError
			if errors.As(execution.err, &ceiling) != (offset > 0) {
				t.Fatalf("ceiling reset on resume: %v", execution.err)
			}
			if execution.result.Tokens.TotalTokens != 60 {
				t.Fatalf("offset double-counted usage: %+v", execution.result.Tokens)
			}
		})
	}
}

type preparingCheckpointBackend struct {
	*fakeWorkspaceBackend
	checkpointBackendStub
	plan       workspace.CheckpointPlan
	prepareErr error
}

func (b *preparingCheckpointBackend) PrepareCheckpoint(context.Context, workspace.Info, workspace.Issue) (workspace.CheckpointPlan, error) {
	return b.plan, b.prepareErr
}

func TestPrepareWorkerCheckpoint(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"prepared", "unsupported", "routine", "unavailable", "journal unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			r, cp, _ := checkpointRunnerFixture(t)
			base := &preparingCheckpointBackend{fakeWorkspaceBackend: &fakeWorkspaceBackend{recoveryStates: []workspace.RecoveryState{{WorkspaceFingerprint: "state"}}}, plan: cp.plan}
			var backend workspace.Backend = base
			req := cp.request
			switch scenario {
			case "unsupported":
				backend = base.fakeWorkspaceBackend
			case "routine":
				req.Mode = RunModeRoutine
			case "unavailable", "journal unavailable":
				base.prepareErr = errors.New("preparation unavailable")
			}
			if scenario == "journal unavailable" {
				base.plan.Journal = ""
			}
			got := r.prepareWorkerCheckpoint(t.Context(), req, backend, cp.plan.Info, workspace.Issue{})
			if scenario == "prepared" {
				if got == nil {
					t.Fatal("checkpoint unavailable")
				}
				fingerprint, err := got.fingerprint(t.Context())
				if err != nil || fingerprint != "state" {
					t.Fatalf("fingerprint = %s, %v", fingerprint, err)
				}
			} else if scenario == "unavailable" {
				if got == nil || !got.prepareFailed {
					t.Fatal("preparation failure lost its recovery state")
				}
				execution := r.runCheckpointedTurn(t.Context(), t.Context(), got, config.Agent{CheckpointIntervalMS: 1}, checkpointAgentStub{}, AgentTurnRequest{}, req, func(ctx context.Context, turn AgentTurnRequest, _ RunRequest) agentTurnExecution {
					if turn.ReadOnly {
						t.Fatal("unavailable preparation launched a checkpoint worker")
					}
					if _, bounded := ctx.Deadline(); bounded {
						t.Fatal("unavailable preparation interrupted work for a periodic checkpoint")
					}
					return agentTurnExecution{err: ErrSessionDurationExceeded}
				})
				if execution.result.Checkpoint == nil || execution.result.Checkpoint.Published || !strings.Contains(execution.result.Checkpoint.Detail, "No checkpoint epilogue ran") {
					t.Fatalf("missing terminal recovery state: %+v", execution.result.Checkpoint)
				}
			} else if got != nil {
				t.Fatal("unexpected checkpoint")
			}
			if scenario == "unavailable" {
				data, err := os.ReadFile(cp.plan.Journal)
				if err != nil || !strings.Contains(string(data), "local_only") {
					t.Fatalf("missing preparation recovery: %s, %v", data, err)
				}
			}
		})
	}
}

func TestCheckpointAuthorityAndJournalFailures(t *testing.T) {
	t.Parallel()
	r, cp, _ := checkpointRunnerFixture(t)
	cp.request.CheckpointValidate = nil
	if !errors.Is(cp.validate(t.Context()), ErrExecutionAuthorityUnavailable) {
		t.Fatal("missing authority accepted")
	}
	cp.request.Execution = &testExecution{validateErr: context.Canceled}
	if !errors.Is(cp.validate(t.Context()), context.Canceled) {
		t.Fatal("native authority ignored")
	}
	cp.plan.Journal = ""
	r.saveWorkerCheckpoint(cp, workspace.CheckpointRecord{Status: "local_only"})
	if cp.record == nil || cp.record.Status != "local_only" {
		t.Fatal("journal failure erased in-memory recovery")
	}
}

func TestCheckpointCommandCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := checkpointCommand(ctx, AgentTurnRequest{Workspace: t.TempDir()}, "--version")
	if err == nil {
		t.Fatal("cancelled command succeeded")
	}
}

func TestCheckpointRetentionSkipsAfterRun(t *testing.T) {
	t.Parallel()
	for _, native := range []bool{false, true} {
		for _, retain := range []bool{false, true} {
			t.Run(strconv.FormatBool(native)+"/"+strconv.FormatBool(retain), func(t *testing.T) {
				t.Parallel()
				r, _, _ := checkpointRunnerFixture(t)
				r.afterRunTimeout = time.Minute
				backend := &fakeWorkspaceBackend{}
				req := RunRequest{retainCheckpoint: retain}
				execution := &testExecution{}
				if native {
					req.Execution = execution
				}
				if err := r.afterExecution(t.Context(), req, backend, workspace.Info{}, workspace.Issue{}); err != nil {
					t.Fatal(err)
				}
				if backend.afterRun == retain {
					t.Fatalf("retained checkpoint cleanup = %v", backend.afterRun)
				}
				if native && execution.checkpoint == nil {
					t.Fatal("native recovery evidence missing")
				}
			})
		}
	}
}

func TestPeriodicCheckpointPreservesRecoveryUsageOffset(t *testing.T) {
	t.Parallel()
	r, cp, _ := checkpointRunnerFixture(t)
	synctest.Test(t, func(t *testing.T) {
		req := cp.request
		req.sessionTokenOffset = 40
		calls := 0
		run := func(ctx context.Context, turn AgentTurnRequest, req RunRequest) agentTurnExecution {
			if turn.ReadOnly {
				if req.sessionTokenOffset != 50 {
					t.Fatalf("handshake offset = %d", req.sessionTokenOffset)
				}
				submitCheckpoint(t, ctx, req)
				return agentTurnExecution{result: RunResult{Tokens: TokenTotals{TotalTokens: 5}}}
			}
			calls++
			if calls == 1 {
				if req.sessionTokenOffset != 40 {
					t.Fatalf("initial offset = %d", req.sessionTokenOffset)
				}
				<-ctx.Done()
				return agentTurnExecution{err: context.Cause(ctx), result: RunResult{Tokens: TokenTotals{TotalTokens: 10}}}
			}
			if req.sessionTokenOffset != 55 {
				t.Fatalf("resume offset = %d", req.sessionTokenOffset)
			}
			return agentTurnExecution{result: RunResult{FinalState: FinalStateCompleted, Tokens: TokenTotals{TotalTokens: 10}}}
		}
		result := r.runCheckpointedTurn(t.Context(), t.Context(), cp, config.Agent{CheckpointIntervalMS: 1000}, checkpointAgentStub{}, AgentTurnRequest{}, req, run)
		if result.err != nil || result.result.Tokens.TotalTokens != 25 {
			t.Fatalf("recovery usage = %+v, %v", result.result.Tokens, result.err)
		}
	})
}

func TestTerminalCheckpointRetriesUnavailablePeriodicPublication(t *testing.T) {
	t.Parallel()
	r, cp, publication := checkpointRunnerFixture(t)
	cp.fingerprint = func(context.Context) (string, error) { return "same-work", nil }
	publication.err = errors.New("network unavailable")
	handshakes := 0
	run := func(ctx context.Context, _ AgentTurnRequest, req RunRequest) agentTurnExecution {
		handshakes++
		submitCheckpoint(t, ctx, req)
		return agentTurnExecution{}
	}
	r.workerCheckpoint(t.Context(), cp, "periodic", checkpointAgentStub{}, AgentTurnRequest{}, run)
	publication.err = nil
	r.workerCheckpoint(t.Context(), cp, "periodic", checkpointAgentStub{}, AgentTurnRequest{}, run)
	if handshakes != 1 || publication.calls != 1 {
		t.Fatal("identical periodic work retried")
	}
	r.workerCheckpoint(t.Context(), cp, SessionBrakeReasonDuration, checkpointAgentStub{}, AgentTurnRequest{}, run)
	if handshakes != 2 || publication.calls != 2 || !cp.record.Published {
		t.Fatal("terminal checkpoint did not retry restored delivery")
	}
	r.workerCheckpoint(t.Context(), cp, SessionBrakeReasonDuration, checkpointAgentStub{}, AgentTurnRequest{}, run)
	if handshakes != 2 {
		t.Fatal("published work repeated its checkpoint")
	}
}

func TestUnsupportedCheckpointProviderDoesNotInterruptPeriodically(t *testing.T) {
	t.Parallel()
	r, cp, publication := checkpointRunnerFixture(t)
	run := func(ctx context.Context, turn AgentTurnRequest, _ RunRequest) agentTurnExecution {
		if turn.ReadOnly {
			t.Fatal("unsupported provider launched checkpoint turn")
		}
		if _, bounded := ctx.Deadline(); bounded {
			t.Fatal("unsupported provider received a periodic interruption")
		}
		return agentTurnExecution{err: ErrSessionDurationExceeded}
	}
	execution := r.runCheckpointedTurn(t.Context(), t.Context(), cp, config.Agent{CheckpointIntervalMS: 1}, nonVerifyingAgentBackend{}, AgentTurnRequest{}, cp.request, run)
	if execution.result.Checkpoint == nil || publication.calls != 0 || !strings.Contains(execution.result.Checkpoint.Detail, "does not support") {
		t.Fatalf("provider recovery = %+v", execution.result.Checkpoint)
	}
}
