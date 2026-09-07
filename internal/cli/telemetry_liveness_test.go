package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitaldrywood/detent/internal/boardsnapshot"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/hub"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/tui"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestTelemetryPublicationSurvivesStalledSourceAfterRestart(t *testing.T) {
	for _, source := range []string{"project_state", "lifetime_totals"} {
		for _, cached := range []bool{false, true} {
			name := source + "/initializing"
			if cached {
				name = source + "/cached_draining"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					snapshots := hub.New[telemetry.Snapshot]()
					registry := project.NewRegistry()
					var requests []orchestrator.RunRequest
					for _, id := range []string{"beta", "gamma"} {
						tracker := memory.New(memory.Config{Issues: []connector.Issue{{ID: id, Identifier: id + "#1", Title: id + " active worker", State: "In Progress", AssignedToWorker: true}}})
						runner := newRestartRecoveryRunner()
						tracked := newTelemetryProject(t, id, tracker, runner)
						if err := tracked.Start(ctx); err != nil {
							t.Fatal(err)
						}
						mustSetProject(t, registry, tracked)
						select {
						case request := <-runner.started:
							requests = append(requests, request)
						case <-time.After(10 * time.Second):
							state, err := tracked.Orchestrator().State(ctx)
							t.Fatalf("worker did not start: dispatch = %#v, error = %v", state.DispatchStatus, err)
						}
					}
					var totals lifetimeTotalsSource
					if source == "project_state" {
						tracker := stalledTelemetryConnector{Connector: memory.New(memory.Config{})}
						tracked := newTelemetryProject(t, "alpha", tracker, nil)
						if err := tracked.Start(ctx); err != nil {
							t.Fatal(err)
						}
						mustSetProject(t, registry, tracked)
					} else {
						totals = stalledTelemetryTotals{}
					}
					if err := publishStartupSnapshotOnce(ctx, globalconfig.Config{}, snapshots, nil, "", time.Now()); err != nil {
						t.Fatal(err)
					}
					if cached {
						seedRestartTelemetry(t, ctx, snapshots)
					}
					initial, _ := snapshots.Latest()
					var previousSeq uint64
					if initial.Shutdown.Draining || initial.Shutdown.Status != "running" {
						t.Fatalf("startup retained prior shutdown state: %#v", initial.Shutdown)
					}
					server := newTelemetryWebServer(t, snapshots, registry)
					events := httptest.NewRecorder()
					eventsDone := make(chan struct{})
					go func() {
						defer close(eventsDone)
						server.Handler().ServeHTTP(events, httptest.NewRequestWithContext(ctx, http.MethodGet, "/events?view=board", nil))
					}()
					defer func() {
						cancel()
						<-eventsDone
					}()
					synctest.Wait()
					frames := strings.Count(events.Body.String(), "event: snapshot\n")
					if events.Code != http.StatusOK || frames != 1 {
						t.Fatalf("startup SSE status = %d, snapshot frames = %d", events.Code, frames)
					}
					model, err := tui.NewModel(ctx, snapshots)
					if err != nil {
						t.Fatal(err)
					}
					defer model.Close()
					terminal, next := model.Update(model.Init()())
					var seq atomic.Uint64
					done := make(chan struct{})
					go func() {
						defer close(done)
						publishSnapshots(ctx, registry, nil, snapshots, &seq, nil, totals, "", nil, time.Second, time.Now)
					}()
					defer func() {
						cancel()
						<-done
					}()
					for index := range 2 {
						for _, request := range requests {
							if err := request.OnUsageUpdate(orchestrator.UsageUpdate{LastEventAt: time.Now(), LastMessage: "worker progress", Tokens: orchestrator.TokenTotals{TotalTokens: int64(index+1) * 100}}); err != nil {
								t.Fatal(err)
							}
						}
						time.Sleep(2 * time.Second)
						synctest.Wait()
						current, _ := snapshots.Latest()
						if !current.GeneratedAt.After(initial.GeneratedAt) || current.Seq <= previousSeq {
							t.Fatalf("publication froze at %v (seq %d) while two workers run", current.GeneratedAt, current.Seq)
						}
						if len(current.Running) != 2 || current.Shutdown.Draining || current.Shutdown.Status != "running" {
							t.Fatalf("running = %d, shutdown = %#v", len(current.Running), current.Shutdown)
						}
						for _, running := range current.Running {
							if running.Tokens.Total != int64(index+1)*100 {
								t.Fatalf("worker progress froze: %#v", running.Tokens)
							}
						}
						healthResponse := httptest.NewRecorder()
						server.Handler().ServeHTTP(healthResponse, httptest.NewRequestWithContext(ctx, http.MethodGet, "/health", nil))
						var health struct {
							GeneratedAt time.Time `json:"snapshot_generated_at"`
							AgeSeconds  int64     `json:"snapshot_age_seconds"`
						}
						if err := json.Unmarshal(healthResponse.Body.Bytes(), &health); err != nil {
							t.Fatal(err)
						}
						if !health.GeneratedAt.Equal(current.GeneratedAt) || health.AgeSeconds > 1 {
							t.Fatalf("health freshness = %#v, snapshot generated at %v", health, current.GeneratedAt)
						}
						if got := strings.Count(events.Body.String(), "event: snapshot\n"); got <= frames {
							t.Fatalf("SSE froze at %d snapshot frames", got)
						} else {
							frames = got
						}
						if !strings.Contains(events.Body.String(), "beta active worker") || !strings.Contains(events.Body.String(), "gamma active worker") {
							t.Fatal("SSE did not include current running workers")
						}
						if source == "lifetime_totals" && (current.LifetimeTotals.Available || !strings.Contains(current.LifetimeTotals.DegradedReason, "deadline")) {
							t.Fatalf("lifetime totals = %#v, want unavailable with deadline diagnostic", current.LifetimeTotals)
						}
						if source == "project_state" {
							alpha := current.Projects[0]
							if alpha.Runtime.Source != telemetry.SnapshotSourceUnknown || !strings.Contains(alpha.Refresh.LastError, "deadline") {
								t.Fatalf("stalled project = %#v, want unknown with deadline diagnostic", alpha)
							}
							if cached && alpha.Tracker.Source != telemetry.SnapshotSourceCached {
								t.Fatal("stalled project lost cached tracker provenance")
							}
						}
						terminal, next = terminal.Update(next())
						view := terminal.View().Content
						if !strings.Contains(view, "beta#1") || !strings.Contains(view, "gamma#1") || strings.Contains(view, "draining sessions") {
							t.Fatalf("terminal did not advance to live workers: %s", view)
						}
						initial = current
						previousSeq = current.Seq
					}
				})
			})
		}
	}
}

func TestReadTelemetrySource(t *testing.T) {
	sourceErr := errors.New("source unavailable")
	for _, tt := range []struct {
		name         string
		cancelParent bool
		stall        bool
		readErr      error
		wantErr      error
	}{
		{name: "success"},
		{name: "source error", readErr: sourceErr, wantErr: sourceErr},
		{name: "deadline", stall: true, wantErr: context.DeadlineExceeded},
		{name: "service cancellation", cancelParent: true, stall: true, wantErr: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tt.cancelParent {
					cancel()
				}
				var sourceContextErr func() error
				value, err := readTelemetrySource(parent, "example", "alpha", func(ctx context.Context) (int, error) {
					sourceContextErr = ctx.Err
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) != defaultTelemetryReadTimeout {
						t.Fatalf("source deadline = %v, %v", deadline, ok)
					}
					if tt.stall {
						<-ctx.Done()
						return 0, ctx.Err()
					}
					return 42, tt.readErr
				})
				if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && value != 42) {
					t.Fatalf("read = %d, %v; want error %v", value, err, tt.wantErr)
				}
				if sourceContextErr() == nil || (parent.Err() != nil && !tt.cancelParent) {
					t.Fatal("source context was not released independently of service context")
				}
				if err != nil && !strings.Contains(err.Error(), "telemetry source example:") {
					t.Fatalf("error lacks source identity: %v", err)
				}
			})
		})
	}
}

func newTelemetryProject(t *testing.T, id string, tracker connector.Connector, runner orchestrator.Runner) *project.Project {
	t.Helper()
	cfg := workflowconfig.Default()
	cfg.Tracker.Kind = workflowconfig.TrackerMemory
	tracked, err := project.New(project.Config{
		Project:  globalconfig.Project{ID: id, Workdir: t.TempDir(), Weight: 1},
		Workflow: workflowconfig.Workflow{Config: cfg, Prompt: "Telemetry fixture"},
	}, project.Dependencies{
		Connector: tracker,
		Runner:    runner,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		OrchestratorFactory: func(cfg orchestrator.Config, deps orchestrator.Dependencies) (*orchestrator.Orchestrator, error) {
			return orchestrator.New(orchestrator.Config{
				Project: cfg.Project, PollInterval: time.Hour, MaxConcurrentAgents: 1,
				ActiveStates: []string{"In Progress"}, TerminalStates: []string{"Done"},
			}, deps)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tracked.Stop(context.Background()); err != nil && !errors.Is(err, project.ErrNotRunning) {
			t.Error(err)
		}
	})
	return tracked
}

func seedRestartTelemetry(t *testing.T, ctx context.Context, snapshots *hub.Hub[telemetry.Snapshot]) {
	t.Helper()
	cache, err := boardsnapshot.New(boardsnapshot.Config{Path: filepath.Join(t.TempDir(), "snapshot.json"), MaxAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(ctx, telemetry.Snapshot{
		Seq:         900,
		GeneratedAt: time.Now().Add(-time.Second),
		Counts:      telemetry.Counts{Running: 1},
		Running:     []telemetry.Running{{Issue: telemetry.Issue{ID: "previous-process-worker", ProjectID: "alpha"}}},
		Shutdown:    telemetry.Shutdown{Status: "draining", Draining: true},
		Project:     telemetry.Project{ID: "alpha"},
		Projects:    []telemetry.ProjectSnapshot{{Project: telemetry.Project{ID: "alpha"}}},
		BoardIssues: []telemetry.Issue{{ID: "cached", ProjectID: "alpha", Identifier: "alpha#1", State: "Todo"}},
	}); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := cache.Load(ctx)
	if err != nil || !found {
		t.Fatalf("Load() = %v, %v", found, err)
	}
	if loaded.Counts.Running != 0 || len(loaded.Running) != 0 || loaded.Runtime.Source != telemetry.SnapshotSourceUnknown {
		t.Fatalf("cached runtime was not cleared: counts %#v, running %#v, runtime %#v", loaded.Counts, loaded.Running, loaded.Runtime)
	}
	if err := snapshots.Publish(loaded); err != nil {
		t.Fatal(err)
	}
}

type stalledTelemetryTotals struct{}

func newTelemetryWebServer(t *testing.T, snapshots *hub.Hub[telemetry.Snapshot], registry *project.Registry) *web.Server {
	t.Helper()
	db, err := store.Open(t.Context(), store.Config{Path: filepath.Join(t.TempDir(), "runtime.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	server, err := web.NewServer(web.Config{
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		LookupEnv:           func(string) string { return "" },
		SSEFragmentInterval: time.Millisecond,
	}, web.Dependencies{Hub: snapshots, Registry: registry, Store: db, Connector: memory.New(memory.Config{})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	return server
}

func (stalledTelemetryTotals) LifetimeTotals(ctx context.Context) (store.LifetimeTotals, error) {
	<-ctx.Done()
	return store.LifetimeTotals{}, ctx.Err()
}

type stalledTelemetryConnector struct {
	connector.Connector
}

func (stalledTelemetryConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stalledTelemetryConnector) FetchIssuesByStates(ctx context.Context, _ []string) ([]connector.Issue, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
