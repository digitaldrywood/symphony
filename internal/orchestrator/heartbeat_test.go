package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestHeartbeatManagerRenewsWithoutSchedulerLoop(t *testing.T) {
	initial := time.Now().UTC().Add(-time.Minute)
	issue := claimTestIssue("issue-dedicated-heartbeat")
	issue.AssigneeID = "alpha"
	issue.Assignees = []string{"alpha"}
	issue.Fields["Detent Lease"] = formatClaimTime(initial)
	claimStore := newClaimTestStore([]connector.Issue{issue})
	attempts := &recordingWorkAttemptStore{}
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))
	cfg.Claiming.HeartbeatInterval = 5 * time.Millisecond
	manager := newHeartbeatManager(cfg, claimTestConnector{store: claimStore, login: "alpha"}, attempts, time.Now, nil)

	result := runHeartbeatManagerOnce(t, manager, heartbeatTarget{
		issueID:              issue.ID,
		claimOwner:           "alpha",
		workAttemptHeartbeat: store.WorkAttemptHeartbeat{AttemptID: 1433},
		workspacePath:        t.TempDir(),
	})

	if !result.workAttemptRenewed || !result.claimRenewed {
		t.Fatalf("heartbeat result = %#v, want durable and tracker renewal", result)
	}
	if len(attempts.heartbeats) != 1 || attempts.heartbeats[0].AttemptID != 1433 {
		t.Fatalf("durable heartbeats = %#v, want attempt 1433", attempts.heartbeats)
	}
	lease, ok := parseClaimTime(claimStore.issue(issue.ID).Fields["Detent Lease"])
	if !ok || !lease.After(initial) {
		t.Fatalf("tracker lease = %v, %v, want after %v", lease, ok, initial)
	}
}

func TestCompleteTerminalRunningClearsInFlightHeartbeatLease(t *testing.T) {
	now := time.Now().UTC()
	issue := claimTestIssue("issue-terminal-heartbeat")
	issue.State = "Done"
	issue.AssigneeID = "alpha"
	issue.Assignees = []string{"alpha"}
	issue.Fields["Detent Lease"] = formatClaimTime(now.Add(-time.Minute))
	claimStore := newClaimTestStore([]connector.Issue{issue})
	connectorBackend := &terminalHeartbeatConnector{
		claimTestConnector: claimTestConnector{store: claimStore, login: "alpha"},
		renewalStarted:     make(chan struct{}),
		allowRenewal:       make(chan struct{}),
	}
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))
	cfg.Claiming.HeartbeatInterval = 5 * time.Millisecond
	manager := newHeartbeatManager(cfg, connectorBackend, nil, time.Now, nil)
	orchestrator := &Orchestrator{cfg: cfg, connector: connectorBackend, heartbeats: manager}
	state := newState(cfg)
	running := Running{Issue: cloneIssue(issue), StartedAt: now.Add(-time.Minute)}
	state.Running[issue.ID] = running
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), Owner: "alpha"}
	manager.upsert(heartbeatTarget{issueID: issue.ID, claimOwner: "alpha"})

	ctx, cancel := context.WithCancel(t.Context())
	managerDone := make(chan struct{})
	go func() {
		defer close(managerDone)
		manager.Run(ctx)
	}()
	select {
	case <-connectorBackend.renewalStarted:
	case <-time.After(time.Second):
		cancel()
		<-managerDone
		t.Fatal("timed out waiting for claim heartbeat renewal")
	}

	completionDone := make(chan struct{})
	go func() {
		defer close(completionDone)
		orchestrator.completeTerminalRunning(t.Context(), &state, issue.ID, running, now, TokenTotals{})
	}()
	completedBeforeRenewal := false
	select {
	case <-completionDone:
		completedBeforeRenewal = true
	case <-time.After(20 * time.Millisecond):
	}
	close(connectorBackend.allowRenewal)
	select {
	case <-completionDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal completion")
	}
	cancel()
	<-managerDone

	if completedBeforeRenewal {
		t.Fatal("terminal completion returned before the in-flight heartbeat finished")
	}
	if got := claimStore.issue(issue.ID).Fields["Detent Lease"]; got != "" {
		t.Fatalf("Detent Lease = %q, want cleared after terminal completion", got)
	}
	manager.mu.Lock()
	_, tracked := manager.targets[issue.ID]
	manager.mu.Unlock()
	if tracked {
		t.Fatalf("heartbeat target %q remains after terminal completion", issue.ID)
	}
}

func TestHeartbeatManagerRequiresMatchingLiveProcessIdentity(t *testing.T) {
	identity := startHeartbeatWorkerProcess(t)

	tests := []struct {
		name        string
		identity    procgroup.Identity
		wantRenewed bool
		wantChecked bool
		wantAlive   bool
	}{
		{name: "startup before process identity", wantRenewed: true},
		{name: "matching live process", identity: identity, wantRenewed: true, wantChecked: true, wantAlive: true},
		{name: "reused pid identity", identity: procgroup.Identity{PID: identity.PID, GroupID: identity.GroupID, StartedAt: identity.StartedAt.Add(time.Second)}, wantChecked: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := &recordingWorkAttemptStore{}
			cfg := normalizeConfig(Config{Claiming: ClaimingConfig{LeaseTTL: time.Minute, HeartbeatInterval: 5 * time.Millisecond}})
			now := time.Now()
			manager := newHeartbeatManager(cfg, nil, attempts, func() time.Time { return now }, nil)
			manager.upsert(heartbeatTarget{
				issueID:              tt.name,
				workAttemptHeartbeat: store.WorkAttemptHeartbeat{AttemptID: 1433},
				workerProcess:        tt.identity,
			})
			due := manager.due(now.Add(cfg.Claiming.HeartbeatInterval))
			if len(due) != 1 {
				t.Fatalf("due heartbeat count = %d, want 1", len(due))
			}
			manager.execute(t.Context(), due[0])
			result := <-manager.results

			if result.workAttemptRenewed != tt.wantRenewed || result.workerChecked != tt.wantChecked || result.workerAlive != tt.wantAlive {
				t.Fatalf("heartbeat result = %#v, want renewed=%v checked=%v alive=%v", result, tt.wantRenewed, tt.wantChecked, tt.wantAlive)
			}
			if got := len(attempts.heartbeats); got != boolCount(tt.wantRenewed) {
				t.Fatalf("durable heartbeat count = %d, want %d", got, boolCount(tt.wantRenewed))
			}
		})
	}
}

func TestLatestHeartbeatWorkspaceModificationIncludesWorkerScratch(t *testing.T) {
	workspacePath := t.TempDir()
	scratchPath := filepath.Join(workspacePath, ".detent", "tmp")
	if err := os.MkdirAll(scratchPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	latest := base.Add(30 * time.Second)
	for _, item := range []struct {
		path string
		at   time.Time
	}{
		{path: workspacePath, at: base},
		{path: filepath.Join(workspacePath, ".detent"), at: base.Add(10 * time.Second)},
		{path: scratchPath, at: latest},
	} {
		if err := os.Chtimes(item.path, item.at, item.at); err != nil {
			t.Fatalf("Chtimes(%s) error = %v", item.path, err)
		}
	}

	got, err := latestHeartbeatWorkspaceModification(workspacePath)
	if err != nil {
		t.Fatalf("latestHeartbeatWorkspaceModification() error = %v", err)
	}
	if !got.Equal(latest) {
		t.Fatalf("latest modification = %v, want %v", got, latest)
	}
}

func TestHeartbeatWorkerProcessHelper(t *testing.T) {
	if os.Getenv("DETENT_HEARTBEAT_PROCESS_HELPER") != "1" {
		return
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestWorkHeartbeatInterval(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want time.Duration
	}{
		{name: "configured interval", cfg: Config{Claiming: ClaimingConfig{LeaseTTL: 10 * time.Minute, HeartbeatInterval: time.Minute}}, want: time.Minute},
		{name: "lease midpoint clamped", cfg: Config{Claiming: ClaimingConfig{LeaseTTL: time.Minute}}, want: 20 * time.Second},
		{name: "default lease", want: defaultWorkAttemptLeaseTTL / 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workHeartbeatInterval(tt.cfg); got != tt.want {
				t.Fatalf("workHeartbeatInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClaimableChecksLocalWorkerLivenessBeforeReclaim(t *testing.T) {
	now := time.Now().UTC()
	issue := claimTestIssue("issue-reclaim-guard")
	issue.AssigneeID = "beta"
	issue.Assignees = []string{"beta"}
	issue.Fields["Detent Lease"] = formatClaimTime(now.Add(-2 * time.Minute))
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))

	tests := []struct {
		name        string
		workerAlive bool
		want        bool
	}{
		{name: "matching live worker blocks reclaim", workerAlive: true},
		{name: "dead worker permits reclaim", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newHeartbeatManager(cfg, nil, nil, time.Now, nil)
			manager.upsert(heartbeatTarget{issueID: issue.ID, workAttemptHeartbeat: store.WorkAttemptHeartbeat{AttemptID: 1433}})
			manager.mu.Lock()
			target := manager.targets[issue.ID]
			target.workerChecked = true
			target.workerAlive = tt.workerAlive
			manager.targets[issue.ID] = target
			manager.mu.Unlock()
			orchestrator := &Orchestrator{cfg: cfg, heartbeats: manager}

			if got := orchestrator.claimable(t.Context(), issue, "alpha", now); got != tt.want {
				t.Fatalf("claimable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClaimableChecksPersistedProcessIdentityBeforeReclaim(t *testing.T) {
	now := time.Now().UTC()
	identity := startHeartbeatWorkerProcess(t)
	issue := claimTestIssue("issue-persisted-reclaim-guard")
	issue.AssigneeID = "beta"
	issue.Assignees = []string{"beta"}
	issue.Fields["Detent Lease"] = formatClaimTime(now.Add(-2 * time.Minute))
	cfg := normalizeConfig(claimTestConfig("alpha", "alpha"))

	tests := []struct {
		name     string
		identity procgroup.Identity
		want     bool
	}{
		{name: "matching persisted identity blocks reclaim", identity: identity},
		{name: "stale persisted identity permits reclaim", identity: procgroup.Identity{PID: identity.PID, GroupID: identity.GroupID, StartedAt: identity.StartedAt.Add(time.Second)}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := &heartbeatProcessStore{
				recordingWorkAttemptStore: &recordingWorkAttemptStore{},
				processes: []store.WorkerProcess{{
					SessionID:  1433,
					IssueID:    issue.ID,
					Identifier: issue.Identifier,
					WorkerProcessIdentity: store.WorkerProcessIdentity{
						PID:       tt.identity.PID,
						GroupID:   tt.identity.GroupID,
						StartedAt: tt.identity.StartedAt,
					},
				}},
			}
			orchestrator := &Orchestrator{cfg: cfg, workAttempts: attempts}
			if got := orchestrator.claimable(t.Context(), issue, "alpha", now); got != tt.want {
				t.Fatalf("claimable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func runHeartbeatManagerOnce(t *testing.T, manager *heartbeatManager, target heartbeatTarget) heartbeatResult {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.Run(ctx)
	}()
	manager.upsert(target)
	select {
	case result := <-manager.results:
		manager.remove(target.issueID)
		cancel()
		<-done
		return result
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("timed out waiting for dedicated heartbeat")
		return heartbeatResult{}
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func startHeartbeatWorkerProcess(t *testing.T) procgroup.Identity {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestHeartbeatWorkerProcessHelper$")
	cmd.Env = append(os.Environ(), "DETENT_HEARTBEAT_PROCESS_HELPER=1")
	procgroup.Configure(t.Context(), cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	identity, err := procgroup.Inspect(cmd)
	if err != nil {
		_ = procgroup.TerminateTree(cmd, procgroup.GroupID(cmd))
		_ = cmd.Wait()
		t.Fatalf("Inspect() error = %v", err)
	}
	t.Cleanup(func() {
		_ = procgroup.TerminateTree(cmd, identity.GroupID)
		_ = cmd.Wait()
	})
	return identity
}

type terminalHeartbeatConnector struct {
	claimTestConnector
	renewalStarted chan struct{}
	allowRenewal   chan struct{}
	renewalOnce    sync.Once
}

func (c *terminalHeartbeatConnector) SetField(ctx context.Context, issueID string, fieldName string, value string) error {
	if value != "" {
		c.renewalOnce.Do(func() {
			close(c.renewalStarted)
		})
		select {
		case <-c.allowRenewal:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.claimTestConnector.SetField(ctx, issueID, fieldName, value)
}

type heartbeatProcessStore struct {
	*recordingWorkAttemptStore
	processes []store.WorkerProcess
}

func (s *heartbeatProcessStore) ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error) {
	return append([]store.WorkerProcess(nil), s.processes...), nil
}
