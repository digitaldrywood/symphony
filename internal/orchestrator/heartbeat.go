package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/store"
)

const heartbeatOperationTimeout = 30 * time.Second

type heartbeatManager struct {
	mu           sync.Mutex
	settings     heartbeatSettings
	targets      map[string]heartbeatTarget
	nextSequence uint64
	wake         chan struct{}
	results      chan heartbeatResult
	running      atomic.Bool
	now          func() time.Time
	logger       *slog.Logger
}

type heartbeatSettings struct {
	connector    connector.Connector
	scheduling   SchedulingSource
	workAttempts store.WorkAttemptStore
	claiming     ClaimingConfig
	interval     time.Duration
	leaseTTL     time.Duration
}

type heartbeatTarget struct {
	progress             *workerProgress
	issueID              string
	claimOwner           string
	workAttemptHeartbeat store.WorkAttemptHeartbeat
	workerProcess        procgroup.Identity
	workspacePath        string
	workspaceModifiedAt  time.Time
	workspaceAdvanced    bool
	workerAlive          bool
	workerChecked        bool
	livenessError        error
	sequence             uint64
	nextDue              time.Time
	inFlight             bool
	flightDone           chan struct{}
}

type heartbeatResult struct {
	issueID             string
	sequence            uint64
	heartbeat           store.WorkAttemptHeartbeat
	workAttemptRenewed  bool
	claimRenewed        bool
	claimIssue          connector.Issue
	claim               Claimed
	claimOwnerLost      bool
	currentClaimOwner   string
	workerAlive         bool
	workerChecked       bool
	workspaceModifiedAt time.Time
	workspaceAdvanced   bool
	livenessError       error
	workAttemptError    error
	claimError          error
}

func newHeartbeatManager(cfg Config, connectorBackend connector.Connector, workAttempts store.WorkAttemptStore, now func() time.Time, logger *slog.Logger, scheduling ...SchedulingSource) *heartbeatManager {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	manager := &heartbeatManager{
		targets: make(map[string]heartbeatTarget),
		wake:    make(chan struct{}, 1),
		results: make(chan heartbeatResult, runUpdateBufferSize),
		now:     now,
		logger:  logger,
	}
	manager.configure(cfg, connectorBackend, workAttempts, scheduling...)
	return manager
}

func (m *heartbeatManager) configure(cfg Config, connectorBackend connector.Connector, workAttempts store.WorkAttemptStore, scheduling ...SchedulingSource) {
	if m == nil {
		return
	}
	settings := heartbeatSettings{
		connector:    connectorBackend,
		workAttempts: workAttempts,
		claiming:     cfg.Claiming,
		interval:     workHeartbeatInterval(cfg),
		leaseTTL:     cfg.Claiming.LeaseTTL,
	}
	if len(scheduling) > 0 {
		settings.scheduling = scheduling[0]
	} else {
		m.mu.Lock()
		settings.scheduling = m.settings.scheduling
		m.mu.Unlock()
	}
	if settings.scheduling != nil {
		interval := settings.scheduling.HeartbeatInterval()
		if interval > 0 && interval < settings.interval {
			settings.interval = interval
		}
	}
	if settings.leaseTTL <= 0 {
		settings.leaseTTL = defaultWorkAttemptLeaseTTL
	}
	m.mu.Lock()
	m.settings = settings
	now := m.now()
	for issueID, target := range m.targets {
		if target.nextDue.IsZero() || target.nextDue.After(now.Add(settings.interval)) {
			target.nextDue = now.Add(settings.interval)
			m.targets[issueID] = target
		}
	}
	m.mu.Unlock()
	m.notify()
}

func workHeartbeatInterval(cfg Config) time.Duration {
	interval := cfg.Claiming.HeartbeatInterval
	if interval <= 0 && cfg.Claiming.LeaseTTL > 0 {
		interval = cfg.Claiming.LeaseTTL / 2
	}
	leaseTTL := cfg.Claiming.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultWorkAttemptLeaseTTL
	}
	if maximum := leaseTTL / 3; maximum > 0 && (interval <= 0 || interval > maximum) {
		interval = maximum
	}
	if interval <= 0 {
		interval = defaultPollInterval
	}
	return interval
}

func (m *heartbeatManager) upsert(target heartbeatTarget) {
	if m == nil || strings.TrimSpace(target.issueID) == "" {
		return
	}
	m.mu.Lock()
	current, ok := m.targets[target.issueID]
	if ok && current.workAttemptHeartbeat.AttemptID == target.workAttemptHeartbeat.AttemptID && current.progress == target.progress {
		target.sequence = current.sequence
		target.nextDue = current.nextDue
		target.inFlight = current.inFlight
		target.flightDone = current.flightDone
		target.workspaceModifiedAt = current.workspaceModifiedAt
		target.workspaceAdvanced = current.workspaceAdvanced
		target.workerAlive = current.workerAlive
		target.workerChecked = current.workerChecked
		target.livenessError = current.livenessError
	} else {
		m.nextSequence++
		target.sequence = m.nextSequence
		target.nextDue = m.now().Add(m.settings.interval)
	}
	m.targets[target.issueID] = target
	m.mu.Unlock()
	m.notify()
}

func (m *heartbeatManager) remove(issueID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	target := m.targets[strings.TrimSpace(issueID)]
	delete(m.targets, strings.TrimSpace(issueID))
	m.mu.Unlock()
	m.notify()
	target.progress.close()
	if target.flightDone != nil {
		<-target.flightDone
	}
}

func (m *heartbeatManager) protects(issueID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	target, ok := m.targets[strings.TrimSpace(issueID)]
	if !ok {
		return false
	}
	if !target.workerChecked || target.livenessError != nil {
		return true
	}
	return target.workerAlive
}

func (m *heartbeatManager) current(issueID string, sequence uint64) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	target, ok := m.targets[strings.TrimSpace(issueID)]
	return ok && target.sequence == sequence
}

func (m *heartbeatManager) Running() bool {
	return m != nil && m.running.Load()
}

func (m *heartbeatManager) Run(ctx context.Context) {
	if m == nil || !m.running.CompareAndSwap(false, true) {
		return
	}
	defer m.running.Store(false)
	var workers sync.WaitGroup
	defer workers.Wait()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		resetHeartbeatTimer(timer, m.nextDelay(m.now()))
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			continue
		case <-timer.C:
		}
		for _, target := range m.due(m.now()) {
			workers.Add(1)
			go func() {
				defer workers.Done()
				m.execute(ctx, target)
			}()
		}
	}
}

func (m *heartbeatManager) nextDelay(now time.Time) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	var next time.Time
	for _, target := range m.targets {
		if target.inFlight {
			continue
		}
		if next.IsZero() || target.nextDue.Before(next) {
			next = target.nextDue
		}
	}
	if next.IsZero() {
		return time.Hour
	}
	return max(time.Millisecond, next.Sub(now))
}

func (m *heartbeatManager) due(now time.Time) []heartbeatTarget {
	m.mu.Lock()
	defer m.mu.Unlock()
	due := make([]heartbeatTarget, 0, len(m.targets))
	for issueID, target := range m.targets {
		if target.inFlight || now.Before(target.nextDue) {
			continue
		}
		target.inFlight = true
		target.flightDone = make(chan struct{})
		target.nextDue = now.Add(m.settings.interval)
		m.targets[issueID] = target
		due = append(due, target)
	}
	return due
}

func (m *heartbeatManager) execute(ctx context.Context, target heartbeatTarget) {
	if target.flightDone != nil {
		defer close(target.flightDone)
	}
	operationCtx, cancel := context.WithTimeout(ctx, heartbeatOperationTimeout)
	defer cancel()
	settings := m.settingsSnapshot()
	result := heartbeatResult{issueID: target.issueID, sequence: target.sequence}
	if target.progress != nil {
		running := target.progress.latest.Load()
		target.workerProcess = running.WorkerProcess
		target.workspacePath = running.WorkspacePath
	}
	result.workerAlive, result.workerChecked, result.livenessError = heartbeatWorkerLiveness(target.workerProcess)
	result.workspaceModifiedAt, result.workspaceAdvanced = heartbeatWorkspaceActivity(target.workspacePath, target.workspaceModifiedAt)
	if result.workerChecked && !result.workerAlive && result.livenessError == nil {
		m.finish(target, result)
		return
	}
	now := m.now().UTC()
	result.heartbeat, result.workAttemptError = m.persistHeartbeat(operationCtx, target, settings, now)
	result.workAttemptRenewed = result.workAttemptError == nil && settings.workAttempts != nil && result.heartbeat.AttemptID > 0
	if settings.scheduling != nil {
		result.claim, result.claimError = settings.scheduling.RenewClaim(operationCtx, target.issueID, now)
		result.claimOwnerLost = errors.Is(result.claimError, ErrSchedulingClaimLost)
		result.claimRenewed = result.claimError == nil
		if result.claimRenewed {
			result.claimIssue = result.claim.Issue
			result.currentClaimOwner = result.claim.Owner
		}
	} else if settings.claiming.Enabled && strings.TrimSpace(settings.claiming.LeaseField) != "" && strings.TrimSpace(target.claimOwner) != "" {
		result.claimIssue, result.currentClaimOwner, result.claimOwnerLost, result.claimError = renewTrackerClaim(operationCtx, settings, target, now)
		result.claimRenewed = result.claimError == nil && !result.claimOwnerLost
	}
	m.finish(target, result)
}

func (m *heartbeatManager) persistHeartbeat(ctx context.Context, target heartbeatTarget, settings heartbeatSettings, now time.Time) (store.WorkAttemptHeartbeat, error) {
	heartbeat := target.workAttemptHeartbeat
	heartbeat.HeartbeatAt = now
	heartbeat.LeaseExpiresAt = now.Add(settings.leaseTTL)
	if target.progress != nil {
		target.progress.mu.Lock()
		defer target.progress.mu.Unlock()
		if target.progress.closed {
			return heartbeat, context.Canceled
		}
		now = m.now().UTC()
		heartbeat.LeaseExpiresAt = now.Add(settings.leaseTTL)
		heartbeat = target.progress.heartbeat(heartbeat, now)
	}
	if settings.workAttempts != nil && heartbeat.AttemptID > 0 {
		if err := settings.workAttempts.RecordWorkAttemptHeartbeat(ctx, heartbeat); err != nil {
			return heartbeat, err
		}
		if target.progress != nil {
			target.progress.persisted.Store(&heartbeat)
		}
	}
	return heartbeat, nil
}

func (m *heartbeatManager) settingsSnapshot() heartbeatSettings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settings
}

func (m *heartbeatManager) finish(target heartbeatTarget, result heartbeatResult) {
	m.mu.Lock()
	current, ok := m.targets[target.issueID]
	if !ok || current.sequence != target.sequence {
		m.mu.Unlock()
		return
	}
	current.inFlight = false
	current.workerAlive = result.workerAlive
	current.workerChecked = result.workerChecked
	current.livenessError = result.livenessError
	if !result.workspaceModifiedAt.IsZero() {
		current.workspaceModifiedAt = result.workspaceModifiedAt
	}
	current.workspaceAdvanced = result.workspaceAdvanced
	m.targets[target.issueID] = current
	m.mu.Unlock()
	if result.workAttemptError != nil && !errors.Is(result.workAttemptError, store.ErrNotFound) {
		m.logger.Warn("dedicated work attempt heartbeat failed", "attempt_id", result.heartbeat.AttemptID, "issue_id", result.issueID, "error", result.workAttemptError)
	}
	if result.claimError != nil {
		m.logger.Warn("dedicated claim heartbeat failed", "issue_id", result.issueID, "error", result.claimError)
	}
	if result.livenessError != nil {
		m.logger.Warn("worker liveness inspection failed", "issue_id", result.issueID, "error", result.livenessError)
	}
	select {
	case m.results <- result:
	default:
	}
	m.notify()
}

func (m *heartbeatManager) notify() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func resetHeartbeatTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func heartbeatWorkerLiveness(identity procgroup.Identity) (bool, bool, error) {
	if identity.PID <= 0 || identity.StartedAt.IsZero() {
		return false, false, nil
	}
	alive, err := procgroup.Alive(identity)
	return alive, true, err
}

func heartbeatWorkspaceActivity(path string, previous time.Time) (time.Time, bool) {
	modifiedAt, err := latestHeartbeatWorkspaceModification(path)
	if err != nil {
		return time.Time{}, false
	}
	return modifiedAt, !previous.IsZero() && modifiedAt.After(previous)
}

func latestHeartbeatWorkspaceModification(path string) (time.Time, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return time.Time{}, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	latest := info.ModTime().UTC()
	for _, candidate := range []string{filepath.Join(path, ".detent"), filepath.Join(path, ".detent", "tmp")} {
		info, err := os.Stat(candidate)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return time.Time{}, err
		}
		if modifiedAt := info.ModTime().UTC(); modifiedAt.After(latest) {
			latest = modifiedAt
		}
	}
	return latest, nil
}

func renewTrackerClaim(ctx context.Context, settings heartbeatSettings, target heartbeatTarget, now time.Time) (connector.Issue, string, bool, error) {
	if settings.connector == nil {
		return connector.Issue{}, "", false, errors.New("connector is unavailable")
	}
	issues, err := settings.connector.FetchIssueStatesByIDs(ctx, []string{target.issueID})
	if err != nil {
		return connector.Issue{}, "", false, err
	}
	for _, issue := range issues {
		if issue.ID != target.issueID {
			continue
		}
		owner := heartbeatClaimOwner(issue, settings.claiming)
		if !sameClaimOwner(owner, target.claimOwner) {
			return cloneIssue(issue), owner, true, nil
		}
		if err := settings.connector.SetField(ctx, target.issueID, settings.claiming.LeaseField, formatClaimTime(now)); err != nil {
			return connector.Issue{}, owner, false, err
		}
		return issueWithLeaseField(issue, settings.claiming.LeaseField, now), owner, false, nil
	}
	return connector.Issue{}, "", false, fmt.Errorf("issue %s was not returned during claim heartbeat", target.issueID)
}

func heartbeatClaimOwner(issue connector.Issue, claiming ClaimingConfig) string {
	if claiming.OwnershipMode == workflowconfig.IdentityOwnershipField {
		if issue.Fields == nil {
			return ""
		}
		return strings.TrimSpace(issue.Fields[claiming.OwnerField])
	}
	owners := append([]string{issue.AssigneeID}, issue.Assignees...)
	normalized := make([]string, 0, len(owners))
	seen := make(map[string]struct{}, len(owners))
	for _, owner := range owners {
		owner = strings.TrimSpace(owner)
		key := normalizeClaimOwner(owner)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, owner)
	}
	sortClaimOwners(normalized)
	if len(normalized) == 0 {
		return ""
	}
	return normalized[0]
}

func (o *Orchestrator) persistedWorkerLive(ctx context.Context, issue connector.Issue) bool {
	processStore, ok := o.workAttempts.(interface {
		ListActiveWorkerProcesses(context.Context) ([]store.WorkerProcess, error)
	})
	if !ok {
		return false
	}
	processes, err := processStore.ListActiveWorkerProcesses(ctx)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("worker liveness registry lookup failed", "issue_id", issue.ID, "identifier", issue.Identifier, "error", err)
		}
		return true
	}
	for _, process := range processes {
		if !workerProcessMatchesIssue(process, issue) {
			continue
		}
		alive, err := procgroup.Alive(procgroup.Identity{PID: process.PID, GroupID: process.GroupID, StartedAt: process.StartedAt})
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("persisted worker liveness inspection failed", "issue_id", issue.ID, "identifier", issue.Identifier, "pid", process.PID, "error", err)
			}
			return true
		}
		if alive {
			return true
		}
	}
	return false
}

func workerProcessMatchesIssue(process store.WorkerProcess, issue connector.Issue) bool {
	if issue.ID != "" && process.IssueID == issue.ID {
		return true
	}
	if issue.Identifier != "" && strings.EqualFold(process.Identifier, issue.Identifier) {
		return true
	}
	return issue.URL != "" && strings.EqualFold(process.IssueURL, issue.URL)
}

func (o *Orchestrator) trackRunningHeartbeat(state *State, running Running, claimed Claimed, now time.Time) {
	if o == nil || o.heartbeats == nil || strings.TrimSpace(running.Issue.ID) == "" {
		return
	}
	running = running.withProgress()
	owner := strings.TrimSpace(claimed.Owner)
	if owner == "" {
		owner = o.claimOwner()
	}
	o.heartbeats.upsert(heartbeatTarget{
		progress:             running.progress,
		issueID:              running.Issue.ID,
		claimOwner:           owner,
		workAttemptHeartbeat: o.runningWorkAttemptHeartbeat(state, running, now),
		workerProcess:        running.WorkerProcess,
		workspacePath:        running.WorkspacePath,
	})
}

func (o *Orchestrator) handleHeartbeatResult(state *State, result heartbeatResult) {
	if o == nil || state == nil || o.heartbeats == nil || !o.heartbeats.current(result.issueID, result.sequence) {
		return
	}
	running, ok := state.Running[result.issueID]
	if !ok {
		o.heartbeats.remove(result.issueID)
		return
	}
	if result.claimOwnerLost {
		if o.logger != nil {
			o.logger.Warn("claim heartbeat owner changed", "issue_id", result.issueID, "owner", o.claimOwner(), "current_owner", result.currentClaimOwner)
		}
		o.releaseClaim(state, result.issueID)
		return
	}
	if result.workAttemptRenewed && result.heartbeat.AttemptID == running.WorkAttemptID {
		o.applyWorkAttemptHeartbeatSnapshot(state, running.WorkAttemptID, result.heartbeat, running.LastMessageTruncation)
	}
	if !result.claimRenewed {
		return
	}
	claimed, ok := state.Claimed[result.issueID]
	if !ok {
		return
	}
	if result.claim.LeaseRenewedAt.IsZero() {
		claimed.Owner = result.currentClaimOwner
		claimed.LeaseRenewedAt = result.heartbeat.HeartbeatAt
		claimed.LeaseExpiresAt = o.leaseExpiresAt(result.heartbeat.HeartbeatAt)
		claimed.Issue = mergeIssueTrackerFields(claimed.Issue, result.claimIssue)
	} else {
		renewed := result.claim
		renewed.Issue = mergeIssueTrackerFields(claimed.Issue, renewed.Issue)
		claimed = renewed
	}
	state.Claimed[result.issueID] = claimed
	running.Issue = mergeIssueTrackerFields(running.Issue, result.claimIssue)
	state.Running[result.issueID] = running
}
