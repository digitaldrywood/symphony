package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
)

type sqliteStore struct {
	db      *sql.DB
	queries *sqlc.Queries
	path    string
}

var _ Store = (*sqliteStore)(nil)

func openSQLite(ctx context.Context, cfg Config) (*sqliteStore, error) {
	if cfg.Path == "" {
		return nil, errors.New("sqlite path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("creating sqlite directory: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := configureSQLite(ctx, db, busyTimeoutMillis(cfg.BusyTimeout)); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := runMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &sqliteStore{
		db:      db,
		queries: sqlc.New(db),
		path:    cfg.Path,
	}, nil
}

func (s *sqliteStore) Queries() *sqlc.Queries {
	return s.queries
}

func (s *sqliteStore) RuntimeEvidence(ctx context.Context, query RuntimeEvidenceQuery) (RuntimeEvidence, error) {
	evidence := RuntimeEvidence{
		Backend: BackendSQLite,
		Path:    s.path,
	}
	if err := s.db.PingContext(ctx); err != nil {
		return evidence, fmt.Errorf("pinging runtime store: %w", err)
	}
	evidence.Healthy = true

	version, err := s.runtimeMigrationVersion(ctx)
	if err != nil {
		return evidence, err
	}
	evidence.MigrationVersion = version
	evidence.MigrationStatus = fmt.Sprintf("applied through %d", version)

	tables := []struct {
		name          string
		projectScoped bool
	}{
		{name: "detent_runs"},
		{name: "codex_sessions"},
		{name: "fair_share_usage", projectScoped: true},
		{name: "usage_events", projectScoped: true},
		{name: "workflow_phase_events", projectScoped: true},
		{name: "efficiency_receipts", projectScoped: true},
		{name: "work_attempts", projectScoped: true},
		{name: "scheduler_decisions", projectScoped: true},
		{name: "merge_required_check_streaks", projectScoped: true},
		{name: "validator_verdicts", projectScoped: true},
		{name: "security_audit_runs", projectScoped: true},
		{name: "security_audit_dispositions"},
		{name: "retro_runs", projectScoped: true},
		{name: "routine_runs", projectScoped: true},
		{name: "backlog_admission_proposals", projectScoped: true},
		{name: "backlog_admission_declines", projectScoped: true},
		{name: "backlog_admission_runs", projectScoped: true},
		{name: "backlog_admission_malformed_results", projectScoped: true},
		{name: "scheduled_runs", projectScoped: true},
		{name: "health_notification_states"},
		{name: "api_keys"},
		{name: "api_usage_logs"},
		{name: "auth_magic_links"},
		{name: "auth_sessions"},
	}
	projectID := strings.TrimSpace(query.ProjectID)
	for _, table := range tables {
		count, err := s.runtimeTableCount(ctx, table.name, table.projectScoped, projectID)
		if err != nil {
			return evidence, err
		}
		scope := "fleet"
		if table.projectScoped && projectID != "" {
			scope = "project"
		}
		evidence.Tables = append(evidence.Tables, RuntimeTableEvidence{
			Name:     table.name,
			Scope:    scope,
			RowCount: count,
		})
	}

	workflowEvidence, err := s.runtimeWorkflowPhaseEventEvidence(ctx, projectID)
	if err != nil {
		return evidence, err
	}
	evidence.WorkflowPhaseEvents = workflowEvidence

	return evidence, nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}

func (s *sqliteStore) StartRun(ctx context.Context, attrs RunStart) (int64, error) {
	startedAt, err := requiredTimestamp("started_at", attrs.StartedAt)
	if err != nil {
		return 0, err
	}

	run, err := s.queries.CreateDetentRun(ctx, sqlc.CreateDetentRunParams{
		StartedAt:            startedAt,
		StoppedAt:            sql.NullString{},
		RestartReason:        sql.NullString{},
		PeakConcurrentAgents: nonNegative(attrs.PeakConcurrentAgents),
		SessionsLaunched:     nonNegative(attrs.SessionsLaunched),
		InputTokens:          nonNegative(attrs.InputTokens),
		OutputTokens:         nonNegative(attrs.OutputTokens),
		TotalTokens:          nonNegative(attrs.TotalTokens),
		RuntimeSeconds:       nonNegative(attrs.RuntimeSeconds),
	})
	if err != nil {
		return 0, fmt.Errorf("starting stats run: %w", err)
	}
	return run.ID, nil
}

func (s *sqliteStore) UpdateRun(ctx context.Context, runID int64, attrs RunUpdate) error {
	rows, err := s.queries.UpdateDetentRun(ctx, sqlc.UpdateDetentRunParams{
		StoppedAt:            sql.NullString{},
		RestartReason:        sql.NullString{},
		PeakConcurrentAgents: nonNegative(attrs.PeakConcurrentAgents),
		SessionsLaunched:     nonNegative(attrs.SessionsLaunched),
		InputTokens:          nonNegative(attrs.InputTokens),
		OutputTokens:         nonNegative(attrs.OutputTokens),
		TotalTokens:          nonNegative(attrs.TotalTokens),
		RuntimeSeconds:       nonNegative(attrs.RuntimeSeconds),
		ID:                   runID,
	})
	if err != nil {
		return fmt.Errorf("updating stats run: %w", err)
	}
	return requireAffected(rows, "detent run", runID)
}

func (s *sqliteStore) StopRun(ctx context.Context, runID int64, attrs RunStop) error {
	stoppedAt, err := requiredTimestamp("stopped_at", attrs.StoppedAt)
	if err != nil {
		return err
	}

	rows, err := s.queries.UpdateDetentRun(ctx, sqlc.UpdateDetentRunParams{
		StoppedAt:            sql.NullString{String: stoppedAt, Valid: true},
		RestartReason:        nullString(attrs.RestartReason),
		PeakConcurrentAgents: nonNegative(attrs.PeakConcurrentAgents),
		SessionsLaunched:     nonNegative(attrs.SessionsLaunched),
		InputTokens:          nonNegative(attrs.InputTokens),
		OutputTokens:         nonNegative(attrs.OutputTokens),
		TotalTokens:          nonNegative(attrs.TotalTokens),
		RuntimeSeconds:       nonNegative(attrs.RuntimeSeconds),
		ID:                   runID,
	})
	if err != nil {
		return fmt.Errorf("stopping stats run: %w", err)
	}
	return requireAffected(rows, "detent run", runID)
}

func (s *sqliteStore) StartSession(ctx context.Context, attrs SessionStart) (int64, error) {
	startedAt, err := requiredTimestamp("started_at", attrs.StartedAt)
	if err != nil {
		return 0, err
	}
	requestedModel := strings.TrimSpace(attrs.RequestedModel)
	if requestedModel == "" {
		requestedModel = strings.TrimSpace(attrs.Model)
	}
	identity := attrs.RuntimeIdentity.Normalize()
	identity = identity.Merge(agentidentity.Identity{
		BackendID:      attrs.AgentBackendID,
		BackendKind:    attrs.AgentBackendKind,
		Role:           attrs.AgentRole,
		RequestedModel: agentidentity.NewValue(requestedModel, agentidentity.ProvenanceConfigured),
	})

	identityJSON, err := marshalRuntimeIdentity(identity)
	if err != nil {
		return 0, err
	}
	session, err := s.queries.CreateCodexSession(ctx, sqlc.CreateCodexSessionParams{
		RuntimeIdentityJson:          nullString(identityJSON),
		RunID:                        nullPositiveInt64(attrs.RunID),
		ProjectID:                    nullString(attrs.ProjectID),
		WorkAttemptID:                nullPositiveInt64(attrs.WorkAttemptID),
		IssueID:                      nullString(attrs.IssueID),
		Identifier:                   nullString(attrs.Identifier),
		IssueURL:                     nullString(attrs.IssueURL),
		StartedAt:                    sql.NullString{String: startedAt, Valid: true},
		RequestedModel:               nullString(requestedModel),
		AgentBackendID:               nullString(attrs.AgentBackendID),
		AgentBackendKind:             nullString(attrs.AgentBackendKind),
		AgentRole:                    nullString(attrs.AgentRole),
		AgentRoute:                   nullString(identity.Route),
		Provider:                     nullIdentityValue(identity.Provider),
		ProviderProvenance:           nullIdentityProvenance(identity.Provider),
		RequestedModelProvenance:     nullIdentityProvenance(identity.RequestedModel),
		ModelProvenance:              nullIdentityProvenance(identity.ResolvedModel),
		ReasoningEffort:              nullIdentityValue(identity.ReasoningEffort),
		ReasoningEffortProvenance:    nullIdentityProvenance(identity.ReasoningEffort),
		ServiceTier:                  nullIdentityValue(identity.ServiceTier),
		ServiceTierProvenance:        nullIdentityProvenance(identity.ServiceTier),
		IdentityObservedAt:           nullIdentityObservedAt(identity.ObservedAt),
		CompletedAt:                  sql.NullString{},
		ModelContextWindow:           sql.NullInt64{},
		FinalState:                   sql.NullString{String: SessionStateRunning, Valid: true},
		Model:                        nullString(attrs.Model),
		ProviderThreadID:             nullString(attrs.ProviderThreadID),
		ProviderSessionID:            nullString(attrs.ProviderSessionID),
		ResumedFromSessionID:         nullPositiveInt64(attrs.ResumedFromSessionID),
		OrphanRecoveryOutcome:        nullString(attrs.OrphanRecoveryOutcome),
		OrphanRecoveryFallbackReason: nullString(attrs.OrphanRecoveryFallbackReason),
	})
	if err != nil {
		return 0, fmt.Errorf("starting codex session: %w", err)
	}
	return session.ID, nil
}

func (s *sqliteStore) UpdateSessionProviderIdentity(ctx context.Context, sessionID int64, identity SessionProviderIdentity) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	rows, err := s.queries.UpdateCodexSessionProviderIdentity(ctx, sqlc.UpdateCodexSessionProviderIdentityParams{
		ProviderThreadID:  nullString(identity.ThreadID),
		ProviderSessionID: nullString(identity.SessionID),
		ID:                sessionID,
	})
	if err != nil {
		return fmt.Errorf("updating codex session provider identity: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) UpdateSessionWorkerProcess(ctx context.Context, sessionID int64, registration WorkerProcessRegistration) error {
	if sessionID <= 0 || registration.PID <= 0 || registration.StartedAt.IsZero() {
		return ErrNotFound
	}
	startedAt, err := requiredTimestamp("worker_started_at", registration.StartedAt)
	if err != nil {
		return err
	}
	rows, err := s.queries.UpdateCodexSessionWorkerProcess(ctx, sqlc.UpdateCodexSessionWorkerProcessParams{
		WorkerPid:         sql.NullInt64{Int64: int64(registration.PID), Valid: true},
		WorkerPgid:        sql.NullInt64{Int64: int64(registration.GroupID), Valid: true},
		WorkerStartedAt:   sql.NullString{String: startedAt, Valid: true},
		WorkerCleanupRoot: nullString(registration.CleanupRoot),
		WorkerCleanupPath: nullString(registration.CleanupPath),
		ID:                sessionID,
	})
	if err != nil {
		return fmt.Errorf("updating codex session worker process: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) ListActiveWorkerProcesses(ctx context.Context) ([]WorkerProcess, error) {
	rows, err := s.queries.ListActiveWorkerProcesses(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing active worker processes: %w", err)
	}
	processes := make([]WorkerProcess, 0, len(rows))
	for _, row := range rows {
		startedAt, err := parseTimestamp("worker_started_at", row.WorkerStartedAt)
		if err != nil {
			return nil, err
		}
		var completedAt time.Time
		if strings.TrimSpace(row.CompletedAt) != "" {
			completedAt, err = parseTimestamp("completed_at", row.CompletedAt)
			if err != nil {
				return nil, err
			}
		}
		processes = append(processes, WorkerProcess{
			SessionID:   row.SessionID,
			IssueID:     strings.TrimSpace(row.IssueID),
			Identifier:  strings.TrimSpace(row.Identifier),
			IssueURL:    strings.TrimSpace(row.IssueURL),
			FinalState:  strings.TrimSpace(row.FinalState),
			CompletedAt: completedAt,
			WorkerProcessIdentity: WorkerProcessIdentity{
				PID:       int(row.WorkerPid),
				GroupID:   int(row.WorkerPgid),
				StartedAt: startedAt,
			},
			CleanupRoot: strings.TrimSpace(row.WorkerCleanupRoot),
			CleanupPath: strings.TrimSpace(row.WorkerCleanupPath),
		})
	}
	return processes, nil
}

func (s *sqliteStore) MarkSessionWorkerProcessReaped(ctx context.Context, sessionID int64, reap WorkerProcessReap) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	reapedAt, err := requiredTimestamp("worker_reaped_at", reap.ReapedAt)
	if err != nil {
		return err
	}
	rows, err := s.queries.MarkCodexSessionWorkerProcessReaped(ctx, sqlc.MarkCodexSessionWorkerProcessReapedParams{
		WorkerReapedAt:    sql.NullString{String: reapedAt, Valid: true},
		WorkerReapOutcome: nullString(reap.Outcome),
		WorkerReapReason:  nullString(reap.Reason),
		ID:                sessionID,
	})
	if err != nil {
		return fmt.Errorf("marking codex session worker process reaped: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) UpdateSessionResumeState(ctx context.Context, sessionID int64, state SessionResumeState) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	rows, err := s.queries.UpdateCodexSessionResumeState(ctx, sqlc.UpdateCodexSessionResumeStateParams{
		ResumedFromSessionID:         nullPositiveInt64(state.ResumedFromSessionID),
		OrphanRecoveryOutcome:        nullString(state.OrphanRecoveryOutcome),
		OrphanRecoveryFallbackReason: nullString(state.OrphanRecoveryFallbackReason),
		ID:                           sessionID,
	})
	if err != nil {
		return fmt.Errorf("updating codex session resume state: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) UpdateSessionIdentity(ctx context.Context, sessionID int64, identity agentidentity.Identity) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	identity = identity.Normalize()
	identityJSON, err := marshalRuntimeIdentity(identity)
	if err != nil {
		return err
	}
	rows, err := s.queries.UpdateCodexSessionIdentity(ctx, sqlc.UpdateCodexSessionIdentityParams{
		RuntimeIdentityJson:       nullString(identityJSON),
		AgentBackendID:            nullString(identity.BackendID),
		AgentBackendKind:          nullString(identity.BackendKind),
		AgentRole:                 nullString(identity.Role),
		AgentRoute:                nullString(identity.Route),
		Provider:                  nullIdentityValue(identity.Provider),
		ProviderProvenance:        nullIdentityProvenance(identity.Provider),
		RequestedModel:            nullIdentityValue(identity.RequestedModel),
		RequestedModelProvenance:  nullIdentityProvenance(identity.RequestedModel),
		Model:                     nullIdentityValue(identity.ResolvedModel),
		ModelProvenance:           nullIdentityProvenance(identity.ResolvedModel),
		ReasoningEffort:           nullIdentityValue(identity.ReasoningEffort),
		ReasoningEffortProvenance: nullIdentityProvenance(identity.ReasoningEffort),
		ServiceTier:               nullIdentityValue(identity.ServiceTier),
		ServiceTierProvenance:     nullIdentityProvenance(identity.ServiceTier),
		IdentityObservedAt:        nullIdentityObservedAt(identity.ObservedAt),
		ID:                        sessionID,
	})
	if err != nil {
		return fmt.Errorf("updating codex session identity: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) FinishSession(ctx context.Context, sessionID int64, attrs SessionFinish) error {
	completedAt, err := requiredTimestamp("completed_at", attrs.CompletedAt)
	if err != nil {
		return err
	}
	if !attrs.RuntimeIdentity.IsZero() {
		if err := s.UpdateSessionIdentity(ctx, sessionID, attrs.RuntimeIdentity); err != nil {
			return err
		}
	}

	rows, err := s.queries.FinishCodexSession(ctx, sqlc.FinishCodexSessionParams{
		CompletedAt:           sql.NullString{String: completedAt, Valid: true},
		Turns:                 nonNegative(attrs.Turns),
		InputTokens:           nonNegative(attrs.InputTokens),
		CachedInputTokens:     nullNonNegativeInt64(attrs.CachedInputTokens),
		OutputTokens:          nonNegative(attrs.OutputTokens),
		ReasoningOutputTokens: nullNonNegativeInt64(attrs.ReasoningOutputTokens),
		TotalTokens:           nonNegative(attrs.TotalTokens),
		ModelContextWindow:    nullOptionalInt64(attrs.ModelContextWindow),
		RuntimeSeconds:        nonNegative(attrs.RuntimeSeconds),
		FinalState:            nullString(attrs.FinalState),
		Model:                 nullString(attrs.Model),
		ProviderThreadID:      nullString(attrs.ProviderThreadID),
		ProviderSessionID:     nullString(attrs.ProviderSessionID),
		ResumedFromSessionID:  nullPositiveInt64(attrs.ResumedFromSessionID),
		SkillDraftProposed:    boolInt64(attrs.SkillDraftProposed),
		ID:                    sessionID,
	})
	if err != nil {
		return fmt.Errorf("finishing codex session: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func (s *sqliteStore) LatestCompletedAgentResumeState(ctx context.Context, attrs AgentResumeLookup) (AgentResumeState, error) {
	attrs = normalizeAgentResumeLookup(attrs)
	if attrs.ProjectID == "" || attrs.PRNumber <= 0 || attrs.PRHeadSHA == "" || attrs.PRBaseSHA == "" {
		return AgentResumeState{}, ErrNotFound
	}
	if attrs.RequestedModel == "" || attrs.AgentBackendID == "" || attrs.AgentBackendKind == "" || attrs.AgentRole == "" {
		return AgentResumeState{}, ErrNotFound
	}
	if attrs.IssueID == "" && attrs.Identifier == "" && attrs.IssueURL == "" {
		return AgentResumeState{}, ErrNotFound
	}

	row, err := s.queries.GetLatestCompletedAgentResumeState(ctx, sqlc.GetLatestCompletedAgentResumeStateParams{
		ProjectID:        nullString(attrs.ProjectID),
		PrNumber:         attrs.PRNumber,
		PrHeadSha:        attrs.PRHeadSHA,
		PrBaseSha:        attrs.PRBaseSHA,
		AgentBackendID:   nullString(attrs.AgentBackendID),
		AgentBackendKind: nullString(attrs.AgentBackendKind),
		AgentRole:        nullString(attrs.AgentRole),
		RequestedModel:   nullString(attrs.RequestedModel),
		IssueID:          attrs.IssueID,
		Identifier:       attrs.Identifier,
		IssueURL:         attrs.IssueURL,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentResumeState{}, ErrNotFound
		}
		return AgentResumeState{}, fmt.Errorf("reading latest agent resume state: %w", err)
	}
	completedAt, err := parseTimestamp("completed_at", row.CompletedAt)
	if err != nil {
		return AgentResumeState{}, err
	}
	runtimeIdentity, err := unmarshalRuntimeIdentity(row.RuntimeIdentityJson)
	if err != nil {
		return AgentResumeState{}, err
	}
	return AgentResumeState{
		RuntimeIdentity:   runtimeIdentity,
		DetentSessionID:   row.ID,
		ProviderThreadID:  strings.TrimSpace(row.ProviderThreadID),
		ProviderSessionID: strings.TrimSpace(row.ProviderSessionID),
		RequestedModel:    strings.TrimSpace(row.RequestedModel),
		Model:             strings.TrimSpace(row.Model),
		AgentBackendID:    strings.TrimSpace(row.AgentBackendID),
		AgentBackendKind:  strings.TrimSpace(row.AgentBackendKind),
		AgentRole:         strings.TrimSpace(row.AgentRole),
		CompletedAt:       completedAt,
	}, nil
}

func (s *sqliteStore) LatestIssueAgentResumeState(ctx context.Context, identity IssueIdentity) (AgentResumeState, error) {
	identity = normalizeIssueIdentity(identity)
	if identity.ProjectID == "" {
		return AgentResumeState{}, ErrProjectRequired
	}
	if identity.IssueID == "" && identity.Identifier == "" && identity.IssueURL == "" {
		return AgentResumeState{}, ErrNotFound
	}

	row, err := s.queries.GetLatestIssueAgentResumeState(ctx, sqlc.GetLatestIssueAgentResumeStateParams{
		ProjectID:  nullString(identity.ProjectID),
		IssueID:    identity.IssueID,
		Identifier: identity.Identifier,
		IssueURL:   identity.IssueURL,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AgentResumeState{}, ErrNotFound
		}
		return AgentResumeState{}, fmt.Errorf("reading latest issue agent resume state: %w", err)
	}
	completedAt, err := parseTimestamp("completed_at", row.CompletedAt)
	if err != nil {
		return AgentResumeState{}, err
	}
	runtimeIdentity, err := unmarshalRuntimeIdentity(row.RuntimeIdentityJson)
	if err != nil {
		return AgentResumeState{}, err
	}
	return AgentResumeState{
		RuntimeIdentity:   runtimeIdentity,
		DetentSessionID:   row.ID,
		ProviderThreadID:  strings.TrimSpace(row.ProviderThreadID),
		ProviderSessionID: strings.TrimSpace(row.ProviderSessionID),
		RequestedModel:    strings.TrimSpace(row.RequestedModel),
		Model:             strings.TrimSpace(row.Model),
		AgentBackendID:    strings.TrimSpace(row.AgentBackendID),
		AgentBackendKind:  strings.TrimSpace(row.AgentBackendKind),
		AgentRole:         strings.TrimSpace(row.AgentRole),
		CompletedAt:       completedAt,
	}, nil
}

func (s *sqliteStore) ListOrphanedAgentSessions(ctx context.Context, projectID string) ([]OrphanedAgentSession, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	rows, err := s.queries.ListOrphanedAgentSessions(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("listing orphaned agent sessions: %w", err)
	}
	orphans := make([]OrphanedAgentSession, 0, len(rows))
	for _, row := range rows {
		startedAt, err := parseTimestamp("started_at", row.StartedAt)
		if err != nil {
			return nil, err
		}
		runtimeIdentity, err := unmarshalRuntimeIdentity(row.RuntimeIdentityJson)
		if err != nil {
			return nil, err
		}
		orphans = append(orphans, OrphanedAgentSession{
			ResumeState: AgentResumeState{
				RuntimeIdentity:   runtimeIdentity,
				DetentSessionID:   row.ID,
				ProviderThreadID:  strings.TrimSpace(row.ProviderThreadID),
				ProviderSessionID: strings.TrimSpace(row.ProviderSessionID),
				RequestedModel:    strings.TrimSpace(row.RequestedModel),
				Model:             strings.TrimSpace(row.Model),
				AgentBackendID:    strings.TrimSpace(row.AgentBackendID),
				AgentBackendKind:  strings.TrimSpace(row.AgentBackendKind),
				AgentRole:         strings.TrimSpace(row.AgentRole),
				Orphaned:          true,
			},
			WorkAttemptID: row.WorkAttemptID.Int64,
			ProjectID:     strings.TrimSpace(row.ProjectID),
			IssueID:       strings.TrimSpace(row.IssueID),
			Identifier:    strings.TrimSpace(row.Identifier),
			IssueURL:      strings.TrimSpace(row.IssueURL),
			WorkerType:    strings.TrimSpace(row.WorkerType),
			WorkerHost:    strings.TrimSpace(row.WorkerHost),
			Lane:          strings.TrimSpace(row.Lane),
			AttemptNumber: int(row.AttemptNumber),
			StartedAt:     startedAt,
		})
	}
	return orphans, nil
}

func (s *sqliteStore) MarkAgentSessionOrphaned(ctx context.Context, sessionID int64, completedAt time.Time) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	timestamp, err := requiredTimestamp("completed_at", completedAt)
	if err != nil {
		return err
	}
	rows, err := s.queries.MarkCodexSessionOrphaned(ctx, sqlc.MarkCodexSessionOrphanedParams{
		CompletedAt: sql.NullString{String: timestamp, Valid: true},
		ID:          sessionID,
	})
	if err != nil {
		return fmt.Errorf("marking codex session orphaned: %w", err)
	}
	return requireAffected(rows, "codex session", sessionID)
}

func normalizeAgentResumeLookup(attrs AgentResumeLookup) AgentResumeLookup {
	return AgentResumeLookup{
		ProjectID:        strings.TrimSpace(attrs.ProjectID),
		IssueID:          strings.TrimSpace(attrs.IssueID),
		Identifier:       strings.TrimSpace(attrs.Identifier),
		IssueURL:         strings.TrimSpace(attrs.IssueURL),
		PRNumber:         attrs.PRNumber,
		PRHeadSHA:        strings.TrimSpace(attrs.PRHeadSHA),
		PRBaseSHA:        strings.TrimSpace(attrs.PRBaseSHA),
		RequestedModel:   strings.TrimSpace(attrs.RequestedModel),
		AgentBackendID:   strings.TrimSpace(attrs.AgentBackendID),
		AgentBackendKind: strings.TrimSpace(attrs.AgentBackendKind),
		AgentRole:        strings.TrimSpace(attrs.AgentRole),
	}
}

func (s *sqliteStore) RecordUsageEvent(ctx context.Context, attrs UsageEvent) (int64, error) {
	projectID := strings.TrimSpace(attrs.ProjectID)
	if projectID == "" {
		return 0, errors.New("project_id is required")
	}

	startedAt, err := requiredTimestamp("started_at", attrs.StartedAt)
	if err != nil {
		return 0, err
	}
	finishedAt, err := requiredTimestamp("finished_at", attrs.FinishedAt)
	if err != nil {
		return 0, err
	}

	outcome := strings.TrimSpace(attrs.Outcome)
	if outcome == "" {
		return 0, errors.New("outcome is required")
	}

	event, err := s.queries.CreateUsageEvent(ctx, sqlc.CreateUsageEventParams{
		ProjectID:              projectID,
		RunID:                  nullPositiveInt64(attrs.RunID),
		SessionID:              nullPositiveInt64(attrs.SessionID),
		IssueID:                nullString(attrs.IssueID),
		Identifier:             nullString(attrs.Identifier),
		PrNumber:               nullOptionalInt64(attrs.PRNumber),
		Model:                  strings.TrimSpace(attrs.Model),
		InputTokens:            nonNegative(attrs.InputTokens),
		CachedInputTokens:      nullNonNegativeInt64(attrs.CachedInputTokens),
		OutputTokens:           nonNegative(attrs.OutputTokens),
		ReasoningOutputTokens:  nullNonNegativeInt64(attrs.ReasoningOutputTokens),
		TotalTokens:            nonNegative(attrs.TotalTokens),
		ModelContextWindow:     nullOptionalInt64(attrs.ModelContextWindow),
		CostUsd:                nonNegativeFloat(attrs.CostUSD),
		ProjectedCostUsd:       nullableNonNegativeFloat(attrs.ProjectedCostUSD),
		ProjectionOvershootUsd: nonNegativeFloat(attrs.ProjectionOvershootUSD),
		RuntimeSeconds:         nonNegative(attrs.RuntimeSeconds),
		StartedAt:              startedAt,
		FinishedAt:             finishedAt,
		EventDay:               attrs.FinishedAt.UTC().Format("2006-01-02"),
		Outcome:                outcome,
	})
	if err != nil {
		return 0, fmt.Errorf("recording usage event: %w", err)
	}
	return event.ID, nil
}

func (s *sqliteStore) UsageReport(ctx context.Context, query UsageReportQuery) (UsageReport, error) {
	group, err := normalizeUsageReportGroup(query.By)
	if err != nil {
		return UsageReport{}, err
	}
	from, err := optionalDateString(query.From)
	if err != nil {
		return UsageReport{}, err
	}
	to, err := optionalDateString(query.To)
	if err != nil {
		return UsageReport{}, err
	}
	if from != "" && to != "" && from > to {
		return UsageReport{}, errors.New("from date must be on or before to date")
	}

	rows, err := s.queries.UsageReportRows(ctx, sqlc.UsageReportRowsParams{
		BucketBy: string(group),
		FromDay:  nullString(from),
		ToDay:    nullString(to),
	})
	if err != nil {
		return UsageReport{}, fmt.Errorf("reading usage report: %w", err)
	}

	report := UsageReport{
		By:   group,
		From: from,
		To:   to,
		Rows: []UsageReportRow{},
		Totals: UsageReportTotals{
			Models: []UsageReportModel{},
		},
	}
	rowByKey := map[string]int{}
	modelTotals := map[string]int{}
	for _, row := range rows {
		key := row.GroupKey
		if key == "" {
			key = "unassigned"
		}

		index, ok := rowByKey[key]
		if !ok {
			report.Rows = append(report.Rows, UsageReportRow{
				Key:    key,
				Models: []UsageReportModel{},
			})
			index = len(report.Rows) - 1
			rowByKey[key] = index
		}

		model := UsageReportModel{
			Model:                 row.Model,
			InputTokens:           row.InputTokens,
			CachedInputTokens:     row.CachedInputTokens,
			OutputTokens:          row.OutputTokens,
			ReasoningOutputTokens: row.ReasoningOutputTokens,
			TotalTokens:           row.TotalTokens,
			ModelContextWindow:    row.ModelContextWindow,
			RuntimeSeconds:        row.RuntimeSeconds,
			Events:                row.Events,
		}
		if model.Model == "" {
			model.Model = "unassigned"
		}

		report.Rows[index].InputTokens += model.InputTokens
		report.Rows[index].CachedInputTokens += model.CachedInputTokens
		report.Rows[index].OutputTokens += model.OutputTokens
		report.Rows[index].ReasoningOutputTokens += model.ReasoningOutputTokens
		report.Rows[index].TotalTokens += model.TotalTokens
		if model.ModelContextWindow > report.Rows[index].ModelContextWindow {
			report.Rows[index].ModelContextWindow = model.ModelContextWindow
		}
		report.Rows[index].RuntimeSeconds += model.RuntimeSeconds
		report.Rows[index].Events += model.Events
		report.Rows[index].Models = append(report.Rows[index].Models, model)

		report.Totals.InputTokens += model.InputTokens
		report.Totals.CachedInputTokens += model.CachedInputTokens
		report.Totals.OutputTokens += model.OutputTokens
		report.Totals.ReasoningOutputTokens += model.ReasoningOutputTokens
		report.Totals.TotalTokens += model.TotalTokens
		if model.ModelContextWindow > report.Totals.ModelContextWindow {
			report.Totals.ModelContextWindow = model.ModelContextWindow
		}
		report.Totals.RuntimeSeconds += model.RuntimeSeconds
		report.Totals.Events += model.Events

		modelIndex, ok := modelTotals[model.Model]
		if !ok {
			report.Totals.Models = append(report.Totals.Models, UsageReportModel{
				Model: model.Model,
			})
			modelIndex = len(report.Totals.Models) - 1
			modelTotals[model.Model] = modelIndex
		}
		report.Totals.Models[modelIndex].InputTokens += model.InputTokens
		report.Totals.Models[modelIndex].CachedInputTokens += model.CachedInputTokens
		report.Totals.Models[modelIndex].OutputTokens += model.OutputTokens
		report.Totals.Models[modelIndex].ReasoningOutputTokens += model.ReasoningOutputTokens
		report.Totals.Models[modelIndex].TotalTokens += model.TotalTokens
		if model.ModelContextWindow > report.Totals.Models[modelIndex].ModelContextWindow {
			report.Totals.Models[modelIndex].ModelContextWindow = model.ModelContextWindow
		}
		report.Totals.Models[modelIndex].RuntimeSeconds += model.RuntimeSeconds
		report.Totals.Models[modelIndex].Events += model.Events
	}

	return report, nil
}

func (s *sqliteStore) DailyDigest(ctx context.Context, windows []DailyDigestWindow) ([]DailyDigestDay, error) {
	days := make([]DailyDigestDay, 0, len(windows))
	for _, window := range windows {
		if strings.TrimSpace(window.Date) == "" || window.From.IsZero() || window.To.IsZero() || !window.From.Before(window.To) {
			return nil, errors.New("daily digest window must have a date and increasing boundaries")
		}
		params := sqlc.DailyDigestRuntimeParams{
			FromAt: nullString(window.From.UTC().Format(time.RFC3339Nano)),
			ToAt:   nullString(window.To.UTC().Format(time.RFC3339Nano)),
		}
		runtime, err := s.queries.DailyDigestRuntime(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("reading daily digest runtime for %s: %w", window.Date, err)
		}
		models, err := s.queries.DailyDigestModels(ctx, sqlc.DailyDigestModelsParams(params))
		if err != nil {
			return nil, fmt.Errorf("reading daily digest models for %s: %w", window.Date, err)
		}
		failureClasses, err := s.queries.DailyDigestFailureClasses(ctx, sqlc.DailyDigestFailureClassesParams(params))
		if err != nil {
			return nil, fmt.Errorf("reading daily digest failures for %s: %w", window.Date, err)
		}
		capacityModes, err := s.queries.DailyDigestCapacityModes(ctx, sqlc.DailyDigestCapacityModesParams{
			FromAt: params.FromAt,
			ToAt:   params.ToAt.String,
		})
		if err != nil {
			return nil, fmt.Errorf("reading daily digest capacity modes for %s: %w", window.Date, err)
		}

		day := DailyDigestDay{
			Date:              window.Date,
			Sessions:          runtime.Sessions,
			InputTokens:       runtime.InputTokens,
			CachedInputTokens: runtime.CachedInputTokens,
			OutputTokens:      runtime.OutputTokens,
			TotalTokens:       runtime.TotalTokens,
			OrphanResumed:     runtime.OrphanResumed,
			OrphanFresh:       runtime.OrphanFresh,
			CapacityOutages:   runtime.CapacityOutages,
			CapacitySeconds:   runtime.CapacitySeconds,
			BreakerTrips:      runtime.BreakerTrips,
			FailedSessions:    runtime.FailedSessions,
			Models:            make([]UsageReportModel, 0, len(models)),
		}
		if len(failureClasses) > 0 {
			day.DominantErrorClass = failureClasses[0].ErrorClass
		}
		if len(capacityModes) > 0 {
			day.CapacityRecoveryMode = capacityModes[0].RecoveryMode
		}
		for _, model := range models {
			day.Models = append(day.Models, UsageReportModel{
				Model:                 model.Model,
				InputTokens:           model.InputTokens,
				CachedInputTokens:     model.CachedInputTokens,
				OutputTokens:          model.OutputTokens,
				ReasoningOutputTokens: model.ReasoningOutputTokens,
				TotalTokens:           model.TotalTokens,
				Events:                model.Sessions,
			})
		}
		days = append(days, day)
	}
	return days, nil
}

func (s *sqliteStore) BudgetCostEvents(ctx context.Context, query BudgetCostQuery) ([]BudgetCostEvent, error) {
	from, err := requiredTimestamp("from", query.From)
	if err != nil {
		return nil, err
	}
	to, err := requiredTimestamp("to", query.To)
	if err != nil {
		return nil, err
	}
	if !query.To.After(query.From) {
		return nil, errors.New("from must be before to")
	}

	rows, err := s.queries.BudgetCostEvents(ctx, sqlc.BudgetCostEventsParams{
		FromTime: from,
		ToTime:   to,
	})
	if err != nil {
		return nil, fmt.Errorf("reading budget cost events: %w", err)
	}

	projectFilter := budgetCostProjectFilter(query.ProjectIDs)
	events := make([]BudgetCostEvent, 0, len(rows))
	for _, row := range rows {
		projectID := strings.TrimSpace(row.ProjectID)
		if !projectFilter(projectID) {
			continue
		}
		at, err := parseTimestamp("finished_at", row.FinishedAt)
		if err != nil {
			return nil, err
		}
		events = append(events, BudgetCostEvent{
			ProjectID: projectID,
			At:        at.UTC(),
			CostUSD:   nonNegativeFloat(row.CostUsd),
		})
	}
	return events, nil
}

func (s *sqliteStore) IssueSpendSince(ctx context.Context, query IssueSpendSinceQuery) (IssueSpendSince, error) {
	projectID := strings.TrimSpace(query.ProjectID)
	if projectID == "" {
		return IssueSpendSince{}, errors.New("project_id is required")
	}
	issueID := strings.TrimSpace(query.IssueID)
	identifier := strings.TrimSpace(query.Identifier)
	if issueID == "" && identifier == "" {
		return IssueSpendSince{}, errors.New("issue identity is required")
	}
	since, err := requiredTimestamp("since", query.Since)
	if err != nil {
		return IssueSpendSince{}, err
	}
	row, err := s.queries.IssueSpendSince(ctx, sqlc.IssueSpendSinceParams{
		ProjectID:  projectID,
		Since:      since,
		IssueID:    issueID,
		Identifier: identifier,
	})
	if err != nil {
		return IssueSpendSince{}, fmt.Errorf("reading issue spend since accepted progress: %w", err)
	}
	spend := IssueSpendSince{
		CostUSD:     nonNegativeFloat(row.CostUsd),
		TotalTokens: nonNegative(row.TotalTokens),
		Sessions:    nonNegative(row.Sessions),
	}
	if row.FirstSessionAt != "" {
		spend.FirstSessionAt, err = parseTimestamp("first_session_at", row.FirstSessionAt)
		if err != nil {
			return IssueSpendSince{}, err
		}
	}
	if row.LastSessionAt != "" {
		spend.LastSessionAt, err = parseTimestamp("last_session_at", row.LastSessionAt)
		if err != nil {
			return IssueSpendSince{}, err
		}
	}
	return spend, nil
}

func (s *sqliteStore) LifetimeTotals(ctx context.Context) (LifetimeTotals, error) {
	row, err := s.queries.LifetimeTotals(ctx)
	if err != nil {
		return LifetimeTotals{}, fmt.Errorf("reading lifetime totals: %w", err)
	}
	return LifetimeTotals{
		InputTokens:           row.InputTokens,
		CachedInputTokens:     row.CachedInputTokens,
		OutputTokens:          row.OutputTokens,
		ReasoningOutputTokens: row.ReasoningOutputTokens,
		TotalTokens:           row.TotalTokens,
		RuntimeSeconds:        row.RuntimeSeconds,
		Sessions:              row.Sessions,
		Runs:                  row.Runs,
		OrphanResumed:         row.OrphanResumed,
		OrphanFresh:           row.OrphanFresh,
		ResumedInputTokens:    row.ResumedInputTokens,
		ResumedCachedTokens:   row.ResumedCachedTokens,
	}, nil
}

func (s *sqliteStore) DailyTokenSpend(ctx context.Context, day time.Time) (TokenSpend, error) {
	date, err := dateString(day)
	if err != nil {
		return TokenSpend{}, err
	}

	rows, err := s.queries.DailyTokenSpend(ctx, sql.NullString{String: date, Valid: true})
	if err != nil {
		return TokenSpend{}, fmt.Errorf("reading daily token spend: %w", err)
	}

	spend := TokenSpend{
		Date:    date,
		ByModel: make([]ModelTokenSpend, 0, len(rows)),
	}
	for _, row := range rows {
		modelSpend := ModelTokenSpend{
			Model:                 row.Model,
			InputTokens:           row.InputTokens,
			CachedInputTokens:     row.CachedInputTokens,
			OutputTokens:          row.OutputTokens,
			ReasoningOutputTokens: row.ReasoningOutputTokens,
			TotalTokens:           row.TotalTokens,
			Sessions:              row.Sessions,
		}
		spend.InputTokens += modelSpend.InputTokens
		spend.CachedInputTokens += modelSpend.CachedInputTokens
		spend.OutputTokens += modelSpend.OutputTokens
		spend.ReasoningOutputTokens += modelSpend.ReasoningOutputTokens
		spend.TotalTokens += modelSpend.TotalTokens
		spend.Sessions += modelSpend.Sessions
		spend.ByModel = append(spend.ByModel, modelSpend)
	}
	return spend, nil
}

func (s *sqliteStore) ProjectDailyTokenSpend(ctx context.Context, projectID string, day time.Time) (TokenSpend, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return TokenSpend{}, errors.New("project_id is required")
	}
	date, err := dateString(day)
	if err != nil {
		return TokenSpend{}, err
	}

	rows, err := s.queries.ProjectDailyTokenSpend(ctx, sqlc.ProjectDailyTokenSpendParams{
		CompletedAt: sql.NullString{String: date, Valid: true},
		ProjectID:   nullString(projectID),
	})
	if err != nil {
		return TokenSpend{}, fmt.Errorf("reading project daily token spend: %w", err)
	}

	spend := TokenSpend{Date: date, ByModel: make([]ModelTokenSpend, 0, len(rows))}
	for _, row := range rows {
		modelSpend := ModelTokenSpend{
			Model:                 row.Model,
			InputTokens:           row.InputTokens,
			CachedInputTokens:     row.CachedInputTokens,
			OutputTokens:          row.OutputTokens,
			ReasoningOutputTokens: row.ReasoningOutputTokens,
			TotalTokens:           row.TotalTokens,
			Sessions:              row.Sessions,
		}
		spend.InputTokens += modelSpend.InputTokens
		spend.CachedInputTokens += modelSpend.CachedInputTokens
		spend.OutputTokens += modelSpend.OutputTokens
		spend.ReasoningOutputTokens += modelSpend.ReasoningOutputTokens
		spend.TotalTokens += modelSpend.TotalTokens
		spend.Sessions += modelSpend.Sessions
		spend.ByModel = append(spend.ByModel, modelSpend)
	}
	return spend, nil
}

func (s *sqliteStore) BackfillSessionProjectIDs(ctx context.Context, attributions []SessionProjectAttribution) (int64, error) {
	var updated int64
	for _, attribution := range attributions {
		projectID := strings.TrimSpace(attribution.ProjectID)
		repository := strings.TrimSpace(attribution.Repository)
		if projectID == "" || repository == "" {
			continue
		}
		rows, err := s.queries.BackfillSessionProjectID(ctx, sqlc.BackfillSessionProjectIDParams{
			ProjectID:  nullString(projectID),
			Repository: repository,
		})
		if err != nil {
			return updated, fmt.Errorf("backfilling session project %q: %w", projectID, err)
		}
		updated += rows
	}
	return updated, nil
}

func (s *sqliteStore) IssueTokenSpend(ctx context.Context, identity IssueIdentity) (TokenSpend, error) {
	identity = normalizeIssueIdentity(identity)
	if identity.ProjectID == "" {
		return TokenSpend{}, ErrProjectRequired
	}
	if identity.IssueID == "" && identity.Identifier == "" && identity.IssueURL == "" {
		return TokenSpend{ByModel: []ModelTokenSpend{}}, nil
	}

	rows, err := s.queries.IssueTokenSpend(ctx, sqlc.IssueTokenSpendParams{
		ProjectID:  nullString(identity.ProjectID),
		IssueID:    nullString(identity.IssueID),
		Identifier: nullString(identity.Identifier),
		IssueURL:   nullString(identity.IssueURL),
	})
	if err != nil {
		return TokenSpend{}, fmt.Errorf("reading issue token spend: %w", err)
	}

	spend := TokenSpend{
		ByModel: make([]ModelTokenSpend, 0, len(rows)),
	}
	for _, row := range rows {
		modelSpend := ModelTokenSpend{
			Model:                 row.Model,
			InputTokens:           row.InputTokens,
			CachedInputTokens:     row.CachedInputTokens,
			OutputTokens:          row.OutputTokens,
			ReasoningOutputTokens: row.ReasoningOutputTokens,
			TotalTokens:           row.TotalTokens,
			Sessions:              row.Sessions,
		}
		spend.InputTokens += modelSpend.InputTokens
		spend.CachedInputTokens += modelSpend.CachedInputTokens
		spend.OutputTokens += modelSpend.OutputTokens
		spend.ReasoningOutputTokens += modelSpend.ReasoningOutputTokens
		spend.TotalTokens += modelSpend.TotalTokens
		spend.Sessions += modelSpend.Sessions
		spend.ByModel = append(spend.ByModel, modelSpend)
	}
	return spend, nil
}

func (s *sqliteStore) RecentModelTokenQuantiles(ctx context.Context, query ModelTokenQuantileQuery) (ModelTokenQuantiles, error) {
	model := strings.TrimSpace(query.Model)
	if model == "" {
		return ModelTokenQuantiles{}, nil
	}
	limit := nonNegative(query.Limit)
	if limit == 0 {
		limit = 50
	}

	row, err := s.queries.RecentModelTokenQuantiles(ctx, sqlc.RecentModelTokenQuantilesParams{
		Model: model,
		Limit: limit,
	})
	if err != nil {
		return ModelTokenQuantiles{}, fmt.Errorf("reading model token quantiles: %w", err)
	}
	return ModelTokenQuantiles{
		Model:                model,
		Sessions:             row.Sessions,
		P50InputTokens:       row.P50InputTokens,
		P90InputTokens:       row.P90InputTokens,
		P50CachedInputTokens: row.P50CachedInputTokens,
		P90CachedInputTokens: row.P90CachedInputTokens,
		P50OutputTokens:      row.P50OutputTokens,
		P90OutputTokens:      row.P90OutputTokens,
		P50TotalTokens:       row.P50TotalTokens,
		P90TotalTokens:       row.P90TotalTokens,
	}, nil
}

func (s *sqliteStore) CycleTimeReport(ctx context.Context) (CycleTimeReport, error) {
	rows, err := s.queries.CompletedIssueCycleRows(ctx)
	if err != nil {
		return CycleTimeReport{}, fmt.Errorf("reading cycle time report: %w", err)
	}

	issues := make([]CycleTimeIssue, 0, len(rows))
	for _, row := range rows {
		startedAt, err := parseTimestamp("started_at", row.StartedAt)
		if err != nil {
			return CycleTimeReport{}, err
		}
		completedAt, err := parseTimestamp("completed_at", row.CompletedAt)
		if err != nil {
			return CycleTimeReport{}, err
		}
		seconds, ok := cycleTimeSeconds(startedAt, completedAt)
		if !ok {
			continue
		}
		issues = append(issues, CycleTimeIssue{
			Key:             row.IssueKey,
			StartedAt:       startedAt,
			CompletedAt:     completedAt,
			DurationSeconds: seconds,
			Sessions:        row.Sessions,
		})
	}

	return CycleTimeReport{
		Issues:         issues,
		Buckets:        cycleTimeBuckets(issues),
		AverageSeconds: averageCycleTimeSeconds(issues),
	}, nil
}

func (s *sqliteStore) ListFairShareUsage(ctx context.Context) ([]FairShareUsage, error) {
	rows, err := s.queries.ListFairShareUsage(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading fair-share usage: %w", err)
	}

	usage := make([]FairShareUsage, 0, len(rows))
	for _, row := range rows {
		updatedAt, err := parseTimestamp("updated_at", row.UpdatedAt)
		if err != nil {
			return nil, err
		}
		usage = append(usage, FairShareUsage{
			ProjectID:      row.ProjectID,
			Weight:         int(row.Weight),
			Dispatches:     row.Dispatches,
			RuntimeSeconds: row.RuntimeSeconds,
			UpdatedAt:      updatedAt,
		})
	}
	return usage, nil
}

func (s *sqliteStore) RecordFairShareDispatch(ctx context.Context, attrs FairShareDispatch) error {
	projectID := strings.TrimSpace(attrs.ProjectID)
	if projectID == "" {
		return errors.New("project_id is required")
	}

	dispatchedAt, err := requiredTimestamp("dispatched_at", attrs.DispatchedAt)
	if err != nil {
		return err
	}

	_, err = s.queries.UpsertFairShareUsage(ctx, sqlc.UpsertFairShareUsageParams{
		ProjectID:      projectID,
		Weight:         int64(positiveWeight(attrs.Weight)),
		RuntimeSeconds: nonNegative(attrs.RuntimeSeconds),
		UpdatedAt:      dispatchedAt,
	})
	if err != nil {
		return fmt.Errorf("recording fair-share dispatch: %w", err)
	}
	return nil
}

func normalizeIssueIdentity(identity IssueIdentity) IssueIdentity {
	return IssueIdentity{
		ProjectID:  strings.TrimSpace(identity.ProjectID),
		IssueID:    strings.TrimSpace(identity.IssueID),
		Identifier: strings.TrimSpace(identity.Identifier),
		IssueURL:   strings.TrimSpace(identity.IssueURL),
	}
}

func budgetCostProjectFilter(projectIDs []string) func(string) bool {
	allowed := make(map[string]struct{}, len(projectIDs))
	for _, projectID := range projectIDs {
		projectID = strings.TrimSpace(projectID)
		if projectID == "" {
			continue
		}
		allowed[projectID] = struct{}{}
	}
	if len(allowed) == 0 {
		return func(string) bool {
			return true
		}
	}
	return func(projectID string) bool {
		_, ok := allowed[strings.TrimSpace(projectID)]
		return ok
	}
}

func normalizeUsageReportGroup(group UsageReportGroup) (UsageReportGroup, error) {
	switch group {
	case "", UsageReportByDay:
		return UsageReportByDay, nil
	case UsageReportByProject, UsageReportByIssue, UsageReportByPR, UsageReportByModel:
		return group, nil
	default:
		return "", fmt.Errorf("unsupported usage report group %q", group)
	}
}

func configureSQLite(ctx context.Context, db *sql.DB, busyTimeoutMillis int64) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMillis)); err != nil {
		return fmt.Errorf("setting sqlite busy_timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enabling sqlite foreign keys: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		return fmt.Errorf("enabling sqlite WAL: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging sqlite database: %w", err)
	}
	return nil
}

func (s *sqliteStore) runtimeMigrationVersion(ctx context.Context) (int64, error) {
	var version sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied = 1").Scan(&version); err != nil {
		return 0, fmt.Errorf("reading migration status: %w", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

func (s *sqliteStore) runtimeTableCount(ctx context.Context, tableName string, projectScoped bool, projectID string) (int64, error) {
	var row *sql.Row
	if projectScoped && projectID != "" {
		switch tableName {
		case "fair_share_usage":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fair_share_usage WHERE project_id = ?", projectID)
		case "usage_events":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_events WHERE project_id = ?", projectID)
		case "workflow_phase_events":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_phase_events WHERE project_id = ?", projectID)
		case "efficiency_receipts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM efficiency_receipts WHERE project_id = ?", projectID)
		case "work_attempts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM work_attempts WHERE project_id = ?", projectID)
		case "scheduler_decisions":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scheduler_decisions WHERE project_id = ?", projectID)
		case "merge_required_check_streaks":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM merge_required_check_streaks WHERE project_id = ?", projectID)
		case "validator_verdicts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM validator_verdicts WHERE project_id = ?", projectID)
		case "security_audit_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM security_audit_runs WHERE project_id = ?", projectID)
		case "retro_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM retro_runs WHERE project_id = ?", projectID)
		case "routine_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM routine_runs WHERE project_id = ?", projectID)
		case "backlog_admission_proposals":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_proposals WHERE project_id = ?", projectID)
		case "backlog_admission_declines":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_declines WHERE project_id = ?", projectID)
		case "backlog_admission_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_runs WHERE project_id = ?", projectID)
		case "backlog_admission_malformed_results":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_malformed_results WHERE project_id = ?", projectID)
		case "scheduled_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scheduled_runs WHERE project_id = ?", projectID)
		default:
			return 0, fmt.Errorf("unsupported project-scoped runtime table %q", tableName)
		}
	} else {
		switch tableName {
		case "detent_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM detent_runs")
		case "codex_sessions":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM codex_sessions")
		case "fair_share_usage":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fair_share_usage")
		case "usage_events":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_events")
		case "workflow_phase_events":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_phase_events")
		case "efficiency_receipts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM efficiency_receipts")
		case "work_attempts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM work_attempts")
		case "scheduler_decisions":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scheduler_decisions")
		case "merge_required_check_streaks":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM merge_required_check_streaks")
		case "validator_verdicts":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM validator_verdicts")
		case "security_audit_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM security_audit_runs")
		case "security_audit_dispositions":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM security_audit_dispositions")
		case "retro_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM retro_runs")
		case "routine_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM routine_runs")
		case "backlog_admission_proposals":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_proposals")
		case "backlog_admission_declines":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_declines")
		case "backlog_admission_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_runs")
		case "backlog_admission_malformed_results":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backlog_admission_malformed_results")
		case "scheduled_runs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scheduled_runs")
		case "health_notification_states":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM health_notification_states")
		case "api_keys":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM api_keys")
		case "api_usage_logs":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM api_usage_logs")
		case "auth_magic_links":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM auth_magic_links")
		case "auth_sessions":
			row = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM auth_sessions")
		default:
			return 0, fmt.Errorf("unsupported runtime table %q", tableName)
		}
	}

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("counting runtime table %s: %w", tableName, err)
	}
	return count, nil
}

func (s *sqliteStore) runtimeWorkflowPhaseEventEvidence(ctx context.Context, projectID string) (RuntimeWorkflowPhaseEventEvidence, error) {
	var row *sql.Row
	if projectID != "" {
		row = s.db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(finished_at), MAX(finished_at) FROM workflow_phase_events WHERE project_id = ?", projectID)
	} else {
		row = s.db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(finished_at), MAX(finished_at) FROM workflow_phase_events")
	}

	var count int64
	var oldest sql.NullString
	var newest sql.NullString
	if err := row.Scan(&count, &oldest, &newest); err != nil {
		return RuntimeWorkflowPhaseEventEvidence{}, fmt.Errorf("reading workflow phase event evidence: %w", err)
	}
	return RuntimeWorkflowPhaseEventEvidence{
		RowCount:         count,
		OldestFinishedAt: nullableRuntimeTimestamp(oldest),
		NewestFinishedAt: nullableRuntimeTimestamp(newest),
	}, nil
}

func nullableRuntimeTimestamp(value sql.NullString) *time.Time {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value.String))
	if err != nil {
		return nil
	}
	return &parsed
}

func requiredTimestamp(name string, value time.Time) (string, error) {
	if value.IsZero() {
		return "", fmt.Errorf("%s is required", name)
	}
	return value.UTC().Truncate(time.Second).Format(time.RFC3339), nil
}

func dateString(value time.Time) (string, error) {
	if value.IsZero() {
		return "", errors.New("date is required")
	}
	return value.Format("2006-01-02"), nil
}

func optionalDateString(value time.Time) (string, error) {
	if value.IsZero() {
		return "", nil
	}
	return dateString(value)
}

func parseTimestamp(name string, value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, fmt.Errorf("%s is required", name)
	}

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func nullString(value string) sql.NullString {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func nullIdentityValue(value agentidentity.Value) sql.NullString {
	return nullString(value.Normalize().Value)
}

func nullIdentityProvenance(value agentidentity.Value) sql.NullString {
	return nullString(string(value.Normalize().Provenance))
}

func nullIdentityObservedAt(value *time.Time) sql.NullString {
	if value == nil || value.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{String: value.UTC().Format(time.RFC3339Nano), Valid: true}
}

func nullPositiveInt64(value int64) sql.NullInt64 {
	if value <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: value, Valid: true}
}

func nullOptionalInt64(value *int64) sql.NullInt64 {
	if value == nil || *value <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func nullNonNegativeInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: nonNegative(value), Valid: true}
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func nonNegativeFloat(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}

func nullableNonNegativeFloat(value *float64) sql.NullFloat64 {
	if value == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: nonNegativeFloat(*value), Valid: true}
}

func positiveWeight(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func requireAffected(rows int64, name string, id int64) error {
	if rows == 0 {
		return fmt.Errorf("%w: %s %d", ErrNotFound, name, id)
	}
	return nil
}
