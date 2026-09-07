package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
)

type AIDebugSessionReader interface {
	ListIssueAIDebugSessions(context.Context, IssueIdentity) ([]AIDebugSession, error)
}

type AIDebugAttemptReader interface {
	ListIssueAIDebugWorkAttempts(context.Context, IssueIdentity) ([]WorkAttempt, error)
}

type AIDebugSession struct {
	RuntimeIdentity       agentidentity.Identity `json:"runtime_identity,omitzero"`
	ID                    int64                  `json:"id"`
	WorkAttemptID         int64                  `json:"work_attempt_id,omitempty"`
	StartedAt             *time.Time             `json:"started_at,omitempty"`
	CompletedAt           *time.Time             `json:"completed_at,omitempty"`
	InputTokens           int64                  `json:"input_tokens"`
	CachedInputTokens     int64                  `json:"cached_input_tokens"`
	OutputTokens          int64                  `json:"output_tokens"`
	ReasoningOutputTokens int64                  `json:"reasoning_output_tokens"`
	TotalTokens           int64                  `json:"total_tokens"`
	Turns                 int64                  `json:"turns"`
	Model                 string                 `json:"model,omitempty"`
	RequestedModel        string                 `json:"requested_model,omitempty"`
	Effort                string                 `json:"effort,omitempty"`
	RuntimeSeconds        int64                  `json:"runtime_seconds"`
	FinalState            string                 `json:"final_state,omitempty"`
	ResumedFromSessionID  int64                  `json:"resumed_from_session_id,omitempty"`
	ProviderSessionID     string                 `json:"provider_session_id,omitempty"`
}

func (s *sqliteStore) ListIssueAIDebugWorkAttempts(ctx context.Context, identity IssueIdentity) ([]WorkAttempt, error) {
	identity = normalizeIssueIdentity(identity)
	if identity.ProjectID == "" {
		return nil, ErrProjectRequired
	}
	if identity.IssueID == "" && identity.Identifier == "" && identity.IssueURL == "" {
		return []WorkAttempt{}, nil
	}
	rows, err := s.queries.ListIssueWorkAttempts(ctx, sqlc.ListIssueWorkAttemptsParams{
		ProjectID:  identity.ProjectID,
		IssueID:    identity.IssueID,
		Identifier: identity.Identifier,
		IssueURL:   identity.IssueURL,
	})
	if err != nil {
		return nil, fmt.Errorf("reading AI debug work attempts: %w", err)
	}
	return workAttemptsFromRows(rows)
}

func (s *sqliteStore) ListIssueAIDebugSessions(ctx context.Context, identity IssueIdentity) ([]AIDebugSession, error) {
	identity = normalizeIssueIdentity(identity)
	if identity.ProjectID == "" {
		return nil, ErrProjectRequired
	}
	if identity.IssueID == "" && identity.Identifier == "" && identity.IssueURL == "" {
		return []AIDebugSession{}, nil
	}
	rows, err := s.queries.ListIssueCodexSessions(ctx, sqlc.ListIssueCodexSessionsParams{
		ProjectID:  sql.NullString{String: identity.ProjectID, Valid: true},
		IssueID:    identity.IssueID,
		Identifier: identity.Identifier,
		IssueURL:   identity.IssueURL,
	})
	if err != nil {
		return nil, fmt.Errorf("reading AI debug sessions: %w", err)
	}
	sessions := make([]AIDebugSession, 0, len(rows))
	for _, row := range rows {
		startedAtValue, err := aiDebugSessionTime("started_at", row.StartedAt)
		if err != nil {
			return nil, err
		}
		completedAtValue, err := aiDebugSessionTime("completed_at", row.CompletedAt)
		if err != nil {
			return nil, err
		}
		startedAt := aiDebugTimePointer(startedAtValue)
		completedAt := aiDebugTimePointer(completedAtValue)
		runtimeIdentity, err := unmarshalRuntimeIdentity(row.RuntimeIdentityJson.String)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, AIDebugSession{
			RuntimeIdentity:       runtimeIdentity,
			ID:                    row.ID,
			WorkAttemptID:         row.WorkAttemptID.Int64,
			StartedAt:             startedAt,
			CompletedAt:           completedAt,
			InputTokens:           row.InputTokens,
			CachedInputTokens:     row.CachedInputTokens.Int64,
			OutputTokens:          row.OutputTokens,
			ReasoningOutputTokens: row.ReasoningOutputTokens.Int64,
			TotalTokens:           row.TotalTokens,
			Turns:                 row.Turns,
			Model:                 strings.TrimSpace(row.Model.String),
			RequestedModel:        strings.TrimSpace(row.RequestedModel.String),
			Effort:                strings.TrimSpace(row.ReasoningEffort.String),
			RuntimeSeconds:        row.RuntimeSeconds,
			FinalState:            strings.TrimSpace(row.FinalState.String),
			ResumedFromSessionID:  row.ResumedFromSessionID.Int64,
			ProviderSessionID:     strings.TrimSpace(row.ProviderSessionID.String),
		})
	}
	return sessions, nil
}

func aiDebugSessionTime(name string, value sql.NullString) (time.Time, error) {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, nil
	}
	parsed, err := parseTimestamp(name, value.String)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func aiDebugTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}
