package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type SessionProgressStore interface {
	SessionProgress(context.Context, int64) (SessionProgress, error)
	SaveSessionProgress(context.Context, int64, SessionProgress) error
}

type SessionProgress struct {
	HeadSHA              string    `json:"head_sha,omitempty"`
	CommitFingerprint    string    `json:"commit_fingerprint,omitempty"`
	TrackedFingerprint   string    `json:"tracked_fingerprint,omitempty"`
	WorkspaceFingerprint string    `json:"workspace_fingerprint,omitempty"`
	WorkpadFingerprint   string    `json:"workpad_fingerprint,omitempty"`
	Local                bool      `json:"local,omitempty"`
	LastProgressAt       time.Time `json:"last_progress_at"`
}

func (s *sqliteStore) SessionProgress(ctx context.Context, sessionID int64) (SessionProgress, error) {
	var data string
	err := s.db.QueryRowContext(ctx, "SELECT observation_json FROM session_progress WHERE session_id = ?", sessionID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionProgress{}, ErrNotFound
	}
	if err != nil {
		return SessionProgress{}, fmt.Errorf("read session progress: %w", err)
	}
	var progress SessionProgress
	if err := json.Unmarshal([]byte(data), &progress); err != nil {
		return SessionProgress{}, fmt.Errorf("decode session progress: %w", err)
	}
	if progress.LastProgressAt.IsZero() {
		return SessionProgress{}, errors.New("session progress timestamp is required")
	}
	return progress, nil
}

func (s *sqliteStore) SaveSessionProgress(ctx context.Context, sessionID int64, progress SessionProgress) error {
	if sessionID <= 0 {
		return ErrNotFound
	}
	if progress.LastProgressAt.IsZero() {
		return errors.New("session progress timestamp is required")
	}
	data, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("encode session progress: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO session_progress (session_id, observation_json) VALUES (?, ?)
ON CONFLICT(session_id) DO UPDATE SET observation_json = excluded.observation_json
`, sessionID, string(data))
	if err != nil {
		return fmt.Errorf("save session progress: %w", err)
	}
	return nil
}
