-- +goose Up
CREATE TABLE session_progress (
  session_id INTEGER PRIMARY KEY REFERENCES codex_sessions(id) ON DELETE CASCADE,
  observation_json TEXT NOT NULL
);

-- +goose Down
DROP TABLE session_progress;
