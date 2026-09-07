-- +goose Up
ALTER TABLE codex_sessions ADD COLUMN runtime_identity_json TEXT;

-- +goose Down
ALTER TABLE codex_sessions DROP COLUMN runtime_identity_json;
