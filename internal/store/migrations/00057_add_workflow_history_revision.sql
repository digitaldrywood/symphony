-- +goose Up
CREATE TABLE workflow_history_revision (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  revision INTEGER NOT NULL DEFAULT 0
);
INSERT INTO workflow_history_revision (id) VALUES (1);

-- +goose StatementBegin
CREATE TRIGGER workflow_history_insert AFTER INSERT ON workflow_phase_events
BEGIN
  UPDATE workflow_history_revision SET revision = revision + 1 WHERE id = 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_history_update AFTER UPDATE ON workflow_phase_events
BEGIN
  UPDATE workflow_history_revision SET revision = revision + 1 WHERE id = 1;
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_history_delete AFTER DELETE ON workflow_phase_events
BEGIN
  UPDATE workflow_history_revision SET revision = revision + 1 WHERE id = 1;
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER workflow_history_delete;
DROP TRIGGER workflow_history_update;
DROP TRIGGER workflow_history_insert;
DROP TABLE workflow_history_revision;
