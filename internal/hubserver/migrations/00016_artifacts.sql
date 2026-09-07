-- +goose Up
CREATE TABLE artifact_services (
  organization_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  id TEXT NOT NULL,
  binding_json TEXT NOT NULL CHECK(json_valid(binding_json)),
  publisher_token_id TEXT NOT NULL REFERENCES api_tokens(id),
  PRIMARY KEY(organization_id,project_id,id),
  FOREIGN KEY(organization_id,project_id) REFERENCES projects(organization_id,id)
);
CREATE TABLE artifact_references (
  organization_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  work_item_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  manifest_id TEXT NOT NULL UNIQUE,
  reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),
  PRIMARY KEY(organization_id,project_id,artifact_id,revision),
  FOREIGN KEY(organization_id,project_id,service_id) REFERENCES artifact_services(organization_id,project_id,id),
  FOREIGN KEY(organization_id,project_id,work_item_id) REFERENCES issues(organization_id,project_id,native_id)
);
CREATE TABLE artifact_grants (
  token_hash TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL REFERENCES api_tokens(id),
  organization_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE INDEX artifact_grants_expiry ON artifact_grants(expires_at);
-- +goose StatementBegin
CREATE TRIGGER artifact_reference_immutable BEFORE UPDATE ON artifact_references BEGIN SELECT RAISE(ABORT,'immutable artifact reference'); END;
-- +goose StatementEnd
