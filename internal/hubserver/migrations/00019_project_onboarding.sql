-- +goose Up
CREATE TABLE IF NOT EXISTS project_onboarding (
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    progress_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (organization_id, project_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
);

CREATE TABLE IF NOT EXISTS change_viewed_files (
  version_id TEXT NOT NULL REFERENCES change_versions(id),
  principal_id TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  file_sha256 TEXT NOT NULL,
  viewed INTEGER NOT NULL CHECK (viewed IN (0, 1)),
  PRIMARY KEY (version_id, principal_id, manifest_sha256, file_sha256)
);
