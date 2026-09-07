-- +goose Up
CREATE TABLE change_viewed_files (
  version_id TEXT NOT NULL REFERENCES change_versions(id),
  principal_id TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  file_sha256 TEXT NOT NULL,
  viewed INTEGER NOT NULL CHECK (viewed IN (0, 1)),
  PRIMARY KEY (version_id, principal_id, manifest_sha256, file_sha256)
);
