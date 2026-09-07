-- +goose Up
CREATE TABLE uploads (
  id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  request_json TEXT NOT NULL CHECK(json_valid(request_json)),
  state TEXT NOT NULL CHECK(state IN ('uploading','complete','interrupted','deletion_pending','deleted')),
  reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes >= 0),
  retained_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retained_bytes >= 0),
  created_at INTEGER NOT NULL,
  upload_deadline INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  deleted_at INTEGER,
  UNIQUE(organization_id,project_id,idempotency_key)
);
CREATE TABLE objects (
  upload_id TEXT NOT NULL REFERENCES uploads(id),
  sequence INTEGER NOT NULL,
  id TEXT NOT NULL UNIQUE,
  descriptor_json TEXT NOT NULL CHECK(json_valid(descriptor_json)),
  storage_version TEXT NOT NULL DEFAULT '',
  verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN (0,1)),
  PRIMARY KEY(upload_id,sequence)
);
CREATE TABLE manifests (
  upload_id TEXT NOT NULL REFERENCES uploads(id),
  revision INTEGER NOT NULL,
  id TEXT NOT NULL UNIQUE,
  body BLOB NOT NULL,
  digest TEXT NOT NULL,
  reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),
  PRIMARY KEY(upload_id,revision)
);
CREATE TABLE outbox (
  manifest_id TEXT PRIMARY KEY REFERENCES manifests(id),
  delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0,1))
);
CREATE INDEX uploads_expiry ON uploads(expires_at);
CREATE INDEX uploads_usage ON uploads(organization_id,state);
-- +goose StatementBegin
CREATE TRIGGER immutable_manifest BEFORE UPDATE ON manifests BEGIN SELECT RAISE(ABORT,'immutable manifest'); END;
-- +goose StatementEnd
