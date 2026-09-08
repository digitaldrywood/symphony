-- +goose Up
CREATE TABLE hosted_plans (
  id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK(version > 0),
  record_json TEXT NOT NULL CHECK(json_valid(record_json)),
  PRIMARY KEY(id,version)
);
CREATE TABLE hosted_plan_assignments (
  organization_id TEXT PRIMARY KEY REFERENCES organizations(id),
  base_id TEXT NOT NULL,
  base_version INTEGER NOT NULL,
  subscription_id TEXT,
  subscription_version INTEGER,
  subscription_expires_at TEXT,
  revision INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY(base_id,base_version) REFERENCES hosted_plans(id,version),
  FOREIGN KEY(subscription_id,subscription_version) REFERENCES hosted_plans(id,version)
);
CREATE TABLE hosted_complimentary_grants (
  id TEXT PRIMARY KEY,
  record_json TEXT NOT NULL CHECK(json_valid(record_json))
);
CREATE TABLE hosted_plan_audit (
  id INTEGER PRIMARY KEY,
  command_id TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  action TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  record_json TEXT NOT NULL CHECK(json_valid(record_json))
);
CREATE TABLE hosted_usage_windows (
  window_start INTEGER NOT NULL,
  metric TEXT NOT NULL,
  amount INTEGER NOT NULL CHECK(amount >= 0),
  PRIMARY KEY(window_start,metric)
);
CREATE TABLE hosted_member_reservations (
  email TEXT PRIMARY KEY,
  expires_at INTEGER NOT NULL
);
CREATE TABLE hosted_artifact_usage (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  service_id TEXT NOT NULL,
  usage_json TEXT NOT NULL CHECK(json_valid(usage_json)),
  observed_at INTEGER NOT NULL
);
