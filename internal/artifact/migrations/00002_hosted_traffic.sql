-- +goose Up
CREATE TABLE hosted_traffic (
  minute INTEGER PRIMARY KEY,
  upload_bytes INTEGER NOT NULL DEFAULT 0,
  download_bytes INTEGER NOT NULL DEFAULT 0,
  requests INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE uploads ADD COLUMN admitted_upload_bytes INTEGER NOT NULL DEFAULT 0;
CREATE TABLE hosted_traffic_settings (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  window_seconds INTEGER NOT NULL,
  retention_seconds INTEGER NOT NULL
);
INSERT INTO hosted_traffic_settings VALUES(1,3600,86400);
