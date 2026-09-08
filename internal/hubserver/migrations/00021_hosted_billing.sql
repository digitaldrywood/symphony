-- +goose Up
CREATE TABLE hosted_billing_accounts (
  organization_id TEXT PRIMARY KEY REFERENCES organizations(id),
  account_id TEXT NOT NULL,
  customer_id TEXT NOT NULL UNIQUE,
  state_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(state_json)),
  checkout_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(checkout_json)),
  reconciled_at TEXT
);
CREATE TABLE hosted_billing_prices (
  price_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_version INTEGER NOT NULL,
  FOREIGN KEY(plan_id,plan_version) REFERENCES hosted_plans(id,version)
);
CREATE TABLE hosted_billing_events (
  sequence INTEGER PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  received_at TEXT NOT NULL,
  processed_at TEXT
);
CREATE INDEX hosted_billing_events_pending ON hosted_billing_events(sequence) WHERE processed_at IS NULL;
CREATE TABLE hosted_billing_audit (
  id INTEGER PRIMARY KEY,
  organization_id TEXT NOT NULL REFERENCES organizations(id),
  actor_id TEXT NOT NULL,
  action TEXT NOT NULL,
  record_json TEXT NOT NULL CHECK(json_valid(record_json)),
  recorded_at TEXT NOT NULL
);
