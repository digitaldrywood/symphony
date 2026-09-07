-- +goose Up
CREATE TABLE hosted_tenant (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  organization_id TEXT NOT NULL UNIQUE REFERENCES organizations(id),
  provider_id TEXT NOT NULL DEFAULT '',
  bootstrap_subject TEXT NOT NULL DEFAULT '',
  public_url TEXT NOT NULL
);

CREATE TABLE hosted_members (
  user_id TEXT PRIMARY KEY NOT NULL,
  email TEXT NOT NULL,
  membership_id TEXT NOT NULL UNIQUE,
  role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member', 'viewer')),
  active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
  principal_id TEXT NOT NULL UNIQUE REFERENCES api_tokens(id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE hosted_project_grants (
  user_id TEXT NOT NULL REFERENCES hosted_members(user_id),
  organization_id TEXT NOT NULL REFERENCES organizations(id),
  project_id TEXT NOT NULL,
  can_write INTEGER NOT NULL DEFAULT 0 CHECK (can_write IN (0, 1)),
  manage_runner INTEGER NOT NULL DEFAULT 0 CHECK (manage_runner IN (0, 1)),
  PRIMARY KEY (user_id, project_id),
  FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
);

CREATE TABLE hosted_sessions (
  token_hash TEXT PRIMARY KEY NOT NULL,
  email TEXT NOT NULL,
  identity_json TEXT NOT NULL CHECK (json_valid(identity_json)),
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT
);

CREATE TABLE hosted_transactions (
  token_hash TEXT PRIMARY KEY NOT NULL,
  state TEXT NOT NULL,
  verifier TEXT NOT NULL,
  organization_id TEXT NOT NULL,
  support_actor TEXT NOT NULL DEFAULT '',
  support_session TEXT NOT NULL DEFAULT '',
  invitation_token TEXT NOT NULL DEFAULT '',
  expires_at TEXT NOT NULL,
  consumed_at TEXT
);

CREATE TABLE hosted_invitations (
  id TEXT PRIMARY KEY NOT NULL,
  email TEXT NOT NULL,
  organization_id TEXT NOT NULL REFERENCES organizations(id),
  role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member', 'viewer')),
  accepted_user_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE TABLE hosted_audit (
  id INTEGER PRIMARY KEY,
  organization_id TEXT NOT NULL REFERENCES organizations(id),
  session_id TEXT NOT NULL,
  actual_actor TEXT NOT NULL,
  effective_user TEXT NOT NULL,
  reason TEXT NOT NULL,
  event TEXT NOT NULL,
  route TEXT NOT NULL,
  project_id TEXT NOT NULL,
  status INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL
);

CREATE INDEX hosted_sessions_expiry_idx ON hosted_sessions(expires_at);
CREATE INDEX hosted_audit_session_idx ON hosted_audit(session_id, id);

-- +goose StatementBegin
CREATE TRIGGER hosted_tenant_binding_insert BEFORE INSERT ON hosted_tenant
WHEN EXISTS (SELECT 1 FROM hosted_tenant)
  OR (SELECT count(*) FROM organizations) != 1
  OR NOT EXISTS (SELECT 1 FROM organizations WHERE id = NEW.organization_id AND local = 1)
BEGIN
  SELECT RAISE(ABORT, 'hosted organization binding is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_tenant_binding_update BEFORE UPDATE ON hosted_tenant
WHEN NEW.singleton IS NOT OLD.singleton
  OR NEW.organization_id IS NOT OLD.organization_id
  OR NEW.public_url IS NOT OLD.public_url
  OR NEW.bootstrap_subject IS NOT OLD.bootstrap_subject
  OR (OLD.provider_id != '' AND NEW.provider_id IS NOT OLD.provider_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted organization binding is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_tenant_binding_delete BEFORE DELETE ON hosted_tenant
BEGIN
  SELECT RAISE(ABORT, 'hosted organization binding is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_organization_insert BEFORE INSERT ON organizations
WHEN EXISTS (SELECT 1 FROM hosted_tenant)
BEGIN
  SELECT RAISE(ABORT, 'hosted organization is isolated');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_organization_update BEFORE UPDATE ON organizations
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.id OR NEW.local != 1)
BEGIN
  SELECT RAISE(ABORT, 'hosted organization is isolated');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_organization_delete BEFORE DELETE ON organizations
WHEN EXISTS (SELECT 1 FROM hosted_tenant)
BEGIN
  SELECT RAISE(ABORT, 'hosted organization is isolated');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_project_insert BEFORE INSERT ON projects
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.organization_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted project organization is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_project_update BEFORE UPDATE OF organization_id ON projects
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.organization_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted project organization is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_machine_insert BEFORE INSERT ON machines
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.organization_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted machine organization is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_machine_update BEFORE UPDATE OF organization_id ON machines
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.organization_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted machine organization is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_issue_insert BEFORE INSERT ON issues
WHEN EXISTS (
  SELECT 1 FROM hosted_tenant h
  WHERE (NEW.organization_id IS NOT NULL AND NEW.organization_id IS NOT h.organization_id)
    OR (NEW.organization_id IS NULL AND NOT EXISTS (
      SELECT 1 FROM projects p WHERE p.repository_id = NEW.repository_id AND p.organization_id = h.organization_id
    ))
)
BEGIN
  SELECT RAISE(ABORT, 'hosted issue organization is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER hosted_issue_update BEFORE UPDATE OF organization_id ON issues
WHEN EXISTS (SELECT 1 FROM hosted_tenant WHERE organization_id IS NOT NEW.organization_id)
BEGIN
  SELECT RAISE(ABORT, 'hosted issue organization is invalid');
END;
-- +goose StatementEnd
