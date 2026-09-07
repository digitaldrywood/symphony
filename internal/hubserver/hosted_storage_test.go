package hubserver

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func hostedStorageConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		DatabasePath: filepath.Join(t.TempDir(), "hub.db"),
		Logger:       discardLogger(),
		Hosted: &HostedConfig{
			OrganizationID:       "org_hosted",
			WorkOSOrganizationID: "org_provider",
			PublicURL:            "https://tenant.example.test",
		},
	}
}

func openHostedStorage(t *testing.T, cfg Config) *database {
	t.Helper()
	db, err := openDatabase(t.Context(), cfg.normalized())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestHostedDatabaseBindingRestart(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		change    func(*Config)
		wantError bool
	}{
		{name: "same binding"},
		{name: "local reopen", change: func(c *Config) { c.Hosted = nil }, wantError: true},
		{name: "other organization", change: func(c *Config) { c.Hosted.OrganizationID = "org_other" }, wantError: true},
		{name: "other provider", change: func(c *Config) { c.Hosted.WorkOSOrganizationID = "org_other_provider" }, wantError: true},
		{name: "missing provider", change: func(c *Config) { c.Hosted.WorkOSOrganizationID = "" }, wantError: true},
		{name: "other host", change: func(c *Config) { c.Hosted.PublicURL = "https://other.example.test" }, wantError: true},
		{name: "other bootstrap subject", change: func(c *Config) { c.Hosted.BootstrapSubject = "user_other" }, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := hostedStorageConfig(t)
			db := openHostedStorage(t, cfg)
			seedProjection(t, db.db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if test.change != nil {
				test.change(&cfg)
			}
			reopened, err := openDatabase(t.Context(), cfg.normalized())
			if test.wantError {
				if !errors.Is(err, ErrHostedDatabaseBinding) {
					t.Fatalf("reopen error = %v", err)
				}
				if reopened != nil {
					t.Fatal("rejected binding returned a database")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			var organization string
			if err := reopened.db.QueryRowContext(t.Context(), "SELECT organization_id FROM projects").Scan(&organization); err != nil {
				t.Fatal(err)
			}
			if organization != cfg.Hosted.OrganizationID || string(reopened.hostedOrganization) != organization {
				t.Fatalf("project organization = %q, bound organization = %q", organization, reopened.hostedOrganization)
			}
		})
	}
}

func TestHostedDatabaseBootstrapBinding(t *testing.T) {
	t.Parallel()
	cfg := hostedStorageConfig(t)
	cfg.Hosted.WorkOSOrganizationID = ""
	cfg.Hosted.BootstrapSubject = "user_creator"
	db := openHostedStorage(t, cfg)
	if _, err := db.db.ExecContext(t.Context(), "UPDATE hosted_tenant SET provider_id = 'org_created' WHERE singleton = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), "UPDATE hosted_tenant SET provider_id = 'org_replacement' WHERE singleton = 1"); err == nil {
		t.Fatal("provider rebinding was accepted")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openHostedStorage(t, cfg)
	report, err := db.hostedMetadata(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if report.ProviderOrganizationID != "org_created" {
		t.Fatalf("provider organization = %q", report.ProviderOrganizationID)
	}
}

func TestHostedDatabaseRejectsExistingContent(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		statement string
		wantError bool
	}{
		{name: "empty local database"},
		{name: "extra organization", statement: "INSERT INTO organizations (id, name, created_at) VALUES ('org_other', 'content-sentinel', '2026-09-01T00:00:00Z')", wantError: true},
		{name: "project", statement: "INSERT INTO projects (organization_id, name, profile, created_at) SELECT id, 'content-sentinel', 'native', '2026-09-01T00:00:00Z' FROM organizations", wantError: true},
		{name: "session", statement: "INSERT INTO hosted_sessions (token_hash, email, identity_json, expires_at, created_at) VALUES ('hash', 'content-sentinel', '{}', '2026-09-02T00:00:00Z', '2026-09-01T00:00:00Z')", wantError: true},
		{name: "missing local organization", statement: "UPDATE organizations SET local = 0", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := hostedStorageConfig(t)
			local := cfg
			local.Hosted = nil
			db := openHostedStorage(t, local)
			if test.statement != "" {
				if _, err := db.db.ExecContext(t.Context(), test.statement); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			bound, err := openDatabase(t.Context(), cfg.normalized())
			if test.wantError {
				if !errors.Is(err, ErrHostedDatabaseBinding) || strings.Contains(err.Error(), "content-sentinel") {
					t.Fatalf("binding error = %v", err)
				}
				localDB := openHostedStorage(t, local)
				var count int
				if err := localDB.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_tenant").Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatal("rejected binding persisted a tenant")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Close()
			if bound.hostedOrganization != "org_hosted" {
				t.Fatalf("organization = %q", bound.hostedOrganization)
			}
		})
	}
}

func TestHostedDatabaseRejectsInvalidBinding(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"organization", "public URL", "provider without bootstrap"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			cfg := hostedStorageConfig(t)
			switch field {
			case "organization":
				cfg.Hosted.OrganizationID = ""
			case "public URL":
				cfg.Hosted.PublicURL = ""
			case "provider without bootstrap":
				cfg.Hosted.WorkOSOrganizationID = ""
			}
			db, err := openDatabase(t.Context(), cfg.normalized())
			if !errors.Is(err, ErrHostedDatabaseBinding) || db != nil {
				t.Fatalf("database = %v, error = %v", db, err)
			}
		})
	}
}

func TestHostedDatabaseConstrainsBackgroundWrites(t *testing.T) {
	t.Parallel()
	db := openHostedStorage(t, hostedStorageConfig(t))
	seedProjection(t, db.db)
	for _, test := range []struct {
		name      string
		statement string
	}{
		{name: "additional organization", statement: "INSERT INTO organizations (id, name, created_at) VALUES ('org_other', 'Other', '2026-09-01T00:00:00Z')"},
		{name: "organization reassignment", statement: "UPDATE organizations SET id = 'org_other'"},
		{name: "repository trigger target", statement: "UPDATE organizations SET local = 0"},
		{name: "organization removal", statement: "DELETE FROM organizations"},
		{name: "tenant removal", statement: "DELETE FROM hosted_tenant"},
		{name: "tenant replacement", statement: "INSERT OR REPLACE INTO hosted_tenant (singleton, organization_id, provider_id, public_url) VALUES (1, 'org_hosted', 'org_replacement', 'https://other.example.test')"},
		{name: "tenant reassignment", statement: "UPDATE hosted_tenant SET organization_id = 'org_other'"},
		{name: "host reassignment", statement: "UPDATE hosted_tenant SET public_url = 'https://other.example.test'"},
		{name: "foreign project", statement: "INSERT INTO projects (id, organization_id, name, profile, created_at) VALUES ('prj_other', 'org_other', 'Other', 'native', '2026-09-01T00:00:00Z')"},
		{name: "project reassignment", statement: "UPDATE projects SET organization_id = 'org_other'"},
		{name: "issue reassignment", statement: "UPDATE issues SET organization_id = 'org_other'"},
		{name: "unscoped machine", statement: "INSERT INTO machines (id, hostname, capacity, version, last_heartbeat_at, registered_at, updated_at) VALUES ('machine', 'host', 1, 'v1', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.db.ExecContext(t.Context(), test.statement); err == nil {
				t.Fatal("cross-organization write was accepted")
			}
		})
	}
	targets, err := db.reconcileTargets(t.Context())
	if err != nil || len(targets) != 1 {
		t.Fatalf("background reconciliation targets = %d, error = %v", len(targets), err)
	}
}

func TestHostedMetadataExcludesCustomerContent(t *testing.T) {
	t.Parallel()
	db := openHostedStorage(t, hostedStorageConfig(t))
	_, issueID := seedProjection(t, db.db)
	const sentinel = "private-content-credential-sentinel"
	if err := db.ensureInitialAdminToken(t.Context(), []byte(testHubAdminToken)); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"UPDATE issues SET title = ?, body = ?, url = ?",
		"UPDATE repositories SET github_owner = ?, github_name = ?, config_json = json_object('credential', ?)",
		"UPDATE api_tokens SET name = ?, token_fingerprint = ?, last_used_at = ?",
	} {
		if _, err := db.db.ExecContext(t.Context(), statement, sentinel, sentinel, sentinel); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.db.ExecContext(t.Context(), "INSERT INTO hosted_members (user_id, email, membership_id, role, principal_id, created_at, updated_at) VALUES ('user', ?, 'membership', 'viewer', ?, ?, ?)", sentinel, bootstrapTokenID, testTimestamp, testTimestamp); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), "INSERT INTO hosted_sessions (token_hash, email, identity_json, expires_at, created_at) VALUES (?, ?, json_object('credential', ?), '2026-09-03T12:00:00Z', '2026-09-01T12:00:00.5Z')", sentinel, sentinel, sentinel); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `
INSERT INTO collaboration_events (id, organization_id, project_id, work_item_id, sequence, type, schema_version, actor_json, data_json, recorded_at)
SELECT 'event', organization_id, project_id, native_id, 1, 'issue.created', 1, json_object('actor', ?), json_object('body', ?), ?
FROM issues WHERE id = ?`, sentinel, sentinel, testTimestamp, issueID); err != nil {
		t.Fatal(err)
	}
	report, err := db.hostedMetadata(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	expectedActivity := time.Date(2026, 9, 1, 12, 0, 0, 500_000_000, time.UTC)
	if report.OrganizationID != "org_hosted" || report.ProviderOrganizationID != "org_provider" || report.Members != 1 || report.Projects != 1 || report.Events != 1 || report.Runners != 0 || !report.LastActivity.Equal(expectedActivity) || report.DatabaseBytes <= 0 || !report.Healthy {
		t.Fatalf("report = %#v", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), sentinel) || strings.Contains(string(encoded), testHubAdminToken) {
		t.Fatal("report contains customer content or credentials")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"organization_id": true, "provider_organization_id": true, "members": true, "projects": true, "runners": true, "events": true, "last_activity": true, "database_bytes": true, "healthy": true}
	for name := range fields {
		if !allowed[name] {
			t.Fatalf("unapproved report field %q", name)
		}
	}
}

func TestHostedMetadataFailsWithoutExposingErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		local bool
		fail  func(*testing.T, *database)
	}{
		{name: "local database", local: true},
		{name: "closed database", fail: func(t *testing.T, d *database) {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unavailable file", fail: func(t *testing.T, d *database) { d.path = filepath.Join(t.TempDir(), "private-path-sentinel") }},
		{name: "invalid activity", fail: func(t *testing.T, d *database) {
			if _, err := d.db.ExecContext(t.Context(), "INSERT INTO hosted_sessions (token_hash, email, identity_json, expires_at, created_at) VALUES ('hash', 'email', '{}', 'expiry', 'private-path-sentinel')"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := hostedStorageConfig(t)
			if test.local {
				cfg.Hosted = nil
			}
			db := openHostedStorage(t, cfg)
			if test.fail != nil {
				test.fail(t, db)
			}
			report, err := db.hostedMetadata(t.Context())
			if !errors.Is(err, ErrHostedMetadataUnavailable) || report != (HostedMetadata{}) || strings.Contains(err.Error(), "private-path-sentinel") {
				t.Fatalf("report = %#v, error = %v", report, err)
			}
		})
	}
}
