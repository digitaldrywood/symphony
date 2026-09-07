package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"github.com/digitaldrywood/detent/internal/tracker"
)

var (
	ErrHostedDatabaseBinding     = errors.New("hosted database organization binding is invalid")
	ErrHostedMetadataUnavailable = errors.New("hosted metadata is unavailable")
)

type HostedMetadata struct {
	OrganizationID         tracker.OrganizationID `json:"organization_id"`
	ProviderOrganizationID string                 `json:"provider_organization_id"`
	Members                int                    `json:"members"`
	Projects               int                    `json:"projects"`
	Runners                int                    `json:"runners"`
	Events                 int                    `json:"events"`
	LastActivity           time.Time              `json:"last_activity"`
	DatabaseBytes          int64                  `json:"database_bytes"`
	Healthy                bool                   `json:"healthy"`
}

func (d *database) bindHostedDatabase(ctx context.Context, cfg *HostedConfig) error {
	var organization, provider, bootstrap, publicURL string
	err := d.db.QueryRowContext(ctx, "SELECT organization_id, provider_id, bootstrap_subject, public_url FROM hosted_tenant WHERE singleton = 1").Scan(&organization, &provider, &bootstrap, &publicURL)
	if err == nil {
		if cfg == nil || organization != cfg.OrganizationID || publicURL != cfg.PublicURL || bootstrap != cfg.BootstrapSubject {
			return ErrHostedDatabaseBinding
		}
		if cfg.WorkOSOrganizationID != provider && (cfg.WorkOSOrganizationID != "" || bootstrap == "") {
			return ErrHostedDatabaseBinding
		}
		d.hostedOrganization = tracker.OrganizationID(organization)
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ErrHostedDatabaseBinding
	}
	if cfg == nil {
		return nil
	}
	if cfg.OrganizationID == "" || cfg.PublicURL == "" || cfg.WorkOSOrganizationID == "" && cfg.BootstrapSubject == "" {
		return ErrHostedDatabaseBinding
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return ErrHostedDatabaseBinding
	}
	defer tx.Rollback()
	var organizations, local, content int
	if err := tx.QueryRowContext(ctx, `
SELECT (SELECT count(*) FROM organizations),
       (SELECT count(*) FROM organizations WHERE local = 1),
       (SELECT count(*) FROM projects) + (SELECT count(*) FROM repositories)
       + (SELECT count(*) FROM issues) + (SELECT count(*) FROM machines)
       + (SELECT count(*) FROM native_commands) + (SELECT count(*) FROM hosted_members)
       + (SELECT count(*) FROM hosted_sessions) + (SELECT count(*) FROM github_webhook_inbox)`).Scan(&organizations, &local, &content); err != nil {
		return ErrHostedDatabaseBinding
	}
	if organizations != 1 || local != 1 || content != 0 {
		return ErrHostedDatabaseBinding
	}
	if _, err := tx.ExecContext(ctx, "UPDATE organizations SET id = ? WHERE local = 1", cfg.OrganizationID); err != nil {
		return ErrHostedDatabaseBinding
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO hosted_tenant (singleton, organization_id, provider_id, bootstrap_subject, public_url) VALUES (1, ?, ?, ?, ?)", cfg.OrganizationID, cfg.WorkOSOrganizationID, cfg.BootstrapSubject, cfg.PublicURL); err != nil {
		return ErrHostedDatabaseBinding
	}
	if err := tx.Commit(); err != nil {
		return ErrHostedDatabaseBinding
	}
	d.hostedOrganization = tracker.OrganizationID(cfg.OrganizationID)
	return nil
}

func (d *database) hostedMetadata(ctx context.Context) (HostedMetadata, error) {
	if d.hostedOrganization == "" {
		return HostedMetadata{}, ErrHostedMetadataUnavailable
	}
	var result HostedMetadata
	var activity sql.NullString
	err := d.db.QueryRowContext(ctx, `
SELECT h.organization_id, h.provider_id,
       (SELECT count(*) FROM hosted_members WHERE active = 1),
       (SELECT count(*) FROM projects WHERE organization_id = h.organization_id),
       (SELECT count(*) FROM runner_identities WHERE organization_id = h.organization_id),
       (SELECT count(*) FROM collaboration_events WHERE organization_id = h.organization_id),
       (SELECT activity FROM (
         SELECT recorded_at AS activity FROM collaboration_events WHERE organization_id = h.organization_id
         UNION ALL SELECT created_at FROM hosted_sessions
       ) ORDER BY julianday(activity) DESC LIMIT 1)
FROM hosted_tenant h WHERE h.singleton = 1 AND h.organization_id = ?`, d.hostedOrganization).Scan(&result.OrganizationID, &result.ProviderOrganizationID, &result.Members, &result.Projects, &result.Runners, &result.Events, &activity)
	if err != nil {
		return HostedMetadata{}, ErrHostedMetadataUnavailable
	}
	if activity.Valid {
		result.LastActivity, err = parseTimeValue(activity.String)
		if err != nil {
			return HostedMetadata{}, ErrHostedMetadataUnavailable
		}
	}
	for _, suffix := range []string{"", "-wal"} {
		info, err := os.Stat(d.path + suffix)
		if err != nil {
			if suffix != "" && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return HostedMetadata{}, ErrHostedMetadataUnavailable
		}
		result.DatabaseBytes += info.Size()
	}
	if err := d.health(ctx); err != nil {
		return HostedMetadata{}, ErrHostedMetadataUnavailable
	}
	result.Healthy = true
	return result, nil
}
