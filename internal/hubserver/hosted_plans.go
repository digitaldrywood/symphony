package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

type PlanReference struct {
	ID      string `json:"id" yaml:"id"`
	Version int64  `json:"version" yaml:"version"`
}

type HostedPlan struct {
	PlanReference `yaml:",inline"`
	Features      []string         `json:"features" yaml:"features"`
	Allowances    map[string]int64 `json:"allowances" yaml:"allowances"`
}

type HostedPlansConfig struct {
	Plans             []HostedPlan  `yaml:"plans"`
	Base              PlanReference `yaml:"base"`
	WindowSeconds     int64         `yaml:"window_seconds"`
	RetentionWindows  int64         `yaml:"retention_windows"`
	ConnectedSeconds  int64         `yaml:"connected_seconds"`
	InvitationSeconds int64         `yaml:"invitation_seconds"`
}

type HostedGrant struct {
	ID        string        `json:"id"`
	Plan      PlanReference `json:"plan"`
	Scope     []string      `json:"scope"`
	StartsAt  time.Time     `json:"starts_at"`
	ExpiresAt *time.Time    `json:"expires_at,omitempty"`
	RevokedAt *time.Time    `json:"revoked_at,omitempty"`
}

type HostedEntitlement struct {
	OrganizationID string           `json:"organization_id"`
	Base           PlanReference    `json:"base"`
	EffectiveBase  PlanReference    `json:"effective_base"`
	Source         string           `json:"source"`
	Revision       int64            `json:"revision"`
	Features       []string         `json:"features"`
	Allowances     map[string]int64 `json:"allowances"`
	Grants         []HostedGrant    `json:"grants"`
	Usage          map[string]int64 `json:"usage"`
	WindowEndsAt   time.Time        `json:"window_ends_at"`
}

func hostedAllowanceNames() []string {
	return []string{"members", "projects", "repositories", "registered_runners", "connected_runners", "concurrent_work", "api_mutations", "ingested_events", "collaboration_bytes", "history_records", "artifact_retained_bytes", "artifact_reserved_bytes", "artifact_bytes", "artifact_upload_bytes", "artifact_retention_seconds", "relay_bytes"}
}

func hostedFeatureNames() []string {
	return []string{"collaboration", "native_execution", "github_integration", "hosted_artifacts"}
}

func pilotHostedPlans() HostedPlansConfig {
	return HostedPlansConfig{
		Base: PlanReference{ID: "pilot_free", Version: 1}, WindowSeconds: 3600, RetentionWindows: 24, ConnectedSeconds: 90, InvitationSeconds: 86400,
		Plans: []HostedPlan{{PlanReference: PlanReference{ID: "pilot_free", Version: 1}, Features: []string{"collaboration", "native_execution", "github_integration"}, Allowances: map[string]int64{
			"members": 10, "projects": 10, "repositories": 10, "registered_runners": 10, "connected_runners": 10, "concurrent_work": 5,
			"api_mutations": 10000, "ingested_events": 10000, "collaboration_bytes": 64 << 20, "history_records": 10000,
		}}},
	}
}

func (c HostedPlansConfig) validate() error {
	if c.InvitationSeconds < 60 || c.InvitationSeconds > 7*86400 || len(c.Plans) == 0 || len(c.Plans) > 100 || c.WindowSeconds < 60 || c.WindowSeconds > 86400 || c.WindowSeconds%60 != 0 || c.RetentionWindows*c.WindowSeconds > 30*86400 || c.RetentionWindows < 1 || c.RetentionWindows > 720 || c.ConnectedSeconds < 30 || c.ConnectedSeconds > 3600 {
		return errors.New("hosted pilot plan configuration is invalid")
	}
	seen := make(map[PlanReference]bool)
	for _, p := range c.Plans {
		if !hostedSafeID(p.ID) || p.Version < 1 || seen[p.PlanReference] {
			return errors.New("hosted plan identity is invalid or duplicated")
		}
		seen[p.PlanReference] = true
		for _, feature := range p.Features {
			if !slices.Contains(hostedFeatureNames(), feature) {
				return errors.New("hosted plan feature is unknown")
			}
		}
		for name, limit := range p.Allowances {
			if !slices.Contains(hostedAllowanceNames(), name) || limit < 0 || limit > 1<<50 {
				return errors.New("hosted plan allowance is invalid")
			}
		}
	}
	if !seen[c.Base] {
		return errors.New("hosted base plan must reference a configured version")
	}
	return nil
}

func (d *database) configureHostedPlans(ctx context.Context, cfg *HostedConfig) error {
	if cfg == nil {
		return nil
	}
	config := pilotHostedPlans()
	if cfg.Plans != nil {
		config = *cfg.Plans
	} else {
		if cfg.PlanID != "" {
			config.Plans[0].ID = cfg.PlanID
			config.Base.ID = cfg.PlanID
		}
		if cfg.StorageQuotaBytes > 0 {
			config.Plans[0].Allowances["collaboration_bytes"] = cfg.StorageQuotaBytes
		}
		if cfg.EventQuota > 0 {
			config.Plans[0].Allowances["ingested_events"] = cfg.EventQuota
		}
	}
	if err := config.validate(); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, plan := range config.Plans {
		plan.Allowances = maps.Clone(plan.Allowances)
		if plan.Allowances == nil {
			plan.Allowances = make(map[string]int64)
		}
		for _, name := range hostedAllowanceNames() {
			if _, exists := plan.Allowances[name]; !exists {
				plan.Allowances[name] = 0
			}
		}
		plan.Features = slices.Clone(plan.Features)
		slices.Sort(plan.Features)
		plan.Features = slices.Compact(plan.Features)
		encoded, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		var existing string
		err = tx.QueryRowContext(ctx, "SELECT record_json FROM hosted_plans WHERE id = ? AND version = ?", plan.ID, plan.Version).Scan(&existing)
		if err == nil && existing != string(encoded) {
			return errors.New("hosted plan versions are immutable; configure a new version")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO hosted_plans(id,version,record_json) VALUES(?,?,?) ON CONFLICT DO NOTHING", plan.ID, plan.Version, string(encoded)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_plan_assignments(organization_id,base_id,base_version) VALUES(?,?,?) ON CONFLICT DO NOTHING`, d.hostedOrganization, config.Base.ID, config.Base.Version); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	d.hostedPlans = &config
	return nil
}

func readHostedPlan(ctx context.Context, query nativeQueryer, ref PlanReference) (HostedPlan, error) {
	var raw string
	var plan HostedPlan
	if err := query.QueryRowContext(ctx, "SELECT record_json FROM hosted_plans WHERE id = ? AND version = ?", ref.ID, ref.Version).Scan(&raw); err != nil {
		return plan, err
	}
	err := json.Unmarshal([]byte(raw), &plan)
	return plan, err
}

func (d *database) hostedEntitlement(ctx context.Context, query nativeQueryer, now time.Time) (HostedEntitlement, error) {
	result := HostedEntitlement{OrganizationID: string(d.hostedOrganization), Source: "base", Grants: []HostedGrant{}}
	if d.hostedPlans == nil {
		return result, errors.New("hosted entitlements are disabled")
	}
	var subscription, expiry sql.NullString
	var version sql.NullInt64
	if err := query.QueryRowContext(ctx, `SELECT base_id,base_version,subscription_id,subscription_version,subscription_expires_at,revision FROM hosted_plan_assignments WHERE organization_id = ?`, d.hostedOrganization).Scan(&result.Base.ID, &result.Base.Version, &subscription, &version, &expiry, &result.Revision); err != nil {
		return result, err
	}
	result.EffectiveBase = result.Base
	if subscription.Valid && expiry.Valid {
		until, err := parseTimeValue(expiry.String)
		if err != nil {
			return result, err
		}
		if until.After(now) {
			result.EffectiveBase = PlanReference{ID: subscription.String, Version: version.Int64}
			result.Source = "subscription"
		}
	}
	base, err := readHostedPlan(ctx, query, result.EffectiveBase)
	if err != nil {
		return result, err
	}
	result.Allowances, result.Features = base.Allowances, base.Features
	rows, err := query.QueryContext(ctx, "SELECT record_json FROM hosted_complimentary_grants ORDER BY id")
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var grant HostedGrant
		if err := rows.Scan(&raw); err != nil {
			return result, errors.Join(err, rows.Close())
		}
		if err := json.Unmarshal([]byte(raw), &grant); err != nil {
			return result, errors.Join(err, rows.Close())
		}
		result.Grants = append(result.Grants, grant)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, err
	}
	for _, grant := range result.Grants {
		if grant.RevokedAt != nil || grant.StartsAt.After(now) || grant.ExpiresAt != nil && !grant.ExpiresAt.After(now) {
			continue
		}
		plan, err := readHostedPlan(ctx, query, grant.Plan)
		if err != nil {
			return result, err
		}
		for _, scope := range grant.Scope {
			if value, ok := plan.Allowances[scope]; ok {
				result.Allowances[scope] = max(result.Allowances[scope], value)
			}
			if slices.Contains(plan.Features, scope) && !slices.Contains(result.Features, scope) {
				result.Features = append(result.Features, scope)
			}
		}
	}
	slices.Sort(result.Features)
	result.WindowEndsAt = time.Unix((now.Unix()/d.hostedPlans.WindowSeconds+1)*d.hostedPlans.WindowSeconds, 0).UTC()
	return result, nil
}

type hostedLimitError struct {
	Resource    string `json:"resource"`
	Allowance   int64  `json:"allowance"`
	Consumption int64  `json:"consumption"`
}

func (e *hostedLimitError) Error() string {
	return fmt.Sprintf("Hosted %s allowance reached (%d of %d). Existing data, reading, export and billing remain available.", e.Resource, e.Consumption, e.Allowance)
}

func (d *database) requireHostedFeature(ctx context.Context, query nativeQueryer, feature string, now time.Time) error {
	if d.hostedPlans == nil {
		return nil
	}
	entitlement, err := d.hostedEntitlement(ctx, query, now)
	if err != nil {
		return err
	}
	if !slices.Contains(entitlement.Features, feature) {
		return &hostedLimitError{Resource: feature}
	}
	return nil
}
