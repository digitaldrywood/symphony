package hubserver

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (d *database) hostedConsumption(ctx context.Context, query nativeQueryer, now time.Time) (map[string]int64, error) {
	result := make(map[string]int64)
	if d.hostedPlans == nil {
		return result, nil
	}
	queries := map[string]string{
		"artifact_retained_bytes": "SELECT coalesce(sum(json_extract(usage_json,'$.retained_bytes')),0) FROM hosted_artifact_usage",
		"artifact_reserved_bytes": "SELECT coalesce(sum(json_extract(usage_json,'$.reserved_bytes')),0) FROM hosted_artifact_usage",
		"members":                 "SELECT count(*) FROM hosted_members WHERE active = 1",
		"projects":                "SELECT count(*) FROM projects",
		"repositories":            "SELECT count(*) FROM repositories",
		"registered_runners":      `SELECT (SELECT count(*) FROM runner_identities r JOIN api_tokens t ON t.id = r.token_id WHERE t.revoked_at IS NULL) + (SELECT count(*) FROM machines m JOIN api_tokens t ON t.id = m.token_id WHERE t.revoked_at IS NULL AND NOT EXISTS(SELECT 1 FROM runner_identities r WHERE r.machine_id = m.id))`,
		"history_records":         "SELECT (SELECT count(*) FROM collaboration_events) + (SELECT count(*) FROM collaboration_versions) + (SELECT count(*) FROM native_attempt_events)",
		"events_total":            "SELECT count(*) FROM collaboration_events",
		"collaboration_bytes": `SELECT
		(SELECT coalesce(sum(length(CAST(title AS BLOB))+length(CAST(body AS BLOB))+length(CAST(labels_json AS BLOB))+length(CAST(assignees_json AS BLOB))),0) FROM issues) +
		(SELECT coalesce(sum(length(CAST(body AS BLOB))),0) FROM native_comments) +
		(SELECT coalesce(sum(length(CAST(record_json AS BLOB))),0) FROM collaboration_versions) +
		(SELECT coalesce(sum(length(CAST(data_json AS BLOB))+length(CAST(actor_json AS BLOB))),0) FROM collaboration_events) +
		(SELECT coalesce(sum(length(CAST(response_json AS BLOB))),0) FROM native_commands) +
		(SELECT coalesce(sum(length(CAST(data_json AS BLOB))),0) FROM native_attempts) +
		(SELECT coalesce(sum(length(CAST(reference_json AS BLOB))),0) FROM artifact_references) +
		(SELECT coalesce(sum(length(CAST(record_json AS BLOB))),0) FROM github_import_records)`,
	}
	for name, statement := range queries {
		var count int64
		if err := query.QueryRowContext(ctx, statement).Scan(&count); err != nil {
			return nil, err
		}
		result[name] = count
	}
	for _, sample := range []struct {
		name, statement string
		after           time.Time
	}{
		{"concurrent_work", "SELECT expires_at FROM leases WHERE released_at IS NULL", now},
		{"connected_runners", `SELECT r.last_heartbeat_at FROM runner_identities r JOIN api_tokens t ON t.id = r.token_id WHERE t.revoked_at IS NULL UNION ALL SELECT m.last_heartbeat_at FROM machines m JOIN api_tokens t ON t.id = m.token_id WHERE t.revoked_at IS NULL AND NOT EXISTS(SELECT 1 FROM runner_identities r WHERE r.machine_id = m.id)`, now.Add(-time.Duration(d.hostedPlans.ConnectedSeconds) * time.Second)},
	} {
		count, err := countHostedAfter(ctx, query, sample.statement, sample.after)
		if err != nil {
			return nil, err
		}
		result[sample.name] = count
	}
	var reserved int64
	if err := query.QueryRowContext(ctx, "SELECT count(*) FROM hosted_member_reservations WHERE expires_at > ?", now.Unix()).Scan(&reserved); err != nil {
		return nil, err
	}
	result["members"] += reserved
	window := now.Unix() / d.hostedPlans.WindowSeconds * d.hostedPlans.WindowSeconds
	rows, err := query.QueryContext(ctx, "SELECT metric,amount FROM hosted_usage_windows WHERE window_start = ?", window)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var metric string
		var amount int64
		if err := rows.Scan(&metric, &amount); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		result[metric] = amount
	}
	return result, errors.Join(rows.Err(), rows.Close())
}

func countHostedAfter(ctx context.Context, query nativeQueryer, statement string, after time.Time, args ...any) (int64, error) {
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	var count int64
	defer rows.Close()
	for rows.Next() {
		var stamp string
		if err := rows.Scan(&stamp); err != nil {
			return 0, errors.Join(err, rows.Close())
		}
		parsed, err := parseTimeValue(stamp)
		if err != nil {
			return 0, errors.Join(err, rows.Close())
		}
		if parsed.After(after) {
			count++
		}
	}
	return count, errors.Join(rows.Err(), rows.Close())
}

func (d *database) recordHostedUsage(ctx context.Context, tx *sql.Tx, now time.Time, metric string, amount int64) error {
	if d.hostedPlans == nil || amount == 0 {
		return nil
	}
	window := now.Unix() / d.hostedPlans.WindowSeconds * d.hostedPlans.WindowSeconds
	if _, err := tx.ExecContext(ctx, "DELETE FROM hosted_usage_windows WHERE window_start <= ?", window-d.hostedPlans.RetentionWindows*d.hostedPlans.WindowSeconds); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO hosted_usage_windows(window_start,metric,amount) VALUES(?,?,?) ON CONFLICT(window_start,metric) DO UPDATE SET amount = min(amount+excluded.amount,1125899906842624)`, window, metric, amount)
	return err
}

func (d *database) checkHostedGrowth(ctx context.Context, tx *sql.Tx, before map[string]int64, now time.Time, completion bool) error {
	if d.hostedPlans == nil {
		return nil
	}
	after, err := d.hostedConsumption(ctx, tx, now)
	if err != nil {
		return err
	}
	entitlement, err := d.hostedEntitlement(ctx, tx, now)
	if err != nil {
		return err
	}
	for _, name := range []string{"members", "projects", "repositories", "registered_runners", "connected_runners", "concurrent_work", "collaboration_bytes", "history_records"} {
		if completion && (name == "collaboration_bytes" || name == "history_records" || name == "connected_runners") {
			continue
		}
		if after[name] > before[name] && after[name] > entitlement.Allowances[name] {
			return &hostedLimitError{Resource: name, Allowance: entitlement.Allowances[name], Consumption: before[name]}
		}
	}
	for _, sample := range []struct {
		name   string
		amount int64
	}{
		{"api_mutations", 1}, {"ingested_events", max(int64(0), after["events_total"]-before["events_total"])},
	} {
		if !completion && sample.amount > 0 && sample.amount > entitlement.Allowances[sample.name]-after[sample.name] {
			return &hostedLimitError{Resource: sample.name, Allowance: entitlement.Allowances[sample.name], Consumption: after[sample.name]}
		}
		if err := d.recordHostedUsage(ctx, tx, now, sample.name, sample.amount); err != nil {
			return err
		}
	}
	return nil
}

func (d *database) checkHostedClaim(ctx context.Context, tx *sql.Tx, now time.Time) error {
	if d.hostedPlans == nil {
		return nil
	}
	if err := d.requireHostedFeature(ctx, tx, "native_execution", now); err != nil {
		return err
	}
	entitlement, err := d.hostedEntitlement(ctx, tx, now)
	if err != nil {
		return err
	}
	count, err := countHostedAfter(ctx, tx, "SELECT expires_at FROM leases WHERE released_at IS NULL", now)
	if err != nil {
		return err
	}
	if count >= entitlement.Allowances["concurrent_work"] {
		return &hostedLimitError{Resource: "concurrent_work", Allowance: entitlement.Allowances["concurrent_work"], Consumption: count}
	}
	window := now.Unix() / d.hostedPlans.WindowSeconds * d.hostedPlans.WindowSeconds
	var used int64
	if err := tx.QueryRowContext(ctx, "SELECT coalesce(sum(amount),0) FROM hosted_usage_windows WHERE window_start = ? AND metric = 'api_mutations'", window).Scan(&used); err != nil {
		return err
	}
	if used >= entitlement.Allowances["api_mutations"] {
		return &hostedLimitError{Resource: "api_mutations", Allowance: entitlement.Allowances["api_mutations"], Consumption: used}
	}
	return d.recordHostedUsage(ctx, tx, now, "api_mutations", 1)
}
