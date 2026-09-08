package hubserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/tracker"
)

type leaseRecord struct {
	issueID tracker.WorkItemID
	session tracker.SessionSummary
}

func (d *database) Claim(ctx context.Context, request tracker.ClaimRequest) (lease tracker.Lease, resultErr error) {
	request, err := normalizeClaimRequest(request)
	if err != nil {
		return tracker.Lease{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return tracker.Lease{}, fmt.Errorf("begin hub claim: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, tx.Rollback())
		}
	}()
	now, err := d.currentTime()
	if err != nil {
		return tracker.Lease{}, err
	}
	lease, err = d.claimInTransaction(ctx, tx, request, now)
	if err != nil {
		return tracker.Lease{}, err
	}
	if err := tx.Commit(); err != nil {
		return tracker.Lease{}, fmt.Errorf("commit hub claim: %w", err)
	}
	return lease, nil
}

func (d *database) claimInTransaction(ctx context.Context, tx *sql.Tx, request tracker.ClaimRequest, now time.Time) (tracker.Lease, error) {
	if err := requireWorkItem(ctx, tx, request.WorkItemID); err != nil {
		return tracker.Lease{}, err
	}
	machine, err := requireMachine(ctx, tx, request.MachineID)
	if err != nil {
		return tracker.Lease{}, err
	}

	current, found, err := readUnreleasedLease(ctx, tx, request.WorkItemID)
	if err != nil {
		return tracker.Lease{}, err
	}
	if found && current.session.ExpiresAt.After(now) {
		if current.session.Machine.ID == request.MachineID && current.session.SessionID == request.SessionID {
			return leaseFromRecord(current), nil
		}
		return tracker.Lease{}, fmt.Errorf("%w: work item %d is held by lease %s", tracker.ErrLeaseConflict, request.WorkItemID, current.session.ID)
	}
	if err := requireUnusedSession(ctx, tx, request.SessionID); err != nil {
		return tracker.Lease{}, err
	}

	var previous *tracker.SessionSummary
	if found {
		previous, err = sessionSummary(ctx, tx, current)
		if err != nil {
			return tracker.Lease{}, err
		}
		if err := expireLease(ctx, tx, current, now); err != nil {
			return tracker.Lease{}, err
		}
		releasedAt := now
		previous.ReleasedAt = &releasedAt
	} else {
		prior, priorFound, readErr := readLatestLease(ctx, tx, request.WorkItemID)
		if readErr != nil {
			return tracker.Lease{}, readErr
		}
		if priorFound {
			previous, err = sessionSummary(ctx, tx, prior)
			if err != nil {
				return tracker.Lease{}, err
			}
		}
	}

	if err := d.checkHostedClaim(ctx, tx, now); err != nil {
		return tracker.Lease{}, err
	}
	leaseID := strings.TrimSpace(d.newLeaseID())
	if leaseID == "" {
		return tracker.Lease{}, errors.New("generate hub lease ID: empty value")
	}
	expiresAt := now.Add(request.TTL)
	result, err := tx.ExecContext(ctx, `
INSERT INTO leases (lease_id, issue_id, machine_id, session_id, expires_at, acquired_at, renewed_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		leaseID,
		request.WorkItemID,
		request.MachineID,
		request.SessionID,
		formatHubTime(expiresAt),
		formatHubTime(now),
		formatHubTime(now),
		formatHubTime(now),
		formatHubTime(now),
	)
	if err != nil {
		return tracker.Lease{}, fmt.Errorf("insert hub claim: %w", err)
	}
	token, err := result.LastInsertId()
	if err != nil {
		return tracker.Lease{}, fmt.Errorf("read hub fencing token: %w", err)
	}
	if err := heartbeatMachine(ctx, tx, request.MachineID, now); err != nil {
		return tracker.Lease{}, err
	}
	return tracker.Lease{
		LeaseSummary: tracker.LeaseSummary{
			ID:           tracker.LeaseID(leaseID),
			FencingToken: tracker.FencingToken(token),
			Machine:      machine,
			SessionID:    request.SessionID,
			AcquiredAt:   now,
			RenewedAt:    now,
			ExpiresAt:    expiresAt,
		},
		WorkItemID:      request.WorkItemID,
		PreviousSession: previous,
	}, nil
}

func (d *database) Renew(ctx context.Context, request tracker.RenewRequest) (tracker.Lease, error) {
	return d.renew(ctx, request, false)
}

func (d *database) renew(ctx context.Context, request tracker.RenewRequest, policyRequired bool, scopes ...nativeScope) (lease tracker.Lease, resultErr error) {
	request, err := normalizeRenewRequest(request)
	if err != nil {
		return tracker.Lease{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return tracker.Lease{}, fmt.Errorf("begin hub lease renewal: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, tx.Rollback())
		}
	}()
	now, err := d.currentTime()
	if err != nil {
		return tracker.Lease{}, err
	}

	record, found, err := readLeaseByID(ctx, tx, request.LeaseID)
	if err != nil {
		return tracker.Lease{}, err
	}
	if !found {
		return tracker.Lease{}, fmt.Errorf("%w: %s", tracker.ErrLeaseNotFound, request.LeaseID)
	}
	if err := requireCurrentLease(record, request.FencingToken, now); err != nil {
		return tracker.Lease{}, err
	}
	if err := requireApprovedLeasePolicy(ctx, tx, request.LeaseID, policyRequired); err != nil {
		return tracker.Lease{}, err
	}
	for _, scope := range scopes {
		if err := requireRunnerAuthority(ctx, tx, scope, now); err != nil {
			return tracker.Lease{}, err
		}
		if err := requireLeaseRunner(ctx, tx, request.LeaseID, scope); err != nil {
			return tracker.Lease{}, err
		}
	}

	expiresAt := now.Add(request.TTL)
	result, err := tx.ExecContext(ctx, `
UPDATE leases
SET expires_at = ?, renewed_at = ?, updated_at = ?
WHERE lease_id = ? AND fencing_token = ? AND released_at IS NULL`,
		formatHubTime(expiresAt),
		formatHubTime(now),
		formatHubTime(now),
		request.LeaseID,
		request.FencingToken,
	)
	if err != nil {
		return tracker.Lease{}, fmt.Errorf("renew hub lease: %w", err)
	}
	if err := requireOneLeaseMutation(result, request.LeaseID); err != nil {
		return tracker.Lease{}, err
	}
	if err := heartbeatMachine(ctx, tx, record.session.Machine.ID, now); err != nil {
		return tracker.Lease{}, err
	}
	if err := tx.Commit(); err != nil {
		return tracker.Lease{}, fmt.Errorf("commit hub lease renewal: %w", err)
	}
	record.session.RenewedAt = now
	record.session.ExpiresAt = expiresAt
	return leaseFromRecord(record), nil
}

func (d *database) Release(ctx context.Context, request tracker.ReleaseRequest) (resultErr error) {
	request, err := normalizeReleaseRequest(request)
	if err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin hub lease release: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, tx.Rollback())
		}
	}()
	now, err := d.currentTime()
	if err != nil {
		return err
	}

	record, found, err := readLeaseByID(ctx, tx, request.LeaseID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: %s", tracker.ErrLeaseNotFound, request.LeaseID)
	}
	if err := requireCurrentLease(record, request.FencingToken, now); err != nil {
		return err
	}

	payload := map[string]any{}
	if request.Reason != "" {
		payload["reason"] = request.Reason
	}
	if err := insertWorkEvent(ctx, tx, tracker.WorkEvent{
		WorkItemID:   record.issueID,
		FencingToken: request.FencingToken,
		MachineID:    record.session.Machine.ID,
		SessionID:    record.session.SessionID,
		Kind:         "lease_released",
		Payload:      payload,
		OccurredAt:   now,
	}, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE leases
SET released_at = ?, updated_at = ?
WHERE lease_id = ? AND fencing_token = ? AND released_at IS NULL`,
		formatHubTime(now),
		formatHubTime(now),
		request.LeaseID,
		request.FencingToken,
	)
	if err != nil {
		return fmt.Errorf("release hub lease: %w", err)
	}
	if err := requireOneLeaseMutation(result, request.LeaseID); err != nil {
		return err
	}
	if err := heartbeatMachine(ctx, tx, record.session.Machine.ID, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hub lease release: %w", err)
	}
	return nil
}

func (d *database) AppendEvent(ctx context.Context, event tracker.WorkEvent) error {
	return d.appendEvent(ctx, event, false)
}

func (d *database) appendEvent(ctx context.Context, event tracker.WorkEvent, policyRequired bool) (resultErr error) {
	event, err := normalizeWorkEvent(event)
	if err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin hub work event: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, tx.Rollback())
		}
	}()
	now, err := d.currentTime()
	if err != nil {
		return err
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = now
	} else {
		event.OccurredAt = event.OccurredAt.UTC()
	}

	current, found, err := readUnreleasedLease(ctx, tx, event.WorkItemID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: work item %d", tracker.ErrStaleFencingToken, event.WorkItemID)
	}
	if err := requireCurrentLease(current, event.FencingToken, now); err != nil {
		return err
	}
	if err := requireApprovedLeasePolicy(ctx, tx, current.session.ID, policyRequired); err != nil {
		return err
	}
	if event.MachineID != "" && event.MachineID != current.session.Machine.ID {
		return fmt.Errorf("%w: machine %s does not own lease %s", tracker.ErrInvalidWorkEvent, event.MachineID, current.session.ID)
	}
	if event.SessionID != "" && event.SessionID != current.session.SessionID {
		return fmt.Errorf("%w: session %s does not own lease %s", tracker.ErrInvalidWorkEvent, event.SessionID, current.session.ID)
	}
	event.MachineID = current.session.Machine.ID
	event.SessionID = current.session.SessionID
	if err := insertWorkEvent(ctx, tx, event, now); err != nil {
		return err
	}
	if err := heartbeatMachine(ctx, tx, current.session.Machine.ID, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hub work event: %w", err)
	}
	return nil
}

func normalizeClaimRequest(request tracker.ClaimRequest) (tracker.ClaimRequest, error) {
	request.MachineID = tracker.MachineID(strings.TrimSpace(string(request.MachineID)))
	request.SessionID = strings.TrimSpace(request.SessionID)
	switch {
	case request.WorkItemID <= 0:
		return tracker.ClaimRequest{}, fmt.Errorf("%w: work item ID must be positive", tracker.ErrInvalidClaimRequest)
	case request.MachineID == "":
		return tracker.ClaimRequest{}, fmt.Errorf("%w: machine ID is required", tracker.ErrInvalidClaimRequest)
	case request.SessionID == "":
		return tracker.ClaimRequest{}, fmt.Errorf("%w: session ID is required", tracker.ErrInvalidClaimRequest)
	case request.TTL <= 0:
		return tracker.ClaimRequest{}, fmt.Errorf("%w: TTL must be positive", tracker.ErrInvalidClaimRequest)
	default:
		return request, nil
	}
}

func normalizeRenewRequest(request tracker.RenewRequest) (tracker.RenewRequest, error) {
	request.LeaseID = tracker.LeaseID(strings.TrimSpace(string(request.LeaseID)))
	switch {
	case request.LeaseID == "":
		return tracker.RenewRequest{}, fmt.Errorf("%w: lease ID is required", tracker.ErrInvalidLeaseRequest)
	case request.FencingToken <= 0:
		return tracker.RenewRequest{}, fmt.Errorf("%w: fencing token must be positive", tracker.ErrInvalidLeaseRequest)
	case request.TTL <= 0:
		return tracker.RenewRequest{}, fmt.Errorf("%w: TTL must be positive", tracker.ErrInvalidLeaseRequest)
	default:
		return request, nil
	}
}

func normalizeReleaseRequest(request tracker.ReleaseRequest) (tracker.ReleaseRequest, error) {
	request.LeaseID = tracker.LeaseID(strings.TrimSpace(string(request.LeaseID)))
	request.Reason = strings.TrimSpace(request.Reason)
	if request.LeaseID == "" {
		return tracker.ReleaseRequest{}, fmt.Errorf("%w: lease ID is required", tracker.ErrInvalidLeaseRequest)
	}
	if request.FencingToken <= 0 {
		return tracker.ReleaseRequest{}, fmt.Errorf("%w: fencing token must be positive", tracker.ErrInvalidLeaseRequest)
	}
	return request, nil
}

func normalizeWorkEvent(event tracker.WorkEvent) (tracker.WorkEvent, error) {
	event.MachineID = tracker.MachineID(strings.TrimSpace(string(event.MachineID)))
	event.SessionID = strings.TrimSpace(event.SessionID)
	event.RunID = strings.TrimSpace(event.RunID)
	event.Kind = strings.TrimSpace(event.Kind)
	if event.WorkItemID <= 0 {
		return tracker.WorkEvent{}, fmt.Errorf("%w: work item ID must be positive", tracker.ErrInvalidWorkEvent)
	}
	if event.FencingToken <= 0 {
		return tracker.WorkEvent{}, fmt.Errorf("%w: fencing token is required", tracker.ErrInvalidWorkEvent)
	}
	if event.Kind == "" {
		return tracker.WorkEvent{}, fmt.Errorf("%w: kind is required", tracker.ErrInvalidWorkEvent)
	}
	if _, err := json.Marshal(event.Payload); err != nil {
		return tracker.WorkEvent{}, fmt.Errorf("%w: encode payload: %w", tracker.ErrInvalidWorkEvent, err)
	}
	return event, nil
}

func (d *database) currentTime() (time.Time, error) {
	now := d.now().UTC()
	if now.IsZero() {
		return time.Time{}, ErrInvalidClock
	}
	return now, nil
}

func requireWorkItem(ctx context.Context, tx *sql.Tx, workItemID tracker.WorkItemID) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM issues WHERE id = ?)", workItemID).Scan(&exists); err != nil {
		return fmt.Errorf("verify hub work item: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: %d", tracker.ErrWorkItemNotFound, workItemID)
	}
	return nil
}

func requireMachine(ctx context.Context, tx *sql.Tx, machineID tracker.MachineID) (tracker.MachineSummary, error) {
	machine := tracker.MachineSummary{ID: machineID}
	if err := tx.QueryRowContext(ctx, "SELECT hostname, display_name FROM machines WHERE id = ?", machineID).Scan(&machine.Hostname, &machine.DisplayName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tracker.MachineSummary{}, fmt.Errorf("%w: %s", tracker.ErrMachineNotFound, machineID)
		}
		return tracker.MachineSummary{}, fmt.Errorf("read hub machine: %w", err)
	}
	return machine, nil
}

func requireUnusedSession(ctx context.Context, tx *sql.Tx, sessionID string) error {
	var leaseID tracker.LeaseID
	err := tx.QueryRowContext(ctx, "SELECT lease_id FROM leases WHERE session_id = ?", sessionID).Scan(&leaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify hub lease session: %w", err)
	}
	return fmt.Errorf("%w: session %s already belongs to lease %s", tracker.ErrLeaseConflict, sessionID, leaseID)
}

func readUnreleasedLease(ctx context.Context, tx *sql.Tx, workItemID tracker.WorkItemID) (leaseRecord, bool, error) {
	return scanLeaseRecord(tx.QueryRowContext(ctx, `
SELECT l.issue_id, l.lease_id, l.fencing_token, l.machine_id, m.hostname, m.display_name, l.session_id, l.acquired_at, l.renewed_at, l.expires_at, l.released_at
FROM leases l
JOIN machines m ON m.id = l.machine_id
WHERE l.issue_id = ? AND l.released_at IS NULL
LIMIT 1`, workItemID))
}

func readLatestLease(ctx context.Context, tx *sql.Tx, workItemID tracker.WorkItemID) (leaseRecord, bool, error) {
	return scanLeaseRecord(tx.QueryRowContext(ctx, `
SELECT l.issue_id, l.lease_id, l.fencing_token, l.machine_id, m.hostname, m.display_name, l.session_id, l.acquired_at, l.renewed_at, l.expires_at, l.released_at
FROM leases l
JOIN machines m ON m.id = l.machine_id
WHERE l.issue_id = ?
ORDER BY l.fencing_token DESC
LIMIT 1`, workItemID))
}

func readLeaseByID(ctx context.Context, tx *sql.Tx, leaseID tracker.LeaseID) (leaseRecord, bool, error) {
	return scanLeaseRecord(tx.QueryRowContext(ctx, `
SELECT l.issue_id, l.lease_id, l.fencing_token, l.machine_id, m.hostname, m.display_name, l.session_id, l.acquired_at, l.renewed_at, l.expires_at, l.released_at
FROM leases l
JOIN machines m ON m.id = l.machine_id
WHERE l.lease_id = ?`, leaseID))
}

func scanLeaseRecord(row interface{ Scan(...any) error }) (leaseRecord, bool, error) {
	var record leaseRecord
	var acquiredAt string
	var renewedAt string
	var expiresAt string
	var releasedAt sql.NullString
	err := row.Scan(
		&record.issueID,
		&record.session.ID,
		&record.session.FencingToken,
		&record.session.Machine.ID,
		&record.session.Machine.Hostname,
		&record.session.Machine.DisplayName,
		&record.session.SessionID,
		&acquiredAt,
		&renewedAt,
		&expiresAt,
		&releasedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return leaseRecord{}, false, nil
	}
	if err != nil {
		return leaseRecord{}, false, fmt.Errorf("scan hub lease: %w", err)
	}
	if record.session.AcquiredAt, err = parseTimeValue(acquiredAt); err != nil {
		return leaseRecord{}, false, fmt.Errorf("decode hub lease acquired timestamp: %w", err)
	}
	if record.session.RenewedAt, err = parseTimeValue(renewedAt); err != nil {
		return leaseRecord{}, false, fmt.Errorf("decode hub lease renewed timestamp: %w", err)
	}
	if record.session.ExpiresAt, err = parseTimeValue(expiresAt); err != nil {
		return leaseRecord{}, false, fmt.Errorf("decode hub lease expiry timestamp: %w", err)
	}
	if releasedAt.Valid {
		released, parseErr := parseTimeValue(releasedAt.String)
		if parseErr != nil {
			return leaseRecord{}, false, fmt.Errorf("decode hub lease release timestamp: %w", parseErr)
		}
		record.session.ReleasedAt = &released
	}
	return record, true, nil
}

func sessionSummary(ctx context.Context, tx *sql.Tx, record leaseRecord) (*tracker.SessionSummary, error) {
	event, found, err := readLastWorkEvent(ctx, tx, record.session.FencingToken)
	if err != nil {
		return nil, err
	}
	if found {
		record.session.LastEvent = &event
	}
	return &record.session, nil
}

func readLastWorkEvent(ctx context.Context, tx *sql.Tx, token tracker.FencingToken) (tracker.WorkEvent, bool, error) {
	var event tracker.WorkEvent
	var machineID sql.NullString
	var sessionID sql.NullString
	var runID sql.NullString
	var payloadJSON string
	var occurredAt string
	err := tx.QueryRowContext(ctx, `
SELECT issue_id, fencing_token, machine_id, session_id, run_id, kind, payload_json, occurred_at
FROM work_events
WHERE fencing_token = ?
ORDER BY id DESC
LIMIT 1`, token).Scan(
		&event.WorkItemID,
		&event.FencingToken,
		&machineID,
		&sessionID,
		&runID,
		&event.Kind,
		&payloadJSON,
		&occurredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return tracker.WorkEvent{}, false, nil
	}
	if err != nil {
		return tracker.WorkEvent{}, false, fmt.Errorf("read latest hub work event: %w", err)
	}
	event.MachineID = tracker.MachineID(machineID.String)
	event.SessionID = sessionID.String
	event.RunID = runID.String
	if err := json.Unmarshal([]byte(payloadJSON), &event.Payload); err != nil {
		return tracker.WorkEvent{}, false, fmt.Errorf("decode latest hub work event payload: %w", err)
	}
	if event.OccurredAt, err = parseTimeValue(occurredAt); err != nil {
		return tracker.WorkEvent{}, false, fmt.Errorf("decode latest hub work event timestamp: %w", err)
	}
	return event, true, nil
}

func expireLease(ctx context.Context, tx *sql.Tx, record leaseRecord, now time.Time) error {
	result, err := tx.ExecContext(ctx, `
UPDATE leases
SET released_at = ?, updated_at = ?
WHERE lease_id = ? AND fencing_token = ? AND released_at IS NULL`,
		formatHubTime(now),
		formatHubTime(now),
		record.session.ID,
		record.session.FencingToken,
	)
	if err != nil {
		return fmt.Errorf("expire replaced hub lease: %w", err)
	}
	if err := requireOneLeaseMutation(result, record.session.ID); err != nil {
		return err
	}
	return nil
}

func requireCurrentLease(record leaseRecord, token tracker.FencingToken, now time.Time) error {
	if record.session.FencingToken != token || record.session.ReleasedAt != nil || !record.session.ExpiresAt.After(now) {
		return fmt.Errorf("%w: lease %s", tracker.ErrStaleFencingToken, record.session.ID)
	}
	return nil
}

func requireOneLeaseMutation(result sql.Result, leaseID tracker.LeaseID) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read hub lease mutation result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: lease %s changed concurrently", tracker.ErrStaleFencingToken, leaseID)
	}
	return nil
}

func heartbeatMachine(ctx context.Context, tx *sql.Tx, machineID tracker.MachineID, now time.Time) error {
	result, err := tx.ExecContext(ctx, "UPDATE machines SET last_heartbeat_at = ?, updated_at = ? WHERE id = ?", formatHubTime(now), formatHubTime(now), machineID)
	if err != nil {
		return fmt.Errorf("heartbeat hub machine: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read hub machine heartbeat result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("%w: %s", tracker.ErrMachineNotFound, machineID)
	}
	return nil
}

func insertWorkEvent(ctx context.Context, tx *sql.Tx, event tracker.WorkEvent, recordedAt time.Time) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("encode hub work event payload: %w", err)
	}
	if string(payload) == "null" {
		payload = []byte("{}")
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO work_events (issue_id, fencing_token, machine_id, session_id, run_id, kind, payload_json, occurred_at, recorded_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.WorkItemID,
		event.FencingToken,
		event.MachineID,
		event.SessionID,
		nullString(event.RunID),
		event.Kind,
		string(payload),
		formatHubTime(event.OccurredAt),
		formatHubTime(recordedAt),
	); err != nil {
		return fmt.Errorf("append hub work event: %w", err)
	}
	return nil
}

func leaseFromRecord(record leaseRecord) tracker.Lease {
	return tracker.Lease{LeaseSummary: record.session.LeaseSummary, WorkItemID: record.issueID}
}

func formatHubTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
