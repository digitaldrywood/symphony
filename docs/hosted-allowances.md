# Hosted pilot allowances

Hosted identity enables organization entitlements in the owning Hub database.
Local and self-hosted Hubs do not create assignments, query a billing service, or
apply hosted restrictions. No Stripe account, subscription, or card is necessary
for the free base plan. This delivery does not launch billing or set public prices.

## Configuration and plan versions

Add `entitlements` to the hosted identity YAML. These example quantities are
pilot configuration, not permanent free allowances or a cost promise:

```yaml
entitlements:
  base: {id: pilot_free, version: 1}
  window_seconds: 3600
  retention_windows: 24
  connected_seconds: 90
  invitation_seconds: 86400
  plans:
    - id: pilot_free
      version: 1
      features: [collaboration, native_execution, github_integration]
      allowances:
        members: 10
        projects: 10
        repositories: 10
        registered_runners: 10
        connected_runners: 10
        concurrent_work: 5
        api_mutations: 10000
        ingested_events: 10000
        collaboration_bytes: 67108864
        history_records: 10000
    - id: pilot_extended
      version: 1
      features: [collaboration, native_execution, github_integration]
      allowances:
        members: 20
        projects: 20
        repositories: 20
        registered_runners: 20
        connected_runners: 10
        concurrent_work: 10
        api_mutations: 20000
        ingested_events: 20000
        collaboration_bytes: 134217728
        history_records: 20000
```

Omitting the entire section selects the first plan and pilot settings shown above.
Within an explicit plan, omitted allowances are zero and omitted features are
disabled. Negative values, unknown names and unsupported configuration bounds
are rejected. Windows are whole multiples of 60 seconds, at most one day;
telemetry retains at most 720 windows and 30 days. Invitation reservations last
between one minute and seven days. These are validation bounds, not recommended
public retention periods. Operators choose the values before hosting customers.

Plan records are immutable by `(id, version)`. To change a plan, add another
version and explicitly assign it. Configured `base` initializes a new assignment;
restarting or changing the file does not overwrite an existing assignment or a
later administrator decision. Existing plan versions remain available for audit
and historical grants. Legacy `plan_id`, positive `storage_quota_bytes`, and
positive `event_quota` configure the initial pilot plan when `entitlements` is
absent; they cannot be mixed with the new section.

## Administration and precedence

Configure a separate secret for entitlement administration:

```yaml
entitlement_administrator: operator_example
entitlement_admin_token_env: DETENT_ENTITLEMENT_ADMIN_TOKEN
```

The environment variable must contain at least 32 bytes of independently
generated secret material. Do not reuse the metadata reporter token. Ordinary
staff, support impersonation, organization owners/admins and runners cannot
grant complimentary access. This credential changes allowances only; it gives
no customer project access or runner execution permission. Keeping it unconfigured
disables remote plan mutations while allowing free access and usage reporting.

The authenticated operator posts to
`/api/v2/organizations/{organization}/entitlements`. Each command requires a
unique `idempotency_key`, current `expected_revision`, `action`, and bounded
nonempty audit `reason`. The transaction records operator identity, command,
reason, time and request hash. A matching retry changes nothing; a changed
request under an existing key or a stale revision is rejected. Audit records
remain in the owning organization database and never enter routine analytics.

```json
{
  "idempotency_key": "pilot-access-example",
  "expected_revision": 1,
  "action": "grant",
  "grant_id": "pilot_access",
  "plan": {"id": "pilot_extended", "version": 1},
  "scope": ["projects", "concurrent_work"],
  "reason": "Approved pilot access"
}
```

Grant `starts_at` defaults to transaction time; optional `expires_at` uses an
RFC3339 timestamp after the start. A `revoke` command names `grant_id` and a
reason. `base` assigns a plan version. `subscription` assigns a plan version
with a required future validity deadline; `end_subscription` clears that
derived access. These commands are a payment-independent input contract for a
future trusted subscription adapter, not a public webhook or payment processor.

Resolution at the allocation transaction's clock sample is:

1. Start with the organization base assignment.
2. A subscription with a validity deadline strictly after now replaces the base.
3. For each active grant, take the maximum allowance for each explicitly scoped
   resource and enable each explicitly scoped feature present in its plan.
   Grants do not add quantities together or enable other features from that plan.
4. At the exact expiry timestamp the grant/subscription is no longer valid.
   Revoked grants have no effect. Stored assignments and customer data remain.
5. Membership, project/runner permissions, policy approvals, repository gates,
   host capacity and provider safety/budget brakes always continue to apply.

## Measurement and exhaustion policy

| Allowance | Consumption and enforcement |
| --- | --- |
| Members | Locally active memberships plus unexpired invitation seat reservations. A successful join consumes its reservation. Invitation failure releases it; otherwise it expires. Provider-side membership alone cannot bypass local admission. |
| Projects/repositories | Stored allocations in the dedicated tenant Hub. Their creation/binding transaction must fit the allowance. |
| Registered/connected runners | Non-revoked runner registrations, including legacy machine identities without a runner registration; connection means a heartbeat strictly newer than the configured cutoff. New enrollments and reconnecting idle runners cannot grow the count beyond the limit. |
| Concurrent work | Unreleased leases whose expiry is strictly after the transaction clock. The existing fenced lease is the reservation; failed transactions, explicit release and expiry free capacity. An idempotent claim is checked before allocating again. |
| API mutations | Accepted allocation-changing transactions per configured UTC window. Matching native command retries do not consume another unit. Reads, billing, renew/release and completion/checkpoint events remain available. |
| Ingested events | Newly stored collaboration events per window. Native retries and ordered duplicate event sequences do not add an event. |
| Collaboration bytes | UTF-8 bytes of issue text/labels/assignees, comments, versions, event payload/actor records, idempotency responses, attempts, artifact references and import snapshots. This is logical retained payload, not disk billing. Database/WAL bytes are separately reported. |
| History records | Collaboration events, immutable versions and ordered attempt-event receipts. |

Checks and allocation writes use the same transaction. An operation that increases
an exhausted resource is rolled back with HTTP 429, `allowance_exhausted`, the
resource, allowance and prior consumption. Reductions and unchanged resources
remain possible after a downgrade. Data is not automatically deleted to make
an organization fit a smaller plan. Fenced checkpoint and completion events may
grow bounded, validated execution history beyond the tier; ordinary edits do not
receive this exemption. Existing request/payload and lease safety limits still
bound each event. The worker must checkpoint or finish when ordinary mutations
are exhausted. Existing history can still be read and exported; owner billing
and administrator plan pages remain accessible.

Usage is available at `/organization/plan` to owners/admins and through owner-only
`/api/cloud/billing`. The existing staff/reporter metadata endpoint includes the
same bounded entitlement summary. Project access remains separate. No permanent
banner is added. Model-provider spending is not presented as a Detent charge.

## Hosted artifact storage and relay

Customer-mode artifact services never contact the hosted allowance endpoint and
keep their configured local storage policy. Hosted services opt in explicitly,
use the organization-bound publisher credential, and request allowances from the
owning Hub. The first authorized hosted service ID binds the organization's
artifact allowance owner. A second ID is refused; changing owners requires an
operator-controlled migration of the catalog, not another independent service
with the same full quota. Run exactly one process/catalog per service identity.

To enable hosted artifacts, include the `hosted_artifacts` feature and explicitly
configure all of `artifact_retained_bytes`, `artifact_reserved_bytes`,
`artifact_bytes`, `artifact_upload_bytes`, `artifact_retention_seconds`, and
`relay_bytes`. They default to zero. Service policy intersects these allowances;
a paid plan cannot raise the service's independent safety limits.

Reservation admission is serialized in the artifact service and considers stored
bytes, unfinished reservations and relay headroom for upload plus verification
readback. Duplicate reservations and verified parts reuse existing records.
Each reservation pins its admitted part limit and retention deadline. Downgrades
block new excess reservations while existing uploads may finish within their
reserved bytes, pinned limits and original deadline. Reads/export and deletion
remain available; their measured traffic can exceed the allowance and block new
uploads for the rest of the window. This policy does not promise unlimited relay.

The service records attempted upload bytes, returned download bytes, and logical
storage operations in bounded minute buckets. Failed writes conservatively count
attempted bytes; SDK retries, HTTP overhead and provider-internal operations are
not invoice measurements. The existing maintenance loop reports only aggregate
retained/reserved bytes, current-window relay bytes, operation counts and sample
time. Repeated samples use maxima for window counters, not additive charging.
The Hub never reads the artifact database or receives object keys/content through
this reporting path. Gauges reflect the last received sample and can lag during
an outage; the owning service enforces its allocations locally.

## Cost evidence and retention

Hub telemetry records request counts, response bytes, aggregate handler duration,
heartbeat counts, accepted mutations/events and artifact usage. Telemetry stores
only fixed metric names, integer counts and window timestamps. Expired buckets
are pruned on subsequent samples; no public ingestion or marginal-cost claim is
derived from these counters. Operators must measure idle process/database cost,
heartbeat/request load, database growth, retention and their provider's billed
traffic/operations before setting prices. The metrics are inputs for that review,
not customer model spend or a dollar estimate.

Allocation/grant records, audit records and customer collaboration history are
durable records, distinct from bounded operational telemetry. This change does
not silently purge customer history, alter public retention promises, configure
WorkOS, enable a production artifact service or launch paid billing.
