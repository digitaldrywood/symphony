# Self-hosted Hub operations

One customer-controlled Linux host or ordinary VM, one Hub process, and local
durable storage are the supported deployment described here. Use the released
Detent binary and systemd, with a TLS reverse proxy. This is single-host operation
with explicit downtime for maintenance; there is no active-active replication,
automatic failover, or high availability guarantee. Never share a live SQLite
file over NFS/SMB, mount it into runners, or run a second owner against it.

No WorkOS, Stripe, Detent Cloud account, `cloud.detent.build` callback, or paid
infrastructure is required. The native-only example disables GitHub transport.
Ordinary local Detent remains supported separately: omit `client.hub_url` and
run `detent` using local configuration and the selected local/tracker backend.
Durable artifacts are optional in both modes; local-only artifact availability
depends on retaining the runner and its workspace.

## Install one host

Select a reviewed release version and architecture from the existing
[release distribution](release.md). Verify the archive/package checksum before
installation and keep that version's binary, checksum and deployment files for
recovery. Linux packages install `/usr/bin/detent`; archive installations must
install the executable at that path or adjust the example unit. Do not use an
unversioned download URL for an upgrade. The release includes this runbook and
the [Hub unit](examples/hub/detent-hub.service) and
[Caddy example](examples/hub/Caddyfile); equivalent files are in the same tagged
source checkout. Native package documentation lives under `/usr/share/doc/detent/docs`.

On a clean Linux host with systemd, install the selected Detent package and a
separately pinned Caddy package through your normal OS package process. Then:

```sh
sudo useradd --system --home-dir /var/lib/detent-hub --shell /usr/sbin/nologin detent-hub
sudo install -d -m 0700 -o detent-hub -g detent-hub /var/lib/detent-hub
sudo install -d -m 0700 -o root -g root /etc/detent-hub
sudo install -m 0644 docs/examples/hub/detent-hub.service /etc/systemd/system/detent-hub.service
sudo install -m 0600 /dev/null /etc/detent-hub/credentials.env
sudoedit /etc/detent-hub/credentials.env
```

Use the corresponding `/usr/share/doc/detent/docs/examples/hub` source path when installing
from a native package without a source checkout. In `credentials.env`, set
`DETENT_HUB_ADMIN_TOKEN=<fresh-high-entropy-token-from-your-secret-manager>`.
Generate at least 32 random bytes; the bracketed value is a placeholder, not a
credential. Keep the real value out of shell arguments/history, repositories,
screenshots, logs and issue reports. systemd reads the root-only file before
dropping privileges. Configure your real DNS name in Caddy, install its reviewed
configuration, allow inbound HTTPS, and restrict port 7777 to loopback.

```sh
sudo systemd-analyze verify /etc/systemd/system/detent-hub.service
sudo systemctl daemon-reload
sudo systemctl enable --now detent-hub
sudo systemctl status detent-hub
```

These commands operate on the example installation only. The ordinary
`detent service install` command supervises the local orchestrator; use this
separate Hub unit for `hub serve`. The Hub has no source checkouts, agent login
sessions or execution workspaces on this host. Do not expose the backend port
by relying on `--trusted-proxy` alone: that flag declares trust, it does not
create TLS or configure a firewall. Alternatively use `--tls-cert` and
`--tls-key` with an explicit public listen address. Do not log Authorization
headers or request/response bodies at the proxy. Keep private keys readable
only by the TLS owner. The API continues to enforce scoped bearer auth behind
the proxy; arbitrary identity headers are not an authentication integration.

## Durable paths and outbound access

| Location | Owner and recovery treatment |
| --- | --- |
| `/var/lib/detent-hub/hub.db` and SQLite sidecars | Hub-only local disk; back up through the owner or stopped-owner command |
| `/var/lib/detent-hub/hub.db.lock` | Ownership coordination; not a backup or a lock to manually remove while running |
| `/etc/detent-hub/credentials.env` | Secret manager/root; back up separately with restricted access and revocation records |
| systemd unit, TLS configuration/certificates | Versioned operator configuration; preserve with the selected binary/checksum |
| `/var/lib/detent-hub/backups/` | Private example staging path; copy finished snapshots to encrypted off-host backup storage |
| runner identity, config and workspace roots | Private runner-local state; reenroll credentials on migration |
| artifact catalog and object bucket | Separate durable service; Hub snapshots contain references, not artifact bytes |

Native metadata service operation requires no outbound Internet connection.
TLS certificate automation may require DNS/ACME access; private PKI can avoid
that dependency. Operators installing packages need access to their package
source. Runners need HTTPS to this Hub and, when configured, the independent
artifact service; their chosen Git host, model providers and build tools need
their own outbound access. These are customer-selected endpoints, not Detent
Cloud dependencies. Artifact service outbound access is limited to the selected
Hub and object store (plus any explicitly selected credential provider).

For GitHub compatibility, remove `--github-disabled` only after configuring
GitHub credentials for the dedicated service user and, if used, a webhook HMAC
secret through the existing CLI environment options. Allow GitHub API traffic
and the separately selected Git transport. Follow [GitHub profiles](github-profiles.md)
for import, mirror cutover and projection; native-only mode does not project
changes to GitHub. The example service's `ProtectHome` means interactive login
files in a human home directory are deliberately unavailable.

## Authentication, projects and runners

The bootstrap administrator is inserted once. Changing its environment value on
restart does not rotate it or resurrect a revoked token. Use the existing
`POST /api/v1/tokens`, `POST /api/v1/tokens/{id}/rotate`, and
`DELETE /api/v1/tokens/{id}` operations. Plaintext creation/rotation responses
are returned once with `Cache-Control: no-store`; persist them directly to
private secret storage. Generic worker/operator tokens do not automatically
expire; explicitly rotate/revoke them and track their owners. Issue project
grants through `/api/v2/tokens/{id}/grants`. Use individual operator credentials
so author IDs remain attributable. Generic scoped bearer authentication works
without hosted identity; do not assume an OIDC proxy can mint Hub credentials.

Follow [Hub API enrollment](hub-api.md#scoped-runner-onboarding) for exact
JSON requests and [repository policy](config.md#repository-policy-with-hub-execution)
for approval. The supported sequence is:

1. Create the organization/project and its workflow states as administrator.
2. On each runner run `detent hub runner init --hub-url https://hub.example.com --identity-file /private/runner/identity.json`.
   Give the generated binding IDs to the administrator. Never copy one runner's
   identity file to another machine.
3. Create a one-use enrollment for those exact IDs, project grants and required
   operations. Deliver it privately and run `detent hub runner enroll` with the
   corresponding identity file, Hub URL and enrollment environment variable.
4. Configure `client.hub_url`, `identity_file`, `organization_id` and
   `native_projects` on each runner. Keep backend/provider login on that runner.
5. Approve the trusted repository/workflow descriptor with `detent hub policy`.
   Configure required tags or exact runner/machine IDs in repository policy;
   set administrator-owned runner tags, project access and capacity through
   `detent hub runner`. A runner cannot grant itself tags or repository access.
6. Check two distinct runners against their selectors before enabling real work.
   An exact offline host stays queued; tags are routing constraints and do not
   grant authority. Logical runners sharing a host share its capacity.

Enrollment grants last at most 900 seconds. Enrolled credentials last 24 hours
from enrollment/renewal; renew before expiry through the supported runner CLI.
Revocation is permanent for that identity. After expiry/revocation or migration,
initialize a fresh binding and have an administrator enroll it, then update exact
ID selectors and approve the resulting policy. Preserve old principal rows for
history; never reactivate them by editing SQLite. See `detent hub runner --help`
for renewal, rotation, revocation, routing and fleet inspection commands.

## Optional durable artifacts

Use the existing [artifact deployment](artifacts-deployment.md) and its systemd,
Caddy and S3-compatible configuration examples. It is an independently available
authorization/download process with a durable catalog/outbox and a private object
store. Execution runners may be disposable; the artifact process, catalog and
bucket are not. Keep them outside runner workspaces. No AWS compute, hosted
account or particular storage vendor is mandatory.

Run `detent artifacts --artifact-config /private/service.json verify-storage`
against the selected endpoint before enabling it. Only a passing live probe
establishes that provider's required conditional-write, integrity and optional
versioning behavior; local fixtures are not live provider certification.
Configure a dedicated scoped publisher credential, project service binding and
finite retention/capacity policy. Runners receive no bucket credentials. Omitting
durable configuration remains truthful local-only operation.

## Backup and export

`detent hub backup` is also the supported full-instance collaboration export.
It includes organizations, projects, stable issue/comment/change IDs, versions,
events, provenance, external references, identity rows, grants, approved policies,
outbox and artifact references. It is a SQLite snapshot, not a per-project JSON
merge format. Importing into an existing Hub or selectively merging tenants is
unsupported. Raw artifacts, TLS files, runner workspaces and provider credentials
are separate backups. Treat snapshots as sensitive customer data even though
Hub bearer credentials are stored as hashes.

Drain runners and stop the Hub before CLI maintenance. `backup` and `verify`
acquire its ownership lock and fail if another owner is running. They check
identity, integrity, foreign keys and supported schema without upgrading the
source. The existing service-owned `Service.Backup` API remains available for
embedded online snapshots; no unauthenticated backup-download endpoint is added.

```sh
sudo systemctl stop detent-hub
sudo -u detent-hub install -d -m 0700 /var/lib/detent-hub/backups
sudo -u detent-hub /usr/bin/detent hub backup --database /var/lib/detent-hub/hub.db --output '/var/lib/detent-hub/backups/<unique-snapshot>.db'
sudo -u detent-hub /usr/bin/detent hub verify --database '/var/lib/detent-hub/backups/<unique-snapshot>.db'
sudo systemctl start detent-hub
```

Replace angle-bracket placeholders before execution. Backup output must not
exist and must differ from the source. Copy only the completed snapshot to the
backup system. Never copy live DB/WAL/SHM files. Record checksum, capture time,
binary version, returned schema version, retention deadline, custody policy and
configuration revision outside the snapshot. Test an isolated restore regularly.

## Restore/import and move between Hubs

Use a trusted, unexpired snapshot and a **new** local destination. Fence the old
Hub and stop its runners before moving production traffic. Restoring does not
stop processes or revoke credentials on the source machine. A restored clone
must never run concurrently against the same external projection/workspace.

Load a fresh high-entropy `DETENT_HUB_ADMIN_TOKEN` from private secret storage in
the maintenance process environment, then run as the database owner:

```sh
detent hub verify --database '/private/backup/<snapshot>.db'
detent hub restore --database '/private/backup/<snapshot>.db' --output /private/restored/hub.db
detent hub serve --database /private/restored/hub.db --listen 127.0.0.1:7777 --github-disabled
```

Restore validates the source, copies through SQLite backup, migrates a private
staging copy, checks integrity, and publishes the new path without overwriting
anything. Before publication it revokes every copied generic/runner credential
and enrollment, releases every active lease, rotates cursor authority and creates
a fresh administrator principal. The supplied token must differ from every
source credential. JSON output gives the new administrator ID and schema, never
the token. Existing identities, grants, event ordering, policy provenance and
external references remain intact for attribution. Old pagination cursors must
be discarded. Attempts with released leases are presented as interrupted by the
existing run-history logic; they do not authorize resumed side effects.

Verify authenticated `/health`, content/history and references while isolated.
Create fresh human/operator and integration credentials, restore only currently
approved project grants, reenroll runners using new bindings, and reapprove
policies with exact ID selectors. Reconfigure review-policy principal IDs where
credentials changed. Set the service environment to the fresh administrator
credential; ordinary startup will not replace the copied revoked bootstrap row.

For artifacts, preserve catalog/service/artifact IDs and object versions, update
the trusted Hub origin, replace publisher credentials, and update the project
binding before reopening downloads. Existing grants depend on revoked principals
and must be reacquired. Keep current deletion markers independently of catalog
rollback and follow the artifact restore procedure. Hub backup alone cannot
restore object access. Keep GitHub transport disabled until an operator checks
pending outbox work against current GitHub state and resolves any already-applied
external effects; restoring a snapshot does not roll back a GitHub mutation.

## Upgrade, interruption and compatibility

Pin a release and retain the old binary plus a verified pre-upgrade snapshot.
Drain runners, stop the unit, create/verify a backup, install the selected binary,
then start the Hub. Startup applies embedded forward goose migrations before
health becomes ready. Each migration is transactional; a crash rolls back the
unfinished migration. Earlier completed migrations remain applied and the same
binary retries the remainder on restart. Fix disk/permission problems first.
Never manually advance the schema table or run the unrelated local orchestrator
`make db-migrate` against a Hub database.

Schema 19 repairs the two formerly valid schema-18 branches. Migration 18 remains
`00018_change_viewed_files.sql`; onboarding moves to
`00019_project_onboarding.sql`. The forward-only, transactional migration creates
only missing tables. A viewed-files schema 18 retains all viewed markers and gains
onboarding; an onboarding schema 18 retains progress, revision and timestamps and
gains viewed-files storage. Hosted identity and change-version rows stay in place.
Both-table databases also retain their rows. Ordinary startup handles these
variants automatically; no schema-version edits or table deletion is required.
Use the backup procedure above before upgrading; keep the snapshot for rollback.

If restore is interrupted before publication, the requested destination is absent.
A private `.hub-restore-*` directory may remain beside it; retain it for diagnosis
or remove it only after confirming no maintenance process owns it. Retry from the
original backup to a new destination. Never serve a staging database. Once a
destination is published, it has already been migrated and old authority fenced.

A binary rejects a schema newer than it supports with `ErrUnsupportedSchema`
(`database=N supported=M`); there is no automatic downgrade. Keep the failed
database for diagnosis. Use the newer compatible binary, or restore the
pre-upgrade snapshot into a new path with a binary supporting that snapshot.
Switch the unit's database path only after validation. Never point an older binary
at the upgraded live file. Pre-upgrade backups retain the old schema because
offline backup does not migrate it. Recovery commands are available beginning
with the release that packages this runbook; keep that recovery binary too.

Prefer identical release versions across Hub and runners. Rolling overlap is
supported only where `/api/v2/capabilities` and the runner agree on protocol major
2, schema 1 and all required features. Version strings alone do not confer
compatibility. Unsupported majors/missing required features fail closed before
claim/start; older clients cannot bypass approved policy or provider reservations.
The v1 compatibility API remains for legacy configured clients and requires
GitHub compatibility configuration; it is not a fallback for a failed native
negotiation. Fixtures cover old/new feature sets, not every historical binary.

## Retention, deletion and expiration

Native collaboration currently has no automatic expiry and no supported
per-issue/organization purge command. Current content, revisions, comments,
Changes, events, idempotency responses and identity attribution remain until
whole-instance retirement. Do not promise scoped erasure or disable append-only
triggers. Choose a separate Hub per retention boundary if whole-instance deletion
is required. A finite per-record retention requirement requires further product
work before adopting this deployment for that requirement.

Operators must define a finite backup lifetime, log rotation and export expiry
in their own backup system. For example, a reviewed policy can expire snapshots
after 30 days; Detent does not schedule that expiration. Record the deadline and
refuse expired snapshots during manual restore. Do not keep indefinite copies in
download folders. Revocation records and the retirement/deletion ledger belong
outside rollbackable backups. Restore always invalidates old Hub authority, but
it does not erase previously exported content or apply a customer deletion ledger.

Whole-instance retirement means stopping/fencing Hub and runners, disabling
ingress and artifact authorization, recording the retired scope/IDs in the
external ledger, and deleting the database, sidecars, private staging directories,
exports, backups, service logs and relevant encryption keys through the storage
owner's approved erasure process. Verify expiry/deletion in every backup replica
and disallow restoring a retired snapshot. Do not reuse retired IDs. GitHub
projections and artifact objects/catalogs need their own authorized deletion and
verification; database deletion does not erase them.

Webhook raw payloads have the existing configurable retention (default 7 days);
typed collaboration/projection records are separate retained data. Artifact
policy enforces live/abandoned-upload/deletion-marker lifetimes; its backup system
owns object/catalog backup expiration. Deletion-marker retention must outlive
artifact backups plus abandoned uploads. See [artifact retention](artifacts-deployment.md#retention-and-accounting).

## Diagnostics

Use authenticated `/health`, `detent doctor`, the existing fleet/queue surfaces,
and private service logs. Supply credentials through environment/private files;
do not paste bearer headers or content into diagnostic reports.

| Symptom | Evidence and next action |
| --- | --- |
| No matching runner | Fleet/queue exclusion reason, heartbeat, tags, exact IDs, project access, drain state and host capacity; fix the intended binding or bring that host online; never widen selectors automatically |
| Expired/revoked identity | Unauthorized response and local identity expiry; renew before expiry, otherwise initialize/enroll a new binding and approve changed ID selectors |
| Policy mismatch | Hub policy descriptor/approval and locally resolved policy ID; inspect trusted repository/workflow changes and obtain explicit administrator approval |
| Storage outage | Artifact error (`storage_unreachable`, integrity/missing/expiry reason), service maintenance logs and `verify-storage`; restore configured storage access, preserve pending catalog/outbox, never advertise unverified bytes |
| GitHub compatibility/projection failure | `/api/v1/outbox/health`, `/api/v1/repositories/freshness`, `/health` and GitHub profile; distinguish disabled transport, missing permissions, rate budget, stale projection, conflict and dead-letter/operator action before retrying |
| Startup/schema failure | systemd exit status and private journal, `hub verify` while stopped; check local disk, permissions, ownership and binary/schema versions; do not remove an active lock |

## Reproduce the validation fixture

Build `tmp/detent` with `make build` and run
`python3 scripts/hub-smoke.py tmp/detent`. The fixture installs a binary into an
empty private directory, uses ephemeral loopback ports, verifies bootstrap,
restart, export/verify/import and revoked-source authentication, and cleans up
only its own child processes/files. No real credentials are used.

For Linux/container validation, cross-build the binary and Hub test executable
for the container architecture, copy them and the smoke script into a fixture
directory, then run a Python-capable Linux image with `--network none`, a read-only
fixture mount and private writable temp storage. Run the smoke script plus the
Hub recovery/routing/protocol tests there. No production deployment or paid
resources are needed. The required repository gate remains `make check`.
