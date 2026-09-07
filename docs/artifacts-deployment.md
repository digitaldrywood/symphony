# Artifact service deployment and operations

This is a deployment template, not live provisioning. Build the repository's
single binary with `make build`. A customer can leave durable artifacts disabled;
no hosted account or bucket is required for local-only operation.

## Configure the independent service

[service.json](examples/artifacts/service.json),
[systemd unit](examples/artifacts/detent-artifacts.service), and
[Caddy configuration](examples/artifacts/Caddyfile) describe a portable Linux
installation using an existing private bucket. Equivalent process supervision and
TLS proxies work on other supported platforms. No AWS compute or database service
is required. Install the binary as `/usr/local/bin/detent`, create the dedicated
service user, and keep configuration/credentials readable only by that user.
Use a local durable disk for `/var/lib/detent-artifacts`, independently of runner
workspaces. Install Caddy separately using your deployment's pinned version.
Configure request concurrency/rate limits at the trusted ingress for your memory
budget. The example request ceiling permits a maximum-size base64 video request.
Do not enable CDN/public bucket access or proxy Authorization/body logging.

Replace every example ID, host, bucket and policy with an operator-approved value.
The numeric values are finite examples, not Detent plan allowances or retention
promises. `deletion_record_seconds` must exceed `backup_seconds` plus
`abandoned_upload_seconds`. Protect and expire object backups, service logs and
SQLite backups using the same explicit custody policy. The service enforces live
artifact/abandoned/deletion-record expiry; the backup system owns backup expiry.

Provide the dedicated scoped Hub publisher token through
`DETENT_ARTIFACT_PUBLISHER_TOKEN` in `/etc/detent-artifacts/credentials.env`.
Storage credentials stay in this process's standard AWS SDK credential chain:
workload role/profile on AWS, or locally configured `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN`. Never place them in
Hub, runner config, the repository, command arguments, or logs. Select credentials
with only private-bucket GET/HEAD, conditional PUT, DELETE and version access when
required. Use provider-supported encryption and rotation policies. No identity
federation parity between vendors is assumed.

| Provider | `endpoint` | `region` for signing | Addressing |
| --- | --- | --- | --- |
| AWS S3 | Empty (SDK resolves endpoint) | Actual bucket region, e.g. `us-east-1` | `path_style: false` |
| DigitalOcean Spaces | Regional origin, e.g. `https://nyc3.digitaloceanspaces.com` | `us-east-1` per AWS SDK setup; endpoint selects region | `path_style: false` |
| Tigris | `https://t3.storage.dev` | `auto` | Configure the provider-supported style; fixtures cover path addressing |

These are configuration examples, not live certifications. Compare the current
[Spaces SDK guidance](https://docs.digitalocean.com/products/spaces/how-to/use-aws-sdks/),
[Spaces compatibility](https://docs.digitalocean.com/products/spaces/reference/s3-compatibility/),
[AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html),
and [Tigris endpoint guidance](https://fly.io/docs/tigris/).
Spaces documents partial S3 support; do not infer conditional PUT semantics from
conditional GET support. `require_versioning: true` additionally requires a real
version ID and version-pinned read/delete. When false, immutable IDs, conditional
create and hash checks still apply. Enabling versioning is a separate provider
operation; this service never changes bucket settings.

Before serving, run:

```sh
detent artifacts --artifact-config /etc/detent-artifacts/service.json verify-storage
```

The command creates an opaque temporary probe object, verifies conditional create
rejects replacement, round-trips exact bytes, deletes and confirms absence. It
fails closed with `unsupported_capability`, `checksum_mismatch`, or
`storage_unreachable`; do not remove the conditional-write requirement to force
a provider through. The serve command repeats this probe. It incurs small storage
requests; interrupted probe cleanup may require operator cleanup of `detent-probe/`.
A provider without the required semantics is unsupported until its capability or
an explicitly reviewed adapter changes. Automated tests use local protocol fixtures
and require no customer credentials. Never claim a live bucket passed from fixture
results alone.

## Bind Hub and runners

Create a dedicated Hub worker credential with grants for each project that uses
this service using the existing [Hub API](hub-api.md). As a project administrator,
PUT this body to
`/api/v2/organizations/{org}/projects/{project}/artifact-services/{service}`:

```json
{
  "service_id": "service_11111111111111111111111111111111",
  "origin": "https://artifacts.example.com",
  "mode": "customer",
  "hosted_opt_in": false,
  "publisher_token_id": "replace-with-dedicated-token-id"
}
```

The publisher token ID is a reference; never put the token value in the binding.
The selected service must trust that exact Hub origin and organization. It can
serve several authorized projects in the same organization through one catalog;
`--project` optionally restricts it to one project. Hosted organization accounting
requires one authoritative catalog for the whole organization. Multiple independent
catalogs cannot enforce one global allowance and are not a supported hosted topology.

Configure the native runner's existing `client` stanza:

```yaml
client:
  hub_url: https://hub.example.com
  identity_file: /private/path/runner-identity.json
  organization_id: org_11111111111111111111111111111111
  native_projects:
    example: prj_11111111111111111111111111111111
  artifact_service_id: service_11111111111111111111111111111111
  artifact_bytes: 67108864
```

`artifact_bytes` reserves a finite budget separately for log and review bundle,
including all manifests. It must be greater than 1 MiB and at most 256 MiB and
fit the service allowance. Omitting both artifact fields keeps local-only behavior.
The runner does not get storage credentials. The registered service origin,
explicit mode and hosted consent must match; there is no fallback provider.

For the approved initial Detent-hosted deployment, use a private Spaces bucket
and separate artifact process/catalog, set `mode: hosted` and `hosted_opt_in: true`
in both service configuration and Hub binding, and configure finite organization
limits. This is an explicit change of custody. No customer storage credentials or
existing bytes are migrated. The `Allowances` interface and `Usage` result are the
#2195 integration points; the standalone process uses the configured policy snapshot.
Apply revised allowances by restarting this independent service with reviewed
configuration. Existing retention dates remain unchanged on downgrade.

## Read and diagnose

Run/Change details show references before requesting content. Enter a current
project credential to obtain a bounded read grant; the browser fetches the manifest
and selected objects directly from the artifact service. Raw content is not proxied
through Hub. The credential and grant must not be copied into telemetry, browser
storage or shared links. Every object requires current authorization; a revoked
credential cannot reuse an unexpired grant after revocation reaches Hub.

Read errors distinguish expiry, authorization, missing/corrupt objects and storage
outage. A partial log includes only verified chunks. No published reference means
there is no verified durable copy advertised, including when uploads are queued.
The service's ten-second maintenance loop seals abandoned logs, cleans objects,
and drains at most 32 receipts per batch. It retries automatically after a storage
or Hub outage; maintenance logs contain no underlying provider response/content.

The content API is versioned under `/v1`: reserve with `POST /uploads`, append with
`PUT /uploads/{artifact}/parts`, finalize with `POST /uploads/{artifact}/finalize`,
and read `/artifacts/{artifact}/manifests/{revision}` with optional
`/objects/{object}`. Strict JSON types in `internal/artifact` are the request
contract. Read grants cannot upload. Upload calls require a current fenced running
attempt. Mutation bodies have finite limits; ranges and multipart are unsupported.

## Backup and restore

The catalog is private to its owning service. For command-line maintenance, stop
only the independent artifact service, then run:

```sh
detent artifacts --artifact-config /etc/detent-artifacts/service.json usage
detent artifacts --artifact-config /etc/detent-artifacts/service.json backup --output /private/backup/new-catalog.db
```

Restart the artifact service after maintenance. Downloads are unavailable during
this maintenance window; execution runners need not run. The library's `Backup`
method uses SQLite's online backup API when invoked by its owning service. The CLI
intentionally refuses to open a running catalog. Never copy a live DB/WAL/SHM trio,
replace an existing backup path, or mount its database into another service.

Back up configuration, manifest/catalog/outbox state, and provider object versions
under the selected custody policy. Copying the SQLite catalog does not back up
bucket bytes. Keep the newest content-free `{artifact}/deleted` markers independently
of rollbackable object backups. Their finite retention is longer than the supported
backup lifetime and abandoned-upload window. Never restore an older deletion ledger,
expired backup, revoked identity configuration or obsolete hosted custody choice.

Restore into a separate local path with the service stopped. Verify backup age,
SQLite integrity, schema identity, current membership/credentials, bucket access
and deletion markers before reopening downloads. The service rejects unknown/future
schemas and checks markers before content access or uploads. Exact missing versions
remain `missing`; regenerated content uses a new artifact. Reads still reject expiry
while cleanup is pending. Test a restored catalog against a deleted artifact and a
revoked reader before accepting the restore. No recovery-time or durability SLA is
implied by these templates.

## Retention and accounting

The service meters unique verified payload bytes and every retained manifest;
reservations also cover uncertain/abandoned writes until cleanup. Metadata, deletion
records, SQLite WAL/backups, old provider versions, probe objects and provider logs
have additional infrastructure cost and must be budgeted independently. Reads
check deletion markers and fetch complete objects for integrity. Count PUT, GET,
HEAD, DELETE, capability probes, retries, bytes read for verification, browser
downloads, cross-region traffic, egress, disk, TLS/compute and backup operations.
The provider's usage report is the billing source; `usage` reports logical retained
and reserved artifact bytes, not a provider invoice.

Customer-managed customers pay their own provider storage, requests, egress,
backup and service-operation charges. Hosted artifact prices and retention bounds
are operator configuration owned by #2195; no free quantity or price is promised.
A downgrade can reject new reservations while old objects remain readable until
their accepted expiry. Retention shortening, provider changes and custody migrations
require an explicit separate operator action; none happens automatically.
