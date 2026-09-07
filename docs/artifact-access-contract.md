# Durable artifact access contract

[Architecture RFC](cloud-hub-rfc.md#durable-artifacts-and-immutable-changes) · [Design #2189](https://github.com/digitaldrywood/detent/issues/2189) · [Delivery #2190](https://github.com/digitaldrywood/detent/issues/2190)

The September 7, 2026 operator decision supersedes the AWS-specific proposal.
Local-only operation, customer-managed durable storage, and separately opted-in
hosted storage are approved. DigitalOcean Spaces is the selected initial hosted
provider. Dropbox was a typo; no Dropbox adapter is intended. No live provisioning,
purchases, automatic migration, or fallback to hosted custody is authorized.

## Custody and deployment

| Mode | Content location | Requirements |
| --- | --- | --- |
| Local-only (default) | Runner workspace and existing local evidence | No hosted account, storage credentials, or storage plan. Unuploaded content does not survive machine loss |
| Customer-managed | Customer artifact service, catalog, bucket and backups | Customer configures and pays for their infrastructure. No hosted plan restriction |
| Hosted, explicit opt-in | Separate Detent artifact service, catalog, bucket and backups | Both service configuration and administrator-approved Hub binding select hosted custody; finite organization allowances |

`internal/artifact.Storage` is the provider boundary. The first adapter uses the
S3 protocol with configurable endpoint, signing region, bucket, path or virtual
host addressing, and optional required versioning. AWS S3, Tigris, and Spaces
configuration contracts have local fixture tests. Other providers must pass the
same checks; S3 compatibility does not mean identical behavior.

`detent artifacts serve` runs the portable authorization/download service. Its
catalog uses the repository's goose, modernc SQLite, instance lock, and single
owner conventions. API Gateway, Lambda, DynamoDB, WorkOS, and payment processing
are not dependencies of a customer deployment. The service runs independently
of all execution runners, behind a TLS reverse proxy on a configured loopback
port. Never put its SQLite files on NFS/SMB or open one catalog from two processes.

See [deployment and operations](artifacts-deployment.md) for reproducible examples,
finite policy configuration, provider verification, and restore procedures.

## Authorization and privacy

The current portable identity is a scoped Hub project credential. Hub is the
membership authority: removing its project grant, revoking it, or expiring it
prevents new content requests. Native runner upload authority additionally needs
the current lease, fencing token, running attempt and matching run/work item.

A project administrator binds an artifact service origin and a dedicated scoped
publication credential. Only that credential can publish receipts and introspect
read grants for the binding. Runners and browsers never receive bucket credentials.
Storage credentials use the service's local SDK credential chain, including AWS
workload roles where configured. Customer Hub operation requires no storage keys
and no hosted account. TLS origins cannot include user info, paths, or queries;
HTTP origins are limited to explicitly configured loopback fixtures.

1. The member requests a grant for an exact artifact and manifest revision from
   Hub. The current project grant is checked transactionally. Hub stores only a
   hash of the random grant, its principal, scope, revision and expiry.
2. The grant lasts at most 60 seconds and never beyond artifact expiry. It is
   returned with the administrator-approved service origin and manifest digest.
3. The browser sends it in an Authorization header directly to the artifact
   service. The service checks current Hub authorization for each manifest or
   object request. There is no positive authorization cache or presigned URL.
4. The service verifies the full object size and SHA-256 before returning bytes.
   The browser independently verifies the manifest hash and each downloaded object.

All runners may be stopped. Hub, the artifact service/catalog, storage, and their
network paths must still be available. Hub outage fails authorization closed;
storage outage is a distinct error. Authorization already admitted before a
revocation can finish, and downloaded bytes cannot be recalled. Identity-provider
changes must first reach Hub's authoritative membership state; #2193 owns that
integration and its propagation bound.

Ordinary staff and operator views contain metadata only. Billing or platform
administration does not grant content access. Exceptional support content access
must use the scoped, privileged, audited WorkOS support impersonation boundary
approved in #2193. This implementation adds no staff download bypass. The portable
viewer requires an explicit project credential and never substitutes the dashboard's
operator credential for content access. WorkOS support sessions must enter through
the same current project authorization boundary when #2193 delivers that identity.

| Hub metadata | Selected artifact custody only |
| --- | --- |
| Opaque org/project/work-item/run/attempt/version/service/artifact IDs, kind, immutable manifest reference/hash, aggregate size/count, expiry, state and observation time | Manifest body, source filenames and context, raw diffs/logs, screenshots/video, bucket and object locations/versions, storage credentials, catalog and backups |
| Approved service origin and publication credential ID; hashed short-lived read grants | Plain read grants and member credentials remain transient; never persist them in request logs, exports, analytics, URLs or browser storage |

The service emits closed error codes, never storage-provider error bodies or
content. Reverse proxies must also disable request-body and Authorization logging.
CORS permits only configured exact browser origins. Responses use no-store,
no-referrer and nosniff. The browser omits cross-origin cookies and rejects redirects.
Text uses `textContent`; active HTML/SVG documents are unsupported. Media downloads
use a neutral attachment filename. Ordinary authored collaboration text retains
its existing custody rules; deliberately quoted code is still collaboration content.

## Immutable capture and upload

`CaptureGit` resolves explicit base/head commits from local Git, computes their
merge base, disables external diff/text conversion, and captures a patch plus
complete text for every changed file at merge base and head. Capture metadata
records base, head, merge base, context lines, `file_context: changed_files`, and
`working_tree: false`. The base side is the merge-base tree, including when base
and head have diverged. Unchanged files are not present: the bundle supports a
changed-file viewer, not reconstruction of a repository. No GitHub API or live
workspace is required to read it. Binary changes retain a textual Git binary marker
and omit binary source bodies. Rename detection uses a 1,000-candidate exhaustive
search ceiling. Symlinks, submodules, invalid UTF-8 patches, unsafe paths, and
non-SHA-1 repositories report unsupported/invalid capture.

The [native Files / Review surface](native-review.md) verifies pinned manifests
and supports bounded diffs, version-bound decisions, and opaque viewed-file state.

Native execution records its initial HEAD, journals log deltas privately, freezes
64 KiB UTF-8 chunk boundaries before upload, and replays identical chunks after
lost acknowledgments or restart. Finalization flushes the final short chunk and
captures committed HEAD before workspace cleanup. Capture/upload failures prevent
cleanup from claiming durable completion. Logs below a chunk boundary remain local
until final flush. A lost runner never makes those unuploaded bytes durable.

The service reserves quota and records the immutable request/idempotency hash
before object I/O. It assigns opaque object IDs before conditional PUT, verifies
size/hash with GET, then transactionally records verified bytes. A lost PUT reply
recovers the existing version and verifies bytes instead of replacing the object.
Changed retries conflict. Unknown request fields are rejected.

Each verified log chunk produces a new immutable partial manifest. Finalization
produces a complete or interrupted revision; previous revisions remain immutable.
Manifests live in the durable catalog and include scope, kind, timestamps, policy,
object IDs, media types, lengths, digests, contiguous log sequence/offsets, and
capture options. The exact manifest bytes are hashed. A SQLite transaction commits
manifest, accounting and publication outbox together; the service retries bounded
outbox batches independently of runners. Hub deduplicates identical receipts and
rejects changed content under the same revision/ID.

| Protocol ceiling | Limit |
| --- | --- |
| Manifest | 1 MiB, 1,024 objects |
| Log object | 1 MiB (native runner sends at most 64 KiB) |
| Text/diff/image object | 16 MiB; images at most 16 million pixels |
| Video object | 64 MiB, MP4/WebM signature checks; no automatic recording |
| Artifact reservation | 256 MiB including all retained manifests and objects |
| Viewer | One verified object per selection; displays at most 256 KiB of text, complete object downloadable |

PNG, JPEG, WebP, MP4 and WebM fit the same manifest. Media signature/dimension
validation is not malware scanning. Multipart uploads, archives, compressed
bundles and range-hash verification are not supported. Deployment proxies must
bound concurrent requests; a single permitted video upload can require multiple
bounded buffers during JSON decoding and storage verification.

## Allowances, retention, deletion and recovery

`Allowances.Limits(ctx, organizationID)` is the payment-independent integration
contract for #2195. `Service.Usage` exposes retained bytes and outstanding reserved
bytes. Hosted mode requires an allowance implementation; the standalone command
uses the operator's configured limits. Run one authoritative service/catalog per
hosted organization: do not split an organization quota across independent catalogs.
A future distributed allowance authority must reserve globally before allowing
multiple catalog owners. Customer-managed mode uses customer policy directly.

Admission counts retained plus reserved bytes against the organization maximum
and separately caps outstanding reservations. Mutex serialization under exclusive
catalog ownership prevents concurrent overbooking. Verified bytes and immutable
manifest revisions move from reserved to retained atomically. Identical retries
and reused log chunks are not charged twice. Reservations cover uncertain PUTs
until verification or confirmed cleanup. New writes respect the current per-upload
limit. Existing reservations retain their accepted artifact/retention contract;
a downgrade rejects new reservations above the new limits, without silently deleting
existing data or rewriting existing expiry dates. Operators must select actual
prices, maxima and durations; protocol ceilings are not plan promises.

Policies require finite artifact retention, abandoned-upload deadlines, deletion
record retention and backup lifetime. Expiry begins at reservation creation and
is never extended by reads, retries or manifest revisions. Deletion record lifetime
must exceed backup lifetime plus the abandoned-upload deadline. Plan changes do
not imply consent to shorten retention.

At an abandoned-upload deadline, verified log chunks can be sealed interrupted;
unverified objects must be deleted before releasing their reservations. Incomplete
diff/media uploads are deleted, never presented as complete. At artifact expiry,
reads fail immediately and maintenance marks deletion pending, writes an immutable
content-free deletion marker into storage, removes exact object versions, verifies
absence, and purges manifests/objects/outbox. A content-free catalog deletion record
and storage marker remain for the configured finite deletion-record window.
Storage errors retain deletion-pending state and quota until cleanup succeeds.

Restored catalogs check the independent storage deletion marker before serving or
accepting further uploads. Restore must preserve the newest deletion markers and
current credentials/membership; never roll the deletion ledger back with an old
object backup. Never restore a backup older than the configured backup lifetime.
Catalog backup includes manifests/outbox; object/version backup is a separate
provider operation. Missing exact bytes remain missing; regenerated bytes require
a new artifact and cannot inherit reviews. Offboarding must preserve service
bindings/cleanup authority until physical object and backup cleanup is verified.

## Observable states and validation

| Observation | Meaning |
| --- | --- |
| No verified durable reference | Content may be local or queued; no machine-loss survival promise |
| Partial / uploading | Only published verified chunks are readable; finalization is outstanding |
| Interrupted | Only the known uploaded prefix survived; remaining bytes are not asserted |
| Complete / available | Immutable receipt accepted; current read still verifies authorization and integrity |
| Expired | Retention deadline reached; physical deletion may still be pending |
| Missing | An authorized expected object or manifest is absent |
| Denied | Current credential or grant is invalid/revoked; no cross-scope existence disclosure |
| Checksum mismatch | Manifest/object corruption; never an empty successful viewer |
| Storage unreachable / authorization unavailable | Distinct temporary service failures; no hosted fallback |
| Quota exceeded | Reservation or upload limit reached; preserve already uploaded evidence |

Run/Change details show bounded safe artifact references and contextual errors.
Run details filter by attempt; Change details filter by selected immutable version
or its associated run/attempt. The viewer performs no GitHub requests.

Validation uses local S3 protocol fixtures for provider configuration and required
semantics, catalog restart/backup tests, corruption and permission tests, lost
acknowledgments, concurrent reservations, abandoned uploads, deletion-ledger restore,
runner journal replay, and stopped-runner browser scenarios. Run `make generate`
and `make check`. Live provider credentials are not required by automated tests;
deployment-time capability probes are necessary before advertising a bucket as usable.
