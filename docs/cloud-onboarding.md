# Project onboarding

The shared product journey starts at `https://hub.detent.build`: sign in, create
or join an organization, then open a project explicitly shared with you. The
organization chooser, creation/provisioning state, scoped Work/project navigation
and billing reuse the existing Templ/HTMX shell. There is no forced tenant-domain
navigation or persistent global billing banner. Preserve compact/cozy and narrow
layouts, keyboard access, contextual errors and the tab's current run selection.

The organization lifecycle and acceptance trace below are the reviewed-design
target for #2341/#2342/#2343, not shipped self-service behavior. Today's hosted
implementation starts with a reserved organization; its project/runner setup
is reused. Organization administrators create projects; collaboration and runner
management remain separate explicit grants. Retrying project creation with the
same name and creator's write grant resumes that project. Return to its scoped
page to resume setup; infrastructure errors do not require another organization
or host identity.

## Organization provisioning and recovery

A verified signed-in identity submits `POST /organizations` with a session-bound
CSRF token and stable creation idempotency key. Persist a fingerprint of the
validated creation input and an opaque organization ID before external effects.
Same user/key/payload returns the same operation; a changed payload conflicts.
Separate tabs retrying the same intent cannot acquire duplicate owners or slots.
An organization display name is never identity or proof of ownership.

| Durable checkpoint | Required action and recovery invariant |
| --- | --- |
| `requested` | Bind creator to verified provider issuer/subject and creation intent, enforce eligibility and per-identity quotas. Unverified email, client-submitted owner IDs and support impersonation cannot bootstrap ownership |
| `allocating`: reservation | Atomically reserve bounded process/memory/disk capacity with an allocation generation and fenced worker lease before creating resources |
| `allocating`: provider | Create/recover the WorkOS organization and owner membership for that exact subject; persist provider IDs and step result. Query/reconcile an uncertain response before another create |
| `allocating`: tenant | Allocate a private path and service identity from trusted IDs, initialize a fresh tenant database through its sole owner, persist immutable organization/provider/generation binding and that creator's owner membership |
| `allocating`: free access | Assign the configured versioned free plan idempotently. No Stripe customer, card or paid subscription is required |
| `ready` | Publish the registry route only after database health, tenant/provider membership, allocation and plan references agree. Link to `/organizations/ORG/work` and existing project/runner setup |
| `failed` | Preserve intent, reservation/resource inventory, completed checkpoints, bounded error and retry eligibility. Offer Resume for recoverable failures, without generating a new identity. No capacity shows a retryable state; existing tenants stay healthy |
| `deleting` / `deleted` | Apply the RFC owner-confirmed retirement contract, fence routing/writers and track erasure/retention results. Tombstones prevent reuse and resurrection |

A provider create without a usable idempotency/reconciliation mechanism cannot
be blindly retried after an uncertain response. Record it as unresolved and
reconcile through the provider's supported lookup or operator repair of that
same intent. Likewise reconcile live process ownership before replacement.
Retry workers have finite attempt/deadline/backoff limits; exhaustion exposes a
sanitized resumable failure and retains evidence/resources safely. Cleanup uses
an explicit allocation inventory and ownership checks, never recursive deletion
of an inferred user path. Do not erase customer data after a failed signup.

The verified subject can recover a pending intent after login or process restart.
Provider email changes do not transfer ownership. An existing organization must
be joined through current membership or a provider-validated exact-recipient
invitation, never claimed by matching its name/domain or replaying a bootstrap
subject. Membership removal, re-invitation and last-owner protections remain in
[hosted identity](hosted-identity.md#permissions-and-revocation). Account and
organization deletion follow the [RFC lifecycle contract](cloud-hub-rfc.md#provisioning-recovery-and-capacity).

## Repository configuration and policy

Keep the customer checkout and its `detent.yaml` / `WORKFLOW.md` on the customer
host. Inspect existing files before using `detent onboarding draft-answers`,
`explain-answers`, or `build-workflow`. The builder's `--help` describes its local
output options. Review generated files before applying them. Cloud neither stores
workflow source nor writes repository files.

For the shared-site target, use `client.hub_url: https://hub.detent.build`
and the selected immutable organization ID. Configure `client.organization_id` and
`client.native_projects` mapping in the instance configuration. The project's
page shows its native ID. Run `detent doctor` against that configuration; resolve
reported problems locally. Sign in to the selected agent provider on that host.
Only checkbox acknowledgments of these local checks are saved in Cloud; they are
explicitly user-reported and do not override runtime validation.

Run `detent hub policy inspect --config /path/to/config.yaml --project LOCAL_PROJECT`.
Paste only the resulting descriptor into the policy approval form. The form
accepts validated, bounded policy metadata, including source revision and digests,
never arbitrary workflow fields. An organization owner/admin with project write
access approves it explicitly. Approval retains the existing compare-and-swap
and active-lease guards. Edit gate, review, auto-promotion, merge and runner
requirements in the repository, then inspect and approve the new descriptor.

## Customer host enrollment

On the selected host, run `detent hub runner init --hub-url HUB_URL`.
For shared hosting, `HUB_URL` is `https://hub.detent.build`; self-hosting uses the
customer endpoint. The selected organization is explicit in enrollment and client
configuration, not inferred from hostname. Keep its
private identity file outside repositories. Use the IDs printed on that host in
the enrollment form, which grants only the selected project. The existing Hub
runner administration contract requires runner-management grants for every
organization project; an administrator grants these separately in Organization.
The short-lived token is displayed only in the response, not persisted as setup
progress. Set `DETENT_RUNNER_ENROLLMENT_TOKEN` locally and run
`detent hub runner enroll --organization ORGANIZATION_ID --display-name NAME`.

Retry with the same identity file. Init and enrollment reuse its generated
identity; a display name never transfers host ownership. Do not copy an enrolled
identity file to another machine. Use the existing `--host-identity-file` protocol
for multiple distinct runners on one already enrolled host.

The page lists only runners authorized for this project. Names, host IDs, tags,
health and exclusion reasons come from the existing routing evaluator. Tag edits
preserve the runner's full access scope and use its expected revision. Other
projects' grants, leases and provider account records are omitted from readiness.

## Artifact history and GitHub

Preserve the September 7 choices: local-only history, customer-managed durable
storage, and explicit opt-in hosted storage through the portable S3 adapter
(DigitalOcean Spaces initially). No setup default silently uploads artifacts.
Local history is supported without a bucket or Cloud services. For history with
execution runners offline, follow [artifact deployment](artifacts-deployment.md):
private S3-compatible bucket, durable catalog, separate TLS gateway, dedicated
publisher identity, and local storage credentials. Register only the gateway
origin, service ID and publisher token ID. Never enter the publisher token secret
or storage credentials in the hosted form. Run `verify-storage`, then test opening
a retained artifact with execution runners stopped. A registered binding is
configuration evidence, not a storage-health or offline-availability guarantee.

Native issue creation works with GitHub disabled. If the deployment has GitHub
transport, attach a repository explicitly before enabling manual imports, summary
projection or repository/PR integration. Each choice is separate; saving setup
progress does not activate an integration. Existing profile, repository binding,
idle-work and revision checks still apply. Import execution and summary publication
remain separate actions through the existing native API.

## Shared protocol and recovery

Self-hosted Hub uses the same scoped API with customer-selected authentication,
without a Detent Cloud account, subscription or Stripe connection. These are
currently supported project setup endpoints:

- `GET /api/v2/organizations/ORG/projects/PROJECT/onboarding` reads persisted
  progress, current approved policy, eligible runners, bindings and latest run.
- `PUT` to the same path accepts `idempotency_key` and `progress`, containing
  string `revision`, `repository` (`existing` / `generate`), booleans `doctor`
  and `provider`, and `artifacts` (`local` / `customer`).
- Writes compare revisions and reuse the native idempotency journal. Restarting
  the service retains progress. Browser retries retain a command key and payload
  fingerprint in session storage; no issue body or provider credential is stored
  there. In the shared-site target, browser retry keys also bind organization,
  project and user so concurrent tabs cannot replay into another scope.
- The `/onboarding/policy`, `/onboarding/artifact-services/SERVICE`,
  `/onboarding/integration` and `/onboarding/repository` adapters require scoped
  administration and call the existing validated implementations.

A stale revision returns 409 and requires rereading current state. Invalid input
returns 422, access failures require account/grant correction, and service failures
require retrying the existing operation. The first native issue uses the ordinary
idempotent work-item API. Its state determines whether it queues; policy and runner
checks remain authoritative for execution.

The current `artifacts` progress field accepts only `local`/`customer`; the
approved opt-in hosted mode is a target storage choice, not a new value accepted
by this API today. Future setup must save explicit custody consent and use the
configured portable artifact service rather than overloading local/customer.

## Shared-site acceptance trace

This is a test specification for #2341/#2342/#2343 and the expanded #2199 evidence,
not a claim that current reserved-tenant fixtures pass it. Use synthetic providers,
ephemeral services and one test hostname representing `hub.detent.build`.

Alice and Bob are unrelated verified identities. Alice creates A, Bob creates B,
and Casey receives separate invitations to both. A has project PA and B has PB.
Each tenant owns a separate database and process. Every application URL below is
on **https://hub.detent.build**; WorkOS/Stripe hosted screens may temporarily leave
the site only for their authenticated provider flow and return to that same host.

| Step | Action and expected evidence |
| --- | --- |
| Signup | Alice and Bob create A/B from `/organizations/new` without operator YAML, bootstrap user, DNS or public port allocation. Crash/retry after every checkpoint returns the original A/B IDs with exactly one verified owner each |
| Capacity | Concurrent creates at the admitted limit leave a resumable capacity failure; already-ready A/B remain routable, no extra process or unsafe disk allocation appears |
| Invitations | A/B admins invite Casey through the common `/invite` entry. Exact recipient and provider organization must match; wrong-account, alias, expired/replayed and guessed-organization attempts fail. Casey sees both memberships, never an unrelated organization |
| Grants | Grant Casey read-only PA and write PB, separately from runner management. Ownership/admin status alone never reveals project content. Bob cannot read PA by guessing its ID; cross-tenant searches/cursors return no A content |
| Concurrent tabs | Casey opens `/organizations/A/projects/PA/...` in one tab and `/organizations/B/projects/PB/...` in another. Selecting a B run, submitting a B form, reauthenticating B or opening billing leaves A's URL, CSRF context, stream and run/attempt selection intact |
| Mutations and streams | A write remains denied and B write commits only to B. Swap route/body/header IDs, reuse a CSRF token in the other scope, spoof proxy authority, and bypass the entry service: all fail. Frames/cursors from A never appear in B |
| Billing | Free signup creates zero Stripe customers. Alice's first paid action creates/reuses A's test customer; Bob's creates/reuses B's. Casey cannot purchase as member. Swapped customer IDs, A return URLs in B and cross-account/mode webhooks cannot grant or expose another organization's plan/invoices |
| Runners | Enroll named/tagged RA for A/PA and RB for B/PB against the same base URL. Each renews/posts events/claims only its own scope; cookie changes and forged IDs cannot move credentials. Exact-host no-match waits without fallback |
| Artifacts | Test local-only without uploads, customer-managed durable service, and explicit opt-in hosted Spaces-compatible fixtures. An A grant fails for B, expired/revoked grants fail, and retained uploaded artifacts remain readable with execution runners stopped. Local-only makes no offline-availability promise |
| Review policy | PA requires human review; PB permits automatic merge after checks. Repo-local policy remains authoritative for both, with external branch protections and independent-check principals preserved |
| Revocation | Remove Casey from A while both streams are idle: A protected requests/download authorization fail immediately on recheck and its stream closes within 30 seconds. B stays usable. Shared logout/expiry closes both scopes |
| Restore/deletion | Restore paired registry/tenant fixtures with current tombstones and a new generation; stale authority fails. A owner deletion remains resumable until erasure completes, does not affect B, and is never reversed by old snapshots/webhooks. Casey account deletion removes both memberships without deleting Alice/Bob's work |
| Free self-hosting | With custom/local auth, block all Detent Cloud, WorkOS and Stripe networking. Create native work, enroll a runner, execute/checkpoint, export and recover with no subscription or hosted entitlement requirement. Repeat using a local WorkOS fixture with Cloud/Stripe blocked: auth selection still performs no billing call |

Record browser evidence for the existing shell in compact/cozy and narrow layouts,
no-access/expired-session states, provisioning interruption and the two-tab trace.
Record tenant database IDs and bounded routing/provider call counts without
content or credentials. #2199's free pilot checks free/complimentary billing UI;
paid customer/webhook cases are #2343 fixtures and do not block that free pilot.
The retained single-tenant evidence is a baseline, not shared-site launch evidence.

## Independent Change checks

Known setup limitation: the private non-hosted maintenance recipe below is not
executable for an already bound hosted database, which rejects local-mode reopen.
[#2350](https://github.com/digitaldrywood/detent/issues/2350) tracks a supported
credential-maintenance path and correction of that recipe. Keep the binding
checks intact. The existing scoped independent-check submission protocol remains
valid; this RFC does not add token provisioning/rotation/revocation authority.

Hosted admission permits a dedicated `operator` bearer to submit only
`POST /api/v2/organizations/ORG/projects/PROJECT/work-items/ITEM/changes/CHANGE/versions/VERSION/checks`.
The credential must be active, belong to the hosted organization through an
explicit current project grant, and be named by an `independent` required check
in that project's current approved Change review policy. An administrator bearer,
execution worker, hosted user cookie, or unapproved operator cannot substitute.
This exception grants no reads, publication, review decisions, policy changes,
imports, or token administration.

Enrolled runners retain their existing `collaborate` operation for expected
`customer` checks. They cannot satisfy an `independent` check or change the
source pinned by the immutable version.

Provision through the existing instance-administrator token APIs before enabling
hosted serving, or during an explicitly authorized private maintenance window.
For maintenance, the hosted process must be stopped and the sole database-owning
Hub process must expose its non-hosted administration API only on private
loopback. Never run a second process against the live database. Hosted serving
intentionally does not expose these generic administration APIs.

1. Create a dedicated token with `POST /api/v1/tokens`, using
   `{"name":"project-independent-ci","scope":"operator"}`. Deliver the returned
   secret once through the CI secret store; retain its public `id` for policy.
2. Grant exactly the intended project with `POST /api/v2/tokens/ID/grants`, using
   `{"organization_id":"ORG","project_id":"PROJECT"}`. This also marks the
   token native-only. Use a separate principal per project when independent
   revocation is needed. Restore hosted serving after maintenance.
3. An organization owner/admin with project write access approves
   `PUT /api/v2/organizations/ORG/projects/PROJECT/change-review-policy` using
   their hosted session and CSRF token. Include `idempotency_key`, the current
   `expected_review_policy_id` when replacing a policy, and `policy` containing
   the approved repository `policy_id`, `require_review`, and `required_checks`.
   Each check pins `name`, the credential ID as `principal_id`, `workflow_id`,
   `workflow_sha256`, `source: "independent"`, and `max_age_seconds` (60–604800).
   Repository review and check floors still apply.
4. Publish a version, then deliver its exact check expectation and immutable
   head, run, policy/configuration and workflow identities to the CI job through
   the publishing integration. The result bearer cannot fetch them itself.
   Submit a terminal result and evidence references with a unique idempotency
   key to the exact version's `/checks` route.

To revoke submission access immediately, the hosted project administrator
replaces the approved review policy with an eligible replacement CI principal,
using its current policy identity. The removed principal cannot submit or replay
results, even for old versions. Publish a new version after policy replacement;
old versions retain their immutable policy and display stale-policy readiness.
For permanent credential revocation use `DELETE /api/v1/tokens/ID` through the
private maintenance administration path. Rotation uses
`POST /api/v1/tokens/ID/rotate`, invalidates the old secret, and preserves the
principal ID and grants; distribute the replacement only to the authorized CI.
A removed project grant also immediately denies submissions.

Credential activity, hash, expiry, scope, project grant and current policy pin
are rechecked within the mutation transaction before returning cached results.
Results still validate the version's expected principal, head/run, policy/config,
check-run ID, workflow digest, independent source and completion timestamp.
Changed terminal results conflict; retries cannot approve another version.
Evidence loses readiness at its freshness bound. Human-review repositories also
need current-version human approval; automatic repositories need passing checks.
Native `reviewed` status preserves the separate GitHub branch-protection gate and
does not authorize an external merge.
