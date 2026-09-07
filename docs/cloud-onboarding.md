# Project onboarding

Sign in, create or join the reserved organization, and open a project shared with
you. Organization administrators create projects; collaboration and runner
management are separate explicit grants. Retrying project creation with the same
name and creator's write grant resumes that project. Return to its page to resume
setup. Infrastructure errors do not require another organization or host identity.

## Repository configuration and policy

Keep the customer checkout and its `detent.yaml` / `WORKFLOW.md` on the customer
host. Inspect existing files before using `detent onboarding draft-answers`,
`explain-answers`, or `build-workflow`. The builder's `--help` describes its local
output options. Review generated files before applying them. Cloud neither stores
workflow source nor writes repository files.

Configure the local client URL, `client.organization_id`, and
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

On the selected host, run `detent hub runner init --hub-url HUB_URL`. Keep its
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

Self-hosted Hub uses the same scoped API without WorkOS, Stripe or Cloud:

- `GET /api/v2/organizations/ORG/projects/PROJECT/onboarding` reads persisted
  progress, current approved policy, eligible runners, bindings and latest run.
- `PUT` to the same path accepts `idempotency_key` and `progress`, containing
  string `revision`, `repository` (`existing` / `generate`), booleans `doctor`
  and `provider`, and `artifacts` (`local` / `customer`).
- Writes compare revisions and reuse the native idempotency journal. Restarting
  the service retains progress. Browser retries retain a command key and payload
  fingerprint in session storage; no issue body or provider credential is stored
  there.
- The `/onboarding/policy`, `/onboarding/artifact-services/SERVICE`,
  `/onboarding/integration` and `/onboarding/repository` adapters require scoped
  administration and call the existing validated implementations.

A stale revision returns 409 and requires rereading current state. Invalid input
returns 422, access failures require account/grant correction, and service failures
require retrying the existing operation. The first native issue uses the ordinary
idempotent work-item API. Its state determines whether it queues; policy and runner
checks remain authoritative for execution.
