# Cloud pilot evidence and release checklist

This is a reproducible readiness assessment for #2199. **The pilot release gate
remains incomplete.** Hosted navigation (#2290/#2294), review-policy authorization
(#2291/#2293) and independent Change checks (#2299/#2302) are merged and verified.
Browser evidence covers organization creation, invitation acceptance, native
issue creation, offline uploaded logs and human/automatic review policies.
Hosted infrastructure costs and operational restore objectives remain unmeasured.
No deployment, customer invitations, billing, DNS or live identity changes were
performed or authorized. Native diff delivery remains the immediate independent
follow-up; billing is not a prerequisite for free access.

## Reproduce

Start from the issue branch or its merged successor. Prerequisites #2194,
#2195 and #2197 were closed, with closing PRs #2278 (`4d3871bb`), #2284
(`a2c1e9c5`) and #2269 (`1d94a9a1`) verified as ancestors of `origin/main`.
The continuation also verified #2290/#2294 (`4ea87c17`), #2291/#2293
(`e905e6f8`) and #2299/#2302 (`ea268db7`) merged into `origin/main` before
incorporating them.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/pilot_evidence_test.py
python3 scripts/pilot-evidence.py --output tmp/pilot-evidence.json
python3 scripts/pilot-evidence.py --race --output tmp/pilot-evidence-race.json
GOTOOLCHAIN=go1.26.6 make check
python3 scripts/hub-smoke.py tmp/detent
```

Choose a new output path for each run. The collector refuses to overwrite
evidence and fails if any required test is missing, skipped or failing. It
selects the toolchain declared in `go.mod`, removes ambient `DETENT_API_TOKEN`,
and records the source revision, working-tree state, pilot test hashes, platform,
toolchain, workload counters, known readiness gaps and required test names.
Compilation/dependency downloads are outside the measured workloads. Tests use synthetic customer
content and provider fixtures, temporary SQLite files, and ephemeral listeners.
They never bind to the live dogfood port 4000.

The recorded [verified evidence JSON](../.detent/validation/2199/verified-evidence.json) contains
58 passing required tests and their subtests. It is a local protocol sample;
elapsed time is descriptive, never a cross-machine performance assertion.
The local release gate is `make check`, including build, generated-file checks,
lint, vet, NilAway, race tests, aggregate coverage and configured package/file
coverage floors. Current-head CI is a separate PR gate. Earlier `evidence*` and
`resumed-*` files remain historical records of navigation, policy and independent
CI limitations before their fixes. Current captures use the `verified-` prefix.

## Evidence coverage and limits

All test names below are selected explicitly by the collector. The constituent
suites exercise production handlers and stores; they are not one continuous
production deployment or an actual model executing a customer repository.

| Requirement | Reproducible evidence | Remaining limit |
| --- | --- | --- |
| Sign in, create/join organization, create project, enroll runners, run native work | `TestHostedBrowserFirstOrganization`, `TestHostedLoginInvitationIntentAndReplay`, `TestHostedOnboardingFirstRun`, `TestPilotHostedHistoryAfterRunnerLossAndRestart` | Provider fixtures; local doctor/provider readiness is user-reported. Hosted issue/Change HTML navigation now verified at 390×844. |
| Read history with execution runners stopped | `TestPilotHostedHistoryAfterRunnerLossAndRestart` reads issue, comments, run history and draft Change after expired runner heartbeats and Hub reopen | Completed execution does not imply reviewed Change. |
| Durable uploaded logs/artifacts | `TestPilotHostedArtifactGatewayWithoutRunners`, `TestHTTPArtifactAccessWithoutRunners`, hosted artifact permission/revocation tests | The resumed hosted browser fixture uses a real HTTP gateway and Hub authorizer with synthetic in-memory storage. Real provider compatibility remains an operator prerequisite. |
| Human/automatic policies on one fleet | `TestPilotHostedTwoRepositoryReadiness`, `TestHostedIndependentChangeChecks`, `TestHostedRunnerChangeChecks`, `TestHostedChangePolicyJourney`, `TestProjectPolicyIsolationAndRestart`, `TestChangeApprovalPreservesProtectedMerge`, `TestChangeCIRejectsForgedAndReplayedResults` | Two projects execute on the same tagged runner. Successful independent checks leave human review pending until approval; automatic mode becomes reviewed without native approval. Both retain an external gate. External protected-merge rejection is component evidence; no live GitHub merge is claimed. |
| Explicit host, tags, no-match/offline waits | `TestRunnerRoutingClaims`, `TestRunnerSharedHostConcurrentClaimsAndRestart`, `TestRunnerRoutingRevocationAndDrain` | Routing authorization is tested independently of browser navigation. |
| GitHub issue independence after cutover | `TestGitHubImportCheckpointCutoverAndNativeIsolation`, `TestNativeSchedulerAndConnectorWithoutGitHub` | The latter's transport rejects non-native network requests; optional import, PR/CI and Git traffic remain separate. |
| Idle fleet request scaling | `TestPilotIdleRunnerReconciliation`, `TestPilotGitHubRequestBudgets`, `TestPilotGitHubImportBudget`, `TestPilotGitHubBackoffAndOperationBound` | Counts and scope are described below; these are not all customer GitHub requests. |
| Restart, partition/reassignment, stale writes | `TestNativeRecoveryAfterReassignmentAndRestart`, `TestNativeExecutionContextKeepsItsOriginalFence`, `TestNativeDelayedResponsesPreserveSuccessor`, `TestNativeCheckpointValidation` | Deterministic protocol failures, not WAN fault/load measurements. |
| Revoked identity/member and tenant isolation | `TestHostedSecurityRoleProjectMatrix`, `TestHostedSecurityRoleRevocationAppliesImmediately`, `TestHostedSecurityReplayAndCursorRevocation`, hosted artifact grant tests | Authorization checks do not replace an independent security review. |
| Storage outage, partial upload, restore | `TestHTTPUploadStorageOutage`, `TestRemoteHubAuthorizationFailsClosed`, `TestUploadRecoveryAndImmutableManifests`, `TestArtifactFailureStates`, `TestArtifactJournalReplay`, `TestRestoredCatalogCannotResurrectDeletedArtifact` | Incomplete or unavailable evidence cannot establish complete/review-ready output. Real provider compatibility and backup recovery must be rehearsed by the operator. |
| Free, grants, atomic quotas, downgrade/read/export | `TestHostedPlanResolutionBoundaries`, `TestHostedConcurrentClaimsDowngradeRelease`, `TestHostedMutationQuotaRollbackAndRetry`, `TestHostedProjectRetryAfterDowngrade`, artifact quota tests | Grants are payment-independent; no Stripe subscription or permanent free-tier promise is made. |
| Bounded telemetry and content custody | `TestHostedMetadataExcludesCustomerContent`, `TestHostedSecurityLogsExcludeCustomerContent`, `TestHostedPlanConfigurationAndTelemetry` | User-submitted issues/comments are hosted customer content and may include source or secrets. |
| Portable self-hosting with Cloud/WorkOS/Stripe unavailable | `TestNativeSchedulerAndConnectorWithoutGitHub`, `TestHostedPlansDisabledWithoutNetwork`, `TestRecoveryPreservesCollaborationAndFencesAuthority`, `TestSelfHostedRunnerVersionCompatibility`; `scripts/hub-smoke.py` | Native client egress is restricted to its local Hub; the smoke fixture omits hosted identity/billing configuration. This is not an OS firewall audit of every local command. |

## GitHub request budgets

The transport records profile, operation/family, requests, errors and last request
time. The native fleet test drives six explicit reconciliation cycles (three
incremental, three repair), with heartbeat and candidate-read activity from 0,
1 and 16 enrolled runners. Every fleet produces exactly six backend calls.
Disabling repository integration removes the target and produces no further
calls. This proves runner count does not multiply Hub reconciliation. Hub HTTP
heartbeats and candidate reads still scale with runner count.

The adapter test drives the real reconciler through the shared request transport.
An unexpected endpoint, including any issue endpoint, fails the scripted client.

| Synthetic repository workload | Repository REST | PR/review REST | CI REST | Issue REST |
| --- | ---: | ---: | ---: | ---: |
| One incremental cycle, no PRs | 1 | 1 | 0 | 0 |
| One incremental cycle, one open PR | 1 | 1 | 0 | 0 |
| Full repair, one open PR, one page per endpoint | 1 | 3 | 2 | 0 |
| Explicit two-page comment import | 0 | 0 | 0 | 2 |

With the current ten-minute incremental and daily full-repair defaults, a
single-page idle repository implies 288 incremental REST requests/day plus
repair. This is an extrapolation from the fixture, not a live daily measurement.
Hydration, pagination, new commits, checks, reviews, merge verification,
projections and explicit import add traffic. Shared-head check hydration can
deduplicate some calls; do not assume every repository has one page or one PR.
Git fetch/push and LFS are outside these HTTP fixture counters and must be
measured on customer execution hosts. Edit-history GraphQL is also separate
from this REST sample.

Primary limits depend on authentication. GitHub documents a typical authenticated
REST allowance of 5,000 requests/hour and separate GraphQL point accounting;
installation and enterprise allowances vary. Inspect response limit, remaining,
reset and resource headers, plus `Retry-After`; do not infer spare capacity from
one credential or multiply an account allowance by runner count.
[GitHub REST limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api),
[GraphQL limits](https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api).

The deterministic backoff fixture injects a one-hour primary reset, a 120-second
secondary retry and a secondary error with the 60-second minimum. Sixteen
attempted requests during each pause never reach the underlying client; at the
boundary, one retry succeeds (two transmitted requests, one error). Endless
pagination sends at most 500 requests in one operation and then returns an
error. That operation bound is not an hourly account cap or proactive secondary
limit admission. The mutation transport serializes sends and spaces mutations;
operators must still budget the shared credential, imports, repairs, agent tools
and CI watchers. Reconciliation counters are process-local and need external
sampling before restart if a continuous usage record is required.

## Workloads and cost worksheet

`TestPilotHostedWorkloads` samples one organization with two projects, using
ten accelerated rounds. Each round includes one plan-page read and, for each
runner, a heartbeat and candidate read. The active case adds 100 sequential
issue creations, comments, claims, started/finished events and lease releases.
No model executes and no real repository is fetched. All reservations are
released. SQLite snapshot sizes include schema/index overhead; page allocation
and response timestamps can change sizes slightly between runs.

| Workload | Hub HTTP requests | API mutation units | Ingestion units | Heartbeats | Approximate backup bytes after workload |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle baseline, no runners | 10 | 0 | 0 | 0 | 836,000 |
| Idle, one runner | 30 | 10 | 0 | 10 | 844,000 |
| Idle, sixteen runners | 330 | 160 | 0 | 160 | 872,000 |
| Active, one runner, 100 jobs | 630 | 510 | 400 | 10 | 2,789,000 |

The JSON contains exact snapshot sizes, HTTP response-body bytes and aggregate
handler microseconds. Elapsed handler time is not CPU consumption, provisioned
compute, network wire bytes, sustained throughput, p95 latency, or a hosting
invoice. TLS, headers, idle memory, process baseline, backups and support are
outside it. Issue creation and comments each create an event, so the active
job consumes four ingestion units, not just its two runner events.

`TestPilotArtifactTraffic` opts into the hosted adapter with an in-memory storage
provider. Reserving, appending, retrying the append, finalizing and reading a
1 KiB / 64 KiB object records eight storage operations and 3,072 / 196,608 relay
bytes. The retry checks a deletion marker without retransmitting the object.
Reservation bytes return to zero. This measures adapter accounting, not S3
latency or billing; the authenticated independent HTTP read and revocation path
is covered by the artifact integration suites.

Before setting public allowances, attach the following measurements from an
explicitly authorized isolated hosted deployment using the same workloads:

| Cost component | Required observation and calculation | Current evidence |
| --- | --- | --- |
| Shared compute baseline | Idle provisioned instance-hours, memory and CPU; gateway/identity/reporting minimum charges | Unmeasured |
| Marginal organization compute | Added organization idle load and active CPU-seconds at fixed fleet capacity; include per-tenant process/database overhead | Handler duration only; not a dollar cost |
| Database and retained events | Physical database/WAL growth, retention behavior, filesystem I/O and billed GB-month/operations | Local snapshot growth; native history has no automatic expiry |
| Backups | Snapshot size × retained copies × provider storage rate; backup writes, transfer and restore reads | Local snapshot sizes; provider cost and restore objectives unmeasured |
| Network | Ingress/egress wire bytes including TLS, API, SSE, identity, backups and region boundaries × applicable rates | HTTP response-body bytes only |
| Artifact relay | Customer versus explicitly opted-in hosted path, storage requests, repeated reads and egress | Synthetic adapter requests/bytes; provider rates unmeasured |
| Support | Operator interventions and support effort per admitted organization, tracked separately | Unmeasured |

Keep marginal organization cost, shared baseline allocation and support as
separate totals. Any financial total with a missing input remains unknown; the
JSON uses `null`, never zero. Do not extrapolate these short local samples into
unmeasured pennies, permanent free service, unlimited ingestion or a public price.
For an invitation-only experiment, propose an operator-selected allowance no
larger than the measured case (one connected runner, two projects and bounded
work), while retaining repository/provider safety limits. Treat even that as
an experiment, not a supported economic commitment. Measure a realistic elapsed
heartbeat schedule and idle read polling before choosing window limits: free
allocation mutations do not bound all read costs.

## Security, custody and operations

Read [hosted identity](hosted-identity.md), [allowances](hosted-allowances.md),
[self-hosted operations](hub-self-hosting.md), [artifact deployment](artifacts-deployment.md)
and the [architecture RFC](cloud-hub-rfc.md) together with this evidence.

- Cloud stores submitted issue/comment/discussion content, revisions, run
  metadata, policy descriptors, authorization and audit records. User content
  may itself contain secrets or source. This differs from automatically cloning
  source or collecting raw logs, credentials and transcripts.
- Repository files, model-provider credentials and execution remain on customer
  hosts. Default durable artifacts use customer storage and an independent
  authorized gateway. Hosted artifact custody requires explicit opt-in;
  entitlements never supply project access or storage credentials.
- Use dedicated organization storage/identity allocation, HTTPS, reviewed proxy
  trust, local SQLite ownership, protected persistent paths and scoped runner
  enrollment. Do not share a live SQLite file over a network filesystem.
- Configure finite telemetry, webhook-payload, artifact, service-log, export and
  backup lifetimes. Native issue/comment/Change history has **no automatic
  expiry or supported scoped purge** today. A requirement for per-record erasure
  needs software work; deleting one database does not erase exports or objects.
- Export only to an approved customer destination. Use the documented Hub
  backup/verify/restore commands and independent artifact catalog/object backup.
  Preserve external references, retain the external retirement/deletion ledger,
  and verify restored data before opening ingress. Restore fences old Hub
  authority; reenroll runners and reconfigure artifact authorization.
- Whole-instance retirement requires separately authorized fencing and erasure
  of databases/sidecars, artifacts, logs, exports and retained backups. Apply
  the external retirement ledger before any restore. Existing immutable records
  and independently held exports cannot be silently erased by revoking access.
- Local/self-hosted setup uses its own bootstrap and generic auth. WorkOS,
  Stripe and Cloud networking are not required. Optional GitHub/provider
  connectivity remains an explicit repository/customer choice.

## Browser evidence

Use the existing Go provider-preview fixture with Chrome DevTools, or automate
these URLs with the existing Playwright infrastructure. The preview is opt-in
and cannot be enabled in a production binary:

```sh
DETENT_PILOT_BROWSER_MANIFEST="$TMPDIR/pilot-browser.json" \
  GOTOOLCHAIN=go1.26.6 go test ./internal/hubserver \
  -run '^TestPilotBrowserPreview$' -count=1 -v
```

Read the manifest from the temporary directory, open `owner`, then `login`, and click the
mocked WorkOS sign-in. Inspect `organization`, `human_review`, `automatic`,
`usage`, `issue`, `change`, `human_change` and `automatic_change`. The manifest
also provides API URLs; it contains no session or runner credentials. Create a
synthetic native issue through the actual project form. Stop only this fixture with POST to its manifest's `stop` URL.
It times out if abandoned. It never starts or manipulates live Detent.

For first-organization creation and invitation acceptance, use a fresh output
path with the organization preview:

```sh
DETENT_PILOT_ORGANIZATION_MANIFEST="$TMPDIR/pilot-organization.json" \
  GOTOOLCHAIN=go1.26.6 go test ./internal/hubserver \
  -run '^TestPilotOrganizationBrowserPreview$' -count=1 -v
```

Open `owner`, then `login`, and follow the provider sign-in. Create the
organization, project and issue through their forms. Invite `invitee@example.test`;
the test-only `invitation_mailbox` URL substitutes for provider email delivery.
Open `invitee` and join with that synthetic invitation ID. Organization membership
must not grant project access. Switch to `owner`, sign in again, grant the member
project access, then switch to `invitee` and sign in again to verify it. Provider
account selection persists independently of application-session rotation; the
collector includes a regression proving that an invalid app session cannot turn
the selected viewer into the owner. The mailbox and account selector exist only
in the Go test fixture.

[Screenshots](../.detent/validation/2199/) use 390×844, DPR 1, viewport capture.
The resumed provider fixture uses a generated valid organization ID, real scoped
runner enrollment, native claims/events, and a customer-mode artifact gateway.
It uploads a synthetic log through the production HTTP upload path while the
lease is active, publishes its receipt to the Hub, releases the lease and
expires runner heartbeats before exposing browser URLs. Object storage is an
in-memory provider fixture; no source checkout, model call or live S3 bucket is
involved. The gateway and Hub remain independent of execution runners.

The resumed Chrome run verified provider sign-in, form-based native issue
creation, issue discussion, Change creation and discussion, stored run history,
and uploaded log access. Manifest/object hashes were verified; script-looking
log text remained inert. Revoking the viewer's project grant changed an already
issued gateway grant from HTTP 200 to 403. Cross-tenant HTML access returned 403
without issue content. Checked pages had no horizontal overflow. Exact sanitized
browser results and screenshots are in the [historical browser record](../.detent/validation/2199/resumed-browser.md).

The [verified browser record](../.detent/validation/2199/verified-browser.md)
adds first-organization creation, invited-member acceptance and explicit project
access, plus refreshed offline log and policy evidence. One shared tagged runner
executes both repository jobs. Approved independent CI submissions return 200.
The human repository remains `needs_evidence` with pending approval despite
successful checks; owner approval returns 200 and changes it to `reviewed`.
Automatic mode reaches `reviewed` with review `not_required`. Both show the
external GitHub gate, and the protected-merge regression verifies denial with
zero merge execution effects. These results do not establish a live deployment,
provider storage compatibility or measured hosting costs.

## Release decision checklist

- [x] Prerequisite implementation merges verified against current tracker/main.
- [x] Reproducible synthetic integration/failure and budget evidence collected.
- [x] Measured quantities distinguished from unmeasured costs and promises.
- [x] #2290 merged; hosted issue/discussion/history/Change navigation verified.
- [x] #2291 merged; hosted review-policy approval and version publication verified.
- [x] Uploaded artifact browser read, integrity and live revocation verified with
  execution runners offline against the isolated provider fixture.
- [x] #2299 merged; hosted independent checks and two-repository native review
  readiness verified, with external protected-merge denial covered separately.
- [x] Organization creation, invited-user acceptance and explicit project access
  verified through the browser without changing the signed-in actor.
- [ ] Hosted idle/active infrastructure, retention, backup, network and support
  measurements recorded; operator approves pilot allowances.
- [ ] Operator records gateway/provider configuration, TLS/proxy trust,
  identity/grant policy, retention/export/deletion rules and a restore rehearsal
  with recovery objectives and responsible owners.
- [ ] Final `make check` and exact-head PR CI pass.
- [ ] Human explicitly authorizes any later deployment or customer invitation.

A green component suite is evidence for the tested contracts. It does not clear
these remaining release gates or authorize production operations.
