# Hosted identity and organization authorization

The operator-hosted product is one shared authenticated site at
`https://hub.detent.build`. Organization creation, invitations, selection, Work,
runner setup and organization billing stay on that public origin. Separate tenant
processes/databases remain the storage baseline; a separate public origin per
organization is superseded by [#2340](https://github.com/digitaldrywood/detent/issues/2340).

The shared-site sections below are the design contract for #2341/#2342/#2343,
pending implementation and review. The existing reserved-tenant configuration is
shown explicitly as current behavior. This document enables no live account,
invitation, DNS, deployment or charge.

Self-hosting is free with operator-selected auth: WorkOS, custom, generic or local.
Selecting an auth adapter must never select a billable deployment mode or require
a Detent Cloud account. The current `--hosted-config` couples WorkOS and hosted
policy; it is not yet a general self-hosted WorkOS switch. Generic scoped Hub
bearer auth and local dashboard auth remain supported without it. The proposed
configuration separation is in [deployment examples](examples/hub/README.md).

## Allocation and configuration

Allocate one dedicated Hub process and local filesystem database per organization,
behind the shared entry service's authenticated private routing. Retain exclusive
ownership and backup procedures. Do not share SQLite over network filesystems,
combine collaboration databases, or expose tenant listeners publicly.

### Supported reserved-tenant configuration

Today `detent hub serve --hosted-config /path/to/hosted.yaml` selects the WorkOS
adapter and tenant Hub UI, with native collaboration independent of GitHub.
It requires a fresh reserved database and stable organization ID, plus a verified
bootstrap subject when creating the WorkOS organization. That subject cannot
reclaim ownership after members exist. Binding rejects a populated/local database,
a different organization/origin, or reopening a hosted database in local mode.
The following YAML is supported today for an isolated reserved-tenant deployment;
it does **not** implement shared signup by replacing its URL with hub.detent.build.

```yaml
organization_id: org_example_opaque_id
bootstrap_subject: user_example
public_url: https://organization.example.test
workos:
  client_id: client_example
  api_key_env: WORKOS_API_KEY
staff_emails:
  - support@example.test
support_actors:
  - support@example.test
directory:
  - organization_id: org_example_opaque_id
    workos_organization_id: org_workos_example
    public_url: https://organization.example.test
```

Set `workos_organization_id` at the root when binding an existing organization.
The WorkOS API key is resolved from the named server environment variable; do
not put its value in YAML. Optional `workos.api_url` and `workos.issuer_url`
support configured provider endpoints; HTTP is accepted only on loopback for
fixtures. The issuer defaults to `https://api.workos.com`; set `workos.issuer_url`
to the exact issuer configured for the environment when using a custom domain
or application-specific issuer. Hosted resource enforcement uses the versioned
[pilot allowance configuration](hosted-allowances.md). Legacy `plan_id`,
`storage_quota_bytes` and `event_quota` initialize the pilot plan when the new
`entitlements` section is absent. These values do not establish public prices.

`DETENT_HUB_ADMIN_TOKEN` remains required for bootstrap compatibility, but in
hosted mode it authorizes only the bounded metadata endpoint. It cannot read
customer projects, create general tokens or use instance-admin routes. Treat it
as a reporter secret. Existing runner credentials retain their separate scoped
identity, renewal, revocation, project and operation permissions.

The current `directory` maps IDs to trusted public origins and switches via a
fresh protected login on the owning Hub. That behavior is retained only as a
legacy deployment/migration input; the shared product uses registry allocations
and scoped navigation, not cross-origin redirects. See [the RFC routing trust
boundary](cloud-hub-rfc.md#shared-site-control-and-tenant-storage).

## Shared-origin route and session contract

These are target routes for #2341, not aliases already accepted by the binary.
`ORG` and `PROJECT` stand for immutable opaque IDs, never organization names.

| Surface | Public route/behavior |
| --- | --- |
| Sign-in and callback | `/auth/oidc/start`, `/auth/oidc/callback`; one shared login transaction authority |
| Entry and chooser | `/` sends an authenticated user to `/organizations`; `/organizations/new` offers creation; `POST /organizations` starts an idempotent intent |
| Provisioning status | `/organizations/ORG/provisioning`; visible only to the verified creator/current authorized member |
| Organization shell | `/organizations/ORG/work`, `/organizations/ORG/projects`, `/organizations/ORG/settings` |
| Project work and run selection | `/organizations/ORG/projects/PROJECT/...`; issue, Change, run and attempt IDs remain in that tab's URL |
| Deletion | Owner-confirmed `POST /organizations/ORG/delete`; account-confirmed `POST /account/delete` and private `/account/deletion` status; resumable lifecycle, never a GET side effect |
| Organization billing | `/organizations/ORG/billing`, with CSRF-protected `/checkout` and `/portal` POST actions |
| Native content/runner API | `/api/v2/organizations/ORG/...`; preserve current project, machine, runner, lease and operation scope |
| Hosted usage/subscription APIs | `/api/cloud/organizations/ORG/billing` and `/billing/subscription` |
| Project SSE | `/organizations/ORG/projects/PROJECT/events` |
| Invitations | `/invite?invitation_token=...`; resolve provider-verified organization through the registry |
| Shared billing ingress | `/webhooks/stripe/test` and separately `/webhooks/stripe/live`; signature/account/environment validation before customer routing |
| Capabilities and static assets | `/api/v2/capabilities` exposes only shared protocol metadata; static shell assets contain no tenant state |

A shared opaque session cookie identifies a signed-in account, not a mutable
current organization. Use a `__Host-` cookie with `Secure`, `HttpOnly`,
`SameSite=Lax`, `Path=/` and no Domain attribute on HTTPS. No cookie is shared
with other detent.build hosts. Loopback HTTP fixtures use an explicitly separate
non-production cookie convention. Rotate authority on authentication changes;
logout invalidates the shared session and all its tenant authorizations.

Each request derives its organization from the canonical route and revalidates
membership plus project grants. Body/header IDs must agree with the route. Reject
ambiguous encoded separators, dot segments, duplicate scope parameters and
conflicting authorities before proxying. An unscoped legacy customer route must
lead to the chooser or fail, never infer a tenant from last selection. No cookie,
localStorage setting or server-global selection may control another tab's action.
HTMX links, forms, history URLs, redirects, polling, dialog actions and browser
retry keys all preserve explicit organization/project/run/attempt context.

Retain provider-validated organization sessions server-side per shared session
and organization. Selecting B may obtain a new B authorization through the common
callback, but cannot replace A's authorization. State/PKCE transactions are one-use,
short-lived and bound to the initiating browser, requested organization and a
validated return route; concurrent tab logins cannot overwrite each other's
transaction. Provider identity, active session and current membership are verified
on every protected request, and tenant mutations recheck local grants/revocation
inside the transaction before replay. Provider failure fails closed. Removed
membership invalidates only that organization's authority; account logout/expiry
invalidates every organization. Support sessions remain separately scoped and
cannot change organization or replace another tab's ordinary session. A tab
selects an opaque support context bound server-side to that shared session, actor
and organization; an unverified header or query value cannot enable impersonation.

All cookie-authenticated mutations require a session-bound CSRF token, exact
configured Origin verification and intended organization binding (including HTMX
requests). Account-level creation/deletion instead binds the account and operation
intent before an organization exists. Login/callback uses state and PKCE instead; signed provider webhooks
and explicit bearer-only machine routes do not accept cookie authority. GETs do
not mutate membership or billing; the invitation GET stores an intent and begins
authentication, with acceptance only after the verified transaction. Reject cross-
origin form submissions even when browser SameSite rules would send the cookie.

Return destinations are server-generated or canonical relative application paths
validated against allowed route templates and the initiating organization. Reject
client-supplied absolute/scheme-relative URLs, backslashes, encoded traversal, userinfo and any
unapproved query fields. Reauthorize on return; invalid/removed access returns to
the chooser with no tenant content. Never put provider, session, invitation or
artifact capabilities into redirect destinations, logs, referrers or analytics.
Use `Referrer-Policy: no-referrer` on auth/invitation and sensitive pages.

SSE authenticates both connection and each emitted frame, closes on revocation
and session expiry, and sends bounded heartbeats that recheck authority even
without content events. Target revocation/idle-close bound is 30 seconds; no
positive authorization cache may extend it. Reconnect/cursors bind organization,
project, session and allocation generation; switching a tab closes that tab's
old stream. Search, caches and idempotency namespaces carry the same scope.
Tenant responses are `no-store`; shared caches store only public static assets.
Escape customer text and sanitize rendered Markdown/links, enforce a restrictive
CSP, and never serve arbitrary active artifact HTML on the authenticated origin.
Separate databases do not protect two organizations from same-origin XSS.

Runner enrollment, renewal, events and claims go to the common hostname using
explicit organization routes and independently verified enrolled credentials.
For existing machine/service APIs without a scoped path, the entry service may
resolve only a durable credential-to-organization binding, never a browser cookie
or unverified token field; ambiguous/missing bindings fail. The tenant repeats
credential, organization, project, machine, operation and fencing checks. Artifact
publisher and independent-check tokens retain their narrow exceptions. Hostname
routing never widens token authority or exposes generic instance-admin APIs.

Artifact grant issuance uses the scoped native API, binds organization/project,
artifact/service, originating session, actual/effective identity and operation,
and expires within one minute or the earlier session/retention deadline. The
artifact authorizer rechecks current session, membership, grant and service
binding at redemption through the shared trusted route. Grants from A cannot be
redeemed for B; path/header substitutions and revoked authority fail. Downloads
continue through the selected customer-managed or opt-in hosted artifact service;
local-only mode never silently uploads. Storage credentials remain at that service.

## WorkOS application setup

For the operator-hosted production application, the exact registered values are:

| WorkOS setting | Value |
| --- | --- |
| Sign-in initiation in Detent | `https://hub.detent.build/auth/oidc/start` |
| Redirect/callback URL | `https://hub.detent.build/auth/oidc/callback` |
| User invitation URL | `https://hub.detent.build/invite` |

These are the common URLs for every organization, with no wildcard callback,
tenant-specific subdomain or public tenant port. The callback matches the
`public_url` used by the shared auth adapter, never a private tenant endpoint.
Configure separate provider environments/applications for fixtures/staging with
explicit callback and invitation URLs on their own origins; do not mix client IDs,
issuer, keys or sessions across environments. Enable intended email/password,
Magic Auth or SSO methods and flat role slugs `owner`, `admin`, `member`, `viewer`.
No GitHub login is required. Account setup still needs separate operator action.

WorkOS appends `invitation_token` to the configured User invitation URL. The
shared entry looks up the invitation with WorkOS and maps its verified provider
organization to the registry; client organization/return parameters cannot select
the destination. The issuing owner/admin and grant authority are checked before
provider dispatch, and a durable invitation intent tracks retries without duplicate
mail. Use the application's `/invite` entry rather than default AuthKit acceptance
so Detent's exact-recipient checks run. See [custom invitation redirects](https://workos.com/docs/authkit/custom-emails)
and [WorkOS invitation behavior](https://workos.com/docs/authkit/invitations).

Detent records the invitation in a one-use browser transaction, starts ordinary
state/PKCE login without forwarding the invitation token to AuthKit, and verifies
the exact recipient and organization before accepting membership. It then
establishes the selected organization authorization under the shared
account session, preserving other tabs. Exact-recipient matching uses the verified
provider email and existing normalization rules, never corporate-domain/alias
equivalence to grant access. The manual **Join with invitation** flow also lets a
signed-in user enter a token. Pending, expired, reused, wrong-account and
wrong-organization invitations fail closed. Provider success followed by a lost
local response is recoverable only for the same verified user and local invitation
intent; a locally consumed invitation cannot be used again.

Only opaque stable identity and ordinary account/organization fields go to
WorkOS. Model, repository and storage credentials do not enter login fields,
provider metadata or the audit trail.

## Permissions and revocation

| Authority | Allowed without project grants |
| --- | --- |
| Owner | Membership/organization administration, ownership role changes, organization billing usage |
| Admin | Membership and organization administration, excluding owner changes |
| Member | Organization selection and explicitly granted project collaboration |
| Viewer | Organization selection and explicitly granted project reads |
| Ordinary staff | Bounded account/usage/health metadata only |

Creating a project requires an explicit self-access checkbox. Other grants are
explicit per user/project, with separate collaboration and runner-management
flags. A viewer remains read-only even if a stored collaboration flag is true.
Organization administrators may explicitly assign grants; the role itself does
not supply project content, code credentials, runner execution or check authority.
Native runner-management operations currently require management grants for all
projects in the tenant Hub, because runner/host routing and listings can cover
multiple projects. This conservative rule prevents partial-management views from
revealing ungranted projects. Runner execution still requires a runner credential.

Each request validates the provider's active session and current membership,
then local membership and project grants. Provider unavailability fails closed.
Native mutations recheck authorization within the database transaction before
idempotent replay; cursors and replay keys also bind to the provider session.
Member removal revokes local sessions and removes project/runner grants before
requesting provider deactivation. Re-inviting a removed member does not restore
old grants. Last-owner removal/demotion is rejected. Ownership transfer is an
explicit promotion followed by demotion; nested teams and inherited permissions
are deferred.

Hosted pages reuse the shell without mounting process-wide dashboards, caches,
search, operator tools or streams. The delivered `/projects/:project/events` (scoped under `/organizations/ORG`
in the shared-site target) checks membership,
session and project permission for each counter frame and closes on revocation.
Content API reads, run events, attempt artifact references and changes use native
organization/project scope. Artifact read grants use the existing artifact
service contract and permit viewers with an explicit project read grant. Each
hosted grant binds to the originating session and expires within one minute or
the earlier provider-session/retention deadline. The artifact service rechecks
that session, current membership, project permission and support authorization
before each download authorization; logout, expiry or revocation invalidates
outstanding grants. Support download authorization preserves both identities
and the reason in the content-free audit trail.

A provisioned artifact service uses its separately issued native-only worker
token, explicitly bound to that service and project. In hosted mode this token
can only publish receipts and authorize reads for that binding; it cannot list
issues, read artifact references, obtain browser grants or administer services.
Routine staff and instance-admin bearer tokens receive no such exception.

## Support access

Routine staff membership is a metadata identity only, even if someone gives the
same account a customer membership. Exceptional support needs all of:

- WorkOS impersonation enabled by a WorkOS Admin in the intended environment.
- A WorkOS team role permitted to impersonate in that environment; restrict this
  to the designated support team. Production Admin/Developer/Support roles can
  impersonate; Support Viewer cannot. Review the current [team role matrix](https://workos.com/docs/dashboard/members-and-roles)
  before granting access.
- The same verified email in Detent's `staff_emails` and `support_actors` lists.
- A current ordinary staff login on the selected Hub, then CSRF-protected
  **Start support access** from `/support`.
- WorkOS dashboard impersonation returning in that browser within the one-use
  transaction's ten-minute window, with the exact selected organization and
  actual support actor in signed-token, exchange and active-session metadata.

Use one of the reason codes `customer-request`, `account-recovery` or
`troubleshooting` in WorkOS. Free-form customer details are rejected as reasons
so they cannot enter routine audit reporting. Impersonation never grants more
than the effective customer's current role and explicit project/runner grants.
It cannot switch organizations, bootstrap ownership or accept invitations.

The temporary indicator shows actual/effective identity and expiry. **Exit support
access** revokes the local session and requests provider revocation. The provider's
absolute expiry is preserved and capped at its documented one-hour impersonation
limit; support sessions are never refreshed into ordinary sessions. Audit records
contain opaque IDs, actual/effective identities, reason code, start/end or expiry
and route-template action metadata. They exclude query strings, request bodies,
raw errors, credentials and bearer capabilities. See [WorkOS impersonation](https://workos.com/docs/authkit/impersonation).

This document describes required configuration; implementation and fixture
validation do not enable impersonation or modify a live WorkOS account.

## Operational metadata and privacy

`GET /api/cloud/metadata` accepts the dedicated reporter token or an ordinary
verified staff session. Its closed schema contains organization/provider IDs,
active member/project/runner/event counts, latest activity, database/WAL size,
health and configured plan/quota fields. Only the owning Hub queries SQLite.
`GET /api/cloud/billing` is the delivered tenant-local usage route; the shared
public route is `/api/cloud/organizations/ORG/billing`, owner-only. Metadata
reporting stays private to authenticated staff/reporters and is never a public
route for inspecting an arbitrary organization.

Reports have no content cache. Hosted process logging drops arbitrary messages
and attributes, retaining a fixed message, timestamp and severity. Customer
responses, errors and reports use `Cache-Control: no-store`; operator endpoints
cannot fetch bodies, titles, repository paths, prompts, source, diffs, logs,
artifact references or artifact contents. Customer-authorized native API replay
records are still customer data and are reauthorized before replay.

This is application authorization and an audited support-access policy. A hosting
infrastructure operator remains technically capable of reading hosted data.
Customer-managed artifacts and optional hosted artifacts have distinct custody
boundaries; hosted artifacts do not imply operator-inaccessible encryption.
No zero-knowledge guarantee is made.

## Existing-origin migration

#2341 must deliver an explicit versioned offline migration, not manual SQL edits
or a startup bypass. The current immutable origin check remains until that tool
and its fixture tests ship; no migration command is available in this RFC.

1. Inventory each old organization/provider mapping, bootstrap state, database,
   origin, runner/service bindings, plan and Stripe account/environment/customer
   reference. Verify no duplicate or conflicting IDs. Create owner-produced
   backups and a registry import manifest; preserve all content/history/grants.
2. Drain/fence the old tenant, block old writes and revoke old browser sessions,
   login/invitation transactions and outstanding artifact grants. Keep a separate
   revocation/deletion ledger. Never copy cookies or trust old-origin headers.
3. With exclusive ownership, migrate the public-origin binding into a versioned
   shared-deployment binding carrying the same organization/provider IDs and a
   new allocation generation. Import allocation/membership/billing references
   into the registry idempotently. Validate tenant and registry agreement before
   readiness; preserve the prohibition on an ordinary local-mode reopen.
4. Rebind runner URLs and artifact authorizer endpoints through authenticated
   administration; preserve identity/history during an origin-only move. A restore
   still follows the existing epoch/credential rotation rules. Test renewal,
   callbacks, invitations, grants and reconciliation on the isolated destination.
5. Register the common callback/invitation URLs through separately authorized
   operator configuration, then enable shared routing only after verification.
   Old read-only bookmarks may redirect through a fixed allowlisted mapping to
   shared sign-in; old mutation/webhook/API endpoints fail closed until explicitly
   migrated. Never forward old cookies, invitation tokens or capabilities in a
   redirect. Reissue unconsumed invitations when their intent cannot safely migrate.
6. For rollback, fence the shared allocation first. Restore only a compatible
   pre-migration backup with the recovery procedure and reconciled external effects;
   do not downgrade a live database or activate both owners. Reapply revocations
   and tombstones and issue fresh sessions. Record the migration result per tenant.

Tests must interrupt each step, repeat it, reject mismatched bindings and prove
that migration preserves IDs, membership, history and billing references without
weakening the tenant identity lock. See [recovery baseline](hub-self-hosting.md#restoreimport-and-move-between-hubs).
