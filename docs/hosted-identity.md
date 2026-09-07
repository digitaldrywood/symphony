# Hosted identity and organization authorization

Hosted identity is optional. `detent hub serve --hosted-config /path/to/hosted.yaml`
selects the WorkOS adapter and tenant Hub UI. Local dashboards, generic OIDC,
magic links and customer-hosted Hub bearer authentication remain independent of
WorkOS. Hosted mode starts native collaboration without GitHub credentials.

## Allocation and configuration

Allocate one process, filesystem database and HTTPS origin per organization.
Retain the existing exclusive database owner and backup procedures. Do not point
two processes at one file, share SQLite over a network filesystem, or combine
customer databases. Hosted binding rejects an existing populated/local database,
a different organization/origin, or reopening a hosted database in local mode.
Origin moves require an explicit controlled migration while the Hub is offline.

Reserve a fresh Hub for a verified WorkOS user ID before enabling signup there.
Generate its stable Detent organization ID independently of the provider. The
bootstrap user creates the WorkOS organization and owner membership; the Hub
persists the provider mapping. The bootstrap subject cannot reclaim ownership
after members exist. For an already-created WorkOS organization, supply its ID
and an existing owner's bootstrap subject for initial local membership. Subsequent
users require invitations issued by this Hub. Changing provider account identity
does not silently change the Detent organization identity.

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
or application-specific issuer. Optional `plan_id`, `storage_quota_bytes` and `event_quota` are reporting
metadata. They do not establish prices or implement quota enforcement (#2195).

`DETENT_HUB_ADMIN_TOKEN` remains required for bootstrap compatibility, but in
hosted mode it authorizes only the bounded metadata endpoint. It cannot read
customer projects, create general tokens or use instance-admin routes. Treat it
as a reporter secret. Existing runner credentials retain their separate scoped
identity, renewal, revocation, project and operation permissions.

The directory is trusted deployment configuration containing only IDs and Hub
origins. Switching checks active provider membership, redirects to the selected
Hub's protected login and creates an organization-bound cookie there. Cookies
are host-only, HttpOnly and SameSite=Lax, with Secure on HTTPS. No session or
capability is transferred in a redirect. Automatic provisioning of another Hub
belongs to deployment orchestration (#2199); this delivery supports one reserved
organization per allocated Hub and switching among configured allocations.

## WorkOS application setup

Configure AuthKit and exact callback URLs ending in `/auth/oidc/callback` for
each permitted Hub origin. Enable the intended email/password, Magic Auth or SSO
methods; a GitHub login is unnecessary. Define flat role slugs `owner`, `admin`,
`member`, `viewer`. Detent accepts only active memberships with these roles.

Configure the application's **User invitation URL** to the owning Hub's `/invite`
entry, or an authenticated deployment router that sends the invitation to that
owning Hub. WorkOS appends `invitation_token`. A deployment serving several Hubs
must route that provider-validated organization to its configured destination;
do not accept an arbitrary redirect URL. Separate WorkOS applications/origins are
another supported deployment arrangement. Do not leave the invitation URL on the
default AuthKit entry: that path can accept corporate-domain aliases before the
application's exact-recipient check. See [custom invitation redirects](https://workos.com/docs/authkit/custom-emails)
and [WorkOS invitation behavior](https://workos.com/docs/authkit/invitations).

Detent records the invitation in a one-use browser transaction, starts ordinary
state/PKCE login without forwarding the invitation token to AuthKit, and verifies
the exact recipient and organization before accepting membership. It then starts
a fresh organization login. The manual **Join with invitation** flow also lets a
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
search, operator tools or streams. `/projects/:project/events` checks membership,
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
`GET /api/cloud/billing` exposes this organization's usage only to its owner.

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
