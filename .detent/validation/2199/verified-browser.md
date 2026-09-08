# Verified hosted pilot browser evidence

Captured 2026-09-08 with Chrome DevTools at 390×844, DPR 1, using isolated
ephemeral Go previews and synthetic provider/storage fixtures. The captures
span independent preview runs. Every preview was stopped with POST to its own
`/__preview/stop` endpoint and exited successfully; live Detent was untouched.

Reproduce both preview entry points from `docs/cloud-pilot-evidence.md`.

| Evidence | Result |
| --- | --- |
| `verified-create-organization-390.png` | Provider sign-in exposes first-organization creation and invitation join. |
| `verified-new-organization-issue-390.png` | Browser forms create an organization, project and native issue; the issue opens as HTML. |
| `verified-browser-invitation.json`, `verified-invited-member-390.png` | The invitation is accepted by `user_browser_invitee`; the member sees the organization without owner controls and receives 403 for an ungranted project. |
| `verified-browser-project-grant.json`, `verified-invited-project-grant-390.png` | After an explicit owner grant, the same member can read the project and issue (200), with no owner controls. |
| `verified-human-awaiting-approval-390.png` | Independent CI succeeds while required human approval remains pending and readiness stays `needs_evidence`. |
| `verified-browser-review.json`, `verified-human-approved-390.png` | Scoped owner approval returns 200, changing the Change to `reviewed` with successful checks and an external GitHub gate. |
| `verified-browser-automatic.json`, `verified-automatic-reviewed-390.png` | The second repository reaches `reviewed` with successful checks and native review `not_required`; the external gate remains. |
| `verified-browser-offline-artifact.json`, `verified-offline-artifact-390.png` | The actual HTTP upload remains readable after runner heartbeat expiry; the browser verifies complete content and leaves script-looking log text inert. |

The two repository versions were produced by work on one named/tagged runner.
Approval uses the scoped native API; the hosted pages display its results.
External protected-merge denial is separately exercised by
`TestChangeApprovalPreservesProtectedMerge`, which verifies zero execution
effects after failed external verification. No live GitHub merge is claimed.

Earlier issue discussion, Change creation/discussion, stored history, tenant
denial and already-issued artifact-grant revocation remain in `resumed-browser.md`.
Current checks found no horizontal overflow.

The invitation fixture originally selected the owner at each provider redirect.
It now retains an explicit fixture account independently of application-session
rotation or revocation. `TestHostedBrowserProviderAccountSelection` verifies the
selected owner/viewer after an invalid app session. The final invitation and
project-grant captures verify the member's restricted permissions. An earlier
capture that accidentally returned to the owner was excluded from evidence.

The fixture mailbox substitutes for provider email delivery. Source/model work,
real object-store compatibility, hosted costs, operational restore objectives
and deployment authorization remain outside these browser observations.
