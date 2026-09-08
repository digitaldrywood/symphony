# Resumed hosted pilot browser evidence

Historical capture before #2299/#2302. The later positive check/review and
organization/invitation evidence is recorded in `verified-browser.md`.

Captured 2026-09-08 with Chrome DevTools, 390×844 viewport, DPR 1. The
isolated `TestPilotBrowserPreview` used ephemeral Hub/gateway ports, a
provider fixture, temporary SQLite databases and synthetic object storage.
All execution leases were released and runner heartbeats expired before
browser verification. The fixture stopped through its own endpoint and
exited successfully. The live Detent process and port 4000 were untouched.

Reproduce with the manifest command in `docs/cloud-pilot-evidence.md`.
Follow `login` through the WorkOS provider fixture, then use the project
form to create an issue, add discussion, open a Change and add discussion.
Use `issue` for the seeded completed run, select Artifacts, Read artifact
and Chunk 1. Use `human_change` and `automatic_change` for the two policy
states. Publishing and approval use the scoped native API; the hosted
pages display their results.

| Evidence | Result |
| --- | --- |
| `resumed-organization-390.png` | Provider login completed; organization and two projects accessible. |
| `resumed-discussion-390.png` | Form-created issue and persisted discussion render as HTML with runners offline. |
| `resumed-created-change-390.png` | Issue form opens a native Change as HTML. |
| `resumed-history-390.png` | Stored successful attempts remain visible with runners offline. |
| `resumed-offline-artifact-390.png` | Real HTTP upload receipt, scoped grant, independent manifest/object download and content hash verification; script text did not execute. |
| `resumed-browser-artifact-revocation.json` | Viewer grant/download 200; owner revokes project access; already issued gateway grant then returns 403. Download requests use bearer authorization and omit cookies. |
| `resumed-browser-review.json` | Hosted owner version approval returns 200; native review becomes approved while missing checks keep status needs_evidence. |
| `resumed-human-reviewed-checks-missing-390.png` | Browser displays the approved human review and missing CI distinctly. |
| `resumed-automatic-checks-missing-390.png` | Automatic repository has no native review requirement and still waits for CI. |
| `resumed-tenant-denial-390.png` | Wrong-organization access returns 403 without issue content. |

Issue, discussion, artifact, human/automatic Change and denial pages were
checked for horizontal overflow; none was observed. The upload contains
109 synthetic bytes, including inert script-looking text. No customer
source, credentials or production logs appear in this evidence.

The CI gap is reproducible with `TestPilotHostedTwoRepositoryReadinessGap`:
two repositories execute on one named/tagged runner, approve distinct
review policies and publish immutable versions, but approved independent
CI bearer submissions return 404. Follow-up #2299 must merge before the
complete check/review/merge acceptance journey can pass. These fixtures
and screenshots do not establish real S3 compatibility, hosted costs,
operational restore objectives or authorization for launch.
