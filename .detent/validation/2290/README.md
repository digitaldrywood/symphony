# Hosted native work browser validation

Baseline on b18e6213: Chrome followed the hosted Project work link for the
fixture issue and rendered `application/json` from the native v2 work-items
endpoint. No dependency on #2199 was needed; the existing hosted provider
fixture reproduces the same failure.

After the change, Chrome at 390×844 and DPR 1 followed the same project link
to an HTML issue page. The issue and Change viewport screenshots in this
directory show the existing hosted layout. Both pages have no horizontal
overflow. The fixture uses an ephemeral loopback listener and isolated
SQLite database, with no execution runner or live Detent process access.

Automated browser command:

```sh
npx playwright test tests/visual/hosted-work.spec.js tests/visual/artifacts.spec.js --project=chromium
```

Result: 10 passed. Coverage includes sign-in, native issue creation,
discussion, stored run history, 27-comment pagination, Change creation and
opening from the project list, Change discussion, viewer/staff/tenant/session
denials, CSRF, and live project-grant revocation. Narrow issue and Change
pages are checked for overflow. The hosted artifact browser adapter test
uses intercepted reference/grant/service responses to inspect the JSON CSRF
request and token-only, checksum-verified download. The existing independent
artifact browser fixture also passes with its real authorization/revocation
flow and offline runners.

Focused Go command:

```sh
go test ./internal/hubserver ./internal/web/templates -run 'TestHosted' -count=1
```

Result: passed. Table-driven hosted route tests cover role/grant permissions,
session/membership/grant revocation, staff, tenant mismatch, missing/foreign
resources, no-store HTML, and redacted storage errors. Existing native hosted
API tests cover artifact grant authorization and CSRF.

Skill draft: no — this change reused established hosted authorization and browser fixture patterns.
