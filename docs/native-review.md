# Native Files / Review

Change detail renders a selected immutable customer-hosted diff bundle directly
in the browser using the [durable access contract](artifact-access-contract.md).
Hub and the dashboard serve metadata, decisions and authored discussion; neither
fetches or proxies source. No repository clone, runner, GitHub API, pricing or
payment service is needed to display a published bundle.

Select a version, enter a current project credential, and load its pinned bundle.
The browser verifies the grant, manifest hash and identity, organization/project/
work-item/run/attempt/version scope, capture base/head/merge base, expiry, object
sizes and hashes. A complete bundle's manifest digest must be pinned by the
immutable version's code, manifest or diff reference. A matching version ID alone
is insufficient. Current/older version and current repository gates remain visible.

File navigation supports additions/deletions, renames, line numbers, highlighting,
unified and split layouts. Native capture enables rename detection with a
1,000-candidate exhaustive-search ceiling. Binary changes retain Git's textual
binary marker and omit binary source bodies. Oversized files have explicit
rendering-limit states; the existing verified artifact download remains available.

## Review state and authorization

Discussion is change-level collaboration text with the selected version attached.
Pasting source into discussion intentionally uses its ordinary collaboration
custody. Decisions use the entered member credential, not the configured dashboard
operator identity. Dashboard form origin/token, write scope and project scope also
apply. Reviewers need a scoped Hub operator credential under the portable contract.

The browser sends the expected current version and bundle artifact ID, revision,
manifest SHA-256 and head SHA. Hub verifies the receipt against the immutable
version transactionally with the decision. New approval of an older version is
rejected. Idempotent retries remain bound to actor, operation, version and request
hash; changed replays conflict. An accepted old replay returns its original
historical decision and never approves a new head. Existing direct API clients
bind decisions through the URL version; the browser additionally requires a bundle.

Viewed files persist per reviewer, version, manifest and opaque file digest.
No filename or source is sent for viewed state. New versions start unviewed and
require renewed approval when policy requires review. Native decisions preserve
GitHub required reviews, branch protection, check requirements, and human/automatic
repository policy. Gate evidence remains separate.

## Bounds and limited context

| Work | Bound |
| --- | --- |
| Manifest | 1 MiB; 1,024 objects; 256 MiB total referenced bytes |
| Patch download/index | One verified object, 16 MiB; 100,000 lines; 2,048 file sections |
| File navigation | 100 buttons appended at a time |
| Selected file render | 256 KiB; 2,000 patch lines; 16 KiB per line |
| Highlighting | Explicit supported language; 500 lines / 64 KiB; 1,000 characters per highlighted line |
| Worker | Five-second deadline per request; terminate on timeout, identity/bundle change or page removal |
| Rendered markup | 4 MiB; 50,000 nodes; one file at a time |
| Viewed state | 4,096 opaque entries per reviewer/version |

Parsing/rendering runs in a worker with line matching and intraline comparison
disabled. Unknown languages and long lines remain plain text. Renderer and
highlighter output is rebuilt through an element/class allowlist without URLs,
style attributes, event handlers, SVG or active elements. File labels use
textContent. No source is persisted in browser storage, telemetry or server logs.

File selection rechecks membership. Credential/bundle changes and artifact expiry
clear the viewer; page removal terminates workers and cancels downloads. Revoked,
expired, missing, malformed, partial, corrupt or unreachable artifacts cannot
appear as an empty successful review. Already downloaded bytes cannot be recalled.
Runner independence still requires Hub authorization, artifact service and storage
to remain reachable. There is no hosted fallback.

The surface uses changed files and captured context, not a full repository.
Repository browsing, inline-thread relocation across rebases, suggested edits and
conflict editing remain follow-ups. Combined/word diffs, compressed bundles and
non-text source bodies are unsupported. The publisher must provide a Git patch;
capture failures retain the existing explicit artifact availability state.

## Dependency and security review

Reviewed September 7, 2026. Browser assets are vendored into local static assets;
the viewer has no CDN/runtime package dependency.

| Component | Version | License | Purpose |
| --- | --- | --- | --- |
| [diff2html](https://github.com/rtfpessoa/diff2html) | 3.4.56 | MIT | Maintained Git/unified-diff parser and renderer compatible with vanilla JS / Templ / HTMX |
| [highlight.js](https://github.com/highlightjs/highlight.js) | 11.12.0 | BSD-3-Clause | Common-language highlighting without autodetection |
| diff, bundled by diff2html | 8.0.3 | BSD-3-Clause | Intraline dependency; comparison disabled here |
| @profoundlogic/hogan, bundled by diff2html | 3.0.4 | Apache-2.0 | Fixed renderer templates; no caller-supplied templates |

License notices are retained under `static/vendor/diff2html` and
`static/vendor/highlight`. Registry metadata identifies diff2html 3.4.56 as the
current release, modified January 31, 2026. Bundled dependency versions match its
[release lockfile](https://github.com/rtfpessoa/diff2html/blob/3.4.56/package-lock.json).
An isolated `npm audit --omit=optional` reported zero known vulnerabilities. This
point-in-time advisory check supplements inspection of escaping, fixed templates,
resource limits, and malformed/XSS browser tests; it is not a security guarantee.

Vendored SHA-256 digests:

```text
a2110a09cee157bd5466da77be02107ac81a0baa2bc1f3fe81aac8183314598e  diff2html.min.js
8ab71eb09c51f501e5e25157d9cff100e46cc29bcbfc744d0b746d451fca7f53  highlight.min.js
```

Reproduce with `npm pack diff2html@3.4.56` and
`npm pack @highlightjs/cdn-assets@11.12.0` in the worker temporary directory;
inspect upstream licenses/advisories and compare these digests.
Patch semantics: [Git diff](https://git-scm.com/docs/git-diff).

Validation: stdlib table-driven Go tests, `tests/visual/change-review.spec.js`,
Chrome DevTools viewport screenshots, `make generate`, and `make check`.
