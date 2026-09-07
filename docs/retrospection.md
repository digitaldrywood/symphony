# Efficiency retrospection

[Back to README](../README.md#documentation)

Each project can opt into an evidence-based reflection loop over its runtime
telemetry. The pass runs after agent completions and on a daily schedule,
groups recurring inefficiency patterns, and creates or updates fingerprinted
board issues for human triage.

```yaml
retro:
  enabled: true
  schedule: "0 3 * * *"
  target_state: Backlog
  labels: [retro]
  product_repository: digitaldrywood/detent
  daily_issue_cap: 3
  lookback_days: 7
  min_occurrences: 2
  single_occurrence_severity: critical
  fallback_threshold: 3
  receipt_baseline_multiple: 4
```

Workflow findings stay on the project's board and include a governed proposed
`WORKFLOW.md` change when the evidence maps to a known setting. Product findings
such as completed-work re-dispatch or systemic capacity handling route to
`product_repository`. The configured `retro` label is always retained, and
`target_state: Todo` can be used when a project explicitly wants findings to
skip Backlog triage.

When `product_repository` differs from the project tracker repository, Detent
checks the destination repository visibility before creating or updating a
finding. Public destinations, and destinations whose visibility cannot be
resolved, receive stable opaque replacements for source repository references,
branches, workspace paths, and contributor logins. Private destinations retain
the original text. Set `allow_public_cross_project_details: true` only when the
operator has explicitly approved publishing the source details verbatim.
`detent doctor` warns about private-source/public-destination configurations,
and `detent exposure [--project <id>]` performs a read-only scan for possible
historical disclosures without editing existing issues.

Detent files a finding after `min_occurrences`, or after one occurrence at or
above `single_occurrence_severity`. `daily_issue_cap` limits newly created
issues; recurrence updates to existing fingerprinted issues remain allowed.
Retrospection never edits workflow files, prompts, or runtime configuration.
Its issue body records the evidence, proposed change, and pending human outcome.
`detent doctor` reports the last run, finding count, and filed/updated issue
counts for every enabled project. ProjectV2 trackers must set
`tracker.repository` so Detent knows where to create project-level findings.

`gate` controls the validation contract the agent and operator flow follow.
Omitting it preserves the code default: `kind: command` with `run: make check`,
plus green CI, no P1 automated PR review findings, a quiet window, and a
current-head automated PR review before auto-promotion. Set
`automated_review: optional` to wait through
`agent.auto_promote.gate_wait_timeout_seconds` and then continue to `Merging`
when the remaining checks pass. Set `automated_review: off` to skip that wait.
The legacy `require_automated_review` boolean remains accepted and maps `true`
to `required` and `false` to `off`. The quiet period resets on observed
issue updates, Project status updates, automated PR review submission, and
linked PR activity such as a fresh push to the PR head. Failed or cancelled
current-head CI moves a `Human Review` item back to
`Rework` by default. Set `ci_failure_action: skip` only when red CI should park
in `Human Review`; pending CI stays parked. Use
`kind: human_review` with `approval_label` only when the workflow explicitly
requires a human approval label to promote.
Set `required_status_checks` to the exact branch-protection or ruleset check
names that are release-blocking for the project. Detent treats a configured
required check as non-green when it is missing, skipped, failed, cancelled,
neutral, or still running on the current PR head.
For required workflows that run only on a `pull_request` `labeled` event, set
`ci_trigger_label` to that label, such as `ci:ready`. Detent then instructs
implementation and rework workers to run the host-coordinated
`detent ci-trigger-label` command after every head-changing push before they
wait for current-head checks, passing the configured GitHub tracker host for
Enterprise installations. Generated commands encode label and host arguments
without shell-specific quoting. Detent records successful worker pushes and
reapplies the configured label at completion when the worker did not leave it
as the final CI trigger after the latest push. This preserves label ordering
when a project uses additional CI lane labels. Merging workers also reapply it
immediately after deterministic head-changing pushes and whenever current-head
hydration reports required checks as missing, including after a rebase or
another merge advances the base. Before scheduling a trigger, Detent refreshes
the current head's check-runs and commit statuses. When every configured required
check has succeeded, it skips reapplication even after a restart or a reported
push. Explicit forced reapplication still repairs CI label ordering. Failed or
unavailable hydration defers the trigger; streamed no-op push output does not
count as a head-changing push. Scheduling logs include the reason and current
required-check states. Trigger events use a shared lock and persisted
timestamp per repository and are spaced by
`ci_trigger_label_stagger_seconds` (a positive value, default `15`) to avoid a
self-hosted CI stampede. `detent doctor` warns when it finds a label-gated
required check without this setting.

Set `gate.validator.enabled: true` to add a validator-agent review before
auto-promotion. The validator inspects the PR diff against the issue acceptance
criteria and returns a structured verdict, score, summary, and severity-tagged
findings. `gate.validator.model` optionally overrides the selected validator
route model, `min_score` below threshold routes to `Rework`, and any finding
severity listed in `block_on` routes to `Rework` regardless of score.
Validator production failures are retried with backoff up to `max_attempts`
(default `3`). Each failure is logged and visible to `detent doctor`; an
exhausted validator routes the item to `Rework` with the failure cause.
For Codex-backed validators, start with the cheap-tier override
`gate.validator.model: gpt-5.4-mini` and watch rework-rate per validator model
once cache/model telemetry lands. `gate.validator.max_inline_diff_bytes`
defaults to `65536`; validator prompts include the full diff only at or below
that size and otherwise seed stat-only context.

`plan` controls the optional plan-approval stop before implementation. It is
disabled by default, preserving the direct dispatch behavior. When enabled, the
first `Todo` dispatch runs in plan-only mode, posts a `## Detent Plan` issue
comment, and moves the issue to the configured `stop` such as `Plan Review`.
`review: human` waits for `approval_label` (`plan-approved` by default),
`review: automated` waits for a `## Detent Plan Review` issue comment or
current-head automated review state, and `review: both` accepts either path.
Blocking P1 plan findings route the issue to `Rework` with feedback.

For production, self-hosted, or multi-instance GitHub Projects, prefer GitHub
App installation authentication instead of a shared personal access token. App
installation tokens have a dedicated GraphQL budget per installation and scale
with larger installations, while a PAT shares one fixed user budget across
Detent, agents, and operator `gh` calls. Configure the tracker with
`github_app_id`, `github_app_installation_id`, and either
`github_app_private_key` or `github_app_private_key_path`; keep `api_key` for
small local setups or one-off evaluation.

Default workflows do not need worktree setup hooks. Detent creates and removes
Git worktrees natively, so a fresh Windows project can dispatch without bash.
Omit `codex.shell` and `hooks.shell` to use the per-OS defaults: `sh` on Unix
and `cmd` on Windows. For portable hooks, prefer no hook when Detent already
does the setup natively. When a hook is necessary, keep it to commands available
on every target or set `hooks.shell: pwsh` and write PowerShell that reads
Detent values from `$env:WORKSPACE`, `$env:WORKSPACE_KEY`, `$env:BRANCH`,
and `$env:ISSUE_IDENTIFIER`. The older `DETENT_*` hook variables remain
available as deprecated aliases for one release.

4. Create the global config and add the project:

```sh
detent init
detent add-project \
  --id <id> \
  --workflow /absolute/path/to/project-checkout/WORKFLOW.md \
  --workdir /absolute/path/to/project-checkout
```

For first-time onboarding, leave `--workflow-ref` unset until this
`WORKFLOW.md` has been merged to the ref Detent should read from.
`detent doctor` validates the configured ref; setting
`--workflow-ref origin/main` before `origin/main:WORKFLOW.md` exists will fail
even when the file exists locally in the working tree. After the first workflow
merge, add `workflow_ref: origin/main` to the project entry when you want Detent
to load the workflow from the branch tip instead of the working-tree file.

Edit the resolved `global.yaml` and set the top-level runtime keys:

```yaml
env: prod
log_level: info
github_token: gh
port: 4000
update:
  auto_check_enabled: true
  check_interval_hours: 24
  auto_apply_enabled: false
```

5. Verify the setup before dispatching:

   ```sh
   detent doctor --allow-write-probes
   ```

`detent doctor` is a preflight check: config resolution, the SQLite database,
the `codex` binary, GitHub auth mode, GitHub tracker readiness, git, and
whether the server port is free. It also reports each project's active
definition root, layout, revision, authority files, local overlays, and whether
the running process is stale. In ProjectV2 mode it checks project access,
Status options, board item reads, repository issue/PR access, and rate-limit
visibility. In issue-field mode it checks repository access, issue field
discovery, Status option discovery, issue reads by field value, and REST/GraphQL
rate-limit visibility. In label mode it checks repository access, status label
mappings, issue reads by configured status labels, and REST/GraphQL rate-limit
visibility. By default doctor is read-only: if a configured workflow would run
write probes, the report warns that they were skipped. Pass
`--allow-write-probes` only after the onboarding mutation gate has passed and
the operator has explicitly confirmed mutation. With that flag, ProjectV2 and
issue-field modes require `tracker.write_probe_issue` when integration needs
status-write proof, because their status mutations target a concrete project
item or issue field value. Label mode does not need a persistent status-labeled
scratch issue for the default permission proof: doctor sends intentionally
invalid repository-label and issue-create requests, expecting GitHub to reject
them with validation while proving the token has the repository Issues write
permission class. Configure `tracker.write_probe_issue` in label mode only for
legacy/deep issue-object proof, such as reapplying an existing status label on a
scratch issue. That proof is stronger for the chosen issue object, but the issue
must be kept off the board by removing Detent status labels or closing it after
migration. Before starting Detent, fix any `FAIL` (missing `github_token: gh` or
an unauthenticated `codex` are the usual culprits). If Detent is already running
on the configured port, the server-port check can fail because
the live service owns the port; use `detent doctor --port 0` for the same
read-only config, toolchain, token, and database preflight without the port
collision, or `detent doctor --port 0 --allow-write-probes` after mutation
authorization to prove writes, then verify the live service with `/health`.

For Detent dogfood/self-tests that need a running server, start an isolated mock
runtime instead of stopping or reusing the live process on `127.0.0.1:4000`:

```sh
detent dev-runtime --port 0
```

The command prints `Mode: isolated dev runtime`, the selected dashboard URL,
temp home, DB mode, tracker mode, and fixture path. By default it uses a temp
config/workspace home, an in-memory SQLite database, a stateful fixture-backed
memory tracker, and a fake runner; it does not call GitHub or mutate a real
ProjectV2 board. It refuses the live dogfood port and live
`~/.detent/detent.db` unless explicitly overridden.

Use the built-in Kanban demo when you want to evaluate the operator board and
mutation dialogs without a GitHub token, a real ProjectV2 board, or production
database state:

```sh
detent dev-runtime --demo kanban --port 0
```

Pass `--demo-project` to choose the generated project ID when you want generic
demo URLs and labels instead of the default dogfood-safe ID:

```sh
detent dev-runtime --demo kanban --demo-project demo-project --port 0
```

Demo runtimes bind to `0.0.0.0` when `--host` is omitted so the selected
random port can be reached from trusted network interfaces. From another
machine on Tailscale, replace the local banner host with the Tailscale
hostname. With the override above, open `http://prometheus:<port>/kanban` for
the mixed-project board or
`http://prometheus:<port>/projects/demo-project/kanban` for the generated
project's interactive board. Pass `--host 127.0.0.1` for a local-only demo run.

The Kanban demo keeps the runtime isolated on the memory tracker, seeds at
least four projects with one or two cards each, and mixes configured project
colors with deterministic automatic colors. The fleet `/kanban` board is
read-only and shows cards across those projects; project-specific pages such as
`/projects/demo-project/kanban` enable integration mode for the generated demo
workflow. The demo includes explicit `server.kanban.allowed_transitions` such
as `Backlog -> Todo` so sheet-based Move actions can be exercised without
weakening production defaults. Demo cards cover Backlog, Todo, In Progress, Blocked,
Human Review, Rework, Merging, Done, and Cancelled states, including
issue-only cards, linked PR cards, CI pass, pending, and failure states, Codex
review clean and finding states, labels, assignees, blockers, and wait
metadata. Issue and PR comments are captured by the memory connector with no
external side effects.

Use the screenshots demo when you need deterministic pages, HTMX fragments, API
responses, reports, and SSE payloads for documentation screenshots, video
recording, or visual e2e baselines:

```sh
detent dev-runtime --demo screenshots --port 0
```

The screenshots demo uses the same isolation model and demo bind default as the
Kanban demo: memory
tracker, fake runner, isolated home, isolated database, isolated workspaces,
fake `https://github.test/...` URLs, no GitHub calls, no real ProjectV2
mutation, and no live dogfood port by default. It freezes demo time at
`2026-06-15T12:00:00Z` unless started with `--demo-clock play`, which advances
SSE ticks and visible running-work counters for video capture. The boot banner
prints the scenario manifest location. Screenshots mode intentionally keeps the
primary project fixed at `dogfood` so page routes and visual baselines remain
deterministic:

```text
Scenario manifest: /api/v1/demo/scenarios
```

Select a scenario with `X-Detent-Demo-Scenario`; the visible URL stays on the
normal page route:

```ts
const scenarios = [
  ["fleet-healthy-parallel-work", "/"],
  ["fleet-kanban-multiproject", "/kanban"],
  ["kanban-full-integration", "/projects/dogfood/kanban"],
  ["reports-normal-window", "/reports"],
];

for (const [scenario, route] of scenarios) {
  await page.setExtraHTTPHeaders({ "X-Detent-Demo-Scenario": scenario });
  await page.goto(`${baseURL}${route}`);
  await page.waitForLoadState("networkidle");
  await expect(page).toHaveScreenshot(`${scenario}.png`);
}
```

For visual comparisons, keep the screenshot environment stable: browser,
viewport, fonts, OS rendering, device scale factor, and generated assets should
match the baseline environment. The manifest includes each scenario ID, route,
required header, recommended viewport, screenshot name, and wait selector. A
quick JSON smoke check looks like this:

```sh
curl -H 'X-Detent-Demo-Scenario: fleet-healthy-parallel-work' "$DETENT_URL/api/v1/state"
```

Use the capture harness when you need the canonical video-production artifact
set from one command:

```sh
detent dev-runtime capture --out ./capture
```

The harness starts an isolated screenshots demo on an ephemeral local port,
loads the scenario manifest, captures the canonical still set, and writes a
deterministic terminal onboarding cast. It does not read or write the operator's
real `~/.config/detent/global.yaml`. Stable output paths are:

```text
capture/demo-capture-v1.json
capture/stills/v1/01-fleet-healthy-parallel-work.png
capture/stills/v1/02-fleet-kanban-multiproject.png
capture/stills/v1/03-kanban-full-integration.png
capture/stills/v1/04-project-active-overview.png
capture/stills/v1/05-reports-normal-window.png
capture/stills/v1/06-onboarding-project-selection.png
capture/terminal/v1/onboarding.cast
```

By default the browser viewport is `1920x1080` with
`--device-scale-factor 2`, producing 4K PNGs. Pass `--scenario <id>` one or
more times for a named subset, `--all-scenarios` for every browser-capturable
GET scenario in the manifest, `--width`, `--height`, and
`--device-scale-factor` for alternate framing, or `--demo-clock play` when a
motion capture needs advancing counters. The PNG capture uses a local
Chrome-family browser; pass `--browser <path>` or set `DETENT_CAPTURE_BROWSER`
when auto-detection cannot find one.

The CI browser visual gate runs Playwright when a PR changes UI-sensitive paths
such as `.github/workflows/ci.yml`, `go.mod`, `go.sum`, `package.json`,
`static/**`, `internal/web/**`, `internal/cli/dev_runtime*.go`,
`internal/devruntime/**`, Templ inputs, or screenshot/onboarding docs. It builds
the PR's Detent binary, starts isolated `dev-runtime` instances on port `0`,
captures current evidence under `tmp/playwright-evidence`, and uploads
Playwright reports, traces, screenshots, and image diffs when assertions fail.
PRs without UI-sensitive changes keep the required `Browser Visual` check fast
by building the Detent binary and running a CLI smoke instead of starting
Playwright.

Run the layout gate locally after installing Playwright's Chromium browser:

```sh
npx playwright install chromium
make visual-e2e
```

Committed image baselines are authoritative for GitHub Actions Ubuntu
x64/Chromium. On non-Linux hosts, `make visual-e2e` still runs the browser
layout assertions and captures evidence, but skips pixel comparison unless
`DETENT_VISUAL_STRICT=1` is set.

Update baselines only when the visual change is intentional. Run the update in
the same Ubuntu x64/Chromium environment as CI, then review and commit the
changed files under `tests/visual/__screenshots__/chromium/`:

```sh
make visual-e2e-update
```

Do not commit `tmp/playwright-evidence`, `tmp/playwright-report`, or
`tmp/playwright-results`; those are transient review and debugging artifacts.

Use the normal live runtime, `detent` with your global config, only when you
intend to operate on the configured tracker and ProjectV2 board. Use
`detent dev-runtime --fixture <path>` for focused fixture validation such as
autopromote behavior, `--demo kanban` for safe board exploration, and
`--demo screenshots` for deterministic page-addressable screenshots.

6. Start Detent:

```sh
detent
```

Open the dashboard at <http://localhost:4000>. Use `--host` and `--port` to
override the address. Before exposing a remote URL such as
`http://prometheus:4000/`, choose the dashboard bind mode:

On shutdown, the first Ctrl-C stops new dispatches and drains running agent
sessions while the terminal reports the blocker count and time remaining. A
second Ctrl-C force-quits immediately, interrupts those sessions, and re-queues
their issues. A new process using the same runtime database will fail with an
actionable error until the prior process has released it; a listener conflict
also fails before SQLite migrations begin.

- `127.0.0.1` keeps the dashboard local to the host and is the safest default
  for SSH tunnel access.
- A specific private or Tailscale IP exposes the dashboard only on that
  interface and is preferred for VPN-only access.
- `0.0.0.0` exposes the dashboard on every interface, not just Tailscale. Use
  it only on trusted private networks with the expected host firewall rules.

When Detent is bound to `127.0.0.1`, `curl` from the same host can work while
`http://<host>:4000/` fails from another machine because loopback is not
reachable remotely. Set `server.host` in `detent.yaml` for the default bind, or
set `--host` in the CLI command or service `ExecStart`:

```sh
detent --host 127.0.0.1 --port 4000
detent --host <tailscale-or-private-ip> --port 4000
detent --headless --host 0.0.0.0 --port 4000
```

Verify the listener and the local or VPN URL you intend operators to use:

```sh
ss -ltnp | rg ':4000|detent'
curl -fsS http://127.0.0.1:4000/api/v1/state
curl -fsS http://<tailscale-or-private-ip>:4000/api/v1/state
```
