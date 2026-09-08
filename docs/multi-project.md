# Multi-Project Operation

[Back to README](../README.md#documentation)

Detent separates host-level orchestration from per-project definitions:

- The resolved global config file lists projects and host-level scheduling settings.
- Each project has `detent.yaml` for Detent-owned machine policy and
  `WORKFLOW.md` for portable agent instructions. The `projects[].workflow`
  path remains the definition anchor; Detent resolves the other files beside
  it, including when that directory is an explicit external definition root.

A minimal global config looks like this:

```yaml
apiVersion: detent/v1
kind: GlobalConfig
env: prod
log_level: info
github_token: gh
api_token: detent_replace_with_random_secret
port: 4000
instance_name: buildbox
update:
  auto_check_enabled: false
  check_interval_hours: 24
  auto_apply_enabled: false
global:
  max_concurrent_agents: 8
  rate_window_pacing:
    mode: proportional
    floor_percent: 20
    stale_after_seconds: 900
  scheduling: weighted
  active_hours:
    timezone: America/Chicago
    windows:
      - Mon-Sun 22:00-06:00
  agent_pools:
    - name: code
      max_concurrent_agents: 5
      burst_to: 8
    - name: video
      max_concurrent_agents: 10
      burst_to: 15
      scheduling: round_robin
  fair_share:
    half_life: 1h
  startup:
    jitter_seconds: 10
    max_spawn_per_second: 2
    max_concurrent_starts: 4
  memory:
    max_agent_rss_bytes: 8589934592
    pressure_some_avg60_threshold: 10
    poll_interval_ms: 1000
  io:
    pressure_full_avg10_threshold: 5
    degraded_max_concurrent_agents: 1
    poll_interval_ms: 1000
  cpu:
    pressure_some_avg10_threshold: 80
    degraded_max_concurrent_agents: 1
    poll_interval_ms: 1000
projects:
  - id: detent
    pool: code
    workflow: /absolute/path/to/detent/WORKFLOW.md
    workdir: /absolute/path/to/detent
    color: "#1192e8"
    weight: 2
    priority: 1
    active_hours:
      timezone: America/Chicago
      windows:
        - Mon-Fri 22:00-06:00
        - Sat-Sun 00:00-24:00
  - id: website
    workflow: /absolute/path/to/website/WORKFLOW.md
    workdir: /absolute/path/to/website
    weight: 1
    priority: 3
    paused: true
    paused_reason: waiting for website#42
    paused_at: 2026-07-27T12:00:00Z
    paused_until_issue: digitaldrywood/website#42
```

Project weights are relative scheduling weights. Higher weights receive a
larger dispatch share in weighted and fair-share scheduling modes. Project
priority is a rank: `0` is highest and `4` is lowest.

Lifecycle-state priority is preferred within each pool. In `weighted`,
`fair_share`, and `round_robin` pools, a ready project bypassed by one
higher-priority lane dispatch becomes rescue-eligible. Rescue-eligible projects
receive reservations before another priority-only reservation, while the
pool's configured project scheduler chooses among equally eligible projects.
With `R` continuously ready projects, a project receives a reservation within
at most `R` dispatch opportunities after its first higher-lane bypass. The
`strict` scheduler does not apply this bound and can intentionally starve lower
project or lifecycle priorities.

Readiness remains eligible for bypass accounting while a project scans its
current candidates and is cleared when that scan finds no demand. Dispatch
stall age survives restarts and refusal-reason changes by using the later of
the project's last successful selection and its oldest current candidate's
lane-entry time. A newly ready project therefore does not inherit idle time
from an older selection.

`global.agent_pools` defines independent agent-capacity partitions. Each
project belongs to exactly one pool through `projects[].pool`. A project with
no `pool` uses the implicit `default` pool, whose capacity is
`global.max_concurrent_agents` and whose policy is `global.scheduling`.
`default` is reserved and cannot be declared in `agent_pools`.

Every named pool requires a unique non-empty `name` and a positive
`max_concurrent_agents`. Its optional `scheduling` accepts `weighted`,
`strict`, `round_robin`, or `fair_share`; when omitted it inherits
`global.scheduling`. `max_concurrent_agents` is the pool's guaranteed
capacity. Optional `burst_to` must be greater than or equal to that guarantee
and lets the pool borrow unused guaranteed capacity from sibling pools up to
the configured ceiling. Omitting `burst_to`, or setting it equal to
`max_concurrent_agents`, keeps the pool rigid.

The sum of active pool guarantees is the shared capacity available for
borrowing. A borrower never displaces a running agent. When a lender has ready
work below its guarantee, new borrowed dispatches stop until natural
completion returns enough capacity; dispatch is not preempted across pool
boundaries. Contending borrowers are served in first-request order, one
admission at a time. Project selection, scheduling history, and strict-mode
preemption remain local to each pool. A configuration without `agent_pools` or
project `pool` fields retains the previous single-pool behavior.

`detent doctor` reports the last seven days of capacity waits for each project,
annotated with its local-heavy or cloud-only workload class. It identifies the
largest observed constraint across pool capacity, the project's
`agent.max_concurrent_agents`, lane-specific
`agent.max_concurrent_agents_by_state`, worker-host capacity, and subscription
provider rate-window backpressure. Each finding names the matching lever.
Rate-window backpressure points to `global.rate_window_pacing` and the
per-project `agent.rate_window_pacing` override. Raising a concurrency cap does
not increase the effective provider-paced limit.
Because pool refusals are sampled, all constraint reasons are normalized to
one observation per five-minute interval before doctor selects the binding
constraint. Telemetry from a project's previous pool assignment is ignored.

`global.rate_window_pacing` controls subscription-provider pacing for every
project unless that project's `agent.rate_window_pacing` explicitly overrides
it. `mode: proportional` is the default and scales concurrency by the lowest
fresh primary or secondary remaining percentage. `mode: off` never scales, so
`agent.max_concurrent_agents` remains the project cap and
`provider_rate_window_backpressure` cannot be a dispatch wait reason. `mode:
floor` preserves full width while the remaining percentage is at or above
`floor_percent`, then uses proportional scaling below that threshold.

Provider buckets older than `stale_after_seconds` (default `900`), buckets
without an observation timestamp, and snapshots with no primary or secondary
bucket fail open to configured concurrency. The board state and `/health`
report the effective mode, bucket status, observation time and remaining
percentage, permit ceiling, and whether scaling is active under
`dispatch.rate_window_pacing`. Turning pacing off or failing open shifts the
risk to exhausting the provider window completely.

A project override belongs in its `detent.yaml`:

```yaml
agent:
  rate_window_pacing:
    mode: off
```

A pool-bound project in a single-class pool is told to raise that pool's
capacity, never to split it. For an elastic pool this names `burst_to`, the
reachable ceiling; rigid pools name `max_concurrent_agents`. When mixed
workload classes share the implicit default pool and pool waits bind, doctor
preserves the initial `code` / `cloud` split recommendation: the code pool
keeps the current cap, the cloud pool gets a provider-tuned starting cap, and
the affected projects are printed as valid YAML. Configured pools are reported
but are never repartitioned automatically.

Doctor also checks capacity coherence without requiring telemetry. It warns
when member project caps cannot add up to a pool's declared capacity, when a
project cap exceeds its pool, or when an active work lane other than the
intentionally serialized `Merging` lane is capped below the project.

Preview or apply that exact recommendation with:

```sh
detent fix agent-pools --dry-run
detent fix agent-pools
# Explicit non-interactive confirmation:
detent fix agent-pools --yes
```

The fixer prints an additions-only diff, requires confirmation unless `--yes`
is supplied, preserves unrelated YAML keys, comments, and project ordering,
and writes `global.yaml` with mode `0600`. It is a no-op unless mixed workload
classes have binding default-pool waits. It also declines any config that
already declares `global.agent_pools`; changing an existing partition is an
operator decision. The accepted change is picked up by global-config hot
reload without a process restart.

Set optional `projects[].color` to an opaque CSS hex color in `#RGB` or
`#RRGGBB` form when a project needs a fixed visual marker. The sidebar,
project cards, and top-level multi-project Kanban board keep the project name
or ID visible and use color only as an additional compact marker. Projects
without a configured color receive a deterministic automatic color from a
curated categorical palette based on the project ID, so colors remain stable
across restarts and do not depend on project order. When there are more
projects than palette entries, Detent deterministically reuses palette colors;
labels and project IDs remain the primary identifiers.

Set `projects[].workflow_ref` only after the workflow file already exists at
that git ref, such as after the first `WORKFLOW.md` merge to `origin/main`.
When set, the workflow file is read from a git ref in the configured source
checkout instead of the checkout's working-tree copy. `workflow` may be an
absolute path under `workdir` or a repository relative path such as
`WORKFLOW.md`. When the ref advances, Detent reloads the workflow content from
that ref. `WORKFLOW.local.md` remains machine-local in the working tree and is
applied over the ref-backed shared file. When `workflow_ref` is omitted, Detent
keeps reading the working-tree shared file. If `workflow_ref` points at a ref
that does not contain the workflow file, `detent doctor` reports a load failure
for `<ref>:WORKFLOW.md`. For GitHub pull-request projects, doctor also warns
when `workflow_ref` is omitted and compares the checked-out branch and
`detent.yaml` with the source repository's default branch. For a configured
remote-tracking ref, doctor compares the local ref revision with its remote
counterpart. This freshness check uses `git ls-remote` but does not fetch;
fetch the ref so Detent can load the new revision when doctor reports it stale.

Use the project administration commands to edit `global.yaml`:

```sh
detent add-project \
  --id <id> \
  --workflow <WORKFLOW.md> \
  --workdir <dir> \
  --weight 1 \
  --priority 3

detent pause <id> \
  --reason "maintenance" \
  --until 2026-08-01T12:00:00Z
detent unpause <id>
detent resume <id> --for 2h
detent promote <id> --priority 1
detent remove-project <id>
```

These commands persist the global config. A running Detent process watches the
active `global.yaml`, including symlinked config targets, and reconciles
supported live-reload fields without a process restart. Invalid edits are
logged and ignored while the last valid config stays live.

`detent pause` requires `--reason` and accepts either `--until-issue <ref>` or
`--until <RFC3339 timestamp>`. Detent polls only the referenced tracker issue
for an issue-based pause and automatically writes the unpause to `global.yaml`
when the issue closes or the timestamp passes. The CLI records `paused_at` for
doctor diagnostics. Hand-edited legacy `paused: true` entries remain valid
without pause metadata or an automatic exit condition.

An issue reference may name another configured project. Detent resolves a
GitHub reference such as `digitaldrywood/website#42` through the configured
project whose tracker owns that repository, even when the paused project uses
a different tracker. Startup and live config reload reject references for
which no compatible configured tracker exists. Repeated runtime evaluation
failures appear in fleet health and on the board; an open issue that evaluates
successfully remains a normal, quiet pause.

Paused projects do not run workflow watchers or periodic workflow reconciliation.
`detent unpause <id>` synchronously reloads the project's current `WORKFLOW.md`
before dispatch resumes, so edits made while paused take effect on unpause. If
the current workflow cannot be loaded or prepared, unpause returns the error and
the project remains paused.

`active_hours` limits new agent dispatches to recurring wall-clock windows. It
may be set as a `global.active_hours` default, overridden by
`projects[].active_hours`, or placed in the project's `detent.yaml`. Host-local
`global.yaml` policy wins over project configuration. Every configured policy
requires an IANA `timezone` and one or more windows in
`Mon-Sun HH:MM-HH:MM` form. Weekday ranges are inclusive, `00:00-24:00`
represents a full day, and a range such as `22:00-06:00` wraps into the next
morning.

The gate is evaluated as a span on every dispatch decision, so a restart inside
a window admits work immediately and a restart outside it stays idle. At window
close Detent drains: running agents continue, while new dispatches receive the
benign `outside_active_window` refusal reason. Active hours never change
`paused` or its metadata, and manual pause remains stronger than an open
window.

Window edges keep wall-clock meaning across daylight-saving changes. A
spring-forward gap can shorten an overnight window by an hour; a fall-back
repeat can lengthen it by an hour. Membership evaluation avoids missed or
duplicated start events in both cases. `detent doctor` shows the next opening
and closing in the configured timezone and UTC.

Use `detent resume <id> --for 2h` or `detent resume <id> --until <RFC3339>` for
a one-shot active-hours override. The timestamp is persisted in
`active_hours_override_until`, admits dispatch outside the recurring window,
and expires without another command. It does not clear a manual pause; unpause
the project first when both gates apply.

For projects whose workflow file is already present on the target branch, you
can include `--workflow-ref origin/main` during registration or add
`workflow_ref: origin/main` to the project entry later.

| Field | Reload behavior |
| --- | --- |
| Project list and project settings, including active hours and overrides | Live reload; additions start only the new project, while removals and non-runtime definition changes replace only the affected project runtime |
| Credentials: `github_token`, `trust_loopback_peer_read`, and project credentials | Live reload |
| `dashboard_access` mode, token, and write access | Live reload; token changes invalidate private dashboard sessions |
| `auth` | Restart required; persisted sessions remain valid until their configured expiry |
| `global.startup` | Live reload |
| `instance_name` | Live reload |
| `ops.tmux_window_status` | Restart required |
| `global.identity` | Live reload; project runtimes restart in-process and `/api/v1/state.instance.name` updates after the next telemetry snapshot |
| `global.active_hours` | Live reload at the next dispatch decision; running agents drain when a window closes |
| `global.rate_window_pacing` and `agent.rate_window_pacing` | Live reload at the next dispatch decision without interrupting running agents; the project value wins when explicitly set |
| `global.max_concurrent_agents`, `global.scheduling`, `global.agent_pools`, `global.fair_share`, and project pool assignments | Live reload at the next dispatch decision; adding, removing, or lowering `burst_to` preserves active workers and drains to the new ceiling, and removed pools drain their active workers before retirement |
| `global.memory.pressure_some_avg60_threshold`, `global.memory.poll_interval_ms`, `global.io`, and `global.cpu` | Live reload in each project orchestrator without interrupting active attempts or provider sessions; threshold changes immediately re-evaluate the latest supported pressure sample instead of waiting for the next poll |
| `log_level` | Live reload |
| `port`, `env`, `log_max_size_bytes`, `log_max_backups` | Restart required |

When a changed field requires restart, Detent logs
`global config setting change requires restart` with the field name.

Each reload rereads the configured file before reconciliation and again after
reconciliation. If the file changes while reconciliation is running, Detent
continues with the newer snapshot and publishes only the final applied config.
Periodic source reconciliation also applies a valid change when the filesystem
watcher misses an event.

Project removal or a definition change that cannot be applied to the live
runtime inventories the affected project's active sessions before replacing
it. The `global config reconciliation completed` and `global config reloaded`
records list those sessions under `drained_sessions`. Unchanged project
runtimes and their sessions continue without interruption.

## Running Multiple Instances

Run more than one Detent instance when a single GitHub ProjectV2 board should
be split across independent workers. Each instance is a separate `detent`
process with its own `global.yaml`, process identity, authorization selector,
runtime database, listener address, and claim lease. The instances may point at
the same `tracker.project_slug`,
but their authorization selectors should be disjoint so each issue belongs to
one worker set before claiming begins.

Use `global.identity` for the process identity in multi-instance operation.
That identity is applied to every project in that `global.yaml` and overrides
workflow-level identity while the project is loaded from global config. A
workflow can still define top-level `identity` for single-project runs, but do
not put identity under a `projects` entry in `global.yaml`; project entries only
carry scheduling, paths, credentials, pause state, and authorization selectors.

```yaml
apiVersion: detent/v1
kind: GlobalConfig
global:
  max_concurrent_agents: 4
  scheduling: weighted
  identity:
    name: detent-alpha
    github_login: detent-alpha
    ownership_mode: field
    owner_field: Detent Owner
projects:
  - id: detent-alpha
    workflow: /absolute/path/to/detent/WORKFLOW.md
    workdir: /absolute/path/to/detent
    weight: 1
    priority: 1
    authorization:
      labels:
        include:
          - scope:alpha
```

A second instance can use the same workflow and board with a different identity
and a non-overlapping selector:

```yaml
apiVersion: detent/v1
kind: GlobalConfig
global:
  max_concurrent_agents: 4
  scheduling: weighted
  identity:
    name: detent-beta
    github_login: detent-beta
    ownership_mode: field
    owner_field: Detent Owner
projects:
  - id: detent-beta
    workflow: /absolute/path/to/detent/WORKFLOW.md
    workdir: /absolute/path/to/detent
    weight: 1
    priority: 1
    authorization:
      labels:
        include:
          - scope:beta
```

The selector schema is the same in `projects[].authorization` and
`tracker.authorization`: `assignee_in`, `author_in`, `priority_in`,
`labels.include`, `labels.exclude`, `fields`, `and`, and `or`.
`projects[].authorization` from `global.yaml` is combined with
`tracker.authorization` from `detent.yaml` as an `and`, so both selectors must
match. Use `@me` inside `assignee_in`, `author_in`, or field selector values to
match the current instance identity (`github_login` and `name`). For example,
one common pattern is a global project selector for a broad lane label and a
workflow selector for a board field:

```yaml
tracker:
  authorization:
    fields:
      - name: Workstream
        value: engineering
```

Authorization only decides which issues an instance is allowed to consider.
Claiming is the final concurrent-dispatch guard. Enable it in the shared
workflow so all instances use the same lease field and TTL:

```yaml
tracker:
  claims:
    enabled: true
    lease_field: Detent Lease
    ttl_seconds: 900
    heartbeat_seconds: 120
```

When claims are enabled, Detent writes ownership first, then writes
`lease_field` with a UTC RFC3339 timestamp, refetches the issue, and dispatches
only if the refreshed owner and lease still match the current instance. With
`ownership_mode: assignee`, ownership is the GitHub assignee and `owner_field`
must be omitted. With `ownership_mode: field`, ownership is written to
`identity.owner_field`, which must exist on the board. While another owner has
a fresh lease, the issue is skipped. When the lease timestamp is stale by
`ttl_seconds` or missing, another matching instance may reclaim it. Detent
refreshes running claim leases every `heartbeat_seconds`; that value must be
greater than zero and less than or equal to `ttl_seconds`.

Setting `identity.assignee_required: true` separately makes an assignee a
dispatch eligibility requirement. The default is `false` so upgrading Detent
cannot silently make an existing `ownership_mode: assignee` declaration begin
blocking unassigned work. Run `detent doctor` before enabling the requirement;
it lists active issues that would stop dispatching.

Task-to-model routing also lives in `detent.yaml`. If `agents.backends` is
omitted, routes can reference the legacy `codex` backend built from the top-level
`codex` block. Routes are evaluated in order, skipping defaults first; the first
non-default selector match wins, then the single `default` route is used. A
route can set a fixed `model`, read a model from a ProjectV2 field with
`model_field`, or fall back to an issue model override when neither is set.
Routes without `role` are code-agent routes. `Runner.Run` dispatches plan mode
with `role: plan`, Rework-state issues with `role: rework`, Merging-state
issues with `role: merge`, and all other implementation dispatches with
`role: code`. Set `role: validator` to give the validator-agent review its own
backend/model route when `gate.validator.enabled` is true. If a stage-specific
route does not match, Detent falls back to that role's default route and then to
the code default route, preserving the zero-config behavior.
If the validator runs through the Codex backend, prefer setting
`gate.validator.model: gpt-5.4-mini` as the cheap-tier override before adding a
separate validator route. Treat rework-rate per validator model as the quality
signal once cache/model telemetry lands; increase the validator tier only when
that rate worsens.

```yaml
agents:
  routes:
    - name: plan-cheap
      role: plan
      backend: codex
      model: gpt-5.4-mini
    - name: rework-high-context
      role: rework
      backend: codex
      model: gpt-5-codex-high
    - name: merge-standard
      role: merge
      backend: codex
      model: gpt-5-codex
    - name: high-context
      backend: codex
      model: gpt-5-codex-high
      selector:
        labels:
          include:
            - model:high
    - name: board-model
      backend: codex
      model_field: Model
    - name: default
      backend: codex
      model: gpt-5-codex
      default: true
```

For explicit backend profiles, configure `agents.backends` and route to those
ids. Supported backend kinds are `codex` with `protocol: app-server` and
`claude_code` with `protocol: headless`. Codex backend `options` use the same
runtime fields as the top-level `codex` block, including `shell`,
`approval_policy`, `thread_sandbox`, `turn_sandbox_policy`, `turn_timeout_ms`,
`read_timeout_ms`, and `stall_timeout_ms`. `agent.max_turns`,
`agent.max_turn_duration_ms`, `agent.max_session_duration_ms`, and
`agent.no_progress_timeout_ms` apply across backends rather than belonging to
an individual backend profile. Claude Code backend `options`
include `permission_mode`, `allowed_tools`, `disallowed_tools`,
`include_partial_messages`, `turn_timeout_ms`, `stall_timeout_ms`, `shell`, and
`extra_args`. When a Codex backend needs different configuration, launch
`codex app-server` with a dedicated `CODEX_HOME` or `-c` overrides. When a
Claude Code backend needs isolated state, launch `claude` with a dedicated
`CLAUDE_CONFIG_DIR`.

```yaml
agents:
  backends:
    - id: codex-standard
      kind: codex
      protocol: app-server
      command: codex app-server
    - id: codex-high
      kind: codex
      protocol: app-server
      command: env CODEX_HOME=/opt/detent/codex-high codex app-server
    - id: claude-worker
      kind: claude_code
      protocol: headless
      command: env CLAUDE_CONFIG_DIR=/var/lib/detent/claude/worker-1 claude
      options:
        permission_mode: bypassPermissions
        allowed_tools:
          - Bash
          - Edit
        disallowed_tools:
          - WebFetch
        extra_args:
          - --no-session-persistence
  routes:
    - name: validator
      role: validator
      backend: claude-worker
      model: fable
    - name: high-label
      backend: codex-high
      model: gpt-5-codex-high
      selector:
        labels:
          include:
            - model:high
    - name: default
      backend: codex-standard
      model: gpt-5-codex
      default: true
```

Claude Code auth is ambient and the backend is auth-agnostic. A logged-in
`claude` CLI uses the operator's subscription login. Setting
`ANTHROPIC_API_KEY` in the Detent worker environment switches the same backend
to API billing, and that key takes precedence over the subscription login.
Detent stores no Anthropic keys, mirroring the way Codex credentials stay
outside Detent.

Claude Pro/Max subscription limits are opaque in headless `claude -p` mode.
The 5-hour windows and weekly caps do not expose an in-band "limit approaching"
signal; a cap hit appears only as an error result from the turn. Use
subscription auth for bounded or bursty personal operation. Use
`ANTHROPIC_API_KEY` for sustained or parallel fleet runs where predictable
billing and capacity matter.

For fleet isolation, set a distinct `CLAUDE_CONFIG_DIR` per worker process so
concurrent `claude` invocations do not race on config or session state. Add
`--no-session-persistence` through `options.extra_args` when workers do not
need Claude Code session continuity; otherwise sessions accumulate under
`~/.claude/projects/`.

The sandbox model differs by backend. Codex runs turns under an OS-level
`workspace-write` sandbox. Claude Code headless runs inside Detent's isolated
git worktree with `permission_mode: bypassPermissions`, but that is not an OS
sandbox: allowed shell tools can still access the host as the Detent worker
user. Treat the worktree as the checkout boundary, use container, VM, or OS
sandbox isolation when you need a hard blast-radius boundary, and tighten role
exposure with `allowed_tools` plus `disallowed_tools`. Choose backend routes
with that trade-off in mind, especially for roles that can execute shell
commands or edit files.

For local Anthropic-compatible inference, point `ANTHROPIC_BASE_URL` at a local
server such as Ollama, which has native Anthropic API compatibility as of
January 2026, and keep using the `claude_code` backend. See
[Local Models With Codex And Ollama](local-models-ollama.md) for the
model sizing and context-window checks that also apply when evaluating local
agent backends.

The dashboard and `/api/v1/state` surface each instance identity, authorization
scope, owner, lease renewal time, lease expiry, and selected model usage, which
lets operators verify that scoped instances are not contending for the same
work.


## Instance agent defaults and Sol-first selection

Enable the recommended preset once in the instance's `global.yaml`. Existing
projects and future registrations inherit it without editing their definitions:

```yaml
global:
  agents:
    model_selection:
      preset: sol_first
```

This is opt-in. Existing pinned models remain pinned. The preset sets normal
work to `gpt-5.6-sol` at medium effort; complex work to `gpt-6-astra` at medium;
and very complex work to Astra at high. It never automatically assigns max.
Explicit issue effort is retained when supported and within the configured
effort ceiling. Legacy xhigh/max requests select complex work but use medium
effort under this preset; very complex work uses high.
A provider default changing to Astra does not affect enabled automatic selection.

Host backend and route defaults use the same schema as project `agents`. For
example, share a Claude design selector alongside each project's Codex default:

```yaml
global:
  agents:
    backends:
      - id: design
        kind: claude_code
        command: /opt/detent/bin/claude-wrapper
    routes:
      - name: design
        backend: design
        model: sonnet
        selector:
          labels:
            include: [design]
    model_selection:
      preset: sol_first
```

The absolute wrapper command is supported, as is `command: claude`. Authenticate
Claude Code on the host using its ambient authentication before enabling the
route. This configuration neither copies credentials nor changes subscription
billing. The preset's `backend_kinds: [codex]` excludes Claude. The selector's
explicit `sonnet` remains authoritative for design work.

Backends merge by ID and routes by name. Project entries replace an inherited
entry as a whole; omitted entries inherit. Project selectors are evaluated in
project order before inherited selectors in instance order. A project default
route takes precedence over an instance default for the same role, even when
named differently. Legacy projects retain their implicit Codex backend and
default route. Inherited entries require names/IDs. Referenced disabled or missing
backends make the effective configuration invalid.

A project can opt out of a selector without copying the instance configuration:

```yaml
agents:
  routes:
    - name: design
      disabled: true
```

Use `disabled: true` on a backend ID to remove that inherited backend too, and
remove/disable any routes referencing it. `agents.backends: []` clears inherited
backends and restores the legacy project Codex backend; `agents.routes: []`
clears inherited routes and restores the implicit project default route. Disabling
the named `default` route explicitly suppresses that implicit route.

To migrate repeated `detent.local.yaml` overlays, copy the common backend and
named selector to `global.agents`, verify with `detent doctor`, then remove each
redundant project entry. Keep per-project defaults and deliberate deviations.
During onboarding, create the `design` label in each repository that will use the
selector; issue labels are not created by loading configuration. Projects with
pinned models must remove those pins to use automatic selection.

### Policy overrides and clearing

Every policy field inherits independently: preset, enable flag, normal/complex
models, eligible backend kinds, default level, level model/effort defaults, stage
model/effort/complexity defaults, ordered rules, unavailable behavior, and fallback
order. The same `agents.model_selection` schema works in a project's
`detent.yaml` or `detent.local.yaml`:

```yaml
agents:
  model_selection:
    complex_model: gpt-6-astra
    levels:
      very_complex:
        effort: high
    stages:
      validator:
        model: normal
        effort: medium
    unavailable: fallback
    fallback_order: [normal]
```

Omission inherits. `enabled: false` disables automatic selection for that project.
`preset: ""` removes the inherited preset; a fully custom enabled policy must then
provide normal/complex models, a default level, level defaults, and unavailable
behavior. `normal` and `complex` in level/stage models and fallback entries refer
to the configured normal/complex model; any other nonempty string is a literal
backend model ID. No model catalog choice is inferred from cost or provider order.

`levels` and `stages` merge by name and then by individual field. Explicit `{}`
clears the corresponding map. Clearing all levels requires disabling the policy
or supplying a replacement default level. Empty stage model/effort/level strings
remove that stage override and use the selected level's defaults. Missing stages
or `issue_complexity: false` do not consume issue-wide complexity signals.

`backend_kinds: []` makes no backend eligible. `fallback_order: []` permits no
fallback. `rules: []` clears all complexity rules. Nonempty rule lists merge by
rule name: replacement retains its order, new names append, and `disabled: true`
disables a named inherited rule. A replacement rule is a complete rule, not a
field patch. The first matching enabled rule wins. To reorder rules, clear them
at the instance and supply a complete project list (or use a custom empty preset).
YAML null is omission, not a clearing operation.

Pricing uses the existing external pricing-file format. Set
`global.budget.pricing_path: /absolute/path/models.yaml` for an instance default;
project `budget.pricing_path` overrides it independently. An explicit empty
project path selects the embedded table. Other budget fields, limits, and
subscription enforcement are unchanged. Use absolute instance pricing paths so
projects do not interpret the same relative path from different working directories.

### Complexity and independent precedence

The preset recognizes these deterministic signals; it does not inspect prose or
make a classification-model call:

| Signal | Automatic model | Default effort |
| --- | --- | --- |
| Missing metadata, simple/basic work, or generic `enhancement` alone | Sol | medium |
| `complexity:complex` for substantive feature/implementation work | Astra | medium |
| `complexity:very-complex` for subsystems, difficult architecture, state, concurrency, or recovery | Astra | high |
| Explicit applicable issue xhigh/max, with model omitted | Astra | medium, bounded by the policy |

Priority, entering Rework, waiting for CI, provider errors, and host/backend
effort defaults are not complexity signals in the preset. Label substantive work
intentionally; a trivial enhancement needs no complexity label. Code, plan, and
rework consume issue-wide signals. Routine, merge, validator, and security-audit
stage defaults suppress issue-wide complexity. A role-specific effort signal or a
rule with an explicit `roles` list can select complexity for just that stage.
For example, a custom rule can match issue fields without upgrading every stage:

```yaml
agents:
  model_selection:
    rules:
      - name: complex-validator
        roles: [validator]
        level: complex
        selector:
          fields:
            - name: Validation complexity
              value: complex
```

The `detent-agent` block supports model and effort independently for code, rework,
plan, merge, routine, and validator. Rework falls back to code for
each omitted role-specific field. The model precedence for normal/validator
execution is role-specific issue body, issue-wide body, explicit stage model
(`gate.validator.model` for validators), selected route fixed model, route model
field, connector issue model, applicable default-route model, then automatic
selection. Within routing, existing role/default precedence is preserved.
Effort precedence is role-specific issue body, issue-wide body, project
`agent.effort`, backend effort, automatic stage effort, then automatic level effort.
The independent security auditor retains its trusted gate model and does not
consume issue-body model or effort overrides; its unpinned defaults use the
configured `security_audit` policy stage.
A model-only override still receives an effort default; an effort-only override
still receives an automatic model.

The highest effort among configured levels and the applicable stage is the
policy ceiling. Issue, role, project, and backend effort overrides above that
ceiling use the selected level/stage effort, with `:ceiling` recorded in the
effort source. Lower supported overrides remain unchanged. To deliberately
permit xhigh, configure an applicable policy level or stage at xhigh. Disabled
policies and excluded backends retain their existing override behavior.

For an Astra fleet, set both model aliases to `gpt-6-astra` and configure normal,
complex, and very_complex effort as low, medium, and high. Existing xhigh/max
complexity signals then request medium instead of passing the legacy effort to
the provider. No issue-body migration is required for this protection.

With the policy enabled on an eligible backend, invalid explicit model/effort
values are reported and dispatch fails; they are never replaced by automatic
fallback. Policy-disabled and excluded backends retain the existing explicit
catalog rejection behavior. If an automatic model is missing or retired,
`unavailable: fallback` tries `fallback_order` in order; the preset tries Sol.
`unavailable: fail` refuses fallback. No available configured model or a failed
catalog lookup stops dispatch with an actionable error. It never inherits an
arbitrary provider default, including during capacity failures.

`detent doctor` shows the effective policy and per-setting instance/project/preset
sources, plus backend/route provenance without commands or environment values.
Session runtime identity and session diagnostics record the policy, matched rule
or default reason, requested automatic model, actual requested/runtime models,
effort, source fields, and fallback reason. Runtime updates preserve the decision.
Policy reload affects new dispatches. Active turns keep their original snapshot;
stop and restart an active turn when an immediate cost reduction is needed.
Resumed sessions preserve their established thread, model, backend, and route,
but recheck the current effort ceiling and honor lower current issue overrides.
A changed resume effort requires catalog validation before another provider turn.
Missing resume backends require
restoring that backend or starting fresh. Legacy sessions without stored full
identity retain legacy recovery checks. Invalid reload retains the last valid
agent configuration. Hub-approved execution continues to require its approved
effective policy; inheritance does not authorize unapproved policy changes.

### Pricing provenance

Verified September 7, 2026 against the official [API pricing table](https://developers.openai.com/api/docs/pricing):

| Embedded model | Input USD / 1M | Cached input USD / 1M | Output USD / 1M |
| --- | ---: | ---: | ---: |
| gpt-5.6-sol | 4.00 | 0.40 | 20.00 |
| gpt-5.6 (Sol alias) | 4.00 | 0.40 | 20.00 |
| gpt-6-astra | 10.00 | 1.00 | 50.00 |

[Sol's model documentation](https://developers.openai.com/api/docs/models/gpt-5.6-sol)
confirms the alias and promotional pricing through at least November 21, 2026.
These are standard short-context API estimates, not verified bills or
subscription invoices. Astra is 2.5 times Sol's token rates for the same token
mix; task cost also depends on consumption, caching, and retries. Fast mode,
long-context rates, cache-write premiums, Batch/Flex, and account billing can
change the estimate. Detent does not separately model cache-write premiums.
The [subscription credit table](https://learn.chatgpt.com/docs/pricing) uses
credits, not USD; its published standard Sol/Astra rates are 100/10/500 and
250/25/1250 per million input/cached/output tokens. Fast-mode multipliers differ
between billing surfaces; do not apply a universal API-to-credit multiplier.
External pricing files retain their existing override behavior.
