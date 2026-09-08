# Quick Start

[Back to README](../README.md#documentation)

The quickest compatibility setup is one GitHub ProjectV2 board and one local
repository checkout. New projects can also run boardless: Detent reads and
writes either a repository's organization-level GitHub issue `Status` field or
repository status labels, then shows workflow visibility in Detent's own
Kanban/dashboard surface.

1. Authenticate GitHub access for the mode you want:

```sh
# ProjectV2-backed board mode.
gh auth login --scopes "repo,read:org,read:project,project"
# For existing auth:
gh auth refresh -h github.com --scopes "repo,read:org,read:project,project"

gh auth status 2>&1 | rg '\brepo\b'
gh auth status 2>&1 | rg '\bread:org\b'
gh auth status 2>&1 | rg '\bread:project\b'
gh auth status 2>&1 | rg "(^|[[:space:],'\"])project([[:space:],'\"]|$)"

# Boardless issue-field mode with a classic PAT.
gh auth login --scopes "repo,read:org"
gh auth status 2>&1 | rg '\brepo\b'
gh auth status 2>&1 | rg '\bread:org\b'

# Boardless label mode with a classic PAT.
gh auth login --scopes "repo"
gh auth status 2>&1 | rg '\brepo\b'
```

Fine-grained PATs and GitHub Apps should grant Issues repository read/write
when Detent will move work or post comments, Pull requests read/checks read for
PR gates, Issue Fields organization read for issue-field status discovery, and
repository label access for label mode. Issue-field writes use the issue field
values API and require issue or pull request repository write permission plus
push access to the repository. Label mode uses repository label reads/writes and
issue label updates. If Kanban integration mode is enabled in a release that
supports it, comment submission also requires issue/PR comment write.

2. Choose the GitHub status source.

For the current/default compatibility path, use a GitHub ProjectV2 board. Find
the node id and use the `id` field, which starts with `PVT_`, as
`tracker.project_slug`:

```sh
gh project list --owner <org-or-user> --format json --limit 20
```

The `gh project list` command verifies the token can read ProjectV2 boards.
The write `project` scope is verified when Detent first performs an intentional
board mutation, such as provisioning fields or editing an issue status.

Detent auto-provisions any missing `Status` and `Priority` options on the board
the first time it runs, so you do not have to hand-create every column — but the
option names it creates and reads must match the states in your `WORKFLOW.md`.
GitHub uses single-select option order as board column order; Detent keeps the
known status options in canonical board order and leaves extra custom options
after the required Detent states.

For issue-field mode, create or reuse an organization-level single-select issue
field named `Status` and make sure it is available to the repository. GitHub
issue fields are issue-only: linked PR cards in Detent derive status from the
linked issue, not from a PR field. Discover the field and options with:

```sh
gh api /orgs/<org>/issue-fields --jq '.[] | select(.name == "Status")'
```

For label mode, create or reuse repository labels for the effective Detent
states. Detent applies `tracker.state_map`, slugifies the resulting state name,
and prefixes it with `tracker.status_label_prefix`, which defaults to
`detent:`. With the default release flow, the required labels are
`detent:backlog`, `detent:todo`, `detent:in-progress`, `detent:blocked`,
`detent:human-review`, `detent:rework`, `detent:merging`, and `detent:done`.
With `tracker.auto_provision` enabled, Detent creates missing repository status
labels for configured workflow states on startup.
Discover existing labels with:

```sh
gh api repos/<owner>/<repo>/labels --paginate --jq '.[].name'
```

3. Create `detent.yaml` and `WORKFLOW.md` in the repository you want Detent to
   work on. Start from a paired `docs/templates/detent.*.yaml` and
   `docs/templates/WORKFLOW.*.md` preset.

Existing combined `WORKFLOW.md` frontmatter remains readable during the
compatibility window. Run `detent doctor` to identify legacy, split, mixed, or
stale layouts. A migratable warning includes the exact
`detent fix workflow-layout --workflow <path>` command; preview it with
`--dry-run`, confirm interactively, or pass `--yes` for explicit
non-interactive confirmation. The fixer uses the normal global-config path and
GitHub credential resolution (`GITHUB_TOKEN`, top-level `github_token`, and
`github_token: gh`) without writing resolved secrets into project files. See
[Workflow Layout Migration](workflow-layout-migration.md).

Legacy combined ProjectV2 example:

```markdown
---
tracker:
  kind: github
  github_status_source: project_v2
  project_slug: PVT_replace_with_project_id
  write_probe_issue: owner/repo#123
  http_max_idle_conns: 100
  http_max_idle_conns_per_host: 32
  http_idle_conn_timeout_ms: 90000
  github_graphql_warn_remaining: 500
  github_graphql_min_remaining_reserve: 1000
  github_rest_min_remaining_reserve: 1000
  github_rest_fanout_max_requests: 80
  github_rest_debug_logging: false
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  observed_states:
    - Backlog
    - Human Review
    - Blocked
  terminal_states:
    - Done
    - Cancelled
  state_map:
    Cancelled: Done
  priority_map:
    Urgent: 1
    High: 2
    Medium: 3
    Low: 4
    No priority: null
  dependency_auto_unblock:
    enabled: false
    source_states:
      - Blocked
    target_state: Todo
    readiness: terminal_or_merged
  blocked_recovery:
    enabled: false
    source_states:
      - Blocked
    target_state: Rework
    reason_codes:
      - merge_conflict
      - stale_base
      - missing_current_head_ci
  blocker_auto_promote:
    enabled: false
    blocker_states:
      - Backlog
      - Blocked
      - Human Review
    target_state: Todo
polling:
  interval_ms: 120000
worker:
  # Resolve ambient gh once, then inject only the token into the isolated worker.
  github_token: gh
  # Brake workers before the default 1000-request orchestrator dispatch floor.
  github_rest_min_remaining_reserve: 1250
  github_rest_poll_interval_ms: 60000
workspace:
  root: /absolute/path/to/detent-workspaces
  source_root: /absolute/path/to/project-checkout
  cache_strategy: isolated
  auto_branch: true
  cleanup_idle_ttl_ms: 86400000
  cleanup_sweep_interval_ms: 600000
agent:
  max_concurrent_agents: 5
  max_turns: 20
  max_turn_duration_ms: 0
  max_session_duration_ms: 7200000
  no_progress_timeout_ms: 5400000
  merge_worker_startup_timeout_ms: 240000
  merge_worker_max_duration_ms: 21600000
  max_retry_backoff_ms: 300000
  overload_retry_delay_ms: 45000
  no_progress_token_limit: 25000000
  no_progress_spend_limit_usd: 3
  failure_breaker:
    same_class_limit: 5
    window_seconds: 3600
    cooldown_seconds: 3600
  resume_orphaned_sessions: true
  max_concurrent_agents_by_state:
    Merging: 1
  dispatch_priority_by_state:
    - Merging
    - Rework
    - In Progress
    - Todo
  dispatch_priority_by_label:
    - bug
    - regression
    - enhancement
  prioritize_unblockers: true
  auto_promote:
    enabled: false
    quiet_seconds: 600
    optout_label: requires-human-review
    allowed_issue_labels: []
    gate_wait_state: review
    gate_wait_timeout_seconds: 3600
    gate_wait_timeout_action: human_review
    rework_limit: 3
  skills:
    enabled: true
    path: .detent/skills
    max_skills_in_prompt: 50
    creation:
      enabled: true
      max_drafts_per_run: 1
codex:
  command: codex app-server
  approval_policy: never
  thread_sandbox: workspace-write
  turn_sandbox_policy:
    type: workspaceWrite
    networkAccess: true
  turn_timeout_ms: 3600000
  read_timeout_ms: 5000
  stall_timeout_ms: 300000
gate:
  kind: command
  run: make check
  automated_review: required
  required_status_checks: []
  ci_failure_action: rework
  transient_ci_retry_limit: 2
  validator:
    enabled: false
    # Recommended cheap override when enabled: gpt-5.4-mini.
    # Watch rework-rate per validator model once cache/model telemetry lands.
    model: ""
    min_score: 0.8
    max_attempts: 3
    max_inline_diff_bytes: 65536
    block_on:
      - p1
server:
  host: 127.0.0.1
  port: 4000
  board_snapshot_stale_after_seconds: 900
hooks:
  timeout_ms: 60000
---
You are working on {{ issue.identifier }}: {{ issue.title }}.

Read the issue description, follow repository instructions, keep changes
scoped to the issue, run the project validation gate, and prepare the work for
human review.
```

Detent persists a last-known board snapshot beside the runtime database every
30 seconds. On restart it renders that snapshot with stale styling until live
tracker hydration completes. `server.board_snapshot_stale_after_seconds`
controls the maximum cache age; older snapshots are ignored.

`github_rest_fanout_max_requests` caps each bounded REST operation independently,
so dependency hydration cannot consume the capacity reserved for scheduled
admission reconciliation. Reaching the cap defers that operation locally and
does not report a GitHub rate-limit response. Set it to `0` to disable fanout
caps while retaining the REST remaining-reserve guard.

Tag-based projects can opt into release cadence in the same workflow file:

```yaml
release:
  enabled: true
  min_merged_issues: 5
  max_age_hours: 24
  require_green_ci: true
  version_bump: auto
  rerun_flaky_once: false
  flaky_check_names: []
```

Set `tracker.repository: owner/repo` as well, including in ProjectV2 mode, so
Detent can inspect commits and file failure issues on the tracked board.

The policy triggers after either threshold is reached, requires every check on
the default-branch head to finish green, creates an annotated semantic-version
tag, and observes the `Release` GitHub Actions workflow through completion.
`feat` commits bump the minor version; other conventional commits bump the
patch version. Failed candidate CI and failed release workflows create a
fingerprint-deduplicated `detent:todo` issue instead of retrying indefinitely.
The flaky rerun option is disabled by default and only reruns named workflow
checks once.

Boardless issue-field mode:

```markdown
---
tracker:
  kind: github
  github_status_source: issue_field
  repository: owner/repo
  status_field: Status
  write_probe_issue: owner/repo#123
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  observed_states:
    - Backlog
    - Human Review
    - Blocked
  terminal_states:
    - Done
    - Cancelled
  state_map:
    Cancelled: Done
workspace:
  root: /absolute/path/to/detent-workspaces
  source_root: /absolute/path/to/project-checkout
agent:
  max_concurrent_agents_by_state:
    Merging: 1
gate:
  kind: command
  run: make check
  required_status_checks: []
  ci_failure_action: rework
  transient_ci_retry_limit: 2
  validator:
    enabled: false
    # Recommended cheap override when enabled: gpt-5.4-mini.
    # Watch rework-rate per validator model once cache/model telemetry lands.
    model: ""
    min_score: 0.8
    max_inline_diff_bytes: 65536
    block_on:
      - p1
---
You are working on {{ issue.identifier }}: {{ issue.title }}.
```

Boardless label mode:

```markdown
---
tracker:
  kind: github
  github_status_source: label
  repository: owner/repo
  status_label_prefix: "detent:"
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  observed_states:
    - Backlog
    - Human Review
    - Blocked
  terminal_states:
    - Done
    - Cancelled
  state_map:
    Cancelled: Done
workspace:
  root: /absolute/path/to/detent-workspaces
  source_root: /absolute/path/to/project-checkout
agent:
  max_concurrent_agents_by_state:
    Merging: 1
gate:
  kind: command
  run: make check
  required_status_checks: []
  ci_failure_action: rework
  transient_ci_retry_limit: 2
  validator:
    enabled: false
    # Recommended cheap override when enabled: gpt-5.4-mini.
    # Watch rework-rate per validator model once cache/model telemetry lands.
    model: ""
    min_score: 0.8
    max_inline_diff_bytes: 65536
    block_on:
      - p1
---
You are working on {{ issue.identifier }}: {{ issue.title }}.
```

In label mode, the `detent:*` labels are tracker state, not workstream filters.
Use `tracker.authorization.labels.*`, `projects[].authorization`, and
`agent.dispatch_priority_by_label` for selecting or ranking work by ordinary
labels such as `documentation`, `bug`, or `enhancement`.
`agent.prioritize_unblockers` defaults to `true` and ranks an unlabeled issue
ahead of otherwise-equal peers based on its number of directly dependent blocked
issues. These issues may still have other unfinished prerequisites; the count
does not predict how many issues become runnable. State,
tracker priority, and every configured dispatch-label tier remain higher.

The fleet `/kanban` board stays read-only, as do observer dashboards and
shared dashboards where users should not mutate GitHub from Detent. For a
trusted operator-owned project board, default `server.kanban.mode:
integration` before mutation authorization. Skipped pre-mutation write probes
are not evidence for `read_only`; mutation still requires authorization and
post-authorization `detent doctor --allow-write-probes` to prove ProjectV2
status write, issue-field status write, or status-label update for the selected
status source, plus issue/PR comment write:

```yaml
server:
  kanban:
    mode: integration
    # Set show_blocked_alerts: true only when red blocked states should appear
    # as one compact top-of-board alert; dependency waits stay on cards.
    # show_blocked_alerts: true
    # Use mode: read_only for observer/shared dashboards, no-writes choices,
    # or failed post-authorization write probes.
    # allowed_transitions:
    #   In Progress: [Blocked, Cancelled]
    #   Rework: [Blocked, Cancelled]
    #   Merging: [Blocked, Cancelled]
    #   QA: [Blocked, Human Review]
```

When `allowed_transitions` is omitted, integration mode keeps conservative
defaults for manual moves from execution states: active work such as
`In Progress`, `Rework`, and `Merging` can only move to configured exception
states such as `Blocked` or `Cancelled`. Add source-specific entries to allow a
project workflow to expose extra manual moves without changing Detent's UI code.

Use `agent.instructions_by_state` and `agent.instructions_by_transition` when a
workflow stage needs prompt text that should stay out of the main
`WORKFLOW.md` body. The runner appends matching workflow instructions after the
base prompt and before Detent's deliverable and validation-gate blocks. State
keys and transition source/target keys must reference configured workflow
states.

For a non-code editorial workflow such as `Research -> Draft -> Review ->
Package -> Publish`, the stage-specific instructions can live in frontmatter:

```yaml
tracker:
  active_states: [Research, Draft, Review, Package]
  terminal_states: [Publish, Cancelled]
agent:
  instructions_by_state:
    Research: |
      Gather source notes, links, and open questions before drafting.
    Draft: |
      Write the article body and keep unresolved facts clearly marked.
    Review: |
      Address editorial feedback and leave a concise change summary.
    Package: |
      Prepare final assets, metadata, and publication handoff notes.
  instructions_by_transition:
    Review:
      Package: |
        Confirm all review comments are resolved before packaging.
```

`workspace.source_root` is the checked-out repository Detent uses for
`git worktree add`; `workspace.root` is where per-issue worktrees are created.
If `workspace.source_root` is omitted, Detent falls back to the project
`workdir` from global config, then to `workspace.root` for older single-root
setups.

`workspace.cache_strategy` controls the worker build environment. The default,
`isolated`, sets `GOCACHE`, `GOMODCACHE`, `GOBIN`, and
`GOLANGCI_LINT_CACHE` inside the per-turn scratch directory and removes them
after the worker exits. Set it to `shared` to place those caches in a stable,
per-project directory under `workspace.root/.detent/cache`, prepend the shared
`GOBIN` to `PATH`, and reuse compiled dependencies and tools across worktrees
and Detent restarts. `TMPDIR`, `TMP`, and `TEMP` remain per-turn in both modes.

When `workspace.auto_branch` is enabled and the source repository has an
`origin` remote, Detent fetches that remote's default branch before creating a
new managed branch. The source checkout's current branch and `HEAD` do not
affect the new branch base. Existing managed branches are reused unchanged;
local-only repositories without an `origin` continue to use `HEAD`.

`workspace.cleanup_idle_ttl_ms` controls how long non-active observed
workspaces can sit idle before cleanup. Terminal issues are cleaned on the next
poll when observed. `workspace.cleanup_sweep_interval_ms` controls the startup
and periodic idle cleanup cadence. `detent doctor` reports a warning when a
local Git workspace root reaches 50 retained workspace directories. Counts
below that boundary are not surfaced as a routine finding. At or above the
boundary, the finding also reports how many directories are not registered as
worktrees with the source repository.

`polling.interval_ms` defaults to `120000` and must be at least `60000`.
`polling.conditional` defaults to `true`. GitHub label and issue-field trackers
cache response ETags for issue lists and REST hydration endpoints, send
`If-None-Match` on later polls, and reuse the cached representation after a
`304 Not Modified`. An authorized conditional request that returns `304` does
not consume GitHub's primary REST quota. This makes a `60000` interval practical
for an idle REST-backed board while preserving the default cadence for existing
workflows. Set `conditional: false` to restore unconditional requests; trackers
without conditional-request support continue using their existing polling path.
`polling.interval_ms` only controls tracker and project-board refreshes; it does
not control Codex terminal-command waits or the enclosing tool-call yield
horizon, which are governed by developer instructions on Codex turns.
See GitHub's [conditional request guidance](https://docs.github.com/en/enterprise-cloud@latest/rest/using-the-rest-api/best-practices-for-using-the-rest-api#use-conditional-requests-if-appropriate).

REST telemetry distinguishes `total_requests`, `conditional_requests`,
`not_modified_requests`, and `billable_requests`. Endpoint contributors expose
the same breakdown, and the REST rate-limit card's cycle cost uses billable
requests rather than free `304` checks.


## Optional fleet model preset

To make existing and newly onboarded projects use Sol for ordinary work and
Astra for explicitly complex work, configure `global.agents.model_selection.preset:
sol_first` in the instance configuration. This defaults omitted effort to medium
(or high for very complex work). Explicit models and supported effort overrides
within the configured policy ceiling are preserved; legacy xhigh/max requests
use the selected level effort, including after resume.
Create `complexity:complex` and `complexity:very-complex` repository labels for
operators who want those signals, plus `design` if using an inherited Claude
selector. Generic `enhancement` does not select Astra. Projects inherit each
omitted setting; `agents.model_selection.enabled: false` opts out.

See the [fleet activation and inheritance examples](multi-project.md#instance-agent-defaults-and-sol-first-selection)
for full precedence, custom rules, fallback, ambient Claude authentication,
project opt-out, and pricing limitations. Run `detent doctor` to inspect effective
settings before dispatching new work.
