# Execution Seams

Detent's orchestration loop is mostly domain-neutral: it reads board state,
dispatches work, tracks retries and budgets, and serializes the `Merging` lane.
The execution edge now has explicit seams for code workflows and local
artifact workflows. GitHub PR delivery remains the default.

## Now Pluggable

### Work Item Source And Status Store

- `tracker.kind: github` remains the default production source for code
  workflows.
- `tracker.kind: local_sqlite` stores domain-neutral work items in a
  Detent-owned SQLite database configured by `tracker.local_sqlite.path`.
- Local SQLite work items persist status, fields, metadata, deliverable
  metadata, timestamps, comments, and state-change events across restarts.
- Kanban integration-mode moves continue to call the connector state update
  contract, so local SQLite projects write card moves into the local status
  store instead of GitHub Projects, labels, or issue fields.
- Relative local SQLite paths resolve against the project `workdir` at runtime
  so a non-code project can keep `WORKFLOW.md`, inputs, outputs, and state under
  one production directory.

### Workspace

- `workspace.kind: local_git` is still the default and creates git worktrees
  and branches.
- `workspace.kind: filesystem` creates an isolated task directory under
  `workspace.root` with an `artifacts/` subdirectory and no git branch or PR
  contract.
- `workspace.output_root` or `deliverable.output_root` can point at a durable
  artifact handoff directory. Detent creates a per-work-item output directory
  there for external production systems to consume.
- Filesystem workspace paths are project-workdir relative when configured as
  relative paths.

### Deliverable

- `deliverable.kind: pull_request` is the default and keeps existing GitHub PR
  behavior.
- `deliverable.kind: artifact` suppresses prompt instructions that require a
  PR closing reference and adds artifact workspace/output/review metadata to
  the agent prompt.
- `connector.Issue.Deliverable` and telemetry snapshots can carry artifact
  kind, path, review URL, validation status, external id, and metadata without
  requiring PR fields.

### Agent Backend

- `internal/runner` owns `AgentBackend`, backend factories, and task routing.
- `internal/config` exposes `agents.backends` and `agents.routes`.
- Domain, label, priority, assignee, author, or ProjectV2 field based routing
  is handled by the existing selector and router.

### Validation Gate

- `internal/config` exposes top-level `gate`.
- `internal/gate` selects and evaluates gate behavior.
- `gate.kind: command` is the default and keeps the code workflow contract:
  run `gate.run` (`make check` by default) before Human Review, then require
  green CI and clean automated review before promotion.
- `gate.kind: command` with `require_automated_review: false` keeps the
  command, linked PR, green CI, no-P1, and quiet-period checks but does not
  require an automated GitHub PR review to exist before promotion.
- `gate.required_status_checks` lists release-blocking check-run names or
  commit status contexts that must be present on the current PR head, completed,
  and successful. Missing, skipped, failed, cancelled, neutral, or still-running
  required checks keep CI non-green and block promotion.
- `gate.kind: command` routes failed or cancelled current-head CI from
  `Human Review` back to `Rework` by default. Set
  `ci_failure_action: skip` only when red CI should remain parked in
  `Human Review`.
- The quiet period resets on observed issue updates, Project status updates,
  automated PR review submission, and linked PR activity such as a fresh push
  to the PR head.
- `gate.kind: human_review` requires a PR plus `gate.approval_label`
  (`human-approved` by default) before promotion.
- `gate.kind: artifact` evaluates a status value from
  `issue.deliverable.validation_status` or a configured work-item field such as
  `render_status`. Passing statuses advance the item, waiting statuses leave it
  in place, and rework statuses route it to rework without requiring PR, CI, or
  automated PR review state.
- `internal/runner/prompt.go` renders gate variables and appends gate
  instructions so `Todo`, `Rework`, and `Merging` agents see the configured
  validation contract.
- `internal/orchestrator/autopromote.go` delegates the pass/wait/rework decision
  to the configured gate while preserving the surrounding opt-out and label
  policy.

### Merge Waits

- The quiet window before auto-promotion is an intentional quality gate.
- The serialized `Merging` lane should run a focused rebase/smoke gate when the
  PR already passed the pre-review gate, the rebase is clean, and no source
  files change during the rebase.
- `Merging` must still run the full configured gate after merge-agent edits,
  conflict resolution, stale validation, or unknown validation state.
- Current-head CI waiting should use REST check-run polling with backoff and
  clear slow-check status rather than GraphQL-heavy PR polling loops.
- Merge telemetry should report quiet-window wait, GitHub queue/start wait,
  local merge-gate duration, current-head PR CI duration, active slow-check
  runtimes, and whether post-merge `main` CI is still running.
- Duplicate full local validation after a source-clean rebase, uncached tool
  install, noisy polling, and post-merge work that is not part of the merge
  decision are optimization targets.
- The CI lint job keeps the project-pinned golangci-lint version and caches its
  binary so cache hits avoid a per-run `go install` without changing lint
  coverage.
- `GoReleaser Snapshot` is a release-blocking check that runs on pull requests,
  `main`, release tags, the nightly CI schedule, and manual workflow dispatch.

### Main Branch Protection

The `main` branch must require pull requests and up-to-date validation before
merge. The expected GitHub branch protection or ruleset setting is
`required_status_checks.strict: true`; if the repository switches to merge
queue, the queue must provide equivalent current-base validation before merge.

Required status checks must include every merge-blocking CI job directly. Do
not depend on a downstream skipped job to protect an upstream gate. A required
check name must not report success from a path- or event-dependent no-op when
the same named check runs real validation on `main`; `Browser Visual` is still a
real gate because non-UI pull requests run a Detent binary smoke instead of a
green no-op.

Required PR merge checks, branch protection/rulesets, and
`gate.required_status_checks` must name the same release-blocking checks:

- `Lint` - budget: `2m`
- `Verify (ubuntu-latest)` - budget: `30m`
- `Test Coverage` - budget: `4m`
- `Browser Visual` - budget: `15m`
- `Portability Verify (macos-latest)` - budget: `8m`
- `Portability Verify (windows-latest)` - budget: `45m`
- `Windows Core` - budget: `4m`
- `Installer Smoke (ubuntu-latest)` - budget: `6m`
- `Installer Smoke (windows-latest)` - budget: `6m`
- `GoReleaser Snapshot` - budget: `15m`

Browser and snapshot budgets are whole-job ceilings, enforced with
`timeout-minutes: 15`, including setup and artifact handling. Repeated hosted
Linux measurements put full browser jobs at `10m49s`–`11m23s` and snapshot jobs
at `10m3s`–`10m39s`, even with Go cache hits. The full Playwright step alone
took `8m17s`–`8m41s`; a non-visual PR's binary smoke took `21s` in a `48s` job
and is not a basis for the full-check budget. The ceilings leave more than
three minutes above the observed maxima for runner variation and retries;
they are bounds, not predicted runtimes or measured cold-cache guarantees.
See [browser and snapshot measurements](../.detent/validation/2310/README.md)
for runner/cache provenance, generation and compilation costs, packaging
costs, and the assessment of repeated build and fixture work. Full browser
selection, release hooks, all six release targets, archives, Linux packages,
checksums, and minisign signing remain required work.

The required portability checks are limited to build, vet, and the non-race
test suite. Windows runs packages sequentially with four-way test parallelism
inside a `45m` job budget and retains per-package JSON evidence plus a failure
classification summary. `Windows Core` owns only the command smoke, so no Go
package has two required Windows test owners. Repeated race loops, the full
race suite on macOS and Windows, and the Windows SQLite artifact lifecycle
stress loop run nightly and on manual dispatch in
`.github/workflows/portability-stress.yml`. That workflow has a `45m` ceiling;
failures are surfaced as failed `Portability Stress` workflow runs with the
failing command output in the job log.

Release and required portability checks also run on `main`, on `v*` release
tags, on the nightly CI schedule, and from manual workflow dispatch. They are
still release-blocking for pull requests; Detent must not promote or merge a PR
head where one of these checks is missing, skipped, failed, cancelled, neutral,
or still running.

GitHub merge queue adoption is deferred. Detent currently owns merge ordering,
rebases each head onto current `main`, validates that exact head, and merges it
through the REST API. GitHub merge queue would require Detent to enqueue rather
than merge, validate `merge_group` commits, and reconcile queue ejections back
to tracker state. That larger delivery-state change is not justified merely to
remove the long portability checks from this critical path. The trade-off is
that strict current-base validation still invalidates other open heads after a
merge; keeping all required jobs within their written budgets bounds that cost
without weakening `required_status_checks.strict: true`.

The CI workflow keeps pull request runs cancellable by newer pushes to the same
PR through `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`.
Push, tag, schedule, and manual runs use a unique run group and must not be
cancelled by later runs. The workflow test in `ci_workflow_test.go` checks this
section against `.github/workflows/ci.yml` so job-name, required-check, wall-time
budget, confidence-check, and green no-op drift fails in local validation.

## Still Git/PR Coupled

- `internal/connector/github` discovers pull requests by issue branch prefix and
  reads PR state, CI status, check-run timing, slow checks, running checks, and
  automated review state.
- The dashboard and telemetry models render a PR pipeline for `Human Review`,
  `Merging`, and terminal states for pull-request workflows.
- Merge workers, CI polling, PR hydration, and branch cleanup still apply only
  to `deliverable.kind: pull_request` and `workspace.kind: local_git`.

## Follow-Up Surface

The local SQLite source is Detent-owned. External production systems can consume
`detent_work_items` and `detent_work_item_events`, or watch a configured output
directory. A future backend can map Detent status changes into an
operator-provided SQLite schema or application database when a stable schema
contract is known.

`docs/templates/WORKFLOW.non_code_artifact.md` shows a video-production
workflow that uses local SQLite status, filesystem workspaces, artifact
deliverables, and artifact gates. That mode is broader than the GitHub local
status mode described by #779: #779 keeps GitHub issues as the catalog while
Detent owns status locally; the artifact template does not require GitHub
issues, PRs, branches, CI, or merge trains at all.
