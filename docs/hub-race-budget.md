# Hub race test budget

Run `make test-race` for the complete repository race gate, or
`make test-race-hub` for the complete Hub package. Both CI and `make check`
use this target. Hub runs separately with two parallel test slots, an
uncached race detector run, and a 15-minute package timeout. Other packages
retain Go's default package concurrency and 10-minute timeout. No test name,
assertion, fixture workload, or individual lifecycle deadline is changed.

The Hub ceiling is a package budget, not an individual operation deadline.
The recorded Linux/amd64 PR #2292 head passed in 474.738 seconds, then
exhausted 600 seconds on its unchanged retry. Fifteen minutes provides 50%
headroom beyond that exhausted budget and 90% beyond the passing sample.
Separating Hub from other package processes prevents their concurrent race
work from consuming its package budget; two test slots bound fixture
contention on constrained runners. `HUB_RACE_TIMEOUT` and
`HUB_RACE_PARALLEL` are explicit Make overrides for diagnostic experiments.

## Evidence and interpretation

CI uploads `hub-race-evidence` for 14 days even when the race step fails.
Locally the same evidence lives in `tmp/hub-race-evidence`:

- `combined.jsonl` and `internal__hubserver.jsonl` preserve Go's timestamped
  run, pause, cont, pass, fail, skip, output, and package events, including
  the goroutine dump emitted by Go's package timeout.
- `summary.json` records the package result and budget, race mode, and each
  test's outcome, start/last lifecycle progress, Go-reported elapsed time,
  wall time, and pause-to-cont queue time. An unfinished test remains in
  its last lifecycle state; its observed wall/queue time ends at package exit.
- `hub_fixture_open_seconds` logs measure the complete `openTestService`
  and `openHostedStorage` helper calls, including migration and service
  setup. The summary aggregates their count and elapsed time by test.
  Direct database/service opens outside those helpers are not included.

Queue time includes waiting for a parent to release its parallel children.
Go-reported elapsed time excludes a test's own pause, but parent elapsed
can include waiting for children. Parent and child times overlap, as do
concurrent fixture opens: do not sum these as package wall time. Go JSON
timestamps are observations at the output collector, not a scheduler trace.
Progress output and a timeout classification alone do not establish a deadlock.

For a package timeout, inspect recent completed tests and resume events,
then the active goroutine stacks. Recent fixture starts, changing completed
tests, runnable migration/SQLite stacks, and long parallel queues support
cumulative exhaustion. A test remaining active without completions needs
its own stack and lifecycle investigation; increasing this ceiling does not
resolve a stuck test. Preserve the raw evidence rather than automatically
classifying a package timeout as a deadlock.

## Issue #2307 measurements

The archived failure at PR head `3ac09f851bfc1c491be4c94e7ab32e7c07275f2b`
had four active subtests aged 0–5 seconds when the package reached 600 seconds.
Their stacks included Goose SQL parsing and SQLite schema creation; other
subtests waited in `testing.(*testState).waitParallel`. The same head's
earlier ordinary package tests took 33.271/41.658 seconds.

The local baseline uses Go 1.26.6, Linux/arm64, a Docker CPU quota of two,
`GOMAXPROCS=2`, 6 GiB memory, and a container-local executable tmpfs for
synthetic test fixtures. It is constrained Linux evidence, not an amd64
hosted-runner speed equivalence. The baseline at `7c5396d9` passed all
1,177 selected tests/subtests (three existing opt-in preview skips) in
204.658 seconds. A CPU profile attributed 159.83 of 329.09 sampled CPU
seconds (48.57%) to stacks through `hubserver.runMigrations`.

The first instrumented repeat passed in 209.454 seconds with 467 measured
fixture opens totaling 308.589 overlapping seconds, a maximum queue of
118.770 seconds, and a longest Go-reported test elapsed time of 5.68 seconds.
Repeated validation and the exact recorded-head pilot measurements are
recorded in `.detent/validation/2307/README.md`.

To reproduce independent samples without Go's test cache:

```sh
GOTOOLCHAIN=go1.26.6 GOMAXPROCS=2 go run ./tools/testgate \
  -race -parallel 2 -timeout 15m -output tmp/hub-race-sample-1 ./internal/hubserver
GOTOOLCHAIN=go1.26.6 GOMAXPROCS=2 go run ./tools/testgate \
  -race -parallel 2 -timeout 15m -output tmp/hub-race-sample-2 ./internal/hubserver
make check
```

Choose distinct output directories to retain previous samples. Keep the
full package selection when measuring the budget. The 100-job pilot resides
on still-open PR #2292, so its recorded head is validated separately in an
isolated checkout; this change neither imports nor reduces that workload.
