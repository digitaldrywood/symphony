---
name: cumulative-race-suite-budget
description: Separate package-wide race test budget exhaustion from stalled tests using progress, parallel queueing, fixture timing, and goroutine evidence.
when_to_use: Use when an unchanged Go package alternates between passing race tests and hitting its package timeout while many subtests wait for parallel slots.
---

Capture the failing revision, toolchain, runner constraints, complete test
selection, package duration, and timeout goroutine dump before changing a
deadline. Active-test ages describe those tests, not elapsed package work.

Run uncached complete-package samples on constrained Linux with synthetic
fixtures. Retain `go test -json` events and compare run/pause/cont/terminal
timestamps. Report queue time separately from Go's elapsed field; parents,
children, and parallel fixtures overlap and cannot be summed as wall time.

Time shared fixture constructors and take a CPU profile. State which direct
fixture paths are excluded. Correlate recent completions and active stacks:
runnable migrations plus changing tests support cumulative work; one
unchanging active operation needs a separate lifecycle investigation.

Prefer a narrowly scoped package budget and bounded fixture concurrency
when preserving migration/fixture coverage matters. Keep every test,
assertion, workload size, race check, and individual lifecycle deadline.
Use the same command in local validation and CI, and upload raw evidence
on failures so future timeouts remain diagnosable.

Repeat representative samples. If the recorded failure includes tests on an
unmerged branch, validate its exact head in a disposable checkout without
importing its feature changes. Record architecture limits and justify
headroom from observed package work, not the single slowest test.
