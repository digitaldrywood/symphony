# Hub race budget validation

All four complete-package runs used Go 1.26.6 on Linux/arm64, a two-CPU
Docker quota, `GOMAXPROCS=2`, 6 GiB memory, and executable container-local
tmpfs fixtures. The runner used `-race -count=1 -p=1 -parallel=2 -timeout=15m`
without a test-name filter. Only synthetic data and ephemeral listeners were
used. This is constrained Linux evidence; amd64 hosted-runner timing is
separate evidence, not directly interchangeable.

| Source/sample | Package seconds | Passed tests/subtests | Fixture opens | Overlapping fixture seconds | Maximum queue seconds | 100-job active seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Main `7c5396d9`, 1 | 209.454 | 1,177 | 467 | 308.589 | 118.770 | Not present on main |
| Main `7c5396d9`, 2 | 226.833 | 1,177 | 467 | 319.810 | 138.202 | Not present on main |
| PR #2292 `3ac09f85`, 1 | 239.159 | 1,017 | 467 | 319.099 | 144.787 | 19.797 |
| PR #2292 `3ac09f85`, 2 | 244.616 | 1,017 | 467 | 329.008 | 153.319 | 21.415 |

Each run retained the same three existing opt-in browser-preview skips.
No skips were added. Selection hashes normalize randomized synthetic project
and work-item IDs in subtest names, preserve multiplicity, and match between
repeats of the same source. Main has additional tests since the older PR head;
these rows are not a before/after optimization comparison.

PR #2292 was still open during measurement. Its exact recorded revision,
`3ac09f851bfc1c491be4c94e7ab32e7c07275f2b`, was checked out in a disposable
clone under Detent's `TMPDIR`. Only the two shared fixture timing logs were
added. The pilot's test file hash and all four workload counters from both
runs are retained. Both active workloads still have 100 jobs, 630 HTTP
requests, 510 API mutations, and 400 ingested events.

## Retained evidence

- [measurements.json](measurements.json): environment, source revisions,
  package outcomes, test selection hashes, fixture aggregates, completion
  counts in 30-second buckets, raw-event hashes, and complete pilot counters.
- [test-timings.csv](test-timings.csv): all 4,400 selected test/subtest
  observations, including elapsed, wall, queue, fixture cost, start, last
  lifecycle progress, and outcome. Times are rounded to microseconds.
- [baseline-cpu.txt](baseline-cpu.txt): uninstrumented current-main baseline
  CPU profile summary. The package passed in 204.658 seconds; migration stacks
  accounted for 159.83 of 329.09 sampled CPU seconds (48.57%).
- [timeout-stacks.txt](timeout-stacks.txt): archived 600-second CI failure,
  its four active test ages, migration/SQLite stacks, and a parallel-slot waiter.

The current-main repeats report longest test elapsed times of 5.68 and
5.40 seconds. The recorded-head pilot extends individual elapsed time, but
its 100-job active scenario remains only 19.797–21.415 seconds. Repeated
completions and runnable migration stacks support cumulative fixture work
and queueing as the package-budget pressure, not a demonstrated stuck test.
Overlapping fixture/parent/child totals are not package wall time.

The 15-minute budget provides 50% headroom beyond the archived exhausted
10-minute budget, while isolating Hub from other test package processes and
limiting fixture parallelism. Full selection, real migrations, race checking,
workload sizes, assertions, and lifecycle deadlines remain intact. See
[the budget guide](../../../docs/hub-race-budget.md) for commands and diagnostic
interpretation. Current-head CI retains full raw JSON and goroutine evidence
as the `hub-race-evidence` artifact for 14 days.

## Local gate

`GOTOOLCHAIN=go1.26.6 make check` passed on macOS/arm64 after the complete
change and skill draft. Build, migration/generated checks, golangci-lint
(zero issues), vet, NilAway, all race packages, and configured coverage
floors passed. Aggregate coverage was 79.4% against the 70% gate; Hub's
uncached race step took 142.760 seconds. The root token-isolation contract
test now verifies both split race commands. A synthetic integration test
confirms the runner detects a real fixture data race and retains evidence.
