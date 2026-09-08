# Workflow history performance — issue 2326

Measured on 2026-09-08, Go 1.26.6, Darwin arm64, Apple M4 Max.
Baseline: `6d3a5eabbe0c46d6cf65fce91c223b8ea68626ad` plus the benchmark fixtures in this change.
Each comparison uses six repetitions and `benchstat`; all reported time
improvements have p=0.002. These are local production-like fixtures, not a
measurement of the live Linux daemon, which was left running unchanged.

## Workload and results

The report fixture contains 500 or 2,000 issues across four projects and 55 days,
with two lane visits and six overlapping active intervals per issue. The SQLite
fixture uses the same 16,000 events. The dashboard fixture uses 8,000 events for
2,000 issues, querying all six current/previous windows. It advances an injected
wall clock by one second per heartbeat and repeats a 30-second observation range
to keep the history population stable while including periodic refreshes.
The baseline ignores the injected clock; the snapshot endpoints are identical.

| Benchmark | Before median | After median | Reduction |
| --- | ---: | ---: | ---: |
| Report, 500 issues | 65.373 ms | 3.480 ms | 94.68% |
| Report, 2,000 issues | 1,003.08 ms | 16.78 ms | 98.33% |
| SQLite report, 2,000 issues | 1,129.1 ms | 112.5 ms | 90.04% |
| Dashboard heartbeat including cadence | 201.717 ms | 2.585 ms | 98.72% |

The SQLite after timings varied substantially (78% interval); every after sample
remained below every before sample. Heartbeat before timings varied by 37%; after
by 7%. The index slightly reduces total allocated bytes (4.32% for 2,000 issues)
but increases small allocation counts (85.3% for in-memory reports, 4.4% for
SQLite reports). Across heartbeat cadence, allocated bytes fall 96.73% and
allocation counts fall 96.54%.

The query-count regressions verify one six-report batch shared by three real SSE
clients, no new batch on a heartbeat, and a new batch on a history revision or
30-second window expiry. With unchanged history at one heartbeat per second,
historical row scans and runtime table-count batches fall from 30 to one per
30 ticks (96.7%). Tiny revision-row reads remain. This is query-work evidence;
Linux read-syscall or logical-byte counters were not measured on this Mac.

## CPU profiles

Before/after CPU profiles and their test binaries were captured with
`go test -cpuprofile` in the Detent-provided temporary directory, under `perf/`.
Fixtures are populated before `b.Loop`, so allocation/timing measurements exclude
setup. Whole-test CPU profiles include fixture population: inspect the
`snapshotWorkflowMetrics` subtree to distinguish reporting from SQLite seeding.
Adaptive benchmark iteration counts differ, so total sample percentages are not
a per-operation performance comparison.

In the baseline dashboard subtree, workflow reports accumulated 3.75 CPU seconds:
`workflowLaneFlows` accounted for 3.02 seconds cumulatively, identity comparison
0.99 seconds, and `strings.TrimSpace` 0.61 seconds flat. In the cadence profile,
the dashboard subtree accumulated 2.48 CPU seconds across many more operations;
SQLite stepping accounts for 1.81 seconds and the broad pairwise identity scan is
absent. The before in-memory profile similarly identifies lane flows as its
largest application hotspot (45.98% cumulative). The six-run timings above
quantify the per-operation reduction.

## Freshness and correctness contract

History windows and runtime table counts are cached for at most 30 seconds,
measured against both wall time and the observation endpoint. Returned window
bounds retain the actual report timestamp. SQLite insert/update/delete triggers
advance a durable revision even when corrections preserve counts and timestamps.
The revision is checked before and after loading. Failed/canceled loads and loads
that span a revision change are not retained. Backends without the optional
revision reader use bounded freshness. Cache scope is the normalized project ID;
the empty ID represents fleet scope, with at most 16 retained entries.

Running status, blockers, gates, lane ages, and active bottlenecks are still
enriched from each current snapshot. The outer enrichment cache also observes
history revision/freshness and coalesces superseded queued snapshots. Identity
correlation preserves direct, project-isolated ID/identifier/URL/PR matches,
interval unions, ordering ties, and representative fallback behavior. Randomized
regressions compare both changed correlation outputs to the original functions.

## Reproduction

Run the benchmark files against the baseline production code and the changed
code, retaining separate output files and binaries. Use an isolated checkout in
`TMPDIR` for the baseline. Do not modify or restart a live daemon.

```sh
go test ./internal/store -run '^$' -bench 'Benchmark(WorkflowHistory|SQLiteWorkflowHistory)$' -benchmem -count=6 -cpuprofile="$TMPDIR/store.cpu" -o "$TMPDIR/store.test"
go test ./internal/web -run '^$' -bench '^BenchmarkWorkflowSnapshotHeartbeat$' -benchmem -count=6 -cpuprofile="$TMPDIR/web.cpu" -o "$TMPDIR/web.test"
go tool pprof -top -cum -focus=snapshotWorkflowMetrics "$TMPDIR/web.test" "$TMPDIR/web.cpu"
```

Goose migration logs can appear between a benchmark name and its result. Remove
those setup log lines and join the benchmark name to its result before running
`benchstat`. Preserve the six individual samples; do not compare only averages.

## Recorded samples

Times below are ns/op, with bytes and allocations per operation from Go.

### before

```text
goos: darwin
goarch: arm64
pkg: github.com/digitaldrywood/detent/internal/store
cpu: Apple M4 Max
BenchmarkWorkflowHistory/issues_500-16         	      16	  65155133 ns/op	 5159081 B/op	   14288 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	      18	  65599442 ns/op	 5157988 B/op	   14287 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	      16	  65541685 ns/op	 5157897 B/op	   14287 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	      18	  65112139 ns/op	 5157563 B/op	   14287 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	      18	  65203863 ns/op	 5157836 B/op	   14287 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	      18	  65611516 ns/op	 5158476 B/op	   14287 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1001681708 ns/op	21141632 B/op	   56327 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1003134417 ns/op	21141632 B/op	   56327 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1009071709 ns/op	21146984 B/op	   56334 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1001626125 ns/op	21141632 B/op	   56327 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1006637458 ns/op	21141632 B/op	   56327 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	       1	1003032041 ns/op	21146952 B/op	   56333 allocs/op
BenchmarkSQLiteWorkflowHistory-16              	       1	1127621333 ns/op	133269224 B/op	 1092060 allocs/op
BenchmarkSQLiteWorkflowHistory-16 1	1104399583 ns/op	133254464 B/op	 1092052 allocs/op
BenchmarkSQLiteWorkflowHistory-16 1	1118588125 ns/op	133254448 B/op	 1092052 allocs/op
BenchmarkSQLiteWorkflowHistory-16 1	1130671917 ns/op	133246288 B/op	 1092050 allocs/op
BenchmarkSQLiteWorkflowHistory-16 1	1133305417 ns/op	133246288 B/op	 1092050 allocs/op
BenchmarkSQLiteWorkflowHistory-16 1	1136438333 ns/op	133246336 B/op	 1092050 allocs/op
```

### final

```text
goos: darwin
goarch: arm64
pkg: github.com/digitaldrywood/detent/internal/store
cpu: Apple M4 Max
BenchmarkWorkflowHistory/issues_500-16         	     348	   3459779 ns/op	 4932656 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	     351	   3415017 ns/op	 4932650 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	     343	   3728013 ns/op	 4932659 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	     345	   3499843 ns/op	 4932644 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	     346	   3460983 ns/op	 4932662 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_500-16         	     331	   3542378 ns/op	 4932614 B/op	   26308 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      67	  16932214 ns/op	20229332 B/op	  104374 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      66	  16596540 ns/op	20229462 B/op	  104374 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      72	  16712961 ns/op	20229510 B/op	  104374 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      69	  17267289 ns/op	20229253 B/op	  104374 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      68	  16850848 ns/op	20229258 B/op	  104374 allocs/op
BenchmarkWorkflowHistory/issues_2000-16        	      73	  16551247 ns/op	20229355 B/op	  104374 allocs/op
BenchmarkSQLiteWorkflowHistory-16              	       5	 200445775 ns/op	132333684 B/op	 1140096 allocs/op
BenchmarkSQLiteWorkflowHistory-16 10	 107345183 ns/op	132334540 B/op	 1140096 allocs/op
BenchmarkSQLiteWorkflowHistory-16 10	 110051000 ns/op	132334560 B/op	 1140096 allocs/op
BenchmarkSQLiteWorkflowHistory-16 9	 127775824 ns/op	132334296 B/op	 1140096 allocs/op
BenchmarkSQLiteWorkflowHistory-16 10	 107465517 ns/op	132333731 B/op	 1140096 allocs/op
BenchmarkSQLiteWorkflowHistory-16 9	 114957926 ns/op	132333730 B/op	 1140096 allocs/op
```

### heartbeat-before

```text
goos: darwin
goarch: arm64
pkg: github.com/digitaldrywood/detent/internal/web
cpu: Apple M4 Max
BenchmarkWorkflowSnapshotHeartbeat-16    	       5	 200109008 ns/op	72527491 B/op	  528939 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 5	 201197125 ns/op	72525665 B/op	  528937 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 5	 200718058 ns/op	72530796 B/op	  528937 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 5	 202237533 ns/op	72525595 B/op	  528936 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 4	 275541166 ns/op	72525410 B/op	  528935 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 5	 202690025 ns/op	72525499 B/op	  528935 allocs/op
```

### heartbeat-cadence

```text
goos: darwin
goarch: arm64
pkg: github.com/digitaldrywood/detent/internal/web
cpu: Apple M4 Max
BenchmarkWorkflowSnapshotHeartbeat-16    	     448	   2555447 ns/op	 2324564 B/op	   17929 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 504	   2430393 ns/op	 2341426 B/op	   18060 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 422	   2596131 ns/op	 2467601 B/op	   19032 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 394	   2712838 ns/op	 2466797 B/op	   19027 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 411	   2573157 ns/op	 2364795 B/op	   18241 allocs/op
BenchmarkWorkflowSnapshotHeartbeat-16 379	   2770613 ns/op	 2381367 B/op	   18369 allocs/op
```
