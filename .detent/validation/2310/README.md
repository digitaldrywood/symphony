# Browser and snapshot budget measurements

Measured on 2026-09-08 from completed GitHub-hosted `ubuntu-latest` jobs,
image `ubuntu-24.04` version `20260831.293.1`, Linux/x64, Go 1.26.6.
[measurements.json](measurements.json) retains job IDs, source heads, URLs,
step timestamps, conclusions, cache keys, and selected timestamped log lines.
Job durations exclude queue time; step durations use the Actions jobs API.
Sub-step durations use adjacent timestamped log boundaries, rounded to seconds.
Small setup/teardown gaps mean the rounded columns need not sum to job time.

## Repeated full-check evidence

| Stage | [Run 34195459084](https://github.com/digitaldrywood/detent/actions/runs/34195459084) | [Run 34196736772](https://github.com/digitaldrywood/detent/actions/runs/34196736772) |
| --- | ---: | ---: |
| Browser whole job | 11m23s | 10m49s |
| Checkout / setup-go / setup-node | 4s / 16s / 3s | 3s / 16s / 2s |
| npm ci / Chromium and OS dependencies | 1s / 23s | 1s / 26s |
| sqlc installation | 62s | 56s |
| make build: generation / binary compilation | 25s / 19s | 23s / 18s |
| Full Playwright step, including fixture processes | 8m41s | 8m17s |
| Browser evidence upload | 3s | 1s |
| Snapshot whole job | 10m3s | 10m39s |
| Checkout / setup-go / minisign installation | 4s / 12s / 10s | 3s / 17s / 10s |
| Snapshot action, including tool download | 9m32s | 10m3s |
| Hook: module download | <1s | <1s |
| Hook: sqlc installation | 59s | 61s |
| Hook: generation | 26s | 27s |
| Hook: Go test suite, including compilation | 2m29s | 2m44s |
| Six-target binary compilation | 5m10s | 5m22s |
| Archives, Linux packages, checksums, signing | 27s | 27s |

Both named jobs passed in both runs. The first overall workflow was later
cancelled for a newer PR push; its completed jobs remain usable evidence.
Both browser runs selected the same 207 cases: 175 passed and 32 existing
conditional skips. Neither had failed cases or retries. No test selection,
skip, worker count, fixture lifetime, or assertion is changed by this issue.
Snapshot logs confirm all six Darwin/Linux/Windows amd64/arm64 binaries,
archives, deb/rpm packages, SHA-256 checksums, and minisign signatures.
Snapshot mode retains GoReleaser's existing non-publishing behavior.

## Cache state and smoke comparison

All four full jobs restored the same exact setup-go primary key (329 MiB).
Both browser jobs also restored the same npm key (10 MiB). Chromium was
downloaded on each runner; no Playwright browser or sqlc executable cache is
configured. An exact setup-go hit does not save newly compiled entries at
job completion: both logs explicitly say the primary key was hit and the
cache was not saved. A cache hit therefore does not mean the generator,
test, or cross-compilation work is warm. The repeated sqlc installs and
five-minute cross-build stages are direct observations, not evidence that
cache restoration failed. No cold-cache or newly populated per-job cache
experiment was performed; these samples cannot quantify either effect.

The [non-visual PR run 34191891374](https://github.com/digitaldrywood/detent/actions/runs/34191891374/job/101951468437)
used the same runner image and exact Go cache key. Its Browser Visual job
passed in 48s: checkout 3s, Go setup 17s, selection 1s, binary build plus
`--help` 21s. Node, generation, Chromium, and Playwright were not selected.
This is a real binary smoke but provides no browser assertion coverage.
It explains why a smoke-derived budget cannot describe full selection.
The PR heads differ; this is a workload comparison, not a controlled speedup.

## Repeated work assessment and budget decision

The application binary is already built once and reused through
`DETENT_BINARY`. `artifacts.spec.js` and `change-review.spec.js` invoke
different `internal/web` Go test fixtures; `hosted-work.spec.js` invokes an
`internal/hubserver` fixture. Go's build cache can reuse compilation, but
`-count=1` is necessary to execute these server fixtures. Reusing one running
fixture would combine distinct routes, authorization state, and shutdown
contracts. The current logs include fixture costs inside Playwright time;
they do not isolate compiler CPU from server startup. Precompiling fixtures
would move that cost to setup, not establish a net saving. No fixture is
removed or shared on this evidence.

Browser and release generation run on separate runners and feed different
binaries. Snapshot's Go tests validate the freshly generated release inputs;
dropping that hook because Verify also tests Go would change the release
contract. Six cross-compiled targets have different GOOS/GOARCH inputs and
cannot be replaced with the browser binary. Generation, hooks, builds,
packaging, and signing therefore remain intact. Dedicated generator/build
caches are a possible future optimization, requiring measured cache-miss
and cache-hit runs; this change makes no unmeasured speedup claim.

Both documented budgets and workflow timeouts are now 15m. This gives 3m37s
above the largest measured browser job and 4m21s above the largest snapshot
job, including room for setup variation and browser retries. These are
whole-job ceilings, not cold-cache guarantees. Future drift should be
assessed from repeated full jobs with runner image, cache keys, selection,
and per-stage timings, rather than from smoke or queue time. Required check
names, selection logic, artifact handling, and release work are unchanged.
