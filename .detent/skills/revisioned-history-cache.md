---
name: revisioned-history-cache
description: Separate expensive historical analytics from live operational snapshots using durable revisions and bounded window freshness.
when_to_use: Use when heartbeat-driven dashboard enrichment repeatedly scans unchanged history and history also has correction or transactional write paths.
---

Trace every writer before choosing invalidation. A counter in the primary insert
method misses direct updates, deletes, and other transactions. A singleton
revision maintained by database triggers covers committed mutations, including
corrections that preserve row count and timestamp extrema; rollback must restore
the revision too.

Cache immutable historical results by scope and revision. Bound freshness by
both wall time and the report window endpoint, and retain the actual computed
window timestamps. Recompute operational blockers, running status, and lane ages
from the current snapshot. Check outer snapshot caches as well: an unchanged
snapshot must not mask a changed history revision indefinitely.

Coalesce concurrent loads, allow waiting requests to cancel, and do not retain
failed or canceled results. Compare revisions before and after a multi-query
load so a mutation during reporting forces another refresh. Bound scope entries
and keep superseded queued snapshots from triggering historical work.

For identity correlation, preserve direct matching semantics. Union candidate
positions from project-scoped identity indexes and deduplicate positions; do not
use transitive connected components when the original predicate is pairwise.
Keep the old implementation as a test oracle for alias collisions, overlapping
intervals, and stable representative ordering.

Benchmark cold history and heartbeat cadence separately, with six or more runs.
Advance an injected clock on every simulated heartbeat: a frozen clock or a
repeating window can accidentally measure permanent cache hits. Include periodic
refreshes in the cadence comparison. Profile fixture setup separately or identify
its frames explicitly; database seeding can dominate whole-test CPU profiles.
