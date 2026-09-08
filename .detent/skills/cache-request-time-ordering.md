---
name: cache-request-time-ordering
description: Diagnose duplicate cache loads caused by request timestamps arriving out of order.
when_to_use: Concurrent callers unexpectedly reload a fresh cache whose freshness predicate rejects negative ages.
---

Trace where the request timestamp is sampled relative to lock acquisition and
single-flight waiting. A caller sampled earlier can acquire the lock after a
later caller has populated the cache; this does not establish clock rollback.

Reproduce with an injected clock and channels: suspend the first sample, let a
second caller start loading, then return the earlier sample. Keep the original
load-count assertion and run both normal and forced ordering cases. Avoid sleeps.

For request-relative expiry, compare the sample to the cached expiry deadline;
a result populated after the request began can satisfy that request. Preserve
revision and identity checks. Do not globally relax negative-age checks: report
window endpoints and actual clock rollback may require invalidation independently.
Test revision changes and the exact expiry boundary alongside reversed samples.
