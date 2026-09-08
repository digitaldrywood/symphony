---
name: shared-registry-fixture-isolation
description: Diagnose repeated HTTP fixture failures caused by process-wide backoff or cache registries.
when_to_use: Use when Go tests pass alone but repeated runs skip fake HTTP handlers or inherit synthetic rate-limit responses.
---

# Isolate registry lifetime from fixture lifetime

- Reproduce with the smallest focused `go test -count=2` command before changing fixtures. Count repetitions share a process and package globals.
- Trace synthetic responses back to their registry and exact key. A new client or closed HTTP server does not imply a new registry; an ephemeral port can be reused while an old entry remains active.
- Recreate the failure deterministically with fake HTTP clients using the same endpoint and credential. Seed a backoff through the real request path, then construct a new client with a healthy transport and assert whether that transport is reached.
- Cover shared registry, private registry, different endpoint, and different credential in a table. Avoid waiting for the OS to reuse a port or for real backoff timers to expire.
- Isolate fixtures that produce persistent state using an existing instance registry seam. Do not reset package globals while parallel tests can use them, disable production brakes, or retry failing assertions.
- Preserve a test of default cross-client sharing with a unique per-invocation credential; a test name alone repeats under `-count`.
- Run repeated race tests for the whole affected package and the repository validation gate.
