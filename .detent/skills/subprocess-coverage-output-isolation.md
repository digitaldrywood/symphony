---
name: subprocess-coverage-output-isolation
description: Diagnose coverage runtime errors contaminating structured output from parallel Go test helper subprocesses.
when_to_use: Use when a helper reexecutes a Go test binary and its output contract fails only under coverage, especially with metadata rename errors.
---

# Isolate helper subprocess coverage output

- Reproduce with the reported Go toolchain and coverage enabled, retaining parallel subtests. Compare repeated coverage runs with ordinary and race runs.
- Inspect that toolchain's `internal/coverage/cfile/hooks.go` and `emit.go`. An instrumented test helper calling `os.Exit` can run coverage exit hooks after printing its result, bypassing normal testing coverage finalization.
- Trace the child's inherited `GOCOVERDIR`. In Go 1.26.6, temporary metadata names combine the binary hash and timestamp without exclusive creation; concurrent helpers sharing a directory can collide and report rename failures on stderr.
- Assign an existing, unique directory to each child before starting it, such as appending `"GOCOVERDIR="+t.TempDir()` to `cmd.Env`. Keep the directory alive until the child exits. Avoid process-wide environment changes in parallel tests.
- Apply isolation to every launch of the same helper, including platform-specific tests. Merely clearing `GOCOVERDIR` can produce a missing-directory warning; discarding stderr can hide other helper failures.
- Keep strict output decoding and argument-boundary assertions. Repeat focused coverage runs and race tests, then run the repository gate. Before isolating coverage data, verify the helper does not exercise production code whose counters must be merged into the parent profile.
