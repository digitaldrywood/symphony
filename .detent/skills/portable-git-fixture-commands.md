---
name: portable-git-fixture-commands
description: Diagnose Windows-only Git fixture failures caused by quotes reaching child commands literally.
when_to_use: Use when a shell-driven Git fixture passes on POSIX but Windows reports a quoted directory argument as invalid.
---

# Portable Git fixture commands

- Check the child command's actual error. Literal surrounding quotes in a rejected `git -C` directory can indicate command-line construction at the `cmd /C` boundary, before the behavior under test executes. A unit test of the constructed argument slice does not prove Windows execution works.
- For large GitHub Actions logs containing Go test JSON, identify events with `Action: fail`, then collect output for their package/test pairs. Searching for words such as `timeout` also matches passing test names and can hide the relevant failure.
- When the test only needs to mutate a generated sibling repository, derive its path with `filepath.Rel` from the command's working directory and normalize separators with `filepath.ToSlash`. An unquoted relative path is appropriate only when its remaining components are controlled fixture names without whitespace or shell metacharacters; a shared temporary-root prefix containing spaces then disappears.
- Keep quoted user-path coverage at the shared command boundary. Do not treat a fixture-relative path as a production quoting fix, strip user quotes, or skip Windows coverage. Track a discovered shared-shell defect separately when it is outside the current task.
- Verify the fixture still performs its intended mutation and that the protected operation rejects or accepts it for the intended reason. Confirm the corrected command through native Windows CI as well as focused local tests.
