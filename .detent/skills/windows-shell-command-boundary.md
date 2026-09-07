---
name: windows-shell-command-boundary
description: Diagnose literal quotes or corrupted arguments at Go's Windows shell boundary.
when_to_use: A configured Windows command works interactively but a Go-launched child receives literal quotes, split paths, expanded percent expressions, or corrupted backslashes.
---

- Trace both parsers: Go normally serializes argv for CommandLineToArgvW, while cmd.exe interprets a script. Inspect the actual child argv rather than only exec.Cmd.Args.
- Reproduce with a real child process. A test helper that emits its argv as JSON exposes empty arguments, quotes, spaces, percent signs, and trailing backslashes. Use Git with a quoted existing directory when the reported symptom involves git -C.
- Keep a legacy-launch control when Windows execution is available only in CI: require the recorded failure from the original boundary and success from the corrected boundary in the same Windows test.
- For cmd.exe, use Windows-specific SysProcAttr.CmdLine with an outer quote pair and /S /C. Preserve the operator's configured script verbatim; do not escape intentional operators or environment expansion.
- Treat appended literal arguments separately. Encode backslashes before quotes and at the end for the child parser, then escape cmd metacharacters. Batch-file escaping rules are not interchangeable with direct /C execution.
- Test configured shell syntax and literal appended arguments independently, including percent expressions that name real environment variables. Test a quoted executable path as well as quoted directory arguments.
- Keep non-cmd shells on their established launch path. Windows PowerShell's legacy native argument parsing differs from modern PowerShell; do not silently redefine that contract while repairing cmd.
- Cross-compilation proves build compatibility only. Require current-head Windows execution evidence before declaring the repair validated.
