---
name: synctest-context-ownership
description: Diagnose virtual-time tests that hang while selecting between a timer and cancellation from an outer context.
when_to_use: Use when a testing/synctest retry or timeout test stops advancing fake time and a goroutine dump shows a select without the durable-blocking annotation.
---

# Keep cancellation inside the virtual-time scope

- Capture the isolated test process's goroutine dump and locate the timer/cancellation select. A plain `select` state inside a synctest bubble, while the test runner waits in `synctest.Run (durable)`, indicates a wait that the fake clock cannot treat as durably blocked.
- Check where every selected channel was created, including the context's `Done` channel. An outer test's `t.Context()` can leave the select dependent on a channel outside the bubble and prevent fake time from advancing.
- Use the inner `*testing.T` passed to `synctest.Test` to create `t.Context()` and any timeout or cancellation context consumed by the timer loop. Keep the application's cancellation behavior intact.
- Keep external fixtures such as SQLite stores under their owning test's cleanup. Pass a context created inside the bubble to the operation whose timers are being tested; do not assume the fixture's setup context is suitable for that operation.
- Verify retry duration and cancellation with fake time, then repeat focused race tests. Test the real persisted state separately so the virtual-time harness does not replace the storage contract.
