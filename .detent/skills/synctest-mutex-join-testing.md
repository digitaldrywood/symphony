---
name: synctest-mutex-join-testing
description: Test cancellation and retirement of a writer without deadlocking the virtual-time harness on mutex contention.
when_to_use: Use when a testing/synctest test waits for quiescence while a shutdown or retirement goroutine is blocked acquiring a mutex held by a deliberately stalled operation.
---

- Inspect the isolated test's goroutine dump. A test in `synctest.Wait`, a fake operation blocked on a channel, and a retirement goroutine in `sync.Mutex.Lock` identify a harness cycle: mutex contention is not a durable block that lets synctest declare quiescence.
- Keep channel barriers at the actual external operation. Wait for its entry, inspect the observable state, then release it or cancel its context before joining retirement.
- Do not call `synctest.Wait` while intentionally retaining that mutex cycle. Do not replace it with sleeps or change production locking solely to accommodate virtual time.
- For a negative join assertion, check that retirement has not completed while the external operation is held; then release or cancel the operation and await both its result and retirement's completion.
- Verify successful completion and cancellation as separate cases, and run the focused cases under the race detector.
