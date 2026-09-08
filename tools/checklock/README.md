# Validation queue

`make check` passes the repository's Git common-directory lock to checklock,
so all worktrees share one validation gate. Checklock registers a FIFO ticket
before attempting that existing lock. Once registered, a live waiter cannot be
overtaken by later registrations, including when the head waiter is descheduled.
Concurrent registrations are ordered by acquisition of a short queue mutex.

The queue uses two kinds of OS locks supplied by `internal/instancelock`:

- `<lock>.queue.lock` serializes ticket registration, inspection, and pruning.
  It is released before waiting or running validation and is never removed.
- `<lock>.queue/<ticket>.lock` stays locked while its waiter is queued. Only the
  lowest live ticket may attempt the original validation lock. After acquisition
  or cancellation, its waiter lock closes; the next queue operation prunes the
  released entry. The original validation lock stays held until the command exits.

The queue mutex prevents a ticket from being opened or reused between its
liveness inspection and removal. A crashed process releases its OS locks, making
its tickets recoverable without PID checks, age thresholds, or deleting a live
lock. Pruning retries transient deletion failures up to five times with
cancellation-aware polling, allowing an unlocked Windows file handle to finish
closing. Partial receipts left by a crash are also recoverable. Numeric tickets
avoid wall-clock ordering assumptions. The queue admits up to 1024 live waiters;
excess invocations fail explicitly instead of growing it without a bound.

`-wait-timeout` (15 minutes by default) bounds registration and queue waiting.
Interrupt/termination signals cancel waiting. Once validation starts, this wait
budget does not limit execution. Parent cancellation and termination signals
stop the active command through the shared process-group helper. The wrapper
retains the lock until command exit and process-group cleanup finish. The
validation command controls its own active timeouts.

Waiting diagnostics report queue position, queue size, elapsed wait, and the
active owner's PID/acquisition time when available. They refresh when the queue
or owner changes and at least every 30 seconds while polling the validation
queue. Timeout messages identify waiting before validation has started. Normal
diagnostics omit repository paths and owner hostnames.

Older checklock binaries still contend on the original exclusive validation
lock, so exclusivity is preserved during rollout. FIFO ordering applies to
invocations using this queue protocol; older polling binaries cannot honor it.

Validation:

```sh
go test -race ./tools/checklock ./internal/instancelock -count=10
GOOS=windows GOARCH=amd64 go test -c -o tmp/checklock-windows.test.exe ./tools/checklock
make check
```
