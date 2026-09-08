---
name: fifo-file-lock-admission
description: Add fair admission to a cross-process lock without weakening its exclusion or stale-owner recovery.
when_to_use: Use when periodic nonblocking lock retries allow new processes to overtake older live waiters across worktrees.
---

- Reproduce overtaking by pausing an older contender after its waiting handshake,
  releasing the holder, and starting new contenders before resuming the older one.
  New contenders must acknowledge queueing; a successful acquisition at that point
  is the regression. Avoid scheduler sleeps as ordering assertions.
- Keep the existing exclusive lock as the execution authority. Introduce a short
  metadata mutex for FIFO ticket assignment and a distinct OS lock per live waiter.
  Derive ordering from serialized numeric tickets rather than wall-clock times.
- Serialize publication, stale inspection, pruning, and ticket reuse under the
  same mutex. Never unlink a live lock or the shared metadata mutex: replacing its
  inode can split ownership across processes. Do not infer death from ticket age.
- Account for unlock-before-close windows: Windows can reject deleting an
  unlocked file whose handle has not closed yet. Bound deletion retries, retain
  cancellation, and surface persistent errors instead of deleting live entries.
- Close a canceled waiter's liveness lock without needing to regain the metadata
  mutex. Later operations can prune it. Bound both admission count and waiting;
  keep the wait deadline separate from the active command lifetime.
- Test crash recovery with helper subprocesses that acknowledge acquiring their
  OS lock, then exit on an explicit pipe handshake without application cleanup.
  Cover registration crashes, queued crashes, live holders, and owner handoff.
- Give each instrumented helper a unique existing GOCOVERDIR. Use fake time for
  deadline policy and generous real deadlines only to detect stuck OS handshakes.
  Run focused race tests, Windows compilation/runtime tests, and the full gate.
- State the rollout boundary: older clients retain exclusion on the original
  lock but cannot participate in the new fairness protocol.
