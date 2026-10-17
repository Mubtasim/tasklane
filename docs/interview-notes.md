# Interview notes

Questions about Tasklane's design, with model answers. Grows with every milestone.

## Q1. A worker leases a job with a 30 s visibility timeout, and the job takes 45 s. Nothing crashes. What happens?

At 30 s the lease expires. The server can't tell a slow worker from a dead one, so the reaper makes the job available again and a second worker leases it: the job now runs twice.

- **Heartbeats prevent it** in the normal case: the worker extends its lease while it works.
- **The fencing token protects the queue's state** when heartbeats fail (GC pause, network blip): the second lease gets a new token, so the first worker's late ack is rejected (`409 FAILED_PRECONDITION`) and only the current holder can complete or fail the job.
- **Idempotent workers protect the outside world:** the side effect (the email) may still happen twice, which is inherent to at-least-once delivery. Workers use the job ID as an idempotency key, e.g. with the email provider.

Key words: heartbeat (prevent), fencing token (protect state), idempotent worker (protect side effects).

## Q2. A worker crashes right after leasing a job. The visibility timeout is 30 s and the reaper runs every 5 s. How long until another worker can pick the job up, and how would you make that faster?

Worst case is about the visibility timeout plus the reaper interval (≈ 30 + 5 s), plus the first retry's backoff (up to 2 s), because nobody may touch the job until its lease expires. Adding workers doesn't help: the job is waiting on a timer, not on capacity.

To make crash recovery faster:
- shorten the visibility timeout and have workers send heartbeats, so a healthy slow job keeps its lease while a crashed worker's lease expires within seconds;
- shorten the reaper interval (cheap, thanks to the `jobs_leased` index, but a small gain);
- nack in-flight jobs on graceful shutdown, so deploys don't wait for expiry.

Trade-off: shorter leases mean more heartbeat writes to the database.

Lesson: find what the job is *waiting on* before scaling anything.

## Q3. Why does leasing use `SELECT … FOR UPDATE SKIP LOCKED`? What goes wrong with plain `FOR UPDATE`, and with no locking?

- **No locking:** two workers read the same available job at the same time and both lease it, so the job runs twice.
- **Plain `FOR UPDATE`:** no duplicates, but every worker targets the same top row, so they block behind each other. Throughput collapses to one worker's worth (lock contention).
- **`FOR UPDATE SKIP LOCKED`:** each worker skips rows that others hold and takes the next one, so leasing is both exclusive and parallel. After locking, Postgres re-checks the `WHERE` clause on the latest row version (EvalPlanQual), so a job another worker just leased is never picked up in a stale state.

The reaper uses the same trick, so every server can run it at once without coordination or leader election.

## Q4. Retry delays are computed in Go but applied in SQL as `now() + interval`. Why not store `time.Now().Add(delay)` from Go?

Server clocks drift and can jump (NTP corrections, paused VMs). If one server writes a lease expiry with its clock and another server's reaper checks it with a different clock, a skewed clock can expire healthy leases early (duplicate runs) or late (slow crash recovery). So every absolute timestamp comes from one clock, the database's `now()`.

Durations are clock-independent, so the backoff policy (exponential growth, jitter, an injectable random source for tests) stays in Go and becomes a timestamp only inside SQL. Related ideas: "there is no global clock"; Spanner's TrueTime; Raft uses logical terms instead of wall clocks for correctness.

## Q5. Two producers enqueue with the same idempotency key at the same moment. Walk through what Postgres does.

1. A's `INSERT` writes the row and a unique-index entry on `(queue, idempotency_key)`, not yet committed.
2. B's `INSERT` hits A's uncommitted index entry and **blocks** until A's transaction ends.
3. If A commits, B's `ON CONFLICT … DO NOTHING` returns no row. B then runs a new `SELECT`, which under READ COMMITTED takes a fresh snapshot, sees A's committed row and returns the same ID with `created = false`.
4. If A rolls back, B's insert simply succeeds and B creates the job.

Either way exactly one job exists. This must be enforced by the database's unique index: a "check, then insert" in application code has a race window.

## Q6. Tell me about a bug you prevented or fixed.

**The zero-value priority bug.** Priorities were planned as 0 = critical … 3 = low. In Go an unset `int` is 0, so every job enqueued without a priority would silently have become critical. A test-first test ("an unset priority gets the default") caught it before any code shipped. The fix follows the protobuf convention: `PRIORITY_UNSPECIFIED = 0`, stored as default, so the zero value means "not set" in Go and in the proto3 API. Lesson: design types so the zero value is safe.

**The connection-pool deadlock.** When a worker nacked with a stale token, Nack held a pooled connection in its transaction and then asked the pool for a second connection to check whether the job existed. With a pool of 4 connections, 4 concurrent stale nacks each held one and waited for another, and the whole server froze. A regression test reproduced it (2 connections, 8 concurrent stale nacks, all timing out) before the fix: run the existence check on the transaction's own connection. Lesson: never acquire a second pooled connection while holding one.
