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

## Q7. Why does `Enqueue` call `pg_notify` inside its transaction, rather than before commit or from Go after commit?

Postgres holds a notification sent inside a transaction until commit, and drops it on rollback, so the job and its wake-up are atomic:
- **Inside the transaction (what we do):** a woken lease always finds the row, because it is already visible; a rollback (for example an idempotency conflict) sends nothing.
- **Before commit:** the waiter wakes, sees nothing (the row isn't visible yet) and goes back to sleep. That is a spurious wake-up followed by a lost one, and a rolled-back enqueue would ring the bell for a job that never existed.
- **From Go after commit:** this is a dual write. If the process dies between the commit and the notify, the wake-up is lost and the job waits until something else wakes the worker.

Related pattern: the transactional outbox (put the message in the same transaction as the data change).

## Q8. LISTEN/NOTIFY already wakes waiting leases. Why does a waiting lease still re-check every second?

- Delayed jobs and retries become runnable because time passes, with no enqueue and therefore no notification.
- Notifications are not queued for a disconnected listener, so anything sent while the LISTEN connection is down is lost. (Tasklane wakes all waiters once LISTEN is re-established, and pings after 30 s of silence to detect half-open connections, but the poll is the safety net.)

A notification is a hint; the database is the truth. **Notifications give speed, polling gives correctness.** Cost: one cheap indexed query per waiter per second (a load-test item along with the thundering herd when one NOTIFY wakes every waiter).

## Q9. Why must `Ack` be idempotent, and why is "same token on a completed job → success, any other token → 409" safe?

Networks lose responses, not just requests. If an ack succeeds but the response is lost, the worker retries. Without idempotency the retry gets 409, and the worker can't tell "my work counted" from "someone else took my job": it might alert, nack or redo work for a job that already succeeded.

It is safe because success requires the exact token that completed the job:
- Tokens are random UUIDs, issued per lease and only in that lease's response (never by `GetJob`), so holding one is proof of being the lease holder.
- If the lease expired and the job was re-leased, it got a new token. A stale worker's retry no longer matches, so it gets 409, and the completed row holds the new worker's token.
- If the lease expired but nobody re-leased the job, the stale worker's token is still current, and its ack succeeds. That is correct: it really did the work.

Nack is different: a repeated nack is rejected harmlessly, because the job is already scheduled for retry.

## Q10. Tell me about a bug where the database and the network disagreed.

**Stranded leases.** Lease marks jobs leased and commits, then sends them back. With 1 MB payloads, a 5-job lease produced a 5 MB response, over the client's 4 MB gRPC limit: the commit succeeded but the delivery failed. The jobs stayed leased to nobody, their attempts were burned, and the error surfaced as HTTP 429, which told clients to retry and strand more batches.

The fix was to make an undeliverable response impossible: a per-job payload cap (256 KiB), explicit matching message limits on both sides (16 MiB), and a cap of 50 jobs per lease, so the largest legal lease always fits. Size errors return 400/413, not a retryable 429.

A follow-up review found the cap measured JSON bytes while the wire carries protobuf `Struct`, which is about 3.7× larger for number-heavy payloads, so it was moved to protobuf bytes. Lesson: measure a limit in the units it is enforced in. Recovery still exists in the worst case: stranded jobs return after the visibility timeout.

## Q11. Your worker runs 20 handlers. Why does it hold one long-poll instead of 20, and what does that save, idle and busy? Numbers for 10 workers.

Not connections: gRPC multiplexes every call from a process over one HTTP/2 connection either way. The difference is **parked Lease requests**, and each one costs the server work.

- **Idle (empty queue):** a parked lease re-checks the database every second (Q8). One lease per handler is 10 × 20 = 200 parked leases, so about **200 queries/s** against Postgres that find nothing; one per process is **10 queries/s**. Each long-poll also times out after 20 s and is re-issued: 200 / 20 = 10 RPCs/s against 10 / 20 = 0.5/s. Plus 200 server goroutines and timers instead of 10.
- **One job arrives:** the NOTIFY wakes every waiter on the queue. 200 wake-ups mean 200 `SKIP LOCKED` queries for one row (one wins, 199 return empty and park again): a thundering herd. With 10, it's 10 queries.
- **Busy (queue full):** one lease takes up to k free slots in **one transaction** (`LIMIT k`), so filling 20 slots is 1 query and 1 RPC, not 20. Fewer transactions also means less contention on the rows at the head of the queue index.

Workers never query Postgres themselves: the waiting lease is held inside the server, and the server runs the query and answers the open request.

Cost of the choice: the lease loop must hand back the k − m slots it didn't fill (see Q15), and one slow handler doesn't delay leasing, because the loop runs separately from the handlers.

## Q12. A heartbeat comes back "lease lost". What does the SDK do, step by step, and why doesn't that make duplicate execution impossible?

"Lease lost" means the heartbeat failed with `FAILED_PRECONDITION` (the token no longer matches) or `NOT_FOUND`. `UNAVAILABLE` and other transient errors are not lost leases: the heartbeat simply tries again at the next tick (every vt/3) until the handler returns or the server reports the lease lost. The local lease deadline only bounds the retries of the final Ack or Nack.

1. **Mark the job lost** (`lost = true`) and stop heartbeating it.
2. **Cancel the handler's context.** Cancellation is cooperative: the handler stops only if it checks `ctx`.
3. **Send no Ack and no Nack.** The job may already belong to another worker under a new token; either call would be rejected anyway, and a Nack must not affect someone else's attempt. The new holder decides the outcome.
4. **Free the slot when the handler returns** (not when the cancel is sent). A handler that ignores `ctx` keeps its slot until it finishes.
5. Log it (`lease lost; cancelling the handler`).

**Why duplicates are still possible:**
- **Late detection:** the lease expires in the database, but the worker finds out only at its next heartbeat, up to vt/3 later (5 s in the demo). Longer if the worker was frozen. During that window both workers may run the job.
- **Side effects can't be undone:** the handler may already have sent the email or charged the card before the cancel arrives, or even finished and had its Ack rejected. The next worker runs it again.
- **Cancellation is a request, not a kill:** a handler that doesn't check `ctx` runs to completion.

This is at-least-once delivery by design. Exactly-once *effects* come from the handler: use the job ID (or the idempotency key) as a dedupe key at the side effect (an email provider's idempotency key, a unique constraint written in the same transaction as the business change), or pass the lease token as a **fencing token** so downstream storage rejects writes from a stale holder.

## Q13. Kubernetes stops a worker with SIGTERM while a 2-minute job runs. What happens second by second until the job runs again, with and without the shutdown Nack? What if SIGKILL arrives first?

On SIGTERM the worker stops leasing, lets running handlers continue and keeps heartbeating them; at `ShutdownTimeout` (25 s) it cancels what's still running, nacks it with "worker shutting down" and exits. Those Nacks and any Ack still in flight share one 2 s grace period, so the worker exits by t≈27 at the latest. Assume a 15 s visibility timeout and Kubernetes' default 30 s grace period.

- **With the Nack:** t=0 SIGTERM, job keeps running, heartbeats every 5 s. t=25 handler cancelled, Nack sent; the job is scheduled after its backoff (~1 s + jitter). t≈27 another worker leases it (within the 1 s re-check). The pod exits before SIGKILL at t=30.
- **Without the Nack:** the last heartbeat (~t=25) pushed the lease to ~t=40. The process exits; the job sits leased to nobody until it expires, then the reaper and backoff add a few seconds: runs again at ~t=42–45. About **one visibility timeout later**.
- **SIGKILL first** (grace period shorter than `ShutdownTimeout`, e.g. Docker's default 10 s): killed at t=10 with no Nack; the lease (last extended at ≤t=10) expires by t≈25, the reaper returns it, runs again at ~t=27–29 with `last_error: lease expired`. Same path as a crash.

Takeaways:
- The job restarts **from zero** in every case and uses one attempt (attempts are counted at lease). Long jobs need idempotent handlers and, ideally, checkpoints.
- The Nack is an optimisation for **speed**, not correctness: lease expiry guarantees recovery even under SIGKILL.
- Align timeouts across layers: `terminationGracePeriodSeconds` (or compose `stop_grace_period`) > `ShutdownTimeout` + Nack/in-flight Ack time (30 s > 25 s + ~2 s).

## Q14. Why does `GetQueue` count completed jobs only over the last 10 seconds, what makes that cheap, what breaks if you count all of them, and why is a counters table no better?

- **Why a window:** the live view wants a **rate** (`completed/s` = count over 10 s ÷ 10), not a lifetime total. A total also stops meaning anything once completed jobs are pruned.
- **Why it's cheap:** the B-tree index on `finished_at` lets Postgres jump straight to `now() - 10 s` and read only the rows after it: about 100 entries at 10 jobs/s, however big the table gets. The cost depends on the window, not the table size.
- **Counting everything:** Postgres keeps no stored row count, because under MVCC each transaction can see a different set of rows, so `COUNT(*)` has to visit every row (or index entry) and check visibility. With millions of completed jobs, every dashboard refresh (once a second per viewer) becomes a scan that grows forever.
- **Counters table:** every Ack would run `UPDATE counters SET n = n + 1` on **the same row**. That row's lock is held until commit, so all Acks from all workers serialise on one row (a **hot row**), and every update leaves a dead row version behind for vacuum. You trade a slow read for a slow, contended write path, which is worse because Acks are the hot path. The usual fix, sharded counters (N rows, add to a random one, sum on read), adds complexity a live view doesn't need. Staleness is not the problem: every count is a snapshot anyway.

## Q15. The lease loop gives back the k − m unused slots after each lease. What happens over time if that line is missing, how would you notice in production, and which test catches it?

The loop takes every free slot (k), leases `max_jobs = k` and gets m ≤ k jobs. Each of the m jobs frees its own slot when it finishes. The other **k − m slots got no job, so nothing would ever free them**: that is the leak.

- **How fast:** an empty poll has m = 0, so the **first empty poll leaks every free slot**. On a quiet queue the worker is dead after one 20 s long-poll. On a busy queue m is usually k, so it leaks a little on each partial lease: capacity drops gradually, then reaches zero.
- **What it looks like:** the process is alive, logs no errors, passes health checks and makes no lease calls. Every worker has the same bug, so the fleet stops. `available` grows without bound; the jobs are never leased, so attempts never increase and **nothing reaches the DLQ**. No retry or alert path fires on its own.
- **How you'd notice:** queue depth (`available`) rising while `leased` ≈ 0 and worker CPU is idle. That's distinctive: a real capacity problem shows `leased` at the maximum. Also, per worker, the lease call rate drops to zero and in-flight jobs sit below concurrency while work is waiting. Alert on "oldest available job age" (or depth growing with low `leased`).
- **The test:** `TestSlotsComeBackAfterEmptyLeases` (sdk/go/tasklane/worker_test.go). With concurrency 2 against an empty fake server it waits for 3 lease calls (two empty polls), then pushes a job and expects it acked and the next lease to ask for `max_jobs = 2`. Without the give-back the loop blocks after the first empty poll, so the third lease call never happens and the test times out. A unit test with a fake server is enough; no real server is needed.
