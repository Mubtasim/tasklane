# Interview notes

Questions about Tasklane's design, with model answers. Grows with every milestone.

From Q16 on, every answer uses the shape practiced for interviews: say how many parts the question has, then **What** (one line), **How** (numbered steps), **Numbers**, **Fails halfway** (what happens if it breaks in the middle) and **Trade-off**. Numbers come from this repo; where the repo has no measurement, the answer says so.

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
- If the lease expired but the reaper hasn't reclaimed the job yet, the stale worker's token is still current, and its ack succeeds (Ack checks the token, not the expiry time). That is correct: it really did the work. Once the reaper has run, the token is cleared (`lease_token = NULL`), so the late ack gets 409 even if nobody has leased the job again.

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


## Q16. A worker leases a job at the same moment an operator cancels it. What happens, step by step, and why can't both win?

Three parts: the two statements, the interleavings, and why exactly one wins.

**What:** both are single SQL statements that need the row lock, and both re-check `state = 'available'` after getting it, so whoever locks first wins and the other sees the new state.

**How:**
1. `CancelJob` is one conditional update: `UPDATE jobs SET state = 'cancelled', finished_at = now() WHERE id = $1 AND state = 'available' RETURNING …` (`internal/pgstore/cancel.go`).
2. `Lease` is one statement too: a CTE selects the next available rows `FOR UPDATE OF j SKIP LOCKED`, then updates them to `leased` with a new `lease_token` and `attempts + 1` (`internal/pgstore/lease.go`).
3. **Lease locks first.** Cancel's `UPDATE` finds the row and waits for the lock. When Lease commits, Postgres re-evaluates Cancel's `WHERE` on the newest row version (EvalPlanQual under READ COMMITTED), sees `leased`, and updates nothing.
4. Cancel matched zero rows, so it re-reads the job to say why: `leased` → `store.ErrWrongState` → `FAILED_PRECONDITION` (HTTP 409) with "job N is leased; only available jobs can be cancelled". A missing job is `NOT_FOUND`; an already `cancelled` job returns success, so a retried cancel is harmless.
5. **Cancel locks first.** Lease's `SKIP LOCKED` skips the row and takes the next one. If Cancel has already committed, Lease's re-check sees `cancelled` and the row no longer qualifies.
6. Either way the row changes state once. Cancel sends no `NOTIFY`: it never makes a job runnable.

**Numbers:** `TestCancelRacesLease` (`internal/pgstore/cancel_test.go`) runs 50 rounds of Lease and Cancel released at the same instant on one job and asserts exactly one winner each round: either the job is leased and Cancel got `ErrWrongState`, or it is cancelled and Lease got nothing. Each statement is autocommit, so the lock is held only for one statement.

**Fails halfway:**
- Cancel commits and its reply is lost: the retry finds `cancelled` and succeeds.
- Lease commits and its reply is lost: the job is leased to nobody. A cancel now gets 409; after the visibility timeout the reaper makes it available again, and a cancel can win then.
- A worker dies holding the lease: same as above, the job comes back after the visibility timeout.

**Trade-off:** cancel only removes work that hasn't started. Stopping a running job would need a signal to the worker (for example in the heartbeat response) and cooperative handlers; v1 doesn't do it. The gain is that cancel needs no coordination with workers at all, just the row lock.

## Q17. Why does the dead-letter list page with a keyset instead of `OFFSET`?

Two parts: cost per page, and correctness while the list changes.

**What:** a keyset page starts right after the last row the client saw, `(finished_at, id) < (last_finished_at, last_id)`, so every page is one short index range scan.

**How:**
1. The query is `WHERE queue = $1 AND state = 'dead' AND (finished_at, id) < ($2, $3) ORDER BY finished_at DESC, id DESC LIMIT n` (newest first; the condition is left out on the first page).
2. Migration `0002` rebuilt the partial index as `jobs_dead (queue, finished_at, id) WHERE state = 'dead'`, so the sort order, the tiebreak and the cursor condition all come from the index.
3. The server reads one row more than the page. If it gets it, the page carries a `next_page_token`; otherwise the last page has none.
4. The token is the last row's `(finished_at, id)`, base64-encoded and opaque to clients. A token that doesn't decode is `INVALID_ARGUMENT`.
5. `id` is the tiebreak: many jobs can die in the same microsecond, and without it rows with equal `finished_at` could repeat or vanish between pages.

**Numbers:** `page_size` defaults to 100, which is also the maximum, so a page reads at most 101 index entries wherever it is. With `OFFSET 100000` Postgres would read and throw away 100,000 entries before returning 100: the cost grows with the page number.

**Fails halfway:** the list changes between requests (jobs die, are redriven or pruned).
- Keyset: the next page still starts after the last row seen. A job that died meanwhile is newer than the cursor, so it isn't in this walk (a fresh listing shows it), but nothing repeats or is skipped.
- `OFFSET`: a new death pushes every row down one place, so the next page repeats a row; a pruned row pulls them up, so one is skipped.

**Trade-off:** no "jump to page 50" and no total count, and the cursor only works for one sort order. For an operator scrolling dead jobs, newest first, that is fine.

## Q18. Redrive-all was quadratic until a review caught it. What was wrong, and how did `EXPLAIN` show it?

Three parts: the bad query, why it was quadratic, and how the plan proved the fix.

**What:** the redrive query ordered by `id` and folded both modes into one statement with `$2 IS NULL OR id = ANY($2)`, so neither mode could use the index it needed.

**How:**
1. Redrive runs in batches of 1,000, one transaction each: a CTE picks dead jobs `FOR UPDATE SKIP LOCKED`, the `UPDATE` makes them available with `attempts = 0`, and the same transaction sends `pg_notify`.
2. The first version used one statement for both modes: `WHERE queue = $1 AND state = 'dead' AND ($2::bigint[] IS NULL OR id = ANY($2)) ORDER BY id LIMIT $3`.
3. **All mode:** `jobs_dead` is ordered by `(queue, finished_at, id)`, not by `id`. To find the 1,000 lowest ids, Postgres had to read every remaining dead job of the queue and sort it, in every batch.
4. **Ids mode:** pgx reuses prepared statements, and after a few runs Postgres can switch to a generic plan that doesn't know whether `$2` is NULL. Such a plan can't use `id = ANY($2)` on the primary key, so it scans the queue's dead jobs too.
5. The fix (commit `090745b`) uses two statements. All mode orders by `finished_at, id`, so it reads `jobs_dead` from the front; redriven rows leave the partial index, so every batch starts at the front again. Ids mode is `WHERE id = ANY($2) AND queue = $1 AND state = 'dead'` and looks the ids up in the primary key.
6. `EXPLAIN` on 200,000 dead jobs showed the fix: all mode as an index scan on `jobs_dead` with `Index Cond` on `queue` under the `Limit`, no sort of the whole set; ids mode as `jobs_pkey` with `Index Cond: id = ANY(...)`, also with `plan_cache_mode = force_generic_plan`. The thing to look for in the bad plan is a `Sort` over every dead row of the queue below the `Limit`.

**Numbers:** 200,000 dead jobs is 200 batches. Before the fix each batch read all remaining dead jobs, about 200,000 + 199,000 + … ≈ 20 million rows in total (n²/2,000); after it each batch reads about 1,000 index entries, about 200,000 in total. The `EXPLAIN` output and the before-fix timings are in the review notes, not in the repo.

**Fails halfway:** each batch commits on its own. If the server stops in the middle, the committed batches stay redriven and calling redrive-all again finishes the rest; a gone client starts no new batch.

**Trade-off:** two statements instead of one is a little more code; the alternative was a plan that depends on a NULL check. Still open, parked for the M5 load test: at 200,000 rows the outer `UPDATE` joined the CTE with a Hash Join and a Seq Scan on `jobs` (the planner's cost choice), which would be a table scan per batch on a large table.

*Measured in M5a:* with a 1-million-row table, redriving 100,000 dead jobs took about 3 s (30,000–34,000 jobs/s), and the logged batch plan used index scans and a Nested Loop: one batch of 1,000 took 16 ms (`docs/benchmarks.md`, S4). The Hash Join did not appear.

## Q19. Tasklane's queue limit is "soft". What does that mean, why not a hard limit, and why does it stop refusing after 10 s of stale data?

Three parts: soft vs hard, how the soft one works, and fail-open.

**What:** each server checks a cached depth that is refreshed once a second, not a count taken inside the enqueue transaction, so the limit can be overshot by about one second of traffic.

**How:**
1. A goroutine on each server runs one query for all queues every second: available plus leased jobs per queue, with each queue's `max_depth`.
2. `Enqueue` (and `EnqueueBatch`, refused whole) checks the cached value before touching the store. At or above `max_depth` it returns `RESOURCE_EXHAUSTED` (HTTP 429) with a 1 s retry hint (`Retry-After`, gRPC `RetryInfo`). The Go SDK returns `ErrQueueFull` and doesn't retry it: a full queue is a signal for the producer's owner.
3. **A hard limit** would count inside each enqueue's transaction and need a lock (or a counter row) to stop two enqueues from both seeing room. Every enqueue on the queue would then serialize on that lock: a hot row on the hot path, the same problem as the counters table in Q14.
4. **Fail-open:** if refreshes fail, the last good values are used for up to 10 s; after that the cache says "not full" for every queue. Refusing all enqueues on unknown data would turn a short database blip into a producer outage, and while the database is really down `Enqueue` fails with `UNAVAILABLE` anyway.

**Numbers:** demo producer at 10 jobs/s against a queue with `max_depth = 50` and no workers: refusals start after about 50 jobs (README). Overshoot is at most about 1 s of traffic, about 10 jobs here. Default `max_depth` is 100,000. `TasklaneStatsStale` fires when the cached depths are over 30 s old.

**Fails halfway:**
- The refresh fails for 10 s: the server keeps refusing or admitting on 10 s old numbers; after 10 s it admits everything until a refresh works.
- Two servers each have their own cache, refreshed at different moments, so for up to a second one may refuse while the other admits.
- An enqueue retried with an existing idempotency key also gets 429 while the queue is full, because the check runs before the store.

**Trade-off:** soft and cheap (one query per server per second, no lock on the enqueue path) against exact. For backpressure, being 1 s late is fine; the point is to stop unbounded growth, not to hit a precise number.

## Q20. Why do dead jobs expire after 7 days instead of staying until someone looks?

Two parts: why expire at all, and why that is safe.

**What:** the pruner deletes dead jobs older than `TASKLANE_DEAD_RETENTION` (default `168h`), like messages in an SQS dead-letter queue.

**How:**
1. Every server runs the pruner every minute (`TASKLANE_PRUNE_INTERVAL`). Each pass deletes in batches of 1,000 with `FOR UPDATE SKIP LOCKED` until a batch is short, so servers don't collide and no transaction is large.
2. Completed and cancelled jobs go after 24 h (`TASKLANE_RETENTION`, also the idempotency-key window); dead jobs after 7 days.
3. Age is measured from `finished_at`. A redrive sets `finished_at = NULL`, so a job that dies again starts a fresh 7 days.
4. Failures are seen long before they expire: `TasklaneDeadJobs` (`increase(tasklane_jobs_dead_total[10m]) > 0`) fires within a scrape or two of a job dying and stays firing for 10 minutes, and the dashboard shows dead jobs in the last hour.

**Numbers:** 7 days of dead jobs, 1,000 rows per delete batch, one pass a minute. Payloads can be up to 256 KiB each, so a forgotten DLQ is not small.

**Fails halfway:** each batch commits on its own; if a server dies mid-pass, the next pass (on any server) continues. The dead-job delete has no queue to fix the `jobs_dead` prefix, so it scans that index across queues; dead jobs are few and it runs once a minute.

**Trade-off:** without expiry the table, its indexes and vacuum work grow forever with jobs nobody will redrive. With it, the evidence is gone after a week. An operator who needs longer raises the retention; exporting dead jobs before they expire is not built.

## Q21. Why do you graph counters with `rate()` instead of their raw value?

Two parts: what a counter's value means, and what `rate()` does with it.

**What:** a counter's raw value is "events since this process started", which depends on uptime; `rate()` turns it into events per second, which is what you want to see.

**How:**
1. Counters only go up while the process runs (`tasklane_jobs_acked_total` is incremented after each committed Ack) and start again at 0 when the process restarts.
2. `rate(x[1m])` takes the samples in the last minute and divides the increase by the time span. If a sample is lower than the one before, it assumes a restart and counts from 0, so a restart doesn't show up as a huge negative rate.
3. The dashboard sums the per-server rates: `sum(rate(tasklane_jobs_acked_total{queue=~"$queue"}[1m]))`. `increase()` is the same idea as a total over a window: "dead in the last hour" is `sum(increase(tasklane_jobs_dead_total[1h]))`.

**Numbers:** Prometheus scrapes every 5 s in the demo, so a 1 m window holds about 12 samples. `rate()` needs at least two samples in the window; a window of a few scrape intervals smooths out a missed scrape.

**Fails halfway:** a server restarts mid-window: `rate()` handles the reset, but the increments between the last scrape and the crash are lost (at most 5 s of them). That's why the chaos test can't check counters against the database (rule 5): restarted servers start at 0.

**Trade-off:** `rate()` averages over its window, so a 1 m window hides a 5-second spike and reacts a bit late. A shorter window is noisier.

## Q22. Why is `job_id` never a metric label?

Two parts: what a label costs, and where per-job data goes instead.

**What:** every distinct label combination is its own time series, and `job_id` is unbounded, so it would create a new series for every job.

**How:**
1. Prometheus keeps every active series in memory and indexes it. The cost grows with the number of series, not the number of samples.
2. `queue` is a label because operators choose queues: a handful of values. `kind` (free text from producers), `job_id` and `priority` are never labels (design §16.2).
3. Histograms multiply it: `tasklane_queue_wait_seconds` has 16 buckets plus `+Inf`, `_sum` and `_count`, so 19 series per queue per server.
4. Per-job questions ("what happened to job 42") go to logs, which carry `job_id`, and to traces (M4c). Metrics answer "how many, how fast".

**Numbers:** at the demo's 10 jobs/s, a `job_id` label on queue wait would add 19 series per job: 864,000 jobs a day, about 16 million new series a day, each used once. With `queue` only it is 19 per queue per server.

**Fails halfway:** this is a slow failure, not a crash on day one: Prometheus memory grows with each new series, queries slow down, and eventually it runs out of memory and dashboards and alerts for everything go dark with it.

**Trade-off:** metrics can't tell you about one job; you pay for that with a log search. That's the right split: metrics are cheap aggregates, logs and traces hold the detail.

## Q23. With two servers, why does the dashboard use `max by (queue)` for queue depth but `sum` for throughput?

Two parts: what each kind of metric measures, and the right aggregation for each.

**What:** depth gauges are the same database-wide number reported by every server, so you take one copy; counters count what each server did, so you add them.

**How:**
1. Each server's depth refresh queries the whole database and sets `tasklane_queue_depth{queue,state}` and `tasklane_oldest_ready_job_age_seconds`. Two servers report the same value twice.
2. `sum` over those would double the depth with two servers and triple it with three. `max by (queue)` keeps one copy per queue; the dashboard then sums across queues: `sum(max by (queue) (tasklane_queue_depth{state="available"}))`.
3. Counters such as `tasklane_jobs_acked_total` count the Acks that server handled. The total is `sum(rate(...))` over both servers.
4. The alert rules follow the same rule: `max by (queue)` for `TasklaneJobsStuck`, `sum by (queue)` for the counter alerts.

**Numbers:** in the two-server smoke test a plain `sum` of available showed about 13 (7 + 6: the two servers reporting the same queue a moment apart), against about 7 real jobs. "Requests per server" uses `sum by (instance)`, which is correct because it is a per-server counter.

**Fails halfway:** if one server can't reach the database, its gauges keep their last values, so `max` can show a stale higher number. `tasklane_queue_stats_age_seconds` grows on that server and `TasklaneStatsStale` fires after 30 s.

**Trade-off:** every server repeats the same query (one per second each) so that any one of them can be scraped alone. One exporter or one leader reporting the gauges would remove the duplicates, at the cost of one more thing to keep running.

## Q24. Prometheus pulls metrics from Tasklane. Why pull instead of push?

Two parts: what pull gives you, and when push is the better choice.

**What:** Prometheus scrapes each server's `/metrics` on a schedule; the servers only keep current values in memory.

**How:**
1. Each server serves `/metrics` on `:9464` (`TASKLANE_METRICS_ADDR`), a port separate from the API, without an API key, so operators can keep it internal.
2. Prometheus has the target list (`tasklane-1:9464`, `tasklane-2:9464`) and scrapes every 5 s in the demo.
3. Because Prometheus knows which targets should exist, a failed scrape becomes `up == 0`: `TasklaneDown` and the "Servers up" panel come for free. With push, a dead server just goes quiet.
4. A slow or down Prometheus can't slow the server: there is no send path, no buffer, no retry.
5. Push fits short-lived jobs that may exit before a scrape (a batch job pushes to a Pushgateway), or targets Prometheus can't reach.

**Numbers:** scrape interval 5 s (15 s is the usual default), retention 1 day in the demo. A missed scrape loses resolution, not data: counters are cumulative, so the next scrape has the total.

**Fails halfway:** a server crashes between scrapes: the increments since the last scrape (≤ 5 s) are lost and its counters restart at 0, which `rate()` handles. A network split between Prometheus and a server shows as `up == 0` even if the server is fine.

**Trade-off:** pull needs Prometheus to know and reach every target (service discovery, network access); push needs the app to handle a slow or missing collector and loses the free liveness signal.

## Q25. Why does Tasklane create every queue's counters at 0 instead of when the first event happens?

Two parts: what goes wrong with a series that is born at 1, and how the zero is created.

**What:** a series that first appears with value 1 has no earlier sample, so `increase()` sees no increase and an alert on the first dead job never fires.

**How:**
1. A Prometheus client creates a labeled series the first time it is touched. Without help, a queue's `tasklane_jobs_dead_total{reason="nacked"}` would first appear when its first job dies, already at 1.
2. `increase()` and `rate()` compute from differences between samples. One sample at 1 has nothing to compare against, so `increase(tasklane_jobs_dead_total[10m]) > 0` (`TasklaneDeadJobs`) stays silent for that first dead job.
3. Each depth refresh (every second) calls `SetQueueStats`, which touches every counter for every known queue (`WithLabelValues` without `Inc`), including both `dead` reasons. The series exist at 0 before anything happens.
4. Graphs then show 0 instead of "no data", which is also easier to read.

**Numbers:** 8 per-queue counters plus 2 `dead` reasons: 10 series per queue per server, created within a second of the queue existing.

**Fails halfway:** a queue created and used within the same second, before a refresh, can still have a series born at 1. A server that restarts recreates the zeros at its first refresh.

**Trade-off:** a few idle series per queue (cheap, since queues are few) for alerts that see the first event.

## Q26. How do you prove a job queue never loses or double-completes a job?

Three parts: what is recorded, the five rules, and why one rule is only a lower bound.

**What:** run real jobs through two servers while things break, record what every producer and worker saw (a ledger), then check five rules against the ledger and the final `jobs` table (`test/invariants`).

**How:**
1. **Record.** The producer records each idempotency key with the job ID only after the server confirms the enqueue. Each worker records every run through `WorkerOptions.OnResult`: job, attempt, start, end, outcome (`acked`, `nacked`, `lease_lost`, `gave_up`) and the handler error.
2. **Rule 1, nothing lost or stuck:** every confirmed job still exists and every job ends `completed` or `dead`.
3. **Rule 2, no duplicate jobs:** one job per idempotency key, even when an enqueue was retried after a server died.
4. **Rule 3, fencing works:** two runs never hold the same (job, attempt); an accepted Ack is always from the last attempt; a completed job has at most one accepted Ack; a dead job has none.
5. **Rule 4, repeats are justified:** attempt k+1 starts no earlier than attempt k's start plus the visibility timeout, minus a tolerance, unless attempt k nacked or its handler failed.
6. **Rule 5, counters match the database** (in-process test only): summed over both servers, acked equals completed and dead equals dead.
7. **Why rule 4 is a lower bound:** the worker doesn't know when its lease really ends. It lasts at least one visibility timeout from the lease time, and heartbeats can extend it. So the check only catches a repeat that started before even the shortest possible lease could end; it can't prove a repeat was justified. It errs on the side of no false alarms.

**Numbers:** the in-process test runs 10 workers × 1,000 jobs on every PR (about 8 s) and 50 × 10,000 with `-invariant.full` (about 1m17s, 380 MB); visibility timeout 2 s, tolerance 500 ms. The chaos test ran 1,889 and 1,654 jobs through 12 faults each (tolerance 1 s). Rule 5 caught a real bug (Q32).

**Fails halfway:** the ledger itself must survive crashes. Each record is one `write` on an `O_APPEND` file, so a SIGKILL loses at most the line being written; a restarted worker closes a torn last line first, and the reader drops cut-off lines. The rules are written to tolerate what a crash can legitimately cause: a completed job may have no `acked` run (the worker died after the server committed), and `gave_up` runs are never checked.

**Trade-off:** the ledger and the database each show half the story. The table alone can't show two accepted Acks (the job just says `completed`); the ledger alone can't show a lost job. Checking both catches both, but the rules need careful exemptions, and every exemption (like the freeze window in Q27) is a place a real bug could hide.

## Q27. What is a zombie worker, and how does Tasklane stop it from corrupting a job?

Three parts: what a zombie is, how the lease and token fence it off, and what fencing can't undo.

**What:** a worker that stalls past its lease (a long pause, GC, a network split), comes back and acts as if it still owns the job.

**How:**
1. A lease lasts the visibility timeout; SDK workers heartbeat every VT/3 to extend it. A frozen process sends no heartbeats.
2. The reaper (every 5 s; 2 s in the demo) finds `state = 'leased' AND lease_expires_at < now()` with `SKIP LOCKED` and makes the job available again after a backoff, or `dead` if that was the last attempt.
3. The next Lease gives it a new random `lease_token` and `attempts + 1`.
4. The zombie wakes and Acks with its old token. The Ack is `UPDATE … WHERE id = $1 AND state = 'leased' AND lease_token = $2`, which matches nothing, so the server returns `FAILED_PRECONDITION` (409). The SDK records `lease_lost` and doesn't retry.
5. Only the current holder's Ack counts. The reaper clears the token when it reclaims the job (`lease_token = NULL` in `retryOrBury`, `internal/pgstore/complete.go`), so after a reap the zombie's Ack fails even if nobody has leased the job again. If the lease expired but the reaper hasn't run yet, the token is still current and the Ack succeeds (Ack's `WHERE` has no expiry check), which is correct: it did the work (Q9).
6. What fencing can't undo: the zombie's handler already ran. If it sent an email or charged a card, that happened. Handlers must be idempotent, or pass the lease token downstream as a fencing token so other storage can reject a stale holder too.

**Numbers:** the in-process test runs two raw-API zombies (SDK workers can't be zombies: heartbeats keep slow handlers' leases), each leasing one job, sleeping VT + 500 ms = 2.5 s with no heartbeats, then acking; a default run records 5–6 `lease_lost`, the full run 52. The chaos test pauses a worker for 15 s against a 10 s visibility timeout: 16 `lease_lost` in the seed-1 run.

**Fails halfway:**
- The zombie's Ack is rejected but its side effect is done: the job runs twice; that's at-least-once.
- A frozen worker stamps its run's start after the unpause, so the checker can't tell when its lease began. The first chaos run flagged three correct jobs that way; rule 4 now skips runs started in a freeze window (a known weakness: a real violation there would be missed).

**Trade-off:** a shorter visibility timeout finds zombies sooner but costs more heartbeat writes and risks expiring slow but healthy jobs whose heartbeats are delayed.

## Q28. Why does gRPC need an L7 load balancer? And why a proxy rather than client-side balancing?

Three parts: why L4 fails for gRPC, what Envoy does, and proxy vs client-side.

**What:** gRPC sends all of a client's calls over one long-lived HTTP/2 connection, so balancing connections (L4) pins each client to one server; balancing requests (L7) spreads them.

**How:**
1. An L4 balancer picks a server per TCP connection. Each worker opens one connection and multiplexes every Lease, Ack and Heartbeat on it, so with 3 workers and 2 servers one server gets two workers' traffic, and a restarted server gets nothing until clients reconnect.
2. Envoy terminates HTTP/2 and picks a server per request with `least_request`. Long-poll Leases hold a request for up to 30 s, so counting open requests matters more than taking turns.
3. Envoy health-checks `/readyz` every 2 s (out after 2 failures), ejects a server after 3 gateway errors in a row for 10 s, and retries only requests that never reached a server.
4. **Client-side balancing** (gRPC's `round_robin` over DNS) has no extra hop, but every SDK would need it (Go now, TS and C# planned), each with its own health checking, and HTTP/JSON clients like `curl` get nothing.
5. **A proxy** gives every client, in any language, the same behavior from one config file, and owns the host ports, so clients see one address.

**Numbers:** with both servers up, about 16 and 17 requests/s each; after `docker compose kill tasklane-1`, `tasklane-2` took about 33 requests/s and the acked rate held at about 9.3–9.7 jobs/s. Envoy uses about 17 MiB.

**Fails halfway:** a server is killed mid-request. New calls failed fast with `Unavailable` for about 14–18 s, most likely until two failed health checks and the 5 s DNS re-resolution dropped the dead host (not proven; keepalive and a `previous_hosts` retry rule didn't shorten it; M5b later added `previous_hosts` back for rolling restarts, see Q51 and design §20.3). The SDK retries. A Lease that reached the dead server isn't retried by the proxy, because it may have leased jobs; those wait out their visibility timeout.

**Trade-off:** the proxy is one more hop and one more component that must stay up (in production you'd run several). Client-side balancing removes the hop but multiplies the work by the number of SDKs. A look-aside balancer (xDS) is the middle ground; not needed here.

## Q29. Your proxy has a 15 s timeout and your long-poll waits 30 s. What happens?

Three parts: what breaks, what it costs, and the rule that prevents it.

**What:** the proxy cuts every idle long-poll at 15 s and returns an error, so an idle system produces a steady stream of failed Leases.

**How:**
1. The SDK sends Lease with a 20 s wait; the server allows up to 30 s (`MaxWait`). Envoy's default route timeout is 15 s.
2. On an empty queue, at 15 s Envoy gives up, answers the client with a timeout (its 504, which Envoy turns into gRPC `Unavailable`) and resets the stream to the server. Tasklane never ran with the 15 s default, so this is from Envoy's documented behavior, not a measurement.
3. The server sees the request context cancelled and ends the wait. It checks the context after every wake-up, so a cancelled Lease isn't handed jobs.
4. The SDK treats `Unavailable` as transient: it backs off (full jitter, 1 s base) and long-polls again. A job that arrives meanwhile waits for the backoff.
5. The race: a job arrives at about 15 s, the server leases it (the store write runs to completion even if the client goes away, Q32) just as Envoy cuts the stream. The response is dropped, and the job sits leased to nobody until its visibility timeout (30 s default) expires, with one attempt burned.
6. Envoy doesn't retry a timeout here (only `connect-failure`, `refused-stream`, `reset-before-request`), which is right: a retried Lease could lease a second batch.

**Numbers:** Tasklane sets the route timeout to 35 s: above `MaxWait` (30 s), which is above the SDK's 20 s wait (whose own call deadline is 30 s). With 15 s, every idle worker would log one failed Lease every ~15 s. `TasklaneErrors` would **not** fire: it reads the server's `tasklane_rpc_duration_seconds_count{code}`, and when Envoy cuts the stream the server records `Canceled`, not `Unavailable` (only clients see `Unavailable`). So the dashboard looks healthy; the symptom shows only in client errors and logs and in Envoy's own stats, which makes it easy to miss.

**Fails halfway:** that is the race in step 5: the commit succeeded, the delivery didn't, the same shape as the stranded-lease bug in Q10. Recovery is the visibility timeout.

**Trade-off:** the rule is that each layer's timeout must be longer than the one inside it (client deadline > proxy timeout > server's maximum wait). The cost of a long proxy timeout is that a really stuck request holds a proxy slot for 35 s; the server's own 30 s cap bounds that.

## Q30. Why doesn't a third or fifth server copy add throughput?

Three parts: where the work really happens, what each copy adds, and what would scale.

**What:** every server is a thin layer over one shared Postgres; the work per job (enqueue, lease, ack: three write transactions) lands on the same database whichever server handles it.

**How:**
1. Servers are almost free (about 11 MiB each). The bottleneck is the database: each job is a few writes, each with WAL and index updates, on one primary.
2. Each copy adds background work: a reaper (every 5 s), a pruner (every minute) and a depth refresh (every second), all repeating the same queries. Two depth refreshes are two queries a second; five are five.
3. Each copy adds connections: its own pool (pgx's default, the larger of 4 and the CPU count) plus one LISTEN connection. Postgres connections are processes, and `max_connections` defaults to 100.
4. Each `NOTIFY` goes to every server's listener, and each wakes its waiting Leases, so one enqueue on an idle queue costs one query per waiting Lease on every server.
5. Fixes, in order: batching (`EnqueueBatch` exists, a Lease already takes k jobs in one transaction; Ack batching doesn't); one leader for the background loops (for example a Postgres advisory lock); PgBouncer to cap connections (the LISTEN connection needs a session, so it would bypass transaction pooling); then splitting queues across databases, or the Raft store planned for Phase 2.

**Numbers:** no measurement with more than two servers yet: the M5 load test compares two against one. One relevant number: the depth refresh's available count ran as a Seq Scan taking 443 ms on a fresh, never-analyzed 12,000-job table (parked for M5). Repeated by five servers every second, that would be over 2 s of database time per second, but only until autovacuum's ANALYZE gives the planner statistics and it switches to the index; it is not the steady state. The point is that every per-server query is multiplied by the number of copies.

*Measured in M5a:* the load test replaced the two-copy run with capacity maths (two copies on one pinned CPU would show nothing). Server CPU per job was 1.29 ms and Postgres 2.87 ms, so one server CPU keeps about 2.2 Postgres CPUs busy (Q40). The depth count did become a real cost, with a large backlog rather than missing statistics: about 440 ms per count with 1 million available jobs (Q44).

**Fails halfway:** adding copies during an overload makes it worse: more connections and more repeated loops on the database that is already the bottleneck.

**Trade-off:** more copies still buy availability (one can die, as the demo shows) and rolling deploys; they just don't buy throughput. Scaling the database means giving up a single source of truth or adding a consensus layer.

## Q31. A backlog of 2,000 jobs, arrivals at 10 jobs/s, capacity 14.7 jobs/s. How long until the backlog is gone?

Two parts: the arithmetic, and what the demo measured.

**What:** only the spare capacity drains the backlog: 14.7 − 10 = 4.7 jobs/s, so 2,000 ÷ 4.7 ≈ 425 s, about 7 minutes.

**How:**
1. New arrivals take 10 jobs/s of the capacity just to stand still.
2. The remaining 4.7 jobs/s eats into the backlog: 2,000 ÷ 4.7 ≈ 425 s.
3. A job arriving at the start waits behind the whole backlog: 2,000 ÷ 14.7 ≈ 136 s at first (Little's law: wait = jobs ahead ÷ throughput), falling to near zero as the backlog drains.
4. Spare capacity is what matters, not total capacity: going from 10 to 15 slots took the drain rate from negative to about 5 jobs/s.

**Numbers** (the 2026-11-01 demo lab, 10 jobs/s arriving, 5 slots per worker):
- 1 worker (5 slots): about 4 jobs/s; the backlog grew about 6/s.
- 2 workers (10 slots): 9.32–9.44 jobs/s; the backlog still grew about 0.6/s.
- 3 workers (15 slots): 14.68–14.8 jobs/s; the backlog went from 2,110 to 2,033 in 15 s, about 5 jobs/s net, so about 7 minutes for ~2,000.

Caveat on measuring capacity: count it as completed jobs per second, as above. That already absorbs retries (flaky and poison jobs take extra runs; the chaos run had about 1.19 runs per job). Capacity estimated from slots ÷ average handler time would overstate it.

**Fails halfway:** if a worker dies during the drain, capacity drops to about 9.3–9.4 jobs/s (two workers) and the backlog grows again by about 0.6/s instead of shrinking. If the backlog reaches `max_depth`, enqueues are refused (Q19).

**Trade-off:** keeping spare capacity costs idle workers most of the time; without it, any burst turns into a long tail. The usual target is enough headroom to drain a typical burst within the latency the producers can accept.

## Q32. Your invariant test failed once in 60 runs: the counters didn't match the database. What was wrong, and how did you fix it?

Three parts: the symptom, the cause, and the fix with its edges.

**What:** when a client disconnected during an Ack, Postgres committed the Ack, but the server saw `context canceled`, skipped its metrics and returned an error, so `tasklane_jobs_acked_total` ended one below the completed count.

**How:**
1. Rule 5 failed about once in 60 default runs: 966 accepted Acks plus 1 `gave_up` run against 967 completed jobs, and the acked counter one short.
2. The test cuts some workers' connections mid-run. gRPC then cancels the request context of their in-flight calls.
3. The Ack is one autocommit `UPDATE`. Postgres can commit it while pgx, seeing the context cancelled, stops waiting for the reply and returns `context canceled`. The handler took that as a failure: no `metrics.Acked`, an error to a client that was gone anyway, and the row `completed`.
4. A throwaway test proved it: cancelling `pgstore.Ack`'s context 0–400 µs in left the row completed with `context canceled` returned in 1,669 of 2,000 tries. Nack to `dead` had the same exposure.
5. The fix, `detach` (`internal/api/server.go`): every store write reached from an RPC runs on `context.WithoutCancel(ctx)` with a 5 s `storeWriteTimeout`. A write that starts also finishes, and its bookkeeping runs, whatever the client does.
6. The Lease long-poll wait stays cancellable; only each lease attempt is detached. After every wake-up the server checks the client's context again, because `select` picks at random when a wake-up and a cancel are ready together, and a gone client must not be handed jobs.

**Numbers:** 5 s is far above a healthy write (milliseconds). A write in flight when the drain starts finishes inside the 10 s graceful-stop budget; one started in its last seconds can delay exit by at most 5 s, and it still completes and is counted (`grpcServer.Stop()` doesn't wait for handlers, but the deferred `pool.Close()` does). Verified with handler tests that call every write with an already-cancelled context (all failed before the fix), then 180 clean invariant runs (30 with `-race`, 150 without). The old code would pass 180 in a row only about 5% of the time.

**Fails halfway:** remaining edges. A write that commits exactly at the 5 s timeout can still be reported as `DeadlineExceeded`. A Lease whose client is gone before its first attempt still leases; the jobs are counted but wait out their visibility timeout. A client deadline shorter than the write no longer cuts the write short.

**Trade-off:** the server now finishes work for clients that left, up to 5 s per call, in exchange for counters and responses that always match what was committed. The general lesson: a cancelled context means "the caller stopped waiting", not "the database did nothing". Any write whose outcome you record must outlive the request that started it, or check the database afterwards.

## Q33. A job is enqueued now and runs in a different process minutes later. How does one trace survive the queue?

Three parts: what gets stored, who writes it, who reads it.

**What:** the trace context travels in the job's row, not in any connection. The server stores the W3C `traceparent` in `jobs.trace_context` at enqueue; the worker reads it back at lease time and starts its span as a child.

**How:**
1. The producer's span context goes out on the `Enqueue` call as a `traceparent` header (the SDK, `curl` or any instrumented client).
2. The server creates its own `Enqueue` span as a child and, when the request's `trace_context` field is empty, stores that span's `traceparent` with the job (inside the same insert). An explicit field wins. With a no-op provider on the server it stores the incoming one, so producer and worker still connect.
3. When a `Lease` returns the job, the server records a `queue wait` span under the stored context (Q35); the worker SDK extracts the same context and starts `handle <kind>` as a sibling, then runs the handler with that span in its `ctx`.
4. The last field of a `traceparent` is the `sampled` flag. It is stored with the rest, so the producer's decision (head sampling, Q34) reaches the worker minutes later with no extra coordination: an unsampled job produces no spans anywhere.
5. Every retry reads the same stored value, so all attempts are siblings in one trace, and the gap between a Nack and the next `queue wait` is the retry backoff.

**Numbers:** a normal job's trace is 12 spans (producer, client and server `Enqueue`, four Postgres spans, `queue wait`, `handle`, client and server `Ack`, one `UPDATE`). A flaky job retried once was 20 spans over 5 services in 2.0 s. The stored value is 55 bytes (the field allows 512).

**Fails halfway:** a missing or malformed `traceparent` gives the worker a fresh root span and no wait span, with no error: a broken trace is better than a failed job. If the exporter is down, spans are dropped (batch processor, bounded queue), never blocking the handler. If a worker dies mid-job its `handle` span never ends, so it never exports; the retry's spans still join the trace.

**Trade-off:** one long trace per job, even across minutes of retries, is easy to read but can be big for a poison job (27 spans seen over 3 attempts in the demo, and a poison job runs 5). The OpenTelemetry messaging convention of a new trace per message linked to the producer's keeps traces small but makes the reader jump between them.

## Q34. Head sampling or tail sampling, and why does tail sampling fit a queue badly?

Two parts: what each decides on, and the memory cost of waiting.

**What:** head sampling decides when the trace starts (a coin flip by the producer); tail sampling waits for the trace to end and then keeps the interesting ones (errors, slow). Tasklane uses parent-based head sampling.

**How:**
1. Head, parent-based: the producer's sampler decides once (`OTEL_TRACES_SAMPLER=parentbased_traceidratio`, ratio 0.1) and every other process follows the `sampled` flag in the `traceparent`. A trace is whole or absent. Independent ratios per process (10% × 10% × ...) would leave most traces with holes.
2. Tail needs a Collector that holds every span of a trace in memory until it decides, because it cannot know the outcome earlier. It must see all spans of a trace in one place.
3. Its buffer is about span rate × trace duration × span size. At 1,000 jobs/s × 12 spans × ~1 KB × 120 s that is 12,000 spans/s × ~1 KB × 120 s ≈ 1.4 GB, before any overhead.
4. A queued job's trace lasts as long as its wait plus run plus retries: seconds to minutes, not the milliseconds of an HTTP request. That multiplies the buffer.
5. The decision window has to be fixed (say 120 s). Spans that arrive after it, such as a retry attempt 10 minutes later, find the decision already made and are dropped or form a fragment.

**Numbers:** the demo samples 100%: about 200 spans/s, 12 spans per normal job (my count from Jaeger). The 1.4 GB figure is arithmetic for a hypothetical 1,000 jobs/s, not a measurement; the demo is about 100 times smaller. Tail sampling is listed as future work (design §18).

**Fails halfway:** if the buffer is too small the Collector evicts traces early and decides on partial data; if the decision window is too short, long-lived traces are cut. Head sampling has neither problem but can drop the one slow trace you wanted: a 10% head sample keeps 1 in 10 failures.

**Trade-off:** head sampling is cheap, stateless and predictable, but blind to outcome. Tail sampling keeps what matters but costs memory and a stateful Collector that scales badly for long traces. For a queue, a higher head ratio plus exemplars (Q36, which point at specific sampled traces) is the cheaper way to be able to find a slow one.

## Q35. Why does the server record the `queue wait` span from Postgres times instead of the SDK?

Three parts: which clock, who has both numbers, and what it matches.

**What:** the server creates a finished span at lease time, from the job's `run_at` to the lease time, both of which come from Postgres.

**How:**
1. `run_at` is set by Postgres (`now()` or the requested time) and the lease time (`leased_at`) is Postgres `now()` too. One clock, so no skew between the producer's machine and the worker's.
2. The server already has both values inside `Lease`, so it needs no extra query and no proto change. If the SDK did it, the job would need to carry `run_at` to every SDK, and every SDK (Go, TypeScript, C#) would repeat the code.
3. The span is parented to the job's stored `traceparent`, so it shows in the right trace even though the `Lease` RPC started before any job was known (that RPC is its own short trace).
4. The same two values feed the `tasklane_queue_wait_seconds` histogram, so the span and the metric agree to the microsecond, and the histogram's exemplar (Q36) links to this very trace.

**Numbers:** in the flaky job's trace attempt 1 waited 6.9 ms and attempt 2 17.6 ms. With the demo's 1 s retry backoff, the gap between the Nack and the second wait span is about 1 s, shown as empty space, not as wait.

**Fails halfway:** a stored `traceparent` that is missing, malformed or unsampled records no wait span and no error. A `Lease` of 10 jobs records 10 spans, each in its own job's trace. A crash after the lease but before the span is recorded loses that one span only.

**Trade-off:** the wait span is accurate because both ends come from Postgres's one clock. The cost is that it exists only when the server is traced. A span measured on the worker's clock would include network time and clock skew, which this measurement should not contain.

## Q36. What is an exemplar, and why is it the bridge between metrics and traces?

Two parts: what is attached, and what it lets you do.

**What:** an exemplar is a sample trace ID attached to one histogram bucket observation. The metric says "p99 spiked at 14:03"; the exemplar on that bucket says "here is one request that landed in it".

**How:**
1. When a request's span is sampled, the server observes the histogram with the span's `trace_id` (`ObserveWithExemplar`) on `tasklane_rpc_duration_seconds` and `tasklane_queue_wait_seconds` (for waits, the job's own trace).
2. The `/metrics` endpoint serves the OpenMetrics format, the only text format that carries exemplars, and Prometheus runs with `--enable-feature=exemplar-storage`.
3. Grafana's panels have exemplars switched on and the Prometheus data source maps the `trace_id` label to the Jaeger data source, so a dot on the graph opens that trace.
4. Aggregates answer how many and how slow; one trace answers why. The exemplar is the link between them.

**Numbers:** with a query on Prometheus's exemplar API, both histograms returned exemplars with `trace_id` labels in the demo. Prometheus keeps exemplars in a fixed-size in-memory buffer (100,000 by default), so they cover recent history only.

**Fails halfway:** exemplars are only attached to sampled traces; with a 10% ratio a bucket may have none, or a trace that Jaeger has already evicted (the demo keeps 10,000 traces in memory). A scrape in the classic Prometheus text format drops them.

**Trade-off:** one exemplar per bucket per scrape interval is a sample, not every slow request, so it points at a representative trace, not the worst one. In return it costs almost nothing: no extra series, a few bytes per bucket.

## Q37. Why not put the job ID in the span name?

**What:** span names are grouped, indexed and aggregated by tracing backends, so they must have few distinct values; IDs belong in attributes.

**How:**
1. Jaeger's operation list, span-metrics and most backends key on the span name. `handle email.send` is one entry; `handle job 48213` would be one entry per job.
2. With 10 jobs/s the demo would create 36,000 operation names an hour. Every dropdown, index and metrics series grows with that number (the same reasoning as metric label cardinality).
3. The job ID is still searchable: `messaging.message.id=<job id>` is an attribute on each span. The name carries the job's kind (`handle <kind>`), which is a small set.
4. The same reason keeps the queue name, attempt and priority in attributes (`messaging.destination.name`, `tasklane.attempt`, `tasklane.priority`), following the OpenTelemetry messaging conventions.

**Numbers:** the demo has 3 kinds (`normal`, `flaky`, `poison`), so 3 `handle` names; job IDs would make one per job. Searching `tasklane.attempt=2` found retried jobs in Jaeger's UI without any special naming.

**Fails halfway:** if a name does carry an ID, nothing breaks at once: the UI gets slow and the backend's operation index grows without bound, long after the change that caused it. That makes it a hard bug to attribute.

**Trade-off:** attributes are searchable but cost storage per span, and some backends index only a configured set. A readable name like `handle email.send` is worth more than one that tries to be unique.

## Q38. Why do the SDK and the server use W3C trace context explicitly instead of the global propagator?

**What:** OpenTelemetry's global propagator is a no-op until the application sets one, so code that relied on it would silently write and read nothing, and every trace would break at the queue.

**How:**
1. The stored `trace_context` field is defined as a W3C `traceparent`. The code that writes it and the code that reads it both call `traceparent.W3C` directly, so the format is fixed by the field, not by configuration.
2. A library must not change global state. If the SDK installed the global propagator it would fight the application's own (B3, for example), and any other library in the process would change behaviour.
3. The gRPC and HTTP instrumentation does take a propagator option, and the same W3C one is passed to it, so incoming headers from `curl` or other tracing clients are understood.
4. The tests run with the global propagator left alone and still expect producer, server and worker spans in one trace.

**Numbers:** the failure is binary, not slow: with a no-op global propagator the `traceparent` is empty, so jobs show up with no `trace_context` and each worker span is a separate root. The demo sets no global propagator and its traces are whole.

**Fails halfway:** an application that sets a different global propagator (B3 only) still gets Tasklane's W3C inside the library; its own calls use B3. The two never mix because each side picks its format.

**Trade-off:** the library ignores the application's choice of propagation format for its own calls. That is deliberate: a job's stored trace context has to be readable by every SDK and every future reader, so the format must be part of the data, not the environment.

## Q39. What is coordinated omission, and why does the Tasklane load test use an open model for latency?

Three parts: what goes wrong in a closed test, how the open model fixes it, and the numbers that show the difference.

**What:** in a closed test each client waits for its reply before sending the next request, so when the server stalls the clients stop sending, and the requests that *would* have waited are never made or measured. The test "coordinates" with the server and omits its worst moments, so the p99 looks better than users would see.

**How:**
1. **Closed model:** a fixed number of clients (16 producers, 50 worker slots), each sending as soon as the last call returns. It finds the maximum throughput M, but its latency is not honest.
2. **Open model:** jobs are sent on a fixed schedule, for example 346 jobs/s, whether or not the server keeps up. Real producers behave like this: users don't wait for your queue before clicking.
3. **Measure from the planned time:** the producer writes each job's *scheduled* send time into its payload, and the worker measures handler start minus that time. If a send itself is late because the system is slow, that lateness is counted too. Measuring from the actual send time would bring coordinated omission back.
4. **Test it:** a unit test stalls a fake server and checks that the stall shows up in p99.
5. So the suite runs closed first (find M), then open at 50%, 80% and 95% of M for latency.

**Numbers** (`docs/benchmarks.md`): before tuning, the closed run at 352 jobs/s reported p99 123 ms, while the open run at a *lower* 334 jobs/s reported p99 837 ms. After step 1, a closed run at 480 jobs/s reported p99 103 ms, while an open run at 455 jobs/s reported 2,281 ms. Same system, same hour: the closed numbers hid the queueing.

**Fails halfway:** if the load generator itself falls behind (here it shares a physical core with the server), the sends drift from the schedule. Measuring from the planned time still counts that delay, so it can only make latency look worse, never better; the report checks the achieved rate against the offered rate.

**Trade-off:** an open test needs a rate to aim at, and a wrong M wrecks it: open runs set from one high closed sample (480 jobs/s) were in fact at 88% and 105% of the median M and showed p99 over 2 s. So M must be a median of several runs.

## Q40. Why pin each part of the load test to its own CPUs, and why is CPU per job a better number to report than jobs per second?

Three parts: why pinning, why CPU per job, and how to turn it into capacity.

**What:** pinning makes runs repeatable enough to compare before and after; CPU per job is the cost of one job in milliseconds of CPU, which carries over to other machines, while jobs/s only describes this laptop's bottleneck.

**How:**
1. **Pinning:** without it the load generator steals CPU from the server and runs vary by more than a tuning step gains. The laptop has 2 cores with 2 threads each: Postgres gets core 0 (both threads), the server one thread of core 1, the load generator the other.
2. **CPU per job:** read each container's cgroup CPU time (`cpu.stat`, `usage_usec`) before and after the 60 s window and divide by the jobs done.
3. **Why it travels:** jobs/s is set by whatever limits first. Here that was the SSD's flush time per commit, which a different disk changes completely. CPU per job is the work the code does per job, which stays roughly the same on any x86 machine.
4. **Capacity:** `max jobs/s ≈ CPUs × 1000 / CPU ms per job`, once for the server and once for Postgres. The smaller one is the CPU ceiling.

**Numbers:** at 80% load after tuning, the server used 1.29 ms per job and Postgres 2.87 ms. Server ceiling: 1 × 1000 / 1.29 ≈ 775 jobs/s; Postgres: 2 × 1000 / 2.87 ≈ 697 jobs/s. The measured maximum was 433 jobs/s (median), below both, because commits waited on the WAL flush. Ratio 2.87 / 1.29 ≈ 2.2: one server CPU keeps about 2.2 Postgres CPUs busy. An 8-vCPU Postgres would have a CPU ceiling of about 8 × 1000 / 2.87 ≈ 2,790 jobs/s and need about 2,790 × 1.29 / 1000 ≈ 4 server CPUs (arithmetic, not measured). Even pinned, closed runs varied ±20% (352, 532, 392 jobs/s).

**Fails halfway:** CPU per job is not a constant. It depends on load: Postgres used 4.52 ms per job at 50% load and 2.22 ms at 95%, because fixed costs (idle re-checks, the depth refresh) are spread over fewer or more jobs and leases carry more jobs when busy. And hyper-threads are not full cores: the server and the load generator share one core, and Postgres's "2 CPUs" are one core, so the ceilings above are optimistic.

**Trade-off:** pinned runs on a small machine give small absolute numbers that look unimpressive; in return, a +10% change can be told apart from noise. Unpinned runs give bigger numbers that nobody can reproduce.

## Q41. How do you read a latency-vs-load curve, and what does Little's law say near saturation?

Three parts: the shape of the curve, Little's law, and what Tasklane's curve showed.

**What:** latency stays flat while the system has spare capacity, then rises steeply as load approaches the maximum (the "knee"), because jobs start waiting behind each other. You run below the knee.

**How:**
1. **Little's law:** jobs in the system = arrival rate × time each spends in it (L = λ × W). It holds for any stable system, with no assumption about the distribution.
2. **Why the knee:** at utilization ρ (load ÷ capacity), the time spent waiting in a simple single-server queue grows like ρ / (1 − ρ): about 1 at 50%, 4 at 80% and 19 at 95% (in units of the service time). The last few percent of capacity cost the most latency.
3. **Past capacity:** if arrivals exceed throughput, the backlog grows every second (by λ − μ jobs) and so does waiting time; there is no stable latency to report.
4. **Reading a real curve:** measure at several loads (here 50, 80 and 95% of M), use p99, not the mean, and set the operating point where p99 is still within the latency your producers accept, typically about 70–80%.

**Numbers:** after tuning, p99 was 34, 61 and 97 ms at 216, 346 and 411 jobs/s (50, 80 and 95% of M = 433), and p50 7.7, 11.7 and 16.4 ms. Using p50 as a rough stand-in for the mean (the mean was not recorded), Little's law gives about 346 × 0.0117 ≈ 4 jobs between enqueue and start at 80% load. Before tuning, p99 went from 47 ms at 281 jobs/s to 837 ms at 334 jobs/s: that curve hit its knee between 80% and 95%. Open runs at 88% and 105% of the median M had p99 of about 2.3 s.

**Fails halfway:** M is noisy (±20% run to run), so "95% of M" may really be 85% or 105%. Near the knee that difference is a 10× change in p99, which is why M is a median of several runs.

**Trade-off:** running near the knee uses hardware fully but turns any burst into a long tail; headroom costs idle capacity most of the time. A queue makes this softer than an API, because jobs can wait, but the backlog still has to drain (Q31).

## Q42. What was Tasklane's bottleneck under load, how did the evidence point to it, and why is moving NOTIFY after the commit safe?

Four parts: the evidence, the cause, the fix, and why the fix loses no jobs.

**What:** every `Enqueue` called `pg_notify` inside its transaction. Postgres makes such a transaction take a database-wide lock at commit and hold it until its commit is flushed, so enqueue commits went through one at a time.

**How:**
1. **CPU was not full:** at the peak (50 slots, closed) the server used 56% of its one CPU and Postgres 87% of one CPU out of two. So something was waiting.
2. **Wait events said what:** `Lock:object` 31% of Postgres's samples in the full cycle, 55% with enqueues alone; the rest mostly WAL (`IO:WalSync`, `LWLock:WALWrite`).
3. **More connections made it worse:** a pool of 16 instead of 4 gave no more throughput, and `Lock:object` rose to about 80%. A serial lock behaves exactly like that: more waiters, same rate.
4. **Why NOTIFY locks:** Postgres delivers notifications in commit order, so at commit a notifying transaction locks the notification queue for the whole database until the commit is done, including its disk flush. Commits could not share a flush any more. The `pg_notify` call itself cost 0.009 ms; the cost was the lock.
5. **The fix (tuning step 1):** the store no longer notifies. After the commit, the API server wakes its own waiting leases in memory at once and sends one `pg_notify` per queue per 5 ms window for the other servers.
6. **Why it's safe:** a NOTIFY is only a hint. Waiting leases also re-check the queue every second, because delayed jobs and retries become due without any notification. A rolled-back enqueue never reaches the publish step; a crash between commit and publish, or a failed NOTIFY, costs at most that one-second re-check. No job is lost, only up to a second of latency in a rare case.

**Numbers:** enqueue 747 → 1,048 jobs/s (+40%) and full cycle 392 → 433 jobs/s (+10%), medians of 3–4 runs; `Lock:object` 31–55% → 0%. What remains is the WAL flush: 72–83% of wait samples.

**Fails halfway:** if the server dies after the commit but before the publish, workers on other servers find the job at their next re-check, within 1 s. A side effect showed up: the server now wakes its own leases twice (in memory, then from its own NOTIFY 5 ms later). At 5 jobs/s, lease queries went from 1,597 to 2,523 a minute. Fixed right after M5a by tagging each NOTIFY with its sender and skipping your own (Q45).

**Trade-off:** before, Postgres guaranteed "notify only on commit" for free. Now the code guarantees it by publishing after the commit returns, and other servers hear up to 5 ms later. In return, enqueue commits no longer queue behind one lock.

## Q43. What are HOT updates, what did the load test measure, and why doesn't fillfactor help Tasklane?

Three parts: what HOT is, the measured ratio, and what that means for fillfactor and autovacuum.

**What:** a HOT (heap-only tuple) update is a Postgres update that changes no indexed column and fits the new row version on the same page, so no index needs a new entry. It is much cheaper than a normal update.

**How:**
1. Postgres never updates a row in place: it writes a new version and marks the old one dead. Normally every index gets an entry for the new version.
2. If no indexed column changed and the page has room, the new version is chained from the old one on the same page and the indexes are left alone: that's HOT.
3. `fillfactor = 70` leaves 30% of each page empty at insert time, so later updates find room on the same page. It is the usual way to get more HOT updates.
4. Tasklane's updates change indexed columns on purpose. Lease sets `state = 'leased'` and `lease_expires_at`; Ack sets `state = 'completed'` and `finished_at`. `state` decides which partial index a row is in, and the time columns are indexed. So no update can be HOT, whatever the free space.
5. The dead versions are cleaned by autovacuum, so the real questions are whether autovacuum keeps up and how big the table gets.

**Numbers:** 0 HOT updates in every run; for example 0 of 41,903 updates in the baseline 50-slot run, as the 0001 migration predicted. On the small table, autovacuum ran once per 60 s window and dead rows ended a window at most around 45,000. With a 1-million-row backlog, no autovacuum ran in the window and dead rows grew by about 41,000 a minute; the default trigger (20% of the table) is about 200,000 dead rows, so about 5 minutes (arithmetic).

**Fails halfway:** if autovacuum falls behind, dead rows pile up, the table and indexes grow, and every scan reads more pages. With a big standing backlog the fix is a lower `autovacuum_vacuum_scale_factor` for the `jobs` table only.

**Trade-off:** the partial indexes that make leasing fast are exactly what rules out HOT. Removing them would allow HOT updates but make every lease slower. The free space from `fillfactor = 70` mostly sits unused for HOT here; 70 against 100 was not measured, so the setting stayed.

## Q44. What did the 1-million-job backlog show about partial indexes, and how do you bound a cost that grows with the backlog?

Three parts: what stayed fast, what didn't, and the adaptive refresh.

**What:** partial indexes keep the lease fast with any backlog, because a lease reads only the front of its own queue. They don't make a *count* cheap: counting a million rows visits a million rows. Tasklane counts every queue's depth for backpressure, so that count was the cost to bound.

**How:**
1. **Lease:** the `jobs_available` partial index is ordered by queue, priority, run time. A lease reads the first k entries of its queue with `LIMIT` and `SKIP LOCKED`, so its cost does not depend on the backlog.
2. **Oldest ready age:** one `LIMIT 1` per priority on the same index: also constant.
3. **Count:** `count(*)` must visit every available row, and with most of the table available Postgres chose a sequential scan. So each count is O(backlog).
4. **Step 2:** count all queues in one grouped pass instead of one subquery per queue, and turn off JIT, which compiled the query to machine code for no gain.
5. **Step 3 (adaptive refresh interval):** after each count, wait 10 times as long as it took, clamped to 1–5 s. The gap between counts is the wait plus the count, so for counts of about 100–500 ms counting takes about 1/11 (9%) of one Postgres backend. A 20 ms count every 1 s is 2%. Above 500 ms the 5 s cap wins and the share rises again (a 1 s count would be 1/6); the cap exists because the server stops trusting counts older than 10 s.

**Numbers:** with the 1M backlog in another queue, the lease query took 0.31 ms on average against 0.24 ms on an empty table, and p99 was 46 ms against 47 ms. The count took 610 ms (JIT was 243 ms of one 710 ms run), then about 440 ms after step 2, once a second: 26.3 s of Postgres time per minute, p99 233 ms. After step 3: 12 counts a minute instead of 60, 5.2 s of Postgres time, p99 61 ms. With 1,000 queues, step 2 took the depth query from 50.8 s to 0.68 s. One weak spot remains: 10,000 delayed priority-1 jobs made each lease slower (0.37 → 1.58 ms), because the index puts them first and the lease steps over them.

**Fails halfway:** if a count fails, the next try comes after 1 s, and the server keeps its last good counts for up to 10 s, then fails open (refuses no enqueues) rather than refuse everything on old data.

**Trade-off:** backpressure is soft by up to one refresh interval: a queue can overshoot `max_depth` by up to 5 s of traffic instead of 1 s when counts are slow. The alternative, a counter row updated on every enqueue and ack, would make every write lock one hot row (rejected in §14.3).

## Q45. After moving NOTIFY out of the commit, each server woke its own waiting leases twice. Why, how did you fix it, and what happens during a rolling upgrade?

Four parts: why it happened, the fix, the numbers, and the upgrade.

**What:** Postgres delivers a NOTIFY to every connection that LISTENs on the channel, including the sender's own listener. The publishing server had already woken its own waiting leases in memory, so its own NOTIFY woke them a second time, about 5 ms later. Each wasted wake is one lease query per waiting long-poll that finds nothing.

**How:**
1. Each server makes a random 16-character token when it starts.
2. The publisher sends `pg_notify('tasklane_jobs', '<queue> <token>')`.
3. The listener splits the payload at the first space. Queue names cannot contain a space, so that split is unambiguous (a NUL byte can't be used: NOTIFY payloads can't hold one).
4. If the token is its own, it skips the notification. Otherwise it wakes the subscribers of that queue. A payload without a space (an older server) also wakes the queue.
5. Tests: one sends a notification with the listener's own token and expects no wake, then another server's token and expects a wake. An end-to-end test runs two listeners on Postgres: after one Publish, the other server is woken (so the NOTIFY was delivered), and the publisher's own lease was woken exactly once. Both tests fail with the skip turned off.

**Numbers:** at 5 jobs/s with 200 worker slots, lease queries went from 2,528 to 1,597 a minute (medians of three back-to-back runs), exactly the number before step 1. Postgres ran 75 instead of 106 queries a second and used 14.3% of one CPU instead of 17.4%. p99 didn't move (28 ms), because workers never noticed the extra wakes: lease requests per job stayed at 1.0.

**Fails halfway (rolling upgrade):** a new server still understands old untagged payloads. An old server reads `emails a1b2…` as a queue named that, which has no subscribers, so it wakes no one. Its waiting leases still find the jobs at their 1 s re-check. So mixing versions costs at most 1 s of extra latency on the old servers, for the length of the rollout, and loses nothing. That is the same guarantee as any lost notification.

**Trade-off:** the payload format is now a small protocol between servers, so a later change has to stay readable by the previous version. A more compatible alternative would be a second channel just for tagged payloads, but the 1 s re-check already makes a mixed rollout safe, so one channel keeps it simple. Comparing the NOTIFY's sender process ID against our own isn't possible: the publisher sends through pooled connections, not through the listener's connection.

## Q46. Why does gRPC need a layer-7 balancer in Kubernetes, and what did you choose?

Three parts: why a Service isn't enough, what Envoy does, and the alternatives.

**What:** a Kubernetes Service balances connections, and gRPC sends all of a client's calls over one long-lived HTTP/2 connection. So one pod gets all of that client's calls, and a new pod gets none until the client reconnects. Tasklane runs Envoy in the cluster, which picks a server pod for every request.

**How:**
1. A Service's ClusterIP is handled by kube-proxy, which picks a pod once per TCP connection. After that, every call on the connection goes to the same pod.
2. gRPC multiplexes every call as a stream on that one connection, and a client keeps it for hours. Long-poll Leases make it worse: each holds a request open for up to 30 s.
3. Envoy ends the client's HTTP/2 connection and opens its own to every server pod. For each request it picks the pod with the fewest requests in flight (`least_request`).
4. It finds the pods through the headless Service `tasklane-headless`, whose DNS name lists every ready server pod. Envoy re-reads it every 2 s and health-checks `/readyz` on each pod every 2 s.
5. Clients use the Service `tasklane`, which points at 2 Envoy pods. Both Envoy and the servers have a disruption budget.

**Numbers:** 2 servers, 2 Envoys; DNS re-read every 2 s; health checks every 2 s, down after 2 failures. In the Compose demo the same Envoy setup split traffic about 16 and 17 requests/s across two servers and used about 17 MiB. In Kubernetes a rolling restart of both servers at 50 jobs/s lost 0 requests in 3 runs.

**Fails halfway:** if an Envoy pod dies, or is simply restarted, the client connections it held are dropped: the chart has no admin drain for Envoy, so even a planned Envoy restart resets them (a known limitation, design §20.3). Clients reconnect and the Service sends them to the other Envoy. An Enqueue in flight fails, and a Lease long-poll that already leased jobs is cut, so those jobs come back after their visibility timeout. The follow-up is a graceful drain in Envoy's preStop and a grace period above the 30 s long-poll. SDK workers retry their Lease with backoff; a producer retries an Enqueue itself, with the same idempotency key so it can't create the job twice. The disruption budget keeps a voluntary eviction from taking both Envoys at once.

**Trade-off:** one more hop and one more component to run. The alternatives: client-side `round_robin` over the headless Service (every SDK must do it, new pods are seen only after a DNS refresh, which can lag about 30 s with CoreDNS's cache on kind, and plain HTTP clients get nothing), or a service mesh (a proxy next to every pod, about 300–500 MB on the laptop; an estimate, not measured). One shared Envoy covers gRPC and HTTP clients for about the cost of one small pod.

## Q47. A pod is deleted: why can requests fail, and how does the chart prevent it?

Three parts: the race, the timeline, and the rollout.

**What:** Kubernetes sends SIGTERM and removes the pod from the endpoints at the same time. Everything that routes traffic (kube-proxy, Envoy's DNS view) learns about the removal a little later. A server that stopped serving at SIGTERM would refuse requests that are still on their way to it.

**How:**
1. **0 s:** the pod is marked terminating. A `preStop` hook sleeps 5 s while the server keeps serving normally. The endpoints drop the pod, and Envoy stops sending to it at its next DNS read, every 2 s (plus the DNS cache time, see Q51).
2. **5 s:** SIGTERM. The server's existing drain marks itself not ready, ends waiting long-polls (the SDK retries them through Envoy on the other server), and gives HTTP up to 10 s, gRPC up to 10 s, metrics up to 2 s and the final trace flush up to 2 s.
3. **≤ 29 s:** the server exits. The grace period is 40 s, so there are 11 s spare. A chart test asserts preStop + drain < grace.
4. A rollout uses `maxUnavailable: 0` and `maxSurge: 1`: a new pod must pass `/readyz` before an old one stops, so there are always 2 ready servers.
5. Envoy drops a pod as soon as DNS drops it, and retries a failed connect on another server (Q51).

**Numbers:** a rolling restart of both servers under 50 jobs/s: 0 failed sends, 0 dropped, valid, in 3 runs out of 3, and again in the end-to-end test. Before the Envoy fix of Q51, the same test lost 170 and 123 sends.

**Fails halfway:** if the drain overruns, the kubelet sends SIGKILL at 40 s. Jobs that were leased on that server are not lost: their lease expires after the visibility timeout and the reaper makes them available again. A worker whose Ack call failed retries it; Ack is idempotent (the same token gets success again), so a retry is safe, and a late Ack from an old lease is rejected by the fencing token.

**Trade-off:** every pod deletion takes at least 5 s longer, and a full rollout of 2 servers is slower: in the tests it ended about 25–30 s after the load started (that includes a 5 s warm-up). A fixed sleep is a guess about how slow the rest of the cluster is: if DNS caching is longer than the sleep, the sleep alone isn't enough (Q51).

## Q48. KEDA vs a plain HPA, and what does scale-to-zero cost?

Three parts: what each does, the cost, and when to keep one pod.

**What:** a HorizontalPodAutoscaler scales on CPU, memory or custom metrics, but never below 1 pod. KEDA adds the step between 0 and 1 (activation) and feeds the HPA an external metric for 1 to N. Tasklane's demo workers scale from 0 to 5 on queue depth.

**How:**
1. KEDA polls Prometheus every 5 s for the waiting plus running jobs in queue `demo` (Q49).
2. With 0 pods, any job (`activationThreshold: 0`) makes KEDA set the Deployment to 1.
3. From 1 to N, KEDA creates an HPA with a target of 50 jobs per pod, capped at 5 pods. A 3,000-job burst asks for 60 pods, so the HPA jumps straight to 5.
4. Scale-down: the HPA waits out a 30 s stabilization window before going down to 1; KEDA goes to 0 after a 30 s cooldown with no jobs.

**Numbers:** a 3,000-job burst, medians of 3 on the laptop: the first worker started 8 s after the burst (5–9 s), the queue was empty at 77 s with 5 pods (about 43 jobs/s, against the 45 estimated), and the workers were back to 0 31 s after that. A warm worker that is already long-polling starts a job in about 30 ms. So scale-to-zero costs the first job about 8 s: scrape (≤ 5 s) + poll (5 s) + pod start.

**Fails halfway:** if Prometheus or the metric is gone, KEDA's Prometheus scaler by default reads the empty result as 0 (`ignoreNullValues` is true). So it is expected to add no workers and to scale the running ones down, each draining first; this was inferred from that default, not tested. Jobs wait in the queue but are not lost. A worker removed by a scale-down finishes or nacks its jobs within its 35 s grace period, so none waits for a lease timeout.

**Trade-off:** scale to zero saves the pods' memory and CPU on a quiet queue, at the price of seconds of latency for the first job. Keep a minimum of 1 for queues where a job must start within a second (a password-reset email); use 0 for batch work and quiet queues.

## Q49. Why does the scaling signal count leased jobs, and why `max by (state)`?

Two parts: which jobs to count, and how to read a gauge that two servers export.

**What:** the signal is `sum(max by (state) (tasklane_queue_depth{queue="demo", state=~"available|leased"}))`: waiting plus running jobs, each counted once.

**How:**
1. **Count leased jobs:** with only `available`, a burst that the workers have fully leased reads 0 while 50 jobs are still running. KEDA would then scale to 0 mid-job. Each removed worker drains (it finishes or nacks its jobs within 27 s), so work is delayed, and the nacked jobs run again.
2. **`max by (state)`:** both servers read the same Postgres and export the same gauge. `sum` would count every job twice. `max by (state)` keeps one value per state, then `sum` adds available and leased.
3. **Threshold:** 50 jobs per pod. A pod runs 10 jobs at once and a demo job takes about 1.1 s, so a pod does about 9 jobs/s, and 50 jobs is about 5 s of work per pod.
4. A chart test checks that the query names only metrics the server really exports.

**Numbers:** in the 3,000-job bursts, workers went back to 0 only after the queue was empty: 31 s later (median), from the 30 s HPA window and the 30 s KEDA cooldown. 5 pods drained about 43 jobs/s, about 8.6 per pod against the 9 estimated.

**Fails halfway:** if one server stops reporting, `max` still takes the other server's value, so the signal stays right. If both stop, the result is empty and KEDA by default reads 0 (Q48; not tested). The gauge is refreshed every 1–5 s and scraped every 5 s, so the signal is up to about 10 s old; scale-down waits 30 s, which covers that.

**Trade-off:** counting running jobs keeps some workers while jobs still run, so they scale down a little later. It does not keep all of them until the last job ends: the HPA wants ceil((available + leased) / 50) pods, so it can go from 5 to 1 while 40 jobs still run. That is safe because a removed worker finishes or nacks its jobs within 27 s. The other way, scaling on waiting jobs only, is cheaper on paper but kills workers in the middle of jobs. A per-pod target also ignores job length: with longer jobs, 50 per pod is more seconds of work, so the threshold should follow the job time.

## Q50. PodDisruptionBudget vs `maxUnavailable` in a rollout: which protects what?

Three parts: the rollout, the budget, and what neither covers.

**What:** `maxUnavailable` in the Deployment strategy limits the Deployment's own rollouts. A PodDisruptionBudget limits voluntary evictions started by someone else: a node drain, a cluster upgrade, the cluster autoscaler. Neither protects against a node crash.

**How:**
1. **Rollout:** the server Deployment has `maxUnavailable: 0`, `maxSurge: 1`. To replace pods it starts a third one, waits for its `/readyz`, then stops an old one, so 2 servers are always ready.
2. **Budget:** the chart has a PDB with `maxUnavailable: 1` for the servers and one for Envoy. `kubectl drain` evicts through the Eviction API, which refuses an eviction that would break the budget, so a drain removes one server at a time and waits until a replacement is ready.
3. A rollout does not go through the Eviction API, so the PDB does not limit it; the rollout's own `maxUnavailable` does.
4. **Not covered:** a node that crashes or loses power is an involuntary disruption. Only having replicas on other nodes helps there.

**Numbers:** 2 replicas, PDB `maxUnavailable: 1`, rollout `maxUnavailable: 0` and `maxSurge: 1` (3 pods for a moment). A rolling restart of both servers ended about 25–30 s after the load started (including a 5 s warm-up) and lost 0 requests. A node drain was not measured: the laptop cluster has one node.

**Fails halfway:** with both pods on one node (the laptop), a drain cordons the node, evicts one server, and the replacement cannot start anywhere else. The second eviction would break the budget, so the drain keeps retrying it, forever unless `--timeout` is set. On a real cluster the replacement starts on another node and the drain goes on.

**Trade-off:** a budget can block maintenance: with `minAvailable` equal to the replica count, no drain can ever finish. `maxUnavailable: 1` with 2 replicas lets maintenance proceed but leaves one server during a drain. The chart sets no anti-affinity or topology spread, so on a multi-node cluster both servers can still land on one node; add a spread constraint so a node crash takes only one.

## Q51. The rolling-restart test lost requests: why, how did you find it, and what fixed it?

Four parts: the symptom, how the cause was found, the two fixes, and the numbers.

**What:** the first rolling restarts under 50 jobs/s lost 4–6% of sends. The test on my own project found it, not a user. Envoy kept sending requests to a server pod that had already exited, because by default it keeps a host that left DNS while the host still passes health checks.

**How:**
1. **Symptom:** every failure was the same: `Unavailable: upstream connect error or disconnect/reset before headers. reset reason: connection timeout`, which is Envoy's 1 s connect timeout. A connect to an IP that no longer exists gets no reply, so it times out instead of failing fast.
2. **Finding the cause:** I ran a restart with an observer that polled the pods and the headless EndpointSlice every 0.3 s, and lined up its timeline with loadgen's errors. The old pod's endpoint was already `ready=false, terminating=true` (out of DNS) when the pod started terminating. The pod completed 5.3 s later, right after the 5 s preStop, because its drain had nothing left to do. The first failed send came less than a second after that.
3. **Cause:** Envoy's default `ignore_health_on_host_removal: false`. The pod passed `/readyz` for the whole preStop, so Envoy kept it after DNS dropped it, until two health checks failed 2–4 s after it exited. All 3 tries of a request could hit the same dead pod, or the other old pod, which died about 1.5 s later. The design assumed the next DNS refresh stops traffic; it doesn't, by default.
4. **Fix 1:** `ignore_health_on_host_removal: true`: drop a pod as soon as DNS stops listing it, even if its last health check passed.
5. **Why some still failed:** CoreDNS caches answers for 5 s on k3s. With Envoy's 2 s refresh, a gone pod can stay in Envoy's view about 7 s, longer than the 5 s preStop.
6. **Fix 2:** the retry host predicate `previous_hosts`: a retried connect goes to a different server. Retries still apply only to requests that never reached a server, so a Lease that did is never retried by the proxy.
7. A chart test asserts both settings, so they can't silently go away.

**Numbers:** before: 170 and 123 failed sends out of about 3,000 (50 jobs/s for 60 s). Fix 1 alone: 10, 0, 0 (the 10 were also connect timeouts, about 6 s after the delete). Fix 1 and 2: 0, 0, 0, and 0 again in the end-to-end test. On kind (30 s DNS cache), with the panic threshold at 0 as well: 0, 0, 0. No timing was changed: preStop 5 s, grace 40 s, connect timeout 1 s, 2 retries.

**Fails halfway:** if all 3 tries land on gone pods (only 2 servers, both old pods replaced about 1.5 s apart), the client gets `Unavailable`: SDK workers retry with backoff, and a producer retries the Enqueue with the same idempotency key. On kind, CI's cluster, CoreDNS caches for 30 s, so the window is about 32 s; the retry on another server and the health checks (a gone pod is marked down 2–4 s after it exits, and `healthy_panic_threshold: 0` keeps Envoy from routing to it even when under half the hosts are healthy) cover it. Measured on kind locally: 0 failed sends in 3 restarts out of 3. If CI ever shows connect timeouts there, the next lever is a longer preStop.

**Trade-off:** trusting DNS over health checks means a pod that drops out of DNS for a moment, for example a readiness blip, loses traffic at once, even though it still works. The other fix, a preStop longer than DNS cache plus refresh, would cost every pod deletion 30 s or more on kind. Dropping on removal plus retrying elsewhere keeps the preStop at 5 s.
