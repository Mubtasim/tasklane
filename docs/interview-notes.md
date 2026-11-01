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

**Fails halfway:** a server is killed mid-request. New calls failed fast with `Unavailable` for about 14–18 s, most likely until two failed health checks and the 5 s DNS re-resolution dropped the dead host (not proven; keepalive and a `previous_hosts` retry rule didn't shorten it). The SDK retries. A Lease that reached the dead server isn't retried by the proxy, because it may have leased jobs; those wait out their visibility timeout.

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

**Numbers:** 5 s is far above a healthy write (milliseconds) and below the 10 s graceful-stop budget, so a detached write can't hold a drain past it. Verified with handler tests that call every write with an already-cancelled context (all failed before the fix), then 180 clean invariant runs (30 with `-race`, 150 without). The old code would pass 180 in a row only about 5% of the time.

**Fails halfway:** remaining edges. A write that commits exactly at the 5 s timeout can still be reported as `DeadlineExceeded`. A Lease whose client is gone before its first attempt still leases; the jobs are counted but wait out their visibility timeout. A client deadline shorter than the write no longer cuts the write short.

**Trade-off:** the server now finishes work for clients that left, up to 5 s per call, in exchange for counters and responses that always match what was committed. The general lesson: a cancelled context means "the caller stopped waiting", not "the database did nothing". Any write whose outcome you record must outlive the request that started it, or check the database afterwards.
