# Tasklane numbers

The numbers worth knowing by heart, and why each one is what it is. Read it before an interview; every answer about Tasklane gets better with one of these in it.

## Leases

| What | Value | Why |
|---|---|---|
| Visibility timeout (queue default) | 30 s (demo: 15 s) | How long a lease lasts without a heartbeat; a crashed worker's job comes back after this. |
| Heartbeat interval | visibility timeout ÷ 3 (demo: 5 s) | Two heartbeats can be lost before the lease expires. |
| Heartbeat extends the lease to | now + visibility timeout | Measured on the database clock, so worker clocks don't matter. |
| Lost-lease detection delay | up to vt ÷ 3 (demo: 5 s) | The worker only learns at its next heartbeat; both workers may run the job meanwhile. |
| Reaper interval | 5 s (demo: 2 s) | Worst case a crashed worker's job is back after vt + reaper interval + backoff. |

## Leasing and waking

| What | Value | Why |
|---|---|---|
| Long-poll wait (SDK) | 20 s, call deadline 30 s | Server caps `wait` at 30 s; the extra 10 s covers the response. |
| Re-check while waiting | every 1 s | Delayed jobs, retries and lost NOTIFYs need no notification. Cost: 1 query per waiting lease per second. |
| Long-polls per worker process | 1 (sized to free slots) | 10 workers × 20 slots = 10 waiting leases, not 200 (10 queries/s instead of 200 when idle). |
| Max jobs per lease | 50 | Keeps the largest response under the message limit. |
| Listener health ping | after 30 s of silence, 5 s timeout | Detects half-open LISTEN connections. |

## Retries and failure

| What | Value | Why |
|---|---|---|
| Max attempts (queue default) | 10 (demo: 5) | After the last failure the job is `dead` (DLQ). |
| Job retry backoff | full jitter in [0, min(1 h, 1 s · 2^attempt)) | Attempt 1 → up to 2 s, 2 → 4 s, 3 → 8 s, 4 → 16 s. A 5-attempt poison job is dead about 15 s after its first run on average (≤ 30 s). |
| SDK backoff on transient errors | full jitter, 1 s base, 30 s cap | For Lease/Ack/Nack when the server is unavailable. |
| Ack/Nack retries | until the local lease deadline | After that the lease is gone anyway; the job will be re-run. |
| Fatal SDK errors (no retry) | NOT_FOUND, UNAUTHENTICATED, PERMISSION_DENIED, INVALID_ARGUMENT | Retrying can't fix them. FAILED_PRECONDITION on Ack/Nack = lease lost, never retried. |
| Attempts counted | at lease | A crash or shutdown uses up an attempt. |

## Shutdown

| What | Value | Why |
|---|---|---|
| Worker `ShutdownTimeout` | 25 s | Running jobs get this long to finish; the rest are cancelled and nacked. |
| Shutdown Nack / in-flight Ack budget | 2 s (shared) | Worst case Run returns at ShutdownTimeout + ~2 s. |
| Platform grace period | must be > 27 s (compose: 30 s; Kubernetes default: 30 s; Docker default: 10 s!) | Otherwise SIGKILL arrives before the Nack and recovery waits for lease expiry. |
| What the Nack saves | ~one visibility timeout | Speed, not correctness: lease expiry recovers the job either way. |

## Limits

| What | Value | Why |
|---|---|---|
| Payload | 256 KiB, measured in protobuf bytes | Measure a limit in the units it's enforced in (JSON was 3.7× off). |
| gRPC message size | 16 MiB both ways | 50 jobs × 256 KiB plus overhead always fits. |
| Max queue depth (default) | 100,000 | Enqueue beyond it → backpressure (429 / RESOURCE_EXHAUSTED). |
| Queue name | `[A-Za-z0-9._-]{1,128}` | URL-safe. |
| Nack error text | truncated to 4 KiB | |

## Live stats

| What | Value | Why |
|---|---|---|
| Completed window | last 10 s | A rate, read through the `finished_at` B-tree: ~100 rows at 10 jobs/s, whatever the table size. |
| tasklane-watch refresh | 1 s | |

## Demo

| What | Value |
|---|---|
| Producer | 10 jobs/s: 85% normal (0.2–2 s), 12% flaky (fails 30%), 3% poison |
| Workers | 3 × 5 slots = 15 slots, ~15 jobs/s capacity |
| One worker | ~4–5 jobs/s, so the backlog grows ~6/s (Little's law: throughput = slots ÷ average job time) |
| Stack RAM | ~700 MB idle (measured ~690 MiB: Grafana 484, Prometheus 98, Postgres 50, Envoy 17, each server ~11) |
| Servers | 2 behind Envoy; killing one: acked rate held at ~9.3–9.7 jobs/s, `tasklane-2` went from ~16 to ~33 requests/s |

## Envoy (two servers)

| What | Value | Why |
|---|---|---|
| Route timeout | 35 s | Longer than the server's `MaxWait` (30 s) and the SDK's 20 s long-poll. Envoy's 15 s default would cut every idle Lease. |
| Balancing | `least_request`, per request | Long-polls hold a request for up to 30 s; an L4 balancer would pin each client's one HTTP/2 connection to one server. |
| Health check | `GET /readyz` every 2 s, unhealthy after 2 failures, healthy after 1 | `/readyz` also fails when the database is unreachable. Two failed checks take ~4 s, but the measured failover was longer (below). |
| Outlier ejection | 3 consecutive gateway errors → ejected for 10 s | Reacts to real traffic before the health check does. |
| Connect timeout | 1 s (default 5 s) | A killed container's IP never answers. |
| Proxy retries | 2, only `connect-failure`, `refused-stream`, `reset-before-request` | A Lease that reached a server may have leased jobs, so it is never retried by the proxy. |
| DNS re-resolution | every 5 s (`STRICT_DNS` default) | |
| Failover after `kill tasklane-1` | ~14–18 s of fast `Unavailable` errors on new calls, then none | Most likely health checks (2 × 2 s) plus DNS re-resolution (5 s); not proven. The SDK retries, so throughput held. |

## Correctness tests

| What | Value | Why |
|---|---|---|
| Invariant test VT / max attempts | 2 s / 3 | Short, so zombies and reaps happen within seconds. Reaper every 200 ms, retry backoff 100 ms–1 s. |
| Invariant test sizes | 10 workers × 1,000 jobs (~8 s; default, every PR) / 50 × 10,000 (`-invariant.full`, ~1m17s, ~380 MB) | 5 slots per worker. |
| Invariant job mix | 85% normal (5–25 ms), 12% flaky (1–2 failures), 3% poison | Plus 2 zombies (ack after VT + 500 ms) and 2 workers cut off at 30% and 60%. |
| Rule 4 tolerance | 500 ms (in-process), 1 s (chaos, through Envoy) | The recorded start trails the server's lease time. |
| Chaos VT | 10 s | Reaps happen within the 3-minute run; a 15 s pause outlasts it. |
| Chaos faults | one every ~15 s for 3 min: kill worker (back after 10 s), kill server (back after 15 s), pause worker 15 s, restart Postgres | One fault at a time, never both servers down. |
| Chaos producer | 10 jobs/s, 5 s per enqueue attempt, retry after 200 ms with the same key | Records a key only after the server confirms it. |
| Chaos drain | ≤ 2 min | Then rules 1–4. |
| Chaos runs | seed 1: 1,889 enqueued, 1,827 completed, 62 dead, 16 lease_lost, 3m42s; seed 1793568724465150275 (4 server kills): 1,654 / 1,610 / 44, 5 lease_lost, 4 gave_up, 3m52s | Both PASS. |
| The counter bug | ~1 run in 60 failed rule 5; 0 in 180 after the fix | A cancelled client could make a committed write look failed. |
| `storeWriteTimeout` | 5 s | Store writes run detached from the client; below the 10 s graceful-stop budget. |
