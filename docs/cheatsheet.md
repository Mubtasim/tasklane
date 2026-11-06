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
| Stack RAM | ~0.6-1 GiB with Jaeger (`docker stats`: Grafana 215-700, Jaeger ~240, Postgres 55, Prometheus 32, Envoy 19, each server ~15); see Tracing |
| Servers | 2 behind Envoy; killing one: acked rate held at ~9.3–9.7 jobs/s, `tasklane-2` went from ~16 to ~33 requests/s |

## Tracing

| What | Value | Why |
|---|---|---|
| Spans per normal job | 12 | demo produce, Enqueue client + server, BEGIN/INSERT/SELECT/COMMIT, queue wait, handle, Ack client + server, UPDATE. Counted in Jaeger: 20 of 20 normal jobs had 12. |
| Retried job | 20 spans for 2 attempts; +~8 per extra attempt | Each attempt adds a wait span, a handle span, Nack or Ack client + server and their Postgres spans. |
| `Lease` traces | ~12/s across 3 workers, 5-15 spans each | A lease starts before any job is known, so it is its own short trace. |
| Demo span rate | ~200 spans/s (about 10 jobs/s × ~13 spans, plus ~85/s of `Lease`) | An estimate from counting one minute in Jaeger, not a counter. |
| Jaeger memory | 96 MiB at 2 min, 179 at 6, 226 at 9, 239 at 12-16, 226.5 at 19-20 | In-memory store capped at 10,000 traces (~8 min at ~22 traces/s); flat at ~225-240 (239 at 12-16 min, anon 216; 226.5 at 19-20 min by `docker stats`) once the cap filled. |
| Whole stack `docker stats` | ~615 MiB at 12 min after a Jaeger restart (~1 GiB earlier) | Grafana ranges 215-700 MiB because docker stats counts its file cache (cgroup anon ~285 MB); servers ~15 MiB each, workers ~9 MiB. |
| Sampler | `parentbased_traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0.1` for production; the demo uses 100% | The producer decides once; everyone follows the `sampled` flag. |
| Propagation | W3C `traceparent`, explicit | The global propagator is a no-op unless the app sets one. |
| Not traced | `Heartbeat` RPCs, `/healthz`, `/readyz`, reaper, pruner, depth refresh | Noise; metrics cover them. |
| Tail-sampling buffer | span rate × trace duration × span size: 1,000 jobs/s × 12 spans × ~1 KB × 120 s ≈ 1.4 GB | Arithmetic, not measured. |
| `Enqueue` benchmark | `main` 926 µs, off 959 µs (+3.5%), 100% sampled 991 µs (+3.3% over off, noise) | 2-core laptop, demo running, 6 × 2,000 calls, `benchstat`. Off vs `main` is +2.9% when no trace context is sent. |

## Envoy (two servers)

| What | Value | Why |
|---|---|---|
| Route timeout | 35 s | Longer than the server's `MaxWait` (30 s) and the SDK's 20 s long-poll. Envoy's 15 s default would cut every idle Lease. |
| Balancing | `least_request`, per request | Long-polls hold a request for up to 30 s; an L4 balancer would pin each client's one HTTP/2 connection to one server. |
| Health check | `GET /readyz` every 2 s, unhealthy after 2 failures, healthy after 1 | `/readyz` also fails when the database is unreachable. Two failed checks take ~4 s, but the measured failover was longer (below). |
| Outlier ejection | 3 consecutive gateway errors → ejected for 10 s | Reacts to real traffic before the health check does. |
| Connect timeout | 1 s (default 5 s) | A killed container's IP never answers. |
| Proxy retries | 2, only `connect-failure`, `refused-stream`, `reset-before-request`; on another server (`previous_hosts`, since M5b) | A Lease that reached a server may have leased jobs, so it is never retried by the proxy. |
| DNS re-resolution (Compose) | every 5 s (`STRICT_DNS` default) | The Compose setup. In Kubernetes the chart re-reads DNS every 2 s (table below). |
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

## Load test (M5a)

Laptop, 2 cores / 4 threads, pinned: Postgres on one core (2 threads), the server on 1 thread, the load generator on the other thread of the server's core. Full report: `docs/benchmarks.md`.

| What | Value | Why |
|---|---|---|
| Max full cycle (enqueue → lease → ack) | **433 jobs/s** (median of 4; 392 before tuning) | Runs vary ±20%, so medians. Limit: the per-commit WAL flush, not CPU. |
| Max enqueue (after step 1) | 1,048 jobs/s one per call (median; 747 before); 5,891 jobs/s in batches of 100 (one run; 5,934 before) | Batching: 100 jobs share one transaction and one commit. |
| CPU per job at 80% load | server **1.29 ms**, Postgres **2.87 ms** | Portable number: `max jobs/s ≈ CPUs × 1000 / CPU ms per job`. |
| CPU ceilings here | server 1 × 1000 / 1.29 ≈ 775 jobs/s; Postgres 2 × 1000 / 2.87 ≈ 697 jobs/s | Both above the measured 433, so waits, not CPU, set the limit. |
| Servers per Postgres | 2.87 / 1.29 ≈ 2.2 Postgres CPUs per server CPU | 8-vCPU Postgres: ≈ 2,790 jobs/s CPU ceiling, needs ≈ 4 server CPUs (arithmetic). |
| p99 enqueue-to-start at 50 / 80 / 95% | 34 / **61** / 97 ms at 216 / 346 / 411 jobs/s | Before tuning: 837 ms at 95% (334 jobs/s). |
| Idle cost, 200 slots | **9.6 Postgres queries/s**, 5.8% of one CPU; server 0.45% | 4 waiting long-polls re-check once a second, plus depth refresh and reaper. |
| Lease requests per job | about 1.0 or less (0.29–0.89 under load) | `LeaseStream` trigger not hit. |
| The bottleneck found | `Lock:object` 31% of wait samples (55% enqueue-only) | NOTIFY's database-wide commit lock; fixed by notifying after commit. |
| Double wake (after step 1) | lease queries at 5 jobs/s: 2,528 → **1,597**/min; Postgres CPU 17.4 → 14.3% | NOTIFY payload `<queue> <sender>`; a server skips its own (already woken in memory). |
| 1M backlog | depth count ~440 ms; refresh every 10 × count (1–5 s): 26.3 → 5.2 s of Postgres time per minute, p99 233 → 61 ms | A count is O(backlog) even with partial indexes. |
| 1,000 queues | depth query 50.8 s → 0.68 s (one grouped pass) | One subquery per queue was 1,000 counts. |
| HOT updates | 0 of 41,903 | Every state change touches an indexed column. |
| Tracing at 100% | +38% server CPU per job | Sample a ratio in production. |
| 200 slots vs 50 | 293 vs 352 jobs/s | Ack `UPDATE` 0.32 → 2.75 ms: more slots, more contention. |

## Kubernetes (M5b)

k3d on the laptop, one node. Full results: design §20.6.

| What | Number | Why |
|---|---|---|
| Server pod shutdown | preStop 5 s → drain ≤ 24 s (HTTP 10 + gRPC 10 + metrics 2 + traces 2) → exit ≤ 29 s → grace 40 s | Endpoints drop the pod at the same time as SIGTERM; keep serving until Envoy (DNS every 2 s) stops sending. |
| Envoy and removed pods | `ignore_health_on_host_removal: true`; retry connects on another server (`previous_hosts`) | By default Envoy keeps a pod that left DNS while it passes health checks, and a terminating pod passes `/readyz` until it exits. |
| DNS cache | CoreDNS 5 s on k3s, 30 s on kind | A removed pod stays in Envoy's view up to cache + 2 s: ~7 s on k3s, ~32 s on kind, longer than the 5 s preStop. Retries on another server, health checks and `healthy_panic_threshold: 0` cover the gap: 0 failed sends in 3 restarts on each. |
| Restart bug, before → after | 170 and 123 failed sends (4–6%) → 10, 0, 0 (fix 1) → 0, 0, 0 (fix 2) | Every failure was Envoy's 1 s connect timeout to a pod that had exited. |
| Worker pod grace | 35 s | SDK ShutdownTimeout 25 s + 2 s to nack the rest. |
| Rollout | maxUnavailable 0, maxSurge 1 | A new pod is ready before an old one stops. |
| KEDA signal | available + leased jobs, `max by (state)` | Counting only waiting jobs reads 0 while workers still run jobs. |
| KEDA rule | 50 jobs per pod, 0–5 pods, poll 5 s, cooldown 30 s | Burst of 3,000 → capped at 5 pods ≈ 45 jobs/s (measured ≈ 43). |
| Scale from zero | 8 s to the first worker (median; 5–9 s) | Scrape ≤ 5 s + poll 5 s + pod start 2–5 s. |
| 3,000-job burst | drained 77 s after the burst with 5 pods (median) | ≈ 5.5 min with one pod (≈ 9 jobs/s per pod). |
| Scale back to 0 | 31 s after the queue empties (median) | 5 → 1 after the HPA's 30 s window, 1 → 0 after KEDA's 30 s cooldown. |
| Rolling restart under 50 jobs/s | 0 failed sends (3 of 3 runs) | preStop + readiness + Envoy dropping removed pods + retries on another server. |
| Demo cluster memory | ~0.9 GB (k3s container 882 MiB idle, `docker stats`) | KEDA, Postgres, Prometheus, 2 servers, 2 Envoys in one container. `demo.sh up`: 2 min 59 s. |

## Replication and Raft (phase 2)

Phase 2 replaces the single PostgreSQL behind the `Store` interface with `RaftStore`: a small purpose-built database (the jobs and queues in memory, a log and snapshots on disk) that runs as several copies kept identical by the Raft consensus algorithm (design §12). Raft itself stores nothing; it only makes every copy apply the same changes in the same order.

| Word | Meaning |
|---|---|
| Node | One running copy of the program; in Raft, one member of the group, usually one per machine. |
| Cluster | The nodes working together as one system: "a 3-node cluster". |
| Replica | A copy of the data; every Raft node holds a full one. |
| Replication | Keeping the replicas the same by sending each change from the leader to the others. |
| Leader | The one node that accepts writes and decides the order of changes; exactly one per term. |
| Follower | Receives changes from the leader and stores them; doesn't accept writes. |
| Candidate | A follower that stopped hearing the leader and asks for votes to replace it. |
| Term | An election round number. A higher term always wins, so a term works like a fencing token for the whole cluster. |
| Log, entry | The ordered list of changes ("enqueue job 1", "lease job 1 to worker A"); one change is an entry. |
| Commit | An entry is committed once a majority has it on disk; from then on it is never lost. |
| State machine | The data you get by applying committed entries in order: here, the jobs and queues. |
| Majority, quorum | More than half of the nodes; needed to elect a leader and to commit anything. |
| Snapshot | A saved copy of the whole data at entry N, so the log before N can be deleted. |
| Partition | The network splits, so some running nodes can't reach the others. |
| Split brain | Two nodes both acting as leader and accepting different writes; majorities and terms prevent it. |
| Failover | A new leader taking over after the old one dies. |
| Fault tolerance | How many nodes can fail while the system keeps working. |
| Kubernetes node vs pod | In Kubernetes a node is a machine and a pod is a running program, so 3 Raft nodes are 3 pods, ideally on 3 different Kubernetes nodes. |

| What | Value | Why |
|---|---|---|
| Nodes needed to survive f failures | 2f + 1 | A majority must stay alive. |
| 1 / 2 / 3 / 4 / 5 nodes | survive 0 / 0 / 1 / 1 / 2 failures | Majority is 1 / 2 / 2 / 3 / 3. Two nodes are worse than one, and four are no better than three, so clusters are odd-sized. |
| Why a majority, not "whoever is alive" | Two majorities of the same group always share a node | Only one side of a split can have a majority, so only one side accepts writes; the shared node carries every committed entry into the next term. |
| Leader heartbeat | ~50 ms | Followers notice a dead leader quickly. |
| Election timeout | random 150–300 ms | Random, like retry jitter, so two followers rarely run for election at once. |
| Write latency | ≈ 2–6 ms in one datacenter | One round trip to the faster follower (~0.5 ms) plus a disk sync (1–5 ms). |
| Failover time | ≈ 0.3–1 s | Election timeout plus one voting round. |
| Throughput ceiling | etcd: roughly 10k–30k writes/s with batching on good SSDs | Every write goes through the one leader. |
| Patroni (HA PostgreSQL) | uses etcd, a Raft store, to choose the Postgres leader | Raft already sits under highly available Postgres in practice. |
