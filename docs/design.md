# Tasklane design

**Status:** draft v1 · **Author:** Mubtasim Shahriar · **Date:** 2026-10-17

Tasklane is a self-hosted, language-agnostic job queue server backed by PostgreSQL. Producers enqueue jobs and workers lease, run and acknowledge them over gRPC or HTTP/JSON. It gives at-least-once delivery, retries with exponential backoff, a dead-letter queue, delayed jobs and priorities, and it exposes Prometheus metrics.

## 1. Background

Some work should not happen inside a request: it is slow (PDFs, video), unreliable (third-party APIs, email providers) or must survive crashes (payments, webhooks). The standard answer is a job queue. The request records a job and returns. Separate worker processes pick jobs up, do the work and report back.

Most Postgres-backed queues (River for Go, Oban for Elixir, Graphile Worker for Node) are libraries tied to one language. Teams with a mixed stack that already run Postgres and don't want Redis, Kafka or a cloud dependency have no good self-hosted "one queue for all our languages" option. Tasklane is that option: a small server in front of the Postgres you already have, with thin SDKs for Go, TypeScript and C#.

Key concepts used throughout this document:

- **Lease and visibility timeout.** A worker doesn't remove a job when it takes it; it *leases* it for a period (default 30 s). If the worker crashes, the lease expires and the job becomes available again. Long jobs extend their lease with heartbeats.
- **At-least-once delivery.** A worker can finish the work and crash before acknowledging, so the job runs again. Exactly-once delivery is impossible across a network. Tasklane promises at-least-once and gives tools (job IDs, idempotency keys) to make a repeat harmless.
- **Idempotency key.** A producer-chosen key, unique per queue. Retrying an enqueue with the same key returns the original job instead of creating a duplicate.
- **Backoff with jitter.** Failed jobs retry after growing, randomised delays so a struggling dependency isn't hammered and retries don't arrive in lockstep.
- **Dead-letter queue (DLQ).** Jobs that fail `max_attempts` times are parked for a human instead of retrying forever.
- **Backpressure.** When producers outpace workers, the server refuses new work with a retryable error instead of growing without bound.
- **Fencing token.** Every lease carries a fresh token, and only the current token may complete the job. This stops a stalled worker whose lease expired from completing a job that another worker now owns.

## 2. Goals and non-goals

**Goals (v1)**
- Durable jobs: an acknowledged enqueue is never lost while Postgres is intact.
- At-least-once delivery, with duplicates only after a lease expiry or a crash.
- Retries with exponential backoff and full jitter; a DLQ with inspect and redrive.
- Delayed jobs (`run_at`) and four strict priority levels (1 critical … 4 low; 0 means unspecified and is stored as 3, the default).
- Idempotent enqueue.
- Backpressure on both the producer and the worker side.
- Stateless servers: run one or many copies behind a load balancer.
- gRPC API with an HTTP/JSON gateway generated from one `.proto` file; SDKs for Go, TypeScript and C#.
- Prometheus metrics, structured logs, OpenTelemetry trace propagation and health endpoints.
- A published load-test report with the measured bottleneck and the tuning applied.

**Non-goals (v1)**
- Exactly-once processing.
- Per-queue rate or concurrency limits (planned for v1.1).
- Cron and recurring jobs (planned with the Raft storage, which provides leader election).
- Multi-step workflows or job dependencies (use Temporal for that).
- Multi-tenant auth and a web UI (static API keys and a Grafana dashboard instead).

## 3. Architecture

```
 Producer (Go / TS / C# SDK)                    Worker (Go / TS / C# SDK)
   │ Enqueue                                       ▲ Lease / LeaseStream → Ack / Nack / Heartbeat
   ▼                                               │
 ┌──────────────────────── tasklane server (Go), N copies ────────────────────────┐
 │ gRPC API  ·  HTTP/JSON gateway (grpc-gateway)  ·  /metrics  /healthz  /readyz   │
 │ service layer: validation, backoff policy, backpressure                         │
 │ background loops: reaper (expired leases) · pruner (old completed jobs)         │
 │ Store interface ───────────────▶ PostgresStore (v1)   RaftStore (phase 2)       │
 └───────────────────────────────────────┬────────────────────────────────────────┘
                                         ▼
                                     PostgreSQL
```

**Approach: stateless servers, Postgres is the source of truth.** Every lease is a single transaction using `SELECT … FOR UPDATE SKIP LOCKED`. Servers hold no job state in memory, so any copy can serve any request and a crashed server loses nothing. Idle workers don't poll in a tight loop: servers `LISTEN` on a Postgres channel, the enqueue path sends `NOTIFY`, and waiting long-polls and streams are woken.

**Alternatives considered**
- *Single leader with in-memory queues, Postgres only for persistence.* Much higher throughput, but it needs leader election and failover in v1. That is the Raft phase's job, so it is deferred rather than rejected (see §12).
- *Partitioned log (Kafka-style offsets).* A poor fit for per-job ack, retries and delays; it is a different product.

**The `Store` interface** is the seam for phase 2. The service layer depends only on it:

```go
type Store interface {
    Enqueue(ctx context.Context, jobs []NewJob) ([]EnqueueResult, error)
    Lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]Job, error) // Job carries LeaseToken and LeaseExpiresAt
    Ack(ctx context.Context, id int64, token uuid.UUID) error
    Nack(ctx context.Context, id int64, token uuid.UUID, errMsg string, retryAt *time.Time) error
    Heartbeat(ctx context.Context, id int64, token uuid.UUID, ttl time.Duration) (time.Time, error)
    ReapExpired(ctx context.Context, limit int) (int, error)
    CreateQueue(ctx context.Context, q Queue) error
    GetQueue(ctx context.Context, name string) (Queue, error)
    QueueStats(ctx context.Context, name string) (QueueStats, error) // live counts (§14)
    GetJob(ctx context.Context, id int64) (Job, error)
    // plus Cancel, ListDead, Redrive, PruneCompleted later
}
```

## 4. Data model

```sql
CREATE TABLE queues (
    name               text PRIMARY KEY,
    max_depth             bigint  NOT NULL CHECK (max_depth > 0),   -- backpressure threshold (available + leased)
    visibility_timeout_ms bigint  NOT NULL CHECK (visibility_timeout_ms > 0),
    max_attempts          int     NOT NULL CHECK (max_attempts > 0),
    paused                boolean NOT NULL DEFAULT false,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TYPE job_state AS ENUM ('available', 'leased', 'completed', 'dead', 'cancelled');

CREATE TABLE jobs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue            text      NOT NULL REFERENCES queues(name),
    kind             text      NOT NULL,
    payload          jsonb     NOT NULL DEFAULT '{}',
    priority         smallint  NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 4), -- 1 critical … 4 low; 0 = unspecified, stored as 3
    state            job_state NOT NULL DEFAULT 'available',
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int       NOT NULL DEFAULT 0,
    max_attempts     int       NOT NULL,
    last_error       text,
    lease_token      uuid,
    lease_expires_at timestamptz,
    idempotency_key  text,
    trace_context    text,                                 -- W3C traceparent from the producer
    created_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- Leasing: the next runnable job by priority, then time, then id
CREATE INDEX jobs_available ON jobs (queue, priority, run_at, id) WHERE state = 'available';
-- Reaper: expired leases
CREATE INDEX jobs_leased ON jobs (lease_expires_at) WHERE state = 'leased';
-- Idempotent enqueue
CREATE UNIQUE INDEX jobs_idempotency ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
-- Pruner and DLQ listing
CREATE INDEX jobs_finished ON jobs (finished_at) WHERE state IN ('completed', 'cancelled');
CREATE INDEX jobs_dead ON jobs (queue, finished_at, id) WHERE state = 'dead';   -- id added in M3a (§15.2)
```

Decisions:
- **Queue defaults (100000, 30 s, 10) are applied in Go** (`store.Queue.WithDefaults`), not as column defaults.
- **No "scheduled" state.** A delayed job and a job waiting to retry are both `available` with a future `run_at`. One mechanism covers both.
- **Idempotent enqueue is race-free.** Enqueue uses `INSERT … ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id`; when nothing is returned, it reads the existing job and replies with `created = false`.
- **Batches can't deadlock on keys.** A keyed insert holds its unique-index entry until commit, so two batches taking the same keys in opposite orders would deadlock (SQLSTATE 40P01). Enqueue inserts keyed jobs first, sorted by `(queue, idempotency_key)`, and still returns results in the caller's order. Within one mixed batch, keyed jobs therefore get lower IDs (and lease first among equal priorities) than unkeyed ones.
- **The DLQ is a state, not a table.** Redrive sets `state = 'available'`, `attempts = 0`, `run_at = now()`.
- **Idempotency keys live as long as the job row.** Once a completed job is pruned, its key can be reused. The retention window (default 24 h) is the deduplication window, and the docs say so.
- **Table churn.** Queue tables see constant updates and deletes. State transitions change indexed columns (`state` appears in partial-index predicates; `run_at`, `lease_expires_at` and `finished_at` are indexed), so they are not HOT updates. `fillfactor = 70` is a hypothesis to reduce page splits and bloat. The load test measures the HOT ratio (`pg_stat_user_tables.n_tup_hot_upd`) and bloat, and autovacuum settings are tuned during the load test.
- **Durations are stored as milliseconds** (bigint), which map directly to Go's time.Duration.

## 5. Job lifecycle

```
Enqueue ──▶ available ──Lease──▶ leased ──Ack──▶ completed ──(pruned after retention)
   ▲   (run_at may be in the future)  │
   │                                  ├── Heartbeat: extend lease_expires_at
   │                                  ├── Nack, or lease expired (reaper):
   └──── retry: run_at = now + backoff ◀── attempts < max_attempts
                                      └──▶ dead (DLQ) ──Redrive──▶ available
 Cancel: available ──▶ cancelled (leased jobs cannot be cancelled)
```

**Lease** (one transaction, up to `n` jobs):

```sql
WITH next AS (
    SELECT j.id FROM jobs j
    JOIN queues q ON q.name = j.queue
    WHERE j.queue = $1 AND j.state = 'available' AND j.run_at <= now() AND NOT q.paused
    ORDER BY j.priority, j.run_at, j.id
    LIMIT $2
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs j
SET state = 'leased', attempts = j.attempts + 1,
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + interval '1 millisecond' *
        coalesce($3::bigint, (SELECT visibility_timeout_ms FROM queues WHERE name = $1))
FROM next WHERE j.id = next.id
RETURNING j.*;
```

**Ack:** `UPDATE jobs SET state = 'completed', finished_at = now(), lease_expires_at = NULL WHERE id = $1 AND state = 'leased' AND lease_token = $2`. The completed row keeps its `lease_token`, so when zero rows are updated the server re-reads the job: completed with the same token means a retried ack and returns success; a missing job is `NOT_FOUND`; anything else means the lease was lost, and the server returns `FAILED_PRECONDITION`. An ack that arrives after `lease_expires_at` but before the reaper has run still succeeds: nobody else holds the job, so accepting it is safe and avoids a needless retry. Heartbeats follow the same rule.

**Nack and expiry** use the same retry rule. If `attempts < max_attempts`, the job goes back to `available` with `run_at = now() + backoff(attempts)`; otherwise it becomes `dead` with `last_error` set. A worker may pass an explicit `retry_at` (for example, from a downstream `Retry-After` header) to override the backoff.

**Backoff:** full jitter, `delay = random(0, min(cap, base · 2^attempts))`, with `base = 1 s` and `cap = 1 h` by default. `attempts` is incremented at lease, so the first retry's ceiling is `base · 2` (up to 2 s with a 1 s base).

**An expired lease counts as a failed attempt**, so a job that crashes its worker every time reaches the DLQ instead of looping forever.

**Reaper:** every server runs it every 5 s. It processes expired leases in batches with `FOR UPDATE SKIP LOCKED`, so several servers can run it at once without conflicting.

**Pruner:** deletes `completed` and `cancelled` jobs older than the retention period (default 24 h), and dead jobs older than the dead retention (default 7 days), in batches (§15.3).

**Priorities are strict.** A steady stream of priority-1 (critical) work starves priority-4 (low) jobs. This is a deliberate v1 trade-off; the fix (aging, i.e. raising a job's effective priority as it waits) is planned for v1.1.

**Paused queues** accept enqueues but return no leases.

## 6. API

One `proto/tasklane/v1/tasklane.proto` generates the gRPC server, the HTTP/JSON gateway (grpc-gateway) and the client stubs. The toolchain is `buf`.

| RPC | HTTP (gateway) | Notes |
|---|---|---|
| `Enqueue` | `POST /v1/queues/{queue}/jobs` | kind, payload, priority (1 critical … 4 low; 0 = unspecified, stored as 3), run_at, idempotency_key, max_attempts; returns `job_id`, `created` |
| `EnqueueBatch` | `POST /v1/queues/{queue}/jobs:batch` | Up to 1,000 jobs in one transaction |
| `Lease` | `POST /v1/queues/{queue}:lease` | `max_jobs`, `wait` (long poll, ≤ 30 s), optional `visibility_timeout` |
| `LeaseStream` | gRPC only | Bidirectional: the worker sends capacity credits, the server pushes jobs. **Deferred past v1** (§13) |
| `Ack` | `POST /v1/jobs/{id}:ack` | Requires `lease_token` |
| `Nack` | `POST /v1/jobs/{id}:nack` | `lease_token`, `error`, optional `retry_at` |
| `Heartbeat` | `POST /v1/jobs/{id}:heartbeat` | Returns the new `lease_expires_at` |
| `GetJob` | `GET /v1/jobs/{id}` | |
| `CancelJob` | `POST /v1/jobs/{id}:cancel` | Only while `available` |
| `ListDeadJobs` | `GET /v1/queues/{queue}/dead` | Paginated |
| `RedriveJobs` | `POST /v1/queues/{queue}/dead:redrive` | By IDs or all |
| `CreateQueue`, `UpdateQueue`, `GetQueue`, `ListQueues` | `/v1/queues…` | Settings and live counts (`GetQueue` returns `stats` since M2b, §14) |

The proto `priority` field is an enum with `PRIORITY_UNSPECIFIED = 0`, `PRIORITY_CRITICAL = 1` … `PRIORITY_LOW = 4`, so an unset field becomes the default rather than critical.

**Payloads** are JSON objects (`google.protobuf.Struct` in the proto), so HTTP clients send and receive natural JSON.

**Retry safety.** Every worker-facing call must be safe to retry, because networks lose responses, not just requests:
- *Ack is idempotent.* A completed job keeps the token that completed it. A repeated `Ack` with that same token returns success; any other token gets `FAILED_PRECONDITION`.
- *A repeated Nack* returns `FAILED_PRECONDITION`. This is harmless, because the job is already scheduled for retry; the SDK logs it and moves on.
- *Enqueue* is made safe to retry with idempotency keys.

**Long-polling.** `Lease` with `wait > 0` returns as soon as jobs are available or when `wait` elapses (empty result). Servers `LISTEN` on the Postgres channel `tasklane_jobs`; `Enqueue` sends `pg_notify('tasklane_jobs', queue)` once per distinct queue inside its transaction, so the notification is delivered only on commit. Waiting leases for that queue wake immediately. Because delayed jobs and retries become due without any notification, waiting leases also re-check every second.

Each server holds one dedicated `LISTEN` connection. If it drops, the server reconnects with backoff and wakes every waiting lease once LISTEN is re-established, since notifications sent in the meantime are lost.

**Leasing from an unknown queue** returns `NOT_FOUND` (an empty result means "nothing runnable right now").

**Limits.** A job's payload is at most 256 KiB, checked twice: as encoded JSON (what the database stores) and as a protobuf `Struct` (what `Lease` sends back). The `Struct` is the larger one, because a number costs about 11 bytes there against 2 in JSON, so a cap on the JSON alone would let a full lease exceed the message limit; `kind` and `idempotency_key` are at most 255 bytes and `trace_context` at most 512. Larger values are `INVALID_ARGUMENT` (a value that still exceeds a database limit, SQLSTATE 54000, maps there too). gRPC messages are limited to 16 MiB in both directions, on the server and on the gateway's connection to it, and HTTP request bodies to the same 16 MiB (larger bodies get 413). `Lease` clamps `max_jobs` to 50 without an error. Because the payload cap is measured in protobuf bytes, 50 jobs of maximum size plus 4 KiB each for their other fields (about 13 MiB) fit in one 16 MiB response; a test asserts this. An `EnqueueBatch` whose protobuf size is over 16 MiB minus 64 KiB is `INVALID_ARGUMENT`. Over HTTP, a batch that is under the 16 MiB body limit as JSON but larger as protobuf (JSON numbers grow about 5x) is refused by the gateway's own gRPC client before it is sent, and the gateway answers 413. A `Nack` error message longer than 4 KiB is truncated (at a UTF-8 character boundary), not rejected. A long-polling client's request timeout must exceed `wait` plus a margin for the round trip. A lease that commits but whose response never reaches the worker costs that job one attempt: the job comes back when its visibility timeout expires.

Queue names are 1–128 characters from [A-Za-z0-9._-], so they are safe in URL paths.

**Errors**

| Situation | gRPC status | HTTP |
|---|---|---|
| Queue over `max_depth` | `RESOURCE_EXHAUSTED` + retry-after hint | 429 |
| Stale or unknown lease token | `FAILED_PRECONDITION` | 409 |
| Unknown queue or job | `NOT_FOUND` | 404 |
| Bad arguments | `INVALID_ARGUMENT` | 400 |
| Queue already exists | `ALREADY_EXISTS` | 409 |
| Database unavailable or connection error | `UNAVAILABLE` | 503 |
| Request deadline exceeded / cancelled | `DEADLINE_EXCEEDED` / `CANCELLED` | 504 / 499 |
| Anything else (logged with details) | `INTERNAL` | 500 |
| Missing or wrong API key | `UNAUTHENTICATED` | 401 |

The HTTP gateway uses the proto field names in JSON (`job_id`, `lease_token`) and, as proto3 JSON requires, encodes 64-bit integers as strings. grpc-gateway's default for `FAILED_PRECONDITION` is 400; Tasklane overrides it to 409 to match this table.

**Backpressure**
- *Producer side:* `Enqueue` checks the queue's depth (`available + leased`) against `max_depth`. To avoid a `COUNT(*)` on every call, each server refreshes each queue's depth once a second (one count over the partial indexes) and checks against that cached value, so the limit is soft (it can overshoot by about one second of traffic). The docs state this.
- *Worker side:* each `Lease` asks for at most the worker's free slots (§14.2), so a slow worker is never flooded. `LeaseStream`, if it is built (§13), is credit-based. The server never sends more jobs than the worker has granted credits for, so a slow worker is never flooded.

**Auth:** a static API key per client, sent as `authorization: Bearer <key>` (gRPC metadata or HTTP header; the scheme is case-insensitive) and configured with `TASKLANE_API_KEYS` (comma-separated). With no keys configured, auth is disabled and the server logs a warning at startup. mTLS or OAuth are recommended for real deployments and are out of scope for v1.

**SDKs:** Go (`github.com/mubtasim/tasklane/sdk/go/tasklane`, §14), TypeScript (`@mubtasim/tasklane`) and C# (`Tasklane.Client` on NuGet). Each provides a producer client and a worker runner that handles leasing, concurrency, heartbeats, ack/nack, retrying the server connection with backoff, and graceful shutdown.

## 7. Failure handling

| Failure | Behavior |
|---|---|
| Worker crashes mid-job | The lease expires; the reaper retries the job (attempt counted) |
| Worker stalls past its lease, then acks | Stale token → 409; the current holder's result stands |
| Server crashes | No job state is lost (it is all in Postgres); SDKs reconnect to another copy |
| Postgres unavailable | API returns `UNAVAILABLE`; SDKs back off; `/readyz` fails so load balancers drain the server |
| Network timeout during enqueue | The producer retries with the same idempotency key; no duplicate |
| Poison job | After `max_attempts` → `dead`; a metric and an example alert flag it |
| Duplicate execution after a crash before ack | Expected under at-least-once; workers use the job ID or their own keys to stay idempotent |
| Deploy / graceful shutdown | The server stops accepting new requests, ends streams with a "reconnect" signal and drains in-flight RPCs within a timeout; SDK workers finish current jobs before exiting |
| Clock skew between servers | All time comparisons use Postgres `now()`, never server clocks |

## 8. Observability

- **Metrics** (`/metrics`; the M4a spec in §16 supersedes this list):
  - `tasklane_jobs_enqueued_total`, `_leased_total`, `_acked_total`, `_nacked_total`, `_dead_total`, `_reaped_total`
  - `tasklane_queue_depth{queue,state,priority}`, `tasklane_oldest_available_age_seconds{queue}`
  - `tasklane_lease_latency_seconds` and `tasklane_rpc_duration_seconds` (histograms)
  - DB pool stats
- **Logs:** structured JSON (`log/slog`) with `job_id`, `queue`, `kind` and the trace ID.
- **Tracing:** the producer's W3C `traceparent` is stored with the job and handed to the worker, so a request, its job and the worker's work form one trace.
- **Health:** `/healthz` (process alive) and `/readyz` (database reachable, migrations applied).
- **Dashboards and alerts:** the repo ships a Grafana dashboard and example Prometheus alert rules for DLQ growth, oldest-job age and a rising reap rate.

## 9. Testing

- **Unit:** backoff and jitter bounds, state-transition rules, request validation.
- **Integration:** against a real Postgres (testcontainers-go). Every row of the §7 failure table is a test.
- **Concurrency invariant test:** 50 workers × 10,000 jobs with random failures. It checks that every job reaches a final state, none is lost, and every duplicate run follows a lease expiry.
- **Chaos test:** kill workers and one of two server copies at random during a run, then check the same invariants.
- **SDK contract tests:** each SDK runs one shared scenario suite against a live server in CI.
- **CI:** GitHub Actions running lint (`golangci-lint`), tests with the race detector, `buf lint` and `buf breaking`, and image builds.

## 10. Load-test plan

- **Tool:** a Go load generator in `bench/` (producer and worker modes), driven by a script that records results as CSV.
- **Scenarios:**
  1. Enqueue throughput (single vs batch).
  2. Full cycle (enqueue → lease → ack) at 1, 10, 50 and 200 workers.
  3. Payload sizes of 100 B, 10 KB and 100 KB.
  4. Queue depth of 1 million jobs.
  5. Two server copies vs one.
- **Measured:** jobs/sec, p50/p99 enqueue-to-start latency, Postgres CPU, WAL rate and table bloat.
- **Report** (`docs/benchmarks.md`): hardware, numbers, the bottleneck found, each tuning step (batching, fillfactor, autovacuum, pool size, `synchronous_commit` trade-off discussion) with before/after charts.

## 11. Packaging and distribution

- Apache-2.0 license.
- Docker image on GHCR; `deploy/docker-compose.yml` with Postgres, two server copies, demo producer and workers, Prometheus and Grafana. One command gives a live demo.
- The server applies versioned SQL migrations on startup (embedded with `go:embed`).
- Configuration via file and environment variables.
- Semantic versioning; the v1 API won't break, enforced by `buf breaking` in CI.
- Docs: README quickstart, API reference generated from the proto, this design document, the benchmark report and an operations guide.

## 12. Future work

- **v1.1:** per-queue concurrency and rate limits; priority aging; a CLI (`tasklane jobs ls`, `tasklane dead redrive`).
- **Phase 2 (Feb–May 2027):** `RaftStore`, which implements `Store` on a Raft-replicated KV store written from scratch. A Raft leader also enables cron jobs without duplicates and lets the queue move towards the in-memory, leader-based design rejected for v1 (§3), trading Postgres for replicated state.
- **Later:** a web UI, multi-tenant auth, and job dependencies.

## 13. Milestones (~5 h/week)

| # | Target | Deliverable |
|---|---|---|
| M1 | mid-Nov 2026 | Repo, schema and migrations, `PostgresStore` with enqueue/lease/ack/nack and integration tests |
| M2a | end of Nov | API server: proto + buf, gRPC and HTTP gateway, error mapping, API-key auth, idempotent ack, long-poll leasing with LISTEN/NOTIFY, reaper loop, graceful shutdown |
| M2b | early Dec | Go SDK (producer + worker runner with heartbeats), live queue stats, `tasklane-watch`, docker-compose demo with flaky and poison jobs (§14) |
| M3a | mid-Dec | Pruner, DLQ listing and redrive, cancel, backpressure (§15) |
| M3b | — | `LeaseStream`: **deferred past v1** (see below) |
| M4a | mid-Dec | CI, Prometheus metrics, Grafana dashboard, alert rules (§16) |
| M4b | end of Dec | Tracing, invariant and chaos tests, two server copies behind a gRPC-aware load balancer |
| M5 | mid-Jan 2027 | TS and C# SDKs, load tests and benchmark report, v1.0.0 release and a blog post |

Notes for M3: like `Enqueue`, `RedriveJobs` must send `pg_notify('tasklane_jobs', queue)` inside its transaction, because it makes jobs runnable, or waiting leases only notice at their next one-second re-check. `CancelJob` only removes runnable work, so it sends none. `LeaseStream` is the first streaming RPC, so it needs a `StreamServerInterceptor` for API-key auth; the M2a server only installs the unary interceptor.

**`LeaseStream` deferred (decided 2026-11-01).** A stream would replace one `Lease` request per batch with a credit message on an open call. That saves little: requests are already multiplexed over one HTTP/2 connection, each batch still needs the same Postgres transaction, and a batch of k jobs already costs k Acks besides its Lease. A waiting stream re-checks the queue once a second just like a waiting long-poll, and long-polls already ask only for free slots. Against that, a stream adds per-stream credit state, reconnects and a reconnect signal on shutdown, keepalives to detect half-open connections, a stream auth interceptor, no HTTP/JSON support, and stickiness: a stream is one request, so even a gRPC-aware load balancer pins it to one server copy. The M5 load test measures lease requests per job and p99 enqueue-to-start latency; `LeaseStream` is built only if the long-poll shows up as a bottleneck there.

## 14. M2b: Go SDK, live queue stats and demo

Decided 2026-10-20. This section is the M2b spec.

### 14.1 Public API

Package `github.com/mubtasim/tasklane/sdk/go/tasklane`, in the server's module (Go compiles only imported packages, so SDK users don't build the server or its dependencies). gRPC only.

```go
client, err := tasklane.Dial("localhost:7070", tasklane.WithAPIKey("secret"))
defer client.Close()

// Producer
res, err := client.Enqueue(ctx, "demo", tasklane.Job{
    Kind:           "send_email",
    Payload:        EmailPayload{To: "a@b.com"}, // any value; JSON-encoded into the proto Struct
    Priority:       tasklane.PriorityHigh,
    RunAt:          time.Now().Add(time.Minute), // optional
    IdempotencyKey: "order-123-email",           // optional
})                                               // res.JobID, res.Created
client.EnqueueBatch(ctx, "demo", jobs)
client.CreateQueue(ctx, tasklane.QueueConfig{Name: "demo", VisibilityTimeout: 15 * time.Second})
client.GetQueue(ctx, "demo") // config + Stats (§14.3)

// Worker
w := client.NewWorker("demo", tasklane.WorkerOptions{Concurrency: 5})
w.Handle("send_email", func(ctx context.Context, job *tasklane.LeasedJob) error {
    var p EmailPayload
    if err := job.Decode(&p); err != nil {
        return err
    }
    return send(ctx, p) // nil → Ack, error → Nack
})
err = w.Run(ctx) // blocks; cancelling ctx starts graceful shutdown
```

- `LeasedJob` exposes `ID`, `Kind`, `Attempt`, `MaxAttempts`, `Payload` and `Decode(&v)`. The lease token stays inside the SDK: only the runner sends Ack, Nack and Heartbeat.
- Handlers are registered per job kind. A job whose kind has no handler is Nacked with `no handler for kind "X"`, so it ends in the DLQ instead of looping.
- `tasklane.RetryAt(err, t)` wraps an error so the Nack carries `retry_at = t` (for example from a downstream `Retry-After`).
- A panicking handler is recovered and Nacked with the panic message.
- Producer calls are returned to the caller as errors; the SDK does not retry them. The caller retries an enqueue with the same idempotency key.

### 14.2 Worker runner

**Leasing: one loop per worker, sized to free slots.** Slots are a buffered channel with capacity `Concurrency`. The loop:

1. Blocks until at least one slot is free, then takes every other free slot without blocking: `k` slots.
2. Calls `Lease(max_jobs = k, wait = 20 s, visibility_timeout = V)` with a call deadline of `wait + 10 s`.
3. Gets `m ≤ k` jobs, returns the `k − m` unused slots (forgetting this drains every slot on the first empty long-poll and silently stalls the worker), and starts one goroutine per job.

Rejected alternative: `Concurrency` goroutines that each lease one job. That holds one long-poll per goroutine instead of one per process, and each waiting lease costs the server one query per second (§6), so idle database load would grow with total worker concurrency; it also gives up batch leasing. Moving the loop to `LeaseStream` credits in M3 changes no public API.

**Visibility timeout.** `Run` first calls `GetQueue`, which validates that the queue exists and gives its visibility timeout `V` unless `WorkerOptions.VisibilityTimeout` overrides it. The runner sends `V` explicitly on Lease and Heartbeat and schedules heartbeats from its own monotonic clock, never by comparing the server's `lease_expires_at` with the local clock, so clock skew between worker and database cannot break heartbeats.

**Per job.** Each job gets its own `jobCtx`, not derived from `Run`'s context, so shutdown does not cancel running handlers at once.

- A heartbeat ticker runs every `V / 3`, so one lost heartbeat does not lose the lease.
  - Success extends the lease.
  - `FAILED_PRECONDITION` or `NOT_FOUND` means the lease is lost (another worker may hold the job): cancel `jobCtx`, send no Ack or Nack, log `lease lost`. This shortens, but cannot remove, the window in which two workers run the same job; handlers must stay idempotent (at-least-once).
  - `UNAVAILABLE` waits for the next tick.
- After the handler returns, the heartbeat stops, then the runner sends Ack (nil) or Nack (error text, optional `retry_at`). Both are retried with backoff on `UNAVAILABLE` until the lease would have expired: a retried Ack is idempotent and a repeated Nack gets a harmless `FAILED_PRECONDITION`.
- The slot is released.

**Errors from the server.** `UNAVAILABLE` on Lease is retried with full-jitter backoff (ceiling 1 s, doubling to a 30 s cap). `NOT_FOUND` (queue deleted) and `UNAUTHENTICATED` end `Run` with an error.

**Graceful shutdown** when `Run`'s context is cancelled:

1. The lease loop stops; no new jobs are taken.
2. Running handlers get up to `WorkerOptions.ShutdownTimeout` (default 25 s) to finish and Ack or Nack normally.
3. At the deadline the runner cancels the remaining `jobCtx`s and Nacks those jobs with `worker shutting down`; these Nacks and any Acks still in flight share one 2 s budget, then returns without waiting for handlers that ignore their context.

If the Nack can't be sent (SIGKILL, network down), the lease simply expires and the reaper retries the job, so the Nack is a best-effort speed-up over a safe fallback. Either way the attempt is already counted, because `attempts` is incremented at lease time.

### 14.3 Live queue stats

`GetQueueResponse` gains `QueueStats stats` (an additive proto change): `available`, `leased`, `dead`, `recently_completed` and `recent_window` (10 s, `store.StatsWindow`). `Store.QueueStats` computes them in one statement, on demand (no cache until M3 adds backpressure):

- `available` and `dead` use the partial indexes that start with `queue`.
- `leased` reads the `jobs_leased` partial index, which has no `queue` column; leased jobs are bounded by the total worker slots, so this stays small.
- Completed jobs are not counted in full: they pile up until pruned, and `COUNT(*)` in Postgres visits every row (MVCC keeps no stored count). `recently_completed` instead counts `state = 'completed' AND finished_at > now() - 10 s` through the B-tree `jobs_finished` index, so its cost follows recent throughput, not table size.

Rejected: a counters table updated on every transition (one hot row that every Ack must lock, serialising completions).

The same query later feeds the M3 backpressure cache and the M4 `tasklane_queue_depth` metric.

### 14.4 `tasklane-watch` and the demo

- `cmd/tasklane-watch`: redraws once a second: `queue=demo  available 120 | leased 15 | dead 4 | completed 42/s` (`recently_completed / recent_window`). It is the start of the v1.1 CLI.
- `examples/demo/producer`: creates the `demo` queue (`visibility_timeout` 15 s, `max_attempts` 5) and enqueues about 10 jobs/s: `normal` ~85% (sleeps 0.2–2 s, succeeds), `flaky` ~12% (fails 30% of runs), `poison` ~3% (always fails, reaches the DLQ).
- `examples/demo/worker`: handlers for the three kinds, `Concurrency` 5.
- `deploy/docker-compose.yml` with one multi-stage `Dockerfile`: `postgres` (not published on the host), `tasklane` (8080 and 7070 published), `producer`, `worker-1..3` (`stop_grace_period: 30s`, since Docker's default of 10 s is shorter than `ShutdownTimeout`). About 250 MB of RAM in total. `tasklane-watch` runs on the host.
- Two failure paths to show: `docker compose kill worker-1` (SIGKILL: its jobs stay leased ~15 s until the reaper returns them) and `docker compose stop worker-2` (SIGTERM: running jobs finish, unfinished ones are Nacked and return within seconds). The README gets a Demo section with the tmux layout.
- Deferred: two server copies with one killed (needs multi-address dialing or a proxy).

### 14.5 Testing

- **SDK unit tests** against an in-memory gRPC server (`bufconn`) that scripts every reply: unused slots are returned (a job still runs after an empty lease); never more than `Concurrency` jobs at once; a lost lease cancels the handler's context and sends no Ack or Nack; panic and unknown kind become Nacks; `RetryAt` sets `retry_at`; backoff on `UNAVAILABLE`; `NOT_FOUND` and `UNAUTHENTICATED` end `Run`; shutdown stops leasing, lets running jobs finish and Nacks the rest at the deadline.
- **Integration tests** (real server, Postgres via testcontainers): enqueue → worker → `completed`; a handler that never returns loses its job to the reaper and a second worker completes it; `QueueStats` per state, including the 10 s window.
- Everything runs with `-race`. The demo is checked by hand (smoke test in tmux).

### 14.6 Carried over from the M2a review

- Gate the ~40 s `-race` max-lease test behind `testing.Short()`.
- Raise per-job size headroom from 4 KiB to 16 KiB so `last_error` is covered.
- Check the protobuf size before the gateway's JSON marshal.
- Fix the stale `LeaseRequest.max_jobs` proto comment ("1 to 1000"): the server clamps to 50 (`MaxLeaseJobs`).

## 15. M3a: cancel, dead-letter operations, pruning and backpressure

Decided 2026-10-31. This section is the M3a spec. `LeaseStream` moved to M3b and gets its own design.

### 15.1 API

Three new unary RPCs, so the existing unary auth interceptor covers them:

| RPC | HTTP | Request → response |
|---|---|---|
| `CancelJob` | `POST /v1/jobs/{id}:cancel` | `id` → the job, now `cancelled` |
| `ListDeadJobs` | `GET /v1/queues/{queue}/dead` | `page_size` (default and max 100), `page_token` → dead jobs, newest first, and `next_page_token` (empty on the last page) (the server reads one job more than the page, so a last page never carries a token) |
| `RedriveJobs` | `POST /v1/queues/{queue}/dead:redrive` | `ids` (at most 1,000) or `all = true` → `redriven`, and `skipped` (IDs that were not dead jobs of this queue) |

- **Cancel** works only while the job is `available`. Cancelling an already-cancelled job returns it with success, so a retried cancel is harmless. A leased, completed or dead job is `FAILED_PRECONDITION` (409) with the current state in the message; an unknown job is `NOT_FOUND`.
- **Redrive** with both `ids` and `all`, or neither, is `INVALID_ARGUMENT`. IDs that are not dead are skipped and counted, not errors, so a retried redrive is harmless.
- **Page tokens** are opaque to clients: the last row's `(finished_at, id)`, base64-encoded. A token that does not decode is `INVALID_ARGUMENT`.
- **Enqueue over `max_depth`** (and `EnqueueBatch`, refused whole) is `RESOURCE_EXHAUSTED` (429) with a 1 s retry-after hint, the depth refresh interval (§15.3). This is the only change to an existing RPC. The depth check runs before the store, so while a queue is full an enqueue retry that carries an existing idempotency key also gets 429. A cancelled job keeps its idempotency key until it is pruned (the retention window), so re-enqueueing with that key returns the cancelled job. Any API key may call the admin RPCs (no roles in v1; see §12).

**Go SDK:** `Client.CancelJob`, `Client.ListDeadJobs`, `Client.RedriveJobs` (by IDs) and `Client.RedriveAll`. `Enqueue` and `EnqueueBatch` return `ErrQueueFull` (matched with `errors.Is`, carrying the retry-after) on 429 and do not retry it: a full queue is a signal for the producer's owner, like every other producer error (§14.1).

### 15.2 Storage

New `Store` methods, implemented in `pgstore`, so `RaftStore` can implement them later.

**Cancel** is one conditional update:

```sql
UPDATE jobs SET state = 'cancelled', finished_at = now()
WHERE id = $1 AND state = 'available'
RETURNING *;
```

On zero rows the store re-reads the job: `cancelled` returns it, a missing job is `NOT_FOUND`, any other state is `FAILED_PRECONDITION`. No `NOTIFY`: cancelling never makes a job runnable.

*Race with Lease.* Lease selects rows `FOR UPDATE SKIP LOCKED`. If Lease locks the row first, Cancel's update waits for the lock; when Lease commits, Postgres re-checks `state = 'available'` on the new row version (EvalPlanQual), finds `leased` and updates nothing, so Cancel returns 409. If Cancel locks it first, Lease skips the row. Exactly one of them wins.

**ListDeadJobs** is a keyset page:

```sql
SELECT * FROM jobs
WHERE queue = $1 AND state = 'dead' AND (finished_at, id) < ($2, $3)   -- omitted on the first page
ORDER BY finished_at DESC, id DESC
LIMIT $4;
```

A migration replaces `jobs_dead (queue, finished_at)` with `jobs_dead (queue, finished_at, id)`, so the tiebreak is in the index and every page is an index range scan. Keyset rather than `OFFSET`, whose cost grows with the page number.

**RedriveJobs** runs in batches of up to 1,000, one transaction each:

```sql
WITH d AS (
    SELECT id FROM jobs
    WHERE queue = $1 AND state = 'dead'      -- by IDs: WHERE id = ANY($2) AND queue = $1 AND state = 'dead'
    ORDER BY finished_at, id                 -- by IDs: ORDER BY id
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET state = 'available', attempts = 0, run_at = now(), finished_at = NULL
FROM d WHERE j.id = d.id;
```

All-mode orders by `(finished_at, id)` on `jobs_dead`, so every batch starts at the front of the partial index (redriven rows leave it); ids-mode looks up the primary key. They are two statements so neither plan depends on a NULL check.

The same transaction sends `pg_notify('tasklane_jobs', queue)` when it changed at least one row. `all` repeats batches until one updates nothing or the request is cancelled; the response counts every committed batch.

- Not atomic, by choice: one transaction over 200,000 rows would hold locks for seconds, write a burst of WAL and leave bloat, and a timeout would undo everything. If the server stops halfway, the committed batches stay redriven and calling `all` again finishes the rest.
- `last_error` is kept, so the operator still sees why the job died until it fails again.
- `finished_at = NULL` matters: dead retention is measured from `finished_at`, so a job that dies again starts a fresh retention period.
- Redriving many jobs makes them all runnable at once. Workers take only what their slots allow, but the retries can hit a just-recovered dependency hard; the docs say so, and per-queue rate limits (v1.1) are the fix.

**Pruner** deletes in batches of 1,000 with `SKIP LOCKED`, so every server can run it:

```sql
DELETE FROM jobs WHERE id IN (
    SELECT id FROM jobs
    WHERE state IN ('completed', 'cancelled') AND finished_at < now() - $1::interval
    LIMIT 1000 FOR UPDATE SKIP LOCKED);
-- the same for state = 'dead' with the dead retention
```

The first uses `jobs_finished`. The second has no queue to fix the `jobs_dead` prefix, so it scans that index across queues; dead jobs are few and this runs once a minute.

**Queue depth** for backpressure is one statement for all queues, with each queue's `max_depth`:

```sql
SELECT q.name, q.max_depth,
    (SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'available') +
    (SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'leased')
FROM queues q;
```

The available count is an index-only range on `jobs_available`, which starts with `queue`; leased jobs are bounded by the workers' slots. A queue with no jobs gets depth 0.

**Reaper message.** When an expired lease follows an earlier failure, the reaper writes `lease expired (previous: <last_error>)`, truncated to 4 KiB at a UTF-8 boundary, instead of overwriting the real error with `lease expired`.

### 15.3 Server loops and configuration

- **Pruner** (`internal/pruner`): every server, every `TASKLANE_PRUNE_INTERVAL`. One pass deletes batch after batch until a batch is short, so a large backlog clears in one pass without one large transaction, and logs the rows deleted. Started and stopped with the server, like the reaper.
- **Depth cache** (`internal/api`): one goroutine refreshes `map[queue]depth` every second; `Enqueue` refuses when the cached depth is at or above `max_depth`, so the limit is soft (it can overshoot by about one second of traffic). A queue missing from the map has depth 0. If refreshes fail, the last good values are used for up to 10 s, then the cache **fails open** and stops refusing: refusing all enqueues on unknown data would turn a short database blip into a producer outage, and while the database is down `Enqueue` fails with `UNAVAILABLE` anyway.

| Variable | Default | Meaning |
|---|---|---|
| `TASKLANE_RETENTION` | `24h` | Age at which completed and cancelled jobs are pruned (also the idempotency-key window, §4) |
| `TASKLANE_DEAD_RETENTION` | `168h` | Age at which dead jobs are pruned |
| `TASKLANE_PRUNE_INTERVAL` | `1m` | Pruner period |

Dead jobs expire like messages in an SQS dead-letter queue. The docs say so; M4's dead-job metric and example alert are what make failures seen before they expire.

### 15.4 Demo

- README gains an Operations section with `curl` examples against the compose demo: list dead jobs, redrive all, cancel a delayed job.
- The demo's poison jobs already fill the DLQ, so redriving them shows in `tasklane-watch`: `dead` drops, `available` jumps, and they die again.
- Backpressure: a queue with `max_depth = 50` and the workers stopped; the demo producer handles `ErrQueueFull` and logs `queue full, retry after 1s`.

### 15.5 Testing

- **pgstore integration tests:** every cancel outcome; Cancel racing Lease over many rounds (exactly one wins each time); keyset paging with equal `finished_at` values; redrive by IDs with skips, redrive all across several batches, `NOTIFY` delivered only on commit; pruner retention boundaries for both rules; the reaper message.
- **API tests:** error mapping (409, 404, 400, 429 with retry-after), the page-token round trip and a malformed token.
- **Depth cache unit tests** with a fake clock: refresh, stale values, fail-open after 10 s, unknown queue.
- **SDK tests** (`bufconn`): the three methods and `ErrQueueFull`.
- Everything runs with `-race`. The demo is checked by hand.

### 15.6 Carried over

- Interview notes Q16 onward at the M3a checkpoint.

Out of scope: `LeaseStream` (M3b), a CLI, rate limits, a delete-dead-jobs RPC.

## 16. M4a: CI, metrics, dashboard and alerts

Decided 2026-11-01. This section is the M4a spec. Tracing, invariant and chaos tests and a two-server demo are M4b.

### 16.1 CI

`.github/workflows/ci.yml` runs on every push to `main` and every pull request, with four parallel jobs on GitHub's Ubuntu runners:

| Job | Runs |
|---|---|
| `lint` | `golangci-lint` (pinned) with `.golangci.yml`: the default linters (errcheck, govet, staticcheck, unused, ineffassign) plus errorlint and misspell, and the gofmt and goimports formatters |
| `test` | `go test -race -count=1 ./...`; testcontainers starts Postgres 17 in the runner's Docker |
| `proto` | `buf lint`; on pull requests `buf breaking` against `main`; `buf generate` followed by `git diff --exit-code gen/`; `promtool check rules deploy/prometheus/alerts.yml` |
| `docker` | `docker build` of the server image, without pushing (publishing to GHCR comes with v1.0.0 in M5) |

- **Pinned tools.** buf and the protoc plugins are `tool` directives in a separate `tools.mod` (with `tools.sum`) at the repo root and run as `go tool -modfile=tools.mod <tool>`; `buf.gen.yaml` runs the plugins the same way, so local runs and CI use the same versions and SDK users don't inherit buf's dependencies through the main `go.mod`. The README's generate command changes to match.
- **First lint run.** Every finding in existing code is fixed in M4a; no linter is disabled to get a green build.
- **Flaky tests** are fixed by widening their timing margins (the M2b follow-ups list the tight ones), never retried.
- The README gets a CI badge. Requiring CI on `main` is a branch rule the maintainer sets in the GitHub UI.
- Also folded in from the M4 follow-ups: a connect timeout on the LISTEN connection, and the database DSN parsed once.

### 16.2 Metrics

A new package `internal/metrics` defines every metric on its own registry (not the global one, so tests can create a fresh one) using `prometheus/client_golang`. The server serves it at `/metrics` on `TASKLANE_METRICS_ADDR` (default `:9464`; `off` disables metrics), a port separate from the API so operators can keep it internal. `/healthz` and `/readyz` stay on the HTTP port. `/metrics` has no API key.

| Metric | Type | Labels | Recorded |
|---|---|---|---|
| `tasklane_jobs_enqueued_total` | counter | `queue` | per job created; an idempotent enqueue that returns an existing job doesn't count |
| `tasklane_jobs_leased_total` | counter | `queue` | per job leased |
| `tasklane_jobs_acked_total` | counter | `queue` | per job completed; a repeated Ack with the same token doesn't count |
| `tasklane_jobs_nacked_total` | counter | `queue` | per Nack that schedules a retry |
| `tasklane_jobs_dead_total` | counter | `queue`, `reason` (`nacked`, `expired`) | per job that reaches `dead`, from Nack or the reaper |
| `tasklane_jobs_reaped_total` | counter | `queue` | per expired lease the reaper handles (retry or dead) |
| `tasklane_jobs_cancelled_total` | counter | `queue` | per job cancelled; a repeated cancel doesn't count |
| `tasklane_jobs_redriven_total` | counter | `queue` | per dead job made available again |
| `tasklane_jobs_pruned_total` | counter | `state` (`finished` for completed and cancelled, `dead`) | per job deleted by the pruner |
| `tasklane_enqueue_refused_total` | counter | `queue` | per `Enqueue`/`EnqueueBatch` call refused by backpressure |
| `tasklane_rpc_duration_seconds` | histogram | `method`, `code` | every RPC, from a unary gRPC interceptor; HTTP calls pass through the gateway, so they are included |
| `tasklane_queue_wait_seconds` | histogram | `queue` | per job leased: the lease time minus the time the job became ready (`run_at`), both from Postgres `now()` |
| `tasklane_queue_depth` | gauge | `queue`, `state` (`available`, `leased`) | from the depth refresh; `available` includes delayed jobs, as in the backpressure check |
| `tasklane_oldest_ready_job_age_seconds` | gauge | `queue` | from the depth refresh: now minus the oldest `run_at` among available jobs whose `run_at` has passed; 0 when there are none or the queue is paused |
| `tasklane_queue_stats_age_seconds` | gauge | | seconds since the last successful depth refresh |
| `tasklane_db_pool_acquired_connections`, `_idle_connections`, `_max_connections` | gauge | | the pgxpool stats, read on each scrape |
| `tasklane_db_pool_empty_acquire_total` | counter | | acquires that had to wait for a connection |
| Go runtime and process | | | the standard Go and process collectors (`go_goroutines`, `process_resident_memory_bytes`, …) |

- **Counters count real changes after commit.** The store reports how many rows actually changed (a repeated Ack, cancel or idempotent enqueue changes none), and the API increments only after the transaction commits, so a rolled-back call counts nothing.
- **Every known queue's counters start at 0.** Each depth refresh creates the queue's counter series at 0, so graphs show 0 rather than no data and `increase()` sees a queue's first dead job (a series born at 1 has no increase).
- **Labels are bounded.** `queue` is chosen by operators; `kind` (free text from producers), `job_id` and `priority` are never labels.
- **Gauges come from the existing one-second depth refresh (§15.3)**, which also returns the state split and the oldest ready age. The oldest ready age is one index lookup per priority on `jobs_available`. Each refresh replaces all gauge series, so a deleted queue's series disappear. While refreshes fail, the gauges keep their last values and `tasklane_queue_stats_age_seconds` grows.
- **Several servers** each report the same database-wide gauges, so dashboards and alerts aggregate gauges with `max by (queue)` and counters with `sum`.

### 16.3 Dashboard and alerts

`deploy/grafana/dashboards/tasklane.json`, provisioned on startup (with the Prometheus data source) from `deploy/grafana/provisioning/`, with a `queue` variable and a 5 s refresh:

1. Stats: throughput (acked/s), available, leased, dead in the last hour, oldest ready job age, refused enqueues/s, with yellow and red thresholds.
2. Jobs per second by outcome (enqueued, acked, nacked, dead, reaped); queue depth by state.
3. Queue wait p50 and p99; RPC latency p99 by method; RPC errors by code.
4. DB pool (in use, idle, max, waits per second); goroutines; resident memory.

The RPC latency panel leaves out `Lease`, whose duration includes the long-poll wait (up to 30 s).

`deploy/prometheus/alerts.yml` (example rules; nothing is routed to a notifier):

| Alert | Condition |
|---|---|
| `TasklaneDown` | `up == 0` for 1 m |
| `TasklaneJobsStuck` | oldest ready job age > 5 m for 5 m (no workers, or workers that stopped leasing) |
| `TasklaneDeadJobs` | any job became dead in the last 10 m |
| `TasklaneReapRateHigh` | reaped > 10% of leased for 10 m |
| `TasklaneQueueFull` | enqueues refused for 5 m |
| `TasklaneErrors` | `Internal` or `Unavailable` (as gRPC code strings) > 5% of RPCs for 5 m |

### 16.4 Demo

- `deploy/docker-compose.yml` gains Prometheus (`127.0.0.1:9090`, scrape every 5 s, 1 day of retention, the alert rules loaded) and Grafana (`127.0.0.1:3000`, anonymous read-only access, the dashboard provisioned), both with pinned images and started by the plain `docker compose up`. Together they add about 375 MB of RAM.
- The README gains a Monitoring section: what each panel shows, a walkthrough (kill a worker: acked/s holds while the others have spare capacity, available and queue wait rise, the reaped spike follows after the visibility timeout; stop `worker-2` and `worker-3`: one worker handles about 4 jobs/s against 10/s arriving, so acked/s drops and available grows until they are started again), and a dashboard screenshot.

### 16.5 Testing

- **Unit:** each counter on its success path, its failure path and its idempotent-retry path; the interceptor records method and code; gauges are replaced on refresh and keep their values on a failed one.
- **Integration:** a real server with known jobs; scrape `/metrics` and check the counters and gauges; `/metrics` is absent when `TASKLANE_METRICS_ADDR` is empty.
- **Files:** `promtool check rules` in CI; a Go test parses the dashboard JSON and checks every panel has a query.
- Everything runs with `-race`, locally and in CI. The demo and screenshot are checked by hand.

Out of scope: tracing, invariant and chaos tests, two server copies (M4b); SDK-side metrics; a `priority` label on depth.
