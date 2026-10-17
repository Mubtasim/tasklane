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
CREATE INDEX jobs_dead ON jobs (queue, finished_at) WHERE state = 'dead';
```

Decisions:
- **Queue defaults (100000, 30 s, 10) are applied in Go** (`store.Queue.WithDefaults`), not as column defaults.
- **No "scheduled" state.** A delayed job and a job waiting to retry are both `available` with a future `run_at`. One mechanism covers both.
- **Idempotent enqueue is race-free.** Enqueue uses `INSERT … ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id`; when nothing is returned, it reads the existing job and replies with `created = false`.
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

**Ack:** `UPDATE jobs SET state = 'completed', finished_at = now(), lease_expires_at = NULL WHERE id = $1 AND state = 'leased' AND lease_token = $2`. The completed row keeps its `lease_token`, so when zero rows are updated the server reads the job on the same connection: completed with the same token means a retried ack and returns success; a missing job is `NOT_FOUND`; anything else means the lease was lost, and the server returns `FAILED_PRECONDITION`. An ack that arrives after `lease_expires_at` but before the reaper has run still succeeds: nobody else holds the job, so accepting it is safe and avoids a needless retry. Heartbeats follow the same rule.

**Nack and expiry** use the same retry rule. If `attempts < max_attempts`, the job goes back to `available` with `run_at = now() + backoff(attempts)`; otherwise it becomes `dead` with `last_error` set. A worker may pass an explicit `retry_at` (for example, from a downstream `Retry-After` header) to override the backoff.

**Backoff:** full jitter, `delay = random(0, min(cap, base · 2^attempts))`, with `base = 1 s` and `cap = 1 h` by default. `attempts` is incremented at lease, so the first retry's ceiling is `base · 2` (up to 2 s with a 1 s base).

**An expired lease counts as a failed attempt**, so a job that crashes its worker every time reaches the DLQ instead of looping forever.

**Reaper:** every server runs it every 5 s. It processes expired leases in batches with `FOR UPDATE SKIP LOCKED`, so several servers can run it at once without conflicting.

**Pruner:** deletes `completed` and `cancelled` jobs older than the retention period, in batches. Dead jobs are kept until they are redriven or deleted through the admin API.

**Priorities are strict.** A steady stream of priority-1 (critical) work starves priority-4 (low) jobs. This is a deliberate v1 trade-off; the fix (aging, i.e. raising a job's effective priority as it waits) is planned for v1.1.

**Paused queues** accept enqueues but return no leases.

## 6. API

One `proto/tasklane/v1/tasklane.proto` generates the gRPC server, the HTTP/JSON gateway (grpc-gateway) and the client stubs. The toolchain is `buf`.

| RPC | HTTP (gateway) | Notes |
|---|---|---|
| `Enqueue` | `POST /v1/queues/{queue}/jobs` | kind, payload, priority (1 critical … 4 low; 0 = unspecified, stored as 3), run_at, idempotency_key, max_attempts; returns `job_id`, `created` |
| `EnqueueBatch` | `POST /v1/queues/{queue}/jobs:batch` | Up to 1,000 jobs in one transaction |
| `Lease` | `POST /v1/queues/{queue}:lease` | `max_jobs`, `wait` (long poll, ≤ 30 s), optional `visibility_timeout` |
| `LeaseStream` | gRPC only | Bidirectional: the worker sends capacity credits, the server pushes jobs |
| `Ack` | `POST /v1/jobs/{id}:ack` | Requires `lease_token` |
| `Nack` | `POST /v1/jobs/{id}:nack` | `lease_token`, `error`, optional `retry_at` |
| `Heartbeat` | `POST /v1/jobs/{id}:heartbeat` | Returns the new `lease_expires_at` |
| `GetJob` | `GET /v1/jobs/{id}` | |
| `CancelJob` | `POST /v1/jobs/{id}:cancel` | Only while `available` |
| `ListDeadJobs` | `GET /v1/queues/{queue}/dead` | Paginated |
| `RedriveJobs` | `POST /v1/queues/{queue}/dead:redrive` | By IDs or all |
| `CreateQueue`, `UpdateQueue`, `GetQueue`, `ListQueues` | `/v1/queues…` | Settings and live counts |

The proto `priority` field is an enum with `PRIORITY_UNSPECIFIED = 0`, `PRIORITY_CRITICAL = 1` … `PRIORITY_LOW = 4`, so an unset field becomes the default rather than critical.

**Payloads** are JSON objects (`google.protobuf.Struct` in the proto), so HTTP clients send and receive natural JSON.

**Retry safety.** Every worker-facing call must be safe to retry, because networks lose responses, not just requests:
- *Ack is idempotent.* A completed job keeps the token that completed it. A repeated `Ack` with that same token returns success; any other token gets `FAILED_PRECONDITION`.
- *A repeated Nack* returns `FAILED_PRECONDITION`. This is harmless, because the job is already scheduled for retry; the SDK logs it and moves on.
- *Enqueue* is made safe to retry with idempotency keys.

**Long-polling.** `Lease` with `wait > 0` returns as soon as jobs are available or when `wait` elapses (empty result). Servers `LISTEN` on the Postgres channel `tasklane_jobs`; `Enqueue` sends `pg_notify('tasklane_jobs', queue)` inside its transaction, so the notification is delivered only on commit. Waiting leases for that queue wake immediately. Because delayed jobs and retries become due without any notification, waiting leases also re-check every second.

**Leasing from an unknown queue** returns `NOT_FOUND` (an empty result means "nothing runnable right now").

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

**Backpressure**
- *Producer side:* `Enqueue` checks the queue's depth (`available + leased`) against `max_depth`. To avoid a `COUNT(*)` on every call, each server refreshes each queue's depth once a second (one count over the partial indexes) and checks against that cached value, so the limit is soft (it can overshoot by about one second of traffic). The docs state this.
- *Worker side:* `LeaseStream` is credit-based. The server never sends more jobs than the worker has granted credits for, so a slow worker is never flooded.

**Auth:** a static API key per client, passed in the `authorization` metadata or header and configured in the server config file. mTLS or OAuth are recommended for real deployments and are out of scope for v1.

**SDKs:** Go (`github.com/mubtasim/tasklane/sdk/go`), TypeScript (`@mubtasim/tasklane`) and C# (`Tasklane.Client` on NuGet). Each provides a producer client and a worker runner that handles leasing, concurrency, heartbeats, ack/nack, retrying the server connection with backoff, and graceful shutdown.

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

- **Metrics** (`/metrics`):
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
| M2b | early Dec | Go SDK (producer + worker runner with heartbeats), docker-compose demo with a failing demo worker |
| M3 | mid-Dec | Pruner, DLQ listing and redrive, cancel, backpressure, `LeaseStream` |
| M4 | end of Dec | Metrics, tracing, Grafana dashboard, chaos and invariant tests |
| M5 | mid-Jan 2027 | TS and C# SDKs, load tests and benchmark report, v1.0.0 release and a blog post |
