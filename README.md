# Tasklane

[![CI](https://github.com/Mubtasim/tasklane/actions/workflows/ci.yml/badge.svg)](https://github.com/Mubtasim/tasklane/actions/workflows/ci.yml)

A self-hosted, language-agnostic job queue server backed by PostgreSQL.

Producers enqueue jobs; workers lease, run and acknowledge them. At-least-once delivery, retries with exponential backoff and jitter, a dead-letter queue, delayed jobs and priorities.

**Status:** early development (milestone M4b: invariant and chaos tests, two servers behind Envoy). Not ready for production use.

- Design: [docs/design.md](docs/design.md)

## Running the server

Requires Go and PostgreSQL. The server applies its migrations on start.

```bash
docker run -d --rm --name tasklane-pg -e POSTGRES_USER=tasklane -e POSTGRES_PASSWORD=tasklane \
  -e POSTGRES_DB=tasklane -p 5432:5432 postgres:17-alpine
TASKLANE_DATABASE_URL='postgres://tasklane:tasklane@localhost:5432/tasklane?sslmode=disable' \
  go run ./cmd/tasklane
```

gRPC listens on `:7070`, HTTP/JSON on `:8080` (`/healthz`, `/readyz`, `/v1/...`). Set `TASKLANE_API_KEYS=key1,key2` to require `Authorization: Bearer <key>`.

```bash
curl -d '{"name":"emails"}' localhost:8080/v1/queues
curl -d '{"kind":"send_welcome","payload":{"user":42}}' localhost:8080/v1/queues/emails/jobs
curl -d '{"max_jobs":1,"wait":"10s"}' localhost:8080/v1/queues/emails:lease
```

With API keys set, add the header to every `/v1` request:

```bash
curl -H "Authorization: Bearer key1" -d '{"name":"emails"}' localhost:8080/v1/queues
```

**Upgrading to M3a.** Migration `0002` rebuilds one partial index (`jobs_dead`) and blocks writes to `jobs` while it builds. The first pruner pass on an existing database deletes the whole completed and cancelled backlog older than the retention (in batches, but expect a WAL and autovacuum burst).

| Variable | Default | |
|---|---|---|
| `TASKLANE_DATABASE_URL` | (required) | PostgreSQL connection string |
| `TASKLANE_GRPC_ADDR` | `:7070` | gRPC listen address |
| `TASKLANE_HTTP_ADDR` | `:8080` | HTTP/JSON gateway listen address |
| `TASKLANE_API_KEYS` | empty (auth off) | comma-separated API keys |
| `TASKLANE_REAPER_INTERVAL` | `5s` | how often expired leases are reaped |
| `TASKLANE_RETENTION` | `24h` | completed and cancelled jobs are deleted after this; also the idempotency-key window |
| `TASKLANE_DEAD_RETENTION` | `168h` | dead jobs are deleted after this (like an SQS dead-letter queue) |
| `TASKLANE_PRUNE_INTERVAL` | `1m` | how often old jobs are pruned |

## Go SDK

```go
import "github.com/mubtasim/tasklane/sdk/go/tasklane"

client, err := tasklane.Dial("localhost:7070", tasklane.WithAPIKey("key1"))
defer client.Close()

// Producer
client.Enqueue(ctx, "emails", tasklane.Job{Kind: "send_welcome", Payload: map[string]any{"user": 42}})

// Worker: one long-poll per process, up to Concurrency jobs at once,
// heartbeats for long jobs, graceful shutdown when ctx is cancelled.
w := client.NewWorker("emails", tasklane.WorkerOptions{Concurrency: 5})
w.Handle("send_welcome", func(ctx context.Context, job *tasklane.LeasedJob) error {
    var p struct{ User int `json:"user"` }
    if err := job.Decode(&p); err != nil {
        return err
    }
    return sendWelcome(ctx, p.User) // nil acks the job, an error retries it with backoff
})
err = w.Run(ctx)
```

Delivery is at-least-once: a job can run twice (for example after a worker crashes before acking), so handlers should be idempotent.

To see how each run ended (whether the server accepted the Ack or Nack), set `OnResult`. It is called once per run, from the job's goroutine, so it must be safe for concurrent use:

```go
w := client.NewWorker("emails", tasklane.WorkerOptions{Concurrency: 5, OnResult: func(r tasklane.Result) {
    // r.Outcome: OutcomeAcked, OutcomeNacked, OutcomeLeaseLost or OutcomeGaveUp
    log.Printf("job %d attempt %d: %s in %v", r.Job.ID, r.Job.Attempt, r.Outcome, r.End.Sub(r.Start))
}})
```

## Demo

One command starts PostgreSQL, two servers behind Envoy, a producer (about 10 jobs a second: 85% normal, 12% flaky, 3% poison), three workers with 5 slots each, Prometheus and Grafana. It needs Docker and about 700 MB of RAM. Envoy owns the host ports 7070 and 8080 and balances each request across the two servers, so every client sees one address.

The main way to watch is the Grafana dashboard at http://localhost:3000 (see [Monitoring](#monitoring)). `tasklane-watch` is a lighter alternative in the terminal.

A tmux layout that works well (`Ctrl+b |` and `Ctrl+b -` split panes):

```bash
# pane 1: the stack
docker compose -f deploy/docker-compose.yml up --build
# pane 2: live counts, redrawn every second
go run ./cmd/tasklane-watch -api-key demo-key
# pane 3: what the workers are doing
docker compose -f deploy/docker-compose.yml logs -f worker-1 worker-2 worker-3
# pane 4: break things (commands below)
```

`tasklane-watch` shows `queue=demo  available … | leased … | dead … | completed …/s`. Poison jobs reach `dead` after 5 attempts; flaky jobs fail sometimes and succeed on a retry.

Two ways to stop a worker:

```bash
docker compose -f deploy/docker-compose.yml kill worker-1   # SIGKILL: a crash
docker compose -f deploy/docker-compose.yml stop worker-2   # SIGTERM: a graceful shutdown
```

- **Crash:** worker-1's jobs stay `leased` until their 15 s visibility timeout expires; then the reaper makes them available and the other workers finish them.
- **Graceful shutdown:** worker-2 stops leasing, lets running jobs finish (up to 25 s), nacks any that don't, and exits. Its jobs come back within seconds.

Bring them back with `docker compose -f deploy/docker-compose.yml start worker-1 worker-2`, and stop everything with `docker compose -f deploy/docker-compose.yml down`.

### Operations

Against the running demo (the HTTP gateway is on `localhost:8080`, the demo API key is `demo-key`):

```bash
H='Authorization: Bearer demo-key'
# Dead jobs, newest first (poison jobs land here after 5 attempts)
curl -H "$H" 'localhost:8080/v1/queues/demo/dead?page_size=5'
# Send every dead job back with its attempts reset; watch `dead` drop and `available` jump in tasklane-watch
curl -H "$H" -d '{"all":true}' localhost:8080/v1/queues/demo/dead:redrive
# Cancel a job that hasn't run yet: enqueue one for later, then cancel it by its job_id
curl -H "$H" -d '{"kind":"normal","run_at":"2030-01-01T00:00:00Z"}' localhost:8080/v1/queues/demo/jobs
curl -H "$H" -d '{}' localhost:8080/v1/jobs/<job_id>:cancel
```

Backpressure: a queue refuses enqueues once its available and leased jobs reach `max_depth` (HTTP 429 with `Retry-After`, `tasklane.ErrQueueFull` in the Go SDK). The limit is soft: each server recounts once a second. To see it, run a second producer into a small queue that no worker reads:

```bash
TASKLANE_API_KEY=demo-key DEMO_QUEUE=bp DEMO_MAX_DEPTH=50 go run ./examples/demo/producer
```

After about 50 jobs it logs `queue full, retry after` once a second.

## Monitoring

The server exposes Prometheus metrics on `:9464/metrics` (`TASKLANE_METRICS_ADDR`; `off` disables them). The port is separate from the API, so you can keep it internal. The demo runs Prometheus and Grafana with a ready-made dashboard: open http://localhost:3000.

![Tasklane dashboard with a worker killed and restarted](docs/images/dashboard.png)

The screenshot shows "Dead (last hour)" in red because the demo produces poison jobs on purpose.

- **Top row:** throughput, available and leased jobs, jobs that died in the last hour, the oldest ready job's wait, and refused enqueues.
- **Jobs per second by outcome** and **queue depth by state:** the flow through the queue.
- **Queue wait** (ready to leased, p50/p99), **RPC latency** and **RPC errors by code.**
- **DB pool, goroutines and memory** for the servers.
- **Servers up** (red below 2) and **Requests per server:** whether both servers are alive and how Envoy splits the traffic.

Try it: kill a worker with `docker compose -f deploy/docker-compose.yml kill worker-1`. The acked rate holds at about 9.7 jobs/s, because the two other workers have spare capacity. Available jobs climb to about 30 and queue wait p99 to about 5 s while they catch up. Once the demo's 15 s visibility timeout passes, about 4 jobs (the killed worker's leases) are reaped and returned to the queue. `docker compose -f deploy/docker-compose.yml start worker-1` brings everything back to normal. Throughput only drops when the remaining workers can't keep up. Run `docker compose -f deploy/docker-compose.yml stop worker-2 worker-3`: one worker (5 slots) handles about 4 jobs/s against 10/s arriving, so acked/s drops and available grows until you `start` them again.

Try killing a server: `docker compose -f deploy/docker-compose.yml kill tasklane-1`. "Servers up" drops to 1 and "Requests per server" moves to `tasklane-2`, which went from about 16 to about 33 requests/s. After a hard kill, new calls fail fast for about 14-18 s, most likely until Envoy's health checks (2 s, 2 failures) and DNS re-resolution (5 s) drop the dead server (upstream keepalive and a retry rule that avoids the previous host didn't shorten it): in the run I measured, the producer and workers logged 25 failed calls (mostly Unavailable) between 0.2 s and 18 s after the kill, none after that. They retry, so throughput held. The acked rate was about 9.3 jobs/s with one server (9.6 before). `docker compose -f deploy/docker-compose.yml start tasklane-1` and the traffic splits again, about 16 and 17 requests/s, with the acked rate back at about 9.5 jobs/s.

Example alert rules are in `deploy/prometheus/alerts.yml` (server down, jobs stuck, dead jobs, high reap rate, queue full, RPC errors, stale queue stats); see them at http://localhost:9090/alerts. The demo's poison jobs keep `TasklaneDeadJobs` firing, as they should. `TasklaneStatsStale` fires when a server's cached queue depths are over 30 s old, which usually means it can't reach the database. With several server copies, aggregate the queue gauges with `max by (queue)` (every server reports the same database-wide numbers) and counters with `sum`. All metrics are listed in [design §16.2](docs/design.md#162-metrics).

## Correctness

Two tests run jobs through two servers while things break, then check five rules against what the workers and the producer recorded (their ledgers) and the final `jobs` table:

1. **Nothing lost or stuck:** every confirmed enqueue has a job, and every job ends `completed` or `dead`.
2. **No duplicate jobs:** one job per idempotency key, even when an enqueue was retried after its server died.
3. **Fencing works:** two runs never hold the same attempt, an accepted Ack is always from the job's last attempt, a completed job has at most one accepted Ack and a dead job none.
4. **Repeat runs are justified:** a job runs again only after a Nack, a failed handler, or a gap of at least one visibility timeout (its lease could have expired).
5. **Counters match the database** (in-process test only): the acked and dead counters equal the completed and dead counts.

The details, including what each rule tolerates, are in [design §17](docs/design.md#17-m4b-invariant-and-chaos-tests-two-servers-behind-envoy).

**Invariant test** (in-process, part of `go test ./...`): one Postgres, two servers with their own reapers, SDK workers with flaky and poison jobs, two zombie workers that ack after their lease expired, and two workers whose connection is cut mid-run.

```bash
go test -count=1 ./test/invariant/                      # 10 workers × 1,000 jobs, about 8 s (12 s for the package)
go test -count=1 ./test/invariant/ -invariant.full      # 50 workers × 10,000 jobs, about 1m17s, ~380 MB
go test -count=1 ./test/invariant/ -invariant.seed=42   # replay a seed (the job mix and stops, not the timing)
```

It found a real bug: when a client disconnected during an Ack, Postgres could commit while the server saw `context canceled` and skipped its metrics, so the counters drifted from the database (about 1 run in 60). Store writes now run detached from the client.

**Chaos test** (Docker, about 3m45s once the image is built, the demo stack's ~700 MB; stop the demo first, since it uses the same ports):

```bash
go test -tags chaos -count=1 -v -timeout 15m ./test/chaos/
go test -tags chaos -count=1 -v -timeout 15m ./test/chaos/ -chaos.seed=1   # replay a fault sequence
```

For 3 minutes it injects one fault about every 15 s: kill a worker (back after 10 s), kill a server (back after 15 s), pause a worker for 15 s (longer than the 10 s visibility timeout, so it comes back as a zombie whose late Acks must be rejected), or restart Postgres. Then it heals everything, waits for the queue to drain and checks rules 1–4. On failure the ledgers and compose logs are copied to `test/chaos/out/`.

CI runs the default invariant test on every pull request (inside `go test -race ./...`) and the chaos test nightly; the chaos workflow can also be started by hand with a seed.

## Development

Requires Go and Docker (integration tests start PostgreSQL in a container).

    go test ./...

The API is defined in `proto/tasklane/v1/tasklane.proto`. After changing it, run

    go tool -modfile=tools.mod buf lint
    go tool -modfile=tools.mod buf generate

and commit `gen/`. buf and the protoc plugins are pinned in `tools.mod`, so there is nothing to install. Lint with [golangci-lint](https://golangci-lint.run) v2 (`golangci-lint run ./...`); CI runs the linters, the tests with `-race`, `buf lint`, `buf breaking` and the image build on pushes to `main` and on pull requests.

## License

Apache-2.0
