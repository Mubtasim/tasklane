# Tasklane

[![CI](https://github.com/Mubtasim/tasklane/actions/workflows/ci.yml/badge.svg)](https://github.com/Mubtasim/tasklane/actions/workflows/ci.yml)

A self-hosted, language-agnostic job queue server backed by PostgreSQL.

Producers enqueue jobs; workers lease, run and acknowledge them. At-least-once delivery, retries with exponential backoff and jitter, a dead-letter queue, delayed jobs and priorities.

**Status:** early development (milestone M3a: cancel, dead-letter operations, pruning, backpressure). Not ready for production use.

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

## Demo

One command starts PostgreSQL, the server, a producer (about 10 jobs a second: 85% normal, 12% flaky, 3% poison) and three workers with 5 slots each. It needs Docker and about 250 MB of RAM.

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

## Development

Requires Go and Docker (integration tests start PostgreSQL in a container).

    go test ./...

The API is defined in `proto/tasklane/v1/tasklane.proto`. After changing it, run

    go tool -modfile=tools.mod buf lint
    go tool -modfile=tools.mod buf generate

and commit `gen/`. buf and the protoc plugins are pinned in `tools.mod`, so there is nothing to install. Lint with [golangci-lint](https://golangci-lint.run) v2 (`golangci-lint run ./...`); CI runs the linters, the tests with `-race`, `buf lint`, `buf breaking` and the image build on every push and pull request.

## License

Apache-2.0
