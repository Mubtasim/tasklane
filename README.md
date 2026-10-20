# Tasklane

A self-hosted, language-agnostic job queue server backed by PostgreSQL.

Producers enqueue jobs; workers lease, run and acknowledge them. At-least-once delivery, retries with exponential backoff and jitter, a dead-letter queue, delayed jobs and priorities.

**Status:** early development (milestone M2b: Go SDK and demo). Not ready for production use.

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

| Variable | Default | |
|---|---|---|
| `TASKLANE_DATABASE_URL` | (required) | PostgreSQL connection string |
| `TASKLANE_GRPC_ADDR` | `:7070` | gRPC listen address |
| `TASKLANE_HTTP_ADDR` | `:8080` | HTTP/JSON gateway listen address |
| `TASKLANE_API_KEYS` | empty (auth off) | comma-separated API keys |
| `TASKLANE_REAPER_INTERVAL` | `5s` | how often expired leases are reaped |

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

## Development

Requires Go and Docker (integration tests start PostgreSQL in a container).

    go test ./...

The API is defined in `proto/tasklane/v1/tasklane.proto`. After changing it, run `buf lint && buf generate` and commit `gen/`. buf and the plugins install with `go install`:

    go install github.com/bufbuild/buf/cmd/buf@v1.74.0
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
    go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.32.0

## License

Apache-2.0
