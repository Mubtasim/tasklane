# Tasklane

A self-hosted, language-agnostic job queue server backed by PostgreSQL.

Producers enqueue jobs; workers lease, run and acknowledge them. At-least-once delivery, retries with exponential backoff and jitter, a dead-letter queue, delayed jobs and priorities.

**Status:** early development (milestone M2a: API server). Not ready for production use.

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

## Development

Requires Go and Docker (integration tests start PostgreSQL in a container).

    go test ./...

The API is defined in `proto/tasklane/v1/tasklane.proto`. After changing it, run `buf lint && buf generate` (buf and the plugins are installed with `go install`; see `docs/plans/2026-10-18-m2a-api-server.md`, Task 1) and commit `gen/`.

## License

Apache-2.0
