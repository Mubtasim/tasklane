# Tasklane

A self-hosted, language-agnostic job queue server backed by PostgreSQL.

Producers enqueue jobs; workers lease, run and acknowledge them. At-least-once delivery, retries with exponential backoff and jitter, a dead-letter queue, delayed jobs and priorities.

**Status:** early development (milestone M1: storage core). Not ready for production use.

- Design: [docs/design.md](docs/design.md)

## Development

Requires Go and Docker (integration tests start PostgreSQL in a container).

    go test ./...

## License

Apache-2.0
