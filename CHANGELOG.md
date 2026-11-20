# Changelog

One version covers the server image, the Go SDK, the TypeScript SDK and the Helm chart (design §22). Release steps: [docs/releasing.md](docs/releasing.md).

## 1.0.0-rc.2

Second release candidate: the first one published by the release workflow (image and chart on GHCR, npm through trusted publishing with provenance). No code changes since rc.1.

- Docs: the first-release steps in docs/releasing.md now cover the npm login link opened on another device, the trusted publisher's allowed actions, and staged publishing as a later option.

## 1.0.0-rc.1

First release candidate of 1.0.0.

- **Server** (`ghcr.io/mubtasim/tasklane`, linux/amd64 and linux/arm64): a job queue server on PostgreSQL with gRPC and HTTP/JSON APIs. Leases with heartbeats and fencing tokens; retries with backoff and jitter; a dead-letter queue with listing and redrive; delayed jobs and strict priorities; cancel; backpressure when a queue is full; pruning of old jobs; Prometheus metrics (`/metrics`, now with `tasklane_build_info`); OpenTelemetry tracing; `tasklane -version`.
- **Go SDK** (`github.com/mubtasim/tasklane/sdk/go`): now its own Go module (Go 1.26 or newer) with only the SDK's dependencies. The import path `github.com/mubtasim/tasklane/sdk/go/tasklane` is unchanged. Producer, admin calls and a worker with heartbeats, retries and graceful shutdown.
- **TypeScript SDK** (`@mubtasim91/tasklane`, Node 22 or newer): first version; producer (`enqueue`, `enqueueBatch`, `createQueue`, `getQueue`), admin calls (`getJob`, `cancelJob`, `listDeadJobs`, `redriveJobs`, `redriveAll`) and a worker with heartbeats, retries and graceful shutdown, matching the Go SDK.
- **Helm chart** (`oci://ghcr.io/mubtasim/charts/tasklane`): the server behind Envoy, probes, graceful shutdown, disruption budgets.
- **Upgrading from source builds:** generated Go code moved from `gen/tasklane/v1` to `sdk/go/gen/tasklane/v1`. `go install github.com/mubtasim/tasklane/cmd/...@version` is not supported; run the image.
