# Changelog

One version covers the server image, the Go SDK, the TypeScript SDK and the Helm chart (design §22). Release steps: [docs/releasing.md](docs/releasing.md).

## 1.0.0

First stable release. The code is the same as `1.0.0-rc.2`, which was installed from the published artifacts and taken through a full Helm lifecycle (install, scale, rolling key change, rollback, uninstall) with no jobs lost and clean shutdowns ([docs/labs/helm.md](docs/labs/helm.md)).

From here on, the public APIs follow semantic versioning: breaking changes only in a new major version.

- **Server** (`ghcr.io/mubtasim/tasklane:1.0.0`, linux/amd64 and linux/arm64): a job queue server on PostgreSQL with gRPC and HTTP/JSON APIs. Leases with heartbeats and fencing tokens; retries with backoff and jitter; a dead-letter queue with listing and redrive; delayed jobs and strict priorities; cancel; backpressure when a queue is full; pruning of old jobs; Prometheus metrics (`/metrics`, including `tasklane_build_info`); OpenTelemetry tracing; `tasklane -version`.
- **Go SDK** (`github.com/mubtasim/tasklane/sdk/go@v1.0.0`, Go 1.26 or newer): its own Go module with only the SDK's dependencies. Producer, admin calls and a worker with heartbeats, retries and graceful shutdown.
- **TypeScript SDK** (`@mubtasim91/tasklane@1.0.0`, Node 22 or newer): producer (`enqueue`, `enqueueBatch`, `createQueue`, `getQueue`), admin calls (`getJob`, `cancelJob`, `listDeadJobs`, `redriveJobs`, `redriveAll`) and a worker with heartbeats, retries and graceful shutdown, matching the Go SDK. Published with provenance.
- **Helm chart** (`oci://ghcr.io/mubtasim/charts/tasklane --version 1.0.0`): the server behind Envoy, readiness probes, graceful shutdown with a `preStop` pause, rolling updates that never drop below the requested number of ready servers, disruption budgets, and pod restarts when the API keys or the Envoy config change.

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
