# Changelog

## Unreleased

- First version: producer (`enqueue`, `enqueueBatch`, `createQueue`, `getQueue`), admin calls (`getJob`, `cancelJob`, `listDeadJobs`, `redriveJobs`, `redriveAll`) and a worker with heartbeats, retries and graceful shutdown, matching the Go SDK.
