# @mubtasim91/tasklane

TypeScript client and worker for [Tasklane](https://github.com/Mubtasim/tasklane), a self-hosted job queue server on PostgreSQL. Producers and workers in any language share one queue; this package matches the Go SDK's behaviour (leases, heartbeats, retries, dead-letter queue, graceful shutdown) and is checked against it by the same invariant test.

Node 22 or later. ESM only: use `import`, not `require()` (the tracing module uses top-level `await`, so `require()` of this package fails with `ERR_REQUIRE_ASYNC_MODULE`). Talks gRPC to the server's port 7070.

```bash
npm install @mubtasim91/tasklane
```

## Enqueue

```ts
import { Priority, createClient } from "@mubtasim91/tasklane";

const client = createClient("http://localhost:7070", { apiKey: process.env.TASKLANE_API_KEY });

const { jobId, created } = await client.enqueue("emails", {
  kind: "welcome",
  payload: { userId: 42 },          // a JSON object
  priority: Priority.High,
  runAt: new Date(Date.now() + 60_000),
  idempotencyKey: "welcome-42",     // a repeat returns the same job
});
// jobId is a bigint, e.g. 1234n
```

A full queue throws `QueueFullError` (with `retryAfterMs`); the SDK never retries it for you. Every other server error is a `TasklaneError` with the gRPC `code`. Call `client.close()` when you are done: the HTTP/2 connection otherwise keeps the process alive.

## Run a worker

```ts
import { Worker, retryAt, type LeasedJob } from "@mubtasim91/tasklane";

const worker = new Worker(client, "emails", {
  concurrency: 10,
  handlers: {
    welcome: async (job: LeasedJob<{ userId: number }>, signal: AbortSignal) => {
      const res = await fetch(`https://mail.example/send/${job.payload.userId}`, { method: "POST", signal });
      if (res.status === 429) throw retryAt(new Error("rate limited"), new Date(Date.now() + 30_000));
      if (!res.ok) throw new Error(`mail API ${res.status}`); // retried with backoff
    },                                                       // returning = done
  },
});

process.on("SIGTERM", () => void worker.shutdown());
await worker.run();
client.close();
```

- **Delivery is at least once.** Make handlers idempotent: a job can run again after a crash or a lost lease.
- **`signal`** aborts when the lease is lost or shutdown runs out of time. Pass it to `fetch` and database calls.
- **Shutdown** lets running jobs finish for 25 s (`shutdownTimeoutMs`), then nacks the rest within 2 s. Give the process at least 30 s to stop (Kubernetes `terminationGracePeriodSeconds`, Docker `stop_grace_period`; Docker's default is 10 s). `shutdown()` is safe to call twice: it returns the same promise.
- **Lost leases.** If the server refuses an Ack or Nack because the lease is gone (`FAILED_PRECONDITION` or `NOT_FOUND`), the run is reported as `lease_lost` and the signal is aborted.
- **CPU-heavy work** (image resizing, big parses, bcrypt) blocks Node's one thread, and with it the heartbeats: past the visibility timeout the job runs twice. Run such work in `worker_threads` or a child process. The worker logs `event loop blocked for about N s` when a heartbeat timer is more than 5 s late.
- **`onResult`** is called once per leased job with a `Result` (`job`, `outcome`, `start`, `end`, `error`). `outcome` is `acked`, `nacked`, `lease_lost` or `gave_up`.
- **Options:** `concurrency` (default 1), `visibilityTimeoutMs`, `shutdownTimeoutMs`, `handlers`, `onResult`, `logger` (anything with `info`, `warn` and `error`; defaults to `console`).

## Tracing

Install `@opentelemetry/api` and set up an OpenTelemetry SDK: enqueue calls send `traceparent`, and each run gets a `handle <kind>` span in the job's trace, so one trace shows the producer, the server and every attempt. A handler runs inside its `handle <kind>` span, so a job it enqueues joins the same trace (this needs a context manager, which the OpenTelemetry Node SDK registers). Without the package, tracing costs nothing.

## Admin calls

`getJob`, `cancelJob` (available jobs only), `listDeadJobs` (pages), `redriveJobs`, `redriveAll`, `createQueue`, `getQueue`.

## License

Apache-2.0
