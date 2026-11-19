// The compose demo's TypeScript worker (design §21.5): the same job kinds,
// environment and queue settings as the Go demo worker
// (examples/demo/worker), so both can share the demo queue.
import { hostname } from "node:os";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { Code, TasklaneError, Worker, createClient, type Client, type LeasedJob } from "@mubtasim91/tasklane";

const name = process.env.WORKER_NAME || hostname();
const queue = process.env.DEMO_QUEUE || "demo";
const concurrency = Number(process.env.DEMO_CONCURRENCY || "5");
const vtMs = parseDuration(process.env.DEMO_VISIBILITY_TIMEOUT) ?? 15_000;
const maxDepth = Number(process.env.DEMO_MAX_DEPTH || "0") || 0;
const rawAddr = process.env.TASKLANE_ADDR || "localhost:7070";
const addr = rawAddr.includes("://") ? rawAddr : `http://${rawAddr}`;

// Spans are exported only when an OTLP endpoint is set, as in the Go demo.
const otel = process.env.OTEL_EXPORTER_OTLP_ENDPOINT
  ? new NodeSDK({ serviceName: process.env.OTEL_SERVICE_NAME || name, traceExporter: new OTLPTraceExporter() })
  : undefined;
otel?.start();

function log(level: "INFO" | "WARN" | "ERROR", msg: string, fields: Record<string, unknown> = {}): void {
  const kv = Object.entries({ worker: name, ...fields })
    .map(([k, v]) => `${k}=${typeof v === "string" && v.includes(" ") ? JSON.stringify(v) : String(v)}`)
    .join(" ");
  console.log(`time=${new Date().toISOString()} level=${level} msg=${JSON.stringify(msg)} ${kv}`);
}

/** parseDuration reads a Go-style duration such as "15s", "500ms" or "2m". */
function parseDuration(s: string | undefined): number | undefined {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m)$/.exec(s ?? "");
  if (!m) return undefined;
  const n = Number(m[1]) * { ms: 1, s: 1000, m: 60_000 }[m[2] as "ms" | "s" | "m"];
  return n > 0 ? n : undefined;
}

/** work waits a random time in [lo, hi) ms, or rejects when signal aborts. */
function work(signal: AbortSignal, lo: number, hi: number): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const t = setTimeout(resolve, lo + Math.random() * (hi - lo));
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    }, { once: true });
  });
}

const fields = (job: LeasedJob) => ({ job_id: String(job.id), kind: job.kind, attempt: job.attempt });

/** ensureQueue creates the demo queue like demo.EnsureQueue: 5 attempts; an existing queue is fine. */
async function ensureQueue(client: Client, stop: AbortSignal): Promise<boolean> {
  for (;;) {
    try {
      await client.createQueue({ name: queue, maxDepth, visibilityTimeoutMs: vtMs, maxAttempts: 5 });
      return true;
    } catch (e) {
      if (e instanceof TasklaneError && e.code === Code.AlreadyExists) return true;
      if (!(e instanceof TasklaneError) || e.code !== Code.Unavailable) throw e;
      log("INFO", "waiting for the server", { err: e.message });
      await new Promise((r) => setTimeout(r, 1000));
      if (stop.aborted) return false;
    }
  }
}

const client = createClient(addr, { apiKey: process.env.TASKLANE_API_KEY || undefined });
const stop = new AbortController();
const worker = new Worker(client, queue, {
  concurrency,
  logger: {
    info: (m, f) => log("INFO", m, f),
    warn: (m, f) => log("WARN", m, f),
    error: (m, f) => log("ERROR", m, f),
  },
  handlers: {
    // normal simulates real work: 0.2–2 s, then success.
    normal: async (job: LeasedJob, signal: AbortSignal) => {
      await work(signal, 200, 2000);
      log("INFO", "job done", fields(job));
    },
    // flaky fails about 30% of its runs, so retries with backoff show up.
    flaky: async (job: LeasedJob, signal: AbortSignal) => {
      await work(signal, 100, 500);
      if (Math.random() < 0.3) {
        log("WARN", "job failed (simulated)", fields(job));
        throw new Error("simulated flaky failure");
      }
      log("INFO", "job done", fields(job));
    },
    // poison always fails, so the job uses up its attempts and becomes dead.
    poison: async (job: LeasedJob) => {
      log("WARN", "job failed (always fails)", { ...fields(job), max_attempts: job.maxAttempts });
      throw new Error("simulated poison job: always fails");
    },
  },
});

for (const sig of ["SIGTERM", "SIGINT"] as const) {
  process.on(sig, () => {
    stop.abort();
    void worker.shutdown();
  });
}

let code = 0;
try {
  if (await ensureQueue(client, stop.signal)) {
    log("INFO", "worker started", { queue, concurrency });
    await worker.run();
    log("INFO", "worker stopped cleanly");
  }
} catch (e) {
  log("ERROR", "worker stopped", { err: e instanceof Error ? e.message : String(e) });
  code = 1;
} finally {
  client.close();
  await otel?.shutdown();
}
process.exit(code);
