// A TypeScript SDK worker for the cross-language invariant test (design
// §21.5). It runs the test's job kinds and appends one JSON line per run to
// <LEDGER_DIR>/<WORKER_NAME>.runs.jsonl in the Go ledger format
// (internal/ledger.Run), so the Go checker reads it. One append per record:
// a SIGKILL can cut off at most the last line, which the reader drops.
import { appendFileSync } from "node:fs";
import { join } from "node:path";
import { Worker, createClient } from "@mubtasim91/tasklane";

const env = (k) => {
  const v = process.env[k];
  if (!v) throw new Error(`${k} is required`);
  return v;
};
const name = env("WORKER_NAME");
const file = join(env("LEDGER_DIR"), `${name}.runs.jsonl`);
const client = createClient(env("TASKLANE_ADDR"));

const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    }, { once: true });
  });

const quiet = { info() {}, warn() {}, error: (m, f) => console.error(`[${name}]`, m, f ?? "") };

const worker = new Worker(client, process.env.QUEUE || "inv", {
  concurrency: 5,
  shutdownTimeoutMs: 1000,
  logger: quiet,
  handlers: {
    // normal takes a few milliseconds, varied by job, as in harness_test.go.
    normal: (job, signal) => sleep(5 + Number(job.id % 20n), signal),
    // flaky fails its first `fails` attempts, then succeeds.
    flaky: (job) => {
      if (job.attempt <= (job.payload.fails ?? 0)) throw new Error("flaky: planned failure");
    },
    poison: () => {
      throw new Error("poison: always fails");
    },
  },
  onResult: (r) => {
    const rec = {
      worker: name,
      job_id: Number(r.job.id),
      attempt: r.job.attempt,
      start: r.start.toISOString(),
      end: r.end.toISOString(),
      outcome: r.outcome,
    };
    if (r.error) rec.err = r.error.message;
    appendFileSync(file, JSON.stringify(rec) + "\n");
  },
});

process.on("SIGTERM", () => void worker.shutdown());
await worker.run();
client.close();
