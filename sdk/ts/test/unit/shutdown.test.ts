import { describe, expect, it } from "vitest";
import type { Handler, Result } from "../../src/job.js";
import { Worker } from "../../src/worker.js";
import { FakeServer } from "./fake.js";
import { memoryLogger, until } from "./helpers.js";
import { fast } from "./fast.js";

function setup(handlers: Record<string, Handler>, shutdownTimeoutMs: number) {
  const fake = new FakeServer();
  fake.addQueue("q", 5000, 3);
  const log = memoryLogger();
  const results: Result[] = [];
  const started: bigint[] = [];
  const wrapped = Object.fromEntries(
    Object.entries(handlers).map(([k, h]) => [k, ((job, signal) => (started.push(job.id), h(job, signal))) as Handler]),
  );
  const worker = new Worker(fake.client(), "q", { concurrency: 2, shutdownTimeoutMs, handlers: wrapped, logger: log.logger, onResult: (r) => results.push(r) }, fast);
  return { fake, worker, log, results, started };
}

describe("shutdown", () => {
  it("lets running jobs finish within the timeout and acks them", async () => {
    const { fake, worker, results, started } = setup({ a: () => new Promise((r) => setTimeout(r, 150)) }, 1000);
    fake.add("q", "a");
    const done = worker.run();
    await until(() => started.length === 1);
    await worker.shutdown();
    await done;
    expect(results.map((r) => r.outcome)).toEqual(["acked"]);
    expect(fake.callsOf("nack")).toHaveLength(0);
  });

  it("aborts and nacks jobs still running at the deadline, within timeout + 2 s", async () => {
    let signalled = 0;
    const { fake, worker, results, started } = setup(
      {
        slow: (_job, signal) =>
          new Promise<void>((_resolve, reject) =>
            signal.addEventListener("abort", () => {
              signalled++;
              reject(signal.reason);
            }),
          ),
      },
      200,
    );
    fake.add("q", "slow");
    fake.add("q", "slow");
    void worker.run();
    await until(() => started.length === 2);
    const t0 = performance.now();
    await worker.shutdown();
    expect(performance.now() - t0).toBeLessThan(200 + 200 + 300); // timeout + finishTimeout + slack
    expect(signalled).toBe(2);
    expect(fake.callsOf("nack").map((c) => c.req.error)).toEqual(["worker shutting down", "worker shutting down"]);
    expect(results.map((r) => [r.outcome, r.error?.message])).toEqual([
      ["nacked", "worker shutting down"],
      ["nacked", "worker shutting down"],
    ]);
  });

  it("does not nack a job whose handler finished just in time", async () => {
    const { fake, worker, results, started } = setup({ a: () => new Promise((r) => setTimeout(r, 120)) }, 200);
    fake.add("q", "a");
    void worker.run();
    await until(() => started.length === 1);
    await worker.shutdown();
    expect(results.map((r) => r.outcome)).toEqual(["acked"]);
  });

  it("returns the same promise from a second shutdown()", async () => {
    const { worker } = setup({}, 100);
    void worker.run();
    const [a, b] = [worker.shutdown(), worker.shutdown()];
    expect(a).toBe(b);
    await Promise.all([a, b]);
  });

  it("shares the grace between an in-flight Ack and a shutdown Nack", async () => {
    const { fake, worker, results, started } = setup(
      {
        quick: () => new Promise((r) => setTimeout(r, 100)),
        slow: (_job, signal) => new Promise<void>((_res, rej) => signal.addEventListener("abort", () => rej(signal.reason))),
      },
      300,
    );
    fake.ackDelayMs = 150;
    fake.add("q", "quick");
    fake.add("q", "slow");
    const done = worker.run();
    await until(() => started.length === 2);
    const t0 = performance.now();
    await worker.shutdown();
    await done;
    expect(performance.now() - t0).toBeLessThan(300 + 200 + 300);
    expect(results.map((r) => [r.job.kind, r.outcome]).sort()).toEqual([
      ["quick", "acked"],
      ["slow", "nacked"],
    ]);
  });
});
