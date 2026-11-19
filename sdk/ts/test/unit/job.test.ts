import { ConnectError } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";
import { Code } from "../../src/errors.js";
import { retryAt, type Handler, type Result } from "../../src/job.js";
import { Worker } from "../../src/worker.js";
import { FakeServer } from "./fake.js";
import { memoryLogger, until } from "./helpers.js";
import { fast } from "./fast.js";

function setup(handlers: Record<string, Handler>, vtMs = 300) {
  const fake = new FakeServer();
  fake.addQueue("q", vtMs, 3);
  const log = memoryLogger();
  const results: Result[] = [];
  const worker = new Worker(fake.client(), "q", { handlers, logger: log.logger, onResult: (r) => results.push(r) }, fast);
  return { fake, worker, log, results };
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("running a job", () => {
  it("heartbeats every VT/3 while the handler runs", async () => {
    const { fake, worker, results } = setup({ a: () => wait(700) });
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(fake.callsOf("heartbeat").length).toBeGreaterThanOrEqual(5); // 700 ms / 100 ms
    expect(results[0]!.outcome).toBe("acked");
  });

  it("aborts the handler and sends nothing when the lease is lost", async () => {
    let aborted = false;
    const { fake, worker, results } = setup({
      a: (_job, signal) =>
        new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => {
            aborted = true;
            reject(signal.reason);
          });
        }),
    });
    fake.fail("heartbeat", new ConnectError("stale", Code.FailedPrecondition));
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(aborted).toBe(true);
    expect(results[0]!.outcome).toBe("lease_lost");
    expect(fake.callsOf("ack")).toHaveLength(0);
    expect(fake.callsOf("nack")).toHaveLength(0);
  });

  it("retries an Ack until it gets through", async () => {
    const { fake, worker, results } = setup({ a: () => undefined });
    fake.fail("ack", new ConnectError("down", Code.Unavailable), new ConnectError("down", Code.Unavailable));
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(results[0]!.outcome).toBe("acked");
    expect(fake.callsOf("ack")).toHaveLength(3);
  });

  it("gives up on the Ack once the lease would have expired", async () => {
    const { fake, worker, results } = setup({ a: () => undefined });
    fake.fail("ack", ...Array.from({ length: 1000 }, () => new ConnectError("down", Code.Unavailable)));
    fake.add("q", "a");
    const started = performance.now();
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(results[0]!.outcome).toBe("gave_up");
    expect(fake.callsOf("ack").length).toBeGreaterThan(1);
    expect(performance.now() - started).toBeLessThan(300 + 500); // VT plus slack
  });

  it("reports a refused Nack as lease_lost", async () => {
    const { fake, worker, results } = setup({
      a: () => {
        throw new Error("boom");
      },
    });
    fake.fail("nack", new ConnectError("stale", Code.FailedPrecondition));
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(results[0]).toMatchObject({ outcome: "lease_lost", error: { message: "boom" } });
  });

  it("nacks with retry_at when the handler throws retryAt", async () => {
    const at = new Date(Date.now() + 3_600_000);
    const { fake, worker, results } = setup({
      a: () => {
        throw retryAt(new Error("rate limited"), at);
      },
    });
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    const req = fake.callsOf("nack")[0]!.req;
    expect(req.error).toBe("rate limited");
    expect(Number(req.retryAt.seconds)).toBe(Math.floor(at.getTime() / 1000));
    expect(results[0]!.outcome).toBe("nacked");
  });

  it("calls onResult exactly once per leased job and survives a throwing onResult", async () => {
    const fake = new FakeServer();
    fake.addQueue("q", 300, 3);
    const log = memoryLogger();
    let calls = 0;
    const worker = new Worker(
      fake.client(),
      "q",
      {
        concurrency: 3,
        handlers: { a: () => undefined },
        logger: log.logger,
        onResult: () => {
          calls++;
          throw new Error("observer bug");
        },
      },
      fast,
    );
    for (let i = 0; i < 6; i++) fake.add("q", "a");
    void worker.run();
    await until(() => fake.callsOf("ack").length === 6);
    await until(() => calls === 6);
    await worker.shutdown();
    expect(calls).toBe(6);
    expect(log.lines.filter((l) => l.message.includes("onResult threw"))).toHaveLength(6);
  });

  it("warns when the event loop was blocked past a heartbeat", async () => {
    const { fake, worker, log, results } = setup({
      a: async () => {
        await wait(20); // let the heartbeat timer start
        const end = performance.now() + 400;
        while (performance.now() < end) {
          // busy: the heartbeat due at +100 ms fires about 300 ms late
        }
      },
    });
    fake.add("q", "a");
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    expect(log.lines.some((l) => l.level === "warn" && /event loop blocked for about \d+ s; heartbeats were delayed/.test(l.message))).toBe(true);
  });
});
