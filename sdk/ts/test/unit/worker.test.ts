import { ConnectError } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";
import { Code } from "../../src/errors.js";
import { Worker } from "../../src/worker.js";
import type { Handler } from "../../src/job.js";
import { fast } from "./fast.js";
import { FakeServer } from "./fake.js";
import { memoryLogger, until } from "./helpers.js";

function setup(handlers: Record<string, Handler>, concurrency = 1) {
  const fake = new FakeServer();
  fake.addQueue("q", 1000, 3);
  const log = memoryLogger();
  const worker = new Worker(fake.client(), "q", { concurrency, handlers, logger: log.logger }, fast);
  return { fake, worker, log };
}

describe("Worker lease loop", () => {
  it("runs every job and acks it", async () => {
    const seen: bigint[] = [];
    const { fake, worker } = setup({ a: (job) => void seen.push(job.id) }, 2);
    const ids = [1, 2, 3, 4, 5].map(() => fake.add("q", "a"));
    const done = worker.run();
    await until(() => fake.callsOf("ack").length === 5);
    await worker.shutdown();
    await done;
    expect([...seen].sort()).toEqual(ids);
  });

  it("asks only for free slots", async () => {
    const gates: Array<() => void> = [];
    const { fake, worker } = setup({ a: () => new Promise<void>((r) => gates.push(r)) }, 3);
    for (let i = 0; i < 3; i++) fake.add("q", "a");
    void worker.run();
    await until(() => gates.length === 3);
    expect(fake.callsOf("lease")[0]!.req.maxJobs).toBe(3);
    gates[0]!();
    await until(() => fake.callsOf("lease").some((c) => c.req.maxJobs === 1));
    gates.slice(1).forEach((g) => g());
    await worker.shutdown();
  });

  it("gives back the slots an empty lease did not fill", async () => {
    const { fake, worker } = setup({ a: () => undefined }, 2);
    void worker.run();
    await until(() => fake.callsOf("lease").length >= 3); // three empty long-polls
    fake.add("q", "a");
    fake.add("q", "a");
    await until(() => fake.callsOf("ack").length === 2, 1000);
    await worker.shutdown();
  });

  it("rejects run() when the queue does not exist", async () => {
    const fake = new FakeServer();
    const worker = new Worker(fake.client(), "missing", { handlers: {} }, fast);
    await expect(worker.run()).rejects.toMatchObject({ code: Code.NotFound });
  });

  it("rejects run() when leasing hits a fatal error", async () => {
    const { fake, worker } = setup({});
    fake.fail("lease", new ConnectError("bad key", Code.Unauthenticated));
    await expect(worker.run()).rejects.toMatchObject({ code: Code.Unauthenticated, message: expect.stringContaining('lease from "q"') });
  });

  it("retries a failed lease with backoff", async () => {
    const { fake, worker, log } = setup({ a: () => undefined });
    fake.fail("lease", new ConnectError("down", Code.Unavailable), new ConnectError("down", Code.Unavailable));
    fake.add("q", "a");
    void worker.run();
    await until(() => fake.callsOf("ack").length === 1);
    await worker.shutdown();
    expect(log.lines.filter((l) => l.message.includes("lease failed"))).toHaveLength(2);
  });

  it("retries GetQueue while the server is unreachable", async () => {
    const { fake, worker } = setup({ a: () => undefined });
    fake.fail("getQueue", new ConnectError("starting", Code.Unavailable));
    fake.add("q", "a");
    void worker.run();
    await until(() => fake.callsOf("ack").length === 1);
    await worker.shutdown();
    expect(fake.callsOf("getQueue")).toHaveLength(2);
  });

  it("ignores jobs beyond what it asked for", async () => {
    const { fake, worker, log } = setup({ a: () => undefined }, 1);
    fake.extraOnNextLease = 2;
    for (let i = 0; i < 3; i++) fake.add("q", "a");
    void worker.run();
    await until(() => log.lines.some((l) => l.message.includes("more jobs than requested")));
    await until(() => fake.callsOf("ack").length === 1);
    await worker.shutdown();
    expect(fake.callsOf("ack")).toHaveLength(1);
  });

  it("nacks a job whose kind has no handler", async () => {
    const { fake, worker } = setup({});
    fake.add("q", "mystery");
    void worker.run();
    await until(() => fake.callsOf("nack").length >= 1);
    await worker.shutdown();
    expect(fake.callsOf("nack")[0]!.req.error).toBe('no handler for kind "mystery"');
  });

  it("nacks with the handler's error message", async () => {
    const { fake, worker } = setup({
      a: () => {
        throw new Error("boom");
      },
    });
    fake.add("q", "a");
    void worker.run();
    await until(() => fake.callsOf("nack").length >= 1);
    await worker.shutdown();
    expect(fake.callsOf("nack")[0]!.req.error).toBe("boom");
  });

  it("uses the visibility timeout option over the queue's", async () => {
    const fake = new FakeServer();
    fake.addQueue("q", 1000, 3);
    fake.add("q", "a");
    const worker = new Worker(fake.client(), "q", { handlers: { a: () => undefined }, visibilityTimeoutMs: 4500 }, fast);
    void worker.run();
    await until(() => fake.callsOf("ack").length === 1);
    await worker.shutdown();
    expect(Number(fake.callsOf("lease")[0]!.req.visibilityTimeout.seconds)).toBe(4);
    expect(fake.callsOf("lease")[0]!.req.visibilityTimeout.nanos).toBe(500_000_000);
  });

  it("refuses a second run() and a bad concurrency", async () => {
    const { worker } = setup({});
    const first = worker.run();
    await expect(worker.run()).rejects.toThrow(/more than once/);
    await worker.shutdown();
    await first;
    const fake = new FakeServer();
    expect(() => new Worker(fake.client(), "q", { handlers: {}, concurrency: 0 })).toThrow(RangeError);
  });

  it("resolves shutdown() at once when run() was never called", async () => {
    const { worker } = setup({});
    await expect(worker.shutdown()).resolves.toBeUndefined();
  });
});
