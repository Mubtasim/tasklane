import { afterAll, describe, expect, it } from "vitest";
import { Code, QueueFullError, TasklaneError } from "../../src/index.js";
import { durationFromMs } from "../../src/internal/time.js";
import { newClient, queueName } from "./env.js";

const client = newClient();
afterAll(() => client.close());

describe("against a real server", () => {
  it("creates a queue and enqueues idempotently", async () => {
    const q = queueName();
    expect(await client.createQueue({ name: q, visibilityTimeoutMs: 5000, maxAttempts: 2 })).toMatchObject({ name: q, visibilityTimeoutMs: 5000, maxAttempts: 2 });
    const a = await client.enqueue(q, { kind: "a", payload: { n: 1 }, idempotencyKey: "k" });
    const b = await client.enqueue(q, { kind: "a", payload: { n: 1 }, idempotencyKey: "k" });
    expect(b).toEqual({ jobId: a.jobId, created: false });
    expect((await client.getJob(a.jobId)).payload).toEqual({ n: 1 });
    const batch = await client.enqueueBatch(q, [{ kind: "a" }, { kind: "b" }]);
    expect(batch).toHaveLength(2);
    expect((await client.getQueue(q)).stats.available).toBe(3n);
  });

  it("holds a delayed job back until run_at", async () => {
    const q = queueName();
    await client.createQueue({ name: q });
    await client.enqueue(q, { kind: "later", runAt: new Date(Date.now() + 1500) });
    const early = await client.raw.lease({ queue: q, maxJobs: 1, wait: durationFromMs(0) });
    expect(early.jobs).toHaveLength(0);
    const later = await client.raw.lease({ queue: q, maxJobs: 1, wait: durationFromMs(5000) }, { timeoutMs: 15_000 });
    expect(later.jobs.map((j) => j.kind)).toEqual(["later"]);
  });

  it("cancels an available job; a retried cancel succeeds, a leased job is refused", async () => {
    const q = queueName();
    await client.createQueue({ name: q });
    const { jobId } = await client.enqueue(q, { kind: "a" });
    expect((await client.cancelJob(jobId)).state).toBe("cancelled");
    expect((await client.cancelJob(jobId)).state).toBe("cancelled");
    await client.enqueue(q, { kind: "b" });
    const { jobs } = await client.raw.lease({ queue: q, maxJobs: 1, wait: durationFromMs(1000) }, { timeoutMs: 10_000 });
    const err = await client.cancelJob(jobs[0]!.id).catch((e: unknown) => e);
    expect((err as TasklaneError).code).toBe(Code.FailedPrecondition);
  });

  it("lists a dead job and redrives it", async () => {
    const q = queueName();
    await client.createQueue({ name: q, maxAttempts: 1 });
    const { jobId } = await client.enqueue(q, { kind: "doomed" });
    const [job] = (await client.raw.lease({ queue: q, maxJobs: 1, wait: durationFromMs(1000) }, { timeoutMs: 10_000 })).jobs;
    await client.raw.nack({ id: job!.id, leaseToken: job!.leaseToken, error: "boom" });
    const page = await client.listDeadJobs(q);
    expect(page.jobs.map((j) => [j.id, j.lastError])).toEqual([[jobId, "boom"]]);
    expect(await client.redriveJobs(q, [jobId])).toEqual({ redriven: 1n, skipped: 0n });
    expect((await client.getJob(jobId)).state).toBe("available");
  });

  it("refuses a wrong API key with Unauthenticated", async () => {
    const bad = newClient("wrong-key");
    const err = await bad.getQueue("anything").catch((e: unknown) => e);
    bad.close();
    expect((err as TasklaneError).code).toBe(Code.Unauthenticated);
  });

  it("reports a full queue as QueueFullError", async () => {
    const q = queueName();
    await client.createQueue({ name: q, maxDepth: 1 });
    // The server's depth cache refreshes every 1–5 s, so the limit is soft
    // (design §15): keep enqueueing until it bites.
    let err: unknown;
    for (let i = 0; i < 200 && err === undefined; i++) {
      err = await client.enqueue(q, { kind: "a" }).then(
        () => undefined,
        (e: unknown) => e,
      );
      if (err === undefined) await new Promise((r) => setTimeout(r, 50));
    }
    expect(err).toBeInstanceOf(QueueFullError);
    expect((err as QueueFullError).retryAfterMs).toBeGreaterThan(0);
  });
});
