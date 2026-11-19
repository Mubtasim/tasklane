import { afterAll, expect, it } from "vitest";
import { Worker, type Result } from "../../src/index.js";
import { newClient, queueName } from "./env.js";

const client = newClient();
afterAll(() => client.close());

it("runs normal, flaky and poison jobs end to end", async () => {
  const q = queueName("ts-worker");
  await client.createQueue({ name: q, visibilityTimeoutMs: 3000, maxAttempts: 2 });
  const normal = await Promise.all(Array.from({ length: 20 }, () => client.enqueue(q, { kind: "normal" })));
  const flaky = await client.enqueue(q, { kind: "flaky" });
  const poison = await client.enqueue(q, { kind: "poison" });

  const results: Result[] = [];
  const worker = new Worker(client, q, {
    concurrency: 5,
    logger: { info() {}, warn() {}, error: console.error },
    onResult: (r) => results.push(r),
    handlers: {
      normal: () => new Promise((r) => setTimeout(r, 20)),
      flaky: (job) => {
        if (job.attempt === 1) throw new Error("first attempt fails");
      },
      poison: () => {
        throw new Error("always fails");
      },
    },
  });
  const done = worker.run();
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const s = (await client.getQueue(q)).stats;
    if (s.available === 0n && s.leased === 0n && s.dead === 1n) break;
    await new Promise((r) => setTimeout(r, 200));
  }
  await worker.shutdown();
  await done;

  for (const { jobId } of normal) expect((await client.getJob(jobId)).state).toBe("completed");
  expect((await client.getJob(flaky.jobId)).state).toBe("completed");
  const dead = await client.getJob(poison.jobId);
  expect(dead).toMatchObject({ state: "dead", attempts: 2, lastError: "always fails" });
  expect(results.filter((r) => r.outcome === "acked")).toHaveLength(21);
  expect(results.filter((r) => r.outcome === "nacked")).toHaveLength(3); // flaky once, poison twice
});
