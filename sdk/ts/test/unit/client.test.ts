import { describe, expect, it } from "vitest";
import { Code, QueueFullError, TasklaneError } from "../../src/errors.js";
import { Priority } from "../../src/producer.js";
import { JobState as ProtoJobState } from "../../src/gen/tasklane/v1/tasklane_pb.js";
import { FakeServer } from "./fake.js";

function setup() {
  const fake = new FakeServer();
  fake.addQueue("q", 300, 3);
  return { fake, client: fake.client({ apiKey: "k1" }) };
}

describe("producer", () => {
  it("sends the API key as a bearer token", async () => {
    const { fake, client } = setup();
    await client.enqueue("q", { kind: "a" });
    expect(fake.callsOf("enqueue")[0]!.headers.get("authorization")).toBe("Bearer k1");
  });

  it("returns a bigint id, and the same id for a repeated idempotency key", async () => {
    const { client } = setup();
    const first = await client.enqueue("q", { kind: "a", idempotencyKey: "k" });
    const again = await client.enqueue("q", { kind: "a", idempotencyKey: "k" });
    expect(typeof first.jobId).toBe("bigint");
    expect(first.created).toBe(true);
    expect(again).toEqual({ jobId: first.jobId, created: false });
  });

  it("maps every job field", async () => {
    const { fake, client } = setup();
    const runAt = new Date(Date.now() + 60_000);
    await client.enqueue("q", { kind: "a", payload: { n: 1, at: new Date(0) }, priority: Priority.High, runAt, maxAttempts: 7 });
    const spec = fake.callsOf("enqueue")[0]!.req.job;
    expect(spec.kind).toBe("a");
    expect(spec.payload).toEqual({ n: 1, at: "1970-01-01T00:00:00.000Z" }); // JSON-normalized
    expect(spec.priority).toBe(2);
    expect(spec.maxAttempts).toBe(7);
    expect(Number(spec.runAt.seconds)).toBe(Math.floor(runAt.getTime() / 1000));
  });

  it("refuses a payload that is not a JSON object before sending anything", async () => {
    const { fake, client } = setup();
    await expect(client.enqueue("q", { kind: "a", payload: [1, 2] as unknown as object })).rejects.toThrow(TypeError);
    await expect(client.enqueue("q", { kind: "a", payload: "x" as unknown as object })).rejects.toThrow(TypeError);
    expect(fake.callsOf("enqueue")).toHaveLength(0);
  });

  it("enqueues a batch and keeps the order", async () => {
    const { client } = setup();
    const res = await client.enqueueBatch("q", [{ kind: "a" }, { kind: "b", idempotencyKey: "x" }, { kind: "b", idempotencyKey: "x" }]);
    expect(res.map((r) => r.created)).toEqual([true, true, false]);
    expect(res[1]!.jobId).toBe(res[2]!.jobId);
  });

  it("turns a full queue into QueueFullError with the retry hint, never retried", async () => {
    const fake = new FakeServer();
    fake.addQueue("full", 300, 3, 1n);
    const client = fake.client();
    await client.enqueue("full", { kind: "a" });
    const err = await client.enqueue("full", { kind: "a" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(QueueFullError);
    expect(err).toBeInstanceOf(TasklaneError);
    expect((err as QueueFullError).code).toBe(Code.ResourceExhausted);
    expect((err as QueueFullError).retryAfterMs).toBe(1000);
    expect(fake.callsOf("enqueue")).toHaveLength(2);
  });

  it("keeps the gRPC code of other errors", async () => {
    const { client } = setup();
    const err = await client.enqueue("missing", { kind: "a" }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(TasklaneError);
    expect(err).not.toBeInstanceOf(QueueFullError);
    expect((err as TasklaneError).code).toBe(Code.NotFound);
  });

  it("creates and reads queues in milliseconds", async () => {
    const { fake, client } = setup();
    const created = await client.createQueue({ name: "new", visibilityTimeoutMs: 15_000, maxAttempts: 5, maxDepth: 100 });
    expect(created).toEqual({ name: "new", maxDepth: 100n, visibilityTimeoutMs: 15_000, maxAttempts: 5, paused: false });
    fake.add("new", "a");
    const info = await client.getQueue("new");
    expect(info.config.visibilityTimeoutMs).toBe(15_000);
    expect(info.stats).toEqual({ available: 1n, leased: 0n, dead: 0n, recentlyCompleted: 0n, recentWindowMs: 10_000 });
  });
});

describe("admin", () => {
  it("gets and cancels a job; a retried cancel succeeds, a leased job is FailedPrecondition", async () => {
    const { fake, client } = setup();
    const { jobId } = await client.enqueue("q", { kind: "a", payload: { x: 1 } });
    const info = await client.getJob(jobId);
    expect(info).toMatchObject({ id: jobId, queue: "q", kind: "a", payload: { x: 1 }, state: "available", attempts: 0 });
    expect((await client.cancelJob(jobId)).state).toBe("cancelled");
    expect((await client.cancelJob(jobId)).state).toBe("cancelled");
    const leased = fake.add("q", "a", {}, { state: ProtoJobState.LEASED });
    const err = await client.cancelJob(leased).catch((e: unknown) => e);
    expect((err as TasklaneError).code).toBe(Code.FailedPrecondition);
  });

  it("lists dead jobs page by page and redrives them", async () => {
    const { fake, client } = setup();
    const ids = [1, 2, 3].map(() => fake.add("q", "a", {}, { state: ProtoJobState.DEAD, attempts: 3 }));
    const page1 = await client.listDeadJobs("q", { pageSize: 2 });
    expect(page1.jobs.map((j) => j.id)).toEqual(ids.slice(0, 2));
    const page2 = await client.listDeadJobs("q", { pageSize: 2, pageToken: page1.nextPageToken });
    expect(page2.jobs.map((j) => j.id)).toEqual(ids.slice(2));
    expect(page2.nextPageToken).toBe("");
    expect(await client.redriveJobs("q", [ids[0]!, 999n])).toEqual({ redriven: 1n, skipped: 1n });
    expect(await client.redriveAll("q")).toBe(2n);
    expect((await client.getJob(ids[1]!)).state).toBe("available");
  });
});
