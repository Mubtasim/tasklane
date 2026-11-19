import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { context, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { beforeAll, describe, expect, it } from "vitest";
import { formatTraceparent, parseTraceparent, truncateUtf8 } from "../../src/tracing.js";
import { Worker } from "../../src/worker.js";
import type { Result } from "../../src/job.js";
import { FakeServer } from "./fake.js";
import { until } from "./helpers.js";
import { fast } from "./fast.js";

// Apps get a context manager from the OpenTelemetry Node SDK; without one,
// the Ack's client span could not find the handle span as its parent.
beforeAll(() => {
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
});

function tracer() {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { exporter, provider };
}

describe("traceparent", () => {
  it("formats and parses W3C traceparent", () => {
    const sc = { traceId: "0af7651916cd43dd8448eb211c80319c", spanId: "b7ad6b7169203331", traceFlags: 1 };
    const s = formatTraceparent(sc);
    expect(s).toBe("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    expect(parseTraceparent(s)).toEqual({ ...sc, isRemote: true });
    expect(parseTraceparent("garbage")).toBeUndefined();
    expect(parseTraceparent("00-00000000000000000000000000000000-b7ad6b7169203331-01")).toBeUndefined();
  });

  it("truncates on a UTF-8 boundary", () => {
    expect(truncateUtf8("héllo", 2)).toBe("h");
    expect(truncateUtf8("abc", 10)).toBe("abc");
  });
});

describe("spans", () => {
  it("joins producer and worker in one trace and skips heartbeats", async () => {
    const { exporter, provider } = tracer();
    const fake = new FakeServer();
    fake.addQueue("q", 300, 3);
    const client = fake.client({ tracerProvider: provider });
    await client.enqueue("q", { kind: "a" });
    const results: Result[] = [];
    const worker = new Worker(client, "q", { handlers: { a: () => new Promise((r) => setTimeout(r, 250)) }, onResult: (r) => results.push(r) }, fast);
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();

    const spans = exporter.getFinishedSpans();
    const enqueue = spans.find((s) => s.name === "tasklane.v1.TasklaneService/Enqueue")!;
    const handle = spans.find((s) => s.name === "handle a")!;
    expect(enqueue.kind).toBe(SpanKind.CLIENT);
    expect(handle.kind).toBe(SpanKind.CONSUMER);
    expect(handle.spanContext().traceId).toBe(enqueue.spanContext().traceId);
    expect(handle.attributes).toMatchObject({ "messaging.system": "tasklane", "messaging.destination.name": "q", "tasklane.kind": "a", "tasklane.attempt": 1 });
    expect(handle.status.code).toBe(SpanStatusCode.OK);
    const ack = spans.find((s) => s.name === "tasklane.v1.TasklaneService/Ack")!;
    expect(ack.spanContext().traceId).toBe(handle.spanContext().traceId);
    expect(spans.some((s) => s.name.endsWith("/Heartbeat"))).toBe(false);
    expect(fake.callsOf("heartbeat").length).toBeGreaterThan(0);
  });

  it("marks a failed run with the error and a dead job", async () => {
    const { exporter, provider } = tracer();
    const fake = new FakeServer();
    fake.addQueue("q", 300, 1);
    fake.add("q", "bad");
    const results: Result[] = [];
    const worker = new Worker(fake.client({ tracerProvider: provider }), "q", {
      handlers: {
        bad: () => {
          throw new Error("x".repeat(5000));
        },
      },
      logger: { info() {}, warn() {}, error() {} },
      onResult: (r) => results.push(r),
    }, fast);
    void worker.run();
    await until(() => results.length === 1);
    await worker.shutdown();
    const handle = exporter.getFinishedSpans().find((s) => s.name === "handle bad")!;
    expect(handle.status.code).toBe(SpanStatusCode.ERROR);
    expect(handle.status.message).toHaveLength(4096);
    expect(handle.attributes["tasklane.dead"]).toBe(true);
  });
});
