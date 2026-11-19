import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { expect, it } from "vitest";
import { Worker, createClient, type Result } from "../../src/index.js";
import { addr, apiKey, queueName } from "./env.js";

it("puts the producer, the server's job and the worker in one trace", async () => {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  const client = createClient(addr, { apiKey, tracerProvider: provider });
  const q = queueName("ts-trace");
  await client.createQueue({ name: q });
  const { jobId } = await client.enqueue(q, { kind: "t" });
  const stored = (await client.raw.getJob({ id: jobId })).job!.traceContext;
  expect(stored).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);

  const results: Result[] = [];
  const worker = new Worker(client, q, { handlers: { t: () => undefined }, onResult: (r) => results.push(r) });
  void worker.run();
  while (results.length === 0) await new Promise((r) => setTimeout(r, 50));
  await worker.shutdown();
  client.close();

  const spans = exporter.getFinishedSpans();
  const enqueue = spans.find((s) => s.name.endsWith("/Enqueue"))!;
  const handle = spans.find((s) => s.name === "handle t")!;
  expect(stored.split("-")[1]).toBe(enqueue.spanContext().traceId);
  expect(handle.spanContext().traceId).toBe(enqueue.spanContext().traceId);
});
