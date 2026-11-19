import type { Interceptor } from "@connectrpc/connect";
import type * as Otel from "@opentelemetry/api";
import type { Job as ProtoJob } from "./gen/tasklane/v1/tasklane_pb.js";
import type { JobTracing, LeasedJob, Outcome } from "./job.js";
import { ShutdownError } from "./errors.js";
import type { TracerProviderLike } from "./types.js";

// @opentelemetry/api is an optional peer dependency: without it every hook
// here is a no-op.
let api: typeof Otel | undefined;
try {
  api = await import("@opentelemetry/api");
} catch {
  api = undefined;
}

const tracerName = "@mubtasim91/tasklane";
/** RecordException already exports the full error; the status gets at most this much. */
const maxStatusBytes = 4096;

export function getTracer(tp?: TracerProviderLike): Otel.Tracer | undefined {
  if (!api) return undefined;
  const provider = (tp as Otel.TracerProvider | undefined) ?? api.trace.getTracerProvider();
  return provider.getTracer(tracerName);
}

export function formatTraceparent(sc: { traceId: string; spanId: string; traceFlags: number }): string {
  return `00-${sc.traceId}-${sc.spanId}-${(sc.traceFlags & 0xff).toString(16).padStart(2, "0")}`;
}

const traceparentRe = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

export function parseTraceparent(s: string): { traceId: string; spanId: string; traceFlags: number; isRemote: true } | undefined {
  const m = traceparentRe.exec(s);
  if (!m) return undefined;
  const [, traceId, spanId, flags] = m as unknown as [string, string, string, string];
  if (/^0+$/.test(traceId) || /^0+$/.test(spanId)) return undefined;
  return { traceId, spanId, traceFlags: parseInt(flags, 16), isRemote: true };
}

export function truncateUtf8(s: string, maxBytes: number): string {
  const b = Buffer.from(s, "utf8");
  if (b.length <= maxBytes) return s;
  let n = maxBytes;
  while (n > 0 && (b[n]! & 0xc0) === 0x80) n--; // don't split a character
  return b.subarray(0, n).toString("utf8");
}

/**
 * tracingInterceptor gives every call but Heartbeat a client span and sends
 * its W3C traceparent, so the server joins the job to the caller's trace
 * (design §18, choice 7).
 */
export function tracingInterceptor(tracer: Otel.Tracer | undefined): Interceptor {
  return (next) => async (req) => {
    if (!tracer || !api || req.method.name === "Heartbeat") return next(req);
    const otel = api;
    const span = tracer.startSpan(
      `${req.service.typeName}/${req.method.name}`,
      { kind: otel.SpanKind.CLIENT, attributes: { "rpc.system": "grpc", "rpc.service": req.service.typeName, "rpc.method": req.method.name } },
      otel.context.active(),
    );
    const sc = span.spanContext();
    if (otel.trace.isSpanContextValid(sc)) req.header.set("traceparent", formatTraceparent(sc));
    try {
      return await next(req);
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      span.recordException(err);
      span.setStatus({ code: otel.SpanStatusCode.ERROR, message: truncateUtf8(err.message, maxStatusBytes) });
      throw e;
    } finally {
      span.end();
    }
  };
}

/** jobTracing builds the worker's "handle <kind>" consumer spans, or undefined without OpenTelemetry. */
export function jobTracing(tracer: Otel.Tracer | undefined, queue: string): JobTracing | undefined {
  if (!tracer || !api) return undefined;
  const otel = api;
  return {
    start(pj: ProtoJob): unknown {
      const parent = parseTraceparent(pj.traceContext);
      const ctx = parent ? otel.trace.setSpanContext(otel.ROOT_CONTEXT, parent) : otel.ROOT_CONTEXT;
      return tracer.startSpan(
        `handle ${pj.kind}`,
        {
          kind: otel.SpanKind.CONSUMER,
          attributes: {
            "messaging.system": "tasklane",
            "messaging.destination.name": queue,
            "messaging.operation.type": "process",
            "messaging.message.id": pj.id.toString(),
            "tasklane.kind": pj.kind,
            "tasklane.attempt": pj.attempts,
            "tasklane.priority": pj.priority,
          },
        },
        ctx,
      );
    },
    end(span: unknown, job: LeasedJob, outcome: Outcome, err?: Error): void {
      const s = span as Otel.Span;
      switch (outcome) {
        case "acked":
          s.setStatus({ code: otel.SpanStatusCode.OK });
          break;
        case "nacked":
          if (err instanceof ShutdownError) {
            s.setStatus({ code: otel.SpanStatusCode.ERROR, message: "shutdown" });
          } else if (err) {
            s.recordException(err);
            s.setStatus({ code: otel.SpanStatusCode.ERROR, message: truncateUtf8(err.message, maxStatusBytes) });
          }
          // A Nack on the last attempt always buries the job.
          if (job.maxAttempts > 0 && job.attempt >= job.maxAttempts) s.setAttribute("tasklane.dead", true);
          break;
        case "lease_lost":
          s.setStatus({ code: otel.SpanStatusCode.ERROR, message: "lease lost" });
          break;
        case "gave_up":
          s.setStatus({ code: otel.SpanStatusCode.ERROR, message: "gave up" });
          break;
      }
      s.end();
    },
    around<T>(span: unknown, fn: () => Promise<T>): Promise<T> {
      return otel.context.with(otel.trace.setSpan(otel.context.active(), span as Otel.Span), fn);
    },
  };
}
