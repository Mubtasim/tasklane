import type { Job as ProtoJob } from "./gen/tasklane/v1/tasklane_pb.js";
import type { Client } from "./client.js";
import { toError } from "./errors.js";
import { optionalDate } from "./internal/time.js";
import type { Priority } from "./producer.js";
import type { Slots } from "./slots.js";
import type { Logger } from "./types.js";
import type { WorkerInternals } from "./worker.js";

/** How one run of a job ended, as far as this worker knows (Go's Outcome). */
export type Outcome = "acked" | "nacked" | "lease_lost" | "gave_up";

export interface LeasedJob<P extends object = Record<string, unknown>> {
  readonly id: bigint;
  readonly kind: string;
  readonly payload: P;
  /** 1 on the first run. */
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly priority: Priority;
  readonly enqueuedAt?: Date;
  readonly leaseToken: string;
  readonly traceContext: string;
}

/**
 * Handler runs one job. Returning acks it; throwing nacks it with the error's
 * message (throw retryAt(err, date) to choose the retry time). signal aborts
 * when the lease is lost or the shutdown deadline passes: return soon after.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Handler<P extends object = any> = (job: LeasedJob<P>, signal: AbortSignal) => unknown;

/** One run of a job, reported once through onResult. */
export interface Result {
  job: LeasedJob;
  outcome: Outcome;
  /** The job was picked up, just before the handler started. */
  start: Date;
  /** The outcome was known. */
  end: Date;
  /** The handler's error, or the shutdown reason for a job nacked at shutdown. */
  error?: Error;
}

/** @internal Tracing hooks; tracing.ts implements them (design §21.4). */
export interface JobTracing {
  start(pj: ProtoJob): unknown;
  end(span: unknown, job: LeasedJob, outcome: Outcome, err?: Error): void;
  around<T>(span: unknown, fn: () => Promise<T>): Promise<T>;
}

/** @internal A job this worker is running. */
export interface ActiveJob {
  job: LeasedJob;
  start: Date;
  abort: AbortController;
}

/** @internal The state of one run() call, shared by the worker's parts. */
export interface RunContext {
  client: Client;
  queue: string;
  vtMs: number;
  handlers: Record<string, Handler>;
  onResult?: (r: Result) => void;
  log: Logger;
  internals: WorkerInternals;
  slots: Slots;
  active: Set<ActiveJob>;
  jobs: Set<Promise<void>>;
  tracing?: JobTracing;
}

export function leasedJobFromProto(pj: ProtoJob): LeasedJob {
  return {
    id: pj.id,
    kind: pj.kind,
    payload: (pj.payload ?? {}) as Record<string, unknown>,
    attempt: pj.attempts,
    maxAttempts: pj.maxAttempts,
    priority: pj.priority as number as Priority,
    enqueuedAt: optionalDate(pj.createdAt),
    leaseToken: pj.leaseToken,
    traceContext: pj.traceContext,
  };
}

/** callHandler runs the handler for the job's kind; a missing handler or a throw becomes the error. */
export async function callHandler(ctx: RunContext, job: LeasedJob, signal: AbortSignal): Promise<Error | undefined> {
  const handler = ctx.handlers[job.kind];
  if (!handler) return new Error(`no handler for kind "${job.kind}"`);
  try {
    await handler(job, signal);
    return undefined;
  } catch (e) {
    return toError(e);
  }
}

/** runJob (first version): run the handler, then one Ack or Nack. */
// eslint-disable-next-line @typescript-eslint/no-unused-vars -- Task 4 uses leasedAt
export async function runJob(ctx: RunContext, pj: ProtoJob, _leasedAt: number): Promise<void> {
  let a: ActiveJob | undefined;
  try {
    const job = leasedJobFromProto(pj);
    a = { job, start: new Date(), abort: new AbortController() };
    ctx.active.add(a);
    const err = await callHandler(ctx, job, a.abort.signal);
    try {
      if (err === undefined) await ctx.client.raw.ack({ id: job.id, leaseToken: job.leaseToken });
      else await ctx.client.raw.nack({ id: job.id, leaseToken: job.leaseToken, error: err.message });
    } catch (e) {
      ctx.log.warn("tasklane: final call failed", { jobId: String(job.id), err: toError(e).message });
    }
  } finally {
    if (a) ctx.active.delete(a);
    ctx.slots.release();
  }
}
