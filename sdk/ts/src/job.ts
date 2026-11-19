import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import type { CallOptions } from "@connectrpc/connect";
import type { Job as ProtoJob } from "./gen/tasklane/v1/tasklane_pb.js";
import type { Client } from "./client.js";
import { backoffDelay, sleep } from "./backoff.js";
import { Code, codeOf, isFatal, isLeaseLost, toError } from "./errors.js";
import { heartbeat } from "./heartbeat.js";
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

/** @internal */
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

/** @internal callHandler runs the handler for the job's kind; a missing handler or a throw becomes the error. */
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

/** RetryAtError asks for the retry at a chosen time, e.g. from a downstream Retry-After. */
export class RetryAtError extends Error {
  readonly at: Date;

  constructor(err: Error, at: Date) {
    super(err.message, { cause: err });
    this.name = "RetryAtError";
    this.at = at;
  }
}

/** retryAt wraps err so the job's Nack carries retry_at = at instead of the server's backoff. */
export function retryAt(err: unknown, at: Date): RetryAtError {
  if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
    throw new TypeError("tasklane: retryAt needs a valid Date");
  }
  return new RetryAtError(toError(err), at);
}

/** @internal A job this worker is running. */
export interface ActiveJob {
  job: LeasedJob;
  /** Set before the job is shared; the handler starts right after. */
  start: Date;
  /** Aborts the handler's signal. */
  abort: AbortController;
  hbStop: AbortController;
  /** Settles once the heartbeat loop has exited. */
  hbDone: Promise<void>;
  /** The server reported the lease gone. */
  lost: boolean;
  /** The final Ack or Nack has been claimed. */
  finished: boolean;
  /** Settles when the job's own path is done with the final call it claimed. */
  sent: Promise<void>;
  markSent: () => void;
  /**
   * When the lease expires by this process's clock (performance.now()).
   * Counted from when the Lease response arrived; heartbeats move it from
   * the moment each one was sent.
   */
  deadline: number;
  /** The run's "handle" span, or undefined without tracing. */
  span: unknown;
}

/**
 * @internal claim reports whether the caller may send the job's final Ack or Nack: only
 * the first caller may, so the job's own path and a shutdown never both
 * answer for it.
 */
export function claim(a: ActiveJob): boolean {
  if (a.finished) return false;
  a.finished = true;
  return true;
}

/** @internal report ends the run's span and calls onResult once; a throwing onResult is logged. */
export function report(ctx: RunContext, a: ActiveJob, outcome: Outcome, err?: Error): void {
  try {
    ctx.tracing?.end(a.span, a.job, outcome, err);
  } catch (e) {
    try {
      ctx.log.error("tasklane: tracing end threw", { jobId: String(a.job.id), err: toError(e).message });
    } catch {
      // a throwing logger must not skip onResult
    }
  }
  if (!ctx.onResult) return;
  try {
    ctx.onResult({ job: a.job, outcome, start: a.start, end: new Date(), error: err });
  } catch (e) {
    ctx.log.error("tasklane: onResult threw", { jobId: String(a.job.id), err: toError(e).message });
  }
}

/** @internal runJob runs one leased job, sends its Ack or Nack and frees its slot. leasedAt is performance.now() when the Lease response arrived. */
export async function runJob(ctx: RunContext, pj: ProtoJob, leasedAt: number): Promise<void> {
  let a: ActiveJob | undefined;
  try {
    let markSent: () => void = () => {};
    const sent = new Promise<void>((resolve) => (markSent = resolve));
    a = {
      job: leasedJobFromProto(pj),
      start: new Date(),
      abort: new AbortController(),
      hbStop: new AbortController(),
      hbDone: Promise.resolve(),
      lost: false,
      finished: false,
      sent,
      markSent,
      deadline: leasedAt + ctx.vtMs,
      span: ctx.tracing?.start(pj),
    };
    ctx.active.add(a);
    const hbJob = a;
    // Never rejects: a throwing logger inside the loop must not become an
    // unhandled rejection while the handler runs.
    a.hbDone = heartbeat(ctx, a).catch((e: unknown) => {
      try {
        ctx.log.error("tasklane: heartbeat loop failed", { jobId: String(hbJob.job.id), err: toError(e).message });
      } catch {
        // nothing more to do
      }
    });
    const hj = a;
    // Run inside the span's context, so spans and enqueues made by the
    // handler join the job's trace (as in Go).
    const err = await (ctx.tracing
      ? ctx.tracing.around(hj.span, () => callHandler(ctx, hj.job, hj.abort.signal))
      : callHandler(ctx, a.job, a.abort.signal));
    // Claim the final call as soon as the handler returns, so a shutdown
    // that reaches its deadline now can't nack a job that just succeeded.
    // If the shutdown claimed it first, the shutdown answers for the job.
    if (!claim(a)) return;
    try {
      // Stop heartbeating before the final call, so no heartbeat races the
      // Ack or Nack. A lost lease can only be reported before the heartbeat
      // exits, so check it after the join.
      a.hbStop.abort();
      await a.hbDone;
      if (a.lost) {
        report(ctx, a, "lease_lost", err);
        return;
      }
      report(ctx, a, await finish(ctx, a, err), err);
    } finally {
      a.markSent();
    }
  } finally {
    if (a) {
      a.hbStop.abort();
      ctx.active.delete(a);
    }
    ctx.slots.release();
  }
}

/** findRetryAt walks err's cause chain (like Go's errors.As), to a bounded depth. */
function findRetryAt(err: unknown): RetryAtError | undefined {
  for (let i = 0, e = err; i < 10 && e instanceof Error; i++, e = e.cause) {
    if (e instanceof RetryAtError) return e;
  }
  return undefined;
}

/** finish acks the job when err is undefined and nacks it with err's message otherwise. */
async function finish(ctx: RunContext, a: ActiveJob, err: Error | undefined): Promise<Outcome> {
  const { id, leaseToken } = a.job;
  if (err === undefined) {
    return send(ctx, a, "ack", "acked", (opts) => ctx.client.raw.ack({ id, leaseToken }, opts));
  }
  ctx.log.warn("tasklane: job failed", { jobId: String(id), kind: a.job.kind, attempt: a.job.attempt, err: err.message });
  const wanted = findRetryAt(err);
  const retry = wanted ? timestampFromDate(wanted.at) : undefined;
  return send(ctx, a, "nack", "nacked", (opts) => ctx.client.raw.nack({ id, leaseToken, error: err.message, retryAt: retry }, opts));
}

/**
 * send makes the final Ack or Nack, retrying with backoff while the server is
 * unreachable: a retried Ack is idempotent, and a repeated Nack gets a
 * harmless FAILED_PRECONDITION. It gives up once the lease would have
 * expired, because the token is useless then and the reaper retries the job.
 */
async function send(
  ctx: RunContext,
  a: ActiveJob,
  what: "ack" | "nack",
  ok: Outcome,
  call: (opts: CallOptions) => Promise<unknown>,
): Promise<Outcome> {
  const traced = (opts: CallOptions) => (ctx.tracing ? ctx.tracing.around(a.span, () => call(opts)) : call(opts));
  for (let attempt = 0; ; attempt++) {
    const deadline = a.deadline;
    try {
      // Connect requires an integer timeout.
      const timeoutMs = Math.ceil(Math.max(deadline - performance.now(), ctx.internals.finishTimeoutMs));
      await traced({ timeoutMs });
      return ok;
    } catch (e) {
      const code = codeOf(e);
      const fields = { jobId: String(a.job.id), err: toError(e).message };
      if (isLeaseLost(code)) {
        ctx.log.info(`tasklane: ${what} rejected: the lease is no longer ours`, fields);
        return "lease_lost";
      }
      if (ctx.client.closed || code === Code.Canceled) {
        ctx.log.warn(`tasklane: ${what} cancelled: the client is closed (the job will be retried)`, fields);
        return "gave_up";
      }
      if (isFatal(code)) {
        ctx.log.error(`tasklane: ${what} failed`, fields);
        return "gave_up";
      }
      const delay = backoffDelay(ctx.internals.retry, attempt);
      if (performance.now() + delay > deadline) {
        ctx.log.warn(`tasklane: ${what} failed; giving up because the lease expires (the job will be retried)`, fields);
        return "gave_up";
      }
      await sleep(delay);
    }
  }
}
