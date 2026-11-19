import type { Job as ProtoJob } from "./gen/tasklane/v1/tasklane_pb.js";
import type { Client } from "./client.js";
import { backoffDelay, sleep, type BackoffPolicy } from "./backoff.js";
import { codeOf, isFatal, rpc, TasklaneError, toError } from "./errors.js";
import { durationFromMs } from "./internal/time.js";
import { runJob, type Handler, type Result, type RunContext } from "./job.js";
import { getQueue } from "./producer.js";
import { Slots } from "./slots.js";
import { defaultLogger, type Logger } from "./types.js";

export interface WorkerOptions {
  /** How many jobs run at once (default 1). */
  concurrency?: number;
  /** The lease length; omitted uses the queue's setting. */
  visibilityTimeoutMs?: number;
  /**
   * How long running jobs may take to finish after shutdown() (default
   * 25 000 ms); the Nacks and Acks after it take up to 2 s more. Give the
   * process at least that long to stop: Docker's default is only 10 s.
   */
  shutdownTimeoutMs?: number;
  /** One handler per job kind. A job whose kind has none is nacked. */
  handlers: Record<string, Handler>;
  /**
   * Called once per leased job after the worker is done with it. It may run
   * after run() resolves, for a job whose final call was still in flight. An
   * exception inside it is logged, not thrown.
   */
  onResult?: (result: Result) => void;
  /** Defaults to console. */
  logger?: Logger;
}

/** @internal Timings tests shorten, like Go's package-private fields. */
export interface WorkerInternals {
  leaseWaitMs: number;
  leaseMarginMs: number;
  retry: BackoffPolicy;
  finishTimeoutMs: number;
  lagWarnMs: number;
}

/** @internal */
export const defaultInternals: WorkerInternals = {
  // The server caps waits at 30 s; the margin lets a slow network deliver a
  // response that is on its way.
  leaseWaitMs: 20_000,
  leaseMarginMs: 10_000,
  // Full jitter, ceiling 1 s doubling to a 30 s cap: workers that lost the
  // server at the same moment don't come back in lockstep.
  retry: { baseMs: 1000, capMs: 30_000 },
  finishTimeoutMs: 2000,
  lagWarnMs: 5000,
};

const defaultShutdownTimeoutMs = 25_000;

/**
 * Worker leases jobs from one queue and runs the handler registered for each
 * job's kind. There is one lease loop, so one long-poll, per worker whatever
 * its concurrency: each waiting lease costs the server a query per second.
 */
export class Worker {
  readonly #client: Client;
  readonly #queue: string;
  readonly #options: WorkerOptions;
  readonly #concurrency: number;
  // eslint-disable-next-line no-unused-private-class-members -- Task 5 uses it
  readonly #shutdownTimeoutMs: number;
  readonly #internals: WorkerInternals;
  readonly #log: Logger;
  readonly #stop = new AbortController();
  #done: Promise<void> | undefined;

  constructor(client: Client, queue: string, options: WorkerOptions, internals: Partial<WorkerInternals> = {}) {
    const concurrency = options.concurrency ?? 1;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new RangeError("tasklane: concurrency must be a positive integer");
    }
    this.#client = client;
    this.#queue = queue;
    this.#options = options;
    this.#concurrency = concurrency;
    this.#shutdownTimeoutMs = options.shutdownTimeoutMs ?? defaultShutdownTimeoutMs;
    this.#internals = { ...defaultInternals, ...internals };
    this.#log = options.logger ?? defaultLogger;
  }

  /**
   * run leases and runs jobs until shutdown(), then shuts down gracefully and
   * resolves. It rejects only when retrying can't help: the queue doesn't
   * exist, the API key is refused or a request is invalid. While the server
   * is unreachable it keeps retrying with backoff.
   */
  run(): Promise<void> {
    if (this.#done) return Promise.reject(new Error("tasklane: Worker.run called more than once"));
    this.#done = this.#run();
    return this.#done;
  }

  /** shutdown stops leasing and resolves when run() has finished. Safe to call twice. */
  shutdown(): Promise<void> {
    this.#stop.abort();
    return (this.#done ?? Promise.resolve()).catch(() => undefined);
  }

  async #run(): Promise<void> {
    const vtMs = await this.#visibilityTimeout();
    if (vtMs === undefined) return;
    const ctx: RunContext = {
      client: this.#client,
      queue: this.#queue,
      vtMs,
      handlers: this.#options.handlers,
      onResult: this.#options.onResult,
      log: this.#log,
      internals: this.#internals,
      slots: new Slots(this.#concurrency),
      active: new Set(),
      jobs: new Set(),
    };
    const fatal = await this.#leaseLoop(ctx);
    await this.#finishRuns(ctx);
    if (fatal) throw fatal;
  }

  /** #finishRuns waits for running jobs (Task 5 replaces it with drain). */
  async #finishRuns(ctx: RunContext): Promise<void> {
    await Promise.allSettled([...ctx.jobs]);
  }

  /**
   * #visibilityTimeout checks that the queue exists and returns the lease
   * length. It retries while the server is unreachable (it may still be
   * starting) and returns undefined if shutdown() comes first.
   */
  async #visibilityTimeout(): Promise<number | undefined> {
    const signal = this.#stop.signal;
    for (let attempt = 0; ; attempt++) {
      try {
        // Bounded like a lease: leaseMarginMs (10 s) is the existing "slow network" allowance.
        const info = await getQueue(this.#client.raw, this.#queue, { signal, timeoutMs: this.#internals.leaseMarginMs });
        const own = this.#options.visibilityTimeoutMs;
        return own !== undefined && own > 0 ? own : info.config.visibilityTimeoutMs;
      } catch (e) {
        if (signal.aborted) return undefined;
        const code = codeOf(e);
        if (isFatal(code)) {
          throw new TasklaneError(`tasklane: queue "${this.#queue}": ${toError(e).message}`, code!, { cause: e });
        }
        this.#log.warn("tasklane: server unreachable; retrying", { err: toError(e).message });
        if (!(await sleep(backoffDelay(this.#internals.retry, attempt), signal))) return undefined;
      }
    }
  }

  /** #leaseLoop leases jobs into free slots until shutdown(); it returns a fatal error, if any. */
  async #leaseLoop(ctx: RunContext): Promise<TasklaneError | undefined> {
    const signal = this.#stop.signal;
    for (let attempt = 0; ; ) {
      // Wait until a slot is free, then take every other free slot.
      if (!(await ctx.slots.acquire(signal))) return undefined;
      let k = 1;
      while (k < ctx.slots.capacity && ctx.slots.tryAcquire()) k++;

      let jobs: ProtoJob[] = [];
      let err: unknown;
      try {
        jobs = await this.#lease(k, ctx.vtMs, signal);
      } catch (e) {
        err = e;
      }
      const leasedAt = performance.now();
      // The server must not return more than k jobs. If it does, run only
      // k: the extras' leases expire and the reaper retries them.
      if (jobs.length > k) {
        const extras = jobs.slice(k).map((j) => String(j.id));
        this.#log.warn("tasklane: server returned more jobs than requested; ignoring the extras", { requested: k, jobIds: extras });
        jobs = jobs.slice(0, k);
      }
      // Give back the slots this lease didn't fill, before looking at the
      // error. Without this, one empty long-poll would keep every slot and
      // the worker would stop taking jobs without any error.
      for (let i = jobs.length; i < k; i++) ctx.slots.release();

      if (err !== undefined) {
        if (signal.aborted) return undefined;
        const code = codeOf(err);
        if (isFatal(code)) {
          return new TasklaneError(`tasklane: lease from "${this.#queue}": ${toError(err).message}`, code!, { cause: err });
        }
        this.#log.warn("tasklane: lease failed; backing off", { err: toError(err).message });
        if (!(await sleep(backoffDelay(this.#internals.retry, attempt), signal))) return undefined;
        attempt++;
        continue;
      }
      attempt = 0;
      for (const pj of jobs) {
        const p = runJob(ctx, pj, leasedAt).catch((e: unknown) => {
          // runJob must not reject; if it does, log without letting a bad logger escape.
          try {
            ctx.log.error("tasklane: job run failed", { jobId: String(pj.id), err: toError(e).message });
          } catch {
            // nothing more to do
          }
        });
        ctx.jobs.add(p);
        void p.finally(() => ctx.jobs.delete(p));
      }
    }
  }

  async #lease(n: number, vtMs: number, signal: AbortSignal): Promise<ProtoJob[]> {
    const { leaseWaitMs, leaseMarginMs } = this.#internals;
    return rpc(async () => {
      const res = await this.#client.raw.lease(
        { queue: this.#queue, maxJobs: n, wait: durationFromMs(leaseWaitMs), visibilityTimeout: durationFromMs(vtMs) },
        { timeoutMs: leaseWaitMs + leaseMarginMs, signal },
      );
      return res.jobs;
    });
  }
}
