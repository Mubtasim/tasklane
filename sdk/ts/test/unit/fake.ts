// An in-memory Tasklane server for unit tests, served through Connect's
// router transport. It implements the API's rules that the SDK depends on
// (design §5, §6): lease tokens, idempotent Ack, FAILED_PRECONDITION for a
// stale token, Nack with retry_at or dead after the last attempt, queue-full
// with RetryInfo. Tests inject failures per method with fail().
import { clone, create } from "@bufbuild/protobuf";
import { timestampFromMs, timestampMs, timestampNow } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, createRouterTransport, type HandlerContext } from "@connectrpc/connect";
import { RetryInfoSchema } from "../../src/gen/google/rpc/error_details_pb.js";
import { JobSchema, JobState, TasklaneService, type Job, type JobSpec } from "../../src/gen/tasklane/v1/tasklane_pb.js";
import { Client, type ClientOptions } from "../../src/client.js";
import { durationFromMs, durationToMs } from "../../src/internal/time.js";

export type Method =
  | "createQueue" | "getQueue" | "enqueue" | "enqueueBatch" | "lease" | "ack" | "nack"
  | "heartbeat" | "getJob" | "cancelJob" | "listDeadJobs" | "redriveJobs";

export interface Call {
  method: Method;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  req: any;
  headers: Headers;
}

interface FakeQueue {
  vtMs: number;
  maxAttempts: number;
  maxDepth: bigint;
}

export class FakeServer {
  readonly queues = new Map<string, FakeQueue>();
  readonly jobs = new Map<bigint, Job>();
  readonly calls: Call[] = [];
  /** Errors thrown, in order, by the next calls of a method. */
  readonly failures = new Map<Method, ConnectError[]>();
  /** The next Lease returns this many jobs more than it was asked for. */
  extraOnNextLease = 0;
  /** GetQueue never answers (until the caller aborts). */
  hangGetQueue = false;
  /** Called per Heartbeat; true makes that call hang until the caller aborts. */
  hangHeartbeat?: () => boolean;
  /** Each Ack waits this long before it is applied. */
  ackDelayMs = 0;
  #nextId = 1n;
  #nextToken = 0;
  #waiters: Array<() => void> = [];

  fail(method: Method, ...errs: ConnectError[]): void {
    this.failures.set(method, [...(this.failures.get(method) ?? []), ...errs]);
  }

  callsOf(method: Method): Call[] {
    return this.calls.filter((c) => c.method === method);
  }

  addQueue(name: string, vtMs = 300, maxAttempts = 3, maxDepth = 0n): void {
    this.queues.set(name, { vtMs, maxAttempts, maxDepth });
  }

  /** Adds an available job and wakes waiting leases. */
  add(queue: string, kind: string, payload: Record<string, unknown> = {}, extra: Omit<Partial<Job>, "$typeName" | "$unknown"> = {}): bigint {
    const id = this.#nextId++;
    this.jobs.set(
      id,
      create(JobSchema, {
        id,
        queue,
        kind,
        payload: payload as Job["payload"],
        state: JobState.AVAILABLE,
        maxAttempts: this.queues.get(queue)?.maxAttempts ?? 3,
        createdAt: timestampNow(),
        ...extra,
      }),
    );
    this.#wakeAll();
    return id;
  }

  /** Expires leases like the reaper: back to available, or dead after the last attempt. */
  reap(): void {
    const now = Date.now();
    for (const j of this.jobs.values()) {
      if (j.state === JobState.LEASED && j.leaseExpiresAt && timestampMs(j.leaseExpiresAt) <= now) {
        j.state = j.attempts >= j.maxAttempts ? JobState.DEAD : JobState.AVAILABLE;
        j.lastError = "lease expired";
      }
    }
    this.#wakeAll();
  }

  client(options: ClientOptions = {}): Client {
    return Client.make(
      (interceptors) =>
        createRouterTransport((router) => router.service(TasklaneService, this.#impl()), {
          transport: { interceptors },
        }),
      options,
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #record(method: Method, req: any, ctx: HandlerContext): void {
    this.calls.push({ method, req, headers: ctx.requestHeader });
    const err = this.failures.get(method)?.shift();
    if (err) throw err;
  }

  #wakeAll(): void {
    const ws = this.#waiters;
    this.#waiters = [];
    for (const w of ws) w();
  }

  #queue(name: string): FakeQueue {
    const q = this.queues.get(name);
    if (!q) throw new ConnectError(`queue "${name}" not found`, Code.NotFound);
    return q;
  }

  #job(id: bigint): Job {
    const j = this.jobs.get(id);
    if (!j) throw new ConnectError(`job ${id} not found`, Code.NotFound);
    return j;
  }

  #holds(j: Job, token: string): void {
    if (j.state !== JobState.LEASED || j.leaseToken !== token) {
      throw new ConnectError("lease token is stale", Code.FailedPrecondition);
    }
  }

  #enqueueOne(queue: string, spec: JobSpec, traceparent: string): { jobId: bigint; created: boolean } {
    const q = this.#queue(queue);
    if (spec.idempotencyKey) {
      for (const j of this.jobs.values()) {
        if (j.queue === queue && j.idempotencyKey === spec.idempotencyKey) return { jobId: j.id, created: false };
      }
    }
    const available = [...this.jobs.values()].filter((j) => j.queue === queue && j.state === JobState.AVAILABLE).length;
    if (q.maxDepth > 0n && BigInt(available) >= q.maxDepth) {
      throw new ConnectError("queue is full", Code.ResourceExhausted, undefined, [
        { desc: RetryInfoSchema, value: { retryDelay: durationFromMs(1000) } },
      ]);
    }
    const id = this.add(queue, spec.kind, (spec.payload ?? {}) as Record<string, unknown>, {
      priority: spec.priority,
      runAt: spec.runAt,
      idempotencyKey: spec.idempotencyKey,
      maxAttempts: spec.maxAttempts || q.maxAttempts,
      // Like the server (design §18, choice 7): an empty field takes the
      // caller's traceparent.
      traceContext: spec.traceContext || traceparent,
    });
    return { jobId: id, created: true };
  }

  #impl() {
    return {
      createQueue: async (req: { queue?: { name: string; maxDepth: bigint; visibilityTimeout?: Parameters<typeof durationToMs>[0]; maxAttempts: number; paused: boolean } }, ctx: HandlerContext) => {
        this.#record("createQueue", req, ctx);
        const q = req.queue!;
        if (this.queues.has(q.name)) throw new ConnectError(`queue "${q.name}" already exists`, Code.AlreadyExists);
        const fq = { vtMs: durationToMs(q.visibilityTimeout) || 30_000, maxAttempts: q.maxAttempts || 10, maxDepth: q.maxDepth };
        this.queues.set(q.name, fq);
        return { queue: { name: q.name, maxDepth: fq.maxDepth, visibilityTimeout: durationFromMs(fq.vtMs), maxAttempts: fq.maxAttempts, paused: q.paused } };
      },
      getQueue: async (req: { name: string }, ctx: HandlerContext) => {
        this.#record("getQueue", req, ctx);
        if (this.hangGetQueue) await new Promise<void>((r) => ctx.signal.addEventListener("abort", () => r(), { once: true }));
        const q = this.#queue(req.name);
        const count = (s: JobState) => BigInt([...this.jobs.values()].filter((j) => j.queue === req.name && j.state === s).length);
        return {
          queue: { name: req.name, maxDepth: q.maxDepth, visibilityTimeout: durationFromMs(q.vtMs), maxAttempts: q.maxAttempts, paused: false },
          stats: { available: count(JobState.AVAILABLE), leased: count(JobState.LEASED), dead: count(JobState.DEAD), recentlyCompleted: count(JobState.COMPLETED), recentWindow: durationFromMs(10_000) },
        };
      },
      enqueue: async (req: { queue: string; job?: JobSpec }, ctx: HandlerContext) => {
        this.#record("enqueue", req, ctx);
        return this.#enqueueOne(req.queue, req.job!, ctx.requestHeader.get("traceparent") ?? "");
      },
      enqueueBatch: async (req: { queue: string; jobs: JobSpec[] }, ctx: HandlerContext) => {
        this.#record("enqueueBatch", req, ctx);
        const tp = ctx.requestHeader.get("traceparent") ?? "";
        return { results: req.jobs.map((spec) => this.#enqueueOne(req.queue, spec, tp)) };
      },
      lease: async (req: { queue: string; maxJobs: number; wait?: Parameters<typeof durationToMs>[0]; visibilityTimeout?: Parameters<typeof durationToMs>[0] }, ctx: HandlerContext) => {
        this.#record("lease", req, ctx);
        const q = this.#queue(req.queue);
        const deadline = Date.now() + durationToMs(req.wait);
        for (;;) {
          const now = Date.now();
          const ready = [...this.jobs.values()].filter(
            (j) => j.queue === req.queue && j.state === JobState.AVAILABLE && (!j.runAt || timestampMs(j.runAt) <= now),
          );
          if (ready.length > 0) {
            const n = Math.min(ready.length, req.maxJobs + this.extraOnNextLease);
            this.extraOnNextLease = 0;
            const vt = durationToMs(req.visibilityTimeout) || q.vtMs;
            return {
              jobs: ready.slice(0, n).map((j) => {
                j.state = JobState.LEASED;
                j.attempts++;
                j.leaseToken = `token-${++this.#nextToken}`;
                j.leaseExpiresAt = timestampFromMs(now + vt);
                return clone(JobSchema, j);
              }),
            };
          }
          const left = deadline - Date.now();
          if (left <= 0) return { jobs: [] };
          await new Promise<void>((resolve) => {
            const timer = setTimeout(done, Math.min(left, 50));
            function done() {
              clearTimeout(timer);
              resolve();
            }
            this.#waiters.push(done);
            ctx.signal.addEventListener("abort", done, { once: true });
          });
          if (ctx.signal.aborted) throw new ConnectError("lease cancelled", Code.Canceled);
        }
      },
      ack: async (req: { id: bigint; leaseToken: string }, ctx: HandlerContext) => {
        this.#record("ack", req, ctx);
        if (this.ackDelayMs > 0) await new Promise((r) => setTimeout(r, this.ackDelayMs));
        const j = this.#job(req.id);
        if (j.state === JobState.COMPLETED && j.leaseToken === req.leaseToken) return {}; // retried Ack
        this.#holds(j, req.leaseToken);
        j.state = JobState.COMPLETED;
        j.finishedAt = timestampNow();
        return {};
      },
      nack: async (req: { id: bigint; leaseToken: string; error: string; retryAt?: Parameters<typeof timestampMs>[0] }, ctx: HandlerContext) => {
        this.#record("nack", req, ctx);
        const j = this.#job(req.id);
        this.#holds(j, req.leaseToken);
        j.lastError = req.error;
        if (j.attempts >= j.maxAttempts) {
          j.state = JobState.DEAD;
          j.finishedAt = timestampNow();
        } else {
          j.state = JobState.AVAILABLE;
          j.runAt = req.retryAt ?? timestampNow();
        }
        this.#wakeAll();
        return {};
      },
      heartbeat: async (req: { id: bigint; leaseToken: string; visibilityTimeout?: Parameters<typeof durationToMs>[0] }, ctx: HandlerContext) => {
        this.#record("heartbeat", req, ctx);
        if (this.hangHeartbeat?.()) await new Promise<void>((r) => ctx.signal.addEventListener("abort", () => r(), { once: true }));
        const j = this.#job(req.id);
        this.#holds(j, req.leaseToken);
        // Like the server, a heartbeat after the lease ran out is refused.
        if (j.leaseExpiresAt && timestampMs(j.leaseExpiresAt) < Date.now()) throw new ConnectError("lease expired", Code.FailedPrecondition);
        j.leaseExpiresAt = timestampFromMs(Date.now() + durationToMs(req.visibilityTimeout));
        return { leaseExpiresAt: j.leaseExpiresAt };
      },
      getJob: async (req: { id: bigint }, ctx: HandlerContext) => {
        this.#record("getJob", req, ctx);
        return { job: clone(JobSchema, this.#job(req.id)) };
      },
      cancelJob: async (req: { id: bigint }, ctx: HandlerContext) => {
        this.#record("cancelJob", req, ctx);
        const j = this.#job(req.id);
        if (j.state === JobState.CANCELLED) return { job: clone(JobSchema, j) }; // a retried cancel succeeds, like the server
        if (j.state !== JobState.AVAILABLE) throw new ConnectError("only available jobs can be cancelled", Code.FailedPrecondition);
        j.state = JobState.CANCELLED;
        j.finishedAt = timestampNow();
        return { job: clone(JobSchema, j) };
      },
      listDeadJobs: async (req: { queue: string; pageSize: number; pageToken: string }, ctx: HandlerContext) => {
        this.#record("listDeadJobs", req, ctx);
        this.#queue(req.queue);
        const dead = [...this.jobs.values()].filter((j) => j.queue === req.queue && j.state === JobState.DEAD);
        const start = req.pageToken ? Number(req.pageToken) : 0;
        const size = req.pageSize || 100;
        const page = dead.slice(start, start + size);
        return { jobs: page.map((j) => clone(JobSchema, j)), nextPageToken: start + size < dead.length ? String(start + size) : "" };
      },
      redriveJobs: async (req: { queue: string; ids: bigint[]; all: boolean }, ctx: HandlerContext) => {
        this.#record("redriveJobs", req, ctx);
        this.#queue(req.queue);
        const targets = req.all
          ? [...this.jobs.values()].filter((j) => j.queue === req.queue && j.state === JobState.DEAD).map((j) => j.id)
          : req.ids;
        let redriven = 0n;
        let skipped = 0n;
        for (const id of targets) {
          const j = this.jobs.get(id);
          if (j && j.queue === req.queue && j.state === JobState.DEAD) {
            j.state = JobState.AVAILABLE;
            j.attempts = 0;
            j.runAt = timestampNow();
            redriven++;
          } else {
            skipped++;
          }
        }
        this.#wakeAll();
        return { redriven, skipped };
      },
    };
  }
}
