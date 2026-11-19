import type { JsonObject, MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import type { JobSpecSchema, Priority as ProtoPriority, Queue } from "./gen/tasklane/v1/tasklane_pb.js";
import type { RawService } from "./client.js";
import { rpc } from "./errors.js";
import { durationFromMs, durationToMs } from "./internal/time.js";

/** Leasing order: lower values run first. Unspecified lets the server use Default. */
export const Priority = { Unspecified: 0, Critical: 1, High: 2, Default: 3, Low: 4 } as const;
export type Priority = (typeof Priority)[keyof typeof Priority];

export interface Job<P extends object = Record<string, unknown>> {
  kind: string;
  /** JSON-encoded; must be a plain object. Omitted means an empty payload. */
  payload?: P;
  priority?: Priority;
  /** Delays the job until then. */
  runAt?: Date;
  /** A repeated enqueue with the same key returns the original job. */
  idempotencyKey?: string;
  /** Overrides the queue's setting. */
  maxAttempts?: number;
}

export interface EnqueueResult {
  jobId: bigint;
  /** false when idempotencyKey matched an existing job. */
  created: boolean;
}

/** Queue settings to create; omitted values take the server's defaults. */
export interface QueueConfig {
  name: string;
  maxDepth?: bigint | number;
  visibilityTimeoutMs?: number;
  maxAttempts?: number;
  paused?: boolean;
}

export interface QueueSettings {
  name: string;
  maxDepth: bigint;
  visibilityTimeoutMs: number;
  maxAttempts: number;
  paused: boolean;
}

/** Live counts. Completed jobs are counted only over the last recentWindowMs. */
export interface QueueStats {
  available: bigint;
  leased: bigint;
  dead: bigint;
  recentlyCompleted: bigint;
  recentWindowMs: number;
}

export interface QueueInfo {
  config: QueueSettings;
  stats: QueueStats;
}

/** toPayload checks and JSON-normalizes a payload: undefined or null means none. */
export function toPayload(payload: unknown): JsonObject | undefined {
  if (payload === undefined || payload === null) return undefined;
  if (typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError(`tasklane: payload must be a JSON object, not ${Array.isArray(payload) ? "an array" : typeof payload}`);
  }
  return JSON.parse(JSON.stringify(payload)) as JsonObject;
}

function jobToSpec(job: Job<object>): MessageInitShape<typeof JobSpecSchema> {
  return {
    kind: job.kind,
    payload: toPayload(job.payload),
    priority: (job.priority ?? Priority.Unspecified) as number as ProtoPriority,
    runAt: job.runAt ? timestampFromDate(job.runAt) : undefined,
    idempotencyKey: job.idempotencyKey ?? "",
    maxAttempts: job.maxAttempts ?? 0,
  };
}

export function queueFromProto(q: Queue | undefined): QueueSettings {
  return {
    name: q?.name ?? "",
    maxDepth: q?.maxDepth ?? 0n,
    visibilityTimeoutMs: durationToMs(q?.visibilityTimeout),
    maxAttempts: q?.maxAttempts ?? 0,
    paused: q?.paused ?? false,
  };
}

export async function enqueue(raw: RawService, queue: string, job: Job<object>): Promise<EnqueueResult> {
  const spec = jobToSpec(job);
  return rpc(async () => {
    const res = await raw.enqueue({ queue, job: spec });
    return { jobId: res.jobId, created: res.created };
  });
}

export async function enqueueBatch(raw: RawService, queue: string, jobs: Job<object>[]): Promise<EnqueueResult[]> {
  const specs = jobs.map(jobToSpec);
  return rpc(async () => {
    const res = await raw.enqueueBatch({ queue, jobs: specs });
    return res.results.map((r) => ({ jobId: r.jobId, created: r.created }));
  });
}

export async function createQueue(raw: RawService, cfg: QueueConfig): Promise<QueueSettings> {
  return rpc(async () => {
    const res = await raw.createQueue({
      queue: {
        name: cfg.name,
        maxDepth: BigInt(cfg.maxDepth ?? 0),
        visibilityTimeout: cfg.visibilityTimeoutMs ? durationFromMs(cfg.visibilityTimeoutMs) : undefined,
        maxAttempts: cfg.maxAttempts ?? 0,
        paused: cfg.paused ?? false,
      },
    });
    return queueFromProto(res.queue);
  });
}

export async function getQueue(raw: RawService, name: string): Promise<QueueInfo> {
  return rpc(async () => {
    const res = await raw.getQueue({ name });
    const s = res.stats;
    return {
      config: queueFromProto(res.queue),
      stats: {
        available: s?.available ?? 0n,
        leased: s?.leased ?? 0n,
        dead: s?.dead ?? 0n,
        recentlyCompleted: s?.recentlyCompleted ?? 0n,
        recentWindowMs: durationToMs(s?.recentWindow),
      },
    };
  });
}
