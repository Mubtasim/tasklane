import { JobState as ProtoJobState, type Job as ProtoJob } from "./gen/tasklane/v1/tasklane_pb.js";
import type { RawService } from "./client.js";
import { rpc } from "./errors.js";
import { optionalDate } from "./internal/time.js";
import type { Priority } from "./producer.js";

/** Where a job is in its lifecycle (design §5); "" for a state this SDK doesn't know. */
export type JobState = "available" | "leased" | "completed" | "dead" | "cancelled" | "";

const states: Record<number, JobState> = {
  [ProtoJobState.AVAILABLE]: "available",
  [ProtoJobState.LEASED]: "leased",
  [ProtoJobState.COMPLETED]: "completed",
  [ProtoJobState.DEAD]: "dead",
  [ProtoJobState.CANCELLED]: "cancelled",
};

export interface JobInfo {
  id: bigint;
  queue: string;
  kind: string;
  payload: Record<string, unknown>;
  priority: Priority;
  state: JobState;
  runAt?: Date;
  attempts: number;
  maxAttempts: number;
  lastError: string;
  idempotencyKey: string;
  createdAt?: Date;
  finishedAt?: Date;
}

export interface DeadJobsPage {
  jobs: JobInfo[];
  /** Pass it back to get the next page; "" on the last page. */
  nextPageToken: string;
}

export interface RedriveResult {
  redriven: bigint;
  /** Ids that were not dead jobs of the queue. */
  skipped: bigint;
}

export function jobInfoFromProto(j: ProtoJob): JobInfo {
  return {
    id: j.id,
    queue: j.queue,
    kind: j.kind,
    payload: (j.payload ?? {}) as Record<string, unknown>,
    priority: j.priority as number as Priority,
    state: states[j.state] ?? "",
    runAt: optionalDate(j.runAt),
    attempts: j.attempts,
    maxAttempts: j.maxAttempts,
    lastError: j.lastError,
    idempotencyKey: j.idempotencyKey,
    createdAt: optionalDate(j.createdAt),
    finishedAt: optionalDate(j.finishedAt),
  };
}

export async function getJob(raw: RawService, id: bigint): Promise<JobInfo> {
  return rpc(async () => jobInfoFromProto((await raw.getJob({ id })).job!));
}

/** cancelJob cancels an available job; a leased or finished one is FailedPrecondition. */
export async function cancelJob(raw: RawService, id: bigint): Promise<JobInfo> {
  return rpc(async () => jobInfoFromProto((await raw.cancelJob({ id })).job!));
}

export async function listDeadJobs(
  raw: RawService,
  queue: string,
  page: { pageSize?: number; pageToken?: string } = {},
): Promise<DeadJobsPage> {
  return rpc(async () => {
    const res = await raw.listDeadJobs({ queue, pageSize: page.pageSize ?? 0, pageToken: page.pageToken ?? "" });
    return { jobs: res.jobs.map(jobInfoFromProto), nextPageToken: res.nextPageToken };
  });
}

export async function redriveJobs(raw: RawService, queue: string, ids: bigint[]): Promise<RedriveResult> {
  return rpc(async () => {
    const res = await raw.redriveJobs({ queue, ids });
    return { redriven: res.redriven, skipped: res.skipped };
  });
}

/** redriveAll sends every dead job of the queue back, in batches of 1,000 on the server. */
export async function redriveAll(raw: RawService, queue: string): Promise<bigint> {
  return rpc(async () => (await raw.redriveJobs({ queue, all: true })).redriven);
}
