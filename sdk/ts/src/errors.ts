import { Code, ConnectError } from "@connectrpc/connect";
import { RetryInfoSchema } from "./gen/google/rpc/error_details_pb.js";
import { durationToMs } from "./internal/time.js";

export { Code };

/** An error from the server, with its gRPC code (design §6 lists them). */
export class TasklaneError extends Error {
  readonly code: Code;

  constructor(message: string, code: Code, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "TasklaneError";
    this.code = code;
  }
}

/**
 * The queue is at its max_depth. The SDK never retries it: a full queue means
 * the workers are behind, which the producer's owner should see.
 */
export class QueueFullError extends TasklaneError {
  readonly retryAfterMs: number;

  constructor(message: string, retryAfterMs: number, options?: { cause?: unknown }) {
    super(message, Code.ResourceExhausted, options);
    this.name = "QueueFullError";
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * toTasklaneError turns an RPC error into a TasklaneError. A queue-full
 * RESOURCE_EXHAUSTED carries RetryInfo; an oversize message is
 * RESOURCE_EXHAUSTED without it and stays a plain TasklaneError, as in Go.
 */
export function toTasklaneError(err: unknown): unknown {
  if (err instanceof TasklaneError) return err;
  if (!(err instanceof ConnectError)) return err;
  if (err.code === Code.ResourceExhausted) {
    const [info] = err.findDetails(RetryInfoSchema);
    if (info) return new QueueFullError(err.message, durationToMs(info.retryDelay), { cause: err });
  }
  return new TasklaneError(err.message, err.code, { cause: err });
}

export function codeOf(err: unknown): Code | undefined {
  if (err instanceof TasklaneError || err instanceof ConnectError) return err.code;
  return undefined;
}

const fatalCodes = new Set<Code>([Code.NotFound, Code.Unauthenticated, Code.PermissionDenied, Code.InvalidArgument]);

/** isFatal reports whether retrying cannot help (Go's fatal). */
export function isFatal(code: Code | undefined): boolean {
  return code !== undefined && fatalCodes.has(code);
}

/** isLeaseLost reports whether the server said this worker no longer holds the lease. */
export function isLeaseLost(code: Code | undefined): boolean {
  return code === Code.FailedPrecondition || code === Code.NotFound;
}

/** rpc runs one call and converts its error. */
export async function rpc<T>(call: () => Promise<T>): Promise<T> {
  try {
    return await call();
  } catch (e) {
    throw toTasklaneError(e);
  }
}

export function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}

/** @internal The reason a job's signal is aborted at shutdown; tracing tells it from a handler's own error by type. */
export class ShutdownError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ShutdownError";
  }
}
