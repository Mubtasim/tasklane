import { settlesWithin } from "./backoff.js";
import { codeOf, isLeaseLost, toError } from "./errors.js";
import { claim, report, type ActiveJob, type RunContext } from "./job.js";

/** The Nack message for jobs still running at the shutdown deadline. */
export const shutdownNackError = "worker shutting down";

/**
 * drain waits for running jobs after the lease loop stopped. Jobs still
 * running after shutdownTimeoutMs are aborted and nacked, so the server
 * retries them within seconds instead of after their lease expires. If a Nack
 * can't be sent, the lease expires and the reaper retries the job: the Nack
 * only makes recovery faster. drain doesn't wait for handlers that ignore
 * their signal. Shutdown Nacks and the Acks or Nacks already in flight share
 * one grace period (finishTimeoutMs), so run() resolves within
 * shutdownTimeoutMs + finishTimeoutMs.
 */
export async function drain(ctx: RunContext, shutdownTimeoutMs: number): Promise<void> {
  if (await settlesWithin(Promise.allSettled([...ctx.jobs]), shutdownTimeoutMs)) return;

  const graceEnd = performance.now() + ctx.internals.finishTimeoutMs;
  const nacks: Promise<void>[] = [];
  const finishing: ActiveJob[] = [];
  for (const a of [...ctx.active]) {
    if (a.lost) continue; // the job may belong to another worker
    if (!claim(a)) {
      // The job's own path won: its handler finished in time and its Ack or
      // Nack is about to be sent. Wait for it below.
      finishing.push(a);
      continue;
    }
    a.hbStop.abort();
    a.abort.abort(new Error(shutdownNackError));
    nacks.push(nackAtShutdown(ctx, a, graceEnd));
  }

  const inFlight = Promise.all(finishing.map((a) => a.sent));
  if (!(await settlesWithin(inFlight, Math.max(0, graceEnd - performance.now())))) {
    ctx.log.warn("tasklane: shutdown: an Ack or Nack is still in flight; the lease expiry covers it");
  }
  await Promise.all(nacks);
}

async function nackAtShutdown(ctx: RunContext, a: ActiveJob, graceEnd: number): Promise<void> {
  // Join the heartbeat before the Nack, as runJob does before its final
  // call. A lost lease can only be reported before the heartbeat exits.
  await a.hbDone;
  const reason = new Error(shutdownNackError);
  if (a.lost) {
    report(ctx, a, "lease_lost", reason);
    return;
  }
  const call = () =>
    ctx.client.raw.nack(
      { id: a.job.id, leaseToken: a.job.leaseToken, error: shutdownNackError },
      { timeoutMs: Math.ceil(Math.max(1, graceEnd - performance.now())) },
    );
  try {
    await (ctx.tracing ? ctx.tracing.around(a.span, call) : call());
    report(ctx, a, "nacked", reason);
    ctx.log.info("tasklane: nacked an unfinished job at shutdown", { jobId: String(a.job.id), kind: a.job.kind });
  } catch (e) {
    ctx.log.warn("tasklane: shutdown nack failed; the job is retried when its lease expires", { jobId: String(a.job.id), err: toError(e).message });
    report(ctx, a, isLeaseLost(codeOf(e)) ? "lease_lost" : "gave_up", reason);
  }
}
