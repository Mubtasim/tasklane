import { sleep } from "./backoff.js";
import { codeOf, isLeaseLost, toError } from "./errors.js";
import { durationFromMs } from "./internal/time.js";
import type { ActiveJob, RunContext } from "./job.js";

/**
 * heartbeat extends a's lease every VT/3 until a.hbStop aborts, so one lost
 * or late heartbeat doesn't lose the lease. It uses performance.now(), never
 * the server's lease_expires_at, so clock skew can't break it. When the
 * server says the lease is gone, it aborts the handler.
 *
 * Node runs JavaScript on one thread: a handler that keeps the CPU busy also
 * delays these timers. A timer that fires more than lagWarnMs late is logged,
 * because past the visibility timeout the job runs twice (design §21.1, 2).
 */
export async function heartbeat(ctx: RunContext, a: ActiveJob): Promise<void> {
  const vt = ctx.vtMs;
  const interval = vt / 3 > 0 ? vt / 3 : vt;
  const signal = a.hbStop.signal;
  // Fixed due times, like Go's time.Ticker: a heartbeat that hangs until its
  // timeout must not push the later ones back, or the lease would run out.
  let next = performance.now() + interval;
  for (;;) {
    const due = next;
    const slept = await sleep(due - performance.now(), signal);
    // Measure even when the job just finished: a handler that blocked the
    // loop past this tick returns before the timer callback gets to run, so
    // the stop is the first thing this loop sees. Stopping early is never late.
    const late = performance.now() - due;
    if (late > ctx.internals.lagWarnMs) {
      ctx.log.warn(`tasklane: event loop blocked for about ${Math.round(late / 1000)} s; heartbeats were delayed`, {
        jobId: String(a.job.id),
        kind: a.job.kind,
        lateMs: Math.round(late),
      });
    }
    if (!slept) return;
    next += interval;
    const sent = performance.now();
    try {
      await ctx.client.raw.heartbeat(
        { id: a.job.id, leaseToken: a.job.leaseToken, visibilityTimeout: durationFromMs(vt) },
        { timeoutMs: Math.ceil(interval), signal },
      );
      a.deadline = sent + vt;
    } catch (e) {
      if (signal.aborted) return;
      if (isLeaseLost(codeOf(e))) {
        // Another worker may hold the job now. Stopping the handler shortens
        // the time both run it; it can't remove it, which is why delivery is
        // at-least-once and handlers must be idempotent.
        ctx.log.warn("tasklane: lease lost; aborting the handler", { jobId: String(a.job.id), kind: a.job.kind, err: toError(e).message });
        a.lost = true;
        a.abort.abort(new Error("tasklane: lease lost"));
        return;
      }
      ctx.log.warn("tasklane: heartbeat failed; retrying at the next tick", { jobId: String(a.job.id), err: toError(e).message });
    }
    // Ticks were missed (a long block or a slow call): don't fire a burst.
    const now = performance.now();
    if (now > next + interval) next = now + interval;
  }
}
