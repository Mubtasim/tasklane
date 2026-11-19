export interface BackoffPolicy {
  baseMs: number;
  capMs: number;
  /** Returns a value in [0, 1); injectable for tests. */
  random?: () => number;
}

/**
 * backoffDelay is full jitter, as Go's internal/backoff: a random delay in
 * [0, min(cap, base · 2^attempt)).
 */
export function backoffDelay(p: BackoffPolicy, attempt: number): number {
  let ceiling = p.baseMs;
  for (let i = 0; i < attempt && ceiling < p.capMs; i++) ceiling *= 2;
  if (ceiling > p.capMs) ceiling = p.capMs;
  return (p.random ?? Math.random)() * ceiling;
}

/** sleep waits ms and resolves true, or resolves false as soon as signal aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<boolean> {
  if (signal?.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timer);
      resolve(false);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve(true);
    }, Math.max(0, ms));
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** settlesWithin reports whether p settles within ms; its timer never outlives the call. */
export async function settlesWithin(p: Promise<unknown>, ms: number): Promise<boolean> {
  const stop = new AbortController();
  try {
    return await Promise.race([
      p.then(
        () => true,
        () => true,
      ),
      sleep(ms, stop.signal).then(() => false),
    ]);
  } finally {
    stop.abort();
  }
}
