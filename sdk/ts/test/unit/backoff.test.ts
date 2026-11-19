import { describe, expect, it } from "vitest";
import { backoffDelay, settlesWithin, sleep } from "../../src/backoff.js";

describe("backoffDelay", () => {
  it("is full jitter under a doubling ceiling with a cap", () => {
    const p = { baseMs: 1000, capMs: 30_000, random: () => 0.999 };
    expect(backoffDelay(p, 0)).toBeCloseTo(999, 0);
    expect(backoffDelay(p, 1)).toBeCloseTo(1998, 0);
    expect(backoffDelay(p, 4)).toBeCloseTo(15_984, 0);
    expect(backoffDelay(p, 5)).toBeCloseTo(29_970, 0); // 32 s capped at 30 s
    expect(backoffDelay(p, 1000)).toBeCloseTo(29_970, 0); // no overflow
    expect(backoffDelay({ ...p, random: () => 0 }, 3)).toBe(0);
  });
});

describe("sleep", () => {
  it("resolves true after the delay and false when aborted", async () => {
    expect(await sleep(5)).toBe(true);
    const c = new AbortController();
    const p = sleep(10_000, c.signal);
    c.abort();
    expect(await p).toBe(false);
  });
});

describe("settlesWithin", () => {
  it("reports whether the promise settled in time", async () => {
    expect(await settlesWithin(Promise.resolve(), 50)).toBe(true);
    expect(await settlesWithin(new Promise(() => {}), 20)).toBe(false);
    expect(await settlesWithin(Promise.reject(new Error("x")), 50)).toBe(true);
  });
});
