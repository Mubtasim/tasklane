import { describe, expect, it } from "vitest";
import { Slots } from "../../src/slots.js";

describe("Slots", () => {
  it("hands out up to capacity and gives a released slot to a waiter", async () => {
    const s = new Slots(2);
    const stop = new AbortController();
    expect(s.tryAcquire()).toBe(true);
    expect(await s.acquire(stop.signal)).toBe(true);
    expect(s.tryAcquire()).toBe(false);
    const waiting = s.acquire(stop.signal);
    s.release();
    expect(await waiting).toBe(true);
    expect(s.free).toBe(0);
  });

  it("stops waiting when aborted", async () => {
    const s = new Slots(1);
    s.tryAcquire();
    const stop = new AbortController();
    const waiting = s.acquire(stop.signal);
    stop.abort();
    expect(await waiting).toBe(false);
    s.release();
    expect(s.free).toBe(1);
  });

  it("refuses a release with nothing held", () => {
    expect(() => new Slots(1).release()).toThrow(/released twice/);
  });
});
