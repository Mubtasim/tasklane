/**
 * Slots counts running jobs, like Go's buffered channel: acquire waits for a
 * free slot, release hands it to the next waiter (or frees it). Every acquired
 * slot must be released on every path, success and error: a leaked slot stops
 * the worker without any error.
 */
export class Slots {
  readonly capacity: number;
  #free: number;
  #waiters: Array<() => void> = [];

  constructor(capacity: number) {
    this.capacity = capacity;
    this.#free = capacity;
  }

  get free(): number {
    return this.#free;
  }

  tryAcquire(): boolean {
    if (this.#free === 0) return false;
    this.#free--;
    return true;
  }

  /** acquire resolves true with a slot, or false if signal aborts first. */
  acquire(signal: AbortSignal): Promise<boolean> {
    if (this.tryAcquire()) return Promise.resolve(true);
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const waiter = () => {
        signal.removeEventListener("abort", onAbort);
        resolve(true);
      };
      const onAbort = () => {
        this.#waiters = this.#waiters.filter((w) => w !== waiter);
        resolve(false);
      };
      this.#waiters.push(waiter);
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  release(): void {
    const next = this.#waiters.shift();
    if (next) {
      next(); // the slot passes straight to the waiter
      return;
    }
    if (this.#free >= this.capacity) throw new Error("tasklane: slot released twice");
    this.#free++;
  }
}
