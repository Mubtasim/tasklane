import type { WorkerInternals } from "../../src/worker.js";

export const fast: Partial<WorkerInternals> = {
  leaseWaitMs: 100,
  leaseMarginMs: 1000,
  retry: { baseMs: 10, capMs: 50 },
  finishTimeoutMs: 200,
  lagWarnMs: 150,
};
