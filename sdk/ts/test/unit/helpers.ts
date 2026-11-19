import type { Logger } from "../../src/types.js";

export interface LogLine {
  level: "info" | "warn" | "error";
  message: string;
  fields?: Record<string, unknown>;
}

export function memoryLogger(): { lines: LogLine[]; logger: Logger } {
  const lines: LogLine[] = [];
  const at = (level: LogLine["level"]) => (message: string, fields?: Record<string, unknown>) => {
    lines.push({ level, message, fields });
  };
  return { lines, logger: { info: at("info"), warn: at("warn"), error: at("error") } };
}

/** Polls cond every 10 ms until it is true; throws after timeoutMs. */
export async function until(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const end = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > end) throw new Error(`condition not met within ${timeoutMs} ms`);
    await new Promise((r) => setTimeout(r, 10));
  }
}
