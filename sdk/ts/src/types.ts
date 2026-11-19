/** Anything with getTracer, such as an OpenTelemetry TracerProvider. */
export interface TracerProviderLike {
  getTracer(name: string, version?: string): unknown;
}

/** Where the SDK logs. `console` fits. */
export interface Logger {
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

const write =
  (fn: (...args: unknown[]) => void) =>
  (message: string, fields?: Record<string, unknown>): void => {
    if (fields === undefined) fn(message);
    else fn(message, fields);
  };

export const defaultLogger: Logger = {
  info: write(console.info),
  warn: write(console.warn),
  error: write(console.error),
};
