import { create } from "@bufbuild/protobuf";
import { DurationSchema, timestampDate, type Duration, type Timestamp } from "@bufbuild/protobuf/wkt";

export function durationFromMs(ms: number): Duration {
  const seconds = Math.floor(ms / 1000);
  return create(DurationSchema, {
    seconds: BigInt(seconds),
    nanos: Math.round((ms - seconds * 1000) * 1_000_000),
  });
}

export function durationToMs(d: Duration | undefined): number {
  if (!d) return 0;
  return Number(d.seconds) * 1000 + Math.round(d.nanos / 1_000_000);
}

export function optionalDate(t: Timestamp | undefined): Date | undefined {
  return t ? timestampDate(t) : undefined;
}
