import { describe, expect, it } from "vitest";
import { JobState, Priority, TasklaneService } from "../../src/gen/tasklane/v1/tasklane_pb.js";
import { RetryInfoSchema } from "../../src/gen/google/rpc/error_details_pb.js";

describe("generated code", () => {
  it("has the service with every RPC", () => {
    expect(TasklaneService.typeName).toBe("tasklane.v1.TasklaneService");
    expect(Object.keys(TasklaneService.method).sort()).toEqual(
      [
        "ack", "cancelJob", "createQueue", "enqueue", "enqueueBatch", "getJob",
        "getQueue", "heartbeat", "lease", "listDeadJobs", "nack", "redriveJobs",
      ].sort(),
    );
  });

  it("strips the enum prefixes", () => {
    expect(Priority.CRITICAL).toBe(1);
    expect(Priority.LOW).toBe(4);
    expect(JobState.AVAILABLE).toBe(1);
    expect(JobState.CANCELLED).toBe(5);
  });

  it("has RetryInfo for the queue-full error", () => {
    expect(RetryInfoSchema.typeName).toBe("google.rpc.RetryInfo");
  });
});
