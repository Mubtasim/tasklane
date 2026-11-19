import { expect, it } from "vitest";
import * as sdk from "../../src/index.js";

it("exports the public API", () => {
  expect(Object.keys(sdk).sort()).toEqual(
    ["Client", "Code", "Priority", "QueueFullError", "TasklaneError", "Worker", "createClient"].sort(),
  );
});
