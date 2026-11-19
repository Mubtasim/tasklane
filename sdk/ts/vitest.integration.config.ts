import { defineConfig } from "vitest/config";

// Runs against a real server: TASKLANE_TEST_ADDR (default
// http://127.0.0.1:7070) with TASKLANE_TEST_API_KEY (default test-key).
// Start one with scripts/integration-server.sh.
export default defineConfig({
  test: {
    include: ["test/integration/**/*.test.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    fileParallelism: false,
  },
});
