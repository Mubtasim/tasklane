import { createClient, type Client } from "../../src/index.js";

export const addr = process.env.TASKLANE_TEST_ADDR ?? "http://127.0.0.1:7070";
export const apiKey = process.env.TASKLANE_TEST_API_KEY ?? "test-key";

export function newClient(key = apiKey): Client {
  return createClient(addr, { apiKey: key });
}

/** A fresh queue name per test, so tests never see each other's jobs. */
export function queueName(prefix = "ts"): string {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
}
