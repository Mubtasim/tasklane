import { createClient as createConnectClient, type Client as ConnectClient, type Interceptor, type Transport } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { TasklaneService } from "./gen/tasklane/v1/tasklane_pb.js";
import * as admin from "./admin.js";
import * as producer from "./producer.js";
import { getTracer, tracingInterceptor } from "./tracing.js";
import type { TracerProviderLike } from "./types.js";

/** The server's message limit (design §6), both directions. */
const maxMessageBytes = 16 << 20;

export type RawService = ConnectClient<typeof TasklaneService>;

export interface ClientOptions {
  /** Sent as `authorization: Bearer <key>` on every call. */
  apiKey?: string;
  /** Defaults to OpenTelemetry's global provider when @opentelemetry/api is installed. */
  tracerProvider?: TracerProviderLike;
}

export class Client {
  /** @internal */
  readonly raw: RawService;
  /** @internal */
  readonly options: ClientOptions;
  /** @internal */
  readonly interceptors: Interceptor[] = [];
  /** @internal The OpenTelemetry tracer, or undefined without @opentelemetry/api. */
  readonly tracer: unknown;
  #close: () => void;
  #closed = false;

  /** @internal Use createClient. */
  constructor(makeTransport: (interceptors: Interceptor[]) => Transport, options: ClientOptions = {}, close: () => void = () => {}) {
    this.options = options;
    this.#close = close;
    if (options.apiKey) {
      const value = `Bearer ${options.apiKey}`;
      this.interceptors.push((next) => (req) => {
        req.header.set("authorization", value);
        return next(req);
      });
    }
    this.tracer = getTracer(options.tracerProvider);
    this.interceptors.push(tracingInterceptor(this.tracer as Parameters<typeof tracingInterceptor>[0]));
    this.raw = createConnectClient(TasklaneService, makeTransport(this.interceptors));
  }

  get closed(): boolean {
    return this.#closed;
  }

  /** close drops the connection. Stop every Worker first. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#close();
  }

  enqueue<P extends object>(queue: string, job: producer.Job<P>): Promise<producer.EnqueueResult> {
    return producer.enqueue(this.raw, queue, job);
  }

  /** enqueueBatch enqueues up to 1,000 jobs in one transaction; results keep the order. */
  enqueueBatch(queue: string, jobs: producer.Job<object>[]): Promise<producer.EnqueueResult[]> {
    return producer.enqueueBatch(this.raw, queue, jobs);
  }

  createQueue(cfg: producer.QueueConfig): Promise<producer.QueueSettings> {
    return producer.createQueue(this.raw, cfg);
  }

  getQueue(name: string): Promise<producer.QueueInfo> {
    return producer.getQueue(this.raw, name);
  }

  getJob(id: bigint): Promise<admin.JobInfo> {
    return admin.getJob(this.raw, id);
  }

  cancelJob(id: bigint): Promise<admin.JobInfo> {
    return admin.cancelJob(this.raw, id);
  }

  listDeadJobs(queue: string, page?: { pageSize?: number; pageToken?: string }): Promise<admin.DeadJobsPage> {
    return admin.listDeadJobs(this.raw, queue, page);
  }

  redriveJobs(queue: string, ids: bigint[]): Promise<admin.RedriveResult> {
    return admin.redriveJobs(this.raw, queue, ids);
  }

  redriveAll(queue: string): Promise<bigint> {
    return admin.redriveAll(this.raw, queue);
  }
}

/**
 * createClient connects to the server's gRPC port, e.g.
 * "http://localhost:7070" (h2c) or "https://…" (TLS). It does not block: the
 * HTTP/2 connection is made on first use and re-made after failures.
 */
export function createClient(baseUrl: string, options: ClientOptions = {}): Client {
  const sessionManager = new Http2SessionManager(baseUrl);
  return new Client(
    (interceptors) =>
      createGrpcTransport({ baseUrl, sessionManager, interceptors, readMaxBytes: maxMessageBytes, writeMaxBytes: maxMessageBytes }),
    options,
    () => sessionManager.abort(),
  );
}
