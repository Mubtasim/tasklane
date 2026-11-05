// Package tasklane is the Go client for a Tasklane job queue server: a
// producer API (Enqueue, queues) and a Worker that leases jobs, runs the
// handler registered for each job's kind, heartbeats long jobs and
// acknowledges them, with graceful shutdown.
package tasklane

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/traceparent"
)

// maxMessageBytes matches the server's gRPC message limit: a full Lease
// response can be about 13 MiB.
const maxMessageBytes = 16 << 20

// Client talks to one Tasklane server. It is safe for concurrent use; create
// one per process and share it.
type Client struct {
	conn *grpc.ClientConn
	api  tasklanev1.TasklaneServiceClient

	tracer trace.Tracer
}

// Option configures Dial.
type Option func(*dialConfig)

type dialConfig struct {
	apiKey   string
	grpcOpts []grpc.DialOption
	tp       trace.TracerProvider
}

// WithAPIKey sends "authorization: Bearer <key>" with every call.
func WithAPIKey(key string) Option {
	return func(c *dialConfig) { c.apiKey = key }
}

// WithGRPCDialOptions adds gRPC dial options. They apply after the defaults,
// so they can replace them: for example grpc.WithTransportCredentials for TLS.
func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(c *dialConfig) { c.grpcOpts = append(c.grpcOpts, opts...) }
}

// WithTracerProvider records the client's spans with tp: a client span per
// RPC (heartbeats excepted) and a "handle <kind>" span per job run. The
// default is the global provider, a no-op unless the application sets one.
// The SDK never configures exporters; the application does.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *dialConfig) { c.tp = tp }
}

// Dial creates a client for the server's gRPC address ("host:7070"). It does
// not block: the connection is made on first use and re-made after failures.
// The default transport is plaintext; pass TLS credentials with
// WithGRPCDialOptions.
func Dial(target string, opts ...Option) (*Client, error) {
	var cfg dialConfig
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.tp == nil {
		cfg.tp = otel.GetTracerProvider()
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(cfg.tp),
			otelgrpc.WithPropagators(traceparent.W3C),
			otelgrpc.WithFilter(notHeartbeat),
		)),
	}
	if cfg.apiKey != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerToken(cfg.apiKey)))
	}
	conn, err := grpc.NewClient(target, append(dialOpts, cfg.grpcOpts...)...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: tasklanev1.NewTasklaneServiceClient(conn),
		tracer: cfg.tp.Tracer("github.com/mubtasim/tasklane/sdk/go/tasklane")}, nil
}

// Close closes the connection. Stop every Worker first.
func (c *Client) Close() error {
	return c.conn.Close()
}

// bearerToken sends the API key as metadata on every call. It doesn't require
// TLS, so it works with the default plaintext transport (local development, a
// private network); use TLS wherever the key could be read on the wire.
type bearerToken string

func (t bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (bearerToken) RequireTransportSecurity() bool { return false }
