// Package tasklane is the Go client for a Tasklane job queue server: a
// producer API (Enqueue, queues) and a Worker that leases jobs, runs the
// handler registered for each job's kind, heartbeats long jobs and
// acknowledges them, with graceful shutdown.
package tasklane

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// maxMessageBytes matches the server's gRPC message limit: a full Lease
// response can be about 13 MiB.
const maxMessageBytes = 16 << 20

// Client talks to one Tasklane server. It is safe for concurrent use; create
// one per process and share it.
type Client struct {
	conn *grpc.ClientConn
	api  tasklanev1.TasklaneServiceClient
}

// Option configures Dial.
type Option func(*dialConfig)

type dialConfig struct {
	apiKey   string
	grpcOpts []grpc.DialOption
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

// Dial creates a client for the server's gRPC address ("host:7070"). It does
// not block: the connection is made on first use and re-made after failures.
// The default transport is plaintext; pass TLS credentials with
// WithGRPCDialOptions.
func Dial(target string, opts ...Option) (*Client, error) {
	var cfg dialConfig
	for _, o := range opts {
		o(&cfg)
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
	}
	if cfg.apiKey != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerToken(cfg.apiKey)))
	}
	conn, err := grpc.NewClient(target, append(dialOpts, cfg.grpcOpts...)...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: tasklanev1.NewTasklaneServiceClient(conn)}, nil
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
