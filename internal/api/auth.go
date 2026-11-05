package api

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"path"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/traceparent"
)

// AuthInterceptor requires metadata "authorization: Bearer <key>" with one of
// keys; the scheme is case-insensitive. With no keys configured, every call
// is allowed.
func AuthInterceptor(keys []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if len(keys) == 0 {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing API key")
		}
		scheme, key, ok := strings.Cut(values[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || !validKey(keys, key) {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}
		return handler(ctx, req)
	}
}

// validKey compares in constant time, so response timing doesn't reveal how
// much of a key was right.
func validKey(keys []string, key string) bool {
	found := 0
	for _, k := range keys {
		found |= subtle.ConstantTimeCompare([]byte(k), []byte(key))
	}
	return found == 1
}

// notHeartbeat keeps heartbeats out of traces: one every third of a
// visibility timeout per running job would be noise, and metrics cover them.
func notHeartbeat(info *stats.RPCTagInfo) bool {
	return path.Base(info.FullMethodName) != "Heartbeat"
}

// NewGRPCServer returns a gRPC server with the API registered behind the
// API-key interceptor, sending and receiving messages up to MaxMessageBytes.
// With no keys it logs a warning: auth is disabled.
func NewGRPCServer(srv *Server, keys []string, log *slog.Logger) *grpc.Server {
	if len(keys) == 0 {
		log.Warn("no API keys configured (TASKLANE_API_KEYS); authentication is disabled")
	}
	gs := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithTracerProvider(srv.tp),
			otelgrpc.WithPropagators(traceparent.W3C),
			otelgrpc.WithFilter(notHeartbeat),
		)),
		// Metrics go first, so refused calls (bad API key) are timed too.
		grpc.ChainUnaryInterceptor(srv.metrics.UnaryInterceptor(), AuthInterceptor(keys)),
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
	)
	tasklanev1.RegisterTasklaneServiceServer(gs, srv)
	return gs
}
