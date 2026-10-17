package api

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// AuthInterceptor requires metadata "authorization: Bearer <key>" with one of
// keys. With no keys configured, every call is allowed.
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
		key, ok := strings.CutPrefix(values[0], "Bearer ")
		if !ok || !validKey(keys, key) {
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

// NewGRPCServer returns a gRPC server with the API registered behind the
// API-key interceptor. With no keys it logs a warning: auth is disabled.
func NewGRPCServer(srv *Server, keys []string, log *slog.Logger) *grpc.Server {
	if len(keys) == 0 {
		log.Warn("no API keys configured (TASKLANE_API_KEYS); authentication is disabled")
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(AuthInterceptor(keys)))
	tasklanev1.RegisterTasklaneServiceServer(gs, srv)
	return gs
}
