package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

func withKey(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
}

func TestAuthAcceptsAndRejects(t *testing.T) {
	e := newEnv(t, []string{"key-one", "key-two"})
	req := &tasklanev1.GetQueueRequest{Name: "nope"}

	_, err := e.client.GetQueue(context.Background(), req)
	requireCode(t, codes.Unauthenticated, err) // no key

	_, err = e.client.GetQueue(withKey("wrong"), req)
	requireCode(t, codes.Unauthenticated, err)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "key-one") // no "Bearer "
	_, err = e.client.GetQueue(ctx, req)
	requireCode(t, codes.Unauthenticated, err)

	_, err = e.client.GetQueue(withKey("key-two"), req)
	requireCode(t, codes.NotFound, err) // authenticated: the call reached the store
}

func TestAuthDisabledWithoutKeys(t *testing.T) {
	e := newEnv(t, nil)
	_, err := e.client.GetQueue(context.Background(), &tasklanev1.GetQueueRequest{Name: "nope"})
	requireCode(t, codes.NotFound, err)
}
