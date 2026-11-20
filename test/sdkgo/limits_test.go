package integrationtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// Both errors are RESOURCE_EXHAUSTED on the wire. A full queue carries
// RetryInfo and means "wait and retry"; a batch over the 16 MiB message limit
// means "this request can never succeed". The SDK must keep them apart.
func TestOversizeBatchIsNotQueueFull(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "small", MaxDepth: 1, VisibilityTimeout: time.Minute, MaxAttempts: 3})
	require.NoError(t, err)

	// 70 jobs × 250 KB ≈ 17.5 MB, over the 16 MiB send limit; each payload
	// alone is under the 256 KiB per-job cap.
	big := map[string]string{"pad": strings.Repeat("x", 250_000)}
	jobs := make([]tasklane.Job, 70)
	for i := range jobs {
		jobs[i] = tasklane.Job{Kind: "k", Payload: big}
	}
	_, err = e.client.EnqueueBatch(ctx, "small", jobs)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "err: %v", err)
	require.False(t, errors.Is(err, tasklane.ErrQueueFull), "an oversize request is not a full queue")

	refreshCtx, stop := context.WithCancel(ctx)
	defer stop()
	go e.server.RunDepthRefresh(refreshCtx)
	_, err = e.client.Enqueue(ctx, "small", tasklane.Job{Kind: "k"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := e.client.Enqueue(ctx, "small", tasklane.Job{Kind: "k"})
		var qf *tasklane.QueueFullError
		return errors.As(err, &qf) && qf.RetryAfter > 0
	}, 5*time.Second, 100*time.Millisecond, "a full queue is ErrQueueFull with a retry hint")
}
