package tasklane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

func TestHeartbeatsExtendALongJobsLease(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 90 * time.Millisecond}) // heartbeat every 30 ms
	w.Handle("long", func(context.Context, *LeasedJob) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	startWorker(t, w)

	f.push(fakeJob(1, "long"))
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
	hbs := f.calls().heartbeats
	require.GreaterOrEqual(t, len(hbs), 3)
	for _, hb := range hbs {
		require.Equal(t, int64(1), hb.GetId())
		require.Equal(t, "token-1", hb.GetLeaseToken())
		require.Equal(t, 90*time.Millisecond, hb.GetVisibilityTimeout().AsDuration())
	}
}

func TestALostLeaseCancelsTheHandlerAndSkipsAckAndNack(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.heartbeatFn = func(*tasklanev1.HeartbeatRequest) error { return status.Error(codes.FailedPrecondition, "lease lost") }
	})
	cancelled := make(chan error, 1)
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 60 * time.Millisecond})
	w.Handle("long", func(ctx context.Context, _ *LeasedJob) error {
		select {
		case <-ctx.Done():
			cancelled <- ctx.Err()
			return ctx.Err()
		case <-time.After(5 * time.Second):
			cancelled <- nil
			return nil
		}
	})
	startWorker(t, w)

	f.push(fakeJob(1, "long"))
	select {
	case err := <-cancelled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("the handler was not cancelled")
	}
	require.Never(t, func() bool {
		calls := f.calls()
		return len(calls.acks)+len(calls.nacks) > 0
	}, 150*time.Millisecond, 5*time.Millisecond)
	require.Len(t, f.calls().heartbeats, 1, "heartbeats stop once the lease is lost")
}

func TestAFailedHeartbeatIsRetriedAtTheNextTick(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.heartbeatFn = func(*tasklanev1.HeartbeatRequest) error { return status.Error(codes.Unavailable, "connection refused") }
	})
	handlerErr := make(chan error, 1)
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 60 * time.Millisecond}) // heartbeat every 20 ms
	w.Handle("long", func(ctx context.Context, _ *LeasedJob) error {
		time.Sleep(150 * time.Millisecond)
		handlerErr <- ctx.Err()
		return nil
	})
	startWorker(t, w)

	f.push(fakeJob(1, "long"))
	select {
	case err := <-handlerErr:
		require.NoError(t, err, "an unavailable server doesn't cancel the handler")
	case <-time.After(2 * time.Second):
		t.Fatal("the handler did not finish")
	}
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
	require.GreaterOrEqual(t, len(f.calls().heartbeats), 3)
}

func TestNoHeartbeatAfterTheJobFinishes(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 30 * time.Millisecond})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
	n := len(f.calls().heartbeats)
	require.Never(t, func() bool { return len(f.calls().heartbeats) > n }, 100*time.Millisecond, 5*time.Millisecond)
}
