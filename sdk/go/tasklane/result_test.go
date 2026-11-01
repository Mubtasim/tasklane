package tasklane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// results collects OnResult calls.
type results struct {
	mu  sync.Mutex
	got []Result
}

func (r *results) add(res Result) { r.mu.Lock(); defer r.mu.Unlock(); r.got = append(r.got, res) }
func (r *results) list() []Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Result(nil), r.got...)
}

func TestOnResultReportsAcked(t *testing.T) {
	f, c := newFake(t)
	var r results
	w := testWorker(c, WorkerOptions{OnResult: r.add})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(r.list()) == 1 })
	got := r.list()[0]
	require.Equal(t, OutcomeAcked, got.Outcome)
	require.Equal(t, int64(1), got.Job.ID)
	require.NoError(t, got.Err)
	require.False(t, got.Start.IsZero())
	require.False(t, got.End.Before(got.Start))
}

func TestOnResultReportsNackedWithTheHandlerError(t *testing.T) {
	f, c := newFake(t)
	var r results
	w := testWorker(c, WorkerOptions{OnResult: r.add})
	w.Handle("bad", func(context.Context, *LeasedJob) error { return errors.New("boom") })
	startWorker(t, w)

	f.push(fakeJob(1, "bad"))
	waitFor(t, func() bool { return len(r.list()) == 1 })
	require.Equal(t, OutcomeNacked, r.list()[0].Outcome)
	require.EqualError(t, r.list()[0].Err, "boom")
}

func TestOnResultReportsLeaseLostWhenTheAckIsRejected(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error { return status.Error(codes.FailedPrecondition, "lease lost") }
	})
	var r results
	w := testWorker(c, WorkerOptions{OnResult: r.add})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(r.list()) == 1 })
	require.Equal(t, OutcomeLeaseLost, r.list()[0].Outcome)
}

func TestOnResultReportsLeaseLostFromAHeartbeat(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.heartbeatFn = func(*tasklanev1.HeartbeatRequest) error { return status.Error(codes.FailedPrecondition, "lease lost") }
	})
	var r results
	// vt/3 heartbeats: a 60 ms lease heartbeats after 20 ms.
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 60 * time.Millisecond, OnResult: r.add})
	w.Handle("slow", func(ctx context.Context, _ *LeasedJob) error { <-ctx.Done(); return ctx.Err() })
	startWorker(t, w)

	f.push(fakeJob(1, "slow"))
	waitFor(t, func() bool { return len(r.list()) == 1 })
	require.Equal(t, OutcomeLeaseLost, r.list()[0].Outcome)
	require.Empty(t, f.calls().acks)
	require.Empty(t, f.calls().nacks)
}

func TestOnResultReportsGaveUpWhenTheServerIsUnreachable(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error { return status.Error(codes.Unavailable, "connection refused") }
	})
	var r results
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 50 * time.Millisecond, OnResult: r.add})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(r.list()) == 1 })
	require.Equal(t, OutcomeGaveUp, r.list()[0].Outcome)
}

func TestOnResultReportsAShutdownNack(t *testing.T) {
	f, c := newFake(t)
	var r results
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 50 * time.Millisecond, OnResult: r.add})
	started := make(chan struct{})
	w.Handle("slow", func(ctx context.Context, _ *LeasedJob) error { close(started); <-ctx.Done(); return ctx.Err() })
	stop := startWorker(t, w)

	f.push(fakeJob(1, "slow"))
	<-started
	_ = stop()
	require.Len(t, r.list(), 1, "exactly one result per job, even when drain answers for it")
	require.Equal(t, OutcomeNacked, r.list()[0].Outcome)
}

func TestOnResultPanicIsRecovered(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{OnResult: func(Result) { panic("observer bug") }})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	f.push(fakeJob(2, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 2 }) // the worker kept going
}

func TestOutcomeString(t *testing.T) {
	require.Equal(t, "acked", OutcomeAcked.String())
	require.Equal(t, "nacked", OutcomeNacked.String())
	require.Equal(t, "lease_lost", OutcomeLeaseLost.String())
	require.Equal(t, "gave_up", OutcomeGaveUp.String())
	require.Equal(t, "unknown", Outcome(0).String())
}
