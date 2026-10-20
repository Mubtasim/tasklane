package tasklane

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// awaitClosed fails the test if ch isn't closed within a second, so a
// regression fails fast instead of hanging.
func awaitClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the handler to start")
	}
}

// stopWithin stops the worker and fails if Run takes longer than d.
func stopWithin(t *testing.T, stop func() error, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- stop() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(d):
		t.Fatalf("Run did not return within %v", d)
	}
}

// releaseOnCleanup returns a channel that is closed at most once, and at the
// latest when the test ends, so a handler blocked on it can't outlive the test.
func releaseOnCleanup(t *testing.T) (release func(), ch chan struct{}) {
	ch = make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release, ch
}

func TestShutdownStopsLeasingAtOnce(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	w.leaseWait = 10 * time.Second // a long-poll is in flight when we stop
	stop := startWorker(t, w)

	waitFor(t, func() bool { return len(f.calls().leases) == 1 })
	start := time.Now()
	stopWithin(t, stop, time.Second)
	require.Less(t, time.Since(start), time.Second)
	require.Len(t, f.calls().leases, 1, "no lease after shutdown")
}

func TestShutdownLetsRunningJobsFinish(t *testing.T) {
	f, c := newFake(t)
	started := make(chan struct{})
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 5 * time.Second})
	w.Handle("slow", func(context.Context, *LeasedJob) error {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "slow"))
	awaitClosed(t, started)
	stopWithin(t, stop, 3*time.Second)
	require.Len(t, f.calls().acks, 1, "Run returns after the job's Ack")
	require.Empty(t, f.calls().nacks)
}

func TestShutdownNacksJobsThatOverrunTheDeadline(t *testing.T) {
	f, c := newFake(t)
	started := make(chan struct{})
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 100 * time.Millisecond})
	w.Handle("slow", func(ctx context.Context, _ *LeasedJob) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "slow"))
	awaitClosed(t, started)
	stopWithin(t, stop, 3*time.Second)
	nacks := f.calls().nacks
	require.Len(t, nacks, 1)
	require.Equal(t, "worker shutting down", nacks[0].GetError())
	require.Equal(t, "token-1", nacks[0].GetLeaseToken())
	// The handler's own return must not send a second Nack or an Ack.
	require.Never(t, func() bool {
		calls := f.calls()
		return len(calls.nacks) > 1 || len(calls.acks) > 0
	}, 150*time.Millisecond, 5*time.Millisecond)
}

func TestShutdownDoesNotWaitForAHandlerThatIgnoresItsContext(t *testing.T) {
	f, c := newFake(t)
	started := make(chan struct{})
	release, gate := releaseOnCleanup(t)
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 50 * time.Millisecond})
	w.Handle("stubborn", func(context.Context, *LeasedJob) error {
		close(started)
		<-gate
		return nil
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "stubborn"))
	awaitClosed(t, started)
	start := time.Now()
	stopWithin(t, stop, time.Second)
	require.Less(t, time.Since(start), time.Second)
	nacks := f.calls().nacks
	require.Len(t, nacks, 1)
	require.Equal(t, "worker shutting down", nacks[0].GetError())

	release() // the handler finishes late: its result must be dropped
	require.Never(t, func() bool {
		calls := f.calls()
		return len(calls.acks) > 0 || len(calls.nacks) > 1
	}, 150*time.Millisecond, 5*time.Millisecond)
}

func TestShutdownDoesNotNackAJobWhoseLeaseWasLost(t *testing.T) {
	f, c := newFake(t)
	started := make(chan struct{})
	release, gate := releaseOnCleanup(t)
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 50 * time.Millisecond, VisibilityTimeout: 30 * time.Millisecond})
	cancelled := make(chan struct{})
	w.Handle("slow", func(ctx context.Context, _ *LeasedJob) error {
		close(started)
		<-ctx.Done() // the heartbeat cancels it after setting lost
		close(cancelled)
		<-gate // then ignores the cancellation
		return nil
	})
	f.set(func(f *fakeServer) {
		f.heartbeatFn = func(*tasklanev1.HeartbeatRequest) error { return status.Error(codes.FailedPrecondition, "lease lost") }
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "slow"))
	awaitClosed(t, started)
	awaitClosed(t, cancelled)
	stopWithin(t, stop, time.Second)
	release()
	require.Never(t, func() bool {
		calls := f.calls()
		return len(calls.acks) > 0 || len(calls.nacks) > 0
	}, 150*time.Millisecond, 5*time.Millisecond)
}

func TestShutdownWaitsForAnAckInFlightAtTheDeadline(t *testing.T) {
	f, c := newFake(t)
	var acked atomic.Bool
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error {
			time.Sleep(150 * time.Millisecond) // slow: still running at the deadline
			acked.Store(true)
			return nil
		}
	})
	started := make(chan struct{})
	w := testWorker(c, WorkerOptions{ShutdownTimeout: 200 * time.Millisecond})
	w.Handle("late", func(context.Context, *LeasedJob) error {
		close(started)
		time.Sleep(120 * time.Millisecond) // returns shortly before the deadline
		return nil
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "late"))
	awaitClosed(t, started)
	stopWithin(t, stop, 2*time.Second)
	require.True(t, acked.Load(), "Run returned before the job's Ack finished")
	require.Empty(t, f.calls().nacks)
}

// A slow shutdown Nack and a slow in-flight Ack share one grace period after
// ShutdownTimeout, so Run returns within ShutdownTimeout + finishTimeout.
func TestShutdownWaitsOneGracePeriodForSlowNacksAndAcks(t *testing.T) {
	f, c := newFake(t)
	_, gate := releaseOnCleanup(t)
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error {
			<-gate // still in flight when Run returns
			return nil
		}
		f.nackFn = func(*tasklanev1.NackRequest) error {
			<-gate // the client gives up at its deadline
			return nil
		}
	})
	var started sync.WaitGroup
	started.Add(2)
	const shutdownTimeout, finishTimeout = 100 * time.Millisecond, 300 * time.Millisecond
	w := testWorker(c, WorkerOptions{Concurrency: 2, ShutdownTimeout: shutdownTimeout})
	w.finishTimeout = finishTimeout
	w.Handle("late", func(context.Context, *LeasedJob) error {
		started.Done()
		time.Sleep(50 * time.Millisecond) // returns before the deadline; its Ack hangs
		return nil
	})
	w.Handle("overrun", func(ctx context.Context, _ *LeasedJob) error {
		started.Done()
		<-ctx.Done() // cancelled at the deadline; its shutdown Nack hangs
		return ctx.Err()
	})
	stop := startWorker(t, w)

	f.push(fakeJob(1, "late"), fakeJob(2, "overrun"))
	allStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(allStarted)
	}()
	awaitClosed(t, allStarted)
	start := time.Now()
	stopWithin(t, stop, 2*time.Second)
	elapsed := time.Since(start)
	require.Less(t, elapsed, shutdownTimeout+finishTimeout+150*time.Millisecond)
	require.Len(t, f.calls().acks, 1)
	require.Len(t, f.calls().nacks, 1)
}
