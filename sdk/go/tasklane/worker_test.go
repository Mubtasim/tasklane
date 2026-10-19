package tasklane

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/backoff"
)

// testWorker returns a worker on queue "q" with short waits, so tests run in
// milliseconds: 50 ms long-polls, 1–5 ms backoff and (unless set) a 1 s
// shutdown timeout.
func testWorker(c *Client, opts WorkerOptions) *Worker {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = time.Second
	}
	w := c.NewWorker("q", opts)
	w.leaseWait = 50 * time.Millisecond
	w.retry = backoff.Policy{Base: time.Millisecond, Cap: 5 * time.Millisecond, Rand: rand.Float64}
	return w
}

// startWorker runs w in the background. stop cancels Run and returns its
// error; it also runs when the test ends, before the fake server stops.
func startWorker(t *testing.T, w *Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			err = <-errc
		})
		return err
	}
	t.Cleanup(func() { stop() })
	return stop
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 2*time.Second, 5*time.Millisecond)
}

func succeed(context.Context, *LeasedJob) error { return nil }

func TestWorkerAcksASuccessfulJob(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
	ack := f.calls().acks[0]
	require.Equal(t, int64(1), ack.GetId())
	require.Equal(t, "token-1", ack.GetLeaseToken())
	require.Empty(t, f.calls().nacks)
}

func TestWorkerNacksAFailedJob(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	w.Handle("bad", func(context.Context, *LeasedJob) error { return errors.New("boom") })
	startWorker(t, w)

	f.push(fakeJob(1, "bad"))
	waitFor(t, func() bool { return len(f.calls().nacks) == 1 })
	nack := f.calls().nacks[0]
	require.Equal(t, "boom", nack.GetError())
	require.Equal(t, "token-1", nack.GetLeaseToken())
	require.Nil(t, nack.GetRetryAt())
	require.Empty(t, f.calls().acks)
}

func TestRetryAtSetsTheRetryTime(t *testing.T) {
	f, c := newFake(t)
	at := time.Now().Add(time.Hour).Truncate(time.Second)
	w := testWorker(c, WorkerOptions{})
	w.Handle("slow_down", func(context.Context, *LeasedJob) error { return RetryAt(errors.New("rate limited"), at) })
	startWorker(t, w)

	f.push(fakeJob(1, "slow_down"))
	waitFor(t, func() bool { return len(f.calls().nacks) == 1 })
	nack := f.calls().nacks[0]
	require.Equal(t, "rate limited", nack.GetError())
	require.True(t, at.Equal(nack.GetRetryAt().AsTime()))
}

func TestAPanicIsRecoveredAndNacked(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	w.Handle("explode", func(context.Context, *LeasedJob) error { panic("kaboom") })
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "explode"), fakeJob(2, "ok"))
	waitFor(t, func() bool { return len(f.calls().nacks) == 1 && len(f.calls().acks) == 1 })
	require.Equal(t, "handler panicked: kaboom", f.calls().nacks[0].GetError())
	require.Equal(t, int64(2), f.calls().acks[0].GetId(), "the worker keeps running")
}

func TestAJobWithoutAHandlerIsNacked(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	startWorker(t, w)

	f.push(fakeJob(1, "mystery"))
	waitFor(t, func() bool { return len(f.calls().nacks) == 1 })
	require.Equal(t, `no handler for kind "mystery"`, f.calls().nacks[0].GetError())
}

func TestHandlerSeesTheJob(t *testing.T) {
	f, c := newFake(t)
	got := make(chan LeasedJob, 1)
	decoded := make(chan emailPayload, 1)
	w := testWorker(c, WorkerOptions{})
	w.Handle("send_email", func(_ context.Context, job *LeasedJob) error {
		var p emailPayload
		if err := job.Decode(&p); err != nil {
			return err
		}
		got <- *job
		decoded <- p
		return nil
	})
	startWorker(t, w)

	job := fakeJob(7, "send_email")
	job.Attempts = 2
	job.Payload, _ = structpb.NewStruct(map[string]any{"to": "a@example.com", "retries": 1})
	f.push(job)

	j := <-got
	require.Equal(t, int64(7), j.ID)
	require.Equal(t, "send_email", j.Kind)
	require.Equal(t, 2, j.Attempt)
	require.Equal(t, 5, j.MaxAttempts)
	require.Equal(t, emailPayload{To: "a@example.com", Retries: 1}, <-decoded)
}

func TestLeaseAsksForFreeSlotsWithTheQueuesVisibilityTimeout(t *testing.T) {
	f, c := newFake(t)
	startWorker(t, testWorker(c, WorkerOptions{Concurrency: 3}))

	waitFor(t, func() bool { return len(f.calls().leases) >= 1 })
	lease := f.calls().leases[0]
	require.Equal(t, "q", lease.GetQueue())
	require.Equal(t, int32(3), lease.GetMaxJobs())
	require.Equal(t, 50*time.Millisecond, lease.GetWait().AsDuration())
	require.Equal(t, 30*time.Second, lease.GetVisibilityTimeout().AsDuration())
}

func TestVisibilityTimeoutOptionOverridesTheQueue(t *testing.T) {
	f, c := newFake(t)
	startWorker(t, testWorker(c, WorkerOptions{VisibilityTimeout: 2 * time.Second}))

	waitFor(t, func() bool { return len(f.calls().leases) >= 1 })
	require.Equal(t, 2*time.Second, f.calls().leases[0].GetVisibilityTimeout().AsDuration())
}

// The silent-stall regression: an empty long-poll must give its slots back,
// or the worker stops taking jobs without any error.
func TestSlotsComeBackAfterEmptyLeases(t *testing.T) {
	f, c := newFake(t)
	w := testWorker(c, WorkerOptions{Concurrency: 2})
	w.Handle("ok", succeed)
	startWorker(t, w)

	waitFor(t, func() bool { return len(f.calls().leases) >= 3 }) // two empty polls done
	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
	waitFor(t, func() bool {
		leases := f.calls().leases
		return leases[len(leases)-1].GetMaxJobs() == 2
	})
}

func TestNeverRunsMoreThanConcurrencyJobs(t *testing.T) {
	f, c := newFake(t)
	var running, peak atomic.Int32
	w := testWorker(c, WorkerOptions{Concurrency: 2})
	w.Handle("work", func(context.Context, *LeasedJob) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	for i := range 6 {
		f.push(fakeJob(int64(i+1), "work"))
	}
	startWorker(t, w)

	waitFor(t, func() bool { return len(f.calls().acks) == 6 })
	require.Equal(t, int32(2), peak.Load(), "two jobs ran at once, never more")
	for _, l := range f.calls().leases {
		require.LessOrEqual(t, l.GetMaxJobs(), int32(2))
	}
}

func TestLeaseErrorsBackOffAndRecover(t *testing.T) {
	f, c := newFake(t)
	var failures atomic.Int32
	f.set(func(f *fakeServer) {
		f.leaseFn = func(context.Context, *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error) {
			if failures.Add(1) <= 3 {
				return nil, status.Error(codes.Unavailable, "server restarting")
			}
			return &tasklanev1.LeaseResponse{Jobs: []*tasklanev1.Job{fakeJob(1, "ok")}}, nil
		}
	})
	w := testWorker(c, WorkerOptions{})
	w.Handle("ok", succeed)
	startWorker(t, w)

	waitFor(t, func() bool { return len(f.calls().acks) >= 1 })
	require.GreaterOrEqual(t, len(f.calls().leases), 4)
}

func TestRunFailsForAnUnknownQueue(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) { f.queue = nil })

	err := testWorker(c, WorkerOptions{}).Run(context.Background())
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestRunFailsWhenLeaseIsUnauthenticated(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.leaseFn = func(context.Context, *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error) {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}
	})

	err := testWorker(c, WorkerOptions{}).Run(context.Background())
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestRunWaitsForAServerThatIsStarting(t *testing.T) {
	f, c := newFake(t)
	var calls atomic.Int32
	f.set(func(f *fakeServer) {
		f.getQueueFn = func() (*tasklanev1.GetQueueResponse, error) {
			if calls.Add(1) <= 2 {
				return nil, status.Error(codes.Unavailable, "connection refused")
			}
			return &tasklanev1.GetQueueResponse{Queue: f.queue}, nil
		}
	})
	w := testWorker(c, WorkerOptions{})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 1 })
}

func TestRunCanOnlyBeCalledOnce(t *testing.T) {
	_, c := newFake(t)
	w := testWorker(c, WorkerOptions{})
	startWorker(t, w)

	require.Eventually(t, func() bool { return w.started.Load() }, time.Second, time.Millisecond)
	require.ErrorContains(t, w.Run(context.Background()), "more than once")
}

func TestAckIsRetriedWhileTheServerIsUnavailable(t *testing.T) {
	f, c := newFake(t)
	var calls atomic.Int32
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error {
			if calls.Add(1) <= 2 {
				return status.Error(codes.Unavailable, "connection refused")
			}
			return nil
		}
	})
	w := testWorker(c, WorkerOptions{})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) == 3 })
	require.Never(t, func() bool { return len(f.calls().acks) > 3 }, 50*time.Millisecond, 5*time.Millisecond)
}

func TestAckGivesUpWhenTheLeaseHasExpired(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.ackFn = func(*tasklanev1.AckRequest) error { return status.Error(codes.Unavailable, "connection refused") }
	})
	w := testWorker(c, WorkerOptions{VisibilityTimeout: 50 * time.Millisecond})
	w.Handle("ok", succeed)
	startWorker(t, w)

	f.push(fakeJob(1, "ok"))
	waitFor(t, func() bool { return len(f.calls().acks) >= 1 })
	time.Sleep(200 * time.Millisecond) // well past the 50 ms lease
	n := len(f.calls().acks)
	require.Never(t, func() bool { return len(f.calls().acks) > n }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestARejectedNackIsNotRetried(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.nackFn = func(*tasklanev1.NackRequest) error { return status.Error(codes.FailedPrecondition, "lease lost") }
	})
	w := testWorker(c, WorkerOptions{})
	w.Handle("bad", func(context.Context, *LeasedJob) error { return errors.New("boom") })
	startWorker(t, w)

	f.push(fakeJob(1, "bad"))
	waitFor(t, func() bool { return len(f.calls().nacks) == 1 })
	require.Never(t, func() bool { return len(f.calls().nacks) > 1 }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestExtraJobsFromTheServerAreIgnored(t *testing.T) {
	f, c := newFake(t)
	var sent atomic.Bool
	var running, peak atomic.Int32
	f.set(func(f *fakeServer) {
		f.leaseFn = func(ctx context.Context, req *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error) {
			if sent.CompareAndSwap(false, true) {
				return &tasklanev1.LeaseResponse{Jobs: []*tasklanev1.Job{fakeJob(1, "w"), fakeJob(2, "w"), fakeJob(3, "w")}}, nil
			}
			select {
			case <-ctx.Done():
			case <-time.After(20 * time.Millisecond):
			}
			return &tasklanev1.LeaseResponse{}, nil
		}
	})
	w := testWorker(c, WorkerOptions{Concurrency: 1})
	w.Handle("w", func(context.Context, *LeasedJob) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	stop := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { stop <- w.Run(ctx) }()

	waitFor(t, func() bool { return len(f.calls().acks) >= 1 })
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-stop:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
	require.Equal(t, int32(1), peak.Load())
	acks := f.calls().acks
	require.Len(t, acks, 1)
	require.Equal(t, int64(1), acks[0].GetId())
}
