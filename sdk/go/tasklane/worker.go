package tasklane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/backoff"
)

const (
	// defaultLeaseWait is the long-poll wait of each Lease (the server caps
	// waits at 30 s).
	defaultLeaseWait = 20 * time.Second
	// leaseMargin is added to the wait for the Lease call's deadline, so a
	// slow network doesn't cut off a response that is on its way.
	leaseMargin            = 10 * time.Second
	defaultShutdownTimeout = 25 * time.Second
)

// WorkerOptions configures a Worker. Zero values take the defaults.
type WorkerOptions struct {
	// Concurrency is how many jobs run at once (default 1).
	Concurrency int
	// VisibilityTimeout is the lease length; zero uses the queue's setting.
	VisibilityTimeout time.Duration
	// ShutdownTimeout is how long running jobs may take to finish after
	// Run's context is cancelled (default 25 s); the Nacks and Acks after it
	// take up to 2 s more. Give the process at least that long to stop:
	// Docker's default grace period is only 10 s.
	ShutdownTimeout time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// OnResult, if set, is called once per leased job after the worker is
	// done with it, with the run's outcome. It runs on the job's goroutine,
	// so it must be safe for concurrent use and return quickly.
	OnResult func(Result)
}

// Worker leases jobs from one queue and runs the handler registered for each
// job's kind. Register handlers with Handle, then call Run once.
type Worker struct {
	client *Client
	queue  string
	opts   WorkerOptions
	log    *slog.Logger

	mu       sync.RWMutex
	handlers map[string]HandlerFunc
	started  atomic.Bool

	// Tests shorten these.
	leaseWait     time.Duration
	retry         backoff.Policy
	finishTimeout time.Duration
}

// NewWorker returns a worker for queue.
func (c *Client) NewWorker(queue string, opts WorkerOptions) *Worker {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = defaultShutdownTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Worker{
		client:        c,
		queue:         queue,
		opts:          opts,
		log:           opts.Logger.With("queue", queue),
		handlers:      map[string]HandlerFunc{},
		leaseWait:     defaultLeaseWait,
		finishTimeout: defaultFinishTimeout,
		// Full jitter, ceiling 1 s doubling to a 30 s cap: workers that lost
		// the server at the same moment don't come back in lockstep.
		retry: backoff.Policy{Base: time.Second, Cap: 30 * time.Second, Rand: rand.Float64},
	}
}

// Handle registers h for jobs of the given kind, replacing any earlier
// handler. A job whose kind has no handler is nacked with an error, so it
// reaches the dead-letter queue instead of looping.
func (w *Worker) Handle(kind string, h HandlerFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[kind] = h
}

func (w *Worker) handler(kind string) HandlerFunc {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.handlers[kind]
}

// run is the state of one Run call.
type run struct {
	vt    time.Duration
	slots chan struct{} // holds one value per running job; capacity = Concurrency
	jobs  sync.WaitGroup

	mu     sync.Mutex
	active map[*activeJob]struct{}
}

// Run leases and runs jobs until ctx is cancelled, then shuts down
// gracefully: it stops leasing, gives running jobs up to ShutdownTimeout to
// finish, nacks the ones that don't, and returns nil (within about
// ShutdownTimeout + 2 s). It returns an error only when retrying can't help:
// the queue doesn't exist or the API key is rejected. While the server is
// unreachable it keeps retrying with backoff.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("tasklane: Worker.Run called more than once")
	}
	vt, err := w.visibilityTimeout(ctx)
	if err != nil || ctx.Err() != nil {
		return err
	}
	r := &run{vt: vt, slots: make(chan struct{}, w.opts.Concurrency), active: map[*activeJob]struct{}{}}
	err = w.leaseLoop(ctx, r)
	w.drain(r)
	return err
}

// visibilityTimeout checks that the queue exists and returns the lease length
// to use. It retries while the server is unreachable (it may still be
// starting) and returns 0, nil if ctx is cancelled first.
func (w *Worker) visibilityTimeout(ctx context.Context) (time.Duration, error) {
	for attempt := 0; ; attempt++ {
		info, err := w.client.GetQueue(ctx, w.queue)
		if err == nil {
			if w.opts.VisibilityTimeout > 0 {
				return w.opts.VisibilityTimeout, nil
			}
			return info.Config.VisibilityTimeout, nil
		}
		if ctx.Err() != nil {
			return 0, nil
		}
		if fatal(err) {
			return 0, fmt.Errorf("tasklane: queue %q: %w", w.queue, err)
		}
		w.log.Warn("server unreachable; retrying", "err", err)
		if !sleepCtx(ctx, w.retry.Delay(attempt)) {
			return 0, nil
		}
	}
}

// leaseLoop leases jobs into free slots until ctx is cancelled. There is one
// loop, and so one long-poll, per worker whatever its Concurrency: each
// waiting lease costs the server a query per second, so a long-poll per
// goroutine would make idle load grow with total worker concurrency.
func (w *Worker) leaseLoop(ctx context.Context, r *run) error {
	for attempt := 0; ; {
		// Block until a slot is free, then take every other free slot.
		select {
		case <-ctx.Done():
			return nil
		case r.slots <- struct{}{}:
		}
		k := 1
	fill:
		for k < cap(r.slots) {
			select {
			case r.slots <- struct{}{}:
				k++
			default:
				break fill
			}
		}

		jobs, err := w.lease(ctx, k, r.vt)
		leasedAt := time.Now()
		// The server must not return more than k jobs. If it does, run only
		// k: the extras' leases expire and the reaper retries them.
		if len(jobs) > k {
			ids := make([]int64, 0, len(jobs)-k)
			for _, pj := range jobs[k:] {
				ids = append(ids, pj.GetId())
			}
			w.log.Warn("server returned more jobs than requested; ignoring the extras", "requested", k, "extras", len(ids), "job_ids", ids)
			jobs = jobs[:k]
		}
		// Give back the slots this lease didn't fill. Without this, one
		// empty long-poll would keep every slot and the worker would stop
		// taking jobs without any error.
		for range k - len(jobs) {
			<-r.slots
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if fatal(err) {
				return fmt.Errorf("tasklane: lease from %q: %w", w.queue, err)
			}
			w.log.Warn("lease failed; backing off", "err", err)
			if !sleepCtx(ctx, w.retry.Delay(attempt)) {
				return nil
			}
			attempt++
			continue
		}
		attempt = 0
		for _, pj := range jobs {
			r.jobs.Add(1)
			go func() {
				defer r.jobs.Done()
				w.runJob(r, pj, leasedAt)
			}()
		}
	}
}

func (w *Worker) lease(ctx context.Context, n int, vt time.Duration) ([]*tasklanev1.Job, error) {
	ctx, cancel := context.WithTimeout(ctx, w.leaseWait+leaseMargin)
	defer cancel()
	res, err := w.client.api.Lease(ctx, &tasklanev1.LeaseRequest{
		Queue:             w.queue,
		MaxJobs:           int32(n),
		Wait:              durationpb.New(w.leaseWait),
		VisibilityTimeout: durationpb.New(vt),
	})
	return res.GetJobs(), err
}

// fatal reports whether retrying err can't help: the queue or job doesn't
// exist, the API key is rejected or the request is invalid. Every other error
// (the server down, restarting or overloaded) is retried with backoff.
func fatal(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
		return true
	}
	return false
}

// sleepCtx waits for d and reports false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
