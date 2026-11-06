package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mubtasim/tasklane/bench/internal/pace"
	"github.com/mubtasim/tasklane/bench/internal/stats"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// window is the measured interval; zero until the warm-up ends. Producers
// and workers read it on every job, so it lives behind an atomic pointer.
type window struct{ start, end time.Time }

func (w window) contains(t time.Time) bool {
	return !w.start.IsZero() && !t.Before(w.start) && t.Before(w.end)
}

// counts are a run's counters. sends counts enqueue calls inside the window
// (one per batch); errors and dropped too.
type counts struct {
	sends, errors, dropped, enqueued, acked int64
}

// load drives one run: producers enqueue, workers ack, and both record into
// the shared recorders only inside the window.
type load struct {
	cfg    Config
	client *tasklane.Client
	win    atomic.Pointer[window]

	sends, errors, dropped, enqueued, acked atomic.Int64
	e2s, enq                                *stats.Recorder
	firstErr                                sync.Once
}

func newLoad(cfg Config, client *tasklane.Client) *load {
	l := &load{cfg: cfg, client: client, e2s: stats.NewRecorder(), enq: stats.NewRecorder()}
	l.win.Store(&window{})
	return l
}

func (l *load) counts() counts {
	return counts{l.sends.Load(), l.errors.Load(), l.dropped.Load(), l.enqueued.Load(), l.acked.Load()}
}

// startWorkers runs the worker slots until ctx is done; wait returns once
// every worker has shut down.
func (l *load) startWorkers(ctx context.Context) (wait func()) {
	var wg sync.WaitGroup
	for _, n := range workerSplit(l.cfg.Workers) {
		w := l.client.NewWorker(l.cfg.Queue, tasklane.WorkerOptions{
			Concurrency:     n,
			ShutdownTimeout: 2 * time.Second,
			Logger:          slog.New(slog.DiscardHandler),
			OnResult: func(r tasklane.Result) {
				if r.Outcome == tasklane.OutcomeAcked && l.win.Load().contains(r.End) {
					l.acked.Add(1)
				}
			},
		})
		w.Handle("bench", func(_ context.Context, j *tasklane.LeasedJob) error {
			now := time.Now()
			if at, ok := scheduledTime(j.Payload); ok && l.win.Load().contains(at) {
				l.e2s.Record(now.Sub(at))
			}
			return nil
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("worker stopped", "err", err)
			}
		}()
	}
	return wg.Wait
}

// send enqueues one batch scheduled at planned and records the result.
func (l *load) send(ctx context.Context, planned time.Time) {
	jobs := make([]tasklane.Job, l.cfg.Batch)
	for i := range jobs {
		jobs[i] = tasklane.Job{Kind: "bench", Payload: newPayload(planned, l.cfg.Payload)}
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var err error
	if len(jobs) == 1 {
		_, err = l.client.Enqueue(cctx, l.cfg.Queue, jobs[0])
	} else {
		_, err = l.client.EnqueueBatch(cctx, l.cfg.Queue, jobs)
	}
	if !l.win.Load().contains(planned) {
		return
	}
	l.sends.Add(1)
	if err != nil {
		if ctx.Err() == nil { // a send cut off by the run's end is not a failure
			l.errors.Add(1)
			l.firstErr.Do(func() { slog.Warn("enqueue failed (first error)", "err", err) })
		}
		return
	}
	l.enq.Record(time.Since(planned))
	l.enqueued.Add(int64(len(jobs)))
}

// runClosed runs cfg.Producers loops, each sending as soon as the previous
// call returns, until ctx is done. Latency from the actual send time is all
// a closed loop can measure; the report uses closed runs for throughput only.
func (l *load) runClosed(ctx context.Context) {
	var wg sync.WaitGroup
	for range l.cfg.Producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				l.send(ctx, time.Now())
			}
		}()
	}
	wg.Wait()
}

// runOpen sends batches at cfg.Rate jobs per second until ctx is done.
func (l *load) runOpen(ctx context.Context) {
	batchesPerSec := l.cfg.Rate / float64(l.cfg.Batch)
	// Enough in flight for 5 s of sends: a server slower than that has
	// already failed the run.
	maxInFlight := max(int(batchesPerSec*5), 100)
	_, dropped := pace.Run(ctx, time.Now(), batchesPerSec, maxInFlight, func(planned time.Time) { l.send(ctx, planned) })
	l.dropped.Add(dropped)
}

// fill enqueues cfg.Jobs jobs in batches as fast as cfg.Producers
// goroutines can, round-robin over cfg.Queues queues, and returns the rate.
func fill(ctx context.Context, cfg Config, client *tasklane.Client) (float64, error) {
	var next atomic.Int64
	var firstErr error
	var mu sync.Mutex
	start := time.Now()
	var wg sync.WaitGroup
	for range max(cfg.Producers, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				from := next.Add(int64(cfg.Batch)) - int64(cfg.Batch)
				if from >= int64(cfg.Jobs) || ctx.Err() != nil {
					return
				}
				n := min(int64(cfg.Batch), int64(cfg.Jobs)-from)
				jobs := make([]tasklane.Job, n)
				for i := range jobs {
					j := tasklane.Job{Kind: "bench", Payload: newPayload(time.Now(), cfg.Payload), Priority: tasklane.Priority(cfg.Priority)}
					if cfg.Delay > 0 {
						j.RunAt = time.Now().Add(cfg.Delay)
					}
					jobs[i] = j
				}
				queue := queueName(cfg, from/int64(cfg.Batch))
				if _, err := client.EnqueueBatch(ctx, queue, jobs); err != nil {
					mu.Lock()
					firstErr = cmp.Or(firstErr, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return 0, fmt.Errorf("fill: %w", firstErr)
	}
	return float64(cfg.Jobs) / time.Since(start).Seconds(), nil
}

// queueName is the queue for batch i: cfg.Queue, or with cfg.Queues > 1,
// cfg.Queue-0000 … round-robin.
func queueName(cfg Config, i int64) string {
	if cfg.Queues == 1 {
		return cfg.Queue
	}
	return fmt.Sprintf("%s-%04d", cfg.Queue, i%int64(cfg.Queues))
}
