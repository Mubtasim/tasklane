package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// normal simulates real work: 0.2–2 s, then success. It stops early when its
// context is cancelled (lease lost or shutdown deadline).
func normal(log *slog.Logger) tasklane.HandlerFunc {
	return func(ctx context.Context, job *tasklane.LeasedJob) error {
		if err := work(ctx, 200*time.Millisecond, 2*time.Second); err != nil {
			return err
		}
		log.Info("job done", "job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)
		return nil
	}
}

// flaky fails about failRate of its runs (roll returns a value in [0, 1)),
// so the dashboard shows retries with backoff that eventually succeed.
func flaky(log *slog.Logger, failRate float64, roll func() float64) tasklane.HandlerFunc {
	return func(ctx context.Context, job *tasklane.LeasedJob) error {
		if err := work(ctx, 100*time.Millisecond, 500*time.Millisecond); err != nil {
			return err
		}
		if roll() < failRate {
			log.Warn("job failed (simulated)", "job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)
			return errors.New("simulated flaky failure")
		}
		log.Info("job done", "job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)
		return nil
	}
}

// poison always fails, so the job uses up its attempts and becomes dead.
func poison(log *slog.Logger) tasklane.HandlerFunc {
	return func(_ context.Context, job *tasklane.LeasedJob) error {
		log.Warn("job failed (always fails)", "job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt, "max_attempts", job.MaxAttempts)
		return errors.New("simulated poison job: always fails")
	}
}

// work sleeps a random time in [lo, hi) or until ctx is cancelled.
func work(ctx context.Context, lo, hi time.Duration) error {
	t := time.NewTimer(lo + rand.N(hi-lo))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
