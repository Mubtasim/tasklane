// Package pruner runs the background loop that deletes old finished jobs:
// completed and cancelled ones after the retention period, dead ones after
// the (longer) dead retention. Every server runs it; the store's SKIP LOCKED
// makes that safe.
package pruner

import (
	"context"
	"log/slog"
	"time"

	"github.com/mubtasim/tasklane/internal/metrics"
)

const (
	DefaultInterval      = time.Minute
	DefaultRetention     = 24 * time.Hour
	DefaultDeadRetention = 7 * 24 * time.Hour
	DefaultBatch         = 1000
)

// Store is the part of store.Store the pruner needs.
type Store interface {
	PruneFinished(ctx context.Context, age time.Duration, limit int) (int, error)
	PruneDead(ctx context.Context, age time.Duration, limit int) (int, error)
}

// Config sets the loop's period, both retention periods and the batch size.
// Non-positive values take the defaults.
type Config struct {
	Interval      time.Duration
	Retention     time.Duration
	DeadRetention time.Duration
	Batch         int
	Metrics       *metrics.Metrics // nil records nothing
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.Retention <= 0 {
		c.Retention = DefaultRetention
	}
	if c.DeadRetention <= 0 {
		c.DeadRetention = DefaultDeadRetention
	}
	if c.Batch <= 0 {
		c.Batch = DefaultBatch
	}
	return c
}

// Run prunes once immediately and then every Interval until ctx is
// cancelled. Errors are logged, never fatal: the next tick tries again.
func Run(ctx context.Context, s Store, cfg Config, log *slog.Logger) {
	cfg = cfg.withDefaults()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		pass(ctx, s, cfg, log)
		if ctx.Err() != nil {
			return // select picks randomly when a tick is also ready
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pass deletes batch after batch of each kind until a batch comes back
// short, so a large backlog clears in one pass without one large
// transaction.
func pass(ctx context.Context, s Store, cfg Config, log *slog.Logger) {
	drain(ctx, "completed and cancelled", metrics.PrunedFinished, s.PruneFinished, cfg.Retention, cfg.Batch, log, cfg.Metrics)
	drain(ctx, "dead", metrics.PrunedDead, s.PruneDead, cfg.DeadRetention, cfg.Batch, log, cfg.Metrics)
}

func drain(ctx context.Context, what, state string, prune func(context.Context, time.Duration, int) (int, error), age time.Duration, batch int, log *slog.Logger, m *metrics.Metrics) {
	total := 0
	for ctx.Err() == nil {
		n, err := prune(ctx, age, batch)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("prune failed", "jobs", what, "err", err)
			}
			break
		}
		m.Pruned(state, n)
		total += n
		if n < batch {
			break
		}
	}
	if total > 0 {
		log.Info("pruned jobs", "jobs", what, "count", total)
	}
}
