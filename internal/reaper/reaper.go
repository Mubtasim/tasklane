// Package reaper runs the background loop that returns expired leases to the
// queue. Every server runs it; the store's SKIP LOCKED makes that safe.
package reaper

import (
	"context"
	"log/slog"
	"time"
)

const (
	DefaultInterval = 5 * time.Second
	DefaultBatch    = 500
)

// Store is the part of store.Store the reaper needs.
type Store interface {
	ReapExpired(ctx context.Context, limit int) (int, error)
}

// Run reaps once immediately and then every interval until ctx is cancelled.
// While a call returns a full batch, it calls again at once instead of waiting
// for the next tick, so a backlog drains quickly. Errors are logged, never
// fatal: the next tick tries again.
func Run(ctx context.Context, s Store, interval time.Duration, batch int, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		reapAll(ctx, s, batch, log)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func reapAll(ctx context.Context, s Store, batch int, log *slog.Logger) {
	for ctx.Err() == nil {
		n, err := s.ReapExpired(ctx, batch)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("reap expired leases failed", "err", err)
			}
			return
		}
		if n > 0 {
			log.Info("reaped expired leases", "count", n)
		}
		if n < batch {
			return
		}
	}
}
