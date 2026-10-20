package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/store"
)

// QueueStats counts in one statement. available and dead use the partial
// indexes that start with queue; leased reads the jobs_leased partial index,
// which has no queue column, but leased jobs are bounded by the workers'
// slots. Completed jobs are counted only over the last StatsWindow through
// the B-tree index on finished_at, which jumps to the start of the window, so
// that count costs what the recent throughput costs, not the table size.
func (s *Store) QueueStats(ctx context.Context, queue string) (store.QueueStats, error) {
	var st store.QueueStats
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'available'),
			(SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'leased'),
			(SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'dead'),
			(SELECT count(*) FROM jobs WHERE queue = $1 AND state = 'completed'
				AND finished_at > now() - interval '1 millisecond' * $2::bigint)
		FROM queues WHERE name = $1`,
		queue, store.StatsWindow.Milliseconds(),
	).Scan(&st.Available, &st.Leased, &st.Dead, &st.RecentlyCompleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.QueueStats{}, fmt.Errorf("%w: queue %q", store.ErrNotFound, queue)
	}
	return st, err
}
