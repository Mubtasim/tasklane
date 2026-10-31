package pgstore

import (
	"context"

	"github.com/mubtasim/tasklane/internal/store"
)

// QueueDepths counts per queue with correlated subqueries, like QueueStats:
// the available count is an index-only range on jobs_available, which starts
// with queue, and leased jobs are bounded by the workers' slots. A queue with
// no jobs still gets a row, with depth 0.
func (s *Store) QueueDepths(ctx context.Context) ([]store.QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT q.name, q.max_depth,
			(SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'available') +
			(SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'leased')
		FROM queues q`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.QueueDepth
	for rows.Next() {
		var d store.QueueDepth
		if err := rows.Scan(&d.Queue, &d.MaxDepth, &d.Depth); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
