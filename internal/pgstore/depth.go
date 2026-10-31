package pgstore

import (
	"context"
	"time"

	"github.com/mubtasim/tasklane/internal/store"
)

// QueueDepths counts per queue with correlated subqueries, like QueueStats:
// the available count is an index-only range on jobs_available, which starts
// with queue, and leased jobs are bounded by the workers' slots. The oldest
// ready job is one jobs_available lookup per priority: within (queue,
// priority) the index is ordered by run_at, so the first entry is the oldest,
// and it is ready if its run_at has passed. A queue with no jobs still gets a
// row, with zeros.
func (s *Store) QueueDepths(ctx context.Context) ([]store.QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT q.name, q.max_depth,
			(SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'available'),
			(SELECT count(*) FROM jobs j WHERE j.queue = q.name AND j.state = 'leased'),
			CASE WHEN q.paused THEN 0 ELSE coalesce((
				SELECT extract(epoch FROM now() - min(o.run_at))::float8
				FROM unnest('{1,2,3,4}'::smallint[]) AS p(priority)
				CROSS JOIN LATERAL (
					SELECT j.run_at FROM jobs j
					WHERE j.queue = q.name AND j.state = 'available' AND j.priority = p.priority AND j.run_at <= now()
					ORDER BY j.run_at LIMIT 1) o), 0) END
		FROM queues q`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.QueueDepth
	for rows.Next() {
		var (
			d    store.QueueDepth
			secs float64
		)
		if err := rows.Scan(&d.Queue, &d.MaxDepth, &d.Available, &d.Leased, &secs); err != nil {
			return nil, err
		}
		d.OldestReady = time.Duration(secs * float64(time.Second))
		out = append(out, d)
	}
	return out, rows.Err()
}
