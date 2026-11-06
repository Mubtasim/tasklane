package pgstore

import (
	"context"
	"time"

	"github.com/mubtasim/tasklane/internal/store"
)

// queueDepthsSQL counts every queue in one grouped pass over the available and
// leased jobs. A correlated count per queue made Postgres scan the whole table
// once per queue, which at 1,000 queues took 50.8 s per call. The oldest-ready
// lookup stays per queue: it is four jobs_available probes, cheap at any queue
// count.
const queueDepthsSQL = `
	WITH counts AS (
		SELECT queue,
			count(*) FILTER (WHERE state = 'available') AS available,
			count(*) FILTER (WHERE state = 'leased')    AS leased
		FROM jobs
		WHERE state IN ('available', 'leased')
		GROUP BY queue
	)
	SELECT q.name, q.max_depth, coalesce(c.available, 0), coalesce(c.leased, 0),
		CASE WHEN q.paused THEN 0 ELSE coalesce((
			SELECT extract(epoch FROM now() - min(o.run_at))::float8
			FROM unnest('{1,2,3,4}'::smallint[]) AS p(priority)
			CROSS JOIN LATERAL (
				SELECT j.run_at FROM jobs j
				WHERE j.queue = q.name AND j.state = 'available' AND j.priority = p.priority AND j.run_at <= now()
				ORDER BY j.run_at LIMIT 1) o), 0) END
	FROM queues q LEFT JOIN counts c ON c.queue = q.name`

// QueueDepths reports each queue's available and leased counts and the age of
// its oldest ready job. The cost is one pass over the available and leased
// jobs per call, O(backlog), not O(queues x table). Within (queue, priority)
// jobs_available is ordered by run_at, so the first entry is the oldest, and
// it is ready if its run_at has passed. A queue with no jobs still gets a row,
// with zeros.
func (s *Store) QueueDepths(ctx context.Context) ([]store.QueueDepth, error) {
	rows, err := s.pool.Query(ctx, queueDepthsSQL)
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
