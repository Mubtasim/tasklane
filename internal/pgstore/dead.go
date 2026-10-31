package pgstore

import (
	"context"
	"fmt"

	"github.com/mubtasim/tasklane/internal/store"
)

// ListDeadJobs pages with a keyset, not OFFSET: (finished_at, id) < cursor
// is a range scan on jobs_dead (queue, finished_at, id), so a deep page costs
// what the first one does. The first page has no cursor condition at all
// (rather than "$3 IS NULL OR …"), so the planner never has to guess.
func (s *Store) ListDeadJobs(ctx context.Context, queue string, after *store.DeadCursor, limit int) ([]store.Job, error) {
	if limit < 1 {
		return nil, fmt.Errorf("%w: limit must be positive, got %d", store.ErrInvalid, limit)
	}
	sql := `SELECT ` + jobColumns + ` FROM jobs j
		WHERE j.queue = $1 AND j.state = 'dead'%s
		ORDER BY j.finished_at DESC, j.id DESC
		LIMIT $2`
	args := []any{queue, limit}
	if after != nil {
		sql = fmt.Sprintf(sql, ` AND (j.finished_at, j.id) < ($3, $4)`)
		args = append(args, after.FinishedAt, after.ID)
	} else {
		sql = fmt.Sprintf(sql, "")
	}
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []store.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		if err := queueExists(ctx, s.pool, queue); err != nil {
			return nil, err
		}
	}
	return jobs, nil
}
