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

// RedriveDead takes its batch FOR UPDATE SKIP LOCKED, so two operators
// redriving the same queue at once split the jobs instead of waiting on each
// other. A nil ids slice is sent as NULL, which the "IS NULL OR" turns into
// "any dead job of the queue". The NOTIFY is part of the transaction, so
// waiting leases hear it only once the jobs are visible.
func (s *Store) RedriveDead(ctx context.Context, queue string, ids []int64, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("%w: limit must be positive, got %d", store.ErrInvalid, limit)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		WITH d AS (
			SELECT id FROM jobs
			WHERE queue = $1 AND state = 'dead' AND ($2::bigint[] IS NULL OR id = ANY($2))
			ORDER BY id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET state = 'available', attempts = 0, run_at = now(), finished_at = NULL
		FROM d WHERE j.id = d.id`, queue, ids, limit)
	if err != nil {
		return 0, err
	}
	n := int(tag.RowsAffected())
	if n == 0 {
		return 0, queueExists(ctx, tx, queue)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", NotifyChannel, queue); err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}
