package pgstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/store"
)

const maxLeaseBatch = 1000

// leaseJobs picks the next runnable jobs and leases them in one statement.
// SKIP LOCKED makes concurrent callers take different rows instead of waiting
// on, or double-leasing, the same ones. Each lease gets a fresh token (the
// fencing token) and attempts is incremented.
const leaseJobs = `
WITH next AS (
    SELECT j.id FROM jobs j
    JOIN queues q ON q.name = j.queue
    WHERE j.queue = $1 AND j.state = 'available' AND j.run_at <= now() AND NOT q.paused
    ORDER BY j.priority, j.run_at, j.id
    LIMIT $2
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs j
SET state = 'leased',
    attempts = j.attempts + 1,
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + interval '1 millisecond' *
        coalesce($3::bigint, (SELECT visibility_timeout_ms FROM queues WHERE name = $1))
FROM next
WHERE j.id = next.id
RETURNING ` + jobColumns + `, now()`

func (s *Store) Lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]store.Job, error) {
	if n < 1 || n > maxLeaseBatch {
		return nil, fmt.Errorf("%w: lease batch size must be 1-%d, got %d", store.ErrInvalid, maxLeaseBatch, n)
	}
	rows, err := s.pool.Query(ctx, leaseJobs, queue, n, ttlMillis(ttl))
	if err != nil {
		return nil, err
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Job, error) {
		var leasedAt time.Time
		j, err := scanJob(r, &leasedAt)
		j.LeasedAt = leasedAt
		return j, err
	})
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		// Only an empty result pays for this check, so the hot path is unchanged.
		var exists bool
		if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM queues WHERE name = $1)", queue).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: queue %q", store.ErrNotFound, queue)
		}
		return jobs, nil
	}
	// UPDATE … RETURNING doesn't preserve the CTE's order.
	slices.SortFunc(jobs, func(a, b store.Job) int {
		if a.Priority != b.Priority {
			return int(a.Priority - b.Priority)
		}
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return int(a.ID - b.ID)
	})
	return jobs, nil
}

func (s *Store) GetJob(ctx context.Context, id int64) (store.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs j WHERE j.id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Job{}, fmt.Errorf("%w: job %d", store.ErrNotFound, id)
	}
	return j, err
}
