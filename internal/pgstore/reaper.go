package pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/mubtasim/tasklane/internal/store"
)

// ReapExpired treats expired leases as failed attempts. SKIP LOCKED lets every
// server run the reaper at the same time without processing a job twice.
func (s *Store) ReapExpired(ctx context.Context, limit int) ([]store.ReapedJob, error) {
	if limit < 1 {
		return nil, fmt.Errorf("%w: reap limit must be positive", store.ErrInvalid)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, queue, attempts, max_attempts, coalesce(last_error, '') FROM jobs
		 WHERE state = 'leased' AND lease_expires_at < now()
		 ORDER BY lease_expires_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	type expired struct {
		id                    int64
		queue                 string
		lastErr               string
		attempts, maxAttempts int
	}
	var batch []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.queue, &e.attempts, &e.maxAttempts, &e.lastErr); err != nil {
			return nil, err
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var reaped []store.ReapedJob
	for _, e := range batch {
		dead, err := s.retryOrBury(ctx, tx, e.id, e.attempts, e.maxAttempts, leaseExpiredError(e.lastErr), nil)
		if err != nil {
			return nil, err
		}
		reaped = append(reaped, store.ReapedJob{Queue: e.queue, Dead: dead})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return reaped, nil
}

// leaseExpiredError is the reaper's last_error. It keeps the previous
// attempt's error, which is usually the real reason the job is struggling:
// a worker that crashes after a failed call would otherwise erase it. A
// message that already says "lease expired" is kept as it is, so repeated
// expiries don't nest.
func leaseExpiredError(prev string) string {
	switch {
	case prev == "":
		return "lease expired"
	case strings.HasPrefix(prev, "lease expired"):
		return prev
	}
	return store.TruncateUTF8("lease expired (previous: "+prev+")", store.MaxErrorBytes)
}
