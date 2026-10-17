package pgstore

import (
	"context"
	"fmt"

	"github.com/mubtasim/tasklane/internal/store"
)

// ReapExpired treats expired leases as failed attempts. SKIP LOCKED lets every
// server run the reaper at the same time without processing a job twice.
func (s *Store) ReapExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("%w: reap limit must be positive", store.ErrInvalid)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id, attempts, max_attempts FROM jobs
		 WHERE state = 'leased' AND lease_expires_at < now()
		 ORDER BY lease_expires_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id                    int64
		attempts, maxAttempts int
	}
	var batch []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.attempts, &e.maxAttempts); err != nil {
			return 0, err
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, e := range batch {
		if err := s.retryOrBury(ctx, tx, e.id, e.attempts, e.maxAttempts, "lease expired", nil); err != nil {
			return 0, err
		}
	}
	return len(batch), tx.Commit(ctx)
}
