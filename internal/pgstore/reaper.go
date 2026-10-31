package pgstore

import (
	"context"
	"fmt"
	"strings"

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
		`SELECT id, attempts, max_attempts, coalesce(last_error, '') FROM jobs
		 WHERE state = 'leased' AND lease_expires_at < now()
		 ORDER BY lease_expires_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id                    int64
		lastErr               string
		attempts, maxAttempts int
	}
	var batch []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.attempts, &e.maxAttempts, &e.lastErr); err != nil {
			return 0, err
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, e := range batch {
		if err := s.retryOrBury(ctx, tx, e.id, e.attempts, e.maxAttempts, leaseExpiredError(e.lastErr), nil); err != nil {
			return 0, err
		}
	}
	return len(batch), tx.Commit(ctx)
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
