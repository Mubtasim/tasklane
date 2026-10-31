package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/mubtasim/tasklane/internal/store"
)

// Each statement deletes one batch. The inner SELECT takes its rows FOR
// UPDATE SKIP LOCKED, so servers pruning at the same time split the work, and
// its WHERE matches a partial index exactly: jobs_finished for the first,
// jobs_dead for the second (which has no queue to fix the index prefix, so it
// scans that small index across queues).
const (
	pruneFinishedSQL = `DELETE FROM jobs WHERE id IN (
		SELECT id FROM jobs
		WHERE state IN ('completed', 'cancelled') AND finished_at < now() - interval '1 millisecond' * $1::bigint
		LIMIT $2 FOR UPDATE SKIP LOCKED)`
	pruneDeadSQL = `DELETE FROM jobs WHERE id IN (
		SELECT id FROM jobs
		WHERE state = 'dead' AND finished_at < now() - interval '1 millisecond' * $1::bigint
		LIMIT $2 FOR UPDATE SKIP LOCKED)`
)

func (s *Store) PruneFinished(ctx context.Context, age time.Duration, limit int) (int, error) {
	return s.prune(ctx, pruneFinishedSQL, age, limit)
}

func (s *Store) PruneDead(ctx context.Context, age time.Duration, limit int) (int, error) {
	return s.prune(ctx, pruneDeadSQL, age, limit)
}

func (s *Store) prune(ctx context.Context, sql string, age time.Duration, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("%w: prune limit must be positive", store.ErrInvalid)
	}
	tag, err := s.pool.Exec(ctx, sql, age.Milliseconds(), limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
