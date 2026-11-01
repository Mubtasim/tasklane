package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/mubtasim/tasklane/internal/store"
)

// Every statement here matches on the lease token, so only the worker that
// currently holds the job can change it (fencing).

// Ack keeps the token on the completed row, so a retried Ack (the first
// response was lost) can be recognised and answered with success.
func (s *Store) Ack(ctx context.Context, id int64, token uuid.UUID) (store.AckResult, error) {
	var queue string
	err := s.pool.QueryRow(ctx,
		`UPDATE jobs SET state = 'completed', finished_at = now(), lease_expires_at = NULL
		 WHERE id = $1 AND state = 'leased' AND lease_token = $2
		 RETURNING queue`, id, token).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ackOutcome(ctx, s.pool, id, token)
	}
	if err != nil {
		return store.AckResult{}, err
	}
	return store.AckResult{Queue: queue}, nil
}

// ackOutcome explains why an Ack matched no row: the same token already
// completed the job (success, Repeated), the job is missing, or the token is
// stale. Like missingOrLost, it runs on the caller's querier.
func ackOutcome(ctx context.Context, q querier, id int64, token uuid.UUID) (store.AckResult, error) {
	var (
		queue, state string
		held         pgtype.UUID
	)
	err := q.QueryRow(ctx, "SELECT queue, state::text, lease_token FROM jobs WHERE id = $1", id).Scan(&queue, &state, &held)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AckResult{}, store.ErrNotFound
	}
	if err != nil {
		return store.AckResult{}, err
	}
	if store.JobState(state) == store.StateCompleted && held.Valid && uuid.UUID(held.Bytes) == token {
		return store.AckResult{Queue: queue, Repeated: true}, nil
	}
	return store.AckResult{}, store.ErrLeaseLost
}

func (s *Store) Heartbeat(ctx context.Context, id int64, token uuid.UUID, ttl time.Duration) (time.Time, error) {
	var expires time.Time
	err := s.pool.QueryRow(ctx,
		`UPDATE jobs SET lease_expires_at = now() + interval '1 millisecond' *
		     coalesce($3::bigint, (SELECT visibility_timeout_ms FROM queues q WHERE q.name = jobs.queue))
		 WHERE id = $1 AND state = 'leased' AND lease_token = $2
		 RETURNING lease_expires_at`, id, token, ttlMillis(ttl)).Scan(&expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, missingOrLost(ctx, s.pool, id)
	}
	return expires, err
}

func (s *Store) Nack(ctx context.Context, id int64, token uuid.UUID, errMsg string, retryAt *time.Time) (store.NackResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.NackResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		queue                 string
		attempts, maxAttempts int
	)
	err = tx.QueryRow(ctx,
		`SELECT queue, attempts, max_attempts FROM jobs
		 WHERE id = $1 AND state = 'leased' AND lease_token = $2 FOR UPDATE`, id, token,
	).Scan(&queue, &attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.NackResult{}, missingOrLost(ctx, tx, id)
	}
	if err != nil {
		return store.NackResult{}, err
	}
	dead, err := s.retryOrBury(ctx, tx, id, attempts, maxAttempts, errMsg, retryAt)
	if err != nil {
		return store.NackResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.NackResult{}, err
	}
	return store.NackResult{Queue: queue, Dead: dead}, nil
}

// retryOrBury handles a failed attempt on a job whose row the caller has
// locked: back to available after a delay, or to dead (the DLQ) once all
// attempts are used (dead is true). The backoff delay is computed here but
// applied with the database clock. Nack and the reaper both use it, so their
// rules can't drift.
func (s *Store) retryOrBury(ctx context.Context, tx pgx.Tx, id int64, attempts, maxAttempts int, errMsg string, retryAt *time.Time) (dead bool, err error) {
	if attempts >= maxAttempts {
		_, err = tx.Exec(ctx,
			`UPDATE jobs SET state = 'dead', last_error = $2, finished_at = now(),
			     lease_token = NULL, lease_expires_at = NULL
			 WHERE id = $1`, id, errMsg)
		return true, err
	}
	_, err = tx.Exec(ctx,
		`UPDATE jobs SET state = 'available', last_error = $2,
		     run_at = coalesce($3::timestamptz, now() + interval '1 millisecond' * $4::bigint),
		     lease_token = NULL, lease_expires_at = NULL
		 WHERE id = $1`, id, errMsg, retryAt, s.backoff.Delay(attempts).Milliseconds())
	return false, err
}
