package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Every statement here matches on the lease token, so only the worker that
// currently holds the job can change it (fencing).

func (s *Store) Ack(ctx context.Context, id int64, token uuid.UUID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET state = 'completed', finished_at = now(), lease_token = NULL, lease_expires_at = NULL
		 WHERE id = $1 AND state = 'leased' AND lease_token = $2`, id, token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return missingOrLost(ctx, s.pool, id)
	}
	return nil
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

func (s *Store) Nack(ctx context.Context, id int64, token uuid.UUID, errMsg string, retryAt *time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var attempts, maxAttempts int
	err = tx.QueryRow(ctx,
		`SELECT attempts, max_attempts FROM jobs
		 WHERE id = $1 AND state = 'leased' AND lease_token = $2 FOR UPDATE`, id, token,
	).Scan(&attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return missingOrLost(ctx, tx, id)
	}
	if err != nil {
		return err
	}
	if err := s.retryOrBury(ctx, tx, id, attempts, maxAttempts, errMsg, retryAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// retryOrBury handles a failed attempt on a job whose row the caller has
// locked: back to available after a delay, or to dead (the DLQ) once all
// attempts are used. The backoff delay is computed here but applied with the
// database clock. Nack and the reaper both use it, so their rules can't drift.
func (s *Store) retryOrBury(ctx context.Context, tx pgx.Tx, id int64, attempts, maxAttempts int, errMsg string, retryAt *time.Time) error {
	if attempts >= maxAttempts {
		_, err := tx.Exec(ctx,
			`UPDATE jobs SET state = 'dead', last_error = $2, finished_at = now(),
			     lease_token = NULL, lease_expires_at = NULL
			 WHERE id = $1`, id, errMsg)
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE jobs SET state = 'available', last_error = $2,
		     run_at = coalesce($3::timestamptz, now() + interval '1 millisecond' * $4::bigint),
		     lease_token = NULL, lease_expires_at = NULL
		 WHERE id = $1`, id, errMsg, retryAt, s.backoff.Delay(attempts).Milliseconds())
	return err
}
