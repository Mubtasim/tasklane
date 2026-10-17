// Package pgstore implements store.Store on PostgreSQL. All time comparisons
// use the database clock (now()), never the server's.
package pgstore

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/store"
)

type Store struct {
	pool    *pgxpool.Pool
	backoff backoff.Policy
}

var _ store.Store = (*Store)(nil)

func New(pool *pgxpool.Pool, policy backoff.Policy) *Store {
	return &Store{pool: pool, backoff: policy}
}

// jobColumns is the select list for scanJob; queries alias jobs as j.
const jobColumns = `j.id, j.queue, j.kind, j.payload, j.priority, j.state, j.run_at,
	j.attempts, j.max_attempts, j.last_error, j.lease_token, j.lease_expires_at,
	j.idempotency_key, j.trace_context, j.created_at, j.finished_at`

func scanJob(row pgx.Row) (store.Job, error) {
	var (
		j                         store.Job
		state                     string
		lastErr, idemKey, traceCx *string
		token                     pgtype.UUID
		leaseExp, finished        pgtype.Timestamptz
	)
	err := row.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Priority, &state, &j.RunAt,
		&j.Attempts, &j.MaxAttempts, &lastErr, &token, &leaseExp,
		&idemKey, &traceCx, &j.CreatedAt, &finished)
	if err != nil {
		return store.Job{}, err
	}
	j.State = store.JobState(state)
	if lastErr != nil {
		j.LastError = *lastErr
	}
	if idemKey != nil {
		j.IdempotencyKey = *idemKey
	}
	if traceCx != nil {
		j.TraceContext = *traceCx
	}
	if token.Valid {
		j.LeaseToken = uuid.UUID(token.Bytes)
	}
	if leaseExp.Valid {
		j.LeaseExpiresAt = leaseExp.Time
	}
	if finished.Valid {
		j.FinishedAt = finished.Time
	}
	return j, nil
}

// ttlMillis converts a lease duration for SQL; nil means "use the queue's
// visibility timeout".
func ttlMillis(ttl time.Duration) *int64 {
	if ttl <= 0 {
		return nil
	}
	ms := max(ttl.Milliseconds(), 1)
	return &ms
}

// missingOrLost explains why a token-guarded update matched no row.
func (s *Store) missingOrLost(ctx context.Context, id int64) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)", id).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return store.ErrNotFound
	}
	return store.ErrLeaseLost
}
