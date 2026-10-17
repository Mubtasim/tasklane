package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mubtasim/tasklane/internal/store"
)

const uniqueViolation = "23505"

func (s *Store) CreateQueue(ctx context.Context, q store.Queue) error {
	if q.Name == "" {
		return fmt.Errorf("%w: queue name is required", store.ErrInvalid)
	}
	q = q.WithDefaults()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO queues (name, max_depth, visibility_timeout_ms, max_attempts, paused)
		 VALUES ($1, $2, $3, $4, $5)`,
		q.Name, q.MaxDepth, q.VisibilityTimeout.Milliseconds(), q.MaxAttempts, q.Paused)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return fmt.Errorf("%w: queue %q", store.ErrAlreadyExists, q.Name)
	}
	return err
}

func (s *Store) GetQueue(ctx context.Context, name string) (store.Queue, error) {
	q := store.Queue{Name: name}
	var vtMillis int64
	err := s.pool.QueryRow(ctx,
		`SELECT max_depth, visibility_timeout_ms, max_attempts, paused FROM queues WHERE name = $1`, name,
	).Scan(&q.MaxDepth, &vtMillis, &q.MaxAttempts, &q.Paused)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Queue{}, fmt.Errorf("%w: queue %q", store.ErrNotFound, name)
	}
	q.VisibilityTimeout = time.Duration(vtMillis) * time.Millisecond
	return q, err
}
