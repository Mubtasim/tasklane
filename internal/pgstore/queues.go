package pgstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mubtasim/tasklane/internal/store"
)

const uniqueViolation = "23505"

func (s *Store) CreateQueue(ctx context.Context, q store.Queue) error {
	if err := validateQueue(q); err != nil {
		return err
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

// queueNameRe keeps queue names safe in URL paths such as /v1/queues/{queue}:lease.
var queueNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// validateQueue runs before defaults are applied: zero means "default", and
// the database CHECKs would reject the rest with an opaque error.
func validateQueue(q store.Queue) error {
	switch {
	case q.Name == "":
		return fmt.Errorf("%w: queue name is required", store.ErrInvalid)
	case !queueNameRe.MatchString(q.Name):
		return fmt.Errorf("%w: queue name must be 1-128 characters from [A-Za-z0-9._-]", store.ErrInvalid)
	case q.MaxDepth < 0:
		return fmt.Errorf("%w: max depth must not be negative", store.ErrInvalid)
	case q.MaxAttempts < 0:
		return fmt.Errorf("%w: max attempts must not be negative", store.ErrInvalid)
	case q.VisibilityTimeout < 0:
		return fmt.Errorf("%w: visibility timeout must not be negative", store.ErrInvalid)
	case q.VisibilityTimeout > 0 && q.VisibilityTimeout < time.Millisecond:
		return fmt.Errorf("%w: visibility timeout must be at least 1ms", store.ErrInvalid)
	}
	return nil
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
