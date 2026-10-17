package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/store"
)

// insertJob inserts nothing when the queue doesn't exist (the SELECT is empty)
// or when the idempotency key is taken (ON CONFLICT). Concurrent inserts with
// the same key wait on the unique index, so exactly one of them creates the job.
const insertJob = `
INSERT INTO jobs (queue, kind, payload, priority, run_at, max_attempts, idempotency_key, trace_context)
SELECT q.name, $2::text, $3::jsonb, $4::smallint, coalesce($5::timestamptz, now()),
       coalesce(nullif($6::int, 0), q.max_attempts), nullif($7::text, ''), nullif($8::text, '')
FROM queues q WHERE q.name = $1
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id`

func (s *Store) Enqueue(ctx context.Context, jobs []store.NewJob) ([]store.EnqueueResult, error) {
	for _, j := range jobs {
		if err := validateJob(j); err != nil {
			return nil, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	results := make([]store.EnqueueResult, 0, len(jobs))
	for _, j := range jobs {
		r, err := enqueueOne(ctx, tx, j)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, tx.Commit(ctx)
}

func validateJob(j store.NewJob) error {
	switch {
	case j.Queue == "":
		return fmt.Errorf("%w: queue is required", store.ErrInvalid)
	case j.Kind == "":
		return fmt.Errorf("%w: kind is required", store.ErrInvalid)
	case j.Priority < store.PriorityUnspecified || j.Priority > store.PriorityLow:
		return fmt.Errorf("%w: priority must be 0-4, got %d", store.ErrInvalid, j.Priority)
	case j.MaxAttempts < 0:
		return fmt.Errorf("%w: max attempts must not be negative", store.ErrInvalid)
	case len(j.Payload) > 0 && !json.Valid(j.Payload):
		return fmt.Errorf("%w: payload is not valid JSON", store.ErrInvalid)
	}
	return nil
}

func enqueueOne(ctx context.Context, tx pgx.Tx, j store.NewJob) (store.EnqueueResult, error) {
	payload := j.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	var runAt *time.Time
	if !j.RunAt.IsZero() {
		runAt = &j.RunAt
	}

	priority := j.Priority
	if priority == store.PriorityUnspecified {
		priority = store.PriorityDefault
	}

	var id int64
	err := tx.QueryRow(ctx, insertJob, j.Queue, j.Kind, payload, int(priority), runAt,
		j.MaxAttempts, j.IdempotencyKey, j.TraceContext).Scan(&id)
	if err == nil {
		return store.EnqueueResult{ID: id, Created: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.EnqueueResult{}, err
	}
	if j.IdempotencyKey != "" {
		err = tx.QueryRow(ctx, "SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2",
			j.Queue, j.IdempotencyKey).Scan(&id)
		if err == nil {
			return store.EnqueueResult{ID: id, Created: false}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return store.EnqueueResult{}, err
		}
	}
	return store.EnqueueResult{}, fmt.Errorf("%w: queue %q", store.ErrNotFound, j.Queue)
}
