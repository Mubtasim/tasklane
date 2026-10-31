package pgstore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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

// MaxEnqueueBatch is the largest batch Enqueue accepts.
const MaxEnqueueBatch = 1000

// NotifyChannel is the Postgres channel that wakes long-polling leases. The
// payload is the queue name.
const NotifyChannel = "tasklane_jobs"

func (s *Store) Enqueue(ctx context.Context, jobs []store.NewJob) ([]store.EnqueueResult, error) {
	if len(jobs) == 0 || len(jobs) > MaxEnqueueBatch {
		return nil, fmt.Errorf("%w: batch must have 1-%d jobs, got %d", store.ErrInvalid, MaxEnqueueBatch, len(jobs))
	}
	for _, j := range jobs {
		if err := validateJob(j); err != nil {
			return nil, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	results := make([]store.EnqueueResult, len(jobs))
	for _, i := range insertOrder(jobs) {
		r, err := enqueueOne(ctx, tx, jobs[i])
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	// NOTIFY is transactional: listeners hear it only if this commit succeeds,
	// and never before the jobs are visible.
	for _, q := range distinctQueues(jobs) {
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", NotifyChannel, q); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

// insertOrder returns the indexes of jobs in the order to insert them: jobs
// with an idempotency key first, sorted by (queue, key), then the rest in the
// caller's order. An insert with a key locks that key's unique-index entry
// until commit, so two batches that take the same keys in different orders
// could each wait for the other (deadlock, SQLSTATE 40P01). A single global
// order makes that impossible.
func insertOrder(jobs []store.NewJob) []int {
	order := make([]int, len(jobs))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		ja, jb := jobs[a], jobs[b]
		keyedA, keyedB := ja.IdempotencyKey != "", jb.IdempotencyKey != ""
		switch {
		case keyedA && keyedB:
			return cmp.Or(strings.Compare(ja.Queue, jb.Queue), strings.Compare(ja.IdempotencyKey, jb.IdempotencyKey))
		case keyedA:
			return -1
		case keyedB:
			return 1
		}
		return 0
	})
	return order
}

func distinctQueues(jobs []store.NewJob) []string {
	queues := make([]string, 0, 1)
	for _, j := range jobs {
		if !slices.Contains(queues, j.Queue) {
			queues = append(queues, j.Queue)
		}
	}
	return queues
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
