// Package store defines Tasklane's storage contract. The service layer depends
// only on this package: pgstore implements it on PostgreSQL in v1, and a
// Raft-backed store will implement it in phase 2.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type JobState string

const (
	StateAvailable JobState = "available"
	StateLeased    JobState = "leased"
	StateCompleted JobState = "completed"
	StateDead      JobState = "dead"
	StateCancelled JobState = "cancelled"
)

// Priority orders leasing: lower values run first. The zero value means
// "unspecified", which Enqueue stores as PriorityDefault (the protobuf enum
// convention, so an unset field never silently becomes critical).
type Priority int

const (
	PriorityUnspecified Priority = iota
	PriorityCritical
	PriorityHigh
	PriorityDefault
	PriorityLow
)

var (
	// ErrNotFound means the queue or job does not exist.
	ErrNotFound = errors.New("tasklane: not found")
	// ErrLeaseLost means the job exists but the caller's lease token is stale: the lease expired, was reaped or the job is no longer leased.
	ErrLeaseLost = errors.New("tasklane: lease lost")
	// ErrInvalid means an argument failed validation.
	ErrInvalid = errors.New("tasklane: invalid argument")
	// ErrAlreadyExists means the resource, such as a queue, already exists.
	ErrAlreadyExists = errors.New("tasklane: already exists")
	// ErrWrongState means the job exists but its state doesn't allow the
	// operation, such as cancelling a job that is already leased.
	ErrWrongState = errors.New("tasklane: wrong job state")
)

type Queue struct {
	Name              string
	MaxDepth          int64
	VisibilityTimeout time.Duration
	MaxAttempts       int
	Paused            bool
}

// WithDefaults fills zero-valued settings with the queue defaults.
func (q Queue) WithDefaults() Queue {
	if q.MaxDepth == 0 {
		q.MaxDepth = 100_000
	}
	if q.VisibilityTimeout == 0 {
		q.VisibilityTimeout = 30 * time.Second
	}
	if q.MaxAttempts == 0 {
		q.MaxAttempts = 10
	}
	return q
}

// StatsWindow is how far back QueueStats counts completed jobs. Completed
// jobs pile up until they are pruned, so counting all of them would get
// slower as the table grows; a recent window costs what the recent
// throughput costs, whatever the table size.
const StatsWindow = 10 * time.Second

// QueueStats are a queue's live job counts.
type QueueStats struct {
	Available         int64
	Leased            int64
	Dead              int64
	RecentlyCompleted int64 // completed within the last StatsWindow
}

type NewJob struct {
	Queue          string
	Kind           string
	Payload        json.RawMessage // empty means {}
	Priority       Priority        // PriorityUnspecified means PriorityDefault
	RunAt          time.Time       // zero means now
	MaxAttempts    int             // zero means the queue's default
	IdempotencyKey string          // empty means no deduplication
	TraceContext   string          // W3C traceparent, passed through to the worker
}

type EnqueueResult struct {
	ID      int64
	Created bool // false when the idempotency key matched an existing job
}

// DeadCursor marks where a page of dead jobs ended: the last job's
// FinishedAt and ID. The next page starts after it.
type DeadCursor struct {
	FinishedAt time.Time
	ID         int64
}

type Job struct {
	ID             int64
	Queue          string
	Kind           string
	Payload        json.RawMessage
	Priority       Priority
	State          JobState
	RunAt          time.Time
	Attempts       int
	MaxAttempts    int
	LastError      string
	LeaseToken     uuid.UUID // the current lease's token; on a completed job, the token that completed it; otherwise uuid.Nil
	LeaseExpiresAt time.Time // zero unless leased
	IdempotencyKey string
	TraceContext   string
	CreatedAt      time.Time
	FinishedAt     time.Time // zero until completed, dead or cancelled
}

type Store interface {
	// CreateQueue returns ErrAlreadyExists if the name is taken, and
	// ErrInvalid for an empty or unsafe name (1-128 characters from
	// [A-Za-z0-9._-]), negative settings or a visibility timeout that is
	// positive but under 1 ms.
	CreateQueue(ctx context.Context, q Queue) error
	// GetQueue returns ErrNotFound for an unknown queue.
	GetQueue(ctx context.Context, name string) (Queue, error)
	// QueueStats counts the queue's available, leased and dead jobs and the
	// jobs completed within the last StatsWindow. It returns ErrNotFound for
	// an unknown queue.
	QueueStats(ctx context.Context, queue string) (QueueStats, error)

	// Enqueue inserts 1 to 1000 jobs atomically and returns one result per
	// job, in the caller's order. A job whose idempotency key already exists
	// in its queue returns the existing ID with Created=false. Jobs with keys
	// are inserted first, sorted by (queue, key), so concurrent batches lock
	// keys in the same order and cannot deadlock. The commit wakes long-polling
	// leases on every queue in the batch.
	Enqueue(ctx context.Context, jobs []NewJob) ([]EnqueueResult, error)

	// Lease takes up to n runnable jobs, highest priority first. A ttl <= 0
	// uses the queue's visibility timeout. A paused queue returns no jobs and
	// no error; an unknown queue returns ErrNotFound.
	Lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]Job, error)

	// Ack, Nack and Heartbeat require the job's current lease token (Ack also
	// accepts the token that completed a completed job; see Ack). They
	// return ErrLeaseLost if the job exists but the token is stale, and
	// ErrNotFound if the job does not exist.

	// Ack marks the leased job completed and keeps its token, so repeating
	// the Ack with the same token returns nil (a retry after a lost response).
	Ack(ctx context.Context, id int64, token uuid.UUID) error
	// Nack fails the current attempt. A job whose attempts have reached
	// max_attempts moves to StateDead; otherwise it becomes available again at
	// retryAt, or after the store's backoff delay when retryAt is nil.
	Nack(ctx context.Context, id int64, token uuid.UUID, errMsg string, retryAt *time.Time) error
	// Heartbeat extends the lease and returns the new expiry. A ttl <= 0 uses
	// the queue's visibility timeout.
	Heartbeat(ctx context.Context, id int64, token uuid.UUID, ttl time.Duration) (time.Time, error)

	// ReapExpired treats up to limit expired leases as failed attempts and
	// returns how many it processed. Like Nack, a job whose attempts have
	// reached max_attempts moves to StateDead instead of being retried.
	ReapExpired(ctx context.Context, limit int) (int, error)

	// GetJob returns ErrNotFound for an unknown job.
	GetJob(ctx context.Context, id int64) (Job, error)

	// CancelJob moves an available job to StateCancelled and returns it.
	// Cancelling a cancelled job returns it unchanged, so a retry is
	// harmless. A job in any other state returns ErrWrongState; an unknown
	// job, ErrNotFound. Cancelling never makes a job runnable, so it wakes
	// no leases.
	CancelJob(ctx context.Context, id int64) (Job, error)

	// ListDeadJobs returns up to limit dead jobs of the queue, newest first
	// (finished_at, then id, both descending), starting after the cursor; a
	// nil cursor starts at the newest. It returns ErrNotFound for an unknown
	// queue and ErrInvalid for a limit below 1.
	ListDeadJobs(ctx context.Context, queue string, after *DeadCursor, limit int) ([]Job, error)

	// RedriveDead makes up to limit dead jobs of the queue available again,
	// with attempts reset to 0, run_at now and finished_at cleared, in one
	// transaction whose commit wakes long-polling leases. With ids, only
	// those jobs are considered and any that aren't dead jobs of this queue
	// are ignored; with nil ids, any dead job of the queue. It returns how
	// many jobs it redrove. last_error is kept. It returns ErrNotFound for an
	// unknown queue and ErrInvalid for a limit below 1.
	RedriveDead(ctx context.Context, queue string, ids []int64, limit int) (int, error)
}
