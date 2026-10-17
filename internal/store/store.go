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
	ErrNotFound      = errors.New("tasklane: not found")
	ErrLeaseLost     = errors.New("tasklane: lease lost")
	ErrInvalid       = errors.New("tasklane: invalid argument")
	ErrAlreadyExists = errors.New("tasklane: already exists")
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
	LeaseToken     uuid.UUID // uuid.Nil unless leased
	LeaseExpiresAt time.Time // zero unless leased
	IdempotencyKey string
	TraceContext   string
	CreatedAt      time.Time
	FinishedAt     time.Time // zero until completed, dead or cancelled
}

type Store interface {
	CreateQueue(ctx context.Context, q Queue) error
	GetQueue(ctx context.Context, name string) (Queue, error)

	// Enqueue inserts jobs atomically, in order. A job whose idempotency key
	// already exists in its queue returns the existing ID with Created=false.
	Enqueue(ctx context.Context, jobs []NewJob) ([]EnqueueResult, error)

	// Lease takes up to n runnable jobs, highest priority first. A ttl <= 0
	// uses the queue's visibility timeout.
	Lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]Job, error)

	// Ack, Nack and Heartbeat require the job's current lease token and
	// return ErrLeaseLost otherwise.
	Ack(ctx context.Context, id int64, token uuid.UUID) error
	Nack(ctx context.Context, id int64, token uuid.UUID, errMsg string, retryAt *time.Time) error
	Heartbeat(ctx context.Context, id int64, token uuid.UUID, ttl time.Duration) (time.Time, error)

	// ReapExpired treats up to limit expired leases as failed attempts and
	// returns how many it processed.
	ReapExpired(ctx context.Context, limit int) (int, error)

	GetJob(ctx context.Context, id int64) (Job, error)
}
