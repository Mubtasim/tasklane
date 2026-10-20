package tasklane

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// Server errors are returned as gRPC status errors: check them with
// status.Code(err), for example codes.AlreadyExists from CreateQueue. The
// client does not retry producer calls; to retry an Enqueue safely, give the
// job an IdempotencyKey and send it again.

// Priority orders leasing: lower values run first. The zero value lets the
// server use PriorityDefault.
type Priority int32

const (
	PriorityUnspecified Priority = iota
	PriorityCritical
	PriorityHigh
	PriorityDefault
	PriorityLow
)

// Job is a job to enqueue.
type Job struct {
	Kind string
	// Payload is JSON-encoded and must encode to a JSON object (a struct or a
	// map); nil means an empty payload.
	Payload  any
	Priority Priority
	// RunAt delays the job until then; zero means now.
	RunAt time.Time
	// IdempotencyKey makes a repeated Enqueue return the original job.
	IdempotencyKey string
	// MaxAttempts overrides the queue's setting; zero keeps it.
	MaxAttempts int
}

type EnqueueResult struct {
	JobID   int64
	Created bool // false when IdempotencyKey matched an existing job
}

// QueueConfig is a queue's settings. When creating a queue, zero values take
// the server's defaults.
type QueueConfig struct {
	Name              string
	MaxDepth          int64
	VisibilityTimeout time.Duration
	MaxAttempts       int
	Paused            bool
}

// QueueStats are a queue's live job counts. Completed jobs are counted only
// over the last RecentWindow, because counting all of them would get slower
// as they pile up.
type QueueStats struct {
	Available         int64
	Leased            int64
	Dead              int64
	RecentlyCompleted int64
	RecentWindow      time.Duration
}

// QueueInfo is what GetQueue returns.
type QueueInfo struct {
	Config QueueConfig
	Stats  QueueStats
}

func (c *Client) Enqueue(ctx context.Context, queue string, job Job) (EnqueueResult, error) {
	spec, err := job.toProto()
	if err != nil {
		return EnqueueResult{}, err
	}
	res, err := c.api.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: queue, Job: spec})
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{JobID: res.GetJobId(), Created: res.GetCreated()}, nil
}

// EnqueueBatch inserts up to 1000 jobs in one transaction. The results are
// in the same order as jobs.
func (c *Client) EnqueueBatch(ctx context.Context, queue string, jobs []Job) ([]EnqueueResult, error) {
	specs := make([]*tasklanev1.JobSpec, len(jobs))
	for i, j := range jobs {
		spec, err := j.toProto()
		if err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		specs[i] = spec
	}
	res, err := c.api.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: queue, Jobs: specs})
	if err != nil {
		return nil, err
	}
	out := make([]EnqueueResult, len(res.GetResults()))
	for i, r := range res.GetResults() {
		out[i] = EnqueueResult{JobID: r.GetJobId(), Created: r.GetCreated()}
	}
	return out, nil
}

// CreateQueue creates a queue and returns its settings with the defaults
// applied.
func (c *Client) CreateQueue(ctx context.Context, cfg QueueConfig) (QueueConfig, error) {
	q := &tasklanev1.Queue{Name: cfg.Name, MaxDepth: cfg.MaxDepth, MaxAttempts: int32(cfg.MaxAttempts), Paused: cfg.Paused}
	if cfg.VisibilityTimeout != 0 {
		q.VisibilityTimeout = durationpb.New(cfg.VisibilityTimeout)
	}
	res, err := c.api.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: q})
	if err != nil {
		return QueueConfig{}, err
	}
	return queueFromProto(res.GetQueue()), nil
}

// GetQueue returns a queue's settings and live counts.
func (c *Client) GetQueue(ctx context.Context, name string) (QueueInfo, error) {
	res, err := c.api.GetQueue(ctx, &tasklanev1.GetQueueRequest{Name: name})
	if err != nil {
		return QueueInfo{}, err
	}
	s := res.GetStats()
	return QueueInfo{
		Config: queueFromProto(res.GetQueue()),
		Stats: QueueStats{
			Available:         s.GetAvailable(),
			Leased:            s.GetLeased(),
			Dead:              s.GetDead(),
			RecentlyCompleted: s.GetRecentlyCompleted(),
			RecentWindow:      s.GetRecentWindow().AsDuration(),
		},
	}, nil
}

func (j Job) toProto() (*tasklanev1.JobSpec, error) {
	payload, err := payloadToStruct(j.Payload)
	if err != nil {
		return nil, err
	}
	spec := &tasklanev1.JobSpec{
		Kind:           j.Kind,
		Payload:        payload,
		Priority:       tasklanev1.Priority(j.Priority),
		IdempotencyKey: j.IdempotencyKey,
		MaxAttempts:    int32(j.MaxAttempts),
	}
	if !j.RunAt.IsZero() {
		spec.RunAt = timestamppb.New(j.RunAt)
	}
	return spec, nil
}

func queueFromProto(q *tasklanev1.Queue) QueueConfig {
	return QueueConfig{
		Name:              q.GetName(),
		MaxDepth:          q.GetMaxDepth(),
		VisibilityTimeout: q.GetVisibilityTimeout().AsDuration(),
		MaxAttempts:       int(q.GetMaxAttempts()),
		Paused:            q.GetPaused(),
	}
}
