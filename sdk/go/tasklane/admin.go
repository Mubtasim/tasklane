package tasklane

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// JobState is where a job is in its lifecycle.
type JobState string

const (
	JobAvailable JobState = "available"
	JobLeased    JobState = "leased"
	JobCompleted JobState = "completed"
	JobDead      JobState = "dead"
	JobCancelled JobState = "cancelled"
)

var jobStateFromProto = map[tasklanev1.JobState]JobState{
	tasklanev1.JobState_JOB_STATE_AVAILABLE: JobAvailable,
	tasklanev1.JobState_JOB_STATE_LEASED:    JobLeased,
	tasklanev1.JobState_JOB_STATE_COMPLETED: JobCompleted,
	tasklanev1.JobState_JOB_STATE_DEAD:      JobDead,
	tasklanev1.JobState_JOB_STATE_CANCELLED: JobCancelled,
}

// JobInfo is a stored job as the server reports it. Zero times mean unset
// (FinishedAt stays zero until the job is completed, dead or cancelled).
type JobInfo struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage // a JSON object
	Priority    Priority
	State       JobState
	RunAt       time.Time
	Attempts    int
	MaxAttempts int
	LastError   string
	CreatedAt   time.Time
	FinishedAt  time.Time
}

// DeadJobsPage is one page of ListDeadJobs.
type DeadJobsPage struct {
	Jobs []JobInfo
	// NextPageToken fetches the next page; it is empty on the last page.
	NextPageToken string
}

// RedriveResult is what RedriveJobs did.
type RedriveResult struct {
	Redriven int64
	Skipped  int64 // ids that weren't dead jobs of the queue
}

// CancelJob cancels a job that is still available and returns it.
// Cancelling a cancelled job succeeds. A leased or finished job is a
// codes.FailedPrecondition error.
func (c *Client) CancelJob(ctx context.Context, id int64) (JobInfo, error) {
	res, err := c.api.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: id})
	if err != nil {
		return JobInfo{}, err
	}
	return jobInfoFromProto(res.GetJob())
}

// ListDeadJobs returns one page of the queue's dead jobs, newest first.
// pageSize 0 means the server's maximum (100); pass the previous page's
// NextPageToken to continue, or "" to start.
func (c *Client) ListDeadJobs(ctx context.Context, queue string, pageSize int, pageToken string) (DeadJobsPage, error) {
	res, err := c.api.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: queue, PageSize: int32(pageSize), PageToken: pageToken})
	if err != nil {
		return DeadJobsPage{}, err
	}
	page := DeadJobsPage{NextPageToken: res.GetNextPageToken(), Jobs: make([]JobInfo, 0, len(res.GetJobs()))}
	for _, pj := range res.GetJobs() {
		j, err := jobInfoFromProto(pj)
		if err != nil {
			return DeadJobsPage{}, err
		}
		page.Jobs = append(page.Jobs, j)
	}
	return page, nil
}

// RedriveJobs makes the given dead jobs (at most 1000) available again with
// their attempts reset. Ids that aren't dead jobs of the queue are skipped.
func (c *Client) RedriveJobs(ctx context.Context, queue string, ids []int64) (RedriveResult, error) {
	res, err := c.api.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: queue, Ids: ids})
	if err != nil {
		return RedriveResult{}, err
	}
	return RedriveResult{Redriven: res.GetRedriven(), Skipped: res.GetSkipped()}, nil
}

// RedriveAll redrives every dead job of the queue, in batches on the
// server, and returns how many it redrove. It is not atomic: after an
// error, some batches may already be redriven; calling it again finishes
// the rest.
func (c *Client) RedriveAll(ctx context.Context, queue string) (int64, error) {
	res, err := c.api.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: queue, All: true})
	if err != nil {
		return 0, err
	}
	return res.GetRedriven(), nil
}

func jobInfoFromProto(pj *tasklanev1.Job) (JobInfo, error) {
	payload, err := payloadFromStruct(pj.GetPayload())
	if err != nil {
		return JobInfo{}, fmt.Errorf("tasklane: job %d: decode payload: %w", pj.GetId(), err)
	}
	return JobInfo{
		ID:          pj.GetId(),
		Queue:       pj.GetQueue(),
		Kind:        pj.GetKind(),
		Payload:     payload,
		Priority:    Priority(pj.GetPriority()),
		State:       jobStateFromProto[pj.GetState()],
		RunAt:       optionalTime(pj.GetRunAt()),
		Attempts:    int(pj.GetAttempts()),
		MaxAttempts: int(pj.GetMaxAttempts()),
		LastError:   pj.GetLastError(),
		CreatedAt:   optionalTime(pj.GetCreatedAt()),
		FinishedAt:  optionalTime(pj.GetFinishedAt()),
	}, nil
}

// optionalTime maps an unset timestamp to the zero time (AsTime would give
// the Unix epoch).
func optionalTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
