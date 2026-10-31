package tasklane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

func TestCancelJobConvertsTheJob(t *testing.T) {
	f, c := newFake(t)
	finished := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	payload, err := structpb.NewStruct(map[string]any{"user": 42})
	require.NoError(t, err)
	var got *tasklanev1.CancelJobRequest
	f.set(func(f *fakeServer) {
		f.cancelFn = func(req *tasklanev1.CancelJobRequest) (*tasklanev1.CancelJobResponse, error) {
			got = req
			return &tasklanev1.CancelJobResponse{Job: &tasklanev1.Job{
				Id: 7, Queue: "q", Kind: "send", Payload: payload, Priority: tasklanev1.Priority_PRIORITY_HIGH,
				State: tasklanev1.JobState_JOB_STATE_CANCELLED, MaxAttempts: 5, FinishedAt: timestamppb.New(finished),
			}}, nil
		}
	})

	job, err := c.CancelJob(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, int64(7), got.GetId())
	require.Equal(t, JobCancelled, job.State)
	require.Equal(t, PriorityHigh, job.Priority)
	require.True(t, finished.Equal(job.FinishedAt))
	require.True(t, job.RunAt.IsZero(), "unset timestamps are zero times")
	require.JSONEq(t, `{"user":42}`, string(job.Payload))
}

func TestListDeadJobsSendsPaging(t *testing.T) {
	f, c := newFake(t)
	var got *tasklanev1.ListDeadJobsRequest
	f.set(func(f *fakeServer) {
		f.listDeadFn = func(req *tasklanev1.ListDeadJobsRequest) (*tasklanev1.ListDeadJobsResponse, error) {
			got = req
			return &tasklanev1.ListDeadJobsResponse{
				Jobs:          []*tasklanev1.Job{{Id: 3, State: tasklanev1.JobState_JOB_STATE_DEAD, LastError: "boom"}},
				NextPageToken: "next",
			}, nil
		}
	})

	page, err := c.ListDeadJobs(context.Background(), "q", 10, "tok")
	require.NoError(t, err)
	require.Equal(t, "q", got.GetQueue())
	require.Equal(t, int32(10), got.GetPageSize())
	require.Equal(t, "tok", got.GetPageToken())
	require.Equal(t, "next", page.NextPageToken)
	require.Len(t, page.Jobs, 1)
	require.Equal(t, JobDead, page.Jobs[0].State)
	require.Equal(t, "boom", page.Jobs[0].LastError)
	require.Equal(t, json.RawMessage("{}"), page.Jobs[0].Payload, "no payload is the empty object")
}

func TestRedriveJobsAndRedriveAll(t *testing.T) {
	f, c := newFake(t)
	var reqs []*tasklanev1.RedriveJobsRequest
	f.set(func(f *fakeServer) {
		f.redriveFn = func(req *tasklanev1.RedriveJobsRequest) (*tasklanev1.RedriveJobsResponse, error) {
			reqs = append(reqs, req)
			return &tasklanev1.RedriveJobsResponse{Redriven: 2, Skipped: 1}, nil
		}
	})

	res, err := c.RedriveJobs(context.Background(), "q", []int64{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, RedriveResult{Redriven: 2, Skipped: 1}, res)

	n, err := c.RedriveAll(context.Background(), "q")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	require.Equal(t, []int64{1, 2, 3}, reqs[0].GetIds())
	require.False(t, reqs[0].GetAll())
	require.True(t, reqs[1].GetAll())
	require.Empty(t, reqs[1].GetIds())
}
