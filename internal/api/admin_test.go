package api

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/store"
)

func mustEnqueueAPI(t *testing.T, e *env, queue string) int64 {
	t.Helper()
	res, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: queue, Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	return res.GetJobId()
}

// mustDeadJobs creates queue with max_attempts 1 and n jobs that each fail
// their only attempt, returning their ids in enqueue order.
func mustDeadJobs(t *testing.T, e *env, queue string, n int) []int64 {
	t.Helper()
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: queue, MaxAttempts: 1}})
	require.NoError(t, err)
	var ids []int64
	for range n {
		ids = append(ids, mustEnqueueAPI(t, e, queue))
	}
	res, err := e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: queue, MaxJobs: int32(n)})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), n)
	for _, j := range res.GetJobs() {
		_, err := e.client.Nack(ctx, &tasklanev1.NackRequest{Id: j.GetId(), LeaseToken: j.GetLeaseToken(), Error: "boom"})
		require.NoError(t, err)
	}
	return ids
}

func TestCancelJobOutcomes(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "emails"}})
	require.NoError(t, err)
	id := mustEnqueueAPI(t, e, "emails")

	res, err := e.client.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_CANCELLED, res.GetJob().GetState())
	require.Empty(t, res.GetJob().GetLeaseToken())

	_, err = e.client.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: id})
	require.NoError(t, err, "a retried cancel succeeds")

	leasedID := mustEnqueueAPI(t, e, "emails")
	_, err = e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails"})
	require.NoError(t, err)
	_, err = e.client.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: leasedID})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "leased")

	_, err = e.client.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: 999})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestListDeadJobsPages(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	ids := mustDeadJobs(t, e, "emails", 3)

	first, err := e.client.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: "emails", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, first.GetJobs(), 2)
	require.NotEmpty(t, first.GetNextPageToken())
	require.Equal(t, "boom", first.GetJobs()[0].GetLastError())

	second, err := e.client.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: "emails", PageSize: 2, PageToken: first.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, second.GetJobs(), 1)
	require.Empty(t, second.GetNextPageToken(), "the last page has no token")

	var got []int64
	for _, j := range append(first.GetJobs(), second.GetJobs()...) {
		got = append(got, j.GetId())
	}
	require.ElementsMatch(t, ids, got)

	exact, err := e.client.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: "emails", PageSize: 3})
	require.NoError(t, err)
	require.Len(t, exact.GetJobs(), 3)
	require.Empty(t, exact.GetNextPageToken(), "a full last page still has no token")
}

func TestListDeadJobsErrors(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	mustDeadJobs(t, e, "emails", 1)

	_, err := e.client.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: "emails", PageToken: "not-a-token!"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = e.client.ListDeadJobs(ctx, &tasklanev1.ListDeadJobsRequest{Queue: "nope"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestPageTokenRoundTrip(t *testing.T) {
	c := store.DeadCursor{FinishedAt: time.Date(2026, 10, 31, 12, 0, 0, 123456000, time.UTC), ID: 42}
	got, err := decodePageToken(encodePageToken(c))
	require.NoError(t, err)
	require.True(t, c.FinishedAt.Equal(got.FinishedAt))
	require.Equal(t, c.ID, got.ID)

	none, err := decodePageToken("")
	require.NoError(t, err)
	require.Nil(t, none)
}

func TestRedriveJobsByIDs(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	ids := mustDeadJobs(t, e, "emails", 2)

	res, err := e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", Ids: []int64{ids[0], ids[0], 999}})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.GetRedriven())
	require.Equal(t, int64(1), res.GetSkipped(), "999 is skipped; the duplicate counts once")

	job, err := e.client.GetJob(ctx, &tasklanev1.GetJobRequest{Id: ids[0]})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_AVAILABLE, job.GetJob().GetState())
	require.Zero(t, job.GetJob().GetAttempts())
}

func TestRedriveJobsAll(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	mustDeadJobs(t, e, "emails", 3)

	res, err := e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", All: true})
	require.NoError(t, err)
	require.Equal(t, int64(3), res.GetRedriven())

	again, err := e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", All: true})
	require.NoError(t, err)
	require.Zero(t, again.GetRedriven(), "repeating is harmless")
}

func TestRedriveJobsValidation(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	mustDeadJobs(t, e, "emails", 1)

	_, err := e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails"})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "neither ids nor all")
	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", Ids: []int64{1}, All: true})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "both")
	many := make([]int64, maxRedriveIDs+1)
	for i := range many {
		many[i] = int64(i + 1)
	}
	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", Ids: many})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "too many ids")
	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "nope", All: true})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestHTTPCancelAndRedrive(t *testing.T) {
	e := newEnv(t, nil)
	base := newHTTP(t, e, func(context.Context) error { return nil })
	ids := mustDeadJobs(t, e, "emails", 1)

	code, body := call(t, "GET", base+"/v1/queues/emails/dead?page_size=10", "", "")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["jobs"], 1)

	code, body = call(t, "POST", base+"/v1/queues/emails/dead:redrive", "", `{"all":true}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "1", body["redriven"], "int64 is a string in proto3 JSON")

	code, _ = call(t, "POST", base+"/v1/jobs/"+strconv.FormatInt(ids[0], 10)+":cancel", "", `{}`)
	require.Equal(t, http.StatusOK, code, "the redriven job is available again, so it can be cancelled")

	leased := mustEnqueueAPI(t, e, "emails")
	_, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails"})
	require.NoError(t, err)
	code, _ = call(t, "POST", base+"/v1/jobs/"+strconv.FormatInt(leased, 10)+":cancel", "", `{}`)
	require.Equal(t, http.StatusConflict, code)
}
