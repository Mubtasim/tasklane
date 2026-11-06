package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// Only a write that made jobs runnable wakes anyone: an idempotent repeat
// creates nothing, and a failed write commits nothing.
func TestEnqueuePublishesOnlyWhenItCreatedJobs(t *testing.T) {
	n := newFakeNotifier()
	e := newEnv(t, nil, withNotifier(n))
	ctx := context.Background()
	e.mustQueue(t, "emails")

	spec := &tasklanev1.JobSpec{Kind: "k", IdempotencyKey: "once"}
	_, err := e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "emails", Job: spec})
	require.NoError(t, err)
	require.Equal(t, []string{"emails"}, n.publishes())

	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "emails", Job: spec})
	require.NoError(t, err)
	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "emails", Jobs: []*tasklanev1.JobSpec{spec}})
	require.NoError(t, err)
	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "missing", Job: &tasklanev1.JobSpec{Kind: "k"}})
	requireCode(t, codes.NotFound, err)
	require.Equal(t, []string{"emails"}, n.publishes(), "repeats and failures publish nothing")

	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "emails", Jobs: []*tasklanev1.JobSpec{spec, {Kind: "a"}, {Kind: "b"}}})
	require.NoError(t, err)
	require.Equal(t, []string{"emails", "emails"}, n.publishes(), "one publish per batch")
}

func TestRedriveJobsPublishesOnlyWhenItRedrove(t *testing.T) {
	n := newFakeNotifier()
	e := newEnv(t, nil, withNotifier(n))
	ctx := context.Background()
	ids := mustDeadJobs(t, e, "emails", 2)
	before := len(n.publishes()) // the enqueues published

	_, err := e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", Ids: []int64{999}})
	require.NoError(t, err)
	require.Len(t, n.publishes(), before, "nothing redriven, nothing published")

	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", Ids: []int64{ids[0]}})
	require.NoError(t, err)
	require.Len(t, n.publishes(), before+1)

	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", All: true})
	require.NoError(t, err)
	require.Len(t, n.publishes(), before+2)

	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "emails", All: true})
	require.NoError(t, err)
	require.Len(t, n.publishes(), before+2, "an empty redrive publishes nothing")
	require.Equal(t, "emails", n.publishes()[before])
}
