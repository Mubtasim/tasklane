package api

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/metrics"
)

// TestMetricsCountRealChanges drives every counted RPC through a real store
// and checks each counter moved once per real change, not per call.
func TestMetricsCountRealChanges(t *testing.T) {
	m := metrics.New()
	m.WatchPool(testDB.Pool.Stat)
	e := newEnv(t, nil, WithMetrics(m))
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "q", MaxAttempts: 1}})
	require.NoError(t, err)

	// Two enqueues with the same idempotency key create one job.
	for range 2 {
		_, err := e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k", IdempotencyKey: "same"}})
		require.NoError(t, err)
	}
	require.Equal(t, 1.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"))

	// Lease it, ack twice (the second is a retry): one ack.
	job := e.mustLeaseOne(t, "q")
	for range 2 {
		_, err = e.client.Ack(ctx, &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken()})
		require.NoError(t, err)
	}
	require.Equal(t, 1.0, m.Value("tasklane_jobs_leased_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_queue_wait_seconds", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_acked_total", "queue", "q"))

	// A second job: nacked with max_attempts 1, so it goes dead; redrive it;
	// cancel it twice (the repeat changes nothing).
	id := e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
	job = e.mustLeaseOne(t, "q")
	require.Equal(t, id, job.GetId())
	_, err = e.client.Nack(ctx, &tasklanev1.NackRequest{Id: id, LeaseToken: job.GetLeaseToken(), Error: "boom"})
	require.NoError(t, err)
	_, err = e.client.RedriveJobs(ctx, &tasklanev1.RedriveJobsRequest{Queue: "q", Ids: []int64{id}})
	require.NoError(t, err)
	for range 2 {
		_, err = e.client.CancelJob(ctx, &tasklanev1.CancelJobRequest{Id: id})
		require.NoError(t, err)
	}
	require.Equal(t, 1.0, m.Value("tasklane_jobs_dead_total", "queue", "q", "reason", "nacked"))
	require.Zero(t, m.Value("tasklane_jobs_nacked_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_redriven_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_cancelled_total", "queue", "q"))

	// Every call above was timed, by method and code.
	require.Equal(t, 2.0, m.Value("tasklane_rpc_duration_seconds", "method", "Ack", "code", "OK"))
	require.Equal(t, 2.0, m.Value("tasklane_rpc_duration_seconds", "method", "CancelJob", "code", "OK"))

	// The pool gauges read the real pool.
	require.Positive(t, m.Value("tasklane_db_pool_max_connections"))
}

// A batch that partly hits idempotency keys counts only the jobs it created.
func TestMetricsBatchCountsOnlyNewJobs(t *testing.T) {
	m := metrics.New()
	e := newEnv(t, nil, WithMetrics(m))
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "q"}})
	require.NoError(t, err)
	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k", IdempotencyKey: "a"}})
	require.NoError(t, err)
	require.Equal(t, 1.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"))

	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "q", Jobs: []*tasklanev1.JobSpec{
		{Kind: "k", IdempotencyKey: "a"},
		{Kind: "k", IdempotencyKey: "b"},
		{Kind: "k", IdempotencyKey: "c"},
	}})
	require.NoError(t, err)
	require.Equal(t, 3.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"), "1 from the first call plus 2 new jobs")
}

func TestMetricsRefusedEnqueueAndGauges(t *testing.T) {
	m := metrics.New()
	e := newEnv(t, nil, WithMetrics(m))
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "small", MaxDepth: 1}})
	require.NoError(t, err)
	e.mustEnqueue(t, "small", &tasklanev1.JobSpec{Kind: "k"})

	e.server.refreshDepths(ctx) // what RunDepthRefresh does each tick

	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "small", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, 1.0, m.Value("tasklane_enqueue_refused_total", "queue", "small"))
	require.Equal(t, 1.0, m.Value("tasklane_queue_depth", "queue", "small", "state", "available"))

	// A refused batch counts once, not once per job.
	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "small", Jobs: []*tasklanev1.JobSpec{{Kind: "k"}, {Kind: "k"}}})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, 2.0, m.Value("tasklane_enqueue_refused_total", "queue", "small"))

	// A zero-valued counter series exists for the known queue, so graphs
	// show 0 rather than no data. m.Value can't tell 0 from absent: read
	// the exposition text.
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	require.Contains(t, rec.Body.String(), `tasklane_jobs_dead_total{queue="small",reason="expired"} 0`)
}
