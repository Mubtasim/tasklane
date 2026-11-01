package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/metrics"
	"github.com/mubtasim/tasklane/internal/store"
)

// cancelledCtx stands for a client that went away while its write was in
// flight. pgx refuses to start anything on a cancelled context, so a store
// call that still used it would fail here every time.
func cancelledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// Each handler is called directly (not through gRPC, which would drop a call
// whose context is already cancelled) with a cancelled context. The write
// must still commit, its counter must move, and the handler must report
// success: the server must never call a committed write a failure.
func TestWritesFinishAfterTheClientCancels(t *testing.T) {
	setup := func(t *testing.T) (*env, *metrics.Metrics) {
		m := metrics.New()
		e := newEnv(t, nil, WithMetrics(m))
		_, err := e.client.CreateQueue(context.Background(), &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "q", MaxAttempts: 1}})
		require.NoError(t, err)
		return e, m
	}
	jobState := func(t *testing.T, e *env, id int64) store.JobState {
		t.Helper()
		j, err := e.store.GetJob(context.Background(), id)
		require.NoError(t, err)
		return j.State
	}

	t.Run("Enqueue", func(t *testing.T) {
		e, m := setup(t)
		res, err := e.server.Enqueue(cancelledCtx(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
		require.NoError(t, err)
		require.Equal(t, store.StateAvailable, jobState(t, e, res.GetJobId()))
		require.Equal(t, 1.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"))
	})

	t.Run("EnqueueBatch", func(t *testing.T) {
		e, m := setup(t)
		res, err := e.server.EnqueueBatch(cancelledCtx(), &tasklanev1.EnqueueBatchRequest{Queue: "q", Jobs: []*tasklanev1.JobSpec{{Kind: "k"}, {Kind: "k"}}})
		require.NoError(t, err)
		require.Len(t, res.GetResults(), 2)
		require.Equal(t, 2.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"))
	})

	t.Run("Lease", func(t *testing.T) {
		e, m := setup(t)
		id := e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		res, err := e.server.Lease(cancelledCtx(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 1})
		require.NoError(t, err)
		require.Len(t, res.GetJobs(), 1)
		require.Equal(t, store.StateLeased, jobState(t, e, id))
		require.Equal(t, 1.0, m.Value("tasklane_jobs_leased_total", "queue", "q"))
	})

	t.Run("Ack", func(t *testing.T) {
		e, m := setup(t)
		e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		job := e.mustLeaseOne(t, "q")
		_, err := e.server.Ack(cancelledCtx(), &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken()})
		require.NoError(t, err)
		require.Equal(t, store.StateCompleted, jobState(t, e, job.GetId()))
		require.Equal(t, 1.0, m.Value("tasklane_jobs_acked_total", "queue", "q"))
	})

	t.Run("Nack", func(t *testing.T) {
		e, m := setup(t)
		e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		job := e.mustLeaseOne(t, "q")
		_, err := e.server.Nack(cancelledCtx(), &tasklanev1.NackRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), Error: "boom"})
		require.NoError(t, err)
		require.Equal(t, store.StateDead, jobState(t, e, job.GetId()), "max_attempts is 1")
		require.Equal(t, 1.0, m.Value("tasklane_jobs_dead_total", "queue", "q", "reason", "nacked"))
	})

	t.Run("Heartbeat", func(t *testing.T) {
		e, _ := setup(t)
		e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		job := e.mustLeaseOne(t, "q")
		res, err := e.server.Heartbeat(cancelledCtx(), &tasklanev1.HeartbeatRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), VisibilityTimeout: durationpb.New(time.Hour)})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now().Add(time.Hour), res.GetLeaseExpiresAt().AsTime(), time.Minute)
	})

	t.Run("CancelJob", func(t *testing.T) {
		e, m := setup(t)
		id := e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		_, err := e.server.CancelJob(cancelledCtx(), &tasklanev1.CancelJobRequest{Id: id})
		require.NoError(t, err)
		require.Equal(t, store.StateCancelled, jobState(t, e, id))
		require.Equal(t, 1.0, m.Value("tasklane_jobs_cancelled_total", "queue", "q"))
	})

	t.Run("RedriveJobs", func(t *testing.T) {
		e, m := setup(t)
		e.mustEnqueue(t, "q", &tasklanev1.JobSpec{Kind: "k"})
		job := e.mustLeaseOne(t, "q")
		_, err := e.client.Nack(context.Background(), &tasklanev1.NackRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken()})
		require.NoError(t, err)
		res, err := e.server.RedriveJobs(cancelledCtx(), &tasklanev1.RedriveJobsRequest{Queue: "q", Ids: []int64{job.GetId()}})
		require.NoError(t, err)
		require.Equal(t, int64(1), res.GetRedriven())
		require.Equal(t, store.StateAvailable, jobState(t, e, job.GetId()))
		require.Equal(t, 1.0, m.Value("tasklane_jobs_redriven_total", "queue", "q"))
	})

	t.Run("CreateQueue", func(t *testing.T) {
		e, _ := setup(t)
		_, err := e.server.CreateQueue(cancelledCtx(), &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "other"}})
		require.NoError(t, err)
		_, err = e.store.GetQueue(context.Background(), "other")
		require.NoError(t, err)
	})
}

// Detaching the lease attempts must not detach the wait: a client that goes
// away mid long-poll frees its handler at once.
func TestLeaseWaitEndsWhenTheClientCancels(t *testing.T) {
	e := newEnv(t, nil, WithPollInterval(time.Hour))
	e.mustQueue(t, "q")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := e.server.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "q", Wait: durationpb.New(MaxWait)})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // let it reach the wait
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		requireCode(t, codes.Canceled, err)
		require.Less(t, time.Since(start), time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll outlived its client")
	}
}

// A lease that wakes after its client is gone must not take jobs for it:
// they would sit leased until their visibility timeout. Here the wake-up and
// the cancel are both ready when the wait starts; select picks between ready
// cases at random, so the case runs 20 times.
func TestLeaseDoesNotRetryForAGoneClient(t *testing.T) {
	for range 20 {
		n := newFakeNotifier()
		ctx, cancel := context.WithCancel(context.Background())
		st := &scriptedStore{hooks: []func() []store.Job{
			func() []store.Job { cancel(); n.fire(); return nil },
			func() []store.Job { return []store.Job{leasedJob()} },
		}}
		srv := NewServer(st, n, slog.New(slog.DiscardHandler), WithPollInterval(time.Hour))
		_, err := srv.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(MaxWait)})
		requireCode(t, codes.Canceled, err)
		require.Equal(t, 1, st.calls, "no second attempt for a cancelled client")
		cancel()
	}
}
