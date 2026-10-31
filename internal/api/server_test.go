package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

func (e *env) mustQueue(t *testing.T, name string) {
	t.Helper()
	e.mustQueueCtx(t, context.Background(), name)
}

// mustQueueCtx creates a queue with ctx, which may carry an API key.
func (e *env) mustQueueCtx(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: name}})
	require.NoError(t, err)
}

func (e *env) mustEnqueue(t *testing.T, queue string, spec *tasklanev1.JobSpec) int64 {
	t.Helper()
	res, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: queue, Job: spec})
	require.NoError(t, err)
	return res.GetJobId()
}

func (e *env) mustLeaseOne(t *testing.T, queue string) *tasklanev1.Job {
	t.Helper()
	res, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: queue, MaxJobs: 1})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), 1)
	return res.GetJobs()[0]
}

func TestCreateAndGetQueue(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	created, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "emails", MaxAttempts: 3}})
	require.NoError(t, err)
	require.Equal(t, int64(100_000), created.GetQueue().GetMaxDepth(), "defaults are applied")
	require.Equal(t, int32(3), created.GetQueue().GetMaxAttempts())

	got, err := e.client.GetQueue(ctx, &tasklanev1.GetQueueRequest{Name: "emails"})
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, got.GetQueue().GetVisibilityTimeout().AsDuration())
}

// CreateQueue answers with the queue as stored: the store keeps whole
// milliseconds, so GetQueue must agree with the CreateQueue response.
func TestCreateQueueEchoesStoredQueue(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()

	created, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "q", VisibilityTimeout: durationpb.New(1500 * time.Microsecond)}})
	require.NoError(t, err)
	got, err := e.client.GetQueue(ctx, &tasklanev1.GetQueueRequest{Name: "q"})
	require.NoError(t, err)
	require.Equal(t, time.Millisecond, got.GetQueue().GetVisibilityTimeout().AsDuration())
	require.Equal(t, got.GetQueue().GetVisibilityTimeout().AsDuration(), created.GetQueue().GetVisibilityTimeout().AsDuration())
}

func TestErrorMapping(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})
	job := e.mustLeaseOne(t, "emails")

	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "emails"}})
	requireCode(t, codes.AlreadyExists, err)

	_, err = e.client.GetQueue(ctx, &tasklanev1.GetQueueRequest{Name: "nope"})
	requireCode(t, codes.NotFound, err)

	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "emails", Job: &tasklanev1.JobSpec{}})
	requireCode(t, codes.InvalidArgument, err) // kind is required

	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "nope", Job: &tasklanev1.JobSpec{Kind: "x"}})
	requireCode(t, codes.NotFound, err)

	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "emails"})
	requireCode(t, codes.InvalidArgument, err) // empty batch

	_, err = e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "nope"})
	requireCode(t, codes.NotFound, err)

	_, err = e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(-time.Second)})
	requireCode(t, codes.InvalidArgument, err)

	_, err = e.client.Ack(ctx, &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: "not-a-uuid"})
	requireCode(t, codes.InvalidArgument, err)

	_, err = e.client.Ack(ctx, &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: uuid.NewString()})
	requireCode(t, codes.FailedPrecondition, err)

	_, err = e.client.Heartbeat(ctx, &tasklanev1.HeartbeatRequest{Id: 999_999, LeaseToken: job.GetLeaseToken()})
	requireCode(t, codes.NotFound, err)

	_, err = e.client.GetJob(ctx, &tasklanev1.GetJobRequest{Id: 999_999})
	requireCode(t, codes.NotFound, err)
}

func TestPayloadRoundTripsThroughTheAPI(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")
	payload, err := structpb.NewStruct(map[string]any{"user": 42, "to": "a@example.com", "tags": []any{"new"}})
	require.NoError(t, err)

	id := e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome", Payload: payload, Priority: tasklanev1.Priority_PRIORITY_HIGH})

	job := e.mustLeaseOne(t, "emails")
	require.Equal(t, id, job.GetId())
	require.Equal(t, payload.AsMap(), job.GetPayload().AsMap())
	require.Equal(t, tasklanev1.Priority_PRIORITY_HIGH, job.GetPriority())
	require.Equal(t, tasklanev1.JobState_JOB_STATE_LEASED, job.GetState())
	require.NotEmpty(t, job.GetLeaseToken())

	got, err := e.client.GetJob(context.Background(), &tasklanev1.GetJobRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, payload.AsMap(), got.GetJob().GetPayload().AsMap())
	require.Empty(t, got.GetJob().GetLeaseToken(), "GetJob never returns the token")
}

func TestAckIsIdempotentThroughTheAPI(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})
	job := e.mustLeaseOne(t, "emails")

	ack := &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken()}
	_, err := e.client.Ack(ctx, ack)
	require.NoError(t, err)
	_, err = e.client.Ack(ctx, ack)
	require.NoError(t, err, "a retried ack succeeds")

	got, err := e.client.GetJob(ctx, &tasklanev1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_COMPLETED, got.GetJob().GetState())
}

func TestNackAndHeartbeat(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})
	job := e.mustLeaseOne(t, "emails")

	hb, err := e.client.Heartbeat(ctx, &tasklanev1.HeartbeatRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), VisibilityTimeout: durationpb.New(time.Hour)})
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Hour), hb.GetLeaseExpiresAt().AsTime(), 5*time.Second)

	retryAt := time.Now().Add(10 * time.Minute)
	_, err = e.client.Nack(ctx, &tasklanev1.NackRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), Error: "smtp timeout", RetryAt: timestamppb.New(retryAt)})
	require.NoError(t, err)

	got, err := e.client.GetJob(ctx, &tasklanev1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_AVAILABLE, got.GetJob().GetState())
	require.Equal(t, "smtp timeout", got.GetJob().GetLastError())
	require.WithinDuration(t, retryAt, got.GetJob().GetRunAt().AsTime(), time.Millisecond)

	_, err = e.client.Nack(ctx, &tasklanev1.NackRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), Error: "again"})
	requireCode(t, codes.FailedPrecondition, err) // a repeated nack (design §6)
}

func TestEnqueueBatchKeepsOrder(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")

	res, err := e.client.EnqueueBatch(context.Background(), &tasklanev1.EnqueueBatchRequest{Queue: "emails", Jobs: []*tasklanev1.JobSpec{
		{Kind: "a", IdempotencyKey: "z"}, {Kind: "b"}, {Kind: "c", IdempotencyKey: "z"},
	}})
	require.NoError(t, err)
	require.Len(t, res.GetResults(), 3)
	require.True(t, res.GetResults()[0].GetCreated())
	require.True(t, res.GetResults()[1].GetCreated())
	require.False(t, res.GetResults()[2].GetCreated())
	require.Equal(t, res.GetResults()[0].GetJobId(), res.GetResults()[2].GetJobId())
}

// The poll interval is set far above the test's timings, so only the
// LISTEN/NOTIFY path can wake the lease in time.
func TestLongPollWakesOnEnqueue(t *testing.T) {
	e := newEnv(t, nil, WithPollInterval(time.Minute))
	e.mustQueue(t, "emails")

	enqueued := make(chan time.Time, 1)
	done := make(chan struct{})
	t.Cleanup(func() { <-done }) // no t.Error after the test has ended
	go func() {
		defer close(done)
		time.Sleep(300 * time.Millisecond) // let the lease start waiting
		enqueued <- time.Now()
		if _, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "emails", Job: &tasklanev1.JobSpec{Kind: "welcome"}}); err != nil {
			t.Error(err) // require/FailNow must not be called from a goroutine
		}
	}()

	res, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(10 * time.Second)})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), 1)
	require.Less(t, time.Since(<-enqueued), time.Second, "woken by the notification, not by a timeout")
}

// Delayed jobs become due without a notification; the periodic re-check
// finds them.
func TestLongPollRechecksForDelayedJobs(t *testing.T) {
	e := newEnv(t, nil, WithPollInterval(100*time.Millisecond))
	e.mustQueue(t, "emails")
	e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "later", RunAt: timestamppb.New(time.Now().Add(300 * time.Millisecond))})
	// The enqueue's notification fired before the lease started, and the job
	// isn't due until later: only the periodic re-check can find it.

	start := time.Now()
	res, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(5 * time.Second)})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), 1)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestLongPollReturnsEmptyWhenWaitElapses(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")

	start := time.Now()
	res, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(300 * time.Millisecond)})
	require.NoError(t, err)
	require.Empty(t, res.GetJobs())
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
}

func TestShutdownEndsWaitingLeases(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")

	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		time.Sleep(200 * time.Millisecond)
		e.server.Shutdown()
	}()
	start := time.Now()
	res, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(10 * time.Second)})
	require.NoError(t, err)
	require.Empty(t, res.GetJobs())
	require.Less(t, time.Since(start), 2*time.Second)
}

// Workers must back off after a shutdown instead of hot-looping on empty
// leases, and the store must not be touched.
func TestLeaseAfterShutdownIsUnavailable(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")
	id := e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})

	e.server.Shutdown()
	for _, wait := range []time.Duration{0, 10 * time.Second} {
		_, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(wait)})
		requireCode(t, codes.Unavailable, err)
	}
	got, err := e.client.GetJob(context.Background(), &tasklanev1.GetJobRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_AVAILABLE, got.GetJob().GetState(), "the job was not leased")
}

func TestWithPollIntervalIgnoresNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		s := NewServer(nil, nil, slog.New(slog.DiscardHandler), WithPollInterval(d))
		require.Equal(t, DefaultPollInterval, s.pollInterval, d)
	}
	s := NewServer(nil, nil, slog.New(slog.DiscardHandler), WithPollInterval(time.Minute))
	require.Equal(t, time.Minute, s.pollInterval)
}

func TestGetQueueReturnsLiveStats(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	for range 3 {
		e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})
	}
	done := e.mustLeaseOne(t, "emails")
	_, err := e.client.Ack(ctx, &tasklanev1.AckRequest{Id: done.GetId(), LeaseToken: done.GetLeaseToken()})
	require.NoError(t, err)
	e.mustLeaseOne(t, "emails")

	got, err := e.client.GetQueue(ctx, &tasklanev1.GetQueueRequest{Name: "emails"})
	require.NoError(t, err)
	st := got.GetStats()
	require.Equal(t, int64(1), st.GetAvailable())
	require.Equal(t, int64(1), st.GetLeased())
	require.Equal(t, int64(0), st.GetDead())
	require.Equal(t, int64(1), st.GetRecentlyCompleted())
	require.Equal(t, 10*time.Second, st.GetRecentWindow().AsDuration())
}

func TestEnqueueRefusedWhenQueueIsFull(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "small", MaxDepth: 2}})
	require.NoError(t, err)
	for range 2 {
		_, err := e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "small", Job: &tasklanev1.JobSpec{Kind: "k"}})
		require.NoError(t, err, "no refresh has run yet: the cache fails open")
	}
	mustRefresh(t, e.server.depths, ctx, e.store)

	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "small", Job: &tasklanev1.JobSpec{Kind: "k"}})
	st := status.Convert(err)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	require.Len(t, st.Details(), 1)
	require.Equal(t, time.Second, st.Details()[0].(*errdetails.RetryInfo).GetRetryDelay().AsDuration())

	_, err = e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "small", Jobs: []*tasklanev1.JobSpec{{Kind: "k"}}})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "a batch is refused whole")

	// Draining the queue makes room after the next refresh.
	jobs, err := e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "small", MaxJobs: 2})
	require.NoError(t, err)
	for _, j := range jobs.GetJobs() {
		_, err := e.client.Ack(ctx, &tasklanev1.AckRequest{Id: j.GetId(), LeaseToken: j.GetLeaseToken()})
		require.NoError(t, err)
	}
	mustRefresh(t, e.server.depths, ctx, e.store)
	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "small", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
}
