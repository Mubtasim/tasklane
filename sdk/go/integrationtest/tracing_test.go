package integrationtest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func recorder() (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	rec := tracetest.NewSpanRecorder()
	return rec, sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
}

// inTrace returns the ended spans of one trace.
func inTrace(rec *tracetest.SpanRecorder, id trace.TraceID) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == id {
			out = append(out, s)
		}
	}
	return out
}

func named(spans []sdktrace.ReadOnlySpan, name string, kind trace.SpanKind) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name && (kind == trace.SpanKindUnspecified || s.SpanKind() == kind) {
			out = append(out, s)
		}
	}
	return out
}

func attempt(s sdktrace.ReadOnlySpan) int64 {
	for _, a := range s.Attributes() {
		if a.Key == "tasklane.attempt" {
			return a.Value.AsInt64()
		}
	}
	return 0
}

// The §18.2 tree: a job that fails once and succeeds on its second attempt
// is one trace from the producer's span to the final Ack.
func TestOneTraceFromProducerToWorker(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, tp, tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "traced", MaxAttempts: 3})
	require.NoError(t, err)

	pctx, root := tp.Tracer("test").Start(ctx, "produce")
	_, err = e.client.Enqueue(pctx, "traced", tasklane.Job{Kind: "flaky"})
	require.NoError(t, err)
	root.End()

	var calls atomic.Int32
	w := e.client.NewWorker("traced", tasklane.WorkerOptions{Logger: discard})
	w.Handle("flaky", func(context.Context, *tasklane.LeasedJob) error {
		if calls.Add(1) == 1 {
			return errors.New("boom")
		}
		return nil
	})
	runWorker(t, w)
	id := root.SpanContext().TraceID()
	require.Eventually(t, func() bool {
		return len(named(inTrace(rec, id), "handle flaky", trace.SpanKindConsumer)) == 2
	}, 10*time.Second, 20*time.Millisecond)

	spans := inTrace(rec, id)
	enqClient := named(spans, "tasklane.v1.TasklaneService/Enqueue", trace.SpanKindClient)
	enqServer := named(spans, "tasklane.v1.TasklaneService/Enqueue", trace.SpanKindServer)
	require.Len(t, enqClient, 1)
	require.Len(t, enqServer, 1)
	require.Equal(t, root.SpanContext().SpanID(), enqClient[0].Parent().SpanID())
	require.Equal(t, enqClient[0].SpanContext().SpanID(), enqServer[0].Parent().SpanID())

	waits := named(spans, "queue wait", trace.SpanKindUnspecified)
	handles := named(spans, "handle flaky", trace.SpanKindConsumer)
	require.Len(t, waits, 2)
	require.Len(t, handles, 2)
	byAttempt := map[int64][2]sdktrace.ReadOnlySpan{}
	for _, wsp := range waits {
		require.Equal(t, enqServer[0].SpanContext().SpanID(), wsp.Parent().SpanID())
		p := byAttempt[attempt(wsp)]
		p[0] = wsp
		byAttempt[attempt(wsp)] = p
	}
	for _, h := range handles {
		require.Equal(t, enqServer[0].SpanContext().SpanID(), h.Parent().SpanID())
		p := byAttempt[attempt(h)]
		p[1] = h
		byAttempt[attempt(h)] = p
	}
	require.Len(t, byAttempt, 2)
	for n, p := range byAttempt {
		wait, handle := p[0], p[1]
		require.NotNil(t, wait, "attempt %d has no wait span", n)
		require.NotNil(t, handle, "attempt %d has no handle span", n)
		require.False(t, wait.EndTime().Before(wait.StartTime()))
		// The wait's times are Postgres's clock and the handle's the
		// worker's; both run on this machine in the test.
		require.False(t, handle.StartTime().Before(wait.EndTime().Add(-5*time.Millisecond)), "attempt %d: handle starts before its wait ends", n)
	}
	require.Equal(t, codes.Error, byAttempt[1][1].Status().Code)
	require.Equal(t, codes.Ok, byAttempt[2][1].Status().Code)

	nack := named(spans, "tasklane.v1.TasklaneService/Nack", trace.SpanKindClient)
	ack := named(spans, "tasklane.v1.TasklaneService/Ack", trace.SpanKindClient)
	require.Len(t, nack, 1)
	require.Len(t, ack, 1)
	require.Equal(t, byAttempt[1][1].SpanContext().SpanID(), nack[0].Parent().SpanID())
	require.Equal(t, byAttempt[2][1].SpanContext().SpanID(), ack[0].Parent().SpanID())
	require.Len(t, named(spans, "tasklane.v1.TasklaneService/Ack", trace.SpanKindServer), 1)
}

func TestAnUnsampledProducerGivesNoSpans(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, tp, tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "quiet"})
	require.NoError(t, err)
	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	sid, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	pctx := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid})) // not sampled
	res, err := e.client.Enqueue(pctx, "quiet", tasklane.Job{Kind: "k"})
	require.NoError(t, err)
	w := e.client.NewWorker("quiet", tasklane.WorkerOptions{Logger: discard})
	w.Handle("k", func(context.Context, *tasklane.LeasedJob) error { return nil })
	runWorker(t, w)
	require.Eventually(t, func() bool {
		got, err := e.raw.GetJob(ctx, &tasklanev1.GetJobRequest{Id: res.JobID})
		return err == nil && got.GetJob().GetState() == tasklanev1.JobState_JOB_STATE_COMPLETED
	}, 10*time.Second, 20*time.Millisecond)
	require.Empty(t, inTrace(rec, tid))
}

func TestAMalformedTraceContextStartsAFreshTrace(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, tp, tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "bad"})
	require.NoError(t, err)
	_, err = e.raw.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "bad", Job: &tasklanev1.JobSpec{Kind: "k", TraceContext: "garbage"}})
	require.NoError(t, err)
	w := e.client.NewWorker("bad", tasklane.WorkerOptions{Logger: discard})
	w.Handle("k", func(context.Context, *tasklane.LeasedJob) error { return nil })
	runWorker(t, w)
	require.Eventually(t, func() bool { return len(named(rec.Ended(), "handle k", trace.SpanKindConsumer)) == 1 }, 10*time.Second, 20*time.Millisecond)
	h := named(rec.Ended(), "handle k", trace.SpanKindConsumer)[0]
	require.False(t, h.Parent().IsValid())
	require.Equal(t, codes.Ok, h.Status().Code)
	require.Empty(t, named(rec.Ended(), "queue wait", trace.SpanKindUnspecified))
}

func TestALostLeaseMarksTheHandleSpan(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, tp, tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "lost", VisibilityTimeout: 300 * time.Millisecond})
	require.NoError(t, err)
	res, err := e.client.Enqueue(ctx, "lost", tasklane.Job{Kind: "k"})
	require.NoError(t, err)
	started := make(chan struct{})
	w := e.client.NewWorker("lost", tasklane.WorkerOptions{Logger: discard})
	w.Handle("k", func(ctx context.Context, _ *tasklane.LeasedJob) error {
		close(started)
		<-ctx.Done() // cancelled when the next heartbeat is rejected
		return ctx.Err()
	})
	runWorker(t, w)
	<-started
	// Take the lease away: the next heartbeat (every 100 ms) is rejected.
	_, err = testDB.Pool.Exec(ctx, "UPDATE jobs SET lease_token = gen_random_uuid() WHERE id = $1", res.JobID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(named(rec.Ended(), "handle k", trace.SpanKindConsumer)) == 1 }, 10*time.Second, 20*time.Millisecond)
	h := named(rec.Ended(), "handle k", trace.SpanKindConsumer)[0]
	require.Equal(t, codes.Error, h.Status().Code)
	require.Equal(t, "lease lost", h.Status().Description)
}

func TestAnUntracedServerStillConnectsProducerAndWorker(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, noop.NewTracerProvider(), tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "plain"})
	require.NoError(t, err)
	pctx, root := tp.Tracer("test").Start(ctx, "produce")
	_, err = e.client.Enqueue(pctx, "plain", tasklane.Job{Kind: "k"})
	require.NoError(t, err)
	root.End()
	w := e.client.NewWorker("plain", tasklane.WorkerOptions{Logger: discard})
	w.Handle("k", func(context.Context, *tasklane.LeasedJob) error { return nil })
	runWorker(t, w)
	id := root.SpanContext().TraceID()
	require.Eventually(t, func() bool { return len(named(inTrace(rec, id), "handle k", trace.SpanKindConsumer)) == 1 }, 10*time.Second, 20*time.Millisecond)
	spans := inTrace(rec, id)
	enqClient := named(spans, "tasklane.v1.TasklaneService/Enqueue", trace.SpanKindClient)
	require.Len(t, enqClient, 1)
	h := named(spans, "handle k", trace.SpanKindConsumer)[0]
	require.Equal(t, enqClient[0].SpanContext().SpanID(), h.Parent().SpanID(), "with no server span, the job's parent is the producer's client span")
	require.Empty(t, named(spans, "queue wait", trace.SpanKindUnspecified))
}

func TestTheDeadAttributeOnTheLastNack(t *testing.T) {
	rec, tp := recorder()
	e := newEnvWith(t, tp, tp)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "poison", MaxAttempts: 1})
	require.NoError(t, err)
	_, err = e.client.Enqueue(ctx, "poison", tasklane.Job{Kind: "k"})
	require.NoError(t, err)
	w := e.client.NewWorker("poison", tasklane.WorkerOptions{Logger: discard})
	w.Handle("k", func(context.Context, *tasklane.LeasedJob) error { return errors.New("always") })
	runWorker(t, w)
	require.Eventually(t, func() bool { return len(named(rec.Ended(), "handle k", trace.SpanKindConsumer)) == 1 }, 10*time.Second, 20*time.Millisecond)
	h := named(rec.Ended(), "handle k", trace.SpanKindConsumer)[0]
	require.Contains(t, h.Attributes(), attribute.Bool("tasklane.dead", true))
	require.Equal(t, "always", h.Status().Description)
}
