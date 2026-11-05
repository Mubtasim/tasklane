package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/traceparent"
)

// The caller's span: sampled, as a producer at 100% would send it.
const callerTP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func tracedEnv(t *testing.T) (*env, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	return newEnv(t, nil, WithTracerProvider(tp)), rec
}

// fromCaller makes an outgoing call carry the caller's traceparent, as a
// traced client's middleware would.
func fromCaller(ctx context.Context, tp string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "traceparent", tp)
}

func serverSpan(t *testing.T, rec *tracetest.SpanRecorder, method string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer && s.Name() == "tasklane.v1.TasklaneService/"+method {
			return s
		}
	}
	t.Fatalf("no server span for %s", method)
	return nil
}

func createQueue(t *testing.T, e *env, name string, maxAttempts int32) {
	t.Helper()
	_, err := e.client.CreateQueue(context.Background(), &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: name, MaxAttempts: maxAttempts}})
	require.NoError(t, err)
}

func storedTrace(t *testing.T, e *env, id int64) string {
	t.Helper()
	got, err := e.client.GetJob(context.Background(), &tasklanev1.GetJobRequest{Id: id})
	require.NoError(t, err)
	return got.GetJob().GetTraceContext()
}

func TestEnqueueStoresTheServerSpanAsTheJobsParent(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 0)
	res, err := e.client.Enqueue(fromCaller(context.Background(), callerTP), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)

	enq := serverSpan(t, rec, "Enqueue")
	caller, _ := traceparent.Parse(callerTP)
	require.Equal(t, caller.SpanID(), enq.Parent().SpanID())
	stored, ok := traceparent.Parse(storedTrace(t, e, res.GetJobId()))
	require.True(t, ok)
	require.Equal(t, caller.TraceID(), stored.TraceID())
	require.Equal(t, enq.SpanContext().SpanID(), stored.SpanID())
	require.True(t, stored.IsSampled())
	require.Contains(t, enq.Attributes(), attribute.String("messaging.destination.name", "q"))
}

func TestAnExplicitTraceContextWins(t *testing.T) {
	e, _ := tracedEnv(t)
	createQueue(t, e, "q", 0)
	own := "00-11111111111111111111111111111111-2222222222222222-01"
	res, err := e.client.Enqueue(fromCaller(context.Background(), callerTP), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k", TraceContext: own}})
	require.NoError(t, err)
	require.Equal(t, own, storedTrace(t, e, res.GetJobId()))
}

func TestBatchJobsShareOneParent(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 0)
	res, err := e.client.EnqueueBatch(fromCaller(context.Background(), callerTP), &tasklanev1.EnqueueBatchRequest{Queue: "q", Jobs: []*tasklanev1.JobSpec{{Kind: "a"}, {Kind: "b"}, {Kind: "c"}}})
	require.NoError(t, err)
	want := traceparent.Format(serverSpan(t, rec, "EnqueueBatch").SpanContext())
	for _, r := range res.GetResults() {
		require.Equal(t, want, storedTrace(t, e, r.GetJobId()))
	}
}

func TestAnUntracedServerStoresTheCallersTraceparent(t *testing.T) {
	e := newEnv(t, nil) // the global provider: a no-op
	createQueue(t, e, "q", 0)
	res, err := e.client.Enqueue(fromCaller(context.Background(), callerTP), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	require.Equal(t, callerTP, storedTrace(t, e, res.GetJobId()))
}

func TestNoTraceAnywhereStoresNothing(t *testing.T) {
	e := newEnv(t, nil)
	createQueue(t, e, "q", 0)
	res, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	require.Empty(t, storedTrace(t, e, res.GetJobId()))
}

func TestAnHTTPTraceparentHeaderJoinsTheTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	e := newEnv(t, nil, WithTracerProvider(tp))
	createQueue(t, e, "q", 0)
	conn, err := grpc.NewClient("passthrough:///bufnet", append(GatewayDialOptions(tp),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) }))...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	h, err := NewHTTPHandler(context.Background(), conn, func(context.Context) error { return nil }, nil, tp)
	require.NoError(t, err)
	base := serveHandler(t, h)

	req, err := http.NewRequest("POST", base+"/v1/queues/q/jobs", strings.NewReader(`{"kind":"k"}`))
	require.NoError(t, err)
	req.Header.Set("traceparent", callerTP)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	caller, _ := traceparent.Parse(callerTP)
	enq := serverSpan(t, rec, "Enqueue")
	require.Equal(t, caller.TraceID(), enq.SpanContext().TraceID())
}

func TestNackToDeadMarksTheServerSpan(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 1)
	_, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	leased, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 1})
	require.NoError(t, err)
	j := leased.GetJobs()[0]
	_, err = e.client.Nack(context.Background(), &tasklanev1.NackRequest{Id: j.GetId(), LeaseToken: j.GetLeaseToken(), Error: "boom"})
	require.NoError(t, err)
	require.Contains(t, serverSpan(t, rec, "Nack").Attributes(), attribute.Bool("tasklane.dead", true))
}

func TestHeartbeatsAreNotTraced(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 0)
	_, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	leased, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 1})
	require.NoError(t, err)
	j := leased.GetJobs()[0]
	_, err = e.client.Heartbeat(context.Background(), &tasklanev1.HeartbeatRequest{Id: j.GetId(), LeaseToken: j.GetLeaseToken()})
	require.NoError(t, err)
	for _, s := range rec.Ended() {
		require.NotContains(t, s.Name(), "Heartbeat")
	}
}

func TestHTTPHeartbeatsAreNotTraced(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	e := newEnv(t, nil, WithTracerProvider(tp))
	createQueue(t, e, "q", 0)
	_, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k"}})
	require.NoError(t, err)
	leased, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 1})
	require.NoError(t, err)
	j := leased.GetJobs()[0]
	conn, err := grpc.NewClient("passthrough:///bufnet", append(GatewayDialOptions(tp),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) }))...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	h, err := NewHTTPHandler(context.Background(), conn, func(context.Context) error { return nil }, nil, tp)
	require.NoError(t, err)
	base := serveHandler(t, h)

	url := fmt.Sprintf("%s/v1/jobs/%d:heartbeat", base, j.GetId())
	resp, err := http.Post(url, "application/json", strings.NewReader(`{"lease_token":"`+j.GetLeaseToken()+`"}`))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	for _, s := range rec.Ended() {
		// otelhttp names its span "<method> <route>", here "POST /".
		require.False(t, strings.HasPrefix(s.Name(), "POST"), "HTTP span %q", s.Name())
		require.NotContains(t, s.Name(), "Heartbeat")
	}
}

func TestLeaseRecordsAQueueWaitSpanInEachJobsTrace(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 0)
	parents := []string{
		"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01",
		"00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01",
	}
	for _, p := range parents {
		_, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k", TraceContext: p}})
		require.NoError(t, err)
	}
	leased, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 2})
	require.NoError(t, err)
	require.Len(t, leased.GetJobs(), 2)

	lease := serverSpan(t, rec, "Lease")
	var waits []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "queue wait" {
			waits = append(waits, s)
		}
	}
	require.Len(t, waits, 2)
	for _, w := range waits {
		require.NotEqual(t, lease.SpanContext().TraceID(), w.SpanContext().TraceID(), "a wait span belongs to its job's trace, not the Lease call's")
		var want trace.SpanContext
		for _, p := range parents {
			if sc, _ := traceparent.Parse(p); sc.TraceID() == w.SpanContext().TraceID() {
				want = sc
			}
		}
		require.True(t, want.IsValid(), "wait span in an unknown trace")
		require.Equal(t, want.SpanID(), w.Parent().SpanID())
		require.False(t, w.EndTime().Before(w.StartTime()))
		require.Contains(t, w.Attributes(), attribute.Int("tasklane.attempt", 1))
		require.Contains(t, w.Attributes(), attribute.String("messaging.destination.name", "q"))
	}
}

func TestNoWaitSpanForAMalformedOrUnsampledTrace(t *testing.T) {
	e, rec := tracedEnv(t)
	createQueue(t, e, "q", 0)
	for _, tc := range []string{"garbage", "00-cccccccccccccccccccccccccccccccc-3333333333333333-00"} {
		_, err := e.client.Enqueue(context.Background(), &tasklanev1.EnqueueRequest{Queue: "q", Job: &tasklanev1.JobSpec{Kind: "k", TraceContext: tc}})
		require.NoError(t, err)
	}
	leased, err := e.client.Lease(context.Background(), &tasklanev1.LeaseRequest{Queue: "q", MaxJobs: 2})
	require.NoError(t, err)
	require.Len(t, leased.GetJobs(), 2)
	for _, s := range rec.Ended() {
		require.NotEqual(t, "queue wait", s.Name())
	}
}
