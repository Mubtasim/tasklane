package tasklane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

type emailPayload struct {
	To      string `json:"to"`
	Retries int    `json:"retries"`
}

func TestDialSendsTheAPIKey(t *testing.T) {
	f, c := newFake(t, WithAPIKey("secret"))

	_, err := c.GetQueue(context.Background(), "q")
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer secret"}, f.calls().auth)
}

func TestDialWithoutAPIKeySendsNoHeader(t *testing.T) {
	f, c := newFake(t)

	_, err := c.GetQueue(context.Background(), "q")
	require.NoError(t, err)
	require.Equal(t, []string{""}, f.calls().auth)
}

func TestEnqueueConvertsTheJob(t *testing.T) {
	f, c := newFake(t)
	runAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	res, err := c.Enqueue(context.Background(), "emails", Job{
		Kind:           "send_email",
		Payload:        emailPayload{To: "a@example.com", Retries: 2},
		Priority:       PriorityHigh,
		RunAt:          runAt,
		IdempotencyKey: "order-1",
		MaxAttempts:    7,
	})
	require.NoError(t, err)
	require.Equal(t, EnqueueResult{JobID: 1, Created: true}, res)

	got := f.calls().enqueues
	require.Len(t, got, 1)
	require.Equal(t, "emails", got[0].GetQueue())
	spec := got[0].GetJob()
	require.Equal(t, "send_email", spec.GetKind())
	require.Equal(t, map[string]any{"to": "a@example.com", "retries": float64(2)}, spec.GetPayload().AsMap())
	require.Equal(t, tasklanev1.Priority_PRIORITY_HIGH, spec.GetPriority())
	require.True(t, runAt.Equal(spec.GetRunAt().AsTime()))
	require.Equal(t, "order-1", spec.GetIdempotencyKey())
	require.Equal(t, int32(7), spec.GetMaxAttempts())
}

func TestEnqueueWithoutPayloadOrRunAt(t *testing.T) {
	f, c := newFake(t)

	_, err := c.Enqueue(context.Background(), "emails", Job{Kind: "ping"})
	require.NoError(t, err)
	spec := f.calls().enqueues[0].GetJob()
	require.Nil(t, spec.GetPayload())
	require.Nil(t, spec.GetRunAt())
	require.Equal(t, tasklanev1.Priority_PRIORITY_UNSPECIFIED, spec.GetPriority())
}

func TestEnqueueRejectsAPayloadThatIsNotAnObject(t *testing.T) {
	f, c := newFake(t)

	_, err := c.Enqueue(context.Background(), "emails", Job{Kind: "k", Payload: []int{1, 2}})
	require.ErrorContains(t, err, "JSON object")
	require.Empty(t, f.calls().enqueues, "nothing is sent")
}

func TestEnqueueBatchKeepsOrder(t *testing.T) {
	f, c := newFake(t)

	res, err := c.EnqueueBatch(context.Background(), "emails", []Job{{Kind: "a"}, {Kind: "b"}})
	require.NoError(t, err)
	require.Equal(t, []EnqueueResult{{JobID: 1, Created: true}, {JobID: 2, Created: true}}, res)
	jobs := f.calls().batches[0].GetJobs()
	require.Equal(t, "a", jobs[0].GetKind())
	require.Equal(t, "b", jobs[1].GetKind())
}

func TestEnqueueBatchNamesTheBadJob(t *testing.T) {
	_, c := newFake(t)

	_, err := c.EnqueueBatch(context.Background(), "emails", []Job{{Kind: "a"}, {Kind: "b", Payload: "text"}})
	require.ErrorContains(t, err, "job 1")
}

func TestCreateQueue(t *testing.T) {
	f, c := newFake(t)

	got, err := c.CreateQueue(context.Background(), QueueConfig{Name: "demo", VisibilityTimeout: 15 * time.Second, MaxAttempts: 5})
	require.NoError(t, err)
	require.Equal(t, QueueConfig{Name: "demo", VisibilityTimeout: 15 * time.Second, MaxAttempts: 5}, got)

	_, err = c.CreateQueue(context.Background(), QueueConfig{Name: "defaults"})
	require.NoError(t, err)
	require.Nil(t, f.calls().createQueues[1].GetQueue().GetVisibilityTimeout(), "zero means the server default")
}

func TestGetQueueReturnsConfigAndStats(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) {
		f.stats = &tasklanev1.QueueStats{Available: 120, Leased: 15, Dead: 4, RecentlyCompleted: 420, RecentWindow: durationpb.New(10 * time.Second)}
	})

	info, err := c.GetQueue(context.Background(), "q")
	require.NoError(t, err)
	require.Equal(t, QueueConfig{Name: "q", MaxDepth: 100_000, VisibilityTimeout: 30 * time.Second, MaxAttempts: 5}, info.Config)
	require.Equal(t, QueueStats{Available: 120, Leased: 15, Dead: 4, RecentlyCompleted: 420, RecentWindow: 10 * time.Second}, info.Stats)
}

func TestServerErrorsAreStatusErrors(t *testing.T) {
	f, c := newFake(t)
	f.set(func(f *fakeServer) { f.queue = nil })

	_, err := c.GetQueue(context.Background(), "q")
	require.Equal(t, codes.NotFound, status.Code(err))
}
