package integrationtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func TestJobsFlowFromProducerToWorker(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "e2e"})
	require.NoError(t, err)

	var mu sync.Mutex
	seen := map[int]bool{}
	w := e.client.NewWorker("e2e", tasklane.WorkerOptions{Concurrency: 2, Logger: discard})
	w.Handle("record", func(_ context.Context, job *tasklane.LeasedJob) error {
		var p struct {
			N int `json:"n"`
		}
		if err := job.Decode(&p); err != nil {
			return err
		}
		mu.Lock()
		seen[p.N] = true
		mu.Unlock()
		return nil
	})
	runWorker(t, w)

	for i := range 5 {
		_, err := e.client.Enqueue(ctx, "e2e", tasklane.Job{Kind: "record", Payload: map[string]int{"n": i}})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		info, err := e.client.GetQueue(ctx, "e2e")
		return err == nil && info.Stats.RecentlyCompleted == 5 && info.Stats.Available == 0 && info.Stats.Leased == 0
	}, 10*time.Second, 50*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 5)
}

// A worker that dies holding a lease sends no heartbeats and no Ack. It is
// played here by a raw Lease that is never answered. The reaper must return
// the job, and a live worker must complete it on its second attempt.
func TestJobOfACrashedWorkerIsCompletedByAnother(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "crash", VisibilityTimeout: 300 * time.Millisecond, MaxAttempts: 3})
	require.NoError(t, err)
	res, err := e.client.Enqueue(ctx, "crash", tasklane.Job{Kind: "work"})
	require.NoError(t, err)
	leased, err := e.raw.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "crash", MaxJobs: 1})
	require.NoError(t, err)
	require.Len(t, leased.GetJobs(), 1)

	w := e.client.NewWorker("crash", tasklane.WorkerOptions{Logger: discard})
	w.Handle("work", func(context.Context, *tasklane.LeasedJob) error { return nil })
	runWorker(t, w)

	require.Eventually(t, func() bool {
		got, err := e.raw.GetJob(ctx, &tasklanev1.GetJobRequest{Id: res.JobID})
		return err == nil && got.GetJob().GetState() == tasklanev1.JobState_JOB_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond)
	got, err := e.raw.GetJob(ctx, &tasklanev1.GetJobRequest{Id: res.JobID})
	require.NoError(t, err)
	require.Equal(t, int32(2), got.GetJob().GetAttempts())
}

// A graceful shutdown nacks a job that overruns ShutdownTimeout, so it is
// available again at once instead of after its 30 s lease.
func TestShutdownReturnsAnUnfinishedJobAtOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.client.CreateQueue(ctx, tasklane.QueueConfig{Name: "deploy", VisibilityTimeout: 30 * time.Second})
	require.NoError(t, err)
	res, err := e.client.Enqueue(ctx, "deploy", tasklane.Job{Kind: "slow"})
	require.NoError(t, err)

	started := make(chan struct{})
	w := e.client.NewWorker("deploy", tasklane.WorkerOptions{ShutdownTimeout: 100 * time.Millisecond, Logger: discard})
	w.Handle("slow", func(ctx context.Context, _ *tasklane.LeasedJob) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	stop := runWorker(t, w)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the job never started")
	}
	require.NoError(t, stop())

	got, err := e.raw.GetJob(ctx, &tasklanev1.GetJobRequest{Id: res.JobID})
	require.NoError(t, err)
	require.Equal(t, tasklanev1.JobState_JOB_STATE_AVAILABLE, got.GetJob().GetState())
	require.Equal(t, "worker shutting down", got.GetJob().GetLastError())
}
