package metrics

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestNilMetricsIsANoOp(t *testing.T) {
	var m *Metrics
	m.Enqueued("q", 1)
	m.Leased([]store.Job{{Queue: "q"}})
	m.Acked("q")
	m.Nacked("q", true)
	m.Reaped([]store.ReapedJob{{Queue: "q"}})
	m.Cancelled("q")
	m.Redriven("q", 2)
	m.Pruned(PrunedDead, 3)
	m.EnqueueRefused("q")
	m.SetQueueStats([]store.QueueDepth{{Queue: "q"}})
	m.WatchPool(nil)
	resp, err := m.UnaryInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"},
		func(context.Context, any) (any, error) { return "ok", nil })
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
	require.Zero(t, m.Value("tasklane_jobs_acked_total", "queue", "q"))
}

func TestCounters(t *testing.T) {
	m := New()
	m.Enqueued("q", 3)
	m.Enqueued("q", 0) // an idempotent hit creates nothing
	m.Acked("q")
	m.Nacked("q", false)
	m.Nacked("q", true)
	m.Reaped([]store.ReapedJob{{Queue: "q"}, {Queue: "q", Dead: true}})
	m.Cancelled("q")
	m.Redriven("q", 5)
	m.Pruned(PrunedFinished, 7)
	m.EnqueueRefused("q")

	require.Equal(t, 3.0, m.Value("tasklane_jobs_enqueued_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_acked_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_nacked_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_dead_total", "queue", "q", "reason", "nacked"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_dead_total", "queue", "q", "reason", "expired"))
	require.Equal(t, 2.0, m.Value("tasklane_jobs_reaped_total", "queue", "q"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_cancelled_total", "queue", "q"))
	require.Equal(t, 5.0, m.Value("tasklane_jobs_redriven_total", "queue", "q"))
	require.Equal(t, 7.0, m.Value("tasklane_jobs_pruned_total", "state", "finished"))
	require.Equal(t, 1.0, m.Value("tasklane_enqueue_refused_total", "queue", "q"))
}

func TestLeasedRecordsQueueWait(t *testing.T) {
	m := New()
	now := time.Now()
	m.Leased([]store.Job{
		{Queue: "q", RunAt: now.Add(-2 * time.Second), LeasedAt: now},
		{Queue: "q", RunAt: now.Add(time.Second), LeasedAt: now}, // clock noise: never negative
	})
	require.Equal(t, 2.0, m.Value("tasklane_jobs_leased_total", "queue", "q"))
	require.Equal(t, 2.0, m.Value("tasklane_queue_wait_seconds", "queue", "q"), "two samples")
}

func TestSetQueueStatsReplacesGaugesAndZeroesCounters(t *testing.T) {
	m := New()
	m.SetQueueStats([]store.QueueDepth{
		{Queue: "a", Available: 4, Leased: 2, OldestReady: 90 * time.Second},
		{Queue: "gone", Available: 1},
	})
	require.Equal(t, 4.0, m.Value("tasklane_queue_depth", "queue", "a", "state", "available"))
	require.Equal(t, 2.0, m.Value("tasklane_queue_depth", "queue", "a", "state", "leased"))
	require.Equal(t, 90.0, m.Value("tasklane_oldest_ready_job_age_seconds", "queue", "a"))
	require.True(t, m.has("tasklane_jobs_dead_total", "queue", "a", "reason", "expired"), "counter series exist at 0")

	m.SetQueueStats([]store.QueueDepth{{Queue: "a", Available: 1}})
	require.False(t, m.has("tasklane_queue_depth", "queue", "gone", "state", "available"), "a deleted queue's gauges disappear")
	require.Less(t, m.Value("tasklane_queue_stats_age_seconds"), 1.0)
}

func TestUnaryInterceptorRecordsMethodAndCode(t *testing.T) {
	m := New()
	info := &grpc.UnaryServerInfo{FullMethod: "/tasklane.v1.TasklaneService/Ack"}
	_, _ = m.UnaryInterceptor()(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return nil, status.Error(codes.FailedPrecondition, "stale") })
	_, _ = m.UnaryInterceptor()(context.Background(), nil, info,
		func(context.Context, any) (any, error) { return nil, errors.New("plain") })
	require.Equal(t, 1.0, m.Value("tasklane_rpc_duration_seconds", "method", "Ack", "code", "FailedPrecondition"))
	require.Equal(t, 1.0, m.Value("tasklane_rpc_duration_seconds", "method", "Ack", "code", "Unknown"))
}

func TestHandlerServesTextFormat(t *testing.T) {
	m := New()
	m.Acked("q")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	require.Equal(t, 200, rec.Code)
	require.True(t, strings.Contains(string(body), `tasklane_jobs_acked_total{queue="q"} 1`))
	require.True(t, strings.Contains(string(body), "go_goroutines"))
}

// A scrape must see one refresh's gauges or the next one's, never a mix or
// an empty set (Reset followed by Set used to leave that window).
func TestQueueStatsScrapeIsAtomic(t *testing.T) {
	m := New()
	a := []store.QueueDepth{{Queue: "a", Available: 1}, {Queue: "b", Available: 2}}
	b := []store.QueueDepth{{Queue: "c", Available: 3}, {Queue: "d", Available: 4}}
	m.SetQueueStats(a)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				m.SetQueueStats(b)
			} else {
				m.SetQueueStats(a)
			}
		}
	}()
	for range 2000 {
		families, err := m.reg.Gather()
		require.NoError(t, err)
		queues := map[string]bool{}
		for _, f := range families {
			if f.GetName() != "tasklane_queue_depth" {
				continue
			}
			for _, s := range f.GetMetric() {
				for _, l := range s.GetLabel() {
					if l.GetName() == "queue" {
						queues[l.GetValue()] = true
					}
				}
			}
		}
		ok := (queues["a"] && queues["b"] && len(queues) == 2) || (queues["c"] && queues["d"] && len(queues) == 2)
		require.True(t, ok, "mixed or empty scrape: %v", queues)
	}
	close(stop)
	<-done
}
