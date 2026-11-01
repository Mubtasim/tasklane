// Package metrics defines Tasklane's Prometheus metrics (design §16.2) on a
// registry of their own, so tests can create a fresh one. Every method is a
// no-op on a nil *Metrics, so code and tests that don't care about metrics
// pass nil.
package metrics

import (
	"context"
	"net/http"
	"path"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/internal/store"
)

const (
	PrunedFinished = "finished" // completed and cancelled jobs
	PrunedDead     = "dead"
)

// rpcBuckets reach 30 s because a long-polling Lease waits up to MaxWait.
var rpcBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// waitBuckets cover a job picked up at once up to one waiting an hour.
var waitBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

type Metrics struct {
	reg *prometheus.Registry

	enqueued, leased, acked, nacked, cancelled, redriven, reaped, refused *prometheus.CounterVec
	dead, pruned                                                          *prometheus.CounterVec
	rpc, wait                                                             *prometheus.HistogramVec
	depth, oldest                                                         *prometheus.GaugeVec

	start   time.Time
	statsAt atomic.Int64 // UnixNano of the last SetQueueStats; 0 before the first
}

func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry(), start: time.Now()}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		m.reg.MustRegister(c)
		return c
	}
	m.enqueued = counter("tasklane_jobs_enqueued_total", "Jobs created by Enqueue and EnqueueBatch.", "queue")
	m.leased = counter("tasklane_jobs_leased_total", "Jobs leased to workers.", "queue")
	m.acked = counter("tasklane_jobs_acked_total", "Jobs completed by Ack.", "queue")
	m.nacked = counter("tasklane_jobs_nacked_total", "Nacks that scheduled a retry.", "queue")
	m.dead = counter("tasklane_jobs_dead_total", "Jobs that reached dead, by how.", "queue", "reason")
	m.reaped = counter("tasklane_jobs_reaped_total", "Expired leases handled by the reaper.", "queue")
	m.cancelled = counter("tasklane_jobs_cancelled_total", "Jobs cancelled.", "queue")
	m.redriven = counter("tasklane_jobs_redriven_total", "Dead jobs made available again.", "queue")
	m.pruned = counter("tasklane_jobs_pruned_total", "Jobs deleted by the pruner.", "state")
	m.refused = counter("tasklane_enqueue_refused_total", "Enqueue calls refused because the queue was full.", "queue")
	m.pruned.WithLabelValues(PrunedFinished)
	m.pruned.WithLabelValues(PrunedDead)

	m.rpc = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tasklane_rpc_duration_seconds", Help: "RPC duration, including a Lease's long-poll wait.", Buckets: rpcBuckets,
	}, []string{"method", "code"})
	m.wait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tasklane_queue_wait_seconds", Help: "Time from a job becoming ready (run_at) to its lease.", Buckets: waitBuckets,
	}, []string{"queue"})
	m.depth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tasklane_queue_depth", Help: "Jobs per queue and state; available includes delayed jobs.",
	}, []string{"queue", "state"})
	m.oldest = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tasklane_oldest_ready_job_age_seconds", Help: "How long the oldest ready job has waited; 0 if none or paused.",
	}, []string{"queue"})
	m.reg.MustRegister(m.rpc, m.wait, m.depth, m.oldest,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "tasklane_queue_stats_age_seconds", Help: "Seconds since the queue gauges were last refreshed.",
		}, m.statsAge),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Metrics) statsAge() float64 {
	since := m.start
	if ns := m.statsAt.Load(); ns != 0 {
		since = time.Unix(0, ns)
	}
	return time.Since(since).Seconds()
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// UnaryInterceptor times every unary RPC by method and status code. It goes
// first in the chain, so refused calls (bad API key) are counted too.
func (m *Metrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if m == nil {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		m.rpc.WithLabelValues(path.Base(info.FullMethod), status.Code(err).String()).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

func (m *Metrics) Enqueued(queue string, created int) {
	if m != nil && created > 0 {
		m.enqueued.WithLabelValues(queue).Add(float64(created))
	}
}

// Leased counts the jobs and records how long each waited after becoming
// ready. Both times are database times; a negative wait (a job leased the
// instant it became ready) is recorded as 0.
func (m *Metrics) Leased(jobs []store.Job) {
	if m == nil {
		return
	}
	for _, j := range jobs {
		m.leased.WithLabelValues(j.Queue).Inc()
		if !j.LeasedAt.IsZero() {
			m.wait.WithLabelValues(j.Queue).Observe(max(0, j.LeasedAt.Sub(j.RunAt).Seconds()))
		}
	}
}

func (m *Metrics) Acked(queue string) {
	if m != nil {
		m.acked.WithLabelValues(queue).Inc()
	}
}

func (m *Metrics) Nacked(queue string, dead bool) {
	switch {
	case m == nil:
	case dead:
		m.dead.WithLabelValues(queue, "nacked").Inc()
	default:
		m.nacked.WithLabelValues(queue).Inc()
	}
}

func (m *Metrics) Reaped(jobs []store.ReapedJob) {
	if m == nil {
		return
	}
	for _, j := range jobs {
		m.reaped.WithLabelValues(j.Queue).Inc()
		if j.Dead {
			m.dead.WithLabelValues(j.Queue, "expired").Inc()
		}
	}
}

func (m *Metrics) Cancelled(queue string) {
	if m != nil {
		m.cancelled.WithLabelValues(queue).Inc()
	}
}

func (m *Metrics) Redriven(queue string, n int) {
	if m != nil && n > 0 {
		m.redriven.WithLabelValues(queue).Add(float64(n))
	}
}

func (m *Metrics) Pruned(state string, n int) {
	if m != nil && n > 0 {
		m.pruned.WithLabelValues(state).Add(float64(n))
	}
}

func (m *Metrics) EnqueueRefused(queue string) {
	if m != nil {
		m.refused.WithLabelValues(queue).Inc()
	}
}

// SetQueueStats replaces the queue gauges with one refresh's counts, so a
// deleted queue's series disappear, and creates every known queue's counter
// series at 0: graphs then show 0 instead of no data, and increase() sees a
// queue's first event (a series born at 1 has no increase).
func (m *Metrics) SetQueueStats(depths []store.QueueDepth) {
	if m == nil {
		return
	}
	m.depth.Reset()
	m.oldest.Reset()
	for _, d := range depths {
		m.depth.WithLabelValues(d.Queue, string(store.StateAvailable)).Set(float64(d.Available))
		m.depth.WithLabelValues(d.Queue, string(store.StateLeased)).Set(float64(d.Leased))
		m.oldest.WithLabelValues(d.Queue).Set(d.OldestReady.Seconds())
		for _, c := range []*prometheus.CounterVec{m.enqueued, m.leased, m.acked, m.nacked, m.reaped, m.cancelled, m.redriven, m.refused} {
			c.WithLabelValues(d.Queue)
		}
		m.dead.WithLabelValues(d.Queue, "nacked")
		m.dead.WithLabelValues(d.Queue, "expired")
	}
	m.statsAt.Store(time.Now().UnixNano())
}
