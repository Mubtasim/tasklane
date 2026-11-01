package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mubtasim/tasklane/internal/store"
)

// queueStats exposes the queue gauges from one immutable snapshot. A
// GaugeVec that is Reset and then refilled shows a scrape the empty or
// half-filled set in between; swapping a pointer lets every scrape see one
// whole refresh. Before the first refresh it emits nothing.
type queueStats struct {
	snap          atomic.Pointer[[]store.QueueDepth]
	depth, oldest *prometheus.Desc
}

func newQueueStats() *queueStats {
	return &queueStats{
		depth: prometheus.NewDesc("tasklane_queue_depth",
			"Jobs per queue and state; available includes delayed jobs.", []string{"queue", "state"}, nil),
		oldest: prometheus.NewDesc("tasklane_oldest_ready_job_age_seconds",
			"How long the oldest ready job has waited; 0 if none or paused.", []string{"queue"}, nil),
	}
}

func (q *queueStats) Describe(ch chan<- *prometheus.Desc) {
	ch <- q.depth
	ch <- q.oldest
}

func (q *queueStats) Collect(ch chan<- prometheus.Metric) {
	snap := q.snap.Load()
	if snap == nil {
		return
	}
	for _, d := range *snap {
		ch <- prometheus.MustNewConstMetric(q.depth, prometheus.GaugeValue, float64(d.Available), d.Queue, string(store.StateAvailable))
		ch <- prometheus.MustNewConstMetric(q.depth, prometheus.GaugeValue, float64(d.Leased), d.Queue, string(store.StateLeased))
		ch <- prometheus.MustNewConstMetric(q.oldest, prometheus.GaugeValue, d.OldestReady.Seconds(), d.Queue)
	}
}
