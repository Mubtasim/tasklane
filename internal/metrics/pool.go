package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// WatchPool exports the database pool's stats. stat (pgxpool.Pool.Stat) is
// called on every scrape.
func (m *Metrics) WatchPool(stat func() *pgxpool.Stat) {
	if m == nil || stat == nil {
		return
	}
	gauge := func(name, help string, read func(*pgxpool.Stat) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return read(stat()) })
	}
	m.reg.MustRegister(
		gauge("tasklane_db_pool_acquired_connections", "Connections in use.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }),
		gauge("tasklane_db_pool_idle_connections", "Idle connections.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }),
		gauge("tasklane_db_pool_max_connections", "The pool's size limit.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "tasklane_db_pool_empty_acquire_total", Help: "Acquires that had to wait for a connection.",
		}, func() float64 { return float64(stat().EmptyAcquireCount()) }),
	)
}
