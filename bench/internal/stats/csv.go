package stats

import (
	"encoding/csv"
	"errors"
	"os"
	"strconv"
	"time"
)

// Row is one run's result. Durations are written in milliseconds and CPU in
// milliseconds per job, so the CSV is easy to plot and to read by eye.
type Row struct {
	Scenario   string // s1 … s6
	Label      string // unique per run, for example s2-open80-w50
	Mode       string // closed, open, fill, redrive
	Batch      int
	Workers    int // worker slots
	Payload    int // payload size in bytes
	TargetRate float64
	Window     time.Duration

	Enqueued   int64 // jobs enqueued inside the window
	Acked      int64 // jobs acked inside the window
	Throughput float64

	E2S     Summary // enqueue-to-start latency
	Enqueue Summary // enqueue RPC latency (from the scheduled time)

	ServerCPUPerJob  float64 // ms of CPU per job
	PGCPUPerJob      float64
	LoadgenCPUPerJob float64
	ServerCPUPct     float64 // % of one CPU over the window (for idle runs)
	PGCPUPct         float64
	WALBytesPerJob   float64
	LeaseRPCsPerJob  float64
	PGQueriesPerSec  float64

	Errors  int64
	Dropped int64
	Valid   bool
	Reason  string
}

func Header() []string {
	return []string{
		"scenario", "label", "mode", "batch", "workers", "payload_bytes", "target_rate", "window_s",
		"enqueued", "acked", "throughput_jobs_s",
		"e2s_count", "e2s_p50_ms", "e2s_p99_ms", "e2s_p999_ms", "e2s_max_ms",
		"enq_p50_ms", "enq_p99_ms", "enq_max_ms",
		"server_cpu_ms_per_job", "pg_cpu_ms_per_job", "loadgen_cpu_ms_per_job",
		"server_cpu_pct", "pg_cpu_pct",
		"wal_bytes_per_job", "lease_rpcs_per_job", "pg_queries_per_s",
		"errors", "dropped", "valid", "reason",
	}
}

func (r Row) Values() []string {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	ms := func(d time.Duration) string { return f(float64(d.Microseconds()) / 1000) }
	i := func(v int64) string { return strconv.FormatInt(v, 10) }
	return []string{
		r.Scenario, r.Label, r.Mode, i(int64(r.Batch)), i(int64(r.Workers)), i(int64(r.Payload)), f(r.TargetRate), f(r.Window.Seconds()),
		i(r.Enqueued), i(r.Acked), f(r.Throughput),
		i(r.E2S.Count), ms(r.E2S.P50), ms(r.E2S.P99), ms(r.E2S.P999), ms(r.E2S.Max),
		ms(r.Enqueue.P50), ms(r.Enqueue.P99), ms(r.Enqueue.Max),
		f(r.ServerCPUPerJob), f(r.PGCPUPerJob), f(r.LoadgenCPUPerJob),
		f(r.ServerCPUPct), f(r.PGCPUPct),
		f(r.WALBytesPerJob), f(r.LeaseRPCsPerJob), f(r.PGQueriesPerSec),
		i(r.Errors), i(r.Dropped), strconv.FormatBool(r.Valid), r.Reason,
	}
}

// AppendRow adds r to the CSV at path, writing the header first if the file
// is new or empty, so every run of the suite appends to one file.
func AppendRow(path string, r Row) error {
	st, err := os.Stat(path)
	needHeader := errors.Is(err, os.ErrNotExist) || (err == nil && st.Size() == 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if needHeader {
		_ = w.Write(Header())
	}
	_ = w.Write(r.Values())
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
