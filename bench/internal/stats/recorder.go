// Package stats holds the load generator's latency recorder and its CSV
// output.
package stats

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// The histogram stores microseconds from 1 µs to 10 minutes with 3
// significant digits: about 0.1% error at any value, in a fixed ~200 KB,
// however many samples arrive. A sorted slice of samples would grow by 8
// bytes per job (millions per suite).
const (
	minMicros = 1
	maxMicros = int64(10 * time.Minute / time.Microsecond)
)

// Recorder collects durations from many goroutines.
type Recorder struct {
	mu sync.Mutex
	h  *hdrhistogram.Histogram
}

func NewRecorder() *Recorder {
	return &Recorder{h: hdrhistogram.New(minMicros, maxMicros, 3)}
}

// Record adds one duration. Values outside the range are clamped rather than
// dropped, so a clock oddity or a stuck job still counts as a sample.
func (r *Recorder) Record(d time.Duration) {
	us := min(max(d.Microseconds(), minMicros), maxMicros)
	r.mu.Lock()
	_ = r.h.RecordValue(us) // in range by construction
	r.mu.Unlock()
}

// Summary is a recorder's percentiles.
type Summary struct {
	Count               int64
	P50, P99, P999, Max time.Duration
}

func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.h.TotalCount() == 0 {
		return Summary{}
	}
	us := func(v int64) time.Duration { return time.Duration(v) * time.Microsecond }
	return Summary{
		Count: r.h.TotalCount(),
		P50:   us(r.h.ValueAtQuantile(50)),
		P99:   us(r.h.ValueAtQuantile(99)),
		P999:  us(r.h.ValueAtQuantile(99.9)),
		Max:   us(r.h.Max()),
	}
}
