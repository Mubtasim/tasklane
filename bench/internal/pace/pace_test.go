package pace

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/bench/internal/stats"
)

func TestPlannedSpacesEventsEvenly(t *testing.T) {
	start := time.Unix(0, 0)
	require.Equal(t, start, Planned(start, 100, 0))
	require.Equal(t, start.Add(10*time.Millisecond), Planned(start, 100, 1))
	require.Equal(t, start.Add(time.Second), Planned(start, 100, 100))
}

func TestRunKeepsTheRate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var n atomic.Int64
	sent, dropped := Run(ctx, time.Now(), 200, 100, func(time.Time) { n.Add(1) })
	require.Zero(t, dropped)
	require.Equal(t, sent, n.Load(), "every sent event was fired")
	require.InDelta(t, 200, sent, 20)
}

// A server that stalls for 300 ms must show up in p99: the sends planned
// during the stall still go out on time, and each one's latency counts from
// its planned time. A closed loop would record a single slow sample.
func TestRunExposesAServerStall(t *testing.T) {
	start := time.Now()
	stallFrom, stallTo := start.Add(200*time.Millisecond), start.Add(500*time.Millisecond)
	rec := stats.NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sent, dropped := Run(ctx, start, 100, 1000, func(planned time.Time) {
		if now := time.Now(); now.After(stallFrom) && now.Before(stallTo) {
			time.Sleep(time.Until(stallTo)) // the server answers nothing until the stall ends
		}
		rec.Record(time.Since(planned))
	})
	require.Zero(t, dropped)
	require.InDelta(t, 100, sent, 10, "the schedule was kept through the stall")
	s := rec.Summary()
	require.Greater(t, s.P99, 250*time.Millisecond, "the stall reached p99")
	require.Less(t, s.P50, 50*time.Millisecond, "most sends were unaffected")
}

func TestRunDropsWhenInFlightIsFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	go func() { <-ctx.Done(); once.Do(func() { close(release) }) }()
	sent, dropped := Run(ctx, time.Now(), 100, 5, func(time.Time) { <-release })
	require.Equal(t, int64(5), sent)
	require.Greater(t, dropped, int64(20))
}

func TestRunWaitsForInFlightCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var done atomic.Int64
	Run(ctx, time.Now(), 100, 10, func(time.Time) {
		time.Sleep(100 * time.Millisecond)
		done.Add(1)
	})
	require.Positive(t, done.Load(), "Run returned only after its calls finished")
}
