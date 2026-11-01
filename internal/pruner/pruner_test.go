package pruner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/metrics"
)

// fakeStore returns scripted counts per kind, then 0, and records each call.
type fakeStore struct {
	mu       sync.Mutex
	finished []int
	dead     []int
	err      error
	calls    []string // "finished 24h0m0s" or "dead 168h0m0s"
	called   chan struct{}
}

func newFakeStore() *fakeStore { return &fakeStore{called: make(chan struct{}, 100)} }

func (f *fakeStore) next(kind string, age time.Duration, script *[]int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, kind+" "+age.String())
	select {
	case f.called <- struct{}{}:
	default:
	}
	if f.err != nil {
		return 0, f.err
	}
	if len(*script) == 0 {
		return 0, nil
	}
	n := (*script)[0]
	*script = (*script)[1:]
	return n, nil
}

func (f *fakeStore) PruneFinished(_ context.Context, age time.Duration, _ int) (int, error) {
	return f.next("finished", age, &f.finished)
}

func (f *fakeStore) PruneDead(_ context.Context, age time.Duration, _ int) (int, error) {
	return f.next("dead", age, &f.dead)
}

func (f *fakeStore) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func TestPassDrainsFullBatchesThenStops(t *testing.T) {
	f := newFakeStore()
	f.finished = []int{2, 2, 1} // two full batches of 2, then a short one
	f.dead = []int{1}
	var logs bytes.Buffer
	cfg := Config{Retention: 24 * time.Hour, DeadRetention: 7 * 24 * time.Hour, Batch: 2}

	pass(context.Background(), f, cfg, slog.New(slog.NewTextHandler(&logs, nil)))

	require.Equal(t, []string{"finished 24h0m0s", "finished 24h0m0s", "finished 24h0m0s", "dead 168h0m0s"}, f.recorded())
	require.Contains(t, logs.String(), "count=5")
	require.Contains(t, logs.String(), "count=1")
}

func TestPassLogsErrorsAndContinuesWithTheOtherKind(t *testing.T) {
	f := newFakeStore()
	f.err = errors.New("db down")
	var logs bytes.Buffer

	pass(context.Background(), f, Config{}.withDefaults(), slog.New(slog.NewTextHandler(&logs, nil)))

	require.Len(t, f.recorded(), 2, "one failed call per kind, no retry loop")
	require.Contains(t, logs.String(), "db down")
}

func TestRunPrunesAtOnceAndStopsOnCancel(t *testing.T) {
	f := newFakeStore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, f, Config{Interval: time.Hour}, slog.New(slog.DiscardHandler))
	}()
	<-f.called // finished
	<-f.called // dead: the first pass runs immediately, not after an hour
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestConfigDefaults(t *testing.T) {
	require.Equal(t, Config{Interval: time.Minute, Retention: 24 * time.Hour, DeadRetention: 7 * 24 * time.Hour, Batch: 1000}, Config{}.withDefaults())
}

func TestPassRecordsPrunedCountsByState(t *testing.T) {
	f := newFakeStore()
	f.finished = []int{2, 1} // a full batch of 2, then a short one
	f.dead = []int{1}
	m := metrics.New()

	pass(context.Background(), f, Config{Batch: 2, Metrics: m}.withDefaults(), slog.New(slog.DiscardHandler))

	require.Equal(t, 3.0, m.Value("tasklane_jobs_pruned_total", "state", "finished"))
	require.Equal(t, 1.0, m.Value("tasklane_jobs_pruned_total", "state", "dead"))
}
