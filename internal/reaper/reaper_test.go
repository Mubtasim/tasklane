package reaper

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

// fakeStore returns scripted results, then 0, and records every call.
type fakeStore struct {
	mu      sync.Mutex
	results []result
	calls   int
	called  chan struct{}
}

type result struct {
	n   int
	err error
}

func newFakeStore(results ...result) *fakeStore {
	return &fakeStore{results: results, called: make(chan struct{}, 100)}
}

func (f *fakeStore) ReapExpired(ctx context.Context, limit int) ([]store.ReapedJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	select {
	case f.called <- struct{}{}:
	default: // the test isn't counting this many calls
	}
	if len(f.results) == 0 {
		return nil, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	return make([]store.ReapedJob, r.n), r.err
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// waitCalls waits until the store has been called n times.
func waitCalls(t *testing.T, f *fakeStore, n int) {
	t.Helper()
	for range n {
		select {
		case <-f.called:
		case <-time.After(2 * time.Second):
			t.Fatalf("store called %d times, want at least %d", f.callCount(), n)
		}
	}
}

// start runs the loop in the background and returns a stop function that
// cancels it and waits for Run to return.
func start(t *testing.T, s Store, interval time.Duration, log *slog.Logger) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, s, interval, 10, log)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestRepeatsWhileBatchesAreFull(t *testing.T) {
	f := newFakeStore(result{n: 10}, result{n: 10}, result{n: 3})
	stop := start(t, f, time.Hour, discard()) // no tick during the test
	defer stop()

	waitCalls(t, f, 3)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 3, f.callCount(), "a short batch ends the pass until the next tick")
}

func TestRunsOnEveryTick(t *testing.T) {
	f := newFakeStore()
	stop := start(t, f, 10*time.Millisecond, discard())
	defer stop()

	waitCalls(t, f, 3)
}

func TestLogsErrorsAndKeepsRunning(t *testing.T) {
	var logs syncBuffer
	f := newFakeStore(result{err: errors.New("connection refused")})
	stop := start(t, f, 10*time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil)))

	waitCalls(t, f, 2) // the tick after the error calls again
	stop()
	require.Contains(t, logs.String(), "reap expired leases failed")
	require.Contains(t, logs.String(), "connection refused")
}

func TestStopsOnCancel(t *testing.T) {
	f := newFakeStore()
	stop := start(t, f, time.Hour, discard())
	waitCalls(t, f, 1)
	stop() // fails the test if Run doesn't return
}

// fullStore always reports a full batch, as if the backlog never ended.
type fullStore struct{ calls atomic.Int64 }

func (f *fullStore) ReapExpired(ctx context.Context, limit int) ([]store.ReapedJob, error) {
	f.calls.Add(1)
	return make([]store.ReapedJob, limit), nil
}

func TestAlwaysFullBatchStopsOnCancel(t *testing.T) {
	f := &fullStore{}
	stop := start(t, f, time.Hour, discard())
	for f.calls.Load() < 100 {
		runtime.Gosched()
	}
	stop() // fails the test if Run doesn't return
}

func TestPersistentErrorWaitsForTheTick(t *testing.T) {
	f := newFakeStore()
	f.results = make([]result, 1000)
	for i := range f.results {
		f.results[i] = result{err: errors.New("connection refused")}
	}
	stop := start(t, f, time.Hour, discard())
	waitCalls(t, f, 1)
	time.Sleep(100 * time.Millisecond)
	stop()
	require.Equal(t, 1, f.callCount(), "an error ends the pass until the next tick")
}

// A zero batch or interval falls back to the defaults instead of spinning
// (batch 0: every pass is "full") or panicking (NewTicker(0)).
func TestNonPositiveSettingsUseDefaults(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		batch    int
	}{{0, 10}, {-time.Second, 10}, {time.Hour, 0}, {time.Hour, -1}} {
		f := newFakeStore()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			Run(ctx, f, tc.interval, tc.batch, discard())
		}()
		waitCalls(t, f, 1)
		time.Sleep(100 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
		require.Equal(t, 1, f.callCount(), "interval %v batch %d", tc.interval, tc.batch)
	}
}
