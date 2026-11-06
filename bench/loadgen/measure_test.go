package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWindowContains(t *testing.T) {
	start := time.Unix(100, 0)
	w := window{start: start, end: start.Add(time.Minute)}
	require.True(t, w.contains(start))
	require.True(t, w.contains(start.Add(59*time.Second)))
	require.False(t, w.contains(start.Add(-time.Nanosecond)), "warm-up")
	require.False(t, w.contains(start.Add(time.Minute)), "end is exclusive")
	require.False(t, window{}.contains(start), "before the window is set")
}

func TestVerdict(t *testing.T) {
	ok, reason := verdict(counts{sends: 1000, errors: 10}, nil)
	require.True(t, ok, reason)
	ok, reason = verdict(counts{sends: 1000, errors: 11}, nil)
	require.False(t, ok)
	require.Contains(t, reason, "1.1% failed sends")
	ok, reason = verdict(counts{sends: 1000, dropped: 1}, nil)
	require.False(t, ok)
	require.Contains(t, reason, "dropped")
	ok, reason = verdict(counts{}, []string{"cgroup-server: no such file"})
	require.False(t, ok)
	require.Contains(t, reason, "cgroup-server")
	ok, reason = verdict(counts{sends: 10, notStarted: 7}, nil)
	require.False(t, ok)
	require.Contains(t, reason, "7 in-window jobs never started")
	ok, _ = verdict(counts{}, nil)
	require.True(t, ok, "an idle run with no sends is valid")
}

func TestPerJob(t *testing.T) {
	require.Equal(t, 2.5, perJob(5*time.Second, 2000))
	require.Zero(t, perJob(time.Second, 0), "no jobs: zero, not a division by zero")
}
