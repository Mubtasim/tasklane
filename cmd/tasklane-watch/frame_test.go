package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func TestFrameShowsCountsAndCompletionRate(t *testing.T) {
	got := frame("demo", tasklane.QueueStats{Available: 120, Leased: 15, Dead: 4, RecentlyCompleted: 420, RecentWindow: 10 * time.Second}, nil)
	require.Equal(t, "queue=demo  available 120 | leased 15 | dead 4 | completed 42.0/s", got)
}

func TestFrameWithoutAWindowShowsZeroRate(t *testing.T) {
	got := frame("demo", tasklane.QueueStats{RecentlyCompleted: 5}, nil)
	require.Equal(t, "queue=demo  available 0 | leased 0 | dead 0 | completed 0.0/s", got)
}

func TestFrameShowsErrors(t *testing.T) {
	got := frame("demo", tasklane.QueueStats{}, status.Error(codes.Unavailable, "connection refused"))
	require.Equal(t, "queue=demo  Unavailable: connection refused (retrying)", got)
}
