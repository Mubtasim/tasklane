package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

var (
	quiet = slog.New(slog.DiscardHandler)
	job   = &tasklane.LeasedJob{ID: 1, Kind: "test", Attempt: 1, MaxAttempts: 5}
)

func TestFlakyFailsWhenTheRollIsBelowTheRate(t *testing.T) {
	require.Error(t, flaky(quiet, 0.3, func() float64 { return 0.1 })(context.Background(), job))
	require.NoError(t, flaky(quiet, 0.3, func() float64 { return 0.5 })(context.Background(), job))
}

func TestPoisonAlwaysFails(t *testing.T) {
	require.Error(t, poison(quiet)(context.Background(), job))
}

func TestNormalStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	require.ErrorIs(t, normal(quiet)(ctx, job), context.Canceled)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}
