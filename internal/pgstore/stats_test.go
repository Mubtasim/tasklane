package pgstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestQueueStatsCountsEachState(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "q", MaxAttempts: 1})
	mustQueue(t, s, store.Queue{Name: "other"})
	for range 6 {
		mustEnqueue(t, s, store.NewJob{Queue: "q", Kind: "k"})
	}
	mustEnqueue(t, s, store.NewJob{Queue: "other", Kind: "k"}) // must not be counted

	leased, err := s.Lease(ctx, "q", 4, 0)
	require.NoError(t, err)
	require.Len(t, leased, 4)
	require.NoError(t, s.Ack(ctx, leased[0].ID, leased[0].LeaseToken))
	require.NoError(t, s.Ack(ctx, leased[1].ID, leased[1].LeaseToken))
	// max_attempts 1: the first failure is final.
	require.NoError(t, s.Nack(ctx, leased[2].ID, leased[2].LeaseToken, "boom", nil))
	// One completion falls outside the window.
	_, err = testPool.Exec(ctx, `UPDATE jobs SET finished_at = now() - interval '1 minute' WHERE id = $1`, leased[1].ID)
	require.NoError(t, err)

	st, err := s.QueueStats(ctx, "q")
	require.NoError(t, err)
	require.Equal(t, store.QueueStats{Available: 2, Leased: 1, Dead: 1, RecentlyCompleted: 1}, st)
}

func TestQueueStatsOfEmptyQueue(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "q"})

	st, err := s.QueueStats(context.Background(), "q")
	require.NoError(t, err)
	require.Equal(t, store.QueueStats{}, st)
}

func TestQueueStatsOfUnknownQueue(t *testing.T) {
	s := newStore(t)

	_, err := s.QueueStats(context.Background(), "nope")
	require.ErrorIs(t, err, store.ErrNotFound)
}
