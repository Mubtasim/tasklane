package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestQueueDepthsCountsAvailableAndLeased(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails", MaxDepth: 10})
	mustQueue(t, s, store.Queue{Name: "empty"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k", RunAt: time.Now().Add(time.Hour)}) // delayed: counts
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	mustLeaseOne(t, s, "emails", time.Minute) // leased: counts
	done := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	finish(t, done, store.StateCompleted, time.Minute) // completed: doesn't

	depths, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []store.QueueDepth{
		{Queue: "emails", Depth: 3, MaxDepth: 10},
		{Queue: "empty", Depth: 0, MaxDepth: 100_000},
	}, depths)
}
