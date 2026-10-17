package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestReapExpiredRetriesJob(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	mustLeaseOne(t, s, "emails", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	n, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateAvailable, got.State)
	require.Equal(t, 1, got.Attempts, "the expired lease counts as an attempt")
	require.Equal(t, "lease expired", got.LastError)
}

func TestReapExpiredBuriesWhenOutOfAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails", MaxAttempts: 1})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	mustLeaseOne(t, s, "emails", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	_, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateDead, got.State)
}

func TestReapIgnoresLiveLeases(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	mustLeaseOne(t, s, "emails", time.Hour)

	n, err := s.ReapExpired(context.Background(), 100)
	require.NoError(t, err)
	require.Zero(t, n)
}

// The scenario from interview-notes Q1: worker A stalls past its lease, the
// reaper hands the job to worker B, then A wakes up and acks.
func TestStalledWorkerCannotAckAfterReassignment(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})

	a := mustLeaseOne(t, s, "emails", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	b := mustLeaseOne(t, s, "emails", time.Hour)

	require.Equal(t, a.ID, b.ID)
	require.NotEqual(t, a.LeaseToken, b.LeaseToken)
	require.ErrorIs(t, s.Ack(ctx, a.ID, a.LeaseToken), store.ErrLeaseLost, "A's token is fenced off")
	require.NoError(t, s.Ack(ctx, b.ID, b.LeaseToken))

	// Once B has completed the job, A's old token is still rejected and B's
	// retried ack still succeeds.
	require.ErrorIs(t, s.Ack(ctx, a.ID, a.LeaseToken), store.ErrLeaseLost, "A's token after B completed")
	require.NoError(t, s.Ack(ctx, b.ID, b.LeaseToken), "B's repeated ack")
}
