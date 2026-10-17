package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/store"
)

func TestAckCompletesJob(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 0)

	require.NoError(t, s.Ack(ctx, j.ID, j.LeaseToken))

	got, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCompleted, got.State)
	require.False(t, got.FinishedAt.IsZero())
	require.Equal(t, uuid.Nil, got.LeaseToken)
}

func TestAckRequiresCurrentToken(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 0)

	require.ErrorIs(t, s.Ack(ctx, j.ID, uuid.New()), store.ErrLeaseLost)
	require.ErrorIs(t, s.Ack(ctx, 999, j.LeaseToken), store.ErrNotFound)
	require.NoError(t, s.Ack(ctx, j.ID, j.LeaseToken))
	require.ErrorIs(t, s.Ack(ctx, j.ID, j.LeaseToken), store.ErrLeaseLost, "double ack")
}

// A late ack is accepted while nobody else holds the job (design §5).
func TestLateAckBeforeReapSucceeds(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, s.Ack(context.Background(), j.ID, j.LeaseToken))
}

func TestHeartbeatExtendsLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 50*time.Millisecond)

	exp, err := s.Heartbeat(ctx, j.ID, j.LeaseToken, time.Hour)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Hour), exp, 5*time.Second)

	_, err = s.Heartbeat(ctx, j.ID, uuid.New(), time.Hour)
	require.ErrorIs(t, err, store.ErrLeaseLost)
}

func TestNackRetriesThenBuries(t *testing.T) {
	s := newStore(t) // zero backoff: retries are immediately leasable
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails", MaxAttempts: 2})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})

	j := mustLeaseOne(t, s, "emails", 0)
	require.NoError(t, s.Nack(ctx, j.ID, j.LeaseToken, "smtp timeout", nil))
	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateAvailable, got.State)
	require.Equal(t, "smtp timeout", got.LastError)

	j = mustLeaseOne(t, s, "emails", 0)
	require.Equal(t, 2, j.Attempts)
	require.NoError(t, s.Nack(ctx, j.ID, j.LeaseToken, "smtp timeout again", nil))
	got, err = s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateDead, got.State)
	require.Equal(t, "smtp timeout again", got.LastError)
	require.False(t, got.FinishedAt.IsZero())
}

func TestNackUsesBackoffOrExplicitRetryAt(t *testing.T) {
	// Rand always 1, so the first retry waits exactly base·2^1 = 2 s.
	s := newStoreWithBackoff(t, backoff.Policy{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 1 }})
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	a := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "a"})
	b := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "b"})

	jobs, err := s.Lease(ctx, "emails", 2, 0)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	retryAt := time.Now().Add(10 * time.Minute)
	require.NoError(t, s.Nack(ctx, jobs[0].ID, jobs[0].LeaseToken, "boom", nil))
	require.NoError(t, s.Nack(ctx, jobs[1].ID, jobs[1].LeaseToken, "rate limited", &retryAt))

	gotA, err := s.GetJob(ctx, a)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(2*time.Second), gotA.RunAt, time.Second)
	gotB, err := s.GetJob(ctx, b)
	require.NoError(t, err)
	require.WithinDuration(t, retryAt, gotB.RunAt, time.Millisecond)

	none, err := s.Lease(ctx, "emails", 2, 0)
	require.NoError(t, err)
	require.Empty(t, none, "both jobs are waiting out their delay")
}

func TestNackRequiresCurrentToken(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 0)

	require.ErrorIs(t, s.Nack(context.Background(), j.ID, uuid.New(), "x", nil), store.ErrLeaseLost)
}
