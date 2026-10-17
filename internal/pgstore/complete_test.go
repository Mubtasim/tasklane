package pgstore_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/pgstore"
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
	require.ErrorIs(t, s.Ack(ctx, math.MaxInt64, j.LeaseToken), store.ErrNotFound)
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

// Nack holds a transaction while it decides the error to return, so the stale
// token path must not need a second connection from the same pool.
func TestStaleNacksDoNotExhaustPool(t *testing.T) {
	newStore(t) // truncates the tables
	ctx := context.Background()

	cfg := testPool.Config().Copy()
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	s := pgstore.New(pool, backoff.Policy{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 0 }})

	mustQueue(t, s, store.Queue{Name: "emails"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	j := mustLeaseOne(t, s, "emails", 0)

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Nack(tctx, j.ID, uuid.New(), "x", nil); err != store.ErrLeaseLost {
				t.Errorf("Nack with stale token = %v, want ErrLeaseLost", err)
			}
		}()
	}
	wg.Wait()
}
