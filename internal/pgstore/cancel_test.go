package pgstore_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/store"
)

func TestCancelAvailableJob(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})

	got, cancelled, err := s.CancelJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateCancelled, got.State)
	require.True(t, cancelled)
	require.False(t, got.FinishedAt.IsZero(), "finished_at is set so the pruner removes it")

	jobs, err := s.Lease(ctx, "emails", 1, time.Minute)
	require.NoError(t, err)
	require.Empty(t, jobs, "a cancelled job is never leased")
}

func TestCancelTwiceSucceeds(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})

	first, firstCancelled, err := s.CancelJob(ctx, id)
	require.NoError(t, err)
	second, secondCancelled, err := s.CancelJob(ctx, id)
	require.NoError(t, err, "a retried cancel is harmless")
	require.Equal(t, first.FinishedAt, second.FinishedAt, "the second cancel changes nothing")
	require.True(t, firstCancelled)
	require.False(t, secondCancelled, "a repeat cancel reports no change")
}

// Retries of a cancel can race each other; exactly one of them changes the job.
func TestConcurrentCancelsChangeOnce(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var changed atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, cancelled, err := s.CancelJob(ctx, id)
			if err != nil {
				t.Errorf("concurrent CancelJob = %v, want nil", err)
				return
			}
			if cancelled {
				changed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, changed.Load(), "exactly one racing cancel changes the job")
}

func TestCancelLeasedJobIsWrongState(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	mustLeaseOne(t, s, "emails", time.Minute)

	_, _, err := s.CancelJob(ctx, id)
	require.ErrorIs(t, err, store.ErrWrongState)
	require.Contains(t, err.Error(), "leased")

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateLeased, got.State)
}

func TestCancelUnknownJob(t *testing.T) {
	s := newStore(t)
	_, _, err := s.CancelJob(context.Background(), 42)
	require.ErrorIs(t, err, store.ErrNotFound)
}

// Cancel and Lease race for the same available job. Lease locks rows FOR
// UPDATE SKIP LOCKED and Cancel's UPDATE re-checks state = 'available' after
// waiting for a lock, so exactly one of them must win every round.
func TestCancelRacesLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "race"})

	for round := range 50 {
		id := mustEnqueue(t, s, store.NewJob{Queue: "race", Kind: "k"})
		start := make(chan struct{})
		var (
			wg        sync.WaitGroup
			leased    []store.Job
			leaseErr  error
			cancelErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			leased, leaseErr = s.Lease(ctx, "race", 1, time.Minute)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _, cancelErr = s.CancelJob(ctx, id)
		}()
		close(start)
		wg.Wait()

		require.NoError(t, leaseErr, "round %d", round)
		got, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		if len(leased) == 1 {
			require.ErrorIs(t, cancelErr, store.ErrWrongState, "round %d: lease won, cancel must lose", round)
			require.Equal(t, store.StateLeased, got.State, "round %d", round)
		} else {
			require.NoError(t, cancelErr, "round %d: cancel won", round)
			require.Equal(t, store.StateCancelled, got.State, "round %d", round)
		}
	}
}

func TestCancelDuringRetryBackoff(t *testing.T) {
	// A one-hour backoff keeps the nacked job waiting (available, run_at in the future).
	s := newStoreWithBackoff(t, backoff.Policy{Base: time.Hour, Cap: time.Hour, Rand: func() float64 { return 1 }})
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	job := mustLeaseOne(t, s, "emails", time.Minute)
	require.NoError(t, nackErr(s, ctx, id, job.LeaseToken, "boom", nil))

	got, cancelled, err := s.CancelJob(ctx, id)
	require.NoError(t, err)
	require.True(t, cancelled)
	require.Equal(t, store.StateCancelled, got.State)
}
