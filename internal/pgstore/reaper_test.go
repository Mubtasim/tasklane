package pgstore_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
	reaped, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	require.Len(t, reaped, 1)

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

	reaped, err := s.ReapExpired(context.Background(), 100)
	require.NoError(t, err)
	require.Empty(t, reaped)
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
	require.ErrorIs(t, ackErr(s, ctx, a.ID, a.LeaseToken), store.ErrLeaseLost, "A's token is fenced off")
	require.NoError(t, ackErr(s, ctx, b.ID, b.LeaseToken))

	// Once B has completed the job, A's old token is still rejected and B's
	// retried ack still succeeds.
	require.ErrorIs(t, ackErr(s, ctx, a.ID, a.LeaseToken), store.ErrLeaseLost, "A's token after B completed")
	require.NoError(t, ackErr(s, ctx, b.ID, b.LeaseToken), "B's repeated ack")
}

func TestReapKeepsThePreviousError(t *testing.T) {
	s := newStore(t) // zero backoff: a nacked job is leasable at once
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	first := mustLeaseOne(t, s, "emails", time.Minute)
	require.NoError(t, nackErr(s, ctx, first.ID, first.LeaseToken, "smtp: connection refused", nil))

	mustLeaseOne(t, s, "emails", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "lease expired (previous: smtp: connection refused)", got.LastError)

	// A second expiry doesn't nest the message.
	mustLeaseOne(t, s, "emails", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err = s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	got, err = s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "lease expired (previous: smtp: connection refused)", got.LastError)
}

func TestReapTruncatesALongPreviousError(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	first := mustLeaseOne(t, s, "emails", time.Minute)
	require.NoError(t, nackErr(s, ctx, first.ID, first.LeaseToken, strings.Repeat("€", 1365), nil)) // 4095 bytes

	mustLeaseOne(t, s, "emails", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := s.ReapExpired(ctx, 100)
	require.NoError(t, err)
	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.LessOrEqual(t, len(got.LastError), store.MaxErrorBytes)
	require.True(t, utf8.ValidString(got.LastError))
	require.True(t, strings.HasPrefix(got.LastError, "lease expired (previous: €"))
}

func TestReapExpiredReportsQueueAndDead(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "a", MaxAttempts: 1})
	mustQueue(t, s, store.Queue{Name: "b", MaxAttempts: 5})
	mustEnqueue(t, s, store.NewJob{Queue: "a", Kind: "k"})
	mustEnqueue(t, s, store.NewJob{Queue: "b", Kind: "k"})
	mustLeaseOne(t, s, "a", time.Millisecond)
	mustLeaseOne(t, s, "b", time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	reaped, err := s.ReapExpired(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []store.ReapedJob{{Queue: "a", Dead: true}, {Queue: "b", Dead: false}}, reaped)
}

func TestConcurrentReapersReapEachJobOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	for range 20 {
		mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	}
	jobs, err := s.Lease(ctx, "emails", 20, 50*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, jobs, 20)
	time.Sleep(100 * time.Millisecond)

	start := make(chan struct{})
	counts := make([]int, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Go(func() {
			<-start
			reaped, err := s.ReapExpired(ctx, 100)
			require.NoError(t, err)
			counts[i] = len(reaped)
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, 20, counts[0]+counts[1], "SKIP LOCKED splits the work; no job is reaped twice")
	for _, j := range jobs {
		got, err := s.GetJob(ctx, j.ID)
		require.NoError(t, err)
		require.Equal(t, store.StateAvailable, got.State)
		require.Equal(t, 1, got.Attempts)
	}
}
