package pgstore_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestLeaseMarksJobLeased(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails", VisibilityTimeout: time.Minute})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome", Payload: []byte(`{"user":42}`)})

	j := mustLeaseOne(t, s, "emails", 0)
	require.Equal(t, id, j.ID)
	require.Equal(t, store.StateLeased, j.State)
	require.Equal(t, 1, j.Attempts)
	require.NotEqual(t, uuid.Nil, j.LeaseToken)
	require.JSONEq(t, `{"user":42}`, string(j.Payload))
	// ttl 0 uses the queue's 1-minute visibility timeout.
	require.WithinDuration(t, time.Now().Add(time.Minute), j.LeaseExpiresAt, 5*time.Second)

	again, err := s.Lease(context.Background(), "emails", 1, 0)
	require.NoError(t, err)
	require.Empty(t, again, "a leased job must not be leased twice")
}

func TestLeaseOrdersByPriorityThenAge(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	low := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "newsletter", Priority: store.PriorityLow})
	old := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "receipt"})
	young := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "receipt"})
	critical := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "reset", Priority: store.PriorityCritical})

	jobs, err := s.Lease(context.Background(), "emails", 10, 0)
	require.NoError(t, err)
	var ids []int64
	for _, j := range jobs {
		ids = append(ids, j.ID)
	}
	require.Equal(t, []int64{critical, old, young, low}, ids)
}

func TestLeaseSkipsDelayedAndPausedJobs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustQueue(t, s, store.Queue{Name: "frozen", Paused: true})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "reminder", RunAt: time.Now().Add(time.Hour)})
	mustEnqueue(t, s, store.NewJob{Queue: "frozen", Kind: "x"})

	for _, q := range []string{"emails", "frozen"} {
		jobs, err := s.Lease(ctx, q, 10, 0)
		require.NoError(t, err)
		require.Empty(t, jobs, q)
	}
}

func TestLeaseUnknownQueueIsNotFound(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})

	_, err := s.Lease(context.Background(), "nope", 1, 0)
	require.ErrorIs(t, err, store.ErrNotFound)

	jobs, err := s.Lease(context.Background(), "emails", 1, 0)
	require.NoError(t, err, "an existing but empty queue is not an error")
	require.Empty(t, jobs)
}

func TestLeaseRejectsBadBatchSize(t *testing.T) {
	s := newStore(t)
	for _, n := range []int{0, 1001} {
		_, err := s.Lease(context.Background(), "emails", n, 0)
		require.ErrorIs(t, err, store.ErrInvalid)
	}
}

// Many workers leasing at once must never receive the same job: this is what
// FOR UPDATE SKIP LOCKED guarantees.
func TestConcurrentLeasesNeverShareAJob(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	const total = 200
	batch := make([]store.NewJob, total)
	for i := range batch {
		batch[i] = store.NewJob{Queue: "emails", Kind: "welcome"}
	}
	_, err := s.Enqueue(ctx, batch)
	require.NoError(t, err)

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := s.Lease(ctx, "emails", 5, 0)
				if err != nil {
					t.Error(err) // require/FailNow must not be called from a goroutine
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Len(t, seen, total)
	for id, n := range seen {
		require.Equal(t, 1, n, "job %d leased %d times", id, n)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.GetJob(context.Background(), math.MaxInt64)
	require.ErrorIs(t, err, store.ErrNotFound)
}
