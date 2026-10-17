package pgstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/store"
)

func jobRow(t *testing.T, id int64) (state string, priority, maxAttempts int, payload string) {
	t.Helper()
	require.NoError(t, testPool.QueryRow(context.Background(),
		"SELECT state::text, priority, max_attempts, payload::text FROM jobs WHERE id = $1", id,
	).Scan(&state, &priority, &maxAttempts, &payload))
	return
}

func TestEnqueueUsesDefaults(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails", MaxAttempts: 4})

	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "welcome"})
	state, priority, maxAttempts, payload := jobRow(t, id)
	require.Equal(t, "available", state)
	require.Equal(t, int(store.PriorityDefault), priority)
	require.Equal(t, 4, maxAttempts) // from the queue
	require.Equal(t, "{}", payload)
}

func TestEnqueueIdempotencyKey(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustQueue(t, s, store.Queue{Name: "sms"})

	job := store.NewJob{Queue: "emails", Kind: "welcome", IdempotencyKey: "welcome:42"}
	first, err := s.Enqueue(ctx, []store.NewJob{job})
	require.NoError(t, err)
	second, err := s.Enqueue(ctx, []store.NewJob{job})
	require.NoError(t, err)

	require.True(t, first[0].Created)
	require.False(t, second[0].Created)
	require.Equal(t, first[0].ID, second[0].ID)

	// Keys are scoped per queue.
	other, err := s.Enqueue(ctx, []store.NewJob{{Queue: "sms", Kind: "welcome", IdempotencyKey: "welcome:42"}})
	require.NoError(t, err)
	require.True(t, other[0].Created)
}

func TestEnqueueIdempotencyUnderConcurrency(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})

	const n = 10
	results := make([]store.EnqueueResult, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Enqueue(context.Background(), []store.NewJob{{Queue: "emails", Kind: "welcome", IdempotencyKey: "same"}})
			if err != nil {
				t.Error(err) // require/FailNow must not be called from a goroutine
				return
			}
			results[i] = res[0]
		}()
	}
	wg.Wait()

	created := 0
	for _, r := range results {
		require.Equal(t, results[0].ID, r.ID)
		if r.Created {
			created++
		}
	}
	require.Equal(t, 1, created)
}

func TestEnqueueValidatesAndIsAtomic(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	_, err := s.Enqueue(ctx, []store.NewJob{{Queue: "emails", Kind: "x", Priority: 5}})
	require.ErrorIs(t, err, store.ErrInvalid)

	// The second job's queue doesn't exist, so the first must not be inserted either.
	_, err = s.Enqueue(ctx, []store.NewJob{{Queue: "emails", Kind: "ok"}, {Queue: "missing", Kind: "x"}})
	require.ErrorIs(t, err, store.ErrNotFound)
	var count int
	require.NoError(t, testPool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count))
	require.Zero(t, count)
}

func TestEnqueueKeepsExplicitPriority(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})

	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "urgent", Priority: store.PriorityCritical})
	_, priority, _, _ := jobRow(t, id)
	require.Equal(t, int(store.PriorityCritical), priority)
}

func TestEnqueueRejectsEmptyAndOversizedBatches(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	_, err := s.Enqueue(ctx, nil)
	require.ErrorIs(t, err, store.ErrInvalid)

	big := make([]store.NewJob, pgstore.MaxEnqueueBatch+1)
	for i := range big {
		big[i] = store.NewJob{Queue: "emails", Kind: "x"}
	}
	_, err = s.Enqueue(ctx, big)
	require.ErrorIs(t, err, store.ErrInvalid)

	_, err = s.Enqueue(ctx, big[:pgstore.MaxEnqueueBatch])
	require.NoError(t, err, "exactly the maximum is allowed")
}

// Keyed jobs are inserted in key order, but results follow the caller's order.
func TestEnqueueReturnsResultsInCallerOrder(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	res, err := s.Enqueue(ctx, []store.NewJob{
		{Queue: "emails", Kind: "k", IdempotencyKey: "b"},
		{Queue: "emails", Kind: "plain"},
		{Queue: "emails", Kind: "k", IdempotencyKey: "a"},
		{Queue: "emails", Kind: "k", IdempotencyKey: "b"}, // duplicate within the batch
	})
	require.NoError(t, err)
	require.Len(t, res, 4)

	wantKeys := []string{"b", "", "a", "b"}
	for i, r := range res {
		j, err := s.GetJob(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, wantKeys[i], j.IdempotencyKey, "result %d", i)
	}
	require.True(t, res[0].Created)
	require.True(t, res[1].Created)
	require.True(t, res[2].Created)
	require.False(t, res[3].Created)
	require.Equal(t, res[0].ID, res[3].ID)
}

// Two batches that use the same keys in opposite orders would deadlock
// (SQLSTATE 40P01) if keys were inserted in the caller's order.
func TestEnqueueConcurrentBatchesDoNotDeadlock(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})
	keys := []string{"a", "b", "c", "d", "e"}

	for round := range 10 {
		forward := make([]store.NewJob, len(keys))
		backward := make([]store.NewJob, len(keys))
		for i, k := range keys {
			key := fmt.Sprintf("%d-%s", round, k)
			forward[i] = store.NewJob{Queue: "emails", Kind: "x", IdempotencyKey: key}
			backward[len(keys)-1-i] = store.NewJob{Queue: "emails", Kind: "x", IdempotencyKey: key}
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, batch := range [][]store.NewJob{forward, backward} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := s.Enqueue(context.Background(), batch); err != nil {
					t.Errorf("round %d: %v", round, err) // require/FailNow must not be called from a goroutine
				}
			}()
		}
		close(start)
		wg.Wait()
		if t.Failed() {
			return
		}
	}
}

// Enqueue wakes listeners once per queue, and only when it commits.
func TestEnqueueNotifiesOnCommit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustQueue(t, s, store.Queue{Name: "sms"})

	conn, err := pgx.Connect(ctx, testDB.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "LISTEN "+pgstore.NotifyChannel)
	require.NoError(t, err)

	_, err = s.Enqueue(ctx, []store.NewJob{
		{Queue: "emails", Kind: "a"}, {Queue: "sms", Kind: "b"}, {Queue: "emails", Kind: "c"},
	})
	require.NoError(t, err)

	got := map[string]int{}
	for range 2 {
		wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		n, err := conn.WaitForNotification(wctx)
		cancel()
		require.NoError(t, err)
		got[n.Payload]++
	}
	require.Equal(t, map[string]int{"emails": 1, "sms": 1}, got)

	// A failed (rolled back) enqueue sends nothing.
	_, err = s.Enqueue(ctx, []store.NewJob{{Queue: "emails", Kind: "a"}, {Queue: "missing", Kind: "b"}})
	require.ErrorIs(t, err, store.ErrNotFound)
	wctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = conn.WaitForNotification(wctx)
	require.Error(t, err, "no notification after a rollback")
	require.Equal(t, context.DeadlineExceeded, wctx.Err())
}
