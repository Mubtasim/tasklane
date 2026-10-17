package pgstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

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
