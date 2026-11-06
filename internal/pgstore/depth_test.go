package pgstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/pgstore"
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
	byName := depthsByName(depths)
	require.Len(t, byName, 2)
	emails := byName["emails"]
	require.Equal(t, int64(2), emails.Available, "the delayed job counts, the leased and completed ones don't")
	require.Equal(t, int64(1), emails.Leased)
	require.Equal(t, int64(3), emails.Depth())
	require.Equal(t, int64(10), emails.MaxDepth)
	require.Equal(t, store.QueueDepth{Queue: "empty", MaxDepth: 100_000}, byName["empty"])
}

func depthsByName(depths []store.QueueDepth) map[string]store.QueueDepth {
	m := map[string]store.QueueDepth{}
	for _, d := range depths {
		m[d.Queue] = d
	}
	return m
}

// enqueueAt enqueues a job whose run_at is offset from now.
func enqueueAt(t *testing.T, s *pgstore.Store, queue string, p store.Priority, offset time.Duration) int64 {
	t.Helper()
	return mustEnqueue(t, s, store.NewJob{Queue: queue, Kind: "k", Priority: p, RunAt: time.Now().Add(offset)})
}

func TestQueueDepthsOldestReady(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "a"})
	mustQueue(t, s, store.Queue{Name: "empty"})
	mustQueue(t, s, store.Queue{Name: "later"})

	// "a": a priority-4 job ready for 10 minutes and a priority-1 job ready
	// for 2 minutes; the oldest across priorities counts.
	enqueueAt(t, s, "a", store.PriorityLow, -10*time.Minute)
	enqueueAt(t, s, "a", store.PriorityCritical, -2*time.Minute)
	// "later": only a delayed job, not ready yet.
	enqueueAt(t, s, "later", store.PriorityDefault, time.Hour)

	depths, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	byName := depthsByName(depths)
	require.InDelta(t, (10 * time.Minute).Seconds(), byName["a"].OldestReady.Seconds(), 5)
	require.Equal(t, int64(2), byName["a"].Available)
	require.Zero(t, byName["empty"].OldestReady)
	require.Zero(t, byName["later"].OldestReady)
	require.Equal(t, int64(1), byName["later"].Available, "delayed jobs count towards depth")
}

func TestQueueDepthsPausedQueueHasNoOldestReady(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "p"})
	enqueueAt(t, s, "p", store.PriorityDefault, -10*time.Minute)

	before, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.InDelta(t, (10 * time.Minute).Seconds(), depthsByName(before)["p"].OldestReady.Seconds(), 5)

	_, err = testPool.Exec(ctx, "UPDATE queues SET paused = true WHERE name = 'p'")
	require.NoError(t, err)
	after, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.Zero(t, depthsByName(after)["p"].OldestReady)
	require.Equal(t, int64(1), depthsByName(after)["p"].Available)
}

func TestQueueDepthsWithManyQueues(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const queues = 200
	name := func(i int) string { return fmt.Sprintf("q%03d", i) }
	for i := range queues {
		mustQueue(t, s, store.Queue{Name: name(i)})
		for range i % 7 {
			mustEnqueue(t, s, store.NewJob{Queue: name(i), Kind: "k"})
		}
		if i%7 > 0 {
			mustLeaseOne(t, s, name(i), time.Minute)
		}
	}
	_, err := testPool.Exec(ctx, "UPDATE queues SET paused = true WHERE name = $1", name(1))
	require.NoError(t, err)

	depths, err := s.QueueDepths(ctx)
	require.NoError(t, err)
	require.Len(t, depths, queues)
	byName := depthsByName(depths)
	for i := range queues {
		d := byName[name(i)]
		jobs := int64(i % 7)
		wantLeased := min(jobs, 1)
		require.Equal(t, jobs-wantLeased, d.Available, name(i))
		require.Equal(t, wantLeased, d.Leased, name(i))
	}
	require.Zero(t, byName[name(1)].OldestReady, "a paused queue has no oldest ready job")
}

// The counts must come from one grouped pass over jobs; a per-queue
// correlated count scans the whole table once per queue.
func TestQueueDepthsPlanGroupsJobsOnce(t *testing.T) {
	newStore(t)
	var plan string
	require.NoError(t, testPool.QueryRow(context.Background(), "EXPLAIN (FORMAT JSON) "+pgstore.QueueDepthsSQL).Scan(&plan))
	require.Contains(t, plan, `"Group Key"`, "the counts should be one GROUP BY queue pass")
}
