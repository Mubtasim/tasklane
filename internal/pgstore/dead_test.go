package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

// bury makes a job dead as if its last attempt failed at finishedAt.
func bury(t *testing.T, id int64, finishedAt time.Time) {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET state = 'dead', attempts = max_attempts, last_error = 'boom', finished_at = $2 WHERE id = $1`,
		id, finishedAt)
	require.NoError(t, err)
}

func TestListDeadJobsPagesNewestFirst(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	base := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	var ids []int64
	for range 5 {
		ids = append(ids, mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"}))
	}
	bury(t, ids[0], base)
	bury(t, ids[1], base.Add(time.Minute))
	bury(t, ids[2], base.Add(time.Minute)) // same finished_at as ids[1]: id breaks the tie
	bury(t, ids[3], base.Add(2*time.Minute))
	bury(t, ids[4], base.Add(3*time.Minute))
	want := []int64{ids[4], ids[3], ids[2], ids[1], ids[0]}

	var got []int64
	var after *store.DeadCursor
	for page := 0; ; page++ {
		require.Less(t, page, 10, "paging must end")
		jobs, err := s.ListDeadJobs(ctx, "emails", after, 2)
		require.NoError(t, err)
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			require.Equal(t, store.StateDead, j.State)
			got = append(got, j.ID)
		}
		last := jobs[len(jobs)-1]
		after = &store.DeadCursor{FinishedAt: last.FinishedAt, ID: last.ID}
	}
	require.Equal(t, want, got, "every dead job exactly once, newest first")
}

func TestListDeadJobsOnlyThisQueuesDeadJobs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustQueue(t, s, store.Queue{Name: "other"})
	dead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"}) // available
	elsewhere := mustEnqueue(t, s, store.NewJob{Queue: "other", Kind: "k"})
	bury(t, dead, time.Now())
	bury(t, elsewhere, time.Now())

	jobs, err := s.ListDeadJobs(ctx, "emails", nil, 100)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, dead, jobs[0].ID)
	require.Equal(t, "boom", jobs[0].LastError)
}

func TestListDeadJobsEmptyAndUnknownQueue(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	jobs, err := s.ListDeadJobs(ctx, "emails", nil, 100)
	require.NoError(t, err)
	require.Empty(t, jobs)

	_, err = s.ListDeadJobs(ctx, "nope", nil, 100)
	require.ErrorIs(t, err, store.ErrNotFound)

	_, err = s.ListDeadJobs(ctx, "emails", nil, 0)
	require.ErrorIs(t, err, store.ErrInvalid)
}
