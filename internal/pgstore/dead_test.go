package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

func TestRedriveDeadResetsJobs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	id := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	bury(t, id, time.Now())

	n, err := s.RedriveDead(ctx, "emails", nil, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, store.StateAvailable, got.State)
	require.Zero(t, got.Attempts, "a redriven job gets all its attempts back")
	require.True(t, got.FinishedAt.IsZero(), "dead retention restarts if it dies again")
	require.Equal(t, "boom", got.LastError, "the operator still sees why it died")
	mustLeaseOne(t, s, "emails", time.Minute)
}

func TestRedriveDeadByIDsIgnoresOthers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	mustQueue(t, s, store.Queue{Name: "other"})
	dead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	stillDead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	available := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	otherQueue := mustEnqueue(t, s, store.NewJob{Queue: "other", Kind: "k"})
	bury(t, dead, time.Now())
	bury(t, stillDead, time.Now())
	bury(t, otherQueue, time.Now())

	n, err := s.RedriveDead(ctx, "emails", []int64{dead, available, otherQueue, 999}, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the dead job of this queue")

	got, err := s.GetJob(ctx, stillDead)
	require.NoError(t, err)
	require.Equal(t, store.StateDead, got.State, "not in the list")
	got, err = s.GetJob(ctx, otherQueue)
	require.NoError(t, err)
	require.Equal(t, store.StateDead, got.State, "another queue's job")
}

func TestRedriveDeadRespectsLimit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	for range 5 {
		bury(t, mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"}), time.Now())
	}
	var counts []int
	for range 4 {
		n, err := s.RedriveDead(ctx, "emails", nil, 2)
		require.NoError(t, err)
		counts = append(counts, n)
	}
	require.Equal(t, []int{2, 2, 1, 0}, counts)
}

func TestRedriveDeadErrors(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	n, err := s.RedriveDead(ctx, "emails", nil, 1000)
	require.NoError(t, err, "nothing dead is not an error")
	require.Zero(t, n)

	_, err = s.RedriveDead(ctx, "nope", nil, 1000)
	require.ErrorIs(t, err, store.ErrNotFound)

	_, err = s.RedriveDead(ctx, "emails", nil, 0)
	require.ErrorIs(t, err, store.ErrInvalid)
}

// A redrive makes jobs runnable, so it must NOTIFY like Enqueue, and only
// when it changed something.
func TestRedriveDeadNotifiesOnlyWhenItRedrove(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	conn, err := pgx.Connect(ctx, testDB.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "LISTEN tasklane_jobs")
	require.NoError(t, err)

	_, err = s.RedriveDead(ctx, "emails", nil, 1000)
	require.NoError(t, err)
	quiet, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = conn.WaitForNotification(quiet)
	cancel()
	require.Error(t, err, "no notification when nothing was redriven")

	bury(t, mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"}), time.Now())
	// mustEnqueue itself notified; drain that first.
	wait, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err = conn.WaitForNotification(wait)
	require.NoError(t, err)

	_, err = s.RedriveDead(ctx, "emails", nil, 1000)
	require.NoError(t, err)
	n, err := conn.WaitForNotification(wait)
	require.NoError(t, err)
	require.Equal(t, "emails", n.Payload)
}

func TestRedriveDeadAllTakesOldestDeathsFirst(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	base := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	late := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	oldest := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	middle := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	bury(t, late, base.Add(2*time.Minute))
	bury(t, oldest, base)
	bury(t, middle, base.Add(time.Minute))

	n, err := s.RedriveDead(ctx, "emails", nil, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := s.GetJob(ctx, oldest)
	require.NoError(t, err)
	require.Equal(t, store.StateAvailable, got.State, "the oldest death goes first")
	for _, id := range []int64{late, middle} {
		got, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		require.Equal(t, store.StateDead, got.State)
	}
}
