package pgstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

// finish puts a job in state, finished at the given age ago.
func finish(t *testing.T, id int64, state store.JobState, age time.Duration) {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET state = $2::job_state, finished_at = now() - interval '1 millisecond' * $3::bigint WHERE id = $1`,
		id, string(state), age.Milliseconds())
	require.NoError(t, err)
}

func jobExists(t *testing.T, s interface {
	GetJob(context.Context, int64) (store.Job, error)
}, id int64) bool {
	t.Helper()
	_, err := s.GetJob(context.Background(), id)
	if err != nil {
		require.ErrorIs(t, err, store.ErrNotFound)
		return false
	}
	return true
}

func TestPruneFinishedDeletesOnlyOldCompletedAndCancelled(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	oldDone := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	oldCancelled := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	recent := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	oldDead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	available := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	finish(t, oldDone, store.StateCompleted, 25*time.Hour)
	finish(t, oldCancelled, store.StateCancelled, 25*time.Hour)
	finish(t, recent, store.StateCompleted, time.Hour)
	finish(t, oldDead, store.StateDead, 25*time.Hour)

	n, err := s.PruneFinished(ctx, 24*time.Hour, 1000)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.False(t, jobExists(t, s, oldDone))
	require.False(t, jobExists(t, s, oldCancelled))
	require.True(t, jobExists(t, s, recent), "inside the retention window")
	require.True(t, jobExists(t, s, oldDead), "dead jobs have their own retention")
	require.True(t, jobExists(t, s, available))
}

func TestPruneDeadDeletesOnlyOldDeadJobs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	oldDead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	recentDead := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	oldDone := mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"})
	finish(t, oldDead, store.StateDead, 8*24*time.Hour)
	finish(t, recentDead, store.StateDead, 6*24*time.Hour)
	finish(t, oldDone, store.StateCompleted, 8*24*time.Hour)

	n, err := s.PruneDead(ctx, 7*24*time.Hour, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.False(t, jobExists(t, s, oldDead))
	require.True(t, jobExists(t, s, recentDead))
	require.True(t, jobExists(t, s, oldDone), "PruneFinished's job, not PruneDead's")
}

func TestPruneRespectsLimit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})
	for range 5 {
		finish(t, mustEnqueue(t, s, store.NewJob{Queue: "emails", Kind: "k"}), store.StateCompleted, 25*time.Hour)
	}
	var counts []int
	for range 4 {
		n, err := s.PruneFinished(ctx, 24*time.Hour, 2)
		require.NoError(t, err)
		counts = append(counts, n)
	}
	require.Equal(t, []int{2, 2, 1, 0}, counts)

	_, err := s.PruneFinished(ctx, 24*time.Hour, 0)
	require.ErrorIs(t, err, store.ErrInvalid)
}
