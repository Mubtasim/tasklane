package pgstore_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/store"
	"github.com/mubtasim/tasklane/internal/testpg"
)

// testDB is shared by every test in the package; tests reset it first, so
// they must not run in parallel.
var (
	testDB   *testpg.DB
	testPool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	var err error
	testDB, err = testpg.Start(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	testPool = testDB.Pool
	code := m.Run()
	testDB.Close()
	os.Exit(code)
}

// newStore empties the tables and returns a store with zero backoff, so a
// retried job is immediately leasable.
func newStore(t *testing.T) *pgstore.Store {
	t.Helper()
	return newStoreWithBackoff(t, backoff.Policy{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 0 }})
}

func newStoreWithBackoff(t *testing.T, p backoff.Policy) *pgstore.Store {
	t.Helper()
	require.NoError(t, testDB.Reset(context.Background()))
	return pgstore.New(testPool, p)
}

func mustQueue(t *testing.T, s *pgstore.Store, q store.Queue) {
	t.Helper()
	require.NoError(t, s.CreateQueue(context.Background(), q))
}

func mustEnqueue(t *testing.T, s *pgstore.Store, j store.NewJob) int64 {
	t.Helper()
	res, err := s.Enqueue(context.Background(), []store.NewJob{j})
	require.NoError(t, err)
	return res[0].ID
}

func mustLeaseOne(t *testing.T, s *pgstore.Store, queue string, ttl time.Duration) store.Job {
	t.Helper()
	jobs, err := s.Lease(context.Background(), queue, 1, ttl)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	return jobs[0]
}
