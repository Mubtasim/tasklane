package pgstore_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/store"
)

var testPool *pgxpool.Pool

// TestMain starts one Postgres container for the whole package. Tests share
// it and reset the tables first, so they must not run in parallel.
func TestMain(m *testing.M) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("tasklane"),
		tcpostgres.WithUsername("tasklane"),
		tcpostgres.WithPassword("tasklane"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}
	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	if err := pgstore.Migrate(ctx, testPool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	code := m.Run()
	testPool.Close()
	_ = testcontainers.TerminateContainer(ctr)
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
	_, err := testPool.Exec(context.Background(), "TRUNCATE jobs, queues")
	require.NoError(t, err)
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
