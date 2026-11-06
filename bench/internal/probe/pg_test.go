package probe

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTakePGSeesInsertsAndWAL(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, testDB.Reset(ctx))
	_, err := testDB.Pool.Exec(ctx, `INSERT INTO queues (name, max_depth, visibility_timeout_ms, max_attempts) VALUES ('q', 1000, 30000, 3)`)
	require.NoError(t, err)

	before, err := TakePG(ctx, testDB.Pool)
	require.NoError(t, err)
	_, err = testDB.Pool.Exec(ctx, `INSERT INTO jobs (queue, kind, max_attempts) SELECT 'q', 'k', 3 FROM generate_series(1, 100)`)
	require.NoError(t, err)
	// The statistics collector flushes table counters when a transaction
	// ends, at most once a second per backend; force it.
	_, err = testDB.Pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		after, err := TakePG(ctx, testDB.Pool)
		return err == nil && after.Jobs.Inserts-before.Jobs.Inserts == 100 && after.WALBytes > before.WALBytes
	}, 5*time.Second, 50*time.Millisecond)
}

func TestTopStatementsNeedsTheExtension(t *testing.T) {
	// testpg doesn't preload pg_stat_statements; the bench compose file does.
	_, _, err := TopStatements(context.Background(), testDB.Pool, 5)
	require.ErrorContains(t, err, "pg_stat_statements")
}

func TestSampleWaitsCountsActiveBackends(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	go func() { _, _ = testDB.Pool.Exec(ctx, `SELECT pg_sleep(0.5)`) }()
	waits := SampleWaits(ctx, testDB.Pool, 50*time.Millisecond)
	require.Positive(t, waits["Timeout:PgSleep"], "pg_sleep shows as wait event Timeout/PgSleep: %v", waits)
}
