package pgstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/pgstore"
)

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	// TestMain already migrated once; running again must be a no-op.
	require.NoError(t, pgstore.Migrate(ctx, testPool))

	var n int
	require.NoError(t, testPool.QueryRow(ctx, "SELECT count(*) FROM tasklane_migrations").Scan(&n))
	require.Equal(t, 1, n)
}

func TestMigrateCreatesTables(t *testing.T) {
	ctx := context.Background()
	for _, table := range []string{"queues", "jobs"} {
		var exists bool
		require.NoError(t, testPool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists))
		require.True(t, exists, table)
	}
}
