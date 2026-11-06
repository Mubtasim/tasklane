package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is what the probes need from a pool (*pgxpool.Pool satisfies it).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// TableStats are the jobs table's cumulative counters from
// pg_stat_user_tables. HOTUpdates / Updates is the share of updates that
// didn't touch an index (heap-only tuples); state changes update indexed
// columns, so the design expects it near zero (migration 0001).
type TableStats struct {
	Inserts, Updates, HOTUpdates, Deletes int64
	LiveRows, DeadRows                    int64
	Autovacuums, Autoanalyzes             int64
}

// PGSnapshot is Postgres state at one moment; subtract two to get a run's
// numbers.
type PGSnapshot struct {
	WALBytes int64 // WAL position in bytes since 0/0
	Jobs     TableStats
}

func TakePG(ctx context.Context, db Querier) (PGSnapshot, error) {
	var s PGSnapshot
	err := db.QueryRow(ctx, `
		SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
			n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del, n_live_tup, n_dead_tup,
			autovacuum_count, autoanalyze_count
		FROM pg_stat_user_tables WHERE relname = 'jobs'`).Scan(
		&s.WALBytes, &s.Jobs.Inserts, &s.Jobs.Updates, &s.Jobs.HOTUpdates, &s.Jobs.Deletes,
		&s.Jobs.LiveRows, &s.Jobs.DeadRows, &s.Jobs.Autovacuums, &s.Jobs.Autoanalyzes)
	if err != nil {
		return PGSnapshot{}, fmt.Errorf("pg snapshot: %w", err)
	}
	return s, nil
}

// Statement is one normalized query from pg_stat_statements.
type Statement struct {
	Query           string
	Calls           int64
	TotalMs, MeanMs float64
	Rows            int64
}

// ResetStatements clears pg_stat_statements so the next TopStatements covers
// only the measured window.
func ResetStatements(ctx context.Context, db Querier) error {
	_, err := db.Exec(ctx, `SELECT pg_stat_statements_reset()`)
	if err != nil {
		return fmt.Errorf("pg_stat_statements reset: %w", err)
	}
	return nil
}

// ownQueries excludes the load generator's own probes (pg_stat_activity
// sampling, TakePG's table and WAL reads, the statistics queries themselves)
// from the totals: they run every 100 ms and would otherwise inflate the
// queries-per-second figure, badly so in idle runs.
const ownQueries = `query NOT LIKE '%pg\_stat\_%' AND query NOT LIKE '%pg\_current\_wal\_lsn%'`

// TopStatements returns the n statements with the most total execution time
// since the last reset, and the calls of all statements (queries per second
// = totalCalls / window).
func TopStatements(ctx context.Context, db Querier, n int) ([]Statement, int64, error) {
	var total int64
	if err := db.QueryRow(ctx, `SELECT coalesce(sum(calls), 0)::bigint FROM pg_stat_statements WHERE `+ownQueries).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("pg_stat_statements: %w", err)
	}
	rows, err := db.Query(ctx, `
		SELECT query, calls, total_exec_time, mean_exec_time, rows
		FROM pg_stat_statements WHERE `+ownQueries+` ORDER BY total_exec_time DESC LIMIT $1`, n)
	if err != nil {
		return nil, 0, fmt.Errorf("pg_stat_statements: %w", err)
	}
	top, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Statement, error) {
		var s Statement
		err := r.Scan(&s.Query, &s.Calls, &s.TotalMs, &s.MeanMs, &s.Rows)
		s.Query = strings.Join(strings.Fields(s.Query), " ")
		return s, err
	})
	if err != nil {
		return nil, 0, fmt.Errorf("pg_stat_statements: %w", err)
	}
	return top, total, nil
}

// SampleWaits polls pg_stat_activity every interval until ctx is done and
// counts what each active client backend was doing: "CPU" when it had no
// wait event (running), otherwise "type:event", such as "LWLock:WALWrite"
// or "Lock:transactionid". The counts show where Postgres time goes, which
// pg_stat_statements' execution times alone don't (a commit waiting on the
// WAL flush, for example).
func SampleWaits(ctx context.Context, db Querier, every time.Duration) map[string]int {
	counts := map[string]int{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		rows, err := db.Query(ctx, `
			SELECT coalesce(wait_event_type || ':' || wait_event, 'CPU')
			FROM pg_stat_activity
			WHERE state = 'active' AND backend_type = 'client backend' AND pid <> pg_backend_pid()`)
		if err == nil {
			for rows.Next() {
				var w string
				if rows.Scan(&w) == nil {
					counts[w]++
				}
			}
			rows.Close()
		}
		select {
		case <-ctx.Done():
			return counts
		case <-t.C:
		}
	}
}
