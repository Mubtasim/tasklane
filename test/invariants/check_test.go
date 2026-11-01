package invariants

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/ledger"
)

var (
	t0  = time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	cfg = Config{VisibilityTimeout: 2 * time.Second, Tolerance: 500 * time.Millisecond}
)

func run(job int64, attempt int, start time.Duration, outcome string) ledger.Run {
	return ledger.Run{Worker: "w", JobID: job, Attempt: attempt, Start: t0.Add(start), End: t0.Add(start + 100*time.Millisecond), Outcome: outcome}
}

func rules(vs []Violation) []int {
	var out []int
	for _, v := range vs {
		out = append(out, v.Rule)
	}
	return out
}

func TestHealthyRunPasses(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}, {Key: "b", JobID: 2}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}, {ID: 2, Key: "b", State: "dead", Attempts: 3}}
	runs := []ledger.Run{
		run(1, 1, 0, "nacked"), run(1, 2, time.Second, "acked"),
		run(2, 1, 0, "nacked"), run(2, 2, time.Second, "nacked"), run(2, 3, 2*time.Second, "nacked"),
	}
	require.Empty(t, Check(cfg, enq, runs, jobs))
}

func TestRule1LostJob(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	require.Equal(t, []int{1}, rules(Check(cfg, enq, nil, nil)))
}

func TestRule1StuckJob(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "leased", Attempts: 1}}
	require.Equal(t, []int{1}, rules(Check(cfg, enq, nil, jobs)))
}

func TestRule2DuplicateKey(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}, {Key: "a", JobID: 2}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}, {ID: 2, Key: "a", State: "completed", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(2, 1, 0, "acked")}
	require.Contains(t, rules(Check(cfg, enq, runs, jobs)), 2)
}

func TestRule3TwoAcceptedAcks(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(1, 2, 3*time.Second, "acked")}
	require.Contains(t, rules(Check(cfg, enq, runs, jobs)), 3)
}

func TestRule3AckFromAnOldAttempt(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(1, 2, 3*time.Second, "lease_lost")}
	require.Contains(t, rules(Check(cfg, enq, runs, jobs)), 3)
}

func TestRule3DeadJobWithAnAck(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "dead", Attempts: 1}}
	require.Contains(t, rules(Check(cfg, enq, []ledger.Run{run(1, 1, 0, "acked")}, jobs)), 3)
}

func TestRule3SameAttemptTwice(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(1, 1, 0, "lease_lost")}
	require.Contains(t, rules(Check(cfg, enq, runs, jobs)), 3)
}

func TestRule3LeaseLostOnLastAttemptOfADeadJobIsFine(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "dead", Attempts: 1}}
	require.Empty(t, Check(cfg, enq, []ledger.Run{run(1, 1, 0, "lease_lost")}, jobs))
}

func TestRule3GaveUpOnTheLastAttemptIsFine(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}}
	require.Empty(t, Check(cfg, enq, []ledger.Run{run(1, 1, 0, "gave_up")}, jobs))
}

func TestRule4RepeatTooSoon(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	// attempt 1 was not nacked, so attempt 2 may start only after ~2 s.
	runs := []ledger.Run{run(1, 1, 0, "lease_lost"), run(1, 2, time.Second, "acked")}
	require.Equal(t, []int{4}, rules(Check(cfg, enq, runs, jobs)))
}

func TestRule4RepeatAfterTheLeaseExpiredIsFine(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	runs := []ledger.Run{run(1, 1, 0, "lease_lost"), run(1, 2, 1600*time.Millisecond, "acked")} // within tolerance
	require.Empty(t, Check(cfg, enq, runs, jobs))
}

func TestRule4SkipsMissingAttempts(t *testing.T) {
	// attempt 1's worker was killed: no record. Nothing to compare.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	require.Empty(t, Check(cfg, enq, []ledger.Run{run(1, 2, 0, "acked")}, jobs))
}

func TestReportListsTheJobsRuns(t *testing.T) {
	runs := []ledger.Run{run(1, 1, 0, "acked")}
	out := Report([]Violation{{Rule: 3, JobID: 1, Msg: "two accepted acks"}}, runs, 10)
	require.Contains(t, out, "rule 3")
	require.Contains(t, out, "job 1")
	require.Contains(t, out, "attempt 1")
}
