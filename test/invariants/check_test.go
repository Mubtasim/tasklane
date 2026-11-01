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

// onlyViolation asserts that exactly one violation fired, of the given rule,
// and that its message names the check, so a test fails if that check is
// removed even when another check would still fire.
func onlyViolation(t *testing.T, vs []Violation, rule int, msg string) {
	t.Helper()
	require.Len(t, vs, 1, "%v", vs)
	require.Equal(t, rule, vs[0].Rule)
	require.Contains(t, vs[0].Msg, msg)
}

func TestRule1RunForAnUnknownJob(t *testing.T) {
	onlyViolation(t, Check(cfg, nil, []ledger.Run{run(9, 1, 0, "acked")}, nil), 1, "job is gone")
}

func TestRule2DuplicateKeyInTheDatabase(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}, {ID: 2, Key: "a", State: "completed", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(2, 1, 0, "acked")}
	onlyViolation(t, Check(cfg, enq, runs, jobs), 2, "share idempotency key")
}

func TestRule2KeyConfirmedAsTwoJobs(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}, {Key: "a", JobID: 2}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}, {ID: 2, Key: "b", State: "completed", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(2, 1, 0, "acked")}
	onlyViolation(t, Check(cfg, enq, runs, jobs), 2, "was confirmed as job")
}

func TestRule2KeyConfirmedAsTwoJobsWithOneInTheDatabase(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}, {Key: "a", JobID: 2}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}}
	vs := Check(cfg, enq, []ledger.Run{run(1, 1, 0, "acked")}, jobs)
	require.Equal(t, []int{1, 2}, rules(vs)) // job 2 is also gone
	require.Contains(t, vs[0].Msg, "is gone")
	require.Contains(t, vs[1].Msg, "was confirmed as job")
}

func TestRule3TwoAcceptedAcks(t *testing.T) {
	// Both Acks are on the last attempt, so the only way to hold two is two
	// runs of the same attempt: that check fires as well, so assert both.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "acked"), run(1, 1, 0, "acked")}
	vs := Check(cfg, enq, runs, jobs)
	require.Equal(t, []int{3, 3}, rules(vs))
	require.Contains(t, vs[0].Msg, "same lease")
	require.Contains(t, vs[1].Msg, "accepted Acks")
}

func TestRule3AckFromAnOldAttempt(t *testing.T) {
	// Attempt 2's worker left no record, so only the stale Ack is visible.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	onlyViolation(t, Check(cfg, enq, []ledger.Run{run(1, 1, 0, "acked")}, jobs), 3, "last attempt is 2")
}

func TestRule3DeadJobWithAnAck(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "dead", Attempts: 1}}
	onlyViolation(t, Check(cfg, enq, []ledger.Run{run(1, 1, 0, "acked")}, jobs), 3, "job is dead")
}

func TestRule3SameAttemptTwice(t *testing.T) {
	// lease_lost on the last attempt of a dead job is allowed, so only the
	// duplicate attempt number is left.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "dead", Attempts: 1}}
	runs := []ledger.Run{run(1, 1, 0, "lease_lost"), run(1, 1, 0, "lease_lost")}
	onlyViolation(t, Check(cfg, enq, runs, jobs), 3, "same lease")
}

func TestRule3LeaseLostOnTheLastAttemptOfACompletedJob(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 1}}
	onlyViolation(t, Check(cfg, enq, []ledger.Run{run(1, 1, 0, "lease_lost")}, jobs), 3, "lost its lease")
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
	onlyViolation(t, Check(cfg, enq, runs, jobs), 4, "before its lease could expire")
}

func TestRule4SkipsAFailedHandlerWhoseNackWasNotRecorded(t *testing.T) {
	// The Nack committed but the reply was lost, so the run says lease_lost;
	// the retry is due after the Nack backoff, before the lease would expire.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	first := run(1, 1, 0, "lease_lost")
	first.Err = "boom"
	require.Empty(t, Check(cfg, enq, []ledger.Run{first, run(1, 2, 300*time.Millisecond, "acked")}, jobs))
}

func TestRule4RepeatAfterTheLeaseExpiredIsFine(t *testing.T) {
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	runs := []ledger.Run{run(1, 1, 0, "lease_lost"), run(1, 2, 1600*time.Millisecond, "acked")} // within tolerance
	require.Empty(t, Check(cfg, enq, runs, jobs))
}

func TestRule4SkipsAStartStampedLateByAFreeze(t *testing.T) {
	// Attempt 1 was leased just before its worker was paused, reaped, and
	// leased again as attempt 2; the paused worker stamped its start only
	// after the unpause, later than attempt 2's.
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	first := run(1, 1, 4*time.Second, "lease_lost")
	first.Worker = "frozen"
	runs := []ledger.Run{first, run(1, 2, 3*time.Second, "acked")}
	onlyViolation(t, Check(cfg, enq, runs, jobs), 4, "before its lease could expire")

	frozen := cfg
	frozen.Frozen = []Freeze{{Worker: "frozen", From: t0, To: t0.Add(3500 * time.Millisecond)}}
	require.Empty(t, Check(frozen, enq, runs, jobs))

	// A freeze of another worker, or one that ended long before, excuses nothing.
	frozen.Frozen = []Freeze{{Worker: "w", From: t0, To: t0.Add(3500 * time.Millisecond)}, {Worker: "frozen", From: t0.Add(-time.Minute), To: t0.Add(-50 * time.Second)}}
	onlyViolation(t, Check(frozen, enq, runs, jobs), 4, "before its lease could expire")
}

func TestRule4FreezeExemptionLastsOneVisibilityTimeout(t *testing.T) {
	// With a 10 s visibility timeout, a start stamped up to 10 s after the
	// unfreeze may still come from a lease taken before the freeze; one
	// stamped later cannot, so it is checked.
	cfg := Config{VisibilityTimeout: 10 * time.Second, Tolerance: 500 * time.Millisecond,
		Frozen: []Freeze{{Worker: "frozen", From: t0, To: t0.Add(15 * time.Second)}}}
	enq := []ledger.Enqueued{{Key: "a", JobID: 1}}
	jobs := []Job{{ID: 1, Key: "a", State: "completed", Attempts: 2}}
	pair := func(firstStart time.Duration) []ledger.Run {
		first := run(1, 1, firstStart, "lease_lost")
		first.Worker = "frozen"
		return []ledger.Run{first, run(1, 2, firstStart+time.Second, "acked")}
	}
	require.Empty(t, Check(cfg, enq, pair(20*time.Second), jobs))                                     // To + 5 s
	onlyViolation(t, Check(cfg, enq, pair(26*time.Second), jobs), 4, "before its lease could expire") // To + VT + 1 s
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
