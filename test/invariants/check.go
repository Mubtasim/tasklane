// Package invariants checks the correctness rules of design §17.2 against
// what workers and producers recorded (package ledger) and the final jobs
// table. The invariant test and the chaos test share it, so "correct" means
// the same thing in both.
package invariants

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/ledger"
)

type Job struct {
	ID       int64
	Key      string
	State    string
	Attempts int
}

type Config struct {
	VisibilityTimeout time.Duration
	Tolerance         time.Duration
	// Frozen lists the times a worker process was suspended (docker pause).
	Frozen []Freeze
}

// Freeze is a window in which a worker process was suspended. A worker
// stamps a run's start when its goroutine handles the Lease response, so a
// job leased just before the freeze gets a start stamped just after it: the
// recorded start can be later than the lease by the whole freeze.
type Freeze struct {
	Worker   string
	From, To time.Time
}

// freezeSlack is how long after a freeze ends a worker can still be
// catching up on Lease responses that arrived before or during it.
const freezeSlack = 2 * time.Second

// stampedAfterFreeze reports whether r's start may have been stamped late
// because its worker was frozen between the lease and the stamp.
func (c Config) stampedAfterFreeze(r ledger.Run) bool {
	for _, f := range c.Frozen {
		if f.Worker == r.Worker && !r.Start.Before(f.From) && !r.Start.After(f.To.Add(freezeSlack)) {
			return true
		}
	}
	return false
}

type Violation struct {
	Rule  int
	JobID int64
	Msg   string
}

func (v Violation) String() string { return fmt.Sprintf("rule %d, job %d: %s", v.Rule, v.JobID, v.Msg) }

// LoadJobs reads every job of the queue after the run has drained.
func LoadJobs(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, queue string) ([]Job, error) {
	rows, err := q.Query(ctx, `SELECT id, coalesce(idempotency_key, ''), state::text, attempts FROM jobs WHERE queue = $1 ORDER BY id`, queue)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Job, error) {
		var j Job
		err := r.Scan(&j.ID, &j.Key, &j.State, &j.Attempts)
		return j, err
	})
}

func Check(cfg Config, enqueued []ledger.Enqueued, runs []ledger.Run, jobs []Job) []Violation {
	var vs []Violation
	add := func(rule int, id int64, format string, args ...any) {
		vs = append(vs, Violation{Rule: rule, JobID: id, Msg: fmt.Sprintf(format, args...)})
	}
	byID := map[int64]Job{}
	byKey := map[string][]Job{}
	for _, j := range jobs {
		byID[j.ID] = j
		if j.Key != "" {
			byKey[j.Key] = append(byKey[j.Key], j)
		}
	}

	// Rule 1: every confirmed enqueue exists and every job is final.
	for _, e := range enqueued {
		if _, ok := byID[e.JobID]; !ok {
			add(1, e.JobID, "enqueue of key %q was confirmed but the job is gone", e.Key)
		}
	}
	for _, j := range jobs {
		if j.State != "completed" && j.State != "dead" {
			add(1, j.ID, "still %s after the run drained", j.State)
		}
	}

	// Rule 2: one job per idempotency key, and the ledger agrees.
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys) // stable report order
	for _, key := range keys {
		if js := byKey[key]; len(js) > 1 {
			add(2, js[0].ID, "%d jobs share idempotency key %q", len(js), key)
		}
	}
	confirmed := map[string]int64{}
	for _, e := range enqueued {
		if prev, ok := confirmed[e.Key]; ok && prev != e.JobID {
			add(2, e.JobID, "key %q was confirmed as job %d and job %d", e.Key, prev, e.JobID)
		}
		confirmed[e.Key] = e.JobID
	}

	// Rules 3 and 4 per job.
	perJob := map[int64][]ledger.Run{}
	for _, r := range runs {
		perJob[r.JobID] = append(perJob[r.JobID], r)
	}
	ids := make([]int64, 0, len(perJob))
	for id := range perJob {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	for _, id := range ids {
		rs := perJob[id]
		sort.SliceStable(rs, func(a, b int) bool { return rs[a].Attempt < rs[b].Attempt })
		j, ok := byID[id]
		if !ok {
			add(1, id, "workers ran it but the job is gone")
			continue
		}
		acked := 0
		for i, r := range rs {
			if i > 0 && rs[i-1].Attempt == r.Attempt {
				add(3, id, "two runs hold attempt %d: two workers had the same lease", r.Attempt)
			}
			switch r.Outcome {
			case "acked":
				acked++
				if r.Attempt != j.Attempts {
					add(3, id, "an Ack from attempt %d was accepted, but the last attempt is %d", r.Attempt, j.Attempts)
				}
			case "lease_lost":
				// gave_up is not checked: the Ack may have committed just
				// before the connection broke.
				if r.Attempt >= j.Attempts && j.State != "dead" {
					add(3, id, "attempt %d lost its lease, but no later attempt ran and the job is %s", r.Attempt, j.State)
				}
			}
		}
		// A completed job may have no acked run: the acking worker can have
		// been killed after the server committed the Ack, or its client closed
		// mid-Ack (gave_up). Only a second accepted Ack is a violation.
		switch {
		case j.State == "completed" && acked > 1:
			add(3, id, "%d accepted Acks for one completed job", acked)
		case j.State == "dead" && acked > 0:
			add(3, id, "the job is dead but an Ack was accepted")
		}
		for i := 1; i < len(rs); i++ {
			prev, next := rs[i-1], rs[i]
			// A handler that failed ends in a Nack, which makes the retry due
			// after the backoff, well before the lease expires. The Nack may
			// still be recorded as lease_lost or gave_up (the server was
			// killed after committing it, or its retries ran out), so a run
			// with an error is skipped too.
			// When attempt k's start was stamped late by a freeze, the lease
			// it measures from is unknown, so the pair can't be checked.
			if next.Attempt != prev.Attempt+1 || prev.Outcome == "nacked" || prev.Err != "" || cfg.stampedAfterFreeze(prev) {
				continue
			}
			earliest := prev.Start.Add(cfg.VisibilityTimeout - cfg.Tolerance)
			if next.Start.Before(earliest) {
				add(4, id, "attempt %d started %v after attempt %d, before its lease could expire (%v)",
					next.Attempt, next.Start.Sub(prev.Start).Round(time.Millisecond), prev.Attempt, cfg.VisibilityTimeout)
			}
		}
	}
	return vs
}

// Report formats up to max violations with every run of each job involved.
func Report(vs []Violation, runs []ledger.Run, max int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d invariant violations\n", len(vs))
	for i, v := range vs {
		if i == max {
			fmt.Fprintf(&b, "... and %d more\n", len(vs)-max)
			break
		}
		fmt.Fprintf(&b, "%s\n", v)
		for _, r := range runs {
			if r.JobID == v.JobID {
				fmt.Fprintf(&b, "    attempt %d by %s: %s, %s → %s %s\n", r.Attempt, r.Worker, r.Outcome,
					r.Start.Format("15:04:05.000"), r.End.Format("15:04:05.000"), r.Err)
			}
		}
	}
	return b.String()
}
