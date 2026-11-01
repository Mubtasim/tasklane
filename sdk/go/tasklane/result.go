package tasklane

import (
	"fmt"
	"time"
)

// Outcome is how one run of a job ended, as far as this worker knows.
type Outcome int

const (
	// OutcomeAcked: the server accepted the Ack; the job is completed.
	OutcomeAcked Outcome = iota + 1
	// OutcomeNacked: the server accepted the Nack (a retry is scheduled,
	// or the job is dead after its last attempt).
	OutcomeNacked
	// OutcomeLeaseLost: the server rejected a heartbeat, Ack or Nack because
	// this worker no longer holds the lease (it expired and the job may be
	// running elsewhere).
	OutcomeLeaseLost
	// OutcomeGaveUp: the Ack or Nack could not reach the server before the
	// lease would have expired, or the client was closed. The server
	// retries the job after the lease expires.
	OutcomeGaveUp
)

func (o Outcome) String() string {
	switch o {
	case OutcomeAcked:
		return "acked"
	case OutcomeNacked:
		return "nacked"
	case OutcomeLeaseLost:
		return "lease_lost"
	case OutcomeGaveUp:
		return "gave_up"
	}
	return "unknown"
}

// Result describes one run of a job, reported once through
// WorkerOptions.OnResult after the worker is done with the job.
type Result struct {
	Job     *LeasedJob
	Outcome Outcome
	Start   time.Time // the job was picked up, just before the handler started
	End     time.Time // the outcome was known
	Err     error     // the handler's error, or the shutdown reason when the worker cancelled and nacked a still-running job at shutdown
}

// report calls OnResult, recovering a panic so a faulty observer can't stop
// the worker.
func (w *Worker) report(a *activeJob, o Outcome, err error) {
	if w.opts.OnResult == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			w.log.Error("OnResult panicked", "job_id", a.job.ID, "panic", fmt.Sprint(p))
		}
	}()
	w.opts.OnResult(Result{Job: a.job, Outcome: o, Start: a.start, End: time.Now(), Err: err})
}
