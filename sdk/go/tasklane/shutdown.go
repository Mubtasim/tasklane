package tasklane

import (
	"context"
	"sync"
	"time"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// shutdownNackError is the Nack message for jobs still running at the
// shutdown deadline.
const shutdownNackError = "worker shutting down"

// drain waits for running jobs after Run's context is cancelled. Jobs still
// running after ShutdownTimeout are cancelled and nacked, so the server
// retries them within seconds instead of after their lease expires. If a
// Nack can't be sent (the network is down), the lease simply expires and the
// reaper retries the job: the Nack only makes recovery faster. drain doesn't
// wait for handlers that ignore their context.
func (w *Worker) drain(r *run) {
	done := make(chan struct{})
	go func() {
		r.jobs.Wait()
		close(done)
	}()
	deadline := time.NewTimer(w.opts.ShutdownTimeout)
	defer deadline.Stop()
	select {
	case <-done:
		return
	case <-deadline.C:
	}

	var nacks sync.WaitGroup
	var finishing []*activeJob
	for _, a := range r.snapshot() {
		if a.lost.Load() {
			continue // the job may belong to another worker
		}
		if !a.claim() {
			// The job's own goroutine won: its handler finished in time
			// and its Ack or Nack is in flight. Wait for it below.
			finishing = append(finishing, a)
			continue
		}
		a.stopHeartbeat()
		a.cancel()
		nacks.Go(func() {
			// Join the heartbeat before the Nack, as runJob does before
			// its final call. A lost lease can only be reported before
			// the heartbeat exits, so check it after the join.
			<-a.hbDone
			if a.lost.Load() {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
			defer cancel()
			_, err := w.client.api.Nack(ctx, &tasklanev1.NackRequest{Id: a.job.ID, LeaseToken: a.job.token, Error: shutdownNackError})
			if err != nil {
				w.log.Warn("shutdown nack failed; the job is retried when its lease expires", "job_id", a.job.ID, "err", err)
				return
			}
			w.log.Info("nacked an unfinished job at shutdown", "job_id", a.job.ID, "kind", a.job.Kind)
		})
	}
	nacks.Wait()

	// Jobs that finished inside the window get their Ack or Nack; wait for
	// it, but no longer than a Nack of our own may take, so Run stays
	// bounded by ShutdownTimeout + finishTimeout.
	grace := time.NewTimer(finishTimeout)
	defer grace.Stop()
	for _, a := range finishing {
		select {
		case <-a.sent:
		case <-grace.C:
			w.log.Warn("shutdown: a job's Ack or Nack is still in flight; the lease expiry covers it", "job_id", a.job.ID)
			return
		}
	}
}

func (r *run) add(a *activeJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[a] = struct{}{}
}

func (r *run) remove(a *activeJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, a)
}

func (r *run) snapshot() []*activeJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeJob, 0, len(r.active))
	for a := range r.active {
		out = append(out, a)
	}
	return out
}
