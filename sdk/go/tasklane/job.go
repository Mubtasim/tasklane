package tasklane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// defaultFinishTimeout bounds an Ack or Nack attempt whose lease deadline
// has already passed (it may still succeed: nobody else holds the job until
// the reaper runs), and the Nacks and in-flight Acks a shutdown waits for.
const defaultFinishTimeout = 2 * time.Second

// HandlerFunc runs one job. Returning nil acks the job; returning an error
// nacks it, and the server retries it after a backoff delay (or moves it to
// the dead-letter queue after its last attempt). ctx is cancelled when the
// lease is lost or the worker's shutdown deadline passes: return soon after.
type HandlerFunc func(ctx context.Context, job *LeasedJob) error

// LeasedJob is a job the worker holds a lease on.
type LeasedJob struct {
	ID          int64
	Kind        string
	Attempt     int // 1 on the first run
	MaxAttempts int
	Payload     json.RawMessage // a JSON object

	token string // the lease token: only the runner sends Ack, Nack and Heartbeat
}

// Decode unmarshals the job's JSON payload into v.
func (j *LeasedJob) Decode(v any) error {
	return json.Unmarshal(j.Payload, v)
}

// RetryAt wraps err so the job is retried at t instead of after the server's
// backoff delay, for example when a downstream API answers Retry-After.
func RetryAt(err error, t time.Time) error {
	if err == nil {
		err = errors.New("retry requested")
	}
	return &retryAtError{err: err, at: t}
}

type retryAtError struct {
	err error
	at  time.Time
}

func (e *retryAtError) Error() string { return e.err.Error() }
func (e *retryAtError) Unwrap() error { return e.err }

// activeJob is a job this worker is running.
type activeJob struct {
	job           *LeasedJob
	start         time.Time          // set before the job is shared (drain reads it); the handler starts right after
	cancel        context.CancelFunc // cancels the handler's context
	stopHeartbeat context.CancelFunc
	lost          atomic.Bool   // the server reported the lease gone
	finished      atomic.Bool   // the final Ack or Nack has been claimed
	hbDone        chan struct{} // closed once the heartbeat goroutine has exited (or never started)
	sent          chan struct{} // closed when the job's own goroutine is done with the final call it claimed

	mu sync.Mutex
	// deadline is when the lease expires by this process's monotonic clock.
	// It is counted from when the Lease response arrived, not from when the
	// request was sent: the server sets lease_expires_at when it leases the
	// job, which can be a whole long-poll later. Its now() is at most one
	// network latency earlier than our clock reading, and a late Ack is
	// either rejected harmlessly or accepted before the reaper runs.
	// Heartbeats move it from the moment each one was sent.
	deadline time.Time
}

// claim reports whether the caller may send the job's final Ack or Nack:
// only the first caller may, so the job's goroutine and a shutdown never
// both answer for it.
func (a *activeJob) claim() bool {
	return a.finished.CompareAndSwap(false, true)
}

func (a *activeJob) leaseDeadline() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deadline
}

func (a *activeJob) extendLease(t time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deadline = t
}

func leasedJobFromProto(pj *tasklanev1.Job) (*LeasedJob, error) {
	job := &LeasedJob{
		ID:          pj.GetId(),
		Kind:        pj.GetKind(),
		Attempt:     int(pj.GetAttempts()),
		MaxAttempts: int(pj.GetMaxAttempts()),
		token:       pj.GetLeaseToken(),
	}
	payload, err := payloadFromStruct(pj.GetPayload())
	if err != nil {
		return job, fmt.Errorf("decode payload: %w", err)
	}
	job.Payload = payload
	return job, nil
}

// runJob runs one leased job, sends its Ack or Nack and frees its slot.
// leasedAt is when the Lease response arrived.
func (w *Worker) runJob(r *run, pj *tasklanev1.Job, leasedAt time.Time) {
	defer func() { <-r.slots }()
	job, err := leasedJobFromProto(pj)
	// Not derived from Run's context: a shutdown lets running jobs finish.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	a := &activeJob{job: job, start: time.Now(), cancel: cancel, stopHeartbeat: stopHeartbeat, deadline: leasedAt.Add(r.vt),
		hbDone: make(chan struct{}), sent: make(chan struct{})}
	if err != nil {
		close(a.hbDone) // no heartbeat runs for a job that can't be decoded
	}
	r.add(a)
	defer r.remove(a)
	if err == nil {
		go func() {
			defer close(a.hbDone)
			w.heartbeat(hbCtx, a, r.vt)
		}()
		err = w.call(ctx, job)
	}
	// Claim the final call as soon as the handler returns, so a shutdown
	// that reaches its deadline now can't nack a job that just succeeded.
	// If drain claimed it first, drain answers for the job.
	if !a.claim() {
		return
	}
	// Tell drain when the final call is done, so it can wait for it.
	defer close(a.sent)
	// Stop heartbeating before the final call, so no heartbeat races the
	// Ack or Nack. A lost lease can only be reported before the heartbeat
	// exits, so check it after the join: the job may belong to another
	// worker now.
	stopHeartbeat()
	<-a.hbDone
	if a.lost.Load() {
		w.report(a, OutcomeLeaseLost, err)
		return
	}
	w.report(a, w.finish(a, err), err)
}

// call runs the handler for the job's kind. A missing handler or a panic
// becomes an error, so the job is nacked and the worker keeps running.
func (w *Worker) call(ctx context.Context, job *LeasedJob) (err error) {
	h := w.handler(job.Kind)
	if h == nil {
		return fmt.Errorf("no handler for kind %q", job.Kind)
	}
	defer func() {
		if p := recover(); p != nil {
			w.log.Error("handler panicked", "job_id", job.ID, "kind", job.Kind, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", p)
		}
	}()
	return h(ctx, job)
}

// finish acks the job when err is nil and nacks it with err's message
// otherwise. It returns how the call ended.
func (w *Worker) finish(a *activeJob, err error) Outcome {
	if err == nil {
		return w.send(a, "ack", OutcomeAcked, func(ctx context.Context) error {
			_, err := w.client.api.Ack(ctx, &tasklanev1.AckRequest{Id: a.job.ID, LeaseToken: a.job.token})
			return err
		})
	}
	w.log.Warn("job failed", "job_id", a.job.ID, "kind", a.job.Kind, "attempt", a.job.Attempt, "err", err)
	req := &tasklanev1.NackRequest{Id: a.job.ID, LeaseToken: a.job.token, Error: err.Error()}
	var ra *retryAtError
	if errors.As(err, &ra) {
		req.RetryAt = timestamppb.New(ra.at)
	}
	return w.send(a, "nack", OutcomeNacked, func(ctx context.Context) error {
		_, err := w.client.api.Nack(ctx, req)
		return err
	})
}

// send makes the job's final Ack or Nack call, retrying with backoff while
// the server is unreachable: a retried Ack is idempotent, and a repeated Nack
// gets a harmless FAILED_PRECONDITION. It gives up once the lease would have
// expired, because the token is useless then and the reaper retries the job.
// It returns ok when the server accepted the call.
func (w *Worker) send(a *activeJob, what string, ok Outcome, call func(context.Context) error) Outcome {
	for attempt := 0; ; attempt++ {
		deadline := a.leaseDeadline()
		ctx, cancel := context.WithTimeout(context.Background(), max(time.Until(deadline), w.finishTimeout))
		err := call(ctx)
		cancel()
		switch {
		case err == nil:
			return ok
		case status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound:
			w.log.Info(what+" rejected: the lease is no longer ours", "job_id", a.job.ID, "err", err)
			return OutcomeLeaseLost
		case status.Code(err) == codes.Canceled:
			// The Client was closed: no retry can get through.
			w.log.Warn(what+" cancelled: the client is closed (the job will be retried)", "job_id", a.job.ID, "err", err)
			return OutcomeGaveUp
		case fatal(err):
			w.log.Error(what+" failed", "job_id", a.job.ID, "err", err)
			return OutcomeGaveUp
		}
		delay := w.retry.Delay(attempt)
		if time.Now().Add(delay).After(deadline) {
			w.log.Warn(what+" failed; giving up because the lease expires (the job will be retried)", "job_id", a.job.ID, "err", err)
			return OutcomeGaveUp
		}
		time.Sleep(delay)
	}
}
