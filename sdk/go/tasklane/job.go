package tasklane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// finishTimeout bounds an Ack or Nack attempt whose lease deadline has
// already passed (it may still succeed: nobody else holds the job until the
// reaper runs), and each Nack sent at shutdown.
const finishTimeout = 2 * time.Second

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
	job *LeasedJob

	mu sync.Mutex
	// deadline is when the lease expires by this process's monotonic clock.
	// It is counted from before the request that set the lease was sent, so
	// it is never later than the server's lease_expires_at.
	deadline time.Time
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
// leasedAt is when the Lease request was sent.
func (w *Worker) runJob(r *run, pj *tasklanev1.Job, leasedAt time.Time) {
	defer func() { <-r.slots }()
	job, err := leasedJobFromProto(pj)
	a := &activeJob{job: job, deadline: leasedAt.Add(r.vt)}
	if err == nil {
		// Not derived from Run's context: a shutdown lets running jobs finish.
		ctx, cancel := context.WithCancel(context.Background())
		err = w.call(ctx, job)
		cancel()
	}
	w.finish(a, err)
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
// otherwise.
func (w *Worker) finish(a *activeJob, err error) {
	if err == nil {
		w.send(a, "ack", func(ctx context.Context) error {
			_, err := w.client.api.Ack(ctx, &tasklanev1.AckRequest{Id: a.job.ID, LeaseToken: a.job.token})
			return err
		})
		return
	}
	w.log.Warn("job failed", "job_id", a.job.ID, "kind", a.job.Kind, "attempt", a.job.Attempt, "err", err)
	req := &tasklanev1.NackRequest{Id: a.job.ID, LeaseToken: a.job.token, Error: err.Error()}
	var ra *retryAtError
	if errors.As(err, &ra) {
		req.RetryAt = timestamppb.New(ra.at)
	}
	w.send(a, "nack", func(ctx context.Context) error {
		_, err := w.client.api.Nack(ctx, req)
		return err
	})
}

// send makes the job's final Ack or Nack call, retrying with backoff while
// the server is unreachable: a retried Ack is idempotent, and a repeated Nack
// gets a harmless FAILED_PRECONDITION. It gives up once the lease would have
// expired, because the token is useless then and the reaper retries the job.
func (w *Worker) send(a *activeJob, what string, call func(context.Context) error) {
	for attempt := 0; ; attempt++ {
		deadline := a.leaseDeadline()
		ctx, cancel := context.WithTimeout(context.Background(), max(time.Until(deadline), finishTimeout))
		err := call(ctx)
		cancel()
		switch {
		case err == nil:
			return
		case status.Code(err) == codes.FailedPrecondition:
			w.log.Info(what+" rejected: the lease is no longer ours", "job_id", a.job.ID, "err", err)
			return
		case fatal(err):
			w.log.Error(what+" failed", "job_id", a.job.ID, "err", err)
			return
		}
		delay := w.retry.Delay(attempt)
		if time.Now().Add(delay).After(deadline) {
			w.log.Warn(what+" failed; giving up because the lease expires (the job will be retried)", "job_id", a.job.ID, "err", err)
			return
		}
		time.Sleep(delay)
	}
}
