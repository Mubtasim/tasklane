package tasklane

import (
	"errors"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrQueueFull matches, with errors.Is, the error Enqueue and EnqueueBatch
// return when the queue has reached its max_depth. The error is a
// *QueueFullError carrying the server's retry-after hint. The client does
// not retry it: a full queue means the workers are behind, which the
// producer's owner should see.
var ErrQueueFull = errors.New("tasklane: queue full")

// QueueFullError is the error behind ErrQueueFull. It unwraps to the gRPC
// status error, so status.Code(err) is still codes.ResourceExhausted.
type QueueFullError struct {
	RetryAfter time.Duration
	err        error
}

func (e *QueueFullError) Error() string        { return e.err.Error() }
func (e *QueueFullError) Unwrap() error        { return e.err }
func (e *QueueFullError) Is(target error) bool { return target == ErrQueueFull }

// queueFull turns the server's queue-full status (RESOURCE_EXHAUSTED with a
// RetryInfo detail) into a *QueueFullError and returns other errors as they
// are. A message over the size limit is RESOURCE_EXHAUSTED too, but without
// RetryInfo.
func queueFull(err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return err
	}
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			return &QueueFullError{RetryAfter: ri.GetRetryDelay().AsDuration(), err: err}
		}
	}
	return err
}
