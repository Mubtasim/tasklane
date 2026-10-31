package tasklane

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// heartbeat extends a's lease every vt/3 until ctx is cancelled, so one lost
// or late heartbeat doesn't lose the lease. It uses this process's monotonic
// clock and never compares the server's lease_expires_at with the local
// clock, so clock skew between the worker and the database can't break it.
// When the server says the lease is gone, it cancels the handler.
func (w *Worker) heartbeat(ctx context.Context, a *activeJob, vt time.Duration) {
	interval := vt / 3
	if interval <= 0 {
		interval = vt
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sent := time.Now()
		hctx, cancel := context.WithTimeout(ctx, interval)
		_, err := w.client.api.Heartbeat(hctx, &tasklanev1.HeartbeatRequest{
			Id:                a.job.ID,
			LeaseToken:        a.job.token,
			VisibilityTimeout: durationpb.New(vt),
		})
		cancel()
		switch code := status.Code(err); {
		case err == nil:
			a.extendLease(sent.Add(vt))
		case ctx.Err() != nil:
			return
		case code == codes.FailedPrecondition || code == codes.NotFound:
			// Another worker may hold the job now. Stopping the handler
			// shortens the time both run it; it can't remove it, which is
			// why delivery is at-least-once and handlers must be idempotent.
			w.log.Warn("lease lost; cancelling the handler", "job_id", a.job.ID, "kind", a.job.Kind, "err", err)
			a.lost.Store(true)
			a.cancel()
			return
		default:
			w.log.Warn("heartbeat failed; retrying at the next tick", "job_id", a.job.ID, "err", err)
		}
	}
}
