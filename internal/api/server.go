// Package api implements the TasklaneService gRPC API over store.Store, the
// API-key interceptor and the HTTP/JSON gateway.
package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/metrics"
	"github.com/mubtasim/tasklane/internal/store"
)

const (
	// MaxWait caps a long-polling Lease (design §6).
	MaxWait = 30 * time.Second
	// DefaultPollInterval is how often a waiting Lease re-checks without a
	// notification: delayed jobs and retries become due silently.
	DefaultPollInterval = time.Second
	// storeWriteTimeout bounds a store write that runs detached from its
	// client (see detach). A write takes milliseconds when the database is
	// healthy, so this only matters when it is stuck. It is kept under the
	// server's 10 s graceful-stop budget, so a detached write never holds a
	// drain past it, and well under the 30 s default visibility timeout, so
	// a lease taken by a slow write is still worth handing over.
	storeWriteTimeout = 5 * time.Second
)

// detach returns a context for one store write that ignores the client's
// cancellation but is bounded by storeWriteTimeout. Postgres can commit a
// write and pgx still return "context canceled" when the client goes away
// mid-call; the handler would then report a committed write as failed and
// skip its metrics, and the counters would drift from the database. On a
// detached context, a write that starts also finishes, and its bookkeeping
// runs, whatever the client does. Values (such as the request's metadata)
// are kept.
func detach(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), storeWriteTimeout)
}

// Notifier wakes waiting leases when a queue gets new jobs; *notify.Listener
// implements it.
type Notifier interface {
	Subscribe(queue string) (<-chan struct{}, func())
}

type Server struct {
	tasklanev1.UnimplementedTasklaneServiceServer

	store        store.Store
	notifier     Notifier
	log          *slog.Logger
	pollInterval time.Duration
	depths       *depthCache
	metrics      *metrics.Metrics // nil records nothing

	stopOnce sync.Once
	stopping chan struct{}
}

type Option func(*Server)

// WithMetrics records the server's metrics in m (design §16.2).
func WithMetrics(m *metrics.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithPollInterval overrides DefaultPollInterval (tests use it). A d <= 0
// is ignored.
func WithPollInterval(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

func NewServer(st store.Store, n Notifier, log *slog.Logger, opts ...Option) *Server {
	s := &Server{store: st, notifier: n, log: log, pollInterval: DefaultPollInterval, depths: newDepthCache(), stopping: make(chan struct{})}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Shutdown makes waiting leases return their (empty) result now, so a
// graceful stop doesn't wait out 30 s long polls, and later leases fail with
// UNAVAILABLE, so workers back off instead of polling a draining server.
// Call it before grpc.Server.GracefulStop.
func (s *Server) Shutdown() {
	s.stopOnce.Do(func() { close(s.stopping) })
}

func (s *Server) CreateQueue(ctx context.Context, req *tasklanev1.CreateQueueRequest) (*tasklanev1.CreateQueueResponse, error) {
	q, err := queueToStore(req.GetQueue())
	if err != nil {
		return nil, err
	}
	wctx, cancel := detach(ctx)
	defer cancel()
	if err := s.store.CreateQueue(wctx, q); err != nil {
		return nil, toStatus(s.log, "CreateQueue", err)
	}
	// Answer with the queue as stored, so CreateQueue and GetQueue agree: the
	// store keeps the visibility timeout in whole milliseconds.
	stored := q.WithDefaults()
	stored.VisibilityTimeout = stored.VisibilityTimeout.Truncate(time.Millisecond)
	return &tasklanev1.CreateQueueResponse{Queue: queueFromStore(stored)}, nil
}

func (s *Server) GetQueue(ctx context.Context, req *tasklanev1.GetQueueRequest) (*tasklanev1.GetQueueResponse, error) {
	q, err := s.store.GetQueue(ctx, req.GetName())
	if err != nil {
		return nil, toStatus(s.log, "GetQueue", err)
	}
	stats, err := s.store.QueueStats(ctx, req.GetName())
	if err != nil {
		return nil, toStatus(s.log, "GetQueue", err)
	}
	return &tasklanev1.GetQueueResponse{Queue: queueFromStore(q), Stats: statsFromStore(stats)}, nil
}

func (s *Server) Enqueue(ctx context.Context, req *tasklanev1.EnqueueRequest) (*tasklanev1.EnqueueResponse, error) {
	job, err := jobSpecToStore(req.GetQueue(), req.GetJob())
	if err != nil {
		return nil, err
	}
	if s.depths.isFull(req.GetQueue()) {
		s.metrics.EnqueueRefused(req.GetQueue())
		return nil, queueFullError(req.GetQueue())
	}
	wctx, cancel := detach(ctx)
	defer cancel()
	res, err := s.store.Enqueue(wctx, []store.NewJob{job})
	if err != nil {
		return nil, toStatus(s.log, "Enqueue", err)
	}
	if res[0].Created {
		s.metrics.Enqueued(req.GetQueue(), 1)
	}
	return &tasklanev1.EnqueueResponse{JobId: res[0].ID, Created: res[0].Created}, nil
}

func (s *Server) EnqueueBatch(ctx context.Context, req *tasklanev1.EnqueueBatchRequest) (*tasklanev1.EnqueueBatchResponse, error) {
	if n := proto.Size(req); n > MaxMessageBytes-maxBatchSlack {
		return nil, invalid("batch must be at most %d bytes as protobuf, got %d", MaxMessageBytes-maxBatchSlack, n)
	}
	if s.depths.isFull(req.GetQueue()) {
		s.metrics.EnqueueRefused(req.GetQueue())
		return nil, queueFullError(req.GetQueue())
	}
	jobs := make([]store.NewJob, 0, len(req.GetJobs()))
	for i, spec := range req.GetJobs() {
		j, err := jobSpecToStore(req.GetQueue(), spec)
		if err != nil {
			return nil, invalid("jobs[%d]: %s", i, status.Convert(err).Message())
		}
		jobs = append(jobs, j)
	}
	wctx, cancel := detach(ctx)
	defer cancel()
	res, err := s.store.Enqueue(wctx, jobs) // the store rejects 0 or > 1000 jobs
	if err != nil {
		return nil, toStatus(s.log, "EnqueueBatch", err)
	}
	out := &tasklanev1.EnqueueBatchResponse{Results: make([]*tasklanev1.EnqueueResult, len(res))}
	created := 0
	for i, r := range res {
		out.Results[i] = &tasklanev1.EnqueueResult{JobId: r.ID, Created: r.Created}
		if r.Created {
			created++
		}
	}
	s.metrics.Enqueued(req.GetQueue(), created)
	return out, nil
}

// Lease tries once; with wait > 0 and nothing runnable, it waits for a
// notification for the queue, or the next poll tick, and tries again until
// the wait elapses. It subscribes before the first try, so a job enqueued
// between that try and the wait still wakes it. max_jobs above MaxLeaseJobs
// is clamped to it silently, so the response fits in MaxMessageBytes.
func (s *Server) Lease(ctx context.Context, req *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error) {
	select {
	case <-s.stopping:
		return nil, status.Error(codes.Unavailable, "server shutting down")
	default:
	}
	n := int(req.GetMaxJobs())
	if n == 0 {
		n = 1
	}
	n = min(n, MaxLeaseJobs) // a negative n still reaches the store's validation
	wait, err := durationToStore("wait", req.GetWait())
	if err != nil {
		return nil, err
	}
	wait = min(wait, MaxWait)
	ttl, err := durationToStore("visibility_timeout", req.GetVisibilityTimeout())
	if err != nil {
		return nil, err
	}

	var wake <-chan struct{} // nil (never ready) without a notifier
	if wait > 0 && s.notifier != nil {
		ch, unsubscribe := s.notifier.Subscribe(req.GetQueue())
		defer unsubscribe()
		wake = ch
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(s.pollInterval)
	defer poll.Stop()

	for {
		jobs, err := s.lease(ctx, req.GetQueue(), n, ttl)
		if err != nil {
			return nil, toStatus(s.log, "Lease", err)
		}
		if len(jobs) > 0 || wait <= 0 {
			s.metrics.Leased(jobs)
			resp, err := leaseResponse(jobs)
			return resp, toStatus(s.log, "Lease", err)
		}
		select {
		case <-ctx.Done():
			return nil, toStatus(s.log, "Lease", ctx.Err())
		case <-deadline.C:
			return &tasklanev1.LeaseResponse{}, nil
		case <-s.stopping:
			return &tasklanev1.LeaseResponse{}, nil
		case <-wake:
		case <-poll.C:
		}
		// select picks at random among ready cases, so a wake-up can win
		// over a cancel that came at the same time. A gone client must not
		// be handed jobs it will never run: they would sit leased until
		// their visibility timeout.
		if err := ctx.Err(); err != nil {
			return nil, toStatus(s.log, "Lease", err)
		}
	}
}

// lease is one lease attempt, detached from the client (see detach): once
// the transaction starts, the jobs it leases are counted and returned. Only
// Lease's wait between attempts follows the client.
func (s *Server) lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]store.Job, error) {
	wctx, cancel := detach(ctx)
	defer cancel()
	return s.store.Lease(wctx, queue, n, ttl)
}

func leaseResponse(jobs []store.Job) (*tasklanev1.LeaseResponse, error) {
	out := &tasklanev1.LeaseResponse{Jobs: make([]*tasklanev1.Job, 0, len(jobs))}
	for _, j := range jobs {
		pj, err := jobFromStore(j, true)
		if err != nil {
			return nil, err
		}
		out.Jobs = append(out.Jobs, pj)
	}
	return out, nil
}

func (s *Server) Ack(ctx context.Context, req *tasklanev1.AckRequest) (*tasklanev1.AckResponse, error) {
	token, err := tokenToStore(req.GetLeaseToken())
	if err != nil {
		return nil, err
	}
	wctx, cancel := detach(ctx)
	defer cancel()
	res, err := s.store.Ack(wctx, req.GetId(), token)
	if err != nil {
		return nil, toStatus(s.log, "Ack", err)
	}
	if !res.Repeated {
		s.metrics.Acked(res.Queue)
	}
	return &tasklanev1.AckResponse{}, nil
}

func (s *Server) Nack(ctx context.Context, req *tasklanev1.NackRequest) (*tasklanev1.NackResponse, error) {
	token, err := tokenToStore(req.GetLeaseToken())
	if err != nil {
		return nil, err
	}
	var retryAt *time.Time
	if req.GetRetryAt() != nil {
		t, err := timeToStore("retry_at", req.GetRetryAt())
		if err != nil {
			return nil, err
		}
		retryAt = &t
	}
	// A long error (a stack trace, say) is truncated rather than rejected:
	// failing the Nack would only leave the job to the reaper.
	errMsg := store.TruncateUTF8(req.GetError(), store.MaxErrorBytes)
	wctx, cancel := detach(ctx)
	defer cancel()
	res, err := s.store.Nack(wctx, req.GetId(), token, errMsg, retryAt)
	if err != nil {
		return nil, toStatus(s.log, "Nack", err)
	}
	s.metrics.Nacked(res.Queue, res.Dead)
	return &tasklanev1.NackResponse{}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *tasklanev1.HeartbeatRequest) (*tasklanev1.HeartbeatResponse, error) {
	token, err := tokenToStore(req.GetLeaseToken())
	if err != nil {
		return nil, err
	}
	ttl, err := durationToStore("visibility_timeout", req.GetVisibilityTimeout())
	if err != nil {
		return nil, err
	}
	wctx, cancel := detach(ctx)
	defer cancel()
	expires, err := s.store.Heartbeat(wctx, req.GetId(), token, ttl)
	if err != nil {
		return nil, toStatus(s.log, "Heartbeat", err)
	}
	return &tasklanev1.HeartbeatResponse{LeaseExpiresAt: timeFromStore(expires)}, nil
}

func (s *Server) GetJob(ctx context.Context, req *tasklanev1.GetJobRequest) (*tasklanev1.GetJobResponse, error) {
	j, err := s.store.GetJob(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(s.log, "GetJob", err)
	}
	pj, err := jobFromStore(j, false)
	if err != nil {
		return nil, toStatus(s.log, "GetJob", err)
	}
	return &tasklanev1.GetJobResponse{Job: pj}, nil
}
