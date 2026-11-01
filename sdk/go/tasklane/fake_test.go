package tasklane

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// fakeServer is an in-memory TasklaneService for the SDK's unit tests. By
// default GetQueue answers with queue, Lease hands out the jobs given to
// push and long-polls like the real server, and Ack, Nack and Heartbeat
// succeed. Tests change behavior through the *Fn hooks (set them with set)
// and inspect the requests with calls. All fields are guarded by mu.
type fakeServer struct {
	tasklanev1.UnimplementedTasklaneServiceServer

	mu      sync.Mutex
	queue   *tasklanev1.Queue // GetQueue's answer; nil means NOT_FOUND
	stats   *tasklanev1.QueueStats
	pending []*tasklanev1.Job
	wake    chan struct{} // closed and replaced by push

	getQueueFn  func() (*tasklanev1.GetQueueResponse, error)
	leaseFn     func(ctx context.Context, req *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error)
	ackFn       func(req *tasklanev1.AckRequest) error
	nackFn      func(req *tasklanev1.NackRequest) error
	heartbeatFn func(req *tasklanev1.HeartbeatRequest) error
	enqueueErr  error // returned by Enqueue and EnqueueBatch after recording the request
	cancelFn    func(req *tasklanev1.CancelJobRequest) (*tasklanev1.CancelJobResponse, error)
	listDeadFn  func(req *tasklanev1.ListDeadJobsRequest) (*tasklanev1.ListDeadJobsResponse, error)
	redriveFn   func(req *tasklanev1.RedriveJobsRequest) (*tasklanev1.RedriveJobsResponse, error)

	recorded fakeCalls
}

// fakeCalls are the requests the fake received, in order.
type fakeCalls struct {
	auth         []string // the authorization metadata of every call
	enqueues     []*tasklanev1.EnqueueRequest
	batches      []*tasklanev1.EnqueueBatchRequest
	createQueues []*tasklanev1.CreateQueueRequest
	leases       []*tasklanev1.LeaseRequest
	acks         []*tasklanev1.AckRequest
	nacks        []*tasklanev1.NackRequest
	heartbeats   []*tasklanev1.HeartbeatRequest
}

// newFake serves a fakeServer over an in-memory connection (bufconn) and
// returns a Client dialed to it with opts.
func newFake(t *testing.T, opts ...Option) (*fakeServer, *Client) {
	t.Helper()
	f := &fakeServer{
		wake:  make(chan struct{}),
		queue: &tasklanev1.Queue{Name: "q", MaxDepth: 100_000, VisibilityTimeout: durationpb.New(30 * time.Second), MaxAttempts: 5},
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		f.mu.Lock()
		f.recorded.auth = append(f.recorded.auth, strings.Join(md.Get("authorization"), ","))
		f.mu.Unlock()
		return handler(ctx, req)
	}))
	tasklanev1.RegisterTasklaneServiceServer(gs, f)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	c, err := Dial("passthrough:///bufnet", append(opts, WithGRPCDialOptions(dialer))...)
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
		gs.Stop()
	})
	return f, c
}

// fakeJob is a leased job as the server would return it.
func fakeJob(id int64, kind string) *tasklanev1.Job {
	return &tasklanev1.Job{
		Id:          id,
		Queue:       "q",
		Kind:        kind,
		Payload:     &structpb.Struct{},
		State:       tasklanev1.JobState_JOB_STATE_LEASED,
		Attempts:    1,
		MaxAttempts: 5,
		LeaseToken:  fmt.Sprintf("token-%d", id),
	}
}

func (f *fakeServer) set(change func(f *fakeServer)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f)
}

// push makes jobs available to Lease and wakes waiting leases.
func (f *fakeServer) push(jobs ...*tasklanev1.Job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending = append(f.pending, jobs...)
	close(f.wake)
	f.wake = make(chan struct{})
}

func (f *fakeServer) calls() fakeCalls {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.recorded
	return fakeCalls{
		auth:         slices.Clone(r.auth),
		enqueues:     slices.Clone(r.enqueues),
		batches:      slices.Clone(r.batches),
		createQueues: slices.Clone(r.createQueues),
		leases:       slices.Clone(r.leases),
		acks:         slices.Clone(r.acks),
		nacks:        slices.Clone(r.nacks),
		heartbeats:   slices.Clone(r.heartbeats),
	}
}

func (f *fakeServer) CreateQueue(_ context.Context, req *tasklanev1.CreateQueueRequest) (*tasklanev1.CreateQueueResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded.createQueues = append(f.recorded.createQueues, req)
	return &tasklanev1.CreateQueueResponse{Queue: req.GetQueue()}, nil
}

func (f *fakeServer) GetQueue(_ context.Context, _ *tasklanev1.GetQueueRequest) (*tasklanev1.GetQueueResponse, error) {
	f.mu.Lock()
	fn, q, st := f.getQueueFn, f.queue, f.stats
	f.mu.Unlock()
	if fn != nil {
		return fn()
	}
	if q == nil {
		return nil, status.Error(codes.NotFound, "queue not found")
	}
	return &tasklanev1.GetQueueResponse{Queue: q, Stats: st}, nil
}

func (f *fakeServer) Enqueue(_ context.Context, req *tasklanev1.EnqueueRequest) (*tasklanev1.EnqueueResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded.enqueues = append(f.recorded.enqueues, req)
	if f.enqueueErr != nil {
		return nil, f.enqueueErr
	}
	return &tasklanev1.EnqueueResponse{JobId: int64(len(f.recorded.enqueues)), Created: true}, nil
}

func (f *fakeServer) EnqueueBatch(_ context.Context, req *tasklanev1.EnqueueBatchRequest) (*tasklanev1.EnqueueBatchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded.batches = append(f.recorded.batches, req)
	if f.enqueueErr != nil {
		return nil, f.enqueueErr
	}
	res := &tasklanev1.EnqueueBatchResponse{}
	for i := range req.GetJobs() {
		res.Results = append(res.Results, &tasklanev1.EnqueueResult{JobId: int64(i + 1), Created: true})
	}
	return res, nil
}

func (f *fakeServer) Lease(ctx context.Context, req *tasklanev1.LeaseRequest) (*tasklanev1.LeaseResponse, error) {
	f.mu.Lock()
	f.recorded.leases = append(f.recorded.leases, req)
	fn := f.leaseFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	n := max(int(req.GetMaxJobs()), 1)
	deadline := time.NewTimer(req.GetWait().AsDuration())
	defer deadline.Stop()
	for {
		f.mu.Lock()
		if len(f.pending) > 0 {
			k := min(n, len(f.pending))
			jobs := f.pending[:k:k]
			f.pending = f.pending[k:]
			f.mu.Unlock()
			return &tasklanev1.LeaseResponse{Jobs: jobs}, nil
		}
		wake := f.wake
		f.mu.Unlock()
		select {
		case <-wake:
		case <-deadline.C:
			return &tasklanev1.LeaseResponse{}, nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (f *fakeServer) Ack(_ context.Context, req *tasklanev1.AckRequest) (*tasklanev1.AckResponse, error) {
	f.mu.Lock()
	f.recorded.acks = append(f.recorded.acks, req)
	fn := f.ackFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(req); err != nil {
			return nil, err
		}
	}
	return &tasklanev1.AckResponse{}, nil
}

func (f *fakeServer) Nack(_ context.Context, req *tasklanev1.NackRequest) (*tasklanev1.NackResponse, error) {
	f.mu.Lock()
	f.recorded.nacks = append(f.recorded.nacks, req)
	fn := f.nackFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(req); err != nil {
			return nil, err
		}
	}
	return &tasklanev1.NackResponse{}, nil
}

func (f *fakeServer) Heartbeat(_ context.Context, req *tasklanev1.HeartbeatRequest) (*tasklanev1.HeartbeatResponse, error) {
	f.mu.Lock()
	f.recorded.heartbeats = append(f.recorded.heartbeats, req)
	fn := f.heartbeatFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(req); err != nil {
			return nil, err
		}
	}
	return &tasklanev1.HeartbeatResponse{LeaseExpiresAt: timestamppb.New(time.Now().Add(req.GetVisibilityTimeout().AsDuration()))}, nil
}

func (f *fakeServer) CancelJob(_ context.Context, req *tasklanev1.CancelJobRequest) (*tasklanev1.CancelJobResponse, error) {
	f.mu.Lock()
	fn := f.cancelFn
	f.mu.Unlock()
	if fn == nil {
		return nil, status.Error(codes.Unimplemented, "no cancelFn")
	}
	return fn(req)
}

func (f *fakeServer) ListDeadJobs(_ context.Context, req *tasklanev1.ListDeadJobsRequest) (*tasklanev1.ListDeadJobsResponse, error) {
	f.mu.Lock()
	fn := f.listDeadFn
	f.mu.Unlock()
	if fn == nil {
		return nil, status.Error(codes.Unimplemented, "no listDeadFn")
	}
	return fn(req)
}

func (f *fakeServer) RedriveJobs(_ context.Context, req *tasklanev1.RedriveJobsRequest) (*tasklanev1.RedriveJobsResponse, error) {
	f.mu.Lock()
	fn := f.redriveFn
	f.mu.Unlock()
	if fn == nil {
		return nil, status.Error(codes.Unimplemented, "no redriveFn")
	}
	return fn(req)
}
