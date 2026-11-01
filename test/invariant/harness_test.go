package invariant

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/api"
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/ledger"
	"github.com/mubtasim/tasklane/internal/metrics"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/reaper"
	"github.com/mubtasim/tasklane/internal/testpg"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// startServer runs one Tasklane server on db, as cmd/tasklane would: its own
// notify listener, depth refresh and reaper, so two of them exercise the
// background loops running side by side on one database. It serves gRPC on
// a real TCP port, so closing a client really drops its connection.
func startServer(t *testing.T, ctx context.Context, db *testpg.DB) (addr string, m *metrics.Metrics) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	var bg sync.WaitGroup

	n, err := notify.New(db.DSN, discard)
	require.NoError(t, err)
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = n.Run(ctx) // always nil once ctx is done
	}()
	// Leases wait on notifications; without a listening connection the first
	// jobs would only be found by the poll fallback.
	for i := 0; !n.Listening(); i++ {
		if i == 500 {
			cancel()
			t.Fatal("notify listener did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	m = metrics.New()
	st := pgstore.New(db.Pool, backoff.Policy{Base: 100 * time.Millisecond, Cap: time.Second, Rand: rand.Float64})
	srv := api.NewServer(st, n, discard, api.WithMetrics(m), api.WithPollInterval(100*time.Millisecond))
	bg.Add(2)
	go func() {
		defer bg.Done()
		srv.RunDepthRefresh(ctx)
	}()
	go func() {
		defer bg.Done()
		reaper.Run(ctx, st, 200*time.Millisecond, reaper.DefaultBatch, discard, m)
	}()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := api.NewGRPCServer(srv, nil, discard)
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = gs.Serve(lis)
	}()

	t.Cleanup(func() {
		srv.Shutdown()
		gs.Stop()
		cancel()
		bg.Wait()
	})
	return lis.Addr().String(), m
}

// ledgerMem is the in-memory ledger: worker callbacks and the producer append
// to it concurrently.
type ledgerMem struct {
	mu   sync.Mutex
	runs []ledger.Run
	enq  []ledger.Enqueued
}

func (l *ledgerMem) addRun(r ledger.Run) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.runs = append(l.runs, r)
}

func (l *ledgerMem) addEnq(e ledger.Enqueued) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enq = append(l.enq, e)
}

// normal takes a few milliseconds, varied by job so runs overlap unevenly.
func normal(ctx context.Context, job *tasklane.LeasedJob) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(5+job.ID%20) * time.Millisecond):
		return nil
	}
}

// flaky fails its first n attempts, then succeeds.
func flaky(_ context.Context, job *tasklane.LeasedJob) error {
	var p struct {
		Fails int `json:"fails"`
	}
	if err := job.Decode(&p); err != nil {
		return err
	}
	if job.Attempt <= p.Fails {
		return errors.New("flaky: planned failure")
	}
	return nil
}

func poison(context.Context, *tasklane.LeasedJob) error {
	return errors.New("poison: always fails")
}

// runZombie is a worker that stalls: it leases one job at a time, sends no
// heartbeats and acks only after its lease has expired. By then the reaper
// has normally taken the job back (and another worker may be running it),
// so the late Ack must be refused: that is the fencing the test checks. It
// runs until ctx is done, but always finishes the job it holds, so its last
// run is recorded too.
func runZombie(ctx context.Context, raw tasklanev1.TasklaneServiceClient, name, queue string, vt time.Duration, led *ledgerMem) {
	for ctx.Err() == nil {
		res, err := raw.Lease(ctx, &tasklanev1.LeaseRequest{
			Queue:             queue,
			MaxJobs:           1,
			Wait:              durationpb.New(time.Second),
			VisibilityTimeout: durationpb.New(vt),
		})
		if err != nil {
			if ctx.Err() == nil {
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
		for _, job := range res.GetJobs() {
			start := time.Now()
			time.Sleep(vt + 500*time.Millisecond)
			// A fresh context: the zombie may have been told to stop while
			// it slept, and its Ack must still be sent and recorded.
			ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := raw.Ack(ackCtx, &tasklanev1.AckRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken()})
			cancel()
			outcome := "acked"
			switch status.Code(err) {
			case codes.OK:
			case codes.FailedPrecondition, codes.NotFound:
				outcome = "lease_lost"
			default:
				outcome = "gave_up"
			}
			led.addRun(ledger.Run{Worker: name, JobID: job.GetId(), Attempt: int(job.GetAttempts()), Start: start, End: time.Now(), Outcome: outcome})
		}
	}
}
