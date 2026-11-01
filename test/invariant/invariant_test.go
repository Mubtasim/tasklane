package invariant

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/ledger"
	"github.com/mubtasim/tasklane/internal/metrics"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
	"github.com/mubtasim/tasklane/test/invariants"
)

const (
	queue = "inv"
	vt    = 2 * time.Second
)

// sdkWorker is one SDK worker with its own client, so stopping it abruptly
// (closing the client) cuts only its own connection.
type sdkWorker struct {
	name    string
	client  *tasklane.Client
	cancel  context.CancelFunc
	done    chan error
	stopped bool // stopped abruptly by the producer
}

// stop cancels Run and waits for it to return. Only the first call gets
// Run's error; later calls return nil at once.
func (w *sdkWorker) stop() error {
	w.cancel()
	err, ok := <-w.done
	if ok {
		close(w.done)
	}
	return err
}

func TestInvariants(t *testing.T) {
	workers, jobs := 10, 1000
	drainTimeout := 60 * time.Second
	if *full {
		workers, jobs = 50, 10000
		drainTimeout = 180 * time.Second
	}
	seed := *seedArg
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("seed %d (replay with -invariant.seed=%d)", seed, seed)
	began := time.Now()

	ctx := context.Background()
	require.NoError(t, testDB.Reset(ctx))
	addrs := make([]string, 2)
	ms := make([]*metrics.Metrics, 2)
	for i := range addrs {
		addrs[i], ms[i] = startServer(t, ctx, testDB)
	}

	producers := make([]*tasklane.Client, 2)
	for i, addr := range addrs {
		c, err := tasklane.Dial(addr)
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })
		producers[i] = c
	}
	_, err := producers[0].CreateQueue(ctx, tasklane.QueueConfig{Name: queue, VisibilityTimeout: vt, MaxAttempts: 3, MaxDepth: int64(jobs) * 2})
	require.NoError(t, err)

	led := &ledgerMem{}
	ws := make([]*sdkWorker, workers)
	for i := range ws {
		ws[i] = startWorker(t, fmt.Sprintf("w%d", i), addrs[i%2], led)
	}

	// The zombies use the plain API on their own connections, one per server.
	zctx, stopZombies := context.WithCancel(ctx)
	var zombies sync.WaitGroup
	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		zombies.Add(1)
		go func() {
			defer zombies.Done()
			runZombie(zctx, tasklanev1.NewTasklaneServiceClient(conn), fmt.Sprintf("zombie%d", i), queue, vt, led)
		}()
	}
	// Registered after the servers, so it runs before they stop.
	t.Cleanup(func() {
		stopZombies()
		zombies.Wait()
	})

	produce(t, seed, jobs, producers, ws, led)

	// Wait for the zombies' last stalled jobs, so their runs are recorded
	// before the check.
	stopZombies()
	zombies.Wait()

	waitDrained(t, seed, drainTimeout)

	// Waiting for the stopped workers too: their last gave_up runs are
	// recorded only when their Run returns.
	for _, w := range ws {
		err := w.stop()
		switch {
		case !w.stopped:
			require.NoError(t, err, "worker %s Run", w.name)
		case err != nil:
			// After Close every call fails with Canceled, which the worker
			// retries, so an error here is unexpected but not a correctness
			// failure.
			t.Logf("stopped worker %s: Run: %v", w.name, err)
		}
	}

	final, err := invariants.LoadJobs(ctx, testDB.Pool, queue)
	require.NoError(t, err)
	led.mu.Lock()
	defer led.mu.Unlock()
	if vs := invariants.Check(invariants.Config{VisibilityTimeout: vt, Tolerance: 500 * time.Millisecond}, led.enq, led.runs, final); len(vs) > 0 {
		t.Fatalf("seed %d\n%s", seed, invariants.Report(vs, led.runs, 20))
	}

	outcomes := map[string]int{}
	for _, r := range led.runs {
		outcomes[r.Outcome]++
	}
	states := map[string]int{}
	for _, j := range final {
		states[j.State]++
	}
	t.Logf("summary: %d runs (%s), %d completed, %d dead, %d lease_lost, %v",
		len(led.runs), formatCounts(outcomes), states["completed"], states["dead"], outcomes["lease_lost"],
		time.Since(began).Round(100*time.Millisecond))

	// Rule 5: both servers' counters together match the database.
	var acked, dead, reaped float64
	for _, m := range ms {
		acked += m.Value("tasklane_jobs_acked_total", "queue", queue)
		dead += m.Value("tasklane_jobs_dead_total", "queue", queue, "reason", "nacked") +
			m.Value("tasklane_jobs_dead_total", "queue", queue, "reason", "expired")
		reaped += m.Value("tasklane_jobs_reaped_total", "queue", queue)
	}
	require.Equal(t, float64(states["completed"]), acked, "seed %d: acked counter vs completed jobs", seed)
	require.Equal(t, float64(states["dead"]), dead, "seed %d: dead counter vs dead jobs", seed)
	require.GreaterOrEqual(t, reaped, float64(outcomes["lease_lost"]), "seed %d: reaped counter vs lease_lost runs", seed)
	require.Positive(t, outcomes["lease_lost"],
		"seed %d: no run lost its lease, so the zombies were never fenced off and fencing went untested", seed)

	require.Len(t, led.enq, jobs)
}

// startWorker runs an SDK worker on addr that records every run in led. Its
// cleanup (before the servers stop, since it is registered later) makes sure
// Run has returned even if the test fails early.
func startWorker(t *testing.T, name, addr string, led *ledgerMem) *sdkWorker {
	t.Helper()
	c, err := tasklane.Dial(addr)
	require.NoError(t, err)
	w := c.NewWorker(queue, tasklane.WorkerOptions{
		Concurrency:     5,
		ShutdownTimeout: time.Second,
		Logger:          discard,
		OnResult:        func(r tasklane.Result) { led.addRun(ledger.RunFromResult(name, r)) },
	})
	w.Handle("normal", normal)
	w.Handle("flaky", flaky)
	w.Handle("poison", poison)
	ctx, cancel := context.WithCancel(context.Background())
	sw := &sdkWorker{name: name, client: c, cancel: cancel, done: make(chan error, 1)}
	go func() { sw.done <- w.Run(ctx) }()
	t.Cleanup(func() {
		_ = sw.stop()
		c.Close()
	})
	return sw
}

// produce enqueues n jobs, alternating between the servers, with a seeded
// mix of kinds. It runs on the test goroutine and is the only user of rng,
// so a seed replays the same jobs and the same worker stops. At 30 % and
// 60 % it stops a random worker abruptly.
func produce(t *testing.T, seed int64, n int, producers []*tasklane.Client, ws []*sdkWorker, led *ledgerMem) {
	t.Helper()
	rng := rand.New(rand.NewPCG(uint64(seed), 0))
	stopAt := map[int]bool{n * 3 / 10: true, n * 6 / 10: true}
	for i := range n {
		if stopAt[i] {
			stopAbruptly(t, rng, ws)
		}
		job := tasklane.Job{Kind: "normal", IdempotencyKey: fmt.Sprintf("inv-%d", i)}
		switch p := rng.IntN(100); {
		case p < 3:
			job.Kind = "poison"
		case p < 15:
			job.Kind = "flaky"
			job.Payload = map[string]int{"fails": 1 + rng.IntN(2)}
		}
		// A failed enqueue is retried with the same key, so it can never
		// create a second job; give up only if the server stays unusable.
		c := producers[i%2]
		deadline := time.Now().Add(30 * time.Second)
		for {
			res, err := c.Enqueue(context.Background(), queue, job)
			if err == nil {
				led.addEnq(ledger.Enqueued{Key: job.IdempotencyKey, JobID: res.JobID})
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("seed %d: enqueue %s keeps failing: %v", seed, job.IdempotencyKey, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// stopAbruptly kills a random running worker the hard way: its connection
// closes first, so the Acks and Nacks of its running jobs fail (gave_up) and
// their leases are left for the reaper; only then is Run told to stop.
func stopAbruptly(t *testing.T, rng *rand.Rand, ws []*sdkWorker) {
	t.Helper()
	var running []*sdkWorker
	for _, w := range ws {
		if !w.stopped {
			running = append(running, w)
		}
	}
	w := running[rng.IntN(len(running))]
	w.stopped = true
	w.client.Close()
	// Not waiting here: Run takes up to ShutdownTimeout + 2 s to give up on
	// its jobs, and the producer should keep going meanwhile.
	w.cancel()
	t.Logf("stopped worker %s abruptly", w.name)
}

// waitDrained waits until no job of the queue is available or leased.
func waitDrained(t *testing.T, seed int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var n int
		err := testDB.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE queue = $1 AND state IN ('available', 'leased')`, queue).Scan(&n)
		require.NoError(t, err)
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed %d: %d jobs still available or leased after %v", seed, n, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func formatCounts(c map[string]int) string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, c[k])
	}
	return strings.Join(parts, ", ")
}
