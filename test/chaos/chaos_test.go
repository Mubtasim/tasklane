//go:build chaos

// Package chaos runs the Docker demo stack, kills and freezes its containers
// while jobs flow, and checks the correctness rules of design §17.2 against
// the run ledgers and the jobs table (design §17.5). It needs Docker and
// runs only with the chaos build tag:
//
//	go test -tags chaos -count=1 -v -timeout 15m ./test/chaos/
package chaos

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mubtasim/tasklane/internal/ledger"
	"github.com/mubtasim/tasklane/test/invariants"
)

var (
	seedArg  = flag.Int64("chaos.seed", 0, "seed for the fault sequence; 0 picks one from the clock")
	duration = flag.Duration("chaos.duration", 3*time.Minute, "how long faults are injected")
	interval = flag.Duration("chaos.interval", 15*time.Second, "time from the start of one fault to the start of the next")
	keep     = flag.Bool("chaos.keep", false, "keep the ledger directory and compose log in the temp directory")
)

const (
	queue       = "chaos"
	vt          = 10 * time.Second // DEMO_VISIBILITY_TIMEOUT in deploy/chaos.override.yml
	databaseURL = "postgres://tasklane:tasklane@127.0.0.1:5433/tasklane?sslmode=disable"
	readyURL    = "http://127.0.0.1:8080/readyz"
)

// The services that must be running after the heal. The producer is left
// out: it is stopped on purpose before the drain.
var services = []string{"postgres", "tasklane-1", "tasklane-2", "envoy", "worker-1", "worker-2", "worker-3"}

func TestChaos(t *testing.T) {
	seed := *seedArg
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("seed %d (replay with -chaos.seed=%d)", seed, seed)
	began := time.Now()

	dir, err := os.MkdirTemp("", "tasklane-chaos-")
	if err != nil {
		t.Fatal(err)
	}
	// The containers run as uid 65532 and write their ledgers here.
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	s := newStack(t, dir)

	// A stack left behind by an interrupted run would hold the ports; this
	// only removes containers of the tasklane-chaos project.
	if err := s.down(); err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{"127.0.0.1:7070", "127.0.0.1:8080", "127.0.0.1:5433", "127.0.0.1:9090", "127.0.0.1:3000"} {
		if l, err := net.Listen("tcp", addr); err != nil {
			t.Fatalf("%s is in use; stop the demo first: docker compose -f deploy/docker-compose.yml down", addr)
		} else {
			l.Close()
		}
	}

	// Cleanups run last-registered first: the logs are saved before the
	// stack goes down.
	t.Cleanup(func() {
		if err := s.down(); err != nil {
			t.Errorf("down: %v", err)
		}
	})
	t.Cleanup(func() { saveOutput(t, s, dir, seed) })

	t.Log("building and starting the stack")
	if err := s.up(); err != nil {
		t.Fatal(err)
	}
	// The distroless images have no shell for a healthcheck, so up --wait
	// can't tell when the servers are ready; Envoy's /readyz answers 200
	// once a server behind it is ready.
	waitReady(t, 3*time.Minute)
	t.Logf("stack ready after %v", time.Since(began).Round(time.Second))

	faults, frozen := injectFaults(t, s, seed)

	t.Log("healing: stopping the producer, starting everything else")
	if err := s.stop("producer"); err != nil {
		t.Fatal(err)
	}
	if err := s.start(services...); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	waitDrained(t, ctx, pool, seed, 2*time.Minute)

	// A worker writes a run when its Result is final; stopping the workers
	// cleanly makes sure every run is in the ledger before it is read.
	if err := s.stop("worker-1", "worker-2", "worker-3"); err != nil {
		t.Fatal(err)
	}

	runs, err := ledger.ReadRuns(dir)
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := ledger.ReadEnqueued(dir)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := invariants.LoadJobs(ctx, pool, queue)
	if err != nil {
		t.Fatal(err)
	}
	if len(enqueued) == 0 || len(runs) == 0 {
		t.Fatalf("seed %d: empty ledgers (%d enqueued, %d runs): the producer or workers didn't write to %s", seed, len(enqueued), len(runs), dir)
	}
	// Lease responses cross Envoy, so the worker's recorded start can trail
	// the server's lease time a little; the clocks themselves are the host's.
	// The pause windows tell rule 4 which recorded starts may be late.
	cfg := invariants.Config{VisibilityTimeout: vt, Tolerance: time.Second, Frozen: frozen}
	if vs := invariants.Check(cfg, enqueued, runs, jobs); len(vs) > 0 {
		t.Fatalf("seed %d\n%s", seed, invariants.Report(vs, runs, 20))
	}

	outcomes := map[string]int{}
	for _, r := range runs {
		outcomes[r.Outcome]++
	}
	states := map[string]int{}
	for _, j := range jobs {
		states[j.State]++
	}
	t.Logf("summary: seed %d, faults (%s), %d enqueued, %d jobs, %d completed, %d dead, %d runs (%s), %d lease_lost, %v",
		seed, formatCounts(faults), len(enqueued), len(jobs), states["completed"], states["dead"],
		len(runs), formatCounts(outcomes), outcomes["lease_lost"], time.Since(began).Round(time.Second))
	if faults["pauseWorker"] > 0 && outcomes["lease_lost"] == 0 {
		// The reaper and the unpause race: the frozen worker's jobs may have
		// been reaped and finished elsewhere, or its leases may have just
		// survived, so this is not a failure.
		t.Logf("warning: a worker was paused but no run lost its lease; the zombie path was not exercised")
	}
}

// injectFaults runs one fault at a time, chosen by the seed, until the
// duration ends. It returns how many of each type it injected and when each
// paused worker was frozen (from before the pause to after the unpause).
func injectFaults(t *testing.T, s *stack, seed int64) (map[string]int, []invariants.Freeze) {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)>>32))
	counts := map[string]int{}
	var frozen []invariants.Freeze
	end := time.Now().Add(*duration)
	must := func(err error) {
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	for time.Now().Before(end) {
		began := time.Now()
		var kind, target string
		switch rng.IntN(4) {
		case 0:
			kind, target = "killWorker", fmt.Sprintf("worker-%d", rng.IntN(3)+1)
			logFault(t, kind, target)
			must(s.kill(target))
			time.Sleep(10 * time.Second)
			must(s.start(target))
		case 1:
			// Faults are sequential and the server is back before the next
			// one, so both servers are never down at once.
			kind, target = "killServer", fmt.Sprintf("tasklane-%d", rng.IntN(2)+1)
			logFault(t, kind, target)
			must(s.kill(target))
			time.Sleep(15 * time.Second)
			must(s.start(target))
		case 2:
			// Frozen longer than the visibility timeout: its leases are
			// reaped and given to others, and its late Acks must be rejected.
			kind, target = "pauseWorker", fmt.Sprintf("worker-%d", rng.IntN(3)+1)
			logFault(t, kind, target)
			from := time.Now()
			must(s.pause(target))
			time.Sleep(15 * time.Second)
			must(s.unpause(target))
			frozen = append(frozen, invariants.Freeze{Worker: target, From: from, To: time.Now()})
		case 3:
			kind, target = "restartPostgres", "postgres"
			logFault(t, kind, target)
			must(s.restart(target))
		}
		counts[kind]++
		t.Logf("%s %s %s done after %v", time.Now().Format("15:04:05"), kind, target, time.Since(began).Round(100*time.Millisecond))
		time.Sleep(max(*interval-time.Since(began), 2*time.Second))
	}
	return counts, frozen
}

func logFault(t *testing.T, kind, target string) {
	t.Logf("%s %s %s", time.Now().Format("15:04:05"), kind, target)
}

func waitReady(t *testing.T, timeout time.Duration) {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for {
		resp, err := client.Get(readyURL)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready after %v (last error: %v)", readyURL, timeout, err)
		}
		time.Sleep(time.Second)
	}
}

// waitDrained waits until no job of the queue is available or leased.
// Available includes jobs waiting out a retry backoff.
func waitDrained(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seed int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		var open int
		lastErr = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1 AND state IN ('available', 'leased')`, queue).Scan(&open)
		if lastErr == nil && open == 0 {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	counts := map[string]int{}
	rows, err := pool.Query(ctx, `SELECT state::text, count(*) FROM jobs WHERE queue = $1 GROUP BY state`, queue)
	if err == nil {
		for rows.Next() {
			var state string
			var n int
			if rows.Scan(&state, &n) == nil {
				counts[state] = n
			}
		}
		rows.Close()
	}
	t.Fatalf("seed %d: queue not drained after %v: %s (last error: %v)", seed, timeout, formatCounts(counts), lastErr)
}

// saveOutput keeps what is needed to investigate a failure: the compose
// logs next to the ledgers, either in the temp directory (-chaos.keep) or
// copied to test/chaos/out/, which CI uploads.
func saveOutput(t *testing.T, s *stack, dir string, seed int64) {
	if t.Failed() {
		logPath := filepath.Join(dir, "compose.log")
		if err := s.logs(logPath); err != nil {
			t.Logf("saving compose logs: %v", err)
		}
	}
	if *keep {
		t.Logf("ledgers and logs kept in %s", dir)
		return
	}
	if t.Failed() {
		out := filepath.Join("out", fmt.Sprintf("%s-seed-%d", time.Now().Format("20060102-150405"), seed))
		if err := copyDir(dir, out); err != nil {
			t.Logf("copying the output: %v (left in %s)", err, dir)
			return
		}
		t.Logf("ledgers and logs saved in test/chaos/%s", out)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Logf("removing %s: %v", dir, err)
	}
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func formatCounts(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	return strings.Join(parts, " ")
}
