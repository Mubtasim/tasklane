package invariant

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/ledger"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
	"github.com/mubtasim/tasklane/test/invariants"
)

var tsWorkers = flag.Bool("invariant.ts", false, "run TestInvariantsTS with TypeScript SDK workers (needs Node and `npm ci && npm run build` in sdk/ts)")

// tsProc is one TypeScript worker process.
type tsProc struct {
	name   string
	cmd    *exec.Cmd
	exited chan struct{}
	err    error // valid after exited is closed
}

func (p *tsProc) waitExit(timeout time.Duration) bool {
	select {
	case <-p.exited:
		return true
	case <-time.After(timeout):
		return false
	}
}

func startTSWorker(t *testing.T, script, name, addr, dir string) *tsProc {
	t.Helper()
	cmd := exec.Command("node", script)
	cmd.Env = append(os.Environ(), "TASKLANE_ADDR=http://"+addr, "WORKER_NAME="+name, "LEDGER_DIR="+dir, "QUEUE="+queue)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	p := &tsProc{name: name, cmd: cmd, exited: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.exited)
	}()
	// Registered after the servers, so it runs before they stop.
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGCONT)
		_ = cmd.Process.Kill()
		<-p.exited
	})
	return p
}

// TestInvariantsTS checks the TypeScript SDK against the rules of design
// §17.2 (§21.5): a Go producer, three TypeScript worker processes (one killed
// with SIGKILL at 30 %, one frozen past its visibility timeout at 50 %), the
// two Go zombies that prove fencing, and the same checker as TestInvariants.
func TestInvariantsTS(t *testing.T) {
	if !*tsWorkers {
		t.Skip("TypeScript workers: run with -invariant.ts after `cd sdk/ts && npm ci && npm run build`")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "sdk", "ts", "test", "invariant", "worker.mjs"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(filepath.Dir(script), "..", "..", "dist", "index.js"))
	require.NoError(t, err, "build the SDK first: cd sdk/ts && npm ci && npm run build")
	_, err = exec.LookPath("node")
	require.NoError(t, err, "node is not on PATH")

	const jobs = 2000
	seed := *seedArg
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("seed %d (replay the job mix with -invariant.seed=%d)", seed, seed)
	began := time.Now()

	ctx := context.Background()
	require.NoError(t, testDB.Reset(ctx))
	addrs := make([]string, 2)
	for i := range addrs {
		addrs[i], _ = startServer(t, ctx, testDB)
	}
	producers := make([]*tasklane.Client, 2)
	for i, addr := range addrs {
		c, err := tasklane.Dial(addr)
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })
		producers[i] = c
	}
	_, err = producers[0].CreateQueue(ctx, tasklane.QueueConfig{Name: queue, VisibilityTimeout: vt, MaxAttempts: 3, MaxDepth: jobs * 2})
	require.NoError(t, err)

	led := &ledgerMem{}
	dir := t.TempDir()
	procs := make([]*tsProc, 3)
	for i := range procs {
		procs[i] = startTSWorker(t, script, fmt.Sprintf("ts%d", i), addrs[i%2], dir)
	}
	stopZombies := startZombies(t, ctx, addrs, led)

	var frozen []invariants.Freeze
	freezeDone := make(chan struct{})
	produce(t, seed, jobs, producers, led, map[int]func(*rand.Rand){
		jobs * 3 / 10: func(*rand.Rand) {
			require.NoError(t, procs[0].cmd.Process.Kill())
			t.Logf("killed %s (SIGKILL)", procs[0].name)
		},
		jobs / 2: func(*rand.Rand) {
			p := procs[1]
			go func() {
				defer close(freezeDone)
				from := time.Now()
				_ = p.cmd.Process.Signal(syscall.SIGSTOP)
				time.Sleep(vt + time.Second)
				_ = p.cmd.Process.Signal(syscall.SIGCONT)
				frozen = append(frozen, invariants.Freeze{Worker: p.name, From: from, To: time.Now()})
			}()
			t.Logf("froze %s for %v (SIGSTOP)", p.name, vt+time.Second)
		},
	})
	<-freezeDone // also orders the write to frozen before the read below

	stopZombies()
	waitDrained(t, seed, 120*time.Second)

	for _, p := range procs[1:] {
		require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
		require.True(t, p.waitExit(15*time.Second), "%s did not exit after SIGTERM", p.name)
		require.NoError(t, p.err, "%s exited with an error", p.name)
	}
	procs[0].waitExit(5 * time.Second)

	tsRuns, err := ledger.ReadRuns(dir)
	require.NoError(t, err)
	final, err := invariants.LoadJobs(ctx, testDB.Pool, queue)
	require.NoError(t, err)
	led.mu.Lock()
	defer led.mu.Unlock()
	runs := append(append([]ledger.Run{}, led.runs...), tsRuns...)
	cfg := invariants.Config{VisibilityTimeout: vt, Tolerance: 500 * time.Millisecond, Frozen: frozen}
	if vs := invariants.Check(cfg, led.enq, runs, final); len(vs) > 0 {
		t.Fatalf("seed %d\n%s", seed, invariants.Report(vs, runs, 20))
	}

	outcomes := map[string]int{}
	perWorker := map[string]int{}
	for _, r := range runs {
		outcomes[r.Outcome]++
		perWorker[r.Worker]++
	}
	states := map[string]int{}
	for _, j := range final {
		states[j.State]++
	}
	t.Logf("summary: %d runs (%s), per worker %v, %d completed, %d dead, %v",
		len(runs), formatCounts(outcomes), perWorker, states["completed"], states["dead"], time.Since(began).Round(100*time.Millisecond))

	require.Len(t, led.enq, jobs)
	require.Positive(t, outcomes["lease_lost"], "seed %d: nothing lost its lease, so fencing went untested", seed)
	for _, p := range procs {
		require.Positive(t, perWorker[p.name], "%s recorded no runs", p.name)
	}
}
