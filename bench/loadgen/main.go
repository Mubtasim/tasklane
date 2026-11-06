// Command loadgen drives a Tasklane server for the M5a load test (design.md
// §19). bench/run.sh runs it once per scenario, pinned to its own CPU with
// taskset; each run appends one row to a CSV.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/bench/internal/probe"
	"github.com/mubtasim/tasklane/bench/internal/stats"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		slog.Error("loadgen", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg Config) error {
	client, err := tasklane.Dial(cfg.Addr, tasklane.WithAPIKey(cfg.APIKey))
	if err != nil {
		return err
	}
	defer client.Close()
	if err := ensureQueues(ctx, cfg, client); err != nil {
		return err
	}
	switch cfg.Mode {
	case "fill":
		rate, err := fill(ctx, cfg, client)
		if err != nil {
			return err
		}
		fmt.Printf("throughput=%.1f\n", rate)
		return nil
	case "redrive":
		start := time.Now()
		n, err := client.RedriveAll(ctx, cfg.Queue)
		if err != nil {
			return err
		}
		took := time.Since(start)
		row := stats.Row{Scenario: cfg.Scenario, Label: cfg.Label, Mode: "redrive", Window: took,
			Enqueued: n, Throughput: float64(n) / took.Seconds(), Valid: true}
		fmt.Printf("redrove %d jobs in %s\nthroughput=%.1f\n", n, took, row.Throughput)
		return stats.AppendRow(cfg.Out, row)
	}
	return measured(ctx, cfg, client)
}

// ensureQueues creates the queues a run uses; an existing queue is fine.
// max_depth is far above any backlog here, so backpressure never triggers.
func ensureQueues(ctx context.Context, cfg Config, client *tasklane.Client) error {
	for i := range cfg.Queues {
		_, err := client.CreateQueue(ctx, tasklane.QueueConfig{
			Name: queueName(cfg, int64(i)), MaxDepth: 1 << 40, VisibilityTimeout: 30 * time.Second, MaxAttempts: 3,
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create queue: %w", err)
		}
	}
	return nil
}

// measured runs a closed or open load: warm up, snapshot, measure, snapshot,
// then stop producers first and workers after, so jobs enqueued at the end
// of the window still get picked up and counted.
func measured(ctx context.Context, cfg Config, client *tasklane.Client) error {
	pool, err := pgxpool.New(ctx, cfg.PGURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	l := newLoad(cfg, client)
	workCtx, stopWork := context.WithCancel(ctx)
	waitWorkers := l.startWorkers(workCtx)
	prodCtx, stopProd := context.WithCancel(ctx)
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		switch {
		case cfg.Mode == "open":
			l.runOpen(prodCtx)
		case cfg.Producers > 0:
			l.runClosed(prodCtx)
		default:
			<-prodCtx.Done() // workers only: an idle run
		}
	}()

	sleepCtx(ctx, cfg.Warmup)
	var problems []string
	if err := probe.ResetStatements(ctx, pool); err != nil {
		problems = append(problems, err.Error())
	}
	before := take(ctx, cfg, pool)
	l.win.Store(&window{start: before.at, end: before.at.Add(cfg.Duration)})
	waitCtx, stopWaits := context.WithCancel(ctx)
	waitsc := make(chan map[string]int, 1)
	go func() { waitsc <- probe.SampleWaits(waitCtx, pool, 100*time.Millisecond) }()

	sleepCtx(ctx, cfg.Duration)
	after := take(ctx, cfg, pool)
	stopWaits()
	waits := <-waitsc
	top, calls, err := probe.TopStatements(ctx, pool, 10)
	if err != nil {
		problems = append(problems, err.Error())
	}

	stopProd()
	<-prodDone
	// Jobs enqueued just before the window ended are acked within a
	// moment; give them a second before stopping the workers.
	sleepCtx(ctx, time.Second)
	stopWork()
	waitWorkers()
	if ctx.Err() != nil {
		return errors.New("interrupted")
	}

	c := l.counts()
	win := after.at.Sub(before.at)
	jobs := c.acked
	if cfg.Workers == 0 {
		jobs = c.enqueued
	}
	problems = append(problems, before.problems...)
	problems = append(problems, after.problems...)
	valid, reason := verdict(c, problems)
	row := stats.Row{
		Scenario: cfg.Scenario, Label: cfg.Label, Mode: cfg.Mode, Batch: cfg.Batch, Workers: cfg.Workers,
		Payload: cfg.Payload, TargetRate: cfg.Rate, Window: win,
		Enqueued: c.enqueued, Acked: c.acked, Throughput: float64(jobs) / win.Seconds(),
		E2S: l.e2s.Summary(), Enqueue: l.enq.Summary(),
		ServerCPUPerJob:  perJob(after.serverCPU-before.serverCPU, jobs),
		PGCPUPerJob:      perJob(after.pgCPU-before.pgCPU, jobs),
		LoadgenCPUPerJob: perJob(after.self-before.self, jobs),
		ServerCPUPct:     100 * (after.serverCPU - before.serverCPU).Seconds() / win.Seconds(),
		PGCPUPct:         100 * (after.pgCPU - before.pgCPU).Seconds() / win.Seconds(),
		PGQueriesPerSec:  float64(calls) / win.Seconds(),
		Errors:           c.errors, Dropped: c.dropped, Valid: valid, Reason: reason,
	}
	if jobs > 0 {
		row.WALBytesPerJob = float64(after.pg.WALBytes-before.pg.WALBytes) / float64(jobs)
		row.LeaseRPCsPerJob = (after.leaseRPCs - before.leaseRPCs) / float64(jobs)
	}
	if err := writeDetail(cfg.DetailDir, detail{Label: cfg.Label, Statements: top, Waits: waits, Before: before.pg, After: after.pg}); err != nil {
		return err
	}
	if err := stats.AppendRow(cfg.Out, row); err != nil {
		return err
	}
	fmt.Printf("%s: %.1f jobs/s, e2s p99 %s, valid=%t %s\nthroughput=%.1f\n",
		cfg.Label, row.Throughput, row.E2S.P99, valid, reason, row.Throughput)
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
