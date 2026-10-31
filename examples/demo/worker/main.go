// Command worker runs the demo handlers: normal, flaky and poison jobs.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/mubtasim/tasklane/examples/demo/internal/demo"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func main() {
	host, _ := os.Hostname()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("worker", demo.Env("WORKER_NAME", host))
	cfg := demo.LoadConfig()
	concurrency, err := strconv.Atoi(demo.Env("DEMO_CONCURRENCY", "5"))
	if err != nil || concurrency <= 0 {
		log.Error("DEMO_CONCURRENCY must be a positive integer", "value", os.Getenv("DEMO_CONCURRENCY"))
		os.Exit(1)
	}
	client, err := demo.Dial(cfg)
	if err != nil {
		log.Error("dial", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Create the queue too, so the worker doesn't depend on the producer
	// starting first.
	if err := demo.EnsureQueue(ctx, client, cfg, log); err != nil {
		if ctx.Err() == nil {
			log.Error("create queue", "err", err)
			os.Exit(1)
		}
		return
	}

	w := client.NewWorker(cfg.Queue, tasklane.WorkerOptions{Concurrency: concurrency, Logger: log})
	w.Handle("normal", normal(log))
	w.Handle("flaky", flaky(log, 0.3, rand.Float64))
	w.Handle("poison", poison(log))
	log.Info("worker started", "queue", cfg.Queue, "concurrency", concurrency)
	if err := w.Run(ctx); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped cleanly")
}
