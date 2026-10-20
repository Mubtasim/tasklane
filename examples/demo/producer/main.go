// Command producer enqueues a steady stream of demo jobs: mostly normal
// work, some flaky jobs that fail now and then, and a few poison jobs that
// always fail and end in the dead-letter queue.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mubtasim/tasklane/examples/demo/internal/demo"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

type weighted struct {
	kind   string
	weight int
}

// kinds is the job mix, in percent.
var kinds = []weighted{{"normal", 85}, {"flaky", 12}, {"poison", 3}}

func totalWeight(ws []weighted) int {
	n := 0
	for _, w := range ws {
		n += w.weight
	}
	return n
}

// pick returns the kind whose share of [0, totalWeight) contains roll.
func pick(ws []weighted, roll int) string {
	for _, w := range ws {
		if roll < w.weight {
			return w.kind
		}
		roll -= w.weight
	}
	return ws[len(ws)-1].kind
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("app", "producer")
	cfg := demo.LoadConfig()
	rate, err := strconv.ParseFloat(demo.Env("DEMO_RATE", "10"), 64)
	if err != nil || rate <= 0 {
		log.Error("DEMO_RATE must be a positive number of jobs per second", "value", os.Getenv("DEMO_RATE"))
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
	if err := demo.EnsureQueue(ctx, client, cfg.Queue, log); err != nil {
		if ctx.Err() == nil {
			log.Error("create queue", "err", err)
			os.Exit(1)
		}
		return
	}
	log.Info("producing", "queue", cfg.Queue, "jobs_per_second", rate)

	tick := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer tick.Stop()
	total := totalWeight(kinds)
	for seq := 1; ; seq++ {
		select {
		case <-ctx.Done():
			log.Info("stopped", "enqueued", seq-1)
			return
		case <-tick.C:
		}
		kind := pick(kinds, rand.IntN(total))
		if _, err := client.Enqueue(ctx, cfg.Queue, tasklane.Job{Kind: kind, Payload: map[string]int{"seq": seq}}); err != nil {
			log.Warn("enqueue failed", "kind", kind, "err", err)
			continue
		}
		if seq%100 == 0 {
			log.Info("enqueued", "total", seq)
		}
	}
}
