// Command producer enqueues a steady stream of demo jobs: mostly normal
// work, some flaky jobs that fail now and then, and a few poison jobs that
// always fail and end in the dead-letter queue.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/mubtasim/tasklane/examples/demo/internal/demo"
	"github.com/mubtasim/tasklane/internal/ledger"
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

// retryDelay is how long ledger mode waits before retrying a failed enqueue.
const retryDelay = 200 * time.Millisecond

// enqueueTimeout caps a single Enqueue attempt.
const enqueueTimeout = 5 * time.Second

// sleep waits d and reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
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
	if err := demo.EnsureQueue(ctx, client, cfg, log); err != nil {
		if ctx.Err() == nil {
			log.Error("create queue", "err", err)
			os.Exit(1)
		}
		return
	}
	log.Info("producing", "queue", cfg.Queue, "jobs_per_second", rate)

	var led *ledger.Writer
	runID := ""
	if cfg.LedgerDir != "" {
		if led, err = ledger.Open(filepath.Join(cfg.LedgerDir, "producer.enqueued.jsonl")); err != nil {
			log.Error("open ledger", "err", err)
			os.Exit(1)
		}
		defer led.Close()
		// A restarted producer gets a new run ID, so it never reuses a key.
		var b [4]byte
		_, _ = crand.Read(b[:])
		runID = hex.EncodeToString(b[:])
	}

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
		job := tasklane.Job{Kind: kind, Payload: map[string]int{"seq": seq}}
		if led != nil {
			job.IdempotencyKey = fmt.Sprintf("%s-%d", runID, seq)
		}
		for {
			// Bound each attempt so a paused server cannot stall the producer;
			// the retry reuses the same key, so a timed-out call that did
			// land is not enqueued twice.
			actx, cancel := context.WithTimeout(ctx, enqueueTimeout)
			res, err := client.Enqueue(actx, cfg.Queue, job)
			cancel()
			if err == nil {
				if led != nil {
					if err := led.Append(ledger.Enqueued{Key: job.IdempotencyKey, JobID: res.JobID}); err != nil {
						log.Error("ledger", "err", err)
					}
				}
				break
			}
			var full *tasklane.QueueFullError
			wait := retryDelay
			if errors.As(err, &full) {
				// Backpressure: the workers are behind. Wait as the server
				// asks instead of hammering it.
				log.Warn("queue full, retry after", "retry_after", full.RetryAfter)
				wait = full.RetryAfter
			} else {
				log.Warn("enqueue failed", "kind", kind, "err", err)
			}
			if led == nil {
				// Without a ledger a failed job is skipped; only a full queue waits.
				if full != nil {
					sleep(ctx, wait)
				}
				break
			}
			// With a ledger the job must not be lost: retry it with the same
			// key, so a request that actually landed is not enqueued twice.
			if !sleep(ctx, wait) {
				break
			}
		}
		if seq%100 == 0 {
			log.Info("enqueued", "total", seq)
		}
	}
}
