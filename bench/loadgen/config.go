package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/mubtasim/tasklane/internal/api"
)

// Config is the load generator's command line.
type Config struct {
	Mode, Addr, APIKey, Queue string
	Queues                    int
	MetricsURL, PGURL         string
	CgroupServer, CgroupPG    string
	Scenario, Label           string
	Out, DetailDir            string
	Producers, Batch, Workers int
	Rate                      float64
	Payload                   int
	Warmup, Duration          time.Duration
	Jobs                      int
	Priority                  int
	Delay                     time.Duration
}

func parseConfig(args []string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.StringVar(&c.Mode, "mode", "", "fill, closed, open or redrive")
	fs.StringVar(&c.Addr, "addr", "127.0.0.1:7070", "Tasklane gRPC address")
	fs.StringVar(&c.APIKey, "api-key", "bench-key", "API key")
	fs.StringVar(&c.Queue, "queue", "bench", "queue name (prefix when -queues > 1)")
	fs.IntVar(&c.Queues, "queues", 1, "fill: spread jobs over this many queues")
	fs.StringVar(&c.MetricsURL, "metrics", "http://127.0.0.1:9464/metrics", "server metrics URL")
	fs.StringVar(&c.PGURL, "pg", "postgres://tasklane:tasklane@127.0.0.1:5433/tasklane?sslmode=disable", "Postgres URL for probes")
	fs.StringVar(&c.CgroupServer, "cgroup-server", "", "server container cgroup directory")
	fs.StringVar(&c.CgroupPG, "cgroup-pg", "", "Postgres container cgroup directory")
	fs.StringVar(&c.Scenario, "scenario", "", "scenario id for the CSV")
	fs.StringVar(&c.Label, "label", "", "unique run label for the CSV")
	fs.StringVar(&c.Out, "out", "bench/results/results.csv", "CSV file to append to")
	fs.StringVar(&c.DetailDir, "detail-dir", "", "directory for <label>.json details")
	fs.IntVar(&c.Producers, "producers", 16, "closed: producer goroutines (0 = workers only)")
	fs.IntVar(&c.Batch, "batch", 1, "jobs per enqueue call")
	fs.IntVar(&c.Workers, "workers", 0, "worker slots (0 = enqueue only)")
	fs.Float64Var(&c.Rate, "rate", 0, "open: jobs per second")
	fs.IntVar(&c.Payload, "payload", 100, "payload size in bytes")
	fs.DurationVar(&c.Warmup, "warmup", 10*time.Second, "warm-up, not measured")
	fs.DurationVar(&c.Duration, "duration", 60*time.Second, "measured window")
	fs.IntVar(&c.Jobs, "jobs", 0, "fill: number of jobs")
	fs.IntVar(&c.Priority, "priority", 3, "fill: priority 1 (critical) to 4 (low)")
	fs.DurationVar(&c.Delay, "delay", 0, "fill: run_at = now + delay")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	return c, c.validate()
}

func (c Config) validate() error {
	switch c.Mode {
	case "fill":
		if c.Jobs <= 0 {
			return errors.New("fill needs -jobs > 0")
		}
	case "closed", "open", "redrive":
		if c.Label == "" {
			return fmt.Errorf("%s needs -label", c.Mode)
		}
	case "":
		return errors.New("-mode is required")
	default:
		return fmt.Errorf("unknown -mode %q", c.Mode)
	}
	if c.Mode == "open" && c.Rate <= 0 {
		return errors.New("open needs -rate > 0")
	}
	if c.Mode == "closed" && c.Producers == 0 && c.Workers == 0 {
		return errors.New("closed needs producers or workers")
	}
	// The server refuses batches by message size, not count; 1000 keeps a
	// batch of 10 KB payloads well under its 16 MiB limit.
	if c.Batch < 1 || c.Batch > 1000 {
		return errors.New("-batch must be 1-1000")
	}
	if c.Priority < 1 || c.Priority > 4 {
		return errors.New("-priority must be 1-4")
	}
	if c.Queues < 1 || c.Producers < 0 || c.Workers < 0 || c.Payload < 0 {
		return errors.New("-queues must be ≥ 1; counts and sizes must not be negative")
	}
	return nil
}

// workerSplit divides slots into SDK workers of at most api.MaxLeaseJobs
// each: one Lease asks for at most that many jobs, and each SDK worker runs
// one long-poll, so 200 slots behave like 4 worker processes.
func workerSplit(slots int) []int {
	out := []int{}
	for slots > 0 {
		n := min(slots, api.MaxLeaseJobs)
		out = append(out, n)
		slots -= n
	}
	return out
}
