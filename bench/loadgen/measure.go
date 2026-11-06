package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mubtasim/tasklane/bench/internal/probe"
)

// snapshot is everything read at one edge of the window. A failed read is
// kept as a problem instead of aborting: the run still finishes and is
// marked invalid.
type snapshot struct {
	at                     time.Time
	serverCPU, pgCPU, self time.Duration
	leaseRPCs              float64
	pg                     probe.PGSnapshot
	problems               []string
}

func take(ctx context.Context, cfg Config, pool *pgxpool.Pool) snapshot {
	s := snapshot{at: time.Now(), self: selfCPU()}
	if !cfg.Probes {
		return s
	}
	var err error
	if s.serverCPU, err = probe.CgroupCPU(cfg.CgroupServer); err != nil {
		s.problems = append(s.problems, "cgroup-server: "+err.Error())
	}
	if s.pgCPU, err = probe.CgroupCPU(cfg.CgroupPG); err != nil {
		s.problems = append(s.problems, "cgroup-pg: "+err.Error())
	}
	if s.leaseRPCs, err = probe.ScrapeSum(ctx, cfg.MetricsURL, "tasklane_rpc_duration_seconds_count", map[string]string{"method": "Lease"}); err != nil {
		s.problems = append(s.problems, "metrics: "+err.Error())
	}
	if s.pg, err = probe.TakePG(ctx, pool); err != nil {
		s.problems = append(s.problems, err.Error())
	}
	return s
}

// selfCPU is this process's user + system CPU time.
func selfCPU() time.Duration {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func perJob(cpu time.Duration, jobs int64) float64 {
	if jobs == 0 {
		return 0
	}
	return float64(cpu.Microseconds()) / 1000 / float64(jobs)
}

// verdict decides whether a run is a data point. More than 1% failed sends
// or any dropped send means the load generator, not the server, shaped the
// result; a failed probe means a column is missing.
func verdict(c counts, problems []string) (bool, string) {
	var reasons []string
	if c.sends > 0 && c.errors*100 > c.sends {
		reasons = append(reasons, fmt.Sprintf("%.1f%% failed sends", float64(c.errors)*100/float64(c.sends)))
	}
	if c.dropped > 0 {
		reasons = append(reasons, fmt.Sprintf("%d sends dropped (load generator saturated)", c.dropped))
	}
	if c.notStarted > 0 {
		reasons = append(reasons, fmt.Sprintf("%d in-window jobs never started", c.notStarted))
	}
	reasons = append(reasons, problems...)
	return len(reasons) == 0, strings.Join(reasons, "; ")
}

// detail is the JSON written next to the CSV for one run.
type detail struct {
	Label      string            `json:"label"`
	Statements []probe.Statement `json:"top_statements"`
	Waits      map[string]int    `json:"wait_samples"`
	Before     probe.PGSnapshot  `json:"pg_before"`
	After      probe.PGSnapshot  `json:"pg_after"`
}

func writeDetail(dir string, d detail) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, d.Label+".json"), b, 0o644)
}
