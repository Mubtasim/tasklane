// Package ledger records what workers and producers did, one JSON object per
// line, so the invariant and chaos tests can check it against the database.
// Each record is written with a single write call on an O_APPEND file: a
// process killed with SIGKILL loses at most the record it was writing, never
// one already written (the data is in the kernel's page cache).
package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

type Run struct {
	Worker  string    `json:"worker"`
	JobID   int64     `json:"job_id"`
	Attempt int       `json:"attempt"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Outcome string    `json:"outcome"`
	Err     string    `json:"err,omitempty"`
}

type Enqueued struct {
	Key   string `json:"key"`
	JobID int64  `json:"job_id"`
}

func RunFromResult(worker string, r tasklane.Result) Run {
	run := Run{Worker: worker, JobID: r.Job.ID, Attempt: r.Job.Attempt, Start: r.Start, End: r.End, Outcome: r.Outcome.String()}
	if r.Err != nil {
		run.Err = r.Err.Error()
	}
	return run
}

type Writer struct {
	mu sync.Mutex
	f  *os.File
}

func Open(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

// Append writes v as one line with one write call.
func (w *Writer) Append(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = w.f.Write(b)
	return err
}

func (w *Writer) Close() error { return w.f.Close() }

func ReadRuns(dir string) ([]Run, error) { return readAll[Run](dir, "*.runs.jsonl") }

func ReadEnqueued(dir string) ([]Enqueued, error) { return readAll[Enqueued](dir, "*.enqueued.jsonl") }

func readAll[T any](dir, pattern string) ([]T, error) {
	paths, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var out []T
	for _, p := range paths {
		recs, err := readFile[T](p)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// readFile tolerates an incomplete last line (the writer was killed while
// writing it) but not a bad line in the middle.
func readFile[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	var pending error
	line := 0
	for sc.Scan() {
		line++
		if pending != nil {
			return nil, pending
		}
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			pending = fmt.Errorf("%s:%d: %w", path, line, err)
			continue
		}
		out = append(out, v)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil // a bad last line is dropped
}
