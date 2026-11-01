// Package ledger records what workers and producers did, one JSON object per
// line, so the invariant and chaos tests can check it against the database.
// Each record is written with a single write call on an O_APPEND file: a
// process killed with SIGKILL loses at most the record it was writing, never
// one already written (the data is in the kernel's page cache).
package ledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
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

// Open appends to path, creating it if needed. If a killed process left a
// torn last line (no trailing newline), Open ends that line first, so the
// next record starts on its own line instead of being glued onto the fragment.
func Open(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, fi.Size()-1); err == nil && last[0] != '\n' {
			if _, err := f.Write([]byte{'\n'}); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
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

// readFile skips empty lines and tolerates torn lines: an incomplete last line
// (the writer was killed while writing it), and a cut-off line in the middle
// (the writer restarted and Open ended the fragment with a newline). Any other
// bad line in the middle is an error.
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
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		if pending != nil {
			return nil, pending
		}
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			if isCutOff(err) {
				continue
			}
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

// isCutOff reports whether err means the JSON text ended early, which is what
// a record interrupted by SIGKILL looks like.
func isCutOff(err error) bool {
	var se *json.SyntaxError
	return errors.As(err, &se) && se.Error() == "unexpected end of JSON input"
}
