package ledger

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppendAndReadRuns(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "worker-1.runs.jsonl"))
	require.NoError(t, err)
	start := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	require.NoError(t, w.Append(Run{Worker: "worker-1", JobID: 7, Attempt: 2, Start: start, End: start.Add(time.Second), Outcome: "acked"}))
	require.NoError(t, w.Close())

	runs, err := ReadRuns(dir)
	require.NoError(t, err)
	require.Equal(t, []Run{{Worker: "worker-1", JobID: 7, Attempt: 2, Start: start, End: start.Add(time.Second), Outcome: "acked"}}, runs)
}

func TestReadSkipsATruncatedLastLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.runs.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(`{"worker":"w","job_id":1,"attempt":1,"outcome":"acked"}`+"\n"+`{"worker":"w","jo`), 0o644))
	runs, err := ReadRuns(dir)
	require.NoError(t, err)
	require.Len(t, runs, 1)
}

func TestReadRejectsACorruptMiddleLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.runs.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{bad\n"+`{"worker":"w","job_id":1,"attempt":1,"outcome":"acked"}`+"\n"), 0o644))
	_, err := ReadRuns(dir)
	require.Error(t, err)
}

func TestReadEnqueued(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "producer.enqueued.jsonl"))
	require.NoError(t, err)
	require.NoError(t, w.Append(Enqueued{Key: "k1", JobID: 1}))
	require.NoError(t, w.Close())
	got, err := ReadEnqueued(dir)
	require.NoError(t, err)
	require.Equal(t, []Enqueued{{Key: "k1", JobID: 1}}, got)
}
