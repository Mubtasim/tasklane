package notify

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/pgstore"
)

// fakeExec records each Exec. When block is set, every Exec waits for a
// value on it, so a test can hold a send in flight; when fail is set, the
// next Exec returns that error.
type fakeExec struct {
	mu      sync.Mutex
	calls   []execCall
	started chan struct{} // receives a value as each Exec starts, if set
	block   chan struct{}
	fail    error
}

type execCall struct {
	sql  string
	args []any
}

func (f *fakeExec) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	f.calls = append(f.calls, execCall{sql: sql, args: args})
	err := f.fail
	f.fail = nil
	started, block := f.started, f.block
	f.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return pgconn.CommandTag{}, ctx.Err()
		}
	}
	return pgconn.CommandTag{}, err
}

func (f *fakeExec) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeExec) queues() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.args[1].(string))
	}
	return out
}

// waitStarted waits for a send to reach Exec, failing rather than hanging
// when none does.
func waitStarted(t *testing.T, db *fakeExec) {
	t.Helper()
	select {
	case <-db.started:
	case <-time.After(time.Second):
		t.Fatal("no send started")
	}
}

// newTestPublisher builds a Publisher over a Listener that never connects:
// these tests need its subscriptions, not its connection.
func newTestPublisher(t *testing.T, db Execer, window time.Duration, log *slog.Logger) *Publisher {
	t.Helper()
	l, err := New(testDB.DSN, quietLogger())
	require.NoError(t, err)
	p := NewPublisher(l, db, window, log)
	t.Cleanup(p.Close)
	return p
}

func TestPublishWakesLocalSubscribersAtOnce(t *testing.T) {
	db := &fakeExec{}
	// A long window: the wake-up must not wait for the send.
	p := newTestPublisher(t, db, time.Hour, quietLogger())
	ch, unsubscribe := p.Subscribe("q")
	defer unsubscribe()

	p.Publish("q")
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("local subscriber not woken within 10 ms")
	}
	require.Zero(t, db.count(), "woken before any NOTIFY was sent")
}

func TestPublishCoalescesWithinAWindow(t *testing.T) {
	const window = 50 * time.Millisecond
	db := &fakeExec{}
	p := newTestPublisher(t, db, window, quietLogger())

	for range 100 {
		p.Publish("q")
	}
	require.Eventually(t, func() bool { return db.count() == 1 }, time.Second, time.Millisecond)
	time.Sleep(2 * window)
	require.Equal(t, 1, db.count(), "100 publishes in one window send one NOTIFY")

	db.mu.Lock()
	call := db.calls[0]
	db.mu.Unlock()
	require.Equal(t, "SELECT pg_notify($1, $2)", call.sql)
	require.Equal(t, []any{pgstore.NotifyChannel, "q"}, call.args)
}

// A Publish that arrives while a send is in flight may come after that
// NOTIFY's snapshot of the world (its job committed later), so it needs a
// send of its own.
func TestPublishDuringASendSchedulesAnother(t *testing.T) {
	db := &fakeExec{started: make(chan struct{}, 4), block: make(chan struct{})}
	p := newTestPublisher(t, db, time.Millisecond, quietLogger())

	p.Publish("q")
	waitStarted(t, db)
	p.Publish("q") // the first send is still blocked in Exec
	close(db.block)
	require.Eventually(t, func() bool { return db.count() == 2 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, db.count())
}

func TestPublishCoalescesEachQueueSeparately(t *testing.T) {
	db := &fakeExec{}
	p := newTestPublisher(t, db, 20*time.Millisecond, quietLogger())

	p.Publish("a")
	p.Publish("b")
	require.Eventually(t, func() bool { return db.count() == 2 }, time.Second, time.Millisecond)
	require.ElementsMatch(t, []string{"a", "b"}, db.queues())
}

// syncBuffer lets the publisher's goroutines log while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestPublishLogsAFailedSendAndKeepsGoing(t *testing.T) {
	db := &fakeExec{fail: errors.New("connection refused")}
	var logs syncBuffer
	p := newTestPublisher(t, db, time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil)))

	p.Publish("q")
	require.Eventually(t, func() bool { return db.count() == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return bytes.Contains([]byte(logs.String()), []byte("connection refused")) }, time.Second, time.Millisecond)
	require.Contains(t, logs.String(), "level=WARN")

	p.Publish("q")
	require.Eventually(t, func() bool { return db.count() == 2 }, time.Second, time.Millisecond)
}

func TestCloseWaitsForAnInFlightSend(t *testing.T) {
	db := &fakeExec{started: make(chan struct{}, 4), block: make(chan struct{})}
	p := newTestPublisher(t, db, time.Millisecond, quietLogger())

	p.Publish("q")
	waitStarted(t, db)
	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while a send was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(db.block)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the send finished")
	}

	// After Close, Publish still wakes this server's own leases but sends
	// nothing: the pool may already be closed.
	ch, unsubscribe := p.Subscribe("q")
	defer unsubscribe()
	p.Publish("q")
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("local subscriber not woken after Close")
	}
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, db.count(), "no send after Close")
	p.Close() // a second Close is harmless
}
