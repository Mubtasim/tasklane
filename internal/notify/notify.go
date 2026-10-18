// Package notify wakes long-polling leases when jobs are enqueued. One
// Listener per server holds a dedicated Postgres connection that LISTENs on
// pgstore.NotifyChannel and fans each notification (payload: the queue name)
// out to the subscribers of that queue.
package notify

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/pgstore"
)

// ApplicationName identifies the listener's connection in pg_stat_activity.
const ApplicationName = "tasklane-notify"

const (
	// idleCheck bounds each wait for a notification; when it elapses the
	// connection is pinged, so a half-open connection is noticed.
	idleCheck = 30 * time.Second
	// pingTimeout bounds that liveness check.
	pingTimeout = 5 * time.Second
)

type Listener struct {
	connString string
	log        *slog.Logger
	retry      backoff.Policy // delay between reconnect attempts

	listening atomic.Bool
	pid       atomic.Uint32 // backend PID of the LISTEN connection; 0 when down

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func New(connString string, log *slog.Logger) *Listener {
	return &Listener{
		connString: connString,
		log:        log,
		retry:      backoff.Policy{Base: 100 * time.Millisecond, Cap: 5 * time.Second, Rand: rand.Float64},
		subs:       map[string]map[chan struct{}]struct{}{},
	}
}

// Subscribe returns a channel that receives a value after jobs are enqueued
// to queue, and a function that ends the subscription. The channel holds at
// most one pending wake-up: several notifications before the subscriber reads
// collapse into one, which is all a "go and re-check" signal needs.
func (l *Listener) Subscribe(queue string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	l.mu.Lock()
	if l.subs[queue] == nil {
		l.subs[queue] = map[chan struct{}]struct{}{}
	}
	l.subs[queue][ch] = struct{}{}
	l.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.subs[queue], ch)
			if len(l.subs[queue]) == 0 {
				delete(l.subs, queue)
			}
			l.mu.Unlock()
		})
	}
}

// Listening reports whether the LISTEN connection is currently up.
func (l *Listener) Listening() bool { return l.listening.Load() }

// Run listens until ctx is cancelled, reconnecting with backoff whenever the
// connection fails. It always returns nil once ctx is done.
func (l *Listener) Run(ctx context.Context) error {
	attempt := 0
	for {
		err := l.listen(ctx, func() { attempt = 0 })
		l.listening.Store(false)
		l.pid.Store(0)
		if ctx.Err() != nil {
			return nil
		}
		delay := l.retry.Delay(attempt)
		attempt++
		l.log.Warn("notify listener disconnected; reconnecting", "err", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// listen runs one connection's lifetime; it returns when the connection fails
// or ctx is cancelled. connected is called once LISTEN is in effect.
func (l *Listener) listen(ctx context.Context, connected func()) error {
	cfg, err := pgx.ParseConfig(l.connString)
	if err != nil {
		return err
	}
	cfg.RuntimeParams["application_name"] = ApplicationName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+pgstore.NotifyChannel); err != nil {
		return err
	}
	l.pid.Store(conn.PgConn().PID())
	l.listening.Store(true)
	connected()
	// Notifications sent while we were disconnected (or before LISTEN took
	// effect) are lost, so wake everyone once LISTEN is in effect: each
	// waiter re-checks the database once. On the first connect this is a
	// harmless extra wake-up.
	l.wakeAll()
	for {
		waitCtx, cancel := context.WithTimeout(ctx, idleCheck)
		n, err := conn.WaitForNotification(waitCtx)
		timedOut := waitCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if err != nil {
			if !timedOut || !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			// Idle for a while: make sure the connection is still alive.
			pingCtx, cancelPing := context.WithTimeout(ctx, pingTimeout)
			err := conn.Ping(pingCtx)
			cancelPing()
			if err != nil {
				return err
			}
			continue
		}
		l.wake(n.Payload)
	}
}

// wake signals every subscriber of queue without blocking: a subscriber whose
// buffer is full already has a wake-up pending.
func (l *Listener) wake(queue string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs[queue] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (l *Listener) wakeAll() {
	l.mu.Lock()
	queues := make([]string, 0, len(l.subs))
	for q := range l.subs {
		queues = append(queues, q)
	}
	l.mu.Unlock()
	for _, q := range queues {
		l.wake(q)
	}
}
