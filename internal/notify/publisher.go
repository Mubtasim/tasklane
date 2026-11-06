package notify

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mubtasim/tasklane/internal/pgstore"
)

// DefaultCoalesceWindow is how long a Publisher gathers publishes for one
// queue before it sends one NOTIFY for all of them.
const DefaultCoalesceWindow = 5 * time.Millisecond

// sendTimeout bounds one NOTIFY. A lost notification costs at most one
// periodic re-check, so a stuck send is abandoned rather than waited for.
const sendTimeout = 2 * time.Second

// Execer runs one statement; *pgxpool.Pool satisfies it.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Publisher tells waiting leases that a queue has new runnable jobs, after
// the transaction that made them runnable has committed.
//
// Postgres serializes every transaction that calls NOTIFY behind one
// database-wide lock, held until its commit is flushed to the WAL, so a
// NOTIFY inside each enqueue's transaction made enqueue commits wait in line
// (M5a: Lock:object was 30-55% of backend samples). Sending after commit,
// once per queue per window, takes NOTIFY out of the enqueue's commit.
//
// It stays correct because:
//   - A rolled-back enqueue never reaches Publish.
//   - A crash between commit and Publish, or a failed NOTIFY, leaves waiting
//     leases to find the job at their next periodic re-check (1 s), as for
//     any lost notification.
//   - This server's own waiting leases are woken in memory at once, without
//     a round trip.
//   - Other servers hear the NOTIFY at most one window later.
type Publisher struct {
	l      *Listener
	db     Execer
	window time.Duration
	log    *slog.Logger

	mu      sync.Mutex
	pending map[string]*time.Timer // queues with a send scheduled but not yet started
	closed  bool
	// sends counts scheduled and in-flight sends. It is incremented when a
	// send is scheduled, under mu and only while open, so Close's Wait never
	// races an Add from zero.
	sends sync.WaitGroup
}

// NewPublisher wakes l's subscribers directly and sends NOTIFY through db,
// at most once per queue per window.
func NewPublisher(l *Listener, db Execer, window time.Duration, log *slog.Logger) *Publisher {
	return &Publisher{l: l, db: db, window: window, log: log, pending: map[string]*time.Timer{}}
}

// Subscribe delegates to the Listener, so the API server needs only the
// Publisher.
func (p *Publisher) Subscribe(queue string) (<-chan struct{}, func()) {
	return p.l.Subscribe(queue)
}

// Publish wakes this server's waiting leases for queue at once and schedules
// a NOTIFY for the other servers, unless one is already scheduled. It never
// blocks. After Close it only wakes local subscribers.
func (p *Publisher) Publish(queue string) {
	p.l.wake(queue)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.pending[queue] != nil {
		return
	}
	p.sends.Add(1)
	p.pending[queue] = time.AfterFunc(p.window, func() { p.send(queue) })
}

func (p *Publisher) send(queue string) {
	defer p.sends.Done()
	// Clear the entry before the NOTIFY, not after: a Publish that arrives
	// while it is in flight may be for a job committed after the NOTIFY was
	// sent, and a listener that has already re-checked would miss it.
	p.mu.Lock()
	delete(p.pending, queue)
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if _, err := p.db.Exec(ctx, "SELECT pg_notify($1, $2)", pgstore.NotifyChannel, queue); err != nil {
		p.log.Warn("notify: send failed; other servers' leases will find the jobs at their next re-check", "queue", queue, "err", err)
	}
}

// Close stops scheduling sends, sends the scheduled ones now instead of at
// the end of their window, and waits for every send in flight, so it must
// run before the pool closes. Calling it again is harmless.
func (p *Publisher) Close() {
	p.mu.Lock()
	p.closed = true
	var due []string
	for queue, t := range p.pending {
		// Stop reports false when the timer has already fired: its send is
		// running or waiting for mu, and the WaitGroup covers it.
		if t.Stop() {
			due = append(due, queue)
		}
	}
	p.mu.Unlock()
	for _, queue := range due {
		p.send(queue)
	}
	p.sends.Wait()
}
