package api

import (
	"context"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/mubtasim/tasklane/internal/store"
)

const (
	// depthRefreshInterval is the shortest wait between depth recounts, and
	// the retry-after hint a refused enqueue gets. A producer that retries
	// after this long may be refused again until the next refresh, which is
	// fine: it backs off and tries once more.
	depthRefreshInterval = time.Second
	// depthRefreshMax is the longest wait between recounts. It must stay
	// below depthMaxStale, or the cache would go stale between two good
	// refreshes and isFull would fail open on a healthy database.
	depthRefreshMax = 5 * time.Second
	// depthRefreshCostFactor is how many times longer than the last count we
	// wait before the next one. The count is O(backlog), so waiting 10x its
	// duration caps counting at about 1/11, or 9%, of one Postgres backend's
	// time whatever the backlog.
	depthRefreshCostFactor = 10
	// depthMaxStale is how long the last good counts are trusted while
	// refreshes fail.
	depthMaxStale = 10 * time.Second
)

// nextDepthRefresh is how long to wait after a refresh that took the given
// time: depthRefreshCostFactor times as long, clamped to
// [depthRefreshInterval, depthRefreshMax].
func nextDepthRefresh(took time.Duration) time.Duration {
	return min(max(took*depthRefreshCostFactor, depthRefreshInterval), depthRefreshMax)
}

// depthCache answers "is this queue full?" without a COUNT on every Enqueue
// (design §6, §15.3). The limit is soft: between refreshes a queue can
// overshoot by about one second of traffic.
type depthCache struct {
	mu          sync.Mutex
	full        map[string]bool // queues at or above max_depth at the last good refresh
	refreshedAt time.Time       // zero until the first good refresh
	now         func() time.Time
}

func newDepthCache() *depthCache {
	return &depthCache{now: time.Now}
}

// refresh recounts every queue and returns the depths it just stored. On
// error it keeps the previous values and returns nil.
func (c *depthCache) refresh(ctx context.Context, st store.Store) ([]store.QueueDepth, error) {
	depths, err := st.QueueDepths(ctx)
	if err != nil {
		return nil, err
	}
	full := make(map[string]bool)
	for _, d := range depths {
		if d.Depth() >= d.MaxDepth {
			full[d.Queue] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.full = full
	c.refreshedAt = c.now()
	return depths, nil
}

// isFull reports whether the queue was full at the last good refresh. It
// fails open: before the first refresh, or once the counts are older than
// depthMaxStale, nothing is full. Refusing every enqueue on unknown data
// would turn a short database blip into a producer outage, and while the
// database is down Enqueue fails with UNAVAILABLE anyway.
func (c *depthCache) isFull(queue string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshedAt.IsZero() || c.now().Sub(c.refreshedAt) > depthMaxStale {
		return false
	}
	return c.full[queue]
}

// queueFullError is RESOURCE_EXHAUSTED (HTTP 429) with a RetryInfo detail.
// The detail is what tells a full queue apart from the other
// RESOURCE_EXHAUSTED, a message over the size limit.
func queueFullError(queue string) error {
	st := status.Newf(codes.ResourceExhausted, "queue %q is full (max_depth reached); retry later", queue)
	withInfo, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(depthRefreshInterval)})
	if err != nil {
		return st.Err() // only fails if the detail can't be marshalled
	}
	return withInfo.Err()
}

// refreshDepths recounts queue depths once and updates the backpressure
// cache and the queue gauges. It reports whether the refresh succeeded.
func (s *Server) refreshDepths(ctx context.Context) bool {
	depths, err := s.depths.refresh(ctx, s.store)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("refresh queue depths failed", "err", err)
		}
		return false
	}
	s.metrics.SetQueueStats(depths)
	return true
}

// RunDepthRefresh recounts queue depths until ctx is cancelled, the first
// time immediately and then after nextDepthRefresh of the last count's
// duration, so a huge backlog can't make counting eat the database. A failed
// refresh waits only depthRefreshInterval so recovery is quick. Until it has
// run once, Enqueue refuses nothing.
func (s *Server) RunDepthRefresh(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		start := time.Now()
		wait := depthRefreshInterval
		if s.refreshDepths(ctx) {
			wait = nextDepthRefresh(time.Since(start))
		}
		timer.Reset(wait)
	}
}
