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
	// depthRefreshInterval is how often each server recounts queue depths,
	// and the retry-after hint a refused enqueue gets: the queue can't look
	// any emptier before the next refresh.
	depthRefreshInterval = time.Second
	// depthMaxStale is how long the last good counts are trusted while
	// refreshes fail.
	depthMaxStale = 10 * time.Second
)

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

// refresh recounts every queue. On error it keeps the previous values.
func (c *depthCache) refresh(ctx context.Context, st store.Store) error {
	depths, err := st.QueueDepths(ctx)
	if err != nil {
		return err
	}
	full := make(map[string]bool)
	for _, d := range depths {
		if d.Depth >= d.MaxDepth {
			full[d.Queue] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.full = full
	c.refreshedAt = c.now()
	return nil
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

// RunDepthRefresh recounts queue depths every second until ctx is
// cancelled. Until it has run once, Enqueue refuses nothing.
func (s *Server) RunDepthRefresh(ctx context.Context) {
	ticker := time.NewTicker(depthRefreshInterval)
	defer ticker.Stop()
	for {
		if err := s.depths.refresh(ctx, s.store); err != nil && ctx.Err() == nil {
			s.log.Warn("refresh queue depths failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
