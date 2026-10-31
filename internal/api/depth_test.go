package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/internal/store"
)

// depthStore implements only QueueDepths.
type depthStore struct {
	store.Store
	depths []store.QueueDepth
	err    error
}

func (d *depthStore) QueueDepths(context.Context) ([]store.QueueDepth, error) {
	return d.depths, d.err
}

func newTestCache() (*depthCache, *time.Time) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	c := newDepthCache()
	c.now = func() time.Time { return now }
	return c, &now
}

func TestDepthCacheFullAtMaxDepth(t *testing.T) {
	c, _ := newTestCache()
	st := &depthStore{depths: []store.QueueDepth{
		{Queue: "full", Available: 30, Leased: 20, MaxDepth: 50},
		{Queue: "over", Available: 40, Leased: 11, MaxDepth: 50},
		{Queue: "room", Available: 40, Leased: 9, MaxDepth: 50},
	}}
	mustRefresh(t, c, context.Background(), st)
	require.True(t, c.isFull("full"), "at max_depth is full")
	require.True(t, c.isFull("over"))
	require.False(t, c.isFull("room"))
	require.False(t, c.isFull("unknown"), "a queue missing from the cache has depth 0")
}

func TestDepthCacheFailsOpenBeforeTheFirstRefresh(t *testing.T) {
	c, _ := newTestCache()
	require.False(t, c.isFull("anything"))
}

func TestDepthCacheKeepsValuesThroughShortOutages(t *testing.T) {
	c, now := newTestCache()
	st := &depthStore{depths: []store.QueueDepth{{Queue: "q", Available: 3, Leased: 2, MaxDepth: 5}}}
	mustRefresh(t, c, context.Background(), st)

	st.err = errors.New("db down")
	*now = now.Add(9 * time.Second)
	_, err := c.refresh(context.Background(), st)
	require.Error(t, err)
	require.True(t, c.isFull("q"), "the last good values are used for up to 10 s")

	*now = now.Add(2 * time.Second) // 11 s since the last good refresh
	require.False(t, c.isFull("q"), "stale values are not trusted: fail open")
}

func TestQueueFullErrorCarriesRetryInfo(t *testing.T) {
	err := queueFullError("emails")
	st := status.Convert(err)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	require.Contains(t, st.Message(), `"emails"`)
	require.Len(t, st.Details(), 1)
	ri, ok := st.Details()[0].(*errdetails.RetryInfo)
	require.True(t, ok)
	require.Equal(t, time.Second, ri.GetRetryDelay().AsDuration())
}

// mustRefresh refreshes the cache and fails the test on error.
func mustRefresh(t *testing.T, c *depthCache, ctx context.Context, st store.Store) {
	t.Helper()
	_, err := c.refresh(ctx, st)
	require.NoError(t, err)
}
