package api

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/store"
)

// fakeNotifier records subscriptions and delivers wake-ups like
// notify.Listener: a buffered channel of 1 per subscriber.
type fakeNotifier struct {
	mu           sync.Mutex
	subs         map[chan struct{}]bool
	subscribed   int
	unsubscribed int
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{subs: map[chan struct{}]bool{}}
}

func (n *fakeNotifier) Subscribe(string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs[ch] = true
	n.subscribed++
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			delete(n.subs, ch)
			n.unsubscribed++
		})
	}
}

// fire wakes every subscriber and reports how many there were: zero means
// the wake-up was lost.
func (n *fakeNotifier) fire() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return len(n.subs)
}

func (n *fakeNotifier) counts() (subscribed, unsubscribed int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.subscribed, n.unsubscribed
}

// scriptedStore implements only Lease (the rest of store.Store is nil): each
// call runs the next hook, or returns nothing once the hooks run out.
type scriptedStore struct {
	store.Store
	mu    sync.Mutex
	hooks []func() []store.Job
	calls int
}

func (s *scriptedStore) Lease(ctx context.Context, queue string, n int, ttl time.Duration) ([]store.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.hooks) == 0 {
		return nil, nil
	}
	h := s.hooks[0]
	s.hooks = s.hooks[1:]
	return h(), nil
}

func leasedJob() store.Job {
	return store.Job{ID: 1, Queue: "emails", Kind: "k", State: store.StateLeased, Priority: store.PriorityDefault, Attempts: 1, LeaseToken: uuid.New()}
}

// A job enqueued between the first attempt and the wait must still wake the
// lease: the subscription has to exist before the first attempt. Here the
// first attempt itself fires the notification, the worst case of that race.
func TestLeaseSubscribesBeforeTheFirstAttempt(t *testing.T) {
	n := newFakeNotifier()
	var subscribersAtFire int
	st := &scriptedStore{hooks: []func() []store.Job{
		func() []store.Job { subscribersAtFire = n.fire(); return nil }, // a job lands right after this attempt
		func() []store.Job { return []store.Job{leasedJob()} },
	}}
	// No poll tick and a long wait: only the wake-up can end the wait in time.
	srv := NewServer(st, n, slog.New(slog.DiscardHandler), WithPollInterval(time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := srv.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(MaxWait)})
	require.NoError(t, err)
	require.Equal(t, 1, subscribersAtFire, "the lease was subscribed when the first attempt ran")
	require.Len(t, res.GetJobs(), 1, "woken by the notification")
	sub, unsub := n.counts()
	require.Equal(t, 1, sub)
	require.Equal(t, 1, unsub, "the subscription is released")
}

func TestLeaseCancelledWhileWaitingUnsubscribes(t *testing.T) {
	n := newFakeNotifier()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := &scriptedStore{hooks: []func() []store.Job{
		func() []store.Job { cancel(); return nil }, // the client goes away during the wait
	}}
	srv := NewServer(st, n, slog.New(slog.DiscardHandler), WithPollInterval(time.Hour))

	_, err := srv.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", Wait: durationpb.New(MaxWait)})
	requireCode(t, codes.Canceled, err)
	sub, unsub := n.counts()
	require.Equal(t, 1, sub)
	require.Equal(t, 1, unsub)
}
