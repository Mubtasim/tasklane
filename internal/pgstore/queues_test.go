package pgstore_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestCreateQueueAppliesDefaults(t *testing.T) {
	s := newStore(t)
	mustQueue(t, s, store.Queue{Name: "emails"})

	q, err := s.GetQueue(context.Background(), "emails")
	require.NoError(t, err)
	require.Equal(t, store.Queue{Name: "emails", MaxDepth: 100_000, VisibilityTimeout: 30 * time.Second, MaxAttempts: 10}, q)
}

func TestCreateQueueKeepsSettings(t *testing.T) {
	s := newStore(t)
	want := store.Queue{Name: "pdf", MaxDepth: 5, VisibilityTimeout: 2 * time.Minute, MaxAttempts: 3, Paused: true}
	mustQueue(t, s, want)

	got, err := s.GetQueue(context.Background(), "pdf")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCreateQueueRejectsDuplicateAndEmptyName(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustQueue(t, s, store.Queue{Name: "emails"})

	require.ErrorIs(t, s.CreateQueue(ctx, store.Queue{Name: "emails"}), store.ErrAlreadyExists)
	require.ErrorIs(t, s.CreateQueue(ctx, store.Queue{}), store.ErrInvalid)
}

func TestGetQueueNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.GetQueue(context.Background(), "nope")
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestCreateQueueRejectsBadSettings(t *testing.T) {
	s := newStore(t)
	for name, q := range map[string]store.Queue{
		"negative max depth":          {Name: "q", MaxDepth: -1},
		"negative max attempts":       {Name: "q", MaxAttempts: -1},
		"negative visibility timeout": {Name: "q", VisibilityTimeout: -time.Second},
		"sub-millisecond timeout":     {Name: "q", VisibilityTimeout: 500 * time.Microsecond},
	} {
		require.ErrorIs(t, s.CreateQueue(context.Background(), q), store.ErrInvalid, name)
	}
	_, err := s.GetQueue(context.Background(), "q")
	require.ErrorIs(t, err, store.ErrNotFound, "nothing was created")

	// The smallest positive timeout is accepted.
	mustQueue(t, s, store.Queue{Name: "fast", VisibilityTimeout: time.Millisecond})
	q, err := s.GetQueue(context.Background(), "fast")
	require.NoError(t, err)
	require.Equal(t, time.Millisecond, q.VisibilityTimeout)
}

func TestCreateQueueRejectsUnsafeNames(t *testing.T) {
	s := newStore(t)
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"a/b", false},
		{"a:b", false},
		{"has space", false},
		{"a\n", false}, // a trailing newline must not slip past the anchored pattern
		{"ünïcode", false},
		{strings.Repeat("a", 129), false},
		{"emails", true},
		{"pdf.render-v2", true},
		{strings.Repeat("a", 128), true},
	} {
		err := s.CreateQueue(context.Background(), store.Queue{Name: tc.name})
		if tc.ok {
			require.NoError(t, err, tc.name)
		} else {
			require.ErrorIs(t, err, store.ErrInvalid, tc.name)
		}
	}
}
