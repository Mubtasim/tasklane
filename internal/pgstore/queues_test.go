package pgstore_test

import (
	"context"
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
