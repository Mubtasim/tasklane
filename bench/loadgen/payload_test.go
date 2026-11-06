package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPayloadRoundTripsTheScheduledTime(t *testing.T) {
	at := time.Unix(1_762_400_000, 123_456_789)
	p := newPayload(at, 100)
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	got, ok := scheduledTime(raw)
	require.True(t, ok)
	require.True(t, at.Equal(got), "nanosecond-exact: %v vs %v", at, got)
}

func TestPayloadHasRoughlyTheRequestedSize(t *testing.T) {
	for _, size := range []int{100, 10_000, 100_000} {
		raw, err := json.Marshal(newPayload(time.Now(), size))
		require.NoError(t, err)
		require.InDelta(t, size, len(raw), float64(size)*0.05+40, "size %d", size)
	}
}

func TestScheduledTimeRejectsOtherPayloads(t *testing.T) {
	_, ok := scheduledTime([]byte(`{"x":1}`))
	require.False(t, ok)
	_, ok = scheduledTime([]byte(`{"t":"abc"}`))
	require.False(t, ok)
}
