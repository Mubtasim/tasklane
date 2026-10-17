package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/store"
)

func requireCode(t *testing.T, want codes.Code, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, want, status.Code(err), err.Error())
}

func TestPayloadRoundTrip(t *testing.T) {
	in, err := structpb.NewStruct(map[string]any{"user": 42, "tags": []any{"a", "b"}, "vip": true})
	require.NoError(t, err)

	raw, err := payloadToStore(in)
	require.NoError(t, err)
	require.JSONEq(t, `{"user":42,"tags":["a","b"],"vip":true}`, string(raw))

	out, err := payloadFromStore(raw)
	require.NoError(t, err)
	require.Equal(t, in.AsMap(), out.AsMap())

	raw, err = payloadToStore(nil)
	require.NoError(t, err)
	require.Nil(t, raw, "no payload: the store stores {}")

	_, err = payloadFromStore(json.RawMessage(`[1,2]`))
	require.Error(t, err, "stored payloads must be objects")
}

func TestDurationAndTimeConversions(t *testing.T) {
	d, err := durationToStore("wait", durationpb.New(5*time.Second))
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, d)

	d, err = durationToStore("wait", nil)
	require.NoError(t, err)
	require.Zero(t, d)

	_, err = durationToStore("wait", durationpb.New(-time.Second))
	requireCode(t, codes.InvalidArgument, err)

	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	got, err := timeToStore("run_at", timestamppb.New(at))
	require.NoError(t, err)
	require.True(t, at.Equal(got))
	require.Nil(t, timeFromStore(time.Time{}))

	_, err = timeToStore("run_at", &timestamppb.Timestamp{Seconds: -1 << 62})
	requireCode(t, codes.InvalidArgument, err)
}

func TestTokenConversion(t *testing.T) {
	tok := uuid.New()
	got, err := tokenToStore(tok.String())
	require.NoError(t, err)
	require.Equal(t, tok, got)

	_, err = tokenToStore("not-a-token")
	requireCode(t, codes.InvalidArgument, err)
	_, err = tokenToStore("")
	requireCode(t, codes.InvalidArgument, err)
}

func TestPriorityConversion(t *testing.T) {
	p, err := priorityToStore(tasklanev1.Priority_PRIORITY_CRITICAL)
	require.NoError(t, err)
	require.Equal(t, store.PriorityCritical, p)

	p, err = priorityToStore(tasklanev1.Priority_PRIORITY_UNSPECIFIED)
	require.NoError(t, err)
	require.Equal(t, store.PriorityUnspecified, p)

	_, err = priorityToStore(tasklanev1.Priority(9))
	requireCode(t, codes.InvalidArgument, err)
}

func TestJobFromStoreHidesTokenUnlessLeasing(t *testing.T) {
	j := store.Job{ID: 7, State: store.StateLeased, Priority: store.PriorityHigh, LeaseToken: uuid.New(), Payload: json.RawMessage(`{"a":1}`)}

	leased, err := jobFromStore(j, true)
	require.NoError(t, err)
	require.Equal(t, j.LeaseToken.String(), leased.GetLeaseToken())
	require.Equal(t, tasklanev1.JobState_JOB_STATE_LEASED, leased.GetState())
	require.Equal(t, tasklanev1.Priority_PRIORITY_HIGH, leased.GetPriority())

	viewed, err := jobFromStore(j, false)
	require.NoError(t, err)
	require.Empty(t, viewed.GetLeaseToken())
}

func TestQueueConversion(t *testing.T) {
	q, err := queueToStore(&tasklanev1.Queue{Name: "emails", MaxDepth: 5, VisibilityTimeout: durationpb.New(time.Minute), MaxAttempts: 3, Paused: true})
	require.NoError(t, err)
	require.Equal(t, store.Queue{Name: "emails", MaxDepth: 5, VisibilityTimeout: time.Minute, MaxAttempts: 3, Paused: true}, q)

	_, err = queueToStore(nil)
	requireCode(t, codes.InvalidArgument, err)
}
