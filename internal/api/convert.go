package api

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/store"
)

// Conversions between the proto API and the store types. Request-side
// helpers return INVALID_ARGUMENT status errors, so RPC handlers can return
// them unchanged.

func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// payloadToStore turns a Struct into a JSON object; nil means "no payload".
func payloadToStore(p *structpb.Struct) (json.RawMessage, error) {
	if p == nil {
		return nil, nil
	}
	b, err := protojson.Marshal(p)
	if err != nil {
		return nil, invalid("payload: %v", err)
	}
	return b, nil
}

// payloadFromStore parses a stored payload. Payloads always enter through the
// API as objects, so a failure here is an internal error, not a bad request.
func payloadFromStore(raw json.RawMessage) (*structpb.Struct, error) {
	if len(raw) == 0 {
		return &structpb.Struct{}, nil
	}
	s := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, s); err != nil {
		return nil, fmt.Errorf("decode stored payload: %w", err)
	}
	return s, nil
}

// durationToStore converts an optional duration; nil means zero ("default").
func durationToStore(field string, d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, invalid("%s: %v", field, err)
	}
	v := d.AsDuration()
	if v < 0 {
		return 0, invalid("%s must not be negative", field)
	}
	return v, nil
}

// timeToStore converts an optional timestamp; nil means the zero time.
func timeToStore(field string, ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, invalid("%s: %v", field, err)
	}
	return ts.AsTime(), nil
}

// timeFromStore maps the zero time to an unset field.
func timeFromStore(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func tokenToStore(s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.Nil, invalid("lease_token is required")
	}
	t, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, invalid("lease_token is not a valid token")
	}
	return t, nil
}

func priorityToStore(p tasklanev1.Priority) (store.Priority, error) {
	if p < tasklanev1.Priority_PRIORITY_UNSPECIFIED || p > tasklanev1.Priority_PRIORITY_LOW {
		return 0, invalid("unknown priority %d", p)
	}
	return store.Priority(p), nil // the enums share their numbering
}

var stateFromStore = map[store.JobState]tasklanev1.JobState{
	store.StateAvailable: tasklanev1.JobState_JOB_STATE_AVAILABLE,
	store.StateLeased:    tasklanev1.JobState_JOB_STATE_LEASED,
	store.StateCompleted: tasklanev1.JobState_JOB_STATE_COMPLETED,
	store.StateDead:      tasklanev1.JobState_JOB_STATE_DEAD,
	store.StateCancelled: tasklanev1.JobState_JOB_STATE_CANCELLED,
}

func queueToStore(q *tasklanev1.Queue) (store.Queue, error) {
	if q == nil {
		return store.Queue{}, invalid("queue is required")
	}
	vt, err := durationToStore("visibility_timeout", q.GetVisibilityTimeout())
	if err != nil {
		return store.Queue{}, err
	}
	return store.Queue{
		Name:              q.GetName(),
		MaxDepth:          q.GetMaxDepth(),
		VisibilityTimeout: vt,
		MaxAttempts:       int(q.GetMaxAttempts()),
		Paused:            q.GetPaused(),
	}, nil
}

func queueFromStore(q store.Queue) *tasklanev1.Queue {
	return &tasklanev1.Queue{
		Name:              q.Name,
		MaxDepth:          q.MaxDepth,
		VisibilityTimeout: durationpb.New(q.VisibilityTimeout),
		MaxAttempts:       int32(q.MaxAttempts),
		Paused:            q.Paused,
	}
}

func jobSpecToStore(queue string, spec *tasklanev1.JobSpec) (store.NewJob, error) {
	if spec == nil {
		return store.NewJob{}, invalid("job is required")
	}
	switch {
	case len(spec.GetKind()) > maxKindBytes:
		return store.NewJob{}, invalid("kind must be at most %d bytes", maxKindBytes)
	case len(spec.GetIdempotencyKey()) > maxIdempotencyKeyBytes:
		return store.NewJob{}, invalid("idempotency_key must be at most %d bytes", maxIdempotencyKeyBytes)
	case len(spec.GetTraceContext()) > maxTraceContextBytes:
		return store.NewJob{}, invalid("trace_context must be at most %d bytes", maxTraceContextBytes)
	}
	payload, err := payloadToStore(spec.GetPayload())
	if err != nil {
		return store.NewJob{}, err
	}
	if len(payload) > MaxPayloadBytes {
		return store.NewJob{}, invalid("payload must be at most %d bytes as JSON, got %d", MaxPayloadBytes, len(payload))
	}
	priority, err := priorityToStore(spec.GetPriority())
	if err != nil {
		return store.NewJob{}, err
	}
	runAt, err := timeToStore("run_at", spec.GetRunAt())
	if err != nil {
		return store.NewJob{}, err
	}
	return store.NewJob{
		Queue:          queue,
		Kind:           spec.GetKind(),
		Payload:        payload,
		Priority:       priority,
		RunAt:          runAt,
		MaxAttempts:    int(spec.GetMaxAttempts()),
		IdempotencyKey: spec.GetIdempotencyKey(),
		TraceContext:   spec.GetTraceContext(),
	}, nil
}

// jobFromStore converts a job. withToken is true only for Lease responses:
// the token is the right to complete the job, so only its holder gets it.
func jobFromStore(j store.Job, withToken bool) (*tasklanev1.Job, error) {
	payload, err := payloadFromStore(j.Payload)
	if err != nil {
		return nil, err
	}
	state, ok := stateFromStore[j.State]
	if !ok {
		return nil, fmt.Errorf("job %d has unknown state %q", j.ID, j.State)
	}
	if j.Priority < store.PriorityCritical || j.Priority > store.PriorityLow {
		return nil, fmt.Errorf("job %d has invalid priority %d", j.ID, j.Priority)
	}
	out := &tasklanev1.Job{
		Id:             j.ID,
		Queue:          j.Queue,
		Kind:           j.Kind,
		Payload:        payload,
		Priority:       tasklanev1.Priority(j.Priority),
		State:          state,
		RunAt:          timeFromStore(j.RunAt),
		Attempts:       int32(j.Attempts),
		MaxAttempts:    int32(j.MaxAttempts),
		LastError:      j.LastError,
		LeaseExpiresAt: timeFromStore(j.LeaseExpiresAt),
		IdempotencyKey: j.IdempotencyKey,
		TraceContext:   j.TraceContext,
		CreatedAt:      timeFromStore(j.CreatedAt),
		FinishedAt:     timeFromStore(j.FinishedAt),
	}
	if withToken && j.LeaseToken != uuid.Nil {
		out.LeaseToken = j.LeaseToken.String()
	}
	return out, nil
}

// truncateUTF8 cuts s to at most n bytes without splitting a character.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
