package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// payloadOfSize returns a payload whose JSON encoding is exactly n bytes.
func payloadOfSize(t *testing.T, n int) *structpb.Struct {
	t.Helper()
	p := func(k int) *structpb.Struct {
		return &structpb.Struct{Fields: map[string]*structpb.Value{"d": structpb.NewStringValue(strings.Repeat("x", k))}}
	}
	empty, err := protojson.Marshal(p(0))
	require.NoError(t, err)
	s := p(n - len(empty))
	raw, err := protojson.Marshal(s)
	require.NoError(t, err)
	require.Len(t, raw, n)
	return s
}

func TestJobSpecToStoreEnforcesLimits(t *testing.T) {
	ok := func(spec *tasklanev1.JobSpec) {
		t.Helper()
		_, err := jobSpecToStore("q", spec)
		require.NoError(t, err)
	}
	bad := func(spec *tasklanev1.JobSpec) {
		t.Helper()
		_, err := jobSpecToStore("q", spec)
		requireCode(t, codes.InvalidArgument, err)
	}
	ok(&tasklanev1.JobSpec{Kind: "k", Payload: payloadOfSize(t, MaxPayloadBytes)})
	bad(&tasklanev1.JobSpec{Kind: "k", Payload: payloadOfSize(t, MaxPayloadBytes+1)})
	ok(&tasklanev1.JobSpec{Kind: strings.Repeat("k", 255)})
	bad(&tasklanev1.JobSpec{Kind: strings.Repeat("k", 256)})
	ok(&tasklanev1.JobSpec{Kind: "k", IdempotencyKey: strings.Repeat("i", 255)})
	bad(&tasklanev1.JobSpec{Kind: "k", IdempotencyKey: strings.Repeat("i", 256)})
	ok(&tasklanev1.JobSpec{Kind: "k", TraceContext: strings.Repeat("t", 512)})
	bad(&tasklanev1.JobSpec{Kind: "k", TraceContext: strings.Repeat("t", 513)})
}

func TestTruncateUTF8(t *testing.T) {
	require.Equal(t, "short", truncateUTF8("short", 10))
	require.Equal(t, "abc", truncateUTF8("abcdef", 3))
	// "é" is 2 bytes: cutting at 3 would split the second one.
	require.Equal(t, "é", truncateUTF8("éé", 3))
	long := strings.Repeat("€", 2000) // 3 bytes each
	got := truncateUTF8(long, maxErrorBytes)
	require.LessOrEqual(t, len(got), maxErrorBytes)
	require.Greater(t, len(got), maxErrorBytes-3)
	require.True(t, utf8.ValidString(got))
}

func TestProgramLimitExceededIsInvalidArgument(t *testing.T) {
	err := toStatus(slog.New(slog.DiscardHandler), "Enqueue", &pgconn.PgError{Code: "54000", Message: "index row size 6016 exceeds btree version 4 maximum 2704"})
	requireCode(t, codes.InvalidArgument, err)
	require.NotContains(t, err.Error(), "btree", "database details are not leaked")
}

func TestOversizeValuesAreInvalidArgument(t *testing.T) {
	e := newEnv(t, []string{"secret"})
	ctx := withKey("secret")
	e.mustQueueCtx(t, ctx, "emails")

	_, err := e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "emails", Job: &tasklanev1.JobSpec{Kind: "x", Payload: payloadOfSize(t, MaxPayloadBytes+1)}})
	requireCode(t, codes.InvalidArgument, err)

	// Before the caps, this key reached the unique index and failed there
	// with SQLSTATE 54000, reported as INTERNAL.
	_, err = e.client.Enqueue(ctx, &tasklanev1.EnqueueRequest{Queue: "emails", Job: &tasklanev1.JobSpec{Kind: "x", IdempotencyKey: strings.Repeat("k", 6000)}})
	requireCode(t, codes.InvalidArgument, err)

	base := newHTTP(t, e, func(context.Context) error { return nil })
	big := `{"kind":"x","payload":{"d":"` + strings.Repeat("x", MaxPayloadBytes) + `"}}`
	code, _ := call(t, "POST", base+"/v1/queues/emails/jobs", "secret", big)
	require.Equal(t, http.StatusBadRequest, code)
}

// A full lease at the largest payload must reach the client: before the
// limits, 5 jobs of 1 MiB exceeded the client's default 4 MiB receive limit
// after the jobs were already leased.
func TestLeaseOfMaxJobsAtMaxPayloadSucceeds(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	payload := payloadOfSize(t, MaxPayloadBytes)
	jobs := make([]*tasklanev1.JobSpec, MaxLeaseJobs+1)
	for i := range jobs {
		jobs[i] = &tasklanev1.JobSpec{Kind: "big", Payload: payload}
	}
	_, err := e.client.EnqueueBatch(ctx, &tasklanev1.EnqueueBatchRequest{Queue: "emails", Jobs: jobs})
	require.NoError(t, err)

	res, err := e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", MaxJobs: 1000})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), MaxLeaseJobs, "max_jobs is clamped to MaxLeaseJobs")
	require.Equal(t, payload.AsMap(), res.GetJobs()[0].GetPayload().AsMap())

	res, err = e.client.Lease(ctx, &tasklanev1.LeaseRequest{Queue: "emails", MaxJobs: 1000})
	require.NoError(t, err)
	require.Len(t, res.GetJobs(), 1)
}

func TestNackTruncatesLongErrors(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.mustQueue(t, "emails")
	e.mustEnqueue(t, "emails", &tasklanev1.JobSpec{Kind: "welcome"})
	job := e.mustLeaseOne(t, "emails")

	msg := strings.Repeat("€", 5000) // 15000 bytes
	_, err := e.client.Nack(ctx, &tasklanev1.NackRequest{Id: job.GetId(), LeaseToken: job.GetLeaseToken(), Error: msg})
	require.NoError(t, err)

	got, err := e.client.GetJob(ctx, &tasklanev1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	stored := got.GetJob().GetLastError()
	require.LessOrEqual(t, len(stored), maxErrorBytes)
	require.Greater(t, len(stored), maxErrorBytes-3)
	require.True(t, utf8.ValidString(stored))
	require.True(t, strings.HasPrefix(msg, stored))
}

// endlessJSON is a request body of unknown length that is larger than
// MaxMessageBytes and stays valid JSON for as long as it is read.
type endlessJSON struct {
	n    int
	read bool
}

func (b *endlessJSON) Read(p []byte) (int, error) {
	b.read = true
	if b.n >= MaxMessageBytes+1<<20 {
		return 0, io.EOF
	}
	i := 0
	if b.n == 0 {
		i = copy(p, `{"kind":"x","payload":{"d":"`)
	}
	for ; i < len(p); i++ {
		p[i] = 'x'
	}
	b.n += len(p)
	return len(p), nil
}

func TestHTTPOversizeBodyIs413(t *testing.T) {
	e := newEnv(t, nil)
	e.mustQueue(t, "emails")

	// With a Content-Length, through a real server.
	base := newHTTP(t, e, func(context.Context) error { return nil })
	body := `{"kind":"x","payload":{"d":"` + strings.Repeat("x", MaxMessageBytes) + `"}}`
	code, _ := call(t, "POST", base+"/v1/queues/emails/jobs", "", body)
	require.Equal(t, http.StatusRequestEntityTooLarge, code)

	// Without one (chunked): the limit applies while the body is read.
	h := newHandler(t, e, func(context.Context) error { return nil })
	req := httptest.NewRequest("POST", "/v1/queues/emails/jobs", &endlessJSON{})
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
}

func TestHTTPMissingAuthorizationIsRejectedBeforeTheBodyIsRead(t *testing.T) {
	e := newEnv(t, []string{"secret"})
	e.mustQueueCtx(t, withKey("secret"), "emails")
	h := newHandler(t, e, func(context.Context) error { return nil })

	body := &endlessJSON{}
	req := httptest.NewRequest("POST", "/v1/queues/emails/jobs", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.False(t, body.read, "the body must not be read before the auth check")

	// Health endpoints stay open.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}
