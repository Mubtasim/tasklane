package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// newHTTP serves the gateway for e over a real HTTP server.
func newHTTP(t *testing.T, e *env, ready func(context.Context) error) string {
	t.Helper()
	h, err := NewHTTPHandler(context.Background(), e.conn, ready)
	require.NoError(t, err)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// call sends a request and returns the status and the decoded JSON body (nil
// for a non-JSON body).
func call(t *testing.T, method, url, key, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	require.NoError(t, err)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestHTTPGatewayJobCycle(t *testing.T) {
	e := newEnv(t, []string{"secret"})
	base := newHTTP(t, e, func(context.Context) error { return nil })

	code, _ := call(t, "POST", base+"/v1/queues", "secret", `{"name":"emails"}`)
	require.Equal(t, http.StatusOK, code)

	// The README's curl example: the body is the JobSpec.
	code, body := call(t, "POST", base+"/v1/queues/emails/jobs", "secret", `{"kind":"send_welcome","payload":{"user":42}}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "1", body["job_id"], "int64 is a JSON string in proto3 JSON")
	require.Equal(t, true, body["created"])

	code, body = call(t, "POST", base+"/v1/queues/emails:lease", "secret", `{"max_jobs":1}`)
	require.Equal(t, http.StatusOK, code)
	jobs := body["jobs"].([]any)
	require.Len(t, jobs, 1)
	job := jobs[0].(map[string]any)
	require.Equal(t, map[string]any{"user": 42.0}, job["payload"])
	require.Equal(t, "JOB_STATE_LEASED", job["state"])
	token := job["lease_token"].(string)

	ack := `{"lease_token":"` + token + `"}`
	code, _ = call(t, "POST", base+"/v1/jobs/1:ack", "secret", ack)
	require.Equal(t, http.StatusOK, code)
	code, _ = call(t, "POST", base+"/v1/jobs/1:ack", "secret", ack)
	require.Equal(t, http.StatusOK, code, "idempotent ack")

	code, body = call(t, "GET", base+"/v1/jobs/1", "secret", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "JOB_STATE_COMPLETED", body["job"].(map[string]any)["state"])
}

func TestHTTPGatewayErrorStatuses(t *testing.T) {
	e := newEnv(t, []string{"secret"})
	base := newHTTP(t, e, func(context.Context) error { return nil })
	code, _ := call(t, "POST", base+"/v1/queues", "secret", `{"name":"emails"}`)
	require.Equal(t, http.StatusOK, code)

	code, _ = call(t, "GET", base+"/v1/queues/emails", "", "")
	require.Equal(t, http.StatusUnauthorized, code, "the Authorization header is forwarded and checked")

	code, body := call(t, "POST", base+"/v1/queues/nope:lease", "secret", `{}`)
	require.Equal(t, http.StatusNotFound, code)
	require.Contains(t, body["message"], "not found")

	code, _ = call(t, "POST", base+"/v1/queues/emails/jobs", "secret", `{"payload":{}}`)
	require.Equal(t, http.StatusBadRequest, code) // kind is required

	code, _ = call(t, "POST", base+"/v1/queues", "secret", `{"name":"emails"}`)
	require.Equal(t, http.StatusConflict, code) // already exists

	code, _ = call(t, "POST", base+"/v1/queues/emails/jobs", "secret", `{"kind":"x"}`)
	require.Equal(t, http.StatusOK, code)
	code, _ = call(t, "POST", base+"/v1/jobs/1:ack", "secret", `{"lease_token":"8a0c5b9e-1111-4d6a-9a43-2d0f6f0f0f0f"}`)
	require.Equal(t, http.StatusConflict, code, "stale token: FAILED_PRECONDITION is 409, not the gateway's default 400")
}

func TestHealthAndReadiness(t *testing.T) {
	e := newEnv(t, []string{"secret"})
	var dbDown atomic.Bool // read by the HTTP server's goroutine
	base := newHTTP(t, e, func(context.Context) error {
		if dbDown.Load() {
			return errors.New("connection refused")
		}
		return nil
	})

	code, _ := call(t, "GET", base+"/healthz", "", "")
	require.Equal(t, http.StatusOK, code, "no API key needed")
	code, _ = call(t, "GET", base+"/readyz", "", "")
	require.Equal(t, http.StatusOK, code)

	dbDown.Store(true)
	code, _ = call(t, "GET", base+"/readyz", "", "")
	require.Equal(t, http.StatusServiceUnavailable, code)
	code, _ = call(t, "GET", base+"/healthz", "", "")
	require.Equal(t, http.StatusOK, code, "liveness doesn't depend on the database")
}
