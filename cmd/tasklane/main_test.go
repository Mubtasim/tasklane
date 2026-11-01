package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/metrics"
)

func TestReadyFailsOnceShutdownBegins(t *testing.T) {
	var shuttingDown atomic.Bool
	pings := 0
	ready := readyFunc(&shuttingDown, func(context.Context) error { pings++; return nil })

	require.NoError(t, ready(context.Background()))
	shuttingDown.Store(true)
	require.Error(t, ready(context.Background()))
	require.Equal(t, 1, pings, "no database ping once shutting down")

	shuttingDown.Store(false)
	down := readyFunc(&shuttingDown, func(context.Context) error { return errors.New("refused") })
	require.Error(t, down(context.Background()))
}

func TestMetricsServerWiring(t *testing.T) {
	m := metrics.New()
	require.Nil(t, newMetricsServer(Config{MetricsAddr: ""}, m), "off means no server")

	srv := newMetricsServer(Config{MetricsAddr: ":9464"}, m)
	require.NotNil(t, srv)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "tasklane_")

	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusNotFound, rec.Code, "only /metrics lives on this port")
}
