package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
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
