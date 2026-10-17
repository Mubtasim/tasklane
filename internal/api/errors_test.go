package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/internal/store"
)

func TestToStatusMapsErrors(t *testing.T) {
	// A real connection error: nothing listens on port 1.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, connectErr := pgconn.Connect(ctx, "postgres://u@127.0.0.1:1/db?connect_timeout=1")
	require.Error(t, connectErr)

	// A connect timeout: pgconn wraps context.DeadlineExceeded in a
	// ConnectError, and it must still map to UNAVAILABLE.
	tctx, tcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer tcancel()
	_, timeoutErr := pgconn.Connect(tctx, "postgres://u@10.255.255.1:5432/db?connect_timeout=1")
	require.Error(t, timeoutErr)
	var ce *pgconn.ConnectError
	require.ErrorAs(t, timeoutErr, &ce, "expected a ConnectError, got %v", timeoutErr)
	require.ErrorIs(t, timeoutErr, context.DeadlineExceeded)

	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not found", fmt.Errorf("%w: job 7", store.ErrNotFound), codes.NotFound},
		{"lease lost", store.ErrLeaseLost, codes.FailedPrecondition},
		{"invalid", fmt.Errorf("%w: kind is required", store.ErrInvalid), codes.InvalidArgument},
		{"already exists", store.ErrAlreadyExists, codes.AlreadyExists},
		{"deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"cancelled", context.Canceled, codes.Canceled},
		{"connect error", connectErr, codes.Unavailable},
		{"net error", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, codes.Unavailable},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, codes.Unavailable},
		{"connection exception", &pgconn.PgError{Code: "08006"}, codes.Unavailable},
		{"connect timeout", timeoutErr, codes.Unavailable},
		{"wrapped connect timeout", fmt.Errorf("acquire: %w", timeoutErr), codes.Unavailable},
		{"timeout net error", fmt.Errorf("q: %w", timeoutNetErr{}), codes.Unavailable},
		{"unexpected EOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), codes.Unavailable},
		{"crash shutdown", &pgconn.PgError{Code: "57P02"}, codes.Unavailable},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, codes.Unavailable},
		{"too many connections", &pgconn.PgError{Code: "53300"}, codes.Unavailable},
		{"wrapped status", fmt.Errorf("wrap: %w", status.Error(codes.InvalidArgument, "bad")), codes.InvalidArgument},
		{"existing status", status.Error(codes.InvalidArgument, "bad"), codes.InvalidArgument},
		{"anything else", &pgconn.PgError{Code: "42P01", Message: `relation "jobs" does not exist`}, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := toStatus(slog.New(slog.DiscardHandler), "/test", tc.err)
			require.Equal(t, tc.want, status.Code(got))
		})
	}
	require.NoError(t, toStatus(slog.New(slog.DiscardHandler), "/test", nil))
}

func TestInternalErrorsAreLoggedNotLeaked(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	secret := errors.New(`password authentication failed for user "tasklane"`)

	got := toStatus(log, "/tasklane.v1.TasklaneService/GetJob", secret)

	require.Equal(t, codes.Internal, status.Code(got))
	require.NotContains(t, status.Convert(got).Message(), "password")
	require.Contains(t, logs.String(), "password authentication failed")
	require.Contains(t, logs.String(), "GetJob")
}

// timeoutNetErr is a net.Error that reports a timeout.
type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "i/o timeout" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

func TestUnavailableHasFixedMessage(t *testing.T) {
	err := &pgconn.PgError{Code: "08006", Message: "host 10.0.0.5 refused"}
	got := toStatus(slog.New(slog.DiscardHandler), "/test", err)
	require.Equal(t, codes.Unavailable, status.Code(got))
	require.Equal(t, "database unavailable", status.Convert(got).Message())
}
