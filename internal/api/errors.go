package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/internal/store"
)

// toStatus maps a store or database error to a gRPC status (design §6,
// Errors). Sentinel errors keep their message, which never holds secrets.
// Anything unexpected is logged in full and returned as a bare INTERNAL, so
// SQL text and connection details never reach clients.
func toStatus(log *slog.Logger, method string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err // already a status, e.g. INVALID_ARGUMENT from a conversion
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrLeaseLost):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrWrongState):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case isProgramLimitExceeded(err):
		// A safety net behind the request caps: a value too large for an
		// index or row is the caller's argument, not a server fault.
		log.Warn("value exceeds a database limit", "method", method, "err", err)
		return status.Error(codes.InvalidArgument, "a value exceeds a size limit")
	case isUnavailable(err):
		// Before the context cases: pgconn wraps connect timeouts so that
		// errors.Is(err, context.DeadlineExceeded) is true for them too.
		log.Warn("database unavailable", "method", method, "err", err)
		return status.Error(codes.Unavailable, "database unavailable")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	}
	log.Error("internal error", "method", method, "err", err)
	return status.Error(codes.Internal, "internal error")
}

// isProgramLimitExceeded reports SQLSTATE 54000 (program_limit_exceeded),
// such as an index row over the btree size limit.
func isProgramLimitExceeded(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "54000"
}

// isUnavailable reports errors that mean "the database can't be reached right
// now": a retry later may succeed.
func isUnavailable(err error) bool {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	// context.DeadlineExceeded is itself a net.Error; a bare one is the
	// caller's deadline, not an unreachable database.
	var netErr net.Error
	if (errors.As(err, &netErr) && !errors.Is(err, context.DeadlineExceeded)) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), // connection exception class
			pgErr.Code == "57P01", // admin_shutdown
			pgErr.Code == "57P02", // crash_shutdown
			pgErr.Code == "57P03", // cannot_connect_now
			pgErr.Code == "53300": // too_many_connections
			return true
		}
	}
	return false
}
