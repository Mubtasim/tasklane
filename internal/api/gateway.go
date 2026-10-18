package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// NewHTTPHandler serves the HTTP/JSON gateway, which forwards each call over
// conn to the gRPC server (so the auth interceptor applies to HTTP too; the
// gateway passes the Authorization header on as "authorization" metadata),
// plus /healthz (the process is up) and /readyz (ready reports the database
// reachable).
//
// Gateway request bodies are limited to MaxMessageBytes (413 beyond it). With
// keys configured, a gateway request without an Authorization header gets a
// 401 before its body is read; the gRPC interceptor still checks the key.
func NewHTTPHandler(ctx context.Context, conn *grpc.ClientConn, ready func(context.Context) error, keys []string) (http.Handler, error) {
	gw := runtime.NewServeMux(
		runtime.WithErrorHandler(errorHandler),
		// JSON field names as in the proto and the design doc (job_id, lease_token).
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
		}),
	)
	if err := tasklanev1.RegisterTasklaneServiceHandler(ctx, gw, conn); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok\n"))
	})
	mux.Handle("/", limitGateway(gw, len(keys) > 0))
	return mux, nil
}

// limitGateway rejects unauthenticated requests early (when auth is on) and
// caps the request body at MaxMessageBytes.
func limitGateway(gw *runtime.ServeMux, requireAuth bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && r.Header.Get("Authorization") == "" {
			writeError(gw, w, r, status.Error(codes.Unauthenticated, "missing API key"), 0)
			return
		}
		if r.ContentLength > MaxMessageBytes {
			writeError(gw, w, r, errBodyTooLarge, http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = &limitedBody{ReadCloser: http.MaxBytesReader(w, r.Body, MaxMessageBytes)}
		gw.ServeHTTP(w, r)
	})
}

var errBodyTooLarge = status.Errorf(codes.InvalidArgument, "request body exceeds %d bytes", MaxMessageBytes)

// limitedBody remembers that the body hit its limit: the gateway reports any
// decode error as INVALID_ARGUMENT, and errorHandler turns this case into 413.
type limitedBody struct {
	io.ReadCloser
	tooLarge bool
}

func (b *limitedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		b.tooLarge = true
	}
	return n, err
}

// writeError writes err in the gateway's JSON error format, with httpStatus
// overriding the status code's default mapping when it is non-zero.
func writeError(gw *runtime.ServeMux, w http.ResponseWriter, r *http.Request, err error, httpStatus int) {
	if httpStatus != 0 {
		err = &runtime.HTTPStatusError{HTTPStatus: httpStatus, Err: err}
	}
	_, outbound := runtime.MarshalerForRequest(gw, r)
	runtime.DefaultHTTPErrorHandler(r.Context(), gw, outbound, w, r, err)
}

// isSendTooLarge reports whether err is the gRPC client refusing to send the
// gateway's request because, as protobuf, it is over MaxMessageBytes (a JSON
// body under the HTTP limit can still be). That is the caller's request being
// too large, not a quota: queue-depth RESOURCE_EXHAUSTED stays 429.
func isSendTooLarge(err error) bool {
	return status.Code(err) == codes.ResourceExhausted &&
		strings.Contains(status.Convert(err).Message(), "trying to send message larger than max")
}

// errorHandler follows the design's error table: grpc-gateway maps
// FAILED_PRECONDITION to 400 by default, but a stale lease token is a 409.
// A body over the limit is a 413, not the gateway's 400 for a decode error.
func errorHandler(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	if b, ok := r.Body.(*limitedBody); ok && b.tooLarge {
		err = &runtime.HTTPStatusError{HTTPStatus: http.StatusRequestEntityTooLarge, Err: errBodyTooLarge}
	} else if isSendTooLarge(err) {
		err = &runtime.HTTPStatusError{HTTPStatus: http.StatusRequestEntityTooLarge, Err: err}
	} else if status.Code(err) == codes.FailedPrecondition {
		err = &runtime.HTTPStatusError{HTTPStatus: http.StatusConflict, Err: err}
	}
	runtime.DefaultHTTPErrorHandler(ctx, mux, m, w, r, err)
}
