package api

import (
	"context"
	"net/http"
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
func NewHTTPHandler(ctx context.Context, conn *grpc.ClientConn, ready func(context.Context) error) (http.Handler, error) {
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
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})
	mux.Handle("/", gw)
	return mux, nil
}

// errorHandler follows the design's error table: grpc-gateway maps
// FAILED_PRECONDITION to 400 by default, but a stale lease token is a 409.
func errorHandler(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	if status.Code(err) == codes.FailedPrecondition {
		err = &runtime.HTTPStatusError{HTTPStatus: http.StatusConflict, Err: err}
	}
	runtime.DefaultHTTPErrorHandler(ctx, mux, m, w, r, err)
}
