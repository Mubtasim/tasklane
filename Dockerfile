# syntax=docker/dockerfile:1
# One image for the demo: the server and the demo programs. Pick the program
# with the entrypoint (compose does).
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
# The cache mounts keep downloaded modules and build output between builds;
# only the modules these three programs import are downloaded.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/tasklane ./examples/demo/producer ./examples/demo/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["/usr/local/bin/tasklane"]
