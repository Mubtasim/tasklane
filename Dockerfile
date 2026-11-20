# syntax=docker/dockerfile:1
# One build stage, two images (design §22.4):
#   server: only the server; published to GHCR by the release workflow.
#   demo (the default, last stage): the server plus the demo producer and
#     worker; used by compose, deploy/k8s/demo.sh and bench/run.sh.
# The build stage runs on the build machine's platform and cross-compiles,
# so a multi-platform build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
WORKDIR /src
COPY . .
# The cache mounts keep downloaded modules and build output between builds;
# only the modules these three programs import are downloaded.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X github.com/mubtasim/tasklane/internal/version.Version=${VERSION} -X github.com/mubtasim/tasklane/internal/version.Revision=${REVISION}" \
      -o /out/ ./cmd/tasklane ./examples/demo/producer ./examples/demo/worker

FROM gcr.io/distroless/static-debian12:nonroot AS server
COPY --from=build /out/tasklane /usr/local/bin/tasklane
USER nonroot
ENTRYPOINT ["/usr/local/bin/tasklane"]

FROM gcr.io/distroless/static-debian12:nonroot AS demo
COPY --from=build /out/ /usr/local/bin/
USER nonroot
ENTRYPOINT ["/usr/local/bin/tasklane"]
