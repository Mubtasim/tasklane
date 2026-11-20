#!/usr/bin/env bash
# The Go SDK module must stay light (design §22.2): none of the server's
# heavy dependencies may appear in what it compiles or in its module graph.
set -euo pipefail
cd "$(dirname "$0")/../sdk/go"
banned='github.com/jackc/pgx|github.com/prometheus/|github.com/testcontainers/|github.com/grpc-ecosystem/grpc-gateway|github.com/docker/'
found=$({ go list -deps -test ./...; go list -m all; } | grep -E "$banned" | sort -u || true)
if [[ -n $found ]]; then
  echo "the Go SDK module depends on server-only modules:" >&2
  echo "$found" >&2
  exit 1
fi
echo "Go SDK dependencies ok ($(go list -m all | wc -l) modules)"
