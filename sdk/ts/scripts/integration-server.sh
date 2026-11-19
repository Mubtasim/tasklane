#!/usr/bin/env bash
# Runs a Tasklane server for the integration tests on 127.0.0.1:7070 (gRPC)
# and :8080 (HTTP), API key test-key, with Postgres in Docker on port 5433
# (5432 is often taken by a local Postgres). Ctrl+C stops the server; remove
# the database with: docker rm -f tasklane-ts-pg
set -euo pipefail
cd "$(dirname "$0")/../../.."
if ! docker inspect tasklane-ts-pg >/dev/null 2>&1; then
  docker run -d --name tasklane-ts-pg -e POSTGRES_PASSWORD=postgres -p 127.0.0.1:5433:5432 postgres:17-alpine >/dev/null
fi
docker start tasklane-ts-pg >/dev/null
until docker exec tasklane-ts-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
export TASKLANE_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5433/postgres?sslmode=disable'
export TASKLANE_API_KEYS=test-key
export TASKLANE_METRICS_ADDR=off
exec go run ./cmd/tasklane
