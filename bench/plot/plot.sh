#!/usr/bin/env bash
# Draws the benchmark charts in a throwaway Python container, so nothing is
# installed on the host.  Usage: bench/plot/plot.sh bench/results/<dir> [plot.py options]
#   bench/plot/plot.sh bench/results/2026-11-06-laptop --only s1-batch,s3-payload
#   bench/plot/plot.sh bench/results/<after> --only s2-latency-vs-load --before bench/results/<before>
set -euo pipefail
cd "$(dirname "$0")/../.."
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/w" -w /w python:3.13-slim \
  sh -c 'pip install -q --disable-pip-version-check matplotlib==3.9.2 && python bench/plot/plot.py "$0" docs/images/bench "$@"' "$@"
