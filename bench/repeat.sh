#!/usr/bin/env bash
# Runs one load-generator configuration several times against the running
# bench stack, so a tuning step is judged on medians: closed full-cycle runs
# vary by about ±20% run to run on the laptop.
#   bench/repeat.sh <out dir> <label> <n> <loadgen flags...>
#   bench/repeat.sh bench/results/x s2-closed-w50 3 -mode closed -workers 50
set -euo pipefail
cd "$(dirname "$0")/.."
out=$1 label=$2 n=$3; shift 3
LOADGEN_CPUS=${LOADGEN_CPUS:-3}
C=(docker compose -f bench/compose.bench.yml)
mkdir -p "$out/details"
cg() { echo "/sys/fs/cgroup/system.slice/docker-$("${C[@]}" ps -q "$1").scope"; }
for i in $(seq "$n"); do
  "${C[@]}" exec -T postgres psql -U tasklane -d tasklane -qAt -c "TRUNCATE jobs, queues RESTART IDENTITY" -c "VACUUM ANALYZE jobs"
  taskset -c "$LOADGEN_CPUS" bench/bin/loadgen -scenario repeat -label "$label-$i" -out "$out/results.csv" -detail-dir "$out/details" \
    -cgroup-server "$(cg tasklane)" -cgroup-pg "$(cg postgres)" "$@" | tail -2
done
