#!/usr/bin/env bash
# Runs the M5a load-test scenarios (design.md §19.4) and appends results to
# bench/results/<date>-<profile>/results.csv. A full laptop suite takes about
# an hour and keeps every CPU busy; leave the machine alone meanwhile.
#
#   bench/run.sh                         # full suite, laptop profile
#   SMOKE=1 bench/run.sh                 # 2 s warm-up, 5 s windows, small sizes
#   SCENARIOS="s2 s4" OUT=bench/results/2026-11-08-laptop bench/run.sh
#   SCENARIOS=s2t bench/run.sh           # S2's idle and trickle runs only
#   PROFILE=vm bench/run.sh              # 8+ CPUs
#
# S2 finds the closed-loop maximum M (jobs/s) and the best worker count
# BEST_W; S3, S4 and S6 build their rates from them. A SCENARIOS subset that
# skips s2 must supply them: M=1500 BEST_W=50 SCENARIOS="s4" bench/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE=${PROFILE:-laptop}
SMOKE=${SMOKE:-0}
SCENARIOS=${SCENARIOS:-"s1 s2 s3 s4 s6"}
case $PROFILE in
  laptop) PG_CPUS=0,2 SERVER_CPUS=1 LOADGEN_CPUS=3 ;;
  vm)     PG_CPUS=0-3 SERVER_CPUS=4-5 LOADGEN_CPUS=6-7 ;;
  *) echo "unknown PROFILE $PROFILE" >&2; exit 2 ;;
esac
export PG_CPUS SERVER_CPUS LOADGEN_CPUS

WARMUP=10s DUR=60s BACKLOG=1000000 DEAD=100000 DELAYED=10000 QUEUES=1000
if [[ $SMOKE == 1 ]]; then
  WARMUP=2s DUR=5s BACKLOG=20000 DEAD=5000 DELAYED=1000 QUEUES=50
fi
SUFFIX=$([[ $SMOKE == 1 ]] && echo -smoke || true)
OUT=${OUT:-bench/results/$(date +%F)-$PROFILE$SUFFIX}
mkdir -p "$OUT/details" "$OUT/plans"
COMPOSE=(docker compose -f bench/compose.bench.yml)

# The demo binds the same ports; refuse to fight it.
if docker ps --format '{{.Names}}' | grep -q '^tasklane-demo-'; then
  echo "The demo stack is running; stop it first: docker compose -f deploy/docker-compose.yml down" >&2
  exit 1
fi

log() { printf '\n== %s\n' "$*"; }

up() {
  docker build -q -t tasklane:dev . >/dev/null
  "${COMPOSE[@]}" up -d --wait "$@"
}

psql_() { "${COMPOSE[@]}" exec -T postgres psql -U tasklane -d tasklane -qAt -c "$1"; }

cgroup() {
  local id
  id=$("${COMPOSE[@]}" ps -q "$1")
  echo "/sys/fs/cgroup/system.slice/docker-${id}.scope"
}

reset_db() {
  psql_ "TRUNCATE jobs, queues RESTART IDENTITY"
  psql_ "VACUUM ANALYZE jobs"
}

# lg <scenario> <label> <loadgen flags...>: one measured run; prints its
# throughput on stdout and the loadgen output on stderr.
lg() {
  local scenario=$1 label=$2; shift 2
  taskset -c "$LOADGEN_CPUS" bench/bin/loadgen \
    -scenario "$scenario" -label "$label" -out "$OUT/results.csv" -detail-dir "$OUT/details" \
    -cgroup-server "$(cgroup tasklane)" -cgroup-pg "$(cgroup postgres)" \
    -warmup "$WARMUP" -duration "$DUR" "$@" | tee /dev/stderr | sed -n 's/^throughput=//p' | tail -1
}

fill_() { taskset -c "$LOADGEN_CPUS" bench/bin/loadgen -mode fill -batch 100 "$@" | sed -n 's/^throughput=//p' | tail -1; }

pct() { awk -v m="$1" -v p="$2" 'BEGIN { printf "%d", m * p / 100 }'; }

# explain_on / explain_off bracket a window whose query plans are logged.
# sample_rate keeps the log volume down; explain_off keeps the raw capture in
# plans/<label>.log (gitignored) and distils plans/<label>.txt: the first full
# plan of each distinct query, small enough to commit.
explain_reset() {
  for k in log_min_duration log_analyze sample_rate; do
    psql_ "ALTER SYSTEM RESET auto_explain.$k" >/dev/null 2>&1 || true
  done
  psql_ "SELECT pg_reload_conf()" >/dev/null 2>&1 || true
}
# explain_on [sample_rate]: the redrive makes only a handful of statements, so
# it passes 1; the throughput windows run thousands and take the default.
explain_on() {
  psql_ "ALTER SYSTEM SET auto_explain.log_min_duration = 0"
  psql_ "ALTER SYSTEM SET auto_explain.log_analyze = on"
  psql_ "ALTER SYSTEM SET auto_explain.sample_rate = ${1:-0.05}"
  psql_ "SELECT pg_reload_conf()" >/dev/null
  EXPLAIN_SINCE=$(date -u +%FT%TZ)
}
explain_off() {
  explain_reset
  "${COMPOSE[@]}" logs --no-log-prefix --since "$EXPLAIN_SINCE" postgres > "$OUT/plans/$1.log" 2>&1
  # A plan entry is a "LOG:  duration: ... plan:" line plus the tab-indented
  # lines under it. Key: the first line of the query text, whitespace-folded.
  awk '
    function flush(   k, i, t) {
      if (n == 0) return
      for (i = 1; i <= n; i++) if (buf[i] ~ /Query Text:/) {
        t = buf[i]; sub(/.*Query Text:[ \t]*/, "", t)
        if (t == "" && i < n) t = buf[i + 1]
        gsub(/[ \t]+/, " ", t); k = t; break
      }
      if (k != "" && !(k in seen)) { seen[k] = 1; for (i = 1; i <= n; i++) print buf[i]; print "" }
      n = 0
    }
    /LOG:  duration: .* plan:/ { flush(); buf[++n] = $0; next }
    n > 0 && /^[ \t]/ { buf[++n] = $0; next }
    { flush() }
    END { flush() }
  ' "$OUT/plans/$1.log" > "$OUT/plans/$1.txt"
}
trap 'explain_reset' EXIT

# fill_ and lg print a throughput; an empty one means the run failed, and
# carrying on would compute rate 0 for the rest of the hour-long suite.
need() { [[ -n $1 ]] || { echo "no throughput from: $2" >&2; exit 1; }; }

# wait_ready: the server answers on its metrics port only once it is serving.
wait_ready() {
  for _ in $(seq 60); do
    curl -fs -o /dev/null http://127.0.0.1:9464/metrics && return 0
    sleep 0.5
  done
  echo "server not ready after 30 s" >&2; exit 1
}

go build -o bench/bin/loadgen ./bench/loadgen
up postgres tasklane
wait_ready
# A previous aborted run may have left auto_explain on (ALTER SYSTEM persists).
explain_reset

BEST_W=${BEST_W:-50} M=${M:-0}
for s in $SCENARIOS; do case $s in
s1)
  log "S1 enqueue only"
  for b in 1 10 100; do
    reset_db; m=$(lg s1 "s1-closed-b$b" -mode closed -batch "$b" -producers 16); need "$m" "s1-closed-b$b"
    reset_db; lg s1 "s1-open80-b$b" -mode open -batch "$b" -rate "$(pct "$m" 80)" >/dev/null
  done ;;
s2)
  log "S2 full cycle"
  best=0
  for w in 1 10 50 200; do
    reset_db; m=$(lg s2 "s2-closed-w$w" -mode closed -workers "$w"); need "$m" "s2-closed-w$w"
    if awk -v a="$m" -v b="$best" 'BEGIN { exit !(a > b) }'; then best=$m BEST_W=$w; fi
  done
  M=$best
  for p in 50 80 95; do reset_db; lg s2 "s2-open$p-w$BEST_W" -mode open -workers "$BEST_W" -rate "$(pct "$M" "$p")" >/dev/null; done
  reset_db; lg s2 s2-idle-w200 -mode closed -producers 0 -workers 200 >/dev/null
  reset_db; lg s2 s2-trickle-w200 -mode open -workers 200 -rate 5 >/dev/null ;;
s2t)
  # S2's idle and trickle runs alone: they need no maximum, and they are where
  # wake-up costs show (one lease query per woken long-poll).
  log "S2 idle and trickle"
  reset_db; lg s2 s2-idle-w200 -mode closed -producers 0 -workers 200 >/dev/null
  reset_db; lg s2 s2-trickle-w200 -mode open -workers 200 -rate 5 >/dev/null ;;
s3)
  log "S3 payload size"
  for size in 100 10000 100000; do
    reset_db; m=$(lg s3 "s3-closed-p$size" -mode closed -workers "$BEST_W" -payload "$size"); need "$m" "s3-closed-p$size"
    reset_db; lg s3 "s3-open80-p$size" -mode open -workers "$BEST_W" -payload "$size" -rate "$(pct "$m" 80)" >/dev/null
  done ;;
s4)
  log "S4 big table"
  [[ $M == 0 ]] && { echo "S4 needs S2's maximum; run s2 first in the same invocation" >&2; exit 1; }
  reset_db
  r=$(fill_ -queue backlog -jobs "$BACKLOG" -producers 4); need "$r" "backlog fill"; echo "filled $BACKLOG backlog jobs: $r jobs/s"
  psql_ "VACUUM ANALYZE jobs"
  explain_on; lg s4 s4a-explain -mode open -workers "$BEST_W" -rate "$(pct "$M" 50)" -warmup 2s -duration 10s >/dev/null; explain_off s4a-explain
  lg s4 "s4b-open80-w$BEST_W" -mode open -workers "$BEST_W" -rate "$(pct "$M" 80)" >/dev/null
  r=$(fill_ -queue bench -jobs "$DELAYED" -priority 1 -delay 1h); need "$r" "delayed fill"; echo "delayed: $r jobs/s"
  explain_on; lg s4 s4c-explain -mode open -workers "$BEST_W" -rate "$(pct "$M" 50)" -warmup 2s -duration 10s >/dev/null; explain_off s4c-explain
  lg s4 "s4c-open80-w$BEST_W" -mode open -workers "$BEST_W" -rate "$(pct "$M" 80)" >/dev/null
  psql_ "UPDATE jobs SET state = 'dead', finished_at = now(), last_error = 'bench' WHERE id IN (SELECT id FROM jobs WHERE queue = 'backlog' AND state = 'available' LIMIT $DEAD)"
  explain_on 1; taskset -c "$LOADGEN_CPUS" bench/bin/loadgen -mode redrive -queue backlog -scenario s4 -label s4d-redrive -out "$OUT/results.csv"; explain_off s4d-redrive
  reset_db
  r=$(fill_ -queue q -queues "$QUEUES" -jobs $((QUEUES * 100)) -producers 4); need "$r" "queues fill"; echo "queues: $r jobs/s"
  lg s4 "s4e-open80-w$BEST_W" -mode open -workers "$BEST_W" -rate "$(pct "$M" 80)" >/dev/null ;;
s6)
  log "S6 tracing cost"
  [[ $M == 0 ]] && { echo "S6 needs S2's maximum; run s2 first in the same invocation" >&2; exit 1; }
  # Clear a Jaeger container left by an earlier run: `down -v` without the
  # profile does not remove it, and its network is gone, so `up` would fail.
  "${COMPOSE[@]}" --profile tracing rm -sf jaeger >/dev/null 2>&1 || true
  OTEL_ENDPOINT=http://jaeger:4317 "${COMPOSE[@]}" --profile tracing up -d --wait jaeger tasklane
  wait_ready
  reset_db; lg s6 "s6-traced-open80-w$BEST_W" -mode open -workers "$BEST_W" -rate "$(pct "$M" 80)" >/dev/null
  reset_db; lg s6 s6-traced-idle-w200 -mode closed -producers 0 -workers 200 >/dev/null
  "${COMPOSE[@]}" --profile tracing rm -sf jaeger
  "${COMPOSE[@]}" up -d --wait tasklane
  wait_ready ;;
*) echo "unknown scenario $s" >&2; exit 2 ;;
esac; done

log "done: $OUT/results.csv"
echo "stop the stack with: docker compose -f bench/compose.bench.yml --profile tracing down -v"
