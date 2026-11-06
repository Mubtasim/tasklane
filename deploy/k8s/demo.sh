#!/usr/bin/env bash
# Tasklane on Kubernetes on this machine (design §20). From the repo root:
#   deploy/k8s/demo.sh up          # cluster, KEDA, Postgres, Prometheus, Tasklane, workers at 0
#   deploy/k8s/demo.sh load 3000   # a burst into queue demo; measures scale-up, drain, scale-down
#   deploy/k8s/demo.sh restart     # rolling restart of the servers under load; counts failed sends
#   deploy/k8s/demo.sh watch       # worker pods and queue depth, every 2 s
#   deploy/k8s/demo.sh logs        # everything needed to debug a failed run
#   deploy/k8s/demo.sh down
# CLUSTER=kind uses kind (CI); the default is k3d. Tools: deploy/k8s/tools.sh.
# About 1.3-1.5 GB of RAM; stop the Compose demo first (same ports).
set -euo pipefail
cd "$(dirname "$0")/../.."
source deploy/k8s/versions.sh
CLUSTER=${CLUSTER:-k3d}
NAME=tasklane NS=tasklane API_KEY=demo-key HTTP=http://127.0.0.1:8080
K=(kubectl -n "$NS")

log() { printf '\n== %s\n' "$*"; }

# The probes print a number, or nothing and fail when they can't tell. 0 is
# what ends a load measurement, so a failed request must never read as 0.

# depth prints waiting + running jobs in queue demo; 0 only when the server
# answers that the queue doesn't exist (gRPC NOT_FOUND = 5).
depth() {
  local out code body
  out=$(curl -sS -w '\n%{http_code}' -H "Authorization: Bearer $API_KEY" "$HTTP/v1/queues/demo" 2>/dev/null) || return 1
  code=${out##*$'\n'} body=${out%$'\n'*}
  case $code in
  200) jq -e '((.stats.available // "0") | tonumber) + ((.stats.leased // "0") | tonumber)' <<<"$body" 2>/dev/null ;;
  404) jq -e '.code == 5' <<<"$body" >/dev/null 2>&1 && echo 0 ;;
  *) return 1 ;;
  esac
}

# workers prints the ready demo-worker pods; 0 only when the API server
# answers (no readyReplicas yet, or the Deployment doesn't exist).
workers() {
  local n err
  if n=$("${K[@]}" get deploy demo-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null); then
    echo "${n:-0}"
  elif err=$("${K[@]}" get deploy demo-worker 2>&1 >/dev/null); [[ $err == *NotFound* ]]; then
    echo 0
  else
    return 1
  fi
}

cluster_up() {
  case $CLUSTER in
  k3d)
    k3d cluster create "$NAME" --image "$K3S_IMAGE" --agents 0 --wait \
      -p "127.0.0.1:7070:30070@server:0" -p "127.0.0.1:8080:30080@server:0" -p "127.0.0.1:9090:30090@server:0" \
      --k3s-arg "--disable=traefik@server:0" --k3s-arg "--disable=metrics-server@server:0"
    ;;
  kind) kind create cluster --name "$NAME" --image "$KIND_NODE_IMAGE" --config deploy/k8s/kind.yaml --wait 120s ;;
  *) echo "CLUSTER must be k3d or kind" >&2; exit 2 ;;
  esac
}

image_in() {
  docker build -q -t tasklane:dev . >/dev/null
  case $CLUSTER in
  k3d) k3d image import tasklane:dev -c "$NAME" ;;
  kind) kind load docker-image tasklane:dev --name "$NAME" ;;
  esac
}

up() {
  if docker ps --format '{{.Names}}' | grep -q '^tasklane-demo-\|^tasklane-bench-'; then
    echo "The Compose demo or bench stack is running and holds ports 7070/8080; stop it first." >&2
    exit 1
  fi
  log "cluster ($CLUSTER)"
  cluster_up
  log "image"
  image_in
  log "KEDA $KEDA_CHART_VERSION"
  helm repo add kedacore https://kedacore.github.io/charts --force-update >/dev/null
  helm upgrade --install keda kedacore/keda --version "$KEDA_CHART_VERSION" \
    --namespace keda --create-namespace --set webhooks.enabled=false --wait --timeout 5m
  log "Postgres and Prometheus"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
  "${K[@]}" apply -f deploy/k8s/demo/postgres.yaml -f deploy/k8s/demo/prometheus.yaml
  "${K[@]}" rollout status statefulset/postgres --timeout 3m
  log "Tasklane"
  helm upgrade --install tasklane deploy/k8s/tasklane -n "$NS" -f deploy/k8s/demo/values.yaml --wait --timeout 5m
  "${K[@]}" rollout status deploy/prometheus --timeout 2m
  log "workers (KEDA) and producer"
  "${K[@]}" apply -f deploy/k8s/demo/worker.yaml -f deploy/k8s/demo/producer.yaml
  "${K[@]}" wait --for=condition=Ready scaledobject/demo-worker --timeout 2m
  "${K[@]}" get pods
  echo "Ready: gRPC 127.0.0.1:7070, HTTP 127.0.0.1:8080, Prometheus http://127.0.0.1:9090"
}

watch_() {
  while true; do
    printf '%s  workers=%s  jobs=%s\n' "$(date +%T)" "$(workers || echo '?')" "$(depth || echo '?')"
    sleep 2
  done
}

logs() {
  for ns in "$NS" keda; do
    kubectl -n "$ns" get all,scaledobject,hpa,pdb -o wide || true
    kubectl -n "$ns" get events --sort-by=.lastTimestamp | tail -50 || true
    for p in $(kubectl -n "$ns" get pods -o name); do
      echo "---- $ns $p"
      kubectl -n "$ns" logs "$p" --all-containers --tail 200 || true
    done
  done
}

down() {
  case $CLUSTER in
  k3d) k3d cluster delete "$NAME" ;;
  kind) kind delete cluster --name "$NAME" ;;
  esac
}

OUT=${OUT:-deploy/k8s/out}

loadgen() {
  mkdir -p bench/bin "$OUT"
  go build -o bench/bin/loadgen ./bench/loadgen
  bench/bin/loadgen -addr 127.0.0.1:7070 -api-key "$API_KEY" "$@"
}

# load N: put N normal jobs into queue demo at once, then follow KEDA:
# seconds until the first worker, until 3 workers, until the queue is empty,
# and until the workers are back at 0. CHECK=1 enforces design §20.5's limits.
load() {
  local n=${1:-3000} start now first="" three="" empty="" zero="" peak=0 gaveup="" w d t
  w=$(workers || echo '?')
  [[ $w == 0 ]] || echo "note: workers=$w at the start; first_worker may read 0" >&2
  start=$(date +%s)
  loadgen -mode fill -queue demo -kind normal -jobs "$n" -batch 100 -producers 4 >/dev/null
  while true; do
    now=$(date +%s); t=$((now - start))
    # A failed probe is skipped, never counted: 0 would end the run early.
    w="" d=""
    if ! w=$(workers) || ! d=$(depth) || [[ -z $w || -z $d ]]; then
      printf '%4ss workers=%s jobs=%s (probe failed, sample skipped)\n' "$t" "${w:-?}" "${d:-?}"
      ((t > 900)) && { gaveup=1; break; }
      sleep 1
      continue
    fi
    ((w > peak)) && peak=$w
    [[ -z $first && $w -ge 1 ]] && first=$t
    [[ -z $three && $w -ge 3 ]] && three=$t
    [[ -z $empty && $d == 0 ]] && empty=$t
    [[ -n $empty && -z $zero && $w == 0 ]] && zero=$t
    printf '%4ss workers=%s jobs=%s\n' "$t" "$w" "$d"
    [[ -n $zero ]] && break
    ((t > 900)) && { gaveup=1; break; }
    sleep 1
  done
  local line="burst=$n first_worker=${first:-none}${first:+s} three_workers=${three:-none}${three:+s} peak_workers=$peak drained=${empty:-none}${empty:+s} back_to_zero=${zero:-none}${zero:+s}"
  echo "$line" | tee -a "$OUT/load.txt"
  [[ -z $gaveup ]] || { echo "FAIL: gave up after 15 minutes" >&2; exit 1; }
  if [[ ${CHECK:-0} == 1 ]]; then
    [[ -n $three && $three -le 60 ]] || { echo "FAIL: 3 workers not within 60 s" >&2; exit 1; }
    [[ -n $empty && $empty -le 300 ]] || { echo "FAIL: queue not empty within 5 minutes" >&2; exit 1; }
    [[ -n $zero && $((zero - empty)) -le 120 ]] || { echo "FAIL: workers not back to 0 within 2 minutes" >&2; exit 1; }
  fi
}

# restart: open load (RATE jobs/s, 20 worker slots inside loadgen, queue
# restart so KEDA's workers stay out of it) while both servers are replaced.
# Every send must succeed and every job in the window must start.
restart() {
  local rate=${RATE:-50} pid row start took
  rm -f "$OUT/restart.csv"
  start=$(date +%s)
  loadgen -mode open -queue restart -rate "$rate" -workers 20 -warmup 5s -duration 60s -probes=false \
    -scenario k8s -label restart -out "$OUT/restart.csv" &
  pid=$!
  # If the rollout fails and set -e ends the script, don't leave loadgen behind.
  trap "kill $pid 2>/dev/null || true" EXIT
  sleep 15
  log "rolling restart"
  "${K[@]}" rollout restart deploy/tasklane-server
  "${K[@]}" rollout status deploy/tasklane-server --timeout 3m
  # loadgen measures 5 s warm-up + 60 s; a rollout that ended later was only
  # partly measured, so the zero-loss result would not cover it.
  took=$(($(date +%s) - start))
  ((took <= 65)) || { echo "FAIL: rollout finished ${took}s after loadgen started, after its 65 s window" >&2; exit 1; }
  wait "$pid"
  trap - EXIT
  row=$(tail -1 "$OUT/restart.csv")
  # Columns by name, so a new CSV column can't shift them.
  read -r errors dropped valid < <(awk -F, -v row="$row" 'NR == 1 {
      for (i = 1; i <= NF; i++) col[$i] = i
      split(row, r, ",")
      print r[col["errors"]], r[col["dropped"]], r[col["valid"]]
    }' "$OUT/restart.csv")
  echo "rolling restart at $rate jobs/s: errors=$errors dropped=$dropped valid=$valid" | tee -a "$OUT/restart.txt"
  [[ $errors == 0 && $dropped == 0 && $valid == true ]] || { echo "FAIL: requests lost during the restart" >&2; exit 1; }
}

case ${1:-} in
up) up ;;
watch) watch_ ;;
logs) logs ;;
load) load "${2:-3000}" ;;
restart) restart ;;
down) down ;;
*) sed -n '2,10p' "$0"; exit 2 ;;
esac
