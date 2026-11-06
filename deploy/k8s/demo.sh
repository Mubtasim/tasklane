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

# depth prints waiting + running jobs in queue demo (0 if it doesn't exist).
depth() {
  curl -fsS -H "Authorization: Bearer $API_KEY" "$HTTP/v1/queues/demo" 2>/dev/null |
    jq '((.stats.available // "0") | tonumber) + ((.stats.leased // "0") | tonumber)' || echo 0
}

# workers prints the ready demo-worker pods.
workers() {
  local n
  n=$("${K[@]}" get deploy demo-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  echo "${n:-0}"
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
    printf '%s  workers=%s  jobs=%s\n' "$(date +%T)" "$(workers)" "$(depth)"
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

case ${1:-} in
up) up ;;
watch) watch_ ;;
logs) logs ;;
down) down ;;
*) sed -n '2,10p' "$0"; exit 2 ;;
esac
