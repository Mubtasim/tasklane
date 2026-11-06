#!/usr/bin/env bash
# CI end-to-end test for the Kubernetes deployment (design §20.5): bring the
# demo up, run 200 jobs through KEDA-scaled workers, then replace both
# servers under load and require zero failed requests.
set -euo pipefail
cd "$(dirname "$0")/../.."
export CLUSTER=${CLUSTER:-kind}
demo=deploy/k8s/demo.sh
stats='{}' left=unknown
API_KEY=demo-key HTTP=http://127.0.0.1:8080

$demo up

echo "== 200 jobs"
go build -o bench/bin/loadgen ./bench/loadgen
bench/bin/loadgen -addr 127.0.0.1:7070 -api-key "$API_KEY" -mode fill -queue demo -kind normal -jobs 200 -batch 50 >/dev/null
for i in $(seq 180); do
  # A failed request is retried on the next pass, not fatal under set -e.
  if s=$(curl -fsS -H "Authorization: Bearer $API_KEY" "$HTTP/v1/queues/demo") &&
    l=$(jq -e '((.stats.available // "0") | tonumber) + ((.stats.leased // "0") | tonumber)' <<<"$s"); then
    stats=$s left=$l
    [[ $left == 0 ]] && break
  fi
  sleep 1
done
dead=$(jq '(.stats.dead // "0") | tonumber' <<<"$stats")
[[ $left == 0 && $dead == 0 ]] || { echo "FAIL: $left jobs left, $dead dead after 180 s" >&2; exit 1; }
echo "200 jobs done in about ${i}s"

$demo restart
echo "k8s e2e passed"
