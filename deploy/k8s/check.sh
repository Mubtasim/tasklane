#!/usr/bin/env bash
# Static checks for the Kubernetes deployment (design §20.5): helm lint,
# kubeconform on every rendered variant and the demo manifests, Envoy's own
# config validation, and shell syntax. Needs helm, kubeconform and Docker.
set -euo pipefail
cd "$(dirname "$0")/../.."
source deploy/k8s/versions.sh
chart=deploy/k8s/tasklane
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

kc() {
  kubeconform -strict -summary \
    -schema-location default \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
    "$@"
}

helm lint "$chart" --set databaseURL=postgres://u:p@db/tasklane
helm template tasklane "$chart" -n tasklane --set databaseURL=postgres://u:p@db/tasklane > "$tmp/default.yaml"
helm template tasklane "$chart" -n tasklane --set databaseURL=postgres://u:p@db/tasklane --set envoy.enabled=false > "$tmp/no-envoy.yaml"
kc "$tmp/default.yaml" "$tmp/no-envoy.yaml"
# The demo: the chart with its values, and the plain manifests (values.yaml is
# Helm input, not a Kubernetes object).
helm template tasklane "$chart" -n tasklane -f deploy/k8s/demo/values.yaml > "$tmp/demo-values.yaml"
kc "$tmp/demo-values.yaml" deploy/k8s/demo/{postgres,prometheus,worker,producer}.yaml

sed 's/TASKLANE_HEADLESS/tasklane-headless.tasklane.svc.cluster.local/' "$chart/files/envoy.yaml" > "$tmp/envoy.yaml"
chmod 755 "$tmp" # mktemp makes it 700; the envoy image runs as a non-root user
docker run --rm -v "$tmp:/cfg:ro" envoyproxy/envoy:v1.36.2 --mode validate -c /cfg/envoy.yaml

bash -n deploy/k8s/tools.sh
bash -n deploy/k8s/demo.sh
echo "k8s static checks passed"
