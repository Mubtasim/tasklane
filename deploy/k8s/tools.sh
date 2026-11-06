#!/usr/bin/env bash
# Installs pinned single-binary tools (Linux amd64) into $BIN, default
# ~/.local/bin. A tool already at the pinned version is left alone.
#   deploy/k8s/tools.sh kubectl helm k3d        # the laptop demo
#   BIN=$RUNNER_TEMP/bin deploy/k8s/tools.sh kind kubectl helm
set -euo pipefail
source "$(dirname "$0")/versions.sh"
BIN=${BIN:-$HOME/.local/bin}
mkdir -p "$BIN"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

have() { [[ -x $BIN/$1 ]] && "$BIN/$1" "${@:3}" 2>/dev/null | grep -q -- "$2"; }

for tool in "$@"; do
  case $tool in
  kubectl)
    have kubectl "$KUBECTL_VERSION" version --client && continue
    curl -fsSLo "$BIN/kubectl" "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl" ;;
  helm)
    have helm "$HELM_VERSION" version --short && continue
    curl -fsSL "https://get.helm.sh/helm-$HELM_VERSION-linux-amd64.tar.gz" | tar -xz -C "$tmp"
    mv "$tmp/linux-amd64/helm" "$BIN/helm" ;;
  k3d)
    have k3d "$K3D_VERSION" version && continue
    curl -fsSLo "$BIN/k3d" "https://github.com/k3d-io/k3d/releases/download/$K3D_VERSION/k3d-linux-amd64" ;;
  kind)
    have kind "$KIND_VERSION" version && continue
    curl -fsSLo "$BIN/kind" "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-linux-amd64" ;;
  kubeconform)
    have kubeconform "${KUBECONFORM_VERSION#v}" -v && continue
    curl -fsSL "https://github.com/yannh/kubeconform/releases/download/$KUBECONFORM_VERSION/kubeconform-linux-amd64.tar.gz" | tar -xz -C "$tmp"
    mv "$tmp/kubeconform" "$BIN/kubeconform" ;;
  *) echo "unknown tool $tool" >&2; exit 2 ;;
  esac
  chmod +x "$BIN/$tool"
  echo "installed $tool"
done
