#!/usr/bin/env bash
# Tests for release-check.sh in throwaway git repos.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
pass=0; failed=0

setup() { # $1 = version written into the files
  repo=$(mktemp -d)
  mkdir -p "$repo/scripts" "$repo/sdk/ts" "$repo/deploy/k8s/tasklane"
  cp "$here/release-check.sh" "$repo/scripts/"
  cat > "$repo/sdk/ts/package.json" <<EOF
{
  "name": "@mubtasim91/tasklane",
  "version": "$1",
  "type": "module"
}
EOF
  printf 'apiVersion: v2\nname: tasklane\nversion: %s\nappVersion: "%s"\n' "$1" "$1" > "$repo/deploy/k8s/tasklane/Chart.yaml"
  printf '# Changelog\n\n## %s\n\n- First line.\n- Second line.\n\n## 0.9.0\n\n- Old.\n' "$1" > "$repo/CHANGELOG.md"
  git -C "$repo" init -q -b main
  git -C "$repo" -c user.name=t -c user.email=t@t add -A
  git -C "$repo" -c user.name=t -c user.email=t@t commit -qm init
}
tag() { git -C "$repo" tag "$@"; }
expect() { # $1 = ok|fail, $2 = name, $3 = substring of output, rest = tag
  local want=$1 name=$2 needle=$3; shift 3
  out=$(cd "$repo" && RELEASE_BRANCH=main scripts/release-check.sh "$@" "$repo/notes.md" 2>&1); code=$?
  if { [[ $want == ok && $code -eq 0 ]] || [[ $want == fail && $code -ne 0 ]]; } && [[ $out == *"$needle"* ]]; then
    pass=$((pass + 1))
  else
    failed=$((failed + 1)); echo "FAIL: $name (exit $code): $out"
  fi
}

setup 1.0.0; tag v1.0.0; tag sdk/go/v1.0.0
expect ok "good release" "v1.0.0 ok" v1.0.0
notes=$(cat "$repo/notes.md")
if [[ $notes == *"- First line."*"- Second line."* && $notes != *"Old."* ]]; then pass=$((pass + 1)); else failed=$((failed + 1)); echo "FAIL: notes content: $notes"; fi

setup 1.0.0-rc.2; tag v1.0.0-rc.2; tag sdk/go/v1.0.0-rc.2
expect ok "release candidate" "v1.0.0-rc.2 ok" v1.0.0-rc.2

setup 1.0.0; tag v1.0.0
expect fail "missing sdk tag" "sdk/go/v1.0.0 does not exist" v1.0.0

setup 1.0.0; tag v1.0.0
git -C "$repo" -c user.name=t -c user.email=t@t commit -q --allow-empty -m two; tag sdk/go/v1.0.0
expect fail "sdk tag elsewhere" "different commits" v1.0.0

setup 1.0.1; tag v1.0.0; tag sdk/go/v1.0.0
expect fail "package.json mismatch" "package.json version is '1.0.1'" v1.0.0

setup 1.0.0; sed -i 's/^appVersion: .*/appVersion: "0.0.0-dev"/' "$repo/deploy/k8s/tasklane/Chart.yaml"
git -C "$repo" -c user.name=t -c user.email=t@t commit -qam chart; tag v1.0.0; tag sdk/go/v1.0.0
expect fail "chart appVersion mismatch" "appVersion is '0.0.0-dev'" v1.0.0

setup 1.0.0; sed -i 's/^## 1.0.0$/## 1.0.0-draft/' "$repo/CHANGELOG.md"
git -C "$repo" -c user.name=t -c user.email=t@t commit -qam cl; tag v1.0.0; tag sdk/go/v1.0.0
expect fail "no changelog section" "no '## 1.0.0' section" v1.0.0

setup 1.0.0
expect fail "bad tag format" "is not vX.Y.Z" 1.0.0
expect fail "missing tag" "v1.0.0 does not exist" v1.0.0

setup 1.0.0; git -C "$repo" checkout -qb side
git -C "$repo" -c user.name=t -c user.email=t@t commit -q --allow-empty -m side; tag v1.0.0; tag sdk/go/v1.0.0
expect fail "not on main" "not on main" v1.0.0

echo "release-check tests: $pass passed, $failed failed"
[[ $failed -eq 0 ]]
