#!/usr/bin/env bash
# Checks that a release tag matches the repo, and writes the release notes
# (design §22.3 step 1). Reads the files from the tagged commit, not the
# working tree.
#   scripts/release-check.sh v1.0.0 [notes-file]
# RELEASE_BRANCH (default origin/main) is the branch the tag must be on.
set -euo pipefail
tag=${1:?usage: release-check.sh vX.Y.Z[-rc.N] [notes-file]}
notes=${2:-/dev/null}
branch=${RELEASE_BRANCH:-origin/main}
cd "$(dirname "$0")/.."
fail() { echo "release-check: $*" >&2; exit 1; }

[[ $tag =~ ^v([0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?)$ ]] || fail "tag $tag is not vX.Y.Z or vX.Y.Z-rc.N"
v=${BASH_REMATCH[1]}

commit=$(git rev-parse -q --verify "refs/tags/$tag^{commit}") || fail "tag $tag does not exist"
sdk=$(git rev-parse -q --verify "refs/tags/sdk/go/$tag^{commit}") || fail "tag sdk/go/$tag does not exist"
[[ $sdk == "$commit" ]] || fail "sdk/go/$tag and $tag point at different commits ($sdk, $commit)"
git merge-base --is-ancestor "$commit" "$branch" || fail "$tag is not on main ($branch)"

ts=$(git show "$commit:sdk/ts/package.json" | sed -n 's/^  "version": "\(.*\)",$/\1/p')
[[ $ts == "$v" ]] || fail "sdk/ts/package.json version is '$ts', want $v"

chart=$(git show "$commit:deploy/k8s/tasklane/Chart.yaml")
field() { sed -n "s/^$1: *\"\{0,1\}\([^\"]*\)\"\{0,1\} *\$/\1/p" <<<"$chart"; }
cv=$(field version); av=$(field appVersion)
[[ $cv == "$v" ]] || fail "chart version is '$cv', want $v"
[[ $av == "$v" ]] || fail "chart appVersion is '$av', want $v"

body=$(git show "$commit:CHANGELOG.md" | awk -v h="## $v" '$0 == h {on = 1; next} on && /^## / {exit} on {print}')
[[ -n ${body//[[:space:]]/} ]] || fail "CHANGELOG.md has no '## $v' section, or it is empty"
printf '%s\n' "$body" > "$notes"
echo "release-check: $tag ok (commit ${commit:0:12}, version $v)"
