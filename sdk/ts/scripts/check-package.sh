#!/usr/bin/env bash
# Checks the package as a user gets it (design §21.5): build, lint the
# manifest, check the types resolve, pack, look at what is inside, install the
# tarball into an empty project and import it.
set -euo pipefail
cd "$(dirname "$0")/.."

npm run build
npx --no-install publint --strict
npx --no-install attw --pack . --profile esm-only

tarball=$(npm pack --silent)
trap 'rm -f "$tarball"' EXIT
echo "packed $tarball ($(du -k "$tarball" | cut -f1) KiB)"

# Only dist/, the manifest, README, LICENSE and CHANGELOG may ship.
unexpected=$(tar -tzf "$tarball" | grep -Ev '^package/(dist/.+|package\.json|README\.md|LICENSE|CHANGELOG\.md)$' || true)
if [[ -n "$unexpected" ]]; then
  echo "unexpected files in the tarball:" >&2
  echo "$unexpected" >&2
  exit 1
fi

project=$(mktemp -d)
trap 'rm -f "$tarball"; rm -rf "$project"' EXIT
cp "$tarball" "$project/"
(
  cd "$project"
  npm init -y >/dev/null
  npm install --no-audit --no-fund "./$tarball" >/dev/null
  node --input-type=module -e '
    const m = await import("@mubtasim91/tasklane");
    const names = Object.keys(m).sort();
    if (names.length === 0) { console.error("no exports"); process.exit(1); }
    console.log("import ok:", names.join(", "));
  '
)
