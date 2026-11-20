#!/usr/bin/env bash
# Checks the package as a user gets it (design §21.5): build, lint the
# manifest, check the types resolve, pack, look at what is inside, install the
# tarball into an empty project and import it.
set -euo pipefail
cd "$(dirname "$0")/.."
sdk=$(pwd)

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
tar -tzf "$tarball" | grep -qx 'package/CHANGELOG.md' || { echo "CHANGELOG.md missing from the tarball" >&2; exit 1; }

project=$(mktemp -d)
trap 'rm -f "$tarball"; rm -rf "$project"' EXIT
cp "$tarball" "$project/"
(
  cd "$project"
  npm init -y >/dev/null
  npm pkg set type=module >/dev/null
  npm install --no-audit --no-fund "./$tarball" >/dev/null
  node --input-type=module -e '
    const m = await import("@mubtasim91/tasklane");
    const names = Object.keys(m).sort();
    if (names.length === 0) { console.error("no exports"); process.exit(1); }
    console.log("import ok:", names.join(", "));
  '

  # The published .d.ts files must typecheck for a strict consumer with
  # skipLibCheck off: a declaration that mentions a type stripInternal removed
  # fails here (TS2304). Uses the SDK's own typescript and @types/node.
  mkdir -p node_modules/@types
  ln -s "$sdk/node_modules/@types/node" node_modules/@types/node
  cat > consumer.ts <<'TS'
import {
  createClient,
  Priority,
  QueueFullError,
  retryAt,
  TasklaneError,
  Worker,
  Client,
  type LeasedJob,
  type Result,
} from "@mubtasim91/tasklane";

const client = createClient("http://localhost:7070");
const worker = new Worker(client, "emails", {
  handlers: {
    send: async (job: LeasedJob, signal: AbortSignal) => {
      if (signal.aborted) throw retryAt(new Error("later"), new Date());
      void job.payload;
    },
  },
  onResult: (r: Result) => void r.outcome,
});
await client.enqueue("emails", { kind: "send", payload: {}, priority: Priority.High });
// createClient is the only way to make a Client: a public constructor would typecheck and then crash.
// @ts-expect-error the constructor is private
new Client();
void [worker, QueueFullError, TasklaneError];
TS
  node "$sdk/node_modules/typescript/bin/tsc" --noEmit --strict --skipLibCheck false \
    --target es2022 --module nodenext --moduleResolution nodenext --types node consumer.ts
  echo "consumer typecheck ok"
)
