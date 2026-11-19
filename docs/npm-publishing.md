# Publishing the TypeScript SDK to npm

How npm publishing works and what this repository does about it. The package is `@mubtasim91/tasklane`; it is published at v1.0 (M5e).

## What npm is

npm is a public store of tarballs. `npm publish` packs files from the package folder into one `.tgz` and uploads it under a name and a version; `npm install @mubtasim91/tasklane` downloads that tarball into `node_modules/`.

- **A published version never changes.** `1.0.0` stays `1.0.0`; a fix is `1.0.1`.
- **Unpublishing is limited**: within 72 hours, and only if nothing depends on it. After that a version can only be deprecated (a warning on install). Check before publishing.
- **A scope is owned.** Only the `mubtasim91` account can publish under `@mubtasim91/`. (`@mubtasim` belongs to an old npm account with no working email, so it can't be recovered.)

## Versions (semantic versioning)

`MAJOR.MINOR.PATCH`:

| Change | Bump | Example |
|---|---|---|
| Bug fix, nothing breaks | PATCH | 1.0.0 → 1.0.1 |
| New feature, old code still works | MINOR | 1.0.1 → 1.1.0 |
| Breaking change (renamed function, removed option) | MAJOR | 1.1.0 → 2.0.0 |

Users depend on `^1.0.0`, "any 1.x.x", so a breaking change in a MINOR breaks their builds without warning. `0.x` means "anything may change", which is why the package stays at `0.0.0` until v1.0.

## What goes into the package

Only what users run:

```
@mubtasim91/tasklane-1.0.0.tgz   (target: under ~100 KB)
├── package.json
├── README.md       shown on npmjs.com
├── LICENSE         Apache-2.0
├── CHANGELOG.md    what changed per version
└── dist/
    ├── index.js     compiled JavaScript
    ├── index.d.ts   type definitions
    └── *.js.map     source maps
```

Not included: `src/`, tests, examples, `.env`, CI files, `node_modules`. `"files": ["dist", "CHANGELOG.md"]` in `package.json` is an allowlist (safer than `.npmignore`); `scripts/check-package.sh` fails if anything else is in the tarball.

## The package.json fields that matter

| Field | Why |
|---|---|
| `"type": "module"` | ESM only. |
| `exports` | The only entry points users may import; internal files can change without a major version. |
| `files` | The allowlist above. |
| `engines` | `node >= 22`. |
| `dependencies` vs `peerDependencies` | Dependencies install with the package. A peer dependency must already be in the app: `@opentelemetry/api` is an optional peer, because two copies would break tracing. |
| `sideEffects: false` | Bundlers may drop unused code. |
| `publishConfig.access: "public"` | Scoped packages are private (paid) by default; without it the first publish fails. |
| `publishConfig.provenance: true` | See below. |

## Publishing from CI, not a laptop

- **Trusted publishing (OIDC).** On npmjs.com the package is linked to this repository and its release workflow; GitHub Actions proves its identity to npm on each release, so no npm token is stored anywhere to leak. npm restricts tokens that bypass 2FA for direct publishing from January 2027, so this is the path that keeps working.
- **Provenance.** The package page shows which commit and workflow built the tarball, so users can check it came from the public code.
- **2FA** on the npm account.
- **Release flow (M5e):** tag `ts-v1.0.0` → the workflow runs the tests, builds, runs the checks below and publishes → GitHub release notes.

## Checks before every publish

| Check | Catches |
|---|---|
| `npm pack --dry-run` | Missing or leaking files, size |
| `publint` | `package.json` mistakes (wrong `exports` paths, missing `types`) |
| `@arethetypeswrong/cli` | Types that resolve wrongly for TypeScript users |
| Install the tarball into an empty project and import it | "Works here, broken when installed" |

`npm run check:package` runs all four; CI runs it on every pull request, and `prepublishOnly` runs it before any publish.
