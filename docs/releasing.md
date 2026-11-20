# Releasing Tasklane

How Tasklane versions are made public, and why it works this way. The design is in [design.md §22](design.md#22-m5e-v100-release); the workflow is [`.github/workflows/release.yml`](../.github/workflows/release.yml).

## 1. Deploying an app vs releasing software

Two different jobs that look alike:

- **Deploying a service you run.** You merge to `dev`, `staging` or `main`, and a pipeline deploys that branch. There is one live copy; each deploy replaces the last. Users never pick a version.
- **Releasing software other people install.** Nobody uses "the" Tasklane: each team downloads a version and runs it themselves. Many versions live side by side (one team on 1.0.0, another on 1.2.0), and each one must stay downloadable, unchanged, for years.

A website versus a book: you can edit a website any time, and everyone sees the new page. A printed book comes out in editions; once the 1st edition is sold you can't change those copies, you print a 2nd. **A release tag is the edition label.**

## 2. Git tags

A **branch** is a pointer that moves: every commit on `main` moves `main` forward. A **tag** is a label on one commit that never moves.

```
commits:  A──B──C──D──E──F──G   ← main (moves with each commit)
                ▲           ▲
             v1.0.0      v1.1.0  (stay where they were put)
```

```sh
git tag -a v1.0.0 -m "Tasklane 1.0.0"   # annotated tag: who, when, a message
git push origin v1.0.0                  # tags are only pushed on purpose
git checkout v1.0.0                     # the exact code of that release, years later
git tag -d v1.0.0                       # delete a local tag (never a pushed release tag)
```

Releases use **annotated** tags (`-a`). Development still happens with branches and pull requests into `main`; a tag only marks "this commit on main is release X".

Tasklane pushes **two tags on the same commit**: `vX.Y.Z` (the release) and `sdk/go/vX.Y.Z` (the Go SDK, see §4). Push them together with `--atomic`, so GitHub gets both or neither:

```sh
git push --atomic origin v1.0.0 sdk/go/v1.0.0
```

## 3. Version numbers (semantic versioning)

`MAJOR.MINOR.PATCH`, e.g. `1.4.2`:

| Bump | When | Example | Users upgrade without changing their code? |
|---|---|---|---|
| PATCH 1.4.2 → 1.4.3 | bug fix only | heartbeat timing fix | yes |
| MINOR 1.4 → 1.5.0 | new feature, nothing removed | rate limits | yes |
| MAJOR 1 → 2.0.0 | something breaks | rename an RPC field | **no** |

- **1.0.0 is a promise:** no breaking change until 2.0. Before 1.0 (0.x) anything may change. `buf breaking` in CI guards the API.
- **Release candidates** (`1.0.0-rc.1`, `-rc.2`, …) test the whole publish pipeline before the real version. npm installs them only when asked (`@next`); Docker gets no `latest` tag for them. One catch: npm marks the very first version of a new package as `latest`, even with `--tag next`. So `1.0.0-rc.1`, published by hand as the first version (§9), is `latest` until `1.0.0` is out.
- **One version for everything:** a release `X.Y.Z` is server image `X.Y.Z`, Go SDK `vX.Y.Z`, npm `X.Y.Z` and chart `X.Y.Z` (design §22.1, decision 1).

## 4. Where each part lives

| Part | Registry | Install | What gets uploaded |
|---|---|---|---|
| Server image | GHCR (GitHub Container Registry): [`ghcr.io/mubtasim/tasklane`](https://github.com/Mubtasim/tasklane/pkgs/container/tasklane) | `docker pull ghcr.io/mubtasim/tasklane:1.0.0` | the image (linux/amd64 + linux/arm64) |
| Helm chart | GHCR: `oci://ghcr.io/mubtasim/charts/tasklane` | `helm install tasklane oci://ghcr.io/mubtasim/charts/tasklane --version 1.0.0` | the packaged chart |
| TypeScript SDK | npm: [`@mubtasim91/tasklane`](https://www.npmjs.com/package/@mubtasim91/tasklane) | `npm install @mubtasim91/tasklane` | the built package (`dist/`) |
| Go SDK | **nothing to upload: the git repo is the registry** | `go get github.com/mubtasim/tasklane/sdk/go@v1.0.0` | only the tag |
| C# SDK (after v1.0) | NuGet (nuget.org) | `dotnet add package <name>` | the `.nupkg` |
| Release notes | [GitHub Releases](https://github.com/Mubtasim/tasklane/releases) | read | CHANGELOG section + install commands |

**GHCR** is Docker Hub built into GitHub: images and charts live under the GitHub account, free for public packages, and the workflow logs in with GitHub's built-in token. A newly pushed package starts **private**; make it public once in the package settings.

**Go has no upload step.** When someone runs `go get …@v1.0.0`:

1. Go asks **proxy.golang.org**, Google's module cache. On first request it fetches the repo at that tag and **keeps the copy forever**, even if the tag or the repo is later deleted.
2. **sum.golang.org** records a checksum of that code, so nobody can quietly change it later.
3. **pkg.go.dev** shows the SDK's documentation.

In a repo with several Go modules, Go finds a module's versions by tags that start with the module's folder: the SDK in `sdk/go/` is versioned by `sdk/go/vX.Y.Z`. That is why every release has two tags.

**Docker tags are not git tags.** Git tags never move; image tags may. Each release pushes `X.Y.Z` (never moved) plus `X.Y`, `X` and `latest`, which move to the newest matching release. An image's **digest** (`sha256:…`) is its permanent ID.

The workflow marks every non-pre-release as `latest`. That is right while versions only go up. A backport (say `1.0.2` after `1.1.0` is out) would move `latest` and `1` backwards. Don't release a backport this way without first changing the `flavor` setting in `release.yml`. This does not matter for 1.0.0.

## 5. What can never change

| Registry | Change a published version? |
|---|---|
| npm | No. Unpublish is possible within 72 h, but **that version number can never be used again** |
| Go module proxy | No. Cached forever, even after the tag is deleted |
| NuGet | No. You can only unlist (hide) it |
| GHCR | `X.Y.Z` is never moved by the workflow; `latest` moves by design |

So the human check comes **before** anything is published: the workflow waits for approval (§7).

## 6. Provenance

A sealed label saying where a package came from and how it was made, like a factory seal: "made at factory X, batch Y". The label (a signed [SLSA](https://slsa.dev) statement) says:

- repo `github.com/Mubtasim/tasklane`, commit `<sha>`, the exact code;
- built by `.github/workflows/release.yml`, run `<n>`, on GitHub's machines;
- the checksum of what was built.

How it is signed, with no secret key stored anywhere:

```
workflow runs ─▶ GitHub gives it a short-lived identity (OIDC: "I am release.yml in Mubtasim/tasklane")
             ─▶ Sigstore signs the label with a key that lives ~10 minutes
             ─▶ the signature goes into a public log anyone can search (Rekor)
             ─▶ npm / GHCR store the label next to the package
```

- **Protects against:** someone who steals the npm password and publishes a malicious version from their laptop: it has no label (or one pointing elsewhere). With "disallow tokens" on the npm package, they can't publish at all.
- **Does not prove:** that the code is safe or bug-free. Only that the package was built from that public commit by that workflow, so anyone can read exactly that code.
- **Check it:** npm shows a "Provenance" badge linking to the commit and the run; `npm audit signatures` verifies installed packages; for the image: `gh attestation verify oci://ghcr.io/mubtasim/tasklane:1.0.0 --owner Mubtasim`.
- `1.0.0-rc.1` on npm has no provenance: it was published by hand (§9), and a laptop has no workflow identity to sign with.

## 7. Making a release

0. **Check the approval gate (once, before the first tag).** The environment `release` must exist and must list you as a required reviewer (§9, step 0). If it is missing, GitHub creates it with no protection when the tag is pushed, and the publish job then runs without asking you. Check: `gh api repos/Mubtasim/tasklane/environments/release --jq '.protection_rules'` must show a `required_reviewers` rule.
1. **One pull request** that sets the version in `sdk/ts/package.json` (`cd sdk/ts && npm version X.Y.Z --no-git-tag-version`, which also updates `package-lock.json`), `deploy/k8s/tasklane/Chart.yaml` (`version` and `appVersion`), and adds a `## X.Y.Z` section to `CHANGELOG.md`. Merge it.
2. Tag the merge commit on `main` and push both tags:
   ```sh
   git checkout main && git pull
   git tag -a vX.Y.Z -m "Tasklane X.Y.Z"
   git tag -a sdk/go/vX.Y.Z -m "Tasklane Go SDK X.Y.Z"
   git push --atomic origin vX.Y.Z sdk/go/vX.Y.Z
   ```
3. The **Release** workflow runs:
   ```
   check ─▶ tests (ci.yml + TS integration) ─▶ ⏸ approval ─▶ publish ─▶ confirm
   ```
   - **check** — `scripts/release-check.sh`: the tag is on `main`, both tags are on one commit, the versions in package.json and Chart.yaml match, the CHANGELOG has the section (it becomes the release notes).
   - **tests** — the normal CI on the tagged commit. A failure stops everything; nothing is published.
   - **approval** — the publish job runs in the GitHub environment `release`, which needs the maintainer's review: open the run and click *Review deployments → Approve*.
   - **publish** — image (amd64 + arm64, provenance attestation) → npm (trusted publishing, provenance; `--tag next` for release candidates) → chart → GitHub Release.
   - **confirm** — what a stranger gets, without logging in: `docker pull` + `tasklane -version`, the Go SDK through proxy.golang.org, `npm view`, `helm show chart`.

## 8. When a release fails halfway

- **Re-run the failed job.** Every publish step can run again: the image step skips a version that is already pushed (a rebuild would give a different image under the same tag); npm skips a version that is already there; the chart push skips an existing version; the GitHub Release is edited, not duplicated.
- **Confirm fails on the first release** because new GHCR packages are private: make `tasklane` and `charts/tasklane` public, then re-run `confirm`.
- **Never move or reuse a tag.** If a release is wrong, fix it in the next patch version (1.0.1). A Go version, once fetched by the proxy, and an npm version number are permanent anyway.
- **A tag pushed by mistake** fails `check` (or waits at approval): reject the deployment, then delete the tag (`git push origin :refs/tags/vX.Y.Z`) before anything was published.

## 9. First release only (done once)

npm can't set up trusted publishing for a package that doesn't exist yet, so:

0. **Create the approval gate first.** The publish job runs in the GitHub environment `release`. If that environment doesn't exist when a tag is pushed, GitHub creates it with no rules, and publishing runs with nobody's approval. So create it now: repo Settings → Environments → New environment `release`, then:
   - **Required reviewers:** add yourself.
   - **Prevent self-review:** leave OFF. You are the only maintainer, and you must be able to approve your own release.
   - **Deployment branches and tags:** choose "Selected branches and tags" and add the tag rule `v*`, so only release tags can use it.

   Or with the API (the second call adds the tag rule; replace `<your-user-id>` with the number from `gh api user --jq .id`):
   ```sh
   echo '{"reviewers":[{"type":"User","id":<your-user-id>}],"prevent_self_review":false,"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}' \
     | gh api -X PUT repos/Mubtasim/tasklane/environments/release --input -
   gh api -X POST repos/Mubtasim/tasklane/environments/release/deployment-branch-policies -f name='v*' -f type=tag
   ```
1. The maintainer publishes `1.0.0-rc.1` by hand: `npm login` (2FA), then `cd sdk/ts && npm ci && npm publish --tag next --provenance=false`. Note: npm sets `latest` on the first publish of a new package even with `--tag next`, so `rc.1` is `latest` until `1.0.0` is out. That is expected.
2. npmjs.com → package → Settings → **Trusted publisher**: GitHub Actions, `Mubtasim/tasklane`, workflow `release.yml`, environment `release`. Then *Require two-factor authentication and disallow tokens*.
3. Within two days (npm [expires a trusted-publisher setup that goes unused](https://docs.npmjs.com/trusted-publishers)), release `1.0.0-rc.2` through the workflow.
4. Make the new GHCR packages public; re-run `confirm`.

## 10. When to give each part its own version

Tasklane uses one version for everything (design §22.1). Switch to separate versions (tags like `sdk/ts/vX.Y.Z`, one workflow per part) when:

- SDK-only releases outnumber server releases;
- users complain that a new version changed nothing for them;
- one SDK needs a breaking 2.0 while the server stays 1.x.

Switching later only changes the tag scheme: every published package already has its own version field, so nothing is renamed or withdrawn.
