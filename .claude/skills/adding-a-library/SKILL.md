---
name: adding-a-library
description: Use when adding a new library/package to the platform-libraries monorepo (a new libs/<name> directory) and wiring it into Docker, CI and the publish/release pipeline.
---

# Adding a library to platform-libraries

## Overview

A library is a self-contained composer package under `libs/<name>/` that is tested in the monorepo and published (split) to a standalone read-only repo `keboola/<repo>`. Adding one means creating the package **and** wiring it into Docker Compose, the CI test fan-out, the `tests-result` barrier, and (if its repo name differs) the publish action. Miss a wiring step and the library is silently never tested or never published.

`<name>` = the directory under `libs/`. `<repo>` = the standalone GitHub repo (usually equal to `<name>`).

## Steps

### 1. Create `libs/<name>/`
Mirror an existing simple library (e.g. `libs/settle`, `libs/key-generator`):
- `composer.json` — `"name": "keboola/<pkg>"`, PSR-4 `autoload`/`autoload-dev`, the standard `scripts` (`ci` = `composer validate --strict` + `build`; `build` = `phpcs` + `phpstan` + `tests`), and `"config": { "lock": false, "sort-packages": true, "allow-plugins": {...} }`. **`lock: false` is mandatory** (monorepo never commits `composer.lock`).
- `phpcs.xml` (ruleset `keboola/coding-standard`), `phpstan.neon` (level max), `phpunit.xml.dist`, `README.md`, `.gitignore`, `src/`, `tests/`.
- **If it depends on other monorepo libraries**, add the path repository and use `*@dev`:
  ```json
  "repositories": { "libs": { "type": "path", "url": "../../libs/*" } },
  "require": { "keboola/<other-lib>": "*@dev" }
  ```
  Only `*@dev` path deps are seen by change detection (see Gotchas).

### 2. Add the Docker Compose service
In `docker-compose.yml`, add a `dev-<name>` service merging the right PHP base anchor (`*dev81`–`*dev84`, pick the PHP version the lib targets):
```yaml
  dev-<name>:
    <<: *dev82
    image: keboola/<name>
    working_dir: /code/libs/<name>
    # environment: [STORAGE_API_TOKEN, STORAGE_API_URL, ...]   # only if tests need them
    # depends_on: [mockserver]                                  # only if needed
```

### 3. Add the CI test workflow
Create `.github/workflows/lib-<name>.yml` (copy `lib-settle.yml` for the simple case):
- `on: workflow_call: {}`
- `concurrency: { group: <name>-lock, cancel-in-progress: false }`
- a `tests` job: Checkout → Set up Docker Buildx → `docker compose build dev-<name>` → `docker compose run --rm dev-<name> bash -c 'composer install && composer ci'` → show logs on failure.
- pass any required env from `secrets.*` / `vars.*` (see root `CLAUDE.md` for the variables-vs-secrets split). Add new secrets/variables there + in the repo settings.

### 4. Wire it into `ci.yml` (two edits)
- Add a test job in the fan-out (these stay explicit — a reusable-workflow `uses:` cannot be matrixed):
  ```yaml
    <name>:
      needs: [detect-changes]
      if: contains(needs.detect-changes.outputs.libs, '"<name>"')
      uses: ./.github/workflows/lib-<name>.yml
      secrets: inherit
  ```
- Add `<name>` to the `tests-result` job's `needs:` list (the required status check). **Easy to forget — without it the barrier ignores this library.**

### 5. Publish wiring (only if repo name ≠ dir name)
Publish/release run automatically via the `publish` matrix and `release.yml` once the library is detected — no per-lib job needed. **Only** if the standalone repo name differs from `<name>`, add a `case` entry in `.github/actions/split-library/action.yml` (like `key-generator → php-key-generator`).

Also add the **standalone repo name** to the `matrix.repo` list in `.github/workflows/cleanup-branch.yml` (the `on: delete` workflow that prunes deleted branches from standalone repos), otherwise deleted branches won't be cleaned up there.

### 6. Create the standalone repo + grant the publish App access
1. Create the standalone repo `keboola/<repo>` (or run `bin/adopt-repo.sh`).
2. **Add `<repo>` to the publish GitHub App's installation.** In the `keboola` org → Settings → GitHub Apps → the publish App (the one behind `SPLIT_APP_ID`) → *Configure* → **Repository access**: either keep "All repositories", or under "Only select repositories" **add `<repo>`**. If the App is not installed on `<repo>`, the publish step fails — `actions/create-github-app-token` mints the token with `repositories: <repo>` (scoped to that one repo) and cannot issue a token for a repo outside its installation.
3. **Ensure the App has `Contents: Read and write`** permission (set once at the App level → *Permissions*; it applies to every installed repo). Without write, the publish `git push` is rejected. (Note: the App must also be allowed to push to any protected branch it needs to update in the target repo.)

This is **not in code** — it is a one-time admin action in GitHub org settings. Without it the first publish/release for the new library fails at authentication.

### 7. Docs
Update `libs/<name>/README.md` (required env vars) and, if it adds a new env var, the lists in root `CLAUDE.md` / `README.md`.

## Verify
```bash
docker compose run --rm dev-<name> bash -c 'composer install && composer ci'   # lib passes
docker compose run --rm dev82 bash -c 'cd bin/ci && composer ci'               # CI tooling still green
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest               # workflows valid
```
Sanity-check detection picks it up: edit a file under `libs/<name>/`, then
`php bin/ci/affected-libraries.php <base-sha>` should include `"<name>"`.

## Quick reference — files to touch

| File | Always? |
|------|---------|
| `libs/<name>/**` (package) | yes |
| `docker-compose.yml` (`dev-<name>` service) | yes |
| `.github/workflows/lib-<name>.yml` | yes |
| `ci.yml` — test job **and** `tests-result` `needs:` | yes (both) |
| `.github/actions/split-library/action.yml` `case` | only if repo name ≠ dir |
| `.github/workflows/cleanup-branch.yml` `matrix.repo` (standalone repo name) | yes |
| repo `CLAUDE.md`/`README.md`, repo secrets/vars | if new env vars |
| standalone repo + App install | yes (for publishing) |

## Gotchas
- **Change detection only follows `*@dev` path deps.** A versioned dependency on another lib (e.g. `^2.0`) will NOT make this lib test when that dep changes. If you need that, the dependency must be `*@dev`.
- **Forgetting the `tests-result` `needs:` entry** means the required check passes even if this lib's tests fail. Add both edits in step 4.
- **PHP base image** must match the lib's supported version (`dev81`–`dev84`); the image name is `keboola/<name>` but the PHP comes from the merged `*devNN` anchor.
- Run quality checks and `actionlint` before committing; commit message scope is the library name (e.g. `feat(<name>): ...`).
