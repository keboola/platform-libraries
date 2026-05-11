# Migration: Azure Pipelines → GitHub Actions

**Status:** Design (awaiting approval)
**Date:** 2026-05-11
**Repo:** `keboola/platform-libraries` (monorepo of 22 PHP libraries)

## Goals

- Replace Azure Pipelines with GitHub Actions while modernizing where it's a clear win.
- Introduce dependency-aware test selection (today's pipeline misses cross-library regressions).
- Preserve current publish behavior: every successful branch CI splits affected libraries to their standalone `keboola/<lib>` repos; tag pushes mirror tags to those repos.

## Non-goals

- Restructuring the monorepo or libraries themselves.
- Migrating local dev workflows (Docker Compose stays as-is).
- Introducing image registries for CI base images (continue with buildx `type=gha` cache).

## High-level architecture

### Triggers

| Event | Azure Pipelines today | GitHub Actions (proposed) |
|---|---|---|
| Push to any branch | `azure-pipelines.yml`: build → conditional tests → wait-all → split affected libs | `ci.yml`: dep-aware detect → tests (matrix + complex libs) → split affected libs |
| Push of `<lib>/*` tag | `azure-pipelines.tags.yml`: split the matching library | `tag-publish.yml`: split the matching library |
| Pull request | `pr: none` — never runs | No `pull_request` trigger — checks come from the branch push |

### File layout

```
Azure (today)                              GitHub Actions (proposed)
─────────────────────────────────────      ─────────────────────────────────────
azure-pipelines.yml                        .github/workflows/
azure-pipelines.tags.yml                   ├── ci.yml                  ← branch-push dispatcher
azure-pipelines/                           ├── tag-publish.yml         ← tag-push splitter
├── jobs/                                  ├── _lib-common.yml         ← reusable, ~19 libs
│   ├── lock.yml                           ├── _lib-input-mapping.yml  ← reusable, multi-backend
│   ├── run-tests.yml                      ├── _lib-output-mapping.yml ← reusable, writer matrix
│   └── split-library.yml                  ├── _lib-k8s-client.yml     ← reusable + terraform
└── steps/                                 └── _split-library.yml      ← reusable subtree-split
    ├── push-acr.yml          (unused)     ci/
    ├── push-ecr.yml          (unused)     ├── deps.json               ← static forward-dep graph
    ├── push-production-registry.yml       ├── lib-meta.json           ← php-version, service, vars/secrets per lib
    ├── push-testing-registry.yml          ├── publish-targets.json    ← lib → standalone-repo mapping
    └── restore-docker-artifacts.yml       └── affected-libs.sh        ← graph + diff → matrix
libs/<lib>/azure-pipelines.tests.yml       bin/
bin/                                       ├── split-repo.sh           ← unchanged
├── ci-find-changes.sh                     ├── adopt-repo.sh           ← unchanged
├── split-repo.sh                          └── migrate-secrets.sh      ← one-time, template only
└── adopt-repo.sh
[libs/<lib>/azure-pipelines.tests.yml deleted; per-lib logic moves into _lib-*.yml]
[bin/ci-find-changes.sh deleted]
```

### Behavioral differences vs. today

| Area | Azure today | GHA proposed | Net effect |
|---|---|---|---|
| Image build | One job builds 4 base images + 22 retags, saves `docker-images.tar`, every test job downloads + `docker load`s | Each test job runs `docker buildx` with `cache-from=type=gha`; no central build, no artifact transport | Removes serial bottleneck; warm jobs save ~30–60s/each |
| Lock semantics | `lockBehavior: sequential` + ACR-login mutex on `input-mapping` / `output-mapping` | `concurrency: { group: tests-<lib>-<ref>, cancel-in-progress: false }` per library | Same serialization, no ACR dependency |
| Test selection | Direct path match per lib; empty matches → run all; main → run all | Path match + transitive dependents via `ci/deps.json`; same rule on main and branches | Catches cross-library regressions; main no longer rebuilds the world |
| Test-pass gate | `testsResults` stage waits for all `tests_*` stages (Succeeded or Skipped) | `split` job `needs:` all triggered test jobs; `if: !failure() && !cancelled()` | Equivalent gate, native GHA primitive |
| Cross-repo push auth | Azure DevOps GitHub service connection (`endpoint: keboola`) | GitHub App installation token, scoped `contents: write` on `keboola/*` | Auditable, short-lived tokens, no human PAT |
| Secrets | One flat list of Azure variable group entries | Repo-level Actions Variables (non-secret) + Secrets (sensitive), names preserved | Same flat model; values copied 1:1 |
| Unused registry-push templates | `push-acr.yml`, `push-ecr.yml`, etc. exist but no library calls them | Deleted | −5 files of dead code |
| Runner | Azure-hosted `ubuntu-latest` | GitHub-hosted `ubuntu-latest` | Same |

### `ci.yml` job DAG

```
                  ┌──► tests-common (matrix over simple affected libs)            ─┐
                  │      └─ uses _lib-common.yml                                   │
detect ──────────►┼──► tests-input-mapping  (if affected, uses _lib-input-mapping) ┤
(emits matrix +   │                                                                ├──► split (matrix over affected libs,
 per-complex-lib  ├──► tests-output-mapping (if affected, uses _lib-output-mapping)│      uses _split-library.yml)
 booleans)        │                                                                │
                  └──► tests-k8s-client     (if affected, uses _lib-k8s-client)   ─┘
```

`detect` outputs:
- `common-matrix`: JSON array of affected libraries served by `_lib-common.yml`
- `has-input-mapping`, `has-output-mapping`, `has-k8s-client`: booleans
- `all-affected`: JSON array of all affected libraries (input to the `split` matrix)

Concurrency groups are declared *inside each reusable workflow* (so `tests-input-mapping` queues across branches, but `tests-common[settle]` runs in parallel with `tests-input-mapping`).

## Change detection

### `ci/deps.json` — forward dependency graph

Each entry lists the libraries **this library depends on** (matches `composer.json` direction). The script inverts the graph at runtime.

```json
{
  "libraries": {
    "api-bundle":                       { "depends-on": ["permission-checker", "service-client"] },
    "azure-api-client":                 { "depends-on": [] },
    "configuration-variables-resolver": { "depends-on": ["service-client", "vault-api-client"] },
    "doctrine-retry-bundle":            { "depends-on": [] },
    "git-service-api-client":           { "depends-on": [] },
    "input-mapping":                    { "depends-on": ["key-generator", "settle", "staging-provider"] },
    "k8s-client":                       { "depends-on": [] },
    "key-generator":                    { "depends-on": [] },
    "logging-bundle":                   { "depends-on": [] },
    "messenger-bundle":                 { "depends-on": [] },
    "output-mapping":                   { "depends-on": ["input-mapping", "key-generator", "php-storage-names-sanitizer", "slicer", "staging-provider"] },
    "permission-checker":               { "depends-on": [] },
    "php-storage-names-sanitizer":      { "depends-on": [] },
    "php-test-utils":                   { "depends-on": ["service-client"] },
    "query-service-api-client":         { "depends-on": [] },
    "sandboxes-service-api-client":     { "depends-on": [] },
    "service-client":                   { "depends-on": [] },
    "settle":                           { "depends-on": [] },
    "slicer":                           { "depends-on": [] },
    "staging-provider":                 { "depends-on": ["key-generator"] },
    "sync-actions-api-php-client":      { "depends-on": [] },
    "vault-api-client":                 { "depends-on": [] }
  },
  "complex": ["input-mapping", "output-mapping", "k8s-client"]
}
```

`complex` lists libraries that don't ride `_lib-common.yml` — they need their own bespoke reusable workflow.

### `ci/affected-libs.sh` — algorithm

```
1. BASE = merge-base(origin/main, HEAD)
2. CHANGED_PATHS = git diff --name-only $BASE HEAD
3. DIRECT = { lib : any path in CHANGED_PATHS matches libs/<lib>/** }
4. Invert deps.json into a reverse map in jq:
     reverse[Y] = { X : Y ∈ depends-on[X] }
5. BFS from DIRECT over `reverse` to compute the transitive set:
     AFFECTED = DIRECT ∪ closure(DIRECT, reverse)
6. Partition AFFECTED into:
     - common-matrix = AFFECTED \ complex
     - per-complex flags: has-input-mapping, has-output-mapping, has-k8s-client
7. Emit to $GITHUB_OUTPUT:
     - common-matrix=<json array>
     - all-affected=<json array>
     - has-input-mapping=true|false
     - has-output-mapping=true|false
     - has-k8s-client=true|false
```

Implementation: bash + `jq` (preinstalled on `ubuntu-latest`). ~80 lines.

### Lint (drift detection)

`ci/affected-libs.sh --lint` parses every `libs/*/composer.json`, extracts `keboola/*` deps that match a local library, compares against `ci/deps.json`, fails the workflow with a clear message if drift is detected. Runs in the `detect` job before computing the matrix.

### Worked example

Change `libs/key-generator/src/Foo.php`:
- DIRECT = `{key-generator}`
- Inverted graph for `key-generator`: depended on by `{input-mapping, output-mapping, staging-provider}`
- BFS: also visits `output-mapping` (already in the set via direct dependents)
- AFFECTED = `{key-generator, input-mapping, output-mapping, staging-provider}`
- common-matrix = `["key-generator", "staging-provider"]`
- has-input-mapping = `true`, has-output-mapping = `true`, has-k8s-client = `false`
- Result: 2 common matrix jobs + 2 bespoke workflows; 18 other libraries skipped.

Today's pipeline would only run `tests_keyGenerator` and miss regressions in the three dependents.

## Build & test execution

### Per-job buildx cache layer

Every test job starts with:

```yaml
- uses: actions/checkout@v4
- uses: docker/setup-buildx-action@v3
- name: Warm buildx cache for required image
  uses: docker/build-push-action@v6
  with:
    context: .
    file: ./Dockerfile
    build-args: |
      PHP_VERSION=${{ inputs.php-version }}
    tags: keboola/php-dev${{ inputs.php-version-tag }}:ci
    load: true
    cache-from: type=gha,scope=php-dev${{ inputs.php-version-tag }}
    cache-to:   type=gha,scope=php-dev${{ inputs.php-version-tag }},mode=max
- name: Retag for docker-compose
  run: docker tag keboola/php-dev${{ inputs.php-version-tag }}:ci keboola/php-dev${{ inputs.php-version-tag }}
```

One cache scope per PHP version (`php-dev81`–`php-dev84`). First job on a cold cache pays ~3–5 min; warm cache hits in seconds.

### `_lib-common.yml` (handles 19 simple libraries)

```yaml
name: lib-common
on:
  workflow_call:
    inputs:
      library:       { required: true, type: string }
      php-version:   { required: true, type: string }
      service-name:  { required: true, type: string }
      test-command:
        type: string
        default: "composer install && composer ci"

concurrency:
  group: tests-${{ inputs.library }}-${{ github.ref }}
  cancel-in-progress: false

jobs:
  tests:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - name: Warm buildx cache (php-dev${{ inputs.php-version }})
        uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ inputs.php-version }}
          tags: ci-base:${{ inputs.php-version }}
          load: true
          cache-from: type=gha,scope=php-dev${{ inputs.php-version }}
          cache-to:   type=gha,scope=php-dev${{ inputs.php-version }},mode=max
      - name: Retag for compose
        run: |
          PV=$(echo ${{ inputs.php-version }} | tr -d '.')
          docker tag ci-base:${{ inputs.php-version }} keboola/php-dev${PV}
      - name: Run tests
        run: docker compose run --rm ${{ inputs.service-name }} bash -c "${{ inputs.test-command }}"
      - name: Show logs on failure
        if: failure()
        run: docker compose logs
```

### `ci/lib-meta.json` — per-library config consumed by the dispatcher

```json
{
  "api-bundle":                       { "php": "8.3", "service": "dev-api-bundle" },
  "azure-api-client":                 { "php": "8.1", "service": "dev-azure-api-client" },
  "configuration-variables-resolver": { "php": "8.2", "service": "dev-configuration-variables-resolver" },
  "doctrine-retry-bundle":            { "php": "8.4", "service": "dev-doctrine-retry-bundle" },
  "git-service-api-client":           { "php": "8.2", "service": "dev-git-service-api-client" },
  "key-generator":                    { "php": "8.2", "service": "dev-key-generator" },
  "logging-bundle":                   { "php": "8.1", "service": "dev-logging-bundle" },
  "messenger-bundle":                 { "php": "8.2", "service": "dev-messenger-bundle" },
  "permission-checker":               { "php": "8.1", "service": "dev-permission-checker" },
  "php-storage-names-sanitizer":      { "php": "8.2", "service": "dev-php-storage-names-sanitizer" },
  "php-test-utils":                   { "php": "8.4", "service": "dev-php-test-utils" },
  "query-service-api-client":         { "php": "8.4", "service": "dev-query-service-api-client" },
  "sandboxes-service-api-client":     { "php": "8.2", "service": "dev-sandboxes-service-api-client" },
  "service-client":                   { "php": "8.3", "service": "dev-service-client" },
  "settle":                           { "php": "8.1", "service": "dev-settle" },
  "slicer":                           { "php": "8.1", "service": "dev-slicer" },
  "staging-provider":                 { "php": "8.2", "service": "dev-staging-provider" },
  "sync-actions-api-php-client":      { "php": "8.4", "service": "dev-sync-actions-api-php-client" },
  "vault-api-client":                 { "php": "8.2", "service": "dev-vault-api-client" }
}
```

(PHP versions sourced from the YAML-anchor merge in `docker-compose.yml`.)

### Bespoke workflows

- **`_lib-input-mapping.yml`**: 10 jobs (one `cs` gate + 9 backend/testsuite combos). Each backend job sets `STORAGE_API_URL` from a repo variable and `STORAGE_API_TOKEN*` from secrets. Workflow-level `concurrency: tests-input-mapping-<ref>` serializes across branches.
- **`_lib-output-mapping.yml`**: 7 jobs (`cs`, `general`, `main-writer-1`, `main-writer-2`, `workspace-writer`, `native-types`, `new-native-types`, `slice`). Each uses a project-specific `OUTPUT_MAPPING_*__STORAGE_API_TOKEN_AWS`. Workflow-level `concurrency: tests-output-mapping-<ref>`.
- **`_lib-k8s-client.yml`**: 2 jobs (`tests-aws`, `tests-azure`). Each: install Terraform → `terraform init` → `update-env.sh aws|azure` → run tests. No serialization (ephemeral clusters per run).

## Publishing (library split)

### `_split-library.yml` (reusable)

```yaml
name: split-library
on:
  workflow_call:
    inputs:
      library:     { required: true, type: string }
      target-repo: { required: true, type: string }
      tag-prefix:  { required: true, type: string }

concurrency:
  group: split-${{ inputs.library }}
  cancel-in-progress: false

jobs:
  split:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - name: Generate GitHub App installation token
        id: app-token
        uses: actions/create-github-app-token@v1
        with:
          app-id:       ${{ vars.LIBS_PUBLISHER_APP_ID }}
          private-key:  ${{ secrets.LIBS_PUBLISHER_APP_PRIVATE_KEY }}
          owner:        keboola
          repositories: ${{ inputs.library }}
      - uses: actions/checkout@v4
        with: { fetch-depth: 0, fetch-tags: true }
      - run: sudo apt-get update -q && sudo apt-get install -y git-filter-repo
      - name: Split + push
        env:
          TARGET_REPO_URL: https://x-access-token:${{ steps.app-token.outputs.token }}@github.com/${{ inputs.target-repo }}.git
        run: ./bin/split-repo.sh "$PWD" "$TARGET_REPO_URL" "libs/${{ inputs.library }}" "${{ inputs.tag-prefix }}"
```

`bin/split-repo.sh` reused **unchanged**.

### `ci/publish-targets.json` — library → standalone-repo mapping

Encodes the few mismatches between library name and target repo name (e.g., `key-generator` → `keboola/php-key-generator`, `git-service-api-client` → `keboola/git-service-php-api-client`). Full mapping in design conversation; mirrors today's per-library `targetRepo:` parameters.

### `tag-publish.yml`

```yaml
name: tag-publish
on:
  push:
    tags: ['*/*']
jobs:
  resolve:
    runs-on: ubuntu-latest
    outputs:
      library: ${{ steps.parse.outputs.library }}
      target:  ${{ steps.parse.outputs.target }}
      prefix:  ${{ steps.parse.outputs.prefix }}
    steps:
      - uses: actions/checkout@v4
      - id: parse
        run: |
          TAG="${GITHUB_REF_NAME}"; LIB="${TAG%%/*}"
          META=$(jq -r --arg lib "$LIB" '.[$lib]' ci/publish-targets.json)
          echo "library=$LIB" >> "$GITHUB_OUTPUT"
          echo "target=$(echo "$META" | jq -r '.target')" >> "$GITHUB_OUTPUT"
          echo "prefix=$(echo "$META" | jq -r '.tag_prefix')" >> "$GITHUB_OUTPUT"
  split:
    needs: resolve
    uses: ./.github/workflows/_split-library.yml
    with:
      library:     ${{ needs.resolve.outputs.library }}
      target-repo: ${{ needs.resolve.outputs.target }}
      tag-prefix:  ${{ needs.resolve.outputs.prefix }}
    secrets: inherit
```

### Branch-push split (in `ci.yml`)

```yaml
split:
  needs: [detect, tests-common, tests-input-mapping, tests-output-mapping, tests-k8s-client]
  if: |
    always()
    && needs.detect.outputs.all-affected != '[]'
    && !contains(needs.*.result, 'failure')
    && !contains(needs.*.result, 'cancelled')
  strategy:
    fail-fast: false
    matrix:
      library: ${{ fromJSON(needs.detect.outputs.all-affected) }}
  uses: ./.github/workflows/_split-library.yml
  with:
    library:     ${{ matrix.library }}
    target-repo: ${{ fromJSON(needs.detect.outputs.publish-targets)[matrix.library].target }}
    tag-prefix:  ${{ fromJSON(needs.detect.outputs.publish-targets)[matrix.library].tag_prefix }}
  secrets: inherit
```

(The `detect` job reads `ci/publish-targets.json` and emits it as a `publish-targets` output; matrix `with:` then looks up each library's target.)

### GitHub App setup (one-time)

1. Keboola org → Developer settings → GitHub Apps → New
   - Name: `keboola-libs-publisher`
   - Permissions: `Contents: Read & write`, `Metadata: Read-only`
   - Install on all 22 `keboola/<lib>` standalone repos
2. Generate private key → store as `LIBS_PUBLISHER_APP_PRIVATE_KEY` (secret)
3. Store App ID as `LIBS_PUBLISHER_APP_ID` (variable)

## Secrets & variables inventory

### Repository Variables (non-secret)

| Name | Source |
|---|---|
| `STORAGE_API_URL_AWS` | Azure var |
| `STORAGE_API_URL_AZURE` | Azure var |
| `OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL` | Azure var |
| `LIBS_PUBLISHER_APP_ID` | New (GitHub App) |

### Repository Secrets

| Name | Used by |
|---|---|
| `INPUT_MAPPING__STORAGE_API_TOKEN_AWS` | `_lib-input-mapping.yml` |
| `INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS` | `_lib-input-mapping.yml` |
| `INPUT_MAPPING__STORAGE_API_TOKEN_AZURE` | `_lib-input-mapping.yml` |
| `INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE` | `_lib-input-mapping.yml` |
| `INPUT_MAPPING__STORAGE_API_TOKEN_AWS_BQ` | `_lib-input-mapping.yml` |
| `OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING__BIGQUERY_STORAGE_API_TOKEN` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING_NATIVE_TYPES__STORAGE_API_TOKEN_AWS` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING_NEW_NATIVE_TYPES__STORAGE_API_TOKEN_AWS` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_AWS` | `_lib-output-mapping.yml` |
| `OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_MASTER_AWS` | `_lib-output-mapping.yml` |
| `K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID` | `_lib-k8s-client.yml` |
| `K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY` | `_lib-k8s-client.yml` |
| `LIBS_PUBLISHER_APP_PRIVATE_KEY` | `_split-library.yml` |

Names preserved 1:1 from Azure so no library code needs updating.

### Migration mechanics

`bin/migrate-secrets.sh` — template only, run once by a human with `gh` CLI auth during cutover. Not checked in with real values.

## Rollout

Big-bang cutover, single PR.

### Pre-PR (manual, one-time)

1. Create GitHub App `keboola-libs-publisher`; install on all 22 `keboola/*` library repos
2. Add `LIBS_PUBLISHER_APP_ID` (var) and `LIBS_PUBLISHER_APP_PRIVATE_KEY` (secret) to `platform-libraries`
3. Run `bin/migrate-secrets.sh` (template form) to copy ~15 variables/secrets from Azure to GitHub

### PR contents

- **Add:** `.github/workflows/{ci,tag-publish,_lib-common,_lib-input-mapping,_lib-output-mapping,_lib-k8s-client,_split-library}.yml`
- **Add:** `ci/{deps.json,lib-meta.json,publish-targets.json,affected-libs.sh}`
- **Delete:** `azure-pipelines.yml`, `azure-pipelines.tags.yml`, `azure-pipelines/` (entire dir)
- **Delete:** `libs/*/azure-pipelines.tests.yml` (22 files)
- **Delete:** `bin/ci-find-changes.sh`
- **Keep:** `bin/split-repo.sh`, `bin/adopt-repo.sh`, `Dockerfile`, `docker-compose.yml`
- **Update:** `README.md` / `CLAUDE.md` references

### Validation strategy

- The dispatcher runs on the PR branch's pushes — first validation is the PR itself
- The PR should touch at least one library directly to exercise dep-aware detection
- Smoke-test `_split-library.yml` against a sandbox repo (`keboola/api-bundle-ci-test` or similar) via `workflow_dispatch` before flipping to the real targets
- Tag flow validated by pushing `api-bundle/v0.0.0-test` on a feature branch first

### Cut-over

Merge PR to `main`. Azure config is deleted on `main`, so its pipeline no longer triggers. GHA workflows take over immediately.

### Post-cutover monitoring (1–2 weeks)

- Watch `Actions` tab for failures
- Compare tag-to-standalone-repo cadence with prior baseline
- Spot-check Composer `dev-main` resolution from a downstream service

### Rollback

Revert the PR. Azure files come back; Azure pipeline resumes automatically.

## Open follow-ups (not in scope)

- Auto-generate `ci/deps.json` from `composer.json` files (current: hand-maintained + drift lint)
- Path-filter triggers per library (`paths: ['libs/<lib>/**']`) as a future optimization
- Consider GHCR-pushed base images (option B) if the per-job buildx warm-up cost becomes painful
- Tear down unused Azure DevOps pipeline definitions after confidence period
