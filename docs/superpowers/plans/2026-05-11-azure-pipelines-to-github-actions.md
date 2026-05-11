# Azure Pipelines → GitHub Actions Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Azure Pipelines with GitHub Actions while introducing dependency-aware test selection. Mirror today's publish behavior (split affected libraries to standalone repos on every branch push; mirror tags to those repos on tag push), but use GHA-native concurrency, secrets, and a GitHub App for cross-repo auth.

**Architecture:** A change-detection bash script (`ci/affected-libs.sh`) reads a hand-maintained forward dependency graph (`ci/deps.json`) plus `git diff` to emit a job matrix. The main dispatcher (`ci.yml`) calls per-library reusable workflows: `_lib-common.yml` (parameterized, handles 19 simple libraries) plus three bespoke ones (`_lib-input-mapping.yml`, `_lib-output-mapping.yml`, `_lib-k8s-client.yml`). After tests pass, the dispatcher calls `_split-library.yml` to push affected libraries to their standalone `keboola/<lib>` repos using a GitHub App installation token. Tag pushes go through `tag-publish.yml` directly to `_split-library.yml` (no tests).

**Tech Stack:** GitHub Actions, bash + jq (preinstalled on `ubuntu-latest`), docker buildx (`type=gha` cache), `bats-core` for bash unit tests, `actionlint` for workflow validation, `actions/create-github-app-token@v1` for cross-repo auth.

**Reference spec:** `docs/superpowers/specs/2026-05-11-azure-pipelines-to-github-actions-design.md`

---

## File structure

**New files:**

```
.github/workflows/
├── ci.yml                       ← main dispatcher, push to branches
├── tag-publish.yml              ← tag-push splitter
├── _lib-common.yml              ← reusable, parameterized for 19 libs
├── _lib-input-mapping.yml       ← bespoke
├── _lib-output-mapping.yml      ← bespoke
├── _lib-k8s-client.yml          ← bespoke
├── _split-library.yml           ← reusable
└── test-ci-tooling.yml          ← runs bats tests on ci/ changes

ci/
├── deps.json                    ← forward dep graph (hand-maintained)
├── lib-meta.json                ← per-lib php-version + service name
├── publish-targets.json         ← sparse override map for repo name mismatches
└── affected-libs.sh             ← change detection + lint + matrix emission

tests/ci/
├── test_affected_libs.bats      ← bats tests for affected-libs.sh
└── fixtures/
    ├── deps-sample.json         ← test fixture
    └── composer-fixtures/       ← per-lib composer.json samples

bin/
└── migrate-secrets.sh           ← one-time human-run template
```

**Files deleted at cutover:**
- `azure-pipelines.yml`
- `azure-pipelines.tags.yml`
- `azure-pipelines/` (entire directory)
- `libs/*/azure-pipelines.tests.yml` (22 files)
- `bin/ci-find-changes.sh`

**Files unchanged:** `bin/split-repo.sh`, `bin/adopt-repo.sh`, `Dockerfile`, `docker-compose.yml`.

---

## Task 1: Bootstrap CI directory with config files

**Files:**
- Create: `ci/deps.json`
- Create: `ci/lib-meta.json`
- Create: `ci/publish-targets.json`

- [ ] **Step 1: Create `ci/deps.json`**

Write the file with this exact content:

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

- [ ] **Step 2: Validate JSON syntax**

Run: `jq empty ci/deps.json`
Expected: no output, exit 0.

- [ ] **Step 3: Create `ci/lib-meta.json`**

PHP versions sourced from `docker-compose.yml` YAML-anchor merges:

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

Note: only the 19 "simple" libraries appear here. The 3 complex libraries (`input-mapping`, `output-mapping`, `k8s-client`) handle PHP version and service name in their own bespoke workflows.

- [ ] **Step 4: Validate JSON syntax**

Run: `jq empty ci/lib-meta.json`
Expected: no output, exit 0.

- [ ] **Step 5: Create `ci/publish-targets.json` (sparse override map)**

```json
{
  "git-service-api-client":       "keboola/git-service-php-api-client",
  "key-generator":                "keboola/php-key-generator",
  "query-service-api-client":     "keboola/query-service-api-php-client",
  "sandboxes-service-api-client": "keboola/sandboxes-service-api-php-client",
  "vault-api-client":             "keboola/vault-api-php-client"
}
```

- [ ] **Step 6: Validate JSON syntax**

Run: `jq empty ci/publish-targets.json`
Expected: no output, exit 0.

- [ ] **Step 7: Cross-check `lib-meta.json` against `deps.json`**

Run:
```bash
jq -r '.libraries | keys[]' ci/deps.json | sort > /tmp/deps-libs.txt
jq -r '. + (input | reduce (.complex[]) as $c (.; .[$c] = null)) | keys[]' \
   ci/lib-meta.json ci/deps.json 2>/dev/null
# Simpler check: lib-meta has exactly (deps.libraries.keys − deps.complex)
diff <(jq -r '.libraries | keys[]' ci/deps.json | sort) \
     <(jq -r '(.libraries | keys) - .complex | sort | .[]' ci/deps.json | cat ci/lib-meta.json - | jq -rs '.[0] | keys[]') 2>/dev/null || true

# Final assertion:
expected=$(jq -r '(.libraries | keys) - .complex | sort | .[]' ci/deps.json | tr '\n' ' ')
actual=$(jq -r 'keys | sort | .[]' ci/lib-meta.json | tr '\n' ' ')
if [ "$expected" != "$actual" ]; then
  echo "MISMATCH:"; echo "  expected: $expected"; echo "  actual:   $actual"; exit 1
fi
echo "OK: lib-meta covers exactly the non-complex libraries"
```

Expected: prints `OK: lib-meta covers exactly the non-complex libraries`.

- [ ] **Step 8: Commit**

```bash
git add ci/deps.json ci/lib-meta.json ci/publish-targets.json
git commit -m "ci: add dependency graph and library metadata for GHA migration"
```

---

## Task 2: Implement `ci/affected-libs.sh` — diff + direct path detection

We're building up the script in small TDD slices. First slice: given a git diff, identify which libraries are *directly* changed.

**Files:**
- Create: `ci/affected-libs.sh`
- Create: `tests/ci/test_affected_libs.bats`
- Create: `tests/ci/fixtures/deps-sample.json`

- [ ] **Step 1: Install `bats-core` locally (one-time engineer setup)**

If `bats` is not already installed:
```bash
# Ubuntu / Debian
sudo apt-get install -y bats
# macOS
brew install bats-core
# verify
bats --version
```
Expected: prints a version like `Bats 1.x.x`.

- [ ] **Step 2: Create `tests/ci/fixtures/deps-sample.json`**

Minimal graph for tests, decoupled from the real `ci/deps.json`:

```json
{
  "libraries": {
    "alpha": { "depends-on": [] },
    "beta":  { "depends-on": ["alpha"] },
    "gamma": { "depends-on": ["beta"] },
    "delta": { "depends-on": [] },
    "epsilon": { "depends-on": ["alpha", "delta"] }
  },
  "complex": ["gamma"]
}
```

- [ ] **Step 3: Write the first failing test**

Create `tests/ci/test_affected_libs.bats`:

```bash
#!/usr/bin/env bats

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../.." && pwd)"
  SCRIPT="$REPO_ROOT/ci/affected-libs.sh"
  FIXTURES="$REPO_ROOT/tests/ci/fixtures"
  TMP="$(mktemp -d)"
  cd "$TMP"
  git init -q -b main
  git config user.email "test@example.com"
  git config user.name  "Test"
  # Seed a base commit on main with all five libs
  for lib in alpha beta gamma delta epsilon; do
    mkdir -p "libs/$lib"
    echo "// $lib v1" > "libs/$lib/file.php"
  done
  git add -A
  git commit -qm "initial"
  git checkout -qb feature
}

teardown() {
  rm -rf "$TMP"
}

@test "direct: single library change" {
  echo "// alpha v2" > libs/alpha/file.php
  git commit -qam "edit alpha"
  run "$SCRIPT" --direct --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "alpha" ]
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: fails because `ci/affected-libs.sh` doesn't exist yet.

- [ ] **Step 5: Create minimal `ci/affected-libs.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: affected-libs.sh <mode> [options]

Modes:
  --direct       Print directly-changed libraries (one per line)
  --affected     Print all affected libraries (direct + transitive dependents)
  --emit         Emit GitHub Actions outputs to \$GITHUB_OUTPUT
  --lint         Verify deps.json against composer.json files

Options:
  --deps <path>  Path to deps.json (default: ci/deps.json)
  --base <ref>   Base ref to diff against (default: origin/main)
EOF
}

MODE=""
DEPS_FILE="ci/deps.json"
BASE_REF="origin/main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --direct|--affected|--emit|--lint) MODE="$1"; shift ;;
    --deps) DEPS_FILE="$2"; shift 2 ;;
    --base) BASE_REF="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -z "$MODE" ]] && { usage >&2; exit 1; }

# Compute the diff base
BASE=$(git merge-base "$BASE_REF" HEAD 2>/dev/null || git rev-parse "$BASE_REF")

# DIRECT = set of libraries with paths in the diff matching libs/<lib>/**
direct() {
  git diff --name-only "$BASE" HEAD \
    | awk -F/ '$1=="libs" && NF>=2 { print $2 }' \
    | sort -u
}

case "$MODE" in
  --direct) direct ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
```

- [ ] **Step 6: Make it executable**

Run: `chmod +x ci/affected-libs.sh`

- [ ] **Step 7: Run the test to verify it passes**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: 1 test passes.

- [ ] **Step 8: Add tests for edge cases**

Append to `tests/ci/test_affected_libs.bats`:

```bash
@test "direct: multiple library changes" {
  echo "// alpha v2" > libs/alpha/file.php
  echo "// beta v2"  > libs/beta/file.php
  git commit -qam "edit two"
  run "$SCRIPT" --direct --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "$(printf 'alpha\nbeta')" ]
}

@test "direct: nested path is recognized" {
  mkdir -p libs/alpha/src/foo
  echo "// nested" > libs/alpha/src/foo/bar.php
  git add libs/alpha/src
  git commit -qm "nested edit"
  run "$SCRIPT" --direct --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "alpha" ]
}

@test "direct: root-file changes are ignored" {
  echo "ROOT" > Dockerfile
  git add Dockerfile
  git commit -qm "root change"
  run "$SCRIPT" --direct --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}

@test "direct: no changes returns empty" {
  run "$SCRIPT" --direct --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}
```

- [ ] **Step 9: Run tests to verify all pass**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: 4 tests pass.

- [ ] **Step 10: Commit**

```bash
git add ci/affected-libs.sh tests/ci/
git commit -m "ci: add affected-libs.sh with direct-change detection"
```

---

## Task 3: Implement `--affected` mode (transitive closure via inverted BFS)

**Files:**
- Modify: `ci/affected-libs.sh`
- Modify: `tests/ci/test_affected_libs.bats`

- [ ] **Step 1: Write the failing test for direct-only (no dependents)**

Append to `tests/ci/test_affected_libs.bats`:

```bash
@test "affected: change without dependents = only the changed lib" {
  echo "// gamma v2" > libs/gamma/file.php
  git commit -qam "edit gamma"
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "gamma" ]
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: the new test fails with "Mode --affected not implemented yet" (exit 2).

- [ ] **Step 3: Implement `--affected` mode**

Replace the `case "$MODE"` block at the bottom of `ci/affected-libs.sh` with:

```bash
# AFFECTED = DIRECT ∪ transitive dependents (via inverted depends-on graph)
affected() {
  local direct_libs
  direct_libs=$(direct)
  [[ -z "$direct_libs" ]] && return 0

  # Build reverse graph as JSON: { Y: [X1, X2, ...] } where each Xi has Y in its depends-on list
  local direct_json
  direct_json=$(jq -Rsc 'split("\n") | map(select(length > 0))' <<<"$direct_libs")

  jq -r \
    --argjson direct "$direct_json" \
    '
      # Compute reverse: { Y: [X for X in libraries if Y in X.depends-on] }
      (
        .libraries
        | to_entries
        | reduce .[] as $e ({};
            reduce $e.value["depends-on"][] as $d (.;
              .[$d] = ((.[$d] // []) + [$e.key])
            )
          )
      ) as $reverse
      # BFS from $direct over $reverse
      | reduce range(0; 100) as $_ (
          { frontier: $direct, visited: ($direct | unique) };
          if (.frontier | length) == 0 then .
          else
            .frontier as $f
            | ([ $f[] | ($reverse[.] // [])[] ] | unique - .visited) as $next
            | { frontier: $next, visited: (.visited + $next | unique) }
          end
        )
      | .visited
      | sort
      | .[]
    ' "$DEPS_FILE"
}

case "$MODE" in
  --direct) direct ;;
  --affected) affected ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
```

- [ ] **Step 4: Run to verify the new test passes**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: all tests pass.

- [ ] **Step 5: Add tests for transitive closure**

Append to `tests/ci/test_affected_libs.bats`:

```bash
@test "affected: change to alpha pulls in beta, gamma, epsilon" {
  echo "// alpha v2" > libs/alpha/file.php
  git commit -qam "edit alpha"
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  # beta depends on alpha; gamma depends on beta; epsilon depends on alpha
  [ "$output" = "$(printf 'alpha\nbeta\nepsilon\ngamma')" ]
}

@test "affected: change to delta pulls in epsilon only" {
  echo "// delta v2" > libs/delta/file.php
  git commit -qam "edit delta"
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "$(printf 'delta\nepsilon')" ]
}

@test "affected: multiple direct changes union correctly" {
  echo "// alpha" > libs/alpha/file.php
  echo "// delta" > libs/delta/file.php
  git commit -qam "edit two"
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "$(printf 'alpha\nbeta\ndelta\nepsilon\ngamma')" ]
}

@test "affected: no changes returns empty" {
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "" ]
}
```

- [ ] **Step 6: Run to verify all pass**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: 8 tests pass.

- [ ] **Step 7: Sanity-check against real `ci/deps.json` from the repo**

Run from repo root (a one-off manual check, not part of automated tests):
```bash
# Simulate a change to key-generator and check
cd "$(mktemp -d)" && git clone "$OLDPWD" repo && cd repo && git checkout -b smoke
echo "// edit" > libs/key-generator/src/check.php
git add -A && git commit -qm "smoke"
./ci/affected-libs.sh --affected --base main
```
Expected output (sorted):
```
input-mapping
key-generator
output-mapping
staging-provider
```

- [ ] **Step 8: Commit**

```bash
git add ci/affected-libs.sh tests/ci/test_affected_libs.bats
git commit -m "ci: add transitive closure to affected-libs.sh"
```

---

## Task 4: Implement `--emit` mode (GitHub Actions outputs)

**Files:**
- Modify: `ci/affected-libs.sh`
- Modify: `tests/ci/test_affected_libs.bats`

- [ ] **Step 1: Write the failing test**

Append to `tests/ci/test_affected_libs.bats`:

```bash
@test "emit: writes GHA outputs for affected libs" {
  echo "// alpha" > libs/alpha/file.php
  git commit -qam "edit alpha"
  export GITHUB_OUTPUT="$TMP/gh-output"
  : > "$GITHUB_OUTPUT"
  run "$SCRIPT" --emit --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]

  # Verify each output key=value line
  grep -q '^all-affected=\["alpha","beta","epsilon","gamma"\]$' "$GITHUB_OUTPUT"
  # common-matrix excludes "gamma" (it's in complex)
  grep -q '^common-matrix=\["alpha","beta","epsilon"\]$' "$GITHUB_OUTPUT"
  # No complex libs from sample fixture map to has-input-mapping etc. — only "gamma" is complex here.
  grep -q '^has-gamma=true$' "$GITHUB_OUTPUT"
  # publish-targets is emitted as JSON object
  grep -q '^publish-targets={.*"alpha":"keboola/alpha".*}$' "$GITHUB_OUTPUT"
}

@test "emit: empty affected set still writes valid outputs" {
  export GITHUB_OUTPUT="$TMP/gh-output"
  : > "$GITHUB_OUTPUT"
  run "$SCRIPT" --emit --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  grep -q '^all-affected=\[\]$' "$GITHUB_OUTPUT"
  grep -q '^common-matrix=\[\]$' "$GITHUB_OUTPUT"
  grep -q '^has-gamma=false$' "$GITHUB_OUTPUT"
  grep -q '^publish-targets={}$' "$GITHUB_OUTPUT"
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: the two new tests fail with exit 2 ("Mode --emit not implemented yet").

- [ ] **Step 3: Implement `--emit` mode**

Add an `emit()` function to `ci/affected-libs.sh` (before the `case` block):

```bash
# Path to publish-targets.json (sparse override map). Defaults to repo-relative.
PUBLISH_TARGETS_FILE="${PUBLISH_TARGETS_FILE:-ci/publish-targets.json}"

emit() {
  local affected_libs
  affected_libs=$(affected)

  # Convert affected list to JSON array
  local all_json
  all_json=$(jq -Rsc 'split("\n") | map(select(length > 0))' <<<"$affected_libs")

  # Partition: common-matrix = affected \ complex
  local common_json
  common_json=$(jq -c --argjson all "$all_json" '
    . as $deps
    | $all - $deps.complex
  ' "$DEPS_FILE")

  # publish-targets: map every affected lib to its target repo (override or default)
  local targets_json
  if [[ -f "$PUBLISH_TARGETS_FILE" ]]; then
    targets_json=$(jq -c --argjson all "$all_json" --slurpfile o "$PUBLISH_TARGETS_FILE" '
      $all
      | map({ key: ., value: ($o[0][.] // ("keboola/" + .)) })
      | from_entries
    ' <<<'null')
  else
    targets_json=$(jq -c --argjson all "$all_json" '
      $all | map({ key: ., value: ("keboola/" + .) }) | from_entries
    ' <<<'null')
  fi

  # Write GHA outputs
  : "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set in --emit mode}"
  {
    echo "all-affected=$all_json"
    echo "common-matrix=$common_json"
    echo "publish-targets=$targets_json"
    # Per-complex-lib boolean: has-<libname>=true|false
    jq -r --argjson all "$all_json" '
      .complex[] as $c
      | "has-\($c)=\($all | index($c) != null)"
    ' "$DEPS_FILE"
  } >> "$GITHUB_OUTPUT"
}
```

Update the `case` block:
```bash
case "$MODE" in
  --direct) direct ;;
  --affected) affected ;;
  --emit) emit ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
```

- [ ] **Step 4: Run to verify tests pass**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: 10 tests pass.

- [ ] **Step 5: Smoke-check emit against the real `ci/deps.json`**

```bash
cd "$(mktemp -d)" && git clone "$OLDPWD" repo && cd repo && git checkout -b smoke
echo "// edit" > libs/key-generator/src/check.php
git add -A && git commit -qm "smoke"
export GITHUB_OUTPUT=/tmp/gh-out; : > $GITHUB_OUTPUT
./ci/affected-libs.sh --emit --base main
cat $GITHUB_OUTPUT
```

Expected: should contain
```
all-affected=["input-mapping","key-generator","output-mapping","staging-provider"]
common-matrix=["key-generator","staging-provider"]
publish-targets={"input-mapping":"keboola/input-mapping","key-generator":"keboola/php-key-generator","output-mapping":"keboola/output-mapping","staging-provider":"keboola/staging-provider"}
has-input-mapping=true
has-output-mapping=true
has-k8s-client=false
```

- [ ] **Step 6: Commit**

```bash
git add ci/affected-libs.sh tests/ci/test_affected_libs.bats
git commit -m "ci: add --emit mode for GitHub Actions outputs"
```

---

## Task 5: Implement `--lint` mode (drift detection vs. `composer.json`)

**Files:**
- Modify: `ci/affected-libs.sh`
- Modify: `tests/ci/test_affected_libs.bats`
- Create: `tests/ci/fixtures/composer-fixtures/` (per-test composer.json samples)

- [ ] **Step 1: Create fixtures directory**

```bash
mkdir -p tests/ci/fixtures/composer-fixtures
```

- [ ] **Step 2: Write failing tests for lint mode**

Append to `tests/ci/test_affected_libs.bats`:

```bash
# Lint mode helper: stages a libs/ tree with composer.json files in a temp repo
setup_lint_repo() {
  for lib in alpha beta gamma delta epsilon; do
    mkdir -p "libs/$lib"
    cat > "libs/$lib/composer.json" <<EOF
{
  "name": "keboola/$lib",
  "require": {}
}
EOF
  done
}

@test "lint: clean state with no edges passes" {
  setup_lint_repo
  # All libs have empty depends-on in fixture → no edges declared, no edges in composer
  cat > "$TMP/empty-deps.json" <<EOF
{"libraries":{"alpha":{"depends-on":[]},"beta":{"depends-on":[]},"gamma":{"depends-on":[]},"delta":{"depends-on":[]},"epsilon":{"depends-on":[]}},"complex":[]}
EOF
  run "$SCRIPT" --lint --deps "$TMP/empty-deps.json"
  [ "$status" -eq 0 ]
  [[ "$output" == *"OK"* ]]
}

@test "lint: missing edge in deps.json is detected" {
  setup_lint_repo
  # beta requires alpha in composer.json
  cat > libs/beta/composer.json <<EOF
{
  "name": "keboola/beta",
  "require": { "keboola/alpha": "*@dev" }
}
EOF
  cat > "$TMP/missing-edge.json" <<EOF
{"libraries":{"alpha":{"depends-on":[]},"beta":{"depends-on":[]},"gamma":{"depends-on":[]},"delta":{"depends-on":[]},"epsilon":{"depends-on":[]}},"complex":[]}
EOF
  run "$SCRIPT" --lint --deps "$TMP/missing-edge.json"
  [ "$status" -ne 0 ]
  [[ "$output" == *"beta"* ]]
  [[ "$output" == *"alpha"* ]]
}

@test "lint: missing library entry is detected" {
  setup_lint_repo
  # composer-fixtures has lib 'alpha' but deps.json doesn't list it
  cat > "$TMP/missing-lib.json" <<EOF
{"libraries":{"beta":{"depends-on":[]},"gamma":{"depends-on":[]},"delta":{"depends-on":[]},"epsilon":{"depends-on":[]}},"complex":[]}
EOF
  run "$SCRIPT" --lint --deps "$TMP/missing-lib.json"
  [ "$status" -ne 0 ]
  [[ "$output" == *"alpha"* ]]
}

@test "lint: stale edge in deps.json is detected" {
  setup_lint_repo
  # deps.json claims beta depends on alpha but composer.json doesn't
  cat > "$TMP/stale-edge.json" <<EOF
{"libraries":{"alpha":{"depends-on":[]},"beta":{"depends-on":["alpha"]},"gamma":{"depends-on":[]},"delta":{"depends-on":[]},"epsilon":{"depends-on":[]}},"complex":[]}
EOF
  run "$SCRIPT" --lint --deps "$TMP/stale-edge.json"
  [ "$status" -ne 0 ]
  [[ "$output" == *"stale"* || "$output" == *"unused"* || "$output" == *"missing"* ]]
}
```

- [ ] **Step 3: Run tests to confirm they fail**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: the four new lint tests fail (mode not implemented).

- [ ] **Step 4: Implement `--lint`**

Add a `lint()` function to `ci/affected-libs.sh` (before the `case` block):

```bash
lint() {
  # 1. Collect declared libraries from deps.json
  local declared
  declared=$(jq -r '.libraries | keys[]' "$DEPS_FILE" | sort -u)

  # 2. Collect actual libraries from libs/<lib>/composer.json
  local actual
  actual=$(find libs -mindepth 2 -maxdepth 2 -name composer.json -printf '%h\n' \
    | awk -F/ '{print $NF}' | sort -u)

  # 3. Check parity of library lists
  local missing_in_deps
  missing_in_deps=$(comm -23 <(echo "$actual") <(echo "$declared"))
  local missing_in_libs
  missing_in_libs=$(comm -13 <(echo "$actual") <(echo "$declared"))

  local errors=0
  if [[ -n "$missing_in_deps" ]]; then
    echo "ERROR: libraries present on disk but missing from $DEPS_FILE:"
    echo "$missing_in_deps" | sed 's/^/  - /'
    errors=$((errors + 1))
  fi
  if [[ -n "$missing_in_libs" ]]; then
    echo "ERROR: libraries declared in $DEPS_FILE but absent from libs/:"
    echo "$missing_in_libs" | sed 's/^/  - /'
    errors=$((errors + 1))
  fi

  # 4. For each library, compare composer.json keboola/* deps with deps.json
  while IFS= read -r lib; do
    [[ -z "$lib" ]] && continue
    local composer_file="libs/$lib/composer.json"
    [[ ! -f "$composer_file" ]] && continue

    # Extract keboola/<name> deps where <name> matches a declared library
    local actual_edges
    actual_edges=$(jq -r --argjson declared "$(echo "$declared" | jq -Rsc 'split("\n") | map(select(length > 0))')" '
      [(.require // {}), (.["require-dev"] // {})]
      | map(keys) | flatten
      | map(select(startswith("keboola/")))
      | map(sub("^keboola/"; ""))
      | map(select(. as $x | $declared | index($x) != null))
      | unique | sort | .[]
    ' "$composer_file")

    local declared_edges
    declared_edges=$(jq -r --arg lib "$lib" '.libraries[$lib]["depends-on"] | sort | .[]' "$DEPS_FILE")

    local extra_in_composer
    extra_in_composer=$(comm -23 <(echo "$actual_edges") <(echo "$declared_edges"))
    local stale_in_deps
    stale_in_deps=$(comm -13 <(echo "$actual_edges") <(echo "$declared_edges"))

    if [[ -n "$extra_in_composer" ]]; then
      echo "ERROR: $lib depends on libraries not declared in $DEPS_FILE:"
      echo "$extra_in_composer" | sed 's/^/  - missing edge: /'
      errors=$((errors + 1))
    fi
    if [[ -n "$stale_in_deps" ]]; then
      echo "ERROR: $lib has stale/unused edges in $DEPS_FILE:"
      echo "$stale_in_deps" | sed 's/^/  - stale edge: /'
      errors=$((errors + 1))
    fi
  done <<<"$declared"

  if [[ $errors -gt 0 ]]; then
    echo
    echo "Lint failed: $errors issue(s) found"
    return 1
  fi
  echo "OK: deps.json matches composer.json across $(echo "$declared" | wc -l) libraries"
}
```

Update the `case` block:
```bash
case "$MODE" in
  --direct) direct ;;
  --affected) affected ;;
  --emit) emit ;;
  --lint) lint ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
```

- [ ] **Step 5: Run lint tests**

Run: `bats tests/ci/test_affected_libs.bats`
Expected: all 14 tests pass.

- [ ] **Step 6: Run lint against the real `ci/deps.json`**

From repo root:
```bash
./ci/affected-libs.sh --lint
```
Expected: prints `OK: deps.json matches composer.json across 22 libraries`.

If it fails, the printed output tells you exactly which edges to add or remove from `ci/deps.json`. Fix `ci/deps.json` until lint passes; this is expected since we hand-derived the graph.

- [ ] **Step 7: Commit**

```bash
git add ci/affected-libs.sh tests/ci/
git commit -m "ci: add --lint mode for deps.json drift detection"
```

---

## Task 6: Create `_lib-common.yml` reusable workflow

**Files:**
- Create: `.github/workflows/_lib-common.yml`

- [ ] **Step 1: Install `actionlint` for workflow validation**

```bash
# Linux
curl -sLo /tmp/actionlint.tar.gz "https://github.com/rhysd/actionlint/releases/latest/download/actionlint_$(curl -s https://api.github.com/repos/rhysd/actionlint/releases/latest | jq -r .tag_name | sed 's/^v//')_linux_amd64.tar.gz"
tar -xzf /tmp/actionlint.tar.gz -C /tmp actionlint
sudo mv /tmp/actionlint /usr/local/bin/actionlint
actionlint -version
```
Expected: prints a version like `1.x.x`.

- [ ] **Step 2: Create `.github/workflows/_lib-common.yml`**

```yaml
name: lib-common

on:
  workflow_call:
    inputs:
      library:
        description: Library directory name (e.g., "settle")
        required: true
        type: string
      php-version:
        description: PHP version string (e.g., "8.2")
        required: true
        type: string
      service-name:
        description: docker-compose service name (e.g., "dev-settle")
        required: true
        type: string
      test-command:
        description: Shell command run inside the service container
        required: false
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

      - name: Warm buildx cache for php-dev image
        uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: |
            PHP_VERSION=${{ inputs.php-version }}
          tags: ci-base:${{ inputs.php-version }}
          load: true
          cache-from: type=gha,scope=php-dev${{ inputs.php-version }}
          cache-to: type=gha,scope=php-dev${{ inputs.php-version }},mode=max

      - name: Retag base image for docker-compose
        run: |
          PV=$(echo "${{ inputs.php-version }}" | tr -d '.')
          docker tag ci-base:${{ inputs.php-version }} keboola/php-dev${PV}

      - name: Run tests
        run: docker compose run --rm ${{ inputs.service-name }} bash -c "${{ inputs.test-command }}"

      - name: Show logs on failure
        if: failure()
        run: docker compose logs
```

- [ ] **Step 3: Validate the workflow with actionlint**

Run: `actionlint .github/workflows/_lib-common.yml`
Expected: no output, exit 0.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/_lib-common.yml
git commit -m "ci: add reusable workflow for common library test pattern"
```

---

## Task 7: Create `ci.yml` — detect job + tests-common matrix

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches:
      - '**'

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  detect:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    outputs:
      common-matrix:     ${{ steps.emit.outputs.common-matrix }}
      all-affected:      ${{ steps.emit.outputs.all-affected }}
      publish-targets:   ${{ steps.emit.outputs.publish-targets }}
      has-input-mapping: ${{ steps.emit.outputs.has-input-mapping }}
      has-output-mapping: ${{ steps.emit.outputs.has-output-mapping }}
      has-k8s-client:    ${{ steps.emit.outputs.has-k8s-client }}
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Lint deps.json against composer.json
        run: ./ci/affected-libs.sh --lint

      - name: Emit affected-libs outputs
        id: emit
        run: ./ci/affected-libs.sh --emit --base origin/main

      - name: Show detection summary
        run: |
          echo "Affected libraries:  ${{ steps.emit.outputs.all-affected }}"
          echo "Common matrix:       ${{ steps.emit.outputs.common-matrix }}"
          echo "has-input-mapping:   ${{ steps.emit.outputs.has-input-mapping }}"
          echo "has-output-mapping:  ${{ steps.emit.outputs.has-output-mapping }}"
          echo "has-k8s-client:      ${{ steps.emit.outputs.has-k8s-client }}"

  tests-common:
    needs: detect
    if: needs.detect.outputs.common-matrix != '[]'
    strategy:
      fail-fast: false
      matrix:
        library: ${{ fromJSON(needs.detect.outputs.common-matrix) }}
    uses: ./.github/workflows/_lib-common.yml
    with:
      library:      ${{ matrix.library }}
      php-version:  ${{ fromJSON(vars.LIB_META || '{}')[matrix.library].php }}
      service-name: ${{ fromJSON(vars.LIB_META || '{}')[matrix.library].service }}
    secrets: inherit
```

Wait — `vars.LIB_META` would require pre-loading `lib-meta.json` into a repo variable. Cleaner: have `detect` emit per-library metadata.

Use this revised version instead:

```yaml
name: CI

on:
  push:
    branches:
      - '**'

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  detect:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    outputs:
      common-matrix:      ${{ steps.emit.outputs.common-matrix }}
      common-with-meta:   ${{ steps.meta.outputs.common-with-meta }}
      all-affected:       ${{ steps.emit.outputs.all-affected }}
      publish-targets:    ${{ steps.emit.outputs.publish-targets }}
      has-input-mapping:  ${{ steps.emit.outputs.has-input-mapping }}
      has-output-mapping: ${{ steps.emit.outputs.has-output-mapping }}
      has-k8s-client:     ${{ steps.emit.outputs.has-k8s-client }}
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Lint deps.json against composer.json
        run: ./ci/affected-libs.sh --lint

      - name: Emit affected-libs outputs
        id: emit
        run: ./ci/affected-libs.sh --emit --base origin/main

      - name: Build per-library metadata for matrix
        id: meta
        run: |
          MATRIX=$(jq -nc \
            --argjson libs '${{ steps.emit.outputs.common-matrix }}' \
            --slurpfile meta ci/lib-meta.json \
            '$libs | map({ library: ., php: $meta[0][.].php, service: $meta[0][.].service })')
          echo "common-with-meta=$MATRIX" >> "$GITHUB_OUTPUT"

      - name: Show detection summary
        run: |
          echo "Affected libraries:  ${{ steps.emit.outputs.all-affected }}"
          echo "Common matrix:       ${{ steps.meta.outputs.common-with-meta }}"
          echo "has-input-mapping:   ${{ steps.emit.outputs.has-input-mapping }}"
          echo "has-output-mapping:  ${{ steps.emit.outputs.has-output-mapping }}"
          echo "has-k8s-client:      ${{ steps.emit.outputs.has-k8s-client }}"

  tests-common:
    needs: detect
    if: needs.detect.outputs.common-matrix != '[]'
    strategy:
      fail-fast: false
      matrix:
        include: ${{ fromJSON(needs.detect.outputs.common-with-meta) }}
    uses: ./.github/workflows/_lib-common.yml
    with:
      library:      ${{ matrix.library }}
      php-version:  ${{ matrix.php }}
      service-name: ${{ matrix.service }}
    secrets: inherit
```

- [ ] **Step 2: Validate with actionlint**

Run: `actionlint .github/workflows/ci.yml`
Expected: no errors (warnings about `fromJSON` on dynamic strings are acceptable).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add main dispatcher with detect and tests-common matrix"
```

- [ ] **Step 4: Push the branch and observe the first run on GitHub**

```bash
git push -u origin "$(git branch --show-current)"
```

Visit the repo's `Actions` tab and confirm:
- The `CI` workflow appears
- The `detect` job runs and outputs are visible in the run summary
- If no libraries changed in this PR yet (only `ci/` and `.github/` so far), `tests-common` should be skipped (`if:` condition is `common-matrix != '[]'`)

Note: this validation is informational. It's expected that no library tests run on this commit since only CI infrastructure changed.

---

## Task 8: Create `_lib-input-mapping.yml` and wire into `ci.yml`

**Files:**
- Create: `.github/workflows/_lib-input-mapping.yml`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the reusable workflow**

```yaml
name: lib-input-mapping

on:
  workflow_call: {}

concurrency:
  group: tests-input-mapping-${{ github.ref }}
  cancel-in-progress: false

env:
  PHP_VERSION: "8.2"

jobs:
  warm-cache:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - name: Warm buildx cache
        uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
          cache-to: type=gha,scope=php-dev${{ env.PHP_VERSION }},mode=max

  cs:
    needs: warm-cache
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && composer check"
      - if: failure()
        run: docker compose logs

  aws-common-part-1:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && composer paratest -- --testsuite=CommonPart1 -f -p 3"
      - if: failure()
        run: docker compose logs

  aws-testsuite:
    needs: [cs, aws-common-part-1]
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && vendor/bin/phpunit --testsuite Aws"
      - if: failure()
        run: docker compose logs

  aws-common-part-2:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && vendor/bin/phpunit --testsuite CommonPart2"
      - if: failure()
        run: docker compose logs

  aws-common-files:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && composer paratest -- --testsuite=CommonFiles -f -p 3"
      - if: failure()
        run: docker compose logs

  azure-common-part-1:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AZURE }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AZURE }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && composer paratest -- --testsuite=CommonPart1 -f -p 3"
      - if: failure()
        run: docker compose logs

  azure-testsuite:
    needs: [cs, azure-common-part-1]
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AZURE }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AZURE }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && vendor/bin/phpunit --testsuite Azure"
      - if: failure()
        run: docker compose logs

  azure-common-part-2:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AZURE }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AZURE }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && vendor/bin/phpunit --testsuite CommonPart2"
      - if: failure()
        run: docker compose logs

  azure-common-files:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AZURE }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AZURE }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && composer paratest -- --testsuite=CommonFiles -f -p 3"
      - if: failure()
        run: docker compose logs

  bigquery:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS_BQ }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.INPUT_MAPPING__STORAGE_API_TOKEN_AWS_BQ }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-input-mapping bash -c "composer install && vendor/bin/phpunit --testsuite BigQuery"
      - if: failure()
        run: docker compose logs
```

The job names (`aws-common-part-1`, etc.) and their commands mirror `libs/input-mapping/azure-pipelines.tests.yml` 1:1.

- [ ] **Step 2: Validate with actionlint**

Run: `actionlint .github/workflows/_lib-input-mapping.yml`
Expected: no errors.

- [ ] **Step 3: Wire into `ci.yml`**

Insert this job into `.github/workflows/ci.yml` after the `tests-common` job:

```yaml
  tests-input-mapping:
    needs: detect
    if: needs.detect.outputs.has-input-mapping == 'true'
    uses: ./.github/workflows/_lib-input-mapping.yml
    secrets: inherit
```

- [ ] **Step 4: Validate**

Run: `actionlint .github/workflows/ci.yml .github/workflows/_lib-input-mapping.yml`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/_lib-input-mapping.yml .github/workflows/ci.yml
git commit -m "ci: add input-mapping bespoke workflow with multi-backend matrix"
```

---

## Task 9: Create `_lib-output-mapping.yml` and wire into `ci.yml`

**Files:**
- Create: `.github/workflows/_lib-output-mapping.yml`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the reusable workflow**

```yaml
name: lib-output-mapping

on:
  workflow_call: {}

concurrency:
  group: tests-output-mapping-${{ github.ref }}
  cancel-in-progress: false

env:
  PHP_VERSION: "8.2"

jobs:
  cs:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
          cache-to: type=gha,scope=php-dev${{ env.PHP_VERSION }},mode=max
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && composer check"
      - if: failure()
        run: docker compose logs

  general:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:           ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:         ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER:  ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
      BIGQUERY_STORAGE_API_URL:  ${{ vars.OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL }}
      BIGQUERY_STORAGE_API_TOKEN: ${{ secrets.OUTPUT_MAPPING__BIGQUERY_STORAGE_API_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && composer paratests -- --testsuite general-tests -f -p 4"
      - if: failure()
        run: docker compose logs

  main-writer-1:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && composer paratests -- --testsuite main-writer-tests-1 -f"
      - if: failure()
        run: docker compose logs

  main-writer-2:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && vendor/bin/phpunit --testsuite main-writer-tests-2"
      - if: failure()
        run: docker compose logs

  workspace-writer:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && composer paratests -- --testsuite workspace-writer-tests -f"
      - if: failure()
        run: docker compose logs

  native-types:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING_NATIVE_TYPES__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && vendor/bin/phpunit --testsuite native-types"
      - if: failure()
        run: docker compose logs

  new-native-types:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING_NEW_NATIVE_TYPES__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && vendor/bin/phpunit --testsuite new-native-types"
      - if: failure()
        run: docker compose logs

  slice:
    needs: cs
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      STORAGE_API_URL:          ${{ vars.STORAGE_API_URL_AWS }}
      STORAGE_API_TOKEN:        ${{ secrets.OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_AWS }}
      STORAGE_API_TOKEN_MASTER: ${{ secrets.OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_MASTER_AWS }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - run: |
          docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
          docker compose run --rm dev-output-mapping bash -c "composer install && composer paratests -- --testsuite slice -f -p 4"
      - if: failure()
        run: docker compose logs
```

The jobs (`general`, `main-writer-1`, …, `slice`) and their secrets mirror `libs/output-mapping/azure-pipelines.tests.yml` 1:1.

- [ ] **Step 2: Validate**

Run: `actionlint .github/workflows/_lib-output-mapping.yml`
Expected: no errors.

- [ ] **Step 3: Wire into `ci.yml`**

Insert into `.github/workflows/ci.yml` after `tests-input-mapping`:

```yaml
  tests-output-mapping:
    needs: detect
    if: needs.detect.outputs.has-output-mapping == 'true'
    uses: ./.github/workflows/_lib-output-mapping.yml
    secrets: inherit
```

- [ ] **Step 4: Validate and commit**

```bash
actionlint .github/workflows/ci.yml .github/workflows/_lib-output-mapping.yml
git add .github/workflows/_lib-output-mapping.yml .github/workflows/ci.yml
git commit -m "ci: add output-mapping bespoke workflow with writer matrix"
```

---

## Task 10: Create `_lib-k8s-client.yml` and wire into `ci.yml`

**Files:**
- Create: `.github/workflows/_lib-k8s-client.yml`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the reusable workflow**

```yaml
name: lib-k8s-client

on:
  workflow_call: {}

env:
  PHP_VERSION: "8.2"

jobs:
  tests-aws:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      AWS_ACCESS_KEY_ID:     ${{ secrets.K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
          cache-to: type=gha,scope=php-dev${{ env.PHP_VERSION }},mode=max
      - name: Retag base image
        run: docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
      - run: ./libs/k8s-client/provisioning/ci/pipelines-scripts/terraform-install.sh
      - run: ./libs/k8s-client/provisioning/ci/pipelines-scripts/terraform-init.sh
      - run: ./libs/k8s-client/provisioning/ci/update-env.sh -v -e .env.local aws
      - run: docker compose run --rm dev-k8s-client bash -c 'composer install && composer ci'
      - if: failure()
        run: docker compose logs

  tests-azure:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      AWS_ACCESS_KEY_ID:     ${{ secrets.K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY }}
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: ./Dockerfile
          build-args: PHP_VERSION=${{ env.PHP_VERSION }}
          tags: ci-base:${{ env.PHP_VERSION }}
          load: true
          cache-from: type=gha,scope=php-dev${{ env.PHP_VERSION }}
      - name: Retag base image
        run: docker tag ci-base:${{ env.PHP_VERSION }} keboola/php-dev82
      - run: ./libs/k8s-client/provisioning/ci/pipelines-scripts/terraform-install.sh
      - run: ./libs/k8s-client/provisioning/ci/pipelines-scripts/terraform-init.sh
      - run: ./libs/k8s-client/provisioning/ci/update-env.sh -v -e .env.local azure
      - run: docker compose run --rm dev-k8s-client bash -c 'composer install && composer ci'
      - if: failure()
        run: docker compose logs
```

- [ ] **Step 2: Wire into `ci.yml`**

Append after `tests-output-mapping`:

```yaml
  tests-k8s-client:
    needs: detect
    if: needs.detect.outputs.has-k8s-client == 'true'
    uses: ./.github/workflows/_lib-k8s-client.yml
    secrets: inherit
```

- [ ] **Step 3: Validate and commit**

```bash
actionlint .github/workflows/_lib-k8s-client.yml .github/workflows/ci.yml
git add .github/workflows/_lib-k8s-client.yml .github/workflows/ci.yml
git commit -m "ci: add k8s-client bespoke workflow with terraform provisioning"
```

---

## Task 11: Create `_split-library.yml` reusable workflow

**Files:**
- Create: `.github/workflows/_split-library.yml`

- [ ] **Step 1: Create the workflow**

```yaml
name: split-library

on:
  workflow_call:
    inputs:
      library:
        description: Library directory name (e.g., "api-bundle")
        required: true
        type: string
      target-repo:
        description: Full GitHub repo path (e.g., "keboola/api-bundle")
        required: true
        type: string

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
          app-id:        ${{ vars.LIBS_PUBLISHER_APP_ID }}
          private-key:   ${{ secrets.LIBS_PUBLISHER_APP_PRIVATE_KEY }}
          owner:         keboola
          repositories:  ${{ inputs.library }}

      - name: Checkout monorepo (full history + tags)
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          fetch-tags: true

      - name: Install git-filter-repo
        run: |
          sudo apt-get update -q
          sudo apt-get install -y git-filter-repo

      - name: Split and push to standalone repo
        env:
          TARGET_REPO_URL: https://x-access-token:${{ steps.app-token.outputs.token }}@github.com/${{ inputs.target-repo }}.git
        run: ./bin/split-repo.sh "$PWD" "$TARGET_REPO_URL" "libs/${{ inputs.library }}" "${{ inputs.library }}/"
```

`bin/split-repo.sh` is reused unchanged. The tag prefix is always `<library>/`.

- [ ] **Step 2: Validate and commit**

```bash
actionlint .github/workflows/_split-library.yml
git add .github/workflows/_split-library.yml
git commit -m "ci: add reusable workflow for library subtree split"
```

---

## Task 12: Wire `split` job into `ci.yml`

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Append the `split` job to `ci.yml`**

After the `tests-k8s-client` job:

```yaml
  split:
    needs: [detect, tests-common, tests-input-mapping, tests-output-mapping, tests-k8s-client]
    if: |
      always()
      && needs.detect.outputs.all-affected != '[]'
      && needs.detect.result == 'success'
      && (needs.tests-common.result == 'success' || needs.tests-common.result == 'skipped')
      && (needs.tests-input-mapping.result == 'success' || needs.tests-input-mapping.result == 'skipped')
      && (needs.tests-output-mapping.result == 'success' || needs.tests-output-mapping.result == 'skipped')
      && (needs.tests-k8s-client.result == 'success' || needs.tests-k8s-client.result == 'skipped')
    strategy:
      fail-fast: false
      matrix:
        library: ${{ fromJSON(needs.detect.outputs.all-affected) }}
    uses: ./.github/workflows/_split-library.yml
    with:
      library:     ${{ matrix.library }}
      target-repo: ${{ fromJSON(needs.detect.outputs.publish-targets)[matrix.library] }}
    secrets: inherit
```

The `if:` condition mirrors today's `testsResults` stage gate: pass if every test job either succeeded or was skipped (no library affected → job skipped → still allowed).

- [ ] **Step 2: Validate and commit**

```bash
actionlint .github/workflows/ci.yml
git add .github/workflows/ci.yml
git commit -m "ci: wire split job into dispatcher gated on test outcomes"
```

---

## Task 13: Create `tag-publish.yml`

**Files:**
- Create: `.github/workflows/tag-publish.yml`

- [ ] **Step 1: Create the workflow**

```yaml
name: tag-publish

on:
  push:
    tags:
      - '*/*'

jobs:
  resolve:
    runs-on: ubuntu-latest
    timeout-minutes: 2
    outputs:
      library: ${{ steps.parse.outputs.library }}
      target:  ${{ steps.parse.outputs.target }}
    steps:
      - uses: actions/checkout@v4
      - id: parse
        run: |
          TAG="${GITHUB_REF_NAME}"
          LIB="${TAG%%/*}"
          if [[ -z "$LIB" || "$LIB" == "$TAG" ]]; then
            echo "Tag '$TAG' does not match '<library>/<version>' shape; skipping." >&2
            exit 1
          fi
          TARGET=$(jq -r --arg lib "$LIB" '.[$lib] // ("keboola/" + $lib)' ci/publish-targets.json)
          echo "library=$LIB"   >> "$GITHUB_OUTPUT"
          echo "target=$TARGET" >> "$GITHUB_OUTPUT"
          echo "Resolved: library=$LIB target=$TARGET"

  split:
    needs: resolve
    uses: ./.github/workflows/_split-library.yml
    with:
      library:     ${{ needs.resolve.outputs.library }}
      target-repo: ${{ needs.resolve.outputs.target }}
    secrets: inherit
```

- [ ] **Step 2: Validate and commit**

```bash
actionlint .github/workflows/tag-publish.yml
git add .github/workflows/tag-publish.yml
git commit -m "ci: add tag-publish workflow for mirroring tags to standalone repos"
```

---

## Task 14: Add bats-test workflow for CI tooling

**Files:**
- Create: `.github/workflows/test-ci-tooling.yml`

- [ ] **Step 1: Create the workflow**

```yaml
name: test-ci-tooling

on:
  push:
    paths:
      - 'ci/**'
      - 'tests/ci/**'
      - '.github/workflows/test-ci-tooling.yml'

jobs:
  bats:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v4
      - name: Install bats
        run: sudo apt-get update -q && sudo apt-get install -y bats
      - name: Run bats tests
        run: bats tests/ci/
```

- [ ] **Step 2: Validate and commit**

```bash
actionlint .github/workflows/test-ci-tooling.yml
git add .github/workflows/test-ci-tooling.yml
git commit -m "ci: add bats test workflow for ci/ tooling"
```

---

## Task 15: Create `bin/migrate-secrets.sh` template

**Files:**
- Create: `bin/migrate-secrets.sh`

- [ ] **Step 1: Create the template script**

```bash
#!/usr/bin/env bash
# bin/migrate-secrets.sh
#
# One-time migration helper. Run by a human with `gh` CLI auth after Azure
# variable values have been retrieved. Not invoked by CI.
#
# Usage:
#   1. Authenticate gh:    gh auth login
#   2. Edit the values below with secrets retrieved from the Azure DevOps
#      variable group.
#   3. Run from repo root: bash bin/migrate-secrets.sh
#
# Idempotent: safe to re-run; existing variables/secrets get overwritten.

set -euo pipefail

require_value() {
  local name="$1"
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    echo "ERROR: \$$name is empty. Edit this script or export the variable before running." >&2
    return 1
  fi
}

# === Non-secret variables ===
: "${STORAGE_API_URL_AWS:?set this env var}"
: "${STORAGE_API_URL_AZURE:?set this env var}"
: "${OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL:?set this env var}"
: "${LIBS_PUBLISHER_APP_ID:?set this env var}"

gh variable set STORAGE_API_URL_AWS                         --body "$STORAGE_API_URL_AWS"
gh variable set STORAGE_API_URL_AZURE                       --body "$STORAGE_API_URL_AZURE"
gh variable set OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL    --body "$OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL"
gh variable set LIBS_PUBLISHER_APP_ID                       --body "$LIBS_PUBLISHER_APP_ID"

# === Secrets ===
# Pass each secret via stdin to avoid leaking via process listings.
# Each line below reads from an env var with the corresponding name.

declare -a SECRETS=(
  INPUT_MAPPING__STORAGE_API_TOKEN_AWS
  INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS
  INPUT_MAPPING__STORAGE_API_TOKEN_AZURE
  INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE
  INPUT_MAPPING__STORAGE_API_TOKEN_AWS_BQ
  OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS
  OUTPUT_MAPPING__BIGQUERY_STORAGE_API_TOKEN
  OUTPUT_MAPPING_NATIVE_TYPES__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_NEW_NATIVE_TYPES__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_MASTER_AWS
  K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID
  K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY
  LIBS_PUBLISHER_APP_PRIVATE_KEY
)

for name in "${SECRETS[@]}"; do
  value="${!name:-}"
  if [[ -z "$value" ]]; then
    echo "WARN: \$$name is empty; skipping." >&2
    continue
  fi
  echo "Setting secret $name ..."
  printf '%s' "$value" | gh secret set "$name"
done

echo "Done. Verify with: gh secret list && gh variable list"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x bin/migrate-secrets.sh
```

- [ ] **Step 3: Commit**

```bash
git add bin/migrate-secrets.sh
git commit -m "ci: add one-time secrets migration template script"
```

---

## Task 16: Document the GitHub App setup

**Files:**
- Create: `docs/ci-github-app-setup.md`

- [ ] **Step 1: Write the setup doc**

```markdown
# GitHub App setup for library publishing

The `_split-library.yml` workflow pushes to standalone `keboola/<library>` repos
using a short-lived installation token from a GitHub App. This document covers
the one-time setup steps. Run these before merging the migration PR.

## Create the App

1. Go to https://github.com/organizations/keboola/settings/apps and click **New GitHub App**.
2. Fill in:
   - **Name:** `keboola-libs-publisher`
   - **Homepage URL:** the URL of this repo
   - **Webhook → Active:** unchecked
   - **Repository permissions:**
     - **Contents:** Read and write
     - **Metadata:** Read-only (auto-selected)
   - **Where can this GitHub App be installed?** Only on this account
3. Create the App and note the **App ID** shown on the next page.
4. Scroll to **Private keys** and click **Generate a private key**. A `.pem` file downloads.

## Install on standalone repos

1. From the App page, click **Install App** → choose the `keboola` org.
2. Pick **Only select repositories** and tick all 22 `keboola/<lib>` standalone repos:
   - api-bundle, azure-api-client, configuration-variables-resolver,
     doctrine-retry-bundle, git-service-php-api-client, input-mapping,
     k8s-client, logging-bundle, messenger-bundle, output-mapping,
     permission-checker, php-key-generator, php-storage-names-sanitizer,
     php-test-utils, query-service-api-php-client,
     sandboxes-service-api-php-client, service-client, settle, slicer,
     staging-provider, sync-actions-api-php-client, vault-api-php-client
3. Confirm.

## Add credentials to `platform-libraries`

1. Visit https://github.com/keboola/platform-libraries/settings/variables/actions
2. Click **New repository variable**, name it `LIBS_PUBLISHER_APP_ID`, paste the App ID.
3. Visit https://github.com/keboola/platform-libraries/settings/secrets/actions
4. Click **New repository secret**, name it `LIBS_PUBLISHER_APP_PRIVATE_KEY`, paste the entire contents of the `.pem` file (including `-----BEGIN…` and `-----END…` lines).

## Verification

After cutover, the `_split-library.yml` workflow should succeed on the first push that affects any library. If it fails with `403` or `Resource not accessible by integration`, double-check:
- The App is installed on the target repo
- The App has `Contents: Read and write` permission
- `LIBS_PUBLISHER_APP_PRIVATE_KEY` includes the full PEM block
```

- [ ] **Step 2: Commit**

```bash
git add docs/ci-github-app-setup.md
git commit -m "docs: add GitHub App setup guide for library publishing"
```

---

## Task 17: Cutover — delete Azure config and update READMEs

This is the breaking change. After this task is merged, Azure Pipelines stops triggering. **Make sure tasks 1–16 are merged or in the same PR, and that the pre-rollout steps from `docs/ci-github-app-setup.md` and `bin/migrate-secrets.sh` have been completed.**

**Files:**
- Delete: `azure-pipelines.yml`
- Delete: `azure-pipelines.tags.yml`
- Delete: `azure-pipelines/` (entire directory)
- Delete: `libs/*/azure-pipelines.tests.yml` (22 files)
- Delete: `bin/ci-find-changes.sh`
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Confirm pre-rollout is done**

Verify (do not proceed until all return success):
```bash
gh variable list  | grep -q LIBS_PUBLISHER_APP_ID
gh variable list  | grep -q STORAGE_API_URL_AWS
gh secret list    | grep -q LIBS_PUBLISHER_APP_PRIVATE_KEY
gh secret list    | grep -q INPUT_MAPPING__STORAGE_API_TOKEN_AWS
gh api orgs/keboola/installations 2>/dev/null | jq -e '.installations[] | select(.app_slug=="keboola-libs-publisher")' >/dev/null \
  && echo "App installed in keboola org"
```

If any of these fail, complete the corresponding setup step before continuing.

- [ ] **Step 2: Delete Azure pipeline files**

```bash
git rm azure-pipelines.yml
git rm azure-pipelines.tags.yml
git rm -r azure-pipelines/
git rm libs/*/azure-pipelines.tests.yml
git rm bin/ci-find-changes.sh
```

- [ ] **Step 3: Update `README.md`**

Remove or replace any Azure Pipelines references. Specifically, the README does not currently document CI in detail — no change may be needed. Verify:

```bash
grep -n -i 'azure' README.md || echo "No Azure references in README — skip"
```

If matches appear, replace each with a GitHub Actions reference (e.g., "CI runs on GitHub Actions; see `.github/workflows/ci.yml`").

- [ ] **Step 4: Update `CLAUDE.md`**

Find this section:
```
## CI/CD

- Azure Pipelines configuration in `azure-pipelines.yml`
- Each library has its own pipeline that runs on changes
- Libraries are independently published to GitHub repositories
```

Replace with:
```
## CI/CD

- GitHub Actions configuration in `.github/workflows/`:
  - `ci.yml` — main dispatcher (push to any branch); detects affected libraries via `ci/affected-libs.sh` + `ci/deps.json`, runs tests, splits affected libraries to standalone repos
  - `tag-publish.yml` — splits the matching library on tag push (`<lib>/<version>`)
  - `_lib-common.yml` — reusable workflow for the 19 simple libraries
  - `_lib-{input-mapping,output-mapping,k8s-client}.yml` — bespoke workflows for libraries with custom test shapes
  - `_split-library.yml` — reusable workflow that pushes a library subtree to its standalone repo using a GitHub App installation token
- Each library's CI is selected dynamically based on changed files plus transitive dependents (declared in `ci/deps.json`)
- Libraries are independently published to GitHub repositories via the split job (auth: `keboola-libs-publisher` GitHub App)
```

- [ ] **Step 5: Update commit message format note in `CLAUDE.md`**

No change needed; conventional-commits format is unchanged.

- [ ] **Step 6: Validate the working tree**

```bash
# Should now contain only:
# .github/workflows/{ci,tag-publish,_lib-common,_lib-input-mapping,_lib-output-mapping,_lib-k8s-client,_split-library,test-ci-tooling}.yml
# ci/{deps,lib-meta,publish-targets}.json + ci/affected-libs.sh
# tests/ci/...
ls .github/workflows/
ls ci/
test ! -e azure-pipelines.yml
test ! -e azure-pipelines.tags.yml
test ! -e azure-pipelines/
test ! -e bin/ci-find-changes.sh
find libs -name 'azure-pipelines*.yml' | grep . && echo "leftover Azure files!" && exit 1
echo "OK: Azure files are gone"
```

Expected: prints `OK: Azure files are gone`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "ci: cut over to GitHub Actions, delete Azure Pipelines config"
```

- [ ] **Step 8: Final sanity check before merge**

Run:
```bash
actionlint .github/workflows/*.yml
bats tests/ci/
```
Expected: both exit 0.

---

## Spec coverage review

Cross-checking the implementation tasks against the spec sections:

| Spec section | Implementing task(s) |
|---|---|
| Triggers (push branches; push tags) | Task 7 (ci.yml `on: push: branches`), Task 13 (tag-publish.yml `on: push: tags`) |
| File layout | Tasks 1–14 (each file created) |
| Image build via per-job buildx + `type=gha` cache | Tasks 6, 8, 9, 10 (every test workflow includes the warm-cache step) |
| Lock semantics → concurrency groups | Tasks 6, 8, 9 (each reusable workflow declares `concurrency`); Task 11 (split has its own group) |
| Test selection via `ci/deps.json` + transitive closure | Tasks 1, 3 |
| Test-pass gate via `needs:` + `if:` | Task 12 |
| Cross-repo push auth via GitHub App | Task 11, Task 16 (setup docs) |
| Secrets as flat repo Variables + Secrets | Tasks 8, 9, 10 (per-job env mappings); Task 15 (migration script) |
| Removal of unused registry-push templates | Task 17 (deletes `azure-pipelines/steps/`) |
| `_lib-common.yml` for ~19 simple libs | Task 6 |
| Bespoke workflows for input-mapping, output-mapping, k8s-client | Tasks 8–10 |
| `_split-library.yml` (reusable subtree split) | Task 11 |
| `tag-publish.yml` | Task 13 |
| `ci/publish-targets.json` sparse override map | Task 1 (file created); Tasks 12, 13 (consumed by jq fallback `// ("keboola/" + $lib)`) |
| Drift lint between composer.json and deps.json | Task 5 |
| Big-bang cutover | Task 17 |
| Migrate secrets script | Task 15 |
| GitHub App setup doc | Task 16 |

No gaps identified.
