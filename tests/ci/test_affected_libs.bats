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

@test "affected: change without dependents = only the changed lib" {
  echo "// gamma v2" > libs/gamma/file.php
  git commit -qam "edit gamma"
  run "$SCRIPT" --affected --deps "$FIXTURES/deps-sample.json" --base main
  [ "$status" -eq 0 ]
  [ "$output" = "gamma" ]
}

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
