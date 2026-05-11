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
