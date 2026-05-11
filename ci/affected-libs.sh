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

# Compute the diff base (not needed for --lint)
if [[ "$MODE" != "--lint" ]]; then
  BASE=$(git merge-base "$BASE_REF" HEAD 2>/dev/null || git rev-parse "$BASE_REF")
fi

# DIRECT = set of libraries with paths in the diff matching libs/<lib>/**
direct() {
  git diff --name-only "$BASE" HEAD \
    | awk -F/ '$1=="libs" && NF>=2 { print $2 }' \
    | sort -u
}

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
            . as $acc
            | ([ $acc.frontier[] | ($reverse[.] // [])[] ] | unique - $acc.visited) as $next
            | { frontier: $next, visited: ($acc.visited + $next | unique) }
          end
        )
      | .visited
      | sort
      | .[]
    ' "$DEPS_FILE"
}

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

case "$MODE" in
  --direct) direct ;;
  --affected) affected ;;
  --emit) emit ;;
  --lint) lint ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
