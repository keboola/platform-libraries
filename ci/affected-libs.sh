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

case "$MODE" in
  --direct) direct ;;
  --affected) affected ;;
  --emit) emit ;;
  *) echo "Mode $MODE not implemented yet" >&2; exit 2 ;;
esac
