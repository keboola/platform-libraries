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
