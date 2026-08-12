#!/usr/bin/env bash
set -e

if [[ -z ${1+x} || -z ${2+x} || -z ${3+x} ]]; then
  echo "Usage: release-context.sh <library> <version> <output-file>"
  echo ""
  echo " <library>     Library directory name under libs/ (e.g. output-mapping)"
  echo " <version>     Released version, without the library prefix (e.g. 27.10.0)"
  echo " <output-file> Where to write the collected context (markdown)"
  echo ""
  echo "Collects everything needed to write release notes for a library version: the previous"
  echo "released version, the commits that changed the library since then and the pull requests"
  echo "those commits came from. Prints the previous version (empty for a first release) to stdout."
  echo ""
  echo "Requires GH_TOKEN with read access to the monorepo pull requests."
  exit 1
fi

LIBRARY="${1}"
VERSION="${2}"
OUTPUT_FILE="${3}"
MONOREPO="${GITHUB_REPOSITORY:-keboola/platform-libraries}"
TAG="${LIBRARY}/${VERSION}"

# A first release has no range to diff against, so it would take the library's entire history.
INITIAL_RELEASE_COMMIT_LIMIT=50

cd "$(git rev-parse --show-toplevel)"

if [[ ! -d "libs/${LIBRARY}" ]]; then
  echo "Library 'libs/${LIBRARY}' does not exist" >&2
  exit 1
fi

if ! git rev-parse -q --verify "refs/tags/${TAG}" > /dev/null; then
  echo "Tag '${TAG}' does not exist in the monorepo" >&2
  exit 1
fi

# Releases are strictly '<library>/X.Y.Z'. The same tag namespace also holds throwaway build and dev
# tags (e.g. 'output-mapping/build-pat-664.1'), which must not be taken for the previous release —
# and which is why 'git tag --sort=-v:refname' cannot be used here, it sorts them to the top.
PREVIOUS_TAG="$(
  git tag --list "${LIBRARY}/*" \
    | grep -E "^${LIBRARY}/[0-9]+\.[0-9]+\.[0-9]+$" \
    | sort -V \
    | awk -v tag="${TAG}" '$0 == tag { print previous; exit } { previous = $0 }'
)"
PREVIOUS_VERSION="${PREVIOUS_TAG#"${LIBRARY}/"}"

if [[ -n "${PREVIOUS_TAG}" ]]; then
  RANGE="${PREVIOUS_TAG}..${TAG}"
  LOG_LIMIT=()
else
  RANGE="${TAG}"
  LOG_LIMIT=(--max-count="${INITIAL_RELEASE_COMMIT_LIMIT}")
fi

echo ">> Collecting commits in '${RANGE}' touching 'libs/${LIBRARY}'" >&2
mapfile -t COMMITS < <(git log --format='%H' "${LOG_LIMIT[@]}" "${RANGE}" -- "libs/${LIBRARY}")

# Resolve pull requests per commit rather than by parsing the merge commits in the range: the range
# also contains merges of pull requests that changed other libraries, and a path-filtered 'git log'
# drops the merge commits themselves. The commit -> pull request lookup maps reliably either way,
# and also covers squash merges, which leave no merge commit at all.
echo ">> Resolving pull requests for ${#COMMITS[@]} commit(s)" >&2
PULL_REQUESTS="$(
  for COMMIT in "${COMMITS[@]}"; do
    gh api "repos/${MONOREPO}/commits/${COMMIT}/pulls" --jq '.[].number' 2> /dev/null || true
  done | sort -un
)"

mkdir -p "$(dirname "${OUTPUT_FILE}")"

{
  echo "# Release context"
  echo ""
  echo "- Monorepo: \`${MONOREPO}\`"
  echo "- Library: \`libs/${LIBRARY}\`"
  echo "- Version being released: \`${VERSION}\`"
  if [[ -n "${PREVIOUS_TAG}" ]]; then
    echo "- Previous released version: \`${PREVIOUS_VERSION}\`"
  else
    echo "- Previous released version: none, this is the library's first release (showing at most" \
      "${INITIAL_RELEASE_COMMIT_LIMIT} commits)"
  fi
  echo ""

  echo "## Pull requests"
  echo ""
  if [[ -n "${PULL_REQUESTS}" ]]; then
    echo "These pull requests changed the library in this release:"
    echo ""
    while read -r PULL_REQUEST; do
      echo "- ${MONOREPO}#${PULL_REQUEST}"
    done <<< "${PULL_REQUESTS}"
  else
    echo "None found — every change goes through a pull request, so this is unexpected; work from" \
      "the commits below."
  fi
  echo ""

  echo "## Commits touching libs/${LIBRARY}"
  echo ""
  if [[ ${#COMMITS[@]} -gt 0 ]]; then
    git log --format='- `%h` %s' "${LOG_LIMIT[@]}" "${RANGE}" -- "libs/${LIBRARY}"
  else
    echo "None — no commit in this range changed the library."
  fi
  echo ""

  if [[ -n "${PREVIOUS_TAG}" ]]; then
    echo "## Files changed"
    echo ""
    echo '```'
    git diff --stat "${PREVIOUS_TAG}" "${TAG}" -- "libs/${LIBRARY}"
    echo '```'
  fi
} > "${OUTPUT_FILE}"

echo ">> Written to '${OUTPUT_FILE}'" >&2
echo "${PREVIOUS_VERSION}"
