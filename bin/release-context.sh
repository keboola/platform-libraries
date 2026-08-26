#!/usr/bin/env bash
# Deliberately 'set -e' without '-o pipefail': the RELEASE_TAGS pipeline below relies on a 'grep'
# that finds nothing exiting 1 without failing the script, which is what a library's first release
# looks like. Adding pipefail here breaks every first release.
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
  echo "those commits came from."
  echo ""
  echo "Prints 'previous-version=<version>' (empty for a first release), 'has-changes=true|false'"
  echo "and 'is-newest=true|false' (whether this is the library's highest released version) to"
  echo "stdout, so a GitHub Actions caller can append them straight to \$GITHUB_OUTPUT."
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
RELEASE_TAGS="$(
  git tag --list "${LIBRARY}/*" \
    | grep -E "^${LIBRARY}/[0-9]+\.[0-9]+\.[0-9]+$" \
    | sort -V
)"
PREVIOUS_TAG="$(awk -v tag="${TAG}" '$0 == tag { print previous; exit } { previous = $0 }' <<< "${RELEASE_TAGS}")"
PREVIOUS_VERSION="${PREVIOUS_TAG#"${LIBRARY}/"}"

# Whether this is the library's highest released version, so the caller can pass an explicit
# '--latest' to 'gh release create'. Without it the GitHub API defaults make_latest to true, and
# backfilling an older version — the reason create-release.yml exists — would demote the real
# latest release.
if [[ "$(tail -n 1 <<< "${RELEASE_TAGS}")" == "${TAG}" ]]; then
  IS_NEWEST=true
else
  IS_NEWEST=false
fi

if [[ -n "${PREVIOUS_TAG}" ]]; then
  RANGE="${PREVIOUS_TAG}..${TAG}"
  LOG_LIMIT=()
else
  RANGE="${TAG}"
  LOG_LIMIT=(--max-count="${INITIAL_RELEASE_COMMIT_LIMIT}")
fi

echo ">> Collecting commits in '${RANGE}' touching 'libs/${LIBRARY}'" >&2
# Assigned, not piped into 'mapfile': a failing 'git log' inside a process substitution leaves
# mapfile succeeding on empty input, and empty input here has to be fatal, not quietly plausible.
COMMIT_LIST="$(git log --format='%H' "${LOG_LIMIT[@]}" "${RANGE}" -- "libs/${LIBRARY}")"

# An empty commit list must never be passed off as context: the model would still write *something*
# from it, and that something is non-empty text which passes the caller's emptiness check and ships
# as a release note. It is not an error though — 14 of the first 518 releases changed nothing in
# their own library, because this monorepo re-releases a library to pick up the sibling libraries it
# depends on through '*@dev'. So it is reported as has-changes=false and the caller writes a fixed
# note instead of asking the model. A broken 'git log' or 'gh api' below stays fatal.
if [[ -z "${COMMIT_LIST}" ]]; then
  echo ">> No commit in '${RANGE}' changed 'libs/${LIBRARY}'" >&2
  mkdir -p "$(dirname "${OUTPUT_FILE}")"
  {
    echo "# Release context"
    echo ""
    echo "No commit in \`${RANGE}\` changed \`libs/${LIBRARY}\`."
  } > "${OUTPUT_FILE}"
  echo "previous-version=${PREVIOUS_VERSION}"
  echo "is-newest=${IS_NEWEST}"
  echo "has-changes=false"
  exit 0
fi
mapfile -t COMMITS <<< "${COMMIT_LIST}"

# Resolve pull requests per commit rather than by parsing the merge commits in the range: the range
# also contains merges of pull requests that changed other libraries, and a path-filtered 'git log'
# drops the merge commits themselves. The commit -> pull request lookup maps reliably either way,
# and also covers squash merges, which leave no merge commit at all.
echo ">> Resolving pull requests for ${#COMMITS[@]} commit(s)" >&2
PULL_REQUEST_NUMBERS=()
for COMMIT in "${COMMITS[@]}"; do
  # No '|| true' and no discarded stderr: an expired token or a rate limit must fail the release
  # rather than silently turn into "no pull requests found". A commit with no pull request is not
  # an error and simply returns nothing.
  COMMIT_PULL_REQUESTS="$(gh api "repos/${MONOREPO}/commits/${COMMIT}/pulls" --jq '.[].number')"
  if [[ -n "${COMMIT_PULL_REQUESTS}" ]]; then
    while read -r PULL_REQUEST; do
      PULL_REQUEST_NUMBERS+=("${PULL_REQUEST}")
    done <<< "${COMMIT_PULL_REQUESTS}"
  fi
done

PULL_REQUESTS=""
if [[ ${#PULL_REQUEST_NUMBERS[@]} -gt 0 ]]; then
  PULL_REQUESTS="$(printf '%s\n' "${PULL_REQUEST_NUMBERS[@]}" | sort -un)"
fi

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
  git log --format='- `%h` %s' "${LOG_LIMIT[@]}" "${RANGE}" -- "libs/${LIBRARY}"
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
echo "previous-version=${PREVIOUS_VERSION}"
echo "is-newest=${IS_NEWEST}"
echo "has-changes=true"
