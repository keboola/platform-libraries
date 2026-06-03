#!/usr/bin/env bash
set -e

if [[ -z ${1+x} || -z ${2+x} || -z ${3+x} || -z ${4+x} ]]; then
  echo "Usage: split-repo.sh <source-repo-path> <target-repo-url> <library-path> <tag-prefix>"
  echo ""
  echo " <source-repo-path> Source Git repository path (the monorepo, may be also local path)"
  echo " <target-repo-url>  Target Git repository URL (the read-only library repo)"
  echo " <library-path>     Relative path to the library inside the source repo"
  echo " <tag-prefix>       Common prefix of tags to mirror. The prefix will be stripped from tags"
  echo ""
  echo "Example: split-repo.sh /build/monorepo git@github.com:keboola/library-repo.git libs/my-lib my-lib/"
  exit 1
fi

SOURCE_REPO_PATH="${1}"
TARGET_REPO_URL="${2}"
LIB_PATH="${3}"
TAG_PREFIX="${4}"

# We require the source to be a local path because we use --mirror flag. The --mirror flag is needed on the other hand
# to copy all refs when doing a local clone.
if [[ ! -d "${SOURCE_REPO_PATH}/.git" ]]; then
  echo "Source repo '${SOURCE_REPO_PATH}' is not a valid GIT repository"
  exit 1
fi

TMP_DIR=`mktemp -d`
clean_up () {
    ARG=$?
    rm -rf $TMP_DIR
    exit $ARG
}
trap clean_up EXIT

echo ">> Cloning source repo '${SOURCE_REPO_PATH}'"
git clone --no-local --mirror "${SOURCE_REPO_PATH}" $TMP_DIR
cd $TMP_DIR

echo ">> Rebuild repo"
LIB_PATH="${LIB_PATH%/}/" # ensure trailing slash
git filter-repo --quiet --subdirectory-filter "${LIB_PATH}" --refname-callback "
# not a tag -> keep as is
if not refname.startswith(b'refs/tags/'):
  return refname

# tag, but not matching prefix -> SKIP
if not refname.startswith(b'refs/tags/${TAG_PREFIX}'):
  return b'refs/tags/SKIP'

# tag, with correct prefix -> strip prefix
return b'refs/tags/' + refname[len(b'refs/tags/${TAG_PREFIX}'):]
"
git update-ref -d refs/tags/SKIP

echo ">> Push to target repo '${TARGET_REPO_URL}'"
git remote add origin "${TARGET_REPO_URL}"
git push -v origin --mirror

echo ">> Done"
