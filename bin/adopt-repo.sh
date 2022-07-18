#!/bin/bash
set -Eeuo pipefail

if [[ -z ${1+x} || -z ${2+x} || -z ${3+x} ]]; then
  echo "Usage: adopt-repo.sh <repo> <local-path> <tag-prefix>"
  echo ""
  echo " <repo>        Source Git repository URL"
  echo " <local-path>  Local path where to put the repo inside monorepo"
  echo " <tag-prefix>  Common prefix to add to all tags from the repo. Good practice is to add some separator like slash on the end ('internal-api/')"
  echo ""
  echo "Example: adopt-repo.sh git@github.com:keboola/job-queue-api.git apps/internal-api internal-api/"
  exit 1
fi

REPO_URL=$1
LOCAL_PATH=$2
TAG_PREFIX=$3

REPO_NAME=${REPO_URL}
REPO_NAME=${REPO_NAME##*/} # keep only last path element git@github.com:keboola/job-queue-api.git -> job-queue-api.git
REPO_NAME=${REPO_NAME%.*}  # strip "extension" job-queue-api.git -> job-queue-api

TMP_DIR=`mktemp -d`
clean_up () {
    ARG=$?
    rm -rf $TMP_DIR
    exit $ARG
}
trap clean_up EXIT

REPO_PATH="${TMP_DIR}"

# do cleanup from possible previous run
rm -rf "${LOCAL_PATH}" || true
git remote rm -q "${REPO_NAME}" || true

echo "=> Cloning repo ${REPO_URL}"
git clone "${REPO_URL}" "${REPO_PATH}"

echo "=> Rewriting history"
git -C "${REPO_PATH}" filter-repo --to-subdirectory-filter "${LOCAL_PATH}" --refname-callback "
# not a tag -> keep as is
if not refname.startswith(b'refs/tags/'):
  return refname

# tag, add prefix
print(b'moving tag ' + refname)
return b'refs/tags/${TAG_PREFIX}' + refname[len(b'refs/tags/'):]
"

echo "=> Merging to monorepo"
git remote add "${REPO_NAME}" "${REPO_PATH}"
git fetch "${REPO_NAME}"
git merge --allow-unrelated-histories -m "Merge ${REPO_NAME} to monorepo" "${REPO_NAME}/main"

echo "=> Clean-up"
rm -rf "${REPO_PATH}"
git remote rm "${REPO_NAME}"

echo "Done. Don't forget to push changes!"
