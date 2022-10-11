#!/usr/bin/env bash
set -e

VERBOSE=false

help () {
  echo "Syntax: ci-find-changes.sh [-v,--verbose] <targetBranch> <varName>:<path>"
  echo "Options:"
  echo "  -v|--verbose    Output extra information"
  echo ""
  echo "Example: ci-find-changes.sh main internalApi:apps/internal-api internalApiPhpClient:libs/internal-api-php-client"
  echo ""
}

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      help
      exit 0
      ;;
    -*|--*)
      echo "Unknown option $1"
      echo ""
      help
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done
set -- "${POSITIONAL_ARGS[@]}"

TARGET_BRANCH=${1:-}
if [[ $TARGET_BRANCH = "" ]]; then
    echo "Missing <targetBranch> argument"
    echo ""
    help
    exit 1
fi

ALL_CHANGES=
for PROJECT in ${@:2}; do
  PROJECT_CONFIG=(${PROJECT//:/ })
  PROJECT_VAR_NAME=${PROJECT_CONFIG[0]}
  PROJECT_DIR=${PROJECT_CONFIG[1]}

  echo -n "Checking ${PROJECT_DIR} ... "
  DIR_EXISTS_IN_TARGET_BRANCH=$(git ls-tree -d "origin/${TARGET_BRANCH}:${PROJECT_DIR}" >/dev/null 2>&1 && echo 1 || echo 0)
  if [[ $DIR_EXISTS_IN_TARGET_BRANCH -eq 0 ]]; then
    HAS_CHANGES=1
    echo "does not exists in ${TARGET_BRANCH}"
  else
    PROJECT_CHANGES=$(git diff --name-only "origin/${TARGET_BRANCH}" "${PROJECT_DIR}")

    if [[ $(echo -n "${PROJECT_CHANGES}" | wc -l) -gt 0 ]]; then
      HAS_CHANGES=1
      echo "has changes"

      if [ "${VERBOSE}" = true ]; then
        echo "${PROJECT_CHANGES}"
        echo ""
      fi
    else
      HAS_CHANGES=0
      echo "no changes"
    fi
  fi

  if [[ $HAS_CHANGES -eq 1 ]]; then
    echo "##vso[task.setvariable variable=changedProjects_${PROJECT_VAR_NAME};isOutput=true]1"
    ALL_CHANGES="${ALL_CHANGES} \"${PROJECT_VAR_NAME}\""
  fi
done

if [[ "${ALL_CHANGES}" == "" ]]; then
  echo ">> No changes detected, triggering all projects builds"
  for PROJECT in $@; do
    PROJECT_CONFIG=(${PROJECT//:/ })
    PROJECT_VAR_NAME=${PROJECT_CONFIG[0]}

    echo "##vso[task.setvariable variable=changedProjects_${PROJECT_VAR_NAME};isOutput=true]1"
    ALL_CHANGES="${ALL_CHANGES} \"${PROJECT_VAR_NAME}\""
  done
fi

echo "##vso[task.setvariable variable=changedProjects;isOutput=true]$ALL_CHANGES"
