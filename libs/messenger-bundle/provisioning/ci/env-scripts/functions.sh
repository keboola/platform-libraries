#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_PATH}/../../.."

terraform_output () {
  jq ".${1}.value" -r ../tfoutput.json
}

output_var () {
  echo "${1}=\"${2}\""
}

output_file () {
  mkdir -p "${PROJECT_ROOT}/$(dirname "${1}")"
  echo "${2}" > "${PROJECT_ROOT}/${1}"
}

print_url () {
  FORMAT=$1
  shift

  ARGS=""
  for ARG in "$@"; do
    URL_ENCODED="$(echo -n "${ARG}" | jq -sRr @uri)"
    ARGS="${ARGS} ${URL_ENCODED}"
  done

  printf "${FORMAT}" ${ARGS}
}
