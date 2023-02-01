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
