#!/usr/bin/env bash
set -Eeuo pipefail

terraform_output () {
  jq ".${1}.value" -r ../tfoutput.json
}

output_var () {
  echo "${1}=\"${2}\""
}
