#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_PATH}/.."

terraform init -input=false -backend-config=./s3.tfbackend
terraform validate
