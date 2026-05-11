#!/usr/bin/env bash
# bin/migrate-secrets.sh
#
# One-time migration helper. Run by a human with `gh` CLI auth after Azure
# variable values have been retrieved. Not invoked by CI.
#
# Usage:
#   1. Authenticate gh:    gh auth login
#   2. Edit the values below with secrets retrieved from the Azure DevOps
#      variable group.
#   3. Run from repo root: bash bin/migrate-secrets.sh
#
# Idempotent: safe to re-run; existing variables/secrets get overwritten.

set -euo pipefail

require_value() {
  local name="$1"
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    echo "ERROR: \$$name is empty. Edit this script or export the variable before running." >&2
    return 1
  fi
}

# === Non-secret variables ===
: "${STORAGE_API_URL_AWS:?set this env var}"
: "${STORAGE_API_URL_AZURE:?set this env var}"
: "${OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL:?set this env var}"
: "${LIBS_PUBLISHER_APP_ID:?set this env var}"

gh variable set STORAGE_API_URL_AWS                         --body "$STORAGE_API_URL_AWS"
gh variable set STORAGE_API_URL_AZURE                       --body "$STORAGE_API_URL_AZURE"
gh variable set OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL    --body "$OUTPUT_MAPPING__BIGQUERY_STORAGE_API_URL"
gh variable set LIBS_PUBLISHER_APP_ID                       --body "$LIBS_PUBLISHER_APP_ID"

# === Secrets ===
# Pass each secret via stdin to avoid leaking via process listings.
# Each line below reads from an env var with the corresponding name.

declare -a SECRETS=(
  INPUT_MAPPING__STORAGE_API_TOKEN_AWS
  INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS
  INPUT_MAPPING__STORAGE_API_TOKEN_AZURE
  INPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AZURE
  INPUT_MAPPING__STORAGE_API_TOKEN_AWS_BQ
  OUTPUT_MAPPING__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING__STORAGE_API_TOKEN_MASTER_AWS
  OUTPUT_MAPPING__BIGQUERY_STORAGE_API_TOKEN
  OUTPUT_MAPPING_NATIVE_TYPES__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_NEW_NATIVE_TYPES__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_AWS
  OUTPUT_MAPPING_SLICE_FEATURE__STORAGE_API_TOKEN_MASTER_AWS
  K8S_CLIENT_TERRAFORM_AWS_ACCESS_KEY_ID
  K8S_CLIENT_TERRAFORM_AWS_SECRET_ACCESS_KEY
  LIBS_PUBLISHER_APP_PRIVATE_KEY
)

for name in "${SECRETS[@]}"; do
  value="${!name:-}"
  if [[ -z "$value" ]]; then
    echo "WARN: \$$name is empty; skipping." >&2
    continue
  fi
  echo "Setting secret $name ..."
  printf '%s' "$value" | gh secret set "$name"
done

echo "Done. Verify with: gh secret list && gh variable list"
