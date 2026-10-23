#!/usr/bin/env bash
# Writes the Terraform outputs and Entra tokens of the logged-in Azure CLI identity as KEY=value lines to the given env file (the monorepo root .env by default),
# replacing earlier AZURE_API_CLIENT_TEST__ lines. Values go to the file only, never to stdout.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${1:-${SCRIPT_DIR}/../../../.env}"

# the file holds keys and the service principal secret
umask 077
touch "${ENV_FILE}"
chmod 600 "${ENV_FILE}"
TMP_FILE="$(mktemp)"
trap 'rm -f "${TMP_FILE}"' EXIT

grep -v '^AZURE_API_CLIENT_TEST__' "${ENV_FILE}" > "${TMP_FILE}" || true
terraform -chdir="${SCRIPT_DIR}" output -json \
  | jq -r 'to_entries[] | select(.key | startswith("AZURE_API_CLIENT_TEST__")) | "\(.key)=\(.value.value)"' \
  >> "${TMP_FILE}"
# Entra tokens of the identity logged in to the Azure CLI, valid for about an hour
echo "AZURE_API_CLIENT_TEST__SERVICE_BUS_ENTRA_TOKEN=$(az account get-access-token --resource https://servicebus.azure.net --query accessToken -o tsv)" >> "${TMP_FILE}"
echo "AZURE_API_CLIENT_TEST__EVENT_GRID_ENTRA_TOKEN=$(az account get-access-token --resource https://eventgrid.azure.net --query accessToken -o tsv)" >> "${TMP_FILE}"
cat "${TMP_FILE}" > "${ENV_FILE}"

echo "Wrote $(grep -c '^AZURE_API_CLIENT_TEST__' "${ENV_FILE}") variables to ${ENV_FILE}"
