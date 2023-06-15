#!/usr/bin/env bash
set -Eeuo pipefail

help () {
  echo "Syntax: deploy.sh [--apply]"
  echo "Options:"
  echo "  --apply    Apply changes"
  echo ""
  echo "Example: deploy.sh"
  echo ""
}

get_backend_output() {
  OUTPUT_KEY=$1

  az deployment group show \
    --resource-group "${RESOURCE_GROUP}-terraform-backend" \
    --name kbc-stack-terraform-backend \
    --query "properties.outputs.${OUTPUT_KEY}.value" \
    --output tsv
}

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_PATH}"

APPLY=false
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --apply)
      APPLY=true
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
if [ ! -z "${POSITIONAL_ARGS:-}" ]; then
  set -- "${POSITIONAL_ARGS[@]}"
fi

# AZ CLI is authorized using service principal and in that case, Terraform needs following variables
# https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/guides/service_principal_client_secret
# https://github.com/microsoft/azure-pipelines-tasks/blob/master/Tasks/AzureCLIV2/Readme.md - how to access service principal credentials
export ARM_CLIENT_ID=$servicePrincipalId
export ARM_CLIENT_SECRET=$servicePrincipalKey
export ARM_TENANT_ID=$tenantId
export ARM_SUBSCRIPTION_ID=$(az account list --all --query "[?isDefault].id" --output tsv)

echo ""
echo "Terraform backend configuration:"
echo "resource_group_name=$(get_backend_output "resourceGroupName")"
echo "storage_account_name=$(get_backend_output "storageAccountName")"
echo "container_name=$(get_backend_output "storageContainerName")"
echo "key=vault.tfstate"
echo ""

terraform init -input=false -no-color \
  -backend-config="resource_group_name=$(get_backend_output "resourceGroupName")" \
  -backend-config="storage_account_name=$(get_backend_output "storageAccountName")" \
  -backend-config="container_name=$(get_backend_output "storageContainerName")" \
  -backend-config="key=vault.tfstate" \
  -migrate-state -force-copy

echo "=> Validating configuration"
terraform validate -no-color

echo "=> Planning changes"
terraform plan -input=false -no-color -out=terraform.tfplan \
  -var "keboola_stack=${KEBOOLA_STACK}" \
  -var "hostname_suffix=${HOSTNAME_SUFFIX}" \
  -var "az_resource_group_name=${RESOURCE_GROUP}" \
  -var "release_id=${RELEASE_RELEASENAME}" \
  -var "app_image_name=${VAULT_REPOSITORY}" \
  -var "app_image_tag=${VAULT_IMAGE_TAG}"

if [ "${APPLY}" = true ]; then

  echo "=> Applying changes"
  terraform apply -no-color terraform.tfplan

  echo "=> Updating Application Gateway"
  APPLICATION_GATEWAY_NAME=$(terraform output -raw az_application_gateway_name)
  VAULT_API_IP=$(terraform output -raw vault_api_ip)

  az network application-gateway address-pool update \
    --gateway-name=$APPLICATION_GATEWAY_NAME \
    --resource-group $RESOURCE_GROUP \
    --name=data-science \
    --servers $VAULT_API_IP

  az network application-gateway probe update \
    --gateway-name=$APPLICATION_GATEWAY_NAME \
    --resource-group $RESOURCE_GROUP \
    --name=data-science-health-probe \
    --host $VAULT_API_IP

else
  echo "=> Dry run finished. To apply changes set --apply flag"
fi
