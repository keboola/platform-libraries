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

  aws cloudformation describe-stacks \
    --stack-name "kbc-stack-terraform-backend" \
    --query "Stacks[0].Outputs[?OutputKey=='$OUTPUT_KEY'].OutputValue" \
    --output text
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

echo ""
echo "Terraform backend configuration:"
echo "region=${AWS_REGION}"
echo "bucket=$(get_backend_output "TerraformRemoteStateS3BucketName")"
echo "dynamodb_table=$(get_backend_output "TerraformRemoteStateDynamoDBTableName")"
echo "key=vault.tfstate"
echo ""

terraform init -input=false -no-color \
  -backend-config="region=${AWS_REGION}" \
  -backend-config="bucket=$(get_backend_output "TerraformRemoteStateS3BucketName")" \
  -backend-config="dynamodb_table=$(get_backend_output "TerraformRemoteStateDynamoDBTableName")" \
  -backend-config="key=vault.tfstate"

echo "=> Validating configuration"
terraform validate -no-color

echo "=> Planning changes"
terraform plan -input=false -no-color -out=terraform.tfplan \
  -var "k8s_cluster_name=${AWS_EKS_CLUSTER_NAME}" \
  -var "keboola_stack=${KEBOOLA_STACK}" \
  -var "hostname_suffix=${HOSTNAME_SUFFIX}" \
  -var "release_id=${RELEASE_RELEASENAME}" \
  -var "app_image_name=${VAULT_REPOSITORY}" \
  -var "app_image_tag=${VAULT_IMAGE_TAG}"

if [ "${APPLY}" = true ]; then
  echo "=> Applying changes"
  terraform apply -no-color terraform.tfplan
else
  echo "=> Dry run finished. To apply changes set --apply flag"
fi
