#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh

output_file 'var/k8s/caCert.pem' "$(terraform_output 'az_aks_ca_certificate')"
output_var 'K8S_HOST' "$(terraform_output 'az_aks_host')"
output_var 'K8S_CA_CERT_PATH' 'var/k8s/caCert.pem'
output_var 'K8S_TOKEN' "$(terraform_output 'az_aks_token')"
output_var 'K8S_NAMESPACE' "$(terraform_output 'az_aks_namespace')"
echo ""
