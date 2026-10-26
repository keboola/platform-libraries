#!/usr/bin/env bash
set -Eeuo pipefail

TERRAFORM_VERSION="${TERRAFORM_VERSION:-1.1.7}"
curl -L -s -o terraform.zip \
  "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"

sudo unzip -o terraform.zip -d /usr/local/bin
rm terraform.zip
