provider "aws" {
  allowed_account_ids = ["480319613404"] # CI-Platform-Services-Team
  region              = "eu-central-1"

  default_tags {
    tags = {
      KebolaStack = "${var.name_prefix}-sandboxes-service"
      KeboolaRole = "sandboxes-service"
    }
  }
}

# to manage K8S we have to use ci-ps-eu-central-1-admin20220415152656187300000002 role
# regular developer account does not have access (without assume role)
provider "aws" {
  alias               = "k8s_access_role"
  allowed_account_ids = ["480319613404"] # CI-Platform-Services-Team
  region              = "eu-central-1"
  assume_role {
    role_arn = "arn:aws:iam::480319613404:role/ci-ps-eu-central-1-admin20220415152656187300000002"
  }

  default_tags {
    tags = {
      KebolaStack = "${var.name_prefix}-sandboxes-service"
      KeboolaRole = "sandboxes-service"
    }
  }
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
