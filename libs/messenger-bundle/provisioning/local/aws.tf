locals {
  aws_region = "eu-central-1"
}

provider "aws" {
  profile             = "Keboola-Dev-Platform-Services-AWSAdministratorAccess"
  allowed_account_ids = ["025303414634"] # Dev - Platform Services
  region              = local.aws_region

  default_tags {
    tags = {
      KebolaStack = "${var.name_prefix}-${local.app_name}"
      KeboolaRole = local.app_name
    }
  }
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

resource "aws_iam_user" "messenger_bundle" {
  name = "${var.name_prefix}-${local.app_name}"
}

resource "aws_iam_access_key" "messenger_bundle" {
  user = aws_iam_user.messenger_bundle.name
}

output "aws_region" {
  value = local.aws_region
}

output "aws_access_key_id" {
  value = aws_iam_access_key.messenger_bundle.id
}

output "aws_access_key_secret" {
  value     = aws_iam_access_key.messenger_bundle.secret
  sensitive = true
}
