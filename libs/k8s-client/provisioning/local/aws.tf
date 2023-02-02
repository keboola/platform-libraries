provider "aws" {
  profile = "Keboola-Dev-Platform-Services-AWSAdministratorAccess"
  allowed_account_ids = ["025303414634"] # Dev - Platform Services
  region  = "eu-central-1"

  default_tags {
    tags = {
      KebolaStack = "${var.name_prefix}-sandboxes-service"
      KeboolaRole = "sandboxes-service"
    }
  }
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
