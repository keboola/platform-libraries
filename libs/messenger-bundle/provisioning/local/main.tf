terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.11"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.68"
    }

    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.41"
    }
  }

  backend "s3" {
    role_arn = "arn:aws:iam::681277395786:role/kbc-local-dev-terraform"
    region = "eu-central-1"
    bucket = "local-dev-terraform-bucket"
    dynamodb_table = "local-dev-terraform-table"
  }
}

locals {
  app_name = "messenger-bundle"
  app_display_name = "Keboola Messenger Bundle"
}

variable "name_prefix" {
  type = string
}
