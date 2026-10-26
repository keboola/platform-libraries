terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.39"
    }

    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.41"
    }
  }

  backend "s3" {
    assume_role = {
      role_arn = "arn:aws:iam::681277395786:role/kbc-local-dev-terraform"
    }
    region         = "eu-central-1"
    bucket         = "local-dev-terraform-bucket"
    dynamodb_table = "local-dev-terraform-table"
  }
}

locals {
  app_name = "azure-api-client"
}

variable "name_prefix" {
  type = string
}
