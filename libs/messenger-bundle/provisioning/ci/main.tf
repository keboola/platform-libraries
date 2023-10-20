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

  backend "s3" {}
}

locals {
  app_name         = "messenger-bundle"
  app_display_name = "Keboola Messenger Bundle"
}

variable "name_prefix" {
  type    = string
  default = "ci"
}
