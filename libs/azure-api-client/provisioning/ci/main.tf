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

  backend "s3" {}
}

locals {
  app_name = "azure-api-client"
}

variable "name_prefix" {
  type    = string
  default = "ci"
}
