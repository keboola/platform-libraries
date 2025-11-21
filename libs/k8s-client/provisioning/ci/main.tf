terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.8"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.39"
    }

    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.38"
    }
  }

  backend "s3" {}
}

variable "name_prefix" {
  type    = string
  default = "ci"
}
