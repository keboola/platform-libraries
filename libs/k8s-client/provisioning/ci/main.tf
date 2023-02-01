terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.52"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.14"
    }

    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.17"
    }
  }

  backend "s3" {}
}

variable "name_prefix" {
  type    = string
  default = "ci"
}
