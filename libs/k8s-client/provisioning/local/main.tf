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

  backend "s3" {
    assume_role = {
      role_arn = "arn:aws:iam::681277395786:role/kbc-local-dev-terraform"
    }
    region         = "eu-central-1"
    bucket         = "local-dev-terraform-bucket"
    dynamodb_table = "local-dev-terraform-table"
  }
}

variable "name_prefix" {
  type = string
}
