terraform {
  required_version = "~> 1.1"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.3"
    }
  }

  backend "s3" {}
}

// ==== PROVIDERS ====
provider "aws" {
  default_tags {
    tags = {
      KeboolaStack = var.keboola_stack
      KeboolaRole  = "vault"
    }
  }
}

data "aws_eks_cluster" "k8s" {
  name = var.k8s_cluster_name
}

data "aws_eks_cluster_auth" "k8s" {
  name = var.k8s_cluster_name
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.k8s.endpoint
  token                  = data.aws_eks_cluster_auth.k8s.token
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.k8s.certificate_authority[0].data)
}

// ==== APPLICATION ====
locals {
  k8s_namespace            = "default"
  k8s_service_account_name = "vault"
}

module "common" {
  source = "../common"

  app_image_name           = var.app_image_name
  app_image_tag            = var.app_image_tag
  keboola_stack            = var.keboola_stack
  release_id               = var.release_id
  k8s_namespace            = local.k8s_namespace
  k8s_service_account_name = local.k8s_service_account_name

  storage_api_url = "https://connection.${var.hostname_suffix}"

  mysql_host            = data.aws_cloudformation_stack.kbc_job_queue_rds.outputs["Host"]
  mysql_port            = "3306"
  mysql_master_user     = data.aws_cloudformation_stack.kbc_job_queue_rds.outputs["Login"]
  mysql_master_password = data.aws_ssm_parameter.rds_master_password.value
}

// ==== VARIABLES ====
variable "k8s_cluster_name" {
  type = string
}

variable "keboola_stack" {
  type = string
}

variable "hostname_suffix" {
  type = string
}

variable "release_id" {
  type = string
}

variable "app_image_name" {
  type = string
}

variable "app_image_tag" {
  type = string
}
