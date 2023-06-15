terraform {
  required_version = "~> 1.1"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.61"
    }
  }

  backend "azurerm" {}
}

// ==== PROVIDERS ====
provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

data "azurerm_kubernetes_cluster" "k8s" {
  name                = local.kbc_aks_data["clusterName"]
  resource_group_name = var.az_resource_group_name
}

provider "kubernetes" {
  host                   = data.azurerm_kubernetes_cluster.k8s.kube_config.0.host
  username               = data.azurerm_kubernetes_cluster.k8s.kube_config.0.username
  password               = data.azurerm_kubernetes_cluster.k8s.kube_config.0.password
  client_certificate     = base64decode(data.azurerm_kubernetes_cluster.k8s.kube_config.0.client_certificate)
  client_key             = base64decode(data.azurerm_kubernetes_cluster.k8s.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(data.azurerm_kubernetes_cluster.k8s.kube_config.0.cluster_ca_certificate)
}

// ==== APPLICATION ====
locals {
  k8s_namespace = "default"
}

module "common" {
  source = "../common"

  app_image_name           = var.app_image_name
  app_image_tag            = var.app_image_tag
  keboola_stack            = var.keboola_stack
  release_id               = var.release_id
  k8s_namespace            = local.k8s_namespace
  k8s_service_account_name = ""

  storage_api_url = "https://connection.${var.hostname_suffix}"

  mysql_host            = local.kbc_mysql_databases_data["jobQueueServerHost"]
  mysql_port            = "3306"
  mysql_master_user     = "keboola@${local.kbc_mysql_databases_data["jobQueueServerName"]}"
  mysql_master_password = data.azurerm_key_vault_secret.mysql_master_password.value
}

// ==== VARIABLES ====
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

variable "az_resource_group_name" {
  type = string
}

// ==== OUTPUT ====
output "az_application_gateway_name" {
  value = local.kbc_application_gateway_data["appGatewayName"]
}

output "vault_api_ip" {
  value = kubernetes_service.vault_api.status[0].load_balancer[0].ingress[0].ip
}
