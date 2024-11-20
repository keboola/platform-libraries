variable "az_aks_resource_group_name" {
  type    = string
  default = "sandboxes-ci-2021"
}

variable "az_aks_cluster_name" {
  type    = string
  default = "sandboxes-ci-2021-aks"
}

data "azurerm_kubernetes_cluster" "current" {
  provider            = azurerm.aks_subscription
  name                = var.az_aks_cluster_name
  resource_group_name = var.az_aks_resource_group_name
}

provider "kubernetes" {
  alias                  = "azure_k8s_cluster"
  host                   = data.azurerm_kubernetes_cluster.current.kube_config.0.host
  username               = data.azurerm_kubernetes_cluster.current.kube_config.0.username
  password               = data.azurerm_kubernetes_cluster.current.kube_config.0.password
  client_certificate     = base64decode(data.azurerm_kubernetes_cluster.current.kube_config.0.client_certificate)
  client_key             = base64decode(data.azurerm_kubernetes_cluster.current.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(data.azurerm_kubernetes_cluster.current.kube_config.0.cluster_ca_certificate)
}

module "az_kubernetes" {
  source = "../local/kubernetes"
  providers = {
    kubernetes = kubernetes.azure_k8s_cluster
  }

  name_prefix = var.name_prefix
}

output "az_aks_host" {
  value     = data.azurerm_kubernetes_cluster.current.kube_config.0.host
  sensitive = true
}

output "az_aks_ca_certificate" {
  value     = base64decode(data.azurerm_kubernetes_cluster.current.kube_config.0.cluster_ca_certificate)
  sensitive = true
}

output "az_aks_token" {
  value     = module.az_kubernetes.user_token
  sensitive = true
}

output "az_aks_namespace" {
  value     = module.az_kubernetes.namespace
  sensitive = true
}
