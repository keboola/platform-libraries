locals {
  azure_location = "West Europe"
  # Service Bus namespace and Event Grid topic names are global
  resource_suffix = substr(md5(azurerm_resource_group.azure_api_client.id), 0, 12)
  resource_tags = {
    service = local.app_name
    env     = var.name_prefix
  }
}

variable "az_tenant_id" {
  type    = string
  default = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89" # Keboola
}

provider "azurerm" {
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = "c5182964-8dca-42c8-a77a-fa2a3c6946ea" # Keboola DEV Platform Services Team
}

provider "azuread" {
  tenant_id = var.az_tenant_id
}

data "azuread_client_config" "current" {}

// service principal
resource "azuread_application" "azure_api_client" {
  display_name = "${var.name_prefix}-${local.app_name}"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "azure_api_client" {
  client_id = azuread_application.azure_api_client.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal_password" "azure_api_client" {
  service_principal_id = azuread_service_principal.azure_api_client.id
}

// resource group
resource "azurerm_resource_group" "azure_api_client" {
  name     = "${var.name_prefix}-${local.app_name}"
  location = local.azure_location
  tags     = local.resource_tags
}
