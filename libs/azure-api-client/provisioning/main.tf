terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.0"
    }
  }
}

variable "name_prefix" {
  type = string
  description = "Use your nickname, RG will be prefixed with this value"
  default = "ci"
}

provider "azurerm" {
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = var.az_subscription_id
}

provider "azuread" {
  tenant_id = var.az_tenant_id
}

variable "az_tenant_id" {
  type    = string
  description = "Default is Keboola tenant"
  default = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89"
}
variable "az_subscription_id" {
  type    = string
  description = "Default is PS team CI subscription"
  default = "9577e289-304e-4165-abe0-91c933200878"
}

data "azurerm_client_config" "current" {}
data "azuread_client_config" "current" {}

locals {
  resource_group_uuid = substr(md5(azurerm_resource_group.api_client_tests.id), 0, 17)
  resource_tags       = {
    service = "api_client_tests"
    env     = azurerm_resource_group.api_client_tests.name
  }
}

// resource group
resource "azurerm_resource_group" "api_client_tests" {
  name     = "${var.name_prefix}-api_client"
  location = "North Europe"
}

output "AZURE_API_CLIENT_CI__SERVICE_BUS__ENDPOINT" {
  value = "https://${azurerm_servicebus_namespace.queues.name}.servicebus.windows.net:443/"
}

output "AZURE_API_CLIENT_CI__SERVICE_BUS__SHARED_ACCESS_KEY" {
  sensitive = true
  value = azurerm_servicebus_namespace.queues.default_primary_key
}

output "AZURE_API_CLIENT_CI__EVENT_GRID__ACCESS_KEY" {
  sensitive = true
  value = azurerm_eventgrid_topic.topic_tests.primary_access_key
}
output "AZURE_API_CLIENT_CI__EVENT_GRID__TOPIC_HOSTNAME" {
  value = replace(replace(azurerm_eventgrid_topic.topic_tests.endpoint, "https://", ""),"/api/events","")
}

output "AZURE_API_CLIENT_CI__EVENT_GRID__TOPIC_NAME" {
  value = azurerm_eventgrid_topic.topic_tests.name
}
