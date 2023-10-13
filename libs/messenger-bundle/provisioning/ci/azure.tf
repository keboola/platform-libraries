locals {
  azure_location = "West Europe"
}

variable "az_tenant_id" {
  type    = string
  default = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89" # Keboola
}

provider "azurerm" {
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = "9577e289-304e-4165-abe0-91c933200878" # Keboola DEV PS Team CI
}

provider "azuread" {
  tenant_id = var.az_tenant_id
}

data "azurerm_client_config" "current" {}
data "azuread_client_config" "current" {}

// service principal
resource "azuread_application" "messenger_bundle" {
  display_name = "${var.name_prefix}-${local.app_name}"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "messenger_bundle" {
  application_id = azuread_application.messenger_bundle.application_id
  owners         = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal_password" "messenger_bundle" {
  service_principal_id = azuread_service_principal.messenger_bundle.id
}

// resource group
resource "azurerm_resource_group" "messenger_bundle" {
  name     = "${var.name_prefix}-${local.app_name}"
  location = local.azure_location
}

resource "azurerm_role_assignment" "messenger_bundle_contributor" {
  scope                = azurerm_resource_group.messenger_bundle.id
  principal_id         = azuread_service_principal.messenger_bundle.id
  role_definition_name = "Contributor"
}

output "az_tenant_id" {
  value = var.az_tenant_id
}

output "az_application_id" {
  value = azuread_application.messenger_bundle.application_id
}

output "az_application_secret" {
  value     = azuread_service_principal_password.messenger_bundle.value
  sensitive = true
}
