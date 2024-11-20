variable "az_tenant_id" {
  type    = string
  default = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89"
}

provider "azurerm" {
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = "9577e289-304e-4165-abe0-91c933200878"
}

provider "azurerm" {
  alias = "aks_subscription"
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = "9577e289-304e-4165-abe0-91c933200878"
}

data "azurerm_client_config" "current" {}
