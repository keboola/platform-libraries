data "azurerm_resource_group" "current_stack" {
  name = var.az_resource_group_name
}

// fetch data from ARM templates
data "azurerm_resource_group_template_deployment" "kbc_aks" {
  resource_group_name = data.azurerm_resource_group.current_stack.name
  name                = "kbc-aks"
}

data "azurerm_resource_group_template_deployment" "kbc_key_vaults" {
  resource_group_name = data.azurerm_resource_group.current_stack.name
  name                = "kbc-key-vaults"
}

data "azurerm_resource_group_template_deployment" "kbc_mysql_databases" {
  resource_group_name = data.azurerm_resource_group.current_stack.name
  name                = "kbc-mysql-databases"
}

data "azurerm_resource_group_template_deployment" "kbc_application_gateway" {
  resource_group_name = data.azurerm_resource_group.current_stack.name
  name                = "kbc-application-gateway"
}

locals {
  kbc_aks_data = {
    for k, v in jsondecode(data.azurerm_resource_group_template_deployment.kbc_aks.output_content) : k => v.value
  }
  kbc_key_vaults_data = {
    for k, v in jsondecode(data.azurerm_resource_group_template_deployment.kbc_key_vaults.output_content) : k => v.value
  }
  kbc_mysql_databases_data = {
    for k, v in jsondecode(data.azurerm_resource_group_template_deployment.kbc_mysql_databases.output_content) : k => v.value
  }
  kbc_application_gateway_data = {
    for k, v in jsondecode(data.azurerm_resource_group_template_deployment.kbc_application_gateway.output_content) : k => v.value
  }
}

data "azurerm_key_vault" "stack_key_vault" {
  resource_group_name = data.azurerm_resource_group.current_stack.name
  name                = local.kbc_key_vaults_data["stackParametersKeyVaultName"]
}

data "azurerm_key_vault_secret" "mysql_master_password" {
  key_vault_id = data.azurerm_key_vault.stack_key_vault.id
  name         = "jobQueueMysqlMasterPassword"
}
