provider "azuread" {
  tenant_id = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89" // Keboola
}

provider "azurerm" {
  tenant_id       = "9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89" // Keboola
  subscription_id = "eac4eb61-1abe-47e2-a0a1-f0a7e066f385" // Keboola DEV Connection Team
  features {}
}

locals {
  location = "North Europe"
}

data "azurerm_client_config" "current" {}

data "azurerm_key_vault_secret" "automation_certificate" {
  key_vault_id = azurerm_key_vault.synapse_shutdown_keyvault.id
  name         = azurerm_key_vault_certificate.automation_certificate.name
}

resource "azuread_application" "automation_application" {
  display_name = "${var.synapse_server_name}-Synapse-Shared-automation"
  owners = [
    data.azurerm_client_config.current.object_id
  ]
}

resource "azuread_application_certificate" "automation_application_certificate" {
  application_object_id = azuread_application.automation_application.id
  type                  = "AsymmetricX509Cert"
  value                 = azurerm_key_vault_certificate.automation_certificate.certificate_data_base64
}

resource "azurerm_automation_account" "synapse_automation_account" {
  resource_group_name = var.resource_group
  name                = "${var.resource_group}-Shared-automation"
  sku_name            = "Basic"
  location            = local.location
}

resource "azurerm_automation_runbook" "synapse_shutdown_automation_runbook" {
  name                    = "StopSynapseServer"
  automation_account_name = azurerm_automation_account.synapse_automation_account.name
  log_progress            = "false"
  log_verbose             = "false"
  resource_group_name     = var.resource_group
  location                = local.location
  runbook_type            = "PowerShell"
  content = templatefile("./runbooks/StopSynapseServer.ps1", {
    connection_name     = azuread_application.automation_application.display_name,
    resource_group      = var.resource_group,
    synapse_server_name = var.synapse_server_name
  })
}

resource "azurerm_automation_schedule" "synapse_shutdown_automation_schedule" {
  name                    = "StopSynapseServer"
  automation_account_name = azurerm_automation_account.synapse_automation_account.name
  resource_group_name     = var.resource_group
  frequency               = "Day"
  timezone                = "Europe/Prague"
}

resource "azurerm_automation_job_schedule" "synapse_shutdown_automation_runbook_schedule" {
  automation_account_name = azurerm_automation_account.synapse_automation_account.name
  resource_group_name     = var.resource_group
  runbook_name            = azurerm_automation_runbook.synapse_shutdown_automation_runbook.name
  schedule_name           = azurerm_automation_schedule.synapse_shutdown_automation_schedule.name
}

resource "azurerm_automation_certificate" "synapse_shutdown_automation_certificate" {
  automation_account_name = azurerm_automation_account.synapse_automation_account.name
  base64                  = data.azurerm_key_vault_secret.automation_certificate.value
  name                    = azuread_application.automation_application.display_name
  resource_group_name     = var.resource_group
}

resource "azurerm_automation_connection" "synapse_shutdown_automation_connection" {
  automation_account_name = azurerm_automation_account.synapse_automation_account.name
  name                    = azuread_application.automation_application.display_name
  resource_group_name     = var.resource_group
  type                    = "AzureServicePrincipal"
  values = {
    ApplicationId         = azuread_application.automation_application.application_id
    CertificateThumbprint = azurerm_automation_certificate.synapse_shutdown_automation_certificate.thumbprint
    SubscriptionId        = data.azurerm_client_config.current.subscription_id
    TenantId              = data.azurerm_client_config.current.tenant_id
  }
}

resource "azurerm_key_vault" "synapse_shutdown_keyvault" {
  location            = local.location
  name                = substr("${var.resource_group}-Shared-automation", 0, 24)
  resource_group_name = var.resource_group
  sku_name            = "standard"
  tenant_id           = data.azurerm_client_config.current.tenant_id
}

resource "azurerm_key_vault_access_policy" "synapse_shutdown_keyvault_current_user_policy" {
  key_vault_id = azurerm_key_vault.synapse_shutdown_keyvault.id
  object_id    = data.azurerm_client_config.current.object_id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  certificate_permissions = [
    "Create",
    "Delete",
    "Get",
    "List",
    "Purge",
    "Recover"
  ]
  secret_permissions = [
    "Get"
  ]
}

resource "azurerm_key_vault_certificate" "automation_certificate" {
  name         = "SharedAutomation"
  key_vault_id = azurerm_key_vault.synapse_shutdown_keyvault.id
  certificate_policy {
    issuer_parameters {
      name = "Self"
    }
    key_properties {
      exportable = true
      key_type   = "RSA"
      key_size   = 2048
      reuse_key  = false
    }
    secret_properties {
      content_type = "application/x-pkcs12"
    }
    lifetime_action {
      action {
        action_type = "EmailContacts"
      }
      trigger {
        days_before_expiry  = 0
        lifetime_percentage = 80
      }
    }
    x509_certificate_properties {
      key_usage = [
        "digitalSignature"
      ]
      subject            = "CN=${var.resource_group}"
      validity_in_months = 12
    }
  }
  depends_on = [
    azurerm_key_vault_access_policy.synapse_shutdown_keyvault_current_user_policy
  ]
}

output "application_account" {
  value = azuread_application.automation_application.display_name
}

output "runbook" {
  value = azurerm_automation_runbook.synapse_shutdown_automation_runbook.name
}

output "schedule" {
  value = azurerm_automation_job_schedule.synapse_shutdown_automation_runbook_schedule.schedule_name
}
