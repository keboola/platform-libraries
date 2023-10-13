resource "azurerm_servicebus_namespace" "testing_queue" {
  name                = "${var.name_prefix}-${local.app_name}"
  location            = azurerm_resource_group.messenger_bundle.location
  resource_group_name = azurerm_resource_group.messenger_bundle.name
  sku                 = "Basic"
}

resource "azurerm_servicebus_queue" "testing_queue" {
  name         = "${var.name_prefix}-${local.app_name}"
  namespace_id = azurerm_servicebus_namespace.testing_queue.id

  lock_duration = "PT30S"
}

resource "azurerm_servicebus_queue_authorization_rule" "testing_queue" {
  name     = "${var.name_prefix}-local-dev"
  queue_id = azurerm_servicebus_queue.testing_queue.id

  listen = true
  send   = true
  manage = false
}

output "az_servicebus_namespace" {
  value = azurerm_servicebus_namespace.testing_queue.name
}

output "az_servicebus_queue_name" {
  value = azurerm_servicebus_queue.testing_queue.name
}

output "az_servicebus_sas_key_name" {
  value = azurerm_servicebus_queue_authorization_rule.testing_queue.name
}

output "az_servicebus_sas_key_value" {
  value     = azurerm_servicebus_queue_authorization_rule.testing_queue.primary_key
  sensitive = true
}
