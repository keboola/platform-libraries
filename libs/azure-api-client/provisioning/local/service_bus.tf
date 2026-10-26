resource "azurerm_servicebus_namespace" "tests" {
  name                = "${var.name_prefix}-${local.app_name}-${local.resource_suffix}"
  location            = azurerm_resource_group.azure_api_client.location
  resource_group_name = azurerm_resource_group.azure_api_client.name
  sku                 = "Basic"
  tags                = local.resource_tags
}

resource "azurerm_servicebus_queue" "tests" {
  for_each = toset(["service-bus-tests", "event-grid-tests"])

  name         = each.key
  namespace_id = azurerm_servicebus_namespace.tests.id

  lock_duration = "PT30S"
  # leftovers of interrupted runs expire on their own
  default_message_ttl = "PT5M"
}

resource "azurerm_servicebus_namespace_authorization_rule" "tests" {
  name         = "tests"
  namespace_id = azurerm_servicebus_namespace.tests.id

  listen = true
  send   = true
  manage = false
}

// the roles a production sender/receiver gets, so the Entra token tests prove they are enough
resource "azurerm_role_assignment" "tests_service_bus" {
  for_each = toset(["Azure Service Bus Data Sender", "Azure Service Bus Data Receiver"])

  scope                = azurerm_servicebus_namespace.tests.id
  principal_id         = azuread_service_principal.azure_api_client.object_id
  role_definition_name = each.key
}
