output "az_tenant_id" {
  value = var.az_tenant_id
}

output "az_application_id" {
  value = azuread_application.azure_api_client.client_id
}

output "az_application_secret" {
  value     = azuread_service_principal_password.azure_api_client.value
  sensitive = true
}

output "az_servicebus_endpoint" {
  value = azurerm_servicebus_namespace.tests.endpoint
}

output "az_servicebus_sas_key_name" {
  value = azurerm_servicebus_namespace_authorization_rule.tests.name
}

output "az_servicebus_sas_key_value" {
  value     = azurerm_servicebus_namespace_authorization_rule.tests.primary_key
  sensitive = true
}

output "az_servicebus_queue_name" {
  value = azurerm_servicebus_queue.tests["service-bus-tests"].name
}

output "az_eventgrid_topic_hostname" {
  value = regex("^https://([^/]+)/", azurerm_eventgrid_topic.tests.endpoint)[0]
}

output "az_eventgrid_topic_name" {
  value = azurerm_eventgrid_topic.tests.name
}

output "az_eventgrid_access_key" {
  value     = azurerm_eventgrid_topic.tests.primary_access_key
  sensitive = true
}

output "az_eventgrid_queue_name" {
  value = azurerm_servicebus_queue.tests["event-grid-tests"].name
}
