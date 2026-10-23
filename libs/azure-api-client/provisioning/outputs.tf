// output names are the environment variables the "azure" test group reads, see write-env.sh

output "AZURE_API_CLIENT_TEST__SERVICE_BUS_ENDPOINT" {
  value = azurerm_servicebus_namespace.tests.endpoint
}

output "AZURE_API_CLIENT_TEST__SERVICE_BUS_SAS_KEY_NAME" {
  value = azurerm_servicebus_namespace_authorization_rule.tests.name
}

output "AZURE_API_CLIENT_TEST__SERVICE_BUS_SAS_KEY" {
  value     = azurerm_servicebus_namespace_authorization_rule.tests.primary_key
  sensitive = true
}

output "AZURE_API_CLIENT_TEST__SERVICE_BUS_QUEUE" {
  value = azurerm_servicebus_queue.tests["service-bus-tests"].name
}

output "AZURE_API_CLIENT_TEST__EVENT_GRID_TOPIC_HOSTNAME" {
  value = regex("^https://([^/]+)/", azurerm_eventgrid_topic.tests.endpoint)[0]
}

output "AZURE_API_CLIENT_TEST__EVENT_GRID_TOPIC_NAME" {
  value = azurerm_eventgrid_topic.tests.name
}

output "AZURE_API_CLIENT_TEST__EVENT_GRID_ACCESS_KEY" {
  value     = azurerm_eventgrid_topic.tests.primary_access_key
  sensitive = true
}

output "AZURE_API_CLIENT_TEST__EVENT_GRID_QUEUE" {
  value = azurerm_servicebus_queue.tests["event-grid-tests"].name
}
