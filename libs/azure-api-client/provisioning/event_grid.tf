resource "azurerm_eventgrid_topic" "tests" {
  name                = "${var.name_prefix}-${local.app_name}-${local.resource_suffix}"
  location            = azurerm_resource_group.tests.location
  resource_group_name = azurerm_resource_group.tests.name
  tags                = local.resource_tags
}

resource "azurerm_eventgrid_event_subscription" "tests" {
  name                          = "service-bus-queue"
  scope                         = azurerm_eventgrid_topic.tests.id
  service_bus_queue_endpoint_id = azurerm_servicebus_queue.tests["event-grid-tests"].id
  event_delivery_schema         = "EventGridSchema"
}

resource "azurerm_role_assignment" "tests_event_grid_data_sender" {
  scope                = azurerm_eventgrid_topic.tests.id
  principal_id         = var.test_principal_object_id
  role_definition_name = "EventGrid Data Sender"
}
