#
# Topics
#

resource "azurerm_eventgrid_topic" "topic_tests" {
  location            = azurerm_resource_group.api_client_tests.location
  name                = "tests-${local.resource_group_uuid}"
  resource_group_name = azurerm_resource_group.api_client_tests.name
  tags                = local.resource_tags
}

#
# Subscriptions
#

resource "azurerm_eventgrid_event_subscription" "subscription_tests" {
  name  = "TestSubscriber"
  scope = azurerm_eventgrid_topic.topic_tests.id
  service_bus_queue_endpoint_id = azurerm_servicebus_queue.queue["queue-tests"].id
  event_delivery_schema = "EventGridSchema"
}
