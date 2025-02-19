#
# Service bus
#

resource "azurerm_servicebus_namespace" "queues" {
  location            = azurerm_resource_group.api_client_tests.location
  name                = "queue-${local.resource_group_uuid}"
  resource_group_name = azurerm_resource_group.api_client_tests.name
  sku                 = "Basic"
  tags                = local.resource_tags
}

#
# Queues
#

resource "azurerm_servicebus_queue" "queue" {
  for_each = {
    "queue-tests" = {
      lock_duration : "PT5S"
    }
  }
  name                                    = each.key
  namespace_id                            = azurerm_servicebus_namespace.queues.id
  lock_duration                           = each.value.lock_duration
  max_size_in_megabytes                   = "1024"
  requires_duplicate_detection            = false
  requires_session                        = false
  default_message_ttl                     = "PT10S"
  dead_lettering_on_message_expiration    = false
  duplicate_detection_history_time_window = "PT10S"
  enable_batched_operations               = true
  max_delivery_count                      = "10"
  status                                  = "Active"
  enable_partitioning                     = false
  enable_express                          = false
}
