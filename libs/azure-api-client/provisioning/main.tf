terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.39"
    }
  }
}

variable "name_prefix" {
  type        = string
  description = "Prefix of all created resources, use your nickname for a personal environment"
}

variable "az_tenant_id" {
  type        = string
  description = "Entra tenant of the subscription"
}

variable "az_subscription_id" {
  type        = string
  description = "Subscription to create the resources in"
}

variable "test_principal_object_id" {
  type        = string
  description = "Object id of the identity running the tests (your user, or the CI service principal); it gets the data plane roles"
}

variable "az_location" {
  type        = string
  description = "Azure region of the resources, e.g. \"westeurope\""
}

provider "azurerm" {
  features {}
  tenant_id       = var.az_tenant_id
  subscription_id = var.az_subscription_id
}

locals {
  app_name = "azure-api-client"
  # Service Bus namespace names are global and allow only letters, digits and hyphens
  resource_suffix = substr(md5(azurerm_resource_group.tests.id), 0, 12)
  resource_tags = {
    service = local.app_name
    env     = var.name_prefix
  }
}

resource "azurerm_resource_group" "tests" {
  name     = "${var.name_prefix}-${local.app_name}"
  location = var.az_location
  tags     = local.resource_tags
}
