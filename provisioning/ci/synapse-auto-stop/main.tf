terraform {
  required_providers {
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.33.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.41.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0.4"
    }
  }
}

variable "resource_group" {
  type        = string
  description = "Azure Resource Group of Synapse Warehouse"
  validation {
    condition     = length(var.resource_group) > 0
    error_message = "The \"resource_group\" must be non-empty string."
  }
}

variable "synapse_server_name" {
  type = string
  validation {
    condition     = length(var.synapse_server_name) > 0
    error_message = "The \"synapse_server_name\" must be non-empty string."
  }
}
