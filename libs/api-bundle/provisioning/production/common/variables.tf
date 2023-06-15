variable "k8s_namespace" {
  type     = string
  nullable = false
}

variable "k8s_service_account_name" {
  type     = string
  nullable = false
}

variable "storage_api_url" {
  type     = string
  nullable = false
}

variable "keboola_stack" {
  type     = string
  nullable = false
}

variable "release_id" {
  type     = string
  nullable = false
}

variable "app_image_name" {
  type     = string
  nullable = false
}

variable "app_image_tag" {
  type     = string
  nullable = false
}

variable "mysql_host" {
  type     = string
  nullable = false
}

variable "mysql_port" {
  type     = string
  nullable = false
}

variable "mysql_master_user" {
  type     = string
  nullable = false
}

variable "mysql_master_password" {
  type     = string
  nullable = false
}
