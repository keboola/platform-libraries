locals {
  gcp_project = "kbc-ci-platform-services"
  gcp_region = "us-central1"
}

provider "google" {
  project = local.gcp_project
}

resource "google_service_account" "messenger_bundle" {
  account_id   = "${var.name_prefix}-${local.app_name}"
  display_name = "${var.name_prefix} ${local.app_display_name}"
}

resource "google_service_account_key" "messenger_bundle" {
  service_account_id = google_service_account.messenger_bundle.name
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

output "gcp_application_credentials" {
  value     = google_service_account_key.messenger_bundle.private_key
  sensitive = true
}
