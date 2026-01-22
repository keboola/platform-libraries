locals {
  gcp_project = "kbc-dev-platform-services"
  gcp_region = "europe-west1"
}

provider "google" {
  project = local.gcp_project
}

resource "google_service_account" "messenger_bundle" {
  account_id   = "${var.name_prefix}-${local.app_name}"
  display_name = "${var.name_prefix} ${local.app_display_name}"
}

resource "time_sleep" "wait_for_service_account" {
  depends_on = [google_service_account.messenger_bundle]

  create_duration = "30s"
}

resource "google_service_account_key" "messenger_bundle" {
  depends_on = [time_sleep.wait_for_service_account]

  service_account_id = google_service_account.messenger_bundle.name
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

output "gcp_application_credentials" {
  value     = google_service_account_key.messenger_bundle.private_key
  sensitive = true
}
