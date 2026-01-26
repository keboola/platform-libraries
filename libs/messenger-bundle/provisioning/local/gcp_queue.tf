resource "google_pubsub_topic" "testing_queue" {
  name = "${var.name_prefix}-${local.app_name}"
}

resource "google_pubsub_subscription" "testing_queue" {
  name  = "${var.name_prefix}-${local.app_name}"
  topic = google_pubsub_topic.testing_queue.name

  ack_deadline_seconds       = 30
  message_retention_duration = "3600s"
  retain_acked_messages      = false
  enable_message_ordering    = false
}

resource "google_pubsub_topic_iam_binding" "messenger_bundle_iam" {
  topic = google_pubsub_topic.testing_queue.name
  role  = "roles/pubsub.publisher"

  members = [
    google_service_account.messenger_bundle.member,
  ]
}

resource "google_pubsub_subscription_iam_binding" "messenger_bundle_iam" {
  subscription = google_pubsub_subscription.testing_queue.name
  role         = "roles/pubsub.subscriber"

  members = [
    google_service_account.messenger_bundle.member,
  ]
}

output "gcp_topic_name" {
  value = google_pubsub_topic.testing_queue.name
}
