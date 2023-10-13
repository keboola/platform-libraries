resource "aws_sqs_queue" "testing_queue" {
  name = "${var.name_prefix}-${local.app_name}"

  visibility_timeout_seconds = 30
}

data "aws_iam_policy_document" "testing_queue_access" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:GetQueueAttributes",
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [
      aws_sqs_queue.testing_queue.arn,
    ]
  }
}

resource "aws_iam_user_policy" "testing_queue_access" {
  user        = aws_iam_user.messenger_bundle.name
  name_prefix = "queue-access-"
  policy      = data.aws_iam_policy_document.testing_queue_access.json
}

output "aws_sqs_queue_url" {
  value = aws_sqs_queue.testing_queue.url
}
