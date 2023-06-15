data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_cloudformation_stack" "kbc_vpc" {
  name = "kbc-vpc"
}

data "aws_cloudformation_stack" "kbc_job_queue_rds" {
  name = "kbc-job-queue-rds"
}

data "aws_ssm_parameter" "rds_master_password" {
  name = "/keboola/${var.keboola_stack}/global/JOB_QUEUE_RDS_MASTER_PASSWORD"
}

locals {
  aws_name_prefix = "kbc"
  oidc_provider   = trimprefix(data.aws_eks_cluster.k8s.identity.0.oidc.0.issuer, "https://")
}

// application role
data "aws_iam_policy_document" "vault_oidc" {
  statement {
    effect = "Allow"
    principals {
      type = "Federated"
      identifiers = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_provider}"
      ]
    }

    actions = [
      "sts:AssumeRoleWithWebIdentity",
    ]

    condition {
      variable = "${local.oidc_provider}:sub"
      test     = "StringEquals"
      values = [
        format(
          "system:serviceaccount:%s:%s",
          local.k8s_namespace,
          local.k8s_service_account_name
        )
      ]
    }
  }
}

resource "aws_iam_role" "vault" {
  name_prefix        = "${local.aws_name_prefix}-vault-"
  assume_role_policy = data.aws_iam_policy_document.vault_oidc.json
}
