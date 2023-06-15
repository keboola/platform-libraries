resource "kubernetes_service_account" "vault" {
  metadata {
    namespace = local.k8s_namespace
    name      = local.k8s_service_account_name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.vault.arn
    }
  }
}
