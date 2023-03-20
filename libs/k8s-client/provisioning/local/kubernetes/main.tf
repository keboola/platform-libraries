terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.17"
    }
  }
}

# this is namespace where service account (and anything that should not be wiped by tests) exists
resource "kubernetes_namespace" "k8s_client_account" {
  metadata {
    name = "${var.name_prefix}-k8s-client-account"
  }
}

resource "kubernetes_service_account" "k8s_client" {
  metadata {
    namespace = kubernetes_namespace.k8s_client_account.metadata.0.name
    name      = "k8s-client"
  }
}

resource "kubernetes_secret" "k8s_client" {
  metadata {
    namespace   = kubernetes_namespace.k8s_client_account.metadata.0.name
    name        = "k8s-client"
    annotations = {
      "kubernetes.io/service-account.name" = kubernetes_service_account.k8s_client.metadata.0.name
    }
  }

  type = "kubernetes.io/service-account-token"
}

# this is namespace used by tests, can be completely purged by tests, except the role & role-binding
resource "kubernetes_namespace" "k8s_client_tests" {
  metadata {
    name = "${var.name_prefix}-k8s-client-tests"
  }
}

resource "kubernetes_role" "k8s_client" {
  metadata {
    namespace = kubernetes_namespace.k8s_client_tests.metadata.0.name
    name      = "k8s-client"
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["get", "list"]
  }

  rule {
    api_groups = [""]
    resources  = [
      "persistentvolumeclaims",
      "pods",
      "secrets",
    ]
    verbs      = ["*"]
  }
}

resource "kubernetes_role_binding" "k8s_client" {
  metadata {
    namespace = kubernetes_namespace.k8s_client_tests.metadata.0.name
    name      = "k8s-client"
  }

  role_ref {
    kind      = "Role"
    api_group = "rbac.authorization.k8s.io"
    name      = kubernetes_role.k8s_client.metadata.0.name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.k8s_client.metadata.0.name
    namespace = kubernetes_service_account.k8s_client.metadata.0.namespace
  }
}

resource "kubernetes_cluster_role" "k8s_client" {
  metadata {
    name = "${var.name_prefix}-k8s-client"
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["get", "list"]
  }
}

resource "kubernetes_cluster_role_binding" "k8s_client" {
  metadata {
    name = "${var.name_prefix}-k8s-client"
  }

  role_ref {
    kind      = "ClusterRole"
    api_group = "rbac.authorization.k8s.io"
    name      = kubernetes_cluster_role.k8s_client.metadata.0.name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.k8s_client.metadata.0.name
    namespace = kubernetes_service_account.k8s_client.metadata.0.namespace
  }
}
