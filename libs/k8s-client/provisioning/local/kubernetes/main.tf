terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.17"
    }
  }
}

resource "kubernetes_namespace" "k8s_client_account" {
  metadata {
    name = "${var.name_prefix}-k8s-client"
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

resource "kubernetes_role" "k8s_client" {
  metadata {
    namespace = kubernetes_namespace.k8s_client_account.metadata.0.name
    name      = "k8s-client"
  }

  rule {
    api_groups = [""]
    resources  = [
      "configmaps",
      "events",
      "persistentvolumeclaims",
      "pods",
      "secrets",
      "services",
    ]
    verbs      = ["get", "list", "delete", "create", "patch", "deletecollection"]
  }

  rule {
    api_groups = ["apps"]
    resources  = [
      "deployments",
    ]
    verbs      = ["get", "list", "delete", "create", "patch", "deletecollection"]
  }
}

resource "kubernetes_role_binding" "k8s_client" {
  metadata {
    namespace = kubernetes_namespace.k8s_client_account.metadata.0.name
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

  rule {
    api_groups = ["networking.k8s.io"]
    resources  = [
      "ingresses",
    ]
    verbs      = ["get", "list", "delete", "create", "patch", "deletecollection"]
  }

  rule {
    api_groups = [""]
    resources  = [
      "persistentvolumes",
    ]
    verbs      = ["get", "list", "delete", "create", "patch", "deletecollection"]
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
