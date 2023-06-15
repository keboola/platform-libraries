resource "kubernetes_service" "vault_api" {
  metadata {
    namespace = local.k8s_namespace
    name      = "vault-api"
    annotations = {
      "service.beta.kubernetes.io/azure-load-balancer-internal" : "true"
    }
    labels = {
      app = "vault-api"
    }
  }

  spec {
    type = "LoadBalancer"
    selector = {
      app = "vault-api"
    }
    port {
      name        = "http"
      protocol    = "TCP"
      port        = 80
      target_port = 8080
    }
  }
}
