resource "kubernetes_service" "vault_api" {
  metadata {
    namespace = local.k8s_namespace
    name      = "vault-api"
    labels = {
      app = "vault-api"
    }
  }

  spec {
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

resource "kubernetes_ingress_v1" "vault_api" {
  metadata {
    namespace = local.k8s_namespace
    name      = "vault-api"
    annotations = {
      "kubernetes.io/ingress.class" : "nginx"
    }
  }

  spec {
    rule {
      host = "vault.${var.hostname_suffix}"
      http {
        path {
          path_type = "Prefix"
          path      = "/"
          backend {
            service {
              name = kubernetes_service.vault_api.metadata[0].name
              port {
                number = kubernetes_service.vault_api.spec[0].port[0].port
              }
            }
          }
        }
      }
    }
  }
}
