resource "kubernetes_deployment" "vault_api" {
  depends_on = [kubernetes_job.database_migration]

  metadata {
    namespace = var.k8s_namespace
    name      = "vault-api"

    labels = {
      "app"                        = "vault-api"
      "tags.datadoghq.com/env"     = var.keboola_stack
      "tags.datadoghq.com/service" = "vault-api"
      "tags.datadoghq.com/version" = "${var.app_image_tag}.${var.release_id}"
    }
  }

  spec {
    replicas = 2
    selector {
      match_labels = {
        app = "vault-api"
      }
    }

    template {
      metadata {
        labels = {
          "app"                        = "vault-api"
          "tags.datadoghq.com/env"     = var.keboola_stack
          "tags.datadoghq.com/service" = "vault-api"
          "tags.datadoghq.com/version" = "${var.app_image_tag}.${var.release_id}"
        }
        annotations = {
          "log" = true
        }
      }

      spec {
        service_account_name = var.k8s_service_account_name

        container {
          name  = "app"
          image = "${var.app_image_name}:${var.app_image_tag}"

          env_from {
            secret_ref {
              name = kubernetes_secret.app_secret.metadata.0.name
            }
          }

          env {
            name = "DD_AGENT_HOST"
            value_from {
              field_ref {
                field_path = "status.hostIP"
              }
            }
          }

          env {
            name = "DD_ENV"
            value_from {
              field_ref {
                field_path = "metadata.labels['tags.datadoghq.com/env']"
              }
            }
          }

          env {
            name = "DD_SERVICE"
            value_from {
              field_ref {
                field_path = "metadata.labels['tags.datadoghq.com/service']"
              }
            }
          }

          env {
            name = "DD_VERSION"
            value_from {
              field_ref {
                field_path = "metadata.labels['tags.datadoghq.com/version']"
              }
            }
          }

          env {
            name  = "DD_TRACE_ANALYTICS_ENABLED"
            value = "true"
          }

          env {
            name  = "DD_PROFILING_ENABLED"
            value = "false"
          }

          port {
            container_port = 8080
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "64M"
            }

            limits = {
              cpu    = "1"
              memory = "256M"
            }
          }

          readiness_probe {
            http_get {
              path = "/health-check"
              port = "8080"
            }
            initial_delay_seconds = 10
            period_seconds        = 5
            success_threshold     = 1
            failure_threshold     = 3
          }

          volume_mount {
            mount_path = "/code/var/mysql"
            name       = "mysql-root-ca-cert"
          }
        }

        // https://keboola.atlassian.net/wiki/spaces/TECH/pages/1453785161/Service+requirements#Jak-um%C3%ADstit-pod-do-node-poolu
        toleration {
          key      = "app"
          operator = "Exists"
          effect   = "NoSchedule"
        }

        node_selector = {
          "nodepool" = "main"
        }

        volume {
          name = "mysql-root-ca-cert"
          config_map {
            name = "mysql-root-ca-cert"
            items {
              key  = "MySQLRootCACert.pem"
              path = "MySQLRootCACert.pem"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_pod_disruption_budget_v1" "vault_api" {
  metadata {
    namespace = var.k8s_namespace
    name      = "vault-api-pdb"
  }
  spec {
    min_available = 1
    selector {
      match_labels = {
        "app" = "vault-api-pdb"
      }
    }
  }
}
