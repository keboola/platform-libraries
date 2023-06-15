resource "kubernetes_job" "database_migration" {
  depends_on          = [kubernetes_job.database_initialization]
  wait_for_completion = true

  metadata {
    namespace = var.k8s_namespace
    name      = "vault-database-migration-${md5(var.release_id)}"
    labels = {
      "app"                        = "vault-database-migration"
      "tags.datadoghq.com/env"     = var.keboola_stack
      "tags.datadoghq.com/service" = "vault-database-migration"
      "tags.datadoghq.com/version" = "${var.app_image_tag}.${var.release_id}"
    }
  }

  spec {
    template {
      metadata {
        labels = {
          "app"                        = "vault-database-migration"
          "tags.datadoghq.com/env"     = var.keboola_stack
          "tags.datadoghq.com/service" = "vault-database-migration"
          "tags.datadoghq.com/version" = "${var.app_image_tag}.${var.release_id}"
        }
        annotations = {
          "log" = true
        }
      }

      spec {
        container {
          name  = "app"
          image = "${var.app_image_name}:${var.app_image_tag}"

          env_from {
            secret_ref {
              name = kubernetes_secret.app_secret.metadata.0.name
            }
          }

          volume_mount {
            mount_path = "/code/var/mysql"
            name       = "mysql-root-ca-cert"
          }
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

        // https://keboola.atlassian.net/wiki/spaces/TECH/pages/1453785161/Service+requirements#Jak-um%C3%ADstit-pod-do-node-poolu
        toleration {
          key      = "app"
          operator = "Exists"
          effect   = "NoSchedule"
        }

        node_selector = {
          "nodepool" = "main"
        }
      }
    }
  }
}
