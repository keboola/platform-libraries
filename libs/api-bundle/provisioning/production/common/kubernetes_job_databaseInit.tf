resource "kubernetes_job" "database_initialization" {
  wait_for_completion = true

  metadata {
    namespace = var.k8s_namespace
    name      = "vault-database-initialization"
    labels = {
      "app" = "vault-database-initialization"
    }
  }

  spec {
    template {
      metadata {
        labels = {
          "app" = "vault-database-initialization"
        }
      }

      spec {
        container {
          name  = "mysql"
          image = "mysql:8.0"
          command = ["bash", "-c", <<-EOT
            set -e;
            mysql --host="$${MYSQL_HOST}" --port="$${MYSQL_PORT}" --user="$${MASTER_USER}" --password="$${MASTER_PASSWORD}" --ssl-mode=VERIFY_IDENTITY --ssl-ca=/code/var/mysql/MySQLRootCACert.pem --execute "\
               CREATE DATABASE IF NOT EXISTS $${APP_DATABASE}; \
               CREATE USER IF NOT EXISTS $${APP_USER}@'%' IDENTIFIED BY '$${APP_PASSWORD}' REQUIRE SSL; \
               GRANT ALL ON $${APP_DATABASE}.* TO $${APP_USER}@'%'; \
            "
          EOT
          ]

          env_from {
            secret_ref {
              name = kubernetes_secret.database_master_config.metadata[0].name
            }
          }

          volume_mount {
            name       = "mysql-root-ca-cert"
            mount_path = "/code/var/mysql/"
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
