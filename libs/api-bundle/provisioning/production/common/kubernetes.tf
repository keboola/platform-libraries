resource "random_string" "app_secret" {
  length  = 32
  special = false
}

resource "random_password" "mysql_password" {
  length = 32
}

resource "kubernetes_secret" "database_master_config" {
  metadata {
    namespace = var.k8s_namespace
    name      = "vault-master-database-config"
  }

  data = {
    MYSQL_HOST = var.mysql_host
    MYSQL_PORT = var.mysql_port

    MASTER_USER     = var.mysql_master_user
    MASTER_PASSWORD = var.mysql_master_password

    APP_USER     = "vault"
    APP_PASSWORD = random_password.mysql_password.result
    APP_DATABASE = "vault"
  }
}

resource "kubernetes_secret" "app_secret" {
  metadata {
    namespace = var.k8s_namespace
    name      = "vault-app-secret"
  }

  data = {
    APP_ENV    = "prod"
    APP_SECRET = random_string.app_secret.result

    STORAGE_API_URL = var.storage_api_url

    DATABASE_URL = format(
      "mysql://%s:%s@%s:%s/%s",
      urlencode(kubernetes_secret.database_master_config.data.APP_USER),
      urlencode(kubernetes_secret.database_master_config.data.APP_PASSWORD),
      urlencode(kubernetes_secret.database_master_config.data.MYSQL_HOST),
      urlencode(kubernetes_secret.database_master_config.data.MYSQL_PORT),
      urlencode(kubernetes_secret.database_master_config.data.APP_DATABASE),
    )
    DATABASE_SSL_VERIFY_ENABLE = "1"
    DATABASE_SSL_CA_PATH       = "/code/var/mysql/MySQLRootCACert.pem"
  }
}
