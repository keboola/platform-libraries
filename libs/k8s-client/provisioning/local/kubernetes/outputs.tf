output "namespace" {
  value     = kubernetes_namespace.k8s_client.metadata.0.name
  sensitive = true
}

output "user_token" {
  value     = kubernetes_secret.k8s_client.data["token"]
  sensitive = true
}
