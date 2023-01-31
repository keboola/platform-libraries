# cluster `dev-ps-eu-central-1` requires specific role
provider "aws" {
  alias   = "aws_k8s_cluster"
  profile = "Keboola-Dev-Platform-Services-AWSAdministratorAccess"
  region  = "eu-central-1"

  assume_role {
    // https://keboola.atlassian.net/wiki/spaces/KB/pages/1885863951/EKS+cluster+s+p+stupem+do+centralizovan+ho+ECR#Cluster-dev-ps-eu-central-1%3A
    role_arn     = "arn:aws:iam::025303414634:role/dev-ps-eu-central-1-admin20220407124852230600000001"
    session_name = "terraform"
  }
}

data "aws_eks_cluster" "current" {
  name = "dev-ps-eu-central-1"
}

data "aws_eks_cluster_auth" "current" {
  provider = aws.aws_k8s_cluster
  name     = data.aws_eks_cluster.current.name
}

provider "kubernetes" {
  alias                  = "aws_k8s_cluster"
  host                   = data.aws_eks_cluster.current.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.current.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.current.token
}

module "aws_kubernetes" {
  source = "./kubernetes"
  providers = {
    kubernetes = kubernetes.aws_k8s_cluster
  }

  name_prefix = var.name_prefix
}

output "aws_eks_host" {
  value = data.aws_eks_cluster.current.endpoint
}

output "aws_eks_ca_certificate" {
  value = base64decode(data.aws_eks_cluster.current.certificate_authority[0].data)
}

output "aws_eks_token" {
  value     = module.aws_kubernetes.user_token
  sensitive = true
}

output "aws_eks_namespace" {
  value     = module.aws_kubernetes.namespace
  sensitive = true
}
