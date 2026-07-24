<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClient;
use Psr\Log\LoggerInterface;

class AutoDetectKubernetesApiClientFactory implements KubernetesApiClientFactory
{
    public function __construct(
        private readonly EnvVariablesKubernetesApiClientFactory $envVariablesFactory,
        private readonly InClusterKubernetesApiClientFactory $inClusterFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function createApiClient(?string $namespace = null): KubernetesApiClient
    {
        if ($this->envVariablesFactory->isAvailable($namespace)) {
            $this->logger->debug('Using ENV variables configuration for K8S client.');
            return $this->envVariablesFactory->createApiClient($namespace);
        }

        // preserve original AutoDetect semantics: the in-cluster branch checks the namespace file too
        // (unlike the env branch), so it is called without the caller-supplied namespace
        if ($this->inClusterFactory->isAvailable()) {
            $this->logger->debug('Using in-cluster configuration for K8S client.');
            return $this->inClusterFactory->createApiClient($namespace);
        }

        throw new ConfigurationException('No valid K8S client configuration found.');
    }
}
