<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Psr\Log\LoggerInterface;

class AutoDetectClientFacadeFactory
{
    public function __construct(
        private readonly EnvVariablesClientFacadeFactory $envVariablesFactory,
        private readonly InClusterClientFacadeFactory $inClusterFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function createClusterClient(?string $namespace = null): KubernetesApiClientFacade
    {
        return
            $this->tryCreateClientFromEnv($namespace) ??
            $this->tryCreateClientFromInCluster($namespace) ??
            throw new ConfigurationException('No valid K8S client configuration found.')
        ;
    }

    private function tryCreateClientFromEnv(?string $namespace): ?KubernetesApiClientFacade
    {
        if (!$this->envVariablesFactory->isAvailable($namespace)) {
            return null;
        }

        $this->logger->debug('Using ENV variables configuration for K8S client.');
        return $this->envVariablesFactory->createClusterClient($namespace);
    }

    private function tryCreateClientFromInCluster(?string $namespace): ?KubernetesApiClientFacade
    {
        if (!$this->inClusterFactory->isAvailable()) {
            return null;
        }

        $this->logger->debug('Using in-cluster configuration for K8S client.');
        return $this->inClusterFactory->createClusterClient($namespace);
    }
}
