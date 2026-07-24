<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ApiClient\ApiClientInterface;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use KubernetesRuntime\AbstractModel;
use Psr\Log\LoggerInterface;

class AutoDetectClientFacadeFactory
{
    public function __construct(
        private readonly EnvVariablesClientFacadeFactory $envVariablesFactory,
        private readonly InClusterClientFacadeFactory $inClusterFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param array<class-string<AbstractModel>, ApiClientInterface<AbstractModel, AbstractModel>> $extraClients
     */
    public function createClusterClient(?string $namespace = null, array $extraClients = []): KubernetesApiClientFacade
    {
        return
            $this->tryCreateClientFromEnv($namespace, $extraClients) ??
            $this->tryCreateClientFromInCluster($namespace, $extraClients) ??
            throw new ConfigurationException('No valid K8S client configuration found.')
        ;
    }

    /**
     * @param array<class-string<AbstractModel>, ApiClientInterface<AbstractModel, AbstractModel>> $extraClients
     */
    private function tryCreateClientFromEnv(?string $namespace, array $extraClients): ?KubernetesApiClientFacade
    {
        if (!$this->envVariablesFactory->isAvailable($namespace)) {
            return null;
        }

        $this->logger->debug('Using ENV variables configuration for K8S client.');
        return $this->envVariablesFactory->createClusterClient($namespace, $extraClients);
    }

    /**
     * @param array<class-string<AbstractModel>, ApiClientInterface<AbstractModel, AbstractModel>> $extraClients
     */
    private function tryCreateClientFromInCluster(?string $namespace, array $extraClients): ?KubernetesApiClientFacade
    {
        if (!$this->inClusterFactory->isAvailable()) {
            return null;
        }

        $this->logger->debug('Using in-cluster configuration for K8S client.');
        return $this->inClusterFactory->createClusterClient($namespace, $extraClients);
    }
}
