<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;

class AutoDetectClientFacadeFactory
{
    public function __construct(
        private readonly GenericClientFacadeFactory $genericFactory,
        private readonly InClusterClientFacadeFactory $inClusterFactory,
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
        $k8sHost = self::getEnv('K8S_HOST');
        $k8sToken = self::getEnv('K8S_TOKEN');
        $k8sCaCertPath = self::getEnv('K8S_CA_CERT_PATH');
        $k8sNamespace = $namespace ?? self::getEnv('K8S_NAMESPACE');

        if ($k8sHost === null || $k8sToken === null || $k8sCaCertPath === null || $k8sNamespace === null) {
            return null;
        }

        return $this->genericFactory->createClusterClient($k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace);
    }

    private function tryCreateClientFromInCluster(?string $namespace): ?KubernetesApiClientFacade
    {
        try {
            return $this->inClusterFactory->createClusterClient($namespace);
        } catch (ConfigurationException) {
            return null;
        }
    }

    /**
     * @param non-empty-string $envName
     * @return non-empty-string|null
     */
    private static function getEnv(string $envName): ?string
    {
        return ((string) getenv($envName)) ?: null;
    }
}
