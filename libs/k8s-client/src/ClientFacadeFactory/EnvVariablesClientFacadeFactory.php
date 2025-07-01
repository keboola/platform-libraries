<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\Token\StaticToken;
use Keboola\K8sClient\KubernetesApiClientFacade;
use RuntimeException;

class EnvVariablesClientFacadeFactory
{
    public function __construct(
        private readonly GenericClientFacadeFactory $genericFactory,
    ) {
    }

    public function isAvailable(?string $namespace = null): bool
    {
        [$k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace] = $this->loadEnvValues($namespace);

        return $k8sHost !== null && $k8sToken !== null && $k8sCaCertPath !== null && $k8sNamespace !== null;
    }

    public function createClusterClient(?string $namespace = null): KubernetesApiClientFacade
    {
        [$k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace] = $this->loadEnvValues($namespace);
        if ($k8sHost === null || $k8sToken === null || $k8sCaCertPath === null || $k8sNamespace === null) {
            throw new RuntimeException(
                'Configuration is not complete. Use isAvailable() to check if the factory can be used.',
            );
        }

        return $this->genericFactory->createClusterClient(
            $k8sHost,
            new StaticToken($k8sToken),
            $k8sCaCertPath,
            $namespace ?? $k8sNamespace,
        );
    }

    /**
     * @return array{?string, ?string, ?string, ?string}
     */
    private function loadEnvValues(?string $namespace = null): array
    {
        $k8sHost = self::getEnv('K8S_HOST');
        $k8sToken = self::getEnv('K8S_TOKEN');
        $k8sCaCertPath = self::getEnv('K8S_CA_CERT_PATH');
        $k8sNamespace = $namespace ?? self::getEnv('K8S_NAMESPACE');

        return [$k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace];
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
