<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

use Keboola\K8sClient\ClientFactory\Token\StaticToken;
use Keboola\K8sClient\KubernetesApiClient;
use Retry\RetryProxy;
use RuntimeException;

class EnvVariablesKubernetesApiClientFactory implements KubernetesApiClientFactory
{
    public function __construct(
        private readonly RetryProxy $retryProxy,
    ) {
    }

    public function isAvailable(?string $namespace = null): bool
    {
        [$k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace] = $this->loadEnvValues($namespace);

        return $k8sHost !== null && $k8sToken !== null && $k8sCaCertPath !== null && $k8sNamespace !== null;
    }

    public function createApiClient(?string $namespace = null): KubernetesApiClient
    {
        [$k8sHost, $k8sToken, $k8sCaCertPath, $k8sNamespace] = $this->loadEnvValues($namespace);
        if ($k8sHost === null || $k8sToken === null || $k8sCaCertPath === null || $k8sNamespace === null) {
            throw new RuntimeException(
                'Configuration is not complete. Use isAvailable() to check if the factory can be used.',
            );
        }

        ClientConfigurator::configureBaseClient($k8sHost, $k8sCaCertPath, new StaticToken($k8sToken));

        return new KubernetesApiClient($this->retryProxy, $namespace ?? $k8sNamespace);
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
