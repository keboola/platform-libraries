<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

use Keboola\K8sClient\ClientFactory\Token\InClusterToken;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClient;
use Retry\RetryProxy;

class InClusterKubernetesApiClientFactory implements KubernetesApiClientFactory
{
    private const IN_CLUSTER_AUTH_PATH = '/var/run/secrets/kubernetes.io/serviceaccount';
    private const IN_CLUSTER_API_URL = 'https://kubernetes.default.svc';

    public function __construct(
        private readonly RetryProxy $retryProxy,
        private readonly string $credentialsPath = self::IN_CLUSTER_AUTH_PATH,
    ) {
    }

    public function isAvailable(?string $namespace = null): bool
    {
        try {
            $this->findInClusterConfigFile('token');
            $this->findInClusterConfigFile('ca.crt');
            $namespace ?? $this->findInClusterConfigFile('namespace');

            return true;
        } catch (ConfigurationException) {
            return false;
        }
    }

    public function createApiClient(?string $namespace = null): KubernetesApiClient
    {
        ClientConfigurator::configureBaseClient(
            self::IN_CLUSTER_API_URL,
            $this->findInClusterConfigFile('ca.crt'),
            new InClusterToken($this->findInClusterConfigFile('token')),
        );

        return new KubernetesApiClient(
            $this->retryProxy,
            $namespace ?? $this->readInClusterConfigFile('namespace'),
        );
    }

    private function readInClusterConfigFile(string $file): string
    {
        $filePath = $this->findInClusterConfigFile($file);
        $fileContents = @file_get_contents($filePath);

        if ($fileContents === false) {
            throw new ConfigurationException(sprintf(
                'Failed to read contents of in-cluster configuration file "%s"',
                $filePath,
            ));
        }

        return $fileContents;
    }

    private function findInClusterConfigFile(string $file): string
    {
        $filePath = $this->credentialsPath.'/'.$file;

        if (!file_exists($filePath)) {
            throw new ConfigurationException(sprintf(
                'In-cluster configuration file "%s" does not exist',
                $filePath,
            ));
        }

        return $filePath;
    }
}
