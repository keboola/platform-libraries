<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\Token\InClusterToken;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;

class InClusterClientFacadeFactory
{
    private const IN_CLUSTER_AUTH_PATH = '/var/run/secrets/kubernetes.io/serviceaccount';
    private const IN_CLUSTER_API_URL = 'https://kubernetes.default.svc';

    private GenericClientFacadeFactory $genericFactory;
    private string $credentialsPath;

    public function __construct(
        GenericClientFacadeFactory $genericFactory,
        string $credentialsPath = self::IN_CLUSTER_AUTH_PATH,
    ) {
        $this->genericFactory = $genericFactory;
        $this->credentialsPath = $credentialsPath;
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

    public function createClusterClient(?string $namespace = null): KubernetesApiClientFacade
    {
        return $this->genericFactory->createClusterClient(
            self::IN_CLUSTER_API_URL,
            new InClusterToken($this->findInClusterConfigFile('token')),
            $this->findInClusterConfigFile('ca.crt'),
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
