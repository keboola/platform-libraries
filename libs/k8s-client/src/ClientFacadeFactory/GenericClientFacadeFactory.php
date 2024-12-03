<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ApiClient\IngressesApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumeClaimsApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumesApiClient;
use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\KubernetesApiClientFacade;
use KubernetesRuntime\Client;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;

class GenericClientFacadeFactory
{
    private RetryProxy $retryProxy;
    private LoggerInterface $logger;

    public function __construct(RetryProxy $retryProxy, LoggerInterface $logger)
    {
        $this->retryProxy = $retryProxy;
        $this->logger = $logger;
    }

    public function createClusterClient(
        string $apiUrl,
        string $token,
        string $caCertFile,
        string $namespace,
    ): KubernetesApiClientFacade {
        if (!is_file($caCertFile) || !is_readable($caCertFile)) {
            throw new ConfigurationException(sprintf(
                'Invalid K8S CA cert path "%s". File does not exist or can\'t be read.',
                $caCertFile,
            ));
        }

        Client::configure(
            $apiUrl,
            [
                'caCert' => $caCertFile,
                'token' => $token,
            ],
            [
                'connect_timeout' => '30',
                'timeout' => '60',
            ],
        );

        $apiClient = new KubernetesApiClient($this->retryProxy, $namespace);

        // all K8S API clients created here will use the configuration above, even if the Client is reconfigured later
        return new KubernetesApiClientFacade(
            $this->logger,
            new ConfigMapsApiClient($apiClient),
            new EventsApiClient($apiClient),
            new IngressesApiClient($apiClient),
            new PersistentVolumeClaimsApiClient($apiClient),
            new PersistentVolumesApiClient($apiClient),
            new PodsApiClient($apiClient, new PodWithLogStream()),
            new SecretsApiClient($apiClient),
            new ServicesApiClient($apiClient),
        );
    }
}
