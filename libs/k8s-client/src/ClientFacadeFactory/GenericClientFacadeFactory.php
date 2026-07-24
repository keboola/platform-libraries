<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory;

use Keboola\K8sClient\ApiClient\ApiClientInterface;
use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ApiClient\IngressesApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumeClaimsApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumesApiClient;
use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use Keboola\K8sClient\ClientFacadeFactory\Token\TokenInterface;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\KubernetesApiClientFacade;
use KubernetesRuntime\AbstractModel;
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

    public function createApiClient(
        string $apiUrl,
        TokenInterface|string $token,
        string $caCertFile,
        string $namespace,
    ): KubernetesApiClient {
        ClientConfigurator::configureBaseClient($apiUrl, $caCertFile, $token);
        return new KubernetesApiClient($this->retryProxy, $namespace);
    }

    /**
     * @param array<class-string<AbstractModel>, ApiClientInterface<AbstractModel, AbstractModel>> $extraClients
     */
    public function createClusterClient(
        string $apiUrl,
        TokenInterface|string $token,
        string $caCertFile,
        string $namespace,
        array $extraClients = [],
    ): KubernetesApiClientFacade {
        $apiClient = $this->createApiClient($apiUrl, $token, $caCertFile, $namespace);

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
            $extraClients,
        );
    }
}
