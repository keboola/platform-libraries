<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFactory;

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
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\KubernetesApiClientFacade;
use KubernetesRuntime\AbstractModel;
use Psr\Log\LoggerInterface;

/**
 * Assembles a KubernetesApiClientFacade from a single, already-configured KubernetesApiClient.
 *
 * Unlike the client factories (which differ in how they resolve credentials), this factory is universal: it is
 * used the same way regardless of which KubernetesApiClientFactory produced the underlying client.
 */
class KubernetesApiClientFacadeFactory
{
    public function __construct(
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param array<class-string<AbstractModel>, ApiClientInterface<AbstractModel, AbstractModel>> $extraClients
     */
    public function create(KubernetesApiClient $apiClient, array $extraClients = []): KubernetesApiClientFacade
    {
        // all K8S API clients created here will use the configuration active at the time $apiClient was built,
        // even if the underlying Client is reconfigured later
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
