<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\ConfigMap as ConfigMapsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMap;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMapList;

/**
 * @template-extends BaseNamespaceApiClient<ConfigMapsApi, ConfigMapList, ConfigMap>
 */
class ConfigMapsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new ConfigMapsApi(),
            ConfigMapList::class,
            ConfigMap::class,
        );
    }
}
