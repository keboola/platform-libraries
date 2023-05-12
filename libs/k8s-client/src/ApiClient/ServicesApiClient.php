<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Service as ServicesApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ServiceList;

/**
 * @template-extends BaseNamespaceApiClient<ServicesApi, ServiceList, Service>
 */
class ServicesApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new ServicesApi(),
            ServiceList::class,
            Service::class,
        );
    }
}
