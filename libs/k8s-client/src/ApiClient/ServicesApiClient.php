<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use BadMethodCallException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Service as ServicesApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ServiceList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;

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

    public function deleteCollection(?DeleteOptions $options = null, array $queries = []): Status
    {
        // override parent method because deleteCollection is not implemented in lastest version of k8s-client
        throw new BadMethodCallException('DeleteCollection is not yet implemented for "Service" resource');
    }
}
