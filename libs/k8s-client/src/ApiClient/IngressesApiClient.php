<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Ingress as IngressesApi;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\IngressList;

/**
 * @template-extends BaseNamespaceApiClient<IngressesApi, IngressList, Ingress>
 */
class IngressesApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new IngressesApi(),
            IngressList::class,
            Ingress::class,
        );
    }
}
