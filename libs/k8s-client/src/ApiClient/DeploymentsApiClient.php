<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Deployment as DeploymentsApi;
use Kubernetes\Model\Io\K8s\Api\Apps\V1\Deployment;
use Kubernetes\Model\Io\K8s\Api\Apps\V1\DeploymentList;

/**
 * @template-extends BaseNamespaceApiClient<DeploymentsApi, DeploymentList, Deployment>
 */
class DeploymentsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new DeploymentsApi(),
            DeploymentList::class,
            Deployment::class,
        );
    }
}
