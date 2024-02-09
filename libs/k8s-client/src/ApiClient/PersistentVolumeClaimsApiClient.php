<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\PersistentVolumeClaim as PersistentVolumeClaimApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeClaim;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeClaimList;

/**
 * @template-extends BaseNamespaceApiClient<PersistentVolumeClaimApi, PersistentVolumeClaimList, PersistentVolumeClaim>
 */
class PersistentVolumeClaimsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new PersistentVolumeClaimApi(),
            PersistentVolumeClaimList::class,
            PersistentVolumeClaim::class,
        );
    }
}
