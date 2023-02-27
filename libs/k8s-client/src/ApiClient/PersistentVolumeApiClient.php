<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\PersistentVolume as PersistentVolumeApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolume;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeList;

/**
 * @template-extends BaseApiClient<PersistentVolumeApi, PersistentVolumeList, PersistentVolume>
 */
class PersistentVolumeApiClient extends BaseApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new PersistentVolumeApi(),
            PersistentVolumeList::class,
            PersistentVolume::class,
        );
    }
}
