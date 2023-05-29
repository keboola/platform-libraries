<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Secret as SecretsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Api\Core\V1\SecretList;

/**
 * @template-extends BaseNamespaceApiClient<SecretsApi, SecretList, Secret>
 */
class SecretsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new SecretsApi(),
            SecretList::class,
            Secret::class,
        );
    }
}
