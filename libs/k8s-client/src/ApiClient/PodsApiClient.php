<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\Exception\KubernetesResponseException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Pod as PodsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PodList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;

/**
 * @template-implements ApiClientInterface<PodList, Pod>
 */
class PodsApiClient implements ApiClientInterface
{
    private KubernetesApiClient $apiClient;
    private PodsApi $baseApi;

    public function __construct(KubernetesApiClient $apiClient)
    {
        $this->apiClient = $apiClient;
        $this->baseApi = new PodsApi();
    }

    public function list(array $queries = []): PodList
    {
        return $this->apiClient->request($this->baseApi, 'list', PodList::class, $queries);
    }

    public function get(string $name, array $queries = []): Pod
    {
        return $this->apiClient->request($this->baseApi, 'read', Pod::class, $name, $queries);
    }

    public function create($model, array $queries = []): Pod
    {
        return $this->apiClient->request($this->baseApi, 'create', Pod::class, $model, $queries);
    }

    public function delete(string $name, ?DeleteOptions $options = null, array $queries = []): Status
    {
        $options ??= new DeleteOptions();
        return $this->apiClient->request($this->baseApi, 'delete', Status::class, $name, $options, $queries);
    }

    public function deleteCollection(?DeleteOptions $options = null, array $queries = []): Status
    {
        $options ??= new DeleteOptions();
        return $this->apiClient->request($this->baseApi, 'deleteCollection', Status::class, $options, $queries);
    }

    public function getStatus(string $name, array $queries = []): Pod
    {
        return $this->apiClient->request($this->baseApi, 'readStatus', Pod::class, $name, $queries);
    }

    public function readLog(string $name, array $queries = []): string
    {
        // do not use $this->request() wrapper, method does not return a class
        $result = $this->baseApi->readLog($this->apiClient->getK8sNamespace(), $name, $queries);

        if ($result instanceof Status) {
            throw new KubernetesResponseException(sprintf('K8S request has failed: %s', $result->message), $result);
        }

        if (!is_string($result)) {
            throw new KubernetesResponseException(
                sprintf('Unexpected response type: %s', get_debug_type($result)),
                null
            );
        }

        return $result;
    }
}
