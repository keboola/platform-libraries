<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;
use KubernetesRuntime\AbstractModel;

/**
 * @template TBaseApi of AbstractAPI
 * @template TList of AbstractModel
 * @template TItem of AbstractModel
 * @implements ApiClientInterface<TList, TItem>
 */
abstract class BaseClusterApiClient implements ApiClientInterface
{
    /**
     * @param TBaseApi $baseApi
     * @param class-string<TList> $listClass
     * @param class-string<TItem> $itemClass
     */
    public function __construct(
        protected readonly KubernetesApiClient $apiClient,
        protected readonly AbstractAPI $baseApi,
        protected readonly string $listClass,
        protected readonly string $itemClass,
    ) {
    }

    /**
     * @return TList
     */
    public function list(array $queries = []): AbstractModel
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'list', $this->listClass, $queries);
    }

    /**
     * @return TItem
     */
    public function get(string $name, array $queries = []): AbstractModel
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'read', $this->itemClass, $name, $queries);
    }

    /**
     * @param TItem $model
     * @return TItem
     */
    public function create(AbstractModel $model, array $queries = []): AbstractModel
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'create', $this->itemClass, $model, $queries);
    }

    /**
     * @return TItem
     */
    public function patch(string $name, Patch $model, array $queries = []): AbstractModel
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'patch', $this->itemClass, $name, $model, $queries);
    }

    public function delete(string $name, ?DeleteOptions $options = null, array $queries = []): Status
    {
        $options ??= new DeleteOptions();
        return $this->apiClient->clusterRequest($this->baseApi, 'delete', Status::class, $name, $options, $queries);
    }

    public function deleteCollection(?DeleteOptions $options = null, array $queries = []): Status
    {
        $options ??= new DeleteOptions();
        return $this->apiClient->clusterRequest($this->baseApi, 'deleteCollection', Status::class, $options, $queries);
    }
}
