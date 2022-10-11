<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Secret as SecretsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Api\Core\V1\SecretList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;

class SecretsApiClient
{
    private KubernetesApiClient $apiClient;
    private SecretsApi $baseApi;

    public function __construct(KubernetesApiClient $apiClient)
    {
        $this->apiClient = $apiClient;
        $this->baseApi = new SecretsApi();
    }

    public function list(array $queries = []): SecretList
    {
        return $this->apiClient->request($this->baseApi, 'list', SecretList::class, $queries);
    }

    public function get(string $name, array $queries = []): Secret
    {
        return $this->apiClient->request($this->baseApi, 'read', Secret::class, $name, $queries);
    }

    public function create(Secret $model, array $queries = []): Secret
    {
        return $this->apiClient->request($this->baseApi, 'create', Secret::class, $model, $queries);
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
}
