<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Event as EventsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EventList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;

/**
 * @template-implements ApiClientInterface<EventList, Event>
 */
class EventsApiClient implements ApiClientInterface
{
    private KubernetesApiClient $apiClient;
    private EventsApi $baseApi;

    public function __construct(KubernetesApiClient $apiClient)
    {
        $this->apiClient = $apiClient;
        $this->baseApi = new EventsApi();
    }

    public function listForAllNamespaces(array $queries = []): EventList
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'listForAllNamespaces', EventList::class, $queries);
    }

    public function list(array $queries = []): EventList
    {
        return $this->apiClient->request($this->baseApi, 'list', EventList::class, $queries);
    }

    public function get(string $name, array $queries = []): Event
    {
        return $this->apiClient->request($this->baseApi, 'read', Event::class, $name, $queries);
    }

    public function create($model, array $queries = []): Event
    {
        return $this->apiClient->request($this->baseApi, 'create', Event::class, $model, $queries);
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
