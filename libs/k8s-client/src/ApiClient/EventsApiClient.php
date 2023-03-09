<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Event as EventsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EventList;

/**
 * @template-extends  BaseNamespaceApiClient<EventsApi, EventList, Event>
 */
class EventsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new EventsApi(),
            EventList::class,
            Event::class,
        );
    }

    public function listForAllNamespaces(array $queries = []): EventList
    {
        return $this->apiClient->clusterRequest($this->baseApi, 'listForAllNamespaces', EventList::class, $queries);
    }
}
