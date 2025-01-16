<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ClientFacadeFactory\ClientConfigurator;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Event as EventsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EventList;
use PHPUnit\Framework\TestCase;

class EventsApiClientTest extends TestCase
{
    public function testListForAllNamespaces(): void
    {
        ClientConfigurator::configureBaseClient(
            apiUrl: (string) getenv('K8S_HOST'),
            caCertFile: (string) getenv('K8S_CA_CERT_PATH'),
            token: (string) getenv('K8S_TOKEN'),
        );

        $k8sClientMock = $this->createMock(KubernetesApiClient::class);
        $k8sClientMock->expects(self::once())
            ->method('clusterRequest')
            ->with(
                $this->isInstanceOf(EventsApi::class),
                'listForAllNamespaces',
                EventList::class,
                ['fieldSelector' => 'involvedObject.kind=ConfigMap'],
            )
            ->willReturn(new EventList())
        ;

        $eventsApi = new EventsApiClient($k8sClientMock);

        $eventsApi->listForAllNamespaces(['fieldSelector' => 'involvedObject.kind=ConfigMap']);
    }
}
