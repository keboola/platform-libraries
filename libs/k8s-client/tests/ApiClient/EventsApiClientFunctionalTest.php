<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\EventsApiClient;
use Kubernetes\API\Event as EventsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use PHPUnit\Framework\TestCase;

class EventsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<EventsApi, EventsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            EventsApi::class,
            EventsApiClient::class,
        );
    }

    protected function createResource(array $metadata): Event
    {
        return new Event([
            'metadata' => $metadata,
            'type' => 'Normal',
            'reason' => 'Testing',
            'message' => 'EventsApiClientFunctionalTest event',
            'involvedObject' => [
                'kind' => 'ConfigMap',
                'name' => 'kube-root-ca.crt',
                'namespace' => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
    }

    public function testListForAllNamespaces(): void
    {
        $event = $this->createResource([
            'name' => 'test-1',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
        $this->apiClient->create($event);

        $result = $this->apiClient->listForAllNamespaces();
        self::assertGreaterThanOrEqual(1, count($result->items));

        $namespaces = [];
        foreach ($result->items as $event) {
            $namespaces[$event->metadata->namespace] = $event->metadata->namespace;
        }

        self::assertGreaterThan(1, $namespaces);
        self::assertArrayHasKey((string) getenv('K8S_NAMESPACE'), $namespaces);
    }
}
