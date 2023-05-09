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
            'message' => 'EventsErikApiClientFunctionalTest event',
            'involvedObject' => [
                'kind' => 'ConfigMap',
                'name' => 'kube-root-ca.crt',
                'namespace' => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
    }

    public function testListForAllNamespaces(): void
    {
        $result = $this->apiClient->listForAllNamespaces();
        self::assertGreaterThan(0, count($result->items));
        $namespaces = [];
        foreach ($result->items as $event) {
            $namespaces[$event->metadata->namespace] = $event->metadata->namespace;
        }

        self::assertGreaterThan(1, $namespaces);
    }

    private function getExcludedItemNamesFromCleanup(): array
    {
        return [];
    }
}
