<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Kubernetes\API\ConfigMap as ConfigMapsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMap;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ConfigMapsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<ConfigMapsApi, ConfigMapsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        //@TODO resolve problems with root crt that is also saved in configmap in same namespace
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            ConfigMapsApi::class,
            ConfigMapsApiClient::class,
        );
    }

    private function cleanupK8sResources(float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        $this->baseApiClient->deleteCollection(
            (string) getenv('K8S_NAMESPACE'),
            new DeleteOptions([
                'gracePeriodSeconds' => 0,
                'propagationPolicy' => 'Foreground',
            ]),
        );

        while ($startTime + $timeout > microtime(true)) {
            $result = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));

            if ($result instanceof Status) {
                throw new RuntimeException('Failed to read resource state: ' . $result->message);
            }

            assert(is_object($result) && property_exists($result, 'items'));
            if (count($result->items) === 0) {
                return;
            }
            if (count($result->items) === 1 && $result->items[0]->metadata->name === 'kube-root-ca.crt') {
                return; // it is automaticaly created by cluster
            }

            usleep(100_000);
        }

        throw new RuntimeException('Timeout while waiting for resource delete');
    }

    public function testListResources(): void
    {
        $result = $this->apiClient->list();
        self::assertCount(1, $result->items);
        self::assertSame(
            ['kube-root-ca.crt'],
            array_map(fn($resource) => $resource->metadata->name, $result->items)
        );

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
            ],
        ]));

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $this->createResource([
            'name' => 'test-resource-2',
        ]));

        // list all
        $result = $this->apiClient->list();
        self::assertCount(3, $result->items);
        self::assertSame(
            ['kube-root-ca.crt', 'test-resource-1', 'test-resource-2'],
            array_map(fn($resource) => $resource->metadata->name, $result->items)
        );

        // list using labelSelector
        $result = $this->apiClient->list([
            'labelSelector' => 'app=test-1',
        ]);
        self::assertCount(1, $result->items);
        self::assertSame(
            ['test-resource-1'],
            array_map(fn($resource) => $resource->metadata->name, $result->items)
        );
    }

    public function testCreateResource(): void
    {
        $resourceToCreate = $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
            ],
        ]);

        $createdResource = $this->apiClient->create($resourceToCreate);

        self::assertNotSame($resourceToCreate, $createdResource);
        self::assertSame($resourceToCreate->metadata->name, $createdResource->metadata->name);

        $result = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($result) && property_exists($result, 'items'));
        self::assertCount(2, $result->items);
        self::assertSame(
            ['kube-root-ca.crt', $createdResource->metadata->name],
            array_map(fn($resource) => $resource->metadata->name, $result->items)
        );
    }

    public function testDeleteCollection(): void
    {
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $this->createResource([
            'name' => 'test-resource-11',
            'labels' => [
                'app' => 'test-1',
            ],
        ]));
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $this->createResource([
            'name' => 'test-resource-12',
            'labels' => [
                'app' => 'test-1',
            ],
        ]));
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $this->createResource([
            'name' => 'test-resource-21',
        ]));

        $listResult = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        self::assertCount(4, $listResult->items);

        $this->apiClient->deleteCollection(new DeleteOptions(), [
            'labelSelector' => 'app=test-1',
        ]);

        $this->waitWhileResourceExists('test-resource-11');
        $this->waitWhileResourceExists('test-resource-12');

        $listResult = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        self::assertCount(2, $listResult->items);
    }

    protected function createResource(array $metadata): ConfigMap
    {
        return new ConfigMap([
            'metadata' => $metadata,
            'data' => [
                'test_key' => 'test_value',
            ],
        ]);
    }
}
