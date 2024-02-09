<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\BaseClusterApiClient;
use Keboola\K8sClient\Exception\ResourceAlreadyExistsException;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;
use KubernetesRuntime\AbstractModel;
use KubernetesRuntime\Client;
use Retry\RetryProxy;
use RuntimeException;

/**
 * @template TBaseApi of AbstractAPI
 * @template TApi of BaseClusterApiClient
 */
trait BaseClusterApiClientTestCase
{
    /** @var TBaseApi */
    private AbstractAPI $baseApiClient;

    /** @var TApi */
    private BaseClusterApiClient $apiClient;

    abstract protected function createResource(array $metadata): AbstractModel;

    /**
     * @param class-string<TBaseApi> $baseApiClientClass
     * @param class-string<TApi> $apiClientClass
     */
    public function setUpBaseClusterApiClientTest(string $baseApiClientClass, string $apiClientClass): void
    {
        Client::configure(
            (string) getenv('K8S_HOST'),
            [
                'caCert' => (string) getenv('K8S_CA_CERT_PATH'),
                'token' => (string) getenv('K8S_TOKEN'),
            ],
        );

        $this->baseApiClient = new $baseApiClientClass;
        $this->apiClient = new $apiClientClass(
            new KubernetesApiClient(
                new RetryProxy(),
                (string) getenv('K8S_NAMESPACE'),
            ),
            $this->baseApiClient,
        );

        $this->cleanupK8sResources();
    }

    private function cleanupK8sResources(float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        $queries = [
            'labelSelector' => sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
        ];

        $this->baseApiClient->deleteCollection(
            new DeleteOptions([
                'gracePeriodSeconds' => 0,
                'propagationPolicy' => 'Foreground',
            ]),
            $queries,
        );

        while ($startTime + $timeout > microtime(true)) {
            $result = $this->baseApiClient->list($queries);

            if ($result instanceof Status) {
                throw new RuntimeException('Failed to read resource state: ' . $result->message);
            }

            assert(is_object($result) && property_exists($result, 'items'));
            if (count($result->items) === 0) {
                return;
            }

            usleep(100_000);
        }

        throw new RuntimeException('Timeout while waiting for resource delete');
    }

    protected function waitWhileResourceExists(string $name, float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        while ($startTime + $timeout > microtime(true)) {
            $result = $this->baseApiClient->read($name);

            if ($result instanceof Status) {
                if ($result->code === 404) {
                    return;
                }

                throw new RuntimeException('Failed to read resource state: ' . $result->message);
            }

            usleep(100_000);
        }

        throw new RuntimeException('Timeout while waiting for resource delete');
    }

    public function testListResources(): void
    {
        $result = $this->apiClient->list();

        $originalItemNames = array_map(fn($resource) => $resource->metadata->name, $result->items);

        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-1'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));

        // list all
        $result = $this->apiClient->list();
        $this->assertResultItems(
            array_merge(
                $originalItemNames,
                [self::getTestResourceNameWithNamespacePrefix('test-resource-1')],
            ),
            $result->items,
        );

        // list using labelSelector
        $result = $this->apiClient->list([
            'labelSelector' => sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
        ]);
        self::assertCount(1, $result->items);
        self::assertSame(
            [self::getTestResourceNameWithNamespacePrefix('test-resource-1')],
            array_map(fn($resource) => $resource->metadata->name, $result->items),
        );
    }

    public function testGetResource(): void
    {
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-1'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));

        $result = $this->apiClient->get(self::getTestResourceNameWithNamespacePrefix('test-resource-1'));
        self::assertSame(self::getTestResourceNameWithNamespacePrefix('test-resource-1'), $result->metadata->name);
    }

    public function testGetNonExistingResourceThrowsException(): void
    {
        $this->expectException(ResourceNotFoundException::class);
        $this->expectExceptionMessage('Resource not found:');

        $this->apiClient->get(self::getTestResourceNameWithNamespacePrefix('test-resource-1'));
    }

    public function testCreateResource(): void
    {
        $resourceToCreate = $this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-1'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $createdResource = $this->apiClient->create($resourceToCreate);

        self::assertNotSame($resourceToCreate, $createdResource);
        self::assertSame($resourceToCreate->metadata->name, $createdResource->metadata->name);

        $result = $this->baseApiClient->list([
            'labelSelector' => sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
        ]);
        assert(is_object($result) && property_exists($result, 'items'));

        $this->assertResultItems([$createdResource->metadata->name], $result->items);
    }

    public function testCreateResourceWithDuplicateNameThrowsException(): void
    {
        $resourceToCreate = $this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-1'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $this->apiClient->create($resourceToCreate);

        $this->expectException(ResourceAlreadyExistsException::class);
        $this->expectExceptionMessage('Resource already exists:');

        $this->apiClient->create($resourceToCreate);
    }

    public function testDeleteResource(): void
    {
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-1'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-2'),
            'labels' => [
                'app' => 'test-2',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));

        // delete the resource
        $this->apiClient->delete(self::getTestResourceNameWithNamespacePrefix('test-resource-1'));
        $this->waitWhileResourceExists(self::getTestResourceNameWithNamespacePrefix('test-resource-1'));

        // check the other resource was not deleted
        $this->apiClient->get(self::getTestResourceNameWithNamespacePrefix('test-resource-2'));

        $this->expectNotToPerformAssertions();
    }

    public function testDeleteNotExistingResourceThrowsException(): void
    {
        $this->expectException(ResourceNotFoundException::class);
        $this->expectExceptionMessage('Resource not found:');

        $this->apiClient->delete(self::getTestResourceNameWithNamespacePrefix('test-resource-1'));
    }

    public function testDeleteCollection(): void
    {
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-11'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-12'),
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));
        $this->baseApiClient->create($this->createResource([
            'name' => self::getTestResourceNameWithNamespacePrefix('test-resource-21'),
            'labels' => [
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]));

        $listResult = $this->baseApiClient->list([
            'labelSelector' => sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
        ]);
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        $this->assertResultItems(
            [
                self::getTestResourceNameWithNamespacePrefix('test-resource-11'),
                self::getTestResourceNameWithNamespacePrefix('test-resource-12'),
                self::getTestResourceNameWithNamespacePrefix('test-resource-21'),
            ],
            $listResult->items,
        );

        $this->apiClient->deleteCollection(new DeleteOptions(), [
            'labelSelector' => implode(
                ',',
                [
                    'app=test-1',
                    sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
                ],
            ),
        ]);

        $this->waitWhileResourceExists(self::getTestResourceNameWithNamespacePrefix('test-resource-11'));
        $this->waitWhileResourceExists(self::getTestResourceNameWithNamespacePrefix('test-resource-12'));

        $listResult = $this->baseApiClient->list([
            'labelSelector' => sprintf('%s=%s', self::getTestResourcesLabelName(), (string) getenv('K8S_NAMESPACE')),
        ]);
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        $this->assertResultItems(
            [self::getTestResourceNameWithNamespacePrefix('test-resource-21')],
            $listResult->items,
        );
    }

    private function assertResultItems(array $expectedNames, array $resultItems): void
    {
        self::assertCount(count($expectedNames), $resultItems);

        $resultItemNames = array_map(fn($resource) => $resource->metadata->name, $resultItems);
        sort($expectedNames);
        sort($resultItemNames);
        self::assertSame($expectedNames, $resultItemNames);
    }

    private static function getTestResourcesLabelName(): string
    {
        return 'k8s-client-tests-namespace';
    }

    private static function getTestResourceNameWithNamespacePrefix(string $name): string
    {
        return sprintf('%s-%s', getenv('K8S_NAMESPACE'), $name);
    }
}
