<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\Exception\ResourceAlreadyExistsException;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Pod as PodsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;
use RuntimeException;

class PodsApiClientFunctionalTest extends TestCase
{
    private PodsApi $baseApiClient;
    private PodsApiClient $apiClient;

    public function setUp(): void
    {
        parent::setUp();

        Client::configure(
            (string) getenv('K8S_HOST'),
            [
                'caCert' => (string) getenv('K8S_CA_CERT_PATH'),
                'token' => (string) getenv('K8S_TOKEN'),
            ],
        );

        $this->baseApiClient = new PodsApi();
        $this->apiClient = new PodsApiClient(
            new KubernetesApiClient(
                new RetryProxy(),
                (string) getenv('K8S_NAMESPACE'),
            ),
            $this->baseApiClient,
        );

        $this->cleanupResources();
    }

    private function cleanupResources(float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        $this->baseApiClient->deleteCollection(
            (string) getenv('K8S_NAMESPACE'),
            new DeleteOptions(['propagationPolicy' => 'Foreground']),
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

            usleep(100_000);
        }

        throw new RuntimeException('Timeout while waiting for resource delete');
    }

    private function waitWhileResourceExists(string $name, float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        while ($startTime + $timeout > microtime(true)) {
            $result = $this->baseApiClient->read((string) getenv('K8S_NAMESPACE'), $name);

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
        self::assertCount(0, $result->items);

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-1',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-2',
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));

        // list all
        $result = $this->apiClient->list();
        self::assertCount(2, $result->items);
        self::assertSame(
            ['test-pod-1', 'test-pod-2'],
            array_map(fn(Pod $pod) => $pod->metadata->name, $result->items)
        );

        // list using labelSelector
        $result = $this->apiClient->list([
            'labelSelector' => 'app=test-1',
        ]);
        self::assertCount(1, $result->items);
        self::assertSame(
            ['test-pod-1'],
            array_map(fn(Pod $pod) => $pod->metadata->name, $result->items)
        );
    }

    public function testGetResource(): void
    {
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-1',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));

        $result = $this->apiClient->get('test-pod-1');
        self::assertSame('test-pod-1', $result->metadata->name);
    }

    public function testGetNonExistingResourceThrowsException(): void
    {
        $this->expectException(ResourceNotFoundException::class);
        $this->expectExceptionMessage('Resource not found:');

        $this->apiClient->get('test-pod-1');
    }

    public function testCreateResource(): void
    {
        $podToCreate = new Pod([
            'metadata' => [
                'name' => 'test-pod-1',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]);

        $createdPod = $this->apiClient->create($podToCreate);

        self::assertNotSame($podToCreate, $createdPod);
        self::assertSame($podToCreate->metadata->name, $createdPod->metadata->name);

        $result = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($result) && property_exists($result, 'items'));
        self::assertCount(1, $result->items);
        self::assertSame($result->items[0]->metadata->name, $createdPod->metadata->name);
    }

    public function testCreateResourceWithDuplicateNameThrowsException(): void
    {
        $podToCreate = new Pod([
            'metadata' => [
                'name' => 'test-pod-1',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]);

        $this->apiClient->create($podToCreate);

        $this->expectException(ResourceAlreadyExistsException::class);
        $this->expectExceptionMessage('Resource already exists:');

        $this->apiClient->create($podToCreate);
    }

    public function testDeleteResource(): void
    {
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-1',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-2',
                'labels' => [
                    'app' => 'test-2',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));

        // delete the resource
        $this->apiClient->delete('test-pod-1');
        $this->waitWhileResourceExists('test-pod-1');

        // check the other resource was not deleted
        $this->apiClient->get('test-pod-2');

        $this->expectNotToPerformAssertions();
    }

    public function testDeleteNotExistingResourceThrowsException(): void
    {
        $this->expectException(ResourceNotFoundException::class);
        $this->expectExceptionMessage('Resource not found:');

        $this->apiClient->delete('test-pod-1');
    }

    public function testDeleteCollection(): void
    {
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-11',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-12',
                'labels' => [
                    'app' => 'test-1',
                ],
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));
        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), new Pod([
            'metadata' => [
                'name' => 'test-pod-21',
            ],
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]));

        $listResult = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        self::assertCount(3, $listResult->items);

        $this->apiClient->deleteCollection(new DeleteOptions(), [
            'labelSelector' => 'app=test-1',
        ]);

        $this->waitWhileResourceExists('test-pod-11');
        $this->waitWhileResourceExists('test-pod-12');

        $listResult = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));
        assert(is_object($listResult) && property_exists($listResult, 'items'));
        self::assertCount(1, $listResult->items);
    }
}
