<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use BadMethodCallException;
use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Kubernetes\API\Service as ServicesApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ServicesApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<ServicesApi, ServicesApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            ServicesApi::class,
            ServicesApiClient::class,
        );
    }

    private function getExcludedItemNamesFromCleanup(): array
    {
        return [];
    }

    protected function createResource(array $metadata): Service
    {
        return new Service([
            'metadata' => $metadata,
            'spec' => [
                'selector' => [
                    'app' => 'ServicesApiClientFunctionalTest',
                ],
                'ports' => [
                    [
                        'name' => 'test-port',
                        'port' => 1234,
                    ],
                ],
            ],
        ]);
    }

    public function testDeleteCollection(): void
    {
        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('DeleteCollection is not yet implemented for "Service" resource');

        $this->apiClient->deleteCollection(new DeleteOptions(), [
            'labelSelector' => 'app=test-1',
        ]);
    }

    private function cleanupK8sResources(float $timeout = 30.0): void
    {
        $startTime = microtime(true);

        $queries = [];
        $excludeItemNames = $this->getExcludedItemNamesFromCleanup();
        if ($excludeItemNames) {
            $queries['fieldSelector'] = implode(
                ',',
                array_map(function (string $name): string {
                    return sprintf(
                        'metadata.name!=%s',
                        $name
                    );
                }, $excludeItemNames)
            );
        }

        $serviceList = $this->apiClient->list($queries);
        foreach ($serviceList->items as $service) {
            $this->apiClient->delete(
                $service->metadata->name,
                new DeleteOptions([
                    'gracePeriodSeconds' => 0,
                    'propagationPolicy' => 'Foreground',
                ])
            );
        }

        while ($startTime + $timeout > microtime(true)) {
            $result = $this->baseApiClient->list((string) getenv('K8S_NAMESPACE'));

            if ($result instanceof Status) {
                throw new RuntimeException('Failed to read resource state: ' . $result->message);
            }

            assert(is_object($result) && property_exists($result, 'items'));
            if (count($result->items) === 0) {
                return;
            }

            if ($excludeItemNames && count($result->items) === count($excludeItemNames)) {
                $itemNames = array_map(fn($resource) => $resource->metadata->name, $result->items);
                $diffA = array_diff($itemNames, $excludeItemNames);
                $diffB = array_diff($excludeItemNames, $itemNames);
                if (count($diffA) === 0 && count($diffB) === 0) {
                    return;
                }
            }

            usleep(100_000);
        }

        throw new RuntimeException('Timeout while waiting for resource delete');
    }
}
