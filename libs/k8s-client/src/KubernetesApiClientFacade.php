<?php

declare(strict_types=1);

namespace Keboola\K8sClient;

use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\Exception\TimeoutException;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

/**
 * @phpstan-type ResourceType Pod|Secret
 */
class KubernetesApiClientFacade
{
    private LoggerInterface $logger;
    private array $apis;

    public function __construct(
        LoggerInterface $logger,
        PodsApiClient $podsApiClient,
        SecretsApiClient $secretsApiClient
    ) {
        $this->logger = $logger;
        $this->apis = [
            Pod::class => $podsApiClient,
            Secret::class => $secretsApiClient,
        ];
    }

    public function pods(): PodsApiClient
    {
        return $this->apis[Pod::class];
    }

    public function secrets(): SecretsApiClient
    {
        return $this->apis[Secret::class];
    }

    /**
     * @phpstan-template T of Pod|Secret
     * @phpstan-param class-string<T> $resourceType
     * @phpstan-return T
     */
    public function get(string $resourceType, string $name, array $queries = [])
    {
        // @phpstan-ignore-next-line
        return $this->getApiForResource($resourceType)->get($name, $queries);
    }

    /**
     * Create multiple resources at once.
     *
     * Resources are created sequentially. If some resource fails to create, exception is thrown and the rest of
     * resources are skipped.
     *
     * Example:
     *     $apiFacade->create([
     *       new ConfigMap(...),
     *       new ConfigMap(...),
     *       new Secret(...),
     *       new Pod(...),
     *     ])
     *
     * @param iterable<ResourceType> $resources
     * @return ResourceType[]
     */
    public function createModels(iterable $resources, array $queries = []): array
    {
        $results = [];

        foreach ($resources as $k => $resource) {
            $api = $this->getApiForResource($resource::class);
            $results[$k] = $api->create($resource, $queries);
        }

        return $results;
    }

    /**
     * Delete multiple resources at once.
     *
     * Resources are delete sequentially. If some delete request fails, the error is logged and other resources are
     * still deleted. Finally, the last exception is re-thrown.
     *
     * Example:
     *     $apiFacade->delete([
     *       new ConfigMap(...),
     *       new ConfigMap(...),
     *       new Secret(...),
     *       new Pod(...),
     *     ])
     *
     * @param iterable<ResourceType> $resources
     * @return Status[]
     */
    public function deleteModels(iterable $resources, ?DeleteOptions $deleteOptions = null, array $queries = []): array
    {
        $results = [];

        foreach ($resources as $k => $resource) {
            $api = $this->getApiForResource($resource::class);
            $results[$k] = $api->delete($resource->metadata->name, $deleteOptions, $queries);
        }

        return $results;
    }

    /**
     * @param array<Pod|Secret> $resources
     */
    public function waitWhileExists(array $resources, float $timeout = INF): void
    {
        $this->logger->debug('Wait while resources exists', [
            'resources' => $resources,
        ]);

        $startTime = microtime(true);
        $updateCollection = function () use (&$resources) {
            foreach ($resources as $i => $resource) {
                try {
                    $this->getApiForResource($resource::class)->get($resource->metadata->name);
                    $this->logger->debug('Resource still exists', [
                        'resource' => $resource,
                    ]);
                } catch (ResourceNotFoundException) {
                    unset($resources[$i]);
                    $this->logger->debug('Resource no longer exists', [
                        'resource' => $resource,
                    ]);
                }
            }
        };

        $updateCollection();

        while (count($resources) > 0) {
            if (microtime(true) - $startTime > $timeout) {
                throw new TimeoutException();
            }

            usleep(100_000);
            $updateCollection();
        }

        $this->logger->debug('No resource exists anymore');
    }

    /**
     * Delete all matching resources, regardless of type.
     *
     * Resources are delete sequentially by API type. If some delete request fails, the error is logged and other APIs
     * are still called. Finally, the last exception is re-thrown.
     *
     * Example:
     *     $apiFacade->delete(
     *         new DeleteOptions(),
     *         [
     *             'labelSelector' => 'app=job-1234',
     *         ]
     *     )
     */
    public function deleteAllMatching(?DeleteOptions $deleteOptions = null, array $queries = []): void
    {
        foreach ($this->apis as $api) {
            try {
                $api->deleteCollection($deleteOptions, $queries);
            } catch (Throwable $exception) {
                $this->logger->error('DeleteCollection request has failed', [
                    'exception' => $exception,
                ]);
            }
        }

        if (isset($exception)) {
            throw $exception;
        }
    }

    /**
     * @param class-string<ResourceType> $resourceType
     */
    private function getApiForResource(string $resourceType): PodsApiClient|SecretsApiClient
    {
        return $this->apis[$resourceType] ?? throw new RuntimeException(sprintf(
            'Unknown K8S resource type "%s"',
            get_debug_type($resourceType)
        ));
    }
}
