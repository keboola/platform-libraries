<?php

declare(strict_types=1);

namespace Keboola\K8sClient;

use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Keboola\K8sClient\ApiClient\EventsApiClient;
use Keboola\K8sClient\ApiClient\IngressesApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumeClaimsApiClient;
use Keboola\K8sClient\ApiClient\PersistentVolumesApiClient;
use Keboola\K8sClient\ApiClient\PodsApiClient;
use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\Exception\TimeoutException;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMap;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolume;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeClaim;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractModel;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

class KubernetesApiClientFacade
{
    private const LIST_INTERNAL_PAGE_SIZE = 100;

    private readonly array $resourceTypeClientMap;

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly ConfigMapsApiClient $configMapApiClient,
        private readonly EventsApiClient $eventsApiClient,
        private readonly IngressesApiClient $ingressesApiClient,
        private readonly PersistentVolumeClaimsApiClient $persistentVolumeClaimsApiClient,
        private readonly PersistentVolumesApiClient $persistentVolumesApiClient,
        private readonly PodsApiClient $podsApiClient,
        private readonly SecretsApiClient $secretsApiClient,
        private readonly ServicesApiClient $servicesApiClient,
    ) {
        $this->resourceTypeClientMap = [
            ConfigMap::class => $this->configMapApiClient,
            Event::class => $this->eventsApiClient,
            PersistentVolumeClaim::class => $this->persistentVolumeClaimsApiClient,
            Pod::class => $this->podsApiClient,
            Secret::class => $this->secretsApiClient,
            Service::class => $this->servicesApiClient,
            Ingress::class => $this->ingressesApiClient,
            PersistentVolume::class => $this->persistentVolumesApiClient,
        ];
    }

    public function ingresses(): IngressesApiClient
    {
        return $this->ingressesApiClient;
    }

    public function services(): ServicesApiClient
    {
        return $this->servicesApiClient;
    }

    public function configMaps(): ConfigMapsApiClient
    {
        return $this->configMapApiClient;
    }

    public function events(): EventsApiClient
    {
        return $this->eventsApiClient;
    }

    public function persistentVolumeClaims(): PersistentVolumeClaimsApiClient
    {
        return $this->persistentVolumeClaimsApiClient;
    }

    public function pods(): PodsApiClient
    {
        return $this->podsApiClient;
    }

    public function secrets(): SecretsApiClient
    {
        return $this->secretsApiClient;
    }

    public function persistentVolumes(): PersistentVolumesApiClient
    {
        return $this->persistentVolumesApiClient;
    }

    /**
     * @phpstan-template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume
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
     *       new Service(...),
     *       new Ingress(...),
     *     ])
     *
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume> $resources
     * @return (ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume)[]
     */
    public function createModels(array $resources, array $queries = []): array
    {
        return array_map(
            fn($resource) => $this->getApiForResource($resource::class)->create($resource, $queries),
            $resources,
        );
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
     *       new Service(...),
     *       new Ingress(...),
     *       new PersistentVolume(...),
     *     ])
     *
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume> $resources
     * @return Status[]
     */
    public function deleteModels(array $resources, ?DeleteOptions $deleteOptions = null, array $queries = []): array
    {
        return array_map(
            fn($resource) => $this->getApiForResource($resource::class)->delete(
                $resource->metadata->name,
                $deleteOptions,
                $queries,
            ),
            $resources,
        );
    }

    /**
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume> $resources
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
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume
     * @param class-string<T> $resourceType
     * @return iterable<T>
     */
    public function listMatching(string $resourceType, array $queries = []): iterable
    {
        $queries['limit'] ??= self::LIST_INTERNAL_PAGE_SIZE;
        $api = $this->getApiForResource($resourceType);

        do {
            $response = $api->list($queries);
            foreach ($response->items as $item) {
                /** @var T $item */
                yield $item;
            }

            $queries['continue'] = $response->metadata?->continue;
        } while ($queries['continue']);
    }

    /**
     * Delete all matching resources, regardless of type.
     *
     * Resources are delete sequentially by API type. If some delete request fails, the error is logged and other APIs
     * are still called. Finally, the last exception is re-thrown.
     *
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume
     * @param array{
     *     resourceTypes?: class-string<T>[]
     * } $queries
     *     - resourceTypes: (optional) array of resource types to delete, by default is [ConfigMap::class,
     *         Ingress::class, PersistentVolumeClaim::class, PersistentVolume::class, Pod::class, Secret::class,
     *         Service::class]
     *     Other keys represent additional query parameters for the Kubernetes API's deleteCollection endpoint.
     *
     * Example:
     *     $apiFacade->delete(
     *         new DeleteOptions(),
     *         [
     *             'resourceTypes' => [ConfigMap::class],
     *             'labelSelector' => 'app=job-1234',
     *         ]
     *     )
     */
    public function deleteAllMatching(?DeleteOptions $deleteOptions = null, array $queries = []): void
    {
        $resourceTypes = $queries['resourceTypes'] ?? $this->getDefaultResourceTypesForDeleteAll();
        unset($queries['resourceTypes']);

        foreach ($resourceTypes as $resourceType) {
            try {
                $this->getApiForResource($resourceType)->deleteCollection($deleteOptions, $queries);
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
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume
     * @param class-string<T> $resourceType
     */
    public function checkResourceExists(string $resourceType, string $resourceName): bool
    {
        try {
            $this->getApiForResource($resourceType)->get($resourceName);
            return true;
        } catch (ResourceNotFoundException $e) {
        }

        return false;
    }

    /**
     * @param class-string<AbstractModel> $resourceType
     * @return ($resourceType is class-string<ConfigMap> ? ConfigMapsApiClient :
     *         ($resourceType is class-string<Event> ? EventsApiClient :
     *         ($resourceType is class-string<PersistentVolumeClaim> ? PersistentVolumeClaimsApiClient :
     *         ($resourceType is class-string<Pod> ? PodsApiClient :
     *         ($resourceType is class-string<Secret> ? SecretsApiClient :
     *         ($resourceType is class-string<Service> ? ServicesApiClient :
     *         ($resourceType is class-string<Ingress> ? IngressesApiClient :
     *         ($resourceType is class-string<PersistentVolume> ? PersistentVolumesApiClient :
     *         never))))))))
     */
    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
    private function getApiForResource(string $resourceType): ConfigMapsApiClient|EventsApiClient|PersistentVolumeClaimsApiClient|PodsApiClient|SecretsApiClient|ServicesApiClient|IngressesApiClient|PersistentVolumesApiClient
    {
        if (!array_key_exists($resourceType, $this->resourceTypeClientMap)) {
            throw new RuntimeException(sprintf(
                'Unknown K8S resource type "%s"',
                $resourceType,
            ));
        }

        return $this->resourceTypeClientMap[$resourceType];
    }

    private function getDefaultResourceTypesForDeleteAll(): array
    {
        $resourceTypeClientMap = $this->resourceTypeClientMap;
        unset($resourceTypeClientMap[Event::class]);
        return array_keys($resourceTypeClientMap);
    }
}
