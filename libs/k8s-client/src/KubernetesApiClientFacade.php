<?php

declare(strict_types=1);

namespace Keboola\K8sClient;

use InvalidArgumentException;
use Keboola\K8sClient\ApiClient\AppRunsApiClient;
use Keboola\K8sClient\ApiClient\AppsApiClient;
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
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMap;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolume;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PersistentVolumeClaim;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
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
        private readonly AppsApiClient $appsApiClient,
        private readonly AppRunsApiClient $appRunsApiClient,
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
            App::class => $this->appsApiClient,
            AppRun::class => $this->appRunsApiClient,
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

    public function apps(): AppsApiClient
    {
        return $this->appsApiClient;
    }

    public function appRuns(): AppRunsApiClient
    {
        return $this->appRunsApiClient;
    }

    // phpcs:disable Generic.Files.LineLength.MaxExceeded
    /**
     * @phpstan-template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
     * @phpstan-param class-string<T> $resourceType
     * @phpstan-return T
     */
    // phpcs:enable Generic.Files.LineLength.MaxExceeded
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
     *       new PersistentVolume(...),
     *       new App(...),
     *       new AppRun(...),
     *     ])
     *
     * phpcs:disable Generic.Files.LineLength.MaxExceeded
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun> $resources
     * @return (ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun)[]
     * phpcs:enable Generic.Files.LineLength.MaxExceeded
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
     *       new App(...),
     *       new AppRun(...),
     *     ])
     *
     * phpcs:disable Generic.Files.LineLength.MaxExceeded
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun> $resources
     * phpcs:enable Generic.Files.LineLength.MaxExceeded
     * @return Status[]
     */
    public function deleteModels(array $resources, ?DeleteOptions $deleteOptions = null, array $queries = []): array
    {
        return array_map(
            fn($resource) => $this->getApiForResource($resource::class)->delete(
                // @phpstan-ignore-next-line metadata is always set for valid K8s resources
                $resource->metadata->name,
                $deleteOptions,
                $queries,
            ),
            $resources,
        );
    }

    /**
     * Patch a resource using the specified patch strategy.
     *
     * Example:
     *     $app = new App(['metadata' => ['name' => 'my-app'], 'spec' => ['replicas' => 3]]);
     *     $updatedApp = $apiFacade->patch($app);
     *
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
     * @param T $resource The resource to patch (name extracted from metadata)
     * @return T The patched resource
     */
    public function patch(
        AbstractModel $resource,
        PatchStrategy $strategy = PatchStrategy::JsonMergePatch,
        array $queries = [],
    ): AbstractModel {
        $name = $resource->metadata?->name;
        if ($name === null) {
            throw new InvalidArgumentException('Resource metadata.name is required for patch operation');
        }

        $data = $resource->getArrayCopy();
        $data['patchOperation'] = $strategy->value;

        /** @var T */
        return $this->getApiForResource($resource::class)->patch($name, new Patch($data), $queries);
    }

    /**
     * Create or patch a resource (patch if exists, create if not).
     *
     * This is a convenience method that attempts to patch the resource first and falls back to creating it
     * if it doesn't exist.
     *
     * Example:
     *     $app = new App(['metadata' => ['name' => 'my-app'], 'spec' => ['replicas' => 3]]);
     *     $result = $apiFacade->createOrPatch($app);
     *
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
     * @param T $resource The resource to create or patch
     * @return T The created/patched resource
     */
    public function createOrPatch(
        AbstractModel $resource,
        PatchStrategy $strategy = PatchStrategy::JsonMergePatch,
        array $queries = [],
    ): AbstractModel {
        try {
            /** @var T */
            return $this->patch($resource, $strategy, $queries);
        } catch (ResourceNotFoundException) {
            /** @var T */
            return $this->getApiForResource($resource::class)->create($resource, $queries);
        }
    }

    /**
     * phpcs:disable Generic.Files.LineLength.MaxExceeded
     * @param array<ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun> $resources
     * phpcs:enable Generic.Files.LineLength.MaxExceeded
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
                    // @phpstan-ignore-next-line metadata is always set for valid K8s resources
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
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
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
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
     * @param array{
     *     resourceTypes?: class-string<T>[]
     * } $queries
     *     - resourceTypes: (optional) array of resource types to delete, by default is [ConfigMap::class,
     *         Ingress::class, PersistentVolumeClaim::class, PersistentVolume::class, Pod::class, Secret::class,
     *         Service::class, App::class, AppRun::class]
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
     * @template T of ConfigMap|Event|PersistentVolumeClaim|Pod|Secret|Service|Ingress|PersistentVolume|App|AppRun
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
     *         ($resourceType is class-string<App> ? AppsApiClient :
     *         ($resourceType is class-string<AppRun> ? AppRunsApiClient :
     *         never))))))))))
     */
    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
    private function getApiForResource(string $resourceType): ConfigMapsApiClient|EventsApiClient|PersistentVolumeClaimsApiClient|PodsApiClient|SecretsApiClient|ServicesApiClient|IngressesApiClient|PersistentVolumesApiClient|AppsApiClient|AppRunsApiClient
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
