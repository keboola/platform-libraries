<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppsApiClient;
use Keboola\K8sClient\BaseApi\App as AppsApi;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use PHPUnit\Framework\TestCase;

class AppsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<AppsApi, AppsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            AppsApi::class,
            AppsApiClient::class,
        );
    }

    protected function createResource(array $metadata): App
    {
        return new App([
            'metadata' => $metadata,
            'spec' => [
                'appId' => 'app-123',
                'projectId' => 'project-456',
                'state' => 'Running',
                'replicas' => 1,
                'features' => [
                    'storageToken' => [
                        'description' => 'test-token',
                        'canManageBuckets' => true,
                        'canReadAllFileUploads' => true,
                        'canPurgeTrash' => false,
                        'setEnvs' => [['container' => 'main', 'envName' => 'KBC_TOKEN']],
                    ],
                    'appsProxyIngress' => [
                        'container' => 'main',
                        'targetPort' => 8080,
                    ],
                    'dataDir' => [
                        'mount' => [['container' => 'main']],
                        'dataLoader' => [
                            'branchId' => 'main',
                            'componentId' => 'component-1',
                            'configId' => 'config-1',
                        ],
                    ],
                    'mountConfig' => [
                        'branchId' => 'main',
                        'componentId' => 'component-1',
                        'configId' => 'config-1',
                        'mount' => [[
                            'container' => 'main',
                            'path' => '/config.json',
                            'fields' => [['source' => '$.foo', 'target' => 'bar']],
                        ]],
                    ],
                ],
                'podSpec' => [
                    'restartPolicy' => 'Always',
                    'containers' => [[
                        'name' => 'main',
                        'image' => 'busybox',
                        'env' => [['name' => 'FOO', 'value' => 'bar']],
                        'resources' => [
                            'requests' => ['memory' => '64M', 'cpu' => '100m'],
                            'limits' => ['memory' => '128M', 'cpu' => '200m'],
                        ],
                        'startupProbe' => [
                            'httpGet' => ['path' => '/', 'port' => 8080],
                            'periodSeconds' => 1,
                            'failureThreshold' => 30,
                        ],
                        'readinessProbe' => [
                            'httpGet' => ['path' => '/', 'port' => 8080],
                            'periodSeconds' => 10,
                        ],
                    ]],
                ],
            ],
        ]);
    }

    public function testCreateOrPatchCreatesNewApp(): void
    {
        $app = $this->createResource([
            'name' => 'test-create-new-app',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $result = $this->apiClient->createOrPatch($app);

        // Metadata
        self::assertNotNull($result->metadata);
        self::assertSame('test-create-new-app', $result->metadata->name);

        // Spec basics
        self::assertNotNull($result->spec);
        self::assertSame('app-123', $result->spec->appId);
        self::assertSame('project-456', $result->spec->projectId);
        self::assertSame('Running', $result->spec->state);
        self::assertSame(1, $result->spec->replicas);

        // Features - storageToken
        self::assertNotNull($result->spec->features);
        self::assertNotNull($result->spec->features->storageToken);
        self::assertSame('test-token', $result->spec->features->storageToken->description);
        self::assertTrue($result->spec->features->storageToken->canManageBuckets);
        self::assertNotNull($result->spec->features->storageToken->setEnvs);
        self::assertCount(1, $result->spec->features->storageToken->setEnvs);
        self::assertSame('main', $result->spec->features->storageToken->setEnvs[0]->container);
        self::assertSame('KBC_TOKEN', $result->spec->features->storageToken->setEnvs[0]->envName);

        // Features - appsProxyIngress
        self::assertNotNull($result->spec->features->appsProxyIngress);
        self::assertSame('main', $result->spec->features->appsProxyIngress->container);
        self::assertSame(8080, $result->spec->features->appsProxyIngress->targetPort);

        // Features - dataDir
        self::assertNotNull($result->spec->features->dataDir);
        self::assertNotNull($result->spec->features->dataDir->mount);
        self::assertCount(1, $result->spec->features->dataDir->mount);
        self::assertSame('main', $result->spec->features->dataDir->mount[0]->container);
        self::assertNotNull($result->spec->features->dataDir->dataLoader);
        self::assertSame('config-1', $result->spec->features->dataDir->dataLoader->configId);

        // Features - mountConfig
        self::assertNotNull($result->spec->features->mountConfig);
        self::assertSame('config-1', $result->spec->features->mountConfig->configId);
        self::assertNotNull($result->spec->features->mountConfig->mount);
        self::assertCount(1, $result->spec->features->mountConfig->mount);
        self::assertSame('/config.json', $result->spec->features->mountConfig->mount[0]->path);
        self::assertNotNull($result->spec->features->mountConfig->mount[0]->fields);
        self::assertCount(1, $result->spec->features->mountConfig->mount[0]->fields);
        self::assertSame('$.foo', $result->spec->features->mountConfig->mount[0]->fields[0]->source);

        // PodSpec
        self::assertNotNull($result->spec->podSpec);
        self::assertSame('Always', $result->spec->podSpec->restartPolicy);
        self::assertNotNull($result->spec->podSpec->containers);
        self::assertCount(1, $result->spec->podSpec->containers);
        self::assertSame('main', $result->spec->podSpec->containers[0]->name);
        self::assertSame('busybox', $result->spec->podSpec->containers[0]->image);
        self::assertNotNull($result->spec->podSpec->containers[0]->env);
        self::assertCount(1, $result->spec->podSpec->containers[0]->env);
        self::assertSame('FOO', $result->spec->podSpec->containers[0]->env[0]->name);
    }

    public function testCreateOrPatchUpdatesExistingApp(): void
    {
        // Create initial app
        $app = $this->createResource([
            'name' => 'test-patch-existing-app',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
        $created = $this->apiClient->create($app);

        // Verify initial state
        self::assertNotNull($created->spec);
        self::assertSame('Running', $created->spec->state);
        self::assertSame(1, $created->spec->replicas);

        // Update state and replicas
        self::assertNotNull($app->spec);
        $app->spec->replicas = 3;
        $app->spec->state = 'Stopped';

        $result = $this->apiClient->createOrPatch($app);

        self::assertNotNull($result->metadata);
        self::assertSame('test-patch-existing-app', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame(3, $result->spec->replicas);
        self::assertSame('Stopped', $result->spec->state);
    }
}
