<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Model\V1;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppContainer;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppFeatures;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppPodSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppsProxyIngressSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\ConfigMountItemSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\ConfigMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\DataDirMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\DataDirSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\DataLoaderSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\MountConfigField;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\SetEnvSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\StorageTokenSpec;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EnvVar;
use Kubernetes\Model\Io\K8s\Api\Core\V1\HTTPGetAction;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Probe;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ResourceRequirements;
use PHPUnit\Framework\TestCase;
use TypeError;

class AppModelTest extends TestCase
{
    /**
     * Real-life test data based on StreamlitAppManifestFactory from sandboxes-service
     */
    private static function getAppTestData(): array
    {
        return [
            'apiVersion' => 'apps.keboola.com/v1',
            'kind' => 'App',
            'metadata' => [
                'name' => 'app-12345',
            ],
            'spec' => [
                'appId' => '12345',
                'projectId' => 'project-789',
                'state' => 'Running',
                'replicas' => 1,
                'autoRestartEnabled' => false,
                'features' => [
                    'storageToken' => [
                        'description' => '[_internal][app] App 12345',
                        'canManageBuckets' => true,
                        'canReadAllFileUploads' => true,
                        'canPurgeTrash' => false,
                        'setEnvs' => [
                            [
                                'container' => 'app',
                                'envName' => 'KBC_TOKEN',
                            ],
                        ],
                    ],
                    'appsProxyIngress' => [
                        'container' => 'app',
                        'targetPort' => 8888,
                    ],
                    'dataDir' => [
                        'mount' => [
                            [
                                'container' => 'app',
                            ],
                        ],
                        'dataLoader' => [
                            'branchId' => 'main',
                            'componentId' => 'keboola.streamlit',
                            'configId' => 'config-456',
                        ],
                    ],
                    'mountConfig' => [
                        'branchId' => 'main',
                        'componentId' => 'keboola.streamlit',
                        'configId' => 'config-456',
                        'configVersion' => '3',
                        'mount' => [
                            [
                                'container' => 'app',
                                'path' => '/data/config.json',
                                'fields' => [
                                    [
                                        'source' => '$.parameters.packages',
                                        'target' => 'packages',
                                    ],
                                    [
                                        'source' => '$.parameters.script',
                                        'target' => 'script',
                                    ],
                                    [
                                        'source' => '$.parameters.dataApp',
                                        'target' => 'dataApp',
                                    ],
                                    [
                                        'source' => '$.storage.input',
                                        'target' => 'input',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
                'podSpec' => [
                    'restartPolicy' => 'Always',
                    'annotations' => [
                        'apps.keboola.com/restartedAt' => '2024-01-15T10:30:00+00:00',
                    ],
                    'containers' => [
                        [
                            'name' => 'app',
                            'image' => 'keboola.azurecr.io/docker-python-streamlit:1.2.3',
                            'env' => [
                                ['name' => 'KBC_URL', 'value' => 'https://connection.keboola.com'],
                                ['name' => 'SANDBOX_ID', 'value' => '12345'],
                                ['name' => 'PROJECT_ID', 'value' => 'project-789'],
                                ['name' => 'ROOT_DIR', 'value' => ''],
                                ['name' => 'DATA_LOADER_API_URL', 'value' => 'localhost:8080'],
                                ['name' => 'IS_OPERATOR', 'value' => 'true'],
                            ],
                            'resources' => [
                                'requests' => [
                                    'memory' => '256M',
                                    'cpu' => '250m',
                                ],
                                'limits' => [
                                    'memory' => '512M',
                                    'cpu' => '500m',
                                ],
                            ],
                            'startupProbe' => [
                                'httpGet' => [
                                    'path' => '/',
                                    'port' => 8888,
                                ],
                                'initialDelaySeconds' => 1,
                                'periodSeconds' => 1,
                                'failureThreshold' => 120,
                            ],
                            'readinessProbe' => [
                                'httpGet' => [
                                    'path' => '/',
                                    'port' => 8888,
                                ],
                                'initialDelaySeconds' => 0,
                                'periodSeconds' => 10,
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }

    public function testStreamlitAppModelHydration(): void
    {
        $data = self::getAppTestData();
        $app = new App($data);

        // Basic metadata
        self::assertNotNull($app->metadata);
        self::assertSame('app-12345', $app->metadata->name);

        // Spec basics
        self::assertNotNull($app->spec);
        self::assertInstanceOf(AppSpec::class, $app->spec);
        self::assertSame('12345', $app->spec->appId);
        self::assertSame('project-789', $app->spec->projectId);
        self::assertSame('Running', $app->spec->state);
        self::assertSame(1, $app->spec->replicas);
        self::assertFalse($app->spec->autoRestartEnabled);

        // Features
        self::assertNotNull($app->spec->features);
        self::assertInstanceOf(AppFeatures::class, $app->spec->features);
        $this->assertStorageTokenFeature($app->spec->features->storageToken);
        $this->assertAppsProxyIngressFeature($app->spec->features->appsProxyIngress);
        $this->assertDataDirFeature($app->spec->features->dataDir);
        $this->assertMountConfigFeature($app->spec->features->mountConfig);

        // PodSpec
        self::assertNotNull($app->spec->podSpec);
        self::assertInstanceOf(AppPodSpec::class, $app->spec->podSpec);
        $this->assertPodSpec($app->spec->podSpec);
    }

    private function assertStorageTokenFeature(?StorageTokenSpec $storageToken): void
    {
        self::assertNotNull($storageToken);
        self::assertInstanceOf(StorageTokenSpec::class, $storageToken);
        self::assertSame('[_internal][app] App 12345', $storageToken->description);
        self::assertTrue($storageToken->canManageBuckets);
        self::assertTrue($storageToken->canReadAllFileUploads);
        self::assertFalse($storageToken->canPurgeTrash);

        // setEnvs
        self::assertNotNull($storageToken->setEnvs);
        self::assertCount(1, $storageToken->setEnvs);
        self::assertInstanceOf(SetEnvSpec::class, $storageToken->setEnvs[0]);
        self::assertSame('app', $storageToken->setEnvs[0]->container);
        self::assertSame('KBC_TOKEN', $storageToken->setEnvs[0]->envName);
    }

    private function assertAppsProxyIngressFeature(?AppsProxyIngressSpec $appsProxyIngress): void
    {
        self::assertNotNull($appsProxyIngress);
        self::assertInstanceOf(AppsProxyIngressSpec::class, $appsProxyIngress);
        self::assertSame('app', $appsProxyIngress->container);
        self::assertSame(8888, $appsProxyIngress->targetPort);
    }

    private function assertDataDirFeature(?DataDirSpec $dataDir): void
    {
        self::assertNotNull($dataDir);
        self::assertInstanceOf(DataDirSpec::class, $dataDir);

        // mount
        self::assertNotNull($dataDir->mount);
        self::assertCount(1, $dataDir->mount);
        self::assertInstanceOf(DataDirMountSpec::class, $dataDir->mount[0]);
        self::assertSame('app', $dataDir->mount[0]->container);

        // dataLoader
        self::assertNotNull($dataDir->dataLoader);
        self::assertInstanceOf(DataLoaderSpec::class, $dataDir->dataLoader);
        self::assertSame('main', $dataDir->dataLoader->branchId);
        self::assertSame('keboola.streamlit', $dataDir->dataLoader->componentId);
        self::assertSame('config-456', $dataDir->dataLoader->configId);
    }

    private function assertMountConfigFeature(?ConfigMountSpec $mountConfig): void
    {
        self::assertNotNull($mountConfig);
        self::assertInstanceOf(ConfigMountSpec::class, $mountConfig);
        self::assertSame('main', $mountConfig->branchId);
        self::assertSame('keboola.streamlit', $mountConfig->componentId);
        self::assertSame('config-456', $mountConfig->configId);
        self::assertSame('3', $mountConfig->configVersion);

        // mount
        self::assertNotNull($mountConfig->mount);
        self::assertCount(1, $mountConfig->mount);
        self::assertInstanceOf(ConfigMountItemSpec::class, $mountConfig->mount[0]);
        self::assertSame('app', $mountConfig->mount[0]->container);
        self::assertSame('/data/config.json', $mountConfig->mount[0]->path);

        // fields
        self::assertNotNull($mountConfig->mount[0]->fields);
        self::assertCount(4, $mountConfig->mount[0]->fields);

        $expectedFields = [
            ['$.parameters.packages', 'packages'],
            ['$.parameters.script', 'script'],
            ['$.parameters.dataApp', 'dataApp'],
            ['$.storage.input', 'input'],
        ];

        foreach ($mountConfig->mount[0]->fields as $index => $field) {
            self::assertInstanceOf(MountConfigField::class, $field);
            self::assertSame($expectedFields[$index][0], $field->source);
            self::assertSame($expectedFields[$index][1], $field->target);
        }
    }

    private function assertPodSpec(AppPodSpec $podSpec): void
    {
        self::assertSame('Always', $podSpec->restartPolicy);

        // annotations
        self::assertNotNull($podSpec->annotations);
        self::assertArrayHasKey('apps.keboola.com/restartedAt', $podSpec->annotations);
        self::assertSame('2024-01-15T10:30:00+00:00', $podSpec->annotations['apps.keboola.com/restartedAt']);

        // containers
        self::assertNotNull($podSpec->containers);
        self::assertCount(1, $podSpec->containers);
        self::assertInstanceOf(AppContainer::class, $podSpec->containers[0]);

        $container = $podSpec->containers[0];
        self::assertSame('app', $container->name);
        self::assertSame('keboola.azurecr.io/docker-python-streamlit:1.2.3', $container->image);

        // env vars
        self::assertNotNull($container->env);
        self::assertCount(6, $container->env);
        self::assertInstanceOf(EnvVar::class, $container->env[0]);
        self::assertSame('KBC_URL', $container->env[0]->name);
        self::assertSame('https://connection.keboola.com', $container->env[0]->value);

        // resources
        self::assertNotNull($container->resources);
        self::assertInstanceOf(ResourceRequirements::class, $container->resources);

        // probes
        self::assertNotNull($container->startupProbe);
        self::assertInstanceOf(Probe::class, $container->startupProbe);
        self::assertNotNull($container->startupProbe->httpGet);
        self::assertInstanceOf(HTTPGetAction::class, $container->startupProbe->httpGet);
        self::assertSame('/', $container->startupProbe->httpGet->path);
        self::assertEquals(8888, $container->startupProbe->httpGet->port);
        self::assertSame(1, $container->startupProbe->initialDelaySeconds);
        self::assertSame(1, $container->startupProbe->periodSeconds);
        self::assertSame(120, $container->startupProbe->failureThreshold);

        self::assertNotNull($container->readinessProbe);
        self::assertInstanceOf(Probe::class, $container->readinessProbe);
        self::assertSame(0, $container->readinessProbe->initialDelaySeconds);
        self::assertSame(10, $container->readinessProbe->periodSeconds);
    }

    public function testAppModelSerialization(): void
    {
        $data = self::getAppTestData();
        $app = new App($data);

        // Test that the model can be serialized back to array
        $serialized = $app->getArrayCopy();

        self::assertIsArray($serialized);
        self::assertArrayHasKey('metadata', $serialized);
        self::assertArrayHasKey('spec', $serialized);

        // Verify key nested values survive round-trip
        self::assertSame('12345', $serialized['spec']['appId']);
        self::assertSame('project-789', $serialized['spec']['projectId']);
        self::assertSame('Running', $serialized['spec']['state']);
    }

    public static function provideInvalidTestData(): iterable
    {
        yield 'wrong replicas type - string instead of int' => [
            'data' => ['spec' => ['replicas' => 'one']],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong targetPort type - string instead of int' => [
            'data' => ['spec' => ['features' => ['appsProxyIngress' => ['targetPort' => 'invalid']]]],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong canManageBuckets type - array instead of bool' => [
            'data' => ['spec' => ['features' => ['storageToken' => ['canManageBuckets' => []]]]],
            'expectedMessage' => 'Cannot assign array to property',
        ];

        yield 'wrong observedGeneration type - string instead of int' => [
            'data' => ['status' => ['observedGeneration' => 'invalid']],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong autoRestartEnabled type - string instead of bool' => [
            'data' => ['spec' => ['autoRestartEnabled' => []]],
            'expectedMessage' => 'Cannot assign array to property',
        ];
    }

    /**
     * @dataProvider provideInvalidTestData
     */
    public function testInvalidDataTypesThrowTypeError(array $data, string $expectedMessage): void
    {
        $this->expectException(TypeError::class);
        $this->expectExceptionMessageMatches('/' . preg_quote($expectedMessage, '/') . '/');

        new App($data);
    }
}
